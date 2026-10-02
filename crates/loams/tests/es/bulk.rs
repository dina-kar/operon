//! Task 5: `_bulk`.

use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use crate::docs::{NO_WRITE_INDEX, cache_fixture, count, stored};
use crate::harness::{Answer, Es};

/// `lines` as an NDJSON body, each line terminated.
fn ndjson(lines: &[Value]) -> Vec<u8> {
    let mut body = String::new();
    for line in lines {
        body.push_str(&line.to_string());
        body.push('\n');
    }
    body.into_bytes()
}

async fn bulk(es: &Es, method: Method, path: &str, lines: &[Value]) -> Answer {
    let a = es
        .send_raw(
            method,
            path,
            Some(("application/x-ndjson", ndjson(lines))),
            &[],
        )
        .await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.text);
    a
}

/// `(action, status)` of every item.
fn statuses(a: &Answer) -> Vec<(String, u64)> {
    a.body["items"]
        .as_array()
        .expect("items")
        .iter()
        .map(|item| {
            let (action, body) = item.as_object().expect("item").iter().next().expect("one");
            (action.clone(), body["status"].as_u64().expect("status"))
        })
        .collect()
}

fn item(a: &Answer, i: usize) -> &Value {
    let item = &a.body["items"][i];
    item.as_object()
        .and_then(|o| o.values().next())
        .expect("item body")
}

#[tokio::test]
async fn helpers_bulk_ndjson_indexes_every_item() {
    let es = Es::start().await;
    let mut lines = Vec::new();
    for (id, text) in [("1", "foo"), ("2", "bar"), ("3", "baz")] {
        lines.push(json!({"index": {"_index": "test_b", "_id": id}}));
        lines.push(json!({"text": text, "metadata": {"page": 0}}));
    }
    let a = bulk(&es, Method::PUT, "/_bulk?refresh=true", &lines).await;
    assert_eq!(a.body["errors"], false, "{}", a.text);
    assert!(a.body["took"].is_u64());
    for i in 0..3 {
        let body = item(&a, i);
        assert_eq!(body["status"], 201, "{}", a.text);
        assert_eq!(body["result"], "created");
        assert_eq!(body["_index"], "test_b");
        assert_eq!(body["forced_refresh"], true);
    }
    assert!(a.header("loams-consistency-token").starts_with("v1:"));
    assert_eq!(count(&es, "test_b").await, 3);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn bulk_delete_of_a_missing_id_is_item_404() {
    let es = Es::start().await;
    Es::ok(es.put("/i", None).await);
    let a = es.put("/i/_doc/1", Some(json!({"a": 1}))).await;
    assert_eq!(a.status, StatusCode::CREATED, "{}", a.text);
    let a = bulk(
        &es,
        Method::PUT,
        "/_bulk?refresh=true",
        &[
            json!({"delete": {"_index": "i", "_id": "not-existing"}}),
            json!({"delete": {"_index": "i", "_id": "1"}}),
        ],
    )
    .await;
    assert_eq!(
        statuses(&a),
        [("delete".to_string(), 404), ("delete".to_string(), 200)]
    );
    assert_eq!(item(&a, 0)["result"], "not_found");
    assert_eq!(item(&a, 1)["result"], "deleted");
    assert_eq!(a.body["errors"], true);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_dims_mismatch_fails_only_that_item() {
    let es = Es::start().await;
    let body = json!({"mappings": {"properties": {
        "vector": {"type": "dense_vector", "dims": 5, "index": true, "similarity": "cosine"},
    }}});
    Es::ok(es.put("/v", Some(body)).await);
    let a = bulk(
        &es,
        Method::POST,
        "/v/_bulk",
        &[
            json!({"index": {"_id": "16"}}),
            json!({"vector": vec![1.0; 16]}),
            json!({"index": {"_id": "5"}}),
            json!({"vector": vec![1.0; 5]}),
        ],
    )
    .await;
    assert_eq!(
        statuses(&a),
        [("index".to_string(), 400), ("index".to_string(), 201)]
    );
    let error = &item(&a, 0)["error"];
    assert_eq!(error["type"], "document_parsing_exception");
    assert!(
        error["reason"]
            .as_str()
            .expect("reason")
            .contains("has a different number of dimensions [16] than defined in the mapping [5]"),
        "{}",
        a.text
    );
    assert_eq!(item(&a, 0)["_id"], "16");
    assert_eq!(count(&es, "v").await, 1);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_bulk_with_repeated_ids_reports_each_item_in_order() {
    let es = Es::start().await;
    let a = bulk(
        &es,
        Method::POST,
        "/i/_bulk",
        &[
            json!({"index": {"_id": "a"}}),
            json!({"x": 1}),
            json!({"update": {"_id": "a"}}),
            json!({"doc": {"x": 2}}),
            json!({"delete": {"_id": "a"}}),
            json!({"create": {"_id": "a"}}),
            json!({"x": 3}),
            json!({"update": {"_id": "b"}}),
            json!({"doc": {}}),
        ],
    )
    .await;
    let got: Vec<u64> = statuses(&a).into_iter().map(|(_, s)| s).collect();
    assert_eq!(got, [201, 200, 200, 201, 404], "{}", a.text);
    let actions: Vec<String> = statuses(&a).into_iter().map(|(n, _)| n).collect();
    assert_eq!(actions, ["index", "update", "delete", "create", "update"]);
    assert_eq!(item(&a, 4)["error"]["type"], "document_missing_exception");
    assert_eq!(a.body["errors"], true);
    assert_eq!(stored(&es, "i", "a").await, Some(json!({"x": 3})));
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn bulk_spans_several_indices() {
    let es = Es::start().await;
    let mut lines = Vec::new();
    for n in 0..6 {
        let index = if n % 2 == 0 { "test_1" } else { "test_2" };
        lines.push(json!({"index": {"_index": index, "_id": n.to_string()}}));
        lines.push(json!({"n": n}));
    }
    let a = bulk(&es, Method::POST, "/_bulk", &lines).await;
    assert_eq!(a.body["errors"], false, "{}", a.text);
    for n in 0..6 {
        let body = item(&a, n);
        assert_eq!(body["_id"], n.to_string());
        assert_eq!(body["_index"], if n % 2 == 0 { "test_1" } else { "test_2" });
    }
    assert_eq!(count(&es, "test_1").await, 3);
    assert_eq!(count(&es, "test_2").await, 3);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_default_pipeline_fails_every_write_item() {
    let es = Es::start().await;
    let body = json!({"settings": {"index": {"default_pipeline": "not-existing-pipeline"}}});
    Es::ok(es.put("/i", Some(body)).await);
    let a = bulk(
        &es,
        Method::POST,
        "/_bulk",
        &[
            json!({"index": {"_index": "i", "_id": "1"}}),
            json!({"a": 1}),
        ],
    )
    .await;
    let body = item(&a, 0);
    assert_eq!(body["status"], 400, "{}", a.text);
    assert_eq!(
        body["error"]["reason"],
        "pipeline with id [not-existing-pipeline] does not exist"
    );
    assert_eq!(a.body["errors"], true);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn update_actions_in_bulk_upsert() {
    let es = Es::start().await;
    let a = bulk(
        &es,
        Method::POST,
        "/i/_bulk",
        &[
            json!({"update": {"_id": "u"}}),
            json!({"doc": {"a": 1}, "doc_as_upsert": true}),
        ],
    )
    .await;
    assert_eq!(item(&a, 0)["status"], 201, "{}", a.text);
    assert_eq!(item(&a, 0)["result"], "created");
    assert_eq!(stored(&es, "i", "u").await, Some(json!({"a": 1})));
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn require_alias_on_a_plain_index_fails_the_items() {
    let es = Es::start().await;
    Es::ok(es.put("/test_1", None).await);
    let a = bulk(
        &es,
        Method::POST,
        "/_bulk?require_alias=true",
        &[
            json!({"index": {"_index": "test_1", "_id": "1"}}),
            json!({"a": 1}),
        ],
    )
    .await;
    let body = item(&a, 0);
    assert_eq!(body["status"], 404, "{}", a.text);
    assert_eq!(body["error"]["type"], "index_not_found_exception");
    assert_eq!(
        body["error"]["reason"],
        "no such index [test_1] and [require_alias] request flag is [true] and [test_1] is not \
         an alias"
    );
    assert_eq!(count(&es, "test_1").await, 0);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn bulk_through_an_alias_uses_the_write_index() {
    let es = Es::start().await;
    cache_fixture(&es).await;
    let index_items = || {
        let mut lines = Vec::new();
        for id in ["k1", "k2", "k3"] {
            lines.push(json!({"index": {"_index": "test_alias", "_id": id}}));
            lines.push(json!({"vector_dump": "AAAA", "namespace": "n"}));
        }
        lines
    };
    let delete_items = |ids: &[&str]| {
        ids.iter()
            .map(|id| json!({"delete": {"_index": "test_alias", "_id": id}}))
            .collect::<Vec<_>>()
    };
    let path = "/_bulk?refresh=true&require_alias=true";
    let a = bulk(&es, Method::POST, path, &index_items()).await;
    assert_eq!(a.body["errors"], false, "{}", a.text);
    for i in 0..3 {
        assert_eq!(item(&a, i)["status"], 201);
        assert_eq!(item(&a, i)["_index"], "test_index2");
    }
    let a = bulk(&es, Method::POST, path, &delete_items(&["k1", "k2"])).await;
    assert_eq!(a.body["errors"], false, "{}", a.text);
    assert_eq!(item(&a, 0)["result"], "deleted");
    assert_eq!(item(&a, 1)["result"], "deleted");
    let a = bulk(&es, Method::POST, path, &delete_items(&["k1", "k2", "k3"])).await;
    let got: Vec<u64> = statuses(&a).into_iter().map(|(_, s)| s).collect();
    assert_eq!(got, [404, 404, 200], "{}", a.text);
    assert_eq!(a.body["errors"], true);
    // Both members unset: no write index.
    Es::ok(es.put("/test_index2/_alias/test_alias", None).await);
    let a = bulk(&es, Method::POST, path, &index_items()).await;
    for i in 0..3 {
        let body = item(&a, i);
        assert_eq!(body["status"], 400, "{}", a.text);
        assert_eq!(body["error"]["reason"], NO_WRITE_INDEX);
    }
    assert_eq!(count(&es, "test_index1").await, 0);
    assert_eq!(count(&es, "test_index2").await, 0);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn request_level_errors_fail_the_whole_bulk() {
    let es = Es::start().await;
    let a = es
        .send_raw(
            Method::POST,
            "/_bulk",
            Some((
                "application/x-ndjson",
                b"{\"index\": {\"_id\": \"1\"}}\n{\"a\": 1}\n".to_vec(),
            )),
            &[],
        )
        .await;
    a.assert_error(
        400,
        "action_request_validation_exception",
        Some("Validation Failed: 1: index is missing;"),
    );
    let a = es
        .send_raw(
            Method::POST,
            "/i/_bulk",
            Some(("application/x-ndjson", b"{\"index\": {}}\n{}".to_vec())),
            &[],
        )
        .await;
    a.assert_error(400, "illegal_argument_exception", None);
    // A source that is not JSON and an OCC key fail only their items.
    let a = es
        .send_raw(
            Method::POST,
            "/i/_bulk",
            Some((
                "application/x-ndjson",
                b"{\"index\": {\"_id\": \"1\"}}\n{\"a\": \n{\"delete\": {\"_id\": \"2\", \"version\": 1}}\n{\"index\": {\"_id\": \"3\"}}\n{\"a\": 3}\n"
                    .to_vec(),
            )),
            &[],
        )
        .await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.text);
    let got: Vec<u64> = statuses(&a).into_iter().map(|(_, s)| s).collect();
    assert_eq!(got, [400, 400, 201], "{}", a.text);
    assert_eq!(item(&a, 0)["error"]["type"], "document_parsing_exception");
    assert_eq!(item(&a, 1)["error"]["type"], "illegal_argument_exception");
    es.server.shutdown().await.expect("shutdown");
}
