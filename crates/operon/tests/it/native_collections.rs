//! Collections, aliases and documents over the native API (plan M1.2 Task 11).

use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::common::{Native, TOKEN, UUID, kb, kb_docs, kb_schema};

fn keys(value: &Value) -> Vec<&str> {
    let mut keys: Vec<&str> = value
        .as_object()
        .expect("an object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    keys
}

const INFO_KEYS: [&str; 13] = [
    "aliases",
    "created_at_ms",
    "hot",
    "id",
    "link_lag_records",
    "live_doc_count",
    "manifest_version",
    "name",
    "namespace",
    "partitions",
    "schema",
    "size_bytes",
    "stream",
];

#[tokio::test]
async fn collection_routes_speak_the_documented_json() {
    let api = Native::start().await;
    let create = json!({"name": "kb", "schema": kb_schema(), "partitions": 2});
    let first = api
        .post("/v1/namespaces/w/collections", create.clone())
        .await
        .expect(StatusCode::CREATED);
    assert_eq!(keys(&first), INFO_KEYS);
    assert_eq!(first["name"], "kb");
    assert_eq!(first["namespace"], "w");
    assert_eq!(first["partitions"], 2);
    assert_eq!(first["schema"]["version"], 1);
    assert_eq!(first["manifest_version"], 0);
    assert_eq!(first["aliases"], json!([]));
    assert_eq!(first["schema"]["vectors"][0]["dim"], 3);

    // A retry-safe repeat is 201 with the same id.
    let again = api
        .post("/v1/namespaces/w/collections", create.clone())
        .await
        .expect(StatusCode::CREATED);
    assert_eq!(again["id"], first["id"]);

    // A different schema under the name is 409 already_exists.
    let mut other = kb_schema();
    other["vectors"][0]["dim"] = json!(4);
    let reply = api
        .post(
            "/v1/namespaces/w/collections",
            json!({"name": "kb", "schema": other}),
        )
        .await;
    let body = reply.expect(StatusCode::CONFLICT);
    assert_eq!(body["error"], "already_exists", "{body}");

    // Unknown keys and malformed JSON are 400.
    let body = api
        .post(
            "/v1/namespaces/w/collections",
            json!({"name": "x", "schema": kb_schema(), "partitons": 2}),
        )
        .await
        .expect(StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_argument");

    // A second collection, to check the sort.
    api.post(
        "/v1/namespaces/w/collections",
        json!({"name": "alpha", "schema": kb_schema()}),
    )
    .await
    .expect(StatusCode::CREATED);
    let list = api
        .get("/v1/namespaces/w/collections")
        .await
        .expect(StatusCode::OK);
    assert_eq!(keys(&list), ["collections"]);
    let names: Vec<&str> = list["collections"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["alpha", "kb"]);
    assert_eq!(keys(&list["collections"][1]), INFO_KEYS);

    // Aliases, and a get by alias.
    let body = api
        .post(
            "/v1/namespaces/w/aliases",
            json!({"actions": [{"create": {"alias": "kb_live", "collection": "kb"}}]}),
        )
        .await
        .expect(StatusCode::OK);
    assert_eq!(body, json!({}));
    let by_alias = api
        .get("/v1/namespaces/w/collections/kb_live")
        .await
        .expect(StatusCode::OK);
    assert_eq!(keys(&by_alias), INFO_KEYS);
    assert_eq!(by_alias["id"], first["id"]);
    assert_eq!(by_alias["name"], "kb");
    assert_eq!(by_alias["aliases"], json!(["kb_live"]));

    // Fields: the new schema.
    let body = api
        .post(
            "/v1/namespaces/w/collections/kb/fields",
            json!({"fields": [{"name": "color", "kind": "keyword"}], "annotations": {"operon.team": "x"}}),
        )
        .await
        .expect(StatusCode::OK);
    assert_eq!(keys(&body), ["schema"]);
    assert_eq!(body["schema"]["version"], 2);
    assert_eq!(body["schema"]["fields"][3]["name"], "color");
    assert_eq!(body["schema"]["annotations"]["operon.team"], "x");

    // Versions: none before the first commit.
    let body = api
        .get("/v1/namespaces/w/collections/kb/versions")
        .await
        .expect(StatusCode::OK);
    assert_eq!(body, json!({"versions": []}));

    // A missing collection is 404 with its kind and name.
    let body = api
        .get("/v1/namespaces/w/collections/nope")
        .await
        .expect(StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "not_found");
    assert_eq!(body["kind"], "collection");
    assert_eq!(body["name"], "nope");

    // Drop twice.
    let body = api
        .delete("/v1/namespaces/w/collections/kb")
        .await
        .expect(StatusCode::OK);
    assert_eq!(body, json!({"dropped": true}));
    let body = api
        .delete("/v1/namespaces/w/collections/kb")
        .await
        .expect(StatusCode::OK);
    assert_eq!(body, json!({"dropped": false}));
    api.shutdown().await;
}

#[tokio::test]
async fn write_get_scroll_count_round_trip_over_http() {
    let api = Native::start().await;
    api.post(
        "/v1/namespaces/w/collections",
        json!({"name": "kb", "schema": kb_schema(), "partitions": 2}),
    )
    .await
    .expect(StatusCode::CREATED);
    // Step 10.
    let reply = api
        .post("/v1/namespaces/w/collections/kb/documents", kb_docs())
        .await;
    let header = reply.header(TOKEN).expect("token header").to_string();
    let body = reply.expect(StatusCode::OK);
    assert_eq!(keys(&body), ["positions", "results", "token"]);
    assert_eq!(body["token"], header.as_str());
    assert_eq!(body["results"], json!(vec!["accepted"; 6]));
    for position in body["positions"].as_array().unwrap() {
        assert_eq!(keys(position), ["partition", "seq_no"]);
    }

    // Step 11: request order, with null for 999.
    let docs = api
        .post_with(
            "/v1/namespaces/w/collections/kb/documents/get",
            &[(TOKEN, &header)],
            json!({"ids": [1, 18446744073709551615u64, "k-str", {"uuid": UUID}, 999],
                   "select": {"source": "all", "vectors": ["embedding"], "fields": []}}),
        )
        .await
        .expect(StatusCode::OK);
    assert_eq!(keys(&docs), ["documents", "read_token"]);
    let documents = docs["documents"].as_array().unwrap();
    assert_eq!(documents.len(), 5);
    assert_eq!(
        keys(&documents[0]),
        ["fields", "id", "partition", "seq_no", "source", "vectors"]
    );
    assert_eq!(documents[0]["id"], 1);
    assert_eq!(documents[0]["source"]["body"], "refund policy");
    assert_eq!(documents[0]["vectors"]["embedding"], json!([1.0, 0.0, 0.0]));
    assert_eq!(documents[1]["id"], json!(18446744073709551615u64));
    assert_eq!(documents[2]["id"], "k-str");
    assert_eq!(documents[3]["id"]["uuid"], UUID);
    assert_eq!(documents[4], Value::Null);

    // Steps 14–16: a patch and a delete, visible to the next get with no
    // token (strong by default).
    let body = api
        .post(
            "/v1/namespaces/w/collections/kb/documents",
            json!({"ops": [{"patch": {"id": 1, "mode": "merge_deep", "source": {"meta": {"x": 1}}, "delete_keys": ["tenant"], "vectors": {}, "upsert": null}}], "report_existence": true}),
        )
        .await
        .expect(StatusCode::OK);
    assert_eq!(body["results"], json!(["updated"]));
    let body = api
        .post(
            "/v1/namespaces/w/collections/kb/documents",
            json!({"ops": [{"delete": {"id": 2}}, {"delete": {"id": 424242}}], "report_existence": true}),
        )
        .await
        .expect(StatusCode::OK);
    assert_eq!(body["results"], json!(["deleted", "not_found"]));
    assert!(body["positions"][0]["seq_no"].is_u64());
    let docs = api
        .post(
            "/v1/namespaces/w/collections/kb/documents/get",
            json!({"ids": [1, 2]}),
        )
        .await
        .expect(StatusCode::OK);
    assert_eq!(docs["documents"][0]["source"]["meta"]["x"], 1);
    assert!(docs["documents"][0]["source"].get("tenant").is_none());
    assert_eq!(docs["documents"][1], Value::Null);

    // Scroll in pages of two until `next` is null.
    let mut seen = Vec::new();
    let mut after = Value::Null;
    loop {
        let page = api
            .post(
                "/v1/namespaces/w/collections/kb/documents/scroll",
                json!({"after": after, "limit": 2, "select": {"source": "none", "vectors": [], "fields": []}}),
            )
            .await
            .expect(StatusCode::OK);
        assert_eq!(keys(&page), ["documents", "next", "read_token"]);
        for doc in page["documents"].as_array().unwrap() {
            assert_eq!(doc["source"], Value::Null);
            seen.push(doc["id"].clone());
        }
        if page["next"].is_null() {
            break;
        }
        after = page["next"].clone();
    }
    assert_eq!(
        seen,
        [
            json!(1),
            json!(3),
            json!(18446744073709551615u64),
            json!({"uuid": UUID}),
            json!("k-str")
        ]
    );

    // Count, with and without a filter.
    let reply = api
        .post("/v1/namespaces/w/collections/kb/documents/count", json!({}))
        .await;
    let read = reply.header(TOKEN).expect("token header").to_string();
    let body = reply.expect(StatusCode::OK);
    assert_eq!(keys(&body), ["count", "read_token"]);
    assert_eq!(body["count"], 5);
    assert_eq!(body["read_token"], read.as_str());
    let body = api
        .post(
            "/v1/namespaces/w/collections/kb/documents/count",
            json!({"filter": {"term": {"field": "tenant", "value": "c"}}, "consistency": "eventual"}),
        )
        .await
        .expect(StatusCode::OK);
    assert_eq!(body["count"], 3);
    api.shutdown().await;
}

#[tokio::test]
async fn a_rejected_atomic_write_is_400_with_the_op_index() {
    let api = Native::start().await;
    kb(&api, "w").await;
    // A wrong dimension in op 1: nothing of the request is written.
    let body = api
        .post(
            "/v1/namespaces/w/collections/kb/documents",
            json!({"ops": [
                {"upsert": {"id": 7, "source": {"tenant": "d"}, "vectors": {"embedding": [1.0, 0.0, 0.0]}}},
                {"upsert": {"id": 8, "source": {}, "vectors": {"embedding": [1.0, 2.0]}}}
            ]}),
        )
        .await
        .expect(StatusCode::BAD_REQUEST);
    assert!(
        body["error"] == "schema_violation" || body["error"] == "invalid_argument",
        "{body}"
    );
    assert_eq!(body["index"], 1, "{body}");
    let docs = api
        .post(
            "/v1/namespaces/w/collections/kb/documents/get",
            json!({"ids": [7, 8]}),
        )
        .await
        .expect(StatusCode::OK);
    assert_eq!(docs["documents"], json!([null, null]));

    // An op the API cannot parse names its index too.
    let body = api
        .post(
            "/v1/namespaces/w/collections/kb/documents",
            json!({"ops": [
                {"delete": {"id": 1}},
                {"upsert": {"id": 9, "sparse_vectors": {"s": {"indices": [1, 1], "values": [1.0, 2.0]}}}}
            ]}),
        )
        .await
        .expect(StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_argument");
    assert_eq!(body["index"], 1);
    let message = body["message"].as_str().unwrap();
    assert!(message.contains("op 1: sparse vector s: "), "{message}");
    let body = api
        .post(
            "/v1/namespaces/w/collections/kb/documents",
            json!({"ops": [{"upsert": {"id": true}}]}),
        )
        .await
        .expect(StatusCode::BAD_REQUEST);
    assert_eq!(body["index"], 0);
    api.shutdown().await;
}

#[tokio::test]
async fn write_responses_carry_the_consistency_token_header() {
    let api = Native::start().await;
    api.post(
        "/v1/namespaces/w/collections",
        json!({"name": "kb", "schema": kb_schema(), "partitions": 2}),
    )
    .await
    .expect(StatusCode::CREATED);
    let reply = api
        .post(
            "/v1/namespaces/w/collections/kb/documents",
            json!({"ops": [{"upsert": {"id": 1, "source": {"tenant": "a"}}}]}),
        )
        .await;
    let header = reply.header(TOKEN).expect("token header").to_string();
    let body = reply.expect(StatusCode::OK);
    assert!(header.starts_with("v1:s"), "{header}");
    assert_eq!(body["token"], header.as_str());

    // A read with the token (merged into an `at_least` body token) sees the
    // write, and answers its own read token in the header and the body.
    let reply = api
        .post_with(
            "/v1/namespaces/w/collections/kb/documents/get",
            &[(TOKEN, &header)],
            json!({"ids": [1], "consistency": {"at_least": "v1:"}}),
        )
        .await;
    let read = reply.header(TOKEN).expect("read token header").to_string();
    let body = reply.expect(StatusCode::OK);
    assert_eq!(body["read_token"], read.as_str());
    assert_eq!(body["documents"][0]["source"]["tenant"], "a");

    // An unparsable header token is 400.
    let body = api
        .post_with(
            "/v1/namespaces/w/collections/kb/documents/get",
            &[(TOKEN, "c1:nope")],
            json!({"ids": [1]}),
        )
        .await
        .expect(StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_argument");
    api.shutdown().await;
}
