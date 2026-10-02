//! Task 9a: `_update_by_query` over the native `patch_by_filter` (D87).

use serde_json::{Value, json};

use crate::harness::{Answer, Es};

const REFUSAL: &str = "Loams supports only params assignments and remove() in [_update_by_query] scripts (Elasticsearch API Phase A)";

/// `index` with `metadata.ref` (keyword) and `metadata.page` (long), and
/// `docs` documents: ids 1…, `ref` `a` for odd ids and `b` for even ones.
async fn fixture(es: &Es, index: &str, docs: u64) {
    Es::ok(
        es.put(
            &format!("/{index}"),
            Some(json!({"mappings": {"properties": {
                "text": {"type": "text"},
                "metadata": {"properties": {
                    "ref": {"type": "keyword"},
                    "page": {"type": "long"}
                }}
            }}})),
        )
        .await,
    );
    for id in 1..=docs {
        let doc = json!({
            "text": format!("doc {id}"),
            "metadata": {"ref": if id % 2 == 1 { "a" } else { "b" }, "page": id, "old": 1}
        });
        let a = es.put(&format!("/{index}/_doc/{id}"), Some(doc)).await;
        assert_eq!(a.status.as_u16(), 201, "{}", a.text);
    }
}

async fn source(es: &Es, index: &str, id: u64) -> Value {
    let a = Es::ok(es.get(&format!("/{index}/_doc/{id}")).await);
    a.body["_source"].clone()
}

async fn ubq(es: &Es, path: &str, body: Value) -> Answer {
    es.post(path, body).await
}

fn keys(value: &Value) -> Vec<&str> {
    value
        .as_object()
        .expect("an object")
        .keys()
        .map(String::as_str)
        .collect()
}

#[tokio::test]
async fn update_by_query_applies_param_assignments_and_removes() {
    let es = Es::start().await;
    fixture(&es, "i", 4).await;
    let body = json!({
        "query": {"term": {"metadata.ref": "a"}},
        "conflicts": "proceed",
        "script": {
            "source": "ctx._source.metadata.page = params.p; ctx._source.status = params.s; ctx._source.metadata.remove('old');",
            "lang": "painless",
            "params": {"p": 9, "s": "done"}
        }
    });
    let a = Es::ok(ubq(&es, "/i/_update_by_query?refresh=true", body.clone()).await);
    assert_eq!(
        keys(&a.body),
        [
            "took",
            "timed_out",
            "total",
            "updated",
            "deleted",
            "batches",
            "version_conflicts",
            "noops",
            "retries",
            "throttled_millis",
            "requests_per_second",
            "throttled_until_millis",
            "failures"
        ]
    );
    assert_eq!(a.body["total"], 2, "{}", a.text);
    assert_eq!(a.body["updated"], 2);
    assert_eq!(a.body["noops"], 0);
    assert_eq!(a.body["deleted"], 0);
    assert_eq!(a.body["batches"], 1);
    assert_eq!(a.body["failures"], json!([]));
    assert_eq!(a.body["requests_per_second"], -1.0);
    for id in [1, 3] {
        assert_eq!(
            source(&es, "i", id).await,
            json!({"text": format!("doc {id}"), "metadata": {"ref": "a", "page": 9}, "status": "done"})
        );
    }
    for id in [2, 4] {
        assert_eq!(
            source(&es, "i", id).await,
            json!({"text": format!("doc {id}"), "metadata": {"ref": "b", "page": id, "old": 1}})
        );
    }
    // The same script again changes nothing: every match is a noop.
    let a = Es::ok(ubq(&es, "/i/_update_by_query", body).await);
    assert_eq!(
        (&a.body["total"], &a.body["updated"]),
        (&json!(2), &json!(0))
    );
    assert_eq!(a.body["noops"], 2);
    // `q` selects too.
    let a = Es::ok(
        ubq(
            &es,
            "/i/_update_by_query?q=metadata.ref:b",
            json!({"script": {"source": "ctx._source.status = params.s", "params": {"s": "b"}}}),
        )
        .await,
    );
    assert_eq!(a.body["updated"], 2, "{}", a.text);
    assert_eq!(source(&es, "i", 2).await["status"], "b");
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn update_by_query_through_an_alias_updates_every_member() {
    let es = Es::start().await;
    fixture(&es, "m1", 1).await;
    fixture(&es, "m2", 2).await;
    Es::ok(
        es.post(
            "/_aliases",
            json!({"actions": [
                {"add": {"index": "m1", "alias": "both"}},
                {"add": {"index": "m2", "alias": "both"}}
            ]}),
        )
        .await,
    );
    let a = Es::ok(
        ubq(
            &es,
            "/both/_update_by_query?refresh=true",
            json!({
                "query": {"match_all": {}},
                "script": {"source": "ctx._source.seen = params.t", "params": {"t": true}}
            }),
        )
        .await,
    );
    assert_eq!(
        (&a.body["total"], &a.body["updated"]),
        (&json!(3), &json!(3))
    );
    assert_eq!(a.body["batches"], 2);
    assert_eq!(source(&es, "m1", 1).await["seen"], true);
    for id in [1, 2] {
        assert_eq!(source(&es, "m2", id).await["seen"], true);
    }
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn update_by_query_refuses_other_scripts() {
    let es = Es::start().await;
    fixture(&es, "i", 2).await;
    for script in [
        json!({"source": "ctx._source.metadata.page += 1"}),
        json!({"source": "ctx.op = 'delete'"}),
        json!({"source": "ctx._source.x = params.missing", "params": {"y": 1}}),
        json!({"source": "ctx._source.x = params.y", "lang": "expression", "params": {"y": 1}}),
        json!({"source": "ctx._source.x = params.y; ctx._source.remove('x')", "params": {"y": 1}}),
        json!({"id": "stored-script"}),
    ] {
        let a = ubq(
            &es,
            "/i/_update_by_query",
            json!({"query": {"match_all": {}}, "script": script}),
        )
        .await;
        a.assert_error(400, "illegal_argument_exception", Some(REFUSAL));
    }
    // Phase B parameters and body keys.
    for path in [
        "/i/_update_by_query?wait_for_completion=false",
        "/i/_update_by_query?slices=2",
        "/i/_update_by_query?requests_per_second=10",
    ] {
        let a = ubq(
            &es,
            path,
            json!({"query": {"match_all": {}}, "script": "ctx._source.remove('text')"}),
        )
        .await;
        a.assert_error(400, "illegal_argument_exception", None);
        assert!(
            a.body["error"]["reason"]
                .as_str()
                .is_some_and(|r| r.contains("Elasticsearch API Phase A")),
            "{}",
            a.text
        );
    }
    let a = ubq(
        &es,
        "/i/_update_by_query",
        json!({"query": {"match_all": {}}, "script": "ctx._source.remove('text')", "slice": {"id": 0, "max": 2}}),
    )
    .await;
    a.assert_error(400, "illegal_argument_exception", None);
    // A missing index is 404; nothing was written anywhere.
    let a = ubq(
        &es,
        "/missing/_update_by_query",
        json!({"query": {"match_all": {}}, "script": "ctx._source.remove('text')"}),
    )
    .await;
    a.assert_error(404, "index_not_found_exception", None);
    assert_eq!(source(&es, "i", 1).await["text"], "doc 1");
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn update_by_query_refuses_an_object_assignment() {
    let es = Es::start().await;
    fixture(&es, "i", 2).await;
    let a = ubq(
        &es,
        "/i/_update_by_query",
        json!({
            "query": {"match_all": {}},
            "script": {"source": "ctx._source.metadata = params.m", "params": {"m": {"ref": "c"}}}
        }),
    )
    .await;
    a.assert_error(400, "illegal_argument_exception", Some(REFUSAL));
    assert_eq!(source(&es, "i", 1).await["metadata"]["ref"], "a");
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn update_by_query_without_a_script_is_refused() {
    let es = Es::start().await;
    fixture(&es, "i", 1).await;
    let a = ubq(
        &es,
        "/i/_update_by_query",
        json!({"query": {"match_all": {}}}),
    )
    .await;
    a.assert_error(
        400,
        "illegal_argument_exception",
        Some(
            "re-indexing without a script needs online index changes (D97); not supported in Elasticsearch API Phase A",
        ),
    );
    // A script without a query (and no `q`) updates every document, as in
    // ES (row T11-3).
    let a = Es::ok(
        ubq(
            &es,
            "/i/_update_by_query?refresh=true",
            json!({"script": "ctx._source.remove('text')"}),
        )
        .await,
    );
    assert_eq!(a.body["updated"], a.body["total"], "{}", a.text);
    assert!(a.body["updated"].as_u64().expect("updated") > 0);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn update_by_query_with_max_docs_stops() {
    let es = Es::start_with(|config| config.query.filter_write_batch = 4).await;
    fixture(&es, "i", 25).await;
    let script = json!({"source": "ctx._source.seen = params.t", "params": {"t": 1}});
    let a = Es::ok(
        ubq(
            &es,
            "/i/_update_by_query?refresh=true",
            json!({"query": {"match_all": {}}, "max_docs": 10, "script": script}),
        )
        .await,
    );
    assert_eq!(
        (&a.body["total"], &a.body["updated"]),
        (&json!(10), &json!(10))
    );
    assert_eq!(a.body["batches"], 3, "{}", a.text);
    let counted = Es::ok(
        es.post("/i/_count", json!({"query": {"exists": {"field": "seen"}}}))
            .await,
    );
    assert_eq!(counted.body["count"], 10, "{}", counted.text);
    // The URL's `max_docs`, over the documents already updated: the noops
    // count against it too.
    let a = Es::ok(
        ubq(
            &es,
            "/i/_update_by_query?max_docs=12",
            json!({"query": {"match_all": {}}, "script": script}),
        )
        .await,
    );
    assert_eq!(
        (&a.body["total"], &a.body["updated"]),
        (&json!(12), &json!(2))
    );
    assert_eq!(a.body["noops"], 10);
    let a = ubq(
        &es,
        "/i/_update_by_query",
        json!({"query": {"match_all": {}}, "max_docs": 0, "script": script}),
    )
    .await;
    a.assert_error(
        400,
        "illegal_argument_exception",
        Some("[max_docs] should be >= [slices]"),
    );
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_backpressure_refusal_is_429_es_rejected_execution_exception() {
    // A link that never commits within the test, and a budget of three
    // unapplied records (M1.3 Task 15).
    let es = Es::start_with(|config| {
        config.link.batch_interval = std::time::Duration::from_secs(3600);
        config.link.batch_records = 1_000_000;
        config.query.backpressure.max_unapplied_records = 3;
        config.query.backpressure.refresh_interval = std::time::Duration::ZERO;
    })
    .await;
    Es::ok(
        es.put("/bp", Some(json!({"settings": {"number_of_shards": 1}})))
            .await,
    );
    for n in 0..3 {
        let a = es
            .put(&format!("/bp/_doc/{n}"), Some(json!({"n": n})))
            .await;
        assert_eq!(a.status.as_u16(), 201, "{}", a.text);
    }
    let a = ubq(
        &es,
        "/bp/_update_by_query?timeout=1s",
        json!({"query": {"match_all": {}}, "script": {"source": "ctx._source.m = params.m", "params": {"m": 1}}}),
    )
    .await;
    a.assert_error(429, "es_rejected_execution_exception", None);
    let secs: u64 = a.header("retry-after").parse().expect("Retry-After");
    assert!(secs >= 1);
    es.server.shutdown().await.expect("shutdown");
}
