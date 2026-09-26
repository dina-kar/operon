//! Search over the native API (plan M1.2 Task 11), with the M1.6 fixture.

use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::common::{Native, TOKEN, kb};

fn pks(body: &Value) -> Vec<Value> {
    body["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|hit| hit["pk"].clone())
        .collect()
}

/// M1.6 fixture step 12.
fn hybrid_request() -> Value {
    json!({
        "collection": "kb",
        "retrievers": [
            {"vector": {"field": "embedding", "query": [1.0, 0.0, 0.0], "k": 10,
                        "params": {"exact": false, "nprobes": null, "refine_factor": null, "ef": null, "oversampling": null, "distance": null},
                        "filter": null}},
            {"text": {"query": {"match": {"field": "body", "text": "refund", "operator": "or", "minimum_should_match": null, "fuzziness": null, "analyzer": null}}, "k": 10}}
        ],
        "fusion": {"rrf": {"k": 60}},
        "limit": 3
    })
}

#[tokio::test]
async fn hybrid_search_over_http_returns_the_fixture_ranking() {
    let api = Native::start().await;
    kb(&api, "w").await;
    let reply = api.post("/v1/namespaces/w/query", hybrid_request()).await;
    let header = reply.header(TOKEN).expect("token header").to_string();
    let body = reply.expect(StatusCode::OK);
    assert_eq!(pks(&body), [json!(1), json!(3), json!(2)], "{body}");
    assert_eq!(body["read_token"], header.as_str());
    let scores: Vec<f64> = body["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|hit| hit["score"].as_f64().unwrap())
        .collect();
    assert!(scores.windows(2).all(|w| w[0] >= w[1]), "{scores:?}");
    api.shutdown().await;
}

#[tokio::test]
async fn a_filter_only_query_over_http() {
    let api = Native::start().await;
    kb(&api, "w").await;
    // Fixture step 13.
    let body = api
        .post(
            "/v1/namespaces/w/query",
            json!({"collection": "kb", "retrievers": [], "fusion": null,
                   "filter": {"term": {"field": "tenant", "value": "b"}}, "limit": 10}),
        )
        .await
        .expect(StatusCode::OK);
    assert_eq!(pks(&body), [json!(3)], "{body}");
    assert!(body["hits"].get(1).is_none());
    api.shutdown().await;
}

#[tokio::test]
async fn sparse_and_hybrid_queries_over_http() {
    let api = Native::start().await;
    // Steps 22–23.
    let body = api
        .post(
            "/v1/namespaces/w/collections",
            json!({"name": "sp", "schema": {"fields": [], "vectors": [{"name": "e", "dim": 2, "distance": "cosine"}],
                   "sparse_vectors": [{"name": "s", "modifier": "none"}], "dynamic": "ignore", "max_fields": 1000}}),
        )
        .await
        .expect(StatusCode::CREATED);
    assert_eq!(body["schema"]["sparse_vectors"][0]["name"], "s");
    let upsert = |id: u64, e: Value, s: Value| json!({"upsert": {"id": id, "source": {}, "vectors": {"e": e}, "sparse_vectors": {"s": s}}});
    let reply = api
        .post(
            "/v1/namespaces/w/collections/sp/documents",
            json!({"ops": [
                upsert(1, json!([1.0, 0.0]), json!({"indices": [5, 1], "values": [2.0, 1.0]})),
                upsert(2, json!([0.0, 1.0]), json!({"indices": [5], "values": [0.5]})),
                upsert(3, json!([0.8, 0.6]), json!({"indices": [7], "values": [3.0]})),
            ]}),
        )
        .await;
    let token = reply.header(TOKEN).expect("token header").to_string();
    reply.expect(StatusCode::OK);

    // Step 24: sparse only.
    let sparse = json!({"sparse": {"field": "s", "query": {"indices": [5], "values": [1.0]}, "k": 10,
                                   "filter": null, "params": {"idf_corpus": null}}});
    let body = api
        .post(
            "/v1/namespaces/w/query",
            json!({"collection": "sp", "retrievers": [sparse.clone()], "limit": 10}),
        )
        .await
        .expect(StatusCode::OK);
    assert_eq!(pks(&body), [json!(1), json!(2)], "{body}");
    assert_eq!(body["hits"][0]["score"], 2.0);
    assert_eq!(body["hits"][1]["score"], 0.5);
    assert!(body["hits"].get(2).is_none());

    // Step 25: dense + sparse, RRF.
    let body = api
        .post(
            "/v1/namespaces/w/query",
            json!({"collection": "sp", "retrievers": [
                sparse,
                {"vector": {"field": "e", "query": [1.0, 0.0], "k": 10}}
            ], "fusion": {"rrf": {"k": 60}}, "limit": 3}),
        )
        .await
        .expect(StatusCode::OK);
    assert_eq!(pks(&body), [json!(1), json!(2), json!(3)], "{body}");

    // Step 26: sparse vectors in documents/get, sorted by index.
    let body = api
        .post_with(
            "/v1/namespaces/w/collections/sp/documents/get",
            &[(TOKEN, &token)],
            json!({"ids": [1], "select": {"source": "none", "vectors": ["s"], "fields": []}}),
        )
        .await
        .expect(StatusCode::OK);
    let doc = &body["documents"][0];
    assert_eq!(
        doc["sparse_vectors"]["s"]["indices"],
        json!([1, 5]),
        "{doc}"
    );
    assert_eq!(doc["sparse_vectors"]["s"]["values"], json!([1.0, 2.0]));
    api.shutdown().await;
}

#[tokio::test]
async fn the_section_05_body_is_accepted() {
    let api = Native::start().await;
    kb(&api, "w").await;
    let hybrid = json!({
        "from": "collections.kb",
        "consistency": "strong",
        "retrieve": [
            {"vector": {"field": "embedding", "query": [1.0, 0.0, 0.0], "k": 10}},
            {"text": {"field": "body", "query": "refund", "k": 10}}
        ],
        "fuse": {"method": "rrf", "k": 60},
        "select": ["id", "_score", "body"],
        "limit": 3
    });
    let body = api
        .post("/v1/namespaces/w/query", hybrid.clone())
        .await
        .expect(StatusCode::OK);
    assert_eq!(pks(&body), [json!(1), json!(3), json!(2)], "{body}");
    assert_eq!(body["hits"][0]["source"], json!({"body": "refund policy"}));

    let mut rerank = hybrid;
    rerank["rerank"] = json!({"model": "x"});
    let body = api
        .post("/v1/namespaces/w/query", rerank)
        .await
        .expect(StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_argument");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("rerank arrives in M3"),
        "{body}"
    );
    api.shutdown().await;
}

#[tokio::test]
async fn operon_hot_used_is_none_without_a_hot_tier_and_off_is_honoured() {
    let api = Native::start().await;
    kb(&api, "w").await;
    for headers in [
        &[][..],
        &[("Operon-Hot", "off")][..],
        &[("Operon-Hot", "ON")][..],
    ] {
        let reply = api
            .post_with("/v1/namespaces/w/query", headers, hybrid_request())
            .await;
        assert_eq!(reply.header("operon-hot-used"), Some("none"), "{headers:?}");
        let body = reply.expect(StatusCode::OK);
        // No hot structure served the read, with or without the switch.
        assert!(body.get("hot_used").is_none(), "{body}");
        assert_eq!(pks(&body), [json!(1), json!(3), json!(2)]);
    }
    // Every route is inside the hot layer.
    let reply = api.get("/v1/namespaces/w/collections/kb").await;
    assert_eq!(reply.header("operon-hot-used"), Some("none"));
    api.shutdown().await;
}

#[tokio::test]
async fn an_invalid_operon_hot_header_is_400() {
    let api = Native::start().await;
    kb(&api, "w").await;
    let body = api
        .post_with(
            "/v1/namespaces/w/query",
            &[("Operon-Hot", "maybe")],
            hybrid_request(),
        )
        .await
        .expect(StatusCode::BAD_REQUEST);
    assert_eq!(
        body,
        json!({"error": "invalid_argument", "message": "invalid Operon-Hot header: maybe (expected on or off)"})
    );
    api.shutdown().await;
}
