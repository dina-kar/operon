//! Task 4: document writes (`_doc`, `_create`, `_update`, `DELETE`) and the
//! write engine.
//!
//! The document read routes are Task 6's, so these tests read a stored
//! document through the collection service and put its vectors back with
//! `doc::restore_vectors`, as Task 6's `GET` will (row T4-8).

use operon_collection::PrimaryKey;
use operon_es::doc::restore_vectors;
use operon_query::{
    AnnParams, Projection, ReadConsistency, Retriever, SearchRequest, SourceFilter,
};
use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::harness::Es;

pub const NS: &str = "default";

/// The stored `_source` of `id` in `index`, vectors restored; `None` when
/// missing.
pub async fn stored(es: &Es, index: &str, id: &str) -> Option<Value> {
    let service = es.server.collections();
    let info = service.get_collection(NS, index).await.expect("index");
    let vectors = info.schema.vectors.iter().map(|v| v.name.clone()).collect();
    let select = Projection {
        source: SourceFilter::All,
        vectors,
        fields: Vec::new(),
    };
    let docs = service
        .get(
            NS,
            index,
            &[PrimaryKey::Str(id.to_string())],
            &select,
            ReadConsistency::Strong,
        )
        .await
        .expect("get");
    docs.into_iter().next().flatten().map(|doc| {
        let mut source = doc.source.unwrap_or_default();
        restore_vectors(&mut source, &doc.vectors);
        Value::Object(source)
    })
}

/// The live documents of `index`.
pub async fn count(es: &Es, index: &str) -> u64 {
    es.server
        .collections()
        .count(NS, index, None, ReadConsistency::Strong)
        .await
        .expect("count")
}

/// The `es_env_fx` fixture of LangChain's `test_cache.py`.
pub async fn cache_fixture(es: &Es) {
    Es::ok(es.put("/test_index1", None).await);
    Es::ok(es.put("/test_index2", None).await);
    Es::ok(es.put("/test_index1/_alias/test_alias", None).await);
    Es::ok(
        es.put(
            "/test_index2/_alias/test_alias",
            Some(json!({"is_write_index": true})),
        )
        .await,
    );
}

pub const NO_WRITE_INDEX: &str = "no write index is defined for alias [test_alias]. The write \
     index may be explicitly disabled using is_write_index=false or the alias points to multiple \
     indices without one being designated as a write index";

fn status(a: &crate::harness::Answer) -> u16 {
    a.status.as_u16()
}

#[tokio::test]
async fn index_into_a_missing_index_creates_it() {
    let es = Es::start().await;
    let a = es
        .put(
            "/test_r/_doc/1?refresh=true",
            Some(json!({"text": "foo bar", "another_field": 1})),
        )
        .await;
    assert_eq!(a.status, StatusCode::CREATED, "{}", a.text);
    assert_eq!(a.body["result"], "created");
    assert_eq!(a.body["_index"], "test_r");
    assert_eq!(a.body["_id"], "1");
    assert!(a.body["_version"].as_u64().expect("version") >= 1);
    assert_eq!(a.body["forced_refresh"], true);
    assert_eq!(a.body["_primary_term"], 1);
    assert_eq!(
        a.body["_shards"],
        json!({"total": 1, "successful": 1, "failed": 0})
    );
    assert!(a.header("operon-consistency-token").starts_with("v1:"));
    let m = Es::ok(es.get("/test_r/_mapping").await);
    let props = &m.body["test_r"]["mappings"]["properties"];
    assert_eq!(
        props["text"],
        json!({"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}})
    );
    assert_eq!(props["another_field"], json!({"type": "long"}));
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn reindexing_an_id_is_updated_with_a_larger_version() {
    let es = Es::start().await;
    let a = es.put("/i/_doc/1", Some(json!({"a": 1}))).await;
    assert_eq!(status(&a), 201, "{}", a.text);
    let b = es.put("/i/_doc/1", Some(json!({"a": 2}))).await;
    assert_eq!(status(&b), 200, "{}", b.text);
    assert_eq!(b.body["result"], "updated");
    assert!(b.body.get("forced_refresh").is_none());
    let (v1, v2) = (a.body["_version"].as_u64(), b.body["_version"].as_u64());
    let (s1, s2) = (a.body["_seq_no"].as_u64(), b.body["_seq_no"].as_u64());
    assert!(v2 > v1, "{v1:?} {v2:?}");
    assert!(s2 > s1, "{s1:?} {s2:?}");
    assert_eq!(v2, s2.map(|s| s + 1));
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn an_auto_id_is_a_monotonic_ulid() {
    let es = Es::start().await;
    let mut ids = Vec::new();
    for n in 0..50 {
        let a = es.post("/test_r/_doc?refresh=true", json!({"n": n})).await;
        assert_eq!(status(&a), 201, "{}", a.text);
        assert_eq!(a.body["result"], "created");
        let id = a.body["_id"].as_str().expect("id").to_string();
        assert_eq!(id.len(), 26, "{id}");
        assert!(
            id.chars()
                .all(|c| c.is_ascii_digit() || (c.is_ascii_uppercase() && !"ILOU".contains(c))),
            "{id}"
        );
        ids.push(id);
    }
    assert!(ids.windows(2).all(|w| w[0] < w[1]), "{ids:?}");
    assert_eq!(count(&es, "test_r").await, 50);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn create_of_an_existing_id_is_409() {
    let es = Es::start().await;
    let a = es.put("/i/_create/1", Some(json!({"w": "a"}))).await;
    assert_eq!(status(&a), 201, "{}", a.text);
    assert_eq!(a.body["result"], "created");
    let b = es.put("/i/_create/1", Some(json!({"w": "b"}))).await;
    b.assert_error(409, "version_conflict_engine_exception", None);
    let reason = b.body["error"]["reason"].as_str().expect("reason");
    assert!(
        reason.starts_with("[1]: version conflict, document already exists"),
        "{reason}"
    );
    assert_eq!(b.body["error"]["shard"], "0");
    assert_eq!(b.body["error"]["index"], "i");
    // op_type=create on _doc is the same.
    let c = es
        .put("/i/_doc/1?op_type=create", Some(json!({"w": "c"})))
        .await;
    c.assert_error(409, "version_conflict_engine_exception", None);
    assert_eq!(stored(&es, "i", "1").await, Some(json!({"w": "a"})));
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn two_concurrent_creates_leave_the_first_document() {
    let es = Es::start().await;
    Es::ok(es.put("/i", None).await);
    let (a, b) = tokio::join!(
        es.put("/i/_create/1", Some(json!({"w": "a"}))),
        es.put("/i/_create/1", Some(json!({"w": "b"}))),
    );
    let created: Vec<_> = [&a, &b].into_iter().filter(|x| status(x) == 201).collect();
    assert_eq!(created.len(), 1, "{} / {}", a.text, b.text);
    let loser = if status(&a) == 201 { &b } else { &a };
    loser.assert_error(409, "version_conflict_engine_exception", None);
    let winner = if status(&a) == 201 { "a" } else { "b" };
    assert_eq!(stored(&es, "i", "1").await, Some(json!({"w": winner})));
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn update_merges_objects_and_replaces_arrays() {
    let es = Es::start().await;
    let a = es
        .put(
            "/i/_doc/1",
            Some(json!({"a": {"x": 1, "y": 2}, "t": [1, 2]})),
        )
        .await;
    assert_eq!(status(&a), 201, "{}", a.text);
    let u = Es::ok(
        es.post(
            "/i/_update/1",
            json!({"doc": {"a": {"y": 3}, "t": [9], "n": null}}),
        )
        .await,
    );
    assert_eq!(u.body["result"], "updated");
    assert_eq!(
        stored(&es, "i", "1").await,
        Some(json!({"a": {"x": 1, "y": 3}, "t": [9], "n": null}))
    );
    // `_source` asks for the merged document back.
    let u = Es::ok(
        es.post("/i/_update/1?_source=a", json!({"doc": {"t": [8]}}))
            .await,
    );
    assert_eq!(u.body["get"]["found"], true);
    assert_eq!(u.body["get"]["_source"], json!({"a": {"x": 1, "y": 3}}));
    assert_eq!(u.body["get"]["_seq_no"], u.body["_seq_no"]);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn update_of_a_missing_document_is_404_unless_upserting() {
    let es = Es::start().await;
    Es::ok(es.put("/i", None).await);
    let a = es.post("/i/_update/zz", json!({"doc": {"a": 1}})).await;
    a.assert_error(
        404,
        "document_missing_exception",
        Some("[zz]: document missing"),
    );
    let a = es
        .post(
            "/i/_update/zz",
            json!({"doc": {"a": 1}, "doc_as_upsert": true}),
        )
        .await;
    assert_eq!(status(&a), 201, "{}", a.text);
    assert_eq!(a.body["result"], "created");
    assert_eq!(stored(&es, "i", "zz").await, Some(json!({"a": 1})));
    let a = es
        .post(
            "/i/_update/yy",
            json!({"doc": {"a": 1}, "upsert": {"a": 0}}),
        )
        .await;
    assert_eq!(status(&a), 201, "{}", a.text);
    assert_eq!(stored(&es, "i", "yy").await, Some(json!({"a": 0})));
    // A missing index without an upsert is 404 and creates nothing.
    let a = es.post("/nope/_update/1", json!({"doc": {"a": 1}})).await;
    a.assert_error(404, "index_not_found_exception", None);
    assert_eq!(es.head("/nope").await.status, StatusCode::NOT_FOUND);
    // Scripts are Phase A's refusal.
    let a = es
        .post(
            "/i/_update/zz",
            json!({"script": {"source": "ctx._source.a++"}}),
        )
        .await;
    a.assert_error(
        400,
        "illegal_argument_exception",
        Some("Operon does not support scripted updates (Elasticsearch API Phase A)"),
    );
    let a = es.post("/i/_update/zz", json!({})).await;
    a.assert_error(
        400,
        "action_request_validation_exception",
        Some("Validation Failed: 1: script or doc is missing;"),
    );
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn an_unchanged_update_is_noop() {
    let es = Es::start().await;
    let a = es.put("/i/_doc/1", Some(json!({"a": 1}))).await;
    assert_eq!(status(&a), 201, "{}", a.text);
    let u = Es::ok(
        es.post("/i/_update/1?refresh=true", json!({"doc": {"a": 1}}))
            .await,
    );
    assert_eq!(u.body["result"], "noop");
    assert_eq!(u.body["_shards"]["total"], 0);
    assert_eq!(u.body["_version"], a.body["_version"]);
    assert_eq!(u.body["_seq_no"], a.body["_seq_no"]);
    let u = Es::ok(
        es.post(
            "/i/_update/1",
            json!({"doc": {"a": 1}, "detect_noop": false}),
        )
        .await,
    );
    assert_eq!(u.body["result"], "updated");
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn delete_is_deleted_then_not_found() {
    let es = Es::start().await;
    let a = es.put("/i/_doc/1", Some(json!({"a": 1}))).await;
    assert_eq!(status(&a), 201, "{}", a.text);
    let d = Es::ok(es.delete("/i/_doc/1?refresh=true").await);
    assert_eq!(d.body["result"], "deleted");
    assert_eq!(d.body["forced_refresh"], true);
    let d = es.delete("/i/_doc/1").await;
    assert_eq!(d.status, StatusCode::NOT_FOUND, "{}", d.text);
    assert_eq!(d.body["result"], "not_found");
    assert_eq!(stored(&es, "i", "1").await, None);
    let d = es.delete("/nope/_doc/1").await;
    d.assert_error(404, "index_not_found_exception", None);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn an_index_update_delete_sequence_on_one_id_answers_like_es() {
    use operon_es::write::{WriteCall, WriteItem, execute};
    let es = Es::start().await;
    Es::ok(es.put("/i", None).await);
    let gateway =
        operon_es::EsGateway::new(es.server.collections(), operon_es::EsConfig::default());
    let ctx = operon_es::RequestCtx::fallback();
    let ctx = operon_es::RequestCtx {
        namespace: NS.to_string(),
        ..ctx
    };
    let call = WriteCall {
        gateway: &gateway,
        ctx: &ctx,
        index: "i",
        require_alias: false,
        pipeline: None,
        refresh: false,
        source_on_update: None,
    };
    let id = || "a".to_string();
    let items = vec![
        WriteItem::Index {
            id: Some(id()),
            source: json!({"x": 1}),
            create: false,
        },
        WriteItem::Update {
            id: id(),
            body: json!({"doc": {"x": 2}}),
        },
        WriteItem::Index {
            id: Some(id()),
            source: json!({"x": 3}),
            create: true,
        },
        WriteItem::Delete { id: id() },
        WriteItem::Update {
            id: id(),
            body: json!({"doc": {"x": 4}}),
        },
    ];
    let (outcomes, token) = execute(call, items).await;
    assert!(token.is_some());
    let statuses: Vec<u16> = outcomes.iter().map(|o| o.status).collect();
    assert_eq!(statuses, [201, 200, 409, 200, 404], "{outcomes:?}");
    assert_eq!(outcomes[0].body["result"], "created");
    assert_eq!(outcomes[1].body["result"], "updated");
    assert_eq!(
        outcomes[2].body["error"]["type"],
        "version_conflict_engine_exception"
    );
    assert_eq!(outcomes[3].body["result"], "deleted");
    assert_eq!(
        outcomes[4].body["error"]["type"],
        "document_missing_exception"
    );
    // The conflict names the version the update gave the document.
    let version = outcomes[1].body["_version"].as_u64().expect("version");
    let reason = outcomes[2].body["error"]["reason"]
        .as_str()
        .expect("reason");
    assert!(
        reason.ends_with(&format!("(current version [{version}])")),
        "{reason}"
    );
    assert_eq!(stored(&es, "i", "a").await, None);
    es.server.shutdown().await.expect("shutdown");
}

/// A dims-3 cosine index.
async fn vector_index(es: &Es) {
    let body = json!({"mappings": {"properties": {
        "vector": {"type": "dense_vector", "dims": 3, "index": true, "similarity": "cosine"},
    }}});
    Es::ok(es.put("/v", Some(body)).await);
}

#[tokio::test]
async fn a_vector_in_source_is_stored_once_and_restored() {
    let es = Es::start().await;
    vector_index(&es).await;
    let a = es
        .put(
            "/v/_doc/1",
            Some(json!({"text": "x", "vector": [0.1, 0.2, 0.3]})),
        )
        .await;
    assert_eq!(status(&a), 201, "{}", a.text);
    let source = stored(&es, "v", "1").await.expect("stored");
    assert_eq!(source.to_string(), r#"{"text":"x","vector":[0.1,0.2,0.3]}"#);
    let docs = es
        .server
        .collections()
        .get(
            NS,
            "v",
            &[PrimaryKey::Str("1".to_string())],
            &Projection {
                source: SourceFilter::All,
                vectors: vec!["vector".to_string()],
                fields: Vec::new(),
            },
            ReadConsistency::Strong,
        )
        .await
        .expect("get");
    let doc = docs[0].clone().expect("doc");
    assert_eq!(doc.vectors["vector"], vec![0.1f32, 0.2, 0.3]);
    assert!(!doc.source.expect("source").contains_key("vector"));
    // Wrong dimensions, zero magnitude and a non-array are refused.
    let a = es
        .put("/v/_doc/2", Some(json!({"vector": [1.0, 2.0]})))
        .await;
    a.assert_error(
        400,
        "document_parsing_exception",
        Some("The number of dimensions for field [vector] should be [3] but found [2]"),
    );
    let a = es
        .put("/v/_doc/2", Some(json!({"vector": [0, 0, 0]})))
        .await;
    a.assert_error(
        400,
        "document_parsing_exception",
        Some("The [cosine] similarity does not support vectors with zero magnitude."),
    );
    let a = es.put("/v/_doc/2", Some(json!({"vector": "abc"}))).await;
    a.assert_error(400, "document_parsing_exception", None);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn dot_product_needs_unit_vectors() {
    let es = Es::start().await;
    let body = json!({"mappings": {"properties": {
        "vector": {"type": "dense_vector", "dims": 2, "similarity": "dot_product"},
    }}});
    Es::ok(es.put("/d", Some(body)).await);
    let a = es
        .put("/d/_doc/1", Some(json!({"vector": [0.6, 0.8]})))
        .await;
    assert_eq!(status(&a), 201, "{}", a.text);
    let a = es
        .put("/d/_doc/2", Some(json!({"vector": [1.0, 1.0]})))
        .await;
    a.assert_error(
        400,
        "document_parsing_exception",
        Some("The [dot_product] similarity can only be used with unit-length vectors."),
    );
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_vector_declared_without_dims_takes_them_from_the_first_document() {
    let es = Es::start().await;
    let body = json!({"mappings": {"properties": {
        "emb": {"type": "dense_vector", "similarity": "l2_norm"},
    }}});
    Es::ok(es.put("/p", Some(body)).await);
    let a = es
        .put("/p/_doc/1", Some(json!({"emb": [1.0, 2.0, 3.0, 4.0]})))
        .await;
    assert_eq!(status(&a), 201, "{}", a.text);
    let info = es
        .server
        .collections()
        .get_collection(NS, "p")
        .await
        .expect("info");
    let (_, spec) = info.schema.vector("emb").expect("vector");
    assert_eq!(spec.dim, 4);
    let m = Es::ok(es.get("/p/_mapping").await);
    assert_eq!(
        m.body["p"]["mappings"]["properties"]["emb"],
        json!({"type": "dense_vector", "dims": 4, "similarity": "l2_norm"})
    );
    assert_eq!(
        stored(&es, "p", "1").await,
        Some(json!({"emb": [1.0, 2.0, 3.0, 4.0]}))
    );
    let a = es.put("/p/_doc/2", Some(json!({"emb": [1.0, 2.0]}))).await;
    a.assert_error(
        400,
        "document_parsing_exception",
        Some("The number of dimensions for field [emb] should be [4] but found [2]"),
    );
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_partial_update_replaces_and_removes_a_vector() {
    let es = Es::start().await;
    vector_index(&es).await;
    let a = es
        .put(
            "/v/_doc/1",
            Some(json!({"text": "x", "vector": [0.1, 0.2, 0.3]})),
        )
        .await;
    assert_eq!(status(&a), 201, "{}", a.text);
    Es::ok(
        es.post("/v/_update/1", json!({"doc": {"vector": [0.3, 0.2, 0.1]}}))
            .await,
    );
    assert_eq!(
        stored(&es, "v", "1").await,
        Some(json!({"text": "x", "vector": [0.3, 0.2, 0.1]}))
    );
    let knn = || {
        let mut request = SearchRequest::new("v");
        request.retrievers = vec![Retriever::Vector {
            field: "vector".to_string(),
            query: vec![0.3, 0.2, 0.1],
            k: 10,
            params: AnnParams::default(),
            filter: None,
        }];
        request
    };
    let hits = es
        .server
        .collections()
        .search(NS, knn())
        .await
        .expect("search")
        .hits;
    assert_eq!(hits.len(), 1);
    Es::ok(
        es.post("/v/_update/1", json!({"doc": {"vector": null}}))
            .await,
    );
    assert_eq!(
        stored(&es, "v", "1").await,
        Some(json!({"text": "x", "vector": null}))
    );
    let hits = es
        .server
        .collections()
        .search(NS, knn())
        .await
        .expect("search")
        .hits;
    assert!(hits.is_empty(), "{hits:?}");
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn concurrent_writers_agree_on_a_new_dynamic_field() {
    let es = Es::start().await;
    Es::ok(es.put("/i", None).await);
    let writes = (0..8).map(|n| {
        let es = &es;
        async move {
            es.put(
                &format!("/i/_doc/{n}"),
                Some(json!({"newf": format!("x{n}")})),
            )
            .await
        }
    });
    for a in futures::future::join_all(writes).await {
        assert_eq!(status(&a), 201, "{}", a.text);
    }
    let m = Es::ok(es.get("/i/_mapping").await);
    let props = m.body["i"]["mappings"]["properties"]
        .as_object()
        .cloned()
        .expect("properties");
    assert_eq!(props.len(), 1, "{props:?}");
    assert_eq!(
        props["newf"],
        json!({"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}})
    );
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_conflicting_dynamic_type_fails_per_item() {
    let es = Es::start().await;
    let a = es.put("/i/_doc/1", Some(json!({"newn": 5}))).await;
    assert_eq!(status(&a), 201, "{}", a.text);
    let a = es.put("/i/_doc/2", Some(json!({"newn": "abc"}))).await;
    a.assert_error(400, "document_parsing_exception", None);
    let reason = a.body["error"]["reason"].as_str().expect("reason");
    assert!(
        reason.contains("failed to parse field [newn] of type [long]"),
        "{reason}"
    );
    assert!(
        reason.contains("Preview of field's value: 'abc'"),
        "{reason}"
    );
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn strict_mapping_rejects_an_unmapped_field() {
    let es = Es::start().await;
    let body = json!({"mappings": {"dynamic": "strict", "properties": {"a": {"type": "long"}}}});
    Es::ok(es.put("/s", Some(body)).await);
    let a = es.put("/s/_doc/1", Some(json!({"x": 1}))).await;
    a.assert_error(
        400,
        "strict_dynamic_mapping_exception",
        Some("[dynamic] set to [strict], dynamic introduction of [x] within [_doc] is not allowed"),
    );
    let a = es.put("/s/_doc/1", Some(json!({"a": 1}))).await;
    assert_eq!(status(&a), 201, "{}", a.text);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn the_field_limit_rejects_a_document() {
    let es = Es::start().await;
    let body = json!({"settings": {"index.mapping.total_fields.limit": 3}});
    Es::ok(es.put("/l", Some(body)).await);
    let a = es
        .put("/l/_doc/1", Some(json!({"a": 1, "b": 2, "c": 3, "d": 4})))
        .await;
    assert_eq!(status(&a), 400, "{}", a.text);
    let reason = a.body["error"]["reason"].as_str().expect("reason");
    assert!(
        reason.contains("Limit of total fields [3] has been exceeded"),
        "{reason}"
    );
    assert_eq!(count(&es, "l").await, 0);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn occ_parameters_are_phase_b() {
    let es = Es::start().await;
    let reason = "optimistic concurrency control (if_seq_no, if_primary_term, version) is not \
                  supported by Operon (Phase B)";
    for path in [
        "/i/_doc/1?if_seq_no=1&if_primary_term=1",
        "/i/_doc/1?version=3",
        "/i/_create/1?version_type=external",
    ] {
        let a = es.put(path, Some(json!({"a": 1}))).await;
        a.assert_error(400, "illegal_argument_exception", Some(reason));
    }
    let a = es
        .post("/i/_update/1?if_seq_no=1", json!({"doc": {}}))
        .await;
    a.assert_error(400, "illegal_argument_exception", Some(reason));
    let a = es.delete("/i/_doc/1?if_primary_term=1").await;
    a.assert_error(400, "illegal_argument_exception", Some(reason));
    // Nothing was created.
    assert_eq!(es.head("/i").await.status, StatusCode::NOT_FOUND);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn metadata_fields_in_source_are_refused() {
    let es = Es::start().await;
    let a = es.put("/i/_doc/1", Some(json!({"_id": "x"}))).await;
    a.assert_error(400, "document_parsing_exception", None);
    assert!(
        a.body["error"]["caused_by"]["reason"]
            .as_str()
            .expect("cause")
            .contains("Field [_id] is a metadata field and cannot be added inside a document"),
        "{}",
        a.text
    );
    let a = es
        .put("/i/_doc/1", Some(json!(["not", "an", "object"])))
        .await;
    a.assert_error(
        400,
        "document_parsing_exception",
        Some("[1:1] failed to parse: source is not an object"),
    );
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn ids_are_validated() {
    let es = Es::start().await;
    let long = "a".repeat(513);
    let a = es
        .put(&format!("/i/_doc/{long}"), Some(json!({"a": 1})))
        .await;
    a.assert_error(400, "illegal_argument_exception", None);
    assert!(a.text.contains("is too long"), "{}", a.text);
    let a = es.put("/i/_doc/a%2Fb", Some(json!({"a": 1}))).await;
    assert_eq!(status(&a), 201, "{}", a.text);
    assert_eq!(a.body["_id"], "a/b");
    assert_eq!(stored(&es, "i", "a/b").await, Some(json!({"a": 1})));
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_binary_value_must_be_base64() {
    let es = Es::start().await;
    let body = json!({"mappings": {"properties": {
        "text_input": {"type": "text", "index": false},
        "vector_dump": {"type": "binary", "doc_values": false},
        "metadata": {"type": "object"},
        "timestamp": {"type": "date"},
        "namespace": {"type": "keyword"},
    }}});
    Es::ok(es.put("/e", Some(body)).await);
    let a = es
        .put("/e/_doc/1", Some(json!({"vector_dump": "AAAAAA=="})))
        .await;
    assert_eq!(status(&a), 201, "{}", a.text);
    assert_eq!(
        stored(&es, "e", "1").await,
        Some(json!({"vector_dump": "AAAAAA=="}))
    );
    let a = es
        .put("/e/_doc/2", Some(json!({"vector_dump": "not base64!"})))
        .await;
    a.assert_error(400, "document_parsing_exception", None);
    let reason = a.body["error"]["reason"].as_str().expect("reason");
    assert!(
        reason.contains("failed to parse field [vector_dump] of type [binary]"),
        "{reason}"
    );
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_default_pipeline_fails_index_and_update_but_not_delete() {
    let es = Es::start().await;
    let body = json!({"settings": {"index": {"default_pipeline": "not-existing-pipeline"}}});
    Es::ok(es.put("/i", Some(body)).await);
    let reason = "pipeline with id [not-existing-pipeline] does not exist";
    let a = es.put("/i/_doc/1", Some(json!({"a": 1}))).await;
    a.assert_error(400, "illegal_argument_exception", Some(reason));
    let a = es
        .post(
            "/i/_update/1",
            json!({"doc": {"a": 1}, "doc_as_upsert": true}),
        )
        .await;
    a.assert_error(400, "illegal_argument_exception", Some(reason));
    // `pipeline=_none` turns the default off.
    let a = es
        .put("/i/_doc/1?pipeline=_none", Some(json!({"a": 1})))
        .await;
    assert_eq!(status(&a), 201, "{}", a.text);
    let a = es.delete("/i/_doc/1").await;
    assert_eq!(status(&a), 200, "{}", a.text);
    let a = es.put("/j/_doc/1?pipeline=p", Some(json!({"a": 1}))).await;
    a.assert_error(
        400,
        "illegal_argument_exception",
        Some("pipeline with id [p] does not exist"),
    );
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn writes_through_an_alias_go_to_its_write_index() {
    let es = Es::start().await;
    cache_fixture(&es).await;
    let a = es
        .put(
            "/test_alias/_doc/k?require_alias=true&refresh=true",
            Some(json!({"v": 1})),
        )
        .await;
    assert_eq!(status(&a), 201, "{}", a.text);
    assert_eq!(a.body["_index"], "test_index2");
    assert_eq!(count(&es, "test_index2").await, 1);
    assert_eq!(count(&es, "test_index1").await, 0);
    // Move the write index to test_index1.
    Es::ok(
        es.put(
            "/test_index2/_alias/test_alias",
            Some(json!({"is_write_index": false})),
        )
        .await,
    );
    Es::ok(
        es.put(
            "/test_index1/_alias/test_alias",
            Some(json!({"is_write_index": true})),
        )
        .await,
    );
    let a = es
        .put(
            "/test_alias/_doc/j?require_alias=true",
            Some(json!({"v": 2})),
        )
        .await;
    assert_eq!(status(&a), 201, "{}", a.text);
    assert_eq!(a.body["_index"], "test_index1");
    // `k` lives only in test_index2: an update and a delete through the
    // alias act on test_index1.
    let a = es
        .post("/test_alias/_update/k", json!({"doc": {"v": 3}}))
        .await;
    a.assert_error(404, "document_missing_exception", None);
    assert_eq!(a.body["error"]["index"], "test_index1");
    let a = es.delete("/test_alias/_doc/k").await;
    assert_eq!(a.status, StatusCode::NOT_FOUND, "{}", a.text);
    assert_eq!(a.body["result"], "not_found");
    assert_eq!(a.body["_index"], "test_index1");
    let a = Es::ok(
        es.post("/test_alias/_update/j", json!({"doc": {"v": 4}}))
            .await,
    );
    assert_eq!(a.body["_index"], "test_index1");
    assert_eq!(stored(&es, "test_index1", "j").await, Some(json!({"v": 4})));
    assert_eq!(stored(&es, "test_index2", "k").await, Some(json!({"v": 1})));
    // require_alias on a concrete index or a missing name is 404.
    let a = es
        .put("/test_index1/_doc/x?require_alias=true", Some(json!({})))
        .await;
    a.assert_error(
        404,
        "index_not_found_exception",
        Some(
            "no such index [test_index1] and [require_alias] request flag is [true] and \
             [test_index1] is not an alias",
        ),
    );
    let a = es
        .put("/fresh/_doc/x?require_alias=true", Some(json!({})))
        .await;
    a.assert_error(404, "index_not_found_exception", None);
    assert_eq!(es.head("/fresh").await.status, StatusCode::NOT_FOUND);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_write_through_an_alias_without_a_write_index_is_refused() {
    let es = Es::start().await;
    Es::ok(es.put("/test_index1", None).await);
    Es::ok(es.put("/test_index2", None).await);
    Es::ok(es.put("/test_index1/_alias/test_alias", None).await);
    Es::ok(es.put("/test_index2/_alias/test_alias", None).await);
    let a = es.put("/test_alias/_doc/1", Some(json!({"a": 1}))).await;
    a.assert_error(400, "illegal_argument_exception", Some(NO_WRITE_INDEX));
    // The only member, set to false.
    Es::ok(es.delete("/test_index2/_alias/test_alias").await);
    Es::ok(
        es.put(
            "/test_index1/_alias/test_alias",
            Some(json!({"is_write_index": false})),
        )
        .await,
    );
    let a = es.put("/test_alias/_doc/1", Some(json!({"a": 1}))).await;
    a.assert_error(400, "illegal_argument_exception", Some(NO_WRITE_INDEX));
    assert_eq!(count(&es, "test_index1").await, 0);
    assert_eq!(count(&es, "test_index2").await, 0);
    assert_eq!(es.head("/test_alias").await.status, StatusCode::OK);
    let a = Es::ok(es.get("/_all").await);
    assert_eq!(a.body.as_object().expect("indices").len(), 2, "{}", a.text);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn write_backpressure_is_429_with_retry_after() {
    // A link that never commits within the test, and a budget of three
    // unapplied records (M1.3 Task 15; the M1.4 gateway's pattern).
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
        assert_eq!(status(&a), 201, "{}", a.text);
    }
    let a = es.put("/bp/_doc/9", Some(json!({"n": 9}))).await;
    a.assert_error(429, "es_rejected_execution_exception", None);
    let secs: u64 = a.header("retry-after").parse().expect("Retry-After");
    assert!(secs >= 1);
    es.server.shutdown().await.expect("shutdown");
}
