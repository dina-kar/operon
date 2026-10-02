//! Task 3: index, mapping and alias administration and `_refresh`.
//!
//! Where the plan checks a write index, `write_index` asks
//! `names::resolve_write` and also writes through the alias with
//! `PUT /{alias}/_doc/{id}?require_alias=true` (rows T3-5, T4-9).

use std::sync::atomic::{AtomicU64, Ordering};

use loams_es::mapping::index_uuid;
use loams_es::names::resolve_write;
use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::harness::Es;

const NS: &str = "default";

/// The C5 LangChain dense mapping, 16 dims, cosine.
fn langchain_dense() -> Value {
    json!({
        "mappings": {"properties": {
            "vector": {"type": "dense_vector", "dims": 16, "index": true, "similarity": "cosine"},
        }},
        "settings": {},
    })
}

/// The C50 LLM-cache mapping.
fn llm_cache_mapping() -> Value {
    json!({"properties": {
        "llm_output": {"type": "text", "index": false},
        "llm_params": {"type": "text", "index": false},
        "llm_input": {"type": "text", "index": false},
        "metadata": {"type": "object"},
        "timestamp": {"type": "date"},
    }})
}

/// Where a write through `alias` goes: `Ok(index)`, or the error's reason.
/// It asks `names::resolve_write`, then writes a new document with
/// `PUT /{alias}/_doc/<n>?require_alias=true` and checks that the write
/// lands (or fails) the same way (C49).
async fn write_index(es: &Es, alias: &str) -> Result<String, String> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let resolved = match resolve_write(&es.server.collections(), NS, alias).await {
        Ok(Some(target)) => Ok(target.name),
        Ok(None) => Err("no such index or alias".to_string()),
        Err(err) => Err(err.reason),
    };
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let a = es
        .put(
            &format!("/{alias}/_doc/w{n}?require_alias=true&refresh=true"),
            Some(json!({"n": n})),
        )
        .await;
    let written = if a.status == StatusCode::CREATED {
        Ok(a.body["_index"].as_str().unwrap_or_default().to_string())
    } else {
        Err(a.body["error"]["reason"]
            .as_str()
            .unwrap_or_default()
            .to_string())
    };
    assert_eq!(written, resolved, "{}", a.text);
    resolved
}

/// The `es_env_fx` fixture of LangChain's `test_cache.py`.
async fn cache_fixture(es: &Es) {
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

#[tokio::test]
async fn create_index_with_langchain_dense_mapping() {
    let es = Es::start().await;
    let a = Es::ok(es.put("/test_a", Some(langchain_dense())).await);
    assert_eq!(
        a.body,
        json!({"acknowledged": true, "shards_acknowledged": true, "index": "test_a"})
    );
    let a = Es::ok(es.get("/test_a").await);
    let index = &a.body["test_a"];
    assert_eq!(
        index["mappings"]["properties"]["vector"],
        json!({"type": "dense_vector", "dims": 16, "index": true, "similarity": "cosine"})
    );
    let info = es
        .server
        .collections()
        .get_collection(NS, "test_a")
        .await
        .expect("collection");
    let settings = &index["settings"]["index"];
    assert_eq!(settings["number_of_shards"], info.partitions.to_string());
    assert_eq!(settings["uuid"], index_uuid(info.id));
    assert_eq!(settings["provided_name"], "test_a");
    assert_eq!(index["aliases"], json!({}));
    // number_of_shards is the partition count (BEIR, C47).
    Es::ok(
        es.put(
            "/beir-x",
            Some(
                json!({"settings": {"number_of_shards": 1}, "mappings": {"properties": {
                    "title": {"type": "text", "analyzer": "english"},
                }}}),
            ),
        )
        .await,
    );
    let a = Es::ok(es.get("/beir-x?flat_settings=true").await);
    assert_eq!(a.body["beir-x"]["settings"]["index.number_of_shards"], "1");
    // A bad mapping and a bad name are refused.
    let a = es
        .put(
            "/bad",
            Some(json!({"mappings": {"properties": {"x": {"type": "nested"}}}})),
        )
        .await;
    a.assert_error(400, "mapper_parsing_exception", None);
    let a = es.put("/Bad", None).await;
    a.assert_error(400, "invalid_index_name_exception", None);
    let a = es.put("/bad", Some(json!({"mapping": {}}))).await;
    a.assert_error(
        400,
        "parse_exception",
        Some("unknown key [mapping] for create index"),
    );
    let a = es.head("/bad").await;
    assert_eq!(a.status, StatusCode::NOT_FOUND);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn creating_an_existing_index_is_resource_already_exists() {
    let es = Es::start().await;
    Es::ok(es.put("/test_a", Some(langchain_dense())).await);
    let a = es.put("/test_a", Some(langchain_dense())).await;
    a.assert_error(400, "resource_already_exists_exception", None);
    let uuid = index_uuid(
        es.server
            .collections()
            .get_collection(NS, "test_a")
            .await
            .expect("collection")
            .id,
    );
    assert_eq!(a.body["error"]["index"], "test_a");
    assert_eq!(a.body["error"]["index_uuid"], uuid);
    assert_eq!(
        a.body["error"]["reason"],
        format!("index [test_a/{uuid}] already exists")
    );
    // An alias's name is not a free index name.
    Es::ok(es.put("/test_a/_alias/al", None).await);
    let a = es.put("/al", None).await;
    a.assert_error(
        400,
        "invalid_index_name_exception",
        Some("Invalid index name [al], already exists as alias"),
    );
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn head_index_is_200_or_404_without_body() {
    let es = Es::start().await;
    Es::ok(es.put("/test_a", None).await);
    let a = es.head("/test_a").await;
    assert_eq!(a.status, StatusCode::OK);
    assert!(a.text.is_empty(), "{}", a.text);
    assert_eq!(a.header("x-elastic-product"), "Elasticsearch");
    let a = es.head("/nope").await;
    assert_eq!(a.status, StatusCode::NOT_FOUND);
    assert!(a.text.is_empty(), "{}", a.text);
    assert_eq!(es.head("/test_*").await.status, StatusCode::OK);
    assert_eq!(es.head("/zz_*").await.status, StatusCode::NOT_FOUND);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn get_mapping_returns_the_langchain_metadata_mapping_verbatim() {
    let es = Es::start().await;
    let body = json!({"mappings": {"properties": {"metadata": {"properties": {
        "category": {"type": "keyword"},
        "score": {"type": "float"},
        "tags": {"type": "text"},
    }}}}});
    Es::ok(es.put("/i", Some(body)).await);
    // One document with a dynamic metadata.page.
    let source = json!({"metadata": {"page": 1, "category": "c"}});
    let a = es.put("/i/_doc/1", Some(source)).await;
    assert_eq!(a.status, StatusCode::CREATED, "{}", a.text);
    let a = Es::ok(es.get("/i/_mapping").await);
    let metadata = &a.body["i"]["mappings"]["properties"]["metadata"]["properties"];
    assert_eq!(metadata["category"], json!({"type": "keyword"}));
    assert_eq!(metadata["score"], json!({"type": "float"}));
    assert_eq!(metadata["tags"], json!({"type": "text"}));
    assert_eq!(metadata["page"], json!({"type": "long"}));
    let a = Es::ok(es.get("/_mapping").await);
    assert!(
        a.body["i"]["mappings"]["properties"].is_object(),
        "{}",
        a.text
    );
    let a = es.get("/nope/_mapping").await;
    a.assert_error(404, "index_not_found_exception", None);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn get_all_lists_every_index() {
    let es = Es::start().await;
    Es::ok(es.put("/test_1", None).await);
    Es::ok(es.put("/test_2", None).await);
    let a = Es::ok(es.get("/_all").await);
    let keys: Vec<&String> = a.body.as_object().expect("object").keys().collect();
    assert!(keys.contains(&&"test_1".to_string()) && keys.contains(&&"test_2".to_string()));
    Es::ok(es.delete("/test_1").await);
    let a = Es::ok(es.get("/_all").await);
    assert!(a.body.get("test_1").is_none(), "{}", a.text);
    assert!(a.body.get("test_2").is_some(), "{}", a.text);
    let a = Es::ok(es.get("/test_2?features=aliases").await);
    assert_eq!(a.body, json!({"test_2": {"aliases": {}}}));
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn wildcard_and_all_deletes_are_refused() {
    let es = Es::start().await;
    for name in ["test_1", "test_2", "other"] {
        Es::ok(es.put(&format!("/{name}"), None).await);
    }
    for path in ["/test_*", "/_all", "/*", "/test_1,other_*"] {
        let a = es.delete(path).await;
        a.assert_error(
            400,
            "illegal_argument_exception",
            Some("Wildcard expressions or all indices are not allowed"),
        );
    }
    for name in ["test_1", "test_2", "other"] {
        assert_eq!(es.head(&format!("/{name}")).await.status, StatusCode::OK);
    }
    // An alias is not deleted as an index.
    Es::ok(es.put("/other/_alias/al", None).await);
    let a = es.delete("/al").await;
    a.assert_error(
        400,
        "illegal_argument_exception",
        Some(
            "The provided expression [al] matches an alias, specify the corresponding concrete \
             indices instead.",
        ),
    );
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_comma_list_delete_deletes_every_index() {
    let es = Es::start().await;
    Es::ok(es.put("/test_1", None).await);
    Es::ok(es.put("/test_2", None).await);
    let a = Es::ok(es.delete("/test_1,test_2").await);
    assert_eq!(a.body, json!({"acknowledged": true}));
    assert_eq!(es.head("/test_1").await.status, StatusCode::NOT_FOUND);
    assert_eq!(es.head("/test_2").await.status, StatusCode::NOT_FOUND);
    Es::ok(es.put("/test_1", None).await);
    let a = es.delete("/test_1,nope").await;
    a.assert_error(
        404,
        "index_not_found_exception",
        Some("no such index [nope]"),
    );
    // ES resolves the whole list first: nothing is deleted (row T11-3).
    assert_eq!(es.head("/test_1").await.status, StatusCode::OK);
    Es::ok(es.delete("/test_1,nope?ignore_unavailable=true").await);
    assert_eq!(es.head("/test_1").await.status, StatusCode::NOT_FOUND);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn refresh_all_is_a_noop_success() {
    let es = Es::start().await;
    Es::ok(es.put("/a", None).await);
    Es::ok(es.put("/b", None).await);
    let a = Es::ok(es.post("/_all/_refresh", json!({})).await);
    assert_eq!(
        a.body,
        json!({"_shards": {"total": 2, "successful": 2, "failed": 0}})
    );
    let a = Es::ok(es.get("/a/_refresh").await);
    assert_eq!(a.body["_shards"]["total"], 1);
    Es::ok(es.send(reqwest::Method::POST, "/_refresh", None, &[]).await);
    let a = es.get("/nope/_refresh").await;
    a.assert_error(404, "index_not_found_exception", None);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn aliases_repoint_atomically() {
    let es = Es::start().await;
    Es::ok(es.put("/test_1", None).await);
    Es::ok(es.put("/test_2", None).await);
    Es::ok(es.put("/test_1/_alias/al", None).await);
    let a = Es::ok(
        es.post(
            "/_aliases",
            json!({"actions": [
                {"remove": {"index": "test_1", "alias": "al"}},
                {"add": {"index": "test_2", "alias": "al"}},
            ]}),
        )
        .await,
    );
    assert_eq!(a.body, json!({"acknowledged": true, "errors": false}));
    let a = Es::ok(es.get("/_alias/al").await);
    assert_eq!(a.body, json!({"test_2": {"aliases": {"al": {}}}}));
    assert_eq!(es.head("/_alias/al").await.status, StatusCode::OK);
    assert_eq!(es.head("/_alias/zz").await.status, StatusCode::NOT_FOUND);
    let a = es.get("/_alias/zz").await;
    assert_eq!(a.status, StatusCode::NOT_FOUND);
    assert_eq!(
        a.body,
        json!({"error": "alias [zz] missing", "status": 404})
    );
    // GET /_alias lists every index, with or without aliases.
    let a = Es::ok(es.get("/_alias").await);
    assert_eq!(
        a.body,
        json!({"test_1": {"aliases": {}}, "test_2": {"aliases": {"al": {}}}})
    );
    let a = Es::ok(es.get("/test_2/_alias/al").await);
    assert_eq!(a.body, json!({"test_2": {"aliases": {"al": {}}}}));
    // A failing action applies nothing.
    let a = es
        .post(
            "/_aliases",
            json!({"actions": [
                {"add": {"index": "test_1", "alias": "b"}},
                {"add": {"index": "missing", "alias": "b"}},
            ]}),
        )
        .await;
    a.assert_error(404, "index_not_found_exception", None);
    assert_eq!(es.head("/_alias/b").await.status, StatusCode::NOT_FOUND);
    let a = es
        .post(
            "/_aliases",
            json!({"actions": [{"add": {"index": "test_1", "alias": "b", "filter": {}}}]}),
        )
        .await;
    a.assert_error(400, "illegal_argument_exception", None);
    let a = es
        .post(
            "/_aliases",
            json!({"actions": [{"remove_index": {"index": "test_1"}}]}),
        )
        .await;
    a.assert_error(400, "illegal_argument_exception", None);
    let a = es
        .post(
            "/_aliases",
            json!({"actions": [{"add": {"index": "test_1", "alias": "test_2"}}]}),
        )
        .await;
    a.assert_error(
        400,
        "invalid_alias_name_exception",
        Some(
            "Invalid alias name [test_2]: an index or data stream exists with the same name as \
             the alias",
        ),
    );
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn an_alias_can_name_two_indices_with_one_write_index() {
    let es = Es::start().await;
    cache_fixture(&es).await;
    let a = Es::ok(es.get("/_alias/test_alias").await);
    assert_eq!(
        a.body,
        json!({
            "test_index1": {"aliases": {"test_alias": {}}},
            "test_index2": {"aliases": {"test_alias": {"is_write_index": true}}},
        })
    );
    assert_eq!(es.head("/_alias/test_alias").await.status, StatusCode::OK);
    let a = Es::ok(es.get("/test_alias").await);
    let keys: Vec<&String> = a.body.as_object().expect("object").keys().collect();
    assert_eq!(keys, ["test_index1", "test_index2"]);
    assert_eq!(
        a.body["test_index2"]["aliases"],
        json!({"test_alias": {"is_write_index": true}})
    );
    assert_eq!(
        write_index(&es, "test_alias").await,
        Ok("test_index2".to_string())
    );
    let a = Es::ok(
        es.delete("/test_index1,test_index2/_alias/test_alias")
            .await,
    );
    assert_eq!(a.body, json!({"acknowledged": true}));
    assert_eq!(
        es.head("/_alias/test_alias").await.status,
        StatusCode::NOT_FOUND
    );
    // Deleting it again is 404, which the fixture ignores.
    let a = es
        .delete("/test_index1,test_index2/_alias/test_alias")
        .await;
    a.assert_error(
        404,
        "aliases_not_found_exception",
        Some("aliases [test_alias] missing"),
    );
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn write_index_moves_like_elasticsearch() {
    let es = Es::start().await;
    cache_fixture(&es).await;
    let no_write_index = Err(
        "no write index is defined for alias [test_alias]. The write index may be explicitly \
         disabled using is_write_index=false or the alias points to multiple indices without \
         one being designated as a write index"
            .to_string(),
    );
    assert_eq!(
        write_index(&es, "test_alias").await,
        Ok("test_index2".to_string())
    );
    Es::ok(
        es.put(
            "/test_index2/_alias/test_alias",
            Some(json!({"is_write_index": false})),
        )
        .await,
    );
    let a = Es::ok(es.get("/_alias/test_alias").await);
    assert_eq!(
        a.body["test_index2"]["aliases"]["test_alias"],
        json!({"is_write_index": false})
    );
    assert_eq!(write_index(&es, "test_alias").await, no_write_index);
    Es::ok(
        es.put(
            "/test_index1/_alias/test_alias",
            Some(json!({"is_write_index": true})),
        )
        .await,
    );
    assert_eq!(
        write_index(&es, "test_alias").await,
        Ok("test_index1".to_string())
    );
    Es::ok(es.delete("/test_index2/_alias/test_alias").await);
    assert_eq!(
        write_index(&es, "test_alias").await,
        Ok("test_index1".to_string())
    );
    Es::ok(es.put("/test_index2/_alias/test_alias", None).await);
    let a = Es::ok(es.get("/_alias/test_alias").await);
    assert_eq!(
        a.body,
        json!({
            "test_index1": {"aliases": {"test_alias": {"is_write_index": true}}},
            "test_index2": {"aliases": {"test_alias": {}}},
        })
    );
    assert_eq!(
        write_index(&es, "test_alias").await,
        Ok("test_index1".to_string())
    );
    Es::ok(es.put("/test_index3", None).await);
    Es::ok(
        es.post(
            "/_aliases",
            json!({"actions": [{"add": {"index": "test_index3", "alias": "test_alias"}}]}),
        )
        .await,
    );
    let a = Es::ok(es.get("/_alias/test_alias").await);
    assert_eq!(a.body.as_object().expect("object").len(), 3, "{}", a.text);
    assert_eq!(
        write_index(&es, "test_alias").await,
        Ok("test_index1".to_string())
    );
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn two_write_indices_are_refused() {
    let es = Es::start().await;
    Es::ok(es.put("/test_index1", None).await);
    Es::ok(es.put("/test_index2", None).await);
    let a = es
        .post(
            "/_aliases",
            json!({"actions": [
                {"add": {"index": "test_index1", "alias": "test_alias", "is_write_index": true}},
                {"add": {"index": "test_index2", "alias": "test_alias", "is_write_index": true}},
            ]}),
        )
        .await;
    a.assert_error(
        500,
        "illegal_state_exception",
        Some("alias [test_alias] has more than one write index [test_index1,test_index2]"),
    );
    let a = es.get("/_alias/test_alias").await;
    assert_eq!(a.status, StatusCode::NOT_FOUND, "{}", a.text);
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn deleting_a_member_index_removes_it_from_the_alias() {
    let es = Es::start().await;
    cache_fixture(&es).await;
    Es::ok(es.delete("/test_index2").await);
    let a = Es::ok(es.get("/_alias/test_alias").await);
    assert_eq!(
        a.body,
        json!({"test_index1": {"aliases": {"test_alias": {}}}})
    );
    // The only member, unset, is the write index (Ruling 9).
    assert_eq!(
        write_index(&es, "test_alias").await,
        Ok("test_index1".to_string())
    );
    Es::ok(es.delete("/test_index1").await);
    assert_eq!(
        es.head("/_alias/test_alias").await.status,
        StatusCode::NOT_FOUND
    );
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn put_mapping_through_an_alias_updates_every_member() {
    let es = Es::start().await;
    cache_fixture(&es).await;
    let a = Es::ok(
        es.put("/test_alias/_mapping", Some(llm_cache_mapping()))
            .await,
    );
    assert_eq!(a.body, json!({"acknowledged": true}));
    for index in ["test_index1", "test_index2"] {
        let a = Es::ok(es.get(&format!("/{index}/_mapping")).await);
        assert_eq!(a.body[index]["mappings"], llm_cache_mapping(), "{index}");
    }
    // A mapping that conflicts in one member only changes neither.
    Es::ok(
        es.put(
            "/test_index2/_mapping",
            Some(json!({"properties": {"tag": {"type": "long"}}})),
        )
        .await,
    );
    let a = es
        .put(
            "/test_alias/_mapping",
            Some(json!({"properties": {"tag": {"type": "keyword"}, "extra": {"type": "keyword"}}})),
        )
        .await;
    a.assert_error(
        400,
        "illegal_argument_exception",
        Some("mapper [tag] cannot be changed from type [long] to [keyword]"),
    );
    for index in ["test_index1", "test_index2"] {
        let a = Es::ok(es.get(&format!("/{index}/_mapping")).await);
        assert!(
            a.body[index]["mappings"]["properties"]
                .get("extra")
                .is_none(),
            "{index}: {}",
            a.text
        );
    }
    // write_index_only updates the write index alone.
    Es::ok(
        es.put(
            "/test_alias/_mapping?write_index_only=true",
            Some(json!({"properties": {"only": {"type": "keyword"}}})),
        )
        .await,
    );
    let a = Es::ok(es.get("/test_index1,test_index2/_mapping").await);
    assert!(
        a.body["test_index1"]["mappings"]["properties"]
            .get("only")
            .is_none()
    );
    assert_eq!(
        a.body["test_index2"]["mappings"]["properties"]["only"],
        json!({"type": "keyword"})
    );
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn put_mapping_adds_a_field() {
    let es = Es::start().await;
    Es::ok(es.put("/i", None).await);
    Es::ok(
        es.put(
            "/i/_mapping",
            Some(json!({"properties": {"tag": {"type": "keyword"}}})),
        )
        .await,
    );
    let a = Es::ok(es.get("/i/_mapping").await);
    assert_eq!(
        a.body["i"]["mappings"]["properties"]["tag"],
        json!({"type": "keyword"})
    );
    let a = es
        .put(
            "/i/_mapping",
            Some(json!({"properties": {"tag": {"type": "long"}}})),
        )
        .await;
    a.assert_error(
        400,
        "illegal_argument_exception",
        Some("mapper [tag] cannot be changed from type [keyword] to [long]"),
    );
    // POST works too, and redeclaring is a no-op.
    Es::ok(
        es.post(
            "/i/_mapping",
            json!({"properties": {"tag": {"type": "keyword"}}}),
        )
        .await,
    );
    let a = es.put("/i/_mapping", None).await;
    a.assert_error(400, "parse_exception", Some("request body is required"));
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_default_pipeline_fails_every_write_item() {
    // The index side; the write side is Task 5's.
    let es = Es::start().await;
    Es::ok(
        es.put(
            "/i",
            Some(json!({"settings": {"index": {"default_pipeline": "not-existing-pipeline"}}})),
        )
        .await,
    );
    let a = Es::ok(es.get("/i").await);
    assert_eq!(
        a.body["i"]["settings"]["index"]["default_pipeline"],
        "not-existing-pipeline"
    );
    es.server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn create_with_aliases_adds_them() {
    let es = Es::start().await;
    Es::ok(
        es.put(
            "/i",
            Some(json!({"aliases": {"a1": {}, "a2": {"is_write_index": true}}})),
        )
        .await,
    );
    let a = Es::ok(es.get("/i/_alias").await);
    assert_eq!(
        a.body,
        json!({"i": {"aliases": {"a1": {}, "a2": {"is_write_index": true}}}})
    );
    let a = es.put("/j", Some(json!({"aliases": {"i": {}}}))).await;
    a.assert_error(400, "invalid_alias_name_exception", None);
    assert_eq!(es.head("/j").await.status, StatusCode::NOT_FOUND);
    let a = es
        .put("/j", Some(json!({"aliases": {"a3": {"routing": "1"}}})))
        .await;
    a.assert_error(400, "illegal_argument_exception", None);
    assert_eq!(es.head("/j").await.status, StatusCode::NOT_FOUND);
    es.server.shutdown().await.expect("shutdown");
}
