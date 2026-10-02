//! `loams dev` on the TiKV metastore (R1 plan Task 6): every role runs on
//! `--meta tikv://…`, and a restart on the same keyspace and root keeps the
//! state. Each test uses a random root in the test keyspace and skips
//! without `LOAMS_TEST_PD`.
#![cfg(feature = "tikv")]

#[path = "it/common/mod.rs"]
mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::{Native, kb, kb_docs, kb_schema};
use loams::{MetaBackend, Server, ServerConfig};
use loams_tikv::testing::{self, TEST_META, TestCluster};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tempfile::TempDir;

/// `tikv://<pd>/loams_test_meta?root=<hex>` for `root`.
fn meta_url(cluster: &TestCluster, root: &[u8]) -> String {
    let hex: String = root.iter().map(|b| format!("{b:02x}")).collect();
    format!("tikv://{}/{TEST_META}?root={hex}", cluster.pd.join(","))
}

fn config(dir: &TempDir, meta: MetaBackend) -> ServerConfig {
    let mut config = ServerConfig::new(dir.path());
    config.listen = SocketAddr::from(([127, 0, 0, 1], 0));
    config.log.flush_interval = Duration::from_millis(20);
    config.worker_poll_interval = Duration::from_millis(50);
    config.link.batch_interval = Duration::ZERO;
    config.meta = meta;
    config
}

fn pks(body: &Value) -> Vec<Value> {
    body["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|hit| hit["pk"].clone())
        .collect()
}

/// A strong hybrid query over the M1.6 fixture's `kb`.
fn strong_query() -> Value {
    json!({
        "from": "collections.kb",
        "consistency": "strong",
        "retrieve": [
            {"vector": {"field": "embedding", "query": [1.0, 0.0, 0.0], "k": 10}},
            {"text": {"field": "body", "query": "refund", "k": 10}}
        ],
        "fuse": {"method": "rrf", "k": 60},
        "select": ["id", "_score", "body"],
        "limit": 3
    })
}

/// Create a collection, write the fixture's documents and read them back
/// with a strong query, on the TiKV metastore; the metastore's invariants
/// hold afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dev_on_tikv_ingests_and_searches() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let root = testing::random_root();
    let meta = MetaBackend::parse(&meta_url(&cluster, &root)).expect("url");
    let api = Native::start_with(|config| config.meta = meta).await;
    assert!(api.server.meta().is_none(), "no openraft client on TiKV");
    kb(&api, "w").await;
    let body = api
        .post("/v1/namespaces/w/query", strong_query())
        .await
        .expect(StatusCode::OK);
    assert_eq!(pks(&body), [json!(1), json!(3), json!(2)], "{body}");
    // The documents are in the TiKV metastore's catalog.
    let store = api.server.meta_store();
    let ns = store
        .namespace_by_name(loams_common::meta::Consistency::Linearizable, "w")
        .await
        .expect("read")
        .expect("namespace w");
    let collection = store
        .resolve_collection(loams_common::meta::Consistency::Linearizable, ns.id, "kb")
        .await
        .expect("read")
        .expect("collection kb");
    assert_eq!(collection.partitions, 2);
    api.shutdown().await;
    let check = loams_meta_tikv::TikvMeta::open(loams_meta_tikv::TikvMetaConfig::new(
        loams_tikv::TikvConfig {
            root,
            ..cluster.config(TEST_META)
        },
    ))
    .await
    .expect("open");
    assert_eq!(
        check.check_invariants().await.expect("invariants"),
        Vec::<String>::new()
    );
}

/// Posts `body` to `server` and returns the JSON reply, asserting `status`.
async fn post(server: &Server, path: &str, body: Value, status: StatusCode) -> Value {
    let response = reqwest::Client::new()
        .post(format!("http://{}{path}", server.local_addr()))
        .json(&body)
        .send()
        .await
        .expect("send");
    assert_eq!(response.status(), status, "{path}");
    response.json().await.unwrap_or(Value::Null)
}

/// Stop, then start on the same keyspace and root (and data directory):
/// the collection and its documents are still there, and writes continue.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_keeps_state() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let url = meta_url(&cluster, &testing::random_root());
    let dir = TempDir::new().expect("temp dir");
    let start = || async {
        Server::start(config(&dir, MetaBackend::parse(&url).expect("url")))
            .await
            .expect("start")
    };
    let server = start().await;
    post(
        &server,
        "/v1/namespaces/w/collections",
        json!({"name": "kb", "schema": kb_schema(), "partitions": 2}),
        StatusCode::CREATED,
    )
    .await;
    post(
        &server,
        "/v1/namespaces/w/collections/kb/documents",
        kb_docs(),
        StatusCode::OK,
    )
    .await;
    server.shutdown().await.expect("shutdown");

    let server = start().await;
    let body = post(
        &server,
        "/v1/namespaces/w/query",
        strong_query(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(pks(&body), [json!(1), json!(3), json!(2)], "{body}");
    // Writes continue after the restart.
    post(
        &server,
        "/v1/namespaces/w/collections/kb/documents",
        json!({"ops": [{"upsert": {"id": 7, "source": {"body": "refund later", "tenant": "d", "n": 7},
                                   "vectors": {"embedding": [1.0, 0.0, 0.0]}}}],
               "report_existence": false}),
        StatusCode::OK,
    )
    .await;
    let body = post(
        &server,
        "/v1/namespaces/w/collections/kb/documents/get",
        json!({"ids": [7, 1], "consistency": "strong"}),
        StatusCode::OK,
    )
    .await;
    let documents = body["documents"].as_array().expect("documents");
    assert_eq!(documents.len(), 2, "{body}");
    assert!(documents.iter().all(|d| !d.is_null()), "{body}");
    server.shutdown().await.expect("shutdown");
}
