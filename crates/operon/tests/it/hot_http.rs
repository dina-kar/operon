//! The hot routes and the hot tier in the server (plan M1.3 Task 8): pin,
//! warm and status over HTTP, the hot flags, and maintenance and artifact
//! builds running in the server's worker.

use std::sync::Arc;
use std::time::{Duration, Instant};

use operon::ServerConfig;
use operon_common::meta::Consistency;
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use crate::common::{Native, Reply};

const NS: &str = "acme";
const DOCS: &str = "/v1/namespaces/acme/collections/docs";
const WAIT: Duration = Duration::from_secs(30);

/// Fast hot tuning (the Tests preamble), with `FlatEngine`.
fn fast(config: &mut ServerConfig) {
    config.hot.reconcile_interval = Duration::from_millis(50);
    config.hot_build.poll_interval = Duration::from_millis(100);
    config.hot_build.rebuild_max_staleness = Duration::from_millis(500);
    config.hnsw_engine = Some(Arc::new(operon_hnsw::FlatEngine));
    config.collection.index_poll_interval = Duration::from_millis(100);
}

async fn start(edit: impl FnOnce(&mut ServerConfig)) -> Native {
    Native::start_with(|config| {
        fast(config);
        edit(config);
    })
    .await
}

fn schema() -> Value {
    json!({
        "fields": [
            {"name": "body", "source_path": "body", "kind": {"text": {"analyzer": "standard", "positions": true}}, "indexed": true, "fast": false},
            {"name": "tenant", "source_path": "tenant", "kind": "keyword", "indexed": true, "fast": true}
        ],
        "vectors": [{"name": "embedding", "dim": 3, "distance": "cosine"}],
        "sparse_vectors": [],
        "dynamic": "ignore",
        "max_fields": 1000
    })
}

fn embedding(i: u64) -> Value {
    let a = i as f64 * 0.37;
    json!([a.cos(), a.sin(), 0.1 + (i % 7) as f64 * 0.05])
}

fn upserts(keys: std::ops::Range<u64>) -> Value {
    let ops: Vec<Value> = keys
        .map(|i| {
            json!({"upsert": {
                "id": i,
                "source": {"body": format!("word{} common", i % 5), "tenant": format!("t{}", i % 3)},
                "vectors": {"embedding": embedding(i)}
            }})
        })
        .collect();
    json!({ "ops": ops })
}

/// Creates `acme/docs` and writes `n` documents.
async fn docs(api: &Native, n: u64) {
    api.post(
        "/v1/namespaces/acme/collections",
        json!({"name": "docs", "schema": schema(), "partitions": 1}),
    )
    .await
    .expect(StatusCode::CREATED);
    write(api, 0..n).await;
}

async fn write(api: &Native, keys: std::ops::Range<u64>) {
    api.post(&format!("{DOCS}/documents"), upserts(keys))
        .await
        .expect(StatusCode::OK);
}

async fn put_hot(api: &Native, path: &str, body: Value) -> Reply {
    api.call(Method::PUT, &format!("{path}/hot"), &[], Some(body))
        .await
}

async fn collection(api: &Native) -> Value {
    api.get(DOCS).await.expect(StatusCode::OK)
}

/// Polls `GET …/docs` until `check` holds; returns that body.
async fn until(api: &Native, what: &str, check: impl Fn(&Value) -> bool) -> Value {
    let deadline = Instant::now() + WAIT;
    loop {
        let body = collection(api).await;
        if check(&body) {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting until {what}: {}",
            body["hot"]
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn state(body: &Value, structure: &str) -> String {
    body["hot"][structure]["state"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

fn all_ready(body: &Value) -> bool {
    ["vectors", "text", "fragments"]
        .iter()
        .all(|s| state(body, s) == "ready")
}

fn text_query() -> Value {
    json!({
        "collection": "docs",
        "retrievers": [{"text": {"query": {"match": {"field": "body", "text": "word2", "operator": "or", "minimum_should_match": null, "fuzziness": null, "analyzer": null}}, "k": 10}}],
        "fusion": null,
        "limit": 10
    })
}

fn vector_query(exact: bool) -> Value {
    json!({
        "collection": "docs",
        "retrievers": [{"vector": {"field": "embedding", "query": [1.0, 0.2, 0.1], "k": 5,
                        "params": {"exact": exact, "nprobes": null, "refine_factor": null, "ef": null, "oversampling": null, "distance": null},
                        "filter": null}}],
        "fusion": null,
        "limit": 5
    })
}

async fn query(api: &Native, body: Value, hot: Option<&str>) -> (Value, String) {
    let headers: Vec<(&str, &str)> = hot.map(|h| ("Operon-Hot", h)).into_iter().collect();
    let reply = api
        .post_with("/v1/namespaces/acme/query", &headers, body)
        .await;
    let used = reply.header("operon-hot-used").unwrap_or("").to_string();
    let body = reply.expect(StatusCode::OK);
    (body["hits"].clone(), used)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_hot_sets_the_catalog_and_returns_status() {
    let api = start(|_| {}).await;
    docs(&api, 50).await;
    let body = put_hot(&api, DOCS, json!({"vectors": true}))
        .await
        .expect(StatusCode::OK);
    assert_eq!(body["hot"]["config"]["vectors"], true, "{body}");
    assert_eq!(body["hot"]["enabled"], true);
    assert_eq!(body["hot"]["owner"], json!({"node_id": 1, "local": true}));
    assert_eq!(body["hot"]["vectors"]["state"], "building", "{body}");
    let ready = until(&api, "vectors are ready", |b| {
        state(b, "vectors") == "ready"
    })
    .await;
    assert_eq!(state(&ready, "text"), "off");
    assert_eq!(state(&ready, "fragments"), "off");
    assert_eq!(
        ready["hot"]["vectors"]["columns"]["embedding"]["state"],
        "ready"
    );
    api.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_collection_reports_hot_status_per_structure() {
    let api = start(|_| {}).await;
    docs(&api, 60).await;
    put_hot(
        &api,
        DOCS,
        json!({"vectors": true, "text": true, "fragments": true}),
    )
    .await
    .expect(StatusCode::OK);
    let body = until(&api, "every structure is ready at the live version", |b| {
        all_ready(b) && b["hot"]["vectors"]["source_version"] == b["manifest_version"]
    })
    .await;
    let hot = &body["hot"];
    assert_eq!(hot["text"]["pinned_splits"], hot["text"]["splits"]);
    assert_eq!(
        hot["fragments"]["prefetched_bytes"],
        hot["fragments"]["bytes"]
    );
    assert_eq!(hot["text"]["source_version"], body["manifest_version"]);
    assert_eq!(hot["promoted"], false);
    api.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_version_catches_up_after_writes_and_a_rebuild() {
    // A wider staleness window than the preamble's, so the stale state is
    // observable before the rebuild lands.
    let api = start(|config| {
        config.hot_build.rebuild_max_staleness = Duration::from_secs(3);
    })
    .await;
    docs(&api, 50).await;
    put_hot(&api, DOCS, json!({"vectors": true}))
        .await
        .expect(StatusCode::OK);
    let ready = until(&api, "vectors are current", |b| {
        state(b, "vectors") == "ready"
            && b["hot"]["vectors"]["source_version"] == b["manifest_version"]
    })
    .await;
    let before = ready["manifest_version"].as_u64().expect("version");
    write(&api, 50..100).await;
    let stale = until(&api, "the view is stale", |b| {
        state(b, "vectors") == "ready"
            && b["manifest_version"].as_u64() > Some(before)
            && b["hot"]["vectors"]["source_version"].as_u64() < b["manifest_version"].as_u64()
    })
    .await;
    assert!(stale["hot"]["vectors"]["columns"]["embedding"]["delta_rows"].as_u64() > Some(0));
    until(&api, "the rebuilt artifact is current", |b| {
        state(b, "vectors") == "ready"
            && b["live_doc_count"] == 100
            && b["hot"]["vectors"]["source_version"] == b["manifest_version"]
    })
    .await;
    api.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_hot_off_header_gives_identical_results_and_reports_none() {
    // A Lance vector index, so approximate queries take the hot path (E32);
    // Lance trains IVF-PQ on at least 256 rows.
    let api = start(|config| config.collection.index_min_rows = 256).await;
    docs(&api, 300).await;
    put_hot(
        &api,
        DOCS,
        json!({"vectors": true, "text": true, "fragments": true}),
    )
    .await
    .expect(StatusCode::OK);
    until(&api, "every structure is ready", all_ready).await;
    // The Lance vector index must exist for the hot ANN path (E32).
    let deadline = Instant::now() + WAIT;
    loop {
        let (_, used) = query(&api, vector_query(false), None).await;
        if used == "hnsw" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the hot ANN path was never used: {used}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for request in [text_query(), vector_query(true)] {
        let (on, used_on) = query(&api, request.clone(), None).await;
        let (off, used_off) = query(&api, request.clone(), Some("off")).await;
        assert_eq!(on, off, "{request}");
        assert_eq!(used_off, "none");
        assert!(
            ["splits", "none", "hnsw"].contains(&used_on.as_str()),
            "{used_on}"
        );
    }
    let (_, used) = query(&api, text_query(), None).await;
    assert_eq!(used, "splits");
    let (hits, used) = query(&api, vector_query(false), None).await;
    assert_eq!(used, "hnsw");
    assert_eq!(hits.as_array().map(Vec::len), Some(5));
    api.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_hot_off_flag_serves_everything_cold() {
    let api = start(|config| {
        config.hot.enabled = false;
        config.query.hot_default = false;
    })
    .await;
    assert!(api.server.hot_tier().is_none());
    docs(&api, 40).await;
    let body = put_hot(
        &api,
        DOCS,
        json!({"vectors": true, "text": true, "fragments": true}),
    )
    .await
    .expect(StatusCode::OK);
    assert_eq!(body["hot"]["enabled"], false, "{body}");
    assert_eq!(body["hot"]["config"]["text"], true);
    for request in [text_query(), vector_query(false), vector_query(true)] {
        let (_, used) = query(&api, request, None).await;
        assert_eq!(used, "none");
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let body = collection(&api).await;
    assert_eq!(state(&body, "vectors"), "off");
    let tasks = api
        .server
        .meta_store()
        .leases_with_prefix(Consistency::Local, "")
        .await
        .expect("leases");
    assert!(
        !tasks.iter().any(|(key, _)| key.contains("hot-build/")),
        "{tasks:?}"
    );
    let reply = api.post(&format!("{DOCS}/warm"), json!({})).await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST, "{}", reply.body);
    assert!(
        reply.body["message"]
            .as_str()
            .is_some_and(|m| m.contains("the hot tier is off on node 1")),
        "{}",
        reply.body
    );
    api.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_pin_all_pins_every_collection() {
    let api = start(|config| config.hot.pin_all = true).await;
    docs(&api, 40).await;
    let body = until(&api, "vectors and text are ready", |b| {
        state(b, "vectors") == "ready" && state(b, "text") == "ready"
    })
    .await;
    assert_eq!(body["hot"]["pin_all"], true);
    assert_eq!(body["hot"]["config"]["vectors"], false, "no catalog change");
    assert_eq!(state(&body, "fragments"), "off");
    api.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn warm_prefetches_an_unpinned_collection() {
    let api = start(|config| config.hot.heat_window = Duration::from_millis(200)).await;
    docs(&api, 40).await;
    let body = api
        .post(&format!("{DOCS}/warm"), json!({}))
        .await
        .expect(StatusCode::ACCEPTED);
    assert_eq!(body["hot"]["owner"]["local"], true, "{body}");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if state(&collection(&api).await, "text") == "ready" {
            break;
        }
        assert!(Instant::now() < deadline, "the warm never pinned the text");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    until(&api, "the warm cools off", |b| state(b, "text") == "off").await;
    // An empty body is fine too; a body with fields is not.
    let reply = api
        .call(Method::POST, &format!("{DOCS}/warm"), &[], None)
        .await;
    assert_eq!(reply.status, StatusCode::ACCEPTED, "{}", reply.body);
    let reply = api
        .post(&format!("{DOCS}/warm"), json!({"now": true}))
        .await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
    api.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operon_warm_posts_the_warm_request() {
    let dir = tempfile::TempDir::new().expect("dir");
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.local_addr().expect("addr").port()
    };
    let url = format!("http://127.0.0.1:{port}");
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_operon"))
        .args([
            "dev",
            "--data-dir",
            dir.path().to_str().expect("utf-8"),
            "--listen",
            &format!("127.0.0.1:{port}"),
            "--no-flight-sql",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn operon dev");
    let http = reqwest::Client::new();
    let deadline = Instant::now() + WAIT;
    loop {
        if let Ok(response) = http.get(format!("{url}/ready")).send().await
            && response.status().is_success()
        {
            break;
        }
        assert!(Instant::now() < deadline, "operon dev never became ready");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let created = http
        .post(format!("{url}/v1/namespaces/{NS}/collections"))
        .json(&json!({"name": "docs", "schema": schema(), "partitions": 1}))
        .send()
        .await
        .expect("create");
    assert_eq!(created.status(), StatusCode::CREATED);
    let warm = |target: &'static str| {
        let url = url.clone();
        tokio::task::spawn_blocking(move || {
            std::process::Command::new(env!("CARGO_BIN_EXE_operon"))
                .args(["warm", target, "--server", &url])
                .output()
                .expect("run operon warm")
        })
    };
    let output = warm("acme/docs").await.expect("join");
    assert!(output.status.success(), "{output:?}");
    let body: Value = serde_json::from_slice(&output.stdout).expect("JSON on stdout");
    assert_eq!(body["hot"]["enabled"], true, "{body}");
    let output = warm("acme/missing").await.expect("join");
    assert_eq!(output.status.code(), Some(1));
    assert!(!output.stderr.is_empty());
    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_hot_on_a_missing_collection_is_404() {
    let api = start(|_| {}).await;
    docs(&api, 5).await;
    let reply = put_hot(
        &api,
        "/v1/namespaces/acme/collections/nope",
        json!({"vectors": true}),
    )
    .await;
    assert_eq!(reply.status, StatusCode::NOT_FOUND, "{}", reply.body);
    assert_eq!(reply.body["error"], "not_found");
    let reply = put_hot(
        &api,
        "/v1/namespaces/nobody/collections/docs",
        json!({"vectors": true}),
    )
    .await;
    assert_eq!(reply.status, StatusCode::NOT_FOUND);
    let reply = api
        .post("/v1/namespaces/acme/collections/nope/warm", json!({}))
        .await;
    assert_eq!(reply.status, StatusCode::NOT_FOUND);
    api.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_malformed_hot_body_is_400() {
    let api = start(|_| {}).await;
    docs(&api, 5).await;
    for body in [
        json!({"vectors": "yes"}),
        json!({"vectors": true, "graphs": true}),
        json!([true]),
    ] {
        let reply = put_hot(&api, DOCS, body.clone()).await;
        assert_eq!(reply.status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(reply.body["error"], "invalid_argument");
    }
    // Absent keys are false.
    let body = put_hot(&api, DOCS, json!({})).await.expect(StatusCode::OK);
    assert_eq!(
        body["hot"]["config"],
        json!({"vectors": false, "text": false, "fragments": false})
    );
    api.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_routes_accept_aliases() {
    let api = start(|_| {}).await;
    docs(&api, 20).await;
    api.post(
        "/v1/namespaces/acme/aliases",
        json!({"actions": [{"create": {"alias": "live", "collection": "docs"}}]}),
    )
    .await
    .expect(StatusCode::OK);
    let body = put_hot(
        &api,
        "/v1/namespaces/acme/collections/live",
        json!({"text": true}),
    )
    .await
    .expect(StatusCode::OK);
    assert_eq!(body["hot"]["config"]["text"], true);
    assert_eq!(collection(&api).await["hot"]["config"]["text"], true);
    api.post("/v1/namespaces/acme/collections/live/warm", json!({}))
        .await
        .expect(StatusCode::ACCEPTED);
    let by_alias = api
        .get("/v1/namespaces/acme/collections/live")
        .await
        .expect(StatusCode::OK);
    assert_eq!(by_alias["hot"]["config"]["text"], true);
    api.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn maintenance_runs_in_the_server() {
    let api = start(|config| {
        config.maintenance.poll_interval = Duration::from_millis(100);
        // One link commit per write.
        config.link.batch_interval = Duration::ZERO;
    })
    .await;
    api.post(
        "/v1/namespaces/acme/collections",
        json!({"name": "docs", "schema": schema(), "partitions": 1}),
    )
    .await
    .expect(StatusCode::CREATED);
    let ctx = api.server.collection_context().clone();
    let meta = api.server.meta_store();
    let ns = meta
        .namespace_by_name(Consistency::Local, NS)
        .await
        .expect("namespace")
        .expect("acme")
        .id;
    let cid = meta
        .resolve_collection(Consistency::Local, ns, "docs")
        .await
        .expect("resolve")
        .expect("docs")
        .id;
    let manifest = || async {
        operon_collection::live_manifest(
            &*ctx.meta,
            &ctx.store,
            &ctx.manifests,
            ns,
            cid,
            Consistency::Linearizable,
        )
        .await
        .expect("manifest")
        .map(|(_, manifest)| manifest)
    };
    const COMMITS: u64 = 30;
    for i in 0..COMMITS {
        write(&api, i * 2..i * 2 + 2).await;
        // Wait for its own link commit.
        let deadline = Instant::now() + WAIT;
        loop {
            if manifest()
                .await
                .is_some_and(|m| m.live_doc_count >= (i + 1) * 2)
            {
                break;
            }
            assert!(Instant::now() < deadline, "commit {i} never applied");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    let deadline = Instant::now() + WAIT;
    loop {
        let live = manifest().await.expect("a manifest");
        let snapshot =
            operon_collection::CollectionSnapshot::open(&ctx, ns, cid, Consistency::Linearizable)
                .await
                .expect("snapshot");
        let fragments = snapshot.dataset().map_or(0, |d| d.fragments().len()) as u64;
        if (live.splits.len() as u64) < COMMITS && fragments < COMMITS {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no maintenance: {} splits, {fragments} fragments",
            live.splits.len()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let count = api
        .post(&format!("{DOCS}/documents/count"), json!({}))
        .await
        .expect(StatusCode::OK);
    assert_eq!(count["count"], COMMITS * 2);
    let ids: Vec<u64> = (0..COMMITS * 2).collect();
    let got = api
        .post(&format!("{DOCS}/documents/get"), json!({ "ids": ids }))
        .await
        .expect(StatusCode::OK);
    let found = got["documents"]
        .as_array()
        .expect("documents")
        .iter()
        .filter(|doc| !doc.is_null())
        .count() as u64;
    assert_eq!(found, COMMITS * 2, "{got}");
    api.shutdown().await;
}

/// The same pin with the server's default engine (qdrant-edge with the
/// `hnsw` feature, the flat engine without it).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_default_engine_serves_hot_vectors() {
    let api = start(|config| config.hnsw_engine = None).await;
    docs(&api, 50).await;
    put_hot(&api, DOCS, json!({"vectors": true}))
        .await
        .expect(StatusCode::OK);
    let body = until(&api, "vectors are ready", |b| {
        state(b, "vectors") == "ready"
    })
    .await;
    assert_eq!(
        body["hot"]["vectors"]["columns"]["embedding"]["state"],
        "ready"
    );
    api.shutdown().await;
}
