//! The native HTTP API, against a server started in-process.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use operon::{Server, ServerConfig};
use operon_log::SegmenterConfig;
use operon_meta::{Consistency, EntryKind};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tempfile::TempDir;

const WAIT: Duration = Duration::from_secs(20);

fn config(dir: &TempDir, segmenter: SegmenterConfig) -> ServerConfig {
    let mut config = ServerConfig::new(dir.path());
    config.listen = SocketAddr::from(([127, 0, 0, 1], 0));
    config.log.flush_interval = Duration::from_millis(20);
    config.segmenter = segmenter;
    config.worker_poll_interval = Duration::from_millis(50);
    config
}

/// Never segments small test data (64 MiB or 10 minutes).
fn lazy_segmenter() -> SegmenterConfig {
    SegmenterConfig::default()
}

/// Segments everything, often.
fn eager_segmenter() -> SegmenterConfig {
    SegmenterConfig {
        min_bytes: 1,
        target_bytes: 400,
        ..SegmenterConfig::default()
    }
}

struct Api {
    base: String,
    http: reqwest::Client,
}

impl Api {
    fn new(server: &Server) -> Self {
        Self {
            base: format!("http://{}", server.local_addr()),
            http: reqwest::Client::new(),
        }
    }

    async fn post(&self, path: &str, body: Value) -> (StatusCode, Value) {
        let response = self
            .http
            .post(format!("{}{path}", self.base))
            .json(&body)
            .send()
            .await
            .expect("send");
        let status = response.status();
        (status, response.json().await.unwrap_or(Value::Null))
    }

    async fn get(&self, path: &str) -> (StatusCode, Value) {
        let response = self
            .http
            .get(format!("{}{path}", self.base))
            .send()
            .await
            .expect("send");
        let status = response.status();
        (status, response.json().await.unwrap_or(Value::Null))
    }

    async fn setup(&self, partitions: u32) {
        let (status, _) = self.post("/v1/namespaces", json!({"name": "acme"})).await;
        assert_eq!(status, StatusCode::CREATED);
        let (status, _) = self
            .post(
                "/v1/namespaces/acme/streams",
                json!({"name": "events", "partitions": partitions}),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);
    }

    /// Produces records with values `values` to partition `p`; returns the base offset.
    async fn produce(&self, p: u32, values: &[&str]) -> u64 {
        let records: Vec<Value> = values
            .iter()
            .map(|v| json!({"value": BASE64.encode(v)}))
            .collect();
        let (status, body) = self
            .post(
                &format!("/v1/namespaces/acme/streams/events/partitions/{p}/records"),
                json!({ "records": records }),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body["base_offset"].as_u64().expect("base_offset")
    }

    /// Fetches everything from `offset` to the high watermark.
    async fn fetch_all(&self, p: u32, mut offset: u64) -> Vec<(u64, String)> {
        let mut out = Vec::new();
        loop {
            let (status, body) = self
                .get(&format!(
                    "/v1/namespaces/acme/streams/events/partitions/{p}/records?offset={offset}&max_bytes=200"
                ))
                .await;
            assert_eq!(status, StatusCode::OK, "{body}");
            let records = body["records"].as_array().expect("records");
            if records.is_empty() {
                return out;
            }
            for record in records {
                let value = BASE64
                    .decode(record["value"].as_str().expect("value"))
                    .expect("base64");
                out.push((
                    record["offset"].as_u64().expect("offset"),
                    String::from_utf8(value).expect("utf-8"),
                ));
            }
            offset = body["next_offset"].as_u64().expect("next_offset");
        }
    }
}

#[tokio::test]
async fn namespaces_and_streams_are_created_once() {
    let dir = TempDir::new().unwrap();
    let server = Server::start(config(&dir, lazy_segmenter())).await.unwrap();
    let api = Api::new(&server);

    let (status, body) = api.post("/v1/namespaces", json!({"name": "acme"})).await;
    assert_eq!(status, StatusCode::CREATED);
    let ns = body["id"].as_u64().unwrap();
    let (status, body) = api.post("/v1/namespaces", json!({"name": "acme"})).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "already_exists");
    assert_eq!(body["id"].as_u64(), Some(ns));

    let stream = json!({"name": "events", "partitions": 2, "retention": {"max_age_ms": 60000}});
    let (status, body) = api
        .post("/v1/namespaces/acme/streams", stream.clone())
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let id = body["id"].as_u64().unwrap();
    let (status, body) = api.post("/v1/namespaces/acme/streams", stream).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["id"].as_u64(), Some(id));

    let (status, body) = api.get("/v1/namespaces/acme/streams/events").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        json!({
            "id": id,
            "partitions": [
                {"partition": 0, "log_start_offset": 0, "high_watermark": 0},
                {"partition": 1, "log_start_offset": 0, "high_watermark": 0},
            ],
            "retention": {"max_age_ms": 60000, "max_bytes": null},
        })
    );

    let (status, body) = api.get("/v1/namespaces/nope/streams/events").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "not_found");
    let (status, _) = api
        .post(
            "/v1/namespaces/nope/streams",
            json!({"name": "x", "partitions": 1}),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, body) = api
        .post("/v1/namespaces", json!({"name": "bad name!"}))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_argument");
    let (status, body) = api.post("/v1/namespaces", json!({"nom": "x"})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid_argument");

    let (status, _) = api.get("/health").await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = api.get("/ready").await;
    assert_eq!(status, StatusCode::OK);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn produced_records_are_fetched_back() {
    let dir = TempDir::new().unwrap();
    let server = Server::start(config(&dir, lazy_segmenter())).await.unwrap();
    let api = Api::new(&server);
    api.setup(2).await;

    let (status, body) = api
        .post(
            "/v1/namespaces/acme/streams/events/partitions/1/records",
            json!({"records": [
                {"key": BASE64.encode("k"), "value": BASE64.encode("v1"),
                 "headers": [{"key": "h", "value": BASE64.encode("x")}, {"key": "n"}],
                 "timestamp_ms": 1234},
                {"value": BASE64.encode("v2")},
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["base_offset"], 0);
    assert_eq!(body["last_offset"], 1);
    let token = &body["token"][0];
    assert_eq!(token["partition"], 1);
    assert_eq!(token["offset"], 1);
    assert!(token["stream"].as_u64().is_some());

    let (status, body) = api
        .get("/v1/namespaces/acme/streams/events/partitions/1/records?offset=0")
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["next_offset"], 2);
    assert_eq!(body["high_watermark"], 2);
    assert_eq!(body["log_start_offset"], 0);
    let first = &body["records"][0];
    assert_eq!(first["offset"], 0);
    assert_eq!(first["key"], BASE64.encode("k"));
    assert_eq!(first["value"], BASE64.encode("v1"));
    assert_eq!(first["timestamp_ms"], 1234);
    assert_eq!(
        first["headers"],
        json!([{"key": "h", "value": BASE64.encode("x")}, {"key": "n", "value": null}])
    );
    let second = &body["records"][1];
    assert_eq!(second["key"], Value::Null);
    assert!(second["timestamp_ms"].as_i64().unwrap() > 1_600_000_000_000);

    // Bad requests.
    let (status, _) = api
        .post(
            "/v1/namespaces/acme/streams/events/partitions/1/records",
            json!({"records": []}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = api
        .post(
            "/v1/namespaces/acme/streams/events/partitions/1/records",
            json!({"records": [{"value": "not base64!"}]}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = api
        .post(
            "/v1/namespaces/acme/streams/events/partitions/9/records",
            json!({"records": [{"value": BASE64.encode("x")}]}),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = api
        .get("/v1/namespaces/acme/streams/events/partitions/1/records")
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = api
        .get("/v1/namespaces/acme/streams/events/partitions/x/records?offset=0")
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_long_poll_fetch_wakes_on_produce() {
    let dir = TempDir::new().unwrap();
    let server = Server::start(config(&dir, lazy_segmenter())).await.unwrap();
    let api = Api::new(&server);
    api.setup(1).await;
    api.produce(0, &["a"]).await;

    let poll = {
        let url = format!(
            "{}/v1/namespaces/acme/streams/events/partitions/0/records?offset=1&max_wait_ms=20000",
            api.base
        );
        let http = api.http.clone();
        tokio::spawn(async move {
            let body: Value = http
                .get(url)
                .send()
                .await
                .expect("send")
                .json()
                .await
                .expect("json");
            (body, Instant::now())
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!poll.is_finished());
    let started = Instant::now();
    api.produce(0, &["b"]).await;
    let (body, woke) = poll.await.unwrap();
    assert_eq!(body["records"][0]["offset"], 1);
    assert_eq!(body["records"][0]["value"], BASE64.encode("b"));
    assert!(woke.duration_since(started) < Duration::from_secs(5));

    // At the high watermark with a short wait: empty, not an error.
    let (status, body) = api
        .get("/v1/namespaces/acme/streams/events/partitions/0/records?offset=2&max_wait_ms=50")
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["records"], json!([]));
    assert_eq!(body["next_offset"], 2);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn out_of_range_offsets_get_416_with_both_bounds() {
    let dir = TempDir::new().unwrap();
    let server = Server::start(config(&dir, lazy_segmenter())).await.unwrap();
    let api = Api::new(&server);
    api.setup(1).await;
    api.produce(0, &["a", "b", "c"]).await;

    let (status, body) = api
        .get("/v1/namespaces/acme/streams/events/partitions/0/records?offset=7")
        .await;
    assert_eq!(status, StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(body["error"], "offset_out_of_range");
    assert_eq!(body["log_start_offset"], 0);
    assert_eq!(body["high_watermark"], 3);
    assert!(body["message"].as_str().is_some());

    let stream = server
        .meta()
        .read(Consistency::Local, |s| {
            let ns = s.namespace_by_name("acme").unwrap().id;
            s.stream_by_name(ns, "events").unwrap().id
        })
        .await
        .unwrap();
    server
        .meta()
        .trim_partition(stream, 0, 2, None)
        .await
        .unwrap();
    let (status, body) = api
        .get("/v1/namespaces/acme/streams/events/partitions/0/records?offset=1")
        .await;
    assert_eq!(status, StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(body["log_start_offset"], 2);
    assert_eq!(body["high_watermark"], 3);
    server.shutdown().await.unwrap();
}

async fn segment_count(server: &Server) -> usize {
    server
        .meta()
        .read(Consistency::Local, |s| {
            s.all_streams()
                .flat_map(|st| (0..st.partitions).map(move |p| (st.id, p)))
                .filter_map(|(id, p)| s.partition(id, p))
                .flat_map(|p| p.entries())
                .filter(|e| e.kind == EntryKind::Segment)
                .count()
        })
        .await
        .expect("read")
}

/// Every acknowledged record is readable at its offset after a restart on the
/// same data directory: first while only in WAL objects, then after the
/// segmenter rewrote them.
#[tokio::test]
async fn acknowledged_records_survive_a_restart() {
    let dir = TempDir::new().unwrap();
    let server = Server::start(config(&dir, lazy_segmenter())).await.unwrap();
    let api = Api::new(&server);
    api.setup(2).await;
    let mut model: Vec<Vec<(u64, String)>> = vec![Vec::new(), Vec::new()];
    for i in 0..10 {
        let p = i % 2;
        let values: Vec<String> = (0..3).map(|j| format!("r{i}-{j}")).collect();
        let refs: Vec<&str> = values.iter().map(String::as_str).collect();
        let base = api.produce(p as u32, &refs).await;
        model[p].extend((base..).zip(values));
    }
    assert_eq!(segment_count(&server).await, 0);
    server.shutdown().await.unwrap();

    // Restart: the records are still only in WAL objects.
    let server = Server::start(config(&dir, lazy_segmenter())).await.unwrap();
    let api = Api::new(&server);
    for (p, expected) in model.iter().enumerate() {
        assert_eq!(api.fetch_all(p as u32, 0).await, *expected);
    }
    assert_eq!(segment_count(&server).await, 0);
    // New appends continue after the old offsets.
    let base = api.produce(0, &["after-restart"]).await;
    assert_eq!(base, model[0].len() as u64);
    model[0].push((base, "after-restart".to_string()));
    server.shutdown().await.unwrap();

    // Restart with an eager segmenter and wait until it has rewritten the WAL.
    let server = Server::start(config(&dir, eager_segmenter()))
        .await
        .unwrap();
    let deadline = Instant::now() + WAIT;
    loop {
        let wal_left = server
            .meta()
            .read(Consistency::Local, |s| {
                s.all_streams()
                    .flat_map(|st| (0..st.partitions).map(move |p| (st.id, p)))
                    .filter_map(|(id, p)| s.partition(id, p))
                    .flat_map(|p| p.entries())
                    .any(|e| e.kind == EntryKind::Wal)
            })
            .await
            .unwrap();
        if !wal_left {
            break;
        }
        assert!(Instant::now() < deadline, "the segmenter never finished");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(segment_count(&server).await >= 2);
    server.shutdown().await.unwrap();

    let server = Server::start(config(&dir, lazy_segmenter())).await.unwrap();
    let api = Api::new(&server);
    for (p, expected) in model.iter().enumerate() {
        assert_eq!(api.fetch_all(p as u32, 0).await, *expected);
    }
    server.shutdown().await.unwrap();
}

#[test]
fn the_dev_command_prints_help() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_operon"))
        .args(["dev", "--help"])
        .output()
        .expect("run operon");
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    for flag in ["--data-dir", "--listen", "--flush-interval-ms"] {
        assert!(help.contains(flag), "{help}");
    }
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_operon"))
        .args(["standalone", "--help"])
        .output()
        .expect("run operon");
    assert!(output.status.success());
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("--bucket")
    );
}

/// Review M2 and M3: framework-level rejections use the API's JSON error
/// body, oversized bodies get 413, and `max_bytes` is capped instead of
/// letting one request read a whole partition.
#[tokio::test]
async fn framework_rejections_use_the_json_error_body() {
    let dir = TempDir::new().unwrap();
    let server = Server::start(config(&dir, lazy_segmenter())).await.unwrap();
    let api = Api::new(&server);
    api.setup(1).await;
    api.produce(0, &["a", "b"]).await;

    let big = "x".repeat(operon::api::MAX_BODY_BYTES + 1);
    let response = api
        .http
        .post(format!("{}/v1/namespaces", api.base))
        .body(big)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["error"], "invalid_argument");
    assert!(body["message"].as_str().is_some());

    for path in [
        "/v1/namespaces/%FF/streams/events",
        "/v1/namespaces/acme/streams/events/partitions/0/records?offset=%FF%FE",
        "/no/such/route",
    ] {
        let response = api
            .http
            .get(format!("{}{path}", api.base))
            .send()
            .await
            .unwrap();
        let status = response.status();
        assert!(status.is_client_error(), "{path}: {status}");
        let body: Value = response.json().await.expect("a JSON error body");
        assert!(body["error"].as_str().is_some(), "{path}: {body}");
        assert!(body["message"].as_str().is_some(), "{path}: {body}");
    }

    // M0.3 re-review M3: a known route with the wrong method is a 405 with
    // the JSON error body too.
    for (method, path) in [
        (reqwest::Method::GET, "/v1/namespaces"),
        (
            reqwest::Method::DELETE,
            "/v1/namespaces/acme/streams/events",
        ),
        (reqwest::Method::PUT, "/v1/namespaces/acme/links"),
    ] {
        let response = api
            .http
            .request(method.clone(), format!("{}{path}", api.base))
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::METHOD_NOT_ALLOWED,
            "{method} {path}"
        );
        let body: Value = response.json().await.expect("a JSON error body");
        assert_eq!(body["error"], "invalid_argument", "{method} {path}");
        assert!(
            body["message"].as_str().is_some(),
            "{method} {path}: {body}"
        );
    }

    let (status, body) = api
        .get(&format!(
            "/v1/namespaces/acme/streams/events/partitions/0/records?offset=0&max_bytes={}",
            u64::MAX
        ))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["records"].as_array().unwrap().len(), 2);
    server.shutdown().await.unwrap();
}

/// Review M11: a failed start stops what it started, so the same data
/// directory can be opened again in the same process.
#[tokio::test]
async fn a_failed_start_releases_the_data_directory() {
    let dir = TempDir::new().unwrap();
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut busy = config(&dir, lazy_segmenter());
    busy.listen = taken.local_addr().unwrap();
    let err = Server::start(busy).await.unwrap_err();
    assert!(matches!(err, operon::ServerError::Listen { .. }), "{err}");

    let server = Server::start(config(&dir, lazy_segmenter())).await.unwrap();
    let (status, _) = Api::new(&server).get("/ready").await;
    assert_eq!(status, StatusCode::OK);
    server.shutdown().await.unwrap();
}

/// M0.4 re-review m1: a freshness deadline at or above GC's grace period is
/// refused at startup, not clamped. The segmenter's deadline is below grace,
/// so the error must name the link's.
#[tokio::test]
async fn a_config_with_a_deadline_at_grace_is_refused() {
    let dir = TempDir::new().unwrap();
    let mut bad = config(&dir, lazy_segmenter());
    bad.gc.grace = Duration::from_secs(2);
    bad.segmenter.swap_deadline = Duration::from_secs(1);
    bad.link.max_commit_delay = Duration::from_secs(2);
    let err = Server::start(bad).await.unwrap_err();
    let operon::ServerError::Config(message) = &err else {
        panic!("expected a config error, got {err:?}");
    };
    assert!(message.contains("link.max_commit_delay"), "{message}");
    assert!(!message.contains("segmenter.swap_deadline"), "{message}");
}

/// M1.1 Task 13 (Ruling 22, controller ruling P11): the collection index
/// commit delay is a freshness deadline too. The other deadlines are below
/// grace, so the error must name it.
#[tokio::test]
async fn a_config_with_index_commit_delay_at_grace_is_refused() {
    let dir = TempDir::new().unwrap();
    let mut bad = config(&dir, lazy_segmenter());
    bad.gc.grace = Duration::from_secs(2);
    bad.segmenter.swap_deadline = Duration::from_secs(1);
    bad.link.max_commit_delay = Duration::from_secs(1);
    bad.collection.index_commit_delay = Duration::from_secs(2);
    let err = Server::start(bad).await.unwrap_err();
    let operon::ServerError::Config(message) = &err else {
        panic!("expected a config error, got {err:?}");
    };
    assert!(
        message.contains("collection.index_commit_delay"),
        "{message}"
    );
    assert!(!message.contains("segmenter.swap_deadline"), "{message}");
    assert!(!message.contains("link.max_commit_delay"), "{message}");

    // Below grace, the same config starts.
    let mut good = config(&dir, lazy_segmenter());
    good.gc.grace = Duration::from_secs(2);
    good.segmenter.swap_deadline = Duration::from_secs(1);
    good.link.max_commit_delay = Duration::from_secs(1);
    good.collection.index_commit_delay = Duration::from_secs(1);
    Server::start(good).await.unwrap().shutdown().await.unwrap();
}

/// M1.1 Task 13: the server runs collection link apply. A write through a
/// `CollectionWriter` on the server's log writer is applied by the server's
/// worker, and a snapshot on the server's collection context reads it.
#[tokio::test]
async fn the_server_applies_collection_links() {
    use operon_collection::{
        CollectionSchema, CollectionSnapshot, CollectionWriter, DocOp, Document, DynamicMapping,
        OpResult, PrimaryKey,
    };

    let dir = TempDir::new().unwrap();
    let mut cfg = config(&dir, lazy_segmenter());
    cfg.link.batch_interval = Duration::ZERO;
    let server = Server::start(cfg).await.unwrap();
    let api = Api::new(&server);
    let meta = server.meta();
    let ns = meta.create_namespace("acme").await.unwrap();
    let schema = CollectionSchema::new(vec![], vec![], DynamicMapping::Ignore);
    let (cid, stream, _) = meta.create_collection(ns, "docs", schema, 2).await.unwrap();

    let doc = |n: u64| Document {
        pk: PrimaryKey::U64(n),
        source: serde_json::Map::from_iter([("n".to_string(), json!(n))]),
        vectors: Default::default(),
        sparse_vectors: Default::default(),
    };
    let mut ops: Vec<DocOp> = (0..6).map(|n| DocOp::Upsert(doc(n))).collect();
    ops.push(DocOp::Delete(PrimaryKey::U64(5)));
    let outcome = CollectionWriter::new(meta.clone(), server.log_writer().clone())
        .write(ns, cid, ops)
        .await
        .unwrap();
    assert!(
        outcome
            .results
            .iter()
            .all(|r| matches!(r, OpResult::Written { .. })),
        "{outcome:?}"
    );

    let hwms: Vec<Value> = meta
        .read(Consistency::Linearizable, |s| {
            (0..2)
                .filter_map(|p| {
                    let hwm = s.partition(stream, p)?.high_watermark();
                    (hwm > 0).then(|| json!({ "partition": p, "offset": hwm }))
                })
                .collect()
        })
        .await
        .unwrap();
    let path = format!(
        "/v1/namespaces/acme/links/{}",
        operon_meta::implicit_name("docs", cid)
    );
    let deadline = Instant::now() + WAIT;
    loop {
        let (status, body) = api.get(&path).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        if body["applied"] == Value::from(hwms.clone()) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the collection link never caught up: {body}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let snapshot = CollectionSnapshot::open(
        server.collection_context(),
        ns,
        cid,
        Consistency::Linearizable,
    )
    .await
    .unwrap();
    let pks: Vec<PrimaryKey> = (0..6).map(PrimaryKey::U64).collect();
    let found = snapshot.get_by_pk(&pks).await.unwrap();
    for (n, stored) in (0..6).zip(&found) {
        if n == 5 {
            assert!(stored.is_none(), "{stored:?}");
        } else {
            let stored = stored.as_ref().expect("a live document");
            assert_eq!(stored.source, doc(n).source);
        }
    }
    assert_eq!(snapshot.manifest().live_doc_count, 5);
    server.shutdown().await.unwrap();
}

/// A running `operon dev` process.
struct Dev {
    child: std::process::Child,
    base: String,
}

impl Dev {
    fn start(dir: &TempDir) -> Self {
        use std::io::BufRead;
        let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_operon"))
            .args([
                "dev",
                "--listen",
                "127.0.0.1:0",
                "--flush-interval-ms",
                "20",
                // Parallel servers must not share Flight SQL's fixed port.
                "--flight-sql-listen",
                "127.0.0.1:0",
            ])
            .arg("--data-dir")
            .arg(dir.path())
            .env("RUST_LOG", "warn")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn operon");
        let stdout = child.stdout.take().expect("stdout");
        let mut lines = std::io::BufReader::new(stdout).lines();
        let base = loop {
            let line = lines
                .next()
                .expect("operon exited before listening")
                .expect("read stdout");
            if let Some(url) = line.strip_prefix("operon listening on ") {
                break url.trim().to_string();
            }
        };
        // Keep draining stdout so the process never blocks on a full pipe.
        std::thread::spawn(move || for _ in lines {});
        Self { child, base }
    }

    fn api(&self) -> Api {
        Api {
            base: self.base.clone(),
            http: reqwest::Client::new(),
        }
    }

    /// SIGKILL: no flush, no shutdown, no final snapshot.
    fn kill(self) {
        drop(self);
    }
}

impl Drop for Dev {
    /// Also kills the process when a test fails, so none is left behind.
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Review M12 / Review Focus 5: every acknowledged record survives a crash
/// (SIGKILL of `operon dev`), not only a graceful shutdown.
#[tokio::test]
async fn acknowledged_records_survive_a_crash() {
    let dir = TempDir::new().unwrap();
    let dev = Dev::start(&dir);
    let api = dev.api();
    api.setup(1).await;
    let mut model = Vec::new();
    for i in 0..10 {
        let values: Vec<String> = (0..2).map(|j| format!("c{i}-{j}")).collect();
        let refs: Vec<&str> = values.iter().map(String::as_str).collect();
        let base = api.produce(0, &refs).await;
        model.extend((base..).zip(values));
    }
    dev.kill();

    let dev = Dev::start(&dir);
    let api = dev.api();
    assert_eq!(api.fetch_all(0, 0).await, model);
    let base = api.produce(0, &["after-crash"]).await;
    assert_eq!(base, model.len() as u64);
    dev.kill();
}

/// M0.4: links are declared and read over HTTP; a link sums its source.
#[tokio::test]
async fn a_link_is_created_once_and_sums_its_stream() {
    let dir = TempDir::new().unwrap();
    let mut cfg = config(&dir, lazy_segmenter());
    cfg.link.batch_interval = Duration::ZERO;
    let server = Server::start(cfg).await.unwrap();
    let api = Api::new(&server);
    api.setup(2).await;
    let (status, body) = api
        .post(
            "/v1/namespaces/acme/links",
            json!({"name": "counts", "source": "events"}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let id = body["id"].as_u64().unwrap();
    let (status, body) = api
        .post(
            "/v1/namespaces/acme/links",
            json!({"name": "counts", "source": "events"}),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["id"].as_u64(), Some(id));
    let (status, _) = api
        .post(
            "/v1/namespaces/acme/links",
            json!({"name": "other", "source": "missing"}),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    for (p, key, delta) in [(0, "a", "5"), (1, "a", "-2"), (0, "b", "10"), (1, "b", "x")] {
        let (status, body) = api
            .post(
                &format!("/v1/namespaces/acme/streams/events/partitions/{p}/records"),
                json!({"records": [{"key": BASE64.encode(key), "value": BASE64.encode(delta)}]}),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    let deadline = Instant::now() + WAIT;
    let body = loop {
        let (status, body) = api.get("/v1/namespaces/acme/links/counts").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        if body["applied"] == json!([{"partition": 0, "offset": 2}, {"partition": 1, "offset": 2}])
        {
            break body;
        }
        assert!(
            Instant::now() < deadline,
            "the link never caught up: {body}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(body["counters"], json!({"a": 3, "b": 10}));
    assert_eq!(body["skipped"], json!(1));
    assert_eq!(body["target"], json!({"kind": "counter", "name": "counts"}));
    let (status, _) = api.get("/v1/namespaces/acme/links/missing").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    server.shutdown().await.unwrap();
}

/// M1.1 Task 10: the link description of a collection's implicit link shows
/// its target's version and applied offsets, read through the registry.
#[tokio::test]
async fn the_link_endpoint_shows_version_and_applied_for_collections() {
    use std::sync::Arc;

    use operon_collection::{
        CollectionConfig, CollectionContext, CollectionSchema, CollectionTargetFactory,
        CollectionWriter, DocOp, Document, DynamicMapping, LanceConfig, LanceEnv, ManifestCache,
        PrimaryKey,
    };
    use operon_link::{
        CounterTargetFactory, LinkApplySource, LinkConfig, MAX_COMMIT_DELAY, TargetRegistry,
    };
    use operon_log::{LogConfig, LogReader, LogWriter};
    use operon_meta::{MetaClient, MetaClientConfig, MetaConfig, MetaNode, SystemClock};
    use operon_store::Store;

    let dir = TempDir::new().unwrap();
    let store = Store::in_memory();
    let node = MetaNode::start(
        MetaConfig::new(1, dir.path().join("meta"), store.clone()),
        &operon_meta::Router::new(),
    )
    .await
    .unwrap();
    node.initialize([1]).await.unwrap();
    node.wait_for_leader(WAIT).await.unwrap();
    let meta = MetaClient::new(
        node.clone(),
        vec![],
        Arc::new(SystemClock),
        MetaClientConfig::default(),
    );
    let writer = LogWriter::start(
        meta.clone(),
        store.clone(),
        LogConfig {
            flush_interval: Duration::from_millis(20),
            ..LogConfig::new(1)
        },
    )
    .unwrap();
    let cache = operon_cache::RangeCache::new(store.clone(), Default::default())
        .await
        .unwrap();
    let reader = LogReader::new(meta.clone(), cache.clone());
    let config = CollectionConfig::default();
    let ctx = CollectionContext {
        meta: meta.clone().into(),
        store: store.clone(),
        cache: cache.clone(),
        lance: LanceEnv::new(store.clone(), LanceConfig::default()),
        manifests: ManifestCache::new(config.manifest_cache_entries),
        config,
    };
    let factory = Arc::new(CollectionTargetFactory::new(ctx.clone()));
    let registry = TargetRegistry::new()
        .with(Arc::new(CounterTargetFactory::new(
            store.clone(),
            MAX_COMMIT_DELAY,
        )))
        .with(factory.clone());
    let app = operon::api::router(operon::api::AppState {
        meta: meta.clone().into(),
        writer: writer.clone(),
        reader: reader.clone(),
        store: store.clone(),
        registry: registry.clone(),
        collections: operon_query::CollectionService::new(
            ctx.clone(),
            operon_collection::CollectionWriter::new(meta.clone(), writer.clone()),
            reader.clone(),
            operon_query::ServiceConfig::default(),
        ),
        hot: None,
        placement: Arc::new(operon_hot::AlwaysLocal),
        node_id: 1,
        internal: reqwest::Client::new(),
        hot_pin_all: false,
        roles: operon_hot::Roles::all(),
        forwarded: None,
        forward_stats: Arc::default(),
        node_info: None,
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let http = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let api = Api {
        base,
        http: reqwest::Client::new(),
    };

    let ns = meta.create_namespace("acme").await.unwrap();
    let schema = CollectionSchema::new(vec![], vec![], DynamicMapping::Ignore);
    let (cid, stream, _) = meta.create_collection(ns, "docs", schema, 2).await.unwrap();
    let path = format!(
        "/v1/namespaces/acme/links/{}",
        operon_meta::implicit_name("docs", cid)
    );
    let (status, body) = api.get(&path).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["target"]["kind"], json!("collection"));
    assert_eq!(body["version"], json!(0));
    assert_eq!(body["applied"], json!([]));
    assert!(body.get("counters").is_none(), "{body}");

    let ops = (0..6)
        .map(|n| {
            DocOp::Upsert(Document {
                pk: PrimaryKey::U64(n),
                source: serde_json::Map::from_iter([("n".to_string(), json!(n))]),
                vectors: Default::default(),
                sparse_vectors: Default::default(),
            })
        })
        .collect();
    CollectionWriter::new(meta.clone(), writer.clone())
        .write(ns, cid, ops)
        .await
        .unwrap();
    let source = LinkApplySource::new(
        meta.clone(),
        reader,
        registry,
        LinkConfig {
            batch_interval: Duration::ZERO,
            ..LinkConfig::default()
        },
    );
    operon_worker::run_once(&meta, "w1", Duration::from_secs(5), &source)
        .await
        .unwrap();
    let hwms: Vec<Value> = meta
        .read(Consistency::Linearizable, |s| {
            (0..2)
                .filter_map(|p| {
                    let hwm = s.partition(stream, p)?.high_watermark();
                    (hwm > 0).then(|| json!({ "partition": p, "offset": hwm }))
                })
                .collect()
        })
        .await
        .unwrap();
    let (status, body) = api.get(&path).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["version"], json!(1));
    assert_eq!(body["applied"], Value::from(hwms));

    http.abort();
    writer.shutdown().await.unwrap();
    factory.close().await;
    cache.close().await.unwrap();
    node.shutdown().await.unwrap();
}

/// Failpoints exist only in builds with the `failpoints` feature; a normal
/// build refuses to run with them requested rather than ignoring them.
#[cfg(not(feature = "failpoints"))]
#[test]
fn a_build_without_failpoints_refuses_to_arm_them() {
    let dir = TempDir::new().unwrap();
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_operon"))
        .args([
            "dev",
            "--listen",
            "127.0.0.1:0",
            "--flight-sql-listen",
            "127.0.0.1:0",
        ])
        .arg("--data-dir")
        .arg(dir.path())
        .env("OPERON_FAILPOINTS", "wal.after_put")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn operon");
    // A binary built with failpoints would ignore the variable and serve
    // forever: bound the wait instead of hanging the test run.
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait().expect("wait for operon") {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "the operon binary did not exit within 30 s with OPERON_FAILPOINTS set; \
                 it was probably built with --features failpoints: \
                 run \"cargo build -p operon\" and retry"
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(!status.success());
}

/// Ruling 17 (plan M1.2 Task 11 rule 3): every `ApplyError` variant's
/// status and code, and `Retry-After: 1` on every 503.
#[test]
fn meta_conflicts_map_to_409_and_stale_objects_to_503() {
    use axum::response::IntoResponse;
    use operon::api::ApiError;
    use operon_common::meta::LinkId;
    use operon_common::meta::{ApplyError, MetaError};
    use operon_common::{CollectionId, NamespaceId, StreamId};

    let stream = StreamId(3);
    let table: Vec<(ApplyError, u16, &str)> = vec![
        (
            ApplyError::InvalidArgument("x".into()),
            400,
            "invalid_argument",
        ),
        (
            ApplyError::NamespaceExists(NamespaceId(1)),
            409,
            "already_exists",
        ),
        (
            ApplyError::NamespaceNotFound(NamespaceId(1)),
            404,
            "not_found",
        ),
        (ApplyError::StreamExists(stream), 409, "already_exists"),
        (ApplyError::StreamNotFound(stream), 404, "not_found"),
        (ApplyError::LinkExists(LinkId(4)), 409, "already_exists"),
        (
            ApplyError::PartitionNotFound {
                stream,
                partition: 1,
            },
            404,
            "not_found",
        ),
        (
            ApplyError::LeaseHeld {
                owner: "o".into(),
                deadline_ms: 5,
            },
            409,
            "conflict",
        ),
        (ApplyError::LeaseLost { key: "k".into() }, 409, "conflict"),
        (
            ApplyError::VersionMismatch { current: None },
            409,
            "conflict",
        ),
        (ApplyError::Fenced { lease: "l".into() }, 409, "conflict"),
        (
            ApplyError::IndexMismatch {
                stream,
                partition: 0,
            },
            503,
            "unavailable",
        ),
        (
            ApplyError::StaleCommit { object: "w".into() },
            503,
            "unavailable",
        ),
        (
            ApplyError::StaleObject {
                object: "o".into(),
                created_at_ms: 1,
                max_age_ms: 2,
                clock_ms: 3,
            },
            503,
            "unavailable",
        ),
        (
            ApplyError::CollectionExists(CollectionId(5)),
            409,
            "already_exists",
        ),
        (
            ApplyError::CollectionNotFound(CollectionId(5)),
            404,
            "not_found",
        ),
        (ApplyError::NameTaken("kb".into()), 409, "already_exists"),
        (
            ApplyError::IncompatibleSchema("x".into()),
            400,
            "invalid_argument",
        ),
        (
            ApplyError::SchemaVersionMismatch {
                collection: CollectionId(5),
                current: 2,
            },
            409,
            "conflict",
        ),
        (ApplyError::UnknownCollection("kb".into()), 404, "not_found"),
    ];
    for (apply, status, code) in table {
        let label = format!("{apply:?}");
        let err = ApiError::from(MetaError::Rejected(apply));
        assert_eq!(err.status().as_u16(), status, "{label}");
        assert_eq!(err.code(), code, "{label}");
        let response = err.into_response();
        let retry = response.headers().get("retry-after");
        if status == 503 {
            assert_eq!(retry.map(|v| v.as_bytes()), Some(&b"1"[..]), "{label}");
        } else {
            assert!(retry.is_none(), "{label}");
        }
    }
    // M0's 503s get the header too.
    let response = ApiError::from(MetaError::Timeout).into_response();
    assert_eq!(response.status().as_u16(), 503);
    assert_eq!(response.headers()["retry-after"], "1");
}

/// Ruling 17 (rule 4): a record produced onto an implicit stream must be
/// keyed by a primary key of the partition it is produced to.
#[tokio::test]
async fn a_record_for_the_wrong_partition_of_an_implicit_stream_is_rejected() {
    use operon_collection::{PrimaryKey, partition_of};

    let dir = TempDir::new().unwrap();
    let server = Server::start(config(&dir, lazy_segmenter())).await.unwrap();
    let api = Api::new(&server);
    let (status, info) = api
        .post(
            "/v1/namespaces/acme/collections",
            json!({"name": "kb", "schema": {"fields": [], "vectors": [], "dynamic": "ignore"}, "partitions": 2}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{info}");
    let id = info["id"].as_u64().unwrap();
    let path =
        |p: u32| format!("/v1/namespaces/acme/streams/_collection.kb.{id}/partitions/{p}/records");
    let pk = PrimaryKey::U64(1);
    let home = partition_of(&pk, 2);
    let key = BASE64.encode(pk.canonical());
    let value = BASE64.encode(b"x");

    let (status, body) = api
        .post(
            &path(1 - home),
            json!({"records": [{"key": key, "value": value}]}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"], "invalid_argument");
    assert_eq!(
        body["message"],
        format!("record 0: key does not belong to partition {}", 1 - home)
    );
    // A missing key, or one that is no primary key, is refused the same way.
    for record in [
        json!({"value": value}),
        json!({"key": BASE64.encode(b"\xffjunk"), "value": value}),
    ] {
        let (status, body) = api
            .post(
                &path(home),
                json!({"records": [{"key": key, "value": value}, record]}),
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(
            body["message"],
            format!("record 1: key does not belong to partition {home}")
        );
    }
    // The key's own partition is accepted, with a token header.
    let response = reqwest::Client::new()
        .post(format!("http://{}{}", server.local_addr(), path(home)))
        .json(&json!({"records": [{"key": key, "value": value}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().contains_key("operon-consistency-token"));
    server.shutdown().await.unwrap();
}

/// Rule 6 (M1.6 W14, M1.7 A4): the dev binary prints the Flight SQL line
/// once its listener is bound.
#[test]
fn the_dev_binary_prints_the_flight_sql_line() {
    use std::io::BufRead;

    let dir = TempDir::new().unwrap();
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_operon"))
        .args([
            "dev",
            "--listen",
            "127.0.0.1:0",
            "--flight-sql-listen",
            "127.0.0.1:0",
        ])
        .arg("--data-dir")
        .arg(dir.path())
        .env("RUST_LOG", "warn")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn operon");
    let stdout = child.stdout.take().expect("stdout");
    let (lines_tx, lines_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in std::io::BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            if lines_tx.send(line).is_err() {
                break;
            }
        }
    });
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut http = None;
    let mut flight = None;
    while flight.is_none() {
        let left = deadline.saturating_duration_since(Instant::now());
        let Ok(line) = lines_rx.recv_timeout(left) else {
            let _ = child.kill();
            let _ = child.wait();
            panic!("no Flight SQL line within 60 s (http: {http:?})");
        };
        if let Some(addr) = line.strip_prefix("operon listening on http://") {
            http = Some(addr.to_string());
        } else if let Some(addr) = line.strip_prefix("operon flight sql listening on grpc://") {
            flight = Some(addr.to_string());
        }
    }
    let _ = child.kill();
    let _ = child.wait();
    assert!(http.is_some(), "the HTTP line comes first");
    let flight: SocketAddr = flight.unwrap().parse().expect("an address");
    assert!(flight.ip().is_loopback());
    assert_ne!(flight.port(), 0, "the bound port, not the requested one");
}
