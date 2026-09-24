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

/// Failpoints exist only in builds with the `failpoints` feature; a normal
/// build refuses to run with them requested rather than ignoring them.
#[cfg(not(feature = "failpoints"))]
#[test]
fn a_build_without_failpoints_refuses_to_arm_them() {
    let dir = TempDir::new().unwrap();
    let status = std::process::Command::new(env!("CARGO_BIN_EXE_operon"))
        .args(["dev", "--listen", "127.0.0.1:0"])
        .arg("--data-dir")
        .arg(dir.path())
        .env("OPERON_FAILPOINTS", "wal.after_put")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .expect("run operon");
    assert!(!status.success());
}
