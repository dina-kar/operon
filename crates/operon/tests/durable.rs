//! The durable listener in `operon` (D1 Task 3): `operon dev` serves the
//! embedded Resonate server, it starts after the metastore and before the
//! collection service, and it stops after Flight SQL and before the
//! collection service and the metastore (rows T0-6, X8).
//!
//! Run with `cargo test -p operon --features durable`.

#![cfg(feature = "durable")]

use std::io::BufRead;
use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use operon::{Server, ServerConfig};
use operon_durable::DurableConfig;
use tempfile::TempDir;
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::{Context, SubscriberExt};

/// A loopback address nothing listens on (probed, then released).
fn free_addr() -> SocketAddr {
    let probe = TcpListener::bind("127.0.0.1:0").expect("probe bind");
    probe.local_addr().expect("probe addr")
}

/// An in-process single-node config in `dir`, with the durable server on
/// `durable` and every other listener on an ephemeral port.
fn config(dir: &TempDir, durable: SocketAddr) -> ServerConfig {
    let mut config = ServerConfig::new(dir.path());
    config.listen = SocketAddr::from(([127, 0, 0, 1], 0));
    config.flight_sql = Some(SocketAddr::from(([127, 0, 0, 1], 0)));
    config.log.flush_interval = Duration::from_millis(20);
    config.worker_poll_interval = Duration::from_millis(50);
    let mut durable_config = DurableConfig::sqlite(dir.path().join("durable").join("default.db"));
    durable_config.listen = durable;
    config.durable = Some(durable_config);
    config
}

async fn ready(addr: SocketAddr) -> Option<u16> {
    let response = reqwest::Client::new()
        .get(format!("http://{addr}/ready"))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .ok()?;
    Some(response.status().as_u16())
}

/// A running `operon dev` process, killed on drop.
struct Dev(std::process::Child);

impl Drop for Dev {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Semantics 2 and 5: `operon dev --durable-listen <addr>` serves the
/// durable API there, on the default SQLite store, and prints its line
/// before the HTTP line that harnesses wait for.
#[tokio::test(flavor = "multi_thread")]
async fn dev_serves_durable_on_8001_style_port() {
    let dir = TempDir::new().unwrap();
    let durable = free_addr();
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_operon"))
        .args([
            "dev",
            "--listen",
            "127.0.0.1:0",
            "--flight-sql-listen",
            "127.0.0.1:0",
            "--no-qdrant",
            "--durable-listen",
        ])
        .arg(durable.to_string())
        .arg("--data-dir")
        .arg(dir.path())
        .env("RUST_LOG", "warn")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn operon");
    let stdout = child.stdout.take().expect("stdout");
    let dev = Dev(child);
    let mut lines = std::io::BufReader::new(stdout).lines();
    let mut seen = Vec::new();
    loop {
        let line = lines
            .next()
            .unwrap_or_else(|| panic!("operon exited before listening; printed {seen:?}"))
            .expect("read stdout");
        let http = line.starts_with("operon listening on ");
        seen.push(line);
        if http {
            break;
        }
    }
    std::thread::spawn(move || for _ in lines {});
    let expected = format!("operon durable listening on http://{durable}");
    assert!(
        seen.contains(&expected),
        "no {expected:?} before the HTTP line: {seen:?}"
    );
    assert_eq!(ready(durable).await, Some(200));
    assert!(
        dir.path().join("durable").join("default.db").exists(),
        "the default store is <data-dir>/durable/default.db"
    );
    drop(dev);
}

/// Collects the `phase` of every `operon::shutdown` event, in order.
#[derive(Clone, Default)]
struct Phases(Arc<Mutex<Vec<String>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Phases {
    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        if event.metadata().target() != "operon::shutdown" {
            return;
        }
        struct Phase(Option<String>);
        impl Visit for Phase {
            fn record_str(&mut self, field: &Field, value: &str) {
                if field.name() == "phase" {
                    self.0 = Some(value.to_string());
                }
            }
            fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
                if field.name() == "phase" {
                    self.0 = Some(format!("{value:?}").trim_matches('"').to_string());
                }
            }
        }
        let mut phase = Phase(None);
        event.record(&mut phase);
        if let Some(phase) = phase.0 {
            self.0.lock().expect("phases").push(phase);
        }
    }
}

/// Semantics 4 (T0-6, X8): the durable runtime and then the durable server
/// stop after Flight SQL and before the collection service, the writer flush, the worker and the
/// metastore, and leaves its port and its store free.
#[tokio::test(flavor = "multi_thread")]
async fn stop_order_drains_durable_before_meta() {
    let dir = TempDir::new().unwrap();
    let durable = free_addr();
    let server = Server::start(config(&dir, durable)).await.expect("start");
    assert_eq!(server.durable_addr(), Some(durable));
    assert_eq!(ready(durable).await, Some(200));

    let phases = Phases::default();
    let subscriber = tracing_subscriber::registry().with(phases.clone());
    {
        let _guard = tracing::subscriber::set_default(subscriber);
        server.shutdown().await.expect("shutdown");
    }
    let phases = phases.0.lock().unwrap().clone();
    let at = |name: &str| {
        phases
            .iter()
            .position(|p| p == name)
            .unwrap_or_else(|| panic!("no {name} phase in {phases:?}"))
    };
    // Task 6: the runtime (the SDK) stops first, then the server under it.
    assert!(at("flight") < at("durable_runtime"), "{phases:?}");
    assert!(at("durable_runtime") < at("durable"), "{phases:?}");
    for later in ["collections", "writer", "worker", "hot", "metastore"] {
        assert!(
            at("durable") < at(later),
            "durable after {later}: {phases:?}"
        );
    }
    assert_eq!(ready(durable).await, None, "the durable port still accepts");

    // The port and the store lock are free: the same config starts again.
    let server = Server::start(config(&dir, durable)).await.expect("restart");
    assert_eq!(ready(durable).await, Some(200));
    server.shutdown().await.expect("shutdown");
}

/// Semantics 4: a durable start failure is fatal, with its message, and
/// what started before it (the metastore) is released again.
#[tokio::test(flavor = "multi_thread")]
async fn durable_start_failure_is_fatal() {
    let dir = TempDir::new().unwrap();
    let taken = TcpListener::bind("127.0.0.1:0").unwrap();
    let durable = taken.local_addr().unwrap();
    let err = Server::start(config(&dir, durable))
        .await
        .expect_err("the durable port is taken");
    let message = err.to_string();
    assert!(
        message.contains("--durable-listen") && message.contains("--no-durable"),
        "{message}"
    );
    drop(taken);
    let server = Server::start(config(&dir, durable))
        .await
        .expect("the metastore was released");
    server.shutdown().await.expect("shutdown");
}

async fn get_json(url: String) -> (u16, serde_json::Value) {
    let response = reqwest::Client::new()
        .get(url)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("request");
    let status = response.status().as_u16();
    (status, response.json().await.unwrap_or_default())
}

/// D1 Task 7: the operations API is on the native listener once the node
/// serves (the runtime has started), and absent without durable execution.
#[tokio::test(flavor = "multi_thread")]
async fn operations_routes_serve_with_durable_only() {
    let dir = TempDir::new().unwrap();
    let server = Server::start(config(&dir, free_addr()))
        .await
        .expect("start");
    let base = format!("http://{}", server.local_addr());
    let (status, body) = get_json(format!(
        "{base}/v1/operations/op-00000000000000000000000000"
    ))
    .await;
    assert_eq!(
        (status, body["error"].as_str()),
        (404, Some("not_found")),
        "{body}"
    );
    assert!(
        body["message"]
            .as_str()
            .is_some_and(|m| m.contains("no operation")),
        "the operations route answered, not the fallback: {body}"
    );
    let (status, body) = get_json(format!("{base}/v1/namespaces/default/operations")).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body, serde_json::json!({ "operations": [], "next": null }));
    server.shutdown().await.expect("shutdown");

    // --no-durable: no operations API.
    let dir = TempDir::new().unwrap();
    let mut plain = config(&dir, free_addr());
    plain.durable = None;
    let server = Server::start(plain).await.expect("start");
    let (status, body) = get_json(format!(
        "http://{}/v1/namespaces/default/operations",
        server.local_addr()
    ))
    .await;
    assert_eq!(
        (status, body["message"].as_str()),
        (404, Some("no such route")),
        "{body}"
    );
    server.shutdown().await.expect("shutdown");
}

/// A POST of `body` with `headers`: the status, the headers and the body.
async fn post_json(
    url: String,
    headers: &[(&str, &str)],
    body: serde_json::Value,
) -> (u16, reqwest::header::HeaderMap, serde_json::Value) {
    let mut request = reqwest::Client::new()
        .post(url)
        .timeout(Duration::from_secs(10))
        .json(&body);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = request.send().await.expect("request");
    let status = response.status().as_u16();
    let headers = response.headers().clone();
    (status, headers, response.json().await.unwrap_or_default())
}

/// D1 Task 8: `imports_parquet_and_ndjson` into a real collection through
/// the route. The operation's token makes a read see every document, and
/// the refusals before submit answer 400, 404 and 409.
#[tokio::test(flavor = "multi_thread")]
async fn imports_parquet_and_ndjson() {
    use std::sync::Arc as StdArc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};

    let dir = TempDir::new().unwrap();
    let source = dir.path().join("lake");
    std::fs::create_dir_all(&source).unwrap();
    // 10 Parquet rows in row groups of 4, with their own _id.
    let ids: Vec<String> = (0..10).map(|n| format!("p{n}")).collect();
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("_id", StdArc::new(StringArray::from(ids))),
        (
            "n",
            StdArc::new(Int64Array::from((0..10).collect::<Vec<i64>>())),
        ),
    ];
    let batch = RecordBatch::try_from_iter(columns).unwrap();
    let mut bytes = Vec::new();
    let props = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(4))
        .build();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(&mut bytes, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    std::fs::write(source.join("a.parquet"), bytes).unwrap();
    // 5 NDJSON rows without _id: generated ids.
    let lines: String = (0..5)
        .map(|n| format!("{{\"n\": {}}}\n", 100 + n))
        .collect();
    std::fs::write(source.join("b.ndjson"), lines).unwrap();

    let mut dev = config(&dir, free_addr());
    dev.import_file_sources = true;
    let server = Server::start(dev).await.expect("start");
    let base = format!("http://{}", server.local_addr());
    let (status, _, body) = post_json(
        format!("{base}/v1/namespaces/default/collections"),
        &[],
        serde_json::json!({ "name": "docs",
            "schema": { "fields": [], "vectors": [], "dynamic": "ignore" } }),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    let url = format!("{base}/v1/namespaces/default/collections/docs/import");
    let lake = format!("file://{}/", source.display());

    let mut token = String::new();
    for (format, pattern, rows) in [("parquet", "*.parquet", 10), ("ndjson", "*.ndjson", 5)] {
        let request = serde_json::json!({ "source": lake, "format": format, "pattern": pattern });
        let key = format!("load-{format}");
        let (status, headers, body) =
            post_json(url.clone(), &[("Idempotency-Key", &key)], request.clone()).await;
        assert_eq!(status, 202, "{body}");
        let location = headers["location"].to_str().unwrap().to_string();
        assert_eq!(location, body["location"].as_str().unwrap());
        // An idempotent repeat: 200, the same Location.
        let (status, headers, _) =
            post_json(url.clone(), &[("Idempotency-Key", &key)], request).await;
        assert_eq!(status, 200);
        assert_eq!(headers["location"].to_str().unwrap(), location);
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        let op = loop {
            let (status, op) = get_json(format!("{base}{location}")).await;
            assert_eq!(status, 200, "{op}");
            if op["state"] != "queued" && op["state"] != "running" {
                break op;
            }
            assert!(std::time::Instant::now() < deadline, "{op}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        assert_eq!(op["state"], "succeeded", "{op}");
        assert_eq!(op["result"]["rows_written"], rows, "{op}");
        assert_eq!(op["target"], serde_json::json!({ "collection": "docs" }));
        token = op["result"]["token"].as_str().unwrap().to_string();
        assert!(!token.is_empty(), "{op}");
    }
    // The last token covers the collection's writes: a read at it sees all
    // 15 documents.
    let (status, _, body) = post_json(
        format!("{base}/v1/namespaces/default/collections/docs/documents/count"),
        &[("Operon-Consistency-Token", &token)],
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["count"], 15, "{body}");
    let (status, _, body) = post_json(
        format!("{base}/v1/namespaces/default/collections/docs/documents/get"),
        &[("Operon-Consistency-Token", &token)],
        serde_json::json!({ "ids": ["p7"] }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["documents"][0]["source"]["n"], 7, "{body}");

    // Refusals before submit.
    let bad =
        |source: &str, format: &str| serde_json::json!({ "source": source, "format": format });
    let (status, _, body) = post_json(url.clone(), &[], bad("ftp://x/", "parquet")).await;
    assert_eq!(
        (status, body["error"].as_str()),
        (400, Some("invalid_argument")),
        "{body}"
    );
    let (status, _, body) = post_json(url.clone(), &[], bad(&lake, "xml")).await;
    assert_eq!(status, 400, "{body}");
    let (status, _, body) = post_json(
        format!("{base}/v1/namespaces/default/collections/missing/import"),
        &[],
        bad(&lake, "parquet"),
    )
    .await;
    assert_eq!(
        (status, body["error"].as_str()),
        (404, Some("not_found")),
        "{body}"
    );
    let (status, _, body) = post_json(
        url.clone(),
        &[("Idempotency-Key", "load-parquet")],
        bad(&lake, "ndjson"),
    )
    .await;
    assert_eq!(
        (status, body["error"].as_str()),
        (409, Some("idempotency_key_reused")),
        "{body}"
    );
    server.shutdown().await.expect("shutdown");

    // Without import_file_sources (every mode but dev), file:// is refused.
    let dir = TempDir::new().unwrap();
    let server = Server::start(config(&dir, free_addr()))
        .await
        .expect("start");
    let (status, _, body) = post_json(
        format!(
            "http://{}/v1/namespaces/default/collections/docs/import",
            server.local_addr()
        ),
        &[],
        bad(&lake, "parquet"),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert!(
        body["message"].as_str().unwrap().contains("scheme"),
        "{body}"
    );
    server.shutdown().await.expect("shutdown");
}
