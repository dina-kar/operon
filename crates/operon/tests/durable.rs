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

/// Semantics 4 (T0-6, X8): the durable server stops after Flight SQL and
/// before the collection service, the writer flush, the worker and the
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
    assert!(at("flight") < at("durable"), "{phases:?}");
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
