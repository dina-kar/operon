//! The embedded Resonate server on SQLite (D1 Task 2): the listener, the
//! store, in-process calls and what the embed must not do to its host.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use loams_durable::{DurableConfig, DurableError, DurableServer};
use resonate_plugin::{
    ConfigError, ResonateWorker, Settings, WorkerDependencies, WorkerPlugin, axum,
};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A loopback address nothing listens on (probed, then released).
fn free_addr() -> SocketAddr {
    let probe = TcpListener::bind("127.0.0.1:0").expect("probe bind");
    probe.local_addr().expect("probe addr")
}

fn config(dir: &Path) -> DurableConfig {
    let mut config = DurableConfig::sqlite(dir.join("durable").join("default.db"));
    config.listen = free_addr();
    config
}

/// One HTTP/1.1 request; the status, or `None` when nothing accepts.
async fn http(addr: SocketAddr, method: &str, path: &str) -> Option<u16> {
    let mut stream = tokio::net::TcpStream::connect(addr).await.ok()?;
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await.ok()?;
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
        .await
        .ok()?
        .ok()?;
    let text = String::from_utf8_lossy(&response);
    text.split_whitespace().nth(1)?.parse().ok()
}

fn far() -> i64 {
    i64::MAX / 2
}

fn create(id: &str, tags: Value) -> Value {
    json!({
        "kind": "promise.create",
        "data": { "id": id, "timeoutAt": far(), "param": {}, "tags": tags },
    })
}

fn get(id: &str) -> Value {
    json!({ "kind": "promise.get", "data": { "id": id } })
}

#[tokio::test(flavor = "multi_thread")]
async fn start_stop_leaves_no_listener() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = config(dir.path());
    let addr = config.listen;
    let server = DurableServer::start(config, "node-a").await.expect("start");
    assert!(server.ready().await);
    assert_eq!(http(addr, "GET", "/ready").await, Some(200));
    server.stop().await;
    assert_eq!(http(addr, "GET", "/ready").await, None, "still accepting");
    TcpListener::bind(addr).expect("the port is free again after stop");
}

#[tokio::test(flavor = "multi_thread")]
async fn port_in_use_names_flags() {
    let dir = tempfile::tempdir().expect("tempdir");
    let taken = TcpListener::bind("127.0.0.1:0").expect("bind");
    let mut config = config(dir.path());
    config.listen = taken.local_addr().expect("addr");
    let err = DurableServer::start(config, "node-a")
        .await
        .expect_err("the port is taken");
    assert!(matches!(err, DurableError::Bind { .. }), "{err:?}");
    let message = err.to_string();
    assert!(message.contains("--durable-listen"), "{message}");
    assert!(message.contains("--no-durable"), "{message}");
}

#[tokio::test(flavor = "multi_thread")]
async fn non_loopback_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    for addr in [
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 8001)),
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 168, 1, 10), 8001)),
        "[::]:8001".parse().expect("addr"),
    ] {
        let mut config = config(dir.path());
        config.listen = addr;
        let err = DurableServer::start(config, "node-a")
            .await
            .expect_err("non-loopback must be refused");
        assert!(matches!(err, DurableError::NotLoopback { .. }), "{err:?}");
        assert_eq!(
            err.to_string(),
            format!(
                "durable listener must be loopback until authentication is configured (D111); got {addr}"
            )
        );
    }
    // Nothing was created for a refused start.
    assert!(!dir.path().join("durable").exists());
}

#[test]
fn loopback_names_parse() {
    for (text, expect) in [
        ("127.0.0.1:8001", "127.0.0.1:8001"),
        ("127.3.2.1:9", "127.3.2.1:9"),
        ("[::1]:8001", "[::1]:8001"),
        ("localhost:8001", "127.0.0.1:8001"),
    ] {
        let parsed = loams_durable::parse_listen(text).expect(text);
        assert_eq!(parsed.to_string(), expect);
    }
    for text in ["0.0.0.0:8001", "10.0.0.1:8001"] {
        let err = loams_durable::parse_listen(text).expect_err(text);
        assert!(
            err.to_string()
                .starts_with("durable listener must be loopback until authentication"),
            "{err}"
        );
    }
    // A name other than localhost is never resolved.
    assert!(loams_durable::parse_listen("example.com:8001").is_err());
    assert!(loams_durable::parse_listen("not an address").is_err());
}

/// A `resonate:target` over http:// is never delivered without `push`.
#[tokio::test(flavor = "multi_thread")]
async fn push_is_off_by_default() {
    async fn deliveries(push: bool) -> bool {
        let dir = tempfile::tempdir().expect("tempdir");
        let hook = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("hook");
        let target = format!("http://{}/hook", hook.local_addr().expect("addr"));
        let mut config = config(dir.path());
        config.push = push;
        let server = DurableServer::start(config, "node-a").await.expect("start");
        let created = server
            .process(create("push-probe", json!({ "resonate:target": target })))
            .await
            .expect("create");
        assert!(created["head"]["status"].as_i64().is_some(), "{created}");
        let wait = if push { 10 } else { 2 };
        let got = tokio::time::timeout(Duration::from_secs(wait), hook.accept())
            .await
            .is_ok();
        server.stop().await;
        got
    }
    assert!(!deliveries(false).await, "push is off by default");
    assert!(deliveries(true).await, "push = true delivers");
}

#[tokio::test(flavor = "multi_thread")]
async fn in_process_create_is_idempotent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let server = DurableServer::start(config(dir.path()), "node-a")
        .await
        .expect("start");
    let first = server
        .process(create("op-idem", json!({ "loams:op": "op-idem" })))
        .await
        .expect("first create");
    let second = server
        .process(create("op-idem", json!({ "loams:op": "op-idem" })))
        .await
        .expect("second create");
    assert_eq!(first["kind"], "promise.create");
    assert_eq!(first["data"]["promise"], second["data"]["promise"]);
    assert_eq!(first["data"]["promise"]["id"], "op-idem");
    assert_eq!(first["data"]["promise"]["state"], "pending");
    // A non-2xx answer is a protocol error, with the status.
    let err = server
        .process(get("op-missing"))
        .await
        .expect_err("a missing promise");
    assert!(
        matches!(err, DurableError::Protocol { status: 404, .. }),
        "{err:?}"
    );
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn state_survives_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let server = DurableServer::start(config(dir.path()), "node-a")
        .await
        .expect("start");
    let created = server
        .process(create("op-restart", json!({})))
        .await
        .expect("create");
    server.stop().await;
    let server = DurableServer::start(config(dir.path()), "node-a")
        .await
        .expect("restart");
    let got = server.process(get("op-restart")).await.expect("get");
    assert_eq!(got["data"]["promise"], created["data"]["promise"]);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn second_process_on_same_store_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let first = DurableServer::start(config(dir.path()), "node-a")
        .await
        .expect("start");
    // A second open file description on the lock conflicts exactly as a
    // second process's would (flock semantics).
    let err = DurableServer::start(config(dir.path()), "node-b")
        .await
        .expect_err("the store is locked");
    let path = dir.path().join("durable").join("default.db");
    assert_eq!(
        err.to_string(),
        format!(
            "the durable store {} is in use by another process",
            path.display()
        )
    );
    first.stop().await;
    // Released on stop.
    let again = DurableServer::start(config(dir.path()), "node-b")
        .await
        .expect("the lock is released on stop");
    again.stop().await;
}

/// A worker plugin that only registers a route whose handler panics.
static PANIC_ROUTE: WorkerPlugin =
    WorkerPlugin::new("test-panic-route", &["panic-test"], panic_route);

#[allow(clippy::unnecessary_wraps)]
fn panic_route(
    _settings: &Settings<'_>,
    deps: WorkerDependencies,
) -> Result<Option<Arc<dyn ResonateWorker>>, ConfigError> {
    deps.routes.add("test_panic_route", |_auth| {
        axum::Router::new().route(
            "/test/panic",
            axum::routing::get(|| async {
                if true {
                    panic!("a handler panicked on purpose");
                }
                "unreachable"
            }),
        )
    });
    Ok(None)
}

#[tokio::test(flavor = "multi_thread")]
async fn handler_panic_answers_500() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = config(dir.path());
    let addr = config.listen;
    let server = DurableServer::start_with_plugins(config, "node-a", &[&PANIC_ROUTE])
        .await
        .expect("start");
    assert_eq!(http(addr, "GET", "/test/panic").await, Some(500));
    // The process lives and keeps serving.
    assert_eq!(http(addr, "GET", "/ready").await, Some(200));
    assert!(
        server
            .process(create("after-panic", json!({})))
            .await
            .is_ok()
    );
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn no_global_subscriber_installed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let server = DurableServer::start(config(dir.path()), "node-a")
        .await
        .expect("start");
    tracing::subscriber::set_global_default(tracing_subscriber::registry())
        .expect("the embed installed no global subscriber");
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn overrides_cannot_touch_bind_or_abort() {
    let dir = tempfile::tempdir().expect("tempdir");
    for (key, value) in [
        ("gateways.gateway_http.bind", "\"0.0.0.0:8001\""),
        ("gateways.gateway_http.abort_on_panic", "true"),
        ("servers.active", "\"server_mysql\""),
        ("gateways.gateway_http.auth.publickey", "\"/k.pem\""),
        ("gateways.gateway_http.workos.api_key", "\"sk\""),
        // A table over a protected key is the same assignment.
        ("gateways.gateway_http", "{ bind = \"0.0.0.0:8001\" }"),
        ("gateways", "{ gateway_http = { abort_on_panic = true } }"),
        (" gateways . gateway_http . bind ", "\"0.0.0.0:1\""),
        ("gateways.\"gateway_http\".bind", "\"0.0.0.0:1\""),
        // Owned by other flags.
        ("workers.transport_http_push.enabled", "true"),
        ("servers.server_sqlite.path", "\"/elsewhere.db\""),
        // The process section is not read by the embed.
        ("debug", "true"),
    ] {
        let mut config = config(dir.path());
        config.overrides = vec![(key.to_string(), value.to_string())];
        let err = DurableServer::start(config, "node-a").await.expect_err(key);
        assert!(matches!(err, DurableError::Config(_)), "{key}: {err:?}");
    }
    // A key no plugin reads fails too, naming it.
    let mut bad = config(dir.path());
    bad.overrides = vec![(
        "servers.server_sqlite.no_such_key".to_string(),
        "1".to_string(),
    )];
    let err = DurableServer::start(bad, "node-a")
        .await
        .expect_err("unknown");
    assert!(err.to_string().contains("no_such_key"), "{err}");
    // A plugin setting that is not protected applies.
    let mut good = config(dir.path());
    good.overrides = vec![(
        "servers.server_sqlite.preload_limit".to_string(),
        "5".to_string(),
    )];
    let server = DurableServer::start(good, "node-a").await.expect("start");
    server.stop().await;
}

#[cfg(not(feature = "mysql"))]
#[tokio::test(flavor = "multi_thread")]
async fn mysql_store_needs_the_feature() {
    let mut config = DurableConfig::new(loams_durable::DurableStore::Mysql {
        url: "mysql://root@127.0.0.1:4000/loams_durable_default".into(),
        tls: loams_durable::MysqlTls::Disabled,
    });
    config.listen = free_addr();
    let err = DurableServer::start(config, "node-a")
        .await
        .expect_err("no mysql plugin in this build");
    assert!(
        err.to_string().contains("durable-mysql feature is off"),
        "{err}"
    );
}
