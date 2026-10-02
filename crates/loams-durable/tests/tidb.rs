//! The TiDB backend (D1 Task 4): Resonate's MySQL plugin against a real
//! TiDB, `loams durable migrate`, and the pessimistic session pin of the
//! fork (PR 1).
//!
//! Every test skips without `LOAMS_TEST_TIDB`, a MySQL URL for an admin user
//! (`scripts/durable/tidb.sh up` prints one). Each test creates its own
//! database and drops it at the end.
#![cfg(feature = "mysql")]

use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use loams_durable::{DurableConfig, DurableError, DurableServer, DurableStore};
use serde_json::{Value, json};
use sqlx::{Connection, MySqlConnection};

/// The admin URL, or `None` (and the skip line) without one.
fn admin_url(test: &str) -> Option<String> {
    match std::env::var("LOAMS_TEST_TIDB") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            println!("skipped: {test} needs LOAMS_TEST_TIDB");
            None
        }
    }
}

async fn admin(url: &str) -> MySqlConnection {
    MySqlConnection::connect(url).await.expect("admin connect")
}

/// A database of the test's own, created empty and dropped by [`Db::drop`].
struct Db {
    admin: String,
    name: String,
    url: String,
}

impl Db {
    async fn fresh(admin_url: &str, test: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .subsec_nanos();
        let name = format!("loams_t_{test}_{}_{nanos}", std::process::id());
        let mut conn = admin(admin_url).await;
        sqlx::raw_sql(&format!("CREATE DATABASE `{name}`"))
            .execute(&mut conn)
            .await
            .expect("create database");
        let mut url = url::Url::parse(admin_url).expect("LOAMS_TEST_TIDB is a URL");
        url.set_path(&format!("/{name}"));
        Self {
            admin: admin_url.to_string(),
            name,
            url: url.to_string(),
        }
    }

    fn store(&self) -> DurableStore {
        DurableStore::mysql(&self.url).expect("a mysql store")
    }

    fn config(&self) -> DurableConfig {
        let mut config = DurableConfig::new(self.store());
        config.listen = free_addr();
        config
    }

    async fn tables(&self) -> i64 {
        let mut conn = admin(&self.admin).await;
        sqlx::query_scalar("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ?")
            .bind(&self.name)
            .fetch_one(&mut conn)
            .await
            .expect("count tables")
    }

    async fn exec(&self, sql: &str) {
        let mut conn = MySqlConnection::connect(&self.url).await.expect("connect");
        sqlx::raw_sql(sql).execute(&mut conn).await.expect(sql);
    }

    async fn drop(self) {
        let mut conn = admin(&self.admin).await;
        sqlx::raw_sql(&format!("DROP DATABASE IF EXISTS `{}`", self.name))
            .execute(&mut conn)
            .await
            .expect("drop database");
    }
}

/// A loopback address nothing listens on (probed, then released).
fn free_addr() -> SocketAddr {
    let probe = TcpListener::bind("127.0.0.1:0").expect("probe bind");
    probe.local_addr().expect("probe addr")
}

fn far() -> i64 {
    i64::MAX / 2
}

fn create(id: &str) -> Value {
    json!({
        "kind": "promise.create",
        "data": { "id": id, "timeoutAt": far(), "param": {}, "tags": {} },
    })
}

fn get(id: &str) -> Value {
    json!({ "kind": "promise.get", "data": { "id": id } })
}

fn settle(id: &str, state: &str, value: &str) -> Value {
    json!({
        "kind": "promise.settle",
        "data": { "id": id, "state": state, "value": { "data": value } },
    })
}

async fn migrated(db: &Db) -> DurableServer {
    DurableServer::migrate(db.store()).await.expect("migrate");
    DurableServer::start(db.config(), "node-a")
        .await
        .expect("start")
}

#[tokio::test(flavor = "multi_thread")]
async fn migrate_then_serve() {
    let Some(admin_url) = admin_url("migrate_then_serve") else {
        return;
    };
    let db = Db::fresh(&admin_url, "mts").await;
    DurableServer::migrate(db.store()).await.expect("migrate");
    assert!(db.tables().await > 0, "migrate created the schema");
    // Migrating an up-to-date schema is a no-op.
    DurableServer::migrate(db.store())
        .await
        .expect("migrate again");
    let server = DurableServer::start(db.config(), "node-a")
        .await
        .expect("serve a migrated store");
    assert!(server.ready().await);
    let created = server.process(create("op-tidb")).await.expect("create");
    assert_eq!(created["data"]["promise"]["state"], "pending");
    let got = server.process(get("op-tidb")).await.expect("get");
    assert_eq!(got["data"]["promise"], created["data"]["promise"]);
    server.stop().await;
    db.drop().await;
}

/// Owner ruling Q8: a verifying `ssl-mode` really verifies. The playground's
/// TiDB serves a self-signed certificate: `required` connects (TLS works),
/// while `verify_identity` against the driver's roots is refused before any
/// schema is made.
#[tokio::test(flavor = "multi_thread")]
async fn verify_identity_refuses_an_untrusted_certificate() {
    let Some(admin_url) = admin_url("verify_identity_refuses_an_untrusted_certificate") else {
        return;
    };
    let db = Db::fresh(&admin_url, "vid").await;
    let with_mode = |mode: &str| {
        DurableStore::mysql(&format!("{}?ssl-mode={mode}", db.url)).expect("a mysql store")
    };
    let err = DurableServer::migrate(with_mode("verify_identity"))
        .await
        .expect_err("a self-signed certificate is not trusted");
    let message = err.to_string();
    assert!(
        message.to_ascii_lowercase().contains("certificate"),
        "{message}"
    );
    assert_eq!(db.tables().await, 0, "nothing was migrated");
    DurableServer::migrate(with_mode("required"))
        .await
        .expect("TLS without verification connects");
    assert!(db.tables().await > 0);
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unmigrated_schema_names_the_command() {
    let Some(admin_url) = admin_url("unmigrated_schema_names_the_command") else {
        return;
    };
    // An empty database: serving never creates the schema.
    let db = Db::fresh(&admin_url, "ums").await;
    let err = DurableServer::start(db.config(), "node-a")
        .await
        .expect_err("an empty database is not served");
    assert!(matches!(err, DurableError::Start(_)), "{err:?}");
    let message = err.to_string();
    assert!(
        message.contains("run 'loams durable migrate' first"),
        "{message}"
    );
    assert_eq!(db.tables().await, 0, "serving must not migrate");

    // A schema the binary does not match (the initial migration was edited
    // after this database was created): Resonate's message and the command.
    DurableServer::migrate(db.store()).await.expect("migrate");
    db.exec("UPDATE _sqlx_migrations SET checksum = x'00'")
        .await;
    let err = DurableServer::start(db.config(), "node-a")
        .await
        .expect_err("a stale schema is not served");
    let message = err.to_string();
    assert!(message.contains("different checksum"), "{message}");
    assert!(message.contains("loams durable migrate"), "{message}");
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn state_survives_restart_on_tidb() {
    let Some(admin_url) = admin_url("state_survives_restart_on_tidb") else {
        return;
    };
    let db = Db::fresh(&admin_url, "ssr").await;
    let server = migrated(&db).await;
    let created = server.process(create("op-restart")).await.expect("create");
    server
        .process(settle("op-restart", "resolved", "YQ=="))
        .await
        .expect("settle");
    server.stop().await;
    let server = DurableServer::start(db.config(), "node-a")
        .await
        .expect("restart");
    let got = server.process(get("op-restart")).await.expect("get");
    assert_eq!(
        got["data"]["promise"]["id"],
        created["data"]["promise"]["id"]
    );
    assert_eq!(got["data"]["promise"]["state"], "resolved");
    assert_eq!(got["data"]["promise"]["value"]["data"], "YQ==");
    server.stop().await;
    db.drop().await;
}

/// What a 2xx answer or an error says about one call.
enum Outcome {
    Ok(Value),
    Refused(u16),
    Unavailable,
}

async fn call(server: &DurableServer, req: Value) -> Outcome {
    match server.process(req).await {
        Ok(response) => Outcome::Ok(response),
        Err(DurableError::Protocol { status, .. }) => Outcome::Refused(status),
        Err(DurableError::Unavailable(_)) => Outcome::Unavailable,
        Err(other) => panic!("unexpected: {other}"),
    }
}

const VALUES: [&str; 8] = [
    "YQ==", "Yg==", "Yw==", "ZA==", "ZQ==", "Zg==", "Zw==", "aA==",
];

/// Concurrent `promise.create` and `promise.settle` on 4 ids through two
/// servers on one database. Returns how many calls answered 503 or not at
/// all, and checks every final state:
/// - both servers read the same settled promise;
/// - every 2xx settle answered with that promise (the first settle wins,
///   a later one sees the winner);
/// - every 2xx create answered with the same id.
async fn contend(a: Arc<DurableServer>, b: Arc<DurableServer>, prefix: &str) -> usize {
    let ids: Vec<String> = (0..4).map(|i| format!("{prefix}-{i}")).collect();
    let mut calls = Vec::new();
    for id in &ids {
        for (n, value) in VALUES.into_iter().enumerate() {
            let server = if n % 2 == 0 {
                Arc::clone(&a)
            } else {
                Arc::clone(&b)
            };
            let id = id.clone();
            calls.push(tokio::spawn(async move {
                let created = call(&server, create(&id)).await;
                let state = if n % 3 == 0 { "rejected" } else { "resolved" };
                let settled = call(&server, settle(&id, state, value)).await;
                (id, created, settled)
            }));
        }
    }
    let mut unavailable = 0;
    let mut settles: Vec<(String, Value)> = Vec::new();
    for handle in calls {
        let (id, created, settled) = handle.await.expect("task");
        match created {
            Outcome::Ok(response) => assert_eq!(response["data"]["promise"]["id"], id.as_str()),
            Outcome::Refused(503) | Outcome::Unavailable => unavailable += 1,
            Outcome::Refused(status) => panic!("create {id}: {status}"),
        }
        match settled {
            Outcome::Ok(response) => settles.push((id, response["data"]["promise"].clone())),
            Outcome::Refused(503) | Outcome::Unavailable => unavailable += 1,
            // A settle that ran before any create committed.
            Outcome::Refused(404) => {}
            Outcome::Refused(status) => panic!("settle {id}: {status}"),
        }
    }
    for id in &ids {
        let from_a = a.process(get(id)).await.expect("get on a");
        let from_b = b.process(get(id)).await.expect("get on b");
        let promise = &from_a["data"]["promise"];
        assert_eq!(
            promise, &from_b["data"]["promise"],
            "{id}: both servers agree"
        );
        let state = promise["state"].as_str().expect("state");
        assert!(
            ["resolved", "rejected", "pending"].contains(&state),
            "{id}: {state}"
        );
        for (settled_id, answer) in settles.iter().filter(|(s, _)| s == id) {
            assert_eq!(answer["state"], promise["state"], "{settled_id}");
            assert_eq!(answer["value"], promise["value"], "{settled_id}");
        }
        if settles.iter().any(|(s, _)| s == id) {
            assert_ne!(state, "pending", "{id}: a settle answered 2xx");
        }
    }
    unavailable
}

#[tokio::test(flavor = "multi_thread")]
async fn two_servers_one_database_are_linearizable_smoke() {
    let Some(admin_url) = admin_url("two_servers_one_database_are_linearizable_smoke") else {
        return;
    };
    let db = Db::fresh(&admin_url, "two").await;
    let a = Arc::new(migrated(&db).await);
    let b = Arc::new(
        DurableServer::start(db.config(), "node-b")
            .await
            .expect("second server"),
    );
    let unavailable = contend(Arc::clone(&a), Arc::clone(&b), "lin").await;
    println!("two servers: {unavailable} calls answered 503 or nothing");
    for server in [a, b] {
        Arc::into_inner(server).expect("one owner").stop().await;
    }
    db.drop().await;
}

/// The fork pins `tidb_txn_mode = 'pessimistic'` on every pooled connection
/// (PR 1). With the cluster default switched to optimistic, a new plain
/// session is optimistic, yet the server's sessions are not: the pin ran on
/// them, and concurrent settles on one row never fail with a write conflict.
#[tokio::test(flavor = "multi_thread")]
async fn optimistic_cluster_still_pessimistic_sessions() {
    let Some(admin_url) = admin_url("optimistic_cluster_still_pessimistic_sessions") else {
        return;
    };
    let db = Db::fresh(&admin_url, "opt").await;
    let mut root = admin(&admin_url).await;
    let version: String = sqlx::query_scalar("SELECT VERSION()")
        .fetch_one(&mut root)
        .await
        .expect("version");
    if !version.contains("TiDB") {
        println!(
            "skipped: optimistic_cluster_still_pessimistic_sessions needs TiDB, not {version}"
        );
        db.drop().await;
        return;
    }
    sqlx::raw_sql("SET GLOBAL tidb_txn_mode = 'optimistic'")
        .execute(&mut root)
        .await
        .expect("set global optimistic");
    let url = db.url.clone();
    let name = db.name.clone();
    let config_a = db.config();
    let config_b = db.config();
    let store = db.store();
    // In a task, so a failed assertion still restores the global mode.
    let result = tokio::spawn(async move {
        let mut plain = MySqlConnection::connect(&url).await.expect("connect");
        let mode: String = sqlx::query_scalar("SELECT @@tidb_txn_mode")
            .fetch_one(&mut plain)
            .await
            .expect("mode");
        assert_eq!(mode, "optimistic", "a new plain session takes the global");

        DurableServer::migrate(store).await.expect("migrate");
        let a = Arc::new(
            DurableServer::start(config_a, "node-a")
                .await
                .expect("start"),
        );
        let b = Arc::new(
            DurableServer::start(config_b, "node-b")
                .await
                .expect("second server"),
        );
        let unavailable = contend(Arc::clone(&a), Arc::clone(&b), "opt").await;

        // The servers' own sessions: TiDB's statement summary counts, per
        // database, the fork's connect statements. Every connection that
        // asked for the version (TiDB) then ran the pin.
        let count = |digest: &'static str| {
            let name = name.clone();
            let url = url.clone();
            async move {
                let mut conn = MySqlConnection::connect(&url).await.expect("connect");
                let n: i64 = sqlx::query_scalar(
                    "SELECT CAST(COALESCE(SUM(EXEC_COUNT), 0) AS SIGNED) \
                     FROM information_schema.cluster_statements_summary \
                     WHERE SCHEMA_NAME = ? AND DIGEST_TEXT = ?",
                )
                .bind(name)
                .bind(digest)
                .fetch_one(&mut conn)
                .await
                .expect("statement summary");
                n
            }
        };
        let connections = count("select `version` ( )").await;
        let pinned = count("set session `tidb_txn_mode` = ?").await;
        assert!(connections > 0, "the servers opened connections");
        assert_eq!(pinned, connections, "every server session ran the pin");
        for server in [a, b] {
            Arc::into_inner(server).expect("one owner").stop().await;
        }
        unavailable
    })
    .await;
    sqlx::raw_sql("SET GLOBAL tidb_txn_mode = 'pessimistic'")
        .execute(&mut root)
        .await
        .expect("restore the global mode");
    let unavailable = match result {
        Ok(outcome) => outcome,
        Err(e) => std::panic::resume_unwind(e.into_panic()),
    };
    assert_eq!(
        unavailable, 0,
        "optimistic sessions would fail concurrent settles with write conflicts (503)"
    );
    db.drop().await;
}
