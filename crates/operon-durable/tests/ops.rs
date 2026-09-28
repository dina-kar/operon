//! The operations API (D1 Task 7, D146): an operation is a durable promise,
//! submitted with an optional idempotency key, then polled.
//!
//! Each test starts its own server and runtime on its own SQLite store and
//! uses its own counters: the tests share one process. The workflows are
//! test kinds; the real ones (import) come with Task 8.

use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use operon_durable::ops::{check_canceled, fail, map_state};
use operon_durable::{
    DurableConfig, DurableRuntime, DurableServer, OpInput, Operation, OperationId, OperationKinds,
    OperationState, Operations, OpsConfig, OpsError, RuntimeOptions,
};
use serde_json::{Value, json};

/// A loopback address nothing listens on (probed, then released).
fn free_addr() -> SocketAddr {
    let probe = TcpListener::bind("127.0.0.1:0").expect("probe bind");
    probe.local_addr().expect("probe addr")
}

/// A SQLite store in `dir`, with a short retry timeout so an undelivered
/// task comes back within a test's patience.
fn config(dir: &Path) -> DurableConfig {
    let mut config = DurableConfig::sqlite(dir.join("durable").join("default.db"));
    config.listen = free_addr();
    config.retry_timeout = Duration::from_secs(1);
    config
}

/// A short lease, as in the runtime tests.
fn options() -> RuntimeOptions {
    RuntimeOptions {
        ttl: Duration::from_secs(2),
    }
}

fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("ms")
}

const DAY_MS: i64 = 24 * 3600 * 1000;

/// Poll `cond` every 20 ms for up to 30 s.
async fn until(what: &str, cond: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Poll the operation until it is in `state` (30 s at most).
async fn wait_state(ops: &Operations, id: &OperationId, state: OperationState) -> Operation {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let op = ops.get(id).await.expect("get");
        if op.state == state {
            return op;
        }
        assert!(
            Instant::now() < deadline,
            "{id}: still {:?}, waiting for {state:?}",
            op.state
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ─── the test kinds ─────────────────────────────────────────────────────────

#[resonate_sdk::function]
async fn echo_step(x: Value) -> Result<Value> {
    Ok(x)
}

/// Succeeds with its params, after one step.
#[resonate_sdk::function(name = "test.echo")]
async fn echo(ctx: &Context, input: OpInput) -> Result<Value> {
    let params: Value = ctx.run(echo_step, input.params.clone()).await?;
    Ok(json!({ "echo": params }))
}

/// Fails with a code.
#[resonate_sdk::function(name = "test.fail")]
async fn failing(_ctx: &Context, _input: OpInput) -> Result<Value> {
    Err(fail("bad_input", "no good"))
}

/// Never finishes: its one step waits forever.
#[resonate_sdk::function]
async fn stuck_step(_x: u64) -> Result<u64> {
    std::future::pending::<()>().await;
    Ok(0)
}

#[resonate_sdk::function(name = "test.stuck")]
async fn stuck(ctx: &Context, _input: OpInput) -> Result<u64> {
    ctx.run(stuck_step, 0_u64).await
}

static GATED_ENTERED: AtomicBool = AtomicBool::new(false);
static GATED_RELEASE: AtomicBool = AtomicBool::new(false);
static GATED_TWO: AtomicUsize = AtomicUsize::new(0);

/// Step 1 of `test.gated`: holds until the test releases it.
#[resonate_sdk::function]
async fn gated_one(x: u64) -> Result<u64> {
    GATED_ENTERED.store(true, Ordering::SeqCst);
    while !GATED_RELEASE.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(x + 1)
}

#[resonate_sdk::function]
async fn gated_two(x: u64) -> Result<u64> {
    GATED_TWO.fetch_add(1, Ordering::SeqCst);
    Ok(x + 1)
}

/// A branch of `test.gated`, a task of its own (as Task 8's file branches
/// are): settling the root does not stop it, so it checks the root between
/// its two steps.
#[resonate_sdk::function]
async fn gated_branch(ctx: &Context, root: OperationId) -> Result<u64> {
    let a: u64 = ctx.run(gated_one, 0_u64).await?;
    check_canceled(ctx, &root).await?;
    ctx.run(gated_two, a).await
}

#[resonate_sdk::function(name = "test.gated")]
async fn gated(ctx: &Context, input: OpInput) -> Result<u64> {
    ctx.rpc::<u64>("gated_branch", &input.id).await
}

fn kinds() -> OperationKinds {
    OperationKinds::new()
        .kind(echo)
        .kind(failing)
        .kind(stuck)
        .kind(gated)
        .function(echo_step)
        .function(stuck_step)
        .function(gated_branch)
        .function(gated_one)
        .function(gated_two)
}

/// A server, a runtime with the test kinds, and the operations over them.
struct Env {
    _dir: tempfile::TempDir,
    server: DurableServer,
    runtime: Option<DurableRuntime>,
    ops: Operations,
}

impl Env {
    async fn start(config: OpsConfig) -> Self {
        let mut env = Self::without_runtime(config).await;
        env.start_runtime().await;
        env
    }

    async fn without_runtime(config: OpsConfig) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let server = DurableServer::start(self::config(dir.path()), "1")
            .await
            .expect("server");
        let ops = Operations::new(server.client(), server.store().clone(), &kinds(), config);
        Self {
            _dir: dir,
            server,
            runtime: None,
            ops,
        }
    }

    async fn start_runtime(&mut self) {
        let kinds = kinds();
        let runtime =
            DurableRuntime::start_with(&self.server, "1", options(), |sdk| kinds.register(sdk))
                .await
                .expect("runtime");
        self.runtime = Some(runtime);
    }

    async fn stop(self) {
        if let Some(runtime) = self.runtime {
            runtime.stop().await;
        }
        self.server.stop().await;
    }
}

// ─── same_key_same_operation ────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn same_key_same_operation() {
    let env = Env::start(OpsConfig::default()).await;
    let params = json!({ "source": "s3://bucket/a/", "on_error": "fail" });

    let (id, created) = env
        .ops
        .submit("default", "test.echo", params.clone(), Some("load-1"))
        .await
        .expect("submit");
    assert!(created, "the first submit creates the operation");
    assert_eq!(id, OperationId::for_key("default", "load-1"), "D146's id");

    // The same key and parameters, keys in another order: the same one.
    let reordered = json!({ "on_error": "fail", "source": "s3://bucket/a/" });
    let (again, created) = env
        .ops
        .submit("default", "test.echo", reordered, Some("load-1"))
        .await
        .expect("resubmit");
    assert_eq!(again, id);
    assert!(!created, "a repeat does not create another");

    let op = wait_state(&env.ops, &id, OperationState::Succeeded).await;
    assert_eq!(op.kind, "test.echo");
    assert_eq!(op.namespace, "default");
    assert_eq!(op.result, Some(json!({ "echo": params })));
    assert_eq!(op.error, None);
    assert!(
        op.created_at > 0 && op.updated_at >= op.created_at,
        "{op:?}"
    );

    // After it finished, the key still names it.
    let (late, created) = env
        .ops
        .submit("default", "test.echo", params, Some("load-1"))
        .await
        .expect("late resubmit");
    assert_eq!((late, created), (id.clone(), false));

    // Without a key every submit is a new operation.
    let (a, a_created) = env
        .ops
        .submit("default", "test.echo", json!({}), None)
        .await
        .expect("a");
    let (b, b_created) = env
        .ops
        .submit("default", "test.echo", json!({}), None)
        .await
        .expect("b");
    assert!(a_created && b_created && a != b && a != id);
    env.stop().await;
}

// ─── same_key_other_params_conflicts ────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn same_key_other_params_conflicts() {
    let env = Env::start(OpsConfig::default()).await;
    let (id, _) = env
        .ops
        .submit("default", "test.echo", json!({ "n": 1 }), Some("k"))
        .await
        .expect("submit");

    for (kind, params) in [
        ("test.echo", json!({ "n": 2 })),
        ("test.fail", json!({ "n": 1 })),
    ] {
        match env.ops.submit("default", kind, params, Some("k")).await {
            Err(OpsError::IdempotencyKeyReused(reused)) => assert_eq!(reused, id),
            other => panic!("{kind}: expected idempotency_key_reused, got {other:?}"),
        }
    }

    // The same key in another namespace is another operation.
    let (other, created) = env
        .ops
        .submit("other", "test.echo", json!({ "n": 2 }), Some("k"))
        .await
        .expect("other namespace");
    assert!(created);
    assert_ne!(other, id);

    // A key must be usable as one.
    for key in ["", &"x".repeat(256)] {
        assert!(
            matches!(
                env.ops
                    .submit("default", "test.echo", json!({}), Some(key))
                    .await,
                Err(OpsError::Invalid(_))
            ),
            "key of {} bytes",
            key.len()
        );
    }
    env.stop().await;
}

// ─── state_mapping_covers_every_promise_state ───────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn state_mapping_covers_every_promise_state() {
    use OperationState::{Canceled, Failed, Queued, Running, Succeeded};
    // §21 §6.4, every promise state and every task state.
    let table: &[(&str, Option<&str>, OperationState, Option<&str>)] = &[
        ("pending", None, Queued, None),
        ("pending", Some("pending"), Queued, None),
        ("pending", Some("halted"), Queued, None),
        ("pending", Some("acquired"), Running, None),
        ("pending", Some("suspended"), Running, None),
        ("pending", Some("fulfilled"), Running, None),
        ("resolved", Some("fulfilled"), Succeeded, None),
        ("resolved", None, Succeeded, None),
        ("rejected", Some("fulfilled"), Failed, None),
        (
            "rejected_canceled",
            Some("fulfilled"),
            Canceled,
            Some("canceled"),
        ),
        ("rejected_timedout", None, Failed, Some("deadline_exceeded")),
    ];
    for (promise, task, state, code) in table {
        let mapped = map_state(promise, *task).expect("a known state");
        assert_eq!(mapped, (*state, *code), "{promise} / {task:?}");
    }
    assert!(map_state("settled", None).is_err());
    assert!(map_state("pending", Some("sleeping")).is_err());

    // The same states, reached for real. Queued: no runtime takes the task.
    let mut env = Env::without_runtime(OpsConfig::default()).await;
    let (queued, _) = env
        .ops
        .submit("default", "test.echo", json!({ "q": 1 }), None)
        .await
        .expect("submit");
    let op = env.ops.get(&queued).await.expect("get");
    assert_eq!((op.state, op.result, op.error), (Queued, None, None));

    // Running, then succeeded, once a runtime is up.
    env.start_runtime().await;
    let op = wait_state(&env.ops, &queued, Succeeded).await;
    assert_eq!(op.result, Some(json!({ "echo": { "q": 1 } })));
    let (running, _) = env
        .ops
        .submit("default", "test.stuck", json!({}), None)
        .await
        .expect("stuck");
    wait_state(&env.ops, &running, Running).await;

    // Failed, with the workflow's code.
    let (failed, _) = env
        .ops
        .submit("default", "test.fail", json!({}), None)
        .await
        .expect("fail");
    let op = wait_state(&env.ops, &failed, Failed).await;
    let error = op.error.expect("an error");
    assert_eq!(error.code, "bad_input");
    assert!(error.message.contains("no good"), "{error:?}");
    assert_eq!(op.result, None);

    // Canceled.
    env.ops.cancel(&running).await.expect("cancel");
    let op = wait_state(&env.ops, &running, Canceled).await;
    assert_eq!(op.error.expect("an error").code, "canceled");

    // Failed with deadline_exceeded: the root timed out.
    let short = Operations::new(
        env.server.client(),
        env.server.store().clone(),
        &kinds(),
        OpsConfig {
            operation_timeout: Duration::from_secs(1),
            ..OpsConfig::default()
        },
    );
    let (late, _) = short
        .submit("default", "test.stuck", json!({}), None)
        .await
        .expect("stuck");
    let op = wait_state(&short, &late, Failed).await;
    assert_eq!(op.error.expect("an error").code, "deadline_exceeded");
    env.stop().await;
}

// ─── cancel_stops_at_next_step ──────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn cancel_stops_at_next_step() {
    let env = Env::start(OpsConfig::default()).await;
    let (id, _) = env
        .ops
        .submit("default", "test.gated", json!({}), None)
        .await
        .expect("submit");
    until("step 1 to start", || GATED_ENTERED.load(Ordering::SeqCst)).await;
    assert_eq!(
        env.ops.get(&id).await.expect("get").state,
        OperationState::Running
    );

    env.ops.cancel(&id).await.expect("cancel");
    let op = env.ops.get(&id).await.expect("get");
    assert_eq!(op.state, OperationState::Canceled);

    // Step 1 finishes; the workflow must stop before step 2.
    GATED_RELEASE.store(true, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(GATED_TWO.load(Ordering::SeqCst), 0, "step 2 never ran");
    assert_eq!(
        env.ops.get(&id).await.expect("get").state,
        OperationState::Canceled
    );

    // A finished operation cannot be canceled.
    match env.ops.cancel(&id).await {
        Err(OpsError::Finished(finished)) => assert_eq!(finished, id),
        other => panic!("expected operation_finished, got {other:?}"),
    }
    let (done, _) = env
        .ops
        .submit("default", "test.echo", json!({}), None)
        .await
        .expect("echo");
    wait_state(&env.ops, &done, OperationState::Succeeded).await;
    assert!(matches!(
        env.ops.cancel(&done).await,
        Err(OpsError::Finished(_))
    ));
    env.stop().await;
}

// ─── list_filters_by_state_and_pages ────────────────────────────────────────

/// Every page of `list(ns, state)`, in order, following `next`.
async fn list_all(ops: &Operations, ns: &str, state: Option<OperationState>) -> Vec<Operation> {
    let mut all = Vec::new();
    let mut cursor = None;
    for _ in 0..50 {
        let (page, next) = ops.list(ns, state, cursor).await.expect("list");
        assert!(page.len() <= 2, "a page holds at most page_size");
        all.extend(page);
        match next {
            Some(next) => cursor = Some(next),
            None => return all,
        }
    }
    panic!("more than 50 pages");
}

#[tokio::test(flavor = "multi_thread")]
async fn list_filters_by_state_and_pages() {
    let env = Env::start(OpsConfig {
        page_size: 2,
        ..OpsConfig::default()
    })
    .await;
    let mut succeeded = Vec::new();
    for n in 0..5 {
        let (id, _) = env
            .ops
            .submit("lst", "test.echo", json!({ "n": n }), None)
            .await
            .expect("echo");
        succeeded.push(id);
    }
    let (failed, _) = env
        .ops
        .submit("lst", "test.fail", json!({}), None)
        .await
        .expect("fail");
    let (running, _) = env
        .ops
        .submit("lst", "test.stuck", json!({}), None)
        .await
        .expect("stuck");
    let (elsewhere, _) = env
        .ops
        .submit("other", "test.echo", json!({}), None)
        .await
        .expect("elsewhere");
    for id in &succeeded {
        wait_state(&env.ops, id, OperationState::Succeeded).await;
    }
    wait_state(&env.ops, &failed, OperationState::Failed).await;
    wait_state(&env.ops, &running, OperationState::Running).await;

    let ids = |ops: &[Operation]| {
        let mut ids: Vec<OperationId> = ops.iter().map(|op| op.id.clone()).collect();
        ids.sort();
        ids
    };
    let all = list_all(&env.ops, "lst", None).await;
    assert_eq!(all.len(), 7, "{:?}", ids(&all));
    assert!(all.iter().all(|op| op.namespace == "lst"));
    assert!(!ids(&all).contains(&elsewhere));
    let mut unique = ids(&all);
    unique.dedup();
    assert_eq!(unique.len(), 7, "no operation on two pages");

    let mut expected = succeeded.clone();
    expected.sort();
    let done = list_all(&env.ops, "lst", Some(OperationState::Succeeded)).await;
    assert_eq!(ids(&done), expected);
    let fails = list_all(&env.ops, "lst", Some(OperationState::Failed)).await;
    assert_eq!(ids(&fails), vec![failed]);
    let runs = list_all(&env.ops, "lst", Some(OperationState::Running)).await;
    assert_eq!(ids(&runs), vec![running.clone()]);
    assert!(
        list_all(&env.ops, "lst", Some(OperationState::Queued))
            .await
            .is_empty()
    );
    assert!(
        list_all(&env.ops, "lst", Some(OperationState::Canceled))
            .await
            .is_empty()
    );
    env.ops.cancel(&running).await.expect("cancel");
    let canceled = list_all(&env.ops, "lst", Some(OperationState::Canceled)).await;
    assert_eq!(ids(&canceled), vec![running]);
    assert!(list_all(&env.ops, "nobody", None).await.is_empty());
    env.stop().await;
}

// ─── retention_prunes_finished_after_7_days ─────────────────────────────────

/// Leave a listener and a callback in `id`'s origin that nothing will clean
/// up: a pending, awaitable `<id>:a`, awaited by a pending, targeted
/// `<id>:w`. Retention must remove them with the operation (the cascades).
async fn leave_callbacks(server: &DurableServer, id: &OperationId) {
    let far = i64::MAX / 2;
    for (child, tags) in [
        ("a", json!({ "resonate:scope": "global" })),
        ("w", json!({ "resonate:target": "inproc://any@nobody" })),
    ] {
        server
            .process(json!({
                "kind": "promise.create",
                "data": { "id": format!("{id}:{child}"), "timeoutAt": far, "param": {}, "tags": tags },
            }))
            .await
            .expect("create a child");
    }
    server
        .process(json!({
            "kind": "promise.register_callback",
            "data": { "awaited": format!("{id}:a"), "awaiter": format!("{id}:w") },
        }))
        .await
        .expect("register a callback");
    server
        .process(json!({
            "kind": "promise.register_listener",
            "data": { "awaited": format!("{id}:a"), "address": "inproc://any@nobody" },
        }))
        .await
        .expect("register a listener");
}

/// The rows of `id`'s origin in the SQLite store at `db`: promises,
/// callbacks, listeners.
fn sqlite_rows(db: &Path, id: &OperationId) -> (i64, i64, i64) {
    let conn = rusqlite::Connection::open(db).expect("open the store");
    let count = |sql: &str| -> i64 {
        conn.query_row(sql, [id.as_str()], |row| row.get(0))
            .expect(sql)
    };
    (
        count("SELECT COUNT(*) FROM promises WHERE origin_id = ?1"),
        // By id, not through a join: a row the delete left behind would
        // join nothing.
        count(
            "SELECT COUNT(*) FROM callbacks WHERE awaited_id LIKE ?1 || ':%' \
             OR awaiter_id LIKE ?1 || ':%'",
        ),
        count("SELECT COUNT(*) FROM listeners WHERE promise_id LIKE ?1 || ':%'"),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn retention_prunes_finished_after_7_days() {
    let env = Env::start(OpsConfig::default()).await;
    let db = env._dir.path().join("durable").join("default.db");
    let (done, _) = env
        .ops
        .submit("default", "test.echo", json!({}), Some("old"))
        .await
        .expect("echo");
    let finished = wait_state(&env.ops, &done, OperationState::Succeeded).await;
    let (live, _) = env
        .ops
        .submit("default", "test.stuck", json!({}), None)
        .await
        .expect("stuck");
    wait_state(&env.ops, &live, OperationState::Running).await;
    leave_callbacks(&env.server, &done).await;
    let (promises, callbacks, listeners) = sqlite_rows(&db, &done);
    assert!(promises >= 4, "root, its step and two children: {promises}");
    assert_eq!((callbacks, listeners), (1, 1));

    // Six days after it finished: kept.
    let settled = finished.updated_at;
    assert_eq!(
        env.ops
            .prune_finished(settled + 6 * DAY_MS)
            .await
            .expect("prune"),
        0
    );
    env.ops.get(&done).await.expect("still listed");

    // Eight days after: gone, with every promise, callback and listener of
    // its origin. The running one stays.
    assert_eq!(
        env.ops
            .prune_finished(settled + 8 * DAY_MS)
            .await
            .expect("prune"),
        1
    );
    assert!(matches!(
        env.ops.get(&done).await,
        Err(OpsError::NotFound(_))
    ));
    assert_eq!(sqlite_rows(&db, &done), (0, 0, 0));
    assert_eq!(
        env.ops.get(&live).await.expect("live").state,
        OperationState::Running
    );
    assert_eq!(
        env.ops
            .prune_finished(now_ms() + 8 * DAY_MS)
            .await
            .expect("prune"),
        0,
        "a running operation is never pruned"
    );
    // A pruned key can be used again: it is a new operation.
    let (again, created) = env
        .ops
        .submit("default", "test.echo", json!({}), Some("old"))
        .await
        .expect("resubmit");
    assert_eq!((again, created), (done, true));
    env.stop().await;
}

/// The same on TiDB (skipped without `OPERON_TEST_TIDB`).
#[cfg(feature = "mysql")]
#[tokio::test(flavor = "multi_thread")]
async fn retention_prunes_finished_after_7_days_on_tidb() {
    use sqlx::{Connection, MySqlConnection};

    let Ok(admin) = std::env::var("OPERON_TEST_TIDB") else {
        println!("skipped: retention_prunes_finished_after_7_days_on_tidb needs OPERON_TEST_TIDB");
        return;
    };
    let name = format!(
        "loam_t_retention_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .subsec_nanos()
    );
    let mut conn = MySqlConnection::connect(&admin).await.expect("admin");
    sqlx::raw_sql(&format!("CREATE DATABASE `{name}`"))
        .execute(&mut conn)
        .await
        .expect("create database");
    let mut url = url::Url::parse(&admin).expect("a URL");
    url.set_path(&format!("/{name}"));
    let url = url.to_string();
    let store = operon_durable::DurableStore::mysql(&url).expect("store");
    DurableServer::migrate(store.clone())
        .await
        .expect("migrate");
    let mut durable = DurableConfig::new(store);
    durable.listen = free_addr();
    durable.retry_timeout = Duration::from_secs(1);
    let server = DurableServer::start(durable, "1").await.expect("server");
    let kinds = kinds();
    let runtime = DurableRuntime::start_with(&server, "1", options(), |sdk| kinds.register(sdk))
        .await
        .expect("runtime");
    let ops = Operations::new(
        server.client(),
        server.store().clone(),
        &kinds,
        OpsConfig::default(),
    );

    let (done, _) = ops
        .submit("default", "test.echo", json!({}), None)
        .await
        .expect("echo");
    let finished = wait_state(&ops, &done, OperationState::Succeeded).await;
    leave_callbacks(&server, &done).await;
    let mut db = MySqlConnection::connect(&url).await.expect("connect");
    let rows = async |db: &mut MySqlConnection| -> (i64, i64, i64) {
        let one = async |db: &mut MySqlConnection, sql: &str| -> i64 {
            sqlx::query_scalar(sql)
                .bind(done.as_str())
                .fetch_one(db)
                .await
                .expect(sql)
        };
        (
            one(db, "SELECT COUNT(*) FROM promises WHERE origin_id = ?").await,
            one(
                db,
                "SELECT COUNT(*) FROM callbacks WHERE awaited_id LIKE CONCAT(?, ':%')",
            )
            .await,
            one(
                db,
                "SELECT COUNT(*) FROM listeners WHERE promise_id LIKE CONCAT(?, ':%')",
            )
            .await,
        )
    };
    let (promises, callbacks, listeners) = rows(&mut db).await;
    assert!(promises >= 4, "{promises}");
    assert_eq!((callbacks, listeners), (1, 1));
    let callbacks_total = async |db: &mut MySqlConnection| {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM callbacks")
            .fetch_one(db)
            .await
    };
    assert_eq!(callbacks_total(&mut db).await.expect("callbacks"), 1);

    assert_eq!(
        ops.prune_finished(finished.updated_at + 8 * DAY_MS)
            .await
            .expect("prune"),
        1
    );
    assert!(matches!(ops.get(&done).await, Err(OpsError::NotFound(_))));
    assert_eq!(rows(&mut db).await, (0, 0, 0));
    // The test's database held only this operation's callback.
    assert_eq!(callbacks_total(&mut db).await.expect("callbacks"), 0);
    let _ = db.close().await;

    runtime.stop().await;
    server.stop().await;
    sqlx::raw_sql(&format!("DROP DATABASE IF EXISTS `{name}`"))
        .execute(&mut conn)
        .await
        .expect("drop database");
}

// ─── unknown_id_is_404 ──────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn unknown_id_is_404() {
    let env = Env::start(OpsConfig::default()).await;
    let unknown = OperationId::parse(&format!("op-{}", "0".repeat(26))).expect("an id");
    assert!(matches!(
        env.ops.get(&unknown).await,
        Err(OpsError::NotFound(_))
    ));
    assert!(matches!(
        env.ops.cancel(&unknown).await,
        Err(OpsError::NotFound(_))
    ));

    // A promise that is not an operation (an SDK user's, say) is not one.
    let foreign = OperationId::parse(&format!("op-{}", "Z".repeat(26))).expect("an id");
    env.server
        .process(json!({
            "kind": "promise.create",
            "data": { "id": foreign.as_str(), "timeoutAt": i64::MAX / 2, "param": {}, "tags": {} },
        }))
        .await
        .expect("create");
    assert!(matches!(
        env.ops.get(&foreign).await,
        Err(OpsError::NotFound(_))
    ));
    assert!(matches!(
        env.ops.cancel(&foreign).await,
        Err(OpsError::NotFound(_))
    ));

    assert!(matches!(
        env.ops
            .submit("default", "no.such.kind", json!({}), None)
            .await,
        Err(OpsError::UnknownKind(_))
    ));
    env.stop().await;
}

// ─── the server gone ────────────────────────────────────────────────────────

/// After the server stops, the API answers `Unavailable` (503), not a hang.
#[tokio::test(flavor = "multi_thread")]
async fn a_stopped_server_is_unavailable() {
    let env = Env::start(OpsConfig::default()).await;
    let Env {
        _dir,
        server,
        runtime,
        ops,
    } = env;
    if let Some(runtime) = runtime {
        runtime.stop().await;
    }
    server.stop().await;
    let id = OperationId::generate();
    assert!(matches!(ops.get(&id).await, Err(OpsError::Unavailable(_))));
    assert!(matches!(
        ops.submit("default", "test.echo", json!({}), None).await,
        Err(OpsError::Unavailable(_))
    ));
    // Retention does not touch a store whose server has stopped (and
    // released its lock): #100 review.
    assert!(matches!(
        ops.prune_finished(now_ms() + 30 * DAY_MS).await,
        Err(OpsError::Unavailable(_))
    ));
    drop(_dir);
}
