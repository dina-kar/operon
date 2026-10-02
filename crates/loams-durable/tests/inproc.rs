//! Loams's durable runtime (D1 Task 6): the Resonate Rust SDK over the
//! in-process network, served by the embedded server's `worker_inproc`.
//!
//! Every test uses its own functions and counters: the tests share one
//! process, and a counter is how a test tells a step that ran from one that
//! was replayed from its settled promise.

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use loams_durable::{DurableConfig, DurableRuntime, DurableServer, RuntimeOptions};
use resonate_sdk::prelude::*;

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

/// A short lease, so a task held by a stopped runtime is redispatched in
/// seconds rather than a minute.
fn options() -> RuntimeOptions {
    RuntimeOptions {
        ttl: Duration::from_secs(2),
    }
}

/// Wait up to 30 s for `f`.
async fn within<F: std::future::IntoFuture>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), f.into_future())
        .await
        .expect("timed out")
}

/// Poll `cond` every 20 ms for up to 30 s.
async fn until(what: &str, cond: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ─── two_step_function_runs ─────────────────────────────────────────────────

static TWO_STEP_ONE: AtomicUsize = AtomicUsize::new(0);
static TWO_STEP_TWO: AtomicUsize = AtomicUsize::new(0);

#[resonate_sdk::function]
async fn two_step_one(x: i64) -> Result<i64> {
    TWO_STEP_ONE.fetch_add(1, Ordering::SeqCst);
    Ok(x + 1)
}

#[resonate_sdk::function]
async fn two_step_two(x: i64) -> Result<i64> {
    TWO_STEP_TWO.fetch_add(1, Ordering::SeqCst);
    Ok(x * 10)
}

#[resonate_sdk::function]
async fn two_step(ctx: &Context, x: i64) -> Result<i64> {
    let a: i64 = ctx.run(two_step_one, x).await?;
    let b: i64 = ctx.run(two_step_two, a).await?;
    Ok(b)
}

fn register_two_step(sdk: &Resonate) -> Result<()> {
    sdk.register(two_step)?;
    sdk.register(two_step_one)?;
    sdk.register(two_step_two)
}

#[tokio::test(flavor = "multi_thread")]
async fn two_step_function_runs() {
    let dir = tempfile::tempdir().expect("tempdir");
    let server = DurableServer::start(config(dir.path()), "1")
        .await
        .expect("server");
    let runtime = DurableRuntime::start_with(&server, "1", options(), register_two_step)
        .await
        .expect("runtime");

    let result: i64 = within(runtime.sdk().run("two-step-1", two_step, 4_i64))
        .await
        .expect("the workflow's result");

    assert_eq!(result, 50, "(4 + 1) * 10");
    assert_eq!(TWO_STEP_ONE.load(Ordering::SeqCst), 1, "step 1 ran once");
    assert_eq!(TWO_STEP_TWO.load(Ordering::SeqCst), 1, "step 2 ran once");

    // The same id again is the same execution: nothing runs a second time.
    let again: i64 = within(runtime.sdk().run("two-step-1", two_step, 4_i64))
        .await
        .expect("the memoized result");
    assert_eq!(again, 50);
    assert_eq!(TWO_STEP_ONE.load(Ordering::SeqCst), 1);
    assert_eq!(TWO_STEP_TWO.load(Ordering::SeqCst), 1);

    runtime.stop().await;
    server.stop().await;
}

// ─── crash_between_steps_resumes_without_rerunning_step_1 ───────────────────

static CRASH_ONE: AtomicUsize = AtomicUsize::new(0);
static CRASH_TWO: AtomicUsize = AtomicUsize::new(0);
/// Set when step 2 is first entered, which is after step 1 settled.
static CRASH_TWO_ENTERED: AtomicBool = AtomicBool::new(false);

#[resonate_sdk::function]
async fn crash_one(x: i64) -> Result<i64> {
    CRASH_ONE.fetch_add(1, Ordering::SeqCst);
    Ok(x + 1)
}

/// The first attempt never finishes: the runtime running it is stopped under
/// it, as a crash would. It counts only a finished run.
#[resonate_sdk::function]
async fn crash_two(x: i64) -> Result<i64> {
    if !CRASH_TWO_ENTERED.swap(true, Ordering::SeqCst) {
        std::future::pending::<()>().await;
    }
    CRASH_TWO.fetch_add(1, Ordering::SeqCst);
    Ok(x * 10)
}

#[resonate_sdk::function]
async fn crash_flow(ctx: &Context, x: i64) -> Result<i64> {
    let a: i64 = ctx.run(crash_one, x).await?;
    let b: i64 = ctx.run(crash_two, a).await?;
    Ok(b)
}

fn register_crash(sdk: &Resonate) -> Result<()> {
    sdk.register(crash_flow)?;
    sdk.register(crash_one)?;
    sdk.register(crash_two)
}

#[tokio::test(flavor = "multi_thread")]
async fn crash_between_steps_resumes_without_rerunning_step_1() {
    let dir = tempfile::tempdir().expect("tempdir");
    let server = DurableServer::start(config(dir.path()), "1")
        .await
        .expect("server");

    let first = DurableRuntime::start_with(&server, "1", options(), register_crash)
        .await
        .expect("first runtime");
    let _handle = within(first.sdk().run("crash-1", crash_flow, 4_i64).spawn())
        .await
        .expect("started");
    until("step 2 to start", || {
        CRASH_TWO_ENTERED.load(Ordering::SeqCst)
    })
    .await;
    assert_eq!(CRASH_ONE.load(Ordering::SeqCst), 1);
    // The crash: the runtime goes away with step 2 in flight.
    first.stop().await;

    let second = DurableRuntime::start_with(&server, "1", options(), register_crash)
        .await
        .expect("second runtime");
    let handle = within(second.sdk().get::<i64>("crash-1"))
        .await
        .expect("a handle");
    let result = within(handle.result()).await.expect("the result");

    assert_eq!(result, 50);
    assert_eq!(CRASH_ONE.load(Ordering::SeqCst), 1, "step 1 ran once");
    assert_eq!(CRASH_TWO.load(Ordering::SeqCst), 1, "step 2 finished once");

    second.stop().await;
    server.stop().await;
}

// ─── failed_branch_alone_retries ────────────────────────────────────────────

/// Runs per branch, 0..4.
static BRANCH_RUNS: [AtomicUsize; 4] = [
    AtomicUsize::new(0),
    AtomicUsize::new(0),
    AtomicUsize::new(0),
    AtomicUsize::new(0),
];
/// The branch that fails, once.
const FAILING: usize = 2;
static FAILED_ONCE: AtomicBool = AtomicBool::new(false);

/// A branch is its own task (an rpc), so a failed attempt releases that task
/// alone and the server hands it out again.
#[resonate_sdk::function]
async fn branch(i: usize) -> Result<usize> {
    BRANCH_RUNS[i].fetch_add(1, Ordering::SeqCst);
    if i == FAILING && !FAILED_ONCE.swap(true, Ordering::SeqCst) {
        panic!("branch {i} fails its first attempt");
    }
    Ok(i * 100)
}

#[resonate_sdk::function]
async fn fan_out(ctx: &Context) -> Result<usize> {
    let mut handles = Vec::new();
    for i in 0..4_usize {
        handles.push(ctx.rpc::<usize>("branch", i).spawn()?);
    }
    let mut sum = 0;
    for handle in handles {
        sum += handle.await?;
    }
    Ok(sum)
}

fn register_fan_out(sdk: &Resonate) -> Result<()> {
    sdk.register(fan_out)?;
    sdk.register(branch)
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_branch_alone_retries() {
    let dir = tempfile::tempdir().expect("tempdir");
    let server = DurableServer::start(config(dir.path()), "1")
        .await
        .expect("server");
    let runtime = DurableRuntime::start_with(&server, "1", options(), register_fan_out)
        .await
        .expect("runtime");

    let sum: usize = within(runtime.sdk().run("fan-out-1", fan_out, ()))
        .await
        .expect("the fan-out's result");

    assert_eq!(sum, 600, "0 + 100 + 200 + 300");
    assert!(FAILED_ONCE.load(Ordering::SeqCst), "the branch did fail");
    for (i, runs) in BRANCH_RUNS.iter().enumerate() {
        let expected = if i == FAILING { 2 } else { 1 };
        assert_eq!(runs.load(Ordering::SeqCst), expected, "branch {i}");
    }

    runtime.stop().await;
    server.stop().await;
}

// ─── sleep_survives_restart ─────────────────────────────────────────────────

static BEFORE_SLEEP: AtomicUsize = AtomicUsize::new(0);
static AFTER_SLEEP: AtomicUsize = AtomicUsize::new(0);

#[resonate_sdk::function]
async fn note_before() -> Result<()> {
    BEFORE_SLEEP.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[resonate_sdk::function]
async fn note_after() -> Result<()> {
    AFTER_SLEEP.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[resonate_sdk::function]
async fn sleepy(ctx: &Context) -> Result<u64> {
    ctx.run(note_before, ()).await?;
    ctx.sleep(Duration::from_secs(2)).await?;
    ctx.run(note_after, ()).await?;
    Ok(7)
}

fn register_sleepy(sdk: &Resonate) -> Result<()> {
    sdk.register(sleepy)?;
    sdk.register(note_before)?;
    sdk.register(note_after)
}

#[tokio::test(flavor = "multi_thread")]
async fn sleep_survives_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let server = DurableServer::start(config(dir.path()), "1")
        .await
        .expect("server");

    let started = Instant::now();
    let first = DurableRuntime::start_with(&server, "1", options(), register_sleepy)
        .await
        .expect("first runtime");
    let _handle = within(first.sdk().run("sleepy-1", sleepy, ()).spawn())
        .await
        .expect("started");
    until("the step before the sleep", || {
        BEFORE_SLEEP.load(Ordering::SeqCst) == 1
    })
    .await;
    // The workflow is suspended on its timer; nothing of it is in memory
    // that the next runtime needs.
    tokio::time::sleep(Duration::from_millis(300)).await;
    first.stop().await;
    assert_eq!(AFTER_SLEEP.load(Ordering::SeqCst), 0, "still asleep");

    let second = DurableRuntime::start_with(&server, "1", options(), register_sleepy)
        .await
        .expect("second runtime");
    let handle = within(second.sdk().get::<u64>("sleepy-1"))
        .await
        .expect("a handle");
    let result = within(handle.result()).await.expect("the result");

    assert_eq!(result, 7);
    assert!(
        started.elapsed() >= Duration::from_secs(2),
        "woke after {:?}",
        started.elapsed()
    );
    assert_eq!(BEFORE_SLEEP.load(Ordering::SeqCst), 1, "not replayed");
    assert_eq!(AFTER_SLEEP.load(Ordering::SeqCst), 1);

    second.stop().await;
    server.stop().await;
}

// ─── no_http_is_used ────────────────────────────────────────────────────────

static QUIET_RUNS: AtomicUsize = AtomicUsize::new(0);

#[resonate_sdk::function]
async fn quiet_step(x: i64) -> Result<i64> {
    QUIET_RUNS.fetch_add(1, Ordering::SeqCst);
    Ok(x + 1)
}

#[resonate_sdk::function]
async fn quiet(ctx: &Context, x: i64) -> Result<i64> {
    let a: i64 = ctx.run(quiet_step, x).await?;
    let b: i64 = ctx.run(quiet_step, a).await?;
    Ok(b)
}

fn register_quiet(sdk: &Resonate) -> Result<()> {
    sdk.register(quiet)?;
    sdk.register(quiet_step)
}

/// The listener off (the gateway and the poll transport both disabled
/// through `--durable-set`): the SDK reaches the server with no socket.
#[tokio::test(flavor = "multi_thread")]
async fn no_http_is_used() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = config(dir.path());
    config.overrides.extend([
        ("gateways.gateway_http.enabled".into(), "false".into()),
        ("workers.transport_http_poll.enabled".into(), "false".into()),
    ]);
    let addr = config.listen;
    let server = DurableServer::start(config, "1").await.expect("server");
    assert!(
        TcpStream::connect_timeout(&addr, Duration::from_millis(500)).is_err(),
        "nothing listens on {addr}"
    );
    let runtime = DurableRuntime::start_with(&server, "1", options(), register_quiet)
        .await
        .expect("runtime");

    let result: i64 = within(runtime.sdk().run("quiet-1", quiet, 1_i64))
        .await
        .expect("the result");

    assert_eq!(result, 3);
    assert_eq!(QUIET_RUNS.load(Ordering::SeqCst), 2);
    assert!(
        TcpStream::connect_timeout(&addr, Duration::from_millis(500)).is_err(),
        "still nothing listens on {addr}"
    );

    runtime.stop().await;
    server.stop().await;
}

// ─── the runtime's own contract ─────────────────────────────────────────────

/// `start` (the plan's signature) with the functions registered afterwards
/// through `sdk()`.
#[tokio::test(flavor = "multi_thread")]
async fn plain_start_registers_later() {
    let dir = tempfile::tempdir().expect("tempdir");
    let server = DurableServer::start(config(dir.path()), "7")
        .await
        .expect("server");
    let runtime = DurableRuntime::start(&server, "7").await.expect("runtime");
    runtime.sdk().register(later).expect("register after start");
    let result: String = within(runtime.sdk().run("later-1", later, "x".to_string()))
        .await
        .expect("the result");
    assert_eq!(result, "x!");
    runtime.stop().await;
    server.stop().await;
}

#[resonate_sdk::function]
async fn later(s: String) -> Result<String> {
    Ok(format!("{s}!"))
}
