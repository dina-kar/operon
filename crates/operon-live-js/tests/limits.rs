//! R1 plan Task 13 semantics 3: the CPU limit interrupts a handler with
//! `FunctionTimeout`, the memory limit gives `FunctionOutOfMemory`, and the
//! engine serves the next call after either. No cluster needed.

mod common;

use std::time::{Duration, Instant};

use common::{FakeHost, invocation, load, query_bundle, run};
use operon_live::{LiveError, LiveValue};
use operon_live_js::{Bundle, JsConfig};

fn small(cpu: Duration, memory: usize) -> JsConfig {
    JsConfig {
        memory_limit: memory,
        cpu_limit: cpu,
        contexts: 1,
    }
}

/// A `for (;;) {}` handler stops at the CPU limit, even inside a `try`.
#[tokio::test]
async fn busy_loop_times_out() {
    let bundle = load(
        &query_bundle("try { for (;;) {} } catch (e) { return \"caught\"; }"),
        small(Duration::from_millis(200), 64 << 20),
    );
    let start = Instant::now();
    match run(&bundle, "t:q").await {
        Err(LiveError::FunctionTimeout(m)) => assert!(m.contains("CPU limit"), "{m}"),
        other => panic!("{other:?}"),
    }
    let took = start.elapsed();
    assert!(
        took >= Duration::from_millis(200) && took < Duration::from_secs(3),
        "{took:?}"
    );
    assert_eq!(
        LiveError::FunctionTimeout(String::new()).code(),
        operon_live::pb::ErrorCode::ERROR_CODE_FUNCTION_TIMEOUT
    );
}

/// An allocation bomb stops at the runtime's memory limit.
#[tokio::test]
async fn allocation_bomb_hits_memory_limit() {
    let bundle = load(
        &query_bundle("const a = []; for (;;) { a.push(new Array(1024).fill(a.length)); }"),
        small(Duration::from_secs(20), 16 << 20),
    );
    match run(&bundle, "t:q").await {
        Err(LiveError::FunctionOutOfMemory(m)) => assert!(m.contains("out of memory"), "{m}"),
        other => panic!("{other:?}"),
    }
    assert_eq!(
        LiveError::FunctionOutOfMemory(String::new()).code(),
        operon_live::pb::ErrorCode::ERROR_CODE_FUNCTION_OUT_OF_MEMORY
    );
}

/// After a timeout and after running out of memory, the same (one-context)
/// bundle answers the next call.
#[tokio::test]
async fn context_recovers_after_timeout() {
    let bundle = load(
        r#"
        import { query } from "loam:server";
        export const t = {
          spin: query(async () => { for (;;) {} }),
          bomb: query(async () => { const a = []; for (;;) a.push(new Array(1 << 20).fill(0)); }),
          ok: query(async (ctx, args) => 41 + 1),
        };
        "#,
        small(Duration::from_millis(150), 16 << 20),
    );
    for _ in 0..2 {
        assert!(matches!(
            run(&bundle, "t:spin").await,
            Err(LiveError::FunctionTimeout(_))
        ));
        assert_eq!(run(&bundle, "t:ok").await, Ok(LiveValue::F64(42.0)));
        let bomb = run(&bundle, "t:bomb").await;
        assert!(
            matches!(bomb, Err(LiveError::FunctionOutOfMemory(_))),
            "{bomb:?}"
        );
        assert_eq!(run(&bundle, "t:ok").await, Ok(LiveValue::F64(42.0)));
    }
}

/// Time spent waiting for host calls does not count against the CPU limit.
#[tokio::test]
async fn host_waits_do_not_count_against_the_cpu_limit() {
    let bundle = load(
        &query_bundle("for (let i = 0; i < 4; i++) { await ctx.db.get(\"x\"); } return 1;"),
        small(Duration::from_millis(100), 64 << 20),
    );
    let mut host = FakeHost::null();
    host.delay = Duration::from_millis(80);
    let got = bundle
        .invoke("t:q", &mut host, invocation(), LiveValue::Null)
        .await;
    assert_eq!(got, Ok(LiveValue::F64(1.0)));
    assert_eq!(host.calls.len(), 4);
}

/// A bundle whose top level spins or throws is refused at load.
#[test]
fn a_bad_top_level_is_refused_at_load() {
    let cases = [
        ("for (;;) {}", "CPU limit"),
        ("throw new Error(\"nope\")", "nope"),
        ("import x from \"./other.js\";", "other.js"),
        ("export const = ;", "failed to load"),
        (
            "import { query } from \"loam:server\"; export const list = query(async () => 1);",
            "module:export",
        ),
    ];
    for (source, expect) in cases {
        match Bundle::load(source, small(Duration::from_millis(200), 64 << 20)) {
            Err(LiveError::InvalidArgument(m)) => assert!(m.contains(expect), "{source}: {m}"),
            other => panic!("{source}: {other:?}"),
        }
    }
}
