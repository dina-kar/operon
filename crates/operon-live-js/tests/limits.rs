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

/// Review of #93: a thrown value whose `toString` loops is described under
/// the CPU limit, so the call times out instead of hanging a worker.
#[tokio::test]
async fn describing_a_thrown_value_is_under_the_cpu_limit() {
    // Review of #97: the recovery check calls the same bundle, whose one
    // worker ran the timed-out call.
    let bundle = load(
        "import { query } from \"loam:server\";\n\
         export const t = { q: query({ handler: async () => { throw { toString() { for (;;) {} } }; } }),\n\
         ok: query({ handler: async () => 1n }) };\n",
        small(Duration::from_millis(200), 64 << 20),
    );
    let run_once = async {
        match run(&bundle, "t:q").await {
            Err(LiveError::FunctionTimeout(m)) => assert!(m.contains("CPU limit"), "{m}"),
            other => panic!("{other:?}"),
        }
    };
    tokio::time::timeout(Duration::from_secs(10), run_once)
        .await
        .expect("the call ends");
    // The one worker is free again.
    assert!(matches!(run(&bundle, "t:ok").await, Ok(LiveValue::I64(1))));
}

/// Review of #97: converting a result runs its getters; one that loops
/// stops at the CPU limit and is reported as a timeout.
#[tokio::test]
async fn a_looping_getter_in_the_result_times_out() {
    let bundle = load(
        &query_bundle("return { get x() { for (;;) {} } };"),
        small(Duration::from_millis(200), 64 << 20),
    );
    let result = tokio::time::timeout(Duration::from_secs(10), run(&bundle, "t:q"))
        .await
        .expect("the call ends");
    match result {
        Err(LiveError::FunctionTimeout(m)) => assert!(m.contains("CPU limit"), "{m}"),
        other => panic!("{other:?}"),
    }
}

/// Review of #97: a result that repeats one large string is small in
/// QuickJS but large once copied out; the copy stops at its byte budget.
#[tokio::test]
async fn a_result_of_shared_large_strings_is_refused() {
    let bundle = load(
        &query_bundle("const s = \"x\".repeat(1 << 22); return Array(64).fill(s);"),
        small(Duration::from_secs(5), 64 << 20),
    );
    match run(&bundle, "t:q").await {
        Err(LiveError::FunctionError(m)) => assert!(m.contains("bytes"), "{m}"),
        other => panic!("{other:?}"),
    }
}

/// Review of #97: a sparse array's length does not reserve Rust memory
/// beyond the part budget.
#[tokio::test]
async fn a_huge_sparse_array_is_refused_by_its_parts() {
    let bundle = load(
        &query_bundle("const a = []; a.length = 1 << 26; return a;"),
        small(Duration::from_secs(5), 64 << 20),
    );
    match run(&bundle, "t:q").await {
        Err(LiveError::FunctionError(m)) => assert!(m.contains("parts"), "{m}"),
        other => panic!("{other:?}"),
    }
}

/// Review of #93: a value that shares its parts (`a = [a, a]`, 40 times) is
/// small in QuickJS but has 2^40 parts once copied out; the copy stops at
/// its part budget instead of growing outside the memory limit.
#[tokio::test]
async fn a_shared_value_graph_is_refused() {
    let bundle = load(
        &query_bundle("let a = []; for (let i = 0; i < 40; i++) a = [a, a]; return a;"),
        small(Duration::from_secs(5), 64 << 20),
    );
    let started = Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(20), run(&bundle, "t:q"))
        .await
        .expect("the call ends");
    match result {
        Err(LiveError::FunctionError(m)) => assert!(m.contains("parts"), "{m}"),
        other => panic!("{other:?}"),
    }
    assert!(started.elapsed() < Duration::from_secs(20));
}
