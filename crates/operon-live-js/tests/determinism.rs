//! R1 plan Task 13 semantics 2 and 2a (design §20 §6.2–§6.3): queries and
//! mutations see the start timestamp as the clock, a seeded `Math.random`,
//! no crypto randomness, timers, `fetch` or WebAssembly, frozen built-ins,
//! and a fresh context on every call. No cluster needed except for
//! `date_now_is_start_ts`'s runner half.

mod common;

use common::{FakeHost, invocation, load, obj, query_bundle, run, s};
use operon_live::{LiveConfig, LiveError, LiveValue, Runner};
use operon_live_js::{Invocation, JsConfig};
use operon_tikv::testing::{self, TEST_LIVE};
use operon_tikv::{Tikv, TimestampExt};

fn f(v: f64) -> LiveValue {
    LiveValue::F64(v)
}

/// Semantics 2a: a module-level counter starts at 0 in every call, well
/// past the pool's size; so does a module-level `Map`.
#[tokio::test]
async fn module_state_does_not_leak_between_calls() {
    let bundle = load(
        r#"
        import { mutation, query } from "loam:server";
        let count = 0;
        const seen = new Map();
        export const counter = {
          bump: mutation(async () => { count += 1; return count; }),
          remember: query(async (ctx, args) => { seen.set(args.k, true); return seen.size; }),
        };
        "#,
        JsConfig {
            contexts: 2,
            ..JsConfig::default()
        },
    );
    for _ in 0..10 {
        assert_eq!(run(&bundle, "counter:bump").await, Ok(f(1.0)));
    }
    for k in ["a", "b", "c", "d", "e"] {
        let got = bundle
            .invoke(
                "counter:remember",
                &mut FakeHost::null(),
                invocation(),
                obj(&[("k", s(k))]),
            )
            .await;
        assert_eq!(got, Ok(f(1.0)), "{k}");
    }
}

/// §6.2: `crypto.getRandomValues` and `crypto.randomUUID` throw
/// `DeterminismError` in queries and mutations; a handler can catch it as
/// one.
#[tokio::test]
async fn crypto_random_throws_in_queries_and_mutations() {
    for call in [
        "crypto.getRandomValues(new Uint8Array(4))",
        "crypto.randomUUID()",
    ] {
        let bundle = load(
            &query_bundle(&format!("return {call};")),
            JsConfig::default(),
        );
        for path in ["t:q", "t:m"] {
            match run(&bundle, path).await {
                Err(LiveError::FunctionError(m)) => {
                    assert!(m.contains("DeterminismError"), "{call} {path}: {m}");
                    assert!(m.contains("use an action"), "{call} {path}: {m}");
                }
                other => panic!("{call} {path}: {other:?}"),
            }
        }
        let caught = load(
            &query_bundle(&format!(
                "try {{ {call}; return false; }} catch (e) {{ \
                 return e instanceof DeterminismError && e.name === \"DeterminismError\"; }}"
            )),
            JsConfig::default(),
        );
        assert_eq!(
            run(&caught, "t:q").await,
            Ok(LiveValue::Bool(true)),
            "{call}"
        );
    }
}

/// §6.2: `Date.now()` and `new Date()` are the start timestamp's ms; with a
/// cluster, a query run by the runner at `ts` sees `ts`'s physical time.
#[tokio::test]
async fn date_now_is_start_ts() {
    let bundle = load(
        &query_bundle(
            "return [Date.now(), new Date().getTime(), typeof Date(), new Date(5).getTime()];",
        ),
        JsConfig::default(),
    );
    let inv = Invocation::new(7, 1_700_000_000_123, "");
    let got = bundle
        .invoke("t:q", &mut FakeHost::null(), inv, LiveValue::Null)
        .await;
    assert_eq!(
        got,
        Ok(LiveValue::Array(vec![
            f(1_700_000_000_123.0),
            f(1_700_000_000_123.0),
            s("string"),
            f(5.0),
        ]))
    );

    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let tikv = cluster.connect(TEST_LIVE).await;
    let runner = Runner::open(
        tikv.clone(),
        &LiveConfig::with_tikv("t13", cluster.config(TEST_LIVE)),
    )
    .await
    .expect("the runner opens");
    let at = tikv.now().await.expect("a timestamp");
    let f = bundle.function("t:q").expect("t:q");
    let queried = runner
        .query(&*f, LiveValue::Null, at.clone())
        .await
        .expect("the query");
    let ms = Tikv::physical_ms(&at) as f64;
    let LiveValue::Array(items) = queried.result else {
        panic!("{:?}", queried.result);
    };
    assert_eq!(items[0], LiveValue::F64(ms), "ts {}", at.version());
    assert_eq!(items[1], LiveValue::F64(ms));
}

/// §6.2: `Math.random` repeats for the same start timestamp and request id,
/// and differs when either changes.
#[tokio::test]
async fn random_is_repeatable_for_same_ts_and_request() {
    let bundle = load(
        &query_bundle(
            "const r = []; for (let i = 0; i < 5; i++) { const x = Math.random(); \
             if (!(x >= 0 && x < 1)) throw new Error(`out of range: ${x}`); r.push(x); } return r;",
        ),
        JsConfig::default(),
    );
    let draw = |inv: Invocation| {
        let bundle = &bundle;
        async move {
            bundle
                .invoke("t:m", &mut FakeHost::null(), inv, LiveValue::Null)
                .await
                .expect("the call")
        }
    };
    let a = draw(Invocation::new(100, 1, "req-1")).await;
    assert_eq!(a, draw(Invocation::new(100, 1, "req-1")).await);
    assert_ne!(a, draw(Invocation::new(100, 1, "req-2")).await);
    assert_ne!(a, draw(Invocation::new(101, 1, "req-1")).await);
    let LiveValue::Array(items) = &a else {
        panic!("{a:?}");
    };
    let distinct: std::collections::BTreeSet<u64> = items
        .iter()
        .map(|v| match v {
            LiveValue::F64(x) => x.to_bits(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(distinct.len(), 5);
}

/// §6.2: no timers, no `fetch`, no WebAssembly; a handler that waits on a
/// promise nothing can settle fails instead of hanging.
#[tokio::test]
async fn no_fetch_no_timers() {
    let bundle = load(
        &query_bundle(
            "return [typeof setTimeout, typeof setInterval, typeof fetch, typeof WebAssembly, \
             typeof XMLHttpRequest, typeof require, typeof process];",
        ),
        JsConfig::default(),
    );
    assert_eq!(
        run(&bundle, "t:q").await,
        Ok(LiveValue::Array(vec![s("undefined"); 7]))
    );
    let stuck = load(
        &query_bundle("await new Promise(() => {}); return 1;"),
        JsConfig::default(),
    );
    match run(&stuck, "t:q").await {
        Err(LiveError::FunctionError(m)) => assert!(m.contains("never settles"), "{m}"),
        other => panic!("{other:?}"),
    }
}

/// Semantics 2a: built-ins are frozen before the bundle runs, and the usual
/// Error-subclass pattern still works on the frozen prototypes.
#[tokio::test]
async fn builtins_are_frozen_and_subclassing_still_works() {
    let bundle = load(
        &query_bundle(
            r#"
            class AppError extends Error {
              constructor(m) { super(m); this.name = "AppError"; }
            }
            const e = new AppError("boom");
            let patched = true;
            try { Array.prototype.push = () => 0; } catch { patched = false; }
            let math = true;
            try { Math.random = () => 0.5; } catch { math = false; }
            return [
              Object.isFrozen(Array.prototype), Object.isFrozen(Object.prototype),
              Object.isFrozen(globalThis), patched, math, e.name, e.message,
              e instanceof Error, String(e),
            ];
            "#,
        ),
        JsConfig::default(),
    );
    assert_eq!(
        run(&bundle, "t:q").await,
        Ok(LiveValue::Array(vec![
            LiveValue::Bool(true),
            LiveValue::Bool(true),
            LiveValue::Bool(true),
            LiveValue::Bool(false),
            LiveValue::Bool(false),
            s("AppError"),
            s("boom"),
            LiveValue::Bool(true),
            s("AppError: boom"),
        ]))
    );
}

/// Review of #93: `new` on a `Date` subclass builds an instance of the
/// subclass, and a subclass without arguments still reads the start
/// timestamp.
#[tokio::test]
async fn date_subclasses_keep_their_prototype_and_the_clock() {
    let bundle = load(
        &query_bundle(
            r#"
            class Stamp extends Date { fmt() { return "at " + this.getTime(); } }
            const now = new Stamp();
            const at = new Stamp(5);
            return [
              now instanceof Stamp, now instanceof Date, typeof now.fmt,
              now.getTime() === Date.now(), at.getTime(), at.fmt(),
            ];
            "#,
        ),
        JsConfig::default(),
    );
    assert_eq!(
        run(&bundle, "t:q").await,
        Ok(LiveValue::Array(vec![
            LiveValue::Bool(true),
            LiveValue::Bool(true),
            s("function"),
            LiveValue::Bool(true),
            LiveValue::F64(5.0),
            s("at 5"),
        ]))
    );
}
