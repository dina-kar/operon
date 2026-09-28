//! R1 plan Task 13 semantics 1 and 4: bundles, the `ctx.db` host API, how
//! host errors reach the function and the runner, and `Deploy` with its
//! gate. The host-API tests run without a cluster (a fake host); the
//! runner and `Deploy` tests need TiKV and skip without `OPERON_TEST_PD`.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{FakeHost, invocation, load, obj, query_bundle, s};
use futures::future::BoxFuture;
use operon_live::deploy::{Deployments, bundle_path};
use operon_live::{
    FnKind, Function, LiveConfig, LiveError, LiveTxn, LiveValue, Runner, pb, system,
};
use operon_live_js::{Bundle, JsConfig, JsEngine};
use operon_store::Store;
use operon_tikv::TxnError;
use operon_tikv::testing::{self, TEST_LIVE};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

const CHAT: &str = r#"
import { query, mutation } from "loam:server";

export const messages = {
  send: mutation({
    args: { channel: "string", body: "string" },
    handler: async (ctx, { channel, body }) => ctx.db.insert("messages", { channel, body }),
  }),
  list: query(async (ctx, { channel }) =>
    ctx.db.query("messages").withIndex("by_channel", (q) => q.eq("channel", channel)).collect()),
  latest: query(async (ctx, { channel }) =>
    ctx.db.query("messages").withIndex("by_channel", (q) => q.eq("channel", channel))
      .order("desc").first()),
  get: query(async (ctx, { id }) => ctx.db.get(id)),
};

export const counters = {
  bump: mutation(async (ctx, { id }) => {
    const doc = await ctx.db.get(id);
    const n = doc.n + 1n;
    await ctx.db.patch(id, { n });
    return n;
  }),
};
"#;

fn chat_schema() -> pb::Schema {
    pb::Schema {
        tables: vec![
            pb::TableSchema {
                name: "messages".into(),
                indexes: vec![pb::IndexSchema {
                    name: "by_channel".into(),
                    fields: vec!["channel".into()],
                    ..Default::default()
                }],
                ..Default::default()
            },
            pb::TableSchema {
                name: "counters".into(),
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

// ---- without a cluster ----

/// The bundle lists its functions as `module:export`, sorted, with kinds.
#[test]
fn functions_are_listed_as_module_exports() {
    let bundle = load(CHAT, JsConfig::default());
    assert_eq!(
        bundle.functions(),
        vec![
            ("counters:bump".to_string(), FnKind::Mutation),
            ("messages:get".to_string(), FnKind::Query),
            ("messages:latest".to_string(), FnKind::Query),
            ("messages:list".to_string(), FnKind::Query),
            ("messages:send".to_string(), FnKind::Mutation),
        ]
    );
    assert!(bundle.function("messages:send").is_some());
    assert!(bundle.function("messages:nope").is_none());
}

/// Semantics 1: `ctx.db` calls become host calls in the system functions'
/// shapes; values cross as `bigint` ↔ `I64`, number ↔ `F64` and
/// `ArrayBuffer` ↔ `Bytes`.
#[tokio::test]
async fn db_calls_reach_the_host_with_live_values() {
    let bundle = load(
        &query_bundle(
            r#"
            const got = await ctx.db.get("id1");
            const id = await ctx.db.insert("t", { n: 1n, x: 1.5, b: new Uint8Array([1, 2]).buffer, u: undefined });
            await ctx.db.query("t").withIndex("by_n", (q) => q.eq("n", 1n).gt("x", 0)).order("desc").take(3);
            return [got.big + 1n, got.bytes instanceof ArrayBuffer, new Uint8Array(got.bytes)[1], id];
            "#,
        ),
        JsConfig::default(),
    );
    let mut host = FakeHost::new(|op, _| match op {
        "get" => Ok(obj(&[
            ("big", LiveValue::I64(41)),
            ("bytes", LiveValue::Bytes(vec![7, 9])),
        ])),
        "insert" => Ok(s("new-id")),
        _ => Ok(LiveValue::Array(Vec::new())),
    });
    let got = bundle
        .invoke("t:m", &mut host, invocation(), LiveValue::Null)
        .await;
    assert_eq!(
        got,
        Ok(LiveValue::Array(vec![
            LiveValue::I64(42),
            LiveValue::Bool(true),
            LiveValue::F64(9.0),
            s("new-id"),
        ]))
    );
    assert_eq!(host.calls[0], ("get".into(), obj(&[("id", s("id1"))])));
    assert_eq!(
        host.calls[1],
        (
            "insert".into(),
            obj(&[
                ("table", s("t")),
                (
                    "fields",
                    obj(&[
                        ("n", LiveValue::I64(1)),
                        ("x", LiveValue::F64(1.5)),
                        ("b", LiveValue::Bytes(vec![1, 2])),
                    ])
                ),
            ])
        )
    );
    assert_eq!(
        host.calls[2],
        (
            "query".into(),
            obj(&[
                ("table", s("t")),
                ("index", s("by_n")),
                ("eqFields", LiveValue::Array(vec![s("n")])),
                ("eq", LiveValue::Array(vec![LiveValue::I64(1)])),
                ("rangeField", s("x")),
                (
                    "lower",
                    obj(&[
                        ("value", LiveValue::F64(0.0)),
                        ("inclusive", LiveValue::Bool(false)),
                    ])
                ),
                ("order", s("desc")),
                ("limit", LiveValue::I64(3)),
            ])
        )
    );
}

/// Carry T10-14: a storage error from a host call ends the call with that
/// error even when the function catches everything, so the runner sees it
/// and reruns; an exceeded limit too. Other host errors are ordinary
/// exceptions: catchable, and when uncaught the call fails with the
/// original error.
#[tokio::test]
async fn a_storage_error_escapes_a_js_catch() {
    let bundle = load(
        &query_bundle(
            r#"
            try { await ctx.db.get("x"); } catch (e) { return `swallowed ${e.code}`; }
            return "no error";
            "#,
        ),
        JsConfig::default(),
    );
    let fatal = [
        LiveError::Txn(TxnError::Conflict),
        LiveError::Txn(TxnError::NotApplied("a lock".into())),
        LiveError::LimitExceeded {
            limit: "max_scanned_docs",
            message: "too many".into(),
        },
    ];
    for e in fatal {
        let answer = e.clone();
        let mut host = FakeHost::new(move |_, _| Err(answer.clone()));
        let got = bundle
            .invoke("t:m", &mut host, invocation(), LiveValue::Null)
            .await;
        assert_eq!(got, Err(e));
    }
    let mut host = FakeHost::new(|_, _| Err(LiveError::NotFound("document x".into())));
    let got = bundle
        .invoke("t:m", &mut host, invocation(), LiveValue::Null)
        .await;
    assert_eq!(got, Ok(s("swallowed NOT_FOUND")));

    let uncaught = load(
        &query_bundle("return await ctx.db.get(\"x\");"),
        JsConfig::default(),
    );
    let mut host = FakeHost::new(|_, _| Err(LiveError::NotFound("document x".into())));
    let got = uncaught
        .invoke("t:q", &mut host, invocation(), LiveValue::Null)
        .await;
    assert_eq!(got, Err(LiveError::NotFound("document x".into())));

    let thrown = load(
        &query_bundle("throw new TypeError(\"bad input\");"),
        JsConfig::default(),
    );
    match thrown
        .invoke("t:q", &mut FakeHost::null(), invocation(), LiveValue::Null)
        .await
    {
        Err(LiveError::FunctionError(m)) => assert!(m.contains("TypeError: bad input"), "{m}"),
        other => panic!("{other:?}"),
    }
}

// ---- with a cluster ----

struct App {
    runner: Runner,
    deployments: Deployments,
    store: Store,
    config: LiveConfig,
}

async fn app(name: &str) -> Option<App> {
    let cluster = testing::cluster().await?;
    let tikv = cluster.connect(TEST_LIVE).await;
    let config = LiveConfig::with_tikv(name, cluster.config(TEST_LIVE));
    let runner = Runner::open(tikv, &config).await.expect("the runner opens");
    let store = Store::in_memory();
    let deployments = Deployments::new(
        name,
        runner.clone(),
        store.clone(),
        Some(Arc::new(JsEngine::default())),
    );
    Some(App {
        runner,
        deployments,
        store,
        config,
    })
}

async fn mutate(a: &App, f: &str, args: LiveValue) -> Result<LiveValue, LiveError> {
    let f = a.deployments.resolve(f)?;
    a.runner.mutate(f, args, None).await.map(|m| m.result)
}

async fn query(a: &App, f: &str, args: LiveValue) -> Result<operon_live::Queried, LiveError> {
    let f = a.deployments.resolve(f)?;
    let at = a.runner.tikv().now().await.expect("a timestamp");
    a.runner.query(&*f, args, at).await
}

/// Semantics 1: deployed queries and mutations run through the runner, and
/// their `ctx.db` reads land in the read set (points for `get`, ranges for
/// index queries).
#[tokio::test]
async fn query_and_mutation_run_and_record_read_sets() {
    let Some(a) = app("t13f").await else {
        return;
    };
    a.deployments
        .deploy(CHAT.as_bytes(), Some(chat_schema()))
        .await
        .expect("the deploy");
    let id = mutate(
        &a,
        "messages:send",
        obj(&[("channel", s("general")), ("body", s("hi"))]),
    )
    .await
    .expect("send");
    let LiveValue::Str(id) = id else {
        panic!("{id:?}");
    };
    mutate(
        &a,
        "messages:send",
        obj(&[("channel", s("random")), ("body", s("other"))]),
    )
    .await
    .expect("send");
    let listed = query(&a, "messages:list", obj(&[("channel", s("general"))]))
        .await
        .expect("list");
    let LiveValue::Array(docs) = &listed.result else {
        panic!("{:?}", listed.result);
    };
    assert_eq!(docs.len(), 1);
    let LiveValue::Object(doc) = &docs[0] else {
        panic!("{:?}", docs[0]);
    };
    assert_eq!(doc["body"], s("hi"));
    assert_eq!(doc["_id"], s(&id));
    assert!(matches!(doc["_creationTime"], LiveValue::I64(_)));
    assert_eq!(listed.read_set.ranges.len(), 1);
    assert!(listed.read_set.points.is_empty());

    let got = query(&a, "messages:get", obj(&[("id", s(&id))]))
        .await
        .expect("get");
    assert_eq!(got.read_set.points.len(), 1);
    assert!(got.read_set.ranges.is_empty());
    let latest = query(&a, "messages:latest", obj(&[("channel", s("general"))]))
        .await
        .expect("latest");
    assert_eq!(latest.result, docs[0]);

    // The schema is deployed: inserts do not create tables.
    let e = a
        .runner
        .mutate(
            system::lookup(system::INSERT).expect("insert"),
            obj(&[("table", s("undeclared")), ("fields", obj(&[]))]),
            None,
        )
        .await
        .expect_err("not in the schema");
    assert!(matches!(e, LiveError::NotFound(_)), "{e}");
    // A range on the wrong field of the index is refused.
    let wrong = load(
        r#"import { query } from "loam:server";
        export const q = { wrong: query(async (ctx) =>
          ctx.db.query("messages").withIndex("by_channel", (q) => q.eq("body", "x")).collect()) };"#,
        JsConfig::default(),
    );
    let f = wrong.function("q:wrong").expect("q:wrong");
    let at = a.runner.tikv().now().await.expect("a timestamp");
    let e = a
        .runner
        .query(&*f, LiveValue::Null, at)
        .await
        .expect_err("wrong field");
    assert!(matches!(e, LiveError::InvalidArgument(_)), "{e}");
}

/// A mutation rerun on a conflict is invisible to its caller: 16
/// concurrent increments of one counter return 1 to 16, each once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mutation_rerun_on_conflict_is_invisible_to_the_caller() {
    let Some(a) = app("t13r").await else {
        return;
    };
    a.deployments
        .deploy(CHAT.as_bytes(), Some(chat_schema()))
        .await
        .expect("the deploy");
    let id = a
        .runner
        .mutate(
            system::lookup(system::INSERT).expect("insert"),
            obj(&[
                ("table", s("counters")),
                ("fields", obj(&[("n", LiveValue::I64(0))])),
            ]),
            None,
        )
        .await
        .expect("insert")
        .result;
    let bump = a.deployments.resolve("counters:bump").expect("bump");
    let runs = (0..16).map(|_| {
        let runner = a.runner.clone();
        let bump = bump.clone();
        let args = obj(&[("id", id.clone())]);
        tokio::spawn(async move { runner.mutate(bump, args, None).await })
    });
    let mut results = Vec::new();
    let mut reruns = 0;
    for run in runs {
        let m = run.await.expect("the task").expect("the bump");
        reruns += m.attempts - 1;
        let LiveValue::I64(n) = m.result else {
            panic!("{:?}", m.result);
        };
        results.push(n);
    }
    results.sort_unstable();
    assert_eq!(results, (1..=16).collect::<Vec<i64>>(), "reruns: {reruns}");
    eprintln!("mutation_rerun_on_conflict_is_invisible_to_the_caller: {reruns} reruns");

    // A forced conflict: another mutation sets the counter to 100 after the
    // first attempt ran; the attempt fails to commit and reruns, and the
    // caller sees only the rerun's 101.
    let interfering: Arc<dyn Function> = Arc::new(Interfering {
        inner: bump.clone(),
        runner: a.runner.clone(),
        id: id.clone(),
        first: std::sync::atomic::AtomicBool::new(true),
    });
    let m = a
        .runner
        .mutate(interfering, obj(&[("id", id.clone())]), None)
        .await
        .expect("the bump");
    assert_eq!(m.result, LiveValue::I64(101));
    assert!(m.attempts >= 2, "{m:?}");
    let got = query(&a, "messages:get", obj(&[("id", id)]))
        .await
        .expect("get");
    let LiveValue::Object(doc) = got.result else {
        panic!("{:?}", got.result);
    };
    assert_eq!(doc["n"], LiveValue::I64(101));
}

/// Runs `inner`, and after its first attempt commits another write to the
/// same document, so that attempt conflicts.
struct Interfering {
    inner: Arc<dyn Function>,
    runner: Runner,
    id: LiveValue,
    first: std::sync::atomic::AtomicBool,
}

impl Function for Interfering {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn kind(&self) -> FnKind {
        FnKind::Mutation
    }

    fn call<'a>(
        &'a self,
        txn: &'a mut LiveTxn<'_>,
        args: LiveValue,
    ) -> BoxFuture<'a, Result<LiveValue, LiveError>> {
        Box::pin(async move {
            let result = self.inner.call(txn, args).await?;
            if self.first.swap(false, std::sync::atomic::Ordering::SeqCst) {
                self.runner
                    .mutate(
                        system::lookup(system::PATCH).expect("patch"),
                        obj(&[
                            ("id", self.id.clone()),
                            ("fields", obj(&[("n", LiveValue::I64(100))])),
                        ]),
                        None,
                    )
                    .await
                    .expect("the interfering write");
            }
            Ok(result)
        })
    }
}

/// An unknown function is `NOT_FOUND`, before and after a deploy.
#[tokio::test]
async fn unknown_function_is_not_found() {
    let Some(a) = app("t13u").await else {
        return;
    };
    assert!(matches!(
        a.deployments.resolve("messages:list"),
        Err(LiveError::NotFound(_))
    ));
    a.deployments
        .deploy(CHAT.as_bytes(), None)
        .await
        .expect("the deploy");
    assert!(a.deployments.resolve("messages:list").is_ok());
    for name in ["messages:nope", "nope:list", "_system:nope", ""] {
        assert!(
            matches!(a.deployments.resolve(name), Err(LiveError::NotFound(_))),
            "{name}"
        );
    }
    assert!(a.deployments.resolve(system::GET).is_ok());
}

/// Semantics 4: a deploy stores the bundle at
/// `live/<app>/deployments/<id>.js` and swaps the catalog record; new calls
/// (and functions resolved before, at their next call) run the new code; a
/// restarted node loads it back.
#[tokio::test]
async fn deploy_swaps_functions_for_new_calls() {
    let Some(a) = app("t13d").await else {
        return;
    };
    let v = |n: u32| {
        format!(
            "import {{ mutation }} from \"loam:server\";\n\
             export const app = {{ version: mutation(async () => {n}) }};\n"
        )
    };
    let first = a
        .deployments
        .deploy(v(1).as_bytes(), None)
        .await
        .expect("v1");
    assert_eq!(a.deployments.current_id(), Some(first.clone()));
    let resolved = a.deployments.resolve("app:version").expect("resolved");
    assert_eq!(
        a.runner
            .mutate(resolved.clone(), LiveValue::Null, None)
            .await
            .map(|m| m.result),
        Ok(LiveValue::F64(1.0))
    );
    let second = a
        .deployments
        .deploy(v(2).as_bytes(), None)
        .await
        .expect("v2");
    assert_ne!(first, second);
    assert_eq!(
        mutate(&a, "app:version", LiveValue::Null).await,
        Ok(LiveValue::F64(2.0))
    );
    assert_eq!(
        a.runner
            .mutate(resolved, LiveValue::Null, None)
            .await
            .map(|m| m.result),
        Ok(LiveValue::F64(2.0))
    );
    let (stored, _) = a
        .store
        .get(&bundle_path("t13d", &second))
        .await
        .expect("the bundle is stored");
    assert_eq!(&stored[..], v(2).as_bytes());

    let restarted = Deployments::new(
        &a.config.app,
        a.runner.clone(),
        a.store.clone(),
        Some(Arc::new(JsEngine::default())),
    );
    assert_eq!(restarted.load_current().await, Ok(Some(second)));
    let f = restarted.resolve("app:version").expect("after a restart");
    assert_eq!(
        a.runner
            .mutate(f, LiveValue::Null, None)
            .await
            .map(|m| m.result),
        Ok(LiveValue::F64(2.0))
    );

    let bad = a.deployments.deploy(b"export const = ;", None).await;
    assert!(matches!(bad, Err(LiveError::InvalidArgument(_))), "{bad:?}");
}

/// A mutation that holds the runner's admission gate until released.
struct Blocking {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl Function for Blocking {
    fn name(&self) -> &str {
        "test:blocking"
    }

    fn kind(&self) -> FnKind {
        FnKind::Mutation
    }

    fn call<'a>(
        &'a self,
        _txn: &'a mut LiveTxn<'_>,
        _args: LiveValue,
    ) -> BoxFuture<'a, Result<LiveValue, LiveError>> {
        Box::pin(async move {
            self.entered.notify_one();
            self.release.notified().await;
            Ok(LiveValue::Null)
        })
    }
}

/// Row T9-1 and the review of #73: a deploy that changes an index is
/// refused (busy, `UNAVAILABLE`) while a mutation is in flight, and passes
/// once none is; a deploy that changes no index passes meanwhile; a
/// mutation that arrives while the deploy holds the gate waits, then
/// writes the new index's entries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn index_deploy_is_refused_while_a_mutation_is_in_flight() {
    let Some(a) = app("t13g").await else {
        return;
    };
    let plain = pb::Schema {
        tables: vec![pb::TableSchema {
            name: "messages".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    a.deployments
        .deploy(CHAT.as_bytes(), Some(plain.clone()))
        .await
        .expect("the first deploy");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let blocking: Arc<dyn Function> = Arc::new(Blocking {
        entered: entered.clone(),
        release: release.clone(),
    });
    let runner = a.runner.clone();
    let in_flight =
        tokio::spawn(async move { runner.mutate(blocking, LiveValue::Null, None).await });
    entered.notified().await;

    let busy = a
        .deployments
        .deploy(CHAT.as_bytes(), Some(chat_schema()))
        .await
        .expect_err("busy");
    assert!(matches!(busy, LiveError::Busy(_)), "{busy}");
    assert_eq!(busy.code(), pb::ErrorCode::ERROR_CODE_UNAVAILABLE);
    a.deployments
        .deploy(CHAT.as_bytes(), Some(plain))
        .await
        .expect("no index change passes while a mutation is in flight");
    release.notify_one();
    in_flight
        .await
        .expect("the task")
        .expect("the blocking mutation");

    let pause = a.deployments.pause_after_gate();
    let deployments = a.deployments.clone();
    let deploy = tokio::spawn(async move {
        deployments
            .deploy(CHAT.as_bytes(), Some(chat_schema()))
            .await
    });
    pause.reached.await.expect("the deploy holds the gate");
    let send = a.deployments.resolve("messages:send").expect("send");
    let runner = a.runner.clone();
    let arriving = tokio::spawn(async move {
        runner
            .mutate(
                send,
                obj(&[("channel", s("general")), ("body", s("during"))]),
                None,
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!arriving.is_finished(), "the mutation waits at admission");
    let _ = pause.release.send(());
    deploy.await.expect("the task").expect("the index deploy");
    arriving
        .await
        .expect("the task")
        .expect("the arriving mutation");
    let listed = query(&a, "messages:list", obj(&[("channel", s("general"))]))
        .await
        .expect("list by the new index");
    let LiveValue::Array(docs) = listed.result else {
        panic!("{:?}", listed.result);
    };
    assert_eq!(docs.len(), 1, "the new index has the arriving document");
}

/// Owner ruling T14-3 (on row T13-10): the deploy takes its busy check
/// before it stores the bundle, so a refused deploy leaves no object behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_deploy_stores_no_bundle() {
    let Some(a) = app("t14r").await else {
        return;
    };
    let plain = pb::Schema {
        tables: vec![pb::TableSchema {
            name: "messages".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    a.deployments
        .deploy(CHAT.as_bytes(), Some(plain))
        .await
        .expect("the first deploy");
    let prefix = "live/t14r/deployments/";
    let stored = |store: Store| async move {
        store
            .list(prefix)
            .await
            .expect("the bundles")
            .into_iter()
            .map(|o| o.path)
            .collect::<Vec<_>>()
    };
    let before = stored(a.store.clone()).await;
    assert_eq!(before.len(), 1, "{before:?}");

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let blocking: Arc<dyn Function> = Arc::new(Blocking {
        entered: entered.clone(),
        release: release.clone(),
    });
    let runner = a.runner.clone();
    let in_flight =
        tokio::spawn(async move { runner.mutate(blocking, LiveValue::Null, None).await });
    entered.notified().await;
    let busy = a
        .deployments
        .deploy(CHAT.as_bytes(), Some(chat_schema()))
        .await
        .expect_err("busy");
    assert!(matches!(busy, LiveError::Busy(_)), "{busy}");
    assert_eq!(
        stored(a.store.clone()).await,
        before,
        "a refused deploy stores nothing"
    );
    release.notify_one();
    in_flight
        .await
        .expect("the task")
        .expect("the blocking mutation");

    a.deployments
        .deploy(CHAT.as_bytes(), Some(chat_schema()))
        .await
        .expect("the retried deploy");
    assert_eq!(stored(a.store.clone()).await.len(), 2);
}

/// `Bundle` is the engine's deployment.
#[test]
fn the_engine_loads_bundles() {
    let engine = JsEngine::new(JsConfig {
        contexts: 1,
        ..JsConfig::default()
    });
    let deployment = operon_live::deploy::Engine::load(&engine, CHAT).expect("loads");
    assert_eq!(deployment.functions().len(), 5);
    let _ = Bundle::load(CHAT, JsConfig::default()).expect("loads");
}

/// Semantics 4 end to end: `Deploy` over the sync API (Connect, HTTP/1.1)
/// with the QuickJS engine, then `Mutate` and `Query` of the deployed
/// functions; a bad bundle is `INVALID_ARGUMENT`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deploy_through_the_sync_api() {
    use connectrpc::client::{ClientConfig, HttpClient};

    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let mut config = LiveConfig::with_tikv("t13s", cluster.config(TEST_LIVE));
    config.listen = "127.0.0.1:0".parse().expect("an address");
    config.engine = Some(Arc::new(JsEngine::default()));
    let handle = operon_live::LiveServer::start(config, CancellationToken::new())
        .await
        .expect("the server starts");
    let uri = format!("http://{}", handle.addr).parse().expect("a uri");
    let c = pb::LiveServiceClient::new(HttpClient::plaintext(), ClientConfig::new(uri));
    let deployed = c
        .deploy(pb::DeployRequest {
            bundle: CHAT.as_bytes().to_vec(),
            schema: chat_schema().into(),
            ..Default::default()
        })
        .await
        .expect("Deploy")
        .into_owned();
    assert_eq!(deployed.deployment_id.len(), 32);
    let args = obj(&[("channel", s("general")), ("body", s("hello"))]);
    let sent = c
        .mutate(pb::MutateRequest {
            function: "messages:send".into(),
            args: buffa::MessageField::some(args.to_proto()),
            ..Default::default()
        })
        .await
        .expect("Mutate")
        .into_owned();
    // At the commit timestamp: without `ts`, Query reads the manager's
    // tick, `tick_read_lag` behind (row T12-14).
    let q = c
        .query(pb::QueryRequest {
            function: "messages:list".into(),
            args: buffa::MessageField::some(obj(&[("channel", s("general"))]).to_proto()),
            ts: Some(sent.commit_ts),
            ..Default::default()
        })
        .await
        .expect("Query")
        .into_owned();
    let result = LiveValue::from_proto(q.result.into_option().expect("a result")).expect("ok");
    assert!(
        matches!(&result, LiveValue::Array(d) if d.len() == 1),
        "{result:?}"
    );
    let e = c
        .deploy(pb::DeployRequest {
            bundle: b"export const = ;".to_vec(),
            ..Default::default()
        })
        .await
        .expect_err("a bad bundle");
    assert_eq!(e.code, connectrpc::ErrorCode::InvalidArgument);
    handle.stop().await;
}
