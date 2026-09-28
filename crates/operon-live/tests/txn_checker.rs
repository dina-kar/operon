//! R1 plan Task 16 semantics 2: the transaction checker.
//!
//! A list-append workload over Live documents: each key is a document whose
//! `list` field is an array, and each transaction reads keys and appends
//! unique elements to them through [`LiveTxn`] (`get`, then `patch`), run by
//! a [`Runner`] whose TiKV handle carries a random [`FaultPlan`] (Task 2's
//! refusals, conflicts, lost acknowledgements and delays). Read-only
//! transactions read a snapshot through `Runner::query`. After the workers
//! stop, a final read of every key closes the history, and the Elle-style
//! checker (`operon_live::testing::elle`) searches it for dependency cycles:
//! G0, G1a–c, lost updates, G-single and, since these reads are point reads
//! that lock their documents (D118), G2 too, plus lost and duplicated
//! appends. None may occur.
//!
//! A second workload looks for write skew on point reads: pairs of
//! documents that start "on call" (`v = 1`), and two concurrent mutations
//! per pair that each read both and take one off call if both are on. Under
//! snapshot isolation without the lock on read documents both could commit;
//! with it, a pair never ends with both off.
//!
//! Settings: `OPERON_CHECKER_SEED`, `OPERON_CHECKER_SECS` (30 per PR, 1 800
//! with `OPERON_TEST_NIGHTLY`), `OPERON_CHECKER_FAULTS=0`,
//! `OPERON_CHECKER_NEMESIS=1` (under `scripts/tikv/nemesis.sh`: unknown
//! outcomes and failures are expected, only anomalies fail).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use operon_live::system::{self, INSERT};
use operon_live::testing::elle::{self, Elem, Key, Kind, Op, Outcome, Txn};
use operon_live::{
    DocId, FnKind, Function, LiveConfig, LiveError, LiveTxn, LiveValue, Runner, RunnerOptions,
};
use operon_tikv::testing::{self, TEST_LIVE};
use operon_tikv::{CommitMode, Fault, FaultPlan, FaultPoint, Tikv, TxnError};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// Whether this is the nightly run (`OPERON_TEST_NIGHTLY`), without
/// `testing::nightly`'s skip line: the checkers run every time and only
/// last longer at night.
fn nightly() -> bool {
    std::env::var(testing::NIGHTLY_ENV).is_ok_and(|v| !v.trim().is_empty())
}

const READ: i64 = 0;
const APPEND: i64 = 1;
/// An append computed from a list read before the transaction (the broken
/// build of `checker_catches_injected_lost_update`).
const STALE_APPEND: i64 = 2;

#[derive(Debug, Clone)]
struct Opts {
    seed: u64,
    duration: Duration,
    workers: usize,
    keys: usize,
    faults: bool,
    nemesis: bool,
    /// The broken build: appends write a list read outside the transaction.
    stale_appends: bool,
}

impl Opts {
    fn from_env() -> Self {
        let default_secs = if nightly() { 1_800 } else { 30 };
        Opts {
            seed: std::env::var("OPERON_CHECKER_SEED")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(rand::random),
            duration: Duration::from_secs(
                std::env::var("OPERON_CHECKER_SECS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(default_secs),
            ),
            workers: 8,
            keys: 8,
            faults: std::env::var("OPERON_CHECKER_FAULTS").map_or(true, |v| v != "0"),
            nemesis: std::env::var("OPERON_CHECKER_NEMESIS").is_ok_and(|v| v == "1"),
            stale_appends: false,
        }
    }
}

/// Task 2's faults at random on the first two attempts of a run.
struct RandomFaults {
    rng: Mutex<ChaCha8Rng>,
    injected: AtomicU64,
}

impl FaultPlan for RandomFaults {
    fn at(&self, _op: &str, point: FaultPoint, attempt: u32) -> Option<Fault> {
        if attempt > 2 {
            return None;
        }
        let mut rng = self
            .rng
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !rng.random_bool(0.05) {
            return None;
        }
        self.injected.fetch_add(1, Ordering::Relaxed);
        Some(match (point, rng.random_range(0..3)) {
            (FaultPoint::AfterCommit, 0 | 1) => Fault::LoseAck,
            (FaultPoint::BeforeCommit, 0) => Fault::LoseAck,
            (FaultPoint::BeforeCommit | FaultPoint::BeforePrewrite, 1) => Fault::Conflict,
            (FaultPoint::BeforePrewrite, 0) => Fault::Refuse,
            _ => Fault::Delay(Duration::from_millis(rng.random_range(1..30))),
        })
    }
}

fn int(v: i64) -> LiveValue {
    LiveValue::I64(v)
}

fn obj(pairs: &[(&str, LiveValue)]) -> LiveValue {
    LiveValue::Object(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    )
}

fn list_of(fields: &BTreeMap<String, LiveValue>) -> Vec<LiveValue> {
    match fields.get("list") {
        Some(LiveValue::Array(l)) => l.clone(),
        _ => Vec::new(),
    }
}

fn elems(list: &[LiveValue]) -> Vec<Elem> {
    list.iter()
        .map(|v| match v {
            LiveValue::I64(e) => u64::try_from(*e).expect("elements are positive"),
            other => panic!("a list element is an int64, not {other:?}"),
        })
        .collect()
}

/// A list-append transaction: `args` is an array of `[kind, key, elem,
/// base?]`; the result holds each read's list (`null` for appends).
struct ListTxn {
    ids: Arc<Vec<DocId>>,
    kind: FnKind,
}

impl Function for ListTxn {
    fn name(&self) -> &str {
        "checker:list"
    }

    fn kind(&self) -> FnKind {
        self.kind
    }

    fn call<'a>(
        &'a self,
        txn: &'a mut LiveTxn<'_>,
        args: LiveValue,
    ) -> BoxFuture<'a, Result<LiveValue, LiveError>> {
        Box::pin(async move {
            let LiveValue::Array(ops) = args else {
                return Err(LiveError::InvalidArgument("ops".into()));
            };
            let mut out = Vec::with_capacity(ops.len());
            for op in ops {
                let LiveValue::Array(op) = op else {
                    return Err(LiveError::InvalidArgument("an op".into()));
                };
                let (LiveValue::I64(kind), LiveValue::I64(key)) = (&op[0], &op[1]) else {
                    return Err(LiveError::InvalidArgument("an op's kind and key".into()));
                };
                let id = self.ids[usize::try_from(*key).expect("a key")];
                match *kind {
                    READ => {
                        let doc = txn
                            .get(id)
                            .await?
                            .ok_or_else(|| LiveError::NotFound("a key's document".into()))?;
                        out.push(LiveValue::Array(list_of(&doc.fields)));
                    }
                    APPEND => {
                        let doc = txn
                            .get(id)
                            .await?
                            .ok_or_else(|| LiveError::NotFound("a key's document".into()))?;
                        let mut list = list_of(&doc.fields);
                        list.push(op[2].clone());
                        let fields = BTreeMap::from([("list".to_string(), LiveValue::Array(list))]);
                        txn.patch(id, fields).await?;
                        out.push(LiveValue::Null);
                    }
                    STALE_APPEND => {
                        let LiveValue::Array(mut list) = op[3].clone() else {
                            return Err(LiveError::InvalidArgument("a base list".into()));
                        };
                        list.push(op[2].clone());
                        let fields = BTreeMap::from([("list".to_string(), LiveValue::Array(list))]);
                        txn.patch(id, fields).await?;
                        out.push(LiveValue::Null);
                    }
                    _ => return Err(LiveError::InvalidArgument("an op's kind".into())),
                }
            }
            Ok(LiveValue::Array(out))
        })
    }
}

/// How a run ended, as the history records it.
fn outcome_of(e: &LiveError) -> Outcome {
    match e {
        LiveError::Txn(TxnError::Undetermined { .. } | TxnError::Deadline) => Outcome::Info,
        // Every other error means the transaction did not commit (row R9;
        // a function error rolls its attempt back).
        _ => Outcome::Fail,
    }
}

struct Setup {
    runner: Runner,
    /// Fresh snapshots for read-only transactions, on a handle without
    /// faults.
    reader: Runner,
    ids: Arc<Vec<DocId>>,
    plan: Arc<RandomFaults>,
}

async fn setup(
    cluster: &testing::TestCluster,
    opts: &Opts,
    docs: usize,
    fields: LiveValue,
) -> Setup {
    let tikv_config = cluster.config(TEST_LIVE);
    let config = LiveConfig::with_tikv("t16tx", tikv_config.clone());
    let plan = Arc::new(RandomFaults {
        rng: Mutex::new(ChaCha8Rng::seed_from_u64(opts.seed ^ 0xFA17)),
        injected: AtomicU64::new(0),
    });
    let mut tikv = Tikv::connect(tikv_config.clone()).await.expect("connects");
    if opts.faults {
        tikv = tikv.with_faults(plan.clone());
    }
    let runner = Runner::open(tikv, &config).await.expect("the runner opens");
    let reader = Runner::open(Tikv::connect(tikv_config).await.expect("connects"), &config)
        .await
        .expect("the reader opens");
    let insert = system::lookup(INSERT).expect("insert");
    let mut ids = Vec::new();
    for _ in 0..docs {
        let m = reader
            .mutate(
                insert.clone(),
                obj(&[
                    ("table", LiveValue::Str("lists".into())),
                    ("fields", fields.clone()),
                ]),
                None,
            )
            .await
            .expect("a document is created");
        let LiveValue::Str(id) = m.result else {
            panic!("an id")
        };
        ids.push(id.parse().expect("a document id"));
    }
    Setup {
        runner,
        reader,
        ids: Arc::new(ids),
        plan,
    }
}

#[derive(Debug, Default)]
#[allow(dead_code)] // read through Debug
struct Report {
    seed: u64,
    txns: usize,
    committed: usize,
    failed: usize,
    unknown: usize,
    read_only: usize,
    reruns: u64,
    faults_injected: u64,
    edges: [usize; 3],
    /// Every kind found, and the first 20 anomalies.
    kinds: Vec<Kind>,
    anomalies: Vec<String>,
}

async fn list_append(cluster: &testing::TestCluster, opts: Opts) -> Report {
    eprintln!("transaction checker: {opts:?}");
    let setup = Arc::new(
        setup(
            cluster,
            &opts,
            opts.keys,
            obj(&[("list", LiveValue::Array(Vec::new()))]),
        )
        .await,
    );
    let next_elem = Arc::new(AtomicU64::new(1));
    let reruns = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + opts.duration;
    let mut workers = tokio::task::JoinSet::new();
    for w in 0..opts.workers {
        let (setup, next_elem, reruns, opts) = (
            setup.clone(),
            next_elem.clone(),
            reruns.clone(),
            opts.clone(),
        );
        workers.spawn(async move {
            let mut rng = ChaCha8Rng::seed_from_u64(opts.seed ^ (0x7478 + w as u64));
            let mutation: Arc<dyn Function> = Arc::new(ListTxn {
                ids: setup.ids.clone(),
                kind: FnKind::Mutation,
            });
            let query = ListTxn {
                ids: setup.ids.clone(),
                kind: FnKind::Query,
            };
            let mut history = Vec::new();
            let mut read_only = 0usize;
            while Instant::now() < deadline {
                let read_only_txn = rng.random_bool(0.2);
                // The broken build appends alone, so its history shows
                // lost updates and nothing else.
                let n = if opts.stale_appends {
                    1
                } else {
                    rng.random_range(1..=4)
                };
                let mut ops: Vec<Op> = Vec::new();
                let mut args = Vec::new();
                for _ in 0..n {
                    // Skewed toward the first keys, for contention.
                    let key = (rng.random_range(0..opts.keys) * rng.random_range(1..=opts.keys))
                        / opts.keys;
                    let key = key as Key;
                    if read_only_txn || (!opts.stale_appends && rng.random_bool(0.4)) {
                        ops.push(Op::Read { key, list: None });
                        args.push(LiveValue::Array(vec![int(READ), int(key as i64)]));
                    } else {
                        let elem = next_elem.fetch_add(1, Ordering::Relaxed);
                        if opts.stale_appends {
                            // The broken build: the base list comes from a
                            // snapshot read before the transaction.
                            let at = setup.reader.tikv().now().await.expect("a timestamp");
                            let base = setup
                                .reader
                                .query(
                                    &query,
                                    LiveValue::Array(vec![LiveValue::Array(vec![
                                        int(READ),
                                        int(key as i64),
                                    ])]),
                                    at,
                                )
                                .await
                                .expect("a base read");
                            let LiveValue::Array(mut lists) = base.result else {
                                panic!("lists")
                            };
                            let base = lists.pop().expect("one list");
                            let LiveValue::Array(base_list) = &base else {
                                panic!("a list")
                            };
                            ops.push(Op::Read {
                                key,
                                list: Some(elems(base_list)),
                            });
                            ops.push(Op::Append { key, elem });
                            args.push(LiveValue::Array(vec![
                                int(STALE_APPEND),
                                int(key as i64),
                                int(elem as i64),
                                base,
                            ]));
                        } else {
                            ops.push(Op::Append { key, elem });
                            args.push(LiveValue::Array(vec![
                                int(APPEND),
                                int(key as i64),
                                int(elem as i64),
                            ]));
                        }
                    }
                }
                let args = LiveValue::Array(args);
                let (result, outcome) = if read_only_txn {
                    read_only += 1;
                    let at = match setup.reader.tikv().now().await {
                        Ok(at) => at,
                        Err(_) => continue,
                    };
                    match setup.reader.query(&query, args, at).await {
                        Ok(q) => (Some(q.result), Outcome::Ok),
                        // A failed read-only transaction observed nothing.
                        Err(_) => continue,
                    }
                } else {
                    match setup.runner.mutate(mutation.clone(), args, None).await {
                        Ok(m) => {
                            reruns.fetch_add(
                                u64::from(m.attempts.saturating_sub(1)),
                                Ordering::Relaxed,
                            );
                            (Some(m.result), Outcome::Ok)
                        }
                        Err(e) => {
                            if !opts.nemesis && !matches!(e, LiveError::Txn(_)) {
                                panic!("seed {}: a list-append failed: {e}", opts.seed);
                            }
                            (None, outcome_of(&e))
                        }
                    }
                };
                // Fill in the reads from the result (the stale build's
                // reads are its base lists, already filled).
                if let Some(LiveValue::Array(results)) = result {
                    let mut results = results.into_iter();
                    let mut filled = Vec::with_capacity(ops.len());
                    for op in ops {
                        match op {
                            Op::Read { key, list: None } => {
                                let r = results.next().expect("one result per op");
                                let LiveValue::Array(l) = r else {
                                    panic!("a read returns a list")
                                };
                                filled.push(Op::Read {
                                    key,
                                    list: Some(elems(&l)),
                                });
                            }
                            Op::Read { .. } => filled.push(op),
                            Op::Append { .. } => {
                                results.next();
                                filled.push(op);
                            }
                        }
                    }
                    history.push(Txn::new(filled, outcome));
                } else {
                    history.push(Txn::new(ops, outcome));
                }
            }
            (history, read_only)
        });
    }
    let mut history = Vec::new();
    let mut read_only = 0;
    while let Some(joined) = workers.join_next().await {
        let (h, r) = joined.expect("a worker");
        history.extend(h);
        read_only += r;
    }
    // The final read of every key closes the history.
    let query = ListTxn {
        ids: setup.ids.clone(),
        kind: FnKind::Query,
    };
    let all = LiveValue::Array(
        (0..opts.keys)
            .map(|k| LiveValue::Array(vec![int(READ), int(k as i64)]))
            .collect(),
    );
    let mut final_read = None;
    for _ in 0..240 {
        let Ok(at) = setup.reader.tikv().now().await else {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        };
        if let Ok(q) = setup.reader.query(&query, all.clone(), at).await {
            final_read = Some(q.result);
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let Some(LiveValue::Array(lists)) = final_read else {
        panic!("seed {}: the final read never succeeded", opts.seed)
    };
    history.push(Txn {
        ops: lists
            .iter()
            .enumerate()
            .map(|(k, l)| {
                let LiveValue::Array(l) = l else {
                    panic!("a list")
                };
                Op::Read {
                    key: k as Key,
                    list: Some(elems(l)),
                }
            })
            .collect(),
        outcome: Outcome::Ok,
        final_read: true,
    });
    let checked = elle::check(&history);
    let report = Report {
        seed: opts.seed,
        txns: checked.txns,
        committed: checked.committed,
        failed: history
            .iter()
            .filter(|t| t.outcome == Outcome::Fail)
            .count(),
        unknown: history
            .iter()
            .filter(|t| t.outcome == Outcome::Info)
            .count(),
        read_only,
        reruns: reruns.load(Ordering::Relaxed),
        faults_injected: setup.plan.injected.load(Ordering::Relaxed),
        edges: checked.edges,
        kinds: checked.kinds().into_iter().collect(),
        anomalies: checked
            .anomalies
            .iter()
            .take(20)
            .map(ToString::to_string)
            .collect(),
    };
    eprintln!("transaction checker report: {report:#?}");
    report
}

/// The gate: no anomaly at all on a list-append history with faults.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn txn_checker() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let report = list_append(&cluster, Opts::from_env()).await;
    assert!(
        report.committed > 100,
        "seed {}: too few commits: {report:?}",
        report.seed
    );
    assert!(
        report.edges[0] > 0 && report.edges[1] > 0,
        "seed {}: no dependencies to check",
        report.seed
    );
    assert!(
        report.anomalies.is_empty(),
        "seed {}: anomalies:\n{}",
        report.seed,
        report.anomalies.join("\n")
    );
}

/// The checker fails on a deliberately broken build: appends that write a
/// list read before their transaction lose each other's elements.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn checker_catches_injected_lost_update() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let opts = Opts {
        duration: Duration::from_secs(10),
        faults: false,
        nemesis: false,
        stale_appends: true,
        keys: 2,
        ..Opts::from_env()
    };
    let report = list_append(&cluster, opts).await;
    assert!(
        report.kinds.contains(&Kind::LostUpdate) && report.kinds.contains(&Kind::LostAppend),
        "seed {}: the broken build must show lost updates: {report:?}",
        report.seed
    );
}

/// Write skew on point reads never happens: `get` locks the documents a
/// mutation reads (D118).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn point_read_write_skew_never_happens() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let opts = Opts::from_env();
    const PAIRS: usize = 8;
    let rounds = if nightly() { 200 } else { 25 };
    let setup = setup(&cluster, &opts, PAIRS * 2, obj(&[("v", int(1))])).await;
    let skew: Arc<dyn Function> = Arc::new(OffCall {
        ids: setup.ids.clone(),
    });
    let get = system::lookup(system::GET).expect("get");
    let patch = system::lookup(system::PATCH).expect("patch");
    let mut both_committed = 0;
    for round in 0..rounds {
        let mut calls = Vec::new();
        for p in 0..PAIRS {
            for side in 0..2i64 {
                let (runner, skew) = (setup.runner.clone(), skew.clone());
                calls.push(tokio::spawn(async move {
                    runner
                        .mutate(skew, LiveValue::Array(vec![int(p as i64), int(side)]), None)
                        .await
                        .map(|m| m.result == LiveValue::Bool(true))
                }));
            }
        }
        let mut took = [0; PAIRS];
        for (i, c) in calls.into_iter().enumerate() {
            if let Ok(true) = c.await.expect("a call") {
                took[i / 2] += 1;
            }
        }
        for (p, took) in took.iter().enumerate() {
            if *took == 2 {
                both_committed += 1;
            }
            let mut on = 0;
            for side in 0..2 {
                let id = setup.ids[p * 2 + side];
                let at = setup.reader.tikv().now().await.expect("a timestamp");
                let doc = setup
                    .reader
                    .query(&*get, obj(&[("id", LiveValue::Str(id.to_string()))]), at)
                    .await
                    .expect("a read")
                    .result;
                let LiveValue::Object(fields) = doc else {
                    panic!("a document")
                };
                if fields.get("v") == Some(&int(1)) {
                    on += 1;
                }
                // Back on call for the next round.
                setup
                    .reader
                    .mutate(
                        patch.clone(),
                        obj(&[
                            ("id", LiveValue::Str(id.to_string())),
                            ("fields", obj(&[("v", int(1))])),
                        ]),
                        None,
                    )
                    .await
                    .expect("a reset");
            }
            assert!(
                on >= 1,
                "seed {}: round {round}, pair {p}: write skew left both off call",
                opts.seed
            );
        }
    }
    eprintln!(
        "write skew: {rounds} rounds × {PAIRS} pairs, {both_committed} pairs where both took one off"
    );
    assert_eq!(
        both_committed, 0,
        "seed {}: both mutations of a pair took a document off call",
        opts.seed
    );
}

/// Reads both documents of pair `args[0]`; if both are on call, takes side
/// `args[1]` off and returns `true`.
struct OffCall {
    ids: Arc<Vec<DocId>>,
}

impl Function for OffCall {
    fn name(&self) -> &str {
        "checker:off_call"
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
            let LiveValue::Array(a) = args else {
                return Err(LiveError::InvalidArgument("args".into()));
            };
            let (LiveValue::I64(p), LiveValue::I64(side)) = (&a[0], &a[1]) else {
                return Err(LiveError::InvalidArgument("pair and side".into()));
            };
            let p = usize::try_from(*p).expect("a pair");
            let mut on = 0;
            for s in 0..2 {
                let doc = txn
                    .get(self.ids[p * 2 + s])
                    .await?
                    .ok_or_else(|| LiveError::NotFound("a document".into()))?;
                if doc.fields.get("v") == Some(&int(1)) {
                    on += 1;
                }
            }
            if on < 2 {
                return Ok(LiveValue::Bool(false));
            }
            let id = self.ids[p * 2 + usize::try_from(*side).expect("a side")];
            txn.patch(id, BTreeMap::from([("v".to_string(), int(0))]))
                .await?;
            Ok(LiveValue::Bool(true))
        })
    }
}

// ---- commit latency (the exit report, Task 16 semantics 4) ----

/// The p50, p99 and max of `samples`.
fn percentiles(samples: &mut [Duration]) -> (Duration, Duration, Duration) {
    samples.sort();
    let at = |p: usize| samples[(samples.len() - 1) * p / 100];
    (at(50), at(99), at(100))
}

/// Live mutation latency, `two_pc` against `async_1pc`, interleaved one by
/// one on one writer as the spike measured (R1 plan Task 16 semantics 4).
/// Runs only with `OPERON_TEST_LATENCY=1` (`OPERON_TEST_LATENCY_N`
/// mutations per mode, default 300); the exit report records its output.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_latency_interleaved() {
    if std::env::var("OPERON_TEST_LATENCY").map_or(true, |v| v != "1") {
        eprintln!("skipped: commit_latency_interleaved needs OPERON_TEST_LATENCY=1");
        return;
    }
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let n: usize = std::env::var("OPERON_TEST_LATENCY_N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);
    let tikv_config = cluster.config(TEST_LIVE);
    let config = LiveConfig::with_tikv("t16lat", tikv_config.clone());
    let mut runners = Vec::new();
    for mode in [CommitMode::TwoPc, CommitMode::Async1pc] {
        let tikv = Tikv::connect(tikv_config.clone()).await.expect("connects");
        let options = RunnerOptions {
            commit_mode: mode,
            ..RunnerOptions::default()
        };
        runners.push((
            mode,
            Runner::open_with(tikv, &config, options)
                .await
                .expect("opens"),
        ));
    }
    let insert = system::lookup(INSERT).expect("insert");
    let doc = |v: i64| {
        obj(&[
            ("table", LiveValue::Str("lat".into())),
            ("fields", obj(&[("v", int(v))])),
        ])
    };
    // Warm both handles (connections, the region cache, the table record).
    for (_, r) in &runners {
        for _ in 0..20 {
            r.mutate(insert.clone(), doc(0), None)
                .await
                .expect("a warm-up insert");
        }
    }
    let mut samples = vec![Vec::with_capacity(n), Vec::with_capacity(n)];
    for i in 0..n * 2 {
        let (_, r) = &runners[i % 2];
        let started = Instant::now();
        r.mutate(insert.clone(), doc(i as i64), None)
            .await
            .expect("an insert");
        samples[i % 2].push(started.elapsed());
    }
    for ((mode, _), s) in runners.iter().zip(&mut samples) {
        let (p50, p99, max) = percentiles(s);
        eprintln!(
            "Live _system:insert, {mode:?}, {n} interleaved: p50 {p50:?} p99 {p99:?} max {max:?}"
        );
    }
}
