//! Concurrent histories through every handle, checked for linearizability
//! (M0.4 ruling 6), with the backend disturbed when it can be.
//!
//! Outcomes are classified as the simulation classifies them: `Ok` is the
//! output; `Rejected` or `ClockSkew` without an earlier attempt of unknown
//! outcome was not applied and is not recorded; anything else is
//! `Indeterminate`, with its completion at infinity (M0.4 E14).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use operon_common::meta::{
    ApplyError, Consistency, MetaError, MetaStore, Tracked, WalChunk, WalCommit,
};
use operon_common::{NamespaceId, StreamId};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use super::{cas, namespace, stream};
use crate::linearizability::{
    CasInput, CasOutput, CasRegisterModel, Model, Op, Outcome, SequencerInput, SequencerModel,
    SequencerOutput, check,
};
use crate::{Backend, Faults, Instance};

const TASKS: usize = 4;
const OPS_PER_TASK: u64 = 40;
const SEED: u64 = 0x6d65_7461_7374_6f72;

/// Histories by object, a logical clock and a completed-operation count.
struct Recorder<I, R> {
    clock: AtomicU64,
    done: AtomicU64,
    histories: Mutex<BTreeMap<String, Vec<Op<I, R>>>>,
}

impl<I, R> Recorder<I, R> {
    fn new() -> Self {
        Self {
            clock: AtomicU64::new(0),
            done: AtomicU64::new(0),
            histories: Mutex::new(BTreeMap::new()),
        }
    }

    fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::SeqCst)
    }

    fn record(&self, object: &str, op: Op<I, R>) {
        self.histories
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(object.to_string())
            .or_default()
            .push(op);
    }

    fn finished(&self) {
        self.done.fetch_add(1, Ordering::SeqCst);
    }

    fn into_histories(self) -> BTreeMap<String, Vec<Op<I, R>>> {
        self.histories
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// How a tracked write ended, as the simulation classifies it: `Some(output)`
/// or `Some(Indeterminate)` to record, `None` when it was definitely not
/// applied. `mismatch` turns a rejection into an output when there was no
/// earlier attempt of unknown outcome.
fn classify<T, R>(
    tracked: Tracked<T>,
    ok: impl FnOnce(T) -> R,
    mismatch: impl FnOnce(&ApplyError) -> Option<R>,
) -> Option<Outcome<R>> {
    let Tracked {
        result,
        earlier_unknown,
    } = tracked;
    match result {
        Ok(value) => Some(Outcome::Ok(ok(value))),
        Err(MetaError::Rejected(err)) if !earlier_unknown => mismatch(&err).map(Outcome::Ok),
        Err(MetaError::ClockSkew { .. }) if !earlier_unknown => None,
        Err(_) => Some(Outcome::Indeterminate),
    }
}

/// With faults, disturbs the backend twice while the workers run (once a
/// quarter and again once 60 % of the operations completed), healing it
/// 25 operations later each time; heals it at the end in any case.
async fn disturber(faults: Option<Arc<dyn Faults>>, done: impl Fn() -> u64, stop: Arc<AtomicU64>) {
    let Some(faults) = faults else {
        return;
    };
    let total = TASKS as u64 * OPS_PER_TASK;
    for (round, at) in [(0, total / 4), (1, total * 6 / 10)] {
        while done() < at && stop.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        if stop.load(Ordering::SeqCst) != 0 {
            break;
        }
        faults.disturb(SEED ^ round).await;
        let heal_at = done() + 25;
        while done() < heal_at && stop.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        faults.heal().await;
    }
}

/// Runs `TASKS` workers of `OPS_PER_TASK` operations each, worker `i` over
/// `clients[i % n]`, alongside the disturber; then checks every history.
async fn run_and_check<M, W, Fut>(db: &Instance, rec: Arc<Recorder<M::Input, M::Output>>, worker: W)
where
    M: Model + Default + Send + 'static,
    M::Input: Send + 'static,
    M::Output: Send + 'static,
    W: Fn(usize, Arc<dyn MetaStore>, Arc<Recorder<M::Input, M::Output>>) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let stop = Arc::new(AtomicU64::new(0));
    let progress = rec.clone();
    let disturbing = tokio::spawn(disturber(
        db.faults.clone(),
        move || progress.done.load(Ordering::SeqCst),
        stop.clone(),
    ));
    let mut workers = Vec::new();
    for task in 0..TASKS {
        let meta = db.clients[task % db.len()].clone();
        workers.push(tokio::spawn(worker(task, meta, rec.clone())));
    }
    for w in workers {
        w.await.expect("worker");
    }
    stop.store(1, Ordering::SeqCst);
    disturbing.await.expect("disturber");
    if let Some(faults) = &db.faults {
        faults.heal().await;
    }
    let rec = Arc::try_unwrap(rec).unwrap_or_else(|_| panic!("a worker still holds the recorder"));
    let histories = rec.into_histories();
    assert!(!histories.is_empty(), "nothing was recorded");
    for (object, history) in histories {
        if let Err(violation) = check(M::default(), &history) {
            panic!(
                "seed {SEED:#x}: {object} is not linearizable: {violation}\nhistory: {history:#?}"
            );
        }
    }
}

pub async fn concurrent_cas_on_two_keys_is_linearizable(backend: &dyn Backend) {
    let db = backend.start().await;
    let ns = namespace(db.first(), "lin-cas").await;
    let rec = Arc::new(Recorder::<CasInput, CasOutput>::new());
    run_and_check::<CasRegisterModel, _, _>(&db, rec, move |task, meta, rec| async move {
        let mut rng = ChaCha8Rng::seed_from_u64(SEED ^ task as u64);
        for op in 0..OPS_PER_TASK {
            let key = if rng.random_bool(0.5) {
                "lin-cas/a"
            } else {
                "lin-cas/b"
            };
            cas_op(&*meta, &rec, ns, key, task, op).await;
            rec.finished();
        }
    })
    .await;
}

/// A `Linearizable` read of the pointer, then a CAS from what it saw.
async fn cas_op(
    meta: &dyn MetaStore,
    rec: &Recorder<CasInput, CasOutput>,
    ns: NamespaceId,
    key: &str,
    task: usize,
    op: u64,
) {
    let client = u32::try_from(task).unwrap_or(u32::MAX);
    let invoke = rec.tick();
    let read = meta.pointer(Consistency::Linearizable, ns, key).await;
    let complete = rec.tick();
    let Ok(current) = read else {
        return;
    };
    rec.record(
        key,
        Op {
            client,
            invoke,
            complete,
            input: CasInput::Read,
            outcome: Outcome::Ok(CasOutput::Read(
                current.as_ref().map(|p| (p.version, p.value.clone())),
            )),
        },
    );
    let expected = current.map(|p| p.version);
    let value = format!("t{task}-{op}");
    let invoke = rec.tick();
    let tracked = meta.cas_pointer(cas(ns, key, expected, &value)).await;
    let complete = rec.tick();
    let outcome = classify(tracked, CasOutput::Ok, |err| match err {
        // A mismatch after an attempt of unknown outcome may be our own
        // first attempt's effect, so only a first-attempt one is an output.
        ApplyError::VersionMismatch { current } => Some(CasOutput::Mismatch(
            current.as_ref().map(|p| (p.version, p.value.clone())),
        )),
        _ => None,
    });
    let Some(outcome) = outcome else {
        return;
    };
    let complete = if outcome == Outcome::Indeterminate {
        u64::MAX
    } else {
        complete
    };
    rec.record(
        key,
        Op {
            client,
            invoke,
            complete,
            input: CasInput::Cas { expected, value },
            outcome,
        },
    );
}

pub async fn concurrent_wal_commits_are_linearizable(backend: &dyn Backend) {
    let db = backend.start().await;
    let ns = namespace(db.first(), "lin-wal").await;
    let s = stream(db.first(), ns, "events", 2).await;
    let rec = Arc::new(Recorder::<SequencerInput, SequencerOutput>::new());
    run_and_check::<SequencerModel, _, _>(&db, rec, move |task, meta, rec| async move {
        let mut rng = ChaCha8Rng::seed_from_u64(SEED ^ 0x77 ^ task as u64);
        // This task's earlier commits: (partition, object, records).
        let mut earlier: Vec<(u32, String, u32)> = Vec::new();
        for op in 0..OPS_PER_TASK {
            let roll = rng.random_range(0..100u32);
            if roll < 20 {
                let partition = rng.random_range(0..2u32);
                read_hwm(&*meta, &rec, s, partition, task).await;
            } else if roll < 30 && !earlier.is_empty() {
                let (partition, object, records) =
                    earlier[rng.random_range(0..earlier.len())].clone();
                commit_op(&*meta, &rec, s, partition, object, records, task).await;
            } else {
                let partition = rng.random_range(0..2u32);
                let records = rng.random_range(1..=5u32);
                let object = format!("lin-wal/t{task}-{op}");
                earlier.push((partition, object.clone(), records));
                commit_op(&*meta, &rec, s, partition, object, records, task).await;
            }
            rec.finished();
        }
    })
    .await;
}

fn history(partition: u32) -> String {
    format!("partition/{partition}")
}

/// A `commit_wal` of one chunk of `records` records under `object`.
async fn commit_op(
    meta: &dyn MetaStore,
    rec: &Recorder<SequencerInput, SequencerOutput>,
    s: StreamId,
    partition: u32,
    object: String,
    records: u32,
    task: usize,
) {
    let commit = WalCommit {
        object: object.clone(),
        created_at_ms: meta.now_ms(),
        chunks: vec![WalChunk {
            stream: s,
            partition,
            records,
            byte_range: 0..u64::from(records) * 10,
            max_timestamp_ms: 0,
        }],
    };
    let invoke = rec.tick();
    let tracked = meta.commit_wal(commit).await;
    let complete = rec.tick();
    let outcome = classify(
        tracked,
        |offsets| match offsets.as_slice() {
            [base] => SequencerOutput::BaseOffset(*base),
            // Not a base offset the model can produce: a violation.
            _ => SequencerOutput::Hwm(u64::MAX),
        },
        |_| None,
    );
    let Some(outcome) = outcome else {
        return;
    };
    let complete = if outcome == Outcome::Indeterminate {
        u64::MAX
    } else {
        complete
    };
    rec.record(
        &history(partition),
        Op {
            client: u32::try_from(task).unwrap_or(u32::MAX),
            invoke,
            complete,
            input: SequencerInput::Commit { object, records },
            outcome,
        },
    );
}

/// A `Linearizable` read of a partition's high watermark, through
/// `partition_index` from the end.
async fn read_hwm(
    meta: &dyn MetaStore,
    rec: &Recorder<SequencerInput, SequencerOutput>,
    s: StreamId,
    partition: u32,
    task: usize,
) {
    let invoke = rec.tick();
    let read = meta
        .partition_index(Consistency::Linearizable, s, partition, u64::MAX, Some(0))
        .await;
    let complete = rec.tick();
    if let Ok(Some(index)) = read {
        rec.record(
            &history(partition),
            Op {
                client: u32::try_from(task).unwrap_or(u32::MAX),
                invoke,
                complete,
                input: SequencerInput::ReadHwm,
                outcome: Outcome::Ok(SequencerOutput::Hwm(index.high_watermark())),
            },
        );
    }
}
