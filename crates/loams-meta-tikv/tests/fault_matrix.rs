//! The TiKV metastore fault matrix (R1 plan Task 6; design §18 §4.2).
//!
//! Every cell crosses one method group with one fault, hitting either the
//! first attempt of the group's transaction or its second (the second
//! attempt is reached by a `Conflict` injected before the first one's
//! commit, so the fault lands on the retry path). An attempt counts when it
//! reaches the fault's point: one the runner restarts earlier (a lock left
//! by the previous partition group's commit, a pessimistic retry) does not.
//! Each cell runs on a fresh root with two handles: the faulted one (with
//! the cell's `FaultPlan`) and a clean one for setup, the checks and the
//! `Race` competitor.
//!
//! The faults, as the runner's fault points implement them (`faults.rs`):
//! - `Refuse` (`BeforeSend`): `Refuse` at `BeforeBegin`, not applied;
//! - `LoseAck` (`AfterApply`): `LoseAck` at `AfterCommit`, applied and the
//!   acknowledgement lost, resolved through the commit token;
//! - `Undetermined`: `LoseAck` at `BeforeCommit`, an unknown outcome for a
//!   commit that was never sent, which the token resolution fences and turns
//!   into "not applied";
//! - `Conflict`: at `BeforeCommit`;
//! - `Delay`: 500 ms at `BeforeCommit` (a pessimistic write holds its locks
//!   meanwhile);
//! - `Race`: at `BeforeCommit` the clean handle starts a competing write on
//!   what the call is about to write, and the cell waits up to 500 ms for it.
//!   An optimistic call has no locks yet, so the competitor commits first and
//!   the call meets it; a pessimistic call holds its locks, so the competitor
//!   waits and restarts after the call commits (row T5-2).
//!
//! Each cell ends:
//! - `Retried`: the fault fired and the call succeeded as without it;
//! - `SurfacedUnknown`: the call reported an earlier unknown outcome
//!   (`earlier_unknown`, the trait's retry form of a result, or a timeout);
//! - `SurfacedRetryable`: the call failed with a retryable error;
//! - `NoEffect`: the fault never fired;
//! - `Rejected`: the call was refused because a competitor won (`Race` only,
//!   row T6-3).
//!
//! The outcome table, and the `Race` competitors' outcomes, must equal
//! `tests/meta_fault_matrix.tikv.expected.md` (`BLESS=1` rewrites it; review
//! the diff). After every cell, with the faults off: the metastore's
//! invariants hold (`TikvMeta::check_invariants`), no WAL object is committed
//! twice (a clean retry returns the same offsets and the high watermarks
//! count its records once), no acknowledged write is lost, and pointer
//! versions and lease epochs are dense.
//!
//! Also here: a large drop that fails midway applies all or nothing (T5-3,
//! T4-8), once through a refusal on every attempt and once through
//! `tikv-client`'s `after-prewrite` failpoint.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use loams_common::StreamId;
use loams_common::meta::{
    ApplyError, Consistency, EntryKind, Fence, Freshness, MetaError, MetaStore, PointerCas,
    Retention, SegmentSwap, WalChunk, WalClass, WalCommit,
};
use loams_common::schema::{CollectionSchema, DynamicMapping};
use loams_common::{CollectionId, NamespaceId};
use loams_meta_tikv::{MAX_GROUP_CHUNKS, TikvMeta};
use loams_tikv::testing::{self, TEST_META, TestCluster};
use loams_tikv::{Fault, FaultPlan, FaultPoint, Tikv, TikvConfig};

const L: Consistency = Consistency::Linearizable;

/// How long a `Race` waits for its competitor before the call goes on.
const RACE_WAIT: Duration = Duration::from_millis(500);
/// The `Delay` fault.
const DELAY: Duration = Duration::from_millis(500);
/// Cells run at once.
const PARALLEL: usize = 6;
/// Partitions of the large collection, and its chunks (one group).
const LARGE_PARTITIONS: u32 = 64;
const EXPECTED: &str = "meta_fault_matrix.tikv.expected.md";

/// The failpoint test and the matrix never overlap: `tikv-client`'s
/// failpoints are process-wide.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// ---- The matrix's axes ----

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Group {
    CreateNamespace,
    CreateCollection,
    CommitWal,
    /// A call over two partition groups, the fault in the first group.
    CommitWalGroup1,
    /// The same, the fault in the second group (the first has committed).
    CommitWalGroup2,
    SwapSegment,
    Trim,
    CasPointer,
    CasPointerFenced,
    Lease,
    DropCollection,
    /// A collection of 64 partitions and 1 024 index entries: one
    /// transaction (T4-8, T5-3).
    DropLarge,
}

const GROUPS: [Group; 12] = [
    Group::CreateNamespace,
    Group::CreateCollection,
    Group::CommitWal,
    Group::CommitWalGroup1,
    Group::CommitWalGroup2,
    Group::SwapSegment,
    Group::Trim,
    Group::CasPointer,
    Group::CasPointerFenced,
    Group::Lease,
    Group::DropCollection,
    Group::DropLarge,
];

impl Group {
    fn name(self) -> &'static str {
        match self {
            Group::CreateNamespace => "create_namespace",
            Group::CreateCollection => "create_collection",
            Group::CommitWal => "commit_wal",
            Group::CommitWalGroup1 => "commit_wal (2 groups, fault in group 1)",
            Group::CommitWalGroup2 => "commit_wal (2 groups, fault in group 2)",
            Group::SwapSegment => "swap_segment",
            Group::Trim => "trim_partition",
            Group::CasPointer => "cas_pointer",
            Group::CasPointerFenced => "cas_pointer (fenced)",
            Group::Lease => "acquire_lease",
            Group::DropCollection => "drop_collection",
            Group::DropLarge => "drop_collection (64 partitions, 1 024 entries)",
        }
    }

    /// The runner's name for the group's transaction.
    fn op(self) -> &'static str {
        match self {
            Group::CreateNamespace => "meta.create_namespace",
            Group::CreateCollection => "meta.create_collection",
            Group::CommitWal | Group::CommitWalGroup1 | Group::CommitWalGroup2 => "meta.commit_wal",
            Group::SwapSegment => "meta.swap_segment",
            Group::Trim => "meta.trim_partition",
            Group::CasPointer | Group::CasPointerFenced => "meta.cas_pointer",
            Group::Lease => "meta.acquire_lease",
            Group::DropCollection | Group::DropLarge => "meta.drop_collection",
        }
    }

    /// Whether the cell writes a 1 024-entry transaction.
    fn large(self) -> bool {
        matches!(
            self,
            Group::CommitWalGroup1 | Group::CommitWalGroup2 | Group::DropLarge
        )
    }

    /// Which run of the op the fault hits (a run is one call of the
    /// runner; each partition group is one).
    fn run(self) -> u32 {
        match self {
            Group::CommitWalGroup2 => 2,
            _ => 1,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
    Refuse,
    LoseAck,
    Undetermined,
    Conflict,
    Delay,
    Race,
}

const KINDS: [Kind; 6] = [
    Kind::Refuse,
    Kind::LoseAck,
    Kind::Undetermined,
    Kind::Conflict,
    Kind::Delay,
    Kind::Race,
];

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Refuse => "Refuse (BeforeSend)",
            Kind::LoseAck => "LoseAck (AfterApply)",
            Kind::Undetermined => "Undetermined",
            Kind::Conflict => "Conflict",
            Kind::Delay => "Delay(500ms)",
            Kind::Race => "Race",
        }
    }

    fn point(self) -> FaultPoint {
        match self {
            Kind::Refuse => FaultPoint::BeforeBegin,
            Kind::LoseAck => FaultPoint::AfterCommit,
            Kind::Undetermined | Kind::Conflict | Kind::Delay | Kind::Race => {
                FaultPoint::BeforeCommit
            }
        }
    }

    fn fault(self) -> Option<Fault> {
        match self {
            Kind::Refuse => Some(Fault::Refuse),
            Kind::LoseAck | Kind::Undetermined => Some(Fault::LoseAck),
            Kind::Conflict => Some(Fault::Conflict),
            Kind::Delay => Some(Fault::Delay(DELAY)),
            Kind::Race => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Retried,
    SurfacedUnknown,
    SurfacedRetryable,
    NoEffect,
    Rejected,
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

// ---- The cell's fault plan ----

/// A competitor's outcome.
type Competitor = BoxFuture<'static, String>;

/// One cell's plan: the fault fires once, the first time run `run` of `op`
/// reaches `kind.point()`; for attempt 2, a `Conflict` at the run's first
/// `BeforeCommit` comes first and forces a retry.
struct CellPlan {
    op: &'static str,
    kind: Kind,
    attempt: u32,
    run: u32,
    runs: AtomicU32,
    primed: AtomicBool,
    fired: AtomicBool,
    race: Mutex<Option<Competitor>>,
    competitor: Mutex<Option<tokio::task::JoinHandle<String>>>,
}

impl CellPlan {
    fn new(group: Group, kind: Kind, attempt: u32) -> Self {
        CellPlan {
            op: group.op(),
            kind,
            attempt,
            run: group.run(),
            runs: AtomicU32::new(0),
            primed: AtomicBool::new(false),
            fired: AtomicBool::new(false),
            race: Mutex::new(None),
            competitor: Mutex::new(None),
        }
    }

    fn fired(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }

    /// Starts the competitor and waits up to [`RACE_WAIT`] for it.
    fn race(&self) {
        let Some(competitor) = self.race.lock().expect("lock").take() else {
            return;
        };
        let (done, wait) = std::sync::mpsc::channel();
        let handle = tokio::spawn(async move {
            let outcome = competitor.await;
            let _ = done.send(());
            outcome
        });
        tokio::task::block_in_place(|| {
            let _ = wait.recv_timeout(RACE_WAIT);
        });
        *self.competitor.lock().expect("lock") = Some(handle);
    }
}

impl FaultPlan for CellPlan {
    fn at(&self, op: &str, point: FaultPoint, attempt: u32) -> Option<Fault> {
        if op != self.op {
            return None;
        }
        if point == FaultPoint::BeforeBegin && attempt == 1 {
            self.runs.fetch_add(1, Ordering::SeqCst);
        }
        if self.runs.load(Ordering::SeqCst) != self.run {
            return None;
        }
        // Attempts are counted by the points they reach, not by the runner's
        // number: an attempt that restarts before its commit (a lock left by
        // the previous group's commit, a pessimistic retry) does not count.
        if self.attempt == 2
            && point == FaultPoint::BeforeCommit
            && !self.primed.swap(true, Ordering::SeqCst)
        {
            return Some(Fault::Conflict);
        }
        if point != self.kind.point() || (self.attempt == 2 && !self.primed.load(Ordering::SeqCst))
        {
            return None;
        }
        if self.fired.swap(true, Ordering::SeqCst) {
            return None;
        }
        match self.kind {
            Kind::Race => {
                self.race();
                None
            }
            kind => kind.fault(),
        }
    }
}

// ---- Cells ----

/// What a call reported.
struct Call {
    /// `Ok(true)`: the result a fault-free call returns; `Ok(false)`: the
    /// trait's retry form of it (an `…Exists` with the id, `None` from a
    /// drop).
    result: Result<bool, MetaError>,
    unknown: bool,
}

impl Call {
    fn tracked<T>(t: loams_common::meta::Tracked<T>) -> Self {
        Call {
            result: t.result.map(|_| true),
            unknown: t.earlier_unknown,
        }
    }

    fn plain<T>(result: Result<T, MetaError>) -> Self {
        Call {
            result: result.map(|_| true),
            unknown: false,
        }
    }
}

fn classify(fired: bool, kind: Kind, call: &Call) -> Outcome {
    if !fired {
        return Outcome::NoEffect;
    }
    if call.unknown {
        return Outcome::SurfacedUnknown;
    }
    match &call.result {
        Ok(true) => Outcome::Retried,
        Ok(false) => Outcome::SurfacedUnknown,
        Err(MetaError::Timeout) => Outcome::SurfacedUnknown,
        Err(MetaError::Unavailable(_) | MetaError::NotLeader { .. }) => Outcome::SurfacedRetryable,
        Err(MetaError::Rejected(_)) if kind == Kind::Race => Outcome::Rejected,
        Err(e) => panic!("a non-retryable error fails the matrix: {e:?}"),
    }
}

fn one_record(stream: StreamId, partition: u32) -> WalChunk {
    WalChunk {
        stream,
        partition,
        records: 1,
        byte_range: 0..10,
        max_timestamp_ms: 0,
    }
}

fn wal(meta: &TikvMeta, object: &str, chunks: Vec<WalChunk>) -> WalCommit {
    WalCommit {
        object: object.to_string(),
        created_at_ms: meta.now_ms(),
        chunks,
    }
}

fn schema() -> CollectionSchema {
    CollectionSchema::new(Vec::new(), Vec::new(), DynamicMapping::Ignore)
}

async fn open(config: &TikvConfig, plan: Option<Arc<dyn FaultPlan>>) -> TikvMeta {
    let mut tikv = Tikv::connect(config.clone())
        .await
        .expect("connect to the test keyspace");
    if let Some(plan) = plan {
        tikv = tikv.with_faults(plan);
    }
    TikvMeta::open_on(
        tikv,
        loams_meta_tikv::DEFAULT_ID_BLOCK,
        loams_meta_tikv::DEFAULT_POLL,
    )
    .await
    .expect("open the metastore")
}

async fn stream(meta: &TikvMeta, ns: NamespaceId, name: &str, partitions: u32) -> StreamId {
    meta.create_stream(
        ns,
        name,
        partitions,
        WalClass::Standard,
        Retention::default(),
    )
    .await
    .expect("stream")
}

/// The sum of a stream's high watermarks.
async fn records_in(meta: &TikvMeta, s: StreamId) -> u64 {
    meta.stream_state(L, s)
        .await
        .expect("read")
        .map_or(0, |state| {
            state
                .partitions
                .into_iter()
                .map(|b| b.map_or(0, |b| b.high_watermark))
                .sum()
        })
}

/// A commit_wal competitor: one record into `stream`/`partition` under a
/// new object, labelled with whether it restarted.
fn commit_competitor(clean: &TikvMeta, stream: StreamId, partition: u32) -> Competitor {
    let clean = clean.clone();
    Box::pin(async move {
        let before = clean.tikv().stats().restarts;
        let request = wal(
            &clean,
            "fm/wal-competitor",
            vec![one_record(stream, partition)],
        );
        let result = clean.commit_wal(request).await.into_result();
        label(result, clean.tikv().stats().restarts > before)
    })
}

fn label<T>(result: Result<T, MetaError>, restarted: bool) -> String {
    match (result, restarted) {
        (Ok(_), false) => "committed".to_string(),
        (Ok(_), true) => "committed after a restart".to_string(),
        (Err(MetaError::Rejected(e)), _) => format!("rejected ({})", rejection(&e)),
        (Err(e), _) => format!("failed ({e})"),
    }
}

fn rejection(e: &ApplyError) -> &'static str {
    match e {
        ApplyError::NamespaceExists(_) => "NamespaceExists",
        ApplyError::CollectionExists(_) => "CollectionExists",
        ApplyError::StreamNotFound(_) => "StreamNotFound",
        ApplyError::LeaseHeld { .. } => "LeaseHeld",
        ApplyError::VersionMismatch { .. } => "VersionMismatch",
        ApplyError::LeaseLost { .. } => "LeaseLost",
        ApplyError::Fenced { .. } => "Fenced",
        _ => "other",
    }
}

/// The result of one cell.
struct Ran {
    outcome: Outcome,
    competitor: Option<String>,
}

/// Runs one cell and checks the invariants after it.
async fn run_cell(cluster: &TestCluster, group: Group, kind: Kind, attempt: u32) -> Ran {
    let config = cluster.config(TEST_META);
    let plan = Arc::new(CellPlan::new(group, kind, attempt));
    let meta = open(&config, Some(plan.clone())).await;
    let clean = open(&config, None).await;
    let what = format!("{} × {} @ {attempt}", group.name(), kind.name());
    let mut problems = Vec::new();
    let mut competitor: Option<String> = None;
    let call = match group {
        Group::CreateNamespace => {
            if kind == Kind::Race {
                let clean = clean.clone();
                *plan.race.lock().expect("lock") = Some(Box::pin(async move {
                    let before = clean.tikv().stats().restarts;
                    let r = clean.create_namespace("fm-ns").await;
                    label(r, clean.tikv().stats().restarts > before)
                }));
            }
            let result = meta.create_namespace("fm-ns").await;
            let found = clean
                .namespace_by_name(L, "fm-ns")
                .await
                .expect("read")
                .map(|n| n.id);
            match (&result, found) {
                (Ok(id), Some(f)) if *id != f => problems.push(format!("created {id}, found {f}")),
                (Ok(_), None) => problems.push("an acknowledged namespace is missing".into()),
                _ => {}
            }
            let exists = matches!(
                result,
                Err(MetaError::Rejected(ApplyError::NamespaceExists(_)))
            );
            Call {
                result: match result {
                    Ok(_) => Ok(true),
                    Err(_) if exists && kind != Kind::Race => Ok(false),
                    Err(e) => Err(e),
                },
                unknown: false,
            }
        }
        Group::CreateCollection => {
            let ns = clean.create_namespace("fm").await.expect("namespace");
            if kind == Kind::Race {
                let clean = clean.clone();
                *plan.race.lock().expect("lock") = Some(Box::pin(async move {
                    let before = clean.tikv().stats().restarts;
                    let r = clean.create_collection(ns, "docs", schema(), 2).await;
                    label(r, clean.tikv().stats().restarts > before)
                }));
            }
            let result = meta.create_collection(ns, "docs", schema(), 2).await;
            let found = clean
                .resolve_collection(L, ns, "docs")
                .await
                .expect("read")
                .map(|c| (c.id, c.stream, c.link));
            match (&result, found) {
                (Ok(ids), Some(f)) if *ids != f => {
                    problems.push(format!("created {ids:?}, found {f:?}"));
                }
                (Ok(_), None) => problems.push("an acknowledged collection is missing".into()),
                _ => {}
            }
            let exists = matches!(
                result,
                Err(MetaError::Rejected(ApplyError::CollectionExists(_)))
            );
            Call {
                result: match result {
                    Ok(_) => Ok(true),
                    Err(_) if exists && kind != Kind::Race => Ok(false),
                    Err(e) => Err(e),
                },
                unknown: false,
            }
        }
        Group::CommitWal | Group::CommitWalGroup1 | Group::CommitWalGroup2 => {
            let ns = clean.create_namespace("fm").await.expect("namespace");
            let (streams, chunks) = if group == Group::CommitWal {
                let s = stream(&clean, ns, "events", 2).await;
                (vec![s], vec![one_record(s, 0), one_record(s, 1)])
            } else {
                // One stream of exactly one group's chunks, then another:
                // two groups.
                let a = stream(&clean, ns, "a", 4).await;
                let b = stream(&clean, ns, "b", 1).await;
                let limit = u32::try_from(MAX_GROUP_CHUNKS).expect("limit");
                let mut chunks: Vec<WalChunk> = (0..limit).map(|i| one_record(a, i % 4)).collect();
                chunks.push(one_record(b, 0));
                (vec![a, b], chunks)
            };
            let competitor_stream = *streams.last().expect("a stream");
            if kind == Kind::Race {
                *plan.race.lock().expect("lock") =
                    Some(commit_competitor(&clean, competitor_stream, 0));
            }
            let request = wal(&meta, "fm/wal-1", chunks.clone());
            let tracked = meta.commit_wal(request.clone()).await;
            let offsets = tracked.result.as_ref().ok().cloned();
            let call = Call::tracked(tracked);
            competitor = join_competitor(&plan).await;
            let competitor_ok = competitor
                .as_deref()
                .is_some_and(|c| c.starts_with("committed"));
            // No WAL object committed twice: a clean retry returns the
            // same offsets, and the records are counted once.
            let retry = clean
                .commit_wal(WalCommit {
                    created_at_ms: clean.now_ms(),
                    ..request
                })
                .await
                .into_result();
            match (&offsets, &retry) {
                (_, Err(e)) => problems.push(format!("the clean retry failed: {e:?}")),
                (Some(first), Ok(again)) if first != again => {
                    problems.push(format!(
                        "the retry returned other offsets: {first:?} vs {again:?}"
                    ));
                }
                _ => {}
            }
            let mut total = 0;
            for &s in &streams {
                total += records_in(&clean, s).await;
            }
            let expected = chunks.len() as u64 + u64::from(competitor_ok);
            if total != expected {
                problems.push(format!("{total} records committed, expected {expected}"));
            }
            call
        }
        Group::SwapSegment | Group::Trim => {
            let ns = clean.create_namespace("fm").await.expect("namespace");
            let s = stream(&clean, ns, "events", 1).await;
            for object in ["fm/wal-a", "fm/wal-b", "fm/wal-c"] {
                clean
                    .commit_wal(wal(&clean, object, vec![one_record(s, 0)]))
                    .await
                    .into_result()
                    .expect("setup commit");
            }
            if kind == Kind::Race {
                *plan.race.lock().expect("lock") = Some(commit_competitor(&clean, s, 0));
            }
            let call = if group == Group::SwapSegment {
                let swap = SegmentSwap {
                    stream: s,
                    partition: 0,
                    replaces: vec![(0, "fm/wal-a".to_string())],
                    segment: "fm/seg-1".to_string(),
                    byte_range: 0..10,
                    max_timestamp_ms: 0,
                    fence: None,
                    fresh: Freshness {
                        created_at_ms: meta.now_ms(),
                        max_age_ms: 600_000,
                    },
                };
                let call = Call::tracked(meta.swap_segment(swap.clone()).await);
                if let Err(e) = clean.swap_segment(swap).await.into_result() {
                    problems.push(format!("the clean retry of the swap failed: {e:?}"));
                }
                let index = clean
                    .partition_index(L, s, 0, 0, None)
                    .await
                    .expect("read")
                    .expect("partition");
                let first = index.entries().next().cloned();
                if !first.is_some_and(|e| e.kind == EntryKind::Segment && e.object == "fm/seg-1") {
                    problems.push("the swapped segment is not the first entry".into());
                }
                call
            } else {
                let result = meta.trim_partition(s, 0, 2, None).await;
                if let Ok(start) = &result
                    && *start != 2
                {
                    problems.push(format!("the trim returned log start {start}"));
                }
                if clean.trim_partition(s, 0, 2, None).await.ok() != Some(2) {
                    problems.push("the clean retry of the trim did not return 2".into());
                }
                Call::plain(result)
            };
            competitor = join_competitor(&plan).await;
            let competitor_ok = competitor
                .as_deref()
                .is_some_and(|c| c.starts_with("committed"));
            let hwm = records_in(&clean, s).await;
            if hwm != 3 + u64::from(competitor_ok) {
                problems.push(format!("high watermark {hwm}"));
            }
            call
        }
        Group::CasPointer | Group::CasPointerFenced => {
            let ns = clean.create_namespace("fm").await.expect("namespace");
            let fence = if group == Group::CasPointerFenced {
                let grant = clean
                    .acquire_lease("fm/lease", "owner-1", Duration::from_secs(60))
                    .await
                    .expect("lease");
                Some(Fence {
                    lease: "fm/lease".to_string(),
                    epoch: grant.epoch,
                })
            } else {
                None
            };
            if kind == Kind::Race {
                let clean = clean.clone();
                let fence = fence.clone();
                *plan.race.lock().expect("lock") = Some(Box::pin(async move {
                    let before = clean.tikv().stats().restarts;
                    let result = match fence {
                        // A write to the fence's lease record, which the
                        // call reads and locks.
                        Some(fence) => clean
                            .renew_lease(
                                "fm/lease",
                                "owner-1",
                                fence.epoch,
                                Duration::from_secs(60),
                            )
                            .await
                            .map(|_| ()),
                        None => clean
                            .cas_pointer(PointerCas {
                                namespace: ns,
                                key: "manifest".to_string(),
                                expected: None,
                                value: "theirs".to_string(),
                                fence: None,
                                fresh: None,
                            })
                            .await
                            .into_result()
                            .map(|_| ()),
                    };
                    label(result, clean.tikv().stats().restarts > before)
                }));
            }
            let tracked = meta
                .cas_pointer(PointerCas {
                    namespace: ns,
                    key: "manifest".to_string(),
                    expected: None,
                    value: "ours".to_string(),
                    fence: fence.clone(),
                    fresh: None,
                })
                .await;
            let acknowledged = tracked.result.as_ref().ok().copied();
            let call = Call::tracked(tracked);
            competitor = join_competitor(&plan).await;
            let pointer = clean.pointer(L, ns, "manifest").await.expect("read");
            match (&pointer, acknowledged) {
                (Some(p), _) if p.version != 1 => {
                    problems.push(format!("pointer version {} after one write", p.version));
                }
                (Some(p), Some(v)) if p.value != "ours" || v != 1 => {
                    problems.push(format!("an acknowledged write is lost: {p:?}, got {v}"));
                }
                (None, Some(_)) => problems.push("an acknowledged pointer is missing".into()),
                _ => {}
            }
            if group == Group::CasPointerFenced {
                let lease = clean.lease(L, "fm/lease").await.expect("read");
                if lease.map(|l| l.epoch) != fence.as_ref().map(|f| f.epoch) {
                    problems.push("the fence's lease moved".into());
                }
            } else if competitor.as_deref() == Some("committed")
                && pointer.is_some_and(|p| p.value != "theirs")
            {
                problems.push("the competitor's acknowledged write is lost".into());
            }
            call
        }
        Group::Lease => {
            if kind == Kind::Race {
                let clean = clean.clone();
                *plan.race.lock().expect("lock") = Some(Box::pin(async move {
                    let before = clean.tikv().stats().restarts;
                    let r = clean
                        .acquire_lease("fm/lease", "other", Duration::from_secs(60))
                        .await;
                    label(r, clean.tikv().stats().restarts > before)
                }));
            }
            let result = meta
                .acquire_lease("fm/lease", "me", Duration::from_secs(60))
                .await;
            let lease = clean.lease(L, "fm/lease").await.expect("read");
            match (&result, &lease) {
                (Ok(grant), Some(l))
                    if l.owner.as_deref() != Some("me") || l.epoch != grant.epoch =>
                {
                    problems.push(format!("an acknowledged lease is lost: {l:?}"));
                }
                (Ok(_), None) => problems.push("an acknowledged lease is missing".into()),
                _ => {}
            }
            if let Some(l) = &lease
                && l.epoch != 1
            {
                problems.push(format!("epoch {} after one take", l.epoch));
            }
            Call::plain(result)
        }
        Group::DropCollection | Group::DropLarge => {
            let ns = clean.create_namespace("fm").await.expect("namespace");
            let partitions = if group == Group::DropLarge {
                LARGE_PARTITIONS
            } else {
                4
            };
            let (_, s, _) = clean
                .create_collection(ns, "docs", schema(), partitions)
                .await
                .expect("collection");
            let chunks: Vec<WalChunk> = if group == Group::DropLarge {
                let limit = u32::try_from(MAX_GROUP_CHUNKS).expect("limit");
                (0..limit).map(|i| one_record(s, i % partitions)).collect()
            } else {
                vec![one_record(s, 0), one_record(s, 1)]
            };
            clean
                .commit_wal(wal(&clean, "fm/wal-d", chunks))
                .await
                .into_result()
                .expect("setup commit");
            if kind == Kind::Race {
                *plan.race.lock().expect("lock") = Some(commit_competitor(&clean, s, 2));
            }
            let result = meta.drop_collection(ns, "docs").await;
            competitor = join_competitor(&plan).await;
            if clean
                .resolve_collection(L, ns, "docs")
                .await
                .expect("read")
                .is_some()
                && result.is_ok()
            {
                problems.push("an acknowledged drop left the collection".into());
            }
            if result.is_ok() {
                let retired = clean.retired_expired(0).await.expect("retired");
                if !retired.iter().any(|o| o == "fm/wal-d") {
                    problems.push("the dropped collection's WAL object is not retired".into());
                }
                if competitor
                    .as_deref()
                    .is_some_and(|c| c.starts_with("committed"))
                    && !retired.iter().any(|o| o == "fm/wal-competitor")
                {
                    problems.push("the competitor's WAL object is not retired".into());
                }
            }
            Call {
                result: result.map(|dropped| dropped.is_some()),
                unknown: false,
            }
        }
    };
    if competitor.is_none() {
        competitor = join_competitor(&plan).await;
    }
    let outcome = classify(plan.fired(), kind, &call);
    let violations = clean.check_invariants().await.expect("invariants");
    problems.extend(violations);
    assert!(problems.is_empty(), "{what}: {problems:#?}");
    Ran {
        outcome,
        competitor,
    }
}

/// The competitor's outcome, once it has finished.
async fn join_competitor(plan: &CellPlan) -> Option<String> {
    let handle = plan.competitor.lock().expect("lock").take()?;
    Some(handle.await.expect("the competitor panicked"))
}

// ---- The table ----

fn render(
    outcomes: &BTreeMap<(Group, Kind), [Outcome; 2]>,
    competitors: &BTreeMap<Group, [String; 2]>,
) -> String {
    let mut out = String::from("| Group | Fault | Attempt 1 | Attempt 2 |\n|---|---|---|---|\n");
    for ((group, kind), [a, b]) in outcomes {
        out.push_str(&format!(
            "| {} | {} | {a} | {b} |\n",
            group.name(),
            kind.name()
        ));
    }
    out.push_str("\n## Race competitors\n\n| Group | Attempt 1 | Attempt 2 |\n|---|---|---|\n");
    for (group, [a, b]) in competitors {
        out.push_str(&format!("| {} | {a} | {b} |\n", group.name()));
    }
    out
}

/// The matrix: every group × fault × attempt, compared with the blessed
/// table.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn meta_fault_matrix() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let cells: Vec<(Group, Kind, u32)> = GROUPS
        .iter()
        .flat_map(|&g| KINDS.iter().flat_map(move |&k| [(g, k, 1), (g, k, 2)]))
        .collect();
    let permits = Arc::new(tokio::sync::Semaphore::new(PARALLEL));
    let mut tasks = Vec::new();
    for (group, kind, attempt) in cells {
        let cluster = cluster.clone();
        let permits = permits.clone();
        tasks.push(tokio::spawn(async move {
            // A cell with a 1 024-entry transaction runs alone: several at
            // once outlast the runner's deadline on one playground store.
            let weight = if group.large() { PARALLEL } else { 1 };
            let weight = u32::try_from(weight).expect("permits");
            let _permit = permits.acquire_many_owned(weight).await.expect("semaphore");
            let ran = run_cell(&cluster, group, kind, attempt).await;
            (group, kind, attempt, ran)
        }));
    }
    let mut outcomes: BTreeMap<(Group, Kind), [Outcome; 2]> = BTreeMap::new();
    let mut competitors: BTreeMap<Group, [String; 2]> = BTreeMap::new();
    for task in tasks {
        let (group, kind, attempt, ran) = task.await.expect("a cell panicked");
        let slot = usize::try_from(attempt - 1).expect("attempt");
        outcomes
            .entry((group, kind))
            .or_insert([Outcome::NoEffect; 2])[slot] = ran.outcome;
        if let Some(c) = ran.competitor {
            competitors.entry(group).or_default()[slot] = c;
        }
    }
    let actual = render(&outcomes, &competitors);
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join(EXPECTED);
    if std::env::var("BLESS").is_ok_and(|v| v == "1") {
        std::fs::write(&path, &actual).expect("bless");
        return;
    }
    let expected = std::fs::read_to_string(&path).unwrap_or_default();
    assert_eq!(
        actual, expected,
        "the fault matrix changed; rerun with BLESS=1 and review the diff"
    );
}

// ---- A large drop failing midway ----

/// A collection of [`LARGE_PARTITIONS`] partitions with one group's worth
/// of index entries (1 024), created through `clean`.
async fn large_collection(clean: &TikvMeta) -> (NamespaceId, CollectionId, StreamId) {
    let ns = clean.create_namespace("large").await.expect("namespace");
    let (cid, s, _) = clean
        .create_collection(ns, "docs", schema(), LARGE_PARTITIONS)
        .await
        .expect("collection");
    let limit = u32::try_from(MAX_GROUP_CHUNKS).expect("limit");
    let chunks = (0..limit)
        .map(|i| one_record(s, i % LARGE_PARTITIONS))
        .collect();
    clean
        .commit_wal(wal(clean, "large/wal-1", chunks))
        .await
        .into_result()
        .expect("setup commit");
    (ns, cid, s)
}

/// Whether the large collection is wholly present (`Some(true)`), wholly
/// gone (`Some(false)`), or half applied (`None`).
async fn large_state(clean: &TikvMeta, ns: NamespaceId, s: StreamId) -> Option<bool> {
    let collection = clean
        .resolve_collection(L, ns, "docs")
        .await
        .expect("read")
        .is_some();
    let state = clean.stream_state(L, s).await.expect("read");
    let entries: u64 = match &state {
        Some(_) => {
            let mut n = 0;
            for p in 0..LARGE_PARTITIONS {
                n += clean
                    .partition_index(L, s, p, 0, None)
                    .await
                    .expect("read")
                    .map_or(0, |i| i.entries().count() as u64);
            }
            n
        }
        None => 0,
    };
    let retired = clean
        .retired_expired(0)
        .await
        .expect("retired")
        .iter()
        .any(|o| o == "large/wal-1");
    match (collection, state.is_some(), entries, retired) {
        (true, true, n, false) if n == MAX_GROUP_CHUNKS as u64 => Some(true),
        (false, false, 0, true) => Some(false),
        _ => None,
    }
}

/// A drop refused on every attempt fails and leaves the collection, its
/// stream, all 1 024 entries and the WAL object's live count in place.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_drop_refused_on_every_attempt_leaves_everything() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let config = cluster.config(TEST_META);
    struct RefuseDrops;
    impl FaultPlan for RefuseDrops {
        fn at(&self, op: &str, point: FaultPoint, _attempt: u32) -> Option<Fault> {
            (op == "meta.drop_collection" && point == FaultPoint::BeforeCommit)
                .then_some(Fault::Refuse)
        }
    }
    let meta = open(&config, Some(Arc::new(RefuseDrops))).await;
    let clean = open(&config, None).await;
    let (ns, _, s) = large_collection(&clean).await;
    let dropped = meta.drop_collection(ns, "docs").await;
    assert!(
        matches!(dropped, Err(MetaError::Unavailable(_) | MetaError::Timeout)),
        "{dropped:?}"
    );
    assert_eq!(
        large_state(&clean, ns, s).await,
        Some(true),
        "nothing applied"
    );
    assert_eq!(
        clean.check_invariants().await.expect("invariants"),
        Vec::<String>::new()
    );
    // A clean drop then removes everything at once.
    assert!(
        clean
            .drop_collection(ns, "docs")
            .await
            .expect("drop")
            .is_some()
    );
    assert_eq!(large_state(&clean, ns, s).await, Some(false));
    assert_eq!(
        clean.check_invariants().await.expect("invariants"),
        Vec::<String>::new()
    );
}

/// A drop whose prewrite succeeded but whose commit step failed (the
/// `after-prewrite` failpoint, with the rollback skipped so its locks stay)
/// is resolved by the next reader as a whole: the collection is wholly
/// dropped or wholly present, never half, and the retried call finishes the
/// drop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_drop_failing_after_its_prewrite_applies_all_or_nothing() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let config = cluster.config(TEST_META);
    let meta = open(&config, None).await;
    let clean = open(&config, None).await;
    let (ns, cid, s) = large_collection(&clean).await;
    // Once each: the first attempt's commit step fails after its prewrite
    // and leaves its locks; the retry meets them and resolves them.
    fail::cfg("after-prewrite", "1*return").expect("failpoint");
    fail::cfg("before-rollback", "1*return").expect("failpoint");
    let dropped = meta.drop_collection(ns, "docs").await;
    fail::remove("after-prewrite");
    fail::remove("before-rollback");
    match dropped {
        // The retry met the first attempt's locks and resolved them once
        // their TTL passed: with two-phase commit it rolled them back and
        // dropped the collection itself (row T6-5); with async commit (row
        // F4) every key was prewritten, so the first attempt is committed
        // and the retry finds the collection dropped.
        Ok(Some(id)) => assert_eq!(id, cid),
        Ok(None) => {}
        Err(e) => panic!("the drop failed: {e:?}"),
    }
    assert_eq!(
        large_state(&clean, ns, s).await,
        Some(false),
        "all or nothing"
    );
    assert_eq!(
        clean.check_invariants().await.expect("invariants"),
        Vec::<String>::new()
    );
}
