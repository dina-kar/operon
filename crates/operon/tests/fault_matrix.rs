//! The object-store fault matrix (M0 exit gate; M0.4 plan Task 7).
//!
//! Every component operation (writer flush, reader fetch, segmenter swap,
//! retention trim, link commit, GC pass, meta snapshot) is crossed with every
//! `(Op, Fault)` pair, the fault hitting the operation's first or its second
//! call of that store operation. Each cell runs on a fresh in-process setup
//! and ends up in one of four outcomes:
//! - `Retried`: the fault was reached and the operation still completed
//!   (retrying internally or riding out a delay);
//! - `Deferred`: GC only: the fault was reached, the pass completed and left
//!   the object it could not handle for its next pass;
//! - `SurfacedRetryable`: the fault was reached and the caller saw a
//!   *retryable* error (store, cache, metastore unavailability, unknown
//!   commit outcome, a blocked link commit), and nothing was acknowledged
//!   that is not durable;
//! - `NoEffect`: the operation never reached the faulted call.
//!
//! An error without the fault being reached, or a non-retryable error
//! (corrupt data, an unexpected reply), fails the gate. Every cell's outcome
//! must equal the committed table `tests/fault_matrix.expected.md`
//! (`FAULT_MATRIX_BLESS=1` rewrites it; review the diff). After every cell,
//! with faults off and the components run once more, the invariants must
//! hold: the segmenter runs without failures, no acknowledged data loss, no
//! torn state (meta invariants, and reads equal to the model), and the link's
//! `CounterTable` exact. The matrix is written to `target/fault-matrix.md`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use object_store::memory::InMemory;
use operon_cache::{CacheError, RangeCache, RangeCacheConfig};
use operon_common::{NamespaceId, StreamId};
use operon_link::{CounterTable, LinkApplySource, LinkConfig, LinkError, LinkGcRoots};
use operon_log::gc::{GcConfig, GcSource};
use operon_log::{
    FetchRequest, LogConfig, LogError, LogReader, LogWriter, Record, Retention, RetentionConfig,
    Segmenter, SegmenterConfig,
};
use operon_meta::{
    Consistency, MetaClient, MetaClientConfig, MetaConfig, MetaError, MetaNode, Router,
    SystemClock, TargetRef, WalClass,
};
use operon_store::{Fault, FaultyStore, Op, Store};
use operon_worker::{RunResult, TaskError, run_once};
use tempfile::TempDir;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Component {
    WriterFlush,
    ReaderFetch,
    SegmenterSwap,
    RetentionTrim,
    LinkCommit,
    GcPass,
    MetaSnapshot,
}

const COMPONENTS: [Component; 7] = [
    Component::WriterFlush,
    Component::ReaderFetch,
    Component::SegmenterSwap,
    Component::RetentionTrim,
    Component::LinkCommit,
    Component::GcPass,
    Component::MetaSnapshot,
];

const OPS: [Op; 6] = [
    Op::Put,
    Op::PutCreate,
    Op::PutIfMatch,
    Op::Get,
    Op::Delete,
    Op::List,
];

const DELAY: Duration = Duration::from_secs(2);

fn faults() -> [Fault; 4] {
    [
        Fault::Error,
        Fault::ErrorAfterApply,
        Fault::Precondition,
        Fault::Delay(DELAY),
    ]
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Retried,
    Deferred,
    SurfacedRetryable,
    NoEffect,
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Outcome::Retried => "Retried",
            Outcome::Deferred => "Deferred",
            Outcome::SurfacedRetryable => "SurfacedRetryable",
            Outcome::NoEffect => "NoEffect",
        })
    }
}

/// Why a component operation failed.
#[derive(Debug)]
enum Failure {
    Log(LogError),
    Meta(MetaError),
    Task(TaskError),
    /// Not an error of the component (a wrong result): never retryable.
    Other(String),
}

impl From<LogError> for Failure {
    fn from(err: LogError) -> Self {
        Failure::Log(err)
    }
}

impl From<MetaError> for Failure {
    fn from(err: MetaError) -> Self {
        Failure::Meta(err)
    }
}

fn meta_retryable(err: &MetaError) -> bool {
    matches!(
        err,
        MetaError::NotLeader { .. }
            | MetaError::Timeout
            | MetaError::Unavailable(_)
            | MetaError::Storage(_)
    )
}

fn log_retryable(err: &LogError) -> bool {
    match err {
        LogError::Store(err) => err.is_retryable(),
        LogError::Cache(CacheError::Store(err)) => err.is_retryable(),
        LogError::CommitUnknown(_) | LogError::Backpressure => true,
        LogError::Meta(err) => meta_retryable(err),
        LogError::Task(err) => task_retryable(err),
        _ => false,
    }
}

fn link_retryable(err: &LinkError) -> bool {
    match err {
        LinkError::Store(err) => err.is_retryable(),
        LinkError::Blocked(_) => true,
        LinkError::Meta(err) => meta_retryable(err),
        LinkError::Log(err) => log_retryable(err),
        LinkError::Corrupt(_) | LinkError::NotFound(_) => false,
    }
}

fn task_retryable(err: &TaskError) -> bool {
    match err {
        TaskError::Fenced => true,
        TaskError::Meta(err) => meta_retryable(err),
        TaskError::Failed(err) => {
            if let Some(err) = err.downcast_ref::<LogError>() {
                log_retryable(err)
            } else if let Some(err) = err.downcast_ref::<LinkError>() {
                link_retryable(err)
            } else {
                err.downcast_ref::<operon_store::StoreError>()
                    .is_some_and(operon_store::StoreError::is_retryable)
            }
        }
    }
}

impl Failure {
    fn retryable(&self) -> bool {
        match self {
            Failure::Log(err) => log_retryable(err),
            Failure::Meta(err) => meta_retryable(err),
            Failure::Task(err) => task_retryable(err),
            Failure::Other(_) => false,
        }
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Failure::Log(err) => write!(f, "log: {err}"),
            Failure::Meta(err) => write!(f, "meta: {err}"),
            Failure::Task(err) => write!(f, "task: {err}"),
            Failure::Other(err) => f.write_str(err),
        }
    }
}

/// A fresh single-node setup with data in every state the components touch.
struct Fixture {
    node: MetaNode,
    meta: MetaClient,
    faults: Arc<FaultyStore>,
    store: Store,
    writer: LogWriter,
    ns: NamespaceId,
    events: StreamId,
    logs: StreamId,
    /// (stream, partition) → offset → value, acknowledged.
    acked: Mutex<BTreeMap<(StreamId, u32), BTreeMap<u64, String>>>,
    /// Values whose append had an unknown outcome.
    unknown: Mutex<BTreeSet<String>>,
    /// Values whose append failed definitely.
    failed: Mutex<BTreeSet<String>>,
    next: Mutex<u64>,
    _dir: TempDir,
}

fn link_source(f: &Fixture, reader: LogReader) -> LinkApplySource {
    LinkApplySource::new(
        reader,
        f.store.clone(),
        LinkConfig {
            batch_records: 1_000,
            batch_interval: Duration::ZERO,
            ..LinkConfig::default()
        },
    )
}

fn segmenter(f: &Fixture, cache: RangeCache) -> Segmenter {
    Segmenter::new(
        f.meta.clone(),
        f.store.clone(),
        cache,
        "matrix-segmenter",
        SegmenterConfig {
            min_bytes: 1,
            ..SegmenterConfig::default()
        },
    )
}

fn gc(f: &Fixture) -> GcSource {
    GcSource::with_roots(
        f.store.clone(),
        GcConfig {
            // Nothing else runs during a cell, so everything unreferenced is
            // garbage at once.
            grace: Duration::ZERO,
            ..GcConfig::default()
        },
        vec![Arc::new(LinkGcRoots)],
    )
}

impl Fixture {
    async fn start() -> Self {
        let dir = TempDir::new().expect("temp dir");
        let faults = Arc::new(FaultyStore::new(Arc::new(InMemory::new())));
        let store = Store::new(faults.clone());
        // The snapshot store is the faulty store too.
        let node = MetaNode::start(
            MetaConfig::new(1, dir.path(), store.clone()),
            &Router::new(),
        )
        .await
        .expect("start meta");
        node.initialize([1]).await.expect("initialize");
        node.wait_for_leader(Duration::from_secs(20))
            .await
            .expect("leader");
        let meta = MetaClient::new(
            node.clone(),
            vec![],
            Arc::new(SystemClock),
            MetaClientConfig::default(),
        );
        let ns = meta.create_namespace("acme").await.expect("namespace");
        let events = meta
            .create_stream(ns, "events", 2, WalClass::Standard)
            .await
            .expect("stream");
        let logs = meta
            .create_stream_with_retention(
                ns,
                "logs",
                1,
                WalClass::Standard,
                operon_meta::Retention {
                    max_age_ms: None,
                    max_bytes: Some(64),
                },
            )
            .await
            .expect("stream");
        meta.create_link(
            ns,
            "counts",
            events,
            TargetRef {
                kind: "counter".to_string(),
                name: "counts".to_string(),
            },
            BTreeMap::new(),
        )
        .await
        .expect("link");
        let writer = LogWriter::start(
            meta.clone(),
            store.clone(),
            LogConfig {
                flush_interval: Duration::from_millis(2),
                commit_retry_deadline: Duration::from_secs(10),
                ..LogConfig::new(1)
            },
        )
        .expect("writer");
        let f = Self {
            node,
            meta,
            faults,
            store,
            writer,
            ns,
            events,
            logs,
            acked: Mutex::default(),
            unknown: Mutex::default(),
            failed: Mutex::default(),
            next: Mutex::new(0),
            _dir: dir,
        };
        // Partition 0 segmented, partition 1 in WAL objects, one link commit,
        // retired WAL objects for GC, and a trimmed logs stream.
        for _ in 0..3 {
            f.append(f.events, 0, 2).await.expect("append");
            f.append(f.logs, 0, 2).await.expect("append");
        }
        segmenter(&f, f.cache().await)
            .run_once()
            .await
            .expect("segment");
        for _ in 0..2 {
            f.append(f.events, 1, 2).await.expect("append");
        }
        f.apply_link().await;
        f
    }

    async fn cache(&self) -> RangeCache {
        RangeCache::new(self.store.clone(), RangeCacheConfig::default())
            .await
            .expect("cache")
    }

    async fn reader(&self) -> LogReader {
        LogReader::new(self.meta.clone(), self.cache().await)
    }

    fn values(&self, n: u32) -> Vec<String> {
        let mut next = self.next.lock().expect("lock");
        (0..n)
            .map(|_| {
                *next += 1;
                next.to_string()
            })
            .collect()
    }

    /// Appends `n` records, keeping the model.
    async fn append(&self, stream: StreamId, partition: u32, n: u32) -> Result<(), LogError> {
        let values = self.values(n);
        let records = values
            .iter()
            .map(|v| Record {
                key: Some(Bytes::from(format!("c{}", v.len() % 3))),
                value: Some(Bytes::from(v.clone())),
                headers: vec![],
                timestamp_ms: -1,
            })
            .collect();
        match self.writer.append(stream, partition, records).await {
            Ok(ack) => {
                let mut acked = self.acked.lock().expect("lock");
                let slot = acked.entry((stream, partition)).or_default();
                for (i, v) in values.into_iter().enumerate() {
                    slot.insert(ack.base_offset + i as u64, v);
                }
                Ok(())
            }
            Err(err @ LogError::CommitUnknown(_)) => {
                self.unknown.lock().expect("lock").extend(values);
                Err(err)
            }
            Err(err) => {
                self.failed.lock().expect("lock").extend(values);
                Err(err)
            }
        }
    }

    /// Runs link apply until it has applied everything (fault-free).
    async fn apply_link(&self) {
        let source = link_source(self, self.reader().await);
        for _ in 0..50 {
            run_once(&self.meta, "matrix-link", Duration::from_secs(30), &source)
                .await
                .expect("link run");
            if self.link_caught_up().await {
                return;
            }
        }
        panic!("the link never caught up");
    }

    async fn hwms(&self) -> BTreeMap<u32, u64> {
        let events = self.events;
        self.meta
            .read(Consistency::Local, |s| {
                (0..2)
                    .filter_map(|p| {
                        let hwm = s.partition(events, p)?.high_watermark();
                        (hwm > 0).then_some((p, hwm))
                    })
                    .collect()
            })
            .await
            .expect("read")
    }

    fn table(&self) -> CounterTable {
        let link = operon_meta::Link {
            id: operon_meta::LinkId(1),
            namespace: self.ns,
            name: "counts".to_string(),
            source: self.events,
            target: TargetRef {
                kind: "counter".to_string(),
                name: "counts".to_string(),
            },
            options: BTreeMap::new(),
        };
        CounterTable::for_link(self.meta.clone(), self.store.clone(), &link)
    }

    async fn link_caught_up(&self) -> bool {
        self.table().applied().await.ok() == Some(self.hwms().await)
    }

    /// Runs one component operation; `Ok` if it succeeded.
    async fn operate(&self, component: Component) -> Result<(), Failure> {
        match component {
            // Two flushes, so a fault can hit the second WAL PUT too.
            Component::WriterFlush => {
                self.append(self.events, 1, 3).await?;
                Ok(self.append(self.events, 0, 2).await?)
            }
            Component::ReaderFetch => {
                let reader = self.reader().await;
                for partition in 0..2 {
                    let read = read_all(&reader, self.events, partition, 0).await?;
                    let acked = self
                        .acked
                        .lock()
                        .expect("lock")
                        .get(&(self.events, partition))
                        .cloned()
                        .unwrap_or_default();
                    for (offset, value) in &acked {
                        assert_eq!(
                            read.get(offset),
                            Some(value),
                            "a fetch returned wrong data at {partition}@{offset}"
                        );
                    }
                }
                Ok(())
            }
            Component::SegmenterSwap => {
                // Through the worker directly, to see each task's error.
                let segmenter = segmenter(self, self.cache().await);
                let before = segmenter.source().report();
                let results = run_once(
                    &self.meta,
                    "matrix-segmenter",
                    Duration::from_secs(30),
                    segmenter.source(),
                )
                .await
                .map_err(Failure::Task)?;
                for (_, result) in results {
                    match result {
                        RunResult::Ran(Ok(_)) => {}
                        RunResult::Ran(Err(err)) => return Err(Failure::Task(err)),
                        RunResult::LeaseHeld => {
                            return Err(Failure::Other("segmenter lease held".to_string()));
                        }
                    }
                }
                let report = segmenter.source().report();
                if report.segments == before.segments {
                    return Err(Failure::Other(format!("nothing segmented: {report:?}")));
                }
                Ok(())
            }
            Component::RetentionTrim => Ok(Retention::new(
                self.meta.clone(),
                "matrix-retention",
                RetentionConfig::default(),
            )
            .run_once()
            .await
            .map(|_| ())?),
            Component::LinkCommit => {
                let source = link_source(self, self.reader().await);
                let results = run_once(&self.meta, "matrix-link", Duration::from_secs(30), &source)
                    .await
                    .map_err(Failure::Task)?;
                match results.into_iter().next() {
                    Some((_, RunResult::Ran(Ok(_)))) => Ok(()),
                    Some((_, RunResult::Ran(Err(err)))) => Err(Failure::Task(err)),
                    other => Err(Failure::Other(format!("{other:?}"))),
                }
            }
            Component::GcPass => match gc(self).run_once(&self.meta, "matrix-gc").await? {
                Some(_) => Ok(()),
                None => Err(Failure::Other("gc lease held".to_string())),
            },
            // Two snapshots: the second replaces (and deletes) the first.
            Component::MetaSnapshot => {
                self.node.snapshot().await?;
                self.meta.create_namespace("snapshotted-again").await?;
                Ok(self.node.snapshot().await?)
            }
        }
    }

    /// Prepares the work a component's operation will do (fault-free).
    async fn prepare(&self, component: Component) {
        match component {
            Component::SegmenterSwap | Component::LinkCommit => {
                self.append(self.events, 1, 2).await.expect("append");
                self.append(self.events, 0, 1).await.expect("append");
            }
            Component::RetentionTrim => {
                self.append(self.logs, 0, 3).await.expect("append");
            }
            Component::MetaSnapshot => {
                self.meta
                    .create_namespace("snapshotted")
                    .await
                    .expect("namespace");
            }
            Component::GcPass => {
                // Retire the WAL objects of partition 1.
                segmenter(self, self.cache().await)
                    .run_once()
                    .await
                    .expect("segment");
            }
            Component::WriterFlush | Component::ReaderFetch => {}
        }
    }

    /// The invariants after a cell, with faults off and every component run
    /// once more so that retries complete.
    async fn check(&self, what: &str) {
        self.faults.clear();
        let report = segmenter(self, self.cache().await)
            .run_once()
            .await
            .unwrap_or_else(|e| panic!("{what}: segmenter after the cell: {e}"));
        assert_eq!(
            report.failed, 0,
            "{what}: segmenter after the cell: {report:?}"
        );
        self.apply_link().await;
        gc(self)
            .run_once(&self.meta, "matrix-gc")
            .await
            .unwrap_or_else(|e| panic!("{what}: gc after the cell: {e}"));
        let violations = self
            .meta
            .read(Consistency::Local, |s| s.check_invariants())
            .await
            .expect("read");
        assert!(violations.is_empty(), "{what}: {violations:?}");

        let reader = self.reader().await;
        let acked = self.acked.lock().expect("lock").clone();
        let unknown = self.unknown.lock().expect("lock").clone();
        let failed = self.failed.lock().expect("lock").clone();
        let mut sums: BTreeMap<String, i64> = BTreeMap::new();
        for (stream, partitions) in [(self.events, 2u32), (self.logs, 1)] {
            for partition in 0..partitions {
                let start = self
                    .meta
                    .read(Consistency::Local, |s| {
                        s.partition(stream, partition)
                            .expect("p")
                            .log_start_offset()
                    })
                    .await
                    .expect("read");
                let read = read_all(&reader, stream, partition, start)
                    .await
                    .unwrap_or_else(|e| panic!("{what}: read {stream}/{partition}: {e}"));
                let expected = acked.get(&(stream, partition)).cloned().unwrap_or_default();
                for (offset, value) in expected.range(start..) {
                    assert_eq!(
                        read.get(offset),
                        Some(value),
                        "{what}: acknowledged {stream}/{partition}@{offset} lost"
                    );
                }
                let acked_values: BTreeSet<&String> = expected.values().collect();
                let mut seen = BTreeSet::new();
                for (offset, value) in &read {
                    assert!(seen.insert(value), "{what}: {value} twice");
                    assert!(
                        !failed.contains(value)
                            && (acked_values.contains(value) || unknown.contains(value)),
                        "{what}: {stream}/{partition}@{offset} holds {value}, never acknowledged"
                    );
                    if stream == self.events {
                        *sums.entry(format!("c{}", value.len() % 3)).or_default() +=
                            value.parse::<i64>().expect("delta");
                    }
                }
            }
        }
        let snapshot = self.table().snapshot().await.expect("snapshot");
        assert_eq!(snapshot.counters, sums, "{what}: CounterTable is not exact");
        assert_eq!(snapshot.skipped, 0, "{what}");
    }

    async fn shutdown(self) {
        self.faults.clear();
        let _ = self.writer.shutdown().await;
        self.node.shutdown().await.expect("shutdown");
    }
}

async fn read_all(
    reader: &LogReader,
    stream: StreamId,
    partition: u32,
    from: u64,
) -> Result<BTreeMap<u64, String>, LogError> {
    let mut out = BTreeMap::new();
    let mut offset = from;
    loop {
        let response = reader
            .fetch(FetchRequest {
                stream,
                partition,
                offset,
                max_bytes: 1 << 16,
                max_wait: Duration::ZERO,
            })
            .await?;
        if response.records.is_empty() {
            return Ok(out);
        }
        for r in response.records {
            let value =
                String::from_utf8_lossy(r.record.value.as_deref().unwrap_or_default()).to_string();
            out.insert(r.offset, value);
        }
        offset = response.next_offset;
    }
}

/// Runs one cell.
async fn cell(component: Component, op: Op, fault: Fault, nth: u64) -> Outcome {
    let what = format!("{component:?} x {op:?} {fault:?} on call {nth}");
    let f = Fixture::start().await;
    f.prepare(component).await;
    f.faults.inject_nth(op, nth, fault);
    let result = tokio::time::timeout(Duration::from_secs(60), f.operate(component))
        .await
        .unwrap_or_else(|_| panic!("{what}: hung"));
    let consumed = f.faults.pending(op) == 0;
    let outcome = match (result, consumed) {
        (Ok(()), false) => Outcome::NoEffect,
        (Ok(()), true) if component == Component::GcPass && !matches!(fault, Fault::Delay(_)) => {
            Outcome::Deferred
        }
        (Ok(()), true) => Outcome::Retried,
        (Err(err), true) if err.retryable() => Outcome::SurfacedRetryable,
        (Err(err), true) => panic!("{what}: a non-retryable error surfaced: {err}"),
        (Err(err), false) => panic!("{what}: failed without reaching the fault: {err}"),
    };
    f.check(&what).await;
    f.shutdown().await;
    outcome
}

/// Cells whose outcome is structural: a component that never issues that
/// store operation cannot be affected by its faults.
fn structural(component: Component, op: Op) -> Option<Outcome> {
    let writes = matches!(op, Op::Put | Op::PutCreate | Op::PutIfMatch | Op::Delete);
    match component {
        Component::ReaderFetch if writes || op == Op::List => Some(Outcome::NoEffect),
        Component::RetentionTrim => Some(Outcome::NoEffect),
        Component::WriterFlush
            if matches!(op, Op::Get | Op::Delete | Op::List | Op::PutIfMatch) =>
        {
            Some(Outcome::NoEffect)
        }
        _ => None,
    }
}

/// Per (component, op, fault): the outcomes on the first and second call.
type Results = BTreeMap<(Component, String, String), [Option<Outcome>; 2]>;

#[test]
fn every_component_survives_every_store_fault() {
    let mut cells = Vec::new();
    for component in COMPONENTS {
        for op in OPS {
            for fault in faults() {
                cells.push((component, op, fault));
            }
        }
    }
    let results: Mutex<Results> = Mutex::default();
    let queue = Mutex::new(cells.clone());
    let threads = std::env::var("FAULT_MATRIX_THREADS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8usize);
    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("runtime");
                loop {
                    let Some((component, op, fault)) = queue.lock().expect("lock").pop() else {
                        return;
                    };
                    for nth in [1, 2] {
                        let outcome = runtime.block_on(cell(component, op, fault, nth));
                        if let Some(expected) = structural(component, op) {
                            assert_eq!(
                                outcome, expected,
                                "{component:?} x {op:?} {fault:?} on call {nth}"
                            );
                        }
                        let key = (component, format!("{op:?}"), format!("{fault:?}"));
                        results.lock().expect("lock").entry(key).or_default()
                            [usize::try_from(nth - 1).expect("index")] = Some(outcome);
                    }
                }
            });
        }
    });
    let results = results.into_inner().expect("lock");
    assert_eq!(results.len(), cells.len());
    let mut table =
        String::from("| Component | Op | Fault | 1st call | 2nd call |\n|---|---|---|---|---|\n");
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for component in COMPONENTS {
        for op in OPS {
            for fault in faults() {
                let key = (component, format!("{op:?}"), format!("{fault:?}"));
                let [first, second] = results[&key];
                let show = |o: Option<Outcome>| o.map_or("-".to_string(), |o| o.to_string());
                for o in [first, second].into_iter().flatten() {
                    *counts.entry(o.to_string()).or_default() += 1;
                }
                table.push_str(&format!(
                    "| {component:?} | {op:?} | {fault:?} | {} | {} |\n",
                    show(first),
                    show(second)
                ));
            }
        }
    }
    let rows = table.clone();
    table.push_str(&format!("\nCells: {} ({counts:?}).\n", 2 * results.len()));
    let target = std::env::var("CARGO_TARGET_DIR")
        .unwrap_or_else(|_| concat!(env!("CARGO_MANIFEST_DIR"), "/../../target").to_string());
    let path = std::path::Path::new(&target).join("fault-matrix.md");
    std::fs::write(&path, &table).expect("write the fault matrix");
    eprintln!("fault matrix written to {}", path.display());

    // Every cell must have the committed outcome (review I2).
    let expected_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fault_matrix.expected.md"
    );
    if std::env::var_os("FAULT_MATRIX_BLESS").is_some() {
        std::fs::write(expected_path, &rows).expect("write the expected matrix");
        return;
    }
    let expected = std::fs::read_to_string(expected_path).expect("read the expected matrix");
    let mismatches: Vec<String> = diff_rows(&expected, &rows);
    assert!(
        mismatches.is_empty(),
        "cells differ from {expected_path} (expected => actual):\n{}",
        mismatches.join("\n")
    );
}

/// The rows of `actual` that differ from `expected`, by (component, op,
/// fault), and rows only one of them has.
fn diff_rows(expected: &str, actual: &str) -> Vec<String> {
    fn rows(table: &str) -> BTreeMap<String, String> {
        table
            .lines()
            .filter(|l| l.starts_with("| ") && !l.starts_with("| Component"))
            .filter_map(|l| {
                let cells: Vec<&str> = l.split('|').map(str::trim).collect();
                (cells.len() >= 6).then(|| {
                    (
                        format!("{} x {} {}", cells[1], cells[2], cells[3]),
                        format!("{} / {}", cells[4], cells[5]),
                    )
                })
            })
            .collect()
    }
    let (expected, actual) = (rows(expected), rows(actual));
    let keys: BTreeSet<&String> = expected.keys().chain(actual.keys()).collect();
    keys.into_iter()
        .filter(|k| expected.get(*k) != actual.get(*k))
        .map(|k| format!("{k}: {:?} => {:?}", expected.get(k), actual.get(k)))
        .collect()
}
