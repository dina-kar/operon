//! Exactly-once link apply into `CounterTable`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::FutureExt;
use operon_cache::{RangeCache, RangeCacheConfig};
use operon_common::{NamespaceId, StreamId};
use operon_link::{
    ApplyBatch, CommitError, CommitHook, CommitStep, CounterSnapshot, CounterTable,
    LinkApplySource, LinkConfig, LinkTarget,
};
use operon_log::{
    FetchRequest, LogConfig, LogReader, LogWriter, Record, SegmenterConfig, SegmenterSource,
};
use operon_meta::{
    Consistency, Fence, LinkId, MetaClient, MetaClientConfig, MetaConfig, MetaNode, Router,
    SystemClock, TargetRef, WalClass,
};
use operon_store::Store;
use operon_worker::{RunResult, TaskOutcome, Worker, WorkerConfig, run_once};
use tempfile::TempDir;

const WAIT: Duration = Duration::from_secs(30);
const PARTITIONS: u32 = 3;

struct Fixture {
    node: MetaNode,
    meta: MetaClient,
    store: Store,
    writer: LogWriter,
    reader: LogReader,
    ns: NamespaceId,
    stream: StreamId,
    link: LinkId,
    /// Counter name → expected sum over every acknowledged record.
    model: Mutex<BTreeMap<String, i64>>,
    appended: AtomicU32,
    _dir: TempDir,
}

fn link_config() -> LinkConfig {
    LinkConfig {
        batch_records: 7,
        batch_interval: Duration::ZERO,
        ..LinkConfig::default()
    }
}

impl Fixture {
    async fn start() -> Self {
        let dir = TempDir::new().expect("temp dir");
        let node = MetaNode::start(
            MetaConfig::new(1, dir.path(), Store::in_memory()),
            &Router::new(),
        )
        .await
        .expect("start meta");
        node.initialize([1]).await.expect("initialize");
        node.wait_for_leader(WAIT).await.expect("leader");
        let meta = MetaClient::new(
            node.clone(),
            vec![],
            Arc::new(SystemClock),
            MetaClientConfig::default(),
        );
        let ns = meta.create_namespace("acme").await.expect("namespace");
        let stream = meta
            .create_stream(ns, "events", PARTITIONS, WalClass::Standard)
            .await
            .expect("stream");
        let link = meta
            .create_link(
                ns,
                "counts",
                stream,
                TargetRef {
                    kind: "counter".to_string(),
                    name: "counts".to_string(),
                },
                BTreeMap::new(),
            )
            .await
            .expect("link");
        let store = Store::in_memory();
        let writer = LogWriter::start(
            meta.clone(),
            store.clone(),
            LogConfig {
                flush_interval: Duration::from_millis(5),
                ..LogConfig::new(1)
            },
        )
        .expect("writer");
        let cache = RangeCache::new(store.clone(), RangeCacheConfig::default())
            .await
            .expect("cache");
        let reader = LogReader::new(meta.clone(), cache);
        Self {
            node,
            meta,
            store,
            writer,
            reader,
            ns,
            stream,
            link,
            model: Mutex::default(),
            appended: AtomicU32::new(0),
            _dir: dir,
        }
    }

    /// Appends `n` records spread over the partitions, with counter names
    /// `c0`..`c4` and deltas that make any double apply visible.
    async fn append(&self, n: u32) {
        for _ in 0..n {
            let i = self.appended.fetch_add(1, Ordering::SeqCst);
            let name = format!("c{}", i % 5);
            let delta = i64::from(i) * 7 - 3;
            let record = Record {
                key: Some(Bytes::from(name.clone())),
                value: Some(Bytes::from(delta.to_string())),
                headers: vec![],
                timestamp_ms: -1,
            };
            self.writer
                .append(self.stream, i % PARTITIONS, vec![record])
                .await
                .expect("append");
            *self.model.lock().expect("lock").entry(name).or_default() += delta;
        }
    }

    fn source(&self, hook: Option<CommitHook>) -> LinkApplySource {
        match hook {
            Some(hook) => LinkApplySource::with_hook(
                self.reader.clone(),
                self.store.clone(),
                link_config(),
                hook,
            ),
            None => LinkApplySource::new(self.reader.clone(), self.store.clone(), link_config()),
        }
    }

    fn table(&self) -> CounterTable {
        let link = operon_meta::Link {
            id: self.link,
            namespace: self.ns,
            name: "counts".to_string(),
            source: self.stream,
            target: TargetRef {
                kind: "counter".to_string(),
                name: "counts".to_string(),
            },
            options: BTreeMap::new(),
        };
        CounterTable::for_link(self.meta.clone(), self.store.clone(), &link)
    }

    async fn high_watermarks(&self) -> BTreeMap<u32, u64> {
        let stream = self.stream;
        self.meta
            .read(Consistency::Local, |s| {
                (0..PARTITIONS)
                    .map(|p| {
                        (
                            p,
                            s.partition(stream, p).expect("partition").high_watermark(),
                        )
                    })
                    .filter(|(_, hwm)| *hwm > 0)
                    .collect()
            })
            .await
            .expect("read")
    }

    /// Runs the link task until it has applied everything, through the
    /// worker framework, as `owner`.
    async fn apply_all(&self, owner: &str) {
        let source = self.source(None);
        let deadline = Instant::now() + WAIT;
        loop {
            let results = run_once(&self.meta, owner, Duration::from_secs(5), &source)
                .await
                .expect("run");
            let caught_up =
                self.table().applied().await.expect("applied") == self.high_watermarks().await;
            if caught_up {
                return;
            }
            for (_, result) in &results {
                match result {
                    RunResult::Ran(Ok(_)) | RunResult::LeaseHeld => {}
                    RunResult::Ran(Err(err)) => tracing_free_log(&format!("link run: {err}")),
                }
            }
            assert!(Instant::now() < deadline, "the link never caught up");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn check(&self) -> CounterSnapshot {
        let snapshot = self.table().snapshot().await.expect("snapshot");
        let model = self.model.lock().expect("lock").clone();
        assert_eq!(snapshot.counters, model, "sums differ from the model");
        assert_eq!(snapshot.applied, self.high_watermarks().await);
        assert_eq!(snapshot.skipped, 0);
        snapshot
    }

    async fn shutdown(self) {
        self.writer.shutdown().await.expect("writer");
        self.node.shutdown().await.expect("meta");
    }
}

fn tracing_free_log(message: &str) {
    eprintln!("{message}");
}

/// A hook that holds the `nth` commit (1-based) forever at `step`, and says
/// when it got there.
fn crash_at(step: CommitStep, nth: u32) -> (CommitHook, Arc<AtomicBool>) {
    let reached = Arc::new(AtomicBool::new(false));
    let seen = Arc::new(AtomicU32::new(0));
    let flag = reached.clone();
    let hook: CommitHook = Arc::new(move |at, _fence| {
        if at == step && seen.fetch_add(1, Ordering::SeqCst) + 1 == nth {
            flag.store(true, Ordering::SeqCst);
            futures::future::pending::<()>().boxed()
        } else {
            futures::future::ready(()).boxed()
        }
    });
    (hook, reached)
}

async fn wait_for(what: &str, check: impl Fn() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !check() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Review focus 3: a crash after the data PUT, after the manifest PUT, and
/// after the pointer CAS (the task dropped at that point, like a killed
/// process). After a restart every record is applied exactly once.
#[tokio::test]
async fn every_record_is_applied_exactly_once_across_crashes_at_every_step() {
    for step in [
        CommitStep::AfterDataPut,
        CommitStep::AfterManifestPut,
        CommitStep::AfterCas,
    ] {
        let f = Fixture::start().await;
        f.append(30).await;
        let (hook, reached) = crash_at(step, 2);
        let source = f.source(Some(hook));
        let meta = f.meta.clone();
        let crashed = tokio::spawn(async move {
            loop {
                run_once(&meta, "w1", Duration::from_millis(400), &source)
                    .await
                    .expect("run");
            }
        });
        wait_for("the crash point", || reached.load(Ordering::SeqCst)).await;
        crashed.abort();
        let _ = crashed.await;
        // The crashed run's lease runs out; a new owner takes over.
        f.apply_all("w2").await;
        f.check().await;
        f.append(10).await;
        f.apply_all("w2").await;
        let snapshot = f.check().await;
        assert!(snapshot.version >= 5, "{step:?}: {snapshot:?}");
        f.shutdown().await;
    }
}

/// Review focus 1: a task whose lease is taken over keeps running (its
/// renewals stalled) and tries to commit after the new holder applied
/// everything. The zombie cannot change the table.
#[tokio::test]
async fn a_zombie_task_cannot_double_apply() {
    let f = Fixture::start().await;
    f.append(20).await;
    let release = Arc::new(tokio::sync::Notify::new());
    let reached = Arc::new(AtomicBool::new(false));
    let hook: CommitHook = {
        let (release, reached) = (release.clone(), reached.clone());
        let first = Arc::new(AtomicBool::new(true));
        Arc::new(move |at, _fence| {
            if at == CommitStep::AfterDataPut && first.swap(false, Ordering::SeqCst) {
                reached.store(true, Ordering::SeqCst);
                let release = release.clone();
                async move { release.notified().await }.boxed()
            } else {
                futures::future::ready(()).boxed()
            }
        })
    };
    let worker_config = |owner: &str| WorkerConfig {
        poll_interval: Duration::from_millis(10),
        lease_ttl: Duration::from_millis(400),
        ..WorkerConfig::new(owner)
    };
    let mut zombie = Worker::new(f.meta.clone(), worker_config("zombie"));
    zombie.add_source(Arc::new(f.source(Some(hook))));
    let zombie = zombie.start();
    wait_for("the zombie's first commit", || {
        reached.load(Ordering::SeqCst)
    })
    .await;
    zombie.pause_renewals(true);

    let mut successor = Worker::new(f.meta.clone(), worker_config("successor"));
    successor.add_source(Arc::new(f.source(None)));
    let successor = successor.start();
    let deadline = Instant::now() + WAIT;
    while f.table().applied().await.unwrap() != f.high_watermarks().await {
        assert!(Instant::now() < deadline, "the successor never caught up");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    f.check().await;
    // More records, so the zombie finds work after it resumes.
    f.append(10).await;
    release.notify_one();
    zombie.pause_renewals(false);
    let deadline = Instant::now() + WAIT;
    while f.table().applied().await.unwrap() != f.high_watermarks().await {
        assert!(Instant::now() < deadline, "the successor never caught up");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Give the zombie time to try its commits.
    tokio::time::sleep(Duration::from_millis(300)).await;
    f.check().await;
    zombie.stop().await;
    successor.stop().await;
    f.shutdown().await;
}

/// The target reports `Conflict` for a commit on a version that moved, and
/// changes nothing.
#[tokio::test]
async fn a_commit_on_a_stale_version_conflicts() {
    let f = Fixture::start().await;
    f.append(6).await;
    let lease = "task/link/1";
    let grant = f
        .meta
        .acquire_lease(lease, "w1", Duration::from_secs(30))
        .await
        .unwrap();
    let fence = Fence {
        lease: lease.to_string(),
        epoch: grant.epoch,
    };
    let fetch = |partition: u32| {
        let reader = f.reader.clone();
        let stream = f.stream;
        async move {
            reader
                .fetch(FetchRequest {
                    stream,
                    partition,
                    offset: 0,
                    max_bytes: usize::MAX,
                    max_wait: Duration::ZERO,
                })
                .await
                .unwrap()
                .records
        }
    };
    let batch = |partition: u32, records: Vec<operon_log::OffsetRecord>| ApplyBatch {
        applied_after: [(partition, records.last().unwrap().offset + 1)].into(),
        records: records.into_iter().map(|r| (partition, r)).collect(),
    };
    let first = f.table();
    let second = f.table();
    let state = first.load().await.unwrap();
    assert_eq!(state.version, 0);
    assert_eq!(second.load().await.unwrap().version, 0);
    assert_eq!(
        first
            .commit(0, batch(0, fetch(0).await), &fence)
            .await
            .unwrap(),
        1
    );
    let before = f.table().snapshot().await.unwrap();
    let err = second
        .commit(0, batch(1, fetch(1).await), &fence)
        .await
        .unwrap_err();
    assert!(matches!(err, CommitError::Conflict), "{err:?}");
    assert_eq!(f.table().snapshot().await.unwrap(), before);
    // After a reload, the same batch commits on top.
    let state = second.load().await.unwrap();
    assert_eq!(state.version, 1);
    assert_eq!(
        second
            .commit(1, batch(1, fetch(1).await), &fence)
            .await
            .unwrap(),
        2
    );
    f.shutdown().await;
}

/// The apply loop's conflict path: a concurrent commit lands between this
/// task's data PUT and its CAS; the loop reloads and applies the rest.
#[tokio::test]
async fn the_apply_loop_retries_after_a_conflict() {
    let f = Arc::new(Fixture::start().await);
    f.append(20).await;
    let interleaved = Arc::new(AtomicBool::new(false));
    let hook: CommitHook = {
        let f = f.clone();
        let interleaved = interleaved.clone();
        Arc::new(move |at, fence: Fence| {
            if at != CommitStep::AfterDataPut || interleaved.swap(true, Ordering::SeqCst) {
                return futures::future::ready(()).boxed();
            }
            let f = f.clone();
            async move {
                // Someone else commits the first records of partition 0.
                let other = f.table();
                let state = other.load().await.expect("load");
                let records = f
                    .reader
                    .fetch(FetchRequest {
                        stream: f.stream,
                        partition: 0,
                        offset: state.applied.get(&0).copied().unwrap_or(0),
                        max_bytes: usize::MAX,
                        max_wait: Duration::ZERO,
                    })
                    .await
                    .expect("fetch")
                    .records;
                let take: Vec<_> = records.into_iter().take(2).collect();
                let batch = ApplyBatch {
                    applied_after: [(0, take.last().expect("records").offset + 1)].into(),
                    records: take.into_iter().map(|r| (0, r)).collect(),
                };
                other
                    .commit(state.version, batch, &fence)
                    .await
                    .expect("interleaved commit");
            }
            .boxed()
        })
    };
    let source = f.source(Some(hook));
    let results = run_once(&f.meta, "w1", Duration::from_secs(5), &source)
        .await
        .unwrap();
    assert!(
        matches!(
            results[..],
            [(
                _,
                RunResult::Ran(Ok(TaskOutcome::MoreWork | TaskOutcome::Idle))
            )]
        ),
        "{results:?}"
    );
    assert!(interleaved.load(Ordering::SeqCst));
    // The hook holds the fixture.
    drop(source);
    f.apply_all("w1").await;
    f.check().await;
    match Arc::try_unwrap(f) {
        Ok(f) => f.shutdown().await,
        Err(_) => panic!("fixture still shared"),
    }
}

/// A link keeps up with a stream that is segmented and trimmed (below what
/// the link applied) while it runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_link_over_a_stream_being_segmented_and_trimmed() {
    let f = Arc::new(Fixture::start().await);
    let cache = RangeCache::new(f.store.clone(), RangeCacheConfig::default())
        .await
        .unwrap();
    let mut worker = Worker::new(
        f.meta.clone(),
        WorkerConfig {
            poll_interval: Duration::from_millis(10),
            ..WorkerConfig::new("w1")
        },
    );
    worker.add_source(Arc::new(f.source(None)));
    worker.add_source(Arc::new(SegmenterSource::new(
        f.store.clone(),
        cache,
        SegmenterConfig {
            min_bytes: 1,
            target_bytes: 512,
            ..SegmenterConfig::default()
        },
    )));
    let worker = worker.start();
    let appender = {
        let f = f.clone();
        tokio::spawn(async move {
            for _ in 0..30 {
                f.append(5).await;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };
    let trimmer = {
        let f = f.clone();
        tokio::spawn(async move {
            let mut trims = 0;
            for _ in 0..40 {
                let applied = f.table().applied().await.expect("applied");
                for (partition, offset) in applied {
                    f.meta
                        .trim_partition(f.stream, partition, offset, None)
                        .await
                        .expect("trim");
                    trims += 1;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            trims
        })
    };
    appender.await.unwrap();
    assert!(trimmer.await.unwrap() > 0);
    let deadline = Instant::now() + WAIT;
    while f.table().applied().await.unwrap() != f.high_watermarks().await {
        assert!(Instant::now() < deadline, "the link never caught up");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    f.check().await;
    let segments = f
        .meta
        .read(Consistency::Local, |s| {
            (0..PARTITIONS)
                .flat_map(|p| s.partition(f.stream, p).unwrap().entries())
                .filter(|e| e.kind == operon_meta::EntryKind::Segment)
                .count()
        })
        .await
        .unwrap();
    let starts = f
        .meta
        .read(Consistency::Local, |s| {
            (0..PARTITIONS)
                .map(|p| s.partition(f.stream, p).unwrap().log_start_offset())
                .collect::<Vec<_>>()
        })
        .await
        .unwrap();
    assert!(starts.iter().any(|s| *s > 0), "nothing was trimmed");
    eprintln!("segments in the index at the end: {segments}");
    worker.stop().await;
    match Arc::try_unwrap(f) {
        Ok(f) => f.shutdown().await,
        Err(_) => panic!("fixture still shared"),
    }
}

/// Values that are not decimal integers, and records without a key, are
/// dead letters: counted as skipped, never applied.
#[tokio::test]
async fn dead_letters_are_counted_and_not_applied() {
    let f = Fixture::start().await;
    f.append(3).await;
    let bad = [
        (Some("c0"), Some("not a number")),
        (None, Some("5")),
        (Some("c1"), None),
    ];
    for (key, value) in bad {
        f.writer
            .append(
                f.stream,
                0,
                vec![Record {
                    key: key.map(|k| Bytes::from(k.to_string())),
                    value: value.map(|v| Bytes::from(v.to_string())),
                    headers: vec![],
                    timestamp_ms: -1,
                }],
            )
            .await
            .unwrap();
    }
    f.apply_all("w1").await;
    let snapshot = f.table().snapshot().await.unwrap();
    assert_eq!(snapshot.counters, *f.model.lock().unwrap());
    assert_eq!(snapshot.skipped, 3);
    f.shutdown().await;
}
