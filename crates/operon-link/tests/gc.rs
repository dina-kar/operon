//! Garbage collection alongside writers, readers, the segmenter, retention
//! and a link.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use operon_cache::{RangeCache, RangeCacheConfig};
use operon_common::{NamespaceId, StreamId};
use operon_link::{CounterTable, LinkApplySource, LinkConfig, LinkGcRoots};
use operon_log::gc::{GcConfig, GcSource};
use operon_log::{
    FetchRequest, LogConfig, LogError, LogReader, LogWriter, Record, RetentionConfig,
    RetentionSource, SegmenterConfig, SegmenterSource,
};
use operon_meta::{
    Clock, Consistency, ManualClock, MetaClient, MetaClientConfig, MetaConfig, MetaNode, Router,
    SystemClock, TargetRef, WalClass,
};
use operon_store::{Store, StoreError};
use operon_worker::{Worker, WorkerConfig, run_once};
use proptest::prelude::*;
use tempfile::TempDir;
use ulid::Ulid;

const WAIT: Duration = Duration::from_secs(30);

async fn start_meta(clock: Arc<dyn Clock>) -> (MetaNode, MetaClient, TempDir) {
    let dir = TempDir::new().expect("temp dir");
    let mut config = MetaConfig::new(1, dir.path(), Store::in_memory());
    config.clock = clock.clone();
    let node = MetaNode::start(config, &Router::new())
        .await
        .expect("start meta");
    node.initialize([1]).await.expect("initialize");
    node.wait_for_leader(WAIT).await.expect("leader");
    let client = MetaClient::new(node.clone(), vec![], clock, MetaClientConfig::default());
    (node, client, dir)
}

fn counter_target() -> TargetRef {
    TargetRef {
        kind: "counter".to_string(),
        name: "counts".to_string(),
    }
}

async fn exists(store: &Store, path: &str) -> bool {
    match store.head(path).await {
        Ok(_) => true,
        Err(StoreError::NotFound { .. }) => false,
        Err(err) => panic!("head {path}: {err}"),
    }
}

fn record(key: &str, value: String) -> Record {
    Record {
        key: Some(Bytes::from(key.to_string())),
        value: Some(Bytes::from(value)),
        headers: vec![],
        timestamp_ms: -1,
    }
}

/// Reads a partition from `from` to its high watermark.
async fn read_all(
    reader: &LogReader,
    stream: StreamId,
    partition: u32,
    from: u64,
) -> Result<Vec<(u64, Option<Bytes>)>, LogError> {
    let mut out = Vec::new();
    let mut offset = from;
    loop {
        let response = reader
            .fetch(FetchRequest {
                stream,
                partition,
                offset,
                max_bytes: 4096,
                max_wait: Duration::ZERO,
            })
            .await?;
        if response.records.is_empty() {
            return Ok(out);
        }
        out.extend(
            response
                .records
                .into_iter()
                .map(|r| (r.offset, r.record.value)),
        );
        offset = response.next_offset;
    }
}

/// The link's old manifests beyond `keep_manifests`, and the data and
/// manifests of commits that never landed, are deleted once old; the live
/// manifest, its recent ancestors and every data file it lists stay.
#[tokio::test]
async fn link_objects_are_collected_by_reachability() {
    let clock = Arc::new(ManualClock::new(SystemClock.now_ms()));
    let (node, meta, _dir) = start_meta(clock.clone()).await;
    let store = Store::in_memory();
    let ns = meta.create_namespace("acme").await.unwrap();
    let stream = meta
        .create_stream(ns, "events", 1, WalClass::Standard)
        .await
        .unwrap();
    let link = meta
        .create_link(ns, "counts", stream, counter_target(), BTreeMap::new())
        .await
        .unwrap();
    let writer = LogWriter::start(
        meta.clone(),
        store.clone(),
        LogConfig {
            flush_interval: Duration::from_millis(5),
            ..LogConfig::new(1)
        },
    )
    .unwrap();
    let cache = RangeCache::new(store.clone(), RangeCacheConfig::default())
        .await
        .unwrap();
    let reader = LogReader::new(meta.clone(), cache);
    let source = LinkApplySource::new(
        reader,
        store.clone(),
        LinkConfig {
            batch_records: 1,
            batch_interval: Duration::ZERO,
            ..LinkConfig::default()
        },
    );
    let mut sum = 0;
    for i in 0..15i64 {
        writer
            .append(stream, 0, vec![record("c", i.to_string())])
            .await
            .unwrap();
        sum += i;
        run_once(&meta, "w1", Duration::from_secs(30), &source)
            .await
            .unwrap();
    }
    let table = CounterTable::open(meta.clone(), store.clone(), ns, "counts")
        .await
        .unwrap();
    let before = table.snapshot().await.unwrap();
    assert_eq!(before.version, 15);
    assert_eq!(before.counters["c"], sum);
    // Orphans a crashed commit would leave.
    let now = clock.now_ms();
    let prefix = format!("ns/{ns}/links/{link}/");
    let orphan_data = format!(
        "{prefix}data/{}.cnt",
        Ulid::from_parts(now, Ulid::generate().random())
    );
    let orphan_manifest = format!(
        "{prefix}manifests/{:020}-{}.man",
        16,
        Ulid::from_parts(now, Ulid::generate().random())
    );
    for path in [&orphan_data, &orphan_manifest] {
        store.put(path, Bytes::from_static(b"x")).await.unwrap();
    }
    let gc = GcSource::with_roots(
        store.clone(),
        GcConfig {
            grace: Duration::from_secs(60),
            keep_manifests: 3,
            ..GcConfig::default()
        },
        vec![Arc::new(LinkGcRoots)],
    );
    // Young: nothing goes.
    let report = gc.run_once(&meta, "gc").await.unwrap().unwrap();
    assert_eq!(report.orphan_other, 0);
    // Step well past the grace period.
    clock.advance(Duration::from_secs(600));
    let report = gc.run_once(&meta, "gc").await.unwrap().unwrap();
    // Manifests 1..=11 and the two orphans.
    assert_eq!(report.orphan_other, 13, "{report:?}");
    assert!(!exists(&store, &orphan_data).await);
    assert!(!exists(&store, &orphan_manifest).await);
    let kept: Vec<u64> = store
        .list(&format!("{prefix}manifests/"))
        .await
        .unwrap()
        .iter()
        .map(|info| {
            let name = info.path.rsplit('/').next().unwrap();
            name.split('-').next().unwrap().parse().unwrap()
        })
        .collect();
    assert_eq!(kept, vec![12, 13, 14, 15]);
    // The table reads the same, and keeps working.
    assert_eq!(table.snapshot().await.unwrap(), before);
    writer
        .append(stream, 0, vec![record("c", "100".to_string())])
        .await
        .unwrap();
    run_once(&meta, "w1", Duration::from_secs(30), &source)
        .await
        .unwrap();
    assert_eq!(table.get("c").await.unwrap(), sum + 100);
    writer.shutdown().await.unwrap();
    node.shutdown().await.unwrap();
}

#[derive(Clone, Debug)]
struct Workload {
    partitions: u32,
    rounds: u32,
    value_len: usize,
}

fn workload() -> impl Strategy<Value = Workload> {
    (1u32..4, 20u32..40, 1usize..64).prop_map(|(partitions, rounds, value_len)| Workload {
        partitions,
        rounds,
        value_len,
    })
}

struct Harness {
    node: MetaNode,
    meta: MetaClient,
    store: Store,
    writer: LogWriter,
    reader: LogReader,
    ns: NamespaceId,
    /// No retention; the link reads it.
    events: StreamId,
    /// Trimmed by size.
    logs: StreamId,
    /// Per stream and partition: offset → value of every acknowledged record.
    acked: Mutex<BTreeMap<(StreamId, u32), BTreeMap<u64, Bytes>>>,
    sums: Mutex<BTreeMap<String, i64>>,
    _dir: TempDir,
}

impl Harness {
    async fn append(&self, stream: StreamId, partition: u32, key: &str, delta: i64, pad: usize) {
        let value = if stream == self.events {
            delta.to_string()
        } else {
            format!("{delta}-{}", "x".repeat(pad))
        };
        let ack = self
            .writer
            .append(stream, partition, vec![record(key, value.clone())])
            .await
            .expect("append");
        self.acked
            .lock()
            .expect("lock")
            .entry((stream, partition))
            .or_default()
            .insert(ack.base_offset, Bytes::from(value));
        if stream == self.events {
            *self
                .sums
                .lock()
                .expect("lock")
                .entry(key.to_string())
                .or_default() += delta;
        }
    }

    fn acked_now(&self, stream: StreamId, partition: u32) -> BTreeMap<u64, Bytes> {
        self.acked
            .lock()
            .expect("lock")
            .get(&(stream, partition))
            .cloned()
            .unwrap_or_default()
    }

    /// Reads every partition while GC, the segmenter and retention run:
    /// the reads are dense, return exactly the acknowledged values, and (for
    /// the stream without retention) include every record acknowledged
    /// before the read began.
    async fn verify_reads(&self, partitions: u32) {
        for stream in [self.events, self.logs] {
            for partition in 0..partitions {
                let before = self.acked_now(stream, partition);
                let mut attempts = 0;
                let (start, read) = loop {
                    let start = self
                        .meta
                        .read(Consistency::Local, |s| {
                            s.partition(stream, partition)
                                .expect("partition")
                                .log_start_offset()
                        })
                        .await
                        .expect("read");
                    match read_all(&self.reader, stream, partition, start).await {
                        Ok(read) => break (start, read),
                        // Retention trimmed under the read: start again.
                        Err(LogError::OffsetOutOfRange { .. }) if stream == self.logs => {
                            attempts += 1;
                            assert!(attempts < 100, "reads keep losing to retention");
                        }
                        Err(err) => panic!("read {stream}/{partition}: {err}"),
                    }
                };
                let after = self.acked_now(stream, partition);
                for (i, (offset, value)) in read.iter().enumerate() {
                    assert_eq!(*offset, start + i as u64, "{stream}/{partition}: not dense");
                    if let Some(acked) = after.get(offset) {
                        assert_eq!(value.as_ref(), Some(acked), "{stream}/{partition}/{offset}");
                    }
                }
                if stream == self.events {
                    assert_eq!(start, 0);
                    let end = start + read.len() as u64;
                    for offset in before.keys() {
                        assert!(
                            *offset < end,
                            "{stream}/{partition}: acked {offset} missing"
                        );
                    }
                }
            }
        }
    }
}

async fn gc_alongside_everything(w: Workload) {
    let (node, meta, dir) = start_meta(Arc::new(SystemClock)).await;
    let store = Store::in_memory();
    let ns = meta.create_namespace("acme").await.expect("namespace");
    let events = meta
        .create_stream(ns, "events", w.partitions, WalClass::Standard)
        .await
        .expect("stream");
    let logs = meta
        .create_stream_with_retention(
            ns,
            "logs",
            w.partitions,
            WalClass::Standard,
            operon_meta::Retention {
                max_age_ms: None,
                max_bytes: Some(1024),
            },
        )
        .await
        .expect("stream");
    meta.create_link(ns, "counts", events, counter_target(), BTreeMap::new())
        .await
        .expect("link");
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
    let reader = LogReader::new(meta.clone(), cache.clone());
    let h = Arc::new(Harness {
        node,
        meta: meta.clone(),
        store: store.clone(),
        writer,
        reader: reader.clone(),
        ns,
        events,
        logs,
        acked: Mutex::default(),
        sums: Mutex::default(),
        _dir: dir,
    });

    // Orphans: a WAL object whose commit never landed (old enough), and a
    // young one that must survive the whole run.
    let now = SystemClock.now_ms();
    let old_wal = operon_log::paths::wal_object(
        WalClass::Standard,
        7,
        Ulid::from_parts(now - 3 * 3_600_000, Ulid::generate().random()),
    );
    let young_wal = operon_log::paths::wal_object(
        WalClass::Standard,
        7,
        Ulid::from_parts(now, Ulid::generate().random()),
    );
    let orphan_segment = operon_log::paths::segment(
        ns,
        events,
        0,
        0,
        Ulid::from_parts(now, Ulid::generate().random()),
    );
    for path in [&old_wal, &young_wal, &orphan_segment] {
        store
            .put(path, Bytes::from_static(b"orphan"))
            .await
            .expect("put");
    }

    let grace = Duration::from_millis(1_500);
    let gc = GcSource::with_roots(
        store.clone(),
        GcConfig {
            grace,
            interval: Duration::from_millis(100),
            keep_manifests: 2,
            ..GcConfig::default()
        },
        vec![Arc::new(LinkGcRoots)],
    );
    let mut worker = Worker::new(
        meta.clone(),
        WorkerConfig {
            poll_interval: Duration::from_millis(20),
            ..WorkerConfig::new("w1")
        },
    );
    worker.add_source(Arc::new(LinkApplySource::new(
        reader.clone(),
        store.clone(),
        LinkConfig {
            batch_records: 5,
            batch_interval: Duration::ZERO,
            // Below the grace period, as the server clamps it (review M3).
            max_commit_delay: grace / 2,
            ..LinkConfig::default()
        },
    )));
    worker.add_source(Arc::new(SegmenterSource::new(
        store.clone(),
        cache,
        SegmenterConfig {
            min_bytes: 1,
            target_bytes: 1024,
            swap_deadline: grace / 2,
            ..SegmenterConfig::default()
        },
    )));
    worker.add_source(Arc::new(RetentionSource::new(RetentionConfig {
        interval: Duration::from_millis(50),
    })));
    worker.add_source(Arc::new(gc.clone()));
    let worker = worker.start();

    let stop = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = (0..2)
        .map(|_| {
            let h = h.clone();
            let stop = stop.clone();
            let partitions = w.partitions;
            tokio::spawn(async move {
                let mut rounds = 0u32;
                while !stop.load(Ordering::SeqCst) {
                    h.verify_reads(partitions).await;
                    rounds += 1;
                    tokio::time::sleep(Duration::from_millis(15)).await;
                }
                rounds
            })
        })
        .collect();
    for round in 0..w.rounds {
        for partition in 0..w.partitions {
            let delta = i64::from(round) * 3 - i64::from(partition);
            let key = format!("c{}", round % 4);
            h.append(events, partition, &key, delta, w.value_len).await;
            h.append(logs, partition, &key, delta, w.value_len).await;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    // Run on past the grace period, so retired objects get collected.
    tokio::time::sleep(grace + Duration::from_millis(500)).await;
    stop.store(true, Ordering::SeqCst);
    for reader in readers {
        assert!(reader.await.expect("reader") > 0);
    }

    // The link catches up with exact sums.
    let table = CounterTable::open(meta.clone(), store.clone(), ns, "counts")
        .await
        .expect("table");
    let hwms = || async {
        meta.read(Consistency::Local, |s| {
            (0..w.partitions)
                .map(|p| (p, s.partition(events, p).expect("p").high_watermark()))
                .collect::<BTreeMap<u32, u64>>()
        })
        .await
        .expect("read")
    };
    let deadline = Instant::now() + WAIT;
    while table.applied().await.expect("applied") != hwms().await {
        assert!(Instant::now() < deadline, "the link never caught up");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let snapshot = table.snapshot().await.expect("snapshot");
    assert_eq!(snapshot.counters, *h.sums.lock().expect("lock"));
    assert_eq!(snapshot.skipped, 0);
    h.verify_reads(w.partitions).await;

    // Old orphans and retired objects are collected; young orphans are not.
    let deadline = Instant::now() + WAIT;
    loop {
        let retired_old = meta
            .read(Consistency::Local, |s| {
                let cutoff = SystemClock.now_ms().saturating_sub(3 * 1_500);
                s.retired().filter(|(_, at)| *at < cutoff).count()
            })
            .await
            .expect("read");
        let done = retired_old == 0
            && !exists(&store, &old_wal).await
            && !exists(&store, &orphan_segment).await;
        if done {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "orphans or retired objects were never collected"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        exists(&store, &young_wal).await,
        "a young WAL orphan was deleted"
    );
    let report = gc.report();
    assert!(report.retired > 0, "{report:?}");
    assert!(
        report.orphan_wal >= 1 && report.orphan_segments >= 1,
        "{report:?}"
    );
    let violations = meta
        .read(Consistency::Local, |s| s.check_invariants())
        .await
        .expect("read");
    assert!(violations.is_empty(), "{violations:?}");
    worker.stop().await;
    let h = match Arc::try_unwrap(h) {
        Ok(h) => h,
        Err(_) => panic!("harness still shared"),
    };
    h.writer.shutdown().await.expect("writer");
    h.node.shutdown().await.expect("meta");
    let _ = (h.ns, h.store);
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 3, ..ProptestConfig::default() })]

    /// Review focus 2: GC running continuously never removes an object a
    /// read, a segment swap or a link still needs.
    #[test]
    fn gc_never_breaks_reads_links_or_the_index(w in workload()) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(gc_alongside_everything(w));
    }
}
