//! The log end to end: concurrent writers on a three-node metastore through a
//! leader failover, and a run under intermittent object-store faults. Every
//! acknowledged append must be readable at its acknowledged offset, offsets
//! must be dense, and no record may appear twice.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::{Meta, WAIT, small_cache};
use operon_common::StreamId;
use operon_log::{
    FetchRequest, LogConfig, LogError, LogReader, LogWriter, Record, Retention, RetentionConfig,
    Segmenter, SegmenterConfig,
};
use operon_meta::{
    Consistency, MetaClient, MetaClientConfig, MetaConfig, MetaNode, Router, SystemClock, WalClass,
};
use operon_store::{Fault, FaultyStore, Op, Store};
use tempfile::TempDir;

const PARTITIONS: u32 = 8;

/// What happened to one append.
#[derive(Clone, Debug)]
enum Outcome {
    Acked {
        partition: u32,
        base: u64,
        values: Vec<String>,
    },
    /// Failed with `CommitUnknown`: may appear, at most once.
    Unknown { values: Vec<String> },
    /// Failed otherwise: must not appear.
    Failed { values: Vec<String> },
}

fn batch(values: &[String]) -> Vec<Record> {
    values
        .iter()
        .map(|v| Record {
            key: None,
            value: Some(Bytes::from(v.clone())),
            headers: vec![],
            timestamp_ms: -1,
        })
        .collect()
}

/// Fetches a whole partition from its log start, retrying failed fetches
/// (injected faults). Returns the log start and the records.
async fn read_partition(
    reader: &LogReader,
    stream: StreamId,
    partition: u32,
) -> (u64, Vec<(u64, String)>) {
    let mut out = Vec::new();
    let mut offset = 0;
    let mut log_start = 0;
    let deadline = Instant::now() + WAIT;
    loop {
        let fetched = reader
            .fetch(FetchRequest {
                stream,
                partition,
                offset,
                max_bytes: 4096,
                max_wait: Duration::ZERO,
            })
            .await;
        let response = match fetched {
            Ok(response) => response,
            Err(err @ (LogError::Cache(_) | LogError::Store(_))) => {
                assert!(Instant::now() < deadline, "fetch kept failing: {err}");
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            }
            Err(LogError::OffsetOutOfRange {
                log_start_offset, ..
            }) if out.is_empty() && offset < log_start_offset => {
                offset = log_start_offset;
                log_start = log_start_offset;
                continue;
            }
            Err(err) => panic!("fetch {partition}@{offset}: {err:?}"),
        };
        if response.records.is_empty() {
            return (log_start, out);
        }
        for record in response.records {
            let value =
                String::from_utf8(record.record.value.expect("value").to_vec()).expect("utf-8");
            out.push((record.offset, value));
        }
        offset = out.last().map_or(offset, |(o, _)| o + 1);
    }
}

/// Checks the log against the outcomes: dense offsets from each partition's
/// log start, every acknowledged append at its offsets (unless trimmed), no
/// duplicates, nothing from a definitely failed append. Returns the log
/// starts.
async fn verify(reader: &LogReader, stream: StreamId, outcomes: &[Outcome]) -> Vec<u64> {
    let mut acked = 0;
    let mut logs: BTreeMap<u32, Vec<(u64, String)>> = BTreeMap::new();
    let mut starts = BTreeMap::new();
    for partition in 0..PARTITIONS {
        let (start, log) = read_partition(reader, stream, partition).await;
        for (i, (offset, _)) in log.iter().enumerate() {
            assert_eq!(
                *offset,
                start + i as u64,
                "partition {partition} is not dense"
            );
        }
        logs.insert(partition, log);
        starts.insert(partition, start);
    }
    let mut seen = BTreeSet::new();
    for log in logs.values() {
        for (_, value) in log {
            assert!(seen.insert(value.clone()), "{value} appears twice");
        }
    }
    let mut allowed = BTreeSet::new();
    for outcome in outcomes {
        match outcome {
            Outcome::Acked {
                partition,
                base,
                values,
            } => {
                acked += 1;
                let (log, start) = (&logs[partition], starts[partition]);
                for (offset, value) in (*base..).zip(values) {
                    if offset < start {
                        continue; // trimmed by retention
                    }
                    let got = log.get((offset - start) as usize).map(|(_, v)| v);
                    assert_eq!(got, Some(value), "partition {partition} offset {offset}");
                }
                allowed.extend(values.iter().cloned());
            }
            Outcome::Unknown { values } => allowed.extend(values.iter().cloned()),
            Outcome::Failed { values } => {
                for value in values {
                    assert!(!seen.contains(value), "failed append's {value} is readable");
                }
            }
        }
    }
    for value in &seen {
        assert!(allowed.contains(value), "{value} came from nowhere");
    }
    assert!(acked > 0);
    starts.into_values().collect()
}

struct Cluster {
    router: Router,
    _dirs: Vec<TempDir>,
    nodes: Vec<MetaNode>,
}

impl Cluster {
    async fn start() -> Self {
        let router = Router::new();
        let snapshots = Store::in_memory();
        let dirs: Vec<TempDir> = (0..3).map(|_| TempDir::new().expect("temp dir")).collect();
        let mut nodes = Vec::new();
        for (id, dir) in (1..=3).zip(&dirs) {
            let config = MetaConfig::new(id, dir.path(), snapshots.clone());
            nodes.push(MetaNode::start(config, &router).await.expect("start"));
        }
        nodes[0].initialize([1, 2, 3]).await.expect("initialize");
        nodes[0].wait_for_leader(WAIT).await.expect("leader");
        Self {
            router,
            _dirs: dirs,
            nodes,
        }
    }

    fn client(&self, local: u64) -> MetaClient {
        let peers = self
            .nodes
            .iter()
            .filter(|n| n.id() != local)
            .cloned()
            .collect();
        let node = self
            .nodes
            .iter()
            .find(|n| n.id() == local)
            .expect("node")
            .clone();
        MetaClient::new(
            node,
            peers,
            Arc::new(SystemClock),
            MetaClientConfig::default(),
        )
    }

    /// The leader every node agrees on.
    async fn leader(&self) -> u64 {
        let deadline = Instant::now() + WAIT;
        loop {
            let mut seen = Vec::new();
            for node in &self.nodes {
                seen.push(node.current_leader().await);
            }
            if let Some(Some(leader)) = seen.first()
                && seen.iter().all(|s| *s == Some(*leader))
            {
                return *leader;
            }
            assert!(Instant::now() < deadline, "no agreed leader: {seen:?}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Waits until every node has applied what the leader has.
    async fn converge(&self, stream: StreamId) {
        let leader = self.client(self.leader().await);
        let expected: Vec<(u64, u64)> = leader
            .read(Consistency::Linearizable, |s| {
                (0..PARTITIONS)
                    .map(|p| {
                        let state = s.partition(stream, p).expect("partition");
                        (state.log_start_offset(), state.high_watermark())
                    })
                    .collect()
            })
            .await
            .expect("linearizable read");
        for node in &self.nodes {
            common::eventually("a node to catch up", || async {
                let local: Vec<(u64, u64)> = node
                    .read(Consistency::Local, |s| {
                        (0..PARTITIONS)
                            .map(|p| {
                                let state = s.partition(stream, p).expect("partition");
                                (state.log_start_offset(), state.high_watermark())
                            })
                            .collect()
                    })
                    .await
                    .expect("read");
                local == expected
            })
            .await;
        }
    }

    async fn shutdown(&self) {
        for node in &self.nodes {
            node.shutdown().await.expect("shutdown");
        }
    }
}

/// Appends from `tasks` tasks of `writer`, each `appends` times, recording
/// every outcome. Store errors are retried (nothing was committed), as is
/// backpressure.
fn spawn_appenders(
    name: &str,
    writer: &LogWriter,
    stream: StreamId,
    tasks: usize,
    appends: usize,
    acks: &Arc<AtomicUsize>,
) -> Vec<tokio::task::JoinHandle<Vec<Outcome>>> {
    (0..tasks)
        .map(|task| {
            let writer = writer.clone();
            let acks = acks.clone();
            let name = format!("{name}-t{task}");
            tokio::spawn(async move {
                let mut outcomes = Vec::new();
                for i in 0..appends {
                    let partition = ((task + i) % PARTITIONS as usize) as u32;
                    let values: Vec<String> =
                        (0..1 + i % 3).map(|j| format!("{name}-{i}-{j}")).collect();
                    let mut attempt = 0;
                    loop {
                        // A retry after a failure that committed nothing gets
                        // fresh values, so the model stays unambiguous.
                        let values: Vec<String> =
                            values.iter().map(|v| format!("{v}-a{attempt}")).collect();
                        match writer.append(stream, partition, batch(&values)).await {
                            Ok(ack) => {
                                acks.fetch_add(1, Ordering::SeqCst);
                                outcomes.push(Outcome::Acked {
                                    partition,
                                    base: ack.base_offset,
                                    values,
                                });
                                break;
                            }
                            Err(LogError::CommitUnknown(_)) => {
                                outcomes.push(Outcome::Unknown { values });
                                break;
                            }
                            Err(LogError::Store(_) | LogError::Backpressure) if attempt < 50 => {
                                outcomes.push(Outcome::Failed { values });
                                attempt += 1;
                                tokio::time::sleep(Duration::from_millis(5)).await;
                            }
                            Err(err) => panic!("append failed: {err:?}"),
                        }
                    }
                }
                outcomes
            })
        })
        .collect()
}

fn log_config(node_id: u64) -> LogConfig {
    LogConfig {
        flush_interval: Duration::from_millis(20),
        ..LogConfig::new(node_id)
    }
}

fn segmenter_config() -> SegmenterConfig {
    SegmenterConfig {
        min_bytes: 1,
        target_bytes: 2048,
        interval: Duration::from_millis(50),
        ..SegmenterConfig::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acknowledged_appends_survive_a_meta_leader_failover() {
    let cluster = Cluster::start().await;
    let admin = cluster.client(1);
    let ns = admin.create_namespace("acme").await.unwrap();
    let stream = admin
        .create_stream_with_retention(
            ns,
            "events",
            PARTITIONS,
            WalClass::Standard,
            // Small enough that retention trims during the run.
            operon_meta::Retention {
                max_age_ms: None,
                max_bytes: Some(1024),
            },
        )
        .await
        .unwrap();
    for node in &cluster.nodes {
        common::eventually("the stream to replicate", || async {
            node.read(Consistency::Local, |s| s.stream(stream).is_some())
                .await
                .unwrap()
        })
        .await;
    }

    let data = Store::in_memory();
    let writer_a =
        LogWriter::start(cluster.client(1), data.clone(), log_config(1)).expect("start writer");
    let writer_b =
        LogWriter::start(cluster.client(2), data.clone(), log_config(2)).expect("start writer");
    let segmenter = Segmenter::new(
        cluster.client(3),
        data.clone(),
        small_cache(&data).await,
        "segmenter-3",
        segmenter_config(),
    )
    .spawn();
    let retention = Retention::new(
        cluster.client(1),
        "retention-1",
        RetentionConfig {
            interval: Duration::from_millis(100),
        },
    )
    .spawn();

    let acks = Arc::new(AtomicUsize::new(0));
    let mut appenders = spawn_appenders("a", &writer_a, stream, 4, 30, &acks);
    appenders.extend(spawn_appenders("b", &writer_b, stream, 4, 30, &acks));

    // Mid-run: cut the leader off, keep writing through the new one, heal.
    common::eventually("some appends", || async {
        acks.load(Ordering::SeqCst) >= 30
    })
    .await;
    let old = cluster.leader().await;
    cluster.router.isolate(old);
    let before = acks.load(Ordering::SeqCst);
    common::eventually("progress without the old leader", || async {
        acks.load(Ordering::SeqCst) >= before + 30
    })
    .await;
    cluster.router.heal(old);

    let mut outcomes = Vec::new();
    for appender in appenders {
        outcomes.extend(appender.await.unwrap());
    }
    writer_a.shutdown().await.unwrap();
    writer_b.shutdown().await.unwrap();
    segmenter.stop().await;
    retention.stop().await;
    // The background loop raced the failover, but under load (or while its
    // node was the isolated leader) it may not have finished a run; one more
    // run makes sure the check below sees trimmed partitions.
    let report = Retention::new(
        cluster.client(cluster.leader().await),
        "retention-1",
        RetentionConfig::default(),
    )
    .run_once()
    .await
    .unwrap();
    assert!(!report.skipped);
    cluster.converge(stream).await;

    // Every node serves the same, correct log.
    for id in 1..=3 {
        let reader = LogReader::new(cluster.client(id), small_cache(&data).await);
        let starts = verify(&reader, stream, &outcomes).await;
        assert!(
            starts.iter().any(|s| *s > 0),
            "retention never trimmed: {starts:?}"
        );
    }
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn intermittent_store_faults_never_corrupt_reads() {
    let meta = Meta::start().await;
    let (_, stream) = meta.stream("acme", "events", PARTITIONS).await;
    let faulty = Arc::new(FaultyStore::new(Store::in_memory().inner().clone()));
    let data = Store::new(faulty.clone());
    let writer =
        LogWriter::start(meta.client.clone(), data.clone(), log_config(1)).expect("start writer");
    let segmenter = Segmenter::new(
        meta.client.clone(),
        data.clone(),
        small_cache(&data).await,
        "segmenter-1",
        segmenter_config(),
    )
    .spawn();
    let reader = LogReader::new(meta.client.clone(), small_cache(&data).await);

    let stop = Arc::new(AtomicBool::new(false));
    // Fail about one PUT in three and one GET in four, never queueing more
    // than one fault per operation.
    let injector = {
        let faulty = faulty.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let (mut last_put, mut last_get, mut i) = (0, 0, 0u64);
            while !stop.load(Ordering::SeqCst) {
                let puts = faulty.calls(Op::Put);
                if puts >= last_put + 3 {
                    let fault = if i % 2 == 0 {
                        Fault::Error
                    } else {
                        Fault::ErrorAfterApply
                    };
                    faulty.inject(Op::Put, fault);
                    last_put = puts + 1;
                    i += 1;
                }
                let gets = faulty.calls(Op::Get);
                if gets >= last_get + 4 {
                    faulty.inject(Op::Get, Fault::Error);
                    last_get = gets + 1;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
    };

    let acks = Arc::new(AtomicUsize::new(0));
    let appenders = spawn_appenders("w", &writer, stream, 6, 25, &acks);
    // Read concurrently: whatever a fetch returns must be what was acknowledged.
    let mut outcomes = Vec::new();
    let mut known: BTreeMap<(u32, u64), String> = BTreeMap::new();
    let mut handles = appenders;
    while !handles.iter().all(|h| h.is_finished()) {
        for partition in 0..PARTITIONS {
            match reader
                .fetch(FetchRequest {
                    stream,
                    partition,
                    offset: 0,
                    max_bytes: 1 << 20,
                    max_wait: Duration::ZERO,
                })
                .await
            {
                Ok(response) => {
                    for (i, record) in response.records.iter().enumerate() {
                        assert_eq!(record.offset, i as u64);
                        let value =
                            String::from_utf8(record.record.value.clone().unwrap().to_vec())
                                .unwrap();
                        let slot = known
                            .entry((partition, record.offset))
                            .or_insert(value.clone());
                        assert_eq!(*slot, value, "offset {} changed", record.offset);
                    }
                }
                Err(LogError::Cache(_) | LogError::Store(_)) => {}
                Err(err) => panic!("fetch: {err:?}"),
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    for handle in handles.drain(..) {
        outcomes.extend(handle.await.unwrap());
    }
    // Faults keep coming while the final state is verified.
    verify(&reader, stream, &outcomes).await;
    let failed = outcomes
        .iter()
        .filter(|o| matches!(o, Outcome::Failed { .. }))
        .count();
    assert!(failed > 0, "no append saw an injected fault");
    stop.store(true, Ordering::SeqCst);
    injector.await.unwrap();

    writer.shutdown().await.unwrap();
    segmenter.stop().await;
    // Drain the queued faults: a call fails while a fault is queued for its
    // operation, so the first success means the queue is empty.
    let existing = data.list("wal/").await.unwrap().remove(0).path;
    while data.get_range(&existing, 0..1).await.is_err() {}
    while data.put("drain", Bytes::new()).await.is_err() {}
    // Fault-free, after the segmenter has had its chance: still the same log.
    let clean = LogReader::new(meta.client.clone(), small_cache(&data).await);
    verify(&clean, stream, &outcomes).await;
    meta.shutdown().await;
}
