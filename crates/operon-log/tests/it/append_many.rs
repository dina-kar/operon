//! `LogWriter::append_many`: one request, one WAL object, one commit.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crate::common::{Meta, PausingStore, faulty_store, read_direct, records, value};
use operon_common::StreamId;
use operon_log::{AppendAck, LogConfig, LogError, LogWriter, Record};
use operon_meta::{Consistency, EntryKind, IndexEntry, MetaClient, MetaClientConfig};
use operon_store::{Fault, Op, Store};

/// A writer config that flushes only when asked (or when the buffer fills).
fn manual_flush() -> LogConfig {
    LogConfig {
        flush_interval: Duration::from_secs(3600),
        ..LogConfig::new(1)
    }
}

/// One batch of `n` records per partition, tagged `"<tag>p<partition>"`.
fn batches(tag: &str, partitions: &[u32], n: usize) -> Vec<(u32, Vec<Record>)> {
    partitions
        .iter()
        .map(|&p| (p, records(&format!("{tag}p{p}"), n)))
        .collect()
}

/// The WAL objects a partition's index entries point to.
async fn objects(meta: &MetaClient, stream: StreamId, partition: u32) -> Vec<String> {
    meta.read(Consistency::Local, |s| {
        s.partition(stream, partition)
            .expect("partition")
            .entries()
            .map(|e| e.object.clone())
            .collect()
    })
    .await
    .expect("read")
}

async fn values(meta: &MetaClient, store: &Store, stream: StreamId, partition: u32) -> Vec<String> {
    read_direct(meta, store, stream, partition)
        .await
        .iter()
        .map(|r| value(&r.record))
        .collect()
}

#[tokio::test]
async fn append_many_places_every_partition_in_one_wal_object_and_one_commit() {
    let meta = Meta::start().await;
    let (_, stream) = meta.stream("acme", "events", 3).await;
    let (faulty, store) = faulty_store();
    let writer =
        LogWriter::start(meta.client.clone(), store.clone(), manual_flush()).expect("start");

    let puts = faulty.calls(Op::PutCreate);
    // Not in partition order: the acks follow the input order.
    let acks = {
        let request = batches("a", &[2, 0, 1], 5);
        let task = {
            let writer = writer.clone();
            tokio::spawn(async move { writer.append_many(stream, request).await })
        };
        crate::common::eventually("the request is buffered", || async {
            writer.buffered_appends() == 3
        })
        .await;
        writer.flush().await.unwrap();
        task.await.unwrap().unwrap()
    };
    assert_eq!(faulty.calls(Op::PutCreate), puts + 1);
    let expected: Vec<AppendAck> = [2, 0, 1]
        .iter()
        .map(|&partition| AppendAck {
            stream,
            partition,
            base_offset: 0,
            last_offset: 4,
        })
        .collect();
    assert_eq!(acks, expected);

    let mut all = Vec::new();
    for partition in 0..3 {
        let objects = objects(&meta.client, stream, partition).await;
        assert_eq!(objects.len(), 1, "partition {partition}");
        all.extend(objects);
        assert_eq!(meta.high_watermark(stream, partition).await, 5);
        let expected: Vec<String> = (0..5).map(|i| format!("ap{partition}-{i}")).collect();
        assert_eq!(
            values(&meta.client, &store, stream, partition).await,
            expected
        );
    }
    assert!(all.iter().all(|o| *o == all[0]), "one WAL object: {all:?}");
    let object = all[0].clone();
    let base_offsets = meta
        .client
        .read(Consistency::Local, move |s| {
            s.wal_commit(&object).map(|c| c.base_offsets.clone())
        })
        .await
        .unwrap()
        .expect("the object's commit record");
    assert_eq!(base_offsets, [0, 0, 0], "one commit with three chunks");
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}

#[tokio::test]
async fn append_many_is_all_or_nothing_under_a_failed_put() {
    let meta = Meta::start().await;
    let (_, stream) = meta.stream("acme", "events", 3).await;
    let (faulty, store) = faulty_store();
    let writer = LogWriter::start(
        meta.client.clone(),
        store.clone(),
        crate::common::fast_config(),
    )
    .expect("start");
    writer
        .append_many(stream, batches("before", &[0, 1, 2], 2))
        .await
        .unwrap();

    faulty.inject(Op::PutCreate, Fault::Error);
    let err = writer
        .append_many(stream, batches("lost", &[0, 1, 2], 5))
        .await
        .unwrap_err();
    assert!(matches!(err, LogError::Store(_)), "{err:?}");
    for partition in 0..3 {
        assert_eq!(meta.high_watermark(stream, partition).await, 2);
        let expected = [
            format!("beforep{partition}-0"),
            format!("beforep{partition}-1"),
        ];
        assert_eq!(
            values(&meta.client, &store, stream, partition).await,
            expected
        );
    }
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}

/// The metastore applies the commit but the writer sees a timeout: the retry
/// gets the first commit's offsets, and nothing is committed twice.
#[tokio::test]
async fn append_many_with_a_lost_commit_ack_commits_once() {
    // No retries inside the client, so the writer's own commit loop retries.
    let no_retry = MetaClientConfig {
        retry_deadline: Duration::ZERO,
        ..MetaClientConfig::default()
    };
    let meta = Meta::start_with(Arc::new(operon_meta::SystemClock), no_retry).await;
    let (_, stream) = meta.stream("acme", "events", 3).await;
    let store = Store::in_memory();
    let writer = LogWriter::start(
        meta.client.clone(),
        store.clone(),
        crate::common::fast_config(),
    )
    .expect("start");
    writer
        .append_many(stream, batches("before", &[0, 1, 2], 2))
        .await
        .unwrap();

    meta.client.inject_lost_ack();
    let acks = writer
        .append_many(stream, batches("a", &[0, 1, 2], 5))
        .await
        .unwrap();
    for (partition, ack) in (0..3).zip(&acks) {
        assert_eq!(ack.partition, partition);
        assert_eq!((ack.base_offset, ack.last_offset), (2, 6));
        assert_eq!(meta.high_watermark(stream, partition).await, 2 + 5);
        let got = values(&meta.client, &store, stream, partition).await;
        let expected: Vec<String> = (0..2)
            .map(|i| format!("beforep{partition}-{i}"))
            .chain((0..5).map(|i| format!("ap{partition}-{i}")))
            .collect();
        assert_eq!(got, expected);
    }
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}

/// Starts a writer that never flushes on its own, so anything a rejected
/// request buffered would still be visible afterwards.
async fn rejecting_writer(meta: &Meta) -> (StreamId, LogWriter) {
    let (_, stream) = meta.stream("acme", "events", 3).await;
    let config = LogConfig {
        max_batch_records: 10,
        ..manual_flush()
    };
    let writer = LogWriter::start(meta.client.clone(), Store::in_memory(), config).expect("start");
    (stream, writer)
}

async fn assert_nothing_buffered(meta: &Meta, stream: StreamId, writer: &LogWriter) {
    assert_eq!(writer.buffered_appends(), 0);
    writer.flush().await.expect("flush");
    for partition in 0..3 {
        assert_eq!(meta.high_watermark(stream, partition).await, 0);
    }
}

#[tokio::test]
async fn append_many_rejects_a_repeated_partition() {
    let meta = Meta::start().await;
    let (stream, writer) = rejecting_writer(&meta).await;
    let err = writer
        .append_many(stream, batches("x", &[0, 1, 0], 2))
        .await
        .unwrap_err();
    assert!(matches!(err, LogError::InvalidArgument(_)), "{err:?}");
    assert_nothing_buffered(&meta, stream, &writer).await;
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}

#[tokio::test]
async fn append_many_rejects_an_empty_batch() {
    let meta = Meta::start().await;
    let (stream, writer) = rejecting_writer(&meta).await;
    let mut request = batches("x", &[0, 1], 2);
    request.push((2, vec![]));
    let err = writer.append_many(stream, request).await.unwrap_err();
    assert!(matches!(err, LogError::InvalidArgument(_)), "{err:?}");
    // An empty request is refused too.
    let err = writer.append_many(stream, vec![]).await.unwrap_err();
    assert!(matches!(err, LogError::InvalidArgument(_)), "{err:?}");
    assert_nothing_buffered(&meta, stream, &writer).await;
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}

#[tokio::test]
async fn append_many_rejects_an_unknown_partition() {
    let meta = Meta::start().await;
    let (stream, writer) = rejecting_writer(&meta).await;
    let err = writer
        .append_many(stream, batches("x", &[0, 3], 2))
        .await
        .unwrap_err();
    assert!(
        matches!(err, LogError::UnknownPartition { partition: 3, .. }),
        "{err:?}"
    );
    let err = writer
        .append_many(StreamId(99), batches("x", &[0], 2))
        .await
        .unwrap_err();
    assert!(
        matches!(err, LogError::UnknownStream(StreamId(99))),
        "{err:?}"
    );
    assert_nothing_buffered(&meta, stream, &writer).await;
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}

/// A batch over `max_batch_records` is refused, and so is a request whose
/// total size exceeds the buffer: alone (`InvalidArgument`) or together with
/// what is buffered (`Backpressure`).
#[tokio::test]
async fn append_many_refuses_requests_beyond_the_limits() {
    let meta = Meta::start().await;
    let (_, stream) = meta.stream("acme", "events", 3).await;
    let batch_len = operon_log::batch::encode(&records("xp0", 5)).unwrap().len();
    let config = LogConfig {
        max_batch_records: 10,
        max_buffered_bytes: batch_len * 5 / 2,
        ..manual_flush()
    };
    let writer = LogWriter::start(meta.client.clone(), Store::in_memory(), config).expect("start");

    let err = writer
        .append_many(stream, vec![(0, records("x", 2)), (1, records("x", 11))])
        .await
        .unwrap_err();
    assert!(matches!(err, LogError::InvalidArgument(_)), "{err:?}");
    let err = writer
        .append_many(stream, batches("x", &[0, 1, 2], 5))
        .await
        .unwrap_err();
    assert!(matches!(err, LogError::InvalidArgument(_)), "{err:?}");
    assert_eq!(writer.buffered_appends(), 0);

    let buffered = {
        let writer = writer.clone();
        tokio::spawn(async move { writer.append_many(stream, batches("x", &[0], 5)).await })
    };
    crate::common::eventually("the first request is buffered", || async {
        writer.buffered_appends() == 1
    })
    .await;
    let err = writer
        .append_many(stream, batches("x", &[1, 2], 5))
        .await
        .unwrap_err();
    assert!(matches!(err, LogError::Backpressure), "{err:?}");
    assert_eq!(writer.buffered_appends(), 1);
    writer.flush().await.unwrap();
    assert_eq!(buffered.await.unwrap().unwrap()[0].base_offset, 0);
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}

/// Tasks mixing `append` and `append_many` on the same partitions: every ack
/// matches the stored records, offsets are dense, each task's records keep
/// its submission order in every partition, and every `append_many` request
/// landed in one WAL object.
#[tokio::test]
async fn concurrent_appends_and_append_many_keep_per_partition_order() {
    const TASKS: usize = 8;
    const ITERATIONS: usize = 200;
    const PARTITIONS: u32 = 4;
    let meta = Meta::start().await;
    let (_, stream) = meta.stream("acme", "events", PARTITIONS).await;
    let store = Store::in_memory();
    let config = LogConfig {
        flush_interval: Duration::from_millis(2),
        ..LogConfig::new(1)
    };
    let writer = LogWriter::start(meta.client.clone(), store.clone(), config).expect("start");

    let mut tasks = Vec::new();
    for task in 0..TASKS {
        let writer = writer.clone();
        tasks.push(tokio::spawn(async move {
            let mut acked = Vec::new();
            let mut requests = Vec::new();
            for i in 0..ITERATIONS {
                let first = ((task + i) % PARTITIONS as usize) as u32;
                let n = 1 + i % 3;
                if i % 2 == 0 {
                    let batch = records(&format!("t{task}-i{i}-p{first}"), n);
                    let ack = writer
                        .append(stream, first, batch.clone())
                        .await
                        .expect("append");
                    acked.push((ack, batch));
                } else {
                    let width = 2 + i % 3;
                    let request: Vec<(u32, Vec<Record>)> = (0..width as u32)
                        .map(|k| {
                            let p = (first + k) % PARTITIONS;
                            (p, records(&format!("t{task}-i{i}-p{p}"), n))
                        })
                        .collect();
                    let acks = writer
                        .append_many(stream, request.clone())
                        .await
                        .expect("append_many");
                    assert_eq!(acks.len(), request.len());
                    requests.push(acks.clone());
                    for (ack, (partition, batch)) in acks.into_iter().zip(request) {
                        assert_eq!(ack.partition, partition);
                        acked.push((ack, batch));
                    }
                }
            }
            (acked, requests)
        }));
    }
    let mut acked: Vec<(AppendAck, Vec<Record>)> = Vec::new();
    let mut requests: Vec<Vec<AppendAck>> = Vec::new();
    for task in tasks {
        let (task_acked, task_requests) = task.await.unwrap();
        acked.extend(task_acked);
        requests.extend(task_requests);
    }

    let mut stored: BTreeMap<u32, Vec<String>> = BTreeMap::new();
    for partition in 0..PARTITIONS {
        let records = read_direct(&meta.client, &store, stream, partition).await;
        for (expected, r) in (0..).zip(&records) {
            assert_eq!(r.offset, expected, "dense offsets in partition {partition}");
        }
        assert_eq!(
            meta.high_watermark(stream, partition).await,
            records.len() as u64
        );
        stored.insert(
            partition,
            records.iter().map(|r| value(&r.record)).collect(),
        );
    }
    let mut total = 0;
    for (ack, batch) in &acked {
        assert_eq!(ack.stream, stream);
        let got = &stored[&ack.partition][ack.base_offset as usize..=ack.last_offset as usize];
        let expected: Vec<String> = batch.iter().map(value).collect();
        assert_eq!(got, expected.as_slice());
        total += batch.len();
    }
    assert_eq!(
        total,
        stored.values().map(Vec::len).sum::<usize>(),
        "every stored record was acknowledged"
    );
    // Every batch of one request is in the same WAL object.
    let mut entries = BTreeMap::new();
    for partition in 0..PARTITIONS {
        let partition_entries: Vec<IndexEntry> = meta
            .client
            .read(Consistency::Local, move |s| {
                s.partition(stream, partition)
                    .expect("partition")
                    .entries()
                    .cloned()
                    .collect()
            })
            .await
            .unwrap();
        entries.insert(partition, partition_entries);
    }
    let object_of = |ack: &AppendAck| -> String {
        let entry = entries[&ack.partition]
            .iter()
            .find(|e| e.base_offset <= ack.base_offset && ack.last_offset < e.end_offset())
            .expect("an index entry holds the acked batch");
        assert_eq!(entry.kind, EntryKind::Wal);
        entry.object.clone()
    };
    for acks in &requests {
        let object = object_of(&acks[0]);
        for ack in &acks[1..] {
            assert_eq!(
                object_of(ack),
                object,
                "one WAL object per request: {acks:?}"
            );
        }
    }
    // Per task and partition, the iteration numbers only grow with the offset.
    for (partition, values) in &stored {
        let mut last: BTreeMap<usize, usize> = BTreeMap::new();
        for v in values {
            let mut parts = v.split('-');
            let task: usize = parts.next().unwrap()[1..].parse().unwrap();
            let iteration: usize = parts.next().unwrap()[1..].parse().unwrap();
            if let Some(previous) = last.insert(task, iteration) {
                assert!(
                    iteration >= previous,
                    "partition {partition}: task {task} wrote i{iteration} after i{previous}"
                );
            }
        }
    }
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}

/// The writer crashes after the request's WAL object was written but before
/// it was committed: the object is an orphan (GC's job), no partition's index
/// points to it, and a restarted writer sees none of the request's batches.
#[tokio::test]
async fn a_crash_between_put_and_commit_loses_every_batch_of_the_request_together() {
    let orphan = crash_during_wal_put(true).await;
    assert!(orphan, "the held WAL object was written");
}

/// The writer crashes while the request's WAL object is being written: no
/// object, and none of the request's batches.
#[tokio::test]
async fn a_crash_during_the_put_loses_every_batch_of_the_request_together() {
    let orphan = crash_during_wal_put(false).await;
    assert!(!orphan, "the held WAL object was never written");
}

/// Holds the WAL PUT of a three-partition `append_many` (after applying it
/// if `after_apply`), crashes the writer there, and checks that a restarted
/// writer sees none of the request and starts every partition at offset 0.
/// Returns whether the held object exists in the store.
async fn crash_during_wal_put(after_apply: bool) -> bool {
    let meta = Meta::start().await;
    let (_, stream) = meta.stream("acme", "events", 3).await;
    let (gate, store) = PausingStore::create();
    if after_apply {
        gate.arm_after("wal/");
    } else {
        gate.arm("wal/");
    }
    let writer = LogWriter::start(
        meta.client.clone(),
        store.clone(),
        crate::common::fast_config(),
    )
    .expect("start");
    let request = {
        let writer = writer.clone();
        tokio::spawn(async move {
            writer
                .append_many(stream, batches("lost", &[0, 1, 2], 5))
                .await
        })
    };
    gate.paused().await;
    let held = gate.held_path();
    assert!(held.starts_with("wal/standard/1/"), "{held}");
    // Crash: the caller and the writer are gone, and the held PUT never
    // returns. The flush task stays parked on it until the runtime stops.
    request.abort();
    let _ = request.await;
    drop(writer);

    let orphan = store.head(&held).await.is_ok();
    let restarted = LogWriter::start(
        meta.client.clone(),
        store.clone(),
        crate::common::fast_config(),
    )
    .expect("start");
    let committed = {
        let held = held.clone();
        meta.client
            .read(Consistency::Local, move |s| s.wal_commit(&held).is_some())
            .await
            .expect("read")
    };
    assert!(!committed, "the held object was not committed");
    for partition in 0..3 {
        assert_eq!(meta.high_watermark(stream, partition).await, 0);
        assert!(objects(&meta.client, stream, partition).await.is_empty());
        assert!(
            values(&meta.client, &store, stream, partition)
                .await
                .is_empty()
        );
    }
    // The restarted writer appends from offset 0 on every partition.
    let acks = restarted
        .append_many(stream, batches("after", &[0, 1, 2], 2))
        .await
        .expect("append_many");
    assert!(
        acks.iter()
            .all(|a| (a.base_offset, a.last_offset) == (0, 1))
    );
    for partition in 0..3 {
        let objects = objects(&meta.client, stream, partition).await;
        assert!(!objects.contains(&held), "partition {partition}");
    }
    restarted.shutdown().await.expect("shutdown");
    meta.shutdown().await;
    orphan
}
