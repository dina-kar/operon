//! The leaderless `standard` write path.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{Meta, fast_config, faulty_store, read_direct, records, value};
use operon_common::StreamId;
use operon_log::{LogConfig, LogError, LogWriter, Record};
use operon_meta::{
    ApplyError, Clock, Consistency, ManualClock, MetaClient, MetaClientConfig, MetaError,
    WAL_COMMIT_WINDOW_MS,
};
use operon_store::{Fault, Op, Store};

/// Appends from many tasks to many partitions: every partition's acks tile
/// `[0, n)` in the order each task submitted, and the stored records match.
#[tokio::test]
async fn acks_are_dense_and_ordered_per_partition() {
    let meta = Meta::start().await;
    let (_, stream) = meta.stream("acme", "events", 4).await;
    let store = Store::in_memory();
    let writer =
        LogWriter::start(meta.client.clone(), store.clone(), fast_config()).expect("start writer");

    let mut tasks = Vec::new();
    for task in 0..8 {
        let writer = writer.clone();
        tasks.push(tokio::spawn(async move {
            let mut acks = Vec::new();
            for i in 0..15 {
                let partition = (task + i) % 4;
                let batch = records(&format!("t{task}-{i}"), 1 + i % 3);
                let ack = writer
                    .append(stream, partition as u32, batch.clone())
                    .await
                    .expect("append");
                acks.push((ack, batch));
            }
            acks
        }));
    }
    let mut by_partition: BTreeMap<u32, Vec<(u64, u64, Vec<Record>)>> = BTreeMap::new();
    for task in tasks {
        let mut last: BTreeMap<u32, u64> = BTreeMap::new();
        for (ack, batch) in task.await.unwrap() {
            assert_eq!(ack.stream, stream);
            assert_eq!(ack.last_offset - ack.base_offset + 1, batch.len() as u64);
            // One task's appends to a partition keep their submission order.
            if let Some(previous) = last.insert(ack.partition, ack.last_offset) {
                assert!(ack.base_offset > previous);
            }
            by_partition.entry(ack.partition).or_default().push((
                ack.base_offset,
                ack.last_offset,
                batch,
            ));
        }
    }
    for (partition, mut acks) in by_partition {
        acks.sort_by_key(|(base, _, _)| *base);
        let mut next = 0;
        for (base, last, _) in &acks {
            assert_eq!(*base, next, "partition {partition}");
            next = last + 1;
        }
        assert_eq!(meta.high_watermark(stream, partition).await, next);
        let stored = read_direct(&meta.client, &store, stream, partition).await;
        let expected: Vec<String> = acks
            .iter()
            .flat_map(|(_, _, batch)| batch.iter().map(value))
            .collect();
        let got: Vec<String> = stored.iter().map(|r| value(&r.record)).collect();
        assert_eq!(got, expected, "partition {partition}");
    }
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}

#[tokio::test]
async fn one_flush_writes_one_object_for_many_streams_and_partitions() {
    let meta = Meta::start().await;
    let (_, a) = meta.stream("acme", "a", 2).await;
    let (_, b) = meta.stream("acme", "b", 2).await;
    let (faulty, store) = faulty_store();
    let config = LogConfig {
        flush_interval: Duration::from_secs(3600),
        ..LogConfig::new(1)
    };
    let writer =
        LogWriter::start(meta.client.clone(), store.clone(), config).expect("start writer");

    let mut appends = Vec::new();
    for (stream, partition) in [(a, 0), (a, 1), (b, 0), (b, 1), (a, 0)] {
        let writer = writer.clone();
        appends.push(tokio::spawn(async move {
            writer.append(stream, partition, records("x", 2)).await
        }));
    }
    // Wait until every append is buffered, then flush once.
    common::eventually("appends buffered", || async {
        writer.buffered_appends() == 5
    })
    .await;
    assert_eq!(faulty.calls(Op::Put), 0);
    writer.flush().await.unwrap();
    for append in appends {
        append.await.unwrap().unwrap();
    }
    assert_eq!(faulty.calls(Op::Put), 1);

    let objects: Vec<String> = meta
        .client
        .read(Consistency::Local, |s| {
            [(a, 0), (a, 1), (b, 0), (b, 1)]
                .iter()
                .flat_map(|(stream, p)| s.partition(*stream, *p).unwrap().entries())
                .map(|e| e.object.clone())
                .collect()
        })
        .await
        .unwrap();
    assert_eq!(objects.len(), 4, "one chunk per partition");
    assert!(objects.iter().all(|o| *o == objects[0]));
    assert!(objects[0].starts_with("wal/standard/1/") && objects[0].ends_with(".wal"));
    assert_eq!(meta.high_watermark(a, 0).await, 4);
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}

#[tokio::test]
async fn a_failed_put_fails_its_appends_and_commits_nothing() {
    let meta = Meta::start().await;
    let (_, stream) = meta.stream("acme", "events", 1).await;
    let (faulty, store) = faulty_store();
    let writer =
        LogWriter::start(meta.client.clone(), store.clone(), fast_config()).expect("start writer");

    for fault in [Fault::Error, Fault::ErrorAfterApply] {
        faulty.inject(Op::Put, fault);
        let err = writer
            .append(stream, 0, records("lost", 3))
            .await
            .unwrap_err();
        assert!(matches!(err, LogError::Store(_)), "{err:?}");
        assert_eq!(meta.high_watermark(stream, 0).await, 0);
    }
    // The next flush works, starting at offset 0.
    let ack = writer.append(stream, 0, records("kept", 2)).await.unwrap();
    assert_eq!((ack.base_offset, ack.last_offset), (0, 1));
    // The object written despite the error is an orphan: not in the index.
    let wal_objects = store.list("wal/").await.unwrap();
    assert_eq!(wal_objects.len(), 2);
    let stored = read_direct(&meta.client, &store, stream, 0).await;
    let values: Vec<String> = stored.iter().map(|r| value(&r.record)).collect();
    assert_eq!(values, ["kept-0", "kept-1"]);
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}

/// The metastore applies the commit but the writer sees a timeout: the retry
/// gets the first commit's offsets, and nothing is committed twice.
#[tokio::test]
async fn a_lost_commit_acknowledgement_is_retried_without_duplicates() {
    // No retries inside the client, so the writer's own commit loop retries.
    let no_retry = MetaClientConfig {
        retry_deadline: Duration::ZERO,
        ..MetaClientConfig::default()
    };
    let meta = Meta::start_with(Arc::new(operon_meta::SystemClock), no_retry).await;
    let (_, stream) = meta.stream("acme", "events", 1).await;
    let store = Store::in_memory();
    let writer =
        LogWriter::start(meta.client.clone(), store.clone(), fast_config()).expect("start writer");

    meta.client.inject_lost_ack();
    let first = writer.append(stream, 0, records("a", 3)).await.unwrap();
    assert_eq!((first.base_offset, first.last_offset), (0, 2));
    assert_eq!(meta.high_watermark(stream, 0).await, 3);
    let second = writer.append(stream, 0, records("b", 2)).await.unwrap();
    assert_eq!((second.base_offset, second.last_offset), (3, 4));
    let values: Vec<String> = read_direct(&meta.client, &store, stream, 0)
        .await
        .iter()
        .map(|r| value(&r.record))
        .collect();
    assert_eq!(values, ["a-0", "a-1", "a-2", "b-0", "b-1"]);
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}

#[tokio::test]
async fn a_commit_that_never_lands_fails_with_commit_unknown() {
    let no_retry = MetaClientConfig {
        retry_deadline: Duration::ZERO,
        ..MetaClientConfig::default()
    };
    let meta = Meta::start_with(Arc::new(operon_meta::SystemClock), no_retry).await;
    let (_, stream) = meta.stream("acme", "events", 1).await;
    let config = LogConfig {
        commit_retry_deadline: Duration::from_millis(300),
        ..fast_config()
    };
    let writer =
        LogWriter::start(meta.client.clone(), Store::in_memory(), config).expect("start writer");
    // Local reads keep working, but no write can commit any more.
    meta.shutdown().await;
    let started = Instant::now();
    let err = writer.append(stream, 0, records("x", 1)).await.unwrap_err();
    assert!(matches!(err, LogError::CommitUnknown(_)), "{err:?}");
    assert!(started.elapsed() >= Duration::from_millis(250));
    writer.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_rejected_commit_fails_its_appends_with_the_rejection() {
    // The writer's clock is 20 minutes behind the metastore's.
    let clock = Arc::new(ManualClock::new(10 * WAL_COMMIT_WINDOW_MS));
    let meta = Meta::start_with(clock.clone(), MetaClientConfig::default()).await;
    let (_, stream) = meta.stream("acme", "events", 1).await;
    meta.client.prune_wal_commits().await.unwrap();
    clock.set(10 * WAL_COMMIT_WINDOW_MS - 2 * WAL_COMMIT_WINDOW_MS);
    let writer = LogWriter::start(meta.client.clone(), Store::in_memory(), fast_config())
        .expect("start writer");
    let err = writer.append(stream, 0, records("x", 1)).await.unwrap_err();
    assert!(
        matches!(
            err,
            LogError::Meta(MetaError::Rejected(ApplyError::StaleCommit { .. }))
        ),
        "{err:?}"
    );
    assert_eq!(meta.high_watermark(stream, 0).await, 0);
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}

#[tokio::test]
async fn appends_beyond_the_buffer_limit_are_refused() {
    let meta = Meta::start().await;
    let (_, stream) = meta.stream("acme", "events", 1).await;
    let batch_len = operon_log::batch::encode(&records("x", 20)).unwrap().len();
    let config = LogConfig {
        flush_interval: Duration::from_secs(3600),
        max_buffered_bytes: batch_len * 3 / 2,
        ..LogConfig::new(1)
    };
    let writer =
        LogWriter::start(meta.client.clone(), Store::in_memory(), config).expect("start writer");

    let buffered = {
        let writer = writer.clone();
        tokio::spawn(async move { writer.append(stream, 0, records("x", 20)).await })
    };
    common::eventually("the first append is buffered", || async {
        writer.buffered_appends() == 1
    })
    .await;
    let err = writer
        .append(stream, 0, records("x", 20))
        .await
        .unwrap_err();
    assert!(matches!(err, LogError::Backpressure), "{err:?}");
    let err = writer
        .append(stream, 0, records("x", 40))
        .await
        .unwrap_err();
    assert!(matches!(err, LogError::InvalidArgument(_)), "{err:?}");

    writer.flush().await.unwrap();
    assert_eq!(buffered.await.unwrap().unwrap().base_offset, 0);
    // With the buffer drained, appends are accepted again.
    let writer2 = writer.clone();
    let next = tokio::spawn(async move { writer2.append(stream, 0, records("y", 20)).await });
    common::eventually("the next append is buffered", || async {
        writer.buffered_appends() == 1
    })
    .await;
    writer.flush().await.unwrap();
    assert_eq!(next.await.unwrap().unwrap().base_offset, 20);
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}

#[tokio::test]
async fn shutdown_flushes_buffered_appends_and_then_refuses_new_ones() {
    let meta = Meta::start().await;
    let (_, stream) = meta.stream("acme", "events", 1).await;
    let config = LogConfig {
        flush_interval: Duration::from_secs(3600),
        ..LogConfig::new(1)
    };
    let store = Store::in_memory();
    let writer =
        LogWriter::start(meta.client.clone(), store.clone(), config).expect("start writer");
    let mut appends = Vec::new();
    for i in 0..3 {
        let writer = writer.clone();
        appends.push(tokio::spawn(async move {
            writer.append(stream, 0, records(&format!("s{i}"), 2)).await
        }));
    }
    common::eventually("appends buffered", || async {
        writer.buffered_appends() == 3
    })
    .await;
    assert_eq!(meta.high_watermark(stream, 0).await, 0);

    writer.shutdown().await.unwrap();
    for append in appends {
        append.await.unwrap().unwrap();
    }
    assert_eq!(meta.high_watermark(stream, 0).await, 6);
    let err = writer
        .append(stream, 0, records("late", 1))
        .await
        .unwrap_err();
    assert!(matches!(err, LogError::Closed), "{err:?}");
    // A second shutdown is harmless.
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}

#[tokio::test]
async fn invalid_appends_are_rejected() {
    let meta = Meta::start().await;
    let (_, stream) = meta.stream("acme", "events", 2).await;
    let config = LogConfig {
        max_batch_records: 5,
        ..fast_config()
    };
    let writer =
        LogWriter::start(meta.client.clone(), Store::in_memory(), config).expect("start writer");

    let err = writer
        .append(StreamId(99), 0, records("x", 1))
        .await
        .unwrap_err();
    assert!(
        matches!(err, LogError::UnknownStream(StreamId(99))),
        "{err:?}"
    );
    let err = writer.append(stream, 2, records("x", 1)).await.unwrap_err();
    assert!(
        matches!(err, LogError::UnknownPartition { partition: 2, .. }),
        "{err:?}"
    );
    let err = writer.append(stream, 0, vec![]).await.unwrap_err();
    assert!(matches!(err, LogError::InvalidArgument(_)), "{err:?}");
    let err = writer.append(stream, 0, records("x", 6)).await.unwrap_err();
    assert!(matches!(err, LogError::InvalidArgument(_)), "{err:?}");
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}

#[tokio::test]
async fn negative_timestamps_get_the_writers_clock() {
    let clock = Arc::new(ManualClock::new(1_700_000_000_000));
    let meta = Meta::start_with(clock, MetaClientConfig::default()).await;
    let (_, stream) = meta.stream("acme", "events", 1).await;
    let store = Store::in_memory();
    let writer =
        LogWriter::start(meta.client.clone(), store.clone(), fast_config()).expect("start writer");
    let mut batch = records("t", 2);
    batch[0].timestamp_ms = -1;
    writer.append(stream, 0, batch).await.unwrap();
    let stored = read_direct(&meta.client, &store, stream, 0).await;
    assert_eq!(stored[0].record.timestamp_ms, 1_700_000_000_000);
    assert_eq!(stored[1].record.timestamp_ms, 1_001);
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}

#[tokio::test]
async fn buffered_appends_flush_on_the_interval() {
    let meta = Meta::start().await;
    let (_, stream) = meta.stream("acme", "events", 1).await;
    let config = LogConfig {
        flush_interval: Duration::from_millis(100),
        ..LogConfig::new(1)
    };
    let writer =
        LogWriter::start(meta.client.clone(), Store::in_memory(), config).expect("start writer");
    let started = Instant::now();
    writer.append(stream, 0, records("x", 1)).await.unwrap();
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_millis(100), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}

/// Review I1: the first commit attempt is applied but its acknowledgement is
/// lost; before the retry lands, the commit record is pruned. The retry is
/// rejected as stale, but the records are committed, so the append must fail
/// with `CommitUnknown`, never as a definite rejection.
#[tokio::test]
async fn a_stale_rejection_of_a_retried_commit_is_commit_unknown() {
    let clock = Arc::new(ManualClock::new(1_700_000_000_000));
    let slow_retries = MetaClientConfig {
        backoff: Duration::from_secs(2),
        ..MetaClientConfig::default()
    };
    let meta = Meta::start_with(clock.clone(), slow_retries).await;
    let (_, stream) = meta.stream("acme", "events", 1).await;
    let writer = LogWriter::start(meta.client.clone(), Store::in_memory(), fast_config())
        .expect("start writer");
    let pruner = MetaClient::new(
        meta.node.clone(),
        vec![],
        clock.clone(),
        MetaClientConfig::default(),
    );

    meta.client.inject_lost_ack();
    let append = {
        let writer = writer.clone();
        tokio::spawn(async move { writer.append(stream, 0, records("x", 3)).await })
    };
    // The first attempt is applied; the client now backs off for 2 s.
    common::eventually("the first attempt to apply", || async {
        meta.high_watermark(stream, 0).await == 3
    })
    .await;
    clock.advance(Duration::from_millis(2 * WAL_COMMIT_WINDOW_MS + 10));
    assert_eq!(pruner.prune_wal_commits().await.unwrap(), 1);

    let err = append.await.unwrap().unwrap_err();
    assert!(matches!(err, LogError::CommitUnknown(_)), "{err:?}");
    // The records are committed, once.
    assert_eq!(meta.high_watermark(stream, 0).await, 3);
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}

/// Review I2: a writer whose clock is far ahead is refused before anything is
/// proposed (a definite failure), and appends from a correct clock keep working.
#[tokio::test]
async fn a_writer_with_a_skewed_clock_is_refused_and_others_keep_working() {
    let meta = Meta::start().await;
    let (_, stream) = meta.stream("acme", "events", 1).await;
    let store = Store::in_memory();
    let ahead = MetaClient::new(
        meta.node.clone(),
        vec![],
        Arc::new(ManualClock::new(
            operon_meta::SystemClock.now_ms() + 86_400_000,
        )),
        MetaClientConfig::default(),
    );
    let skewed = LogWriter::start(ahead, store.clone(), fast_config()).expect("start writer");
    let err = skewed
        .append(stream, 0, records("future", 1))
        .await
        .unwrap_err();
    assert!(
        matches!(err, LogError::Meta(MetaError::ClockSkew { .. })),
        "{err:?}"
    );
    skewed.shutdown().await.unwrap();

    let writer =
        LogWriter::start(meta.client.clone(), store.clone(), fast_config()).expect("start writer");
    for i in 0..3 {
        let ack = writer.append(stream, 0, records("now", 2)).await.unwrap();
        assert_eq!(ack.base_offset, 2 * i);
    }
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}

/// Review M5: a flush or shutdown that waits for the flush in flight gets
/// that flush's error.
#[tokio::test]
async fn shutdown_reports_the_error_of_the_flush_in_flight() {
    let no_retry = MetaClientConfig {
        retry_deadline: Duration::ZERO,
        ..MetaClientConfig::default()
    };
    let meta = Meta::start_with(Arc::new(operon_meta::SystemClock), no_retry).await;
    let (_, stream) = meta.stream("acme", "events", 1).await;
    let config = LogConfig {
        commit_retry_deadline: Duration::from_secs(2),
        flush_interval: Duration::from_millis(200),
        ..LogConfig::new(1)
    };
    let writer =
        LogWriter::start(meta.client.clone(), Store::in_memory(), config).expect("start writer");
    meta.shutdown().await;
    let append = {
        let writer = writer.clone();
        tokio::spawn(async move { writer.append(stream, 0, records("x", 1)).await })
    };
    common::eventually("the append to be buffered", || async {
        writer.buffered_appends() == 1
    })
    .await;
    // The flush takes it; its commit then fails for 2 s.
    common::eventually("the flush to start", || async {
        writer.buffered_appends() == 0
    })
    .await;
    assert!(!append.is_finished());
    let err = writer.shutdown().await.unwrap_err();
    assert!(matches!(err, LogError::CommitUnknown(_)), "{err:?}");
    let err = append.await.unwrap().unwrap_err();
    assert!(matches!(err, LogError::CommitUnknown(_)), "{err:?}");
}

#[tokio::test]
async fn invalid_configs_are_rejected() {
    let meta = Meta::start().await;
    for config in [
        LogConfig {
            commit_retry_deadline: Duration::from_secs(301),
            ..LogConfig::new(1)
        },
        LogConfig {
            max_batch_records: 0,
            ..LogConfig::new(1)
        },
        LogConfig {
            flush_bytes: 0,
            ..LogConfig::new(1)
        },
    ] {
        let err = LogWriter::start(meta.client.clone(), Store::in_memory(), config).unwrap_err();
        assert!(matches!(err, LogError::InvalidArgument(_)), "{err:?}");
    }
    meta.shutdown().await;
}
