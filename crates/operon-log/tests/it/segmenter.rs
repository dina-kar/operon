//! The segmenter, and a differential test of the whole log against a model.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crate::common::{Meta, PausingStore, fast_config, records, small_cache, value};
use operon_common::StreamId;
use operon_log::{
    FetchRequest, LogError, LogReader, LogWriter, Segmenter, SegmenterConfig, SegmenterReport,
    segment,
};
use operon_meta::{Consistency, EntryKind, IndexEntry, ManualClock, MetaClientConfig};
use operon_store::Store;
use proptest::prelude::*;

/// Segments every run of WAL entries as soon as it exists.
fn eager(target_bytes: u64) -> SegmenterConfig {
    SegmenterConfig {
        min_bytes: 1,
        target_bytes,
        ..SegmenterConfig::default()
    }
}

async fn entries(meta: &Meta, stream: StreamId, partition: u32) -> Vec<IndexEntry> {
    meta.client
        .read(Consistency::Local, |s| {
            s.partition(stream, partition)
                .expect("partition")
                .entries()
                .cloned()
                .collect()
        })
        .await
        .expect("read")
}

async fn retired(meta: &Meta) -> Vec<String> {
    meta.client
        .read(Consistency::Local, |s| {
            s.retired().map(|(p, _)| p.to_string()).collect()
        })
        .await
        .expect("read")
}

/// Fetches a partition from its log start to its high watermark in steps of
/// `max_bytes`.
async fn fetch_all(
    reader: &LogReader,
    stream: StreamId,
    partition: u32,
    from: u64,
    max_bytes: usize,
) -> Vec<(u64, String)> {
    let mut out = Vec::new();
    let mut offset = from;
    loop {
        let response = reader
            .fetch(FetchRequest {
                stream,
                partition,
                offset,
                max_bytes,
                max_wait: Duration::ZERO,
            })
            .await
            .expect("fetch");
        if response.records.is_empty() {
            assert_eq!(offset, response.high_watermark);
            return out;
        }
        out.extend(
            response
                .records
                .iter()
                .map(|r| (r.offset, value(&r.record))),
        );
        offset = response.next_offset;
    }
}

#[tokio::test]
async fn segments_hold_contiguous_offsets_and_correct_footers() {
    let meta = Meta::start().await;
    let (_, stream) = meta.stream("acme", "events", 2).await;
    let store = Store::in_memory();
    let writer =
        LogWriter::start(meta.client.clone(), store.clone(), fast_config()).expect("start writer");
    let mut model: BTreeMap<u32, Vec<(u64, String)>> = BTreeMap::new();
    for i in 0..24 {
        let partition = i % 2;
        let batch = records(&format!("r{i}"), 1 + (i as usize) % 4);
        let ack = writer
            .append(stream, partition, batch.clone())
            .await
            .unwrap();
        let slot = model.entry(partition).or_default();
        slot.extend((ack.base_offset..).zip(batch.iter().map(value)));
    }
    let wal_objects: Vec<String> = store
        .list("wal/")
        .await
        .unwrap()
        .into_iter()
        .map(|o| o.path)
        .collect();

    let cache = small_cache(&store).await;
    let segmenter = Segmenter::new(
        meta.client.clone(),
        store.clone(),
        cache.clone(),
        "seg-a",
        eager(600),
    );
    let mut runs = 0;
    loop {
        let report = segmenter.run_once().await.unwrap();
        assert_eq!(report.failed, 0);
        if report.segments == 0 {
            break;
        }
        runs += 1;
        assert!(runs < 100);
    }
    assert!(runs > 1, "the target size splits the runs");

    let reader = LogReader::new(meta.client.clone(), cache);
    for partition in 0..2 {
        let entries = entries(&meta, stream, partition).await;
        assert!(entries.iter().all(|e| e.kind == EntryKind::Segment));
        let mut next = 0;
        for entry in &entries {
            assert_eq!(entry.base_offset, next);
            let (bytes, _) = store.get(&entry.object).await.unwrap();
            let footer = segment::parse(&bytes).unwrap();
            assert_eq!(footer.stream, stream);
            assert_eq!(footer.partition, partition);
            assert_eq!(footer.base_offset, entry.base_offset);
            assert_eq!(footer.end_offset, entry.end_offset());
            assert_eq!(footer.data, entry.byte_range);
            next = entry.end_offset();
        }
        assert_eq!(
            fetch_all(&reader, stream, partition, 0, 100).await,
            model[&partition]
        );
    }
    // The WAL objects are retired, not deleted.
    let mut retired = retired(&meta).await;
    retired.sort();
    let mut expected = wal_objects.clone();
    expected.sort();
    assert_eq!(retired, expected);
    for object in &wal_objects {
        store.head(object).await.unwrap();
    }
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}

#[tokio::test]
async fn runs_wait_for_enough_bytes_or_enough_age() {
    let clock = Arc::new(ManualClock::new(1_000_000));
    let meta = Meta::start_with(clock.clone(), MetaClientConfig::default()).await;
    let (_, stream) = meta.stream("acme", "events", 1).await;
    let store = Store::in_memory();
    let writer =
        LogWriter::start(meta.client.clone(), store.clone(), fast_config()).expect("start writer");
    let mut batch = records("young", 3);
    for record in &mut batch {
        record.timestamp_ms = 1_000_000;
    }
    writer.append(stream, 0, batch).await.unwrap();

    let config = SegmenterConfig {
        min_bytes: 1 << 20,
        max_wal_age: Duration::from_secs(60),
        ..SegmenterConfig::default()
    };
    let segmenter = Segmenter::new(
        meta.client.clone(),
        store.clone(),
        small_cache(&store).await,
        "seg-a",
        config,
    );
    assert_eq!(
        segmenter.run_once().await.unwrap(),
        SegmenterReport::default()
    );
    clock.advance(Duration::from_secs(61));
    assert_eq!(segmenter.run_once().await.unwrap().segments, 1);
    assert_eq!(entries(&meta, stream, 0).await[0].kind, EntryKind::Segment);
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}

/// A segmenter whose lease is taken over while it writes its segment is
/// fenced: the index is unchanged, and its segment object is deleted.
#[tokio::test]
async fn a_zombie_segmenter_is_fenced_and_changes_nothing() {
    let clock = Arc::new(ManualClock::new(1_000_000));
    let meta = Meta::start_with(clock.clone(), MetaClientConfig::default()).await;
    let (_, stream) = meta.stream("acme", "events", 1).await;
    let (gate, store) = PausingStore::create();
    let writer =
        LogWriter::start(meta.client.clone(), store.clone(), fast_config()).expect("start writer");
    writer.append(stream, 0, records("a", 3)).await.unwrap();
    let before = entries(&meta, stream, 0).await;

    let config = SegmenterConfig {
        lease_ttl: Duration::from_secs(30),
        ..eager(1 << 20)
    };
    let zombie = Segmenter::new(
        meta.client.clone(),
        store.clone(),
        small_cache(&store).await,
        "zombie",
        config,
    );
    gate.arm("ns/");
    let run = tokio::spawn(async move { zombie.run_once().await });
    gate.paused().await;
    // The zombie stalls past its lease; someone else takes the partition.
    clock.advance(Duration::from_secs(31));
    let key = format!("task/segmenter/{stream}/0");
    let grant = meta
        .client
        .acquire_lease(&key, "successor", Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(grant.epoch, 2);
    gate.release();

    let report = run.await.unwrap().unwrap();
    assert_eq!(
        report,
        SegmenterReport {
            segments: 0,
            skipped: 1,
            failed: 0
        }
    );
    assert_eq!(entries(&meta, stream, 0).await, before);
    assert!(store.list("ns/").await.unwrap().is_empty());
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}

/// The index changes under a run (a trim here): the swap is an index
/// mismatch, and the segmenter deletes the segment it wrote.
#[tokio::test]
async fn an_index_mismatch_deletes_the_new_segment() {
    let meta = Meta::start().await;
    let (_, stream) = meta.stream("acme", "events", 1).await;
    let (gate, store) = PausingStore::create();
    let writer =
        LogWriter::start(meta.client.clone(), store.clone(), fast_config()).expect("start writer");
    writer.append(stream, 0, records("a", 3)).await.unwrap();
    writer.append(stream, 0, records("b", 2)).await.unwrap();

    let segmenter = Segmenter::new(
        meta.client.clone(),
        store.clone(),
        small_cache(&store).await,
        "seg-a",
        eager(1 << 20),
    );
    gate.arm("ns/");
    let run = tokio::spawn(async move { segmenter.run_once().await });
    gate.paused().await;
    meta.client
        .trim_partition(stream, 0, 3, None)
        .await
        .unwrap();
    gate.release();

    let report = run.await.unwrap().unwrap();
    assert_eq!(report.skipped, 1);
    assert_eq!(report.segments, 0);
    assert!(store.list("ns/").await.unwrap().is_empty());
    let kinds: Vec<EntryKind> = entries(&meta, stream, 0)
        .await
        .iter()
        .map(|e| e.kind)
        .collect();
    assert_eq!(kinds, [EntryKind::Wal]);
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}

#[derive(Clone, Debug)]
enum Step {
    Append {
        partition: u32,
        records: usize,
    },
    Segment {
        target_bytes: u64,
    },
    Trim {
        partition: u32,
        keep: prop::sample::Index,
    },
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        4 => (0u32..2, 1usize..6).prop_map(|(partition, records)| Step::Append { partition, records }),
        2 => (100u64..800).prop_map(|target_bytes| Step::Segment { target_bytes }),
        1 => (0u32..2, any::<prop::sample::Index>()).prop_map(|(partition, keep)| Step::Trim { partition, keep }),
    ]
}

/// Runs a random schedule and checks, after every step, that fetching each
/// partition from its log start returns exactly the model's records, and that
/// offsets below the log start are out of range.
async fn differential(steps: Vec<Step>, max_bytes: usize) {
    let meta = Meta::start().await;
    let (_, stream) = meta.stream("acme", "events", 2).await;
    let store = Store::in_memory();
    let writer =
        LogWriter::start(meta.client.clone(), store.clone(), fast_config()).expect("start writer");
    let cache = small_cache(&store).await;
    let reader = LogReader::new(meta.client.clone(), cache.clone());
    let mut model: BTreeMap<u32, Vec<(u64, String)>> = BTreeMap::new();
    let mut log_start: BTreeMap<u32, u64> = BTreeMap::new();

    for (i, step) in steps.into_iter().enumerate() {
        match step {
            Step::Append {
                partition,
                records: n,
            } => {
                let batch = records(&format!("s{i}"), n);
                let ack = writer
                    .append(stream, partition, batch.clone())
                    .await
                    .expect("append");
                model
                    .entry(partition)
                    .or_default()
                    .extend((ack.base_offset..).zip(batch.iter().map(value)));
            }
            Step::Segment { target_bytes } => {
                let segmenter = Segmenter::new(
                    meta.client.clone(),
                    store.clone(),
                    cache.clone(),
                    "seg",
                    eager(target_bytes),
                );
                let report = segmenter.run_once().await.expect("segment");
                assert_eq!(report.failed, 0);
            }
            Step::Trim { partition, keep } => {
                let hwm = meta.high_watermark(stream, partition).await;
                let before = keep.index(hwm as usize + 1) as u64;
                let start = meta
                    .client
                    .trim_partition(stream, partition, before, None)
                    .await
                    .expect("trim");
                let slot = log_start.entry(partition).or_default();
                *slot = (*slot).max(before);
                assert_eq!(start, *slot);
            }
        }
        for partition in 0..2 {
            let start = log_start.get(&partition).copied().unwrap_or(0);
            let expected: Vec<(u64, String)> = model
                .get(&partition)
                .map(|m| m.iter().filter(|(o, _)| *o >= start).cloned().collect())
                .unwrap_or_default();
            assert_eq!(
                fetch_all(&reader, stream, partition, start, max_bytes).await,
                expected
            );
            if start > 0 {
                let err = reader
                    .fetch(FetchRequest {
                        stream,
                        partition,
                        offset: start - 1,
                        max_bytes,
                        max_wait: Duration::ZERO,
                    })
                    .await
                    .expect_err("below the log start");
                assert!(matches!(err, LogError::OffsetOutOfRange { .. }), "{err:?}");
            }
        }
    }
    writer.shutdown().await.expect("shutdown");
    meta.shutdown().await;
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 12, ..ProptestConfig::default() })]

    #[test]
    fn fetches_match_the_model_under_random_schedules(
        steps in prop::collection::vec(step(), 1..25),
        max_bytes in prop_oneof![Just(1usize), 50usize..400, Just(usize::MAX)],
    ) {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(differential(steps, max_bytes));
    }
}

/// Review M7: a swap applied by an earlier attempt (whose acknowledgement was
/// lost) and then trimmed makes the retry an index mismatch. The segment is
/// retired in the metastore, so the segmenter must leave it to garbage
/// collection instead of deleting it.
#[tokio::test]
async fn a_segment_the_metastore_retired_is_not_deleted_on_mismatch() {
    let meta = Meta::start().await;
    let (_, stream) = meta.stream("acme", "events", 1).await;
    let (gate, store) = PausingStore::create();
    let writer =
        LogWriter::start(meta.client.clone(), store.clone(), fast_config()).expect("start writer");
    writer.append(stream, 0, records("a", 3)).await.unwrap();
    let wal = entries(&meta, stream, 0).await;

    let segmenter = Segmenter::new(
        meta.client.clone(),
        store.clone(),
        small_cache(&store).await,
        "seg-a",
        eager(1 << 20),
    );
    gate.arm("ns/");
    let run = tokio::spawn(async move { segmenter.run_once().await });
    gate.paused().await;
    // As if an earlier attempt of this swap had been applied, then trimmed.
    let path = gate.held_path();
    meta.client
        .swap_segment(
            stream,
            0,
            vec![(0, wal[0].object.clone())],
            &path,
            40..41,
            0,
            None,
            operon_meta::Freshness {
                created_at_ms: meta.client.now_ms(),
                max_age_ms: 600_000,
            },
        )
        .await
        .unwrap();
    meta.client
        .trim_partition(stream, 0, 3, None)
        .await
        .unwrap();
    assert!(retired(&meta).await.contains(&path));
    gate.release();

    let report = run.await.unwrap().unwrap();
    assert_eq!(report.skipped, 1);
    store
        .head(&path)
        .await
        .expect("the retired segment is kept");
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}

/// Review M10, M0.4: the worker framework renews the task lease while a run
/// takes longer than the lease TTL, so the run's own lease expiring does not
/// fence its swap.
#[tokio::test]
async fn a_run_longer_than_its_lease_still_swaps() {
    let meta = Meta::start().await;
    let (_, stream) = meta.stream("acme", "events", 1).await;
    let (gate, store) = PausingStore::create();
    let writer =
        LogWriter::start(meta.client.clone(), store.clone(), fast_config()).expect("start writer");
    writer.append(stream, 0, records("a", 3)).await.unwrap();

    let config = SegmenterConfig {
        lease_ttl: Duration::from_millis(300),
        ..eager(1 << 20)
    };
    let segmenter = Segmenter::new(
        meta.client.clone(),
        store.clone(),
        small_cache(&store).await,
        "seg-a",
        config,
    );
    gate.arm("ns/");
    let run = tokio::spawn(async move { segmenter.run_once().await });
    gate.paused().await;
    // Hold the segment PUT for several lease TTLs.
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    gate.release();
    assert_eq!(run.await.unwrap().unwrap().segments, 1);
    let epoch = meta
        .client
        .read(Consistency::Local, move |s| {
            s.lease(&format!("task/segmenter/{stream}/0"))
                .unwrap()
                .epoch
        })
        .await
        .unwrap();
    assert_eq!(epoch, 1);
    assert_eq!(entries(&meta, stream, 0).await[0].kind, EntryKind::Segment);
    writer.shutdown().await.unwrap();
    meta.shutdown().await;
}
