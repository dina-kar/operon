//! Sequencer and offset index: WAL commits, segment swaps, trims.

use operon_common::StreamId;
use operon_common::meta::{
    ApplyError, Consistency, EntryKind, Freshness, IndexEntry, SegmentSwap, WAL_COMMIT_WINDOW_MS,
    WalCommit,
};

use super::{chunk, commit, high_watermark, namespace, rejected, retired, stamp, stream};
use crate::Backend;

const L: Consistency = Consistency::Linearizable;

fn fresh_now(now_ms: u64) -> Freshness {
    Freshness {
        created_at_ms: now_ms,
        max_age_ms: 60_000,
    }
}

fn swap(
    stream: StreamId,
    replaces: &[(u64, &str)],
    segment: &str,
    fresh: Freshness,
) -> SegmentSwap {
    SegmentSwap {
        stream,
        partition: 0,
        replaces: replaces
            .iter()
            .map(|(base, object)| (*base, (*object).to_string()))
            .collect(),
        segment: segment.to_string(),
        byte_range: 0..500,
        max_timestamp_ms: 7,
        fence: None,
        fresh,
    }
}

/// The objects of a partition's index entries, from offset 0.
async fn objects(meta: &dyn operon_common::meta::MetaStore, s: StreamId) -> Vec<(u64, String)> {
    meta.partition_index(L, s, 0, 0, None)
        .await
        .expect("read")
        .expect("partition")
        .into_entries()
        .into_iter()
        .map(|e| (e.base_offset, e.object))
        .collect()
}

pub async fn commit_wal_assigns_dense_offsets_per_partition(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "seq-dense").await;
    let s = stream(meta, ns, "events", 2).await;
    assert_eq!(
        commit(
            meta,
            "seq-dense/wal-1",
            vec![chunk(s, 0, 3, 0..30), chunk(s, 1, 2, 30..50)]
        )
        .await,
        vec![0, 0]
    );
    assert_eq!(
        commit(db.last(), "seq-dense/wal-2", vec![chunk(s, 0, 4, 0..40)]).await,
        vec![3]
    );
    assert_eq!(
        commit(
            meta,
            "seq-dense/wal-3",
            vec![chunk(s, 1, 1, 0..10), chunk(s, 0, 1, 10..20)]
        )
        .await,
        vec![2, 7]
    );
    assert_eq!(high_watermark(db.last(), s, 0).await, 8);
    assert_eq!(high_watermark(db.last(), s, 1).await, 3);

    let missing = StreamId(s.0 + 1000);
    let stale_stream = meta
        .commit_wal(WalCommit {
            object: "seq-dense/wal-4".to_string(),
            created_at_ms: meta.now_ms(),
            chunks: vec![chunk(missing, 0, 1, 0..10)],
        })
        .await;
    assert!(!stale_stream.earlier_unknown);
    assert_eq!(
        rejected(stale_stream.result),
        ApplyError::StreamNotFound(missing)
    );
    let bad_partition = meta
        .commit_wal(WalCommit {
            object: "seq-dense/wal-5".to_string(),
            created_at_ms: meta.now_ms(),
            chunks: vec![chunk(s, 0, 1, 0..10), chunk(s, 2, 1, 10..20)],
        })
        .await
        .into_result();
    assert_eq!(
        rejected(bad_partition),
        ApplyError::PartitionNotFound {
            stream: s,
            partition: 2
        }
    );
    // A rejected commit changes nothing.
    assert_eq!(high_watermark(db.last(), s, 0).await, 8);
}

pub async fn commit_wal_retry_returns_the_first_offsets(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "seq-retry").await;
    let s = stream(meta, ns, "events", 1).await;
    assert_eq!(
        commit(meta, "seq-retry/wal-1", vec![chunk(s, 0, 5, 0..50)]).await,
        vec![0]
    );
    assert_eq!(
        commit(meta, "seq-retry/wal-2", vec![chunk(s, 0, 2, 0..20)]).await,
        vec![5]
    );
    // The retry returns the first commit's offsets, whatever it carries now.
    let retry = db
        .last()
        .commit_wal(WalCommit {
            object: "seq-retry/wal-1".to_string(),
            created_at_ms: meta.now_ms(),
            chunks: vec![chunk(s, 0, 9, 0..90)],
        })
        .await;
    assert!(!retry.earlier_unknown);
    assert_eq!(retry.result.expect("retry"), vec![0]);
    assert_eq!(high_watermark(db.last(), s, 0).await, 7);
    assert_eq!(
        objects(db.last(), s).await,
        vec![
            (0, "seq-retry/wal-1".to_string()),
            (5, "seq-retry/wal-2".to_string())
        ]
    );
}

pub async fn a_wal_object_older_than_the_commit_window_is_stale(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "seq-stale").await;
    let s = stream(meta, ns, "events", 1).await;
    stamp(meta).await;
    let old = meta.now_ms() - WAL_COMMIT_WINDOW_MS - 60_000;
    let stale = meta
        .commit_wal(WalCommit {
            object: "seq-stale/wal-old".to_string(),
            created_at_ms: old,
            chunks: vec![chunk(s, 0, 1, 0..10)],
        })
        .await;
    assert!(!stale.earlier_unknown);
    assert_eq!(
        rejected(stale.result),
        ApplyError::StaleCommit {
            object: "seq-stale/wal-old".to_string()
        }
    );
    assert_eq!(high_watermark(db.last(), s, 0).await, 0);
    // A recent object commits.
    let recent = meta.now_ms() - WAL_COMMIT_WINDOW_MS / 2;
    let fresh = meta
        .commit_wal(WalCommit {
            object: "seq-stale/wal-recent".to_string(),
            created_at_ms: recent,
            chunks: vec![chunk(s, 0, 1, 0..10)],
        })
        .await
        .into_result()
        .expect("commit");
    assert_eq!(fresh, vec![0]);
}

pub async fn partition_index_pages_by_bytes(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "seq-pages").await;
    let s = stream(meta, ns, "events", 2).await;
    for i in 0..5 {
        commit(
            meta,
            &format!("seq-pages/wal-{i}"),
            vec![chunk(s, 0, 2, 0..100)],
        )
        .await;
    }
    let reader = db.last();
    let page = |from: u64, max: Option<u64>| async move {
        let index = reader
            .partition_index(L, s, 0, from, max)
            .await
            .expect("read")
            .expect("partition");
        assert_eq!(index.log_start_offset(), 0);
        assert_eq!(index.next_offset(), 10);
        assert_eq!(index.high_watermark(), 10);
        assert_eq!(index.bytes(), 500);
        index
            .into_entries()
            .into_iter()
            .map(|e| e.base_offset)
            .collect::<Vec<_>>()
    };
    assert_eq!(page(0, None).await, vec![0, 2, 4, 6, 8]);
    // It stops once the entries reach the budget.
    assert_eq!(page(0, Some(250)).await, vec![0, 2, 4]);
    assert_eq!(page(0, Some(200)).await, vec![0, 2]);
    // At least one entry, whatever the budget.
    assert_eq!(page(0, Some(0)).await, vec![0]);
    // From the entry holding the offset.
    assert_eq!(page(3, Some(250)).await, vec![2, 4, 6]);
    assert_eq!(page(9, None).await, vec![8]);
    assert_eq!(page(10, None).await, Vec::<u64>::new());

    let entry = db
        .last()
        .partition_index(L, s, 0, 4, Some(1))
        .await
        .expect("read")
        .expect("partition")
        .into_entries();
    assert_eq!(
        entry,
        vec![IndexEntry {
            kind: EntryKind::Wal,
            base_offset: 4,
            records: 2,
            object: "seq-pages/wal-2".to_string(),
            byte_range: 0..100,
            max_timestamp_ms: 0,
        }]
    );
    let empty = meta
        .partition_index(L, s, 1, 0, None)
        .await
        .expect("read")
        .expect("partition");
    assert_eq!(empty.high_watermark(), 0);
    assert_eq!(empty.entries().count(), 0);
    assert_eq!(
        meta.partition_index(L, s, 2, 0, None).await.expect("read"),
        None
    );
    assert_eq!(
        meta.partition_index(L, StreamId(s.0 + 1000), 0, 0, None)
            .await
            .expect("read"),
        None
    );
}

pub async fn swap_segment_replaces_wal_entries_and_retires_objects(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "seq-swap").await;
    let s = stream(meta, ns, "events", 1).await;
    commit(meta, "seq-swap/wal-1", vec![chunk(s, 0, 2, 0..20)]).await;
    commit(meta, "seq-swap/wal-2", vec![chunk(s, 0, 3, 0..30)]).await;
    commit(meta, "seq-swap/wal-3", vec![chunk(s, 0, 1, 0..10)]).await;
    let request = swap(
        s,
        &[(0, "seq-swap/wal-1"), (2, "seq-swap/wal-2")],
        "seq-swap/seg-1",
        fresh_now(meta.now_ms()),
    );
    let swapped = meta.swap_segment(request.clone()).await;
    assert!(!swapped.earlier_unknown);
    swapped.result.expect("swap");
    let entries = db
        .last()
        .partition_index(L, s, 0, 0, None)
        .await
        .expect("read")
        .expect("partition")
        .into_entries();
    assert_eq!(
        entries[0],
        IndexEntry {
            kind: EntryKind::Segment,
            base_offset: 0,
            records: 5,
            object: "seq-swap/seg-1".to_string(),
            byte_range: 0..500,
            max_timestamp_ms: 7,
        }
    );
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[1].object, "seq-swap/wal-3");
    assert_eq!(
        retired(db.last()).await,
        vec!["seq-swap/wal-1".to_string(), "seq-swap/wal-2".to_string()]
    );
    assert!(
        db.last()
            .segment_referenced(s, 0, "seq-swap/seg-1")
            .await
            .expect("read")
    );
    // A retry finds the segment in place and changes nothing.
    db.last()
        .swap_segment(request)
        .await
        .into_result()
        .expect("retry");
    assert_eq!(high_watermark(db.last(), s, 0).await, 6);
    assert_eq!(objects(db.last(), s).await.len(), 2);
}

pub async fn swap_segment_against_a_moved_index_is_index_mismatch(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "seq-mismatch").await;
    let s = stream(meta, ns, "events", 1).await;
    commit(meta, "seq-mismatch/wal-1", vec![chunk(s, 0, 2, 0..20)]).await;
    commit(meta, "seq-mismatch/wal-2", vec![chunk(s, 0, 3, 0..30)]).await;
    let now = meta.now_ms();
    let mismatch = ApplyError::IndexMismatch {
        stream: s,
        partition: 0,
    };
    for replaces in [
        vec![(0, "seq-mismatch/wal-1"), (2, "seq-mismatch/other")],
        vec![(0, "seq-mismatch/wal-1"), (3, "seq-mismatch/wal-2")],
        vec![(2, "seq-mismatch/wal-2"), (0, "seq-mismatch/wal-1")],
        vec![(7, "seq-mismatch/wal-9")],
    ] {
        let result = meta
            .swap_segment(swap(s, &replaces, "seq-mismatch/seg", fresh_now(now)))
            .await;
        assert!(!result.earlier_unknown);
        assert_eq!(rejected(result.result), mismatch, "{replaces:?}");
    }
    // A segmented entry is no longer a WAL entry of its object.
    meta.swap_segment(swap(
        s,
        &[(0, "seq-mismatch/wal-1")],
        "seq-mismatch/seg-a",
        fresh_now(now),
    ))
    .await
    .into_result()
    .expect("swap");
    let again = meta
        .swap_segment(swap(
            s,
            &[(0, "seq-mismatch/wal-1"), (2, "seq-mismatch/wal-2")],
            "seq-mismatch/seg-b",
            fresh_now(now),
        ))
        .await;
    assert_eq!(rejected(again.into_result()), mismatch);
    // A trim moves entries too.
    meta.trim_partition(s, 0, 5, None).await.expect("trim");
    let trimmed = meta
        .swap_segment(swap(
            s,
            &[(2, "seq-mismatch/wal-2")],
            "seq-mismatch/seg-c",
            fresh_now(now),
        ))
        .await;
    assert_eq!(rejected(trimmed.into_result()), mismatch);
    let missing = StreamId(s.0 + 1000);
    let unknown = meta
        .swap_segment(swap(
            missing,
            &[(0, "seq-mismatch/wal-1")],
            "seq-mismatch/seg-d",
            fresh_now(now),
        ))
        .await;
    assert_eq!(
        rejected(unknown.into_result()),
        ApplyError::StreamNotFound(missing)
    );
}

pub async fn a_segment_past_its_freshness_is_stale_object(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "seq-fresh").await;
    let s = stream(meta, ns, "events", 1).await;
    commit(meta, "seq-fresh/wal-1", vec![chunk(s, 0, 2, 0..20)]).await;
    stamp(meta).await;
    let stale = Freshness {
        created_at_ms: 1,
        max_age_ms: 1,
    };
    let result = meta
        .swap_segment(swap(s, &[(0, "seq-fresh/wal-1")], "seq-fresh/seg", stale))
        .await;
    assert!(!result.earlier_unknown);
    match rejected(result.result) {
        ApplyError::StaleObject {
            object,
            created_at_ms: 1,
            max_age_ms: 1,
            clock_ms,
        } => {
            assert_eq!(object, "seq-fresh/seg");
            assert!(clock_ms > 2, "clock {clock_ms}");
        }
        other => panic!("expected StaleObject, got {other:?}"),
    }
    // Nothing changed: the WAL entry is still there, and the segment is not
    // referenced.
    assert_eq!(
        objects(db.last(), s).await,
        vec![(0, "seq-fresh/wal-1".to_string())]
    );
    assert!(
        !db.last()
            .segment_referenced(s, 0, "seq-fresh/seg")
            .await
            .expect("read")
    );
    assert!(retired(db.last()).await.is_empty());
}

pub async fn trim_moves_the_log_start_and_retires_whole_entries(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "seq-trim").await;
    let s = stream(meta, ns, "events", 1).await;
    commit(meta, "seq-trim/wal-1", vec![chunk(s, 0, 2, 0..20)]).await;
    commit(meta, "seq-trim/wal-2", vec![chunk(s, 0, 3, 0..30)]).await;
    commit(meta, "seq-trim/wal-3", vec![chunk(s, 0, 1, 0..10)]).await;
    assert_eq!(meta.trim_partition(s, 0, 3, None).await.expect("trim"), 3);
    let index = db
        .last()
        .partition_index(L, s, 0, 0, None)
        .await
        .expect("read")
        .expect("partition");
    assert_eq!(index.log_start_offset(), 3);
    assert_eq!(index.high_watermark(), 6);
    assert_eq!(index.bytes(), 40);
    // The entry holding offset 3 stays whole; the one wholly below goes.
    assert_eq!(
        index
            .into_entries()
            .into_iter()
            .map(|e| e.object)
            .collect::<Vec<_>>(),
        vec!["seq-trim/wal-2".to_string(), "seq-trim/wal-3".to_string()]
    );
    assert_eq!(retired(db.last()).await, vec!["seq-trim/wal-1".to_string()]);
    // The log start only moves forward.
    assert_eq!(
        db.last().trim_partition(s, 0, 2, None).await.expect("trim"),
        3
    );
    // Capped at the high watermark.
    assert_eq!(meta.trim_partition(s, 0, 100, None).await.expect("trim"), 6);
    assert!(objects(db.last(), s).await.is_empty());
    assert_eq!(
        retired(db.last()).await,
        vec![
            "seq-trim/wal-1".to_string(),
            "seq-trim/wal-2".to_string(),
            "seq-trim/wal-3".to_string()
        ]
    );
    let missing = StreamId(s.0 + 1000);
    assert_eq!(
        rejected(meta.trim_partition(missing, 0, 1, None).await),
        ApplyError::StreamNotFound(missing)
    );
    assert_eq!(
        rejected(meta.trim_partition(s, 1, 1, None).await),
        ApplyError::PartitionNotFound {
            stream: s,
            partition: 1
        }
    );
}
