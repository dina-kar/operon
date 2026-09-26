//! Garbage collection reads (always `Linearizable`, against the metastore
//! clock of the same state) and the bookkeeping writes.

use std::time::Duration;

use operon_common::StreamId;
use operon_common::meta::{
    ApplyError, CollectionRoots, Fence, Freshness, MetaStore, Pointer, SegmentSwap,
    collection_pk_prefix, collection_pointer_key, collection_prefix,
};

use super::{cas, chunk, commit, namespace, rejected, retired, schema, stamp, stream};
use crate::Backend;

/// Well past any `min_age_ms` these cases use, by any clock.
const OLD: u64 = 1;
const MIN_AGE_MS: u64 = 60_000;

/// Commits `object` (two records) to partition 0 and trims it away, so it is
/// retired.
async fn retire(meta: &dyn MetaStore, s: StreamId, object: &str) {
    commit(meta, object, vec![chunk(s, 0, 2, 0..20)]).await;
    meta.trim_partition(s, 0, u64::MAX, None)
        .await
        .expect("trim");
}

async fn swap_into(meta: &dyn MetaStore, s: StreamId, base: u64, wal: &str, segment: &str) {
    meta.swap_segment(SegmentSwap {
        stream: s,
        partition: 0,
        replaces: vec![(base, wal.to_string())],
        segment: segment.to_string(),
        byte_range: 0..100,
        max_timestamp_ms: 0,
        fence: None,
        fresh: Freshness {
            created_at_ms: meta.now_ms(),
            max_age_ms: 60_000,
        },
    })
    .await
    .into_result()
    .expect("swap");
}

pub async fn retired_expired_respects_grace(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "gc-grace").await;
    let s = stream(meta, ns, "events", 1).await;
    retire(meta, s, "gc-grace/wal-b").await;
    retire(meta, s, "gc-grace/wal-a").await;
    let reader = db.last();
    // In path order, once their grace is over.
    assert_eq!(
        reader.retired_expired(0).await.expect("read"),
        vec!["gc-grace/wal-a".to_string(), "gc-grace/wal-b".to_string()]
    );
    // Retired moments ago: an hour's grace is not over.
    assert_eq!(
        reader.retired_expired(3_600_000).await.expect("read"),
        Vec::<String>::new()
    );
    assert_eq!(
        reader.retired_expired(u64::MAX).await.expect("read"),
        Vec::<String>::new()
    );
    // Retirement is stamped with the metastore clock, which later writes
    // move on.
    tokio::time::sleep(Duration::from_millis(50)).await;
    stamp(meta).await;
    assert_eq!(reader.retired_expired(20).await.expect("read").len(), 2);
}

pub async fn forget_objects_removes_retired_entries(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "gc-forget").await;
    let s = stream(meta, ns, "events", 1).await;
    retire(meta, s, "gc-forget/wal-1").await;
    retire(meta, s, "gc-forget/wal-2").await;
    let forgotten = db
        .last()
        .forget_objects(
            vec![
                "gc-forget/wal-1".to_string(),
                "gc-forget/unknown".to_string(),
            ],
            None,
        )
        .await
        .expect("forget");
    assert_eq!(forgotten, 1);
    assert_eq!(retired(meta).await, vec!["gc-forget/wal-2".to_string()]);
    // A retry forgets nothing more.
    assert_eq!(
        meta.forget_objects(vec!["gc-forget/wal-1".to_string()], None)
            .await
            .expect("forget"),
        0
    );
    let lease = "gc-forget/lease";
    let grant = meta
        .acquire_lease(lease, "gc", Duration::from_secs(30))
        .await
        .expect("acquire");
    let fence = |epoch| {
        Some(Fence {
            lease: lease.to_string(),
            epoch,
        })
    };
    assert_eq!(
        rejected(
            meta.forget_objects(vec!["gc-forget/wal-2".to_string()], fence(grant.epoch + 1))
                .await
        ),
        ApplyError::Fenced {
            lease: lease.to_string()
        }
    );
    assert_eq!(retired(meta).await, vec!["gc-forget/wal-2".to_string()]);
    assert_eq!(
        meta.forget_objects(vec!["gc-forget/wal-2".to_string()], fence(grant.epoch))
            .await
            .expect("forget"),
        1
    );
    assert!(retired(db.last()).await.is_empty());
}

pub async fn orphan_wal_objects_skips_live_retired_and_young(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "gc-orphan-wal").await;
    let s = stream(meta, ns, "events", 1).await;
    retire(meta, s, "gc-orphan-wal/retired").await;
    commit(meta, "gc-orphan-wal/live", vec![chunk(s, 0, 1, 0..10)]).await;
    // Taken before the stamp, so the metastore clock is not behind it.
    let now = meta.now_ms();
    stamp(meta).await;
    let candidates = vec![
        ("gc-orphan-wal/live".to_string(), OLD),
        ("gc-orphan-wal/retired".to_string(), OLD),
        ("gc-orphan-wal/young".to_string(), now),
        ("gc-orphan-wal/orphan-1".to_string(), OLD),
        ("gc-orphan-wal/orphan-2".to_string(), OLD),
    ];
    let reader = db.last();
    assert_eq!(
        reader
            .orphan_wal_objects(candidates.clone(), MIN_AGE_MS, 10)
            .await
            .expect("read"),
        vec![
            "gc-orphan-wal/orphan-1".to_string(),
            "gc-orphan-wal/orphan-2".to_string()
        ]
    );
    // At most `limit`, in input order.
    assert_eq!(
        reader
            .orphan_wal_objects(candidates.clone(), MIN_AGE_MS, 1)
            .await
            .expect("read"),
        vec!["gc-orphan-wal/orphan-1".to_string()]
    );
    // With no minimum age, the young one qualifies too.
    assert_eq!(
        reader
            .orphan_wal_objects(candidates, 0, 10)
            .await
            .expect("read"),
        vec![
            "gc-orphan-wal/young".to_string(),
            "gc-orphan-wal/orphan-1".to_string(),
            "gc-orphan-wal/orphan-2".to_string()
        ]
    );
}

pub async fn orphan_segments_skips_indexed_retired_and_young(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "gc-orphan-seg").await;
    let other = namespace(meta, "gc-orphan-seg-other").await;
    let s = stream(meta, ns, "events", 1).await;
    commit(meta, "gc-orphan-seg/wal-1", vec![chunk(s, 0, 2, 0..20)]).await;
    swap_into(meta, s, 0, "gc-orphan-seg/wal-1", "gc-orphan-seg/seg-old").await;
    meta.trim_partition(s, 0, 2, None).await.expect("trim");
    commit(meta, "gc-orphan-seg/wal-2", vec![chunk(s, 0, 2, 0..20)]).await;
    swap_into(meta, s, 2, "gc-orphan-seg/wal-2", "gc-orphan-seg/seg-live").await;
    commit(meta, "gc-orphan-seg/wal-3", vec![chunk(s, 0, 1, 0..10)]).await;
    // Taken before the stamp, so the metastore clock is not behind it.
    let now = meta.now_ms();
    stamp(meta).await;
    let candidates = vec![
        ("gc-orphan-seg/seg-live".to_string(), OLD),
        ("gc-orphan-seg/seg-old".to_string(), OLD),
        ("gc-orphan-seg/wal-3".to_string(), OLD),
        ("gc-orphan-seg/seg-young".to_string(), now),
        ("gc-orphan-seg/seg-orphan-1".to_string(), OLD),
        ("gc-orphan-seg/seg-orphan-2".to_string(), OLD),
    ];
    let reader = db.last();
    assert_eq!(
        reader
            .orphan_segments(ns, candidates.clone(), MIN_AGE_MS, 10)
            .await
            .expect("read"),
        vec![
            "gc-orphan-seg/seg-orphan-1".to_string(),
            "gc-orphan-seg/seg-orphan-2".to_string()
        ]
    );
    assert_eq!(
        reader
            .orphan_segments(ns, candidates.clone(), MIN_AGE_MS, 1)
            .await
            .expect("read"),
        vec!["gc-orphan-seg/seg-orphan-1".to_string()]
    );
    // Index entries count only in their own namespace; retired objects
    // everywhere.
    assert_eq!(
        reader
            .orphan_segments(other, candidates, MIN_AGE_MS, 10)
            .await
            .expect("read"),
        vec![
            "gc-orphan-seg/seg-live".to_string(),
            "gc-orphan-seg/wal-3".to_string(),
            "gc-orphan-seg/seg-orphan-1".to_string(),
            "gc-orphan-seg/seg-orphan-2".to_string()
        ]
    );
}

pub async fn segment_referenced_sees_the_index_and_retired_set(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "gc-referenced").await;
    let s = stream(meta, ns, "events", 2).await;
    commit(meta, "gc-referenced/wal-1", vec![chunk(s, 0, 2, 0..20)]).await;
    swap_into(meta, s, 0, "gc-referenced/wal-1", "gc-referenced/seg-1").await;
    commit(meta, "gc-referenced/wal-2", vec![chunk(s, 0, 2, 0..20)]).await;
    swap_into(meta, s, 2, "gc-referenced/wal-2", "gc-referenced/seg-2").await;
    meta.trim_partition(s, 0, 2, None).await.expect("trim");
    let reader = db.last();
    let referenced = |partition: u32, object: &'static str| async move {
        reader
            .segment_referenced(s, partition, object)
            .await
            .expect("read")
    };
    // Indexed.
    assert!(referenced(0, "gc-referenced/seg-2").await);
    // Retired (trimmed away), in any partition.
    assert!(referenced(0, "gc-referenced/seg-1").await);
    assert!(referenced(1, "gc-referenced/seg-1").await);
    // The replaced WAL objects are retired as well.
    assert!(referenced(0, "gc-referenced/wal-1").await);
    // Indexed only in partition 0.
    assert!(!referenced(1, "gc-referenced/seg-2").await);
    assert!(!referenced(0, "gc-referenced/seg-unknown").await);
    assert!(
        !reader
            .segment_referenced(StreamId(s.0 + 1000), 0, "gc-referenced/seg-2")
            .await
            .expect("read")
    );
}

pub async fn collection_roots_lists_prefixes_under_a_path(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "gc-roots").await;
    let (c1, _, _) = meta
        .create_collection(ns, "docs-1", schema(), 1)
        .await
        .expect("create");
    let (c2, _, _) = meta
        .create_collection(ns, "docs-2", schema(), 1)
        .await
        .expect("create");
    let (c3, _, _) = meta
        .create_collection(ns, "docs-3", schema(), 1)
        .await
        .expect("create");
    meta.cas_pointer(cas(ns, &collection_pointer_key(c1), None, "gc-roots/m-1"))
        .await
        .into_result()
        .expect("cas");
    meta.drop_collection(ns, "docs-3").await.expect("drop");
    // A retired object that is not a prefix, under the same path.
    let s = stream(meta, ns, "events", 1).await;
    let under = format!("ns/{ns}/collections/");
    retire(meta, s, &format!("{under}stray.bin")).await;
    let reader = db.last();
    let clock = reader
        .clock_ms(operon_common::meta::Consistency::Linearizable)
        .await
        .expect("clock");
    let CollectionRoots {
        clock_ms,
        collections,
        retired_prefixes,
    } = reader.collection_roots(ns, &under).await.expect("read");
    assert_eq!(clock_ms, clock);
    let ids: Vec<_> = collections
        .iter()
        .map(|(c, pointer)| (c.id, pointer.clone()))
        .collect();
    assert_eq!(
        ids,
        vec![
            (
                c1,
                Some(Pointer {
                    version: 1,
                    value: "gc-roots/m-1".to_string(),
                })
            ),
            (c2, None)
        ]
    );
    assert_eq!(retired_prefixes, vec![collection_prefix(ns, c3)]);
    let pk = reader
        .collection_roots(ns, &format!("ns/{ns}/pk/"))
        .await
        .expect("read");
    assert_eq!(pk.retired_prefixes, vec![collection_pk_prefix(ns, c3)]);
    assert_eq!(pk.collections.len(), 2);
    // Another namespace's roots are empty.
    let other = namespace(meta, "gc-roots-other").await;
    let empty = reader
        .collection_roots(other, &format!("ns/{other}/"))
        .await
        .expect("read");
    assert!(empty.collections.is_empty());
    assert!(empty.retired_prefixes.is_empty());
}

pub async fn prune_wal_commits_is_fenced(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "gc-prune").await;
    let s = stream(meta, ns, "events", 1).await;
    commit(meta, "gc-prune/wal-1", vec![chunk(s, 0, 1, 0..10)]).await;
    let lease = "gc-prune/lease";
    let grant = meta
        .acquire_lease(lease, "gc", Duration::from_secs(30))
        .await
        .expect("acquire");
    let fence = |epoch| {
        Some(Fence {
            lease: lease.to_string(),
            epoch,
        })
    };
    let refusal = ApplyError::Fenced {
        lease: lease.to_string(),
    };
    assert_eq!(
        rejected(meta.prune_wal_commits(fence(grant.epoch + 1)).await),
        refusal
    );
    // A recent commit record is kept.
    assert_eq!(
        db.last()
            .prune_wal_commits(fence(grant.epoch))
            .await
            .expect("prune"),
        0
    );
    assert_eq!(meta.prune_wal_commits(None).await.expect("prune"), 0);
    // So a retried commit still finds it.
    assert_eq!(
        commit(meta, "gc-prune/wal-1", vec![chunk(s, 0, 1, 0..10)]).await,
        vec![0]
    );
    meta.release_lease(lease, "gc", grant.epoch)
        .await
        .expect("release");
    assert_eq!(
        rejected(meta.prune_wal_commits(fence(grant.epoch)).await),
        refusal
    );
}
