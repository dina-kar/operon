//! The `MetaStore` conformance suite against the TiKV metastore (R1 plan
//! Tasks 4–5): every case with their linearizability histories, plus the
//! TiKV-specific tests of the clock, the id blocks and `commit_wal`'s
//! partition groups. Every test needs a cluster and skips without
//! `LOAMS_TEST_PD`.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use loams_common::StreamId;
use loams_common::meta::{
    ApplyError, Consistency, MetaError, MetaStore, PointerCas, Retention, WalChunk, WalClass,
    WalCommit, link_pointer_key,
};
use loams_common::schema::{CollectionSchema, DynamicMapping};
use loams_meta_conformance::{Backend, Faults, Instance};
use loams_meta_tikv::{MAX_GROUP_CHUNKS, TikvMeta};
use loams_tikv::testing::{self, TEST_META, TestCluster};
use loams_tikv::{Fault, FaultPlan, FaultPoint, Tikv, TikvConfig};

/// Handles per case: three, as for a three-node backend.
const HANDLES: usize = 3;

/// One fresh metastore per case: a random root in `loams_test_meta`, three
/// `TikvMeta` handles on it (each with its own `tikv-client`), and a fault
/// adapter.
struct TikvBackend;

#[async_trait]
impl Backend for TikvBackend {
    async fn start(&self) -> Instance {
        let cluster = testing::cluster()
            .await
            .expect("unavailable() said a cluster is configured");
        let (clients, plans) =
            open_handles(&cluster, HANDLES, loams_meta_tikv::DEFAULT_ID_BLOCK).await;
        Instance {
            clients: clients
                .into_iter()
                .map(|m| Arc::new(m) as Arc<dyn MetaStore>)
                .collect(),
            faults: Some(Arc::new(LoseAcks(plans))),
            guard: Box::new(()),
        }
    }

    fn unavailable(&self) -> Option<String> {
        std::env::var(testing::PD_ENV)
            .map_or(true, |v| v.trim().is_empty())
            .then(|| testing::PD_ENV.to_string())
    }
}

/// `count` handles on one new random root, each with its own lost-ack plan.
async fn open_handles(
    cluster: &TestCluster,
    count: usize,
    id_block: u64,
) -> (Vec<TikvMeta>, Vec<Arc<LoseAck>>) {
    let config = cluster.config(TEST_META);
    let mut metas = Vec::new();
    let mut plans = Vec::new();
    for _ in 0..count {
        let plan = Arc::new(LoseAck::default());
        let tikv = Tikv::connect(config.clone())
            .await
            .expect("connect to the test keyspace")
            .with_faults(plan.clone());
        let meta = TikvMeta::open_on(tikv, id_block, loams_meta_tikv::DEFAULT_POLL)
            .await
            .expect("open the metastore");
        metas.push(meta);
        plans.push(plan);
    }
    (metas, plans)
}

/// A one-shot lost acknowledgement: the next commit of the handle succeeds,
/// then reports an unknown outcome, which the runner resolves through the
/// commit token (row R3).
#[derive(Default)]
struct LoseAck {
    armed: AtomicBool,
}

impl FaultPlan for LoseAck {
    fn at(&self, _op: &str, point: FaultPoint, _attempt: u32) -> Option<Fault> {
        (point == FaultPoint::AfterCommit && self.armed.swap(false, Ordering::SeqCst))
            .then_some(Fault::LoseAck)
    }
}

/// The suite's fault hooks: `lose_next_ack` arms a handle's [`LoseAck`];
/// `disturb` and `heal` do nothing until Task 6's nemesis.
struct LoseAcks(Vec<Arc<LoseAck>>);

#[async_trait]
impl Faults for LoseAcks {
    fn lose_next_ack(&self, client: usize) {
        self.0[client].armed.store(true, Ordering::SeqCst);
    }

    async fn disturb(&self, _seed: u64) {}

    async fn heal(&self) {}
}

mod suite {
    // Every case, linearizability histories included (row R3).
    loams_meta_conformance::metastore_conformance!(super::TikvBackend);
}

/// `clock_ms` is the physical part of a fresh TSO timestamp: between two TSO
/// fetches around it, and never behind `now_ms`'s anchor.
#[tokio::test]
async fn clock_is_tso_physical() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let (metas, _) = open_handles(&cluster, 1, 10).await;
    let meta = &metas[0];
    let tikv = meta.tikv();
    for _ in 0..20 {
        let before = Tikv::physical_ms(&tikv.now().await.expect("tso"));
        let clock = meta
            .clock_ms(Consistency::Linearizable)
            .await
            .expect("clock");
        let after = Tikv::physical_ms(&tikv.now().await.expect("tso"));
        assert!(
            before <= clock && clock <= after,
            "{before} <= {clock} <= {after}"
        );
        // now_ms never trails the TSO this handle has seen.
        assert!(meta.now_ms() >= after, "now_ms {} < {after}", meta.now_ms());
    }
    // now_ms is monotonic and keeps up with wall time between fetches.
    let mut last = meta.now_ms();
    for _ in 0..100 {
        let now = meta.now_ms();
        assert!(now >= last);
        last = now;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(meta.now_ms() >= last + 50);
}

/// Ids come from per-handle blocks: two handles interleave from different
/// blocks, a handle that stops leaves the rest of its block unused, and no
/// id is ever handed out twice, also under concurrent creates.
#[tokio::test]
async fn id_blocks_leave_gaps_but_never_repeat() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    const BLOCK: u64 = 4;
    let config = cluster.config(TEST_META);
    let open = || async {
        let tikv = Tikv::connect(config.clone()).await.expect("connect");
        TikvMeta::open_on(tikv, BLOCK, loams_meta_tikv::DEFAULT_POLL)
            .await
            .expect("open")
    };
    let a = open().await;
    let b = open().await;
    let mut ids = Vec::new();
    for i in 0..6 {
        ids.push(
            a.create_namespace(&format!("ids-a-{i}"))
                .await
                .expect("a")
                .0,
        );
        ids.push(
            b.create_namespace(&format!("ids-b-{i}"))
                .await
                .expect("b")
                .0,
        );
    }
    // `a` took block 1..5 then 9..13; `b` took 5..9 then 13..17.
    let from_a: Vec<u64> = ids.iter().step_by(2).copied().collect();
    let from_b: Vec<u64> = ids.iter().skip(1).step_by(2).copied().collect();
    assert_eq!(from_a, [1, 2, 3, 4, 9, 10]);
    assert_eq!(from_b, [5, 6, 7, 8, 13, 14]);
    // A new handle takes a fresh block: 11, 12 and 15, 16 stay unused.
    drop(a);
    let c = open().await;
    ids.push(c.create_namespace("ids-c-0").await.expect("c").0);
    assert_eq!(ids.last(), Some(&17));

    // Concurrent creates through fresh handles never repeat an id.
    let mut tasks = Vec::new();
    for h in 0..4 {
        let meta = open().await;
        tasks.push(tokio::spawn(async move {
            let mut got = Vec::new();
            for i in 0..10 {
                got.push(
                    meta.create_namespace(&format!("ids-par-{h}-{i}"))
                        .await
                        .expect("create")
                        .0,
                );
            }
            got
        }));
    }
    for task in tasks {
        ids.extend(task.await.expect("join"));
    }
    let unique: BTreeSet<u64> = ids.iter().copied().collect();
    assert_eq!(unique.len(), ids.len(), "an id repeated: {ids:?}");
    // The namespaces list agrees, in id order.
    let listed: Vec<u64> = c
        .namespaces(Consistency::Linearizable)
        .await
        .expect("list")
        .into_iter()
        .map(|n| n.id.0)
        .collect();
    assert_eq!(listed, unique.into_iter().collect::<Vec<_>>());
}

// ---- commit_wal's partition groups (R1 plan Task 5) ----

/// A fault plan driven by a closure, for the tests below.
struct Script<F>(F);

impl<F> FaultPlan for Script<F>
where
    F: Fn(&str, FaultPoint, u32) -> Option<Fault> + Send + Sync,
{
    fn at(&self, op: &str, point: FaultPoint, attempt: u32) -> Option<Fault> {
        (self.0)(op, point, attempt)
    }
}

/// One handle on `config`'s root, with `plan`.
async fn open_with(config: &TikvConfig, plan: Arc<dyn FaultPlan>) -> TikvMeta {
    let tikv = Tikv::connect(config.clone())
        .await
        .expect("connect to the test keyspace")
        .with_faults(plan);
    TikvMeta::open_on(
        tikv,
        loams_meta_tikv::DEFAULT_ID_BLOCK,
        loams_meta_tikv::DEFAULT_POLL,
    )
    .await
    .expect("open the metastore")
}

const L: Consistency = Consistency::Linearizable;
const COMMIT: &str = "meta.commit_wal";

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

async fn setup(meta: &TikvMeta, ns: &str, streams: &[(&str, u32)]) -> Vec<StreamId> {
    let ns = meta.create_namespace(ns).await.expect("namespace");
    let mut ids = Vec::new();
    for (name, partitions) in streams {
        ids.push(
            meta.create_stream(
                ns,
                name,
                *partitions,
                WalClass::Standard,
                Retention::default(),
            )
            .await
            .expect("stream"),
        );
    }
    ids
}

/// The high watermark of every partition of `stream`.
async fn watermarks(meta: &TikvMeta, stream: StreamId) -> Vec<u64> {
    meta.stream_state(L, stream)
        .await
        .expect("read")
        .expect("stream")
        .partitions
        .into_iter()
        .map(|b| b.expect("bounds").high_watermark)
        .collect()
}

/// One group: a commit refused on every attempt leaves nothing in any
/// partition; one whose acknowledgement is lost lands in all of them, once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_wal_is_atomic_across_partitions() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    // 0: no fault; 1: refuse every commit; 2: lose the next acknowledgement.
    let mode = Arc::new(std::sync::atomic::AtomicU8::new(0));
    let plan = {
        let mode = mode.clone();
        Script(move |op: &str, point, _| {
            if op != COMMIT {
                return None;
            }
            match (mode.load(Ordering::SeqCst), point) {
                (1, FaultPoint::BeforeCommit) => Some(Fault::Refuse),
                (2, FaultPoint::AfterCommit) => {
                    mode.store(0, Ordering::SeqCst);
                    Some(Fault::LoseAck)
                }
                _ => None,
            }
        })
    };
    let meta = open_with(&cluster.config(TEST_META), Arc::new(plan)).await;
    let streams = setup(&meta, "atomic", &[("a", 4), ("b", 4), ("c", 4)]).await;
    let chunks: Vec<WalChunk> = streams
        .iter()
        .flat_map(|&s| (0..4).map(move |p| one_record(s, p)))
        .collect();

    mode.store(1, Ordering::SeqCst);
    let crashed = meta
        .commit_wal(wal(&meta, "atomic/wal-1", chunks.clone()))
        .await;
    assert!(
        matches!(
            crashed.result,
            Err(MetaError::Timeout | MetaError::Unavailable(_))
        ),
        "{:?}",
        crashed.result
    );
    for &s in &streams {
        assert_eq!(watermarks(&meta, s).await, vec![0; 4], "nothing applied");
    }

    mode.store(2, Ordering::SeqCst);
    let lost = meta
        .commit_wal(wal(&meta, "atomic/wal-1", chunks.clone()))
        .await;
    assert!(lost.earlier_unknown);
    assert_eq!(lost.result.expect("commit"), vec![0; 12]);
    for &s in &streams {
        assert_eq!(watermarks(&meta, s).await, vec![1; 4], "applied once");
    }
    // The retry returns the same offsets and changes nothing.
    let again = meta.commit_wal(wal(&meta, "atomic/wal-1", chunks)).await;
    assert!(!again.earlier_unknown);
    assert_eq!(again.result.expect("retry"), vec![0; 12]);
    for &s in &streams {
        assert_eq!(watermarks(&meta, s).await, vec![1; 4]);
    }
}

/// A call over two groups whose first group's acknowledgement is lost and
/// whose second group is refused fails, with the first group committed; the
/// retry commits only the second group, and no offset is assigned twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_commit_wal_commits_per_group_and_retry_completes() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    // 0: no fault; 1: lose the next acknowledgement, then refuse every
    // commit until disarmed.
    let mode = Arc::new(std::sync::atomic::AtomicU8::new(0));
    let plan = {
        let mode = mode.clone();
        Script(move |op: &str, point, _| {
            if op != COMMIT {
                return None;
            }
            match (mode.load(Ordering::SeqCst), point) {
                (1, FaultPoint::AfterCommit) => {
                    mode.store(2, Ordering::SeqCst);
                    Some(Fault::LoseAck)
                }
                (2, FaultPoint::BeforeBegin) => Some(Fault::Refuse),
                _ => None,
            }
        })
    };
    let meta = open_with(&cluster.config(TEST_META), Arc::new(plan)).await;
    let streams = setup(&meta, "groups", &[("a", 4), ("b", 4)]).await;
    // 600 chunks of each stream: 1 200 > 1 024, so two groups, one stream
    // each.
    const PER_STREAM: u32 = 600;
    let chunks: Vec<WalChunk> = streams
        .iter()
        .flat_map(|&s| (0..PER_STREAM).map(move |i| one_record(s, i % 4)))
        .collect();
    assert!(chunks.len() > MAX_GROUP_CHUNKS);
    let expected: Vec<u64> = (0..2)
        .flat_map(|_| (0..u64::from(PER_STREAM)).map(|i| i / 4))
        .collect();

    mode.store(1, Ordering::SeqCst);
    let failed = meta
        .commit_wal(wal(&meta, "groups/wal-1", chunks.clone()))
        .await;
    assert!(failed.earlier_unknown, "the lost first group is flagged");
    assert!(failed.result.is_err(), "{:?}", failed.result);
    let per_partition = u64::from(PER_STREAM / 4);
    assert_eq!(watermarks(&meta, streams[0]).await, vec![per_partition; 4]);
    assert_eq!(watermarks(&meta, streams[1]).await, vec![0; 4]);

    mode.store(0, Ordering::SeqCst);
    let retried = meta
        .commit_wal(wal(&meta, "groups/wal-1", chunks.clone()))
        .await;
    assert!(!retried.earlier_unknown);
    assert_eq!(retried.result.expect("retry"), expected);
    // The first group was not committed again.
    assert_eq!(watermarks(&meta, streams[0]).await, vec![per_partition; 4]);
    assert_eq!(watermarks(&meta, streams[1]).await, vec![per_partition; 4]);
    let index = meta
        .partition_index(L, streams[0], 0, 0, None)
        .await
        .expect("read")
        .expect("partition")
        .into_entries();
    let bases: Vec<u64> = index.iter().map(|e| e.base_offset).collect();
    assert_eq!(bases, (0..per_partition).collect::<Vec<_>>(), "dense");
    // Once complete, the call returns the same offsets from its records.
    let again = meta.commit_wal(wal(&meta, "groups/wal-1", chunks)).await;
    assert_eq!(again.result.expect("again"), expected);
}

/// One stream's chunks never span groups: a stream of exactly one group's
/// chunks plus another stream commits as two groups, and a stream over one
/// group is refused before anything is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_stream_never_spans_groups() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let (metas, _) = open_handles(&cluster, 1, loams_meta_tikv::DEFAULT_ID_BLOCK).await;
    let meta = &metas[0];
    let streams = setup(meta, "spans", &[("big", 8), ("small", 1)]).await;
    let (big, small) = (streams[0], streams[1]);
    let limit = u32::try_from(MAX_GROUP_CHUNKS).expect("limit");

    let over: Vec<WalChunk> = (0..=limit).map(|i| one_record(big, i % 8)).collect();
    let refused = meta.commit_wal(wal(meta, "spans/wal-over", over)).await;
    assert!(!refused.earlier_unknown);
    match refused.result {
        Err(MetaError::Rejected(ApplyError::InvalidArgument(m))) => {
            assert!(m.contains(&format!("stream {big}")), "{m}");
        }
        other => panic!("expected InvalidArgument, got {other:?}"),
    }
    assert_eq!(watermarks(meta, big).await, vec![0; 8]);

    let mut fits: Vec<WalChunk> = (0..limit).map(|i| one_record(big, i % 8)).collect();
    fits.insert(3, one_record(small, 0));
    let offsets = meta
        .commit_wal(wal(meta, "spans/wal-fits", fits.clone()))
        .await
        .into_result()
        .expect("commit");
    let mut expected: Vec<u64> = (0..u64::from(limit)).map(|i| i / 8).collect();
    expected.insert(3, 0);
    assert_eq!(offsets, expected);
    assert_eq!(watermarks(meta, big).await, vec![u64::from(limit / 8); 8]);
    assert_eq!(watermarks(meta, small).await, vec![1]);
}

/// Sixteen writers committing to one partition at once all succeed (the
/// head's pessimistic lock queues them), and the offsets are dense.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_head_commits_queue_without_abort_loops() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    const WRITERS: u32 = 16;
    const COMMITS: u32 = 6;
    let (metas, _) = open_handles(&cluster, HANDLES, loams_meta_tikv::DEFAULT_ID_BLOCK).await;
    let s = setup(&metas[0], "hot", &[("events", 1)]).await[0];
    let mut tasks = Vec::new();
    for w in 0..WRITERS {
        let meta = metas[w as usize % metas.len()].clone();
        tasks.push(tokio::spawn(async move {
            let mut got = Vec::new();
            for i in 0..COMMITS {
                let records = 1 + (w + i) % 3;
                let chunk = WalChunk {
                    records,
                    ..one_record(s, 0)
                };
                let tracked = meta
                    .commit_wal(wal(&meta, &format!("hot/w{w}-{i}"), vec![chunk]))
                    .await;
                let offsets = tracked.result.expect("every commit succeeds");
                got.push((offsets[0], records));
            }
            got
        }));
    }
    let mut all = Vec::new();
    for task in tasks {
        let got = task.await.expect("join");
        // Each writer's own commits are in offset order.
        assert!(got.windows(2).all(|w| w[0].0 < w[1].0), "{got:?}");
        all.extend(got);
    }
    all.sort_unstable();
    let mut next = 0u64;
    for (base, records) in &all {
        assert_eq!(*base, next, "offsets are dense: {all:?}");
        next += u64::from(*records);
    }
    assert_eq!(watermarks(&metas[0], s).await, vec![next]);
}

/// A drop racing first commits into its collection's partitions never leaves
/// a live WAL chunk behind: each commit is either refused (the stream is
/// gone) or lands before the drop, which then retires its object.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drop_racing_first_commits_leaves_no_live_chunk() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let (metas, _) = open_handles(&cluster, 2, loams_meta_tikv::DEFAULT_ID_BLOCK).await;
    let ns = metas[0].create_namespace("race").await.expect("namespace");
    let schema = CollectionSchema::new(Vec::new(), Vec::new(), DynamicMapping::Ignore);
    for round in 0..6 {
        let name = format!("docs-{round}");
        let (_, stream, _) = metas[0]
            .create_collection(ns, &name, schema.clone(), 4)
            .await
            .expect("create");
        let object = format!("race/wal-{round}");
        let committer = metas[1].clone();
        let commit = {
            let object = object.clone();
            tokio::spawn(async move {
                let request = wal(&committer, &object, vec![one_record(stream, round % 4)]);
                committer.commit_wal(request).await.into_result()
            })
        };
        let dropped = metas[0].drop_collection(ns, &name).await.expect("drop");
        assert!(dropped.is_some());
        match commit.await.expect("join") {
            Ok(_) => {
                let retired = metas[0].retired_expired(0).await.expect("retired");
                assert!(retired.contains(&object), "round {round}: {retired:?}");
            }
            Err(MetaError::Rejected(ApplyError::StreamNotFound(s))) => assert_eq!(s, stream),
            Err(e) => panic!("round {round}: {e:?}"),
        }
        // No live chunk: the object is either retired or was never indexed.
        let orphan = metas[0]
            .orphan_wal_objects(vec![(object.clone(), 1)], 0, 1)
            .await
            .expect("orphans");
        let retired = metas[0].retired_expired(0).await.expect("retired");
        assert!(
            orphan == vec![object.clone()] || retired.contains(&object),
            "round {round}: {object} still has a live chunk"
        );
    }
}

/// Dropping a collection removes its implicit link's pointer too (review of
/// #61).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_deletes_the_implicit_links_pointer() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let (metas, _) = open_handles(&cluster, 1, loams_meta_tikv::DEFAULT_ID_BLOCK).await;
    let meta = &metas[0];
    let ns = meta.create_namespace("drop-link").await.expect("namespace");
    let schema = CollectionSchema::new(Vec::new(), Vec::new(), DynamicMapping::Ignore);
    let (_, _, link) = meta
        .create_collection(ns, "docs", schema, 1)
        .await
        .expect("create");
    let key = link_pointer_key(link);
    meta.cas_pointer(PointerCas {
        namespace: ns,
        key: key.clone(),
        expected: None,
        value: "drop-link/m-1".to_string(),
        fence: None,
        fresh: None,
    })
    .await
    .into_result()
    .expect("cas");
    meta.drop_collection(ns, "docs").await.expect("drop");
    assert_eq!(meta.pointer(L, ns, &key).await.expect("read"), None);
}
