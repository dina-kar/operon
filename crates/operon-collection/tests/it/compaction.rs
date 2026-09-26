//! Lance compaction (plan M1.3 Task 2, Ruling 5; overview R7): Lance's
//! planner and rewriter, committed as a detached `Rewrite` with real
//! fragment ids through the collection manifest CAS, and rebased group by
//! group on a conflict.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::common::{TargetFixture, WAIT, doc, field, vector};
use arrow_array::{Array, BinaryArray, Float32Array};
use futures::FutureExt;
use lance::Dataset;
use lance::dataset::optimize::plan_compaction;
use lance::index::{DatasetIndexExt, DatasetIndexInternalExt};
use operon_collection::{
    COMPACTION_TASK_PREFIX, CollectionCommitHook, CollectionCommitStep, CollectionConfig,
    CollectionGcRoots, CollectionSchema, CommitKind, DocOp, DynamicMapping, FieldKind,
    IndexBuildSource, IndexWork, LanceCompactionSource, LanceConfig, LanceEnv, MaintenanceConfig,
    PK_COLUMN, PkGcRoots, PrimaryKey, StoredDoc, compaction_options, needs_compaction,
    plan_index_work, vector_column, vector_index_name,
};
use operon_log::gc::{GcConfig, GcSource};
use operon_meta::{Clock, ManualClock, SystemClock};
use operon_worker::{RunResult, TaskError, TaskKey, TaskOutcome, run_once};
use proptest::prelude::*;
use serde_json::json;

const DIM: u32 = 8;

/// The lease TTL of a compaction run.
const TTL: Duration = Duration::from_secs(30);

/// The lease TTL of a run that crashes: short, so a successor soon takes over.
const CRASHED_TTL: Duration = Duration::from_millis(300);

/// Rows per compacted fragment, and per Lance data file of a link commit.
const TARGET_ROWS: usize = 400;

/// The plan's test configuration; every poll proposes the collection.
fn maintenance() -> MaintenanceConfig {
    MaintenanceConfig {
        compaction_target_rows: TARGET_ROWS,
        compaction_min_small_fragments: 3,
        poll_interval: Duration::ZERO,
        ..MaintenanceConfig::default()
    }
}

/// A keyword and one Cosine vector `v` of dimension 8.
fn with_vector() -> CollectionSchema {
    let schema = CollectionSchema::new(
        vec![field("tag", FieldKind::Keyword)],
        vec![vector("v", DIM)],
        DynamicMapping::Ignore,
    );
    schema.validate().expect("valid schema");
    schema
}

/// The fixture with Lance data files of at most [`TARGET_ROWS`] rows.
async fn fixture(config: CollectionConfig) -> TargetFixture {
    fixture_over(TargetFixture::start_with(with_vector(), 2, config).await)
}

fn fixture_over(mut f: TargetFixture) -> TargetFixture {
    f.ctx.lance = LanceEnv::new(
        f.store.clone(),
        LanceConfig {
            max_rows_per_file: TARGET_ROWS,
            ..LanceConfig::default()
        },
    );
    f
}

/// A deterministic pseudo-random vector for key `n` (splitmix64).
fn vector_of(n: u64) -> Vec<f32> {
    let mut state = n.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
    (0..DIM)
        .map(|_| {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            (z >> 40) as f32 / (1u64 << 24) as f32 - 0.5
        })
        .collect()
}

/// An upsert of key `k`, generation `g`, with its vector.
fn upsert(k: u64, g: u64) -> DocOp {
    let mut d = doc(
        PrimaryKey::U64(k),
        json!({ "tag": format!("t{}", k % 3), "g": g }),
    );
    d.vectors.insert("v".to_string(), vector_of(k));
    DocOp::Upsert(d)
}

/// Writes `ops` and applies them in one link commit.
async fn commit(f: &TargetFixture, ops: Vec<DocOp>) {
    f.write(ops).await;
    f.apply_all(&f.source(f.factory()), "linker").await;
}

/// `count` commits of `size` new keys each, from key `first`: one fragment
/// each.
async fn commits(f: &TargetFixture, first: u64, count: u64, size: u64) {
    for c in 0..count {
        let start = first + c * size;
        commit(f, (start..start + size).map(|k| upsert(k, 0)).collect()).await;
    }
}

fn outcome(results: Vec<(TaskKey, RunResult)>) -> Result<TaskOutcome, TaskError> {
    let [(key, RunResult::Ran(result))] = <[_; 1]>::try_from(results).expect("one task") else {
        panic!("the compaction task did not run");
    };
    assert!(key.key.starts_with(COMPACTION_TASK_PREFIX), "{key}");
    result
}

async fn compact_once(
    f: &TargetFixture,
    source: &LanceCompactionSource,
) -> Result<TaskOutcome, TaskError> {
    outcome(
        run_once(&f.meta.client, "compactor", TTL, source)
            .await
            .expect("run"),
    )
}

async fn live_dataset(f: &TargetFixture) -> Arc<Dataset> {
    f.snapshot()
        .await
        .dataset()
        .cloned()
        .expect("a lance dataset")
}

fn fragment_ids(dataset: &Dataset) -> Vec<u64> {
    dataset.manifest.fragments.iter().map(|f| f.id).collect()
}

fn assert_verified(problems: Vec<String>) {
    assert!(problems.is_empty(), "{problems:#?}");
}

async fn wait_for(what: &str, check: impl Fn() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !check() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A hook that holds the first compaction at `step` until `release` is
/// notified, and says when it got there.
fn hold_at(
    step: CollectionCommitStep,
) -> (
    CollectionCommitHook,
    Arc<AtomicBool>,
    Arc<tokio::sync::Notify>,
) {
    let reached = Arc::new(AtomicBool::new(false));
    let release = Arc::new(tokio::sync::Notify::new());
    let (flag, gate) = (reached.clone(), release.clone());
    let hook: CollectionCommitHook = Arc::new(move |at, _fence| {
        if at == step && !flag.swap(true, Ordering::SeqCst) {
            let gate = gate.clone();
            async move { gate.notified().await }.boxed()
        } else {
            futures::future::ready(()).boxed()
        }
    });
    (hook, reached, release)
}

/// The fragment ids of each task Lance would plan on the live dataset.
async fn planned_groups(f: &TargetFixture) -> Vec<Vec<u64>> {
    let dataset = live_dataset(f).await;
    plan_compaction(&dataset, &compaction_options(&maintenance()))
        .await
        .expect("plan")
        .tasks
        .iter()
        .map(|task| task.fragments.iter().map(|f| f.id).collect())
        .collect()
}

/// 20 commits of 50 rows: one run leaves at most 3 fragments, with the same
/// documents, row ids and positions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn small_fragments_are_compacted_into_target_sized_ones() {
    let f = fixture(CollectionConfig::default()).await;
    commits(&f, 0, 20, 50).await;
    let before_manifest = f.manifest().await;
    assert_eq!(live_dataset(&f).await.manifest.fragments.len(), 20);
    assert!(needs_compaction(&*live_dataset(&f).await, &maintenance()));
    let before = f.snapshot().await.scan_all().await.expect("scan");

    let source = LanceCompactionSource::new(f.ctx.clone(), maintenance());
    assert_eq!(compact_once(&f, &source).await.unwrap(), TaskOutcome::Idle);
    let manifest = f.manifest().await;
    assert_eq!(manifest.version, before_manifest.version + 1);
    assert_eq!(manifest.kind, CommitKind::Maintenance);
    assert_ne!(manifest.lance_version, before_manifest.lance_version);
    let dataset = live_dataset(&f).await;
    assert!(
        dataset.manifest.fragments.len() <= 3,
        "{:?}",
        fragment_ids(&dataset)
    );
    assert!(!fragment_ids(&dataset).contains(&0));
    assert!(!needs_compaction(&dataset, &maintenance()));
    assert_eq!(f.snapshot().await.scan_all().await.expect("scan"), before);
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// 30 % of 1 000 rows deleted: compaction drops them, every surviving row
/// id reads the same document, and every deleted one reads nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_purges_deleted_rows_and_keeps_row_ids() {
    let f = fixture(CollectionConfig::default()).await;
    commit(&f, (0..1_000).map(|k| upsert(k, 0)).collect()).await;
    let all = f.snapshot().await.scan_all().await.expect("scan");
    commit(
        &f,
        (0..1_000)
            .filter(|k| k % 10 < 3)
            .map(|k| DocOp::Delete(PrimaryKey::U64(k)))
            .collect(),
    )
    .await;
    let deleted: Vec<u64> = all
        .iter()
        .filter(|d| matches!(d.pk, PrimaryKey::U64(k) if k % 10 < 3))
        .map(|d| d.row_id)
        .collect();
    let survivors: BTreeMap<u64, StoredDoc> = f
        .snapshot()
        .await
        .scan_all()
        .await
        .expect("scan")
        .into_iter()
        .map(|d| (d.row_id, d))
        .collect();
    assert_eq!((deleted.len(), survivors.len()), (300, 700));
    assert!(needs_compaction(&*live_dataset(&f).await, &maintenance()));

    let source = LanceCompactionSource::new(f.ctx.clone(), maintenance());
    assert!(compact_once(&f, &source).await.is_ok());
    let dataset = live_dataset(&f).await;
    let physical: usize = dataset
        .manifest
        .fragments
        .iter()
        .map(|f| f.physical_rows.expect("physical rows"))
        .sum();
    assert_eq!(physical, 700);
    assert!(
        dataset
            .manifest
            .fragments
            .iter()
            .all(|f| f.deletion_file.is_none())
    );
    let snapshot = f.snapshot().await;
    let ids: Vec<u64> = survivors.keys().copied().collect();
    let taken = snapshot.take_rows(&ids).await.expect("take");
    for (id, doc) in ids.iter().zip(taken) {
        assert_eq!(doc.as_ref(), Some(&survivors[id]), "row {id}");
    }
    let gone = snapshot.take_rows(&deleted).await.expect("take");
    assert!(gone.iter().all(Option::is_none));
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// The `k` nearest keys to `query` on `column`, probing `nprobes`
/// partitions and refining 20 × `k` candidates with the full vectors.
async fn nearest(dataset: &Dataset, column: &str, query: &[f32], nprobes: usize) -> PrimaryKey {
    let query = Float32Array::from(query.to_vec());
    let mut scanner = dataset.scan();
    scanner
        .nearest(column, &query, 1)
        .expect("nearest")
        .nprobes(nprobes)
        .refine(20);
    scanner.project(&[PK_COLUMN]).expect("project");
    let batch = scanner.try_into_batch().await.expect("search");
    let pks = batch
        .column_by_name(PK_COLUMN)
        .expect("pk")
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("binary")
        .clone();
    PrimaryKey::from_canonical(pks.value(0)).expect("a primary key")
}

/// M1.2's default nprobes (Task 0 E54): `max(20, ceil(p / 16))` for the
/// largest segment's partition count `p`, read through Lance's
/// `DatasetIndexInternalExt`.
async fn default_nprobes(dataset: &Dataset, column: &str, index_name: &str) -> usize {
    let index = dataset
        .open_logical_vector_index(column, index_name)
        .await
        .expect("open the vector index");
    let partitions = index
        .as_ivf()
        .expect("an ivf index")
        .num_partitions_per_segment()
        .into_iter()
        .map(|(_, n)| n)
        .max()
        .unwrap_or(0);
    20usize.max(partitions.div_ceil(16))
}

/// An IVF_PQ segment over 600 rows, then a compaction: no fragment id is 0,
/// the segment follows the new fragments, every probe finds itself, and no
/// delta segment is planned.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_fragments_get_real_ids_and_indexes_follow_them() {
    let config = CollectionConfig {
        index_min_rows: 500,
        index_delta_min_rows: 10_000,
        index_poll_interval: Duration::ZERO,
        ..CollectionConfig::default()
    };
    let f = fixture(config).await;
    commits(&f, 0, 6, 100).await;
    let indexer = IndexBuildSource::new(f.ctx.clone());
    let results = run_once(&f.meta.client, "indexer", TTL, &indexer)
        .await
        .expect("run");
    assert!(
        matches!(results.as_slice(), [(_, RunResult::Ran(Ok(_)))]),
        "{results:?}"
    );
    let before = f.manifest().await;
    let [segment] = before.vector_indexes.as_slice() else {
        panic!("one segment: {:?}", before.vector_indexes);
    };

    let source = LanceCompactionSource::new(f.ctx.clone(), maintenance());
    assert!(compact_once(&f, &source).await.is_ok());
    let manifest = f.manifest().await;
    assert_eq!(manifest.vector_indexes, before.vector_indexes);
    let dataset = live_dataset(&f).await;
    let ids = fragment_ids(&dataset);
    assert!(ids.len() < 6, "{ids:?}");
    assert!(!ids.contains(&0), "{ids:?}");
    let indices = dataset.load_indices().await.expect("indices");
    let index = indices
        .iter()
        .find(|i| i.uuid.to_string() == segment.lance_index_uuid)
        .expect("the segment");
    let covered: Vec<u64> = index
        .fragment_bitmap
        .as_ref()
        .expect("a fragment bitmap")
        .iter()
        .map(u64::from)
        .collect();
    assert_eq!(covered, ids);
    let column = vector_column(0);
    let partitions = (600f64).sqrt().round() as usize;
    let nprobes = default_nprobes(&dataset, &column, &vector_index_name(0)).await;
    for k in [0, 99, 250, 599] {
        for probes in [partitions, nprobes] {
            assert_eq!(
                nearest(&dataset, &column, &vector_of(k), probes).await,
                PrimaryKey::U64(k),
                "doc {k} with {probes} probes"
            );
        }
    }
    let work = plan_index_work(&with_vector(), &manifest, &dataset, &indices, &f.ctx.config);
    assert!(
        !work
            .iter()
            .any(|w| matches!(w, IndexWork::VectorDelta { .. })),
        "{work:?}"
    );
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// `get_by_pk` through the `_pk` BTREE answers the same before and after a
/// compaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_pk_btree_still_serves_lookups_after_compaction() {
    let config = CollectionConfig {
        pk_index_min_unindexed_rows: 50,
        index_poll_interval: Duration::ZERO,
        ..CollectionConfig::default()
    };
    let f = fixture(config).await;
    commits(&f, 0, 6, 50).await;
    let indexer = IndexBuildSource::new(f.ctx.clone());
    let results = run_once(&f.meta.client, "indexer", TTL, &indexer)
        .await
        .expect("run");
    assert!(
        matches!(results.as_slice(), [(_, RunResult::Ran(Ok(_)))]),
        "{results:?}"
    );
    assert_eq!(f.manifest().await.scalar_indexes.len(), 1);
    let keys: Vec<PrimaryKey> = (0..300).step_by(6).map(PrimaryKey::U64).collect();
    assert_eq!(keys.len(), 50);
    let before = f.snapshot().await.get_by_pk(&keys).await.expect("get");
    assert!(before.iter().all(Option::is_some));

    let source = LanceCompactionSource::new(f.ctx.clone(), maintenance());
    assert!(compact_once(&f, &source).await.is_ok());
    let manifest = f.manifest().await;
    assert_eq!(manifest.scalar_indexes.len(), 1);
    assert!(live_dataset(&f).await.manifest.fragments.len() < 6);
    assert_eq!(
        f.snapshot().await.get_by_pk(&keys).await.expect("get"),
        before
    );
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// A compaction commits a new Lance version only: the splits, `applied`,
/// the counts and the PK index stay as they were.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn splits_and_the_pk_index_are_untouched_by_compaction() {
    let f = fixture(CollectionConfig::default()).await;
    commits(&f, 0, 5, 40).await;
    commit(
        &f,
        (0..40)
            .step_by(4)
            .map(|k| DocOp::Delete(PrimaryKey::U64(k)))
            .collect(),
    )
    .await;
    let before = f.manifest().await;
    let watermark = f.pk_watermark().await;
    let source = LanceCompactionSource::new(f.ctx.clone(), maintenance());
    assert!(compact_once(&f, &source).await.is_ok());
    let after = f.manifest().await;
    assert_eq!(after.version, before.version + 1);
    assert_eq!(after.splits, before.splits);
    assert_eq!(
        (
            after.applied.clone(),
            after.live_doc_count,
            after.pk_delta.clone()
        ),
        (before.applied.clone(), before.live_doc_count, None)
    );
    assert_eq!(f.pk_watermark().await, watermark);
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// The compaction is held after its Lance commit while a link commit
/// deletes a row of the first group's fragments and appends 10 rows: the
/// rebase drops that group, keeps the other, and the row stays deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_whose_fragment_gained_deletions_is_dropped_on_rebase() {
    let f = fixture(CollectionConfig::default()).await;
    commits(&f, 0, 20, 50).await;
    let groups = planned_groups(&f).await;
    assert_eq!(groups.len(), 2, "{groups:?}");
    let first = live_dataset(&f).await.manifest.fragments[0].id;
    assert!(groups[0].contains(&first));
    let (hook, reached, release) = hold_at(CollectionCommitStep::AfterLanceCommit);
    let source = LanceCompactionSource::new(f.ctx.clone(), maintenance()).with_hook(hook);
    let meta = f.meta.client.clone();
    let run = tokio::spawn(async move { run_once(&meta, "compactor", TTL, &source).await });
    wait_for("the compaction's lance commit", || {
        reached.load(Ordering::SeqCst)
    })
    .await;

    // Key 0 was written by the first commit, into the first fragment.
    let mut ops = vec![DocOp::Delete(PrimaryKey::U64(0))];
    ops.extend((1_000..1_010).map(|k| upsert(k, 0)));
    commit(&f, ops).await;
    let appended = *fragment_ids(&*live_dataset(&f).await)
        .last()
        .expect("a fragment");
    let link_version = f.manifest().await.version;
    release.notify_one();
    let result = outcome(run.await.expect("join").expect("run"));
    assert!(result.is_ok(), "{result:?}");

    let manifest = f.manifest().await;
    assert_eq!(manifest.version, link_version + 1);
    assert_eq!(manifest.kind, CommitKind::Maintenance);
    let dataset = live_dataset(&f).await;
    let ids: BTreeSet<u64> = fragment_ids(&dataset).into_iter().collect();
    assert!(groups[0].iter().all(|id| ids.contains(id)), "{ids:?}");
    assert!(groups[1].iter().all(|id| !ids.contains(id)), "{ids:?}");
    assert!(ids.contains(&appended));
    let deleted = dataset
        .manifest
        .fragments
        .iter()
        .find(|f| f.id == first)
        .expect("the first fragment");
    assert!(deleted.deletion_file.is_some());
    let docs = f.snapshot().await.scan_all().await.expect("scan");
    assert!(!docs.iter().any(|d| d.pk == PrimaryKey::U64(0)));
    assert_eq!(docs.len(), 1_009);
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// An append-only link commit during the hold: every group is re-applied,
/// with ids above the appended fragment's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_append_during_compaction_rebases_cleanly() {
    let f = fixture(CollectionConfig::default()).await;
    commits(&f, 0, 20, 50).await;
    let groups = planned_groups(&f).await;
    let (hook, reached, release) = hold_at(CollectionCommitStep::AfterLanceCommit);
    let source = LanceCompactionSource::new(f.ctx.clone(), maintenance()).with_hook(hook);
    let meta = f.meta.client.clone();
    let run = tokio::spawn(async move { run_once(&meta, "compactor", TTL, &source).await });
    wait_for("the compaction's lance commit", || {
        reached.load(Ordering::SeqCst)
    })
    .await;

    commit(&f, (1_000..1_010).map(|k| upsert(k, 0)).collect()).await;
    let appended = *fragment_ids(&*live_dataset(&f).await)
        .last()
        .expect("a fragment");
    release.notify_one();
    let result = outcome(run.await.expect("join").expect("run"));
    assert!(result.is_ok(), "{result:?}");

    let dataset = live_dataset(&f).await;
    let ids = fragment_ids(&dataset);
    let old: BTreeSet<u64> = groups.iter().flatten().copied().collect();
    assert!(ids.iter().all(|id| !old.contains(id)), "{ids:?}");
    assert!(ids.contains(&appended));
    let new: Vec<u64> = ids.iter().copied().filter(|id| *id != appended).collect();
    assert!(!new.is_empty());
    assert!(new.iter().all(|id| *id > appended), "{ids:?}");
    assert_eq!(
        f.snapshot().await.scan_all().await.expect("scan").len(),
        1_010
    );
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// The task is dropped after its Lance commit (a crash): the live manifest
/// is unchanged, and the next run compacts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_after_the_compaction_lance_commit_leaves_no_trace() {
    let f = fixture(CollectionConfig::default()).await;
    commits(&f, 0, 5, 50).await;
    let before = f.manifest().await;
    let (hook, reached, _release) = hold_at(CollectionCommitStep::AfterLanceCommit);
    let source = LanceCompactionSource::new(f.ctx.clone(), maintenance()).with_hook(hook);
    let meta = f.meta.client.clone();
    let crashed = tokio::spawn(async move {
        let _ = run_once(&meta, "crashed", CRASHED_TTL, &source).await;
    });
    wait_for("the compaction's lance commit", || {
        reached.load(Ordering::SeqCst)
    })
    .await;
    crashed.abort();
    let _ = crashed.await;
    assert_eq!(f.manifest().await, before);
    assert_eq!(live_dataset(&f).await.manifest.fragments.len(), 5);

    let source = LanceCompactionSource::new(f.ctx.clone(), maintenance());
    let deadline = Instant::now() + WAIT;
    loop {
        let results = run_once(&f.meta.client, "w2", TTL, &source)
            .await
            .expect("run");
        match results.as_slice() {
            [(_, RunResult::LeaseHeld)] => {
                assert!(Instant::now() < deadline, "the crashed lease never expired");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            _ => {
                assert!(outcome(results).is_ok());
                break;
            }
        }
    }
    assert_eq!(f.manifest().await.version, before.version + 1);
    assert_eq!(live_dataset(&f).await.manifest.fragments.len(), 1);
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// The task's lease is taken over after its manifest PUT: its CAS is
/// refused as `Fenced` and the manifest is unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fenced_compaction_changes_nothing() {
    let f = fixture(CollectionConfig::default()).await;
    commits(&f, 0, 5, 50).await;
    let before = f.manifest().await;
    let meta = f.meta.client.clone();
    let taken = Arc::new(AtomicBool::new(false));
    let flag = taken.clone();
    let hook: CollectionCommitHook = Arc::new(move |at, fence| {
        let meta = meta.clone();
        let flag = flag.clone();
        async move {
            if at == CollectionCommitStep::AfterManifestPut && !flag.swap(true, Ordering::SeqCst) {
                meta.release_lease(&fence.lease, "compactor", fence.epoch)
                    .await
                    .expect("release");
                meta.acquire_lease(&fence.lease, "thief", TTL)
                    .await
                    .expect("take over");
            }
        }
        .boxed()
    });
    let source = LanceCompactionSource::new(f.ctx.clone(), maintenance()).with_hook(hook);
    let result = compact_once(&f, &source).await;
    assert!(matches!(result, Err(TaskError::Fenced)), "{result:?}");
    assert!(taken.load(Ordering::SeqCst));
    assert_eq!(f.manifest().await, before);
    f.shutdown().await;
}

/// With `compaction` off nothing is proposed; a dataset without enough
/// small fragments is left alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_runs_only_when_needed() {
    let f = fixture(CollectionConfig::default()).await;
    commits(&f, 0, 2, 50).await;
    let off = LanceCompactionSource::new(
        f.ctx.clone(),
        MaintenanceConfig {
            compaction: false,
            ..maintenance()
        },
    );
    let results = run_once(&f.meta.client, "compactor", TTL, &off)
        .await
        .expect("run");
    assert!(results.is_empty());
    let before = f.manifest().await;
    let source = LanceCompactionSource::new(f.ctx.clone(), maintenance());
    assert_eq!(compact_once(&f, &source).await.unwrap(), TaskOutcome::Idle);
    assert_eq!(f.manifest().await, before);
    f.shutdown().await;
}

/// M1.1's check after 3 compactions: the Lance mainline holds only version
/// 1; every other manifest is detached.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mainline_never_moves_past_version_one_after_compaction() {
    let f = fixture(CollectionConfig::default()).await;
    let source = LanceCompactionSource::new(f.ctx.clone(), maintenance());
    for round in 0..3 {
        commits(&f, round * 1_000, 4, 30).await;
        assert!(compact_once(&f, &source).await.is_ok());
    }
    let versions = format!("ns/{}/collections/{}/lance/_versions/", f.ns, f.cid);
    let names: Vec<String> = f
        .store
        .list(&versions)
        .await
        .expect("list")
        .into_iter()
        .map(|info| info.path.rsplit('/').next().expect("name").to_string())
        .filter(|name| name.ends_with(".manifest"))
        .collect();
    let mainline: Vec<&String> = names.iter().filter(|n| !n.starts_with('d')).collect();
    assert_eq!(
        mainline,
        vec![&format!("{:020}.manifest", u64::MAX - 1)],
        "{names:?}"
    );
    assert_eq!(
        f.snapshot().await.scan_all().await.expect("scan").len(),
        3 * 4 * 30
    );
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// `keep_manifests 1`, retention 0: once no retained manifest names the
/// compacted fragments' old data files, GC past grace deletes them and keeps
/// the new ones.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_collects_rewritten_fragments_after_retention() {
    const GRACE: Duration = Duration::from_secs(60);
    let clock = Arc::new(ManualClock::new(SystemClock.now_ms()));
    let config = CollectionConfig {
        keep_manifests: 1,
        time_travel_retention: Duration::ZERO,
        ..CollectionConfig::default()
    };
    let f = fixture_over(
        TargetFixture::start_with_clock(with_vector(), 2, config, clock.clone()).await,
    );
    let data = format!("ns/{}/collections/{}/lance/data/", f.ns, f.cid);
    let files = || async {
        f.store
            .list(&data)
            .await
            .expect("list")
            .into_iter()
            .map(|info| info.path)
            .collect::<BTreeSet<String>>()
    };
    commits(&f, 0, 5, 50).await;
    let old = files().await;
    let source = LanceCompactionSource::new(f.ctx.clone(), maintenance());
    assert!(compact_once(&f, &source).await.is_ok());
    let new: BTreeSet<String> = files().await.difference(&old).cloned().collect();
    assert!(!new.is_empty());
    // Two more commits: the compaction's parent leaves the retained set.
    commits(&f, 100, 2, 5).await;
    let gc = GcSource::with_roots(
        f.store.clone(),
        GcConfig {
            grace: GRACE,
            ..GcConfig::default()
        },
        vec![
            Arc::new(CollectionGcRoots::new(f.ctx.clone())),
            Arc::new(PkGcRoots),
        ],
    );
    let now = clock.now_ms().max(SystemClock.now_ms());
    clock.set(now + u64::try_from(GRACE.as_millis()).unwrap() + 1_000);
    for _ in 0..2 {
        gc.run_once(&f.meta.client, "gc")
            .await
            .expect("gc")
            .expect("the gc lease");
    }
    let left = files().await;
    assert!(old.iter().all(|path| !left.contains(path)), "{left:?}");
    assert!(new.iter().all(|path| left.contains(path)), "{left:?}");
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// One random op over 40 keys.
#[derive(Clone, Debug)]
enum RandomOp {
    Upsert(u64, u64),
    Delete(u64),
}

impl RandomOp {
    fn op(&self) -> DocOp {
        match *self {
            RandomOp::Upsert(k, g) => upsert(k, g),
            RandomOp::Delete(k) => DocOp::Delete(PrimaryKey::U64(k)),
        }
    }
}

fn random_op() -> impl Strategy<Value = RandomOp> {
    prop_oneof![
        3 => (0u64..40, 0u64..5).prop_map(|(k, g)| RandomOp::Upsert(k, g)),
        2 => (0u64..40).prop_map(RandomOp::Delete),
    ]
}

/// After each batch: nothing, a compaction run, or a compaction held after
/// its Lance commit while the next batch commits.
#[derive(Clone, Copy, Debug)]
enum Then {
    Nothing,
    Compact,
    Hold,
}

fn then() -> impl Strategy<Value = Then> {
    prop_oneof![
        1 => Just(Then::Nothing),
        2 => Just(Then::Compact),
        2 => Just(Then::Hold),
    ]
}

fn workload() -> impl Strategy<Value = Vec<(Vec<RandomOp>, Then)>> {
    prop::collection::vec((prop::collection::vec(random_op(), 1..15), then()), 4..=8)
}

/// A compaction run in the background.
type Run = tokio::task::JoinHandle<Result<Vec<(TaskKey, RunResult)>, TaskError>>;

/// Whether a run would reach its Lance commit.
async fn would_compact(f: &TargetFixture, config: &MaintenanceConfig) -> bool {
    let Some(dataset) = f.snapshot().await.dataset().cloned() else {
        return false;
    };
    needs_compaction(&dataset, config)
        && plan_compaction(&dataset, &compaction_options(config))
            .await
            .expect("plan")
            .num_tasks()
            > 0
}

async fn random_batches_and_compactions(steps: Vec<(Vec<RandomOp>, Then)>) {
    let f = fixture(CollectionConfig::default()).await;
    let config = maintenance();
    let mut held: Option<(Run, Arc<tokio::sync::Notify>)> = None;
    for (batch, then) in steps {
        commit(&f, batch.iter().map(RandomOp::op).collect()).await;
        if let Some((run, release)) = held.take() {
            release.notify_one();
            let result = outcome(run.await.expect("join").expect("run"));
            assert!(result.is_ok(), "{result:?}");
            assert_verified(f.verify().await);
        }
        match then {
            Then::Nothing => {}
            Then::Compact => {
                let source = LanceCompactionSource::new(f.ctx.clone(), config.clone());
                let result = compact_once(&f, &source).await;
                assert!(result.is_ok(), "{result:?}");
            }
            Then::Hold if would_compact(&f, &config).await => {
                let (hook, reached, release) = hold_at(CollectionCommitStep::AfterLanceCommit);
                let source =
                    LanceCompactionSource::new(f.ctx.clone(), config.clone()).with_hook(hook);
                let meta = f.meta.client.clone();
                let run =
                    tokio::spawn(async move { run_once(&meta, "compactor", TTL, &source).await });
                wait_for("the hold", || reached.load(Ordering::SeqCst)).await;
                held = Some((run, release));
            }
            Then::Hold => {}
        }
        assert_verified(f.verify().await);
    }
    if let Some((run, release)) = held.take() {
        release.notify_one();
        let result = outcome(run.await.expect("join").expect("run"));
        assert!(result.is_ok(), "{result:?}");
    }
    assert_verified(f.verify().await);
    f.shutdown().await;
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 16, ..ProptestConfig::default() })]

    /// Random link batches interleaved with compactions and held
    /// compactions never bring a deleted row back.
    #[test]
    fn compaction_never_resurrects_a_deleted_row(steps in workload()) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(random_batches_and_compactions(steps));
    }
}
