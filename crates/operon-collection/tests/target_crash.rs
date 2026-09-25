//! Crashes and zombies on the collection commit path (plan M1.1 Task 10,
//! Review Focus 1 and 2). A hook holds a commit at a step and the test drops
//! the task there, like a killed process; a fresh factory then re-runs, and
//! the collection must be the stream's fold, every record applied once.

mod common;

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{TargetFixture, WAIT, field, patch, schema, upsert};
use futures::FutureExt;
use operon_collection::{
    CollectionCommitHook, CollectionCommitStep, CollectionSchema, CollectionTargetFactory, DocOp,
    Document, DynamicMapping, FieldKind, LanceCommitter, NewRow, PkWatermark, PrimaryKey,
    RebuildHook, to_record_batch,
};
use operon_common::meta::MetaStore;
use operon_link::{CommitError, LinkTargetFactory};
use operon_meta::{Fence, collection_pk_prefix};
use operon_pk::{PkIndex, PkIndexConfig};
use operon_worker::{
    Candidate, Priority, RunResult, Task, TaskContext, TaskError, TaskKey, TaskOutcome, TaskSource,
    Worker, WorkerConfig, run_once,
};
use proptest::prelude::*;
use serde_json::json;

const STEPS: [CollectionCommitStep; 5] = [
    CollectionCommitStep::AfterLanceCommit,
    CollectionCommitStep::AfterSplitPut,
    CollectionCommitStep::AfterManifestPut,
    CollectionCommitStep::AfterCas,
    CollectionCommitStep::AfterPkWrite,
];

/// The lease TTL of a run that crashes: short, so a successor soon takes over.
const CRASHED_TTL: Duration = Duration::from_millis(300);

fn tagged() -> CollectionSchema {
    schema(
        vec![field("tag", FieldKind::Keyword), field("n", FieldKind::I64)],
        DynamicMapping::Ignore,
    )
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

/// A hook that holds the `nth` commit (1-based) at `step` forever, and says
/// when it got there.
fn crash_at(step: CollectionCommitStep, nth: u32) -> (CollectionCommitHook, Arc<AtomicBool>) {
    let reached = Arc::new(AtomicBool::new(false));
    let seen = Arc::new(AtomicU32::new(0));
    let flag = reached.clone();
    let hook: CollectionCommitHook = Arc::new(move |at, _fence| {
        if at == step && seen.fetch_add(1, Ordering::SeqCst) + 1 == nth {
            flag.store(true, Ordering::SeqCst);
            futures::future::pending::<()>().boxed()
        } else {
            futures::future::ready(()).boxed()
        }
    });
    (hook, reached)
}

/// Runs the link with a fresh factory until its first commit reaches `step`,
/// then drops the task there.
async fn crash_run(f: &TargetFixture, step: CollectionCommitStep) {
    let (hook, reached) = crash_at(step, 1);
    crash_with(f, f.hooked_factory(hook), reached).await;
}

/// Runs the link with `factory` until `reached`, then drops the task there.
async fn crash_with(
    f: &TargetFixture,
    factory: Arc<CollectionTargetFactory>,
    reached: Arc<AtomicBool>,
) {
    let source = f.source(factory);
    let meta = f.meta.client.clone();
    let crashed = tokio::spawn(async move {
        loop {
            run_once(&meta, "crashed", CRASHED_TTL, &source)
                .await
                .expect("run");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    wait_for("the crash point", || reached.load(Ordering::SeqCst)).await;
    crashed.abort();
    let _ = crashed.await;
}

/// The detached Lance version ids of the collection's dataset.
async fn detached_versions(f: &TargetFixture) -> BTreeSet<u64> {
    let prefix = format!("ns/{}/collections/{}/lance/_versions/", f.ns, f.cid);
    f.store
        .list(&prefix)
        .await
        .expect("list")
        .into_iter()
        .filter_map(|info| {
            let name = info.path.rsplit('/').next()?.to_string();
            name.strip_prefix('d')?
                .strip_suffix(".manifest")?
                .parse()
                .ok()
        })
        .collect()
}

/// The data files of Lance version `version`.
async fn data_files(f: &TargetFixture, version: u64) -> BTreeSet<String> {
    let dataset = f.ctx.lance.open(f.ns, f.cid, version).await.expect("open");
    dataset
        .manifest
        .fragments
        .iter()
        .flat_map(|fragment| fragment.files.iter().map(|file| file.path.clone()))
        .collect()
}

fn key(n: u64) -> PrimaryKey {
    PrimaryKey::U64(n)
}

/// Batch 2 of the crash tests: replaces, deletes, patches and new keys over
/// batch 1's keys 0..12.
fn mixed_batch() -> Vec<DocOp> {
    let mut ops: Vec<DocOp> = (0..6)
        .map(|n| upsert(n, json!({ "n": n + 100, "tag": "replaced" })))
        .collect();
    ops.extend((6..9).map(|n| DocOp::Delete(key(n))));
    ops.extend((9..12).map(|n| patch(key(n), json!({ "extra": { "n": n } }))));
    ops.extend((12..15).map(|n| upsert(n, json!({ "n": n }))));
    ops
}

/// A crash at `step` in the second commit, a recovery by a fresh factory,
/// then one more batch over the same keys. Returns the fixture and the
/// detached Lance versions the crashed run left.
async fn crash_and_recover(step: CollectionCommitStep) -> (TargetFixture, BTreeSet<u64>) {
    let f = TargetFixture::start(tagged(), 3).await;
    f.write((0..12).map(|n| upsert(n, json!({ "n": n }))).collect())
        .await;
    f.apply_all(&f.source(f.factory()), "w1").await;
    f.write(mixed_batch()).await;
    let before = detached_versions(&f).await;
    crash_run(&f, step).await;
    let orphans: BTreeSet<u64> = detached_versions(&f)
        .await
        .difference(&before)
        .copied()
        .collect();
    let fresh = f.source(f.factory());
    f.apply_all(&fresh, "w2").await;
    assert_verified(f.verify().await);
    f.write((0..15).map(|n| upsert(n, json!({ "n": n * 2 }))).collect())
        .await;
    f.apply_all(&fresh, "w2").await;
    assert_verified(f.verify().await);
    (f, orphans)
}

/// Review focus 1: the crashed run's Lance version is an orphan that no
/// manifest ever references.
#[tokio::test]
async fn a_crash_after_the_lance_commit_applies_the_batch_once() {
    let (f, orphans) = crash_and_recover(CollectionCommitStep::AfterLanceCommit).await;
    assert!(
        !orphans.is_empty(),
        "the crashed run committed a Lance version"
    );
    let mut manifest = f.manifest().await;
    loop {
        assert!(
            !orphans.contains(&manifest.lance_version),
            "manifest {} references the orphan {}",
            manifest.version,
            manifest.lance_version
        );
        let Some(parent) = manifest.parent_manifest.clone() else {
            break;
        };
        manifest = (*f.ctx.manifests.load(&f.store, &parent).await.unwrap()).clone();
    }
    f.shutdown().await;
}

#[tokio::test]
async fn a_crash_after_the_split_put_applies_the_batch_once() {
    let (f, _) = crash_and_recover(CollectionCommitStep::AfterSplitPut).await;
    f.shutdown().await;
}

#[tokio::test]
async fn a_crash_after_the_manifest_put_applies_the_batch_once() {
    let (f, _) = crash_and_recover(CollectionCommitStep::AfterManifestPut).await;
    f.shutdown().await;
}

/// Review focus 2: the commit landed but the PK index was not written; the
/// next run repairs it before resolving batch 3's keys, so no key gets a
/// second row.
#[tokio::test]
async fn a_crash_after_the_cas_repairs_the_pk_index_before_the_next_batch() {
    let (f, _) = crash_and_recover(CollectionCommitStep::AfterCas).await;
    let docs = f.snapshot().await.scan_all().await.unwrap();
    let keys: BTreeSet<&PrimaryKey> = docs.iter().map(|d| &d.pk).collect();
    assert_eq!(keys.len(), docs.len(), "one live row per key");
    assert_eq!(docs.len(), 15);
    f.shutdown().await;
}

#[tokio::test]
async fn a_crash_after_the_pk_write_is_clean() {
    let (f, _) = crash_and_recover(CollectionCommitStep::AfterPkWrite).await;
    f.shutdown().await;
}

/// Review focus 1: a zombie holds its commit after its Lance commit while a
/// successor commits the same batch and more; released, the zombie's CAS is
/// fenced, and its Lance version never reaches the live manifest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_zombie_lance_commit_never_reaches_the_live_manifest() {
    let f = TargetFixture::start(tagged(), 3).await;
    f.write((0..10).map(|n| upsert(n, json!({ "n": n }))).collect())
        .await;
    let before = detached_versions(&f).await;
    let release = Arc::new(tokio::sync::Notify::new());
    let reached = Arc::new(AtomicBool::new(false));
    // The held commit's fence, and the steps the zombie took under it after
    // it resumed.
    let zombie_fence: Arc<Mutex<Option<Fence>>> = Arc::default();
    let zombie_steps: Arc<Mutex<Vec<CollectionCommitStep>>> = Arc::default();
    let hook: CollectionCommitHook = {
        let (release, reached) = (release.clone(), reached.clone());
        let (zombie_fence, zombie_steps) = (zombie_fence.clone(), zombie_steps.clone());
        let first = Arc::new(AtomicBool::new(true));
        Arc::new(move |at, fence| {
            if at == CollectionCommitStep::AfterLanceCommit && first.swap(false, Ordering::SeqCst) {
                *zombie_fence.lock().unwrap() = Some(fence);
                reached.store(true, Ordering::SeqCst);
                let release = release.clone();
                async move { release.notified().await }.boxed()
            } else {
                if zombie_fence.lock().unwrap().as_ref() == Some(&fence) {
                    zombie_steps.lock().unwrap().push(at);
                }
                futures::future::ready(()).boxed()
            }
        })
    };
    let worker_config = |owner: &str| WorkerConfig {
        poll_interval: Duration::from_millis(10),
        lease_ttl: Duration::from_millis(400),
        ..WorkerConfig::new(owner)
    };
    let zombie_runs: Arc<Mutex<Vec<String>>> = Arc::default();
    let mut zombie = Worker::new(f.meta.client.clone(), worker_config("zombie"));
    zombie.add_source(Arc::new(Recording {
        inner: f.source(f.hooked_factory(hook)),
        runs: zombie_runs.clone(),
    }));
    let zombie = zombie.start();
    wait_for("the zombie's lance commit", || {
        reached.load(Ordering::SeqCst)
    })
    .await;
    zombie.pause_renewals(true);
    let zombie_versions: BTreeSet<u64> = detached_versions(&f)
        .await
        .difference(&before)
        .copied()
        .collect();
    assert_eq!(zombie_versions.len(), 1, "{zombie_versions:?}");

    let mut successor = Worker::new(f.meta.client.clone(), worker_config("successor"));
    successor.add_source(Arc::new(f.source(f.factory())));
    let successor = successor.start();
    caught_up(&f).await;
    // The same batch plus more.
    f.write((5..15).map(|n| upsert(n, json!({ "n": n + 50 }))).collect())
        .await;
    caught_up(&f).await;
    release.notify_one();
    zombie.pause_renewals(false);
    // The zombie resumed its held commit (it wrote its manifest), and its
    // fenced CAS was refused, so it never reached the step after the CAS.
    wait_for("the zombie's manifest PUT", || {
        zombie_steps
            .lock()
            .unwrap()
            .contains(&CollectionCommitStep::AfterManifestPut)
    })
    .await;
    let task = TaskKey::new(f.ns, format!("link/{}", f.link));
    wait_for("the zombie's task to end", || {
        !zombie.running().contains(&task)
    })
    .await;
    assert!(
        !zombie_steps
            .lock()
            .unwrap()
            .contains(&CollectionCommitStep::AfterCas),
        "{:?}",
        zombie_steps.lock().unwrap()
    );
    zombie.stop().await;
    successor.stop().await;

    // The zombie's run ended fenced: its CAS was refused.
    assert!(
        zombie_runs
            .lock()
            .unwrap()
            .iter()
            .any(|run| run == "Err(Fenced)"),
        "{:?}",
        zombie_runs.lock().unwrap()
    );
    // Detached commits on one parent allocate the same row ids, so the
    // zombie's rows are told apart by the data files it added on top of its
    // parent (mainline version 1, as the collection had no commit yet): none
    // of them is in the live Lance version.
    let live = f.manifest().await.lance_version;
    let zombie_version = *zombie_versions.iter().next().unwrap();
    assert!(!zombie_versions.contains(&live));
    let zombie_added: BTreeSet<String> = data_files(&f, zombie_version)
        .await
        .difference(&data_files(&f, 1).await)
        .cloned()
        .collect();
    assert!(!zombie_added.is_empty());
    let live_files = data_files(&f, live).await;
    assert!(
        zombie_added.is_disjoint(&live_files),
        "{zombie_added:?} ∩ {live_files:?}"
    );
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// A task source that records how each of its runs ended.
struct Recording {
    inner: operon_link::LinkApplySource,
    runs: Arc<Mutex<Vec<String>>>,
}

struct RecordingTask {
    inner: Arc<dyn Task>,
    runs: Arc<Mutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl TaskSource for Recording {
    fn priority(&self) -> Priority {
        self.inner.priority()
    }

    async fn candidates(&self, meta: &dyn MetaStore) -> Result<Vec<Candidate>, TaskError> {
        let candidates = self.inner.candidates(meta).await?;
        Ok(candidates
            .into_iter()
            .map(|(key, task)| {
                let task: Arc<dyn Task> = Arc::new(RecordingTask {
                    inner: task,
                    runs: self.runs.clone(),
                });
                (key, task)
            })
            .collect())
    }
}

#[async_trait::async_trait]
impl Task for RecordingTask {
    async fn run(&self, ctx: TaskContext) -> Result<TaskOutcome, TaskError> {
        let result = self.inner.run(ctx).await;
        let summary = match &result {
            Err(TaskError::Fenced) => "Err(Fenced)".to_string(),
            other => format!("{other:?}"),
        };
        self.runs.lock().expect("lock").push(summary);
        result
    }
}

/// Waits until the link has applied the whole stream (a worker applies it).
async fn caught_up(f: &TargetFixture) {
    let deadline = Instant::now() + WAIT;
    while f.applied().await != f.high_watermarks().await {
        assert!(Instant::now() < deadline, "the link never caught up");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Review focus 1: a Lance version committed on the parent before the task
/// starts (by a crashed writer) is ignored.
#[tokio::test]
async fn an_orphan_lance_version_at_task_start_is_ignored() {
    let f = TargetFixture::start(tagged(), 3).await;
    f.write((0..5).map(|n| upsert(n, json!({ "n": n }))).collect())
        .await;
    let schema = f.schema().await;
    let parent = f.ctx.lance.ensure_created(f.ns, f.cid).await.unwrap();
    let bogus: Vec<Document> = (100..103)
        .map(|n| common::doc(key(n), json!({ "bogus": true })))
        .collect();
    let rows: Vec<NewRow<'_>> = bogus
        .iter()
        .map(|doc| NewRow {
            doc,
            partition: 0,
            offset: 0,
        })
        .collect();
    let fragments = LanceCommitter::write_fragments(
        &f.ctx.lance,
        &parent,
        to_record_batch(&schema, &rows).unwrap(),
    )
    .await
    .unwrap();
    let orphan = LanceCommitter::commit(
        &f.ctx.lance,
        &parent,
        lance::dataset::transaction::Operation::Append { fragments },
    )
    .await
    .unwrap();
    f.apply_all(&f.source(f.factory()), "w1").await;
    let manifest = f.manifest().await;
    assert_ne!(manifest.lance_version, orphan.manifest.version);
    let docs = f.snapshot().await.scan_all().await.unwrap();
    assert_eq!(docs.len(), 5);
    assert!(docs.iter().all(|d| !bogus.iter().any(|b| b.pk == d.pk)));
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// Review focus 2: another PK writer opens the index right after the CAS;
/// the task's PK write is fenced, so the commit (which landed) reports
/// `Fenced` and the task ends. The next run opens a new handle and repairs.
#[tokio::test]
async fn a_fenced_pk_writer_ends_the_task() {
    let f = TargetFixture::start(tagged(), 3).await;
    f.write((0..8).map(|n| upsert(n, json!({ "n": n }))).collect())
        .await;
    let (store, prefix) = (f.store.clone(), collection_pk_prefix(f.ns, f.cid));
    let fenced = Arc::new(AtomicBool::new(false));
    let hook: CollectionCommitHook = {
        let fenced = fenced.clone();
        Arc::new(move |at, _fence| {
            if at != CollectionCommitStep::AfterCas || fenced.swap(true, Ordering::SeqCst) {
                return futures::future::ready(()).boxed();
            }
            let (store, prefix) = (store.clone(), prefix.clone());
            async move {
                let outside = PkIndex::open(&store, &prefix, PkIndexConfig::default())
                    .await
                    .expect("open the pk index");
                outside.close().await.expect("close");
            }
            .boxed()
        })
    };
    let source = f.source(f.hooked_factory(hook));
    let results = f.run_once(&source, "w1").await;
    assert!(
        matches!(results[..], [(_, RunResult::Ran(Err(TaskError::Fenced)))]),
        "{results:?}"
    );
    assert_eq!(f.manifest().await.version, 1, "the commit landed");
    f.write((0..12).map(|n| upsert(n, json!({ "n": n + 1 }))).collect())
        .await;
    let results = f.run_once(&source, "w1").await;
    assert!(
        matches!(results[..], [(_, RunResult::Ran(Ok(_)))]),
        "{results:?}"
    );
    assert_eq!(f.manifest().await.version, 2);
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// Ruling 7: a cached handle whose watermark is not the parent's `applied`
/// (another writer committed since) is reopened and repaired.
#[tokio::test]
async fn a_cached_pk_handle_is_reopened_after_another_writer_committed() {
    let f = TargetFixture::start(tagged(), 3).await;
    let a = f.source(f.factory());
    let b = f.source(f.factory());
    f.write((0..6).map(|n| upsert(n, json!({ "n": n }))).collect())
        .await;
    assert!(matches!(
        f.run_once(&a, "w1").await[..],
        [(_, RunResult::Ran(Ok(_)))]
    ));
    f.write((0..3).map(|n| upsert(n, json!({ "n": n + 10 }))).collect())
        .await;
    assert!(matches!(
        f.run_once(&b, "w1").await[..],
        [(_, RunResult::Ran(Ok(_)))]
    ));
    f.write((0..6).map(|n| upsert(n, json!({ "n": n + 20 }))).collect())
        .await;
    let results = f.run_once(&a, "w1").await;
    assert!(
        matches!(results[..], [(_, RunResult::Ran(Ok(_)))]),
        "{results:?}"
    );
    assert_eq!(f.manifest().await.version, 3);
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// Ruling 7: when the PK delta a repair needs is gone, the index is rebuilt
/// from the parent's Lance version: its keys are put, and keys it no longer
/// has (deleted by the commit the index missed) are removed.
#[tokio::test]
async fn a_pk_index_whose_delta_is_gone_is_rebuilt_from_lance() {
    let f = TargetFixture::start(tagged(), 3).await;
    lose_the_pk_delta(&f).await;
    // More upserts of the replaced keys: a stale index would leave their
    // old rows live.
    f.write(
        (0..10)
            .map(|n| upsert(n, json!({ "n": n + 200 })))
            .collect(),
    )
    .await;
    f.apply_all(&f.source(f.factory()), "w2").await;
    assert_verified(f.verify().await);
    assert_eq!(f.snapshot().await.scan_all().await.unwrap().len(), 10);
    f.shutdown().await;
}

/// Commits keys 0..10, then a batch that deletes and replaces keys whose
/// PK write is lost (a crash after the CAS), then deletes that commit's PK
/// delta: the next PK repair must rebuild from Lance.
async fn lose_the_pk_delta(f: &TargetFixture) {
    f.write((0..10).map(|n| upsert(n, json!({ "n": n }))).collect())
        .await;
    f.apply_all(&f.source(f.factory()), "w1").await;
    let mut ops: Vec<DocOp> = (0..4).map(|n| DocOp::Delete(key(n))).collect();
    ops.extend((4..8).map(|n| upsert(n, json!({ "n": n + 100 }))));
    f.write(ops).await;
    crash_run(f, CollectionCommitStep::AfterCas).await;
    let manifest = f.manifest().await;
    assert_eq!(manifest.version, 2);
    assert_ne!(f.pk_watermark().await.applied, manifest.applied);
    f.store
        .delete(manifest.pk_delta.as_deref().expect("a pk delta"))
        .await
        .expect("delete the pk delta");
}

/// A rebuild of several writes first marks the index as rebuilding, so one
/// interrupted after a chunk is never trusted: the next opener rebuilds.
#[tokio::test]
async fn an_interrupted_pk_rebuild_is_never_trusted() {
    let f = TargetFixture::start(tagged(), 3).await;
    lose_the_pk_delta(&f).await;
    f.write(
        (0..10)
            .map(|n| upsert(n, json!({ "n": n + 200 })))
            .collect(),
    )
    .await;
    // Three entries per write; the task is dropped after the first chunk.
    let reached = Arc::new(AtomicBool::new(false));
    let hook: RebuildHook = {
        let reached = reached.clone();
        Arc::new(move |write| {
            if write == 1 {
                reached.store(true, Ordering::SeqCst);
                futures::future::pending::<()>().boxed()
            } else {
                futures::future::ready(()).boxed()
            }
        })
    };
    let factory =
        Arc::new(CollectionTargetFactory::new(f.ctx.clone()).with_rebuild_chunks(3, hook));
    crash_with(&f, factory, reached).await;
    assert_eq!(f.pk_watermark().await, PkWatermark::rebuilding());
    f.apply_all(&f.source(f.factory()), "w2").await;
    assert_verified(f.verify().await);
    assert_eq!(f.pk_watermark().await.applied, f.manifest().await.applied);
    f.shutdown().await;
}

/// Review focus 2: a target whose parent is stale (another writer committed
/// since its `load`) must not roll the PK index back to that parent: its
/// commit is a `Conflict` and the index keeps the newer watermark.
#[tokio::test]
async fn a_stale_parent_never_rolls_the_pk_index_back() {
    let f = TargetFixture::start(tagged(), 3).await;
    f.write((0..6).map(|n| upsert(n, json!({ "n": n }))).collect())
        .await;
    f.apply_all(&f.source(f.factory()), "w1").await;
    // Target A loads version 1 with no PK handle of its own.
    let a = f.factory();
    let target = a
        .open(&f.meta.client.clone().into(), &f.link().await)
        .unwrap();
    let state = target.load().await.unwrap();
    assert_eq!(state.version, 1);
    // Another factory commits version 2.
    f.write((0..3).map(|n| upsert(n, json!({ "n": n + 10 }))).collect())
        .await;
    f.apply_all(&f.source(f.factory()), "w2").await;
    let live = f.manifest().await;
    assert_eq!(live.version, 2);
    let watermark = f.pk_watermark().await;
    assert_eq!(
        (watermark.manifest_version, &watermark.applied),
        (2, &live.applied)
    );

    let batch = f.batch_after(&state).await;
    let fence = f.fence("w3").await;
    let result = target.commit(1, batch, &fence).await;
    assert!(matches!(result, Err(CommitError::Conflict)), "{result:?}");
    assert_eq!(
        f.pk_watermark().await,
        watermark,
        "the pk index was rolled back"
    );
    // A reloads and commits on top.
    f.write(vec![upsert(9, json!({ "n": 9 }))]).await;
    let state = target.load().await.unwrap();
    assert_eq!(state.version, 2);
    let batch = f.batch_after(&state).await;
    let result = target.commit(2, batch, &fence).await;
    assert!(matches!(result, Ok(3)), "{result:?}");
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// Ruling P34: a watermark at a version the parent's chain has, whose
/// offsets match no manifest of it, is not a stale parent (the version check
/// catches those) but unexpected history: the index is rebuilt, and the
/// commit succeeds instead of conflicting forever.
#[tokio::test]
async fn an_unmatched_watermark_is_rebuilt_not_a_conflict() {
    let f = TargetFixture::start(tagged(), 3).await;
    f.write((0..6).map(|n| upsert(n, json!({ "n": n }))).collect())
        .await;
    f.apply_all(&f.source(f.factory()), "w1").await;
    f.write((0..3).map(|n| upsert(n, json!({ "n": n + 10 }))).collect())
        .await;
    f.apply_all(&f.source(f.factory()), "w1").await;
    assert_eq!(f.manifest().await.version, 2);
    // Version 1, but offsets no manifest has, plus a key no row has.
    let bogus = PkWatermark {
        manifest_version: 1,
        applied: [(0, 12_345)].into(),
    };
    let index = PkIndex::open(
        &f.store,
        &collection_pk_prefix(f.ns, f.cid),
        PkIndexConfig::default(),
    )
    .await
    .unwrap();
    index
        .write(vec![
            (
                bytes::Bytes::from_static(operon_collection::PK_WATERMARK_KEY),
                Some(bogus.encode()),
            ),
            (
                bytes::Bytes::from(key(77).canonical()),
                Some(bytes::Bytes::copy_from_slice(&operon_collection::pk_value(
                    9_999,
                ))),
            ),
        ])
        .await
        .unwrap();
    index.close().await.unwrap();

    let target = f
        .factory()
        .open(&f.meta.client.clone().into(), &f.link().await)
        .unwrap();
    f.write((0..8).map(|n| upsert(n, json!({ "n": n + 20 }))).collect())
        .await;
    let state = target.load().await.unwrap();
    let batch = f.batch_after(&state).await;
    let fence = f.fence("w2").await;
    let result = target.commit(state.version, batch, &fence).await;
    assert!(matches!(result, Ok(3)), "{result:?}");
    assert_eq!(f.pk_watermark().await.applied, f.manifest().await.applied);
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// One random op over keys 0..12.
#[derive(Clone, Debug)]
enum RandomOp {
    Upsert(u64, i64),
    Patch { key: u64, n: i64, upsert: bool },
    Delete(u64),
}

impl RandomOp {
    fn op(&self) -> DocOp {
        match *self {
            RandomOp::Upsert(k, n) => upsert(k, json!({ "n": n, "tag": format!("t{}", n % 3) })),
            RandomOp::Patch { key: k, n, upsert } => {
                match patch(key(k), json!({ "m": { "n": n } })) {
                    DocOp::Patch {
                        pk,
                        mode,
                        source,
                        delete_keys,
                        vectors,
                        sparse_vectors,
                        ..
                    } => DocOp::Patch {
                        upsert: upsert.then(|| common::doc(pk.clone(), json!({ "n": n }))),
                        pk,
                        mode,
                        source,
                        delete_keys,
                        vectors,
                        sparse_vectors,
                    },
                    other => other,
                }
            }
            RandomOp::Delete(k) => DocOp::Delete(key(k)),
        }
    }
}

fn random_op() -> impl Strategy<Value = RandomOp> {
    prop_oneof![
        3 => (0u64..12, 0i64..1000).prop_map(|(k, n)| RandomOp::Upsert(k, n)),
        2 => (0u64..12, 0i64..1000, any::<bool>())
            .prop_map(|(key, n, upsert)| RandomOp::Patch { key, n, upsert }),
        1 => (0u64..12).prop_map(RandomOp::Delete),
    ]
}

/// 3–6 batches, and where to crash: a batch index and a step.
fn workload() -> impl Strategy<Value = (Vec<Vec<RandomOp>>, usize, usize)> {
    prop::collection::vec(prop::collection::vec(random_op(), 1..10), 3..=6).prop_flat_map(
        |batches| {
            let n = batches.len();
            (Just(batches), 0..n, 0..STEPS.len())
        },
    )
}

async fn random_batches(batches: Vec<Vec<RandomOp>>, crash_batch: usize, step: usize) {
    let f = TargetFixture::start(tagged(), 3).await;
    let mut source = f.source(f.factory());
    for (i, batch) in batches.iter().enumerate() {
        f.write(batch.iter().map(RandomOp::op).collect()).await;
        if i == crash_batch {
            crash_run(&f, STEPS[step]).await;
            source = f.source(f.factory());
        }
        f.apply_all(&source, "w2").await;
        assert_verified(f.verify().await);
    }
    f.shutdown().await;
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 32, ..ProptestConfig::default() })]

    /// Every batch of random ops applies exactly once, whichever commit
    /// crashes at whichever step.
    #[test]
    fn random_batches_with_random_crash_points_verify(
        (batches, crash_batch, step) in workload()
    ) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(random_batches(batches, crash_batch, step));
    }
}
