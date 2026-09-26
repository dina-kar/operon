//! The hot artifact build task (plan M1.3 Task 5): when a column gets an
//! artifact, what it holds, when it is rebuilt, and how its commit rebases,
//! loses races, crashes and is fenced.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use futures::FutureExt;
use operon_collection::{
    CollectionSchema, CommitKind, DocOp, DynamicMapping, FieldKind, HotArtifactRef,
    LanceCompactionSource, MaintenanceConfig, PrimaryKey, SplitMergeSource,
};
use operon_common::meta::{HotConfig, Lease, MetaStore};
use operon_hnsw::{Distance, HnswParams, PayloadField, PayloadKind};
use operon_hot::{
    BuildDecision, HNSW_KIND, HotBuildConfig, HotBuildHook, HotBuildStep, decide, download,
    effective_hot, payload_fields, promote_lease_key,
};
use operon_log::gc::{GcConfig, GcSource};
use operon_meta::{Clock, SystemClock};
use operon_quickwit::merge_policy::StableLogMergePolicyConfig;
use operon_worker::{RunResult, TaskError, TaskOutcome, run_once};
use serde_json::json;
use tempfile::TempDir;

use crate::common::{
    DIM, Fixture, TTL, WAIT, build_once, doc, docs, field, hot_schema, live_rows, run_all, vector,
};

const VECTORS: HotConfig = HotConfig {
    vectors: true,
    text: false,
    fragments: false,
};

/// The live manifest's HNSW artifact of `_vector_0`.
fn artifact(manifest: &operon_collection::CollectionManifest) -> Option<HotArtifactRef> {
    manifest
        .hot_artifacts
        .iter()
        .find(|a| a.kind == HNSW_KIND && a.column == "_vector_0")
        .cloned()
}

async fn wait_for(what: &str, check: impl Fn() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !check() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// The fence a held build was given.
type KeptFence = Arc<std::sync::Mutex<Option<operon_meta::Fence>>>;

/// A hook that holds the first build at `step` until `release` is notified,
/// says when it got there, and keeps the fence it was given.
fn hold_at(
    step: HotBuildStep,
) -> (
    HotBuildHook,
    Arc<AtomicBool>,
    Arc<tokio::sync::Notify>,
    KeptFence,
) {
    let reached = Arc::new(AtomicBool::new(false));
    let release = Arc::new(tokio::sync::Notify::new());
    let fence = Arc::new(std::sync::Mutex::new(None));
    let (flag, gate, kept) = (reached.clone(), release.clone(), fence.clone());
    let hook: HotBuildHook = Arc::new(move |at, f| {
        if at == step && !flag.load(Ordering::SeqCst) {
            *kept.lock().expect("fence") = Some(f);
            flag.store(true, Ordering::SeqCst);
            let gate = gate.clone();
            async move { gate.notified().await }.boxed()
        } else {
            futures::future::ready(()).boxed()
        }
    });
    (hook, reached, release, fence)
}

/// Downloads the live artifact of `_vector_0` into a fresh directory.
async fn downloaded(f: &Fixture) -> (operon_hot::ArtifactDescriptor, roaring::RoaringTreemap) {
    let artifact = artifact(&f.manifest().await).expect("an artifact");
    let dir = TempDir::new().expect("dir");
    download(&f.store, &artifact.prefix, dir.path(), 4)
        .await
        .expect("download")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pinned_vector_gets_an_artifact() {
    let f = Fixture::start().await;
    f.commit(docs(0..500)).await;
    let s = f.manifest().await.version;
    f.pin_vectors().await;
    let source = f.source();
    assert_eq!(
        build_once(&f, &source).await.expect("build"),
        TaskOutcome::Done
    );
    let manifest = f.manifest().await;
    assert_eq!(manifest.version, s + 1);
    assert_eq!(manifest.kind, CommitKind::Maintenance);
    let artifact = artifact(&manifest).expect("an artifact");
    assert_eq!(
        (
            artifact.kind.as_str(),
            artifact.column.as_str(),
            artifact.source_version
        ),
        (HNSW_KIND, "_vector_0", s)
    );
    let (descriptor, covered) = downloaded(&f).await;
    assert_eq!(descriptor.points, 500);
    assert_eq!(descriptor.source_version, s);
    assert_eq!(covered, live_rows(&f).await);
    // The next run finds it current.
    assert_eq!(
        build_once(&f, &source).await.expect("run"),
        TaskOutcome::Idle
    );
    assert_eq!(f.manifest().await.version, s + 1);
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rows_without_the_vector_are_scanned_but_not_points() {
    let f = Fixture::start().await;
    f.commit((0..300).map(|k| doc(k, k % 3 != 0)).collect())
        .await;
    f.pin_vectors().await;
    build_once(&f, &f.source()).await.expect("build");
    let (descriptor, covered) = downloaded(&f).await;
    assert_eq!(descriptor.scanned, 300);
    assert_eq!(covered.len(), 300);
    assert_eq!(descriptor.points, 200);
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unpinned_collection_gets_no_artifact() {
    let f = Fixture::start().await;
    f.commit(docs(0..20)).await;
    assert!(run_all(&f, &f.source(), "builder").await.is_empty());
    assert!(f.manifest().await.hot_artifacts.is_empty());
    // Pinning text only builds nothing either.
    f.pin(
        f.cid,
        HotConfig {
            text: true,
            ..HotConfig::default()
        },
    )
    .await;
    assert!(run_all(&f, &f.source(), "builder").await.is_empty());
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pin_all_builds_every_collection() {
    let f = Fixture::start().await;
    let other = f.create("other", hot_schema()).await;
    f.write_to(f.coll, docs(0..30)).await;
    f.write_to(other, docs(100..140)).await;
    f.apply(&[f.coll, other]).await;
    let source = f.source_with(HotBuildConfig {
        pin_all: true,
        ..f.config()
    });
    let results = run_all(&f, &source, "builder").await;
    assert_eq!(results.len(), 2, "{results:?}");
    for (key, result) in results {
        assert!(
            matches!(result, RunResult::Ran(Ok(TaskOutcome::Done))),
            "{key}: {result:?}"
        );
    }
    assert!(artifact(&f.manifest().await).is_some());
    assert!(artifact(&f.manifest_of(other.cid).await).is_some());
    // Nothing was written to the catalog.
    assert_eq!(
        f.meta
            .client
            .collection_hot(operon_meta::Consistency::Linearizable, f.ns, f.cid)
            .await
            .expect("read"),
        HotConfig::default()
    );
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_promotion_lease_builds_like_a_pin() {
    let f = Fixture::start().await;
    f.commit(docs(0..30)).await;
    f.meta
        .client
        .acquire_lease(&promote_lease_key(f.ns, f.cid), "node-1;vectors,text", TTL)
        .await
        .expect("promote");
    assert_eq!(
        build_once(&f, &f.source()).await.expect("build"),
        TaskOutcome::Done
    );
    assert!(artifact(&f.manifest().await).is_some());
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_expired_promotion_lease_does_not() {
    let f = Fixture::start().await;
    f.commit(docs(0..30)).await;
    f.meta
        .client
        .acquire_lease(
            &promote_lease_key(f.ns, f.cid),
            "node-1;vectors",
            Duration::from_secs(1),
        )
        .await
        .expect("promote");
    f.clock.advance(Duration::from_secs(2));
    assert!(run_all(&f, &f.source(), "builder").await.is_empty());
    // A lease naming other structures does not build either.
    f.meta
        .client
        .acquire_lease(&promote_lease_key(f.ns, f.cid), "node-2;text", TTL)
        .await
        .expect("promote");
    assert!(run_all(&f, &f.source(), "builder").await.is_empty());
    f.shutdown().await;
}

#[test]
fn effective_hot_ors_pins_pin_all_and_held_leases() {
    let lease = |owner: &str, deadline_ms| Lease {
        epoch: 1,
        owner: Some(owner.to_string()),
        deadline_ms,
    };
    let none = HotConfig::default();
    assert_eq!(effective_hot(none, None, false, 0), none);
    assert_eq!(effective_hot(VECTORS, None, false, 0), VECTORS);
    assert_eq!(
        effective_hot(none, None, true, 0),
        HotConfig {
            vectors: true,
            text: true,
            fragments: false
        }
    );
    let held = lease("7;text, fragments", 100);
    assert_eq!(
        effective_hot(VECTORS, Some(&held), false, 50),
        HotConfig {
            vectors: true,
            text: true,
            fragments: true
        }
    );
    assert_eq!(effective_hot(none, Some(&held), false, 100), none);
    let released = Lease {
        owner: None,
        ..held.clone()
    };
    assert_eq!(effective_hot(none, Some(&released), false, 50), none);
    assert_eq!(effective_hot(none, Some(&lease("7", 100)), false, 50), none);
}

#[test]
fn payload_fields_follow_the_schema() {
    let mut schema = CollectionSchema::new(
        vec![
            field("tag", FieldKind::Keyword),
            field("n", FieldKind::I64),
            field(
                "t",
                FieldKind::Text {
                    analyzer: "standard".to_string(),
                    positions: false,
                },
            ),
            field("payload", FieldKind::Json),
        ],
        vec![vector(DIM)],
        DynamicMapping::Ignore,
    );
    let fields = payload_fields(&schema, &schema.vectors[0], 8);
    assert_eq!(
        fields,
        vec![
            (
                0,
                PayloadField {
                    key: "f0".to_string(),
                    kind: PayloadKind::Keyword
                }
            ),
            (
                1,
                PayloadField {
                    key: "f1".to_string(),
                    kind: PayloadKind::Integer
                }
            ),
        ]
    );
    assert_eq!(payload_fields(&schema, &schema.vectors[0], 1).len(), 1);
    schema.fields[0].indexed = false;
    assert_eq!(payload_fields(&schema, &schema.vectors[0], 8)[0].0, 1);
    schema.vectors[0].hnsw = HnswParams {
        payload_m: Some(0),
        ..HnswParams::default()
    };
    assert!(payload_fields(&schema, &schema.vectors[0], 8).is_empty());
    let spec = operon_hot::build_spec(&schema, 0, &HotBuildConfig::new(std::path::Path::new("/x")));
    assert_eq!((spec.dim, spec.distance), (DIM, Distance::Cosine));
    assert!(spec.payload_fields.is_empty());
}

/// Maintenance that keeps an artifact current: a split merge, a Lance
/// compaction, and a link commit with only dead letters.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_current_artifact_is_not_rebuilt() {
    let f = Fixture::start().await;
    for c in 0..4 {
        f.commit(docs(c * 10..c * 10 + 10)).await;
    }
    f.pin_vectors().await;
    let source = f.source();
    build_once(&f, &source).await.expect("build");
    let s = artifact(&f.manifest().await)
        .expect("artifact")
        .source_version;
    let assert_current = |what: &'static str| {
        let f = &f;
        async move {
            let (path, manifest) = f.live().await;
            let next_row_id = f
                .snapshot()
                .await
                .dataset()
                .expect("dataset")
                .manifest
                .next_row_id;
            let decision = decide(
                &f.ctx,
                (&path, &manifest),
                "_vector_0",
                next_row_id,
                &f.config(),
                f.ctx.meta.now_ms(),
            )
            .await
            .expect("decide");
            assert_eq!(decision, BuildDecision::Current, "after {what}");
        }
    };

    let maintenance = MaintenanceConfig {
        merge_policy: StableLogMergePolicyConfig {
            min_level_num_docs: 100,
            merge_factor: 2,
            max_merge_factor: 4,
            maturation_period: Duration::from_hours(48),
        },
        split_num_docs_target: 10_000,
        compaction_target_rows: 400,
        compaction_min_small_fragments: 3,
        poll_interval: Duration::ZERO,
        ..MaintenanceConfig::default()
    };
    let before = f.manifest().await.version;
    let merges = SplitMergeSource::new(f.ctx.clone(), maintenance.clone());
    run_once(&f.meta.client, "merger", TTL, &merges)
        .await
        .expect("merge");
    let merged = f.manifest().await;
    assert!(merged.version > before, "no merge committed");
    assert_eq!(merged.kind, CommitKind::Maintenance);
    assert_current("a merge").await;

    let before = f.manifest().await.version;
    let compaction = LanceCompactionSource::new(f.ctx.clone(), maintenance);
    run_once(&f.meta.client, "compactor", TTL, &compaction)
        .await
        .expect("compaction");
    let compacted = f.manifest().await;
    assert!(compacted.version > before, "no compaction committed");
    assert_eq!(compacted.kind, CommitKind::Maintenance);
    assert_current("a compaction").await;

    // A document that fails its schema at apply time: a dead letter only.
    let bad = match json!({ "tag": "x", "n": "not a number" }) {
        serde_json::Value::Object(map) => map,
        _ => unreachable!(),
    };
    let op = DocOp::Upsert(operon_collection::Document {
        pk: PrimaryKey::U64(999),
        source: bad,
        vectors: Default::default(),
        sparse_vectors: Default::default(),
    });
    f.append_op(0, &op).await;
    f.apply(&[f.coll]).await;
    let dead = f.manifest().await;
    assert_eq!(dead.kind, CommitKind::LinkApply);
    assert!(
        dead.pk_delta.is_none() && dead.dead_letters.is_some(),
        "{dead:?}"
    );
    assert_current("a dead-letter-only link commit").await;

    let version = f.manifest().await.version;
    assert_eq!(
        build_once(&f, &source).await.expect("run"),
        TaskOutcome::Idle
    );
    assert_eq!(f.manifest().await.version, version);
    assert_eq!(
        artifact(&f.manifest().await)
            .expect("artifact")
            .source_version,
        s
    );
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_artifact_is_rebuilt_past_the_insert_threshold() {
    let f = Fixture::start().await;
    f.commit(docs(0..100)).await;
    f.pin_vectors().await;
    let source = f.source_with(HotBuildConfig {
        rebuild_min_inserted: 50,
        rebuild_inserted_ppm: 0,
        ..f.config()
    });
    build_once(&f, &source).await.expect("build");
    let first = artifact(&f.manifest().await).expect("artifact");
    f.commit(docs(100..149)).await;
    assert_eq!(
        build_once(&f, &source).await.expect("run"),
        TaskOutcome::Idle
    );
    assert_eq!(artifact(&f.manifest().await), Some(first.clone()));
    f.commit(docs(149..150)).await;
    let s = f.manifest().await.version;
    assert_eq!(
        build_once(&f, &source).await.expect("run"),
        TaskOutcome::Done
    );
    let second = artifact(&f.manifest().await).expect("artifact");
    assert_eq!(second.source_version, s);
    assert_ne!(second.prefix, first.prefix);
    assert_eq!(downloaded(&f).await.0.points, 150);
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_artifact_is_rebuilt_after_max_staleness() {
    let f = Fixture::start().await;
    f.commit(docs(0..20)).await;
    f.pin_vectors().await;
    let source = f.source_with(HotBuildConfig {
        rebuild_min_inserted: 1_000,
        rebuild_max_staleness: Duration::from_secs(60),
        ..f.config()
    });
    build_once(&f, &source).await.expect("build");
    f.commit(docs(20..21)).await;
    assert_eq!(
        build_once(&f, &source).await.expect("run"),
        TaskOutcome::Idle
    );
    f.clock.advance(Duration::from_secs(61));
    let s = f.manifest().await.version;
    assert_eq!(
        build_once(&f, &source).await.expect("run"),
        TaskOutcome::Done
    );
    assert_eq!(
        artifact(&f.manifest().await)
            .expect("artifact")
            .source_version,
        s
    );
    f.shutdown().await;
}

/// The build is held after its artifact PUT while two link commits land:
/// its CAS conflicts, and the rebase commits the artifact of the older
/// manifest on top of both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_artifact_commit_rebases_over_link_commits() {
    let f = Fixture::start().await;
    f.commit(docs(0..50)).await;
    f.pin_vectors().await;
    let s = f.manifest().await.version;
    let (hook, reached, release, _) = hold_at(HotBuildStep::AfterArtifactPut);
    let source = f.source().with_hook(hook);
    let meta = f.meta.client.clone();
    let run = tokio::spawn(async move { run_once(&meta, "builder", TTL, &source).await });
    wait_for("the artifact PUT", || reached.load(Ordering::SeqCst)).await;
    f.commit(docs(50..60)).await;
    f.commit(vec![DocOp::Delete(PrimaryKey::U64(3))]).await;
    let links = f.manifest().await;
    release.notify_one();
    let results = run.await.expect("join").expect("run");
    assert!(
        matches!(
            results.as_slice(),
            [(_, RunResult::Ran(Ok(TaskOutcome::Done)))]
        ),
        "{results:?}"
    );
    let manifest = f.manifest().await;
    assert_eq!(manifest.version, links.version + 1);
    assert_eq!(manifest.live_doc_count, 59);
    assert_eq!(manifest.applied, links.applied);
    assert_eq!(artifact(&manifest).expect("artifact").source_version, s);
    let problems = f.verify().await;
    assert!(problems.is_empty(), "{problems:#?}");
    f.shutdown().await;
}

/// Build A (from manifest s₁) is held after its artifact PUT; its task
/// lease moves to build B, which builds from a newer manifest and commits.
/// A then finds the newer artifact and abandons.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_older_build_never_replaces_a_newer_artifact() {
    let f = Fixture::start().await;
    f.commit(docs(0..40)).await;
    f.pin_vectors().await;
    let config = HotBuildConfig {
        rebuild_min_inserted: 1,
        rebuild_inserted_ppm: 0,
        ..f.config()
    };
    let (hook, reached, release, fence) = hold_at(HotBuildStep::AfterArtifactPut);
    let a = f.source_with(config.clone()).with_hook(hook);
    let meta = f.meta.client.clone();
    let run_a = tokio::spawn(async move { run_once(&meta, "worker-a", TTL, &a).await });
    wait_for("A's artifact PUT", || reached.load(Ordering::SeqCst)).await;
    f.commit(docs(40..60)).await;
    let fence = fence.lock().expect("fence").clone().expect("a fence");
    f.meta
        .client
        .release_lease(&fence.lease, "worker-a", fence.epoch)
        .await
        .expect("release A's lease");
    let b = f.source_with(config);
    assert_eq!(build_once(&f, &b).await.expect("B"), TaskOutcome::Done);
    let newer = artifact(&f.manifest().await).expect("B's artifact");
    let committed = f.manifest().await;
    release.notify_one();
    let results = run_a.await.expect("join").expect("run");
    assert!(
        matches!(
            results.as_slice(),
            [(_, RunResult::Ran(Ok(TaskOutcome::Done)))]
        ),
        "{results:?}"
    );
    assert_eq!(f.manifest().await, committed);
    assert_eq!(artifact(&f.manifest().await), Some(newer));
    f.shutdown().await;
}

/// The build is dropped after its artifact PUT (a crash): the manifest is
/// unchanged, and GC past its grace deletes every object of the artifact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_after_the_artifact_put_leaves_an_orphan_for_gc() {
    const GRACE: Duration = Duration::from_secs(60);
    let f = Fixture::start().await;
    f.commit(docs(0..30)).await;
    f.pin_vectors().await;
    let before = f.manifest().await;
    let (hook, reached, _release, _) = hold_at(HotBuildStep::AfterArtifactPut);
    let source = f.source().with_hook(hook);
    let meta = f.meta.client.clone();
    let crashed = tokio::spawn(async move {
        let _ = run_once(&meta, "crashed", TTL, &source).await;
    });
    wait_for("the artifact PUT", || reached.load(Ordering::SeqCst)).await;
    crashed.abort();
    let _ = crashed.await;
    assert_eq!(f.manifest().await, before);
    let hot = format!("ns/{}/collections/{}/hot/", f.ns, f.cid);
    let orphans = f.store.list(&hot).await.expect("list");
    assert!(!orphans.is_empty());

    let gc = GcSource::with_roots(
        f.store.clone(),
        GcConfig {
            grace: GRACE,
            ..GcConfig::default()
        },
        vec![Arc::new(operon_collection::CollectionGcRoots::new(
            f.ctx.clone(),
        ))],
    );
    // Young objects stay.
    gc.run_once(&f.meta.client, "gc").await.expect("gc");
    assert_eq!(f.store.list(&hot).await.expect("list").len(), orphans.len());
    let now = f.clock.now_ms().max(SystemClock.now_ms());
    f.clock.set(now + GRACE.as_millis() as u64 + 1_000);
    gc.run_once(&f.meta.client, "gc").await.expect("gc");
    assert!(f.store.list(&hot).await.expect("list").is_empty());
    f.shutdown().await;
}

/// The build's lease is taken over after its manifest PUT: its CAS is
/// refused as `Fenced` and the manifest is unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fenced_build_changes_nothing() {
    let f = Fixture::start().await;
    f.commit(docs(0..30)).await;
    f.pin_vectors().await;
    let before = f.manifest().await;
    let meta = f.meta.client.clone();
    let taken = Arc::new(AtomicBool::new(false));
    let flag = taken.clone();
    let hook: HotBuildHook = Arc::new(move |at, fence| {
        let meta = meta.clone();
        let flag = flag.clone();
        async move {
            if at == HotBuildStep::AfterManifestPut && !flag.swap(true, Ordering::SeqCst) {
                meta.release_lease(&fence.lease, "builder", fence.epoch)
                    .await
                    .expect("release");
                meta.acquire_lease(&fence.lease, "thief", TTL)
                    .await
                    .expect("take over");
            }
        }
        .boxed()
    });
    let source = f.source().with_hook(hook);
    let result = build_once(&f, &source).await;
    assert!(matches!(result, Err(TaskError::Fenced)), "{result:?}");
    assert!(taken.load(Ordering::SeqCst));
    assert_eq!(f.manifest().await, before);
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_work_dir_is_cleared_at_start() {
    let f = Fixture::start().await;
    let config = f.config();
    let leftover = config
        .work_dir
        .join("7")
        .join("0")
        .join("01ARZ3NDEKTSV4RRFFQ69G5FAV");
    std::fs::create_dir_all(&leftover).expect("mkdir");
    std::fs::write(leftover.join("graph.bin"), b"half a build").expect("write");
    let source = f.source_with(config.clone());
    assert!(config.work_dir.is_dir());
    assert_eq!(
        std::fs::read_dir(&config.work_dir)
            .expect("read dir")
            .count(),
        0
    );
    // A run leaves nothing behind either.
    f.commit(docs(0..10)).await;
    f.pin_vectors().await;
    build_once(&f, &source).await.expect("build");
    let mut left = Vec::new();
    for entry in walk(&config.work_dir) {
        left.push(entry);
    }
    assert!(left.iter().all(|p| p.is_dir()), "{left:?}");
    f.shutdown().await;
}

fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).expect("read dir") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            out.extend(walk(&path));
        }
        out.push(path);
    }
    out
}

/// The qdrant-edge engine's artifact survives publish and download: opened
/// from a fresh directory, 50 self-queries find themselves first.
#[cfg(feature = "hnsw")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_qdrant_engine_artifact_round_trips() {
    use operon_hnsw::{HnswEngine, IdFilter, QdrantEngine, SearchParams};

    let f = Fixture::start().await;
    f.commit(docs(0..300)).await;
    f.pin_vectors().await;
    let source = f.source_over(f.config(), Arc::new(QdrantEngine));
    assert_eq!(
        build_once(&f, &source).await.expect("build"),
        TaskOutcome::Done
    );
    let artifact = artifact(&f.manifest().await).expect("artifact");
    let dir = TempDir::new().expect("dir");
    let (descriptor, _) = download(&f.store, &artifact.prefix, dir.path(), 4)
        .await
        .expect("download");
    assert_eq!(descriptor.engine, QdrantEngine.name());
    let index = QdrantEngine
        .open(&descriptor.spec, dir.path())
        .expect("open");
    assert_eq!(index.len(), 300);
    let snapshot = f.snapshot().await;
    let docs = snapshot.scan_all().await.expect("scan");
    for doc in docs.iter().take(50) {
        let query = &doc.vectors[""];
        let hits = index
            .search(query, 1, IdFilter::All, SearchParams::default())
            .expect("search");
        assert_eq!(hits.first().map(|(id, _)| *id), Some(doc.row_id));
    }
    f.shutdown().await;
}
