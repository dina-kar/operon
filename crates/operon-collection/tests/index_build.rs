//! Index builds (plan M1.1 Task 11; Ruling 3): vector indexes (IVF_PQ by
//! default, IVF_RQ, IVF_HNSW_SQ) grow by delta segments with periodic full
//! rebuilds, and the `_pk` BTREE, each built by a worker task and committed
//! under the collection manifest by the fenced, freshness-checked CAS.
//!
//! Lance's 8-bit PQ needs 256 training rows (controller ruling P1), so the
//! plan's delta sizes (+150 docs) are raised to +300.

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use arrow_array::{Array, BinaryArray, Float32Array};
use common::{TargetFixture, WAIT, doc, schema, vector};
use futures::FutureExt;
use lance::Dataset;
use lance::index::DatasetIndexExt;
use operon_collection::{
    CollectionCommitHook, CollectionCommitStep, CollectionConfig, CollectionSchema, CommitKind,
    Distance, DocOp, DynamicMapping, HnswParams, INDEX_TASK_PREFIX, IndexBuildSource, IndexWork,
    PK_COLUMN, PK_INDEX_NAME, PrimaryKey, VectorIndexKind, VectorIndexSpec, VectorSpec,
    plan_index_work, vector_column, vector_index_name,
};
use operon_worker::{RunResult, TaskError, TaskOutcome, run_once};
use serde_json::json;

const DIM: u32 = 8;

/// The lease TTL of an index run.
const TTL: Duration = Duration::from_secs(30);

/// The lease TTL of a run that crashes: short, so a successor soon takes over.
const CRASHED_TTL: Duration = Duration::from_millis(300);

/// The plan's test configuration; every poll proposes the collection.
fn config() -> CollectionConfig {
    CollectionConfig {
        index_min_rows: 200,
        index_delta_min_rows: 100,
        index_max_segments: 2,
        index_poll_interval: Duration::ZERO,
        ..CollectionConfig::default()
    }
}

/// One Cosine vector `v` of dimension 8 with the default (`Auto`) index.
fn with_vector() -> CollectionSchema {
    CollectionSchema::new(vec![], vec![vector("v", DIM)], DynamicMapping::Ignore)
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

/// An upsert of key `n`, with `vectors` (name → value).
fn upsert_with(n: u64, vectors: &[&str]) -> DocOp {
    let mut d = doc(PrimaryKey::U64(n), json!({ "n": n }));
    for name in vectors {
        d.vectors.insert(name.to_string(), vector_of(n));
    }
    DocOp::Upsert(d)
}

/// Writes `ops` and applies them (one link commit, so one new fragment).
async fn write_applied(f: &TargetFixture, ops: Vec<DocOp>) {
    f.write(ops).await;
    let source = f.source(f.factory());
    f.apply_all(&source, "linker").await;
}

fn outcome(results: Vec<(operon_worker::TaskKey, RunResult)>) -> Result<TaskOutcome, TaskError> {
    let [(key, RunResult::Ran(result))] = <[_; 1]>::try_from(results).expect("one task") else {
        panic!("the index task did not run");
    };
    assert!(key.key.starts_with(INDEX_TASK_PREFIX), "{key}");
    result
}

/// Runs index tasks until one ends `Idle`; every run must succeed.
async fn index_all(f: &TargetFixture, source: &IndexBuildSource) {
    for _ in 0..20 {
        let results = run_once(&f.meta.client, "indexer", TTL, source)
            .await
            .expect("run");
        match outcome(results) {
            Ok(TaskOutcome::Idle) => return,
            Ok(_) => {}
            Err(err) => panic!("index run failed: {err}"),
        }
    }
    panic!("the index task never went idle");
}

/// The live dataset.
async fn live_dataset(f: &TargetFixture) -> Arc<Dataset> {
    f.snapshot()
        .await
        .dataset()
        .cloned()
        .expect("a lance dataset")
}

/// The `k` nearest keys to `query` on vector column `column`, probing
/// `nprobes` partitions.
async fn nearest(
    dataset: &Dataset,
    column: &str,
    query: &[f32],
    k: usize,
    nprobes: usize,
) -> Vec<PrimaryKey> {
    nearest_refined(dataset, column, query, k, nprobes, None).await
}

/// [`nearest`], rescoring `refine` × `k` candidates with the full vectors.
async fn nearest_refined(
    dataset: &Dataset,
    column: &str,
    query: &[f32],
    k: usize,
    nprobes: usize,
    refine: Option<u32>,
) -> Vec<PrimaryKey> {
    let query = Float32Array::from(query.to_vec());
    let mut scanner = dataset.scan();
    scanner
        .nearest(column, &query, k)
        .expect("nearest")
        .nprobes(nprobes);
    if let Some(factor) = refine {
        scanner.refine(factor);
    }
    scanner.project(&[PK_COLUMN]).expect("project");
    let batch = scanner.try_into_batch().await.expect("search");
    let pks = batch
        .column_by_name(PK_COLUMN)
        .expect("pk")
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("binary")
        .clone();
    (0..pks.len())
        .map(|i| PrimaryKey::from_canonical(pks.value(i)).expect("a primary key"))
        .collect()
}

/// Candidates rescored with the full vectors per result: PQ and RQ codes
/// are approximate and their training is random, so an unrefined top 1 is
/// not deterministic (every returned score is exact, overview R12).
const REFINE: u32 = 20;

/// `n`'s own vector finds `n` first.
async fn finds_itself(dataset: &Dataset, column: &str, n: u64, nprobes: usize) {
    let found = nearest_refined(dataset, column, &vector_of(n), 1, nprobes, Some(REFINE)).await;
    assert_eq!(found, vec![PrimaryKey::U64(n)], "doc {n}");
}

/// The fragment ids the segments named `name` cover.
fn covered(indices: &[lance_table::format::IndexMetadata], name: &str) -> BTreeSet<u32> {
    indices
        .iter()
        .filter(|index| index.name == name)
        .flat_map(|index| index.fragment_bitmap.clone().unwrap_or_default())
        .collect()
}

fn fragment_ids(dataset: &Dataset) -> BTreeSet<u32> {
    dataset
        .manifest
        .fragments
        .iter()
        .map(|fragment| u32::try_from(fragment.id).expect("a u32 fragment id"))
        .collect()
}

/// `round(sqrt(rows))`, the default partition count.
fn partitions(rows: u64) -> usize {
    (rows as f64).sqrt().round() as usize
}

async fn plan(f: &TargetFixture) -> Vec<IndexWork> {
    let snapshot = f.snapshot().await;
    let dataset = snapshot.dataset().expect("a lance dataset");
    let indices = dataset.load_indices().await.expect("indices");
    plan_index_work(
        &snapshot.collection().schema,
        snapshot.manifest(),
        dataset,
        &indices,
        &f.ctx.config,
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

/// A hook that holds the first commit at `step` until `release` is
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

/// The index directories under the dataset's `_indices/`.
async fn index_dirs(f: &TargetFixture) -> BTreeSet<String> {
    let prefix = format!("ns/{}/collections/{}/lance/_indices/", f.ns, f.cid);
    f.store
        .list(&prefix)
        .await
        .expect("list")
        .into_iter()
        .filter_map(|info| {
            info.path
                .strip_prefix(&prefix)?
                .split('/')
                .next()
                .map(str::to_string)
        })
        .collect()
}

/// 100 docs are too few; at 300 one IVF_PQ segment is built, recorded in the
/// manifest, and serves ANN search.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_vector_index_is_built_once_enough_rows_exist() {
    let f = TargetFixture::start_with(with_vector(), 2, config()).await;
    let source = IndexBuildSource::new(f.ctx.clone());
    write_applied(&f, (0..100).map(|n| upsert_with(n, &["v"])).collect()).await;
    assert_eq!(plan(&f).await, vec![]);
    index_all(&f, &source).await;
    let before = f.manifest().await;
    assert!(before.vector_indexes.is_empty());

    write_applied(&f, (100..300).map(|n| upsert_with(n, &["v"])).collect()).await;
    assert_eq!(plan(&f).await, vec![IndexWork::VectorFull { vector: 0 }]);
    index_all(&f, &source).await;
    let manifest = f.manifest().await;
    assert_eq!(manifest.kind, CommitKind::IndexBuild);
    assert_eq!(manifest.pk_delta, None);
    assert_eq!(manifest.dead_letters, None);
    assert_eq!(manifest.live_doc_count, 300);
    let dataset = live_dataset(&f).await;
    let [segment] = manifest.vector_indexes.as_slice() else {
        panic!("one segment: {:?}", manifest.vector_indexes);
    };
    assert_eq!(segment.kind, VectorIndexKind::IvfPq);
    assert_eq!(segment.index_name, vector_index_name(0));
    assert_eq!(segment.index_name, "vec_0");
    assert_eq!(segment.column, vector_column(0));
    assert_eq!(segment.vector, "v");
    assert_eq!(segment.indexed_row_ids_upto, dataset.manifest.next_row_id);
    let indices = dataset.load_indices().await.unwrap();
    let uuids: Vec<String> = indices.iter().map(|i| i.uuid.to_string()).collect();
    assert_eq!(uuids, vec![segment.lance_index_uuid.clone()]);
    assert_eq!(plan(&f).await, vec![]);

    let column = vector_column(0);
    let explained = {
        let query = Float32Array::from(vector_of(7));
        let mut scanner = dataset.scan();
        scanner.nearest(&column, &query, 1).unwrap();
        scanner.explain_plan(true).await.unwrap()
    };
    assert!(explained.contains("ANN"), "{explained}");
    for n in [0, 7, 150, 299] {
        finds_itself(&dataset, &column, n, partitions(300)).await;
    }
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// Past the delta threshold a new segment covers exactly the unindexed
/// fragments; past `index_max_segments` a full rebuild replaces them all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delta_segment_covers_new_rows() {
    let f = TargetFixture::start_with(with_vector(), 2, config()).await;
    let source = IndexBuildSource::new(f.ctx.clone());
    write_applied(&f, (0..300).map(|n| upsert_with(n, &["v"])).collect()).await;
    index_all(&f, &source).await;
    let first = live_dataset(&f).await;
    let first_fragments = fragment_ids(&first);

    write_applied(&f, (300..600).map(|n| upsert_with(n, &["v"])).collect()).await;
    let grown = live_dataset(&f).await;
    let new_fragments: Vec<u32> = fragment_ids(&grown)
        .difference(&first_fragments)
        .copied()
        .collect();
    assert_eq!(
        plan(&f).await,
        vec![IndexWork::VectorDelta {
            vector: 0,
            fragments: new_fragments.clone(),
        }]
    );
    index_all(&f, &source).await;
    let manifest = f.manifest().await;
    assert_eq!(
        manifest.vector_indexes.len(),
        2,
        "{:?}",
        manifest.vector_indexes
    );
    assert!(
        manifest
            .vector_indexes
            .iter()
            .all(|s| s.index_name == "vec_0" && s.kind == VectorIndexKind::IvfPq)
    );
    let dataset = live_dataset(&f).await;
    let indices = dataset.load_indices().await.unwrap();
    let segments: Vec<_> = indices.iter().filter(|i| i.name == "vec_0").collect();
    assert_eq!(segments.len(), 2);
    let delta = segments
        .iter()
        .find(|s| {
            s.fragment_bitmap
                .as_ref()
                .unwrap()
                .iter()
                .eq(new_fragments.iter().copied())
        })
        .expect("a segment covering exactly the new fragments");
    let delta_ref = manifest
        .vector_indexes
        .iter()
        .find(|s| s.lance_index_uuid == delta.uuid.to_string())
        .expect("the delta segment is in the manifest");
    assert_eq!(delta_ref.indexed_row_ids_upto, dataset.manifest.next_row_id);
    assert_eq!(covered(&indices, "vec_0"), fragment_ids(&dataset));
    let column = vector_column(0);
    for n in [300, 450, 599, 3] {
        finds_itself(&dataset, &column, n, partitions(300)).await;
    }
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// With `index_max_segments = 2` segments, the next build is a full rebuild:
/// one segment covering every fragment.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn past_max_segments_a_full_rebuild_replaces_every_segment() {
    let f = TargetFixture::start_with(with_vector(), 2, config()).await;
    let source = IndexBuildSource::new(f.ctx.clone());
    write_applied(&f, (0..300).map(|n| upsert_with(n, &["v"])).collect()).await;
    index_all(&f, &source).await;
    write_applied(&f, (300..600).map(|n| upsert_with(n, &["v"])).collect()).await;
    index_all(&f, &source).await;
    assert_eq!(f.manifest().await.vector_indexes.len(), 2);
    let old: BTreeSet<String> = f
        .manifest()
        .await
        .vector_indexes
        .iter()
        .map(|s| s.lance_index_uuid.clone())
        .collect();

    write_applied(&f, (600..900).map(|n| upsert_with(n, &["v"])).collect()).await;
    assert_eq!(plan(&f).await, vec![IndexWork::VectorFull { vector: 0 }]);
    index_all(&f, &source).await;
    let manifest = f.manifest().await;
    let [segment] = manifest.vector_indexes.as_slice() else {
        panic!("one segment: {:?}", manifest.vector_indexes);
    };
    assert!(!old.contains(&segment.lance_index_uuid));
    let dataset = live_dataset(&f).await;
    assert_eq!(segment.indexed_row_ids_upto, dataset.manifest.next_row_id);
    let indices = dataset.load_indices().await.unwrap();
    assert_eq!(indices.len(), 1);
    assert_eq!(covered(&indices, "vec_0"), fragment_ids(&dataset));
    // Every index commit was detached: the mainline holds only version 1.
    let versions = format!("ns/{}/collections/{}/lance/_versions/", f.ns, f.cid);
    let mainline: Vec<String> = f
        .store
        .list(&versions)
        .await
        .expect("list")
        .into_iter()
        .map(|info| info.path)
        .filter(|path| !path.rsplit('/').next().unwrap_or("").starts_with('d'))
        .collect();
    assert_eq!(mainline.len(), 1, "{mainline:?}");
    let column = vector_column(0);
    for n in [0, 450, 899] {
        finds_itself(&dataset, &column, n, partitions(900)).await;
    }
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// Rows without the vector (null `_vector_0`) are not indexed and never
/// found, by the index or by the brute-force scan of unindexed fragments.
/// Too few rows with the vector defer the build without an error (P1).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_vector_index_skips_rows_without_the_vector() {
    let f = TargetFixture::start_with(with_vector(), 2, config()).await;
    let source = IndexBuildSource::new(f.ctx.clone());
    let half = |range: std::ops::Range<u64>| -> Vec<DocOp> {
        range
            .map(|n| {
                let vectors: &[&str] = if n % 2 == 0 { &["v"] } else { &[] };
                upsert_with(n, vectors)
            })
            .collect()
    };
    // 300 docs, 150 with the vector: too few to train; deferred.
    write_applied(&f, half(0..300)).await;
    let version = f.manifest().await.version;
    index_all(&f, &source).await;
    assert_eq!(f.manifest().await.version, version, "nothing was built");

    // 600 docs, 300 with the vector: built.
    write_applied(&f, half(300..600)).await;
    index_all(&f, &source).await;
    assert_eq!(f.manifest().await.vector_indexes.len(), 1);
    // 40 more, unindexed: searched by brute force.
    write_applied(&f, half(600..640)).await;

    let dataset = live_dataset(&f).await;
    let column = vector_column(0);
    for n in [0, 300, 598, 610, 638] {
        finds_itself(&dataset, &column, n, partitions(300)).await;
    }
    for n in [1, 301, 611] {
        let found = nearest(&dataset, &column, &vector_of(n), 100, partitions(300)).await;
        assert_eq!(found.len(), 100);
        for pk in found {
            let PrimaryKey::U64(k) = pk else {
                panic!("{pk:?}");
            };
            assert_eq!(k % 2, 0, "doc {k} has no vector but was found");
        }
    }
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// IVF_RQ and IVF_HNSW_SQ vectors get the index their spec names, with its
/// partition count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ivf_rq_and_ivf_hnsw_sq_follow_the_vector_spec() {
    let rq = VectorSpec {
        index: VectorIndexSpec::IvfRq {
            num_partitions: Some(4),
            num_bits: 1,
        },
        distance: Distance::Euclid,
        ..vector("rq", DIM)
    };
    let hnsw = VectorSpec {
        index: VectorIndexSpec::IvfHnswSq {
            num_partitions: None,
        },
        distance: Distance::Dot,
        hnsw: HnswParams {
            m: 8,
            ef_construct: 64,
            ..HnswParams::default()
        },
        ..vector("hnsw", DIM)
    };
    let schema = CollectionSchema::new(vec![], vec![rq, hnsw], DynamicMapping::Ignore);
    schema.validate().expect("valid schema");
    let f = TargetFixture::start_with(schema, 2, config()).await;
    let source = IndexBuildSource::new(f.ctx.clone());
    write_applied(
        &f,
        (0..300).map(|n| upsert_with(n, &["rq", "hnsw"])).collect(),
    )
    .await;
    index_all(&f, &source).await;
    let manifest = f.manifest().await;
    let kinds: Vec<(String, String, VectorIndexKind)> = manifest
        .vector_indexes
        .iter()
        .map(|s| (s.index_name.clone(), s.vector.clone(), s.kind))
        .collect();
    assert_eq!(
        kinds,
        vec![
            (
                "vec_0".to_string(),
                "rq".to_string(),
                VectorIndexKind::IvfRq
            ),
            (
                "vec_1".to_string(),
                "hnsw".to_string(),
                VectorIndexKind::IvfHnswSq
            ),
        ]
    );
    let dataset = live_dataset(&f).await;
    for (name, index_type, partitions) in [
        ("vec_0", "IVF_RQ", 4),
        ("vec_1", "IVF_HNSW_SQ", partitions(300)),
    ] {
        let stats: serde_json::Value =
            serde_json::from_str(&dataset.index_statistics(name).await.unwrap()).unwrap();
        assert_eq!(stats["index_type"], index_type, "{stats}");
        let indices = stats["indices"].as_array().expect("indices");
        assert_eq!(indices[0]["num_partitions"], partitions, "{stats}");
    }
    finds_itself(&dataset, &vector_column(0), 11, 4).await;
    finds_itself(&dataset, &vector_column(1), 12, partitions(300)).await;
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// A Manhattan vector (and one with `index: None`) gets no index and no
/// index work.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manhattan_vectors_get_no_index() {
    let manhattan = VectorSpec {
        distance: Distance::Manhattan,
        ..vector("m", DIM)
    };
    let none = VectorSpec {
        index: VectorIndexSpec::None,
        ..vector("none", DIM)
    };
    let schema = CollectionSchema::new(vec![], vec![manhattan, none], DynamicMapping::Ignore);
    schema.validate().expect("valid schema");
    let f = TargetFixture::start_with(schema, 2, config()).await;
    let source = IndexBuildSource::new(f.ctx.clone());
    write_applied(
        &f,
        (0..300).map(|n| upsert_with(n, &["m", "none"])).collect(),
    )
    .await;
    assert_eq!(plan(&f).await, vec![]);
    let version = f.manifest().await.version;
    index_all(&f, &source).await;
    let manifest = f.manifest().await;
    assert_eq!(manifest.version, version);
    assert!(manifest.vector_indexes.is_empty());
    assert!(
        live_dataset(&f)
            .await
            .load_indices()
            .await
            .unwrap()
            .is_empty()
    );
    f.shutdown().await;
}

/// After `PkBtree`, `scalar_indexes` holds `pk_btree` and `get_by_pk`
/// returns what it returned before; enough unindexed rows rebuild it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_pk_btree_serves_get_by_pk() {
    let config = CollectionConfig {
        pk_index_min_unindexed_rows: 50,
        ..config()
    };
    let f = TargetFixture::start_with(schema(vec![], DynamicMapping::Ignore), 2, config).await;
    let source = IndexBuildSource::new(f.ctx.clone());
    write_applied(&f, (0..40).map(|n| upsert_with(n, &[])).collect()).await;
    assert_eq!(plan(&f).await, vec![]);
    write_applied(&f, (40..100).map(|n| upsert_with(n, &[])).collect()).await;
    assert_eq!(plan(&f).await, vec![IndexWork::PkBtree]);
    let keys: Vec<PrimaryKey> = (0..110).step_by(3).map(PrimaryKey::U64).collect();
    let before = f.snapshot().await.get_by_pk(&keys).await.unwrap();
    assert_eq!(before.iter().filter(|d| d.is_some()).count(), 34);

    index_all(&f, &source).await;
    let manifest = f.manifest().await;
    let [pk] = manifest.scalar_indexes.as_slice() else {
        panic!("one scalar index: {:?}", manifest.scalar_indexes);
    };
    assert_eq!(pk.index_name, PK_INDEX_NAME);
    assert_eq!(pk.column, PK_COLUMN);
    let dataset = live_dataset(&f).await;
    assert_eq!(pk.indexed_row_ids_upto, dataset.manifest.next_row_id);
    let snapshot = f.snapshot().await;
    assert_eq!(snapshot.get_by_pk(&keys).await.unwrap(), before);
    assert_eq!(plan(&f).await, vec![]);

    // 60 new rows are unindexed: the index is rebuilt over every fragment.
    write_applied(&f, (100..160).map(|n| upsert_with(n, &[])).collect()).await;
    assert_eq!(plan(&f).await, vec![IndexWork::PkBtree]);
    index_all(&f, &source).await;
    let manifest = f.manifest().await;
    let [rebuilt] = manifest.scalar_indexes.as_slice() else {
        panic!("one scalar index: {:?}", manifest.scalar_indexes);
    };
    assert_ne!(rebuilt.lance_index_uuid, pk.lance_index_uuid);
    let dataset = live_dataset(&f).await;
    let indices = dataset.load_indices().await.unwrap();
    assert_eq!(covered(&indices, PK_INDEX_NAME), fragment_ids(&dataset));
    let keys: Vec<PrimaryKey> = (0..170).map(PrimaryKey::U64).collect();
    let found = f.snapshot().await.get_by_pk(&keys).await.unwrap();
    for (pk, doc) in keys.iter().zip(&found) {
        let PrimaryKey::U64(n) = pk else {
            unreachable!()
        };
        assert_eq!(
            doc.as_ref().map(|d| &d.pk),
            (*n < 160).then_some(pk),
            "{pk:?}"
        );
    }
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// The hook holds the index task after its Lance commit while a link commit
/// lands; released, its CAS conflicts, it rebases onto the link commit and
/// succeeds, with the same index files.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_index_commit_rebases_on_a_concurrent_link_commit() {
    let f = TargetFixture::start_with(with_vector(), 2, config()).await;
    write_applied(&f, (0..300).map(|n| upsert_with(n, &["v"])).collect()).await;
    let (hook, reached, release) = hold_at(CollectionCommitStep::AfterLanceCommit);
    let source = IndexBuildSource::new(f.ctx.clone()).with_hook(hook);
    let meta = f.meta.client.clone();
    let run = tokio::spawn(async move { run_once(&meta, "indexer", TTL, &source).await });
    wait_for("the index lance commit", || reached.load(Ordering::SeqCst)).await;

    write_applied(&f, (300..350).map(|n| upsert_with(n, &["v"])).collect()).await;
    let link_version = f.manifest().await.version;
    let dirs = index_dirs(&f).await;
    assert_eq!(dirs.len(), 1, "{dirs:?}");
    release.notify_one();
    let result = outcome(run.await.expect("join").expect("run"));
    assert!(result.is_ok(), "{result:?}");

    let manifest = f.manifest().await;
    assert_eq!(manifest.version, link_version + 1);
    assert_eq!(manifest.parent_version, link_version);
    assert_eq!(manifest.kind, CommitKind::IndexBuild);
    assert_eq!(manifest.live_doc_count, 350);
    let [segment] = manifest.vector_indexes.as_slice() else {
        panic!("one segment: {:?}", manifest.vector_indexes);
    };
    // The rebase re-committed the same index: no new index files.
    assert_eq!(index_dirs(&f).await, dirs);
    assert!(dirs.contains(&segment.lance_index_uuid));
    let dataset = live_dataset(&f).await;
    assert_eq!(dataset.count_rows(None).await.unwrap(), 350);
    assert!(segment.indexed_row_ids_upto < dataset.manifest.next_row_id);
    let column = vector_column(0);
    finds_itself(&dataset, &column, 10, partitions(300)).await;
    finds_itself(&dataset, &column, 320, partitions(300)).await;
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// The task is dropped after its Lance commit (a crash): the live manifest
/// is unchanged, and the next run builds the index again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_after_the_index_lance_commit_leaves_no_trace() {
    let f = TargetFixture::start_with(with_vector(), 2, config()).await;
    write_applied(&f, (0..300).map(|n| upsert_with(n, &["v"])).collect()).await;
    let before = f.manifest().await;
    let (hook, reached, _release) = hold_at(CollectionCommitStep::AfterLanceCommit);
    let source = IndexBuildSource::new(f.ctx.clone()).with_hook(hook);
    let meta = f.meta.client.clone();
    let crashed = tokio::spawn(async move {
        let _ = run_once(&meta, "crashed", CRASHED_TTL, &source).await;
    });
    wait_for("the index lance commit", || reached.load(Ordering::SeqCst)).await;
    crashed.abort();
    let _ = crashed.await;
    assert_eq!(f.manifest().await, before);
    let orphans = index_dirs(&f).await;
    assert_eq!(orphans.len(), 1);

    let source = IndexBuildSource::new(f.ctx.clone());
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
                let result = outcome(results);
                assert!(result.is_ok(), "{result:?}");
                break;
            }
        }
    }
    let manifest = f.manifest().await;
    assert_eq!(manifest.version, before.version + 1);
    let [segment] = manifest.vector_indexes.as_slice() else {
        panic!("one segment: {:?}", manifest.vector_indexes);
    };
    assert!(!orphans.contains(&segment.lance_index_uuid));
    assert_verified(f.verify().await);
    f.shutdown().await;
}

/// The task's lease is taken over after its Lance commit: its CAS is
/// refused as `Fenced` and the manifest is unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fenced_index_task_changes_nothing() {
    let f = TargetFixture::start_with(with_vector(), 2, config()).await;
    write_applied(&f, (0..300).map(|n| upsert_with(n, &["v"])).collect()).await;
    let before = f.manifest().await;
    let meta = f.meta.client.clone();
    let taken = Arc::new(AtomicBool::new(false));
    let flag = taken.clone();
    let hook: CollectionCommitHook = Arc::new(move |at, fence| {
        let meta = meta.clone();
        let flag = flag.clone();
        async move {
            if at == CollectionCommitStep::AfterLanceCommit && !flag.swap(true, Ordering::SeqCst) {
                meta.release_lease(&fence.lease, "indexer", fence.epoch)
                    .await
                    .expect("release");
                meta.acquire_lease(&fence.lease, "thief", TTL)
                    .await
                    .expect("take over");
            }
        }
        .boxed()
    });
    let source = IndexBuildSource::new(f.ctx.clone()).with_hook(hook);
    let result = outcome(
        run_once(&f.meta.client, "indexer", TTL, &source)
            .await
            .expect("run"),
    );
    assert!(matches!(result, Err(TaskError::Fenced)), "{result:?}");
    assert!(taken.load(Ordering::SeqCst));
    assert_eq!(f.manifest().await, before);
    f.shutdown().await;
}
