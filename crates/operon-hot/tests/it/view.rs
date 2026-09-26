//! Per-manifest views (plan M1.3 Task 6 rules 1–3), built directly from
//! Task 5 artifacts over `FlatEngine`: live rows, exclusions, the delta
//! index, coverage, and what a search may return.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use operon_collection::{
    CollectionSnapshot, DocOp, LanceCompactionSource, MaintenanceConfig, PrimaryKey,
    SplitMergeSource, StoredDoc,
};
use operon_hnsw::{Distance, FlatEngine, HnswEngine, exact_score};
use operon_hot::{ColumnView, DeltaIndex, HNSW_KIND, HotTierConfig, LoadedArtifact, live_rows};
use operon_query::hot::HotAnn;
use operon_quickwit::merge_policy::StableLogMergePolicyConfig;
use operon_worker::run_once;
use roaring::RoaringTreemap;
use tempfile::TempDir;

use crate::common::{DIM, Fixture, TTL, build_once, doc, docs, vector_of};

/// An artifact loaded from the store and its delta index.
struct Loaded {
    artifact: Arc<LoadedArtifact>,
    delta: Arc<DeltaIndex>,
    _dirs: TempDir,
}

/// Pins the first collection and builds its artifact at the live manifest.
async fn build(f: &Fixture) {
    f.pin_vectors().await;
    build_once(f, &f.source()).await.expect("build");
}

/// Loads the live manifest's artifact of `_vector_0` with a fresh delta.
async fn load(f: &Fixture) -> Loaded {
    let manifest = f.manifest().await;
    let reference = manifest
        .hot_artifacts
        .iter()
        .find(|a| a.kind == HNSW_KIND && a.column == "_vector_0")
        .expect("an artifact")
        .clone();
    let dirs = TempDir::new().expect("dir");
    let engine: Arc<dyn HnswEngine> = Arc::new(FlatEngine);
    let artifact = LoadedArtifact::load(
        &f.store,
        &reference.prefix,
        dirs.path().join("artifact"),
        4,
        &engine,
    )
    .await
    .expect("load");
    let artifact = Arc::new(artifact);
    let delta = DeltaIndex::create(&artifact, dirs.path().join("delta"))
        .await
        .expect("delta");
    Loaded {
        artifact,
        delta,
        _dirs: dirs,
    }
}

/// Extends the delta for the live manifest and makes its view; returns the
/// view and the live rows.
async fn view_with(
    f: &Fixture,
    l: &Loaded,
    config: &HotTierConfig,
) -> (ColumnView, RoaringTreemap) {
    let snapshot = f.snapshot().await;
    let live = live_rows(&snapshot).await.expect("live rows");
    l.delta
        .extend(&snapshot, &live, &l.artifact, config)
        .await
        .expect("extend");
    let view = ColumnView::new(
        snapshot.manifest().version,
        l.artifact.descriptor.source_version,
        l.artifact.clone(),
        l.delta.clone(),
        &live,
    );
    (view, live)
}

async fn view(f: &Fixture, l: &Loaded) -> (ColumnView, RoaringTreemap) {
    view_with(f, l, &f.tier_config()).await
}

/// Every live document of the first collection.
async fn live_docs(f: &Fixture) -> Vec<StoredDoc> {
    f.snapshot().await.scan_all().await.expect("scan")
}

/// Row id by key.
fn rows_by_key(docs: &[StoredDoc]) -> BTreeMap<u64, u64> {
    docs.iter()
        .map(|d| match d.pk {
            PrimaryKey::U64(k) => (k, d.row_id),
            ref other => panic!("unexpected key {other:?}"),
        })
        .collect()
}

fn rows_of(rows: &BTreeMap<u64, u64>, keys: std::ops::Range<u64>) -> RoaringTreemap {
    keys.map(|k| rows[&k]).collect()
}

fn deletes(keys: std::ops::Range<u64>) -> Vec<DocOp> {
    keys.map(|k| DocOp::Delete(PrimaryKey::U64(k))).collect()
}

/// A probe vector no document has.
fn probe(i: u64) -> Vec<f32> {
    vector_of(1_000_000 + i, DIM)
}

async fn search(
    view: &ColumnView,
    query: &[f32],
    k: usize,
    allow: Option<&RoaringTreemap>,
) -> Vec<(u64, f32)> {
    view.search(query, k, allow, None).await.expect("search")
}

fn ids(hits: &[(u64, f32)]) -> RoaringTreemap {
    hits.iter().map(|(id, _)| *id).collect()
}

/// The maintenance configuration of Task 5's `a_current_artifact_is_not_rebuilt`.
fn maintenance() -> MaintenanceConfig {
    MaintenanceConfig {
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
    }
}

async fn merge(f: &Fixture) {
    let before = f.manifest().await.version;
    let merges = SplitMergeSource::new(f.ctx.clone(), maintenance());
    run_once(&f.meta.client, "merger", TTL, &merges)
        .await
        .expect("merge");
    assert!(f.manifest().await.version > before, "no merge committed");
}

async fn compact(f: &Fixture) {
    let before = f.manifest().await.version;
    let compaction = LanceCompactionSource::new(f.ctx.clone(), maintenance());
    run_once(&f.meta.client, "compactor", TTL, &compaction)
        .await
        .expect("compaction");
    assert!(
        f.manifest().await.version > before,
        "no compaction committed"
    );
}

async fn assert_live_rows_match(f: &Fixture, what: &str) {
    let snapshot: CollectionSnapshot = f.snapshot().await;
    let lance: RoaringTreemap = snapshot
        .scan_all()
        .await
        .expect("scan")
        .into_iter()
        .map(|d| d.row_id)
        .collect();
    assert_eq!(
        live_rows(&snapshot).await.expect("live rows"),
        lance,
        "after {what}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_rows_equals_the_lance_row_set() {
    let f = Fixture::start().await;
    f.commit(docs(0..60)).await;
    assert_live_rows_match(&f, "the first commit").await;
    // A splitmix-style sequence decides each round's ops.
    let mut state = 0x5EED_u64;
    let mut next = move |n: u64| {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        (state >> 33) % n
    };
    let mut keys: Vec<u64> = (0..60).collect();
    let mut fresh = 60;
    for round in 0..6 {
        let mut ops = Vec::new();
        let mut touched = std::collections::BTreeSet::new();
        for _ in 0..5 {
            ops.push(doc(fresh, true));
            keys.push(fresh);
            fresh += 1;
        }
        for _ in 0..8 {
            let key = keys[next(keys.len() as u64) as usize];
            if !touched.insert(key) {
                continue;
            }
            match next(2) {
                0 => ops.push(doc(key, next(4) != 0)),
                _ => {
                    ops.push(DocOp::Delete(PrimaryKey::U64(key)));
                    keys.retain(|k| *k != key);
                }
            }
        }
        f.commit(ops).await;
        assert_live_rows_match(&f, &format!("round {round}")).await;
    }
    merge(&f).await;
    assert_live_rows_match(&f, "a merge").await;
    compact(&f).await;
    assert_live_rows_match(&f, "a compaction").await;
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_view_never_returns_a_row_deleted_since_the_artifact() {
    let f = Fixture::start().await;
    f.commit(docs(0..200)).await;
    build(&f).await;
    let l = load(&f).await;
    let rows = rows_by_key(&live_docs(&f).await);
    // Keys 0..20 are deleted: each is the true nearest of its own probe.
    f.commit(deletes(0..20)).await;
    let deleted = rows_of(&rows, 0..20);
    let (view, _) = view(&f, &l).await;
    assert_eq!(view.excluded(), &deleted);
    let everything: RoaringTreemap = rows.values().copied().collect();
    for k in 0..50 {
        let query = match k < 20 {
            true => vector_of(k, DIM),
            false => probe(k),
        };
        for allow in [None, Some(&everything)] {
            let hits = search(&view, &query, 10, allow).await;
            assert_eq!(hits.len(), 10, "probe {k}");
            assert!(ids(&hits).is_disjoint(&deleted), "probe {k}: {hits:?}");
        }
    }
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn covered_is_every_live_row_the_view_has_scanned() {
    let f = Fixture::start().await;
    f.commit(docs(0..100)).await;
    build(&f).await;
    let l = load(&f).await;
    let mut ops = docs(100..150);
    ops.extend(deletes(0..10));
    ops.extend((20..25).map(|k| doc(k, true)));
    f.commit(ops).await;
    let (view, live) = view(&f, &l).await;
    let delta = l.delta.scanned();
    let expected = (&live & &l.artifact.scanned) | (&live & &delta);
    assert_eq!(view.covered(), &expected);
    assert_eq!(view.covered(), &live);
    assert_eq!(l.delta.appended(), 55, "50 inserts and 5 upserts");
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rows_inserted_since_the_artifact_are_found_through_the_delta() {
    let f = Fixture::start().await;
    f.commit(docs(0..100)).await;
    build(&f).await;
    let l = load(&f).await;
    f.commit(docs(100..200)).await;
    let (view, _) = view(&f, &l).await;
    let rows = rows_by_key(&live_docs(&f).await);
    for k in 100..200 {
        let hits = search(&view, &vector_of(k, DIM), 1, None).await;
        assert_eq!(hits.first().map(|(id, _)| *id), Some(rows[&k]), "key {k}");
    }
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_upserted_row_is_found_under_its_new_row_id_only() {
    let f = Fixture::start().await;
    f.commit(docs(0..100)).await;
    build(&f).await;
    let l = load(&f).await;
    let old = rows_by_key(&live_docs(&f).await)[&5];
    let moved = vector_of(10_005, DIM);
    let DocOp::Upsert(mut upsert) = doc(5, true) else {
        unreachable!("an upsert");
    };
    upsert.vectors.insert(String::new(), moved.clone());
    f.commit(vec![DocOp::Upsert(upsert)]).await;
    let new = rows_by_key(&live_docs(&f).await)[&5];
    assert_ne!(new, old);
    let (view, _) = view(&f, &l).await;
    let hits = search(&view, &moved, 1, None).await;
    assert_eq!(hits.first().map(|(id, _)| *id), Some(new));
    for query in [moved, vector_of(5, DIM)] {
        let hits = search(&view, &query, 200, None).await;
        assert_eq!(hits.len(), 100);
        assert!(!ids(&hits).contains(old), "the old row id came back");
    }
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_capped_delta_shrinks_covered() {
    let f = Fixture::start().await;
    f.commit(docs(0..50)).await;
    build(&f).await;
    let l = load(&f).await;
    f.commit(docs(50..75)).await;
    let config = HotTierConfig {
        delta_max_rows: 10,
        ..f.tier_config()
    };
    let (view, live) = view_with(&f, &l, &config).await;
    let uncovered = &live - view.covered();
    assert_eq!(uncovered.len(), 15);
    assert_eq!(l.delta.scanned().len(), 10);
    let rows = rows_by_key(&live_docs(&f).await);
    for k in 50..75 {
        let hits = search(&view, &vector_of(k, DIM), 100, None).await;
        assert!(ids(&hits).is_disjoint(&uncovered), "key {k}");
    }
    assert!(
        search(&view, &vector_of(60, DIM), 10, Some(&uncovered))
            .await
            .is_empty()
    );
    // The ten lowest new row ids are the ones covered.
    let new_rows = rows_of(&rows, 50..75);
    let lowest: RoaringTreemap = new_rows.iter().take(10).collect();
    assert_eq!(view.covered() & &new_rows, lowest);
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_artifact_survives_compaction_and_merges() {
    let f = Fixture::start().await;
    for c in 0..4 {
        f.commit(docs(c * 10..c * 10 + 10)).await;
    }
    build(&f).await;
    let l = load(&f).await;
    let (before, _) = view(&f, &l).await;
    merge(&f).await;
    compact(&f).await;
    let (after, live) = view(&f, &l).await;
    assert!(after.version() > before.version());
    assert!(after.excluded().is_empty());
    assert!(l.delta.scanned().is_empty());
    assert_eq!(l.delta.appended(), 0);
    assert_eq!(after.covered(), &live);
    for i in 0..20 {
        let query = probe(i);
        assert_eq!(
            search(&after, &query, 10, None).await,
            search(&before, &query, 10, None).await,
            "probe {i}"
        );
    }
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn allow_lists_never_widen_results() {
    let f = Fixture::start().await;
    f.commit(docs(0..50)).await;
    build(&f).await;
    let l = load(&f).await;
    let before = rows_by_key(&live_docs(&f).await);
    let mut ops = docs(50..60);
    ops.extend(deletes(0..5));
    f.commit(ops).await;
    let config = HotTierConfig {
        delta_max_rows: 5,
        ..f.tier_config()
    };
    let (view, live) = view_with(&f, &l, &config).await;
    let deleted = rows_of(&before, 0..5);
    let uncovered = &live - view.covered();
    assert_eq!(uncovered.len(), 5);
    let covered_part = rows_of(&before, 10..15);
    let allow = &(&deleted | &uncovered) | &covered_part;
    for i in 0..20 {
        let hits = search(&view, &probe(i), 50, Some(&allow)).await;
        assert_eq!(ids(&hits), covered_part, "probe {i}");
    }
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_older_view_post_filters_rows_appended_later() {
    let f = Fixture::start().await;
    f.commit(docs(0..50)).await;
    build(&f).await;
    let l = load(&f).await;
    f.commit(docs(50..60)).await;
    let (old, _) = view(&f, &l).await;
    let appended = l.delta.appended();
    f.commit(docs(60..80)).await;
    let (new, _) = view(&f, &l).await;
    assert_eq!(l.delta.appended(), appended + 20);
    let later = new.covered() - old.covered();
    assert_eq!(later.len(), 20);
    for k in 60..80 {
        let hits = search(&old, &vector_of(k, DIM), 100, None).await;
        assert_eq!(hits.len(), 60, "key {k}");
        assert!(ids(&hits).is_disjoint(&later), "key {k}");
        let hits = search(&old, &vector_of(k, DIM), 100, Some(&later)).await;
        assert!(hits.is_empty(), "key {k}");
    }
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn view_scores_are_exact_under_the_flat_engine() {
    let f = Fixture::start().await;
    f.commit(docs(0..100)).await;
    build(&f).await;
    let l = load(&f).await;
    let mut ops = docs(100..130);
    ops.extend(deletes(0..10));
    f.commit(ops).await;
    let (view, _) = view(&f, &l).await;
    let vectors: BTreeMap<u64, Vec<f32>> = live_docs(&f)
        .await
        .into_iter()
        .map(|d| (d.row_id, d.vectors[""].clone()))
        .collect();
    let half: RoaringTreemap = vectors.keys().copied().step_by(2).collect();
    for i in 0..20 {
        let query = probe(i);
        for allow in [None, Some(&half)] {
            let hits = search(&view, &query, 10, allow).await;
            assert_eq!(hits.len(), 10);
            for (id, score) in &hits {
                let exact = exact_score(Distance::Cosine, &query, &vectors[id]);
                assert_eq!(score.to_bits(), exact.to_bits(), "probe {i}, row {id}");
            }
            // And they are the exact top 10 of the allowed live rows.
            let mut expected: Vec<(u64, f32)> = vectors
                .iter()
                .filter(|(id, _)| allow.is_none_or(|a| a.contains(**id)))
                .map(|(id, v)| (*id, exact_score(Distance::Cosine, &query, v)))
                .collect();
            expected.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
            expected.truncate(10);
            assert_eq!(hits, expected, "probe {i}");
        }
    }
    f.shutdown().await;
}
