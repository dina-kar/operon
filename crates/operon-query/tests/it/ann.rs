//! Dense vector search (plan M1.2 Task 6): `AnnExec` over Lance, the tail
//! and the hot tier, with every score recomputed by `vector::score`
//! (Ruling 3, R12).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::common::{TailFixture, field, vector};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use operon_collection::{
    CollectionConfig, CollectionSchema, Distance, DocOp, Document, DynamicMapping, FieldKind,
    PrimaryKey,
};
use operon_common::{CollectionId, NamespaceId};
use operon_query::exec::{AnnExec, FilterBitmapExec, Ranked, batch_to_ranked};
use operon_query::hot::{HotAnn, HotError, HotKind, HotTier, NoHotTier, RequestHot};
use operon_query::read::{ReadConfig, ReadView, Reads};
use operon_query::tail::{TAIL_ROWID_BASE, TailConfig};
use operon_query::vector::{AnnConfig, AnnStrategy, score};
use operon_query::{AnnParams, Query, ReadConsistency, ServiceError};
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use roaring::RoaringTreemap;
use serde_json::json;

const DIM: usize = 16;

// ----- fixtures -----

/// `n` I64 and vector `v` (dim 16, Cosine, the default `Auto` index), with
/// a 1 KiB full-scan threshold so the brute-force bound is the config's.
fn ann_schema() -> CollectionSchema {
    let mut v = vector("v", DIM as u32);
    v.hnsw.full_scan_threshold_kb = 1;
    let schema = CollectionSchema::new(
        vec![field("n", FieldKind::I64)],
        vec![v],
        DynamicMapping::Ignore,
    );
    schema.validate().expect("valid schema");
    schema
}

fn unit_vector(rng: &mut ChaCha8Rng) -> Vec<f32> {
    loop {
        let v: Vec<f32> = (0..DIM).map(|_| rng.random_range(-1.0f32..1.0)).collect();
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 1e-3 {
            return v.into_iter().map(|x| x / norm).collect();
        }
    }
}

fn with_vector(pk: u64, v: Vec<f32>) -> DocOp {
    let serde_json::Value::Object(source) = json!({ "n": pk }) else {
        unreachable!()
    };
    DocOp::Upsert(Document {
        pk: PrimaryKey::U64(pk),
        source,
        vectors: BTreeMap::from([("v".to_string(), v)]),
        sparse_vectors: BTreeMap::new(),
    })
}

/// The Task 6 corpus: 3 000 rows indexed at `index_min_rows = 500`, 200
/// more unindexed rows and 20 durable deletes, then in the tail 50 new
/// rows, 100 updates and 20 deletes.
struct Ann {
    fixture: TailFixture,
    reads: Reads,
    /// Row id → vector of the first commit (the hot artifact's source).
    initial: BTreeMap<u64, Vec<f32>>,
    /// Row id → key of the first commit.
    initial_keys: BTreeMap<u64, u64>,
    /// Keys deleted durably or in the tail.
    deleted: Vec<u64>,
    /// (key, old vector, new vector) of the tail updates.
    updated: Vec<(u64, Vec<f32>, Vec<f32>)>,
    /// (key, vector) of the rows newer than the index.
    unindexed: Vec<(u64, Vec<f32>)>,
    /// (key, vector) of the new tail rows.
    tail_new: Vec<(u64, Vec<f32>)>,
}

async fn ann_fixture() -> Ann {
    let config = CollectionConfig {
        index_min_rows: 500,
        index_delta_min_rows: 1_000_000,
        index_poll_interval: std::time::Duration::ZERO,
        ..CollectionConfig::default()
    };
    let fixture = TailFixture::start_configured(ann_schema(), 2, config).await;
    let mut rng = ChaCha8Rng::seed_from_u64(16);
    let first: Vec<(u64, Vec<f32>)> = (0..3_000).map(|pk| (pk, unit_vector(&mut rng))).collect();
    let ops: Vec<DocOp> = first
        .iter()
        .map(|(pk, v)| with_vector(*pk, v.clone()))
        .collect();
    fixture.append_all(&ops).await;
    fixture.apply_link().await;
    fixture.build_indexes().await;
    let manifest = fixture.manifest().await.1;
    assert!(
        !manifest.vector_indexes.is_empty(),
        "the vector index was built"
    );
    let mut initial = BTreeMap::new();
    let mut initial_keys = BTreeMap::new();
    for stored in fixture.snapshot().await.scan_all().await.expect("scan") {
        let PrimaryKey::U64(pk) = stored.pk else {
            panic!("u64 keys")
        };
        initial.insert(stored.row_id, stored.vectors["v"].clone());
        initial_keys.insert(stored.row_id, pk);
    }

    let unindexed: Vec<(u64, Vec<f32>)> = (3_000..3_200)
        .map(|pk| (pk, unit_vector(&mut rng)))
        .collect();
    let durable_deletes: Vec<u64> = (1..=20).map(|i| i * 10).collect();
    let mut ops: Vec<DocOp> = unindexed
        .iter()
        .map(|(pk, v)| with_vector(*pk, v.clone()))
        .collect();
    ops.extend(
        durable_deletes
            .iter()
            .map(|pk| DocOp::Delete(PrimaryKey::U64(*pk))),
    );
    fixture.append_all(&ops).await;
    fixture.apply_link().await;
    assert_eq!(
        fixture.manifest().await.1.vector_indexes.len(),
        manifest.vector_indexes.len(),
        "no delta segment"
    );

    let tail_new: Vec<(u64, Vec<f32>)> = (3_200..3_250)
        .map(|pk| (pk, unit_vector(&mut rng)))
        .collect();
    let updated: Vec<(u64, Vec<f32>, Vec<f32>)> = (1_000..1_100)
        .map(|pk| (pk, first[pk as usize].1.clone(), unit_vector(&mut rng)))
        .collect();
    let tail_deletes: Vec<u64> = (2_500..2_520).collect();
    let mut ops: Vec<DocOp> = tail_new
        .iter()
        .map(|(pk, v)| with_vector(*pk, v.clone()))
        .collect();
    ops.extend(
        updated
            .iter()
            .map(|(pk, _, new)| with_vector(*pk, new.clone())),
    );
    ops.extend(
        tail_deletes
            .iter()
            .map(|pk| DocOp::Delete(PrimaryKey::U64(*pk))),
    );
    fixture.append_all(&ops).await;
    let reads = fixture.reads(TailConfig::default(), ReadConfig::default());
    let mut deleted = durable_deletes;
    deleted.extend(tail_deletes);
    Ann {
        fixture,
        reads,
        initial,
        initial_keys,
        deleted,
        updated,
        unindexed,
        tail_new,
    }
}

impl Ann {
    async fn view(&self) -> Arc<ReadView> {
        let view = self
            .fixture
            .view(&self.reads, &ReadConsistency::Strong)
            .await
            .expect("view");
        assert_eq!(view.tail.shadow().len(), 120, "100 updates, 20 deletes");
        assert_eq!(view.live_rows(), 3_000 + 200 - 20 - 20 + 50);
        Arc::new(view)
    }

    async fn shutdown(self) {
        self.reads.shutdown().await;
        self.fixture.shutdown().await;
    }
}

/// Every live row of the view with its vector: unshadowed durable rows,
/// then the tail's live docs.
async fn live_rows(view: &ReadView) -> Vec<(u64, PrimaryKey, Vec<f32>)> {
    let mut out = Vec::new();
    for stored in view.snapshot.scan_all().await.expect("scan") {
        if !view.is_shadowed(stored.row_id)
            && let Some(v) = stored.vectors.get("v")
        {
            out.push((stored.row_id, stored.pk, v.clone()));
        }
    }
    for doc in view.tail.live_docs() {
        if let Some(v) = doc.doc.as_ref().and_then(|d| d.vectors.get("v")) {
            out.push((doc.row_id, doc.pk.clone(), v.clone()));
        }
    }
    out
}

/// The in-test brute force: (key, score bits) of the best `k` of `rows`
/// (restricted to `allowed` keys when given).
fn brute(
    rows: &[(u64, PrimaryKey, Vec<f32>)],
    metric: Distance,
    query: &[f32],
    k: usize,
    allowed: Option<&BTreeSet<PrimaryKey>>,
) -> Vec<(PrimaryKey, u32)> {
    let mut scored: Vec<(f32, PrimaryKey)> = rows
        .iter()
        .filter(|(_, pk, _)| allowed.is_none_or(|a| a.contains(pk)))
        .map(|(_, pk, v)| (score(metric, query, v), pk.clone()))
        .collect();
    scored.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    scored
        .into_iter()
        .take(k)
        .map(|(s, pk)| (pk, s.to_bits()))
        .collect()
}

fn bits(hits: &[Ranked]) -> Vec<(PrimaryKey, u32)> {
    hits.iter()
        .map(|hit| (hit.pk.clone(), hit.score.to_bits()))
        .collect()
}

fn ids(pks: impl IntoIterator<Item = u64>) -> Query {
    Query::Ids(pks.into_iter().map(PrimaryKey::U64).collect())
}

fn exec(
    view: &Arc<ReadView>,
    query: &[f32],
    k: usize,
    params: AnnParams,
    filter: Option<Query>,
    config: AnnConfig,
) -> AnnExec {
    let allow = filter.map(|filter| Arc::new(FilterBitmapExec::new(view.clone(), filter, 8)));
    AnnExec::new(
        view.clone(),
        "v".to_string(),
        query.to_vec(),
        k,
        params,
        allow,
        config,
    )
}

async fn search(exec: &AnnExec) -> Vec<Ranked> {
    exec.search().await.expect("search")
}

fn exact() -> AnnParams {
    AnnParams {
        exact: true,
        ..AnnParams::default()
    }
}

fn sorted(hits: &[Ranked]) -> bool {
    hits.windows(2).all(|pair| {
        pair[0]
            .score
            .total_cmp(&pair[1].score)
            .then_with(|| pair[1].pk.cmp(&pair[0].pk))
            == std::cmp::Ordering::Greater
    })
}

/// Every hit's score is `score(metric, q, its stored vector)`, bit for bit.
fn assert_exact_scores(hits: &[Ranked], rows: &[(u64, PrimaryKey, Vec<f32>)], query: &[f32]) {
    let by_row: BTreeMap<u64, &Vec<f32>> = rows.iter().map(|(row, _, v)| (*row, v)).collect();
    for hit in hits {
        let stored = by_row
            .get(&hit.row_id)
            .unwrap_or_else(|| panic!("{hit:?} is not a live row"));
        assert_eq!(
            hit.score.to_bits(),
            score(Distance::Cosine, query, stored).to_bits(),
            "{hit:?}"
        );
    }
}

// ----- the kernel -----

#[test]
fn metric_conventions_follow_the_overview() {
    assert_eq!(score(Distance::Cosine, &[1.0, 0.0], &[0.6, 0.8]), 0.6);
    assert_eq!(score(Distance::Dot, &[1.0, 2.0], &[3.0, 4.0]), 11.0);
    assert_eq!(score(Distance::Euclid, &[0.0, 0.0], &[3.0, 4.0]), -5.0);
    assert_eq!(score(Distance::Manhattan, &[0.0, 0.0], &[3.0, 4.0]), -7.0);
    // Larger is better on every metric.
    for metric in [
        Distance::Cosine,
        Distance::Dot,
        Distance::Euclid,
        Distance::Manhattan,
    ] {
        assert!(
            score(metric, &[1.0, 0.0], &[1.0, 0.0]) > score(metric, &[1.0, 0.0], &[-1.0, 0.0]),
            "{metric:?}"
        );
    }
}

#[test]
fn zero_vector_cosine_scores_zero() {
    assert_eq!(score(Distance::Cosine, &[0.0, 0.0], &[0.6, 0.8]), 0.0);
    assert_eq!(score(Distance::Cosine, &[0.6, 0.8], &[0.0, 0.0]), 0.0);
    assert_eq!(score(Distance::Cosine, &[0.0, 0.0], &[0.0, 0.0]), 0.0);
}

// ----- exact search -----

#[tokio::test]
async fn exact_search_equals_brute_force_over_live_rows() {
    let ann = ann_fixture().await;
    let view = ann.view().await;
    let rows = live_rows(&view).await;
    let mut rng = ChaCha8Rng::seed_from_u64(61);
    for _ in 0..20 {
        let query = unit_vector(&mut rng);
        // The spec's metric, then every metric as an override.
        let exec = exec(&view, &query, 10, exact(), None, AnnConfig::default());
        let hits = search(&exec).await;
        assert_eq!(exec.strategy(), Some(AnnStrategy::Exact));
        assert_eq!(
            bits(&hits),
            brute(&rows, Distance::Cosine, &query, 10, None)
        );
        for metric in [
            Distance::Cosine,
            Distance::Dot,
            Distance::Euclid,
            Distance::Manhattan,
        ] {
            let params = AnnParams {
                distance: Some(metric),
                ..exact()
            };
            let hits = search(&exec_with(&view, &query, 10, params)).await;
            assert_eq!(
                bits(&hits),
                brute(&rows, metric, &query, 10, None),
                "{metric:?}"
            );
        }
    }
    // The operator protocol: one ranked batch through DataFusion.
    let query = unit_vector(&mut rng);
    let plan: Arc<dyn ExecutionPlan> =
        Arc::new(exec(&view, &query, 10, exact(), None, AnnConfig::default()));
    let batches =
        datafusion::physical_plan::collect(plan.clone(), Arc::new(TaskContext::default()))
            .await
            .expect("collect");
    let through: Vec<Ranked> = batches
        .iter()
        .flat_map(|batch| batch_to_ranked(batch).expect("ranked"))
        .collect();
    assert_eq!(
        bits(&through),
        brute(&rows, Distance::Cosine, &query, 10, None)
    );
    assert_eq!(plan.metrics().expect("metrics").output_rows(), Some(10));
    assert_eq!(plan.name(), "AnnExec");
    ann.shutdown().await;
}

fn exec_with(view: &Arc<ReadView>, query: &[f32], k: usize, params: AnnParams) -> AnnExec {
    exec(view, query, k, params, None, AnnConfig::default())
}

#[tokio::test]
async fn a_distance_override_runs_exact_search_with_that_metric() {
    let ann = ann_fixture().await;
    let view = ann.view().await;
    let rows = live_rows(&view).await;
    let mut rng = ChaCha8Rng::seed_from_u64(62);
    let query = unit_vector(&mut rng);
    let params = AnnParams {
        distance: Some(Distance::Euclid),
        ..exact()
    };
    let exec = exec_with(&view, &query, 20, params);
    let hits = search(&exec).await;
    assert_eq!(exec.strategy(), Some(AnnStrategy::Exact));
    assert_eq!(
        bits(&hits),
        brute(&rows, Distance::Euclid, &query, 20, None)
    );
    assert!(hits.iter().all(|hit| hit.score <= 0.0));
    ann.shutdown().await;
}

// ----- index search -----

#[tokio::test]
async fn ann_scores_are_exact() {
    let ann = ann_fixture().await;
    let view = ann.view().await;
    let rows = live_rows(&view).await;
    let mut rng = ChaCha8Rng::seed_from_u64(63);
    for _ in 0..20 {
        let query = unit_vector(&mut rng);
        let exec = exec_with(&view, &query, 10, AnnParams::default());
        let hits = search(&exec).await;
        assert_eq!(exec.strategy(), Some(AnnStrategy::Postfilter));
        assert_eq!(hits.len(), 10);
        assert!(sorted(&hits));
        assert_exact_scores(&hits, &rows, &query);
    }
    ann.shutdown().await;
}

#[tokio::test]
async fn rows_newer_than_the_index_are_found() {
    let ann = ann_fixture().await;
    let view = ann.view().await;
    for (pk, v) in &ann.unindexed {
        let hits = search(&exec_with(&view, v, 5, AnnParams::default())).await;
        assert_eq!(hits.first().map(|h| &h.pk), Some(&PrimaryKey::U64(*pk)));
    }
    ann.shutdown().await;
}

#[tokio::test]
async fn shadowed_rows_are_never_returned() {
    let ann = ann_fixture().await;
    let view = ann.view().await;
    for (pk, old, new) in &ann.updated {
        for params in [AnnParams::default(), exact()] {
            let hits = search(&exec_with(&view, old, 10, params.clone())).await;
            assert!(
                hits.iter().all(|hit| !view.is_shadowed(hit.row_id)),
                "{pk}: {hits:?}"
            );
            if let Some(hit) = hits.iter().find(|hit| hit.pk == PrimaryKey::U64(*pk)) {
                assert!(hit.row_id >= TAIL_ROWID_BASE, "{pk}: the old version");
            }
            let hits = search(&exec_with(&view, new, 10, params)).await;
            assert_eq!(hits[0].pk, PrimaryKey::U64(*pk));
            assert!(hits[0].row_id >= TAIL_ROWID_BASE, "{pk}: the new version");
        }
    }
    // Deleted keys never come back either.
    let rows = live_rows(&view).await;
    let deleted: BTreeSet<PrimaryKey> = ann.deleted.iter().map(|pk| PrimaryKey::U64(*pk)).collect();
    assert!(rows.iter().all(|(_, pk, _)| !deleted.contains(pk)));
    for pk in ann.deleted.iter().take(10) {
        let (row, _) = ann
            .initial_keys
            .iter()
            .find(|(_, key)| *key == pk)
            .expect("an initial row");
        let hits = search(&exec_with(
            &view,
            &ann.initial[row],
            10,
            AnnParams::default(),
        ))
        .await;
        assert!(hits.iter().all(|hit| !deleted.contains(&hit.pk)));
    }
    ann.shutdown().await;
}

#[tokio::test]
async fn recall_of_the_durable_defaults_is_at_least_ninety_percent() {
    let ann = ann_fixture().await;
    let view = ann.view().await;
    let rows = live_rows(&view).await;
    let mut rng = ChaCha8Rng::seed_from_u64(64);
    let mut found = 0;
    for _ in 0..50 {
        let query = unit_vector(&mut rng);
        let truth: BTreeSet<PrimaryKey> = brute(&rows, Distance::Cosine, &query, 10, None)
            .into_iter()
            .map(|(pk, _)| pk)
            .collect();
        let hits = search(&exec_with(&view, &query, 10, AnnParams::default())).await;
        found += hits.iter().filter(|hit| truth.contains(&hit.pk)).count();
    }
    let recall = found as f64 / 500.0;
    // Lance 12.0.0 trains the index with unseeded k-means (see
    // `a_moderate_filter_prefilters`), so this recall differs from run to
    // run. Twenty-one runs measured 0.938–0.978 (mean 0.96, standard
    // deviation 0.01): the plan's bound, 0.90, sits 0.038 below the lowest
    // and six standard deviations below the mean (plan row 15.4).
    assert!(recall >= 0.90, "recall@10 {recall}");
    ann.shutdown().await;
}

// ----- filters -----

#[tokio::test]
async fn a_filter_matching_nothing_returns_nothing() {
    let ann = ann_fixture().await;
    let view = ann.view().await;
    let query = ann.unindexed[0].1.clone();
    for params in [AnnParams::default(), exact()] {
        let exec = exec(
            &view,
            &query,
            10,
            params,
            Some(ids([999_999])),
            AnnConfig::default(),
        );
        assert!(search(&exec).await.is_empty());
    }
    ann.shutdown().await;
}

#[tokio::test]
async fn a_filter_matching_only_tail_docs_uses_the_tail() {
    let ann = ann_fixture().await;
    let view = ann.view().await;
    let rows = live_rows(&view).await;
    let chosen: Vec<u64> = ann.tail_new[..5].iter().map(|(pk, _)| *pk).collect();
    let allowed: BTreeSet<PrimaryKey> = chosen.iter().map(|pk| PrimaryKey::U64(*pk)).collect();
    let query = ann.unindexed[0].1.clone();
    let exec = exec(
        &view,
        &query,
        10,
        AnnParams::default(),
        Some(ids(chosen)),
        AnnConfig::default(),
    );
    let hits = search(&exec).await;
    assert_eq!(exec.strategy(), Some(AnnStrategy::BruteForceAllowed));
    assert_eq!(hits.len(), 5);
    assert!(hits.iter().all(|hit| hit.row_id >= TAIL_ROWID_BASE));
    assert_eq!(
        bits(&hits),
        brute(&rows, Distance::Cosine, &query, 10, Some(&allowed))
    );
    ann.shutdown().await;
}

/// Live durable keys of the view, ascending.
fn durable_keys(rows: &[(u64, PrimaryKey, Vec<f32>)]) -> Vec<u64> {
    rows.iter()
        .filter(|(row, _, _)| *row < TAIL_ROWID_BASE)
        .map(|(_, pk, _)| match pk {
            PrimaryKey::U64(pk) => *pk,
            other => panic!("{other:?}"),
        })
        .collect()
}

#[tokio::test]
async fn a_selective_filter_uses_brute_force_exactly() {
    let ann = ann_fixture().await;
    let view = ann.view().await;
    let rows = live_rows(&view).await;
    let mut rng = ChaCha8Rng::seed_from_u64(65);
    let keys = durable_keys(&rows);
    let chosen: Vec<u64> = keys.choose_multiple(&mut rng, 300).copied().collect();
    let allowed: BTreeSet<PrimaryKey> = chosen.iter().map(|pk| PrimaryKey::U64(*pk)).collect();
    for _ in 0..5 {
        let query = unit_vector(&mut rng);
        let exec = exec(
            &view,
            &query,
            10,
            AnnParams::default(),
            Some(ids(chosen.clone())),
            AnnConfig::default(),
        );
        let hits = search(&exec).await;
        assert_eq!(exec.strategy(), Some(AnnStrategy::BruteForceAllowed));
        assert_eq!(
            bits(&hits),
            brute(&rows, Distance::Cosine, &query, 10, Some(&allowed))
        );
        let exact_hits = search(&exec_filtered(&view, &query, exact(), &chosen)).await;
        assert_eq!(bits(&hits), bits(&exact_hits));
    }
    ann.shutdown().await;
}

fn exec_filtered(view: &Arc<ReadView>, query: &[f32], params: AnnParams, keys: &[u64]) -> AnnExec {
    exec(
        view,
        query,
        10,
        params,
        Some(ids(keys.iter().copied())),
        AnnConfig::default(),
    )
}

/// A config whose brute-force bound (100 rows) lets small filters use the
/// index.
fn index_config() -> AnnConfig {
    AnnConfig {
        brute_force_min_rows: 100,
        ..AnnConfig::default()
    }
}

#[tokio::test]
async fn a_moderate_filter_prefilters() {
    let ann = ann_fixture().await;
    let view = ann.view().await;
    let rows = live_rows(&view).await;
    let keys = durable_keys(&rows);
    // Every 11th live durable row: about 9 % of them.
    let chosen: Vec<u64> = keys.iter().step_by(11).copied().collect();
    let allowed: BTreeSet<PrimaryKey> = chosen.iter().map(|pk| PrimaryKey::U64(*pk)).collect();
    let mut rng = ChaCha8Rng::seed_from_u64(66);
    let mut found = 0;
    for _ in 0..10 {
        let query = unit_vector(&mut rng);
        let exec = exec(
            &view,
            &query,
            10,
            AnnParams::default(),
            Some(ids(chosen.clone())),
            index_config(),
        );
        let hits = search(&exec).await;
        assert_eq!(exec.strategy(), Some(AnnStrategy::Prefilter));
        assert_eq!(hits.len(), 10);
        assert!(hits.iter().all(|hit| allowed.contains(&hit.pk)));
        assert!(sorted(&hits));
        assert_exact_scores(&hits, &rows, &query);
        let truth: BTreeSet<PrimaryKey> =
            brute(&rows, Distance::Cosine, &query, 10, Some(&allowed))
                .into_iter()
                .map(|(pk, _)| pk)
                .collect();
        found += hits.iter().filter(|hit| truth.contains(&hit.pk)).count();
    }
    // Lance 12.0.0 trains the IVF centroids and the PQ codebooks with
    // unseeded k-means (`KMeansParams::new` draws its seed from the OS, and
    // neither `IvfBuildParams` nor `PQBuildParams` exposes one), so the
    // index, and this recall, differ from run to run. Twenty runs of this
    // test measured 86–96/100 (4 runs below the first bound, 90), and 21
    // more (Task 15) 83–96/100; the bound is the first minimum less a
    // margin of 10: it still catches a collapse of recall without failing
    // on an unlucky training.
    assert!(found >= 76, "prefiltered recall@10 {found}/100");
    ann.shutdown().await;
}

#[tokio::test]
async fn a_broad_filter_postfilters_and_retries() {
    let ann = ann_fixture().await;
    let view = ann.view().await;
    let rows = live_rows(&view).await;
    let (_, anchor, query) = rows
        .iter()
        .find(|(row, _, _)| *row < TAIL_ROWID_BASE)
        .cloned()
        .expect("a durable row");
    // The 60 nearest durable rows are excluded; of the rest, keys ≡ 0, 1, 2
    // (mod 5) are allowed: about 60 % of the durable rows.
    let durable: Vec<(u64, PrimaryKey, Vec<f32>)> = rows
        .iter()
        .filter(|(row, _, _)| *row < TAIL_ROWID_BASE)
        .cloned()
        .collect();
    let near: BTreeSet<PrimaryKey> = brute(&durable, Distance::Cosine, &query, 60, None)
        .into_iter()
        .map(|(pk, _)| pk)
        .collect();
    assert!(near.contains(&anchor));
    let chosen: Vec<u64> = durable_keys(&rows)
        .into_iter()
        .filter(|pk| pk % 5 < 3 && !near.contains(&PrimaryKey::U64(*pk)))
        .collect();
    let allowed: BTreeSet<PrimaryKey> = chosen.iter().map(|pk| PrimaryKey::U64(*pk)).collect();
    let exec = exec(
        &view,
        &query,
        10,
        AnnParams::default(),
        Some(ids(chosen)),
        index_config(),
    );
    let hits = search(&exec).await;
    assert_eq!(exec.strategy(), Some(AnnStrategy::Postfilter));
    let retries = exec
        .metrics()
        .expect("metrics")
        .sum_by_name("retries")
        .map(|m| m.as_usize());
    assert_eq!(retries, Some(1), "one retry");
    assert_eq!(hits.len(), 10);
    assert!(hits.iter().all(|hit| allowed.contains(&hit.pk)));
    assert_exact_scores(&hits, &rows, &query);
    ann.shutdown().await;
}

#[tokio::test]
async fn a_wrong_dimension_is_invalid() {
    let fixture = TailFixture::start(ann_schema(), 1).await;
    let mut rng = ChaCha8Rng::seed_from_u64(67);
    let ops: Vec<DocOp> = (0..5)
        .map(|pk| with_vector(pk, unit_vector(&mut rng)))
        .collect();
    fixture.append_all(&ops).await;
    let reads = fixture.reads(TailConfig::default(), ReadConfig::default());
    let view = Arc::new(
        fixture
            .view(&reads, &ReadConsistency::Strong)
            .await
            .expect("view"),
    );
    let err = exec_with(&view, &[1.0; 15], 5, AnnParams::default())
        .search()
        .await
        .expect_err("a wrong dimension");
    assert_eq!(
        err,
        ServiceError::InvalidArgument("vector v has 16 dimensions, the query has 15".to_string())
    );
    let unknown = AnnExec::new(
        view.clone(),
        "w".to_string(),
        vec![1.0; 16],
        5,
        AnnParams::default(),
        None,
        AnnConfig::default(),
    );
    assert_eq!(
        unknown.search().await.expect_err("unknown"),
        ServiceError::InvalidArgument("unknown vector w".to_string())
    );
    // Without an index the search is exact over the tail.
    let hits = search(&exec_with(&view, &[1.0; 16], 5, AnnParams::default())).await;
    assert_eq!(hits.len(), 5);
    reads.shutdown().await;
    fixture.shutdown().await;
}

// ----- the hot tier -----

/// A hot artifact over the first 2 000 row ids of the first commit: exact
/// scores plus 0.01, and 5 rows deleted since then on top of every answer.
#[derive(Debug)]
struct FakeHot {
    covered: RoaringTreemap,
    vectors: BTreeMap<u64, Vec<f32>>,
    dead: Vec<u64>,
}

#[async_trait::async_trait]
impl HotAnn for FakeHot {
    fn source_version(&self) -> u64 {
        1
    }

    fn covered(&self) -> &RoaringTreemap {
        &self.covered
    }

    async fn search(
        &self,
        query: &[f32],
        k: usize,
        allow: Option<&RoaringTreemap>,
        _ef: Option<u32>,
    ) -> Result<Vec<(u64, f32)>, HotError> {
        let mut scored: Vec<(u64, f32)> = self
            .vectors
            .iter()
            .filter(|(row, _)| allow.is_none_or(|a| a.contains(**row)))
            .map(|(row, v)| (*row, score(Distance::Cosine, query, v) + 0.01))
            .collect();
        scored.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        let mut out: Vec<(u64, f32)> = self.dead.iter().map(|row| (*row, 9.0)).collect();
        out.extend(scored.into_iter().take(k.saturating_sub(out.len())));
        Ok(out)
    }
}

/// A hot tier serving one artifact, counting `ann` calls.
#[derive(Debug)]
struct FakeTier {
    ann: Arc<FakeHot>,
    calls: Arc<AtomicUsize>,
}

impl HotTier for FakeTier {
    fn ann(
        &self,
        _: NamespaceId,
        _: CollectionId,
        column: &str,
        _: u64,
    ) -> Option<Arc<dyn HotAnn>> {
        assert_eq!(column, "_vector_0");
        self.calls.fetch_add(1, Ordering::SeqCst);
        Some(self.ann.clone())
    }

    fn split_file(
        &self,
        _: NamespaceId,
        _: CollectionId,
        _: ulid::Ulid,
    ) -> Option<std::path::PathBuf> {
        None
    }

    fn record_access(&self, _: NamespaceId, _: CollectionId) {}
}

fn fake_tier(ann: &Ann) -> (Arc<FakeTier>, Arc<AtomicUsize>) {
    let covered: RoaringTreemap = ann
        .initial
        .keys()
        .copied()
        .filter(|row| *row < 2_000)
        .collect();
    let vectors = ann
        .initial
        .iter()
        .filter(|(row, _)| covered.contains(**row))
        .map(|(row, v)| (*row, v.clone()))
        .collect();
    let dead: Vec<u64> = ann
        .initial_keys
        .iter()
        .filter(|(row, pk)| covered.contains(**row) && ann.deleted.contains(pk))
        .map(|(row, _)| *row)
        .take(5)
        .collect();
    assert_eq!(dead.len(), 5);
    let calls = Arc::new(AtomicUsize::new(0));
    let tier = Arc::new(FakeTier {
        ann: Arc::new(FakeHot {
            covered,
            vectors,
            dead,
        }),
        calls: calls.clone(),
    });
    (tier, calls)
}

async fn hot_view(ann: &Ann, enabled: bool, tier: Arc<dyn HotTier>) -> Arc<ReadView> {
    let collection = ann.fixture.collection().await;
    let hot = RequestHot {
        enabled,
        used: Default::default(),
    };
    Arc::new(
        ann.reads
            .view(
                ann.fixture.ns,
                &collection,
                &ReadConsistency::Strong,
                &hot,
                tier,
            )
            .await
            .expect("view"),
    )
}

#[tokio::test]
async fn hot_results_are_rescored_and_merged_with_uncovered_rows() {
    let ann = ann_fixture().await;
    let (tier, calls) = fake_tier(&ann);
    let dead: BTreeSet<u64> = tier.ann.dead.iter().copied().collect();
    let view = hot_view(&ann, true, tier.clone()).await;
    let rows = live_rows(&view).await;
    let mut rng = ChaCha8Rng::seed_from_u64(68);
    for _ in 0..10 {
        let query = unit_vector(&mut rng);
        let exec = exec_with(&view, &query, 10, AnnParams::default());
        let hits = search(&exec).await;
        assert_eq!(exec.strategy(), Some(AnnStrategy::Hot));
        assert_eq!(hits.len(), 10);
        assert!(hits.iter().all(|hit| !dead.contains(&hit.row_id)));
        assert!(sorted(&hits));
        assert_exact_scores(&hits, &rows, &query);
    }
    // An uncovered row's own vector finds it first: a durable row past the
    // covered ids, an unindexed row, and a tail row.
    let (row, pk, v) = rows
        .iter()
        .find(|(row, _, _)| (2_000..TAIL_ROWID_BASE).contains(row))
        .cloned()
        .expect("an uncovered durable row");
    let hits = search(&exec_with(&view, &v, 5, AnnParams::default())).await;
    assert_eq!((hits[0].row_id, &hits[0].pk), (row, &pk));
    for (pk, v) in [&ann.unindexed[7], &ann.tail_new[3]] {
        let hits = search(&exec_with(&view, v, 5, AnnParams::default())).await;
        assert_eq!(hits[0].pk, PrimaryKey::U64(*pk));
    }
    // A filter goes to the artifact too.
    let chosen: Vec<u64> = durable_keys(&rows)
        .into_iter()
        .filter(|pk| pk % 2 == 0)
        .collect();
    let allowed: BTreeSet<PrimaryKey> = chosen.iter().map(|pk| PrimaryKey::U64(*pk)).collect();
    let query = unit_vector(&mut rng);
    let exec = exec(
        &view,
        &query,
        10,
        AnnParams::default(),
        Some(ids(chosen)),
        index_config(),
    );
    let hits = search(&exec).await;
    assert_eq!(exec.strategy(), Some(AnnStrategy::Hot));
    assert!(hits.iter().all(|hit| allowed.contains(&hit.pk)));
    assert_exact_scores(&hits, &rows, &query);
    assert!(calls.load(Ordering::SeqCst) > 0);
    assert_eq!(
        view.hot_used.kinds().into_iter().collect::<Vec<_>>(),
        vec![HotKind::Hnsw]
    );
    // Exact search never asks the hot tier.
    let before = calls.load(Ordering::SeqCst);
    search(&exec_with(&view, &query, 10, exact())).await;
    assert_eq!(calls.load(Ordering::SeqCst), before);
    ann.shutdown().await;
}

#[tokio::test]
async fn hot_off_never_calls_the_hot_tier() {
    let ann = ann_fixture().await;
    let (tier, calls) = fake_tier(&ann);
    let off = hot_view(&ann, false, tier).await;
    let none = hot_view(&ann, true, Arc::new(NoHotTier)).await;
    let mut rng = ChaCha8Rng::seed_from_u64(69);
    for _ in 0..5 {
        let query = unit_vector(&mut rng);
        let off_hits = search(&exec_with(&off, &query, 10, AnnParams::default())).await;
        let none_hits = search(&exec_with(&none, &query, 10, AnnParams::default())).await;
        assert_eq!(bits(&off_hits), bits(&none_hits));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(off.hot_used.kinds().is_empty());
    ann.shutdown().await;
}
