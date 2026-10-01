//! The hot on/off differential harness (plan M1.3 Task 12): seeded
//! workloads whose exact queries answer identically with the hot tier on
//! and off, and whose approximate queries keep R12's rule; and two broken
//! hot tiers the harness must catch.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};

use futures::StreamExt;
use loams_common::{CollectionId, NamespaceId};
use loams_hnsw::{FlatEngine, HnswEngine};
use loams_hot::HotTierImpl;
use loams_hot::differential::{
    DiffConfig, DiffFixture, DiffHarness, DiffReport, Model, QueryClass, generate, generate_with,
};
use loams_query::hot::{HotAnn, HotError, HotStatus, HotTier};
use roaring::RoaringTreemap;
use ulid::Ulid;

use crate::common::Fixture;

/// `DIFF_SEED` runs one seed; else `DIFF_SEEDS` seeds (4 in debug builds,
/// 16 in release builds).
fn seeds() -> Vec<u64> {
    if let Ok(seed) = std::env::var("DIFF_SEED") {
        return vec![seed.parse().expect("DIFF_SEED is a u64")];
    }
    let count = match std::env::var("DIFF_SEEDS") {
        Ok(n) => n.parse().expect("DIFF_SEEDS is a count"),
        Err(_) if cfg!(debug_assertions) => 4,
        Err(_) => 16,
    };
    (1..=count).collect()
}

fn plain(tier: HotTierImpl) -> Arc<dyn HotTier> {
    Arc::new(tier)
}

/// Runs the harness once on a fresh fixture.
async fn run(
    config: &DiffConfig,
    engine: Arc<dyn HnswEngine>,
    wrap: impl FnOnce(HotTierImpl) -> Arc<dyn HotTier>,
) -> DiffReport {
    run_with(config, engine, wrap, |_, _| {}).await
}

/// [`run`], with `adjust` applied to the harness's fixture first.
async fn run_with(
    config: &DiffConfig,
    engine: Arc<dyn HnswEngine>,
    wrap: impl FnOnce(HotTierImpl) -> Arc<dyn HotTier>,
    adjust: impl FnOnce(&Fixture, &mut DiffFixture),
) -> DiffReport {
    let f = Fixture::start_diff().await;
    let mut fixture = f.diff_fixture("diff", engine, wrap).await;
    adjust(&f, &mut fixture);
    let mut harness = DiffHarness::new(fixture).await;
    let report = harness.run(config).await;
    harness.fixture().tier.shutdown().await;
    drop(harness);
    f.shutdown().await;
    report
}

fn assert_ok(report: &DiffReport) {
    assert!(report.is_ok(), "{}", report.describe());
    eprintln!("{}", report.describe());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_on_and_off_are_identical_on_seeded_workloads() {
    // Two seeds at a time: each is CPU-bound for minutes in a debug build.
    let reports: Vec<DiffReport> = futures::stream::iter(seeds())
        .map(|seed| async move { run(&DiffConfig::new(seed), Arc::new(FlatEngine), plain).await })
        .buffer_unordered(2)
        .collect()
        .await;
    for report in &reports {
        assert_ok(report);
    }
}

#[cfg(feature = "hnsw")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_qdrant_engine_passes_one_seed() {
    let report = run(
        &DiffConfig::new(101),
        Arc::new(loams_hnsw::QdrantEngine),
        plain,
    )
    .await;
    assert_ok(&report);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_per_request_disabling_matches_cold() {
    let config = DiffConfig {
        queries_per_phase: 20,
        phase_queries: [("applied", 1_000)].into(),
        ..DiffConfig::new(0xD15A_B1ED)
    };
    let report = run(&config, Arc::new(FlatEngine), plain).await;
    let applied = report
        .phases
        .iter()
        .find(|p| p.phase == "applied")
        .expect("the applied phase");
    assert_eq!(applied.identical + applied.approximate, 1_000);
    assert_ok(&report);
}

/// A small workload for the broken tiers.
fn small(seed: u64) -> DiffConfig {
    DiffConfig {
        docs: 600,
        queries_per_phase: 40,
        ..DiffConfig::new(seed)
    }
}

/// Answers every hot search with one row the view does not cover (a row
/// deleted since the artifact when there is one) instead of its hits.
#[derive(Debug)]
struct DeletedRowTier(HotTierImpl);

#[derive(Debug)]
struct DeletedRowAnn {
    inner: Arc<dyn HotAnn>,
    row: u64,
}

#[async_trait::async_trait]
impl HotAnn for DeletedRowAnn {
    fn source_version(&self) -> u64 {
        self.inner.source_version()
    }

    fn covered(&self) -> &RoaringTreemap {
        self.inner.covered()
    }

    async fn search(
        &self,
        _: &[f32],
        _: usize,
        _: Option<&RoaringTreemap>,
        _: Option<u32>,
    ) -> Result<Vec<(u64, f32)>, HotError> {
        Ok(vec![(self.row, 1.0)])
    }
}

impl HotTier for DeletedRowTier {
    fn ann(
        &self,
        ns: NamespaceId,
        cid: CollectionId,
        column: &str,
        manifest_version: u64,
    ) -> Option<Arc<dyn HotAnn>> {
        let inner = self.0.ann(ns, cid, column, manifest_version)?;
        let view = self.0.column_view(ns, cid, column, manifest_version)?;
        let deleted = &view.artifact().scanned - inner.covered();
        let row = deleted
            .min()
            .unwrap_or_else(|| inner.covered().max().map_or(0, |m| m + 1_000_000));
        Some(Arc::new(DeletedRowAnn { inner, row }))
    }

    fn split_file(&self, ns: NamespaceId, cid: CollectionId, split: Ulid) -> Option<PathBuf> {
        self.0.split_file(ns, cid, split)
    }

    fn record_access(&self, ns: NamespaceId, cid: CollectionId) {
        self.0.record_access(ns, cid);
    }

    fn status(&self, ns: NamespaceId, cid: CollectionId) -> HotStatus {
        self.0.status(ns, cid)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hot_tier_that_returns_a_deleted_row_is_caught() {
    let report = run(&small(7), Arc::new(FlatEngine), |tier| {
        Arc::new(DeletedRowTier(tier))
    })
    .await;
    assert!(
        report
            .mismatches
            .iter()
            .any(|m| m.query.class == QueryClass::Approximate),
        "{}",
        report.describe()
    );
}

/// Answers every hot search with every other hit: valid rows at exact
/// scores, so only the recall gate can see it.
#[derive(Debug)]
struct HalfHitsTier(HotTierImpl);

#[derive(Debug)]
struct HalfHitsAnn(Arc<dyn HotAnn>);

#[async_trait::async_trait]
impl HotAnn for HalfHitsAnn {
    fn source_version(&self) -> u64 {
        self.0.source_version()
    }

    fn covered(&self) -> &RoaringTreemap {
        self.0.covered()
    }

    async fn search(
        &self,
        query: &[f32],
        k: usize,
        filter: Option<&RoaringTreemap>,
        ef: Option<u32>,
    ) -> Result<Vec<(u64, f32)>, HotError> {
        let hits = self.0.search(query, k, filter, ef).await?;
        Ok(hits.into_iter().step_by(2).collect())
    }
}

impl HotTier for HalfHitsTier {
    fn ann(
        &self,
        ns: NamespaceId,
        cid: CollectionId,
        column: &str,
        manifest_version: u64,
    ) -> Option<Arc<dyn HotAnn>> {
        let inner = self.0.ann(ns, cid, column, manifest_version)?;
        Some(Arc::new(HalfHitsAnn(inner)))
    }

    fn split_file(&self, ns: NamespaceId, cid: CollectionId, split: Ulid) -> Option<PathBuf> {
        self.0.split_file(ns, cid, split)
    }

    fn record_access(&self, ns: NamespaceId, cid: CollectionId) {
        self.0.record_access(ns, cid);
    }

    fn status(&self, ns: NamespaceId, cid: CollectionId) -> HotStatus {
        self.0.status(ns, cid)
    }
}

/// Ruling C4: the hot tier is still judged on a small pool. No phase has
/// 20 approximate queries, and a tier that drops every other hit (valid
/// rows at exact scores) is caught only by the pool.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_pool_catches_a_hot_tier_that_drops_hits() {
    let report = run(&small(10), Arc::new(FlatEngine), |tier| {
        Arc::new(HalfHitsTier(tier))
    })
    .await;
    assert!(
        report.phases.iter().all(|p| p.approximate < 20),
        "{}",
        report.describe()
    );
    assert!(
        report
            .mismatches
            .iter()
            .any(|m| m.phase == "pooled" && m.reason.contains("of the hot tier")),
        "{}",
        report.describe()
    );
}

/// Serves each split from the pinned file of the next split it has been
/// asked for (in ULID order), once it knows two.
#[derive(Debug)]
struct WrongSplitTier {
    inner: HotTierImpl,
    known: Mutex<BTreeSet<Ulid>>,
}

impl HotTier for WrongSplitTier {
    fn ann(
        &self,
        ns: NamespaceId,
        cid: CollectionId,
        column: &str,
        manifest_version: u64,
    ) -> Option<Arc<dyn HotAnn>> {
        self.inner.ann(ns, cid, column, manifest_version)
    }

    fn split_file(&self, ns: NamespaceId, cid: CollectionId, split: Ulid) -> Option<PathBuf> {
        let other = {
            let mut known = self.known.lock().unwrap_or_else(PoisonError::into_inner);
            known.insert(split);
            known
                .range(split..)
                .nth(1)
                .or_else(|| known.iter().next())
                .copied()
                .unwrap_or(split)
        };
        self.inner.split_file(ns, cid, other)
    }

    fn record_access(&self, ns: NamespaceId, cid: CollectionId) {
        self.inner.record_access(ns, cid);
    }
}

/// Row 12.7: another split's file never opens at this split's footer range
/// (`open_local` reads the footer at `SplitRef.footer_range`), so the query
/// engine catches the wrong file itself and reads the split remotely; the
/// harness then sees every exact answer unchanged while the wrong files
/// were handed out. A tier that changes exact answers is the next test.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hot_tier_that_serves_the_wrong_split_is_caught() {
    let report = run(&small(8), Arc::new(FlatEngine), |inner| {
        Arc::new(WrongSplitTier {
            inner,
            known: Mutex::default(),
        })
    })
    .await;
    assert_ok(&report);
    let loaded = &report.phases[0];
    assert!(loaded.split_files_served > 0, "{}", report.describe());
}

/// The harness's identical-class check: a hot service whose search window
/// is 20 refuses deeper requests that the cold service answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hot_side_that_changes_an_exact_answer_is_caught() {
    let report = run_with(&small(9), Arc::new(FlatEngine), plain, |f, fixture| {
        let hot = f.diff_service_with(|config| config.search.limits.max_window = 20);
        hot.set_hot_tier(Arc::new(fixture.tier.clone()));
        fixture.hot = hot;
    })
    .await;
    assert!(
        report
            .mismatches
            .iter()
            .any(|m| m.query.class == QueryClass::Identical && m.reason == "hot and cold differ"),
        "{}",
        report.describe()
    );
}

#[test]
fn the_generator_is_deterministic_per_seed() {
    let model = Model::default();
    let render = |seed: u64, phase: &str| -> Vec<String> {
        generate(seed, phase, &model, 300)
            .into_iter()
            .map(|q| format!("{q:?}"))
            .collect()
    };
    assert_eq!(render(3, "loaded"), render(3, "loaded"));
    assert_ne!(render(3, "loaded"), render(4, "loaded"));
    assert_ne!(render(3, "loaded"), render(3, "tail"));
    let queries = generate_with(3, "applied", &model, 300, 8);
    let approximate = queries
        .iter()
        .filter(|q| q.class == QueryClass::Approximate)
        .count();
    // 15 % of 300, give or take the draw.
    assert!((20..=70).contains(&approximate), "{approximate}");
}
