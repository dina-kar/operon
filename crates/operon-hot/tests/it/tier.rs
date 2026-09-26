//! `HotTierImpl` (plan M1.3 Task 6 rules 4–6): what it loads, which views
//! `ann` serves, how an artifact is replaced, failed and reloaded, and what
//! a restart leaves on disk.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use operon_collection::PrimaryKey;
use operon_common::meta::HotConfig;
use operon_hnsw::{FlatEngine, HnswEngine};
use operon_hot::{HNSW_KIND, HotBuildConfig, HotTierConfig, HotTierImpl, chunk_path, live_rows};
use operon_query::hot::HotTier;
use operon_query::placement::LocalOnly;

use crate::common::{Elsewhere, Fixture, WAIT, build_once, docs};

const COLUMN: &str = "_vector_0";

/// Pins the first collection and builds its artifact; returns the manifest
/// version of the artifact commit.
async fn build(f: &Fixture) -> u64 {
    f.pin_vectors().await;
    build_once(f, &f.source()).await.expect("build");
    f.manifest().await.version
}

/// Builds a new artifact for a stale one (every insert makes it due).
async fn rebuild(f: &Fixture) -> u64 {
    let source = f.source_with(HotBuildConfig {
        rebuild_min_inserted: 1,
        rebuild_inserted_ppm: 0,
        ..f.config()
    });
    build_once(f, &source).await.expect("rebuild");
    f.manifest().await.version
}

fn prefix(manifest: &operon_collection::CollectionManifest) -> String {
    manifest
        .hot_artifacts
        .iter()
        .find(|a| a.kind == HNSW_KIND && a.column == COLUMN)
        .expect("an artifact")
        .prefix
        .clone()
}

/// A tier configuration with its own local directory `hot-<name>`.
fn config_in(f: &Fixture, name: &str) -> HotTierConfig {
    HotTierConfig {
        dir: f.data_dir.path().join(format!("hot-{name}")),
        ..f.tier_config()
    }
}

/// The entries of `dir` (none when it does not exist).
fn entries(dir: &Path) -> Vec<PathBuf> {
    match std::fs::read_dir(dir) {
        Ok(read) => read.map(|e| e.expect("entry").path()).collect(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(err) => panic!("{}: {err}", dir.display()),
    }
}

/// Waits until `check` holds: local copies are removed on a blocking thread
/// once their last user is gone (row 6.7).
async fn eventually(what: &str, check: impl Fn() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !check() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn column_dir(f: &Fixture, config: &HotTierConfig, kind: &str) -> PathBuf {
    config
        .dir
        .join(kind)
        .join(f.ns.to_string())
        .join(f.cid.to_string())
        .join(COLUMN)
}

fn ann(
    f: &Fixture,
    tier: &HotTierImpl,
    version: u64,
) -> Option<Arc<dyn operon_query::hot::HotAnn>> {
    tier.ann(f.ns, f.cid, COLUMN, version)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_owned_pinned_collection_loads_its_artifact_and_serves_ann() {
    let f = Fixture::start().await;
    f.commit(docs(0..100)).await;
    let hot = build(&f).await;
    let s = hot - 1;
    let tier = f.tier().await;
    let report = tier.reconcile_once().await.expect("reconcile");
    assert_eq!((report.loaded, report.views, report.dropped), (1, 1, 0));
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    // Current at its own commit: the effective source is that version.
    let at_hot = ann(&f, &tier, hot).expect("a view");
    assert_eq!(at_hot.source_version(), hot);

    f.commit(docs(100..110)).await;
    let v = f.manifest().await.version;
    assert!(ann(&f, &tier, v).is_none(), "served before a reconcile");
    tier.reconcile_once().await.expect("reconcile");
    let view = ann(&f, &tier, v).expect("a view of v");
    assert_eq!(view.source_version(), s, "stale since the inserts");
    assert_eq!(
        view.covered(),
        &live_rows(&f.snapshot().await).await.expect("live")
    );

    f.commit(docs(110..120)).await;
    assert!(ann(&f, &tier, v + 1).is_none(), "v + 1 before a reconcile");
    assert!(ann(&f, &tier, v).is_some());
    let report = tier.reconcile_once().await.expect("reconcile");
    assert_eq!((report.loaded, report.views), (0, 1));
    assert!(ann(&f, &tier, v + 1).is_some());
    let counters = tier.counters();
    assert_eq!((counters.loads, counters.load_failures), (1, 0));
    assert_eq!((counters.ann_served, counters.ann_missed), (4, 2));
    tier.shutdown().await;
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ann_is_none_when_disabled_unpinned_or_not_owned() {
    let f = Fixture::start().await;
    f.commit(docs(0..50)).await;
    let v = build(&f).await;
    let flat: Arc<dyn HnswEngine> = Arc::new(FlatEngine);

    let off = HotTierConfig {
        enabled: false,
        ..config_in(&f, "off")
    };
    let disabled = f.tier_with(off, Arc::new(LocalOnly), flat.clone()).await;
    assert_eq!(
        disabled.reconcile_once().await.expect("reconcile"),
        Default::default()
    );
    assert!(ann(&f, &disabled, v).is_none(), "disabled");

    let remote = f
        .tier_with(config_in(&f, "remote"), Arc::new(Elsewhere), flat.clone())
        .await;
    let report = remote.reconcile_once().await.expect("reconcile");
    assert_eq!(report.loaded, 0);
    assert!(ann(&f, &remote, v).is_none(), "not owned");

    let tier = f
        .tier_with(config_in(&f, "local"), Arc::new(LocalOnly), flat)
        .await;
    tier.reconcile_once().await.expect("reconcile");
    assert!(ann(&f, &tier, v).is_some());
    f.pin(f.cid, HotConfig::default()).await;
    let report = tier.reconcile_once().await.expect("reconcile");
    assert_eq!(report.dropped, 1);
    assert!(ann(&f, &tier, v).is_none(), "unpinned");
    for t in [disabled, remote, tier] {
        t.shutdown().await;
    }
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_artifact_replaces_the_old_one_and_resets_the_delta() {
    let f = Fixture::start().await;
    f.commit(docs(0..50)).await;
    build(&f).await;
    let config = f.tier_config();
    let tier = f.tier().await;
    tier.reconcile_once().await.expect("reconcile");
    f.commit(docs(50..70)).await;
    let v = f.manifest().await.version;
    tier.reconcile_once().await.expect("reconcile");
    let old = tier.column_view(f.ns, f.cid, COLUMN, v).expect("a view");
    assert_eq!(old.delta().appended(), 20);
    let old_prefix = prefix(&f.manifest().await);
    drop(old);

    let rebuilt = rebuild(&f).await;
    assert_ne!(prefix(&f.manifest().await), old_prefix);
    let report = tier.reconcile_once().await.expect("reconcile");
    assert_eq!((report.loaded, report.views), (1, 1));
    let new = tier
        .column_view(f.ns, f.cid, COLUMN, rebuilt)
        .expect("a view");
    assert_eq!(new.artifact().descriptor.source_version, v);
    assert_eq!(new.delta().appended(), 0);
    assert!(new.delta().scanned().is_empty());
    assert!(new.excluded().is_empty());
    // The old views went with the old artifact.
    assert!(tier.column_view(f.ns, f.cid, COLUMN, v).is_none());
    // Only the new artifact and delta stay on disk.
    let (hnsw, delta) = (
        column_dir(&f, &config, "hnsw"),
        column_dir(&f, &config, "delta"),
    );
    eventually("the old copies are removed", || {
        entries(&hnsw) == vec![new.artifact().dir().to_owned()]
            && entries(&delta) == vec![new.delta().dir().to_owned()]
    })
    .await;
    drop(new);
    tier.shutdown().await;
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replaced_artifact_is_deleted_after_its_last_view() {
    let f = Fixture::start().await;
    f.commit(docs(0..50)).await;
    let hot = build(&f).await;
    let tier = f.tier().await;
    tier.reconcile_once().await.expect("reconcile");
    let held = ann(&f, &tier, hot).expect("a view");
    let (old_artifact, old_delta) = {
        let view = tier.column_view(f.ns, f.cid, COLUMN, hot).expect("a view");
        (
            view.artifact().dir().to_owned(),
            view.delta().dir().to_owned(),
        )
    };
    f.commit(docs(50..60)).await;
    rebuild(&f).await;
    let report = tier.reconcile_once().await.expect("reconcile");
    assert_eq!(report.loaded, 1);
    assert!(
        ann(&f, &tier, hot).is_none(),
        "the tier dropped the old view"
    );
    assert!(
        old_artifact.exists() && old_delta.exists(),
        "removed while a view holds them"
    );
    // The held view still answers.
    let hits = held
        .search(
            &crate::common::vector_of(7, crate::common::DIM),
            1,
            None,
            None,
        )
        .await
        .expect("search");
    assert_eq!(hits.len(), 1);
    drop(held);
    eventually("the old artifact and delta are removed", || {
        !old_artifact.exists() && !old_delta.exists()
    })
    .await;
    tier.shutdown().await;
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_corrupt_artifact_is_not_loaded_and_is_retried() {
    let f = Fixture::start().await;
    f.commit(docs(0..50)).await;
    let hot = build(&f).await;
    let chunk = chunk_path(&prefix(&f.manifest().await), operon_hnsw::FLAT_FILE, 0);
    let (original, _) = f.store.get(&chunk).await.expect("chunk");
    let mut flipped = original.to_vec();
    let middle = flipped.len() / 2;
    flipped[middle] ^= 0xFF;
    f.store
        .put(&chunk, Bytes::from(flipped))
        .await
        .expect("put");

    let config = f.tier_config();
    let tier = f.tier().await;
    let report = tier.reconcile_once().await.expect("reconcile");
    assert_eq!((report.loaded, report.views), (0, 0));
    assert_eq!(report.failures.len(), 1, "{:?}", report.failures);
    assert!(ann(&f, &tier, hot).is_none());
    assert_eq!(tier.counters().load_failures, 1);
    let hnsw = column_dir(&f, &config, "hnsw");
    eventually("the partial download is removed", || {
        entries(&hnsw).is_empty()
    })
    .await;

    f.store.put(&chunk, original).await.expect("restore");
    let report = tier.reconcile_once().await.expect("reconcile");
    assert_eq!((report.loaded, report.views), (1, 1));
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    assert!(ann(&f, &tier, hot).is_some());
    assert_eq!(tier.counters().load_failures, 1);
    tier.shutdown().await;
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_rebuilds_the_hot_directory() {
    let f = Fixture::start().await;
    f.commit(docs(0..50)).await;
    let hot = build(&f).await;
    let config = HotTierConfig {
        reconcile_interval: Duration::from_millis(50),
        ..f.tier_config()
    };
    let leftovers = [
        config
            .dir
            .join("hnsw/9/9/_vector_0/00000000000000000001-x/flat.bin"),
        config.dir.join("delta/9/9/_vector_0/x/wal"),
    ];
    for file in &leftovers {
        std::fs::create_dir_all(file.parent().expect("parent")).expect("mkdir");
        std::fs::write(file, b"left over").expect("write");
    }
    let other = config.dir.join("splits-not-mine");
    std::fs::write(&other, b"kept").expect("write");

    let tier = HotTierImpl::start(
        f.ctx.clone(),
        config.clone(),
        1,
        Arc::new(LocalOnly),
        Arc::new(FlatEngine),
    )
    .await
    .expect("start");
    for file in &leftovers {
        assert!(!file.exists(), "{} survived the start", file.display());
    }
    assert!(other.exists(), "only hnsw/ and delta/ are cleared");
    // The loop reloads everything from the published artifact.
    let deadline = Instant::now() + WAIT;
    while ann(&f, &tier, hot).is_none() {
        assert!(
            Instant::now() < deadline,
            "the loop never loaded the artifact"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // A commit wakes the loop: the next version is served without a manual pass.
    f.commit(docs(50..55)).await;
    let v = f.manifest().await.version;
    while ann(&f, &tier, v).is_none() {
        assert!(
            Instant::now() < deadline,
            "the loop never served the new version"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tier.shutdown().await;
    let (hnsw, delta) = (
        column_dir(&f, &config, "hnsw"),
        column_dir(&f, &config, "delta"),
    );
    eventually("shutdown removes every local copy", || {
        entries(&hnsw).is_empty() && entries(&delta).is_empty()
    })
    .await;
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_view_with_too_many_exclusions_is_not_served() {
    let f = Fixture::start().await;
    f.commit(docs(0..50)).await;
    let hot = build(&f).await;
    let config = HotTierConfig {
        max_view_exclusions: 5,
        ..f.tier_config()
    };
    let tier = f
        .tier_with(config, Arc::new(LocalOnly), Arc::new(FlatEngine))
        .await;
    tier.reconcile_once().await.expect("reconcile");
    assert!(ann(&f, &tier, hot).is_some());
    let delete = |keys: std::ops::Range<u64>| {
        keys.map(|k| operon_collection::DocOp::Delete(PrimaryKey::U64(k)))
            .collect::<Vec<_>>()
    };
    f.commit(delete(0..5)).await;
    let five = f.manifest().await.version;
    tier.reconcile_once().await.expect("reconcile");
    assert!(ann(&f, &tier, five).is_some(), "five exclusions are served");
    f.commit(delete(5..6)).await;
    let six = f.manifest().await.version;
    let report = tier.reconcile_once().await.expect("reconcile");
    assert_eq!(report.views, 0);
    assert!(ann(&f, &tier, six).is_none(), "six exclusions are not");
    assert!(ann(&f, &tier, five).is_some(), "older views stay");
    tier.shutdown().await;
    f.shutdown().await;
}

/// The view tests' probes through `QdrantEngine`: recall@10 against the
/// exact top 10 of the live rows.
#[cfg(feature = "hnsw")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_qdrant_engine_serves_views() {
    use operon_hnsw::{Distance, QdrantEngine, exact_score};

    let f = Fixture::start().await;
    f.commit(docs(0..1_000)).await;
    f.pin_vectors().await;
    let source = f.source_over(f.config(), Arc::new(QdrantEngine));
    build_once(&f, &source).await.expect("build");
    let mut ops = docs(1_000..1_200);
    ops.extend((0..50).map(|k| operon_collection::DocOp::Delete(PrimaryKey::U64(k))));
    f.commit(ops).await;
    let v = f.manifest().await.version;
    let tier = f
        .tier_with(f.tier_config(), Arc::new(LocalOnly), Arc::new(QdrantEngine))
        .await;
    let report = tier.reconcile_once().await.expect("reconcile");
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    let view = ann(&f, &tier, v).expect("a view");
    let docs = f.snapshot().await.scan_all().await.expect("scan");
    assert_eq!(view.covered().len(), docs.len() as u64);
    let (mut found, mut wanted) = (0usize, 0usize);
    for i in 0..50 {
        let query = crate::common::vector_of(2_000_000 + i, crate::common::DIM);
        let mut exact: Vec<(u64, f32)> = docs
            .iter()
            .map(|d| {
                (
                    d.row_id,
                    exact_score(Distance::Cosine, &query, &d.vectors[""]),
                )
            })
            .collect();
        exact.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        let truth: std::collections::BTreeSet<u64> = exact.iter().take(10).map(|h| h.0).collect();
        let hits = view.search(&query, 10, None, None).await.expect("search");
        assert!(hits.iter().all(|(id, _)| view.covered().contains(*id)));
        found += hits.iter().filter(|(id, _)| truth.contains(id)).count();
        wanted += truth.len();
    }
    let recall = found as f64 / wanted as f64;
    assert!(recall >= 0.95, "recall@10 {recall}");
    tier.shutdown().await;
    f.shutdown().await;
}

// ----- Task 7: budgets, heat, promotion and warm -----

const DIM: u32 = crate::common::DIM;

/// Whether the promotion lease of the first collection is held now.
async fn promotion_held(f: &Fixture) -> Option<String> {
    use operon_common::meta::MetaStore;
    let lease = f
        .meta
        .client
        .lease(
            operon_meta::Consistency::Linearizable,
            &operon_hot::promote_lease_key(f.ns, f.cid),
        )
        .await
        .expect("lease")?;
    match lease.is_held_at(f.ctx.meta.now_ms()) {
        true => lease.owner,
        false => None,
    }
}

/// A tier with auto-promotion and a short heat window.
fn promoting(f: &Fixture, name: &str, window: Duration) -> HotTierConfig {
    HotTierConfig {
        auto_promote: true,
        heat_window: window,
        ..config_in(f, name)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_demoted_artifact_stays_usable_by_a_running_query() {
    let f = Fixture::start().await;
    f.commit(docs(0..60)).await;
    let v = build(&f).await;
    let config = config_in(&f, "demote");
    let tier = f
        .tier_with(config.clone(), Arc::new(LocalOnly), Arc::new(FlatEngine))
        .await;
    tier.reconcile_once().await.expect("reconcile");
    let held = ann(&f, &tier, v).expect("a view");
    let dir = tier
        .column_view(f.ns, f.cid, COLUMN, v)
        .expect("a view")
        .artifact()
        .dir()
        .to_path_buf();
    let kinds: Vec<_> = tier.resident().iter().map(|r| r.kind).collect();
    assert_eq!(
        kinds,
        [
            operon_hot::StructureKind::Hnsw,
            operon_hot::StructureKind::Delta
        ]
    );

    tier.set_budget(operon_hot::Budget {
        nvme_bytes: 0,
        ..tier.budget()
    });
    let report = tier.reconcile_once().await.expect("reconcile");
    assert!(report.evicted >= 1, "{report:?}");
    assert!(ann(&f, &tier, v).is_none(), "served after its eviction");
    assert!(tier.resident().is_empty());
    // The running query's view still searches, from its files.
    let hits = held
        .search(&crate::common::vector_of(7, DIM), 5, None, None)
        .await
        .expect("search");
    assert_eq!(hits.len(), 5);
    assert!(dir.exists());
    // Not reloaded while it does not fit.
    let report = tier.reconcile_once().await.expect("reconcile");
    assert_eq!(report.loaded, 0);
    drop(hits);
    drop(held);
    eventually("the demoted artifact's directory is removed", || {
        !dir.exists()
    })
    .await;
    tier.shutdown().await;
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_loaded_artifacts_bounds_the_open_artifacts() {
    let f = Fixture::start().await;
    let other = f.create("more", crate::common::hot_schema()).await;
    f.write_to(f.coll, docs(0..40)).await;
    f.write_to(other, docs(0..40)).await;
    f.apply(&[f.coll, other]).await;
    let vectors = HotConfig {
        vectors: true,
        ..HotConfig::default()
    };
    f.pin(f.cid, vectors).await;
    f.pin(other.cid, vectors).await;
    let results = crate::common::run_all(&f, &f.source(), "builder").await;
    assert_eq!(results.len(), 2, "{results:?}");
    let tier = f
        .tier_with(
            HotTierConfig {
                max_loaded_artifacts: 1,
                ..config_in(&f, "max")
            },
            Arc::new(LocalOnly),
            Arc::new(FlatEngine),
        )
        .await;
    // The second may displace the first within the pass if it is smaller
    // (a higher heat per byte); either way one stays open.
    let report = tier.reconcile_once().await.expect("reconcile");
    assert!(report.loaded >= 1, "{report:?}");
    let artifacts = tier
        .resident()
        .into_iter()
        .filter(|r| r.kind == operon_hot::StructureKind::Hnsw)
        .count();
    assert_eq!(artifacts, 1);
    // Stable from then on: the open one is not displaced back.
    let report = tier.reconcile_once().await.expect("reconcile");
    assert_eq!((report.loaded, report.evicted), (0, 0));
    tier.shutdown().await;
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_promotion_is_off_by_default() {
    let f = Fixture::start().await;
    f.commit(docs(0..30)).await;
    let config = config_in(&f, "default");
    assert!(!config.auto_promote);
    let tier = f
        .tier_with(config, Arc::new(LocalOnly), Arc::new(FlatEngine))
        .await;
    for _ in 0..200 {
        tier.record_access(f.ns, f.cid);
    }
    assert!(tier.heat(f.ns, f.cid) >= 200);
    let report = tier.reconcile_once().await.expect("reconcile");
    assert_eq!(report.pinned_splits, 0);
    assert_eq!(promotion_held(&f).await, None);
    assert!(tier.resident().is_empty());
    let ulid = f.manifest().await.splits[0].ulid;
    assert!(tier.split_file(f.ns, f.cid, ulid).is_none());
    tier.shutdown().await;
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hot_collection_is_promoted_and_holds_the_lease() {
    let f = Fixture::start().await;
    f.commit(docs(0..50)).await;
    let tier = f
        .tier_with(
            promoting(&f, "promote", Duration::from_secs(60)),
            Arc::new(LocalOnly),
            Arc::new(FlatEngine),
        )
        .await;
    for _ in 0..100 {
        tier.record_access(f.ns, f.cid);
    }
    let report = tier.reconcile_once().await.expect("reconcile");
    assert_eq!(
        promotion_held(&f).await.as_deref(),
        Some("1;vectors,text,fragments")
    );
    // Text is promoted too: its splits are pinned in the same pass.
    assert!(report.pinned_splits >= 1, "{report:?}");
    // The lease makes the collection hot for workers: they build.
    build_once(&f, &f.source()).await.expect("build");
    let v = f.manifest().await.version;
    let report = tier.reconcile_once().await.expect("reconcile");
    assert_eq!(report.loaded, 1, "{report:?}");
    assert!(ann(&f, &tier, v).is_some());
    let classes: Vec<_> = tier.resident().iter().map(|r| r.class).collect();
    assert!(
        classes.iter().all(|c| *c == operon_hot::HotClass::Promoted),
        "{classes:?}"
    );
    tier.shutdown().await;
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cooled_collection_is_demoted_and_releases_the_lease() {
    let f = Fixture::start().await;
    f.commit(docs(0..50)).await;
    let window = Duration::from_millis(50);
    let tier = f
        .tier_with(
            promoting(&f, "cool", window),
            Arc::new(LocalOnly),
            Arc::new(FlatEngine),
        )
        .await;
    for _ in 0..100 {
        tier.record_access(f.ns, f.cid);
    }
    tier.reconcile_once().await.expect("reconcile");
    assert!(promotion_held(&f).await.is_some());
    let ulid = f.manifest().await.splits[0].ulid;
    assert!(tier.split_file(f.ns, f.cid, ulid).is_some());
    // 100 → 50 → 25 → 12 → 6 → 3: five windows to fall under 4.
    let deadline = Instant::now() + WAIT;
    while promotion_held(&f).await.is_some() {
        assert!(Instant::now() < deadline, "never demoted");
        tokio::time::sleep(window).await;
        tier.reconcile_once().await.expect("reconcile");
    }
    assert!(tier.heat(f.ns, f.cid) < 4);
    assert!(tier.split_file(f.ns, f.cid, ulid).is_none());
    // Hits again promote it again.
    for _ in 0..100 {
        tier.record_access(f.ns, f.cid);
    }
    tier.reconcile_once().await.expect("reconcile");
    assert!(promotion_held(&f).await.is_some());
    tier.shutdown().await;
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn warm_loads_an_unpinned_collection_until_its_heat_decays() {
    let f = Fixture::start().await;
    f.commit(docs(0..50)).await;
    // An artifact built while pinned; then unpinned.
    let v = build(&f).await;
    f.pin(f.cid, HotConfig::default()).await;
    let window = Duration::from_millis(100);
    let tier = f
        .tier_with(
            HotTierConfig {
                heat_window: window,
                ..config_in(&f, "warm")
            },
            Arc::new(LocalOnly),
            Arc::new(FlatEngine),
        )
        .await;
    tier.reconcile_once().await.expect("reconcile");
    assert!(ann(&f, &tier, v).is_none(), "unpinned");

    tier.warm(f.ns, f.cid).await.expect("warm");
    assert!(ann(&f, &tier, v).is_some(), "warm loads the artifact");
    let ulid = f.manifest().await.splits[0].ulid;
    assert!(tier.split_file(f.ns, f.cid, ulid).is_some());
    // Warm never writes the metastore.
    assert_eq!(promotion_held(&f).await, None);

    let deadline = Instant::now() + WAIT;
    while ann(&f, &tier, v).is_some() {
        assert!(Instant::now() < deadline, "never cooled");
        tokio::time::sleep(window).await;
        tier.reconcile_once().await.expect("reconcile");
    }
    assert!(tier.split_file(f.ns, f.cid, ulid).is_none());

    // A disabled tier refuses.
    let off = f
        .tier_with(
            HotTierConfig {
                enabled: false,
                ..config_in(&f, "warm-off")
            },
            Arc::new(LocalOnly),
            Arc::new(FlatEngine),
        )
        .await;
    assert!(off.warm(f.ns, f.cid).await.is_err());
    tier.shutdown().await;
    f.shutdown().await;
}
