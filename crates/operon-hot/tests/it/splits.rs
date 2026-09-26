//! Pinned splits and fragment prefetch (plan M1.3 Task 7 rules 1 and 2):
//! whole split files on local disk served through `split_file`, their
//! linger, failed and corrupt downloads, and Lance files read ahead into the
//! range cache.

use std::path::Path;
use std::time::{Duration, Instant};

use bytes::Bytes;
use operon_collection::{
    DocOp, MaintenanceConfig, PrimaryKey, SplitMergeSource, lance_prefix, split_path,
};
use operon_common::meta::HotConfig;
use operon_hot::{
    FragmentProgress, HotTierConfig, HotTierImpl, download_split, prefetch_fragments,
    prefetch_fragments_resuming,
};
use operon_query::hot::HotTier;
use operon_quickwit::merge_policy::StableLogMergePolicyConfig;
use operon_store::{Fault, Op};
use operon_worker::run_once;

use crate::common::{Fixture, TTL, WAIT, docs};

const TEXT: HotConfig = HotConfig {
    vectors: false,
    text: true,
    fragments: false,
};

const FRAGMENTS: HotConfig = HotConfig {
    vectors: false,
    text: false,
    fragments: true,
};

fn config_with(f: &Fixture, linger: Duration) -> HotTierConfig {
    HotTierConfig {
        split_linger: linger,
        ..f.tier_config()
    }
}

async fn tier_with(f: &Fixture, config: HotTierConfig) -> HotTierImpl {
    f.tier_with(
        config,
        std::sync::Arc::new(operon_query::placement::LocalOnly),
        std::sync::Arc::new(operon_hnsw::FlatEngine),
    )
    .await
}

async fn object(f: &Fixture, path: &str) -> Bytes {
    f.store.get(path).await.expect("get").0
}

/// Waits until `check` holds.
async fn eventually(what: &str, check: impl Fn() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !check() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pinned_splits_are_downloaded_whole_and_served() {
    let f = Fixture::start().await;
    f.commit(docs(0..50)).await;
    f.commit(docs(50..80)).await;
    f.pin(f.cid, TEXT).await;
    let tier = f.tier().await;
    let manifest = f.manifest().await;
    assert!(manifest.splits.len() >= 2, "{:?}", manifest.splits);
    let report = tier.reconcile_once().await.expect("reconcile");
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    assert_eq!(report.pinned_splits as usize, manifest.splits.len());
    for split in &manifest.splits {
        let path = tier.split_file(f.ns, f.cid, split.ulid).expect("pinned");
        let local = std::fs::read(&path).expect("read the pinned file");
        assert_eq!(
            Bytes::from(local),
            object(&f, &split_path(f.ns, f.cid, split.ulid)).await,
            "{}",
            path.display()
        );
        assert!(!path.with_extension("split.tmp").exists());
    }
    assert_eq!(
        tier.counters().split_files_served,
        manifest.splits.len() as u64
    );
    // A second pass downloads nothing.
    let report = tier.reconcile_once().await.expect("reconcile");
    assert_eq!(report.pinned_splits, 0);
    tier.shutdown().await;
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_split_leaving_the_manifest_is_removed_after_the_linger() {
    let f = Fixture::start().await;
    f.commit(docs(0..20)).await;
    f.commit(docs(20..40)).await;
    f.pin(f.cid, TEXT).await;
    let linger = Duration::from_millis(100);
    let tier = tier_with(&f, config_with(&f, linger)).await;
    tier.reconcile_once().await.expect("reconcile");
    let before = f.manifest().await.splits;
    let old: Vec<_> = before
        .iter()
        .map(|split| tier.split_file(f.ns, f.cid, split.ulid).expect("pinned"))
        .collect();

    let merges = SplitMergeSource::new(
        f.ctx.clone(),
        MaintenanceConfig {
            merge_policy: StableLogMergePolicyConfig {
                min_level_num_docs: 100,
                merge_factor: 2,
                max_merge_factor: 4,
                maturation_period: Duration::from_hours(48),
            },
            split_num_docs_target: 10_000,
            poll_interval: Duration::ZERO,
            ..MaintenanceConfig::default()
        },
    );
    run_once(&f.meta.client, "merger", TTL, &merges)
        .await
        .expect("merge");
    let after = f.manifest().await.splits;
    assert!(
        before
            .iter()
            .all(|s| !after.iter().any(|a| a.ulid == s.ulid)),
        "the merge kept an input: {before:?} → {after:?}"
    );

    // Right after the merge the old splits are still served (a query at the
    // older manifest may ask for them) and the merged one is pinned.
    tier.reconcile_once().await.expect("reconcile");
    for split in &before {
        assert!(tier.split_file(f.ns, f.cid, split.ulid).is_some());
    }
    for split in &after {
        assert!(tier.split_file(f.ns, f.cid, split.ulid).is_some());
    }
    tokio::time::sleep(linger + Duration::from_millis(20)).await;
    tier.reconcile_once().await.expect("reconcile");
    for (split, path) in before.iter().zip(&old) {
        assert!(tier.split_file(f.ns, f.cid, split.ulid).is_none());
        let path = path.clone();
        eventually("the old split file is deleted", move || !path.exists()).await;
    }
    for split in &after {
        assert!(tier.split_file(f.ns, f.cid, split.ulid).is_some());
    }
    tier.shutdown().await;
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_download_is_retried_not_served() {
    let f = Fixture::start().await;
    f.commit(docs(0..30)).await;
    f.pin(f.cid, TEXT).await;
    let split = f.manifest().await.splits[0].clone();
    let path = split_path(f.ns, f.cid, split.ulid);

    // Directly: a GET failing mid-download leaves neither the file nor its
    // temporary.
    let dir = tempfile::TempDir::new().expect("dir");
    let to = dir.path().join("a").join("x.split");
    f.faulty.inject(Op::Get, Fault::Error);
    let err = download_split(
        &f.store,
        &path,
        split.size_bytes,
        split.footer_range.start,
        &to,
    )
    .await
    .expect_err("the injected GET fails");
    assert!(err.is_retryable(), "{err}");
    assert!(!to.exists());
    assert!(!Path::new(&format!("{}.tmp", to.display())).exists());

    // Through the tier: every GET of the pass fails, so nothing is served;
    // the next pass pins it.
    let tier = f.tier().await;
    for _ in 0..1_000 {
        f.faulty.inject(Op::Get, Fault::Error);
    }
    let report = tier.reconcile_once().await.expect("reconcile");
    assert!(!report.failures.is_empty());
    assert!(tier.split_file(f.ns, f.cid, split.ulid).is_none());
    f.faulty.clear();
    let report = tier.reconcile_once().await.expect("reconcile");
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    let local = tier.split_file(f.ns, f.cid, split.ulid).expect("pinned");
    assert_eq!(
        Bytes::from(std::fs::read(local).expect("read")),
        object(&f, &path).await
    );
    tier.shutdown().await;
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_truncated_object_is_rejected() {
    let f = Fixture::start().await;
    f.commit(docs(0..30)).await;
    f.pin(f.cid, TEXT).await;
    let split = f.manifest().await.splits[0].clone();
    let path = split_path(f.ns, f.cid, split.ulid);
    let original = object(&f, &path).await;
    let dir = tempfile::TempDir::new().expect("dir");
    let to = dir.path().join("x.split");
    let tmp = dir.path().join("x.split.tmp");

    // Truncated: the size does not match the manifest.
    f.store
        .put(&path, original.slice(..original.len() - 10))
        .await
        .expect("put");
    let err = download_split(
        &f.store,
        &path,
        split.size_bytes,
        split.footer_range.start,
        &to,
    )
    .await
    .expect_err("truncated");
    assert!(matches!(err, operon_hot::TierError::Corrupt(_)), "{err}");
    assert!(!to.exists() && !tmp.exists());

    // Same size, broken trailer.
    let mut broken = original.to_vec();
    let last = broken.len() - 1;
    broken[last] ^= 0xff;
    f.store.put(&path, Bytes::from(broken)).await.expect("put");
    let err = download_split(
        &f.store,
        &path,
        split.size_bytes,
        split.footer_range.start,
        &to,
    )
    .await
    .expect_err("no trailer");
    assert!(matches!(err, operon_hot::TierError::Corrupt(_)), "{err}");
    assert!(!to.exists() && !tmp.exists());

    // Through the tier: never served; restored, it is.
    let tier = f.tier().await;
    let report = tier.reconcile_once().await.expect("reconcile");
    assert_eq!(report.failures.len(), 1, "{:?}", report.failures);
    assert!(tier.split_file(f.ns, f.cid, split.ulid).is_none());
    f.store.put(&path, original).await.expect("restore");
    tier.reconcile_once().await.expect("reconcile");
    assert!(tier.split_file(f.ns, f.cid, split.ulid).is_some());
    tier.shutdown().await;
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_file_is_none_when_text_is_not_hot() {
    let f = Fixture::start().await;
    f.commit(docs(0..30)).await;
    let ulid = f.manifest().await.splits[0].ulid;
    let tier = f.tier().await;
    f.pin(
        f.cid,
        HotConfig {
            vectors: true,
            ..HotConfig::default()
        },
    )
    .await;
    tier.reconcile_once().await.expect("reconcile");
    assert!(tier.split_file(f.ns, f.cid, ulid).is_none(), "vectors only");

    f.pin(f.cid, TEXT).await;
    tier.reconcile_once().await.expect("reconcile");
    let path = tier.split_file(f.ns, f.cid, ulid).expect("text is hot");

    // Unpinned: not served from the next pass, while the file lingers for
    // a query that already holds its path.
    f.pin(f.cid, HotConfig::default()).await;
    tier.reconcile_once().await.expect("reconcile");
    assert!(tier.split_file(f.ns, f.cid, ulid).is_none(), "unpinned");
    assert!(path.exists(), "deleted before the linger");

    // Not owned, or disabled: never served.
    let elsewhere = f
        .tier_with(
            HotTierConfig {
                dir: f.data_dir.path().join("hot-elsewhere"),
                ..f.tier_config()
            },
            std::sync::Arc::new(crate::common::Elsewhere),
            std::sync::Arc::new(operon_hnsw::FlatEngine),
        )
        .await;
    f.pin(f.cid, TEXT).await;
    elsewhere.reconcile_once().await.expect("reconcile");
    assert!(elsewhere.split_file(f.ns, f.cid, ulid).is_none());
    let off = tier_with(
        &f,
        HotTierConfig {
            enabled: false,
            dir: f.data_dir.path().join("hot-off"),
            ..f.tier_config()
        },
    )
    .await;
    off.reconcile_once().await.expect("reconcile");
    assert!(off.split_file(f.ns, f.cid, ulid).is_none());
    tier.shutdown().await;
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fragments_are_prefetched_into_the_range_cache() {
    let f = Fixture::start_cached().await;
    f.commit(docs(0..200)).await;
    f.commit(docs(200..300)).await;
    // An upsert and a delete give fragments deletion files.
    f.commit(vec![DocOp::Delete(PrimaryKey::U64(7))]).await;
    f.commit(docs(10..20)).await;
    f.pin(f.cid, FRAGMENTS).await;
    let tier = f.tier().await;
    let report = tier.reconcile_once().await.expect("reconcile");
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    assert!(report.prefetched_bytes > 0);

    let snapshot = f.snapshot().await;
    let before = f.faulty.calls(Op::Get);
    let rows = snapshot.scan_all().await.expect("scan");
    assert_eq!(rows.len(), 299);
    assert_eq!(
        f.faulty.calls(Op::Get),
        before,
        "the scan read the object store after the prefetch"
    );
    // Nothing more to read at the same version.
    let report = tier.reconcile_once().await.expect("reconcile");
    assert_eq!(report.prefetched_bytes, 0);
    tier.shutdown().await;
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prefetch_stops_at_max_bytes_and_resumes() {
    let f = Fixture::start_cached().await;
    f.commit(docs(0..200)).await;
    f.commit(docs(200..400)).await;
    let snapshot = f.snapshot().await;
    let dataset = snapshot.dataset().expect("a dataset").clone();
    let prefix = lance_prefix(f.ns, f.cid);
    let total = prefetch_fragments(&f.ctx.cache, &f.store, &prefix, &dataset, u64::MAX)
        .await
        .expect("prefetch");
    assert!(total > 0);

    // Resuming, a third of the bytes at a time.
    let step = total / 3 + 1;
    let mut progress = FragmentProgress::default();
    let mut read = 0;
    for pass in 0..3 {
        let done = prefetch_fragments_resuming(
            &f.ctx.cache,
            &f.store,
            &prefix,
            &dataset,
            &mut progress,
            step,
        )
        .await
        .expect("prefetch");
        assert!(done.read <= step, "pass {pass} read {}", done.read);
        read += done.read;
        assert_eq!(done.prefetched, read);
        assert_eq!(done.total, total);
        assert_eq!(done.complete(), pass == 2, "pass {pass}: {done:?}");
    }
    assert_eq!(read, total);

    // Through the tier: `fragments_max_bytes` per pass.
    f.pin(f.cid, FRAGMENTS).await;
    let tier = tier_with(
        &f,
        HotTierConfig {
            fragments_max_bytes: step,
            ..f.tier_config()
        },
    )
    .await;
    let mut passes = 0;
    let mut sum = 0;
    loop {
        let report = tier.reconcile_once().await.expect("reconcile");
        assert!(report.prefetched_bytes <= step);
        if report.prefetched_bytes == 0 {
            break;
        }
        sum += report.prefetched_bytes;
        passes += 1;
        assert!(passes <= 3, "never finished");
    }
    assert_eq!((passes, sum), (3, total));
    tier.shutdown().await;
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prefetch_progress_forgets_files_the_version_no_longer_lists() {
    let f = Fixture::start_cached().await;
    f.commit(docs(0..200)).await;
    // Each delete replaces the fragment's deletion file.
    f.commit(vec![DocOp::Delete(PrimaryKey::U64(7))]).await;
    let old = f.snapshot().await.dataset().expect("a dataset").clone();
    f.commit(vec![DocOp::Delete(PrimaryKey::U64(8))]).await;
    let new = f.snapshot().await.dataset().expect("a dataset").clone();
    let prefix = lance_prefix(f.ns, f.cid);
    let mut progress = FragmentProgress::default();
    for dataset in [&old, &new] {
        let pass = prefetch_fragments_resuming(
            &f.ctx.cache,
            &f.store,
            &prefix,
            dataset,
            &mut progress,
            u64::MAX,
        )
        .await
        .expect("prefetch");
        assert!(pass.complete());
    }
    // The old version's deletion file was forgotten, so it is read again.
    let again = prefetch_fragments_resuming(
        &f.ctx.cache,
        &f.store,
        &prefix,
        &old,
        &mut progress,
        u64::MAX,
    )
    .await
    .expect("prefetch");
    assert!(again.read > 0, "{again:?}");
    assert!(again.read < again.total, "{again:?}");
    f.shutdown().await;
}
