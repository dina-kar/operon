//! M1.3 Task 1 (Task 0 E52): a split merge changes no query result, scores
//! included: BM25 is computed over live documents and rescored canonically
//! (M1.2 Ruling 2, row 15.1), so dropping deleted docs and re-indexing into
//! fewer splits leaves every probe of the battery bit-identical.

use std::time::Duration;

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

use crate::common::{
    Fixture, GATE_NS, battery, battery_schema, gate_ids, random_history, run_probe,
};
use operon_collection::{MaintenanceConfig, SplitMergeSource, plan_merges};
use operon_common::meta::Consistency;
use operon_query::ReadConsistency;
use operon_quickwit::merge_policy::StableLogMergePolicyConfig;
use operon_worker::{RunResult, run_once};
use serde_json::Value;

const COLLECTION: &str = "merged";

/// Every probe of the battery against the live version.
async fn probes(f: &Fixture) -> Vec<(&'static str, Value)> {
    let service = f.service();
    let mut out = Vec::new();
    for (name, probe) in battery("tail") {
        let result = run_probe(&service, COLLECTION, &probe, &ReadConsistency::Strong).await;
        assert!(result.get("error").is_none(), "{name}: {result}");
        out.push((name, result));
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_merge_changes_no_search_result() {
    let mut rng = ChaCha8Rng::seed_from_u64(1_303);
    let history = random_history(&mut rng, 180);
    let f = Fixture::start().await;
    let service = f.service();
    service
        .create_collection(GATE_NS, COLLECTION, battery_schema(), Some(3))
        .await
        .expect("create the collection");
    for chunk in history.chunks(15) {
        crate::common::write_ops(&service, COLLECTION, chunk.to_vec()).await;
        f.settle().await;
    }
    let (ns, cid) = gate_ids(&f, COLLECTION).await;
    let ctx = f.storage.ctx.clone();
    let live = || async {
        operon_collection::live_manifest(
            &*ctx.meta,
            &ctx.store,
            &ctx.manifests,
            ns,
            cid,
            Consistency::Linearizable,
        )
        .await
        .expect("live manifest")
        .expect("committed")
        .1
    };
    let deleted = |m: &operon_collection::CollectionManifest| {
        m.splits.iter().map(|s| s.deleted_count).sum::<u64>()
    };
    let before_manifest = live().await;
    let before_splits = before_manifest.splits.len();
    assert!(before_splits >= 3, "{before_splits} splits");
    assert!(deleted(&before_manifest) > 0, "no deleted docs to drop");
    let before = probes(&f).await;

    let config = MaintenanceConfig {
        merge_policy: StableLogMergePolicyConfig {
            min_level_num_docs: 10,
            merge_factor: 2,
            max_merge_factor: 4,
            maturation_period: Duration::from_hours(48),
        },
        split_num_docs_target: 10_000,
        purge_min_deleted: 1,
        poll_interval: Duration::ZERO,
        ..MaintenanceConfig::default()
    };
    let source = SplitMergeSource::new(ctx.clone(), config.clone());
    for _ in 0..50 {
        if plan_merges(&*live().await, &config, ctx.meta.now_ms()).is_empty() {
            break;
        }
        let results = run_once(
            f.meta.client.clone(),
            "merger",
            Duration::from_secs(30),
            &source,
        )
        .await
        .expect("run");
        for (key, result) in results {
            assert!(matches!(result, RunResult::Ran(Ok(_))), "{key}: {result:?}");
        }
    }
    let after_manifest = live().await;
    assert!(after_manifest.splits.len() < before_splits);
    assert!(deleted(&after_manifest) < deleted(&before_manifest));

    let after = probes(&f).await;
    assert_eq!(before.len(), after.len());
    for ((name, a), (_, b)) in before.iter().zip(&after) {
        assert_eq!(a, b, "probe {name} changed across the merge");
    }
    f.shutdown().await;
}
