//! Task 15 item 4: pinned reads stay stable under writes, commits, tail
//! compaction and GC while their manifest is retained, and are
//! `NotFound { kind: "pin" }` once it is not (Ruling 14, Review Focus 4).

use std::sync::Arc;
use std::time::Duration;

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use serde_json::Value;

use crate::common::{
    Fixture, GATE_NS, WAIT, battery, battery_schema, random_history, run_probe, write_ops,
};
use operon_collection::{
    CollectionConfig, CollectionContext, CollectionGcRoots, CollectionTrimSource,
};
use operon_log::gc::{GcConfig, GcSource};
use operon_query::tail::TailConfig;
use operon_query::{CollectionService, ReadConsistency, ServiceConfig, ServiceError};

const DOCS: &str = "docs";

/// One GC pass with no grace over `ctx`'s collections (retention from its
/// config).
async fn gc(f: &Fixture, ctx: &CollectionContext) {
    let config = GcConfig {
        grace: Duration::ZERO,
        ..GcConfig::default()
    };
    let roots: Vec<Arc<dyn operon_log::gc::GcRoots>> =
        vec![Arc::new(CollectionGcRoots::new(ctx.clone()))];
    GcSource::with_roots(f.storage.store.clone(), config, roots)
        .run_once(f.meta.client.clone(), "gc")
        .await
        .expect("gc")
        .expect("the gc lease");
}

/// One trim pass over `ctx`'s collections.
async fn trim(f: &Fixture, ctx: &CollectionContext) {
    let source = CollectionTrimSource::new(ctx.clone());
    operon_worker::run_once(f.meta.client.clone(), "trimmer", WAIT, &source)
        .await
        .expect("trim");
}

/// Every probe of the battery at `consistency`.
async fn results(service: &CollectionService, consistency: &ReadConsistency) -> Vec<Value> {
    let mut out = Vec::new();
    for (_, probe) in battery(DOCS) {
        out.push(run_probe(service, DOCS, &probe, consistency).await);
    }
    out
}

/// Writes `ops` and applies them in `commits` commits, reading strongly in
/// between so the tail follows (and compacts).
async fn write_in_commits(f: &Fixture, ops: Vec<operon_collection::DocOp>, commits: usize) {
    let service = f.service();
    let per = ops.len().div_ceil(commits);
    for chunk in ops.chunks(per) {
        write_ops(&service, DOCS, chunk.to_vec()).await;
        service
            .count(GATE_NS, DOCS, None, ReadConsistency::Strong)
            .await
            .expect("count");
        f.settle().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pinned_reads_are_stable_under_writes_commits_and_gc() {
    let service_config = ServiceConfig {
        tail: TailConfig {
            compact_min_entries: 10,
            ..TailConfig::default()
        },
        ..ServiceConfig::default()
    };
    let retained = CollectionConfig {
        keep_manifests: 10,
        time_travel_retention: Duration::from_secs(3600),
        ..CollectionConfig::default()
    };
    let f = Fixture::start_configured(service_config.clone(), retained.clone()).await;
    let service = f.service();
    service
        .create_collection(GATE_NS, DOCS, battery_schema(), Some(3))
        .await
        .expect("create");
    let mut rng = ChaCha8Rng::seed_from_u64(44);
    write_ops(&service, DOCS, random_history(&mut rng, 80)).await;
    f.settle().await;
    // The pin covers writes the link has not applied.
    write_ops(&service, DOCS, random_history(&mut rng, 20)).await;

    // 1. Pin and record.
    let pin = service.pin(GATE_NS, DOCS).await.expect("pin");
    let pinned = pin.consistency();
    let recorded = results(&service, &pinned).await;
    for result in &recorded {
        assert!(result.get("error").is_none(), "{result}");
    }
    // The pin reads the whole acknowledged state, unapplied writes included.
    assert!(f.link_lag().await > 0, "writes wait in the tail");
    assert_eq!(recorded, results(&service, &ReadConsistency::Strong).await);

    // 2. 200 more ops in 6 commits, the tail compacting in between.
    write_in_commits(&f, random_history(&mut rng, 200), 6).await;
    let info = service.get_collection(GATE_NS, DOCS).await.expect("info");
    assert!(info.manifest_version >= pin.manifest_version + 6);

    // 3. GC keeps the retained manifests: the pinned reads are unchanged,
    //    also through cold caches, which read the objects GC kept.
    gc(&f, &f.storage.ctx).await;
    assert_eq!(results(&service, &pinned).await, recorded);
    let cold = f
        .cold_service(retained.clone(), service_config.clone())
        .await;
    assert_eq!(results(&cold, &pinned).await, recorded);
    cold.shutdown().await;
    assert_ne!(
        results(&service, &ReadConsistency::Strong).await,
        recorded,
        "the collection moved on"
    );

    // 4. With one kept manifest and no retention, three more commits, GC
    //    and trim, the pin is gone.
    let expiring_config = CollectionConfig {
        keep_manifests: 1,
        time_travel_retention: Duration::ZERO,
        ..CollectionConfig::default()
    };
    let short = CollectionContext {
        config: expiring_config.clone(),
        ..f.storage.ctx.clone()
    };
    let expiring = f.cold_service(expiring_config, service_config).await;
    write_in_commits(&f, random_history(&mut rng, 30), 3).await;
    gc(&f, &short).await;
    trim(&f, &short).await;
    for ((name, _), result) in battery(DOCS).iter().zip(results(&expiring, &pinned).await) {
        let error = result["error"].as_str().unwrap_or_default();
        assert!(
            error.starts_with("pin \"") && error.ends_with("not found"),
            "{name}: {result}"
        );
    }
    assert!(matches!(
        expiring.count(GATE_NS, DOCS, None, pinned.clone()).await,
        Err(ServiceError::NotFound { kind: "pin", .. })
    ));
    expiring.shutdown().await;
    f.shutdown().await;
}
