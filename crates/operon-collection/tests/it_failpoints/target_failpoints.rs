//! The named failpoints of the collection commit path (plan M1.1 Task 10
//! rule 4). Only with the `failpoints` feature. In a test binary of its
//! own: failpoints are process-global, so arming one would crash every
//! other test running alongside.
#![cfg(feature = "failpoints")]

use std::time::Duration;

use crate::common::{TargetFixture, field, schema, upsert};
use operon_collection::{DynamicMapping, FieldKind};
use operon_worker::run_once;
use serde_json::json;

/// The lease TTL of the run that hits the failpoint.
const CRASHED_TTL: Duration = Duration::from_millis(300);

/// With the `failpoints` feature, each named failpoint fires at its step (a
/// panic stands in for the crash gate's abort), and a fresh run recovers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_failpoint_fires_at_its_step() {
    let scenario = fail::FailScenario::setup();
    for (name, landed) in [
        ("collection.after_lance_commit", false),
        ("collection.after_split_put", false),
        ("collection.after_manifest_put", false),
        ("collection.after_cas", true),
        ("collection.after_pk_write", true),
    ] {
        let f = TargetFixture::start(
            schema(
                vec![field("tag", FieldKind::Keyword), field("n", FieldKind::I64)],
                DynamicMapping::Ignore,
            ),
            3,
        )
        .await;
        f.write((0..6).map(|n| upsert(n, json!({ "n": n }))).collect())
            .await;
        fail::cfg(name, "panic").expect("arm the failpoint");
        let source = f.source(f.factory());
        let meta = f.meta.client.clone();
        let run =
            tokio::spawn(async move { run_once(&meta, "crashed", CRASHED_TTL, &source).await });
        let err = run.await.expect_err("the run panics at the failpoint");
        assert!(err.is_panic(), "{name}: {err}");
        fail::remove(name);
        let version = f.manifest().await.version;
        assert_eq!(version, u64::from(landed), "{name}");
        f.apply_all(&f.source(f.factory()), "w2").await;
        let problems = f.verify().await;
        assert!(problems.is_empty(), "{name}: {problems:#?}");
        f.shutdown().await;
    }
    scenario.teardown();
}
