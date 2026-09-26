//! Task 15 item 5: one history in the four placements of item 2, each read
//! with a hot tier on and off, gives identical exact results (R10, R12).

use std::collections::BTreeMap;

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

use crate::common::{
    FakeHot, Fixture, PLACEMENTS, battery, build_placements, mixed_key, random_document,
    random_history, run_probe, with_hot, without_positions,
};
use operon_collection::{DocOp, Document};
use operon_query::ReadConsistency;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn results_are_identical_across_placements_and_hot_modes() {
    let mut rng = ChaCha8Rng::seed_from_u64(55);
    let mut history = random_history(&mut rng, 100);
    // Six keys of all three types hold one document, so every score ties
    // and only the key order decides.
    let twin = random_document(&mut rng, mixed_key(0));
    for i in 0..6 {
        history.push(DocOp::Upsert(Document {
            pk: mixed_key(i),
            ..twin.clone()
        }));
    }
    let f = Fixture::start().await;
    build_placements(&f, &history, &[30, 70], 60).await;
    let service = f.service();
    let hot = FakeHot::new();
    for name in PLACEMENTS {
        hot.pin_splits(&f, name).await;
    }
    service.set_hot_tier(hot.clone());

    let mut tied = false;
    for (name, probe) in battery("tail") {
        assert!(!probe.is_approximate(), "{name} is exact");
        let mut results = Vec::new();
        for placement in PLACEMENTS {
            for enabled in [false, true] {
                let result = with_hot(
                    enabled,
                    run_probe(&service, placement, &probe, &ReadConsistency::Strong),
                )
                .await;
                results.push(((placement, enabled), result));
            }
        }
        let (_, reference) = &results[0];
        assert!(reference.get("error").is_none(), "{name}: {reference}");
        for ((placement, enabled), result) in &results[1..] {
            let (a, b) = if *placement == "rebuild" {
                (
                    without_positions(reference.clone()),
                    without_positions(result.clone()),
                )
            } else {
                (reference.clone(), result.clone())
            };
            assert_eq!(a, b, "{name}: {placement} with the hot tier {enabled}");
        }
        // Equal scores somewhere in the hits.
        if let Some(hits) = reference["hits"].as_array() {
            let mut scores: BTreeMap<String, usize> = BTreeMap::new();
            for hit in hits {
                *scores.entry(hit["score"].to_string()).or_default() += 1;
            }
            tied |= scores.values().any(|n| *n >= 6);
        }
    }
    assert!(tied, "the twins tie in some probe");
    assert!(
        hot.split_calls.load(std::sync::atomic::Ordering::SeqCst) > 0,
        "the hot splits were read"
    );
    f.shutdown().await;
}
