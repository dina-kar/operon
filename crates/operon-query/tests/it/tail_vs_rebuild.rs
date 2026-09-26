//! Task 15 item 2: every probe of the battery gives identical results on a
//! history held in the tail, applied in commits, split between the two, and
//! rebuilt from its folded documents (R10, R11, Review Focus 1–3).

use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde_json::Value;

use crate::common::{
    Fixture, PLACEMENTS, battery, build_placements, random_history, run_probe, without_positions,
};
use operon_query::ReadConsistency;

/// Cases per run: `TAIL_PROP_CASES`, 24 by default (the nightly CI job runs
/// 256).
fn cases() -> u32 {
    std::env::var("TAIL_PROP_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok())
        .unwrap_or(24)
}

/// One case: the history of `seed`, its four placements, every probe; the
/// results of the tail placement.
async fn check(seed: u64) -> Result<Vec<(&'static str, Value)>, TestCaseError> {
    check_len(seed, None).await
}

/// [`check`], with `len` ops instead of 20–120.
async fn check_len(
    seed: u64,
    len: Option<usize>,
) -> Result<Vec<(&'static str, Value)>, TestCaseError> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let drawn = rng.random_range(20..=120);
    let len = len.unwrap_or(drawn);
    let history = random_history(&mut rng, len);
    let mut cuts: Vec<usize> = (0..rng.random_range(0..=3))
        .map(|_| rng.random_range(1..len))
        .collect();
    cuts.sort_unstable();
    let split_at = rng.random_range(0..=len);

    let f = Fixture::start().await;
    build_placements(&f, &history, &cuts, split_at).await;
    let service = f.service();
    let mut failure = None;
    let mut reference_results = Vec::new();
    'probes: for (name, probe) in battery("tail") {
        let mut results = Vec::new();
        for placement in PLACEMENTS {
            results.push(run_probe(&service, placement, &probe, &ReadConsistency::Strong).await);
        }
        let reference = &results[0];
        if reference.get("error").is_some() {
            failure = Some(format!("probe {name} failed: {reference}"));
            break;
        }
        for (placement, result) in PLACEMENTS.iter().zip(&results).skip(1) {
            let equal = if *placement == "rebuild" {
                without_positions(reference.clone()) == without_positions(result.clone())
            } else {
                reference == result
            };
            if !equal {
                failure = Some(format!(
                    "probe {name}: {placement} differs from tail\n  tail: {}\n  {placement}: {}",
                    pretty(reference),
                    pretty(result)
                ));
                break 'probes;
            }
        }
        reference_results.push((name, results.swap_remove(0)));
    }
    f.shutdown().await;
    match failure {
        None => Ok(reference_results),
        Some(diff) => Err(TestCaseError::fail(format!(
            "seed {seed}: {len} ops, cuts {cuts:?}, split at {split_at}\nhistory: {history:#?}\n{diff}"
        ))),
    }
}

fn pretty(value: &Value) -> String {
    serde_json::to_string_pretty(value).expect("JSON")
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: cases(),
        // Each case builds four collections; a shrink of the seed would
        // only draw another history.
        max_shrink_iters: 0,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn tail_merge_equals_full_rebuild(seed in any::<u64>()) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("a runtime");
        runtime.block_on(check(seed))?;
    }
}

/// Seeds that once failed (the first: `terms` buckets tied on their count
/// came back in hash order, plan row 15.3), and `TAIL_PROP_SEED` when set.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tail_merge_regressions() {
    let mut seeds: Vec<u64> = vec![14_413_613_377_658_170_746];
    if let Some(seed) = std::env::var("TAIL_PROP_SEED")
        .ok()
        .and_then(|seed| seed.parse().ok())
    {
        seeds = vec![seed];
    }
    for seed in seeds {
        if let Err(err) = check(seed).await {
            panic!("{err}");
        }
    }
}

/// The battery is not vacuous: on a history of 120 ops every search probe but
/// the aggregations returns hits, and the gets, counts and scrolls find
/// documents.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_battery_finds_documents() {
    let results = match check_len(3, Some(120)).await {
        Ok(results) => results,
        Err(err) => panic!("{err}"),
    };
    for (name, result) in results {
        if name.starts_with("aggs_") {
            let buckets = &result["aggregations"]["x"];
            assert!(buckets != &Value::Null, "{name}: {result}");
        } else if name.starts_with("count_") {
            assert!(result.as_u64().is_some_and(|n| n > 0), "{name}: {result}");
        } else if name == "get" {
            let found = result.as_array().expect("docs").iter();
            assert!(found.filter(|doc| !doc.is_null()).count() > 5, "{result}");
        } else if name.starts_with("scroll_") {
            assert!(
                result.as_array().is_some_and(|pages| !pages.is_empty()),
                "{name}"
            );
        } else {
            let key = if name == "group_by" { "groups" } else { "hits" };
            let hits = result[key].as_array().expect("hits");
            assert!(!hits.is_empty(), "{name} found nothing: {result}");
        }
    }
}
