//! The seeded simulation seed sweep (M0.4 plan Task 6).
//!
//! - `SIM_SEEDS`: how many seeds (default 32 in release builds, as CI's `sim`
//!   job runs it, and 4 in debug builds, so `cargo test --workspace` stays
//!   quick; nightly 1000);
//! - `SIM_STEPS`: steps per seed (default 300);
//! - `SIM_SEED_BASE`: the first seed (default 0), or `SIM_SEED` for one seed;
//! - `SIM_THREADS`: seeds run at once, each on its own single-threaded
//!   runtime (default 4).
//!
//! Run it in release mode: `cargo test -p operon-sim --release`.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use operon_sim::{Event, SimConfig, SimReport, run};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[test]
fn every_seed_passes() {
    let (first, count) = match std::env::var("SIM_SEED").ok().and_then(|v| v.parse().ok()) {
        Some(seed) => (seed, 1),
        None => {
            let default = if cfg!(debug_assertions) { 4 } else { 32 };
            (env_u64("SIM_SEED_BASE", 0), env_u64("SIM_SEEDS", default))
        }
    };
    let steps = u32::try_from(env_u64("SIM_STEPS", 300)).expect("SIM_STEPS fits u32");
    let threads = env_u64("SIM_THREADS", 4).max(1);
    let next = AtomicU64::new(first);
    let failures: Mutex<Vec<SimReport>> = Mutex::new(Vec::new());
    let totals: Mutex<(u64, u64, u64)> = Mutex::new((0, 0, 0));
    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                loop {
                    let seed = next.fetch_add(1, Ordering::SeqCst);
                    if seed >= first + count {
                        return;
                    }
                    let report = run(SimConfig {
                        steps,
                        ..SimConfig::new(seed)
                    });
                    {
                        let mut totals = totals.lock().expect("lock");
                        totals.0 += 1;
                        totals.1 += report.stats.appends_acked;
                        totals.2 += report.stats.indeterminate;
                    }
                    eprintln!(
                        "seed {seed}: {} ({:?})",
                        if report.is_ok() { "ok" } else { "FAILED" },
                        report.stats
                    );
                    if !report.is_ok() {
                        failures.lock().expect("lock").push(report);
                    }
                }
            });
        }
    });
    let totals = *totals.lock().expect("lock");
    eprintln!(
        "{} seeds x {steps} steps: {} acknowledged appends, {} indeterminate operations",
        totals.0, totals.1, totals.2
    );
    let failures = failures.into_inner().expect("lock");
    for report in &failures {
        eprintln!("{}", report.describe());
    }
    assert!(
        failures.is_empty(),
        "{} of {count} seeds failed: {:?}",
        failures.len(),
        failures.iter().map(|r| r.seed).collect::<Vec<_>>()
    );
}

/// Plan M1.1 Task 13: the event schedule, collection writes included, is a
/// function of the seed alone, so a failing seed's schedule replays.
#[test]
fn a_seed_with_collection_writes_is_reproducible_in_schedule() {
    let config = SimConfig {
        steps: 80,
        ..SimConfig::new(env_u64("SIM_SEED", 7))
    };
    let first = run(config.clone());
    let second = run(config);
    assert!(first.is_ok(), "{}", first.describe());
    assert!(second.is_ok(), "{}", second.describe());
    assert!(
        first
            .schedule
            .iter()
            .any(|event| matches!(event, Event::DocWrite { .. })),
        "the schedule has no collection write: {:?}",
        first.schedule
    );
    assert_eq!(first.schedule, second.schedule);
}
