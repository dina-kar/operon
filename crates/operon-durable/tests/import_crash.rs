//! Exactly-once imports under crashes (D1 Task 8, Review Focus 1): a fault
//! hook stops a step after its effect and before its settle, the runtime is
//! stopped (a crash) and started again, and the import converges: no
//! duplicate and no missing document, and finished work is not read again.

mod import_harness;

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use import_harness::{Harness, SOURCE, config, parquet_bytes, rows, until};
use operon_durable::OperationId;
use operon_durable::import::{Step, StepHook, derived_id};
use serde_json::json;

/// 3 files × 3 row groups of 2 rows, without `_id` (Ruling 7's ids).
async fn three_by_three(h: &Harness) {
    for file in 0..3 {
        h.put(
            &format!("{file}.parquet"),
            parquet_bytes(&rows("", file * 6, 6, false), 2),
        )
        .await;
    }
}

fn expected_ids(id: &OperationId) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    for file in 0..3 {
        for slice in 0..3 {
            for row in 0..2 {
                ids.insert(derived_id(id, file, slice, row));
            }
        }
    }
    ids
}

/// A hook that stops the `k`-th step (from 1) and says when it did.
fn crash_at(
    k: usize,
) -> (
    StepHook,
    Arc<AtomicBool>,
    Arc<std::sync::Mutex<Option<Step>>>,
) {
    let count = Arc::new(AtomicUsize::new(0));
    let hit = Arc::new(AtomicBool::new(false));
    let which = Arc::new(std::sync::Mutex::new(None));
    let (hit2, which2) = (hit.clone(), which.clone());
    let hook: StepHook = Arc::new(move |step| {
        if count.fetch_add(1, Ordering::SeqCst) + 1 == k {
            *which2.lock().expect("lock") = Some(step.clone());
            hit2.store(true, Ordering::SeqCst);
            return true;
        }
        false
    });
    (hook, hit, which)
}

/// A hook that never stops.
fn no_crash() -> StepHook {
    Arc::new(|_| false)
}

fn body() -> serde_json::Value {
    json!({ "source": SOURCE, "format": "parquet", "max_parallel_files": 1 })
}

// ─── import_survives_crash_at_every_step ────────────────────────────────────

/// 1 plan + 3 × (1 layout + 3 slices).
const STEPS: usize = 13;

#[tokio::test(flavor = "multi_thread")]
async fn import_survives_crash_at_every_step() {
    for k in 1..=STEPS {
        let (hook, hit, which) = crash_at(k);
        let mut h = Harness::start_with(config(), Some(hook)).await;
        three_by_three(&h).await;
        let (id, _) = h.submit(body(), None).await;
        until(&format!("step {k}"), || hit.load(Ordering::SeqCst)).await;
        let step = which.lock().expect("lock").clone().expect("the step");
        let reads_at_crash = h.counting.all_reads();
        h.crash().await;
        h.start_runtime(Some(no_crash())).await;
        let op = h.succeeded(&id).await;

        let ids: BTreeSet<String> = h.sink.ids().into_iter().collect();
        assert_eq!(
            ids,
            expected_ids(&id),
            "crash at step {k} ({step:?}): the documents"
        );
        let result = op.result.unwrap();
        assert_eq!(
            result["rows_written"], 18,
            "crash at {k} ({step:?}): {result}"
        );
        assert_eq!(result["files_done"], 3, "crash at {k} ({step:?}): {result}");
        // Only the stopped step re-ran: a written document was written again
        // only when the crash stopped its slice after the write.
        let rewritten: Vec<String> = h
            .sink
            .docs
            .lock()
            .expect("lock")
            .iter()
            .filter(|(_, d)| d.writes > 1)
            .map(|(id, _)| id.clone())
            .collect();
        match &step {
            Step::Slice { file, slice } => {
                let slice_ids: BTreeSet<String> = (0..2)
                    .map(|row| derived_id(&id, *file, *slice, row))
                    .collect();
                assert!(
                    rewritten.iter().all(|r| slice_ids.contains(r)),
                    "crash at {k} ({step:?}): only its slice is rewritten, got {rewritten:?}"
                );
                assert_eq!(rewritten.len(), 2, "crash at {k} ({step:?})");
            }
            _ => assert!(
                rewritten.is_empty(),
                "crash at {k} ({step:?}): {rewritten:?}"
            ),
        }
        // Files finished before the crash were not read again.
        let crashed_file = match step {
            Step::Plan => 0,
            Step::Layout { file } | Step::Slice { file, .. } => file,
        };
        for file in 0..crashed_file {
            let key = format!("{file}.parquet");
            assert_eq!(
                h.counting.reads(&key),
                reads_at_crash.get(&key).copied().unwrap_or(0),
                "crash at {k} ({step:?}): {key} was read again"
            );
        }
        h.stop().await;
    }
}

// ─── rerun_slice_converges ──────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn rerun_slice_converges() {
    // Crash right after file 1's slice 1 was written, before it settled.
    let hit = Arc::new(AtomicBool::new(false));
    let hit2 = hit.clone();
    let hook: StepHook = Arc::new(move |step| {
        if *step == (Step::Slice { file: 1, slice: 1 }) && !hit2.swap(true, Ordering::SeqCst) {
            return true;
        }
        false
    });
    let mut h = Harness::start_with(config(), Some(hook)).await;
    three_by_three(&h).await;
    let (id, _) = h.submit(body(), None).await;
    until("the slice", || hit.load(Ordering::SeqCst)).await;
    let before: HashMap<String, serde_json::Map<String, serde_json::Value>> = h
        .sink
        .docs
        .lock()
        .expect("lock")
        .iter()
        .map(|(id, d)| (id.clone(), d.row.clone()))
        .collect();
    h.crash().await;
    h.start_runtime(Some(no_crash())).await;
    h.succeeded(&id).await;
    let docs = h.sink.docs.lock().expect("lock").clone();
    assert_eq!(docs.len(), 18);
    for row in 0..2 {
        let id = derived_id(&id, 1, 1, row);
        let doc = &docs[&id];
        assert_eq!(doc.writes, 2, "the slice was written twice");
        assert_eq!(&doc.row, &before[&id], "to the same document");
    }
    h.stop().await;
}

// ─── finished_files_are_not_reread ──────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn finished_files_are_not_reread() {
    // Crash in file 2's first slice: files 0 and 1 are finished.
    let hit = Arc::new(AtomicBool::new(false));
    let hit2 = hit.clone();
    let hook: StepHook = Arc::new(move |step| {
        if *step == (Step::Slice { file: 2, slice: 0 }) && !hit2.swap(true, Ordering::SeqCst) {
            return true;
        }
        false
    });
    let mut h = Harness::start_with(config(), Some(hook)).await;
    three_by_three(&h).await;
    let (id, _) = h.submit(body(), None).await;
    until("file 2", || hit.load(Ordering::SeqCst)).await;
    let (r0, r1, r2) = (
        h.counting.reads("0.parquet"),
        h.counting.reads("1.parquet"),
        h.counting.reads("2.parquet"),
    );
    assert!(r0 > 0 && r1 > 0 && r2 > 0);
    h.crash().await;
    h.start_runtime(Some(no_crash())).await;
    h.succeeded(&id).await;
    assert_eq!(h.counting.reads("0.parquet"), r0, "file 0 was read again");
    assert_eq!(h.counting.reads("1.parquet"), r1, "file 1 was read again");
    assert!(h.counting.reads("2.parquet") > r2, "file 2's slice re-ran");
    assert_eq!(h.sink.len(), 18);
    h.stop().await;
}
