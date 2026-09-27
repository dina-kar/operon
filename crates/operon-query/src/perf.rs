//! The per-response `performance` block (M1.6 Task 10, D92).
//!
//! [`with_perf`] installs a [`PerfScope`] as a task-local scope around a
//! whole request, together with the range cache's byte counts
//! ([`operon_cache::perf`]) and the store's request counts
//! ([`operon_store::perf`]). Operon's own reads on the request's task are
//! counted exactly; reads on tasks the request spawns (Lance's among them)
//! do not inherit the scope, so the cache and store counts are lower
//! bounds. Counting is a few relaxed atomic adds per read, so the block is
//! always present.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use operon_cache::perf::ByteCounts;
use operon_store::perf::RequestCounts;
use serde::{Deserialize, Serialize};

/// What one search cost the server; field names are the JSON keys.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Performance {
    /// From the service call's entry to its return.
    pub server_total_ms: f64,
    /// Waiting for admission: 0 in M1 (the M2 concurrency limit, D98).
    pub queue_ms: f64,
    /// Resolution, the read snapshot (tail sync included) and compiling
    /// the request into its plan.
    pub planning_ms: f64,
    /// Running the plan.
    pub execution_ms: f64,
    /// The read snapshot's manifest version.
    pub manifest_version: u64,
    /// Unindexed (tail) documents visible to the snapshot: present and
    /// deleted overlay entries.
    pub tail_records: u64,
    /// `Eventual` only: records written but not yet in the tail, summed
    /// over partitions (D86).
    pub stale_records: Option<u64>,
    /// One entry per retriever, in request order.
    pub rows_scanned: Vec<RetrieverRows>,
    pub cache: CacheStats,
    pub object_store_requests: StoreRequests,
}

/// The rows one retriever scored.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RetrieverRows {
    /// `"text"`, `"vector"`, `"sparse"` or `"filter"`.
    pub kind: String,
    /// Text: documents rescored; vector: candidates scored with the exact
    /// kernel; sparse: candidates scored; filter: matches counted.
    pub candidates: u64,
    /// Of those, rows scored without an index (the tail, brute force).
    pub brute_force: u64,
}

/// Range-cache bytes of the request (a lower bound).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CacheStats {
    pub hit_bytes: u64,
    pub miss_bytes: u64,
    /// `hit / (hit + miss)`; `None` when no byte was read.
    pub hit_ratio: Option<f64>,
}

/// Object-store reads of the request (a lower bound).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StoreRequests {
    pub get: u64,
    pub head: u64,
    pub list: u64,
}

/// The SQL response's `performance`: the four timings only.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SqlPerformance {
    pub server_total_ms: f64,
    pub queue_ms: f64,
    pub planning_ms: f64,
    pub execution_ms: f64,
}

/// What one retriever counts as it scores.
#[derive(Debug, Default)]
pub struct RowCounter {
    candidates: AtomicU64,
    brute_force: AtomicU64,
}

impl RowCounter {
    /// Adds `candidates` scored rows, `brute_force` of them without an
    /// index.
    pub fn add(&self, candidates: u64, brute_force: u64) {
        self.candidates.fetch_add(candidates, Ordering::Relaxed);
        self.brute_force.fetch_add(brute_force, Ordering::Relaxed);
    }

    pub fn candidates(&self) -> u64 {
        self.candidates.load(Ordering::Relaxed)
    }

    pub fn brute_force(&self) -> u64 {
        self.brute_force.load(Ordering::Relaxed)
    }
}

/// A per-request collector, installed as a task-local scope around the
/// whole request (like the hot switch) by [`with_perf`].
#[derive(Debug)]
pub struct PerfScope {
    started: Instant,
    planned: OnceLock<Instant>,
    executed: OnceLock<Instant>,
    rows: Mutex<Vec<(&'static str, Arc<RowCounter>)>>,
    cache: Arc<ByteCounts>,
    store: Arc<RequestCounts>,
}

impl PerfScope {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            started: Instant::now(),
            planned: OnceLock::new(),
            executed: OnceLock::new(),
            rows: Mutex::new(Vec::new()),
            cache: ByteCounts::new(),
            store: RequestCounts::new(),
        })
    }

    /// Since the scope was installed.
    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    /// Marks the end of planning; the first mark wins.
    pub fn mark_planned(&self) {
        let _ = self.planned.set(Instant::now());
    }

    /// Marks the end of execution; the first mark wins.
    pub fn mark_executed(&self) {
        let _ = self.executed.set(Instant::now());
    }

    /// Planning's end, since the scope was installed; `None` before the
    /// mark.
    pub fn planned(&self) -> Option<Duration> {
        self.planned.get().map(|at| *at - self.started)
    }

    /// Execution's end, since the scope was installed.
    pub fn executed(&self) -> Option<Duration> {
        self.executed.get().map(|at| *at - self.started)
    }

    /// A new row counter for a retriever of `kind`, reported in the order
    /// of these calls.
    pub fn retriever(&self, kind: &'static str) -> Arc<RowCounter> {
        let counter = Arc::new(RowCounter::default());
        self.rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((kind, counter.clone()));
        counter
    }

    pub fn rows_scanned(&self) -> Vec<RetrieverRows> {
        self.rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(kind, counter)| RetrieverRows {
                kind: (*kind).to_string(),
                candidates: counter.candidates(),
                brute_force: counter.brute_force(),
            })
            .collect()
    }

    pub fn cache(&self) -> CacheStats {
        let (hit_bytes, miss_bytes) = (self.cache.hit_bytes(), self.cache.miss_bytes());
        let total = hit_bytes + miss_bytes;
        CacheStats {
            hit_bytes,
            miss_bytes,
            hit_ratio: (total > 0).then(|| hit_bytes as f64 / total as f64),
        }
    }

    pub fn store_requests(&self) -> StoreRequests {
        StoreRequests {
            get: self.store.get(),
            head: self.store.head(),
            list: self.store.list(),
        }
    }
}

tokio::task_local! {
    static PERF: Arc<PerfScope>;
}

/// Runs `fut` in a new [`PerfScope`] (with the cache's and the store's
/// counts); its output and the scope.
pub async fn with_perf<F: Future>(fut: F) -> (F::Output, Arc<PerfScope>) {
    let scope = PerfScope::new();
    let counted = operon_cache::perf::scope(
        scope.cache.clone(),
        operon_store::perf::scope(scope.store.clone(), fut),
    );
    let output = PERF.scope(scope.clone(), counted).await;
    (output, scope)
}

/// The current scope, if one is installed on this task.
pub fn current() -> Option<Arc<PerfScope>> {
    PERF.try_with(Arc::clone).ok()
}

/// A row counter for a retriever of `kind`: registered with the current
/// scope, or detached outside one.
pub fn retriever(kind: &'static str) -> Arc<RowCounter> {
    match current() {
        Some(scope) => scope.retriever(kind),
        None => Arc::new(RowCounter::default()),
    }
}

/// Marks the end of planning in the current scope, if any.
pub fn mark_planned() {
    if let Some(scope) = current() {
        scope.mark_planned();
    }
}

/// Marks the end of execution in the current scope, if any.
pub fn mark_executed() {
    if let Some(scope) = current() {
        scope.mark_executed();
    }
}

/// `duration` in milliseconds, with microsecond precision.
pub fn millis(duration: Duration) -> f64 {
    duration.as_micros() as f64 / 1000.0
}

impl Performance {
    /// Fills the timings, rows and counts of a request served locally in
    /// `scope`; a forwarded one keeps the owner's block and takes only this
    /// node's total.
    pub fn finish(&mut self, scope: &PerfScope) {
        let total = scope.elapsed();
        if let Some(planned) = scope.planned() {
            let executed = scope.executed().unwrap_or(total);
            self.queue_ms = 0.0;
            self.planning_ms = millis(planned);
            self.execution_ms = millis(executed.saturating_sub(planned));
            self.rows_scanned = scope.rows_scanned();
            self.cache = scope.cache();
            self.object_store_requests = scope.store_requests();
        }
        self.server_total_ms = millis(total).max(self.planning_ms + self.execution_ms);
    }
}

impl SqlPerformance {
    /// The timings of a statement planned by `planned` and run by
    /// `executed`, both since its start, which was `total` ago.
    pub fn of(planned: Duration, executed: Duration, total: Duration) -> Self {
        let planning_ms = millis(planned);
        let execution_ms = millis(executed.saturating_sub(planned));
        Self {
            server_total_ms: millis(total).max(planning_ms + execution_ms),
            queue_ms: 0.0,
            planning_ms,
            execution_ms,
        }
    }
}
