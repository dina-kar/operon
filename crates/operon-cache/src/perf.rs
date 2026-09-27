//! Per-request range-cache byte counts (M1.6 Task 10, D92).
//!
//! A request installs a [`ByteCounts`] as a task-local scope with
//! [`scope`]; every [`RangeCache`](crate::RangeCache) read made on that task
//! (not on tasks it spawns) adds the bytes of the whole blocks it served
//! from the cache and of those it fetched from the store. A scope made
//! inside another scope also adds to the enclosing one. Outside a scope
//! nothing is counted, and counting is a relaxed atomic add.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Range-cache bytes of one request.
#[derive(Debug, Default)]
pub struct ByteCounts {
    hit: AtomicU64,
    miss: AtomicU64,
    parent: Option<Arc<ByteCounts>>,
}

impl ByteCounts {
    /// New zero counts that also add to the current scope's, if any.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            parent: current(),
            ..Self::default()
        })
    }

    /// Bytes of blocks served from the cache.
    pub fn hit_bytes(&self) -> u64 {
        self.hit.load(Ordering::Relaxed)
    }

    /// Bytes of blocks fetched from the store.
    pub fn miss_bytes(&self) -> u64 {
        self.miss.load(Ordering::Relaxed)
    }

    fn add(&self, hit: u64, miss: u64) {
        self.hit.fetch_add(hit, Ordering::Relaxed);
        self.miss.fetch_add(miss, Ordering::Relaxed);
        if let Some(parent) = &self.parent {
            parent.add(hit, miss);
        }
    }
}

tokio::task_local! {
    static COUNTS: Arc<ByteCounts>;
}

/// Runs `fut` with `counts` as the task's byte counts.
pub async fn scope<F: Future>(counts: Arc<ByteCounts>, fut: F) -> F::Output {
    COUNTS.scope(counts, fut).await
}

/// The counts of the current scope, if one is installed on this task.
pub fn current() -> Option<Arc<ByteCounts>> {
    COUNTS.try_with(Arc::clone).ok()
}

/// Adds `hit` and `miss` bytes to the current scope, if any.
pub(crate) fn record(hit: u64, miss: u64) {
    let _ = COUNTS.try_with(|counts| counts.add(hit, miss));
}
