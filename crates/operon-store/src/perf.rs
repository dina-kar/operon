//! Per-request object-store request counts (M1.6 Task 10, D92).
//!
//! A request installs a [`RequestCounts`] as a task-local scope with
//! [`scope`]; every [`Store`](crate::Store) read made on that task (not on
//! tasks it spawns) adds one to its kind. A scope made inside another
//! scope also adds to the enclosing one. Outside a scope nothing is
//! counted, and counting is a relaxed atomic add.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Object-store reads of one request, by kind.
#[derive(Debug, Default)]
pub struct RequestCounts {
    get: AtomicU64,
    head: AtomicU64,
    list: AtomicU64,
    parent: Option<Arc<RequestCounts>>,
}

/// A kind of counted read.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Read {
    Get,
    Head,
    List,
}

impl RequestCounts {
    /// New zero counts that also add to the current scope's, if any.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            parent: current(),
            ..Self::default()
        })
    }

    /// GETs, whole-object and ranged.
    pub fn get(&self) -> u64 {
        self.get.load(Ordering::Relaxed)
    }

    pub fn head(&self) -> u64 {
        self.head.load(Ordering::Relaxed)
    }

    pub fn list(&self) -> u64 {
        self.list.load(Ordering::Relaxed)
    }

    fn add(&self, read: Read) {
        let counter = match read {
            Read::Get => &self.get,
            Read::Head => &self.head,
            Read::List => &self.list,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        if let Some(parent) = &self.parent {
            parent.add(read);
        }
    }
}

tokio::task_local! {
    static COUNTS: Arc<RequestCounts>;
}

/// Runs `fut` with `counts` as the task's request counts.
pub async fn scope<F: Future>(counts: Arc<RequestCounts>, fut: F) -> F::Output {
    COUNTS.scope(counts, fut).await
}

/// The counts of the current scope, if one is installed on this task.
pub fn current() -> Option<Arc<RequestCounts>> {
    COUNTS.try_with(Arc::clone).ok()
}

/// Adds one `read` to the current scope, if any.
pub(crate) fn record(read: Read) {
    let _ = COUNTS.try_with(|counts| counts.add(read));
}
