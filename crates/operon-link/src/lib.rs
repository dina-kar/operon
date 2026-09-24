//! Operon's link framework (design §09 §1, §3).
//!
//! A link continuously applies a stream to a target. Exactly-once comes from
//! the target: each commit carries the batch's data *and* the offsets it
//! applied, atomically, under optimistic concurrency (the target's version)
//! and the task lease's fence. A crash anywhere re-reads the committed
//! offsets and re-applies only what was not committed; a zombie task (stale
//! epoch) fails its commit.
//!
//! - [`LinkTarget`]: the target contract ([`LinkTarget::load`],
//!   [`LinkTarget::commit`]).
//! - [`LinkApplySource`]: the worker task source, one task per link (plan
//!   ruling 3), at `Priority::LinkApply`.
//! - [`CounterTable`]: the M0 test target (plan ruling 4): record keys name
//!   counters, values are decimal `i64` deltas, and the table holds sums, so
//!   a double apply is visible.

mod apply;
mod counter;
mod error;
mod gc;
mod target;

pub use apply::{LinkApplySource, LinkConfig};
pub use counter::{COUNTER_KIND, CounterSnapshot, CounterTable, MAX_COMMIT_DELAY};
pub use error::LinkError;
pub use gc::LinkGcRoots;
pub use target::{ApplyBatch, CommitError, CommitStep, LinkTarget, TargetState};

#[cfg(feature = "test-util")]
pub use target::CommitHook;

/// Evaluates a named failpoint (M0.4 Task 5). With the `failpoints` feature
/// the `fail` crate may act on it (the crash gate aborts the process there);
/// without it, this expands to nothing.
macro_rules! failpoint {
    ($name:literal) => {
        #[cfg(feature = "failpoints")]
        fail::fail_point!($name);
    };
}
pub(crate) use failpoint;
