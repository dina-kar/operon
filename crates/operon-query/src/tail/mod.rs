//! The tail index (H3; plan M1.2 Task 3): the writes of a collection's
//! implicit stream that its live manifest does not reflect yet, folded per
//! key over the durable state, in a RAM Tantivy index with the splits'
//! mapping.
//!
//! A tail entry for key *k* on partition *p* at offset *o* overrides the
//! durable state iff `o >= manifest.applied[p]` (Ruling 1); entries below
//! are *covered* and ignored. The durable rows the overlay supersedes form
//! the *shadow* set, kept exact across manifest commits through their PK
//! deltas. Tail row ids are `TAIL_ROWID_BASE + seq` (Ruling 7).
//!
//! - [`snapshot`]: [`TailSnapshot`], what a read sees;
//! - `index`: one generation (the entries and the RAM index) and the
//!   writer-side overlay state;
//! - `apply`: folding records, durable resolution, manifest adoption and
//!   [`build_range_tail`];
//! - `follower`: [`Tail`], one follower task per collection;
//! - [`registry`]: [`TailRegistry`], the tails of this node (Task 4);
//! - [`range`]: [`RangeTailCache`], the range tails of pinned reads (Task 4).
//!
//! Nothing here is persisted: dropping a tail changes only latency.

mod apply;
mod follower;
mod index;
pub mod range;
pub mod registry;
pub mod snapshot;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use operon_collection::{CollectionError, Document, PrimaryKey};
use operon_log::LogError;

pub use apply::build_range_tail;
pub use follower::Tail;
pub use range::RangeTailCache;
pub use registry::TailRegistry;
pub use snapshot::TailSnapshot;

/// The first tail row id; Lance stable row ids are below it (Ruling 7).
pub const TAIL_ROWID_BASE: u64 = 1 << 63;

/// How a tail follows and bounds itself.
#[derive(Clone, Debug, PartialEq)]
pub struct TailConfig {
    /// 256 MiB per collection.
    pub max_bytes: usize,
    /// 2 GiB per process (the [`TailBudget`]).
    pub total_max_bytes: usize,
    /// 10 min without a read: the tail stops.
    pub idle_ttl: Duration,
    /// 250 ms (the follower also wakes on metastore changes and `notify`).
    pub follow_interval: Duration,
    /// 4 MiB per fetch.
    pub fetch_bytes: usize,
    /// 0.5.
    pub compact_garbage_ratio: f64,
    /// 10 000.
    pub compact_min_entries: usize,
    /// 32 MiB Tantivy writer arena.
    pub writer_memory: usize,
    /// 1 000 keys per durable lookup.
    pub resolve_batch: usize,
}

impl Default for TailConfig {
    fn default() -> Self {
        Self {
            max_bytes: 256 << 20,
            total_max_bytes: 2 << 30,
            idle_ttl: Duration::from_secs(600),
            follow_interval: Duration::from_millis(250),
            fetch_bytes: 4 << 20,
            compact_garbage_ratio: 0.5,
            compact_min_entries: 10_000,
            writer_memory: 32 << 20,
            resolve_batch: 1_000,
        }
    }
}

/// One entry of the tail: the state of `pk` after the record at
/// (`partition`, `offset`).
#[derive(Clone, Debug)]
pub struct TailDoc {
    pub row_id: u64,
    pub pk: PrimaryKey,
    pub partition: u32,
    pub offset: u64,
    /// `None`: deleted.
    pub doc: Option<Arc<Document>>,
}

/// What a snapshot knows of a key.
#[derive(Clone, Debug)]
pub enum TailLookup {
    Present(Arc<TailDoc>),
    Deleted(Arc<TailDoc>),
    /// The durable state is current.
    Absent,
}

/// Whether a tail follows its stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TailState {
    Following,
    /// Over its memory bound: fetching stopped at `head` (§09 §7);
    /// adoption goes on.
    Overflow {
        head: BTreeMap<u32, u64>,
    },
    /// The tail could not restart; it retries.
    Failed(String),
}

#[derive(Debug, thiserror::Error)]
pub enum TailError {
    #[error("the tail is over its memory bound at {head:?}")]
    Overflow { head: BTreeMap<u32, u64> },
    #[error("partition {partition} is trimmed below {offset} (log start {log_start})")]
    Trimmed {
        partition: u32,
        offset: u64,
        log_start: u64,
    },
    #[error("the tail did not reach {target:?} in time")]
    Timeout { target: BTreeMap<u32, u64> },
    #[error("tail: {0}")]
    Log(#[from] LogError),
    #[error("tail: {0}")]
    Collection(#[from] CollectionError),
    #[error("the tail stopped")]
    Stopped,
}

impl From<tantivy::TantivyError> for TailError {
    fn from(err: tantivy::TantivyError) -> Self {
        TailError::Collection(CollectionError::Text(operon_text::TextError::Tantivy(err)))
    }
}

/// The memory every tail of a process holds together.
#[derive(Debug)]
pub struct TailBudget {
    total_max_bytes: usize,
    used: AtomicUsize,
}

impl TailBudget {
    pub fn new(total_max_bytes: usize) -> Self {
        Self {
            total_max_bytes,
            used: AtomicUsize::new(0),
        }
    }

    pub fn total_max_bytes(&self) -> usize {
        self.total_max_bytes
    }

    /// The bytes every tail holds now.
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    pub fn is_over(&self) -> bool {
        self.used() > self.total_max_bytes
    }

    /// Replaces a tail's contribution `old` with `new`.
    fn account(&self, old: usize, new: usize) {
        if new >= old {
            self.used.fetch_add(new - old, Ordering::Relaxed);
        } else {
            self.used.fetch_sub(old - new, Ordering::Relaxed);
        }
    }
}

impl Default for TailBudget {
    /// [`TailConfig::default`]'s `total_max_bytes` (2 GiB).
    fn default() -> Self {
        Self::new(TailConfig::default().total_max_bytes)
    }
}
