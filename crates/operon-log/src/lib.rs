//! Operon's internal log on the `standard` WAL class (design §02 §3, §5, §6).
//!
//! - [`batch`]: Kafka `RecordBatch` v2 encoding of [`Record`]s.
//! - [`wal`] and [`segment`]: the object formats.
//! - [`LogWriter`]: the leaderless write path. Appends are buffered, written as
//!   one multi-partition WAL object per flush, committed to the metastore's
//!   sequencer, and only then acknowledged with their offsets.
//! - [`LogReader`]: fetch by offset through the range cache, with long-poll.
//! - [`SegmenterSource`] and [`RetentionSource`]: worker task sources
//!   (`operon-worker`) that rewrite WAL chunks into per-partition segments and
//!   trim old records; [`Segmenter`] and [`Retention`] run them once.
//! - [`gc`]: garbage collection of retired and orphaned objects, also a
//!   worker task.

pub mod batch;
mod error;
pub mod gc;
pub mod paths;
mod reader;
mod record;
mod retention;
pub mod segment;
mod segmenter;
pub mod wal;
mod writer;

pub use error::LogError;
pub use reader::{FetchRequest, FetchResponse, LogReader};
pub use record::{Encoding, OffsetRecord, Record};
pub use retention::{RETENTION_TASK, Retention, RetentionConfig, RetentionReport, RetentionSource};
pub use segmenter::{Segmenter, SegmenterConfig, SegmenterReport, SegmenterSource};
pub use writer::{AppendAck, LogConfig, LogWriter};

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
