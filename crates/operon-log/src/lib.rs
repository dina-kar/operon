//! Operon's internal log on the `standard` WAL class (design §02 §3, §5, §6).
//!
//! - [`batch`]: Kafka `RecordBatch` v2 encoding of [`Record`]s.
//! - [`wal`] and [`segment`]: the object formats.
//! - [`LogWriter`]: the leaderless write path. Appends are buffered, written as
//!   one multi-partition WAL object per flush, committed to the metastore's
//!   sequencer, and only then acknowledged with their offsets.
//! - [`LogReader`]: fetch by offset through the range cache, with long-poll.
//! - [`Segmenter`] and [`Retention`]: background loops that rewrite WAL chunks
//!   into per-partition segments and trim old records.

pub mod batch;
mod error;
pub mod paths;
mod reader;
mod record;
pub mod segment;
pub mod wal;
mod writer;

pub use error::LogError;
pub use reader::{FetchRequest, FetchResponse, LogReader};
pub use record::{Encoding, OffsetRecord, Record};
pub use writer::{AppendAck, LogConfig, LogWriter};
