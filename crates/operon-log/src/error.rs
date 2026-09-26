use std::sync::Arc;

use operon_cache::CacheError;
use operon_common::StreamId;
use operon_common::meta::MetaError;
use operon_store::StoreError;

/// Errors returned by the log.
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    /// Stored bytes failed a check (magic, version, bounds or checksum). A
    /// reader never returns data from an object that fails a check.
    #[error("corrupt data: {0}")]
    Corrupt(String),
    /// The data uses a record encoding this build cannot read (for example
    /// `arrow`, reserved for a later milestone).
    #[error("unsupported record encoding {0}")]
    UnsupportedEncoding(u8),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("unknown stream {0}")]
    UnknownStream(StreamId),
    #[error("unknown partition: stream {stream} partition {partition}")]
    UnknownPartition { stream: StreamId, partition: u32 },
    /// The requested offset is below the log start or above the high watermark.
    #[error(
        "offset {requested} out of range: log start {log_start_offset}, high watermark {high_watermark}"
    )]
    OffsetOutOfRange {
        requested: u64,
        log_start_offset: u64,
        high_watermark: u64,
    },
    /// The writer holds too many unflushed bytes; retry later.
    #[error("too many buffered bytes; retry later")]
    Backpressure,
    /// The writer was shut down.
    #[error("the log writer is closed")]
    Closed,
    /// The records were written to a WAL object, but the writer could not
    /// learn whether the metastore committed them. They may be readable (once)
    /// or not at all; retrying the append may duplicate them.
    #[error("commit outcome unknown: {0}")]
    CommitUnknown(String),
    /// An object store operation failed. Shared, because one failed WAL PUT
    /// fails every append in it.
    #[error("object store: {0}")]
    Store(Arc<StoreError>),
    #[error("cache: {0}")]
    Cache(#[from] CacheError),
    #[error("metastore: {0}")]
    Meta(#[from] MetaError),
    /// A worker task failed (see [`operon_worker::TaskError`]).
    #[error("task: {0}")]
    Task(operon_worker::TaskError),
}

impl From<StoreError> for LogError {
    fn from(err: StoreError) -> Self {
        LogError::Store(Arc::new(err))
    }
}

pub(crate) fn corrupt(what: impl Into<String>) -> LogError {
    LogError::Corrupt(what.into())
}
