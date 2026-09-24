use operon_log::LogError;
use operon_meta::MetaError;
use operon_store::StoreError;

/// Errors of the link framework and its targets.
#[derive(Debug, thiserror::Error)]
pub enum LinkError {
    #[error("metastore: {0}")]
    Meta(#[from] MetaError),
    #[error("object store: {0}")]
    Store(#[from] StoreError),
    #[error("log: {0}")]
    Log(#[from] LogError),
    /// Stored target data failed a check (magic, version, checksum or shape).
    #[error("corrupt target data: {0}")]
    Corrupt(String),
    #[error("not found: {0}")]
    NotFound(String),
    /// The commit cannot proceed yet, for example because an orphaned
    /// manifest occupies the next version until garbage collection removes
    /// it. Retry later.
    #[error("blocked: {0}")]
    Blocked(String),
}
