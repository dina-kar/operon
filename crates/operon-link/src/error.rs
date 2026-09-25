use operon_common::meta::MetaError;
use operon_log::LogError;
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
    /// The commit cannot proceed now, for example because it took longer
    /// than `max_commit_delay` and the metastore refused it as stale. Retry
    /// later.
    #[error("blocked: {0}")]
    Blocked(String),
    /// A target of another crate failed (the collection target, M1.1).
    /// `retryable`: whether retrying the whole commit, after reloading the
    /// target, may succeed.
    #[error("target: {source}")]
    Target {
        retryable: bool,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}
