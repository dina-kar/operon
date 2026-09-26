//! The error every hot-tier operation returns.

use operon_cache::CacheError;
use operon_collection::CollectionError;
use operon_common::meta::MetaError;
use operon_hnsw::HnswError;
use operon_store::StoreError;

/// A hot-tier error.
#[derive(Debug, thiserror::Error)]
pub enum TierError {
    #[error("collection: {0}")]
    Collection(#[from] CollectionError),
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("range cache: {0}")]
    Cache(#[from] CacheError),
    #[error("metastore: {0}")]
    Meta(#[from] MetaError),
    #[error("hnsw: {0}")]
    Hnsw(#[from] HnswError),
    /// A hot artifact failed a check: its descriptor, covered set or a
    /// chunk is missing, malformed or does not match its checksum.
    #[error("corrupt hot artifact: {0}")]
    Corrupt(String),
    #[error("over budget: {0}")]
    OverBudget(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Other(String),
}

impl TierError {
    /// Whether retrying may succeed: store and collection errors by their
    /// own rule, a metastore that lost its leader, timed out or is
    /// unavailable, and local I/O. Everything else is not.
    pub fn is_retryable(&self) -> bool {
        match self {
            TierError::Collection(err) => err.is_retryable(),
            TierError::Store(err) => err.is_retryable(),
            TierError::Cache(CacheError::Store(err)) => err.is_retryable(),
            TierError::Cache(_) => false,
            TierError::Meta(err) => matches!(
                err,
                MetaError::NotLeader { .. } | MetaError::Timeout | MetaError::Unavailable(_)
            ),
            TierError::Io(_) => true,
            TierError::Hnsw(_)
            | TierError::Corrupt(_)
            | TierError::OverBudget(_)
            | TierError::Other(_) => false,
        }
    }
}
