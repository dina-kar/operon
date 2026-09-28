//! [`LiveError`], the error of every Loam Live operation, and its wire code.

use operon_tikv::TxnError;

use crate::pb;

/// Why a Loam Live operation failed. Each variant maps to one
/// [`pb::ErrorCode`], which is what clients see (design §20 §7.1).
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum LiveError {
    /// The request is malformed: a bad value, name, id or range.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// A document, table or index does not exist.
    #[error("not found: {0}")]
    NotFound(String),
    /// The request is valid but the state refuses it (for example an index
    /// change on a non-empty table, R1 plan Ruling 5).
    #[error("failed precondition: {0}")]
    FailedPrecondition(String),
    /// A limit of [`Limits`](crate::Limits) was exceeded. `limit` is the
    /// field name of the limit.
    #[error("limit {limit} exceeded: {message}")]
    LimitExceeded {
        limit: &'static str,
        message: String,
    },
    /// A journal consumer's position is below what the janitor has kept:
    /// the entries after `position` in `shard` are gone (`first` is the
    /// oldest one left, if any). The consumer must resynchronise (rerun
    /// everything) and start again from the current heads.
    #[error(
        "journal shard {shard} was trimmed past position {position} (oldest entry left: {first:?})"
    )]
    JournalTrimmed {
        shard: u16,
        position: u64,
        first: Option<u64>,
    },
    /// A stored record could not be decoded.
    #[error("corrupt record: {0}")]
    Corrupt(String),
    /// A failure of the server itself (the OS random source, a bug).
    #[error("internal error: {0}")]
    Internal(String),
    /// A TiKV transaction or read failed. Inside [`Tikv::run`] bodies this
    /// carries the runner's retry signals ([`TxnError::Conflict`],
    /// [`TxnError::NotApplied`]); [`LiveError::into_txn`] hands them back.
    ///
    /// [`Tikv::run`]: operon_tikv::Tikv::run
    #[error("storage: {0}")]
    Txn(#[from] TxnError),
}

impl LiveError {
    /// The wire code of this error.
    pub fn code(&self) -> pb::ErrorCode {
        match self {
            LiveError::InvalidArgument(_) => pb::ErrorCode::ERROR_CODE_INVALID_ARGUMENT,
            LiveError::NotFound(_) => pb::ErrorCode::ERROR_CODE_NOT_FOUND,
            LiveError::FailedPrecondition(_) | LiveError::JournalTrimmed { .. } => {
                pb::ErrorCode::ERROR_CODE_FAILED_PRECONDITION
            }
            LiveError::LimitExceeded { .. } => pb::ErrorCode::ERROR_CODE_RESOURCE_EXHAUSTED,
            LiveError::Corrupt(_) | LiveError::Internal(_) => pb::ErrorCode::ERROR_CODE_INTERNAL,
            LiveError::Txn(e) => match e {
                TxnError::Conflict
                | TxnError::NotApplied(_)
                | TxnError::Undetermined { .. }
                | TxnError::Deadline => pb::ErrorCode::ERROR_CODE_UNAVAILABLE,
                TxnError::AlreadyExists(_) | TxnError::Fatal(_) => {
                    pb::ErrorCode::ERROR_CODE_INTERNAL
                }
            },
        }
    }

    /// The wire form of this error.
    pub fn to_proto(&self) -> pb::LiveError {
        pb::LiveError {
            code: self.code().into(),
            message: self.to_string(),
            ..Default::default()
        }
    }

    /// Splits a storage error off: `Err(txn_error)` for [`LiveError::Txn`],
    /// so a [`Tikv::run`](operon_tikv::Tikv::run) body can return it and let
    /// the runner retry, and `Ok(self)` for every other error, which the body
    /// returns inside its value so the runner does not retry it.
    pub fn into_txn(self) -> Result<LiveError, TxnError> {
        match self {
            LiveError::Txn(e) => Err(e),
            other => Ok(other),
        }
    }

    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        LiveError::InvalidArgument(message.into())
    }

    pub(crate) fn limit(limit: &'static str, message: impl Into<String>) -> Self {
        LiveError::LimitExceeded {
            limit,
            message: message.into(),
        }
    }
}
