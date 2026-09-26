use std::io;

use serde::{Deserialize, Serialize};

use crate::meta::types::{LinkId, Pointer};
use crate::{CollectionId, NamespaceId, StreamId};

/// Why a `Command` was rejected. A rejected command leaves the state unchanged.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum ApplyError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// Carries the existing id, so a retry after a lost acknowledgement can
    /// recover it.
    #[error("namespace already exists: {0}")]
    NamespaceExists(NamespaceId),
    #[error("namespace not found: {0}")]
    NamespaceNotFound(NamespaceId),
    /// Carries the existing id, so a retry after a lost acknowledgement can
    /// recover it.
    #[error("stream already exists: {0}")]
    StreamExists(StreamId),
    #[error("stream not found: {0}")]
    StreamNotFound(StreamId),
    /// Carries the existing id, so a retry after a lost acknowledgement can
    /// recover it.
    #[error("link already exists: {0}")]
    LinkExists(LinkId),
    #[error("partition not found: stream {stream} partition {partition}")]
    PartitionNotFound { stream: StreamId, partition: u32 },
    #[error("lease is held by {owner} until {deadline_ms}")]
    LeaseHeld { owner: String, deadline_ms: u64 },
    /// The caller no longer holds the lease at the epoch it named: it expired,
    /// was released, or was taken over.
    #[error("lease lost: {key}")]
    LeaseLost { key: String },
    /// Carries the current pointer, so a writer retrying after a lost
    /// acknowledgement can check whether the current value is its own.
    #[error("pointer version mismatch, current: {current:?}")]
    VersionMismatch { current: Option<Pointer> },
    #[error("fenced: lease {lease} is no longer at the given epoch")]
    Fenced { lease: String },
    /// A segment swap named index entries that are no longer there as given:
    /// a concurrent swap or trim changed the partition's index.
    #[error("index mismatch: stream {stream} partition {partition}")]
    IndexMismatch { stream: StreamId, partition: u32 },
    /// A WAL object is too old to commit (see `Command::CommitWal`), and no
    /// commit record for it remains. On a first attempt its records were never
    /// committed. On a retry after an attempt whose outcome was unknown, the
    /// first attempt may have committed them and its record may since have
    /// been pruned: the outcome is still unknown.
    #[error("stale WAL commit: {object}")]
    StaleCommit { object: String },
    /// A command would reference an object created too long ago
    /// ([`Freshness`](crate::meta::Freshness)): garbage collection may already
    /// have deleted it. Nothing changed; the object is left to garbage
    /// collection.
    #[error(
        "stale object {object}: created at {created_at_ms} ms, max age {max_age_ms} ms, metastore clock {clock_ms} ms"
    )]
    StaleObject {
        object: String,
        created_at_ms: u64,
        max_age_ms: u64,
        clock_ms: u64,
    },
    /// Carries the existing id, so a retry after a lost acknowledgement can
    /// recover it.
    #[error("collection already exists: {0}")]
    CollectionExists(CollectionId),
    #[error("collection not found: {0}")]
    CollectionNotFound(CollectionId),
    /// The name is held by a collection with another schema or partition
    /// count, or by an alias.
    #[error("name already taken: {0}")]
    NameTaken(String),
    #[error("incompatible schema update: {0}")]
    IncompatibleSchema(String),
    #[error("schema version mismatch: collection {collection} is at schema version {current}")]
    SchemaVersionMismatch {
        collection: CollectionId,
        current: u64,
    },
    /// An alias action named a collection that does not exist.
    #[error("unknown collection: {0}")]
    UnknownCollection(String),
}

/// How far the proposer's clock lagged the metastore's, for a StaleObject refusal.
///
/// A proposer checks an object's deadline against its own clock before it
/// proposes; the metastore checks it again against its clock when it
/// applies. A refusal the proposer did not predict means the command took
/// too long between the two checks, or the proposer's clock lags.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StaleLag {
    /// `created_at_ms + max_age_ms`: the last metastore time the object was
    /// fresh.
    pub deadline_ms: u64,
    /// The metastore clock when it refused the command.
    pub clock_ms: u64,
    /// The proposer's clock when it handled the refusal.
    pub proposer_now_ms: u64,
    /// `clock_ms - deadline_ms`: how late the command was applied.
    pub late_by_ms: u64,
    /// `clock_ms - proposer_now_ms`: how far the proposer's clock is behind
    /// the metastore's (negative when ahead).
    pub proposer_lag_ms: i64,
}

impl ApplyError {
    /// `Some` for `StaleObject`, else `None`.
    pub fn stale_lag(&self, proposer_now_ms: u64) -> Option<StaleLag> {
        let ApplyError::StaleObject {
            created_at_ms,
            max_age_ms,
            clock_ms,
            ..
        } = *self
        else {
            return None;
        };
        let deadline_ms = created_at_ms.saturating_add(max_age_ms);
        let lag = i128::from(clock_ms) - i128::from(proposer_now_ms);
        Some(StaleLag {
            deadline_ms,
            clock_ms,
            proposer_now_ms,
            late_by_ms: clock_ms.saturating_sub(deadline_ms),
            proposer_lag_ms: i64::try_from(lag).unwrap_or(if lag < 0 {
                i64::MIN
            } else {
                i64::MAX
            }),
        })
    }
}

/// Logs a StaleObject refusal at WARN with every StaleLag field and the object path; no-op otherwise.
pub fn log_stale_object(err: &ApplyError, proposer_now_ms: u64) {
    let (ApplyError::StaleObject { object, .. }, Some(lag)) = (err, err.stale_lag(proposer_now_ms))
    else {
        return;
    };
    tracing::warn!(
        %object,
        deadline_ms = lag.deadline_ms,
        clock_ms = lag.clock_ms,
        proposer_now_ms = lag.proposer_now_ms,
        late_by_ms = lag.late_by_ms,
        proposer_lag_ms = lag.proposer_lag_ms,
        "the metastore refused a stale object"
    );
}

/// Errors returned by `MetaNode`.
#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    /// The command was applied and rejected by the state machine; nothing changed.
    #[error("rejected: {0}")]
    Rejected(#[from] ApplyError),
    /// This node is not the leader, or stopped being the leader before the
    /// request finished. Writes and linearizable reads must go to `leader`, if
    /// one is known.
    ///
    /// For a write, the outcome is unknown: openraft also returns this for a
    /// write it had already proposed, which may have been committed (for
    /// example by the next leader) or may still be. Retrying is safe, because
    /// every command is retry-safe; the retry may then report the first
    /// attempt's effect, such as [`ApplyError::NamespaceExists`] or a
    /// [`ApplyError::VersionMismatch`] whose current pointer is the caller's.
    #[error("not the leader (leader: {leader:?})")]
    NotLeader { leader: Option<u64> },
    /// The request did not finish within the request timeout. A timed-out
    /// write may still be applied later; retry it (commands are retry-safe) or
    /// read to find out.
    #[error("request timed out; a write may still be applied")]
    Timeout,
    /// Raft has stopped or cannot make progress (for example, a leader could
    /// not reach a quorum). As with [`MetaError::NotLeader`], a write's outcome
    /// is unknown, and retrying it is safe.
    #[error("metastore unavailable: {0}")]
    Unavailable(String),
    /// The leader refused to propose a command stamped `stamped_ms`, more
    /// than `MetaConfig::max_clock_skew` ahead of its own clock `leader_ms`.
    /// Nothing was proposed; fix the proposer's clock. Not retried by
    /// `MetaClient`.
    #[error("clock skew: command stamped {stamped_ms} ms, leader clock {leader_ms} ms")]
    ClockSkew { stamped_ms: u64, leader_ms: u64 },
    /// The node-local database or the snapshot store failed.
    #[error("storage error: {0}")]
    Storage(#[from] io::Error),
    #[error("invalid configuration: {0}")]
    Config(String),
    /// The state machine replied with a variant the caller did not expect: a
    /// bug. Carries the reply's `Debug` text.
    #[error("unexpected reply: {0}")]
    UnexpectedReply(String),
}

/// The result of a metastore operation.
pub type MetaResult<T> = Result<T, MetaError>;
