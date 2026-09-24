use std::io;

use crate::command::{ApplyError, Reply};
use crate::raft::NodeId;

/// Errors returned by [`crate::MetaNode`].
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
    NotLeader { leader: Option<NodeId> },
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
    /// than [`MetaConfig::max_clock_skew`](crate::MetaConfig::max_clock_skew)
    /// ahead of its own clock `leader_ms`. Nothing was proposed; fix the
    /// proposer's clock. Not retried by [`MetaClient`](crate::MetaClient).
    #[error("clock skew: command stamped {stamped_ms} ms, leader clock {leader_ms} ms")]
    ClockSkew { stamped_ms: u64, leader_ms: u64 },
    /// The node-local database or the snapshot store failed.
    #[error("storage error: {0}")]
    Storage(#[from] io::Error),
    #[error("invalid configuration: {0}")]
    Config(String),
    /// The state machine replied with a variant the caller did not expect: a bug.
    #[error("unexpected reply: {0:?}")]
    UnexpectedReply(Reply),
}
