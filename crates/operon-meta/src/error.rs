use std::io;

use crate::command::{ApplyError, Reply};
use crate::raft::NodeId;

/// Errors returned by [`crate::MetaNode`].
#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    /// The command was applied and rejected by the state machine; nothing changed.
    #[error("rejected: {0}")]
    Rejected(#[from] ApplyError),
    /// This node is not the leader. Writes and linearizable reads must go to
    /// `leader`, if one is known.
    #[error("not the leader (leader: {leader:?})")]
    NotLeader { leader: Option<NodeId> },
    /// The request did not finish within the request timeout. A timed-out
    /// write may still be applied later; retry it (commands are retry-safe) or
    /// read to find out.
    #[error("request timed out; a write may still be applied")]
    Timeout,
    /// Raft has stopped or cannot make progress.
    #[error("metastore unavailable: {0}")]
    Unavailable(String),
    /// The node-local database or the snapshot store failed.
    #[error("storage error: {0}")]
    Storage(#[from] io::Error),
    #[error("invalid configuration: {0}")]
    Config(String),
    /// The state machine replied with a variant the caller did not expect: a bug.
    #[error("unexpected reply: {0:?}")]
    UnexpectedReply(Reply),
}
