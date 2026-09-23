use operon_common::{NamespaceId, StreamId};
use serde::{Deserialize, Serialize};

use crate::types::WalClass;

/// A change to the metastore. Commands are replicated through the Raft log and
/// applied in log order by [`crate::MetaState::apply`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    /// Creates a namespace. Names are unique.
    CreateNamespace { name: String },
    /// Creates a stream in a namespace. Names are unique within the namespace.
    CreateStream {
        namespace: NamespaceId,
        name: String,
        partitions: u32,
        class: WalClass,
    },
}

/// The result of successfully applying a [`Command`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Reply {
    NamespaceCreated(NamespaceId),
    StreamCreated(StreamId),
}

/// Why a [`Command`] was rejected. A rejected command leaves the state unchanged.
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
}
