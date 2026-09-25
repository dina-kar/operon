//! openraft type configuration for the metastore.

use operon_common::meta::ApplyError;

use crate::command::{Command, Reply};

/// Identifies a meta node. Chosen by the operator and stable across restarts.
pub type NodeId = u64;

/// What applying one Raft log entry produced: the command's result for a
/// normal entry, `None` for blank and membership entries.
pub type EntryReply = Option<Result<Reply, ApplyError>>;

openraft::declare_raft_types!(
    /// openraft type configuration for the metastore.
    pub TypeConfig:
        D = Command,
        R = EntryReply,
        NodeId = NodeId,
        Node = openraft::BasicNode,
);

pub(crate) type LogId = openraft::type_config::alias::LogIdOf<TypeConfig>;
pub(crate) type StoredMembership = openraft::type_config::alias::StoredMembershipOf<TypeConfig>;
pub(crate) type SnapshotMeta = openraft::type_config::alias::SnapshotMetaOf<TypeConfig>;
/// Snapshot bytes as they travel between openraft, nodes and object storage.
pub type SnapshotData = std::io::Cursor<Vec<u8>>;
pub(crate) type Snapshot = openraft::type_config::alias::SnapshotOf<TypeConfig, SnapshotData>;
