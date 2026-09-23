use operon_common::{NamespaceId, StreamId};
use serde::{Deserialize, Serialize};

use crate::types::{Fence, LeaseGrant, Pointer, WalChunk, WalClass};

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
    /// Assigns offsets to every chunk of a durable WAL object and appends the
    /// chunks to their partitions' offset indexes, atomically. Committing the
    /// same object again returns the offsets of the first commit and changes
    /// nothing, so a log node may retry after a lost acknowledgement.
    CommitWal {
        object: String,
        chunks: Vec<WalChunk>,
    },
    /// Takes a free or expired lease for `ttl_ms`, bumping its epoch. If
    /// `owner` already holds the lease, extends it and keeps the epoch, so a
    /// retry after a lost acknowledgement gets the same grant back.
    AcquireLease {
        key: String,
        owner: String,
        ttl_ms: u64,
        now_ms: u64,
    },
    /// Extends a held, unexpired lease by `ttl_ms` from `now_ms`.
    RenewLease {
        key: String,
        owner: String,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
    },
    /// Releases a lease. Releasing an already-released lease at the same
    /// epoch succeeds, so the command is safe to retry.
    ReleaseLease {
        key: String,
        owner: String,
        epoch: u64,
    },
    /// Sets a pointer if its current version is `expected` (`None`: the
    /// pointer must not exist yet) and, when `fence` is given, the fencing
    /// lease is still at the fence's epoch. The new version is `expected + 1`
    /// (or 1 for a new pointer).
    CasPointer {
        namespace: NamespaceId,
        key: String,
        expected: Option<u64>,
        value: String,
        fence: Option<Fence>,
    },
}

/// The result of successfully applying a [`Command`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Reply {
    NamespaceCreated(NamespaceId),
    StreamCreated(StreamId),
    /// The base offset of each chunk, in the order the chunks were given.
    WalCommitted {
        base_offsets: Vec<u64>,
    },
    Lease(LeaseGrant),
    LeaseReleased,
    PointerSet {
        version: u64,
    },
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
    #[error("stream not found: {0}")]
    StreamNotFound(StreamId),
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
}

impl std::fmt::Display for Command {
    /// A short summary for logs; openraft requires log payloads to be `Display`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::CreateNamespace { name } => write!(f, "CreateNamespace({name})"),
            Command::CreateStream {
                namespace, name, ..
            } => write!(f, "CreateStream({namespace}/{name})"),
            Command::CommitWal { object, chunks } => {
                write!(f, "CommitWal({object}, {} chunks)", chunks.len())
            }
            Command::AcquireLease { key, owner, .. } => write!(f, "AcquireLease({key}, {owner})"),
            Command::RenewLease {
                key, owner, epoch, ..
            } => write!(f, "RenewLease({key}, {owner}, epoch {epoch})"),
            Command::ReleaseLease { key, owner, epoch } => {
                write!(f, "ReleaseLease({key}, {owner}, epoch {epoch})")
            }
            Command::CasPointer {
                namespace,
                key,
                expected,
                ..
            } => write!(f, "CasPointer({namespace}/{key}, expected {expected:?})"),
        }
    }
}
