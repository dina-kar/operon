use std::ops::Range;

use operon_common::{NamespaceId, StreamId};
use serde::{Deserialize, Serialize};

use std::collections::BTreeMap;

use crate::types::{
    Fence, Freshness, LeaseGrant, LinkId, Pointer, Retention, TargetRef, WalChunk, WalClass,
};

/// A change to the metastore. Commands are replicated through the Raft log and
/// applied in log order by [`crate::MetaState::apply`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    /// Creates a namespace. Names are unique. A retry after a lost
    /// acknowledgement fails with [`ApplyError::NamespaceExists`], which
    /// carries the id the first attempt created.
    CreateNamespace { name: String },
    /// Creates a stream in a namespace, with its retention policy. Names are
    /// unique within the namespace. A retry after a lost acknowledgement fails
    /// with [`ApplyError::StreamExists`], which carries the id the first
    /// attempt created.
    CreateStream {
        namespace: NamespaceId,
        name: String,
        partitions: u32,
        class: WalClass,
        retention: Retention,
    },
    /// Declares a link from stream `source` (in `namespace`) into `target`.
    /// Names are unique within the namespace. A retry after a lost
    /// acknowledgement fails with [`ApplyError::LinkExists`], which carries
    /// the id the first attempt created.
    CreateLink {
        namespace: NamespaceId,
        name: String,
        source: StreamId,
        target: TargetRef,
        options: BTreeMap<String, String>,
    },
    /// Assigns offsets to every chunk of a durable WAL object and appends the
    /// chunks to their partitions' offset indexes, atomically. Committing the
    /// same object again returns the offsets of the first commit and changes
    /// nothing, so a log node may retry after a lost acknowledgement.
    ///
    /// `created_at_ms` is the WAL object's creation time (its ULID time). A
    /// commit of an object not seen before is rejected with
    /// [`ApplyError::StaleCommit`] once `created_at_ms` is more than
    /// [`WAL_COMMIT_WINDOW_MS`](crate::WAL_COMMIT_WINDOW_MS) behind the
    /// metastore clock, and commit records are pruned only after twice that
    /// ([`Command::PruneWalCommits`]). A retry therefore either returns the
    /// first commit's offsets or is rejected; it never commits the object
    /// twice. A rejected *retry* does not mean the first attempt failed: its
    /// record may have been pruned (see [`ApplyError::StaleCommit`]).
    /// `created_at_ms` does not advance the metastore clock; the proposing
    /// leader refuses one too far in its future
    /// ([`MetaConfig::max_clock_skew`](crate::MetaConfig::max_clock_skew)).
    CommitWal {
        object: String,
        created_at_ms: u64,
        chunks: Vec<WalChunk>,
    },
    /// Sets a stream's retention policy. Setting the same policy again is a
    /// no-op, so a retry is safe.
    SetRetention {
        stream: StreamId,
        retention: Retention,
    },
    /// Replaces a contiguous run of WAL index entries of one partition with one
    /// segment entry covering the same offsets (design §02 §5). `replaces`
    /// names each replaced entry by `(base_offset, WAL object)`, in offset
    /// order; `byte_range` is the segment's data region.
    ///
    /// A retry after a lost acknowledgement finds a segment entry for
    /// `segment` at `replaces[0].0` and succeeds without changing anything (a
    /// segment path contains a ULID, so it names one swap). Otherwise the fence
    /// is checked, and every replaced entry must still be a WAL entry of the
    /// named object ([`ApplyError::IndexMismatch`] if a concurrent swap or trim
    /// moved it), and the segment must still be fresh: once the metastore
    /// clock (or `now_ms`) is past `fresh`, the swap is refused with
    /// [`ApplyError::StaleObject`], because garbage collection may already
    /// have deleted the segment.
    SwapSegment {
        stream: StreamId,
        partition: u32,
        replaces: Vec<(u64, String)>,
        segment: String,
        byte_range: Range<u64>,
        max_timestamp_ms: i64,
        fence: Option<Fence>,
        now_ms: u64,
        fresh: Freshness,
    },
    /// Makes offsets below `before_offset` (capped at the high watermark)
    /// unreadable and drops the index entries wholly below it. The log start
    /// only moves forward, so a retry is a no-op that returns the same log
    /// start. With a `fence`, the trim is applied only while the fencing lease
    /// is at the fence's epoch ([`ApplyError::Fenced`] otherwise, and nothing
    /// changes); a retry whose fence was broken after the first attempt
    /// applied is rejected, but the first attempt's trim stays.
    TrimPartition {
        stream: StreamId,
        partition: u32,
        before_offset: u64,
        fence: Option<Fence>,
        now_ms: u64,
    },
    /// Forgets WAL commit records older than twice the commit window. A retry
    /// removes nothing more. Fenced like [`Command::TrimPartition`].
    PruneWalCommits { fence: Option<Fence>, now_ms: u64 },
    /// Removes collected objects from the retired set. Unknown paths are
    /// ignored, so a retry is safe. Fenced like [`Command::TrimPartition`]:
    /// garbage collection forgets objects under its task lease.
    ForgetObjects {
        objects: Vec<String>,
        fence: Option<Fence>,
    },
    /// Takes a free or expired lease for `ttl_ms`, bumping its epoch. If
    /// `owner` already holds the lease, extends it to `now_ms + ttl_ms` and
    /// keeps the epoch, so a retry after a lost acknowledgement gets the same
    /// epoch back (with the retry's deadline). If the lease expired between
    /// the attempts, the retry takes it again at the next epoch, and fences at
    /// the first attempt's epoch fail.
    ///
    /// The same owner string always shares the lease and its epoch, so owners
    /// must be unique per process incarnation (for example a host name plus a
    /// random suffix chosen at start): a restarted worker that reused its
    /// predecessor's owner would share fencing rights with it.
    AcquireLease {
        key: String,
        owner: String,
        ttl_ms: u64,
        now_ms: u64,
    },
    /// Extends a held, unexpired lease by `ttl_ms` from `now_ms`. A retry
    /// after a lost acknowledgement extends it again from the retry's
    /// `now_ms`, or fails with [`ApplyError::LeaseLost`] if the lease expired
    /// in between, which the first attempt would not have prevented.
    RenewLease {
        key: String,
        owner: String,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
    },
    /// Re-takes an expired lease that nobody else took: if `owner` still
    /// holds the lease at `epoch` (not released and not taken over), its
    /// deadline becomes `now_ms + ttl_ms` and the epoch stays, whether or not
    /// it had expired. Otherwise it fails with [`ApplyError::LeaseLost`] and
    /// changes nothing. Fences at `epoch` stay valid throughout, because
    /// expiry alone never broke them. A retry after a lost acknowledgement
    /// extends the deadline again (or fails the same way if someone took the
    /// lease in between), so it is safe. Worker tasks use it to keep running
    /// after a renewal came too late (M0.3 re-review N3).
    ReacquireLease {
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
    /// (or 1 for a new pointer). A retry after a lost acknowledgement fails
    /// with [`ApplyError::VersionMismatch`]. A current pointer holding the
    /// caller's value at `expected + 1` means the first attempt *may* have
    /// succeeded: another writer may have written the same value. Callers
    /// that must know write a unique value (for example a manifest path
    /// containing a ULID). With `fresh`, the objects the new value makes
    /// reachable must still be fresh at the metastore clock
    /// ([`ApplyError::StaleObject`] otherwise).
    CasPointer {
        namespace: NamespaceId,
        key: String,
        expected: Option<u64>,
        value: String,
        fence: Option<Fence>,
        fresh: Option<Freshness>,
    },
}

/// The result of successfully applying a [`Command`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Reply {
    NamespaceCreated(NamespaceId),
    StreamCreated(StreamId),
    LinkCreated(LinkId),
    /// The base offset of each chunk, in the order the chunks were given.
    WalCommitted {
        base_offsets: Vec<u64>,
    },
    Lease(LeaseGrant),
    LeaseReleased,
    PointerSet {
        version: u64,
    },
    RetentionSet,
    SegmentSwapped,
    Trimmed {
        log_start_offset: u64,
    },
    Pruned {
        removed: u32,
    },
    Forgotten {
        removed: u32,
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
    /// A WAL object is too old to commit (see [`Command::CommitWal`]), and no
    /// commit record for it remains. On a first attempt its records were never
    /// committed. On a retry after an attempt whose outcome was unknown, the
    /// first attempt may have committed them and its record may since have
    /// been pruned: the outcome is still unknown.
    #[error("stale WAL commit: {object}")]
    StaleCommit { object: String },
    /// A command would reference an object created too long ago
    /// ([`Freshness`]): garbage collection may already have deleted it.
    /// Nothing changed; the object is left to garbage collection.
    #[error(
        "stale object {object}: created at {created_at_ms} ms, max age {max_age_ms} ms, metastore clock {clock_ms} ms"
    )]
    StaleObject {
        object: String,
        created_at_ms: u64,
        max_age_ms: u64,
        clock_ms: u64,
    },
}

impl std::fmt::Display for Command {
    /// A short summary for logs; openraft requires log payloads to be `Display`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::CreateNamespace { name } => write!(f, "CreateNamespace({name})"),
            Command::CreateStream {
                namespace, name, ..
            } => write!(f, "CreateStream({namespace}/{name})"),
            Command::CreateLink {
                namespace, name, ..
            } => write!(f, "CreateLink({namespace}/{name})"),
            Command::CommitWal { object, chunks, .. } => {
                write!(f, "CommitWal({object}, {} chunks)", chunks.len())
            }
            Command::SetRetention { stream, .. } => write!(f, "SetRetention({stream})"),
            Command::SwapSegment {
                stream,
                partition,
                replaces,
                segment,
                ..
            } => write!(
                f,
                "SwapSegment({stream}/{partition}, {segment}, {} entries)",
                replaces.len()
            ),
            Command::TrimPartition {
                stream,
                partition,
                before_offset,
                ..
            } => write!(
                f,
                "TrimPartition({stream}/{partition}, before {before_offset})"
            ),
            Command::PruneWalCommits { .. } => write!(f, "PruneWalCommits"),
            Command::ForgetObjects { objects, .. } => {
                write!(f, "ForgetObjects({} objects)", objects.len())
            }
            Command::AcquireLease { key, owner, .. } => write!(f, "AcquireLease({key}, {owner})"),
            Command::RenewLease {
                key, owner, epoch, ..
            } => write!(f, "RenewLease({key}, {owner}, epoch {epoch})"),
            Command::ReacquireLease {
                key, owner, epoch, ..
            } => write!(f, "ReacquireLease({key}, {owner}, epoch {epoch})"),
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
