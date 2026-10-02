use std::ops::Range;

use loams_common::meta::{
    AliasAction, AliasTargetAction, Fence, Freshness, HotConfig, IdempotencyKey, IdempotencyState,
    LeaseGrant, LinkId, Retention, TargetRef, WalChunk, WalClass,
};
use loams_common::schema::CollectionSchema;
use loams_common::{CollectionId, NamespaceId, StreamId};
use serde::{Deserialize, Serialize};

use std::collections::BTreeMap;

/// A change to the metastore. Commands are replicated through the Raft log and
/// applied in log order by [`crate::MetaState::apply`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    /// Creates a namespace. Names are unique. A retry after a lost
    /// acknowledgement fails with
    /// [`ApplyError::NamespaceExists`](loams_common::meta::ApplyError::NamespaceExists),
    /// which carries the id the first attempt created.
    CreateNamespace { name: String },
    /// Creates a stream in a namespace, with its retention policy. Names are
    /// unique within the namespace. A retry after a lost acknowledgement fails
    /// with
    /// [`ApplyError::StreamExists`](loams_common::meta::ApplyError::StreamExists),
    /// which carries the id the first attempt created.
    CreateStream {
        namespace: NamespaceId,
        name: String,
        partitions: u32,
        class: WalClass,
        retention: Retention,
    },
    /// Declares a link from stream `source` (in `namespace`) into `target`.
    /// Names are unique within the namespace. A retry after a lost
    /// acknowledgement fails with
    /// [`ApplyError::LinkExists`](loams_common::meta::ApplyError::LinkExists),
    /// which carries the id the first attempt created.
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
    /// [`ApplyError::StaleCommit`](loams_common::meta::ApplyError::StaleCommit)
    /// once `created_at_ms` is more than
    /// [`WAL_COMMIT_WINDOW_MS`](loams_common::meta::WAL_COMMIT_WINDOW_MS)
    /// behind the metastore clock, and commit records are pruned only after
    /// twice that ([`Command::PruneWalCommits`]). A retry therefore either
    /// returns the first commit's offsets or is rejected; it never commits the
    /// object twice. A rejected *retry* does not mean the first attempt
    /// failed: its record may have been pruned (see
    /// [`ApplyError::StaleCommit`](loams_common::meta::ApplyError::StaleCommit)).
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
    /// named object
    /// ([`ApplyError::IndexMismatch`](loams_common::meta::ApplyError::IndexMismatch)
    /// if a concurrent swap or trim moved it), and the segment must still be
    /// fresh: once the metastore clock (or `now_ms`) is past `fresh`, the swap
    /// is refused with
    /// [`ApplyError::StaleObject`](loams_common::meta::ApplyError::StaleObject),
    /// because garbage collection may already have deleted the segment.
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
    /// is at the fence's epoch
    /// ([`ApplyError::Fenced`](loams_common::meta::ApplyError::Fenced)
    /// otherwise, and nothing changes); a retry whose fence was broken after
    /// the first attempt applied is rejected, but the first attempt's trim
    /// stays.
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
    /// `now_ms`, or fails with
    /// [`ApplyError::LeaseLost`](loams_common::meta::ApplyError::LeaseLost)
    /// if the lease expired in between, which the first attempt would not
    /// have prevented.
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
    /// it had expired. Otherwise it fails with
    /// [`ApplyError::LeaseLost`](loams_common::meta::ApplyError::LeaseLost)
    /// and changes nothing. Fences at `epoch` stay valid throughout, because
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
    /// with
    /// [`ApplyError::VersionMismatch`](loams_common::meta::ApplyError::VersionMismatch).
    /// A current pointer holding the caller's value at `expected + 1` means
    /// the first attempt *may* have succeeded: another writer may have
    /// written the same value. Callers that must know write a unique value
    /// (for example a manifest path containing a ULID). With `fresh`, the
    /// objects the new value makes reachable must still be fresh at the
    /// metastore clock
    /// ([`ApplyError::StaleObject`](loams_common::meta::ApplyError::StaleObject)
    /// otherwise).
    CasPointer {
        namespace: NamespaceId,
        key: String,
        expected: Option<u64>,
        value: String,
        fence: Option<Fence>,
        fresh: Option<Freshness>,
    },
    /// Creates a collection with its implicit stream and link, both named
    /// [`implicit_name`](loams_common::meta::implicit_name) (class
    /// `Standard`, default retention, `partitions` partitions; the link's
    /// target is `collection`/`name`), in one step. Names are unique within
    /// the namespace across collections and aliases, and never start with
    /// `_`. `schema` must be valid and at version 1.
    ///
    /// A retry after a lost acknowledgement fails with
    /// [`ApplyError::CollectionExists`](loams_common::meta::ApplyError::CollectionExists),
    /// which carries the id the first attempt created. A collection of that
    /// name with another schema or partition count, or an alias of that name,
    /// gives
    /// [`ApplyError::NameTaken`](loams_common::meta::ApplyError::NameTaken).
    CreateCollection {
        namespace: NamespaceId,
        name: String,
        schema: CollectionSchema,
        partitions: u32,
    },
    /// Drops the collection `name` (not an alias): removes it, the aliases
    /// pointing at it, its implicit stream (retiring the objects only its
    /// index entries referenced), its implicit link and its manifest pointer,
    /// and retires its object and primary-key prefixes. The name is free at
    /// once; a new collection of that name gets a new id. Replies `None` when
    /// there is no such collection, so a retry after a lost acknowledgement
    /// succeeds with `None`.
    DropCollection {
        namespace: NamespaceId,
        name: String,
        now_ms: u64,
    },
    /// Replaces a collection's schema at `expected_version` with `schema`,
    /// which must extend it additively
    /// ([`CollectionSchema::check_additive`]); the new version is
    /// `expected_version + 1`. A stale `expected_version` fails with
    /// [`ApplyError::SchemaVersionMismatch`](loams_common::meta::ApplyError::SchemaVersionMismatch).
    /// A retry after a lost acknowledgement finds the same schema at
    /// `expected_version + 1` and succeeds.
    UpdateCollectionSchema {
        collection: CollectionId,
        expected_version: u64,
        schema: CollectionSchema,
    },
    /// Applies 1..=100 alias actions in order, atomically: if any fails,
    /// nothing changes. Creating an existing alias re-points it and deleting
    /// a missing one is a no-op, so a retry succeeds.
    UpdateAliases {
        namespace: NamespaceId,
        actions: Vec<AliasAction>,
    },
    /// Sets collection `collection`'s hot configuration (M1.3 Task 4,
    /// Ruling 20: appended last, so every earlier variant keeps its index).
    /// An all-false `hot` removes it. The collection must exist
    /// ([`ApplyError::CollectionNotFound`](loams_common::meta::ApplyError::CollectionNotFound)
    /// otherwise). Setting the same value again succeeds, so a retry is safe.
    SetCollectionHot {
        collection: CollectionId,
        hot: HotConfig,
    },
    /// Applies 1..=100 alias-target actions in order, atomically: if any
    /// fails, nothing changes (M1.5 Task 0a, Ruling 22: appended last). An
    /// alias may name several collections, each with an `is_write_index`
    /// setting, at most one of them `Some(true)`. Every action is
    /// idempotent, so a retry after a lost acknowledgement succeeds with the
    /// same state. Replies [`Reply::AliasesUpdated`].
    UpdateAliasTargets {
        namespace: NamespaceId,
        actions: Vec<AliasTargetAction>,
    },
    /// Claims 1..=[`MAX_IDEMPOTENCY_KEYS`](loams_common::meta::MAX_IDEMPOTENCY_KEYS)
    /// distinct keys of `stream` for `owner` until `now_ms + ttl_ms` (the
    /// stream ingest ledger, design §02 §7.4, D270; this and the next three
    /// are appended last). Each key absent, lapsed or pending under `owner`
    /// becomes pending under `owner`; the reply says per key what was found
    /// ([`Reply::IdempotencyClaimed`]). A retry claims the same keys again.
    ClaimIdempotencyKeys {
        stream: StreamId,
        owner: String,
        keys: Vec<IdempotencyKey>,
        ttl_ms: u64,
        now_ms: u64,
    },
    /// Marks each key still pending under `owner` done at its partition and
    /// offset, until `now_ms + window_ms`; other keys are left as they are,
    /// so a retry changes nothing.
    CompleteIdempotencyKeys {
        stream: StreamId,
        owner: String,
        done: Vec<(IdempotencyKey, u32, u64)>,
        window_ms: u64,
        now_ms: u64,
    },
    /// Drops each key still pending under `owner`. A retry is a no-op.
    ReleaseIdempotencyKeys {
        stream: StreamId,
        owner: String,
        keys: Vec<IdempotencyKey>,
    },
    /// Forgets lapsed ledger entries. A retry removes nothing more. Fenced
    /// like [`Command::TrimPartition`].
    PruneIdempotencyKeys { fence: Option<Fence>, now_ms: u64 },
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
    CollectionCreated {
        id: CollectionId,
        stream: StreamId,
        link: LinkId,
    },
    /// The dropped collection, or `None` if there was none of that name.
    CollectionDropped(Option<CollectionId>),
    SchemaUpdated {
        version: u64,
    },
    AliasesUpdated,
    /// [`Command::SetCollectionHot`] applied (M1.3; appended last).
    CollectionHotSet,
    /// What [`Command::ClaimIdempotencyKeys`] found for each key, in order
    /// (D270; appended last).
    IdempotencyClaimed {
        states: Vec<IdempotencyState>,
    },
    /// [`Command::CompleteIdempotencyKeys`] or
    /// [`Command::ReleaseIdempotencyKeys`] applied.
    IdempotencyKeysUpdated,
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
            Command::CreateCollection {
                namespace, name, ..
            } => write!(f, "CreateCollection({namespace}/{name})"),
            Command::DropCollection {
                namespace, name, ..
            } => write!(f, "DropCollection({namespace}/{name})"),
            Command::UpdateCollectionSchema {
                collection,
                expected_version,
                ..
            } => write!(
                f,
                "UpdateCollectionSchema({collection}, expected {expected_version})"
            ),
            Command::UpdateAliases { namespace, actions } => {
                write!(f, "UpdateAliases({namespace}, {} actions)", actions.len())
            }
            Command::SetCollectionHot { collection, hot } => write!(
                f,
                "SetCollectionHot({collection}, vectors {}, text {}, fragments {})",
                hot.vectors, hot.text, hot.fragments
            ),
            Command::UpdateAliasTargets { namespace, actions } => write!(
                f,
                "UpdateAliasTargets({namespace}, {} actions)",
                actions.len()
            ),
            Command::ClaimIdempotencyKeys {
                stream,
                owner,
                keys,
                ..
            } => write!(
                f,
                "ClaimIdempotencyKeys({stream}, {owner}, {} keys)",
                keys.len()
            ),
            Command::CompleteIdempotencyKeys {
                stream,
                owner,
                done,
                ..
            } => write!(
                f,
                "CompleteIdempotencyKeys({stream}, {owner}, {} keys)",
                done.len()
            ),
            Command::ReleaseIdempotencyKeys {
                stream,
                owner,
                keys,
            } => write!(
                f,
                "ReleaseIdempotencyKeys({stream}, {owner}, {} keys)",
                keys.len()
            ),
            Command::PruneIdempotencyKeys { .. } => write!(f, "PruneIdempotencyKeys"),
        }
    }
}
