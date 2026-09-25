use std::collections::BTreeMap;
use std::ops::Range;

use operon_common::schema::CollectionSchema;
use operon_common::{CollectionId, NamespaceId, StreamId};
use serde::{Deserialize, Serialize};

/// A namespace: the unit of tenancy, quotas and routing (design §01 §1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Namespace {
    pub id: NamespaceId,
    pub name: String,
}

/// WAL durability class of a stream (design §02 §2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalClass {
    Standard,
    Express,
    Quorum,
}

/// How long a stream keeps its records (design §02 §5). `None` means no limit
/// of that kind; with both `None` (the default) records are kept forever.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Retention {
    /// Records whose index entry's newest timestamp is older than this are
    /// trimmed.
    pub max_age_ms: Option<u64>,
    /// Whole oldest index entries are trimmed while a partition holds more
    /// bytes than this.
    pub max_bytes: Option<u64>,
}

/// Identifies a link (design §09).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LinkId(pub u64);

impl std::fmt::Display for LinkId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// What a link materializes into: a target `kind` (such as `counter`, the
/// M0 test target) and the target's name.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetRef {
    pub kind: String,
    pub name: String,
}

/// A declared, continuously maintained materialization of a stream into a
/// target (design §09 §1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Link {
    pub id: LinkId,
    pub namespace: NamespaceId,
    pub name: String,
    pub source: StreamId,
    pub target: TargetRef,
    pub options: BTreeMap<String, String>,
}

/// A partitioned, offset-addressed stream (design §01 §2.1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stream {
    pub id: StreamId,
    pub namespace: NamespaceId,
    pub name: String,
    pub partitions: u32,
    pub class: WalClass,
    pub retention: Retention,
}

/// One partition's records inside a WAL object, as reported by the log node
/// that wrote the object (design §02 §3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalChunk {
    pub stream: StreamId,
    pub partition: u32,
    /// Number of records in the chunk. Must be at least 1.
    pub records: u32,
    /// Where the chunk's bytes sit inside the WAL object. Must be non-empty.
    pub byte_range: Range<u64>,
    pub max_timestamp_ms: i64,
}

/// What kind of object an offset index entry points into.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryKind {
    /// One partition's chunk inside a multi-partition WAL object; `byte_range`
    /// holds the chunk's record batches.
    Wal,
    /// A per-partition segment; `byte_range` is the segment's data region, and
    /// the segment's footer indexes its batches.
    Segment,
}

/// An offset index entry: records `[base_offset, base_offset + records)` of a
/// partition live at `byte_range` inside `object`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexEntry {
    pub kind: EntryKind,
    pub base_offset: u64,
    pub records: u32,
    pub object: String,
    pub byte_range: Range<u64>,
    pub max_timestamp_ms: i64,
}

impl IndexEntry {
    /// One past the last offset in this entry.
    pub fn end_offset(&self) -> u64 {
        self.base_offset + u64::from(self.records)
    }
}

/// How long the metastore remembers a WAL commit for deduplication, and how
/// old a WAL object may be when it is first committed: 15 minutes (M0.3 plan,
/// ruling 6).
///
/// A commit whose `created_at_ms` is older than this relative to the
/// metastore clock is rejected with `StaleCommit`, and commit records are
/// pruned once they are twice this old. So a retried commit either finds its
/// record (and gets the first commit's offsets) or is rejected; it is never
/// committed twice.
pub const WAL_COMMIT_WINDOW_MS: u64 = 900_000;

/// What the metastore remembers about a committed WAL object, so a retried
/// commit returns the first commit's offsets.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalCommitRecord {
    /// The base offset of each chunk, in the order the chunks were given.
    pub base_offsets: Vec<u64>,
    /// When the WAL object was created (its ULID time), in ms since the epoch.
    pub created_at_ms: u64,
}

/// Sequencer state of one stream partition: the next offset to assign, the
/// first readable offset, and the offset index, keyed by base offset.
///
/// The index entries tile `[first entry's base, next_offset)` without gaps; the
/// first entry may start below `log_start_offset` when a trim cut it in the
/// middle.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionState {
    pub(crate) next_offset: u64,
    pub(crate) log_start_offset: u64,
    pub(crate) index: BTreeMap<u64, IndexEntry>,
    /// The sum of the index entries' byte range lengths, kept as entries are
    /// added and removed (M0.3 re-review M13).
    pub(crate) bytes: u64,
}

fn entry_bytes(entry: &IndexEntry) -> u64 {
    entry.byte_range.end.saturating_sub(entry.byte_range.start)
}

impl PartitionState {
    /// The offset the next committed record will get. For `standard` streams this
    /// is also the high watermark: every offset below it is committed and readable.
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// The first readable offset. Offsets below it were trimmed by retention.
    pub fn log_start_offset(&self) -> u64 {
        self.log_start_offset
    }

    /// One past the last readable offset. For `standard` streams it equals
    /// [`PartitionState::next_offset`].
    pub fn high_watermark(&self) -> u64 {
        self.next_offset
    }

    /// The bytes the partition's index entries cover: the sum of their byte
    /// range lengths. O(1): kept up to date as entries change.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Adds an index entry, keeping the byte count.
    pub(crate) fn insert_entry(&mut self, entry: IndexEntry) {
        self.bytes += entry_bytes(&entry);
        if let Some(replaced) = self.index.insert(entry.base_offset, entry) {
            self.bytes -= entry_bytes(&replaced);
        }
    }

    /// Removes the index entry at `base_offset`, keeping the byte count.
    pub(crate) fn remove_entry(&mut self, base_offset: u64) -> Option<IndexEntry> {
        let entry = self.index.remove(&base_offset)?;
        self.bytes -= entry_bytes(&entry);
        Some(entry)
    }

    /// Removes and returns the first index entry if it ends at or before
    /// `offset`.
    pub(crate) fn pop_first_before(&mut self, offset: u64) -> Option<IndexEntry> {
        let base = self
            .index
            .first_key_value()
            .filter(|(_, e)| e.end_offset() <= offset)
            .map(|(base, _)| *base)?;
        self.remove_entry(base)
    }

    /// The index entry holding `offset`, if that offset is committed and not
    /// trimmed.
    pub fn lookup(&self, offset: u64) -> Option<&IndexEntry> {
        if offset < self.log_start_offset {
            return None;
        }
        self.index
            .range(..=offset)
            .next_back()
            .map(|(_, entry)| entry)
            .filter(|entry| offset < entry.end_offset())
    }

    /// Index entries in offset order, starting with the one holding `offset`
    /// (or the first one after it). Offsets below the log start are treated as
    /// the log start, so trimmed entries are never returned.
    pub fn entries_from(&self, offset: u64) -> impl Iterator<Item = &IndexEntry> {
        let offset = offset.max(self.log_start_offset);
        let start = self
            .lookup(offset)
            .map_or(offset, |entry| entry.base_offset);
        self.index.range(start..).map(|(_, entry)| entry)
    }

    /// Every index entry, in offset order.
    pub fn entries(&self) -> impl Iterator<Item = &IndexEntry> {
        self.index.values()
    }
}

/// A lease on a key, such as a worker task (design §09 §3, §6).
///
/// The epoch grows by one every time a different holder takes the lease and
/// never goes back, so a holder fenced by its epoch can detect that someone
/// else has taken over.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub epoch: u64,
    /// `None` once the holder has released the lease.
    pub owner: Option<String>,
    pub deadline_ms: u64,
}

impl Lease {
    /// Whether the lease is held (not released and not expired) at `now_ms`.
    pub fn is_held_at(&self, now_ms: u64) -> bool {
        self.owner.is_some() && now_ms < self.deadline_ms
    }
}

/// What a successful acquire or renew hands back to the holder.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseGrant {
    pub epoch: u64,
    pub deadline_ms: u64,
}

/// A precondition that a lease is still at `epoch` (not released and not
/// taken over by anyone else). Expiry alone does not break a fence: until
/// another holder takes the lease, nobody else can have acted under it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fence {
    pub lease: String,
    pub epoch: u64,
}

/// How long a newly written object may take to become referenced: a command
/// that makes the metastore reference it is refused
/// ([`ApplyError::StaleObject`](crate::ApplyError::StaleObject)) once the
/// metastore clock is past `created_at_ms + max_age_ms`. Garbage collection
/// deletes an unreferenced object only once the metastore clock is at least
/// `created_at_ms + grace`, so with `max_age_ms` below the grace a command
/// applied after GC decided to delete an object is always refused (M0.4
/// review I1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Freshness {
    /// When the object was created, by the writer's clock (the time in its
    /// ULID).
    pub created_at_ms: u64,
    pub max_age_ms: u64,
}

impl Freshness {
    /// Whether a command carrying this freshness is refused at metastore
    /// clock `clock_ms`.
    pub fn expired_at(&self, clock_ms: u64) -> bool {
        self.created_at_ms.saturating_add(self.max_age_ms) < clock_ms
    }
}

/// A versioned pointer, such as a collection's current manifest location.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pointer {
    pub version: u64,
    pub value: String,
}

/// The target kind of a collection's implicit link.
pub const COLLECTION_KIND: &str = "collection";

/// Longest collection name, in bytes: the implicit stream and link name
/// `_collection.<name>.<id>` must fit [`MAX_NAME_LEN`](crate::MAX_NAME_LEN)
/// with any id (12 + 222 + 1 + 20 = 255).
pub const MAX_COLLECTION_NAME_LEN: usize = 222;

/// A collection (M1 overview §6.1): documents under a schema, written through
/// its implicit stream and materialized by its implicit link. Both are named
/// [`implicit_name`], and live and die with the collection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Collection {
    pub id: CollectionId,
    pub namespace: NamespaceId,
    pub name: String,
    pub schema: CollectionSchema,
    pub partitions: u32,
    pub stream: StreamId,
    pub link: LinkId,
}

/// One change of [`Command::UpdateAliases`](crate::Command::UpdateAliases).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AliasAction {
    /// Points `alias` at the collection named `collection` (a collection
    /// name, not an alias), creating or re-pointing it.
    Create { alias: String, collection: String },
    /// Removes `alias`; a missing alias is a no-op.
    Delete { alias: String },
}

/// The name of a collection's implicit stream and link:
/// `_collection.<name>.<id>`.
pub fn implicit_name(collection: &str, id: CollectionId) -> String {
    format!("_collection.{collection}.{id}")
}

/// Pointer keys under this prefix belong to collections.
pub(crate) const COLLECTION_POINTER_PREFIX: &str = "collection/";

/// The pointer key of a collection's manifest: `collection/<id>`.
pub fn collection_pointer_key(id: CollectionId) -> String {
    format!("{COLLECTION_POINTER_PREFIX}{id}")
}

/// Where a collection's objects live: `ns/<ns>/collections/<id>/`.
pub fn collection_prefix(ns: NamespaceId, id: CollectionId) -> String {
    format!("ns/{ns}/collections/{id}/")
}

/// Where a collection's primary-key index lives: `ns/<ns>/pk/collection-<id>/`.
pub fn collection_pk_prefix(ns: NamespaceId, id: CollectionId) -> String {
    format!("ns/{ns}/pk/collection-{id}/")
}
