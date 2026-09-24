use std::collections::BTreeMap;
use std::ops::Range;

use operon_common::{NamespaceId, StreamId};
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
    /// range lengths.
    pub fn bytes(&self) -> u64 {
        self.index
            .values()
            .map(|entry| entry.byte_range.end - entry.byte_range.start)
            .sum()
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

/// A versioned pointer, such as a collection's current manifest location.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pointer {
    pub version: u64,
    pub value: String,
}
