//! Owned read models the [`MetaStore`](super::MetaStore) returns, and the
//! request structs its tracked writes take. None of them is serialized: they
//! are the trait's vocabulary, not the metastore's on-disk format.

use std::ops::Range;

use crate::meta::types::{
    Collection, Fence, Freshness, IndexEntry, Link, Pointer, Stream, WalChunk,
};
use crate::{NamespaceId, StreamId};

/// A partition's bounds and a run of its offset-index entries.
///
/// The bounds (`log_start_offset`, `next_offset`, `bytes`) describe the whole
/// partition; `entries` is the run a
/// [`partition_index`](super::MetaStore::partition_index) call asked for, in
/// ascending base offset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionIndex {
    log_start_offset: u64,
    next_offset: u64,
    bytes: u64,
    entries: Vec<IndexEntry>,
}

impl PartitionIndex {
    /// A partition index; `entries` must be in ascending base offset.
    pub fn new(
        log_start_offset: u64,
        next_offset: u64,
        bytes: u64,
        entries: Vec<IndexEntry>,
    ) -> Self {
        Self {
            log_start_offset,
            next_offset,
            bytes,
            entries,
        }
    }

    /// The offset the next committed record will get.
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// One past the last readable offset. For `standard` streams it equals
    /// [`PartitionIndex::next_offset`].
    pub fn high_watermark(&self) -> u64 {
        self.next_offset
    }

    /// The first readable offset. Offsets below it were trimmed by retention.
    pub fn log_start_offset(&self) -> u64 {
        self.log_start_offset
    }

    /// The bytes the whole partition's index entries cover (the sum of their
    /// byte range lengths), not only the returned entries'.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// The returned run of index entries, in ascending base offset.
    pub fn entries(&self) -> impl Iterator<Item = &IndexEntry> {
        self.entries.iter()
    }

    /// The returned run of index entries, in ascending base offset.
    pub fn into_entries(self) -> Vec<IndexEntry> {
        self.entries
    }
}

/// A partition's bounds, without its index entries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionBounds {
    /// The first readable offset.
    pub log_start_offset: u64,
    /// One past the last readable offset.
    pub high_watermark: u64,
    /// The bytes the partition's index entries cover.
    pub bytes: u64,
}

/// A stream with the bounds of each of its partitions, from one state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamState {
    pub stream: Stream,
    /// Index = partition, for `0..stream.partitions`. `None` where the
    /// metastore holds no state for the partition.
    pub partitions: Vec<Option<PartitionBounds>>,
}

/// A link with its target's manifest pointer, from one state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkHead {
    pub link: Link,
    /// The pointer at [`link_pointer_key`](crate::meta::link_pointer_key)`(link.id)`
    /// in the link's namespace, if it has been set.
    pub pointer: Option<Pointer>,
}

/// A collection with its manifest pointer, its implicit stream's bounds and
/// the metastore clock, all from one state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CollectionHead {
    pub collection: Collection,
    /// The pointer at
    /// [`collection_pointer_key`](crate::meta::collection_pointer_key)`(collection.id)`
    /// in the collection's namespace, if it has been set.
    pub pointer: Option<Pointer>,
    /// The implicit stream's log start per partition (index = partition); 0
    /// for a partition without state.
    pub log_start_offsets: Vec<u64>,
    /// The implicit stream's high watermark per partition (index =
    /// partition); 0 for a partition without state.
    pub high_watermarks: Vec<u64>,
    /// The metastore clock of the same state.
    pub clock_ms: u64,
}

/// What collection garbage collection must keep in one namespace, from one
/// state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CollectionRoots {
    /// The metastore clock of the same state.
    pub clock_ms: u64,
    /// The namespace's collections, by id, each with its manifest pointer.
    pub collections: Vec<(Collection, Option<Pointer>)>,
    /// Retired paths ending in `/` (the prefixes of dropped collections) that
    /// start with the requested path, in path order.
    pub retired_prefixes: Vec<String>,
}

/// A request to commit a durable WAL object
/// ([`commit_wal`](super::MetaStore::commit_wal)).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalCommit {
    /// The WAL object's path.
    pub object: String,
    /// The WAL object's creation time (its ULID time), in ms since the epoch.
    pub created_at_ms: u64,
    /// The object's chunks, one per partition; offsets are assigned in this
    /// order.
    pub chunks: Vec<WalChunk>,
}

/// A request to replace a run of WAL index entries of one partition with one
/// segment entry ([`swap_segment`](super::MetaStore::swap_segment)).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentSwap {
    pub stream: StreamId,
    pub partition: u32,
    /// Each replaced entry as `(base_offset, WAL object)`, in offset order.
    pub replaces: Vec<(u64, String)>,
    /// The segment's path.
    pub segment: String,
    /// The segment's data region.
    pub byte_range: Range<u64>,
    pub max_timestamp_ms: i64,
    pub fence: Option<Fence>,
    /// How long the segment may take to become referenced.
    pub fresh: Freshness,
}

/// A compare-and-swap of a manifest pointer
/// ([`cas_pointer`](super::MetaStore::cas_pointer)).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PointerCas {
    pub namespace: NamespaceId,
    pub key: String,
    /// The current version the write expects; `None`: the pointer must not
    /// exist yet.
    pub expected: Option<u64>,
    pub value: String,
    pub fence: Option<Fence>,
    /// When given, the objects the new value makes reachable must still be
    /// fresh at the metastore clock.
    pub fresh: Option<Freshness>,
}
