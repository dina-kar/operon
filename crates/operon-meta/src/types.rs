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

/// A partitioned, offset-addressed stream (design §01 §2.1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stream {
    pub id: StreamId,
    pub namespace: NamespaceId,
    pub name: String,
    pub partitions: u32,
    pub class: WalClass,
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

/// An offset index entry: records `[base_offset, base_offset + records)` of a
/// partition live at `byte_range` inside `object`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexEntry {
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

/// Sequencer state of one stream partition: the next offset to assign and the
/// offset index, keyed by base offset.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionState {
    pub(crate) next_offset: u64,
    pub(crate) index: BTreeMap<u64, IndexEntry>,
}

impl PartitionState {
    /// The offset the next committed record will get. For `standard` streams this
    /// is also the high watermark: every offset below it is committed and readable.
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// The index entry holding `offset`, if that offset has been committed.
    pub fn lookup(&self, offset: u64) -> Option<&IndexEntry> {
        self.index
            .range(..=offset)
            .next_back()
            .map(|(_, entry)| entry)
            .filter(|entry| offset < entry.end_offset())
    }

    /// Index entries in offset order, starting with the one holding `offset`
    /// (or the first one after it).
    pub fn entries_from(&self, offset: u64) -> impl Iterator<Item = &IndexEntry> {
        let start = self
            .lookup(offset)
            .map_or(offset, |entry| entry.base_offset);
        self.index.range(start..).map(|(_, entry)| entry)
    }
}
