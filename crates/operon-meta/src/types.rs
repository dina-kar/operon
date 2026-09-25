use std::collections::BTreeMap;

use operon_common::meta::IndexEntry;
use serde::{Deserialize, Serialize};

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
