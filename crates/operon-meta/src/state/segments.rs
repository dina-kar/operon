//! Segment swaps, trimming, and the lifecycle of the objects index entries
//! point into (design §02 §5, §03 §7).

use std::ops::Range;

use operon_common::StreamId;

use super::{MetaState, validate_key};
use crate::command::{ApplyError, Reply};
use crate::types::{EntryKind, Fence, Freshness, IndexEntry, PartitionState};

impl MetaState {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn swap_segment(
        &mut self,
        stream: StreamId,
        partition: u32,
        replaces: Vec<(u64, String)>,
        segment: String,
        byte_range: Range<u64>,
        max_timestamp_ms: i64,
        fence: Option<Fence>,
        now_ms: u64,
        fresh: Freshness,
    ) -> Result<Reply, ApplyError> {
        validate_key("segment path", &segment)?;
        let state = self.partition_state(stream, partition)?;
        let Some((first_base, _)) = replaces.first() else {
            return Err(ApplyError::InvalidArgument(
                "a segment swap must replace at least one entry".to_string(),
            ));
        };
        // A retry of a swap that was applied: the segment entry is in place.
        if let Some(entry) = state.index.get(first_base)
            && entry.kind == EntryKind::Segment
            && entry.object == segment
        {
            return Ok(Reply::SegmentSwapped);
        }
        if let Some(fence) = &fence {
            self.check_fence(fence)?;
        }
        if byte_range.start >= byte_range.end {
            return Err(ApplyError::InvalidArgument(format!(
                "a segment needs a non-empty byte range, got {byte_range:?}"
            )));
        }
        let clock_ms = self.clock_ms.max(now_ms);
        if fresh.expired_at(clock_ms) {
            return Err(ApplyError::StaleObject {
                object: segment,
                created_at_ms: fresh.created_at_ms,
                max_age_ms: fresh.max_age_ms,
                clock_ms,
            });
        }
        let mismatch = || ApplyError::IndexMismatch { stream, partition };
        let mut expected_base = *first_base;
        let mut records: u32 = 0;
        for (base, object) in &replaces {
            let entry = state.index.get(base).ok_or_else(mismatch)?;
            if *base != expected_base || entry.kind != EntryKind::Wal || entry.object != *object {
                return Err(mismatch());
            }
            records = records.checked_add(entry.records).ok_or_else(|| {
                ApplyError::InvalidArgument("a segment holds at most u32::MAX records".to_string())
            })?;
            expected_base = entry.end_offset();
        }

        // Everything is checked: apply.
        self.clock_ms = self.clock_ms.max(now_ms);
        let state = self.partition_state_mut(stream, partition)?;
        let mut removed = Vec::with_capacity(replaces.len());
        for (base, _) in &replaces {
            if let Some(entry) = state.remove_entry(*base) {
                removed.push(entry);
            }
        }
        state.insert_entry(IndexEntry {
            kind: EntryKind::Segment,
            base_offset: *first_base,
            records,
            object: segment,
            byte_range,
            max_timestamp_ms,
        });
        for entry in removed {
            self.release_entry(entry);
        }
        Ok(Reply::SegmentSwapped)
    }

    pub(super) fn trim_partition(
        &mut self,
        stream: StreamId,
        partition: u32,
        before_offset: u64,
        fence: Option<Fence>,
        now_ms: u64,
    ) -> Result<Reply, ApplyError> {
        self.partition_state(stream, partition)?;
        if let Some(fence) = &fence {
            self.check_fence(fence)?;
        }
        self.clock_ms = self.clock_ms.max(now_ms);
        let state = self.partition_state_mut(stream, partition)?;
        let before = before_offset.min(state.next_offset);
        let mut removed = Vec::new();
        while let Some(entry) = state.pop_first_before(before) {
            removed.push(entry);
        }
        state.log_start_offset = state.log_start_offset.max(before);
        let log_start_offset = state.log_start_offset;
        for entry in removed {
            self.release_entry(entry);
        }
        Ok(Reply::Trimmed { log_start_offset })
    }

    /// Accounts for an index entry that was removed: a WAL chunk decrements
    /// its object's live count, retiring the object at zero; a segment is
    /// retired at once. Retirement is stamped with the metastore clock.
    pub(super) fn release_entry(&mut self, entry: IndexEntry) {
        match entry.kind {
            EntryKind::Wal => {
                let remaining = match self.wal_live_chunks.get_mut(&entry.object) {
                    Some(live) => {
                        *live = live.saturating_sub(1);
                        *live
                    }
                    // Every WAL entry is counted at commit, so this is
                    // unreachable; retiring keeps the object collectable.
                    None => 0,
                };
                if remaining == 0 {
                    self.wal_live_chunks.remove(&entry.object);
                    self.retired.insert(entry.object, self.clock_ms);
                }
            }
            EntryKind::Segment => {
                self.retired.insert(entry.object, self.clock_ms);
            }
        }
    }

    fn partition_state(
        &self,
        stream: StreamId,
        partition: u32,
    ) -> Result<&PartitionState, ApplyError> {
        if !self.streams.contains_key(&stream) {
            return Err(ApplyError::StreamNotFound(stream));
        }
        self.partitions
            .get(&(stream, partition))
            .ok_or(ApplyError::PartitionNotFound { stream, partition })
    }

    fn partition_state_mut(
        &mut self,
        stream: StreamId,
        partition: u32,
    ) -> Result<&mut PartitionState, ApplyError> {
        self.partitions
            .get_mut(&(stream, partition))
            .ok_or(ApplyError::PartitionNotFound { stream, partition })
    }

    /// How many chunks of a WAL object are still `Wal` index entries; `None`
    /// once none are (or for an object never committed).
    pub fn wal_live_chunks(&self, object: &str) -> Option<u32> {
        self.wal_live_chunks.get(object).copied()
    }

    /// Objects no index entry references any more, with the metastore clock at
    /// their retirement, in path order.
    pub fn retired(&self) -> impl Iterator<Item = (&str, u64)> {
        self.retired.iter().map(|(path, at)| (path.as_str(), *at))
    }
}
