//! Structural invariants of the state, for tests, the crash gate and the
//! simulation.

use std::collections::BTreeMap;

use super::MetaState;
use crate::types::EntryKind;

impl MetaState {
    /// Checks the invariants every sequence of commands must keep, and
    /// returns every violation found (empty if none):
    /// - each partition's index entries tile `[first base, next_offset)`
    ///   without gaps, the log start lies in the first entry (or equals
    ///   `next_offset` when the index is empty), and the byte count equals
    ///   the sum of the entries' byte ranges;
    /// - every WAL object's live chunk count equals its number of `Wal`
    ///   entries, and every `Wal` entry's object is counted;
    /// - no retired object is referenced by an index entry.
    pub fn check_invariants(&self) -> Vec<String> {
        let mut violations = Vec::new();
        let mut wal_entries: BTreeMap<&str, u32> = BTreeMap::new();
        let mut referenced: BTreeMap<&str, ()> = BTreeMap::new();
        for ((stream, partition), state) in &self.partitions {
            let at = format!("stream {stream} partition {partition}");
            let mut expected: Option<u64> = None;
            let mut bytes: u64 = 0;
            for (base, entry) in &state.index {
                if *base != entry.base_offset {
                    violations.push(format!(
                        "{at}: entry keyed {base} has base {}",
                        entry.base_offset
                    ));
                }
                if let Some(expected) = expected
                    && entry.base_offset != expected
                {
                    violations.push(format!(
                        "{at}: gap or overlap at {expected}..{}",
                        entry.base_offset
                    ));
                }
                if entry.records == 0 {
                    violations.push(format!("{at}: empty entry at {base}"));
                }
                expected = Some(entry.end_offset());
                bytes += entry.byte_range.end.saturating_sub(entry.byte_range.start);
                referenced.insert(entry.object.as_str(), ());
                if entry.kind == EntryKind::Wal {
                    *wal_entries.entry(entry.object.as_str()).or_default() += 1;
                }
            }
            match (state.index.first_key_value(), expected) {
                (Some((_, first)), Some(end)) => {
                    if end != state.next_offset {
                        violations.push(format!(
                            "{at}: entries end at {end}, next offset is {}",
                            state.next_offset
                        ));
                    }
                    if !(first.base_offset <= state.log_start_offset
                        && state.log_start_offset < first.end_offset())
                    {
                        violations.push(format!(
                            "{at}: log start {} is outside the first entry {}..{}",
                            state.log_start_offset,
                            first.base_offset,
                            first.end_offset()
                        ));
                    }
                }
                _ => {
                    if state.log_start_offset != state.next_offset {
                        violations.push(format!(
                            "{at}: empty index, but log start {} != next offset {}",
                            state.log_start_offset, state.next_offset
                        ));
                    }
                }
            }
            if bytes != state.bytes {
                violations.push(format!(
                    "{at}: byte count {} != {bytes} computed from the entries",
                    state.bytes
                ));
            }
        }
        for (object, live) in &self.wal_live_chunks {
            let entries = wal_entries.get(object.as_str()).copied().unwrap_or(0);
            if *live != entries {
                violations.push(format!(
                    "WAL object {object}: {live} live chunks counted, {entries} entries"
                ));
            }
        }
        for object in wal_entries.keys() {
            if !self.wal_live_chunks.contains_key(*object) {
                violations.push(format!("WAL object {object} has entries but no live count"));
            }
        }
        for object in self.retired.keys() {
            if referenced.contains_key(object.as_str()) {
                violations.push(format!("retired object {object} is still referenced"));
            }
        }
        violations
    }
}
