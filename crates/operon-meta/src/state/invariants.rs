//! Structural invariants of the state, for tests, the crash gate and the
//! simulation.

use std::collections::{BTreeMap, BTreeSet};

use operon_common::CollectionId;
use operon_common::meta::{
    COLLECTION_KIND, COLLECTION_POINTER_PREFIX, EntryKind, Retention, WalClass, implicit_name,
};

use super::MetaState;

impl MetaState {
    /// Checks the invariants every sequence of commands must keep, and
    /// returns every violation found (empty if none):
    /// - each partition's index entries tile `[first base, next_offset)`
    ///   without gaps, the log start lies in the first entry (or equals
    ///   `next_offset` when the index is empty), and the byte count equals
    ///   the sum of the entries' byte ranges;
    /// - every WAL object's live chunk count equals its number of `Wal`
    ///   entries, and every `Wal` entry's object is counted;
    /// - no retired object is referenced by an index entry;
    /// - the collection catalog is consistent (see `check_collections`).
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
        self.check_collections(&mut violations);
        violations
    }

    /// The collection catalog's invariants:
    /// - every collection's implicit stream exists in its namespace, named
    ///   [`implicit_name`], with its partition count, class `Standard` and
    ///   default retention, and its implicit link exists with that stream as
    ///   source, the same name, and target `collection`/its name;
    /// - `collection_names` and `collections` agree both ways;
    /// - every alias points at a collection of its namespace and no alias
    ///   has a collection's name;
    /// - every stream or link named with a leading `_` is a collection's;
    /// - every `collection/<id>` pointer names a collection of its namespace;
    /// - `last_collection_id` is at least every collection id;
    /// - every `collection_hot` entry is an existing collection's, and none
    ///   is all false.
    fn check_collections(&self, violations: &mut Vec<String>) {
        for (id, hot) in &self.collection_hot {
            if !self.collections.contains_key(id) {
                violations.push(format!("hot configuration of missing collection {id}"));
            }
            if !hot.any() {
                violations.push(format!(
                    "collection {id} has an all-false hot configuration"
                ));
            }
        }
        let mut implicit_streams = BTreeSet::new();
        let mut implicit_links = BTreeSet::new();
        for (id, c) in &self.collections {
            let at = format!("collection {id} ({}/{})", c.namespace, c.name);
            if c.id != *id {
                violations.push(format!("{at}: keyed {id} but has id {}", c.id));
            }
            if self.collection_names.get(&(c.namespace, c.name.clone())) != Some(id) {
                violations.push(format!("{at}: its name does not map to it"));
            }
            let name = implicit_name(&c.name, c.id);
            implicit_streams.insert(c.stream);
            implicit_links.insert(c.link);
            match self.streams.get(&c.stream) {
                None => violations.push(format!("{at}: stream {} is missing", c.stream)),
                Some(s) => {
                    if s.namespace != c.namespace
                        || s.name != name
                        || s.partitions != c.partitions
                        || s.class != WalClass::Standard
                        || s.retention != Retention::default()
                    {
                        violations.push(format!("{at}: stream {} does not match: {s:?}", s.id));
                    }
                }
            }
            match self.links.get(&c.link) {
                None => violations.push(format!("{at}: link {} is missing", c.link)),
                Some(l) => {
                    if l.namespace != c.namespace
                        || l.name != name
                        || l.source != c.stream
                        || l.target.kind != COLLECTION_KIND
                        || l.target.name != c.name
                    {
                        violations.push(format!("{at}: link {} does not match: {l:?}", l.id));
                    }
                }
            }
            if c.id.0 > self.last_collection_id {
                violations.push(format!(
                    "{at}: id above last_collection_id {}",
                    self.last_collection_id
                ));
            }
        }
        for ((ns, name), id) in &self.collection_names {
            match self.collections.get(id) {
                Some(c) if c.namespace == *ns && c.name == *name => {}
                _ => violations.push(format!("collection name {ns}/{name} maps to {id}")),
            }
        }
        for ((ns, alias), id) in &self.aliases {
            if !self.collections.get(id).is_some_and(|c| c.namespace == *ns) {
                violations.push(format!(
                    "alias {ns}/{alias} points at {id}, not a collection of its namespace"
                ));
            }
            if self.collection_names.contains_key(&(*ns, alias.clone())) {
                violations.push(format!("alias {ns}/{alias} has a collection's name"));
            }
        }
        for s in self.streams.values() {
            if s.name.starts_with('_') && !implicit_streams.contains(&s.id) {
                violations.push(format!(
                    "stream {} ({}) belongs to no collection",
                    s.id, s.name
                ));
            }
        }
        for l in self.links.values() {
            if l.name.starts_with('_') && !implicit_links.contains(&l.id) {
                violations.push(format!(
                    "link {} ({}) belongs to no collection",
                    l.id, l.name
                ));
            }
        }
        for (ns, key) in self.pointers.keys() {
            let Some(id) = key.strip_prefix(COLLECTION_POINTER_PREFIX) else {
                continue;
            };
            let live = id
                .parse::<CollectionId>()
                .ok()
                .and_then(|id| self.collections.get(&id))
                .is_some_and(|c| c.namespace == *ns);
            if !live {
                violations.push(format!(
                    "pointer {ns}/{key} names no collection of its namespace"
                ));
            }
        }
    }
}
