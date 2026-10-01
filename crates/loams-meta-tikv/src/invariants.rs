//! The metastore's state invariants, checked from one snapshot (R1 plan
//! Task 6): the TiKV counterpart of the openraft state machine's
//! `check_invariants`, for the fault matrix and the crash gate over TiKV.

use std::collections::{BTreeMap, BTreeSet};

use loams_common::StreamId;
use loams_common::meta::{Collection, EntryKind, IndexEntry, MetaResult, Stream};

use crate::catalog::{list_collections, list_streams};
use crate::keys::{self, Head};
use crate::{Reader, TikvMeta, decode_all, fatal};

/// What one snapshot holds of the log and the catalog.
struct State {
    streams: BTreeMap<StreamId, Stream>,
    collections: Vec<Collection>,
    /// `(stream, partition)` → head.
    heads: BTreeMap<(StreamId, u32), Head>,
    /// `(stream, partition)` → base offset (from the key) → entry.
    entries: BTreeMap<(StreamId, u32), BTreeMap<u64, IndexEntry>>,
    /// WAL object → live chunk count (`W/`).
    wal_live: BTreeMap<String, u32>,
    retired: BTreeSet<String>,
    /// Keys under `h/`, `i/` or `W/` that do not parse.
    malformed: Vec<String>,
}

impl TikvMeta {
    /// Checks the invariants every sequence of writes must keep, from one
    /// snapshot, and returns every violation found (empty if none):
    /// - every head and index entry belongs to an existing stream and one of
    ///   its partitions;
    /// - each partition's index entries tile `[first base, next)` without
    ///   gaps, the log start lies in the first entry (or equals `next` when
    ///   the index is empty), and the head's byte count equals the sum of the
    ///   entries' byte ranges;
    /// - every WAL object's live count (`W/`) equals its number of WAL
    ///   entries, and every WAL entry's object is counted;
    /// - no retired object is referenced by an index entry;
    /// - every collection's implicit stream exists with its partition count.
    ///
    /// It scans the whole root, so it is meant for tests and gates.
    pub async fn check_invariants(&self) -> MetaResult<Vec<String>> {
        let state = self
            .read(|snap| {
                Box::pin(async move {
                    let state = load(snap).await?;
                    Ok(Ok(state))
                })
            })
            .await?;
        Ok(check(&state))
    }
}

async fn load(r: &mut dyn Reader) -> Result<State, loams_tikv::TxnError> {
    let streams = list_streams(r, None)
        .await?
        .into_iter()
        .map(|s| (s.id, s))
        .collect();
    let collections = list_collections(r, None).await?;
    let mut malformed = Vec::new();
    let mut heads = BTreeMap::new();
    for (key, value) in r.scan_prefix(b"h/").await? {
        match parse_head_key(&key) {
            Some(at) => {
                heads.insert(at, keys::decode("partition head", &value).map_err(fatal)?);
            }
            None => malformed.push(escape(&key)),
        }
    }
    let mut entries: BTreeMap<(StreamId, u32), BTreeMap<u64, IndexEntry>> = BTreeMap::new();
    let pairs = r.scan_prefix(b"i/").await?;
    let decoded: Vec<IndexEntry> = decode_all("index entry", &pairs)?;
    for ((key, _), entry) in pairs.iter().zip(decoded) {
        match parse_entry_key(key) {
            Some((stream, partition, base)) => {
                entries
                    .entry((stream, partition))
                    .or_default()
                    .insert(base, entry);
            }
            None => malformed.push(escape(key)),
        }
    }
    let mut wal_live = BTreeMap::new();
    for (key, value) in r.scan_prefix(b"W/").await? {
        match parse_wal_live_key(&key) {
            Some(object) => {
                let count = keys::decode_u32("WAL live count", &value).map_err(fatal)?;
                wal_live.insert(object, count);
            }
            None => malformed.push(escape(&key)),
        }
    }
    let retired = r
        .scan_prefix(keys::RETIRED)
        .await?
        .iter()
        .filter_map(|(key, _)| keys::retired_path(key))
        .collect();
    Ok(State {
        streams,
        collections,
        heads,
        entries,
        wal_live,
        retired,
        malformed,
    })
}

fn check(state: &State) -> Vec<String> {
    let mut violations: Vec<String> = state
        .malformed
        .iter()
        .map(|key| format!("unparseable key {key}"))
        .collect();
    let partition_ok = |stream: StreamId, partition: u32| {
        state
            .streams
            .get(&stream)
            .is_some_and(|s| partition < s.partitions)
    };
    for &(stream, partition) in state.heads.keys() {
        if !partition_ok(stream, partition) {
            violations.push(format!(
                "head of stream {stream} partition {partition}, which does not exist"
            ));
        }
    }
    let mut wal_entries: BTreeMap<&str, u32> = BTreeMap::new();
    let mut referenced: BTreeSet<&str> = BTreeSet::new();
    let partitions: BTreeSet<(StreamId, u32)> = state
        .heads
        .keys()
        .chain(state.entries.keys())
        .copied()
        .collect();
    for (stream, partition) in partitions {
        let at = format!("stream {stream} partition {partition}");
        let head = state.heads.get(&(stream, partition));
        let index = state.entries.get(&(stream, partition));
        if index.is_some() && !partition_ok(stream, partition) {
            violations.push(format!("{at}: index entries of a missing partition"));
        }
        // An absent head is an empty partition (row T4-5).
        let head = head.copied().unwrap_or_default();
        let mut expected: Option<u64> = None;
        let mut bytes = 0u64;
        for (base, entry) in index.into_iter().flatten() {
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
            referenced.insert(entry.object.as_str());
            if entry.kind == EntryKind::Wal {
                *wal_entries.entry(entry.object.as_str()).or_default() += 1;
            }
        }
        match (index.and_then(|i| i.first_key_value()), expected) {
            (Some((_, first)), Some(end)) => {
                if end != head.next {
                    violations.push(format!(
                        "{at}: entries end at {end}, next offset is {}",
                        head.next
                    ));
                }
                if !(first.base_offset <= head.log_start && head.log_start < first.end_offset()) {
                    violations.push(format!(
                        "{at}: log start {} is outside the first entry {}..{}",
                        head.log_start,
                        first.base_offset,
                        first.end_offset()
                    ));
                }
            }
            _ => {
                if head.log_start != head.next {
                    violations.push(format!(
                        "{at}: empty index, but log start {} != next offset {}",
                        head.log_start, head.next
                    ));
                }
            }
        }
        if bytes != head.bytes {
            violations.push(format!(
                "{at}: byte count {} != {bytes} computed from the entries",
                head.bytes
            ));
        }
    }
    for (object, live) in &state.wal_live {
        let entries = wal_entries.get(object.as_str()).copied().unwrap_or(0);
        if *live != entries {
            violations.push(format!(
                "WAL object {object}: {live} live chunks counted, {entries} entries"
            ));
        }
    }
    for object in wal_entries.keys() {
        if !state.wal_live.contains_key(*object) {
            violations.push(format!("WAL object {object} has entries but no live count"));
        }
    }
    for object in &state.retired {
        if referenced.contains(object.as_str()) {
            violations.push(format!("retired object {object} is still referenced"));
        }
    }
    for c in &state.collections {
        check_collection(state, c, &mut violations);
    }
    violations
}

fn check_collection(state: &State, c: &Collection, violations: &mut Vec<String>) {
    let at = format!("collection {} ({}/{})", c.id, c.namespace, c.name);
    match state.streams.get(&c.stream) {
        None => violations.push(format!("{at}: implicit stream {} is missing", c.stream)),
        Some(s) if s.partitions != c.partitions || s.namespace != c.namespace => {
            violations.push(format!(
                "{at}: implicit stream {} has {} partitions in namespace {}",
                s.id, s.partitions, s.namespace
            ));
        }
        Some(_) => {}
    }
}

/// `h/<stream BE8>/<partition BE4>`.
fn parse_head_key(key: &[u8]) -> Option<(StreamId, u32)> {
    let rest = key.strip_prefix(b"h/")?;
    if rest.len() != 13 || rest[8] != b'/' {
        return None;
    }
    let stream = u64::from_be_bytes(rest[..8].try_into().ok()?);
    let partition = u32::from_be_bytes(rest[9..].try_into().ok()?);
    Some((StreamId(stream), partition))
}

/// `i/<stream BE8>/<partition BE4>/<base BE8>`.
fn parse_entry_key(key: &[u8]) -> Option<(StreamId, u32, u64)> {
    let rest = key.strip_prefix(b"i/")?;
    if rest.len() != 22 || rest[8] != b'/' || rest[13] != b'/' {
        return None;
    }
    let stream = u64::from_be_bytes(rest[..8].try_into().ok()?);
    let partition = u32::from_be_bytes(rest[9..13].try_into().ok()?);
    let base = u64::from_be_bytes(rest[14..].try_into().ok()?);
    Some((StreamId(stream), partition, base))
}

/// `W/<hash8>/<object>`.
fn parse_wal_live_key(key: &[u8]) -> Option<String> {
    let rest = key.strip_prefix(b"W/")?;
    if rest.len() < 9 || rest[8] != b'/' {
        return None;
    }
    let object = String::from_utf8(rest[9..].to_vec()).ok()?;
    (keys::wal_live(&object) == key).then_some(object)
}

fn escape(key: &[u8]) -> String {
    key.escape_ascii().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_parse_back() {
        let s = StreamId(0x2f2f);
        assert_eq!(parse_head_key(&keys::head(s, 7)), Some((s, 7)));
        assert_eq!(
            parse_entry_key(&keys::index_entry(s, 3, 1 << 40)),
            Some((s, 3, 1 << 40))
        );
        assert_eq!(
            parse_wal_live_key(&keys::wal_live("ns/1/wal/x")).as_deref(),
            Some("ns/1/wal/x")
        );
        assert_eq!(parse_head_key(b"h/short"), None);
        assert_eq!(parse_entry_key(&keys::head(s, 0)), None);
    }

    fn entry(base: u64, records: u32, object: &str) -> IndexEntry {
        IndexEntry {
            kind: EntryKind::Wal,
            base_offset: base,
            records,
            object: object.to_string(),
            byte_range: 0..10,
            max_timestamp_ms: 0,
        }
    }

    fn state_with(entries: Vec<IndexEntry>, head: Head, live: u32) -> State {
        let s = StreamId(1);
        let stream = Stream {
            id: s,
            namespace: loams_common::NamespaceId(1),
            name: "events".to_string(),
            partitions: 1,
            class: loams_common::meta::WalClass::Standard,
            retention: loams_common::meta::Retention::default(),
        };
        let mut index = BTreeMap::new();
        for e in entries {
            index.insert(e.base_offset, e);
        }
        State {
            streams: BTreeMap::from([(s, stream)]),
            collections: Vec::new(),
            heads: BTreeMap::from([((s, 0), head)]),
            entries: BTreeMap::from([((s, 0), index)]),
            wal_live: BTreeMap::from([("w".to_string(), live)]),
            retired: BTreeSet::new(),
            malformed: Vec::new(),
        }
    }

    #[test]
    fn a_consistent_partition_passes_and_each_break_is_named() {
        let head = Head {
            next: 5,
            log_start: 0,
            bytes: 20,
        };
        let ok = state_with(vec![entry(0, 2, "w"), entry(2, 3, "w")], head, 2);
        assert_eq!(check(&ok), Vec::<String>::new());

        let gap = state_with(vec![entry(0, 2, "w"), entry(3, 2, "w")], head, 2);
        assert!(check(&gap).iter().any(|v| v.contains("gap or overlap")));

        let counted = state_with(vec![entry(0, 2, "w"), entry(2, 3, "w")], head, 1);
        assert!(check(&counted).iter().any(|v| v.contains("live chunks")));

        let mut retired = ok;
        retired.retired.insert("w".to_string());
        assert!(check(&retired).iter().any(|v| v.contains("retired")));

        let behind = state_with(
            vec![entry(0, 2, "w")],
            Head {
                next: 3,
                log_start: 0,
                bytes: 10,
            },
            1,
        );
        assert!(
            check(&behind)
                .iter()
                .any(|v| v.contains("entries end at 2"))
        );
    }
}
