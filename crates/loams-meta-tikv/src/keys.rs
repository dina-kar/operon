//! The metastore's keys and record encodings (design §20 §11.2, row R17).
//!
//! Every key lives under the handle's root prefix (added by `loams-tikv`).
//! A key is a one-letter tag, `/`, then its fields: ids as 8-byte big-endian
//! integers (so id order is key order), partitions as 4-byte big-endian
//! integers, names and paths as their bytes, last. Two deviations from
//! §11.2, both row R17: namespaces are `n/<name>` (the trait's
//! `create_namespace` takes no org), and the hot configuration is
//! `H/<collection>`.
//!
//! Records are a format byte ([`FORMAT`]) followed by the record's postcard
//! encoding; counters and stamps are 8-byte big-endian integers.

use loams_common::meta::AliasTargets;
use loams_common::meta::LinkId;
use loams_common::meta::MetaError;
use loams_common::{CollectionId, NamespaceId, StreamId};
use serde::Serialize;
use serde::de::DeserializeOwned;
use xxhash_rust::xxh3::xxh3_64;

/// The format byte in front of every postcard record.
pub(crate) const FORMAT: u8 = 1;

/// The retired set is spread over this many shards (`r/<shard>/<path>`).
pub(crate) const RETIRED_SHARDS: u64 = 64;

/// The lease scope of the metastore's own leases (`e/m/<key>`); the GC loop's
/// lease is `e/cluster/gc` (Task 3), so the two never meet.
const LEASE_SCOPE: &[u8] = b"e/m/";

fn tagged(tag: u8, parts: &[&[u8]]) -> Vec<u8> {
    let len = 2 + parts.iter().map(|p| p.len() + 1).sum::<usize>();
    let mut key = Vec::with_capacity(len);
    key.push(tag);
    key.push(b'/');
    for (i, part) in parts.iter().enumerate() {
        if i > 0 {
            key.push(b'/');
        }
        key.extend_from_slice(part);
    }
    key
}

/// The first 8 bytes of a path's xxh3 hash, big-endian: spreads time-ordered
/// names (ULIDs) over regions.
pub(crate) fn hash8(path: &str) -> [u8; 8] {
    xxh3_64(path.as_bytes()).to_be_bytes()
}

/// Every key starting with `prefix`: `(prefix, Some(end))`, or `None` for the
/// end when no bound exists.
pub(crate) fn prefix_range(prefix: &[u8]) -> (Vec<u8>, Option<Vec<u8>>) {
    let end = loams_tikv::tuple::successor(prefix);
    (prefix.to_vec(), (!end.is_empty()).then_some(end))
}

// ---- The id blocks ----

/// What an id block allocates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum IdKind {
    Namespace,
    Stream,
    Link,
    Collection,
}

impl IdKind {
    fn name(self) -> &'static [u8] {
        match self {
            IdKind::Namespace => b"namespace",
            IdKind::Stream => b"stream",
            IdKind::Link => b"link",
            IdKind::Collection => b"collection",
        }
    }
}

/// `c/<kind>`: the next unallocated id of that kind.
pub(crate) fn id_block(kind: IdKind) -> Vec<u8> {
    tagged(b'c', &[kind.name()])
}

// ---- Namespaces ----

/// `n/<name>` → the namespace id (row R17: no org segment in R1).
pub(crate) fn namespace_name(name: &str) -> Vec<u8> {
    tagged(b'n', &[name.as_bytes()])
}

/// `N/<id>` → the namespace record.
pub(crate) fn namespace(id: NamespaceId) -> Vec<u8> {
    tagged(b'N', &[&id.0.to_be_bytes()])
}

/// The prefix of every namespace record.
pub(crate) const NAMESPACES: &[u8] = b"N/";

// ---- Streams, links, collections ----

/// `s/<ns>/<name>` → the stream id.
pub(crate) fn stream_name(ns: NamespaceId, name: &str) -> Vec<u8> {
    tagged(b's', &[&ns.0.to_be_bytes(), name.as_bytes()])
}

/// The prefix of the stream names of `ns`.
pub(crate) fn stream_names(ns: NamespaceId) -> Vec<u8> {
    tagged(b's', &[&ns.0.to_be_bytes(), b""])
}

/// `S/<id>` → the stream record.
pub(crate) fn stream(id: StreamId) -> Vec<u8> {
    tagged(b'S', &[&id.0.to_be_bytes()])
}

/// The prefix of every stream record.
pub(crate) const STREAMS: &[u8] = b"S/";

/// `l/<ns>/<name>` → the link id.
pub(crate) fn link_name(ns: NamespaceId, name: &str) -> Vec<u8> {
    tagged(b'l', &[&ns.0.to_be_bytes(), name.as_bytes()])
}

/// The prefix of the link names of `ns`.
pub(crate) fn link_names(ns: NamespaceId) -> Vec<u8> {
    tagged(b'l', &[&ns.0.to_be_bytes(), b""])
}

/// `L/<id>` → the link record.
pub(crate) fn link(id: LinkId) -> Vec<u8> {
    tagged(b'L', &[&id.0.to_be_bytes()])
}

/// The prefix of every link record.
pub(crate) const LINKS: &[u8] = b"L/";

/// `k/<ns>/<name>` → the collection id.
pub(crate) fn collection_name(ns: NamespaceId, name: &str) -> Vec<u8> {
    tagged(b'k', &[&ns.0.to_be_bytes(), name.as_bytes()])
}

/// The prefix of the collection names of `ns`.
pub(crate) fn collection_names(ns: NamespaceId) -> Vec<u8> {
    tagged(b'k', &[&ns.0.to_be_bytes(), b""])
}

/// `K/<id>` → the collection record. Present while the collection is live:
/// `drop_collection` deletes it.
pub(crate) fn collection(id: CollectionId) -> Vec<u8> {
    tagged(b'K', &[&id.0.to_be_bytes()])
}

/// The prefix of every collection record.
pub(crate) const COLLECTIONS: &[u8] = b"K/";

/// `a/<ns>` → the namespace's alias map ([`AliasMap`]).
pub(crate) fn aliases(ns: NamespaceId) -> Vec<u8> {
    tagged(b'a', &[&ns.0.to_be_bytes()])
}

/// `H/<collection>` → the collection's hot configuration, only while it is
/// not all false (row R17).
pub(crate) fn hot(id: CollectionId) -> Vec<u8> {
    tagged(b'H', &[&id.0.to_be_bytes()])
}

// ---- The log (written by Task 5; read and removed by drop_collection) ----

/// `h/<stream>/<partition>` → the partition head ([`Head`]). Written by the
/// first commit into the partition: an absent head of an existing partition
/// is an empty one.
pub(crate) fn head(stream: StreamId, partition: u32) -> Vec<u8> {
    tagged(b'h', &[&stream.0.to_be_bytes(), &partition.to_be_bytes()])
}

/// The prefix of every head of `stream`.
pub(crate) fn heads(stream: StreamId) -> Vec<u8> {
    tagged(b'h', &[&stream.0.to_be_bytes(), b""])
}

/// The partition of a head key under [`heads`]`(stream)`.
pub(crate) fn head_partition(key: &[u8], stream: StreamId) -> Option<u32> {
    let rest = key.strip_prefix(heads(stream).as_slice())?;
    Some(u32::from_be_bytes(rest.try_into().ok()?))
}

/// The prefix of every index entry of `stream` (all partitions):
/// `i/<stream>/<partition>/<base offset>` → an [`IndexEntry`](loams_common::meta::IndexEntry).
pub(crate) fn index_entries(stream: StreamId) -> Vec<u8> {
    tagged(b'i', &[&stream.0.to_be_bytes(), b""])
}

/// The prefix of the index entries of one partition.
pub(crate) fn partition_entries(stream: StreamId, partition: u32) -> Vec<u8> {
    tagged(
        b'i',
        &[&stream.0.to_be_bytes(), &partition.to_be_bytes(), b""],
    )
}

/// `i/<stream>/<partition>/<base offset>` → the index entry starting at
/// `base` (base offsets are 8-byte big-endian, so key order is offset order).
pub(crate) fn index_entry(stream: StreamId, partition: u32, base: u64) -> Vec<u8> {
    tagged(
        b'i',
        &[
            &stream.0.to_be_bytes(),
            &partition.to_be_bytes(),
            &base.to_be_bytes(),
        ],
    )
}

/// The prefix of every WAL commit record (`w/`).
pub(crate) const WAL_RECORDS: &[u8] = b"w/";

/// The prefix of the commit records of one WAL object: `w/<hash8>/<object>/`.
/// A key under it is one of the object's records only when exactly the
/// 4-byte group number follows ([`wal_group_of`]): another object's name may
/// extend this one's.
pub(crate) fn wal_groups(object: &str) -> Vec<u8> {
    tagged(b'w', &[&hash8(object), object.as_bytes(), b""])
}

/// `w/<hash8>/<object>/<group>` → the commit record of one partition group
/// of a WAL object ([`WalGroup`]); groups are 4-byte big-endian.
pub(crate) fn wal_group(object: &str, group: u32) -> Vec<u8> {
    tagged(
        b'w',
        &[&hash8(object), object.as_bytes(), &group.to_be_bytes()],
    )
}

/// The group number of a key under [`wal_groups`]`(object)`, if the key is
/// one of that object's records.
pub(crate) fn wal_group_of(key: &[u8], object: &str) -> Option<u32> {
    let rest = key.strip_prefix(wal_groups(object).as_slice())?;
    Some(u32::from_be_bytes(rest.try_into().ok()?))
}

/// `W/<hash8>/<object>` → how many of a WAL object's chunks are still WAL
/// index entries (u32 big-endian).
pub(crate) fn wal_live(object: &str) -> Vec<u8> {
    tagged(b'W', &[&hash8(object), object.as_bytes()])
}

/// The prefix of the whole retired set (all shards).
pub(crate) const RETIRED: &[u8] = b"r/";

/// The path of a key under [`RETIRED`]: `r/<shard>/<path>`.
pub(crate) fn retired_path(key: &[u8]) -> Option<String> {
    let rest = key.strip_prefix(RETIRED)?;
    let path = rest.get(2..).filter(|_| rest.get(1) == Some(&b'/'))?;
    String::from_utf8(path.to_vec()).ok()
}

/// `r/<shard>/<path>` → when the path was retired (ms, u64 big-endian).
pub(crate) fn retired(path: &str) -> Vec<u8> {
    let shard = u8::try_from(xxh3_64(path.as_bytes()) % RETIRED_SHARDS).unwrap_or(0);
    tagged(b'r', &[&[shard], path.as_bytes()])
}

/// `o/<hash8>/<path>` → the object's reference record ([`ObjectRef`]).
pub(crate) fn object_ref(path: &str) -> Vec<u8> {
    tagged(b'o', &[&hash8(path), path.as_bytes()])
}

// ---- Leases and pointers ----

/// `e/m/<key>` → the lease record.
pub(crate) fn lease(key: &str) -> Vec<u8> {
    let mut k = LEASE_SCOPE.to_vec();
    k.extend_from_slice(key.as_bytes());
    k
}

/// The lease key of a key under [`lease`]`("")`.
pub(crate) fn lease_key(key: &[u8]) -> Option<String> {
    let rest = key.strip_prefix(LEASE_SCOPE)?;
    String::from_utf8(rest.to_vec()).ok()
}

/// `p/<ns>/<key>` → the pointer record.
pub(crate) fn pointer(ns: NamespaceId, key: &str) -> Vec<u8> {
    tagged(b'p', &[&ns.0.to_be_bytes(), key.as_bytes()])
}

// ---- Records ----

/// A namespace's aliases, and a version bumped by every change. As in the
/// openraft state machine (M1.5 Task 0a rule 1), an alias is in exactly one
/// map: `aliases` when it is one member with no `is_write_index` setting,
/// `targets` otherwise.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct AliasMap {
    pub version: u64,
    /// Alias name → the one collection it points at.
    pub aliases: std::collections::BTreeMap<String, CollectionId>,
    /// Alias name → its members, for every other alias.
    pub targets: std::collections::BTreeMap<String, AliasTargets>,
}

impl AliasMap {
    /// Whether `name` is an alias of either map.
    pub fn contains(&self, name: &str) -> bool {
        self.aliases.contains_key(name) || self.targets.contains_key(name)
    }

    /// The members of `alias` from either map (a single-target alias is one
    /// unset member); empty when there is no such alias.
    pub fn members(&self, alias: &str) -> std::collections::BTreeMap<CollectionId, Option<bool>> {
        if let Some(&id) = self.aliases.get(alias) {
            return std::collections::BTreeMap::from([(id, None)]);
        }
        self.targets
            .get(alias)
            .map(|t| t.members.clone())
            .unwrap_or_default()
    }

    /// Puts `alias` in its canonical map: none when `members` is empty,
    /// `aliases` when it is exactly one unset member, `targets` otherwise.
    pub fn put(
        &mut self,
        alias: String,
        members: std::collections::BTreeMap<CollectionId, Option<bool>>,
    ) {
        self.aliases.remove(&alias);
        self.targets.remove(&alias);
        let mut iter = members.iter();
        match (iter.next(), iter.next()) {
            (None, _) => {}
            (Some((&id, None)), None) => {
                self.aliases.insert(alias, id);
            }
            _ => {
                self.targets.insert(alias, AliasTargets { members });
            }
        }
    }

    /// Every alias with its members, by alias name.
    pub fn all_targets(&self) -> Vec<(String, AliasTargets)> {
        let mut out: Vec<(String, AliasTargets)> = self
            .aliases
            .iter()
            .map(|(alias, &id)| {
                (
                    alias.clone(),
                    AliasTargets {
                        members: std::collections::BTreeMap::from([(id, None)]),
                    },
                )
            })
            .chain(self.targets.iter().map(|(a, t)| (a.clone(), t.clone())))
            .collect();
        out.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        out
    }
}

/// A partition head: the next offset, the log start, and the bytes its index
/// entries cover.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Head {
    pub next: u64,
    pub log_start: u64,
    pub bytes: u64,
}

/// The commit record of one partition group of a WAL object (design §20
/// §11.3): how many groups the call had, the object's creation time (for
/// pruning), and the base offset of each of the group's chunks, keyed by the
/// chunk's position in the call, so a retry of a multi-group call can
/// return every chunk's offset in call order.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct WalGroup {
    pub groups: u32,
    pub created_at_ms: u64,
    pub offsets: Vec<(u32, u64)>,
}

/// An object's reference record: how many index entries or pointers name it,
/// and whether garbage collection has claimed it for deletion. A claim and a
/// new reference write the same row, so they conflict (§20 §11.3).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ObjectRef {
    pub refs: u64,
    pub gc_claim: bool,
}

fn corrupt(what: &str, detail: impl std::fmt::Display) -> MetaError {
    MetaError::Storage(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("TiKV metastore: unreadable {what} record: {detail}"),
    ))
}

/// `FORMAT ‖ postcard(value)`.
pub(crate) fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    let out = vec![FORMAT];
    // Encoding owned in-memory records cannot fail: postcard fails only on
    // unsupported serde features, which these types do not use.
    match postcard::to_extend(value, out) {
        Ok(out) => out,
        Err(e) => unreachable!("postcard refused a metastore record: {e}"),
    }
}

/// Decodes a record written by [`encode`].
pub(crate) fn decode<T: DeserializeOwned>(what: &str, bytes: &[u8]) -> Result<T, MetaError> {
    match bytes.split_first() {
        Some((&FORMAT, body)) => postcard::from_bytes(body).map_err(|e| corrupt(what, e)),
        Some((format, _)) => Err(corrupt(what, format!("unknown format {format}"))),
        None => Err(corrupt(what, "empty value")),
    }
}

/// An 8-byte big-endian integer.
pub(crate) fn encode_u64(n: u64) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}

/// Reads an 8-byte big-endian integer.
pub(crate) fn decode_u64(what: &str, bytes: &[u8]) -> Result<u64, MetaError> {
    <[u8; 8]>::try_from(bytes)
        .map(u64::from_be_bytes)
        .map_err(|_| corrupt(what, format!("{} bytes, not 8", bytes.len())))
}

/// Reads a 4-byte big-endian integer.
pub(crate) fn decode_u32(what: &str, bytes: &[u8]) -> Result<u32, MetaError> {
    <[u8; 4]>::try_from(bytes)
        .map(u32::from_be_bytes)
        .map_err(|_| corrupt(what, format!("{} bytes, not 4", bytes.len())))
}

// ---- The stream ingest ledger ----

/// `q/<stream>/<key>` → the ledger entry of an idempotency key (design §02
/// §7.4, D270); keys are 32-byte SHA-256 digests.
pub(crate) fn idempotency(stream: StreamId, key: &[u8; 32]) -> Vec<u8> {
    tagged(b'q', &[&stream.0.to_be_bytes(), key])
}

/// The prefix of the ledger entries of `stream`.
pub(crate) fn idempotency_entries(stream: StreamId) -> Vec<u8> {
    tagged(b'q', &[&stream.0.to_be_bytes(), b""])
}

/// The prefix of the whole ledger (`q/`).
pub(crate) const IDEMPOTENCY: &[u8] = b"q/";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_order_is_key_order() {
        let keys: Vec<Vec<u8>> = [1u64, 255, 256, 1 << 40]
            .iter()
            .map(|&id| stream(StreamId(id)))
            .collect();
        assert!(keys.is_sorted());
        assert!(keys.iter().all(|k| k.starts_with(STREAMS)));
    }

    #[test]
    fn names_of_one_namespace_share_a_prefix_no_other_namespace_has() {
        let a = NamespaceId(1);
        let b = NamespaceId(2);
        let prefix = stream_names(a);
        assert!(stream_name(a, "events").starts_with(&prefix));
        assert!(!stream_name(b, "events").starts_with(&prefix));
        // A namespace id whose last byte is `/` (0x2f) still cannot spill
        // into a neighbour: the id is fixed-width.
        let slash = NamespaceId(0x2f);
        assert!(!stream_name(NamespaceId(0x2f00), "x").starts_with(&stream_names(slash)));
    }

    #[test]
    fn head_keys_round_trip_their_partition() {
        let s = StreamId(7);
        for p in [0, 1, 9_999] {
            assert_eq!(head_partition(&head(s, p), s), Some(p));
        }
        assert_eq!(head_partition(&head(StreamId(8), 0), s), None);
    }

    #[test]
    fn index_entry_keys_sort_by_offset_within_their_partition() {
        let s = StreamId(3);
        let keys: Vec<Vec<u8>> = [0u64, 1, 255, 256, 1 << 40]
            .iter()
            .map(|&base| index_entry(s, 2, base))
            .collect();
        assert!(keys.is_sorted());
        assert!(keys.iter().all(|k| k.starts_with(&partition_entries(s, 2))));
        assert!(keys.iter().all(|k| k.starts_with(&index_entries(s))));
        assert!(!index_entry(s, 3, 0).starts_with(&partition_entries(s, 2)));
    }

    #[test]
    fn wal_group_keys_belong_only_to_their_object() {
        let key = wal_group("ns/1/wal/a", 7);
        assert_eq!(wal_group_of(&key, "ns/1/wal/a"), Some(7));
        assert!(key.starts_with(WAL_RECORDS));
        // A longer name sharing the prefix is not one of its groups.
        assert_eq!(
            wal_group_of(&wal_group("ns/1/wal/a/x", 0), "ns/1/wal/a"),
            None
        );
    }

    #[test]
    fn retired_keys_round_trip_their_path() {
        for path in ["a", "ns/1/collections/2/", "x/y.bin"] {
            assert_eq!(retired_path(&retired(path)).as_deref(), Some(path));
        }
        assert_eq!(retired_path(b"r/"), None);
        assert_eq!(retired_path(b"W/x"), None);
    }

    #[test]
    fn lease_keys_round_trip() {
        assert_eq!(lease_key(&lease("node/1")).as_deref(), Some("node/1"));
        assert!(lease("node/1").starts_with(&lease("node/")));
        assert_eq!(lease_key(b"e/cluster/gc"), None);
    }

    #[test]
    fn retired_keys_spread_over_the_shards() {
        let key = retired("ns/1/collections/2/");
        assert_eq!(&key[..2], b"r/");
        assert!(u64::from(key[2]) < RETIRED_SHARDS);
        assert_eq!(&key[3..4], b"/");
        assert!(key.ends_with(b"ns/1/collections/2/"));
    }

    #[test]
    fn records_round_trip_and_refuse_garbage() {
        let head = Head {
            next: 5,
            log_start: 2,
            bytes: 50,
        };
        assert_eq!(
            decode::<Head>("head", &encode(&head)).expect("decode"),
            head
        );
        assert!(decode::<Head>("head", &[]).is_err());
        assert!(decode::<Head>("head", &[9, 1, 2]).is_err());
        assert_eq!(decode_u64("n", &encode_u64(42)).expect("u64"), 42);
        assert!(decode_u64("n", &[1, 2]).is_err());
    }

    #[test]
    fn prefix_range_ends_past_every_extension() {
        let (lo, hi) = prefix_range(b"N/");
        assert_eq!(lo, b"N/");
        assert_eq!(hi.as_deref(), Some(&b"N0"[..]));
    }
}
