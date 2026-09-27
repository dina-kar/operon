//! The metastore's keys and record encodings (design §20 §11.2, row R17).
//!
//! Every key lives under the handle's root prefix (added by `operon-tikv`).
//! A key is a one-letter tag, `/`, then its fields: ids as 8-byte big-endian
//! integers (so id order is key order), partitions as 4-byte big-endian
//! integers, names and paths as their bytes, last. Two deviations from
//! §11.2, both row R17: namespaces are `n/<name>` (the trait's
//! `create_namespace` takes no org), and the hot configuration is
//! `H/<collection>`.
//!
//! Records are a format byte ([`FORMAT`]) followed by the record's postcard
//! encoding; counters and stamps are 8-byte big-endian integers.

use operon_common::meta::LinkId;
use operon_common::meta::MetaError;
use operon_common::{CollectionId, NamespaceId, StreamId};
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
    let end = operon_tikv::tuple::successor(prefix);
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
// Written by Task 5's `commit_wal`; Task 4 only scans heads by prefix.
#[cfg_attr(not(test), allow(dead_code))]
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
/// `i/<stream>/<partition>/<base offset>` → an [`IndexEntry`](operon_common::meta::IndexEntry).
pub(crate) fn index_entries(stream: StreamId) -> Vec<u8> {
    tagged(b'i', &[&stream.0.to_be_bytes(), b""])
}

/// `W/<hash8>/<object>` → how many of a WAL object's chunks are still WAL
/// index entries (u32 big-endian).
pub(crate) fn wal_live(object: &str) -> Vec<u8> {
    tagged(b'W', &[&hash8(object), object.as_bytes()])
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

/// A namespace's aliases: alias name → the collection it points at, and a
/// version bumped by every change.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct AliasMap {
    pub version: u64,
    pub aliases: std::collections::BTreeMap<String, CollectionId>,
}

/// A partition head: the next offset, the log start, and the bytes its index
/// entries cover.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Head {
    pub next: u64,
    pub log_start: u64,
    pub bytes: u64,
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
