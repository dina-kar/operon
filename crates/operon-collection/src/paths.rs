//! Where a collection's objects live (M1 overview §6.4, A18):
//!
//! ```text
//! ns/<ns>/collections/<cid>/
//!   lance/…                                   the Lance dataset root
//!   text/splits/<ulid>.split                  Tantivy split bundles
//!   text/deletes/<split_ulid>/<ulid>.bitmap   whole delete bitmaps
//!   manifests/<version:020>-<ulid>.pb         collection manifests
//!   pkdelta/<version:020>-<ulid>.pkd          PK deltas (Ruling 7)
//!   deadletters/<version:020>-<ulid>.dlq      dead letters (Ruling 11)
//!   hot/hnsw/<column>/<source_version:020>-<ulid>/…   derived HNSW artifacts (M1.3)
//! ```
//!
//! Every object is named with a ULID whose time is its creation time
//! (`Ulid::from_parts(meta.now_ms(), Ulid::generate().random())`), so GC
//! reads its age from its name (`operon_log::gc::object_time_ms`).

use operon_common::{CollectionId, NamespaceId};
use operon_meta::collection_prefix;
use ulid::Ulid;

/// `ns/<ns>/collections/<cid>/manifests/<version:020>-<ulid>.pb`.
pub fn manifest_path(ns: NamespaceId, cid: CollectionId, version: u64, ulid: Ulid) -> String {
    format!(
        "{}manifests/{version:020}-{ulid}.pb",
        collection_prefix(ns, cid)
    )
}

/// `ns/<ns>/collections/<cid>/text/splits/<ulid>.split`.
pub fn split_path(ns: NamespaceId, cid: CollectionId, ulid: Ulid) -> String {
    format!("{}text/splits/{ulid}.split", collection_prefix(ns, cid))
}

/// `ns/<ns>/collections/<cid>/text/deletes/<split_ulid>/<ulid>.bitmap`.
pub fn delete_bitmap_path(ns: NamespaceId, cid: CollectionId, split: Ulid, ulid: Ulid) -> String {
    format!(
        "{}text/deletes/{split}/{ulid}.bitmap",
        collection_prefix(ns, cid)
    )
}

/// `ns/<ns>/collections/<cid>/pkdelta/<version:020>-<ulid>.pkd`.
pub fn pk_delta_path(ns: NamespaceId, cid: CollectionId, version: u64, ulid: Ulid) -> String {
    format!(
        "{}pkdelta/{version:020}-{ulid}.pkd",
        collection_prefix(ns, cid)
    )
}

/// `ns/<ns>/collections/<cid>/deadletters/<version:020>-<ulid>.dlq`.
pub fn dead_letters_path(ns: NamespaceId, cid: CollectionId, version: u64, ulid: Ulid) -> String {
    format!(
        "{}deadletters/{version:020}-{ulid}.dlq",
        collection_prefix(ns, cid)
    )
}

/// `ns/<ns>/collections/<cid>/lance/`.
pub fn lance_prefix(ns: NamespaceId, cid: CollectionId) -> String {
    format!("{}lance/", collection_prefix(ns, cid))
}

/// The version in a manifest path's file name `<version:020>-<ulid>.pb`;
/// `None` for any other name.
pub fn manifest_version(path: &str) -> Option<u64> {
    let name = path.rsplit('/').next()?;
    let (version, rest) = name.strip_suffix(".pb")?.split_once('-')?;
    if version.len() != 20 || !version.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Ulid::from_string(rest).ok()?;
    version.parse().ok()
}
