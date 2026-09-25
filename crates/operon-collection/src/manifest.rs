//! The collection manifest (M1 overview §6.4, plan M1.1 Task 9, Rulings 8 and
//! 19): what one collection version is made of, as protobuf inside Operon's
//! envelope:
//!
//! ```text
//! 0   4  magic "OPCM"
//! 4   2  format version u16 LE = 1
//! 6   n  the `operon.collection.v1.CollectionManifest` protobuf
//! ..  4  crc32c u32 LE of every preceding byte
//! ```
//!
//! The protobuf types stay private ([`pb`]); callers see the domain types,
//! in which an empty path is `None`.

use std::collections::BTreeMap;
use std::ops::Range;

use bytes::Bytes;
use operon_common::CollectionId;
use prost::Message;
use ulid::Ulid;

use crate::error::CollectionError;

/// The protobuf of `proto/collection_manifest.proto`.
#[allow(clippy::all)]
mod pb {
    include!(concat!(env!("OUT_DIR"), "/operon.collection.v1.rs"));
}

pub const MANIFEST_MAGIC: &[u8; 4] = b"OPCM";
pub const MANIFEST_FORMAT_VERSION: u16 = 1;

/// The bytes before the protobuf body.
const HEADER_LEN: usize = 6;
const CRC_LEN: usize = 4;

/// One version of a collection: the Lance version and the splits it reads,
/// and how far into the implicit stream it has applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CollectionManifest {
    /// Equals the version of the pointer `collection/<cid>` that names it.
    pub version: u64,
    /// 0 for version 1.
    pub parent_version: u64,
    /// The full object path of the parent manifest; `None` for version 1.
    pub parent_manifest: Option<String>,
    pub collection_id: CollectionId,
    /// The collection schema version the newest split was written with.
    pub schema_version: u64,
    /// The writer's metastore-client clock when the commit started.
    pub created_at_ms: u64,
    /// The detached Lance version id (bit 63 set), or 1, or 0 = no dataset
    /// yet (R7, Ruling 1).
    pub lance_version: u64,
    pub splits: Vec<SplitRef>,
    pub vector_indexes: Vec<VectorIndexRef>,
    pub scalar_indexes: Vec<ScalarIndexRef>,
    /// Written by M1.3; empty before.
    pub hot_artifacts: Vec<HotArtifactRef>,
    /// Partition → the next offset to apply (the exactly-once watermark).
    pub applied: BTreeMap<u32, u64>,
    pub live_doc_count: u64,
    pub kind: CommitKind,
    /// The path of this commit's PK delta (Ruling 7).
    pub pk_delta: Option<String>,
    /// The path of this commit's dead letters (Ruling 11).
    pub dead_letters: Option<String>,
    /// Cumulative.
    pub dead_letters_total: u64,
    /// Cumulative offsets trimmed before they could be applied.
    pub skipped_offsets_total: u64,
}

impl CollectionManifest {
    /// The state before the first commit: version 0, no Lance dataset,
    /// nothing applied.
    pub fn empty(collection: CollectionId) -> Self {
        Self {
            version: 0,
            parent_version: 0,
            parent_manifest: None,
            collection_id: collection,
            schema_version: 0,
            created_at_ms: 0,
            lance_version: 0,
            splits: Vec::new(),
            vector_indexes: Vec::new(),
            scalar_indexes: Vec::new(),
            hot_artifacts: Vec::new(),
            applied: BTreeMap::new(),
            live_doc_count: 0,
            kind: CommitKind::LinkApply,
            pk_delta: None,
            dead_letters: None,
            dead_letters_total: 0,
            skipped_offsets_total: 0,
        }
    }
}

/// One Tantivy split of a manifest (Rulings 8 and 9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SplitRef {
    pub ulid: Ulid,
    pub doc_count: u64,
    pub deleted_count: u64,
    pub size_bytes: u64,
    /// The footer (bundle metadata, hotcache and trailer), which ends the split.
    pub footer_range: Range<u64>,
    /// Sorted, disjoint, maximal runs of row ids in doc-id order; their total
    /// length is `doc_count`.
    pub row_id_ranges: Vec<Range<u64>>,
    /// The path of the split's delete bitmap; `None` when no doc is deleted.
    pub delete_bitmap: Option<String>,
    /// The collection schema version the split was written with.
    pub schema_version: u64,
    pub created_at_ms: u64,
    /// 0 for a split written by link apply (M1.3 merges increment it).
    pub merge_ops: u32,
}

/// What kind of commit wrote a manifest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitKind {
    LinkApply,
    IndexBuild,
    Maintenance,
}

/// The Lance index type of a vector index segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VectorIndexKind {
    IvfPq,
    IvfRq,
    IvfHnswSq,
}

/// One Lance vector index segment (Ruling 3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VectorIndexRef {
    /// `_vector_<i>`.
    pub column: String,
    pub lance_index_uuid: String,
    /// The dataset's `next_row_id` when the segment was built.
    pub indexed_row_ids_upto: u64,
    /// `vec_<i>`.
    pub index_name: String,
    /// `VectorSpec.name`.
    pub vector: String,
    pub kind: VectorIndexKind,
}

/// One Lance scalar index (the `_pk` BTREE, Task 11).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScalarIndexRef {
    pub column: String,
    pub lance_index_uuid: String,
    pub indexed_row_ids_upto: u64,
    pub index_name: String,
}

/// A derived hot-tier artifact (M1.3), e.g. an HNSW graph under
/// `hot/hnsw/<column>/<source_version:020>-<ulid>/`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HotArtifactRef {
    pub kind: String,
    pub column: String,
    pub prefix: String,
    pub source_version: u64,
}

/// `manifest` in its envelope.
pub fn encode_manifest(manifest: &CollectionManifest) -> Bytes {
    let body = to_pb(manifest).encode_to_vec();
    let mut out = Vec::with_capacity(HEADER_LEN + body.len() + CRC_LEN);
    out.extend_from_slice(MANIFEST_MAGIC);
    out.extend_from_slice(&MANIFEST_FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&body);
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    Bytes::from(out)
}

fn corrupt(message: impl Into<String>) -> CollectionError {
    CollectionError::Corrupt(format!("collection manifest: {}", message.into()))
}

/// Decodes a manifest. A wrong magic, an unknown format version, a crc
/// mismatch, a malformed body, a split ULID that is not 16 bytes, a footer
/// that ends before it starts, row-id ranges that are unsorted, overlapping
/// (within a split or across splits) or empty or do not add up to the doc
/// count, or an unspecified enum is [`CollectionError::Corrupt`].
pub fn decode_manifest(bytes: &[u8]) -> Result<CollectionManifest, CollectionError> {
    if bytes.len() < HEADER_LEN + CRC_LEN {
        return Err(corrupt(format!("{} bytes is too short", bytes.len())));
    }
    if &bytes[0..4] != MANIFEST_MAGIC {
        return Err(corrupt("wrong magic"));
    }
    let (framed, crc) = bytes.split_at(bytes.len() - CRC_LEN);
    if crc32c::crc32c(framed).to_le_bytes() != crc {
        return Err(corrupt("crc mismatch"));
    }
    let version = u16::from_le_bytes([framed[4], framed[5]]);
    if version != MANIFEST_FORMAT_VERSION {
        return Err(corrupt(format!("unknown format version {version}")));
    }
    let body = pb::CollectionManifest::decode(&framed[HEADER_LEN..])
        .map_err(|err| corrupt(format!("malformed body: {err}")))?;
    let manifest = from_pb(body)?;
    // Two splits claiming one row id is as corrupt as one split doing so.
    RowLocator::new(&manifest.splits)?;
    Ok(manifest)
}

fn some(path: String) -> Option<String> {
    (!path.is_empty()).then_some(path)
}

fn to_pb(m: &CollectionManifest) -> pb::CollectionManifest {
    pb::CollectionManifest {
        version: m.version,
        parent_version: m.parent_version,
        collection_id: m.collection_id.0,
        schema_version: m.schema_version,
        created_at_ms: m.created_at_ms,
        lance_version: m.lance_version,
        splits: m.splits.iter().map(split_to_pb).collect(),
        vector_indexes: m
            .vector_indexes
            .iter()
            .map(|v| pb::VectorIndexRef {
                column: v.column.clone(),
                lance_index_uuid: v.lance_index_uuid.clone(),
                indexed_row_ids_upto: v.indexed_row_ids_upto,
                index_name: v.index_name.clone(),
                vector: v.vector.clone(),
                kind: vector_kind_to_pb(v.kind) as i32,
            })
            .collect(),
        hot_artifacts: m
            .hot_artifacts
            .iter()
            .map(|h| pb::HotArtifactRef {
                kind: h.kind.clone(),
                column: h.column.clone(),
                prefix: h.prefix.clone(),
                source_version: h.source_version,
            })
            .collect(),
        applied: m.applied.iter().map(|(&p, &o)| (p, o)).collect(),
        live_doc_count: m.live_doc_count,
        parent_manifest: m.parent_manifest.clone().unwrap_or_default(),
        kind: commit_kind_to_pb(m.kind) as i32,
        pk_delta: m.pk_delta.clone().unwrap_or_default(),
        dead_letters: m.dead_letters.clone().unwrap_or_default(),
        dead_letters_total: m.dead_letters_total,
        skipped_offsets_total: m.skipped_offsets_total,
        scalar_indexes: m
            .scalar_indexes
            .iter()
            .map(|s| pb::ScalarIndexRef {
                column: s.column.clone(),
                lance_index_uuid: s.lance_index_uuid.clone(),
                indexed_row_ids_upto: s.indexed_row_ids_upto,
                index_name: s.index_name.clone(),
            })
            .collect(),
    }
}

fn split_to_pb(s: &SplitRef) -> pb::SplitRef {
    pb::SplitRef {
        ulid: s.ulid.0.to_be_bytes().to_vec(),
        doc_count: s.doc_count,
        deleted_count: s.deleted_count,
        size_bytes: s.size_bytes,
        footer_start: s.footer_range.start,
        footer_end: s.footer_range.end,
        row_id_ranges: s
            .row_id_ranges
            .iter()
            .map(|r| pb::RowIdRange {
                start: r.start,
                end: r.end,
            })
            .collect(),
        delete_bitmap: s.delete_bitmap.clone().unwrap_or_default(),
        schema_version: s.schema_version,
        created_at_ms: s.created_at_ms,
        merge_ops: s.merge_ops,
    }
}

fn from_pb(m: pb::CollectionManifest) -> Result<CollectionManifest, CollectionError> {
    let kind = match pb::CommitKind::try_from(m.kind) {
        Ok(pb::CommitKind::LinkApply) => CommitKind::LinkApply,
        Ok(pb::CommitKind::IndexBuild) => CommitKind::IndexBuild,
        Ok(pb::CommitKind::Maintenance) => CommitKind::Maintenance,
        Ok(pb::CommitKind::Unspecified) | Err(_) => {
            return Err(corrupt(format!("commit kind {} is not specified", m.kind)));
        }
    };
    let splits = m
        .splits
        .into_iter()
        .map(split_from_pb)
        .collect::<Result<_, _>>()?;
    let vector_indexes = m
        .vector_indexes
        .into_iter()
        .map(|v| {
            let kind = match pb::VectorIndexKind::try_from(v.kind) {
                Ok(pb::VectorIndexKind::IvfPq) => VectorIndexKind::IvfPq,
                Ok(pb::VectorIndexKind::IvfRq) => VectorIndexKind::IvfRq,
                Ok(pb::VectorIndexKind::IvfHnswSq) => VectorIndexKind::IvfHnswSq,
                Ok(pb::VectorIndexKind::Unspecified) | Err(_) => {
                    return Err(corrupt(format!(
                        "vector index {:?} has unspecified kind {}",
                        v.index_name, v.kind
                    )));
                }
            };
            Ok(VectorIndexRef {
                column: v.column,
                lance_index_uuid: v.lance_index_uuid,
                indexed_row_ids_upto: v.indexed_row_ids_upto,
                index_name: v.index_name,
                vector: v.vector,
                kind,
            })
        })
        .collect::<Result<_, _>>()?;
    Ok(CollectionManifest {
        version: m.version,
        parent_version: m.parent_version,
        parent_manifest: some(m.parent_manifest),
        collection_id: CollectionId(m.collection_id),
        schema_version: m.schema_version,
        created_at_ms: m.created_at_ms,
        lance_version: m.lance_version,
        splits,
        vector_indexes,
        scalar_indexes: m
            .scalar_indexes
            .into_iter()
            .map(|s| ScalarIndexRef {
                column: s.column,
                lance_index_uuid: s.lance_index_uuid,
                indexed_row_ids_upto: s.indexed_row_ids_upto,
                index_name: s.index_name,
            })
            .collect(),
        hot_artifacts: m
            .hot_artifacts
            .into_iter()
            .map(|h| HotArtifactRef {
                kind: h.kind,
                column: h.column,
                prefix: h.prefix,
                source_version: h.source_version,
            })
            .collect(),
        applied: m.applied.into_iter().collect(),
        live_doc_count: m.live_doc_count,
        kind,
        pk_delta: some(m.pk_delta),
        dead_letters: some(m.dead_letters),
        dead_letters_total: m.dead_letters_total,
        skipped_offsets_total: m.skipped_offsets_total,
    })
}

fn split_from_pb(s: pb::SplitRef) -> Result<SplitRef, CollectionError> {
    let ulid: [u8; 16] = s
        .ulid
        .as_slice()
        .try_into()
        .map_err(|_| corrupt(format!("a split ulid has {} bytes, not 16", s.ulid.len())))?;
    let ulid = Ulid(u128::from_be_bytes(ulid));
    if s.footer_start > s.footer_end {
        return Err(corrupt(format!(
            "split {ulid}: footer {}..{} ends before it starts",
            s.footer_start, s.footer_end
        )));
    }
    let row_id_ranges: Vec<Range<u64>> = s.row_id_ranges.iter().map(|r| r.start..r.end).collect();
    check_ranges(ulid, &row_id_ranges, s.doc_count)?;
    Ok(SplitRef {
        ulid,
        doc_count: s.doc_count,
        deleted_count: s.deleted_count,
        size_bytes: s.size_bytes,
        footer_range: s.footer_start..s.footer_end,
        row_id_ranges,
        delete_bitmap: some(s.delete_bitmap),
        schema_version: s.schema_version,
        created_at_ms: s.created_at_ms,
        merge_ops: s.merge_ops,
    })
}

/// `ranges` are non-empty, sorted and disjoint, and cover `doc_count` rows.
fn check_ranges(split: Ulid, ranges: &[Range<u64>], doc_count: u64) -> Result<(), CollectionError> {
    let mut total: u64 = 0;
    let mut previous_end = None;
    for range in ranges {
        if range.start >= range.end {
            return Err(corrupt(format!(
                "split {split}: empty row-id range {range:?}"
            )));
        }
        if previous_end.is_some_and(|end| range.start < end) {
            return Err(corrupt(format!(
                "split {split}: row-id range {range:?} is unsorted or overlaps the previous one"
            )));
        }
        previous_end = Some(range.end);
        total = total.saturating_add(range.end - range.start);
    }
    if total != doc_count {
        return Err(corrupt(format!(
            "split {split}: row-id ranges cover {total} rows, not doc_count {doc_count}"
        )));
    }
    Ok(())
}

fn commit_kind_to_pb(kind: CommitKind) -> pb::CommitKind {
    match kind {
        CommitKind::LinkApply => pb::CommitKind::LinkApply,
        CommitKind::IndexBuild => pb::CommitKind::IndexBuild,
        CommitKind::Maintenance => pb::CommitKind::Maintenance,
    }
}

fn vector_kind_to_pb(kind: VectorIndexKind) -> pb::VectorIndexKind {
    match kind {
        VectorIndexKind::IvfPq => pb::VectorIndexKind::IvfPq,
        VectorIndexKind::IvfRq => pb::VectorIndexKind::IvfRq,
        VectorIndexKind::IvfHnswSq => pb::VectorIndexKind::IvfHnswSq,
    }
}

/// One row-id range of a split.
#[derive(Clone, Copy, Debug)]
struct Located {
    start: u64,
    end: u64,
    split: usize,
    /// The doc id of the range's first row.
    doc_base: u32,
}

/// Row id → (index into `splits`, doc id), by binary search over every
/// split's ranges (Ruling 8).
#[derive(Clone, Debug)]
pub struct RowLocator {
    /// Sorted by `start`, disjoint.
    ranges: Vec<Located>,
}

impl RowLocator {
    /// The locator of `splits`. An empty range, a split with more than
    /// `u32::MAX` docs, or two ranges that overlap (in one split or across
    /// splits) is [`CollectionError::Corrupt`].
    pub fn new(splits: &[SplitRef]) -> Result<Self, CollectionError> {
        let mut ranges = Vec::new();
        for (split, split_ref) in splits.iter().enumerate() {
            let mut doc_base: u64 = 0;
            for range in &split_ref.row_id_ranges {
                if range.start >= range.end {
                    return Err(corrupt(format!(
                        "split {}: empty row-id range {range:?}",
                        split_ref.ulid
                    )));
                }
                let next_base = doc_base.saturating_add(range.end - range.start);
                let (Ok(base), true) = (
                    u32::try_from(doc_base),
                    next_base - 1 <= u64::from(u32::MAX),
                ) else {
                    return Err(corrupt(format!(
                        "split {} has more than 2^32 docs",
                        split_ref.ulid
                    )));
                };
                ranges.push(Located {
                    start: range.start,
                    end: range.end,
                    split,
                    doc_base: base,
                });
                doc_base = next_base;
            }
        }
        ranges.sort_unstable_by_key(|r| r.start);
        for pair in ranges.windows(2) {
            if pair[1].start < pair[0].end {
                return Err(corrupt(format!(
                    "row ids {}..{} of split {} overlap {}..{} of split {}",
                    pair[1].start,
                    pair[1].end,
                    splits[pair[1].split].ulid,
                    pair[0].start,
                    pair[0].end,
                    splits[pair[0].split].ulid
                )));
            }
        }
        Ok(Self { ranges })
    }

    /// The split index and doc id of `row_id`, if a split has it.
    pub fn locate(&self, row_id: u64) -> Option<(usize, u32)> {
        let after = self.ranges.partition_point(|r| r.start <= row_id);
        let range = self.ranges.get(after.checked_sub(1)?)?;
        if row_id >= range.end {
            return None;
        }
        // `new` checked that every doc id of the range fits a u32.
        let offset = u32::try_from(row_id - range.start).ok()?;
        Some((range.split, range.doc_base + offset))
    }
}
