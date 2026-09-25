//! The collection manifest format, its paths and the manifest chain (plan
//! M1.1 Task 9, Rulings 8, 12 and 19; overview §6.4, A21).

// One-element arrays of ranges are row-id range lists here.
#![allow(clippy::single_range_in_vec_init)]

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use operon_collection::{
    CollectionError, CollectionManifest, CommitKind, HotArtifactRef, MANIFEST_FORMAT_VERSION,
    MANIFEST_MAGIC, ManifestCache, RowLocator, ScalarIndexRef, SplitRef, VectorIndexKind,
    VectorIndexRef, dead_letters_path, decode_manifest, delete_bitmap_path, encode_manifest,
    lance_prefix, manifest_path, manifest_version, pk_delta_path, retained_chain, split_path,
};
use operon_common::{CollectionId, NamespaceId};
use operon_log::gc::object_time_ms;
use operon_store::{ObjectInfo, ObjectVersion, Store};
use proptest::collection::vec;
use proptest::prelude::*;
use ulid::Ulid;

const NS: NamespaceId = NamespaceId(1);
const CID: CollectionId = CollectionId(5);

fn split(ulid: u128, ranges: &[Range<u64>]) -> SplitRef {
    SplitRef {
        ulid: Ulid(ulid),
        doc_count: ranges.iter().map(|r| r.end - r.start).sum(),
        deleted_count: 0,
        size_bytes: 1000,
        footer_range: 900..1000,
        row_id_ranges: ranges.to_vec(),
        delete_bitmap: None,
        schema_version: 1,
        created_at_ms: 7,
        merge_ops: 0,
    }
}

fn manifest(splits: Vec<SplitRef>) -> CollectionManifest {
    CollectionManifest {
        version: 3,
        parent_version: 2,
        parent_manifest: Some("ns/1/collections/5/manifests/p.pb".to_string()),
        splits,
        applied: BTreeMap::from([(0, 10), (1, 4)]),
        live_doc_count: 12,
        kind: CommitKind::LinkApply,
        ..CollectionManifest::empty(CID)
    }
}

fn corrupt(result: Result<CollectionManifest, CollectionError>) -> String {
    match result {
        Err(CollectionError::Corrupt(message)) => message,
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

/// `body` in the manifest envelope.
fn envelope(version: u16, body: &[u8]) -> Vec<u8> {
    let mut bytes = MANIFEST_MAGIC.to_vec();
    bytes.extend_from_slice(&version.to_le_bytes());
    bytes.extend_from_slice(body);
    let crc = crc32c::crc32c(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    bytes
}

// ---------------------------------------------------------------------------
// The format

fn arb_string() -> impl Strategy<Value = String> {
    "[a-z0-9/_.-]{1,12}"
}

fn arb_opt_string() -> impl Strategy<Value = Option<String>> {
    prop::option::of(arb_string())
}

/// 0–5 splits with 1–4 ranges each, the ranges of all splits disjoint and
/// interleaved across splits.
fn arb_splits() -> impl Strategy<Value = Vec<SplitRef>> {
    vec(1usize..=4, 0..=5)
        .prop_flat_map(|counts| {
            let owners: Vec<usize> = counts
                .iter()
                .enumerate()
                .flat_map(|(split, &n)| std::iter::repeat_n(split, n))
                .collect();
            let total = owners.len();
            let n = counts.len();
            (
                Just(counts),
                Just(owners).prop_shuffle(),
                vec((0u64..4, 1u64..6), total),
                vec(any::<(u128, u64, u64, u64, u64, u32)>(), n),
                vec((arb_opt_string(), 0u64..3), n),
            )
        })
        .prop_map(|(counts, owners, gaps, fields, bitmaps)| {
            let mut ranges: Vec<Vec<Range<u64>>> = vec![Vec::new(); counts.len()];
            let mut cursor = 0;
            for (owner, (gap, len)) in owners.into_iter().zip(gaps) {
                let start = cursor + gap;
                ranges[owner].push(start..start + len);
                cursor = start + len;
            }
            ranges
                .into_iter()
                .zip(fields)
                .zip(bitmaps)
                .map(
                    |((ranges, (ulid, deleted, size, footer, created, merges)), (bitmap, sv))| {
                        let doc_count = ranges.iter().map(|r| r.end - r.start).sum();
                        let footer_start = footer.min(size);
                        SplitRef {
                            ulid: Ulid(ulid),
                            doc_count,
                            deleted_count: deleted % (doc_count + 1),
                            size_bytes: size,
                            footer_range: footer_start..size,
                            row_id_ranges: ranges,
                            delete_bitmap: bitmap,
                            schema_version: sv,
                            created_at_ms: created,
                            merge_ops: merges,
                        }
                    },
                )
                .collect()
        })
}

fn arb_kind() -> impl Strategy<Value = CommitKind> {
    prop_oneof![
        Just(CommitKind::LinkApply),
        Just(CommitKind::IndexBuild),
        Just(CommitKind::Maintenance),
    ]
}

fn arb_vector_index() -> impl Strategy<Value = VectorIndexRef> {
    (
        arb_string(),
        arb_string(),
        any::<u64>(),
        arb_string(),
        arb_string(),
        prop_oneof![
            Just(VectorIndexKind::IvfPq),
            Just(VectorIndexKind::IvfRq),
            Just(VectorIndexKind::IvfHnswSq),
        ],
    )
        .prop_map(
            |(column, lance_index_uuid, indexed_row_ids_upto, index_name, vector, kind)| {
                VectorIndexRef {
                    column,
                    lance_index_uuid,
                    indexed_row_ids_upto,
                    index_name,
                    vector,
                    kind,
                }
            },
        )
}

fn arb_scalar_index() -> impl Strategy<Value = ScalarIndexRef> {
    (arb_string(), arb_string(), any::<u64>(), arb_string()).prop_map(
        |(column, lance_index_uuid, indexed_row_ids_upto, index_name)| ScalarIndexRef {
            column,
            lance_index_uuid,
            indexed_row_ids_upto,
            index_name,
        },
    )
}

fn arb_hot() -> impl Strategy<Value = HotArtifactRef> {
    (arb_string(), arb_string(), arb_string(), any::<u64>()).prop_map(
        |(kind, column, prefix, source_version)| HotArtifactRef {
            kind,
            column,
            prefix,
            source_version,
        },
    )
}

fn arb_manifest() -> impl Strategy<Value = CollectionManifest> {
    (
        (
            any::<u64>(),
            any::<u64>(),
            arb_opt_string(),
            any::<u64>(),
            any::<u64>(),
            any::<u64>(),
            any::<u64>(),
        ),
        (
            arb_splits(),
            vec(arb_vector_index(), 0..3),
            vec(arb_scalar_index(), 0..2),
            vec(arb_hot(), 0..2),
            prop::collection::btree_map(any::<u32>(), any::<u64>(), 0..4),
        ),
        (
            any::<u64>(),
            arb_kind(),
            arb_opt_string(),
            arb_opt_string(),
            any::<u64>(),
            any::<u64>(),
        ),
    )
        .prop_map(
            |(
                (version, parent_version, parent_manifest, cid, schema_version, created, lance),
                (splits, vector_indexes, scalar_indexes, hot_artifacts, applied),
                (live, kind, pk_delta, dead_letters, dead_total, skipped),
            )| CollectionManifest {
                version,
                parent_version,
                parent_manifest,
                collection_id: CollectionId(cid),
                schema_version,
                created_at_ms: created,
                lance_version: lance,
                splits,
                vector_indexes,
                scalar_indexes,
                hot_artifacts,
                applied,
                live_doc_count: live,
                kind,
                pk_delta,
                dead_letters,
                dead_letters_total: dead_total,
                skipped_offsets_total: skipped,
            },
        )
}

proptest! {
    #[test]
    fn manifests_round_trip(manifest in arb_manifest()) {
        let bytes = encode_manifest(&manifest);
        let decoded = decode_manifest(&bytes).expect("decodes");
        prop_assert_eq!(decoded, manifest);
    }
}

#[test]
fn the_envelope_is_exact() {
    let bytes = encode_manifest(&manifest(vec![split(1, &[0..12])]));
    assert_eq!(&bytes[0..4], b"OPCM");
    assert_eq!(&bytes[0..4], MANIFEST_MAGIC);
    assert_eq!(&bytes[4..6], [0x01, 0x00]);
    assert_eq!(MANIFEST_FORMAT_VERSION, 1);
    let (body, crc) = bytes.split_at(bytes.len() - 4);
    assert_eq!(crc, crc32c::crc32c(body).to_le_bytes());
}

#[test]
fn a_flipped_byte_is_corrupt() {
    let bytes = encode_manifest(&manifest(vec![split(1, &[0..5, 8..15])]));
    for at in 0..bytes.len() {
        for bit in [0x01, 0x80] {
            let mut flipped = bytes.to_vec();
            flipped[at] ^= bit;
            corrupt(decode_manifest(&flipped));
        }
    }
    corrupt(decode_manifest(&bytes[..bytes.len() - 1]));
    corrupt(decode_manifest(&[]));
}

#[test]
fn a_future_format_version_is_rejected() {
    let bytes = encode_manifest(&manifest(vec![]));
    let future = envelope(2, &bytes[6..bytes.len() - 4]);
    let message = corrupt(decode_manifest(&future));
    assert!(message.contains("version 2"), "{message}");
    // The same body at version 1 decodes.
    decode_manifest(&envelope(1, &bytes[6..bytes.len() - 4])).expect("version 1 decodes");
}

#[test]
fn overlapping_row_id_ranges_are_rejected() {
    let reject = |splits: Vec<SplitRef>| {
        corrupt(decode_manifest(&encode_manifest(&manifest(splits))));
    };
    // Within one split: overlapping, unsorted, empty, or not doc_count long.
    reject(vec![split(1, &[0..5, 4..8])]);
    reject(vec![split(1, &[10..12, 0..5])]);
    reject(vec![split(1, &[0..5, 7..7])]);
    let mut short = split(1, &[0..5]);
    short.doc_count = 6;
    reject(vec![short]);
    // Across splits.
    reject(vec![split(1, &[0..5]), split(2, &[3..9])]);
    reject(vec![split(1, &[0..5, 20..30]), split(2, &[25..26])]);
    // Touching ranges are disjoint.
    decode_manifest(&encode_manifest(&manifest(vec![
        split(1, &[0..5]),
        split(2, &[5..9]),
    ])))
    .expect("touching ranges decode");

    // A footer that ends before it starts.
    let mut backwards = split(1, &[0..5]);
    backwards.footer_range = Range { start: 10, end: 9 };
    reject(vec![backwards]);
}

#[test]
fn a_bad_ulid_or_an_unspecified_kind_is_rejected() {
    // An empty body: every field at its default, so kind is UNSPECIFIED.
    let message = corrupt(decode_manifest(&envelope(1, &[])));
    assert!(message.contains("kind"), "{message}");
    // kind = LINK_APPLY (field 13), one split (field 7) whose ulid (field 1)
    // is 3 bytes.
    let body = [0x68, 0x01, 0x3a, 0x05, 0x0a, 0x03, 1, 2, 3];
    let message = corrupt(decode_manifest(&envelope(1, &body)));
    assert!(message.contains("ulid"), "{message}");
}

#[test]
fn empty_strings_decode_to_none() {
    let mut m = manifest(vec![split(1, &[0..3])]);
    m.parent_manifest = None;
    m.pk_delta = None;
    m.dead_letters = None;
    let decoded = decode_manifest(&encode_manifest(&m)).expect("decodes");
    assert_eq!(decoded.parent_manifest, None);
    assert_eq!(decoded.pk_delta, None);
    assert_eq!(decoded.splits[0].delete_bitmap, None);
    let empty = CollectionManifest::empty(CID);
    assert_eq!(empty.version, 0);
    assert_eq!(empty.lance_version, 0);
    assert!(empty.splits.is_empty() && empty.applied.is_empty());
}

// ---------------------------------------------------------------------------
// Paths

#[test]
fn paths_are_exact() {
    let u = Ulid::from_parts(1_700_000_000_000, 42);
    let s = Ulid::from_parts(1_600_000_000_000, 7);
    assert_eq!(
        manifest_path(NS, CID, 7, u),
        format!("ns/1/collections/5/manifests/00000000000000000007-{u}.pb")
    );
    assert_eq!(
        split_path(NS, CID, u),
        format!("ns/1/collections/5/text/splits/{u}.split")
    );
    assert_eq!(
        delete_bitmap_path(NS, CID, s, u),
        format!("ns/1/collections/5/text/deletes/{s}/{u}.bitmap")
    );
    assert_eq!(
        pk_delta_path(NS, CID, 7, u),
        format!("ns/1/collections/5/pkdelta/00000000000000000007-{u}.pkd")
    );
    assert_eq!(
        dead_letters_path(NS, CID, 7, u),
        format!("ns/1/collections/5/deadletters/00000000000000000007-{u}.dlq")
    );
    assert_eq!(lance_prefix(NS, CID), "ns/1/collections/5/lance/");

    assert_eq!(manifest_version(&manifest_path(NS, CID, 7, u)), Some(7));
    assert_eq!(
        manifest_version(&manifest_path(NS, CID, u64::MAX, u)),
        Some(u64::MAX)
    );
    assert_eq!(manifest_version(&pk_delta_path(NS, CID, 7, u)), None);
    assert_eq!(manifest_version(&split_path(NS, CID, u)), None);
    assert_eq!(
        manifest_version("ns/1/collections/5/manifests/7-x.pb"),
        None
    );
}

#[test]
fn gc_object_time_reads_the_ulid_of_every_collection_path() {
    let ms = 1_700_000_123_456;
    let u = Ulid::from_parts(ms, 99);
    let split_ulid = Ulid::from_parts(1_000, 1);
    let paths = [
        manifest_path(NS, CID, 7, u),
        split_path(NS, CID, u),
        delete_bitmap_path(NS, CID, split_ulid, u),
        pk_delta_path(NS, CID, 7, u),
        dead_letters_path(NS, CID, 7, u),
    ];
    for path in paths {
        let info = ObjectInfo {
            path: path.clone(),
            size: 0,
            version: ObjectVersion {
                e_tag: None,
                version: None,
            },
            last_modified_ms: 5,
        };
        assert_eq!(object_time_ms(&info), ms, "{path}");
    }
}

// ---------------------------------------------------------------------------
// RowLocator

#[test]
fn row_locator_finds_doc_ids_across_ranges() {
    let splits = [
        split(1, &[10..13, 20..22]),
        split(2, &[13..20]),
        split(3, &[30..31]),
    ];
    let locator = RowLocator::new(&splits).expect("disjoint");
    let expect = [
        (9, None),
        (10, Some((0, 0))),
        (12, Some((0, 2))),
        (13, Some((1, 0))),
        (19, Some((1, 6))),
        (20, Some((0, 3))),
        (21, Some((0, 4))),
        (22, None),
        (29, None),
        (30, Some((2, 0))),
        (31, None),
        (u64::MAX, None),
    ];
    for (row_id, want) in expect {
        assert_eq!(locator.locate(row_id), want, "row id {row_id}");
    }
    assert_eq!(RowLocator::new(&[]).expect("empty").locate(0), None);

    match RowLocator::new(&[split(1, &[0..10]), split(2, &[9..11])]) {
        Err(CollectionError::Corrupt(_)) => {}
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// The chain

/// Writes a chain of manifests 1..=n with `created_at_ms = i * 1000`;
/// returns their paths, index 0 = version 1.
async fn chain(store: &Store, n: u64) -> Vec<(String, Arc<CollectionManifest>)> {
    let mut out: Vec<(String, Arc<CollectionManifest>)> = Vec::new();
    for version in 1..=n {
        let path = manifest_path(NS, CID, version, Ulid::from_parts(version * 1000, 1));
        let manifest = CollectionManifest {
            version,
            parent_version: version - 1,
            parent_manifest: out.last().map(|(path, _)| path.clone()),
            created_at_ms: version * 1000,
            kind: CommitKind::LinkApply,
            ..CollectionManifest::empty(CID)
        };
        store
            .put_if_absent(&path, encode_manifest(&manifest))
            .await
            .expect("put manifest");
        out.push((path, Arc::new(manifest)));
    }
    out
}

fn versions(chain: &[(String, Arc<CollectionManifest>)]) -> Vec<u64> {
    chain.iter().map(|(_, m)| m.version).collect()
}

#[tokio::test]
async fn retained_chain_keeps_the_last_n_and_the_young() {
    let store = Store::in_memory();
    let cache = ManifestCache::new(1000);
    let all = chain(&store, 30).await;
    let live = all.last().expect("live").clone();
    let retention = Duration::from_secs(10);

    let kept = retained_chain(&store, &cache, live.clone(), 3, retention, 29_000)
        .await
        .expect("chain");
    assert_eq!(versions(&kept), (19..=30).rev().collect::<Vec<_>>());
    assert_eq!(kept[0], live);
    for (path, manifest) in &kept {
        assert_eq!(manifest_version(path), Some(manifest.version));
    }

    let kept = retained_chain(&store, &cache, live.clone(), 3, retention, 100_000)
        .await
        .expect("chain");
    assert_eq!(versions(&kept), [30, 29, 28, 27]);

    // keep 0 and nothing young: only the live manifest.
    let kept = retained_chain(&store, &cache, live.clone(), 0, retention, 100_000)
        .await
        .expect("chain");
    assert_eq!(versions(&kept), [30]);

    // The walk ends at the root.
    let kept = retained_chain(&store, &cache, live.clone(), 100, retention, 100_000)
        .await
        .expect("chain");
    assert_eq!(versions(&kept), (1..=30).rev().collect::<Vec<_>>());
}

#[tokio::test]
async fn retained_chain_stops_at_a_parent_that_no_longer_exists() {
    let store = Store::in_memory();
    let all = chain(&store, 10).await;
    store.delete(&all[5].0).await.expect("delete version 6");
    // A fresh cache, so the walk has to read the store.
    let cache = ManifestCache::new(1000);
    let live = all.last().expect("live").clone();
    let kept = retained_chain(&store, &cache, live, 100, Duration::ZERO, 0)
        .await
        .expect("chain");
    assert_eq!(versions(&kept), [10, 9, 8, 7]);
}

#[tokio::test]
async fn the_manifest_cache_reads_each_path_once() {
    let store = Store::in_memory();
    let all = chain(&store, 2).await;
    let cache = ManifestCache::new(10);
    let (path, manifest) = &all[1];
    assert_eq!(&cache.load(&store, path).await.expect("load"), manifest);
    store.delete(path).await.expect("delete");
    assert_eq!(&cache.load(&store, path).await.expect("cached"), manifest);
    match cache
        .load(&store, "ns/1/collections/5/manifests/missing.pb")
        .await
    {
        Err(CollectionError::Store(operon_store::StoreError::NotFound { .. })) => {}
        other => panic!("expected NotFound, got {other:?}"),
    }
}
