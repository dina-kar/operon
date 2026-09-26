//! The hot artifact format, publishing and downloading it, and artifact
//! currency (plan M1.3 Task 4, Rulings 1 and 6), over `FaultyStore`.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use operon_collection::{
    CollectionManifest, CommitKind, ManifestCache, encode_manifest, manifest_path,
};
use operon_common::schema::Distance;
use operon_common::{CollectionId, NamespaceId};
use operon_hnsw::{BuildSpec, BuiltFiles, FLAT_ENGINE};
use operon_hot::{
    ArtifactDescriptor, COVERED_FILE, Currency, CurrencyCache, TierError, artifact_prefix,
    chunk_path, currency, decode_covered, decode_descriptor, download, effective_source_version,
    encode_covered, encode_descriptor, publish,
};
use operon_store::{Fault, FaultyStore, Op, Store};
use roaring::RoaringTreemap;
use tempfile::TempDir;
use ulid::Ulid;

const NS: NamespaceId = NamespaceId(1);
const CID: CollectionId = CollectionId(7);
const KIB: u64 = 1024;

fn faulty_store() -> (Arc<FaultyStore>, Store) {
    let faulty = Arc::new(FaultyStore::new(Store::in_memory().inner().clone()));
    (faulty.clone(), Store::new(faulty))
}

fn covered() -> RoaringTreemap {
    let mut rows: RoaringTreemap = (0..1_000u64).collect();
    rows.insert(u64::from(u32::MAX) + 5);
    rows.remove(17);
    rows
}

fn descriptor(chunk_bytes: u64, scanned: u64) -> ArtifactDescriptor {
    ArtifactDescriptor {
        engine: FLAT_ENGINE.to_string(),
        collection: CID.0,
        column: "_vector_0".to_string(),
        vector: String::new(),
        spec: BuildSpec::new(8, Distance::Cosine),
        source_version: 3,
        lance_version: 1,
        next_row_id: 1_000,
        points: 900,
        scanned,
        files: Vec::new(),
        covered_len: 0,
        covered_crc32c: 0,
        chunk_bytes,
        created_at_ms: 1_234,
    }
}

/// Bytes that zstd cannot shrink much.
fn noise(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
    (0..len)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 33) as u8
        })
        .collect()
}

/// A 600 KiB file, a 10-byte file and a sparse 32 MiB file of zeros.
fn build_dir(dir: &Path) -> BuiltFiles {
    std::fs::write(dir.join("graph.bin"), noise(600 * KIB as usize, 1)).expect("write");
    std::fs::write(dir.join("small.bin"), b"0123456789").expect("write");
    std::fs::create_dir_all(dir.join("pages")).expect("mkdir");
    let sparse = std::fs::File::create(dir.join("pages/sparse.dat")).expect("create");
    sparse.set_len(32 * 1024 * KIB).expect("set_len");
    built_files()
}

/// What `build_dir` writes.
fn built_files() -> BuiltFiles {
    BuiltFiles {
        engine: FLAT_ENGINE.to_string(),
        files: vec![
            "graph.bin".to_string(),
            "pages/sparse.dat".to_string(),
            "small.bin".to_string(),
        ],
        points: 900,
    }
}

fn prefix() -> String {
    artifact_prefix(NS, CID, "_vector_0", 3, Ulid::from_parts(1_234, 42))
}

async fn published(store: &Store, dir: &Path) -> ArtifactDescriptor {
    let built = build_dir(dir);
    let rows = covered();
    publish(
        store,
        &prefix(),
        dir,
        &built,
        &rows,
        descriptor(256 * KIB, rows.len()),
    )
    .await
    .expect("publish")
}

async fn objects(store: &Store, prefix: &str) -> BTreeMap<String, Bytes> {
    let mut out = BTreeMap::new();
    for info in store.list(prefix).await.expect("list") {
        let (bytes, _) = store.get(&info.path).await.expect("get");
        out.insert(info.path, bytes);
    }
    out
}

fn assert_corrupt(err: TierError, what: &str) {
    match err {
        TierError::Corrupt(message) => assert!(message.contains(what), "{message}"),
        other => panic!("expected Corrupt({what}), got {other:?}"),
    }
}

#[test]
fn the_prefix_and_chunk_paths_follow_ruling_6() {
    let ulid = Ulid::from_parts(1_234, 42);
    let prefix = artifact_prefix(NS, CID, "_vector_0", 3, ulid);
    assert_eq!(
        prefix,
        format!("ns/1/collections/7/hot/hnsw/_vector_0/00000000000000000003-{ulid}/")
    );
    assert_eq!(
        chunk_path(&prefix, "segments/a/graph.bin", 12),
        format!("{prefix}files/segments/a/graph.bin.000012")
    );
}

#[test]
fn descriptors_and_covered_sets_round_trip() {
    let d = descriptor(256 * KIB, 999);
    assert_eq!(decode_descriptor(&encode_descriptor(&d)).unwrap(), d);
    let rows = covered();
    assert_eq!(decode_covered(&encode_covered(&rows)).unwrap(), rows);
    let empty = RoaringTreemap::new();
    assert_eq!(decode_covered(&encode_covered(&empty)).unwrap(), empty);
}

#[test]
fn the_descriptor_envelope_is_exact() {
    for bytes in [
        encode_descriptor(&descriptor(256 * KIB, 999)),
        encode_covered(&covered()),
    ] {
        let n = bytes.len();
        assert_eq!(&bytes[4..6], &[1, 0]);
        assert_eq!(
            &bytes[n - 4..],
            &crc32c::crc32c(&bytes[..n - 4]).to_le_bytes()
        );
    }
    assert_eq!(&encode_descriptor(&descriptor(1, 0))[0..4], b"OPHD");
    let covered_bytes = encode_covered(&covered());
    assert_eq!(&covered_bytes[0..4], b"OPHC");
    assert_eq!(&covered_bytes[6..14], &covered().len().to_le_bytes());
}

#[test]
fn a_flipped_byte_is_corrupt() {
    let d = encode_descriptor(&descriptor(256 * KIB, 999));
    for i in 0..d.len() {
        let mut bad = d.to_vec();
        bad[i] ^= 0x40;
        assert!(
            matches!(decode_descriptor(&bad), Err(TierError::Corrupt(_))),
            "descriptor byte {i}"
        );
    }
    let c = encode_covered(&covered());
    for i in 0..c.len() {
        let mut bad = c.to_vec();
        bad[i] ^= 0x40;
        assert!(
            matches!(decode_covered(&bad), Err(TierError::Corrupt(_))),
            "covered byte {i}"
        );
    }
    assert!(matches!(
        decode_descriptor(&d[..5]),
        Err(TierError::Corrupt(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_then_download_reproduces_every_file() {
    let (_, store) = faulty_store();
    let built = TempDir::new().unwrap();
    let d = published(&store, built.path()).await;
    let chunks: BTreeMap<&str, u32> = d
        .files
        .iter()
        .map(|f| (f.path.as_str(), f.chunks))
        .collect();
    assert_eq!(
        chunks,
        BTreeMap::from([
            ("graph.bin", 3),
            ("pages/sparse.dat", 128),
            ("small.bin", 1)
        ])
    );
    let sparse: u64 = (0..128)
        .map(|n| chunk_path(&prefix(), "pages/sparse.dat", n))
        .map(|path| {
            let store = store.clone();
            async move { store.head(&path).await.expect("head").size }
        })
        .collect::<futures::stream::FuturesOrdered<_>>()
        .fold(0, |sum, size| async move { sum + size })
        .await;
    assert!(
        sparse < 64 * KIB,
        "the sparse file's chunks take {sparse} bytes"
    );

    let out = TempDir::new().unwrap();
    let (downloaded, rows) = download(&store, &prefix(), out.path(), 4)
        .await
        .expect("download");
    assert_eq!(downloaded, d);
    assert_eq!(rows, covered());
    for name in ["graph.bin", "small.bin", "pages/sparse.dat"] {
        assert_eq!(
            std::fs::read(out.path().join(name)).unwrap(),
            std::fs::read(built.path().join(name)).unwrap(),
            "{name}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_descriptor_is_written_last() {
    let puts = {
        let (faulty, store) = faulty_store();
        let dir = TempDir::new().unwrap();
        published(&store, dir.path()).await;
        faulty.calls(Op::PutCreate)
    };
    assert_eq!(puts, 3 + 128 + 1 + 2);
    for k in 1..=puts + 1 {
        let (faulty, store) = faulty_store();
        let dir = TempDir::new().unwrap();
        let built = build_dir(dir.path());
        let rows = covered();
        faulty.inject_nth(Op::PutCreate, k, Fault::Error);
        let result = publish(
            &store,
            &prefix(),
            dir.path(),
            &built,
            &rows,
            descriptor(256 * KIB, rows.len()),
        )
        .await;
        faulty.clear();
        let out = TempDir::new().unwrap();
        let downloaded = download(&store, &prefix(), out.path(), 4).await;
        if k <= puts {
            assert!(result.is_err(), "PUT {k} failed but publish succeeded");
            assert_corrupt(downloaded.expect_err("no descriptor yet"), "no descriptor");
        } else {
            assert!(result.is_ok(), "{result:?}");
            assert!(downloaded.is_ok(), "{downloaded:?}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_corrupt_chunk_fails_the_download() {
    let (_, store) = faulty_store();
    let dir = TempDir::new().unwrap();
    published(&store, dir.path()).await;
    // A valid frame with other content: the file's crc32c catches it.
    let path = chunk_path(&prefix(), "graph.bin", 1);
    let other = zstd::encode_all(&noise(256 * KIB as usize, 9)[..], 3).unwrap();
    store.put(&path, Bytes::from(other)).await.unwrap();
    let out = TempDir::new().unwrap();
    assert_corrupt(
        download(&store, &prefix(), out.path(), 4)
            .await
            .unwrap_err(),
        "graph.bin",
    );
    // Not a zstd frame at all.
    store
        .put(&path, Bytes::from_static(b"garbage"))
        .await
        .unwrap();
    let out = TempDir::new().unwrap();
    assert!(matches!(
        download(&store, &prefix(), out.path(), 4).await,
        Err(TierError::Corrupt(_))
    ));
    // A missing chunk.
    store.delete(&path).await.unwrap();
    let out = TempDir::new().unwrap();
    assert_corrupt(
        download(&store, &prefix(), out.path(), 4)
            .await
            .unwrap_err(),
        "missing chunk",
    );
    // A covered set that does not match the descriptor.
    let covered_path = format!("{}{COVERED_FILE}", prefix());
    store
        .put(&covered_path, encode_covered(&RoaringTreemap::new()))
        .await
        .unwrap();
    let out = TempDir::new().unwrap();
    assert_corrupt(
        download(&store, &prefix(), out.path(), 4)
            .await
            .unwrap_err(),
        "covered set",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retried_publish_is_idempotent() {
    let (_, store) = faulty_store();
    let dir = TempDir::new().unwrap();
    let first = published(&store, dir.path()).await;
    let before = objects(&store, &prefix()).await;
    let second = published(&store, dir.path()).await;
    assert_eq!(first, second);
    assert_eq!(objects(&store, &prefix()).await, before);

    // A lost acknowledgement on a fresh publish: the retry succeeds.
    let (faulty2, store2) = faulty_store();
    faulty2.inject_nth(Op::PutCreate, 2, Fault::ErrorAfterApply);
    let built = build_dir(dir.path());
    let rows = covered();
    let lost = publish(
        &store2,
        &prefix(),
        dir.path(),
        &built,
        &rows,
        descriptor(256 * KIB, rows.len()),
    )
    .await;
    assert!(lost.is_err());
    assert_eq!(published(&store2, dir.path()).await, first);
    assert_eq!(objects(&store2, &prefix()).await, before);
    // Other content under the same prefix is refused.
    std::fs::write(dir.path().join("small.bin"), b"different!").unwrap();
    let rows = covered();
    let err = publish(
        &store,
        &prefix(),
        dir.path(),
        &built_files(),
        &rows,
        descriptor(256 * KIB, rows.len()),
    )
    .await
    .expect_err("other content");
    assert!(matches!(err, TierError::Other(_)), "{err:?}");
}

// ----- Currency (Ruling 1, rule 5) -----

/// Manifest `version` of kind `kind`, with a PK delta when `delta`, child of
/// `parent`.
fn manifest(
    version: u64,
    kind: CommitKind,
    delta: bool,
    parent: Option<&str>,
) -> CollectionManifest {
    CollectionManifest {
        version,
        parent_version: version.saturating_sub(1),
        parent_manifest: parent.map(str::to_string),
        created_at_ms: 1_000 * version,
        kind,
        pk_delta: delta.then(|| format!("ns/1/collections/7/pk-delta-{version}")),
        ..CollectionManifest::empty(CID)
    }
}

/// Stores manifests 1 (link, delta) → 2 (maintenance) → 3 (index build) → 4
/// (link, dead letters only) → 5 (link, delta); returns their paths and
/// manifests, oldest first.
async fn chain(store: &Store) -> Vec<(String, CollectionManifest)> {
    let shapes = [
        (CommitKind::LinkApply, true),
        (CommitKind::Maintenance, false),
        (CommitKind::IndexBuild, false),
        (CommitKind::LinkApply, false),
        (CommitKind::LinkApply, true),
    ];
    let mut out: Vec<(String, CollectionManifest)> = Vec::new();
    for (i, (kind, delta)) in shapes.into_iter().enumerate() {
        let version = i as u64 + 1;
        let parent = out.last().map(|(path, _)| path.as_str());
        let m = manifest(version, kind, delta, parent);
        let path = manifest_path(NS, CID, version, Ulid::from_parts(version, 1));
        store.put(&path, encode_manifest(&m)).await.expect("put");
        out.push((path, m));
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn currency_rolls_the_source_version_forward_over_maintenance_commits() {
    let (_, store) = faulty_store();
    let manifests = ManifestCache::new(100);
    let chain = chain(&store).await;
    for (path, m) in &chain[..4] {
        let result = currency(&store, &manifests, (path, m), 1).await.unwrap();
        assert_eq!(
            result,
            Currency {
                current: true,
                stale_since_ms: None
            },
            "at {}",
            m.version
        );
        assert_eq!(effective_source_version(result, m.version, 1), m.version);
    }
    let (path, m) = &chain[4];
    let result = currency(&store, &manifests, (path, m), 1).await.unwrap();
    assert_eq!(
        result,
        Currency {
            current: false,
            stale_since_ms: Some(m.created_at_ms)
        }
    );
    assert_eq!(effective_source_version(result, 5, 1), 1);
    // An artifact of manifest 5 is current there.
    assert!(
        currency(&store, &manifests, (path, m), 5)
            .await
            .unwrap()
            .current
    );
    // Cached results are the same.
    let cache = CurrencyCache::new();
    let again = cache
        .currency(&store, &manifests, (path, m), 1)
        .await
        .unwrap();
    assert_eq!(again, result);
    assert_eq!(
        cache
            .currency(&store, &manifests, (path, m), 1)
            .await
            .unwrap(),
        result
    );
    // An artifact newer than the manifest is refused.
    let (path3, m3) = &chain[2];
    assert!(matches!(
        currency(&store, &manifests, (path3, m3), 4).await,
        Err(TierError::Corrupt(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_oldest_stale_commit_dates_the_staleness() {
    let (_, store) = faulty_store();
    let manifests = ManifestCache::new(100);
    let mut chain = chain(&store).await;
    // Manifest 6: another link commit with a delta, on 5.
    let parent = chain[4].0.clone();
    let m6 = manifest(6, CommitKind::LinkApply, true, Some(&parent));
    let path6 = manifest_path(NS, CID, 6, Ulid::from_parts(6, 1));
    store.put(&path6, encode_manifest(&m6)).await.unwrap();
    chain.push((path6.clone(), m6.clone()));
    let result = currency(&store, &manifests, (&path6, &m6), 1)
        .await
        .unwrap();
    assert_eq!(result.stale_since_ms, Some(chain[4].1.created_at_ms));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gc_d_ancestor_makes_the_artifact_stale() {
    let (_, store) = faulty_store();
    let chain = chain(&store).await;
    // Manifest 2 is gone: an artifact of 1 cannot be shown current at 4.
    store.delete(&chain[1].0).await.unwrap();
    let manifests = ManifestCache::new(100);
    let (path, m) = &chain[3];
    assert_eq!(
        currency(&store, &manifests, (path, m), 1).await.unwrap(),
        Currency {
            current: false,
            stale_since_ms: Some(0)
        }
    );
    // The artifact's own source manifest is never read: with 1 gone too, an
    // artifact of 2 is still current at 4.
    store.delete(&chain[0].0).await.unwrap();
    let (path, m) = &chain[3];
    assert!(
        currency(&store, &manifests, (path, m), 2)
            .await
            .unwrap()
            .current
    );
}
