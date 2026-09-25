//! `OperonStorage` keeps Quickwit's `Storage` contract (after
//! `quickwit-storage`'s `test_suite.rs`), over `operon-store`.

use std::path::Path;
use std::sync::Arc;

use object_store::memory::InMemory;
use operon_cache::{RangeCache, RangeCacheConfig};
use operon_quickwit::storage::{Storage, StorageErrorKind};
use operon_store::{Fault, FaultyStore, Op, Store};
use operon_text::OperonStorage;
use tokio::io::AsyncReadExt;

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

async fn storage_over(store: Store, root: &str) -> OperonStorage {
    let cache = RangeCache::new(
        store.clone(),
        RangeCacheConfig {
            block_size: 64,
            memory_bytes: 1 << 20,
            disk: None,
        },
    )
    .await
    .expect("cache builds");
    OperonStorage::new(store, cache, root)
}

#[tokio::test]
async fn put_then_read_whole_and_in_slices() {
    let store = Store::in_memory();
    let storage = storage_over(store.clone(), "root/").await;
    let path = Path::new("dir/file");
    let data = payload(1000);
    storage.put(path, Box::new(data.clone())).await.unwrap();

    // Paths are relative to the root.
    assert_eq!(store.get("root/dir/file").await.unwrap().0, data);
    assert_eq!(storage.get_all(path).await.unwrap().as_slice(), &data[..]);
    for range in [0..1000, 3..4, 63..65, 500..1000, 7..7] {
        let slice = storage.get_slice(path, range.clone()).await.unwrap();
        assert_eq!(slice.as_slice(), &data[range.clone()], "{range:?}");
        let with_len = storage
            .get_slice_with_file_len(path, 1000, range.clone())
            .await
            .unwrap();
        assert_eq!(with_len.as_slice(), &data[range.clone()], "{range:?}");
    }
    let mut stream = storage.get_slice_stream(path, 10..20).await.unwrap();
    let mut streamed = Vec::new();
    stream.read_to_end(&mut streamed).await.unwrap();
    assert_eq!(streamed, &data[10..20]);
    assert_eq!(storage.file_num_bytes(path).await.unwrap(), 1000);
    assert!(storage.exists(path).await.unwrap());

    let mut copy = Vec::new();
    storage.copy_to(path, &mut copy).await.unwrap();
    assert_eq!(copy, data);

    // A root without its trailing slash gets one.
    let unslashed = storage_over(store, "root").await;
    assert_eq!(unslashed.get_all(path).await.unwrap().as_slice(), &data[..]);
    storage.check_connectivity().await.unwrap();
}

#[tokio::test]
async fn missing_files_are_not_found_and_deletes_are_idempotent() {
    let storage = storage_over(Store::in_memory(), "root/").await;
    let missing = Path::new("missing");
    assert!(!storage.exists(missing).await.unwrap());
    for err in [
        storage.get_all(missing).await.unwrap_err(),
        storage.get_slice(missing, 0..1).await.unwrap_err(),
        storage.file_num_bytes(missing).await.unwrap_err(),
    ] {
        assert_eq!(err.kind(), StorageErrorKind::NotFound, "{err}");
    }
    storage.delete(missing).await.unwrap();

    let path = Path::new("file");
    storage.put(path, Box::new(payload(10))).await.unwrap();
    storage.delete(path).await.unwrap();
    assert_eq!(
        storage.get_all(path).await.unwrap_err().kind(),
        StorageErrorKind::NotFound
    );
}

#[tokio::test]
async fn bulk_delete_deletes_every_file() {
    let storage = storage_over(Store::in_memory(), "").await;
    let paths = [Path::new("a"), Path::new("b/c"), Path::new("never-written")];
    for path in &paths[..2] {
        storage.put(path, Box::new(payload(5))).await.unwrap();
    }
    storage.bulk_delete(&paths).await.unwrap();
    for path in paths {
        assert!(!storage.exists(path).await.unwrap(), "{path:?}");
    }
}

#[tokio::test]
async fn store_failures_map_to_storage_error_kinds() {
    let faulty = Arc::new(FaultyStore::new(Arc::new(InMemory::new())));
    let storage = storage_over(Store::new(faulty.clone()), "").await;
    let path = Path::new("file");
    storage.put(path, Box::new(payload(10))).await.unwrap();

    // A backend failure is retryable: `Io`.
    faulty.inject(Op::Get, Fault::Error);
    assert_eq!(
        storage.get_all(path).await.unwrap_err().kind(),
        StorageErrorKind::Io
    );
    faulty.inject(Op::Delete, Fault::Error);
    let err = storage
        .bulk_delete(&[path, Path::new("other")])
        .await
        .unwrap_err();
    assert!(err.successes.is_empty());
    assert_eq!(err.failures.len(), 1);
    assert_eq!(err.unattempted, [Path::new("other")]);

    // An existing file with other bytes is not overwritten: `Internal`.
    let err = storage.put(path, Box::new(payload(11))).await.unwrap_err();
    assert_eq!(err.kind(), StorageErrorKind::Internal);
    assert_eq!(
        storage.get_all(path).await.unwrap().as_slice(),
        &payload(10)[..]
    );
}
