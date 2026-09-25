//! Opening a split through its hotcache costs exactly one ranged GET.

mod common;

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use operon_quickwit::directories::{HotDirectory, StorageDirectory};
use operon_quickwit::shim::Uri;
use operon_quickwit::storage::{
    BulkDeleteError, BundleStorage, OwnedBytes, PutPayload, RamStorage, SendableAsync, Storage,
    StorageResult,
};
use tantivy::{Index, ReloadPolicy};
use tokio::io::AsyncRead;

/// Counts the reads that reach the wrapped storage.
#[derive(Debug)]
struct CountingStorage {
    inner: RamStorage,
    get_slice_calls: AtomicUsize,
    get_all_calls: AtomicUsize,
}

#[async_trait]
impl Storage for CountingStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.inner.check_connectivity().await
    }

    async fn put(&self, path: &Path, payload: Box<dyn PutPayload>) -> StorageResult<()> {
        self.inner.put(path, payload).await
    }

    async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
        self.inner.copy_to(path, output).await
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        self.get_slice_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.get_slice(path, range).await
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        self.get_slice_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.get_slice_stream(path, range).await
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        self.get_all_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.get_all(path).await
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        self.inner.delete(path).await
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        self.inner.bulk_delete(paths).await
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        self.inner.file_num_bytes(path).await
    }

    fn uri(&self) -> &Uri {
        self.inner.uri()
    }
}

#[tokio::test]
async fn opening_a_split_is_one_get() {
    let split = common::build_split();
    let footer_range = split.footer_range.clone();
    let split_path = PathBuf::from("split");
    let ram_storage = RamStorage::default();
    ram_storage
        .put(&split_path, Box::new(split.clone()))
        .await
        .unwrap();
    let storage = Arc::new(CountingStorage {
        inner: ram_storage,
        get_slice_calls: AtomicUsize::new(0),
        get_all_calls: AtomicUsize::new(0),
    });

    let footer_bytes = storage
        .get_slice(
            &split_path,
            footer_range.start as usize..footer_range.end as usize,
        )
        .await
        .unwrap();
    let (bundle_storage, hotcache) =
        BundleStorage::open_from_split_bytes(storage.clone(), split_path, footer_bytes).unwrap();
    let directory = StorageDirectory::new(Arc::new(bundle_storage));
    let hot_directory = HotDirectory::open(directory, hotcache).unwrap();
    let index = Index::open(hot_directory).unwrap();
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .unwrap();
    assert_eq!(reader.searcher().num_docs(), common::NUM_DOCS);

    assert_eq!(storage.get_slice_calls.load(Ordering::SeqCst), 1);
    assert_eq!(storage.get_all_calls.load(Ordering::SeqCst), 0);
}
