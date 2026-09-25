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
    StorageResult, add_prefix_to_storage,
};
use tantivy::{Index, ReloadPolicy};
use tokio::io::AsyncRead;

/// Counts the reads that reach the wrapped storage.
#[derive(Debug)]
struct CountingStorage {
    inner: RamStorage,
    get_slice_calls: AtomicUsize,
    get_all_calls: AtomicUsize,
    /// Reads that came with the file length (and a path).
    known_len_calls: std::sync::Mutex<Vec<(PathBuf, u64)>>,
}

impl CountingStorage {
    fn new(inner: RamStorage) -> Self {
        Self {
            inner,
            get_slice_calls: AtomicUsize::new(0),
            get_all_calls: AtomicUsize::new(0),
            known_len_calls: std::sync::Mutex::default(),
        }
    }
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

    async fn get_slice_with_file_len(
        &self,
        path: &Path,
        file_len: u64,
        range: Range<usize>,
    ) -> StorageResult<OwnedBytes> {
        self.known_len_calls
            .lock()
            .expect("lock")
            .push((path.to_path_buf(), file_len));
        self.get_slice(path, range).await
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
    let storage = Arc::new(CountingStorage::new(ram_storage));

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

#[tokio::test]
async fn a_prefix_storage_forwards_the_known_file_length() {
    let ram_storage = RamStorage::default();
    ram_storage
        .put(Path::new("root/split"), Box::new(b"0123456789".to_vec()))
        .await
        .unwrap();
    let counting = Arc::new(CountingStorage::new(ram_storage));
    let prefixed = add_prefix_to_storage(
        counting.clone(),
        PathBuf::from("root"),
        Uri::for_test("ram:///root"),
    );
    let bytes = prefixed
        .get_slice_with_file_len(Path::new("split"), 10, 2..5)
        .await
        .unwrap();
    assert_eq!(bytes.as_slice(), b"234");
    assert_eq!(
        *counting.known_len_calls.lock().unwrap(),
        [(PathBuf::from("root/split"), 10)]
    );
}

/// The hotcache of `common::build_split()`.
fn hotcache() -> Vec<u8> {
    let split = common::build_split();
    let footer_range = split.footer_range.clone();
    let footer = split.range_bytes(footer_range).expect("footer bytes");
    let (_, hotcache) = BundleStorage::open_from_split_bytes(
        Arc::new(RamStorage::default()),
        PathBuf::from("s"),
        footer,
    )
    .expect("bundle opens");
    hotcache.as_slice().to_vec()
}

fn open_hotcache(bytes: Vec<u8>) -> anyhow::Result<HotDirectory> {
    HotDirectory::open(
        tantivy::directory::RamDirectory::create(),
        OwnedBytes::new(bytes),
    )
}

#[test]
fn a_corrupt_hotcache_is_an_error_not_a_panic() {
    let good = hotcache();
    open_hotcache(good.clone()).expect("the real hotcache opens");

    // The meta length (after the 8-byte header) overflows the hotcache.
    let mut long_meta = good.clone();
    long_meta[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(open_hotcache(long_meta).is_err());
    // The last slice cache's body length overflows it.
    let mut long_body = good.clone();
    let at = long_body.len() - 8;
    long_body[at..].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(open_hotcache(long_body).is_err());
    // Truncated hotcaches.
    for len in [0, 4, 8, 12, good.len() / 2, good.len() - 1] {
        let _ = open_hotcache(good[..len].to_vec());
    }
    // No single corrupt byte panics: every open is `Ok` or `Err`.
    for at in 0..good.len() {
        for flip in [0xFF_u8, 0x80] {
            let mut corrupt = good.clone();
            corrupt[at] ^= flip;
            let _ = open_hotcache(corrupt);
        }
    }
}
