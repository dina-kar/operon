use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use operon_cache::{CacheError, DiskConfig, RangeCache, RangeCacheConfig};
use operon_store::{Fault, FaultyStore, Op, Store};

fn data(len: usize) -> Bytes {
    Bytes::from((0..len).map(|i| (i % 251) as u8).collect::<Vec<u8>>())
}

/// An [`ObjectStore`] wrapper that records the peak number of concurrent
/// `get_opts` calls in flight, for asserting bounded fan-out.
struct ConcurrencyTrackingStore {
    inner: InMemory,
    current: AtomicUsize,
    max_seen: AtomicUsize,
}

impl ConcurrencyTrackingStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            current: AtomicUsize::new(0),
            max_seen: AtomicUsize::new(0),
        }
    }
}

impl fmt::Debug for ConcurrencyTrackingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConcurrencyTrackingStore").finish()
    }
}

impl fmt::Display for ConcurrencyTrackingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ConcurrencyTrackingStore")
    }
}

#[async_trait]
impl ObjectStore for ConcurrencyTrackingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let n = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_seen.fetch_max(n, Ordering::SeqCst);
        // Give overlapping calls a chance to actually run concurrently.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let result = self.inner.get_opts(location, options).await;
        self.current.fetch_sub(1, Ordering::SeqCst);
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

async fn cache_over(store: Store, block_size: u64) -> RangeCache {
    RangeCache::new(
        store,
        RangeCacheConfig {
            block_size,
            memory_bytes: 1 << 20,
            disk: None,
        },
    )
    .await
    .expect("cache builds")
}

#[tokio::test]
async fn reads_spanning_blocks_return_exact_bytes() {
    let store = Store::in_memory();
    let body = data(1000);
    store.put("obj", body.clone()).await.unwrap();
    let cache = cache_over(store, 64).await;

    for (start, end) in [
        (0, 1000),
        (10, 20),
        (63, 65),
        (0, 64),
        (999, 1000),
        (500, 500),
    ] {
        let got = cache.read("obj", start..end).await.unwrap();
        assert_eq!(
            got,
            body.slice(start as usize..end as usize),
            "range {start}..{end}"
        );
    }
}

#[tokio::test]
async fn second_read_is_served_from_cache_without_store_gets() {
    let faulty = Arc::new(FaultyStore::new(Arc::new(InMemory::new())));
    let store = Store::new(faulty.clone());
    store.put("obj", data(256)).await.unwrap();
    let cache = cache_over(store, 64).await;

    cache.read("obj", 0..256).await.unwrap();
    let gets_after_first = faulty.calls(Op::Get);
    assert_eq!(cache.stats().misses, 4);

    // Any store GET now fails; a cached read must not need one.
    faulty.inject(Op::Get, Fault::Error);
    cache.read("obj", 10..200).await.unwrap();
    assert_eq!(faulty.calls(Op::Get), gets_after_first);
    assert_eq!(cache.stats().hits, 4);
}

#[tokio::test]
async fn out_of_range_reads_are_rejected() {
    let store = Store::in_memory();
    store.put("obj", data(10)).await.unwrap();
    let cache = cache_over(store, 4).await;

    let err = cache.read("obj", 5..11).await.unwrap_err();
    assert!(
        matches!(err, CacheError::OutOfRange { size: 10, .. }),
        "got {err:?}"
    );
}

#[tokio::test]
async fn missing_object_surfaces_store_not_found() {
    let cache = cache_over(Store::in_memory(), 4).await;
    let err = cache.read("missing", 0..1).await.unwrap_err();
    assert!(matches!(
        err,
        CacheError::Store(operon_store::StoreError::NotFound { .. })
    ));
}

#[tokio::test]
async fn disk_tier_configuration_serves_correct_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::in_memory();
    let body = data(4096);
    store.put("obj", body.clone()).await.unwrap();
    let cache = RangeCache::new(
        store,
        RangeCacheConfig {
            block_size: 512,
            memory_bytes: 1024,
            disk: Some(DiskConfig {
                dir: dir.path().to_path_buf(),
                capacity_bytes: 64 << 20,
            }),
        },
    )
    .await
    .unwrap();

    assert_eq!(cache.read("obj", 0..4096).await.unwrap(), body);
    assert_eq!(
        cache.read("obj", 100..3000).await.unwrap(),
        body.slice(100..3000)
    );
}

#[tokio::test]
async fn reopened_disk_dir_with_different_block_size_does_not_serve_stale_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let body = data(64);

    let store = Store::in_memory();
    store.put("obj", body.clone()).await.unwrap();
    let cache = RangeCache::new(
        store.clone(),
        RangeCacheConfig {
            block_size: 8,
            memory_bytes: 1024,
            disk: Some(DiskConfig {
                dir: dir.path().to_path_buf(),
                capacity_bytes: 64 << 20,
            }),
        },
    )
    .await
    .unwrap();
    assert_eq!(cache.read("obj", 0..64).await.unwrap(), body);
    // Force all in-memory blocks to the disk tier and close cleanly so the
    // reopen below deterministically sees them.
    cache.close().await.unwrap();
    drop(cache);

    // Reopen the same on-disk directory with a different block size. If the
    // disk tier recovers its old contents, the block keyed the same way as
    // before (but sized for a different block_size) would be served as a hit
    // with wrong bytes.
    let cache = RangeCache::new(
        store,
        RangeCacheConfig {
            block_size: 16,
            memory_bytes: 1024,
            disk: Some(DiskConfig {
                dir: dir.path().to_path_buf(),
                capacity_bytes: 64 << 20,
            }),
        },
    )
    .await
    .unwrap();

    let got = cache.read("obj", 16..32).await.unwrap();
    assert_eq!(got, body.slice(16..32), "got wrong bytes after reopen");
}

#[tokio::test]
async fn shrunk_object_after_cached_size_returns_size_mismatch_without_panicking() {
    let store = Store::in_memory();
    store.put("obj", data(100)).await.unwrap();
    let cache = cache_over(store.clone(), 100).await;

    // Prime the cached size at 100 bytes.
    assert_eq!(cache.size("obj").await.unwrap(), 100);

    // The object shrinks after `size` was cached; the cache still believes it
    // is 100 bytes, so `read` will ask the store for a 100-byte block.
    store.put("obj", data(70)).await.unwrap();

    let err = cache.read("obj", 66..100).await.unwrap_err();
    assert!(
        matches!(
            err,
            CacheError::SizeMismatch {
                expected: 100,
                actual: 70,
                ..
            }
        ),
        "got {err:?}"
    );

    // Must not panic while slicing a too-short block.
    let err = cache.read("obj", 71..100).await.unwrap_err();
    assert!(
        matches!(
            err,
            CacheError::SizeMismatch {
                expected: 100,
                actual: 70,
                ..
            }
        ),
        "got {err:?}"
    );
}

#[tokio::test]
async fn multi_block_read_bounds_concurrent_block_fetches() {
    let tracker = Arc::new(ConcurrencyTrackingStore::new());
    let store = Store::new(tracker.clone());
    let block_size = 64usize;
    let blocks = 40;
    let body = data(block_size * blocks);
    store.put("obj", body.clone()).await.unwrap();
    let cache = cache_over(store, block_size as u64).await;

    let got = cache
        .read("obj", 0..(block_size * blocks) as u64)
        .await
        .unwrap();
    assert_eq!(got, body);

    let max_seen = tracker.max_seen.load(Ordering::SeqCst);
    assert!(
        max_seen <= 16,
        "expected at most 16 concurrent block fetches, saw {max_seen}"
    );
}

#[tokio::test]
async fn a_known_size_read_costs_one_get_and_remembers_the_size() {
    let faulty = Arc::new(FaultyStore::new(Arc::new(InMemory::new())));
    let store = Store::new(faulty.clone());
    let body = data(256);
    store.put("obj", body.clone()).await.unwrap();
    let cache = cache_over(store, 1024).await;

    let gets = faulty.calls(Op::Get);
    let got = cache.read_with_size("obj", 256, 200..256).await.unwrap();
    assert_eq!(got, body.slice(200..256));
    assert_eq!(faulty.calls(Op::Get), gets + 1, "one GET and no HEAD");

    // The size is remembered: neither `size` nor a later `read` needs a HEAD.
    faulty.inject(Op::Get, Fault::Error);
    assert_eq!(cache.size("obj").await.unwrap(), 256);
    assert_eq!(cache.read("obj", 0..10).await.unwrap(), body.slice(0..10));
    assert_eq!(faulty.calls(Op::Get), gets + 1);
}

#[tokio::test]
async fn a_known_size_read_checks_the_range_and_the_size() {
    let store = Store::in_memory();
    store.put("obj", data(256)).await.unwrap();
    store.put("other", data(256)).await.unwrap();
    // One block per object, so the short object shows as a short block.
    let cache = cache_over(store, 1024).await;

    let err = cache.read_with_size("obj", 10, 5..11).await.unwrap_err();
    assert!(
        matches!(err, CacheError::OutOfRange { size: 10, .. }),
        "got {err:?}"
    );

    // The object is shorter than the caller claims.
    let err = cache
        .read_with_size("obj", 300, 250..300)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            CacheError::SizeMismatch {
                expected: 300,
                actual: 256,
                ..
            }
        ),
        "got {err:?}"
    );
    // A failed read remembers nothing.
    assert_eq!(cache.size("obj").await.unwrap(), 256);

    // A claim that contradicts the size already known is refused.
    let err = cache.read_with_size("obj", 300, 0..10).await.unwrap_err();
    assert!(
        matches!(
            err,
            CacheError::SizeMismatch {
                expected: 300,
                actual: 256,
                ..
            }
        ),
        "got {err:?}"
    );
    assert_eq!(
        cache.read_with_size("other", 256, 0..4).await.unwrap(),
        data(4)
    );
}

#[tokio::test]
async fn a_cold_read_fetches_each_run_of_missing_blocks_in_one_get() {
    let faulty = Arc::new(FaultyStore::new(Arc::new(InMemory::new())));
    let store = Store::new(faulty.clone());
    let body = data(1000);
    store.put("obj", body.clone()).await.unwrap();
    let cache = cache_over(store, 64).await;

    // Five cold blocks (0..=4), one GET.
    let gets = faulty.calls(Op::Get);
    let got = cache.read_with_size("obj", 1000, 10..300).await.unwrap();
    assert_eq!(got, body.slice(10..300));
    assert_eq!(faulty.calls(Op::Get), gets + 1);
    assert_eq!(cache.stats().misses, 5);

    // Blocks 5..=9 with 7 already cached: two runs, two GETs.
    cache.read("obj", 450..460).await.unwrap();
    let gets = faulty.calls(Op::Get);
    let got = cache.read("obj", 320..640).await.unwrap();
    assert_eq!(got, body.slice(320..640));
    assert_eq!(faulty.calls(Op::Get), gets + 2);
    assert_eq!(cache.stats().misses, 5 + 1 + 4);
    assert_eq!(cache.stats().hits, 1);

    // Every block of those runs was cached on its own.
    faulty.inject(Op::Get, Fault::Error);
    assert_eq!(cache.read("obj", 0..640).await.unwrap(), body.slice(0..640));
}

#[tokio::test]
async fn a_known_size_read_refuses_a_smaller_size_and_caches_nothing() {
    let store = Store::in_memory();
    store.put("obj", data(256)).await.unwrap();
    let cache = cache_over(store, 1024).await;

    // The object is longer than the caller claims: the GET's own metadata
    // says so, before any block is cached or the size remembered.
    let err = cache
        .read_with_size("obj", 200, 150..200)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            CacheError::SizeMismatch {
                expected: 200,
                actual: 256,
                ..
            }
        ),
        "got {err:?}"
    );
    assert_eq!(
        cache.read_with_size("obj", 256, 150..256).await.unwrap(),
        data(256).slice(150..256)
    );
    assert_eq!(
        cache.stats().checksum_failures,
        0,
        "no short block was cached"
    );
}

#[tokio::test]
async fn forget_drops_the_size_and_the_blocks_of_an_object() {
    let faulty = Arc::new(FaultyStore::new(Arc::new(InMemory::new())));
    let store = Store::new(faulty.clone());
    store.put("obj", data(100)).await.unwrap();
    let cache = cache_over(store.clone(), 64).await;
    cache.read("obj", 0..100).await.unwrap();

    store.delete("obj").await.unwrap();
    cache.forget("obj").await;
    assert!(matches!(
        cache.size("obj").await,
        Err(CacheError::Store(operon_store::StoreError::NotFound { .. }))
    ));
    // A new object at the path is read afresh.
    store.put("obj", Bytes::from(vec![7u8; 30])).await.unwrap();
    assert_eq!(cache.read("obj", 0..30).await.unwrap(), vec![7u8; 30]);
    cache.forget("never-read").await;
}
