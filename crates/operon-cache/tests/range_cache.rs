use std::sync::Arc;

use bytes::Bytes;
use object_store::memory::InMemory;
use operon_cache::{CacheError, DiskConfig, RangeCache, RangeCacheConfig};
use operon_store::{Fault, FaultyStore, Op, Store};

fn data(len: usize) -> Bytes {
    Bytes::from((0..len).map(|i| (i % 251) as u8).collect::<Vec<u8>>())
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
