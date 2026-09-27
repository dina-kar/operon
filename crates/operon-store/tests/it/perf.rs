//! Per-request object-store request counts (M1.6 Task 10, D92).

use bytes::Bytes;
use operon_store::Store;
use operon_store::perf::{self, RequestCounts};

async fn seeded() -> Store {
    let store = Store::in_memory();
    store
        .put("a/1", Bytes::from_static(b"abcdef"))
        .await
        .expect("store call");
    store
        .put("a/2", Bytes::from_static(b"gh"))
        .await
        .expect("store call");
    store
}

#[tokio::test]
async fn reads_in_a_scope_are_counted_by_kind() {
    let store = seeded().await;
    let counts = RequestCounts::new();
    perf::scope(counts.clone(), async {
        store.get("a/1").await.expect("store call");
        store.get_range("a/1", 1..3).await.expect("store call");
        store
            .get_range_with_info("a/1", 0..2)
            .await
            .expect("store call");
        store.head("a/2").await.expect("store call");
        store.list("a/").await.expect("store call");
        // An empty range makes no request.
        store.get_range("a/1", 2..2).await.expect("store call");
        // Writes are not reads.
        store
            .put("a/3", Bytes::from_static(b"x"))
            .await
            .expect("store call");
    })
    .await;
    assert_eq!((counts.get(), counts.head(), counts.list()), (3, 1, 1));
}

#[tokio::test]
async fn reads_outside_a_scope_are_not_counted() {
    let store = seeded().await;
    let counts = RequestCounts::new();
    store.get("a/1").await.expect("store call");
    perf::scope(counts.clone(), async {}).await;
    assert!(perf::current().is_none());
    assert_eq!((counts.get(), counts.head(), counts.list()), (0, 0, 0));
}

#[tokio::test]
async fn a_nested_scope_also_counts_into_its_parent() {
    let store = seeded().await;
    let outer = RequestCounts::new();
    let inner = perf::scope(outer.clone(), async {
        store.head("a/1").await.expect("store call");
        let inner = RequestCounts::new();
        perf::scope(inner.clone(), async {
            store.get("a/1").await.expect("store call");
        })
        .await;
        inner
    })
    .await;
    assert_eq!((inner.get(), inner.head()), (1, 0));
    assert_eq!((outer.get(), outer.head()), (1, 1));
}
