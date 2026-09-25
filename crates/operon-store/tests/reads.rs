use bytes::Bytes;
use operon_store::{Store, StoreError};

fn b(s: &'static str) -> Bytes {
    Bytes::from_static(s.as_bytes())
}

#[tokio::test]
async fn get_range_returns_requested_bytes() {
    let store = Store::in_memory();
    store.put("obj", b("0123456789")).await.unwrap();

    assert_eq!(store.get_range("obj", 2..5).await.unwrap(), b("234"));
    assert_eq!(store.get_range("obj", 7..7).await.unwrap(), Bytes::new());
    assert_eq!(store.head("obj").await.unwrap().size, 10);
}

#[tokio::test]
async fn head_of_missing_object_is_not_found() {
    let store = Store::in_memory();
    let err = store.head("nope").await.unwrap_err();
    assert!(matches!(err, StoreError::NotFound { .. }), "got {err:?}");
}

#[tokio::test]
async fn delete_is_idempotent() {
    let store = Store::in_memory();
    store.put("obj", b("x")).await.unwrap();
    store.delete("obj").await.unwrap();
    store.delete("obj").await.unwrap();
    assert!(matches!(
        store.head("obj").await.unwrap_err(),
        StoreError::NotFound { .. }
    ));
}

#[tokio::test]
async fn list_returns_objects_under_prefix_sorted() {
    let store = Store::in_memory();
    for path in ["ns/2/b", "ns/1/c", "ns/1/a", "other/z"] {
        store.put(path, b("x")).await.unwrap();
    }
    let paths: Vec<String> = store
        .list("ns/1")
        .await
        .unwrap()
        .into_iter()
        .map(|info| info.path)
        .collect();
    assert_eq!(paths, vec!["ns/1/a", "ns/1/c"]);
    assert_eq!(store.list("").await.unwrap().len(), 4);
}

#[tokio::test]
async fn get_range_with_info_reports_the_whole_object() {
    let store = Store::in_memory();
    store.put("obj", b("hello world")).await.unwrap();
    let (bytes, info) = store.get_range_with_info("obj", 6..11).await.unwrap();
    assert_eq!(bytes, b("world"));
    assert_eq!(info.size, 11);
    assert_eq!(info.path, "obj");
    // A range past the end is cut at the end.
    let (bytes, info) = store.get_range_with_info("obj", 6..20).await.unwrap();
    assert_eq!((bytes, info.size), (b("world"), 11));
    assert!(matches!(
        store.get_range_with_info("missing", 0..1).await,
        Err(StoreError::NotFound { .. })
    ));
}
