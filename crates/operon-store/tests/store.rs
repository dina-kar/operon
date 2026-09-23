use bytes::Bytes;
use operon_store::{Store, StoreError};

fn b(s: &'static str) -> Bytes {
    Bytes::from_static(s.as_bytes())
}

#[tokio::test]
async fn put_then_get_round_trips_bytes_and_metadata() {
    let store = Store::in_memory();
    store.put("ns/1/obj", b("hello")).await.unwrap();

    let (data, info) = store.get("ns/1/obj").await.unwrap();
    assert_eq!(data, b("hello"));
    assert_eq!(info.path, "ns/1/obj");
    assert_eq!(info.size, 5);
}

#[tokio::test]
async fn get_of_missing_object_is_not_found() {
    let store = Store::in_memory();
    let err = store.get("nope").await.unwrap_err();
    assert!(matches!(err, StoreError::NotFound { .. }), "got {err:?}");
}
