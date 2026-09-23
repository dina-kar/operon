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

#[tokio::test]
async fn paths_with_percent_round_trip_through_list_get_and_delete() {
    let store = Store::in_memory();
    store.put("a%b", b("data")).await.unwrap();

    let listed = store.list("").await.unwrap();
    assert_eq!(listed.len(), 1);
    let path = listed[0].path.clone();

    let (data, _) = store.get(&path).await.unwrap();
    assert_eq!(data, b("data"));

    store.delete(&path).await.unwrap();
    assert!(matches!(
        store.head(&path).await.unwrap_err(),
        StoreError::NotFound { .. }
    ));
}

#[tokio::test]
async fn relative_path_segments_are_rejected() {
    let store = Store::in_memory();
    let err = store.put("d/./e", b("x")).await.unwrap_err();
    assert!(matches!(err, StoreError::InvalidPath(_)), "got {err:?}");
}
