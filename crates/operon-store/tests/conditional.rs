use bytes::Bytes;
use operon_store::{Store, StoreError};

fn b(s: &'static str) -> Bytes {
    Bytes::from_static(s.as_bytes())
}

#[tokio::test]
async fn put_if_absent_creates_then_rejects_second_write() {
    let store = Store::in_memory();
    store.put_if_absent("ns/1/a.wal", b("first")).await.unwrap();

    let err = store
        .put_if_absent("ns/1/a.wal", b("second"))
        .await
        .unwrap_err();
    assert!(
        matches!(err, StoreError::AlreadyExists { .. }),
        "got {err:?}"
    );

    let (data, _) = store.get("ns/1/a.wal").await.unwrap();
    assert_eq!(data, b("first"));
}

#[tokio::test]
async fn put_if_match_succeeds_with_current_version_and_fails_with_stale_one() {
    let store = Store::in_memory();
    let v1 = store.put_if_absent("meta/pointer", b("v1")).await.unwrap();
    let v2 = store
        .put_if_match("meta/pointer", b("v2"), &v1)
        .await
        .unwrap();
    assert_ne!(v1, v2);

    let err = store
        .put_if_match("meta/pointer", b("v3"), &v1)
        .await
        .unwrap_err();
    assert!(
        matches!(err, StoreError::PreconditionFailed { .. }),
        "got {err:?}"
    );

    let (data, _) = store.get("meta/pointer").await.unwrap();
    assert_eq!(data, b("v2"));
}
