use bytes::Bytes;
use operon_store::{Store, StoreError};

const NO_OPTIONS: [(&str, &str); 0] = [];

#[tokio::test]
async fn file_url_opens_local_directory_with_create_only_writes() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("file://{}", dir.path().join("bucket").display());
    let store = Store::from_url(&url, NO_OPTIONS).unwrap();

    store
        .put_if_absent("wal/0001.wal", Bytes::from_static(b"data"))
        .await
        .unwrap();
    let err = store
        .put_if_absent("wal/0001.wal", Bytes::from_static(b"data"))
        .await
        .unwrap_err();
    assert!(
        matches!(err, StoreError::AlreadyExists { .. }),
        "got {err:?}"
    );
    assert!(dir.path().join("bucket/wal/0001.wal").exists());
}

#[test]
fn file_url_stores_fsync_their_writes() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("file://{}", dir.path().join("bucket").display());
    let store = Store::from_url(&url, NO_OPTIONS).unwrap();

    // `LocalFileSystem` does not expose its fsync setting except through
    // `Debug`. Without fsync, a write that returned can vanish on power loss.
    let debug = format!("{store:?}");
    assert!(
        debug.contains("LocalFileSystem") && debug.contains("fsync: true"),
        "expected a LocalFileSystem with fsync enabled, got {debug}"
    );
}

#[tokio::test]
async fn memory_url_opens_in_memory_store() {
    let store = Store::from_url("memory:///", NO_OPTIONS).unwrap();
    store.put("k", Bytes::from_static(b"v")).await.unwrap();
    assert_eq!(store.get("k").await.unwrap().0, Bytes::from_static(b"v"));
}

#[test]
fn unparseable_url_is_rejected() {
    let err = Store::from_url("not a url", NO_OPTIONS).unwrap_err();
    assert!(matches!(err, StoreError::InvalidUrl(_)), "got {err:?}");
}

#[tokio::test]
async fn put_if_match_is_not_supported_on_file_backend() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("file://{}", dir.path().join("bucket").display());
    let store = Store::from_url(&url, NO_OPTIONS).unwrap();

    let version = store
        .put_if_absent("obj", Bytes::from_static(b"v1"))
        .await
        .unwrap();

    let err = store
        .put_if_match("obj", Bytes::from_static(b"v2"), &version)
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::NotSupported(_)), "got {err:?}");
}

#[tokio::test]
async fn memory_url_with_prefix_scopes_the_inner_store_and_round_trips() {
    let store = Store::from_url("memory:///some/prefix", NO_OPTIONS).unwrap();

    // `from_url` wraps the parsed store in an `object_store::prefix::PrefixStore`
    // scoped to the parsed path when the prefix is non-empty; its `Debug` output
    // exposes both the wrapper and the prefix it applies.
    let debug = format!("{store:?}");
    assert!(
        debug.contains("PrefixStore") && debug.contains("some/prefix"),
        "expected a PrefixStore over \"some/prefix\", got {debug}"
    );

    store.put("obj", Bytes::from_static(b"v")).await.unwrap();
    assert_eq!(store.get("obj").await.unwrap().0, Bytes::from_static(b"v"));
}
