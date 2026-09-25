//! The retryable classification of store errors (plan M1.1 ruling 21).

use operon_store::StoreError;

/// Retrying after re-reading may help for a backend failure or a lost race
/// (the object appeared, or its version moved); it cannot for a missing
/// object, an unsupported operation or a malformed url or path.
#[test]
fn retryable_store_errors_are_backend_and_conflicts() {
    let path = || "ns/1/a.wal".to_string();
    // In declaration order.
    let errors = [
        StoreError::NotFound { path: path() },
        StoreError::AlreadyExists { path: path() },
        StoreError::PreconditionFailed { path: path() },
        StoreError::NotSupported("copy_if_not_exists".to_string()),
        StoreError::InvalidUrl("ftp://bucket".to_string()),
        StoreError::InvalidPath("a//b".to_string()),
        StoreError::Backend(object_store::Error::Generic {
            store: "test",
            source: "connection reset".into(),
        }),
    ];
    assert_eq!(
        errors
            .iter()
            .map(StoreError::is_retryable)
            .collect::<Vec<_>>(),
        [false, true, true, false, false, false, true]
    );
}
