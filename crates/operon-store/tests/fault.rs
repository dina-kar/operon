use std::sync::Arc;

use bytes::Bytes;
use object_store::memory::InMemory;
use operon_store::{Fault, FaultyStore, Op, Store, StoreError};

fn faulty() -> (Arc<FaultyStore>, Store) {
    let faulty = Arc::new(FaultyStore::new(Arc::new(InMemory::new())));
    let store = Store::new(faulty.clone());
    (faulty, store)
}

#[tokio::test]
async fn error_fault_fails_put_without_writing() {
    let (faults, store) = faulty();
    faults.inject(Op::Put, Fault::Error);

    let err = store
        .put_if_absent("a", Bytes::from_static(b"x"))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Backend(_)), "got {err:?}");
    assert!(matches!(
        store.head("a").await.unwrap_err(),
        StoreError::NotFound { .. }
    ));

    // Fault consumed: the retry succeeds.
    store
        .put_if_absent("a", Bytes::from_static(b"x"))
        .await
        .unwrap();
    assert_eq!(faults.calls(Op::Put), 2);
}

#[tokio::test]
async fn error_after_apply_writes_but_reports_failure() {
    let (faults, store) = faulty();
    faults.inject(Op::Put, Fault::ErrorAfterApply);

    let err = store
        .put_if_absent("a", Bytes::from_static(b"x"))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Backend(_)), "got {err:?}");

    // The write landed, so a create-only retry now sees AlreadyExists.
    let retry = store
        .put_if_absent("a", Bytes::from_static(b"x"))
        .await
        .unwrap_err();
    assert!(
        matches!(retry, StoreError::AlreadyExists { .. }),
        "got {retry:?}"
    );
}

#[tokio::test]
async fn faults_are_consumed_in_order_per_operation() {
    let (faults, store) = faulty();
    store.put("a", Bytes::from_static(b"x")).await.unwrap();
    faults.inject(Op::Get, Fault::Error);
    faults.inject(Op::Get, Fault::Error);

    assert!(store.get("a").await.is_err());
    assert!(store.get("a").await.is_err());
    assert!(store.get("a").await.is_ok());
    // Puts were never affected by Get faults.
    store.put("b", Bytes::from_static(b"y")).await.unwrap();
}

#[tokio::test]
async fn delete_and_list_faults() {
    let (faults, store) = faulty();
    store.put("dir/a", Bytes::from_static(b"x")).await.unwrap();

    faults.inject(Op::List, Fault::Error);
    assert!(store.list("dir").await.is_err());
    assert_eq!(store.list("dir").await.unwrap().len(), 1);

    faults.inject(Op::Delete, Fault::ErrorAfterApply);
    assert!(store.delete("dir/a").await.is_err());
    assert!(matches!(
        store.head("dir/a").await.unwrap_err(),
        StoreError::NotFound { .. }
    ));
}
