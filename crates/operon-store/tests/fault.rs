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

#[tokio::test]
async fn put_faults_can_target_one_mode() {
    let (faults, store) = faulty();
    faults.inject(Op::PutIfMatch, Fault::Error);
    // A create-only PUT is not a compare-and-swap PUT: it passes.
    let version = store
        .put_if_absent("a", Bytes::from_static(b"1"))
        .await
        .unwrap();
    let err = store
        .put_if_match("a", Bytes::from_static(b"2"), &version)
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Backend(_)), "{err:?}");
    assert_eq!(store.get("a").await.unwrap().0, Bytes::from_static(b"1"));
    // `Put` counts and targets every mode.
    assert_eq!(faults.calls(Op::Put), 2);
    assert_eq!(faults.calls(Op::PutCreate), 1);
    assert_eq!(faults.calls(Op::PutIfMatch), 1);
    faults.inject(Op::Put, Fault::Error);
    assert!(
        store
            .put_if_absent("b", Bytes::from_static(b"x"))
            .await
            .is_err()
    );
}

/// A create-only PUT's precondition fault is a lost acknowledgement seen by
/// a retry (controller ruling P39): a real store says "exists" only when the
/// object exists, so an absent object is written, then `AlreadyExists` is
/// reported.
#[tokio::test]
async fn a_create_precondition_fault_writes_an_absent_object_then_reports_it_exists() {
    let (faults, store) = faulty();
    faults.inject(Op::PutCreate, Fault::Precondition);
    let err = store
        .put_if_absent("a", Bytes::from_static(b"1"))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::AlreadyExists { .. }), "{err:?}");
    assert_eq!(store.get("a").await.unwrap().0, Bytes::from_static(b"1"));
    // An object that exists is left as it is.
    faults.inject(Op::PutCreate, Fault::Precondition);
    let err = store
        .put_if_absent("a", Bytes::from_static(b"2"))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::AlreadyExists { .. }), "{err:?}");
    assert_eq!(store.get("a").await.unwrap().0, Bytes::from_static(b"1"));
    assert_eq!(faults.pending(Op::PutCreate), 0);
}

#[tokio::test]
async fn a_compare_and_swap_precondition_fault_does_not_apply() {
    let (faults, store) = faulty();
    let version = store
        .put_if_absent("a", Bytes::from_static(b"1"))
        .await
        .unwrap();
    faults.inject(Op::PutIfMatch, Fault::Precondition);
    let err = store
        .put_if_match("a", Bytes::from_static(b"2"), &version)
        .await
        .unwrap_err();
    assert!(
        matches!(err, StoreError::PreconditionFailed { .. }),
        "{err:?}"
    );
    assert_eq!(store.get("a").await.unwrap().0, Bytes::from_static(b"1"));
    // The next attempt, without a fault, succeeds.
    store
        .put_if_match("a", Bytes::from_static(b"2"), &version)
        .await
        .unwrap();
}

#[tokio::test]
async fn delay_faults_wait_and_then_apply() {
    let (faults, store) = faulty();
    faults.inject(Op::Put, Fault::Delay(std::time::Duration::from_millis(200)));
    let started = std::time::Instant::now();
    store.put("a", Bytes::from_static(b"1")).await.unwrap();
    assert!(started.elapsed() >= std::time::Duration::from_millis(200));
    faults.inject(Op::Get, Fault::Delay(std::time::Duration::from_millis(100)));
    assert_eq!(store.get("a").await.unwrap().0, Bytes::from_static(b"1"));
    faults.inject(
        Op::Delete,
        Fault::Delay(std::time::Duration::from_millis(50)),
    );
    store.delete("a").await.unwrap();
    assert!(store.head("a").await.is_err());
}

#[tokio::test]
async fn random_faults_follow_their_rates_and_seed() {
    use operon_store::FaultRates;
    let draw = |seed| async move {
        let faults = Arc::new(FaultyStore::random(
            Arc::new(InMemory::new()),
            seed,
            FaultRates {
                error: 0.3,
                precondition: 0.2,
                ..FaultRates::none()
            },
        ));
        let store = Store::new(faults.clone());
        let mut outcomes = Vec::new();
        for i in 0..200 {
            outcomes.push(
                store
                    .put_if_absent(&format!("k{i}"), Bytes::from_static(b"x"))
                    .await
                    .is_ok(),
            );
        }
        faults.set_rates(FaultRates::none());
        for i in 200..210 {
            outcomes.push(
                store
                    .put_if_absent(&format!("k{i}"), Bytes::from_static(b"x"))
                    .await
                    .is_ok(),
            );
        }
        outcomes
    };
    let a = draw(7).await;
    assert_eq!(a, draw(7).await, "same seed, same faults");
    assert_ne!(a, draw(8).await);
    let failed = a[..200].iter().filter(|ok| !**ok).count();
    assert!((60..=140).contains(&failed), "{failed} of 200 failed");
    assert!(
        a[200..].iter().all(|ok| *ok),
        "no faults after the rates drop"
    );
}

#[tokio::test]
async fn a_fault_can_target_a_later_call() {
    let (faults, store) = faulty();
    faults.inject_nth(Op::PutCreate, 2, Fault::Error);
    assert_eq!(faults.pending(Op::PutCreate), 2);
    store
        .put_if_absent("a", Bytes::from_static(b"1"))
        .await
        .unwrap();
    assert_eq!(faults.pending(Op::PutCreate), 1);
    // Other operations do not consume it.
    store.put("x", Bytes::from_static(b"1")).await.unwrap();
    assert!(
        store
            .put_if_absent("b", Bytes::from_static(b"1"))
            .await
            .is_err()
    );
    assert_eq!(faults.pending(Op::PutCreate), 0);
    store
        .put_if_absent("b", Bytes::from_static(b"1"))
        .await
        .unwrap();
    faults.inject(Op::Get, Fault::Error);
    faults.clear();
    assert_eq!(faults.pending(Op::Get), 0);
    store.get("a").await.unwrap();
}
