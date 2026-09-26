//! Unknown outcomes: a lost acknowledgement, which the implementation
//! retries, must surface as the trait documents.

use operon_common::meta::{ApplyError, Consistency, Pointer, WalCommit};

use super::{
    cas, chunk, commit, high_watermark, namespace, rejected, schema, skip_without_faults, stream,
};
use crate::Backend;

const L: Consistency = Consistency::Linearizable;

pub async fn a_lost_ack_on_commit_wal_returns_the_first_offsets_and_flags_it(
    backend: &dyn Backend,
) {
    let db = backend.start().await;
    let Some(faults) = db.faults.clone() else {
        return skip_without_faults(
            "a_lost_ack_on_commit_wal_returns_the_first_offsets_and_flags_it",
        );
    };
    let meta = db.first();
    let ns = namespace(meta, "faults-commit").await;
    let s = stream(meta, ns, "events", 2).await;
    commit(meta, "faults-commit/wal-0", vec![chunk(s, 0, 4, 0..40)]).await;
    faults.lose_next_ack(0);
    let tracked = meta
        .commit_wal(WalCommit {
            object: "faults-commit/wal-1".to_string(),
            created_at_ms: meta.now_ms(),
            chunks: vec![chunk(s, 0, 3, 0..30), chunk(s, 1, 2, 30..50)],
        })
        .await;
    assert!(tracked.earlier_unknown, "the lost attempt is not flagged");
    assert_eq!(tracked.result.expect("commit"), vec![4, 0]);
    // Committed once.
    assert_eq!(high_watermark(db.last(), s, 0).await, 7);
    assert_eq!(high_watermark(db.last(), s, 1).await, 2);
    assert_eq!(
        commit(meta, "faults-commit/wal-2", vec![chunk(s, 0, 1, 0..10)]).await,
        vec![7]
    );
}

pub async fn a_lost_ack_on_create_collection_returns_the_same_ids(backend: &dyn Backend) {
    let db = backend.start().await;
    let Some(faults) = db.faults.clone() else {
        return skip_without_faults("a_lost_ack_on_create_collection_returns_the_same_ids");
    };
    let meta = db.first();
    let ns = namespace(meta, "faults-collection").await;
    faults.lose_next_ack(0);
    let (cid, sid, lid) = meta
        .create_collection(ns, "docs", schema(), 2)
        .await
        .expect("create collection");
    let collection = db
        .last()
        .collection(L, cid)
        .await
        .expect("read")
        .expect("collection");
    assert_eq!((collection.stream, collection.link), (sid, lid));
    assert_eq!(
        db.last().collections(L, Some(ns)).await.expect("read"),
        vec![collection]
    );
    assert_eq!(db.last().streams(L, Some(ns)).await.expect("read").len(), 1);
    // Without a lost acknowledgement, a second create is refused.
    assert_eq!(
        rejected(meta.create_collection(ns, "docs", schema(), 2).await),
        ApplyError::CollectionExists(cid)
    );
}

pub async fn a_lost_ack_on_cas_reports_a_mismatch_with_the_callers_value(backend: &dyn Backend) {
    let db = backend.start().await;
    let Some(faults) = db.faults.clone() else {
        return skip_without_faults("a_lost_ack_on_cas_reports_a_mismatch_with_the_callers_value");
    };
    let meta = db.first();
    let ns = namespace(meta, "faults-cas").await;
    let key = "faults-cas/manifest";
    meta.cas_pointer(cas(ns, key, None, "faults-cas/m-1"))
        .await
        .into_result()
        .expect("create");
    faults.lose_next_ack(0);
    let tracked = meta
        .cas_pointer(cas(ns, key, Some(1), "faults-cas/m-2"))
        .await;
    assert!(tracked.earlier_unknown, "the lost attempt is not flagged");
    let current = Pointer {
        version: 2,
        value: "faults-cas/m-2".to_string(),
    };
    assert_eq!(
        rejected(tracked.result),
        ApplyError::VersionMismatch {
            current: Some(current.clone())
        }
    );
    assert_eq!(
        db.last().pointer(L, ns, key).await.expect("read"),
        Some(current)
    );
}
