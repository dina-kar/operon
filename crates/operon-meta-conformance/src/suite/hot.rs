//! Per-collection hot configuration (M1.3 Task 4; E63: the calls take the
//! namespace, the command keeps the bare id).

use operon_common::CollectionId;
use operon_common::meta::{ApplyError, Consistency, HotConfig};

use super::{namespace, rejected, schema};
use crate::Backend;

const L: Consistency = Consistency::Linearizable;

const VECTORS: HotConfig = HotConfig {
    vectors: true,
    text: false,
    fragments: false,
};
const EVERYTHING: HotConfig = HotConfig {
    vectors: true,
    text: true,
    fragments: true,
};

pub async fn collection_hot_defaults_and_set_is_retry_safe(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "hot-set").await;
    let (cid, _, _) = meta
        .create_collection(ns, "docs", schema(), 1)
        .await
        .expect("create collection");
    let reader = db.last();
    assert_eq!(
        reader.collection_hot(L, ns, cid).await.expect("read"),
        HotConfig::default()
    );
    meta.set_collection_hot(ns, cid, VECTORS)
        .await
        .expect("set");
    // Setting the same value again (a retry) succeeds.
    meta.set_collection_hot(ns, cid, VECTORS)
        .await
        .expect("set again");
    assert_eq!(
        reader.collection_hot(L, ns, cid).await.expect("read"),
        VECTORS
    );
    reader
        .set_collection_hot(ns, cid, EVERYTHING)
        .await
        .expect("replace");
    assert_eq!(
        meta.collection_hot(L, ns, cid).await.expect("read"),
        EVERYTHING
    );
    // All false clears it.
    meta.set_collection_hot(ns, cid, HotConfig::default())
        .await
        .expect("clear");
    assert_eq!(
        reader.collection_hot(L, ns, cid).await.expect("read"),
        HotConfig::default()
    );
}

pub async fn collection_hot_needs_the_collection_in_its_namespace(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "hot-missing").await;
    let other = namespace(meta, "hot-other").await;
    let (cid, _, _) = meta
        .create_collection(ns, "docs", schema(), 1)
        .await
        .expect("create collection");
    let ghost = CollectionId(cid.0 + 100);
    assert_eq!(
        rejected(meta.set_collection_hot(ns, ghost, VECTORS).await),
        ApplyError::CollectionNotFound(ghost)
    );
    assert_eq!(
        rejected(meta.set_collection_hot(other, cid, VECTORS).await),
        ApplyError::CollectionNotFound(cid)
    );
    assert_eq!(
        rejected(db.last().collection_hot(L, ns, ghost).await),
        ApplyError::CollectionNotFound(ghost)
    );
    assert_eq!(
        rejected(db.last().collection_hot(L, other, cid).await),
        ApplyError::CollectionNotFound(cid)
    );
    assert_eq!(
        meta.collection_hot(L, ns, cid).await.expect("read"),
        HotConfig::default()
    );
}

pub async fn a_dropped_collection_forgets_its_hot_config(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "hot-drop").await;
    let (cid, _, _) = meta
        .create_collection(ns, "docs", schema(), 1)
        .await
        .expect("create collection");
    meta.set_collection_hot(ns, cid, EVERYTHING)
        .await
        .expect("set");
    assert_eq!(
        meta.drop_collection(ns, "docs").await.expect("drop"),
        Some(cid)
    );
    assert_eq!(
        rejected(db.last().collection_hot(L, ns, cid).await),
        ApplyError::CollectionNotFound(cid)
    );
    // A new collection of that name is another id, with no configuration.
    let (again, _, _) = meta
        .create_collection(ns, "docs", schema(), 1)
        .await
        .expect("create again");
    assert_ne!(again, cid);
    assert_eq!(
        db.last().collection_hot(L, ns, again).await.expect("read"),
        HotConfig::default()
    );
}
