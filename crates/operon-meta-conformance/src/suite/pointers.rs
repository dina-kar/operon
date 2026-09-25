//! Manifest pointers: versioned, compare-and-swap, fenced, freshness-checked.

use std::time::Duration;

use operon_common::meta::{
    ApplyError, Consistency, Fence, Freshness, MAX_KEY_LEN, Pointer, collection_pointer_key,
};
use operon_common::{CollectionId, NamespaceId};

use super::{cas, namespace, rejected, schema, stamp};
use crate::Backend;

const L: Consistency = Consistency::Linearizable;

fn pointer(version: u64, value: &str) -> Option<Pointer> {
    Some(Pointer {
        version,
        value: value.to_string(),
    })
}

pub async fn cas_create_then_update(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "ptr-update").await;
    let key = "ptr-update/manifest";
    let created = meta.cas_pointer(cas(ns, key, None, "m-1")).await;
    assert!(!created.earlier_unknown);
    assert_eq!(created.result.expect("create"), 1);
    assert_eq!(
        db.last().pointer(L, ns, key).await.expect("read"),
        pointer(1, "m-1")
    );
    let updated = db
        .last()
        .cas_pointer(cas(ns, key, Some(1), "m-2"))
        .await
        .into_result()
        .expect("update");
    assert_eq!(updated, 2);
    assert_eq!(
        meta.pointer(L, ns, key).await.expect("read"),
        pointer(2, "m-2")
    );
    assert_eq!(
        meta.pointer(L, ns, "ptr-update/other").await.expect("read"),
        None
    );
    // Pointers are per namespace.
    let other = namespace(meta, "ptr-update-other").await;
    assert_eq!(meta.pointer(L, other, key).await.expect("read"), None);
}

pub async fn cas_mismatch_carries_the_current_pointer(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "ptr-mismatch").await;
    let key = "ptr-mismatch/manifest";
    meta.cas_pointer(cas(ns, key, None, "m-1"))
        .await
        .into_result()
        .expect("create");
    for expected in [None, Some(0), Some(2), Some(5)] {
        let result = db.last().cas_pointer(cas(ns, key, expected, "other")).await;
        assert!(!result.earlier_unknown);
        assert_eq!(
            rejected(result.result),
            ApplyError::VersionMismatch {
                current: pointer(1, "m-1")
            },
            "{expected:?}"
        );
    }
    assert_eq!(
        rejected(
            meta.cas_pointer(cas(ns, "ptr-mismatch/missing", Some(1), "x"))
                .await
                .into_result()
        ),
        ApplyError::VersionMismatch { current: None }
    );
    assert_eq!(
        meta.pointer(L, ns, key).await.expect("read"),
        pointer(1, "m-1")
    );
}

pub async fn a_fenced_cas_is_refused(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "ptr-fenced").await;
    let key = "ptr-fenced/manifest";
    let lease = "ptr-fenced/lease";
    let grant = meta
        .acquire_lease(lease, "owner-1", Duration::from_secs(30))
        .await
        .expect("acquire");
    let fenced = |epoch: u64, expected: Option<u64>, value: &str| {
        let mut request = cas(ns, key, expected, value);
        request.fence = Some(Fence {
            lease: lease.to_string(),
            epoch,
        });
        request
    };
    // While the lease is held at its epoch, the fence holds.
    assert_eq!(
        meta.cas_pointer(fenced(grant.epoch, None, "m-1"))
            .await
            .into_result()
            .expect("fenced cas"),
        1
    );
    let refusal = ApplyError::Fenced {
        lease: lease.to_string(),
    };
    let wrong_epoch = db
        .last()
        .cas_pointer(fenced(grant.epoch + 1, Some(1), "m-2"))
        .await;
    assert!(!wrong_epoch.earlier_unknown);
    assert_eq!(rejected(wrong_epoch.result), refusal);
    meta.release_lease(lease, "owner-1", grant.epoch)
        .await
        .expect("release");
    // A released lease no longer fences anything.
    assert_eq!(
        rejected(
            meta.cas_pointer(fenced(grant.epoch, Some(1), "m-2"))
                .await
                .into_result()
        ),
        refusal
    );
    let mut unknown = cas(ns, key, Some(1), "m-2");
    unknown.fence = Some(Fence {
        lease: "ptr-fenced/never".to_string(),
        epoch: 1,
    });
    assert_eq!(
        rejected(meta.cas_pointer(unknown).await.into_result()),
        ApplyError::Fenced {
            lease: "ptr-fenced/never".to_string()
        }
    );
    assert_eq!(
        db.last().pointer(L, ns, key).await.expect("read"),
        pointer(1, "m-1")
    );
}

pub async fn a_stale_cas_is_refused(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "ptr-stale").await;
    let key = "ptr-stale/manifest";
    stamp(meta).await;
    let mut stale = cas(ns, key, None, "ptr-stale/m-1");
    stale.fresh = Some(Freshness {
        created_at_ms: 1,
        max_age_ms: 1,
    });
    let result = meta.cas_pointer(stale).await;
    assert!(!result.earlier_unknown);
    match rejected(result.result) {
        ApplyError::StaleObject {
            object,
            created_at_ms: 1,
            max_age_ms: 1,
            clock_ms,
        } => {
            assert_eq!(object, "ptr-stale/m-1");
            assert!(clock_ms > 2, "clock {clock_ms}");
        }
        other => panic!("expected StaleObject, got {other:?}"),
    }
    assert_eq!(db.last().pointer(L, ns, key).await.expect("read"), None);
    let mut fresh = cas(ns, key, None, "ptr-stale/m-2");
    fresh.fresh = Some(Freshness {
        created_at_ms: meta.now_ms(),
        max_age_ms: 60_000,
    });
    assert_eq!(
        meta.cas_pointer(fresh).await.into_result().expect("fresh"),
        1
    );
    // The version check comes first: a stale CAS from a wrong version is a
    // mismatch.
    let mut both = cas(ns, key, None, "ptr-stale/m-3");
    both.fresh = Some(Freshness {
        created_at_ms: 1,
        max_age_ms: 1,
    });
    assert_eq!(
        rejected(meta.cas_pointer(both).await.into_result()),
        ApplyError::VersionMismatch {
            current: pointer(1, "ptr-stale/m-2")
        }
    );
}

pub async fn a_collection_pointer_needs_its_collection(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "ptr-collection").await;
    let other = namespace(meta, "ptr-collection-other").await;
    let missing = CollectionId(999);
    assert_eq!(
        rejected(
            meta.cas_pointer(cas(ns, &collection_pointer_key(missing), None, "m-1"))
                .await
                .into_result()
        ),
        ApplyError::CollectionNotFound(missing)
    );
    let (cid, _, _) = meta
        .create_collection(ns, "ptr-docs", schema(), 1)
        .await
        .expect("create collection");
    let key = collection_pointer_key(cid);
    assert_eq!(
        rejected(
            meta.cas_pointer(cas(other, &key, None, "m-1"))
                .await
                .into_result()
        ),
        ApplyError::CollectionNotFound(cid)
    );
    assert_eq!(
        db.last()
            .cas_pointer(cas(ns, &key, None, "m-1"))
            .await
            .into_result()
            .expect("cas"),
        1
    );
    assert_eq!(
        meta.pointer(L, ns, &key).await.expect("read"),
        pointer(1, "m-1")
    );
    assert!(matches!(
        rejected(
            meta.cas_pointer(cas(ns, "collection/not-a-number", None, "m-1"))
                .await
                .into_result()
        ),
        ApplyError::InvalidArgument(_)
    ));
    let missing_ns = NamespaceId(other.0 + 1000);
    assert_eq!(
        rejected(
            meta.cas_pointer(cas(missing_ns, "ptr-collection/plain", None, "m-1"))
                .await
                .into_result()
        ),
        ApplyError::NamespaceNotFound(missing_ns)
    );
}

pub async fn a_key_over_the_limit_is_invalid(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "ptr-keys").await;
    let too_long = "k".repeat(MAX_KEY_LEN + 1);
    for (key, value) in [
        (too_long.as_str(), "m-1"),
        ("", "m-1"),
        ("ptr-keys/empty-value", ""),
    ] {
        let result = meta.cas_pointer(cas(ns, key, None, value)).await;
        assert!(!result.earlier_unknown);
        assert!(
            matches!(rejected(result.result), ApplyError::InvalidArgument(_)),
            "key of {} bytes, value {value:?}",
            key.len()
        );
    }
    assert_eq!(
        db.last().pointer(L, ns, &too_long).await.expect("read"),
        None
    );
    let longest = "k".repeat(MAX_KEY_LEN);
    assert_eq!(
        meta.cas_pointer(cas(ns, &longest, None, "m-1"))
            .await
            .into_result()
            .expect("the limit itself is allowed"),
        1
    );
    assert_eq!(
        db.last().pointer(L, ns, &longest).await.expect("read"),
        pointer(1, "m-1")
    );
}
