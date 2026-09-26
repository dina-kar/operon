//! Leases and fencing.

use std::time::Duration;

use operon_common::meta::{
    ApplyError, Consistency, Fence, Lease, MAX_KEY_LEN, MAX_LEASE_TTL_MS, PointerCas,
};

use super::{chunk, commit, namespace, rejected, stream};
use crate::Backend;

const L: Consistency = Consistency::Linearizable;
const TTL: Duration = Duration::from_secs(30);
/// A lease this short has expired by the time the next write applies.
const BRIEF: Duration = Duration::from_millis(1);

/// Long enough for a `BRIEF` lease to have expired by any clock.
async fn let_it_expire() {
    tokio::time::sleep(Duration::from_millis(50)).await;
}

pub async fn acquire_renew_release(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let key = "lease/acquire-renew";
    let grant = meta
        .acquire_lease(key, "owner-1", TTL)
        .await
        .expect("acquire");
    assert_eq!(grant.epoch, 1);
    assert_eq!(
        db.last().lease(L, key).await.expect("read"),
        Some(Lease {
            epoch: 1,
            owner: Some("owner-1".to_string()),
            deadline_ms: grant.deadline_ms,
        })
    );
    let renewed = db
        .last()
        .renew_lease(key, "owner-1", 1, TTL)
        .await
        .expect("renew");
    assert_eq!(renewed.epoch, 1);
    assert!(renewed.deadline_ms >= grant.deadline_ms);
    meta.release_lease(key, "owner-1", 1)
        .await
        .expect("release");
    let released = db.last().lease(L, key).await.expect("read").expect("lease");
    assert_eq!(released.epoch, 1);
    assert_eq!(released.owner, None);
    // Releasing again at the same epoch succeeds.
    meta.release_lease(key, "owner-1", 1)
        .await
        .expect("release again");
    // A released lease is free: the next holder gets the next epoch.
    let next = meta
        .acquire_lease(key, "owner-2", TTL)
        .await
        .expect("acquire");
    assert_eq!(next.epoch, 2);
    assert_eq!(meta.lease(L, "lease/never").await.expect("read"), None);
}

pub async fn a_held_lease_refuses_another_owner(backend: &dyn Backend) {
    let db = backend.start().await;
    let key = "lease/held";
    let grant = db
        .first()
        .acquire_lease(key, "owner-1", TTL)
        .await
        .expect("acquire");
    assert_eq!(
        rejected(db.last().acquire_lease(key, "owner-2", TTL).await),
        ApplyError::LeaseHeld {
            owner: "owner-1".to_string(),
            deadline_ms: grant.deadline_ms,
        }
    );
    // The holder acquiring again keeps its epoch (a retry).
    let again = db
        .last()
        .acquire_lease(key, "owner-1", TTL)
        .await
        .expect("acquire again");
    assert_eq!(again.epoch, grant.epoch);
    let lease = db
        .first()
        .lease(L, key)
        .await
        .expect("read")
        .expect("lease");
    assert_eq!(lease.owner.as_deref(), Some("owner-1"));
    assert_eq!(lease.epoch, 1);
}

pub async fn renew_at_a_stale_epoch_is_lease_lost(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let key = "lease/stale-epoch";
    let grant = meta
        .acquire_lease(key, "owner-1", TTL)
        .await
        .expect("acquire");
    let lost = ApplyError::LeaseLost {
        key: key.to_string(),
    };
    assert_eq!(
        rejected(meta.renew_lease(key, "owner-1", grant.epoch + 1, TTL).await),
        lost
    );
    assert_eq!(
        rejected(meta.renew_lease(key, "owner-2", grant.epoch, TTL).await),
        lost
    );
    assert_eq!(
        rejected(meta.renew_lease("lease/unknown", "owner-1", 1, TTL).await),
        ApplyError::LeaseLost {
            key: "lease/unknown".to_string()
        }
    );
    assert_eq!(
        rejected(
            meta.reacquire_lease(key, "owner-1", grant.epoch + 1, TTL)
                .await
        ),
        lost
    );
    assert_eq!(
        rejected(meta.release_lease(key, "owner-1", grant.epoch + 1).await),
        lost
    );
    assert_eq!(
        rejected(meta.release_lease(key, "owner-2", grant.epoch).await),
        lost
    );
    // Still held as it was.
    let lease = db.last().lease(L, key).await.expect("read").expect("lease");
    assert_eq!(lease.owner.as_deref(), Some("owner-1"));
    assert_eq!(lease.epoch, grant.epoch);
    assert_eq!(lease.deadline_ms, grant.deadline_ms);
}

pub async fn reacquire_after_expiry_keeps_the_epoch(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let key = "lease/reacquire";
    let grant = meta
        .acquire_lease(key, "owner-1", BRIEF)
        .await
        .expect("acquire");
    let_it_expire().await;
    // Expired: it cannot be renewed...
    assert_eq!(
        rejected(meta.renew_lease(key, "owner-1", grant.epoch, TTL).await),
        ApplyError::LeaseLost {
            key: key.to_string()
        }
    );
    // ...but nobody took it, so its holder re-takes it at the same epoch.
    let again = db
        .last()
        .reacquire_lease(key, "owner-1", grant.epoch, TTL)
        .await
        .expect("reacquire");
    assert_eq!(again.epoch, grant.epoch);
    assert!(again.deadline_ms > grant.deadline_ms);
    // Fences at that epoch still hold.
    let ns = namespace(meta, "lease-reacquire").await;
    let fence = Fence {
        lease: key.to_string(),
        epoch: grant.epoch,
    };
    let version = meta
        .cas_pointer(PointerCas {
            namespace: ns,
            key: "lease-reacquire/pointer".to_string(),
            expected: None,
            value: "v1".to_string(),
            fence: Some(fence),
            fresh: None,
        })
        .await
        .into_result()
        .expect("fenced cas");
    assert_eq!(version, 1);
    let renewed = meta
        .renew_lease(key, "owner-1", grant.epoch, TTL)
        .await
        .expect("renew");
    assert_eq!(renewed.epoch, grant.epoch);
}

pub async fn takeover_bumps_the_epoch_and_fences_the_old_holder(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let key = "lease/takeover";
    let ns = namespace(meta, "lease-takeover").await;
    let s = stream(meta, ns, "events", 1).await;
    commit(meta, "lease-takeover/wal-1", vec![chunk(s, 0, 2, 0..20)]).await;
    let old = meta
        .acquire_lease(key, "owner-1", BRIEF)
        .await
        .expect("acquire");
    let_it_expire().await;
    let new = db
        .last()
        .acquire_lease(key, "owner-2", TTL)
        .await
        .expect("take over");
    assert_eq!(new.epoch, old.epoch + 1);
    let lost = ApplyError::LeaseLost {
        key: key.to_string(),
    };
    assert_eq!(
        rejected(meta.renew_lease(key, "owner-1", old.epoch, TTL).await),
        lost
    );
    assert_eq!(
        rejected(meta.reacquire_lease(key, "owner-1", old.epoch, TTL).await),
        lost
    );
    assert_eq!(
        rejected(meta.release_lease(key, "owner-1", old.epoch).await),
        lost
    );
    let stale = Fence {
        lease: key.to_string(),
        epoch: old.epoch,
    };
    let fenced = ApplyError::Fenced {
        lease: key.to_string(),
    };
    let cas = meta
        .cas_pointer(PointerCas {
            namespace: ns,
            key: "lease-takeover/pointer".to_string(),
            expected: None,
            value: "v1".to_string(),
            fence: Some(stale.clone()),
            fresh: None,
        })
        .await;
    assert!(!cas.earlier_unknown);
    assert_eq!(rejected(cas.result), fenced);
    assert_eq!(
        rejected(meta.trim_partition(s, 0, 2, Some(stale.clone())).await),
        fenced
    );
    assert_eq!(
        rejected(meta.forget_objects(Vec::new(), Some(stale)).await),
        fenced
    );
    // The fenced writes changed nothing; the new holder's fence holds.
    let current = Fence {
        lease: key.to_string(),
        epoch: new.epoch,
    };
    assert_eq!(
        meta.trim_partition(s, 0, 0, Some(current))
            .await
            .expect("trim"),
        0
    );
    let lease = db.last().lease(L, key).await.expect("read").expect("lease");
    assert_eq!(lease.owner.as_deref(), Some("owner-2"));
    assert_eq!(lease.epoch, new.epoch);
}

pub async fn a_ttl_above_the_limit_is_invalid(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let key = "lease/ttl";
    for ttl in [Duration::from_millis(MAX_LEASE_TTL_MS + 1), Duration::ZERO] {
        assert!(
            matches!(
                rejected(meta.acquire_lease(key, "owner-1", ttl).await),
                ApplyError::InvalidArgument(_)
            ),
            "{ttl:?}"
        );
    }
    let long_key = "k".repeat(MAX_KEY_LEN + 1);
    assert!(matches!(
        rejected(meta.acquire_lease(&long_key, "owner-1", TTL).await),
        ApplyError::InvalidArgument(_)
    ));
    assert!(matches!(
        rejected(meta.acquire_lease(key, "", TTL).await),
        ApplyError::InvalidArgument(_)
    ));
    assert_eq!(db.last().lease(L, key).await.expect("read"), None);
    // The limit itself is allowed.
    let grant = meta
        .acquire_lease(key, "owner-1", Duration::from_millis(MAX_LEASE_TTL_MS))
        .await
        .expect("acquire");
    assert_eq!(grant.epoch, 1);
    let grant = meta
        .renew_lease(
            key,
            "owner-1",
            1,
            Duration::from_millis(MAX_LEASE_TTL_MS + 1),
        )
        .await;
    assert!(matches!(rejected(grant), ApplyError::InvalidArgument(_)));
}

pub async fn leases_with_prefix_lists_only_that_prefix(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    for key in ["task/x", "nodes", "node/2", "node/1"] {
        meta.acquire_lease(key, "owner", TTL)
            .await
            .expect("acquire");
    }
    // A released lease is listed too.
    meta.release_lease("node/2", "owner", 1)
        .await
        .expect("release");
    let listed = db
        .last()
        .leases_with_prefix(L, "node/")
        .await
        .expect("list");
    let keys: Vec<&str> = listed.iter().map(|(key, _)| key.as_str()).collect();
    assert_eq!(keys, ["node/1", "node/2"]);
    assert_eq!(listed[0].1.owner.as_deref(), Some("owner"));
    assert_eq!(listed[1].1.owner, None);
    assert_eq!(
        db.last()
            .leases_with_prefix(L, "node")
            .await
            .expect("list")
            .len(),
        3
    );
    assert!(
        meta.leases_with_prefix(L, "none/")
            .await
            .expect("list")
            .is_empty()
    );
}
