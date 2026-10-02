//! Manifest pointers: versioned, compare-and-swap, fenced, freshness-checked
//! (design §20 §11.3). A CAS reads the pointer at the transaction's start
//! timestamp and writes it; a concurrent writer is a write-write conflict at
//! prewrite, so the runner reruns the loser, which then sees the new version.
//! The fence's lease, the collection's record (its `live` state: present
//! until `drop_collection` deletes it) and the new value's `o/` row
//! (`gc_claim`) are checked and locked in the same transaction, so a
//! takeover, a drop or a GC claim committed concurrently conflicts with it.

use loams_common::meta::{
    ApplyError, COLLECTION_POINTER_PREFIX, MetaResult, Pointer, PointerCas, Tracked,
};
use loams_common::{CollectionId, NamespaceId};
use loams_tikv::{Tikv, Txn, TxnError};

use crate::catalog::{load_collection, namespace_exists, validate_key};
use crate::keys::{self, ObjectRef};
use crate::leases::check_fence;
use crate::{TikvMeta, load};

/// A `collection/<id>` pointer may only be set while that collection exists
/// in `ns` (M1.1 Ruling 14). Locks the collection record.
async fn check_collection_pointer(
    txn: &mut Txn,
    ns: NamespaceId,
    key: &str,
) -> Result<Result<(), ApplyError>, TxnError> {
    let Some(id) = key.strip_prefix(COLLECTION_POINTER_PREFIX) else {
        return Ok(Ok(()));
    };
    let Ok(id) = id.parse::<CollectionId>() else {
        return Ok(Err(ApplyError::InvalidArgument(format!(
            "invalid collection pointer key {key:?}"
        ))));
    };
    match load_collection(txn, id).await? {
        Some(collection) if collection.namespace == ns => {
            txn.lock_keys([keys::collection(id)]).await?;
            Ok(Ok(()))
        }
        _ => Ok(Err(ApplyError::CollectionNotFound(id))),
    }
}

impl TikvMeta {
    pub(crate) async fn cas_pointer_impl(&self, cas: PointerCas) -> Tracked<u64> {
        if let Err(e) = validate_key("pointer key", &cas.key)
            .and_then(|()| validate_key("pointer value", &cas.value))
        {
            return Tracked {
                result: crate::rejected(e),
                earlier_unknown: false,
            };
        }
        let (result, earlier_unknown) = self
            .write("meta.cas_pointer", move |txn| {
                let cas = cas.clone();
                Box::pin(async move {
                    let ns = cas.namespace;
                    if let Err(e) = check_collection_pointer(txn, ns, &cas.key).await? {
                        return Ok(Err(e));
                    }
                    if !namespace_exists(txn, ns).await? {
                        return Ok(Err(ApplyError::NamespaceNotFound(ns)));
                    }
                    if let Some(fence) = &cas.fence
                        && let Err(e) = check_fence(txn, fence).await?
                    {
                        return Ok(Err(e));
                    }
                    let key = keys::pointer(ns, &cas.key);
                    let current: Option<Pointer> = load(txn, "pointer", &key).await?;
                    let version = match (cas.expected, &current) {
                        (None, None) => 1,
                        (Some(expected), Some(pointer)) if pointer.version == expected => {
                            expected + 1
                        }
                        _ => return Ok(Err(ApplyError::VersionMismatch { current })),
                    };
                    // After the version check, so a retry of an applied CAS
                    // still sees its own value in the mismatch.
                    let clock_ms = Tikv::physical_ms(&txn.start_ts());
                    if let Some(fresh) = cas.fresh
                        && fresh.expired_at(clock_ms)
                    {
                        return Ok(Err(ApplyError::StaleObject {
                            object: cas.value,
                            created_at_ms: fresh.created_at_ms,
                            max_age_ms: fresh.max_age_ms,
                            clock_ms,
                        }));
                    }
                    // The new value's GC claim: garbage collection has decided
                    // to delete it, so it must not become reachable.
                    let object_key = keys::object_ref(&cas.value);
                    let claimed = load::<ObjectRef>(txn, "object reference", &object_key)
                        .await?
                        .is_some_and(|r| r.gc_claim);
                    if claimed {
                        let fresh = cas.fresh.unwrap_or(loams_common::meta::Freshness {
                            created_at_ms: 0,
                            max_age_ms: 0,
                        });
                        return Ok(Err(ApplyError::StaleObject {
                            object: cas.value,
                            created_at_ms: fresh.created_at_ms,
                            max_age_ms: fresh.max_age_ms,
                            clock_ms,
                        }));
                    }
                    txn.lock_keys([object_key]).await?;
                    txn.put(
                        &key,
                        keys::encode(&Pointer {
                            version,
                            value: cas.value,
                        }),
                    )
                    .await?;
                    Ok(Ok(version))
                })
            })
            .await;
        Tracked {
            result,
            earlier_unknown,
        }
    }

    pub(crate) async fn pointer_impl(
        &self,
        ns: NamespaceId,
        key: &str,
    ) -> MetaResult<Option<Pointer>> {
        let key = keys::pointer(ns, key);
        self.read(move |snap| {
            let key = key.clone();
            Box::pin(async move { Ok(Ok(load(snap, "pointer", &key).await?)) })
        })
        .await
    }
}
