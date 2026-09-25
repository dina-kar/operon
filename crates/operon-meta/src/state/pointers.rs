//! Versioned pointers with compare-and-swap, for manifest commits (design §03 §3.3).

use operon_common::{CollectionId, NamespaceId};

use super::{MetaState, validate_key};
use crate::command::{ApplyError, Reply};
use crate::types::{COLLECTION_POINTER_PREFIX, Fence, Freshness, Pointer};

impl MetaState {
    pub(super) fn cas_pointer(
        &mut self,
        namespace: NamespaceId,
        key: String,
        expected: Option<u64>,
        value: String,
        fence: Option<Fence>,
        fresh: Option<Freshness>,
    ) -> Result<Reply, ApplyError> {
        validate_key("pointer key", &key)?;
        validate_key("pointer value", &value)?;
        self.check_collection_pointer(namespace, &key)?;
        if !self.namespaces.contains_key(&namespace) {
            return Err(ApplyError::NamespaceNotFound(namespace));
        }
        if let Some(fence) = &fence {
            self.check_fence(fence)?;
        }
        let slot = (namespace, key);
        let current = self.pointers.get(&slot);
        let version = match (expected, current) {
            (None, None) => 1,
            (Some(expected), Some(pointer)) if pointer.version == expected => expected + 1,
            _ => {
                return Err(ApplyError::VersionMismatch {
                    current: current.cloned(),
                });
            }
        };
        // After the version check, so a retry of an applied CAS still sees
        // its own value in the mismatch.
        if let Some(fresh) = fresh
            && fresh.expired_at(self.clock_ms)
        {
            return Err(ApplyError::StaleObject {
                object: value,
                created_at_ms: fresh.created_at_ms,
                max_age_ms: fresh.max_age_ms,
                clock_ms: self.clock_ms,
            });
        }
        self.pointers.insert(slot, Pointer { version, value });
        Ok(Reply::PointerSet { version })
    }

    /// A `collection/<id>` pointer may only be set while that collection
    /// exists in `namespace` (plan M1.1 Ruling 14): a task still running for
    /// a dropped collection must not create a pointer for a dead id.
    fn check_collection_pointer(
        &self,
        namespace: NamespaceId,
        key: &str,
    ) -> Result<(), ApplyError> {
        let Some(id) = key.strip_prefix(COLLECTION_POINTER_PREFIX) else {
            return Ok(());
        };
        let id: CollectionId = id.parse().map_err(|_| {
            ApplyError::InvalidArgument(format!("invalid collection pointer key {key:?}"))
        })?;
        match self.collections.get(&id) {
            Some(collection) if collection.namespace == namespace => Ok(()),
            _ => Err(ApplyError::CollectionNotFound(id)),
        }
    }

    /// The pointer `key` in `namespace`, if it has been set.
    pub fn pointer(&self, namespace: NamespaceId, key: &str) -> Option<&Pointer> {
        self.pointers.get(&(namespace, key.to_string()))
    }
}
