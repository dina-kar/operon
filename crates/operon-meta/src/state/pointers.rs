//! Versioned pointers with compare-and-swap, for manifest commits (design §03 §3.3).

use operon_common::NamespaceId;

use super::{MetaState, validate_key};
use crate::command::{ApplyError, Reply};
use crate::types::{Fence, Freshness, Pointer};

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

    /// The pointer `key` in `namespace`, if it has been set.
    pub fn pointer(&self, namespace: NamespaceId, key: &str) -> Option<&Pointer> {
        self.pointers.get(&(namespace, key.to_string()))
    }
}
