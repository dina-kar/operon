//! Versioned pointers with compare-and-swap, for manifest commits (design §03 §3.3).

use operon_common::NamespaceId;

use super::{MetaState, validate_key};
use crate::command::{ApplyError, Reply};
use crate::types::{Fence, Pointer};

impl MetaState {
    pub(super) fn cas_pointer(
        &mut self,
        namespace: NamespaceId,
        key: String,
        expected: Option<u64>,
        value: String,
        fence: Option<Fence>,
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
        self.pointers.insert(slot, Pointer { version, value });
        Ok(Reply::PointerSet { version })
    }

    /// The pointer `key` in `namespace`, if it has been set.
    pub fn pointer(&self, namespace: NamespaceId, key: &str) -> Option<&Pointer> {
        self.pointers.get(&(namespace, key.to_string()))
    }
}
