//! Credentials at rest: the refresh token in the OS keychain through the
//! `keyring` crate (macOS Keychain, Windows Credential Manager, Linux
//! Secret Service), one entry per `(instance_id, principal_id)` under the
//! service `dev.loams.desktop` (AP1 Ruling 8). Access tokens are never
//! stored. Without a Secret Service on Linux, sign-in lasts the session
//! only, in memory, and the UI says so; never a plaintext file.

use std::collections::HashMap;
use std::sync::Mutex;

/// The keychain service name (AP1 Ruling 1).
pub const SERVICE: &str = "dev.loams.desktop";

#[derive(Debug, thiserror::Error)]
pub enum KeyError {
    #[error("the OS keychain is unavailable: {0}")]
    Unavailable(String),
    #[error("keychain error: {0}")]
    Other(String),
}

/// Where secrets live.
pub trait KeyStore: Send + Sync + std::fmt::Debug {
    /// `true` when secrets survive a restart.
    fn persistent(&self) -> bool;
    /// # Errors
    /// A keychain failure (a missing entry is `Ok(None)`).
    fn get(&self, account: &str) -> Result<Option<String>, KeyError>;
    /// # Errors
    /// A keychain failure.
    fn set(&self, account: &str, secret: &str) -> Result<(), KeyError>;
    /// # Errors
    /// A keychain failure (deleting a missing entry is fine).
    fn delete(&self, account: &str) -> Result<(), KeyError>;
}

/// The account name for one signed-in principal of one instance.
#[must_use]
pub fn account(instance_id: &str, principal_id: &str) -> String {
    format!("{instance_id}:{principal_id}")
}

/// The OS keychain.
#[derive(Debug, Default)]
pub struct OsKeyStore;

fn entry(account: &str) -> Result<keyring::Entry, KeyError> {
    keyring::Entry::new(SERVICE, account).map_err(|e| KeyError::Unavailable(e.to_string()))
}

impl KeyStore for OsKeyStore {
    fn persistent(&self) -> bool {
        true
    }

    fn get(&self, account: &str) -> Result<Option<String>, KeyError> {
        match entry(account)?.get_password() {
            Ok(secret) => Ok(Some(secret)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(KeyError::Other(e.to_string())),
        }
    }

    fn set(&self, account: &str, secret: &str) -> Result<(), KeyError> {
        entry(account)?
            .set_password(secret)
            .map_err(|e| KeyError::Other(e.to_string()))
    }

    fn delete(&self, account: &str) -> Result<(), KeyError> {
        match entry(account)?.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(e) => Err(KeyError::Other(e.to_string())),
        }
    }
}

/// Secrets for this session only (tests, and Linux without a Secret Service).
#[derive(Debug, Default)]
pub struct MemoryKeyStore(Mutex<HashMap<String, String>>);

impl KeyStore for MemoryKeyStore {
    fn persistent(&self) -> bool {
        false
    }

    fn get(&self, account: &str) -> Result<Option<String>, KeyError> {
        Ok(self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(account)
            .cloned())
    }

    fn set(&self, account: &str, secret: &str) -> Result<(), KeyError> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(account.into(), secret.into());
        Ok(())
    }

    fn delete(&self, account: &str) -> Result<(), KeyError> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(account);
        Ok(())
    }
}

/// The OS keychain if it answers, else a session-only store
/// (`keychain_unavailable_is_session_only`).
#[must_use]
pub fn best_available() -> Box<dyn KeyStore> {
    let probe = OsKeyStore.get("dev.loams.desktop:probe");
    match probe {
        Ok(_) => Box::new(OsKeyStore),
        Err(_) => Box::new(MemoryKeyStore::default()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_store_round_trips_and_is_not_persistent() {
        let store = MemoryKeyStore::default();
        let account = account("01J9", "usr_omar");
        assert_eq!(account, "01J9:usr_omar");
        assert!(!store.persistent());
        assert_eq!(store.get(&account).unwrap(), None);
        store.set(&account, "rt-1").unwrap();
        assert_eq!(store.get(&account).unwrap().as_deref(), Some("rt-1"));
        store.delete(&account).unwrap();
        store.delete(&account).unwrap();
        assert_eq!(store.get(&account).unwrap(), None);
    }
}
