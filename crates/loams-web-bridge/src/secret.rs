//! Secrets by reference: the provider never holds a credential, it holds a
//! [`SecretRef`] and asks a [`SecretResolver`] for the value at the moment it
//! is needed (design §30 D288, §37 §18.14.4 D507, §42 D567).
//!
//! The Cloudflare API token is the live case: it lives in the operator's
//! keychain or credential broker, is resolved per session, is registered with
//! [`crate::redact`] before it goes on the wire, and is never written to a log,
//! a tool result, an error, a span or a `Debug` rendering.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use crate::error::BridgeError;

/// A reference to a stored secret, never the secret itself.
///
/// The syntax is the one §18.14.4 uses for the desktop bridge:
/// `<scheme>:<locator>#<field>`, for example `env:cloudflare#api_token` or
/// `loams:acme-staging/zulip#password`. The scheme names the store the host
/// resolves it from; this crate never opens a store itself.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SecretRef(String);

impl SecretRef {
    /// Parse a reference, rejecting the shapes a value could take.
    pub fn parse(raw: &str) -> Result<Self, SecretRefError> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(SecretRefError::Empty);
        }
        if trimmed.chars().any(char::is_whitespace) {
            return Err(SecretRefError::Whitespace);
        }
        if !trimmed.contains(':') {
            return Err(SecretRefError::NoScheme);
        }
        Ok(SecretRef(trimmed.to_string()))
    }

    /// The reference as written, for messages and logs.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SecretRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::str::FromStr for SecretRef {
    type Err = SecretRefError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        SecretRef::parse(raw)
    }
}

/// Why a `secret_ref` is not a reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SecretRefError {
    /// The reference was empty.
    #[error("a secret_ref may not be empty")]
    Empty,
    /// It contained whitespace, which no store uses.
    #[error("a secret_ref may not contain whitespace")]
    Whitespace,
    /// It had no `<scheme>:` prefix, so nothing says where to look.
    #[error("a secret_ref must start with a scheme, for example env:name#field")]
    NoScheme,
}

/// A resolved secret. `Debug`, `Display` and every error carrying it render
/// `<redacted>`; only [`SecretValue::expose`] returns the value, and its
/// callers are the request builder and nothing else.
#[derive(Clone)]
pub struct SecretValue(Arc<str>);

impl SecretValue {
    /// Take ownership of `value` and register it for scrubbing.
    pub fn new(value: impl AsRef<str>) -> Self {
        let value = value.as_ref();
        crate::redact::register(value);
        SecretValue(Arc::from(value))
    }

    /// The value. Every call site is a place a credential could leak, so this
    /// is deliberately not `Deref` and does not implement `Display`.
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// The length in bytes, which is safe to log.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the value is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretValue(<redacted>)")
    }
}

impl fmt::Display for SecretValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// Where a [`SecretRef`] is resolved.
///
/// A host implements this over its keychain or its credential broker (§39
/// §6). The crate ships [`MapSecretResolver`] for tests and local runs and
/// [`EnvSecretResolver`] for a development machine.
#[async_trait::async_trait]
pub trait SecretResolver: Send + Sync + fmt::Debug {
    /// The value behind `reference`, or why there is none.
    async fn resolve(&self, reference: &SecretRef) -> Result<SecretValue, BridgeError>;
}

/// A resolver over an in-memory map. For tests and single-process runs.
#[derive(Debug, Default)]
pub struct MapSecretResolver {
    values: BTreeMap<SecretRef, SecretValue>,
}

impl MapSecretResolver {
    /// An empty resolver.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one entry, builder style.
    pub fn with(mut self, reference: SecretRef, value: &str) -> Self {
        self.values.insert(reference, SecretValue::new(value));
        self
    }
}

#[async_trait::async_trait]
impl SecretResolver for MapSecretResolver {
    async fn resolve(&self, reference: &SecretRef) -> Result<SecretValue, BridgeError> {
        self.values
            .get(reference)
            .cloned()
            .ok_or_else(|| BridgeError::Secret {
                name: reference.to_string(),
                reason: "no such secret in this resolver".to_string(),
            })
    }
}

/// A resolver that reads `LOAMS_WEB_BRIDGE_SECRET_<NAME>` from the
/// environment, where `<NAME>` is the part of the reference after `env:`.
///
/// Development only: an environment variable is a poor place for a credential
/// in production. It exists so that a single-operator self-host can try the
/// remote provider without writing a keyring integration first.
#[derive(Debug, Clone)]
pub struct EnvSecretResolver {
    prefix: String,
}

impl EnvSecretResolver {
    /// The default prefix.
    pub const DEFAULT_PREFIX: &'static str = "LOAMS_WEB_BRIDGE_SECRET_";

    /// A resolver with the default prefix.
    pub fn new() -> Self {
        Self {
            prefix: Self::DEFAULT_PREFIX.to_string(),
        }
    }

    /// A resolver with a custom prefix.
    pub fn with_prefix(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }

    /// The variable a reference would read.
    pub fn variable_for(&self, reference: &SecretRef) -> String {
        let name = reference
            .as_str()
            .strip_prefix("env:")
            .unwrap_or(reference.as_str())
            .split('#')
            .next()
            .unwrap_or_default();
        format!(
            "{}{}",
            self.prefix,
            name.to_uppercase().replace(['-', '.', '/'], "_")
        )
    }
}

impl Default for EnvSecretResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl SecretResolver for EnvSecretResolver {
    async fn resolve(&self, reference: &SecretRef) -> Result<SecretValue, BridgeError> {
        let variable = self.variable_for(reference);
        match std::env::var(&variable) {
            Ok(value) if !value.is_empty() => Ok(SecretValue::new(value)),
            Ok(_) => Err(BridgeError::Secret {
                name: reference.to_string(),
                reason: format!("{variable} is empty"),
            }),
            Err(_) => Err(BridgeError::Secret {
                name: reference.to_string(),
                reason: format!("{variable} is not set"),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_secret_value_never_renders_its_content() {
        let value = SecretValue::new("cf-token-abcdef0123456789");
        assert_eq!(format!("{value}"), "<redacted>");
        assert_eq!(format!("{value:?}"), "SecretValue(<redacted>)");
        assert!(!format!("{value:?}{value}").contains("abcdef0123456789"));
    }

    #[test]
    fn a_reference_needs_a_scheme() {
        assert!(SecretRef::parse("cloudflare#api_token").is_err());
        assert!(SecretRef::parse("").is_err());
        assert!(SecretRef::parse("env:cloudflare#api_token").is_ok());
    }

    #[test]
    fn the_env_resolver_names_a_stable_variable() {
        let resolver = EnvSecretResolver::new();
        let reference = SecretRef::parse("env:cloudflare#api_token")
            .unwrap_or_else(|_| panic!("a well-formed reference parses"));
        assert_eq!(
            resolver.variable_for(&reference),
            "LOAMS_WEB_BRIDGE_SECRET_CLOUDFLARE"
        );
    }
}
