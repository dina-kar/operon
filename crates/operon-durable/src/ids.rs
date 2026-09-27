//! Operation ids (D1 Task 7, D146): `op-` and 26 characters.
//!
//! Without an idempotency key the 26 characters are a ULID. With one they are
//! the first 26 hex digits of SHA-256(namespace ‖ 0x00 ‖ key), so the same key
//! in the same namespace always names the same operation (§21 §6.7). The zero
//! byte keeps `("ab", "c")` and `("a", "bc")` apart; neither a namespace name
//! nor a header value can hold one. The id is also the operation's root
//! promise id and so its origin: it holds no `:` (T0-12) and no personal data
//! (D147).

use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Every operation id starts with this.
pub const OPERATION_PREFIX: &str = "op-";

/// The characters after the prefix.
const ID_LEN: usize = 26;

/// An operation id, which is also its root promise id.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct OperationId(String);

impl OperationId {
    /// A new id: `op-<ULID>`.
    pub fn generate() -> Self {
        Self(format!("{OPERATION_PREFIX}{}", ulid::Ulid::generate()))
    }

    /// The id an idempotency key maps to in `namespace` (D146).
    pub fn for_key(namespace: &str, key: &str) -> Self {
        let mut hash = Sha256::new();
        hash.update(namespace.as_bytes());
        hash.update([0u8]);
        hash.update(key.as_bytes());
        let hex = hex::encode(hash.finalize());
        Self(format!("{OPERATION_PREFIX}{}", &hex[..ID_LEN]))
    }

    /// `text` as an operation id, if it has the shape of one: `op-` and 26
    /// ASCII letters or digits.
    pub fn parse(text: &str) -> Option<Self> {
        let rest = text.strip_prefix(OPERATION_PREFIX)?;
        (rest.len() == ID_LEN && rest.bytes().all(|b| b.is_ascii_alphanumeric()))
            .then(|| Self(text.to_string()))
    }

    /// The id as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for OperationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for OperationId {
    type Error = String;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        Self::parse(&text).ok_or_else(|| format!("{text:?} is not an operation id"))
    }
}

impl From<OperationId> for String {
    fn from(id: OperationId) -> Self {
        id.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_have_the_documented_shape() {
        let generated = OperationId::generate();
        assert_eq!(generated.as_str().len(), 29);
        assert_eq!(OperationId::parse(generated.as_str()), Some(generated));

        let keyed = OperationId::for_key("default", "import-2026-09-27");
        assert_eq!(keyed, OperationId::for_key("default", "import-2026-09-27"));
        assert_eq!(keyed.as_str().len(), 29);
        assert!(keyed.as_str()[3..].bytes().all(|b| b.is_ascii_hexdigit()));
        assert_ne!(keyed, OperationId::for_key("other", "import-2026-09-27"));
        assert_ne!(
            OperationId::for_key("ab", "c"),
            OperationId::for_key("a", "bc")
        );

        for bad in [
            "",
            "op-",
            "op-short",
            "xx-01J9Z0000000000000000000000",
            "op-01J9Z000000000000000000000:1",
            "op-01J9Z00000000000000000000/",
        ] {
            assert_eq!(OperationId::parse(bad), None, "{bad}");
        }
    }
}
