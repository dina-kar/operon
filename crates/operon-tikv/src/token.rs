//! Commit tokens (R1 plan Task 2 semantics 2–3; design §20 §11.2).
//!
//! With [`TxnOptions::commit_token`](crate::TxnOptions::commit_token) the
//! runner writes `root ‖ t/ ‖ token` (16 random bytes) in the transaction.
//! When a commit's outcome is unknown, a read of that key at a fresh timestamp
//! tells whether it committed: TiKV resolves a leftover lock (committing or
//! rolling back the transaction) before it answers. The value is `expires_ms`
//! (u64 big-endian), 30 minutes after the transaction's start; a resolver
//! that finds the token absent writes a fence (`expires_ms ‖ "F"`) there, so
//! the lost commit can no longer apply. Task 3's GC loop deletes expired
//! tokens and fences.

use std::time::Duration;

/// The key prefix of commit tokens, under the handle's root.
pub const TOKEN_PREFIX: &[u8] = b"t/";

/// How long a token is kept.
pub const TOKEN_TTL: Duration = Duration::from_secs(30 * 60);

/// A commit token.
pub type Token = [u8; 16];

/// A new random token.
pub fn new_token() -> Token {
    rand::random()
}

/// The key of `token`, relative to the handle's root.
pub fn token_key(token: &Token) -> Vec<u8> {
    let mut key = Vec::with_capacity(TOKEN_PREFIX.len() + token.len());
    key.extend_from_slice(TOKEN_PREFIX);
    key.extend_from_slice(token);
    key
}

/// The value of a token written at physical time `now_ms`.
pub fn token_value(now_ms: u64) -> Vec<u8> {
    let ttl = u64::try_from(TOKEN_TTL.as_millis()).unwrap_or(u64::MAX);
    now_ms.saturating_add(ttl).to_be_bytes().to_vec()
}

/// The fence a resolver writes at an absent token at physical time `now_ms`:
/// `expires_ms ‖ "F"`. It makes a late request of the lost commit conflict.
pub fn fence_value(now_ms: u64) -> Vec<u8> {
    let mut v = token_value(now_ms);
    v.push(b'F');
    v
}

/// Whether a token value is a resolver's fence rather than a commit's token.
pub fn is_fence(value: &[u8]) -> bool {
    value.len() == 9 && value[8] == b'F'
}

/// The `expires_ms` of a token or fence value, or `None` when it is neither.
pub fn token_expiry(value: &[u8]) -> Option<u64> {
    value
        .get(..8)
        .filter(|_| value.len() == 8 || is_fence(value))
        .and_then(|b| b.try_into().ok())
        .map(u64::from_be_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_carries_the_expiry() {
        let v = token_value(1_000);
        assert_eq!(token_expiry(&v), Some(1_000 + 30 * 60 * 1000));
        assert_eq!(token_expiry(b"short"), None);
        let f = fence_value(1_000);
        assert!(is_fence(&f) && !is_fence(&v));
        assert_eq!(token_expiry(&f), token_expiry(&v));
        let t = new_token();
        let k = token_key(&t);
        assert!(k.starts_with(TOKEN_PREFIX));
        assert_eq!(&k[2..], &t);
    }
}
