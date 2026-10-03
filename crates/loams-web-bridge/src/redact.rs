//! Credential scrubbing for every string that can leave the process (an error
//! message, a `tracing` field, a `Debug` rendering).
//!
//! Two layers:
//!
//! 1. **Registered values.** Every secret the provider resolves goes through
//!    [`register`], which remembers it for the life of the process so that any
//!    later string carrying it is rewritten to `<redacted>`. Nothing is ever
//!    written to disk or to a log; the list lives in memory and is dropped when
//!    the process exits.
//! 2. **Shape rules.** Values we never saw are still caught: bearer headers,
//!    `key = value` pairs with a credential-shaped name, JWTs and long
//!    base64 or hex runs.
//!
//! The rules are deliberately greedy. An error message that loses a base64
//! payload is a cosmetic problem; one that leaks a token is not.

use std::sync::{Arc, LazyLock, Mutex, PoisonError};

use regex::Regex;

/// The placeholder every scrubbed value becomes.
pub const REDACTED: &str = "<redacted>";

/// Registered secret values. Short values are ignored: scrubbing a two-byte
/// string would mangle unrelated output.
static REGISTERED: LazyLock<Mutex<Vec<Arc<str>>>> = LazyLock::new(|| Mutex::new(Vec::new()));

/// The minimum length of a registered value that [`scrub`] will look for.
const MIN_REGISTERED_LEN: usize = 6;

/// Patterns applied to every string, in order.
struct Shapes {
    authorization: Regex,
    bearer: Regex,
    assignment: Regex,
    jwt: Regex,
    long_base64: Regex,
    long_hex: Regex,
}

fn shapes() -> &'static Shapes {
    static SHAPES: LazyLock<Shapes> = LazyLock::new(|| {
        Shapes {
        // `Authorization: Bearer cf-abc...`, with or without JSON quoting.
        authorization: Regex::new(r#"(?i)"?(authorization)"?\s*[:=]\s*"?(?:bearer|basic|token)\s+\S+"#)
            .expect("authorization pattern"),
        bearer: Regex::new(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}").expect("bearer pattern"),
        // `api_key=…`, `"password": "…"` and friends.
        assignment: Regex::new(
            r#"(?i)((?:api[_-]?key|access[_-]?key|secret[_-]?key|client[_-]?secret|token|password|passwd|pwd|passphrase)\s*"?\s*[:=]\s*"?)([^"\s,&}]{4,})"#,
        )
        .expect("assignment pattern"),
        jwt: Regex::new(r"\beyJ[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}\b")
            .expect("jwt pattern"),
        // A base64 or hex blob long enough to be a key rather than a word.
        long_base64: Regex::new(r"\b[A-Za-z0-9+/_-]{40,}={0,2}\b").expect("base64 pattern"),
        long_hex: Regex::new(r"\b[a-fA-F0-9]{40,}\b").expect("hex pattern"),
    }
    });
    &SHAPES
}

/// Remember `value` so that [`scrub`] rewrites it everywhere.
///
/// Called for every secret the provider resolves, before the value is put on
/// the wire. Registering the same value twice is harmless.
pub fn register(value: &str) {
    if value.len() < MIN_REGISTERED_LEN {
        return;
    }
    let mut registered = REGISTERED.lock().unwrap_or_else(PoisonError::into_inner);
    if !registered.iter().any(|seen| seen.as_ref() == value) {
        registered.push(Arc::from(value));
    }
}

/// Whether `value` looks like something worth scrubbing. Used by the tests and
/// by hosts that want to pre-screen their own output.
pub fn is_registered(value: &str) -> bool {
    REGISTERED
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .any(|seen| seen.as_ref() == value)
}

/// Replace every registered secret and every credential-shaped run in `input`
/// with [`REDACTED`].
pub fn scrub(input: &str) -> String {
    let registered: Vec<Arc<str>> = REGISTERED
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .filter(|seen| seen.len() >= MIN_REGISTERED_LEN)
        .cloned()
        .collect();

    let mut out: String = input.to_string();
    for secret in registered {
        if out.contains(secret.as_ref()) {
            out = out.replace(secret.as_ref(), REDACTED);
        }
    }

    let shapes = shapes();
    for (pattern, replacement) in [
        (
            &shapes.authorization,
            "authorization: <redacted>".to_string(),
        ),
        (&shapes.bearer, "$1 <redacted>".to_string()),
        (&shapes.assignment, format!("$1{REDACTED}")),
        (&shapes.jwt, REDACTED.to_string()),
        (&shapes.long_base64, REDACTED.to_string()),
        (&shapes.long_hex, REDACTED.to_string()),
    ] {
        out = pattern.replace_all(&out, replacement.as_str()).into_owned();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_registered_value_is_rewritten_anywhere_it_appears() {
        register("cf-super-secret-token-value");
        assert_eq!(
            scrub("connect failed with cf-super-secret-token-value"),
            "connect failed with <redacted>"
        );
    }

    #[test]
    fn an_unregistered_bearer_header_is_still_scrubbed() {
        let out = scrub("Authorization: Bearer abcdef0123456789abcdef");
        assert!(!out.contains("abcdef0123456789abcdef"), "{out}");
        assert!(out.contains(REDACTED), "{out}");
    }

    #[test]
    fn credential_shaped_assignments_are_scrubbed() {
        for input in [
            "https://x.test/?api_key=sk-1234567890abcdef",
            r#"{"password": "hunter2hunter2"}"#,
            "client_secret: abcdefghijklmnop",
        ] {
            let out = scrub(input);
            assert!(!out.contains("hunter2"), "{out}");
            assert!(out.contains(REDACTED), "{out}");
        }
    }

    #[test]
    fn ordinary_output_survives() {
        let out = scrub("navigated to https://example.com/pricing (200)");
        assert_eq!(out, "navigated to https://example.com/pricing (200)");
    }
}
