//! The provider's error type. Every message that can carry a provider or page
//! string is scrubbed on the way in, so a credential cannot reach a log line, a
//! tool result or an error chain (D567, AP1c gate: a secret never appears in
//! any output).

use crate::egress::EgressError;
use crate::redact;

/// Why a call against a browser provider failed.
///
/// Every variant is `#[non_exhaustive]`: hosts add their own.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BridgeError {
    /// The configuration is not usable (a missing account, an engine and
    /// option combination Cloudflare rejects).
    #[error("configuration: {0}")]
    Config(#[from] ConfigError),

    /// A URL the policy refuses: off the allowlist, or a private address.
    #[error("navigation blocked by the egress policy: {0}")]
    EgressDenied(#[from] EgressError),

    /// The policy refuses the call for a reason that is not a URL (a credential
    /// fill on the remote provider, a capability the engine has not got).
    #[error("refused by policy: {0}")]
    Policy(String),

    /// The agent acted on a uid from an older snapshot.
    #[error(
        "uid {uid} belongs to snapshot {seen}, but the page's latest is {latest}; call take_snapshot first"
    )]
    StaleSnapshot {
        /// The uid the caller used.
        uid: String,
        /// The snapshot the uid came from.
        seen: String,
        /// The page's current snapshot.
        latest: String,
    },

    /// The browser refused a Chrome DevTools Protocol command.
    #[error("cdp: {0}")]
    Cdp(String),

    /// The connection to the browser could not be made or was lost.
    #[error("transport: {0}")]
    Transport(String),

    /// A call did not finish inside its budget.
    #[error("timed out after {0} ms")]
    Timeout(u64),

    /// The configured engine cannot do this (Kitesurf has no `keep_alive`, no
    /// guardrails, no tabs).
    #[error("unsupported by the {engine} engine: {what}")]
    Unsupported {
        /// The engine the call was made against.
        engine: &'static str,
        /// What it cannot do.
        what: String,
    },

    /// A secret could not be resolved.
    #[error("secret {name}: {reason}")]
    Secret {
        /// The `secret_ref`, never its value.
        name: String,
        /// Why it could not be resolved.
        reason: String,
    },

    /// More browser sessions were asked for than the account's plan allows.
    #[error(
        "at most {max} browser sessions may be open at once; close one or raise max_concurrent_sessions"
    )]
    SessionLimit {
        /// The configured ceiling.
        max: usize,
    },

    /// The configured daily browser budget is used up.
    #[error("the daily browser budget of {budget_ms} ms is spent; it resets at 00:00 UTC")]
    DailyBudget {
        /// The configured daily budget, in milliseconds.
        budget_ms: u64,
    },

    /// The engine is not available to this account.
    #[error("{0}")]
    Unavailable(String),

    /// Local filesystem or encoding trouble.
    #[error("io: {0}")]
    Io(String),
}

impl BridgeError {
    /// A `cdp` error whose message is scrubbed.
    pub fn cdp(message: impl AsRef<str>) -> Self {
        BridgeError::Cdp(redact::scrub(message.as_ref()))
    }

    /// A `transport` error whose message is scrubbed.
    pub fn transport(message: impl AsRef<str>) -> Self {
        BridgeError::Transport(redact::scrub(message.as_ref()))
    }

    /// A `policy` refusal whose message is scrubbed.
    pub fn policy(message: impl AsRef<str>) -> Self {
        BridgeError::Policy(redact::scrub(message.as_ref()))
    }

    /// An `unavailable` error whose message is scrubbed.
    pub fn unavailable(message: impl AsRef<str>) -> Self {
        BridgeError::Unavailable(redact::scrub(message.as_ref()))
    }

    /// The engine this error came from, when it names one.
    pub fn engine(&self) -> Option<&'static str> {
        match self {
            BridgeError::Unsupported { engine, .. } => Some(engine),
            _ => None,
        }
    }
}

/// Why a configuration was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// A required field was empty.
    #[error("{field} is required")]
    Missing {
        /// The field's name.
        field: &'static str,
    },

    /// A field's value is not the shape the provider needs.
    #[error("{field}: {reason}")]
    Invalid {
        /// The field's name.
        field: &'static str,
        /// What is wrong with it.
        reason: String,
    },

    /// A combination Cloudflare's own documentation rejects.
    #[error("{field}: {reason}")]
    Unsupported {
        /// The field that causes it.
        field: &'static str,
        /// Why the combination cannot be sent.
        reason: String,
    },

    /// The TOML could not be read.
    #[error("toml: {0}")]
    Toml(String),
}

impl From<std::io::Error> for BridgeError {
    fn from(error: std::io::Error) -> Self {
        BridgeError::Io(redact::scrub(&error.to_string()))
    }
}
