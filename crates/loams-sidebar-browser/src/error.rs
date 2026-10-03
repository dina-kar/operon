//! Errors from the sidebar browser.
//!
//! Variants carry the CDP method or the file involved, because the two failure
//! modes this crate has to be debuggable about are "the engine does not
//! implement this" and "the profile directory is not what we wrote".

use std::path::PathBuf;

/// A CDP call returned an error, or the transport failed.
#[derive(Debug, thiserror::Error)]
pub enum SidebarBrowserError {
    /// The WebSocket transport to the engine failed.
    #[error("cdp transport: {0}")]
    Transport(String),

    /// The engine answered a command with an error object.
    ///
    /// `method` is the CDP method that failed and `message` is the engine's
    /// own text. Obscura's text is the only way to tell "you need a
    /// render-enabled build" apart from "no such target", so it is preserved
    /// verbatim rather than flattened.
    #[error("cdp call {method} failed: {message}")]
    Call {
        /// The CDP method that failed.
        method: String,
        /// The engine's error text, verbatim.
        message: String,
    },

    /// No response arrived for a command id within the call timeout.
    #[error("cdp call {method} timed out after {elapsed_ms}ms")]
    Timeout {
        /// The CDP method that timed out.
        method: String,
        /// How long the call waited.
        elapsed_ms: u64,
    },

    /// The engine binary could not be found or started.
    #[error("engine process: {0}")]
    Engine(String),

    /// The engine on disk is not the version this crate was written against.
    #[error(
        "engine version mismatch: expected {expected}, found {found}. \
         Obscura's CDP subset changes between releases; the sidebar browser is \
         pinned to {expected}."
    )]
    EngineVersion {
        /// The version this crate is written against.
        expected: String,
        /// The version the binary reported.
        found: String,
    },

    /// A profile key or URL was rejected before anything was launched.
    #[error("{0}")]
    Rejected(String),

    /// A URL is not inside the embedded origin's navigation allowlist.
    #[error("navigation to {url} is outside the allowlist for {origin}")]
    NavigationRefused {
        /// The embedded origin the allowlist is built from.
        origin: String,
        /// The refused URL.
        url: String,
    },

    /// The network policy refuses the target outright, regardless of the
    /// engine's own private-network switch.
    #[error("network target refused by Loams: {reason}")]
    NetworkRefused {
        /// Why Loams refuses it, in plain words.
        reason: String,
    },

    /// A frame could not be turned into something the panel can draw.
    #[error("frame decode: {0}")]
    Frame(String),

    /// A profile-directory read or write failed.
    #[error("profile {path}: {source}")]
    ProfileIo {
        /// The file being read or written.
        path: PathBuf,
        /// The underlying error.
        source: std::io::Error,
    },

    /// The credential boundary was violated: something that is not an embedded
    /// app's scoped session cookie reached the engine.
    ///
    /// This is the SF1 Global Constraint "embeds never hold a Loams token"
    /// ([`crate::boundary`]), and it is an error rather than a warning
    /// because there is no safe way to continue once it has happened.
    #[error("credential boundary violated: {0}")]
    BoundaryViolation(String),
}

impl SidebarBrowserError {
    /// Attach a path to an [`std::io::Error`] for [`SidebarBrowserError::ProfileIo`].
    pub fn profile_io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::ProfileIo {
            path: path.into(),
            source,
        }
    }
}

/// The crate's result type.
pub type Result<T, E = SidebarBrowserError> = std::result::Result<T, E>;
