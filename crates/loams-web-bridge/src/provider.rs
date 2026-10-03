//! The provider abstraction: one tool contract, two implementations.
//!
//! D565's choice is a configuration switch, not a fork: credentialed work goes
//! through the client's own browser ([`crate::local::LocalProvider`]), and
//! unattended public-web work goes to Cloudflare Browser Run
//! ([`crate::browser_run::BrowserRunProvider`]). Both answer the same trait, so
//! a caller (the bridge's MCP server, the Factory's agents) does not know which
//! one it is holding.

use std::fmt;

use url::Url;

use crate::config::{Engine, ProviderKind};
use crate::error::BridgeError;
use crate::tool::{
    ActionOutcome, Extracted, FillValue, FindQuery, Navigation, Screenshot, Snapshot,
    SnapshotRequest, Uid, WaitOutcome, WaitRequest,
};

/// A handle to one open page.
///
/// Opaque: it carries the provider and an id the provider resolves against its
/// own state, so nothing a caller can print reveals a session token or a
/// backend node id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageRef {
    provider: ProviderKind,
    id: String,
}

impl PageRef {
    /// A handle for `id` on `provider`.
    pub fn new(provider: ProviderKind, id: impl Into<String>) -> Self {
        Self {
            provider,
            id: id.into(),
        }
    }

    /// Which provider owns it.
    pub fn provider(&self) -> ProviderKind {
        self.provider
    }

    /// The provider's own id for the page.
    pub fn id(&self) -> &str {
        &self.id
    }
}

/// What opening a session asked for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OpenRequest {
    /// Where to go first. `None` opens a blank page.
    pub url: Option<Url>,
    /// The profile to use, for providers that have profiles.
    pub profile: Option<String>,
}

impl OpenRequest {
    /// Open a blank page.
    pub fn blank() -> Self {
        Self::default()
    }

    /// Open at `url`.
    pub fn at(url: Url) -> Self {
        Self {
            url: Some(url),
            profile: None,
        }
    }

    /// In `profile`.
    pub fn in_profile(mut self, profile: impl Into<String>) -> Self {
        self.profile = Some(profile.into());
        self
    }
}

/// What a provider can do, so a caller can degrade instead of guessing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Capabilities {
    /// Snapshots, find, click, fill, wait, screenshot, extraction.
    pub core: bool,
    /// Screenshots (Kitesurf renders, but not pixel-perfectly).
    pub screenshots: bool,
    /// Content extraction, the operation Kitesurf is cheapest for.
    pub extraction: bool,
    /// A session that can be signed in and keep that state.
    pub authenticated_sessions: bool,
    /// Server-side hostname enforcement for the whole session.
    pub guardrails: bool,
    /// Tabs beyond the first.
    pub tabs: bool,
    /// The caller's own machine holds the credentials.
    pub credentials_stay_local: bool,
}

/// A browser a provider drives.
///
/// Implementations are `Send + Sync` and are held behind an `Arc` by the host,
/// which is what makes the local and the remote provider swappable at runtime.
#[async_trait::async_trait]
pub trait BrowserProvider: Send + Sync + fmt::Debug {
    /// Which provider this is.
    fn kind(&self) -> ProviderKind;

    /// The engine behind it, when it has one.
    fn engine(&self) -> Option<Engine>;

    /// What it can do.
    fn capabilities(&self) -> Capabilities;

    /// Open a page.
    async fn open(&self, request: OpenRequest) -> Result<PageRef, BridgeError>;

    /// Navigate to `url`, after the egress policy has accepted it.
    async fn navigate(&self, page: &PageRef, url: &Url) -> Result<Navigation, BridgeError>;

    /// Read the page as text with uids.
    async fn snapshot(
        &self,
        page: &PageRef,
        request: &SnapshotRequest,
    ) -> Result<Snapshot, BridgeError>;

    /// Search the last snapshot instead of returning the page.
    async fn find(
        &self,
        page: &PageRef,
        query: &FindQuery,
    ) -> Result<Vec<crate::tool::SnapshotNode>, BridgeError>;

    /// Click an element by uid.
    async fn click(&self, page: &PageRef, uid: &Uid) -> Result<ActionOutcome, BridgeError>;

    /// Type into an element by uid. A [`FillValue::Secret`] is resolved by the
    /// provider and never appears in the answer.
    async fn fill(
        &self,
        page: &PageRef,
        uid: &Uid,
        value: &FillValue,
    ) -> Result<ActionOutcome, BridgeError>;

    /// Wait for text, a selector, a URL or network idle.
    async fn wait_for(
        &self,
        page: &PageRef,
        request: &WaitRequest,
    ) -> Result<WaitOutcome, BridgeError>;

    /// Write a screenshot to the artifact directory and describe it.
    async fn screenshot(&self, page: &PageRef) -> Result<Screenshot, BridgeError>;

    /// Extract the page's visible text.
    async fn extract(&self, page: &PageRef) -> Result<Extracted, BridgeError>;

    /// Close the page. Closing an unknown or already closed handle is not an
    /// error, so a caller can always clean up.
    async fn close(&self, page: &PageRef) -> Result<(), BridgeError>;
}
