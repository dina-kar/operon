//! The local, agent-driven provider: the person's own machine runs the browser.
//!
//! This is the primary path for credentialed work (D565's first rule). The
//! provider owns the parts that must be the same everywhere — the egress
//! policy, the credential policy, the snapshot, the uid scheme, redaction and
//! the artifact rules — and delegates the mechanics of moving a browser to a
//! [`PageDriver`], which the host implements over a webview (AP1b's Tauri host,
//! or a headless local engine).
//!
//! Splitting it this way keeps the risk in the part that can be tested in
//! milliseconds: the policy and the formatting, not the webview.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use url::Url;

use crate::artifact;
use crate::config::{CredentialPolicy, ProviderKind};
use crate::egress::EgressPolicy;
use crate::error::BridgeError;
use crate::page::{PageState, SnapshotCache, outcome};
use crate::provider::{BrowserProvider, Capabilities, OpenRequest, PageRef};
use crate::secret::{SecretResolver, SecretValue};
use crate::tool::{
    ActionOutcome, Extracted, FillValue, FindQuery, Navigation, Screenshot, Snapshot, SnapshotMode,
    SnapshotNode, SnapshotRequest, Uid, WaitCondition, WaitOutcome, WaitRequest,
};

/// What a driver is asked to do. Values that came from a secret are wrapped in
/// [`SecretValue`], whose `Debug` never renders the value.
#[derive(Debug)]
pub enum DriverAction {
    /// Go to a URL that already passed the egress policy.
    Navigate(Url),
    /// Click the element with this driver handle.
    Click {
        /// The driver's handle for the element.
        backend_id: i64,
    },
    /// Type into the element with this driver handle.
    Fill {
        /// The driver's handle for the element.
        backend_id: i64,
        /// The value, never logged.
        value: SecretValue,
    },
    /// Wait for a condition.
    Wait(WaitRequest),
    /// Capture the page.
    Screenshot,
    /// Read the page's visible text.
    Extract,
    /// Close the page.
    Close,
}

/// A page a driver owns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriverPage {
    id: String,
}

impl DriverPage {
    /// A page the driver will recognise.
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }

    /// The driver's id for it.
    pub fn id(&self) -> &str {
        &self.id
    }
}

/// The browser mechanics, supplied by the host.
#[async_trait]
pub trait PageDriver: Send + Sync + std::fmt::Debug {
    /// Open a page and return its first state.
    async fn open(&self, request: &OpenRequest) -> Result<(DriverPage, PageState), BridgeError>;

    /// Read the page's current state.
    async fn state(&self, page: &DriverPage) -> Result<PageState, BridgeError>;

    /// Do something, and return the state afterwards when the driver has one.
    async fn act(
        &self,
        page: &DriverPage,
        action: DriverAction,
    ) -> Result<Option<PageState>, BridgeError>;
}

#[derive(Debug, Clone)]
struct LocalPage {
    page: DriverPage,
    cache: SnapshotCache,
    state: PageState,
}

/// The local provider.
#[derive(Debug)]
pub struct LocalProvider {
    driver: Arc<dyn PageDriver>,
    egress: EgressPolicy,
    credentials: CredentialPolicy,
    secrets: Arc<dyn SecretResolver>,
    artifacts: PathBuf,
    pages: Mutex<HashMap<String, LocalPage>>,
    next_page: Mutex<u64>,
}

impl LocalProvider {
    /// Build a provider over `driver`.
    pub fn new(
        driver: Arc<dyn PageDriver>,
        egress: EgressPolicy,
        credentials: CredentialPolicy,
        secrets: Arc<dyn SecretResolver>,
        artifacts: PathBuf,
    ) -> Self {
        Self {
            driver,
            egress,
            credentials,
            secrets,
            artifacts,
            pages: Mutex::new(HashMap::new()),
            next_page: Mutex::new(1),
        }
    }

    fn page(&self, page: &PageRef) -> Result<LocalPage, BridgeError> {
        if page.provider() != ProviderKind::Local {
            return Err(BridgeError::policy(format!(
                "{page:?} belongs to the {} provider",
                page.provider().as_str()
            )));
        }
        let pages = self.pages.lock().unwrap_or_else(PoisonError::into_inner);
        pages.get(page.id()).cloned().ok_or_else(|| {
            BridgeError::unavailable(format!("page {} is not open; open it again", page.id()))
        })
    }

    fn store(&self, page: LocalPage) -> Result<PageRef, BridgeError> {
        let id = page.page.id().to_string();
        let mut pages = self.pages.lock().unwrap_or_else(PoisonError::into_inner);
        pages.insert(id.clone(), page);
        Ok(PageRef::new(ProviderKind::Local, id))
    }

    /// The last snapshot of a page, if one was taken.
    pub fn last_snapshot(&self, page: &PageRef) -> Option<Snapshot> {
        self.page(page)
            .ok()
            .and_then(|page| page.cache.latest().cloned())
    }

    fn allow_fill(&self, url: &str) -> Result<(), BridgeError> {
        if !self.credentials.allow_secret_fills {
            return Err(BridgeError::policy(
                "secret fills are off; set credentials.allow-secret-fills, and remember that \
                 they are always off on the remote provider",
            ));
        }
        let host = Url::parse(url)
            .ok()
            .and_then(|url| url.host_str().map(str::to_string))
            .unwrap_or_default();
        if !self.credentials.allows_fill_on(&host) {
            return Err(BridgeError::policy(format!(
                "{host} is not in credentials.allowed-fill-hosts"
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl BrowserProvider for LocalProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Local
    }

    fn engine(&self) -> Option<crate::config::Engine> {
        None
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            core: true,
            screenshots: true,
            extraction: true,
            // The browser is on the person's machine and keeps its profile, so
            // a signed-in session survives (D506).
            authenticated_sessions: true,
            // No server-side enforcement: the allowlist is enforced at the
            // driver's navigation hook and at the tool, not by a third party.
            guardrails: false,
            tabs: true,
            credentials_stay_local: true,
        }
    }

    async fn open(&self, request: OpenRequest) -> Result<PageRef, BridgeError> {
        if let Some(url) = &request.url {
            self.egress.check(url)?;
        }
        let (page, state) = self.driver.open(&request).await?;
        let mut next = self
            .next_page
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *next += 1;
        drop(next);
        self.store(LocalPage {
            page,
            cache: SnapshotCache::new(),
            state,
        })
    }

    async fn navigate(&self, page: &PageRef, url: &Url) -> Result<Navigation, BridgeError> {
        self.egress.check(url)?;
        let mut local = self.page(page)?;
        let after = self
            .driver
            .act(&local.page, DriverAction::Navigate(url.clone()))
            .await?
            .unwrap_or(local.state.clone());
        local.cache.build(&after, false);
        let navigation = Navigation {
            url: crate::tool::redact_url(&after.url),
            title: after.title.clone(),
            change: local.cache.change(),
        };
        local.state = after;
        self.store(local)?;
        Ok(navigation)
    }

    async fn snapshot(
        &self,
        page: &PageRef,
        request: &SnapshotRequest,
    ) -> Result<Snapshot, BridgeError> {
        let mut local = self.page(page)?;
        let state = self.driver.state(&local.page).await?;
        let snapshot = local.cache.build(&state, request.verbose);
        local.state = state;
        self.store(local)?;
        Ok(snapshot)
    }

    async fn find(
        &self,
        page: &PageRef,
        query: &FindQuery,
    ) -> Result<Vec<SnapshotNode>, BridgeError> {
        let local = self.page(page)?;
        local.cache.find(query)
    }

    async fn click(&self, page: &PageRef, uid: &Uid) -> Result<ActionOutcome, BridgeError> {
        let mut local = self.page(page)?;
        let node = local.cache.resolve(uid)?;
        let after = self
            .driver
            .act(
                &local.page,
                DriverAction::Click {
                    backend_id: node.backend_id,
                },
            )
            .await?;
        if let Some(state) = after {
            local.cache.build(&state, false);
            local.state = state;
        }
        let answer = outcome(
            format!("clicked {uid} ({})", node.role),
            &local.cache,
            SnapshotMode::Diff,
            &SnapshotRequest::new(),
        );
        self.store(local)?;
        Ok(answer)
    }

    async fn fill(
        &self,
        page: &PageRef,
        uid: &Uid,
        value: &FillValue,
    ) -> Result<ActionOutcome, BridgeError> {
        let mut local = self.page(page)?;
        let node = local.cache.resolve(uid)?;
        let secret = match value {
            FillValue::Literal(literal) => SecretValue::new(literal),
            FillValue::Secret(reference) => {
                self.allow_fill(&local.state.url)?;
                self.secrets.resolve(reference).await?
            }
        };
        // The value is registered for scrubbing the moment it exists, so a
        // driver error that quotes it cannot leak it.
        crate::redact::register(secret.expose());
        let after = self
            .driver
            .act(
                &local.page,
                DriverAction::Fill {
                    backend_id: node.backend_id,
                    value: secret,
                },
            )
            .await?;
        if let Some(state) = after {
            local.cache.build(&state, false);
            local.state = state;
        }
        let answer = outcome(
            format!("filled {uid} ({})", node.role),
            &local.cache,
            SnapshotMode::Diff,
            &SnapshotRequest::new(),
        );
        self.store(local)?;
        Ok(answer)
    }

    async fn wait_for(
        &self,
        page: &PageRef,
        request: &WaitRequest,
    ) -> Result<WaitOutcome, BridgeError> {
        if let WaitCondition::Selector(selector) = &request.condition {
            return Err(BridgeError::Unsupported {
                engine: "local",
                what: format!(
                    "a selector wait ({selector}) needs a driver that can query CSS; wait for text \
                     or a url fragment instead"
                ),
            });
        }
        let local = self.page(page)?;
        let after = self
            .driver
            .act(&local.page, DriverAction::Wait(request.clone()))
            .await?
            .unwrap_or(local.state.clone());
        let matched = condition_holds(&request.condition, &after);
        Ok(WaitOutcome {
            matched,
            waited_ms: waited(request, matched),
            note: if matched {
                String::new()
            } else {
                "the condition did not hold before the timeout; take_snapshot to see the page"
                    .to_string()
            },
        })
    }

    async fn screenshot(&self, page: &PageRef) -> Result<Screenshot, BridgeError> {
        let local = self.page(page)?;
        let after = self
            .driver
            .act(&local.page, DriverAction::Screenshot)
            .await?
            .unwrap_or(local.state.clone());
        let (bytes, extension) = after
            .screenshot
            .clone()
            .ok_or_else(|| BridgeError::unavailable("the driver returned no screenshot"))?;
        artifact::write(&self.artifacts, "screenshot", extension, &bytes)
    }

    async fn extract(&self, page: &PageRef) -> Result<Extracted, BridgeError> {
        let local = self.page(page)?;
        let state = self
            .driver
            .act(&local.page, DriverAction::Extract)
            .await?
            .unwrap_or(local.state.clone());
        Ok(crate::artifact::extracted(
            &state,
            SnapshotRequest::new().budget(),
        ))
    }

    async fn close(&self, page: &PageRef) -> Result<(), BridgeError> {
        let Some(local) = self.page(page).ok() else {
            return Ok(());
        };
        self.pages
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(page.id());
        self.driver.act(&local.page, DriverAction::Close).await?;
        Ok(())
    }
}

/// How long a wait is reported to have taken when the driver does not say.
fn waited(request: &WaitRequest, matched: bool) -> u64 {
    if matched {
        request.poll_interval_ms
    } else {
        request.timeout_ms
    }
}

/// Whether a condition holds for a page state.
pub(crate) fn condition_holds(condition: &WaitCondition, state: &PageState) -> bool {
    match condition {
        WaitCondition::Text(needle) => crate::tool::flatten(&state.nodes).iter().any(|node| {
            node.name.contains(needle.as_str())
                || node
                    .value
                    .as_deref()
                    .is_some_and(|value| value.contains(needle.as_str()))
        }),
        // A selector needs a driver query; without one the wait cannot lie and
        // claim success.
        WaitCondition::Selector(_) => false,
        WaitCondition::Url(needle) => state.url.contains(needle.as_str()),
        WaitCondition::NetworkIdle => true,
    }
}
