//! The remote provider: Cloudflare Browser Run, over its CDP endpoint (D566).
//!
//! What it is for, and what it is not for (D565, D567, and Cloudflare's own
//! Kitesurf documentation read on 2026-10-03):
//!
//! * **Unattended public-web work** — a screenshot, a scrape, a content
//!   extraction, a check of a page an agent needs to read. The browser lives in
//!   Cloudflare's account, so no person's machine is involved and no personal
//!   credential is on it.
//! * **Not credentialed work.** A remote browser is a third party: it takes no
//!   user-credential `secret_ref` fill, it keeps no persistent profile, and a
//!   service-account fill needs an allow-listed host (D567).
//! * **Chromium by default.** Kitesurf is a stateless Workers isolate: cheaper
//!   and faster for a throwaway render, but Cloudflare states it cannot start
//!   "a long-running, authenticated session that requires persistent state" and
//!   cannot negotiate a bot-challenge TLS fingerprint. It also has no
//!   guardrails, so the provider refuses to run it under a hostname allow list
//!   unless the operator opts in.
//!
//! This is implemented against Cloudflare's documented interface. It has **not**
//! been run against a live account; `docs/remote-browser-provider.md` lists the
//! points that are unverified.

pub mod ax;
pub mod budget;
pub mod cdp;
pub mod endpoint;
pub mod ws;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use url::Url;

use crate::artifact;
use crate::browser_run::budget::BrowserBudget;
use crate::browser_run::cdp::{CdpClient, CdpTransport};
use crate::browser_run::endpoint::RequestHeaders;
use crate::config::{BrowserRunConfig, CredentialPolicy, Engine, ProviderKind};
use crate::egress::EgressPolicy;
use crate::error::BridgeError;
use crate::page::{PageState, SnapshotCache, outcome};
use crate::provider::{BrowserProvider, Capabilities, OpenRequest, PageRef};
use crate::secret::{SecretResolver, SecretValue};
use crate::tool::{
    ActionOutcome, Extracted, FillValue, FindQuery, Navigation, Screenshot, Snapshot, SnapshotMode,
    SnapshotNode, SnapshotRequest, Uid, WaitCondition, WaitOutcome, WaitRequest,
};

/// The script that reads the page's URL, title and visible text in one call.
const READ_PAGE: &str = "JSON.stringify({u: location.href, t: document.title, \
                          x: document.body ? document.body.innerText : ''})";

/// The script that clicks a resolved element.
const CLICK_ELEMENT: &str = "function() { this.scrollIntoView({block: 'center'}); this.click(); \
                             return true; }";

/// The script that types into a resolved element the way a framework expects:
/// through the prototype's setter, then `input` and `change`.
const FILL_ELEMENT: &str = "function(v) { const el = this; const proto = \
                             el instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype : \
                             HTMLInputElement.prototype; const setter = \
                             Object.getOwnPropertyDescriptor(proto, 'value').set; \
                             setter.call(el, v); el.dispatchEvent(new Event('input', \
                             {bubbles: true})); el.dispatchEvent(new Event('change', \
                             {bubbles: true})); return true; }";

/// One open remote page.
#[derive(Debug)]
struct PageSlot {
    client: Arc<CdpClient>,
    session_id: String,
    target_id: String,
    opened_at: Instant,
    /// The concurrency permit, held for as long as the session is open.
    _permit: OwnedSemaphorePermit,
    state: Mutex<PageState>,
    cache: Mutex<SnapshotCache>,
}

/// The Browser Run provider.
#[derive(Debug)]
pub struct BrowserRunProvider {
    config: BrowserRunConfig,
    engine: Engine,
    egress: EgressPolicy,
    credentials: CredentialPolicy,
    secrets: Arc<dyn SecretResolver>,
    transport: Arc<dyn CdpTransport>,
    artifacts: PathBuf,
    budget: Arc<BrowserBudget>,
    permits: Arc<Semaphore>,
    next_handle: AtomicU64,
    last_start: Mutex<Option<Instant>>,
    pages: Mutex<HashMap<String, Arc<PageSlot>>>,
}

impl BrowserRunProvider {
    /// Build a provider, refusing the configurations Browser Run itself refuses
    /// and the combination it cannot police (Kitesurf under an allow list).
    pub fn new(
        config: BrowserRunConfig,
        egress: EgressPolicy,
        credentials: CredentialPolicy,
        secrets: Arc<dyn SecretResolver>,
        transport: Arc<dyn CdpTransport>,
    ) -> Result<Self, BridgeError> {
        let engine = config.engine;
        endpoint::check_endpoint(engine, config.keep_alive())?;

        if let Some(guardrails) = egress.guardrails() {
            if engine != Engine::Chromium && !egress.allows_unenforced_engine() {
                return Err(BridgeError::Unsupported {
                    engine: engine.as_str(),
                    what: "Browser Run does not support session guardrails on this engine, so the \
                           hostname allow list could not be enforced for the session's \
                           sub-resources and redirects"
                        .to_string(),
                });
            }
            endpoint::check_guardrails(&guardrails)?;
        }

        let max_concurrent = config.max_concurrent_sessions();
        let budget = Arc::new(match config.daily_budget() {
            Some(limit) => BrowserBudget::new(u64::try_from(limit.as_millis()).unwrap_or(u64::MAX)),
            None => BrowserBudget::unlimited(),
        });

        Ok(Self {
            artifacts: config.artifacts_dir(),
            engine,
            egress,
            credentials,
            secrets,
            transport,
            budget,
            permits: Arc::new(Semaphore::new(max_concurrent)),
            next_handle: AtomicU64::new(1),
            last_start: Mutex::new(None),
            pages: Mutex::new(HashMap::new()),
            config,
        })
    }

    /// The engine this provider asks for.
    pub fn engine(&self) -> Engine {
        self.engine
    }

    /// The account it uses. An account id is not a secret, and a host shows it
    /// to say which bill a session lands on.
    pub fn account_id(&self) -> &str {
        &self.config.account_id
    }

    /// The local budget guard, so a host can show what has been spent today.
    pub fn budget(&self) -> &BrowserBudget {
        &self.budget
    }

    fn slot(&self, page: &PageRef) -> Result<Arc<PageSlot>, BridgeError> {
        if page.provider() != ProviderKind::BrowserRun {
            return Err(BridgeError::policy(format!(
                "this handle belongs to the {} provider",
                page.provider().as_str()
            )));
        }
        self.pages
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(page.id())
            .cloned()
            .ok_or_else(|| {
                BridgeError::unavailable(format!(
                    "session {} is not open; Browser Run closes idle sessions, so open one again",
                    page.id()
                ))
            })
    }

    fn insert(&self, slot: PageSlot) -> PageRef {
        // The handle is ours, not the browser's: two sessions that reported
        // the same target id must not share a map entry (and so must not share
        // a concurrency permit).
        let id = format!(
            "remote-{}-{}",
            slot.target_id,
            self.next_handle.fetch_add(1, Ordering::Relaxed)
        );
        let handle = PageRef::new(ProviderKind::BrowserRun, id.clone());
        self.pages
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(id, Arc::new(slot));
        handle
    }

    /// The endpoint and handshake headers of one session.
    fn handshake(&self, token: &SecretValue) -> (String, RequestHeaders) {
        let url = endpoint::websocket_endpoint(
            &self.config.account_id,
            self.engine,
            self.config.keep_alive(),
        );
        (
            url,
            endpoint::headers(token, self.egress.guardrails().as_ref()),
        )
    }

    async fn token(&self) -> Result<SecretValue, BridgeError> {
        let reference = self
            .config
            .token_ref()?
            .ok_or_else(|| BridgeError::Secret {
                name: "browser_run.token_secret_ref".to_string(),
                reason: "no token reference is configured".to_string(),
            })?;
        self.secrets.resolve(&reference).await
    }

    /// Wait out the plan's minimum gap between new sessions (one new browser
    /// every 20 seconds on the free plan).
    async fn pace(&self) {
        let Some(interval) = self.config.min_session_interval() else {
            return;
        };
        let wait = {
            let mut last = self
                .last_start
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let wait = match *last {
                Some(previous) => interval.saturating_sub(previous.elapsed()),
                None => Duration::ZERO,
            };
            *last = Some(Instant::now());
            wait
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }

    /// Whether a secret may be typed into this page (D567).
    fn fill_allowed(&self, url: &str) -> Result<(), BridgeError> {
        if !self.credentials.allow_secret_fills {
            return Err(BridgeError::policy(
                "a remote browser is a third party: it takes no user-credential fills. Enable \
                 credentials.allow-secret-fills only for a service account, and only on a host in \
                 credentials.allowed-fill-hosts (D567)",
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

    async fn read_state(client: &CdpClient, session_id: &str) -> Result<PageState, BridgeError> {
        let tree = client
            .call_page(
                session_id,
                "Accessibility.getFullAXTree",
                json!({ "depth": -1 }),
            )
            .await?;
        let nodes = ax::parse_ax_tree(&tree)?;
        let read = client
            .call_page(
                session_id,
                "Runtime.evaluate",
                json!({ "expression": READ_PAGE, "returnByValue": true }),
            )
            .await?;
        let (url, title, text) = parse_read(&read);
        Ok(PageState {
            url,
            title,
            nodes,
            text,
            screenshot: None,
        })
    }

    /// Read the page again and make it the slot's current snapshot.
    async fn resnapshot(slot: &PageSlot, verbose: bool) -> Result<Snapshot, BridgeError> {
        let state = Self::read_state(&slot.client, &slot.session_id).await?;
        let snapshot = {
            let mut cache = slot.cache.lock().unwrap_or_else(PoisonError::into_inner);
            cache.build(&state, verbose)
        };
        *slot.state.lock().unwrap_or_else(PoisonError::into_inner) = state;
        Ok(snapshot)
    }

    async fn call_element(
        slot: &PageSlot,
        backend_id: i64,
        function: &str,
        arguments: Vec<Value>,
    ) -> Result<(), BridgeError> {
        let resolved = slot
            .client
            .call_page(
                &slot.session_id,
                "DOM.resolveNode",
                json!({ "backendNodeId": backend_id }),
            )
            .await?;
        let object_id = resolved
            .get("object")
            .and_then(|object| object.get("objectId"))
            .and_then(Value::as_str)
            .ok_or_else(|| {
                BridgeError::unavailable(format!(
                    "element {backend_id} is not in the page any more; take_snapshot again"
                ))
            })?
            .to_string();

        let result = slot
            .client
            .call_page(
                &slot.session_id,
                "Runtime.callFunctionOn",
                json!({
                    "objectId": object_id,
                    "functionDeclaration": function,
                    "arguments": arguments,
                    "returnByValue": true
                }),
            )
            .await;
        // Release the handle whether or not the call worked, so the session's
        // object group does not grow for its whole life.
        let _ = slot
            .client
            .call_page(
                &slot.session_id,
                "Runtime.releaseObject",
                json!({ "objectId": object_id }),
            )
            .await;
        result.map(|_| ())
    }
}

/// Read the JSON the [`READ_PAGE`] script returned.
fn parse_read(result: &Value) -> (String, String, String) {
    let value = result
        .get("result")
        .and_then(|inner| inner.get("value"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let parsed: Value = serde_json::from_str(value).unwrap_or(Value::Null);
    let text = |key: &str| parsed.get(key).and_then(Value::as_str).unwrap_or_default();
    (
        text("u").to_string(),
        text("t").to_string(),
        text("x").to_string(),
    )
}

#[async_trait]
impl BrowserProvider for BrowserRunProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::BrowserRun
    }

    fn engine(&self) -> Option<Engine> {
        Some(self.engine)
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            core: true,
            // Kitesurf renders, but Cloudflare documents it as not
            // pixel-perfect; say so rather than pretend.
            screenshots: self.engine == Engine::Chromium,
            extraction: true,
            // Chromium keeps state within a session; Kitesurf is stateless by
            // design and Cloudflare rules it out for a signed-in session.
            authenticated_sessions: self.engine == Engine::Chromium,
            guardrails: self.engine == Engine::Chromium && self.egress.is_allow_listed(),
            // Kitesurf has no tabs.
            tabs: self.engine == Engine::Chromium,
            credentials_stay_local: false,
        }
    }

    async fn open(&self, request: OpenRequest) -> Result<PageRef, BridgeError> {
        if let Some(url) = &request.url {
            self.egress.check(url)?;
        }
        self.budget.check()?;
        self.pace().await;

        let permit = Arc::clone(&self.permits).try_acquire_owned().map_err(|_| {
            BridgeError::SessionLimit {
                max: self.config.max_concurrent_sessions(),
            }
        })?;

        let token = self.token().await?;
        let (endpoint, headers) = self.handshake(&token);
        let connection = self.transport.connect(&endpoint, &headers).await?;

        let client = Arc::new(CdpClient::new(connection, self.config.request_timeout()));
        let target = client
            .call("Target.createTarget", json!({ "url": "about:blank" }))
            .await?;
        let target_id = target
            .get("targetId")
            .and_then(Value::as_str)
            .ok_or_else(|| BridgeError::cdp("Target.createTarget returned no targetId"))?
            .to_string();
        let attached = client
            .call(
                "Target.attachToTarget",
                json!({ "targetId": target_id, "flatten": true }),
            )
            .await?;
        let session_id = attached
            .get("sessionId")
            .and_then(Value::as_str)
            .ok_or_else(|| BridgeError::cdp("Target.attachToTarget returned no sessionId"))?
            .to_string();

        for domain in ["Page", "Runtime", "DOM", "Accessibility"] {
            client
                .call_page(&session_id, &format!("{domain}.enable"), json!({}))
                .await?;
        }

        let slot = PageSlot {
            client,
            session_id,
            target_id,
            opened_at: Instant::now(),
            _permit: permit,
            state: Mutex::new(PageState::default()),
            cache: Mutex::new(SnapshotCache::new()),
        };

        let Some(url) = request.url else {
            // A blank page still gets a first read, so a uid exists before the
            // first action.
            let state = Self::read_state(&slot.client, &slot.session_id).await?;
            *slot.state.lock().unwrap_or_else(PoisonError::into_inner) = state.clone();
            slot.cache
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .build(&state, false);
            return Ok(self.insert(slot));
        };

        let handle = self.insert(slot);
        self.navigate(&handle, &url).await?;
        Ok(handle)
    }

    async fn navigate(&self, page: &PageRef, url: &Url) -> Result<Navigation, BridgeError> {
        self.egress.check(url)?;
        let slot = self.slot(page)?;
        let result = slot
            .client
            .call_page(
                &slot.session_id,
                "Page.navigate",
                json!({ "url": url.as_str() }),
            )
            .await?;
        if let Some(error) = result.get("errorText").and_then(Value::as_str) {
            return Err(BridgeError::cdp(format!(
                "Page.navigate: {error} ({})",
                crate::tool::redact_url(url.as_str())
            )));
        }
        let state = Self::read_state(&slot.client, &slot.session_id).await?;
        let change = {
            let mut cache = slot.cache.lock().unwrap_or_else(PoisonError::into_inner);
            *slot.state.lock().unwrap_or_else(PoisonError::into_inner) = state.clone();
            cache.build(&state, false);
            cache.change()
        };
        Ok(Navigation {
            url: crate::tool::redact_url(&state.url),
            title: state.title.clone(),
            change,
        })
    }

    async fn snapshot(
        &self,
        page: &PageRef,
        request: &SnapshotRequest,
    ) -> Result<Snapshot, BridgeError> {
        let slot = self.slot(page)?;
        Self::resnapshot(&slot, request.verbose).await
    }

    async fn find(
        &self,
        page: &PageRef,
        query: &FindQuery,
    ) -> Result<Vec<SnapshotNode>, BridgeError> {
        let slot = self.slot(page)?;
        slot.cache
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .find(query)
    }

    async fn click(&self, page: &PageRef, uid: &Uid) -> Result<ActionOutcome, BridgeError> {
        let slot = self.slot(page)?;
        let node = slot
            .cache
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .resolve(uid)?;
        Self::call_element(&slot, node.backend_id, CLICK_ELEMENT, Vec::new()).await?;
        let request = SnapshotRequest::new();
        Self::resnapshot(&slot, false).await?;
        let answer = outcome(
            format!("clicked {uid} ({})", node.role),
            &slot.cache.lock().unwrap_or_else(PoisonError::into_inner),
            SnapshotMode::Diff,
            &request,
        );
        Ok(answer)
    }

    async fn fill(
        &self,
        page: &PageRef,
        uid: &Uid,
        value: &FillValue,
    ) -> Result<ActionOutcome, BridgeError> {
        let slot = self.slot(page)?;
        let node = slot
            .cache
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .resolve(uid)?;
        let secret = match value {
            FillValue::Literal(literal) => SecretValue::new(literal),
            FillValue::Secret(reference) => {
                self.fill_allowed(
                    &slot
                        .state
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .url,
                )?;
                self.secrets.resolve(reference).await?
            }
        };
        // Register before the value goes anywhere near the socket, so an error
        // from the browser cannot echo it into a log.
        crate::redact::register(secret.expose());
        Self::call_element(
            &slot,
            node.backend_id,
            FILL_ELEMENT,
            vec![json!({ "value": secret.expose() })],
        )
        .await?;
        let request = SnapshotRequest::new();
        Self::resnapshot(&slot, false).await?;
        let answer = outcome(
            format!("filled {uid} ({})", node.role),
            &slot.cache.lock().unwrap_or_else(PoisonError::into_inner),
            SnapshotMode::Diff,
            &request,
        );
        Ok(answer)
    }

    async fn wait_for(
        &self,
        page: &PageRef,
        request: &WaitRequest,
    ) -> Result<WaitOutcome, BridgeError> {
        let slot = self.slot(page)?;
        let started = Instant::now();
        let deadline = started + Duration::from_millis(request.timeout_ms);
        let mut matched = false;
        loop {
            let holds = match &request.condition {
                WaitCondition::Selector(selector) => {
                    let expression = format!(
                        "Boolean(document.querySelector({}))",
                        serde_json::to_string(selector).unwrap_or_else(|_| "\"\"".to_string())
                    );
                    let result = slot
                        .client
                        .call_page(
                            &slot.session_id,
                            "Runtime.evaluate",
                            json!({ "expression": expression, "returnByValue": true }),
                        )
                        .await?;
                    result
                        .get("result")
                        .and_then(|inner| inner.get("value"))
                        .and_then(Value::as_bool)
                        .unwrap_or(false)
                }
                condition => {
                    let fresh = Self::read_state(&slot.client, &slot.session_id).await?;
                    crate::local::condition_holds(condition, &fresh)
                }
            };
            if holds {
                matched = true;
                break;
            }
            if Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(request.poll_interval_ms)).await;
        }
        Ok(WaitOutcome {
            matched,
            waited_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            note: if matched {
                String::new()
            } else {
                "the condition did not hold before the timeout; take_snapshot to see the page"
                    .to_string()
            },
        })
    }

    async fn screenshot(&self, page: &PageRef) -> Result<Screenshot, BridgeError> {
        if self.engine != Engine::Chromium {
            return Err(BridgeError::Unsupported {
                engine: self.engine.as_str(),
                what: "Cloudflare documents Kitesurf as not pixel-perfect; use extract, or the \
                       chromium engine for a screenshot"
                    .to_string(),
            });
        }
        let slot = self.slot(page)?;
        let shot = slot
            .client
            .call_page(
                &slot.session_id,
                "Page.captureScreenshot",
                json!({ "format": "png", "captureBeyondViewport": false }),
            )
            .await?;
        let encoded = shot
            .get("data")
            .and_then(Value::as_str)
            .ok_or_else(|| BridgeError::cdp("Page.captureScreenshot returned no data"))?;
        let bytes = base64_decode(encoded)?;
        artifact::write(&self.artifacts, "screenshot", "png", &bytes)
    }

    async fn extract(&self, page: &PageRef) -> Result<Extracted, BridgeError> {
        let slot = self.slot(page)?;
        let read = slot
            .client
            .call_page(
                &slot.session_id,
                "Runtime.evaluate",
                json!({ "expression": READ_PAGE, "returnByValue": true }),
            )
            .await?;
        let (url, title, text) = parse_read(&read);
        Ok(artifact::extracted(
            &PageState {
                url,
                title,
                text,
                ..PageState::default()
            },
            SnapshotRequest::new().budget(),
        ))
    }

    async fn close(&self, page: &PageRef) -> Result<(), BridgeError> {
        let Some(slot) = self
            .pages
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(page.id())
        else {
            return Ok(());
        };
        self.budget.charge(slot.opened_at.elapsed());
        // `Browser.close` ends the session. A browser that already timed out
        // answers with an error, which is not worth reporting; the socket is
        // ours either way.
        let _ = slot.client.call("Browser.close", json!({})).await;
        slot.client.close().await
    }
}

fn base64_decode(encoded: &str) -> Result<Vec<u8>, BridgeError> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|error| BridgeError::cdp(format!("the screenshot was not base64: {error}")))
}
