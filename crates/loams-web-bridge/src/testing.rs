//! Deterministic drivers for the conformance suite: a fake local browser and a
//! scripted CDP connection.
//!
//! Both record what they were asked, so a test can assert the exact CDP
//! conversation a provider has (module `browser_run`) without a Cloudflare
//! account, and both refuse nothing, so the provider's own policy is what the
//! test is really measuring.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::Value;

use crate::browser_run::cdp::{CdpConnection, CdpRequest, CdpTransport};
use crate::browser_run::endpoint::RequestHeaders;
use crate::error::BridgeError;
use crate::local::{DriverAction, DriverPage, PageDriver};
use crate::page::PageState;
use crate::provider::OpenRequest;
use crate::tool::{AxNode, WaitRequest};

/// A page of the fake local browser.
#[derive(Clone, Default)]
pub struct FakePage {
    /// The URL the driver reports.
    pub url: String,
    /// The title the driver reports.
    pub title: String,
    /// The accessibility tree's roots.
    pub nodes: Vec<AxNode>,
    /// The visible text.
    pub text: String,
    /// The bytes a screenshot returns.
    pub screenshot: Vec<u8>,
}

/// A local driver that serves [`FakePage`]s and records every action.
#[derive(Debug, Default)]
pub struct FakeDriver {
    pages: Mutex<HashMap<String, FakePage>>,
    /// Every action, in order, with the page's own value elided.
    pub actions: Mutex<Vec<String>>,
    next_id: AtomicU64,
    page: Mutex<FakePage>,
}

/// The standard fixture page: a form with a button, a field and a password.
pub fn login_page(url: &str) -> FakePage {
    FakePage {
        url: url.to_string(),
        title: "Sign in".to_string(),
        nodes: vec![
            AxNode::new("document", "Sign in")
                .with_backend_id(1)
                .with_child(
                    AxNode::new("textbox", "Username")
                        .with_backend_id(2)
                        .with_state("focused"),
                ),
            AxNode::new("button", "Sign in").with_backend_id(3),
            AxNode::new("textbox", "Password")
                .with_value("hunter2")
                .with_backend_id(4)
                .sensitive(),
        ],
        text: "Sign in".to_string(),
        screenshot: b"\x89PNG\r\n\x1a\nfake".to_vec(),
    }
}

impl FakeDriver {
    /// A driver serving one page.
    pub fn new(page: FakePage) -> Self {
        Self {
            page: Mutex::new(page),
            ..Self::default()
        }
    }

    /// Replace the page the driver serves, as a navigation would.
    pub fn set_page(&self, page: FakePage) {
        *self
            .page
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = page;
    }

    /// The page the driver serves.
    pub fn page(&self) -> FakePage {
        self.page
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// The actions recorded so far.
    pub fn actions(&self) -> Vec<String> {
        self.actions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl std::fmt::Debug for FakePage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakePage")
            .field("url", &self.url)
            .field("title", &self.title)
            .field("nodes", &self.nodes.len())
            .finish()
    }
}

#[async_trait]
impl PageDriver for FakeDriver {
    async fn open(&self, _request: &OpenRequest) -> Result<(DriverPage, PageState), BridgeError> {
        let id = format!("page-{}", self.next_id.fetch_add(1, Ordering::Relaxed));
        self.pages
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id.clone(), self.page());
        self.actions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push("open".to_string());
        Ok((DriverPage::new(id), state_of(&self.page())))
    }

    async fn state(&self, page: &DriverPage) -> Result<PageState, BridgeError> {
        let pages = self
            .pages
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match pages.get(page.id()) {
            Some(fake) => Ok(state_of(fake)),
            None => Err(BridgeError::unavailable(format!(
                "{} is not open",
                page.id()
            ))),
        }
    }

    async fn act(
        &self,
        page: &DriverPage,
        action: DriverAction,
    ) -> Result<Option<PageState>, BridgeError> {
        // Recorded with the filled value left out on purpose: a test asserts
        // that a secret does not travel through the driver's own log either.
        let described = match &action {
            DriverAction::Navigate(url) => format!("navigate {}", url),
            DriverAction::Click { backend_id } => format!("click {backend_id}"),
            DriverAction::Fill { backend_id, value } => {
                format!("fill {backend_id} ({} bytes)", value.len())
            }
            DriverAction::Wait(WaitRequest { condition, .. }) => format!("wait {condition:?}"),
            DriverAction::Screenshot => "screenshot".to_string(),
            DriverAction::Extract => "extract".to_string(),
            DriverAction::Close => "close".to_string(),
        };
        self.actions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(described);

        if let DriverAction::Navigate(url) = &action {
            let mut fake = self.page();
            fake.url = url.to_string();
            fake.title = format!("Sign in ({})", url.host_str().unwrap_or_default());
            self.set_page(fake);
        }
        let updated = state_of(&self.page());
        if let DriverAction::Screenshot = action {
            let mut updated = updated;
            updated.screenshot = Some((self.page().screenshot.clone(), "png" as &'static str));
            return Ok(Some(updated));
        }
        let pages = self
            .pages
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current = pages.get(page.id()).cloned().unwrap_or_default();
        drop(pages);
        let mut current = current;
        current.url = updated.url;
        current.title = updated.title;
        current.nodes = updated.nodes;
        current.text = updated.text;
        Ok(Some(state_of(&current)))
    }
}

fn state_of(fake: &FakePage) -> PageState {
    PageState {
        url: fake.url.clone(),
        title: fake.title.clone(),
        nodes: fake.nodes.clone(),
        text: fake.text.clone(),
        screenshot: None,
    }
}

/// A CDP connection that answers from a script and records every request.
#[derive(Debug, Default)]
pub struct FakeCdp {
    /// Answers by method name; a method with no entry returns `{}`.
    pub answers: HashMap<String, Value>,
    /// Every request, in order, as `method(session)`.
    pub calls: Mutex<Vec<String>>,
    /// When set, `Target.createTarget` fails with this message.
    pub fail_create_target: Option<String>,
}

impl FakeCdp {
    /// A connection that answers `answers`.
    pub fn new(answers: HashMap<String, Value>) -> Self {
        Self {
            answers,
            ..Self::default()
        }
    }

    /// The methods called so far.
    pub fn calls(&self) -> Vec<String> {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn record(&self, request: &CdpRequest) {
        let session = request.session_id.as_deref().unwrap_or("-");
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(format!("{}({session})", request.method));
    }
}

#[async_trait]
impl CdpConnection for FakeCdp {
    async fn send(&self, request: CdpRequest) -> Result<Value, BridgeError> {
        self.record(&request);
        if request.method == "Target.createTarget"
            && let Some(message) = &self.fail_create_target
        {
            return Err(BridgeError::cdp(message.clone()));
        }
        Ok(self
            .answers
            .get(&request.method)
            .cloned()
            .unwrap_or_else(|| Value::Object(Default::default())))
    }

    async fn close(&self) -> Result<(), BridgeError> {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push("close".to_string());
        Ok(())
    }
}

/// The transport that hands out one [`FakeCdp`] and remembers the handshake.
#[derive(Debug)]
pub struct FakeTransport {
    /// The connection every `connect` returns.
    pub connection: Arc<FakeCdp>,
    /// The endpoints and headers of every handshake.
    pub handshakes: Mutex<Vec<(String, RequestHeaders)>>,
}

impl FakeTransport {
    /// A transport over `connection`.
    pub fn new(connection: Arc<FakeCdp>) -> Self {
        Self {
            connection,
            handshakes: Mutex::new(Vec::new()),
        }
    }

    /// The handshakes recorded so far.
    pub fn handshakes(&self) -> Vec<(String, RequestHeaders)> {
        self.handshakes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

#[async_trait]
impl CdpTransport for FakeTransport {
    async fn connect(
        &self,
        endpoint: &str,
        headers: &RequestHeaders,
    ) -> Result<Arc<dyn CdpConnection>, BridgeError> {
        self.handshakes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((endpoint.to_string(), headers.clone()));
        Ok(Arc::clone(&self.connection) as Arc<dyn CdpConnection>)
    }
}

/// A CDP answer for `Accessibility.getFullAXTree` with one button.
pub fn ax_tree_with_button(name: &str, backend_id: i64) -> Value {
    serde_json::json!({
        "nodes": [
            {"nodeId": "1", "role": {"value": "RootWebArea"}, "name": {"value": "Example"},
             "childIds": ["2"], "backendDOMNodeId": 1},
            {"nodeId": "2", "role": {"value": "button"}, "name": {"value": name},
             "backendDOMNodeId": backend_id}
        ]
    })
}

/// A CDP answer for `Runtime.evaluate` with a URL, title and text.
pub fn read_page(url: &str, title: &str, text: &str) -> Value {
    serde_json::json!({
        "result": {
            "type": "string",
            "value": serde_json::json!({ "u": url, "t": title, "x": text }).to_string()
        }
    })
}
