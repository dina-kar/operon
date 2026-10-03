//! The Chrome DevTools Protocol client: a transport seam and a thin client on
//! top of it.
//!
//! The seam exists so the provider can be tested without a Cloudflare account.
//! `crate::browser_run::ws::WebSocketTransport` is the real one; the
//! conformance suite and the unit tests use a scripted transport.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::browser_run::endpoint::RequestHeaders;
use crate::error::BridgeError;

/// One CDP request.
#[derive(Debug, Serialize)]
pub struct CdpRequest {
    /// The message id, matched to the response.
    pub id: u64,
    /// The method, for example `Page.navigate`.
    pub method: String,
    /// The parameters.
    #[serde(default)]
    pub params: Value,
    /// The attached target's session, when the call is page-scoped.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
}

impl CdpRequest {
    /// A browser-scoped call.
    pub fn browser(id: u64, method: &str, params: Value) -> Self {
        Self {
            id,
            method: method.to_string(),
            params,
            session_id: None,
        }
    }

    /// A page-scoped call.
    pub fn page(id: u64, session_id: &str, method: &str, params: Value) -> Self {
        Self {
            id,
            method: method.to_string(),
            params,
            session_id: Some(session_id.to_string()),
        }
    }
}

/// One CDP response.
#[derive(Debug, Deserialize)]
pub struct CdpResponse {
    /// The id of the request this answers.
    #[serde(default)]
    pub id: Option<u64>,
    /// The result, when the call succeeded.
    #[serde(default)]
    pub result: Option<Value>,
    /// The error, when it did not.
    #[serde(default)]
    pub error: Option<CdpErrorBody>,
}

/// The `error` member of a failed CDP response.
#[derive(Debug, Clone, Deserialize)]
pub struct CdpErrorBody {
    /// The protocol error code.
    #[serde(default)]
    pub code: i64,
    /// The message Cloudflare's browser sent.
    #[serde(default)]
    pub message: String,
}

impl std::fmt::Display for CdpErrorBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "code {}: {}", self.code, self.message)
    }
}

/// An open CDP connection.
#[async_trait::async_trait]
pub trait CdpConnection: Send + Sync + std::fmt::Debug {
    /// Send a command and wait for its result.
    async fn send(&self, request: CdpRequest) -> Result<Value, BridgeError>;

    /// Close the socket. Closing twice is not an error.
    async fn close(&self) -> Result<(), BridgeError>;
}

/// How a provider gets a connection.
#[async_trait]
pub trait CdpTransport: Send + Sync + std::fmt::Debug {
    /// Open `endpoint` with `headers` and speak CDP on it.
    async fn connect(
        &self,
        endpoint: &str,
        headers: &RequestHeaders,
    ) -> Result<Arc<dyn CdpConnection>, BridgeError>;
}

/// A CDP client over a connection: ids, timeouts and the calls the provider
/// needs.
#[derive(Debug)]
pub struct CdpClient {
    connection: Arc<dyn CdpConnection>,
    next_id: std::sync::atomic::AtomicU64,
    timeout: std::time::Duration,
}

impl CdpClient {
    /// A client that gives up on a call after `timeout`.
    pub fn new(connection: Arc<dyn CdpConnection>, timeout: std::time::Duration) -> Self {
        Self {
            connection,
            next_id: std::sync::atomic::AtomicU64::new(1),
            timeout,
        }
    }

    /// A browser-scoped call.
    pub async fn call(&self, method: &str, params: Value) -> Result<Value, BridgeError> {
        let id = self.next_id();
        self.dispatch(CdpRequest::browser(id, method, params)).await
    }

    /// A page-scoped call.
    pub async fn call_page(
        &self,
        session_id: &str,
        method: &str,
        params: Value,
    ) -> Result<Value, BridgeError> {
        let id = self.next_id();
        self.dispatch(CdpRequest::page(id, session_id, method, params))
            .await
    }

    /// Close the connection.
    pub async fn close(&self) -> Result<(), BridgeError> {
        self.connection.close().await
    }

    async fn dispatch(&self, request: CdpRequest) -> Result<Value, BridgeError> {
        let method = request.method.clone();
        match tokio::time::timeout(self.timeout, self.connection.send(request)).await {
            Ok(result) => result,
            Err(_) => Err(BridgeError::Timeout(
                u64::try_from(self.timeout.as_millis()).unwrap_or(u64::MAX),
            )),
        }
        .map_err(|error| {
            // Re-tag a protocol error with the method that produced it, which
            // is what makes an error actionable, and scrub the message.
            match error {
                BridgeError::Cdp(message) => BridgeError::cdp(format!("{method}: {message}")),
                other => other,
            }
        })
    }

    fn next_id(&self) -> u64 {
        self.next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}
