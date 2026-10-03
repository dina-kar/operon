//! The Chrome DevTools Protocol client the sidebar browser drives the engine
//! with.
//!
//! # Why a client rather than a dependency
//!
//! `obscura-cdp` is not published to crates.io — the crate name on crates.io is
//! an unrelated 2019 project — and the engine's only supported embedding
//! surface is the DevTools endpoint it serves over a WebSocket
//! (`ws://127.0.0.1:<port>/devtools/browser`). So Loams speaks the wire
//! protocol itself. That is also the safer direction: the CDP surface is a
//! documented, versioned protocol, whereas a crate's internals are neither.
//!
//! # Transport and session model
//!
//! One WebSocket carries everything, and sessions are **flat**: a session id is
//! a top-level field on each message rather than a nested envelope. Obscura
//! reads it from exactly that place (`obscura-cdp/src/server.rs`), so this
//! client writes it there too. Messages that are not a response to one of our
//! calls are buffered as events, because on a frame stream the events and the
//! acknowledgements interleave and dropping either loses a frame or stalls the
//! stream.
//!
//! # Two places this client's shape is dictated by the engine
//!
//! - **`Page.screencastFrameAck` takes an integer `sessionId`.** Obscura parses
//!   it with `screencast_int32` (`obscura-cdp/src/domains/page.rs`), where
//!   Chromium's CDP uses a string. [`crate::panel`] sends the integer form, and
//!   [`crate::frame::DecodedFrame::ack_params`] is the single place it is
//!   built.
//! - **`Browser.close` is special-cased by the engine** to close the connection
//!   from the server side, so it is how this client asks the engine to stop
//!   before killing the process.

use std::collections::VecDeque;
use std::time::Duration;

use futures::{SinkExt as _, StreamExt as _};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use crate::error::{Result, SidebarBrowserError};

/// How long [`CdpClient::call`] waits for one response before giving up.
pub const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(20);

/// The largest CDP message accepted from the engine, in bytes.
///
/// A frame at the [`crate::frame::MAX_FRAME_BYTES`] ceiling base64-encodes to
/// about 4/3 of that, so this has to be comfortably above it.
pub const MAX_MESSAGE_BYTES: usize = 48 * 1024 * 1024;

/// An event from the engine: a message with a `method` and no matching id.
#[derive(Clone, Debug, PartialEq)]
pub struct CdpEvent {
    /// The CDP method, e.g. `Page.screencastFrame`.
    pub method: String,
    /// The method's parameters.
    pub params: Value,
    /// The session the event belongs to, when it is scoped to one.
    pub session_id: Option<String>,
}

impl CdpEvent {
    /// Read a parameter by key.
    pub fn param(&self, key: &str) -> Option<&Value> {
        self.params.get(key)
    }

    /// Read a string parameter by key.
    pub fn str_param(&self, key: &str) -> Option<&str> {
        self.params.get(key).and_then(Value::as_str)
    }
}

/// A connected CDP client.
#[derive(Debug)]
pub struct CdpClient {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    next_id: u64,
    buffered: VecDeque<CdpEvent>,
    call_timeout: Duration,
    in_flight: Vec<(u64, String, std::time::Instant)>,
}

impl CdpClient {
    /// Connect to an engine's DevTools endpoint.
    ///
    /// `ws_url` is expected to be loopback. Nothing here validates that, and
    /// nothing here should: the transport has no authentication and no
    /// encryption, so exposing an engine's CDP port on a network turns it into
    /// an arbitrary-code-execution surface for whoever can reach it. The
    /// launch spec in [`crate::engine`] binds the engine to `127.0.0.1` for
    /// that reason, and [`crate::engine::EngineLaunchSpec::assert_loopback`]
    /// asserts it.
    pub async fn connect(ws_url: &str) -> Result<Self> {
        Self::connect_with_timeout(ws_url, DEFAULT_CALL_TIMEOUT).await
    }

    /// [`connect`], with an explicit per-call timeout.
    pub async fn connect_with_timeout(ws_url: &str, call_timeout: Duration) -> Result<Self> {
        let (socket, _response) = connect_async(ws_url)
            .await
            .map_err(|error| SidebarBrowserError::Transport(format!("{ws_url}: {error}")))?;
        Ok(Self {
            socket,
            // Chromium's own clients start at 1 and Obscura's tests hard-code
            // ids 1..3, so starting at 1 avoids any chance of colliding with
            // an assumption there.
            next_id: 1,
            buffered: VecDeque::new(),
            call_timeout,
            in_flight: Vec::new(),
        })
    }

    /// Send a command and wait for its response, buffering any events that
    /// arrive first.
    ///
    /// Events seen while waiting are kept in order for
    /// [`take_event`](Self::take_event) and [`next_event`](Self::next_event);
    /// dropping them would lose frames from an active screencast.
    pub async fn call(
        &mut self,
        method: &str,
        params: Value,
        session_id: Option<&str>,
    ) -> Result<Value> {
        let id = self.next_id;
        self.next_id += 1;
        self.in_flight
            .push((id, method.to_string(), std::time::Instant::now()));
        self.send_command(id, method, params, session_id).await?;

        let deadline = tokio::time::Instant::now() + self.call_timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                self.forget(id);
                return Err(SidebarBrowserError::Timeout {
                    method: method.to_string(),
                    elapsed_ms: self.call_timeout.as_millis() as u64,
                });
            }
            let message = match tokio::time::timeout(remaining, self.socket.next()).await {
                Ok(Some(Ok(message))) => message,
                Ok(Some(Err(error))) => {
                    self.forget(id);
                    return Err(SidebarBrowserError::Transport(format!(
                        "reading the response to {method}: {error}"
                    )));
                }
                Ok(None) => {
                    self.forget(id);
                    return Err(SidebarBrowserError::Transport(format!(
                        "the engine closed the connection while {method} was in flight"
                    )));
                }
                Err(_) => {
                    self.forget(id);
                    return Err(SidebarBrowserError::Timeout {
                        method: method.to_string(),
                        elapsed_ms: self.call_timeout.as_millis() as u64,
                    });
                }
            };
            let Some(value) = decode_message(message, method)? else {
                continue;
            };
            if let Some(found) = value.get("id").and_then(Value::as_u64) {
                if found != id {
                    // A response to a call we already gave up on. Buffer it as
                    // nothing and keep waiting rather than failing the call we
                    // do care about.
                    continue;
                }
                self.forget(id);
                if let Some(error) = value.get("error") {
                    let message = error
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("the engine reported an error with no message");
                    return Err(SidebarBrowserError::Call {
                        method: method.to_string(),
                        message: message.to_string(),
                    });
                }
                return Ok(value.get("result").cloned().unwrap_or(Value::Null));
            }
            self.buffer_event(value);
        }
    }

    /// Send a message with no response expected. Obscura has no use for these
    /// today; provided so that future CDP notifications have a home.
    pub async fn notify(
        &mut self,
        method: &str,
        params: Value,
        session_id: Option<&str>,
    ) -> Result<()> {
        self.send_command(0, method, params, session_id).await
    }

    async fn send_command(
        &mut self,
        id: u64,
        method: &str,
        params: Value,
        session_id: Option<&str>,
    ) -> Result<()> {
        let mut message = serde_json::Map::new();
        if id != 0 {
            message.insert("id".to_string(), serde_json::json!(id));
        }
        message.insert("method".to_string(), serde_json::json!(method));
        message.insert("params".to_string(), params);
        if let Some(session_id) = session_id {
            message.insert("sessionId".to_string(), serde_json::json!(session_id));
        }
        let text = serde_json::Value::Object(message).to_string();
        self.socket
            .send(Message::Text(text))
            .await
            .map_err(|error| SidebarBrowserError::Transport(format!("sending {method}: {error}")))
    }

    /// Take the next already-buffered event, if there is one.
    pub fn take_event(&mut self) -> Option<CdpEvent> {
        self.buffered.pop_front()
    }

    /// The next event: buffered first, then from the socket.
    pub async fn next_event(&mut self) -> Result<CdpEvent> {
        if let Some(event) = self.buffered.pop_front() {
            return Ok(event);
        }
        loop {
            let Some(message) = self.socket.next().await else {
                return Err(SidebarBrowserError::Transport(
                    "the engine closed the connection".to_string(),
                ));
            };
            let message = message.map_err(|error| {
                SidebarBrowserError::Transport(format!("reading an event: {error}"))
            })?;
            let Some(value) = decode_message(message, "next_event")? else {
                continue;
            };
            if value.get("id").is_some() {
                // A response to a call that has already been answered or timed
                // out. Nothing to deliver.
                continue;
            }
            return Ok(CdpEvent {
                method: value
                    .get("method")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                params: value.get("params").cloned().unwrap_or(Value::Null),
                session_id: value
                    .get("sessionId")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            });
        }
    }

    /// The commands that have not yet been answered, for diagnostics when a
    /// call times out.
    pub fn in_flight(&self) -> Vec<(u64, String, Duration)> {
        self.in_flight
            .iter()
            .map(|(id, method, since)| (*id, method.clone(), since.elapsed()))
            .collect()
    }

    fn forget(&mut self, id: u64) {
        self.in_flight.retain(|(pending, _, _)| *pending != id);
    }

    fn buffer_event(&mut self, value: Value) {
        let Some(method) = value.get("method").and_then(Value::as_str) else {
            return;
        };
        self.buffered.push_back(CdpEvent {
            method: method.to_string(),
            params: value.get("params").cloned().unwrap_or(Value::Null),
            session_id: value
                .get("sessionId")
                .and_then(Value::as_str)
                .map(str::to_string),
        });
    }

    /// Ask the engine to close, and wait briefly for it to do so.
    ///
    /// `Browser.close` is the one command the engine answers by closing the
    /// socket without replying (`obscura-cdp/src/server.rs` special-cases the
    /// literal text `Browser.close`), so a timeout here is the expected
    /// outcome, not a failure. [`ObscuraEngine::stop`](crate::engine::ObscuraEngine::stop)
    /// escalates to killing the process.
    pub async fn close(mut self) {
        let _ = self
            .send_command(1, "Browser.close", Value::Null, None)
            .await;
        let grace = Duration::from_millis(500);
        let _ = tokio::time::timeout(grace, self.socket.next()).await;
    }
}

fn decode_message(message: Message, context: &str) -> Result<Option<Value>> {
    match message {
        Message::Text(text) => {
            if text.len() > MAX_MESSAGE_BYTES {
                return Err(SidebarBrowserError::Transport(format!(
                    "{context}: the engine sent a {} byte message, over the {MAX_MESSAGE_BYTES}-byte limit",
                    text.len()
                )));
            }
            serde_json::from_str(&text).map(Some).map_err(|error| {
                SidebarBrowserError::Transport(format!(
                    "{context}: the engine sent a message that is not JSON: {error}"
                ))
            })
        }
        Message::Close(_) => Err(SidebarBrowserError::Transport(format!(
            "{context}: the engine closed the connection"
        ))),
        // Control frames carry no CDP payload. tungstenite answers pings itself.
        Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => Ok(None),
        Message::Binary(_) => Err(SidebarBrowserError::Transport(format!(
            "{context}: the engine sent a binary CDP message, which is not part of the protocol"
        ))),
    }
}
