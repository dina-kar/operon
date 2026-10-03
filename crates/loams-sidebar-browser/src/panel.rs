//! The docked panel: frames in, interaction back out.
//!
//! # This is an agent-driven surface, not a human-interactive browser
//!
//! The engine is headless with a software rasteriser. There is no window, no
//! compositor and no GPU surface to host a real webview in: frames arrive as
//! `Page.screencastFrame` PNG payloads and go into the GPUI panel as an image,
//! and interaction goes back as `Input.dispatchMouseEvent` /
//! `Input.dispatchKeyEvent`. That is the whole design, and it has a
//! consequence worth stating plainly rather than discovering in a demo.
//!
//! **The panel is built for an agent reading it and driving it, not for a
//! person clicking through it.** It works — the frames are real, the input
//! routing is real — but there is no scroll momentum, no caret, no text
//! selection, no compositor-driven animation at 60 Hz, and a frame costs a
//! full-page raster plus a base64 round trip. The embedded targets are Zulip,
//! Plane and Forgejo, read-mostly and controlled, which bounds the cost; it
//! does not turn the surface into a browser. See the crate README's
//! "Fidelity" section, which this doc is the crate-internal half of.
//!
//! # Why the frame pump is written this way
//!
//! The engine is single-threaded per connection and emits frames on activity
//! (`obscura-cdp/src/domains/page.rs` documents it as "activity-driven" until it
//! owns a real compositor). Two consequences shape [`FramePump`]:
//!
//! - **Every frame must be acknowledged.** `Page.screencastFrameAck` releases
//!   the next frame, and Obscura parses its `sessionId` as an **integer**, not
//!   the string Chromium's CDP uses. Missing or mistyped acks stall the stream
//!   permanently, so the ack is sent from the same place the frame is decoded.
//! - **Only the newest frame matters.** A slow panel must drop frames, not
//!   queue them; [`PanelState::offer`] replaces the previous frame and reports
//!   how many were dropped, so the drop is visible in a log rather than being a
//!   silent latency increase.

use std::sync::Arc;

use serde_json::{Value, json};

use crate::cdp::{CdpClient, CdpEvent};
use crate::error::{Result, SidebarBrowserError};
use crate::frame::{DecodedFrame, FrameFormat, decode_frame};

/// The `Page.startScreencast` parameters this crate uses.
///
/// `format` is pinned to `png` because that is the only encoding
/// [`decode_frame`] reads. `everyNthFrame` is 1: the panel decides what to
/// draw, and skipping engine-side frames only makes the panel's view lag
/// further behind the page. `maxWidth` / `maxHeight` are unset so the frame
/// arrives at the panel's own size and is not resampled twice.
pub fn start_screencast_params(width: u32, height: u32) -> Value {
    json!({
        "format": FrameFormat::Png.as_str(),
        "quality": 100,
        "everyNthFrame": 1,
        "maxWidth": width,
        "maxHeight": height,
    })
}

/// The most recent frame, plus the accounting a panel needs to know whether it
/// is keeping up.
#[derive(Clone, Debug, Default)]
pub struct PanelState {
    latest: Option<Arc<DecodedFrame>>,
    /// Frames offered since the last [`PanelState::take_frame`].
    ///
    /// A non-zero value when a frame is taken means the engine produced frames
    /// faster than the panel drew them and the intermediate ones were dropped.
    dropped_since_take: u64,
    sequence: u64,
}

impl PanelState {
    /// An empty panel.
    pub fn new() -> Self {
        Self::default()
    }

    /// Offer a decoded frame, replacing any previous one.
    ///
    /// Returns true when this frame replaced an undrawn one.
    pub fn offer(&mut self, frame: DecodedFrame) -> bool {
        let replaced = self.latest.is_some();
        if replaced {
            self.dropped_since_take += 1;
        }
        self.sequence = frame.sequence;
        self.latest = Some(Arc::new(frame));
        replaced
    }

    /// The current frame, if there is one.
    pub fn latest(&self) -> Option<&Arc<DecodedFrame>> {
        self.latest.as_ref()
    }

    /// Take the current frame, clearing it so the panel can tell it has drawn.
    ///
    /// Returns the frame and the number of frames that were dropped since the
    /// last take.
    pub fn take_frame(&mut self) -> Option<(Arc<DecodedFrame>, u64)> {
        let dropped = std::mem::take(&mut self.dropped_since_take);
        self.latest.take().map(|frame| (frame, dropped))
    }

    /// Whether a frame is waiting to be drawn.
    pub fn has_pending_frame(&self) -> bool {
        self.latest.is_some()
    }

    /// The engine frame counter this panel has consumed.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}

/// Decodes frames off a CDP connection and acknowledges every one.
///
/// The pump owns a [`PanelState`] and a counter. It does **not** own the
/// client: the caller drives the connection, because the same connection
/// carries navigation responses and the panel's input acknowledgements, and
/// two owners of one socket is a race.
#[derive(Debug)]
pub struct FramePump {
    state: PanelState,
    format: FrameFormat,
    stream_id: Option<i64>,
    next_sequence: u64,
    acknowledged: u64,
    rejected: u64,
}

impl FramePump {
    /// A pump for a screencast started with `format`.
    pub fn new(format: FrameFormat) -> Self {
        Self {
            state: PanelState::new(),
            format,
            stream_id: None,
            next_sequence: 0,
            acknowledged: 0,
            rejected: 0,
        }
    }

    /// Note the stream id `Page.startScreencast` allocated, by reading it out
    /// of the first frame.
    ///
    /// Obscura does not return the stream id from `startScreencast`; it puts
    /// it on the frame's `sessionId` (`page.rs::queue_screencast_frame`), so
    /// the first frame is where it becomes known.
    pub fn note_stream_id(&mut self, stream_id: i64) {
        self.stream_id = Some(stream_id);
    }

    /// The panel state this pump fills.
    pub fn state(&self) -> &PanelState {
        &self.state
    }

    /// The panel state, mutably, so a renderer can take the frame it has drawn.
    ///
    /// Exposed because the draw happens outside this crate: the GPUI side calls
    /// [`PanelState::take_frame`] once it has put the pixels on screen, which is
    /// what makes the drop count mean "frames skipped while the panel was
    /// behind" rather than "frames skipped while the panel was idle".
    pub fn state_mut(&mut self) -> &mut PanelState {
        &mut self.state
    }

    /// How many frames have been acknowledged, decoded or not.
    pub fn acknowledged(&self) -> u64 {
        self.acknowledged
    }

    /// How many frames arrived but could not be decoded.
    pub fn rejected(&self) -> u64 {
        self.rejected
    }

    /// Whether `event` is a screencast frame.
    pub fn is_frame(event: &CdpEvent) -> bool {
        event.method == "Page.screencastFrame"
    }

    /// Handle one event.
    ///
    /// Returns `Some(frame)` for a frame that decoded, and always acknowledges
    /// a frame it saw — including one that failed to decode. An undecodable
    /// frame still consumed an engine frame slot, and leaving it unacked would
    /// wedge the stream rather than skip a bad picture.
    pub async fn handle(
        &mut self,
        client: &mut CdpClient,
        event: &CdpEvent,
    ) -> Result<Option<DecodedFrame>> {
        if !Self::is_frame(event) {
            return Ok(None);
        }
        let stream_id = event
            .params
            .get("sessionId")
            .and_then(Value::as_i64)
            .ok_or_else(|| {
                SidebarBrowserError::Frame(
                    "Page.screencastFrame has no integer sessionId; Obscura reports the stream \
                     id as an integer and a string here means this is not the pinned engine"
                        .to_string(),
                )
            })?;
        self.stream_id = Some(stream_id);
        let data = event.str_param("data").ok_or_else(|| {
            SidebarBrowserError::Frame("Page.screencastFrame has no data".to_string())
        })?;
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        match decode_frame(stream_id, data, self.format, sequence) {
            Ok(frame) => {
                self.state.offer(frame.clone());
                self.ack(client, stream_id).await?;
                self.acknowledged += 1;
                Ok(Some(frame))
            }
            Err(error) => {
                self.rejected += 1;
                tracing::warn!(
                    stream_id,
                    sequence,
                    error = %error,
                    "sidebar browser dropped an undecodable screencast frame"
                );
                self.ack(client, stream_id).await?;
                self.acknowledged += 1;
                Err(error)
            }
        }
    }

    async fn ack(&self, client: &mut CdpClient, stream_id: i64) -> Result<()> {
        client
            .call(
                "Page.screencastFrameAck",
                json!({ "sessionId": stream_id }),
                None,
            )
            .await?;
        Ok(())
    }

    /// The stream id seen so far.
    pub fn stream_id(&self) -> Option<i64> {
        self.stream_id
    }
}

/// One interaction from the panel, in CSS pixels relative to the frame's
/// top-left.
#[derive(Clone, Debug, PartialEq)]
pub enum PanelInput {
    /// The pointer moved.
    PointerMove {
        /// Horizontal position.
        x: f64,
        /// Vertical position.
        y: f64,
    },
    /// A pointer button went down.
    PointerDown {
        /// Horizontal position.
        x: f64,
        /// Vertical position,
        y: f64,
        /// Which button: 1 left, 2 middle, 3 right.
        button: i64,
        /// How many clicks in this burst.
        click_count: i64,
    },
    /// A pointer button came up.
    PointerUp {
        /// Horizontal position.
        x: f64,
        /// Vertical position,
        y: f64,
        /// Which button: 1 left, 2 middle, 3 right.
        button: i64,
        /// How many clicks in this burst.
        click_count: i64,
    },
    /// The wheel turned.
    Scroll {
        /// Horizontal position.
        x: f64,
        /// Vertical position.
        y: f64,
        /// Horizontal delta.
        delta_x: f64,
        /// Vertical delta.
        delta_y: f64,
    },
    /// A key went down. `key`, `code` and `text` are the three spellings CDP
    /// wants; see [`PanelInput::to_cdp`] for why all three matter.
    KeyDown {
        /// The `key` value, e.g. `Enter`, `a`, `ArrowDown`.
        key: String,
        /// The `code` value, e.g. `Enter`, `KeyA`, `ArrowDown`.
        code: String,
        /// The text the key inserts, empty for keys that insert none.
        text: String,
        /// Whether a modifier was held.
        modifiers: i64,
    },
    /// A key came up.
    KeyUp {
        /// The `key` value.
        key: String,
        /// The `code` value.
        code: String,
        /// Whether a modifier was held.
        modifiers: i64,
    },
}

impl PanelInput {
    /// The CDP method this input dispatches through.
    pub fn method(&self) -> &'static str {
        match self {
            PanelInput::KeyDown { .. } | PanelInput::KeyUp { .. } => "Input.dispatchKeyEvent",
            _ => "Input.dispatchMouseEvent",
        }
    }

    /// The CDP `type` parameter.
    ///
    /// Obscura reads these strings directly (`obscura-cdp/src/domains/input.rs`),
    /// and the two forms matter: `mouseMoved` for a move, and
    /// `mousePressed` / `mouseReleased` / `mouseWheel` for the rest.
    pub fn event_type(&self) -> &'static str {
        match self {
            PanelInput::PointerMove { .. } => "mouseMoved",
            PanelInput::PointerDown { .. } => "mousePressed",
            PanelInput::PointerUp { .. } => "mouseReleased",
            PanelInput::Scroll { .. } => "mouseWheel",
            PanelInput::KeyDown { .. } => "keyDown",
            PanelInput::KeyUp { .. } => "keyUp",
        }
    }

    /// The full parameter object for `Input.dispatchMouseEvent` or
    /// `Input.dispatchKeyEvent`.
    ///
    /// `button: "none"` on a move is what CDP specifies and what a strict
    /// engine needs; without it a move can be read as a press on button 0.
    pub fn to_cdp(&self) -> Value {
        match self {
            PanelInput::PointerMove { x, y } => json!({
                "type": self.event_type(),
                "x": x, "y": y,
                "button": "none",
                "buttons": 0,
                "clickCount": 0,
            }),
            PanelInput::PointerDown {
                x,
                y,
                button,
                click_count,
            }
            | PanelInput::PointerUp {
                x,
                y,
                button,
                click_count,
            } => json!({
                "type": self.event_type(),
                "x": x, "y": y,
                "button": button_name(*button),
                "buttons": bit_for_button(*button),
                "clickCount": click_count,
            }),
            PanelInput::Scroll {
                x,
                y,
                delta_x,
                delta_y,
            } => json!({
                "type": self.event_type(),
                "x": x, "y": y,
                "deltaX": delta_x, "deltaY": delta_y,
                "button": "none",
                "buttons": 0,
            }),
            PanelInput::KeyDown {
                key,
                code,
                text,
                modifiers,
            } => json!({
                "type": self.event_type(),
                "key": key, "code": code,
                "text": text,
                "unmodifiedText": text,
                "modifiers": modifiers,
            }),
            PanelInput::KeyUp {
                key,
                code,
                modifiers,
            } => json!({
                "type": self.event_type(),
                "key": key, "code": code,
                "modifiers": modifiers,
            }),
        }
    }

    /// Dispatch this input to the engine.
    pub async fn dispatch(&self, client: &mut CdpClient, session_id: &str) -> Result<Value> {
        client
            .call(self.method(), self.to_cdp(), Some(session_id))
            .await
    }
}

/// CDP's `button` spelling for a button number.
pub fn button_name(button: i64) -> &'static str {
    match button {
        1 => "left",
        2 => "middle",
        3 => "right",
        _ => "none",
    }
}

/// CDP's `buttons` bitmask for a button number, as observed while it is held.
pub fn bit_for_button(button: i64) -> i64 {
    match button {
        1 => 1,
        2 => 4,
        3 => 2,
        _ => 0,
    }
}
