//! The navigation allowlist, and the CDP wire contract, against a fake engine.
//!
//! Two things are under test here, and they are related.
//!
//! The **allowlist** is the per-request control that keeps a process-wide
//! private-network relaxation from being a general-purpose browser. Its rules
//! come from zeron's browser module, which the SF1 spike read and recorded
//! (`crates/ui/src/browser/model.rs:131-138` in that project): `http` and
//! `https` only, a host is required, and userinfo is refused.
//!
//! The **wire contract** is tested against a WebSocket server that speaks
//! Obscura v0.2.3's message shapes, so the client, the frame pump and the
//! input routing are exercised end to end without needing a 70 MB browser
//! binary. The shapes come from that tag's source: a flat `sessionId` on the
//! message (`obscura-cdp/src/server.rs`), an integer `sessionId` on a
//! screencast frame, and `{id, result}` / `{id, error}` envelopes
//! (`obscura-cdp/src/types.rs`).

use std::time::Duration;

use base64::Engine as _;
use futures::{SinkExt as _, StreamExt as _};
use loams_sidebar_browser::cdp::CdpClient;
use loams_sidebar_browser::{
    CdpEvent, EmbeddedOrigin, FrameFormat, FramePump, NavigationAllowlist, PanelInput, PanelState,
    png_fixture,
};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

// ---------------------------------------------------------------- allowlist

fn app() -> EmbeddedOrigin {
    EmbeddedOrigin::parse("https://chat.example.com").expect("an origin")
}

fn allowlist() -> NavigationAllowlist {
    NavigationAllowlist::new(app())
        .allow_url("https://sso.example.com")
        .expect("a provider origin")
}

fn check(allowlist: &NavigationAllowlist, url: &str) -> Result<(), String> {
    allowlist
        .check(&url::Url::parse(url).expect("a URL"))
        .map_err(|error| error.to_string())
}

/// The embedded origin and the identity provider are reachable; nothing else is.
#[test]
fn only_the_embedded_origin_and_the_provider_are_reachable() {
    for url in [
        "https://chat.example.com/",
        "https://chat.example.com/#narrow/stream/1- Zulip",
        "https://chat.example.com/api/v1/users",
        "https://sso.example.com/application/o/authorize/",
    ] {
        assert!(
            check(&allowlist(), url).is_ok(),
            "{url} should be reachable"
        );
    }
    for url in [
        "https://evil.example.com/",
        "https://chat.example.com.evil.example/",
        // A sibling subdomain is same-site (SF1 Ruling 1) but is not the
        // embedded origin. Ruling 1 is what makes `Lax` cookies work in the
        // frame; it is not a licence to navigate anywhere on the domain.
        "https://plane.example.com/",
        "https://git.example.com/",
        "http://chat.example.com/",
        "https://chat.example.com:8443/",
        "https://user:password@chat.example.com/",
        "ftp://chat.example.com/",
        "file:///etc/passwd",
        "javascript:alert(1)",
    ] {
        assert!(
            check(&allowlist(), url).is_err(),
            "{url} should be refused: {:?}",
            check(&allowlist(), url)
        );
    }
}

/// An allowlist with no provider does not let the provider through.
///
/// The point of the explicit `allow_origin` is that adding an origin is a
/// decision; the default is the embedded origin alone.
#[test]
fn a_provider_is_not_implicit() {
    let plain = NavigationAllowlist::new(app());
    assert!(check(&plain, "https://sso.example.com/").is_err());
    assert!(check(&plain, "https://chat.example.com/").is_ok());
}

/// Adding the same provider twice is not an error and does not duplicate it.
#[test]
fn allowlist_entries_are_deduplicated() {
    let mut built = NavigationAllowlist::new(app())
        .allow_url("https://sso.example.com")
        .expect("a provider origin")
        .allow_url("https://sso.example.com")
        .expect("the same provider again");
    built =
        built.allow_origin(EmbeddedOrigin::parse("https://sso.example.com").expect("an origin"));
    assert_eq!(built.origins().len(), 2);
}

/// The origin parser refuses the things that would widen a cookie's scope.
#[test]
fn an_origin_must_be_exactly_an_origin() {
    for value in [
        "https://chat.example.com/chat",
        "https://chat.example.com/?a=1",
        "https://chat.example.com/#frag",
        "https://user@chat.example.com/",
        "ftp://chat.example.com/",
        "not a url",
        "https://",
    ] {
        assert!(
            EmbeddedOrigin::parse(value).is_err(),
            "{value:?} is not an origin"
        );
    }
    assert!(EmbeddedOrigin::parse("https://chat.example.com").is_ok());
    assert!(
        EmbeddedOrigin::parse("https://chat.example.com/").is_ok(),
        "a trailing slash is the same origin"
    );
}

// ------------------------------------------------------------- the wire

/// A minimal stand-in for the engine's CDP endpoint.
///
/// It answers each request from a routing table keyed by method and records what
/// it was sent, so a test can assert on the exact bytes this crate produces.
struct FakeEngine {
    port: u16,
}

/// The messages the fake engine has recorded.
type Recorded = std::sync::Arc<std::sync::Mutex<Vec<Value>>>;

/// Wait until the fake engine has recorded `count` messages.
///
/// The server task only ends when the client drops the socket, so a test cannot
/// await it and read a list: it waits for the count instead. Counting rather
/// than draining matters, because the assertion is usually "this specific call
/// was sent with these parameters", and the position of the ack depends on how
/// many frames the test pushed.
async fn wait_for(recorded: &Recorded, count: usize) -> Vec<Value> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let seen = recorded.lock().expect("the fake engine's record").clone();
        if seen.len() >= count || tokio::time::Instant::now() >= deadline {
            assert!(
                seen.len() >= count,
                "the fake engine recorded {} messages, expected {count}: {seen:?}",
                seen.len()
            );
            return seen;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

impl FakeEngine {
    async fn start(responses: Vec<(String, Value)>) -> (Self, Recorded) {
        Self::start_with(responses, Vec::new()).await
    }

    async fn start_with(
        responses: Vec<(String, Value)>,
        errors: Vec<(String, String)>,
    ) -> (Self, Recorded) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("a listener");
        let port = listener.local_addr().expect("an address").port();
        let recorded: Recorded = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = recorded.clone();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("an accepted connection");
            let mut socket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("a WebSocket handshake");
            while let Some(Ok(message)) = socket.next().await {
                let Message::Text(text) = message else {
                    continue;
                };
                let request: Value = serde_json::from_str(&text).expect("a JSON request");
                let method = request["method"].as_str().unwrap_or_default().to_string();
                sink.lock()
                    .expect("the fake engine's record")
                    .push(request.clone());
                let result = responses
                    .iter()
                    .find(|(name, _)| *name == method)
                    .map(|(_, value)| value.clone())
                    .unwrap_or(json!({}));
                // Obscura's envelope: `{id, result}` or `{id, error}`, with
                // `sessionId` only where one was sent.
                let mut reply = serde_json::Map::new();
                reply.insert("id".to_string(), request["id"].clone());
                match errors.iter().find(|(name, _)| *name == method) {
                    Some((_, message)) => {
                        reply.insert(
                            "error".to_string(),
                            json!({ "code": -32000, "message": message }),
                        );
                    }
                    None => {
                        reply.insert("result".to_string(), result);
                    }
                }
                if let Some(session) = request.get("sessionId") {
                    reply.insert("sessionId".to_string(), session.clone());
                }
                let text = Value::Object(reply).to_string();
                if socket.send(Message::Text(text)).await.is_err() {
                    break;
                }
            }
        });
        (Self { port }, recorded)
    }

    fn url(&self) -> String {
        format!("ws://127.0.0.1:{}/devtools/browser", self.port)
    }
}

/// A `Page.screencastFrame` event in Obscura's shape.
fn screencast_frame(stream_id: i64, png: &[u8]) -> CdpEvent {
    CdpEvent {
        method: "Page.screencastFrame".into(),
        params: json!({
            "data": base64::engine::general_purpose::STANDARD.encode(png),
            "metadata": {
                "offsetTop": 0.0,
                "pageScaleFactor": 1.0,
                "deviceWidth": 800,
                "deviceHeight": 600,
                "scrollOffsetX": 0.0,
                "scrollOffsetY": 0.0,
                "timestamp": 1.0,
            },
            "sessionId": stream_id,
        }),
        session_id: Some("session-1".into()),
    }
}

/// The full frame path: a frame arrives over the wire, decodes, lands in the
/// panel's state, and is acknowledged with an integer session id.
#[tokio::test]
async fn a_screencast_frame_is_decoded_acknowledged_and_offered() {
    let (engine, recorded) =
        FakeEngine::start(vec![("Page.screencastFrameAck".into(), json!({}))]).await;
    let mut client = CdpClient::connect(&engine.url())
        .await
        .expect("a connection");
    let mut pump = FramePump::new(FrameFormat::Png);

    let png = png_fixture(800, 600);
    let frame = pump
        .handle(&mut client, &screencast_frame(11, &png))
        .await
        .expect("a decoded frame");

    let frame = frame.expect("a decoded frame");
    assert_eq!(frame.width, 800);
    assert_eq!(frame.height, 600);
    assert_eq!(pump.stream_id(), Some(11));
    assert_eq!(pump.acknowledged(), 1);
    assert_eq!(pump.rejected(), 0);
    assert!(pump.state().has_pending_frame());

    let (drawn, dropped) = pump.state_mut().take_frame().expect("a frame to draw");
    assert_eq!(dropped, 0);
    assert_eq!(drawn.width, 800);

    let seen = wait_for(&recorded, 1).await;
    let ack = seen.last().expect("the ack to have been sent");
    assert_eq!(ack["method"], "Page.screencastFrameAck");
    assert_eq!(
        ack["params"]["sessionId"],
        json!(11),
        "Obscura parses this as an i32; a string would never be acknowledged"
    );
}

/// A frame that cannot be decoded is still acknowledged.
///
/// It consumed an engine frame slot, and leaving it unacked would wedge the
/// stream rather than skip a bad picture.
#[tokio::test]
async fn an_undecodable_frame_is_still_acknowledged() {
    let (engine, recorded) =
        FakeEngine::start(vec![("Page.screencastFrameAck".into(), json!({}))]).await;
    let mut client = CdpClient::connect(&engine.url())
        .await
        .expect("a connection");
    let mut pump = FramePump::new(FrameFormat::Png);

    let event = CdpEvent {
        method: "Page.screencastFrame".into(),
        params: json!({
            "data": base64::engine::general_purpose::STANDARD.encode(b"not a png at all"),
            "sessionId": 12,
        }),
        session_id: None,
    };
    pump.handle(&mut client, &event)
        .await
        .expect_err("a non-PNG frame must be reported");
    assert_eq!(pump.acknowledged(), 1, "the ack must still be sent");
    assert_eq!(pump.rejected(), 1);
    assert!(!pump.state().has_pending_frame());

    let seen = wait_for(&recorded, 1).await;
    assert_eq!(
        seen.last().expect("an ack")["params"]["sessionId"],
        json!(12)
    );
}

/// A frame whose session id is a string is refused with an explanation.
///
/// This is the Chromium-shaped value. Obscura wants an integer, so this is not
/// a frame we can acknowledge, and saying so is more useful than a decode error.
#[tokio::test]
async fn a_string_session_id_on_a_frame_is_refused() {
    let (engine, _seen) = FakeEngine::start(vec![]).await;
    let mut client = CdpClient::connect(&engine.url())
        .await
        .expect("a connection");
    let mut pump = FramePump::new(FrameFormat::Png);
    let event = CdpEvent {
        method: "Page.screencastFrame".into(),
        params: json!({
            "data": base64::engine::general_purpose::STANDARD.encode(png_fixture(4, 4)),
            "sessionId": "11",
        }),
        session_id: None,
    };
    let error = pump
        .handle(&mut client, &event)
        .await
        .expect_err("a string session id must be refused");
    assert!(error.to_string().contains("integer"), "got: {error}");
}

/// A call the engine answers with an error surfaces the engine's own text.
///
/// The message asserted here is Obscura's real one for a `-no-render` build
/// (`obscura-cdp/src/domains/page.rs`). It is the single most useful thing a
/// mis-installed engine can tell you, and it arrives in the same envelope as
/// every other CDP error — so if the client flattens errors into a generic
/// failure, a wrong release asset produces an unactionable bug report. That is
/// why the text is preserved verbatim.
#[tokio::test]
async fn an_engine_error_keeps_its_message() {
    const NO_RENDER: &str = "Page.startScreencast requires a build with the render feature";
    let (engine, _seen) = FakeEngine::start_with(
        Vec::new(),
        vec![("Page.startScreencast".into(), NO_RENDER.into())],
    )
    .await;
    let mut client = CdpClient::connect(&engine.url())
        .await
        .expect("a connection");
    let error = client
        .call("Page.startScreencast", json!({}), Some("session-1"))
        .await
        .expect_err("the engine's error must surface");
    let rendered = error.to_string();
    assert!(rendered.contains(NO_RENDER), "got: {rendered}");
    assert!(rendered.contains("Page.startScreencast"), "got: {rendered}");
}

/// Events that arrive while a call is in flight are buffered, not dropped.
///
/// On a live screencast the frames and the acknowledgements interleave. A
/// client that discarded an event seen while waiting for a response would lose a
/// frame every time a `Storage.setCookies` call landed on the same socket.
#[tokio::test]
async fn events_seen_while_a_call_is_in_flight_are_buffered() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("a listener");
    let port = listener.local_addr().expect("an address").port();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("an accepted connection");
        let mut socket = tokio_tungstenite::accept_async(stream)
            .await
            .expect("a WebSocket handshake");
        // Push an event before answering anything.
        let unsolicited = json!({
            "method": "Page.lifecycleEvent",
            "params": { "name": "load" },
            "sessionId": "session-1",
        });
        socket
            .send(Message::Text(unsolicited.to_string()))
            .await
            .expect("an event frame");
        while let Some(Ok(Message::Text(text))) = socket.next().await {
            let request: Value = serde_json::from_str(&text).expect("a JSON request");
            let mut reply = serde_json::Map::new();
            reply.insert("id".to_string(), request["id"].clone());
            reply.insert("result".to_string(), json!({ "ok": true }));
            if socket
                .send(Message::Text(Value::Object(reply).to_string()))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    let mut client = CdpClient::connect(&format!("ws://127.0.0.1:{port}/devtools/browser"))
        .await
        .expect("a connection");
    client
        .call("Page.enable", json!({}), None)
        .await
        .expect("a call");
    let event = client
        .take_event()
        .expect("the unsolicited event must have been buffered");
    assert_eq!(event.method, "Page.lifecycleEvent");
    assert_eq!(event.str_param("name"), Some("load"));
    assert_eq!(event.session_id.as_deref(), Some("session-1"));
    server.abort();
}

/// Pointer and key input becomes the parameter objects the engine reads.
#[tokio::test]
async fn panel_input_becomes_engine_parameters() {
    let (engine, recorded) = FakeEngine::start(vec![]).await;
    let mut client = CdpClient::connect(&engine.url())
        .await
        .expect("a connection");

    for input in [
        PanelInput::PointerMove { x: 10.0, y: 20.0 },
        PanelInput::PointerDown {
            x: 10.0,
            y: 20.0,
            button: 1,
            click_count: 1,
        },
        PanelInput::PointerUp {
            x: 10.0,
            y: 20.0,
            button: 1,
            click_count: 1,
        },
        PanelInput::Scroll {
            x: 5.0,
            y: 6.0,
            delta_x: 0.0,
            delta_y: 120.0,
        },
        PanelInput::KeyDown {
            key: "Enter".into(),
            code: "Enter".into(),
            text: "\r".into(),
            modifiers: 0,
        },
        PanelInput::KeyUp {
            key: "Enter".into(),
            code: "Enter".into(),
            modifiers: 0,
        },
    ] {
        input
            .dispatch(&mut client, "session-1")
            .await
            .expect("a dispatch");
    }

    let seen = wait_for(&recorded, 6).await;
    let methods: Vec<&str> = seen
        .iter()
        .map(|message| message["method"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(
        methods,
        [
            "Input.dispatchMouseEvent",
            "Input.dispatchMouseEvent",
            "Input.dispatchMouseEvent",
            "Input.dispatchMouseEvent",
            "Input.dispatchKeyEvent",
            "Input.dispatchKeyEvent",
        ]
    );
    for message in &seen {
        assert_eq!(
            message["sessionId"],
            json!("session-1"),
            "sessions are flat: the session id is a top-level field"
        );
    }
    // A move must carry `button: "none"`, or the engine can read it as a press.
    assert_eq!(seen[0]["params"]["button"], json!("none"));
    assert_eq!(seen[0]["params"]["type"], json!("mouseMoved"));
    assert_eq!(seen[1]["params"]["button"], json!("left"));
    assert_eq!(seen[1]["params"]["buttons"], json!(1));
    assert_eq!(seen[1]["params"]["type"], json!("mousePressed"));
    assert_eq!(seen[2]["params"]["type"], json!("mouseReleased"));
    assert_eq!(seen[3]["params"]["deltaY"], json!(120.0));
    // A key that inserts text carries it; a key release does not.
    assert_eq!(seen[4]["params"]["text"], json!("\r"));
    assert_eq!(seen[4]["params"]["type"], json!("keyDown"));
    assert_eq!(seen[5]["params"]["type"], json!("keyUp"));
    assert!(seen[5]["params"].get("text").is_none());
}

/// CDP's button names and modifier bitmask match the protocol.
#[test]
fn button_names_and_bits_match_the_protocol() {
    use loams_sidebar_browser::panel::{bit_for_button, button_name};
    assert_eq!(button_name(1), "left");
    assert_eq!(button_name(2), "middle");
    assert_eq!(button_name(3), "right");
    assert_eq!(button_name(9), "none");
    assert_eq!(bit_for_button(1), 1);
    assert_eq!(bit_for_button(2), 4);
    assert_eq!(bit_for_button(3), 2);
}

/// A slow panel drops frames rather than queueing them, and the drop is visible.
#[test]
fn a_slow_panel_drops_frames_and_says_so() {
    let mut state = PanelState::new();
    assert!(!state.has_pending_frame());

    let frame = |sequence: u64| {
        loams_sidebar_browser::decode_frame(
            1,
            &base64::engine::general_purpose::STANDARD.encode(png_fixture(8, 8)),
            FrameFormat::Png,
            sequence,
        )
        .expect("a decodable frame")
    };
    assert!(!state.offer(frame(0)), "the first frame replaces nothing");
    assert!(
        state.offer(frame(1)),
        "the second replaces an undrawn frame"
    );
    assert!(state.offer(frame(2)), "and the third");

    let (drawn, dropped) = state.take_frame().expect("a frame to draw");
    assert_eq!(drawn.sequence, 2, "the newest frame is the one kept");
    assert_eq!(dropped, 2, "the two skipped frames are reported");
    assert!(!state.has_pending_frame());
    assert!(state.take_frame().is_none());
}
