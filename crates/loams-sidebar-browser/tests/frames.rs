//! The frame-decode path: a `Page.screencastFrame` payload into something the
//! panel can draw.
//!
//! The wire shape asserted here is Obscura v0.2.3's, read from that tag's
//! source rather than from its documentation: the frame is base64 of a PNG
//! produced by the software rasteriser, and the stream id arrives as an
//! **integer** on the frame's `sessionId` (`obscura-cdp/src/domains/page.rs`,
//! `queue_screencast_frame`). Chromium's CDP uses a string there, so getting
//! this wrong fails to acknowledge the frame and silently stalls the stream —
//! which is why [`crate::frame::DecodedFrame::ack_params`] is asserted
//! separately here.

use base64::Engine as _;
use loams_sidebar_browser::{
    DecodedFrame, FrameFormat, MAX_FRAME_BYTES, decode_frame, png_dimensions, png_fixture,
};
use serde_json::json;

fn encode(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// The path the engine actually takes: base64 in, dimensions and pixels out.
#[test]
fn a_png_frame_decodes_with_its_dimensions() {
    let png = png_fixture(1280, 800);
    let frame = decode_frame(7, &encode(&png), FrameFormat::Png, 0).expect("a decodable frame");
    assert_eq!(frame.stream_id, 7);
    assert_eq!(frame.width, 1280);
    assert_eq!(frame.height, 800);
    assert_eq!(frame.format, FrameFormat::Png);
    assert_eq!(frame.bytes, png, "the pixels must pass through untouched");
    assert_eq!(frame.len(), png.len());
    assert!(!frame.is_empty());
    assert_eq!(frame.sequence, 0);
}

/// The sequence number is the panel's, passed through rather than counted here.
#[test]
fn the_sequence_number_is_passed_through() {
    let png = png_fixture(64, 64);
    let frame = decode_frame(1, &encode(&png), FrameFormat::Png, 41).expect("a decodable frame");
    assert_eq!(frame.sequence, 41);
}

/// The ack parameter is an integer, which is Obscura's shape and not Chromium's.
#[test]
fn the_ack_uses_an_integer_session_id() {
    let png = png_fixture(32, 32);
    let frame = decode_frame(9, &encode(&png), FrameFormat::Png, 0).expect("a decodable frame");
    assert_eq!(
        frame.ack_params(),
        json!({ "sessionId": 9 }),
        "Obscura parses this as an i32; a string here would never be acknowledged"
    );
    assert!(
        frame.ack_params()["sessionId"].is_i64(),
        "the session id must serialize as a number"
    );
}

/// PNG dimensions come from `IHDR` at a fixed offset, with no image decoder.
#[test]
fn png_dimensions_are_read_from_the_ihdr_chunk() {
    let png = png_fixture(1, 1);
    assert_eq!(png_dimensions(&png).expect("dimensions"), (1, 1));
    assert_eq!(
        png_dimensions(png_fixture(16, 9).as_slice()).expect("dimensions"),
        (16, 9)
    );
    // The fixture's first chunk really is IHDR, so the fixed-offset read is
    // reading a real chunk and not accidentally landing on the right bytes.
    assert_eq!(&png[12..16], b"IHDR");
}

/// A payload that is not a PNG is refused rather than drawn at a guessed size.
#[test]
fn a_non_png_payload_is_refused() {
    let jpeg = [0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, b'J', b'F', b'I', b'F'];
    let error = decode_frame(1, &encode(&jpeg), FrameFormat::Png, 0)
        .expect_err("a JPEG payload must not decode as a PNG");
    assert!(error.to_string().contains("bad signature"), "got: {error}");
}

/// Truncated, empty and over-long frames are each refused with their own reason.
#[test]
fn malformed_frames_are_refused() {
    let empty = decode_frame(1, "", FrameFormat::Png, 0).expect_err("an empty payload");
    assert!(empty.to_string().contains("too short"), "got: {empty}");

    let not_base64 = decode_frame(1, "not base64 !!!", FrameFormat::Png, 0)
        .expect_err("a payload that is not base64");
    assert!(
        not_base64.to_string().contains("base64"),
        "got: {not_base64}"
    );

    let truncated_png = png_fixture(16, 16)[..20].to_vec();
    let truncated =
        decode_frame(1, &encode(&truncated_png), FrameFormat::Png, 0).expect_err("a truncated PNG");
    assert!(truncated.to_string().contains("IHDR"), "got: {truncated}");

    let zero = png_fixture(0, 0);
    let zero_dimensions =
        decode_frame(1, &encode(&zero), FrameFormat::Png, 0).expect_err("a zero-sized frame");
    assert!(
        zero_dimensions.to_string().contains("0x0"),
        "got: {zero_dimensions}"
    );
}

/// A payload that is bigger than the ceiling is refused *before* it is decoded,
/// so a broken or hostile engine cannot make the desktop allocate.
#[test]
fn an_oversized_payload_is_refused_without_being_decoded() {
    let oversized = "A".repeat(MAX_FRAME_BYTES + 4);
    let error = decode_frame(1, &oversized, FrameFormat::Png, 0).expect_err("an oversized payload");
    assert!(
        error.to_string().contains("over the"),
        "the error should say it is over the limit, got: {error}"
    );
}

/// JPEG is refused with an actionable message rather than a decode failure.
///
/// The engine accepts `format: "jpeg"` on `Page.startScreencast`, so this is a
/// reachable state, and the message says what to do about it.
#[test]
fn a_jpeg_screencast_is_refused_with_an_actionable_message() {
    let error = decode_frame(1, &encode(&png_fixture(8, 8)), FrameFormat::Jpeg, 0)
        .expect_err("a JPEG screencast must be refused");
    assert!(
        error.to_string().contains("format \"png\""),
        "the message should say how to fix it, got: {error}"
    );
}

/// `startScreencast` is always asked for PNG, with the panel's own size.
///
/// Pinning the format and the dimensions here is what makes the decoder's
/// PNG-only contract true in practice rather than by convention.
#[test]
fn the_screencast_is_requested_as_png_at_the_panels_size() {
    let params = loams_sidebar_browser::start_screencast_params(1440, 900);
    assert_eq!(params["format"], "png");
    assert_eq!(params["maxWidth"], 1440);
    assert_eq!(params["maxHeight"], 900);
    assert_eq!(
        params["everyNthFrame"], 1,
        "the panel decides what to draw; skipping engine-side frames only adds lag"
    );
}

/// A frame's bytes are never re-encoded on the way through.
///
/// The panel rasterises; this crate hands over what the engine produced. A
/// round trip through a decoder and an encoder would be the single largest
/// avoidable cost in the frame path, so it is asserted rather than assumed.
#[test]
fn pixels_are_not_re_encoded() {
    let png = png_fixture(2048, 64);
    let frame: DecodedFrame =
        decode_frame(1, &encode(&png), FrameFormat::Png, 0).expect("a decodable frame");
    assert_eq!(
        frame.bytes, png,
        "the frame must be byte-identical to what the engine produced"
    );
}
