//! Turning a `Page.screencastFrame` payload into something a panel can draw.
//!
//! # The wire shape, verified against the pinned engine
//!
//! Obscura v0.2.3 emits the frame as base64 of a **PNG** produced by its
//! software rasteriser (`obscura-cdp/src/domains/page.rs`: `queue_screencast_frame`
//! encodes `page.screenshot_with_animation_sample(...)` and base64s it), with
//! metadata carrying `deviceWidth`, `deviceHeight`, `scrollOffsetX`,
//! `scrollOffsetY` and a timestamp. `format: "jpeg"` is accepted by the engine
//! too, in which case the payload is JPEG.
//!
//! # Why only the header is parsed
//!
//! The panel needs the dimensions and the pixels. The dimensions are in the PNG
//! `IHDR` chunk, which is at a fixed offset, so this module reads them with no
//! image dependency at all — no `image` crate, no decoder, and a decode cost
//! that does not grow with frame size. The GPUI side does the actual raster.
//! JPEG is refused rather than guessed at, because there is no fixed-offset
//! width and height in a JPEG header and pretending otherwise would mean
//! either a dependency or a scan for a marker that can appear inside entropy-
//! coded data.

use base64::Engine as _;

use crate::error::{Result, SidebarBrowserError};

/// The 8-byte PNG signature.
pub const PNG_SIGNATURE: [u8; 8] = [0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a];

/// The maximum frame accepted, in bytes.
///
/// A screencast frame of a docked sidebar panel at 2x is a few hundred KB of
/// PNG; this ceiling is generous for that and small enough that a hostile or
/// broken engine cannot make the desktop allocate without bound. It is
/// deliberately checked *before* the base64 decode, so the allocation that
/// matters is the decoded one.
pub const MAX_FRAME_BYTES: usize = 32 * 1024 * 1024;

/// One decoded frame, ready to hand to the panel.
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedFrame {
    /// The engine's stream id, echoed back in `Page.screencastFrameAck`.
    ///
    /// Note the type: Obscura's `screencastFrameAck` parses `sessionId` as an
    /// **integer** (`page.rs::screencast_int32`), where Chromium's CDP uses a
    /// string. [`crate::panel`] sends the integer form.
    pub stream_id: i64,
    /// The frame's bytes, PNG unless [`FrameFormat::Jpeg`] was requested.
    pub bytes: Vec<u8>,
    /// The encoded format.
    pub format: FrameFormat,
    /// Width in CSS pixels, from the PNG header.
    pub width: u32,
    /// Height in CSS pixels, from the PNG header.
    pub height: u32,
    /// How many frames the engine has delivered on this stream.
    pub sequence: u64,
}

impl DecodedFrame {
    /// The `Page.screencastFrameAck` parameter object.
    pub fn ack_params(&self) -> serde_json::Value {
        serde_json::json!({ "sessionId": self.stream_id })
    }

    /// The encoded size in bytes.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether the frame carries no pixels.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

/// The frame encodings Obscura's screencast can produce.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameFormat {
    /// PNG. The default, and the only one [`decode_frame`] decodes.
    Png,
    /// JPEG. Requested with `format: "jpeg"`; refused by [`decode_frame`].
    Jpeg,
}

impl FrameFormat {
    /// The CDP spelling for `Page.startScreencast`'s `format` parameter.
    pub fn as_str(self) -> &'static str {
        match self {
            FrameFormat::Png => "png",
            FrameFormat::Jpeg => "jpeg",
        }
    }
}

/// Decode one `Page.screencastFrame` payload.
///
/// `sequence` is supplied by the caller rather than counted here, because the
/// frame stream's own numbering lives in the panel, not in the transport.
pub fn decode_frame(
    stream_id: i64,
    data: &str,
    requested: FrameFormat,
    sequence: u64,
) -> Result<DecodedFrame> {
    if requested == FrameFormat::Jpeg {
        return Err(SidebarBrowserError::Frame(
            "the screencast was requested as JPEG, which this decoder does not read; start the \
             screencast with format \"png\""
                .to_string(),
        ));
    }
    // Check the encoded length before decoding so an oversized payload is
    // refused without first allocating it.
    if data.len() > MAX_FRAME_BYTES {
        return Err(SidebarBrowserError::Frame(format!(
            "frame payload is {} base64 bytes, over the {MAX_FRAME_BYTES}-byte limit",
            data.len()
        )));
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data.as_bytes())
        .map_err(|error| {
            SidebarBrowserError::Frame(format!("frame payload is not standard base64: {error}"))
        })?;
    let (width, height) = png_dimensions(&bytes)?;
    Ok(DecodedFrame {
        stream_id,
        bytes,
        format: FrameFormat::Png,
        width,
        height,
        sequence,
    })
}

/// Read a PNG's dimensions out of its `IHDR` chunk.
///
/// The `IHDR` chunk is required to be first by the PNG specification, so this
/// is a fixed-offset read: 8 signature bytes, then a 4-byte big-endian length
/// that must be 13, then the 4-byte type `IHDR`, then width and height as
/// big-endian `u32`. Anything that does not match that shape is an error rather
/// than a guess, because a panel that draws at the wrong size is worse than a
/// panel that shows an error.
pub fn png_dimensions(bytes: &[u8]) -> Result<(u32, u32)> {
    let reject = |reason: String| Err(SidebarBrowserError::Frame(reason));
    if bytes.len() < PNG_SIGNATURE.len() {
        return reject(format!(
            "frame is {} bytes, too short to hold a PNG signature",
            bytes.len()
        ));
    }
    if bytes[..PNG_SIGNATURE.len()] != PNG_SIGNATURE {
        return reject("frame is not a PNG: bad signature".into());
    }
    // 8 signature + 4 length + 4 type + 4 width + 4 height.
    if bytes.len() < 33 {
        return reject("frame is truncated inside the PNG IHDR chunk".into());
    }
    let declared = u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
    if declared != 13 {
        return reject(format!("PNG IHDR declares a {declared}-byte chunk, not 13"));
    }
    if &bytes[12..16] != b"IHDR" {
        return reject("the first PNG chunk is not IHDR".into());
    }
    let width = u32::from_be_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]);
    let height = u32::from_be_bytes([bytes[20], bytes[21], bytes[22], bytes[23]]);
    if width == 0 || height == 0 {
        return reject(format!("PNG IHDR declares {width}x{height}"));
    }
    Ok((width, height))
}

/// Build a minimal, valid PNG of the given size, for tests and for the
/// fixture the frame-decode path is exercised against.
///
/// Emits signature, `IHDR`, an empty `IDAT` and `IEND`. The compressed stream
/// is a zlib empty block, which decodes to zero pixels; nothing in this crate
/// rasterises, so the pixels are never read. It exists so that a test can
/// assert on real header bytes instead of on a hand-written array that only
/// looks like a PNG.
pub fn png_fixture(width: u32, height: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(&PNG_SIGNATURE);
    let mut ihdr = Vec::with_capacity(13);
    ihdr.extend_from_slice(&width.to_be_bytes());
    ihdr.extend_from_slice(&height.to_be_bytes());
    ihdr.extend_from_slice(&[8, 6, 0, 0, 0]); // 8-bit, RGBA, no interlace
    push_chunk(&mut out, b"IHDR", &ihdr);
    push_chunk(
        &mut out,
        b"IDAT",
        &[0x78, 0x01, 0x01, 0x00, 0x00, 0x00, 0xff],
    );
    push_chunk(&mut out, b"IEND", &[]);
    out
}

fn push_chunk(out: &mut Vec<u8>, kind: &[u8; 4], data: &[u8]) {
    out.extend_from_slice(&(data.len() as u32).to_be_bytes());
    out.extend_from_slice(kind);
    out.extend_from_slice(data);
    let mut crc = Crc32::new();
    crc.update(kind);
    crc.update(data);
    out.extend_from_slice(&crc.finish().to_be_bytes());
}

/// A CRC-32 (IEEE) over the chunk type and data, for the test fixture.
///
/// Written out because the crate deliberately has no image dependency, and a
/// fixture does not justify one.
struct Crc32 {
    value: u32,
}

impl Crc32 {
    fn new() -> Self {
        Self { value: 0xffff_ffff }
    }

    fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            let index = ((self.value ^ u32::from(*byte)) & 0xff) as usize;
            self.value = CRC_TABLE[index] ^ (self.value >> 8);
        }
    }

    fn finish(self) -> u32 {
        self.value ^ 0xffff_ffff
    }
}

const CRC_TABLE: [u32; 256] = build_crc_table();

const fn build_crc_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut index = 0usize;
    while index < 256 {
        let mut value = index as u32;
        let mut bit = 0;
        while bit < 8 {
            value = if value & 1 != 0 {
                0xedb8_8320 ^ (value >> 1)
            } else {
                value >> 1
            };
            bit += 1;
        }
        table[index] = value;
        index += 1;
    }
    table
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc_table_is_the_ieee_polynomial_table() {
        // The published CRC-32 of "123456789" is 0xcbf43926.
        let mut crc = Crc32::new();
        crc.update(b"123456789");
        assert_eq!(crc.finish(), 0xcbf4_3926);
    }
}
