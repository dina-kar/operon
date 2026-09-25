//! The PK index's values and the PK delta format (plan M1.1 Task 10,
//! Rulings 7 and 19).
//!
//! The PK index (`operon_pk::PkIndex` at `ns/<ns>/pk/collection-<cid>/`)
//! maps a key's canonical bytes to `0x01 ‖ row id u64 BE`, and holds, under
//! the reserved key [`PK_WATERMARK_KEY`](crate::PK_WATERMARK_KEY), the
//! collection manifest it reflects: `0x01 ‖ postcard(PkWatermark)`. Both
//! values live inside SlateDB, whose own format checks them, so they carry
//! no Operon envelope (controller ruling P16).
//!
//! A PK delta is the keys one commit changed, in Operon's envelope:
//!
//! ```text
//! 0   4  magic "OPPD"
//! 4   2  format version u16 LE = 1
//! 6   n  postcard(Vec<(key bytes, Option<row id>)>), keys strictly ascending
//! ..  4  crc32c u32 LE of every preceding byte
//! ```

use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::CollectionError;

/// The first byte of every PK index value.
const VALUE_VERSION: u8 = 0x01;

/// The manifest a PK index reflects: its version and its `applied` offsets.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PkWatermark {
    pub manifest_version: u64,
    pub applied: BTreeMap<u32, u64>,
}

impl PkWatermark {
    /// The watermark a multi-write rebuild writes first: it matches no
    /// manifest (no stream has partition `u32::MAX`), so an index whose
    /// rebuild was interrupted is never trusted, and is rebuilt again.
    pub fn rebuilding() -> Self {
        Self {
            manifest_version: 0,
            applied: BTreeMap::from([(u32::MAX, u64::MAX)]),
        }
    }

    /// The watermark's PK index value: `0x01 ‖ postcard(self)`.
    pub fn encode(&self) -> Bytes {
        let mut out = vec![VALUE_VERSION];
        out.extend(postcard::to_stdvec(self).expect("a plain struct always serializes"));
        Bytes::from(out)
    }

    /// Parses a watermark value.
    pub fn decode(bytes: &[u8]) -> Result<Self, CollectionError> {
        let corrupt = |why: String| CollectionError::Corrupt(format!("pk watermark: {why}"));
        let body = match bytes.split_first() {
            Some((&VALUE_VERSION, body)) => body,
            Some((version, _)) => return Err(corrupt(format!("unknown version {version}"))),
            None => return Err(corrupt("empty value".to_string())),
        };
        let (watermark, rest) =
            postcard::take_from_bytes(body).map_err(|err| corrupt(err.to_string()))?;
        if !rest.is_empty() {
            return Err(corrupt(format!("{} trailing bytes", rest.len())));
        }
        Ok(watermark)
    }
}

/// The PK index value of `row_id`: `0x01 ‖ row id u64 BE`.
pub fn pk_value(row_id: u64) -> [u8; 9] {
    let mut value = [0u8; 9];
    value[0] = VALUE_VERSION;
    value[1..].copy_from_slice(&row_id.to_be_bytes());
    value
}

/// The row id of a PK index value; anything but `0x01` and 8 bytes is
/// `Corrupt`.
pub fn parse_pk_value(bytes: &[u8]) -> Result<u64, CollectionError> {
    match bytes.split_first() {
        Some((&VALUE_VERSION, body)) => {
            let body: [u8; 8] = body.try_into().map_err(|_| {
                CollectionError::Corrupt(format!(
                    "a pk index value has {} bytes, not 9",
                    bytes.len()
                ))
            })?;
            Ok(u64::from_be_bytes(body))
        }
        _ => Err(CollectionError::Corrupt(format!(
            "a pk index value does not start with {VALUE_VERSION:#04x}: {bytes:?}"
        ))),
    }
}

pub const PK_DELTA_MAGIC: &[u8; 4] = b"OPPD";

/// One key of a PK delta: its canonical bytes and its new row id (`None`:
/// the key was deleted).
pub type PkDeltaEntry = (Vec<u8>, Option<u64>);
const PK_DELTA_VERSION: u16 = 1;
const HEADER_LEN: usize = 6;
const CRC_LEN: usize = 4;

/// Encodes one commit's PK changes, sorted by key (a key given twice keeps
/// its last entry); `None` means the key was deleted.
pub fn encode_pk_delta(entries: &[PkDeltaEntry]) -> Bytes {
    let sorted: BTreeMap<&Vec<u8>, Option<u64>> =
        entries.iter().map(|(key, row)| (key, *row)).collect();
    let body: Vec<(&Vec<u8>, Option<u64>)> = sorted.into_iter().collect();
    let body = postcard::to_stdvec(&body).expect("a Vec of plain tuples always serializes");
    seal_envelope(PK_DELTA_MAGIC, PK_DELTA_VERSION, &body)
}

/// Decodes a PK delta. A wrong magic, an unknown version, a crc mismatch, a
/// malformed body or keys that are not strictly ascending is `Corrupt`.
pub fn decode_pk_delta(bytes: &[u8]) -> Result<Vec<PkDeltaEntry>, CollectionError> {
    let corrupt = |why: String| CollectionError::Corrupt(format!("pk delta: {why}"));
    let body = open_envelope(PK_DELTA_MAGIC, PK_DELTA_VERSION, bytes).map_err(corrupt)?;
    let (entries, rest): (Vec<PkDeltaEntry>, _) =
        postcard::take_from_bytes(body).map_err(|err| corrupt(err.to_string()))?;
    if !rest.is_empty() {
        return Err(corrupt(format!("{} trailing bytes", rest.len())));
    }
    if entries.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
        return Err(corrupt("keys are not strictly ascending".to_string()));
    }
    Ok(entries)
}

/// The body of an Operon envelope (`magic ‖ u16 LE version ‖ body ‖ crc32c
/// LE`), or why it is not one.
pub(crate) fn open_envelope<'a>(
    magic: &[u8; 4],
    version: u16,
    bytes: &'a [u8],
) -> Result<&'a [u8], String> {
    if bytes.len() < HEADER_LEN + CRC_LEN {
        return Err(format!("{} bytes is too short", bytes.len()));
    }
    if &bytes[..4] != magic {
        return Err("wrong magic".to_string());
    }
    let (framed, crc) = bytes.split_at(bytes.len() - CRC_LEN);
    if crc32c::crc32c(framed).to_le_bytes() != crc {
        return Err("crc mismatch".to_string());
    }
    let found = u16::from_le_bytes([framed[4], framed[5]]);
    if found != version {
        return Err(format!("unknown format version {found}"));
    }
    Ok(&framed[HEADER_LEN..])
}

/// `magic ‖ u16 LE version ‖ body ‖ crc32c LE`.
pub(crate) fn seal_envelope(magic: &[u8; 4], version: u16, body: &[u8]) -> Bytes {
    let mut out = Vec::with_capacity(HEADER_LEN + body.len() + CRC_LEN);
    out.extend_from_slice(magic);
    out.extend_from_slice(&version.to_le_bytes());
    out.extend_from_slice(body);
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    Bytes::from(out)
}
