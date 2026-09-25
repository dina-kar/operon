//! The delete-bitmap format (plan M1.1 Task 8, Ruling 19). A split's
//! deletions are stored whole, not incrementally, at
//! `text/deletes/<split_ulid>/<ulid>.bitmap`:
//!
//! ```text
//! 0   4  magic "OPDB"
//! 4   2  format version u16 LE = 1
//! 6   16 split ULID (u128 BE)
//! 22  4  split doc_count u32 LE
//! 26  8  cardinality u64 LE
//! 34  n  RoaringBitmap::serialize_into (portable format)
//! ..  4  crc32c u32 LE of every preceding byte
//! ```

use bytes::Bytes;
use roaring::RoaringBitmap;
use ulid::Ulid;

use crate::error::TextError;

pub const DELETE_BITMAP_MAGIC: &[u8; 4] = b"OPDB";
pub const DELETE_BITMAP_VERSION: u16 = 1;

/// The bytes before the bitmap.
const HEADER_LEN: usize = 34;
const CRC_LEN: usize = 4;

/// Encodes the deleted doc ids of split `split`, which has `doc_count`
/// documents. A doc id ≥ `doc_count` is an error.
pub fn encode_delete_bitmap(
    split: Ulid,
    doc_count: u32,
    deleted: &RoaringBitmap,
) -> Result<Bytes, TextError> {
    if let Some(max) = deleted.max()
        && max >= doc_count
    {
        return Err(TextError::Other(format!(
            "doc id {max} is beyond the split's {doc_count} documents"
        )));
    }
    let mut out = Vec::with_capacity(HEADER_LEN + deleted.serialized_size() + CRC_LEN);
    out.extend_from_slice(DELETE_BITMAP_MAGIC);
    out.extend_from_slice(&DELETE_BITMAP_VERSION.to_le_bytes());
    out.extend_from_slice(&split.0.to_be_bytes());
    out.extend_from_slice(&doc_count.to_le_bytes());
    out.extend_from_slice(&deleted.len().to_le_bytes());
    deleted
        .serialize_into(&mut out)
        .map_err(|err| TextError::Other(format!("serializing a delete bitmap: {err}")))?;
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(Bytes::from(out))
}

fn corrupt(message: impl Into<String>) -> TextError {
    TextError::Corrupt(format!("delete bitmap: {}", message.into()))
}

/// Decodes a delete bitmap into its split, the split's doc count and the
/// deleted doc ids. A wrong magic, an unknown version, a crc mismatch, a
/// cardinality mismatch or a doc id ≥ the doc count is
/// [`TextError::Corrupt`].
pub fn decode_delete_bitmap(bytes: &[u8]) -> Result<(Ulid, u32, RoaringBitmap), TextError> {
    if bytes.len() < HEADER_LEN + CRC_LEN {
        return Err(corrupt(format!("{} bytes is too short", bytes.len())));
    }
    if &bytes[0..4] != DELETE_BITMAP_MAGIC {
        return Err(corrupt("wrong magic"));
    }
    let (body, crc) = bytes.split_at(bytes.len() - CRC_LEN);
    if crc32c::crc32c(body).to_le_bytes() != crc {
        return Err(corrupt("crc mismatch"));
    }
    let version = u16::from_le_bytes([body[4], body[5]]);
    if version != DELETE_BITMAP_VERSION {
        return Err(corrupt(format!("unknown format version {version}")));
    }
    let split = Ulid(u128::from_be_bytes(array(&body[6..22])));
    let doc_count = u32::from_le_bytes(array(&body[22..26]));
    let cardinality = u64::from_le_bytes(array(&body[26..34]));
    let mut portable = &body[HEADER_LEN..];
    let deleted = RoaringBitmap::deserialize_from(&mut portable)
        .map_err(|err| corrupt(format!("malformed bitmap: {err}")))?;
    if !portable.is_empty() {
        return Err(corrupt(format!(
            "{} bytes after the bitmap",
            portable.len()
        )));
    }
    if deleted.len() != cardinality {
        return Err(corrupt(format!(
            "cardinality {} != recorded {cardinality}",
            deleted.len()
        )));
    }
    if let Some(max) = deleted.max()
        && max >= doc_count
    {
        return Err(corrupt(format!(
            "doc id {max} is beyond the split's {doc_count} documents"
        )));
    }
    Ok((split, doc_count, deleted))
}

/// `bytes` as an array; the callers slice exactly `N` bytes.
fn array<const N: usize>(bytes: &[u8]) -> [u8; N] {
    let mut out = [0; N];
    out.copy_from_slice(bytes);
    out
}
