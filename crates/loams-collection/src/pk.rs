use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;

use crate::error::CodecError;

const TAG_U64: u8 = 0x01;
const TAG_UUID: u8 = 0x02;
const TAG_STR: u8 = 0x03;

/// The longest string key, in bytes: Elasticsearch's `_id` limit.
pub const MAX_STR_PK_BYTES: usize = 512;

/// A document's primary key (overview §6.2).
///
/// The derived order (variants in tag order, then the value) equals the order
/// of the canonical bytes: numbers compare like their big-endian bytes and
/// strings compare bytewise.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PrimaryKey {
    U64(u64),
    Uuid([u8; 16]),
    Str(String),
}

impl PrimaryKey {
    /// The canonical bytes: `0x01` + u64 big-endian, `0x02` + 16 bytes, or
    /// `0x03` + UTF-8. They are the record key and the PK index key.
    pub fn canonical(&self) -> Vec<u8> {
        match self {
            PrimaryKey::U64(n) => {
                let mut bytes = Vec::with_capacity(9);
                bytes.push(TAG_U64);
                bytes.extend_from_slice(&n.to_be_bytes());
                bytes
            }
            PrimaryKey::Uuid(uuid) => {
                let mut bytes = Vec::with_capacity(17);
                bytes.push(TAG_UUID);
                bytes.extend_from_slice(uuid);
                bytes
            }
            PrimaryKey::Str(s) => {
                let mut bytes = Vec::with_capacity(1 + s.len());
                bytes.push(TAG_STR);
                bytes.extend_from_slice(s.as_bytes());
                bytes
            }
        }
    }

    /// Parses canonical bytes; the key must also be [valid](Self::validate).
    pub fn from_canonical(bytes: &[u8]) -> Result<Self, CodecError> {
        let (&tag, body) = bytes
            .split_first()
            .ok_or_else(|| CodecError::InvalidKey("empty key".to_string()))?;
        let pk = match tag {
            TAG_U64 => PrimaryKey::U64(u64::from_be_bytes(body.try_into().map_err(|_| {
                CodecError::InvalidKey(format!("a u64 key has {} bytes, not 8", body.len()))
            })?)),
            TAG_UUID => PrimaryKey::Uuid(body.try_into().map_err(|_| {
                CodecError::InvalidKey(format!("a uuid key has {} bytes, not 16", body.len()))
            })?),
            TAG_STR => PrimaryKey::Str(
                std::str::from_utf8(body)
                    .map_err(|_| CodecError::InvalidKey("a string key is not UTF-8".to_string()))?
                    .to_string(),
            ),
            other => {
                return Err(CodecError::InvalidKey(format!(
                    "unknown key tag {other:#04x}"
                )));
            }
        };
        pk.validate()?;
        Ok(pk)
    }

    /// A string key must have 1..=[`MAX_STR_PK_BYTES`] bytes; every number
    /// and uuid is valid.
    pub fn validate(&self) -> Result<(), CodecError> {
        match self {
            PrimaryKey::Str(s) if s.is_empty() => {
                Err(CodecError::InvalidKey("a string key is empty".to_string()))
            }
            PrimaryKey::Str(s) if s.len() > MAX_STR_PK_BYTES => {
                Err(CodecError::InvalidKey(format!(
                    "a string key has {} bytes, over {MAX_STR_PK_BYTES}",
                    s.len()
                )))
            }
            _ => Ok(()),
        }
    }
}

/// The partition of the implicit stream that carries `pk`'s records:
/// `xxh3_64(canonical bytes) % partitions`. A stream has at least one
/// partition; `partitions == 0` is treated as 1 rather than dividing by zero.
pub fn partition_of(pk: &PrimaryKey, partitions: u32) -> u32 {
    // The remainder is below `partitions`, so it fits a u32.
    (xxh3_64(&pk.canonical()) % u64::from(partitions.max(1))) as u32
}
