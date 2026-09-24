use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::LogError;

/// One record: an optional key and value, headers, and a timestamp.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Record {
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    /// Header keys may repeat, as in Kafka.
    pub headers: Vec<(String, Option<Bytes>)>,
    /// Milliseconds since the epoch. The writer replaces a negative timestamp
    /// with its own clock.
    pub timestamp_ms: i64,
}

/// A record with the offset the log assigned to it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OffsetRecord {
    pub offset: u64,
    pub record: Record,
}

/// How a WAL chunk or segment encodes its records (design §02 §5, D20).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(into = "u8", try_from = "u8")]
#[repr(u8)]
pub enum Encoding {
    /// Kafka `RecordBatch` v2.
    Kafka = 0,
    /// Arrow IPC record batches. Reserved: readers return
    /// [`LogError::UnsupportedEncoding`].
    Arrow = 1,
}

impl From<Encoding> for u8 {
    fn from(encoding: Encoding) -> u8 {
        encoding as u8
    }
}

impl TryFrom<u8> for Encoding {
    type Error = LogError;

    fn try_from(value: u8) -> Result<Self, LogError> {
        match value {
            0 => Ok(Encoding::Kafka),
            1 => Ok(Encoding::Arrow),
            other => Err(LogError::UnsupportedEncoding(other)),
        }
    }
}

impl Encoding {
    /// Fails with [`LogError::UnsupportedEncoding`] unless this build can
    /// read the encoding.
    pub fn ensure_readable(self) -> Result<(), LogError> {
        match self {
            Encoding::Kafka => Ok(()),
            Encoding::Arrow => Err(LogError::UnsupportedEncoding(self.into())),
        }
    }
}
