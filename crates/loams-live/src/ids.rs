//! Table, index and document ids (design §20 §4.1).
//!
//! A document id is 16 random bytes from the OS random source inside its
//! table's key range. Its text form is Crockford base32 (lower case, no
//! padding) of `varint(table) ‖ 16 bytes ‖ crc16`, so an id names its table
//! and a client can check that an id belongs to the table it claims. The
//! varint is LEB128; the checksum is CRC-16/IBM-3740 (CCITT-FALSE) over the
//! varint and the bytes, big-endian. Decoding is case-insensitive and reads
//! `i`/`l` as `1` and `o` as `0`, as Crockford's alphabet says.

use std::fmt;
use std::str::FromStr;
use std::sync::LazyLock;

use crc::{CRC_16_IBM_3740, Crc};
use data_encoding::{Encoding, Specification};
use rand::TryRngCore;
use rand::rngs::OsRng;

use crate::LiveError;

/// A table id: the `table_id:u32` of its document and index keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableId(pub u32);

/// An index id inside its table. 0 is `by_id`, 1 is `by_creation_time`,
/// user indexes start at 2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IndexId(pub u32);

impl IndexId {
    /// The built-in index on the document key.
    pub const BY_ID: IndexId = IndexId(0);
    /// The built-in index on `_creationTime`, then `_id`.
    pub const BY_CREATION_TIME: IndexId = IndexId(1);
    /// The first id of a user index.
    pub const FIRST_USER: u32 = 2;
}

/// The length of a document id's random part.
pub const DOC_ID_BYTES: usize = 16;

/// A document id: its table and 16 random bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DocId {
    pub table: TableId,
    pub bytes: [u8; DOC_ID_BYTES],
}

const CRC16: Crc<u16> = Crc::<u16>::new(&CRC_16_IBM_3740);

static CROCKFORD: LazyLock<Encoding> = LazyLock::new(|| {
    let mut spec = Specification::new();
    spec.symbols.push_str("0123456789abcdefghjkmnpqrstvwxyz");
    spec.translate.from.push_str("ABCDEFGHJKMNPQRSTVWXYZIiLlOo");
    spec.translate.to.push_str("abcdefghjkmnpqrstvwxyz111100");
    // A fixed specification, checked by the tests.
    #[allow(clippy::unwrap_used)]
    spec.encoding().unwrap()
});

impl DocId {
    /// A new id in `table` with 16 bytes from the OS random source.
    ///
    /// Mutations stay deterministic although ids are random: an id is not
    /// visible to reads until its transaction commits, and a rerun draws new
    /// ids for its own inserts (R1 plan Task 8 semantics 3).
    pub fn random(table: TableId) -> Result<Self, LiveError> {
        let mut bytes = [0; DOC_ID_BYTES];
        OsRng
            .try_fill_bytes(&mut bytes)
            .map_err(|e| LiveError::Internal(format!("the OS random source failed: {e}")))?;
        Ok(DocId { table, bytes })
    }

    fn payload(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(5 + DOC_ID_BYTES + 2);
        let mut t = self.table.0;
        loop {
            let byte = (t & 0x7f) as u8;
            t >>= 7;
            if t == 0 {
                out.push(byte);
                break;
            }
            out.push(byte | 0x80);
        }
        out.extend_from_slice(&self.bytes);
        let sum = CRC16.checksum(&out);
        out.extend_from_slice(&sum.to_be_bytes());
        out
    }
}

impl fmt::Display for DocId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&CROCKFORD.encode(&self.payload()))
    }
}

impl FromStr for DocId {
    type Err = LiveError;

    fn from_str(s: &str) -> Result<Self, LiveError> {
        let bad = |why: &str| LiveError::invalid(format!("'{s}' is not a document id: {why}"));
        let raw = CROCKFORD
            .decode(s.as_bytes())
            .map_err(|_| bad("not Crockford base32"))?;
        let mut table: u64 = 0;
        let mut at = 0;
        loop {
            let byte = *raw.get(at).ok_or_else(|| bad("too short"))?;
            if at == 5 {
                return Err(bad("the table id is too long"));
            }
            table |= u64::from(byte & 0x7f) << (7 * at);
            at += 1;
            if byte & 0x80 == 0 {
                break;
            }
        }
        let table = u32::try_from(table).map_err(|_| bad("the table id is too large"))?;
        if raw.len() != at + DOC_ID_BYTES + 2 {
            return Err(bad("wrong length"));
        }
        let (body, sum) = raw.split_at(at + DOC_ID_BYTES);
        if CRC16.checksum(body).to_be_bytes() != sum {
            return Err(bad("checksum mismatch"));
        }
        let mut bytes = [0; DOC_ID_BYTES];
        bytes.copy_from_slice(&body[at..]);
        let id = DocId {
            table: TableId(table),
            bytes,
        };
        // Only the canonical varint (the one Display writes) is accepted.
        if id.payload() != raw {
            return Err(bad("non-canonical table id"));
        }
        Ok(id)
    }
}
