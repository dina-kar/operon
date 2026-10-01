//! Dead letters (plan M1.1 Ruling 11): the records one link-apply commit
//! skipped because they could not be decoded, were on the wrong partition,
//! or broke the schema, at `deadletters/<version:020>-<ulid>.dlq`:
//!
//! ```text
//! 0   4  magic "OPDL"
//! 4   2  format version u16 LE = 1
//! 6   n  postcard(Vec<DeadLetter>)
//! ..  4  crc32c u32 LE of every preceding byte
//! ```

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::CollectionError;
use crate::pkindex::{open_envelope, seal_envelope};

pub const DEAD_LETTERS_MAGIC: &[u8; 4] = b"OPDL";
const DEAD_LETTERS_VERSION: u16 = 1;

/// One skipped record: where it was, what it held, and why it was skipped.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadLetter {
    pub partition: u32,
    pub offset: u64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub reason: String,
}

/// Encodes one commit's dead letters.
pub fn encode_dead_letters(letters: &[DeadLetter]) -> Bytes {
    let body = postcard::to_stdvec(letters).expect("a slice of plain structs always serializes");
    seal_envelope(DEAD_LETTERS_MAGIC, DEAD_LETTERS_VERSION, &body)
}

/// Decodes a dead-letter object. A wrong magic, an unknown version, a crc
/// mismatch or a malformed body is `Corrupt`.
pub fn decode_dead_letters(bytes: &[u8]) -> Result<Vec<DeadLetter>, CollectionError> {
    let corrupt = |why: String| CollectionError::Corrupt(format!("dead letters: {why}"));
    let body = open_envelope(DEAD_LETTERS_MAGIC, DEAD_LETTERS_VERSION, bytes).map_err(corrupt)?;
    let (letters, rest) =
        postcard::take_from_bytes(body).map_err(|err| corrupt(err.to_string()))?;
    if !rest.is_empty() {
        return Err(corrupt(format!("{} trailing bytes", rest.len())));
    }
    Ok(letters)
}
