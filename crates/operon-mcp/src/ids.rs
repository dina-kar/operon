//! Document ids on the MCP surface (plan M1.6 Task 7 rule 8).

use std::fmt::Write as _;

use operon_collection::PrimaryKey;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::error::ToolError;

/// A document id: a JSON integer (u64), a string, or `{"uuid": "…"}`.
/// JSON `true`, `-1` and `1.5` are not ids.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum DocId {
    Number(u64),
    String(String),
    Uuid { uuid: String },
}

impl TryFrom<DocId> for PrimaryKey {
    type Error = ToolError;

    fn try_from(id: DocId) -> Result<Self, ToolError> {
        match id {
            DocId::Number(n) => Ok(PrimaryKey::U64(n)),
            DocId::String(s) if s.is_empty() => {
                Err(ToolError::invalid("a string id must not be empty"))
            }
            DocId::String(s) => Ok(PrimaryKey::Str(s)),
            DocId::Uuid { uuid } => parse_uuid(&uuid).map(PrimaryKey::Uuid).ok_or_else(|| {
                ToolError::invalid(format!(
                    "uuid id {uuid:?} is not 8-4-4-4-12 hexadecimal digits"
                ))
            }),
        }
    }
}

impl From<&PrimaryKey> for DocId {
    fn from(pk: &PrimaryKey) -> Self {
        match pk {
            PrimaryKey::U64(n) => DocId::Number(*n),
            PrimaryKey::Str(s) => DocId::String(s.clone()),
            PrimaryKey::Uuid(bytes) => DocId::Uuid {
                uuid: format_uuid(bytes),
            },
        }
    }
}

/// The hyphen positions of the 8-4-4-4-12 form.
const HYPHENS: [usize; 4] = [8, 13, 18, 23];

/// Parses the 8-4-4-4-12 form, in either case.
fn parse_uuid(text: &str) -> Option<[u8; 16]> {
    let text = text.as_bytes();
    if text.len() != 36 {
        return None;
    }
    let mut digits = Vec::with_capacity(32);
    for (i, &c) in text.iter().enumerate() {
        if HYPHENS.contains(&i) {
            if c != b'-' {
                return None;
            }
            continue;
        }
        digits.push(char::from(c).to_digit(16)? as u8);
    }
    let mut bytes = [0u8; 16];
    for (byte, pair) in bytes.iter_mut().zip(digits.chunks_exact(2)) {
        *byte = (pair[0] << 4) | pair[1];
    }
    Some(bytes)
}

/// Lowercase, hyphenated.
fn format_uuid(bytes: &[u8; 16]) -> String {
    let mut out = String::with_capacity(36);
    for (i, byte) in bytes.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        let _ = write!(out, "{byte:02x}");
    }
    out
}
