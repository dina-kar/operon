//! `PrimaryKey` as JSON (overview A12): an integer in `0..=2^64−1` is `U64`,
//! a string is `Str`, `{"uuid": "<8-4-4-4-12, any case>"}` is `Uuid` (written
//! lowercase). Anything else is an error.

use operon_collection::PrimaryKey;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Value, json};

use crate::error::ServiceError;

/// The JSON of `pk`.
pub fn to_json(pk: &PrimaryKey) -> Value {
    match pk {
        PrimaryKey::U64(n) => json!(n),
        PrimaryKey::Str(s) => json!(s),
        PrimaryKey::Uuid(bytes) => json!({ "uuid": format_uuid(bytes) }),
    }
}

/// The key `v` names; `InvalidArgument` for any other JSON.
pub fn from_json(v: &Value) -> Result<PrimaryKey, ServiceError> {
    let invalid = || {
        ServiceError::InvalidArgument(format!(
            "invalid id {v}: expected an unsigned integer, a string or {{\"uuid\": \"…\"}}"
        ))
    };
    match v {
        Value::Number(n) => n.as_u64().map(PrimaryKey::U64).ok_or_else(invalid),
        Value::String(s) => Ok(PrimaryKey::Str(s.clone())),
        Value::Object(object) if object.len() == 1 => match object.get("uuid") {
            Some(Value::String(text)) => parse_uuid(text).map(PrimaryKey::Uuid).ok_or_else(invalid),
            _ => Err(invalid()),
        },
        _ => Err(invalid()),
    }
}

/// Lowercase hyphenated `8-4-4-4-12`.
pub(crate) fn format_uuid(bytes: &[u8; 16]) -> String {
    let mut out = String::with_capacity(36);
    for (i, byte) in bytes.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// A canonical `8-4-4-4-12` UUID, in any case.
pub(crate) fn parse_uuid(text: &str) -> Option<[u8; 16]> {
    let bytes = text.as_bytes();
    if bytes.len() != 36 {
        return None;
    }
    let mut hex = Vec::with_capacity(32);
    for (i, &b) in bytes.iter().enumerate() {
        if matches!(i, 8 | 13 | 18 | 23) {
            if b != b'-' {
                return None;
            }
        } else {
            hex.push(char::from(b).to_digit(16)? as u8);
        }
    }
    let mut out = [0u8; 16];
    for (i, pair) in hex.chunks(2).enumerate() {
        out[i] = pair[0] << 4 | pair[1];
    }
    Some(out)
}

pub fn serialize<S: Serializer>(pk: &PrimaryKey, s: S) -> Result<S::Ok, S::Error> {
    to_json(pk).serialize(s)
}

pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<PrimaryKey, D::Error> {
    let value = Value::deserialize(d)?;
    from_json(&value).map_err(D::Error::custom)
}

/// `Vec<PrimaryKey>` as a JSON list.
pub mod vec {
    use super::*;

    pub fn serialize<S: Serializer>(pks: &[PrimaryKey], s: S) -> Result<S::Ok, S::Error> {
        Value::Array(pks.iter().map(to_json).collect()).serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<PrimaryKey>, D::Error> {
        let values = Vec::<Value>::deserialize(d)?;
        values
            .iter()
            .map(|value| from_json(value).map_err(D::Error::custom))
            .collect()
    }
}

/// `Option<PrimaryKey>`; `None` is `null`.
pub mod opt {
    use super::*;

    pub fn serialize<S: Serializer>(pk: &Option<PrimaryKey>, s: S) -> Result<S::Ok, S::Error> {
        pk.as_ref().map(to_json).serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<PrimaryKey>, D::Error> {
        match Option::<Value>::deserialize(d)? {
            None | Some(Value::Null) => Ok(None),
            Some(value) => from_json(&value).map(Some).map_err(D::Error::custom),
        }
    }
}
