//! The JSON event format: structured mode (`application/cloudevents+json`)
//! and batched mode (`application/cloudevents-batch+json`).
//!
//! Members keep their order and their exact string values; `data` keeps its
//! JSON text, so JSON data is written back unchanged.

use std::fmt;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use serde::Deserializer;
use serde::de::{MapAccess, Visitor};
use serde_json::value::RawValue;

use crate::{
    Attr, AttrType, CONTEXT_ATTRIBUTES, CloudEvent, DataKind, Error, is_json_media, media_type,
};

/// The structured-mode content type.
pub const CONTENT_TYPE: &str = "application/cloudevents+json";
/// The batched-mode content type.
pub const BATCH_CONTENT_TYPE: &str = "application/cloudevents-batch+json";

/// An invalid event of a batch, by index.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("event {index}: {error}")]
pub struct BatchError {
    pub index: usize,
    pub error: Error,
}

/// A JSON object's members in order, refusing repeated names.
struct Members(Vec<(String, Box<RawValue>)>);

impl<'de> serde::Deserialize<'de> for Members {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Members;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a JSON object")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Members, A::Error> {
                let mut members: Vec<(String, Box<RawValue>)> = Vec::new();
                let mut seen = std::collections::HashSet::new();
                while let Some((name, value)) = map.next_entry::<String, Box<RawValue>>()? {
                    if !seen.insert(name.clone()) {
                        return Err(serde::de::Error::custom(format!(
                            "member '{name}' appears more than once"
                        )));
                    }
                    members.push((name, value));
                }
                Ok(Members(members))
            }
        }
        deserializer.deserialize_map(V)
    }
}

/// Parses one event in the JSON format.
pub fn parse_event(body: &[u8]) -> Result<CloudEvent, Error> {
    let members: Members =
        serde_json::from_slice(body).map_err(|err| Error::Json(err.to_string()))?;
    from_members(members)
}

/// Parses a batch: a JSON array of events. The first invalid event refuses
/// the batch.
pub fn parse_batch(body: &[u8]) -> Result<Vec<CloudEvent>, BatchError> {
    let items: Vec<&RawValue> = serde_json::from_slice(body).map_err(|err| BatchError {
        index: 0,
        error: Error::Json(format!("a batch is a JSON array of events: {err}")),
    })?;
    items
        .into_iter()
        .enumerate()
        .map(|(index, item)| {
            serde_json::from_str::<Members>(item.get())
                .map_err(|err| Error::Json(err.to_string()))
                .and_then(from_members)
                .map_err(|error| BatchError { index, error })
        })
        .collect()
}

fn from_members(Members(members): Members) -> Result<CloudEvent, Error> {
    let mut attrs = Vec::with_capacity(members.len());
    let mut data: Option<Box<RawValue>> = None;
    let mut data_base64: Option<Box<RawValue>> = None;
    for (name, raw) in members {
        let text = raw.get();
        if text == "null" {
            continue;
        }
        match name.as_str() {
            "data" => data = Some(raw),
            "data_base64" => data_base64 = Some(raw),
            _ => attrs.push(attr(name, text)?),
        }
    }
    let content_type = attrs
        .iter()
        .find(|attr| attr.name == "datacontenttype")
        .map(|attr| attr.value.clone());
    let data = match (data, data_base64) {
        (Some(_), Some(_)) => return Err(Error::DataAndBase64),
        (None, Some(raw)) => {
            let encoded: String = serde_json::from_str(raw.get()).map_err(|_| Error::Type {
                name: "data_base64".into(),
                expected: "a string",
            })?;
            Some(Bytes::from(
                BASE64
                    .decode(encoded.as_bytes())
                    .map_err(|err| Error::Base64(err.to_string()))?,
            ))
        }
        (Some(raw), None) => {
            let json_type = content_type
                .as_deref()
                .is_none_or(|ct| is_json_media(&media_type(ct)));
            match serde_json::from_str::<String>(raw.get()) {
                // A string `data` of a non-JSON type is the data itself.
                Ok(text) if !json_type => Some(Bytes::from(text)),
                _ => Some(Bytes::copy_from_slice(raw.get().as_bytes())),
            }
        }
        (None, None) => None,
    };
    CloudEvent::new(attrs, data)
}

fn attr(name: String, text: &str) -> Result<Attr, Error> {
    let bad_type = |name: String, expected| Error::Type { name, expected };
    if text.starts_with('"') {
        let value: String =
            serde_json::from_str(text).map_err(|err| Error::Json(err.to_string()))?;
        return Ok(Attr {
            name,
            value,
            kind: AttrType::String,
        });
    }
    if CONTEXT_ATTRIBUTES.contains(&name.as_str()) {
        return Err(bad_type(name, "a string"));
    }
    match text {
        "true" | "false" => Ok(Attr {
            name,
            value: text.to_string(),
            kind: AttrType::Boolean,
        }),
        _ if text.parse::<i32>().is_ok() => Ok(Attr {
            name,
            value: text.to_string(),
            kind: AttrType::Integer,
        }),
        _ => Err(bad_type(name, "a string, a 32-bit integer or a boolean")),
    }
}

/// Writes one event in the JSON format: the attributes in order, then the
/// data.
pub fn write_event(event: &CloudEvent) -> Vec<u8> {
    let mut out = Vec::with_capacity(256 + event.data().map_or(0, Bytes::len));
    write_into(event, &mut out);
    out
}

/// Writes a batch.
pub fn write_batch<'a>(events: impl IntoIterator<Item = &'a CloudEvent>) -> Vec<u8> {
    let mut out = vec![b'['];
    for (i, event) in events.into_iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        write_into(event, &mut out);
    }
    out.push(b']');
    out
}

fn push_string(out: &mut Vec<u8>, value: &str) {
    // Serializing a `str` cannot fail.
    out.extend_from_slice(serde_json::to_string(value).unwrap_or_default().as_bytes());
}

fn write_into(event: &CloudEvent, out: &mut Vec<u8>) {
    out.push(b'{');
    for (i, attr) in event.attrs().iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        push_string(out, &attr.name);
        out.push(b':');
        match attr.kind {
            AttrType::Integer | AttrType::Boolean => out.extend_from_slice(attr.value.as_bytes()),
            _ => push_string(out, &attr.value),
        }
    }
    let data = event.data().map(|data| &data[..]);
    match (event.data_kind(), data) {
        (DataKind::Json, Some(data)) => {
            out.extend_from_slice(b",\"data\":");
            out.extend_from_slice(data);
        }
        (DataKind::Text, Some(data)) => {
            out.extend_from_slice(b",\"data\":");
            push_string(out, &String::from_utf8_lossy(data));
        }
        (DataKind::Binary, Some(data)) => {
            out.extend_from_slice(b",\"data_base64\":");
            push_string(out, &BASE64.encode(data));
        }
        _ => {}
    }
    out.push(b'}');
}
