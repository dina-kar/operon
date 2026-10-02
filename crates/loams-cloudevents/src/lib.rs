//! CloudEvents 1.0 for Loams streams (design §02 §7.4, D270).
//!
//! A [`CloudEvent`] keeps every attribute as the exact string it arrived
//! with, in arrival order, plus the type of each extension, so an event
//! read back from a stream round-trips byte for byte on its attributes. The
//! formats are [`json`] (structured and batched mode), [`http`] (binary
//! mode) and [`record`] (the record layout of the Kafka binding's binary
//! mode, which is how an event is stored). The protobuf format lives with
//! the gRPC service, which compiles its schema.
//!
//! `cloudevents-sdk` is not used: it normalizes `time` and typed extension
//! values when it writes them back, which breaks the round trip.

use bytes::Bytes;
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub mod http;
pub mod json;
pub mod partition;
pub mod record;

/// The only `specversion` accepted.
pub const SPEC_VERSION: &str = "1.0";

/// The context attributes other than extensions, in the order the JSON
/// writer prefers when it builds an event itself.
pub const CONTEXT_ATTRIBUTES: [&str; 8] = [
    "specversion",
    "id",
    "source",
    "type",
    "datacontenttype",
    "dataschema",
    "subject",
    "time",
];

/// The attributes every event must carry.
pub const REQUIRED_ATTRIBUTES: [&str; 4] = ["specversion", "id", "source", "type"];

/// A CloudEvents type (spec §"Type System"). Only extensions carry a type
/// other than the one the spec fixes for a context attribute; a string form
/// is kept for every value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AttrType {
    String,
    Integer,
    Boolean,
    Uri,
    UriRef,
    Timestamp,
    Binary,
}

impl AttrType {
    /// The name used in the `loams_ce_types` record header.
    pub fn name(self) -> &'static str {
        match self {
            AttrType::String => "string",
            AttrType::Integer => "integer",
            AttrType::Boolean => "boolean",
            AttrType::Uri => "uri",
            AttrType::UriRef => "urireference",
            AttrType::Timestamp => "timestamp",
            AttrType::Binary => "binary",
        }
    }

    /// The type named `name` in the `loams_ce_types` header.
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "string" => AttrType::String,
            "integer" => AttrType::Integer,
            "boolean" => AttrType::Boolean,
            "uri" => AttrType::Uri,
            "urireference" => AttrType::UriRef,
            "timestamp" => AttrType::Timestamp,
            "binary" => AttrType::Binary,
            _ => return None,
        })
    }
}

/// One attribute: its name, its value's string form (the canonical string
/// encoding of the spec's type system, exactly as it arrived), and its type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Attr {
    /// The attribute's name.
    pub name: String,
    /// The value's string form.
    pub value: String,
    /// The value's type.
    pub kind: AttrType,
}

impl Attr {
    /// A string attribute.
    pub fn string(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
            kind: AttrType::String,
        }
    }
}

/// Why an event was refused. The message names the attribute.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("missing required attribute '{0}'")]
    Missing(&'static str),
    #[error("attribute '{0}' must not be empty")]
    Empty(String),
    #[error("unsupported specversion '{0}': only '1.0' is accepted")]
    SpecVersion(String),
    #[error(
        "invalid attribute name '{0}': names are lowercase ASCII letters and digits, and 'data' and 'data_base64' are reserved"
    )]
    Name(String),
    #[error("attribute '{0}' appears more than once")]
    Duplicate(String),
    #[error("attribute 'time' is not an RFC 3339 timestamp: '{0}'")]
    Time(String),
    #[error("attribute '{name}' must be {expected}")]
    Type {
        name: String,
        expected: &'static str,
    },
    #[error("an event may carry 'data' or 'data_base64', not both")]
    DataAndBase64,
    #[error("'data_base64' is not base64: {0}")]
    Base64(String),
    #[error("not a CloudEvent in the JSON format: {0}")]
    Json(String),
    #[error("invalid header '{name}': {message}")]
    Header { name: String, message: String },
}

/// One CloudEvent: attributes in their order, and data as bytes.
///
/// The data's bytes are those of `data_base64`, the JSON text of a JSON
/// `data` member when the data is JSON, the UTF-8 of a string `data` member
/// otherwise, or the body of a binary-mode message; each writer picks the
/// representation from `datacontenttype` ([`DataKind`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudEvent {
    attrs: Vec<Attr>,
    data: Option<Bytes>,
}

impl CloudEvent {
    /// Builds and validates an event (design §02 §7.4, "Validation").
    pub fn new(attrs: Vec<Attr>, data: Option<Bytes>) -> Result<Self, Error> {
        validate(&attrs)?;
        Ok(Self { attrs, data })
    }

    /// Every attribute, in order.
    pub fn attrs(&self) -> &[Attr] {
        &self.attrs
    }

    /// The value of attribute `name`.
    pub fn attr(&self, name: &str) -> Option<&str> {
        self.attrs
            .iter()
            .find(|attr| attr.name == name)
            .map(|attr| attr.value.as_str())
    }

    /// The `id` attribute.
    pub fn id(&self) -> &str {
        self.attr("id").unwrap_or_default()
    }

    /// The `source` attribute.
    pub fn source(&self) -> &str {
        self.attr("source").unwrap_or_default()
    }

    /// The `type` attribute.
    pub fn ty(&self) -> &str {
        self.attr("type").unwrap_or_default()
    }

    /// The `datacontenttype` attribute, if present.
    pub fn datacontenttype(&self) -> Option<&str> {
        self.attr("datacontenttype")
    }

    /// The data's bytes, if the event has data.
    pub fn data(&self) -> Option<&Bytes> {
        self.data.as_ref()
    }

    /// The record key: the `partitionkey` extension, else `subject`.
    pub fn key(&self) -> Option<&str> {
        self.attr("partitionkey").or_else(|| self.attr("subject"))
    }

    /// `time` in milliseconds since the epoch, if the event has one.
    pub fn time_ms(&self) -> Option<i64> {
        let time = OffsetDateTime::parse(self.attr("time")?, &Rfc3339).ok()?;
        i64::try_from(time.unix_timestamp_nanos() / 1_000_000).ok()
    }

    /// The idempotency key: SHA-256 of `source`, a zero byte and `id`.
    pub fn dedup_key(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(self.source().as_bytes());
        hash.update([0u8]);
        hash.update(self.id().as_bytes());
        hash.finalize().into()
    }

    /// How the data is written in the JSON format.
    pub fn data_kind(&self) -> DataKind {
        DataKind::of(self.datacontenttype(), self.data.as_deref())
    }
}

/// How the JSON writer carries an event's data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataKind {
    None,
    /// `data` holds the bytes as JSON text: `datacontenttype` is absent or
    /// JSON, and the bytes are valid JSON.
    Json,
    /// `data` holds the bytes as a JSON string: a textual content type and
    /// UTF-8 bytes.
    Text,
    /// `data_base64`.
    Binary,
}

impl DataKind {
    pub fn of(content_type: Option<&str>, data: Option<&[u8]>) -> Self {
        let Some(data) = data else {
            return DataKind::None;
        };
        let media = content_type.map(media_type);
        let json_type = media.as_deref().is_none_or(is_json_media);
        if json_type && serde_json::from_slice::<&serde_json::value::RawValue>(data).is_ok() {
            return DataKind::Json;
        }
        if media.as_deref().is_some_and(is_text_media) && std::str::from_utf8(data).is_ok() {
            return DataKind::Text;
        }
        DataKind::Binary
    }
}

/// The media type of a content type, lowercased, without parameters.
pub fn media_type(content_type: &str) -> String {
    content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

/// `application/json`, `text/json` and every `+json` type.
pub fn is_json_media(media: &str) -> bool {
    media == "application/json" || media == "text/json" || media.ends_with("+json")
}

fn is_text_media(media: &str) -> bool {
    media.starts_with("text/") || media == "application/xml" || media.ends_with("+xml")
}

/// Extension names: lowercase ASCII letters and digits, and not a JSON
/// member the format reserves.
pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        && name != "data"
}

fn validate(attrs: &[Attr]) -> Result<(), Error> {
    for (i, attr) in attrs.iter().enumerate() {
        if !valid_name(&attr.name) {
            return Err(Error::Name(attr.name.clone()));
        }
        if attrs[..i].iter().any(|other| other.name == attr.name) {
            return Err(Error::Duplicate(attr.name.clone()));
        }
        let context = CONTEXT_ATTRIBUTES.contains(&attr.name.as_str());
        if context && attr.value.is_empty() {
            return Err(Error::Empty(attr.name.clone()));
        }
        if context
            && !matches!(
                attr.kind,
                AttrType::String | AttrType::Uri | AttrType::UriRef | AttrType::Timestamp
            )
        {
            return Err(Error::Type {
                name: attr.name.clone(),
                expected: "a string",
            });
        }
        match attr.kind {
            AttrType::Integer
                if attr
                    .value
                    .parse::<i32>()
                    .map_or(true, |n| n.to_string() != attr.value) =>
            {
                return Err(Error::Type {
                    name: attr.name.clone(),
                    expected: "a 32-bit integer",
                });
            }
            AttrType::Boolean if !matches!(attr.value.as_str(), "true" | "false") => {
                return Err(Error::Type {
                    name: attr.name.clone(),
                    expected: "a boolean",
                });
            }
            AttrType::Timestamp if OffsetDateTime::parse(&attr.value, &Rfc3339).is_err() => {
                return Err(Error::Type {
                    name: attr.name.clone(),
                    expected: "an RFC 3339 timestamp",
                });
            }
            _ => {}
        }
    }
    for name in REQUIRED_ATTRIBUTES {
        if !attrs.iter().any(|attr| attr.name == name) {
            return Err(Error::Missing(name));
        }
    }
    let specversion = attrs
        .iter()
        .find(|attr| attr.name == "specversion")
        .map(|attr| attr.value.as_str())
        .unwrap_or_default();
    if specversion != SPEC_VERSION {
        return Err(Error::SpecVersion(specversion.to_string()));
    }
    if let Some(time) = attrs.iter().find(|attr| attr.name == "time")
        && OffsetDateTime::parse(&time.value, &Rfc3339).is_err()
    {
        return Err(Error::Time(time.value.clone()));
    }
    Ok(())
}

/// A timestamp as RFC 3339 in UTC with milliseconds, as synthesized
/// envelopes carry it.
pub fn format_time_ms(ms: i64) -> String {
    let time = OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        time.year(),
        u8::from(time.month()),
        time.day(),
        time.hour(),
        time.minute(),
        time.second(),
        time.millisecond()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal() -> Vec<Attr> {
        vec![
            Attr::string("specversion", "1.0"),
            Attr::string("id", "1"),
            Attr::string("source", "/s"),
            Attr::string("type", "t"),
        ]
    }

    #[test]
    fn requires_the_four_attributes() {
        for missing in REQUIRED_ATTRIBUTES {
            let attrs = minimal()
                .into_iter()
                .filter(|attr| attr.name != missing)
                .collect();
            assert_eq!(CloudEvent::new(attrs, None), Err(Error::Missing(missing)));
        }
        assert!(CloudEvent::new(minimal(), None).is_ok());
    }

    #[test]
    fn refuses_other_spec_versions_and_empty_values() {
        let mut attrs = minimal();
        attrs[0].value = "0.3".into();
        assert_eq!(
            CloudEvent::new(attrs, None),
            Err(Error::SpecVersion("0.3".into()))
        );
        let mut attrs = minimal();
        attrs[1].value = String::new();
        assert_eq!(CloudEvent::new(attrs, None), Err(Error::Empty("id".into())));
    }

    #[test]
    fn refuses_bad_names_times_and_types() {
        let mut attrs = minimal();
        attrs.push(Attr::string("Bad", "x"));
        assert_eq!(CloudEvent::new(attrs, None), Err(Error::Name("Bad".into())));
        let mut attrs = minimal();
        attrs.push(Attr::string("time", "yesterday"));
        assert_eq!(
            CloudEvent::new(attrs, None),
            Err(Error::Time("yesterday".into()))
        );
        let mut attrs = minimal();
        attrs.push(Attr {
            name: "n".into(),
            value: "1.5".into(),
            kind: AttrType::Integer,
        });
        assert!(matches!(
            CloudEvent::new(attrs, None),
            Err(Error::Type { .. })
        ));
        let mut attrs = minimal();
        attrs.push(Attr::string("id", "2"));
        assert_eq!(
            CloudEvent::new(attrs, None),
            Err(Error::Duplicate("id".into()))
        );
    }

    #[test]
    fn key_time_and_dedup_key() {
        let mut attrs = minimal();
        attrs.push(Attr::string("subject", "sub"));
        attrs.push(Attr::string("time", "2026-09-30T12:00:00.250+02:00"));
        let event = CloudEvent::new(attrs.clone(), None).unwrap();
        assert_eq!(event.key(), Some("sub"));
        assert_eq!(event.time_ms(), Some(1_790_762_400_250));
        attrs.push(Attr::string("partitionkey", "pk"));
        let keyed = CloudEvent::new(attrs, None).unwrap();
        assert_eq!(keyed.key(), Some("pk"));
        assert_eq!(event.dedup_key(), keyed.dedup_key());
        let mut other = minimal();
        other[2].value = "/s2".into();
        assert_ne!(
            CloudEvent::new(other, None).unwrap().dedup_key(),
            event.dedup_key()
        );
    }

    #[test]
    fn data_kinds() {
        assert_eq!(DataKind::of(None, Some(b"{\"a\":1}")), DataKind::Json);
        assert_eq!(DataKind::of(None, Some(b"not json")), DataKind::Binary);
        assert_eq!(
            DataKind::of(
                Some("application/cloudevents+json; charset=utf-8"),
                Some(b"1")
            ),
            DataKind::Json
        );
        assert_eq!(
            DataKind::of(Some("text/plain"), Some(b"hi")),
            DataKind::Text
        );
        assert_eq!(
            DataKind::of(Some("text/plain"), Some(&[0xff])),
            DataKind::Binary
        );
        assert_eq!(
            DataKind::of(Some("application/octet-stream"), Some(b"1")),
            DataKind::Binary
        );
        assert_eq!(DataKind::of(Some("text/plain"), None), DataKind::None);
    }

    #[test]
    fn formats_times_in_utc_with_milliseconds() {
        assert_eq!(
            format_time_ms(1_790_762_400_250),
            "2026-09-30T10:00:00.250Z"
        );
        assert_eq!(format_time_ms(0), "1970-01-01T00:00:00.000Z");
    }
}
