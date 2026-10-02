//! How an event is stored: one record laid out as the CloudEvents Kafka
//! binding's binary mode (design §02 §7.4), and how a record is read back as
//! an event.

use bytes::Bytes;
use loams_log::Record;

use crate::{
    Attr, AttrType, CloudEvent, DataKind, Error, SPEC_VERSION, format_time_ms, json, media_type,
};

/// The prefix of attribute headers.
pub const HEADER_PREFIX: &str = "ce_";
/// The header of `datacontenttype`, as in the Kafka binding.
pub const CONTENT_TYPE: &str = "content-type";
/// The header naming extensions whose type is not string:
/// `name=integer,other=boolean`.
pub const TYPES_HEADER: &str = "loams_ce_types";
/// The `type` of a synthesized envelope.
pub const RECORD_TYPE: &str = "io.loams.dev.stream.record";

/// Where a record was read from, for a synthesized envelope.
#[derive(Clone, Copy, Debug)]
pub struct RecordContext<'a> {
    pub namespace: &'a str,
    pub stream: &'a str,
    pub stream_id: u64,
    pub partition: u32,
    pub offset: u64,
}

/// The record that stores `event`: `ce_` headers in attribute order
/// (`content-type` for `datacontenttype`), the key from `partitionkey` or
/// `subject`, the timestamp from `time` (else the writer's clock), and the
/// data as the value.
pub fn to_record(event: &CloudEvent) -> Record {
    let mut headers = Vec::with_capacity(event.attrs().len() + 1);
    let mut types = Vec::new();
    for attr in event.attrs() {
        let name = if attr.name == "datacontenttype" {
            CONTENT_TYPE.to_string()
        } else {
            format!("{HEADER_PREFIX}{}", attr.name)
        };
        headers.push((name, Some(Bytes::from(attr.value.clone()))));
        if attr.kind != AttrType::String {
            types.push(format!("{}={}", attr.name, attr.kind.name()));
        }
    }
    if !types.is_empty() {
        headers.push((TYPES_HEADER.to_string(), Some(Bytes::from(types.join(",")))));
    }
    Record {
        key: event
            .key()
            .map(|key| Bytes::copy_from_slice(key.as_bytes())),
        value: event.data().cloned(),
        headers,
        timestamp_ms: event.time_ms().unwrap_or(-1),
    }
}

/// Reads a record back as an event, in the first form that applies: the
/// binary-mode layout (`ce_specversion` present), Kafka structured mode
/// (`content-type: application/cloudevents+json`), or else a synthesized
/// envelope. A record whose `ce_` headers or structured value do not make a
/// valid event is synthesized too, so reading never fails.
pub fn from_record(record: &Record, at: RecordContext<'_>) -> CloudEvent {
    let binary = record
        .headers
        .iter()
        .any(|(name, _)| name == "ce_specversion");
    let parsed = if binary {
        Some(from_binary(record))
    } else if header(record, CONTENT_TYPE).is_some_and(|ct| media_type(ct) == json::CONTENT_TYPE) {
        Some(json::parse_event(
            record.value.as_deref().unwrap_or_default(),
        ))
    } else {
        None
    };
    match parsed {
        Some(Ok(event)) => event,
        _ => synthesize(record, at),
    }
}

fn header<'r>(record: &'r Record, name: &str) -> Option<&'r str> {
    record
        .headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .and_then(|(_, value)| std::str::from_utf8(value.as_deref()?).ok())
}

fn from_binary(record: &Record) -> Result<CloudEvent, Error> {
    let mut attrs: Vec<Attr> = Vec::with_capacity(record.headers.len());
    let mut types = Vec::new();
    for (name, value) in &record.headers {
        let attr = if name.eq_ignore_ascii_case(CONTENT_TYPE) {
            "datacontenttype"
        } else if name == TYPES_HEADER {
            let value = value.as_deref().unwrap_or_default();
            types = String::from_utf8_lossy(value)
                .split(',')
                .filter_map(|entry| {
                    let (name, kind) = entry.split_once('=')?;
                    Some((name.to_string(), AttrType::from_name(kind)?))
                })
                .collect::<Vec<_>>();
            continue;
        } else if let Some(attr) = name.strip_prefix(HEADER_PREFIX) {
            attr
        } else {
            continue;
        };
        let value = value.as_deref().unwrap_or_default();
        let value = std::str::from_utf8(value).map_err(|_| Error::Header {
            name: name.clone(),
            message: "not UTF-8".into(),
        })?;
        attrs.push(Attr::string(attr, value));
    }
    for attr in &mut attrs {
        if let Some((_, kind)) = types.iter().find(|(name, _)| *name == attr.name) {
            attr.kind = *kind;
        }
    }
    CloudEvent::new(attrs, record.value.clone())
}

/// The envelope of a record that is not an event (design §02 §7.4).
pub fn synthesize(record: &Record, at: RecordContext<'_>) -> CloudEvent {
    let mut attrs = vec![
        Attr::string("specversion", SPEC_VERSION),
        Attr::string("id", format!("{}-{}", at.stream_id, at.offset)),
        Attr::string(
            "source",
            format!(
                "/namespaces/{}/streams/{}/partitions/{}",
                at.namespace, at.stream, at.partition
            ),
        ),
        Attr::string("type", RECORD_TYPE),
    ];
    let content_type = header(record, CONTENT_TYPE)
        .filter(|ct| !ct.is_empty())
        .map(str::to_string)
        .or_else(|| {
            record.value.as_deref().map(|value| {
                if DataKind::of(None, Some(value)) == DataKind::Json {
                    "application/json".to_string()
                } else {
                    "application/octet-stream".to_string()
                }
            })
        });
    if let Some(content_type) = content_type {
        attrs.push(Attr::string("datacontenttype", content_type));
    }
    if record.timestamp_ms >= 0 {
        attrs.push(Attr::string("time", format_time_ms(record.timestamp_ms)));
    }
    if let Some(key) = record
        .key
        .as_deref()
        .and_then(|key| std::str::from_utf8(key).ok())
        .filter(|key| !key.is_empty())
    {
        attrs.push(Attr::string("partitionkey", key));
    }
    // Every attribute above is valid by construction.
    CloudEvent::new(attrs.clone(), record.value.clone()).unwrap_or(CloudEvent {
        attrs,
        data: record.value.clone(),
    })
}
