//! CloudEvents over gRPC (design §02 §7.4, D270): the protobuf format's
//! conversion to and from [`CloudEvent`], and the ingest the service calls.

use std::collections::BTreeMap;

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use loams_cloudevents::{Attr, AttrType, CloudEvent, DataKind, Error, json};
use loams_query::ServiceError;
use prost_types::Timestamp;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::proto::EventResult;

/// The protobuf format of CloudEvents (`io.cloudevents.v1`).
pub use crate::generated::io::cloudevents::v1 as cloudevents;

use cloudevents::cloud_event::cloud_event_attribute_value::Attr as Value;
use cloudevents::cloud_event::{CloudEventAttributeValue, Data};

/// What `ProduceCloudEvents` appends through; the server implements it with
/// the ledger-backed ingest that HTTP uses.
#[async_trait]
pub trait EventProducer: Send + Sync + 'static {
    /// Appends `events` to `ns/stream`, once per `source` + `id`; one result
    /// per event, in order.
    async fn produce_events(
        &self,
        ns: &str,
        stream: &str,
        partition: Option<u32>,
        events: Vec<CloudEvent>,
    ) -> Result<Vec<EventResult>, ServiceError>;
}

fn timestamp_string(timestamp: &Timestamp) -> Result<String, Error> {
    let nanos = i128::from(timestamp.seconds) * 1_000_000_000 + i128::from(timestamp.nanos);
    let time = OffsetDateTime::from_unix_timestamp_nanos(nanos).map_err(|_| Error::Type {
        name: "time".into(),
        expected: "a timestamp in range",
    })?;
    time.format(&Rfc3339)
        .map_err(|err| Error::Json(err.to_string()))
}

fn string_timestamp(value: &str) -> Option<Timestamp> {
    let time = OffsetDateTime::parse(value, &Rfc3339).ok()?;
    let nanos = time.unix_timestamp_nanos();
    Some(Timestamp {
        seconds: i64::try_from(nanos.div_euclid(1_000_000_000)).ok()?,
        nanos: i32::try_from(nanos.rem_euclid(1_000_000_000)).ok()?,
    })
}

/// Reads one event of the protobuf format. Context attributes keep their
/// string form; an extension keeps its type (`ce_integer`, …). The map has
/// no order, so the event's attributes are the required four, then the map's
/// entries by name.
///
/// `proto_data` is refused: its `type_url` has nowhere to live in a record.
pub fn from_proto(event: &cloudevents::CloudEvent) -> Result<CloudEvent, Error> {
    let mut attrs = vec![
        Attr::string("specversion", event.spec_version.clone()),
        Attr::string("id", event.id.clone()),
        Attr::string("source", event.source.clone()),
        Attr::string("type", event.r#type.clone()),
    ];
    for (name, value) in &event.attributes {
        let context = loams_cloudevents::CONTEXT_ATTRIBUTES.contains(&name.as_str());
        let mismatch = |expected: &'static str| Error::Type {
            name: name.clone(),
            expected,
        };
        let (value, kind) = match value.attr.as_ref() {
            Some(Value::CeString(s)) => (s.clone(), AttrType::String),
            Some(Value::CeUri(s)) => (s.clone(), AttrType::Uri),
            Some(Value::CeUriRef(s)) => (s.clone(), AttrType::UriRef),
            Some(Value::CeTimestamp(t)) => (timestamp_string(t)?, AttrType::Timestamp),
            _ if context => return Err(mismatch("a string")),
            Some(Value::CeBoolean(b)) => (b.to_string(), AttrType::Boolean),
            Some(Value::CeInteger(i)) => (i.to_string(), AttrType::Integer),
            Some(Value::CeBytes(b)) => (BASE64.encode(b), AttrType::Binary),
            None => return Err(mismatch("a value")),
        };
        // A context attribute is a string whatever its protobuf type, as in
        // the JSON format.
        let kind = if context { AttrType::String } else { kind };
        attrs.push(Attr {
            name: name.clone(),
            value,
            kind,
        });
    }
    let data = match &event.data {
        None => None,
        Some(Data::BinaryData(bytes)) => Some(Bytes::copy_from_slice(bytes)),
        Some(Data::TextData(text)) => Some(Bytes::copy_from_slice(text.as_bytes())),
        Some(Data::ProtoData(_)) => {
            return Err(Error::Type {
                name: "proto_data".into(),
                expected: "binary_data or text_data",
            });
        }
    };
    CloudEvent::new(attrs, data)
}

/// Writes an event in the protobuf format: textual data as `text_data`,
/// anything else as `binary_data`.
pub fn to_proto(event: &CloudEvent) -> cloudevents::CloudEvent {
    let mut attributes = BTreeMap::new();
    for attr in event.attrs() {
        if matches!(attr.name.as_str(), "specversion" | "id" | "source" | "type") {
            continue;
        }
        let value = match (attr.name.as_str(), attr.kind) {
            ("time", _) => string_timestamp(&attr.value).map(Value::CeTimestamp),
            ("dataschema", _) | (_, AttrType::Uri) => Some(Value::CeUri(attr.value.clone())),
            (_, AttrType::UriRef) => Some(Value::CeUriRef(attr.value.clone())),
            (_, AttrType::Timestamp) => string_timestamp(&attr.value).map(Value::CeTimestamp),
            (_, AttrType::Integer) => attr.value.parse().ok().map(Value::CeInteger),
            (_, AttrType::Boolean) => attr.value.parse().ok().map(Value::CeBoolean),
            (_, AttrType::Binary) => BASE64.decode(&attr.value).ok().map(Value::CeBytes),
            (_, AttrType::String) => Some(Value::CeString(attr.value.clone())),
        };
        attributes.insert(
            attr.name.clone(),
            CloudEventAttributeValue {
                attr: value.or_else(|| Some(Value::CeString(attr.value.clone()))),
            },
        );
    }
    let data = event.data().map(|bytes| match event.data_kind() {
        DataKind::Json | DataKind::Text => {
            Data::TextData(String::from_utf8_lossy(bytes).into_owned())
        }
        _ => Data::BinaryData(bytes.to_vec()),
    });
    cloudevents::CloudEvent {
        id: event.id().to_string(),
        source: event.source().to_string(),
        spec_version: event.attr("specversion").unwrap_or_default().to_string(),
        r#type: event.ty().to_string(),
        attributes: attributes.into_iter().collect(),
        data,
    }
}

/// Reads a batch in the JSON format, as sent in `json_batch`.
pub fn from_json_batch(body: &[u8]) -> Result<Vec<CloudEvent>, String> {
    json::parse_batch(body).map_err(|err| err.to_string())
}

/// Reads a protobuf batch; the error names the event's index.
pub fn from_proto_batch(batch: &cloudevents::CloudEventBatch) -> Result<Vec<CloudEvent>, String> {
    batch
        .events
        .iter()
        .enumerate()
        .map(|(index, event)| from_proto(event).map_err(|err| format!("event {index}: {err}")))
        .collect()
}
