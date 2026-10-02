//! The record layout (the Kafka binding's binary mode) and reading records
//! back as events.

use bytes::Bytes;
use http::{HeaderMap, HeaderValue};
use loams_cloudevents::json::{parse_event, write_event};
use loams_cloudevents::record::{RecordContext, TYPES_HEADER, from_record, to_record};
use loams_cloudevents::{AttrType, http as binary};
use loams_log::Record;

use crate::structured::SPEC_EXAMPLE;

const AT: RecordContext<'static> = RecordContext {
    namespace: "acme",
    stream: "events",
    stream_id: 7,
    partition: 2,
    offset: 41,
};

fn header<'r>(record: &'r Record, name: &str) -> Option<&'r [u8]> {
    record
        .headers
        .iter()
        .find(|(key, _)| key == name)
        .and_then(|(_, value)| value.as_deref())
}

#[test]
fn an_event_is_stored_in_the_kafka_binding_layout() {
    let event = parse_event(SPEC_EXAMPLE.as_bytes()).unwrap();
    let record = to_record(&event);
    let names: Vec<&str> = record
        .headers
        .iter()
        .map(|(name, _)| name.as_str())
        .collect();
    assert_eq!(
        names,
        [
            "ce_specversion",
            "ce_type",
            "ce_source",
            "ce_subject",
            "ce_id",
            "ce_time",
            "ce_comexampleextension1",
            "ce_comexampleothervalue",
            "content-type",
            TYPES_HEADER,
        ]
    );
    assert_eq!(header(&record, "content-type"), Some(&b"text/xml"[..]));
    assert_eq!(
        header(&record, TYPES_HEADER),
        Some(&b"comexampleothervalue=integer"[..])
    );
    assert_eq!(record.key.as_deref(), Some(&b"123"[..]));
    assert_eq!(record.timestamp_ms, 1_522_949_460_000);
    assert_eq!(record.value.as_deref(), Some(&br#"<much wow="xml"/>"#[..]));
}

#[test]
fn structured_events_round_trip_through_a_record() {
    let bodies = [
        SPEC_EXAMPLE.to_string(),
        r#"{"specversion":"1.0","id":"1","source":"/s","type":"t","partitionkey":"k","on":false,"data":{"x": 1}}"#.to_string(),
        r#"{"specversion":"1.0","id":"1","source":"/s","type":"t"}"#.to_string(),
        r#"{"specversion":"1.0","id":"1","source":"/s","type":"t","datacontenttype":"image/png","data_base64":"iVBORw=="}"#.to_string(),
    ];
    for body in bodies {
        let event = parse_event(body.as_bytes()).unwrap();
        let mut record = to_record(&event);
        // The writer assigns a clock to events without `time`.
        if record.timestamp_ms < 0 {
            record.timestamp_ms = 5;
        }
        let back = from_record(&record, AT);
        assert_eq!(back, event);
        assert_eq!(String::from_utf8(write_event(&back)).unwrap(), body);
    }
}

#[test]
fn binary_events_round_trip_through_a_record() {
    let mut map = HeaderMap::new();
    for (name, value) in [
        ("ce-specversion", "1.0"),
        ("ce-id", "abc"),
        ("ce-source", "urn:x"),
        ("ce-type", "t"),
        (
            "ce-traceparent",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        ),
        ("content-type", "text/plain"),
    ] {
        map.append(name, HeaderValue::from_static(value));
    }
    let event = binary::parse_binary(&map, Bytes::from_static(b"hi")).unwrap();
    let back = from_record(&to_record(&event), AT);
    assert_eq!(back, event);
    let (headers, body) = binary::write_binary(&back).unwrap();
    assert_eq!(headers, map);
    assert_eq!(body.as_ref(), b"hi");
}

#[test]
fn kafka_produced_ce_headers_are_read_as_events() {
    // What a Kafka producer using the CloudEvents Kafka binding sends; some
    // producers put `datacontenttype` in `ce_datacontenttype`.
    let record = Record {
        key: Some(Bytes::from_static(b"order-9")),
        value: Some(Bytes::from_static(b"{\"total\":3}")),
        headers: vec![
            ("ce_specversion".into(), Some(Bytes::from_static(b"1.0"))),
            ("ce_id".into(), Some(Bytes::from_static(b"k-1"))),
            ("ce_source".into(), Some(Bytes::from_static(b"/orders"))),
            ("ce_type".into(), Some(Bytes::from_static(b"order.created"))),
            (
                "ce_datacontenttype".into(),
                Some(Bytes::from_static(b"application/json")),
            ),
            ("other".into(), Some(Bytes::from_static(b"ignored"))),
        ],
        timestamp_ms: 1,
    };
    let event = from_record(&record, AT);
    assert_eq!(event.id(), "k-1");
    assert_eq!(event.datacontenttype(), Some("application/json"));
    assert_eq!(event.attr("other"), None);
    assert_eq!(
        String::from_utf8(write_event(&event)).unwrap(),
        r#"{"specversion":"1.0","id":"k-1","source":"/orders","type":"order.created","datacontenttype":"application/json","data":{"total":3}}"#
    );
}

#[test]
fn kafka_structured_records_pass_through() {
    let record = Record {
        key: None,
        value: Some(Bytes::from_static(SPEC_EXAMPLE.as_bytes())),
        headers: vec![(
            "content-type".into(),
            Some(Bytes::from_static(
                b"application/cloudevents+json; charset=UTF-8",
            )),
        )],
        timestamp_ms: 1,
    };
    let event = from_record(&record, AT);
    assert_eq!(
        String::from_utf8(write_event(&event)).unwrap(),
        SPEC_EXAMPLE
    );
}

#[test]
fn plain_records_get_a_synthesized_envelope() {
    let json = Record {
        key: Some(Bytes::from_static(b"user-1")),
        value: Some(Bytes::from_static(b"{\"n\":1}")),
        headers: vec![],
        timestamp_ms: 1_790_762_400_250,
    };
    assert_eq!(
        String::from_utf8(write_event(&from_record(&json, AT))).unwrap(),
        r#"{"specversion":"1.0","id":"7-41","source":"/namespaces/acme/streams/events/partitions/2","type":"io.loams.dev.stream.record","datacontenttype":"application/json","time":"2026-09-30T10:00:00.250Z","partitionkey":"user-1","data":{"n":1}}"#
    );
    let bytes = Record {
        key: Some(Bytes::from_static(&[0xff, 0x00])),
        value: Some(Bytes::from_static(&[1, 2, 3])),
        headers: vec![],
        timestamp_ms: 0,
    };
    let event = from_record(&bytes, AT);
    assert_eq!(event.attr("partitionkey"), None);
    assert_eq!(event.datacontenttype(), Some("application/octet-stream"));
    assert!(
        String::from_utf8(write_event(&event))
            .unwrap()
            .ends_with(r#""data_base64":"AQID"}"#)
    );
    let typed = Record {
        key: None,
        value: Some(Bytes::from_static(b"hello")),
        headers: vec![(
            "content-type".into(),
            Some(Bytes::from_static(b"text/plain")),
        )],
        timestamp_ms: 0,
    };
    assert!(
        String::from_utf8(write_event(&from_record(&typed, AT)))
            .unwrap()
            .ends_with(r#""data":"hello"}"#)
    );
}

#[test]
fn invalid_ce_headers_fall_back_to_an_envelope() {
    let record = Record {
        key: None,
        value: None,
        headers: vec![("ce_specversion".into(), Some(Bytes::from_static(b"0.3")))],
        timestamp_ms: 0,
    };
    let event = from_record(&record, AT);
    assert_eq!(event.ty(), "io.loams.dev.stream.record");
    assert_eq!(event.id(), "7-41");
}

#[test]
fn typed_extensions_keep_their_types() {
    let event =
        parse_event(br#"{"specversion":"1.0","id":"1","source":"/s","type":"t","n":-3,"ok":true}"#)
            .unwrap();
    let back = from_record(&to_record(&event), AT);
    let kinds: Vec<AttrType> = back.attrs().iter().map(|a| a.kind).collect();
    assert_eq!(kinds[4..], [AttrType::Integer, AttrType::Boolean]);
    // Binary mode carries strings only and leaves the types header out.
    let (headers, _) = binary::write_binary(&back).unwrap();
    assert_eq!(headers["ce-n"], "-3");
    assert!(!headers.contains_key("loams_ce_types"));
}
