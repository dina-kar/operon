//! Structured and batched mode (the JSON event format).

use loams_cloudevents::json::{BatchError, parse_batch, parse_event, write_batch, write_event};
use loams_cloudevents::{AttrType, DataKind, Error};

/// The JSON format spec's example, compact.
pub const SPEC_EXAMPLE: &str = r#"{"specversion":"1.0","type":"com.github.pull_request.opened","source":"https://github.com/cloudevents/spec/pull","subject":"123","id":"A234-1234-1234","time":"2018-04-05T17:31:00Z","comexampleextension1":"value","comexampleothervalue":5,"datacontenttype":"text/xml","data":"<much wow=\"xml\"/>"}"#;

#[test]
fn the_spec_example_round_trips_byte_for_byte() {
    let event = parse_event(SPEC_EXAMPLE.as_bytes()).unwrap();
    assert_eq!(event.id(), "A234-1234-1234");
    assert_eq!(event.key(), Some("123"));
    assert_eq!(event.data().unwrap().as_ref(), br#"<much wow="xml"/>"#);
    assert_eq!(event.data_kind(), DataKind::Text);
    let other = event
        .attrs()
        .iter()
        .find(|attr| attr.name == "comexampleothervalue")
        .unwrap();
    assert_eq!((other.value.as_str(), other.kind), ("5", AttrType::Integer));
    assert_eq!(
        String::from_utf8(write_event(&event)).unwrap(),
        SPEC_EXAMPLE
    );
}

#[test]
fn json_data_keeps_its_text() {
    let body = r#"{"specversion":"1.0","id":"1","source":"/s","type":"t","flag":true,"data":{"b": [1, 2], "a":null}}"#;
    let event = parse_event(body.as_bytes()).unwrap();
    assert_eq!(
        event.data().unwrap().as_ref(),
        br#"{"b": [1, 2], "a":null}"#
    );
    assert_eq!(String::from_utf8(write_event(&event)).unwrap(), body);
}

#[test]
fn data_base64_round_trips() {
    let body = r#"{"specversion":"1.0","id":"1","source":"/s","type":"t","datacontenttype":"application/octet-stream","data_base64":"AAEC/w=="}"#;
    let event = parse_event(body.as_bytes()).unwrap();
    assert_eq!(event.data().unwrap().as_ref(), &[0, 1, 2, 255]);
    assert_eq!(String::from_utf8(write_event(&event)).unwrap(), body);
}

#[test]
fn string_data_of_a_json_type_stays_json() {
    let body = r#"{"specversion":"1.0","id":"1","source":"/s","type":"t","data":"hello"}"#;
    let event = parse_event(body.as_bytes()).unwrap();
    assert_eq!(event.data().unwrap().as_ref(), br#""hello""#);
    assert_eq!(String::from_utf8(write_event(&event)).unwrap(), body);
}

#[test]
fn null_members_are_absent() {
    let event = parse_event(
        br#"{"specversion":"1.0","id":"1","source":"/s","type":"t","subject":null,"data":null}"#,
    )
    .unwrap();
    assert_eq!(event.attr("subject"), None);
    assert_eq!(event.data(), None);
}

#[test]
fn validation_errors_name_the_attribute() {
    let cases: [(&str, Error); 8] = [
        (
            r#"{"specversion":"1.0","id":"1","type":"t"}"#,
            Error::Missing("source"),
        ),
        (
            r#"{"specversion":"0.3","id":"1","source":"/s","type":"t"}"#,
            Error::SpecVersion("0.3".into()),
        ),
        (
            r#"{"specversion":"1.0","id":"","source":"/s","type":"t"}"#,
            Error::Empty("id".into()),
        ),
        (
            r#"{"specversion":"1.0","id":1,"source":"/s","type":"t"}"#,
            Error::Type {
                name: "id".into(),
                expected: "a string",
            },
        ),
        (
            r#"{"specversion":"1.0","id":"1","source":"/s","type":"t","Ext":"x"}"#,
            Error::Name("Ext".into()),
        ),
        (
            r#"{"specversion":"1.0","id":"1","source":"/s","type":"t","time":"noon"}"#,
            Error::Time("noon".into()),
        ),
        (
            r#"{"specversion":"1.0","id":"1","source":"/s","type":"t","data":1,"data_base64":"AA=="}"#,
            Error::DataAndBase64,
        ),
        (
            r#"{"specversion":"1.0","id":"1","source":"/s","type":"t","n":1.5}"#,
            Error::Type {
                name: "n".into(),
                expected: "a string, a 32-bit integer or a boolean",
            },
        ),
    ];
    for (body, error) in cases {
        assert_eq!(parse_event(body.as_bytes()).unwrap_err(), error, "{body}");
    }
    assert_eq!(
        Error::Missing("source").to_string(),
        "missing required attribute 'source'"
    );
    assert!(matches!(
        parse_event(br#"{"specversion":"1.0","id":"1","id":"2","source":"/s","type":"t"}"#),
        Err(Error::Json(message)) if message.contains("more than once")
    ));
    assert!(matches!(parse_event(b"[1]"), Err(Error::Json(_))));
}

#[test]
fn batches_round_trip_and_name_the_bad_event() {
    let body =
        format!(r#"[{SPEC_EXAMPLE},{{"specversion":"1.0","id":"2","source":"/s","type":"t"}}]"#);
    let events = parse_batch(body.as_bytes()).unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(String::from_utf8(write_batch(&events)).unwrap(), body);
    assert_eq!(parse_batch(b"[]").unwrap(), vec![]);

    let bad = format!(r#"[{SPEC_EXAMPLE},{{"specversion":"1.0","id":"2","source":"/s"}}]"#);
    assert_eq!(
        parse_batch(bad.as_bytes()).unwrap_err(),
        BatchError {
            index: 1,
            error: Error::Missing("type")
        }
    );
    assert_eq!(
        parse_batch(bad.as_bytes()).unwrap_err().to_string(),
        "event 1: missing required attribute 'type'"
    );
    assert!(parse_batch(SPEC_EXAMPLE.as_bytes()).is_err());
}

#[test]
fn an_integer_extension_must_be_canonical() {
    use loams_cloudevents::{Attr, AttrType, CloudEvent};
    for bad in ["+5", "007", "-0", " 5"] {
        let attrs = vec![
            Attr::string("specversion", "1.0"),
            Attr::string("id", "1"),
            Attr::string("source", "/s"),
            Attr::string("type", "t"),
            Attr {
                name: "n".into(),
                value: bad.into(),
                kind: AttrType::Integer,
            },
        ];
        assert!(CloudEvent::new(attrs, None).is_err(), "{bad}");
    }
}
