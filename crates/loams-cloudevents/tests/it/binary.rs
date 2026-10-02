//! The HTTP binding's binary mode.

use bytes::Bytes;
use http::{HeaderMap, HeaderValue};
use loams_cloudevents::Error;
use loams_cloudevents::http::{
    is_binary, parse_binary, percent_decode, percent_encode, unquote, write_binary,
};

fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
    let mut map = HeaderMap::new();
    for (name, value) in pairs {
        map.append(*name, HeaderValue::from_str(value).expect("header value"));
    }
    map
}

#[test]
fn parses_the_http_binding_example() {
    let map = headers(&[
        ("ce-specversion", "1.0"),
        ("ce-type", "com.example.someevent"),
        ("ce-time", "2018-04-05T03:56:24Z"),
        ("ce-id", "1234-1234-1234"),
        ("ce-source", "/mycontext/subcontext"),
        ("ce-comexampleextension", "caf%C3%A9%20%22x%22"),
        ("content-type", "application/json; charset=utf-8"),
        ("content-length", "7"),
    ]);
    assert!(is_binary(&map));
    let event = parse_binary(&map, Bytes::from_static(b"{\"a\":1}")).unwrap();
    let names: Vec<&str> = event.attrs().iter().map(|a| a.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "specversion",
            "type",
            "time",
            "id",
            "source",
            "comexampleextension",
            "datacontenttype"
        ]
    );
    assert_eq!(event.attr("comexampleextension"), Some("café \"x\""));
    assert_eq!(event.time_ms(), Some(1_522_900_584_000));

    let (out, body) = write_binary(&event).unwrap();
    assert_eq!(body.as_ref(), b"{\"a\":1}");
    assert_eq!(out["content-type"], "application/json; charset=utf-8");
    assert_eq!(out["ce-comexampleextension"], "caf%C3%A9%20%22x%22");
    assert_eq!(parse_binary(&out, body).unwrap(), event);
}

#[test]
fn an_empty_body_is_no_data() {
    let map = headers(&[
        ("ce-specversion", "1.0"),
        ("ce-id", "1"),
        ("ce-source", "/s"),
        ("ce-type", "t"),
    ]);
    assert_eq!(parse_binary(&map, Bytes::new()).unwrap().data(), None);
}

#[test]
fn refuses_bad_binary_events() {
    let missing = headers(&[
        ("ce-specversion", "1.0"),
        ("ce-id", "1"),
        ("ce-source", "/s"),
    ]);
    assert_eq!(
        parse_binary(&missing, Bytes::new()).unwrap_err(),
        Error::Missing("type")
    );
    let twice = headers(&[
        ("ce-specversion", "1.0"),
        ("ce-id", "1"),
        ("ce-id", "2"),
        ("ce-source", "/s"),
        ("ce-type", "t"),
    ]);
    assert_eq!(
        parse_binary(&twice, Bytes::new()).unwrap_err(),
        Error::Duplicate("id".into())
    );
    let bad = headers(&[
        ("ce-specversion", "1.0"),
        ("ce-id", "%zz"),
        ("ce-source", "/s"),
        ("ce-type", "t"),
    ]);
    assert!(matches!(
        parse_binary(&bad, Bytes::new()),
        Err(Error::Header { name, .. }) if name == "ce-id"
    ));
    assert!(!is_binary(&headers(&[(
        "content-type",
        "application/json"
    )])));
}

#[test]
fn percent_encoding_round_trips() {
    for value in [
        "plain",
        "with space",
        "100%",
        "quote\"d",
        "ünïcödé",
        "tab\tnewline\n",
    ] {
        let encoded = percent_encode(value);
        assert!(
            encoded.bytes().all(|b| (0x21..=0x7e).contains(&b)),
            "{encoded}"
        );
        assert_eq!(percent_decode(encoded.as_bytes()).unwrap(), value);
    }
    assert!(percent_decode(b"%ff").is_err());
}

#[test]
fn content_type_takes_precedence_over_ce_headers() {
    for media in [
        "application/cloudevents+json",
        "application/cloudevents-batch+json; charset=utf-8",
    ] {
        assert!(!is_binary(&headers(&[
            ("ce-specversion", "1.0"),
            ("content-type", media),
        ])));
    }
    assert!(is_binary(&headers(&[
        ("ce-specversion", "1.0"),
        ("content-type", "application/json"),
    ])));
}

#[test]
fn quoted_header_values_are_unescaped_before_percent_decoding() {
    let map = headers(&[
        ("ce-specversion", "\"1.0\""),
        ("ce-id", "\"a\\\"b%20c\""),
        ("ce-source", "/s"),
        ("ce-type", "t"),
    ]);
    let event = parse_binary(&map, Bytes::new()).unwrap();
    assert_eq!(event.attr("specversion"), Some("1.0"));
    assert_eq!(event.attr("id"), Some("a\"b c"));
    assert_eq!(unquote(b"plain").unwrap(), b"plain");
    assert_eq!(unquote(b"\"").unwrap(), b"\"");
    assert!(unquote(b"\"a\"b\"").is_err());
    assert!(unquote(b"\"a\\\"").is_err());
}

#[test]
fn a_percent_escape_needs_two_hex_digits() {
    for bad in ["%+A", "%+0", "%-1", "%4", "%"] {
        assert!(percent_decode(bad.as_bytes()).is_err(), "{bad}");
    }
    assert_eq!(percent_decode(b"%4a%4A").unwrap(), "JJ");
}

#[test]
fn a_datacontenttype_that_is_not_a_header_value_is_an_error() {
    use loams_cloudevents::{Attr, CloudEvent};
    let event = CloudEvent::new(
        vec![
            Attr::string("specversion", "1.0"),
            Attr::string("id", "1"),
            Attr::string("source", "/s"),
            Attr::string("type", "t"),
            Attr::string("datacontenttype", "text/plain\nx"),
        ],
        None,
    )
    .unwrap();
    assert!(
        matches!(write_binary(&event), Err(Error::Header { name, .. }) if name == "content-type")
    );
}
