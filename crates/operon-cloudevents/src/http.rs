//! The HTTP binding's binary mode: attributes in `ce-*` headers
//! (percent-encoded), `datacontenttype` in `Content-Type`, the data as the
//! body.

use bytes::Bytes;
use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderName, HeaderValue};

use crate::{Attr, CloudEvent, Error};

/// The prefix of attribute headers.
pub const HEADER_PREFIX: &str = "ce-";

/// Whether a request is a binary-mode event: it has a `ce-specversion`
/// header.
pub fn is_binary(headers: &HeaderMap) -> bool {
    headers.contains_key("ce-specversion")
}

/// Reads a binary-mode event. Attributes keep the headers' order; an empty
/// body is an event without data.
pub fn parse_binary(headers: &HeaderMap, body: Bytes) -> Result<CloudEvent, Error> {
    let mut attrs = Vec::new();
    for name in headers.keys() {
        let Some(attr) = name.as_str().strip_prefix(HEADER_PREFIX) else {
            continue;
        };
        let mut values = headers.get_all(name).iter();
        let (Some(value), None) = (values.next(), values.next()) else {
            return Err(Error::Duplicate(attr.to_string()));
        };
        let value = percent_decode(value.as_bytes()).map_err(|message| Error::Header {
            name: name.to_string(),
            message,
        })?;
        attrs.push(Attr::string(attr, value));
    }
    if let Some(content_type) = headers.get(CONTENT_TYPE) {
        let value = content_type.to_str().map_err(|_| Error::Header {
            name: CONTENT_TYPE.to_string(),
            message: "not visible ASCII".into(),
        })?;
        if attrs.iter().any(|attr| attr.name == "datacontenttype") {
            return Err(Error::Duplicate("datacontenttype".into()));
        }
        attrs.push(Attr::string("datacontenttype", value));
    }
    let data = (!body.is_empty()).then_some(body);
    CloudEvent::new(attrs, data)
}

/// An event as binary-mode headers (`Content-Type` included) and a body.
pub fn write_binary(event: &CloudEvent) -> (HeaderMap, Bytes) {
    let mut headers = HeaderMap::new();
    for attr in event.attrs() {
        // Every type's string form is its binary-mode value.
        let value = attr.value.as_str();
        if attr.name == "datacontenttype"
            && let Ok(value) = HeaderValue::from_str(value)
        {
            headers.insert(CONTENT_TYPE, value);
            continue;
        }
        let name = HeaderName::try_from(format!("{HEADER_PREFIX}{}", attr.name));
        let value = HeaderValue::from_str(&percent_encode(value));
        if let (Ok(name), Ok(value)) = (name, value) {
            headers.append(name, value);
        }
    }
    (headers, event.data().cloned().unwrap_or_default())
}

/// Percent-encodes what the HTTP binding requires: space, `"`, `%` and
/// every byte outside visible ASCII.
pub fn percent_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if (0x21..=0x7e).contains(&byte) && byte != b'"' && byte != b'%' {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// Decodes `%XX` sequences; the result must be UTF-8.
pub fn percent_decode(value: &[u8]) -> Result<String, String> {
    let mut out = Vec::with_capacity(value.len());
    let mut i = 0;
    while i < value.len() {
        if value[i] == b'%' {
            let hex = value
                .get(i + 1..i + 3)
                .and_then(|hex| std::str::from_utf8(hex).ok())
                .and_then(|hex| u8::from_str_radix(hex, 16).ok())
                .ok_or_else(|| format!("bad percent-encoding at byte {i}"))?;
            out.push(hex);
            i += 3;
        } else {
            out.push(value[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| "the decoded value is not UTF-8".to_string())
}
