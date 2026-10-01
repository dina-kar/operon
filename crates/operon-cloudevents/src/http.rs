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
///
/// The HTTP binding gives `Content-Type` precedence: a request whose media
/// type is the structured or the batched CloudEvents type is not binary,
/// whatever `ce-*` headers it also carries.
pub fn is_binary(headers: &HeaderMap) -> bool {
    let structured = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(crate::media_type)
        .is_some_and(|media| {
            media == crate::json::CONTENT_TYPE || media == crate::json::BATCH_CONTENT_TYPE
        });
    !structured && headers.contains_key("ce-specversion")
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
        let value = unquote(value.as_bytes())
            .and_then(|value| percent_decode(&value))
            .map_err(|message| Error::Header {
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
///
/// Fails when an attribute cannot be a header value: a `datacontenttype`
/// with a control character would otherwise be written as a
/// `ce-datacontenttype` header, which the binding forbids.
pub fn write_binary(event: &CloudEvent) -> Result<(HeaderMap, Bytes), Error> {
    let mut headers = HeaderMap::new();
    for attr in event.attrs() {
        // Every type's string form is its binary-mode value.
        let value = attr.value.as_str();
        let bad = |name: String, message: &str| Error::Header {
            name,
            message: message.to_string(),
        };
        if attr.name == "datacontenttype" {
            let value = HeaderValue::from_str(value).map_err(|_| {
                bad(
                    "content-type".into(),
                    "datacontenttype is not a valid header value",
                )
            })?;
            headers.insert(CONTENT_TYPE, value);
            continue;
        }
        let name = format!("{HEADER_PREFIX}{}", attr.name);
        let header_name = HeaderName::try_from(name.as_str())
            .map_err(|_| bad(name.clone(), "not a valid header name"))?;
        let header_value = HeaderValue::from_str(&percent_encode(value))
            .map_err(|_| bad(name, "not a valid header value"))?;
        headers.append(header_name, header_value);
    }
    Ok((headers, event.data().cloned().unwrap_or_default()))
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

/// Unescapes a quoted-string header value (`"1.0"`, with `\"` and `\\`
/// escapes), as the HTTP binding requires before percent-decoding; any other
/// value is returned as it is.
pub fn unquote(value: &[u8]) -> Result<Vec<u8>, String> {
    let Some(inner) = value
        .strip_prefix(b"\"")
        .and_then(|rest| rest.strip_suffix(b"\""))
        .filter(|_| value.len() >= 2)
    else {
        return Ok(value.to_vec());
    };
    let mut out = Vec::with_capacity(inner.len());
    let mut bytes = inner.iter();
    while let Some(&byte) = bytes.next() {
        match byte {
            b'\\' => out.push(*bytes.next().ok_or("a quoted header ends in a backslash")?),
            b'"' => return Err("an unescaped quote inside a quoted header".to_string()),
            _ => out.push(byte),
        }
    }
    Ok(out)
}

/// Decodes `%XX` sequences; the result must be UTF-8.
pub fn percent_decode(value: &[u8]) -> Result<String, String> {
    let mut out = Vec::with_capacity(value.len());
    let mut i = 0;
    while i < value.len() {
        if value[i] == b'%' {
            let hex = value
                .get(i + 1..i + 3)
                // Two hex digits: `from_str_radix` alone takes a leading `+`.
                .filter(|hex| hex.iter().all(u8::is_ascii_hexdigit))
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
