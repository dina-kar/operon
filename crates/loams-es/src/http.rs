//! The HTTP plumbing every endpoint shares (Task 1 rules 3–7): media-type
//! negotiation, the `X-Elastic-Product` header, `pretty`, the body limit,
//! query-parameter validation, and the namespace and consistency of a
//! request.

use std::collections::BTreeSet;
use std::time::Instant;

use axum::body::Body;
use axum::extract::{FromRequestParts, Request};
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use loams_collection::ConsistencyToken;
use loams_query::ReadConsistency;
use loams_query::hot::{HOT_HEADER, parse_hot_header};
use serde_json::Value;

use crate::error::EsError;
use crate::{EsGateway, NAMESPACE_HEADER, TOKEN_HEADER};

/// The header every response carries (Global Constraints).
pub const PRODUCT_HEADER: &str = "x-elastic-product";
/// Its value, which elasticsearch-py and elastic-transport-js check.
pub const PRODUCT: &str = "Elasticsearch";
/// The `Content-Type` of a [`ResponseFormat::CompatJson`] answer.
pub const COMPAT_JSON: &str = "application/vnd.elasticsearch+json;compatible-with=8";

/// How a response body is written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResponseFormat {
    /// `application/json`.
    Json,
    /// [`COMPAT_JSON`]: the request's `Accept` asked for compat 8.
    CompatJson,
}

/// What every handler knows about its request.
#[derive(Clone, Debug)]
pub struct RequestCtx {
    /// `Loams-Namespace` if present and non-empty, else the configured
    /// namespace (rule 5).
    pub namespace: String,
    pub format: ResponseFormat,
    /// `pretty` given without a value or as `true`.
    pub pretty: bool,
    /// `AtLeast(token)` with a `Loams-Consistency-Token`, else `Strong`.
    pub consistency: ReadConsistency,
    pub started: Instant,
}

impl RequestCtx {
    /// A context from the request's headers and query string.
    pub fn from_parts(
        headers: &HeaderMap,
        query: Option<&str>,
        format: ResponseFormat,
        default_namespace: &str,
    ) -> Result<Self, EsError> {
        let started = Instant::now();
        let text = |name: &str, value: &HeaderValue| {
            value
                .to_str()
                .map(str::to_string)
                .map_err(|_| EsError::illegal_argument(format!("{name} is not valid ASCII")))
        };
        let namespace = match headers.get(NAMESPACE_HEADER) {
            Some(value) => text("Loams-Namespace", value)?,
            None => String::new(),
        };
        let namespace = if namespace.is_empty() {
            default_namespace.to_string()
        } else {
            namespace
        };
        let consistency = match headers.get(TOKEN_HEADER) {
            None => ReadConsistency::Strong,
            Some(value) => {
                let value = text("Loams-Consistency-Token", value)?;
                let token = value.parse::<ConsistencyToken>().map_err(|err| {
                    EsError::illegal_argument(format!("invalid Loams-Consistency-Token: {err}"))
                })?;
                ReadConsistency::AtLeast(token)
            }
        };
        Ok(Self {
            namespace,
            format,
            pretty: wants_pretty(query),
            consistency,
            started,
        })
    }

    /// A context for answers given before the request was read: JSON, not
    /// pretty.
    pub fn fallback() -> Self {
        Self {
            namespace: String::new(),
            format: ResponseFormat::Json,
            pretty: false,
            consistency: ReadConsistency::Strong,
            started: Instant::now(),
        }
    }
}

impl FromRequestParts<EsGateway> for RequestCtx {
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, gw: &EsGateway) -> Result<Self, Response> {
        let format = parts
            .extensions
            .get::<ResponseFormat>()
            .copied()
            .unwrap_or(ResponseFormat::Json);
        RequestCtx::from_parts(
            &parts.headers,
            parts.uri.query(),
            format,
            &gw.config().namespace,
        )
        .map_err(|err| {
            let mut ctx = RequestCtx::fallback();
            ctx.format = format;
            ctx.pretty = wants_pretty(parts.uri.query());
            fail(&ctx, &err)
        })
    }
}

/// Whether the query string holds `pretty` without a value or as `true`.
fn wants_pretty(query: Option<&str>) -> bool {
    query.is_some_and(|query| {
        pairs(query).any(|(name, value)| name == "pretty" && (value.is_empty() || value == "true"))
    })
}

// ----- negotiation -----

/// The Elasticsearch media types a `compatible-with` parameter may follow.
const COMPAT_TYPES: [&str; 3] = [
    "application/vnd.elasticsearch+json",
    "application/vnd.elasticsearch+x-ndjson",
    "application/vnd.elasticsearch+yaml",
];

/// The body media types a request may send (rule 4).
const BODY_TYPES: [&str; 4] = [
    "application/json",
    "application/x-ndjson",
    "application/vnd.elasticsearch+json",
    "application/vnd.elasticsearch+x-ndjson",
];

/// The `compatible-with` version a header value asks for, if any of its
/// media types is an Elasticsearch type with the parameter.
fn compat_version(value: &str) -> Option<String> {
    value.split(',').find_map(|media| {
        let mut parts = media.split(';');
        let essence = parts.next()?.trim().to_ascii_lowercase();
        if !COMPAT_TYPES.contains(&essence.as_str()) {
            return None;
        }
        parts.find_map(|param| {
            let (name, value) = param.split_once('=')?;
            name.trim()
                .eq_ignore_ascii_case("compatible-with")
                .then(|| value.trim().trim_matches('"').to_string())
        })
    })
}

/// The response format of a request (rule 4): `CompatJson` iff `Accept`
/// asks for compat 8; 400 `media_type_header_exception` for any other
/// version, or when `Accept` and `Content-Type` ask for different ones.
pub fn negotiate(headers: &HeaderMap) -> Result<ResponseFormat, EsError> {
    let get = |name: header::HeaderName| {
        headers
            .get(name)
            .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
    };
    let accept = get(header::ACCEPT);
    let content_type = get(header::CONTENT_TYPE);
    let accept_v = accept.as_deref().and_then(compat_version);
    let content_v = content_type.as_deref().and_then(compat_version);
    let bad = accept_v.as_deref().is_some_and(|v| v != "8")
        || content_v.as_deref().is_some_and(|v| v != "8")
        || matches!((&accept_v, &content_v), (Some(a), Some(c)) if a != c);
    if bad {
        return Err(EsError::new(
            400,
            "media_type_header_exception",
            format!(
                "Loams's Elasticsearch API supports compatible-with=8; got Accept={}, Content-Type={}",
                accept.as_deref().unwrap_or(""),
                content_type.as_deref().unwrap_or("")
            ),
        ));
    }
    Ok(if accept_v.is_some() {
        ResponseFormat::CompatJson
    } else {
        ResponseFormat::Json
    })
}

/// 406 in ES's string shape unless a request with a body declares JSON,
/// NDJSON or one of their compat types (rule 4).
pub fn check_body_type(headers: &HeaderMap) -> Result<(), EsError> {
    let Some(value) = headers.get(header::CONTENT_TYPE) else {
        return Err(EsError::plain(406, "Content-Type header is missing"));
    };
    let value = String::from_utf8_lossy(value.as_bytes());
    let essence = value
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    if BODY_TYPES.contains(&essence.as_str()) {
        Ok(())
    } else {
        Err(EsError::plain(
            406,
            format!("Content-Type header [{value}] is not supported"),
        ))
    }
}

// ----- parameters -----

/// Accepted on every endpoint.
pub const UNIVERSAL_PARAMS: &[&str] = &["pretty", "human", "error_trace"];

/// A request's query parameters, percent-decoded; a repeated name keeps its
/// last value.
#[derive(Clone, Debug, Default)]
pub struct Params {
    pairs: Vec<(String, String)>,
}

impl Params {
    /// Parses `raw`; any name outside `allowed` and [`UNIVERSAL_PARAMS`]
    /// is 400 `illegal_argument_exception` naming it (rule 6).
    pub fn parse(raw: Option<&str>, path: &str, allowed: &[&str]) -> Result<Params, EsError> {
        let pairs: Vec<(String, String)> = raw.map(|raw| pairs(raw).collect()).unwrap_or_default();
        let unknown: BTreeSet<&str> = pairs
            .iter()
            .map(|(name, _)| name.as_str())
            .filter(|name| !allowed.contains(name) && !UNIVERSAL_PARAMS.contains(name))
            .collect();
        match unknown.len() {
            0 => Ok(Params { pairs }),
            1 => Err(EsError::illegal_argument(format!(
                "request [{path}] contains unrecognized parameter: [{}]",
                unknown.first().copied().unwrap_or_default()
            ))),
            _ => Err(EsError::illegal_argument(format!(
                "request [{path}] contains unrecognized parameters: {}",
                unknown
                    .iter()
                    .map(|name| format!("[{name}]"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ))),
        }
    }

    /// The last value of `name`.
    pub fn str(&self, name: &str) -> Option<&str> {
        self.pairs
            .iter()
            .rev()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    /// `""` and `true` are true, `false` false; anything else is 400.
    pub fn bool(&self, name: &str) -> Result<Option<bool>, EsError> {
        match self.str(name) {
            None => Ok(None),
            Some("" | "true") => Ok(Some(true)),
            Some("false") => Ok(Some(false)),
            Some(other) => Err(EsError::illegal_argument(format!(
                "Failed to parse value [{other}] as only [true] or [false] are allowed."
            ))),
        }
    }

    /// A non-negative integer; anything else is 400.
    pub fn usize(&self, name: &str) -> Result<Option<usize>, EsError> {
        self.str(name)
            .map(|value| {
                value.parse::<usize>().map_err(|_| {
                    EsError::illegal_argument(format!(
                        "Failed to parse int parameter [{name}] with value [{value}]"
                    ))
                })
            })
            .transpose()
    }

    /// The comma-separated elements of `name`, empty ones left out.
    pub fn list(&self, name: &str) -> Option<Vec<String>> {
        self.str(name).map(|value| {
            value
                .split(',')
                .filter(|item| !item.is_empty())
                .map(str::to_string)
                .collect()
        })
    }
}

/// The decoded `(name, value)` pairs of a query string.
fn pairs(raw: &str) -> impl Iterator<Item = (String, String)> + '_ {
    raw.split('&').filter(|pair| !pair.is_empty()).map(|pair| {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        (percent_decode(name), percent_decode(value))
    })
}

/// Percent-decoding of a path segment (`+` stays a plus).
pub(crate) fn percent_decode_path(segment: &str) -> String {
    percent_decode(&segment.replace('+', "%2B"))
}

/// `application/x-www-form-urlencoded` decoding: `+` is a space, `%XX` a
/// byte; invalid escapes stay as written, invalid UTF-8 is replaced.
fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3])
                    .ok()
                    .and_then(|h| u8::from_str_radix(h, 16).ok());
                match hex {
                    Some(byte) => {
                        out.push(byte);
                        i += 2;
                    }
                    None => out.push(b'%'),
                }
            }
            byte => out.push(byte),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

// ----- bodies -----

/// A JSON request body; `None` when it is empty or only whitespace, 400
/// `x_content_parse_exception` when it is not JSON.
pub fn json_body(bytes: &[u8]) -> Result<Option<Value>, EsError> {
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(None);
    }
    serde_json::from_slice(bytes).map(Some).map_err(|err| {
        EsError::new(
            400,
            "x_content_parse_exception",
            format!(
                "[{}:{}] Failed to parse the request body: {err}",
                err.line(),
                err.column()
            ),
        )
    })
}

// ----- responses -----

/// `body` with `status`, written per the context's format and `pretty`.
pub fn respond(ctx: &RequestCtx, status: u16, body: &Value) -> Response {
    let text = if ctx.pretty {
        let mut text = serde_json::to_string_pretty(body).unwrap_or_default();
        text.push('\n');
        text
    } else {
        body.to_string()
    };
    let content_type = match ctx.format {
        ResponseFormat::Json => "application/json",
        ResponseFormat::CompatJson => COMPAT_JSON,
    };
    let status = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut response = (status, text).into_response();
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
}

/// An empty answer with `status` (every `HEAD` answer, rule 3).
pub fn respond_head(status: u16) -> Response {
    let status = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut response = (status, Body::empty()).into_response();
    response
        .headers_mut()
        .insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
    response
}

/// The error envelope, with `Retry-After` for write backpressure.
pub fn fail(ctx: &RequestCtx, error: &EsError) -> Response {
    let mut response = respond(ctx, error.status, &error.to_body());
    if let Some(secs) = error.retry_after_secs {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from(secs));
    }
    response
}

impl IntoResponse for EsError {
    /// [`fail`] with the fallback context.
    fn into_response(self) -> Response {
        fail(&RequestCtx::fallback(), &self)
    }
}

// ----- middleware -----

/// The outermost layer: every answer carries `X-Elastic-Product`, and every
/// `HEAD` answer has an empty body with `content-length: 0` (rule 3).
pub(crate) async fn product_header(request: Request, next: Next) -> Response {
    let head = request.method() == Method::HEAD;
    let mut response = next.run(request).await;
    if head {
        let (mut parts, _) = response.into_parts();
        parts
            .headers
            .insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
        response = Response::from_parts(parts, Body::empty());
    }
    response
        .headers_mut()
        .insert(PRODUCT_HEADER, HeaderValue::from_static(PRODUCT));
    response
}

/// Answers an invalid `Loams-Hot` with the ES envelope, before `HotLayer`
/// sees it (rule 2).
pub(crate) async fn check_hot_header(request: Request, next: Next) -> Response {
    if let Some(value) = request.headers().get(HOT_HEADER) {
        let value = String::from_utf8_lossy(value.as_bytes()).into_owned();
        if parse_hot_header(&value).is_err() {
            let error = EsError::illegal_argument(format!(
                "invalid Loams-Hot header [{value}] (expected on or off)"
            ));
            return fail(&pretty_fallback(&request), &error);
        }
    }
    next.run(request).await
}

/// Negotiates the format, reads the body up to the limit (413 over it)
/// and checks a non-empty body's `Content-Type` (406), then hands the
/// buffered request on with its [`ResponseFormat`] as an extension.
pub(crate) async fn prepare(
    axum::extract::State(limit): axum::extract::State<usize>,
    request: Request,
    next: Next,
) -> Response {
    let ctx = pretty_fallback(&request);
    let format = match negotiate(request.headers()) {
        Ok(format) => format,
        Err(err) => return fail(&ctx, &err),
    };
    let ctx = RequestCtx { format, ..ctx };
    let too_long = |n: String| {
        EsError::new(
            413,
            "content_too_long_exception",
            format!("entity content is too long [{n}] for the configured buffer limit [{limit}]"),
        )
    };
    let declared = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    if let Some(len) = declared.filter(|len| *len > limit as u64) {
        return fail(&ctx, &too_long(len.to_string()));
    }
    let (mut parts, body) = request.into_parts();
    let bytes = match axum::body::to_bytes(body, limit).await {
        Ok(bytes) => bytes,
        Err(_) => return fail(&ctx, &too_long(format!("> {limit}"))),
    };
    if !bytes.is_empty()
        && let Err(err) = check_body_type(&parts.headers)
    {
        return fail(&ctx, &err);
    }
    parts.extensions.insert(format);
    next.run(Request::from_parts(parts, Body::from(bytes)))
        .await
}

/// The fallback context, with the request's `pretty`.
fn pretty_fallback(request: &Request) -> RequestCtx {
    let mut ctx = RequestCtx::fallback();
    ctx.pretty = wants_pretty(request.uri().query());
    ctx
}

/// Rewrites axum's 405 into ES's string-shaped answer, listing the path's
/// methods from the `Allow` header axum sets (rule 7).
/// The path part of `uri`.
fn request_path(uri: &str) -> &str {
    uri.split('?').next().unwrap_or(uri)
}

/// ES's list of the methods a path allows, from axum's `Allow` header: in
/// ES's order (GET, POST, PUT, DELETE, HEAD), with `HEAD` only on the paths
/// ES registers it for (`/`, an index, a document, a source and the alias
/// routes); axum answers `HEAD` on every `GET` route.
fn es_allowed(allow: &str, path: &str) -> String {
    let given: Vec<&str> = allow.split(',').map(str::trim).collect();
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let head_route = match segments.as_slice() {
        [] => true,
        [index] => !index.starts_with('_'),
        [_, "_doc" | "_source", _] => true,
        _ => segments.contains(&"_alias"),
    };
    ["GET", "POST", "PUT", "DELETE", "HEAD"]
        .into_iter()
        .filter(|m| given.contains(m) && (*m != "HEAD" || head_route))
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) async fn method_not_allowed(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let uri = request.uri().to_string();
    let ctx = pretty_fallback(&request);
    let response = next.run(request).await;
    if response.status() != StatusCode::METHOD_NOT_ALLOWED {
        return response;
    }
    let allowed = response
        .headers()
        .get(header::ALLOW)
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
        .unwrap_or_default();
    let allowed = es_allowed(&allowed, request_path(&uri));
    let error = EsError::plain(
        405,
        format!(
            "Incorrect HTTP method for uri [{uri}] and method [{method}], allowed: [{allowed}]"
        ),
    );
    let mut answer = fail(&ctx, &error);
    if let Some(allow) = response.headers().get(header::ALLOW) {
        answer.headers_mut().insert(header::ALLOW, allow.clone());
    }
    answer
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(*name, HeaderValue::from_str(value).expect("header"));
        }
        map
    }

    #[test]
    fn negotiation_picks_compat_json_from_accept() {
        let compat = "application/vnd.elasticsearch+json; compatible-with=8";
        assert_eq!(negotiate(&headers(&[])), Ok(ResponseFormat::Json));
        assert_eq!(
            negotiate(&headers(&[("accept", "*/*")])),
            Ok(ResponseFormat::Json)
        );
        assert_eq!(
            negotiate(&headers(&[("accept", compat), ("content-type", compat)])),
            Ok(ResponseFormat::CompatJson)
        );
        assert_eq!(
            negotiate(&headers(&[("content-type", compat)])),
            Ok(ResponseFormat::Json)
        );
        let e = negotiate(&headers(&[(
            "accept",
            "application/vnd.elasticsearch+json; compatible-with=9",
        )]))
        .expect_err("9");
        assert_eq!(
            (e.status, e.kind.as_str()),
            (400, "media_type_header_exception")
        );
        let e = negotiate(&headers(&[
            ("accept", compat),
            (
                "content-type",
                "application/vnd.elasticsearch+x-ndjson;compatible-with=7",
            ),
        ]))
        .expect_err("7");
        assert!(
            e.reason
                .starts_with("Loams's Elasticsearch API supports compatible-with=8; got Accept=")
        );
    }

    #[test]
    fn a_body_must_be_json_or_ndjson() {
        assert!(
            check_body_type(&headers(&[(
                "content-type",
                "application/json; charset=UTF-8"
            )]))
            .is_ok()
        );
        assert!(check_body_type(&headers(&[("content-type", "application/x-ndjson")])).is_ok());
        let e = check_body_type(&headers(&[("content-type", "text/plain")])).expect_err("text");
        assert_eq!(
            e.to_body(),
            serde_json::json!({"error": "Content-Type header [text/plain] is not supported", "status": 406})
        );
    }

    #[test]
    fn params_decode_and_refuse_unknown_names() {
        let p = Params::parse(
            Some("q=a%20b+c&size=10&x&pretty"),
            "/i/_search",
            &["q", "size", "x"],
        )
        .expect("parse");
        assert_eq!(p.str("q"), Some("a b c"));
        assert_eq!(p.usize("size"), Ok(Some(10)));
        assert_eq!(p.bool("x"), Ok(Some(true)));
        assert_eq!(p.bool("missing"), Ok(None));
        assert!(p.usize("q").is_err());
        assert!(p.bool("q").is_err());
        let e = Params::parse(Some("foo=1"), "/", &[]).expect_err("foo");
        assert_eq!(
            e.reason,
            "request [/] contains unrecognized parameter: [foo]"
        );
        let e = Params::parse(Some("b=1&a=2&filter_path=x"), "/", &[]).expect_err("three");
        assert_eq!(
            e.reason,
            "request [/] contains unrecognized parameters: [a], [b], [filter_path]"
        );
        let p = Params::parse(Some("fields=a,,b"), "/", &["fields"]).expect("list");
        assert_eq!(
            p.list("fields"),
            Some(vec!["a".to_string(), "b".to_string()])
        );
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("%zz%41"), "%zzA");
    }
}
