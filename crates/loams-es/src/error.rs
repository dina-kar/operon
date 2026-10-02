//! Elasticsearch's error envelope, and the mapping from the collection
//! service's errors to it (Task 1 rules 7 and 8; Ruling 19, row E12).

use std::fmt;

use loams_query::ServiceError;
use serde_json::{Map, Value, json};

/// An Elasticsearch error: the HTTP status, the ES `type` and `reason`, and
/// the extra fields ES puts beside them (`index`, `index_uuid`, …).
///
/// An error whose `kind` is empty renders ES's string shape,
/// `{"error": reason, "status": status}` (406 and 405, rule 4 and rule 7).
#[derive(Clone, Debug, PartialEq)]
pub struct EsError {
    pub status: u16,
    /// ES's `type`; empty for the string shape.
    pub kind: String,
    pub reason: String,
    /// Rendered after `type` and `reason`, in order.
    pub extra: Map<String, Value>,
    /// The error a wrapper envelope (`search_phase_execution_exception`)
    /// reports as its root cause.
    pub root_cause: Option<Box<EsError>>,
    /// `Retry-After` in seconds: write backpressure only (row E12).
    pub retry_after_secs: Option<u64>,
}

/// Which kind of request a service error came from: it picks the ES type of
/// a `SchemaViolation` (rule 8).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorContext {
    Write,
    Read,
    Admin,
}

impl EsError {
    pub fn new(status: u16, kind: &str, reason: impl Into<String>) -> Self {
        Self {
            status,
            kind: kind.to_string(),
            reason: reason.into(),
            extra: Map::new(),
            root_cause: None,
            retry_after_secs: None,
        }
    }

    /// ES's string-shaped error, `{"error": reason, "status": status}`.
    pub fn plain(status: u16, reason: impl Into<String>) -> Self {
        Self::new(status, "", reason)
    }

    /// `self` with `key: value` appended to its extra fields.
    pub fn with(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.extra.insert(key.to_string(), value.into());
        self
    }

    /// 400 `illegal_argument_exception`.
    pub fn illegal_argument(reason: impl Into<String>) -> Self {
        Self::new(400, "illegal_argument_exception", reason)
    }

    /// 400 `illegal_argument_exception` for a feature outside Phase A.
    pub fn unsupported(feature: &str) -> Self {
        Self::illegal_argument(format!(
            "Loams does not support [{feature}] (Elasticsearch API Phase A)"
        ))
    }

    /// 400 `parsing_exception`.
    pub fn parsing(reason: impl Into<String>) -> Self {
        Self::new(400, "parsing_exception", reason)
    }

    /// 400 `parse_exception` "request body is required": ES's answer to a
    /// request that needs a body and has none (row T11-3).
    pub fn body_required() -> Self {
        Self::new(400, "parse_exception", "request body is required")
    }

    /// 404 `index_not_found_exception`, with ES's resource fields.
    pub fn index_not_found(index: &str) -> Self {
        Self::new(
            404,
            "index_not_found_exception",
            format!("no such index [{index}]"),
        )
        .with("resource.type", "index_or_alias")
        .with("resource.id", index)
        .with("index_uuid", "_na_")
        .with("index", index)
    }

    /// 400 `resource_already_exists_exception` for an existing index, with
    /// its uuid (`_na_` when unknown, row T1-4).
    pub fn already_exists(index: &str, uuid: &str) -> Self {
        Self::new(
            400,
            "resource_already_exists_exception",
            format!("index [{index}/{uuid}] already exists"),
        )
        .with("index_uuid", uuid)
        .with("index", index)
    }

    /// 404 `aliases_not_found_exception` naming the missing aliases.
    pub fn aliases_not_found(names: &str) -> Self {
        Self::new(
            404,
            "aliases_not_found_exception",
            format!("aliases [{names}] missing"),
        )
        .with("resource.type", "aliases")
        .with("resource.id", names)
    }

    /// 400 `invalid_alias_name_exception`.
    pub fn invalid_alias_name(alias: &str, why: &str) -> Self {
        Self::new(
            400,
            "invalid_alias_name_exception",
            format!("Invalid alias name [{alias}]: {why}"),
        )
    }

    /// 400 `search_phase_execution_exception` wrapping `inner`, as ES
    /// reports a query that failed on its (one) shard. When `inner` is a
    /// Java exception (`illegal_argument_exception`) rather than an ES one,
    /// ES also reports it as the wrapper's `caused_by`, twice nested (the
    /// shard's exception around it), with its own cause below (checked
    /// against the 8.19 oracle, row T11-3).
    pub fn search_phase(inner: EsError, index: &str, node: &str) -> Self {
        let shard = json!({
            "shard": 0,
            "index": index,
            "node": node,
            "reason": Value::Object(inner.cause()),
        });
        let mut outer = Self::new(400, "search_phase_execution_exception", "all shards failed")
            .with("phase", "query")
            .with("grouped", true)
            .with("failed_shards", json!([shard]));
        if inner.kind == "illegal_argument_exception" {
            let brief = |e: &EsError| json!({"type": e.kind, "reason": e.reason});
            let mut deepest = brief(&inner);
            if let Some(cause) = inner.extra.get("caused_by") {
                deepest["caused_by"] = cause.clone();
            }
            let mut first = brief(&inner);
            first["caused_by"] = deepest;
            outer = outer.with("caused_by", first);
        }
        outer.root_cause = Some(Box::new(inner));
        outer
    }

    /// `self` as a shard failure of `index` when it is one: the errors ES
    /// raises while building the query on the shard (`query_shard_exception`
    /// and date-math `parse_exception`) come wrapped in
    /// `search_phase_execution_exception` (row T11-3).
    pub fn at_shard(self, index: &str, node: &str) -> Self {
        if matches!(
            self.kind.as_str(),
            "query_shard_exception" | "parse_exception"
        ) {
            Self::search_phase(self, index, node)
        } else {
            self
        }
    }

    /// The ES error for a collection service error. Exhaustive on purpose:
    /// a new `ServiceError` variant must be mapped here (row E12).
    pub fn from_service(error: ServiceError, context: ErrorContext) -> Self {
        match error {
            ServiceError::NotFound {
                kind: "collection" | "alias",
                name,
            } => Self::index_not_found(&name),
            ServiceError::NotFound { kind: "pin", name } => Self::new(
                404,
                "search_context_missing_exception",
                format!("No search context found for id [{name}]"),
            ),
            ServiceError::NotFound { kind, name } => Self::new(
                404,
                "resource_not_found_exception",
                format!("{kind} [{name}] not found"),
            ),
            // The service error carries no uuid; `PUT /{index}` answers
            // its own existence check with the index's uuid (row T1-4).
            ServiceError::AlreadyExists(name) => Self::already_exists(&name, "_na_"),
            ServiceError::InvalidArgument(message) => Self::illegal_argument(message),
            ServiceError::SchemaViolation { field, message } => {
                let kind = match context {
                    ErrorContext::Write => "document_parsing_exception",
                    ErrorContext::Read => "query_shard_exception",
                    ErrorContext::Admin => "mapper_parsing_exception",
                };
                Self::new(
                    400,
                    kind,
                    format!("failed to parse field [{field}]: {message}"),
                )
            }
            ServiceError::Unavailable(message) => {
                Self::new(503, "unavailable_shards_exception", message)
            }
            ServiceError::Timeout => Self::new(504, "timeout_exception", "request timed out"),
            ServiceError::Internal(message) => Self::new(500, "exception", message),
            ServiceError::ResourceExhausted {
                message,
                retry_after_ms,
            } => {
                let mut error = Self::new(429, "es_rejected_execution_exception", message);
                error.retry_after_secs = Some(retry_after_ms.div_ceil(1000).max(1));
                error
            }
        }
    }

    /// `{"type": …, "reason": …, …extra}`.
    fn cause(&self) -> Map<String, Value> {
        let mut cause = Map::new();
        cause.insert("type".to_string(), Value::String(self.kind.clone()));
        cause.insert("reason".to_string(), Value::String(self.reason.clone()));
        cause.extend(self.extra.clone());
        cause
    }

    /// `{"type": …, "reason": …, …extra}` as a JSON value: a `_bulk`
    /// item's `error`.
    pub fn cause_value(&self) -> Value {
        Value::Object(self.cause())
    }

    /// The innermost causes: the wrapped error's, else this one.
    fn root_causes(&self) -> Vec<Value> {
        match &self.root_cause {
            Some(inner) => inner.root_causes(),
            None => vec![Value::Object(self.cause())],
        }
    }

    /// The response body (rule 7).
    pub fn to_body(&self) -> Value {
        if self.kind.is_empty() {
            return json!({"error": self.reason, "status": self.status});
        }
        let mut error = Map::new();
        error.insert("root_cause".to_string(), Value::Array(self.root_causes()));
        error.extend(self.cause());
        json!({"error": error, "status": self.status})
    }
}

impl fmt::Display for EsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.kind.is_empty() {
            write!(f, "{}", self.reason)
        } else {
            write!(f, "{}: {}", self.kind, self.reason)
        }
    }
}

impl std::error::Error for EsError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_envelope_repeats_the_error_as_its_root_cause() {
        let body = EsError::index_not_found("x").to_body();
        assert_eq!(body["status"], 404);
        assert_eq!(body["error"]["type"], "index_not_found_exception");
        assert_eq!(body["error"]["reason"], "no such index [x]");
        assert_eq!(body["error"]["index"], "x");
        assert_eq!(body["error"]["index_uuid"], "_na_");
        assert_eq!(body["error"]["resource.type"], "index_or_alias");
        assert_eq!(body["error"]["resource.id"], "x");
        assert_eq!(body["error"]["root_cause"][0], {
            let mut cause = body["error"].clone();
            cause
                .as_object_mut()
                .expect("object")
                .shift_remove("root_cause");
            cause
        });
    }

    #[test]
    fn a_search_phase_wrapper_reports_the_inner_error() {
        let inner = EsError::parsing("unknown query [foo]");
        let body = EsError::search_phase(inner, "i", "loams").to_body();
        let error = &body["error"];
        assert_eq!(body["status"], 400);
        assert_eq!(error["type"], "search_phase_execution_exception");
        assert_eq!(error["reason"], "all shards failed");
        assert_eq!(error["phase"], "query");
        assert_eq!(error["grouped"], true);
        assert_eq!(error["root_cause"][0]["type"], "parsing_exception");
        assert_eq!(error["failed_shards"][0]["index"], "i");
        assert_eq!(error["failed_shards"][0]["node"], "loams");
        assert_eq!(
            error["failed_shards"][0]["reason"]["reason"],
            "unknown query [foo]"
        );
    }

    #[test]
    fn a_plain_error_is_string_shaped() {
        let body =
            EsError::plain(406, "Content-Type header [text/plain] is not supported").to_body();
        assert_eq!(
            body,
            json!({"error": "Content-Type header [text/plain] is not supported", "status": 406})
        );
    }

    #[test]
    fn service_errors_map_to_es_types() {
        let map = |e| EsError::from_service(e, ErrorContext::Write);
        let not_found = |kind, name: &str| ServiceError::NotFound {
            kind,
            name: name.to_string(),
        };
        let e = map(not_found("alias", "a"));
        assert_eq!(
            (e.status, e.kind.as_str()),
            (404, "index_not_found_exception")
        );
        let e = map(not_found("pin", "7"));
        assert_eq!(e.kind, "search_context_missing_exception");
        assert_eq!(e.reason, "No search context found for id [7]");
        let e = map(not_found("document", "d"));
        assert_eq!(e.kind, "resource_not_found_exception");
        assert_eq!(e.reason, "document [d] not found");
        let e = map(ServiceError::AlreadyExists("x".to_string()));
        assert_eq!(
            (e.status, e.kind.as_str()),
            (400, "resource_already_exists_exception")
        );
        let e = map(ServiceError::InvalidArgument("bad".to_string()));
        assert_eq!(
            (e.status, e.kind.as_str(), e.reason.as_str()),
            (400, "illegal_argument_exception", "bad")
        );
        let violation = || ServiceError::SchemaViolation {
            field: "f".to_string(),
            message: "not a number".to_string(),
        };
        for (context, kind) in [
            (ErrorContext::Write, "document_parsing_exception"),
            (ErrorContext::Read, "query_shard_exception"),
            (ErrorContext::Admin, "mapper_parsing_exception"),
        ] {
            let e = EsError::from_service(violation(), context);
            assert_eq!((e.status, e.kind.as_str()), (400, kind));
            assert_eq!(e.reason, "failed to parse field [f]: not a number");
        }
        let e = map(ServiceError::Unavailable("down".to_string()));
        assert_eq!(
            (e.status, e.kind.as_str()),
            (503, "unavailable_shards_exception")
        );
        let e = map(ServiceError::Timeout);
        assert_eq!((e.status, e.kind.as_str()), (504, "timeout_exception"));
        let e = map(ServiceError::Internal("boom".to_string()));
        assert_eq!((e.status, e.kind.as_str()), (500, "exception"));
        assert_eq!(e.retry_after_secs, None);
    }

    #[test]
    fn backpressure_is_429_with_retry_after_in_whole_seconds() {
        for (ms, secs) in [(0, 1), (1, 1), (999, 1), (1000, 1), (1001, 2), (2500, 3)] {
            let e = EsError::from_service(
                ServiceError::ResourceExhausted {
                    message: "over budget".to_string(),
                    retry_after_ms: ms,
                },
                ErrorContext::Write,
            );
            assert_eq!(e.status, 429);
            assert_eq!(e.kind, "es_rejected_execution_exception");
            assert_eq!(e.reason, "over budget");
            assert_eq!(e.retry_after_secs, Some(secs), "{ms} ms");
        }
    }
}
