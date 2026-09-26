//! The API's error body and how every layer's error maps to it (M0.3 plan
//! Task 7; plan M1.2 Task 11 rule 3 and Ruling 17).

use axum::extract::rejection::{BytesRejection, PathRejection, QueryRejection};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use operon_common::meta::{ApplyError, MetaError};
use operon_link::LinkError;
use operon_log::LogError;
use operon_query::ServiceError;
use serde_json::Value;

/// An error response: `{"error": code, "message": ...}` plus any extra
/// fields. Every 503 carries `Retry-After: 1`.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
    extra: serde_json::Map<String, Value>,
}

impl ApiError {
    pub(crate) fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            extra: serde_json::Map::new(),
        }
    }

    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_argument", message)
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
    }

    pub(crate) fn with(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.extra.insert(key.to_string(), value.into());
        self
    }

    /// The response status.
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// The body's `error` value.
    pub fn code(&self) -> &'static str {
        self.code
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut body = serde_json::Map::new();
        body.insert("error".to_string(), Value::from(self.code));
        body.insert("message".to_string(), Value::from(self.message));
        body.extend(self.extra);
        let mut response = (self.status, axum::Json(Value::Object(body))).into_response();
        // Every 503 succeeds on retry (Ruling 17).
        if self.status == StatusCode::SERVICE_UNAVAILABLE {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
        response
    }
}

pub(crate) fn unavailable(message: impl Into<String>) -> ApiError {
    ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "unavailable", message)
}

pub(crate) fn internal(message: impl Into<String>) -> ApiError {
    ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", message)
}

fn conflict(message: impl Into<String>) -> ApiError {
    ApiError::new(StatusCode::CONFLICT, "conflict", message)
}

/// Turns an axum extractor rejection (a bad path segment, query, or body,
/// including a body over the limit) into the API's JSON error body, keeping
/// its status.
fn rejected(status: StatusCode, message: String) -> ApiError {
    let code = if status.is_server_error() {
        "internal"
    } else {
        "invalid_argument"
    };
    ApiError::new(status, code, message)
}

impl From<PathRejection> for ApiError {
    fn from(err: PathRejection) -> Self {
        rejected(err.status(), err.body_text())
    }
}

impl From<QueryRejection> for ApiError {
    fn from(err: QueryRejection) -> Self {
        rejected(err.status(), err.body_text())
    }
}

impl From<BytesRejection> for ApiError {
    fn from(err: BytesRejection) -> Self {
        rejected(err.status(), err.body_text())
    }
}

impl From<ServiceError> for ApiError {
    /// `{status: http_status(), error: code(), message: Display}` plus
    /// `kind` and `name` for `NotFound` and `field` for `SchemaViolation`
    /// (rule 3).
    fn from(err: ServiceError) -> Self {
        let status =
            StatusCode::from_u16(err.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let mut api = ApiError::new(status, err.code(), err.to_string());
        match err {
            ServiceError::NotFound { kind, name } => {
                api = api.with("kind", kind).with("name", name);
            }
            ServiceError::SchemaViolation { field, .. } => api = api.with("field", field),
            _ => {}
        }
        api
    }
}

impl From<MetaError> for ApiError {
    fn from(err: MetaError) -> Self {
        let message = err.to_string();
        match err {
            MetaError::Rejected(apply) => match apply {
                ApplyError::InvalidArgument(_) => ApiError::invalid(message),
                ApplyError::NamespaceExists(id) => {
                    ApiError::new(StatusCode::CONFLICT, "already_exists", message).with("id", id.0)
                }
                ApplyError::StreamExists(id) => {
                    ApiError::new(StatusCode::CONFLICT, "already_exists", message).with("id", id.0)
                }
                ApplyError::LinkExists(id) => {
                    ApiError::new(StatusCode::CONFLICT, "already_exists", message).with("id", id.0)
                }
                ApplyError::CollectionExists(id) => {
                    ApiError::new(StatusCode::CONFLICT, "already_exists", message).with("id", id.0)
                }
                ApplyError::NameTaken(_) => {
                    ApiError::new(StatusCode::CONFLICT, "already_exists", message)
                }
                ApplyError::NamespaceNotFound(_)
                | ApplyError::StreamNotFound(_)
                | ApplyError::PartitionNotFound { .. }
                | ApplyError::CollectionNotFound(_)
                | ApplyError::UnknownCollection(_) => ApiError::not_found(message),
                ApplyError::IncompatibleSchema(_) => ApiError::invalid(message),
                // A conflict is the caller's to resolve, not a server bug
                // (Ruling 17). The message names the current state.
                ApplyError::VersionMismatch { .. }
                | ApplyError::Fenced { .. }
                | ApplyError::LeaseHeld { .. }
                | ApplyError::LeaseLost { .. }
                | ApplyError::SchemaVersionMismatch { .. } => conflict(message),
                // These succeed on retry (Ruling 17).
                ApplyError::StaleObject { .. }
                | ApplyError::StaleCommit { .. }
                | ApplyError::IndexMismatch { .. } => unavailable(message),
            },
            MetaError::NotLeader { .. }
            | MetaError::Timeout
            | MetaError::Unavailable(_)
            | MetaError::ClockSkew { .. } => unavailable(message),
            _ => internal(message),
        }
    }
}

impl From<LogError> for ApiError {
    fn from(err: LogError) -> Self {
        let message = err.to_string();
        match err {
            LogError::InvalidArgument(_) => ApiError::invalid(message),
            LogError::UnknownStream(_) | LogError::UnknownPartition { .. } => {
                ApiError::not_found(message)
            }
            LogError::OffsetOutOfRange {
                requested,
                log_start_offset,
                high_watermark,
            } => ApiError::new(
                StatusCode::RANGE_NOT_SATISFIABLE,
                "offset_out_of_range",
                message,
            )
            .with("offset", requested)
            .with("log_start_offset", log_start_offset)
            .with("high_watermark", high_watermark),
            // Nothing (or possibly something) was committed; a retry is the
            // caller's call, and the condition is transient.
            LogError::Backpressure
            | LogError::CommitUnknown(_)
            | LogError::Closed
            | LogError::Store(_)
            | LogError::Cache(_) => unavailable(message),
            LogError::Meta(meta) => meta.into(),
            LogError::Task(_) => internal(message),
            LogError::Corrupt(_) | LogError::UnsupportedEncoding(_) => internal(message),
        }
    }
}

impl From<LinkError> for ApiError {
    fn from(err: LinkError) -> Self {
        let message = err.to_string();
        match err {
            LinkError::Meta(meta) => meta.into(),
            LinkError::Log(log) => log.into(),
            LinkError::NotFound(_) => ApiError::not_found(message),
            LinkError::Store(_) | LinkError::Blocked(_) => unavailable(message),
            LinkError::Corrupt(_) => internal(message),
            LinkError::Target {
                retryable: true, ..
            } => unavailable(message),
            LinkError::Target {
                retryable: false, ..
            } => internal(message),
        }
    }
}
