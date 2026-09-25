//! The one error type every `operon-query` operation returns, and its JSON
//! body (plan M1.2 Task 1 rule 6; overview R15).

use operon_collection::{CollectionError, WriteError};
use operon_common::meta::{ApplyError, MetaError};
use operon_log::LogError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value, json};

/// Why a service call failed. Every variant has one code and one HTTP status
/// on the native surface (R15).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ServiceError {
    /// `kind` is one of [`NOT_FOUND_KINDS`].
    #[error("{kind} {name:?} not found")]
    NotFound { kind: &'static str, name: String },
    #[error("already exists: {0}")]
    AlreadyExists(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("schema violation on {field}: {message}")]
    SchemaViolation { field: String, message: String },
    #[error("unavailable: {0}")]
    Unavailable(String),
    #[error("timed out")]
    Timeout,
    #[error("internal error: {0}")]
    Internal(String),
}

/// The kinds a [`ServiceError::NotFound`] names; a body naming another kind
/// reads back as `"object"`.
pub const NOT_FOUND_KINDS: [&str; 9] = [
    "namespace",
    "collection",
    "alias",
    "pin",
    "document",
    "field",
    "vector",
    "stream",
    "object",
];

impl ServiceError {
    /// The body's `error` value.
    pub fn code(&self) -> &'static str {
        match self {
            ServiceError::NotFound { .. } => "not_found",
            ServiceError::AlreadyExists(_) => "already_exists",
            ServiceError::InvalidArgument(_) => "invalid_argument",
            ServiceError::SchemaViolation { .. } => "schema_violation",
            ServiceError::Unavailable(_) => "unavailable",
            ServiceError::Timeout => "timeout",
            ServiceError::Internal(_) => "internal",
        }
    }

    /// The status of the native REST surface.
    pub fn http_status(&self) -> u16 {
        match self {
            ServiceError::NotFound { .. } => 404,
            ServiceError::AlreadyExists(_) => 409,
            ServiceError::InvalidArgument(_) | ServiceError::SchemaViolation { .. } => 400,
            ServiceError::Unavailable(_) => 503,
            ServiceError::Timeout => 504,
            ServiceError::Internal(_) => 500,
        }
    }

    /// Whether the same call may succeed later.
    pub fn is_retryable(&self) -> bool {
        matches!(self, ServiceError::Unavailable(_) | ServiceError::Timeout)
    }

    /// `{"error": code, "message": Display}`, plus `kind` and `name` for
    /// `NotFound` and `field` for `SchemaViolation`.
    pub fn to_body(&self) -> Value {
        let mut body = json!({"error": self.code(), "message": self.to_string()});
        match self {
            ServiceError::NotFound { kind, name } => {
                body["kind"] = json!(kind);
                body["name"] = json!(name);
            }
            ServiceError::SchemaViolation { field, .. } => body["field"] = json!(field),
            _ => {}
        }
        body
    }

    /// The error a [`ServiceError::to_body`] body describes; `None` for a body
    /// that is not an object with a known `error` code.
    pub fn from_body(body: &Value) -> Option<Self> {
        let body = body.as_object()?;
        let text = |key: &str| body.get(key).and_then(Value::as_str);
        let message = text("message").unwrap_or_default();
        // The Display prefix of the variant, when the message carries it.
        let detail = |prefix: &str| message.strip_prefix(prefix).unwrap_or(message).to_string();
        Some(match text("error")? {
            "not_found" => {
                let kind = text("kind").unwrap_or("object");
                let kind = NOT_FOUND_KINDS
                    .iter()
                    .find(|known| **known == kind)
                    .copied()
                    .unwrap_or("object");
                ServiceError::NotFound {
                    kind,
                    name: text("name").unwrap_or_default().to_string(),
                }
            }
            "already_exists" => ServiceError::AlreadyExists(detail("already exists: ")),
            "invalid_argument" => ServiceError::InvalidArgument(detail("invalid argument: ")),
            "schema_violation" => {
                let field = text("field").unwrap_or_default().to_string();
                let message = detail(&format!("schema violation on {field}: "));
                ServiceError::SchemaViolation { field, message }
            }
            "unavailable" => ServiceError::Unavailable(detail("unavailable: ")),
            "timeout" => ServiceError::Timeout,
            "internal" => ServiceError::Internal(detail("internal error: ")),
            _ => return None,
        })
    }
}

impl Serialize for ServiceError {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.to_body().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ServiceError {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let body = Value::Object(Map::deserialize(deserializer)?);
        ServiceError::from_body(&body)
            .ok_or_else(|| serde::de::Error::custom(format!("not a service error body: {body}")))
    }
}

// ----- Mapping the errors of the layers below (plan M1.2 Task 9 rule 9) -----

impl From<ApplyError> for ServiceError {
    /// A metastore command's refusal. `NotFound` names the id or name the
    /// refusal carries; callers that know the requested name replace it.
    fn from(err: ApplyError) -> Self {
        match err {
            ApplyError::InvalidArgument(message) => ServiceError::InvalidArgument(message),
            err @ ApplyError::IncompatibleSchema(_) => {
                ServiceError::InvalidArgument(err.to_string())
            }
            ApplyError::CollectionExists(id) => ServiceError::AlreadyExists(id.to_string()),
            ApplyError::NameTaken(name) => ServiceError::AlreadyExists(name),
            ApplyError::CollectionNotFound(id) => ServiceError::NotFound {
                kind: "collection",
                name: id.to_string(),
            },
            ApplyError::UnknownCollection(name) => ServiceError::NotFound {
                kind: "collection",
                name,
            },
            ApplyError::NamespaceNotFound(id) => ServiceError::NotFound {
                kind: "collection",
                name: id.to_string(),
            },
            // Another writer changed the schema first: the caller retries.
            err @ ApplyError::SchemaVersionMismatch { .. } => {
                ServiceError::Unavailable(err.to_string())
            }
            other => ServiceError::Internal(other.to_string()),
        }
    }
}

impl From<MetaError> for ServiceError {
    fn from(err: MetaError) -> Self {
        match err {
            MetaError::Rejected(err) => err.into(),
            err @ (MetaError::NotLeader { .. }
            | MetaError::Timeout
            | MetaError::Unavailable(_)
            | MetaError::ClockSkew { .. }) => ServiceError::Unavailable(err.to_string()),
            other => ServiceError::Internal(other.to_string()),
        }
    }
}

impl From<LogError> for ServiceError {
    fn from(err: LogError) -> Self {
        match err {
            LogError::CommitUnknown(detail) => ServiceError::Unavailable(format!(
                "the outcome of the write is unknown ({detail}); retrying keyed ops is safe"
            )),
            err @ (LogError::Backpressure
            | LogError::Closed
            | LogError::Store(_)
            | LogError::Cache(_)) => ServiceError::Unavailable(err.to_string()),
            LogError::InvalidArgument(message) => ServiceError::InvalidArgument(message),
            other => ServiceError::Internal(other.to_string()),
        }
    }
}

impl From<WriteError> for ServiceError {
    /// `CollectionNotFound` names the id; callers replace it with the name.
    fn from(err: WriteError) -> Self {
        match err {
            WriteError::CollectionNotFound(id) => ServiceError::NotFound {
                kind: "collection",
                name: id.to_string(),
            },
            err @ WriteError::TooManyOps(_) => ServiceError::InvalidArgument(err.to_string()),
            WriteError::Log(err) => err.into(),
            WriteError::Meta(err) => err.into(),
        }
    }
}

impl From<CollectionError> for ServiceError {
    fn from(err: CollectionError) -> Self {
        if err.is_retryable() {
            return ServiceError::Unavailable(err.to_string());
        }
        match err {
            CollectionError::ManifestGone(version) => ServiceError::NotFound {
                kind: "pin",
                name: version.to_string(),
            },
            CollectionError::NotFound(name) => ServiceError::NotFound {
                kind: "collection",
                name,
            },
            other => ServiceError::Internal(other.to_string()),
        }
    }
}
