//! Tool errors: a refused call is a successful JSON-RPC answer whose tool
//! result has `isError: true` and a structured body (plan M1.6 Task 7 rule
//! 7).

use operon_query::ServiceError;
use rmcp::model::CallToolResult;
use serde_json::{Value, json};

/// Why a tool call failed: `code` is the native API's error code.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolError {
    pub code: &'static str,
    pub message: String,
    /// The offending field of a `schema_violation`.
    pub field: Option<String>,
    /// How long to wait before retrying a `resource_exhausted` (row E18).
    pub retry_after_ms: Option<u64>,
}

impl ToolError {
    /// A tool error with `code` and `message`.
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            field: None,
            retry_after_ms: None,
        }
    }

    /// An `invalid_argument` error.
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new("invalid_argument", message)
    }

    /// The structured body: `{"error", "message"}`, plus `"field"` and
    /// `"retry_after_ms"` when set.
    pub fn body(&self) -> Value {
        let mut body = json!({ "error": self.code, "message": self.message });
        if let Some(field) = &self.field {
            body["field"] = json!(field);
        }
        if let Some(ms) = self.retry_after_ms {
            body["retry_after_ms"] = json!(ms);
        }
        body
    }

    /// The tool result carrying this error (`isError: true`).
    pub fn into_result(self) -> CallToolResult {
        CallToolResult::structured_error(self.body())
    }
}

impl From<ServiceError> for ToolError {
    fn from(err: ServiceError) -> Self {
        match err {
            ServiceError::NotFound { kind, name } => {
                Self::new("not_found", format!("{kind} `{name}` not found"))
            }
            ServiceError::AlreadyExists(message) => Self::new("already_exists", message),
            ServiceError::InvalidArgument(message) => Self::invalid(message),
            ServiceError::SchemaViolation { field, message } => Self {
                field: Some(field),
                ..Self::new("schema_violation", message)
            },
            ServiceError::Unavailable(message) => Self::new("unavailable", message),
            ServiceError::Timeout => Self::new("timeout", "the operation timed out"),
            ServiceError::Internal(message) => Self::new("internal", message),
            ServiceError::ResourceExhausted {
                message,
                retry_after_ms,
            } => Self {
                retry_after_ms: Some(retry_after_ms),
                ..Self::new("resource_exhausted", message)
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_errors_map_to_codes() {
        let not_found = ToolError::from(ServiceError::NotFound {
            kind: "collection",
            name: "kb".into(),
        });
        assert_eq!(not_found.code, "not_found");
        assert_eq!(not_found.message, "collection `kb` not found");
        let violation = ToolError::from(ServiceError::SchemaViolation {
            field: "embedding".into(),
            message: "wrong dimension".into(),
        });
        assert_eq!(
            violation.body(),
            json!({"error": "schema_violation", "message": "wrong dimension", "field": "embedding"})
        );
        let exhausted = ToolError::from(ServiceError::ResourceExhausted {
            message: "busy".into(),
            retry_after_ms: 250,
        });
        assert_eq!(exhausted.body()["retry_after_ms"], 250);
        for (err, code) in [
            (ServiceError::AlreadyExists("x".into()), "already_exists"),
            (
                ServiceError::InvalidArgument("x".into()),
                "invalid_argument",
            ),
            (ServiceError::Unavailable("x".into()), "unavailable"),
            (ServiceError::Timeout, "timeout"),
            (ServiceError::Internal("x".into()), "internal"),
        ] {
            assert_eq!(ToolError::from(err).code, code);
        }
        let result = ToolError::invalid("bad").into_result();
        assert_eq!(result.is_error, Some(true));
        assert_eq!(
            result.structured_content,
            Some(json!({"error": "invalid_argument", "message": "bad"}))
        );
    }
}
