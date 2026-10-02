//! The gateway's errors, with Qdrant's HTTP status, gRPC code and message
//! for each ("Qdrant protocol facts", status table; E2).

use std::time::Duration;

use http::StatusCode;
use loams_query::ServiceError;
use tonic::Code;

/// Why a gateway request failed.
#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    /// A collection service error, with Qdrant's message.
    #[error("{}", service_message(.0))]
    Service(ServiceError),
    #[error("Wrong input: {0}")]
    BadRequest(String),
    /// `what` is `JSON body`, `query parameters` or `path parameters`.
    #[error("Format error in {what}: {message}")]
    Format { what: &'static str, message: String },
    /// `GET /points/{id}` of a missing point.
    #[error("Not found: Point with id {0} does not exists!")]
    PointNotFound(String),
    /// An update of a missing point.
    #[error("Not found: No point with id {0} found")]
    PointsNotFound(String),
    #[error("Unsupported in Loams: {0}")]
    Unsupported(String),
    #[error("Timeout: request timed out after {0:?}")]
    Timeout(Duration),
    /// The gateway's own existence check (Ruling 17).
    #[error("Wrong input: Collection `{0}` already exists!")]
    CollectionExists(String),
    /// A body over `max_request_bytes`.
    #[error("Format error in JSON body: payload too large")]
    TooLarge,
}

impl From<ServiceError> for GatewayError {
    fn from(err: ServiceError) -> Self {
        GatewayError::Service(err)
    }
}

impl GatewayError {
    /// A `Format` error in the JSON body.
    pub fn json(message: impl Into<String>) -> Self {
        GatewayError::Format {
            what: "JSON body",
            message: message.into(),
        }
    }

    /// The HTTP status of the REST answer.
    pub fn http_status(&self) -> StatusCode {
        match self {
            GatewayError::Service(err) => service_status(err).0,
            GatewayError::BadRequest(_) | GatewayError::Format { .. } => StatusCode::BAD_REQUEST,
            GatewayError::PointNotFound(_) | GatewayError::PointsNotFound(_) => {
                StatusCode::NOT_FOUND
            }
            GatewayError::Unsupported(_) => StatusCode::NOT_IMPLEMENTED,
            GatewayError::Timeout(_) => StatusCode::REQUEST_TIMEOUT,
            GatewayError::CollectionExists(_) => StatusCode::CONFLICT,
            GatewayError::TooLarge => StatusCode::PAYLOAD_TOO_LARGE,
        }
    }

    /// The gRPC code of the answer.
    pub fn grpc_code(&self) -> Code {
        match self {
            GatewayError::Service(err) => service_status(err).1,
            GatewayError::BadRequest(_) | GatewayError::Format { .. } => Code::InvalidArgument,
            GatewayError::PointNotFound(_) | GatewayError::PointsNotFound(_) => Code::NotFound,
            GatewayError::Unsupported(_) => Code::Unimplemented,
            GatewayError::Timeout(_) => Code::DeadlineExceeded,
            GatewayError::CollectionExists(_) => Code::AlreadyExists,
            GatewayError::TooLarge => Code::ResourceExhausted,
        }
    }

    /// The gRPC answer: the code and the Display message, plus metadata
    /// `retry-after` for write backpressure (E2).
    pub fn grpc_status(&self) -> tonic::Status {
        let mut status = tonic::Status::new(self.grpc_code(), self.to_string());
        if let Some(secs) = self.retry_after_secs() {
            status
                .metadata_mut()
                .insert("retry-after", tonic::metadata::MetadataValue::from(secs));
        }
        status
    }

    /// `max(1, ceil(retry_after_ms / 1000))` for write backpressure, `None`
    /// for every other error (E2): Qdrant's clients raise their
    /// rate-limit error only when the value is present and non-zero.
    pub fn retry_after_secs(&self) -> Option<u64> {
        match self {
            GatewayError::Service(ServiceError::ResourceExhausted { retry_after_ms, .. }) => {
                Some(retry_after_ms.div_ceil(1000).max(1))
            }
            _ => None,
        }
    }
}

/// The HTTP status and gRPC code of a service error. Exhaustive on purpose:
/// a new `ServiceError` variant must be mapped here (E2).
fn service_status(err: &ServiceError) -> (StatusCode, Code) {
    match err {
        ServiceError::NotFound { .. } => (StatusCode::NOT_FOUND, Code::NotFound),
        ServiceError::AlreadyExists(_) => (StatusCode::CONFLICT, Code::AlreadyExists),
        ServiceError::InvalidArgument(_) | ServiceError::SchemaViolation { .. } => {
            (StatusCode::BAD_REQUEST, Code::InvalidArgument)
        }
        ServiceError::Unavailable(_) => (StatusCode::SERVICE_UNAVAILABLE, Code::Unavailable),
        ServiceError::Timeout => (StatusCode::REQUEST_TIMEOUT, Code::DeadlineExceeded),
        ServiceError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, Code::Internal),
        ServiceError::ResourceExhausted { .. } => {
            (StatusCode::TOO_MANY_REQUESTS, Code::ResourceExhausted)
        }
    }
}

/// Qdrant's message for a service error. Exhaustive on purpose (E2).
fn service_message(err: &ServiceError) -> String {
    match err {
        ServiceError::NotFound {
            kind: "collection",
            name,
        } => format!("Not found: Collection `{name}` doesn't exist!"),
        ServiceError::NotFound {
            kind: "alias",
            name,
        } => format!("Not found: Alias {name} does not exists!"),
        ServiceError::NotFound { kind, name } => {
            format!("Not found: {kind} `{name}` doesn't exist!")
        }
        ServiceError::AlreadyExists(name) => {
            format!("Wrong input: Collection `{name}` already exists!")
        }
        ServiceError::InvalidArgument(message) => format!("Wrong input: {message}"),
        ServiceError::SchemaViolation { field, message } => {
            format!("Wrong input: {field}: {message}")
        }
        ServiceError::Unavailable(message) => format!("Service unavailable: {message}"),
        // No duration to report (row T1-1).
        ServiceError::Timeout => "Timeout: request timed out".to_string(),
        ServiceError::Internal(message) => format!("Service internal error: {message}"),
        ServiceError::ResourceExhausted { message, .. } => {
            format!("Rate limiting exceeded: {message}")
        }
    }
}
