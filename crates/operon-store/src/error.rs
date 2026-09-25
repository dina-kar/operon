/// Errors returned by [`crate::Store`].
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("object not found: {path}")]
    NotFound { path: String },
    #[error("object already exists: {path}")]
    AlreadyExists { path: String },
    #[error("precondition failed: {path}")]
    PreconditionFailed { path: String },
    #[error("operation not supported by this backend: {0}")]
    NotSupported(String),
    #[error("invalid store url: {0}")]
    InvalidUrl(String),
    #[error("invalid path: {0}")]
    InvalidPath(String),
    #[error("object store error: {0}")]
    Backend(#[source] object_store::Error),
}

impl StoreError {
    /// Whether retrying the whole operation, after re-reading anything it depends on, may
    /// succeed (plan M1.1 ruling 21): Backend, AlreadyExists and PreconditionFailed are; NotFound,
    /// NotSupported, InvalidUrl and InvalidPath are not.
    pub fn is_retryable(&self) -> bool {
        match self {
            StoreError::Backend(_)
            | StoreError::AlreadyExists { .. }
            | StoreError::PreconditionFailed { .. } => true,
            StoreError::NotFound { .. }
            | StoreError::NotSupported(_)
            | StoreError::InvalidUrl(_)
            | StoreError::InvalidPath(_) => false,
        }
    }
}

pub(crate) fn map_err(path: &str, err: object_store::Error) -> StoreError {
    use object_store::Error as E;
    match err {
        E::NotFound { .. } => StoreError::NotFound {
            path: path.to_string(),
        },
        E::AlreadyExists { .. } => StoreError::AlreadyExists {
            path: path.to_string(),
        },
        E::Precondition { .. } => StoreError::PreconditionFailed {
            path: path.to_string(),
        },
        E::NotSupported { .. } | E::NotImplemented { .. } => {
            StoreError::NotSupported(err.to_string())
        }
        other => StoreError::Backend(other),
    }
}
