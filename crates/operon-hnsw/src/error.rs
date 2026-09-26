//! The error every engine returns.

/// An HNSW engine error.
#[derive(Debug, thiserror::Error)]
pub enum HnswError {
    /// The caller passed a bad spec, point or query.
    #[error("invalid input: {0}")]
    Invalid(String),
    /// The engine failed.
    #[error("engine: {0}")]
    Engine(String),
    /// The files `open` was given are not an index this engine wrote.
    #[error("corrupt index files: {0}")]
    Corrupt(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
