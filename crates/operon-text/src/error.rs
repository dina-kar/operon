use operon_quickwit::storage::StorageError;

/// Why a split or a delete bitmap could not be written or read.
#[derive(Debug, thiserror::Error)]
pub enum TextError {
    #[error("tantivy: {0}")]
    Tantivy(#[from] tantivy::TantivyError),
    #[error("split storage: {0}")]
    Storage(#[from] StorageError),
    #[error("corrupt: {0}")]
    Corrupt(String),
    #[error("{0}")]
    Other(String),
}
