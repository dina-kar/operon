use operon_meta::MetaError;
use operon_quickwit::storage::StorageErrorKind;
use operon_store::StoreError;
use operon_text::TextError;

/// Why a record could not be encoded or decoded (overview §6.2). At apply
/// time every decode error dead-letters the record (plan M1.1 Ruling 11).
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("missing record key")]
    MissingKey,
    #[error("missing record value")]
    MissingValue,
    #[error("unknown codec version {0}")]
    UnknownVersion(u8),
    #[error("malformed record: {0}")]
    Malformed(String),
    #[error("record key does not match the operation's primary key")]
    KeyMismatch,
    #[error("invalid primary key: {0}")]
    InvalidKey(String),
    #[error("record value of {0} bytes exceeds 16 MiB")]
    TooLarge(usize),
}

/// Why a sparse vector is not canonical (overview A27).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SparseVectorError {
    #[error("indices and values must have the same length ({indices} != {values})")]
    LengthMismatch { indices: usize, values: usize },
    #[error("index {0} appears more than once")]
    DuplicateIndex(u32),
    #[error("value for index {0} is not finite")]
    NonFinite(u32),
}

/// Why a collection storage operation failed.
#[derive(Debug, thiserror::Error)]
pub enum CollectionError {
    #[error("lance: {0}")]
    Lance(#[from] lance::Error),
    #[error("object store: {0}")]
    Store(#[from] StoreError),
    #[error("metastore: {0}")]
    Meta(#[from] MetaError),
    #[error("corrupt collection data: {0}")]
    Corrupt(String),
    #[error("not found: {0}")]
    NotFound(String),
    /// A pinned read of a manifest version that is no longer retained
    /// (Ruling 12).
    #[error("collection manifest version {0} is no longer retained")]
    ManifestGone(u64),
    #[error("text: {0}")]
    Text(#[from] TextError),
    #[error("internal: {0}")]
    Internal(String),
}

impl CollectionError {
    /// Whether retrying the whole operation, after re-reading what it depends
    /// on, may succeed:
    /// - a store error the store deems retryable (plan M1.1 Ruling 21);
    /// - a metastore that is not the leader, timed out or is unavailable;
    /// - Lance's own I/O errors; a Lance create-only write that lost
    ///   (`DatasetAlreadyExists`), like the store's `AlreadyExists`; a commit
    ///   whose outcome Lance could not verify, too much write contention, or a
    ///   timeout. Retrying an ambiguous detached commit is always safe: if the
    ///   first attempt landed, it is an orphan no manifest references (R7);
    /// - a split or bitmap read that failed with an I/O error.
    pub fn is_retryable(&self) -> bool {
        match self {
            CollectionError::Store(err) => err.is_retryable(),
            CollectionError::Meta(err) => matches!(
                err,
                MetaError::NotLeader { .. } | MetaError::Timeout | MetaError::Unavailable(_)
            ),
            CollectionError::Lance(err) => {
                err.is_commit_status_unknown()
                    || matches!(
                        err,
                        lance::Error::IO { .. }
                            | lance::Error::DatasetAlreadyExists { .. }
                            | lance::Error::TooMuchWriteContention { .. }
                            | lance::Error::Timeout { .. }
                    )
            }
            CollectionError::Text(TextError::Storage(err)) => err.kind() == StorageErrorKind::Io,
            CollectionError::Text(_)
            | CollectionError::Corrupt(_)
            | CollectionError::NotFound(_)
            | CollectionError::ManifestGone(_)
            | CollectionError::Internal(_) => false,
        }
    }
}
