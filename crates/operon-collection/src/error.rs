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
