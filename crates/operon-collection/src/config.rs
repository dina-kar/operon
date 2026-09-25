/// How collections use Lance (plan M1.1 Task 7).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LanceConfig {
    /// The Lance session's index cache, in bytes.
    pub index_cache_bytes: usize,
    /// The Lance session's metadata cache (manifests, row-id indexes), in bytes.
    pub metadata_cache_bytes: usize,
    /// Concurrent object-store requests per Lance operation.
    pub io_parallelism: usize,
    /// Rows per Lance data file.
    pub max_rows_per_file: usize,
    /// Rows per row group within a data file.
    pub max_rows_per_group: usize,
}

impl Default for LanceConfig {
    fn default() -> Self {
        Self {
            index_cache_bytes: 256 * 1024 * 1024,
            metadata_cache_bytes: 64 * 1024 * 1024,
            io_parallelism: 32,
            max_rows_per_file: 1_048_576,
            max_rows_per_group: 1_024,
        }
    }
}
