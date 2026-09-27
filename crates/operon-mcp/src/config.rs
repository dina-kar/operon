//! The MCP server's settings (plan M1.6 Task 7).

use std::time::Duration;

/// How the MCP endpoint behaves. The defaults are loopback-only (Rulings 7
/// and 11): rmcp's `Host` allow-list and no allowed browser origin.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McpConfig {
    /// Where the endpoint is served on the MCP listener (`/mcp`).
    pub path: String,
    /// The namespace of requests without an `Operon-Namespace` header.
    pub namespace: String,
    /// `Host` values the endpoint answers (DNS-rebinding protection).
    pub allowed_hosts: Vec<String>,
    /// Browser origins the endpoint answers; empty refuses every request
    /// carrying `Origin`.
    pub allowed_origins: Vec<String>,
    /// Serve only `2026-07-28` and require its per-request metadata.
    pub strict_stateless: bool,
    /// Where `memory_write` stores memories by default (Task 8).
    pub memory_collection: String,
    /// The largest `search` `limit` (Task 8).
    pub search_max_limit: usize,
    /// The most ids one `get_documents` call takes.
    pub get_max_ids: usize,
    /// The most rows one `sql` call returns (Task 8).
    pub sql_max_rows: usize,
    /// How long one `sql` call may run (Task 8).
    pub sql_timeout: Duration,
    /// The most JSON bytes a tool result holds (Ruling 13).
    pub max_output_bytes: usize,
    /// How long clients may cache `tools/list` and `server/discover`.
    pub tools_ttl: Duration,
}

impl Default for McpConfig {
    fn default() -> Self {
        Self {
            path: "/mcp".to_string(),
            namespace: "default".to_string(),
            allowed_hosts: vec!["localhost".into(), "127.0.0.1".into(), "::1".into()],
            allowed_origins: Vec::new(),
            strict_stateless: false,
            memory_collection: "memories".to_string(),
            search_max_limit: 100,
            get_max_ids: 100,
            sql_max_rows: 1_000,
            sql_timeout: Duration::from_secs(30),
            max_output_bytes: 1_048_576,
            tools_ttl: Duration::from_secs(600),
        }
    }
}

/// Why an [`McpConfig`] is refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum McpConfigError {
    #[error(
        "mcp path {0:?} must start with '/', must not be '/', '/health', '/ready' or start with '/v1'"
    )]
    Path(String),
    #[error("mcp namespace must not be empty")]
    Namespace,
    #[error("mcp setting {0} must be at least 1")]
    Limit(&'static str),
}

impl McpConfig {
    /// Checks the path, the namespace and that every limit is at least 1.
    pub fn validate(&self) -> Result<(), McpConfigError> {
        let path = self.path.as_str();
        if !path.starts_with('/')
            || matches!(path, "/" | "/health" | "/ready")
            || path.starts_with("/v1")
        {
            return Err(McpConfigError::Path(self.path.clone()));
        }
        if self.namespace.is_empty() {
            return Err(McpConfigError::Namespace);
        }
        let limits = [
            ("search_max_limit", self.search_max_limit == 0),
            ("get_max_ids", self.get_max_ids == 0),
            ("sql_max_rows", self.sql_max_rows == 0),
            ("sql_timeout", self.sql_timeout.is_zero()),
            ("max_output_bytes", self.max_output_bytes == 0),
        ];
        match limits.into_iter().find(|(_, zero)| *zero) {
            Some((name, _)) => Err(McpConfigError::Limit(name)),
            None => Ok(()),
        }
    }
}
