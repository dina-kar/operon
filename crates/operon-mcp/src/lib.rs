//! Operon's MCP server (plan M1.6 Tasks 7–8; design §15 §10.1, W0): the
//! Model Context Protocol tools over the collection service, served
//! statelessly over streamable HTTP by `rmcp`.
//!
//! This crate opens no socket: the `operon` binary serves [`service`] on
//! its own listener (Ruling 19). It reaches data only through
//! `CollectionService` (overview §8).

use std::sync::Arc;

use operon_query::CollectionService;
use rmcp::transport::streamable_http_server::session::never::NeverSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use tokio_util::sync::CancellationToken;

pub mod config;
pub mod error;
pub mod ids;
pub mod output;
pub mod server;
pub mod tools;

pub use config::{McpConfig, McpConfigError};
pub use error::ToolError;
pub use ids::DocId;
pub use server::{ALL_VERSIONS, INSTRUCTIONS, OperonMcp, STRICT_VERSIONS};

/// The MCP endpoint as a tower service (rule 1): stateless (no session, ever;
/// Ruling 7), JSON answers, `Host` and `Origin` checked, stopped by
/// `shutdown`. Mount it at `config.path`.
pub fn service(
    collections: Arc<CollectionService>,
    config: McpConfig,
    shutdown: CancellationToken,
) -> Result<StreamableHttpService<OperonMcp, NeverSessionManager>, McpConfigError> {
    config.validate()?;
    let http = StreamableHttpServerConfig::default()
        .with_legacy_session_mode(false)
        .with_json_response(true)
        .with_sse_keep_alive(None)
        .with_allowed_hosts(config.allowed_hosts.clone())
        .with_allowed_origins(config.allowed_origins.clone())
        .enforce_origin_validation()
        .with_stateless_protocol_metadata_required(config.strict_stateless)
        .with_cancellation_token(shutdown);
    // Built once; the factory runs per request.
    let prototype = OperonMcp::new(collections, Arc::new(config));
    Ok(StreamableHttpService::new(
        move || Ok(prototype.clone()),
        Arc::new(NeverSessionManager::default()),
        http,
    ))
}
