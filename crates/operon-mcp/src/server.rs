//! The `rmcp` server handler (plan M1.6 Task 7 rules 3–5).

use std::borrow::Cow;
use std::sync::Arc;

use operon_query::CollectionService;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::model::{
    CacheScope, DiscoverResult, Implementation, ListToolsResult, PaginatedRequestParams,
    ProtocolVersion, ResultType, ServerCapabilities, ServerConfig,
};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, ServerHandler, tool_handler};

use crate::config::McpConfig;

/// Every version served when not strict, newest first (Ruling 7).
pub const ALL_VERSIONS: &[ProtocolVersion] = &[
    ProtocolVersion::V_2026_07_28,
    ProtocolVersion::V_2025_11_25,
    ProtocolVersion::V_2025_06_18,
    ProtocolVersion::V_2025_03_26,
];

/// The only version served with `strict_stateless`.
pub const STRICT_VERSIONS: &[ProtocolVersion] = &[ProtocolVersion::V_2026_07_28];

/// The server's `instructions` (rule 5).
pub const INSTRUCTIONS: &str = "Operon database tools. list_collections shows what exists; search runs hybrid search (BM25 full text and/or a vector you supply; Operon computes no embeddings); get_documents fetches by id; sql runs read-only DataFusion SQL where collections are tables; memory_write stores a memory in the memories collection.";

/// The MCP tools over one collection service. Cheap to clone: `rmcp`
/// builds one handler per request from a prototype.
#[derive(Clone)]
pub struct OperonMcp {
    pub(crate) collections: Arc<CollectionService>,
    pub(crate) config: Arc<McpConfig>,
    tool_router: ToolRouter<OperonMcp>,
}

impl std::fmt::Debug for OperonMcp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OperonMcp")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl OperonMcp {
    pub fn new(collections: Arc<CollectionService>, config: Arc<McpConfig>) -> Self {
        Self {
            collections,
            config,
            tool_router: Self::tool_router(),
        }
    }

    /// `tools_ttl` in milliseconds.
    fn ttl_ms(&self) -> u64 {
        u64::try_from(self.config.tools_ttl.as_millis()).unwrap_or(u64::MAX)
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for OperonMcp {
    fn get_info(&self) -> ServerConfig {
        let mut info = ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("operon", env!("CARGO_PKG_VERSION")))
            .with_instructions(INSTRUCTIONS);
        // rmcp falls back to this version for an `initialize` it cannot
        // honour; a legacy default would let a strict server answer one
        // (row T7-2), so strict mode names 2026-07-28 and `initialize` is
        // refused with -32022.
        if self.config.strict_stateless {
            info.protocol_version = ProtocolVersion::V_2026_07_28;
        }
        info
    }

    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        match self.config.strict_stateless {
            true => Cow::Borrowed(STRICT_VERSIONS),
            false => Cow::Borrowed(ALL_VERSIONS),
        }
    }

    async fn discover(
        &self,
        _context: RequestContext<RoleServer>,
    ) -> Result<DiscoverResult, ErrorData> {
        Ok(DiscoverResult::from_server_info(
            self.supported_protocol_versions().into_owned(),
            self.get_info(),
        )
        .with_ttl_ms(self.ttl_ms())
        .with_cache_scope(CacheScope::Public))
    }

    /// Rule 4, Ruling 12: cache hints only for 2026-07-28 and later.
    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let hints = context
            .protocol_version()
            .is_some_and(|version| version >= ProtocolVersion::V_2026_07_28);
        Ok(ListToolsResult {
            result_type: Some(ResultType::COMPLETE),
            tools: self.tool_router.list_all(),
            meta: None,
            next_cursor: None,
            ttl_ms: hints.then(|| self.ttl_ms()),
            cache_scope: hints.then_some(CacheScope::Public),
        })
    }
}
