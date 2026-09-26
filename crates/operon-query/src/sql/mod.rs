//! SQL over collections (plan M1.2 Task 10): a DataFusion catalog per
//! namespace ([`NamespaceCatalog`] → `collections` → [`CollectionProvider`]),
//! the search table functions with Spice's names and argument order
//! (Ruling 23, D56), read-only statement execution and the JSON form of
//! results.

mod catalog;
mod exprs;
mod json_rows;
mod provider;
mod udtf;

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::context::SQLOptions;
use datafusion::logical_expr::ScalarUDF;
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};
use futures::StreamExt;
use operon_common::NamespaceId;
use operon_common::meta::Collection;

pub use catalog::{CollectionsSchema, NamespaceCatalog};
pub use exprs::expr_to_query;
pub use json_rows::rows_to_json;
pub use provider::{CollectionProvider, CollectionScanExec, collection_arrow_schema};
pub use udtf::{
    HybridSearchFunction, RerankFunction, RetrieverDescriptorUdf, RrfFunction, TextSearchFunction,
    VectorSearchFunction,
};

use crate::error::ServiceError;
use crate::hot::RequestHot;
use crate::ir::ReadConsistency;
use crate::read::ReadView;
use crate::service::{CollectionService, SqlConfig};

/// The one schema of every namespace catalog.
pub const COLLECTIONS_SCHEMA: &str = "collections";

/// The search table functions (Ruling 23, D56).
pub const SQL_SEARCH_FUNCTIONS: [&str; 5] = [
    "vector_search",
    "text_search",
    "hybrid_search",
    "rrf",
    "rerank",
];

/// A search table function's default `k`: Spice's `text_search` default.
pub const SQL_DEFAULT_K: usize = 1_000;

/// DDL, DML and statements disallowed (Global Constraints).
pub fn read_only_options() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

/// The rows of one statement, at most `SqlConfig.max_rows`.
#[derive(Clone, Debug)]
pub struct SqlResult {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    /// More rows existed than were returned.
    pub truncated: bool,
}

/// What every table and table function of one context reads with: the
/// service, the namespace, the context's consistency and the hot scope
/// captured when the context was made.
#[derive(Clone)]
pub(crate) struct SqlScope {
    pub(crate) service: Arc<CollectionService>,
    pub(crate) ns: String,
    pub(crate) consistency: ReadConsistency,
    pub(crate) hot: RequestHot,
}

impl fmt::Debug for SqlScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlScope")
            .field("ns", &self.ns)
            .field("consistency", &self.consistency)
            .field("hot", &self.hot.enabled)
            .finish_non_exhaustive()
    }
}

impl SqlScope {
    /// The view of `collection`, resolved now.
    pub(crate) async fn view(
        &self,
        ns_id: NamespaceId,
        collection: &Collection,
    ) -> Result<ReadView, ServiceError> {
        self.service
            .view_for(ns_id, collection, &self.consistency, &self.hot)
            .await
    }
}

/// Rule 1: the read-only context of namespace `ns`.
pub(crate) fn context(
    service: Arc<CollectionService>,
    ns: &str,
    consistency: ReadConsistency,
) -> SessionContext {
    let hot = service.request_hot();
    let scope = SqlScope {
        service,
        ns: ns.to_string(),
        consistency,
        hot,
    };
    let ctx = SessionContext::new_with_config(
        SessionConfig::new()
            .with_default_catalog_and_schema(ns, COLLECTIONS_SCHEMA)
            .with_information_schema(true),
    );
    ctx.register_catalog(ns, Arc::new(NamespaceCatalog::new(scope.clone())));
    ctx.register_udtf(
        "vector_search",
        Arc::new(VectorSearchFunction::new(scope.clone())),
    );
    ctx.register_udtf(
        "text_search",
        Arc::new(TextSearchFunction::new(scope.clone())),
    );
    ctx.register_udtf(
        "hybrid_search",
        Arc::new(HybridSearchFunction::new(scope.clone())),
    );
    ctx.register_udtf("rrf", Arc::new(RrfFunction::new(scope.clone())));
    ctx.register_udtf("rerank", Arc::new(RerankFunction));
    for name in udtf::DESCRIPTOR_FUNCTIONS {
        ctx.register_udf(ScalarUDF::new_from_impl(RetrieverDescriptorUdf::new(name)));
    }
    ctx
}

/// The service error an error carries, if it carries one.
fn carried(err: &DataFusionError) -> Option<ServiceError> {
    if let DataFusionError::External(inner) = err.find_root() {
        return inner.downcast_ref::<ServiceError>().cloned();
    }
    None
}

/// A planning error: the service error it carries, else `InvalidArgument`.
fn planning(err: DataFusionError) -> ServiceError {
    carried(&err).unwrap_or_else(|| ServiceError::InvalidArgument(format!("sql: {err}")))
}

/// An execution error: the service error it carries; DataFusion's own
/// errors of the statement (arithmetic, casts, plans) are the caller's.
pub(crate) fn execution(err: DataFusionError) -> ServiceError {
    if let Some(service) = carried(&err) {
        return service;
    }
    match err.find_root() {
        DataFusionError::ResourcesExhausted(_) | DataFusionError::Internal(_) => {
            ServiceError::Internal(format!("sql: {err}"))
        }
        _ => ServiceError::InvalidArgument(format!("sql: {err}")),
    }
}

/// Rule 6: plans `sql` read-only in `ctx` and collects at most
/// `config.max_rows` rows within `config.timeout`.
pub async fn run_read_only(
    ctx: &SessionContext,
    sql: &str,
    config: &SqlConfig,
) -> Result<SqlResult, ServiceError> {
    tokio::time::timeout(config.timeout, run(ctx, sql, config.max_rows))
        .await
        .map_err(|_| ServiceError::Timeout)?
}

/// Plans `sql` read-only in `ctx` (rules 1 and 6) without running it.
///
/// The search table functions plan synchronously against the catalog cache,
/// so the namespaces of the context are refreshed first: a collection
/// created just before the statement is visible to it. Every served surface
/// (REST, Flight SQL) plans through here.
pub async fn plan_read_only(ctx: &SessionContext, sql: &str) -> Result<DataFrame, ServiceError> {
    for name in ctx.catalog_names() {
        if let Some(catalog) = ctx.catalog(&name)
            && let Some(catalog) = catalog.downcast_ref::<NamespaceCatalog>()
        {
            catalog.refresh().await?;
        }
    }
    // `sql_with_options`, split so a refused plan is told apart.
    let plan = ctx
        .state()
        .create_logical_plan(sql)
        .await
        .map_err(planning)?;
    read_only_options().verify_plan(&plan).map_err(|err| {
        ServiceError::InvalidArgument(format!("only read-only queries are allowed: {err}"))
    })?;
    ctx.execute_logical_plan(plan).await.map_err(planning)
}

/// Starts executing a frame [`plan_read_only`] planned, as a stream.
pub async fn execute_read_only(
    frame: DataFrame,
) -> Result<SendableRecordBatchStream, ServiceError> {
    frame.execute_stream().await.map_err(planning)
}

async fn run(ctx: &SessionContext, sql: &str, max_rows: usize) -> Result<SqlResult, ServiceError> {
    let frame = plan_read_only(ctx, sql).await?;
    let mut stream = execute_read_only(frame).await?;
    let schema = stream.schema();
    let mut batches = Vec::new();
    let mut rows = 0;
    let mut truncated = false;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(execution)?;
        if batch.num_rows() == 0 {
            continue;
        }
        if rows == max_rows {
            truncated = true;
            break;
        }
        let take = batch.num_rows().min(max_rows - rows);
        if take < batch.num_rows() {
            truncated = true;
        }
        rows += take;
        batches.push(batch.slice(0, take));
        if truncated {
            break;
        }
    }
    Ok(SqlResult {
        schema,
        batches,
        truncated,
    })
}
