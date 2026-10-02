//! `loams-es`: an Elasticsearch-compatible gateway over `CollectionService`
//! (plan M1.5). It serves the Elasticsearch 8 REST API for design §06 §7
//! Phase A as D48 defines it, and never touches storage or the metastore
//! itself (overview §8).
//!
//! - [`http`]: media-type negotiation, `X-Elastic-Product`, `pretty`, the
//!   body limit, query-parameter validation and the per-request namespace
//!   and consistency.
//! - [`error`]: ES's error envelope and the mapping of `ServiceError`.
//! - [`names`]: index-name rules and index-expression resolution (comma
//!   lists, `*`, `_all`, aliases with several members; Ruling 9).
//! - `info`: `GET /`, `/_license`, `/_cluster/health` and the trained-model
//!   routes.
//! - [`mapping`]: ES mappings and settings ⇄ collection schemas.
//! - `admin`: index, mapping and alias administration and `_refresh`.
//! - [`doc`]: ES documents ⇄ collection documents (ids, vectors moved out
//!   of `_source` and back, `binary` values, the partial-document merge).
//! - [`write`](mod@write): the write engine and `_doc`, `_create`, `_update`,
//!   `DELETE`.
//! - [`bulk`] serves `_bulk`.
//! - `read`: `GET`/`HEAD` `_doc` and `_source`, and `_mget`, with
//!   `_source` filtering ([`doc::SourceFilter`]).
//! - [`dsl`]: the Query DSL → the search IR.
//! - [`search`]: search bodies and URL parameters → a [`search::SearchPlan`],
//!   and `_search`, `_count` and `_msearch` with ES scores.
//! - [`ubq`] serves `_update_by_query` over the native `patch_by_filter`,
//!   and [`dbq`] serves `_delete_by_query` over `delete_by_filter` with the
//!   loop both share (D87).
//!
//! # Divergences from Elasticsearch 8.19
//!
//! - An unindexed `text` field and a `binary` field are unindexed keywords
//!   with a fast column (M1.1 keeps a field only if it is indexed or fast;
//!   row T2-2); a field ES neither indexes nor keeps doc values for is fast.
//! - `PUT /{index}/_mapping` cannot change the root `dynamic` (row T3-3).
//! - `_seq_no` is the partition offset of a write's record and `_version`
//!   is `_seq_no + 1`, so versions increase but are not dense (Ruling 4).
//! - A `null` `dense_vector` value stays in `_source` as `null` (row T4-3).
//! - The contents of an `enabled: false` object are mapped dynamically by
//!   the collection service, and refused under `dynamic: strict` (row T4-6).
//! - The error texts of document parsing carry `[1:1]` rather than the
//!   value's line and column, and JSON syntax errors carry serde_json's text
//!   rather than Jackson's (rows T4-5, T11-5).
//! - Texts ES builds from its own internals differ: a negative `boost`
//!   names the query without ES's rendering of it, a query nested past 30
//!   levels is one `illegal_argument_exception` rather than one
//!   `x_content_parse_exception` per level, a `dense_vector` value that is
//!   not an array is a field parse error, and the field-limit error counts
//!   the new fields of the whole document (row T11-5).
//! - `_delete_by_query` and `_update_by_query` count `batches` per index,
//!   so a request over an alias with two members reports at least two
//!   (row T11-5).
//! - A `range` with numeric bounds on a `flattened` path compares numbers
//!   numerically; ES compares flattened values as keywords (row E11,
//!   O-M15-2).
//! - `query_string`'s `lenient` is accepted and not applied, and a
//!   `multi_match` or `query_string` without fields searches the text (and,
//!   for `multi_match`, keyword) fields only, not every field (row T7-5).
//! - A search over several indices fails as a whole when one index fails
//!   (for example a sort on a field one member does not map); ES answers
//!   the other shards' hits with `_shards.failed` (row T9-6).
//! - A `script_score` search with `min_score` counts the matches that pass
//!   it among the top `from + size` only, `gte` when they fill that window
//!   (row T9-5).
//! - A sort key other than `_doc` after `_score` is refused: score ties are
//!   broken by `_id` only; a `_doc` sort value is the `_id` (row T9-9).
//! - `_update_by_query` recognises only `params` assignments and
//!   `remove()` in its script; an assignment creates a missing or
//!   non-object parent as an object where Painless fails, and a request
//!   without a script is refused (row T9a-6). Its `scroll_size` is checked
//!   and not applied: the batches are the collection service's, and a
//!   batch refused for backpressure past the deadline is 429 with what was
//!   written kept (rows T9a-6, T9a-7).

// `EsError` carries ES's extra fields and wrapped cause by value (Task 1
// Produces); errors are the cold path, so its size is accepted.
#![allow(clippy::result_large_err)]

use std::fmt;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::http::{Method, Uri};
use axum::middleware;
use axum::response::Response;
use axum::routing::{get, head, post, put};
use loams_query::CollectionService;
use loams_query::hot::HotLayer;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

mod admin;
pub mod bulk;
pub mod dbq;
pub mod doc;
pub mod dsl;
pub mod error;
pub mod http;
mod info;
pub mod mapping;
pub mod names;
mod read;
pub mod search;
pub mod ubq;
pub mod write;

pub use error::{ErrorContext, EsError};
pub use http::{Params, RequestCtx, ResponseFormat};

/// The Elasticsearch version `GET /` reports (Ruling 11).
pub const ES_VERSION: &str = "8.19.0";
/// The header naming a request's namespace (overview §6.9).
pub const NAMESPACE_HEADER: &str = "loams-namespace";
/// The header carrying a consistency token.
pub const TOKEN_HEADER: &str = "loams-consistency-token";

/// How the gateway listens and bounds its requests.
#[derive(Clone, Debug)]
pub struct EsConfig {
    /// 127.0.0.1:9200 (D111: no auth or TLS in M1).
    pub listen: SocketAddr,
    /// The namespace of a request without `Loams-Namespace` ("default").
    pub namespace: String,
    /// `http.max_content_length`: 100 MiB.
    pub max_body_bytes: usize,
    /// `_msearch` searches in flight per request, and index searches in
    /// flight per multi-index search (8).
    pub msearch_concurrency: usize,
    /// `GET /`'s `name` ("loams").
    pub node_name: String,
    /// `GET /`'s and `_cluster/health`'s `cluster_name` ("loams").
    pub cluster_name: String,
}

impl Default for EsConfig {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 9200)),
            namespace: "default".to_string(),
            max_body_bytes: 104_857_600,
            msearch_concurrency: 8,
            node_name: "loams".to_string(),
            cluster_name: "loams".to_string(),
        }
    }
}

/// The gateway: the collection service it calls, its config and the
/// generator of auto ids (Ruling 5). Cheap to clone.
#[derive(Clone)]
pub struct EsGateway {
    inner: Arc<Inner>,
}

struct Inner {
    service: Arc<CollectionService>,
    config: EsConfig,
    ids: Mutex<ulid::Generator>,
}

impl EsGateway {
    pub fn new(service: Arc<CollectionService>, config: EsConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                service,
                config,
                ids: Mutex::new(ulid::Generator::new()),
            }),
        }
    }

    pub fn config(&self) -> &EsConfig {
        &self.inner.config
    }

    /// The collection service every operation calls.
    pub fn service(&self) -> &Arc<CollectionService> {
        &self.inner.service
    }

    /// A new auto `_id`: a ULID from the gateway's one generator, so ids
    /// increase within the process (Ruling 5).
    pub fn next_id(&self) -> String {
        let mut ids = self
            .inner
            .ids
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ids.generate()
            .unwrap_or_else(|overflow| overflow.commit_overflow_increment())
            .to_string()
    }

    /// Every route, inside the gateway's layers: `X-Elastic-Product` on
    /// every answer, the `Loams-Hot` check and `HotLayer`, negotiation and
    /// the body limit.
    pub fn router(&self) -> Router {
        let hot = HotLayer::new(self.service().config().hot_default);
        let router = Router::new()
            .route("/", get(info::root))
            .route("/_license", get(info::license))
            .route("/_cluster/health", get(info::health))
            .route("/_cluster/health/{index}", get(info::health_index))
            .route("/_ml/trained_models/{id}/_infer", post(info::infer))
            .route(
                "/_ml/trained_models/{id}/deployment/_infer",
                post(info::infer),
            )
            // Task 3: index administration.
            .route(
                "/{index}",
                get(admin::get_index)
                    .merge(head(admin::head_index))
                    .put(admin::create_index)
                    .delete(admin::delete_index),
            )
            .route("/_mapping", get(admin::get_mapping_all))
            .route(
                "/{index}/_mapping",
                get(admin::get_mapping)
                    .put(admin::put_mapping)
                    .post(admin::put_mapping),
            )
            .route(
                "/_aliases",
                get(admin::get_aliases_all).post(admin::update_aliases),
            )
            .route("/_alias", get(admin::get_aliases_all))
            .route("/_alias/{name}", get(admin::get_aliases_named))
            .route("/{index}/_alias", get(admin::get_aliases_of_index))
            .route(
                "/{index}/_alias/{name}",
                get(admin::get_aliases_of_index_named)
                    .put(admin::put_alias)
                    .post(admin::put_alias)
                    .delete(admin::delete_alias),
            )
            .route(
                "/{index}/_aliases/{name}",
                put(admin::put_alias)
                    .post(admin::put_alias)
                    .delete(admin::delete_alias),
            )
            .route(
                "/_refresh",
                get(admin::refresh_all).post(admin::refresh_all),
            )
            .route(
                "/{index}/_refresh",
                get(admin::refresh_index).post(admin::refresh_index),
            )
            // Task 4: document writes.
            .route("/{index}/_doc", post(write::index_auto_id))
            .route(
                "/{index}/_doc/{id}",
                get(read::get_doc)
                    .put(write::index_doc)
                    .post(write::index_doc)
                    .delete(write::delete_doc),
            )
            .route(
                "/{index}/_create/{id}",
                put(write::create_doc).post(write::create_doc),
            )
            .route("/{index}/_update/{id}", post(write::update_doc))
            // Task 6: document reads (a `get` route answers `HEAD` too).
            .route("/{index}/_source/{id}", get(read::get_source))
            .route("/_mget", get(read::mget_all).post(read::mget_all))
            .route(
                "/{index}/_mget",
                get(read::mget_index).post(read::mget_index),
            )
            // Task 9: searches.
            .route(
                "/_search",
                get(search::exec::search_all).post(search::exec::search_all),
            )
            .route(
                "/{index}/_search",
                get(search::exec::search_index).post(search::exec::search_index),
            )
            .route(
                "/_count",
                get(search::exec::count_all).post(search::exec::count_all),
            )
            .route(
                "/{index}/_count",
                get(search::exec::count_index).post(search::exec::count_index),
            )
            .route(
                "/_msearch",
                get(search::exec::msearch_all).post(search::exec::msearch_all),
            )
            .route(
                "/{index}/_msearch",
                get(search::exec::msearch_index).post(search::exec::msearch_index),
            )
            // Task 9a: _update_by_query.
            .route(
                "/{index}/_update_by_query",
                post(ubq::update_by_query_index),
            )
            // Task 10: _delete_by_query.
            .route(
                "/{index}/_delete_by_query",
                post(dbq::delete_by_query_index),
            )
            // Task 5: _bulk.
            .route("/_bulk", post(bulk::bulk).put(bulk::bulk))
            .route(
                "/{index}/_bulk",
                post(bulk::bulk_index).put(bulk::bulk_index),
            );
        let routes = router.fallback(no_route).with_state(self.clone());
        // The layers wrap the whole router, not each route (as
        // `Router::layer` would), so the 405 rewrite sees the `Allow`
        // header axum sets after a route's method fallback.
        let service = tower::ServiceBuilder::new()
            .layer(middleware::from_fn(http::product_header))
            .layer(middleware::from_fn(http::check_hot_header))
            .layer(hot)
            .layer(middleware::from_fn_with_state(
                self.config().max_body_bytes,
                http::prepare,
            ))
            .layer(middleware::from_fn(http::method_not_allowed))
            .service(routes);
        Router::new().fallback_service(service)
    }

    /// Serves [`EsGateway::router`] on `listener` until `shutdown` is
    /// cancelled.
    pub fn serve(self, listener: tokio::net::TcpListener, shutdown: CancellationToken) -> EsHandle {
        let addr = listener.local_addr().unwrap_or(self.config().listen);
        let router = self.router();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
                .map_err(EsServerError::Io)
        });
        EsHandle { addr, task }
    }
}

/// 400 for a path no route knows (rule 7), in ES's shape: a string
/// `error` and no `status`.
async fn no_route(ctx: RequestCtx, method: Method, uri: Uri) -> Response {
    let body = serde_json::json!({
        "error": format!("no handler found for uri [{uri}] and method [{method}]")
    });
    http::respond(&ctx, 400, &body)
}

/// The running gateway listener.
#[derive(Debug)]
pub struct EsHandle {
    pub addr: SocketAddr,
    task: JoinHandle<Result<(), EsServerError>>,
}

impl EsHandle {
    /// Waits for the server (it stops once the shutdown token is
    /// cancelled).
    pub async fn join(self) -> Result<(), EsServerError> {
        self.task
            .await
            .unwrap_or_else(|err| Err(EsServerError::Task(err.to_string())))
    }

    /// [`EsHandle::join`], for at most `grace`; then the server is aborted
    /// (with the requests still in flight).
    pub async fn stop_within(self, grace: Duration) -> Result<(), EsServerError> {
        let abort = self.task.abort_handle();
        match tokio::time::timeout(grace, self.join()).await {
            Ok(joined) => joined,
            Err(_) => {
                tracing::warn!("in-flight Elasticsearch requests did not finish; aborting them");
                abort.abort();
                Ok(())
            }
        }
    }
}

/// Why the gateway listener stopped with an error.
#[derive(Debug, thiserror::Error)]
pub enum EsServerError {
    #[error("elasticsearch server failed: {0}")]
    Io(std::io::Error),
    /// The server task panicked or was aborted.
    #[error("elasticsearch server task failed: {0}")]
    Task(String),
}

impl fmt::Debug for EsGateway {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EsGateway")
            .field("config", &self.inner.config)
            .finish_non_exhaustive()
    }
}
