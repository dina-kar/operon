//! `loams-qdrant`: a Qdrant-compatible gateway over `CollectionService`
//! (plan M1.4). It serves Qdrant 1.19.1's REST API and its gRPC API for the
//! Phase A surface of design §06 §8.
//!
//! - [`proto`]: the stubs generated from the nine vendored Qdrant protos
//!   (Ruling 2).
//! - [`model`]: the hand-written serde model of Qdrant's REST types
//!   (Ruling 3); [`convert`] turns protobuf messages into it and back.
//! - [`ids`]: point ids (Ruling 18); [`error`]: Qdrant's status codes and
//!   messages; [`ctx`]: the per-request namespace, consistency and timeout.
//! - [`QdrantGateway::serve`]: the REST listener (axum, Qdrant's envelope)
//!   and the gRPC listener (tonic, gzip), both inside `HotLayer` (Task 2).
//! - [`filter`]: Qdrant filters compiled to the IR over the catch-all
//!   `payload` field (Rulings 5, 6); [`jsonpath`]: Qdrant's key paths and
//!   payload selectors (Ruling 12); [`schema`]: collections and payload
//!   indexes.
//! - Point writes (Task 5) plan each Qdrant operation into `DocOp`s and
//!   write a request with one atomic `write` call; [`scoring`] checks and
//!   normalizes vectors. Point reads (Task 6) serve retrieve, scroll and
//!   count with Qdrant's payload and vector selectors.
//! - [`query`]: the universal query (Task 7) compiled to one IR search
//!   (nearest, sparse nearest, prefetch, RRF and DBSF fusion, rescore), with
//!   Qdrant's scores, thresholds and pages applied by the gateway. Task 8
//!   scores `recommend` (`best_score`, `sum_scores`), `discover`, `context`
//!   and MMR in the gateway over IR candidates (Ruling 10), and serves the
//!   legacy `search`, `recommend` and `discover` routes and methods (and
//!   their batches) by converting them into universal queries (Ruling 1).
//! - [`groups`]: `query/groups` and the legacy `search/groups` and
//!   `recommend/groups` (Task 9), with Qdrant's collect-then-fill driver
//!   over the compiled query and `with_lookup` (Ruling 11).
//!
//! # Divergences from Qdrant 1.19
//!
//! Every behaviour that deliberately differs from Qdrant is listed here,
//! with the ruling that made it.
//!
//! - `ServiceError::Timeout` carries no duration, so its message is
//!   `Timeout: request timed out` (row T1-1).
//! - Snapshots are manifest versions: create names the newest retained
//!   version and writes nothing, list shows every retained version (not
//!   only those created through the API), and download, recover and delete
//!   are unsupported (Rulings 15, 19). Before its first commit a collection
//!   is version 0, the empty collection; right after its first write,
//!   create waits up to 30 s for the first manifest (row T10-3). A snapshot
//!   taken just after a later write may name a version without it.
//! - `wait` never delays a write past its durable append, which already
//!   makes it visible to reads; it only turns the status `acknowledged`
//!   into `completed`. `ordering` and read `consistency` are accepted and
//!   ignored: reads are strong (Ruling 14).
//! - `ScoredPoint.version` is always 0, `segments_count` is 1 and
//!   `indexed_vectors_count` equals `points_count` (Ruling 20);
//!   `count` with `exact: false` is exact.
//! - Shard, replica, WAL, optimizer and strict-mode settings are accepted,
//!   stored and echoed, but change nothing; `PATCH /collections/{c}` answers
//!   `true` for them and changes nothing (Ruling 16). Cluster info is
//!   synthetic: one active local shard (Task 3).
//! - Filters see a key's values after Loams's array flattening: a plain
//!   `a.b` also reaches `{"a": [{"b": …}]}`, and nested arrays are
//!   flattened ("Filter semantics"). A key whose values hold no non-null
//!   leaf (`{}`, `{"d": null}`) counts as missing for `is_empty` and
//!   `except`, and `except` on an array is true only when no element is in
//!   the list, where Qdrant needs one element outside it (row T4-8).
//! - An integer condition (`match.value`, `match.any`, `match.except`)
//!   matches an integral float: `1` matches `1.0` (E8).
//! - `match.text`, `text_any` and `phrase` use the `standard` analyzer
//!   (UAX #29 words, lowercased) over every string at the key, indexed or
//!   not; `text_any` matches whole tokens, not substrings, and `text` may
//!   find its tokens in different values of an array (Ruling 6, row T4-8).
//!   A `text` index with any other tokenizer option is unsupported.
//! - A datetime `range` compares the strings M1.1 reads as dates (RFC 3339,
//!   `YYYY-MM-DD'T'HH:MM:SS[.SSS]`, `YYYY-MM-DD`, `YYYY/MM/DD[ HH:MM:SS]`), at
//!   millisecond precision; other Qdrant formats (`2023-02-08 10:49:00`)
//!   never match, though they are accepted as bounds (Ruling 6).
//! - A `FieldCondition` with several sub-conditions matches when any
//!   matches (Qdrant 1.19 evaluates only `values_count`, `is_empty` or
//!   `is_null` when one is set; row T4-8). A numeric `range` without
//!   bounds matches any value (the IR's `Exists`).
//! - Writes by filter (`delete`, the payload operations and
//!   `delete_vectors` with `filter`) are not atomic: they run as the
//!   collection service's `delete_by_filter`/`patch_by_filter` (D87), which
//!   evaluate the filter at one pin, so points inserted meanwhile are not
//!   touched, and write atomic batches. A batch refused for backpressure is
//!   retried until the request's `timeout` (60 s without one); then the
//!   answer is 429 and the batches already written stay (M1.5 row T9a-9).
//!   Inside a batch request, the operations before a write by filter are
//!   written before it runs, and those after it once it is done.
//! - `set_payload` with a `key` that is not a plain dotted path or with an
//!   object or `null` value, `overwrite_payload` with `key`, and
//!   `delete_payload` of paths with `[]` or `[n]`, read the point and write
//!   its whole new payload, so a concurrent write to the same point in
//!   between is lost (Ruling 12); by filter, their ids are read with
//!   `scroll` and written in chunks of `filter_write_chunk` ops (Ruling 13,
//!   M1.4 row E4's exceptions). Inside a batch, these reads see the points
//!   as they were before the batch, where Qdrant applies the operations one
//!   after another (row T5-4, owner ruling O2).
//! - A scroll `limit` over the search window (100,000) is refused, where
//!   Qdrant reads that many points (row T7-11).
//! - A write request holds at most 10,000 operations after planning, not
//!   counting those a filter resolved (the collection service's limit, one
//!   atomic write); a larger one is 400, asking the client to split the
//!   batch, even next to a filter operation. Qdrant has no such limit (row
//!   T5-12, owner ruling O1, row T8-11).
//! - Geo conditions and indexes, `nested`, `has_vector` and `slice`
//!   conditions, keys with `[n]` or quoted keys holding `.`, payload-index
//!   deletion and type changes are unsupported (Ruling 15).
//! - DBSF over Euclid or Manhattan prefetches normalizes Loams's
//!   larger-is-better scores (negated distances), where Qdrant normalizes
//!   the raw distances and so favours far points (Ruling 9).
//! - `recommend` with `best_score` or `sum_scores`, `discover` and
//!   `context` score the union of one candidate search per example (the
//!   positives, or the target and each pair's positive), each of
//!   `min(max(4 × (offset + limit), 100), max_candidates)` points, where
//!   Qdrant scores during its HNSW walk; a point outside every neighbourhood
//!   is missed (Ruling 10). With negatives only (`best_score` or
//!   `sum_scores`, accepted as Qdrant does, row T8-7), the best points lie
//!   far from the negatives, so the searches go away from them: on Cosine
//!   and Dot the nearest points to each negated negative and to their
//!   negated sum, which are exactly the least similar ones; on Euclid and
//!   Manhattan one exact scan by dot product with their negated sum (the
//!   negated negative when there is one, or when the sum is zero), which
//!   can miss a far point of small norm (owner ruling O-M15-1; one scan per
//!   query, review of #60).
//!   An empty `context` scores `candidate_k` points of the filter 0 each, in
//!   id order, where Qdrant returns the first points of its walk.
//! - Refusal texts for example sets differ from Qdrant's: `No positive
//!   examples given` for `average_vector` without a positive and for a
//!   `recommend` without any example (row T8-7).
//! - MMR's `candidates_limit` is capped at `max_candidates` (10,000), where
//!   Qdrant refuses one over 16,384 (row T8-8).
//! - Groups (Ruling 11, rows T9-2 and T9-3): a collect request leaves out
//!   every point holding a key of a full group, where Qdrant's `except`
//!   keeps a point with one key outside them; a fill request takes the
//!   unsatisfied groups' integer or string keys, where Qdrant requires a
//!   match in both lists at once, which differs only for groups of mixed
//!   key types (kept by the owner ruling on row T9-2); groups whose best
//!   hits tie are ordered by key (integers first); an integer key above
//!   `i64::MAX` voids its point; MMR's default `candidates_limit` under
//!   groups is `limit × group_size`, where Qdrant takes `limit`.
//! - Sparse IDF statistics count live points only, where Qdrant's server
//!   also counts deleted points until its optimizer runs; every sparse
//!   search is exact (`full_scan_threshold` is accepted and ignored), and a
//!   sparse validation error reads `Wrong input: Sparse vector <name>: …`
//!   rather than Qdrant's validator text (Ruling 21).
//! - A query batch holds at most `max_batch_queries` (1,000) requests and a
//!   list retrieve at most min(`max_point_ids` (10,000), native
//!   `max_get_keys`) ids; a larger one is 400
//!   `Wrong input: <what> holds <n> entries, more than the limit of <max>`.
//!   These Loams limits are independent of Qdrant's optional strict-mode
//!   batch limit and the default 32 MiB REST body cap. Single-point GET
//!   retains its fixed one-id bound independently of the list limit (issue #298).
//! - Filters on a collection that was not created through the Qdrant API
//!   (it lacks the `payload` field) are unsupported (row T4-3).
//! - `GET /collections/aliases` answers 405, not the info of a collection
//!   named `aliases` (row T3-9).
//! - Weighted RRF, a prefetch `score_threshold`, a leaf prefetch without a
//!   query (Qdrant's scroll of `limit` points; row T8-1), `order_by`,
//!   `formula`, `sample` and `relevance_feedback` queries, sparse
//!   `recommend`, `discover`, `context` and MMR, sparse rescoring (a sparse
//!   root query over prefetches), multivectors, a `datatype` other than
//!   `float32`, inference objects, adding a sparse vector after creation,
//!   `update_filter`, `update_mode` other than `upsert`, `facet`,
//!   `search/matrix`, and shard keys are unsupported (Rulings 15, 21).

// The write futures hold the collection service's futures, whose `Send`
// check walks deep SQL types.
#![recursion_limit = "256"]

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use loams_query::CollectionService;
use loams_query::flight::{ACCEPT_ERROR_PAUSE, pace_accept_errors};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub mod convert;
pub mod ctx;
pub mod error;
pub mod filter;
pub mod groups;
mod grpc;
pub mod ids;
pub mod jsonpath;
pub mod model;
pub mod query;
mod reads;
mod rest;
pub mod schema;
pub mod scoring;
pub mod snapshots;
mod writes;

pub use ctx::RequestCtx;
pub use error::GatewayError;
pub use ids::PointId;

/// The Qdrant version the gateway implements and reports by default.
pub const QDRANT_COMPAT_VERSION: &str = "1.19.1";
/// `GET /`'s `title`, which LangChain checks to find a local server.
pub const QDRANT_TITLE: &str = "qdrant - vector search engine";
/// The HTTP header and gRPC metadata key naming the request's namespace.
pub const NAMESPACE_HEADER: &str = "loams-namespace";
/// The HTTP header and gRPC metadata key of a consistency token.
pub const TOKEN_HEADER: &str = "loams-consistency-token";
/// The catch-all JSON field over a point's whole payload (Ruling 5).
pub const PAYLOAD_FIELD: &str = "payload";
/// The prefix of the typed payload-index fields (Ruling 5).
pub const PAYLOAD_INDEX_PREFIX: &str = "payload_index.";
/// The `CollectionSchema.annotations` key of the create request's no-op
/// settings (Ruling 16).
pub const EXT_CREATE: &str = "qdrant.create";

/// How the gateway listens and bounds its requests.
#[derive(Clone, Debug)]
pub struct QdrantConfig {
    /// The REST listener (127.0.0.1:6333).
    pub rest_listen: SocketAddr,
    /// The gRPC listener (127.0.0.1:6334).
    pub grpc_listen: SocketAddr,
    /// The namespace of a request that names none ("default").
    pub namespace: String,
    /// The version `GET /` and `HealthCheck` report
    /// ([`QDRANT_COMPAT_VERSION`]).
    pub reported_version: String,
    /// The largest request body or gRPC message: 32 MiB, Qdrant's
    /// `max_request_size_mb`.
    pub max_request_bytes: usize,
    /// The most candidates a gateway-scored query gathers (Ruling 10).
    pub max_candidates: usize,
    /// Ops per `write` call of a read-modify-write by filter (Ruling 13);
    /// the native writes by filter batch by the collection service's
    /// `filter_write_batch`.
    pub filter_write_chunk: usize,
    /// The most requests one query batch (`/points/query/batch`,
    /// `/points/search/batch`, `/points/recommend/batch`, gRPC
    /// `QueryBatch`) holds: 1,000 (issue #298).
    pub max_batch_queries: usize,
    /// The most ids one retrieve (`POST /points`, gRPC `Get`) names:
    /// 10,000, capped by the native service's `max_get_keys`. Does not
    /// restrict single-point GET (issue #298).
    pub max_point_ids: usize,
}

/// Refuses a client-sized list longer than `max` with a Qdrant `Wrong
/// input` error, before executor output is allocated for it (issue #298).
pub fn check_request_len(what: &str, len: usize, max: usize) -> Result<(), GatewayError> {
    if len > max {
        return Err(GatewayError::BadRequest(format!(
            "{what} holds {len} entries, more than the limit of {max}"
        )));
    }
    Ok(())
}

impl Default for QdrantConfig {
    fn default() -> Self {
        Self {
            rest_listen: SocketAddr::from(([127, 0, 0, 1], 6333)),
            grpc_listen: SocketAddr::from(([127, 0, 0, 1], 6334)),
            namespace: "default".to_string(),
            reported_version: QDRANT_COMPAT_VERSION.to_string(),
            max_request_bytes: 33_554_432,
            max_candidates: 10_000,
            filter_write_chunk: 1_000,
            max_batch_queries: 1_000,
            max_point_ids: 10_000,
        }
    }
}

/// The gateway: the collection service it calls and its config. Cheap to
/// clone.
#[derive(Clone)]
pub struct QdrantGateway {
    inner: Arc<Inner>,
}

struct Inner {
    service: Arc<CollectionService>,
    config: QdrantConfig,
}

impl QdrantGateway {
    /// A gateway over `service`.
    pub fn new(service: Arc<CollectionService>, config: QdrantConfig) -> Self {
        Self {
            inner: Arc::new(Inner { service, config }),
        }
    }

    /// How the gateway listens and bounds its requests.
    pub fn config(&self) -> &QdrantConfig {
        &self.inner.config
    }

    /// The list-retrieval ceiling shared by REST and gRPC.
    pub(crate) fn retrieve_id_limit(&self) -> usize {
        self.config()
            .max_point_ids
            .min(self.service().config().max_get_keys)
    }

    /// The collection service every operation calls (Global Constraints:
    /// the only thing the gateway calls).
    pub fn service(&self) -> &Arc<CollectionService> {
        &self.inner.service
    }

    /// Every REST route (Qdrant's envelope on every answer, `GET /`
    /// aside): unknown routes are 404, a known route's other methods 405,
    /// and the 1.19 routes no task serves yet 501.
    pub fn rest_router(&self) -> axum::Router {
        rest::router(self.clone())
    }

    /// The `Qdrant`, `Collections`, `Points`, `Snapshots` and `Health`
    /// services, with gzip and messages of up to `max_request_bytes`.
    pub fn grpc_routes(&self) -> tonic::service::Routes {
        grpc::routes(self)
    }

    /// Serves REST on `rest` and gRPC on `grpc` until `shutdown` is
    /// cancelled.
    pub async fn serve(
        self,
        rest: tokio::net::TcpListener,
        grpc: tokio::net::TcpListener,
        shutdown: CancellationToken,
    ) -> QdrantHandle {
        let rest_addr = rest.local_addr().unwrap_or(self.config().rest_listen);
        let grpc_addr = grpc.local_addr().unwrap_or(self.config().grpc_listen);
        let router = self.rest_router();
        let stop = shutdown.clone();
        let rest_task = tokio::spawn(async move {
            axum::serve(rest, router)
                .with_graceful_shutdown(stop.cancelled_owned())
                .await
                .map_err(QdrantError::Rest)
        });
        let routes = self.grpc_routes();
        let grpc_task = tokio::spawn(async move {
            let incoming = pace_accept_errors(
                tonic::transport::server::TcpIncoming::from(grpc),
                ACCEPT_ERROR_PAUSE,
            );
            tonic::transport::Server::builder()
                .add_routes(routes)
                .serve_with_incoming_shutdown(incoming, shutdown.cancelled_owned())
                .await
                .map_err(QdrantError::Grpc)
        });
        QdrantHandle {
            rest_addr,
            grpc_addr,
            rest: rest_task,
            grpc: grpc_task,
        }
    }
}

/// The running gateway listeners.
#[derive(Debug)]
pub struct QdrantHandle {
    pub rest_addr: SocketAddr,
    pub grpc_addr: SocketAddr,
    rest: JoinHandle<Result<(), QdrantError>>,
    grpc: JoinHandle<Result<(), QdrantError>>,
}

impl QdrantHandle {
    /// Waits for both servers (they stop once the shutdown token is
    /// cancelled); the REST error first when both failed.
    pub async fn join(self) -> Result<(), QdrantError> {
        let joined = |r: Result<Result<(), QdrantError>, tokio::task::JoinError>| {
            r.unwrap_or_else(|err| Err(QdrantError::Task(err.to_string())))
        };
        let rest = joined(self.rest.await);
        let grpc = joined(self.grpc.await);
        rest.and(grpc)
    }

    /// [`QdrantHandle::join`], for at most `grace`; then both servers are
    /// aborted (with the requests still in flight).
    pub async fn stop_within(self, grace: Duration) -> Result<(), QdrantError> {
        let aborts = [self.rest.abort_handle(), self.grpc.abort_handle()];
        match tokio::time::timeout(grace, self.join()).await {
            Ok(joined) => joined,
            Err(_) => {
                tracing::warn!("in-flight Qdrant requests did not finish; aborting them");
                aborts.iter().for_each(tokio::task::AbortHandle::abort);
                Ok(())
            }
        }
    }
}

/// Why a gateway listener stopped with an error.
#[derive(Debug, thiserror::Error)]
pub enum QdrantError {
    #[error("qdrant REST server failed: {0}")]
    Rest(std::io::Error),
    #[error("qdrant gRPC server failed: {0}")]
    Grpc(tonic::transport::Error),
    /// A server task panicked or was aborted.
    #[error("qdrant server task failed: {0}")]
    Task(String),
}

impl fmt::Debug for QdrantGateway {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QdrantGateway")
            .field("config", &self.inner.config)
            .finish_non_exhaustive()
    }
}

/// The generated Qdrant stubs (Ruling 2).
#[allow(missing_debug_implementations, clippy::all, clippy::pedantic)]
pub mod proto {
    /// Package `qdrant`: every Qdrant service and message.
    pub mod qdrant {
        tonic::include_proto!("qdrant");
    }
    /// Package `grpc.health.v1`: the standard health service.
    pub mod health {
        tonic::include_proto!("grpc.health.v1");
    }
}
