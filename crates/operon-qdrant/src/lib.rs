//! `operon-qdrant`: a Qdrant-compatible gateway over `CollectionService`
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
//!
//! # Divergences from Qdrant 1.19
//!
//! Every behaviour that deliberately differs from Qdrant is listed here,
//! with the ruling that made it.
//!
//! - `ServiceError::Timeout` carries no duration, so its message is
//!   `Timeout: request timed out` (row T1-1).
//! - Snapshots are manifest versions: create names the newest retained
//!   version and writes nothing, and download, recover and delete are
//!   unsupported (Rulings 15, 19).
//! - Shard, replica, WAL, optimizer and strict-mode settings are accepted,
//!   stored and echoed, but change nothing; `PATCH /collections/{c}` answers
//!   `true` for them and changes nothing (Ruling 16). Cluster info is
//!   synthetic: one active local shard (Task 3).

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use operon_query::CollectionService;
use operon_query::flight::{ACCEPT_ERROR_PAUSE, pace_accept_errors};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub mod convert;
pub mod ctx;
pub mod error;
mod grpc;
pub mod ids;
pub mod model;
mod reads;
mod rest;
pub mod schema;
pub mod snapshots;

pub use ctx::RequestCtx;
pub use error::GatewayError;
pub use ids::PointId;

/// The Qdrant version the gateway implements and reports by default.
pub const QDRANT_COMPAT_VERSION: &str = "1.19.1";
/// `GET /`'s `title`, which LangChain checks to find a local server.
pub const QDRANT_TITLE: &str = "qdrant - vector search engine";
/// The HTTP header and gRPC metadata key naming the request's namespace.
pub const NAMESPACE_HEADER: &str = "operon-namespace";
/// The HTTP header and gRPC metadata key of a consistency token.
pub const TOKEN_HEADER: &str = "operon-consistency-token";
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
    /// Ops per `write` call of a write by filter (Ruling 13).
    pub filter_write_chunk: usize,
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
    pub fn new(service: Arc<CollectionService>, config: QdrantConfig) -> Self {
        Self {
            inner: Arc::new(Inner { service, config }),
        }
    }

    pub fn config(&self) -> &QdrantConfig {
        &self.inner.config
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
