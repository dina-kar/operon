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
//!
//! # Divergences from Qdrant 1.19
//!
//! Every behaviour that deliberately differs from Qdrant is listed here,
//! with the ruling that made it.
//!
//! - `ServiceError::Timeout` carries no duration, so its message is
//!   `Timeout: request timed out` (row T1-1).

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use operon_query::CollectionService;

pub mod convert;
pub mod ctx;
pub mod error;
pub mod ids;
pub mod model;

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
