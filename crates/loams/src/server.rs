//! One Loams process: a single-node metastore, the log, the range cache,
//! a worker running the background tasks, the collection service, the
//! native HTTP API and the Flight SQL listener (design §10 §1); or, with
//! [`ServerConfig::cluster`], one node of `loams cluster` running the
//! components of its roles (plan M1.3 Task 11).

use std::collections::BTreeMap;
use std::net::SocketAddr;
#[cfg(unix)]
use std::os::fd::AsFd;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use loams_cache::{RangeCache, RangeCacheConfig};
use loams_collection::{
    CollectionConfig, CollectionContext, CollectionGcRoots, CollectionTargetFactory,
    CollectionTrimSource, CollectionWriter, IndexBuildSource, LanceCompactionSource, LanceConfig,
    LanceEnv, MaintenanceConfig, ManifestCache, PkGcRoots, SplitMergeSource,
};
use loams_common::meta::MetaStore;
use loams_hnsw::HnswEngine;
use loams_hot::{
    AlwaysLocal, ForwardStats, HotBuildConfig, HotBuildSource, HotTierConfig, HotTierImpl,
    NodeDescriptor, NodeRegistry, PlacementImpl, RegistryConfig, RemoteReadsConfig,
    RemoteReadsImpl, Roles,
};
use loams_link::{CounterTargetFactory, LinkApplySource, LinkConfig, LinkGcRoots, TargetRegistry};
use loams_log::gc::{GcConfig, GcSource};
use loams_log::{
    LogConfig, LogReader, LogWriter, RetentionConfig, RetentionSource, SegmenterConfig,
    SegmenterSource,
};
use loams_meta::rpc::{self as meta_rpc, JoinRequest, LeaveRequest};
use loams_meta::{
    HttpTransport, HttpTransportConfig, MetaClient, MetaClientConfig, MetaConfig, MetaNode, Router,
    SystemClock, Transport,
};
use loams_query::flight::{FlightConfig, PutTasks, serve_flight_sql_tracked};
use loams_query::flight_ingest::StreamProducer;
use loams_query::placement::Placement;
use loams_query::{CollectionService, ServiceConfig};
use loams_store::Store;
use loams_worker::{TaskSource, Worker, WorkerConfig, WorkerHandle};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

use crate::api::internal::NodeInfo;
use crate::api::{self, AppState, ForwardedReads, NativeStreamProducer};
use crate::cluster::{self, ClusterInfo, LateRouter, MembershipSource};
use crate::meta_backend::MetaBackend;

/// The meta node id of a single-process Loams.
const NODE_ID: u64 = 1;
/// How long startup waits for the single meta node to become leader.
const LEADER_WAIT: Duration = Duration::from_secs(30);
/// How long shutdown lets in-flight HTTP requests (such as long-polls) finish.
const HTTP_GRACE: Duration = Duration::from_secs(10);
/// How long a cluster node waits for a leader after joining (rule 3.5).
const CLUSTER_LEADER_WAIT: Duration = Duration::from_secs(30);
/// How long a leaving learner tries to reach the leader at shutdown.
const LEAVE_WAIT: Duration = Duration::from_secs(10);

/// One node of `loams cluster` (plan M1.3 Task 11; Ruling 15).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterConfig {
    pub node_id: u64,
    pub roles: Roles,
    /// Where other nodes reach this one (`host:port`; default `--listen`).
    pub advertise: String,
    /// The `meta` nodes: id → `host:port`.
    pub peers: BTreeMap<u64, String>,
    /// Inherit a pre-bound listener from stdin (test harness only).
    pub listen_stdin: bool,
    /// Empty when unset.
    pub zone: String,
    /// Owners per collection (1).
    pub replication: usize,
    pub registry: RegistryConfig,
    pub transport: HttpTransportConfig,
    /// How long the start-up join may retry (120 s).
    pub join_deadline: Duration,
    /// A learner whose node lease expired this long ago is removed (10 min).
    pub learner_expiry: Duration,
    /// How often the `meta-membership` task runs (60 s).
    pub membership_interval: Duration,
}

impl ClusterConfig {
    /// The defaults for node `node_id` with `roles`, reachable at
    /// `advertise`, in a cluster whose `meta` nodes are `peers`.
    pub fn new(
        node_id: u64,
        roles: Roles,
        advertise: impl Into<String>,
        peers: BTreeMap<u64, String>,
    ) -> Self {
        Self {
            node_id,
            roles,
            advertise: advertise.into(),
            peers,
            listen_stdin: false,
            zone: String::new(),
            replication: 1,
            registry: RegistryConfig::default(),
            transport: HttpTransportConfig::default(),
            join_deadline: Duration::from_secs(120),
            learner_expiry: Duration::from_secs(600),
            membership_interval: Duration::from_secs(60),
        }
    }

    /// Rule 1.
    pub fn validate(&self) -> Result<(), ServerError> {
        let config = |message: String| Err(ServerError::Config(message));
        if self.roles.is_empty() {
            return config("--roles must name at least one role".to_string());
        }
        if self.peers.is_empty() {
            return config("--peers must name at least one meta node".to_string());
        }
        for (id, addr) in &self.peers {
            if !cluster::is_host_port(addr) {
                return config(format!(
                    "--peers: node {id}'s address {addr:?} is not host:port"
                ));
            }
        }
        if !cluster::is_host_port(&self.advertise) {
            return config(format!("--advertise {:?} is not host:port", self.advertise));
        }
        if let Ok(addr) = self.advertise.parse::<SocketAddr>()
            && addr.ip().is_unspecified()
        {
            return config(format!(
                "--advertise {addr} is an unspecified address; set --advertise to an address other nodes can reach"
            ));
        }
        match (self.roles.meta, self.peers.get(&self.node_id)) {
            (true, None) => {
                return config(format!(
                    "node {} has the meta role but is not in --peers",
                    self.node_id
                ));
            }
            (true, Some(addr)) if *addr != self.advertise => {
                return config(format!(
                    "node {}'s --peers address {addr} differs from --advertise {}",
                    self.node_id, self.advertise
                ));
            }
            (false, Some(_)) => {
                return config(format!(
                    "node {} is in --peers but has no meta role",
                    self.node_id
                ));
            }
            _ => {}
        }
        if self.replication == 0 {
            return config("--replication must be at least 1".to_string());
        }
        Ok(())
    }

    /// The roles the node runs: `gateway` adds `log` (the node that receives
    /// a write appends it).
    pub fn effective_roles(&self) -> Roles {
        let mut roles = self.roles;
        if roles.gateway && !roles.log {
            tracing::info!("the gateway role adds the log role");
            roles.log = true;
        }
        roles
    }
}

/// How to run a single-process Loams.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    /// Holds the metastore's local database (`<data_dir>/meta`) and, without
    /// `bucket`, the local bucket (`<data_dir>/bucket`).
    pub data_dir: PathBuf,
    pub listen: SocketAddr,
    /// Object store URL (`s3://…`, `gs://…`, `az://…`, `file:///…`). Default
    /// `file://<data_dir>/bucket`.
    pub bucket: Option<String>,
    pub log: LogConfig,
    pub segmenter: SegmenterConfig,
    pub retention: RetentionConfig,
    pub cache: RangeCacheConfig,
    pub link: LinkConfig,
    pub gc: GcConfig,
    /// How collections commit, index, trim and keep manifests. The server
    /// overrides `max_commit_delay` with `link.max_commit_delay` and
    /// `keep_manifests` with `gc.keep_manifests`.
    pub collection: CollectionConfig,
    pub lance: LanceConfig,
    /// The metastore builds a snapshot after this many log entries. Default
    /// 10 000.
    pub snapshot_every: u64,
    /// How often the worker polls its task sources. Default 1 s.
    pub worker_poll_interval: Duration,
    /// The worker's task lease TTL. Default 30 s.
    pub worker_lease_ttl: Duration,
    /// The collection service: partitions, the hot default, tails, reads,
    /// search and SQL limits.
    pub query: ServiceConfig,
    /// Where Flight SQL listens (with the `flight` feature); `None` (the
    /// default here) serves no Flight SQL. `loams dev` and `standalone`
    /// set it.
    pub flight_sql: Option<SocketAddr>,
    /// Native stream gRPC listener for Dapr protocol adapters.
    #[cfg(feature = "stream-grpc")]
    pub stream_grpc: Option<SocketAddr>,
    /// How Flight SQL bounds its statements and its ingest.
    pub flight: FlightConfig,
    /// CloudEvents ingest: the dedup window (design §02 §7.4).
    pub cloudevents: crate::api::events::EventsConfig,
    /// MySQL wire listener over collections, when the mysql-wire feature is on.
    #[cfg(feature = "mysql-wire")]
    pub mysql_wire: Option<crate::mysql_wire::MysqlConfig>,
    /// PostgreSQL wire listener over collections, when the pgwire feature is on.
    #[cfg(feature = "pgwire")]
    pub pg: Option<crate::pg::PgConfig>,
    /// Split merges and Lance compaction (plan M1.3 Tasks 1–2); a source
    /// whose switch (`merge`, `compaction`) is off is not run.
    pub maintenance: MaintenanceConfig,
    /// This node's hot tier (plan M1.3 Tasks 6–8). `enabled` false (`--hot
    /// off`): no tier, no artifact builds, every read cold.
    pub hot: HotTierConfig,
    /// Hot artifact builds (plan M1.3 Task 5). The server sets `pin_all` to
    /// `hot.pin_all`.
    pub hot_build: HotBuildConfig,
    /// The HNSW engine of artifact builds and the tier; `None` =
    /// `loams_hnsw::default_engine()` (qdrant-edge with the `hnsw` feature;
    /// without it, no artifact is built unless this is set). Tests set
    /// `FlatEngine`.
    pub hnsw_engine: Option<Arc<dyn HnswEngine>>,
    /// `None`: `dev` or `standalone` (one node, every role). `Some`: one
    /// node of `loams cluster` (plan M1.3 Task 11).
    pub cluster: Option<ClusterConfig>,
    /// The Qdrant gateway's listeners and limits (plan M1.4, feature
    /// `qdrant`), served on gateway nodes. `None` (the default here, E12)
    /// serves no Qdrant API; the CLI sets it unless `--no-qdrant`.
    #[cfg(feature = "qdrant")]
    pub qdrant: Option<loams_qdrant::QdrantConfig>,
    /// The embedded durable server (D1, feature `durable`): its loopback
    /// listener and its store. `None` (the default here) serves none; the
    /// CLI sets it unless `--no-durable`. A cluster node needs a MySQL store.
    #[cfg(feature = "durable")]
    pub durable: Option<loams_durable::DurableConfig>,
    /// The metastore (R1 plan Task 6, D124): the embedded openraft store
    /// (the default) or TiKV (`--meta tikv://…`, on `dev` and `standalone`
    /// only; `loams cluster` refuses it).
    pub meta: MetaBackend,
    /// The Elasticsearch gateway's listener and limits (plan M1.5, feature
    /// `es`), served on gateway nodes. `None` (the default here, row E13)
    /// serves no Elasticsearch API; the CLI sets it unless `--no-es`.
    #[cfg(feature = "es")]
    pub es: Option<loams_es::EsConfig>,
    /// Serve `grpc.reflection.v1` on the main port (design §44 §4). Off
    /// (the default here); `loams dev` turns it on, which is Q603's proposed
    /// answer. Reflection publishes the schema of the API to anyone who can
    /// reach the port, so it is a development convenience, not a default.
    pub reflection: bool,
}

impl ServerConfig {
    /// The defaults, with data in `data_dir`, listening on 127.0.0.1:8080.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        let data_dir: PathBuf = data_dir.into();
        Self {
            hot: HotTierConfig::new(&data_dir),
            hot_build: HotBuildConfig::new(&data_dir),
            maintenance: MaintenanceConfig::default(),
            hnsw_engine: None,
            data_dir,
            listen: SocketAddr::from(([127, 0, 0, 1], 8080)),
            bucket: None,
            log: LogConfig::new(NODE_ID),
            segmenter: SegmenterConfig::default(),
            retention: RetentionConfig::default(),
            cache: RangeCacheConfig::default(),
            link: LinkConfig::default(),
            gc: GcConfig::default(),
            collection: CollectionConfig::default(),
            lance: LanceConfig::default(),
            snapshot_every: 10_000,
            worker_poll_interval: Duration::from_secs(1),
            worker_lease_ttl: Duration::from_secs(30),
            query: ServiceConfig::default(),
            flight_sql: None,
            #[cfg(feature = "stream-grpc")]
            stream_grpc: None,
            flight: FlightConfig::default(),
            cloudevents: crate::api::events::EventsConfig::default(),
            #[cfg(feature = "mysql-wire")]
            mysql_wire: None,
            #[cfg(feature = "pgwire")]
            pg: None,
            cluster: None,
            #[cfg(feature = "qdrant")]
            qdrant: None,
            #[cfg(feature = "durable")]
            durable: None,
            meta: MetaBackend::Raft,
            #[cfg(feature = "es")]
            es: None,
            reflection: false,
        }
    }

    /// Rejects an invalid `flight` config ([`FlightConfig::validate`]), then
    /// any freshness deadline at or above `gc.grace`:
    /// `segmenter.swap_deadline`, `link.max_commit_delay` (which is also the
    /// collection commit delay: the server sets `collection.max_commit_delay`
    /// to it) and `collection.index_commit_delay`. Called first thing by
    /// [`Server::start`]; the error names the first violating deadline in
    /// that order.
    ///
    /// Garbage collection deletes unreferenced objects older than its grace
    /// period, so a segment swap, link commit or index build must reference
    /// its new objects strictly within it (M0.4 ruling E7, re-review m1;
    /// plan M1.1 Ruling 22).
    pub fn validate(&self) -> Result<(), ServerError> {
        if let Some(cluster) = &self.cluster {
            cluster.validate()?;
            if self.meta != MetaBackend::Raft {
                return Err(ServerError::Config(
                    "loams cluster runs its own openraft metastore; --meta is for dev and \
                     standalone only"
                        .to_string(),
                ));
            }
        }
        self.validate_durable()?;
        self.flight.validate().map_err(ServerError::Config)?;
        self.validate_backpressure()?;
        self.cloudevents.validate().map_err(ServerError::Config)?;
        self.gc
            .check_deadlines(&[
                ("segmenter.swap_deadline", self.segmenter.swap_deadline),
                ("link.max_commit_delay", self.link.max_commit_delay),
                (
                    "collection.index_commit_delay",
                    self.collection.index_commit_delay,
                ),
                ("maintenance.commit_delay", self.maintenance.commit_delay),
                (
                    "hot_build.artifact_commit_delay",
                    self.hot_build.artifact_commit_delay,
                ),
            ])
            .map_err(|err| ServerError::Config(err.to_string()))
    }
}

impl ServerConfig {
    /// D1 Task 3: SQLite is a single-node store, so a cluster node needs
    /// MySQL (TiDB).
    #[cfg(feature = "durable")]
    fn validate_durable(&self) -> Result<(), ServerError> {
        if self.cluster.is_some()
            && let Some(durable) = &self.durable
            && let loams_durable::DurableStore::Sqlite { .. } = durable.store
        {
            return Err(ServerError::Config(
                "the sqlite durable store is single-node; use --durable-store mysql://… or tikv://…"
                    .to_string(),
            ));
        }
        Ok(())
    }

    #[cfg(not(feature = "durable"))]
    fn validate_durable(&self) -> Result<(), ServerError> {
        Ok(())
    }

    /// Task 15 rule 8: the byte budget at most half the live tail (so a
    /// backlog at the budget fits the tail and strong reads need no range
    /// tail), non-zero budgets, `override_factor >= 1` and
    /// `min_retry_after <= max_retry_after`.
    fn validate_backpressure(&self) -> Result<(), ServerError> {
        let b = &self.query.backpressure;
        let config = |message: String| Err(ServerError::Config(message));
        let half_tail = (self.query.tail.max_bytes / 2) as u64;
        if b.max_unapplied_bytes > half_tail {
            return config(format!(
                "--max-unapplied-bytes {} is over half the tail's bound ({half_tail} of {} bytes)",
                b.max_unapplied_bytes, self.query.tail.max_bytes
            ));
        }
        if b.max_unapplied_records == 0 || b.max_unapplied_bytes == 0 {
            return config("the unapplied-data budgets must be above 0".to_string());
        }
        if b.override_factor < 1 {
            return config("backpressure.override_factor must be at least 1".to_string());
        }
        if b.min_retry_after > b.max_retry_after {
            return config(format!(
                "backpressure.min_retry_after {:?} is over max_retry_after {:?}",
                b.min_retry_after, b.max_retry_after
            ));
        }
        Ok(())
    }
}

/// Why the server could not start.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// The configuration is inconsistent (see [`ServerConfig::validate`]).
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("data directory {path}: {source}")]
    DataDir {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("object store: {0}")]
    Store(#[from] loams_store::StoreError),
    #[error("metastore: {0}")]
    Meta(#[from] loams_meta::MetaError),
    /// The TiKV metastore's GC loop could not start (R1 plan Task 6).
    #[cfg(feature = "tikv")]
    #[error("TiKV: {0}")]
    Tikv(#[from] loams_tikv::TikvError),
    #[error("cache: {0}")]
    Cache(#[from] loams_cache::CacheError),
    #[error("log: {0}")]
    Log(#[from] loams_log::LogError),
    #[error("hot tier: {0}")]
    Hot(#[from] loams_hot::TierError),
    #[error("listen on {addr}: {source}")]
    Listen {
        addr: SocketAddr,
        source: std::io::Error,
    },
    #[cfg(feature = "mysql-wire")]
    #[error("MySQL wire listen on {addr}: {source}")]
    MysqlListen {
        addr: SocketAddr,
        source: std::io::Error,
    },
    #[cfg(feature = "pgwire")]
    #[error("postgres listen on {addr}: {source}")]
    PgListen {
        addr: SocketAddr,
        source: std::io::Error,
    },
    #[cfg(feature = "stream-grpc")]
    #[error("stream gRPC listen on {addr}: {source}")]
    StreamGrpcListen {
        addr: SocketAddr,
        source: std::io::Error,
    },
    /// A Qdrant gateway listener could not be bound (plan M1.4 Task 2).
    #[cfg(feature = "qdrant")]
    #[error("qdrant listen on {addr}: {source}")]
    QdrantListen {
        addr: SocketAddr,
        source: std::io::Error,
    },
    /// The Elasticsearch gateway's listener could not be bound (plan M1.5
    /// Task 1).
    #[cfg(feature = "es")]
    #[error("elasticsearch listen on {addr}: {source}")]
    EsListen {
        addr: SocketAddr,
        source: std::io::Error,
    },
    /// The embedded durable server did not start (D1); its message as is.
    #[cfg(feature = "durable")]
    #[error("{0}")]
    Durable(#[from] loams_durable::DurableError),
}

/// The embedded durable server (D1 Task 3) and Loams's durable runtime on it
/// (Task 6), when the build has the `durable` feature and the config asks
/// for it.
#[cfg(feature = "durable")]
#[derive(Debug)]
struct Durable {
    server: Option<loams_durable::DurableServer>,
    runtime: Option<loams_durable::DurableRuntime>,
}

/// The embedded durable server: none in a build without `durable`.
#[cfg(not(feature = "durable"))]
#[derive(Debug)]
struct Durable;

impl Durable {
    /// No durable server (a placeholder until the caller hands one over).
    #[cfg(feature = "durable")]
    fn none() -> Self {
        Self {
            server: None,
            runtime: None,
        }
    }

    #[cfg(not(feature = "durable"))]
    fn none() -> Self {
        Self
    }

    /// Starts `config.durable`, if any, for node `node_id`. Runs after the
    /// metastore has a leader and before [`Server::assemble`] (row T0-6).
    #[cfg(feature = "durable")]
    async fn start(config: &ServerConfig, node_id: u64) -> Result<Self, ServerError> {
        match &config.durable {
            Some(durable) => Ok(Self {
                server: Some(
                    loams_durable::DurableServer::start(durable.clone(), &node_id.to_string())
                        .await?,
                ),
                runtime: None,
            }),
            None => Ok(Self::none()),
        }
    }

    #[cfg(not(feature = "durable"))]
    async fn start(_config: &ServerConfig, _node_id: u64) -> Result<Self, ServerError> {
        Ok(Self)
    }

    /// Starts Loams's durable runtime on the server, if there is one: after
    /// [`Server::assemble`], before the routes serve (row T0-6, X8). A
    /// failure stops the server too.
    #[cfg(feature = "durable")]
    async fn start_runtime(&mut self, node_id: u64) -> Result<(), ServerError> {
        let Some(server) = &self.server else {
            return Ok(());
        };
        match loams_durable::DurableRuntime::start(server, &node_id.to_string()).await {
            Ok(runtime) => {
                self.runtime = Some(runtime);
                Ok(())
            }
            Err(err) => {
                if let Some(server) = self.server.take() {
                    server.stop().await;
                }
                Err(err.into())
            }
        }
    }

    #[cfg(not(feature = "durable"))]
    #[allow(clippy::unused_async)]
    async fn start_runtime(&mut self, _node_id: u64) -> Result<(), ServerError> {
        Ok(())
    }

    /// Stops the runtime (it takes no more tasks), then drains and stops the
    /// server; its port and store are free afterwards. Each is a shutdown
    /// phase: `durable_runtime`, then `durable`.
    async fn stop(self) {
        shutdown_phase("durable_runtime");
        #[cfg(feature = "durable")]
        if let Some(runtime) = self.runtime {
            runtime.stop().await;
        }
        shutdown_phase("durable");
        #[cfg(feature = "durable")]
        if let Some(server) = self.server {
            server.stop().await;
        }
    }

    #[cfg(feature = "durable")]
    fn addr(&self) -> Option<SocketAddr> {
        self.server.as_ref().map(|server| server.listen())
    }

    #[cfg(not(feature = "durable"))]
    fn addr(&self) -> Option<SocketAddr> {
        None
    }
}

/// Marks one step of [`Server::shutdown`], in order (target
/// `loams::shutdown`, field `phase`), so tests can check the stop order.
fn shutdown_phase(phase: &'static str) {
    tracing::debug!(target: "loams::shutdown", phase, "stopping");
}

/// A running Loams process.
#[derive(Debug)]
pub struct Server {
    local_addr: SocketAddr,
    /// The openraft metastore node; `None` on the TiKV metastore.
    node: Option<MetaNode>,
    /// The openraft metastore client; `None` on the TiKV metastore.
    meta: Option<MetaClient>,
    /// The cluster MVCC GC loop the TiKV metastore runs.
    #[cfg(feature = "tikv")]
    tikv_gc: Option<TikvGc>,
    /// The metastore as the trait object every component holds.
    meta_store: Arc<dyn MetaStore>,
    writer: LogWriter,
    cache: RangeCache,
    collection_context: CollectionContext,
    collection_factory: Arc<CollectionTargetFactory>,
    collections: Arc<CollectionService>,
    /// `None` on a cluster node without the `worker` role.
    worker: Option<WorkerHandle>,
    /// The hot tier, unless `--hot off` (or no `query` role).
    hot: Option<HotTierImpl>,
    http: JoinHandle<()>,
    stop_http: oneshot::Sender<()>,
    flight: Option<Flight>,
    #[cfg(feature = "mysql-wire")]
    mysql_wire: Option<crate::mysql_wire::MysqlHandle>,
    #[cfg(feature = "pgwire")]
    pg: Option<crate::pg::PgHandle>,
    #[cfg(feature = "stream-grpc")]
    stream_grpc: Option<StreamGrpc>,
    #[cfg(feature = "qdrant")]
    qdrant: Option<Qdrant>,
    /// The embedded durable server (D1).
    durable: Durable,
    #[cfg(feature = "es")]
    es: Option<Es>,
    /// Cluster mode only.
    cluster: Option<ClusterRuntime>,
}

/// The running cluster MVCC GC loop of a server on the TiKV metastore (R1
/// plan Task 3).
#[cfg(feature = "tikv")]
#[derive(Debug)]
struct TikvGc {
    handle: loams_tikv::GcHandle,
    stop: CancellationToken,
}

#[cfg(feature = "tikv")]
impl TikvGc {
    /// Starts the GC loop on the metastore's handle. The loop sweeps its own
    /// handle's commit tokens; Loams Live's handles join `sweep` when Live
    /// runs in the process (R1 plan Task 12).
    fn start(meta: &loams_meta_tikv::TikvMeta) -> Result<Self, ServerError> {
        let stop = CancellationToken::new();
        let handle = loams_tikv::GcLoop::spawn(
            meta.tikv().clone(),
            loams_tikv::GcConfig::default(),
            stop.clone(),
        )?;
        Ok(TikvGc { handle, stop })
    }

    async fn stop(self) {
        self.stop.cancel();
        self.handle.stopped().await;
    }
}

/// What a cluster node keeps for its shutdown.
#[derive(Debug)]
struct ClusterRuntime {
    node_id: u64,
    roles: Roles,
    registry: Arc<NodeRegistry>,
    transport: HttpTransport,
    seeds: Vec<String>,
    late: LateRouter,
}

/// How [`Server::assemble`] wires a node: single-node modes pass every role,
/// `AlwaysLocal` and no extras.
struct NodeSetup {
    node_id: u64,
    roles: Roles,
    meta_store: Arc<dyn MetaStore>,
    placement: Arc<dyn Placement>,
    /// The routed service's placement and transport (cluster mode).
    routing: Option<(Arc<PlacementImpl>, Arc<RemoteReadsImpl>)>,
    forward_stats: Arc<ForwardStats>,
    extra_sources: Vec<Arc<dyn TaskSource>>,
    node_info: Option<Arc<dyn NodeInfo>>,
}

/// Everything [`Server::assemble`] started, and the node's router.
struct Assembled {
    writer: LogWriter,
    cache: RangeCache,
    collection_context: CollectionContext,
    collection_factory: Arc<CollectionTargetFactory>,
    collections: Arc<CollectionService>,
    worker: Option<WorkerHandle>,
    hot: Option<HotTierImpl>,
    app: axum::Router,
    flight: Option<Flight>,
    #[cfg(feature = "mysql-wire")]
    mysql_wire: Option<crate::mysql_wire::MysqlHandle>,
    #[cfg(feature = "pgwire")]
    pg: Option<crate::pg::PgHandle>,
    #[cfg(feature = "stream-grpc")]
    stream_grpc: Option<StreamGrpc>,
    #[cfg(feature = "qdrant")]
    qdrant: Option<Qdrant>,
    /// Started by the caller before `assemble` and handed over here (a
    /// cluster node's start returns it with the parts).
    durable: Durable,
    #[cfg(feature = "es")]
    es: Option<Es>,
}

/// The running Qdrant gateway (plan M1.4 Task 2).
#[cfg(feature = "qdrant")]
#[derive(Debug)]
struct Qdrant {
    handle: loams_qdrant::QdrantHandle,
    stop: CancellationToken,
}

/// The running Elasticsearch gateway (plan M1.5 Task 1).
#[cfg(feature = "es")]
#[derive(Debug)]
struct Es {
    handle: loams_es::EsHandle,
    stop: CancellationToken,
}

/// Binds the Qdrant gateway's REST and gRPC listeners.
#[cfg(feature = "qdrant")]
async fn bind_qdrant(
    config: &loams_qdrant::QdrantConfig,
) -> Result<(tokio::net::TcpListener, tokio::net::TcpListener), ServerError> {
    let bind_one = async |addr: SocketAddr| {
        tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|source| ServerError::QdrantListen { addr, source })
    };
    let rest = bind_one(config.rest_listen).await?;
    let grpc = bind_one(config.grpc_listen).await?;
    Ok((rest, grpc))
}

/// The running Flight SQL server.
#[derive(Debug)]
struct Flight {
    addr: SocketAddr,
    task: JoinHandle<()>,
    stop: CancellationToken,
    /// The `DoPut` tasks, which outlive an aborted `task`.
    puts: PutTasks,
}

#[cfg(feature = "stream-grpc")]
#[derive(Debug)]
struct StreamGrpc {
    addr: SocketAddr,
    task: JoinHandle<()>,
    stop: CancellationToken,
}

#[cfg(feature = "stream-grpc")]
impl StreamGrpc {
    fn start(
        listener: tokio::net::TcpListener,
        addr: SocketAddr,
        streams: Arc<dyn StreamProducer>,
        events: Arc<dyn loams_stream_grpc::EventProducer>,
    ) -> Self {
        let stop = CancellationToken::new();
        let task_stop = stop.clone();
        let task = tokio::spawn(async move {
            if let Err(err) = loams_stream_grpc::serve(listener, streams, events, task_stop).await {
                tracing::error!(%err, "native stream gRPC server failed");
            }
        });
        Self { addr, task, stop }
    }

    async fn stop(self) {
        self.stop.cancel();
        let mut task = self.task;
        if tokio::time::timeout(HTTP_GRACE, &mut task).await.is_err() {
            tracing::warn!("in-flight stream gRPC calls did not finish; aborting them");
            task.abort();
        }
    }
}

impl Flight {
    /// Serves Flight SQL over `collections` on `listener` until stopped
    /// (Task 12), with stream ingest through `streams` (Task 13).
    fn start(
        listener: tokio::net::TcpListener,
        addr: SocketAddr,
        collections: Arc<CollectionService>,
        streams: Arc<dyn StreamProducer>,
        config: FlightConfig,
    ) -> Self {
        let stop = CancellationToken::new();
        let puts = PutTasks::new();
        let task = tokio::spawn(serve(
            listener,
            collections,
            streams,
            config,
            stop.clone(),
            puts.clone(),
        ));
        Self {
            addr,
            task,
            stop,
            puts,
        }
    }

    /// Stops accepting calls, waits up to [`HTTP_GRACE`] for the calls in
    /// flight (a `DoGet` may stream for minutes) and aborts the rest, then
    /// waits up to [`HTTP_GRACE`] for the puts, which stop before their next
    /// chunk, and aborts the rest, so none writes after the collection
    /// service and the writer stop.
    async fn stop(self) {
        self.stop.cancel();
        let mut task = self.task;
        if tokio::time::timeout(HTTP_GRACE, &mut task).await.is_err() {
            tracing::warn!("in-flight Flight SQL calls did not finish; aborting them");
            task.abort();
        }
        self.puts.close();
        if tokio::time::timeout(HTTP_GRACE, self.puts.wait())
            .await
            .is_err()
        {
            tracing::warn!("a Flight put did not finish its chunk in flight; aborting it");
            self.puts.abort();
            self.puts.wait().await;
        }
    }
}

async fn serve(
    listener: tokio::net::TcpListener,
    collections: Arc<CollectionService>,
    streams: Arc<dyn StreamProducer>,
    config: FlightConfig,
    stop: CancellationToken,
    puts: PutTasks,
) {
    let served =
        serve_flight_sql_tracked(listener, collections, Some(streams), config, stop, puts).await;
    if let Err(err) = served {
        tracing::error!(%err, "Flight SQL server failed");
    }
}

/// Where Flight SQL listens: `config.flight_sql` with the `flight` feature,
/// never without it.
fn flight_addr(config: &ServerConfig) -> Option<SocketAddr> {
    if cfg!(feature = "flight") {
        config.flight_sql
    } else {
        if config.flight_sql.is_some() {
            tracing::warn!("this build has no Flight SQL (the flight feature is off)");
        }
        None
    }
}

/// The object store URL for a config: its bucket, or a directory in the data
/// directory.
fn bucket_url(config: &ServerConfig) -> Result<String, ServerError> {
    if let Some(bucket) = &config.bucket {
        return Ok(bucket.clone());
    }
    let dir = config.data_dir.join("bucket");
    let data_dir_error = |source| ServerError::DataDir {
        path: dir.clone(),
        source,
    };
    std::fs::create_dir_all(&dir).map_err(data_dir_error)?;
    let dir = dir.canonicalize().map_err(data_dir_error)?;
    url::Url::from_directory_path(&dir)
        .map(String::from)
        .map_err(|()| ServerError::DataDir {
            path: dir,
            source: std::io::Error::other("not an absolute path"),
        })
}

impl Server {
    /// Opens (or creates) the data directory, starts the metastore, the log
    /// and its background loops, and serves the HTTP API; with
    /// `config.cluster`, starts one cluster node instead (plan M1.3 Task 11
    /// rule 3).
    ///
    /// On failure, everything already started is stopped again (the
    /// metastore releases its local database), so a retry in the same process
    /// can succeed.
    pub async fn start(config: ServerConfig) -> Result<Self, ServerError> {
        config.validate()?;
        config.log.validate()?;
        if config.cluster.is_some() {
            return Self::start_cluster(config).await;
        }
        let store = Store::from_url(&bucket_url(&config)?, Vec::<(String, String)>::new())?;
        #[cfg(feature = "tikv")]
        if let MetaBackend::Tikv(tikv) = &config.meta {
            return Self::start_on_tikv(tikv.clone(), store, config).await;
        }
        let mut meta_config = MetaConfig::new(NODE_ID, config.data_dir.join("meta"), store.clone());
        meta_config.snapshot_every = config.snapshot_every;
        let node = MetaNode::start(meta_config, &Router::new()).await?;
        match Self::start_on(node.clone(), store, config).await {
            Ok(server) => Ok(server),
            Err(err) => {
                if let Err(shutdown) = node.shutdown().await {
                    tracing::warn!(%shutdown, "stopping the metastore after a failed start");
                }
                Err(err)
            }
        }
    }

    /// Everything after the single meta node started.
    async fn start_on(
        node: MetaNode,
        store: Store,
        config: ServerConfig,
    ) -> Result<Self, ServerError> {
        // A no-op once the node is initialized, so restarts keep their state.
        node.initialize([NODE_ID]).await?;
        node.wait_for_leader(LEADER_WAIT).await?;
        let meta = MetaClient::new(
            node.clone(),
            Vec::new(),
            Arc::new(SystemClock),
            MetaClientConfig::default(),
        );
        let meta_store: Arc<dyn MetaStore> = meta.clone().into();
        let mut server = Self::serve_single(meta_store, store, config).await?;
        server.node = Some(node);
        server.meta = Some(meta);
        Ok(server)
    }

    /// `dev` or `standalone` on the TiKV metastore (R1 plan Task 6): opens
    /// the metastore, starts the cluster GC loop on its handle, then every
    /// role as on the openraft store. Nothing is kept in `<data_dir>/meta`.
    #[cfg(feature = "tikv")]
    async fn start_on_tikv(
        tikv: loams_meta_tikv::TikvMetaConfig,
        store: Store,
        config: ServerConfig,
    ) -> Result<Self, ServerError> {
        let meta = loams_meta_tikv::TikvMeta::open(tikv).await?;
        let gc = TikvGc::start(&meta)?;
        let meta_store: Arc<dyn MetaStore> = Arc::new(meta);
        match Self::serve_single(meta_store, store, config).await {
            Ok(mut server) => {
                server.tikv_gc = Some(gc);
                Ok(server)
            }
            Err(err) => {
                gc.stop().await;
                Err(err)
            }
        }
    }

    /// Every role of a single-process server on `meta_store`, and the HTTP
    /// API. The caller keeps its metastore handles in the result.
    async fn serve_single(
        meta_store: Arc<dyn MetaStore>,
        store: Store,
        mut config: ServerConfig,
    ) -> Result<Self, ServerError> {
        let (listener, local_addr) = bind(config.listen).await?;
        // After the leader, before the collection service (row T0-6).
        let durable = Durable::start(&config, NODE_ID).await?;
        let setup = NodeSetup {
            node_id: NODE_ID,
            roles: Roles::all(),
            meta_store: meta_store.clone(),
            placement: Arc::new(AlwaysLocal),
            routing: None,
            forward_stats: Arc::new(ForwardStats::default()),
            extra_sources: Vec::new(),
            node_info: None,
        };
        let parts = match Self::assemble(&mut config, store, setup).await {
            Ok(parts) => parts,
            Err(err) => {
                durable.stop().await;
                return Err(err);
            }
        };
        let (stop_http, stopped) = oneshot::channel::<()>();
        // The listener serves once the durable runtime has started (T0-6,
        // X8); a dropped gate means the start failed.
        let (serve_now, serve_gate) = oneshot::channel::<()>();
        let app = parts.app;
        let http = tokio::spawn(async move {
            if serve_gate.await.is_err() {
                return;
            }
            let serve = axum::serve(listener, app).with_graceful_shutdown(async move {
                let _ = stopped.await;
            });
            if let Err(err) = serve.await {
                tracing::error!(%err, "HTTP server failed");
            }
        });
        let mut server = Self {
            local_addr,
            node: None,
            meta: None,
            #[cfg(feature = "tikv")]
            tikv_gc: None,
            meta_store,
            writer: parts.writer,
            cache: parts.cache,
            collection_context: parts.collection_context,
            collection_factory: parts.collection_factory,
            collections: parts.collections,
            worker: parts.worker,
            hot: parts.hot,
            http,
            stop_http,
            flight: parts.flight,
            #[cfg(feature = "mysql-wire")]
            mysql_wire: parts.mysql_wire,
            #[cfg(feature = "pgwire")]
            pg: parts.pg,
            #[cfg(feature = "stream-grpc")]
            stream_grpc: parts.stream_grpc,
            #[cfg(feature = "qdrant")]
            qdrant: parts.qdrant,
            durable,
            #[cfg(feature = "es")]
            es: parts.es,
            cluster: None,
        };
        // The runtime after the collection service, before the listener
        // serves (T0-6, X8). A failure shuts everything down again.
        if let Err(err) = server.durable.start_runtime(NODE_ID).await {
            drop(serve_now);
            if let Err(shutdown) = server.shutdown().await {
                tracing::warn!(%shutdown, "stopping after a failed start");
            }
            return Err(err);
        }
        let _ = serve_now.send(());
        tracing::info!(%local_addr, "loams is serving");
        Ok(server)
    }

    /// Rule 3: one cluster node. The metastore routes are served (with
    /// `/health`, and `503` for everything else) as soon as `--listen` is
    /// bound, so peers reach the replica during bootstrap.
    async fn start_cluster(mut config: ServerConfig) -> Result<Self, ServerError> {
        let cluster = config.cluster.clone().expect("cluster mode");
        let roles = cluster.effective_roles();
        let node_id = cluster.node_id;
        config.log.node_id = node_id;
        let store = Store::from_url(&bucket_url(&config)?, Vec::<(String, String)>::new())?;
        let (listener, local_addr) = if cluster.listen_stdin {
            inherited_listener(config.listen)?
        } else {
            bind(config.listen).await?
        };
        let transport = HttpTransport::new(cluster.transport)?;
        let mut meta_config = MetaConfig::new(node_id, config.data_dir.join("meta"), store.clone());
        meta_config.snapshot_every = config.snapshot_every;
        // A learner may start empty over existing snapshots (Task 9 rule 7).
        meta_config.allow_fresh_start_with_existing_snapshots = !roles.meta;
        let node = MetaNode::start_with(meta_config, Transport::Http(transport.clone())).await?;
        let late = LateRouter::default();
        let early = {
            let late = late.clone();
            meta_rpc::router(node.clone())
                .route(
                    "/health",
                    axum::routing::get(|| async { http::StatusCode::OK }),
                )
                .fallback(move |request: axum::extract::Request| late.clone().handle(request))
        };
        let (stop_http, stopped) = oneshot::channel::<()>();
        let http = tokio::spawn(async move {
            let serve = axum::serve(listener, early).with_graceful_shutdown(async move {
                let _ = stopped.await;
            });
            if let Err(err) = serve.await {
                tracing::error!(%err, "HTTP server failed");
            }
        });
        let started = Self::start_cluster_on(
            &mut config,
            &cluster,
            roles,
            node.clone(),
            store,
            transport.clone(),
        )
        .await;
        match started {
            Ok((meta, meta_store, registry, parts)) => {
                let app = parts.app.clone();
                let serve = late.clone();
                let mut server = Self {
                    local_addr,
                    node: Some(node),
                    meta: Some(meta),
                    #[cfg(feature = "tikv")]
                    tikv_gc: None,
                    meta_store,
                    writer: parts.writer,
                    cache: parts.cache,
                    collection_context: parts.collection_context,
                    collection_factory: parts.collection_factory,
                    collections: parts.collections,
                    worker: parts.worker,
                    hot: parts.hot,
                    http,
                    stop_http,
                    flight: parts.flight,
                    #[cfg(feature = "mysql-wire")]
                    mysql_wire: parts.mysql_wire,
                    #[cfg(feature = "pgwire")]
                    pg: parts.pg,
                    #[cfg(feature = "stream-grpc")]
                    stream_grpc: parts.stream_grpc,
                    #[cfg(feature = "qdrant")]
                    qdrant: parts.qdrant,
                    durable: parts.durable,
                    #[cfg(feature = "es")]
                    es: parts.es,
                    cluster: Some(ClusterRuntime {
                        node_id,
                        roles,
                        registry,
                        transport,
                        seeds: cluster.peers.values().cloned().collect(),
                        late,
                    }),
                };
                // The runtime after the collection service, before the
                // routes serve (T0-6, X8). A failure shuts the node down.
                if let Err(err) = server.durable.start_runtime(node_id).await {
                    if let Err(shutdown) = server.shutdown().await {
                        tracing::warn!(%shutdown, "stopping the node after a failed start");
                    }
                    return Err(err);
                }
                serve.set(app);
                tracing::info!(%local_addr, node_id, %roles, "loams cluster node is serving");
                Ok(server)
            }
            Err(err) => {
                if let Err(shutdown) = node.shutdown().await {
                    tracing::warn!(%shutdown, "stopping the metastore after a failed start");
                }
                let _ = stop_http.send(());
                http.abort();
                Err(err)
            }
        }
    }

    /// Rule 3 steps 4–8.
    #[allow(clippy::too_many_arguments)]
    async fn start_cluster_on(
        config: &mut ServerConfig,
        cluster: &ClusterConfig,
        roles: Roles,
        node: MetaNode,
        store: Store,
        transport: HttpTransport,
    ) -> Result<(MetaClient, Arc<dyn MetaStore>, Arc<NodeRegistry>, Assembled), ServerError> {
        let node_id = cluster.node_id;
        let lowest = cluster.peers.keys().next().copied();
        if roles.meta && lowest == Some(node_id) {
            // A no-op once initialized.
            node.initialize_with(cluster.peers.clone()).await?;
        }
        let seeds: Vec<String> = cluster.peers.values().cloned().collect();
        // Resolved before the join, so a name that does not resolve adds no
        // learner (PR #40 review).
        let addr = resolve(&cluster.advertise).await?;
        // A learner that never registered has no node lease, so the
        // membership task would never evict it: after a failed start, leave
        // (PR #40 review). That includes a failed join, which may have added
        // the learner anyway (a timeout while it caught up, PR #42 review);
        // leaving a node that is not a member changes nothing.
        let leave_after_failure = async || {
            if roles.meta {
                return;
            }
            let left =
                meta_rpc::leave(&transport, &seeds, LeaveRequest { node_id }, LEAVE_WAIT).await;
            if let Err(err) = left {
                tracing::warn!(%err, "leaving the metastore membership after a failed start");
            }
        };
        let joined = meta_rpc::join(
            &transport,
            &seeds,
            JoinRequest {
                node_id,
                addr: cluster.advertise.clone(),
            },
            cluster.join_deadline,
        )
        .await;
        let join_changed = match joined {
            Ok(changed) => changed,
            Err(err) => {
                leave_after_failure().await;
                return Err(err.into());
            }
        };
        tracing::info!(node_id, changed = join_changed, "joined the metastore");
        let started = Self::start_joined(
            config,
            cluster,
            roles,
            node,
            store,
            transport.clone(),
            addr,
            join_changed,
        )
        .await;
        if started.is_err() {
            leave_after_failure().await;
        }
        started
    }

    /// Rule 3 steps 5–8, after the join.
    #[allow(clippy::too_many_arguments)]
    async fn start_joined(
        config: &mut ServerConfig,
        cluster: &ClusterConfig,
        roles: Roles,
        node: MetaNode,
        store: Store,
        transport: HttpTransport,
        addr: SocketAddr,
        join_changed: bool,
    ) -> Result<(MetaClient, Arc<dyn MetaStore>, Arc<NodeRegistry>, Assembled), ServerError> {
        let node_id = cluster.node_id;
        let seeds: Vec<String> = cluster.peers.values().cloned().collect();
        node.wait_for_leader(CLUSTER_LEADER_WAIT).await?;
        let (meta, meta_store) = cluster::metastore(&node, &transport);
        let registry = NodeRegistry::register(
            meta_store.clone(),
            NodeDescriptor {
                node_id,
                incarnation: Ulid::generate(),
                addr,
                roles,
                zone: cluster.zone.clone(),
            },
            cluster.registry,
        )
        .await?;
        let placement = Arc::new(PlacementImpl::new(registry.clone(), cluster.replication));
        let forward_stats = Arc::new(ForwardStats::default());
        let remote = match RemoteReadsImpl::new(
            placement.clone(),
            forward_stats.clone(),
            RemoteReadsConfig::default(),
        ) {
            Ok(remote) => Arc::new(remote),
            Err(err) => {
                registry.deregister().await;
                return Err(err.into());
            }
        };
        let mut extra_sources: Vec<Arc<dyn TaskSource>> = Vec::new();
        if roles.worker {
            extra_sources.push(Arc::new(MembershipSource::new(
                node.clone(),
                transport.clone(),
                seeds,
                cluster.learner_expiry,
                cluster.membership_interval,
            )));
        }
        let setup = NodeSetup {
            node_id,
            roles,
            meta_store: meta_store.clone(),
            placement: placement.clone(),
            routing: Some((placement, remote)),
            forward_stats,
            extra_sources,
            node_info: Some(Arc::new(ClusterInfo {
                node: node.clone(),
                join_changed,
            })),
        };
        // After the leader, before the collection service (row T0-6).
        let durable = match Durable::start(config, node_id).await {
            Ok(durable) => durable,
            Err(err) => {
                registry.deregister().await;
                return Err(err);
            }
        };
        let mut parts = match Self::assemble(config, store, setup).await {
            Ok(parts) => parts,
            Err(err) => {
                durable.stop().await;
                registry.deregister().await;
                return Err(err);
            }
        };
        parts.durable = durable;
        // The caller starts the durable runtime, then serves the routes
        // (`late.set`), once the node is whole enough to shut down.
        Ok((meta, meta_store, registry, parts))
    }

    /// The components of a node, per its roles (Task 11 rule 2; single-node
    /// modes run every role). Every fallible step runs before any task is
    /// spawned, so a failure leaves only the caller's metastore to stop.
    async fn assemble(
        config: &mut ServerConfig,
        store: Store,
        setup: NodeSetup,
    ) -> Result<Assembled, ServerError> {
        let NodeSetup {
            node_id,
            roles,
            meta_store,
            placement,
            routing,
            forward_stats,
            extra_sources,
            node_info,
        } = setup;
        // Scan plans name the Lance datasets under the bucket (Task 14 rule 8).
        config.query.lance_base_url = Some(bucket_url(config)?);
        let cache = RangeCache::new(store.clone(), config.cache.clone()).await?;
        // Flight SQL listens after the HTTP API (rule 5.3), on gateways only.
        let flight_listener = match flight_addr(config).filter(|_| roles.gateway) {
            Some(addr) => match bind(addr).await {
                Ok(bound) => Some(bound),
                Err(err) => {
                    if let Err(err) = cache.close().await {
                        tracing::warn!(%err, "closing the cache after a failed start");
                    }
                    return Err(err);
                }
            },
            None => None,
        };
        // The MySQL wire listener, on gateways only: bound here, before any
        // task is spawned, and served once the collection service exists.
        #[cfg(feature = "mysql-wire")]
        let mysql_listener = match config.mysql_wire.clone().filter(|_| roles.gateway) {
            Some(mysql_config) => match crate::mysql_wire::listen(&mysql_config).await {
                Ok(bound) => Some((bound, mysql_config)),
                Err(source) => {
                    if let Err(err) = cache.close().await {
                        tracing::warn!(%err, "closing the cache after a failed start");
                    }
                    return Err(ServerError::MysqlListen {
                        addr: mysql_config.listen,
                        source,
                    });
                }
            },
            None => None,
        };
        // The PostgreSQL wire listener, on gateways only: bound here, before
        // any task is spawned, and served once the collection service exists.
        #[cfg(feature = "pgwire")]
        let pg_listener = match config.pg.clone().filter(|_| roles.gateway) {
            Some(pg_config) => match crate::pg::listen(&pg_config).await {
                Ok(bound) => Some((bound, pg_config)),
                Err(source) => {
                    if let Err(err) = cache.close().await {
                        tracing::warn!(%err, "closing the cache after a failed start");
                    }
                    return Err(ServerError::PgListen {
                        addr: pg_config.listen,
                        source,
                    });
                }
            },
            None => None,
        };
        #[cfg(feature = "stream-grpc")]
        let stream_grpc_listener = match config.stream_grpc.filter(|_| roles.gateway) {
            Some(addr) => match loams_stream_grpc::bind(addr)
                .await
                .and_then(|listener| listener.local_addr().map(|bound| (listener, bound)))
            {
                Ok(bound) => Some(bound),
                Err(source) => {
                    if let Err(err) = cache.close().await {
                        tracing::warn!(%err, "closing the cache after a failed start");
                    }
                    return Err(ServerError::StreamGrpcListen { addr, source });
                }
            },
            None => None,
        };
        // The Qdrant gateway's listeners, on gateways only (plan M1.4 Task
        // 2): bound here, before any task is spawned, and served once the
        // collection service exists (row T2-2).
        #[cfg(feature = "qdrant")]
        let qdrant_listeners = match config.qdrant.clone().filter(|_| roles.gateway) {
            Some(qdrant) => match bind_qdrant(&qdrant).await {
                Ok(bound) => Some((qdrant, bound)),
                Err(err) => {
                    if let Err(err) = cache.close().await {
                        tracing::warn!(%err, "closing the cache after a failed start");
                    }
                    return Err(err);
                }
            },
            None => None,
        };
        // The Elasticsearch gateway's listener, next to Qdrant's (plan M1.5
        // Task 1, row E13): bound here and served once the collection
        // service exists.
        #[cfg(feature = "es")]
        let es_listener = match config.es.clone().filter(|_| roles.gateway) {
            Some(es) => match tokio::net::TcpListener::bind(es.listen).await {
                Ok(bound) => Some((es, bound)),
                Err(source) => {
                    if let Err(err) = cache.close().await {
                        tracing::warn!(%err, "closing the cache after a failed start");
                    }
                    return Err(ServerError::EsListen {
                        addr: es.listen,
                        source,
                    });
                }
            },
            None => None,
        };

        // Validated above, so this cannot fail. A node without the `log`
        // role still holds a writer (the service needs one), but serves no
        // write route, so it never appends.
        let writer = LogWriter::start(meta_store.clone(), store.clone(), config.log.clone())?;
        let reader = LogReader::new(meta_store.clone(), cache.clone());
        let stop_early = |writer: LogWriter, cache: RangeCache| async move {
            if let Err(err) = writer.shutdown().await {
                tracing::warn!(%err, "stopping the log writer after a failed start");
            }
            if let Err(err) = cache.close().await {
                tracing::warn!(%err, "closing the cache after a failed start");
            }
        };
        // Unique per process incarnation, as leases require.
        let owner = format!("node-{node_id}-{}", Ulid::generate());
        let mut worker = Worker::new(
            meta_store.clone(),
            WorkerConfig {
                poll_interval: config.worker_poll_interval,
                lease_ttl: config.worker_lease_ttl,
                ..WorkerConfig::new(owner)
            },
        );
        let mut collection = config.collection.clone();
        collection.max_commit_delay = config.link.max_commit_delay;
        collection.keep_manifests = config.gc.keep_manifests;
        // Lance reads go through the range cache (M1.3 Ruling 9), which the
        // hot tier's fragment prefetch fills.
        let collection_context = CollectionContext {
            meta: meta_store.clone(),
            store: store.clone(),
            cache: cache.clone(),
            lance: LanceEnv::with_cache(store.clone(), cache.clone(), config.lance.clone()),
            manifests: ManifestCache::new(collection.manifest_cache_entries),
            config: collection,
        };
        let collection_factory = Arc::new(CollectionTargetFactory::new(collection_context.clone()));
        let registry = TargetRegistry::new()
            .with(Arc::new(CounterTargetFactory::new(
                store.clone(),
                config.link.max_commit_delay,
            )))
            .with(collection_factory.clone());
        worker.add_source(Arc::new(LinkApplySource::new(
            meta_store.clone(),
            reader.clone(),
            registry.clone(),
            config.link.clone(),
        )));
        worker.add_source(Arc::new(SegmenterSource::new(
            store.clone(),
            cache.clone(),
            config.segmenter.clone(),
        )));
        worker.add_source(Arc::new(IndexBuildSource::new(collection_context.clone())));
        // M1.3: maintenance, then hot artifact builds (Task 8 rule 5).
        if config.maintenance.merge {
            worker.add_source(Arc::new(SplitMergeSource::new(
                collection_context.clone(),
                config.maintenance.clone(),
            )));
        }
        if config.maintenance.compaction {
            worker.add_source(Arc::new(LanceCompactionSource::new(
                collection_context.clone(),
                config.maintenance.clone(),
            )));
        }
        let engine = config
            .hnsw_engine
            .clone()
            .unwrap_or_else(loams_hnsw::default_engine);
        if config.hot.enabled && (cfg!(feature = "hnsw") || config.hnsw_engine.is_some()) {
            let hot_build = HotBuildConfig {
                pin_all: config.hot.pin_all,
                ..config.hot_build.clone()
            };
            match HotBuildSource::new(collection_context.clone(), hot_build, engine.clone()) {
                Ok(source) => worker.add_source(Arc::new(source)),
                Err(err) => {
                    stop_early(writer, cache).await;
                    return Err(err.into());
                }
            }
        }
        worker.add_source(Arc::new(RetentionSource::new(config.retention.clone())));
        worker.add_source(Arc::new(CollectionTrimSource::new(
            collection_context.clone(),
        )));
        worker.add_source(Arc::new(GcSource::with_roots(
            store.clone(),
            config.gc.clone(),
            vec![
                Arc::new(LinkGcRoots),
                Arc::new(CollectionGcRoots::new(collection_context.clone())),
                Arc::new(PkGcRoots),
            ],
        )));
        for source in extra_sources {
            worker.add_source(source);
        }
        let hot = match config.hot.enabled && roles.query {
            true => match HotTierImpl::start(
                collection_context.clone(),
                config.hot.clone(),
                node_id,
                placement.clone(),
                engine,
            )
            .await
            {
                Ok(tier) => Some(tier),
                Err(err) => {
                    stop_early(writer, cache).await;
                    return Err(err.into());
                }
            },
            false => None,
        };
        // Rule 5.1: after the collection context, before the router, on the
        // reader built above (the server keeps none, row 0.52).
        let collections = CollectionService::new(
            collection_context.clone(),
            CollectionWriter::new(meta_store.clone(), writer.clone()),
            reader.clone(),
            config.query.clone(),
        );
        if let Some(tier) = &hot {
            collections.set_hot_tier(Arc::new(tier.clone()));
        }
        if let Some((placement, remote)) = routing {
            collections.set_placement(placement, remote);
        }
        // Every fallible PostgreSQL step (the `pg_catalog` setup) runs here,
        // before the worker starts; the accept loop is spawned only after.
        #[cfg(feature = "pgwire")]
        let pg = match pg_listener {
            Some((bound, pg_config)) => {
                match crate::pg::prepare(collections.clone(), bound, pg_config.clone()).await {
                    Ok(prepared) => Some(prepared),
                    Err(source) => {
                        collections.shutdown().await;
                        if let Some(tier) = &hot {
                            tier.shutdown().await;
                        }
                        collection_factory.close().await;
                        stop_early(writer, cache).await;
                        return Err(ServerError::PgListen {
                            addr: pg_config.listen,
                            source,
                        });
                    }
                }
            }
            None => None,
        };
        let worker = roles.worker.then(|| worker.start());
        #[cfg(feature = "pgwire")]
        let pg = pg.map(crate::pg::serve);
        // After every fallible step, like the PostgreSQL accept loop.
        #[cfg(feature = "mysql-wire")]
        let mysql_wire = mysql_listener.map(|(bound, mysql_config)| {
            crate::mysql_wire::start(collections.clone(), bound, mysql_config)
        });
        #[cfg(feature = "qdrant")]
        let qdrant = match qdrant_listeners {
            Some((qdrant, (rest, grpc))) => {
                let stop = CancellationToken::new();
                let handle = loams_qdrant::QdrantGateway::new(collections.clone(), qdrant)
                    .serve(rest, grpc, stop.clone())
                    .await;
                Some(Qdrant { handle, stop })
            }
            None => None,
        };
        #[cfg(feature = "es")]
        let es = es_listener.map(|(es, listener)| {
            let stop = CancellationToken::new();
            let handle =
                loams_es::EsGateway::new(collections.clone(), es).serve(listener, stop.clone());
            Es { handle, stop }
        });
        let internal = reqwest::Client::builder()
            .connect_timeout(api::hot::OWNER_CONNECT_TIMEOUT)
            .timeout(api::hot::OWNER_TIMEOUT)
            .build()
            .unwrap_or_default();
        let state = AppState {
            meta: meta_store.clone(),
            writer: writer.clone(),
            reader,
            store: store.clone(),
            registry,
            collections: collections.clone(),
            hot: hot.clone(),
            placement,
            node_id,
            internal,
            hot_pin_all: config.hot.pin_all,
            roles,
            forwarded: roles.query.then(|| ForwardedReads {
                service: collections.clone(),
                stats: forward_stats.clone(),
            }),
            forward_stats,
            node_info,
            cloudevents: config.cloudevents,
            reflection: config.reflection,
        };
        let app = match roles.gateway {
            true => api::router(state),
            false => api::internal_router(state),
        };
        let flight = flight_listener.map(|(listener, addr)| {
            let streams: Arc<dyn StreamProducer> = Arc::new(NativeStreamProducer {
                meta: meta_store.clone(),
                writer: writer.clone(),
            });
            Flight::start(
                listener,
                addr,
                collections.clone(),
                streams,
                config.flight.clone(),
            )
        });
        #[cfg(feature = "stream-grpc")]
        let stream_grpc = stream_grpc_listener.map(|(listener, addr)| {
            let streams: Arc<dyn StreamProducer> = Arc::new(NativeStreamProducer {
                meta: meta_store.clone(),
                writer: writer.clone(),
            });
            let events: Arc<dyn loams_stream_grpc::EventProducer> =
                Arc::new(api::events::NativeEventProducer {
                    meta: meta_store.clone(),
                    writer: writer.clone(),
                    config: config.cloudevents,
                    node_id,
                });
            StreamGrpc::start(listener, addr, streams, events)
        });
        Ok(Assembled {
            writer,
            cache,
            collection_context,
            collection_factory,
            collections,
            worker,
            hot,
            app,
            flight,
            #[cfg(feature = "mysql-wire")]
            mysql_wire,
            #[cfg(feature = "pgwire")]
            pg,
            #[cfg(feature = "stream-grpc")]
            stream_grpc,
            #[cfg(feature = "qdrant")]
            qdrant,
            durable: Durable::none(),
            #[cfg(feature = "es")]
            es,
        })
    }

    /// The address the HTTP API listens on.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// The openraft metastore client, for embedding and tests; `None` on the
    /// TiKV metastore (use [`Server::meta_store`]).
    pub fn meta(&self) -> Option<&MetaClient> {
        self.meta.as_ref()
    }

    /// The metastore as the trait object the server hands to every
    /// component (the log, the worker and its sources, collection storage
    /// and the HTTP API): the same handle each of them holds.
    pub fn meta_store(&self) -> Arc<dyn MetaStore> {
        self.meta_store.clone()
    }

    /// The collection storage context the server's tasks run on; M1.2 builds
    /// its `CollectionService` on it.
    pub fn collection_context(&self) -> &CollectionContext {
        &self.collection_context
    }

    /// The hot tier, unless `--hot off`.
    pub fn hot_tier(&self) -> Option<&HotTierImpl> {
        self.hot.as_ref()
    }

    /// The collection service behind the native API.
    pub fn collections(&self) -> Arc<CollectionService> {
        self.collections.clone()
    }

    /// The address Flight SQL listens on, when it does.
    pub fn flight_sql_addr(&self) -> Option<SocketAddr> {
        self.flight.as_ref().map(|flight| flight.addr)
    }

    /// The MySQL wire listener, when configured.
    #[cfg(feature = "mysql-wire")]
    pub fn mysql_wire_addr(&self) -> Option<SocketAddr> {
        self.mysql_wire.as_ref().map(|mysql| mysql.addr)
    }

    /// The PostgreSQL wire listener, when configured.
    #[cfg(feature = "pgwire")]
    pub fn pg_addr(&self) -> Option<SocketAddr> {
        self.pg.as_ref().map(|pg| pg.addr)
    }

    /// The native stream gRPC listener, when configured.
    #[cfg(feature = "stream-grpc")]
    pub fn stream_grpc_addr(&self) -> Option<SocketAddr> {
        self.stream_grpc.as_ref().map(|stream| stream.addr)
    }

    /// The address the durable execution API listens on, when it does (D1,
    /// feature `durable`).
    pub fn durable_addr(&self) -> Option<SocketAddr> {
        self.durable.addr()
    }

    /// The address the Qdrant REST API listens on, when it does.
    #[cfg(feature = "qdrant")]
    pub fn qdrant_rest_addr(&self) -> Option<SocketAddr> {
        self.qdrant.as_ref().map(|q| q.handle.rest_addr)
    }

    /// The address the Qdrant gRPC API listens on, when it does.
    #[cfg(feature = "qdrant")]
    pub fn qdrant_grpc_addr(&self) -> Option<SocketAddr> {
        self.qdrant.as_ref().map(|q| q.handle.grpc_addr)
    }

    /// The address the Elasticsearch API listens on, when it does.
    #[cfg(feature = "es")]
    pub fn es_addr(&self) -> Option<SocketAddr> {
        self.es.as_ref().map(|es| es.handle.addr)
    }

    /// The server's log writer; M1.2 builds its `CollectionWriter` on it.
    pub fn log_writer(&self) -> &LogWriter {
        &self.writer
    }

    /// Stops the Qdrant and Elasticsearch gateways (waiting up to 10 s for
    /// their requests), stops accepting requests, stops Flight SQL, stops the
    /// durable server (D1: before anything it may call into), stops the
    /// collection
    /// service's tails, flushes the writer (buffered appends are
    /// acknowledged), stops the worker (releasing its task leases), stops
    /// the hot tier, closes the collection targets' PK index handles, waits
    /// for in-flight requests (up to 10 s), and shuts the metastore down
    /// (rule 5.4).
    ///
    /// A cluster node first answers `503` on its client routes, releases
    /// its node lease and, as a learner, leaves the membership; its
    /// metastore routes keep serving until the replica stops (Task 11
    /// rule 4).
    pub async fn shutdown(self) -> Result<(), ServerError> {
        // The Qdrant gateway stops first, within the HTTP grace period
        // (plan M1.4 Task 2 rule 3).
        shutdown_phase("qdrant");
        // The Elasticsearch gateway stops together with it (plan M1.5 Task
        // 1, row E13): both tokens are cancelled before either is awaited.
        #[cfg(feature = "es")]
        if let Some(es) = &self.es {
            es.stop.cancel();
        }
        #[cfg(feature = "qdrant")]
        if let Some(qdrant) = self.qdrant {
            qdrant.stop.cancel();
            if let Err(err) = qdrant.handle.stop_within(HTTP_GRACE).await {
                tracing::warn!(%err, "the Qdrant gateway stopped with an error");
            }
        }
        #[cfg(feature = "es")]
        if let Some(es) = self.es
            && let Err(err) = es.handle.stop_within(HTTP_GRACE).await
        {
            tracing::warn!(%err, "the Elasticsearch gateway stopped with an error");
        }
        let mut stop_http = Some(self.stop_http);
        shutdown_phase("http");
        match &self.cluster {
            Some(cluster) => {
                cluster.late.close();
                cluster.registry.deregister().await;
                if !cluster.roles.meta {
                    let left = meta_rpc::leave(
                        &cluster.transport,
                        &cluster.seeds,
                        LeaveRequest {
                            node_id: cluster.node_id,
                        },
                        LEAVE_WAIT,
                    )
                    .await;
                    if let Err(err) = left {
                        tracing::warn!(%err, "leaving the metastore membership");
                    }
                }
            }
            None => {
                if let Some(stop) = stop_http.take() {
                    let _ = stop.send(());
                }
            }
        }
        shutdown_phase("flight");
        if let Some(flight) = self.flight {
            flight.stop().await;
        }
        #[cfg(feature = "mysql-wire")]
        if let Some(mysql) = self.mysql_wire {
            mysql.stop_within(HTTP_GRACE).await;
        }
        #[cfg(feature = "pgwire")]
        if let Some(pg) = self.pg {
            pg.stop_within(HTTP_GRACE).await;
        }
        #[cfg(feature = "stream-grpc")]
        if let Some(stream_grpc) = self.stream_grpc {
            stream_grpc.stop().await;
        }
        // D1 (T0-6, X8): after Flight (and, in cluster mode, after
        // `late.close()`), before the collection service: the runtime first,
        // then the server (phases `durable_runtime`, `durable`).
        self.durable.stop().await;
        shutdown_phase("collections");
        self.collections.shutdown().await;
        shutdown_phase("writer");
        if let Err(err) = self.writer.shutdown().await {
            tracing::warn!(%err, "the final flush failed");
        }
        shutdown_phase("worker");
        if let Some(worker) = self.worker {
            worker.stop().await;
        }
        // The tier stops after the worker and before the metastore (Task 8
        // rule 5).
        shutdown_phase("hot");
        if let Some(tier) = &self.hot {
            tier.shutdown().await;
        }
        self.collection_factory.close().await;
        let mut http = self.http;
        if stop_http.is_none() && tokio::time::timeout(HTTP_GRACE, &mut http).await.is_err() {
            tracing::warn!("in-flight requests did not finish; aborting them");
            http.abort();
        }
        if let Err(err) = self.cache.close().await {
            tracing::warn!(%err, "closing the cache failed");
        }
        shutdown_phase("metastore");
        #[cfg(feature = "tikv")]
        if let Some(gc) = self.tikv_gc {
            gc.stop().await;
        }
        let stopped = match &self.node {
            Some(node) => node.shutdown().await,
            None => Ok(()),
        };
        // A cluster node serves its metastore routes until the replica stops.
        if let Some(stop) = stop_http.take() {
            let _ = stop.send(());
            if tokio::time::timeout(HTTP_GRACE, &mut http).await.is_err() {
                http.abort();
            }
        }
        stopped?;
        Ok(())
    }
}

/// Binds `addr`.
async fn bind(addr: SocketAddr) -> Result<(tokio::net::TcpListener, SocketAddr), ServerError> {
    let bound = async {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        Ok::<_, std::io::Error>((listener, local_addr))
    }
    .await;
    bound.map_err(|source| ServerError::Listen { addr, source })
}

#[cfg(unix)]
/// Takes the test harness's bound listener from stdin and validates its address.
fn inherited_listener(
    addr: SocketAddr,
) -> Result<(tokio::net::TcpListener, SocketAddr), ServerError> {
    // The child receives a clone of the parent's bound listener on stdin.
    // Clone it here so the server owns its descriptor independently of stdin.
    let fd = std::io::stdin()
        .as_fd()
        .try_clone_to_owned()
        .map_err(|source| ServerError::Listen { addr, source })?;
    let listener = std::net::TcpListener::from(fd);
    let bound = listener
        .local_addr()
        .map_err(|source| ServerError::Listen { addr, source })?;
    if bound != addr {
        return Err(ServerError::Config(format!(
            "inherited listener is bound to {bound}, not --listen {addr}"
        )));
    }
    listener
        .set_nonblocking(true)
        .map_err(|source| ServerError::Listen { addr, source })?;
    let listener = tokio::net::TcpListener::from_std(listener)
        .map_err(|source| ServerError::Listen { addr, source })?;
    Ok((listener, bound))
}

#[cfg(not(unix))]
/// Rejects the Unix-only listener handoff on other platforms.
fn inherited_listener(
    _addr: SocketAddr,
) -> Result<(tokio::net::TcpListener, SocketAddr), ServerError> {
    Err(ServerError::Config(
        "--listen-stdin requires a Unix socket descriptor".to_string(),
    ))
}

/// `--advertise` as a socket address (E49): an `ip:port` as is, a host name
/// resolved once (its first address).
async fn resolve(advertise: &str) -> Result<SocketAddr, ServerError> {
    if let Ok(addr) = advertise.parse() {
        return Ok(addr);
    }
    tokio::net::lookup_host(advertise)
        .await
        .ok()
        .and_then(|mut addrs| addrs.next())
        .ok_or_else(|| ServerError::Config(format!("--advertise {advertise:?} does not resolve")))
}
