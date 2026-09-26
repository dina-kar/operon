//! One Operon process: a single-node metastore, the log, the range cache,
//! a worker running the background tasks, the collection service, the
//! native HTTP API and the Flight SQL listener (design §10 §1); or, with
//! [`ServerConfig::cluster`], one node of `operon cluster` running the
//! components of its roles (plan M1.3 Task 11).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use operon_cache::{RangeCache, RangeCacheConfig};
use operon_collection::{
    CollectionConfig, CollectionContext, CollectionGcRoots, CollectionTargetFactory,
    CollectionTrimSource, CollectionWriter, IndexBuildSource, LanceCompactionSource, LanceConfig,
    LanceEnv, MaintenanceConfig, ManifestCache, PkGcRoots, SplitMergeSource,
};
use operon_common::meta::MetaStore;
use operon_hnsw::HnswEngine;
use operon_hot::{
    AlwaysLocal, ForwardStats, HotBuildConfig, HotBuildSource, HotTierConfig, HotTierImpl,
    NodeDescriptor, NodeRegistry, PlacementImpl, RegistryConfig, RemoteReadsConfig,
    RemoteReadsImpl, Roles,
};
use operon_link::{CounterTargetFactory, LinkApplySource, LinkConfig, LinkGcRoots, TargetRegistry};
use operon_log::gc::{GcConfig, GcSource};
use operon_log::{
    LogConfig, LogReader, LogWriter, RetentionConfig, RetentionSource, SegmenterConfig,
    SegmenterSource,
};
use operon_meta::rpc::{self as meta_rpc, JoinRequest, LeaveRequest};
use operon_meta::{
    HttpTransport, HttpTransportConfig, MetaClient, MetaClientConfig, MetaConfig, MetaNode, Router,
    SystemClock, Transport,
};
use operon_query::flight::{FlightConfig, PutTasks, serve_flight_sql_tracked};
use operon_query::flight_ingest::StreamProducer;
use operon_query::placement::Placement;
use operon_query::{CollectionService, ServiceConfig};
use operon_store::Store;
use operon_worker::{TaskSource, Worker, WorkerConfig, WorkerHandle};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

use crate::api::internal::NodeInfo;
use crate::api::{self, AppState, ForwardedReads, NativeStreamProducer};
use crate::cluster::{self, ClusterInfo, LateRouter, MembershipSource};

/// The meta node id of a single-process Operon.
const NODE_ID: u64 = 1;
/// How long startup waits for the single meta node to become leader.
const LEADER_WAIT: Duration = Duration::from_secs(30);
/// How long shutdown lets in-flight HTTP requests (such as long-polls) finish.
const HTTP_GRACE: Duration = Duration::from_secs(10);
/// How long a cluster node waits for a leader after joining (rule 3.5).
const CLUSTER_LEADER_WAIT: Duration = Duration::from_secs(30);
/// How long a leaving learner tries to reach the leader at shutdown.
const LEAVE_WAIT: Duration = Duration::from_secs(10);

/// One node of `operon cluster` (plan M1.3 Task 11; Ruling 15).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterConfig {
    pub node_id: u64,
    pub roles: Roles,
    /// Where other nodes reach this one (`host:port`; default `--listen`).
    pub advertise: String,
    /// The `meta` nodes: id → `host:port`.
    pub peers: BTreeMap<u64, String>,
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

/// How to run a single-process Operon.
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
    /// default here) serves no Flight SQL. `operon dev` and `standalone`
    /// set it.
    pub flight_sql: Option<SocketAddr>,
    /// How Flight SQL bounds its statements and its ingest.
    pub flight: FlightConfig,
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
    /// `operon_hnsw::default_engine()` (qdrant-edge with the `hnsw` feature;
    /// without it, no artifact is built unless this is set). Tests set
    /// `FlatEngine`.
    pub hnsw_engine: Option<Arc<dyn HnswEngine>>,
    /// `None`: `dev` or `standalone` (one node, every role). `Some`: one
    /// node of `operon cluster` (plan M1.3 Task 11).
    pub cluster: Option<ClusterConfig>,
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
            flight: FlightConfig::default(),
            cluster: None,
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
        }
        self.flight.validate().map_err(ServerError::Config)?;
        self.gc
            .check_deadlines(&[
                ("segmenter.swap_deadline", self.segmenter.swap_deadline),
                ("link.max_commit_delay", self.link.max_commit_delay),
                (
                    "collection.index_commit_delay",
                    self.collection.index_commit_delay,
                ),
            ])
            .map_err(|err| ServerError::Config(err.to_string()))
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
    Store(#[from] operon_store::StoreError),
    #[error("metastore: {0}")]
    Meta(#[from] operon_meta::MetaError),
    #[error("cache: {0}")]
    Cache(#[from] operon_cache::CacheError),
    #[error("log: {0}")]
    Log(#[from] operon_log::LogError),
    #[error("hot tier: {0}")]
    Hot(#[from] operon_hot::TierError),
    #[error("listen on {addr}: {source}")]
    Listen {
        addr: SocketAddr,
        source: std::io::Error,
    },
}

/// A running Operon process.
#[derive(Debug)]
pub struct Server {
    local_addr: SocketAddr,
    node: MetaNode,
    meta: MetaClient,
    /// `meta` as the trait object every component holds.
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
    /// Cluster mode only.
    cluster: Option<ClusterRuntime>,
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
        mut config: ServerConfig,
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
        let (listener, local_addr) = bind(config.listen).await?;
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
        let parts = Self::assemble(&mut config, store, setup).await?;
        let (stop_http, stopped) = oneshot::channel::<()>();
        let app = parts.app;
        let http = tokio::spawn(async move {
            let serve = axum::serve(listener, app).with_graceful_shutdown(async move {
                let _ = stopped.await;
            });
            if let Err(err) = serve.await {
                tracing::error!(%err, "HTTP server failed");
            }
        });
        tracing::info!(%local_addr, "operon is serving");
        Ok(Self {
            local_addr,
            node,
            meta,
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
            cluster: None,
        })
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
        let (listener, local_addr) = bind(config.listen).await?;
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
            late.clone(),
        )
        .await;
        match started {
            Ok((meta, meta_store, registry, parts)) => {
                tracing::info!(%local_addr, node_id, %roles, "operon cluster node is serving");
                Ok(Self {
                    local_addr,
                    node,
                    meta,
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
                    cluster: Some(ClusterRuntime {
                        node_id,
                        roles,
                        registry,
                        transport,
                        seeds: cluster.peers.values().cloned().collect(),
                        late,
                    }),
                })
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
        late: LateRouter,
    ) -> Result<(MetaClient, Arc<dyn MetaStore>, Arc<NodeRegistry>, Assembled), ServerError> {
        let node_id = cluster.node_id;
        let lowest = cluster.peers.keys().next().copied();
        if roles.meta && lowest == Some(node_id) {
            // A no-op once initialized.
            node.initialize_with(cluster.peers.clone()).await?;
        }
        let seeds: Vec<String> = cluster.peers.values().cloned().collect();
        let join_changed = meta_rpc::join(
            &transport,
            &seeds,
            JoinRequest {
                node_id,
                addr: cluster.advertise.clone(),
            },
            cluster.join_deadline,
        )
        .await?;
        tracing::info!(node_id, changed = join_changed, "joined the metastore");
        node.wait_for_leader(CLUSTER_LEADER_WAIT).await?;
        let (meta, meta_store) = cluster::metastore(&node, &transport);
        let addr = resolve(&cluster.advertise).await?;
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
        let parts = match Self::assemble(config, store, setup).await {
            Ok(parts) => parts,
            Err(err) => {
                registry.deregister().await;
                return Err(err);
            }
        };
        late.set(parts.app.clone());
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
            .unwrap_or_else(operon_hnsw::default_engine);
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
        let worker = roles.worker.then(|| worker.start());
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
        })
    }

    /// The address the HTTP API listens on.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// The metastore client, for embedding and tests.
    pub fn meta(&self) -> &MetaClient {
        &self.meta
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

    /// The server's log writer; M1.2 builds its `CollectionWriter` on it.
    pub fn log_writer(&self) -> &LogWriter {
        &self.writer
    }

    /// Stops accepting requests, stops Flight SQL, stops the collection
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
        let mut stop_http = Some(self.stop_http);
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
        if let Some(flight) = self.flight {
            flight.stop().await;
        }
        self.collections.shutdown().await;
        if let Err(err) = self.writer.shutdown().await {
            tracing::warn!(%err, "the final flush failed");
        }
        if let Some(worker) = self.worker {
            worker.stop().await;
        }
        // The tier stops after the worker and before the metastore (Task 8
        // rule 5).
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
        let stopped = self.node.shutdown().await;
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
