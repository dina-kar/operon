//! One Operon process: a single-node metastore, the log, the range cache,
//! a worker running the background tasks, the collection service, the
//! native HTTP API and the Flight SQL listener (design §10 §1).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use operon_cache::{RangeCache, RangeCacheConfig};
use operon_collection::{
    CollectionConfig, CollectionContext, CollectionGcRoots, CollectionTargetFactory,
    CollectionTrimSource, CollectionWriter, IndexBuildSource, LanceConfig, LanceEnv, ManifestCache,
    PkGcRoots,
};
use operon_common::meta::MetaStore;
use operon_link::{CounterTargetFactory, LinkApplySource, LinkConfig, LinkGcRoots, TargetRegistry};
use operon_log::gc::{GcConfig, GcSource};
use operon_log::{
    LogConfig, LogReader, LogWriter, RetentionConfig, RetentionSource, SegmenterConfig,
    SegmenterSource,
};
use operon_meta::{MetaClient, MetaClientConfig, MetaConfig, MetaNode, Router, SystemClock};
use operon_query::{CollectionService, ServiceConfig};
use operon_store::Store;
use operon_worker::{Worker, WorkerConfig, WorkerHandle};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use ulid::Ulid;

use crate::api::{self, AppState};

/// The meta node id of a single-process Operon.
const NODE_ID: u64 = 1;
/// How long startup waits for the single meta node to become leader.
const LEADER_WAIT: Duration = Duration::from_secs(30);
/// How long shutdown lets in-flight HTTP requests (such as long-polls) finish.
const HTTP_GRACE: Duration = Duration::from_secs(10);

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
}

impl ServerConfig {
    /// The defaults, with data in `data_dir`, listening on 127.0.0.1:8080.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
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
        }
    }

    /// Rejects any freshness deadline at or above `gc.grace`:
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
    worker: WorkerHandle,
    http: JoinHandle<()>,
    stop_http: oneshot::Sender<()>,
    flight: Option<Flight>,
}

/// The running Flight SQL listener.
#[derive(Debug)]
struct Flight {
    addr: SocketAddr,
    task: JoinHandle<()>,
    stop: oneshot::Sender<()>,
}

impl Flight {
    /// Serves `listener` until stopped.
    ///
    /// Task 12 serves Flight SQL here. Until then the listener is bound (so
    /// `--flight-sql-listen` and the startup line work) and every
    /// connection is closed at once.
    fn start(listener: tokio::net::TcpListener, addr: SocketAddr) -> Self {
        let (stop, mut stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    accepted = listener.accept() => {
                        if let Err(err) = accepted {
                            tracing::debug!(%err, "Flight SQL accept failed");
                        }
                    }
                }
            }
        });
        Self { addr, task, stop }
    }

    async fn stop(self) {
        let _ = self.stop.send(());
        let _ = self.task.await;
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
    /// and its background loops, and serves the HTTP API.
    ///
    /// On failure, everything already started is stopped again (the
    /// metastore releases its local database), so a retry in the same process
    /// can succeed.
    pub async fn start(config: ServerConfig) -> Result<Self, ServerError> {
        config.validate()?;
        config.log.validate()?;
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

    /// Everything after the meta node started. Every fallible step runs
    /// before any task is spawned, so a failure leaves only the meta node
    /// (and the cache, closed here) to stop.
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
        let cache = RangeCache::new(store.clone(), config.cache.clone()).await?;
        let bind = |addr: SocketAddr| async move {
            let bound = async {
                let listener = tokio::net::TcpListener::bind(addr).await?;
                let local_addr = listener.local_addr()?;
                Ok::<_, std::io::Error>((listener, local_addr))
            }
            .await;
            bound.map_err(|source| ServerError::Listen { addr, source })
        };
        let bound = async {
            let http = bind(config.listen).await?;
            // Flight SQL listens after the HTTP API (rule 5.3).
            let flight = match flight_addr(&config) {
                Some(addr) => Some(bind(addr).await?),
                None => None,
            };
            Ok::<_, ServerError>((http, flight))
        }
        .await;
        let ((listener, local_addr), flight_listener) = match bound {
            Ok(bound) => bound,
            Err(err) => {
                if let Err(err) = cache.close().await {
                    tracing::warn!(%err, "closing the cache after a failed start");
                }
                return Err(err);
            }
        };

        // Validated above, so this cannot fail.
        let writer = LogWriter::start(meta_store.clone(), store.clone(), config.log.clone())?;
        let reader = LogReader::new(meta_store.clone(), cache.clone());
        // Unique per process incarnation, as leases require.
        let owner = format!("node-{NODE_ID}-{}", Ulid::generate());
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
        let collection_context = CollectionContext {
            meta: meta_store.clone(),
            store: store.clone(),
            cache: cache.clone(),
            lance: LanceEnv::new(store.clone(), config.lance.clone()),
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
        let worker = worker.start();
        // Rule 5.1: after the collection context, before the router, on the
        // reader built above (the server keeps none, row 0.52).
        let collections = CollectionService::new(
            collection_context.clone(),
            CollectionWriter::new(meta_store.clone(), writer.clone()),
            reader.clone(),
            config.query.clone(),
        );
        let app = api::router(AppState {
            meta: meta_store.clone(),
            writer: writer.clone(),
            reader,
            store: store.clone(),
            registry,
            collections: collections.clone(),
        });
        let (stop_http, stopped) = oneshot::channel::<()>();
        let http = tokio::spawn(async move {
            let serve = axum::serve(listener, app).with_graceful_shutdown(async move {
                let _ = stopped.await;
            });
            if let Err(err) = serve.await {
                tracing::error!(%err, "HTTP server failed");
            }
        });
        tracing::info!(%local_addr, "operon is serving");
        let flight = flight_listener.map(|(listener, addr)| Flight::start(listener, addr));
        Ok(Self {
            local_addr,
            node,
            meta,
            meta_store,
            writer,
            cache,
            collection_context,
            collection_factory,
            collections,
            worker,
            http,
            stop_http,
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
    /// acknowledged), stops the worker (releasing its task leases), closes
    /// the collection targets' PK index handles, waits for in-flight
    /// requests (up to 10 s), and shuts the metastore down (rule 5.4).
    pub async fn shutdown(self) -> Result<(), ServerError> {
        let _ = self.stop_http.send(());
        if let Some(flight) = self.flight {
            flight.stop().await;
        }
        self.collections.shutdown().await;
        if let Err(err) = self.writer.shutdown().await {
            tracing::warn!(%err, "the final flush failed");
        }
        self.worker.stop().await;
        self.collection_factory.close().await;
        let mut http = self.http;
        if tokio::time::timeout(HTTP_GRACE, &mut http).await.is_err() {
            tracing::warn!("in-flight requests did not finish; aborting them");
            http.abort();
        }
        if let Err(err) = self.cache.close().await {
            tracing::warn!(%err, "closing the cache failed");
        }
        self.node.shutdown().await?;
        Ok(())
    }
}
