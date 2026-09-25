//! One Operon process: a single-node metastore, the log, the range cache,
//! a worker running the background tasks, and the native HTTP API (design
//! §10 §1).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use operon_cache::{RangeCache, RangeCacheConfig};
use operon_link::{LinkApplySource, LinkConfig, LinkGcRoots};
use operon_log::gc::{GcConfig, GcSource};
use operon_log::{
    LogConfig, LogReader, LogWriter, RetentionConfig, RetentionSource, SegmenterConfig,
    SegmenterSource,
};
use operon_meta::{MetaClient, MetaClientConfig, MetaConfig, MetaNode, Router, SystemClock};
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
    /// The metastore builds a snapshot after this many log entries. Default
    /// 10 000.
    pub snapshot_every: u64,
    /// How often the worker polls its task sources. Default 1 s.
    pub worker_poll_interval: Duration,
    /// The worker's task lease TTL. Default 30 s.
    pub worker_lease_ttl: Duration,
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
            snapshot_every: 10_000,
            worker_poll_interval: Duration::from_secs(1),
            worker_lease_ttl: Duration::from_secs(30),
        }
    }

    /// Rejects: segmenter.swap_deadline, link.max_commit_delay (and, from Task 13,
    /// collection.index_commit_delay) >= gc.grace. Called first thing by Server::start.
    ///
    /// Garbage collection deletes unreferenced objects older than its grace
    /// period, so a segment swap or link commit must reference its new
    /// objects strictly within it (M0.4 ruling E7, re-review m1).
    pub fn validate(&self) -> Result<(), ServerError> {
        self.gc
            .check_deadlines(&[
                ("segmenter.swap_deadline", self.segmenter.swap_deadline),
                ("link.max_commit_delay", self.link.max_commit_delay),
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
    writer: LogWriter,
    cache: RangeCache,
    worker: WorkerHandle,
    http: JoinHandle<()>,
    stop_http: oneshot::Sender<()>,
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
        let cache = RangeCache::new(store.clone(), config.cache.clone()).await?;
        let bound = async {
            let listener = tokio::net::TcpListener::bind(config.listen).await?;
            let local_addr = listener.local_addr()?;
            Ok::<_, std::io::Error>((listener, local_addr))
        }
        .await;
        let (listener, local_addr) = match bound {
            Ok(bound) => bound,
            Err(source) => {
                if let Err(err) = cache.close().await {
                    tracing::warn!(%err, "closing the cache after a failed start");
                }
                return Err(ServerError::Listen {
                    addr: config.listen,
                    source,
                });
            }
        };

        // Validated above, so this cannot fail.
        let writer = LogWriter::start(meta.clone(), store.clone(), config.log.clone())?;
        let reader = LogReader::new(meta.clone(), cache.clone());
        // Unique per process incarnation, as leases require.
        let owner = format!("node-{NODE_ID}-{}", Ulid::generate());
        let mut worker = Worker::new(
            meta.clone(),
            WorkerConfig {
                poll_interval: config.worker_poll_interval,
                lease_ttl: config.worker_lease_ttl,
                ..WorkerConfig::new(owner)
            },
        );
        worker.add_source(Arc::new(LinkApplySource::new(
            reader.clone(),
            store.clone(),
            config.link.clone(),
        )));
        worker.add_source(Arc::new(SegmenterSource::new(
            store.clone(),
            cache.clone(),
            config.segmenter.clone(),
        )));
        worker.add_source(Arc::new(RetentionSource::new(config.retention.clone())));
        worker.add_source(Arc::new(GcSource::with_roots(
            store.clone(),
            config.gc.clone(),
            vec![Arc::new(LinkGcRoots)],
        )));
        let worker = worker.start();
        let app = api::router(AppState {
            meta: meta.clone(),
            writer: writer.clone(),
            reader,
            store: store.clone(),
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
        Ok(Self {
            local_addr,
            node,
            meta,
            writer,
            cache,
            worker,
            http,
            stop_http,
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

    /// Stops accepting requests, flushes the writer (buffered appends are
    /// acknowledged), stops the worker (releasing its task leases), waits for
    /// in-flight requests (up to 10 s), and shuts the metastore down.
    pub async fn shutdown(self) -> Result<(), ServerError> {
        let _ = self.stop_http.send(());
        if let Err(err) = self.writer.shutdown().await {
            tracing::warn!(%err, "the final flush failed");
        }
        self.worker.stop().await;
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
