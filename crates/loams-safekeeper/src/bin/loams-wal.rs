//! `loams-wal`: Loams's WAL service for Neon computes (§28 P4a).
//!
//! Point a compute's `neon.safekeepers` at `--listen-pg`; create timelines
//! through `--listen-http` (or let walproposer create them).
//!
//! ```text
//! LOAMS_WAL_AUTH_TOKEN=… loams-wal --trusted-network --listen-pg 10.0.0.5:5454 --listen-http 10.0.0.5:7676 \
//!          --store tikv --pd 127.0.0.1:19379 --keyspace loams_pgwal
//! ```

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use loams_safekeeper::feeder::FeederConfig;
use loams_safekeeper::http;
use loams_safekeeper::service::{WalService, WalServiceConfig};
use loams_safekeeper::store::{MemWalStore, WalStore};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum StoreKind {
    /// In memory: nothing survives a restart (tests and protocol work).
    Mem,
    /// TiKV: the production hot tier (build with --features tikv), one
    /// fenced 1PC transaction per append.
    Tikv,
    /// TiKV RawKV: blind, pipelined appends fenced once per election
    /// (§28 §7.3; build with --features tikv).
    TikvRaw,
    /// Local NVMe: Arm A's journal and acceptor metadata under --data-dir
    /// (build with --features nvme; §28 §7.2).
    Nvme,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Runtime {
    /// The WAL push runs on the tokio service.
    Tokio,
    /// The WAL push runs on compio shards (build with --features compio).
    Compio,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum UringSyncKind {
    /// fsync when the device reports no FUA, else O_DSYNC writes.
    Auto,
    Dsync,
    Fsync,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum IoKind {
    /// uring on compio, pwritev2 on tokio.
    Auto,
    /// io_uring on the compio shards (--runtime compio).
    Uring,
    /// pwrite + back-to-back fdatasync.
    Buffered,
    /// O_DIRECT + pwritev2(RWF_DSYNC) from a thread pool.
    Pwritev2,
}

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Loams's WAL service: Neon's safekeeper protocol over TiKV or local NVMe"
)]
struct Args {
    /// The Postgres-protocol listener walproposer and readers connect to.
    #[arg(long, default_value = "127.0.0.1:5454")]
    listen_pg: SocketAddr,
    /// The HTTP API.
    #[arg(long, default_value = "127.0.0.1:7676")]
    listen_http: SocketAddr,
    /// The node id walproposer sees.
    #[arg(long, default_value_t = 1)]
    id: u64,
    #[arg(long, value_enum, default_value = "mem")]
    store: StoreKind,
    /// PD endpoints, comma-separated (store tikv).
    #[arg(long, default_value = "127.0.0.1:2379")]
    pd: String,
    /// The TiKV keyspace (store tikv).
    #[arg(long, default_value = "loams_pgwal")]
    keyspace: String,
    /// Appends in flight per timeline (store tikv-raw).
    #[arg(long, default_value_t = loams_safekeeper::tikv_raw::DEFAULT_PIPELINE_DEPTH)]
    pipeline_depth: usize,
    /// Where the NVMe store keeps its journal and metadata (store nvme).
    #[arg(long, default_value = "loams-wal-data")]
    data_dir: std::path::PathBuf,
    /// The journal's durable-write tier (store nvme).
    #[arg(long, value_enum, default_value = "auto")]
    io: IoKind,
    /// Flush units in flight at once (store nvme, direct tiers).
    #[arg(long, default_value_t = 4)]
    io_depth: usize,
    /// The WAL push's runtime (store nvme).
    #[arg(long, value_enum, default_value = "tokio")]
    runtime: Runtime,
    /// compio shards, each with its own journal (fixed at first start).
    #[arg(long, default_value_t = 1)]
    shards: usize,
    /// How the uring tier makes a write durable.
    #[arg(long, value_enum, default_value = "auto")]
    uring_sync: UringSyncKind,
    /// Run the rings with SQPOLL (a kernel thread polls submissions).
    #[arg(long)]
    uring_sqpoll: bool,
    /// SQPOLL's idle time before the poller sleeps, in ms.
    #[arg(long, default_value_t = 50)]
    uring_sqpoll_idle_ms: u64,
    /// Journal segment size in MiB (store nvme).
    #[arg(long, default_value_t = 64)]
    segment_mb: u64,
    /// How often a heartbeat-only commit LSN is persisted, in ms.
    #[arg(long, default_value_t = 1000)]
    commit_flush_ms: u64,
    /// Feed committed WAL to this stock safekeeper (`host:port`), which
    /// serves the pageserver: the interim path until the WAL service speaks
    /// the interpreted protocol itself.
    #[arg(long)]
    feed_safekeeper: Option<String>,
    /// Clients must send this as their password (walproposer's
    /// NEON_AUTH_TOKEN) and as the HTTP bearer token. Required unless both
    /// listeners are on loopback.
    #[arg(long, env = "LOAMS_WAL_AUTH_TOKEN", hide_env_values = true)]
    auth_token: Option<String>,
    /// Acknowledge that non-loopback listeners are on an encrypted private
    /// network or tunnel: loams-wal has no TLS yet, and the token travels in
    /// cleartext on its own connections.
    #[arg(long)]
    trusted_network: bool,
}

type OnStart<S> = Box<dyn FnOnce(Arc<WalService<S>>) -> Result<(), loams_safekeeper::Error>>;

async fn run<S: WalStore>(store: Arc<S>, args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    run_with(store, args, None, None).await
}

async fn run_with<S: WalStore>(
    store: Arc<S>,
    args: &Args,
    handoff: Option<loams_safekeeper::service::Handoff>,
    on_start: Option<OnStart<S>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let loopback = args.listen_pg.ip().is_loopback() && args.listen_http.ip().is_loopback();
    if !loopback {
        // The token travels as a cleartext password and bearer token: loams-wal
        // has no TLS yet, so beyond loopback it must be on a network the
        // operator vouches for (a private link, WireGuard, a service mesh with
        // mTLS). Both a token and that explicit acknowledgement are required.
        if args.auth_token.is_none() {
            return Err("refusing to listen beyond loopback without --auth-token".into());
        }
        if !args.trusted_network {
            return Err(
                "refusing to send the auth token in cleartext beyond loopback: loams-wal \
                        has no TLS yet; pass --trusted-network only on an encrypted private \
                        network or tunnel"
                    .into(),
            );
        }
    }
    let svc = WalService::new(
        store,
        WalServiceConfig {
            node_id: args.id,
            commit_flush_interval: Duration::from_millis(args.commit_flush_ms),
            feeder: args.feed_safekeeper.clone().map(|safekeeper| FeederConfig {
                safekeeper,
                retry: Duration::from_secs(1),
                poll: Duration::from_millis(5),
            }),
            auth_token: args.auth_token.clone(),
            handoff,
            ..WalServiceConfig::default()
        },
    );
    if let Some(f) = on_start {
        f(svc.clone())?;
    }
    let pg = tokio::net::TcpListener::bind(args.listen_pg).await?;
    let web = tokio::net::TcpListener::bind(args.listen_http).await?;
    tracing::info!(pg = %args.listen_pg, http = %args.listen_http, store = ?args.store, "loams-wal listening");
    let app = http::router(svc.clone());
    let http = tokio::spawn(async move { axum::serve(web, app).await });
    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    svc.serve(pg, shutdown).await?;
    http.abort();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    match args.store {
        StoreKind::Mem => run(Arc::new(MemWalStore::new()), &args).await,
        StoreKind::Nvme => {
            #[cfg(feature = "nvme")]
            {
                run_nvme(&args).await
            }
            #[cfg(not(feature = "nvme"))]
            {
                Err("this loams-wal was built without the nvme feature".into())
            }
        }
        StoreKind::Tikv | StoreKind::TikvRaw => {
            #[cfg(feature = "tikv")]
            {
                let pd = args.pd.split(',').map(|s| s.trim().to_string()).collect();
                let config = loams_tikv::TikvConfig::new(pd, args.keyspace.clone());
                if matches!(args.store, StoreKind::TikvRaw) {
                    let kv = loams_safekeeper::tikv_raw::TikvRawKv::connect(config).await?;
                    run(Arc::new(kv.into_store(args.pipeline_depth)), &args).await
                } else {
                    let tikv = loams_tikv::Tikv::connect(config).await?;
                    run(
                        Arc::new(loams_safekeeper::tikv::TikvWalStore::new(tikv)),
                        &args,
                    )
                    .await
                }
            }
            #[cfg(not(feature = "tikv"))]
            {
                Err("this loams-wal was built without the tikv feature".into())
            }
        }
    }
}

#[cfg(feature = "nvme")]
async fn run_nvme(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    use loams_safekeeper::journal::segments::{DeviceCaps, FsKind};
    use loams_safekeeper::journal::{JournalConfig, Tier};
    use loams_safekeeper::nvme::NvmeWalStore;
    std::fs::create_dir_all(&args.data_dir)?;
    let fs = FsKind::of(&args.data_dir)?;
    let caps = DeviceCaps::of(&args.data_dir);
    let depth = args.io_depth;
    let tier = match (args.io, args.runtime) {
        (IoKind::Buffered, _) => Tier::Buffered,
        (IoKind::Pwritev2, _) | (IoKind::Auto, Runtime::Tokio) => Tier::Pwritev2 { depth },
        (IoKind::Uring, Runtime::Tokio) => {
            return Err("--io uring needs --runtime compio".into());
        }
        (IoKind::Uring | IoKind::Auto, Runtime::Compio) => Tier::Uring { depth },
    };
    tracing::info!(dir = %args.data_dir.display(), ?fs, ?caps, requested = ?args.io, runtime = ?args.runtime, ?tier, "I/O tier");
    let meta: Arc<dyn loams_safekeeper::meta::MetaStore> = Arc::new(
        loams_safekeeper::meta::LocalMeta::open(&args.data_dir.join("meta"))?,
    );
    let journal = |dir: std::path::PathBuf| JournalConfig {
        segment_size: args.segment_mb << 20,
        ..JournalConfig::new(dir, tier)
    };
    match args.runtime {
        Runtime::Tokio => {
            if args.data_dir.join("SHARDS").exists() {
                return Err(format!(
                    "{} was made by --runtime compio; start it with --runtime compio",
                    args.data_dir.display()
                )
                .into());
            }
            let store = NvmeWalStore::open(journal(args.data_dir.join("journal")), meta).await?;
            tracing::info!(direct = store.journal().direct(), "journal ready");
            run(Arc::new(store), args).await
        }
        Runtime::Compio => {
            #[cfg(feature = "compio")]
            {
                use loams_safekeeper::shard::{
                    ShardConfig, ShardedStore, Shards, UringSync, check_shard_count, shard_of,
                };
                let n = args.shards.max(1);
                check_shard_count(&args.data_dir, n)?;
                let mut stores = Vec::new();
                for k in 0..n {
                    let cfg = journal(args.data_dir.join(format!("shard-{k}")).join("journal"));
                    let s = NvmeWalStore::open_owning(cfg, meta.clone(), |tl| shard_of(tl, n) == k)
                        .await?;
                    stores.push(Arc::new(s));
                }
                let sync = match args.uring_sync {
                    UringSyncKind::Dsync => UringSync::Dsync,
                    UringSyncKind::Fsync => UringSync::Fsync,
                    UringSyncKind::Auto if caps.fua == Some(false) => UringSync::Fsync,
                    UringSyncKind::Auto => UringSync::Dsync,
                };
                let cfg = ShardConfig {
                    shards: n,
                    depth,
                    sync,
                    uring: matches!(tier, Tier::Uring { .. }),
                    sqpoll: args
                        .uring_sqpoll
                        .then(|| Duration::from_millis(args.uring_sqpoll_idle_ms)),
                    node_id: args.id,
                    commit_flush_interval: Duration::from_millis(args.commit_flush_ms),
                };
                tracing::info!(
                    ?cfg,
                    direct = stores[0].journal().direct(),
                    "journal ready (compio shards)"
                );
                let (shards, starter) = Shards::start(stores.clone(), cfg)?;
                let store = Arc::new(ShardedStore::new(stores));
                let on_start: OnStart<ShardedStore> = Box::new(move |svc| starter.run(svc));
                run_with(store, args, Some(shards.handoff()), Some(on_start)).await
            }
            #[cfg(not(feature = "compio"))]
            {
                Err("this loams-wal was built without the compio feature".into())
            }
        }
    }
}
