//! `loam-wal`: Loam's WAL service for Neon computes (§28 P4a).
//!
//! Point a compute's `neon.safekeepers` at `--listen-pg`; create timelines
//! through `--listen-http` (or let walproposer create them).
//!
//! ```text
//! LOAM_WAL_AUTH_TOKEN=… loam-wal --trusted-network --listen-pg 10.0.0.5:5454 --listen-http 10.0.0.5:7676 \
//!          --store tikv --pd 127.0.0.1:19379 --keyspace loam_pgwal
//! ```

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use operon_safekeeper::feeder::FeederConfig;
use operon_safekeeper::http;
use operon_safekeeper::service::{WalService, WalServiceConfig};
use operon_safekeeper::store::{MemWalStore, WalStore};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum StoreKind {
    /// In memory: nothing survives a restart (tests and protocol work).
    Mem,
    /// TiKV: the production hot tier (build with --features tikv).
    Tikv,
}

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Loam's WAL service: Neon's safekeeper protocol over TiKV"
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
    #[arg(long, default_value = "loam_pgwal")]
    keyspace: String,
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
    #[arg(long, env = "LOAM_WAL_AUTH_TOKEN", hide_env_values = true)]
    auth_token: Option<String>,
    /// Acknowledge that non-loopback listeners are on an encrypted private
    /// network or tunnel: loam-wal has no TLS yet, and the token travels in
    /// cleartext on its own connections.
    #[arg(long)]
    trusted_network: bool,
}

async fn run<S: WalStore>(store: Arc<S>, args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let loopback = args.listen_pg.ip().is_loopback() && args.listen_http.ip().is_loopback();
    if !loopback {
        // The token travels as a cleartext password and bearer token: loam-wal
        // has no TLS yet, so beyond loopback it must be on a network the
        // operator vouches for (a private link, WireGuard, a service mesh with
        // mTLS). Both a token and that explicit acknowledgement are required.
        if args.auth_token.is_none() {
            return Err("refusing to listen beyond loopback without --auth-token".into());
        }
        if !args.trusted_network {
            return Err(
                "refusing to send the auth token in cleartext beyond loopback: loam-wal \
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
            ..WalServiceConfig::default()
        },
    );
    let pg = tokio::net::TcpListener::bind(args.listen_pg).await?;
    let web = tokio::net::TcpListener::bind(args.listen_http).await?;
    tracing::info!(pg = %args.listen_pg, http = %args.listen_http, store = ?args.store, "loam-wal listening");
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
        StoreKind::Tikv => {
            #[cfg(feature = "tikv")]
            {
                let pd = args.pd.split(',').map(|s| s.trim().to_string()).collect();
                let tikv = operon_tikv::Tikv::connect(operon_tikv::TikvConfig::new(
                    pd,
                    args.keyspace.clone(),
                ))
                .await?;
                run(
                    Arc::new(operon_safekeeper::tikv::TikvWalStore::new(tikv)),
                    &args,
                )
                .await
            }
            #[cfg(not(feature = "tikv"))]
            {
                Err("this loam-wal was built without the tikv feature".into())
            }
        }
    }
}
