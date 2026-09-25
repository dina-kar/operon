//! The `operon` binary.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use operon::{Server, ServerConfig};

#[derive(Debug, Parser)]
#[command(
    name = "operon",
    version,
    about = "Operon: an object-storage-native database"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// Background-work tuning, for tests (the crash gate runs everything within
/// seconds). Hidden from `--help`.
#[derive(Debug, Default, clap::Args)]
struct Tuning {
    /// Segment WAL runs once they hold this many bytes.
    #[arg(long, hide = true)]
    segment_min_bytes: Option<u64>,
    /// Segment WAL runs whose newest record is this old.
    #[arg(long, hide = true)]
    segment_max_wal_age_ms: Option<u64>,
    /// How often the worker polls for tasks.
    #[arg(long, hide = true)]
    poll_interval_ms: Option<u64>,
    /// The worker's task lease TTL.
    #[arg(long, hide = true)]
    lease_ttl_ms: Option<u64>,
    /// How often retention runs.
    #[arg(long, hide = true)]
    retention_interval_ms: Option<u64>,
    /// Garbage collection's grace period. Also sets the segmenter's swap
    /// deadline and the link commit delay to half of it.
    #[arg(long, hide = true)]
    gc_grace_ms: Option<u64>,
    /// How often garbage collection runs.
    #[arg(long, hide = true)]
    gc_interval_ms: Option<u64>,
    /// How long a small link batch waits for more records.
    #[arg(long, hide = true)]
    link_batch_interval_ms: Option<u64>,
    /// Most records per link commit.
    #[arg(long, hide = true)]
    link_batch_records: Option<usize>,
    /// Build a metastore snapshot after this many log entries.
    #[arg(long, hide = true)]
    snapshot_every: Option<u64>,
}

impl Tuning {
    fn apply(&self, config: &mut ServerConfig) {
        let ms = Duration::from_millis;
        if let Some(v) = self.segment_min_bytes {
            config.segmenter.min_bytes = v;
        }
        if let Some(v) = self.segment_max_wal_age_ms {
            config.segmenter.max_wal_age = ms(v);
        }
        if let Some(v) = self.poll_interval_ms {
            config.worker_poll_interval = ms(v);
        }
        if let Some(v) = self.lease_ttl_ms {
            config.worker_lease_ttl = ms(v);
        }
        if let Some(v) = self.retention_interval_ms {
            config.retention.interval = ms(v);
        }
        if let Some(v) = self.gc_grace_ms {
            config.gc.grace = ms(v);
            // Freshness deadlines must stay strictly below the grace period
            // (ServerConfig::validate).
            config.segmenter.swap_deadline = ms(v / 2);
            config.link.max_commit_delay = ms(v / 2);
        }
        if let Some(v) = self.gc_interval_ms {
            config.gc.interval = ms(v);
        }
        if let Some(v) = self.link_batch_interval_ms {
            config.link.batch_interval = ms(v);
        }
        if let Some(v) = self.link_batch_records {
            config.link.batch_records = v;
        }
        if let Some(v) = self.snapshot_every {
            config.snapshot_every = v;
        }
    }
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run everything in one process, with data in a local directory.
    Dev {
        /// Holds the metastore and the local bucket.
        #[arg(long, default_value = ".operon")]
        data_dir: PathBuf,
        /// Address of the HTTP API.
        #[arg(long, default_value = "127.0.0.1:8080")]
        listen: SocketAddr,
        /// How long appends are buffered before a WAL flush.
        #[arg(long)]
        flush_interval_ms: Option<u64>,
        #[command(flatten)]
        tuning: Tuning,
    },
    /// Run everything in one process, with data in an object-store bucket.
    Standalone {
        /// Object store URL, such as s3://bucket/prefix, gs://bucket or file:///dir.
        #[arg(long)]
        bucket: String,
        /// Holds the metastore's local database.
        #[arg(long, default_value = ".operon")]
        data_dir: PathBuf,
        /// Address of the HTTP API.
        #[arg(long, default_value = "127.0.0.1:8080")]
        listen: SocketAddr,
    },
}

fn config(command: Command) -> ServerConfig {
    match command {
        Command::Dev {
            data_dir,
            listen,
            flush_interval_ms,
            tuning,
        } => {
            let mut config = ServerConfig::new(data_dir);
            config.listen = listen;
            if let Some(ms) = flush_interval_ms {
                config.log.flush_interval = Duration::from_millis(ms);
            }
            tuning.apply(&mut config);
            config
        }
        Command::Standalone {
            bucket,
            data_dir,
            listen,
        } => {
            let mut config = ServerConfig::new(data_dir);
            config.listen = listen;
            config.bucket = Some(bucket);
            config
        }
    }
}

/// Resolves on SIGINT or SIGTERM.
async fn shutdown_signal() {
    let interrupt = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = interrupt => {}
                    _ = terminate.recv() => {}
                }
            }
            Err(_) => {
                let _ = interrupt.await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = interrupt.await;
    }
}

/// The crash gate's kill -9 proxy (M0.4 plan ruling 2): with the
/// `failpoints` feature, `OPERON_FAILPOINTS="name[,name…]"` arms each named
/// failpoint to call `std::process::abort()` (no destructors, no flush) on
/// its `OPERON_FAILPOINT_HIT`-th hit (default 1).
#[cfg(feature = "failpoints")]
fn arm_failpoints() -> Result<(), String> {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    let Ok(names) = std::env::var("OPERON_FAILPOINTS") else {
        return Ok(());
    };
    let hit: u64 = match std::env::var("OPERON_FAILPOINT_HIT") {
        Ok(n) => n
            .parse()
            .map_err(|_| format!("OPERON_FAILPOINT_HIT must be a number, got {n:?}"))?,
        Err(_) => 1,
    };
    for name in names.split(',').map(str::trim).filter(|n| !n.is_empty()) {
        let hits = Arc::new(AtomicU64::new(0));
        let point = name.to_string();
        fail::cfg_callback(name, move || {
            if hits.fetch_add(1, Ordering::SeqCst) + 1 == hit {
                eprintln!("operon: failpoint {point} hit {hit} times; aborting");
                std::process::abort();
            }
        })?;
    }
    Ok(())
}

#[cfg(not(feature = "failpoints"))]
fn arm_failpoints() -> Result<(), String> {
    if std::env::var_os("OPERON_FAILPOINTS").is_some() {
        return Err(
            "OPERON_FAILPOINTS is set, but this build has no failpoints \
                    (build with --features failpoints)"
                .to_string(),
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,openraft=warn")),
        )
        .init();
    let cli = Cli::parse();
    if let Err(err) = arm_failpoints() {
        eprintln!("operon: {err}");
        return ExitCode::FAILURE;
    }
    let server = match Server::start(config(cli.command)).await {
        Ok(server) => server,
        Err(err) => {
            eprintln!("operon: {err}");
            return ExitCode::FAILURE;
        }
    };
    println!("operon listening on http://{}", server.local_addr());
    shutdown_signal().await;
    tracing::info!("shutting down");
    match server.shutdown().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("operon: shutdown failed: {err}");
            ExitCode::FAILURE
        }
    }
}
