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
    /// Garbage collection's grace period. Also sets every freshness deadline
    /// to half of it: the segmenter's swap deadline, the link (and
    /// collection) commit delay and the collection index commit delay.
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
    /// Whether collections' implicit streams are trimmed.
    #[arg(long, hide = true, action = clap::ArgAction::Set)]
    collection_trim: Option<bool>,
    /// Rows before a collection's first vector index is built.
    #[arg(long, hide = true)]
    collection_index_min_rows: Option<u64>,
    /// Unindexed rows before a delta vector index segment is built.
    #[arg(long, hide = true)]
    collection_index_delta_min_rows: Option<u64>,
    /// How long a superseded collection manifest stays readable.
    #[arg(long, hide = true)]
    collection_retention_ms: Option<u64>,
    /// How often an idle collection is checked for index work.
    #[arg(long, hide = true)]
    collection_index_poll_interval_ms: Option<u64>,
    /// Most bytes one collection's tail index holds.
    #[arg(long, hide = true)]
    tail_max_bytes: Option<usize>,
    /// How long strong and at-least-token reads wait for the tail.
    #[arg(long, hide = true)]
    consistency_wait_ms: Option<u64>,
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
            config.collection.max_commit_delay = ms(v / 2);
            config.collection.index_commit_delay = ms(v / 2);
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
        if let Some(v) = self.collection_trim {
            config.collection.trim = v;
        }
        if let Some(v) = self.collection_index_min_rows {
            config.collection.index_min_rows = v;
        }
        if let Some(v) = self.collection_index_delta_min_rows {
            config.collection.index_delta_min_rows = v;
        }
        if let Some(v) = self.collection_retention_ms {
            config.collection.time_travel_retention = ms(v);
        }
        if let Some(v) = self.collection_index_poll_interval_ms {
            config.collection.index_poll_interval = ms(v);
        }
        if let Some(v) = self.tail_max_bytes {
            config.query.tail.max_bytes = v;
        }
        if let Some(v) = self.consistency_wait_ms {
            config.query.read.consistency_wait = ms(v);
        }
    }
}

/// `--hot on|off`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
enum HotSwitch {
    On,
    Off,
}

/// The native surfaces beside the HTTP API, shared by `dev` and
/// `standalone`.
#[derive(Debug, clap::Args)]
struct Native {
    /// Address of the Arrow Flight SQL listener [default: 127.0.0.1:8082
    /// for dev, 0.0.0.0:8082 for standalone].
    #[arg(long, conflicts_with = "no_flight_sql")]
    flight_sql_listen: Option<SocketAddr>,
    /// Serve no Flight SQL.
    #[arg(long)]
    no_flight_sql: bool,
    /// Whether reads use the hot tier when a request does not say
    /// (`Operon-Hot`).
    #[arg(long, value_enum, default_value = "on")]
    hot: HotSwitch,
}

impl Native {
    fn apply(&self, config: &mut ServerConfig, default_flight: SocketAddr) {
        config.flight_sql = if self.no_flight_sql {
            None
        } else {
            Some(self.flight_sql_listen.unwrap_or(default_flight))
        };
        config.query.hot_default = self.hot == HotSwitch::On;
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
        native: Native,
        #[command(flatten)]
        tuning: Box<Tuning>,
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
        #[command(flatten)]
        native: Native,
    },
}

/// Flight SQL's default address for `operon dev`.
const DEV_FLIGHT_SQL: SocketAddr = SocketAddr::V4(std::net::SocketAddrV4::new(
    std::net::Ipv4Addr::LOCALHOST,
    8082,
));
/// Flight SQL's default address for `operon standalone`.
const STANDALONE_FLIGHT_SQL: SocketAddr = SocketAddr::V4(std::net::SocketAddrV4::new(
    std::net::Ipv4Addr::UNSPECIFIED,
    8082,
));

fn config(command: Command) -> ServerConfig {
    match command {
        Command::Dev {
            data_dir,
            listen,
            flush_interval_ms,
            native,
            tuning,
        } => {
            let mut config = ServerConfig::new(data_dir);
            config.listen = listen;
            if let Some(ms) = flush_interval_ms {
                config.log.flush_interval = Duration::from_millis(ms);
            }
            native.apply(&mut config, DEV_FLIGHT_SQL);
            tuning.apply(&mut config);
            config
        }
        Command::Standalone {
            bucket,
            data_dir,
            listen,
            native,
        } => {
            let mut config = ServerConfig::new(data_dir);
            config.listen = listen;
            config.bucket = Some(bucket);
            native.apply(&mut config, STANDALONE_FLIGHT_SQL);
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
    // M1.6 W14, M1.7 A4: printed once the listener is bound.
    if let Some(addr) = server.flight_sql_addr() {
        println!("operon flight sql listening on grpc://{addr}");
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn dev_config(args: &[&str]) -> ServerConfig {
        let cli = Cli::try_parse_from(["operon", "dev"].iter().chain(args)).expect("parse");
        config(cli.command)
    }

    /// Ruling 22, controller ruling P2: `--gc-grace-ms` lowers every
    /// freshness deadline to half of it, so the config still validates.
    #[test]
    fn gc_grace_lowers_every_deadline_to_half() {
        let config = dev_config(&["--gc-grace-ms", "1500"]);
        let half = Duration::from_millis(750);
        assert_eq!(config.gc.grace, Duration::from_millis(1500));
        assert_eq!(config.segmenter.swap_deadline, half);
        assert_eq!(config.link.max_commit_delay, half);
        assert_eq!(config.collection.max_commit_delay, half);
        assert_eq!(config.collection.index_commit_delay, half);
        config.validate().expect("valid");
    }

    #[test]
    fn flight_sql_and_hot_flags_set_the_config() {
        let config = dev_config(&[]);
        assert_eq!(config.flight_sql, Some("127.0.0.1:8082".parse().unwrap()));
        assert!(config.query.hot_default);
        let config = dev_config(&["--flight-sql-listen", "127.0.0.1:9000", "--hot=off"]);
        assert_eq!(config.flight_sql, Some("127.0.0.1:9000".parse().unwrap()));
        assert!(!config.query.hot_default);
        let config = dev_config(&["--no-flight-sql", "--hot", "on"]);
        assert_eq!(config.flight_sql, None);
        assert!(config.query.hot_default);
        assert!(
            Cli::try_parse_from([
                "operon",
                "dev",
                "--no-flight-sql",
                "--flight-sql-listen",
                "127.0.0.1:1"
            ])
            .is_err()
        );
        assert!(Cli::try_parse_from(["operon", "dev", "--hot", "maybe"]).is_err());
        let cli = Cli::try_parse_from(["operon", "standalone", "--bucket", "file:///tmp/b"])
            .expect("parse");
        assert_eq!(
            config_of(cli).flight_sql,
            Some("0.0.0.0:8082".parse().unwrap())
        );
        let config = dev_config(&["--tail-max-bytes", "4096", "--consistency-wait-ms", "250"]);
        assert_eq!(config.query.tail.max_bytes, 4096);
        assert_eq!(
            config.query.read.consistency_wait,
            Duration::from_millis(250)
        );
    }

    fn config_of(cli: Cli) -> ServerConfig {
        config(cli.command)
    }

    #[test]
    fn collection_tuning_flags_set_the_collection_config() {
        let config = dev_config(&[
            "--collection-trim",
            "false",
            "--collection-index-min-rows",
            "40",
            "--collection-index-delta-min-rows",
            "41",
            "--collection-retention-ms",
            "0",
            "--collection-index-poll-interval-ms",
            "200",
        ]);
        assert!(!config.collection.trim);
        assert_eq!(config.collection.index_min_rows, 40);
        assert_eq!(config.collection.index_delta_min_rows, 41);
        assert_eq!(config.collection.time_travel_retention, Duration::ZERO);
        assert_eq!(
            config.collection.index_poll_interval,
            Duration::from_millis(200)
        );
        assert!(dev_config(&["--collection-trim", "true"]).collection.trim);
    }
}
