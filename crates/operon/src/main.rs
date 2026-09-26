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
    /// How often the hot tier reconciles (M1.3).
    #[arg(long, hide = true)]
    hot_reconcile_interval_ms: Option<u64>,
    /// How long a stale hot artifact may wait for its rebuild.
    #[arg(long, hide = true)]
    hot_rebuild_max_staleness_ms: Option<u64>,
    /// Inserted rows that make a stale hot artifact due for a rebuild.
    #[arg(long, hide = true)]
    hot_rebuild_min_inserted: Option<u64>,
    /// How often an unchanged hot column is checked for a build.
    #[arg(long, hide = true)]
    hot_build_poll_interval_ms: Option<u64>,
    /// Whether split merges and Lance compaction run.
    #[arg(long, hide = true, value_enum)]
    maintenance: Option<HotSwitch>,
    /// How often an unchanged collection is checked for maintenance.
    #[arg(long, hide = true)]
    merge_poll_interval_ms: Option<u64>,
    /// The merge policy's smallest level, in docs.
    #[arg(long, hide = true)]
    merge_min_level_docs: Option<usize>,
    /// Small Lance fragments before a compaction runs.
    #[arg(long, hide = true)]
    compaction_min_small_fragments: Option<usize>,
    /// Rows per compacted Lance fragment.
    #[arg(long, hide = true)]
    compaction_target_rows: Option<usize>,
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
        if let Some(v) = self.hot_reconcile_interval_ms {
            config.hot.reconcile_interval = ms(v);
        }
        if let Some(v) = self.hot_rebuild_max_staleness_ms {
            config.hot_build.rebuild_max_staleness = ms(v);
        }
        if let Some(v) = self.hot_rebuild_min_inserted {
            config.hot_build.rebuild_min_inserted = v;
        }
        if let Some(v) = self.hot_build_poll_interval_ms {
            config.hot_build.poll_interval = ms(v);
        }
        if let Some(switch) = self.maintenance {
            let on = switch == HotSwitch::On;
            config.maintenance.merge = on;
            config.maintenance.compaction = on;
        }
        if let Some(v) = self.merge_poll_interval_ms {
            config.maintenance.poll_interval = ms(v);
        }
        if let Some(v) = self.merge_min_level_docs {
            config.maintenance.merge_policy.min_level_num_docs = v;
        }
        if let Some(v) = self.compaction_min_small_fragments {
            config.maintenance.compaction_min_small_fragments = v;
        }
        if let Some(v) = self.compaction_target_rows {
            config.maintenance.compaction_target_rows = v;
        }
    }
}

/// `--hot on|off` (and `--maintenance on|off`).
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
    /// Whether this node runs a hot tier, and whether reads use it when a
    /// request does not say (`Operon-Hot`).
    #[arg(long, value_enum, default_value = "on")]
    hot: HotSwitch,
    /// Make every collection's vectors and text hot on this process, without
    /// a catalog change.
    #[arg(long)]
    hot_pin_all: bool,
    /// Local directory of the hot tier [default: <data-dir>/hot].
    #[arg(long)]
    hot_dir: Option<PathBuf>,
    /// Local disk the hot tier may use, in bytes [default: 100 GiB].
    #[arg(long)]
    hot_nvme_bytes: Option<u64>,
    /// Memory the hot tier may use, in bytes [default: 8 GiB].
    #[arg(long)]
    hot_ram_bytes: Option<u64>,
}

impl Native {
    fn apply(&self, config: &mut ServerConfig, default_flight: SocketAddr) {
        config.flight_sql = if self.no_flight_sql {
            None
        } else {
            Some(self.flight_sql_listen.unwrap_or(default_flight))
        };
        let hot = self.hot == HotSwitch::On;
        config.query.hot_default = hot;
        config.hot.enabled = hot;
        config.hot.pin_all = self.hot_pin_all;
        if let Some(dir) = &self.hot_dir {
            config.hot.dir = dir.clone();
        }
        if let Some(v) = self.hot_nvme_bytes {
            config.hot.nvme_bytes = v;
        }
        if let Some(v) = self.hot_ram_bytes {
            config.hot.ram_bytes = v;
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
    /// Warm a collection on the node that owns it (`POST …/warm`).
    Warm {
        /// `<namespace>/<collection>`.
        target: String,
        /// The server to ask.
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        server: String,
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
        Command::Warm { .. } => unreachable!("operon warm starts no server"),
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

/// `operon warm <namespace>/<collection>`: POSTs the warm request, prints
/// the response body, and exits 0 on a 2xx status, else 1 with the error
/// message on stderr.
async fn warm(target: &str, server: &str) -> ExitCode {
    let Some((ns, collection)) = target.split_once('/') else {
        eprintln!("operon: expected <namespace>/<collection>, got {target:?}");
        return ExitCode::FAILURE;
    };
    let url = format!(
        "{}/v1/namespaces/{ns}/collections/{collection}/warm",
        server.trim_end_matches('/')
    );
    let response = match reqwest::Client::new()
        .post(&url)
        .json(&serde_json::json!({}))
        .send()
        .await
    {
        Ok(response) => response,
        Err(err) => {
            eprintln!("operon: {url}: {err}");
            return ExitCode::FAILURE;
        }
    };
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if status.is_success() {
        println!("{body}");
        return ExitCode::SUCCESS;
    }
    let message = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|value| value["message"].as_str().map(str::to_string))
        .unwrap_or(body);
    eprintln!("operon: {status}: {message}");
    ExitCode::FAILURE
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
    if let Command::Warm { target, server } = &cli.command {
        return warm(target, server).await;
    }
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
    fn hot_and_maintenance_flags_set_the_config() {
        let config = dev_config(&[]);
        assert!(config.hot.enabled && !config.hot.pin_all);
        assert!(config.maintenance.merge && config.maintenance.compaction);
        let config = dev_config(&[
            "--hot=off",
            "--hot-pin-all",
            "--hot-dir",
            "/tmp/h",
            "--hot-nvme-bytes",
            "1000",
            "--hot-ram-bytes",
            "2000",
            "--hot-reconcile-interval-ms",
            "50",
            "--hot-rebuild-max-staleness-ms",
            "500",
            "--hot-rebuild-min-inserted",
            "7",
            "--hot-build-poll-interval-ms",
            "100",
            "--maintenance",
            "off",
            "--merge-poll-interval-ms",
            "30",
            "--merge-min-level-docs",
            "5",
            "--compaction-min-small-fragments",
            "4",
            "--compaction-target-rows",
            "99",
        ]);
        assert!(!config.hot.enabled && !config.query.hot_default);
        assert!(config.hot.pin_all);
        assert_eq!(config.hot.dir, PathBuf::from("/tmp/h"));
        assert_eq!((config.hot.nvme_bytes, config.hot.ram_bytes), (1000, 2000));
        assert_eq!(config.hot.reconcile_interval, Duration::from_millis(50));
        assert_eq!(
            config.hot_build.rebuild_max_staleness,
            Duration::from_millis(500)
        );
        assert_eq!(config.hot_build.rebuild_min_inserted, 7);
        assert_eq!(config.hot_build.poll_interval, Duration::from_millis(100));
        assert!(!config.maintenance.merge && !config.maintenance.compaction);
        assert_eq!(config.maintenance.poll_interval, Duration::from_millis(30));
        assert_eq!(config.maintenance.merge_policy.min_level_num_docs, 5);
        assert_eq!(config.maintenance.compaction_min_small_fragments, 4);
        assert_eq!(config.maintenance.compaction_target_rows, 99);
        let cli = Cli::try_parse_from([
            "operon",
            "standalone",
            "--bucket",
            "file:///tmp/b",
            "--hot-pin-all",
        ])
        .expect("parse");
        assert!(config_of(cli).hot.pin_all);
        let cli = Cli::try_parse_from(["operon", "warm", "acme/docs"]).expect("parse");
        assert!(matches!(
            cli.command,
            Command::Warm { ref target, ref server }
                if target == "acme/docs" && server == "http://127.0.0.1:8080"
        ));
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
