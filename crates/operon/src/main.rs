//! The `operon` binary.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use std::collections::BTreeMap;

use clap::{Parser, Subcommand};
use operon::{ClusterConfig, MetaBackend, Server, ServerConfig};
use operon_hot::Roles;

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
    /// collection) commit delay, the collection index commit delay, the
    /// maintenance commit delay and the hot artifact commit delay.
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
            config.maintenance.commit_delay = ms(v / 2);
            config.hot_build.artifact_commit_delay = ms(v / 2);
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
            // The byte budget stays at most half the tail (Task 15 rule 8;
            // row 15.5).
            let budget = &mut config.query.backpressure.max_unapplied_bytes;
            *budget = (*budget).min((v / 2) as u64);
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

/// `--hot on|off` (and `--maintenance on|off`, `--backpressure on|off`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
enum HotSwitch {
    On,
    Off,
}

/// The surfaces beside the HTTP API, shared by `dev`, `standalone` and
/// `cluster`.
#[derive(Debug, clap::Args)]
struct Native {
    /// Address of the Arrow Flight SQL listener [default: 127.0.0.1:8082
    /// for dev, 0.0.0.0:8082 for standalone].
    #[arg(long, conflicts_with = "no_flight_sql")]
    flight_sql_listen: Option<SocketAddr>,
    /// Serve no Flight SQL.
    #[arg(long)]
    no_flight_sql: bool,
    /// Address of the Qdrant REST API [default: 127.0.0.1:6333].
    #[arg(long, conflicts_with = "no_qdrant")]
    qdrant_listen: Option<SocketAddr>,
    /// Address of the Qdrant gRPC API [default: 127.0.0.1:6334].
    #[arg(long, conflicts_with = "no_qdrant")]
    qdrant_grpc_listen: Option<SocketAddr>,
    /// The namespace of Qdrant requests without an `Operon-Namespace`
    /// header [default: default].
    #[arg(long, conflicts_with = "no_qdrant")]
    qdrant_namespace: Option<String>,
    /// Serve no Qdrant API.
    #[arg(long)]
    no_qdrant: bool,
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
    /// Whether collection writes are refused (429) while a collection's
    /// unapplied data is over its budget.
    #[arg(long, value_enum, default_value = "on")]
    backpressure: HotSwitch,
    /// Unapplied records per collection before writes are refused
    /// [default: 1000000].
    #[arg(long)]
    max_unapplied_records: Option<u64>,
    /// Unapplied bytes per collection before writes are refused, at most
    /// half the tail's bound [default: 128 MiB].
    #[arg(long)]
    max_unapplied_bytes: Option<u64>,
}

impl Native {
    fn apply(&self, config: &mut ServerConfig, default_flight: SocketAddr) {
        config.flight_sql = if self.no_flight_sql {
            None
        } else {
            Some(self.flight_sql_listen.unwrap_or(default_flight))
        };
        self.apply_qdrant(config);
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
        let backpressure = &mut config.query.backpressure;
        backpressure.enabled = self.backpressure == HotSwitch::On;
        if let Some(v) = self.max_unapplied_records {
            backpressure.max_unapplied_records = v;
        }
        if let Some(v) = self.max_unapplied_bytes {
            backpressure.max_unapplied_bytes = v;
        }
    }

    /// The Qdrant gateway, unless `--no-qdrant` (plan M1.4 Task 2, E12).
    #[cfg(feature = "qdrant")]
    fn apply_qdrant(&self, config: &mut ServerConfig) {
        config.qdrant = (!self.no_qdrant).then(|| {
            let mut qdrant = operon_qdrant::QdrantConfig::default();
            if let Some(addr) = self.qdrant_listen {
                qdrant.rest_listen = addr;
            }
            if let Some(addr) = self.qdrant_grpc_listen {
                qdrant.grpc_listen = addr;
            }
            if let Some(ns) = &self.qdrant_namespace {
                qdrant.namespace = ns.clone();
            }
            qdrant
        });
    }

    #[cfg(not(feature = "qdrant"))]
    fn apply_qdrant(&self, _config: &mut ServerConfig) {
        if self.qdrant_listen.is_some() || self.qdrant_grpc_listen.is_some() {
            tracing::warn!("this build has no Qdrant API (the qdrant feature is off)");
        }
    }
}

/// Loam Live (R1 plan Task 12, feature `live`), on `dev` and `standalone`.
#[cfg(feature = "live")]
#[derive(Debug, clap::Args)]
struct LiveArgs {
    /// Address of the Loam Live sync API; loopback only (127.0.0.0/8, ::1,
    /// localhost), since the Live API has no authentication in R1 (D111).
    #[arg(long, default_value = "127.0.0.1:7710", value_parser = parse_live_listen)]
    live_listen: SocketAddr,
    /// PD endpoints of the Live cluster, comma-separated [dev default: the
    /// playground's 127.0.0.1:19379; standalone: required unless
    /// --no-live].
    #[arg(long, value_delimiter = ',')]
    live_pd: Vec<String>,
    /// The Live app's keyspace [default: loam_live_<app>].
    #[arg(long)]
    live_keyspace: Option<String>,
    /// The Live app.
    #[arg(long, default_value = "dev", value_parser = parse_live_app)]
    live_app: String,
    /// How far behind a fresh TSO timestamp each subscription tick reads,
    /// in milliseconds (R1 plan rows T12-1, T13-1).
    #[arg(long, default_value_t = 200)]
    live_tick_read_lag_ms: u64,
    /// A key prefix inside the Live keyspace, in hex [default: none]. Tests
    /// isolate by it (R1 Ruling 1), as `--meta tikv://…?root=<hex>` does.
    #[arg(long, value_parser = parse_live_root)]
    live_root: Option<LiveRoot>,
    /// QuickJS contexts per deployment, each with its own runtime and
    /// worker thread, so up to this many × 64 MiB per deployment (R1 plan
    /// rows T13-5, T14-1).
    #[arg(long, default_value_t = operon_live_js::DEFAULT_CONTEXTS, value_parser = parse_live_js_contexts)]
    live_js_contexts: usize,
    /// Serve no Loam Live API.
    #[arg(
        long,
        conflicts_with_all = ["live_listen", "live_pd", "live_keyspace", "live_app", "live_tick_read_lag_ms", "live_root", "live_js_contexts"]
    )]
    no_live: bool,
}

/// `operon dev`'s Live PD when `--live-pd` is absent: the dev playground's
/// (owner ruling, R1 plan row T13-2). `operon standalone` has none.
#[cfg(feature = "live")]
const DEV_LIVE_PD: &str = "127.0.0.1:19379";

#[cfg(feature = "live")]
impl LiveArgs {
    /// Sets `config.live`; `default_pd` is used when `--live-pd` is absent
    /// (dev), and without either the PD list stays empty, which
    /// `ServerConfig::validate` refuses (standalone).
    fn apply(&self, config: &mut ServerConfig, default_pd: Option<&str>) {
        if self.no_live {
            config.live = None;
            return;
        }
        let keyspace = self
            .live_keyspace
            .clone()
            .unwrap_or_else(|| operon_live::keyspace_of(&self.live_app));
        let pd = if self.live_pd.is_empty() {
            default_pd.map(str::to_string).into_iter().collect()
        } else {
            self.live_pd.clone()
        };
        let mut tikv = operon_tikv::TikvConfig::new(pd, keyspace);
        if let Some(LiveRoot(root)) = &self.live_root {
            tikv.root.clone_from(root);
        }
        let mut live = operon_live::LiveConfig::with_tikv(&self.live_app, tikv);
        live.listen = self.live_listen;
        live.subs.tick_read_lag = Duration::from_millis(self.live_tick_read_lag_ms);
        live.engine = Some(std::sync::Arc::new(operon_live_js::JsEngine::new(
            operon_live_js::JsConfig {
                contexts: self.live_js_contexts,
                ..operon_live_js::JsConfig::default()
            },
        )));
        config.live = Some(live);
    }
}

/// `--live-js-contexts`: 1 to 256.
#[cfg(feature = "live")]
fn parse_live_js_contexts(value: &str) -> Result<usize, String> {
    match value.parse::<usize>() {
        Ok(n @ 1..=256) => Ok(n),
        _ => Err(format!(
            "--live-js-contexts {value:?}: a number from 1 to 256"
        )),
    }
}

/// `--live-root`'s bytes (a newtype, so clap takes one value, not a list).
#[cfg(feature = "live")]
#[derive(Debug, Clone)]
struct LiveRoot(Vec<u8>);

/// `--live-root`: an even number of hex digits.
#[cfg(feature = "live")]
fn parse_live_root(value: &str) -> Result<LiveRoot, String> {
    let bad = || format!("--live-root {value:?}: not an even number of hex digits");
    if value.is_empty() || !value.len().is_multiple_of(2) {
        return Err(bad());
    }
    (0..value.len())
        .step_by(2)
        .map(|i| {
            value
                .get(i..i + 2)
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                .ok_or_else(bad)
        })
        .collect::<Result<_, _>>()
        .map(LiveRoot)
}

/// `--live-app`: a Live app name (the catalog's name rules).
#[cfg(feature = "live")]
fn parse_live_app(value: &str) -> Result<String, String> {
    operon_live::catalog::check_name("app", value)
        .map(|()| value.to_string())
        .map_err(|err| format!("--live-app {value:?}: {err}"))
}

/// `--live-listen`: an `ip:port`, or `localhost:<port>` (127.0.0.1). The
/// loopback check runs at startup, with the error of design §20 §7.1.
#[cfg(feature = "live")]
fn parse_live_listen(value: &str) -> Result<SocketAddr, String> {
    if let Some(port) = value.strip_prefix("localhost:") {
        let port: u16 = port
            .parse()
            .map_err(|_| format!("--live-listen {value:?}: the port is not a number"))?;
        return Ok(SocketAddr::from(([127, 0, 0, 1], port)));
    }
    value
        .parse()
        .map_err(|_| format!("--live-listen {value:?}: expected ip:port or localhost:port"))
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
        /// The metastore: tikv://<pd-host:port>[,<pd…>]/<keyspace> runs it on
        /// TiKV [default: the embedded store in --data-dir].
        #[arg(long, value_parser = MetaBackend::parse)]
        meta: Option<MetaBackend>,
        #[command(flatten)]
        native: Native,
        #[cfg(feature = "live")]
        #[command(flatten)]
        live: LiveArgs,
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
        /// The metastore: tikv://<pd-host:port>[,<pd…>]/<keyspace> runs it on
        /// TiKV [default: the embedded store in --data-dir].
        #[arg(long, value_parser = MetaBackend::parse)]
        meta: Option<MetaBackend>,
        #[command(flatten)]
        native: Native,
        #[cfg(feature = "live")]
        #[command(flatten)]
        live: LiveArgs,
    },
    /// Run one node of a cluster: the roles given, a metastore replica over
    /// HTTP, and data in an object-store bucket. `--listen` must be on a
    /// private network: the internal routes are unauthenticated.
    Cluster {
        /// This node's id (unique in the cluster).
        #[arg(long)]
        node_id: u64,
        /// A comma-separated subset of meta,log,query,worker,gateway.
        #[arg(long, value_parser = parse_roles)]
        roles: Roles,
        /// Address of this node's HTTP listener (API, internal and metastore routes).
        #[arg(long)]
        listen: SocketAddr,
        /// The ip:port other nodes reach this node at [default: --listen].
        #[arg(long)]
        advertise: Option<String>,
        /// The meta nodes: id=host:port,…
        #[arg(long, value_parser = operon::cluster::parse_peers)]
        peers: BTreeMap<u64, String>,
        /// Object store URL, such as s3://bucket/prefix, gs://bucket or file:///dir.
        #[arg(long)]
        bucket: String,
        /// Holds the metastore replica's local database and the hot tier.
        #[arg(long, default_value = ".operon")]
        data_dir: PathBuf,
        /// This node's zone; owners of a collection spread across zones.
        #[arg(long, default_value = "")]
        zone: String,
        /// Owners per collection.
        #[arg(long, default_value_t = 1)]
        replication: usize,
        /// How long appends are buffered before a WAL flush.
        #[arg(long, hide = true)]
        flush_interval_ms: Option<u64>,
        /// The node registry's lease TTL.
        #[arg(long, hide = true)]
        registry_ttl_ms: Option<u64>,
        /// How long a learner's node lease may be expired before the
        /// learner is removed from the metastore.
        #[arg(long, hide = true)]
        learner_expiry_ms: Option<u64>,
        /// How often the learner eviction task runs.
        #[arg(long, hide = true)]
        membership_interval_ms: Option<u64>,
        #[command(flatten)]
        native: Native,
        #[command(flatten)]
        tuning: Box<Tuning>,
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

fn parse_roles(s: &str) -> Result<Roles, String> {
    Roles::parse(s).map_err(|err| err.to_string())
}

fn config(command: Command) -> ServerConfig {
    match command {
        Command::Cluster {
            node_id,
            roles,
            listen,
            advertise,
            peers,
            bucket,
            data_dir,
            zone,
            replication,
            flush_interval_ms,
            registry_ttl_ms,
            learner_expiry_ms,
            membership_interval_ms,
            native,
            tuning,
        } => {
            let ms = Duration::from_millis;
            let mut config = ServerConfig::new(data_dir);
            config.listen = listen;
            config.bucket = Some(bucket);
            if let Some(v) = flush_interval_ms {
                config.log.flush_interval = ms(v);
            }
            native.apply(&mut config, STANDALONE_FLIGHT_SQL);
            tuning.apply(&mut config);
            let advertise = advertise.unwrap_or_else(|| listen.to_string());
            let mut cluster = ClusterConfig::new(node_id, roles, advertise, peers);
            cluster.zone = zone;
            cluster.replication = replication;
            if let Some(v) = registry_ttl_ms {
                // Renew three times per TTL; refresh at least that often.
                cluster.registry.lease_ttl = ms(v);
                cluster.registry.renew_every = ms((v / 3).max(1));
                cluster.registry.refresh_every = ms((v / 4).clamp(1, 1000));
            }
            if let Some(v) = learner_expiry_ms {
                cluster.learner_expiry = ms(v);
            }
            if let Some(v) = membership_interval_ms {
                cluster.membership_interval = ms(v);
            }
            config.cluster = Some(cluster);
            config
        }
        Command::Warm { .. } => unreachable!("operon warm starts no server"),
        Command::Dev {
            data_dir,
            listen,
            flush_interval_ms,
            meta,
            native,
            #[cfg(feature = "live")]
            live,
            tuning,
        } => {
            let mut config = ServerConfig::new(data_dir);
            config.listen = listen;
            config.meta = meta.unwrap_or_default();
            #[cfg(feature = "live")]
            live.apply(&mut config, Some(DEV_LIVE_PD));
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
            meta,
            native,
            #[cfg(feature = "live")]
            live,
        } => {
            let mut config = ServerConfig::new(data_dir);
            config.listen = listen;
            config.meta = meta.unwrap_or_default();
            #[cfg(feature = "live")]
            live.apply(&mut config, None);
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
    // Plan M1.4 Task 2 rule 2: before the HTTP line, which harnesses wait
    // for.
    #[cfg(feature = "qdrant")]
    if let (Some(rest), Some(grpc)) = (server.qdrant_rest_addr(), server.qdrant_grpc_addr()) {
        println!("operon qdrant REST listening on http://{rest}");
        println!("operon qdrant gRPC listening on grpc://{grpc}");
    }
    // R1 plan Task 12 semantics 7: before the HTTP line.
    #[cfg(feature = "live")]
    if let Some(addr) = server.live_addr() {
        println!("operon live listening on http://{addr}");
    }
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
        assert_eq!(config.maintenance.commit_delay, half);
        assert_eq!(config.hot_build.artifact_commit_delay, half);
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
        assert_eq!(config.query.backpressure.max_unapplied_bytes, 2048);
        config.validate().expect("a lowered budget");
        assert_eq!(
            config.query.read.consistency_wait,
            Duration::from_millis(250)
        );
    }

    #[cfg(feature = "qdrant")]
    #[test]
    fn qdrant_flags_set_the_config() {
        let qdrant = dev_config(&[]).qdrant.expect("on by default");
        assert_eq!(qdrant.rest_listen, "127.0.0.1:6333".parse().unwrap());
        assert_eq!(qdrant.grpc_listen, "127.0.0.1:6334".parse().unwrap());
        assert_eq!(qdrant.namespace, "default");
        let qdrant = dev_config(&[
            "--qdrant-listen",
            "127.0.0.1:0",
            "--qdrant-grpc-listen",
            "127.0.0.1:1",
            "--qdrant-namespace",
            "acme",
        ])
        .qdrant
        .expect("on");
        assert_eq!(qdrant.rest_listen, "127.0.0.1:0".parse().unwrap());
        assert_eq!(qdrant.grpc_listen, "127.0.0.1:1".parse().unwrap());
        assert_eq!(qdrant.namespace, "acme");
        assert!(dev_config(&["--no-qdrant"]).qdrant.is_none());
        assert!(
            Cli::try_parse_from([
                "operon",
                "dev",
                "--no-qdrant",
                "--qdrant-listen",
                "127.0.0.1:1"
            ])
            .is_err()
        );
        let cli = Cli::try_parse_from(["operon", "standalone", "--bucket", "file:///tmp/b"])
            .expect("parse");
        assert!(config_of(cli).qdrant.is_some());
        let cluster = cluster_config(&[
            "--node-id",
            "1",
            "--roles",
            "meta,gateway",
            "--listen",
            "127.0.0.1:7001",
            "--peers",
            "1=127.0.0.1:7001",
            "--bucket",
            "file:///tmp/b",
            "--no-qdrant",
        ])
        .expect("parse");
        assert!(cluster.qdrant.is_none());
        // ServerConfig::new serves no Qdrant API (E12).
        assert!(ServerConfig::new("/tmp/x").qdrant.is_none());
    }

    #[test]
    fn backpressure_flags_set_the_config() {
        let config = dev_config(&[]);
        assert!(config.query.backpressure.enabled);
        assert_eq!(config.query.backpressure.max_unapplied_records, 1_000_000);
        assert_eq!(config.query.backpressure.max_unapplied_bytes, 128 << 20);
        let config = dev_config(&[
            "--backpressure",
            "off",
            "--max-unapplied-records",
            "7",
            "--max-unapplied-bytes",
            "1000",
        ]);
        assert!(!config.query.backpressure.enabled);
        assert_eq!(config.query.backpressure.max_unapplied_records, 7);
        assert_eq!(config.query.backpressure.max_unapplied_bytes, 1000);
        assert!(Cli::try_parse_from(["operon", "dev", "--backpressure", "maybe"]).is_err());
        let cli = Cli::try_parse_from([
            "operon",
            "standalone",
            "--bucket",
            "file:///tmp/b",
            "--max-unapplied-records",
            "9",
        ])
        .expect("parse");
        assert_eq!(config_of(cli).query.backpressure.max_unapplied_records, 9);
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

    /// R1 plan Task 6: `--meta` on `dev` and `standalone` selects the TiKV
    /// metastore; `cluster` has no such flag, and a cluster config with a
    /// TiKV metastore is refused.
    #[cfg(feature = "tikv")]
    #[test]
    fn meta_flag_selects_the_tikv_metastore_on_dev_and_standalone_only() {
        assert_eq!(dev_config(&[]).meta, MetaBackend::Raft);
        let url = "tikv://127.0.0.1:2379/loam_meta";
        let MetaBackend::Tikv(tikv) = dev_config(&["--meta", url]).meta else {
            panic!("expected the TiKV metastore");
        };
        assert_eq!(tikv.tikv.keyspace, "loam_meta");
        assert_eq!(tikv.tikv.pd, ["127.0.0.1:2379"]);
        let standalone = Cli::try_parse_from([
            "operon",
            "standalone",
            "--bucket",
            "file:///tmp/b",
            "--meta",
            url,
        ])
        .map(config_of)
        .expect("parse");
        assert!(matches!(standalone.meta, MetaBackend::Tikv(_)));
        assert!(Cli::try_parse_from(["operon", "dev", "--meta", "raft://x"]).is_err());
        let cluster = Cli::try_parse_from([
            "operon",
            "cluster",
            "--node-id",
            "1",
            "--roles",
            "meta",
            "--listen",
            "127.0.0.1:7001",
            "--peers",
            "1=127.0.0.1:7001",
            "--bucket",
            "file:///tmp/b",
            "--meta",
            url,
        ]);
        assert!(cluster.is_err(), "operon cluster has no --meta");
        let mut config = cluster_config(&[
            "--node-id",
            "1",
            "--roles",
            "meta",
            "--listen",
            "127.0.0.1:7001",
            "--peers",
            "1=127.0.0.1:7001",
            "--bucket",
            "file:///tmp/b",
        ])
        .expect("parse");
        config.meta = MetaBackend::parse(url).expect("url");
        let err = config.validate().expect_err("refused").to_string();
        assert!(err.contains("dev and standalone only"), "{err}");
    }

    /// R1 plan Task 12: `--live-*` on `dev` and `standalone` configure Loam
    /// Live (on by default with the `live` feature; `--no-live` turns it
    /// off); the tick read lag is a Live config key (row T12-1).
    #[cfg(feature = "live")]
    #[test]
    fn live_flags_set_the_live_config() {
        let live = dev_config(&[]).live.expect("on by default");
        assert_eq!(live.listen, SocketAddr::from(([127, 0, 0, 1], 7710)));
        assert_eq!(live.tikv.pd, ["127.0.0.1:19379"]);
        assert_eq!(live.tikv.keyspace, "loam_live_dev");
        assert_eq!(live.app, "dev");
        assert_eq!(live.subs.tick_read_lag, Duration::from_millis(200));
        assert!(live.engine.is_some(), "Deploy has the QuickJS engine");
        let live = dev_config(&[
            "--live-listen",
            "localhost:7711",
            "--live-pd",
            "10.0.0.1:2379,10.0.0.2:2379",
            "--live-app",
            "chat",
            "--live-tick-read-lag-ms",
            "0",
        ])
        .live
        .expect("configured");
        assert_eq!(live.listen, SocketAddr::from(([127, 0, 0, 1], 7711)));
        assert_eq!(live.tikv.pd, ["10.0.0.1:2379", "10.0.0.2:2379"]);
        assert_eq!(live.tikv.keyspace, "loam_live_chat");
        assert_eq!(live.subs.tick_read_lag, Duration::ZERO);
        let live = dev_config(&["--live-keyspace", "other"]).live.expect("on");
        assert_eq!(live.tikv.keyspace, "other");
        assert!(live.tikv.root.is_empty(), "no root by default");
        let live = dev_config(&["--live-root", "00ff1A"]).live.expect("on");
        assert_eq!(live.tikv.root, [0x00, 0xff, 0x1a]);
        for bad in ["", "abc", "zz"] {
            assert!(
                Cli::try_parse_from(["operon", "dev", "--live-root", bad]).is_err(),
                "{bad:?}"
            );
        }
        assert!(Cli::try_parse_from(["operon", "dev", "--no-live", "--live-root", "00"]).is_err());
        // Owner ruling T14-1: the QuickJS pool size per deployment.
        assert!(dev_config(&["--live-js-contexts", "2"]).live.is_some());
        for bad in ["0", "257", "x"] {
            assert!(
                Cli::try_parse_from(["operon", "dev", "--live-js-contexts", bad]).is_err(),
                "{bad:?}"
            );
        }
        assert!(
            Cli::try_parse_from(["operon", "dev", "--no-live", "--live-js-contexts", "2"]).is_err()
        );
        assert!(dev_config(&["--no-live"]).live.is_none());
        let standalone = Cli::try_parse_from([
            "operon",
            "standalone",
            "--bucket",
            "file:///tmp/b",
            "--no-live",
        ])
        .map(config_of)
        .expect("parse");
        assert!(standalone.live.is_none());
        // Owner ruling (row T13-2): standalone has no default PD; Live
        // without --live-pd refuses to start, with --live-pd it is set.
        let standalone = |extra: &[&str]| {
            let mut args = vec!["operon", "standalone", "--bucket", "file:///tmp/b"];
            args.extend_from_slice(extra);
            Cli::try_parse_from(args).map(config_of).expect("parse")
        };
        let missing = standalone(&[]);
        assert!(missing.live.as_ref().is_some_and(|l| l.tikv.pd.is_empty()));
        let err = missing.validate().expect_err("no --live-pd");
        assert!(matches!(err, operon::ServerError::LivePdMissing), "{err}");
        assert!(err.to_string().contains("--live-pd"), "{err}");
        let given = standalone(&["--live-pd", "10.0.0.1:2379"]);
        assert_eq!(
            given.live.as_ref().map(|l| l.tikv.pd.clone()),
            Some(vec!["10.0.0.1:2379".to_string()])
        );
        given.validate().expect("--live-pd given");
        assert!(Cli::try_parse_from(["operon", "dev", "--live-app", "no spaces"]).is_err());
        assert!(Cli::try_parse_from(["operon", "dev", "--no-live", "--live-app", "x"]).is_err());
        // Review of #88: every Live flag conflicts with `--no-live`.
        for flag in [
            ["--live-pd", "10.0.0.1:2379"],
            ["--live-tick-read-lag-ms", "0"],
        ] {
            let mut args = vec!["operon", "dev", "--no-live"];
            args.extend(flag);
            assert!(Cli::try_parse_from(args).is_err(), "{flag:?}");
        }
    }

    /// R1 plan Task 12 semantics 7 and the loopback rule (D111): a
    /// non-loopback `--live-listen` fails startup with the error of design
    /// §20 §7.1; loopback addresses pass.
    #[cfg(feature = "live")]
    #[test]
    fn a_non_loopback_live_listen_fails_startup() {
        for bad in ["0.0.0.0:7710", "192.168.1.10:7710", "[::]:7710"] {
            let err = dev_config(&["--live-listen", bad])
                .validate()
                .expect_err(bad);
            assert!(
                matches!(err, operon::ServerError::LiveListenNotLoopback { .. }),
                "{bad}: {err}"
            );
            assert_eq!(
                format!("operon: {err}"),
                format!(
                    "operon: --live-listen {} is not a loopback address; the Live API has no \
                     authentication until the unified auth plan (D111)",
                    bad.parse::<SocketAddr>().expect("an address")
                )
            );
        }
        for ok in [
            "127.0.0.1:7710",
            "localhost:7710",
            "[::1]:7710",
            "127.0.0.2:1",
        ] {
            dev_config(&["--live-listen", ok])
                .validate()
                .unwrap_or_else(|e| panic!("{ok}: {e}"));
        }
        assert!(Cli::try_parse_from(["operon", "dev", "--live-listen", "nohost:1"]).is_err());
    }

    /// Owner ruling T7-3: a build without the `tikv` feature refuses
    /// `--meta tikv://…` at parse time with an error naming the feature.
    #[cfg(not(feature = "tikv"))]
    #[test]
    fn meta_flag_without_the_tikv_feature_is_refused_naming_it() {
        let url = "tikv://127.0.0.1:2379/loam_meta";
        for args in [
            &["operon", "dev", "--meta", url][..],
            &[
                "operon",
                "standalone",
                "--bucket",
                "file:///tmp/b",
                "--meta",
                url,
            ],
        ] {
            let command = args[1];
            let err = Cli::try_parse_from(args)
                .map(|_| ())
                .expect_err("refused")
                .to_string();
            assert!(
                err.contains("built without the tikv feature"),
                "{command}: {err}"
            );
            assert!(err.contains("--features tikv"), "{command}: {err}");
        }
    }

    fn cluster_config(args: &[&str]) -> Result<ServerConfig, clap::Error> {
        Cli::try_parse_from(["operon", "cluster"].iter().chain(args)).map(config_of)
    }

    #[test]
    fn cluster_flags_set_the_cluster_config() {
        let base = [
            "--node-id",
            "2",
            "--roles",
            "query,meta",
            "--listen",
            "127.0.0.1:7002",
            "--peers",
            "1=127.0.0.1:7001,2=127.0.0.1:7002,3=10.0.0.3:7003",
            "--bucket",
            "file:///tmp/b",
        ];
        let config = cluster_config(&base).expect("parse");
        let cluster = config.cluster.clone().expect("cluster");
        assert_eq!(cluster.node_id, 2);
        assert_eq!(cluster.roles.to_string(), "meta,query");
        assert_eq!(cluster.advertise, "127.0.0.1:7002", "defaults to --listen");
        assert_eq!(cluster.peers.len(), 3);
        assert_eq!((cluster.replication, cluster.zone.as_str()), (1, ""));
        assert_eq!(config.flight_sql, Some("0.0.0.0:8082".parse().unwrap()));
        config.validate().expect("valid");

        let mut args = base.to_vec();
        args.extend([
            "--no-flight-sql",
            "--zone",
            "a",
            "--replication",
            "2",
            "--flush-interval-ms",
            "20",
            "--registry-ttl-ms",
            "1500",
            "--learner-expiry-ms",
            "3000",
            "--membership-interval-ms",
            "500",
            "--lease-ttl-ms",
            "1500",
            "--poll-interval-ms",
            "50",
        ]);
        let config = cluster_config(&args).expect("parse");
        let cluster = config.cluster.clone().expect("cluster");
        assert_eq!(config.flight_sql, None);
        assert_eq!((cluster.zone.as_str(), cluster.replication), ("a", 2));
        assert_eq!(config.log.flush_interval, Duration::from_millis(20));
        assert_eq!(cluster.registry.lease_ttl, Duration::from_millis(1500));
        assert_eq!(cluster.registry.renew_every, Duration::from_millis(500));
        assert_eq!(cluster.learner_expiry, Duration::from_millis(3000));
        assert_eq!(cluster.membership_interval, Duration::from_millis(500));
        assert_eq!(config.worker_lease_ttl, Duration::from_millis(1500));

        assert!(cluster_config(&["--roles", "cook"]).is_err());
        // Rule 1: a meta node must be in --peers, with its advertise address.
        let invalid = |edit: &dyn Fn(&mut Vec<&str>)| {
            let mut args = base.to_vec();
            edit(&mut args);
            let config = cluster_config(&args).expect("parse");
            config.validate().expect_err("invalid").to_string()
        };
        let err = invalid(&|a| a[1] = "4");
        assert!(err.contains("not in --peers"), "{err}");
        let err = invalid(&|a| a.extend(["--advertise", "127.0.0.1:9999"]));
        assert!(err.contains("differs from --advertise"), "{err}");
        let err = invalid(&|a| a[3] = "query");
        assert!(err.contains("no meta role"), "{err}");
        let err = invalid(&|a| a[5] = "0.0.0.0:7002");
        assert!(err.contains("unspecified"), "{err}");
        let err = invalid(&|a| a[7] = "1=nowhere,2=127.0.0.1:7002");
        assert!(err.contains("not host:port"), "{err}");
        let err = invalid(&|a| a.extend(["--replication", "0"]));
        assert!(err.contains("replication"), "{err}");
    }
}
