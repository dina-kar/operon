//! Loam's TiKV client layer (R1 plan Tasks 1–2; design §20 §5, §9).
//!
//! [`Tikv`] is a keyspace-scoped handle on a TiKV cluster on API v2: a
//! `tikv-client` transaction client bound to one keyspace (rebuilt by a
//! supervisor when its TSO stream dies), a root prefix every key of the handle
//! lives under, the PD HTTP endpoint and the TSO clock. Every transaction goes
//! through [`Tikv::run`], the runner that classifies errors, retries, writes
//! commit tokens and calls a [`FaultPlan`]; [`Tikv::snapshot`] reads at a
//! timestamp inside the GC safe window. [`codec::tuple`] is the
//! order-preserving tuple codec. [`ensure_keyspace`] creates a keyspace
//! through PD's HTTP API if it is absent. [`testing`] is the cluster harness:
//! tests that need TiKV call [`testing::cluster`], which skips them unless
//! `OPERON_TEST_PD` is set.
//!
//! Task 3 adds the cluster GC loop.

mod classify;
pub mod codec;
mod config;
pub mod faults;
mod keyspace;
mod runner;
pub mod testing;
pub mod token;
mod tso;
mod txn;

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub use codec::{CodecError, tuple};
pub use config::{
    DEFAULT_GC_LIFE_TIME, DEFAULT_GRPC_MAX_DECODING_BYTES, DEFAULT_REQUEST_TIMEOUT, GC_SAFE_MARGIN,
    TikvConfig,
};
pub use faults::{Fault, FaultPlan, FaultPoint};
pub use keyspace::{KeyspaceMeta, ensure_keyspace};
pub use runner::{CommitMode, Committed, Mode, TikvStats, TxnError, TxnOptions};
pub use tikv_client::{Timestamp, TimestampExt};
pub use txn::{MAX_VALUE_BYTES, PAGE_KEYS, Pair, Snap, Txn};

use runner::Counters;
use tikv_client::TransactionClient;
use tso::{Supervisor, TsoClock};

/// Errors of the TiKV layer.
#[derive(Debug, thiserror::Error)]
pub enum TikvError {
    /// PD has no keyspace of this name.
    #[error(
        "TiKV keyspace '{name}' does not exist: create it (PD's keyspace.pre-alloc, or \
         POST /pd/api/v2/keyspaces) on a cluster whose TiKV runs storage.api-version = 2"
    )]
    KeyspaceMissing { name: String },
    /// The cluster is not on API v2, or the handle has no keyspace.
    #[error("TiKV API version mismatch: {hint}")]
    ApiVersion { hint: String },
    /// The configuration is invalid.
    #[error("invalid TiKV configuration: {0}")]
    Config(String),
    /// PD's HTTP API answered with an unexpected status.
    #[error("PD HTTP API: {op} answered {status}: {body}")]
    Pd {
        op: &'static str,
        status: u16,
        body: String,
    },
    /// PD's HTTP API could not be reached, or its answer could not be read.
    #[error("PD HTTP API: {op}: {message}")]
    Http { op: &'static str, message: String },
    /// An operation did not finish within the request timeout.
    #[error("TiKV: {op} timed out after {after:?}")]
    Timeout { op: &'static str, after: Duration },
    /// A read below the GC safe window (row R7): `at` is older than
    /// `safe_point`, `now − (gc life time − 1 min)`, as TSO versions.
    #[error(
        "read at ts {at} is below the GC safe point: reads older than ts {safe_point} \
         (now − (gc life time − 1 min)) are refused, because GC may have dropped the versions"
    )]
    GcSafePoint { at: u64, safe_point: u64 },
    /// Any other error from `tikv-client`, its keys scrubbed.
    #[error("TiKV client: {0}")]
    Client(String),
}

impl From<tikv_client::Error> for TikvError {
    fn from(e: tikv_client::Error) -> Self {
        TikvError::Client(classify::describe(&e, &[]))
    }
}

/// A keyspace-scoped handle on a TiKV cluster. Cheap to clone.
#[derive(Clone)]
pub struct Tikv {
    clients: Arc<Supervisor>,
    http: reqwest::Client,
    pd_http: String,
    keyspace: String,
    root: Arc<[u8]>,
    tso: Arc<TsoClock>,
    request_timeout: Duration,
    commit_mode: CommitMode,
    gc_life_time: Duration,
    faults: Option<Arc<dyn FaultPlan>>,
    counters: Arc<Counters>,
}

impl fmt::Debug for Tikv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tikv")
            .field("pd_http", &self.pd_http)
            .field("keyspace", &self.keyspace)
            .field("root", &EscapedBytes(&self.root))
            .field("request_timeout", &self.request_timeout)
            .field("commit_mode", &self.commit_mode)
            .field("gc_life_time", &self.gc_life_time)
            .field("faults", &self.faults.is_some())
            .finish_non_exhaustive()
    }
}

impl Tikv {
    /// Connects to the cluster and checks that the keyspace exists and that
    /// the cluster runs API v2.
    ///
    /// Fails with [`TikvError::KeyspaceMissing`] when PD has no such keyspace,
    /// and with [`TikvError::ApiVersion`] when the keyspace is empty or the
    /// cluster refuses the handle's keys (TiKV's `InvalidKeyMode` and
    /// `ApiVersionNotMatched`).
    pub async fn connect(config: TikvConfig) -> Result<Self, TikvError> {
        config.validate()?;
        let pd_http = config.pd_http_url();
        let http = keyspace::http_client(config.request_timeout)?;
        let mut client_config = tikv_client::Config::default()
            .with_timeout(config.request_timeout)
            .with_grpc_max_decoding_message_size(config.grpc_max_decoding_bytes);
        if !config.keyspace.is_empty() {
            client_config = client_config.with_keyspace(&config.keyspace);
        }
        // tikv-client retries PD calls within its own timeout; bound the whole
        // connect by a few of them.
        let connect_limit = config.request_timeout * 4;
        let client = tokio::time::timeout(
            connect_limit,
            TransactionClient::new_with_config(config.pd.clone(), client_config.clone()),
        )
        .await
        .map_err(|_| TikvError::Timeout {
            op: "connect",
            after: connect_limit,
        })?
        .map_err(|e| connect_error(&config.keyspace, e))?;
        let clients = Arc::new(Supervisor::new(
            client,
            config.pd.clone(),
            client_config,
            connect_limit,
        ));
        let tikv = Tikv {
            tso: Arc::new(TsoClock::new(clients.clone(), config.request_timeout)),
            clients,
            http,
            pd_http,
            keyspace: config.keyspace,
            root: config.root.into(),
            request_timeout: config.request_timeout,
            commit_mode: config.commit_mode,
            gc_life_time: config.gc_life_time,
            faults: None,
            counters: Arc::default(),
        };
        tikv.probe().await?;
        Ok(tikv)
    }

    /// The root prefix every key of this handle lives under.
    pub fn root(&self) -> &[u8] {
        &self.root
    }

    /// The keyspace this handle is bound to.
    pub fn keyspace(&self) -> &str {
        &self.keyspace
    }

    /// The base URL of PD's HTTP API (`http://host:port`).
    pub fn pd_http(&self) -> &str {
        &self.pd_http
    }

    /// `root ‖ suffix`: the key of `suffix` under this handle's root.
    pub fn key(&self, suffix: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.root.len() + suffix.len());
        key.extend_from_slice(&self.root);
        key.extend_from_slice(suffix);
        key
    }

    /// A fresh timestamp from PD's TSO.
    pub async fn now(&self) -> Result<Timestamp, TikvError> {
        self.tso.now().await
    }

    /// The latest TSO timestamp this handle obtained and the instant it
    /// arrived, or `None` before the first. The metastore's synchronous
    /// `now_ms` extrapolates from it (R1 plan row R2).
    pub fn latest_timestamp(&self) -> Option<(Timestamp, Instant)> {
        self.tso.latest()
    }

    /// The physical part of a TSO timestamp: milliseconds since the Unix epoch.
    pub fn physical_ms(ts: &Timestamp) -> u64 {
        u64::try_from(ts.physical).unwrap_or(0)
    }

    /// The current `tikv-client` transaction client (keyspace-scoped), for
    /// this crate's own maintenance paths (Task 3's GC loop). Everything else
    /// goes through [`Tikv::run`] and [`Tikv::snapshot`].
    pub(crate) fn client(&self) -> Arc<TransactionClient> {
        self.clients.client().0
    }

    /// The handle's default commit mode.
    pub fn commit_mode(&self) -> CommitMode {
        self.commit_mode
    }

    /// This handle with `plan` consulted at every fault point of every
    /// [`Tikv::run`] (R1 plan Task 2; feature `faults`).
    #[cfg(feature = "faults")]
    pub fn with_faults(mut self, plan: Arc<dyn FaultPlan>) -> Self {
        self.faults = Some(plan);
        self
    }

    /// Makes the next TSO request of this handle fail as `tikv-client` does
    /// once its TSO stream is gone (`TimestampRequest channel is closed`), so
    /// a test can watch the supervisor rebuild the client (feature `faults`).
    #[cfg(feature = "faults")]
    pub fn inject_tso_stream_loss(&self) {
        self.clients.inject_tso_loss();
    }

    /// How long reads stay inside the GC safe window: `gc_life_time − 1 min`.
    pub(crate) fn safe_window(&self) -> Duration {
        self.gc_life_time.saturating_sub(GC_SAFE_MARGIN)
    }

    /// The current physical time by the TSO: the latest timestamp plus the
    /// time since it arrived, or a fresh one before the first.
    pub(crate) async fn now_ms_estimate(&self) -> Result<u64, TikvError> {
        match self.latest_timestamp() {
            Some((ts, arrived)) => {
                let elapsed = u64::try_from(arrived.elapsed().as_millis()).unwrap_or(u64::MAX);
                Ok(Tikv::physical_ms(&ts).saturating_add(elapsed))
            }
            None => Ok(Tikv::physical_ms(&self.now().await?)),
        }
    }

    /// This handle's keyspace as PD's HTTP API reports it.
    pub async fn keyspace_meta(&self) -> Result<KeyspaceMeta, TikvError> {
        keyspace::get(&self.http, &self.pd_http, &self.keyspace)
            .await?
            .ok_or_else(|| TikvError::KeyspaceMissing {
                name: self.keyspace.clone(),
            })
    }

    /// Reads one key under the root at a fresh timestamp. On API v2 a request
    /// without a keyspace fails with `InvalidKeyMode`, and a store on another
    /// API version with `ApiVersionNotMatched`.
    async fn probe(&self) -> Result<(), TikvError> {
        let ts = self.now().await?;
        let mut snapshot = self.client().snapshot(
            ts,
            tikv_client::TransactionOptions::new_optimistic()
                .read_only()
                .drop_check(tikv_client::CheckLevel::None),
        );
        let read = tokio::time::timeout(self.request_timeout, snapshot.get(self.key(b"\0")))
            .await
            .map_err(|_| TikvError::Timeout {
                op: "probe read",
                after: self.request_timeout,
            })?;
        match read {
            Ok(_) if self.keyspace.is_empty() => Err(TikvError::ApiVersion {
                hint: "TikvConfig.keyspace is empty and the cluster accepted a key outside \
                       any keyspace, so it does not run storage.api-version = 2; Loam needs \
                       API v2 and a keyspace"
                    .to_string(),
            }),
            Ok(_) => Ok(()),
            Err(e) if is_api_version_error(&e) => Err(TikvError::ApiVersion {
                hint: if self.keyspace.is_empty() {
                    format!(
                        "set TikvConfig.keyspace: the cluster runs storage.api-version = 2, \
                         which refuses keys outside a keyspace ({})",
                        short(&e, &self.root)
                    )
                } else {
                    format!(
                        "every TiKV store must run storage.api-version = 2 with \
                         storage.enable-ttl = true ({})",
                        short(&e, &self.root)
                    )
                },
            }),
            Err(e) => Err(e.into()),
        }
    }
}

/// Maps a `TransactionClient` connect error: a missing keyspace names it.
fn connect_error(keyspace: &str, e: tikv_client::Error) -> TikvError {
    let missing = matches!(e, tikv_client::Error::KeyspaceNotFound(_))
        || e.to_string().contains("keyspace does not exist");
    if missing && !keyspace.is_empty() {
        TikvError::KeyspaceMissing {
            name: keyspace.to_string(),
        }
    } else {
        e.into()
    }
}

fn is_api_version_error(e: &tikv_client::Error) -> bool {
    let text = format!("{e:?}");
    text.contains("InvalidKeyMode")
        || text.contains("invalid key mode")
        || text.contains("ApiVersionNotMatched")
        || text.contains("api_version_not_matched")
}

/// An error's text, cut to 200 characters and scrubbed of keys, for a hint.
fn short(e: &tikv_client::Error, root: &[u8]) -> String {
    let text = format!("{e:?}");
    let text = match text.char_indices().nth(200) {
        Some((i, _)) => format!("{}…", &text[..i]),
        None => text,
    };
    classify::scrub_text(&text, root)
}

struct EscapedBytes<'a>(&'a [u8]);

impl fmt::Debug for EscapedBytes<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "\"{}\"", self.0.escape_ascii())
    }
}
