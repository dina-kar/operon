//! Loam's TiKV client layer (R1 plan Task 1; design §20 §5, §9).
//!
//! [`Tikv`] is a keyspace-scoped handle on a TiKV cluster on API v2: a
//! `tikv-client` [`TransactionClient`] bound to one keyspace, a root prefix
//! every key of the handle lives under, the PD HTTP endpoint and the TSO clock.
//! [`ensure_keyspace`] creates a keyspace through PD's HTTP API if it is
//! absent. [`testing`] is the cluster harness: tests that need TiKV call
//! [`testing::cluster`], which skips them unless `OPERON_TEST_PD` is set.
//!
//! Later R1 tasks add the transaction runner, commit tokens, fault hooks, the
//! tuple codec and the cluster GC loop.

mod config;
mod keyspace;
pub mod testing;
mod tso;

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub use config::TikvConfig;
pub use keyspace::{KeyspaceMeta, ensure_keyspace};
pub use tikv_client::{Timestamp, TimestampExt, TransactionClient};

use tso::TsoClock;

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
    /// Any other error from `tikv-client`.
    #[error("TiKV client: {0}")]
    Client(#[source] Box<tikv_client::Error>),
}

impl From<tikv_client::Error> for TikvError {
    fn from(e: tikv_client::Error) -> Self {
        TikvError::Client(Box::new(e))
    }
}

/// A keyspace-scoped handle on a TiKV cluster. Cheap to clone.
#[derive(Clone)]
pub struct Tikv {
    client: Arc<TransactionClient>,
    http: reqwest::Client,
    pd_http: String,
    keyspace: String,
    root: Arc<[u8]>,
    tso: Arc<TsoClock>,
    request_timeout: Duration,
}

impl fmt::Debug for Tikv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tikv")
            .field("pd_http", &self.pd_http)
            .field("keyspace", &self.keyspace)
            .field("root", &EscapedBytes(&self.root))
            .field("request_timeout", &self.request_timeout)
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
        let mut client_config = tikv_client::Config::default().with_timeout(config.request_timeout);
        if !config.keyspace.is_empty() {
            client_config = client_config.with_keyspace(&config.keyspace);
        }
        // tikv-client retries PD calls within its own timeout; bound the whole
        // connect by a few of them.
        let connect_limit = config.request_timeout * 4;
        let client = tokio::time::timeout(
            connect_limit,
            TransactionClient::new_with_config(config.pd.clone(), client_config),
        )
        .await
        .map_err(|_| TikvError::Timeout {
            op: "connect",
            after: connect_limit,
        })?
        .map_err(|e| connect_error(&config.keyspace, e))?;
        let client = Arc::new(client);
        let tikv = Tikv {
            tso: Arc::new(TsoClock::new(client.clone(), config.request_timeout)),
            client,
            http,
            pd_http,
            keyspace: config.keyspace,
            root: config.root.into(),
            request_timeout: config.request_timeout,
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

    /// The `tikv-client` transaction client (keyspace-scoped). Task 2's
    /// transaction runner is the interface for everything else; this is for
    /// the harness and for tests.
    #[doc(hidden)]
    pub fn raw_client(&self) -> Arc<TransactionClient> {
        self.client.clone()
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
        let mut snapshot = self.client.snapshot(
            ts,
            tikv_client::TransactionOptions::new_optimistic().read_only(),
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
                        short(&e)
                    )
                } else {
                    format!(
                        "every TiKV store must run storage.api-version = 2 with \
                         storage.enable-ttl = true ({})",
                        short(&e)
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

/// An error's text, cut to 200 characters for a hint.
fn short(e: &tikv_client::Error) -> String {
    let text = format!("{e:?}");
    match text.char_indices().nth(200) {
        Some((i, _)) => format!("{}…", &text[..i]),
        None => text,
    }
}

struct EscapedBytes<'a>(&'a [u8]);

impl fmt::Debug for EscapedBytes<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "\"{}\"", self.0.escape_ascii())
    }
}
