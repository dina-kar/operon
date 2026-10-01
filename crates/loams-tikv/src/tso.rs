//! The TSO clock and the client supervisor.
//!
//! `tikv-client` keeps one TSO stream per `TransactionClient`. Upstream, when
//! that stream died (a PD stall), the stream was not reopened and timestamp
//! requests failed with `TimestampRequest channel is closed` until the PD
//! client reconnected (the feasibility spike saw the client never recover).
//! Loams's fork reopens the stream in place (R1 plan row F1), so that error
//! should no longer occur. The [`Supervisor`] stays as defence in depth: it
//! holds the client behind a `RwLock<Arc<_>>` and rebuilds it on that error,
//! or after three consecutive TSO failures (which now also covers the fork's
//! `TSO stream failed` errors), one rebuild at a time and with backoff between
//! failed rebuilds (R1 plan Task 2 semantics 5).

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use tikv_client::{Timestamp, TimestampExt, TransactionClient};

use crate::TikvError;
use crate::classify::{Class, TSO_CLOSED, classify};

/// Consecutive TSO failures that trigger a rebuild.
const TSO_FAILURES_BEFORE_REBUILD: u32 = 3;
/// The first pause after a failed rebuild; doubles up to [`REBUILD_BACKOFF_MAX`].
const REBUILD_BACKOFF: Duration = Duration::from_millis(100);
const REBUILD_BACKOFF_MAX: Duration = Duration::from_secs(5);

/// Holds the current `TransactionClient` and rebuilds it when its TSO stream
/// is gone.
pub(crate) struct Supervisor {
    current: RwLock<Arc<TransactionClient>>,
    generation: AtomicU64,
    rebuild: tokio::sync::Mutex<RebuildState>,
    tso_failures: AtomicU32,
    rebuilds: AtomicU64,
    pd: Vec<String>,
    config: tikv_client::Config,
    connect_limit: Duration,
    inject_tso_loss: AtomicBool,
}

#[derive(Default)]
struct RebuildState {
    last_failed: Option<Instant>,
    failures: u32,
}

impl Supervisor {
    pub(crate) fn new(
        client: TransactionClient,
        pd: Vec<String>,
        config: tikv_client::Config,
        connect_limit: Duration,
    ) -> Self {
        Supervisor {
            current: RwLock::new(Arc::new(client)),
            generation: AtomicU64::new(0),
            rebuild: tokio::sync::Mutex::new(RebuildState::default()),
            tso_failures: AtomicU32::new(0),
            rebuilds: AtomicU64::new(0),
            pd,
            config,
            connect_limit,
            inject_tso_loss: AtomicBool::new(false),
        }
    }

    /// The current client and its generation.
    pub(crate) fn client(&self) -> (Arc<TransactionClient>, u64) {
        let client = self
            .current
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        (client, self.generation.load(Ordering::Acquire))
    }

    /// The PD endpoints, the client configuration and the connect limit
    /// clients are built with (the GC loop builds one per keyspace).
    pub(crate) fn blueprint(&self) -> (&[String], &tikv_client::Config, Duration) {
        (&self.pd, &self.config, self.connect_limit)
    }

    /// How many times the client was rebuilt.
    pub(crate) fn rebuilds(&self) -> u64 {
        self.rebuilds.load(Ordering::Relaxed)
    }

    /// Makes the next TSO-dependent call fail as if the TSO stream had closed
    /// (the test hook of `tso_stream_loss_rebuilds_the_client`).
    #[cfg(feature = "faults")]
    pub(crate) fn inject_tso_loss(&self) {
        self.inject_tso_loss.store(true, Ordering::Release);
    }

    /// The injected TSO loss, once.
    pub(crate) fn take_injected_tso_loss(&self) -> Option<tikv_client::Error> {
        self.inject_tso_loss.swap(false, Ordering::AcqRel).then(|| {
            tikv_client::Error::InternalError {
                message: format!("[injected]: {TSO_CLOSED}"),
            }
        })
    }

    /// Records a successful TSO-dependent call.
    pub(crate) fn tso_ok(&self) {
        self.tso_failures.store(0, Ordering::Relaxed);
    }

    /// Records a failed TSO-dependent call on the client of `generation`, and
    /// rebuilds the client when the TSO stream is closed or after three
    /// consecutive failures. `class` is `None` for a timeout.
    pub(crate) async fn tso_failed(&self, class: Option<Class>, generation: u64) {
        let failures = self.tso_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if class == Some(Class::TsoClosed) || failures >= TSO_FAILURES_BEFORE_REBUILD {
            self.rebuild(generation).await;
        }
    }

    /// Replaces the client of `generation` with a new one. Does nothing when
    /// another caller already replaced it, or while backing off after a failed
    /// rebuild.
    pub(crate) async fn rebuild(&self, generation: u64) {
        let mut state = self.rebuild.lock().await;
        if self.generation.load(Ordering::Acquire) != generation {
            return;
        }
        if let Some(last) = state.last_failed {
            let pause = REBUILD_BACKOFF
                .saturating_mul(1 << state.failures.min(6))
                .min(REBUILD_BACKOFF_MAX);
            if last.elapsed() < pause {
                return;
            }
        }
        let built = tokio::time::timeout(
            self.connect_limit,
            TransactionClient::new_with_config(self.pd.clone(), self.config.clone()),
        )
        .await;
        match built {
            Ok(Ok(client)) => {
                *self.current.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(client);
                self.generation.fetch_add(1, Ordering::AcqRel);
                self.rebuilds.fetch_add(1, Ordering::Relaxed);
                self.tso_failures.store(0, Ordering::Relaxed);
                *state = RebuildState::default();
                tracing::warn!(
                    rebuilds = self.rebuilds(),
                    "rebuilt the TiKV client after its TSO stream failed"
                );
            }
            Ok(Err(e)) => {
                state.last_failed = Some(Instant::now());
                state.failures += 1;
                tracing::warn!(error = %e, "rebuilding the TiKV client failed");
            }
            Err(_) => {
                state.last_failed = Some(Instant::now());
                state.failures += 1;
                tracing::warn!(after = ?self.connect_limit, "rebuilding the TiKV client timed out");
            }
        }
    }
}

/// Fetches TSO timestamps and remembers the latest one with the instant it
/// arrived (the anchor of the metastore's synchronous `now_ms`, R1 row R2).
pub(crate) struct TsoClock {
    clients: Arc<Supervisor>,
    timeout: Duration,
    latest: Mutex<Option<(Timestamp, Instant)>>,
}

impl TsoClock {
    pub(crate) fn new(clients: Arc<Supervisor>, timeout: Duration) -> Self {
        TsoClock {
            clients,
            timeout,
            latest: Mutex::new(None),
        }
    }

    /// A fresh timestamp, later than every timestamp PD handed out before the
    /// call started.
    pub(crate) async fn now(&self) -> Result<Timestamp, TikvError> {
        let (client, generation) = self.clients.client();
        let answer = match self.clients.take_injected_tso_loss() {
            Some(e) => Ok(Err(e)),
            None => tokio::time::timeout(self.timeout, client.current_timestamp()).await,
        };
        let ts = match answer {
            Ok(Ok(ts)) => ts,
            Ok(Err(e)) => {
                self.clients
                    .tso_failed(Some(classify(&e)), generation)
                    .await;
                return Err(e.into());
            }
            Err(_) => {
                self.clients.tso_failed(None, generation).await;
                return Err(TikvError::Timeout {
                    op: "TSO",
                    after: self.timeout,
                });
            }
        };
        self.clients.tso_ok();
        self.observe(&ts);
        Ok(ts)
    }

    /// Remembers `ts` if it is the latest seen.
    pub(crate) fn observe(&self, ts: &Timestamp) {
        let arrived = Instant::now();
        let mut latest = self.latest.lock().unwrap_or_else(|e| e.into_inner());
        if latest
            .as_ref()
            .is_none_or(|(seen, _)| seen.version() < ts.version())
        {
            *latest = Some((ts.clone(), arrived));
        }
    }

    /// The latest timestamp this clock obtained, and when it arrived.
    pub(crate) fn latest(&self) -> Option<(Timestamp, Instant)> {
        self.latest
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}
