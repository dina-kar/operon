//! The cluster MVCC GC loop and GC barriers (R1 plan Task 3, rows R5–R7, X2).
//!
//! PD and TiKV v8.5.8 have no keyspace-level GC: TiKV reads only the cluster
//! GC safe point (Q32). So Loams is the cluster's GC worker and does what
//! TiDB's GC worker does, for every keyspace. One [`GcLoop`] runs per cluster,
//! held by a lease in the `loams_meta` keyspace. Each run:
//!
//! 1. takes (or renews) the lease `e/cluster/gc` (epoch-fenced);
//! 2. computes `target = now − life_time` and calls
//!    `UpdateServiceGCSafePoint("gc_worker", ttl = i64::MAX, target)`; the
//!    safe point is `min(target, min service safe point)`, so every service
//!    safe point (Loams's [`GcBarrier`]s, BR, TiCDC) holds it back;
//! 3. lists the keyspaces through PD's HTTP API and resolves the locks below
//!    the safe point in every keyspace whose state is not `TOMBSTONE`, TiDB's
//!    included (a keyspace-mode TiDB resolves none itself); if a client
//!    cannot be built for one, or a resolution fails, the run fails without
//!    advancing the safe point;
//! 4. checks that it still holds the lease at the same epoch, then calls
//!    `UpdateGCSafePoint(safe_point)`;
//! 5. sweeps expired commit tokens and fences (`t/…`) under the roots Loams
//!    owns.
//!
//! TiKV drops versions below the safe point only when RocksDB compacts (with
//! the default `gc.enable-compaction-filter = true`) and answers reads below
//! it without an error, so correctness rests on the read refusal of
//! [`Tikv::snapshot`], which a [`GcBarrier`] set through the same handle lifts
//! for the timestamps it covers.
//!
//! No TiDB without `keyspace-name` may run on a Loams cluster: it would be a
//! second GC worker.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tikv_client::{Timestamp, TimestampExt, TransactionClient};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::keyspace::{self, KeyspaceMeta};
use crate::pd::{GC_WORKER_SERVICE, MinServiceSafePoint};
use crate::runner::{TxnError, TxnOptions};
use crate::token::{TOKEN_PREFIX, token_expiry};
use crate::{Tikv, TikvError, codec};

/// The lease key of the GC loop, under the handle's root in `loams_meta`.
pub const GC_LEASE_KEY: &[u8] = b"e/cluster/gc";

/// The default GC interval.
pub const DEFAULT_GC_INTERVAL: Duration = Duration::from_secs(60);

/// How long a cached [`Tikv::gc_safe_point`] answer is used.
const SAFE_POINT_CACHE: Duration = Duration::from_secs(10);

/// Token keys per sweep page (and per delete transaction).
const SWEEP_PAGE: usize = 256;

/// The keyspace state whose range is gone; the loop skips it.
const TOMBSTONE: &str = "TOMBSTONE";

/// The prefix every Loams barrier's service id carries.
const BARRIER_PREFIX: &str = "loams/";

/// The configuration of a [`GcLoop`].
#[derive(Debug, Clone)]
pub struct GcConfig {
    /// Versions younger than this are kept (default 10 min). It must be at
    /// least the handle's `TikvConfig::gc_life_time`, whose read window
    /// assumes GC never runs closer to now.
    pub life_time: Duration,
    /// The pause between runs (default 1 min).
    pub interval: Duration,
    /// The lease key, relative to the handle's root (default
    /// [`GC_LEASE_KEY`]).
    pub lease_key: Vec<u8>,
    /// Further handles whose commit tokens and fences the loop sweeps, beside
    /// its own handle's (Loams Live's keyspaces, for example).
    pub sweep: Vec<Tikv>,
}

impl Default for GcConfig {
    fn default() -> Self {
        GcConfig {
            life_time: crate::DEFAULT_GC_LIFE_TIME,
            interval: DEFAULT_GC_INTERVAL,
            lease_key: GC_LEASE_KEY.to_vec(),
            sweep: Vec::new(),
        }
    }
}

impl GcConfig {
    /// How long the lease lasts without a renewal: three intervals, at least
    /// 30 s.
    fn lease_ttl(&self) -> Duration {
        self.interval.saturating_mul(3).max(Duration::from_secs(30))
    }

    fn validate(&self, tikv: &Tikv) -> Result<(), TikvError> {
        if self.interval.is_zero() {
            return Err(TikvError::Config(
                "GcConfig.interval must be positive".to_string(),
            ));
        }
        if self.life_time < Duration::from_secs(1) {
            return Err(TikvError::Config(
                "GcConfig.life_time must be at least 1 s".to_string(),
            ));
        }
        if self.lease_key.is_empty() {
            return Err(TikvError::Config(
                "GcConfig.lease_key must not be empty".to_string(),
            ));
        }
        // The feature `faults` (tests only) allows a shorter life time, so a
        // test can make locks fall below the safe point in seconds.
        if !cfg!(feature = "faults") && self.life_time < tikv.gc_life_time {
            return Err(TikvError::Config(format!(
                "GcConfig.life_time ({:?}) is shorter than the handle's gc_life_time ({:?}): \
                 GC would drop versions its reads still consider safe",
                self.life_time, tikv.gc_life_time
            )));
        }
        Ok(())
    }
}

/// What one run of the loop did.
#[derive(Debug, Clone, PartialEq)]
pub struct GcReport {
    /// `now − life_time`.
    pub target: Timestamp,
    /// The cluster GC safe point after the run (PD never moves it back, so
    /// it can exceed what this run asked for).
    pub safe_point: Timestamp,
    /// The keyspaces whose locks were resolved (every state but `TOMBSTONE`).
    pub keyspaces: u32,
    /// Locks found below the safe point and resolved.
    pub locks_resolved: u64,
    /// Expired commit tokens and fences deleted.
    pub tokens_swept: u64,
    /// The service safe point that held the safe point below the target.
    pub held_by: Option<String>,
    /// Why the token sweep stopped early, if it did; the safe point has
    /// advanced regardless.
    pub sweep_error: Option<String>,
}

/// The cluster MVCC GC loop: one per cluster, held by a lease.
pub struct GcLoop {
    tikv: Tikv,
    config: GcConfig,
    holder: [u8; 16],
    clients: tokio::sync::Mutex<HashMap<(u32, String), Arc<TransactionClient>>>,
}

impl std::fmt::Debug for GcLoop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcLoop")
            .field("tikv", &self.tikv)
            .field("config", &self.config)
            .field("holder", &hex(&self.holder))
            .finish_non_exhaustive()
    }
}

/// A spawned [`GcLoop`].
#[derive(Debug)]
pub struct GcHandle {
    gc: Arc<GcLoop>,
    last: Arc<Mutex<Option<Result<GcReport, String>>>>,
    task: JoinHandle<()>,
}

impl GcHandle {
    /// The loop.
    pub fn gc_loop(&self) -> &GcLoop {
        &self.gc
    }

    /// The outcome of the latest run, if one finished.
    pub fn last(&self) -> Option<Result<GcReport, String>> {
        self.last.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Waits for the loop to stop (after its shutdown token is cancelled).
    pub async fn stopped(self) {
        let _ = self.task.await;
    }
}

impl GcLoop {
    /// A loop on `tikv` (a handle on `loams_meta`), not yet running.
    pub fn new(tikv: Tikv, config: GcConfig) -> Result<Self, TikvError> {
        config.validate(&tikv)?;
        Ok(GcLoop {
            tikv,
            config,
            holder: rand::random(),
            clients: tokio::sync::Mutex::new(HashMap::new()),
        })
    }

    /// Starts the loop: a run now, then one every `interval`, until
    /// `shutdown` is cancelled. A run that finds the lease held by another
    /// loop does nothing.
    pub fn spawn(
        tikv: Tikv,
        config: GcConfig,
        shutdown: CancellationToken,
    ) -> Result<GcHandle, TikvError> {
        let gc = Arc::new(GcLoop::new(tikv, config)?);
        let last = Arc::new(Mutex::new(None));
        let task = tokio::spawn({
            let gc = gc.clone();
            let last = last.clone();
            async move {
                loop {
                    let outcome = tokio::select! {
                        () = shutdown.cancelled() => return,
                        outcome = gc.run_once() => outcome,
                    };
                    match &outcome {
                        Ok(report) => tracing::info!(
                            safe_point = report.safe_point.version(),
                            keyspaces = report.keyspaces,
                            locks = report.locks_resolved,
                            tokens = report.tokens_swept,
                            held_by = report.held_by.as_deref().unwrap_or(""),
                            "cluster GC run"
                        ),
                        Err(TikvError::GcLease { holder }) => {
                            tracing::debug!(holder = %holder, "GC lease held by another loop");
                        }
                        Err(e) => tracing::warn!(error = %e, "cluster GC run failed"),
                    }
                    *last.lock().unwrap_or_else(|e| e.into_inner()) =
                        Some(outcome.map_err(|e| e.to_string()));
                    tokio::select! {
                        () = shutdown.cancelled() => return,
                        () = tokio::time::sleep(gc.config.interval) => {}
                    }
                }
            }
        });
        Ok(GcHandle { gc, last, task })
    }

    /// One run: the lease, the service safe point, locks in every keyspace,
    /// `UpdateGCSafePoint`, then the token sweep. Fails with
    /// [`TikvError::GcLease`] when another loop holds the lease.
    pub async fn run_once(&self) -> Result<GcReport, TikvError> {
        let epoch = lease::acquire(
            &self.tikv,
            &self.config.lease_key,
            self.holder,
            self.config.lease_ttl(),
        )
        .await?;
        let cluster = LiveCluster { gc: self, epoch };
        let round = round(&cluster, self.config.life_time).await?;
        let mut tokens_swept = 0;
        let mut sweep_error = None;
        for tikv in std::iter::once(&self.tikv).chain(&self.config.sweep) {
            match sweep_tokens(tikv).await {
                Ok(n) => tokens_swept += n,
                Err(e) => {
                    tracing::warn!(keyspace = tikv.keyspace(), error = %e, "token sweep failed");
                    sweep_error = Some(format!("{}: {e}", tikv.keyspace()));
                    break;
                }
            }
        }
        Ok(GcReport {
            target: Timestamp::from_version(round.target),
            safe_point: Timestamp::from_version(round.safe_point),
            keyspaces: round.keyspaces,
            locks_resolved: round.locks_resolved,
            tokens_swept,
            held_by: round.held_by,
            sweep_error,
        })
    }

    /// The keyspace-scoped client of `ks`: the handle's own for its keyspace,
    /// else one built (and kept) for it.
    async fn client_for(&self, ks: &KeyspaceMeta) -> Result<Arc<TransactionClient>, TikvError> {
        if ks.name == self.tikv.keyspace() {
            return Ok(self.tikv.client());
        }
        let key = (ks.id, ks.name.clone());
        let mut clients = self.clients.lock().await;
        if let Some(client) = clients.get(&key) {
            return Ok(client.clone());
        }
        let (pd, config, limit) = self.tikv.clients.blueprint();
        let config = config.clone().with_keyspace(&ks.name);
        let built = tokio::time::timeout(
            limit,
            TransactionClient::new_with_config(pd.to_vec(), config),
        )
        .await
        .map_err(|_| TikvError::GcKeyspace {
            keyspace: ks.name.clone(),
            message: format!("building a client timed out after {limit:?}"),
        })?
        .map_err(|e| TikvError::GcKeyspace {
            keyspace: ks.name.clone(),
            message: format!("building a client: {}", TikvError::from(e)),
        })?;
        let client = Arc::new(built);
        clients.insert(key, client.clone());
        Ok(client)
    }
}

/// The steps of one run that talk to the cluster; a trait so the round's
/// logic is tested without one.
pub(crate) trait GcCluster: Sync {
    /// A fresh TSO version.
    fn now(&self) -> impl Future<Output = Result<u64, TikvError>> + Send;
    /// `UpdateServiceGCSafePoint("gc_worker", i64::MAX, target)`.
    fn gc_worker_safe_point(
        &self,
        target: u64,
    ) -> impl Future<Output = Result<MinServiceSafePoint, TikvError>> + Send;
    /// Every keyspace, in every state.
    fn keyspaces(&self) -> impl Future<Output = Result<Vec<KeyspaceMeta>, TikvError>> + Send;
    /// Resolves the locks of `keyspace` below `safe_point`; the count.
    fn cleanup_locks(
        &self,
        keyspace: &KeyspaceMeta,
        safe_point: u64,
    ) -> impl Future<Output = Result<u64, TikvError>> + Send;
    /// Fails unless the lease is still held at the run's epoch.
    fn confirm_lease(&self) -> impl Future<Output = Result<(), TikvError>> + Send;
    /// `UpdateGCSafePoint`; the cluster safe point after it.
    fn update_gc_safe_point(
        &self,
        safe_point: u64,
    ) -> impl Future<Output = Result<u64, TikvError>> + Send;
}

/// What the cluster part of a run did (TSO versions).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Round {
    pub(crate) target: u64,
    pub(crate) safe_point: u64,
    pub(crate) keyspaces: u32,
    pub(crate) locks_resolved: u64,
    pub(crate) held_by: Option<String>,
}

/// Steps 2–4 of a run (module docs). Any error returns before
/// `UpdateGCSafePoint`, so the safe point never passes an unresolved lock.
pub(crate) async fn round(
    cluster: &impl GcCluster,
    life_time: Duration,
) -> Result<Round, TikvError> {
    let now = cluster.now().await?;
    let target = version_before(now, life_time);
    let min = cluster.gc_worker_safe_point(target).await?;
    let (safe_point, held_by) = if min.safe_point < target {
        (min.safe_point, Some(min.service_id))
    } else {
        (target, None)
    };
    let keyspaces = cluster.keyspaces().await?;
    let mut count = 0u32;
    let mut locks_resolved = 0u64;
    for ks in keyspaces.iter().filter(|k| k.state != TOMBSTONE) {
        locks_resolved += cluster.cleanup_locks(ks, safe_point).await?;
        count += 1;
        // Renews the lease, so a run longer than its TTL keeps it; a lost
        // lease stops the run here instead of after every keyspace.
        cluster.confirm_lease().await?;
    }
    cluster.confirm_lease().await?;
    let safe_point = cluster.update_gc_safe_point(safe_point).await?;
    Ok(Round {
        target,
        safe_point,
        keyspaces: count,
        locks_resolved,
        held_by,
    })
}

/// The TSO version `d` before `version` (logical part zero).
fn version_before(version: u64, d: Duration) -> u64 {
    let ts = Timestamp::from_version(version);
    let back = i64::try_from(d.as_millis()).unwrap_or(i64::MAX);
    Timestamp {
        physical: ts.physical.saturating_sub(back).max(0),
        logical: 0,
        suffix_bits: 0,
    }
    .version()
}

struct LiveCluster<'a> {
    gc: &'a GcLoop,
    epoch: u64,
}

impl GcCluster for LiveCluster<'_> {
    async fn now(&self) -> Result<u64, TikvError> {
        Ok(self.gc.tikv.now().await?.version())
    }

    async fn gc_worker_safe_point(&self, target: u64) -> Result<MinServiceSafePoint, TikvError> {
        self.gc
            .tikv
            .pd
            .update_service_safe_point(GC_WORKER_SERVICE, i64::MAX, target)
            .await
    }

    async fn keyspaces(&self) -> Result<Vec<KeyspaceMeta>, TikvError> {
        keyspace::list(&self.gc.tikv.http, self.gc.tikv.pd_http()).await
    }

    async fn cleanup_locks(&self, ks: &KeyspaceMeta, safe_point: u64) -> Result<u64, TikvError> {
        let client = self.gc.client_for(ks).await?;
        let options = tikv_client::transaction::ResolveLocksOptions {
            async_commit_only: false,
            batch_size: 1024,
        };
        let result = client
            .cleanup_locks(.., &Timestamp::from_version(safe_point), options)
            .await
            .map_err(|e| TikvError::GcKeyspace {
                keyspace: ks.name.clone(),
                message: format!("resolving locks: {}", TikvError::from(e)),
            })?;
        Ok(u64::try_from(result.resolved_locks).unwrap_or(u64::MAX))
    }

    async fn confirm_lease(&self) -> Result<(), TikvError> {
        lease::confirm(
            &self.gc.tikv,
            &self.gc.config.lease_key,
            self.gc.holder,
            self.epoch,
            self.gc.config.lease_ttl(),
        )
        .await
    }

    async fn update_gc_safe_point(&self, safe_point: u64) -> Result<u64, TikvError> {
        let after = self.gc.tikv.pd.update_gc_safe_point(safe_point).await?;
        self.gc.tikv.note_gc_safe_point(after);
        Ok(after)
    }
}

/// Deletes the expired commit tokens and fences under `tikv`'s root; the
/// count. A value that is neither is left alone.
async fn sweep_tokens(tikv: &Tikv) -> Result<u64, TikvError> {
    let end = codec::tuple::successor(TOKEN_PREFIX);
    let mut start = TOKEN_PREFIX.to_vec();
    let mut swept = 0;
    loop {
        let now = tikv.now().await?;
        let now_ms = Tikv::physical_ms(&now);
        let mut snap = tikv.snapshot(now).await?;
        let page = snap.scan(&start, Some(&end), SWEEP_PAGE).await?;
        let expired: Vec<Vec<u8>> = page
            .iter()
            .filter(|(_, v)| token_expiry(v).is_some_and(|e| e < now_ms))
            .map(|(k, _)| k.clone())
            .collect();
        if !expired.is_empty() {
            let done = tikv
                .run(TxnOptions::new("gc.sweep_tokens"), move |txn| {
                    let expired = expired.clone();
                    Box::pin(async move {
                        let now_ms = Tikv::physical_ms(&txn.start_ts());
                        let mut n = 0u64;
                        for (k, v) in txn.batch_get(expired.iter()).await? {
                            if token_expiry(&v).is_some_and(|e| e < now_ms) {
                                txn.delete(&k).await?;
                                n += 1;
                            }
                        }
                        Ok::<u64, TxnError>(n)
                    })
                })
                .await?;
            swept += done.value;
        }
        match page.last() {
            Some((last, _)) if page.len() == SWEEP_PAGE => {
                start = last.clone();
                start.push(0);
            }
            _ => return Ok(swept),
        }
    }
}

/// The GC loop's lease: `holder (16 bytes) ‖ epoch (u64 BE) ‖ expires_ms (u64
/// BE)` at the lease key. A new holder bumps the epoch; the run confirms the
/// epoch before it moves the safe point, so a loop that lost the lease
/// mid-run stops there.
mod lease {
    use std::time::Duration;

    use super::hex;
    use crate::runner::{TxnError, TxnOptions};
    use crate::{Tikv, TikvError};

    const LEN: usize = 32;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct Lease {
        pub(super) holder: [u8; 16],
        pub(super) epoch: u64,
        pub(super) expires_ms: u64,
    }

    impl Lease {
        pub(super) fn encode(&self) -> Vec<u8> {
            let mut v = Vec::with_capacity(LEN);
            v.extend_from_slice(&self.holder);
            v.extend_from_slice(&self.epoch.to_be_bytes());
            v.extend_from_slice(&self.expires_ms.to_be_bytes());
            v
        }

        pub(super) fn decode(v: &[u8]) -> Option<Lease> {
            if v.len() != LEN {
                return None;
            }
            Some(Lease {
                holder: v[..16].try_into().ok()?,
                epoch: u64::from_be_bytes(v[16..24].try_into().ok()?),
                expires_ms: u64::from_be_bytes(v[24..32].try_into().ok()?),
            })
        }
    }

    /// The next lease for `me` at `now_ms`, or the lease that stops it.
    pub(super) fn next(
        current: Option<Lease>,
        me: [u8; 16],
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<Lease, Lease> {
        let expires_ms = now_ms.saturating_add(ttl_ms);
        match current {
            None => Ok(Lease {
                holder: me,
                epoch: 1,
                expires_ms,
            }),
            Some(l) if l.holder == me => Ok(Lease { expires_ms, ..l }),
            Some(l) if l.expires_ms < now_ms => Ok(Lease {
                holder: me,
                epoch: l.epoch + 1,
                expires_ms,
            }),
            Some(l) => Err(l),
        }
    }

    fn ttl_ms(ttl: Duration) -> u64 {
        u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)
    }

    /// Takes or renews the lease; its epoch.
    pub(super) async fn acquire(
        tikv: &Tikv,
        key: &[u8],
        me: [u8; 16],
        ttl: Duration,
    ) -> Result<u64, TikvError> {
        let ttl = ttl_ms(ttl);
        let key = key.to_vec();
        let outcome = tikv
            .run(TxnOptions::new("gc.lease"), move |txn| {
                let key = key.clone();
                Box::pin(async move {
                    let now_ms = Tikv::physical_ms(&txn.start_ts());
                    let current = txn.get(&key).await?.as_deref().and_then(Lease::decode);
                    match next(current, me, now_ms, ttl) {
                        Ok(lease) => {
                            txn.put(&key, lease.encode()).await?;
                            Ok::<_, TxnError>(Ok(lease.epoch))
                        }
                        Err(held) => Ok(Err(held)),
                    }
                })
            })
            .await?;
        outcome.value.map_err(|held| TikvError::GcLease {
            holder: hex(&held.holder),
        })
    }

    /// Renews the lease if `me` still holds it at `epoch`; else
    /// [`TikvError::GcLease`].
    pub(super) async fn confirm(
        tikv: &Tikv,
        key: &[u8],
        me: [u8; 16],
        epoch: u64,
        ttl: Duration,
    ) -> Result<(), TikvError> {
        let ttl = ttl_ms(ttl);
        let key = key.to_vec();
        let outcome = tikv
            .run(TxnOptions::new("gc.lease"), move |txn| {
                let key = key.clone();
                Box::pin(async move {
                    let now_ms = Tikv::physical_ms(&txn.start_ts());
                    match txn.get(&key).await?.as_deref().and_then(Lease::decode) {
                        Some(l) if l.holder == me && l.epoch == epoch => {
                            let renewed = Lease {
                                expires_ms: now_ms.saturating_add(ttl),
                                ..l
                            };
                            txn.put(&key, renewed.encode()).await?;
                            Ok::<_, TxnError>(Ok(()))
                        }
                        Some(l) => Ok(Err(hex(&l.holder))),
                        None => Ok(Err("nobody".to_string())),
                    }
                })
            })
            .await?;
        outcome
            .value
            .map_err(|holder| TikvError::GcLease { holder })
    }
}

/// A GC barrier: a PD service safe point that holds the cluster GC safe
/// point at or below a timestamp until it is deleted or its TTL expires.
///
/// Service ids are `loams/<purpose>/<id>` ([`GcBarrier::service_id`]). A
/// barrier set through a handle also lets that handle's
/// [`Tikv::snapshot`] read below the GC safe window at the timestamps it
/// covers, while it lives.
#[derive(Debug, Clone)]
pub struct GcBarrier {
    tikv: Tikv,
}

impl GcBarrier {
    /// Barriers set through `tikv` (and known to its clones' snapshots).
    pub fn new(tikv: &Tikv) -> Self {
        GcBarrier { tikv: tikv.clone() }
    }

    /// `loams/<purpose>/<id>`.
    pub fn service_id(purpose: &str, id: &str) -> String {
        format!("{BARRIER_PREFIX}{purpose}/{id}")
    }

    /// Holds GC at or below `ts` for `ttl` (whole seconds, at least 1):
    /// `UpdateServiceGCSafePoint(service_id, ttl, ts)`. Setting it again
    /// moves it. Refused with [`TikvError::BarrierBelowSafePoint`] when `ts`
    /// is below the current minimum service safe point (PD then saves
    /// nothing: GC may already be past `ts`).
    pub async fn set(
        &self,
        service_id: &str,
        ts: &Timestamp,
        ttl: Duration,
    ) -> Result<(), TikvError> {
        check_service_id(service_id)?;
        let ttl_secs = i64::try_from(ttl.as_secs()).unwrap_or(i64::MAX).max(1);
        let version = ts.version();
        // The local expiry is measured from before the request, so it never
        // outlives PD's.
        let sent = Instant::now();
        let min = self
            .tikv
            .pd
            .update_service_safe_point(service_id, ttl_secs, version)
            .await?;
        if min.safe_point > version {
            self.tikv.barriers.remove(service_id);
            return Err(TikvError::BarrierBelowSafePoint {
                service_id: service_id.to_string(),
                ts: version,
                min_safe_point: min.safe_point,
            });
        }
        let expires = sent + Duration::from_secs(u64::try_from(ttl_secs).unwrap_or(u64::MAX));
        self.tikv.barriers.insert(service_id, version, expires);
        Ok(())
    }

    /// Removes the barrier (`ttl = 0`).
    pub async fn delete(&self, service_id: &str) -> Result<(), TikvError> {
        check_service_id(service_id)?;
        self.tikv.barriers.remove(service_id);
        self.tikv
            .pd
            .update_service_safe_point(service_id, 0, 0)
            .await?;
        Ok(())
    }
}

fn check_service_id(service_id: &str) -> Result<(), TikvError> {
    let ok = service_id.len() > BARRIER_PREFIX.len()
        && service_id.starts_with(BARRIER_PREFIX)
        && service_id.len() <= 256;
    if ok {
        Ok(())
    } else {
        Err(TikvError::Config(format!(
            "a GC barrier's service id must be loams/<purpose>/<id>, got '{service_id}'"
        )))
    }
}

/// The barriers set through a handle and its clones: service id →
/// (TSO version, local expiry).
#[derive(Debug, Default)]
pub(crate) struct Barriers {
    set: Mutex<HashMap<String, (u64, Instant)>>,
}

impl Barriers {
    fn insert(&self, service_id: &str, version: u64, expires: Instant) {
        self.lock()
            .insert(service_id.to_string(), (version, expires));
    }

    fn remove(&self, service_id: &str) {
        self.lock().remove(service_id);
    }

    /// Whether a live barrier holds GC at or below `version`.
    pub(crate) fn covers(&self, version: u64) -> bool {
        let now = Instant::now();
        let mut set = self.lock();
        set.retain(|_, (_, expires)| *expires > now);
        set.values().any(|(v, _)| *v <= version)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, (u64, Instant)>> {
        self.set.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// The cached cluster GC safe point of a handle.
#[derive(Debug, Default)]
pub(crate) struct SafePointCache {
    last: Mutex<Option<(u64, Instant)>>,
}

impl Tikv {
    /// The cluster GC safe point (`GetGCSafePoint`), cached for at most 10 s.
    pub async fn gc_safe_point(&self) -> Result<Timestamp, TikvError> {
        let cached = *self
            .safe_point_cache
            .last
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some((v, at)) = cached
            && at.elapsed() < SAFE_POINT_CACHE
        {
            return Ok(Timestamp::from_version(v));
        }
        let v = self.pd.gc_safe_point().await?;
        self.note_gc_safe_point(v);
        Ok(Timestamp::from_version(v))
    }

    /// Records a safe point PD reported; the cache only moves forward.
    pub(crate) fn note_gc_safe_point(&self, version: u64) {
        let mut last = self
            .safe_point_cache
            .last
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let keep = last.map_or(0, |(v, _)| v);
        *last = Some((version.max(keep), Instant::now()));
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::lease::{Lease, next};
    use super::*;

    /// A scripted cluster that records the calls of a round.
    #[derive(Default)]
    struct Fake {
        now: u64,
        min: Option<MinServiceSafePoint>,
        keyspaces: Vec<KeyspaceMeta>,
        fail_client: Option<String>,
        lease_lost: bool,
        pd_safe_point: Mutex<u64>,
        calls: Mutex<Vec<String>>,
    }

    impl Fake {
        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("calls").clone()
        }

        fn log(&self, call: String) {
            self.calls.lock().expect("calls").push(call);
        }
    }

    impl GcCluster for Fake {
        async fn now(&self) -> Result<u64, TikvError> {
            Ok(self.now)
        }

        async fn gc_worker_safe_point(
            &self,
            target: u64,
        ) -> Result<MinServiceSafePoint, TikvError> {
            self.log(format!("service {target}"));
            Ok(self.min.clone().unwrap_or(MinServiceSafePoint {
                service_id: GC_WORKER_SERVICE.to_string(),
                safe_point: target,
            }))
        }

        async fn keyspaces(&self) -> Result<Vec<KeyspaceMeta>, TikvError> {
            Ok(self.keyspaces.clone())
        }

        async fn cleanup_locks(
            &self,
            ks: &KeyspaceMeta,
            safe_point: u64,
        ) -> Result<u64, TikvError> {
            if self.fail_client.as_deref() == Some(ks.name.as_str()) {
                return Err(TikvError::GcKeyspace {
                    keyspace: ks.name.clone(),
                    message: "building a client: refused".to_string(),
                });
            }
            self.log(format!("locks {} {safe_point}", ks.name));
            Ok(u64::from(ks.id))
        }

        async fn confirm_lease(&self) -> Result<(), TikvError> {
            if self.lease_lost {
                return Err(TikvError::GcLease {
                    holder: "other".to_string(),
                });
            }
            Ok(())
        }

        async fn update_gc_safe_point(&self, safe_point: u64) -> Result<u64, TikvError> {
            self.log(format!("update {safe_point}"));
            let mut pd = self.pd_safe_point.lock().expect("pd");
            *pd = (*pd).max(safe_point);
            Ok(*pd)
        }
    }

    fn ks(id: u32, name: &str, state: &str) -> KeyspaceMeta {
        KeyspaceMeta {
            id,
            name: name.to_string(),
            state: state.to_string(),
            created_at: 0,
            state_changed_at: 0,
            config: Default::default(),
        }
    }

    fn ts(ms: i64) -> u64 {
        Timestamp {
            physical: ms,
            logical: 0,
            suffix_bits: 0,
        }
        .version()
    }

    fn keyspaces() -> Vec<KeyspaceMeta> {
        vec![
            ks(0, "DEFAULT", "ENABLED"),
            ks(1, "loams_meta", "ENABLED"),
            ks(2, "old", "DISABLED"),
            ks(3, "archived", "ARCHIVED"),
            ks(4, "gone", "TOMBSTONE"),
        ]
    }

    #[tokio::test]
    async fn round_resolves_every_keyspace_but_tombstones_then_advances() {
        let fake = Fake {
            now: ts(1_000_000),
            keyspaces: keyspaces(),
            ..Fake::default()
        };
        let r = round(&fake, Duration::from_secs(600)).await.expect("round");
        let target = ts(1_000_000 - 600_000);
        assert_eq!(r.target, target);
        assert_eq!(r.safe_point, target);
        assert_eq!(r.keyspaces, 4);
        assert_eq!(r.locks_resolved, 1 + 2 + 3);
        assert_eq!(r.held_by, None);
        assert_eq!(
            fake.calls(),
            vec![
                format!("service {target}"),
                format!("locks DEFAULT {target}"),
                format!("locks loams_meta {target}"),
                format!("locks old {target}"),
                format!("locks archived {target}"),
                format!("update {target}"),
            ]
        );
    }

    #[tokio::test]
    async fn a_service_safe_point_holds_the_round_back() {
        let barrier = ts(100_000);
        let fake = Fake {
            now: ts(1_000_000),
            min: Some(MinServiceSafePoint {
                service_id: "loams/test/x".to_string(),
                safe_point: barrier,
            }),
            keyspaces: vec![ks(1, "loams_meta", "ENABLED")],
            ..Fake::default()
        };
        let r = round(&fake, Duration::from_secs(600)).await.expect("round");
        assert_eq!(r.safe_point, barrier);
        assert_eq!(r.held_by.as_deref(), Some("loams/test/x"));
        assert!(
            fake.calls()
                .contains(&format!("locks loams_meta {barrier}"))
        );
    }

    #[tokio::test]
    async fn a_keyspace_without_a_client_stops_the_round_before_the_safe_point_moves() {
        let fake = Fake {
            now: ts(1_000_000),
            keyspaces: keyspaces(),
            fail_client: Some("old".to_string()),
            ..Fake::default()
        };
        let err = round(&fake, Duration::from_secs(600))
            .await
            .expect_err("the round must fail");
        assert!(matches!(err, TikvError::GcKeyspace { ref keyspace, .. } if keyspace == "old"));
        assert!(!fake.calls().iter().any(|c| c.starts_with("update")));
        assert_eq!(*fake.pd_safe_point.lock().expect("pd"), 0);
    }

    #[tokio::test]
    async fn a_lost_lease_stops_the_round_before_the_safe_point_moves() {
        let fake = Fake {
            now: ts(1_000_000),
            keyspaces: keyspaces(),
            lease_lost: true,
            ..Fake::default()
        };
        assert!(matches!(
            round(&fake, Duration::from_secs(600)).await,
            Err(TikvError::GcLease { .. })
        ));
        assert!(!fake.calls().iter().any(|c| c.starts_with("update")));
        // The lease is confirmed (and renewed) after each keyspace, so the
        // loss stops the run after the first one.
        let resolved = fake
            .calls()
            .iter()
            .filter(|c| c.starts_with("locks"))
            .count();
        assert_eq!(resolved, 1, "{:?}", fake.calls());
    }

    #[tokio::test]
    async fn the_report_carries_the_cluster_safe_point_when_it_is_ahead() {
        let fake = Fake {
            now: ts(1_000_000),
            keyspaces: vec![ks(1, "loams_meta", "ENABLED")],
            pd_safe_point: Mutex::new(ts(999_000)),
            ..Fake::default()
        };
        let r = round(&fake, Duration::from_secs(600)).await.expect("round");
        assert_eq!(r.target, ts(400_000));
        assert_eq!(r.safe_point, ts(999_000));
    }

    #[test]
    fn version_before_subtracts_physical_time() {
        let now = Timestamp {
            physical: 10_000,
            logical: 7,
            suffix_bits: 0,
        }
        .version();
        assert_eq!(version_before(now, Duration::from_secs(3)), ts(7_000));
        assert_eq!(version_before(now, Duration::from_secs(60)), 0);
    }

    #[test]
    fn lease_rules() {
        let me = [1; 16];
        let other = [2; 16];
        let fresh = next(None, me, 1_000, 100).expect("free");
        assert_eq!(
            (fresh.holder, fresh.epoch, fresh.expires_ms),
            (me, 1, 1_100)
        );
        let renewed = next(Some(fresh), me, 1_050, 100).expect("mine");
        assert_eq!((renewed.epoch, renewed.expires_ms), (1, 1_150));
        let held = next(Some(renewed), other, 1_100, 100).expect_err("held");
        assert_eq!(held.holder, me);
        let taken = next(Some(renewed), other, 1_151, 100).expect("expired");
        assert_eq!((taken.holder, taken.epoch), (other, 2));
        assert_eq!(Lease::decode(&taken.encode()), Some(taken));
        assert_eq!(Lease::decode(b"short"), None);
    }

    #[test]
    fn barrier_ids_are_checked_and_barriers_expire() {
        assert_eq!(GcBarrier::service_id("snap", "7"), "loams/snap/7");
        assert!(check_service_id("loams/snap/7").is_ok());
        assert!(check_service_id("gc_worker").is_err());
        assert!(check_service_id("loams/").is_err());
        let b = Barriers::default();
        assert!(!b.covers(10));
        b.insert("loams/a/1", 10, Instant::now() + Duration::from_secs(60));
        assert!(b.covers(10) && b.covers(11) && !b.covers(9));
        b.insert("loams/a/2", 5, Instant::now());
        assert!(!b.covers(9), "an expired barrier covers nothing");
        b.remove("loams/a/1");
        assert!(!b.covers(11));
    }
}
