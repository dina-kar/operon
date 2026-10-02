//! Loams's [`MetaStore`](loams_common::meta::MetaStore) on TiKV (R1 plan
//! Tasks 4–5; design §20 §11, D124).
//!
//! [`TikvMeta`] maps each trait method onto one TiKV transaction through
//! `loams-tikv`'s runner, with the key layout of §20 §11.2 (module `keys`)
//! under the handle's root prefix. Writes carry a commit token and are rerun
//! as the trait's retry-safe rules describe when an attempt's outcome was
//! unknown; they are optimistic except the three log writes below. Reads use one snapshot at a fresh TSO
//! timestamp, so every read is `Linearizable` (`Local` is served the same
//! way, as the trait allows). The clock is the TSO's physical part:
//! [`clock_ms`](loams_common::meta::MetaStore::clock_ms) fetches a fresh
//! timestamp, and the synchronous `now_ms` extrapolates from the latest
//! timestamp the handle saw (row R2). Ids come from per-handle blocks of
//! [`TikvMetaConfig::id_block`] (gaps allowed, D18).
//!
//! `commit_wal`, `swap_segment` and `trim_partition` are pessimistic
//! transactions that lock the partition heads they touch (R1 Ruling 2);
//! `commit_wal` commits one transaction per partition group (module `log`).

mod catalog;
mod changes;
mod gc;
mod idempotency;
mod invariants;
mod keys;
mod leases;
mod log;
mod pointers;
mod store;

pub use log::{MAX_CLOCK_SKEW_MS, MAX_GROUP_BYTES, MAX_GROUP_CHUNKS};

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;
use loams_common::meta::{ApplyError, MetaError, MetaResult};
use loams_tikv::{
    CommitMode, Mode, Pair, Snap, Tikv, TikvConfig, TikvError, Txn, TxnError, TxnOptions,
};

use crate::keys::IdKind;

/// The default id block: each handle takes 1 000 ids of a kind at a time.
pub const DEFAULT_ID_BLOCK: u64 = 1_000;

/// The default change-watch poll.
pub const DEFAULT_POLL: Duration = Duration::from_millis(100);

/// How often a write whose outcome was unknown is rerun before the call gives
/// up with an unknown outcome.
const UNKNOWN_RERUNS: u32 = 3;

/// The longest a stamped write waits for the TSO to reach `now_ms`.
const STAMP_WAIT: Duration = Duration::from_secs(1);

/// Attempts of a pessimistic write (row T5-2).
const PESSIMISTIC_ATTEMPTS: u32 = 64;

/// How often a read is retried after a conflict (a lock it met) or an error
/// after which it certainly read nothing.
const READ_ATTEMPTS: u32 = 4;

/// How every metastore write commits: classic two-phase commit, not the
/// handle's default async commit with 1PC (R1 Ruling 3's switch back, row
/// T6-5). Upstream `tikv-client` resolved a reader's async-commit locks only
/// through `CheckTxnStatus`, which never rolls back or commits an
/// async-commit primary, so the locks of a write that failed after its
/// prewrite blocked every reader and writer of those keys until the cluster
/// GC loop resolved them, about ten minutes later. Loams's fork resolves them
/// on the read path (`CheckSecondaryLocks`, row F1), so that reason is gone;
/// switching back to async commit waits for an owner ruling (row F4), because
/// hot-head contention under load was not yet reliable with it.
pub const COMMIT_MODE: CommitMode = CommitMode::TwoPc;

/// How a [`TikvMeta`] reaches its cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TikvMetaConfig {
    /// The TiKV handle: the cluster, the metastore keyspace (`loams_meta`) and
    /// the root prefix.
    pub tikv: TikvConfig,
    /// How many ids of a kind a handle takes at a time (default 1 000).
    pub id_block: u64,
    /// How often a change watch wakes on its own (default 100 ms).
    pub poll: Duration,
}

impl TikvMetaConfig {
    /// A configuration with the default id block and poll.
    pub fn new(tikv: TikvConfig) -> Self {
        TikvMetaConfig {
            tikv,
            id_block: DEFAULT_ID_BLOCK,
            poll: DEFAULT_POLL,
        }
    }
}

/// The TiKV metastore. Cheap to clone; clones share the id blocks, the clock
/// and the change watch.
#[derive(Clone)]
pub struct TikvMeta {
    inner: Arc<Inner>,
}

struct Inner {
    tikv: Tikv,
    id_block: u64,
    poll: Duration,
    ids: tokio::sync::Mutex<HashMap<IdKind, Block>>,
    /// The largest `now_ms` returned, so it never goes backwards.
    last_now: AtomicU64,
    /// Bumped after every write of this handle: wakes its change watches at
    /// once (the poll wakes them for other handles' writes).
    changes: tokio::sync::watch::Sender<u64>,
}

/// The ids `next..end` of a kind this handle may still hand out.
#[derive(Debug, Clone, Copy, Default)]
struct Block {
    next: u64,
    end: u64,
}

impl fmt::Debug for TikvMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TikvMeta")
            .field("tikv", &self.inner.tikv)
            .field("id_block", &self.inner.id_block)
            .field("poll", &self.inner.poll)
            .finish_non_exhaustive()
    }
}

impl TikvMeta {
    /// Connects to the cluster and opens the metastore under the
    /// configuration's keyspace and root.
    pub async fn open(config: TikvMetaConfig) -> Result<Self, MetaError> {
        let tikv = Tikv::connect(config.tikv).await.map_err(tikv_error)?;
        Self::open_on(tikv, config.id_block, config.poll).await
    }

    /// Opens the metastore on an existing handle (tests pass one with a
    /// fault plan). Fetches one TSO timestamp, which anchors `now_ms`.
    pub async fn open_on(tikv: Tikv, id_block: u64, poll: Duration) -> Result<Self, MetaError> {
        if id_block == 0 {
            return Err(MetaError::Config("id_block must be at least 1".to_string()));
        }
        if poll.is_zero() {
            return Err(MetaError::Config("poll must be above zero".to_string()));
        }
        let first = tikv.now().await.map_err(tikv_error)?;
        let (changes, _) = tokio::sync::watch::channel(0);
        Ok(TikvMeta {
            inner: Arc::new(Inner {
                last_now: AtomicU64::new(Tikv::physical_ms(&first)),
                tikv,
                id_block,
                poll,
                ids: tokio::sync::Mutex::new(HashMap::new()),
                changes,
            }),
        })
    }

    /// The TiKV handle under the metastore.
    pub fn tikv(&self) -> &Tikv {
        &self.inner.tikv
    }

    /// `max(previous, physical(latest TSO timestamp) + time since it
    /// arrived)`: monotonic, and never behind the TSO (row R2).
    fn now_estimate(&self) -> u64 {
        let estimate = match self.inner.tikv.latest_timestamp() {
            Some((ts, arrived)) => Tikv::physical_ms(&ts)
                .saturating_add(u64::try_from(arrived.elapsed().as_millis()).unwrap_or(u64::MAX)),
            None => 0,
        };
        let previous = self.inner.last_now.fetch_max(estimate, Ordering::AcqRel);
        previous.max(estimate)
    }

    /// Waits until the TSO's physical time reaches this handle's `now_ms`,
    /// before a write the trait says is "stamped with `now_ms`"
    /// (`swap_segment`, `trim_partition`, `prune_wal_commits`,
    /// `drop_collection`), so the write's start timestamp, and the metastore
    /// clock after it, are not behind the caller's clock (row T5-7). PD
    /// advances the physical part in steps, so the extrapolated `now_ms` can
    /// run up to one step ahead of it. Gives up after [`STAMP_WAIT`].
    async fn reach_now(&self) {
        let stamp = self.now_estimate();
        let deadline = std::time::Instant::now() + STAMP_WAIT;
        while let Ok(ts) = self.inner.tikv.now().await {
            let physical = Tikv::physical_ms(&ts);
            if physical >= stamp || std::time::Instant::now() >= deadline {
                return;
            }
            let behind = Duration::from_millis((stamp - physical).clamp(1, 50));
            tokio::time::sleep(behind).await;
        }
    }

    /// The next id of `kind`, taking a new block when this handle's is used
    /// up. A block is taken in its own transaction, so ids a handle never
    /// hands out (a rejected create, a handle that stops) leave gaps.
    async fn next_id(&self, kind: IdKind) -> MetaResult<u64> {
        let mut ids = self.inner.ids.lock().await;
        let block = ids.entry(kind).or_default();
        if block.next == block.end {
            let size = self.inner.id_block;
            let key = keys::id_block(kind);
            let first = self
                .inner
                .tikv
                .run(
                    TxnOptions {
                        commit_mode: Some(COMMIT_MODE),
                        ..TxnOptions::new("meta.id_block")
                    }
                    .with_token(),
                    move |txn| {
                        let key = key.clone();
                        Box::pin(async move {
                            let first = match txn.get(&key).await? {
                                Some(v) => keys::decode_u64("id block", &v)
                                    .map_err(|e| TxnError::Fatal(e.to_string()))?,
                                None => 1,
                            };
                            let end = first.checked_add(size).ok_or_else(|| {
                                TxnError::Fatal("the id space is exhausted".to_string())
                            })?;
                            txn.put(&key, keys::encode_u64(end)).await?;
                            Ok(first)
                        })
                    },
                )
                .await
                .map_err(txn_error)?
                .value;
            *block = Block {
                next: first,
                end: first + size,
            };
        }
        let id = block.next;
        block.next += 1;
        Ok(id)
    }

    /// Runs a write: `body` returns `Ok(Err(rejection))` to reject without a
    /// retry. Returns the result and whether an earlier attempt's outcome was
    /// unknown.
    ///
    /// When an attempt's outcome was unknown, the call is rerun, so the
    /// result is what the trait documents for a retry after a lost
    /// acknowledgement (its first attempt's effect: `NamespaceExists` with
    /// the id, a `VersionMismatch` holding the caller's value, …). That holds
    /// both when the runner could not resolve the outcome (`Undetermined`)
    /// and when it resolved it through the commit token as committed (row
    /// T4-3).
    async fn write<T, F>(&self, op: &'static str, body: F) -> (MetaResult<T>, bool)
    where
        T: Send,
        F: for<'t> FnMut(&'t mut Txn) -> BoxFuture<'t, Result<Result<T, ApplyError>, TxnError>>,
    {
        self.write_in(Mode::Optimistic, op, body).await
    }

    /// [`write`](Self::write) in `mode`. A pessimistic write (`commit_wal`,
    /// `swap_segment`, `trim_partition`: R1 Ruling 2) locks what it reads
    /// with `get_for_update` as it goes, so a hot partition head queues
    /// instead of aborting; its lock waits end in restarts often under load
    /// (`tikv-client` does not retry a lock at a newer `for_update_ts`), so it
    /// gets [`PESSIMISTIC_ATTEMPTS`] attempts within the same deadline.
    async fn write_in<T, F>(
        &self,
        mode: Mode,
        op: &'static str,
        mut body: F,
    ) -> (MetaResult<T>, bool)
    where
        T: Send,
        F: for<'t> FnMut(&'t mut Txn) -> BoxFuture<'t, Result<Result<T, ApplyError>, TxnError>>,
    {
        let options = match mode {
            Mode::Optimistic => TxnOptions::new(op),
            Mode::Pessimistic => TxnOptions {
                max_attempts: PESSIMISTIC_ATTEMPTS,
                ..TxnOptions::pessimistic(op)
            },
        };
        let options = TxnOptions {
            commit_mode: Some(COMMIT_MODE),
            ..options
        }
        .with_token();
        let mut unknown = false;
        let mut outcome = Err(MetaError::Unavailable(format!(
            "{op}: the commit outcome stayed unknown after {UNKNOWN_RERUNS} reruns"
        )));
        for _ in 0..UNKNOWN_RERUNS {
            match self.inner.tikv.run(options.clone(), &mut body).await {
                Ok(committed) if committed.earlier_unknown => unknown = true,
                Ok(committed) => {
                    outcome = committed.value.map_err(MetaError::Rejected);
                    break;
                }
                Err(TxnError::Undetermined { .. }) => unknown = true,
                Err(e) => {
                    outcome = Err(txn_error(e));
                    break;
                }
            }
        }
        if outcome.is_ok() || unknown {
            self.inner.changes.send_modify(|n| *n = n.wrapping_add(1));
        }
        (outcome, unknown)
    }

    /// A write whose earlier unknown attempts the caller does not report.
    async fn write_plain<T, F>(&self, op: &'static str, body: F) -> MetaResult<T>
    where
        T: Send,
        F: for<'t> FnMut(&'t mut Txn) -> BoxFuture<'t, Result<Result<T, ApplyError>, TxnError>>,
    {
        self.write(op, body).await.0
    }

    /// Runs `body` on a snapshot at a fresh TSO timestamp, retrying a few
    /// times after a conflict or an error after which nothing was read.
    async fn read<T, F>(&self, mut body: F) -> MetaResult<T>
    where
        F: for<'s> FnMut(&'s mut Snap) -> BoxFuture<'s, Result<Result<T, ApplyError>, TxnError>>,
    {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let ts = self.inner.tikv.now().await.map_err(tikv_error)?;
            let mut snap = self.inner.tikv.snapshot(ts).await.map_err(tikv_error)?;
            match body(&mut snap).await {
                Ok(result) => return result.map_err(MetaError::Rejected),
                Err(TxnError::Conflict | TxnError::NotApplied(_)) if attempt < READ_ATTEMPTS => {
                    tokio::time::sleep(Duration::from_millis(10 << attempt)).await;
                }
                Err(e) => return Err(txn_error(e)),
            }
        }
    }
}

/// A snapshot at a fresh TSO timestamp, taken inside a pessimistic write
/// after its locks: it sees every commit before the locks, which the
/// transaction's own start-timestamp reads may miss (row T5-3).
async fn fresh_snapshot(tikv: &Tikv) -> Result<Snap, TxnError> {
    let not_applied = |e: TikvError| TxnError::NotApplied(e.to_string());
    let ts = tikv.now().await.map_err(not_applied)?;
    tikv.snapshot(ts).await.map_err(not_applied)
}

/// Maps the runner's final error. Every one of them leaves a write's
/// outcome unknown to the caller or means it did not apply; the trait's
/// retryable errors say "unknown", which covers both.
fn txn_error(e: TxnError) -> MetaError {
    match e {
        TxnError::Deadline => MetaError::Timeout,
        TxnError::Conflict => {
            MetaError::Unavailable("TiKV: transaction conflicts outlasted the retries".to_string())
        }
        TxnError::NotApplied(m) => MetaError::Unavailable(format!("TiKV: {m}")),
        TxnError::Undetermined { .. } => {
            MetaError::Unavailable("TiKV: the commit outcome is undetermined".to_string())
        }
        TxnError::AlreadyExists(m) => MetaError::UnexpectedReply(format!("TiKV: {m}")),
        TxnError::Fatal(m) => MetaError::Storage(std::io::Error::other(format!("TiKV: {m}"))),
    }
}

/// Maps a handle-level error.
fn tikv_error(e: TikvError) -> MetaError {
    match e {
        TikvError::KeyspaceMissing { .. } | TikvError::ApiVersion { .. } | TikvError::Config(_) => {
            MetaError::Config(e.to_string())
        }
        TikvError::Timeout { .. } => MetaError::Timeout,
        TikvError::Txn(e) => txn_error(e),
        other => MetaError::Unavailable(other.to_string()),
    }
}

/// A record decode error inside a transaction body.
fn fatal(e: MetaError) -> TxnError {
    TxnError::Fatal(e.to_string())
}

/// A rejection as the trait returns it.
fn rejected<T>(e: ApplyError) -> MetaResult<T> {
    Err(MetaError::Rejected(e))
}

/// The reads a transaction and a snapshot share, so one lookup serves both.
pub(crate) trait Reader: Send {
    fn get<'a>(&'a mut self, key: &'a [u8]) -> BoxFuture<'a, Result<Option<Vec<u8>>, TxnError>>;
    /// Absent keys are left out; sorted by key.
    fn batch_get(&mut self, keys: Vec<Vec<u8>>) -> BoxFuture<'_, Result<Vec<Pair>, TxnError>>;
    /// Every pair whose key starts with `prefix`, in key order.
    fn scan_prefix<'a>(
        &'a mut self,
        prefix: &'a [u8],
    ) -> BoxFuture<'a, Result<Vec<Pair>, TxnError>>;
}

impl Reader for Txn {
    fn get<'a>(&'a mut self, key: &'a [u8]) -> BoxFuture<'a, Result<Option<Vec<u8>>, TxnError>> {
        Box::pin(Txn::get(self, key))
    }

    fn batch_get(&mut self, keys: Vec<Vec<u8>>) -> BoxFuture<'_, Result<Vec<Pair>, TxnError>> {
        Box::pin(Txn::batch_get(self, keys))
    }

    fn scan_prefix<'a>(
        &'a mut self,
        prefix: &'a [u8],
    ) -> BoxFuture<'a, Result<Vec<Pair>, TxnError>> {
        Box::pin(async move {
            let (lo, hi) = keys::prefix_range(prefix);
            Txn::scan(self, &lo, hi.as_deref(), usize::MAX).await
        })
    }
}

impl Reader for Snap {
    fn get<'a>(&'a mut self, key: &'a [u8]) -> BoxFuture<'a, Result<Option<Vec<u8>>, TxnError>> {
        Box::pin(Snap::get(self, key))
    }

    fn batch_get(&mut self, keys: Vec<Vec<u8>>) -> BoxFuture<'_, Result<Vec<Pair>, TxnError>> {
        Box::pin(Snap::batch_get(self, keys))
    }

    fn scan_prefix<'a>(
        &'a mut self,
        prefix: &'a [u8],
    ) -> BoxFuture<'a, Result<Vec<Pair>, TxnError>> {
        Box::pin(async move {
            let (lo, hi) = keys::prefix_range(prefix);
            Snap::scan(self, &lo, hi.as_deref(), usize::MAX).await
        })
    }
}

/// Reads and decodes one record.
async fn load<T: serde::de::DeserializeOwned>(
    r: &mut dyn Reader,
    what: &str,
    key: &[u8],
) -> Result<Option<T>, TxnError> {
    match r.get(key).await? {
        Some(v) => keys::decode(what, &v).map(Some).map_err(fatal),
        None => Ok(None),
    }
}

/// Reads an id stored at `key` (a name record).
async fn load_id(r: &mut dyn Reader, what: &str, key: &[u8]) -> Result<Option<u64>, TxnError> {
    match r.get(key).await? {
        Some(v) => keys::decode_u64(what, &v).map(Some).map_err(fatal),
        None => Ok(None),
    }
}

/// Decodes every value of `pairs` as a `T`, in order.
fn decode_all<T: serde::de::DeserializeOwned>(
    what: &str,
    pairs: &[Pair],
) -> Result<Vec<T>, TxnError> {
    pairs
        .iter()
        .map(|(_, v)| keys::decode(what, v).map_err(fatal))
        .collect()
}
