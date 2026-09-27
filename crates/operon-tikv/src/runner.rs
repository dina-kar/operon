//! The transaction runner (R1 plan Task 2; design §20 §5.1).
//!
//! [`Tikv::run`] runs a body in a new transaction and commits it. It restarts
//! the whole transaction at a new start timestamp, after a jittered backoff,
//! when the body or the commit hits a conflict (optimistic `WriteConflict`,
//! pessimistic `PessimisticRetry`, a lock the client's backoff did not outwait)
//! or anything after which the transaction certainly did not commit, until
//! `max_attempts` or the deadline. An unknown commit outcome
//! (`Error::UndeterminedError`, or a commit the runner drops at its deadline)
//! is resolved through the commit token when the options ask for one.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use tikv_client::{CheckLevel, Timestamp, TransactionOptions};

use crate::classify::{Class, classify, describe};
use crate::faults::{Fault, FaultPoint};
use crate::token::{Token, fence_value, is_fence, new_token, token_key, token_value};
use crate::txn::Txn;
use crate::{Tikv, TikvError};

/// Optimistic or pessimistic concurrency control.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Mode {
    /// Reads at the start timestamp; conflicts show at commit (the default).
    #[default]
    Optimistic,
    /// `get_for_update` locks at once, so a second holder queues (R1 Ruling 2).
    Pessimistic,
}

/// How a transaction commits (R1 Ruling 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CommitMode {
    /// Async commit, and one-phase commit when the transaction fits one
    /// region: `use_async_commit()` + `try_one_pc()` (the default).
    #[default]
    Async1pc,
    /// Classic two-phase commit, the switch back if async commit misbehaves.
    TwoPc,
}

/// The options of one [`Tikv::run`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOptions {
    pub mode: Mode,
    /// Attempts before the runner gives up (default 8).
    pub max_attempts: u32,
    /// The wall-clock budget of the whole run (default 10 s).
    pub deadline: Duration,
    /// Write a commit token, so an unknown outcome can be resolved.
    pub commit_token: bool,
    /// The operation's name, for the fault plan and metrics.
    pub op: &'static str,
    /// `None`: the handle's configured [`CommitMode`].
    pub commit_mode: Option<CommitMode>,
}

impl TxnOptions {
    /// Optimistic, 8 attempts, 10 s, no commit token, the handle's commit mode.
    pub fn new(op: &'static str) -> Self {
        TxnOptions {
            mode: Mode::Optimistic,
            max_attempts: 8,
            deadline: Duration::from_secs(10),
            commit_token: false,
            op,
            commit_mode: None,
        }
    }

    /// Like [`new`](Self::new), pessimistic.
    pub fn pessimistic(op: &'static str) -> Self {
        TxnOptions {
            mode: Mode::Pessimistic,
            ..TxnOptions::new(op)
        }
    }

    /// With a commit token.
    pub fn with_token(mut self) -> Self {
        self.commit_token = true;
        self
    }
}

/// Why a [`Tikv::run`] (or a read) failed. Messages never carry a key with its
/// keyspace prefix or root, and show at most 64 bytes of one.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TxnError {
    /// A conflict the runner's attempts or deadline did not outlast. Returned
    /// by a body, it makes the runner restart the transaction.
    #[error("transaction conflict")]
    Conflict,
    /// The transaction certainly did not commit. The runner retries it like
    /// a conflict.
    #[error("not applied: {0}")]
    NotApplied(String),
    /// The commit may or may not have applied. `token` is the commit token
    /// whose presence would tell, when there is one and resolving it failed.
    #[error("commit outcome undetermined")]
    Undetermined { token: Option<[u8; 16]> },
    /// An insert found its key (`KeyError.already_exist`).
    #[error("key already exists: {0}")]
    AlreadyExists(String),
    /// Invalid arguments, misuse, an unknown error kind, or a refusal of this
    /// layer (a value over 2 MiB, a read below the GC safe point).
    #[error("{0}")]
    Fatal(String),
    /// The deadline passed before the transaction could commit.
    #[error("transaction deadline passed")]
    Deadline,
}

impl TxnError {
    /// Whether the runner reruns the transaction after this error.
    fn retryable(&self) -> bool {
        matches!(self, TxnError::Conflict | TxnError::NotApplied(_))
    }
}

/// A committed run.
#[derive(Debug, Clone, PartialEq)]
pub struct Committed<T> {
    /// What the body returned on the attempt that committed.
    pub value: T,
    /// The commit timestamp. For a read-only transaction, its start
    /// timestamp; for a commit resolved through its token, the timestamp of
    /// the resolving read (an upper bound of the real one).
    pub commit_ts: Timestamp,
    /// The attempts it took, from 1.
    pub attempts: u32,
    /// The committing attempt's outcome was unknown and was resolved through
    /// the commit token.
    pub earlier_unknown: bool,
}

/// Counters of a handle (and its clones).
#[derive(Debug, Default)]
pub(crate) struct Counters {
    page_halvings: AtomicU64,
    restarts: AtomicU64,
    unknown_outcomes: AtomicU64,
}

impl Counters {
    pub(crate) fn page_halved(&self) {
        self.page_halvings.fetch_add(1, Ordering::Relaxed);
    }
}

/// A snapshot of a handle's counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TikvStats {
    /// Clients rebuilt by the TSO supervisor.
    pub client_rebuilds: u64,
    /// Scan and batch-get pages halved on gRPC `OutOfRange`.
    pub page_halvings: u64,
    /// Transactions restarted after a conflict or a not-applied error.
    pub restarts: u64,
    /// Commits whose outcome was unknown.
    pub unknown_outcomes: u64,
}

/// How one attempt ended.
enum Attempt<T> {
    Committed {
        value: T,
        commit_ts: Timestamp,
        resolved: bool,
    },
    Retry(TxnError),
    Fail(TxnError),
}

/// The first backoff pause; doubles per attempt up to [`BACKOFF_MAX`], and
/// each pause is drawn from its upper half.
const BACKOFF_BASE: Duration = Duration::from_millis(10);
const BACKOFF_MAX: Duration = Duration::from_secs(1);

fn backoff(attempt: u32) -> Duration {
    let exp = attempt.saturating_sub(2).min(10);
    let ceiling = BACKOFF_BASE.saturating_mul(1 << exp).min(BACKOFF_MAX);
    let ms = u64::try_from(ceiling.as_millis()).unwrap_or(1000).max(2);
    Duration::from_millis(rand::random_range(ms / 2..=ms))
}

impl Tikv {
    /// Runs `body` in a new transaction and commits it; reruns `body` at a new
    /// start timestamp on a conflict or a not-applied error. With
    /// `commit_token`, writes `t/<token>` in the transaction and resolves an
    /// undetermined commit by reading it at a fresh timestamp.
    ///
    /// A body is rerun, so it must be free of side effects outside `txn`. It
    /// returns `Err(TxnError::Conflict)` to ask for a restart; to reject
    /// without a retry, return `Ok` with the caller's own error inside `T`
    /// (the transaction then commits whatever it wrote), or `Fatal`.
    pub async fn run<T, F>(&self, opts: TxnOptions, mut body: F) -> Result<Committed<T>, TxnError>
    where
        T: Send,
        F: for<'t> FnMut(&'t mut Txn) -> BoxFuture<'t, Result<T, TxnError>>,
    {
        let deadline = Instant::now() + opts.deadline;
        let commit_mode = opts.commit_mode.unwrap_or(self.commit_mode);
        let mut last = TxnError::Deadline;
        for attempt in 1..=opts.max_attempts.max(1) {
            if attempt > 1 {
                self.counters.restarts.fetch_add(1, Ordering::Relaxed);
                let pause = backoff(attempt);
                if Instant::now() + pause >= deadline {
                    return Err(TxnError::Deadline);
                }
                tokio::time::sleep(pause).await;
            }
            if Instant::now() >= deadline {
                return Err(TxnError::Deadline);
            }
            match self
                .attempt(&opts, commit_mode, attempt, deadline, &mut body)
                .await
            {
                Attempt::Committed {
                    value,
                    commit_ts,
                    resolved,
                } => {
                    return Ok(Committed {
                        value,
                        commit_ts,
                        attempts: attempt,
                        earlier_unknown: resolved,
                    });
                }
                Attempt::Retry(e) => {
                    tracing::debug!(op = opts.op, attempt, error = %e, "restarting a transaction");
                    last = e;
                }
                Attempt::Fail(e) => return Err(e),
            }
        }
        Err(last)
    }

    /// A read-only view at `at`. Refused with [`TikvError::GcSafePoint`] when
    /// `at` is older than `now − (gc_life_time − 1 min)` (row R7): TiKV would
    /// answer such a read without an error, from versions GC may have dropped.
    /// A [`GcBarrier`](crate::GcBarrier) set through this handle (or a clone)
    /// at or below `at` lets it through while the barrier lives, provided the
    /// cluster GC safe point has not passed `at` (Task 3, row T2-2).
    pub async fn snapshot(&self, at: Timestamp) -> Result<crate::Snap, TikvError> {
        let now_ms = self.now_ms_estimate().await?;
        let window_ms = u64::try_from(self.safe_window().as_millis()).unwrap_or(u64::MAX);
        let floor_ms = now_ms.saturating_sub(window_ms);
        let at_ms = Tikv::physical_ms(&at);
        let version = tikv_client::TimestampExt::version(&at);
        if at_ms < floor_ms {
            let refused = |safe_point: u64| TikvError::GcSafePoint {
                at: version,
                safe_point,
            };
            let floor = tikv_client::TimestampExt::version(&Timestamp {
                physical: i64::try_from(floor_ms).unwrap_or(i64::MAX),
                logical: 0,
                suffix_bits: 0,
            });
            if !self.barriers.covers(version) {
                return Err(refused(floor));
            }
            let cluster = tikv_client::TimestampExt::version(&self.gc_safe_point().await?);
            if cluster > version {
                return Err(refused(cluster));
            }
        }
        let (client, _) = self.clients.client();
        let inner = client.snapshot(
            at.clone(),
            TransactionOptions::new_optimistic()
                .read_only()
                .drop_check(CheckLevel::None),
        );
        let left = Duration::from_millis(at_ms.saturating_sub(floor_ms));
        Ok(crate::Snap::new(inner, self.clone(), at, left))
    }

    /// The handle's counters.
    pub fn stats(&self) -> TikvStats {
        TikvStats {
            client_rebuilds: self.clients.rebuilds(),
            page_halvings: self.counters.page_halvings.load(Ordering::Relaxed),
            restarts: self.counters.restarts.load(Ordering::Relaxed),
            unknown_outcomes: self.counters.unknown_outcomes.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn counters(&self) -> &Counters {
        &self.counters
    }

    /// Maps a `tikv-client` error of a read or write inside a transaction.
    pub(crate) fn txn_error(&self, e: &tikv_client::Error) -> TxnError {
        let text = describe(e, self.root());
        match classify(e) {
            Class::Conflict => TxnError::Conflict,
            Class::Undetermined => TxnError::Undetermined { token: None },
            Class::AlreadyExists => TxnError::AlreadyExists(text),
            Class::NotApplied | Class::TsoClosed => TxnError::NotApplied(text),
            Class::TooLarge | Class::Fatal => TxnError::Fatal(text),
        }
    }

    fn fault(&self, op: &str, point: FaultPoint, attempt: u32) -> Option<Fault> {
        self.faults.as_ref().and_then(|f| f.at(op, point, attempt))
    }

    async fn attempt<T, F>(
        &self,
        opts: &TxnOptions,
        commit_mode: CommitMode,
        attempt: u32,
        deadline: Instant,
        body: &mut F,
    ) -> Attempt<T>
    where
        T: Send,
        F: for<'t> FnMut(&'t mut Txn) -> BoxFuture<'t, Result<T, TxnError>>,
    {
        match self.fault(opts.op, FaultPoint::BeforeBegin, attempt) {
            Some(Fault::Refuse | Fault::LoseAck) => {
                return Attempt::Retry(TxnError::NotApplied("fault: refused before begin".into()));
            }
            Some(Fault::Conflict) => return Attempt::Retry(TxnError::Conflict),
            Some(Fault::Delay(d)) => tokio::time::sleep(d).await,
            None => {}
        }

        // Begin: one TSO request inside the client.
        let (client, generation) = self.clients.client();
        let mut options = match opts.mode {
            Mode::Optimistic => TransactionOptions::new_optimistic(),
            Mode::Pessimistic => TransactionOptions::new_pessimistic(),
        }
        .drop_check(CheckLevel::None);
        if commit_mode == CommitMode::Async1pc {
            options = options.use_async_commit().try_one_pc();
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        let begun = match self.clients.take_injected_tso_loss() {
            Some(e) => Ok(Err(e)),
            None => tokio::time::timeout(remaining, client.begin_with_options(options)).await,
        };
        let inner = match begun {
            Ok(Ok(inner)) => inner,
            Ok(Err(e)) => {
                let class = classify(&e);
                self.clients.tso_failed(Some(class), generation).await;
                return match self.txn_error(&e) {
                    e if e.retryable() => Attempt::Retry(e),
                    e => Attempt::Fail(e),
                };
            }
            Err(_) => {
                self.clients.tso_failed(None, generation).await;
                return Attempt::Fail(TxnError::Deadline);
            }
        };
        self.clients.tso_ok();
        self.tso.observe(&inner.start_timestamp());
        let mut txn = Txn::new(inner, self.clone(), attempt);

        // The body.
        let remaining = deadline.saturating_duration_since(Instant::now());
        let value = match tokio::time::timeout(remaining, body(&mut txn)).await {
            Ok(Ok(value)) => value,
            Ok(Err(e)) => {
                self.rollback(&mut txn).await;
                if txn.tso_closed {
                    self.clients.rebuild(generation).await;
                }
                return if e.retryable() {
                    Attempt::Retry(e)
                } else {
                    Attempt::Fail(e)
                };
            }
            Err(_) => {
                self.rollback(&mut txn).await;
                return Attempt::Fail(TxnError::Deadline);
            }
        };

        let mut lost = false;
        if let Some(f) = self.fault(opts.op, FaultPoint::BeforePrewrite, attempt) {
            match self.pre_commit_fault(f, &mut txn, "prewrite").await {
                Some(outcome) => return outcome,
                None => lost = f == Fault::LoseAck,
            }
        }

        let token = opts.commit_token.then(new_token);
        if !lost && let Some(token) = &token {
            let start_ms = Tikv::physical_ms(&txn.start_ts());
            if let Err(e) = txn.put(&token_key(token), token_value(start_ms)).await {
                self.rollback(&mut txn).await;
                return Attempt::Fail(e);
            }
        }

        if !lost && let Some(f) = self.fault(opts.op, FaultPoint::BeforeCommit, attempt) {
            match self.pre_commit_fault(f, &mut txn, "commit").await {
                Some(outcome) => return outcome,
                None => lost = f == Fault::LoseAck,
            }
        }
        if lost {
            return self.unknown_outcome(token, value).await;
        }

        // The commit.
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            self.rollback(&mut txn).await;
            return Attempt::Fail(TxnError::Deadline);
        }
        let committed = tokio::time::timeout(remaining, txn.inner.commit()).await;
        let commit_ts = match committed {
            Err(_) => {
                // The commit future is dropped: its requests may still land.
                drop(txn);
                return self.unknown_outcome(token, value).await;
            }
            Ok(Ok(ts)) => ts.unwrap_or_else(|| txn.start_ts()),
            Ok(Err(e)) => {
                let class = classify(&e);
                if class == Class::Undetermined {
                    drop(txn);
                    return self.unknown_outcome(token, value).await;
                }
                // Row R9: any other commit error means it did not commit.
                self.rollback(&mut txn).await;
                if class == Class::TsoClosed {
                    self.clients.rebuild(generation).await;
                }
                return match self.txn_error(&e) {
                    e if e.retryable() => Attempt::Retry(e),
                    e => Attempt::Fail(e),
                };
            }
        };

        match self.fault(opts.op, FaultPoint::AfterCommit, attempt) {
            Some(Fault::Delay(d)) => tokio::time::sleep(d).await,
            Some(_) => return self.unknown_outcome(token, value).await,
            None => {}
        }
        Attempt::Committed {
            value,
            commit_ts,
            resolved: false,
        }
    }

    /// Applies a fault before the commit. `None`: carry on (after a delay, or
    /// towards an unknown outcome for `LoseAck`, whose transaction is rolled
    /// back here).
    async fn pre_commit_fault<T>(
        &self,
        fault: Fault,
        txn: &mut Txn,
        stage: &str,
    ) -> Option<Attempt<T>> {
        match fault {
            Fault::Delay(d) => {
                tokio::time::sleep(d).await;
                None
            }
            Fault::Refuse => {
                self.rollback(txn).await;
                Some(Attempt::Retry(TxnError::NotApplied(format!(
                    "fault: refused before {stage}"
                ))))
            }
            Fault::Conflict => {
                self.rollback(txn).await;
                Some(Attempt::Retry(TxnError::Conflict))
            }
            Fault::LoseAck => {
                self.rollback(txn).await;
                None
            }
        }
    }

    /// Best-effort rollback, bounded by the request timeout.
    async fn rollback(&self, txn: &mut Txn) {
        let _ = tokio::time::timeout(self.request_timeout, txn.inner.rollback()).await;
    }

    /// Resolves an unknown commit outcome through the token (semantics 2).
    ///
    /// A resolving transaction reads the token at a fresh start timestamp
    /// (TiKV first resolves a lock the commit left). Present: the commit
    /// applied. Absent: the resolver writes a fence value at the token and
    /// commits it, so a request of the lost commit still in flight can no
    /// longer commit (its prewrite meets a newer write and conflicts); then
    /// the commit certainly did not apply and the runner retries. The resolver
    /// keeps trying for two request timeouts; after that the outcome stays
    /// `Undetermined { token }`.
    async fn unknown_outcome<T>(&self, token: Option<Token>, value: T) -> Attempt<T> {
        self.counters
            .unknown_outcomes
            .fetch_add(1, Ordering::Relaxed);
        let Some(token) = token else {
            return Attempt::Fail(TxnError::Undetermined { token: None });
        };
        let give_up = Instant::now() + self.request_timeout * 2;
        while Instant::now() < give_up {
            match tokio::time::timeout(self.request_timeout, self.resolve_token(&token)).await {
                Ok(Some(Resolved::Committed(ts))) => {
                    return Attempt::Committed {
                        value,
                        commit_ts: ts,
                        resolved: true,
                    };
                }
                Ok(Some(Resolved::NotApplied)) => {
                    return Attempt::Retry(TxnError::NotApplied(
                        "the commit's outcome was unknown and its token is absent".into(),
                    ));
                }
                Ok(None) | Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
        Attempt::Fail(TxnError::Undetermined { token: Some(token) })
    }

    /// One resolving transaction; `None` when it could not decide.
    async fn resolve_token(&self, token: &Token) -> Option<Resolved> {
        let key = self.key(&token_key(token));
        let (client, _) = self.clients.client();
        let options = TransactionOptions::new_optimistic().drop_check(CheckLevel::None);
        let mut txn = client.begin_with_options(options).await.ok()?;
        let start = txn.start_timestamp();
        let found = match txn.get(key.clone()).await {
            Ok(found) => found,
            Err(_) => {
                let _ = txn.rollback().await;
                return None;
            }
        };
        match found {
            Some(v) => {
                let _ = txn.rollback().await;
                Some(if is_fence(&v) {
                    Resolved::NotApplied
                } else {
                    Resolved::Committed(start)
                })
            }
            None => {
                let fence = fence_value(Tikv::physical_ms(&start));
                if txn.put(key, fence).await.is_err() {
                    let _ = txn.rollback().await;
                    return None;
                }
                match txn.commit().await {
                    Ok(_) => Some(Resolved::NotApplied),
                    Err(e) => {
                        if classify(&e) != Class::Undetermined {
                            let _ = txn.rollback().await;
                        }
                        None
                    }
                }
            }
        }
    }
}

/// What a resolving transaction found.
enum Resolved {
    /// The token is there: the commit applied, and is visible at this
    /// timestamp.
    Committed(Timestamp),
    /// The token is fenced: the commit did not apply and never will.
    NotApplied,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_stays_bounded() {
        for attempt in 2..40 {
            let pause = backoff(attempt);
            assert!(pause >= Duration::from_millis(1), "{pause:?}");
            assert!(pause <= BACKOFF_MAX, "{pause:?}");
        }
        assert!(backoff(2) <= Duration::from_millis(10));
    }

    #[test]
    fn only_conflicts_and_not_applied_are_retried() {
        assert!(TxnError::Conflict.retryable());
        assert!(TxnError::NotApplied("x".into()).retryable());
        assert!(!TxnError::Undetermined { token: None }.retryable());
        assert!(!TxnError::Fatal("x".into()).retryable());
        assert!(!TxnError::Deadline.retryable());
        assert!(!TxnError::AlreadyExists("k".into()).retryable());
    }

    #[test]
    fn defaults_follow_the_plan() {
        let o = TxnOptions::new("op");
        assert_eq!(o.max_attempts, 8);
        assert_eq!(o.deadline, Duration::from_secs(10));
        assert!(!o.commit_token);
        assert_eq!(o.mode, Mode::Optimistic);
        assert_eq!(CommitMode::default(), CommitMode::Async1pc);
        assert_eq!(
            TxnOptions::pessimistic("p").with_token().mode,
            Mode::Pessimistic
        );
    }
}
