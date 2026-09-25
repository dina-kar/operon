//! Running one task under its lease: renewals, loss detection, release.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use operon_common::meta::{ApplyError, Fence, MetaError, MetaStore};
use tokio_util::sync::CancellationToken;

use crate::{Task, TaskContext, TaskError, TaskKey, TaskOutcome, TaskSource};

/// The shortest renewal period, so a tiny TTL cannot make renewals spin.
const MIN_RENEW_PERIOD: Duration = Duration::from_millis(5);

/// What happened to one candidate in [`run_once`].
#[derive(Debug)]
pub enum RunResult {
    /// The task ran (under its lease) and ended like this.
    Ran(Result<TaskOutcome, TaskError>),
    /// Another owner holds the task's lease; the task did not run.
    LeaseHeld,
}

/// A task run's lease, as the runner keeps it.
pub(crate) struct Leased<'a> {
    pub meta: &'a Arc<dyn MetaStore>,
    pub owner: &'a str,
    pub ttl: Duration,
    pub key: TaskKey,
    pub epoch: u64,
    /// Test hook: while set, renewals are skipped.
    pub renewals_paused: Arc<AtomicBool>,
}

impl Leased<'_> {
    /// Runs `task` with a fence at the lease epoch, renewing the lease every
    /// `ttl / 3` until the task ends, and releases the lease afterwards
    /// unless it was lost. `cancel` is cancelled when the lease is lost.
    pub async fn run(
        self,
        task: Arc<dyn Task>,
        cancel: CancellationToken,
    ) -> Result<TaskOutcome, TaskError> {
        let lease = self.key.lease();
        let ctx = TaskContext {
            key: self.key.clone(),
            fence: Fence {
                lease: lease.clone(),
                epoch: self.epoch,
            },
            cancel: cancel.clone(),
            meta: self.meta.clone(),
        };
        let lost = AtomicBool::new(false);
        let result = {
            let run = task.run(ctx);
            tokio::pin!(run);
            tokio::select! {
                biased;
                result = &mut run => result,
                () = self.keep_alive(&lease, &cancel, &lost) => run.await,
            }
        };
        if !lost.load(Ordering::SeqCst) {
            match self
                .meta
                .release_lease(&lease, self.owner, self.epoch)
                .await
            {
                Ok(()) | Err(MetaError::Rejected(ApplyError::LeaseLost { .. })) => {}
                Err(err) => tracing::debug!(%lease, %err, "could not release a task lease"),
            }
        }
        result
    }

    /// Renews the lease every `ttl / 3`. A renewal that finds the lease
    /// expired re-takes it at the same epoch if nobody else took it (M0.3
    /// re-review N3); if someone did, cancels the task and returns.
    /// Transient metastore errors are retried at the next period: fencing,
    /// not the deadline, is what keeps a late task from changing anything.
    async fn keep_alive(&self, lease: &str, cancel: &CancellationToken, lost: &AtomicBool) {
        let period = (self.ttl / 3).max(MIN_RENEW_PERIOD);
        loop {
            tokio::time::sleep(period).await;
            if self.renewals_paused.load(Ordering::SeqCst) {
                continue;
            }
            let renewed = self
                .meta
                .renew_lease(lease, self.owner, self.epoch, self.ttl)
                .await;
            let err = match renewed {
                Ok(_) => continue,
                Err(MetaError::Rejected(ApplyError::LeaseLost { .. })) => {
                    match self
                        .meta
                        .reacquire_lease(lease, self.owner, self.epoch, self.ttl)
                        .await
                    {
                        Ok(_) => {
                            tracing::info!(%lease, "re-took an expired task lease nobody else took");
                            continue;
                        }
                        Err(MetaError::Rejected(ApplyError::LeaseLost { .. })) => {
                            tracing::info!(%lease, "task lease lost; cancelling the task");
                            lost.store(true, Ordering::SeqCst);
                            cancel.cancel();
                            return;
                        }
                        Err(err) => err,
                    }
                }
                Err(err) => err,
            };
            tracing::warn!(%lease, %err, "renewing a task lease failed; retrying");
        }
    }
}

/// Runs every candidate `source` proposes once, one after another, each
/// under its task lease exactly as a [`Worker`](crate::Worker) would (with
/// renewals and release), and reports what happened. For tools and tests
/// that want one deterministic pass instead of a polling worker.
pub async fn run_once(
    meta: impl Into<Arc<dyn MetaStore>>,
    owner: &str,
    lease_ttl: Duration,
    source: &dyn TaskSource,
) -> Result<Vec<(TaskKey, RunResult)>, TaskError> {
    let meta = meta.into();
    let mut seen = BTreeSet::new();
    let mut results = Vec::new();
    for (key, task) in source.candidates(&*meta).await? {
        if !seen.insert(key.clone()) {
            continue;
        }
        let grant = match meta.acquire_lease(&key.lease(), owner, lease_ttl).await {
            Ok(grant) => grant,
            Err(MetaError::Rejected(ApplyError::LeaseHeld { .. })) => {
                results.push((key, RunResult::LeaseHeld));
                continue;
            }
            Err(err) => {
                results.push((key, RunResult::Ran(Err(err.into()))));
                continue;
            }
        };
        let leased = Leased {
            meta: &meta,
            owner,
            ttl: lease_ttl,
            key: key.clone(),
            epoch: grant.epoch,
            renewals_paused: Arc::default(),
        };
        let result = leased.run(task, CancellationToken::new()).await;
        results.push((key, RunResult::Ran(result)));
    }
    Ok(results)
}
