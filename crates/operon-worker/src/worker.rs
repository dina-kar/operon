//! The polling scheduler.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use operon_common::NamespaceId;
use operon_common::meta::{ApplyError, MetaError, MetaStore};
use tokio::sync::Notify;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::runner::Leased;
use crate::{Candidate, Priority, TaskKey, TaskOutcome, TaskSource};

/// How long [`WorkerHandle::stop`] waits for cancelled tasks before it
/// aborts them.
const STOP_GRACE: Duration = Duration::from_secs(30);

/// How a [`Worker`] schedules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerConfig {
    /// Names this worker in task leases. Must be unique per process
    /// incarnation (see [`MetaStore::acquire_lease`]).
    pub owner: String,
    /// Time between polls of the task sources. Default 1 s.
    pub poll_interval: Duration,
    /// TTL of task leases; they are renewed every third of it. Default 30 s.
    pub lease_ttl: Duration,
    /// Most tasks running at once. Default 16.
    pub max_concurrent: usize,
    /// Most tasks of one namespace running at once. Cluster-wide tasks (no
    /// namespace) are bounded by `max_concurrent` only. Default 4.
    pub max_per_namespace: usize,
}

impl WorkerConfig {
    /// The defaults, for worker `owner`.
    pub fn new(owner: impl Into<String>) -> Self {
        Self {
            owner: owner.into(),
            poll_interval: Duration::from_secs(1),
            lease_ttl: Duration::from_secs(30),
            max_concurrent: 16,
            max_per_namespace: 4,
        }
    }
}

/// Polls task sources and runs their tasks under leases. Build it with
/// [`Worker::new`] and [`Worker::add_source`], then [`Worker::start`] it.
pub struct Worker {
    meta: Arc<dyn MetaStore>,
    config: WorkerConfig,
    sources: Vec<Arc<dyn TaskSource>>,
}

impl fmt::Debug for Worker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Worker")
            .field("config", &self.config)
            .field("sources", &self.sources.len())
            .finish_non_exhaustive()
    }
}

/// Shared between the scheduler, its runners and the handle.
struct Shared {
    meta: Arc<dyn MetaStore>,
    config: WorkerConfig,
    sources: Vec<Arc<dyn TaskSource>>,
    /// Keys running here, with their namespaces.
    running: Mutex<BTreeMap<TaskKey, CancellationToken>>,
    /// Wakes the scheduler before its next poll (a task wants more work).
    wake: Notify,
    renewals_paused: Arc<AtomicBool>,
}

impl Shared {
    fn running(&self) -> MutexGuard<'_, BTreeMap<TaskKey, CancellationToken>> {
        self.running.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A running [`Worker`].
///
/// [`WorkerHandle::stop`] cancels the tasks, waits for them and releases
/// their leases. Dropping the handle without stopping aborts everything at
/// once and leaves the leases to expire, as a crashed process would.
pub struct WorkerHandle {
    shared: Arc<Shared>,
    shutdown: CancellationToken,
    scheduler: Option<JoinHandle<()>>,
}

impl fmt::Debug for WorkerHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerHandle")
            .field("owner", &self.shared.config.owner)
            .finish_non_exhaustive()
    }
}

impl Worker {
    pub fn new(meta: impl Into<Arc<dyn MetaStore>>, config: WorkerConfig) -> Self {
        Self {
            meta: meta.into(),
            config,
            sources: Vec::new(),
        }
    }

    /// Adds a task source. Sources are polled in the order they were added.
    pub fn add_source(&mut self, source: Arc<dyn TaskSource>) {
        self.sources.push(source);
    }

    /// Starts polling. Must be called inside a Tokio runtime.
    pub fn start(self) -> WorkerHandle {
        let shared = Arc::new(Shared {
            meta: self.meta,
            config: self.config,
            sources: self.sources,
            running: Mutex::default(),
            wake: Notify::new(),
            renewals_paused: Arc::default(),
        });
        let shutdown = CancellationToken::new();
        let scheduler = tokio::spawn(schedule(shared.clone(), shutdown.clone()));
        WorkerHandle {
            shared,
            shutdown,
            scheduler: Some(scheduler),
        }
    }
}

impl WorkerHandle {
    /// The keys of the tasks running on this worker now.
    pub fn running(&self) -> Vec<TaskKey> {
        self.shared.running().keys().cloned().collect()
    }

    /// Stops polling, cancels the running tasks, waits for them (aborting
    /// any still running after 30 s) and releases their leases.
    pub async fn stop(mut self) {
        self.shutdown.cancel();
        if let Some(scheduler) = self.scheduler.take()
            && let Err(err) = scheduler.await
        {
            tracing::error!(%err, "worker scheduler failed");
        }
    }

    /// Test hook: while `paused`, lease renewals are skipped, so leases run
    /// out as if this worker had stalled.
    #[cfg(feature = "test-util")]
    pub fn pause_renewals(&self, paused: bool) {
        self.shared
            .renewals_paused
            .store(paused, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        if let Some(scheduler) = self.scheduler.take() {
            // Dropping the scheduler's future drops its task set, which aborts
            // every runner without releasing leases: a crash, not a stop.
            scheduler.abort();
        }
    }
}

/// The scheduler loop: poll, start what fits, wait.
async fn schedule(shared: Arc<Shared>, shutdown: CancellationToken) {
    let mut runners: JoinSet<()> = JoinSet::new();
    let mut round: usize = 0;
    loop {
        // Reap finished runners so the set does not grow.
        while runners.try_join_next().is_some() {}
        poll(&shared, &mut runners, round).await;
        round = round.wrapping_add(1);
        tokio::select! {
            () = shutdown.cancelled() => break,
            () = tokio::time::sleep(shared.config.poll_interval) => {}
            () = shared.wake.notified() => {}
        }
    }
    for token in shared.running().values() {
        token.cancel();
    }
    let drain = async { while runners.join_next().await.is_some() {} };
    if tokio::time::timeout(STOP_GRACE, drain).await.is_err() {
        tracing::warn!("worker tasks did not stop in time; aborting them");
        runners.abort_all();
        while runners.join_next().await.is_some() {}
    }
}

/// Orders candidates: by priority, then round-robin across namespaces within
/// a priority (namespaces in order of first appearance, rotated by `round`
/// so no namespace always goes first).
fn order(mut by_priority: BTreeMap<Priority, Vec<Candidate>>, round: usize) -> Vec<Candidate> {
    let mut ordered = Vec::new();
    for candidates in by_priority.values_mut() {
        let mut lanes: Vec<(Option<NamespaceId>, Vec<Candidate>)> = Vec::new();
        for candidate in candidates.drain(..) {
            let namespace = candidate.0.namespace;
            match lanes.iter_mut().find(|(ns, _)| *ns == namespace) {
                Some((_, lane)) => lane.push(candidate),
                None => lanes.push((namespace, vec![candidate])),
            }
        }
        if !lanes.is_empty() {
            let shift = round % lanes.len();
            lanes.rotate_left(shift);
        }
        let mut lanes: Vec<std::vec::IntoIter<_>> = lanes
            .into_iter()
            .map(|(_, lane)| lane.into_iter())
            .collect();
        loop {
            let mut any = false;
            for lane in &mut lanes {
                if let Some(candidate) = lane.next() {
                    ordered.push(candidate);
                    any = true;
                }
            }
            if !any {
                break;
            }
        }
    }
    ordered
}

/// One poll: collect candidates from every source and start what fits.
async fn poll(shared: &Arc<Shared>, runners: &mut JoinSet<()>, round: usize) {
    let mut by_priority: BTreeMap<Priority, Vec<Candidate>> = BTreeMap::new();
    let mut seen = BTreeSet::new();
    for source in &shared.sources {
        match source.candidates(&*shared.meta).await {
            Ok(candidates) => {
                for (key, task) in candidates {
                    if seen.insert(key.clone()) {
                        by_priority
                            .entry(source.priority())
                            .or_default()
                            .push((key, task));
                    }
                }
            }
            Err(err) => tracing::warn!(%err, "a task source failed"),
        }
    }
    let config = &shared.config;
    for (key, task) in order(by_priority, round) {
        {
            let running = shared.running();
            if running.len() >= config.max_concurrent {
                break;
            }
            if running.contains_key(&key) {
                continue;
            }
            if key.namespace.is_some()
                && running
                    .keys()
                    .filter(|k| k.namespace == key.namespace)
                    .count()
                    >= config.max_per_namespace
            {
                continue;
            }
        }
        let grant = match shared
            .meta
            .acquire_lease(&key.lease(), &config.owner, config.lease_ttl)
            .await
        {
            Ok(grant) => grant,
            Err(MetaError::Rejected(ApplyError::LeaseHeld { .. })) => continue,
            Err(err) => {
                tracing::warn!(%key, %err, "could not take a task lease");
                continue;
            }
        };
        let cancel = CancellationToken::new();
        shared.running().insert(key.clone(), cancel.clone());
        let shared = shared.clone();
        runners.spawn(async move {
            let leased = Leased {
                meta: &shared.meta,
                owner: &shared.config.owner,
                ttl: shared.config.lease_ttl,
                key: key.clone(),
                epoch: grant.epoch,
                renewals_paused: shared.renewals_paused.clone(),
            };
            let result = leased.run(task, cancel).await;
            shared.running().remove(&key);
            match result {
                Ok(TaskOutcome::MoreWork) => shared.wake.notify_one(),
                Ok(TaskOutcome::Done | TaskOutcome::Idle) => {}
                Err(err) => tracing::warn!(%key, %err, "task failed"),
            }
        });
    }
}
