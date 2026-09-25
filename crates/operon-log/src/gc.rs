//! Garbage collection (design §03 §7): deletes objects nothing references
//! any more, once they are older than a grace period. It runs as the
//! singleton worker task `gc` at [`Priority::Gc`].
//!
//! Each run makes three passes:
//! 1. **Retired objects.** Objects the metastore retired (WAL objects whose
//!    chunks were all segmented or trimmed, trimmed segments) at least
//!    `grace` ago are deleted (a missing object counts as deleted), then
//!    forgotten with `ForgetObjects`, fenced by the task lease. A retired
//!    path ending in `/` is a prefix (a dropped collection's): everything
//!    under it is deleted, and the prefix is forgotten once a later listing
//!    finds it empty.
//! 2. **Orphan WAL objects.** Objects under `wal/` whose ULID time is at
//!    least `2 * WAL_COMMIT_WINDOW_MS + grace` old, that have no live chunk
//!    and are not retired: a WAL PUT whose commit never landed. They can
//!    never be committed any more (`StaleCommit`).
//! 3. **Orphan segments and link objects.** Segments under
//!    `ns/<ns>/streams/` that no index entry references and that are not
//!    retired, and objects under the prefixes of registered [`GcRoots`]
//!    (such as `ns/<ns>/links/`) that the roots do not report reachable and
//!    that are not under a prefix the roots keep, once older than `grace`
//!    (by the root's [`GcRoots::object_time_ms`]: by default the ULID in
//!    their name, else their modification time).
//!
//! Every decision uses a linearizable metastore read, and ages are measured
//! against the **metastore clock** (`MetaState::clock_ms`, read in the same
//! or an earlier linearizable read), not this node's wall clock. Commands
//! that make the metastore reference a new object (`SwapSegment`, a link's
//! fenced `CasPointer`) carry the object's [`Freshness`] and are refused once
//! the metastore clock is past it; with their deadlines below `grace`, any
//! such command applied after GC's read is refused, so an object GC decided
//! to delete can never become referenced (M0.4 review I1). Object deletes
//! cannot be fenced by the metastore, so before each batch of deletes the
//! task confirms it still holds its lease.
//!
//! [`Freshness`]: operon_meta::Freshness

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use operon_common::NamespaceId;
use operon_meta::{ApplyError, Consistency, Fence, MetaClient, MetaError, WAL_COMMIT_WINDOW_MS};
use operon_store::{ObjectInfo, Store};
use operon_worker::{
    Candidate, Priority, RunResult, Task, TaskContext, TaskError, TaskKey, TaskOutcome, TaskSource,
};
use ulid::Ulid;

use crate::error::LogError;

/// The key of the garbage collection task; its lease is `task/gc`.
pub const GC_TASK: &str = "gc";

/// How garbage collection runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcConfig {
    /// How old an unreferenced object must be before it is deleted. Must be
    /// longer than the longest read, the segmenter's `swap_deadline` and the
    /// link commit delay. Default 1 h.
    pub grace: Duration,
    /// The shortest time between two runs started by one source. Default 60 s.
    pub interval: Duration,
    /// Most objects one pass deletes in one run (the rest wait for the next
    /// run), so a run's work is bounded. Default 1 000.
    pub list_page: usize,
    /// How many ancestors of a link's live manifest are kept, besides the
    /// live one. Default 10.
    pub keep_manifests: usize,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            grace: Duration::from_secs(3600),
            interval: Duration::from_secs(60),
            list_page: 1_000,
            keep_manifests: 10,
        }
    }
}

/// A freshness deadline that is not strictly below GC's grace period, so an
/// object could be deleted before the command that references it is refused.
#[derive(Debug, thiserror::Error)]
#[error("{name} ({deadline:?}) must be strictly below gc.grace ({grace:?})")]
pub struct DeadlineError {
    /// The configuration key of the deadline, such as `link.max_commit_delay`.
    pub name: &'static str,
    pub deadline: Duration,
    pub grace: Duration,
}

impl GcConfig {
    /// Every freshness deadline must be strictly below `grace` (M0.4 re-review m1).
    ///
    /// `deadlines` are `(name, deadline)` pairs; the first one at or above
    /// `grace` is reported.
    pub fn check_deadlines(
        &self,
        deadlines: &[(&'static str, Duration)],
    ) -> Result<(), DeadlineError> {
        match deadlines
            .iter()
            .find(|(_, deadline)| *deadline >= self.grace)
        {
            Some(&(name, deadline)) => Err(DeadlineError {
                name,
                deadline,
                grace: self.grace,
            }),
            None => Ok(()),
        }
    }
}

/// What garbage collection runs deleted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GcReport {
    /// Retired objects deleted and forgotten, retired prefixes forgotten,
    /// and objects deleted under retired prefixes.
    pub retired: u32,
    /// Orphan WAL objects deleted.
    pub orphan_wal: u32,
    /// Orphan segments deleted.
    pub orphan_segments: u32,
    /// Unreachable objects under [`GcRoots`] prefixes deleted.
    pub orphan_other: u32,
}

impl GcReport {
    fn add(&mut self, other: GcReport) {
        self.retired += other.retired;
        self.orphan_wal += other.orphan_wal;
        self.orphan_segments += other.orphan_segments;
        self.orphan_other += other.orphan_other;
    }
}

/// What a [`GcRoots`] keeps under its prefix in one namespace, as of one
/// [`reachable`](GcRoots::reachable) call.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GcKeep {
    /// Objects (full paths) that must be kept.
    pub objects: BTreeSet<String>,
    /// Prefixes (full paths) under which nothing is deleted this run.
    pub prefixes: Vec<String>,
}

impl From<BTreeSet<String>> for GcKeep {
    fn from(objects: BTreeSet<String>) -> Self {
        Self {
            objects,
            prefixes: Vec::new(),
        }
    }
}

/// Reports which objects under a per-namespace prefix are still reachable,
/// for objects the log does not track itself (link targets, for example).
#[async_trait]
pub trait GcRoots: Send + Sync {
    /// The prefix under `ns/<ns>/` this source owns, such as `links/`.
    fn prefix(&self) -> &str;

    /// Every object under `ns/<namespace>/<prefix>` that must be kept, and
    /// every prefix under which nothing may be deleted, as of now, in one
    /// call: a run filters its listing with exactly what its own call
    /// returned, so overlapping runs cannot mix up each other's answers.
    /// Objects written after this call are young, so they are safe either
    /// way. An error makes the run skip this prefix.
    async fn reachable(
        &self,
        meta: &MetaClient,
        store: &Store,
        namespace: NamespaceId,
        keep_manifests: usize,
    ) -> Result<GcKeep, LogError>;

    /// This root's notion of an object's creation time. Default:
    /// [`object_time_ms`].
    fn object_time_ms(&self, info: &ObjectInfo) -> u64 {
        object_time_ms(info)
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// The time an object was created: the ULID in its file name if it has one
/// (`<ulid>.wal`, `<base>-<ulid>.seg`, `<ulid>.cnt`), else its modification
/// time.
pub fn object_time_ms(info: &ObjectInfo) -> u64 {
    let name = info.path.rsplit('/').next().unwrap_or_default();
    let stem = name.split('.').next().unwrap_or_default();
    let candidate = stem.rsplit('-').next().unwrap_or_default();
    Ulid::from_string(candidate)
        .map(|u| u.timestamp_ms())
        .unwrap_or(info.last_modified_ms)
}

fn fenced(err: MetaError) -> TaskError {
    match err {
        MetaError::Rejected(ApplyError::Fenced { .. }) => TaskError::Fenced,
        other => TaskError::Meta(other),
    }
}

fn store_failed(err: operon_store::StoreError) -> TaskError {
    TaskError::failed(LogError::from(err))
}

struct Shared {
    store: Store,
    config: GcConfig,
    roots: Vec<Arc<dyn GcRoots>>,
    last_run: Mutex<Option<Instant>>,
    report: Mutex<GcReport>,
}

/// Proposes the singleton GC task ([`GC_TASK`]) at most once per `interval`.
#[derive(Clone)]
pub struct GcSource {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for GcSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcSource")
            .field("config", &self.shared.config)
            .field("roots", &self.shared.roots.len())
            .finish_non_exhaustive()
    }
}

impl GcSource {
    /// GC of the log's own objects only.
    pub fn new(store: Store, config: GcConfig) -> Self {
        Self::with_roots(store, config, Vec::new())
    }

    /// GC of the log's objects and of the objects under `roots`' prefixes.
    /// Without a root for a prefix, nothing under it is ever deleted.
    pub fn with_roots(store: Store, config: GcConfig, roots: Vec<Arc<dyn GcRoots>>) -> Self {
        Self {
            shared: Arc::new(Shared {
                store,
                config,
                roots,
                last_run: Mutex::default(),
                report: Mutex::default(),
            }),
        }
    }

    /// Totals over every run of this source's task.
    pub fn report(&self) -> GcReport {
        *self
            .shared
            .report
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn task(&self) -> Candidate {
        let task: Arc<dyn Task> = Arc::new(GcTask {
            shared: self.shared.clone(),
        });
        (TaskKey::cluster(GC_TASK), task)
    }

    /// Runs one GC pass now (ignoring `interval`) under the task lease, as
    /// `owner`. `Ok(None)` if another owner holds the lease.
    pub async fn run_once(
        &self,
        meta: &MetaClient,
        owner: &str,
    ) -> Result<Option<GcReport>, LogError> {
        struct Once(Candidate);
        #[async_trait]
        impl TaskSource for Once {
            fn priority(&self) -> Priority {
                Priority::Gc
            }
            async fn candidates(&self, _meta: &MetaClient) -> Result<Vec<Candidate>, TaskError> {
                Ok(vec![self.0.clone()])
            }
        }
        let before = self.report();
        let ttl = (self.shared.config.interval * 3)
            .clamp(Duration::from_secs(5), Duration::from_secs(3600));
        let results = operon_worker::run_once(meta, owner, ttl, &Once(self.task()))
            .await
            .map_err(LogError::Task)?;
        for (_, result) in results {
            match result {
                RunResult::LeaseHeld => return Ok(None),
                RunResult::Ran(Ok(_)) => {}
                RunResult::Ran(Err(TaskError::Meta(err))) => return Err(LogError::Meta(err)),
                RunResult::Ran(Err(err)) => return Err(LogError::Task(err)),
            }
        }
        let after = self.report();
        Ok(Some(GcReport {
            retired: after.retired - before.retired,
            orphan_wal: after.orphan_wal - before.orphan_wal,
            orphan_segments: after.orphan_segments - before.orphan_segments,
            orphan_other: after.orphan_other - before.orphan_other,
        }))
    }
}

#[async_trait]
impl TaskSource for GcSource {
    fn priority(&self) -> Priority {
        Priority::Gc
    }

    async fn candidates(&self, _meta: &MetaClient) -> Result<Vec<Candidate>, TaskError> {
        let due = self
            .shared
            .last_run
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_none_or(|last| last.elapsed() >= self.shared.config.interval);
        Ok(if due { vec![self.task()] } else { Vec::new() })
    }
}

struct GcTask {
    shared: Arc<Shared>,
}

#[async_trait]
impl Task for GcTask {
    async fn run(&self, ctx: TaskContext) -> Result<TaskOutcome, TaskError> {
        *self
            .shared
            .last_run
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(Instant::now());
        let mut report = GcReport::default();
        let result = self.passes(&ctx, &mut report).await;
        self.shared
            .report
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .add(report);
        if report != GcReport::default() {
            tracing::debug!(?report, "gc run");
        }
        result.map(|()| TaskOutcome::Done)
    }
}

impl GcTask {
    async fn passes(&self, ctx: &TaskContext, report: &mut GcReport) -> Result<(), TaskError> {
        self.retired(ctx, report).await?;
        if ctx.cancel.is_cancelled() {
            return Ok(());
        }
        self.orphan_wal(ctx, report).await?;
        if ctx.cancel.is_cancelled() {
            return Ok(());
        }
        self.orphan_namespaced(ctx, report).await
    }

    /// Confirms, with a linearizable read, that this run still holds its
    /// lease at its epoch, before it deletes anything.
    async fn confirm_lease(&self, ctx: &TaskContext) -> Result<(), TaskError> {
        let Fence { lease, epoch } = &ctx.fence;
        let held = ctx
            .meta
            .read(Consistency::Linearizable, |s| {
                s.lease(lease)
                    .is_some_and(|l| l.epoch == *epoch && l.owner.is_some())
            })
            .await?;
        if held && !ctx.cancel.is_cancelled() {
            Ok(())
        } else {
            Err(TaskError::Fenced)
        }
    }

    /// Deletes `paths` (missing counts as deleted); returns those deleted.
    async fn delete(
        &self,
        ctx: &TaskContext,
        paths: Vec<String>,
    ) -> Result<Vec<String>, TaskError> {
        if paths.is_empty() {
            return Ok(paths);
        }
        self.confirm_lease(ctx).await?;
        let mut deleted = Vec::with_capacity(paths.len());
        for path in paths {
            // Stop promptly once cancelled (review M8); what was deleted so
            // far is reported, the rest waits for the next run.
            if ctx.cancel.is_cancelled() {
                break;
            }
            match self.shared.store.delete(&path).await {
                Ok(()) => deleted.push(path),
                Err(err) => tracing::warn!(%path, %err, "gc could not delete an object"),
            }
        }
        Ok(deleted)
    }

    /// Pass 1: retired objects past the grace period.
    ///
    /// A retired path ending in `/` is a prefix (a dropped collection's,
    /// plan M1.1 Ruling 13): everything under it is deleted, up to the
    /// run's remaining `list_page` budget, and the prefix is forgotten only
    /// once a later listing finds it empty.
    async fn retired(&self, ctx: &TaskContext, report: &mut GcReport) -> Result<(), TaskError> {
        let config = &self.shared.config;
        let grace = millis(config.grace);
        let due: Vec<String> = ctx
            .meta
            .read(Consistency::Linearizable, |s| {
                let now = s.clock_ms();
                s.retired()
                    .filter(|(_, at)| at.saturating_add(grace) <= now)
                    .map(|(path, _)| path.to_string())
                    .collect()
            })
            .await?;
        let mut budget = config.list_page;
        let mut objects = Vec::new();
        let mut empty_prefixes = Vec::new();
        let mut under_prefixes = Vec::new();
        for path in due {
            if budget == 0 {
                break;
            }
            if !path.ends_with('/') {
                objects.push(path);
                budget -= 1;
                continue;
            }
            let listed = self.shared.store.list(&path).await.map_err(store_failed)?;
            if listed.is_empty() {
                empty_prefixes.push(path);
                continue;
            }
            let take = listed.len().min(budget);
            budget -= take;
            under_prefixes.extend(listed.into_iter().take(take).map(|info| info.path));
        }
        let deleted = self.delete(ctx, under_prefixes).await?;
        report.retired += u32::try_from(deleted.len()).unwrap_or(u32::MAX);
        let mut forget = self.delete(ctx, objects).await?;
        if forget.is_empty() && empty_prefixes.is_empty() {
            return Ok(());
        }
        crate::failpoint!("gc.after_delete");
        forget.extend(empty_prefixes);
        let forgotten = ctx
            .meta
            .forget_objects(forget, Some(ctx.fence.clone()))
            .await
            .map_err(fenced)?;
        report.retired += forgotten;
        Ok(())
    }

    /// Pass 2: WAL objects that were never committed and never can be.
    async fn orphan_wal(&self, ctx: &TaskContext, report: &mut GcReport) -> Result<(), TaskError> {
        let config = &self.shared.config;
        let min_age = 2 * WAL_COMMIT_WINDOW_MS + millis(config.grace);
        let listed = self.shared.store.list("wal/").await.map_err(store_failed)?;
        let candidates: Vec<(String, u64)> = listed
            .iter()
            .filter(|info| info.path.ends_with(".wal"))
            .map(|info| (info.path.clone(), object_time_ms(info)))
            .collect();
        if candidates.is_empty() {
            return Ok(());
        }
        let limit = config.list_page;
        let orphans: Vec<String> = ctx
            .meta
            .read(Consistency::Linearizable, |s| {
                let now = s.clock_ms();
                let retired: BTreeSet<&str> = s.retired().map(|(p, _)| p).collect();
                candidates
                    .iter()
                    .filter(|(_, created)| created.saturating_add(min_age) <= now)
                    .map(|(p, _)| p)
                    .filter(|p| s.wal_live_chunks(p).is_none() && !retired.contains(p.as_str()))
                    .take(limit)
                    .cloned()
                    .collect()
            })
            .await?;
        let deleted = self.delete(ctx, orphans).await?;
        report.orphan_wal += u32::try_from(deleted.len()).unwrap_or(u32::MAX);
        Ok(())
    }

    /// Pass 3: per namespace, unreferenced segments and objects under the
    /// registered roots' prefixes.
    async fn orphan_namespaced(
        &self,
        ctx: &TaskContext,
        report: &mut GcReport,
    ) -> Result<(), TaskError> {
        let config = &self.shared.config;
        let namespaces: Vec<NamespaceId> = ctx
            .meta
            .read(Consistency::Linearizable, |s| {
                s.namespaces().map(|n| n.id).collect()
            })
            .await?;
        let grace = millis(config.grace);
        for namespace in namespaces {
            if ctx.cancel.is_cancelled() {
                return Ok(());
            }
            // Segments.
            let listed = self
                .shared
                .store
                .list(&format!("ns/{namespace}/streams/"))
                .await
                .map_err(store_failed)?;
            let candidates: Vec<(String, u64)> = listed
                .iter()
                .filter(|info| info.path.ends_with(".seg"))
                .map(|info| (info.path.clone(), object_time_ms(info)))
                .collect();
            if !candidates.is_empty() {
                let limit = config.list_page;
                let orphans: Vec<String> = ctx
                    .meta
                    .read(Consistency::Linearizable, |s| {
                        let now = s.clock_ms();
                        let mut kept: BTreeSet<&str> = s.retired().map(|(p, _)| p).collect();
                        for stream in s.streams(namespace) {
                            for partition in 0..stream.partitions {
                                if let Some(state) = s.partition(stream.id, partition) {
                                    kept.extend(state.entries().map(|e| e.object.as_str()));
                                }
                            }
                        }
                        candidates
                            .iter()
                            .filter(|(_, created)| created.saturating_add(grace) <= now)
                            .map(|(p, _)| p)
                            .filter(|p| !kept.contains(p.as_str()))
                            .take(limit)
                            .cloned()
                            .collect()
                    })
                    .await?;
                let deleted = self.delete(ctx, orphans).await?;
                report.orphan_segments += u32::try_from(deleted.len()).unwrap_or(u32::MAX);
            }
            // Objects of the registered roots.
            for root in &self.shared.roots {
                let prefix = format!("ns/{namespace}/{}", root.prefix());
                // The clock is read before the roots: a commit applied after
                // this read that references an object at least `grace` old
                // by this clock is refused as stale.
                let now = ctx
                    .meta
                    .read(Consistency::Linearizable, |s| s.clock_ms())
                    .await?;
                let store = &self.shared.store;
                let keep = match root
                    .reachable(&ctx.meta, store, namespace, config.keep_manifests)
                    .await
                {
                    Ok(keep) => keep,
                    Err(err) => {
                        tracing::warn!(%prefix, %err, "gc skips a prefix whose roots it cannot read");
                        continue;
                    }
                };
                let listed = store.list(&prefix).await.map_err(store_failed)?;
                let orphans: Vec<String> = listed
                    .iter()
                    .filter(|info| !keep.objects.contains(&info.path))
                    .filter(|info| {
                        !keep
                            .prefixes
                            .iter()
                            .any(|p| info.path.starts_with(p.as_str()))
                    })
                    .filter(|info| root.object_time_ms(info).saturating_add(grace) <= now)
                    .map(|info| info.path.clone())
                    .take(config.list_page)
                    .collect();
                let deleted = self.delete(ctx, orphans).await?;
                report.orphan_other += u32::try_from(deleted.len()).unwrap_or(u32::MAX);
            }
        }
        Ok(())
    }
}
