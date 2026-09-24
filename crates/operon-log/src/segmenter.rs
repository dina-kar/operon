//! The segmenter (design §02 §5): rewrites runs of WAL chunks into
//! per-partition segments and swaps them into the offset index. It runs as
//! worker tasks (M0.4): [`SegmenterSource`] proposes one task per partition
//! with a due run, keyed `segmenter/<stream>/<partition>`.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use operon_cache::RangeCache;
use operon_common::{NamespaceId, StreamId};
use operon_meta::{
    ApplyError, Command, Consistency, EntryKind, Freshness, IndexEntry, MetaClient, MetaError,
    PartitionState, Reply, WalClass,
};
use operon_store::Store;
use operon_worker::{
    Candidate, Priority, RunResult, Task, TaskContext, TaskError, TaskKey, TaskOutcome, TaskSource,
};
use ulid::Ulid;

use crate::batch;
use crate::error::{LogError, corrupt};
use crate::paths;
use crate::record::Encoding;
use crate::segment::SegmentBuilder;

/// When the segmenter segments.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmenterConfig {
    /// A run of WAL entries is segmented once it holds this many bytes.
    /// Default 64 MiB.
    pub min_bytes: u64,
    /// A segment holds at most this many bytes of WAL entries (at least one
    /// entry). Default 256 MiB.
    pub target_bytes: u64,
    /// A run is segmented regardless of size once its oldest entry's newest
    /// record timestamp is this old. Default 10 min.
    pub max_wal_age: Duration,
    /// The lease TTL [`Segmenter::run_once`] uses. A [`Worker`] uses its own
    /// (`WorkerConfig::lease_ttl`). Default 30 s.
    ///
    /// [`Worker`]: operon_worker::Worker
    pub lease_ttl: Duration,
    /// A segment not swapped in within this long of its PUT is abandoned
    /// (left for garbage collection), so it can never be swapped in after
    /// garbage collection could have deleted it: GC's grace must be longer.
    /// Default 10 min.
    pub swap_deadline: Duration,
}

impl Default for SegmenterConfig {
    fn default() -> Self {
        Self {
            min_bytes: 64 * 1024 * 1024,
            target_bytes: 256 * 1024 * 1024,
            max_wal_age: Duration::from_secs(600),
            lease_ttl: Duration::from_secs(30),
            swap_deadline: Duration::from_secs(600),
        }
    }
}

/// What segmenter runs did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SegmenterReport {
    /// Segments written and swapped in.
    pub segments: u32,
    /// Partitions skipped because another holder had the lease, or because
    /// the index changed under the run, or the run's lease was taken over.
    pub skipped: u32,
    /// Partitions whose attempt failed (logged; retried later).
    pub failed: u32,
}

impl SegmenterReport {
    fn minus(self, earlier: SegmenterReport) -> SegmenterReport {
        SegmenterReport {
            segments: self.segments - earlier.segments,
            skipped: self.skipped - earlier.skipped,
            failed: self.failed - earlier.failed,
        }
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// The leading run of WAL entries of a partition (after its segments), up to
/// `target_bytes`, if it is due: it holds `min_bytes`, or its oldest entry's
/// newest timestamp is older than `cutoff`. Empty if nothing is due.
fn due_run(state: &PartitionState, config: &SegmenterConfig, cutoff: i64) -> Vec<IndexEntry> {
    let mut entries = Vec::new();
    let mut bytes = 0;
    for entry in state
        .entries()
        .skip_while(|e| e.kind == EntryKind::Segment)
        .take_while(|e| e.kind == EntryKind::Wal)
    {
        let len = entry.byte_range.end - entry.byte_range.start;
        if !entries.is_empty() && bytes + len > config.target_bytes {
            break;
        }
        bytes += len;
        entries.push(entry.clone());
    }
    match entries.first() {
        Some(first) if bytes >= config.min_bytes || first.max_timestamp_ms < cutoff => entries,
        _ => Vec::new(),
    }
}

struct Shared {
    store: Store,
    cache: RangeCache,
    config: SegmenterConfig,
    report: Mutex<SegmenterReport>,
}

impl Shared {
    fn record(&self, f: impl FnOnce(&mut SegmenterReport)) {
        f(&mut self.report.lock().unwrap_or_else(PoisonError::into_inner));
    }
}

/// Proposes a segmenting task for every `standard` partition with a due run
/// of WAL entries, at [`Priority::Segmenting`]. Each task's swap is fenced
/// by its task lease, so a run whose lease was taken over cannot change the
/// index. Replaced WAL objects are not deleted: the metastore retires them,
/// and garbage collection deletes them later.
#[derive(Clone)]
pub struct SegmenterSource {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for SegmenterSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmenterSource")
            .field("config", &self.shared.config)
            .finish_non_exhaustive()
    }
}

impl SegmenterSource {
    pub fn new(store: Store, cache: RangeCache, config: SegmenterConfig) -> Self {
        Self {
            shared: Arc::new(Shared {
                store,
                cache,
                config,
                report: Mutex::default(),
            }),
        }
    }

    /// Totals over every task this source's tasks ran.
    pub fn report(&self) -> SegmenterReport {
        *self
            .shared
            .report
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// The due run of one partition, as of a local read.
async fn due(
    meta: &MetaClient,
    config: &SegmenterConfig,
    stream: StreamId,
    partition: u32,
) -> Result<Vec<IndexEntry>, MetaError> {
    let cutoff =
        i64::try_from(meta.now_ms().saturating_sub(millis(config.max_wal_age))).unwrap_or(i64::MAX);
    meta.read(Consistency::Local, |s| {
        s.partition(stream, partition)
            .map(|state| due_run(state, config, cutoff))
            .unwrap_or_default()
    })
    .await
}

#[async_trait]
impl TaskSource for SegmenterSource {
    fn priority(&self) -> Priority {
        Priority::Segmenting
    }

    async fn candidates(&self, meta: &MetaClient) -> Result<Vec<Candidate>, TaskError> {
        // One short read for the streams, then one per partition, so the scan
        // never holds the state lock for long.
        let streams: Vec<(NamespaceId, StreamId, u32)> = meta
            .read(Consistency::Local, |s| {
                s.all_streams()
                    .filter(|st| st.class == WalClass::Standard)
                    .map(|st| (st.namespace, st.id, st.partitions))
                    .collect()
            })
            .await?;
        let mut candidates = Vec::new();
        for (namespace, stream, partitions) in streams {
            for partition in 0..partitions {
                if due(meta, &self.shared.config, stream, partition)
                    .await?
                    .is_empty()
                {
                    continue;
                }
                let task: Arc<dyn Task> = Arc::new(SegmentTask {
                    shared: self.shared.clone(),
                    namespace,
                    stream,
                    partition,
                });
                candidates.push((
                    TaskKey::new(namespace, format!("segmenter/{stream}/{partition}")),
                    task,
                ));
            }
        }
        Ok(candidates)
    }
}

/// Segments one partition's due run.
struct SegmentTask {
    shared: Arc<Shared>,
    namespace: NamespaceId,
    stream: StreamId,
    partition: u32,
}

/// How one segmenting attempt ended.
enum Attempt {
    Swapped,
    /// The index changed under the run, or the run was cancelled.
    Skipped,
}

#[async_trait]
impl Task for SegmentTask {
    async fn run(&self, ctx: TaskContext) -> Result<TaskOutcome, TaskError> {
        let config = &self.shared.config;
        // Plan again: the candidate was computed at poll time.
        let entries = due(&ctx.meta, config, self.stream, self.partition).await?;
        if entries.is_empty() {
            return Ok(TaskOutcome::Idle);
        }
        match self.segment(&ctx, &entries).await {
            Ok(Attempt::Swapped) => {
                self.shared.record(|r| r.segments += 1);
                // Another run may be due already (a backlog longer than
                // `target_bytes`).
                let more = !due(&ctx.meta, config, self.stream, self.partition)
                    .await?
                    .is_empty();
                Ok(if more {
                    TaskOutcome::MoreWork
                } else {
                    TaskOutcome::Done
                })
            }
            Ok(Attempt::Skipped) => {
                self.shared.record(|r| r.skipped += 1);
                Ok(TaskOutcome::Done)
            }
            Err(TaskError::Fenced) => {
                self.shared.record(|r| r.skipped += 1);
                Err(TaskError::Fenced)
            }
            Err(err) => {
                tracing::warn!(
                    stream = %self.stream,
                    partition = self.partition,
                    %err,
                    "segmenting failed"
                );
                self.shared.record(|r| r.failed += 1);
                Err(err)
            }
        }
    }
}

impl SegmentTask {
    /// Writes and swaps in one segment under the task's fence.
    async fn segment(
        &self,
        ctx: &TaskContext,
        entries: &[IndexEntry],
    ) -> Result<Attempt, TaskError> {
        let (stream, partition) = (self.stream, self.partition);
        let store = &self.shared.store;
        let base_offset = entries[0].base_offset;
        let mut builder = SegmentBuilder::new(stream, partition, base_offset, Encoding::Kafka);
        let mut max_timestamp_ms = i64::MIN;
        for entry in entries {
            let bytes = self
                .shared
                .cache
                .read(&entry.object, entry.byte_range.clone())
                .await
                .map_err(|e| TaskError::failed(LogError::from(e)))?;
            for batch in batch::batches(&bytes) {
                let batch = batch.map_err(TaskError::failed)?;
                builder
                    .push_batch(
                        Bytes::copy_from_slice(batch.bytes),
                        batch.record_count,
                        batch.max_timestamp_ms,
                    )
                    .map_err(|e| {
                        TaskError::failed(corrupt(format!("WAL chunk in {}: {e}", entry.object)))
                    })?;
            }
            if builder.next_offset() != entry.end_offset() {
                return Err(TaskError::failed(corrupt(format!(
                    "WAL chunk in {} holds records up to {}, its index entry up to {}",
                    entry.object,
                    builder.next_offset(),
                    entry.end_offset()
                ))));
            }
            max_timestamp_ms = max_timestamp_ms.max(entry.max_timestamp_ms);
        }
        let (bytes, footer) = builder.finish();
        let written_at = ctx.meta.now_ms();
        let ulid = Ulid::from_parts(written_at, Ulid::generate().random());
        let path = paths::segment(self.namespace, stream, partition, base_offset, ulid);
        store
            .put_if_absent(&path, bytes)
            .await
            .map_err(|e| TaskError::failed(LogError::from(e)))?;
        crate::failpoint!("seg.after_put");

        // Never swapped in, so safe to delete: the run was cancelled (its
        // lease is being lost) or took so long that GC could delete it.
        let late =
            ctx.meta.now_ms().saturating_sub(written_at) > millis(self.shared.config.swap_deadline);
        if ctx.cancel.is_cancelled() || late {
            self.delete_unused(ctx, &path).await;
            return Ok(Attempt::Skipped);
        }
        let command = Command::SwapSegment {
            stream,
            partition,
            replaces: entries
                .iter()
                .map(|e| (e.base_offset, e.object.clone()))
                .collect(),
            segment: path.clone(),
            byte_range: footer.data,
            max_timestamp_ms,
            fence: Some(ctx.fence.clone()),
            now_ms: ctx.meta.now_ms(),
            // Enforced when the swap is applied, so a swap delayed past the
            // deadline (retries, a frozen process) can never reference a
            // segment GC may have deleted (M0.4 review I1).
            fresh: Freshness {
                created_at_ms: written_at,
                max_age_ms: millis(self.shared.config.swap_deadline),
            },
        };
        let (result, earlier_unknown) = ctx.meta.write_tracked(command).await;
        match result {
            Ok(Reply::SegmentSwapped) => {
                crate::failpoint!("seg.after_swap");
                Ok(Attempt::Swapped)
            }
            Ok(other) => Err(MetaError::UnexpectedReply(other).into()),
            // A rejection of the first attempt means the swap was never
            // applied (a retry of an applied swap succeeds), so the segment
            // is unreferenced. After an attempt with an unknown outcome the
            // swap may have been applied and then trimmed: leave the segment
            // to garbage collection (M0.3 re-review M7).
            Err(MetaError::Rejected(
                rejection @ (ApplyError::IndexMismatch { .. }
                | ApplyError::Fenced { .. }
                | ApplyError::StaleObject { .. }),
            )) => {
                if !earlier_unknown {
                    self.delete_unused(ctx, &path).await;
                }
                if matches!(rejection, ApplyError::Fenced { .. }) {
                    Err(TaskError::Fenced)
                } else {
                    Ok(Attempt::Skipped)
                }
            }
            // The outcome may be unknown: leave the object for garbage
            // collection if the swap did not land.
            Err(err) => Err(err.into()),
        }
    }

    /// Deletes a segment this run wrote and never swapped in. As a second
    /// guard, it checks with a linearizable read that the metastore neither
    /// references nor retired the path.
    async fn delete_unused(&self, ctx: &TaskContext, path: &str) {
        let (stream, partition) = (self.stream, self.partition);
        let known = ctx
            .meta
            .read(Consistency::Linearizable, |s| {
                s.retired().any(|(p, _)| p == path)
                    || s.partition(stream, partition)
                        .is_some_and(|state| state.entries().any(|e| e.object == path))
            })
            .await;
        match known {
            Ok(false) => {
                if let Err(err) = self.shared.store.delete(path).await {
                    tracing::warn!(%path, %err, "could not delete an unused segment");
                }
            }
            Ok(true) => tracing::debug!(%path, "segment is known to the metastore; not deleting"),
            Err(err) => tracing::warn!(%path, %err, "could not check a segment; not deleting"),
        }
    }
}

/// Runs the segmenter's tasks once, through the worker framework
/// ([`operon_worker::run_once`]), for tests and tools.
#[derive(Clone, Debug)]
pub struct Segmenter {
    meta: MetaClient,
    owner: String,
    source: SegmenterSource,
}

impl Segmenter {
    /// `owner` names this process incarnation in leases; it must be unique
    /// per process (see `Command::AcquireLease`).
    pub fn new(
        meta: MetaClient,
        store: Store,
        cache: RangeCache,
        owner: impl Into<String>,
        config: SegmenterConfig,
    ) -> Self {
        Self {
            meta,
            owner: owner.into(),
            source: SegmenterSource::new(store, cache, config),
        }
    }

    /// The task source, to add to a [`Worker`](operon_worker::Worker).
    pub fn source(&self) -> &SegmenterSource {
        &self.source
    }

    /// Segments one run of WAL entries in every partition that is due, each
    /// under its task lease.
    pub async fn run_once(&self) -> Result<SegmenterReport, LogError> {
        let before = self.source.report();
        let results = operon_worker::run_once(
            &self.meta,
            &self.owner,
            self.source.shared.config.lease_ttl,
            &self.source,
        )
        .await
        .map_err(|e| match e {
            TaskError::Meta(meta) => LogError::Meta(meta),
            other => LogError::Task(other),
        })?;
        let mut report = self.source.report().minus(before);
        report.skipped += u32::try_from(
            results
                .iter()
                .filter(|(_, r)| matches!(r, RunResult::LeaseHeld))
                .count(),
        )
        .unwrap_or(u32::MAX);
        Ok(report)
    }
}
