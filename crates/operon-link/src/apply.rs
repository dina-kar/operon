//! The link-apply task (design §09 §3) and its task source.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use operon_log::{FetchRequest, LogError, LogReader};
use operon_meta::{Consistency, Link, LinkId, MetaClient};
use operon_worker::{
    Candidate, Priority, Task, TaskContext, TaskError, TaskKey, TaskOutcome, TaskSource,
};

use crate::counter::MAX_COMMIT_DELAY;
use crate::error::LinkError;
use crate::registry::TargetRegistry;
use crate::target::{ApplyBatch, CommitError, LinkTarget, TargetState};

/// Batch limits of link apply (design §09 §1 `WITH (...)`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkConfig {
    /// Most records one commit applies. Default 10 000.
    pub batch_records: usize,
    /// A batch below both size limits waits up to this long (from the start
    /// of the run) for more records before it is committed. Default 2 s.
    pub batch_interval: Duration,
    /// Most record bytes (keys, values and headers) one commit applies.
    /// Default 64 MiB.
    pub max_batch_bytes: usize,
    /// The longest a commit may take from its data PUT to its CAS, enforced
    /// by the metastore when the CAS is applied; garbage collection's grace
    /// must be longer. Default [`MAX_COMMIT_DELAY`] (10 min). The targets
    /// enforce it: whoever builds the [`TargetRegistry`] hands it to the
    /// factories.
    pub max_commit_delay: Duration,
}

impl Default for LinkConfig {
    fn default() -> Self {
        Self {
            batch_records: 10_000,
            batch_interval: Duration::from_secs(2),
            max_batch_bytes: 64 * 1024 * 1024,
            max_commit_delay: MAX_COMMIT_DELAY,
        }
    }
}

/// The most bytes one fetch asks for while gathering a batch.
const FETCH_BYTES: usize = 1024 * 1024;

struct Shared {
    reader: LogReader,
    registry: TargetRegistry,
    config: LinkConfig,
    /// Per link, the applied offsets last seen, to skip links with no lag.
    applied: Mutex<BTreeMap<LinkId, BTreeMap<u32, u64>>>,
    /// Links whose target kind has no factory, with that kind.
    unregistered: Mutex<BTreeMap<LinkId, String>>,
}

/// Proposes one link-apply task per link whose target kind has a factory in
/// its [`TargetRegistry`] (plan ruling 3: one task per link, covering every
/// source partition), at [`Priority::LinkApply`], keyed `link/<link_id>`. A
/// link whose applied offsets (as last seen by this source) equal its
/// source's high watermarks is not proposed. A link of an unregistered kind
/// is reported ([`LinkApplySource::unregistered`]) and never applied.
#[derive(Clone)]
pub struct LinkApplySource {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for LinkApplySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LinkApplySource")
            .field("registry", &self.shared.registry.kinds())
            .field("config", &self.shared.config)
            .finish_non_exhaustive()
    }
}

impl LinkApplySource {
    pub fn new(reader: LogReader, registry: TargetRegistry, config: LinkConfig) -> Self {
        Self {
            shared: Arc::new(Shared {
                reader,
                registry,
                config,
                applied: Mutex::default(),
                unregistered: Mutex::default(),
            }),
        }
    }

    /// Links whose target kind has no factory, with that kind: reported,
    /// never applied. As of the last [`TaskSource::candidates`] call.
    pub fn unregistered(&self) -> BTreeMap<LinkId, String> {
        self.shared
            .unregistered
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Records the links of unregistered kinds, logging each link once.
    fn report_unregistered(&self, links: BTreeMap<LinkId, String>) {
        let mut reported = self
            .shared
            .unregistered
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        for (link, kind) in &links {
            if !reported.contains_key(link) {
                tracing::warn!(
                    %link,
                    kind = %kind,
                    "no target is registered for this link kind; it is not applied"
                );
            }
        }
        *reported = links;
    }
}

#[async_trait]
impl TaskSource for LinkApplySource {
    fn priority(&self) -> Priority {
        Priority::LinkApply
    }

    async fn candidates(&self, meta: &MetaClient) -> Result<Vec<Candidate>, TaskError> {
        let links: Vec<(Link, Vec<u64>)> = meta
            .read(Consistency::Local, |s| {
                s.all_links()
                    .map(|l| {
                        let hwms = s
                            .stream(l.source)
                            .map(|st| {
                                (0..st.partitions)
                                    .map(|p| {
                                        s.partition(l.source, p).map_or(0, |ps| ps.high_watermark())
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();
                        (l.clone(), hwms)
                    })
                    .collect()
            })
            .await?;
        let seen = self
            .shared
            .applied
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let mut candidates = Vec::new();
        let mut unregistered = BTreeMap::new();
        for (link, hwms) in links {
            let Some(factory) = self.shared.registry.get(&link.target.kind) else {
                unregistered.insert(link.id, link.target.kind.clone());
                continue;
            };
            let caught_up = seen.get(&link.id).is_some_and(|applied| {
                hwms.iter().enumerate().all(|(p, hwm)| {
                    let p = u32::try_from(p).unwrap_or(u32::MAX);
                    applied.get(&p).copied().unwrap_or(0) >= *hwm
                })
            });
            if caught_up {
                continue;
            }
            let target = match factory.open(meta, &link) {
                Ok(target) => target,
                // One link's target failing to open must not stop the others.
                Err(err) => {
                    tracing::warn!(link = %link.id, %err, "opening a link target failed");
                    continue;
                }
            };
            let task: Arc<dyn Task> = Arc::new(ApplyTask {
                shared: self.shared.clone(),
                target,
                link: link.clone(),
            });
            candidates.push((
                TaskKey::new(link.namespace, format!("link/{}", link.id)),
                task,
            ));
        }
        self.report_unregistered(unregistered);
        Ok(candidates)
    }
}

/// Applies one link.
struct ApplyTask {
    shared: Arc<Shared>,
    link: Link,
    target: Arc<dyn LinkTarget>,
}

fn failed(err: impl Into<LinkError>) -> TaskError {
    TaskError::failed(err.into())
}

impl ApplyTask {
    fn seen(&self, state: &TargetState) {
        self.shared
            .applied
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(self.link.id, state.applied.clone());
    }

    /// Fetches records from each partition's applied offset, up to the batch
    /// limits. Returns the batch and whether a limit stopped it.
    async fn gather(
        &self,
        meta: &MetaClient,
        state: &TargetState,
    ) -> Result<(ApplyBatch, bool), LinkError> {
        let config = &self.shared.config;
        let stream = self.link.source;
        let partitions = meta
            .read(Consistency::Local, |s| {
                s.stream(stream).map(|st| st.partitions)
            })
            .await?
            .ok_or_else(|| LinkError::NotFound(format!("stream {stream}")))?;
        let mut batch = ApplyBatch::default();
        let mut bytes = 0usize;
        let mut full = false;
        // Start at a different partition on every version, so one busy
        // partition cannot starve the others of batch space.
        let first = u32::try_from(state.version % u64::from(partitions.max(1))).unwrap_or(0);
        'partitions: for i in 0..partitions {
            let partition = (first + i) % partitions;
            let mut offset = state.applied.get(&partition).copied().unwrap_or(0);
            loop {
                if batch.records.len() >= config.batch_records || bytes >= config.max_batch_bytes {
                    full = true;
                    break 'partitions;
                }
                let request = FetchRequest {
                    stream,
                    partition,
                    offset,
                    max_bytes: FETCH_BYTES.min(config.max_batch_bytes - bytes).max(1),
                    max_wait: Duration::ZERO,
                };
                let response = match self.shared.reader.fetch(request).await {
                    Ok(response) => response,
                    // Trimmed before the link got there: skip to the log start
                    // (the gap is counted as skipped by the target).
                    Err(LogError::OffsetOutOfRange {
                        requested,
                        log_start_offset,
                        ..
                    }) if requested < log_start_offset => {
                        offset = log_start_offset;
                        batch.applied_after.insert(partition, offset);
                        continue;
                    }
                    // Ahead of this node's view of the stream (a lagging
                    // replica): nothing to apply from here yet.
                    Err(LogError::OffsetOutOfRange { .. }) => break,
                    Err(err) => return Err(err.into()),
                };
                if response.records.is_empty() {
                    break;
                }
                for record in response.records {
                    if batch.records.len() >= config.batch_records {
                        full = true;
                        break;
                    }
                    let record_bytes = record.record.key.as_ref().map_or(0, |k| k.len())
                        + record.record.value.as_ref().map_or(0, |v| v.len())
                        + record
                            .record
                            .headers
                            .iter()
                            .map(|(k, v)| k.len() + v.as_ref().map_or(0, |v| v.len()))
                            .sum::<usize>();
                    offset = record.offset + 1;
                    bytes += record_bytes;
                    batch.records.push((partition, record));
                }
                batch.applied_after.insert(partition, offset);
                if full || offset >= response.high_watermark {
                    break;
                }
            }
        }
        Ok((batch, full))
    }
}

#[async_trait]
impl Task for ApplyTask {
    async fn run(&self, ctx: TaskContext) -> Result<TaskOutcome, TaskError> {
        let started = Instant::now();
        loop {
            if ctx.cancel.is_cancelled() {
                return Ok(TaskOutcome::Done);
            }
            // 1. What the target has committed.
            let state = self.target.load().await.map_err(failed)?;
            self.seen(&state);
            // 2. Records from each partition's applied offset.
            let (batch, full) = self.gather(&ctx.meta, &state).await.map_err(failed)?;
            let advances = batch
                .applied_after
                .iter()
                .any(|(p, to)| *to > state.applied.get(p).copied().unwrap_or(0));
            // 3. Nothing to do.
            if !advances {
                return Ok(TaskOutcome::Idle);
            }
            // A small batch waits for more records, up to `batch_interval`.
            let waited = started.elapsed();
            if !full && waited < self.shared.config.batch_interval {
                tokio::select! {
                    () = ctx.cancel.cancelled() => return Ok(TaskOutcome::Done),
                    () = tokio::time::sleep(self.shared.config.batch_interval - waited) => {}
                }
                continue;
            }
            // 4. Commit data and offsets together; 5. on conflict, reload.
            match self.target.commit(state.version, batch, &ctx.fence).await {
                Ok(_) => {
                    return Ok(if full {
                        TaskOutcome::MoreWork
                    } else {
                        TaskOutcome::Idle
                    });
                }
                Err(CommitError::Conflict) => continue,
                Err(CommitError::Fenced) => return Err(TaskError::Fenced),
                Err(CommitError::Other(err)) => return Err(failed(err)),
            }
        }
    }
}
