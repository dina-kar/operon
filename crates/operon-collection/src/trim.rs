//! Implicit-stream trimming (plan M1.1 Task 12, Ruling 12; overview A6):
//! each partition of a collection's implicit stream is trimmed below the
//! `applied` offset of the **oldest retained** manifest, so every manifest a
//! pinned read can still open keeps the tail after its `applied` offsets.
//! Link apply itself never trims.
//!
//! The retained set is [`retained_chain`] at the metastore clock, the same
//! rule and clock source GC and `open_version` use. It only shrinks as the
//! clock advances and commits arrive, so a trim computed now never cuts a
//! tail that a later read or GC run still retains.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use async_trait::async_trait;
use operon_common::{CollectionId, NamespaceId, StreamId};
use operon_meta::{ApplyError, Consistency, Fence, MetaClient, MetaError, collection_pointer_key};
use operon_worker::{
    Candidate, Priority, Task, TaskContext, TaskError, TaskKey, TaskOutcome, TaskSource,
};

use crate::chain::{load_pointed, retained_chain};
use crate::error::CollectionError;
use crate::snapshot::CollectionContext;

/// The key prefix of trim tasks: `collection-trim/<cid>` (lease
/// `task/collection-trim/<cid>`).
pub const TRIM_TASK_PREFIX: &str = "collection-trim/";

/// Proposes one trim task per collection (keyed `collection-trim/<cid>`, at
/// [`Priority::Maintenance`]) at most once per `trim_interval`. With
/// `CollectionConfig.trim` off, every run ends `Idle` without trimming.
/// Registered at worker start; it discovers collections from the metastore.
#[derive(Clone)]
pub struct CollectionTrimSource {
    ctx: CollectionContext,
    /// When each collection's last run started.
    last_run: Arc<Mutex<BTreeMap<CollectionId, Instant>>>,
}

impl fmt::Debug for CollectionTrimSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let runs = self
            .last_run
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len();
        f.debug_struct("CollectionTrimSource")
            .field("ctx", &self.ctx)
            .field("runs", &runs)
            .finish_non_exhaustive()
    }
}

impl CollectionTrimSource {
    pub fn new(ctx: CollectionContext) -> Self {
        Self {
            ctx,
            last_run: Arc::default(),
        }
    }
}

#[async_trait]
impl TaskSource for CollectionTrimSource {
    fn priority(&self) -> Priority {
        Priority::Maintenance
    }

    async fn candidates(&self, meta: &MetaClient) -> Result<Vec<Candidate>, TaskError> {
        let collections: Vec<(NamespaceId, CollectionId)> = meta
            .read(Consistency::Local, |s| {
                s.all_collections().map(|c| (c.namespace, c.id)).collect()
            })
            .await?;
        let interval = self.ctx.config.trim_interval;
        let mut last_run = self.last_run.lock().unwrap_or_else(PoisonError::into_inner);
        let live: BTreeSet<CollectionId> = collections.iter().map(|(_, cid)| *cid).collect();
        last_run.retain(|cid, _| live.contains(cid));
        Ok(collections
            .into_iter()
            .filter(|(_, cid)| last_run.get(cid).is_none_or(|at| at.elapsed() >= interval))
            .map(|(ns, cid)| {
                let task: Arc<dyn Task> = Arc::new(TrimTask {
                    source: self.clone(),
                    ns,
                    cid,
                });
                (TaskKey::new(ns, format!("{TRIM_TASK_PREFIX}{cid}")), task)
            })
            .collect())
    }
}

/// Trims the implicit stream of one collection.
struct TrimTask {
    source: CollectionTrimSource,
    ns: NamespaceId,
    cid: CollectionId,
}

/// What one metastore read tells a trim run.
struct Seen {
    stream: StreamId,
    /// The log start of each partition.
    log_starts: Vec<u64>,
    pointer: Option<operon_meta::Pointer>,
    clock_ms: u64,
}

fn fenced(err: MetaError) -> TaskError {
    match err {
        MetaError::Rejected(ApplyError::Fenced { .. }) => TaskError::Fenced,
        other => TaskError::Meta(other),
    }
}

fn failed(err: CollectionError) -> TaskError {
    match err {
        CollectionError::Meta(err) => fenced(err),
        other => TaskError::failed(other),
    }
}

#[async_trait]
impl Task for TrimTask {
    async fn run(&self, ctx: TaskContext) -> Result<TaskOutcome, TaskError> {
        self.source
            .last_run
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(self.cid, Instant::now());
        if !self.source.ctx.config.trim {
            return Ok(TaskOutcome::Idle);
        }
        tokio::select! {
            biased;
            () = ctx.cancel.cancelled() => Ok(TaskOutcome::Done),
            result = self.trim(&ctx.fence) => result,
        }
    }
}

impl TrimTask {
    async fn trim(&self, fence: &Fence) -> Result<TaskOutcome, TaskError> {
        let ctx = &self.source.ctx;
        let (ns, cid) = (self.ns, self.cid);
        let key = collection_pointer_key(cid);
        // The pointer and the clock in one `Linearizable` read.
        let seen = ctx
            .meta
            .read(Consistency::Linearizable, |s| {
                let collection = s.collection(cid).filter(|c| c.namespace == ns)?;
                let log_starts = (0..collection.partitions)
                    .map(|p| {
                        s.partition(collection.stream, p)
                            .map_or(0, |state| state.log_start_offset())
                    })
                    .collect();
                Some(Seen {
                    stream: collection.stream,
                    log_starts,
                    pointer: s.pointer(ns, &key).cloned(),
                    clock_ms: s.clock_ms(),
                })
            })
            .await?;
        // A dropped collection, or one without a commit yet.
        let Some(Seen {
            stream,
            log_starts,
            pointer: Some(pointer),
            clock_ms,
        }) = seen
        else {
            return Ok(TaskOutcome::Idle);
        };
        let live = load_pointed(
            &ctx.store,
            &ctx.manifests,
            cid,
            pointer.version,
            &pointer.value,
        )
        .await
        .map_err(failed)?;
        let chain = retained_chain(
            &ctx.store,
            &ctx.manifests,
            (pointer.value, live),
            ctx.config.keep_manifests,
            ctx.config.time_travel_retention,
            clock_ms,
        )
        .await
        .map_err(failed)?;
        let Some((_, oldest)) = chain.last() else {
            return Ok(TaskOutcome::Idle);
        };
        let mut trimmed = false;
        for (partition, log_start) in (0u32..).zip(log_starts) {
            let bound = oldest.applied.get(&partition).copied().unwrap_or(0);
            if bound <= log_start {
                continue;
            }
            ctx.meta
                .trim_partition(stream, partition, bound, Some(fence.clone()))
                .await
                .map_err(fenced)?;
            trimmed = true;
        }
        Ok(if trimmed {
            TaskOutcome::Done
        } else {
            TaskOutcome::Idle
        })
    }
}
