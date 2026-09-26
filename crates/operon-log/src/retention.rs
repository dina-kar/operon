//! Retention (design §02 §5): trims partitions by age and size,
//! metadata-first. Trimmed objects are retired in the metastore and deleted
//! later by garbage collection. It runs as the singleton worker task
//! `retention` (M0.4), and every trim is fenced by the task lease.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use operon_common::StreamId;
use operon_common::meta::{ApplyError, Consistency, Fence, MetaError, MetaStore, PartitionIndex};
use operon_worker::{
    Candidate, Priority, RunResult, Task, TaskContext, TaskError, TaskKey, TaskOutcome, TaskSource,
};

use crate::error::LogError;

/// The key of the retention task; its lease is `task/retention`.
pub const RETENTION_TASK: &str = "retention";

/// How often retention runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetentionConfig {
    /// The shortest time between two runs started by one source. Default 30 s.
    pub interval: Duration,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
        }
    }
}

/// What retention runs did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RetentionReport {
    /// Partitions whose log start moved.
    pub trimmed: u32,
    /// WAL commit records pruned.
    pub pruned: u32,
    /// Whether another holder had the retention lease, so nothing was done.
    pub skipped: bool,
}

/// Where retention would move one partition's log start, if anywhere.
fn trim_point(
    state: &PartitionIndex,
    max_age_ms: Option<u64>,
    max_bytes: Option<u64>,
    now_ms: u64,
) -> Option<u64> {
    let by_age = max_age_ms.map(|age| {
        let cutoff = i64::try_from(now_ms.saturating_sub(age)).unwrap_or(i64::MAX);
        state
            .entries()
            .find(|e| e.max_timestamp_ms >= cutoff)
            .map_or(state.high_watermark(), |e| e.base_offset)
    });
    // Like Kafka, which never deletes the active segment, the newest entry is
    // always kept, even when it alone exceeds the limit: otherwise a
    // partition could be emptied right after an acknowledged append.
    let by_bytes = max_bytes.map(|limit| {
        let mut bytes = state.bytes();
        let mut before = state.log_start_offset();
        let mut entries = state.entries().peekable();
        while let Some(entry) = entries.next() {
            if bytes <= limit || entries.peek().is_none() {
                break;
            }
            bytes -= entry.byte_range.end - entry.byte_range.start;
            before = entry.end_offset();
        }
        before
    });
    let before = by_age.into_iter().chain(by_bytes).max()?;
    (before > state.log_start_offset()).then_some(before)
}

fn fenced(err: MetaError) -> TaskError {
    match err {
        MetaError::Rejected(ApplyError::Fenced { .. }) => TaskError::Fenced,
        other => TaskError::Meta(other),
    }
}

/// Trims every partition whose policy says so, then prunes WAL commit
/// records older than twice the commit window, all fenced by `fence`.
async fn apply(meta: &dyn MetaStore, fence: &Fence) -> Result<RetentionReport, TaskError> {
    let now = meta.now_ms();
    // One short read for the policies, then one per partition, so the scan
    // never holds the state lock for long.
    let policies: Vec<(StreamId, u32, operon_common::meta::Retention)> = meta
        .streams(Consistency::Local, None)
        .await?
        .into_iter()
        .filter(|st| st.retention.max_age_ms.is_some() || st.retention.max_bytes.is_some())
        .map(|st| (st.id, st.partitions, st.retention))
        .collect();
    let mut report = RetentionReport::default();
    for (stream, partitions, policy) in policies {
        for partition in 0..partitions {
            let before = meta
                .partition_index(Consistency::Local, stream, partition, 0, None)
                .await?
                .and_then(|state| trim_point(&state, policy.max_age_ms, policy.max_bytes, now));
            if let Some(before) = before {
                meta.trim_partition(stream, partition, before, Some(fence.clone()))
                    .await
                    .map_err(fenced)?;
                crate::failpoint!("retention.after_trim");
                report.trimmed += 1;
            }
        }
    }
    report.pruned = meta
        .prune_wal_commits(Some(fence.clone()))
        .await
        .map_err(fenced)?;
    Ok(report)
}

struct Shared {
    config: RetentionConfig,
    /// When a task of this source last started, to keep runs `interval` apart.
    last_run: Mutex<Option<Instant>>,
    report: Mutex<RetentionReport>,
}

/// Proposes the singleton retention task ([`RETENTION_TASK`], at
/// [`Priority::Maintenance`]) at most once per `interval`.
#[derive(Clone)]
pub struct RetentionSource {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for RetentionSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetentionSource")
            .field("config", &self.shared.config)
            .finish_non_exhaustive()
    }
}

impl RetentionSource {
    pub fn new(config: RetentionConfig) -> Self {
        Self {
            shared: Arc::new(Shared {
                config,
                last_run: Mutex::default(),
                report: Mutex::default(),
            }),
        }
    }

    /// Totals over every run of this source's task.
    pub fn report(&self) -> RetentionReport {
        *self
            .shared
            .report
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn task(&self) -> Candidate {
        let task: Arc<dyn Task> = Arc::new(RetentionTask {
            shared: self.shared.clone(),
        });
        (TaskKey::cluster(RETENTION_TASK), task)
    }
}

#[async_trait]
impl TaskSource for RetentionSource {
    fn priority(&self) -> Priority {
        Priority::Maintenance
    }

    async fn candidates(&self, _meta: &dyn MetaStore) -> Result<Vec<Candidate>, TaskError> {
        let due = self
            .shared
            .last_run
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_none_or(|last| last.elapsed() >= self.shared.config.interval);
        Ok(if due { vec![self.task()] } else { Vec::new() })
    }
}

struct RetentionTask {
    shared: Arc<Shared>,
}

#[async_trait]
impl Task for RetentionTask {
    async fn run(&self, ctx: TaskContext) -> Result<TaskOutcome, TaskError> {
        *self
            .shared
            .last_run
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(Instant::now());
        let report = apply(&*ctx.meta, &ctx.fence).await?;
        if report.trimmed > 0 || report.pruned > 0 {
            tracing::debug!(?report, "retention run");
        }
        let mut total = self
            .shared
            .report
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        total.trimmed += report.trimmed;
        total.pruned += report.pruned;
        Ok(TaskOutcome::Done)
    }
}

/// Runs the retention task once, through the worker framework, for tests and
/// tools.
#[derive(Clone, Debug)]
pub struct Retention {
    meta: Arc<dyn MetaStore>,
    owner: String,
    source: RetentionSource,
}

impl Retention {
    /// `owner` names this process incarnation in the task lease.
    pub fn new(
        meta: impl Into<Arc<dyn MetaStore>>,
        owner: impl Into<String>,
        config: RetentionConfig,
    ) -> Self {
        Self {
            meta: meta.into(),
            owner: owner.into(),
            source: RetentionSource::new(config),
        }
    }

    /// The task source, to add to a [`Worker`](operon_worker::Worker).
    pub fn source(&self) -> &RetentionSource {
        &self.source
    }

    /// Runs retention now (ignoring `interval`) under the task lease;
    /// `skipped` if another owner holds it.
    pub async fn run_once(&self) -> Result<RetentionReport, LogError> {
        struct Once(Candidate);
        #[async_trait]
        impl TaskSource for Once {
            fn priority(&self) -> Priority {
                Priority::Maintenance
            }
            async fn candidates(&self, _meta: &dyn MetaStore) -> Result<Vec<Candidate>, TaskError> {
                Ok(vec![self.0.clone()])
            }
        }
        let before = self.source.report();
        let ttl = (self.source.shared.config.interval * 3)
            .clamp(Duration::from_secs(1), Duration::from_secs(3600));
        let results = operon_worker::run_once(
            self.meta.clone(),
            &self.owner,
            ttl,
            &Once(self.source.task()),
        )
        .await
        .map_err(LogError::Task)?;
        let after = self.source.report();
        let mut report = RetentionReport {
            trimmed: after.trimmed - before.trimmed,
            pruned: after.pruned - before.pruned,
            skipped: false,
        };
        for (_, result) in results {
            match result {
                RunResult::LeaseHeld => report.skipped = true,
                RunResult::Ran(Ok(_)) => {}
                RunResult::Ran(Err(TaskError::Meta(err))) => return Err(LogError::Meta(err)),
                RunResult::Ran(Err(err)) => return Err(LogError::Task(err)),
            }
        }
        Ok(report)
    }
}
