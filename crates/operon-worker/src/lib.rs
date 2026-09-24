//! Operon's worker task framework (design §09 §5, §6).
//!
//! Background work (segmenting, retention, link apply, garbage collection) is
//! split into *tasks*, each named by a [`TaskKey`]. [`TaskSource`]s propose
//! the tasks that have work; a [`Worker`] polls them, takes the metastore
//! lease `task/<key>` for each task it runs, and runs it with a [`Fence`] at
//! the lease's epoch and a cancellation signal. The lease is renewed every
//! third of its TTL; a task whose lease is taken over is cancelled, and every
//! metastore commit it attempts with its fence is rejected.
//!
//! Scheduling: candidates are ordered by [`Priority`] (link apply first, GC
//! last), then round-robin across namespaces within a priority, up to
//! [`WorkerConfig::max_concurrent`] tasks in total and
//! [`WorkerConfig::max_per_namespace`] per namespace.

mod runner;
mod worker;

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use operon_common::NamespaceId;
use operon_meta::{Fence, MetaClient, MetaError};
pub use tokio_util::sync::CancellationToken;

pub use runner::{RunResult, run_once};
pub use worker::{Worker, WorkerConfig, WorkerHandle};

/// Task priorities, highest first (design §09 §6): link apply (freshness
/// SLO) > segmenting > merges and compaction > hot builds > maintenance > GC.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Priority {
    LinkApply,
    Segmenting,
    Compaction,
    HotBuild,
    Maintenance,
    Gc,
}

/// Names a task. The lease that guards it is `task/<key>`, so `key` must be
/// unique across the cluster. `namespace` is the tenant the task works for
/// (`None` for cluster-wide tasks such as GC); it drives fair share.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct TaskKey {
    pub namespace: Option<NamespaceId>,
    pub key: String,
}

impl TaskKey {
    /// A task working for `namespace`.
    pub fn new(namespace: NamespaceId, key: impl Into<String>) -> Self {
        Self {
            namespace: Some(namespace),
            key: key.into(),
        }
    }

    /// A cluster-wide task.
    pub fn cluster(key: impl Into<String>) -> Self {
        Self {
            namespace: None,
            key: key.into(),
        }
    }

    /// The metastore lease that guards this task: `task/<key>`.
    pub fn lease(&self) -> String {
        format!("task/{}", self.key)
    }
}

impl fmt::Display for TaskKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.key)
    }
}

/// What a running task gets.
#[derive(Clone, Debug)]
pub struct TaskContext {
    pub key: TaskKey,
    /// The task lease at the epoch this run holds it. Every metastore commit
    /// the task makes must carry it, so that a run whose lease was taken over
    /// cannot change anything.
    pub fence: Fence,
    /// Cancelled when the task must stop: its lease was lost or the worker is
    /// stopping. A task must stop promptly once it is cancelled.
    pub cancel: CancellationToken,
    pub meta: MetaClient,
}

/// How a run ended.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TaskOutcome {
    /// Finished.
    Done,
    /// There is more work right away: the worker polls again at once.
    MoreWork,
    /// Nothing to do: wait for the next poll.
    Idle,
}

/// Why a run failed. The worker logs it and proposes the task again on a
/// later poll.
#[derive(Debug, thiserror::Error)]
pub enum TaskError {
    /// A commit was rejected because the task's lease moved on: another run
    /// holds the task now.
    #[error("fenced: the task lease was taken over")]
    Fenced,
    #[error("metastore: {0}")]
    Meta(#[from] MetaError),
    #[error("{0}")]
    Failed(Box<dyn std::error::Error + Send + Sync>),
}

impl TaskError {
    /// Wraps any error as [`TaskError::Failed`].
    pub fn failed(err: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        TaskError::Failed(err.into())
    }
}

/// A unit of background work. One run is guarded by one lease epoch.
#[async_trait]
pub trait Task: Send + Sync {
    async fn run(&self, ctx: TaskContext) -> Result<TaskOutcome, TaskError>;
}

/// Proposes tasks that have work.
#[async_trait]
pub trait TaskSource: Send + Sync {
    /// The priority of every task this source proposes.
    fn priority(&self) -> Priority;
    /// The tasks that have work now. Called on every poll; keep it cheap
    /// (metastore reads).
    async fn candidates(&self, meta: &MetaClient) -> Result<Vec<Candidate>, TaskError>;
}

/// A proposed task.
pub type Candidate = (TaskKey, Arc<dyn Task>);
