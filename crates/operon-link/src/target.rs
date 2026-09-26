//! The target contract.

use std::collections::BTreeMap;

use async_trait::async_trait;
use operon_common::meta::Fence;
use operon_log::OffsetRecord;

use crate::error::LinkError;

/// What a target has committed: its version and, per source partition, the
/// next offset to apply.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TargetState {
    pub version: u64,
    /// Partition → next offset to apply. A partition without an entry starts
    /// at offset 0.
    pub applied: BTreeMap<u32, u64>,
}

/// One batch to commit: records (with their source partition) and, per
/// partition that advanced, the next offset to apply after this batch.
/// Offsets between a partition's previous applied offset and
/// `applied_after` that have no record in the batch were trimmed from the
/// stream before they could be applied.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ApplyBatch {
    pub records: Vec<(u32, OffsetRecord)>,
    pub applied_after: BTreeMap<u32, u64>,
}

/// Why a commit failed.
#[derive(Debug, thiserror::Error)]
pub enum CommitError {
    /// The target's version moved since `expected_version`: reload and retry.
    #[error("the target version moved")]
    Conflict,
    /// The task's lease moved on: another task owns the link now.
    #[error("fenced")]
    Fenced,
    #[error(transparent)]
    Other(#[from] LinkError),
}

/// A link target: loads its committed state and commits batches together
/// with the offsets they apply, atomically.
#[async_trait]
pub trait LinkTarget: Send + Sync {
    async fn load(&self) -> Result<TargetState, LinkError>;

    /// Commits `batch` on top of version `expected_version`, fenced by
    /// `fence`; returns the new version. A commit that returns an error other
    /// than `Conflict` or `Fenced` may or may not have landed: the next
    /// `load` tells.
    async fn commit(
        &self,
        expected_version: u64,
        batch: ApplyBatch,
        fence: &Fence,
    ) -> Result<u64, CommitError>;
}

/// The steps of a commit, where the crash gate's failpoints sit (and where a
/// test hook can hold a commit).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitStep {
    /// After the data object PUT (`link.after_data_put`).
    AfterDataPut,
    /// After the manifest PUT (`link.after_manifest_put`).
    AfterManifestPut,
    /// After the pointer CAS (`link.after_cas`).
    AfterCas,
}

/// A test hook awaited at every [`CommitStep`], given the committing task's
/// fence. Only with the `test-util` feature.
#[cfg(feature = "test-util")]
pub type CommitHook = std::sync::Arc<
    dyn Fn(CommitStep, Fence) -> futures::future::BoxFuture<'static, ()> + Send + Sync,
>;
