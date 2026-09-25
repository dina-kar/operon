//! Retention settings and the bookkeeping that keeps metastore memory bounded:
//! WAL commit pruning and forgetting collected objects.

use operon_common::StreamId;

use super::MetaState;
use crate::command::{ApplyError, Reply};
use crate::types::{Fence, Retention, WAL_COMMIT_WINDOW_MS};

impl MetaState {
    pub(super) fn set_retention(
        &mut self,
        stream: StreamId,
        retention: Retention,
    ) -> Result<Reply, ApplyError> {
        let stream = self
            .streams
            .get_mut(&stream)
            .ok_or(ApplyError::StreamNotFound(stream))?;
        // A collection's implicit stream is trimmed only by its collection
        // (plan M1.1 Ruling 12), never by a retention policy.
        if stream.name.starts_with('_') {
            return Err(ApplyError::InvalidArgument(format!(
                "stream {} belongs to a collection; its retention cannot be set",
                stream.id
            )));
        }
        stream.retention = retention;
        Ok(Reply::RetentionSet)
    }

    pub(super) fn prune_wal_commits(
        &mut self,
        fence: Option<Fence>,
        now_ms: u64,
    ) -> Result<Reply, ApplyError> {
        if let Some(fence) = &fence {
            self.check_fence(fence)?;
        }
        self.clock_ms = self.clock_ms.max(now_ms);
        let clock_ms = self.clock_ms;
        let before = self.wal_commits.len();
        self.wal_commits.retain(|_, record| {
            record
                .created_at_ms
                .saturating_add(2 * WAL_COMMIT_WINDOW_MS)
                >= clock_ms
        });
        let removed = before - self.wal_commits.len();
        Ok(Reply::Pruned {
            removed: u32::try_from(removed).unwrap_or(u32::MAX),
        })
    }

    pub(super) fn forget_objects(
        &mut self,
        objects: Vec<String>,
        fence: Option<Fence>,
    ) -> Result<Reply, ApplyError> {
        if let Some(fence) = &fence {
            self.check_fence(fence)?;
        }
        let mut removed: u32 = 0;
        for object in objects {
            if self.retired.remove(&object).is_some() {
                removed = removed.saturating_add(1);
            }
        }
        Ok(Reply::Forgotten { removed })
    }
}
