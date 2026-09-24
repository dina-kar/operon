//! Retention settings and the bookkeeping that keeps metastore memory bounded:
//! WAL commit pruning and forgetting collected objects.

use operon_common::StreamId;

use super::MetaState;
use crate::command::{ApplyError, Reply};
use crate::types::{Retention, WAL_COMMIT_WINDOW_MS};

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
        stream.retention = retention;
        Ok(Reply::RetentionSet)
    }

    pub(super) fn prune_wal_commits(&mut self, now_ms: u64) -> Result<Reply, ApplyError> {
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

    pub(super) fn forget_objects(&mut self, objects: Vec<String>) -> Result<Reply, ApplyError> {
        let mut removed: u32 = 0;
        for object in objects {
            if self.retired.remove(&object).is_some() {
                removed = removed.saturating_add(1);
            }
        }
        Ok(Reply::Forgotten { removed })
    }
}
