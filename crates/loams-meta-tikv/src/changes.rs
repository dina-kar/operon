//! The change watch (design §20 §11.3). A watch wakes at once on the
//! handle's own writes (`commit_wal` included, so this handle's offset
//! watchers see its commits directly) and on every poll, spuriously or not.
//!
//! No `v/` change counters (row T5-4): the trait has one unscoped watch that
//! must wake on every change, `commit_wal` and pointer writes of other
//! handles included; a counter those writes bumped would be one hot key
//! every commit touches, and counters they did not bump could not wake the
//! watch for them. Scoped counters arrive with D63's scoped change feed,
//! when a watcher can say what it cares about.

use std::time::Duration;

use async_trait::async_trait;
use loams_common::meta::{ChangeWait, MetaChanges, MetaStopped};

use crate::TikvMeta;

/// Wakes on the handle's own writes at once, and every `poll` in any case.
#[derive(Debug)]
struct PollWait {
    changes: tokio::sync::watch::Receiver<u64>,
    poll: Duration,
}

#[async_trait]
impl ChangeWait for PollWait {
    async fn changed(&mut self) -> Result<(), MetaStopped> {
        tokio::select! {
            changed = self.changes.changed() => changed.map_err(|_| MetaStopped),
            () = tokio::time::sleep(self.poll) => Ok(()),
        }
    }
}

impl TikvMeta {
    pub(crate) fn watch(&self) -> MetaChanges {
        let mut changes = self.inner.changes.subscribe();
        changes.mark_unchanged();
        MetaChanges::new(PollWait {
            changes,
            poll: self.inner.poll,
        })
    }
}
