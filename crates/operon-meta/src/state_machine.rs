//! The Raft state machine: applies commands to a [`MetaState`] and keeps
//! snapshots in object storage (design §01 §6: `meta/snapshots/...`).

use std::io;
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use openraft::storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine};
use openraft::{EntryPayload, OptionalSend};
use operon_store::Store;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::codec;
use crate::db::LocalDb;
use crate::raft::{
    LogId, NodeId, Snapshot, SnapshotData, SnapshotMeta, StoredMembership, TypeConfig,
};
use crate::state::MetaState;

const SNAPSHOT_POINTER_KEY: &str = "snapshot";

/// Where this node's current snapshot lives in object storage.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SnapshotPointer {
    path: String,
    last_log_id: Option<LogId>,
}

#[derive(Debug, Default)]
struct Applied {
    last_applied: Option<LogId>,
    membership: StoredMembership,
    state: MetaState,
}

/// Read access to the applied state, shared with the state machine. Holds no
/// storage handles, so it does not keep the local database open.
#[derive(Clone, Debug)]
pub(crate) struct StateReader {
    applied: Arc<RwLock<Applied>>,
}

impl StateReader {
    fn applied(&self) -> RwLockReadGuard<'_, Applied> {
        self.applied.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn applied_mut(&self) -> RwLockWriteGuard<'_, Applied> {
        self.applied.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// Runs `f` against the applied state.
    pub(crate) fn read<T>(&self, f: impl FnOnce(&MetaState) -> T) -> T {
        f(&self.applied().state)
    }

    /// Index of the last applied log entry.
    pub(crate) fn last_applied_index(&self) -> Option<u64> {
        self.applied().last_applied.map(|id| id.index)
    }
}

#[derive(Debug)]
struct Inner {
    node_id: NodeId,
    store: Store,
    prefix: String,
    db: LocalDb,
    applied: StateReader,
    /// Serializes snapshot builds and installs, so the snapshot pointer only moves forward.
    snapshot_lock: Mutex<()>,
}

/// The metastore's openraft state machine. Cheap to clone; clones share state.
///
/// The applied state is held in memory. Snapshots are written to object storage
/// under `<prefix>/<node_id>/<term>-<index>.snap` (each node writes its own, so a
/// node can delete its previous snapshot without affecting others), and the local
/// database records which one is current. On open, the state is loaded from that
/// snapshot, and openraft re-applies the log entries after it.
#[derive(Clone, Debug)]
pub struct StateMachineStore {
    inner: Arc<Inner>,
}

impl StateMachineStore {
    /// Opens the state machine of `node_id`, loading its current snapshot, if
    /// any, from `store`.
    pub async fn open(
        node_id: NodeId,
        store: Store,
        prefix: impl Into<String>,
        db: LocalDb,
    ) -> io::Result<Self> {
        let mut applied = Applied::default();
        if let Some(bytes) = db.get_meta(SNAPSHOT_POINTER_KEY).await? {
            let pointer: SnapshotPointer = codec::decode(&bytes)?;
            let (data, _) = store.get(&pointer.path).await.map_err(io::Error::other)?;
            let (meta, state) = codec::decode_snapshot(&data)?;
            applied = Applied {
                last_applied: meta.last_log_id,
                membership: meta.last_membership,
                state,
            };
        }
        Ok(Self {
            inner: Arc::new(Inner {
                node_id,
                store,
                prefix: prefix.into(),
                db,
                applied: StateReader {
                    applied: Arc::new(RwLock::new(applied)),
                },
                snapshot_lock: Mutex::new(()),
            }),
        })
    }

    /// Runs `f` against the applied state.
    pub fn read<T>(&self, f: impl FnOnce(&MetaState) -> T) -> T {
        self.inner.applied.read(f)
    }

    pub(crate) fn reader(&self) -> StateReader {
        self.inner.applied.clone()
    }

    fn applied(&self) -> RwLockReadGuard<'_, Applied> {
        self.inner.applied.applied()
    }

    fn applied_mut(&self) -> RwLockWriteGuard<'_, Applied> {
        self.inner.applied.applied_mut()
    }

    fn snapshot_path(&self, last_log_id: Option<&LogId>) -> String {
        let Inner {
            prefix, node_id, ..
        } = &*self.inner;
        match last_log_id {
            Some(id) => format!(
                "{prefix}/{node_id}/{:020}-{:020}.snap",
                id.leader_id.term, id.index
            ),
            None => format!("{prefix}/{node_id}/empty.snap"),
        }
    }

    /// Writes snapshot bytes to object storage and makes them this node's current
    /// snapshot, then deletes the snapshot they replace. Must be called with
    /// `snapshot_lock` held. Returns once the snapshot is durable.
    async fn persist(&self, meta: &SnapshotMeta, bytes: &[u8]) -> io::Result<()> {
        let db = &self.inner.db;
        let previous: Option<SnapshotPointer> = match db.get_meta(SNAPSHOT_POINTER_KEY).await? {
            Some(raw) => Some(codec::decode(&raw)?),
            None => None,
        };
        if let Some(previous) = &previous
            && previous.last_log_id > meta.last_log_id
        {
            // A newer snapshot is already current; this one is redundant.
            return Ok(());
        }
        let path = self.snapshot_path(meta.last_log_id.as_ref());
        self.inner
            .store
            .put(&path, Bytes::copy_from_slice(bytes))
            .await
            .map_err(io::Error::other)?;
        let pointer = SnapshotPointer {
            path: path.clone(),
            last_log_id: meta.last_log_id,
        };
        db.put_meta(SNAPSHOT_POINTER_KEY, codec::encode(&pointer)?)
            .await?;
        if let Some(previous) = previous
            && previous.path != path
            && let Err(err) = self.inner.store.delete(&previous.path).await
        {
            // Harmless: the old snapshot is unreferenced and only costs storage.
            tracing::warn!(path = %previous.path, %err, "failed to delete replaced snapshot");
        }
        Ok(())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    type SnapshotData = SnapshotData;

    async fn build_snapshot(&mut self) -> io::Result<Snapshot> {
        let _guard = self.inner.snapshot_lock.lock().await;
        let (meta, bytes) = {
            let applied = self.applied();
            let meta = SnapshotMeta {
                last_log_id: applied.last_applied,
                last_membership: applied.membership.clone(),
            };
            let bytes = codec::encode_snapshot(&meta, &applied.state)?;
            (meta, bytes)
        };
        self.persist(&meta, &bytes).await?;
        Ok(Snapshot {
            meta,
            snapshot: SnapshotData::new(bytes),
        })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotData = SnapshotData;
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> io::Result<(Option<LogId>, StoredMembership)> {
        let applied = self.applied();
        Ok((applied.last_applied, applied.membership.clone()))
    }

    async fn apply<S>(&mut self, mut entries: S) -> io::Result<()>
    where
        S: Stream<Item = io::Result<EntryResponder<TypeConfig>>> + Unpin + OptionalSend,
    {
        while let Some((entry, responder)) = entries.try_next().await? {
            let reply = {
                let mut applied = self.applied_mut();
                applied.last_applied = Some(entry.log_id);
                match entry.payload {
                    EntryPayload::Blank => None,
                    EntryPayload::Normal(command) => Some(applied.state.apply(command)),
                    EntryPayload::Membership(membership) => {
                        applied.membership = StoredMembership::new(Some(entry.log_id), membership);
                        None
                    }
                }
            };
            if let Some(responder) = responder {
                responder.send(reply);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Self::SnapshotData,
    ) -> io::Result<()> {
        let _guard = self.inner.snapshot_lock.lock().await;
        let bytes = snapshot.into_inner();
        let (embedded, state) = codec::decode_snapshot(&bytes)?;
        if embedded != *meta {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot metadata does not match its contents",
            ));
        }
        // Durable first, so a crash right after installing still finds the snapshot.
        self.persist(meta, &bytes).await?;
        *self.applied_mut() = Applied {
            last_applied: meta.last_log_id,
            membership: meta.last_membership.clone(),
            state,
        };
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> io::Result<Option<Snapshot>> {
        let Some(raw) = self.inner.db.get_meta(SNAPSHOT_POINTER_KEY).await? else {
            return Ok(None);
        };
        let pointer: SnapshotPointer = codec::decode(&raw)?;
        let (data, _) = self
            .inner
            .store
            .get(&pointer.path)
            .await
            .map_err(io::Error::other)?;
        let (meta, _) = codec::decode_snapshot(&data)?;
        Ok(Some(Snapshot {
            meta,
            snapshot: SnapshotData::new(data.to_vec()),
        }))
    }
}
