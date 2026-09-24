//! The Raft state machine: applies commands to a [`MetaState`] and keeps
//! snapshots in object storage (design §01 §6: `meta/snapshots/...`).

use std::fmt;
use std::future::Future;
use std::io;
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use openraft::storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine};
use openraft::{EntryPayload, OptionalSend};
use operon_store::{Store, StoreError};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, watch};

use crate::codec;
use crate::db::LocalDb;
use crate::raft::{
    LogId, NodeId, Snapshot, SnapshotData, SnapshotMeta, StoredMembership, TypeConfig,
};
use crate::state::MetaState;

pub(crate) const SNAPSHOT_POINTER_KEY: &str = "snapshot";

/// How long a snapshot upload or download is retried before its error is
/// returned. openraft treats every snapshot error as fatal to the node, so
/// transient object-store failures are absorbed here. Builds run in their own
/// task, so a long budget only delays the log purge that follows them.
const STORE_RETRY_BUDGET: Duration = Duration::from_secs(60);
/// Deleting a replaced snapshot is best effort, so it gives up sooner.
const DELETE_RETRY_BUDGET: Duration = Duration::from_secs(5);
const RETRY_FIRST_DELAY: Duration = Duration::from_millis(50);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(5);

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
    /// The index of the last applied log entry (0 before any), published after
    /// the state it describes is readable.
    applied_index: Arc<watch::Sender<u64>>,
}

impl StateReader {
    fn new() -> Self {
        Self {
            applied: Arc::default(),
            applied_index: Arc::new(watch::Sender::new(0)),
        }
    }

    /// Watches the index of the last applied log entry.
    pub(crate) fn watch_applied(&self) -> watch::Receiver<u64> {
        self.applied_index.subscribe()
    }

    fn publish_applied(&self) {
        let index = self.last_applied_index().unwrap_or(0);
        self.applied_index.send_if_modified(|current| {
            let changed = *current != index;
            *current = index;
            changed
        });
    }

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

/// This node's current snapshot, kept in memory. Serving it from here means a
/// follower's snapshot request never races a build that replaces (and
/// deletes) the object, and never stalls apply on a download. The cost is one
/// encoded copy of the state in memory.
#[derive(Clone)]
struct CurrentSnapshot {
    path: String,
    meta: SnapshotMeta,
    bytes: Arc<[u8]>,
}

impl CurrentSnapshot {
    fn to_snapshot(&self) -> Snapshot {
        Snapshot {
            meta: self.meta.clone(),
            snapshot: SnapshotData::new(self.bytes.to_vec()),
        }
    }
}

impl fmt::Debug for CurrentSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CurrentSnapshot")
            .field("path", &self.path)
            .field("meta", &self.meta)
            .field("len", &self.bytes.len())
            .finish()
    }
}

/// Stops a state machine's object-store retries, so that shutting a node down
/// does not wait them out. Holds no storage handles.
#[derive(Clone, Debug)]
pub(crate) struct SnapshotIoCloser {
    closed: Arc<watch::Sender<bool>>,
}

impl SnapshotIoCloser {
    /// Makes every pending and future snapshot upload or download fail at once.
    pub(crate) fn close(&self) {
        self.closed.send_replace(true);
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
    /// The snapshot the local pointer names. Written only with `snapshot_lock` held.
    current: std::sync::Mutex<Option<CurrentSnapshot>>,
    closed: Arc<watch::Sender<bool>>,
    /// Total time a snapshot upload or download may take, in ms (default
    /// [`STORE_RETRY_BUDGET`]).
    io_budget_ms: std::sync::atomic::AtomicU64,
}

/// Whether a store error may clear up on its own: only backend errors (network,
/// throttling, 5xx) do. A missing object or a bad path will not.
fn is_transient(err: &StoreError) -> bool {
    matches!(err, StoreError::Backend(_))
}

/// The metastore's openraft state machine. Cheap to clone; clones share state.
///
/// The applied state is held in memory. Snapshots are written to object storage
/// under `<prefix>/<node_id>/<term>-<index>.snap` (each node writes its own, so a
/// node can delete its previous snapshot without affecting others), and the local
/// database records which one is current. On open, the state is loaded from that
/// snapshot, and openraft re-applies the log entries after it. The current
/// snapshot is also kept in memory, to serve it to followers.
///
/// Transient object-store failures are retried with backoff (for up to a
/// minute) instead of being returned, because openraft stops the node on any
/// snapshot error.
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
        let sm = Self {
            inner: Arc::new(Inner {
                node_id,
                store,
                prefix: prefix.into(),
                db,
                applied: StateReader::new(),
                snapshot_lock: Mutex::new(()),
                current: std::sync::Mutex::new(None),
                closed: Arc::new(watch::Sender::new(false)),
                io_budget_ms: std::sync::atomic::AtomicU64::new(
                    u64::try_from(STORE_RETRY_BUDGET.as_millis()).unwrap_or(u64::MAX),
                ),
            }),
        };
        if let Some(raw) = sm.inner.db.get_meta(SNAPSHOT_POINTER_KEY).await? {
            let pointer: SnapshotPointer = codec::decode(&raw)?;
            let store = &sm.inner.store;
            let (data, _) = sm
                .retry(sm.io_budget(), "read snapshot", &pointer.path, || {
                    store.get(&pointer.path)
                })
                .await?;
            let (meta, state) = codec::decode_snapshot(&data)?;
            if meta.last_log_id != pointer.last_log_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "snapshot {} covers {:?}, but the local pointer expects {:?}",
                        pointer.path, meta.last_log_id, pointer.last_log_id
                    ),
                ));
            }
            *sm.applied_mut() = Applied {
                last_applied: meta.last_log_id,
                membership: meta.last_membership.clone(),
                state,
            };
            sm.set_current(Some(CurrentSnapshot {
                path: pointer.path,
                meta,
                bytes: Arc::from(data.as_ref()),
            }));
            sm.inner.applied.publish_applied();
        }
        Ok(sm)
    }

    /// Runs `f` against the applied state.
    pub fn read<T>(&self, f: impl FnOnce(&MetaState) -> T) -> T {
        self.inner.applied.read(f)
    }

    pub(crate) fn reader(&self) -> StateReader {
        self.inner.applied.clone()
    }

    pub(crate) fn closer(&self) -> SnapshotIoCloser {
        SnapshotIoCloser {
            closed: self.inner.closed.clone(),
        }
    }

    /// Sets the total time a snapshot upload or download may take, retries
    /// included (default 60 s). When it runs out the operation fails, and
    /// openraft stops the node, rather than wait on the store forever.
    pub fn set_io_budget(&self, budget: Duration) {
        self.inner.io_budget_ms.store(
            u64::try_from(budget.as_millis()).unwrap_or(u64::MAX),
            std::sync::atomic::Ordering::SeqCst,
        );
    }

    fn io_budget(&self) -> Duration {
        Duration::from_millis(
            self.inner
                .io_budget_ms
                .load(std::sync::atomic::Ordering::SeqCst),
        )
    }

    fn current(&self) -> Option<CurrentSnapshot> {
        let current = self
            .inner
            .current
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        current.clone()
    }

    fn set_current(&self, snapshot: Option<CurrentSnapshot>) {
        *self
            .inner
            .current
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = snapshot;
    }

    /// Runs the object-store operation `op`, retrying transient failures with
    /// exponential backoff, for at most `budget` in total: each attempt is cut
    /// off at the deadline too (M0.2 re-review N2), so a store that hangs
    /// cannot stretch it. Fails at once if the state machine is closed (the
    /// node is shutting down).
    async fn retry<T, F, Fut>(
        &self,
        budget: Duration,
        what: &str,
        path: &str,
        mut op: F,
    ) -> io::Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, StoreError>>,
    {
        let mut closed = self.inner.closed.subscribe();
        let deadline = Instant::now() + budget;
        let mut delay = RETRY_FIRST_DELAY;
        let closed_err = || {
            io::Error::new(
                io::ErrorKind::Interrupted,
                format!("{what} {path}: the node is shutting down"),
            )
        };
        let timed_out = || {
            io::Error::new(
                io::ErrorKind::TimedOut,
                format!("{what} {path}: gave up after {budget:?}"),
            )
        };
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let result = tokio::select! {
                result = tokio::time::timeout(remaining, op()) => match result {
                    Ok(result) => result,
                    Err(_) => return Err(timed_out()),
                },
                _ = closed.wait_for(|closed| *closed) => return Err(closed_err()),
            };
            let err = match result {
                Ok(value) => return Ok(value),
                Err(err) if !is_transient(&err) || Instant::now() + delay > deadline => {
                    return Err(io::Error::other(err));
                }
                Err(err) => err,
            };
            tracing::warn!(%path, %err, ?delay, "{what} failed; retrying");
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                _ = closed.wait_for(|closed| *closed) => return Err(closed_err()),
            }
            delay = (delay * 2).min(RETRY_MAX_DELAY);
        }
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
    /// `snapshot_lock` held, and only with a snapshot no older than the current
    /// one. Returns once the snapshot is durable.
    async fn persist(&self, meta: &SnapshotMeta, bytes: Arc<[u8]>) -> io::Result<()> {
        let previous = self.current();
        let path = self.snapshot_path(meta.last_log_id.as_ref());
        let store = &self.inner.store;
        let data = Bytes::from_owner(bytes.clone());
        self.retry(self.io_budget(), "write snapshot", &path, || {
            store.put(&path, data.clone())
        })
        .await?;
        crate::failpoint!("meta.snapshot.after_put");
        let pointer = SnapshotPointer {
            path: path.clone(),
            last_log_id: meta.last_log_id,
        };
        self.inner
            .db
            .put_meta(SNAPSHOT_POINTER_KEY, codec::encode(&pointer)?)
            .await?;
        crate::failpoint!("meta.snapshot.after_pointer");
        self.set_current(Some(CurrentSnapshot {
            path: path.clone(),
            meta: meta.clone(),
            bytes,
        }));
        if let Some(previous) = previous
            && previous.path != path
            && let Err(err) = self
                .retry(
                    DELETE_RETRY_BUDGET,
                    "delete replaced snapshot",
                    &previous.path,
                    || store.delete(&previous.path),
                )
                .await
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
        // Unreachable: the build reads the live state, which an install has
        // already replaced by the time it releases the lock. If a newer snapshot
        // is current anyway, it serves just as well.
        if let Some(current) = self.current()
            && current.meta.last_log_id > meta.last_log_id
        {
            return Ok(current.to_snapshot());
        }
        let snapshot = Snapshot {
            meta: meta.clone(),
            snapshot: SnapshotData::new(bytes.clone()),
        };
        self.persist(&meta, Arc::from(bytes)).await?;
        Ok(snapshot)
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
            self.inner.applied.publish_applied();
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
        // A corrupt snapshot fails here, and openraft stops the node on any
        // install error. The in-process transport hands over the leader's own
        // verified bytes; a network transport must verify the checksum when it
        // receives a snapshot, so that corruption in transit fails the RPC.
        let (embedded, state) = codec::decode_snapshot(&bytes)?;
        if embedded != *meta {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot metadata does not match its contents",
            ));
        }
        // openraft installs only snapshots newer than everything committed here,
        // so this is unreachable. Installing an older one would move the state
        // back behind the current pointer.
        if let Some(current) = self.current()
            && current.meta.last_log_id > meta.last_log_id
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "snapshot at {:?} is older than the current snapshot at {:?}",
                    meta.last_log_id, current.meta.last_log_id
                ),
            ));
        }
        // Durable first, so a crash right after installing still finds the snapshot.
        self.persist(meta, Arc::from(bytes)).await?;
        *self.applied_mut() = Applied {
            last_applied: meta.last_log_id,
            membership: meta.last_membership.clone(),
            state,
        };
        self.inner.applied.publish_applied();
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> io::Result<Option<Snapshot>> {
        Ok(self.current().map(|current| current.to_snapshot()))
    }
}
