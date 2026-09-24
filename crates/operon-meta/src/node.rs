//! A running meta node: Raft, local storage and the typed client API.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use openraft::async_runtime::WatchReceiver;
use openraft::error::{ClientWriteError, InitializeError, LinearizableReadError, RaftError};
use openraft::metrics::WaitError;
use openraft::{BasicNode, Raft, ReadPolicy, SnapshotPolicy};
use operon_common::{NamespaceId, StreamId};
use operon_store::Store;
use redb::Database;

use crate::clock::{Clock, SystemClock};
use crate::command::{Command, Reply};
use crate::db::LocalDb;
use crate::error::MetaError;
use crate::log_store::{LogStore, VOTE_KEY};
use crate::network::{MetaRaft, NetworkFactory, Router};
use crate::raft::NodeId;
use crate::state::MetaState;
use crate::state_machine::{
    SNAPSHOT_POINTER_KEY, SnapshotIoCloser, StateMachineStore, StateReader,
};
use crate::types::{Fence, LeaseGrant, Retention, WalChunk, WalClass};

/// How to start a meta node.
#[derive(Clone, Debug)]
pub struct MetaConfig {
    pub node_id: NodeId,
    /// Holds `meta.redb`: the Raft log, vote and snapshot pointer.
    pub data_dir: PathBuf,
    /// Where snapshots are written (design §01 §6).
    pub store: Store,
    /// Object path prefix for snapshots. Default `meta/snapshots`.
    pub snapshot_prefix: String,
    /// Build a snapshot after this many log entries; must be at least 1.
    /// Default 10 000.
    pub snapshot_every: u64,
    /// Log entries kept after a snapshot, so slightly lagging followers catch
    /// up from the log instead of a full snapshot. Default 1 000.
    pub logs_after_snapshot: u64,
    /// Upper bound on each write and linearizable read. Default 5 s.
    pub request_timeout: Duration,
    /// Stamps lease commands. Default [`SystemClock`].
    pub clock: Arc<dyn Clock>,
    /// Lets a node with no local state start even though `snapshot_prefix`
    /// already holds snapshots. Default `false`: such a node has most likely
    /// lost its data directory, and starting it empty would fork the
    /// metastore and later overwrite the snapshots it should be restored
    /// from. Set it only when those snapshots are known to be stale.
    pub allow_fresh_start_with_existing_snapshots: bool,
    /// How far ahead of the leader's own clock a command's time stamp
    /// (`now_ms`, or a WAL commit's `created_at_ms`) may be. The leader
    /// refuses to propose a command stamped further ahead, with
    /// [`MetaError::ClockSkew`]: the metastore clock never goes back, so one
    /// such stamp would otherwise make every later WAL commit stale until real
    /// time caught up. Default 60 s.
    pub max_clock_skew: Duration,
}

impl MetaConfig {
    pub fn new(node_id: NodeId, data_dir: impl Into<PathBuf>, store: Store) -> Self {
        Self {
            node_id,
            data_dir: data_dir.into(),
            store,
            snapshot_prefix: "meta/snapshots".to_string(),
            snapshot_every: 10_000,
            logs_after_snapshot: 1_000,
            request_timeout: Duration::from_secs(5),
            clock: Arc::new(SystemClock),
            allow_fresh_start_with_existing_snapshots: false,
            max_clock_skew: Duration::from_secs(60),
        }
    }
}

/// How fresh a read must be.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Consistency {
    /// Reflects every write acknowledged before the read began. Served only by
    /// the leader, after it confirms its leadership with a quorum.
    Linearizable,
    /// Whatever this node has applied so far; may be stale on a follower or on
    /// a leader that has been cut off.
    Local,
}

/// A node's Raft progress, for monitoring and tests. Indexes are Raft log indexes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RaftStatus {
    pub leader: Option<NodeId>,
    pub last_applied: Option<u64>,
    /// Last log index covered by this node's current snapshot.
    pub snapshot: Option<u64>,
    /// Last log index removed from this node's local log.
    pub purged: Option<u64>,
}

struct Inner {
    id: NodeId,
    raft: MetaRaft,
    state: StateReader,
    /// Lets `shutdown` wait until Raft's tasks have closed the local database.
    db: Weak<Database>,
    /// Lets `shutdown` cut short snapshot uploads that are being retried.
    snapshot_io: SnapshotIoCloser,
    router: Router,
    clock: Arc<dyn Clock>,
    request_timeout: Duration,
    max_clock_skew: Duration,
}

/// A running meta node. Cheap to clone; clones share the node.
///
/// Call [`MetaNode::shutdown`] before dropping the last handle: the [`Router`]
/// keeps a node that was not shut down running, with its local database open.
#[derive(Clone)]
pub struct MetaNode {
    inner: Arc<Inner>,
}

impl fmt::Debug for MetaNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetaNode")
            .field("id", &self.inner.id)
            .finish_non_exhaustive()
    }
}

fn unavailable(err: impl fmt::Display) -> MetaError {
    MetaError::Unavailable(err.to_string())
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// How often [`MetaNode::snapshot`] re-requests a build while it waits.
const SNAPSHOT_RETRIGGER: Duration = Duration::from_millis(50);

/// Refuses to start a node that has no local state while the snapshot store
/// already holds metastore snapshots: its data directory was most likely lost
/// or replaced, and starting empty would fork the metastore (design §01 §1:
/// the bucket is the recovery point).
async fn check_fresh_start(config: &MetaConfig, db: &LocalDb) -> Result<(), MetaError> {
    if config.allow_fresh_start_with_existing_snapshots
        || db.get_meta(SNAPSHOT_POINTER_KEY).await?.is_some()
        || db.get_meta(VOTE_KEY).await?.is_some()
    {
        return Ok(());
    }
    let prefix = format!("{}/", config.snapshot_prefix.trim_end_matches('/'));
    let existing = config
        .store
        .list(&prefix)
        .await
        .map_err(std::io::Error::other)?;
    match existing.iter().find(|o| o.path.ends_with(".snap")) {
        None => Ok(()),
        Some(found) => Err(MetaError::Config(format!(
            "node {} has no local state in {}, but the snapshot store already holds \
             metastore snapshots (for example {}); restore the node's data directory, \
             or set allow_fresh_start_with_existing_snapshots if they are stale",
            config.node_id,
            config.data_dir.display(),
            found.path
        ))),
    }
}

impl MetaNode {
    /// Opens the node's local storage, loads its latest snapshot, and starts
    /// Raft. A node that was never initialized waits for
    /// [`MetaNode::initialize`] (here or on a peer) before it can serve requests.
    ///
    /// A node with no local state refuses to start if the snapshot store
    /// already holds snapshots, unless
    /// [`MetaConfig::allow_fresh_start_with_existing_snapshots`] is set.
    pub async fn start(config: MetaConfig, router: &Router) -> Result<Self, MetaError> {
        if config.snapshot_every == 0 {
            return Err(MetaError::Config(
                "snapshot_every must be at least 1".to_string(),
            ));
        }
        let raft_config = openraft::Config {
            cluster_name: "operon-meta".to_string(),
            snapshot_policy: SnapshotPolicy::LogsSinceLast(config.snapshot_every),
            max_in_snapshot_log_to_keep: config.logs_after_snapshot,
            ..Default::default()
        }
        .validate()
        .map_err(|e| MetaError::Config(e.to_string()))?;

        let db = LocalDb::open(&config.data_dir)?;
        check_fresh_start(&config, &db).await?;
        let db_handle = db.downgrade();
        let sm = StateMachineStore::open(
            config.node_id,
            config.store.clone(),
            config.snapshot_prefix.clone(),
            db.clone(),
        )
        .await?;
        let state = sm.reader();
        let snapshot_io = sm.closer();
        let network = NetworkFactory::new(router.clone(), config.node_id);
        let raft = Raft::new(
            config.node_id,
            Arc::new(raft_config),
            network,
            LogStore::new(db),
            sm,
        )
        .await
        .map_err(unavailable)?;
        // `Raft::new` has already re-applied the committed entries after the
        // snapshot (the log store persists the commit index), so local reads
        // see the pre-restart state. Wait for the core's first metrics too, so
        // that `status` reports it from the start.
        let recovered = state.last_applied_index();
        raft.wait(Some(config.request_timeout))
            .metrics(
                |m| m.last_applied.map(|id| id.index) >= recovered,
                "the recovered state in the metrics",
            )
            .await
            .map_err(unavailable)?;
        router.register(config.node_id, raft.clone());

        Ok(Self {
            inner: Arc::new(Inner {
                id: config.node_id,
                raft,
                state,
                db: db_handle,
                snapshot_io,
                router: router.clone(),
                clock: config.clock,
                request_timeout: config.request_timeout,
                max_clock_skew: config.max_clock_skew,
            }),
        })
    }

    /// Makes `members` the cluster's first voters. Call it once, on any member,
    /// when the cluster is new; calling it again (anywhere) with the same
    /// members is a no-op. Calling it on a node that already belongs to a
    /// cluster with other voters fails with [`MetaError::Config`].
    pub async fn initialize(
        &self,
        members: impl IntoIterator<Item = NodeId>,
    ) -> Result<(), MetaError> {
        let raft = &self.inner.raft;
        let members: BTreeSet<NodeId> = members.into_iter().collect();
        if raft.is_initialized().await.map_err(unavailable)? {
            return self.check_voters(&members).await;
        }
        let nodes: BTreeMap<NodeId, BasicNode> = members
            .iter()
            .map(|id| (*id, BasicNode::default()))
            .collect();
        match raft.initialize(nodes).await {
            Ok(()) => Ok(()),
            Err(RaftError::APIError(InitializeError::NotAllowed(_))) => {
                self.check_voters(&members).await
            }
            Err(RaftError::APIError(InitializeError::NotInMembers(e))) => {
                Err(MetaError::Config(e.to_string()))
            }
            Err(e) => Err(unavailable(e)),
        }
    }

    /// Checks that an already initialized node's voters are `members`. A node
    /// that has so far only seen a peer's vote request knows no membership
    /// yet, and passes.
    async fn check_voters(&self, members: &BTreeSet<NodeId>) -> Result<(), MetaError> {
        let voters: BTreeSet<NodeId> = self
            .inner
            .raft
            .with_raft_state(|st| st.membership_state.effective().voter_ids().collect())
            .await
            .map_err(unavailable)?;
        if voters.is_empty() || voters == *members {
            Ok(())
        } else {
            Err(MetaError::Config(format!(
                "already initialized with voters {voters:?}, not {members:?}"
            )))
        }
    }

    pub fn id(&self) -> NodeId {
        self.inner.id
    }

    /// Watches the index of the last log entry this node has applied (0 before
    /// any). It changes after every applied entry, once the entry's effect is
    /// visible to [`Consistency::Local`] reads, so a reader waiting for a
    /// state change subscribes, reads, and then waits for a change.
    pub fn watch_applied(&self) -> tokio::sync::watch::Receiver<u64> {
        self.inner.state.watch_applied()
    }

    /// The leader this node currently knows of.
    pub async fn current_leader(&self) -> Option<NodeId> {
        self.inner.raft.current_leader().await
    }

    /// This node's Raft progress, as of openraft's latest metrics report
    /// (which may trail the node by a moment).
    pub fn status(&self) -> RaftStatus {
        let metrics = self.inner.raft.metrics();
        let m = metrics.borrow_watched();
        RaftStatus {
            leader: m.current_leader,
            last_applied: m.last_applied.map(|id| id.index),
            snapshot: m.snapshot.map(|id| id.index),
            purged: m.purged.map(|id| id.index),
        }
    }

    /// Waits until this node knows of a leader, and returns it.
    pub async fn wait_for_leader(&self, timeout: Duration) -> Result<NodeId, MetaError> {
        let metrics = self
            .inner
            .raft
            .wait(Some(timeout))
            .metrics(|m| m.current_leader.is_some(), "a known leader")
            .await
            .map_err(|_| MetaError::Timeout)?;
        metrics.current_leader.ok_or(MetaError::Timeout)
    }

    /// Proposes a command and waits until it is committed and applied.
    /// Must be sent to the leader.
    ///
    /// On [`MetaError::NotLeader`], [`MetaError::Timeout`] or
    /// [`MetaError::Unavailable`] the outcome is unknown: the command may have
    /// been applied, or may still be. Retrying (on the leader) is safe, because
    /// every command is retry-safe; the retry may then report the first
    /// attempt's effect, for example [`ApplyError::NamespaceExists`] with the
    /// id the first attempt created.
    ///
    /// [`ApplyError::NamespaceExists`]: crate::ApplyError::NamespaceExists
    ///
    /// A leader refuses a command stamped more than
    /// [`MetaConfig::max_clock_skew`] ahead of its own clock with
    /// [`MetaError::ClockSkew`], before proposing it.
    pub async fn write(&self, command: Command) -> Result<Reply, MetaError> {
        self.check_clock(&command)?;
        let write = self.inner.raft.client_write(command);
        let result = tokio::time::timeout(self.inner.request_timeout, write)
            .await
            .map_err(|_| MetaError::Timeout)?;
        match result {
            Ok(response) => match response.data {
                Some(reply) => Ok(reply?),
                None => Err(unavailable("a command entry produced no reply")),
            },
            // openraft also answers a write it already proposed this way, when
            // this node loses leadership (or purges the entry's log range after
            // installing a snapshot) before the reply: the outcome is unknown.
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(forward))) => {
                Err(MetaError::NotLeader {
                    leader: forward.leader_id,
                })
            }
            Err(e) => Err(unavailable(e)),
        }
    }

    /// Refuses a command whose time stamp is too far ahead of this node's
    /// clock, if this node is the leader (a follower answers `NotLeader`
    /// anyway, and the leader checks). The check runs before proposing, so
    /// `MetaState::apply` stays deterministic.
    fn check_clock(&self, command: &Command) -> Result<(), MetaError> {
        let stamped_ms = match command {
            Command::AcquireLease { now_ms, .. }
            | Command::RenewLease { now_ms, .. }
            | Command::SwapSegment { now_ms, .. }
            | Command::TrimPartition { now_ms, .. }
            | Command::PruneWalCommits { now_ms } => *now_ms,
            Command::CommitWal { created_at_ms, .. } => *created_at_ms,
            _ => return Ok(()),
        };
        let leader = self.inner.raft.metrics().borrow_watched().current_leader;
        if leader != Some(self.inner.id) {
            return Ok(());
        }
        let leader_ms = self.inner.clock.now_ms();
        if stamped_ms > leader_ms.saturating_add(millis(self.inner.max_clock_skew)) {
            return Err(MetaError::ClockSkew {
                stamped_ms,
                leader_ms,
            });
        }
        Ok(())
    }

    /// Runs `f` against the state machine at the requested consistency.
    pub async fn read<T>(
        &self,
        consistency: Consistency,
        f: impl FnOnce(&MetaState) -> T,
    ) -> Result<T, MetaError> {
        if consistency == Consistency::Linearizable {
            let confirm = self.inner.raft.ensure_linearizable(ReadPolicy::ReadIndex);
            let result = tokio::time::timeout(self.inner.request_timeout, confirm)
                .await
                .map_err(|_| MetaError::Timeout)?;
            match result {
                Ok(_) => {}
                Err(RaftError::APIError(LinearizableReadError::ForwardToLeader(forward))) => {
                    return Err(MetaError::NotLeader {
                        leader: forward.leader_id,
                    });
                }
                Err(e) => return Err(unavailable(e)),
            }
        }
        Ok(self.inner.state.read(f))
    }

    pub async fn create_namespace(&self, name: &str) -> Result<NamespaceId, MetaError> {
        let command = Command::CreateNamespace {
            name: name.to_string(),
        };
        match self.write(command).await? {
            Reply::NamespaceCreated(id) => Ok(id),
            other => Err(MetaError::UnexpectedReply(other)),
        }
    }

    pub async fn create_stream(
        &self,
        namespace: NamespaceId,
        name: &str,
        partitions: u32,
        class: WalClass,
    ) -> Result<StreamId, MetaError> {
        let command = Command::CreateStream {
            namespace,
            name: name.to_string(),
            partitions,
            class,
            retention: Retention::default(),
        };
        match self.write(command).await? {
            Reply::StreamCreated(id) => Ok(id),
            other => Err(MetaError::UnexpectedReply(other)),
        }
    }

    /// Commits a durable WAL object created at `created_at_ms`; returns each
    /// chunk's base offset.
    pub async fn commit_wal(
        &self,
        object: &str,
        created_at_ms: u64,
        chunks: Vec<WalChunk>,
    ) -> Result<Vec<u64>, MetaError> {
        let command = Command::CommitWal {
            object: object.to_string(),
            created_at_ms,
            chunks,
        };
        match self.write(command).await? {
            Reply::WalCommitted { base_offsets } => Ok(base_offsets),
            other => Err(MetaError::UnexpectedReply(other)),
        }
    }

    pub async fn acquire_lease(
        &self,
        key: &str,
        owner: &str,
        ttl: Duration,
    ) -> Result<LeaseGrant, MetaError> {
        let command = Command::AcquireLease {
            key: key.to_string(),
            owner: owner.to_string(),
            ttl_ms: millis(ttl),
            now_ms: self.inner.clock.now_ms(),
        };
        match self.write(command).await? {
            Reply::Lease(grant) => Ok(grant),
            other => Err(MetaError::UnexpectedReply(other)),
        }
    }

    pub async fn renew_lease(
        &self,
        key: &str,
        owner: &str,
        epoch: u64,
        ttl: Duration,
    ) -> Result<LeaseGrant, MetaError> {
        let command = Command::RenewLease {
            key: key.to_string(),
            owner: owner.to_string(),
            epoch,
            ttl_ms: millis(ttl),
            now_ms: self.inner.clock.now_ms(),
        };
        match self.write(command).await? {
            Reply::Lease(grant) => Ok(grant),
            other => Err(MetaError::UnexpectedReply(other)),
        }
    }

    pub async fn release_lease(&self, key: &str, owner: &str, epoch: u64) -> Result<(), MetaError> {
        let command = Command::ReleaseLease {
            key: key.to_string(),
            owner: owner.to_string(),
            epoch,
        };
        match self.write(command).await? {
            Reply::LeaseReleased => Ok(()),
            other => Err(MetaError::UnexpectedReply(other)),
        }
    }

    /// Compare-and-swap on a pointer; returns the new version.
    pub async fn cas_pointer(
        &self,
        namespace: NamespaceId,
        key: &str,
        expected: Option<u64>,
        value: &str,
        fence: Option<Fence>,
    ) -> Result<u64, MetaError> {
        let command = Command::CasPointer {
            namespace,
            key: key.to_string(),
            expected,
            value: value.to_string(),
            fence,
        };
        match self.write(command).await? {
            Reply::PointerSet { version } => Ok(version),
            other => Err(MetaError::UnexpectedReply(other)),
        }
    }

    /// Builds a snapshot of everything applied so far and waits until it is in
    /// object storage. Returns at once on a node that has applied nothing yet
    /// (it was never initialized): there is nothing to snapshot.
    pub async fn snapshot(&self) -> Result<(), MetaError> {
        let Some(target) = self.inner.state.last_applied_index() else {
            return Ok(());
        };
        let raft = &self.inner.raft;
        let deadline = Instant::now() + self.inner.request_timeout;
        loop {
            // openraft ignores the trigger while a build is in flight, and that
            // build may cover less than `target`; so keep re-triggering until a
            // snapshot covers it.
            raft.trigger().snapshot().await.map_err(unavailable)?;
            let remaining = deadline.saturating_duration_since(Instant::now());
            let covered = raft
                .wait(Some(remaining.min(SNAPSHOT_RETRIGGER)))
                .metrics(
                    |m| m.snapshot.is_some_and(|id| id.index >= target),
                    "a snapshot of everything applied",
                )
                .await;
            match covered {
                Ok(_) => return Ok(()),
                Err(WaitError::ShuttingDown) => return Err(unavailable(WaitError::ShuttingDown)),
                Err(WaitError::Timeout(..)) if Instant::now() >= deadline => {
                    return Err(MetaError::Timeout);
                }
                Err(WaitError::Timeout(..)) => {}
            }
        }
    }

    /// Stops Raft, disconnects the node, and waits until its local database is
    /// closed. The data stays on disk, so [`MetaNode::start`] with the same
    /// config resumes the node.
    pub async fn shutdown(&self) -> Result<(), MetaError> {
        // First, so a snapshot upload being retried cannot hold up Raft's
        // shutdown or keep the local database open.
        self.inner.snapshot_io.close();
        self.inner.router.unregister(self.inner.id);
        self.inner.raft.shutdown().await.map_err(unavailable)?;
        // openraft's state machine and snapshot tasks may still hold storage
        // handles for a moment after the core stops.
        let deadline = Instant::now() + self.inner.request_timeout;
        while self.inner.db.strong_count() > 0 {
            if Instant::now() >= deadline {
                return Err(unavailable("local database still in use after shutdown"));
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok(())
    }
}
