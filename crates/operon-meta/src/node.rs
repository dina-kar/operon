//! A running meta node: Raft, local storage and the typed client API.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use openraft::async_runtime::WatchReceiver;
use openraft::error::{
    ChangeMembershipError, ClientWriteError, InitializeError, LinearizableReadError, RaftError,
};
use openraft::metrics::WaitError;
use openraft::{BasicNode, ChangeMembers, Raft, ReadPolicy, SnapshotPolicy};
use operon_common::meta::{
    ApplyError, Consistency, Fence, LeaseGrant, MetaError, Retention, WalChunk, WalClass,
};
use operon_common::{NamespaceId, StreamId};
use operon_store::Store;

use crate::clock::{Clock, SystemClock};
use crate::command::{Command, Reply};
use crate::db::LocalDb;
use crate::log_store::{LogStore, VOTE_KEY};
use crate::network::{MetaRaft, NetworkFactory, Router};
use crate::raft::NodeId;
use crate::state::MetaState;
use crate::state_machine::{
    SNAPSHOT_POINTER_KEY, SnapshotIoCloser, StateMachineStore, StateReader,
};
use crate::transport::{AddressBook, HttpNetworkFactory, Transport, watch_addresses};

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
    /// time caught up. Default 5 min: generous, because a leader whose own
    /// clock is behind by more than this refuses correct proposers (M0.3
    /// re-review N1; design §10 §2).
    pub max_clock_skew: Duration,
    /// The longest one snapshot upload may take, retries of transient store
    /// errors included (M0.2 re-review N2). openraft stops the node if a
    /// snapshot build fails, so it is generous. Default 60 s.
    pub snapshot_io_budget: Duration,
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
            max_clock_skew: Duration::from_secs(300),
            snapshot_io_budget: Duration::from_secs(60),
        }
    }
}

/// The nodes of the effective membership, with their addresses (empty for
/// an in-process node).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MembershipView {
    pub voters: BTreeMap<NodeId, String>,
    pub learners: BTreeMap<NodeId, String>,
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
    db_closed: tokio::sync::watch::Receiver<bool>,
    /// Lets `shutdown` cut short snapshot uploads that are being retried.
    snapshot_io: SnapshotIoCloser,
    /// The in-process router this node is registered with, if any.
    router: Option<Router>,
    /// Test hook ([`MetaNode::set_partitioned`]): while set, the HTTP routes
    /// answer 503 and outgoing HTTP RPCs are dropped.
    partitioned: Arc<AtomicBool>,
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
    // Only this node's own snapshots (M1.3 E47): a voter first started after
    // a peer snapshotted, or a new learner, has none, and a node that lost
    // its data directory still finds its own.
    let prefix = format!(
        "{}/{}/",
        config.snapshot_prefix.trim_end_matches('/'),
        config.node_id
    );
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
    /// already holds snapshots of this node, unless
    /// [`MetaConfig::allow_fresh_start_with_existing_snapshots`] is set.
    pub async fn start(config: MetaConfig, router: &Router) -> Result<Self, MetaError> {
        Self::start_with(config, Transport::InProcess(router.clone())).await
    }

    /// [`MetaNode::start`] over any [`Transport`]. Over HTTP, peers are
    /// reached at their addresses in the membership
    /// ([`MetaNode::initialize_with`], [`crate::rpc::join`]), and the caller
    /// serves [`crate::rpc::router`] on this node's address.
    pub async fn start_with(config: MetaConfig, transport: Transport) -> Result<Self, MetaError> {
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
        let db_closed = db.closed();
        let sm = StateMachineStore::open(
            config.node_id,
            config.store.clone(),
            config.snapshot_prefix.clone(),
            db.clone(),
        )
        .await?;
        sm.set_io_budget(config.snapshot_io_budget);
        let state = sm.reader();
        let snapshot_io = sm.closer();
        let partitioned = Arc::new(AtomicBool::new(false));
        let raft_config = Arc::new(raft_config);
        let log = LogStore::new(db);
        let (raft, router) = match transport {
            Transport::InProcess(router) => {
                let network = NetworkFactory::new(router.clone(), config.node_id);
                let raft = Raft::new(config.node_id, raft_config, network, log, sm).await;
                (raft, Some(router))
            }
            Transport::Http(transport) => {
                let addresses = AddressBook::default();
                let network = HttpNetworkFactory {
                    transport,
                    from: config.node_id,
                    addresses: addresses.clone(),
                    partitioned: partitioned.clone(),
                };
                let raft = Raft::new(config.node_id, raft_config, network, log, sm).await;
                if let Ok(raft) = &raft {
                    watch_addresses(raft, addresses);
                }
                (raft, None)
            }
        };
        let raft = raft.map_err(unavailable)?;
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
        if let Some(router) = &router {
            router.register(config.node_id, raft.clone());
        }

        Ok(Self {
            inner: Arc::new(Inner {
                id: config.node_id,
                raft,
                state,
                db_closed,
                snapshot_io,
                router,
                partitioned,
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
        let nodes: BTreeMap<NodeId, BasicNode> = members
            .into_iter()
            .map(|id| (id, BasicNode::default()))
            .collect();
        self.initialize_nodes(nodes).await
    }

    /// [`MetaNode::initialize`] with each member's `host:port`, which peers
    /// connect to over HTTP.
    pub async fn initialize_with(
        &self,
        members: BTreeMap<NodeId, String>,
    ) -> Result<(), MetaError> {
        let nodes = members
            .into_iter()
            .map(|(id, addr)| (id, BasicNode::new(addr)))
            .collect();
        self.initialize_nodes(nodes).await
    }

    async fn initialize_nodes(&self, nodes: BTreeMap<NodeId, BasicNode>) -> Result<(), MetaError> {
        let raft = &self.inner.raft;
        let members: BTreeSet<NodeId> = nodes.keys().copied().collect();
        if raft.is_initialized().await.map_err(unavailable)? {
            return self.check_voters(&members).await;
        }
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

    pub(crate) fn raft(&self) -> &MetaRaft {
        &self.inner.raft
    }

    /// Watches the index of the last log entry this node has applied (0 before
    /// any). It changes after every applied entry, once the entry's effect is
    /// visible to [`Consistency::Local`] reads, so a reader waiting for a
    /// state change subscribes, reads, and then waits for a change.
    pub fn watch_applied(&self) -> tokio::sync::watch::Receiver<u64> {
        self.inner.state.watch_applied()
    }

    /// Completes once this node's Raft has stopped, after a
    /// [`MetaNode::shutdown`] or a fatal error: nothing will be applied here
    /// any more. For the `MetaStore` change watch.
    pub(crate) async fn stopped(&self) {
        let mut metrics = self.inner.raft.metrics();
        loop {
            if metrics.borrow_watched().running_state.is_err() {
                return;
            }
            if metrics.changed().await.is_err() {
                return;
            }
        }
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

    /// The fatal error that stopped this node's Raft, if any (M0.2 review
    /// N4: a retrying client cannot tell a fatally failed leader from one
    /// that is merely unavailable). `None` while Raft runs, and after a
    /// clean [`MetaNode::shutdown`].
    pub fn fatal_error(&self) -> Option<String> {
        let metrics = self.inner.raft.metrics();
        let m = metrics.borrow_watched();
        match &m.running_state {
            Ok(()) => None,
            Err(openraft::error::Fatal::Stopped) => None,
            Err(fatal) => Some(fatal.to_string()),
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
    /// [`ApplyError::NamespaceExists`]: operon_common::meta::ApplyError::NamespaceExists
    ///
    /// A leader refuses a command stamped more than
    /// [`MetaConfig::max_clock_skew`] ahead of its own clock with
    /// [`MetaError::ClockSkew`], before proposing it.
    pub async fn write(&self, command: Command) -> Result<Reply, MetaError> {
        self.write_attempt(command)
            .await
            .map_err(|attempt| attempt.error)
    }

    /// [`MetaNode::write`], also saying whether the node refused the command
    /// before proposing it ([`AttemptError::refused`]).
    ///
    /// A node that does not believe it is the leader refuses with
    /// [`MetaError::NotLeader`] before proposing, so that attempt definitely
    /// did not apply. A `NotLeader` from openraft itself is not a refusal:
    /// openraft also answers a write it already proposed this way (when this
    /// node loses leadership, or purges the entry's log range after installing
    /// a snapshot, before the reply), so its outcome is unknown.
    pub(crate) async fn write_attempt(&self, command: Command) -> Result<Reply, AttemptError> {
        let (reply, _) = self.write_indexed_attempt(command).await?;
        Ok(reply.map_err(MetaError::from)?)
    }

    /// Proposes a command, waits until it is applied, and returns its result
    /// with its Raft log index. A rejected command took a log index too, so
    /// it is `Ok((Err(..), index))`. Errors as [`MetaNode::write`].
    pub async fn write_indexed(
        &self,
        command: Command,
    ) -> Result<(Result<Reply, ApplyError>, u64), MetaError> {
        self.write_indexed_attempt(command)
            .await
            .map_err(|attempt| attempt.error)
    }

    /// [`MetaNode::write_indexed`], also saying whether the node refused the
    /// command before proposing it ([`MetaNode::write_attempt`]).
    pub(crate) async fn write_indexed_attempt(
        &self,
        command: Command,
    ) -> Result<(Result<Reply, ApplyError>, u64), AttemptError> {
        let leader = self.inner.raft.metrics().borrow_watched().current_leader;
        if leader != Some(self.inner.id) {
            return Err(AttemptError::before_proposal(MetaError::NotLeader {
                leader,
            }));
        }
        self.check_clock(&command)
            .map_err(AttemptError::before_proposal)?;
        let write = self.inner.raft.client_write(command);
        let result = tokio::time::timeout(self.inner.request_timeout, write)
            .await
            .map_err(|_| MetaError::Timeout)?;
        match result {
            Ok(response) => match response.data {
                Some(reply) => Ok((reply, response.log_id.index)),
                None => Err(unavailable("a command entry produced no reply").into()),
            },
            // Possibly after proposing (see above): the outcome is unknown.
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(forward))) => {
                Err(MetaError::NotLeader {
                    leader: forward.leader_id,
                }
                .into())
            }
            Err(e) => Err(unavailable(e).into()),
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
            | Command::ReacquireLease { now_ms, .. }
            | Command::SwapSegment { now_ms, .. }
            | Command::TrimPartition { now_ms, .. }
            | Command::PruneWalCommits { now_ms, .. }
            | Command::DropCollection { now_ms, .. } => *now_ms,
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

    /// The index a linearizable read must have applied, from this node as
    /// the leader (`ensure_linearizable(ReadIndex)`); [`MetaError::NotLeader`]
    /// on any other node.
    pub async fn read_index(&self) -> Result<u64, MetaError> {
        let leader = self.inner.raft.metrics().borrow_watched().current_leader;
        if leader != Some(self.inner.id) {
            return Err(MetaError::NotLeader { leader });
        }
        let confirm = self.inner.raft.ensure_linearizable(ReadPolicy::ReadIndex);
        let result = tokio::time::timeout(self.inner.request_timeout, confirm)
            .await
            .map_err(|_| MetaError::Timeout)?;
        match result {
            Ok(read) => Ok(read.log_id().index),
            Err(RaftError::APIError(LinearizableReadError::ForwardToLeader(forward))) => {
                Err(MetaError::NotLeader {
                    leader: forward.leader_id,
                })
            }
            Err(e) => Err(unavailable(e)),
        }
    }

    /// The leader this node knows of, with its address in the membership
    /// (`None` when it has none, as in process).
    pub fn leader(&self) -> Option<(NodeId, Option<String>)> {
        let metrics = self.inner.raft.metrics();
        let m = metrics.borrow_watched();
        let leader = m.current_leader?;
        let addr = m
            .membership_config
            .get_node(&leader)
            .map(|node| node.addr.clone())
            .filter(|addr| !addr.is_empty());
        Some((leader, addr))
    }

    /// The effective membership, as of this node's latest metrics.
    pub fn membership(&self) -> MembershipView {
        let metrics = self.inner.raft.metrics();
        let m = metrics.borrow_watched();
        let membership = m.membership_config.membership();
        let addr = |id: &NodeId| {
            membership
                .get_node(id)
                .map(|node| node.addr.clone())
                .unwrap_or_default()
        };
        MembershipView {
            voters: membership.voter_ids().map(|id| (id, addr(&id))).collect(),
            learners: membership.learner_ids().map(|id| (id, addr(&id))).collect(),
        }
    }

    /// Whether this node believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.inner.raft.metrics().borrow_watched().current_leader == Some(self.inner.id)
    }

    /// Waits until this replica applied `index`; false on timeout.
    pub async fn wait_applied(&self, index: u64, timeout: Duration) -> bool {
        let mut applied = self.inner.state.watch_applied();
        tokio::time::timeout(timeout, applied.wait_for(|applied| *applied >= index))
            .await
            .is_ok_and(|seen| seen.is_ok())
    }

    /// Whether Raft still runs here (not shut down, no fatal error).
    pub(crate) fn is_running(&self) -> bool {
        self.inner
            .raft
            .metrics()
            .borrow_watched()
            .running_state
            .is_ok()
    }

    /// For tests: cuts this node off over HTTP while `partitioned` is true.
    /// Its [`crate::rpc::router`] answers every request with 503, and its
    /// outgoing Raft RPCs fail as unreachable. In-process nodes use
    /// [`Router::isolate`] instead. Only with the `test-util` feature.
    #[cfg(feature = "test-util")]
    pub fn set_partitioned(&self, partitioned: bool) {
        self.inner.partitioned.store(partitioned, Ordering::SeqCst);
    }

    pub(crate) fn is_partitioned(&self) -> bool {
        self.inner.partitioned.load(Ordering::SeqCst)
    }

    /// Adds `node_id` at `addr` as a learner, or updates its address
    /// (M1.3 Task 9 rule 5). Leader only; true if the membership changed.
    /// Adding a learner blocks until it has caught up.
    pub(crate) async fn join_member(
        &self,
        node_id: NodeId,
        addr: &str,
    ) -> Result<MembershipChange, MetaError> {
        if !self.is_leader() {
            return Ok(MembershipChange::NotLeader);
        }
        let current = self.membership();
        let known = current
            .voters
            .get(&node_id)
            .or_else(|| current.learners.get(&node_id));
        let result = match known {
            Some(known) if known == addr => return Ok(MembershipChange::Done { changed: false }),
            Some(_) => {
                let nodes = BTreeMap::from([(node_id, BasicNode::new(addr))]);
                self.inner
                    .raft
                    .change_membership(ChangeMembers::SetNodes(nodes), true)
                    .await
            }
            None => {
                self.inner
                    .raft
                    .add_learner(node_id, BasicNode::new(addr), true)
                    .await
            }
        };
        membership_result(result)
    }

    /// Removes the learner `node_id`; refuses a voter (voters change in M2).
    /// Leader only; true if the membership changed.
    pub(crate) async fn leave_member(
        &self,
        node_id: NodeId,
    ) -> Result<MembershipChange, MetaError> {
        if !self.is_leader() {
            return Ok(MembershipChange::NotLeader);
        }
        let current = self.membership();
        if current.voters.contains_key(&node_id) {
            return Ok(MembershipChange::Refused(
                "voters are changed in M2".to_string(),
            ));
        }
        if !current.learners.contains_key(&node_id) {
            return Ok(MembershipChange::Done { changed: false });
        }
        let result = self
            .inner
            .raft
            .change_membership(ChangeMembers::RemoveNodes(BTreeSet::from([node_id])), false)
            .await;
        membership_result(result)
    }

    pub async fn create_namespace(&self, name: &str) -> Result<NamespaceId, MetaError> {
        let command = Command::CreateNamespace {
            name: name.to_string(),
        };
        match self.write(command).await? {
            Reply::NamespaceCreated(id) => Ok(id),
            other => Err(MetaError::UnexpectedReply(format!("{other:?}"))),
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
            other => Err(MetaError::UnexpectedReply(format!("{other:?}"))),
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
            other => Err(MetaError::UnexpectedReply(format!("{other:?}"))),
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
            other => Err(MetaError::UnexpectedReply(format!("{other:?}"))),
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
            other => Err(MetaError::UnexpectedReply(format!("{other:?}"))),
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
            other => Err(MetaError::UnexpectedReply(format!("{other:?}"))),
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
            fresh: None,
        };
        match self.write(command).await? {
            Reply::PointerSet { version } => Ok(version),
            other => Err(MetaError::UnexpectedReply(format!("{other:?}"))),
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
        if let Some(router) = &self.inner.router {
            router.unregister(self.inner.id);
        }
        self.inner.raft.shutdown().await.map_err(unavailable)?;
        // openraft's state machine and snapshot tasks may still hold storage
        // handles for a moment after the core stops.
        // Waits for the file to be closed, not just for the last handle to go
        // (M0.3 re-review N4), so a restart in the same process can open it.
        let mut closed = self.inner.db_closed.clone();
        match tokio::time::timeout(self.inner.request_timeout, closed.wait_for(|c| *c)).await {
            // A dropped sender also means the database is closed.
            Ok(_) => Ok(()),
            Err(_) => Err(unavailable("local database still in use after shutdown")),
        }
    }
}

/// A failed write attempt on one node ([`MetaNode::write_attempt`]).
#[derive(Debug)]
pub(crate) struct AttemptError {
    pub(crate) error: MetaError,
    /// The node refused the command before proposing it, so the attempt
    /// definitely did not apply. When false, a retryable error leaves the
    /// attempt's outcome unknown.
    pub(crate) refused: bool,
}

impl AttemptError {
    pub(crate) fn before_proposal(error: MetaError) -> Self {
        Self {
            error,
            refused: true,
        }
    }
}

impl From<MetaError> for AttemptError {
    fn from(error: MetaError) -> Self {
        Self {
            error,
            refused: false,
        }
    }
}

/// What a membership request did on the leader.
#[derive(Debug)]
pub(crate) enum MembershipChange {
    Done { changed: bool },
    NotLeader,
    Refused(String),
}

fn membership_result<T>(
    result: Result<
        T,
        RaftError<crate::raft::TypeConfig, ClientWriteError<crate::raft::TypeConfig>>,
    >,
) -> Result<MembershipChange, MetaError> {
    match result {
        Ok(_) => Ok(MembershipChange::Done { changed: true }),
        Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_))) => {
            Ok(MembershipChange::NotLeader)
        }
        // Another change is still being committed: retry later.
        Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(
            e @ ChangeMembershipError::InProgress(_),
        ))) => Err(unavailable(e)),
        Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(e))) => {
            Ok(MembershipChange::Refused(e.to_string()))
        }
        Err(e) => Err(unavailable(e)),
    }
}
