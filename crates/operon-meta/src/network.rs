//! In-process transport between meta nodes.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use openraft::BasicNode;
use openraft::error::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::type_config::alias::VoteOf;

use crate::raft::{NodeId, Snapshot, SnapshotData, TypeConfig};
use crate::state_machine::StateMachineStore;

pub(crate) type MetaRaft = openraft::Raft<TypeConfig, StateMachineStore>;

/// Connects the meta nodes of one process to each other: `operon dev`, tests,
/// and the simulation harness. RPCs are direct calls into the target node.
///
/// [`Router::isolate`] cuts a node off from every other node in both
/// directions, to simulate a network partition.
#[derive(Clone, Default)]
pub struct Router {
    inner: Arc<Mutex<RouterState>>,
}

#[derive(Default)]
struct RouterState {
    nodes: BTreeMap<NodeId, MetaRaft>,
    isolated: BTreeSet<NodeId>,
}

#[derive(Debug, thiserror::Error)]
#[error("node {0} is unreachable")]
struct NodeUnreachable(NodeId);

impl Router {
    pub fn new() -> Self {
        Self::default()
    }

    /// Drops all traffic to and from `node` until [`Router::heal`] is called.
    pub fn isolate(&self, node: NodeId) {
        self.state().isolated.insert(node);
    }

    /// Reconnects an isolated node.
    pub fn heal(&self, node: NodeId) {
        self.state().isolated.remove(&node);
    }

    pub(crate) fn register(&self, node: NodeId, raft: MetaRaft) {
        self.state().nodes.insert(node, raft);
    }

    pub(crate) fn unregister(&self, node: NodeId) {
        self.state().nodes.remove(&node);
    }

    fn state(&self) -> MutexGuard<'_, RouterState> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The target's Raft handle, if both ends are connected.
    fn route(&self, from: NodeId, to: NodeId) -> Result<MetaRaft, Unreachable<TypeConfig>> {
        let state = self.state();
        let isolated = state.isolated.contains(&from) || state.isolated.contains(&to);
        match state.nodes.get(&to) {
            Some(raft) if !isolated => Ok(raft.clone()),
            _ => Err(Unreachable::new(&NodeUnreachable(to))),
        }
    }
}

impl fmt::Debug for Router {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state();
        f.debug_struct("Router")
            .field("nodes", &state.nodes.keys().collect::<Vec<_>>())
            .field("isolated", &state.isolated)
            .finish()
    }
}

/// Creates connections from one node to its peers.
pub(crate) struct NetworkFactory {
    router: Router,
    from: NodeId,
}

impl NetworkFactory {
    pub(crate) fn new(router: Router, from: NodeId) -> Self {
        Self { router, from }
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = Connection;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Connection {
        Connection {
            router: self.router.clone(),
            from: self.from,
            to: target,
        }
    }
}

pub(crate) struct Connection {
    router: Router,
    from: NodeId,
    to: NodeId,
}

fn unreachable<E: std::error::Error + 'static>(err: E) -> Unreachable<TypeConfig> {
    Unreachable::new(&err)
}

impl RaftNetworkV2<TypeConfig> for Connection {
    type SnapshotData = SnapshotData;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        let raft = self.router.route(self.from, self.to)?;
        raft.append_entries(rpc)
            .await
            .map_err(|e| RPCError::Unreachable(unreachable(e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let raft = self.router.route(self.from, self.to)?;
        raft.vote(rpc)
            .await
            .map_err(|e| RPCError::Unreachable(unreachable(e)))
    }

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let raft = self.router.route(self.from, self.to)?;
        raft.pre_vote(rpc)
            .await
            .map_err(|e| RPCError::Unreachable(unreachable(e)))
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        snapshot: Snapshot,
        _cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let raft = self.router.route(self.from, self.to)?;
        raft.install_full_snapshot(vote, snapshot)
            .await
            .map_err(|e| StreamingError::Unreachable(unreachable(e)))
    }
}
