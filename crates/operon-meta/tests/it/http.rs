//! The metastore over the network (M1.3 Task 9): openraft RPCs over HTTP,
//! the networked `MetaClient`, and cluster join and leave. Every node in the
//! test process gets its own axum server on `127.0.0.1:0`, bound before the
//! node starts so its address is known.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use openraft::entry::RaftEntry;
use openraft::error::{Fatal, RaftError};
use openraft::impls::leader_id_adv::LeaderId;
use openraft::raft::{AppendEntriesRequest, VoteRequest, VoteResponse};
use openraft::type_config::alias::{EntryOf, LogIdOf, SnapshotMetaOf};
use openraft::{
    BasicNode, ErrorSubject, ErrorVerb, LogId, Membership, StorageError, StoredMembership, Vote,
};
use operon_meta::rpc::{self, JoinRequest, LeaveRequest};
use operon_meta::{
    ApplyError, Command, Consistency, HttpTransport, HttpTransportConfig, MetaClient,
    MetaClientConfig, MetaConfig, MetaError, MetaNode, MetaState, SystemClock, Transport,
    TypeConfig,
};
use operon_store::Store;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub(crate) const WAIT: Duration = Duration::from_secs(20);

/// The transports' request timeout in these tests (default 10 s), so a
/// lagging learner answers within a few seconds.
pub(crate) const REQUEST_TIMEOUT: Duration = Duration::from_secs(3);

pub(crate) fn transport() -> HttpTransport {
    HttpTransport::new(HttpTransportConfig {
        request_timeout: REQUEST_TIMEOUT,
        ..HttpTransportConfig::default()
    })
    .expect("transport")
}

/// One node, its server, and a networked client over it with a transport of
/// its own.
pub(crate) struct Member {
    pub(crate) node: MetaNode,
    pub(crate) addr: String,
    pub(crate) client: MetaClient,
    pub(crate) client_transport: HttpTransport,
    server: JoinHandle<()>,
    dir: TempDir,
}

/// Nodes over HTTP sharing one snapshot store.
pub(crate) struct HttpCluster {
    pub(crate) store: Store,
    pub(crate) members: BTreeMap<u64, Member>,
    tune: fn(&mut MetaConfig),
}

impl HttpCluster {
    /// Nodes `1..=voters`, initialized on node 1 with their addresses, once
    /// every node knows the leader.
    pub(crate) async fn start(voters: u64) -> Self {
        Self::start_tuned(voters, |_| {}).await
    }

    pub(crate) async fn start_tuned(voters: u64, tune: fn(&mut MetaConfig)) -> Self {
        let mut cluster = Self {
            store: Store::in_memory(),
            members: BTreeMap::new(),
            tune,
        };
        let mut addrs = BTreeMap::new();
        for id in 1..=voters {
            let member = cluster.launch(id, false, None).await;
            addrs.insert(id, member.addr.clone());
            cluster.members.insert(id, member);
        }
        cluster.members[&1]
            .node
            .initialize_with(addrs)
            .await
            .expect("initialize");
        for member in cluster.members.values() {
            member.node.wait_for_leader(WAIT).await.expect("a leader");
        }
        cluster
    }

    /// Binds a listener, starts node `id` (in `dir`, or a fresh directory),
    /// and serves its routes.
    async fn launch(&self, id: u64, learner: bool, dir: Option<TempDir>) -> Member {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("address").to_string();
        let dir = dir.unwrap_or_else(|| TempDir::new().expect("temp dir"));
        let mut config = MetaConfig::new(id, dir.path(), self.store.clone());
        // A learner expects the leader's snapshot (Task 9 rule 7).
        config.allow_fresh_start_with_existing_snapshots = learner;
        (self.tune)(&mut config);
        let node = MetaNode::start_with(config, Transport::Http(transport()))
            .await
            .expect("start node");
        let router = rpc::router(node.clone());
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        let client_transport = transport();
        let client = MetaClient::networked(
            node.clone(),
            client_transport.clone(),
            Arc::new(SystemClock),
            MetaClientConfig::default(),
        );
        Member {
            node,
            addr,
            client,
            client_transport,
            server,
            dir,
        }
    }

    pub(crate) fn seeds(&self) -> Vec<String> {
        self.members.values().map(|m| m.addr.clone()).collect()
    }

    /// Starts node `id` and joins it as a learner; whether the membership
    /// changed.
    pub(crate) async fn join_learner(&mut self, id: u64) -> bool {
        let member = self.launch(id, true, None).await;
        let changed = join(&self.seeds(), id, &member.addr).await;
        member
            .node
            .wait_for_leader(WAIT)
            .await
            .expect("the learner knows the leader");
        self.members.insert(id, member);
        changed
    }

    /// Stops node `id` and its server; returns its data directory.
    pub(crate) async fn stop(&mut self, id: u64) -> TempDir {
        let member = self.members.remove(&id).expect("running member");
        member.node.shutdown().await.expect("shutdown");
        member.server.abort();
        member.dir
    }

    /// The node every running voter recognizes as leader.
    pub(crate) async fn leader(&self) -> u64 {
        let deadline = Instant::now() + WAIT;
        loop {
            let voters = self.members[self.members.keys().next().expect("a member")]
                .node
                .membership()
                .voters;
            let views: Vec<Option<u64>> = self
                .members
                .iter()
                .filter(|(id, _)| voters.contains_key(id))
                .map(|(_, m)| m.node.leader().map(|(id, _)| id))
                .collect();
            if let Some(Some(leader)) = views.first()
                && views.iter().all(|v| *v == Some(*leader))
                && self.members.get(leader).is_some_and(|m| m.node.is_leader())
            {
                return *leader;
            }
            assert!(Instant::now() < deadline, "no agreed leader: {views:?}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// A voter that is not the leader.
    pub(crate) async fn follower(&self) -> u64 {
        let leader = self.leader().await;
        let voters = self.members[&leader].node.membership().voters;
        *voters
            .keys()
            .find(|id| **id != leader && self.members.contains_key(id))
            .expect("a follower")
    }

    /// Node `id`'s state once it applied everything the leader has.
    pub(crate) async fn caught_up_state(&self, id: u64) -> MetaState {
        let leader = &self.members[&self.leader().await].node;
        let index = leader.status().last_applied.unwrap_or(0);
        let node = &self.members[&id].node;
        assert!(node.wait_applied(index, WAIT).await, "node {id} lags");
        node.read(Consistency::Local, MetaState::clone)
            .await
            .expect("read")
    }
}

async fn join(seeds: &[String], id: u64, addr: &str) -> bool {
    let request = JoinRequest {
        node_id: id,
        addr: addr.to_string(),
    };
    rpc::join(&transport(), seeds, request, WAIT)
        .await
        .expect("join")
}

fn names(state: &MetaState) -> Vec<String> {
    state.namespaces().map(|n| n.name.clone()).collect()
}

fn log_id(term: u64, index: u64) -> LogIdOf<TypeConfig> {
    LogId::new(LeaderId { term, node_id: 1 }, index)
}

fn round_trip<T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug>(value: &T) {
    let bytes = postcard::to_allocvec(value).expect("encode");
    let back: T = postcard::from_bytes(&bytes).expect("decode");
    assert_eq!(format!("{back:?}"), format!("{value:?}"));
}

#[test]
fn postcard_round_trips_every_raft_message() {
    let commands = crate::encoding::golden_commands()
        .into_iter()
        .chain(crate::encoding::golden_commands_m1_3());
    let mut entries: Vec<EntryOf<TypeConfig>> = commands
        .enumerate()
        .map(|(i, command)| EntryOf::<TypeConfig>::new_normal(log_id(2, i as u64 + 1), command))
        .collect();
    let nodes = BTreeMap::from([
        (1, BasicNode::new("127.0.0.1:1")),
        (4, BasicNode::new("127.0.0.1:4")),
    ]);
    let membership =
        Membership::new(vec![std::collections::BTreeSet::from([1])], nodes).expect("membership");
    entries.push(EntryOf::<TypeConfig>::new_membership(
        log_id(2, 999),
        membership.clone(),
    ));
    entries.push(EntryOf::<TypeConfig>::new_blank(log_id(2, 1000)));
    let vote = Vote::new_committed(2, 1);
    round_trip(&AppendEntriesRequest::<TypeConfig> {
        vote,
        prev_log_id: Some(log_id(1, 0)),
        entries,
        leader_commit: Some(log_id(2, 3)),
    });
    round_trip(&VoteRequest::<TypeConfig>::new(vote, Some(log_id(2, 7))));
    round_trip(&VoteResponse::<TypeConfig>::new(
        vote,
        Some(log_id(2, 7)),
        true,
    ));
    round_trip(&SnapshotMetaOf::<TypeConfig> {
        last_log_id: Some(log_id(2, 9)),
        last_membership: StoredMembership::new(Some(log_id(2, 999)), membership),
    });
    let storage = StorageError::<TypeConfig>::from_io_error(
        ErrorSubject::Store,
        ErrorVerb::Read,
        std::io::Error::other("disk"),
    );
    let fatal = Fatal::<TypeConfig>::StorageError(storage);
    round_trip(&fatal);
    round_trip(&Fatal::<TypeConfig>::Stopped);
    round_trip(&RaftError::<TypeConfig>::Fatal(fatal));
    // The answers carry these inside a `Result`.
    let answer: Result<VoteResponse<TypeConfig>, RaftError<TypeConfig>> =
        Err(RaftError::Fatal(Fatal::Panicked));
    round_trip(&answer);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_three_node_cluster_over_http_elects_a_leader_and_replicates() {
    let cluster = HttpCluster::start(3).await;
    let leader = cluster.leader().await;
    let view = cluster.members[&leader].node.membership();
    assert_eq!(view.voters.len(), 3);
    assert!(view.voters.values().all(|addr| !addr.is_empty()));
    for (id, member) in &cluster.members {
        member
            .client
            .create_namespace(&format!("from-{id}"))
            .await
            .expect("write");
    }
    for id in cluster.members.keys() {
        let state = cluster.caught_up_state(*id).await;
        assert_eq!(names(&state), ["from-1", "from-2", "from-3"], "node {id}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_from_a_follower_are_forwarded_to_the_leader() {
    let cluster = HttpCluster::start(3).await;
    let leader = cluster.leader().await;
    let follower = cluster.follower().await;
    let member = &cluster.members[&follower];
    let id = member
        .client
        .create_namespace("forwarded")
        .await
        .expect("write");
    // The client waited until its own replica applied the write.
    let local = member
        .node
        .read(Consistency::Local, |s| {
            s.namespace_by_name("forwarded").map(|n| n.id)
        })
        .await
        .expect("read");
    assert_eq!(local, Some(id));
    let on_leader = cluster.members[&leader]
        .node
        .read(Consistency::Local, |s| {
            s.namespace_by_name("forwarded").map(|n| n.id)
        })
        .await
        .expect("read");
    assert_eq!(on_leader, Some(id));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_learner_joins_catches_up_and_reads_locally() {
    let mut cluster = HttpCluster::start(3).await;
    let leader = cluster.leader().await;
    for i in 0..200 {
        cluster.members[&leader]
            .client
            .create_namespace(&format!("n{i:03}"))
            .await
            .expect("write");
    }
    assert!(
        cluster.join_learner(4).await,
        "a new learner changes the membership"
    );
    let view = cluster.members[&leader].node.membership();
    assert_eq!(view.learners.keys().copied().collect::<Vec<_>>(), [4]);
    let expected = cluster.caught_up_state(leader).await;
    assert_eq!(cluster.caught_up_state(4).await, expected);
    assert_eq!(names(&expected).len(), 200);
    let addr = cluster.members[&4].addr.clone();
    assert!(
        !join(&cluster.seeds(), 4, &addr).await,
        "a second join is a no-op"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_learner_linearizable_read_sees_the_latest_write() {
    let mut cluster = HttpCluster::start(3).await;
    assert!(cluster.join_learner(4).await);
    for i in 0..100 {
        let name = format!("lin-{i:03}");
        let id = cluster.members[&1]
            .client
            .create_namespace(&name)
            .await
            .expect("write");
        let seen = cluster.members[&4]
            .client
            .read(Consistency::Linearizable, |s| {
                s.namespace_by_name(&name).map(|n| n.id)
            })
            .await
            .expect("linearizable read");
        assert_eq!(seen, Some(id), "iteration {i}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_learner_never_answers_linearizably_while_partitioned() {
    let mut cluster = HttpCluster::start(3).await;
    assert!(cluster.join_learner(4).await);
    cluster.members[&1]
        .client
        .create_namespace("before")
        .await
        .expect("write");
    cluster.caught_up_state(4).await;
    // The leader can no longer replicate to the learner.
    cluster.members[&4].node.set_partitioned(true);
    cluster.members[&1]
        .client
        .create_namespace("after")
        .await
        .expect("a write without the learner");
    let started = Instant::now();
    let read = cluster.members[&4]
        .client
        .read(Consistency::Linearizable, |s| {
            s.namespace_by_name("after").is_some()
        })
        .await;
    assert!(
        matches!(&read, Err(MetaError::Unavailable(msg)) if msg.contains("lagging")),
        "{read:?}"
    );
    assert!(started.elapsed() < REQUEST_TIMEOUT + Duration::from_secs(1));
    // Healed, it answers again, with the write.
    cluster.members[&4].node.set_partitioned(false);
    let read = cluster.members[&4]
        .client
        .read(Consistency::Linearizable, |s| {
            s.namespace_by_name("after").is_some()
        })
        .await
        .expect("read after healing");
    assert!(read);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forwarded_write_with_a_lost_response_applies_once() {
    let mut cluster = HttpCluster::start(3).await;
    assert!(cluster.join_learner(4).await);
    let learner = &cluster.members[&4];
    learner.client_transport.drop_next_response();
    let (result, earlier_unknown) = learner
        .client
        .write_tracked(Command::CreateNamespace {
            name: "x".to_string(),
        })
        .await;
    let state = cluster.caught_up_state(cluster.leader().await).await;
    let x: Vec<_> = state.namespaces().filter(|n| n.name == "x").collect();
    assert_eq!(x.len(), 1, "exactly one namespace x");
    match result {
        Err(MetaError::Rejected(ApplyError::NamespaceExists(id))) => assert_eq!(id, x[0].id),
        other => panic!("expected NamespaceExists, got {other:?}"),
    }
    assert!(earlier_unknown, "the lost response is an unknown outcome");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_is_installed_over_http() {
    let mut cluster = HttpCluster::start_tuned(3, |config| {
        config.snapshot_every = 20;
        config.logs_after_snapshot = 5;
    })
    .await;
    let leader = cluster.leader().await;
    for i in 0..100 {
        cluster.members[&leader]
            .client
            .create_namespace(&format!("s{i:03}"))
            .await
            .expect("write");
    }
    // The leader's log no longer starts at the beginning.
    let deadline = Instant::now() + WAIT;
    while cluster.members[&leader].node.status().purged < Some(50) {
        assert!(Instant::now() < deadline, "the leader never purged its log");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(cluster.join_learner(4).await);
    let expected = cluster.caught_up_state(leader).await;
    assert_eq!(cluster.caught_up_state(4).await, expected);
    let status = cluster.members[&4].node.status();
    assert!(
        status.snapshot >= Some(50),
        "the learner installed no snapshot: {status:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_updates_a_changed_address() {
    let mut cluster = HttpCluster::start(3).await;
    assert!(cluster.join_learner(4).await);
    let old = cluster.members[&4].addr.clone();
    // The learner restarts on another port with its data directory.
    let dir = cluster.stop(4).await;
    let member = cluster.launch(4, true, Some(dir)).await;
    let new = member.addr.clone();
    assert_ne!(old, new);
    cluster.members.insert(4, member);
    assert!(
        join(&cluster.seeds(), 4, &new).await,
        "a new address changes the membership"
    );
    let leader = cluster.leader().await;
    assert_eq!(
        cluster.members[&leader].node.membership().learners,
        BTreeMap::from([(4, new.clone())])
    );
    assert!(!join(&cluster.seeds(), 4, &new).await);
    // The leader replicates to the new address.
    cluster.members[&1]
        .client
        .create_namespace("moved")
        .await
        .expect("write");
    let state = cluster.caught_up_state(4).await;
    assert!(state.namespace_by_name("moved").is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leave_removes_a_learner_and_refuses_a_voter() {
    let mut cluster = HttpCluster::start(3).await;
    assert!(cluster.join_learner(4).await);
    let (transport, seeds) = (transport(), cluster.seeds());
    let leave = |id| rpc::leave(&transport, &seeds, LeaveRequest { node_id: id }, WAIT);
    assert!(leave(4).await.expect("leave"));
    let leader = cluster.leader().await;
    let view = cluster.members[&leader].node.membership();
    assert!(view.learners.is_empty(), "{view:?}");
    assert_eq!(view.voters.len(), 3);
    assert!(!leave(4).await.expect("leave again"));
    let refused = leave(2).await;
    assert!(
        matches!(&refused, Err(MetaError::Config(msg)) if msg.contains("M2")),
        "{refused:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_failover_over_http_keeps_writes_working() {
    let mut cluster = HttpCluster::start(3).await;
    let leader = cluster.leader().await;
    let follower = cluster.follower().await;
    cluster.members[&follower]
        .client
        .create_namespace("before")
        .await
        .expect("write");
    let _dir = cluster.stop(leader).await;
    let stopped = Instant::now();
    let deadline = stopped + Duration::from_secs(10);
    loop {
        match cluster.members[&follower]
            .client
            .create_namespace("after-failover")
            .await
        {
            // A retry after an attempt of unknown outcome may find it applied.
            Ok(_) | Err(MetaError::Rejected(ApplyError::NamespaceExists(_))) => break,
            Err(err) => assert!(Instant::now() < deadline, "no write within 10 s: {err}"),
        }
    }
    assert!(stopped.elapsed() < Duration::from_secs(10));
    assert_ne!(cluster.leader().await, leader);
}
