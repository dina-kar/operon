//! Three meta nodes in one process, connected by a `Router`.

use std::collections::BTreeMap;
use std::future::Future;
use std::time::{Duration, Instant};

use operon_common::NamespaceId;
use operon_meta::{ApplyError, Consistency, MetaConfig, MetaError, MetaNode, MetaState, Router};
use operon_store::Store;
use tempfile::TempDir;

const WAIT: Duration = Duration::from_secs(20);

struct Cluster {
    router: Router,
    store: Store,
    dirs: BTreeMap<u64, TempDir>,
    nodes: BTreeMap<u64, MetaNode>,
    snapshot_every: u64,
}

impl Cluster {
    /// Starts nodes 1, 2 and 3 and initializes them as one cluster.
    async fn start(snapshot_every: u64) -> Self {
        let mut cluster = Cluster {
            router: Router::new(),
            store: Store::in_memory(),
            dirs: BTreeMap::new(),
            nodes: BTreeMap::new(),
            snapshot_every,
        };
        for id in 1..=3 {
            cluster
                .dirs
                .insert(id, TempDir::new().expect("create temp dir"));
            cluster.restart(id).await;
        }
        cluster.nodes[&1]
            .initialize([1, 2, 3])
            .await
            .expect("initialize");
        cluster
    }

    fn config(&self, id: u64) -> MetaConfig {
        let mut config = MetaConfig::new(id, self.dirs[&id].path(), self.store.clone());
        config.snapshot_every = self.snapshot_every;
        config.logs_after_snapshot = 0;
        // The default. A shorter timeout makes the tests fail under heavy
        // fsync contention, and a `Timeout` is never retried (see `on_leader_among`).
        config.request_timeout = Duration::from_secs(5);
        config
    }

    /// Starts (or restarts) node `id` from its data directory.
    async fn restart(&mut self, id: u64) {
        let node = MetaNode::start(self.config(id), &self.router)
            .await
            .expect("start node");
        self.nodes.insert(id, node);
    }

    async fn stop(&mut self, id: u64) {
        let node = self.nodes.remove(&id).expect("node is running");
        node.shutdown().await.expect("shutdown");
    }

    /// A running node that is leader and is recognized by `among`.
    async fn leader_among(&self, among: &[u64]) -> MetaNode {
        let deadline = Instant::now() + WAIT;
        loop {
            let mut seen = Vec::new();
            for id in among {
                seen.push(self.nodes[id].current_leader().await);
            }
            if let Some(Some(leader)) = seen.first()
                && seen.iter().all(|s| *s == Some(*leader))
                && among.contains(leader)
            {
                return self.nodes[leader].clone();
            }
            assert!(Instant::now() < deadline, "no agreed leader: {seen:?}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn leader(&self) -> MetaNode {
        let running: Vec<u64> = self.nodes.keys().copied().collect();
        self.leader_among(&running).await
    }

    /// Runs `op` on the leader, retrying while leadership is changing.
    async fn on_leader<T, F, Fut>(&self, op: F) -> T
    where
        F: Fn(MetaNode, bool) -> Fut,
        Fut: Future<Output = Result<T, MetaError>>,
    {
        let running: Vec<u64> = self.nodes.keys().copied().collect();
        self.on_leader_among(&running, op).await
    }

    /// Runs `op` on the leader recognized by `among`, retrying while leadership
    /// is changing; `op`'s second argument says whether this is a retry.
    ///
    /// `NotLeader` and `Unavailable` are retried. Neither says whether a write
    /// was applied (openraft may answer `NotLeader` for a write it already
    /// proposed), so a retried `op` must accept its first attempt's effect;
    /// every command is retry-safe, so it can. `Timeout` is never retried: in
    /// these tests it means a real stall.
    async fn on_leader_among<T, F, Fut>(&self, among: &[u64], op: F) -> T
    where
        F: Fn(MetaNode, bool) -> Fut,
        Fut: Future<Output = Result<T, MetaError>>,
    {
        let deadline = Instant::now() + WAIT;
        let mut retry = false;
        loop {
            match op(self.leader_among(among).await, retry).await {
                Ok(value) => return value,
                Err(MetaError::NotLeader { .. } | MetaError::Unavailable(_))
                    if Instant::now() < deadline =>
                {
                    retry = true;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(err) => panic!("leader request failed: {err:?}"),
            }
        }
    }

    /// Creates namespace `name` through the leader recognized by `among`. A
    /// retry may find the namespace created by an earlier attempt whose
    /// outcome was unknown; that counts as success.
    async fn create_namespace_among(&self, among: &[u64], name: &str) -> NamespaceId {
        self.on_leader_among(among, |n, retry| {
            let name = name.to_string();
            async move {
                match n.create_namespace(&name).await {
                    Err(MetaError::Rejected(ApplyError::NamespaceExists(id))) if retry => Ok(id),
                    other => other,
                }
            }
        })
        .await
    }

    async fn create_namespace(&self, name: &str) -> NamespaceId {
        let running: Vec<u64> = self.nodes.keys().copied().collect();
        self.create_namespace_among(&running, name).await
    }
}

/// Waits until `check` holds on node `id`'s local state.
async fn eventually(node: &MetaNode, check: impl Fn(&MetaState) -> bool) {
    let deadline = Instant::now() + WAIT;
    while !node
        .read(Consistency::Local, &check)
        .await
        .expect("local read")
    {
        assert!(
            Instant::now() < deadline,
            "node {} never converged",
            node.id()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn has_namespace(name: &'static str) -> impl Fn(&MetaState) -> bool {
    move |s| s.namespace_by_name(name).is_some()
}

#[tokio::test]
async fn writes_replicate_to_every_node() {
    let cluster = Cluster::start(10_000).await;
    let ns = cluster.create_namespace("acme").await;

    for node in cluster.nodes.values() {
        eventually(node, move |s| s.namespace(ns).is_some()).await;
    }
}

#[tokio::test]
async fn followers_redirect_writes_and_linearizable_reads_to_the_leader() {
    let cluster = Cluster::start(10_000).await;
    cluster.create_namespace("acme").await;
    let leader = cluster.leader().await.id();
    let follower = cluster.nodes.values().find(|n| n.id() != leader).unwrap();
    eventually(follower, has_namespace("acme")).await;

    let err = follower.create_namespace("globex").await.unwrap_err();
    assert!(
        matches!(err, MetaError::NotLeader { leader: Some(l) } if l == leader),
        "{err:?}"
    );
    let err = follower
        .read(Consistency::Linearizable, |s| s.namespaces().count())
        .await
        .unwrap_err();
    assert!(
        matches!(err, MetaError::NotLeader { leader: Some(l) } if l == leader),
        "{err:?}"
    );
    let local = follower
        .read(Consistency::Local, |s| s.namespaces().count())
        .await
        .unwrap();
    assert_eq!(local, 1);
}

#[tokio::test]
async fn a_new_leader_takes_over_when_the_leader_stops() {
    let mut cluster = Cluster::start(10_000).await;
    cluster.create_namespace("before").await;
    let old = cluster.leader().await.id();
    cluster.stop(old).await;

    let new = cluster.leader().await;
    assert_ne!(new.id(), old);
    // A brand-new leader may briefly fail to confirm its leadership with a
    // quorum (`Unavailable`), so the read goes through the retrying harness.
    let (id, names): (u64, Vec<String>) = cluster
        .on_leader(|n, _| async move {
            let names = n
                .read(Consistency::Linearizable, |s| {
                    s.namespaces().map(|n| n.name.clone()).collect()
                })
                .await?;
            Ok((n.id(), names))
        })
        .await;
    assert_ne!(id, old);
    assert_eq!(names, ["before"]);
    cluster.create_namespace("after").await;

    cluster.restart(old).await;
    eventually(&cluster.nodes[&old], has_namespace("after")).await;
}

#[tokio::test]
async fn a_leader_cut_off_from_the_quorum_cannot_acknowledge_anything() {
    let cluster = Cluster::start(10_000).await;
    cluster.create_namespace("before").await;
    let old = cluster.leader().await;
    cluster.router.isolate(old.id());

    // The isolated node still believes it leads, so it proposes the write and
    // waits for a quorum that never answers. (A `NotLeader` would mean nothing
    // was proposed, and the check after the heal would prove nothing.)
    let err = old.create_namespace("lost").await.unwrap_err();
    assert!(matches!(err, MetaError::Timeout), "{err:?}");
    let err = old
        .read(Consistency::Linearizable, |s| s.namespaces().count())
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            MetaError::Timeout | MetaError::NotLeader { .. } | MetaError::Unavailable(_)
        ),
        "{err:?}"
    );

    let majority: Vec<u64> = (1..=3).filter(|id| *id != old.id()).collect();
    cluster.create_namespace_among(&majority, "after").await;

    cluster.router.heal(old.id());
    eventually(&old, has_namespace("after")).await;
    for node in cluster.nodes.values() {
        eventually(node, has_namespace("after")).await;
        let lost = node
            .read(Consistency::Local, |s| {
                s.namespace_by_name("lost").is_some()
            })
            .await
            .unwrap();
        assert!(
            !lost,
            "node {} applied a write that was never acknowledged",
            node.id()
        );
    }
}

#[tokio::test]
async fn a_lagging_follower_catches_up_from_a_snapshot() {
    // No automatic snapshots: a snapshot under the follower's prefix can only
    // come from installing one sent by the leader.
    let mut cluster = Cluster::start(10_000).await;
    cluster.create_namespace("ns0").await;
    let leader = cluster.leader().await.id();
    let lagging = (1..=3).find(|id| *id != leader).unwrap();
    cluster.router.isolate(lagging);

    let majority: Vec<u64> = (1..=3).filter(|id| *id != lagging).collect();
    for i in 1..30 {
        cluster
            .create_namespace_among(&majority, &format!("ns{i}"))
            .await;
    }
    // Snapshot and purge the log on both connected nodes, so whichever of them
    // leads after the heal can only catch the follower up with a snapshot. A
    // leader defers purging while a replication request is in flight; more
    // writes give it more chances.
    for id in &majority {
        cluster.nodes[id].snapshot().await.unwrap();
    }
    let deadline = Instant::now() + WAIT;
    loop {
        let purged = majority.iter().all(|id| {
            let status = cluster.nodes[id].status();
            status.snapshot.is_some() && status.purged >= status.snapshot
        });
        if purged {
            break;
        }
        assert!(Instant::now() < deadline, "the log was never purged");
        let ttl = Duration::from_secs(60);
        cluster
            .on_leader_among(&majority, |n, _| async move {
                n.acquire_lease("filler", "test", ttl).await
            })
            .await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    cluster.router.heal(lagging);
    eventually(&cluster.nodes[&lagging], |s| s.namespaces().count() == 30).await;
    // The installed snapshot is persisted under the follower's own prefix
    // before its state is replaced.
    let own = cluster
        .store
        .list(&format!("meta/snapshots/{lagging}/"))
        .await
        .unwrap();
    assert_eq!(own.len(), 1, "installed snapshot not persisted: {own:?}");

    cluster.stop(lagging).await;
    cluster.restart(lagging).await;
    eventually(&cluster.nodes[&lagging], |s| s.namespaces().count() == 30).await;
}

#[tokio::test]
async fn a_node_restarts_while_the_rest_of_the_cluster_is_down() {
    let mut cluster = Cluster::start(10_000).await;
    let ns = cluster.create_namespace("acme").await;
    for node in cluster.nodes.values() {
        eventually(node, move |s| s.namespace(ns).is_some()).await;
    }
    for id in 1..=3 {
        cluster.stop(id).await;
    }

    // Starting must not wait for a quorum; the node serves its last state locally.
    cluster.restart(1).await;
    eventually(&cluster.nodes[&1], move |s| s.namespace(ns).is_some()).await;
    let err = cluster.nodes[&1]
        .create_namespace("globex")
        .await
        .unwrap_err();
    assert!(
        matches!(err, MetaError::NotLeader { .. } | MetaError::Timeout),
        "{err:?}"
    );

    cluster.restart(2).await;
    cluster.create_namespace_among(&[1, 2], "globex").await;
}

#[tokio::test]
async fn initializing_every_node_at_once_forms_one_cluster() {
    let router = Router::new();
    let store = Store::in_memory();
    let dirs: Vec<TempDir> = (0..3).map(|_| TempDir::new().unwrap()).collect();
    let mut nodes = Vec::new();
    for (id, dir) in (1..=3).zip(&dirs) {
        let config = MetaConfig::new(id, dir.path(), store.clone());
        nodes.push(MetaNode::start(config, &router).await.unwrap());
    }
    let inits = nodes.iter().map(|n| n.initialize([1, 2, 3]));
    for result in futures::future::join_all(inits).await {
        result.unwrap();
    }

    let deadline = Instant::now() + WAIT;
    let leader = loop {
        let mut seen = Vec::new();
        for node in &nodes {
            seen.push(node.current_leader().await);
        }
        if let Some(leader) = seen[0]
            && seen.iter().all(|s| *s == seen[0])
        {
            break leader;
        }
        assert!(Instant::now() < deadline, "no agreed leader: {seen:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    // One cluster, not three: a write through the agreed leader reaches every node.
    let leader = nodes.iter().find(|n| n.id() == leader).unwrap();
    let ns = leader.create_namespace("acme").await.unwrap();
    for node in &nodes {
        eventually(node, move |s| s.namespace(ns).is_some()).await;
    }
}
