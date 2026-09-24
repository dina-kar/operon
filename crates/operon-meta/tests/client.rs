//! `MetaClient` against an in-process three-node cluster.

use std::sync::Arc;
use std::time::{Duration, Instant};

use operon_meta::{
    Consistency, MetaClient, MetaClientConfig, MetaConfig, MetaNode, MetaState, Router,
    SystemClock, WalChunk, WalClass,
};
use operon_store::Store;
use tempfile::TempDir;

const WAIT: Duration = Duration::from_secs(20);

struct Cluster {
    router: Router,
    _dirs: Vec<TempDir>,
    nodes: Vec<MetaNode>,
}

impl Cluster {
    async fn start() -> Self {
        let router = Router::new();
        let store = Store::in_memory();
        let dirs: Vec<TempDir> = (0..3).map(|_| TempDir::new().expect("temp dir")).collect();
        let mut nodes = Vec::new();
        for (id, dir) in (1..=3).zip(&dirs) {
            let config = MetaConfig::new(id, dir.path(), store.clone());
            nodes.push(MetaNode::start(config, &router).await.expect("start"));
        }
        nodes[0].initialize([1, 2, 3]).await.expect("initialize");
        let cluster = Self {
            router,
            _dirs: dirs,
            nodes,
        };
        cluster.leader().await;
        cluster
    }

    /// The node every node agrees is the leader.
    async fn leader(&self) -> MetaNode {
        let deadline = Instant::now() + WAIT;
        loop {
            let mut seen = Vec::new();
            for node in &self.nodes {
                seen.push(node.current_leader().await);
            }
            if let Some(Some(leader)) = seen.first()
                && seen.iter().all(|s| *s == Some(*leader))
            {
                return self.node(*leader).clone();
            }
            assert!(Instant::now() < deadline, "no agreed leader: {seen:?}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn node(&self, id: u64) -> &MetaNode {
        self.nodes
            .iter()
            .find(|n| n.id() == id)
            .expect("node exists")
    }

    /// A client whose local node is `local`, with the other nodes as peers.
    fn client(&self, local: u64, config: MetaClientConfig) -> MetaClient {
        let peers = self.nodes.iter().filter(|n| n.id() != local).cloned();
        MetaClient::new(
            self.node(local).clone(),
            peers.collect(),
            Arc::new(SystemClock),
            config,
        )
    }

    async fn shutdown(self) {
        for node in &self.nodes {
            node.shutdown().await.expect("shutdown");
        }
    }
}

async fn eventually(node: &MetaNode, check: impl Fn(&MetaState) -> bool) {
    let deadline = Instant::now() + WAIT;
    while !node.read(Consistency::Local, &check).await.expect("read") {
        assert!(
            Instant::now() < deadline,
            "node {} never converged",
            node.id()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn writes_through_a_followers_client_reach_the_leader() {
    let cluster = Cluster::start().await;
    let leader = cluster.leader().await.id();
    let follower = (1..=3).find(|id| *id != leader).unwrap();
    let client = cluster.client(follower, MetaClientConfig::default());

    let ns = client.create_namespace("acme").await.unwrap();
    let stream = client
        .create_stream(ns, "events", 1, WalClass::Standard)
        .await
        .unwrap();
    assert!(stream.0 >= 1);
    // A linearizable read through the follower's client is served by the leader.
    let names = client
        .read(Consistency::Linearizable, |s| s.namespaces().count())
        .await
        .unwrap();
    assert_eq!(names, 1);
    eventually(client.local(), move |s| s.namespace(ns).is_some()).await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_write_during_leader_isolation_succeeds_after_re_election() {
    let cluster = Cluster::start().await;
    let old = cluster.leader().await.id();
    let config = MetaClientConfig::default();
    // The worst case: the client's local node is the leader being cut off.
    let client = cluster.client(old, config);
    client.create_namespace("before").await.unwrap();

    cluster.router.isolate(old);
    let started = Instant::now();
    let ns = client.create_namespace("during").await.unwrap();
    assert!(
        started.elapsed() < config.retry_deadline + Duration::from_secs(5),
        "took {:?}",
        started.elapsed()
    );
    cluster.router.heal(old);

    for node in &cluster.nodes {
        eventually(node, move |s| {
            s.namespace(ns).is_some_and(|n| n.name == "during") && s.namespaces().count() == 2
        })
        .await;
    }
    cluster.shutdown().await;
}

#[tokio::test]
async fn watch_applied_advances_after_a_commit() {
    let cluster = Cluster::start().await;
    let leader = cluster.leader().await.id();
    let follower = (1..=3).find(|id| *id != leader).unwrap();
    let client = cluster.client(follower, MetaClientConfig::default());
    let ns = client.create_namespace("acme").await.unwrap();
    let stream = client
        .create_stream(ns, "events", 1, WalClass::Standard)
        .await
        .unwrap();
    eventually(client.local(), move |s| s.stream(stream).is_some()).await;

    let mut applied = client.watch_applied();
    let before = *applied.borrow_and_update();
    let chunk = WalChunk {
        stream,
        partition: 0,
        records: 3,
        byte_range: 0..10,
        max_timestamp_ms: 0,
    };
    client
        .commit_wal("wal/1.wal", client.now_ms(), vec![chunk])
        .await
        .unwrap();
    tokio::time::timeout(WAIT, async {
        loop {
            applied.changed().await.expect("watch open");
            let committed = client
                .read(Consistency::Local, |s| {
                    s.partition(stream, 0).map(|p| p.high_watermark())
                })
                .await
                .unwrap();
            if committed == Some(3) {
                break;
            }
        }
    })
    .await
    .expect("the commit reached the follower's watch");
    assert!(*applied.borrow() > before);
    cluster.shutdown().await;
}

#[tokio::test]
async fn a_lost_acknowledgement_is_retried_and_deduplicated() {
    let cluster = Cluster::start().await;
    let leader = cluster.leader().await.id();
    let client = cluster.client(leader, MetaClientConfig::default());
    let ns = client.create_namespace("acme").await.unwrap();
    let stream = client
        .create_stream(ns, "events", 1, WalClass::Standard)
        .await
        .unwrap();
    let chunk = WalChunk {
        stream,
        partition: 0,
        records: 4,
        byte_range: 0..10,
        max_timestamp_ms: 0,
    };

    client.inject_lost_ack();
    let offsets = client
        .commit_wal("wal/1.wal", client.now_ms(), vec![chunk])
        .await
        .unwrap();
    assert_eq!(offsets, [0]);
    let next = client
        .read(Consistency::Linearizable, |s| {
            s.partition(stream, 0).map(|p| p.next_offset())
        })
        .await
        .unwrap();
    assert_eq!(next, Some(4));
    cluster.shutdown().await;
}
