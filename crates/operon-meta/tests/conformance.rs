//! The backend-agnostic `MetaStore` conformance suite
//! (`operon-meta-conformance`) against the openraft implementation: one node,
//! and three nodes in one process over a `Router`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use operon_common::meta::{Consistency, MetaStore};
use operon_meta::{MetaClient, MetaClientConfig, MetaConfig, MetaNode, Router, SystemClock};
use operon_meta_conformance::{Backend, Faults, Instance};
use operon_store::Store;
use tempfile::TempDir;

const WAIT: Duration = Duration::from_secs(20);

/// Keeps a backend's nodes and their directories for one case.
struct Nodes {
    _router: Router,
    _nodes: Vec<MetaNode>,
    _dirs: Vec<TempDir>,
}

/// Starts `count` nodes over one `Router`, initializes them as one cluster,
/// and waits until every node knows the leader; returns one client per node
/// (its local node first, the others as peers).
async fn start(count: u64) -> (Router, Vec<MetaNode>, Vec<TempDir>, Vec<MetaClient>) {
    let router = Router::new();
    let store = Store::in_memory();
    let dirs: Vec<TempDir> = (0..count)
        .map(|_| TempDir::new().expect("temp dir"))
        .collect();
    let mut nodes = Vec::new();
    for (id, dir) in (1..=count).zip(&dirs) {
        let config = MetaConfig::new(id, dir.path(), store.clone());
        nodes.push(MetaNode::start(config, &router).await.expect("start node"));
    }
    nodes[0]
        .initialize((1..=count).collect::<Vec<_>>())
        .await
        .expect("initialize");
    for node in &nodes {
        node.wait_for_leader(WAIT).await.expect("a leader");
    }
    let clients: Vec<MetaClient> = nodes
        .iter()
        .map(|local| {
            MetaClient::new(
                local.clone(),
                nodes.clone(),
                Arc::new(SystemClock),
                MetaClientConfig::default(),
            )
        })
        .collect();
    // One linearizable read per client, so each knows the leader. A client's
    // first write from a follower is redirected by a `NotLeader`, which the
    // client counts as an attempt of unknown outcome (`earlier_unknown`, and
    // a `CollectionExists` taken as its own); the cases assert what a write
    // with no earlier attempt reports.
    for client in &clients {
        MetaStore::clock_ms(client, Consistency::Linearizable)
            .await
            .expect("find the leader");
    }
    (router, nodes, dirs, clients)
}

fn instance(
    router: Router,
    nodes: Vec<MetaNode>,
    dirs: Vec<TempDir>,
    clients: &[MetaClient],
    faults: Arc<dyn Faults>,
) -> Instance {
    Instance {
        clients: clients.iter().map(Arc::<dyn MetaStore>::from).collect(),
        faults: Some(faults),
        guard: Box::new(Nodes {
            _router: router,
            _nodes: nodes,
            _dirs: dirs,
        }),
    }
}

/// One `MetaNode` in a temporary directory over `Store::in_memory()`, and
/// one `MetaClient`.
struct SingleNode;

/// Lost acknowledgements through `inject_lost_ack`; nothing to disturb.
struct SingleFaults {
    clients: Vec<MetaClient>,
}

#[async_trait]
impl Faults for SingleFaults {
    fn lose_next_ack(&self, client: usize) {
        self.clients[client].inject_lost_ack();
    }

    async fn disturb(&self, _seed: u64) {}

    async fn heal(&self) {}
}

#[async_trait]
impl Backend for SingleNode {
    async fn start(&self) -> Instance {
        let (router, nodes, dirs, clients) = start(1).await;
        let faults = Arc::new(SingleFaults {
            clients: clients.clone(),
        });
        instance(router, nodes, dirs, &clients, faults)
    }
}

/// Three nodes over a `Router`, as in `tests/cluster.rs`, and one client per
/// node.
struct ThreeNode;

/// Lost acknowledgements through `inject_lost_ack`; `disturb` isolates the
/// current leader, `heal` heals every node.
struct ClusterFaults {
    router: Router,
    nodes: Vec<MetaNode>,
    clients: Vec<MetaClient>,
}

#[async_trait]
impl Faults for ClusterFaults {
    fn lose_next_ack(&self, client: usize) {
        self.clients[client].inject_lost_ack();
    }

    async fn disturb(&self, seed: u64) {
        let deadline = Instant::now() + WAIT;
        loop {
            // Any node's view will do; start from a seeded one.
            let start = usize::try_from(seed % self.nodes.len() as u64).unwrap_or(0);
            for i in 0..self.nodes.len() {
                let node = &self.nodes[(start + i) % self.nodes.len()];
                if let Some(leader) = node.current_leader().await {
                    self.router.isolate(leader);
                    return;
                }
            }
            assert!(
                Instant::now() < deadline,
                "no node knows a leader to isolate"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn heal(&self) {
        for node in &self.nodes {
            self.router.heal(node.id());
        }
    }
}

#[async_trait]
impl Backend for ThreeNode {
    async fn start(&self) -> Instance {
        let (router, nodes, dirs, clients) = start(3).await;
        let faults = Arc::new(ClusterFaults {
            router: router.clone(),
            nodes: nodes.clone(),
            clients: clients.clone(),
        });
        instance(router, nodes, dirs, &clients, faults)
    }
}

mod single_node {
    operon_meta_conformance::metastore_conformance!(super::SingleNode);
}

mod three_node {
    operon_meta_conformance::metastore_conformance!(super::ThreeNode);
}
