//! M1.2a's `MetaStore` conformance suite over HTTP (M1.3 Task 9 rule 8): a
//! fresh cluster of three voters and one learner per case, each node with
//! its own axum server, and one networked `MetaClient` per node. It runs
//! once with the leader's client first and once with the learner's.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use operon_common::meta::MetaStore;
use operon_meta::{MetaClient, MetaNode};
use operon_meta_conformance::{Backend, Faults, Instance};

use crate::http::{HttpCluster, WAIT};

/// Lost acknowledgements through `inject_lost_ack`; `disturb` cuts the
/// current leader off (its routes answer 503 and its outgoing RPCs are
/// dropped); `heal` reconnects every node.
struct HttpFaults {
    nodes: Vec<MetaNode>,
    clients: Vec<MetaClient>,
}

#[async_trait]
impl Faults for HttpFaults {
    fn lose_next_ack(&self, client: usize) {
        self.clients[client].inject_lost_ack();
    }

    async fn disturb(&self, seed: u64) {
        let deadline = Instant::now() + WAIT;
        let start = usize::try_from(seed % self.nodes.len() as u64).unwrap_or(0);
        loop {
            for i in 0..self.nodes.len() {
                let node = &self.nodes[(start + i) % self.nodes.len()];
                if let Some((leader, _)) = node.leader()
                    && let Some(leader) = self.nodes.iter().find(|n| n.id() == leader)
                {
                    leader.set_partitioned(true);
                    return;
                }
            }
            assert!(
                Instant::now() < deadline,
                "no node knows a leader to cut off"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn heal(&self) {
        for node in &self.nodes {
            node.set_partitioned(false);
        }
    }
}

/// Three voters and learner 4 over HTTP. `learner_first` puts the learner's
/// client first (and the leader's last); otherwise the leader's client is
/// first and the learner's last.
struct Http {
    learner_first: bool,
}

#[async_trait]
impl Backend for Http {
    async fn start(&self) -> Instance {
        let mut cluster = HttpCluster::start(3).await;
        assert!(cluster.join_learner(4).await, "learner 4 joins");
        let leader = cluster.leader().await;
        let mut order: Vec<u64> = vec![leader];
        order.extend([1, 2, 3].into_iter().filter(|id| *id != leader));
        order.push(4);
        if self.learner_first {
            order.reverse();
        }
        let clients: Vec<MetaClient> = order
            .iter()
            .map(|id| cluster.members[id].client.clone())
            .collect();
        let nodes: Vec<MetaNode> = cluster.members.values().map(|m| m.node.clone()).collect();
        let faults = Arc::new(HttpFaults {
            nodes,
            clients: clients.clone(),
        });
        Instance {
            clients: clients
                .iter()
                .cloned()
                .map(Arc::<dyn MetaStore>::from)
                .collect(),
            faults: Some(faults),
            guard: Box::new(cluster),
        }
    }
}

mod leader_client {
    operon_meta_conformance::metastore_conformance!(super::Http {
        learner_first: false
    });
}

mod learner_client {
    operon_meta_conformance::metastore_conformance!(super::Http {
        learner_first: true
    });
}
