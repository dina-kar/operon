//! Multi-process `operon cluster` tests (plan M1.3 Task 11 rule 6; E43):
//! three `meta` nodes with every role and two `query,gateway` learners, each
//! its own `operon` process on `127.0.0.1`, over one `file://` bucket. Run
//! with `cargo test -p operon --features cluster-tests --test cluster`
//! (each test starts five or six processes: run them one at a time).
#![cfg(feature = "cluster-tests")]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use operon_common::{CollectionId, NamespaceId};
use operon_hot::{NodeDescriptor, Roles, owners};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::sync::Mutex;

/// Every wait of these tests.
const WAIT: Duration = Duration::from_secs(90);
const FULL: &str = "meta,log,query,worker,gateway";
const QUERY: &str = "query,gateway";
const TTL_MS: u64 = 1500;
const LEARNER_EXPIRY_MS: u64 = 3000;

/// A port nobody listens on now (bound and released).
fn reserve_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("reserve a port")
        .port()
}

struct Node {
    roles: &'static str,
    child: Option<Child>,
}

struct Cluster {
    dir: TempDir,
    ports: BTreeMap<u64, u16>,
    nodes: BTreeMap<u64, Node>,
    http: reqwest::Client,
}

impl Cluster {
    /// Nodes 1–3 with every role and 4–5 `query,gateway`, all ready.
    async fn start() -> Self {
        let mut cluster = Self {
            dir: TempDir::new().expect("temp dir"),
            ports: (1..=6).map(|id| (id, reserve_port())).collect(),
            nodes: BTreeMap::new(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .build()
                .expect("client"),
        };
        for id in 1..=3 {
            cluster.spawn(id, FULL);
        }
        for id in 4..=5 {
            cluster.spawn(id, QUERY);
        }
        for id in 1..=5 {
            cluster.wait_ready(id).await;
        }
        cluster
    }

    fn addr(&self, id: u64) -> String {
        format!("127.0.0.1:{}", self.ports[&id])
    }

    fn base(&self, id: u64) -> String {
        format!("http://{}", self.addr(id))
    }

    fn log_path(&self, id: u64) -> PathBuf {
        self.dir.path().join(format!("node-{id}.log"))
    }

    fn command(dir: &Path, id: u64, roles: &str, listen: &str, peers: &str) -> Command {
        let bucket = dir.join("bucket");
        std::fs::create_dir_all(&bucket).expect("bucket dir");
        let mut command = Command::new(env!("CARGO_BIN_EXE_operon"));
        command
            .arg("cluster")
            .args(["--node-id", &id.to_string(), "--roles", roles])
            .args(["--listen", listen, "--peers", peers])
            .arg("--bucket")
            .arg(format!("file://{}", bucket.display()))
            .arg("--data-dir")
            .arg(dir.join(format!("node-{id}")))
            .args([
                "--no-flight-sql",
                "--maintenance",
                "off",
                "--lease-ttl-ms",
                "1500",
                "--poll-interval-ms",
                "50",
                "--registry-ttl-ms",
                &TTL_MS.to_string(),
                "--learner-expiry-ms",
                &LEARNER_EXPIRY_MS.to_string(),
                "--membership-interval-ms",
                "500",
                "--flush-interval-ms",
                "20",
            ])
            .env("RUST_LOG", "warn,openraft=error")
            .stdin(Stdio::null())
            .stdout(Stdio::null());
        command
    }

    fn peers(&self) -> String {
        (1..=3)
            .map(|id| format!("{id}={}", self.addr(id)))
            .collect::<Vec<_>>()
            .join(",")
    }

    fn spawn(&mut self, id: u64, roles: &'static str) {
        let log = File::options()
            .create(true)
            .append(true)
            .open(self.log_path(id))
            .expect("log file");
        let child = Self::command(self.dir.path(), id, roles, &self.addr(id), &self.peers())
            .stderr(log)
            .spawn()
            .expect("spawn operon");
        self.nodes.insert(
            id,
            Node {
                roles,
                child: Some(child),
            },
        );
    }

    /// SIGKILL.
    fn kill(&mut self, id: u64) {
        if let Some(node) = self.nodes.get_mut(&id)
            && let Some(mut child) = node.child.take()
        {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// SIGTERM, then waits for the exit; whether it exited cleanly.
    fn terminate(&mut self, id: u64) -> bool {
        let Some(mut child) = self.nodes.get_mut(&id).and_then(|node| node.child.take()) else {
            return false;
        };
        let sent = Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .is_ok_and(|status| status.success());
        let deadline = Instant::now() + WAIT;
        while sent && Instant::now() < deadline {
            if let Ok(Some(status)) = child.try_wait() {
                return status.success();
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = child.kill();
        let _ = child.wait();
        false
    }

    fn restart(&mut self, id: u64) {
        let roles = self.nodes[&id].roles;
        self.kill(id);
        self.spawn(id, roles);
    }

    fn alive(&self) -> Vec<u64> {
        self.nodes
            .iter()
            .filter(|(_, node)| node.child.is_some())
            .map(|(id, _)| *id)
            .collect()
    }

    fn tail_of_log(&self, id: u64) -> String {
        let text = std::fs::read_to_string(self.log_path(id)).unwrap_or_default();
        let lines: Vec<&str> = text.lines().collect();
        lines[lines.len().saturating_sub(30)..].join("\n")
    }

    async fn wait_ready(&mut self, id: u64) {
        let deadline = Instant::now() + WAIT;
        loop {
            if let Some(node) = self.nodes.get_mut(&id)
                && let Some(child) = &mut node.child
                && let Ok(Some(status)) = child.try_wait()
            {
                panic!("node {id} exited ({status}):\n{}", self.tail_of_log(id));
            }
            let ready = self
                .http
                .get(format!("{}/ready", self.base(id)))
                .send()
                .await
                .is_ok_and(|r| r.status() == StatusCode::OK);
            if ready {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "node {id} never became ready:\n{}",
                self.tail_of_log(id)
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn stats(&self, id: u64) -> Option<Value> {
        let response = self
            .http
            .get(format!("{}/internal/v1/node/stats", self.base(id)))
            .send()
            .await
            .ok()?;
        response.json().await.ok()
    }

    /// The metastore leader as a live node sees it.
    async fn leader(&self) -> u64 {
        let deadline = Instant::now() + WAIT;
        loop {
            for id in self.alive() {
                if let Some(stats) = self.stats(id).await
                    && let Some(leader) = stats["meta"]["leader"].as_u64()
                    && self.alive().contains(&leader)
                {
                    return leader;
                }
            }
            assert!(Instant::now() < deadline, "no leader");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn post(&self, id: u64, path: &str, body: Value) -> (StatusCode, Value) {
        let response = self
            .http
            .post(format!("{}{path}", self.base(id)))
            .json(&body)
            .send()
            .await
            .unwrap_or_else(|err| panic!("POST {path} on node {id}: {err}"));
        let status = response.status();
        (status, response.json().await.unwrap_or(Value::Null))
    }

    async fn put(&self, id: u64, path: &str, body: Value) -> (StatusCode, Value) {
        let response = self
            .http
            .put(format!("{}{path}", self.base(id)))
            .json(&body)
            .send()
            .await
            .unwrap_or_else(|err| panic!("PUT {path} on node {id}: {err}"));
        let status = response.status();
        (status, response.json().await.unwrap_or(Value::Null))
    }

    async fn get(&self, id: u64, path: &str) -> (StatusCode, Value) {
        let response = self
            .http
            .get(format!("{}{path}", self.base(id)))
            .send()
            .await
            .unwrap_or_else(|err| panic!("GET {path} on node {id}: {err}"));
        let status = response.status();
        (status, response.json().await.unwrap_or(Value::Null))
    }

    /// POSTs until a 2xx answer (a metastore failover may refuse a few).
    async fn post_ok(&self, id: u64, path: &str, body: Value) -> Value {
        let deadline = Instant::now() + WAIT;
        loop {
            let (status, reply) = self.post(id, path, body.clone()).await;
            if status.is_success() {
                return reply;
            }
            assert!(
                Instant::now() < deadline,
                "POST {path} on node {id}: {status} {reply}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Creates namespace `acme` through `id`; returns its id.
    async fn namespace(&self, id: u64) -> NamespaceId {
        let reply = self
            .post_ok(id, "/v1/namespaces", json!({"name": "acme"}))
            .await;
        NamespaceId(reply["id"].as_u64().expect("namespace id"))
    }

    /// Creates collection `name` through `id`; returns its id.
    async fn collection(&self, id: u64, name: &str) -> CollectionId {
        self.post_ok(
            id,
            "/v1/namespaces/acme/collections",
            json!({"name": name, "schema": schema(), "partitions": 1}),
        )
        .await;
        let (_, info) = self.describe(id, name).await;
        CollectionId(info["id"].as_u64().expect("collection id"))
    }

    async fn describe(&self, id: u64, name: &str) -> (StatusCode, Value) {
        self.get(id, &format!("/v1/namespaces/acme/collections/{name}"))
            .await
    }

    async fn write(&self, id: u64, name: &str, keys: std::ops::Range<u64>) -> (StatusCode, Value) {
        self.post(
            id,
            &format!("/v1/namespaces/acme/collections/{name}/documents"),
            upserts(keys),
        )
        .await
    }

    async fn count(&self, id: u64, name: &str) -> Option<u64> {
        let (status, reply) = self
            .post(
                id,
                &format!("/v1/namespaces/acme/collections/{name}/documents/count"),
                json!({}),
            )
            .await;
        (status == StatusCode::OK)
            .then(|| reply["count"].as_u64())
            .flatten()
    }

    /// Strong gets of `keys` through `id`; the keys that came back.
    async fn present(&self, id: u64, name: &str, keys: &[u64]) -> BTreeSet<u64> {
        let (status, reply) = self
            .post(
                id,
                &format!("/v1/namespaces/acme/collections/{name}/documents/get"),
                json!({"ids": keys}),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{reply}");
        reply["documents"]
            .as_array()
            .expect("documents")
            .iter()
            .filter_map(|doc| doc["id"].as_u64())
            .collect()
    }

    async fn search(&self, id: u64, name: &str) -> (StatusCode, Value) {
        self.post(
            id,
            "/v1/namespaces/acme/query",
            json!({
                "collection": name,
                "retrievers": [{"text": {"query": {"match": {"field": "body", "text": "word2"}}, "k": 10}}],
                "limit": 10
            }),
        )
        .await
    }

    async fn owner(&self, id: u64, name: &str) -> Option<u64> {
        let (status, info) = self.describe(id, name).await;
        (status == StatusCode::OK)
            .then(|| info["hot"]["owner"]["node_id"].as_u64())
            .flatten()
    }

    async fn membership(&self, id: u64) -> (Vec<u64>, Vec<u64>, Option<u64>) {
        let stats = self.stats(id).await.expect("stats");
        let ids = |key: &str| -> Vec<u64> {
            stats["meta"][key]
                .as_array()
                .map(|ids| ids.iter().filter_map(Value::as_u64).collect())
                .unwrap_or_default()
        };
        (
            ids("voters"),
            ids("learners"),
            stats["meta"]["membership_index"].as_u64(),
        )
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for id in self.alive() {
            self.kill(id);
        }
    }
}

fn schema() -> Value {
    json!({
        "fields": [
            {"name": "body", "source_path": "body", "kind": {"text": {"analyzer": "standard", "positions": true}}, "indexed": true, "fast": false},
            {"name": "tenant", "source_path": "tenant", "kind": "keyword", "indexed": true, "fast": true}
        ],
        "vectors": [],
        "sparse_vectors": [],
        "dynamic": "ignore",
        "max_fields": 1000
    })
}

fn upserts(keys: std::ops::Range<u64>) -> Value {
    let ops: Vec<Value> = keys
        .map(|i| {
            json!({"upsert": {
                "id": i,
                "source": {"body": format!("word{} common", i % 5), "tenant": format!("t{}", i % 3)}
            }})
        })
        .collect();
    json!({ "ops": ops })
}

/// Waits until `check` holds.
async fn until<F, Fut>(what: &str, within: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + within;
    while !check().await {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn descriptor(id: u64) -> NodeDescriptor {
    NodeDescriptor {
        node_id: id,
        incarnation: ulid::Ulid::from_parts(0, 0),
        addr: SocketAddr::from(([127, 0, 0, 1], 1)),
        roles: Roles::parse("query").expect("roles"),
        zone: String::new(),
    }
}

/// The rendezvous owner of `(ns, cid)` among query nodes `ids`.
fn expected_owner(ns: NamespaceId, cid: CollectionId, ids: impl IntoIterator<Item = u64>) -> u64 {
    let nodes: Vec<NodeDescriptor> = ids.into_iter().map(descriptor).collect();
    owners(ns, cid, &nodes, 1)[0].node_id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_three_meta_cluster_with_two_query_nodes_starts_and_serves() {
    let mut cluster = Cluster::start().await;
    cluster.namespace(4).await;
    cluster.collection(4, "docs").await;
    for batch in 0..4 {
        let (status, reply) = cluster.write(5, "docs", batch * 50..(batch + 1) * 50).await;
        assert_eq!(status, StatusCode::OK, "{reply}");
    }
    assert_eq!(cluster.count(1, "docs").await, Some(200));
    let keys: Vec<u64> = (0..200).collect();
    assert_eq!(
        cluster.present(1, "docs", &keys).await,
        keys.iter().copied().collect()
    );
    let (voters, learners, _) = cluster.membership(1).await;
    assert_eq!((voters, learners), (vec![1, 2, 3], vec![4, 5]));

    // A learner that shuts down gracefully leaves the membership at once.
    assert!(
        cluster.terminate(4),
        "node 4 did not exit cleanly:\n{}",
        cluster.tail_of_log(4)
    );
    let (_, learners, _) = cluster.membership(1).await;
    assert_eq!(learners, vec![5]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killing_a_meta_follower_keeps_the_cluster_writable() {
    let mut cluster = Cluster::start().await;
    cluster.namespace(4).await;
    cluster.collection(4, "docs").await;
    let leader = cluster.leader().await;
    let follower = (1..=3).find(|id| *id != leader).expect("a follower");
    cluster.kill(follower);
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let (status, reply) = cluster.write(4, "docs", 0..20).await;
        if status == StatusCode::OK {
            break;
        }
        assert!(Instant::now() < deadline, "not writable: {status} {reply}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    until("the writes read back", WAIT, || async {
        cluster.count(4, "docs").await == Some(20)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killing_the_leader_loses_no_acknowledged_write() {
    let cluster = Arc::new(Mutex::new(Cluster::start().await));
    let (http, base) = {
        let c = cluster.lock().await;
        c.namespace(4).await;
        c.collection(4, "docs").await;
        (c.http.clone(), c.base(4))
    };
    let acked = Arc::new(std::sync::Mutex::new(Vec::<(u64, Instant)>::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (acked, stop) = (acked.clone(), stop.clone());
        tokio::spawn(async move {
            let mut key = 0u64;
            while !stop.load(Ordering::SeqCst) {
                let reply = http
                    .post(format!(
                        "{base}/v1/namespaces/acme/collections/docs/documents"
                    ))
                    .json(&upserts(key..key + 1))
                    .send()
                    .await;
                if reply.is_ok_and(|r| r.status() == StatusCode::OK) {
                    acked.lock().expect("lock").push((key, Instant::now()));
                }
                key += 1;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
    };
    until("some writes are acknowledged", WAIT, || async {
        acked.lock().expect("lock").len() >= 20
    })
    .await;
    let killed_at = {
        let mut c = cluster.lock().await;
        let leader = c.leader().await;
        c.kill(leader);
        Instant::now()
    };
    until(
        "writes resume after the leader died",
        Duration::from_secs(15),
        || async {
            acked
                .lock()
                .expect("lock")
                .iter()
                .any(|(_, at)| *at > killed_at + Duration::from_millis(100))
        },
    )
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    stop.store(true, Ordering::SeqCst);
    writer.await.expect("writer");
    let keys: Vec<u64> = acked
        .lock()
        .expect("lock")
        .iter()
        .map(|(k, _)| *k)
        .collect();
    let c = cluster.lock().await;
    let deadline = Instant::now() + WAIT;
    loop {
        let present = c.present(4, "docs", &keys).await;
        let missing: Vec<u64> = keys
            .iter()
            .filter(|k| !present.contains(k))
            .copied()
            .collect();
        if missing.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "acknowledged writes are missing: {missing:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_learner_rejoins_without_a_membership_change() {
    let mut cluster = Cluster::start().await;
    cluster.namespace(4).await;
    cluster.collection(4, "docs").await;
    let (status, _) = cluster.write(4, "docs", 0..10).await;
    assert_eq!(status, StatusCode::OK);
    let (_, learners, index) = cluster.membership(1).await;
    assert_eq!(learners, vec![4, 5]);
    cluster.restart(5);
    cluster.wait_ready(5).await;
    let stats = cluster.stats(5).await.expect("stats");
    assert_eq!(stats["meta"]["join_changed"], json!(false), "{stats}");
    let (_, learners, after) = cluster.membership(1).await;
    assert_eq!((learners, after), (vec![4, 5], index));
    until("node 5 serves reads", WAIT, || async {
        cluster.count(5, "docs").await == Some(10)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_route_to_the_owner_and_survive_its_death() {
    let mut cluster = Cluster::start().await;
    cluster.namespace(4).await;
    let leader = cluster.leader().await;
    // A collection whose owner is not the metastore leader, so killing the
    // owner tests routing, not a metastore failover.
    let mut chosen = None;
    for i in 0..10 {
        let name = format!("c{i}");
        cluster.collection(4, &name).await;
        let owner = cluster.owner(4, &name).await.expect("an owner");
        if owner != leader {
            chosen = Some((name, owner));
            break;
        }
    }
    let (name, owner) = chosen.expect("a collection not owned by the leader");
    let (status, reply) = cluster.write(4, &name, 0..50).await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    let (status, reply) = cluster
        .put(
            4,
            &format!("/v1/namespaces/acme/collections/{name}/hot"),
            json!({"text": true}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    assert_eq!(reply["hot"]["owner"]["node_id"], json!(owner), "{reply}");
    let via = [1, 2, 3, 4, 5]
        .into_iter()
        .find(|id| *id != owner && *id != leader)
        .expect("another node");

    let (status, local) = cluster.search(owner, &name).await;
    assert_eq!(status, StatusCode::OK, "{local}");
    let before = cluster.stats(owner).await.expect("stats")["forwarded_in"]
        .as_u64()
        .expect("forwarded_in");
    for _ in 0..20 {
        let (status, routed) = cluster.search(via, &name).await;
        assert_eq!(status, StatusCode::OK, "{routed}");
        assert_eq!(routed["hits"], local["hits"]);
    }
    let after = cluster.stats(owner).await.expect("stats")["forwarded_in"]
        .as_u64()
        .expect("forwarded_in");
    assert_eq!(after, before + 20);

    cluster.kill(owner);
    for _ in 0..5 {
        let (status, routed) = cluster.search(via, &name).await;
        assert_eq!(status, StatusCode::OK, "{routed}");
        assert_eq!(routed["hits"], local["hits"]);
    }
    until("a new owner is named", WAIT, || async {
        cluster
            .owner(via, &name)
            .await
            .is_some_and(|now| now != owner)
    })
    .await;
    let (status, routed) = cluster.search(via, &name).await;
    assert_eq!(status, StatusCode::OK, "{routed}");
    assert_eq!(routed["hits"], local["hits"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ownership_moves_when_a_query_node_joins() {
    let mut cluster = Cluster::start().await;
    let ns = cluster.namespace(4).await;
    let mut collections = Vec::new();
    for i in 0..50 {
        let name = format!("c{i}");
        let cid = cluster.collection(4, &name).await;
        collections.push((name, cid));
    }
    for (name, cid) in &collections {
        let expected = expected_owner(ns, *cid, 1..=5);
        until(&format!("{name} is owned by {expected}"), WAIT, || async {
            cluster.owner(1, name).await == Some(expected)
        })
        .await;
    }
    cluster.spawn(6, QUERY);
    cluster.wait_ready(6).await;
    let mut moved = 0;
    for (name, cid) in &collections {
        let before = expected_owner(ns, *cid, 1..=5);
        let after = expected_owner(ns, *cid, 1..=6);
        assert!(after == before || after == 6, "{name} moved to an old node");
        moved += usize::from(after != before);
        until(&format!("{name} is owned by {after}"), WAIT, || async {
            cluster.owner(1, name).await == Some(after)
        })
        .await;
        assert_eq!(cluster.count(1, name).await, Some(0), "{name}");
        assert_eq!(cluster.count(6, name).await, Some(0), "{name}");
    }
    assert!(moved > 0, "no collection moved to node 6");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_learner_that_stays_down_is_evicted() {
    let mut cluster = Cluster::start().await;
    let (_, learners, _) = cluster.membership(1).await;
    assert_eq!(learners, vec![4, 5]);
    cluster.kill(5);
    let within = Duration::from_millis(LEARNER_EXPIRY_MS + TTL_MS + 5_000);
    until("node 5 is evicted", within, || async {
        cluster.membership(1).await.1 == vec![4]
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_misconfigured_node_refuses_to_start() {
    let dir = TempDir::new().expect("temp dir");
    let port = reserve_port();
    let listen = format!("127.0.0.1:{port}");
    let output = Cluster::command(
        dir.path(),
        9,
        FULL,
        &listen,
        &format!("1=127.0.0.1:{}", reserve_port()),
    )
    .stderr(Stdio::piped())
    .output()
    .expect("run operon");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("node 9 has the meta role but is not in --peers"),
        "{stderr}"
    );
}
