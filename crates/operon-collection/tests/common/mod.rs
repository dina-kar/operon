//! Shared test helpers: documents, schemas, and a metastore with a log.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use operon_collection::{
    CollectionSchema, DocOp, Document, DynamicMapping, FieldKind, FieldSpec, PatchMode, PrimaryKey,
    SparseVector, VectorSpec,
};
use operon_common::{CollectionId, NamespaceId, StreamId};
use operon_log::{LogConfig, LogWriter};
use operon_meta::{
    ApplyError, Clock, Consistency, MetaClient, MetaClientConfig, MetaConfig, MetaError, MetaNode,
    Router, SystemClock,
};
use operon_store::{FaultyStore, Store};
use serde_json::{Map, Value};
use tempfile::TempDir;

pub const WAIT: Duration = Duration::from_secs(20);

pub fn obj(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(map) => map,
        other => panic!("not an object: {other}"),
    }
}

pub fn doc(pk: PrimaryKey, source: Value) -> Document {
    Document {
        pk,
        source: obj(source),
        vectors: BTreeMap::new(),
        sparse_vectors: BTreeMap::new(),
    }
}

pub fn upsert(pk: u64, source: Value) -> DocOp {
    DocOp::Upsert(doc(PrimaryKey::U64(pk), source))
}

pub fn patch(pk: PrimaryKey, source: Value) -> DocOp {
    DocOp::Patch {
        pk,
        mode: PatchMode::MergeDeep,
        source: obj(source),
        delete_keys: Vec::new(),
        vectors: BTreeMap::new(),
        sparse_vectors: BTreeMap::new(),
        upsert: None,
    }
}

pub fn sparse(indices: &[u32], values: &[f32]) -> SparseVector {
    SparseVector::new(indices.to_vec(), values.to_vec()).expect("canonical sparse vector")
}

/// A field named like its source path.
pub fn field(path: &str, kind: FieldKind) -> FieldSpec {
    FieldSpec {
        name: path.to_string(),
        source_path: path.to_string(),
        fast: !matches!(kind, FieldKind::Text { .. }),
        kind,
        indexed: true,
        ignore_malformed: false,
    }
}

pub fn text(path: &str) -> FieldSpec {
    field(
        path,
        FieldKind::Text {
            analyzer: "standard".to_string(),
            positions: true,
        },
    )
}

pub fn json(name: &str, source_path: &str) -> FieldSpec {
    FieldSpec {
        name: name.to_string(),
        source_path: source_path.to_string(),
        kind: FieldKind::Json,
        indexed: true,
        fast: true,
        ignore_malformed: false,
    }
}

pub fn vector(name: &str, dim: u32) -> VectorSpec {
    VectorSpec {
        name: name.to_string(),
        dim,
        distance: operon_collection::Distance::Cosine,
        element: operon_collection::VectorElement::F32,
        index: operon_collection::VectorIndexSpec::Auto,
        hnsw: operon_collection::HnswParams::default(),
        quantization: None,
    }
}

pub fn schema(fields: Vec<FieldSpec>, dynamic: DynamicMapping) -> CollectionSchema {
    let schema = CollectionSchema::new(fields, vec![], dynamic);
    schema.validate().expect("valid schema");
    schema
}

/// A single-node metastore with a client.
pub struct Meta {
    pub node: MetaNode,
    pub client: MetaClient,
    _dir: TempDir,
}

impl Meta {
    pub async fn start() -> Self {
        let dir = TempDir::new().expect("temp dir");
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let mut config = MetaConfig::new(1, dir.path(), Store::in_memory());
        config.clock = clock.clone();
        let node = MetaNode::start(config, &Router::new())
            .await
            .expect("start meta");
        node.initialize([1]).await.expect("initialize");
        node.wait_for_leader(WAIT).await.expect("leader");
        let client = MetaClient::new(node.clone(), vec![], clock, MetaClientConfig::default());
        Self {
            node,
            client,
            _dir: dir,
        }
    }

    pub async fn shutdown(&self) {
        self.node.shutdown().await.expect("shutdown meta");
    }
}

/// Three meta nodes in one process, connected by a `Router`.
pub struct Cluster {
    pub router: Router,
    pub nodes: Vec<MetaNode>,
    _dirs: Vec<TempDir>,
}

impl Cluster {
    pub async fn start() -> Self {
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
            nodes,
            _dirs: dirs,
        };
        cluster.leader().await;
        cluster
    }

    /// The node every node agrees is the leader.
    pub async fn leader(&self) -> MetaNode {
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

    pub fn node(&self, id: u64) -> &MetaNode {
        self.nodes
            .iter()
            .find(|n| n.id() == id)
            .expect("node exists")
    }

    /// A client whose local node is `local`, with the other nodes as peers.
    pub fn client(&self, local: u64) -> MetaClient {
        let peers = self.nodes.iter().filter(|n| n.id() != local).cloned();
        MetaClient::new(
            self.node(local).clone(),
            peers.collect(),
            Arc::new(SystemClock),
            MetaClientConfig::default(),
        )
    }

    pub async fn shutdown(self) {
        for node in &self.nodes {
            node.shutdown().await.expect("shutdown");
        }
    }
}

/// Polls `check` against `node`'s local state until it holds.
pub async fn eventually(node: &MetaNode, check: impl Fn(&operon_meta::MetaState) -> bool) {
    let deadline = Instant::now() + WAIT;
    while !node.read(Consistency::Local, &check).await.expect("read") {
        assert!(
            Instant::now() < deadline,
            "node {} never converged",
            node.id()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Creates namespace `ns` if needed.
pub async fn namespace(client: &MetaClient, ns: &str) -> NamespaceId {
    match client.create_namespace(ns).await {
        Ok(id) => id,
        Err(MetaError::Rejected(ApplyError::NamespaceExists(id))) => id,
        Err(err) => panic!("create namespace: {err}"),
    }
}

/// Creates collection `name` in `ns`; returns its id and implicit stream.
pub async fn collection(
    client: &MetaClient,
    ns: NamespaceId,
    name: &str,
    schema: CollectionSchema,
    partitions: u32,
) -> (CollectionId, StreamId) {
    let (id, stream, _) = client
        .create_collection(ns, name, schema, partitions)
        .await
        .expect("create collection");
    (id, stream)
}

/// A store whose faults the returned `FaultyStore` controls.
pub fn faulty_store() -> (Arc<FaultyStore>, Store) {
    let faulty = Arc::new(FaultyStore::new(Store::in_memory().inner().clone()));
    (faulty.clone(), Store::new(faulty))
}

/// A log writer that flushes every 20 ms.
pub fn log_writer(meta: &MetaClient, store: &Store) -> LogWriter {
    let config = LogConfig {
        flush_interval: Duration::from_millis(20),
        ..LogConfig::new(1)
    };
    LogWriter::start(meta.clone(), store.clone(), config).expect("start log writer")
}
