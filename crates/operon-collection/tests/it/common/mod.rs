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
        Self::start_with_clock(Arc::new(SystemClock)).await
    }

    /// A metastore whose node and client read time from `clock`.
    pub async fn start_with_clock(clock: Arc<dyn Clock>) -> Self {
        let dir = TempDir::new().expect("temp dir");
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

/// A range cache for tests: 1 MiB blocks, 64 MiB of memory, no disk.
pub async fn range_cache(store: &Store) -> operon_cache::RangeCache {
    operon_cache::RangeCache::new(
        store.clone(),
        operon_cache::RangeCacheConfig {
            block_size: 1 << 20,
            memory_bytes: 64 << 20,
            disk: None,
        },
    )
    .await
    .expect("range cache")
}

/// A collection context over `store`.
pub async fn context(
    meta: &MetaClient,
    store: &Store,
    config: operon_collection::CollectionConfig,
) -> operon_collection::CollectionContext {
    operon_collection::CollectionContext {
        meta: meta.clone().into(),
        store: store.clone(),
        cache: range_cache(store).await,
        lance: operon_collection::LanceEnv::new(
            store.clone(),
            operon_collection::LanceConfig::default(),
        ),
        manifests: operon_collection::ManifestCache::new(config.manifest_cache_entries),
        config,
    }
}

/// The link-apply fixture (plan M1.1 Task 10): a single-node metastore, a
/// `FaultyStore(InMemory)`, a log writer and reader, one collection (its
/// implicit stream and link) and its `CollectionContext`.
pub struct TargetFixture {
    pub meta: Meta,
    pub faulty: Arc<FaultyStore>,
    pub store: Store,
    pub writer: LogWriter,
    pub reader: operon_log::LogReader,
    pub ctx: operon_collection::CollectionContext,
    pub ns: NamespaceId,
    pub cid: CollectionId,
    pub stream: StreamId,
    pub link: operon_meta::LinkId,
    pub partitions: u32,
}

/// Commits at most this many records per run in the target tests: every
/// test batch fits one commit.
pub const TARGET_BATCH_RECORDS: usize = 10_000;

impl TargetFixture {
    pub async fn start(schema: CollectionSchema, partitions: u32) -> Self {
        Self::start_with(
            schema,
            partitions,
            operon_collection::CollectionConfig::default(),
        )
        .await
    }

    pub async fn start_with(
        schema: CollectionSchema,
        partitions: u32,
        config: operon_collection::CollectionConfig,
    ) -> Self {
        Self::start_with_clock(schema, partitions, config, Arc::new(SystemClock)).await
    }

    /// [`TargetFixture::start_with`] over a metastore that reads time from
    /// `clock`.
    pub async fn start_with_clock(
        schema: CollectionSchema,
        partitions: u32,
        config: operon_collection::CollectionConfig,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let meta = Meta::start_with_clock(clock).await;
        let (faulty, store) = faulty_store();
        let ns = namespace(&meta.client, "acme").await;
        let (cid, stream, link) = meta
            .client
            .create_collection(ns, "docs", schema, partitions)
            .await
            .expect("create collection");
        let writer = log_writer(&meta.client, &store);
        let reader = operon_log::LogReader::new(meta.client.clone(), range_cache(&store).await);
        let ctx = context(&meta.client, &store, config).await;
        Self {
            meta,
            faulty,
            store,
            writer,
            reader,
            ctx,
            ns,
            cid,
            stream,
            link,
            partitions,
        }
    }

    pub fn factory(&self) -> Arc<operon_collection::CollectionTargetFactory> {
        Arc::new(operon_collection::CollectionTargetFactory::new(
            self.ctx.clone(),
        ))
    }

    pub fn hooked_factory(
        &self,
        hook: operon_collection::CollectionCommitHook,
    ) -> Arc<operon_collection::CollectionTargetFactory> {
        Arc::new(operon_collection::CollectionTargetFactory::new(self.ctx.clone()).with_hook(hook))
    }

    /// A link-apply source whose registry serves collections with `factory`.
    pub fn source(
        &self,
        factory: Arc<operon_collection::CollectionTargetFactory>,
    ) -> operon_link::LinkApplySource {
        let registry = operon_link::TargetRegistry::new().with(factory);
        operon_link::LinkApplySource::new(
            self.ctx.meta.clone(),
            self.reader.clone(),
            registry,
            operon_link::LinkConfig {
                batch_records: TARGET_BATCH_RECORDS,
                batch_interval: Duration::ZERO,
                ..operon_link::LinkConfig::default()
            },
        )
    }

    /// The collection's link, as the metastore holds it.
    pub async fn link(&self) -> operon_meta::Link {
        let id = self.link;
        self.meta
            .client
            .read(Consistency::Linearizable, |s| s.link(id).cloned())
            .await
            .expect("read")
            .expect("the link exists")
    }

    /// One pass of `source` as `owner`.
    pub async fn run_once(
        &self,
        source: &operon_link::LinkApplySource,
        owner: &str,
    ) -> Vec<(operon_worker::TaskKey, operon_worker::RunResult)> {
        operon_worker::run_once(&self.meta.client, owner, Duration::from_secs(5), source)
            .await
            .expect("run")
    }

    /// Runs `source` as `owner` until the link has applied the whole stream.
    pub async fn apply_all(&self, source: &operon_link::LinkApplySource, owner: &str) {
        let deadline = Instant::now() + WAIT;
        loop {
            let results = self.run_once(source, owner).await;
            if self.applied().await == self.high_watermarks().await {
                return;
            }
            for (_, result) in &results {
                if let operon_worker::RunResult::Ran(Err(err)) = result {
                    eprintln!("link run: {err}");
                }
            }
            assert!(Instant::now() < deadline, "the link never caught up");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Writes `ops` through a `CollectionWriter`; every op must be written.
    pub async fn write(&self, ops: Vec<DocOp>) {
        let writer =
            operon_collection::CollectionWriter::new(self.meta.client.clone(), self.writer.clone());
        let outcome = writer.write(self.ns, self.cid, ops).await.expect("write");
        for result in &outcome.results {
            assert!(
                matches!(result, operon_collection::OpResult::Written { .. }),
                "{result:?}"
            );
        }
    }

    /// Appends `op` as it is, bypassing the writer's validation, to
    /// `partition`.
    pub async fn append_op(&self, partition: u32, op: &DocOp) -> u64 {
        let record = operon_collection::encode(op).expect("encode");
        self.append_raw(partition, record).await
    }

    /// Appends one raw record to the implicit stream; returns its offset.
    pub async fn append_raw(&self, partition: u32, record: operon_log::Record) -> u64 {
        self.writer
            .append(self.stream, partition, vec![record])
            .await
            .expect("append")
            .base_offset
    }

    /// The live manifest's `applied`, empty before the first commit.
    pub async fn applied(&self) -> BTreeMap<u32, u64> {
        self.manifest().await.applied
    }

    /// The live manifest (the empty one before the first commit).
    pub async fn manifest(&self) -> operon_collection::CollectionManifest {
        match operon_collection::live_manifest(
            &*self.ctx.meta,
            &self.ctx.store,
            &self.ctx.manifests,
            self.ns,
            self.cid,
            Consistency::Linearizable,
        )
        .await
        .expect("live manifest")
        {
            Some((_, manifest)) => (*manifest).clone(),
            None => operon_collection::CollectionManifest::empty(self.cid),
        }
    }

    pub async fn high_watermarks(&self) -> BTreeMap<u32, u64> {
        let (stream, partitions) = (self.stream, self.partitions);
        self.meta
            .client
            .read(Consistency::Linearizable, |s| {
                (0..partitions)
                    .map(|p| {
                        (
                            p,
                            s.partition(stream, p).map_or(0, |ps| ps.high_watermark()),
                        )
                    })
                    .filter(|(_, hwm)| *hwm > 0)
                    .collect()
            })
            .await
            .expect("read")
    }

    /// Every record of the implicit stream, with its partition.
    pub async fn records(&self) -> Vec<(u32, operon_log::OffsetRecord)> {
        let mut out = Vec::new();
        for partition in 0..self.partitions {
            let mut offset = 0;
            loop {
                let response = self
                    .reader
                    .fetch(operon_log::FetchRequest {
                        stream: self.stream,
                        partition,
                        offset,
                        max_bytes: 16 << 20,
                        max_wait: Duration::ZERO,
                    })
                    .await
                    .expect("fetch");
                if response.records.is_empty() {
                    break;
                }
                offset = response.next_offset;
                out.extend(response.records.into_iter().map(|r| (partition, r)));
                if offset >= response.high_watermark {
                    break;
                }
            }
        }
        out
    }

    pub async fn schema(&self) -> CollectionSchema {
        let cid = self.cid;
        self.meta
            .client
            .read(Consistency::Linearizable, |s| {
                s.collection(cid).map(|c| c.schema.clone())
            })
            .await
            .expect("read")
            .expect("the collection exists")
    }

    /// What the stream should fold to (the gates' model).
    pub async fn expected(&self) -> BTreeMap<PrimaryKey, operon_collection::Expected> {
        let schema = self.schema().await;
        operon_collection::fold_stream(&schema, self.partitions, &self.records().await)
    }

    /// Every violation of the committed collection against the model.
    pub async fn verify(&self) -> Vec<String> {
        let expected = self.expected().await;
        operon_collection::verify_collection(&self.ctx, self.ns, self.cid, &expected)
            .await
            .expect("verify")
    }

    /// The PK index watermark, read without opening a writer.
    pub async fn pk_watermark(&self) -> operon_collection::PkWatermark {
        let reader = operon_pk::PkReader::open(
            &self.store,
            &operon_meta::collection_pk_prefix(self.ns, self.cid),
        )
        .await
        .expect("pk reader");
        let value = reader
            .get(operon_collection::PK_WATERMARK_KEY)
            .await
            .expect("read the watermark");
        reader.close().await.expect("close the reader");
        value
            .map(|bytes| operon_collection::PkWatermark::decode(&bytes).expect("a watermark"))
            .unwrap_or_default()
    }

    /// The link task's lease, taken as `owner`, as a commit fence.
    pub async fn fence(&self, owner: &str) -> operon_meta::Fence {
        let lease = format!("task/link/{}", self.link);
        let grant = self
            .meta
            .client
            .acquire_lease(&lease, owner, Duration::from_secs(30))
            .await
            .expect("lease");
        operon_meta::Fence {
            lease,
            epoch: grant.epoch,
        }
    }

    /// Every record after `state`'s applied offsets, up to the high
    /// watermarks, as one batch.
    pub async fn batch_after(&self, state: &operon_link::TargetState) -> operon_link::ApplyBatch {
        let records = self
            .records()
            .await
            .into_iter()
            .filter(|(p, r)| r.offset >= state.applied.get(p).copied().unwrap_or(0))
            .collect();
        operon_link::ApplyBatch {
            records,
            applied_after: self.high_watermarks().await,
        }
    }

    pub async fn snapshot(&self) -> operon_collection::CollectionSnapshot {
        operon_collection::CollectionSnapshot::open(
            &self.ctx,
            self.ns,
            self.cid,
            Consistency::Linearizable,
        )
        .await
        .expect("snapshot")
    }

    pub async fn shutdown(self) {
        self.writer.shutdown().await.expect("log writer");
        self.meta.shutdown().await;
    }
}

/// The partition of `pk` in a stream of `partitions`.
pub fn home(pk: &PrimaryKey, partitions: u32) -> u32 {
    operon_collection::partition_of(pk, partitions)
}
