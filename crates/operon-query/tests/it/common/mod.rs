//! Shared test helpers: a single-node (or three-node) metastore,
//! `FaultyStore(PathStore(InMemory))`, a log writer with a 20 ms flush, a
//! collection with its context, M1.1's link to apply the implicit stream,
//! and read views (Task 4).
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

use operon_collection::{
    CollectionConfig, CollectionContext, CollectionManifest, CollectionSchema, CollectionSnapshot,
    DocOp, Document, DynamicMapping, Expected, FieldKind, FieldSpec, LanceConfig, LanceEnv,
    ManifestCache, PatchMode, PrimaryKey, VectorSpec, encode, fold_stream, partition_of,
};
use operon_common::meta::{Collection, Consistency};
use operon_common::{CollectionId, NamespaceId, StreamId};
use operon_link::{ApplyBatch, LinkTargetFactory};
use operon_log::{FetchRequest, LogConfig, LogReader, LogWriter, OffsetRecord, Record};
use operon_meta::{
    ApplyError, MetaClient, MetaClientConfig, MetaConfig, MetaError, MetaNode, Router, SystemClock,
};
use operon_query::hot::{HotTier, NoHotTier, RequestHot};
use operon_query::read::{ReadConfig, ReadView, Reads};
use operon_query::tail::{Tail, TailBudget, TailConfig, TailSnapshot};
use operon_query::{ReadConsistency, ServiceError};
use operon_store::{FaultyStore, Store};
use serde_json::{Map, Value};
use tempfile::TempDir;

pub const WAIT: Duration = Duration::from_secs(30);

pub fn obj(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(map) => map,
        other => panic!("not an object: {other}"),
    }
}

pub fn doc(pk: u64, source: Value) -> Document {
    Document {
        pk: PrimaryKey::U64(pk),
        source: obj(source),
        vectors: BTreeMap::new(),
        sparse_vectors: BTreeMap::new(),
    }
}

pub fn upsert(pk: u64, source: Value) -> DocOp {
    DocOp::Upsert(doc(pk, source))
}

pub fn patch(pk: u64, source: Value) -> DocOp {
    DocOp::Patch {
        pk: PrimaryKey::U64(pk),
        mode: PatchMode::MergeDeep,
        source: obj(source),
        delete_keys: Vec::new(),
        vectors: BTreeMap::new(),
        sparse_vectors: BTreeMap::new(),
        upsert: None,
    }
}

/// A field named like its source path.
pub fn field(name: &str, kind: FieldKind) -> FieldSpec {
    FieldSpec {
        name: name.to_string(),
        source_path: name.to_string(),
        fast: !matches!(kind, FieldKind::Text { .. }),
        kind,
        indexed: true,
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

/// `t` Text standard, `tag` Keyword fast, `n` I64 fast and vector `v` (dim
/// 3, Cosine); unmapped paths are ignored.
pub fn tail_schema() -> CollectionSchema {
    let schema = CollectionSchema::new(
        vec![
            field(
                "t",
                FieldKind::Text {
                    analyzer: "standard".to_string(),
                    positions: true,
                },
            ),
            field("tag", FieldKind::Keyword),
            field("n", FieldKind::I64),
        ],
        vec![vector("v", 3)],
        DynamicMapping::Ignore,
    );
    schema.validate().expect("valid schema");
    schema
}

/// An [`ObjectStore`] over `InMemory` that counts `get`s per path and fails
/// the `get`s under chosen prefixes (row 0.59).
#[derive(Default)]
pub struct PathStore {
    inner: InMemory,
    gets: Mutex<HashMap<String, u64>>,
    failing: Mutex<Vec<String>>,
    /// `get`s that returned an injected failure, per path.
    failed: Mutex<HashMap<String, u64>>,
}

impl PathStore {
    /// The `get`s so far of paths starting with `prefix`.
    pub fn gets_under(&self, prefix: &str) -> u64 {
        self.gets
            .lock()
            .expect("lock")
            .iter()
            .filter(|(path, _)| path.starts_with(prefix))
            .map(|(_, n)| *n)
            .sum()
    }

    /// The `get`s so far of paths starting with `prefix` that returned an
    /// injected failure.
    pub fn failures_under(&self, prefix: &str) -> u64 {
        self.failed
            .lock()
            .expect("lock")
            .iter()
            .filter(|(path, _)| path.starts_with(prefix))
            .map(|(_, n)| *n)
            .sum()
    }

    /// Every `get` of a path under `prefix` fails from now on.
    pub fn fail_gets_under(&self, prefix: &str) {
        self.failing.lock().expect("lock").push(prefix.to_string());
    }

    pub fn clear_failures(&self) {
        self.failing.lock().expect("lock").clear();
    }

    /// Every path written so far.
    pub async fn paths(&self) -> Vec<String> {
        use futures::StreamExt;
        self.inner
            .list(None)
            .map(|meta| meta.expect("list").location.to_string())
            .collect()
            .await
    }
}

impl fmt::Debug for PathStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PathStore").finish()
    }
}

impl fmt::Display for PathStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PathStore")
    }
}

#[async_trait]
impl ObjectStore for PathStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let path = location.to_string();
        *self
            .gets
            .lock()
            .expect("lock")
            .entry(path.clone())
            .or_default() += 1;
        let failing = self
            .failing
            .lock()
            .expect("lock")
            .iter()
            .any(|prefix| path.starts_with(prefix.as_str()));
        if failing {
            *self
                .failed
                .lock()
                .expect("lock")
                .entry(path.clone())
                .or_default() += 1;
            return Err(object_store::Error::Generic {
                store: "PathStore",
                source: format!("injected get failure on {path}").into(),
            });
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// A metastore: one node, or three (then `node` and `client` are the
/// leader's and `followers` holds a client per follower).
pub struct Meta {
    pub node: MetaNode,
    pub client: MetaClient,
    pub nodes: Vec<MetaNode>,
    pub followers: Vec<MetaClient>,
    _router: Router,
    _dirs: Vec<TempDir>,
}

impl Meta {
    pub async fn start() -> Self {
        Self::start_n(1).await
    }

    /// `n` nodes over one object store.
    pub async fn start_n(n: u64) -> Self {
        let router = Router::new();
        let store = Store::in_memory();
        let dirs: Vec<TempDir> = (0..n).map(|_| TempDir::new().expect("temp dir")).collect();
        let mut nodes = Vec::new();
        for (id, dir) in (1..=n).zip(&dirs) {
            let config = MetaConfig::new(id, dir.path(), store.clone());
            nodes.push(MetaNode::start(config, &router).await.expect("start meta"));
        }
        nodes[0].initialize(1..=n).await.expect("initialize");
        for node in &nodes {
            node.wait_for_leader(WAIT).await.expect("leader");
        }
        let deadline = Instant::now() + WAIT;
        let leader = loop {
            let mut seen = Vec::new();
            for node in &nodes {
                seen.push(node.current_leader().await);
            }
            if let Some(Some(leader)) = seen.first()
                && seen.iter().all(|s| *s == Some(*leader))
            {
                break *leader;
            }
            assert!(Instant::now() < deadline, "no agreed leader: {seen:?}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let client_of = |local: &MetaNode| {
            let peers = nodes
                .iter()
                .filter(|n| n.id() != local.id())
                .cloned()
                .collect();
            MetaClient::new(
                local.clone(),
                peers,
                Arc::new(SystemClock),
                MetaClientConfig::default(),
            )
        };
        let node = nodes
            .iter()
            .find(|n| n.id() == leader)
            .expect("the leader is a node")
            .clone();
        let client = client_of(&node);
        let followers = nodes
            .iter()
            .filter(|n| n.id() != leader)
            .map(client_of)
            .collect();
        Self {
            node,
            client,
            nodes,
            followers,
            _router: router,
            _dirs: dirs,
        }
    }

    pub async fn shutdown(&self) {
        for node in &self.nodes {
            node.shutdown().await.expect("shutdown meta");
        }
    }
}

/// The tail fixture (plan M1.2 Task 3 tests).
pub struct TailFixture {
    pub meta: Meta,
    pub paths: Arc<PathStore>,
    pub faulty: Arc<FaultyStore>,
    pub store: Store,
    pub writer: LogWriter,
    pub reader: LogReader,
    pub ctx: CollectionContext,
    pub ns: NamespaceId,
    pub cid: CollectionId,
    pub stream: StreamId,
    pub link: operon_common::meta::LinkId,
    pub partitions: u32,
}

impl TailFixture {
    pub async fn start(schema: CollectionSchema, partitions: u32) -> Self {
        Self::start_with(Meta::start().await, schema, partitions).await
    }

    /// Over a three-node metastore; writes and the link use the leader.
    pub async fn start_cluster(schema: CollectionSchema, partitions: u32) -> Self {
        Self::start_with(Meta::start_n(3).await, schema, partitions).await
    }

    pub async fn start_with(meta: Meta, schema: CollectionSchema, partitions: u32) -> Self {
        Self::start_with_config(meta, schema, partitions, CollectionConfig::default()).await
    }

    /// Over a one-node metastore, with collection config `config`.
    pub async fn start_configured(
        schema: CollectionSchema,
        partitions: u32,
        config: CollectionConfig,
    ) -> Self {
        Self::start_with_config(Meta::start().await, schema, partitions, config).await
    }

    pub async fn start_with_config(
        meta: Meta,
        schema: CollectionSchema,
        partitions: u32,
        config: CollectionConfig,
    ) -> Self {
        let paths = Arc::new(PathStore::default());
        let faulty = Arc::new(FaultyStore::new(paths.clone()));
        let store = Store::new(faulty.clone());
        let ns = match meta.client.create_namespace("acme").await {
            Ok(id) => id,
            Err(MetaError::Rejected(ApplyError::NamespaceExists(id))) => id,
            Err(err) => panic!("create namespace: {err}"),
        };
        let (cid, stream, link) = meta
            .client
            .create_collection(ns, "docs", schema, partitions)
            .await
            .expect("create collection");
        let writer = LogWriter::start(
            meta.client.clone(),
            store.clone(),
            LogConfig {
                flush_interval: Duration::from_millis(20),
                ..LogConfig::new(1)
            },
        )
        .expect("log writer");
        let cache = operon_cache::RangeCache::new(
            store.clone(),
            operon_cache::RangeCacheConfig {
                block_size: 1 << 20,
                memory_bytes: 64 << 20,
                disk: None,
            },
        )
        .await
        .expect("range cache");
        let reader = LogReader::new(meta.client.clone(), cache.clone());
        let ctx = CollectionContext {
            meta: meta.client.clone().into(),
            store: store.clone(),
            cache,
            lance: LanceEnv::new(store.clone(), LanceConfig::default()),
            manifests: ManifestCache::new(config.manifest_cache_entries),
            config,
        };
        Self {
            meta,
            paths,
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

    pub async fn collection(&self) -> Collection {
        self.ctx
            .meta
            .collection(Consistency::Linearizable, self.cid)
            .await
            .expect("read")
            .expect("the collection exists")
    }

    pub async fn tail(&self, config: TailConfig) -> Arc<Tail> {
        Tail::start(
            self.ns,
            self.collection().await,
            self.ctx.clone(),
            self.reader.clone(),
            config,
            Arc::new(TailBudget::default()),
        )
    }

    pub fn home(&self, pk: &PrimaryKey) -> u32 {
        partition_of(pk, self.partitions)
    }

    /// Appends `op` as it is (no validation) to its key's partition;
    /// returns its offset.
    pub async fn append(&self, op: &DocOp) -> u64 {
        let partition = self.home(op.pk());
        self.append_raw(partition, encode(op).expect("encode"))
            .await
    }

    /// Appends `ops` in order, many records per append; returns each op's
    /// (partition, offset).
    pub async fn append_all(&self, ops: &[DocOp]) -> Vec<(u32, u64)> {
        let mut by_partition: BTreeMap<u32, Vec<(usize, Record)>> = BTreeMap::new();
        for (i, op) in ops.iter().enumerate() {
            by_partition
                .entry(self.home(op.pk()))
                .or_default()
                .push((i, encode(op).expect("encode")));
        }
        let mut out = vec![(0, 0); ops.len()];
        for (partition, records) in by_partition {
            for chunk in records.chunks(200) {
                let ack = self
                    .writer
                    .append(
                        self.stream,
                        partition,
                        chunk.iter().map(|(_, r)| r.clone()).collect(),
                    )
                    .await
                    .expect("append");
                for (offset, (i, _)) in (ack.base_offset..).zip(chunk) {
                    out[*i] = (partition, offset);
                }
            }
        }
        out
    }

    pub async fn append_raw(&self, partition: u32, record: Record) -> u64 {
        self.writer
            .append(self.stream, partition, vec![record])
            .await
            .expect("append")
            .base_offset
    }

    /// Every partition's high watermark.
    pub async fn high_watermarks(&self) -> BTreeMap<u32, u64> {
        let head = self
            .ctx
            .meta
            .collection_head(Consistency::Linearizable, self.cid)
            .await
            .expect("read")
            .expect("the collection exists");
        (0..self.partitions).zip(head.high_watermarks).collect()
    }

    /// The tail's snapshot once it covers every write so far.
    pub async fn sync(&self, tail: &Tail) -> Arc<TailSnapshot> {
        let targets = self.high_watermarks().await;
        tail.sync(&targets, tokio::time::Instant::now() + WAIT)
            .await
            .expect("the tail catches up")
    }

    /// The live manifest (the empty one before the first commit).
    pub async fn manifest(&self) -> (Option<String>, Arc<CollectionManifest>) {
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
            Some((path, manifest)) => (Some(path), manifest),
            None => (None, Arc::new(CollectionManifest::empty(self.cid))),
        }
    }

    pub async fn applied(&self) -> BTreeMap<u32, u64> {
        self.manifest().await.1.applied.clone()
    }

    fn factory(&self) -> Arc<operon_collection::CollectionTargetFactory> {
        Arc::new(operon_collection::CollectionTargetFactory::new(
            self.ctx.clone(),
        ))
    }

    /// Runs M1.1's link until it has applied the whole stream.
    pub async fn apply_link(&self) {
        let registry = operon_link::TargetRegistry::new().with(self.factory());
        let source = operon_link::LinkApplySource::new(
            self.ctx.meta.clone(),
            self.reader.clone(),
            registry,
            operon_link::LinkConfig {
                batch_records: 10_000,
                batch_interval: Duration::ZERO,
                ..operon_link::LinkConfig::default()
            },
        );
        let deadline = Instant::now() + WAIT;
        loop {
            let results = operon_worker::run_once(
                self.meta.client.clone(),
                "w1",
                Duration::from_secs(5),
                &source,
            )
            .await
            .expect("run");
            let hwm: BTreeMap<u32, u64> = self
                .high_watermarks()
                .await
                .into_iter()
                .filter(|(_, hwm)| *hwm > 0)
                .collect();
            if self.applied().await == hwm {
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

    /// Runs M1.1's index builds until they are idle.
    pub async fn build_indexes(&self) {
        let source = operon_collection::IndexBuildSource::new(self.ctx.clone());
        for _ in 0..20 {
            let results = operon_worker::run_once(
                self.meta.client.clone(),
                "indexer",
                Duration::from_secs(30),
                &source,
            )
            .await
            .expect("run");
            let idle = results.iter().all(|(_, result)| match result {
                operon_worker::RunResult::Ran(Ok(outcome)) => {
                    matches!(outcome, operon_worker::TaskOutcome::Idle)
                }
                operon_worker::RunResult::Ran(Err(err)) => panic!("index build: {err}"),
                _ => false,
            });
            if idle {
                return;
            }
        }
        panic!("the index builds never went idle");
    }

    /// Commits exactly the records below `upto` (per partition, from the
    /// live manifest's `applied`) through the collection link target;
    /// returns the new manifest version.
    pub async fn commit_upto(&self, upto: &BTreeMap<u32, u64>) -> u64 {
        let link = self
            .meta
            .client
            .read(Consistency::Linearizable, {
                let id = self.link;
                move |s| s.link(id).cloned()
            })
            .await
            .expect("read")
            .expect("the link exists");
        let factory = self.factory();
        let target = factory
            .open(&self.meta.client.clone().into(), &link)
            .expect("open target");
        let state = target.load().await.expect("load");
        let records: Vec<(u32, OffsetRecord)> = self
            .records()
            .await
            .into_iter()
            .filter(|(p, r)| {
                r.offset >= state.applied.get(p).copied().unwrap_or(0)
                    && r.offset < upto.get(p).copied().unwrap_or(0)
            })
            .collect();
        let lease = format!("task/link/{}", self.link);
        let grant = self
            .meta
            .client
            .acquire_lease(&lease, "w-partial", Duration::from_secs(30))
            .await
            .expect("lease");
        let fence = operon_common::meta::Fence {
            lease: lease.clone(),
            epoch: grant.epoch,
        };
        let batch = ApplyBatch {
            records,
            applied_after: upto.clone(),
        };
        let version = target
            .commit(state.version, batch, &fence)
            .await
            .expect("commit");
        self.meta
            .client
            .release_lease(&lease, "w-partial", grant.epoch)
            .await
            .ok();
        version
    }

    /// Every record of the implicit stream, with its partition.
    pub async fn records(&self) -> Vec<(u32, OffsetRecord)> {
        let mut out = Vec::new();
        for partition in 0..self.partitions {
            let mut offset = 0;
            loop {
                let response = self
                    .reader
                    .fetch(FetchRequest {
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

    /// What the stream folds to (M1.1's model).
    pub async fn expected(&self) -> BTreeMap<PrimaryKey, Expected> {
        let schema = self.collection().await.schema;
        fold_stream(&schema, self.partitions, &self.records().await)
    }

    pub async fn snapshot(&self) -> CollectionSnapshot {
        CollectionSnapshot::open(&self.ctx, self.ns, self.cid, Consistency::Linearizable)
            .await
            .expect("snapshot")
    }

    /// The durable row id of `pk` in the live manifest.
    pub async fn row_of(&self, pk: u64) -> Option<u64> {
        self.snapshot()
            .await
            .get_by_pk(&[PrimaryKey::U64(pk)])
            .await
            .expect("get")
            .remove(0)
            .map(|stored| stored.row_id)
    }

    /// Waits until `tail` has adopted manifest `version`.
    pub async fn adopted(&self, tail: &Tail, version: u64) -> Arc<TailSnapshot> {
        let deadline = Instant::now() + WAIT;
        loop {
            let current = tail.current();
            if current.manifest().version >= version {
                return current;
            }
            assert!(
                Instant::now() < deadline,
                "the tail never adopted version {version}"
            );
            tail.notify();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Read views over this fixture's context and log reader.
    pub fn reads(&self, tail: TailConfig, read: ReadConfig) -> Reads {
        Reads::new(self.ctx.clone(), self.reader.clone(), tail, read)
    }

    /// Read views whose metastore is follower `i`'s client (three-node
    /// fixtures).
    pub fn follower_reads(&self, i: usize, tail: TailConfig, read: ReadConfig) -> Reads {
        let follower = self.meta.followers[i].clone();
        let ctx = CollectionContext {
            meta: follower.clone().into(),
            ..self.ctx.clone()
        };
        let reader = LogReader::new(follower, self.ctx.cache.clone());
        Reads::new(ctx, reader, tail, read)
    }

    /// The view of `collection` for `consistency`, hot tier off.
    pub async fn view_of(
        &self,
        reads: &Reads,
        collection: &Collection,
        consistency: &ReadConsistency,
    ) -> Result<ReadView, ServiceError> {
        let hot = RequestHot {
            enabled: false,
            used: Default::default(),
        };
        let tier: Arc<dyn HotTier> = Arc::new(NoHotTier);
        reads
            .view(self.ns, collection, consistency, &hot, tier)
            .await
    }

    /// The view of this fixture's collection.
    pub async fn view(
        &self,
        reads: &Reads,
        consistency: &ReadConsistency,
    ) -> Result<ReadView, ServiceError> {
        let collection = self.collection().await;
        self.view_of(reads, &collection, consistency).await
    }

    pub async fn shutdown(self) {
        self.writer.shutdown().await.expect("log writer");
        self.meta.shutdown().await;
    }
}
