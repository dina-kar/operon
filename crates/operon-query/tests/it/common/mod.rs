//! Shared test helpers: a single-node (or three-node) metastore,
//! `FaultyStore(PathStore(InMemory))`, a log writer with a 20 ms flush, a
//! collection with its context, M1.1's link to apply the implicit stream,
//! read views (Task 4), and a `CollectionService` fixture (Task 9).
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
    CollectionWriter, DocOp, Document, DynamicMapping, Expected, FieldKind, FieldSpec, LanceConfig,
    LanceEnv, ManifestCache, PatchMode, PrimaryKey, VectorSpec, encode, fold_stream, partition_of,
};
use operon_common::meta::{Collection, Consistency, MetaStore};
use operon_common::{CollectionId, NamespaceId, StreamId};
use operon_link::{ApplyBatch, LinkTargetFactory};
use operon_log::{FetchRequest, LogConfig, LogReader, LogWriter, OffsetRecord, Record};
use operon_meta::{
    ApplyError, MetaClient, MetaClientConfig, MetaConfig, MetaError, MetaNode, Router, SystemClock,
};
use operon_query::hot::{HotTier, NoHotTier, RequestHot};
use operon_query::read::{ReadConfig, ReadView, Reads};
use operon_query::tail::{Tail, TailBudget, TailConfig, TailSnapshot};
use operon_query::{CollectionService, ReadConsistency, ServiceConfig, ServiceError};
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

/// The object store, log and collection context over a metastore.
pub struct Storage {
    pub paths: Arc<PathStore>,
    pub faulty: Arc<FaultyStore>,
    pub store: Store,
    pub writer: LogWriter,
    pub reader: LogReader,
    pub ctx: CollectionContext,
}

impl Storage {
    /// `FaultyStore(PathStore(InMemory))`, a log writer with a 20 ms flush,
    /// a log reader and a collection context with `config`.
    pub async fn start(meta: &Meta, config: CollectionConfig) -> Self {
        let paths = Arc::new(PathStore::default());
        let faulty = Arc::new(FaultyStore::new(paths.clone()));
        let store = Store::new(faulty.clone());
        Self::start_over(meta, config, paths, faulty, store).await
    }

    /// [`Self::start`] over a `file://` store in `dir` (`paths` and
    /// `faulty` are not in its path).
    pub async fn start_local(meta: &Meta, config: CollectionConfig, dir: &std::path::Path) -> Self {
        let paths = Arc::new(PathStore::default());
        let faulty = Arc::new(FaultyStore::new(paths.clone()));
        let store = Store::from_url(&file_url(dir), Vec::<(String, String)>::new())
            .expect("a file:// store");
        Self::start_over(meta, config, paths, faulty, store).await
    }

    async fn start_over(
        meta: &Meta,
        config: CollectionConfig,
        paths: Arc<PathStore>,
        faulty: Arc<FaultyStore>,
        store: Store,
    ) -> Self {
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
            paths,
            faulty,
            store,
            writer,
            reader,
            ctx,
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
        let Storage {
            paths,
            faulty,
            store,
            writer,
            reader,
            ctx,
        } = Storage::start(&meta, config).await;
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

/// `file://<dir>/` (`dir` canonicalized, so it is absolute).
pub fn file_url(dir: &std::path::Path) -> String {
    let dir = dir.canonicalize().expect("an existing directory");
    let mut url = format!("file://{}", dir.display());
    if !url.ends_with('/') {
        url.push('/');
    }
    url
}

/// A seeded write history over `keys` keys of mixed types (u64, UUID and
/// string keys in turn): upserts and re-upserts, patches in all three modes
/// (some with an upsert document, some of missing keys), and about 15 %
/// deletes. Every source is a JSON object with a number, a string and a
/// nested object, so the patch modes differ (Task 14; Task 15 item 2 reuses
/// it).
pub fn mixed_history(seed: u64, ops: usize, keys: usize) -> Vec<DocOp> {
    use rand::{Rng, SeedableRng};
    let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(seed);
    let key = |i: usize| match i % 3 {
        0 => PrimaryKey::U64(i as u64 * 1_000_003),
        1 => {
            let mut bytes = [0u8; 16];
            bytes[..8].copy_from_slice(&(i as u64).to_be_bytes());
            bytes[8..].copy_from_slice(&(!(i as u64)).to_be_bytes());
            PrimaryKey::Uuid(bytes)
        }
        _ => PrimaryKey::Str(format!("key-{i:03}")),
    };
    let source = |rng: &mut rand_chacha::ChaCha8Rng| {
        let n: i64 = rng.random_range(-1_000..1_000);
        let word = ["alpha", "beta", "gamma", "delta"][rng.random_range(0..4)];
        let mut nested = Map::new();
        if rng.random_bool(0.5) {
            nested.insert("a".to_string(), Value::from(rng.random_range(0..10)));
        }
        if rng.random_bool(0.5) {
            nested.insert("b".to_string(), Value::from(word));
        }
        let mut map = Map::new();
        if rng.random_bool(0.8) {
            map.insert("n".to_string(), Value::from(n));
        }
        if rng.random_bool(0.8) {
            map.insert("word".to_string(), Value::from(word));
        }
        map.insert("nested".to_string(), Value::Object(nested));
        map
    };
    let modes = [
        PatchMode::MergeDeep,
        PatchMode::MergeTop,
        PatchMode::Replace,
    ];
    (0..ops)
        .map(|i| {
            let pk = key(rng.random_range(0..keys));
            let roll: f64 = rng.random();
            if roll < 0.15 {
                DocOp::Delete(pk)
            } else if roll < 0.50 {
                let upsert = rng.random_bool(0.3).then(|| Document {
                    pk: pk.clone(),
                    source: source(&mut rng),
                    vectors: BTreeMap::new(),
                    sparse_vectors: BTreeMap::new(),
                });
                let delete_keys = if rng.random_bool(0.2) {
                    vec!["nested.a".to_string()]
                } else {
                    Vec::new()
                };
                DocOp::Patch {
                    pk,
                    mode: modes[i % 3],
                    source: source(&mut rng),
                    delete_keys,
                    vectors: BTreeMap::new(),
                    sparse_vectors: BTreeMap::new(),
                    upsert,
                }
            } else {
                DocOp::Upsert(Document {
                    pk,
                    source: source(&mut rng),
                    vectors: BTreeMap::new(),
                    sparse_vectors: BTreeMap::new(),
                })
            }
        })
        .collect()
}

/// The live documents after `history`: the per-key latest-wins [`fold`] of
/// each key's ops in order.
///
/// [`fold`]: operon_collection::fold
pub fn fold_history(history: &[DocOp]) -> BTreeMap<PrimaryKey, Document> {
    let mut by_key: BTreeMap<PrimaryKey, Vec<&DocOp>> = BTreeMap::new();
    for op in history {
        by_key.entry(op.pk().clone()).or_default().push(op);
    }
    by_key
        .into_iter()
        .filter_map(|(pk, ops)| operon_collection::fold(None, ops).map(|doc| (pk, doc)))
        .collect()
}

/// The service fixture (plan M1.2 Task 9 tests): a one-node metastore, the
/// storage, M1.1's link and index sources run on demand or continuously,
/// and a `CollectionService` over them. It creates no namespace and no
/// collection.
pub struct Fixture {
    pub meta: Meta,
    pub storage: Storage,
    service: Arc<CollectionService>,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// One run of M1.1's link source over every collection.
async fn run_link(meta: &MetaClient, ctx: &CollectionContext, reader: &LogReader) {
    let registry = operon_link::TargetRegistry::new().with(Arc::new(
        operon_collection::CollectionTargetFactory::new(ctx.clone()),
    ));
    let source = operon_link::LinkApplySource::new(
        ctx.meta.clone(),
        reader.clone(),
        registry,
        operon_link::LinkConfig {
            batch_records: 10_000,
            batch_interval: Duration::ZERO,
            ..operon_link::LinkConfig::default()
        },
    );
    let results = operon_worker::run_once(meta.clone(), "w1", Duration::from_secs(5), &source)
        .await
        .expect("run the link");
    for (_, result) in &results {
        if let operon_worker::RunResult::Ran(Err(err)) = result {
            eprintln!("link run: {err}");
        }
    }
}

/// One run of M1.1's index builds over every collection.
async fn run_indexes(meta: &MetaClient, ctx: &CollectionContext) {
    let source = operon_collection::IndexBuildSource::new(ctx.clone());
    let results =
        operon_worker::run_once(meta.clone(), "indexer", Duration::from_secs(30), &source)
            .await
            .expect("run the index builds");
    for (_, result) in &results {
        if let operon_worker::RunResult::Ran(Err(err)) = result {
            eprintln!("index build: {err}");
        }
    }
}

impl Fixture {
    pub async fn start() -> Self {
        Self::start_with(ServiceConfig::default()).await
    }

    pub async fn start_with(config: ServiceConfig) -> Self {
        Self::start_configured(config, CollectionConfig::default()).await
    }

    /// [`Self::start_with`], with the collection storage's `collection`
    /// config (retention, kept manifests).
    pub async fn start_configured(config: ServiceConfig, collection: CollectionConfig) -> Self {
        let meta = Meta::start().await;
        let storage = Storage::start(&meta, collection).await;
        Self::over(meta, storage, config)
    }

    /// The fixture over a `file://` bucket in `dir`, with `lance_base_url =
    /// file://<dir>/` (Task 14): the Lance datasets are readable with the
    /// `lance` crate alone.
    pub async fn with_local_bucket(dir: &std::path::Path) -> Self {
        let meta = Meta::start().await;
        let storage = Storage::start_local(&meta, CollectionConfig::default(), dir).await;
        let config = ServiceConfig {
            lance_base_url: Some(file_url(dir)),
            ..ServiceConfig::default()
        };
        Self::over(meta, storage, config)
    }

    fn over(meta: Meta, storage: Storage, config: ServiceConfig) -> Self {
        let writer = CollectionWriter::new(meta.client.clone(), storage.writer.clone());
        let service =
            CollectionService::new(storage.ctx.clone(), writer, storage.reader.clone(), config);
        Self {
            meta,
            storage,
            service,
            worker: Mutex::new(None),
        }
    }

    pub fn service(&self) -> Arc<CollectionService> {
        self.service.clone()
    }

    /// Runs the link source once over every collection.
    pub async fn apply_link(&self) {
        run_link(&self.meta.client, &self.storage.ctx, &self.storage.reader).await;
    }

    /// Runs the index builds once over every collection.
    pub async fn build_indexes(&self) {
        run_indexes(&self.meta.client, &self.storage.ctx).await;
    }

    /// Runs the link and index sources every 20 ms until shutdown.
    pub fn start_worker(&self) {
        let meta = self.meta.client.clone();
        let ctx = self.storage.ctx.clone();
        let reader = self.storage.reader.clone();
        let task = tokio::spawn(async move {
            loop {
                run_link(&meta, &ctx, &reader).await;
                run_indexes(&meta, &ctx).await;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
        if let Some(old) = self.worker.lock().expect("lock").replace(task) {
            old.abort();
        }
    }

    /// Σ `link_lag_records` over every collection of every namespace.
    pub async fn link_lag(&self) -> u64 {
        let mut lag = 0;
        let namespaces = self
            .meta
            .client
            .namespaces(Consistency::Linearizable)
            .await
            .expect("namespaces");
        for namespace in namespaces {
            for info in self
                .service
                .list_collections(&namespace.name)
                .await
                .expect("list collections")
            {
                lag += info.link_lag_records;
            }
        }
        lag
    }

    /// Runs the link until every collection's `link_lag_records` is 0.
    pub async fn settle(&self) {
        let deadline = Instant::now() + WAIT;
        loop {
            let lag = self.link_lag().await;
            if lag == 0 {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "the link never caught up: {lag} records behind"
            );
            self.apply_link().await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub async fn shutdown(self) {
        if let Some(worker) = self.worker.lock().expect("lock").take() {
            worker.abort();
        }
        self.service.shutdown().await;
        self.storage.writer.shutdown().await.expect("log writer");
        self.meta.shutdown().await;
    }
}

// ----- Task 15: the battery, random histories and the four placements -----

/// The namespace of the Task 15 gates.
pub const GATE_NS: &str = "acme";

/// `title` Text standard, `tag` Keyword fast, `n` I64 fast, vector `v`
/// (dim 4, Cosine), sparse `s` (Idf) and `t` (None); unmapped paths are
/// ignored (Task 15).
pub fn battery_schema() -> CollectionSchema {
    let schema = CollectionSchema::new(
        vec![
            field(
                "title",
                FieldKind::Text {
                    analyzer: "standard".to_string(),
                    positions: true,
                },
            ),
            field("tag", FieldKind::Keyword),
            field("n", FieldKind::I64),
        ],
        vec![vector("v", 4)],
        DynamicMapping::Ignore,
    )
    .with_sparse_vectors(vec![
        operon_collection::SparseVectorSpec {
            name: "s".to_string(),
            modifier: operon_collection::SparseModifier::Idf,
        },
        operon_collection::SparseVectorSpec {
            name: "t".to_string(),
            modifier: operon_collection::SparseModifier::None,
        },
    ]);
    schema.validate().expect("valid schema");
    schema
}

/// The keys of the gates' key space (24 keys, then absent ones): u64,
/// UUID and string keys in turn.
pub fn mixed_key(i: usize) -> PrimaryKey {
    match i % 3 {
        0 => PrimaryKey::U64(i as u64 * 1_000_003),
        1 => {
            let mut bytes = [0u8; 16];
            bytes[..8].copy_from_slice(&(i as u64).to_be_bytes());
            bytes[8..].copy_from_slice(&(!(i as u64)).to_be_bytes());
            PrimaryKey::Uuid(bytes)
        }
        _ => PrimaryKey::Str(format!("key-{i:03}")),
    }
}

/// The keys a history writes.
pub const HISTORY_KEYS: usize = 24;
/// The 40-word vocabulary of `title`.
pub fn title_word(i: usize) -> String {
    format!("w{i:02}")
}
const TAGS: [&str; 5] = ["red", "green", "blue", "cyan", "gray"];

type GateRng = rand_chacha::ChaCha8Rng;

/// Up to 12 words: each of the first four words with probability 1/2
/// (sometimes twice), so conjunctions and phrases of them match, then 0–5
/// of the other 36.
fn random_title(rng: &mut GateRng) -> String {
    use rand::Rng;
    let mut words = Vec::new();
    for i in 0..4 {
        if rng.random_bool(0.5) {
            words.push(title_word(i));
            if rng.random_bool(0.2) {
                words.push(title_word(i));
            }
        }
    }
    for _ in 0..rng.random_range(0..=5) {
        words.push(title_word(rng.random_range(4..40)));
    }
    if words.is_empty() {
        words.push(title_word(rng.random_range(0..40)));
    }
    words.join(" ")
}

fn random_n(rng: &mut GateRng) -> Option<Value> {
    use rand::Rng;
    let roll: f64 = rng.random();
    if roll < 0.7 {
        Some(Value::from(rng.random_range(0..10)))
    } else if roll < 0.9 {
        let len = rng.random_range(1..=3);
        Some(Value::from(
            (0..len)
                .map(|_| rng.random_range(0..10))
                .collect::<Vec<i64>>(),
        ))
    } else {
        None
    }
}

fn random_vector(rng: &mut GateRng) -> Vec<f32> {
    use rand::Rng;
    (0..4).map(|_| rng.random_range(-1.0f32..1.0)).collect()
}

/// 0–6 entries over 30 indices, zero weights included (possibly empty).
fn random_sparse(rng: &mut GateRng) -> operon_collection::SparseVector {
    use rand::Rng;
    let n = if rng.random_bool(0.1) {
        0
    } else {
        rng.random_range(1..=6)
    };
    let mut indices = std::collections::BTreeSet::new();
    while indices.len() < n {
        indices.insert(rng.random_range(0u32..30));
    }
    let values = indices
        .iter()
        .map(|_| {
            if rng.random_bool(0.1) {
                0.0
            } else {
                rng.random_range(0.01f32..1.0)
            }
        })
        .collect();
    operon_collection::SparseVector::new(indices.into_iter().collect(), values)
        .expect("a valid sparse vector")
}

/// A whole document of the battery schema.
pub fn random_document(rng: &mut GateRng, pk: PrimaryKey) -> Document {
    use rand::Rng;
    let mut source = Map::new();
    source.insert("title".to_string(), Value::from(random_title(rng)));
    source.insert(
        "tag".to_string(),
        Value::from(TAGS[rng.random_range(0..TAGS.len())]),
    );
    if let Some(n) = random_n(rng) {
        source.insert("n".to_string(), n);
    }
    let mut vectors = BTreeMap::new();
    if rng.random_bool(0.8) {
        vectors.insert("v".to_string(), random_vector(rng));
    }
    let mut sparse_vectors = BTreeMap::new();
    for name in ["s", "t"] {
        if rng.random_bool(0.75) {
            sparse_vectors.insert(name.to_string(), random_sparse(rng));
        }
    }
    Document {
        pk,
        source,
        vectors,
        sparse_vectors,
    }
}

/// A random valid history of `len` ops over [`HISTORY_KEYS`] keys (Task 15
/// item 2): upserts with every battery field, patches in all three modes
/// (with and without an upsert document, deleting keys and setting or
/// deleting dense and sparse vectors), and 15 % deletes.
pub fn random_history(rng: &mut GateRng, len: usize) -> Vec<DocOp> {
    use rand::Rng;
    let modes = [
        PatchMode::MergeDeep,
        PatchMode::MergeTop,
        PatchMode::Replace,
    ];
    (0..len)
        .map(|_| {
            let pk = mixed_key(rng.random_range(0..HISTORY_KEYS));
            let roll: f64 = rng.random();
            if roll < 0.15 {
                return DocOp::Delete(pk);
            }
            if roll < 0.60 {
                return DocOp::Upsert(random_document(rng, pk));
            }
            let mut source = Map::new();
            if rng.random_bool(0.5) {
                source.insert("title".to_string(), Value::from(random_title(rng)));
            }
            if rng.random_bool(0.4) {
                source.insert(
                    "tag".to_string(),
                    Value::from(TAGS[rng.random_range(0..TAGS.len())]),
                );
            }
            if rng.random_bool(0.4)
                && let Some(n) = random_n(rng)
            {
                source.insert("n".to_string(), n);
            }
            let delete_keys = match rng.random_range(0..10) {
                0 => vec!["n".to_string()],
                1 => vec!["tag".to_string()],
                _ => Vec::new(),
            };
            let mut vectors = BTreeMap::new();
            match rng.random_range(0..10) {
                0..=2 => {
                    vectors.insert("v".to_string(), Some(random_vector(rng)));
                }
                3 => {
                    vectors.insert("v".to_string(), None);
                }
                _ => {}
            }
            let mut sparse_vectors = BTreeMap::new();
            for name in ["s", "t"] {
                match rng.random_range(0..10) {
                    0 | 1 => {
                        sparse_vectors.insert(name.to_string(), Some(random_sparse(rng)));
                    }
                    2 => {
                        sparse_vectors.insert(name.to_string(), None);
                    }
                    _ => {}
                }
            }
            let upsert = rng
                .random_bool(0.3)
                .then(|| random_document(rng, pk.clone()));
            DocOp::Patch {
                pk,
                mode: modes[rng.random_range(0..3)],
                source,
                delete_keys,
                vectors,
                sparse_vectors,
                upsert,
            }
        })
        .collect()
}

/// One read of the battery.
#[derive(Clone, Debug)]
pub enum Probe {
    Search(Box<operon_query::SearchRequest>),
    Get(Vec<PrimaryKey>),
    Count(Option<operon_query::Query>),
    Scroll(Option<operon_query::Query>),
}

impl Probe {
    /// Whether it has a vector retriever without `exact` or a metric
    /// override (no probe of [`battery`] has one).
    pub fn is_approximate(&self) -> bool {
        use operon_query::Retriever;
        let Probe::Search(request) = self else {
            return false;
        };
        fn approximate(retriever: &Retriever) -> bool {
            match retriever {
                Retriever::Vector { params, .. } => !params.exact && params.distance.is_none(),
                Retriever::Fused { inputs, .. } => inputs.iter().any(approximate),
                Retriever::Rescore { input, .. } => approximate(input),
                Retriever::Text { .. } | Retriever::Sparse { .. } => false,
            }
        }
        request.retrievers.iter().any(approximate)
    }
}

/// What every battery read projects: the source, every vector and the
/// fields `n` and `tag`.
pub fn battery_projection() -> operon_query::Projection {
    operon_query::Projection {
        source: operon_query::SourceFilter::All,
        vectors: vec!["v".to_string(), "s".to_string(), "t".to_string()],
        fields: vec!["n".to_string(), "tag".to_string()],
    }
}

/// The fixed battery of Task 15 item 1 over `collection`: 40 probes, each
/// with its name. Every retriever asks for 30 candidates, more than the key
/// space holds, so no probe cuts at a k-th place. Every probe is exact.
pub fn battery(collection: &str) -> Vec<(&'static str, Probe)> {
    use operon_query::{
        AnnParams, BoolOperator, FieldValue, Fusion, GroupBy, Highlight, HighlightField,
        MissingOrder, MultiMatchKind, Query, Retriever, SearchRequest, SortKey, SortOrder,
        SortValue, SparseParams, TrackTotalHits,
    };
    let words = |ids: &[usize]| {
        ids.iter()
            .map(|i| title_word(*i))
            .collect::<Vec<_>>()
            .join(" ")
    };
    let matching = |ids: &[usize], operator: BoolOperator| Query::Match {
        field: "title".to_string(),
        text: words(ids),
        operator,
        minimum_should_match: None,
        fuzziness: None,
        analyzer: None,
    };
    let or = |ids: &[usize]| matching(ids, BoolOperator::Or);
    let and = |ids: &[usize]| matching(ids, BoolOperator::And);
    let term = |field: &str, value: &str| Query::Term {
        field: field.to_string(),
        value: FieldValue::Str(value.to_string()),
    };
    let range_n = |gte: i64, lt: i64| Query::Range {
        field: "n".to_string(),
        gt: None,
        gte: Some(FieldValue::I64(gte)),
        lt: Some(FieldValue::I64(lt)),
        lte: None,
    };
    let text = |query: Query| Retriever::Text { query, k: 30 };
    let query_vector = vec![0.3, -0.5, 0.8, 0.1];
    let dense = |distance: Option<operon_collection::Distance>| Retriever::Vector {
        field: "v".to_string(),
        query: query_vector.clone(),
        k: 30,
        params: AnnParams {
            exact: true,
            distance,
            ..AnnParams::default()
        },
        filter: None,
    };
    let sparse_query = |pairs: &[(u32, f32)]| {
        operon_collection::SparseVector::new(
            pairs.iter().map(|(i, _)| *i).collect(),
            pairs.iter().map(|(_, v)| *v).collect(),
        )
        .expect("a valid sparse vector")
    };
    let s_query = sparse_query(&[(1, 0.5), (4, 0.8), (7, 0.3), (12, 1.0), (20, 0.6)]);
    let sparse =
        |field: &str, filter: Option<Query>, idf_corpus: Option<Query>| Retriever::Sparse {
            field: field.to_string(),
            query: if field == "s" {
                s_query.clone()
            } else {
                sparse_query(&[(2, 0.4), (4, 0.9), (29, 0.2)])
            },
            k: 30,
            filter,
            params: SparseParams { idf_corpus },
        };
    let search = |retrievers: Vec<Retriever>, fusion: Option<Fusion>| {
        let mut request = SearchRequest::new(collection);
        request.retrievers = retrievers;
        request.fusion = fusion;
        request.limit = 30;
        request.select = battery_projection();
        request
    };
    let with = |mut request: SearchRequest, edit: &dyn Fn(&mut SearchRequest)| {
        edit(&mut request);
        Probe::Search(Box::new(request))
    };
    let plain = |request: SearchRequest| Probe::Search(Box::new(request));
    let sort_n = vec![SortKey::Field {
        field: "n".to_string(),
        order: SortOrder::Asc,
        missing: MissingOrder::Last,
    }];
    let aggregation = |aggs: Value| {
        let mut request = SearchRequest::new(collection);
        request.limit = 0;
        request.aggregations = Some(aggs);
        Probe::Search(Box::new(request))
    };
    let highlight = HighlightField {
        field: "title".to_string(),
        pre_tag: "<em>".to_string(),
        post_tag: "</em>".to_string(),
        fragment_size: 100,
        number_of_fragments: 5,
    };
    let count_filters = [
        None,
        Some(term("tag", "red")),
        Some(range_n(3, 7)),
        Some(Query::Exists {
            field: "n".to_string(),
        }),
        Some(Query::Bool {
            must: vec![],
            should: vec![],
            must_not: vec![term("tag", "gray")],
            filter: vec![],
            minimum_should_match: None,
        }),
        Some(or(&[0, 5])),
    ];
    let count_names = [
        "count_all",
        "count_term",
        "count_range",
        "count_exists",
        "count_must_not",
        "count_match",
    ];
    let mut probes = vec![
        ("match_or", plain(search(vec![text(or(&[0, 3, 7]))], None))),
        (
            "match_and",
            plain(search(vec![text(and(&[0, 1, 2]))], None)),
        ),
        (
            "match_phrase",
            plain(search(
                vec![text(Query::MatchPhrase {
                    field: "title".to_string(),
                    text: words(&[0, 1]),
                    slop: 0,
                })],
                None,
            )),
        ),
        (
            "multi_match",
            plain(search(
                vec![text(Query::MultiMatch {
                    fields: vec![("title".to_string(), 1.0), ("tag".to_string(), 2.0)],
                    text: format!("{} {} red", title_word(1), title_word(4)),
                    kind: MultiMatchKind::MostFields,
                    operator: BoolOperator::Or,
                    tie_breaker: None,
                })],
                None,
            )),
        ),
        (
            "bool",
            plain(search(
                vec![text(Query::Bool {
                    must: vec![or(&[0])],
                    should: vec![or(&[1]), or(&[2]), or(&[5])],
                    must_not: vec![term("tag", "gray")],
                    filter: vec![range_n(1, 10)],
                    minimum_should_match: None,
                })],
                None,
            )),
        ),
        (
            "filtered_text",
            with(search(vec![text(or(&[0, 2, 4, 6, 8]))], None), &|r| {
                r.filter = Some(term("tag", "red"))
            }),
        ),
        (
            "paged",
            with(search(vec![text(or(&[0, 3, 7]))], None), &|r| {
                r.offset = 5;
                r.limit = 5;
            }),
        ),
        (
            "sort_field",
            with(search(vec![text(or(&[0, 1, 2, 3]))], None), &|r| {
                r.sort = sort_n.clone();
                r.limit = 10;
            }),
        ),
        (
            "sort_search_after",
            with(search(vec![text(or(&[0, 1, 2, 3]))], None), &|r| {
                r.sort = sort_n.clone();
                r.search_after = Some(vec![SortValue::I64(4)]);
                r.limit = 10;
            }),
        ),
        (
            "sort_tag_desc_missing_first",
            with(search(vec![text(Query::MatchAll)], None), &|r| {
                r.sort = vec![SortKey::Field {
                    field: "tag".to_string(),
                    order: SortOrder::Desc,
                    missing: MissingOrder::First,
                }];
            }),
        ),
        ("vector_cosine", plain(search(vec![dense(None)], None))),
        (
            "vector_dot",
            plain(search(
                vec![dense(Some(operon_collection::Distance::Dot))],
                None,
            )),
        ),
        (
            "vector_euclid",
            plain(search(
                vec![dense(Some(operon_collection::Distance::Euclid))],
                None,
            )),
        ),
        (
            "vector_manhattan",
            plain(search(
                vec![dense(Some(operon_collection::Distance::Manhattan))],
                None,
            )),
        ),
        (
            "sparse_s",
            plain(search(vec![sparse("s", None, None)], None)),
        ),
        (
            "sparse_s_filtered",
            plain(search(vec![sparse("s", Some(range_n(0, 6)), None)], None)),
        ),
        (
            "sparse_s_idf_corpus",
            plain(search(
                vec![sparse("s", None, Some(term("tag", "red")))],
                None,
            )),
        ),
        (
            "sparse_t",
            plain(search(vec![sparse("t", None, None)], None)),
        ),
        (
            "rrf_text_vector",
            plain(search(
                vec![text(or(&[0, 3, 7])), dense(None)],
                Some(Fusion::Rrf { k: 60 }),
            )),
        ),
        (
            "dbsf_text_vector",
            plain(search(
                vec![text(and(&[0, 1, 2])), dense(None)],
                Some(Fusion::Dbsf),
            )),
        ),
        (
            "rrf_dense_sparse",
            plain(search(
                vec![dense(None), sparse("s", None, None)],
                Some(Fusion::Rrf { k: 60 }),
            )),
        ),
        (
            "dbsf_dense_sparse",
            plain(search(
                vec![dense(None), sparse("t", None, None)],
                Some(Fusion::Dbsf),
            )),
        ),
        (
            "weighted_text_sparse",
            plain(search(
                vec![text(or(&[0, 3, 7])), sparse("s", None, None)],
                Some(Fusion::WeightedSum {
                    weights: vec![0.7, 0.3],
                }),
            )),
        ),
        (
            "track_total_hits",
            with(search(vec![text(or(&[0, 1]))], None), &|r| {
                r.limit = 3;
                r.track_total_hits = TrackTotalHits::Exact;
            }),
        ),
        (
            "group_by",
            with(search(vec![text(or(&[0, 1, 2]))], None), &|r| {
                r.group_by = Some(GroupBy {
                    field: "tag".to_string(),
                    group_size: 2,
                    limit: 5,
                })
            }),
        ),
        (
            "highlight",
            with(search(vec![text(or(&[0, 3]))], None), &|r| {
                r.highlight = Some(Highlight {
                    fields: vec![highlight.clone()],
                })
            }),
        ),
        (
            "get",
            Probe::Get((0..HISTORY_KEYS + 3).map(mixed_key).collect()),
        ),
    ];
    for (name, filter) in count_names.into_iter().zip(count_filters) {
        probes.push((name, Probe::Count(filter)));
    }
    probes.push(("scroll_all", Probe::Scroll(None)));
    probes.push(("scroll_filtered", Probe::Scroll(Some(range_n(2, 8)))));
    probes.push((
        "aggs_terms",
        aggregation(serde_json::json!({"x": {"terms": {"field": "n", "size": 20}}})),
    ));
    probes.push((
        "aggs_stats",
        aggregation(serde_json::json!({"x": {"stats": {"field": "n"}}})),
    ));
    probes.push((
        "aggs_histogram",
        aggregation(serde_json::json!({"x": {"histogram": {"field": "n", "interval": 3}}})),
    ));
    probes.push((
        "aggs_range",
        aggregation(serde_json::json!({"x": {"range": {"field": "n", "ranges": [
            {"to": 3}, {"from": 3, "to": 6}, {"from": 6}
        ]}}})),
    ));
    probes.push((
        "aggs_cardinality",
        aggregation(serde_json::json!({"x": {"cardinality": {"field": "n"}}})),
    ));
    assert_eq!(probes.len(), 40, "the battery holds 40 probes");
    probes
}

/// A search response as JSON without its read token and hot report, every
/// hit's score as its `f32::to_bits`.
pub fn response_json(response: &operon_query::SearchResponse) -> Value {
    fn score_bits(hits: &mut Value, scores: impl Iterator<Item = f32>) {
        let hits = hits.as_array_mut().expect("hits");
        for (hit, score) in hits.iter_mut().zip(scores) {
            hit["score"] = Value::from(score.to_bits());
        }
    }
    let mut value = serde_json::to_value(response).expect("a response serializes");
    let object = value.as_object_mut().expect("an object");
    object.remove("read_token");
    object.remove("hot_used");
    score_bits(
        &mut value["hits"],
        response.hits.iter().map(|hit| hit.score),
    );
    if let Some(groups) = &response.groups {
        for (i, group) in groups.iter().enumerate() {
            score_bits(
                &mut value["groups"][i]["hits"],
                group.hits.iter().map(|hit| hit.score),
            );
        }
    }
    value
}

/// Runs `probe` over `collection` at `consistency`: its full JSON result
/// (Task 15 item 1), or the error as `{"error": …}`.
pub async fn run_probe(
    service: &CollectionService,
    collection: &str,
    probe: &Probe,
    consistency: &ReadConsistency,
) -> Value {
    let result = match probe {
        Probe::Search(request) => {
            let mut request = (**request).clone();
            request.collection = collection.to_string();
            request.consistency = consistency.clone();
            service
                .search(GATE_NS, request)
                .await
                .map(|response| response_json(&response))
        }
        Probe::Get(keys) => service
            .get(
                GATE_NS,
                collection,
                keys,
                &battery_projection(),
                consistency.clone(),
            )
            .await
            .map(|docs| serde_json::to_value(docs).expect("docs serialize")),
        Probe::Count(filter) => service
            .count(GATE_NS, collection, filter.clone(), consistency.clone())
            .await
            .map(Value::from),
        Probe::Scroll(filter) => {
            let mut pages = Vec::new();
            let mut after = None;
            loop {
                let page = service
                    .scroll(
                        GATE_NS,
                        collection,
                        filter.clone(),
                        after.clone(),
                        7,
                        &battery_projection(),
                        consistency.clone(),
                    )
                    .await;
                match page {
                    Ok((docs, next)) => {
                        pages.push(serde_json::json!({
                            "docs": docs,
                            "next": next.as_ref().map(|pk| format!("{pk:?}")),
                        }));
                        match next {
                            Some(next) => after = Some(next),
                            None => break Ok(Value::Array(pages)),
                        }
                    }
                    Err(err) => break Err(err),
                }
            }
        }
    };
    result.unwrap_or_else(|err| serde_json::json!({"error": err.to_string()}))
}

/// `value` without the `seq_no` and `partition` of its stored documents,
/// which a rebuild assigns anew (Task 15 item 2).
pub fn without_positions(mut value: Value) -> Value {
    fn strip(value: &mut Value) {
        match value {
            Value::Object(map) => {
                if map.contains_key("seq_no") && map.contains_key("partition") {
                    map.remove("seq_no");
                    map.remove("partition");
                }
                map.values_mut().for_each(strip);
            }
            Value::Array(items) => items.iter_mut().for_each(strip),
            _ => {}
        }
    }
    strip(&mut value);
    value
}

/// The four placements of one history (Task 15 item 2).
pub const PLACEMENTS: [&str; 4] = ["tail", "durable", "split", "rebuild"];

/// Writes `ops` to `collection` in one request; every op is accepted.
pub async fn write_ops(service: &CollectionService, collection: &str, ops: Vec<DocOp>) {
    if ops.is_empty() {
        return;
    }
    let result = service
        .write(
            GATE_NS,
            collection,
            ops,
            operon_query::WriteOptions::default(),
        )
        .await
        .expect("write");
    for (i, op) in result.results.iter().enumerate() {
        assert!(
            !matches!(op, operon_query::OpResult::Rejected(_)),
            "op {i} was refused: {op:?}"
        );
    }
}

/// Builds the four placements of `history` in `f`, each a collection of
/// [`battery_schema`] with 3 partitions named after [`PLACEMENTS`]:
/// - `tail`: every op written and never applied;
/// - `durable`: every op written and applied in the commits that end at
///   `cuts` (and at the end);
/// - `split`: the first `split_at` ops written and applied, the rest
///   written and not applied;
/// - `rebuild`: one upsert per live key of the folded history, applied in
///   one commit.
///
/// The link runs over every collection, so the collections are written in
/// the order that leaves exactly this state: rebuild, durable, the split's
/// prefix, then what is never applied.
pub async fn build_placements(f: &Fixture, history: &[DocOp], cuts: &[usize], split_at: usize) {
    let service = f.service();
    for name in PLACEMENTS {
        service
            .create_collection(GATE_NS, name, battery_schema(), Some(3))
            .await
            .expect("create a placement");
    }
    let rebuilt: Vec<DocOp> = fold_history(history)
        .into_values()
        .map(DocOp::Upsert)
        .collect();
    write_ops(&service, "rebuild", rebuilt).await;
    f.settle().await;
    let mut start = 0;
    for end in cuts.iter().copied().chain([history.len()]) {
        write_ops(&service, "durable", history[start..end].to_vec()).await;
        f.settle().await;
        start = end;
    }
    write_ops(&service, "split", history[..split_at].to_vec()).await;
    f.settle().await;
    write_ops(&service, "split", history[split_at..].to_vec()).await;
    write_ops(&service, "tail", history.to_vec()).await;
}
