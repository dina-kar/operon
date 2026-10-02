//! The hot build fixture (plan M1.3 Task 5): M1.1's target fixture (a
//! single-node metastore on a `ManualClock`, a `FaultyStore(InMemory)`, a
//! log writer and reader, collections with their links, link apply) and a
//! `HotBuildSource` over `FlatEngine`.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use loams_collection::{
    CollectionConfig, CollectionContext, CollectionManifest, CollectionSchema, CollectionSnapshot,
    CollectionTargetFactory, CollectionWriter, DocOp, Document, DynamicMapping, FieldKind,
    FieldSpec, OpResult, PrimaryKey, VectorSpec,
};
use loams_common::meta::HotConfig;
use loams_common::{CollectionId, NamespaceId, StreamId};
use loams_hnsw::{FlatEngine, HnswEngine};
use loams_hot::{HotBuildConfig, HotBuildSource, HotTierConfig, HotTierImpl, TierError};
use loams_log::{LogConfig, LogReader, LogWriter};
use loams_meta::{
    Clock, Consistency, LinkId, ManualClock, MetaClient, MetaClientConfig, MetaConfig, MetaNode,
    Router, SystemClock,
};
use loams_query::placement::{LocalOnly, Owner, Placement};
use loams_store::{FaultyStore, Store};
use loams_worker::{RunResult, TaskError, TaskKey, TaskOutcome, run_once};
use serde_json::{Value, json};
use tempfile::TempDir;

pub const WAIT: Duration = Duration::from_secs(30);
/// The lease TTL of a task run.
pub const TTL: Duration = Duration::from_secs(30);

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

pub fn vector(dim: u32) -> VectorSpec {
    VectorSpec {
        name: String::new(),
        dim,
        distance: loams_collection::Distance::Cosine,
        element: loams_collection::VectorElement::F32,
        index: loams_collection::VectorIndexSpec::Auto,
        hnsw: loams_collection::HnswParams::default(),
        quantization: None,
    }
}

pub const DIM: u32 = 4;

/// `tag` (keyword), `n` (i64) and one dense vector of [`DIM`].
pub fn hot_schema() -> CollectionSchema {
    let schema = CollectionSchema::new(
        vec![field("tag", FieldKind::Keyword), field("n", FieldKind::I64)],
        vec![vector(DIM)],
        DynamicMapping::Ignore,
    );
    schema.validate().expect("valid schema");
    schema
}

/// A deterministic pseudo-random vector of `dim` values in [-1, 1) for key
/// `k` (splitmix64).
pub fn vector_of(k: u64, dim: u32) -> Vec<f32> {
    let mut state = k.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
    (0..dim)
        .map(|_| {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            (z >> 40) as f32 / (1u64 << 23) as f32 - 1.0
        })
        .collect()
}

/// Key `k`'s document, with the vector unless `with_vector` is false.
pub fn doc(k: u64, with_vector: bool) -> DocOp {
    let mut vectors = BTreeMap::new();
    if with_vector {
        vectors.insert(String::new(), vector_of(k, DIM));
    }
    let source = match json!({ "tag": format!("t{}", k % 3), "n": k as i64 }) {
        Value::Object(map) => map,
        _ => unreachable!("an object"),
    };
    DocOp::Upsert(Document {
        pk: PrimaryKey::U64(k),
        source,
        vectors,
        sparse_vectors: BTreeMap::new(),
    })
}

pub fn docs(keys: std::ops::Range<u64>) -> Vec<DocOp> {
    keys.map(|k| doc(k, true)).collect()
}

/// A single-node metastore on a manual clock.
pub struct Meta {
    pub node: MetaNode,
    pub client: MetaClient,
    _dir: TempDir,
}

impl Meta {
    pub async fn start(clock: Arc<dyn Clock>) -> Self {
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
}

/// One collection of the fixture.
#[derive(Clone, Copy, Debug)]
pub struct Coll {
    pub cid: CollectionId,
    pub stream: StreamId,
    pub link: LinkId,
    pub partitions: u32,
}

pub struct Fixture {
    pub meta: Meta,
    pub clock: Arc<ManualClock>,
    pub faulty: Arc<FaultyStore>,
    pub store: Store,
    pub writer: LogWriter,
    pub reader: LogReader,
    pub ctx: CollectionContext,
    pub ns: NamespaceId,
    /// The first collection, `docs`.
    pub coll: Coll,
    pub cid: CollectionId,
    pub data_dir: TempDir,
}

pub const PARTITIONS: u32 = 2;

impl Fixture {
    pub async fn start() -> Self {
        Self::start_with(hot_schema(), CollectionConfig::default()).await
    }

    pub async fn start_with(schema: CollectionSchema, config: CollectionConfig) -> Self {
        Self::start_inner(schema, config, false).await
    }

    /// A fixture whose Lance reads go through the range cache
    /// (`LanceEnv::with_cache`, Ruling 9).
    pub async fn start_cached() -> Self {
        Self::start_inner(hot_schema(), CollectionConfig::default(), true).await
    }

    async fn start_inner(schema: CollectionSchema, config: CollectionConfig, cached: bool) -> Self {
        let clock = Arc::new(ManualClock::new(SystemClock.now_ms()));
        let meta = Meta::start(clock.clone()).await;
        let faulty = Arc::new(FaultyStore::new(Store::in_memory().inner().clone()));
        let store = Store::new(faulty.clone());
        let ns = meta
            .client
            .create_namespace("acme")
            .await
            .expect("namespace");
        let writer = LogWriter::start(
            meta.client.clone(),
            store.clone(),
            LogConfig {
                flush_interval: Duration::from_millis(20),
                ..LogConfig::new(1)
            },
        )
        .expect("log writer");
        let cache = loams_cache::RangeCache::new(
            store.clone(),
            loams_cache::RangeCacheConfig {
                block_size: 1 << 20,
                memory_bytes: 64 << 20,
                disk: None,
            },
        )
        .await
        .expect("range cache");
        let reader = LogReader::new(meta.client.clone(), cache.clone());
        let lance = match cached {
            true => loams_collection::LanceEnv::with_cache(
                store.clone(),
                cache.clone(),
                loams_collection::LanceConfig::default(),
            ),
            false => loams_collection::LanceEnv::new(
                store.clone(),
                loams_collection::LanceConfig::default(),
            ),
        };
        let ctx = CollectionContext {
            meta: meta.client.clone().into(),
            store: store.clone(),
            cache,
            lance,
            manifests: loams_collection::ManifestCache::new(config.manifest_cache_entries),
            config,
        };
        let mut f = Self {
            meta,
            clock,
            faulty,
            store,
            writer,
            reader,
            ctx,
            ns,
            coll: Coll {
                cid: CollectionId(0),
                stream: StreamId(0),
                link: LinkId(0),
                partitions: PARTITIONS,
            },
            cid: CollectionId(0),
            data_dir: TempDir::new().expect("data dir"),
        };
        f.coll = f.create("docs", schema).await;
        f.cid = f.coll.cid;
        f
    }

    /// Creates another collection in the namespace.
    pub async fn create(&self, name: &str, schema: CollectionSchema) -> Coll {
        let (cid, stream, link) = self
            .meta
            .client
            .create_collection(self.ns, name, schema, PARTITIONS)
            .await
            .expect("create collection");
        Coll {
            cid,
            stream,
            link,
            partitions: PARTITIONS,
        }
    }

    /// The fixture's hot build configuration: builds under the data
    /// directory, every poll proposes every hot column, and small chunks.
    pub fn config(&self) -> HotBuildConfig {
        HotBuildConfig {
            poll_interval: Duration::ZERO,
            chunk_bytes: 64 << 10,
            scan_batch_rows: 128,
            ..HotBuildConfig::new(self.data_dir.path())
        }
    }

    pub fn source_with(&self, config: HotBuildConfig) -> HotBuildSource {
        self.source_over(config, Arc::new(FlatEngine))
    }

    pub fn source_over(
        &self,
        config: HotBuildConfig,
        engine: Arc<dyn HnswEngine>,
    ) -> HotBuildSource {
        HotBuildSource::new(self.ctx.clone(), config, engine).expect("source")
    }

    pub fn source(&self) -> HotBuildSource {
        self.source_with(self.config())
    }

    /// The fixture's hot tier configuration: local copies under the data
    /// directory, and the defaults otherwise.
    pub fn tier_config(&self) -> HotTierConfig {
        HotTierConfig::new(self.data_dir.path())
    }

    /// A hot tier without its loop (passes run through `reconcile_once`),
    /// over `FlatEngine`, owning every collection.
    pub async fn tier(&self) -> HotTierImpl {
        self.tier_with(
            self.tier_config(),
            Arc::new(LocalOnly),
            Arc::new(FlatEngine),
        )
        .await
    }

    pub async fn tier_with(
        &self,
        config: HotTierConfig,
        placement: Arc<dyn Placement>,
        engine: Arc<dyn HnswEngine>,
    ) -> HotTierImpl {
        HotTierImpl::new(self.ctx.clone(), config, 1, placement, engine)
            .await
            .expect("hot tier")
    }

    pub async fn pin(&self, cid: CollectionId, hot: HotConfig) {
        loams_common::meta::MetaStore::set_collection_hot(&self.meta.client, self.ns, cid, hot)
            .await
            .expect("set hot");
    }

    pub async fn pin_vectors(&self) {
        self.pin(
            self.cid,
            HotConfig {
                vectors: true,
                ..HotConfig::default()
            },
        )
        .await;
    }

    /// Writes `ops` to `coll`; every op must be written.
    pub async fn write_to(&self, coll: Coll, ops: Vec<DocOp>) {
        let writer = CollectionWriter::new(self.meta.client.clone(), self.writer.clone());
        let outcome = writer.write(self.ns, coll.cid, ops).await.expect("write");
        for result in &outcome.results {
            assert!(matches!(result, OpResult::Written { .. }), "{result:?}");
        }
    }

    /// Appends `op` as it is, bypassing the writer's validation.
    pub async fn append_op(&self, partition: u32, op: &DocOp) {
        let record = loams_collection::encode(op).expect("encode");
        self.writer
            .append(self.coll.stream, partition, vec![record])
            .await
            .expect("append");
    }

    pub fn link_source(&self) -> loams_link::LinkApplySource {
        let registry = loams_link::TargetRegistry::new()
            .with(Arc::new(CollectionTargetFactory::new(self.ctx.clone())));
        loams_link::LinkApplySource::new(
            self.ctx.meta.clone(),
            self.reader.clone(),
            registry,
            loams_link::LinkConfig {
                batch_records: 10_000,
                batch_interval: Duration::ZERO,
                ..loams_link::LinkConfig::default()
            },
        )
    }

    /// Runs link apply until every collection in `colls` applied its stream.
    pub async fn apply(&self, colls: &[Coll]) {
        let source = self.link_source();
        let deadline = Instant::now() + WAIT;
        loop {
            run_once(&self.meta.client, "linker", Duration::from_secs(5), &source)
                .await
                .expect("link run");
            let mut done = true;
            for coll in colls {
                if self.applied(*coll).await != self.high_watermarks(*coll).await {
                    done = false;
                }
            }
            if done {
                return;
            }
            assert!(Instant::now() < deadline, "the link never caught up");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Writes `ops` to the first collection and applies them in one commit.
    pub async fn commit(&self, ops: Vec<DocOp>) {
        self.write_to(self.coll, ops).await;
        self.apply(&[self.coll]).await;
    }

    pub async fn applied(&self, coll: Coll) -> BTreeMap<u32, u64> {
        self.manifest_of(coll.cid).await.applied
    }

    pub async fn high_watermarks(&self, coll: Coll) -> BTreeMap<u32, u64> {
        let (stream, partitions) = (coll.stream, coll.partitions);
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

    /// The live manifest of `cid` (the empty one before the first commit).
    pub async fn manifest_of(&self, cid: CollectionId) -> CollectionManifest {
        match self.live_of(cid).await {
            Some((_, manifest)) => manifest,
            None => CollectionManifest::empty(cid),
        }
    }

    pub async fn manifest(&self) -> CollectionManifest {
        self.manifest_of(self.cid).await
    }

    pub async fn live_of(&self, cid: CollectionId) -> Option<(String, CollectionManifest)> {
        loams_collection::live_manifest(
            &*self.ctx.meta,
            &self.ctx.store,
            &self.ctx.manifests,
            self.ns,
            cid,
            Consistency::Linearizable,
        )
        .await
        .expect("live manifest")
        .map(|(path, manifest)| (path, (*manifest).clone()))
    }

    pub async fn live(&self) -> (String, CollectionManifest) {
        self.live_of(self.cid).await.expect("a manifest")
    }

    pub async fn snapshot(&self) -> CollectionSnapshot {
        CollectionSnapshot::open(&self.ctx, self.ns, self.cid, Consistency::Linearizable)
            .await
            .expect("snapshot")
    }

    /// Every record of `coll`'s implicit stream, with its partition.
    pub async fn records(&self, coll: Coll) -> Vec<(u32, loams_log::OffsetRecord)> {
        let mut out = Vec::new();
        for partition in 0..coll.partitions {
            let mut offset = 0;
            loop {
                let response = self
                    .reader
                    .fetch(loams_log::FetchRequest {
                        stream: coll.stream,
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

    /// Every violation of the first collection against the stream's model.
    pub async fn verify(&self) -> Vec<String> {
        let schema = self
            .meta
            .client
            .read(Consistency::Linearizable, {
                let cid = self.cid;
                move |s| s.collection(cid).map(|c| c.schema.clone())
            })
            .await
            .expect("read")
            .expect("the collection");
        let expected = loams_collection::fold_stream(
            &schema,
            self.coll.partitions,
            &self.records(self.coll).await,
        );
        loams_collection::verify_collection(&self.ctx, self.ns, self.cid, &expected)
            .await
            .expect("verify")
    }

    pub async fn shutdown(self) {
        self.writer.shutdown().await.expect("log writer");
        self.meta.node.shutdown().await.expect("shutdown meta");
    }
}

/// A placement that says another node owns every collection.
#[derive(Debug)]
pub struct Elsewhere;

impl Placement for Elsewhere {
    fn owner(&self, _: NamespaceId, _: CollectionId) -> Owner {
        Owner::Remote {
            node_id: 2,
            addr: "127.0.0.1:1".parse().expect("an address"),
        }
    }
}

/// The results of one pass of `source`.
pub async fn run_all(
    f: &Fixture,
    source: &HotBuildSource,
    owner: &str,
) -> Vec<(TaskKey, RunResult)> {
    run_once(&f.meta.client, owner, TTL, source)
        .await
        .expect("run")
}

/// One pass of `source`, which must propose exactly one task.
pub async fn build_once(f: &Fixture, source: &HotBuildSource) -> Result<TaskOutcome, TaskError> {
    let results = run_all(f, source, "builder").await;
    let [(key, RunResult::Ran(result))] = <[_; 1]>::try_from(results).expect("one task") else {
        panic!("the build task did not run");
    };
    assert!(key.key.starts_with(loams_hot::BUILD_TASK_PREFIX), "{key}");
    result
}

/// The row ids of every live document of the first collection.
pub async fn live_rows(f: &Fixture) -> roaring::RoaringTreemap {
    f.snapshot()
        .await
        .scan_all()
        .await
        .expect("scan")
        .into_iter()
        .map(|doc| doc.row_id)
        .collect()
}

pub fn tier(err: TierError) -> String {
    err.to_string()
}

/// The manual clock as the differential harness moves it.
#[derive(Debug)]
pub struct DiffManualClock(pub Arc<ManualClock>);

impl loams_hot::differential::DiffClock for DiffManualClock {
    fn advance(&self, by: Duration) {
        self.0.advance(by);
    }
}

/// The differential harness's rebuild staleness (rule 2 `rebuilt`).
pub const DIFF_STALENESS: Duration = Duration::from_secs(60);

impl Fixture {
    /// A fixture for the differential harness (plan M1.3 Task 12): Lance
    /// reads through the range cache, and vector indexes from 256 rows
    /// (E32: the hot ANN is consulted only on the Lance-index path).
    pub async fn start_diff() -> Self {
        Self::start_inner(
            hot_schema(),
            CollectionConfig {
                index_min_rows: 256,
                ..CollectionConfig::default()
            },
            true,
        )
        .await
    }

    /// A service over the fixture whose brute-force threshold is 16 rows,
    /// so filtered approximate queries reach the ANN path (row 12.2).
    pub fn diff_service(&self) -> Arc<loams_query::CollectionService> {
        self.diff_service_with(|_| {})
    }

    /// [`Fixture::diff_service`] with `adjust` applied to its configuration.
    pub fn diff_service_with(
        &self,
        adjust: impl FnOnce(&mut loams_query::ServiceConfig),
    ) -> Arc<loams_query::CollectionService> {
        let mut config = loams_query::ServiceConfig {
            ann: loams_query::vector::AnnConfig {
                brute_force_min_rows: 16,
                ..loams_query::vector::AnnConfig::default()
            },
            ..loams_query::ServiceConfig::default()
        };
        adjust(&mut config);
        loams_query::CollectionService::new(
            self.ctx.clone(),
            CollectionWriter::new(self.meta.client.clone(), self.writer.clone()),
            self.reader.clone(),
            config,
        )
    }

    /// The differential fixture over collection `name`, with `engine` for
    /// builds and the tier; the hot service reads through `wrap(tier)`.
    pub async fn diff_fixture(
        &self,
        name: &str,
        engine: Arc<dyn HnswEngine>,
        wrap: impl FnOnce(HotTierImpl) -> Arc<dyn loams_query::hot::HotTier>,
    ) -> loams_hot::differential::DiffFixture {
        let tier = self
            .tier_with(self.tier_config(), Arc::new(LocalOnly), engine.clone())
            .await;
        let hot = self.diff_service();
        hot.set_hot_tier(wrap(tier.clone()));
        let maintenance = loams_collection::MaintenanceConfig {
            merge_policy: loams_quickwit::merge_policy::StableLogMergePolicyConfig {
                min_level_num_docs: 100,
                merge_factor: 2,
                max_merge_factor: 4,
                maturation_period: Duration::from_hours(48),
            },
            split_num_docs_target: 10_000,
            compaction_target_rows: 400,
            compaction_min_small_fragments: 3,
            poll_interval: Duration::ZERO,
            ..loams_collection::MaintenanceConfig::default()
        };
        loams_hot::differential::DiffFixture {
            meta: self.ctx.meta.clone(),
            clock: Arc::new(DiffManualClock(self.clock.clone())),
            ctx: self.ctx.clone(),
            ns: "acme".to_string(),
            collection: name.to_string(),
            hot,
            cold: self.diff_service(),
            tier,
            link: self.link_source(),
            index: loams_collection::IndexBuildSource::new(self.ctx.clone()),
            merge: loams_collection::SplitMergeSource::new(self.ctx.clone(), maintenance.clone()),
            compaction: loams_collection::LanceCompactionSource::new(self.ctx.clone(), maintenance),
            build: self.source_over(
                HotBuildConfig {
                    rebuild_max_staleness: DIFF_STALENESS,
                    ..self.config()
                },
                engine,
            ),
        }
    }
}
