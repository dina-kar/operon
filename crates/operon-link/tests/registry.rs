//! The link-target registry: links are applied by the factory of their
//! target kind, and links of an unregistered kind are reported, not applied.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use operon_cache::{RangeCache, RangeCacheConfig};
use operon_common::{NamespaceId, StreamId};
use operon_link::{
    ApplyBatch, CommitError, CounterTable, CounterTargetFactory, LinkApplySource, LinkConfig,
    LinkError, LinkTarget, LinkTargetFactory, TargetRegistry, TargetState,
};
use operon_log::{LogConfig, LogReader, LogWriter, Record};
use operon_meta::{
    Consistency, Fence, Link, LinkId, MetaClient, MetaClientConfig, MetaConfig, MetaNode, Router,
    SystemClock, TargetRef, WalClass,
};
use operon_store::Store;
use operon_worker::{TaskSource, run_once};
use tempfile::TempDir;

const WAIT: Duration = Duration::from_secs(30);
const PARTITIONS: u32 = 2;

struct Fixture {
    node: MetaNode,
    meta: MetaClient,
    store: Store,
    writer: LogWriter,
    reader: LogReader,
    ns: NamespaceId,
    stream: StreamId,
    _dir: TempDir,
}

fn config() -> LinkConfig {
    LinkConfig {
        batch_interval: Duration::ZERO,
        ..LinkConfig::default()
    }
}

fn counter_factory(store: &Store) -> Arc<CounterTargetFactory> {
    Arc::new(CounterTargetFactory::new(
        store.clone(),
        config().max_commit_delay,
    ))
}

impl Fixture {
    async fn start() -> Self {
        let dir = TempDir::new().expect("temp dir");
        let node = MetaNode::start(
            MetaConfig::new(1, dir.path(), Store::in_memory()),
            &Router::new(),
        )
        .await
        .expect("start meta");
        node.initialize([1]).await.expect("initialize");
        node.wait_for_leader(WAIT).await.expect("leader");
        let meta = MetaClient::new(
            node.clone(),
            vec![],
            Arc::new(SystemClock),
            MetaClientConfig::default(),
        );
        let ns = meta.create_namespace("acme").await.expect("namespace");
        let stream = meta
            .create_stream(ns, "events", PARTITIONS, WalClass::Standard)
            .await
            .expect("stream");
        let store = Store::in_memory();
        let writer = LogWriter::start(
            meta.clone(),
            store.clone(),
            LogConfig {
                flush_interval: Duration::from_millis(5),
                ..LogConfig::new(1)
            },
        )
        .expect("writer");
        let cache = RangeCache::new(store.clone(), RangeCacheConfig::default())
            .await
            .expect("cache");
        let reader = LogReader::new(meta.clone(), cache);
        Self {
            node,
            meta,
            store,
            writer,
            reader,
            ns,
            stream,
            _dir: dir,
        }
    }

    async fn link(&self, name: &str, kind: &str) -> Link {
        let target = TargetRef {
            kind: kind.to_string(),
            name: name.to_string(),
        };
        let id = self
            .meta
            .create_link(self.ns, name, self.stream, target, BTreeMap::new())
            .await
            .expect("link");
        self.meta
            .read(Consistency::Linearizable, |s| s.link(id).cloned())
            .await
            .expect("read")
            .expect("the link exists")
    }

    /// Appends counter records `c0 += 1`, `c1 += 2`, … and returns the sums.
    async fn append(&self, n: u32) -> BTreeMap<String, i64> {
        let mut sums = BTreeMap::new();
        for i in 0..n {
            let name = format!("c{}", i % 3);
            let delta = i64::from(i) + 1;
            let record = Record {
                key: Some(Bytes::from(name.clone())),
                value: Some(Bytes::from(delta.to_string())),
                headers: vec![],
                timestamp_ms: -1,
            };
            self.writer
                .append(self.stream, i % PARTITIONS, vec![record])
                .await
                .expect("append");
            *sums.entry(name).or_default() += delta;
        }
        sums
    }

    async fn high_watermarks(&self) -> BTreeMap<u32, u64> {
        let stream = self.stream;
        self.meta
            .read(Consistency::Local, |s| {
                (0..PARTITIONS)
                    .map(|p| {
                        (
                            p,
                            s.partition(stream, p).expect("partition").high_watermark(),
                        )
                    })
                    .filter(|(_, hwm)| *hwm > 0)
                    .collect()
            })
            .await
            .expect("read")
    }

    /// Runs `source` until every target in `targets` has applied everything.
    async fn apply_all(&self, source: &LinkApplySource, targets: &[Arc<dyn LinkTarget>]) {
        let deadline = Instant::now() + WAIT;
        loop {
            run_once(&self.meta, "w1", Duration::from_secs(5), source)
                .await
                .expect("run");
            let hwms = self.high_watermarks().await;
            let mut done = true;
            for target in targets {
                done &= target.load().await.expect("load").applied == hwms;
            }
            if done {
                return;
            }
            assert!(Instant::now() < deadline, "the links never caught up");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn shutdown(self) {
        self.writer.shutdown().await.expect("writer");
        self.node.shutdown().await.expect("meta");
    }
}

/// The M0 counter target, served by `CounterTargetFactory`.
#[tokio::test]
async fn counter_links_apply_through_the_registry() {
    let f = Fixture::start().await;
    let link = f.link("counts", "counter").await;
    let sums = f.append(12).await;
    let registry = TargetRegistry::new().with(counter_factory(&f.store));
    assert_eq!(registry.kinds(), ["counter"]);
    let source = LinkApplySource::new(f.reader.clone(), registry, config());
    let table = Arc::new(CounterTable::for_link(
        f.meta.clone(),
        f.store.clone(),
        &link,
    ));
    let target: Arc<dyn LinkTarget> = table.clone();
    f.apply_all(&source, &[target]).await;
    let snapshot = table.snapshot().await.expect("snapshot");
    assert_eq!(snapshot.counters, sums);
    assert_eq!(snapshot.skipped, 0);
    assert!(source.unregistered().is_empty());
    f.shutdown().await;
}

/// A link of kind `mystery` has no factory: it gets no task and is reported.
#[tokio::test]
async fn a_link_of_an_unregistered_kind_is_reported_not_applied() {
    let f = Fixture::start().await;
    let mystery = f.link("puzzle", "mystery").await;
    let counter = f.link("counts", "counter").await;
    f.append(4).await;
    let registry = TargetRegistry::new().with(counter_factory(&f.store));
    let source = LinkApplySource::new(f.reader.clone(), registry, config());
    for _ in 0..2 {
        let candidates = source.candidates(&f.meta).await.expect("candidates");
        let keys: Vec<String> = candidates.iter().map(|(key, _)| key.to_string()).collect();
        assert_eq!(candidates.len(), 1, "{keys:?}");
        assert!(
            keys[0].contains(&format!("link/{}", counter.id)),
            "{keys:?}"
        );
        assert_eq!(
            source.unregistered(),
            BTreeMap::from([(mystery.id, "mystery".to_string())])
        );
    }
    f.shutdown().await;
}

/// What an in-memory target has committed.
#[derive(Debug, Default)]
struct MemoryState {
    version: u64,
    applied: BTreeMap<u32, u64>,
    records: u64,
}

/// A test-local target that counts the records it applied.
#[derive(Debug, Default)]
struct MemoryTarget {
    state: Mutex<MemoryState>,
}

#[async_trait]
impl LinkTarget for MemoryTarget {
    async fn load(&self) -> Result<TargetState, LinkError> {
        let state = self.state.lock().expect("lock");
        Ok(TargetState {
            version: state.version,
            applied: state.applied.clone(),
        })
    }

    async fn commit(
        &self,
        expected_version: u64,
        batch: ApplyBatch,
        _fence: &Fence,
    ) -> Result<u64, CommitError> {
        let mut state = self.state.lock().expect("lock");
        if state.version != expected_version {
            return Err(CommitError::Conflict);
        }
        state.version += 1;
        state.records += batch.records.len() as u64;
        state.applied.extend(batch.applied_after);
        Ok(state.version)
    }
}

/// Serves kind `memory` with one shared target per link.
#[derive(Debug, Default)]
struct MemoryFactory {
    targets: Mutex<BTreeMap<LinkId, Arc<MemoryTarget>>>,
}

impl MemoryFactory {
    fn target(&self, link: LinkId) -> Arc<MemoryTarget> {
        self.targets
            .lock()
            .expect("lock")
            .entry(link)
            .or_default()
            .clone()
    }
}

impl LinkTargetFactory for MemoryFactory {
    fn kind(&self) -> &str {
        "memory"
    }

    fn open(&self, _meta: &MetaClient, link: &Link) -> Result<Arc<dyn LinkTarget>, LinkError> {
        Ok(self.target(link.id))
    }
}

/// A counter link and a link of a test-local kind over one stream, applied
/// by one source.
#[tokio::test]
async fn two_kinds_apply_side_by_side() {
    let f = Fixture::start().await;
    let counter = f.link("counts", "counter").await;
    let memory = f.link("mem", "memory").await;
    let sums = f.append(10).await;
    let factory = Arc::new(MemoryFactory::default());
    let registry = TargetRegistry::new()
        .with(counter_factory(&f.store))
        .with(factory.clone());
    assert_eq!(registry.kinds(), ["counter", "memory"]);
    // A factory of the same kind replaces the first.
    let registry = registry.with(factory.clone());
    assert_eq!(registry.kinds(), ["counter", "memory"]);
    let source = LinkApplySource::new(f.reader.clone(), registry, config());
    let table = Arc::new(CounterTable::for_link(
        f.meta.clone(),
        f.store.clone(),
        &counter,
    ));
    f.apply_all(&source, &[table.clone(), factory.target(memory.id)])
        .await;
    assert_eq!(table.snapshot().await.expect("snapshot").counters, sums);
    assert_eq!(factory.target(memory.id).state.lock().unwrap().records, 10);
    assert!(source.unregistered().is_empty());
    f.shutdown().await;
}
