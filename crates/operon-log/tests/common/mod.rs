//! Shared test harness: a single-node metastore, streams, and record helpers.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use operon_common::{NamespaceId, StreamId};
use operon_log::{LogConfig, OffsetRecord, Record, batch, segment};
use operon_meta::{
    Clock, Consistency, EntryKind, MetaClient, MetaClientConfig, MetaConfig, MetaNode, Router,
    SystemClock, WalClass,
};
use operon_store::{FaultyStore, Store};
use tempfile::TempDir;

pub const WAIT: Duration = Duration::from_secs(20);

/// A single-node metastore with a client.
pub struct Meta {
    pub node: MetaNode,
    pub client: MetaClient,
    _dir: TempDir,
}

impl Meta {
    pub async fn start() -> Self {
        Self::start_with(Arc::new(SystemClock), MetaClientConfig::default()).await
    }

    pub async fn start_with(clock: Arc<dyn Clock>, client_config: MetaClientConfig) -> Self {
        let dir = TempDir::new().expect("temp dir");
        let mut config = MetaConfig::new(1, dir.path(), Store::in_memory());
        config.clock = clock.clone();
        let node = MetaNode::start(config, &Router::new())
            .await
            .expect("start meta");
        node.initialize([1]).await.expect("initialize");
        node.wait_for_leader(WAIT).await.expect("leader");
        let client = MetaClient::new(node.clone(), vec![], clock, client_config);
        Self {
            node,
            client,
            _dir: dir,
        }
    }

    /// Creates namespace `ns` (if needed) and stream `name`.
    pub async fn stream(&self, ns: &str, name: &str, partitions: u32) -> (NamespaceId, StreamId) {
        let ns = match self.client.create_namespace(ns).await {
            Ok(id) => id,
            Err(operon_meta::MetaError::Rejected(operon_meta::ApplyError::NamespaceExists(id))) => {
                id
            }
            Err(err) => panic!("create namespace: {err}"),
        };
        let stream = self
            .client
            .create_stream(ns, name, partitions, WalClass::Standard)
            .await
            .expect("create stream");
        (ns, stream)
    }

    pub async fn high_watermark(&self, stream: StreamId, partition: u32) -> u64 {
        self.client
            .read(Consistency::Local, |s| {
                s.partition(stream, partition)
                    .map(|p| p.high_watermark())
                    .expect("partition")
            })
            .await
            .expect("read")
    }

    pub async fn shutdown(&self) {
        self.node.shutdown().await.expect("shutdown meta");
    }
}

/// A store whose faults the returned `FaultyStore` controls.
pub fn faulty_store() -> (Arc<FaultyStore>, Store) {
    let faulty = Arc::new(FaultyStore::new(Store::in_memory().inner().clone()));
    (faulty.clone(), Store::new(faulty))
}

/// A writer config that flushes quickly.
pub fn fast_config() -> LogConfig {
    LogConfig {
        flush_interval: Duration::from_millis(20),
        ..LogConfig::new(1)
    }
}

/// `n` records whose values are `"<tag>-<i>"`.
pub fn records(tag: &str, n: usize) -> Vec<Record> {
    (0..n)
        .map(|i| Record {
            key: Some(Bytes::from(format!("k{i}"))),
            value: Some(Bytes::from(format!("{tag}-{i}"))),
            headers: vec![],
            timestamp_ms: 1_000 + i as i64,
        })
        .collect()
}

pub fn value(record: &Record) -> String {
    String::from_utf8(record.value.clone().expect("value").to_vec()).expect("utf-8")
}

/// Reads a partition straight from its index entries and the store, without
/// the reader: every committed record, in offset order.
pub async fn read_direct(
    meta: &MetaClient,
    store: &Store,
    stream: StreamId,
    partition: u32,
) -> Vec<OffsetRecord> {
    let entries = meta
        .read(Consistency::Local, |s| {
            s.partition(stream, partition)
                .expect("partition")
                .entries()
                .cloned()
                .collect::<Vec<_>>()
        })
        .await
        .expect("read");
    let mut out = Vec::new();
    for entry in entries {
        match entry.kind {
            EntryKind::Wal => {
                let bytes = store
                    .get_range(&entry.object, entry.byte_range.clone())
                    .await
                    .expect("get");
                out.extend(batch::decode(&bytes, entry.base_offset).expect("decode"));
            }
            EntryKind::Segment => {
                let (bytes, _) = store.get(&entry.object).await.expect("get segment");
                let footer = segment::parse(&bytes).expect("parse segment");
                let data = &bytes[footer.data.start as usize..footer.data.end as usize];
                out.extend(batch::decode(data, footer.base_offset).expect("decode"));
            }
        }
    }
    out
}

/// Polls `check` until it holds.
pub async fn eventually<F, Fut>(what: &str, check: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + WAIT;
    while !check().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Rewrites up to `max_entries` leading WAL entries of a partition into one
/// segment and swaps it in, the way the segmenter does. Returns the segment
/// path, or `None` if there was nothing to segment or the swap lost a race.
pub async fn segment_now(
    meta: &MetaClient,
    store: &Store,
    namespace: NamespaceId,
    stream: StreamId,
    partition: u32,
    max_entries: usize,
) -> Option<String> {
    let entries: Vec<operon_meta::IndexEntry> = meta
        .read(Consistency::Local, |s| {
            s.partition(stream, partition)
                .expect("partition")
                .entries()
                .skip_while(|e| e.kind == EntryKind::Segment)
                .take_while(|e| e.kind == EntryKind::Wal)
                .take(max_entries)
                .cloned()
                .collect()
        })
        .await
        .expect("read");
    let first = entries.first()?;
    let mut builder = segment::SegmentBuilder::new(
        stream,
        partition,
        first.base_offset,
        operon_log::Encoding::Kafka,
    );
    let mut max_ts = i64::MIN;
    for entry in &entries {
        let bytes = store
            .get_range(&entry.object, entry.byte_range.clone())
            .await
            .ok()?;
        for b in batch::batches(&bytes) {
            let b = b.expect("batch");
            builder
                .push_batch(
                    Bytes::copy_from_slice(b.bytes),
                    b.record_count,
                    b.max_timestamp_ms,
                )
                .expect("push");
        }
        max_ts = max_ts.max(entry.max_timestamp_ms);
    }
    let (bytes, footer) = builder.finish();
    let path = operon_log::paths::segment(
        namespace,
        stream,
        partition,
        first.base_offset,
        ulid::Ulid::generate(),
    );
    store
        .put_if_absent(&path, bytes)
        .await
        .expect("put segment");
    let replaces = entries
        .iter()
        .map(|e| (e.base_offset, e.object.clone()))
        .collect();
    match meta
        .swap_segment(
            stream,
            partition,
            replaces,
            &path,
            footer.data,
            max_ts,
            None,
            operon_meta::Freshness {
                created_at_ms: meta.now_ms(),
                max_age_ms: 600_000,
            },
        )
        .await
    {
        Ok(()) => Some(path),
        Err(operon_meta::MetaError::Rejected(operon_meta::ApplyError::IndexMismatch {
            ..
        })) => {
            store.delete(&path).await.expect("delete");
            None
        }
        Err(err) => panic!("swap: {err}"),
    }
}

/// A small range cache over `store`, so reads often miss and hit the store.
pub async fn small_cache(store: &Store) -> operon_cache::RangeCache {
    operon_cache::RangeCache::new(
        store.clone(),
        operon_cache::RangeCacheConfig {
            block_size: 512,
            memory_bytes: 16 * 1024,
            disk: None,
        },
    )
    .await
    .expect("cache")
}

/// An object store that holds the next PUT under a prefix until released,
/// to interleave a background task with the test.
pub struct PausingStore {
    inner: Arc<dyn object_store::ObjectStore>,
    gate: Arc<Gate>,
}

pub struct Gate {
    prefix: std::sync::Mutex<Option<String>>,
    held: std::sync::Mutex<Option<String>>,
    paused: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}

impl Default for Gate {
    fn default() -> Self {
        Self {
            prefix: std::sync::Mutex::new(None),
            held: std::sync::Mutex::new(None),
            paused: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        }
    }
}

impl Gate {
    /// Holds the next PUT whose path starts with `prefix`.
    pub fn arm(&self, prefix: &str) {
        *self.prefix.lock().expect("lock") = Some(prefix.to_string());
    }

    /// Waits until a PUT is held.
    pub async fn paused(&self) {
        tokio::time::timeout(WAIT, self.paused.notified())
            .await
            .expect("a PUT was held");
    }

    /// Lets the held PUT continue.
    pub fn release(&self) {
        self.release.add_permits(1);
    }

    /// The path of the PUT that was held.
    pub fn held_path(&self) -> String {
        self.held
            .lock()
            .expect("lock")
            .clone()
            .expect("a PUT was held")
    }

    fn take(&self, path: &str) -> bool {
        let mut prefix = self.prefix.lock().expect("lock");
        if prefix.as_deref().is_some_and(|p| path.starts_with(p)) {
            *prefix = None;
            *self.held.lock().expect("lock") = Some(path.to_string());
            true
        } else {
            false
        }
    }
}

impl PausingStore {
    pub fn create() -> (Arc<Gate>, Store) {
        let gate = Arc::new(Gate::default());
        let store = PausingStore {
            inner: Store::in_memory().inner().clone(),
            gate: gate.clone(),
        };
        (gate, Store::new(Arc::new(store)))
    }
}

impl std::fmt::Debug for PausingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PausingStore")
    }
}

impl std::fmt::Display for PausingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PausingStore")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for PausingStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if self.gate.take(location.as_ref()) {
            self.gate.paused.notify_one();
            self.gate
                .release
                .acquire()
                .await
                .expect("semaphore open")
                .forget();
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
