//! The harness of the import tests (D1 Task 8): a durable server and runtime
//! on SQLite, an in-memory source store that counts reads per file, and an
//! in-memory sink that records every document it is given.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::net::{SocketAddr, TcpListener};
use std::path::Path as FsPath;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use operon_durable::import::{
    IdType, ImportConfig, ImportEnv, ImportSink, SinkError, SinkWrite, SourceOpener, StepHook,
};
use operon_durable::{
    DurableConfig, DurableRuntime, DurableServer, Operation, OperationId, OperationKinds,
    OperationState, Operations, OpsConfig, RuntimeOptions,
};
use operon_store::Store;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json::Value;

pub const SOURCE: &str = "memory:///bucket/";

/// A loopback address nothing listens on.
fn free_addr() -> SocketAddr {
    let probe = TcpListener::bind("127.0.0.1:0").expect("probe bind");
    probe.local_addr().expect("probe addr")
}

fn durable_config(dir: &FsPath) -> DurableConfig {
    let mut config = DurableConfig::sqlite(dir.join("durable").join("default.db"));
    config.listen = free_addr();
    config.retry_timeout = Duration::from_secs(1);
    config
}

/// The import settings of the tests: small slices, quick retries.
pub fn config() -> ImportConfig {
    let mut config = ImportConfig::new(false);
    config.schemes = vec!["memory".into(), "s3".into()];
    config.chunk_rows = 1000;
    config.retry_for = Duration::from_secs(10);
    config.backoff = Duration::from_millis(10);
    config.poll = Duration::from_millis(20);
    config.max_concurrent_operations = 8;
    config
}

// ─── the source store ───────────────────────────────────────────────────────

/// An object store that counts the reads of every path.
#[derive(Debug)]
pub struct Counting {
    inner: Arc<dyn ObjectStore>,
    reads: Mutex<HashMap<String, u64>>,
}

impl fmt::Display for Counting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Counting")
    }
}

impl Counting {
    pub fn reads(&self, key: &str) -> u64 {
        self.reads
            .lock()
            .expect("lock")
            .get(key)
            .copied()
            .unwrap_or(0)
    }

    pub fn all_reads(&self) -> HashMap<String, u64> {
        self.reads.lock().expect("lock").clone()
    }
}

#[async_trait]
impl ObjectStore for Counting {
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
        if !options.head {
            *self
                .reads
                .lock()
                .expect("lock")
                .entry(location.to_string())
                .or_default() += 1;
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

/// Every source URL opens the one test store.
pub struct TestSources(pub Store);

impl SourceOpener for TestSources {
    fn open(&self, _source: &str) -> Result<Store, String> {
        Ok(self.0.clone())
    }
}

// ─── the sink ───────────────────────────────────────────────────────────────

/// One document as the sink holds it.
#[derive(Debug, Clone, PartialEq)]
pub struct Doc {
    pub row: serde_json::Map<String, Value>,
    pub id_type: Option<IdType>,
    pub writes: u32,
}

type Refusal = Box<dyn Fn(usize) -> Option<SinkError> + Send + Sync>;

/// A sink that keeps documents by `_id`; a null `_id` is the mapper's row
/// error.
#[derive(Default)]
pub struct MemSink {
    pub docs: Mutex<BTreeMap<String, Doc>>,
    /// Every call of `write`, refused or not.
    pub attempts: AtomicUsize,
    /// Accepted writes.
    pub writes: AtomicUsize,
    tokens: AtomicU64,
    /// Called with the attempt number (from 0): `Some` refuses it.
    refuse: Mutex<Option<Refusal>>,
    /// Writes from this number on (from 0) wait for `release`.
    pause_at: Mutex<Option<usize>>,
    pub paused: AtomicBool,
    release: tokio::sync::Notify,
    released: AtomicBool,
}

impl MemSink {
    pub fn refuse(&self, f: impl Fn(usize) -> Option<SinkError> + Send + Sync + 'static) {
        *self.refuse.lock().expect("lock") = Some(Box::new(f));
    }

    pub fn pause_at(&self, n: usize) {
        *self.pause_at.lock().expect("lock") = Some(n);
    }

    pub fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release.notify_waiters();
    }

    pub fn ids(&self) -> Vec<String> {
        self.docs.lock().expect("lock").keys().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.docs.lock().expect("lock").len()
    }

    pub fn doc(&self, id: &str) -> Option<Doc> {
        self.docs.lock().expect("lock").get(id).cloned()
    }
}

/// The text of `array` at `row`, for `_id`.
fn id_at(array: &ArrayRef, row: usize) -> Option<String> {
    if array.is_null(row) {
        return None;
    }
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        return Some(a.value(row).to_string());
    }
    panic!("an _id of type {}", array.data_type());
}

#[async_trait]
impl ImportSink for MemSink {
    async fn check(&self, _ns: &str, collection: &str) -> Result<(), SinkError> {
        if collection == "missing" {
            return Err(SinkError::NotFound(format!(
                "collection {collection:?} not found"
            )));
        }
        Ok(())
    }

    async fn write(
        &self,
        _ns: &str,
        _collection: &str,
        batch: RecordBatch,
        id_type: Option<IdType>,
    ) -> Result<SinkWrite, SinkError> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        if let Some(refuse) = self.refuse.lock().expect("lock").as_ref()
            && let Some(err) = refuse(attempt)
        {
            return Err(err);
        }
        let n = self.writes.load(Ordering::SeqCst);
        let pause = *self.pause_at.lock().expect("lock");
        if pause.is_some_and(|at| n >= at) {
            self.paused.store(true, Ordering::SeqCst);
            while !self.released.load(Ordering::SeqCst) {
                let wait = self.release.notified();
                if self.released.load(Ordering::SeqCst) {
                    break;
                }
                let _ = tokio::time::timeout(Duration::from_millis(50), wait).await;
            }
        }
        let schema = batch.schema();
        let id_index = schema.index_of("_id").expect("an _id column");
        let ids = batch.column(id_index);
        for row in 0..batch.num_rows() {
            if ids.is_null(row) {
                return Err(SinkError::Failed {
                    code: "row_error".into(),
                    message: format!("row {row} column _id: the primary key is null"),
                });
            }
        }
        let mut rows_json = Vec::new();
        {
            let mut writer = arrow_json::ArrayWriter::new(&mut rows_json);
            writer.write(&batch).expect("json");
            writer.finish().expect("json");
        }
        let rows: Vec<serde_json::Map<String, Value>> = if rows_json.is_empty() {
            Vec::new()
        } else {
            serde_json::from_slice(&rows_json).expect("rows")
        };
        let effective = schema
            .field(id_index)
            .metadata()
            .get("operon-id-type")
            .map(|t| match t.as_str() {
                "uuid" => IdType::Uuid,
                "u64" => IdType::U64,
                _ => IdType::Str,
            })
            .or(id_type);
        let mut docs = self.docs.lock().expect("lock");
        for (row, json) in rows.into_iter().enumerate() {
            let id = id_at(ids, row).expect("checked");
            let doc = docs.entry(id).or_insert(Doc {
                row: serde_json::Map::new(),
                id_type: effective,
                writes: 0,
            });
            doc.row = json;
            doc.id_type = effective;
            doc.writes += 1;
        }
        self.writes.fetch_add(1, Ordering::SeqCst);
        let token = self.tokens.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(SinkWrite {
            rows: batch.num_rows() as u64,
            token: token.to_string(),
        })
    }

    /// Tokens are counters; the merge is the largest.
    fn merge_tokens(&self, tokens: &[String]) -> String {
        tokens
            .iter()
            .filter_map(|t| t.parse::<u64>().ok())
            .max()
            .map(|t| t.to_string())
            .unwrap_or_default()
    }
}

// ─── files ──────────────────────────────────────────────────────────────────

/// A Parquet file of `batch` with row groups of `group_rows` rows.
pub fn parquet_bytes(batch: &RecordBatch, group_rows: usize) -> Bytes {
    let mut out = Vec::new();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(group_rows))
        .build();
    let mut writer = ArrowWriter::try_new(&mut out, batch.schema(), Some(props)).expect("writer");
    writer.write(batch).expect("write");
    writer.close().expect("close");
    Bytes::from(out)
}

/// `rows` rows: `n` (Int64) and `t` (Utf8), with an `_id` column of
/// `<prefix><n>` when `ids` is set.
pub fn rows(prefix: &str, from: i64, count: i64, ids: bool) -> RecordBatch {
    let n: Vec<i64> = (from..from + count).collect();
    let t: Vec<String> = n.iter().map(|n| format!("row {n}")).collect();
    let mut columns: Vec<(&str, ArrayRef)> = Vec::new();
    if ids {
        let ids: Vec<String> = n.iter().map(|n| format!("{prefix}{n}")).collect();
        columns.push(("_id", Arc::new(StringArray::from(ids))));
    }
    columns.push(("n", Arc::new(Int64Array::from(n))));
    columns.push(("t", Arc::new(StringArray::from(t))));
    RecordBatch::try_from_iter(columns).expect("batch")
}

// ─── the environment ────────────────────────────────────────────────────────

/// A durable server, a runtime with the import kind, the operations, the
/// source store and the sink.
pub struct Harness {
    _dir: tempfile::TempDir,
    pub server: DurableServer,
    pub runtime: Option<DurableRuntime>,
    pub ops: Operations,
    pub env: ImportEnv,
    pub sink: Arc<MemSink>,
    pub store: Store,
    pub counting: Arc<Counting>,
}

fn options() -> RuntimeOptions {
    RuntimeOptions {
        ttl: Duration::from_secs(2),
    }
}

impl Harness {
    pub async fn start(config: ImportConfig) -> Self {
        Self::start_with(config, None).await
    }

    pub async fn start_with(config: ImportConfig, hook: Option<StepHook>) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let server = DurableServer::start(durable_config(dir.path()), "1")
            .await
            .expect("server");
        let counting = Arc::new(Counting {
            inner: Arc::new(InMemory::new()),
            reads: Mutex::new(HashMap::new()),
        });
        let store = Store::new(counting.clone());
        let sink = Arc::new(MemSink::default());
        let env = ImportEnv {
            sink: sink.clone(),
            sources: Arc::new(TestSources(store.clone())),
            config,
            hook,
        };
        let kinds = operon_durable::import::kinds(OperationKinds::new(), env.clone());
        let ops = Operations::new(
            server.client(),
            server.store().clone(),
            &kinds,
            OpsConfig::default(),
        );
        let mut harness = Self {
            _dir: dir,
            server,
            runtime: None,
            ops,
            env,
            sink,
            store,
            counting,
        };
        harness.start_runtime(None).await;
        harness
    }

    /// Start the runtime again, with `hook` in place of the old one.
    pub async fn start_runtime(&mut self, hook: Option<StepHook>) {
        if hook.is_some() {
            self.env.hook = hook;
        }
        let kinds = operon_durable::import::kinds(OperationKinds::new(), self.env.clone());
        let runtime = DurableRuntime::start_kinds(&self.server, "1", options(), &kinds)
            .await
            .expect("runtime");
        self.runtime = Some(runtime);
    }

    /// A crash of the runtime: it stops, abandoning what it runs.
    pub async fn crash(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.stop().await;
        }
    }

    pub async fn put(&self, key: &str, bytes: Bytes) {
        self.store.put(key, bytes).await.expect("put");
    }

    pub async fn submit(&self, body: Value, key: Option<&str>) -> (OperationId, bool) {
        operon_durable::import::submit(&self.ops, &self.env, "default", "docs", body, key)
            .await
            .expect("submit")
    }

    /// Poll `id` until it is finished (60 s at most).
    pub async fn finished(&self, id: &OperationId) -> Operation {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let op = self.ops.get(id).await.expect("get");
            if op.state.is_finished() {
                return op;
            }
            assert!(
                Instant::now() < deadline,
                "{id} is still {:?}: {op:?}",
                op.state
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub async fn succeeded(&self, id: &OperationId) -> Operation {
        let op = self.finished(id).await;
        assert_eq!(op.state, OperationState::Succeeded, "{op:?}");
        op
    }

    pub async fn stop(mut self) {
        self.crash().await;
        self.server.stop().await;
    }
}

/// Poll `cond` every 20 ms for up to 30 s.
pub async fn until(what: &str, cond: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
