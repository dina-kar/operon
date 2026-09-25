//! The M0 kill -9 crash gate (M0.4 plan Task 5, rulings 2 and 1).
//!
//! For every named failpoint, `operon dev` runs as a child process with the
//! point armed to `abort()` on its n-th hit, while a client produces
//! numbered records to several partitions and a link sums them. After the
//! abort the process restarts without failpoints, the background work
//! settles, and the gate checks that every acknowledged record is readable
//! exactly once at its acknowledged offset, offsets are dense, the link's
//! sums equal the log's records exactly once, and the metastore's
//! invariants hold. A random-time SIGKILL loop does the same.
//!
//! The collection scenario (plan M1.1 Task 13) does the same for a
//! collection: document ops are produced to its implicit stream, and after
//! the abort and a restart every acknowledged record must be in the stream
//! at its offset, the committed collection must equal the fold of the
//! stream (`fold_stream`, `verify_collection`) and the metastore's
//! invariants must hold. Its rows cover every collection commit failpoint
//! and both index-build failpoints.
//!
//! Runs only with `--features failpoints`:
//! `cargo test -p operon --features failpoints --test crash`.
//! `CRASH_KILLS=<n>` sets the SIGKILL loops' iterations (default 20, and 10
//! for the collection loop).
#![cfg(feature = "failpoints")]

use std::collections::{BTreeMap, BTreeSet};
use std::io::BufRead;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use operon_cache::{RangeCache, RangeCacheConfig};
use operon_collection::{
    CollectionConfig, CollectionContext, CollectionManifest, CollectionSchema, DocOp, Document,
    DynamicMapping, FieldKind, FieldSpec, LanceConfig, LanceEnv, ManifestCache, PrimaryKey,
    VectorSpec, fold_stream, live_manifest, partition_of, verify_collection,
};
use operon_common::{CollectionId, NamespaceId, StreamId};
use operon_log::{FetchRequest, LogReader};
use operon_meta::{
    Consistency, MetaClient, MetaClientConfig, MetaConfig, MetaNode, Router, SystemClock,
};
use operon_store::Store;
use reqwest::StatusCode;
use serde_json::{Value, json};
use tempfile::TempDir;

const EVENTS_PARTITIONS: u32 = 4;
const LOGS_PARTITIONS: u32 = 2;
/// How long a failpoint may take to be hit, and background work to settle.
const WAIT: Duration = Duration::from_secs(90);

/// A running `operon dev` child process.
struct Dev {
    child: Child,
    base: String,
    /// Standard error lines of a run with an armed failpoint.
    stderr: Arc<Mutex<Vec<String>>>,
}

impl Dev {
    fn start(dir: &Path, failpoint: Option<(&str, u32)>) -> Self {
        // Through `sh`, to turn core dumps off: an armed failpoint aborts,
        // and dumping the core of a binary this large (Lance, DataFusion)
        // can take the host's core handler most of a minute, while the
        // process has stopped serving. `exec` keeps the child's pid the
        // server's.
        let mut command = Command::new("sh");
        command
            .args(["-c", "ulimit -c 0 && exec \"$0\" \"$@\""])
            .arg(env!("CARGO_BIN_EXE_operon"))
            .args([
                "dev",
                "--listen",
                "127.0.0.1:0",
                "--flush-interval-ms",
                "10",
            ])
            .arg("--data-dir")
            .arg(dir)
            .args([
                "--segment-min-bytes",
                "1",
                "--poll-interval-ms",
                "50",
                "--lease-ttl-ms",
                "1500",
                "--retention-interval-ms",
                "200",
                "--gc-grace-ms",
                "1500",
                "--gc-interval-ms",
                "300",
                "--link-batch-interval-ms",
                "0",
                "--link-batch-records",
                "25",
                "--snapshot-every",
                "64",
                "--collection-trim",
                "false",
                "--collection-index-min-rows",
                "40",
                "--collection-index-delta-min-rows",
                "40",
                "--collection-index-poll-interval-ms",
                "200",
            ])
            .env("RUST_LOG", "error")
            .env_remove("OPERON_FAILPOINTS")
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        if let Some((name, hit)) = failpoint {
            command
                .env("OPERON_FAILPOINTS", name)
                .env("OPERON_FAILPOINT_HIT", hit.to_string())
                .stderr(Stdio::piped());
        }
        let mut child = command.spawn().expect("spawn operon");
        let stdout = child.stdout.take().expect("stdout");
        let mut lines = std::io::BufReader::new(stdout).lines();
        let base = loop {
            let line = lines
                .next()
                .expect("operon exited before listening")
                .expect("read stdout");
            if let Some(url) = line.strip_prefix("operon listening on ") {
                break url.trim().to_string();
            }
        };
        std::thread::spawn(move || for _ in lines {});
        let stderr = Arc::new(Mutex::new(Vec::new()));
        if let Some(pipe) = child.stderr.take() {
            let stderr = stderr.clone();
            std::thread::spawn(move || {
                for line in std::io::BufReader::new(pipe).lines().map_while(Result::ok) {
                    stderr.lock().expect("lock").push(line);
                }
            });
        }
        Self {
            child,
            base,
            stderr,
        }
    }

    /// Whether the process reported aborting at `point`.
    fn aborted_at(&self, point: &str) -> bool {
        let wanted = format!("failpoint {point} hit");
        self.stderr
            .lock()
            .expect("lock")
            .iter()
            .any(|line| line.contains(&wanted))
    }

    fn exited(&mut self) -> bool {
        self.child.try_wait().expect("try_wait").is_some()
    }

    /// SIGKILL.
    fn kill(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Dev {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[derive(Clone)]
struct Api {
    base: String,
    http: reqwest::Client,
}

fn unb64(v: &Value) -> String {
    let bytes = BASE64
        .decode(v.as_str().expect("base64 string"))
        .expect("base64");
    String::from_utf8(bytes).expect("utf-8")
}

impl Api {
    fn new(dev: &Dev) -> Self {
        Self {
            base: dev.base.clone(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("client"),
        }
    }

    async fn post(&self, path: &str, body: Value) -> Result<(StatusCode, Value), reqwest::Error> {
        let response = self
            .http
            .post(format!("{}{path}", self.base))
            .json(&body)
            .send()
            .await?;
        let status = response.status();
        Ok((status, response.json().await.unwrap_or(Value::Null)))
    }

    async fn get(&self, path: &str) -> Result<(StatusCode, Value), reqwest::Error> {
        let response = self.http.get(format!("{}{path}", self.base)).send().await?;
        let status = response.status();
        Ok((status, response.json().await.unwrap_or(Value::Null)))
    }

    /// Creates the namespace, the two streams and the link (a 409 means an
    /// earlier incarnation created it).
    async fn setup(&self) {
        let created = |status: StatusCode| {
            assert!(
                status == StatusCode::CREATED || status == StatusCode::CONFLICT,
                "{status}"
            );
        };
        let (status, _) = self
            .post("/v1/namespaces", json!({ "name": "acme" }))
            .await
            .expect("namespace");
        created(status);
        let (status, _) = self
            .post(
                "/v1/namespaces/acme/streams",
                json!({ "name": "events", "partitions": EVENTS_PARTITIONS }),
            )
            .await
            .expect("stream");
        created(status);
        let (status, _) = self
            .post(
                "/v1/namespaces/acme/streams",
                json!({
                    "name": "logs",
                    "partitions": LOGS_PARTITIONS,
                    "retention": { "max_bytes": 600 },
                }),
            )
            .await
            .expect("stream");
        created(status);
        let (status, _) = self
            .post(
                "/v1/namespaces/acme/links",
                json!({ "name": "counts", "source": "events" }),
            )
            .await
            .expect("link");
        created(status);
    }

    /// Produces one record; `Some(offset)` once acknowledged, `None` if the
    /// outcome is unknown (an error or a dead server).
    async fn produce(&self, stream: &str, partition: u32, key: &str, value: &str) -> Option<u64> {
        self.produce_bytes(stream, partition, key.as_bytes(), value.as_bytes())
            .await
    }

    /// [`Api::produce`] of a binary key and value.
    async fn produce_bytes(
        &self,
        stream: &str,
        partition: u32,
        key: &[u8],
        value: &[u8],
    ) -> Option<u64> {
        let body =
            json!({ "records": [{ "key": BASE64.encode(key), "value": BASE64.encode(value) }] });
        let path = format!("/v1/namespaces/acme/streams/{stream}/partitions/{partition}/records");
        match self.post(&path, body).await {
            Ok((StatusCode::OK, body)) => body["base_offset"].as_u64(),
            _ => None,
        }
    }

    /// `(log_start, high_watermark)` per partition.
    async fn partitions(&self, stream: &str) -> Vec<(u64, u64)> {
        let (status, body) = self
            .get(&format!("/v1/namespaces/acme/streams/{stream}"))
            .await
            .expect("describe");
        assert_eq!(status, StatusCode::OK, "{body}");
        body["partitions"]
            .as_array()
            .expect("partitions")
            .iter()
            .map(|p| {
                (
                    p["log_start_offset"].as_u64().expect("start"),
                    p["high_watermark"].as_u64().expect("hwm"),
                )
            })
            .collect()
    }

    /// Every record of a partition from `from`: `(offset, key, value)`.
    async fn fetch_all(
        &self,
        stream: &str,
        partition: u32,
        from: u64,
    ) -> Result<Vec<(u64, String, String)>, u64> {
        let mut out = Vec::new();
        let mut offset = from;
        loop {
            let path = format!(
                "/v1/namespaces/acme/streams/{stream}/partitions/{partition}/records?offset={offset}&max_bytes=65536"
            );
            let (status, body) = self.get(&path).await.expect("fetch");
            if status == StatusCode::RANGE_NOT_SATISFIABLE {
                return Err(body["log_start_offset"].as_u64().expect("log start"));
            }
            assert_eq!(status, StatusCode::OK, "{body}");
            let records = body["records"].as_array().expect("records");
            if records.is_empty() {
                return Ok(out);
            }
            for r in records {
                out.push((
                    r["offset"].as_u64().expect("offset"),
                    unb64(&r["key"]),
                    unb64(&r["value"]),
                ));
            }
            offset = body["next_offset"].as_u64().expect("next");
        }
    }

    /// The link: `(applied per partition, counters, skipped)`.
    async fn link(&self) -> (BTreeMap<u32, u64>, BTreeMap<String, i64>, u64) {
        let body = self.describe_link("counts").await;
        let counters = body["counters"]
            .as_object()
            .expect("counters")
            .iter()
            .map(|(k, v)| (k.clone(), v.as_i64().expect("i64")))
            .collect();
        (
            applied(&body),
            counters,
            body["skipped"].as_u64().expect("skipped"),
        )
    }

    async fn describe_link(&self, name: &str) -> Value {
        let (status, body) = self
            .get(&format!("/v1/namespaces/acme/links/{name}"))
            .await
            .expect("link");
        assert_eq!(status, StatusCode::OK, "{body}");
        body
    }

    /// The high watermarks of `stream`'s non-empty partitions.
    async fn high_watermarks(&self, stream: &str) -> BTreeMap<u32, u64> {
        self.partitions(stream)
            .await
            .into_iter()
            .enumerate()
            .map(|(p, (_, hwm))| (u32::try_from(p).expect("u32"), hwm))
            .filter(|(_, hwm)| *hwm > 0)
            .collect()
    }
}

/// A link description's applied offsets, of non-empty partitions.
fn applied(body: &Value) -> BTreeMap<u32, u64> {
    body["applied"]
        .as_array()
        .expect("applied")
        .iter()
        .map(|a| {
            (
                u32::try_from(a["partition"].as_u64().expect("p")).expect("u32"),
                a["offset"].as_u64().expect("offset"),
            )
        })
        .filter(|(_, offset)| *offset > 0)
        .collect()
}

/// What the client knows: acknowledged records (stream, partition, offset →
/// value) and records whose outcome is unknown (stream, partition, value).
/// Events records carry a counter name and a unique delta, so a duplicate is
/// visible; logs records a unique value.
#[derive(Clone, Default)]
struct Model {
    acked: BTreeMap<(String, u32), BTreeMap<u64, String>>,
    unknown: BTreeSet<(String, u32, String)>,
    next: u64,
}

/// Produces the next numbered record and records its outcome.
async fn produce_one(api: &Api, model: &Mutex<Model>) {
    let n = {
        let mut m = model.lock().expect("lock");
        m.next += 1;
        m.next
    };
    let (stream, partitions) = if n % 3 == 0 {
        ("logs", LOGS_PARTITIONS)
    } else {
        ("events", EVENTS_PARTITIONS)
    };
    let partition = u32::try_from(n).expect("u32") % partitions;
    let key = format!("c{}", n % 5);
    let value = if stream == "events" {
        n.to_string()
    } else {
        format!("log-{n}")
    };
    let offset = api.produce(stream, partition, &key, &value).await;
    let mut m = model.lock().expect("lock");
    match offset {
        Some(offset) => {
            m.acked
                .entry((stream.to_string(), partition))
                .or_default()
                .insert(offset, value);
        }
        None => {
            m.unknown.insert((stream.to_string(), partition, value));
        }
    }
}

/// Waits until the link has applied every committed record.
async fn settle(api: &Api) {
    let deadline = Instant::now() + WAIT;
    loop {
        let hwms: BTreeMap<u32, u64> = api
            .partitions("events")
            .await
            .into_iter()
            .enumerate()
            .map(|(p, (_, hwm))| (u32::try_from(p).expect("u32"), hwm))
            .filter(|(_, hwm)| *hwm > 0)
            .collect();
        let (applied, _, _) = api.link().await;
        if applied == hwms {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the link never caught up: applied {applied:?}, high watermarks {hwms:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The gate's assertions against a settled server.
async fn check(api: &Api, model: &Model, what: &str) {
    let mut sums: BTreeMap<String, i64> = BTreeMap::new();
    let mut seen_values = BTreeSet::new();
    for (stream, partitions) in [("events", EVENTS_PARTITIONS), ("logs", LOGS_PARTITIONS)] {
        let bounds = api.partitions(stream).await;
        for partition in 0..partitions {
            let (start, hwm) = bounds[partition as usize];
            if stream == "events" {
                assert_eq!(start, 0, "{what}: events is never trimmed");
            }
            let records = match api.fetch_all(stream, partition, start).await {
                Ok(records) => records,
                Err(start) => api
                    .fetch_all(stream, partition, start)
                    .await
                    .unwrap_or_else(|_| panic!("{what}: {stream}/{partition} keeps moving")),
            };
            let first = records.first().map_or(hwm, |r| r.0);
            // Dense offsets up to the high watermark.
            for (i, (offset, _, _)) in records.iter().enumerate() {
                assert_eq!(
                    *offset,
                    first + i as u64,
                    "{what}: {stream}/{partition} offsets are not dense"
                );
            }
            assert_eq!(
                first + records.len() as u64,
                hwm,
                "{what}: {stream}/{partition} ends before its high watermark"
            );
            let by_offset: BTreeMap<u64, &str> =
                records.iter().map(|(o, _, v)| (*o, v.as_str())).collect();
            let key = (stream.to_string(), partition);
            let acked = model.acked.get(&key).cloned().unwrap_or_default();
            for (offset, value) in &acked {
                if *offset < first {
                    continue; // trimmed by retention
                }
                assert_eq!(
                    by_offset.get(offset).copied(),
                    Some(value.as_str()),
                    "{what}: acknowledged {stream}/{partition}@{offset} is missing or different"
                );
            }
            let acked_values: BTreeSet<&str> = acked.values().map(String::as_str).collect();
            for (offset, key_name, value) in &records {
                assert!(
                    seen_values.insert((stream, value.clone())),
                    "{what}: {stream}/{partition}@{offset} duplicates {value}"
                );
                assert!(
                    acked_values.contains(value.as_str())
                        || model
                            .unknown
                            .contains(&(stream.to_string(), partition, value.clone())),
                    "{what}: {stream}/{partition}@{offset} holds {value}, which was never produced there"
                );
                if stream == "events" {
                    *sums.entry(key_name.clone()).or_default() +=
                        value.parse::<i64>().expect("delta");
                }
            }
        }
    }
    let (applied, counters, skipped) = api.link().await;
    assert_eq!(skipped, 0, "{what}: the link skipped records");
    assert_eq!(
        counters, sums,
        "{what}: the link's sums are not exactly once"
    );
    let hwms: BTreeMap<u32, u64> = api
        .partitions("events")
        .await
        .into_iter()
        .enumerate()
        .map(|(p, (_, hwm))| (u32::try_from(p).expect("u32"), hwm))
        .filter(|(_, hwm)| *hwm > 0)
        .collect();
    assert_eq!(applied, hwms, "{what}: applied offsets");
}

/// Opens the stopped server's metastore in this process and checks its
/// invariants.
async fn check_meta(dir: &Path, what: &str) {
    let bucket = url::Url::from_directory_path(dir.join("bucket").canonicalize().expect("bucket"))
        .expect("url");
    let store = Store::from_url(bucket.as_str(), Vec::<(String, String)>::new()).expect("store");
    let node = MetaNode::start(MetaConfig::new(1, dir.join("meta"), store), &Router::new())
        .await
        .expect("open the metastore");
    let violations = node
        .read(Consistency::Local, |s| s.check_invariants())
        .await
        .expect("read");
    assert!(violations.is_empty(), "{what}: {violations:?}");
    node.shutdown().await.expect("shutdown");
}

/// Arms `point` to abort on its `hit`-th hit, drives load until the process
/// dies, restarts it, and checks everything.
async fn crash_at(point: &str, hit: u32) {
    let dir = TempDir::new().expect("temp dir");
    let mut dev = Dev::start(dir.path(), Some((point, hit)));
    let api = Api::new(&dev);
    api.setup().await;
    let model = Arc::new(Mutex::new(Model::default()));
    let deadline = Instant::now() + WAIT;
    while !dev.exited() {
        assert!(
            Instant::now() < deadline,
            "{point} was not hit {hit} times within {WAIT:?}"
        );
        for _ in 0..8 {
            produce_one(&api, &model).await;
        }
    }
    // The reader thread may still be draining the pipe.
    let deadline = Instant::now() + Duration::from_secs(5);
    while !dev.aborted_at(point) {
        assert!(
            Instant::now() < deadline,
            "the process exited, but not at {point}: {:?}",
            dev.stderr.lock().expect("lock")
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    dev.kill();

    let dev = Dev::start(dir.path(), None);
    let api = Api::new(&dev);
    // The restarted server keeps taking writes.
    for _ in 0..20 {
        produce_one(&api, &model).await;
    }
    settle(&api).await;
    let snapshot = model.lock().expect("lock").clone();
    assert!(
        !snapshot.acked.is_empty(),
        "{point}: nothing was acknowledged"
    );
    check(&api, &snapshot, point).await;
    dev.kill();
    check_meta(dir.path(), point).await;
}

macro_rules! crash_tests {
    ($($name:ident: $point:literal @ $hit:literal,)*) => {
        $(
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $name() {
                crash_at($point, $hit).await;
            }
        )*
    };
}

crash_tests! {
    abort_after_the_wal_put: "wal.after_put" @ 30,
    abort_after_the_wal_commit: "wal.after_commit" @ 30,
    abort_after_the_segment_put: "seg.after_put" @ 3,
    abort_after_the_segment_swap: "seg.after_swap" @ 3,
    abort_after_the_link_data_put: "link.after_data_put" @ 3,
    abort_after_the_link_manifest_put: "link.after_manifest_put" @ 3,
    abort_after_the_link_cas: "link.after_cas" @ 3,
    abort_after_gc_deletes: "gc.after_delete" @ 1,
    abort_after_the_snapshot_put: "meta.snapshot.after_put" @ 1,
    abort_after_the_snapshot_pointer: "meta.snapshot.after_pointer" @ 1,
    abort_after_a_retention_trim: "retention.after_trim" @ 2,
}

/// A deterministic pseudo-random sequence for the SIGKILL loop.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, bound: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % bound
    }
}

/// Ruling 2: SIGKILL at random times under load covers points nobody named.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn random_sigkills_under_load_lose_nothing() {
    let kills: u32 = std::env::var("CRASH_KILLS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);
    let seed: u64 = std::env::var("CRASH_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0x5eed);
    eprintln!("random SIGKILL loop: {kills} kills, seed {seed}");
    let mut rng = Lcg(seed);
    let dir = TempDir::new().expect("temp dir");
    let model = Arc::new(Mutex::new(Model::default()));
    for kill in 0..kills {
        let dev = Dev::start(dir.path(), None);
        let api = Api::new(&dev);
        api.setup().await;
        let run_for = Duration::from_millis(200 + rng.next(1_300));
        let until = Instant::now() + run_for;
        while Instant::now() < until {
            let batch: Vec<_> = (0..4)
                .map(|_| {
                    let api = api.clone();
                    let model = model.clone();
                    tokio::spawn(async move { produce_one(&api, &model).await })
                })
                .collect();
            for handle in batch {
                handle.await.expect("producer");
            }
        }
        dev.kill();
        if kill % 5 == 4 {
            // Every few kills, settle and check everything so far.
            let dev = Dev::start(dir.path(), None);
            let api = Api::new(&dev);
            settle(&api).await;
            let snapshot = model.lock().expect("lock").clone();
            check(&api, &snapshot, &format!("after kill {kill}")).await;
            dev.kill();
        }
    }
    let dev = Dev::start(dir.path(), None);
    let api = Api::new(&dev);
    settle(&api).await;
    let snapshot = model.lock().expect("lock").clone();
    check(&api, &snapshot, "after the last kill").await;
    dev.kill();
    check_meta(dir.path(), "after the last kill").await;
}

// The collection scenario (plan M1.1 Task 13).

const COLLECTION_PARTITIONS: u32 = 3;
/// Keys `k0..k399`, produced in order. At least 256 of them must be live for
/// a vector index to be trained (Lance's 8-bit PQ, controller ruling P1), so
/// the index rows really build an index: one pass over the keys leaves
/// about 320.
const COLLECTION_KEYS: u64 = 400;

/// The collection the scenario writes to.
#[derive(Clone)]
struct Docs {
    ns: NamespaceId,
    cid: CollectionId,
    stream: StreamId,
    /// The implicit stream's and link's name, `_collection.docs.<cid>`.
    name: String,
    schema: CollectionSchema,
}

/// What the client knows of the collection: every acknowledged record
/// `(partition, offset) → (key, value)`, and the next op number.
#[derive(Clone, Default)]
struct DocModel {
    acked: BTreeMap<(u32, u64), (Vec<u8>, Vec<u8>)>,
    next: u64,
}

fn bucket_store(dir: &Path) -> Store {
    let bucket = dir.join("bucket");
    std::fs::create_dir_all(&bucket).expect("bucket dir");
    let url = url::Url::from_directory_path(bucket.canonicalize().expect("bucket")).expect("url");
    Store::from_url(url.as_str(), Vec::<(String, String)>::new()).expect("store")
}

/// The stopped server's metastore, opened in this process, with a client.
async fn open_meta(dir: &Path, store: &Store) -> (MetaNode, MetaClient) {
    let node = MetaNode::start(
        MetaConfig::new(1, dir.join("meta"), store.clone()),
        &Router::new(),
    )
    .await
    .expect("open the metastore");
    node.initialize([1]).await.expect("initialize");
    node.wait_for_leader(WAIT).await.expect("leader");
    let meta = MetaClient::new(
        node.clone(),
        Vec::new(),
        Arc::new(SystemClock),
        MetaClientConfig::default(),
    );
    (node, meta)
}

fn field(name: &str, kind: FieldKind) -> FieldSpec {
    FieldSpec {
        name: name.to_string(),
        source_path: name.to_string(),
        kind,
        indexed: true,
        fast: true,
        ignore_malformed: false,
    }
}

/// Before the first start: namespace `acme` and collection `docs` (3
/// partitions; `tag` Keyword, `n` I64; vector `v` of dim 4, Cosine, `Auto`;
/// dynamic mapping `Ignore`), created in this process.
async fn create_docs(dir: &Path) -> Docs {
    let store = bucket_store(dir);
    let (node, meta) = open_meta(dir, &store).await;
    let ns = meta.create_namespace("acme").await.expect("namespace");
    let vector = VectorSpec {
        name: "v".to_string(),
        dim: 4,
        distance: operon_collection::Distance::Cosine,
        element: operon_collection::VectorElement::F32,
        index: operon_collection::VectorIndexSpec::Auto,
        hnsw: operon_collection::HnswParams::default(),
        quantization: None,
    };
    let schema = CollectionSchema::new(
        vec![field("tag", FieldKind::Keyword), field("n", FieldKind::I64)],
        vec![vector],
        DynamicMapping::Ignore,
    );
    schema.validate().expect("valid schema");
    let (cid, stream, _) = meta
        .create_collection(ns, "docs", schema.clone(), COLLECTION_PARTITIONS)
        .await
        .expect("collection");
    node.shutdown().await.expect("shutdown");
    Docs {
        ns,
        cid,
        stream,
        name: operon_meta::implicit_name("docs", cid),
        schema,
    }
}

/// Whether op `n` is a delete (one in five, spread over the keys).
fn is_delete(n: u64) -> bool {
    let mut x = n.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    x ^= x >> 29;
    x.is_multiple_of(5)
}

/// Op `n`: on key `k<n % 400>`, 80 % `Upsert { tag, n, v }`, 20 % `Delete`.
fn doc_op(n: u64) -> DocOp {
    let i = n % COLLECTION_KEYS;
    let pk = PrimaryKey::Str(format!("k{i}"));
    if is_delete(n) {
        return DocOp::Delete(pk);
    }
    let source = serde_json::Map::from_iter([
        ("tag".to_string(), json!(format!("t{}", i % 3))),
        ("n".to_string(), json!(n)),
    ]);
    let vector = vec![i as f32, 1.0, 0.0, 0.0];
    DocOp::Upsert(Document {
        pk,
        source,
        vectors: BTreeMap::from([("v".to_string(), vector)]),
        sparse_vectors: BTreeMap::new(),
    })
}

/// Produces the next op to its partition of the implicit stream and
/// records it if acknowledged.
async fn produce_doc(api: &Api, docs: &Docs, model: &Mutex<DocModel>) {
    let n = {
        let mut m = model.lock().expect("lock");
        m.next += 1;
        m.next
    };
    let op = doc_op(n);
    let partition = partition_of(op.pk(), COLLECTION_PARTITIONS);
    let record = operon_collection::encode(&op).expect("encode");
    let key = record.key.expect("a key").to_vec();
    let value = record.value.expect("a value").to_vec();
    if let Some(offset) = api.produce_bytes(&docs.name, partition, &key, &value).await {
        model
            .lock()
            .expect("lock")
            .acked
            .insert((partition, offset), (key, value));
    }
}

/// Waits until the collection's link has applied its whole stream.
async fn settle_docs(api: &Api, docs: &Docs) {
    let deadline = Instant::now() + WAIT;
    loop {
        let hwms = api.high_watermarks(&docs.name).await;
        let applied = applied(&api.describe_link(&docs.name).await);
        if applied == hwms {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the collection link never caught up: applied {applied:?}, high watermarks {hwms:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Every record of the implicit stream, from offset 0 (it is never
/// trimmed: `--collection-trim false`).
async fn read_stream(reader: &LogReader, stream: StreamId) -> Vec<(u32, operon_log::OffsetRecord)> {
    let mut out = Vec::new();
    for partition in 0..COLLECTION_PARTITIONS {
        let mut offset = 0;
        loop {
            let response = reader
                .fetch(FetchRequest {
                    stream,
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
        }
    }
    out
}

/// The gate's collection checks against the stopped server's data, in this
/// process: every acknowledged record is in the stream at its offset, and
/// the committed collection is the fold of the stream. Returns the live
/// manifest.
async fn check_docs(dir: &Path, docs: &Docs, model: &DocModel, what: &str) -> CollectionManifest {
    let store = bucket_store(dir);
    let (node, meta) = open_meta(dir, &store).await;
    let cache = RangeCache::new(
        store.clone(),
        RangeCacheConfig {
            memory_bytes: 64 << 20,
            ..RangeCacheConfig::default()
        },
    )
    .await
    .expect("cache");
    let reader = LogReader::new(meta.clone(), cache.clone());
    let records = read_stream(&reader, docs.stream).await;
    let by_offset: BTreeMap<(u32, u64), &operon_log::Record> = records
        .iter()
        .map(|(partition, r)| ((*partition, r.offset), &r.record))
        .collect();
    for ((partition, offset), (key, value)) in &model.acked {
        let found = by_offset.get(&(*partition, *offset));
        assert!(
            found.is_some_and(|r| r.key.as_deref() == Some(key.as_slice())
                && r.value.as_deref() == Some(value.as_slice())),
            "{what}: acknowledged {}/{partition}@{offset} is missing or different",
            docs.name
        );
    }
    let expected = fold_stream(&docs.schema, COLLECTION_PARTITIONS, &records);
    let config = CollectionConfig::default();
    let ctx = CollectionContext {
        meta: meta.clone(),
        store: store.clone(),
        cache: cache.clone(),
        lance: LanceEnv::new(store.clone(), LanceConfig::default()),
        manifests: ManifestCache::new(config.manifest_cache_entries),
        config,
    };
    let problems = verify_collection(&ctx, docs.ns, docs.cid, &expected)
        .await
        .expect("verify");
    assert!(problems.is_empty(), "{what}: {problems:#?}");
    let manifest = live_manifest(
        &ctx.meta,
        &ctx.store,
        &ctx.manifests,
        docs.ns,
        docs.cid,
        Consistency::Linearizable,
    )
    .await
    .expect("live manifest")
    .map_or_else(
        || CollectionManifest::empty(docs.cid),
        |(_, m)| (*m).clone(),
    );
    assert!(
        !expected.is_empty() && manifest.version > 0,
        "{what}: nothing was committed"
    );
    cache.close().await.expect("close the cache");
    node.shutdown().await.expect("shutdown");
    manifest
}

/// Arms `point` to abort on its `hit`-th hit, produces document ops until
/// the process dies, restarts it, and checks everything. An index-build
/// point is checked until the restarted server has built the index.
async fn collection_crash_at(point: &str, hit: u32) {
    let dir = TempDir::new().expect("temp dir");
    let docs = create_docs(dir.path()).await;
    let mut dev = Dev::start(dir.path(), Some((point, hit)));
    let api = Api::new(&dev);
    let model = Mutex::new(DocModel::default());
    let deadline = Instant::now() + WAIT;
    while !dev.exited() {
        assert!(
            Instant::now() < deadline,
            "{point} was not hit {hit} times within {WAIT:?}"
        );
        for _ in 0..8 {
            produce_doc(&api, &docs, &model).await;
        }
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while !dev.aborted_at(point) {
        assert!(
            Instant::now() < deadline,
            "the process exited, but not at {point}: {:?}",
            dev.stderr.lock().expect("lock")
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    dev.kill();

    let index_point = point.starts_with("collection.index.");
    let deadline = Instant::now() + WAIT;
    loop {
        let dev = Dev::start(dir.path(), None);
        let api = Api::new(&dev);
        // The restarted server keeps taking writes.
        for _ in 0..20 {
            produce_doc(&api, &docs, &model).await;
        }
        settle_docs(&api, &docs).await;
        if index_point {
            // Give the index build a moment before the check.
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        dev.kill();
        let snapshot = model.lock().expect("lock").clone();
        let manifest = check_docs(dir.path(), &docs, &snapshot, point).await;
        if !index_point || !manifest.vector_indexes.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{point}: the restarted server never built the vector index"
        );
    }
    check_meta(dir.path(), point).await;
}

macro_rules! collection_crash_tests {
    ($($name:ident: $point:literal @ $hit:literal,)*) => {
        $(
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $name() {
                collection_crash_at($point, $hit).await;
            }
        )*
    };
}

collection_crash_tests! {
    abort_after_the_collection_lance_commit: "collection.after_lance_commit" @ 3,
    abort_after_the_collection_split_put: "collection.after_split_put" @ 3,
    abort_after_the_collection_manifest_put: "collection.after_manifest_put" @ 3,
    abort_after_the_collection_cas: "collection.after_cas" @ 3,
    abort_after_the_collection_pk_write: "collection.after_pk_write" @ 3,
    abort_after_the_index_lance_commit: "collection.index.after_lance_commit" @ 1,
    abort_after_the_index_cas: "collection.index.after_cas" @ 1,
}

/// Ruling 2 for collections: SIGKILL at random times under the collection
/// workload.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn random_sigkills_under_collection_load_lose_nothing() {
    let kills: u32 = std::env::var("CRASH_KILLS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);
    let seed: u64 = std::env::var("CRASH_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0x5eed);
    eprintln!("random SIGKILL loop under collection load: {kills} kills, seed {seed}");
    let mut rng = Lcg(seed);
    let dir = TempDir::new().expect("temp dir");
    let docs = create_docs(dir.path()).await;
    let model = Arc::new(Mutex::new(DocModel::default()));
    for kill in 0..kills {
        let dev = Dev::start(dir.path(), None);
        let api = Api::new(&dev);
        let run_for = Duration::from_millis(200 + rng.next(1_300));
        let until = Instant::now() + run_for;
        while Instant::now() < until {
            let batch: Vec<_> = (0..4)
                .map(|_| {
                    let (api, docs, model) = (api.clone(), docs.clone(), model.clone());
                    tokio::spawn(async move { produce_doc(&api, &docs, &model).await })
                })
                .collect();
            for handle in batch {
                handle.await.expect("producer");
            }
        }
        dev.kill();
        if kill % 5 == 4 {
            let dev = Dev::start(dir.path(), None);
            settle_docs(&Api::new(&dev), &docs).await;
            dev.kill();
            let snapshot = model.lock().expect("lock").clone();
            check_docs(dir.path(), &docs, &snapshot, &format!("after kill {kill}")).await;
        }
    }
    let dev = Dev::start(dir.path(), None);
    settle_docs(&Api::new(&dev), &docs).await;
    dev.kill();
    let snapshot = model.lock().expect("lock").clone();
    check_docs(dir.path(), &docs, &snapshot, "after the last kill").await;
    check_meta(dir.path(), "after the last kill").await;
}
