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
//! Runs only with `--features failpoints`:
//! `cargo test -p operon --features failpoints --test crash`.
//! `CRASH_KILLS=<n>` sets the SIGKILL loop's iterations (default 20).
#![cfg(feature = "failpoints")]

use std::collections::{BTreeMap, BTreeSet};
use std::io::BufRead;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use operon_meta::{Consistency, MetaConfig, MetaNode, Router};
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
        let mut command = Command::new(env!("CARGO_BIN_EXE_operon"));
        command
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

fn b64(s: &str) -> String {
    BASE64.encode(s.as_bytes())
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
        let body = json!({ "records": [{ "key": b64(key), "value": b64(value) }] });
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
        let (status, body) = self
            .get("/v1/namespaces/acme/links/counts")
            .await
            .expect("link");
        assert_eq!(status, StatusCode::OK, "{body}");
        let applied = body["applied"]
            .as_array()
            .expect("applied")
            .iter()
            .map(|a| {
                (
                    u32::try_from(a["partition"].as_u64().expect("p")).expect("u32"),
                    a["offset"].as_u64().expect("offset"),
                )
            })
            .collect();
        let counters = body["counters"]
            .as_object()
            .expect("counters")
            .iter()
            .map(|(k, v)| (k.clone(), v.as_i64().expect("i64")))
            .collect();
        (
            applied,
            counters,
            body["skipped"].as_u64().expect("skipped"),
        )
    }
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
