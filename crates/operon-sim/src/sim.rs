//! The seeded cluster simulation.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use bytes::Bytes;
use object_store::memory::InMemory;
use operon_cache::{RangeCache, RangeCacheConfig};
use operon_common::{NamespaceId, StreamId};
use operon_link::{
    CounterTable, CounterTargetFactory, LinkApplySource, LinkConfig, LinkGcRoots, TargetRegistry,
};
use operon_log::gc::{GcConfig, GcSource};
use operon_log::{
    FetchRequest, LogConfig, LogError, LogReader, LogWriter, Record, RetentionConfig,
    RetentionSource, SegmenterConfig, SegmenterSource,
};
use operon_meta::{
    ApplyError, Command, Consistency, MetaClient, MetaClientConfig, MetaConfig, MetaError,
    MetaNode, MetaState, Reply, Router, SystemClock, TargetRef, WalChunk, WalClass,
};
use operon_store::{FaultRates, FaultyStore, Store};
use operon_worker::{Worker, WorkerConfig, WorkerHandle};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use tempfile::TempDir;
use tokio::task::JoinSet;

use crate::linearizability::{
    CasInput, CasOutput, CasRegisterModel, Op, Outcome, SequencerInput, SequencerModel,
    SequencerOutput, check,
};

/// What to simulate.
#[derive(Clone, Debug, PartialEq)]
pub struct SimConfig {
    pub seed: u64,
    /// Workload steps. Default 300.
    pub steps: u32,
    /// Meta nodes. Default 3.
    pub meta_nodes: u8,
    /// Log writers, spread over the meta nodes. Default 2.
    pub writers: u8,
    /// Partitions of each stream. Default 3.
    pub partitions: u32,
    /// Store faults between bursts.
    pub fault_rates: FaultRates,
}

impl SimConfig {
    /// The defaults for `seed`.
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            steps: 300,
            meta_nodes: 3,
            writers: 2,
            partitions: 3,
            fault_rates: FaultRates {
                error: 0.02,
                error_after_apply: 0.01,
                precondition: 0.005,
                delay: 0.05,
                max_delay: Duration::from_millis(20),
            },
        }
    }
}

/// One scheduled action.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    Append {
        writer: usize,
        partition: u32,
        records: u32,
    },
    /// A direct `CommitWal` (possibly a retry of an earlier object).
    Commit {
        client: usize,
        partition: u32,
        object: String,
        records: u32,
    },
    Cas {
        client: usize,
        key: String,
    },
    ReadHwm {
        client: usize,
        partition: u32,
    },
    Fetch {
        partition: u32,
        offset: u64,
    },
    Isolate(u64),
    Heal(u64),
    RestartWorker {
        crash: bool,
        client: usize,
    },
    FaultBurst,
    FaultsCalm,
    Pause(u64),
}

/// The recorded histories: per partition sequencer (`events/<p>` for
/// appends and high-watermark reads, `raw/<p>` for direct commits) and per
/// CAS register key.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Histories {
    pub sequencers: BTreeMap<String, Vec<Op<SequencerInput, SequencerOutput>>>,
    pub registers: BTreeMap<String, Vec<Op<CasInput, CasOutput>>>,
}

/// Counts of what the run did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SimStats {
    pub appends_acked: u64,
    pub appends_unknown: u64,
    pub appends_failed: u64,
    pub commits: u64,
    pub cas: u64,
    pub indeterminate: u64,
    pub fetches: u64,
    pub isolations: u64,
    pub worker_restarts: u64,
    pub fault_bursts: u64,
    pub link_version: u64,
}

/// The result of one run.
#[derive(Clone, Debug)]
pub struct SimReport {
    pub seed: u64,
    pub schedule: Vec<Event>,
    pub histories: Histories,
    pub violations: Vec<String>,
    pub stats: SimStats,
}

impl SimReport {
    pub fn is_ok(&self) -> bool {
        self.violations.is_empty()
    }

    /// The seed, the violations and the full schedule, for a failure report.
    pub fn describe(&self) -> String {
        let mut out = format!("seed {}: {} violations\n", self.seed, self.violations.len());
        for violation in &self.violations {
            let _ = writeln!(out, "  - {violation}");
        }
        let _ = writeln!(out, "stats: {:?}", self.stats);
        let _ = writeln!(out, "schedule ({} events):", self.schedule.len());
        for (i, event) in self.schedule.iter().enumerate() {
            let _ = writeln!(out, "  {i:4} {event:?}");
        }
        out
    }
}

/// Runs one simulation on a fresh single-threaded runtime.
pub fn run(config: SimConfig) -> SimReport {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            return SimReport {
                seed: config.seed,
                schedule: Vec::new(),
                histories: Histories::default(),
                violations: vec![format!("could not build a runtime: {err}")],
                stats: SimStats::default(),
            };
        }
    };
    runtime.block_on(simulate(config))
}

/// Shared by the workload's tasks.
#[derive(Default)]
struct Recorder {
    clock: AtomicU64,
    ids: AtomicU64,
    histories: Mutex<Histories>,
    /// Partition → offset → value, of acknowledged appends to `events`.
    acked: Mutex<BTreeMap<u32, BTreeMap<u64, String>>>,
    /// Values whose append had an unknown outcome.
    unknown: Mutex<BTreeSet<String>>,
    /// Values whose append failed definitely (never committed).
    failed: Mutex<BTreeSet<String>>,
    violations: Mutex<Vec<String>>,
    stats: Mutex<SimStats>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Recorder {
    fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::SeqCst)
    }

    fn id(&self) -> u64 {
        self.ids.fetch_add(1, Ordering::SeqCst)
    }

    fn violation(&self, message: String) {
        lock(&self.violations).push(message);
    }

    fn sequencer(&self, name: String, op: Op<SequencerInput, SequencerOutput>) {
        if op.outcome == Outcome::Indeterminate {
            lock(&self.stats).indeterminate += 1;
        }
        lock(&self.histories)
            .sequencers
            .entry(name)
            .or_default()
            .push(op);
    }

    fn register(&self, name: String, op: Op<CasInput, CasOutput>) {
        if op.outcome == Outcome::Indeterminate {
            lock(&self.stats).indeterminate += 1;
        }
        lock(&self.histories)
            .registers
            .entry(name)
            .or_default()
            .push(op);
    }
}

/// The cluster under test.
struct Cluster {
    router: Router,
    nodes: Vec<MetaNode>,
    clients: Vec<MetaClient>,
    _dirs: Vec<TempDir>,
    faults: Arc<FaultyStore>,
    store: Store,
    ns: NamespaceId,
    events: StreamId,
    raw: StreamId,
    partitions: u32,
}

const WAIT: Duration = Duration::from_secs(60);
const GRACE: Duration = Duration::from_secs(1);

fn client_config() -> MetaClientConfig {
    MetaClientConfig {
        retry_deadline: Duration::from_secs(5),
        backoff: Duration::from_millis(20),
    }
}

/// Retries `op` while it fails, up to `WAIT`.
async fn retry<T, F, Fut>(what: &str, op: F) -> Result<T, String>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, String>>,
{
    let deadline = Instant::now() + WAIT;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) if Instant::now() >= deadline => return Err(format!("{what}: {err}")),
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}

impl Cluster {
    async fn start(config: &SimConfig) -> Result<Self, String> {
        let router = Router::new();
        let meta_store = Store::in_memory();
        let mut nodes = Vec::new();
        let mut dirs = Vec::new();
        let ids: Vec<u64> = (1..=u64::from(config.meta_nodes)).collect();
        for id in &ids {
            let dir = TempDir::new().map_err(|e| e.to_string())?;
            let mut meta_config = MetaConfig::new(*id, dir.path(), meta_store.clone());
            meta_config.snapshot_every = 200;
            meta_config.logs_after_snapshot = 50;
            let node = MetaNode::start(meta_config, &router)
                .await
                .map_err(|e| format!("start node {id}: {e}"))?;
            nodes.push(node);
            dirs.push(dir);
        }
        nodes[0]
            .initialize(ids.clone())
            .await
            .map_err(|e| format!("initialize: {e}"))?;
        nodes[0]
            .wait_for_leader(WAIT)
            .await
            .map_err(|e| format!("no leader: {e}"))?;
        let clients: Vec<MetaClient> = nodes
            .iter()
            .map(|node| {
                MetaClient::new(
                    node.clone(),
                    nodes.clone(),
                    Arc::new(SystemClock),
                    client_config(),
                )
            })
            .collect();
        let faults = Arc::new(FaultyStore::random(
            Arc::new(InMemory::new()),
            config.seed,
            config.fault_rates,
        ));
        let store = Store::new(faults.clone());
        let admin = clients[0].clone();
        let ns = retry("create the namespace", || async {
            match admin.create_namespace("sim").await {
                Ok(id) | Err(MetaError::Rejected(ApplyError::NamespaceExists(id))) => Ok(id),
                Err(err) => Err(err.to_string()),
            }
        })
        .await?;
        let stream = |name: &'static str, class: WalClass| {
            let admin = admin.clone();
            let partitions = config.partitions;
            async move {
                retry("create a stream", || async {
                    match admin.create_stream(ns, name, partitions, class).await {
                        Ok(id) | Err(MetaError::Rejected(ApplyError::StreamExists(id))) => Ok(id),
                        Err(err) => Err(err.to_string()),
                    }
                })
                .await
            }
        };
        let events = stream("events", WalClass::Standard).await?;
        // Direct commits of objects that do not exist in the store: another
        // class, so the segmenter never reads them.
        let raw = stream("raw", WalClass::Express).await?;
        retry("create the link", || async {
            let target = TargetRef {
                kind: "counter".to_string(),
                name: "counts".to_string(),
            };
            match admin
                .create_link(ns, "counts", events, target, BTreeMap::new())
                .await
            {
                Ok(_) | Err(MetaError::Rejected(ApplyError::LinkExists(_))) => Ok(()),
                Err(err) => Err(err.to_string()),
            }
        })
        .await?;
        Ok(Self {
            router,
            nodes,
            clients,
            _dirs: dirs,
            faults,
            store,
            ns,
            events,
            raw,
            partitions: config.partitions,
        })
    }

    async fn reader(&self, client: usize) -> Result<LogReader, String> {
        let cache = RangeCache::new(self.store.clone(), RangeCacheConfig::default())
            .await
            .map_err(|e| e.to_string())?;
        Ok(LogReader::new(self.clients[client].clone(), cache))
    }

    async fn start_worker(&self, client: usize, owner: String) -> Result<WorkerHandle, String> {
        let meta = self.clients[client].clone();
        let reader = self.reader(client).await?;
        let cache = RangeCache::new(self.store.clone(), RangeCacheConfig::default())
            .await
            .map_err(|e| e.to_string())?;
        let mut worker = Worker::new(
            meta,
            WorkerConfig {
                poll_interval: Duration::from_millis(20),
                lease_ttl: Duration::from_secs(1),
                ..WorkerConfig::new(owner)
            },
        );
        let registry = TargetRegistry::new().with(Arc::new(CounterTargetFactory::new(
            self.store.clone(),
            GRACE / 2,
        )));
        worker.add_source(Arc::new(LinkApplySource::new(
            reader,
            registry,
            LinkConfig {
                batch_records: 20,
                batch_interval: Duration::ZERO,
                max_commit_delay: GRACE / 2,
                ..LinkConfig::default()
            },
        )));
        worker.add_source(Arc::new(SegmenterSource::new(
            self.store.clone(),
            cache,
            SegmenterConfig {
                min_bytes: 1,
                target_bytes: 2048,
                swap_deadline: GRACE / 2,
                ..SegmenterConfig::default()
            },
        )));
        worker.add_source(Arc::new(RetentionSource::new(RetentionConfig {
            interval: Duration::from_millis(100),
        })));
        worker.add_source(Arc::new(GcSource::with_roots(
            self.store.clone(),
            GcConfig {
                grace: GRACE,
                interval: Duration::from_millis(200),
                keep_manifests: 3,
                ..GcConfig::default()
            },
            vec![Arc::new(LinkGcRoots)],
        )));
        Ok(worker.start())
    }

    fn link_table(&self, client: usize) -> CounterTable {
        let link = operon_meta::Link {
            id: operon_meta::LinkId(1),
            namespace: self.ns,
            name: "counts".to_string(),
            source: self.events,
            target: TargetRef {
                kind: "counter".to_string(),
                name: "counts".to_string(),
            },
            options: BTreeMap::new(),
        };
        CounterTable::for_link(self.clients[client].clone(), self.store.clone(), &link)
    }
}

/// Appends `records` records with unique values through `writer`.
async fn append(
    rec: Arc<Recorder>,
    writer: LogWriter,
    stream: StreamId,
    partition: u32,
    records: u32,
    client: u32,
) {
    let values: Vec<String> = (0..records).map(|_| rec.id().to_string()).collect();
    let batch: Vec<Record> = values
        .iter()
        .map(|v| Record {
            key: Some(Bytes::from(format!("c{}", v.len() % 5))),
            value: Some(Bytes::from(v.clone())),
            headers: vec![],
            timestamp_ms: -1,
        })
        .collect();
    let object = format!("append-{}", values[0]);
    let invoke = rec.tick();
    let result = writer.append(stream, partition, batch).await;
    let complete = rec.tick();
    let history = format!("events/{partition}");
    match result {
        Ok(ack) => {
            lock(&rec.stats).appends_acked += 1;
            let mut acked = lock(&rec.acked);
            let slot = acked.entry(partition).or_default();
            for (i, value) in values.into_iter().enumerate() {
                slot.insert(ack.base_offset + i as u64, value);
            }
            drop(acked);
            rec.sequencer(
                history,
                Op {
                    client,
                    invoke,
                    complete,
                    input: SequencerInput::Commit { object, records },
                    outcome: Outcome::Ok(SequencerOutput::BaseOffset(ack.base_offset)),
                },
            );
        }
        Err(LogError::CommitUnknown(_)) => {
            lock(&rec.stats).appends_unknown += 1;
            lock(&rec.unknown).extend(values);
            rec.sequencer(
                history,
                Op {
                    client,
                    invoke,
                    complete: u64::MAX,
                    input: SequencerInput::Commit { object, records },
                    outcome: Outcome::Indeterminate,
                },
            );
        }
        // A definite failure: never committed, so never in any history.
        Err(_) => {
            lock(&rec.stats).appends_failed += 1;
            lock(&rec.failed).extend(values);
        }
    }
}

/// A direct `CommitWal` of `object` (one chunk of `records` records).
async fn commit(
    rec: Arc<Recorder>,
    meta: MetaClient,
    stream: StreamId,
    partition: u32,
    object: String,
    records: u32,
    client: u32,
) {
    let command = Command::CommitWal {
        object: object.clone(),
        created_at_ms: meta.now_ms(),
        chunks: vec![WalChunk {
            stream,
            partition,
            records,
            byte_range: 0..u64::from(records) * 10,
            max_timestamp_ms: 0,
        }],
    };
    let invoke = rec.tick();
    let (result, earlier_unknown) = meta.write_tracked(command).await;
    let complete = rec.tick();
    lock(&rec.stats).commits += 1;
    let outcome = match result {
        Ok(Reply::WalCommitted { base_offsets }) if base_offsets.len() == 1 => {
            Outcome::Ok(SequencerOutput::BaseOffset(base_offsets[0]))
        }
        Ok(other) => {
            rec.violation(format!("CommitWal {object}: unexpected reply {other:?}"));
            return;
        }
        // Definitely not applied.
        Err(MetaError::Rejected(_) | MetaError::ClockSkew { .. }) if !earlier_unknown => return,
        Err(_) => Outcome::Indeterminate,
    };
    let complete = if outcome == Outcome::Indeterminate {
        u64::MAX
    } else {
        complete
    };
    rec.sequencer(
        format!("raw/{partition}"),
        Op {
            client,
            invoke,
            complete,
            input: SequencerInput::Commit { object, records },
            outcome,
        },
    );
}

/// A linearizable read of a pointer, then a CAS from what it saw.
async fn cas(rec: Arc<Recorder>, meta: MetaClient, ns: NamespaceId, key: String, client: u32) {
    let history = format!("pointer/{key}");
    let invoke = rec.tick();
    let read = meta
        .read(Consistency::Linearizable, |s| s.pointer(ns, &key).cloned())
        .await;
    let complete = rec.tick();
    let Ok(current) = read else {
        return;
    };
    let seen = current.as_ref().map(|p| (p.version, p.value.clone()));
    rec.register(
        history.clone(),
        Op {
            client,
            invoke,
            complete,
            input: CasInput::Read,
            outcome: Outcome::Ok(CasOutput::Read(seen)),
        },
    );
    let value = format!("v{}", rec.id());
    let expected = current.map(|p| p.version);
    let command = Command::CasPointer {
        namespace: ns,
        key: key.clone(),
        expected,
        value: value.clone(),
        fence: None,
        fresh: None,
    };
    let invoke = rec.tick();
    let (result, earlier_unknown) = meta.write_tracked(command).await;
    let complete = rec.tick();
    lock(&rec.stats).cas += 1;
    let outcome = match result {
        Ok(Reply::PointerSet { version }) => Outcome::Ok(CasOutput::Ok(version)),
        // A mismatch after an attempt with an unknown outcome may be our
        // own first attempt's effect.
        Err(MetaError::Rejected(ApplyError::VersionMismatch { current })) if !earlier_unknown => {
            Outcome::Ok(CasOutput::Mismatch(current.map(|p| (p.version, p.value))))
        }
        Err(MetaError::Rejected(_) | MetaError::ClockSkew { .. }) if !earlier_unknown => return,
        Ok(other) => {
            rec.violation(format!("CasPointer {key}: unexpected reply {other:?}"));
            return;
        }
        Err(_) => Outcome::Indeterminate,
    };
    let complete = if outcome == Outcome::Indeterminate {
        u64::MAX
    } else {
        complete
    };
    rec.register(
        history,
        Op {
            client,
            invoke,
            complete,
            input: CasInput::Cas { expected, value },
            outcome,
        },
    );
}

/// A linearizable read of a partition's high watermark.
async fn read_hwm(
    rec: Arc<Recorder>,
    meta: MetaClient,
    stream: StreamId,
    partition: u32,
    client: u32,
) {
    let invoke = rec.tick();
    let read = meta
        .read(Consistency::Linearizable, |s| {
            s.partition(stream, partition).map(|p| p.high_watermark())
        })
        .await;
    let complete = rec.tick();
    if let Ok(Some(hwm)) = read {
        rec.sequencer(
            format!("events/{partition}"),
            Op {
                client,
                invoke,
                complete,
                input: SequencerInput::ReadHwm,
                outcome: Outcome::Ok(SequencerOutput::Hwm(hwm)),
            },
        );
    }
}

/// A fetch whose records must match what was acknowledged.
async fn fetch(
    rec: Arc<Recorder>,
    reader: LogReader,
    stream: StreamId,
    partition: u32,
    offset: u64,
    max_bytes: usize,
) {
    lock(&rec.stats).fetches += 1;
    let request = FetchRequest {
        stream,
        partition,
        offset,
        max_bytes,
        max_wait: Duration::ZERO,
    };
    // Errors (store faults, a lagging node) are fine; wrong data is not.
    let Ok(response) = reader.fetch(request).await else {
        return;
    };
    let acked = lock(&rec.acked)
        .get(&partition)
        .cloned()
        .unwrap_or_default();
    let failed = lock(&rec.failed).clone();
    for (i, record) in response.records.iter().enumerate() {
        let value = record
            .record
            .value
            .as_ref()
            .map(|v| String::from_utf8_lossy(v).to_string())
            .unwrap_or_default();
        if record.offset != offset + i as u64 {
            rec.violation(format!(
                "fetch events/{partition}@{offset}: record {i} has offset {}",
                record.offset
            ));
        }
        if let Some(expected) = acked.get(&record.offset)
            && *expected != value
        {
            rec.violation(format!(
                "fetch events/{partition}@{}: {value:?}, acknowledged {expected:?}",
                record.offset
            ));
        }
        if failed.contains(&value) {
            rec.violation(format!(
                "fetch events/{partition}@{}: {value:?} belongs to a failed append",
                record.offset
            ));
        }
    }
}

async fn simulate(config: SimConfig) -> SimReport {
    let mut schedule = Vec::new();
    let rec = Arc::new(Recorder::default());
    let cluster = match Cluster::start(&config).await {
        Ok(cluster) => cluster,
        Err(err) => {
            return SimReport {
                seed: config.seed,
                schedule,
                histories: Histories::default(),
                violations: vec![format!("setup: {err}")],
                stats: SimStats::default(),
            };
        }
    };
    let outcome = drive(&config, &cluster, &rec, &mut schedule).await;
    if let Err(err) = outcome {
        rec.violation(err);
    }
    for node in &cluster.nodes {
        if let Err(err) = node.shutdown().await {
            rec.violation(format!("shutting node {} down: {err}", node.id()));
        }
    }
    let histories = lock(&rec.histories).clone();
    let violations = lock(&rec.violations).clone();
    let stats = lock(&rec.stats).clone();
    SimReport {
        seed: config.seed,
        schedule,
        histories,
        violations,
        stats,
    }
}

/// Most workload operations in flight at once.
const MAX_IN_FLIGHT: usize = 24;

async fn drive(
    config: &SimConfig,
    cluster: &Cluster,
    rec: &Arc<Recorder>,
    schedule: &mut Vec<Event>,
) -> Result<(), String> {
    let mut rng = ChaCha8Rng::seed_from_u64(config.seed ^ 0x51_4d_0f_0e);
    let nodes = cluster.nodes.len();
    let mut writers = Vec::new();
    for w in 0..usize::from(config.writers.max(1)) {
        let writer = LogWriter::start(
            cluster.clients[w % nodes].clone(),
            cluster.store.clone(),
            LogConfig {
                flush_interval: Duration::from_millis(5),
                commit_retry_deadline: Duration::from_secs(10),
                ..LogConfig::new(u64::try_from(w).unwrap_or(0) + 1)
            },
        )
        .map_err(|e| e.to_string())?;
        writers.push(writer);
    }
    let reader = cluster.reader(0).await?;
    let mut restarts = 0u64;
    let mut worker = Some(
        cluster
            .start_worker(0, format!("worker-{}-{restarts}", config.seed))
            .await?,
    );
    let burst = FaultRates {
        error: 0.2,
        error_after_apply: 0.05,
        precondition: 0.03,
        delay: 0.2,
        max_delay: Duration::from_millis(50),
    };
    let mut isolated: Option<u64> = None;
    let mut raw_objects: Vec<(u32, String, u32)> = Vec::new();
    let mut in_flight: JoinSet<()> = JoinSet::new();

    for _ in 0..config.steps {
        let roll = rng.random_range(0..100u32);
        let client = rng.random_range(0..nodes);
        let partition = rng.random_range(0..cluster.partitions);
        let event = match roll {
            0..35 => Event::Append {
                writer: rng.random_range(0..writers.len()),
                partition,
                records: rng.random_range(1..=3),
            },
            35..45 => {
                let retry = !raw_objects.is_empty() && rng.random_bool(0.25);
                let (partition, object, records) = if retry {
                    raw_objects[rng.random_range(0..raw_objects.len())].clone()
                } else {
                    let entry = (
                        partition,
                        format!("raw/{}/{}.wal", config.seed, rec.id()),
                        rng.random_range(1..=4),
                    );
                    raw_objects.push(entry.clone());
                    entry
                };
                Event::Commit {
                    client,
                    partition,
                    object,
                    records,
                }
            }
            45..57 => Event::Cas {
                client,
                key: format!("k{}", rng.random_range(0..2)),
            },
            57..63 => Event::ReadHwm { client, partition },
            63..73 => Event::Fetch {
                partition,
                offset: rng.random_range(0..64),
            },
            73..78 => match isolated {
                Some(node) => Event::Heal(node),
                None => Event::Isolate(u64::try_from(rng.random_range(0..nodes)).unwrap_or(0) + 1),
            },
            78..81 => Event::RestartWorker {
                crash: rng.random_bool(0.5),
                client,
            },
            81..85 => {
                if rng.random_bool(0.5) {
                    Event::FaultBurst
                } else {
                    Event::FaultsCalm
                }
            }
            _ => Event::Pause(rng.random_range(1..15)),
        };
        schedule.push(event.clone());
        while in_flight.len() >= MAX_IN_FLIGHT {
            in_flight.join_next().await;
        }
        let client_id = u32::try_from(client).unwrap_or(0);
        match event {
            Event::Append {
                writer,
                partition,
                records,
            } => {
                in_flight.spawn(append(
                    rec.clone(),
                    writers[writer].clone(),
                    cluster.events,
                    partition,
                    records,
                    u32::try_from(writer).unwrap_or(0) + 100,
                ));
            }
            Event::Commit {
                client,
                partition,
                object,
                records,
            } => {
                in_flight.spawn(commit(
                    rec.clone(),
                    cluster.clients[client].clone(),
                    cluster.raw,
                    partition,
                    object,
                    records,
                    client_id,
                ));
            }
            Event::Cas { client, key } => {
                in_flight.spawn(cas(
                    rec.clone(),
                    cluster.clients[client].clone(),
                    cluster.ns,
                    key,
                    client_id,
                ));
            }
            Event::ReadHwm { client, partition } => {
                in_flight.spawn(read_hwm(
                    rec.clone(),
                    cluster.clients[client].clone(),
                    cluster.events,
                    partition,
                    client_id,
                ));
            }
            Event::Fetch { partition, offset } => {
                let max_bytes = if rng.random_bool(0.3) { 1 } else { 4096 };
                in_flight.spawn(fetch(
                    rec.clone(),
                    reader.clone(),
                    cluster.events,
                    partition,
                    offset,
                    max_bytes,
                ));
            }
            Event::Isolate(node) => {
                lock(&rec.stats).isolations += 1;
                cluster.router.isolate(node);
                isolated = Some(node);
            }
            Event::Heal(node) => {
                cluster.router.heal(node);
                isolated = None;
            }
            Event::RestartWorker { crash, client } => {
                lock(&rec.stats).worker_restarts += 1;
                if let Some(handle) = worker.take() {
                    if crash {
                        drop(handle);
                    } else {
                        handle.stop().await;
                    }
                }
                restarts += 1;
                worker = Some(
                    cluster
                        .start_worker(client, format!("worker-{}-{restarts}", config.seed))
                        .await?,
                );
            }
            Event::FaultBurst => {
                lock(&rec.stats).fault_bursts += 1;
                cluster.faults.set_rates(burst);
            }
            Event::FaultsCalm => cluster.faults.set_rates(config.fault_rates),
            Event::Pause(ms) => tokio::time::sleep(Duration::from_millis(ms)).await,
        }
        tokio::time::sleep(Duration::from_millis(rng.random_range(1..4))).await;
    }

    // Wind down: heal, stop faults, let every operation finish.
    if let Some(node) = isolated.take() {
        cluster.router.heal(node);
    }
    cluster.faults.set_rates(FaultRates::none());
    cluster.faults.clear();
    while in_flight.join_next().await.is_some() {}
    for writer in &writers {
        let _ = writer.shutdown().await;
    }
    let worker = match worker {
        Some(worker) => worker,
        None => {
            cluster
                .start_worker(0, format!("worker-{}-final", config.seed))
                .await?
        }
    };
    settle_link(cluster, rec).await;
    worker.stop().await;
    converge(cluster).await?;
    verify(cluster, rec).await;
    Ok(())
}

/// Waits until the link has applied every committed record of `events`.
async fn settle_link(cluster: &Cluster, rec: &Recorder) {
    let deadline = Instant::now() + WAIT;
    let table = cluster.link_table(0);
    let events = cluster.events;
    let partitions = cluster.partitions;
    loop {
        let hwms = cluster.clients[0]
            .read(Consistency::Linearizable, |s| {
                (0..partitions)
                    .filter_map(|p| {
                        let hwm = s.partition(events, p)?.high_watermark();
                        (hwm > 0).then_some((p, hwm))
                    })
                    .collect::<BTreeMap<u32, u64>>()
            })
            .await;
        if let (Ok(hwms), Ok(applied)) = (hwms, table.applied().await)
            && hwms == applied
        {
            return;
        }
        if Instant::now() >= deadline {
            rec.violation("the link never caught up with its stream".to_string());
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Waits until every node holds the same state.
async fn converge(cluster: &Cluster) -> Result<(), String> {
    let deadline = Instant::now() + WAIT;
    loop {
        let mut states: Vec<MetaState> = Vec::new();
        for node in &cluster.nodes {
            states.push(
                node.read(Consistency::Local, MetaState::clone)
                    .await
                    .map_err(|e| e.to_string())?,
            );
        }
        if states.windows(2).all(|w| w[0] == w[1]) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("the meta nodes never converged on one state".to_string());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The end-of-run checks.
async fn verify(cluster: &Cluster, rec: &Recorder) {
    // 1. Linearizability.
    let histories = lock(&rec.histories).clone();
    for (name, history) in &histories.sequencers {
        if let Err(violation) = check(SequencerModel::default(), history) {
            rec.violation(format!("sequencer {name}: {violation}"));
        }
    }
    for (name, history) in &histories.registers {
        if let Err(violation) = check(CasRegisterModel::default(), history) {
            rec.violation(format!("register {name}: {violation}"));
        }
    }

    // 2. Every acknowledged append is readable at its offset, exactly once.
    let reader = match cluster.reader(0).await {
        Ok(reader) => reader,
        Err(err) => return rec.violation(format!("reader: {err}")),
    };
    let acked = lock(&rec.acked).clone();
    let unknown = lock(&rec.unknown).clone();
    let failed = lock(&rec.failed).clone();
    let mut sums: BTreeMap<String, i64> = BTreeMap::new();
    let mut seen = BTreeSet::new();
    for partition in 0..cluster.partitions {
        let mut records = Vec::new();
        let mut offset = 0;
        loop {
            let request = FetchRequest {
                stream: cluster.events,
                partition,
                offset,
                max_bytes: 1 << 16,
                max_wait: Duration::ZERO,
            };
            match reader.fetch(request).await {
                Ok(response) if response.records.is_empty() => break,
                Ok(response) => {
                    offset = response.next_offset;
                    records.extend(response.records);
                }
                Err(err) => {
                    rec.violation(format!("final read of events/{partition}@{offset}: {err}"));
                    break;
                }
            }
        }
        let by_offset: BTreeMap<u64, String> = records
            .iter()
            .map(|r| {
                let value = r
                    .record
                    .value
                    .as_ref()
                    .map(|v| String::from_utf8_lossy(v).to_string())
                    .unwrap_or_default();
                (r.offset, value)
            })
            .collect();
        for (i, record) in records.iter().enumerate() {
            if record.offset != i as u64 {
                rec.violation(format!("events/{partition}: offsets are not dense at {i}"));
                break;
            }
        }
        for (offset, value) in acked.get(&partition).into_iter().flatten() {
            if by_offset.get(offset) != Some(value) {
                rec.violation(format!(
                    "events/{partition}@{offset}: acknowledged {value:?}, read {:?}",
                    by_offset.get(offset)
                ));
            }
        }
        let acked_values: BTreeSet<&String> = acked
            .get(&partition)
            .into_iter()
            .flat_map(|m| m.values())
            .collect();
        for (offset, value) in &by_offset {
            if !seen.insert(value.clone()) {
                rec.violation(format!(
                    "events/{partition}@{offset}: {value:?} appears twice"
                ));
            }
            if failed.contains(value) || !(acked_values.contains(value) || unknown.contains(value))
            {
                rec.violation(format!(
                    "events/{partition}@{offset}: {value:?} was never successfully produced here"
                ));
            }
            let delta: i64 = value.parse().unwrap_or(0);
            *sums.entry(format!("c{}", value.len() % 5)).or_default() += delta;
        }
    }

    // 3. The link's table equals the model.
    match cluster.link_table(0).snapshot().await {
        Ok(snapshot) => {
            lock(&rec.stats).link_version = snapshot.version;
            if snapshot.counters != sums {
                rec.violation(format!(
                    "CounterTable {:?} differs from the model {sums:?}",
                    snapshot.counters
                ));
            }
            if snapshot.skipped != 0 {
                rec.violation(format!("CounterTable skipped {} records", snapshot.skipped));
            }
        }
        Err(err) => rec.violation(format!("reading the CounterTable: {err}")),
    }

    // 4. Invariants on every node, and no fatal Raft error (M0.2 review N4).
    for node in &cluster.nodes {
        match node
            .read(Consistency::Local, MetaState::check_invariants)
            .await
        {
            Ok(violations) => {
                for violation in violations {
                    rec.violation(format!("node {}: {violation}", node.id()));
                }
            }
            Err(err) => rec.violation(format!("node {}: {err}", node.id())),
        }
        if let Some(fatal) = node.fatal_error() {
            rec.violation(format!(
                "node {} stopped on a fatal Raft error: {fatal}",
                node.id()
            ));
        }
    }

    // 5. No dangling reference (M0.4 review M5): every object an index entry
    // or a link pointer names exists in the store (faults are off by now).
    // The raw stream's commits name objects that were never written.
    let (ns, streams, partitions) = (cluster.ns, [cluster.events], cluster.partitions);
    let referenced = cluster.nodes[0]
        .read(Consistency::Local, |s| {
            let mut objects = BTreeSet::new();
            for stream in streams {
                for partition in 0..partitions {
                    if let Some(state) = s.partition(stream, partition) {
                        objects.extend(state.entries().map(|e| e.object.clone()));
                    }
                }
            }
            for link in s.links(ns) {
                if let Some(pointer) = s.pointer(ns, &format!("link/{}", link.id)) {
                    objects.insert(pointer.value.clone());
                }
            }
            objects
        })
        .await;
    match referenced {
        Ok(objects) => {
            for object in objects {
                if let Err(err) = cluster.store.head(&object).await {
                    rec.violation(format!("dangling reference to {object}: {err}"));
                }
            }
        }
        Err(err) => rec.violation(format!("reading references: {err}")),
    }
}
