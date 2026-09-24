//! The worker task framework against a single-node metastore.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use operon_common::NamespaceId;
use operon_meta::{
    ApplyError, Consistency, MetaClient, MetaClientConfig, MetaConfig, MetaError, MetaNode, Router,
    SystemClock,
};
use operon_store::Store;
use operon_worker::{
    Candidate, Priority, RunResult, Task, TaskContext, TaskError, TaskKey, TaskOutcome, TaskSource,
    Worker, WorkerConfig, run_once,
};
use tempfile::TempDir;

const WAIT: Duration = Duration::from_secs(20);

struct Meta {
    node: MetaNode,
    client: MetaClient,
    _dir: TempDir,
}

impl Meta {
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
        let client = MetaClient::new(
            node.clone(),
            vec![],
            Arc::new(SystemClock),
            MetaClientConfig::default(),
        );
        Self {
            node,
            client,
            _dir: dir,
        }
    }

    async fn namespace(&self, name: &str) -> NamespaceId {
        self.client.create_namespace(name).await.expect("namespace")
    }

    async fn shutdown(self) {
        self.node.shutdown().await.expect("shutdown");
    }
}

fn config(owner: &str) -> WorkerConfig {
    WorkerConfig {
        poll_interval: Duration::from_millis(10),
        lease_ttl: Duration::from_secs(3),
        ..WorkerConfig::new(owner)
    }
}

async fn eventually(what: &str, check: impl Fn() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !check() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Proposes a fixed set of tasks on every poll.
struct FixedSource {
    priority: Priority,
    tasks: Mutex<Vec<Candidate>>,
    /// Remove a task from the proposals once it ran.
    once: bool,
}

impl FixedSource {
    fn new(priority: Priority, tasks: Vec<Candidate>, once: bool) -> Arc<Self> {
        Arc::new(Self {
            priority,
            tasks: Mutex::new(tasks),
            once,
        })
    }

    fn remove(&self, key: &TaskKey) {
        if self.once {
            self.tasks.lock().expect("lock").retain(|(k, _)| k != key);
        }
    }
}

#[async_trait]
impl TaskSource for FixedSource {
    fn priority(&self) -> Priority {
        self.priority
    }

    async fn candidates(&self, _meta: &MetaClient) -> Result<Vec<Candidate>, TaskError> {
        Ok(self.tasks.lock().expect("lock").clone())
    }
}

/// Counts overlapping runs of the same key.
#[derive(Default)]
struct Overlap {
    active: Mutex<BTreeMap<String, u32>>,
    violations: AtomicU32,
    runs: AtomicU32,
}

struct OverlapTask {
    key: String,
    overlap: Arc<Overlap>,
}

#[async_trait]
impl Task for OverlapTask {
    async fn run(&self, _ctx: TaskContext) -> Result<TaskOutcome, TaskError> {
        {
            let mut active = self.overlap.active.lock().expect("lock");
            let count = active.entry(self.key.clone()).or_default();
            *count += 1;
            if *count > 1 {
                self.overlap.violations.fetch_add(1, Ordering::SeqCst);
            }
        }
        tokio::time::sleep(Duration::from_millis(3)).await;
        *self
            .overlap
            .active
            .lock()
            .expect("lock")
            .get_mut(&self.key)
            .expect("active") -= 1;
        self.overlap.runs.fetch_add(1, Ordering::SeqCst);
        Ok(TaskOutcome::Done)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_workers_never_run_the_same_key_concurrently() {
    let meta = Meta::start().await;
    let overlap = Arc::new(Overlap::default());
    let tasks = |overlap: &Arc<Overlap>| -> Vec<Candidate> {
        (0..4)
            .map(|i| {
                let key = format!("k{i}");
                let task: Arc<dyn Task> = Arc::new(OverlapTask {
                    key: key.clone(),
                    overlap: overlap.clone(),
                });
                (TaskKey::cluster(key), task)
            })
            .collect()
    };
    let mut handles = Vec::new();
    for owner in ["w1", "w2"] {
        let mut worker = Worker::new(meta.client.clone(), config(owner));
        worker.add_source(FixedSource::new(
            Priority::Maintenance,
            tasks(&overlap),
            false,
        ));
        handles.push(worker.start());
    }
    eventually("300 runs", || overlap.runs.load(Ordering::SeqCst) >= 300).await;
    for handle in handles {
        handle.stop().await;
    }
    assert_eq!(overlap.violations.load(Ordering::SeqCst), 0);
    // Every lease was released on stop.
    let held = meta
        .client
        .read(Consistency::Local, |s| {
            (0..4)
                .filter(|i| {
                    s.lease(&format!("task/k{i}"))
                        .is_some_and(|l| l.owner.is_some())
                })
                .count()
        })
        .await
        .unwrap();
    assert_eq!(held, 0);
    meta.shutdown().await;
}

/// A task that waits to be told to commit, then CASes a pointer with its
/// fence and records the result.
struct CommitTask {
    namespace: NamespaceId,
    value: String,
    go: Arc<tokio::sync::Notify>,
    started: Arc<AtomicUsize>,
    cancelled: Arc<AtomicBool>,
    result: Arc<Mutex<Option<Result<u64, MetaError>>>>,
}

#[async_trait]
impl Task for CommitTask {
    async fn run(&self, ctx: TaskContext) -> Result<TaskOutcome, TaskError> {
        self.started.fetch_add(1, Ordering::SeqCst);
        tokio::select! {
            () = ctx.cancel.cancelled() => self.cancelled.store(true, Ordering::SeqCst),
            () = self.go.notified() => {}
        }
        // Even a cancelled run tries to commit: the fence must stop it.
        let current = ctx
            .meta
            .read(Consistency::Linearizable, |s| {
                s.pointer(self.namespace, "p").map(|p| p.version)
            })
            .await?;
        let result = ctx
            .meta
            .cas_pointer(
                self.namespace,
                "p",
                current,
                &self.value,
                Some(ctx.fence.clone()),
            )
            .await;
        *self.result.lock().expect("lock") = Some(result);
        Ok(TaskOutcome::Done)
    }
}

struct CommitProbe {
    go: Arc<tokio::sync::Notify>,
    started: Arc<AtomicUsize>,
    cancelled: Arc<AtomicBool>,
    result: Arc<Mutex<Option<Result<u64, MetaError>>>>,
}

fn commit_task(namespace: NamespaceId, value: &str) -> (Arc<dyn Task>, CommitProbe) {
    let probe = CommitProbe {
        go: Arc::default(),
        started: Arc::default(),
        cancelled: Arc::default(),
        result: Arc::default(),
    };
    let task = CommitTask {
        namespace,
        value: value.to_string(),
        go: probe.go.clone(),
        started: probe.started.clone(),
        cancelled: probe.cancelled.clone(),
        result: probe.result.clone(),
    };
    (Arc::new(task), probe)
}

/// Review focus 1: a task whose lease runs out while it still runs (a stalled
/// renewal) and is taken over is cancelled, and its fenced commit is
/// rejected; the new holder's commit lands.
#[tokio::test]
async fn a_task_whose_lease_is_taken_over_is_cancelled_and_fenced() {
    let meta = Meta::start().await;
    let ns = meta.namespace("acme").await;
    let key = TaskKey::new(ns, "commit");
    let ttl = Duration::from_millis(600);

    let (old_task, old) = commit_task(ns, "old");
    let mut first = Worker::new(
        meta.client.clone(),
        WorkerConfig {
            lease_ttl: ttl,
            ..config("w1")
        },
    );
    first.add_source(FixedSource::new(
        Priority::LinkApply,
        vec![(key.clone(), old_task)],
        false,
    ));
    let first = first.start();
    eventually("the first run to start", || {
        old.started.load(Ordering::SeqCst) == 1
    })
    .await;
    first.pause_renewals(true);

    let (new_task, new) = commit_task(ns, "new");
    let mut second = Worker::new(
        meta.client.clone(),
        WorkerConfig {
            lease_ttl: ttl,
            ..config("w2")
        },
    );
    second.add_source(FixedSource::new(
        Priority::LinkApply,
        vec![(key.clone(), new_task)],
        false,
    ));
    let second = second.start();
    eventually("the second worker to take over", || {
        new.started.load(Ordering::SeqCst) == 1
    })
    .await;
    new.go.notify_one();
    eventually("the new commit", || {
        new.result.lock().expect("lock").is_some()
    })
    .await;
    assert_eq!(new.result.lock().expect("lock").take().unwrap().unwrap(), 1);

    // The stalled worker resumes renewing, finds the lease taken, and
    // cancels its run, whose commit is fenced.
    first.pause_renewals(false);
    eventually("the old run to be cancelled", || {
        old.cancelled.load(Ordering::SeqCst)
    })
    .await;
    eventually("the old commit", || {
        old.result.lock().expect("lock").is_some()
    })
    .await;
    let err = old
        .result
        .lock()
        .expect("lock")
        .take()
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(err, MetaError::Rejected(ApplyError::Fenced { .. })),
        "{err:?}"
    );
    let pointer = meta
        .client
        .read(Consistency::Linearizable, |s| s.pointer(ns, "p").cloned())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((pointer.version, pointer.value.as_str()), (1, "new"));
    first.stop().await;
    second.stop().await;
    meta.shutdown().await;
}

/// M0.3 re-review N3: a run whose renewals stalled past its lease, when
/// nobody took the lease, re-takes it at the same epoch and finishes; its
/// fenced commit lands.
#[tokio::test]
async fn an_expired_lease_nobody_took_is_retaken_and_the_task_continues() {
    let meta = Meta::start().await;
    let ns = meta.namespace("acme").await;
    let key = TaskKey::new(ns, "commit");
    let (task, probe) = commit_task(ns, "late");
    let mut worker = Worker::new(
        meta.client.clone(),
        WorkerConfig {
            lease_ttl: Duration::from_millis(300),
            ..config("w1")
        },
    );
    worker.add_source(FixedSource::new(
        Priority::LinkApply,
        vec![(key.clone(), task)],
        false,
    ));
    let worker = worker.start();
    eventually("the run to start", || {
        probe.started.load(Ordering::SeqCst) == 1
    })
    .await;
    worker.pause_renewals(true);
    // Wait until the lease has expired by the metastore's reckoning.
    let lease = key.lease();
    let deadline = meta
        .client
        .read(Consistency::Local, |s| s.lease(&lease).unwrap().deadline_ms)
        .await
        .unwrap();
    while meta.client.now_ms() <= deadline + 100 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    worker.pause_renewals(false);
    // The next renewal fails as expired, and the re-take succeeds.
    let expired_at = meta.client.now_ms();
    let deadline_ms = || async {
        meta.client
            .read(Consistency::Local, |s| s.lease(&lease).unwrap().deadline_ms)
            .await
            .unwrap()
    };
    let started = Instant::now();
    while deadline_ms().await <= expired_at {
        assert!(started.elapsed() < WAIT, "the lease was never re-taken");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!probe.cancelled.load(Ordering::SeqCst));
    probe.go.notify_one();
    eventually("the commit", || {
        probe.result.lock().expect("lock").is_some()
    })
    .await;
    assert_eq!(
        probe.result.lock().expect("lock").take().unwrap().unwrap(),
        1
    );
    let epoch = meta
        .client
        .read(Consistency::Local, |s| s.lease(&lease).unwrap().epoch)
        .await
        .unwrap();
    assert_eq!(epoch, 1);
    worker.stop().await;
    meta.shutdown().await;
}

/// Records the order tasks ran in.
struct Recorder {
    name: String,
    log: Arc<Mutex<Vec<String>>>,
    source: Mutex<Option<Arc<FixedSource>>>,
    key: TaskKey,
}

#[async_trait]
impl Task for Recorder {
    async fn run(&self, _ctx: TaskContext) -> Result<TaskOutcome, TaskError> {
        self.log.lock().expect("lock").push(self.name.clone());
        tokio::time::sleep(Duration::from_millis(2)).await;
        if let Some(source) = self.source.lock().expect("lock").as_ref() {
            source.remove(&self.key);
        }
        Ok(TaskOutcome::Done)
    }
}

fn recorders(
    names: &[(&str, Option<NamespaceId>)],
    log: &Arc<Mutex<Vec<String>>>,
) -> Vec<(Arc<Recorder>, Candidate)> {
    names
        .iter()
        .map(|(name, ns)| {
            let key = TaskKey {
                namespace: *ns,
                key: name.to_string(),
            };
            let recorder = Arc::new(Recorder {
                name: name.to_string(),
                log: log.clone(),
                source: Mutex::new(None),
                key: key.clone(),
            });
            let task: Arc<dyn Task> = recorder.clone();
            (recorder, (key, task))
        })
        .collect()
}

fn source_of(priority: Priority, recorders: Vec<(Arc<Recorder>, Candidate)>) -> Arc<FixedSource> {
    let source = FixedSource::new(
        priority,
        recorders.iter().map(|(_, c)| c.clone()).collect(),
        true,
    );
    for (recorder, _) in &recorders {
        *recorder.source.lock().expect("lock") = Some(source.clone());
    }
    source
}

#[tokio::test]
async fn with_one_slot_higher_priorities_run_first() {
    let meta = Meta::start().await;
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut worker = Worker::new(
        meta.client.clone(),
        WorkerConfig {
            max_concurrent: 1,
            ..config("w1")
        },
    );
    // Added lowest first, so source order cannot explain the result.
    let order = [
        (Priority::Gc, "gc"),
        (Priority::Maintenance, "maintenance"),
        (Priority::HotBuild, "hot"),
        (Priority::Compaction, "compaction"),
        (Priority::Segmenting, "segmenting"),
        (Priority::LinkApply, "link"),
    ];
    for (priority, name) in order {
        let names = [(format!("{name}-1"), None), (format!("{name}-2"), None)];
        let names: Vec<(&str, Option<NamespaceId>)> =
            names.iter().map(|(n, ns)| (n.as_str(), *ns)).collect();
        worker.add_source(source_of(priority, recorders(&names, &log)));
    }
    let handle = worker.start();
    eventually("every task", || log.lock().expect("lock").len() == 12).await;
    handle.stop().await;
    let ran: Vec<String> = log
        .lock()
        .unwrap()
        .iter()
        .map(|n| n.rsplit_once('-').unwrap().0.to_string())
        .collect();
    let expected: Vec<String> = order
        .iter()
        .rev()
        .flat_map(|(_, name)| [name.to_string(), name.to_string()])
        .collect();
    assert_eq!(ran, expected);
    meta.shutdown().await;
}

#[tokio::test]
async fn a_busy_namespace_does_not_starve_a_small_one() {
    let meta = Meta::start().await;
    let busy = meta.namespace("busy").await;
    let small = meta.namespace("small").await;
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut names: Vec<(String, Option<NamespaceId>)> = (0..100)
        .map(|i| (format!("busy-{i}"), Some(busy)))
        .collect();
    names.push(("small-0".to_string(), Some(small)));
    let names: Vec<(&str, Option<NamespaceId>)> =
        names.iter().map(|(n, ns)| (n.as_str(), *ns)).collect();
    let mut worker = Worker::new(
        meta.client.clone(),
        WorkerConfig {
            max_concurrent: 2,
            max_per_namespace: 2,
            ..config("w1")
        },
    );
    worker.add_source(source_of(Priority::Maintenance, recorders(&names, &log)));
    let handle = worker.start();
    eventually("the small namespace's task", || {
        log.lock().expect("lock").iter().any(|n| n == "small-0")
    })
    .await;
    let position = log
        .lock()
        .unwrap()
        .iter()
        .position(|n| n == "small-0")
        .unwrap();
    assert!(position < 2, "small ran after {position} busy tasks");
    handle.stop().await;
    meta.shutdown().await;
}

/// A task that runs until it is cancelled or aborted.
struct Forever {
    started: Arc<Mutex<Vec<String>>>,
    owner: String,
}

#[async_trait]
impl Task for Forever {
    async fn run(&self, ctx: TaskContext) -> Result<TaskOutcome, TaskError> {
        self.started.lock().expect("lock").push(self.owner.clone());
        ctx.cancel.cancelled().await;
        Ok(TaskOutcome::Done)
    }
}

#[tokio::test]
async fn a_dropped_worker_leaves_its_lease_to_expire_and_another_takes_over() {
    let meta = Meta::start().await;
    let started = Arc::new(Mutex::new(Vec::new()));
    let key = TaskKey::cluster("forever");
    let worker_for = |owner: &str| {
        let task: Arc<dyn Task> = Arc::new(Forever {
            started: started.clone(),
            owner: owner.to_string(),
        });
        let mut worker = Worker::new(
            meta.client.clone(),
            WorkerConfig {
                lease_ttl: Duration::from_millis(500),
                ..config(owner)
            },
        );
        worker.add_source(FixedSource::new(
            Priority::Gc,
            vec![(key.clone(), task)],
            false,
        ));
        worker.start()
    };
    let first = worker_for("w1");
    eventually("w1 to run the task", || {
        started.lock().expect("lock").len() == 1
    })
    .await;
    let second = worker_for("w2");
    // w1 keeps renewing, so w2 cannot take the task.
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    assert_eq!(*started.lock().expect("lock"), ["w1"]);
    drop(first);
    eventually("w2 to take over after the lease expired", || {
        started.lock().expect("lock").len() == 2
    })
    .await;
    assert_eq!(*started.lock().expect("lock"), ["w1", "w2"]);
    second.stop().await;
    meta.shutdown().await;
}

/// A task that reports whether its fence was valid.
struct FenceCheck {
    namespace: NamespaceId,
    calls: Arc<AtomicU32>,
}

#[async_trait]
impl Task for FenceCheck {
    async fn run(&self, ctx: TaskContext) -> Result<TaskOutcome, TaskError> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        let expected = (n > 0).then_some(u64::from(n));
        ctx.meta
            .cas_pointer(self.namespace, "p", expected, "v", Some(ctx.fence.clone()))
            .await?;
        Ok(TaskOutcome::Done)
    }
}

#[tokio::test]
async fn run_once_runs_each_candidate_under_its_lease() {
    let meta = Meta::start().await;
    let ns = meta.namespace("acme").await;
    let calls = Arc::new(AtomicU32::new(0));
    let task: Arc<dyn Task> = Arc::new(FenceCheck {
        namespace: ns,
        calls: calls.clone(),
    });
    let source = FixedSource::new(
        Priority::Maintenance,
        vec![(TaskKey::new(ns, "fence"), task)],
        false,
    );
    let ttl = Duration::from_secs(5);
    for _ in 0..2 {
        let results = run_once(&meta.client, "w1", ttl, source.as_ref())
            .await
            .unwrap();
        assert!(
            matches!(results[..], [(_, RunResult::Ran(Ok(TaskOutcome::Done)))]),
            "{results:?}"
        );
    }
    // Held by someone else: skipped.
    meta.client
        .acquire_lease("task/fence", "other", ttl)
        .await
        .unwrap();
    let results = run_once(&meta.client, "w1", ttl, source.as_ref())
        .await
        .unwrap();
    assert!(
        matches!(results[..], [(_, RunResult::LeaseHeld)]),
        "{results:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    meta.shutdown().await;
}
