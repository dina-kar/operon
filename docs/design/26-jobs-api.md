# 26 — Loams Jobs: Celery, BullMQ, PySpark and Flink Jobs on Loams

Status: **Proposed** · 2026-09-29. The direction is the owner's, from 2026-09-29: "The goal is to deploy PySpark, Flink, BullMQ, Celery jobs on Loams and run it, with some modification like adding Resonate decorators ... I want to expose clean Rust API for this." The owner then approved the per-workload direction in §1 ("yes", 2026-09-29). This document turns that direction into decisions **D204–D216** and open questions **Q95–Q104**. The owner decided Q97 and Q100 on 2026-09-29: jobs require TiKV on every deployment except `loams dev`, and `@loams/bullmq` ships for BullMQ v6 only. The choices it makes on top of the approved direction (the semantics, the state layout, the API details, the phasing) are **proposals** until the owner confirms them. **No code is written by this document**; `loams-jobs` is a design.

**Numbering.** On `main` the highest decision is D147 and the highest question Q44. The showcase suite's D-SC-1…16 and Q-SC-1…10 and the Postgres-wire D-PG-1 get numbers at merge, so they may take D148–D164 and Q45–Q54. Docs 23 (Neon/WeSQL) and 24/25 (CPU-time runtime, Clever Cloud stack) are being written on unpushed branches and pick numbers too, so this document leaves a gap of 20 for each: it starts at **D204** (164 + 20 + 20) and **Q95** (54 + 20 + 20). Renumber at merge if the gap was too small or too large.

Markers: **(source)** means read in the upstream repository at the revision in §17. **(estimate)** means computed, not measured. **(verify)** means the plan that builds it checks it first. "The research" is the data-processing research of 2026-09-29 (`.superpowers/research/rust-data-processing-2026-09.md`, not committed), which §9 and §10 follow.

---

## 1. Summary (D204)

Loams runs four families of jobs that teams already have, with as little change to their code as each family allows, on one Rust core:

| Workload | How it runs on Loams | Change to user code | Where Resonate fits |
|---|---|---|---|
| **Celery** | A **kombu transport** (`loams://`), a **result backend** and a **beat scheduler**, in the Python package `loams-celery`, over Loams's queue (TiKV state, Loams streams as the queue's event log, Resonate for schedules and flows) | The broker URL becomes `loams://…` and the result backend `loams://…`; optionally `@app.task` becomes a Resonate function | Very good: durable retries, beat → Resonate schedules, chain/group/chord → durable steps and fan-out |
| **BullMQ** | A drop-in TypeScript package **`@loams/bullmq`**: BullMQ's own `Queue`, `Worker`, `FlowProducer`, `QueueEvents` and `Job` classes, bound to a Loams implementation of BullMQ v6's pluggable `IQueueBackend` (§7.2). **Redis is not emulated** (BullMQ's Redis backend is 49 Lua scripts, §7.2.4) | The import changes from `bullmq` to `@loams/bullmq` | Very good: jobs → durable functions, flows → parent/child promises, repeatable jobs and job schedulers → Resonate schedules |
| **PySpark** | **Sail** (lakehq/sail), run by Loams as a managed Spark Connect server, with Iceberg tables on RustFS through Lakekeeper. Sail is a separate process, never linked (D51) | `SparkSession.builder.remote("sc://…")` | Job level only: step checkpoints, retries and schedules around Spark actions, never inside Spark tasks. RDD jobs and unsupported UDFs fall back to Apache Spark on Kubernetes |
| **Flink** | **Flink SQL** on RisingWave (default) or Arroyo. **DataStream** jobs run unmodified on Apache Flink (the Kubernetes operator, state on RustFS), reading Loams streams through the Kafka gateway (M5, D74) | SQL: small dialect edits; DataStream: none (it stays on the JVM) | Only the job lifecycle (deploy, savepoint, upgrade, rollback). Flink's checkpoints stay the only durability of a running job |

Under all four sits one crate, **`loams-jobs`**, with one protobuf surface, **`loams.jobs.v1`**, served over connect-rust (D128). Python and TypeScript clients are generated from it, and the Celery and BullMQ adapters are thin layers over those clients. The Rust trait (§5) has seven operations: `enqueue`, `lease`, `complete`, `schedule`, `flow`, `submit_engine_job` and `watch`, plus the admin calls around them.

Decorators come from **Resonate's own SDKs** (Python `@resonate.register`, TypeScript `resonate.register`), pointed at the Resonate server embedded in Loams (§21). Loams adds at most a thin `loams` helper that fills in the URL, token and tenant. There is no new decorator system.

## 2. Goals and non-goals

### 2.1 Goals

1. **Existing jobs run with a configuration change.** A Celery app runs with a new broker URL and result backend. A BullMQ app runs with a new import. A PySpark job runs with a new `remote(...)` URL, as long as it uses the Spark Connect surface. A Flink SQL job runs after dialect edits.
2. **Durability is opt-in, per task, with the standard decorators.** Replacing `@app.task` with `@resonate.register` (or registering a BullMQ processor as a Resonate function) makes a task durable: its steps are checkpointed and a crash resumes it. Nothing forces users to rewrite tasks that work.
3. **One Rust core, one protobuf surface.** Every adapter, every SDK and the console use `loams.jobs.v1`. Semantics are defined once, in §6, and every adapter documents where the framework it imitates differs.
4. **State on Loams's own primitives.** Job state and leases on TiKV (or the local store in `loams dev`), payloads on the object store, the queue's event log on Loams streams, schedules and flows on the embedded Resonate server. No Redis, RabbitMQ, ZooKeeper or Postgres is needed.
5. **Buy, not build.** Loams writes the queue core, the adapters and the engine lifecycle glue. It hosts Sail, RisingWave, Arroyo and Flink; it does not rewrite them (user memory: prefer buy over build).

### 2.2 Non-goals

- **No Redis protocol.** Loams does not speak RESP or run Lua. BullMQ users change an import instead (§7.2.4).
- **No AMQP.** Celery reaches Loams through a kombu transport, not an AMQP 0-9-1 server. Other AMQP clients are out of scope.
- **No Spark or Flink engine in the Loams binary.** Sail, RisingWave, Arroyo and Flink run as their own processes. D51 (no embedded engines) holds.
- **No second durability layer on streaming jobs.** A Flink or RisingWave job's state is its own checkpoints. Loams orchestrates the lifecycle around them and never checkpoints inside them.
- **No exactly-once side effects.** Delivery is at least once. Loams gives idempotency keys and fenced completion so that the *record* of a job's outcome is exactly once, and documents how user code makes its side effects idempotent (§6.2).
- **No new decorator or workflow DSL.** Resonate's SDKs are the durable programming model.
- **No BullMQ Pro features** (groups, batches, observables) and no Celery features that need a message broker Loams is not (AMQP exchanges beyond direct, fanout and topic routing).

## 3. Architecture

```
  Celery app (Python)        BullMQ app (Node/Bun)       PySpark client        Flink SQL / jar
  kombu transport loams://    @loams/bullmq                sc://<ns>.jobs…       loams jobs submit
  result backend loams://         │                           │                      │
  beat: LoamsScheduler            │                           │                      │
        │  generated Connect clients (loams.jobs.v1)          │ gRPC (Spark Connect)  │
        ▼                         ▼                           ▼                      ▼
┌───────────────────────────── loams / loams binary ─────────────────────────────────────────────┐
│  jobs listener (connect-rust, Connect + gRPC + gRPC-Web)     Spark Connect proxy (auth, route) │
│        │                                                          │                           │
│        ▼                                                          │                           │
│  loams-jobs:  JobsService ── QueueCore ─── Leases/fencing ────────┼── EngineJobs              │
│                    │            │  ▲             │                │   (Sail, RisingWave,       │
│                    │            │  │ outbox      │                │    Arroyo, Flink operator) │
│                    ▼            ▼  │ relay       ▼                ▼                           │
│  loams-durable (Resonate)   JobStore            payloads      lifecycle workflows             │
│  schedules, flows (promises) (TiKV | local)      (object store) (Resonate Rust SDK, in-proc)  │
│        │                        │                                                              │
│        ▼                        ▼                                                              │
│  durable store (§21)     queue event log: Loams stream `_jobs/<queue>` (idempotent producer)  │
└────────────────────────────────────────────────────────────────────────────────────────────────┘
        │                        │                         │                          │
   SQLite | TiKV            TiKV keyspace `loams_jobs`   RustFS / S3 bucket       Sail pods, RisingWave,
                            (local: redb in dev)        ns/<ns>/jobs/…           Flink (K8s operator)
```

### 3.1 Components

| Component | What it does | Built on |
|---|---|---|
| `loams-jobs` | The `Jobs` trait (§5), the queue core, lease fencing, rate limits, the outbox relay, the engine-job registry | `loams-tikv` (cloud), `redb` (dev), `loams-store`, `loams-durable` |
| `loams-jobs-proto` | `loams.jobs.v1` messages and the `JobsService` trait and client, generated in `build.rs` like `loams-live-proto` | buffa, connect-rust (`connectrpc-build`) |
| Jobs listener | Serves `JobsService` over Connect, gRPC and gRPC-Web on `127.0.0.1:7720` (loopback until the auth plan, like Live's 7710 and durable's 8001) | connect-rust on axum 0.8 |
| `loams-celery` (Python, PyPI) | kombu transport, Celery result backend, beat scheduler | The generated Python Connect client |
| `@loams/bullmq` (TypeScript, npm) | A BullMQ v6 `IQueueBackend` over `loams.jobs.v1`, and BullMQ's classes re-exported bound to it | The generated TypeScript Connect client (protobuf-es) |
| `loams` helpers (Python, TypeScript) | Configure Resonate's SDKs for Loams: URL, token, tenant, group. Nothing else | Resonate's SDKs |
| Engine runners | Deploy and supervise Sail, RisingWave, Arroyo and Flink jobs; each lifecycle is a Resonate workflow | Kubernetes API (kube-rs) in cloud; local processes in dev |

### 3.2 Where each piece of state lives (D214)

| State | Store | Why |
|---|---|---|
| Job records, ready and delayed indexes, leases, dedup keys, rate-limit buckets, chord counters | **TiKV** keyspace `loams_jobs` in cloud; a **redb** file in `loams dev` only (§6.8); every other deployment, self-hosted included, uses TiKV (Q97, owner-decided) | Every state change is a small multi-key transaction (dequeue = read the ready index, lock the job, write the lease). TiKV gives that with Percolator transactions; redb gives it in one process |
| Payloads and results larger than 16 KiB **(estimate)** | **Object store** (RustFS in cloud and self-hosted, any S3 elsewhere), `ns/<ns>/jobs/<queue>/<job>/{payload,result}` | TiKV prefers small values (§21 Q42); payloads can be megabytes |
| The queue's event log | A **Loams stream** per queue, `_jobs/<queue>`, written by an outbox relay with an idempotent producer (D72) | QueueEvents and `watch`, replay, audit, and analytics through a stream → Iceberg link (M4), with exactly-once events |
| Schedules, flows, durable-mode tasks | **Resonate** (the embedded server, §21) | Cron with a promise template, parent/child promises, replay |
| Engine job specs and runs | TiKV (a job record of kind `engine`) plus Resonate (the lifecycle workflow) | The run is a workflow: submit, wait, savepoint, upgrade |
| Engine checkpoints and savepoints, Spark and Iceberg data | Object store (RustFS); Iceberg metadata in Lakekeeper | The engines write them there themselves |

## 4. Concepts

- **Namespace.** The tenant, as in §18 §6 and §21 §5.1. Every queue, schedule, flow and engine job belongs to one namespace. Keys, quotas and the event-log streams are per namespace.
- **Queue.** A named, namespaced set of jobs with its own settings: priorities on or off, the default visibility timeout, the retry policy, rate limits, concurrency caps, retention and the dead-letter queue.
- **Job.** One unit of work: a task name, a payload, options and a state (§6.4). A job has a server-assigned `JobId` (a ULID) and an optional client `jobId` that is unique in the queue (BullMQ's `jobId`, Celery's task id).
- **Task spec.** The task name (Celery's `name`, BullMQ's `name`), the payload (bytes plus a content type: `application/json`, Celery's kombu envelope, msgpack), and headers.
- **Lease.** A worker's exclusive, time-bounded claim on one job, with a **fencing token** (§6.3). Completing, failing, extending or moving a job requires its current token.
- **Worker.** A process that leases jobs from one or more queues. Workers are identified (`WorkerId`: host, pid, a random suffix) so that stalled-job reports and metrics can name them.
- **Schedule.** A cron or interval rule that enqueues a job or starts a durable function. It is a Resonate schedule (§8.1).
- **Flow.** A graph of jobs with dependencies: Celery's chain, group and chord; BullMQ's parent/child flows; a general DAG. The graph's joins are Resonate promises (§8.2).
- **Engine job.** A Spark Connect session or batch job, a streaming SQL pipeline, or a Flink deployment. It is not leased by workers; an engine runner owns it (§9, §10).
- **Mode.** A queue job runs in **queue mode** (plain lease, run, complete, like Celery and BullMQ today) or **durable mode** (the task body is a Resonate function; a retry replays it from its last checkpoint). §8.3 says when each is used.

## 5. The Rust API (D205)

### 5.1 The trait

The owner's sketch had seven methods. The refined trait keeps them and adds what the adapters need: a request context that carries the tenant, a fencing token instead of a bare lease id, lease extension, typed outcomes, typed errors and the admin calls. Everything a Celery transport or a BullMQ backend does is one of these calls.

```rust
/// Who is calling, for which namespace, until when. Built by the transport
/// layer from the credential (the unified auth plan, D111) and passed to
/// every call; the trait never trusts a namespace named in a request body.
pub struct Ctx {
    pub namespace: NamespaceId,
    pub principal: Principal,          // API key, agent token or `system`
    pub deadline: Option<Instant>,
    pub request_id: RequestId,         // for logs and traces
}

pub struct QueueId(pub String);        // unique in the namespace; `[a-z0-9._:-]{1,128}`
pub struct JobId(pub Ulid);            // server-assigned
pub struct WorkerId(pub String);       // host/pid/random, chosen by the worker

/// The fencing token of a lease: the job and the lease epoch. Every write
/// that depends on holding the job (extend, complete, progress, move)
/// carries it, and the store refuses it once the epoch has moved (§6.3).
pub struct LeaseToken { pub queue: QueueId, pub job: JobId, pub epoch: u64 }

#[async_trait]
pub trait Jobs: Send + Sync {
    // Producing
    async fn enqueue(&self, cx: &Ctx, queue: &QueueId, task: TaskSpec, opts: EnqueueOpts)
        -> Result<Enqueued, JobsError>;
    async fn enqueue_bulk(&self, cx: &Ctx, queue: &QueueId, jobs: Vec<(TaskSpec, EnqueueOpts)>)
        -> Result<Vec<Result<Enqueued, JobsError>>, JobsError>;

    // Consuming
    async fn lease(&self, cx: &Ctx, req: LeaseRequest) -> Result<Vec<Lease>, JobsError>;
    async fn extend(&self, cx: &Ctx, token: &LeaseToken, by: Duration)
        -> Result<Deadline, JobsError>;
    async fn complete(&self, cx: &Ctx, token: &LeaseToken, outcome: Outcome)
        -> Result<Completed, JobsError>;
    async fn report(&self, cx: &Ctx, token: &LeaseToken, report: Report)
        -> Result<(), JobsError>;              // progress, log line, data update

    // Orchestration (Resonate)
    async fn schedule(&self, cx: &Ctx, spec: ScheduleSpec) -> Result<ScheduleId, JobsError>;
    async fn unschedule(&self, cx: &Ctx, id: &ScheduleId) -> Result<(), JobsError>;
    async fn flow(&self, cx: &Ctx, graph: FlowSpec) -> Result<FlowHandle, JobsError>;

    // Engines (Sail, RisingWave, Arroyo, Flink)
    async fn submit_engine_job(&self, cx: &Ctx, job: EngineJob, opts: SubmitOpts)
        -> Result<RunId, JobsError>;
    async fn control_engine_job(&self, cx: &Ctx, run: &RunId, action: EngineAction)
        -> Result<ActionId, JobsError>;         // savepoint, suspend, resume, upgrade, rollback, cancel

    // Observing: a resumable stream of events for one job, queue, flow, run or schedule
    fn watch(&self, cx: &Ctx, what: Selector, from: Cursor)
        -> BoxStream<'static, Result<Event, JobsError>>;

    // Admin (§5.4)
    async fn queue_admin(&self, cx: &Ctx, queue: &QueueId, op: QueueOp)
        -> Result<QueueOpResult, JobsError>;
    async fn job_admin(&self, cx: &Ctx, queue: &QueueId, job: JobRef, op: JobOp)
        -> Result<JobOpResult, JobsError>;
    async fn query(&self, cx: &Ctx, q: JobQuery) -> Result<Page<JobView>, JobsError>;

    // Result store (§7.1.4): Celery's key-value result backend, TTL'd, one TiKV txn each
    async fn result_get(&self, cx: &Ctx, key: &ResultKey) -> Result<Option<Payload>, JobsError>;
    async fn result_mget(&self, cx: &Ctx, keys: Vec<ResultKey>)
        -> Result<Vec<Option<Payload>>, JobsError>;
    async fn result_set(&self, cx: &Ctx, key: &ResultKey, value: Payload, ttl: Option<Duration>)
        -> Result<(), JobsError>;
    async fn result_delete(&self, cx: &Ctx, key: &ResultKey) -> Result<(), JobsError>;
    async fn result_incr(&self, cx: &Ctx, key: &ResultKey, member: &str, ttl: Option<Duration>)
        -> Result<Counted, JobsError>;          // atomic; a member counts once (§7.1.4)
    async fn result_expire(&self, cx: &Ctx, key: &ResultKey, ttl: Duration)
        -> Result<(), JobsError>;
}
```

### 5.2 The main types

```rust
pub struct TaskSpec {
    pub name: String,                  // Celery task name, BullMQ job name
    pub payload: Payload,              // bytes + content type (json, celery-kombu, msgpack)
    pub headers: BTreeMap<String, String>,
}

pub struct EnqueueOpts {
    pub job_key: Option<String>,       // unique in the queue (BullMQ jobId, Celery task id)
    pub idempotency: Option<Idempotency>, // key + ttl (default 24 h), §6.2
    pub dedup: Option<Dedup>,          // BullMQ deduplication: simple | throttle(ttl) | debounce(ttl, replace, extend)
    pub not_before: Option<Delay>,     // delay or absolute time (Celery eta/countdown, BullMQ delay)
    pub priority: Priority,            // §6.5; None = the FIFO band
    pub lifo: bool,
    pub attempts: u32,                 // default: the queue's
    pub backoff: Option<Backoff>,      // fixed | exponential { base, max, jitter } | custom(name)
    pub visibility: Option<Duration>,  // lease length for this job, default the queue's
    pub timeout: Option<Duration>,     // hard limit: the lease cannot be extended past it
    pub expires: Option<SystemTime>,   // Celery `expires`: not started after this → discarded
    pub parent: Option<ParentRef>,     // flow membership (§8.2)
    pub retention: Option<Retention>,  // removeOnComplete / removeOnFail, result TTL
    pub mode: Mode,                    // Queue | Durable { function } (§8.3)
}

pub struct Enqueued { pub job: JobId, pub created: bool, pub state: JobState }

pub struct LeaseRequest {
    pub queues: Vec<QueueId>,          // in the worker's order of preference (Celery -Q a,b)
    pub worker: WorkerId,
    pub max: u32,                      // prefetch / free concurrency slots
    pub wait: Duration,                // long-poll, at most 30 s; 0 = return at once
    pub names: Option<Vec<String>>,    // only these task names (Celery routing by name)
}

pub struct Lease {
    pub token: LeaseToken,
    pub task: TaskSpec,                // payload inline, or a presigned object-store URL (§6.7)
    pub attempt: u32,                  // 1-based; stalled redeliveries counted separately
    pub stalled: u32,
    pub deadline: SystemTime,          // by the store's clock (§6.3)
    pub parent: Option<ParentRef>,
    pub children: Option<ChildrenSummary>, // for parents that resumed after their children
}

pub enum Outcome {
    Ok { result: Option<Payload> },
    Retry { error: JobError, after: Option<Duration> }, // after = None: the job's backoff
    Fail { error: JobError, dead_letter: bool },        // no more attempts (UnrecoverableError)
    Release { after: Option<Duration> },                // give back without spending an attempt (nack)
    Delay { until: SystemTime },                        // BullMQ moveToDelayed
    WaitChildren,                                       // BullMQ moveToWaitingChildren
    RateLimited { for_: Duration },                     // BullMQ Worker.RateLimitError: queue-wide pause
}

pub enum JobState {
    Waiting, Prioritized, Delayed, Active, WaitingChildren,
    Completed, Failed, DeadLettered, Canceled,
}
```

`ScheduleSpec`, `FlowSpec` and `EngineJob` are in §8.1, §8.2 and §9–§10.

### 5.3 Errors

One error enum, mapped one to one onto Connect codes, so the Python and TypeScript clients see the same classes:

| `JobsError` | Connect code | Retry? | When |
|---|---|---|---|
| `InvalidArgument(msg)` | `invalid_argument` | No | Bad names, sizes, cron expressions, options that contradict each other |
| `NotFound(what)` | `not_found` | No | Unknown queue, job, schedule, flow or run |
| `AlreadyExists { existing }` | `already_exists` | No | A `job_key` that exists with different parameters. With the same parameters `enqueue` succeeds with `created: false` |
| `IdempotencyConflict { key }` | `failed_precondition` | No | An idempotency key reused with a different request (hash mismatch, like D146 and Live's `args_hash`) |
| `Fenced { current_epoch }` | `failed_precondition` | No | The lease epoch moved: the job was reclaimed and leased again. The worker must drop its result |
| `InvalidState { from, to }` | `failed_precondition` | No | For example `promote` on a job that is not delayed |
| `QueuePaused` | `unavailable` | Yes, after resume | `lease` on a paused queue returns no jobs rather than this error; admin writes may return it |
| `RateLimited { retry_after }` | `resource_exhausted` | Yes | A namespace quota (D65) or the jobs listener's own request limit. A queue's job rate limit never errors: `lease` returns fewer jobs |
| `PayloadTooLarge { limit }` | `resource_exhausted` | No | Over the namespace's payload limit (default 64 MiB) |
| `Unavailable(msg)` | `unavailable` | Yes | TiKV region errors after the runner's retries, the durable server down, an engine runner unreachable |
| `DeadlineExceeded` | `deadline_exceeded` | Caller decides | The `Ctx` deadline passed |
| `Unauthenticated`, `PermissionDenied` | the same | No | From the auth layer (D111) |
| `Internal(msg)` | `internal` | No | A bug or corrupt record; logged with the request id |

A write whose outcome is unknown (a timeout after TiKV's commit, §20 §5) is **retried by the store with its commit token** (`loams-tikv` runner), so clients see either success or a definite error. An `Unavailable` from `enqueue` is safe to retry only with a `job_key` or idempotency key; the generated clients add an idempotency key to every `enqueue` by default (a UUIDv7 made once per call, reused across that call's retries).

### 5.4 Admin operations

`QueueOp`: `Create(QueueConfig)`, `Update(QueueConfig)`, `Pause`, `Resume`, `Drain { delayed }`, `Clean { state, grace, limit }`, `Obliterate { force }`, `Counts`, `RetryAll { state }`, `PromoteAll`, `SetRateLimit`, `SetConcurrency`, `Workers`, `Metrics`, `RedriveDeadLetters { limit }`.
`JobOp`: `Get`, `Remove`, `Retry`, `Promote`, `ChangePriority`, `ChangeDelay`, `UpdateData`, `Logs`, `Cancel`, `RemoveDedupKey`.
`JobQuery`: by queue, state, name, time range and tag, paginated, newest first; BullMQ's `getJobs(types, start, end, asc)` and Celery's `inspect` map onto it.

These cover BullMQ's `IQueueBackend` getters and admin methods (§7.2) and Celery's `purge`, `inspect` and `control` calls that do not need a broadcast channel (§7.1.5).

### 5.5 The protobuf surface (D206)

- **Package** `loams.jobs.v1`, files under `proto/loams/jobs/v1/` at the workspace root (like `proto/loams/live/v1/`): `jobs.proto` (the service), `types.proto`, `engines.proto`, `events.proto`.
- **Service** `JobsService`: unary `Enqueue`, `EnqueueBulk`, `Lease` (long-poll), `Extend`, `Complete`, `Report`, `Schedule`, `Unschedule`, `Flow`, `SubmitEngineJob`, `ControlEngineJob`, `QueueAdmin`, `JobAdmin`, `Query`, and the result store `ResultGet`, `ResultMGet`, `ResultSet`, `ResultDelete`, `ResultIncr`, `ResultExpire` (§7.1.4); server-streaming `Watch`. `Lease` is unary with a wait, not a stream: a worker's prefetch is explicit, and connection loss cannot strand jobs in a push buffer. A server-streamed `LeaseStream` is a later optimization (Q98).
- **Server** `loams-jobs-proto`: buffa messages and the connect-rust `JobsService` trait, generated in `build.rs` with `connectrpc-build` and the system `protoc` (D128, as `loams-live-proto` does). It speaks Connect, gRPC and gRPC-Web on one listener.
- **Clients**: `buf generate` with protobuf-es and `@connectrpc/connect` for TypeScript, and connect-python (`connectrpc` on PyPI) for Python (§12.1). The Celery and BullMQ adapters wrap these clients; they do not hand-write HTTP.
- **Versioning**: additive changes only within `v1`; `buf breaking` runs in CI against the last release.

### 5.6 Method → backing primitives

| Method | TiKV (`loams_jobs`) | Loams streams | Resonate | Object store |
|---|---|---|---|---|
| `enqueue` | One optimistic txn: idempotency record, `job_key` index, dedup key, job record, ready or delayed index entry, outbox row | Event `added`/`waiting`/`delayed` via the outbox relay (§6.6) | Durable mode: nothing at enqueue; the root promise is created when the job first runs (§8.3). With a `parent`: the child's completion promise (§8.2) | Payload PUT before the txn when over 16 KiB (§6.7) |
| `lease` | One pessimistic txn per shard visited: scan the ready index head, check the rate bucket and concurrency counter, lock and move up to `max` jobs to active with epoch + 1 and a deadline | `active` events | — | Presigned GET URL for large payloads |
| `extend` | Txn: check the epoch, move the deadline (never past `timeout`) | — | — | — |
| `complete` | Txn: check the epoch; write the result or error; move to completed, failed, delayed (retry), waiting-children or the DLQ; decrement concurrency; parent bookkeeping; outbox row | `completed`/`failed`/`retries-exhausted`/`delayed` events | Settles the flow node's promise (through the outbox, §8.2) | Result PUT when over 16 KiB |
| `report` | Txn: check the epoch; progress, log append (capped), data update | `progress` event | — | — |
| `schedule` | Schedule metadata (for listing and BullMQ's getters) | — | `schedule.create` with a promise template (§8.1) | — |
| `flow` | Job records for every node, parents in waiting-children | `added` events | Root promise = flow id; one child promise per node; chord joins (§8.2) | — |
| `submit_engine_job` | Engine run record | Run events | The lifecycle workflow (§9, §10) | Artifacts (jars, SQL, Python files), savepoints and checkpoints written by the engines |
| `watch` | Snapshot of current state for the selector | Subscribe from the cursor (a stream offset) | Promise state for flows and durable jobs | — |
| stalled sweep (internal) | Per-shard scan of expired leases → waiting (stalled + 1) or failed | `stalled` events | — | — |
| delayed promoter (internal) | Per-shard scan of due delayed entries → ready | `waiting` events | — | — |

## 6. Semantics (D207)

### 6.1 Delivery

- **At least once.** A job is delivered to one worker at a time. It is delivered again if the lease expires before `complete` (a crashed or stalled worker), or if the worker returns `Retry` or `Release`. Duplicated deliveries are therefore possible; lost jobs are not, once `enqueue` has returned.
- **The outcome is recorded exactly once.** `complete` is fenced (§6.3): of two workers that both ran a job (the first stalled, the second leased it after the lease expired), only the holder of the current epoch can record the outcome. The other gets `Fenced`. So a result, a retry count or a parent's child counter is never applied twice.
- **Effectively once needs idempotent tasks or durable mode.** A task's own side effects happen at least once in queue mode. Loams gives two tools: the **lease token's epoch** as a fencing token for the task's own writes (a Loams write with a `Fence`, an external API with an idempotency key made from `(job id, step)`), and **durable mode**, where each step is a Resonate checkpoint and its promise id is the idempotency key of its effect (§8.3, §21 §6.7). The docs say plainly that an existing task with side effects and no idempotency key can repeat those side effects on redelivery, exactly as it can on Redis or RabbitMQ today.
- **No exactly-once claim.** Loams does not say "exactly once" for job execution anywhere, in the docs or the API.

### 6.2 Idempotency and deduplication

Three separate mechanisms, because the frameworks have three:

| Mechanism | Scope | Behaviour | From |
|---|---|---|---|
| **`job_key`** | Unique in the queue while the job exists (until retention removes it) | A second `enqueue` with the same key returns the existing job (`created: false`) if the parameters match, else `AlreadyExists` | BullMQ `jobId`; Celery task id (the kombu message's `id` header) |
| **Idempotency key** | Per namespace and queue, for a TTL (default 24 h), independent of the job's retention | A retried `enqueue` returns the first call's `Enqueued`. A different request under the same key is `IdempotencyConflict` | D146's operations API; Live's idempotency records (D118) |
| **Dedup** | Per queue and dedup id | BullMQ's modes: simple (while the job is not finished), throttle (for `ttl`), debounce (replace the pending job's data, optionally extending the ttl), keep-last-if-active | BullMQ `deduplication` |

The record layout follows Live's `IdempotencyRecord`: a hash of the key, the result, an expiry by the TSO clock and a hash of the canonical request.

### 6.3 Leases and fencing

- **Epoch per job.** A job record carries `epoch`. `lease` increments it and writes `(worker, epoch, deadline)`. Every later write for that job (`extend`, `complete`, `report`, moves) is one transaction that reads the job with `get_for_update` and refuses the write unless its epoch equals the token's. This is the rule `loams-meta-tikv`'s `check_fence` already implements for metastore leases (`crates/loams-meta-tikv/src/leases.rs`).
- **Expiry alone does not break a fence.** As in `loams_common::meta::Fence`: until the stalled sweep reclaims the job (epoch + 1 on the next lease), a late `complete` from the original worker still succeeds. That avoids failing work that finished a moment after its deadline.
- **Clock.** Deadlines are judged by the store's clock (the TSO physical time in TiKV, as in `leases.rs`; the process clock with the local store), never by a worker's clock. The Celery transport renews at a third of the lease, to leave room for a slow TiKV write; BullMQ's own worker keeps its `lockRenewTime` setting, because `@loams/bullmq` runs BullMQ's unmodified `Worker`.
- **Stalled sweep.** Each queue shard has one owner at a time (a metastore lease `jobs/sweep/<ns>/<queue>/<shard>`, placed by the router's rendezvous hashing, §18 §5). The owner scans the shard's active index by deadline. An expired job goes back to waiting with `stalled + 1` and a `stalled` event; past the queue's `max_stalled` (BullMQ's `maxStalledCount`, default 1) it fails with "job stalled more than allowable limit", as BullMQ does. Celery's `acks_late` redelivery is the same path with `max_stalled` unlimited.
- **Visibility and ETA are separate.** A delayed job is in the delayed index, not leased. It is never "invisible" while it waits, so a long `countdown` or `eta` does not cause the duplicate deliveries it causes on Redis and SQS when it exceeds `visibility_timeout` (§7.1).
- **Hard timeout.** `timeout` caps the total lease; `extend` past it is refused, and the sweep treats the job as timed out (a retryable failure with `error.code = "timeout"`).

### 6.4 States

```
             enqueue                    lease                    complete(Ok)
  (none) ──► waiting | prioritized ───► active ─────────────────► completed ──► (removed by retention)
      │           ▲   ▲                  │  │  │ complete(Retry), attempts left
      │ delay     │   │ due              │  │  └───────────────► delayed ──┐
      └──► delayed┘   └──────────────────┼──┘ complete(Release)            │ due
                                         │     lease expired (stalled)     ▼
                                         │  ──► waiting (stalled+1) ◄──────┘
                                         │ complete(Fail) or attempts exhausted
                                         ├──► failed ──► (dead_letter?) ──► DLQ queue: waiting
                                         └ complete(WaitChildren) ──► waiting-children ──(children done)──► waiting
```

`waiting` and `prioritized` are one index with two bands (§6.5), reported separately because BullMQ reports them separately. Paused queues keep their jobs in place and `lease` returns nothing (BullMQ v6 dropped the public `paused` state).

### 6.5 Priorities, ordering and LIFO

- **Model.** A job with no priority is in the FIFO band, served before any prioritized job. A prioritized job has a priority in 1…2,097,151 and a lower number is served first; ties are FIFO. This is BullMQ's model exactly (`PRIORITY_LIMIT = 2^21 − 1`, score `priority × 2^32 + counter`) **(source)**. Celery's priorities (0–9 on the Redis transport, where 0 is highest; 0–255 on RabbitMQ, where higher is higher) are mapped by the transport (§7.1).
- **Index key.** `ready/<shard>/<band><priority:u32 BE><seq:u64 BE>`; LIFO jobs use `u64::MAX − seq`. A lease scans from the start of the key range, so the order is one range scan.
- **Sharding.** A queue has `S` shards (default 1; up to 64) for throughput. A job's shard is chosen at enqueue (hash of the job id, or round robin). With `S > 1`, order holds per shard, not across shards: priorities are approximate across shards (a worker visits shards in a rotating order and takes the best head it sees in the first two it visits **(estimate)**). A queue that needs strict global priority keeps `S = 1`.
- **No ordering guarantee across workers.** Like BullMQ and Celery, jobs are started in order but may finish in any order. Strict per-key ordering (BullMQ Pro's groups, SQS FIFO) is not offered (Q99).

### 6.6 The event log and `watch`

- Every state change writes an **outbox row** in the same transaction: `outbox/<shard>/<seq>`.
- A **relay** per queue shard (the same owner as the stalled sweep) reads the outbox in order and appends the events to the stream `_jobs/<queue>` in the namespace, partition = shard, with an **idempotent producer** (D72): producer id `jobs-relay/<queue>/<shard>`, epoch = the relay's lease epoch, sequence = the outbox sequence. A relay that crashed after appending and before deleting its outbox rows re-appends them, and the sequencer returns the original offsets without appending (§02 §7). So each event appears in the log exactly once, in order per shard.
- `watch` reads the current state from TiKV and, **in the same snapshot** (one TiKV read at one `start_ts`), each shard's highest outbox sequence. Because every state change writes its outbox row in the same transaction, that sequence is exactly the set of changes the snapshot already reflects. `watch` then subscribes to each partition and skips records whose outbox sequence (every record carries it) is at or below the snapshot's; the returned cursor is the stream offset per partition of the first record it delivers. No change can fall between the snapshot and the subscription: a change committed after the snapshot has a higher outbox sequence and is delivered, even if the relay appends it later. A client that reconnects passes its cursor and misses nothing within the stream's retention (default 7 days for `_jobs/*`, **estimate**). QueueEvents (BullMQ) and `celery events`-style monitors read this.
- The event stream can be linked to an Iceberg table (M4) for job analytics: durations, failure rates, per-task cost.
- **Until D72's idempotent producers land (M2)**, the relay writes events with at-least-once delivery and includes the outbox sequence in each record so consumers can drop duplicates. In `loams dev` without a stream engine, `watch` tails the outbox directly (§6.8).

### 6.7 Payloads and results

- Inline in the job record up to 16 KiB **(estimate)**; larger payloads are written to `ns/<ns>/jobs/<queue>/<job>/payload` before the enqueue transaction, which references the object. The object carries a `Freshness` (§03), so GC deletes objects whose transaction never committed.
- Results follow the same rule. Celery's result backend reads results through `query`/`Get`; large results come back as a presigned URL that the client fetches.
- Payloads are opaque to Loams. The docs repeat §21 §8's rule: pass references, not personal data, or encrypt payloads client-side; erasure (D68) cannot look inside a payload.

### 6.8 Stores: TiKV everywhere, redb in dev

`loams-jobs` defines a `JobStore` trait (the transactions above) with two implementations:

| Store | Where | Notes |
|---|---|---|
| `TikvJobStore` | Loams cloud, self-hosted clusters and `loams standalone`: every deployment except `loams dev`; keyspace `loams_jobs` on the Live cluster, tenant = key prefix `t/<ns>/` (large tenants get their own keyspace, like §21 §5.1) | Uses `loams-tikv`'s runner (classification, retries, commit tokens, fault plan). Optimistic for `enqueue`, pessimistic for `lease` (the ready-index head is contended) |
| `LocalJobStore` | `loams dev` only | One redb file (`<data>/jobs.redb`), one writer; same key layout. `loams standalone` and `loams cluster` refuse it and ask for `--jobs-store tikv://…` |

**Owner decision (Q97, 2026-09-29): jobs require TiKV.** Self-hosted clusters run TiKV for jobs; there is no Postgres or DynamoDB jobs backend, and redb stays a development store for `loams dev` only.

### 6.9 Rate limits and concurrency

- **Queue rate limit** (`max` jobs per `duration`, BullMQ's `limiter`): a token bucket in one TiKV key per queue, debited inside the `lease` transaction by the number of jobs taken. `lease` returns fewer jobs, or none with a `retry_after` hint, when the bucket is empty. `Outcome::RateLimited` (BullMQ's `Worker.RateLimitError`, `queue.rateLimit(ms)`) empties the bucket for the given time.
- **Global concurrency** (BullMQ's `setGlobalConcurrency`): a counter of active jobs per queue, checked and incremented in `lease`, decremented in `complete` and by the stalled sweep.
- **Worker concurrency** stays in the worker (Celery's `--concurrency`, BullMQ's `concurrency`): the worker asks `lease` for at most its free slots.
- **Celery's `rate_limit`** is enforced by the Celery worker itself (per worker, not global) **(source, §7.1)**, so the transport does not need to implement it. The queue rate limit is offered as a global alternative.
- **Hot keys.** The bucket and the counter are one key each, written by every `lease`. With batched leases (a worker takes up to its free slots in one call) this is one write per batch, not per job. A single queue is budgeted at about 2,000 leases/s with `S = 1` and scales with shards **(estimate; J1 measures it)**. Rate-limited queues above that need a sharded bucket (one per queue shard, each with `max / S`), which the queue config can turn on.
- **Namespace quotas** (D65) cap enqueue rate, payload bytes, stored jobs and concurrent engine runs; they return `RateLimited` or `PayloadTooLarge`.

### 6.10 Dead-letter queues and retention

- A queue may name a **dead-letter queue** (another queue in the namespace). A job that fails with `dead_letter: true`, or exhausts its attempts on a queue with `dead_letter_on_exhausted`, is re-enqueued there with its error history and original queue in headers. `RedriveDeadLetters` moves them back.
- Without a DLQ, failed jobs stay in `failed` (BullMQ's behaviour) until retention removes them.
- **Retention** by count or age per state (BullMQ's `removeOnComplete/Fail`, Celery's `result_expires`, default 1 day for results). A per-shard janitor deletes expired records and their payload objects. Idempotency records expire on their own TTL, independent of the job.

### 6.11 Observability

- **Metrics** on the admin listener (`/metrics`): `loams_jobs_enqueued_total{ns,queue}`, `loams_jobs_leased_total`, `loams_jobs_completed_total{outcome}`, `loams_jobs_stalled_total`, `loams_jobs_fenced_total`, `loams_jobs_waiting{band}`, `loams_jobs_delayed`, `loams_jobs_active`, `loams_jobs_oldest_waiting_seconds`, `loams_jobs_lease_latency_seconds`, `loams_jobs_run_seconds{task}`, `loams_jobs_dlq_depth`, `loams_engine_runs{engine,state}`. BullMQ's `getMetrics` and `exportPrometheusMetrics` read the same counters.
- **Traces.** Each job gets a trace context at enqueue (W3C `traceparent` in the headers, as Celery and BullMQ's telemetry propagate it). The lease and the completion are spans; durable steps add Resonate's spans (§21 §6.6). OTLP export follows Loams's M2 observability work; traces ingest into Loams is Q43.
- **Logs.** Job log lines (`report(Log)`, BullMQ's `job.log`) are capped per job (default 1,000 lines) and stored with the job.
- **The console** lists queues, counts, jobs, DLQs, schedules, flows and engine runs from `query` and `watch` (after §19's console work).

## 7. Adapters

### 7.1 Celery: `loams-celery` (D208)

Celery 5.6.3 and kombu 5.6.2 are the current releases, both BSD-3-Clause **(source)**. Celery splits its needs between two plug points: the **broker** (a kombu transport) carries task messages, and the **result backend** stores results, group results and chord counters. Beat is a third plug point. `loams-celery` implements all three over `loams.jobs.v1`.

```python
# celeryconfig.py: the only change for a queue-mode app
import loams_celery                      # registers the kombu alias "loams" (§7.1.1)
# Same host only until the unified auth plan (D111): the jobs listener binds
# 127.0.0.1:7720 (§3.1, §11). After D111 the URL is the authenticated endpoint,
# jobs.<cloud-domain>:443 with TLS, and nothing else changes.
broker_url = "loams://127.0.0.1:7720/my-namespace"
result_backend = "loams://127.0.0.1:7720/my-namespace"
beat_scheduler = "loams"                 # optional: schedules live in Loams (§7.1.6)
broker_transport_options = {"visibility_timeout": 3600, "token_env": "LOAMS_TOKEN"}
result_backend_transport_options = {"token_env": "LOAMS_TOKEN"}
```

**Credentials.** The transport and the result backend are separate Celery objects, so each reads its own options: the transport from `broker_transport_options`, `LoamsBackend` from Celery's `result_backend_transport_options`. Both take `token_env` (the environment variable holding the Loams token) and default to `LOAMS_TOKEN` when it is absent, so the two lines above are optional. `LoamsBackend` builds one `loams.jobs.v1` client per process with that token; `AsyncResult`, `GroupResult.save/restore` and the chord counter all go through the backend and therefore that client. A token is never taken from the URL.

#### 7.1.1 Registration

| Plug point | Mechanism in Celery/kombu | What `loams-celery` does |
|---|---|---|
| Transport | kombu resolves the URL scheme through the dict `TRANSPORT_ALIASES` (`kombu/transport/__init__.py:21-49`); **there is no entry-point group for transports** (entry points are read only for matchers and serializers). A URL scheme may not be a `module:Class` path (`Connection._check_url_transport`, `connection.py:63-86`). An explicit `transport=` (Celery's `broker_transport`, `app/base.py:1213`) wins over the URL scheme | Two supported ways: `import loams_celery` adds `TRANSPORT_ALIASES["loams"] = "loams_celery.transport:Transport"` at import time, or the app sets `broker_transport = "loams_celery.transport:Transport"`. The docs recommend the second, because it does not depend on import order. An upstream kombu PR for a `kombu.transports` entry-point group is proposed, not required (§12.3) |
| Result backend | `by_name` merges `BACKEND_ALIASES`, the loader's overrides and the **`celery.result_backends` entry-point group** (`app/backends.py:15-47`) | Entry point `loams = loams_celery.backend:LoamsBackend`, so `result_backend = "loams://…"` works with no import |
| Beat scheduler | `beat_scheduler` is resolved through `symbol_by_name` and the **`celery.beat_schedulers` entry-point group** (`beat.py:686-690`) | Entry point `loams = loams_celery.beat:LoamsScheduler` |

#### 7.1.2 The transport

kombu's virtual transport (`kombu/transport/virtual/base.py`) keeps unacked messages in an in-process `OrderedDict` (`QoS._delivered`) and restores them only at shutdown or channel close. After a crash, **nothing is recovered unless the backend has its own visibility timeout** (`restore_unacked_once`, lines 192-195, 743-751, 803) **(source)**. Redis emulates one with an `unacked` hash and a restore sweep (`redis.py:408-490`); SQS and the new `pgmq` transport have a server-side one. Loams's lease is that server-side visibility timeout, so the transport follows the `pgmq`/SQS model, not the in-process one:

| kombu call | Loams call | Notes |
|---|---|---|
| `_put(queue, message)` | `enqueue` | The whole kombu message (body, headers, properties) is the payload, content type `application/x-kombu+json`. Loams reads four headers: `task` → `TaskSpec.name`; `id` + `retries` → `job_key` (§7.1.4); `eta` → `not_before`; `expires` → `expires`; and the `priority` property (§7.1.3) |
| `_get(queue)` / `_get_many(queues)` | `lease` (with `wait` up to the polling interval, and `max` = the free prefetch slots) | Long-polling replaces kombu's 1 s `polling_interval` loop. The delivery tag maps to the `LeaseToken` |
| `basic_ack(tag)` | `complete(Ok)` | Celery acks early by default, when the pool accepts the task (`worker/request.py:391-392`), and after the task with `acks_late` |
| `basic_reject(tag, requeue=True)` | `complete(Release)` | Used by `task_reject_on_worker_lost` and timeouts under `acks_late` (`request.py:705-724`) |
| `basic_reject(tag, requeue=False)` | `complete(Fail { dead_letter })` | To the queue's DLQ if it has one |
| `basic_recover(requeue=True)` | `complete(Release)` for every unacked tag | |
| unacked while running | `extend` every `visibility_timeout / 3` from a transport thread | Like the `gcpubsub` transport's `modify_ack_deadline` loop. A long `acks_late` task on a live worker is **never** redelivered by timeout, which removes Redis's "task longer than visibility_timeout runs twice" problem. A dead worker's leases expire and the jobs are redelivered (Loams's stalled path, with `max_stalled` unlimited for Celery queues) |
| `_size`, `_purge`, `_delete`, `_new_queue`, `_has_queue` | `queue_admin(Counts / Drain / Obliterate / Create)`, `query` | Queues are created on first declare, as with Redis |
| `get_table`, `queue_bind`, exchange declare | Bindings stored in Loams (a small table per namespace) | kombu's default `get_table` reads process-local state (`base.py:702`), so topic routing across processes needs shared bindings, as Redis keeps them in `_kombu.binding.*` |
| `_put_fanout`, `supports_fanout = True` | A broadcast stream per fanout exchange (`_celery/<exchange>`); each consumer subscribes from `latest` | Needed for remote control (§7.1.5) |

`Transport.implements` declares `direct`, `topic` and `fanout` exchange types and `asynchronous = False` in J1. `driver_type = "loams"`.

#### 7.1.3 Priorities, ETA and retries

- **Priorities.** kombu's virtual transports clamp priority to 0–9 (`base.py:472-473, 854-872`); Redis serves lower numbers first by polling priority lists in ascending order (`redis.py:1266-1272, 1451-1480`), and a message with no priority counts as 0. `loams-celery` maps 0 or unset → Loams's FIFO band, and 1–9 → Loams priority 1–9, which reproduces the Redis order exactly. A transport option `priority_order = "amqp"` reverses it for apps written for RabbitMQ, where higher is higher. `task_queue_max_priority` only sets an AMQP queue argument (`app/amqp.py:97-100`) and is ignored.
- **ETA and countdown.** Celery workers hold an ETA task **unacked in memory** until it is due (`worker/strategy.py:180-208`), which is why an ETA longer than the visibility timeout makes Redis and SQS redeliver it "again, and again in a loop" (Celery's Redis docs, `redis.rst:303-334`). `loams-celery` puts the job in Loams's delayed index instead (§6.3), so the worker receives it only when it is due and never holds it. This is a behaviour change for the better; the ETA semantics (not before) are unchanged.
- **Retries.** `Task.retry` publishes a **new message with the same task id** (`app/task.py:767-873`). The transport therefore uses `(id, retries)` as the `job_key`, never the id alone, or a retry published while the original is still active (`acks_late`) would collide with it.
- **Rate limits and time limits** are enforced by the Celery worker (`rate_limit` with a kombu `TokenBucket`, `worker/consumer/consumer.py`; time limits by the pool, `request.py:362-373`) and need nothing from the broker. Loams's queue rate limit (§6.9) is an optional global limit on top.

#### 7.1.4 The result backend

`LoamsBackend` subclasses Celery's `BaseKeyValueStoreBackend` (`backends/base.py:1095`) and implements its six primitives, `get`, `mget`, `set`, `delete`, `incr` and `expire`, over a small **result store** in `loams.jobs.v1` (`ResultGet`, `ResultMGet`, `ResultSet { ttl }`, `ResultDelete`, `ResultIncr`, `ResultExpire`), kept in the job store under `results/<ns>/…` with a TTL (`result_expires`, default 1 day). Because it sets `implements_incr = True`, Celery uses the **native chord counter** (`_apply_chord_incr`, `on_chord_part_return` with `incr`, `base.py:1100-1365`): each header task's completion calls `ResultIncr { key: chord counter, member: task id }`. The counter is stored as the set of members that counted, and `ResultIncr` adds the member and returns the new size in one TiKV transaction, so **each `(group_id, task_id)` counts at most once**: a redelivered or re-run header task (a crash after `incr` and before the ack, or a stalled lease) adds nothing, and only the call that moves the size to the chord size gets `reached: true` and sends the body, so the body is sent once. A count is never skipped by a crash either: `on_chord_part_return` runs before the task is acked under `acks_late` (the setting the docs recommend for chords), so a worker that dies before `incr` leaves the job to be redelivered, and the re-run counts. With early acks a crash between the ack and `incr` loses the count, as it does on Redis; the docs say so. Without `incr`, Celery falls back to the `celery.chord_unlock` task, which polls and retries forever (`app/builtins.py:37-79`); Loams avoids that. `AsyncResult.get` polls through `wait_for_pending` in J1; J1.x adds push waiting over `watch` (as the Redis backend does with `AsyncBackendMixin`). `GroupResult.save/restore` are plain `set`/`get`.

#### 7.1.5 What depends on the broker type, and what does not work

Some Celery features are switched on by a hard-coded list of broker `driver_type`s, not by transport capabilities **(source)**:

| Feature | Condition in Celery | On Loams |
|---|---|---|
| Remote control (`inspect`, `control`, `revoke` broadcast), through the pidbox fanout mailbox (`app/control.py:439-441`) | `conninfo.supports_exchange_type("fanout")` (`worker/consumer/control.py:31-33`) | **Works**: the transport declares `fanout` and implements `_put_fanout` |
| Events (`celery events`, Flower) on the `celeryev` topic exchange (`events/event.py:15`) | Switched to fanout only for `driver_type` `redis` or `gcpubsub` (`:58-60`); topic needs shared bindings | **Works** with shared bindings (§7.1.2). Loams's own job events (§6.6) are the better monitor; Flower compatibility is best-effort |
| Mingle (sync revoked tasks at worker start) | `driver_type` in `{amqp, redis, gcpubsub}` (`worker/consumer/mingle.py:25,34`) | **Off.** Revokes still reach running workers through the broadcast; a worker that starts later does not learn earlier revokes (use `--statedb`, or cancel waiting jobs through Loams's `JobOp::Cancel`, which removes them from the queue for good) |
| Gossip | `driver_type` in `{amqp, redis}` (`gossip.py:34,79`) | **Off** |
| `worker_disable_prefetch` | Redis only (`consumer/tasks.py:60-67`) | Not needed: `lease` takes exactly the free slots |

An upstream Celery issue proposing capability checks instead of `driver_type` lists is worth filing later (§12.3); nothing here depends on it.

#### 7.1.6 Beat

Beat has **no leader election**: Celery's docs say to run exactly one scheduler, "otherwise you'd end up with duplicate tasks" (`docs/userguide/periodic-tasks.rst:19-20`). `LoamsScheduler` removes that constraint. On start it upserts every `beat_schedule` entry as a Loams schedule (`Jobs::schedule`, id = the entry name), whose target enqueues the entry's task message into its queue. Loams's server fires the schedule, not the beat process, so running beat twice, or not at all after the first sync, cannot duplicate or miss a tick. `crontab` entries map to cron; `timedelta` entries map to `every` (§8.1). `solar` entries are refused with a message to keep the standard scheduler for them (Q101). `loams-celery sync-schedules app` does the same upsert from CI without a beat process.

#### 7.1.7 Canvas

Queue mode runs Celery's canvas **unchanged**, because the canvas lives in the messages and the result backend: a chain carries its remaining steps in the message body (`options['chain']`, protocol 2, `canvas.py:906-960`, `app/amqp.py:400-405`) and the worker publishes the next step after success (`app/trace.py:426-470`); a group is several publishes plus a saved `GroupResult`; a chord uses the native counter (§7.1.4). Celery itself notes that the chain hand-off is not atomic (`app/trace.py:435-437`): a worker that dies between a task's success and publishing the next step leaves the chain stuck or, with `acks_late`, runs the step twice. Durable mode (§8.3) is the fix for chains where that matters.

#### 7.1.8 Durable mode for Celery

The owner's "optionally swap `@app.task` for a Resonate decorator" is done with Resonate's Python SDK (0.8.1, the version that server 0.10.1 accepts, §21 §4) and no Loams decorator:

```python
import asyncio
from resonate.retry import Exponential
import loams                                  # the thin helper: URL, token, tenant, group from LOAMS_* env

resonate = loams.resonate(group="billing")    # = Resonate(url=…, token=…, group="billing")

@resonate.register(name="charge", version=1)
async def charge(ctx, order_id: str):
    hold = await ctx.options(retry_policy=Exponential(delay=1, max_retries=5)).run(reserve, order_id)
    # ctx.run checkpoints the result, but a crash after capture and before the
    # checkpoint runs the step again: side effects are at least once. Each
    # external call therefore gets an idempotency key made from the promise id
    # and the step, which the payment and mail APIs deduplicate on.
    receipt = await ctx.run(capture, hold, idempotency_key=f"{ctx.id}:capture")
    await ctx.run(send_receipt, receipt, idempotency_key=f"{ctx.id}:send_receipt")
    return receipt

@app.task(bind=True, acks_late=True)         # stays a Celery task: routing, priority, rate limits
def charge_task(self, order_id):
    handle = resonate.run(f"celery:{self.request.id}", charge, order_id)  # id = the job: a redelivery resumes
    return asyncio.run(handle.result())
```

The Celery message still carries admission (queue, priority, rate limit, ETA). The Resonate promise id is derived from the Celery task id, so a redelivered task **resumes** the same durable execution instead of starting it again: `run` with an existing id returns a handle to the same promise (`py/resonate.py:574-575`), finished steps return their memoized values, and only one worker executes it at a time (Resonate's task `version` fence). A chain rewritten as sequential `ctx.run` calls, a group as parallel calls awaited together, and a chord as that plus a final step, become atomic in the sense Celery's canvas is not: a crash anywhere resumes from the last finished step. Beat entries that target durable functions become Resonate schedules directly (`resonate.schedule(...)`, `py/resonate.py:793-842`).

### 7.2 BullMQ: `@loams/bullmq` (D209)

**Finding: BullMQ v6 already has a pluggable backend, so Loams implements it instead of imitating the API.** BullMQ 6.3.9 (MIT, 2026-09-28) defines `IQueueBackend`, "a database-agnostic contract describing every high-level operation that the Queue, Worker and Job classes need" (`src/interfaces/queue-backend.ts:26-61`), with about 81 methods, and ships a Redis backend and a PostgreSQL backend (`src/postgres/`, 81 SQL command files, LISTEN/NOTIFY for wake-ups) that claims full parity: flows, schedulers, rate limiting, priorities, delays, deduplication, metrics and events (`docs/gitbook/guide/postgresql.md:349-363`) **(source)**. A backend is injected as the last constructor argument (`queue-base.ts:38-82`, `flow-producer.ts:121-151`) or process-wide with `setDefaultBackendFactory` (`src/utils/create-backend.ts:120`); custom backends are documented (`docs/gitbook/guide/connections.md:205-267`).

So `@loams/bullmq` is **a `BackendFactory` for the unmodified `bullmq` package** (a peer dependency bounded to the BullMQ minors whose backend-neutral suite passed against Loams: `>=6.3.9 <6.4` at J2; each later minor is added to the range only after the suite passes on it, J-R1), plus a module that re-exports BullMQ's classes already bound to it with `withBackend` (`src/utils/with-backend.ts`):

```ts
// Option A: change the import (the owner's "drop-in")
import { Queue, Worker, FlowProducer, QueueEvents } from "@loams/bullmq";
// Option B: keep `from "bullmq"` and set the backend once at startup
import { setDefaultBackendFactory } from "bullmq";
import { loamsBackend } from "@loams/bullmq";
setDefaultBackendFactory(loamsBackend({ url: process.env.LOAMS_JOBS_URL, token: process.env.LOAMS_TOKEN }));
```

The `Queue`, `Worker`, `Job`, `FlowProducer` and `QueueEvents` code users run is BullMQ's own, so the API is not re-implemented and cannot drift. Loams owns only the backend.

#### 7.2.1 The backend mapping

| `IQueueBackend` group | Loams call |
|---|---|
| `addJob`, `addJobs` | `enqueue`, `enqueue_bulk` (jobId → `job_key`; `deduplication` → `dedup`; `delay`, `priority`, `lifo`, `attempts`, `backoff`, `removeOnComplete/Fail` → the same options) |
| `addFlow` | `flow` (§8.2) |
| `waitForJob(blockTimeout)` | `lease` with `wait` (the doc comment on `waitForJob`, `queue-backend.ts:768`, expects non-Redis backends to use notifications or polling) |
| `moveToActive` | `lease`. BullMQ's worker token (a string it generates) maps to the `LeaseToken` in the backend's memory |
| `extendLock`, `extendLocks` | `extend` |
| `moveToFinished` (completed / failed), `moveToDelayed`, `moveToWaitingChildren`, `retryJob` | `complete` with `Ok`, `Retry`/`Fail`, `Delay`, `WaitChildren` |
| `moveStalledJobsToWait` | A no-op returning nothing: Loams's stalled sweep runs on the server (§6.3) and emits the same `stalled` events |
| `setRateLimit`, global rate limit and concurrency | `complete(RateLimited)`, `queue_admin(SetRateLimit / SetConcurrency)` |
| `getCounts`, `getRanges`, job getters, logs, metrics, workers | `query`, `job_admin`, `queue_admin` |
| Job scheduler operations | `schedule`, `unschedule` and their listing (§8.1) |
| `publishEvent`, `readEvents(id, blockTimeout)` (`queue-backend.ts:752`) | `watch` on the queue's event stream; BullMQ's event id is the stream cursor. With `S = 1` it is the partition's offset. With `S > 1` it is an opaque vector cursor (every partition's offset, encoded in one string): the backend merges the partitions by each record's commit timestamp (the TSO of the transaction that wrote its outbox row, ties broken by shard), which keeps per-shard order, and `readEvents(id)` resumes every partition after its own offset, so no event is skipped or repeated. J2 verifies that `QueueEvents` passes `lastEventId` back unparsed; if it does not, queues with `S > 1` keep their BullMQ events on one partition |
| pause, resume, drain, clean, obliterate, promote, retry-all | `queue_admin` |

#### 7.2.2 Semantics to preserve, and how

| BullMQ semantic | BullMQ (source) | Loams |
|---|---|---|
| States | completed, failed, active, delayed, prioritized, waiting, waiting-children (`src/types/job-type.ts`); v6 dropped the public `paused` state | §6.4, the same set |
| Priority | 0 = none, served before prioritized jobs; otherwise lower first, up to `2^21 − 1`; score `priority × 2^32 + counter` (`job.ts:43`, `getPriorityScore.lua`) | §6.5, the same model |
| Stalled jobs | Lock with the worker's token for `lockDuration` (30 s default); the checker moves a job with no lock back to wait and fails it past `maxStalledCount` (1 by default) with "job stalled more than allowable limit" (`moveStalledJobsToWait-9.lua:48-118`); jobs from schedulers are exempt | §6.3, server-side, the same counts, message and exemption. The worker's `stalledInterval` and `skipStalledCheck` become no-ops |
| Delivery | At least once | §6.1 |
| Delayed jobs | Delayed zset scored by timestamp, promoted by markers | §6.3 delayed index and promoter |
| Rate limiter | Worker `limiter {max, duration}`, `Worker.RateLimitError`, `queue.rateLimit`, global rate limit and concurrency | §6.9 |
| Deduplication | simple, throttle (`ttl`), debounce (`extend`, `replace`), `keepLastIfActive` (`src/types/deduplication-options.ts`) | §6.2 dedup, the same four modes |
| Job schedulers | `upsertJobScheduler` with `pattern` (cron) or `every`, `limit`, `startDate/endDate`, `tz`, `offset`, `immediately` (`src/interfaces/repeat-options.ts`) | §8.1, Resonate schedules |
| Flows | Parent waits in waiting-children; `getChildrenValues`; `failParentOnFailure`, `continueParentOnFailure`, `ignoreDependencyOnFailure`, `removeDependencyOnFailure` | §8.2 |
| Events | 18 event names read by `QueueEvents` (`queue-events.ts:29-247`); stream trimmed to 10,000 entries by default | §6.6: the same names from the event log; retention by time instead of count (a `trimEvents` call maps to a retention change) |
| Retention | `removeOnComplete/Fail` by count or age | §6.10 |

#### 7.2.3 Conformance

BullMQ's backend-neutral tests (`tests/*.test.ts`, as opposed to `*.redis.test.ts`) exist so that "the existing test suite can run unchanged against another backend" (`create-backend.ts:108-116`) **(source)**. J2's gate is that suite, at the pinned BullMQ version, passing against `loams dev` with `setDefaultBackendFactory(loamsBackend(...))`, with every excluded test listed and justified in the plan. Whether every backend-neutral test also passes on BullMQ's own Postgres backend is unverified; J2 Task 0 runs the suite on Postgres first to learn which tests are Redis-specific in practice.

#### 7.2.4 Why not emulate Redis

The owner's direction already rejected Redis emulation; the source confirms it, and v6 makes it unnecessary:

| | Redis emulation (RESP + Lua in Loams) | `IQueueBackend` (chosen) |
|---|---|---|
| What must match | 49 Lua scripts plus 67 includes, 5,243 lines, using 49 distinct Redis commands (most often `EXISTS`, `XADD` 36 times, `ZSCORE`, `ZREM`, `HGET/HSET`, `RPOPLPUSH`, `LPOS`, `RENAME`), `cmsgpack` in 11 scripts and `cjson`, key names built at runtime inside scripts (which breaks cluster slot rules without `{}` hash tags), `BZPOPMIN` on a marker zset for blocking, streams with `XADD`/`XREAD BLOCK`; a minimum of Redis 5.0 and `maxmemory-policy noeviction` **(source)** | About 81 high-level methods with documented semantics |
| Moving target | Every BullMQ release may change scripts; the emulator must track Lua-level behaviour, not an API | The interface is BullMQ's public contract, with a Redis and a Postgres implementation holding it steady |
| Tenancy, fencing, quotas | Redis has none of Loams's concepts; they would be bolted on under a key-prefix scheme | Native: every call carries the namespace, and locks are Loams's fenced leases |
| Other users | A Redis-compatible server invites every Redis workload, which Loams does not want to support | Only BullMQ |
| Build cost **(estimate)** | A RESP server, a Lua VM with Redis semantics, the keyspace, streams and blocking commands: months, forever | A TypeScript backend of about 2,000–3,000 lines (the Redis one is 3,097) and the Rust core it needs anyway |

#### 7.2.5 Versions, Pro features and Python

- **v6 only.** BullMQ v5 has no backend interface. v6 removed legacy repeatable jobs, `Job#discard`, `debounce` and the public `paused` state (`docs/gitbook/changelog.md:193-218`); v5 apps upgrade to v6 first. **Owner decision (Q100, 2026-09-29): ship for BullMQ v6 only; no v5 shim.**
- **BullMQ Pro is out of scope.** Pro is commercial and closed (`@taskforcesh/bullmq-pro`); its features (groups and their rate limits and concurrency, batches, observables and cancellation) are not implemented and not imitated.
- **Python.** BullMQ's Python package (3.2.7, MIT) has pluggable backends too (`python/bullmq/backends`). A Python `loams` backend is a small follow-up once the TypeScript one passes (J2.x).

#### 7.2.6 Durable mode for BullMQ

```ts
import { Worker } from "@loams/bullmq";
import { loamsResonate } from "@loams/durable";        // thin helper: new Resonate({ url, token, group })
const resonate = loamsResonate({ group: "emails" });

function* sendCampaign(ctx, campaignId: string) {
  const list = yield* ctx.run(loadRecipients, campaignId);
  // At least once: a crash after sendBatch and before its checkpoint resends the
  // batch, so the mail API deduplicates on a key made from the promise id and step.
  for (const [i, batch] of chunk(list, 500).entries())
    yield* ctx.run(sendBatch, campaignId, batch, `${ctx.id}:batch:${i}`);
  return list.length;
}
const campaign = resonate.register("sendCampaign", sendCampaign);

new Worker("campaigns", async (job) =>
  (await campaign.beginRun(`bullmq:campaigns:${job.id}`, job.data.campaignId)).result());
```

The same rule as Celery: BullMQ keeps admission (priorities, limiter, delays, schedulers), and the promise id derived from the job id makes a redelivered or stalled job resume. `FlowProducer` flows map to Loams flows (§8.2), whose joins are promises; job schedulers are Resonate schedules (§8.1).

## 8. How Resonate is used (D210)

### 8.1 Schedules

- `Jobs::schedule` creates a Resonate schedule (`schedule.create`) with a cron expression and a promise template. When it fires, the server expands `{{.id}}` and `{{.timestamp}}` in the promise id and inserts the promise with `INSERT OR IGNORE`, so a tick fires once even if the schedule is processed twice (`process_schedule_timeout`, `resonate-server-sqlite` `lib.rs:3725-3790`) **(source)**.
- **Target.** A schedule that enqueues a job targets Loams's own group (`inproc://any@loams`, §21 §3.5): the tick runs a small Loams durable function that calls `enqueue` with the tick's promise id as the idempotency key, so a tick enqueues exactly one job. A schedule that runs a durable function targets the user's group directly (`poll://any@<group>`).
- **`every` intervals** (BullMQ's `every`, Celery's `timedelta` entries) that a cron expression cannot express become a Loams durable function that loops `ctx.sleep(interval)` → `enqueue` with the tick number in the idempotency key. `limit`, `startDate/endDate`, `offset` and `immediately` are fields of the schedule record that the firing function checks. Time zones (`tz`) are applied by Loams when it converts the rule to the server's UTC cron (verify that Resonate's cron parser has no time-zone field).
- **Upsert.** `schedule` with an existing id and a different rule replaces it (BullMQ's `upsertJobScheduler`; Celery beat on restart): delete and create in one Loams operation, idempotent by id.

### 8.2 Flows

A flow (a Celery canvas submitted through `Jobs::flow`, a BullMQ `FlowProducer` tree, or a DAG from the SDK) is a **Loams durable function** in group `loams`, run by the Rust SDK in process (§21 §3.5):

```rust
pub struct FlowSpec {
    pub id: Option<String>,            // idempotency: the flow id = the root promise id
    pub nodes: Vec<FlowNode>,          // each: queue, task, opts, depends_on: Vec<NodeId>
    pub on_child_failure: FailurePolicy, // Fail | Continue | Ignore | Remove (BullMQ's four flags), per node
}
```

1. The root promise id is the flow id. The function enqueues every node whose dependencies are met, with the step's promise id as the idempotency key, and records `parent` on each job.
2. Each node has a completion promise, `flow:<id>:<node>`, in the root's origin (so it commits atomically with the parent, §14 §1). `complete` of a flow job writes an outbox action that settles that promise; the relay retries the settle until Resonate accepts it, and `promise.settle` of a settled promise is idempotent.
3. The function awaits the promises of the next wave and continues: a chain is one node per wave, a group is one wave of N, a chord is a group followed by a node that depends on all of them, and BullMQ's tree is waves from the leaves up (the parent in `waiting-children` until its children's promises settle).
4. A crash of the node running the flow function resumes it elsewhere after Resonate's task retry timeout (30 s by default, `serve.rs:143-145`), from its memoized steps.
5. Large flows are capped per origin (§21 Q42) and split into child flows with their own origins.

BullMQ's `getChildrenValues` reads the children's results from the job store; the promises only carry completion.

### 8.3 Queue mode and durable mode

| | Queue mode | Durable mode |
|---|---|---|
| What runs the task | The framework's worker (Celery, BullMQ) | The framework's worker, which calls a Resonate function |
| What a retry does | Runs the task again from the start | Resumes the function from its last finished step |
| Resonate state per job | None | One root promise (id derived from the job) plus one per step |
| Cost | One TiKV transaction per enqueue, lease and completion | Plus the durable writes of every step (SQLite or TiKV, §21 §3.3, D261), and the promise retention question (§21 Q40) |
| When to use | Short, idempotent tasks; most jobs | Multi-step tasks with side effects that must not repeat (payments, emails, paid model calls, long pipelines) |

Queue-mode jobs never create Resonate promises. That keeps the per-job cost at a few TiKV writes and keeps millions of short jobs out of the durable store, whose settled promises are never pruned today (§21 §8). Durable mode is chosen per task by the user, by writing the task as a Resonate function; Loams does not decide it.

**Why the queue is not built on Resonate tasks.** Resonate's tasks have a fenced lease (the task `version`, compare-and-set on acquire, `resonate-server-sqlite` `lib.rs:2983-2999`) and a retry timeout, which is most of a queue. They lack priorities, rate limits, global concurrency, dedup modes, queue listing and counts by state, and they deliver through best-effort transports whose recovery is the 30 s retry timeout (§21 §3.4). Building those on promise tags and searches (scans on SQLite and blob backends, §21 §6.4) would be slower and harder than a purpose-built index in TiKV. So the queue is Loams's, and Resonate does what it is good at: schedules, flows and step durability.

### 8.4 The `loams` helpers

`loams.resonate(group=…)` (Python) and `loamsResonate({ group })` (TypeScript, `@loams/durable`) return a Resonate client configured from `LOAMS_URL`, `LOAMS_TOKEN` and `LOAMS_NAMESPACE`: the durable endpoint, the bearer token (every Resonate SDK has a `token` option: Python `token=` or `RESONATE_TOKEN`, `py/resonate.py:293`; TypeScript `token`/`tokenProvider`, `ts/resonate.ts:102-129`), and the group. They add nothing to Resonate's API: no decorators, no wrappers around `run`, `rpc`, `ctx` or retry policies. Users who prefer can construct `Resonate(url=…, token=…)` themselves.

## 9. PySpark on Sail (D211)

This section follows the data-processing research of 2026-09-29 (`.superpowers/research/rust-data-processing-2026-09.md`, not committed; "the research" below) and this document's own check of Sail at `1f6bcde0`. They agree on every point used here.

### 9.1 What Sail covers

Sail 0.7.1 (2026-08-24, Apache-2.0) is a Spark Connect server in Rust on DataFusion 55.1 and arrow 59.2 **(source)**. It accepts PySpark 3.5, 4.0, 4.1 and 4.2 clients (`pyspark[connect]` or `pyspark-client`).

| Area | Sail | Source |
|---|---|---|
| Spark SQL and the DataFrame API | Supported. Spark 3.5.9 Connect tests: 909 passed, 93 failed (90.7 % of non-skipped); Spark 4.2.0: 1,990 passed, 801 failed (71.3 %) | Research §2.6 (Sail CI on PR #2681) |
| Python `udf`, `pandas_udf`, UDTFs, `mapInPandas`, `mapInArrow`, `applyInPandas` | Supported (run in Sail's embedded CPython) | Sail docs, `guide/dataframe/features.md` |
| RDD, `SparkContext` | **Not available**: the Spark Connect protocol does not carry them (Spark's own docs: "APIs such as SparkContext and RDD are unsupported in Spark Connect") | Spark 4.2 Connect overview |
| Java and Scala UDFs, MLlib, pandas-on-Spark, `applyInPandasWithState`, ORC, `CacheTable` | Not supported or planned | Sail docs; research §2.6 |
| Structured Streaming | Not ready (Sail's pages disagree between "partial" and "planned"; Iceberg and Delta streaming are unsupported) | Sail docs; research §2.6 |
| Iceberg | v1–v3, merge-on-read DELETE/UPDATE/MERGE with Puffin deletion vectors, REST, Glue, Unity and HMS catalogs; no branch or tag writes | `guide/sources/iceberg/features.md` (2026-09-28) |
| Object storage | S3 (so RustFS), R2, GCS, ADLS, HDFS; Sail's own storage settings, not Hadoop `s3a` | `guide/storage` |
| Deployment | `sail spark server --port 50051` (Spark's own default is 15002); Kubernetes with `SAIL_MODE=kubernetes-cluster`: a server Deployment and Service, a driver per session that launches worker pods, object-store shuffle (0.7) | `guide/deployment/kubernetes.md` |

**Consequence for D55.** D55 budgets an upstream contribution of deletion-vector reads to Sail. Both the research and this check find them already on Sail's main branch. The M4 work becomes a verification against a released Sail (the research's recommendation), and this document records that as a note on D55, not a new decision.

### 9.2 How Loams runs Sail

- **Never linked.** Sail's crates are not on crates.io, are on DataFusion 55.1 / arrow 59.2 against Loams's 54 / 58 (§11), and `sail-spark-connect` pulls an embedded CPython through pyo3. D51 holds: Sail is a separate process.
- **One Sail per namespace.** Sail runs tenants' Python UDFs inside its own process, so a shared server would run one tenant's Python beside another's data. Each namespace that uses Spark gets its own Sail server (a Deployment in `kubernetes-cluster` mode in cloud; a child process in `loams dev`), scaled to zero after an idle period and started on the first connection (Q96).
- **The endpoint.** Clients connect to `sc://<ns>.spark.<cloud-domain>:443/;use_ssl=true`; the URI carries connection settings only, never the token, so it cannot leak through diagnostics or proxy logs. The token travels as `authorization: Bearer <loams token>` gRPC metadata: `loams.spark_session()` builds the session with a PySpark `ChannelBuilder` subclass that adds that header from `LOAMS_TOKEN` (PySpark's own `token=` URI parameter produces the same header, but the docs do not show it; verify the metadata path for each PySpark version). A Spark Connect proxy in Loams's gateway role authenticates the token, finds the namespace's Sail Service, starts it if needed, and forwards the gRPC stream, keeping a session on one Sail server (sticky by Spark Connect's `session_id`). Loams does not implement Spark Connect; it routes to Sail.
- **Data.** Sail's Iceberg REST catalog points at Lakekeeper with credential vending (§10 §4), so tables live on RustFS as standard Iceberg (M4). Loams collections are read and written with the `format("loams")` Python data source (D54, M2), which runs on Sail unchanged (§17 §5.6).
- **Batch jobs.** `submit_engine_job(EngineJob::SparkBatch { entrypoint, args, conf, python_deps })` runs a PySpark script (uploaded to the object store) in a short-lived driver container against the namespace's Sail endpoint. The run is a Loams durable workflow: start the container, stream its logs into the run's events, record the exit status, retry by policy. `schedule` can target it, so "run this Spark job nightly" needs no Airflow.

### 9.3 Resonate at the job level only

A PySpark pipeline with several actions can be written as a Resonate function whose steps are Spark actions:

```python
@resonate.register(name="daily_features")
async def daily_features(ctx, day: str):
    await ctx.run(build_sessions, day)       # spark.sql(...).writeTo("t.sessions").overwritePartitions()
    await ctx.run(build_features, day)       # a crash here resumes at build_features
    await ctx.run(publish, day)
```

Each step is a checkpoint; a failed step retries; a finished step is not re-run. Steps must write idempotently (an Iceberg partition overwrite or a MERGE keyed on the day, never a blind append), because a step that crashed after its write and before its checkpoint runs again (§6.1). Resonate never enters Spark tasks: task retries, stage recomputation and shuffle recovery are Sail's (or Spark's) own.

### 9.4 The fallback: Apache Spark on Kubernetes

Jobs that use RDDs, `SparkContext`, JVM UDFs, MLlib, pandas-on-Spark or Structured Streaming run on **Apache Spark 4**, unmodified:

| Job | Where it runs |
|---|---|
| Spark Connect-compatible, but uses a feature Sail lacks | Apache Spark's own Spark Connect server (port 15002) for the namespace; the client only changes the URL |
| RDD, `SparkContext`, Scala or Java | `spark-submit` through the Kubeflow Spark Operator (Apache-2.0) as `submit_engine_job(EngineJob::SparkSubmit { … })` |

Both read the same Iceberg tables through Lakekeeper. This is the only place Loams runs a JVM, and only when a user asks for it; Loams's default deployment has no JVM (research §6). The migration advice is the research's: **run on both**. `loams spark check` runs a job on Sail and reports the unsupported calls it hit (from Sail's errors), so a team learns which jobs need the fallback before moving them.

## 10. Flink (D212)

### 10.1 Flink SQL: RisingWave by default, Arroyo as an option

| | RisingWave | Arroyo |
|---|---|---|
| Licence | Apache-2.0; some features are "Premium" behind a licence key, and the free tier caps Premium features at 4 RWU. The Glue catalog for Iceberg is Premium; the REST catalog is not | MIT OR Apache-2.0 |
| Latest | v3.1.0, 2026-09-21, very active | v0.15.0, 2025-12-01; no release since; main at 0.17.0-dev; Cloudflare owns the roadmap since the 2025 acquisition |
| SQL | Postgres dialect, not Flink SQL | Its own dialect on a DataFusion 48 fork, not Flink SQL |
| Kafka, Iceberg | Kafka, Pulsar, Kinesis sources; Iceberg source, sink and table engine with Lakekeeper | Kafka source and sink; Iceberg sink only (REST catalog, two-phase commit) |
| State | Hummock on S3 (RustFS) | Checkpoints to any object store |
| Already in Loams's plan | Yes: the companion stream processor (D22), returning with the Kafka gateway in M5 (D74) | Named as a CI smoke client for the Kafka gateway (research §5) |

**Decision:** Flink SQL jobs move to **RisingWave**, which Loams already plans as its companion. Arroyo is documented as an alternative client, not a managed engine, because of its release cadence and its forked DataFusion (Q95). Neither speaks Flink SQL, so "little change" means a dialect port: sources and sinks become `CREATE SOURCE`/`CREATE SINK` with `connector='kafka'`, time windows use RisingWave's `TUMBLE`/`HOP` table functions, and Flink-specific hints and connectors are dropped. The docs carry a porting table with a worked example for each Flink SQL construct in Flink's own examples; an automatic translator is not planned.

`submit_engine_job(EngineJob::StreamingSql { engine: RisingWave, statements })` runs the statements on the namespace's RisingWave database, as a Loams durable workflow that applies them in order, idempotently (`CREATE … IF NOT EXISTS`, and a recorded statement hash per step). Before M5, RisingWave reaches Loams through its Elasticsearch sink (the ES `_bulk` subset, M1.5), its HTTP sink (the native produce endpoint, M2) and, from M4, its Iceberg sink through Lakekeeper (§02 §7.3, research §5). It reads Loams streams once the Kafka gateway exists (M5).

### 10.2 DataStream jobs: unmodified Flink, managed by the operator

There is no Rust replacement for Flink's DataStream API (research §2.1, §6). DataStream jobs therefore run on **Apache Flink** with the **Flink Kubernetes Operator** (1.16.1, 2026-09-17, Apache-2.0; Flink up to 2.4) **(source)**:

- **Deployment.** One `FlinkDeployment` (application mode) per job, in the namespace's Kubernetes namespace. Checkpoints and savepoints go to RustFS: `state.checkpoints.dir: s3://…`, `s3.endpoint`, `s3.path-style-access: true`, with `flink-s3-fs-presto` for checkpoints and `flink-s3-fs-hadoop` where a job's file sink needs a `RecoverableWriter` (Flink's S3 filesystem docs).
- **Reading Loams.** Through Flink's Kafka connector against Loams's Kafka gateway (M5, D74), or Flink's Iceberg connector against Lakekeeper (M4). Flink needs nothing Loams-specific. Before M5, a DataStream job can write to Loams over HTTP or the ES subset but cannot read Loams streams.
- **Lifecycle through `control_engine_job`:**

| Action | Operator mechanism |
|---|---|
| Deploy | Create the `FlinkDeployment` with `upgradeMode: savepoint` (or `last-state`; `stateless` only on request) |
| Savepoint | Create a `FlinkStateSnapshot` resource (the operator's current mechanism; `savepointTriggerNonce` is deprecated); periodic savepoints with `kubernetes.operator.periodic.savepoint.interval` |
| Upgrade | Patch the spec (image, jar, parallelism, configuration); the operator suspends with a savepoint and redeploys from it |
| Rollback | The operator's rollback to `lastStableSpec` (`kubernetes.operator.deployment.rollback.enabled`, which defaults to `false`; Loams turns it on) or, explicitly, a redeploy of a recorded spec from a named savepoint |
| Suspend, resume, cancel | `job.state: suspended` / `running`; delete the resource |
| Status | `status.lifecycleState` (CREATED … STABLE, ROLLING_BACK, ROLLED_BACK, FAILED) and `status.jobStatus`, mapped onto run events |

Each action is a Loams durable workflow: apply the resource, wait for the operator's status, record the outcome. The steps are declarative applies, so a replay after a crash re-applies the same spec and converges. **Loams does not checkpoint anything inside Flink**: Flink's checkpoints and savepoints are the job's only durable state, and the workflow only records their paths.

- **Flink SQL on real Flink.** A team that must keep Flink SQL unported can run it as a `FlinkDeployment` with the operator's SQL-runner pattern (a small jar that executes a SQL script; verify against the operator's examples at J4). Jobs submitted through the Flink SQL Gateway are not managed by the operator (operator docs), so Loams does not use the Gateway.
- **Later.** A Flink `VECTOR_SEARCH` connector (FLIP-540, Flink 2.2) that calls Loams's hybrid search is a natural adapter for streaming enrichment (research §5, item 6). It is a small Java module outside the Rust workspace and not part of track J.

## 11. Tenancy and security (D213)

- **Every object is namespaced**: queues, jobs, schedules, flows, engine runs, result keys, event streams (`_jobs/<queue>` inside the namespace) and engine deployments. The namespace comes from the credential (`Ctx`), never from a request body.
- **Loopback until the unified auth plan.** The jobs listener binds `127.0.0.1:7720` and refuses other addresses, like the durable listener (D138) and Live (D121), because unauthenticated enqueue and schedule creation are writes. The auth plan (D111, Q30) lifts it and adds the actions `jobs:enqueue`, `jobs:consume`, `jobs:admin`, `jobs:schedule` and `jobs:engine`; an agent's policy (§19 §5.1) can grant `jobs:enqueue` on one queue only.
- **Workers are principals.** A Celery or BullMQ worker holds a token with `jobs:consume` on its queues. A lease token is useless to another namespace's credential.
- **Payloads** are opaque and stay out of ids, tags and logs (§6.7). Client-side encryption is supported by the adapters' serializers (Celery's message signing and custom serializers; BullMQ's job data is user JSON) and by Resonate's encryptor hook for durable mode.
- **Engines** run in the namespace's own Kubernetes namespace with network policies that allow only the Loams endpoints, the object store and Lakekeeper. Sail and RisingWave per namespace (§9.2, Q96); Flink per job.
- **Quotas** (D65): enqueue rate, payload bytes, stored jobs, active leases, schedules, and concurrent engine runs and their CPU and memory.
- **Erasure** (D68): deleting a namespace deletes its job store prefix, result keys, event streams, payload objects and engine deployments. Per-subject erasure cannot look inside payloads; retention bounds how long they stay.

## 12. Dependencies and licences (D216)

### 12.1 Licence matrix

The rule: nothing linked into Loams, and nothing in a Loams-published package's required dependencies, may be AGPL, BSL, SSPL or ELv2.

| Dependency | Licence | How Loams uses it | Linked into the Loams binary? |
|---|---|---|---|
| connect-rust (`connectrpc` 0.9.1; github.com/anthropics/connect-rust redirects to connectrpc/connect-rust) | Apache-2.0 | The `JobsService` server | Yes (already, for Live) |
| buffa 0.9.2 | Apache-2.0 | Protobuf messages | Yes (already) |
| redb 4 | MIT OR Apache-2.0 | `LocalJobStore` | Yes (already a workspace dependency) |
| `tikv-client` (fork), `loams-tikv` | Apache-2.0 | `TikvJobStore` | Yes (already, R1) |
| Resonate server crates and Rust SDK (fork `ostrium-labs/resonate`, `loam/0.10.1`) | Apache-2.0 | Schedules, flows, lifecycle workflows | Yes (already, D1) |
| kube-rs | Apache-2.0 (verify at J3) | Engine runners on Kubernetes | Yes, behind a feature, from J3 |
| Resonate Python SDK 0.8.1, TypeScript SDK 0.11.5 | Apache-2.0 (the monorepo `LICENSE`; the Python package declares no licence field) | Durable mode, the `loams` helpers | No: user processes |
| celery 5.6.3, kombu 5.6.2 | BSD-3-Clause | Required by `loams-celery` | No |
| bullmq 6.3.9 | MIT | Peer dependency of `@loams/bullmq` | No |
| `connectrpc` (Python) 0.12.1 | Apache-2.0 | Generated Python client | No |
| `@connectrpc/connect` 2.2.0, `@bufbuild/protobuf` 2.15.0 | Apache-2.0; protobuf-es is Apache-2.0 AND BSD-3-Clause | Generated TypeScript client | No |
| Sail 0.7.1 | Apache-2.0 | Separate process per namespace | No (D51) |
| RisingWave 3.1 | Apache-2.0, with licence-key "Premium" features | Separate service; Loams uses no Premium feature (REST catalog, not Glue) | No |
| Arroyo 0.15 | MIT OR Apache-2.0 | Documented alternative only | No |
| Apache Flink, Flink Kubernetes Operator 1.16.1, Flink Kafka and CDC connectors | Apache-2.0 | Separate services | No |
| Apache Spark 4, Kubeflow Spark Operator | Apache-2.0 | The fallback (§9.4) | No |

Excluded: **BullMQ Pro** (commercial), **Ververica Platform** (proprietary), **RisingWave Premium features** (licence key), Resonate's `resonate-server-scylladb` and NATS pieces (BUSL-1.1 lineage, §21 §11.1). None of the linked dependencies is AGPL, BSL, SSPL or ELv2.

### 12.2 Published packages

| Package | Registry | Licence |
|---|---|---|
| `loams-celery` | PyPI | Apache-2.0 |
| `@loams/bullmq` | npm | Apache-2.0 (BullMQ's MIT notice kept for anything adapted from its Postgres backend) |
| `loams` helpers (in the existing Python SDK and a new `@loams/durable`) | PyPI, npm | Apache-2.0 |
| Generated `loams.jobs.v1` clients | Inside the SDKs | Apache-2.0 |

Package names follow D400 (`loams` everywhere): the Python SDK is `loams`, the helper is `loams.resonate`, and the Celery transport is `loams-celery`.

### 12.3 Upstream proposals (none required; none posted by this document)

| Project | Proposal | Why |
|---|---|---|
| kombu | An entry-point group for transports (`kombu.transports`), as kombu already has for serializers and matchers | Removes the import-order dependency of the `loams` alias (§7.1.1) |
| Celery | Capability checks instead of `driver_type` allow-lists for mingle, gossip and event fanout | Lets third-party transports turn them on (§7.1.5) |
| BullMQ | None expected; report any `IQueueBackend` gaps found by J2 | The interface is new in v6 |

## 13. Risks

| # | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| J-R1 | BullMQ's `IQueueBackend` is new (v6, 2026-07) and changes between minor versions | Medium | Medium | Pin the peer range; run BullMQ's suite per BullMQ release in CI; the backend is small |
| J-R2 | Hot keys on busy queues (the ready-index head, the rate bucket, the concurrency counter) cap throughput below Redis | Medium | Medium | Batched leases, queue shards, sharded buckets (§6.5, §6.9); J1 measures and publishes the numbers before claims |
| J-R3 | Users expect exactly-once execution after a broker change and are surprised by redelivery | Medium | High | §6.1 in the docs; durable mode as the fix; the Celery and BullMQ semantics are unchanged from Redis |
| J-R4 | Celery features gated on `driver_type` (mingle, gossip) are missed | Low | Low | Documented; upstream proposal (§12.3) |
| J-R5 | Sail coverage (71 % on the Spark 4.2 Connect suite) disappoints teams on Spark 4 | High | Medium | `loams spark check`, run-on-both, the Spark fallback (§9.4) |
| J-R6 | Arroyo's open-source cadence stalls | High | Low | Not a managed engine (Q95) |
| J-R7 | Flink DataStream users need Loams streams before M5 | Medium | Medium | Iceberg (M4) and write paths before M5; the Kafka gateway is the dependency, not new work |
| J-R8 | Per-namespace Sail and RisingWave deployments are expensive for small tenants | Medium | Medium | Scale to zero (Sail); shared RisingWave with a database per namespace for small tenants (Q96) |
| J-R9 | The durable store grows without bound under durable-mode jobs (§21 Q40) | Medium | Medium | Queue mode creates no promises (§8.3); retention (Q40) before durable mode is marketed for high-volume queues |
| J-R10 | A second event path (outbox relay) lags or duplicates events before D72's idempotent producers exist | Medium | Low | Sequence numbers in events until M2 (§6.6) |

## 14. Testing

- **Queue core.** Property tests over random interleavings of enqueue, lease, extend, complete, crash and clock advance against both stores: no acknowledged enqueue is lost; no job has two outcomes; a fenced write is always refused; priorities and delays are respected per shard. The TiKV store runs under `loams-tikv`'s `FaultPlan` (region errors, unknown commit outcomes, TSO restarts).
- **Jepsen-style gate** (with the M2/M5 stream gates): kill nodes during a busy queue; check the history for lost jobs, double completions and stuck leases.
- **Celery.** Celery's own integration suite (`t/integration`) with `broker_url = loams://…` and `result_backend = loams://…`, the canvas tests included; plus Loams's tests for ETA without redelivery, `acks_late` with a killed worker, chord counters under concurrent completion, and beat twice without duplicate ticks.
- **BullMQ.** The backend-neutral suite (§7.2.3).
- **Resonate paths.** A schedule driven through several ticks with the durable debug clock enqueues exactly one job per tick; a flow survives `kill -9` of the node running it; durable-mode Celery and BullMQ examples resume after a worker is killed mid-step, without repeating finished steps.
- **Engines.** Sail: the PySpark examples of §17 and a Sail-vs-Spark comparison on a sample job. Flink: a `kind` cluster with the operator, a stateful job, savepoint → upgrade → rollback, checking state is kept. RisingWave: a ported Flink SQL example producing the same output as on Flink.

## 15. Roadmap: track J (D215)

Track J runs beside M, R and D, interleaved on the one-build machine like tracks R and D (D127). It adds new crates and routes and changes no M1 code before M1 exits.

| Milestone | Scope | Depends on | Exit gate |
|---|---|---|---|
| **J0** | This document and its decision rows | — | Owner review |
| **J1: Celery** | `loams.jobs.v1` and `loams-jobs-proto`; `loams-jobs` with `LocalJobStore`; enqueue, lease, extend, complete, fencing, delays, stalled sweep, DLQ, retention; the jobs listener in `loams dev` (feature `jobs`, loopback); schedules and the `every` loop on the embedded Resonate server; the generated Python client; `loams-celery` (transport, result backend, beat); `TikvJobStore` | D1 (embedded Resonate, in-process runtime), R1 (`loams-tikv`) | Celery's integration suite green on `loams dev`; the property tests on both stores; the Loams Celery tests of §14 |
| **J2: BullMQ** | The generated TypeScript client; `@loams/bullmq` (`IQueueBackend`); flows on Resonate promises; event log and `watch`; `@loams/durable` helper; the Python BullMQ backend (J2.x) | J1 | BullMQ's backend-neutral suite green at the pinned version, exclusions justified |
| **J3: PySpark on Sail** | Engine-runner framework; Sail in `loams dev` and on Kubernetes per namespace; the Spark Connect proxy with auth; `SparkBatch` jobs and schedules; the Spark fallback (Connect server and Spark Operator); `loams spark check` | J1; the auth plan (D111) for the proxy beyond loopback; M2 (`format("loams")`); M4 (Iceberg through Lakekeeper) for tables | PySpark examples pass on Sail through the proxy; a Sail-unsupported job runs on the fallback unchanged; Iceberg tables written by Sail read back in Loams (M4) |
| **J4: Flink** | `StreamingSql` on RisingWave; the Flink operator lifecycle (deploy, savepoint, upgrade, rollback, suspend) as workflows; the porting guide; Arroyo documented | J1, J3's runner framework; M5 (Kafka gateway) for reading Loams streams; M4 for Iceberg | A ported Flink SQL example matches Flink's output; a DataStream job keeps its state across upgrade and rollback on a `kind` cluster, reading Loams through the Kafka gateway |

The first PRs of J1, in order, each small: (1) the protos and generated crate; (2) `loams-jobs` types, the `JobStore` trait and `LocalJobStore` with enqueue/lease/complete and fencing; (3) delays, stalled sweep, retention, DLQ; (4) the listener and `loams dev` wiring; (5) the Python client and the kombu transport; (6) the result backend and chord counter; (7) schedules on Resonate and `LoamsScheduler`; (8) the event outbox and relay; (9) `TikvJobStore` under the fault plan; (10) the Celery integration suite in CI.

## 16. Contradictions with earlier decisions, and how they are resolved

| Earlier | Conflict | Resolution |
|---|---|---|
| D42 and §17 §6.1: "Loams serves no Spark Connect endpoint: Sail is the Spark Connect server; Loams is its source and sink" | The owner's direction has Loams host Sail as the Spark Connect endpoint | D211 amends §17 §6.1: Loams still implements no Spark Connect; it runs Sail per namespace and routes `sc://` connections to it. Sail stays a separate process (D51 holds) |
| D51: no embedded engines | Loams now runs Sail, RisingWave and Flink for users | They are managed services beside Loams, never linked. D51 is about the binary, and it holds |
| §00 §7: stay out of stateful stream processing | Loams manages Flink and RisingWave jobs | Loams manages their lifecycle; the stateful processing and its checkpoints are Flink's and RisingWave's (D212) |
| D55: an upstream contribution of DV reads to Sail in M4 | Sail's main already has them (§9.1) | A note on D55: M4 verifies against a released Sail instead of contributing |
| D1: object storage is the only source of truth | Job state is in TiKV (or redb in dev) | Like Live (D130) and durable execution (§21 §15), jobs are a Loams cloud service whose store is TiKV; payloads and results are on object storage |
| The owner's "drop-in TS package with the same API" | BullMQ v6's backend interface makes re-implementing the API unnecessary | Same user-facing result, less code: `@loams/bullmq` re-exports BullMQ's own classes bound to a Loams backend (D209). Changing the import still works |
| The owner's "Resonate decorators" | Queue-mode jobs do not use Resonate | Durable mode is opt-in per task with Resonate's own decorators (D210); queue mode stays promise-free for cost (§8.3) |

## 17. Open questions

| # | Question | Needed by |
|---|---|---|
| Q95 | Arroyo: documented alternative only (proposed), or a managed engine beside RisingWave; revisit if Arroyo resumes releases | J4 plan |
| Q96 | Engine tenancy: Sail per namespace with scale-to-zero (proposed); RisingWave per namespace, or a shared cluster with a database per small namespace | J3 plan |
| Q97 | ~~Jobs on self-hosted clusters without TiKV~~ **Decided by the owner, 2026-09-29:** jobs require TiKV; redb for `loams dev` only; no Postgres or DynamoDB jobs backend (§6.8, D214) | Resolved |
| Q98 | A server-streamed `LeaseStream` for low-latency workers, beside the long-polled unary `Lease` | J2 plan |
| Q99 | Strict per-key ordering (FIFO groups, like SQS FIFO): out of scope (proposed), or a later queue kind | After J2 |
| Q100 | ~~BullMQ v5 apps~~ **Decided by the owner, 2026-09-29:** ship for BullMQ v6 only; no v5 shim (§7.2.5, D209) | Resolved |
| Q101 | Celery beat's `solar` schedules and custom schedule classes: refuse and keep the standard scheduler for them (proposed), or support them in Loams | J1 plan |
| Q102 | Loams-hosted workers: Celery and BullMQ workers are user processes in J1–J2; whether Loams hosts them (with the CPU-time runtime of doc 24) is decided there | Doc 24 |
| Q103 | The jobs listener on its own port (`7720`, proposed, like Live's 7710) or mounted on the native API listener as a connect-rust service | J1 plan |
| Q104 | The Spark fallback: Loams-managed (Spark Connect server and the Kubeflow Spark Operator per namespace) or documented only | J3 plan |

## 18. Sources

- **Celery** `f0b1320` (main, 2026-09-28; release 5.6.3) and **kombu** `b1ba4ba` (main, 2026-09-28; release 5.6.2): `kombu/transport/__init__.py`, `kombu/connection.py`, `kombu/transport/virtual/{base,exchange}.py`, `kombu/transport/{redis,pgmq,gcpubsub}.py`, `kombu/transport/SQS/__init__.py`, `kombu/transport/native_delayed_delivery.py`; `celery/app/{base,backends,amqp,control,task,builtins,defaults,trace}.py`, `celery/backends/{base,redis}.py`, `celery/canvas.py`, `celery/beat.py`, `celery/result.py`, `celery/events/event.py`, `celery/worker/{request,strategy}.py`, `celery/worker/consumer/{consumer,control,mingle,gossip,tasks}.py`, `docs/getting-started/backends-and-brokers/{redis,sqs}.rst`, `docs/userguide/periodic-tasks.rst`.
- **BullMQ** `d1a43ab` (master, 2026-09-28; package 6.3.9): `src/interfaces/queue-backend.ts`, `src/utils/{create-backend,with-backend}.ts`, `src/classes/{queue,queue-base,queue-getters,worker,job,flow-producer,queue-events,queue-keys,redis-connection,redis-queue-backend}.ts`, `src/commands/*.lua`, `src/postgres/`, `src/interfaces/{worker-options,base-job-options,repeat-options,backoff-options}.ts`, `src/types/{job-options,job-type,deduplication-options,processor}.ts`, `docs/gitbook/guide/{connections,postgresql}.md`, `docs/gitbook/changelog.md`, `docs/gitbook/bullmq-pro/`, `python/pyproject.toml`.
- **Resonate**: the fork `ostrium-labs/resonate` at `e360669` (branch `loam/0.10.1` = upstream `28dfd01` plus five commits: RustSec/OpenSSL hygiene, the GCP ID-token feature gate, MySQL errno classification, TiDB support, SDK-rs without default TLS); `impl/sdk/py/src/resonate/{resonate,context,retry,schedules,promises,codec}.py` (0.8.1), `impl/sdk/ts/src/{resonate,context,options,retries}.ts` (0.11.5), `impl/server/core/crates/resonate-core/src/types.rs`, `resonate-server-sqlite/src/lib.rs`, `core/src/serve.rs`. Loams: `crates/loams-durable` on `main` (the registry, the in-process network and runtime); the TiKV server plugin (`crates/loams-durable/src/tikv.rs`) is in progress in the working tree, not on `main`.
- **connect-rust** `fb5f5aa` (github.com/connectrpc/connect-rust; tag v0.9.1); `connectrpc` 0.12.1 on PyPI; `@connectrpc/connect` 2.2.0 and `@bufbuild/protobuf` 2.15.0 on npm.
- **Sail** `1f6bcde` (0.7.1): `docs/introduction/migrating-from-spark`, `docs/guide/{dataframe/features,sources/iceberg/features,catalog/index,storage,deployment/kubernetes,cli}.md`, `k8s/sail.yaml`, `Cargo.toml`. Apache Spark 4.2 Spark Connect overview; Spark 4.1.0 release notes.
- **Flink Kubernetes Operator** `de630f6` (1.16.1): `helm/*/crds`, `docs/content/docs/managing/snapshot-management.md`, `docs/deployment/overview.md`, `ResourceLifecycleState.java`. Flink 2.3 S3 filesystem and SQL Gateway docs.
- **Arroyo** `0630bec` (v0.15.0): `LICENSE-*`, `arroyo-api/src/rest.rs`, `arroyo-rpc/default.toml`; the 2025-04-10 Cloudflare announcement. **RisingWave** `5149d7b` (v3.1.0): Iceberg overview and Premium features docs.
- **Data-processing research** of 2026-09-29: `.superpowers/research/rust-data-processing-2026-09.md` (not committed): §1 table, §2.1 Flink, §2.2 Arroyo, §2.4 RisingWave, §2.6 Sail, §4–§6.
- **Loams**: §01 §3 (ports), §02 §7 (stream API, idempotent producers), §03 (freshness), §09 §3 (leases), §11 §1, §14, §17 §5.6 and §6.1, §18 §5–§6, §19 §5, §20 §5, §7, §11, §21; `crates/loams-meta-tikv/src/leases.rs` (`check_fence`), `crates/loams-common/src/meta/types.rs` (`Fence`, `Lease`), `crates/loams-live-proto/build.rs`, `proto/loams/live/v1/idempotency.proto`; `crates/loams-stream-grpc` (in progress in the working tree, not on `main`: a tonic `StreamService.Produce` without producer ids yet); D1, D22, D42, D51, D54, D55, D65, D68, D72–D74, D76, D111, D118, D121, D127, D128, D130, D138–D147.
