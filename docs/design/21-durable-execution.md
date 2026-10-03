# 21 — Loams Durable: Embedded Resonate and Durable Patterns

Status: **Proposed** · 2026-09-27. The direction is the owner's, from 2026-09-27: "link resonate plugins to loams binary and implement all the useful resonate patterns for loams". The owner also fixed four points: embed the server rather than run a sidecar; use SQLite in dev, TiDB for Loams cloud now and a native TiKV backend as the target; start with Resonate's HTTP transports; and take the dependencies from a pinned fork. This document turns that direction into decisions **D138–D147** and open questions **Q39–Q44**. The choices it makes on top of the owner's points are **proposals** until the owner confirms them. The first milestone, **D1**, is planned in [`docs/plans/2026-09-27-d1-durable-execution.md`](../plans/2026-09-27-d1-durable-execution.md).

> **Amended 2026-09-29 (D260, D261, D262).** TiKV is the durable store for clusters, Loams cloud and self-hosted deployments: the **native TiKV backend** (`--durable-store tikv://<pd-hosts>/<keyspace>`, cargo feature `durable-tikv`) replaces TiDB (D261). There is no TiDB anywhere in Loams (D260). **What `main` does today:** `--durable-store` accepts `sqlite:<path>` and `mysql://…` only, and the TiKV backend is not merged yet. `mysql://` (Resonate's MySQL plugin, feature `durable-mysql`) is **legacy and deprecated**: kept for dev and tests until `tikv://` lands, then removed. The cargo feature `durable` is **opt-in**, not on by default (D262). TiDB text below is kept as history and marked where it no longer applies.

This document **amends §14** (approved 2026-09-24, D19). §14's protocol analysis, consistency model and non-goals still hold. The following parts change:

| §14 said | §21 says | Why |
|---|---|---|
| Resonate's gateway runs in the `gateway` role (§14 §3) | The server is linked into every `loams` binary behind the cargo feature `durable`, on its own listener (D138) | Owner direction. One binary to ship, no extra role or process, and in-process calls from Loams's own code |
| Storage is `resonate-server-blob` over `loams-store`, under `ns/<ns>/durable/` (§14 §3, §4) | SQLite in dev and standalone; the native TiKV backend (a TiKV `Store` for the blob server) for clusters, Loams cloud and self-hosted deployments (D139 as amended by D261). ~~TiDB (Resonate's MySQL plugin) for Loams cloud now~~: legacy, dev and tests only (D261) | Owner direction. Loams runs TiKV anyway (§20), and no TiDB (D260) |
| Phase A in M3, Phase B in M4 (D19, D46) | A parallel track, **D**, like track R: D1 now, D2–D4 later (D145) | Durable operations are needed by M2 features (erasure, restore, reindex), so they cannot wait for M3 |
| Resonate's own auth is replaced by Loams's (§14 §4) | Unchanged in intent, but deferred to the unified auth plan (D111). Until then the listener is loopback-only and refuses other addresses (D138) | D111 |

Markers: **(spike)** means measured in the linking spike of 2026-09-27 (§13). **(research)** means measured in the Resonate/TiDB research of 2026-09-27 (`.superpowers/research/resonate-tidb.md` in the m1.2a worktree, not committed). **(estimate)** means computed, not measured. **(verify)** means the plan that builds it checks it first. Paths of the form `core/…` point into `resonatehq/resonate` at `28dfd01` (server 0.10.1, protocol `2026-04-01`), under `impl/server/core/`. Paths of the form `sdk-rs/…` point into the same repository under `impl/sdk/rs/`.

---

## 1. Summary

Loams links the Resonate server into its own binary and uses it in two ways:

1. **As a product surface.** Loams speaks the standard Resonate protocol on `127.0.0.1:8001`, so the official Resonate SDKs (TypeScript, Python, Go, Java, Rust) run durable functions against Loams unchanged. Agents and applications get durable promises, tasks, retries, sleeps, schedules and human-in-the-loop gates from the same system that holds their memory and retrieval.
2. **As Loams's own engine for long work.** Loams's own long-running operations become durable functions. These are bulk import, and later restore, reindex, copy, erasure, tenant provisioning and re-embedding. They run on the same embedded server, through the Resonate Rust SDK over an in-process network, and survive node crashes and restarts.

The spike (§13) linked and ran the embedded server. It uses Resonate's public composition API (`resonate_base::build` → `Running::start`/`stop`) beside an axum 0.8 app in one Tokio runtime, with the SQLite backend. The Python fan-out example passed against it, including its crash mode. A human-in-the-loop workflow survived `kill -9` of the whole binary and completed after the restart. **No upstream change is needed to embed a single-tenant server.** Two small upstream changes are recommended:

- **a dependency-hygiene PR.** It clears 7 of the 8 RustSec advisories that `cargo deny` reports against Loams's policy, removes the OpenSSL link and halves the semver-major duplicates. The spike built it with no source change, and the examples still pass.
- **a library-entry PR.** Multi-tenant dispatch needs it later (§12).

## 2. Goals and non-goals

### 2.1 Goals

1. **Standard protocol, unchanged SDKs.** Any Resonate SDK at the server's protocol version (`2026-04-01`) works against Loams with only a URL change. Loams's conformance is Resonate's own linearizability check, run against the Loams binary (§10).
2. **One binary.** Durable execution needs no extra process, container or role. The cargo feature `durable` is opt-in (D262); in a binary built with it (release builds, CI's durable jobs, the Loams cloud build), `loams dev` and `loams standalone` start the durable server by default on SQLite, and `--no-durable` turns it off.
3. **Durable Loams operations.** Every Loams operation that can outlive a request becomes a durable function with an operation id, progress and cancellation. It resumes after a crash, and after a failure it re-runs only the failed branches.
4. **Tenant isolation.** Each tenant's durable state lives in its own store: its own SQLite file or its own TiKV key prefix or keyspace (a TiDB database under the legacy `mysql://` backend). Tenants never share Resonate groups, schedules or searches (§5).
5. **Buy, not build.** Loams writes glue, an in-process network and its own workflows. It does not write a durable-execution engine (user memory: prefer buy over build).

### 2.2 Non-goals

- **No Loams durable-execution SDK.** Users use Resonate's SDKs. Loams's own Rust code uses Resonate's Rust SDK.
- **No protocol fork.** Loams never changes wire formats, status codes or semantics. Everything Loams-specific lives in plugins, config and the dispatcher in front of the server.
- **No rewrite of working M1 code.** M1.3's maintenance loops (split merges, Lance compaction, hot builds, GC) stay lease-fenced worker tasks (§6.1).
- **No workflow DSL or designer.** Resonate's own console (`resonate-gateway-web`) is not linked in D1. The Loams console shows operations (D2).
- **No cross-system transactions.** A workflow step and a Loams data write are not atomic. Steps are made idempotent instead (§6.7; §14 §5 still holds).

## 3. Architecture

```
  Resonate SDKs (TS/Py/Go/Java/Rust)          Loams clients (REST/SDK)
        │ HTTP: protocol + /poll SSE                 │ POST …/import, GET /v1/operations/{id}
        ▼                                            ▼
┌──────────────────── loams binary (feature `durable`) ────────────────────────────────┐
│  durable listener 127.0.0.1:8001            native API :8080 (axum 0.8)               │
│  resonate-gateway-http (axum 0.7)            │  operations API (D146)                 │
│    └ routes: protocol, /poll (http-poll)     │                                        │
│              │                               ▼                                        │
│              ▼                     Loams durable runtime (Resonate Rust SDK 0.6)       │
│   ResonateServer (one per tenant: D1 = one)  ◄──── InProcNetwork: process() in-proc   │
│     server plugin: sqlite | tikv | mysql†    ────► worker_inproc (scheme inproc://)   │
│     workers: http-poll (SSE), http-push (off by default), worker_inproc               │
│              │                                                                        │
└──────────────┼────────────────────────────────────────────────────────────────────────┘
               ▼
   SQLite file (dev, standalone) │ TiKV `loams_durable` keyspace (clusters, cloud, self-hosted)
```

† `mysql` is the legacy TiDB/MySQL backend: on `main` today, deprecated by D261, kept for dev and tests until `tikv://` merges.

### 3.1 The embed (D138)

- **Crate.** A new crate, `loams-durable`, owns the embedding. `loams` depends on it behind the feature `durable`, which is **opt-in** (D262; owner ruling O1: toggling it rebuilds about 480 crates). The default features on `main` are `es`, `flight`, `hnsw` and `qdrant`. Release builds, CI's durable jobs and the Loams cloud build turn `durable` on. A build without the feature has no Resonate code in it, and a `--durable-*` flag only logs a warning.
- **Composition.** `loams-durable` builds a `resonate_plugin::Registry` that names only the plugins Loams carries, then calls `resonate_base::build(&registry, &config, &options)` and `Running::start(debug)`. At shutdown it calls `Running::stop(timeout)`. It never calls `resonate_base::run` or `resonate_base::main`, for two reasons. `run` installs a global `tracing_subscriber` with `.init()`, which panics when Loams already installed one. It also waits on SIGINT/SIGTERM itself (`core/crates/resonate-base/src/lib.rs:171-208`). Both calls it does use are public API **(spike)**.
- **Configuration.** Loams builds the `resonate_plugin::Configuration` with `Loader::new().set(key, value)` from its own flags. It reads no `resonate.toml` and no `RESONATE_*` environment, so an operator's standalone Resonate settings cannot leak into Loams. The key space is Resonate's (`servers.server_sqlite.path`, `gateways.gateway_http.bind`, and so on), so every plugin setting stays reachable through `--durable-set key=value`.
- **Order.** The durable server starts after the metastore and before the native API, because Loams's operations API calls it. It stops after the native API and before the metastore. Resonate's own order (workers, then server, then gateways at start; server, then workers, then gateways at stop) stays inside `Running`.
- **Process-global side effects.** The spike checked the embedded plugins for signal handlers, global subscribers and `process::exit`. The only one is `resonate-gateway-http`'s `abort_on_panic`, which calls `std::process::abort()` on a handler panic (`core/crates/resonate-gateway-http/src/lib.rs:194`). In an embedded server that would take all of Loams down, so Loams pins it to `false`. A panic in a handler answers 500. SQLite's reason for aborting (in-memory state after a panic mid-transaction) is handled by restarting the durable subsystem, not the process (§9).
- **Metrics.** Resonate plugins register with the `prometheus` default registry, re-exported by `resonate-plugin`. Loams's admin listener (`:8090/metrics`, §10 §5) gathers that registry beside its own. `resonate-gateway-metrics` is not linked.
- **In-process calls.** `Running::server()` returns the `Arc<dyn ResonateServer>`. Loams's code calls `process(&RequestEnvelope)` directly, with no HTTP. The spike created a promise this way from an axum 0.8 handler **(spike)**.

### 3.2 The listener (D138)

- **Address.** `127.0.0.1:8001`, set with `--durable-listen`. Port 8001 is Resonate's default. The TypeScript examples hard-code `localhost:8001` and the Python examples fall back to it, so they work unchanged. It is also the port §01 §3 and §10 §2 already reserve for Resonate. No other Loams listener uses it: native 8080–8083, Live 7710, Qdrant 6333/6334, ES 9200, OTLP 4317/4318, admin 8090.
- **Loopback only until the unified auth plan.** The durable API is unauthenticated in D1. It lets any caller create promises, schedule cron jobs and, through the push transport, make Loams send HTTP requests to an address named in a promise tag. So `--durable-listen` accepts only loopback addresses (127.0.0.0/8, `::1`, `localhost`) and refuses any other at startup: `durable listener must be loopback until authentication is configured (D111); got <addr>`. This is Live's rule (R1 Global Constraints), which is stricter than D111's warning for the M1 gateways. The unified auth plan lifts it (§5.3).
- **One listener, Resonate's gateway.** The protocol routes and the poll transport's SSE route (`/poll/:group/:id`) share this listener, as Resonate intends ("one door, one lock", `core/crates/resonate-transport-http-poll/src/lib.rs:95-97`). Resonate's gateway serves it with **axum 0.7** (§11).

### 3.3 Storage backends (D139, amended by D261)

| Backend | Where | Resonate plugin | Verification | Milestone |
|---|---|---|---|---|
| **SQLite** | `loams dev`, `loams standalone`; one file per tenant under the data directory (`<data>/durable/<tenant>.db`) | `resonate-server-sqlite` (rusqlite, bundled SQLite) | Upstream CI differential + porcupine; Loams conformance run (§10) | D1 |
| **TiKV** (native) | `loams cluster`, Loams cloud and self-hosted clusters; `--durable-store tikv://<pd-hosts>/<keyspace>` (for example `tikv://127.0.0.1:2379/loams_durable`) on the TiKV cluster, tenant = key prefix; large tenants get their own keyspace | `resonate-server-tikv`: Loams's server plugin running `resonate-server-blob` over a TiKV `Store` in `loams-durable` (one key per object with a random revision; compare-and-set under a pessimistic lock), cargo feature `durable-tikv` | Blob differential + port differential + porcupine with the TiKV store | **D1** (added by D261, moved from D2); on `main`. The durable handle sweeps only its own expired commit tokens (the metastore keeps the cluster GC loop), and `loams durable migrate` has nothing to do on it. CI runs its TiKV integration test (two servers and a restart); the blob differential and porcupine runs are still to do |
| ~~**TiDB**~~ (legacy `mysql://`) | ~~Loams cloud now~~. On `main` today; **deprecated** (D261): dev and tests only until the TiKV backend merges, then removed. One database per tenant (`loams_durable_<tenant>`) | `resonate-server-mysql` (sqlx 0.8) with the TiDB fixes of upstream PR 0a/1 (pessimistic pin, retryable errnos), cargo feature `durable-mysql` | Engine + port differential and porcupine all pass on TiDB v8.5.8 **(research)** | D1 (legacy) |
| ~~Postgres or blob-on-bucket~~ | ~~Self-hosted clusters without TiDB~~ | — | — | Q39 resolved by D261: self-hosted clusters use TiKV |

- **SQLite is single-node.** `loams cluster` refuses the SQLite backend. The message is `the sqlite durable store is single-node; use --durable-store mysql://… or tikv://…`. A cluster node without `--durable-store` serves no durable execution. A second process opening the same file is refused by the lock file Loams holds on the durable directory.
- **History: why TiDB first (superseded by D261).** TiDB runs Resonate's existing MySQL engine with no new engine code, and it passed the whole upstream bar: engine differential (59,400 steps), port differential and porcupine at 8 × 600 and 16 × 400 **(research)**. It costs about 15 differential steps/s on a one-node playground, against about 1,200/s for in-memory SQLite **(research)**. That is enough for D1's operations. The TiKV `Store` is 600–900 lines **(estimate)**. It inherits the blob server's TLA+-checked design: one CAS'd document per origin (`spec/tlap` `Concrete`). It saves the TiDB SQL layer and pool per tenant. It was D2 because it depends on R1's `loams-tikv` and on the upstream placement discussion; D261 moves it into D1, and it lives in `loams-durable` rather than upstream.
- **(Legacy.) The TiDB pool for durable state was Loams's own system data, not tenant SQL.** D123's rule "one TiDB per tenant keyspace" is about tenant SQL, where the tenant connects. Here only Loams connects, so tenants are separated by database, with one sqlx pool per active tenant (`max_connections = 4`, idle-evicted with the tenant's instance, §5.1).
- **Migrations.** Resonate's SQL plugins refuse to start on a schema behind the binary unless `migrate = true`. Loams sets `migrate = true` for SQLite. For the legacy MySQL backend it runs migrations as an explicit upgrade step (`loams durable migrate`), matching Resonate's intent that DDL is a deployment decision.

### 3.4 Transports (D141)

| Transport | Scheme | Use | State |
|---|---|---|---|
| `resonate-transport-http-poll` | `poll://uni@group/id`, `poll://any@group[/id]` | SDK workers hold an SSE connection to `/poll/…` on the durable listener. It is the SDKs' default | On (D1) |
| `worker_inproc` (Loams) | `inproc://any@loams/<node>` | Loams's own durable functions: messages go straight to the Rust SDK's `recv` callback in the same process (§3.5) | On (D1) |
| `resonate-transport-http-push` | `http(s)://…` | Serverless workers (Lambda, Cloud Run) that Resonate calls | **Off by default**. `--durable-push` enables it. On an unauthenticated server it is a server-side request forgery primitive, because any caller can name any URL as a promise's target. An outbound allowlist and the auth plan are required before it is on in cloud (D2) |
| Loams transport over connect-rust | `loams://…` | Server-streamed delivery over the Live sync stack (§20 §7) | Later (D4). It needs upstream's axum 0.8 bump (PR 3a) or a tower adapter, and a client in at least one SDK (research §5) |
| `resonate-transport-gcps` | `gcps://` | Google Pub/Sub | Not linked |

Delivery is best effort in every transport. A dropped message is recovered by the task retry timeout (`retry_timeout`, 30 s by default), as the protocol intends.

### 3.5 Loams's own durable functions (D141)

Loams's Rust code is a Resonate worker like any other, written with the **Resonate Rust SDK** (`sdk-rs/resonate`, 0.6.0, Apache-2.0). The SDK takes a custom network: `ResonateConfig { network: Option<Arc<dyn Network>>, .. }` (`sdk-rs/resonate/src/resonate.rs:45-46`). Its `Network` trait is `send(String) -> String`, `recv(callback)` and addressing (`sdk-rs/resonate/src/network.rs:12-22`). Loams implements it in-process:

- `InProcNetwork::send` deserializes the request envelope and calls `server.process()`. It never touches the listener.
- `InProcNetwork::recv` registers the SDK's callback with `worker_inproc`. That is a Loams `WorkerPlugin` for the scheme `inproc`, and it turns each routed `Message` (`execute` or `unblock`) into the JSON frame the poll transport would have sent.
- The group is `loams`. Each node's address is `inproc://any@loams/<node_id>`. With a shared backend (TiKV, or legacy MySQL), a task whose retry timeout fires on another node is delivered to that node's in-process worker, so any node can resume any Loams operation.

So Loams's workflows run with the SDK's replay semantics (`ctx.run` checkpoints, deterministic ids, fan-out with `ctx.begin_run`/spawn, `ctx.sleep`, `ctx.promise`) and are not reimplemented. The spike did not build this adapter. D1 Task 6 builds it, with a loopback-HTTP fallback (the SDK's `HttpNetwork` against `127.0.0.1:8001`) if the in-process adapter hits a blocker.

## 4. Protocol, SDKs and versions

- **Server protocol `2026-04-01`** (server 0.10.1). SDK versions must match. The research found that PyPI `resonate-sdk` 0.7.x is refused by server 0.10.1 (`400 Promise ID must be prefixed by resonate:origin`), while the monorepo SDK (`impl/sdk/py`, 0.8.1) works. The spike used 0.8.1 **(spike)**. Loams's docs name the SDK versions that pass its conformance run (§10.3), and a release note announces each server bump.
- **SDK story.** A user points an official SDK at `http://127.0.0.1:8001` (or at the cloud endpoint, once auth exists). Nothing Loams-specific is installed. A durable function that also reads and writes Loams data uses Loams's own SDKs (`loams`, `@loams/client`) inside its steps. The step's promise id is the idempotency key of the write (§6.7).
- **Loams SDK helpers (D2).** The Python and TypeScript Loams SDKs gain `operations.wait(id)` and `operations.get(id)` for the operations API (§6.4). They do not wrap Resonate.

## 5. Tenancy and isolation (D142)

### 5.1 One Resonate instance per tenant

Resonate has no tenant concept. Its groups, schedules, searches and timers are global to one server. Prefixing promise ids per tenant would share schedules, groups (a tenant could subscribe to another tenant's `poll://any@default`) and searches. So isolation is **one `Running` per tenant**: one server, workers and routes each, over the tenant's own store:

| Backend | A tenant's store |
|---|---|
| SQLite | `<data>/durable/<tenant>.db` |
| MySQL (legacy) | database `loams_durable_<tenant>` |
| TiKV | prefix `t/<tenant>/` inside keyspace `loams_durable` (the blob `KeySpace` prefix); a dedicated keyspace `loams_durable_<tenant>` for large tenants, by the size classes of D122 (Q41) |

The **tenant** is the namespace (§18 §6), because quotas, erasure and encryption keys are per namespace. Instances are built on first use, evicted after 15 minutes idle (**estimate**, tuned in D2), and capped per node (LRU). An instance with pending timers is not "idle" if it is the only node serving that tenant. Timers need a running instance, so with a shared backend the router's rendezvous owner of `(ns, "durable")` (§18 §5) keeps the tenant's instance warm.

### 5.2 The dispatcher

A dispatcher in front of the per-tenant instances maps each request on the durable listener to the tenant, from the request's credential. It then serves the request through that tenant's gateway routes. Resonate's `routes::api_routes()` and `AppState` are public (`core/crates/resonate-gateway-http/src/routes.rs:61-83`). `build_app`, which merges the poll route and the auth and CORS layers, is private. So the dispatcher needs the library-entry PR (upstream PR 4, §12), or Loams carries that change in its fork. **D1 has no dispatcher**: it serves one tenant, the `default` namespace, which matches D111's single-tenant M1.

### 5.3 Dependency on the unified auth plan (D111, Q30)

Multi-tenancy needs authenticated principals. The unified auth plan must cover the durable listener:

1. API keys and tokens map to a namespace (§19 §5.3: `env` audience).
2. The Resonate SDKs send `Authorization: Bearer <token>`. Every SDK has a `token` option (Rust: `ResonateConfig.token`), and the protocol envelope carries `head.auth`. Q11's base-path fallback is not needed if every SDK sends the header (verify for the Go and Java SDKs).
3. Actions: `durable:invoke`, `durable:resolve` (settling promises, which the human-in-the-loop pattern needs) and `durable:schedule`. Agents get only what their policy grants (§19 §5.1).
4. The non-loopback refusal is lifted only when auth is on, and push delivery only with an outbound allowlist per org.

Until then Loams cloud does not expose the durable listener to tenants. D1's cloud use is internal: Loams's own operations for the `default` namespace of dedicated deployments.

## 6. Patterns (D143)

Each pattern becomes a concrete Loams feature with an owner milestone. For each one the table weighs what durability adds against what it costs.

| # | Pattern | Loams feature | Milestone | v1? | Value vs cost |
|---|---|---|---|---|---|
| d | Async HTTP API (submit, then poll) | `/v1/operations/{id}`, D90's shape; **bulk import from object storage** is the first operation | **D1** | Yes (v1.0 if D1 lands before M2 exits) | High: every onboarding loads data; the API shape is reused by restore, reindex, copy and erasure. Cost: one new API and one workflow |
| b | Fan-out / fan-in | Import fans out per file and per slice; only failed branches re-run | **D1** | Yes | High: TB-scale imports survive crashes without re-reading finished files. Cost: none beyond (d) |
| a | Schedules | **Scheduled incremental import** (bucket sync) | **D1** | Yes | Medium-high: replaces an external cron or Airflow for "load new files from the drop zone". Cost: one schedule and a dedup rule |
| g | Idempotency keys | `Idempotency-Key` → operation id; step promise id → idempotency key of the write; aligned with Live's idempotency records (D118) | **D1** (ops API), R2 (Live actions) | Yes | High, cheap: exactly-once client retries |
| c | Saga with compensation | **GDPR erasure orchestration** across collections, streams, tags, branches, backups, Live keyspaces and durable state, with a deadline sweep | **M2** (erasure path, D68–D69) on D1's engine | Yes | High: D69's 30-day deadline and D115's multi-store log are exactly a durable multi-step process. Cost: the erasure steps exist anyway |
| a | Schedules | **Erasure-deadline sweep** (D69's 30-day completion) | **M2** | Yes | Medium: a durable cron whose run is itself a saga |
| d | Async operations | Restore (M2), online index backfill (D97, M2), collection copy (D90, M2.x) as operations | M2 / M2.x | M2 ones yes | High: each is minutes to hours. Cost: each adds only its own workflow |
| e | Human-in-the-loop | **Approval gates** on destructive operations (collection and namespace drop, erasure, restore over live data) and on agent actions | **D2** (needs the unified auth plan for approver identities) | v1.1 | Medium-high for cloud and agent safety. Cost: needs identities and the console |
| c | Saga with compensation | **Tenant provisioning and deprovisioning**: TiKV keyspace, bucket prefix, OpenFGA store, quotas, directory entry; rollback on failure | **D2**, with R2's router and `ControlStore` (D125) and M2.x's hosted control plane | v1.1 | High for cloud: today a half-provisioned tenant needs manual cleanup. Cost: one workflow per resource kind |
| b | Fan-out / fan-in | Single-collection shard builds (D95), bulk re-embedding | M2.x / D3 | Later | Medium: builds are already lease-fenced tasks; value comes when one logical build spans hours and many nodes |
| a | Schedules / operation | **Re-embedding a whole collection** (a model change) as a durable operation, optionally scheduled | **D3** (needs `embed()` links, §09) | Later | High when it exists: hours of paid model calls must not be redone |
| f | Durable agents | Live **actions** as durable functions; the agent runtime (multi-agent handoffs, deep research with sub-agents as durable calls, MCP long-running tools); agent traces into Loams | **D3** (with R3), D4 | Later | Very high strategically (the AI-native cloud). Cost: Live actions (R2), auth (D111), OTLP traces ingest (Q43) |
| a | Schedules | M1.3 compaction, merges, hot builds, GC | **Not moved** | — | None: see §6.1 |

### 6.1 (a) Schedules: an honest assessment against M1.3

M1.3's maintenance loops are **continuous, state-driven and idempotent**. A `TaskSource` proposes a task when the state says there is work (splits to merge, fragments to compact, objects past the GC grace period). A lease (`task/<key>`) with an epoch fences every commit (`loams-worker`, §09 §3). A crash loses at most one run, and the next poll proposes it again. Each run is short. A cron would **add latency** to event-driven work and **add a dependency** to the most important loops, and it gives them nothing they lack. Durability matters only where one logical job is longer than one task run, or crosses systems:

| Job | Durable? | Why |
|---|---|---|
| Split merge, Lance compaction, hot build, fragment prefetch, GC | No; stays in `loams-worker` | Short, re-derivable from state, fenced |
| Scheduled import (D1) | **Yes** | Hours long, many files, must not re-import |
| Erasure-deadline sweep and erasure (M2) | **Yes** | Multi-store saga with a legal deadline and an audit trail |
| Re-embedding a collection (D3) | **Yes** | Hours of paid calls; per-slice checkpoints |
| Retention of finished operations (D1) | Plain task | A per-tenant sweep; no multi-step state |

Nothing in M1 is rewritten before M1 exits. After M2, the decision is revisited only if a maintenance job grows past one task run (for example, whole-collection rewrites for a format upgrade, §03 §6).

### 6.2 (b) Fan-out / fan-in

A workflow starts N branches with `ctx.begin_run`, or spawns them in the Rust SDK, and awaits them all. Each branch is a child promise in the root's origin, so it commits atomically with the parent's state (§14 §1: single-origin). A failed branch is retried alone, with the SDK's retry policy. Finished branches return their memoized results on replay, as the fan-out example showed: in crash mode only the push branch re-ran **(spike)**. Loams's rules:

- **Branch granularity is bounded.** One branch per file, and inside a file one step per slice (§7.2). A 1 TB import at 64 MiB slices is about 16,000 promises **(estimate)**. That fits SQLite, but it is one origin. The blob and TiKV backends keep an origin as one document, so D2 caps a single origin's promise count (Q42) and splits very large imports into child operations with their own origins.
- **Concurrency is bounded per operation** (`max_parallel_files`, default 4) and per namespace (`max_concurrent_operations`, default 2). The collection write path's backpressure (D86) still applies: a 429 is a retryable step failure.

### 6.3 (c) Sagas with compensation

A saga is a sequence of durable steps, each with a compensating step. On failure, the workflow runs the compensations of completed steps in reverse order. Because the workflow is durable, a crash in the middle of compensating resumes the compensation. It does not leave a half-provisioned tenant. Every step and compensation is idempotent, keyed by the saga's promise id (§6.7).

**Tenant provisioning (D2):**

| Step | Action | Compensation |
|---|---|---|
| 1 | Directory entry `{org, app/namespace, state: provisioning}` in the `ControlStore` (D125) | Mark `failed`, then delete |
| 2 | TiKV keyspace (`ensure_keyspace`, R1 Task 1) or shared-pool prefix (D122) | Disable and GC the keyspace (never reuse its id) |
| 3 | Bucket prefix and envelope key (D96) | Destroy the key (crypto-shred), then delete the prefix |
| 4 | ~~TiDB pool for SQL-enabled tenants (D123)~~ Dropped: no TiDB (D260) | — |
| 5 | OpenFGA store or model tuples (D67) | Delete the tuples |
| 6 | Quotas (D65) and metering rows (D103) | Delete |
| 7 | Durable instance store (§5.1) | Drop the database or prefix |
| 8 | Directory entry `state: active` | — |

**Deprovisioning** runs the compensations as forward steps behind an approval gate (§6.5), with a grace period (`ctx.sleep(7d)`, **estimate**) during which an admin can cancel.

**GDPR erasure (M2)** is a saga whose forward steps are the D68/D69 steps for each store that holds the subject's keys. It is not compensable: erasure is never undone. Failed steps retry until the D69 deadline. The erasure log (D115) is written at start and at completion. The deadline sweep is a Resonate schedule (daily) that starts an erasure run for every request past its soft deadline.

### 6.4 (d) Async HTTP API: submit, then poll (D146)

D90 already fixed the shape for collection copy: `Prefer: respond-async` → `202 Accepted` with `Location: /v1/operations/{id}`. D1 makes it general:

```
POST /v1/namespaces/{ns}/collections/{c}/import        → 202, Location: /v1/operations/op-01J…
GET  /v1/operations/{id}                               → 200 {operation}
GET  /v1/namespaces/{ns}/operations?state=running      → 200 {operations: […], next}
POST /v1/operations/{id}/cancel                        → 202
```

```json
{ "id": "op-01J9Z…", "kind": "collection.import", "namespace": "default",
  "target": {"collection": "docs"}, "state": "running",
  "progress": {"files_total": 120, "files_done": 37, "rows_written": 18233411, "bytes_read": 40128339968},
  "created_at": "…", "updated_at": "…", "result": null, "error": null }
```

- **The operation is a durable promise.** The operation id is the root promise id. `state` maps from the promise and its task: `pending` with no acquired task → `queued`; an acquired or suspended task → `running`; `resolved` → `succeeded`; `rejected` → `failed`; `rejected_canceled` → `canceled`; `rejected_timedout` → `failed` with `error.code = "deadline_exceeded"`.
- **Progress needs no second store.** Branch promises carry tags `loams:op=<id>` and `loams:kind=file`. `progress` counts them with `promise.search`, and the plan step's memoized value holds the totals. Searches are scans on SQLite and blob backends, so progress is cached per operation for 2 s.
- **Cancel** settles the root promise `rejected_canceled`. The workflow checks the root's state between steps and stops at the next slice boundary. Writes already made stay; D1's import reports what it wrote.
- **Retention.** Finished operations are listed for 7 days (**estimate**) and then pruned (Q40).

### 6.5 (e) Human-in-the-loop approval gates (D2)

A destructive request becomes an operation whose first step is `ctx.promise()` (an approval promise, `loams:approval=<op>`). The operation stays `awaiting_approval` until an approver settles that promise, which resolves or rejects it. Approvers settle it in the console or with `POST /v1/operations/{id}/approve|reject`. The approval policy (who may approve, how many approvals, timeout) belongs to the org and is enforced by the auth plan's `Authorizer`. An unapproved request times out (`timeoutAt`, 72 h by default) and is rejected. The same gate serves agent actions: an agent's policy (§19 §5.1) can mark actions as `requires_approval`, and the agent's durable function then waits on the gate. The human-in-the-loop example showed the mechanism survives a crash: the workflow blocked, the whole Loams binary was killed with `kill -9` and restarted, and the resolve then completed the workflow from SQLite state **(spike)**.

### 6.6 (f) Durable agents (D3, D4)

- **Live actions are durable functions.** §20 §6.1 defines actions as non-transactional functions with side effects, never retried automatically (R2). D3 adds `durable: true` actions. Their body runs as a Resonate function, and each `runQuery`, `runMutation`, `fetch` and AI-gateway call becomes a checkpointed step. A crash resumes the action instead of losing it, and a model call is never paid for twice. This replaces §20 §6.1's "R4+ option". The QuickJS runtime needs a JS binding of the Resonate context. It is a host API over the Rust SDK, not the TypeScript SDK (Q35 decides the long-term engine).
- **The agent runtime.** Multi-agent handoffs (researcher → writer → reviewer with a review gate, like the multi-agent example) and deep research (recursive sub-agents fanned out as durable calls) run as durable functions. They run in users' own workers through the SDKs, and later in Loams-hosted workers (D4). Loams supplies the memory (collections), retrieval (`ctx.search`, D129), the durable engine, and the agent identities (§19 §5).
- **MCP long-running tools.** MCP tools that take minutes (imports, research) return an operation id and support polling (the MCP example). Loams's MCP server (§15) exposes `operations.get` as a tool in D3.
- **Agent traces go into Loams.** LLM calls go through an AI gateway (aisix, Apache-2.0, exports OTLP GenAI spans). Durable steps emit OTel spans with `resonate.promise_id` and `resonate.origin` attributes, so an LLM span joins the step that made it. Spans land in Loams through OTLP ingest. D73 covers logs only; traces need an OTLP traces receiver (Q43). A link then maps promises to an execution graph (§14 Phase B §2), so "show me every tool call and model call in this agent run" is a `graph_expand` from the run's root promise.

### 6.7 (g) Idempotency keys everywhere (D146)

- **Operation submit.** An `Idempotency-Key` header on any operation request maps to the operation id: `op-` followed by the first 26 hex characters of SHA-256(namespace ‖ key). A repeated request returns the existing operation (`200`, same `Location`), because `promise.create` with an existing id and the same parameters is idempotent in Resonate. A repeated key with different parameters answers `409 idempotency_key_reused`, with Loams comparing a hash of the canonical request stored in the root promise's `param`.
- **Step writes.** A durable step that writes Loams data uses its own promise id as the write's idempotency key. For collection writes that is natural: imports are upserts by primary key, so a re-run slice converges to the same state. For stream appends (M2, D72) the producer id is `(operation id, branch)` and the sequence number is the slice index. For Live mutations (R2) the step's promise id is the `Mutate` idempotency key, recorded in the same TiKV transaction as the mutation (D118). So a step that crashed after writing and before settling never writes twice.
- **External effects.** Webhooks and external calls made from steps get the step's promise id as their `Idempotency-Key` header, as in Resonate's webhook and money-transfer examples.
- **Promise ids carry no personal data.** Loams's own ids are opaque (`op-…`, hashes). User ids and payloads stay out of ids and tags, because ids and tags are indexed and appear in logs and traces (§8).

## 7. D1's operation and schedule

### 7.1 Why bulk import, and why scheduled import (D145)

The operation must exist on top of M1's code, be long, and make every later operation cheaper. The candidates:

| Candidate | Exists on M1 code? | Long? | Verdict |
|---|---|---|---|
| **Bulk import from object storage** | Yes: M1.2's Flight `DoPut` mapping (`CollectionBatchMapper`) and `CollectionService::write` do the per-batch work; import adds the reading and the orchestration | Minutes to hours | **Chosen** |
| Reindex / online index backfill | No: D97 is M2 | Hours | M2, as an operation on D1's API |
| Snapshot / restore | No: M2 (§10 §6) | Hours | M2 |
| Collection copy | No: M2.x (D90), needs re-keying (D96) | Hours | M2.x |
| Re-embedding | No: `embed()` links are not built | Hours | D3 |

Bulk import wins for four reasons. It is the first thing every user does: loading Parquet from a data lake, or a migration dump from Qdrant, Elasticsearch or pgvector exported to files. It is the longest-running thing Loams does today. It exercises fan-out with partial retry (b), idempotency (g) and the operations API (d) at once. And it establishes `/v1/operations`, which M2's restore, backfill and erasure reuse.

The schedule must add value that a worker loop does not already give (§6.1). **Scheduled incremental import** ("every hour, import new files under `s3://lake/events/`") is chosen. It replaces an external scheduler for the most common continuous-ingest setup short of streaming. It needs only D1's own code. It shows the schedule → promise → task → Loams worker path end to end. The alternatives were weaker: the erasure-deadline sweep (the erasure path is M2), a retention sweep (a plain task is enough) and compaction (§6.1).

### 7.2 Import semantics

```
POST /v1/namespaces/{ns}/collections/{c}/import
{ "source": "s3://bucket/prefix/", "format": "parquet" | "ndjson",
  "pattern": "*.parquet",               // optional glob on the key suffix
  "mapping": { … },                     // optional: same as Flight DoPut's column mapping (M1.2 Task 13)
  "on_error": "fail" | "skip_file",     // default fail
  "max_parallel_files": 4 }
```

1. **Plan step** (`ctx.run`, memoized): list the source prefix through `loams-store` (credentials from the namespace's storage config, §10 §3). Fix the file set as `(key, size, etag)`, sorted by key, and record the totals. Files that change after planning fail their branch (etag mismatch) rather than importing a mix.
2. **File branches** (fan-out): one child per file, at most `max_parallel_files` at a time. A file is cut into **slices**: one Parquet row group per slice (read with `parquet` 58, which matches the workspace's arrow 58), or 64 MiB byte ranges split at newlines for NDJSON (`arrow-json` 58).
3. **Slice steps** (`ctx.run` per slice): read the slice, map it to `DocOp`s with M1.2's `CollectionBatchMapper`, and write it with `CollectionService::write` in chunks of `put_chunk_rows`. Each chunk is whole or not at all. The step's value is `{rows, bytes, token}`. A crash in the middle of a slice re-runs that slice. Upserts by primary key make the re-run converge, and documents without a primary key get ids derived from `(operation id, file, row)`, so a re-run never duplicates.
4. **Fan-in**: sum the branch values. The operation's result carries the **consistency token** covering every write (D76), so a client can read its own import.

The file branch is the unit of retry. With `on_error: fail`, a slice that fails after the SDK's retries fails its branch, and then the operation. Finished files stay imported, and an operation re-submitted with the same `Idempotency-Key` resumes: finished branches return their memoized values. With `skip_file`, the branch records the error and the operation succeeds with `files_failed > 0`.

### 7.3 Scheduled import

```
POST   /v1/namespaces/{ns}/collections/{c}/import-schedules   { "name": "hourly-events", "cron": "0 * * * *", "source": …, "format": …, "pattern": … }
GET    /v1/namespaces/{ns}/collections/{c}/import-schedules
DELETE /v1/namespaces/{ns}/collections/{c}/import-schedules/{name}
```

- It maps to Resonate's `schedule.create`. The schedule id is `isched-<hash(ns, c, name)>` and the promise template creates one run promise per tick. A second registration with the same name answers 409, as Resonate's schedule example does.
- **The durable store is the dedup ledger.** Each run lists the source and starts, for every file, a root durable call whose id is deterministic: `impf-<hash(schedule, key, etag)>`. Each file is its own origin, so a run does not await other origins (single-origin rule, §14 §1). Resonate deduplicates on the id, so a file imported by an earlier run is found already resolved and is not read again. A new version of a file (a new etag) is imported again. Runs do not overlap: a run whose predecessor is still listing waits on it.
- The run's result is the list of file-call ids it started. `GET …/import-schedules/{name}` reports the last run and its counts from the tagged promises.

## 8. Observability, retention and erasure of durable state (D147)

- **Logs and traces.** Resonate emits `tracing` events into Loams's subscriber. Every Loams durable function opens a span with `loams.op`, `resonate.promise_id` and `loams.namespace`. OTLP export follows Loams's M2 observability work.
- **Metrics.** Resonate's Prometheus metrics (request counts and latency by kind, timer and sweep counters) appear under Loams's `/metrics` (§3.1). Loams adds `loams_durable_instances`, `loams_operations{kind,state}`, `loams_import_rows_total` and `loams_import_bytes_total`.
- **Retention (Q40).** The protocol has no delete for settled promises (`promise.*`: `create`, `get`, `settle`, `register_callback`, `register_listener`, `search`), and no backend prunes them. Durable state therefore grows without bound. Pruning is not free either. If a pruned promise's id is created again by a late retry, the work runs again, so retention must exceed the longest retry and deduplication horizon of every client. D1 prunes only Loams's own finished operations after 7 days, with a backend-level delete of the root's origin in SQLite and TiDB. A general retention setting is proposed upstream (PR 5).
- **Erasure.** Deleting a tenant drops its durable store (§5.1). Per-subject erasure (D68) cannot reach inside users' promise payloads, which Loams cannot interpret. So the docs tell users to pass references, not personal data, in `param` and `value`, and to use the SDKs' encryptor hook. Erasure then covers durable state by pruning the tenant's settled promises older than the request, plus the key destruction of D96 where payloads are encrypted with the namespace key (D2).

## 9. Failure modes

| Failure | Behaviour |
|---|---|
| Loams node crash (SQLite, `loams dev`) | State is in the SQLite file. On restart the instance reloads, timers re-arm from durable deadlines, and pending tasks are redispatched after `retry_timeout`. The HITL workflow completed after `kill -9` and restart **(spike)** |
| Node crash (TiKV, cluster) | Another node's instance for the tenant fires the task retry timeout and delivers to its own in-process worker (§3.5). SDK workers reconnect to any node's poll endpoint |
| Durable listener port in use | Startup fails with `durable listener cannot bind 127.0.0.1:8001: …`, naming `--durable-listen` and `--no-durable` |
| TiKV (or legacy MySQL) unavailable | The server's `ready()` is false and the gateway answers 503. The operations API answers 503 `durable_unavailable`. Retrieval surfaces are unaffected |
| Commit-time conflict on legacy TiDB/MySQL | Retried (upstream PR 0a's errno classification, carried in the fork). Without it, conflicts in optimistic mode answer 500: 9 % of requests in research run 9 **(research)** |
| Handler panic | 500, no abort (§3.1). With SQLite, Loams then restarts the durable subsystem, not the process (stop the `Running`, reopen, start) |
| Poison step (always fails) | The SDK's retry policy gives up and the branch fails. The operation fails, or skips the file (§7.2). No infinite loop |
| Slow SDK worker | A bounded channel with `try_send` in the poll transport drops the message, and the retry timeout redelivers. The router never blocks |
| Server version skew with SDKs | 400 errors naming the id rule (§4). The conformance run pins the SDK versions Loams documents |

## 10. Testing

### 10.1 What upstream already tests, and what Loams adds

Resonate holds each storage engine to an executable oracle (the engine differential and the port differential) and holds a live server to a Go port of the Lean abstract machine (porcupine) (research §2). Embedding changes none of the engine code, so Loams **does not re-run the engine and port differentials in its own CI**. They run in the fork's CI at the pinned revision, for SQLite and for TiDB via the MySQL plugin (legacy under D261); the TiKV `Store` is Loams's, so Loams's CI runs its blob and port differentials. Loams tests what embedding adds: the configuration mapping, the listener, the dispatcher (D2), the in-process network, and Loams's own workflows.

### 10.2 The conformance run against embedded Loams (D144)

- **Linearizability.** Start `loams dev` with the durable debug flag (`--durable-debug`, hidden; it sets Resonate's `debug = true`, so `debug.*` is answered and the caller owns the clock). Run upstream's `conctrace --url http://127.0.0.1:8001/ --clients 8 --ops 600`, built from the pinned fork, then `conccheck -partition=false` from `spec/valid/porc`. It must say LINEARIZABLE. This runs per PR on paths under `crates/loams-durable/**` for SQLite, and nightly for the TiKV backend (TiDB before D261), with 16 clients × 400 ops and three seeds.
- **The SDK example suite.** Nightly, against `loams dev`, with the Python examples hello-world, fan-out/fan-in (normal and `--crash`), human-in-the-loop (with `kill -9` of `loams` between suspend and resolve), schedule and money-transfer (saga), and the TypeScript hello-world and fan-out examples. Each asserts its expected output.
- **Checker gap.** Porcupine refutes any history that contains a 503 (research F3). Until upstream PR 0b lands, the nightly leg treats 503s as not applied, using the research's `porc.sh` rule, and reports how many there were.

### 10.3 Loams's own tests

- **Workflow crash tests.** Run an import, kill the durable runtime at a random step (a fault hook between steps), and restart. The final document count is exact, finished files are not re-read (counted by a read hook on the source), and the consistency token covers every write.
- **Schedule dedup.** Drive a schedule through several ticks with debug time. New files are imported once, a changed etag is imported again, and a duplicate registration answers 409.
- **Idempotency.** The same `Idempotency-Key` returns the same operation; different parameters answer 409.
- **Isolation (D2).** Two tenants' instances never see each other's promises, schedules or poll groups.

## 11. Dependencies, duplicates and licensing

### 11.1 Licenses

Every crate Loams links is Apache-2.0: `resonate-base`, `-core`, `-plugin`, `-auth`, `-sql`, `-timer-wheel`, `-server-sqlite`, `-server-mysql`, `-gateway-http`, `-transport-http-poll`, `-transport-http-push`, and later `-server-blob`. So is the Rust SDK. `cargo deny check licenses` against Loams's `deny.toml` passes for the whole embedded graph **(spike)**. That includes the Verus crates that `resonate-timer-wheel` pins (`vstd`, `verus_builtin*`, exact pre-release versions), which are MIT. **Loams never reads, copies or links** `resonate-server-scylladb` or the NATS pieces, because of their BUSL-1.1 lineage (research). The fork's `NOTICE` carries Resonate's attribution.

### 11.2 Source: the fork (D140)

- The dependencies are git dependencies on **`ostrium-labs/resonate`** at a pinned revision, until upstream publishes the server crates. They are not on crates.io.
- The fork branch `loam/0.10.1` is upstream `28dfd01` plus exactly three kinds of commits, each also proposed upstream:
  - the dependency-hygiene commit (PR 0c);
  - the TiDB fixes (PR 0a, PR 1);
  - once needed, the library-entry change (PR 4).
- `deny.toml` gains `[sources] allow-git = ["https://github.com/ostrium-labs/resonate"]`.
- With the hygiene commit, one advisory remains, and it has no fix. It is **RUSTSEC-2023-0071**: `rsa` through `sqlx-mysql`, the Marvin timing attack. `sqlx-mysql` uses RSA only for the `caching_sha2_password`/`sha256_password` key exchange on connections without TLS. Loams connects to TiDB over TLS or inside the cluster network, and the durable TiDB user uses `mysql_native_password`. So `deny.toml` ignores that ID with this rationale.

### 11.3 Duplicates against the Loams workspace **(spike)**

Compared with Loams's `Cargo.lock` on `main`, the embedded graph with the MySQL plugin looks like this:

| | Upstream `28dfd01` | With the hygiene commit |
|---|---|---|
| Crates shared at the same semver with Loams | 209 | 217 |
| New crates | 66 | 55 |
| Semver-incompatible duplicates | 21: `axum` 0.7 (+`axum-core` 0.4, `matchit` 0.7), `rustls` 0.21 (+`rustls-webpki` 0.101, `untrusted` 0.7), `base64` 0.21, `validator` 0.18 (+`idna` 0.5, `darling` 0.20, `syn` 1), `sha2`/`md-5`/`const-oid` (RustCrypto 0.10 line), `spin`, `convert_case`, `synstructure`, `cpufeatures` | 11: `axum` 0.7 (+`axum-core`, `matchit`), `sha2`, `md-5`, `const-oid`, `cpufeatures`, `spin`, `convert_case`, `synstructure`, `untrusted` |
| OpenSSL linked | **Yes**: `reqwest` default features in `resonate-auth` and `-transport-http-push` pull `native-tls`, and feature unification would turn it on for Loams's own `reqwest` | No |
| `cargo deny` advisories | 8: sqlx 0.8.0 (RUSTSEC-2024-0363), `idna` 0.5, `protobuf` 2.28 (via `prometheus` 0.13), three in `rustls-webpki` 0.101, `rsa` | 1 (`rsa`, §11.2) |

- **Why sqlx is stuck at 0.8.0 upstream.** `rusqlite` 0.31 links `libsqlite3-sys` 0.28. Newer sqlx 0.8.x resolves `sqlx-sqlite`'s `libsqlite3-sys` 0.30 into the lockfile even with the sqlite feature off, and two crates cannot share `links = "sqlite3"`. So Cargo can only pick sqlx 0.8.0 **(spike)**. This matters to Loams beyond Resonate: all sqlx 0.8.x versions unify, so once Resonate is in the graph, M2's `loams-meta-postgres` (D58, sqlx) would also be held at the vulnerable 0.8.0. The hygiene commit (rusqlite 0.32, sqlx 0.8.6) fixes both.
- **axum 0.7 stays until upstream PR 3a.** It is isolated. Resonate's gateway serves its own listener, and Loams never passes a router across the boundary. The connect-rust transport (D4) and the multi-tenant dispatcher (D2) are the only places where the two versions meet. They meet as tower services over `http` 1.x.
- **No tonic, prost or hyper duplicates.** Resonate uses none of tonic or prost, and hyper 1.x is shared.

### 11.4 Build and binary cost **(spike)**

Release profile, `CARGO_BUILD_JOBS=4 CARGO_INCREMENTAL=0`, lld, on the 14-core, 15 GB machine. Other agents' builds were running during some runs, as noted.

| Build | Binary (unstripped / stripped) | Time |
|---|---|---|
| Baseline: axum 0.8 hello (the spike's `baseline` bin) | 1.9 MB / 1.3 MB | 23 s cold |
| + Resonate: SQLite, http-poll, http-push, http gateway | 26.8 MB / 19.8 MB | +158 s over the baseline's deps (contended) |
| + the MySQL plugin (TiDB) | 32.2 MB / 23.3 MB | +75 s incremental |
| Everything, cold, fresh target dir | — | 174 s wall; 577 units, 631 s summed unit time. Heaviest: `aws-lc-sys` 74 s, `libsqlite3-sys` 55 s, `protobuf` 2.28 24 s, `google-cloud-auth` 22 s |
| Everything with the hygiene commit | 28.3 MB / 20.6 MB (2.7 MB smaller stripped, no OpenSSL) | 124 s incremental after the dependency bumps (contended) |

In Loams the added cost is smaller than the stand-alone numbers, because `aws-lc-sys`, `rustls`, `tokio`, `hyper`, `reqwest` and most of the tree are already built and linked. The additions are SQLite's C code, sqlx, the Resonate crates, `google-cloud-auth` (through the push transport; the fork can feature-gate it, PR 0c) and `prometheus`. They are about **+8–12 MB stripped and +60–90 s of cold build** (**estimate**). D1 Task 0 measures the real delta of `loams` with and without `durable`.

## 12. Upstream dependency map

| PR | What | Size | Needed by | State |
|---|---|---|---|---|
| **0a** | MySQL commit-time retry classification (sqlx `code()` is the SQLSTATE; errnos 1213/1205 never matched) | ~40 lines | D1 TiDB backend (robustness on optimistic clusters) | Prepared (research); carried in the fork |
| **0b** | porcupine checker: 503 = not applied | ~20 lines Go | D1 TiDB conformance under load | Needs an issue; Loams's run works around it |
| **0c** | **Dependency hygiene** (new): `rusqlite` 0.32, `sqlx` 0.8.6, `validator` 0.20, `prometheus` 0.14, `reqwest` with `default-features = false, features = ["json", "rustls-tls"]` in `resonate-auth` and `-transport-http-push`, versions on internal path dependencies (so `cargo deny`'s wildcard ban and a future crates.io publish pass); optionally a feature for `google-cloud-auth` in http-push | 19 manifests, 52 lines; **no source change** | D1 (advisories, OpenSSL, the sqlx pin that would also hold `loams-meta-postgres` back) | **Proven in the spike**: builds unchanged, examples pass, `cargo deny` down to one advisory; patch saved in the spike directory |
| **1** | TiDB dialect in the MySQL plugin (pessimistic pin, TiDB errnos), xtask backend, CI legs, docs | ~120 lines + YAML | D1 TiDB backend | Prepared (research); carried in the fork |
| **2** | TiKV `Store` for the blob server | 600–900 lines | D2 TiKV backend | Needs an issue on placement; in-tree fallback |
| **3a** | axum 0.8 in `resonate-plugin` | small–medium | D4 connect transport; removes the axum duplicate | Not started |
| **3** | Connect-rust streaming transport | 600–900 lines + RFC | D4 | Out of tree first |
| **4** | **Library entry point** (new): `resonate_base::run_until(registry, options, shutdown)` using `try_init` for tracing, a public `load(&Options)`, and a public router constructor on `resonate-gateway-http` (today `build_app` is private) so an embedder can serve per-tenant routes behind its own dispatcher | ~60 lines | D2 dispatcher (D1 needs none: `build`/`start`/`stop` are public) | Not started |
| **5** | **Settled-promise retention** (new): a server setting that prunes settled promises older than N days, with the replay caveat documented | RFC + per-engine delete | Q40; D1 prunes Loams's own operations by itself | Not started |
| SDK-rs | `reqwest` 0.13 with `default-features = false` in the Rust SDK, so Loams's in-process runtime does not pull OpenSSL back | 1 line | D1 Task 6 | Not started; carried in the fork |

Order: 0c and 0a first (small, bug and security fixes, and how Loams introduces itself), then 1, then 4 and 2 (issues first), then 3a. Per Resonate's CONTRIBUTING, open an issue first for anything non-trivial, one concern per PR.

## 13. Spike results (2026-09-27)

**Setup.** A throwaway crate at `scratchpad/durable-spike` (not committed). It has an axum 0.8 app on `127.0.0.1:18090` and the Resonate server crates as path dependencies on a fresh clone (`resonate-embed`, `28dfd01`), built with `resonate_base::build` + `Running::start`, the SQLite plugin, the poll and push transports and the HTTP gateway on `127.0.0.1:8001`, all in one `#[tokio::main]` runtime. Configuration came from `Loader::new().set(…)`, and the tracing subscriber was the application's. A `/durable/ping` route on the axum 0.8 app called `server.process()` in-process. Target directory `~/.cache/cargo-target/durable-spike`.

| Check | Result |
|---|---|
| Links in one binary with axum 0.8 | **Yes.** Two axum versions coexist without a type crossing the boundary |
| Library entry point exists | **Yes.** `resonate_base::build`, `Running::start`, `Running::stop`, `Running::server` and `resonate_plugin::Loader` are public. `run`/`main` are not usable embedded (global subscriber, own signal wait). No upstream change needed for D1 |
| Both listeners serve | `127.0.0.1:18090` (Loams) and `127.0.0.1:8001` (Resonate), one process |
| In-process protocol call | `promise.create` from an axum 0.8 handler → 200 `pending` |
| Python fan-out/fan-in example (SDK 0.8.1), normal | PASS: 4/4 channels, 407 ms |
| Same, `--crash` | PASS: only the push branch re-ran (attempt 2); email, SMS and Slack not re-sent |
| Python human-in-the-loop example, **`kill -9` of the whole binary** while the workflow was suspended, then restart and resolve | PASS: the root promise went to `resolved` with "workflow loams-hitl-1 completed", from SQLite state |
| MySQL plugin (TiDB backend) links | Yes (+5.3 MB unstripped, +75 s). Not run against TiDB in the spike: embedding does not change the storage layer, and the research ran the same plugin through the full bar on TiDB v8.5.8 |
| `cargo deny` with Loams's `deny.toml` | Upstream: licenses ok, sources ok; bans fail (path dependencies without versions); advisories fail (8). With PR 0c: bans ok, one advisory (`rsa`, no fix) |
| Hygiene commit (PR 0c) | Builds with **no source change**; fan-out `--crash` passes again on the patched binary |
| Duplicates, size, time | §11.3, §11.4 |

**Conclusion.** Embedding works today with public API. The fork needs PR 0c for Loams's `cargo deny` policy and to keep OpenSSL and the old sqlx out of Loams's graph, plus PR 0a/1 for TiDB robustness. Multi-tenancy needs PR 4 later.

## 14. Roadmap: track D (D145)

Track D runs beside M1, M2 and R like track R, interleaved on the one-build machine (D127). It adds new crates and routes and changes no M1 code before M1 exits.

| Milestone | Scope | Depends on | Exit gate |
|---|---|---|---|
| **D1** | Embedded server behind `durable` (opt-in, D262); SQLite and native TiKV backends (TiKV added by D261; not on `main` yet); the legacy TiDB/`mysql://` backend (on `main`, deprecated by D261); the loopback listener; the fork with PR 0c/0a/1; the in-process network and Loams durable runtime; the operations API; bulk import from object storage; scheduled import; the conformance run and SDK example suite | M1.2 (collection write path, Flight mapping), R1 Task 1 (the playground; PD and TiKV only after D260), R1's `loams-tikv` | Porcupine LINEARIZABLE against `loams dev` (SQLite) and against the TiKV backend (TiDB before D261); TiKV backend passes blob differential + porcupine; SDK example suite green; import crash tests exact; D1 exit report |
| **D2** | Multi-tenant instances and the dispatcher (after the unified auth plan); tenant prefixes and keyspaces on the TiKV backend (Q41); approval gates (e); tenant provisioning and deprovisioning sagas (c) with R2; push with an outbound allowlist; retention (Q40); operations in the console | D1, the unified auth plan (D111), R2 | Isolation tests, including on TiKV; saga fault tests |
| **M2 uses D1** | GDPR erasure orchestration and its deadline sweep (c, a); restore and online backfill as operations (d) | D1 | Per the M2 plan |
| **D3** | Durable Live actions (f) with R3; the agent runtime on durable functions; MCP operation tools; agent traces via OTLP (Q43); re-embedding operations (a) once `embed()` exists | D2, R3 | Multi-agent and deep-research examples pass against Loams with crash injection |
| **D4** | The connect-rust `loams://` transport (after PR 3a); Loams-hosted workers; §14 Phase B (change stream, search tables, execution graph) | D3 | Transport and graph gates |

## 15. Contradictions with earlier decisions, and how they are resolved

| Earlier | Conflict | Resolution |
|---|---|---|
| D19 (§14): gateway role, blob over `loams-store`, Phase A in M3 | Owner's direction: embedded, SQLite/TiDB/TiKV, now | D138, D139 and D145 amend D19. §14's protocol, consistency and non-goal sections still hold. Self-hosted clusters use TiKV (D261 resolves Q39) |
| D1: object storage is the only source of truth | Durable state on SQLite, TiDB or TiKV is not on object storage | Like Loams Live (D130), durable execution is a Loams cloud service whose store is TiKV (D261). In dev and standalone the SQLite file sits in the data directory, like the metastore's redb Raft log. Backups of the durable stores follow D131's BR log backup in cloud. Self-hosted clusters run TiKV too (D261) |
| D46: Resonate Phase A in M3 | D1 now | D145: track D. M3 no longer carries Resonate Phase A |
| D111: M1 gateways bind loopback and warn on other addresses | Durable refuses other addresses | Stricter, like Live, because unauthenticated durable writes and push delivery are more dangerous than unauthenticated reads (§3.2) |
| §10 §2: `resonate = { listen = "0.0.0.0:8001" }` | Loopback default | §10's example is updated to `127.0.0.1:8001` |
| Owner: "implement all the useful resonate patterns" | Some patterns wait for later milestones | All seven have a feature and a milestone (§6). The v1 set is (d), (b), (a) import schedule, (g), and M2's erasure saga and sweep. (e) and provisioning (c) need auth and the cloud control plane. (f) needs Live actions. Compaction and GC are deliberately not moved (§6.1) |
| D138: `durable` on by default | `crates/loams/Cargo.toml` on `main` makes it opt-in (owner ruling O1) | D262 amends D138: opt-in cargo feature; the server starts by default at runtime only in binaries built with it |
| D139: TiDB for Loams cloud now | D-SC-16 and the owner's 2026-09-29 direction: TiKV only | D261 supersedes the TiDB clause; `mysql://` is legacy |
| Owner: "link resonate plugins" as a sidecar-free embed with the transport "http-poll/push" | Push is off by default | Push is linked and one flag away. It stays off by default only because an unauthenticated push transport lets any caller make Loams send requests to arbitrary addresses (§3.4). The auth plan and an allowlist turn it on in cloud |

## 16. Open questions

| # | Question | Needed by |
|---|---|---|
| Q39 | ~~The durable backend for self-hosted clusters without TiDB: Resonate's Postgres plugin (enterprises run Postgres; D58 adds Postgres to Loams in M2) or the blob server over `loams-store` (keeps D1 and needs no new service, at S3 PUT cost, §14 §6)~~ **Resolved 2026-09-29 by D261:** self-hosted clusters use the native TiKV backend | Resolved |
| Q40 | Retention of settled promises: an upstream protocol or server setting (PR 5), or Loams-side deletes per backend. What horizon is safe given that a late retry re-creates a pruned id and re-runs the work? | D2 plan |
| Q41 | TiKV durable layout: one `loams_durable` keyspace with tenant prefixes, or keyspaces per size class. It depends on Q36's keyspace limits | D2 plan |
| Q42 | The largest origin the blob/TiKV backend should hold. An origin is one document, and TiKV prefers values under 1 MiB (raft entry limit ~8 MiB). What split rule should large fan-outs use (child operations with their own origins)? | D2 plan |
| Q43 | OTLP traces ingest (D73 is logs only) for agent traces and step spans: a traces receiver into a stream, and a link to an execution graph | D3 plan |
| Q44 | Durable Live actions: a Resonate context bound into QuickJS over the Rust SDK, or actions run by the TypeScript SDK in a Node worker (ties to Q35) | D3 plan |

## 17. Sources

- Resonate `28dfd01`: `core/Cargo.toml`, `core/src/main.rs` (the registry), `core/crates/resonate-base/src/lib.rs` (`build`, `Running`, `run`), `core/crates/resonate-plugin/src/{config.rs,plugin.rs,registry.rs,lib.rs}`, `core/crates/resonate-gateway-http/src/{lib.rs,routes.rs}`, `core/crates/resonate-transport-http-poll/src/lib.rs`, `core/crates/resonate-transport-http-push/{Cargo.toml,src/lib.rs}`, `core/crates/resonate-server-sqlite/{Cargo.toml,src/lib.rs}`, `core/crates/resonate-sql/Cargo.toml`, `core/crates/resonate-timer-wheel/Cargo.toml`, `core/crates/resonate-core/src/{server.rs,types.rs}`; `sdk-rs/resonate/{Cargo.toml,src/network.rs,src/resonate.rs}`.
- Research of 2026-09-27: `resonate-tidb.md` and `resonate-upstream-plan.md` (m1.2a worktree, `.superpowers/research/`, not committed): examples, verification bar, TiDB results, TiKV assessment, transport assessment.
- Examples: `resonatehq-examples/example-fan-out-fan-in-py` and `example-human-in-the-loop-py`, run against the spike.
- aisix (Apache-2.0): README, OTLP/GenAI span export.
- Loams: §01 §3 (ports), §09 §3 (leases), §10 §2 and §5 (listeners, admin), §14, §18 §5–§9, §19 §5, §20 §5.1, §6.1, §9; D19, D46, D68, D69, D73, D76, D86, D90, D95–D97, D111, D115, D118, D122–D125, D127, D130, D131.
