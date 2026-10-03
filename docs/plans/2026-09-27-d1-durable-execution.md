# D1 — Embedded Durable Execution, the Operations API and Bulk Import Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, formats, constants), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: Planned** (2026-09-27). Track D, beside M1, M2 and R (D145). Branches `d1-t<N>`, stacked; PRs target `main`. D1 tasks interleave with M1 and R1 tasks on the one-build machine: never start a D1 build while another build runs, and never run the TiDB playground during a build. D1 adds crates and routes only; it changes no M1 code paths except to register the new routes and listener (D143: no M1 rewrite before M1 exits).

**Goal:** Ship the first slice of design §21 (D138–D147):
- `loams-durable`: the Resonate server embedded in the `loams` binary behind the cargo feature `durable`, on `127.0.0.1:8001`, refusing non-loopback addresses (D138);
- the SQLite backend for `loams dev`/`standalone` and the TiDB backend through Resonate's MySQL plugin (D139), from the pinned fork `ostrium-labs/resonate` (D140);
- Loams's durable runtime: the Resonate Rust SDK over an in-process network (D141);
- the operations API, `/v1/operations/{id}`, with idempotency keys (D146);
- **bulk import from object storage** into a collection, as a durable operation with per-file fan-out (D145);
- **scheduled incremental import** as a Resonate schedule (D145);
- the gates: porcupine linearizability against `loams dev` (SQLite, per PR) and against TiDB (nightly), the SDK example suite, the import crash tests, and the D1 exit report (D144).

**Architecture:**
- **The embed.** `loams-durable` names its plugins in a `resonate_plugin::Registry`: `resonate-server-sqlite`, `resonate-server-mysql` (feature `durable-mysql`), `resonate-transport-http-poll`, `resonate-transport-http-push` (linked, disabled unless `--durable-push`), Loams's `worker_inproc`, and `resonate-gateway-http`. It builds a `Configuration` from Loams's flags with `Loader::new().set(…)`, calls `resonate_base::build`, and starts and stops the result with `Running::start`/`stop`. It never calls `resonate_base::run`.
- **Loams's workflows** are Rust functions registered with the Resonate Rust SDK (package `resonate-sdk` 0.6, lib `resonate_sdk`, from the fork; T0-10). The SDK talks to the embedded server through `InProcNetwork`, which calls `ResonateServer::process` directly. It receives tasks through `worker_inproc`, a `WorkerPlugin` for the scheme `inproc` that hands each routed message to the SDK's `recv` callback.
- **The operations API** lives on the native API (`:8080`). It creates root promises with an in-process `promise.create`, starts workflows through the SDK, and reads state and progress with `promise.get` and `promise.search`.
- **Import** reuses M1.2's `CollectionBatchMapper` and `CollectionService::write`. It reads Parquet with `parquet` 58 and NDJSON with `arrow-json` 58 through `loams-store`.

**Tech Stack:**
- Rust 1.97.1, edition 2024, workspace lints (`unsafe_code = forbid`).
- New dependencies (Task 0 verifies versions, licenses and that they build together in the Loams workspace):
  - From the fork `https://github.com/ostrium-labs/resonate`, branch `loam/0.10.1`, pinned by `rev` `e3606698e6e3f2502bb018bba1e618deb63f907a` (Task 1, T1-1): `resonate-base`, `resonate-plugin`, `resonate-core`, `resonate-server-sqlite`, `resonate-server-mysql`, `resonate-transport-http-poll`, `resonate-transport-http-push`, `resonate-gateway-http` (all Apache-2.0, 0.10.1), and the Rust SDK `resonate-sdk` 0.6.0 (lib `resonate_sdk`; the package named `resonate` in that repository is the server binary; Apache-2.0). They bring `axum` 0.7 beside the workspace's 0.8 (isolated, §21 §11.3), `sqlx` 0.8.6, `rusqlite` 0.32 (bundled SQLite), `prometheus` 0.14 and the Verus crates `resonate-timer-wheel` pins (MIT).
  - `parquet` 58 (Apache-2.0), matching the workspace's arrow 58, features `arrow`, `async`, `object_store`.
  - Workspace crates reused: `arrow-json` 58, `object_store` 0.14 via `loams-store`, `axum` 0.8, `tokio`, `serde_json`, `sha2`, `ulid` (or the workspace's id helper), `tracing`, `thiserror`, `proptest`.
- **Cargo features on `loams` (owner ruling O1):**
  - `durable` (the embedded server on SQLite, the runtime, the operations API and the import routes) and `durable-mysql` (adds the TiDB backend and implies `durable`).
  - **Both are opt-in and off by default.** Turning `durable` on or off rebuilds about 479 crates (T0-16), which a developer build should not pay for every switch.
  - They are turned on explicitly in release builds (`cargo build --release -p loams --features durable-mysql`), in CI's `durable`, `durable-tidb` and `durable-examples` jobs, and in the Loams cloud build (`dina-kar/loam-cloud`).
  - Without the feature there is no durable listener, operations API or import route, and any `--durable-*` flag logs `this build has no durable execution (the durable feature is off)`.
  - Local `-p loams` builds and tests of D1 code pass `--features durable` (or `durable-mysql`); `-p loams-durable` needs no feature.
- Go 1.24 (for upstream's `conccheck`), Python 3.13 with `uv`, Node ≥ 22 with `tsx` (the example suite; O4). CI installs them in the `durable` jobs only.
- TiDB: R1's playground (`scripts/tikv/playground.sh start --with-tidb`, TiDB on `127.0.0.1:21000`, `tiup playground v8.5.8`). If R1 Task 1 has not merged, Task 4 uses `tiup playground v8.5.8 --tag loams-durable --port-offset 17000 --db 1 --kv 1 --pd 1 --tiflash 0 --without-monitor` directly.

**Spec:**
- [`docs/design/21-durable-execution.md`](../design/21-durable-execution.md): all of it; §3 (embed, listener, backends, transports, runtime), §6.2, §6.4, §6.7 (fan-out, operations API, idempotency), §7 (import and scheduled import), §8 (observability, retention), §9 (failures), §10 (testing), §11 (dependencies), §13 (spike).
- [`docs/design/14-durable-execution.md`](../design/14-durable-execution.md): §1 (protocol vocabulary), §5 (consistency model).
- [`docs/design/13-decision-log.md`](../design/13-decision-log.md): D76, D86, D90, D111, D138–D147; Q39–Q44.
- As built: [M1.2](2026-09-24-m1.2-query-engine.md) Task 13 (Flight `DoPut` mapping), `CollectionService::write` (M1.2), the `loams` server config and native router (`crates/loams/src/{server.rs,main.rs,api/mod.rs}`), [R1](2026-09-27-r1-reactive-core.md) Task 1 (the TiDB playground).

## Global Constraints

Same as the M1 overview §8, plus:
- **No protocol changes.** Loams never changes Resonate's wire format, status codes or semantics. Everything Loams-specific is a plugin, a flag or a route outside the protocol.
- **Fork discipline.** The fork branch `loam/0.10.1` holds upstream `28dfd01` plus only these commits: the dependency hygiene of upstream PR 0c, the `gcp-idtoken` feature (PR 0d), the TiDB fixes of PR 0a and PR 1, and the Rust SDK's `reqwest` defaults as a feature (PR 0e) (T1-1). Each is also opened upstream, one concern per PR. Nothing else is patched in the fork during D1.
- **Never read or copy** `resonate-server-scylladb` or the NATS packages (BUSL-1.1 lineage, §21 §11.1).
- **Loopback only (D138).** `--durable-listen` accepts only 127.0.0.0/8, `::1` and `localhost`. Any other address fails startup with `durable listener must be loopback until authentication is configured (D111); got <addr>`. A loopback bind logs one line saying the durable API is unauthenticated. Push delivery is off unless `--durable-push` is given, and `--durable-push` logs a warning naming the server-side request forgery risk.
- **Process hygiene.** `loams-durable` installs no tracing subscriber, no signal handler and no panic hook. The one process-wide setting it may touch is rustls's default crypto provider, and only when none is set and the store is MySQL over TLS (amended, T5-4). `gateways.gateway_http.abort_on_panic` is always `false`, and `--durable-set` refuses to change it.
- **Cluster tests skip without a cluster.** TiDB tests read `LOAMS_TEST_TIDB` (a MySQL URL for an admin user). Without it they print `skipped: <test> needs LOAMS_TEST_TIDB`. CI's `durable-tidb` job sets it.
- **The build machine.** One cargo build at a time, the shared target directory, `-j 6` (Task 0's measurements use `CARGO_BUILD_JOBS=4 CARGO_INCREMENTAL=0` for comparability with the spike), lld; the TiDB playground is stopped before a build.
- **Commit areas:** `durable`, `api`, `import`, `ci`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **`durable` is an opt-in feature of `loams`, off by default**, and `durable-mysql` (the TiDB backend) is a separate opt-in feature that implies it. Release builds, CI's durable jobs and the Loams cloud build turn them on (amended by O1; the plan first made both default) | Toggling `durable` rebuilds about 479 crates (T0-16), which developer builds should not pay. It costs +13.0 MB stripped and +125 s (+8 %) on a cold release build (T0-15) | A `loams dev` from a plain `cargo build` has no durable execution. The feature-off log line says so, and the README's dev setup names the feature |
| 2 | **The default namespace only.** D1 serves one tenant, the `default` namespace; the store is `<data>/durable/default.db` or database `loams_durable_default` | Multi-tenancy needs the unified auth plan (D142) | None in D1: the per-tenant layout is already the naming scheme |
| 3 | **Port 8001** for the durable listener, `--durable-listen` to change it, `--no-durable` to disable it | Resonate's SDK default and §01's reservation (D138) | A conflict with a locally running standalone Resonate: the startup error names both flags |
| 4 | **The engine and port differentials run in the fork's CI, not in Loams's** | Embedding changes no engine code (D144) | A fork CI outage would hide an engine regression at a revision bump; Task 1 requires a green fork CI run for the pinned revision |
| 5 | **Loams's workflows use the Rust SDK over `InProcNetwork`**; if Task 6 finds a blocker (for example, the SDK assuming SSE framing that `worker_inproc` cannot reproduce), the fallback is the SDK's `HttpNetwork` against `http://127.0.0.1:8001`, recorded as a ruling | D141 | The fallback needs the listener on; `--no-durable` would then also disable operations |
| 6 | **Import slices are one Parquet row group, or 64 MiB of NDJSON split at a newline**; a slice's rows are written in chunks of the existing `put_chunk_rows` | Bounded memory; the same chunking as Flight `DoPut` | Very large row groups need more memory; the importer refuses a row group over 512 MiB uncompressed with a message naming the file |
| 7 | **Rows without a primary key get ids derived from `(operation id, file index, row index)`** (UUIDv8 over SHA-256, truncated) | A re-run slice must not duplicate documents | None: the id is deterministic and documented |
| 8 | **Finished operations are listed for 7 days, then pruned** by a plain worker task (not a durable schedule) | §21 §6.1: a retention sweep needs no multi-step state | Users who poll later than 7 days get 404; documented |
| 9 | **`max_parallel_files` defaults to 4 and `max_concurrent_operations` per namespace to 2** | The write path's backpressure (D86) is per collection; a few parallel files saturate one collection | Slow imports on large clusters; both are request and config settings |

## Carried in

None from M1 or R1. From the research and the spike (§21 §13): upstream PR 0a/1 (prepared as the commit `cdc1cbd` in the research worktree), PR 0c (the patch `0001-deps-clear-RustSec-advisories-drop-OpenSSL-version-i.patch` in the spike directory), and the spike's measurements.

## Review Focus

1. **Exactly-once imports under crashes.** A crash at any point leaves no duplicate and no missing document, and finished files are not re-read. Tests: Task 8 (`import_survives_crash_at_every_step`, `rerun_slice_converges`, `finished_files_are_not_reread`), Task 10 (the crash gate).
2. **Idempotency.** The same `Idempotency-Key` returns the same operation, and different parameters are refused. Tests: Task 7 (`same_key_same_operation`, `same_key_other_params_conflicts`).
3. **The embed does not disturb the host.** No global subscriber, no signal handler, no abort, a clean start and stop order, and a clear error on a port conflict. Tests: Task 2 (`start_stop_leaves_no_listener`, `port_in_use_names_flags`, `handler_panic_answers_500`), Task 3.
4. **Loopback refusal and push off.** Tests: Task 2 (`non_loopback_is_refused`, `push_is_off_by_default`).
5. **Linearizability of the embedded server.** Tests: Task 5 (porcupine against `loams dev`, SQLite and TiDB).
6. **Schedule dedup.** New files are imported once, changed files again, and duplicate registrations answer 409. Tests: Task 9.

## File structure

```
Cargo.toml                                   # + fork git deps (rev-pinned), parquet; member loams-durable
deny.toml                                    # allow-git for ostrium-labs/resonate; ignore RUSTSEC-2023-0071 with rationale
NOTICE                                       # + Resonate attribution (Apache-2.0)
crates/loams-durable/                       # new
  Cargo.toml                                 # features: mysql
  src/{lib.rs,config.rs,embed.rs,registry.rs,listen.rs,inproc.rs,runtime.rs,ops.rs,ids.rs,error.rs}
  src/import/{mod.rs,plan.rs,parquet.rs,ndjson.rs,slice.rs,schedule.rs}
  tests/{embed.rs,tidb.rs,inproc.rs,ops.rs,import.rs,import_crash.rs,schedule.rs}
crates/loams/
  Cargo.toml                                 # features durable, durable-mysql (opt-in, O1)
  src/{server.rs,main.rs}                    # --durable-* flags, start/stop order
  src/api/{mod.rs,operations.rs,import.rs}   # operations and import routes
scripts/durable/{conformance.sh,examples.sh,porc-503.sh}
.github/workflows/ci.yml                     # + jobs durable (path-filtered), durable-tidb (nightly), durable-examples (nightly)
docs/plans/d1-dependency-spike.md            # Task 0
docs/plans/d1-exit-report.md                 # Task 10
docs/design/21-durable-execution.md  docs/design/13-decision-log.md  CHANGELOG.md  docs/plans/README.md
```

### Task 0: Reconcile and measure in the workspace

**Files:** read `crates/loams/src/{server.rs,main.rs,api/mod.rs}`, `crates/loams-query/src/{write.rs,flight_ingest.rs}`, `crates/loams-store/src/` as merged on `main`. Write `docs/plans/d1-dependency-spike.md` and fill this plan's "Rulings made during execution".

**Consumes** (each checked against the merged code; every difference is listed with its resolution):

```rust
// loams-query (M1.2)
impl CollectionService { pub async fn write(&self, ns: &str, name: &str, ops: Vec<DocOp>, opts: WriteOptions) -> Result<WriteResult, ServiceError>; }
impl CollectionBatchMapper { pub fn new(arrow: &Schema, schema: &CollectionSchema, id_type: IdType) -> Result<Self, ServiceError>;
                             pub fn map(&self, batch: &RecordBatch) -> Result<Vec<DocOp>, RowError>; }
// resonate (fork, 0.10.1)
pub fn resonate_base::build(registry: &Registry, config: &Configuration, options: &Options) -> Result<Running, String>;
impl Running { pub async fn start(&self, debug: bool) -> Result<(), String>; pub async fn stop(&self, timeout: Duration); pub fn server(&self) -> &Arc<dyn ResonateServer>; }
impl resonate_plugin::Loader { pub fn new() -> Self; pub fn set(self, key: &str, value: &str) -> Result<Self, ConfigError>; pub fn load(self) -> Configuration; }
// resonate SDK 0.6
pub trait Network: Send + Sync { fn pid(&self) -> &str; fn group(&self) -> &str; fn unicast(&self) -> &str; fn anycast(&self) -> &str;
  async fn start(&self) -> Result<()>; async fn stop(&self) -> Result<()>; async fn send(&self, req: String) -> Result<String>;
  fn recv(&self, callback: Box<dyn Fn(String) + Send + Sync>); fn target_resolver(&self, target: &str) -> String; }
pub struct ResonateConfig { pub network: Option<Arc<dyn Network>>, /* … */ }
```

**Checks** (record each result, with the command, in the spike doc):
1. Add the fork dependencies to a scratch branch of the workspace (never pushed) with `loams` depending on them. Record: the build time of `cargo build --release -p loams` with and without `--no-default-features --features <all but durable>`, at `CARGO_BUILD_JOBS=4 CARGO_INCREMENTAL=0`; the binary size, stripped and unstripped; the semver-incompatible duplicates (`cargo tree -d`); `cargo deny check` with the new `deny.toml`. The spike's stand-alone numbers are in §21 §11.4.
2. Confirm that one lockfile unifies: sqlx is 0.8.6, no second `libsqlite3-sys`, no `openssl-sys` in `cargo tree -e normal -i openssl-sys`, and no conflict with R1's `tikv-client` tree if R1 Task 0 has merged.
3. Confirm the Rust SDK builds with `reqwest` `default-features = false` and that `ResonateConfig.network` accepts a custom `Network` (compile a stub).
4. Confirm port 8001 is free in every documented configuration (`docs/`, `crates/loams/src/main.rs` defaults).
5. Confirm `promise.search` with a tag filter works on SQLite and TiDB (the progress rule, §21 §6.4), with an in-process call against the embedded server.
6. Record which Python and TypeScript SDK versions pass against server 0.10.1. Expected: the monorepo's Python 0.8.1 and the TypeScript SDK at the pinned revision; PyPI 0.7.x fails (§21 §4).

**Produces:** the spike doc and a filled "Rulings made during execution" table (no empty rows).

**Commit:** `docs: record the D1 dependency measurements`.

### Task 1: The fork and the pinned dependencies

**Files:** the fork (`ostrium-labs/resonate`, branch `loam/0.10.1`), `Cargo.toml`, `deny.toml`, `NOTICE`.

**Semantics:**
1. Create `loam/0.10.1` from upstream `28dfd01` with these commits, in order (amended, T1-1: one commit per upstream PR):
   - `deps: clear RustSec advisories, drop OpenSSL, version internal path deps`: PR 0c (#1164) as is, `c3f25b9`;
   - `transport-http-push: gate the GCP ID token behind a gcp-idtoken feature`: a `gcp-idtoken` feature on `resonate-transport-http-push` that gates `google-cloud-auth`, on by default upstream and off in Loams (T1-2);
   - `server-mysql: classify retryable errors by MySQL error number`: PR 0a (#1162);
   - `server-mysql: run on TiDB; xtask and CI legs for it`: PR 1 (#1163);
   - `sdk-rs: reqwest without default TLS`, as a default-on SDK feature `reqwest-default` that Loams turns off (T1-3).
2. The fork's CI (`server-core-ci.yml`) must be green at the pinned revision for `check`, the engine and port differentials (SQLite, MySQL, TiDB leg from PR 1) and porcupine (SQLite, TiDB). Record the run URL in the spike doc. (Not run yet: Actions is not enabled on the fork, T1-4.)
3. Open the upstream issues and PRs for 0c and 0a (one concern each) and link them in the spike doc. Merging is not required for D1. (0c, 0a and 1 are open; 0d and 0e have fork branches and PR bodies ready for the owner, T1-6.)
4. `Cargo.toml` `[workspace.dependencies]`: each crate as `{ git = "https://github.com/ostrium-labs/resonate", rev = "<sha>" }`, with `default-features = false` on `resonate-transport-http-push` and `resonate-sdk` (T1-5); `parquet = { version = "58", default-features = false, features = ["arrow", "async", "object_store", "snap", "zstd", "lz4"] }`.
5. `deny.toml`: `[sources] allow-git = ["https://github.com/ostrium-labs/resonate"]`; `[advisories] ignore = [{ id = "RUSTSEC-2023-0071", reason = "rsa via sqlx-mysql: used only for RSA password exchange on non-TLS MySQL connections; Loams connects to TiDB over TLS or the cluster network with mysql_native_password (§21 §11.2)" }]`.

**Tests:** `cargo deny check` passes; `cargo tree -e normal -i openssl-sys` prints nothing. Until Task 2 adds a crate that uses them, the workspace dependencies are not in `Cargo.lock`, so Task 1 also runs both checks with a throwaway crate that depends on all of them (T1-5).

**Commit:** `durable: pin the Resonate fork and extend the license policy`.

### Task 2: `loams-durable`: the embedded server

**Files:** `crates/loams-durable/{Cargo.toml,src/{lib.rs,config.rs,embed.rs,registry.rs,listen.rs,error.rs}}`, `crates/loams-durable/tests/embed.rs`.

**Produces:**

```rust
pub struct DurableConfig {
    pub listen: SocketAddr,                 // 127.0.0.1:8001
    pub store: DurableStore,                // Sqlite { path } | Mysql { url, tls: MysqlTls }
    pub push: bool,                         // false
    pub debug: bool,                        // false; the hidden --durable-debug (caller-owned clock)
    pub retry_timeout: Duration,            // 30 s (Resonate's default)
    pub shutdown_timeout: Duration,         // 10 s
    pub overrides: Vec<(String, String)>,   // --durable-set key=value (Resonate key space)
}
pub enum DurableStore { Sqlite { path: PathBuf }, Mysql { url: String, tls: MysqlTls } }
pub struct DurableServer;                   // holds the Running and the node id
impl DurableServer {
    pub async fn start(config: DurableConfig, node_id: &str) -> Result<Self, DurableError>;
    pub fn server(&self) -> Arc<dyn ResonateServer>;
    pub async fn process(&self, req: serde_json::Value) -> Result<serde_json::Value, DurableError>;  // in-process protocol call
    pub async fn ready(&self) -> bool;
    pub async fn stop(self);
}
pub enum DurableError { NotLoopback { addr: SocketAddr }, Bind { addr: SocketAddr, source: String }, Config(String),
                        Start(String), Unavailable(String), Protocol { status: u16, body: serde_json::Value } }
```

**Semantics:**
1. `registry()` names `resonate_server_sqlite::PLUGIN`, `resonate_server_mysql::PLUGIN` (feature `mysql`), `resonate_transport_http_poll::PLUGIN`, `resonate_transport_http_push::PLUGIN`, `crate::inproc::PLUGIN` (Task 6; a no-op placeholder until then) and `resonate_gateway_http::PLUGIN`. `registry.check()` runs first.
2. The configuration comes from `Loader::new()` only (no file, no environment):
   - `gateways.gateway_http.bind = "<listen>"`, `gateways.gateway_http.abort_on_panic = false`;
   - `servers.active = "server_sqlite" | "server_mysql"`;
   - for SQLite: `servers.server_sqlite.path`, `migrate = true`, `server_url = "http://<listen>"`, `retry_timeout = <ms>`;
   - for MySQL: `servers.server_mysql.url`, `server_url`, `retry_timeout`;
   - `workers.transport_http_push.enabled = <push>` (`debug` is not a configuration key: it is the argument of `Running::start(debug)`, T0-9);
   - then each override, in order. An override of `abort_on_panic`, `bind`, `servers.active`, `gateways.gateway_http.auth` or `gateways.gateway_http.workos` is refused (`Config`), and so are `servers.server_sqlite.path`, `servers.server_mysql.url` (`--durable-store` owns them) and `workers.transport_http_push.enabled` (`--durable-push`), any key above or below one of these, a quoted key, a key outside `servers.*`, `workers.*` and `gateways.*`, and a plugin id the registry does not carry (amended, T2-4).
   - `build` gets `Options::default().default_server("server_sqlite")`; it reads no file or environment (T0-9).
3. Before `build`, `listen` is checked for loopback (`NotLoopback`) and bound once with a `std::net::TcpListener` probe that is dropped at once, so a port conflict becomes `Bind` naming `--durable-listen` and `--no-durable`. Resonate's gateway then binds the port itself; the race window is accepted and logged.
4. `start` = `build` + `Running::start(debug)`. `stop` = `Running::stop(shutdown_timeout)`, then wait until the port is free (at most 2 s).
5. `process` wraps the JSON in a `RequestEnvelope` with `head.version = "2026-04-01"` and a generated `corrId`. A non-2xx status becomes `Protocol`.
6. SQLite: the parent directory is created, and an exclusive lock file `durable.lock` in it makes a second process fail with `the durable store <path> is in use by another process`.

**Tests** (`tests/embed.rs`, all on SQLite in a temp dir, ports picked from 127.0.0.1:0 probes):
- `start_stop_leaves_no_listener`;
- `port_in_use_names_flags`;
- `non_loopback_is_refused` (0.0.0.0 and a LAN address);
- `push_is_off_by_default` (a promise with `resonate:target = http://127.0.0.1:<port>/hook` never produces a request on a local listener within 2 s; with `push = true` it does);
- `in_process_create_is_idempotent` (the same `promise.create` twice → the same promise);
- `state_survives_restart` (create, stop, start, get);
- `second_process_on_same_store_is_refused`;
- `handler_panic_answers_500` (a test-only route injected through `Routes` panics → 500, the process lives);
- `no_global_subscriber_installed` (a test installs its own subscriber after `start`, which must succeed);
- `overrides_cannot_touch_bind_or_abort`.

**Commit:** `durable: embed the Resonate server with the SQLite backend`.

### Task 3: The `loams` binary: flags, feature, lifecycle

**Files:** `crates/loams/{Cargo.toml,src/server.rs,src/main.rs}`, tests in `crates/loams/src/main.rs` (flag parsing) and `crates/loams/tests/durable.rs`.

**Semantics:**
1. Features: `durable = ["dep:loams-durable"]`, `durable-mysql = ["durable", "loams-durable/mysql"]`, neither in `default` (amended, O1). The binary tests in `crates/loams/tests/durable.rs` are `#![cfg(feature = "durable")]` and run with `cargo test -p loams --features durable`.
2. Flags on `dev`, `standalone` and `cluster`: `--durable-listen <addr>` (default `127.0.0.1:8001`), `--no-durable`, `--durable-store <sqlite:<path>|mysql://…>` (default on `dev` and `standalone`: `sqlite:<data_dir>/durable/default.db`; `cluster` has no default, so a cluster node without `--durable-store` serves no durable API, and it refuses `sqlite:` with `the sqlite durable store is single-node; use --durable-store mysql://…` (amended, T3-2)), `--durable-push`, `--durable-set key=value` (repeatable), and hidden `--durable-debug`. Without the feature, any `--durable-*` flag logs `this build has no durable execution (the durable feature is off)`, like the Qdrant flags.
3. `ServerConfig.durable: Option<DurableConfig>`.
4. Start order: metastore (`wait_for_leader`) → `DurableServer::start` → `assemble` (the collection service, worker and router) → `DurableRuntime::start` → the native API and the other listeners. Stop order: Qdrant, the native API and Flight → `DurableRuntime::stop` → `DurableServer::stop` → `collections.shutdown`, the writer flush, workers, the hot tier → metastore. In cluster mode durable stops after `late.close()` (T0-6). A durable start failure is fatal, with its message.
5. `loams dev` prints `loams durable listening on http://<addr>` before the HTTP line, like the Qdrant lines (T0-7).

**Tests:** flag parsing (defaults, `--no-durable`, `cluster` refuses `sqlite:`, overrides); `dev_serves_durable_on_8001_style_port` (an ephemeral port through `--durable-listen`, `GET /ready` → 200); `stop_order_drains_durable_before_meta`.

**Commit:** `durable: serve the durable listener from loams dev, standalone and cluster`.

### Task 4: The TiDB backend

**Files:** `crates/loams-durable/src/config.rs` (MySQL URL and TLS), `crates/loams-durable/tests/tidb.rs`, `crates/loams/src/main.rs` (a `durable migrate` subcommand), `scripts/durable/tidb.sh`.

**Semantics:**
1. `mysql://user:pass@host:port/db?ssl-mode=required|disabled|verify_ca|verify_identity[&ssl-ca=<path>]` maps to `servers.server_mysql.url` (the verifying modes and `ssl-ca` amended by O8, T5-3). `MysqlTls::Required` is the default except for `localhost` and loopback addresses (amended, T4-2). Any other `ssl-mode` is refused. A store's `Debug` and `Display`, and any error that could quote the URL, hide the password (T4-1).
2. `loams durable migrate --durable-store mysql://…` runs Resonate's migrations once (it starts the server plugin with `migrate = true` and stops). `loams standalone|cluster` with a TiDB store never migrates. An empty database fails with `run 'loams durable migrate' first`, and a schema behind the binary fails with Resonate's message plus the command (amended, T4-3).
3. `scripts/durable/tidb.sh up|down` starts a plain TiDB with the fallback command in Tech Stack (`--tag`, `--port-offset`; not R1's keyspace-mode playground, T4-6), creates the database `loams_durable_default` and prints `LOAMS_TEST_TIDB`.
4. The fork's pessimistic pin is verified in the tests, through TiDB's statement summary: every server connection ran the pin (amended, T4-5).

**Tests** (`tests/tidb.rs`, skip without `LOAMS_TEST_TIDB`): `migrate_then_serve`; `unmigrated_schema_names_the_command`; `state_survives_restart_on_tidb`; `two_servers_one_database_are_linearizable_smoke` (two `DurableServer`s on one database, concurrent `promise.create` and `settle` on 4 ids, and every final state is one the protocol allows); `optimistic_cluster_still_pessimistic_sessions` (`SET GLOBAL tidb_txn_mode='optimistic'` on a throwaway playground, then a session still reports `pessimistic`).

**Commit:** `durable: add the TiDB backend through the MySQL plugin`.

### Task 5: The conformance run

**Files:** `scripts/durable/{conformance.sh,porc-503.sh}`, `.github/workflows/ci.yml`.

**Semantics:**
1. `conformance.sh --store sqlite|tidb [--clients N --ops M --seed S] [--tidb-url URL] [--loams-bin PATH] [--fork DIR] [--out DIR]` (amended, T5-5):
   - builds `loams` (release, `--features durable-mysql`, O1) unless `--loams-bin` names one, and, from the pinned fork (the rev in `Cargo.toml`; `--fork` or `$DURABLE_FORK_DIR` must be at it, else it is fetched into `target/durable-fork/resonate`), `conctrace` (`cargo build --release --locked --example conctrace` in `impl/server/core`, target dir `$DURABLE_FORK_TARGET_DIR`, default `~/.cache/cargo-target/durable-fork`);
   - starts `loams dev --durable-debug --durable-listen 127.0.0.1:<free> [--durable-store …]` on a fresh store: a new data directory, and for `tidb` a new database `loams_conf_<pid>_<time>` on the TiDB at `--tidb-url` (default `$LOAMS_TEST_TIDB`), migrated with `loams durable migrate` first and dropped at the end;
   - runs `conctrace --url http://127.0.0.1:<port>/ --out <dir>/trace --clients N --ops M --seed S`;
   - runs `go run ./cmd/conccheck -partition=false < trace.history` in the fork's `spec/valid/porc`;
   - exits non-zero unless both read disciplines say LINEARIZABLE (a TIMEOUT or INCONCLUSIVE verdict fails too: `conccheck` exits 0 on them), and prints the status tally (2xx, 3xx, 4xx, 5xx, and each code). The result line and the tally go to `$GITHUB_STEP_SUMMARY` when it is set.
2. `porc-503.sh <in> <out>` rewrites a history's 503 responses as not applied (the research rule for F3) by dropping those rows, and reports how many there were (T5-6). The TiDB leg runs the checker on the rewritten history until upstream PR 0b lands, and the job summary shows the count.
3. CI (amended, T5-8): job `durable` (PRs touching `crates/loams-durable/**`, `crates/loams/src/api/{operations,import}.rs`, `crates/loams/tests/durable.rs`, `scripts/durable/**`, the workflow, or the Resonate rev in `Cargo.toml`; pushes to `main` always): `cargo test -p loams-durable`, `cargo test -p loams --features durable --test durable --bin loams`, `conformance.sh --self-test`, then `conformance.sh --store sqlite --clients 8 --ops 600`. Job `durable-tidb` (nightly): build first, then `scripts/durable/tidb.sh up --tag loams-ci-durable --port-offset 27000`, `cargo test -p loams-durable --features mysql --test tidb`, `conformance.sh --store tidb` at 8 × 600 and at 16 × 400 with seeds 11, 12 and 13, and `tidb.sh down` always.

**Tests:** the script itself, plus `conformance.sh --self-test`: it must fail on a doctored history (one settle answered twice with different values). It runs on a small history recorded from `loams dev` (`scripts/durable/testdata/embedded-sqlite.history`), which must linearize, and it checks that `porc-503.sh` drops exactly an injected 503 (amended, T5-7).

**Commit:** `ci: run Resonate's linearizability check against the embedded server`.

### Task 6: The in-process network and Loams's durable runtime

**Files:** `crates/loams-durable/src/{inproc.rs,runtime.rs}`, `crates/loams-durable/tests/inproc.rs`.

**Produces:**

```rust
pub static PLUGIN: WorkerPlugin;            // id "worker_inproc" (crate name outside resonate-* keeps its whole name: set explicitly), scheme "inproc"
pub struct InProcNetwork;                   // implements resonate::Network over DurableServer::process
pub struct DurableRuntime;                  // the SDK instance for group "loams"
impl DurableRuntime {
    pub async fn start(server: &DurableServer, node_id: &str) -> Result<Self, DurableError>;
    // Amended (T6-4): register before subscribing, and the lease.
    pub async fn start_with<F>(server: &DurableServer, node_id: &str, options: RuntimeOptions, register: F) -> Result<Self, DurableError>
        where F: FnOnce(&resonate_sdk::resonate::Resonate) -> resonate_sdk::error::Result<()>;
    pub fn sdk(&self) -> &resonate_sdk::resonate::Resonate;     // register Loams functions here
    pub async fn stop(self);
}
```

**Semantics:**
1. Addresses: `unicast = inproc://uni@loams/<node_id>`, `anycast = inproc://any@loams/<node_id>`, `group = loams`, `pid = <node_id>`.
2. `worker_inproc` keeps `group → [callbacks]`. `process(address, msg)` parses the address like `PollAddress` (unicast: that node; anycast: prefer the named node, else any local subscriber). It serializes `msg` to exactly the JSON the poll transport writes in an SSE `data:` frame and invokes the callback on a spawned task. With no subscriber it returns `Unavailable`, and the task retry timeout recovers.
3. `InProcNetwork::send(req)` parses the request, calls `server.process`, and returns the response JSON. Transport errors map to the SDK's retryable error.
4. `DurableRuntime::start` builds `resonate_sdk::resonate::Resonate::new(ResonateConfig { network: Some(Arc::new(InProcNetwork…)), group: Some("loams"), pid: Some(<node_id>), ttl: Some(60_000), .. })`. `new` returns `Self` and spawns `network.start()` itself, so `start` waits on a readiness flag that `InProcNetwork::start` sets, and `InProcNetwork::start` cannot fail (T0-10).
5. If the SDK cannot be driven this way, apply Ruling 5 and record it.

**Tests** (`tests/inproc.rs`):
- `two_step_function_runs` (step 1 then step 2, the result returned);
- `crash_between_steps_resumes_without_rerunning_step_1` (a counter per step; the runtime is stopped after step 1 settles and started again → step 1 ran once, step 2 once);
- `failed_branch_alone_retries` (four branches, one fails once → the other three ran once);
- `sleep_survives_restart` (`ctx.sleep(2 s)` across a runtime restart);
- `no_http_is_used` (the listener disabled through a test hook; the functions still run).

**Commit:** `durable: run Loams's own durable functions in process`.

### Task 7: The operations API

**Files:** `crates/loams-durable/src/{ops.rs,ids.rs}`, `crates/loams/src/api/operations.rs`, `crates/loams-durable/tests/ops.rs`.

**Produces:**

```rust
pub struct OperationId(String);             // "op-" + 26 chars: a ULID, or SHA-256(ns ‖ idempotency key) hex
pub enum OperationState { Queued, Running, Succeeded, Failed, Canceled }
pub struct Operation { pub id: OperationId, pub kind: String, pub namespace: String, pub target: serde_json::Value,
                       pub state: OperationState, pub progress: serde_json::Value, pub result: Option<serde_json::Value>,
                       pub error: Option<OperationError>, pub created_at: i64, pub updated_at: i64 }
pub struct Operations;                      // over DurableServer + DurableRuntime
impl Operations {
    pub async fn submit(&self, ns: &str, kind: &str, params: serde_json::Value, idempotency_key: Option<&str>)
        -> Result<(OperationId, bool /* created */), OpsError>;
    pub async fn get(&self, id: &OperationId) -> Result<Operation, OpsError>;
    pub async fn list(&self, ns: &str, state: Option<OperationState>, cursor: Option<String>) -> Result<(Vec<Operation>, Option<String>), OpsError>;
    pub async fn cancel(&self, id: &OperationId) -> Result<(), OpsError>;
}
```

**Semantics:**
1. **Routes** (`api/operations.rs`, on the native listener):
   - `GET /v1/operations/{id}` → 200 `Operation` or 404;
   - `GET /v1/namespaces/{ns}/operations?state=&cursor=` → 200 `{operations, next}`;
   - `POST /v1/operations/{id}/cancel` → 202, or 409 `operation_finished`.
2. **Submit.** The root promise is `promise.create { id, param: {kind, namespace, params, params_hash}, tags: {"loams:op": id, "loams:kind": kind, "loams:ns": ns, "resonate:target": "inproc://any@loams"}, timeoutAt: now + 7 d }`, and the workflow for `kind` starts on it through the SDK. With an `Idempotency-Key`, an existing root with the same `params_hash` returns `(id, false)`; a different hash is `OpsError::IdempotencyKeyReused` → 409 `idempotency_key_reused`.
3. **State** maps as in §21 §6.4. **Progress** comes from the kind's progress function (Task 8: tagged file branches), cached for 2 s.
4. **Cancel** settles the root `rejected_canceled`. Workflows check it between steps through a helper, `ops::check_canceled(ctx, id)`.
5. **Retention:** a worker task `durable-op-retention` (priority `Maintenance`) deletes finished operations older than 7 days (Ruling 8) through a backend-level delete of the operation's origin. SQLite and TiDB run `DELETE FROM promises WHERE origin_id = ?` (a generated column in both schemas; task state lives on `promises`, and `callbacks` and `listeners` cascade, T0-12). The engine exposes no such operation, so this is a direct SQL statement kept in `ops.rs` with a comment naming upstream PR 5.

**Tests** (`tests/ops.rs`): `submit_returns_202_location` (through the route); `same_key_same_operation`; `same_key_other_params_conflicts`; `state_mapping_covers_every_promise_state`; `cancel_stops_at_next_step`; `list_filters_by_state_and_pages`; `retention_prunes_finished_after_7_days` (debug clock; also on TiDB, skipped without `LOAMS_TEST_TIDB`, asserting the callbacks are gone); `unknown_id_is_404`.

**Commit:** `api: add the durable operations API`.

### Task 8: Bulk import from object storage

**Files:** `crates/loams-durable/src/import/{mod.rs,plan.rs,parquet.rs,ndjson.rs,slice.rs}`, `crates/loams/src/api/import.rs`, `crates/loams-durable/tests/{import.rs,import_crash.rs}`.

**Produces:** `POST /v1/namespaces/{ns}/collections/{c}/import` with the body of §21 §7.2, answering 202 with `Location` (and `200` for an idempotent repeat), and the workflow `collection.import`.

**Semantics:**
1. **Plan step** (`ctx.run("plan")`): list `source` with `loams-store` (`Store::from_url(source, [])`, credentials from the process environment as for `--bucket`; per-namespace credentials are M2, T0-5). Filter by `pattern` (glob on the key's suffix after the prefix). Sort by key. Record `[(key, size, etag)]` and the totals. With zero files the operation succeeds at once with `files_total = 0`. A source outside the allowed schemes (`s3`, `gs`, `az`, `file` only on `dev`) → 400 before submit.
2. **File branches**: one child per file, tagged `loams:op=<id>`, `loams:kind=file`, `loams:file=<index>`, with at most `max_parallel_files` in flight (a semaphore inside the workflow). A branch reads every range with `store.inner().get_opts(path, GetOptions { if_match: Some(etag), range, .. })`, and Parquet through an `AsyncFileReader` over it; a `PreconditionFailed` fails the branch with `file_changed` (T0-5).
3. **Slices**: Parquet → one row group per slice (`ParquetRecordBatchStreamBuilder` over the `object_store` reader, Ruling 6); NDJSON → 64 MiB ranges extended to the next `\n` (the first slice starts at 0, and each later slice skips its partial first line). Each slice is `ctx.run("slice-<n>")` → `{rows, bytes, token}`.
4. **Mapping and writing**: `mapping` (`{columns: {<source>: <collection column>}, id_type, id_column?}`, O3) renames the slice's columns. `id_column` optionally names the source column to use as `_id`: it is renamed to `_id` and typed by `id_type`. A file without that column fails its branch with `id_column_missing`, and an `id_column` together with a `columns` entry whose target is `_id` answers 400 before submit. Renames run first and must leave every column name unique (the mapper refuses a repeated name): two `columns` entries with one target answer 400 before submit, and a target that collides with a column of the file that is not renamed fails that file's branch with `mapping_conflict`. When `id_column` is absent, a slice that has no `_id` after the renames gets a generated `Utf8` `_id` column of Ruling 7's UUIDs, with id type `uuid` (the mapper refuses a schema without `_id`, T0-2). A null in a present `_id` (the file's own or `id_column`'s) is the mapper's row error `the primary key is null` and follows `on_error`; no id is generated for it (T1-8); then `CollectionBatchMapper::new(schema_of_slice, collection_schema, id_type)` and `map(batch)`; `CollectionService::write(ns, c, ops, WriteOptions::default())` in chunks of `config.flight.put_chunk_rows` (T0-3). `ServiceError::ResourceExhausted` (the 429 of D86; waiting at least `retry_after_ms`), `Unavailable` and `Timeout` are retried inside the step with jittered backoff up to 5 minutes; any other error follows `on_error` (T0-1).
5. **Fan-in**: `{files_total, files_done, files_failed, rows_written, bytes_read, token}`, where `token` merges every slice's consistency token (D76).
6. **Progress** for `GET /v1/operations/{id}`: `files_done` counts resolved file branches (a tag search), `rows_written` and `bytes_read` sum the settled slice values of running branches (a search by `loams:op`), and `files_total` comes from the plan value.
7. **Limits**: at most 100,000 files per operation (400 above that, with a hint to use several prefixes); `max_concurrent_operations` per namespace (Ruling 9) → 429 `too_many_operations`.

**Tests** (`tests/import.rs`, `tests/import_crash.rs`; sources are a `file://` bucket and the in-memory store with fault injection):
- `imports_parquet_and_ndjson` (row counts, a search finds the documents, and the result token makes a strong read see them);
- `rows_without_id_get_deterministic_ids`;
- `id_column_becomes_the_id` (and `id_column_missing` fails the file; `id_column` with a `columns` target of `_id` is 400; two `columns` entries with one target are 400; a target that collides with an unrenamed column is `mapping_conflict`; a null `_id` is a row error; without `id_column`, a file's own `_id` column is the id and no UUID is generated, O5);
- `file_changed_fails_its_branch`;
- `skip_file_on_error_counts_failures`;
- `backpressure_is_retried_not_failed` (a tiny unapplied budget);
- `import_survives_crash_at_every_step` (a fault hook stops the runtime after step k, for every k over a 3-file × 3-slice import, then restarts; the final count is exact);
- `rerun_slice_converges` (the same slice written twice → the same documents);
- `finished_files_are_not_reread` (a read counter on the source store);
- `idempotent_resubmit_resumes` (fail on file 2 with `on_error: fail`, fix the file, resubmit with the same key → files 0–1 are not re-read);
- `too_many_files_is_400`.

**Commit:** `import: add bulk import from object storage as a durable operation`.

### Task 9: Scheduled incremental import

**Files:** `crates/loams-durable/src/import/schedule.rs`, `crates/loams/src/api/import.rs`, `crates/loams-durable/tests/schedule.rs`.

**Semantics:**
1. **Routes:**
   - `POST /v1/namespaces/{ns}/collections/{c}/import-schedules {name, cron, source, format, pattern?, mapping?}` → 201, or 409 when the name exists;
   - `GET …/import-schedules` → a list with each schedule's last run: time, files started, files already imported, and failures;
   - `DELETE …/import-schedules/{name}` → 204.
2. **Schedule**: `schedule.create { id: "isched-<hex26(sha256(ns‖c‖name))>", cron, promiseId: "isched-<…>.{{.timestamp}}", promiseTimeout: 24 h, promiseParam: {…}, promiseTags: {"resonate:target": "inproc://any@loams", "loams:kind": "import.run"} }`. The cron syntax is Resonate's (5 fields); an invalid expression is Resonate's 400, passed through.
3. **A run** (`import.run`): list the source (as in the plan step), then for each file start a **root** durable call `impf-<hex26(sha256(schedule id ‖ key ‖ etag))>` running `import.file`. That is a file branch without a parent, tagged `loams:schedule=<id>`. Existing ids return their memoized results and are counted as `already_imported`. The run's value is `{started, already_imported}`. A run whose previous run is still pending returns `{skipped: "previous run still listing"}`.
4. `import.file` is Task 8's file branch, parametrized with the collection, format and mapping, so file semantics are identical.

**Tests** (`tests/schedule.rs`, debug clock, a cron of every minute driven by `debug.tick`): `new_files_import_once` (two ticks, three files, then one new file → 4 file calls in total); `changed_etag_imports_again`; `duplicate_name_is_409`; `delete_stops_future_runs`; `invalid_cron_is_400`; `schedule_survives_restart`.

**Commit:** `import: add scheduled incremental import`.

### Task 10: The D1 gates and the exit report

**Files:** `scripts/durable/examples.sh`, `.github/workflows/ci.yml` (job `durable-examples`), `docs/plans/d1-exit-report.md`.

**Semantics:**
1. `examples.sh` (nightly) clones the pinned example repositories: `example-hello-world-py`, `example-fan-out-fan-in-py`, `example-human-in-the-loop-py`, `example-schedule-py`, `example-money-transfer-py`, `example-hello-world-ts` and `example-fan-out-fan-in-ts`, at recorded commits. It installs the SDK versions from Task 0: Python `resonate-sdk==0.8.1` from PyPI, and TypeScript `@resonatehq/sdk@0.11.5` from npm forced over the examples' `^0.10.0` (0.10.4 fails on `task.fence`), run with Node and `tsx`, not `bun` (amended, O4; T0-20). Against `loams dev` it runs each example and checks its expected lines. For human-in-the-loop it runs `kill -9` on `loams` after the workflow blocks, restarts `loams` and resolves, then expects the root to be resolved.
2. **The import crash gate**: an import of 200 files (about 2 GB generated Parquet), with `loams dev` killed with `kill -9` at 10 random points and restarted each time. The final count is exact, no file is read more than once after it finished, and the total time is recorded.
3. **Exit report**:
   - the conformance results (SQLite and TiDB, the 503 counts);
   - the example suite;
   - the crash gates;
   - import throughput against Flight `DoPut` on the same data, measured on a quiet machine;
   - the binary and build-time delta (Task 0 against final);
   - the status of upstream PRs 0a, 0b, 0c and 1;
   - the rulings made during execution.

**Tests:** the gates themselves, plus `examples.sh --self-test` (an intentionally wrong expected line fails).

**Commit:** `ci: run the Resonate example suite against loams nightly`; `docs: add the D1 exit report`.

### Task 11: Documentation

**Files:** `docs/design/21-durable-execution.md` (as-built notes: rulings made during execution, the measured cost, the SDK versions), `docs/design/13-decision-log.md` (record new decisions; update D138–D147 statuses when the owner confirms them), `docs/design/01-architecture.md` §3 (the Resonate row: 127.0.0.1:8001, D1), `docs/design/10-operations.md` §2 (the `durable` listener block), `CHANGELOG.md`, `docs/plans/README.md` (D1 status), `crates/loams-durable/README.md` (dev setup: `loams dev`, a Python SDK example, the operations API, an import walkthrough).

**Commit:** `docs: record D1 as built`.

## PR grouping

One PR per group, stacked in order; each PR builds and passes CI on its own.

| PR | Tasks | Title |
|---|---|---|
| A | 0 | D1 (1/10): dependency measurements |
| B | 1 | D1 (2/10): the Resonate fork and the license policy |
| C | 2 | D1 (3/10): the embedded server on SQLite |
| D | 3 | D1 (4/10): the durable listener in `loams` |
| E | 4 | D1 (5/10): the TiDB backend |
| F | 5 | D1 (6/10): the linearizability run against `loams` |
| G | 6 | D1 (7/10): Loams's in-process durable runtime |
| H | 7 | D1 (8/10): the operations API |
| I | 8, 9 | D1 (9/10): bulk import and scheduled import |
| J | 10, 11 | D1 (10/10): gates, exit report, docs |

## Rulings made during execution

### Task 0: reconciliation with the as-built code and the embed (2026-09-27)

Checked against `main` at `527efd5` (M1.2 merged, M1.4 through Task 7, R1 Task 0 merged): `crates/loams/src/{server.rs,main.rs,api/mod.rs}`, `crates/loams-query/src/{write.rs,flight_ingest.rs,flight.rs,types.rs,error.rs}` and `crates/loams-store/src/store.rs`. Also checked: the fork `ostrium-labs/resonate` at `c3f25b9` (upstream `28dfd01` + PR 0c) linked into `loams` on a scratch branch, and `tiup playground v8.5.8 --tag loams-d1-t0 --port-offset 27000`. Commands, versions and raw results are in [`d1-dependency-spike.md`](d1-dependency-spike.md). These rows amend the task text; where a row says "amended", the task text above already carries the change.

| # | Checked | As built / found | Ruling |
|---|---|---|---|
| T0-1 | Consumes: `CollectionService::write` | Matches (`loams-query/src/write.rs:180`).<br>• A write holds at most `MAX_WRITE_OPS` = 10,000 ops.<br>• `WriteOptions { report_existence, atomic, backpressure: Override }`.<br>• The D86 refusal is `ServiceError::ResourceExhausted { message, retry_after_ms }` (429). "Unavailable" is `ServiceError::Unavailable(_)` or `Timeout` | Task 8 retries `ResourceExhausted` (waiting at least `retry_after_ms`, with jitter), `Unavailable` and `Timeout` inside the slice step, for up to 5 minutes. Every other `ServiceError` follows `on_error`. Imports write with `WriteOptions::default()` (normal backpressure, not `Bulk`) (amended) |
| T0-2 | Consumes: `CollectionBatchMapper::new` / `map` | Signatures match (`flight_ingest.rs:495`, `:564`), and `RowError { row, column, message }` is as expected. But **`new` refuses an Arrow schema without an `_id` column** ("a collection put needs an _id column"). The id type is resolved by `IdType::of_schema(arrow, option)` from the `ID_TYPE_METADATA` field metadata or the put's option (`Str`, `U64`, `Uuid`). Columns map by name (`_id`, `_source`, vector and sparse names, schema fields, ignored columns); there is no mapping object | Ruling 7 needs a step before the mapper. When a slice has no `_id`, the importer appends an `_id` column of Ruling 7's ids before `CollectionBatchMapper::new`. The column is `Utf8` holding the UUID text, with `ID_TYPE_METADATA` = `uuid`, so the keys are `PrimaryKey::Uuid`. Keys are typed per document (`loams-collection/src/pk.rs`), not per collection, so no collection refuses them (amended, X4) |
| T0-3 | `put_chunk_rows` | A field of `FlightConfig` (`loams-query/src/flight.rs:203`): default 10,000, validated to `1..=MAX_WRITE_OPS`, reached through `ServerConfig.flight`. It is not a constant | The importer takes `config.flight.put_chunk_rows` when the runtime is built (amended) |
| T0-4 | §21 §7.2 `mapping` ("same as Flight `DoPut`'s column mapping") | `DoPut` has no mapping object (T0-2) | D1's `mapping` is `{ "columns": { "<source>": "<collection column>" }, "id_type": "str" \| "u64" \| "uuid" }`. It is applied as an Arrow schema rename, plus the id-type metadata, before the mapper. Unknown keys answer 400 (amended, X5; owner question 3) |
| T0-5 | `loams-store` for import | `Store::from_url(url, options)`, `list(prefix) -> Vec<ObjectInfo { path, size, e_tag, .. }>`, `get`, `get_range`, `head` and `inner() -> &Arc<dyn ObjectStore>` exist. **There is no conditional (If-Match) read.** Per-namespace storage credentials (§10 §3) are not built in M1 | Task 8 reads ranges through `store.inner().get_opts(path, GetOptions { if_match: Some(etag), range, .. })`. Parquet goes through a small `AsyncFileReader` over it, because `ParquetObjectReader` sets no `if_match`. A `PreconditionFailed` is `file_changed`. Sources open with `Store::from_url(source, [])`, with credentials from the process environment as `--bucket` does. Per-namespace credentials come with M2 (amended, X6) |
| T0-6 | `loams` start and stop order (Task 3 semantics 4) | `Server::start` → `MetaNode` → `start_on`: `initialize` → `wait_for_leader` → `MetaClient` → bind → `assemble` (builds `CollectionService`, the worker, the router) → serve. `shutdown`: Qdrant → native HTTP (not in cluster) → Flight → `collections.shutdown` → writer flush → worker → hot tier → factory → cache → metastore. In cluster mode the native listener keeps serving metastore routes until the replica stops | `DurableServer::start` runs after `wait_for_leader` and before `assemble`, because the router needs `Operations`. `DurableRuntime` (and the workflow registrations that need `CollectionService`) starts right after `assemble`, before the listener serves. Stop: after `flight.stop()` and before `collections.shutdown()`, first `DurableRuntime::stop`, then `DurableServer::stop`. In cluster mode this comes after `late.close()`. `stop_order_drains_durable_before_meta` asserts that durable stops before `collections.shutdown` as well (amended, X8) |
| T0-7 | `loams` CLI (Task 3 semantics 2, 5) | `dev`, `standalone` and `cluster` share the flattened `Native` args, and the flags belong there. The feature-off warning pattern is `apply_qdrant`'s `#[cfg(not(feature = …))]`. There is no startup banner: `main` prints one `loams … listening on …` line per listener, with the Qdrant lines "before the HTTP line, which harnesses wait for". `Warm` is the precedent for a subcommand without a server. `main` installs the global subscriber with `.init()` first thing, so D138's ban on `resonate_base::run` stands | `loams dev` prints `loams durable listening on http://<addr>` before the HTTP line. `loams durable migrate` is a subcommand like `Warm` (amended) |
| T0-8 | §21 §3.1 metrics through `:8090/metrics` | There is no admin or `/metrics` listener in `loams`, and `prometheus` is not in the graph before `durable` | D1 wires no metrics. Resonate registers into `prometheus` 0.14's default registry, and nothing serves it until the admin listener exists (M2 observability). With R1's `tikv-client`, `prometheus` 0.13's registry must be gathered too (spike (c)). §8's `loams_*` metrics move to that work; Task 11 records it (X12) |
| T0-9 | Consumes: Resonate `build`, `Running`, `Loader` | These match. `Registry` is `resonate_base::Registry` (a re-export of `resonate-plugin`'s), built as `Registry::new().server(&P).worker(&P).gateway(&P)`. `check() -> Result<(), Vec<RegistryError>>`. `build` reads only `options.default_server`; the file, env and override layers belong to the private `load`. `Loader::set` parses the value as TOML and quotes a value that does not parse. Every plugin `Config` is `deny_unknown_fields`, so a bad `--durable-set` key fails `build` and names the key. **`debug` is not a configuration key**; it is `Running::start(debug)`. `abort_on_panic` already defaults to `false` | Task 2 passes `Options::default().default_server("server_sqlite")`. Semantics 2 drops the `debug = <debug>` key; `--durable-debug` reaches `Running::start` (amended). `--durable-set` also refuses `gateways.gateway_http.auth` and `.workos` until the unified auth plan (amended) |
| T0-10 | Consumes: the Rust SDK | The package is **`resonate-sdk`** (lib `resonate_sdk`) 0.6.0, not `resonate`: that name is the server binary package in the same repository. `Network` is `#[async_trait]`, and the error is `resonate_sdk::error::Error` (`NetworkError(String)` for transport). `ResonateConfig { url, group, pid, ttl, token, encryptor, network }` all match. `Resonate::new(config) -> Self` has no `start`: it spawns `network.start()` and only logs its error. It reads `RESONATE_TOKEN` from the environment even with a custom network | The workspace dependency is `resonate-sdk`. `DurableRuntime::start` = `Resonate::new` + a readiness flag that `InProcNetwork::start` sets; that `start` cannot fail. `worker_inproc` ignores auth, so `RESONATE_TOKEN` is harmless; Task 6 documents it (amended) |
| T0-11 | Check 3 | The stub `InProcNetwork` compiled, and the SDK's `promises.get` and `promises.search` ran in process. The SDK also builds with `reqwest` `default-features = false, features = ["json", "stream"]`. reqwest 0.13's default TLS is rustls, so the defaults bring **no OpenSSL**; they only unify `charset`, `default` and `system-proxy` into `object_store`'s reqwest | The `sdk-rs: reqwest without default TLS` fork commit stays, for feature hygiene and not for OpenSSL. It uses `features = ["json", "stream"]`, and the SDK's `HttpNetwork` fallback (Ruling 5) needs no TLS against `127.0.0.1` |
| T0-12 | Ids and retention (Task 7 semantics 5) | The origin is everything before the first `:`. The server refuses `:` in an origin (`colon_in_origin`). Children are `<root>:<seg>`, then `.`-separated. `.` is allowed in a root. **There is no `tasks` table**: task state is columns on `promises`. Both the SQLite and MySQL schemas have a generated `origin_id` column, and `callbacks` and `listeners` cascade on delete (SQLite runs `PRAGMA foreign_keys = ON`) | Retention is `DELETE FROM promises WHERE origin_id = ?`, relying on the cascades. `retention_prunes_finished_after_7_days` also runs on TiDB (skipped without `LOAMS_TEST_TIDB`) and asserts that callbacks are gone (amended, X9). The ids `op-…`, `isched-<hex>`, `isched-<hex>.{{.timestamp}}` and `impf-…` are all valid roots |
| T0-13 | Check 5 | `promise.search` with `tags` (containment) and `state` works in process on SQLite and on TiDB v8.5.8. The probe searched 4 promises for 3, 2 and 1 matches, and the SDK search returned 3. On SQLite it is a scan | No change; the 2 s progress cache stays |
| T0-14 | Migrations across revision bumps | Upstream edits `0001_initial.sql` in place until its first release ("a database created before an edit holds version 1 under the old checksum. The migrator refuses it"), in both SQLite and MySQL | A pin bump in Task 1 or later compares the migration files with the pinned revision. If `0001` changed, the PR says that existing durable stores must be dropped, and the bump is a release note (X11). D1 has no deployed stores, so this costs nothing until one exists |
| T0-15 | Check 1: cost | Stripped `loams` is 292.6 MB without `durable` and 305.6 MB with it (**+13.0 MB**; unstripped +18.5 MB). A cold release build is 1,622 s against 1,497 s without (**+125 s (+8 %)**). The Resonate-only units sum to 367 s. +85 packages; semver-incompatible duplicate names go from 53 to 68 (+15: the spike's 11 plus `hmac`, `hashlink`, `jsonwebtoken` and `webpki-roots` inside the new subgraph) | Ruling 1 (default on) stood here; O1 then made `durable` opt-in; the cost is recorded in the spike doc and in §21 §11.4 at Task 11. `google-cloud-auth` (29.6 s, and `jsonwebtoken` 11) goes away with Task 1's `gcp-idtoken` feature |
| T0-16 | Feature unification (not in the plan's checks) | `verus_syn` (`resonate-timer-wheel`) turns on `proc-macro2/span-locations`, which changes the fingerprint of every proc-macro and of nearly every crate. Adding `durable` to a warm tree recompiled 479 crates in 23 min 44 s. `-p loams` builds (with `durable` by default) and `-p <library>` builds no longer share artifacts. It also adds features to `syn`, `tower-http`, `hyper-util`, `hyper-rustls`, reqwest 0.12 and 0.13 and others (spike (e)) | Accepted for D1: the verified timer wheel needs the `verus!` macro at compile time. CI's `durable` job builds only `loams`'s graph. Owner question 1 asked whether to keep `durable` default-on or make it opt-in for developer builds (X3); O1: opt-in |
| T0-17 | Check 2 | One lockfile: sqlx 0.8.6, one `libsqlite3-sys` (0.30.1 through `rusqlite` 0.32.1, no `sqlx-sqlite`), no `openssl-sys`. R1's `tikv-client` pin (`ab4be1c`, no default features) resolves beside it, adding `prometheus` 0.13.4 | No change (T0-8 covers the second registry) |
| T0-18 | Check 1: `cargo deny` | On `main`'s `deny.toml`: `advisories FAILED` (RUSTSEC-2023-0071) and `sources FAILED` (13 git sources). With Task 1's `allow-git` and the one ignore, with its rationale: `advisories ok, bans ok, licenses ok, sources ok`. Git dependencies without `version` pass `wildcards = "deny"` | Task 1's `deny.toml` text is exactly the plan's; nothing else is needed |
| T0-19 | Check 4 | 8001 appears only where Resonate is meant (docs), and no Loams default or playground offset reaches it | No change |
| T0-20 | Check 6 | PyPI `resonate-sdk` 0.8.1 (now published; the same version as the monorepo) passes hello-world, and 0.7.4 is refused with `400 Promise ID must be prefixed by resonate:origin`. npm `@resonatehq/sdk` 0.11.5 (the same as the monorepo) passes, and 0.10.4, which the TypeScript examples' `^0.10.0` resolves to, fails on `task.fence` 400. The TypeScript examples run under `bun` | Task 10 installs Python `resonate-sdk==0.8.1` from PyPI and runs the TypeScript examples with `@resonatehq/sdk@0.11.5` forced over their pin, run with Node and `tsx` (amended, X10; O4 replaced `bun`) |
| T0-21 | `google-cloud-auth` in PR 0c | At `c3f25b9`, `resonate-transport-http-push` still depends on `google-cloud-auth` unconditionally. #1164 did not add the feature | Task 1's first fork commit adds `gcp-idtoken` as the plan says. Upstream gets it as a follow-up to #1164, not inside it (one concern per PR) |
| T0-22 | Upstream state | #1162 (0a) open, #1163 (1) draft, #1164 (0c) open; issues #1165 (router, PR 4) and #1166 (retention, PR 5) open | Task 1 links them in the spike doc (done here) and records the fork's CI run |

| # | Ruling | Why | Tasks |
|---|---|---|---|
| X1 | The fork is pinned by `rev` only, never a branch. Task 0 measured `c3f25b9` (PR 0c only); Task 1 pins the head of `loam/0.10.1` | Reproducible builds; `c3f25b9` lacks 0a/1 and the `gcp-idtoken` feature | 1 |
| X2 | The SDK dependency is `resonate-sdk` (lib `resonate_sdk`) | T0-10 | 1, 6 |
| X3 | Superseded by O1: `durable` is opt-in. (It first said: `durable` stays a default feature despite T0-16, pending owner question 1) | T0-16; owner ruling | 1, 3 |
| X4 | Rows without `_id` get a `Utf8` `_id` column of Ruling 7's UUIDs (id type `uuid`) before mapping | T0-2: the mapper refuses a schema without `_id` | 8 |
| X5 | Import `mapping` = `{columns: {src: dst}, id_type, id_column?}`, applied as a schema rename (confirmed and extended by O3) | T0-4: `DoPut` has no mapping object | 8 |
| X6 | Import reads through `Store::inner()` with `GetOptions.if_match`; credentials come from the environment | T0-5 | 8 |
| X7 | Import retries `ResourceExhausted`, `Unavailable` and `Timeout` for 5 minutes and writes with `WriteOptions::default()` | T0-1 | 8 |
| X8 | `DurableServer` starts before `assemble` and `DurableRuntime` after it; both stop after Flight and before `collections.shutdown` | T0-6 | 3, 6 |
| X9 | Retention deletes `promises` by `origin_id` and relies on the cascades; tested on TiDB too | T0-12 | 7 |
| X10 | Examples: Python `resonate-sdk==0.8.1` from PyPI; TypeScript `@resonatehq/sdk@0.11.5` over the examples' pin, under Node and `tsx` (amended by O4; it first said `bun`) | T0-20 | 10 |
| X11 | A pin bump that changes a `0001_initial.sql` says so in its PR: existing durable stores must be dropped | T0-14 | 1, later bumps |
| X12 | D1 exposes no Resonate or `loams_*` metrics; they come with the admin listener (confirmed by O2) | T0-8 | 11 |


Owner questions from Task 0: all four were answered on 2026-09-27; the rulings are O1–O4 below.

### Owner rulings on the Task 0 questions (2026-09-27)

| # | Question | Ruling | Amends | Tasks |
|---|---|---|---|---|
| O1 | 1: `durable` on by default (X3, T0-15, T0-16) | **`durable` is opt-in, off by default**, and so is `durable-mysql`. Toggling it rebuilds about 479 crates. It is turned on explicitly in release builds, in CI's durable jobs (`durable`, `durable-tidb`, `durable-examples`) and in the Loams cloud build. The flag is documented in Tech Stack | Ruling 1, X3, Task 3 semantics 1, Task 5 | 3, 5, 10, 11 |
| O2 | 2: metrics (X12) | **No durable metrics in D1** until an admin `/metrics` listener exists. When metrics land, they must gather two `prometheus` registries: 0.13's default registry (through `tikv-client`) and 0.14's (through Resonate) (spike (c)) | X12 (confirmed) | 11, later metrics work |
| O3 | 3: the import `mapping` shape (X5, T0-4) | **`{columns: {source: target}, id_type}` is accepted**, plus an optional `id_column` naming the source column to use as `_id`. When it is absent, a UUID `_id` is generated (Ruling 7's deterministic ids, so a re-run slice converges) | X5, Task 8 semantics 4 and tests | 8, 9 |
| O4 | 4: `bun` or Node for the TypeScript examples (X10) | **Node and `tsx`, not `bun`**, with the pinned SDK versions: TypeScript `@resonatehq/sdk` 0.11.5 and Python `resonate-sdk` 0.8.1 | X10, T0-20, Task 10 semantics 1, Tech Stack | 10 |

### Task 1: the fork and the pinned dependencies (2026-09-27)

| # | Checked | As built / found | Ruling |
|---|---|---|---|
| T1-1 | The fork branch | `ostrium-labs/resonate` `loam/0.10.1` at **`e3606698e6e3f2502bb018bba1e618deb63f907a`**, five commits on `28dfd01`: `c3f25b9` (PR 0c, the same commit as #1164's head), `229a2f1` (the `gcp-idtoken` feature), `c5dfe9e` (PR 0a, cherry-picked from `6968aa2`), `3ff482b` (PR 1, from `f8d7ef2`) and `e360669` (the SDK's `reqwest` defaults). The only conflict was `resonate-server-mysql/Cargo.toml`: 0c's versioned path dependencies against 0a's `features = ["mysql"]`, resolved by keeping both. No migration file changed against `28dfd01` | One commit per upstream PR, not the plan's three (0c and the feature split, 0a and 1 split), so each maps to one upstream PR (amended). `Cargo.toml` pins this rev |
| T1-2 | `gcp-idtoken` | A feature of `resonate-transport-http-push`, default on, that makes `google-cloud-auth` optional. Without it, `configure` refuses `auth.mode = "gcp"` and names `workers.transport_http_push.auth.mode`. Otherwise every delivery would go out without its header. `Auth::from_config` (public) gets a provider whose every mint fails, which takes the existing failed-mint path. The config schema and `TokenProvider` do not change. There is a new test, `gcp_mode_follows_the_gcp_idtoken_feature`; the crate's tests pass 12/12 with the feature and without it | Loams names the crate with `default-features = false`. In Loams's graph `google-cloud-auth`, `google-cloud-gax`, `-rpc`, `-wkt`, `jsonwebtoken` 11 and reqwest 0.13's default TLS are gone. `jsonwebtoken` 9 (`resonate-auth`) remains |
| T1-3 | The SDK's `reqwest` | reqwest is named with `default-features = false, features = ["json", "stream"]`, and a new default-on SDK feature, `reqwest-default = ["reqwest/default"]`, puts reqwest's defaults back. Without them, the SDK's `HttpNetwork` could not reach an `https://` server | Loams names `resonate-sdk` with `default-features = false`: the same effect as T0-11's text. Upstream builds are unchanged, so the commit can be upstreamed (amended) |
| T1-4 | The fork's CI (Task 1 semantics 2) | Actions is enabled on `ostrium-labs/resonate`, but GitHub lists no workflows for the fork. `gh workflow run server-core-ci.yml --ref loam/0.10.1` answers 404 ("not found on the default branch"). A fork's workflows stay off until someone enables them in its Actions tab. The workflows also run only on pushes to `main` and on PRs | **Not run.** Scoped local verification stands in for now (spike (j)). Owner question 5. Ruling 4's green run is still required before Task 5 relies on the pin |
| T1-5 | Loams's pin | `[workspace.dependencies]` has the nine crates at the rev, with `default-features = false` on `resonate-transport-http-push` and `resonate-sdk`, and `parquet` 58 as the plan gives it. No crate uses them until Task 2, so `Cargo.lock` does not change. `cargo deny check` gives `advisories ok, bans ok, licenses ok, sources ok`, with two warnings: `unmatched-source` for the fork and `advisory-not-detected` for RUSTSEC-2023-0071 | A throwaway crate that depended on all ten (not committed) resolved into one lockfile. With it, `cargo deny check` was all ok with **no warnings**. `cargo tree -i` found no `openssl-sys`, `native-tls` or `google-cloud-auth`. It found sqlx 0.8.6, one `libsqlite3-sys` (0.30.1) and parquet 58.4.0. `cargo check` of it took 45 s. The two warnings go away when Task 2 adds `loams-durable` |
| T1-6 | Upstream (Task 1 semantics 3) | #1164 (0c), #1162 (0a) and #1163 (1) are open. The two new commits have their own fork branches on `28dfd01`: `feat/push-gcp-idtoken-feature` (`849813f`; tested with and without the feature) and `feat/sdk-rs-reqwest-default-feature` (`e5ddb8a`) | PR bodies for 0d and 0e are ready for the owner to post. No PR or issue was opened on `resonatehq/resonate` by this task |
| T1-7 | X11 (migrations) | `git diff 28dfd01 e360669` touches no `migrations` file | No durable store needs dropping at this pin |
| T1-8 | CodeRabbit on #67 (Task 8 semantics 4, line 334): apply the mapping before generating ids, reject duplicate destination names, and give rows with a null mapped `_id` a Ruling 7 id | The order was already renames first; the amended text says so explicitly. `CollectionBatchMapper::new` does refuse a repeated column name (`flight_ingest.rs:475`), and `map` refuses a null key as `the primary key is null` (`:414`) | Duplicates: accepted (400 before submit for two entries with one target; `mapping_conflict` for a collision with an unrenamed file column). Null `_id`: not accepted. A generated UUID is not type-compatible with `id_type` `u64`, and inventing ids for a column the user named as the key would hide bad rows, so a null stays a row error under `on_error` |

Owner questions from Task 1:
5. **The fork's CI (T1-4).** Enable Actions on `ostrium-labs/resonate` (its Actions tab), then dispatch `server-core` and `sdk-rs` on `loam/0.10.1`. Ruling 4 needs the green run, and `server-core` runs only on pushes to `main`, on PRs and on `workflow_dispatch`.
6. **Upstream PRs 0d and 0e (T1-6).** Their bodies are ready to post against `resonatehq/resonate` `main`, from the fork branches `feat/push-gcp-idtoken-feature` and `feat/sdk-rs-reqwest-default-feature`.
7. **`id_column` absent while the file has an `_id` column (O3).** Task 8 as amended uses the file's own `_id` in that case, and generates UUIDs only when the slice has no `_id`. Should an absent `id_column` always generate UUIDs instead? Answered by O5.

### Owner ruling on the Task 1 question (2026-09-27)

| # | Question | Ruling | Amends | Tasks |
|---|---|---|---|---|
| O5 | 7: `id_column` absent while the file has its own `_id` column (O3, T1-8) | **Use the file's `_id` column.** Generate Ruling 7's UUIDs only when the slice has no `_id` column at all (after the renames). This matches the Elasticsearch and Qdrant import conventions. A null in the file's `_id` stays a row error under `on_error` (T1-8) | Task 8 semantics 4 (confirmed as written) and its tests | 8, 9 |

### Task 2: the embedded server (2026-09-27)

| # | Checked | As built / found | Ruling |
|---|---|---|---|
| T2-1 | The crate | `crates/loams-durable`: `lib.rs`, `config.rs`, `embed.rs`, `registry.rs`, `listen.rs`, `error.rs`, and `inproc.rs` with the placeholder `worker_inproc` plugin (krate `worker-inproc`, scheme `inproc`, configures to nothing until Task 6). Feature `mysql` makes `resonate-server-mysql` optional. The SDK is not a dependency yet (Task 6) | As planned |
| T2-2 | `cargo deny check` with the pins in use | Refused `bans` first: `wildcard` for the seven Resonate git dependencies. cargo-deny's `allow-wildcard-paths` spares only private crates; T0-18's "git dependencies without `version` pass" held because the scratch crate was `publish = false` | `loams-durable` is `publish = false`, like `loams-tikv` (crates.io refuses git dependencies). Then `advisories ok, bans ok, licenses ok, sources ok` with **no warnings**: Task 1's `unmatched-source` and `advisory-not-detected` are gone (`[graph] all-features` reaches `sqlx-mysql`). `cargo tree -i` finds no `openssl-sys`, `native-tls` or `google-cloud-auth`; sqlx 0.8.6 and `libsqlite3-sys` 0.30.1 only. `loams` names `loams-durable` by path and version, so Task 3 is not affected |
| T2-3 | `DurableError` | thiserror takes a field named `source` for an error source, and `Bind`'s `source` is a `String` | `Display` and `Error` are written by hand; the plan's variants and field names stand |
| T2-4 | `--durable-set` refusals (semantics 2) | Resonate's own key-space check (`check_key_space`) is private to `resonate_base::run`, and the process section (`level`, `debug`, `shutdown_timeout`) is read only there, so either would be silently ignored in the embed. The store path/URL and push have Loams flags; overriding them would bypass the store lock or the push warning | Refused as well (amended in semantics 2): the store and push keys; any key equal to, under, or a table above a protected key; quoted keys; keys outside the three sections; a plugin id the registry does not carry. An unknown field under a carried plugin fails `build` through `deny_unknown_fields` and names the key (`Config`) |
| T2-5 | `process` (semantics 5) | `ResponseEnvelope` is `Serialize` only | `process` answers a 2xx with the whole response envelope (`kind`, `head.status`, `data`); any other status is `Protocol { status, body }` with the response's `data`; no answer is `Unavailable`. A caller's `head` fields (such as `resonate:debug_time`) are kept; a missing `corrId` becomes `loams-<node_id>-<n>` |
| T2-6 | Parsing `--durable-listen` (for Task 3) | `SocketAddr` cannot hold `localhost` | `loams_durable::parse_listen` takes an IP socket address or `localhost:<port>` (127.0.0.1) and refuses any other host name without resolving it. `is_loopback` accepts 127.0.0.0/8, `::1` and IPv4-mapped loopback |
| T2-7 | Start order (semantics 3, 4, 6) | Loopback check → `registry.check()` → the configuration (overrides) → the SQLite directory and `durable.lock` (`std::fs::File::try_lock`, flock) → the port probe → `build` → `Running::start(debug)`. A gateway bind that loses the race after the probe is `Bind` (logged); any other start failure is `Start`. A `Mysql` store without the `mysql` feature is `Config`: `this build has no MySQL durable store (the durable-mysql feature is off)` | `stop` runs `Running::stop(shutdown_timeout)`, drops it, waits up to 2 s for the port, then releases the lock. A loopback start logs one line that the API is unauthenticated; `push` logs the SSRF warning |
| T2-8 | `handler_panic_answers_500` | `Routes` is created inside `resonate_base::build`; only a plugin's `configure` can add to it | A hidden `DurableServer::start_with_plugins(config, node_id, &[&'static WorkerPlugin])` appends test workers. The test's worker registers a panicking route and configures to nothing; the gateway's catch-panic layer answers 500 and the server keeps serving |
| T2-9 | `second_process_on_same_store_is_refused` | flock conflicts between two open file descriptions in one process exactly as between two processes | The test opens the second server in the same process; no child process is spawned |
| T2-10 | MySQL (Task 4) | Task 2 sets `servers.server_mysql.url`, `server_url` and `retry_timeout`, never `migrate` | `MysqlTls` is carried but not yet applied: Task 4 maps it onto the URL's `ssl-mode` |
| T2-11 | CodeRabbit on #70 | "No actionable comments were generated" | Nothing to fold in |

### Task 3: the durable listener in `loams` (2026-09-27)

| # | Checked | As built / found | Ruling |
|---|---|---|---|
| T3-1 | Features and dependency (semantics 1) | `[workspace.dependencies]` gains `loams-durable = { path, version = "0.0.1" }`; `loams` names it `optional = true`; `durable = ["dep:loams-durable"]`, `durable-mysql = ["durable", "loams-durable/mysql"]`, neither in `default`. `cargo tree -p loams -e normal` finds no `resonate` crate without the feature | As planned (O1) |
| T3-2 | `cluster` without `--durable-store` (semantics 2) | "No default" left open whether the node fails or serves nothing | A cluster node without `--durable-store` serves no durable API (`ServerConfig.durable = None`); any other `--durable-*` flag then logs that it is ignored. `sqlite:` on a cluster is refused by `ServerConfig::validate` (so library callers get it too), with the plan's message (amended in semantics 2) |
| T3-3 | Parsing the flags | `--durable-listen` goes through `loams_durable::parse_listen` as clap's value parser, so a non-loopback address fails argument parsing with D138's message. Without the feature `loams_durable` is not linked: the flag parses as a plain socket address and only logs. `--durable-store` takes `sqlite:<path>` (a URL-style `sqlite:///abs` is read as `/abs`) or `mysql://…`, with `MysqlTls::Required` until Task 4 maps it. `--durable-set` splits at the first `=`; `--no-durable` conflicts with every other durable flag | `--no-durable` alone is not a `--durable-*` flag: without the feature it logs nothing |
| T3-4 | Start order (semantics 4, T0-6) | Single node: `wait_for_leader` → `MetaClient` → native `bind` → `DurableServer::start` → `assemble` → serve. Cluster: `wait_for_leader` → node registry and remote reads → `DurableServer::start` → `assemble` → `late.set`. The durable node id is the meta node id in decimal | A failed `assemble` stops the durable server (and, on a cluster, deregisters); a durable start failure is `ServerError::Durable` with `DurableError`'s message as is, and `Server::start` releases the metastore as for any failure. The points where Task 6 starts and stops `DurableRuntime` are marked in `server.rs` |
| T3-5 | Stop order (semantics 4, X8) | `shutdown` stops the durable server after `flight.stop()` and before `collections.shutdown()`; in cluster mode that is after `late.close()`, deregistration and leave | Each step of `shutdown` emits a debug event (target `loams::shutdown`, field `phase`: `qdrant`, `http`, `flight`, `durable`, `collections`, `writer`, `worker`, `hot`, `metastore`), which `stop_order_drains_durable_before_meta` records |
| T3-6 | The startup line (semantics 5, T0-7) | `main` prints one line per listener | `loams durable listening on http://<addr>` is printed first, before the Qdrant lines and the HTTP line, in every mode that serves it (`Server::durable_addr`, `None` without the feature) |
| T3-7 | Existing tests under `--features durable` | The binary tests spawn several `loams dev` processes at once; with the feature each would take 127.0.0.1:8001 | The spawns in `tests/it/{http,hot_http}.rs`, `tests/crash.rs` and `tests/qdrant/harness.rs` pass `--no-durable` (accepted with or without the feature). `ServerConfig::new` keeps `durable = None`, so in-process tests are unaffected |
| T3-8 | Tests | Flag parsing: `durable_store_and_set_values_parse`, `durable_defaults_on_dev_and_standalone`, `durable_flags_set_the_config`, `cluster_durable_needs_a_mysql_store`, and without the feature `durable_flags_parse_without_the_feature`. `tests/durable.rs`: `dev_serves_durable_on_8001_style_port`, `stop_order_drains_durable_before_meta` (also restarts on the same port and store), and `durable_start_failure_is_fatal` (a taken port names both flags; a retry in the same directory starts) | The third binary test is added beyond the plan's two |
| T3-9 | CodeRabbit on #72 | "No actionable comments were generated" | Nothing to fold in |

### Task 4: the TiDB backend (2026-09-27)

Checked against TiDB v8.5.8 (`scripts/durable/tidb.sh up --tag loams-d1t4 --port-offset 27000`, TiDB on 127.0.0.1:31000) and the fork at `e360669`.

| # | Checked | As built / found | Ruling |
|---|---|---|---|
| T4-1 | The mysql `DurableStore`'s Debug (Task 3 carry) | **A security bug.** `DurableStore` had a derived `Debug`, so the Debug output of a `DurableConfig` or a `ServerConfig` printed the URL with its password. `loams`'s `DurableStoreArg` also derived `Debug`. A plain `fn` value parser's clap error quotes the value | Fixed first, and tested first (`debug_and_display_redact_the_password` failed before the fix). `DurableStore` has a hand-written `Debug` and a new `Display` (`sqlite:<path>`, or the URL through the public `redact_url`, where the password becomes `***`). Errors from sqlx or Resonate on a MySQL store go through `scrub`, which removes the URL forms and any password of 4 or more characters. `--durable-store` uses a `TypedValueParser` whose error does not quote the value. `DurableStoreArg`'s `Debug` redacts too. Test: `durable_store_password_never_printed` (loams) |
| T4-2 | `MysqlTls` on the URL (semantics 1) | sqlx reads `ssl-mode`/`sslmode` = `disabled`, `preferred`, `required`, `verify_ca` or `verify_identity`, and defaults to `preferred`. The url crate treats a `mysql://` host as opaque, so an IP address comes back as a domain | `DurableStore::mysql(url)` reads `ssl-mode` (or `sslmode`): `required` or `disabled`. `preferred` (a silent downgrade) and the `verify_*` modes (they need a CA option) are refused (owner question 8). Without the parameter TLS is `Required`, except for `localhost` and loopback addresses (127.0.0.0/8 and `::1`, not only 127.0.0.1). `tls` wins over the URL: Resonate gets the URL with `ssl-mode=REQUIRED\|DISABLED`, and `/loams_durable_default` when the URL has no database (Ruling 2). The URL is kept as given. The playground's TiDB accepts `ssl-mode=required` (it auto-generates a certificate) |
| T4-3 | Serving never migrates (semantics 2) | Resonate's MySQL `init` always creates the schema in an empty database (`may_apply`: `applied == 0` passes whatever `migrate` says). With one migration (`0001`), "behind the binary" can only be an edited `0001` (a checksum mismatch, T0-14) | Before building, `DurableServer::start` checks a MySQL store on one connection of its own (sqlx 0.8.6, the version Resonate already uses; a new direct dependency of `loams-durable`'s `mysql` feature, with no new crate in the lock). If `_sqlx_migrations` is missing or records nothing, it fails with `the durable store <redacted> has no durable schema; run 'loams durable migrate' first`. For a checksum mismatch it shows Resonate's message ("Drop it…") and adds that the command must then run. A pending-migration message gets `; run 'loams durable migrate' first`. An unknown database adds `create the database first`. Without the check, `unmigrated_schema_names_the_command` failed: serving created the schema |
| T4-4 | `loams durable migrate` (semantics 2) | `Warm` is the precedent for a subcommand that starts no server (T0-7) | `DurableServer::migrate(store)` builds with `servers.server_mysql.migrate = true` and the gateway and poll transport disabled. It opens no listener, starts, stops, and prints `loams durable: <redacted store> is migrated`. A `sqlite:` store is accepted too (it takes the store lock). Without the `durable` feature the command prints the feature-off line and exits 1. `servers.server_mysql.migrate` joins `PROTECTED`, so `--durable-set` cannot turn it on while serving |
| T4-5 | The pessimistic pin (semantics 4) | The fork runs `SET SESSION tidb_txn_mode = 'pessimistic'` in `after_connect` on its private pool. Loams cannot read that pool's sessions without a fork patch, and fork discipline allows none. TiDB's `information_schema.cluster_statements_summary` counts statements per `SCHEMA_NAME` | `optimistic_cluster_still_pessimistic_sessions` sets `GLOBAL tidb_txn_mode = 'optimistic'` and restores it even when an assertion fails. It checks that a new plain session is optimistic. It then runs the contended workload through two servers with zero 503s, and asserts that in the test's database the count of `set session tidb_txn_mode = ?` equals the count of `select version()` (every server connection ran the pin), and that the count is above 0. A test assertion, not a runtime debug assertion (amended) |
| T4-6 | `scripts/durable/tidb.sh` (semantics 3) | R1's `scripts/tikv/playground.sh --with-tidb` runs a **keyspace-mode** TiDB (`keyspace-name = "sql_dev"`) at a fixed `--port-offset 17000` that R1 agents use. Resonate is verified on a plain TiDB (resonatehq/resonate#1161) | `tidb.sh up\|down\|status [--tag T] [--port-offset N]` always runs the plan's fallback command, a plain `tiup playground v8.5.8 … --db 1`. The defaults are tag `loams-durable` and offset 17000. It refuses to start during a build, creates `loams_durable_default` and prints `LOAMS_TEST_TIDB` and the `--durable-store` URL. `down` stops the playground and deletes `~/.tiup/data/<tag>` (amended) |
| T4-7 | Tests | Each TiDB test creates its own database `loams_t_<test>_<pid>_<nanos>` from the admin URL and drops it. The linearizability smoke uses 4 ids × 8 concurrent create+settle calls over two servers. It checks that both servers read the same promise, that every 2xx settle answered with the final state and value, that no id with a 2xx settle is still pending, and that every 2xx create answered with the same id | 5/5 TiDB tests pass, three runs in a row plus a final run after the last edit, about 8 s each. Unit tests: `mysql_urls_map_tls_onto_ssl_mode`, `the_resonate_url_carries_the_tls_mode`, `scrub_removes_the_password_from_driver_messages`, `migrate_is_owned_by_the_migrate_command`, and in loams `durable_migrate_parses` and `durable_mysql_store_maps_tls`. The binary was run by hand against the playground: `dev` on an empty database was refused naming the command; no output from a wrong password, `ssl-mode=preferred` or `migrate` with `ssl-mode=required` showed the password (the store printed as `mysql://loams:***@…`); after `migrate`, `dev` served and `/ready` answered 200 |
| T4-8 | CodeRabbit on #77 | One comment: `--durable-debug` was missing from the `--no-durable` conflict test | Added (`…: address review comments on #77`) |

Owner questions from Task 4:
8. **`ssl-mode=verify_ca` / `verify_identity` (T4-2).** They are refused for now, because `MysqlTls` has only `Required` and `Disabled`, and verification needs a CA (`ssl-ca`). Should D1 add a `VerifyIdentity` mode and pass `ssl-ca` through for managed TiDB (TiDB Cloud)?

### Owner rulings on the Task 1 and Task 4 questions (2026-09-27)

| # | Question | Ruling | Amends | Tasks |
|---|---|---|---|---|
| O6 | 5: the fork's CI (T1-4) | **Resolved.** `ostrium-labs/resonate` `loam/0.10.1` at `e3606698` has green runs: `server-core` 36317061832, `sdk-rs` 36317061789 and S3 CI 36317061786. Ruling 4's precondition for Task 5 holds (T5-1) | T1-4 | 5 |
| O7 | 6: upstream PRs 0d and 0e (T1-6) | **Resolved.** Posted as resonatehq/resonate#1167 (0d, `gcp-idtoken`) and #1168 (0e, the SDK's `reqwest` defaults) (T5-2) | T1-6 | — |
| O8 | 8: verifying TLS modes (T4-2) | **Yes.** `MysqlTls` gains `VerifyCa` and `VerifyIdentity`, with the CA file passed through as `ssl-ca=<path>` on the URL (or a flag), for managed TiDB such as TiDB Cloud. In this PR if small, with tests; otherwise in Task 6's text. Done in Task 5 (T5-3, T5-4) | Task 4 semantics 1 | 5 |

### Task 5: the conformance run (2026-09-27)

Checked against the fork at `e360669` (`~/Documents/research-clones/resonate-loam`), Go 1.24.7 (the module's `go 1.24.7` fetched by `GOTOOLCHAIN=auto` over the local 1.24.5), and TiDB v8.5.8 (`scripts/durable/tidb.sh up --tag loams-d1t5 --port-offset 28000`, TiDB on 127.0.0.1:32000).

| # | Checked | As built / found | Ruling |
|---|---|---|---|
| T5-1 | Ruling 4's precondition (the fork's CI) | Green at `e3606698`: `server-core` run 36317061832, `sdk-rs` 36317061789, S3 CI 36317061786 | Task 5 relies on the pin (O6) |
| T5-2 | Upstream 0d, 0e | resonatehq/resonate#1167 and #1168 are open | Nothing is patched in the fork beyond T1-1 (O7) |
| T5-3 | O8: verifying TLS | `MysqlTls` is `Required`, `Disabled`, `VerifyCa` or `VerifyIdentity` (still `Copy`); `ssl-mode` accepts `verify_ca` and `verify_identity`. `ssl-ca=<path>` stays in the URL and reaches sqlx through `mysql_url` with the other parameters; there is no flag (the URL form covers it). A missing `ssl-ca` file, and `ssl-ca` without a verifying mode (it would look like verification and be none), are refused with the path and the redacted URL. Without `ssl-ca`, sqlx verifies against its built-in webpki (Mozilla) roots, which is what a public TiDB Cloud certificate needs. Tests: `verifying_tls_modes_take_a_ca_file`, `the_resonate_url_carries_the_verifying_modes`, loams's `durable_mysql_store_maps_tls`, and on TiDB `verify_identity_refuses_an_untrusted_certificate` (the playground's self-signed certificate is refused before any schema is made; `required` then migrates) | Implemented here, not carried to Task 6 (amended, Task 4 semantics 1 via O8) |
| T5-4 | `verify_ca` in the `loams` binary | **A panic.** `loams durable migrate --durable-store 'mysql://…?ssl-mode=verify_ca&ssl-ca=<cert>'` panicked in rustls: `Could not automatically determine the process-level CryptoProvider`. In `loams`'s graph rustls 0.23.45 has both `ring` and `aws-lc-rs`, so it has no automatic default, and sqlx builds the `verify_ca` verifier with `WebPkiServerVerifier::builder`, which needs one (the other modes happen to pass a provider). `loams-durable`'s own test graph has only `ring`, so its tests could not show it | `mysql::ensure_crypto_provider(tls)` installs rustls's `ring` provider (sqlx's) as the process default before a TLS MySQL store is opened (`start`, `migrate`), only when none is set; a host's own choice wins. `rustls` 0.23 (no default features; `ring`, `std`) is a new direct, optional dependency of the `mysql` feature, already in the lock. Test: `a_tls_store_has_a_crypto_provider`. After the fix the same command answers with a certificate error instead of panicking. Process hygiene amended |
| T5-5 | `conformance.sh` (semantics 1) | The flags and fork handling in semantics 1. `conctrace` needs debug mode (`--durable-debug`, so `resonate:debug_time` is the clock) and posts to `/`. The tally shows 3xx: the server answers 300 (resume now) a few times per run. A relative `--out` broke the checker, which runs from the fork's directory, so `--out` is made absolute | As amended in semantics 1 |
| T5-6 | `porc-503.sh` (semantics 2) | A 503 means nothing committed, and an operation that took no effect is, to linearizability, one never issued | The rewrite drops 503 rows and keeps every other row byte for byte |
| T5-7 | `--self-test` | On a 400-event history at 66-way concurrency, the doctored copy did not refute within `conccheck`'s 2-minute budget per discipline: it answered TIMEOUT twice (exit 0). On a 120-event history of `loams dev` (2 clients × 120 ops, seed 7) the doctored copy is NOT LINEARIZABLE in about 10 ms and the clean one linearizes in 4 ms | The fixture is `scripts/durable/testdata/embedded-sqlite.history` (120 rows). The self-test also requires the script's own verdict to fail on the doctored copy, and `porc-503.sh` to drop exactly one injected 503. It needs no server, only Go and the fork (fetched at the rev in about 30 s, 32 MB) |
| T5-8 | CI (semantics 3) | The plan's path filter would miss a pin bump, the change most likely to break the engine | `durable` also runs when the diff of `Cargo.toml` touches a `ostrium-labs/resonate` line, and on `crates/loams/tests/durable.rs`. It has a 120-minute timeout (a debug `loams` build with `durable` and a release one). `durable-tidb` installs `tidb:v8.5.8` beside PD and TiKV, the `mysql-client` package, builds everything before TiDB starts, uses tag `loams-ci-durable` at offset 27000, and uploads the conformance directory and the TiDB logs on failure. Go comes from `actions/setup-go` 1.24. `rust-cache` shares one `durable` key between the two jobs |
| T5-9 | Results | SQLite, 8 × 600, seed 1: LINEARIZABLE (both disciplines, about 70 ms), 0 × 5xx, max concurrency 27–30, three runs. TiDB, 8 × 600: LINEARIZABLE, 0 × 5xx, 0 × 503. TiDB, 16 × 400, seeds 11, 12, 13: LINEARIZABLE each, 0 × 5xx, 0 × 503. `tests/tidb.rs` 6/6 | The gate holds on both backends |
| T5-10 | `verify_ca` against a certificate for another name | sqlx 0.8.6's `verify_ca` verifier ignores only rustls's `CertificateError::NotValidForName`, but rustls 0.23.45 reports `NotValidForNameContext`, so `verify_ca` with the playground's own certificate as `ssl-ca` still failed on the host name (`certificate not valid for name "127.0.0.1"`). Verification is stricter than asked, never weaker | Carried: `verify_ca` currently checks the host name too; documented in Task 11 as "use `verify_identity` with a URL host the certificate names". An upstream sqlx fix lifts it |
| T5-11 | CodeRabbit on #79 | "No actionable comments were generated" | Nothing to fold in |

### Task 6: Loams's in-process durable runtime (2026-09-27)

Checked against the fork at `e360669` (`resonate-sdk` 0.6.0, `resonate-plugin`, `resonate-base`, `resonate-transport-http-poll`).

| # | Checked | As built / found | Ruling |
|---|---|---|---|
| T6-1 | How `DurableRuntime` reaches the worker (semantics 2) | A `WorkerPlugin`'s `configure` is a plain `fn` with no state, and `Running` keeps its workers private, so the worker `build` makes cannot be reached from outside | `DurableServer::start` takes a fresh instance number and sets `workers.worker_inproc.instance = <n>`. `configure` parks the worker it builds under that number, and the server takes it back right after `build` (whether or not `build` failed), then keeps it (`DurableServer::inproc()`). With no instance (`loams durable migrate`) the worker is off. `workers.worker_inproc` joins `PROTECTED`, so `--durable-set` cannot change it |
| T6-2 | The bytes delivered (semantics 2) | The poll transport serializes through a `serde_json::Value`. In Loams's workspace `serde_json` has `preserve_order`, so in Loams's build both transports write keys in insertion order | `worker_inproc` goes through the same `to_value` and `to_string` path, so its bytes match the poll transport's in any build. Test: `delivers_the_poll_transport_bytes` compares them with a real `PollRegistry` SSE frame. Unicast reaches that node only. Anycast prefers the named node, and otherwise goes round-robin. No subscriber answers `Unavailable` (`unicast_reaches_that_node_only_and_anycast_prefers_it`, `no_subscriber_is_unavailable`) |
| T6-3 | `InProcNetwork::send` (semantics 3) | The SDK expects the raw response envelope for every status: it checks `kind` and `corrId` and reads non-2xx bodies itself. `DurableServer::process` turns a non-2xx answer into an error. The SDK has no retry of its own. `RequestHead.auth` is parsed and unused by the server | `send` calls `ResonateServer::process` directly and returns the whole envelope as JSON. Transport failures map to `Error::NetworkError`, the SDK's transport variant. That covers the server being gone (it is held through a `Weak`: the server holds the router, the router holds the worker, the worker holds the SDK's callbacks, and they hold the network), a malformed request, and `Unavailable`. `head.auth`, the `RESONATE_TOKEN` that `Resonate::new` reads, is dropped before the server sees it (T0-10). After `stop`, every `send` fails, as for a process that went away |
| T6-4 | Registering functions after `start` (Produces) | `Resonate::new` spawns `network.start()` at once. With the plan's `start` followed by `sdk().register`, a task could arrive before its function existed. The SDK then answers `FunctionNotFound`, releases the task, and the server hands it straight back, so it spins until the function is registered | `InProcNetwork::start` waits on an `arm` gate before it subscribes. `DurableRuntime::start_with(server, node_id, RuntimeOptions, register)` calls `register` on the SDK, arms the gate, then waits up to 10 s for the subscription. `start(server, node_id)` is `start_with` with the defaults and no registrations; functions registered later through `sdk()` still work (`plain_start_registers_later`). `RuntimeOptions { ttl }` defaults to 60 s (semantics 4); the tests use 2 s so a stopped runtime's lease lapses quickly (amended in Produces) |
| T6-5 | Ruling 5 (semantics 5) | The SDK runs over `InProcNetwork` with no socket: `no_http_is_used` turns the gateway and the poll transport off through `--durable-set` (`gateways.gateway_http.enabled=false`, `workers.transport_http_poll.enabled=false`), checks that nothing listens on the address, and runs a two-step workflow | The `HttpNetwork` fallback is not needed; Ruling 5 is not applied |
| T6-6 | What retries (`failed_branch_alone_retries`) | A function that **returns `Err`** rejects its promise durably, and nothing retries it. A task is retried only when its execution fails, which is a panic (the SDK catches it) or a failed request. Then the SDK releases the task and the server hands it out again at once. A local `ctx.run` child runs inside its parent's task | The test's four branches are `ctx.rpc` tasks, and the one that fails panics on its first attempt. Only its task is released and rerun (2 runs); the other three run once. Task 8 retries its own transient write errors inside the slice step (X7) and must not rely on `Err` for a retry |
| T6-7 | `loams` (Task 3 carry) | T3-4 marked the start points, T3-5 the stop point | The private `Durable` wrapper holds `{ server, runtime }`. `start_runtime` runs after `assemble`, once the node is whole enough to shut down; a failure stops the durable server and shuts the node down through `Server::shutdown`. Single node: the native listener is spawned behind a gate and serves only after the runtime has started. Cluster: `late.set` moves from `start_joined` to after the runtime starts. `shutdown` emits the phase `durable_runtime` (the runtime stops), then `durable` (the server). `stop_order_drains_durable_before_meta` asserts `flight` < `durable_runtime` < `durable` < `collections`. `DurableRuntime::start` registers nothing yet: Tasks 7 and 8 add Loams's functions |
| T6-8 | Tests | `tests/inproc.rs` holds `two_step_function_runs` (the same id again runs nothing), `crash_between_steps_resumes_without_rerunning_step_1`, `failed_branch_alone_retries`, `sleep_survives_restart` (woke after 2 s or more; the step before the sleep did not rerun), `no_http_is_used` and `plain_start_registers_later`. Before the worker was real, all six failed: the placeholder `worker_inproc` left the runtime without a worker. After it, 6/6 pass, five runs in a row (about 2 s). The SQLite conformance run on the release binary (`--features durable-mysql`, 8 x 600, seed 1) is still LINEARIZABLE with 0 x 5xx. Unit tests are in `inproc.rs`, and `protected_keys_and_their_parents_are_refused` gains `workers.worker_inproc` | As planned, plus `plain_start_registers_later` |
| T6-9 | sqlx `verify_ca` (T5-10) | The upstream issue is drafted for the owner to post (not in the repository): `NoHostnameTlsVerifier` should also forgive `NotValidForNameContext` | Task 11's docs note stands until sqlx is fixed |
| T6-10 | CodeRabbit on #83 (three comments) | 1. The durable job path filter missed crates/loams/src/main.rs, crates/loams/Cargo.toml and Cargo.lock. 2. Treat sqlx sslca as an alias of ssl-ca. 3. The VerifyCa doc comment says the host name is not checked, which T5-10 contradicts | 1: fixed (`ci: address review comments on #83`); crates/loams/src/server.rs is added too, since it holds the durable lifecycle. 2 and 3: TLS findings, skipped per D111; T5-10 carries the VerifyCa note to Task 11 docs. 2 is an owner question |

## Self-review

| Check | Result |
|---|---|
| The embedded server behind the feature, public API only, no global side effects (D138) | Tasks 2, 3 |
| SQLite and TiDB backends (D139) | Tasks 2, 4 |
| The loopback listener on 127.0.0.1:8001, refusal of other addresses, push off (D138, D141) | Global Constraints, Tasks 2, 3 |
| The fork at a pinned revision, `allow-git`, the advisory ignore with a rationale (D140) | Task 1 |
| The conformance run of Resonate's harness against embedded Loams (D144) | Task 5 |
| Loams's own workflows on the Rust SDK in process (D141) | Task 6 |
| A submit-poll long-operation API for one real operation: bulk import (D145, D146) | Tasks 7, 8 |
| Fan-out with only failed branches re-run (pattern b) | Task 8 (`failed_branch_alone_retries` in Task 6, `idempotent_resubmit_resumes`) |
| One schedule: scheduled incremental import (D145) | Task 9 |
| Idempotency keys (pattern g, D146) | Tasks 7, 8 |
| No M1 code rewritten (D143) | Only routes and the listener are registered in `loams`; import uses M1.2's mapper and write path as they are |
| Other patterns on the roadmap (D143) | `docs/plans/README.md` track D rows; §21 §6, §14 |
| Review Focus → tests | 1: T8/T10 · 2: T7 · 3: T2/T3 · 4: T2 · 5: T5 · 6: T9 |
| Carried in | PR 0a/1 (research), PR 0c (spike) |
