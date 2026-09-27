# D1 — Embedded Durable Execution, the Operations API and Bulk Import Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, formats, constants), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: Planned** (2026-09-27). Track D, beside M1, M2 and R (D145). Branches `d1-t<N>`, stacked; PRs target `main`. D1 tasks interleave with M1 and R1 tasks on the one-build machine: never start a D1 build while another build runs, and never run the TiDB playground during a build. D1 adds crates and routes only; it changes no M1 code paths except to register the new routes and listener (D143: no M1 rewrite before M1 exits).

**Goal:** Ship the first slice of design §21 (D138–D147):
- `operon-durable`: the Resonate server embedded in the `operon` binary behind the cargo feature `durable`, on `127.0.0.1:8001`, refusing non-loopback addresses (D138);
- the SQLite backend for `operon dev`/`standalone` and the TiDB backend through Resonate's MySQL plugin (D139), from the pinned fork `dina-kar/resonate` (D140);
- Loam's durable runtime: the Resonate Rust SDK over an in-process network (D141);
- the operations API, `/v1/operations/{id}`, with idempotency keys (D146);
- **bulk import from object storage** into a collection, as a durable operation with per-file fan-out (D145);
- **scheduled incremental import** as a Resonate schedule (D145);
- the gates: porcupine linearizability against `operon dev` (SQLite, per PR) and against TiDB (nightly), the SDK example suite, the import crash tests, and the D1 exit report (D144).

**Architecture:**
- **The embed.** `operon-durable` names its plugins in a `resonate_plugin::Registry`: `resonate-server-sqlite`, `resonate-server-mysql` (feature `durable-mysql`), `resonate-transport-http-poll`, `resonate-transport-http-push` (linked, disabled unless `--durable-push`), Loam's `worker_inproc`, and `resonate-gateway-http`. It builds a `Configuration` from Loam's flags with `Loader::new().set(…)`, calls `resonate_base::build`, and starts and stops the result with `Running::start`/`stop`. It never calls `resonate_base::run`.
- **Loam's workflows** are Rust functions registered with the Resonate Rust SDK (`resonate` 0.6 from the fork). The SDK talks to the embedded server through `InProcNetwork`, which calls `ResonateServer::process` directly. It receives tasks through `worker_inproc`, a `WorkerPlugin` for the scheme `inproc` that hands each routed message to the SDK's `recv` callback.
- **The operations API** lives on the native API (`:8080`). It creates root promises with an in-process `promise.create`, starts workflows through the SDK, and reads state and progress with `promise.get` and `promise.search`.
- **Import** reuses M1.2's `CollectionBatchMapper` and `CollectionService::write`. It reads Parquet with `parquet` 58 and NDJSON with `arrow-json` 58 through `operon-store`.

**Tech Stack:**
- Rust 1.97.1, edition 2024, workspace lints (`unsafe_code = forbid`).
- New dependencies (Task 0 verifies versions, licenses and that they build together in the Operon workspace):
  - From the fork `https://github.com/dina-kar/resonate`, branch `loam/0.10.1`, pinned by `rev` (Task 1): `resonate-base`, `resonate-plugin`, `resonate-core`, `resonate-server-sqlite`, `resonate-server-mysql`, `resonate-transport-http-poll`, `resonate-transport-http-push`, `resonate-gateway-http` (all Apache-2.0, 0.10.1), and the Rust SDK `resonate` 0.6.0 (Apache-2.0). They bring `axum` 0.7 beside the workspace's 0.8 (isolated, §21 §11.3), `sqlx` 0.8.6, `rusqlite` 0.32 (bundled SQLite), `prometheus` 0.14 and the Verus crates `resonate-timer-wheel` pins (MIT).
  - `parquet` 58 (Apache-2.0), matching the workspace's arrow 58, features `arrow`, `async`, `object_store`.
  - Workspace crates reused: `arrow-json` 58, `object_store` 0.14 via `operon-store`, `axum` 0.8, `tokio`, `serde_json`, `sha2`, `ulid` (or the workspace's id helper), `tracing`, `thiserror`, `proptest`.
- Go 1.24 (for upstream's `conccheck`), Python 3.13 with `uv`, Node ≥ 22 with `pnpm` (the example suite). CI installs them in the `durable` jobs only.
- TiDB: R1's playground (`scripts/tikv/playground.sh start --with-tidb`, TiDB on `127.0.0.1:21000`, `tiup playground v8.5.8`). If R1 Task 1 has not merged, Task 4 uses `tiup playground v8.5.8 --tag loam-durable --port-offset 17000 --db 1 --kv 1 --pd 1 --tiflash 0 --without-monitor` directly.

**Spec:**
- [`docs/design/21-durable-execution.md`](../design/21-durable-execution.md): all of it; §3 (embed, listener, backends, transports, runtime), §6.2, §6.4, §6.7 (fan-out, operations API, idempotency), §7 (import and scheduled import), §8 (observability, retention), §9 (failures), §10 (testing), §11 (dependencies), §13 (spike).
- [`docs/design/14-durable-execution.md`](../design/14-durable-execution.md): §1 (protocol vocabulary), §5 (consistency model).
- [`docs/design/13-decision-log.md`](../design/13-decision-log.md): D76, D86, D90, D111, D138–D147; Q39–Q44.
- As built: [M1.2](2026-09-24-m1.2-query-engine.md) Task 13 (Flight `DoPut` mapping), `CollectionService::write` (M1.2), the `operon` server config and native router (`crates/operon/src/{server.rs,main.rs,api/mod.rs}`), [R1](2026-09-27-r1-reactive-core.md) Task 1 (the TiDB playground).

## Global Constraints

Same as the M1 overview §8, plus:
- **No protocol changes.** Loam never changes Resonate's wire format, status codes or semantics. Everything Loam-specific is a plugin, a flag or a route outside the protocol.
- **Fork discipline.** The fork branch `loam/0.10.1` holds upstream `28dfd01` plus only these commits: the dependency hygiene of upstream PR 0c, the TiDB fixes of PR 0a and PR 1, and the Rust SDK's `reqwest` defaults. Each is also opened upstream, one concern per PR. Nothing else is patched in the fork during D1.
- **Never read or copy** `resonate-server-scylladb` or the NATS packages (BUSL-1.1 lineage, §21 §11.1).
- **Loopback only (D138).** `--durable-listen` accepts only 127.0.0.0/8, `::1` and `localhost`. Any other address fails startup with `durable listener must be loopback until authentication is configured (D111); got <addr>`. A loopback bind logs one line saying the durable API is unauthenticated. Push delivery is off unless `--durable-push` is given, and `--durable-push` logs a warning naming the server-side request forgery risk.
- **Process hygiene.** `operon-durable` installs no tracing subscriber, no signal handler and no panic hook. `gateways.gateway_http.abort_on_panic` is always `false`, and `--durable-set` refuses to change it.
- **Cluster tests skip without a cluster.** TiDB tests read `OPERON_TEST_TIDB` (a MySQL URL for an admin user). Without it they print `skipped: <test> needs OPERON_TEST_TIDB`. CI's `durable-tidb` job sets it.
- **The build machine.** One cargo build at a time, the shared target directory, `-j 6` (Task 0's measurements use `CARGO_BUILD_JOBS=4 CARGO_INCREMENTAL=0` for comparability with the spike), lld; the TiDB playground is stopped before a build.
- **Commit areas:** `durable`, `api`, `import`, `ci`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **`durable` is a default feature of `operon`**, and `durable-mysql` (the TiDB backend) is a separate default feature | `operon dev` should have durable execution with no flags; a slim build can drop either | +8–12 MB and +60–90 s cold build in default builds (estimate; Task 0 measures) |
| 2 | **The default namespace only.** D1 serves one tenant, the `default` namespace; the store is `<data>/durable/default.db` or database `loam_durable_default` | Multi-tenancy needs the unified auth plan (D142) | None in D1: the per-tenant layout is already the naming scheme |
| 3 | **Port 8001** for the durable listener, `--durable-listen` to change it, `--no-durable` to disable it | Resonate's SDK default and §01's reservation (D138) | A conflict with a locally running standalone Resonate: the startup error names both flags |
| 4 | **The engine and port differentials run in the fork's CI, not in Loam's** | Embedding changes no engine code (D144) | A fork CI outage would hide an engine regression at a revision bump; Task 1 requires a green fork CI run for the pinned revision |
| 5 | **Loam's workflows use the Rust SDK over `InProcNetwork`**; if Task 6 finds a blocker (for example, the SDK assuming SSE framing that `worker_inproc` cannot reproduce), the fallback is the SDK's `HttpNetwork` against `http://127.0.0.1:8001`, recorded as a ruling | D141 | The fallback needs the listener on; `--no-durable` would then also disable operations |
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
5. **Linearizability of the embedded server.** Tests: Task 5 (porcupine against `operon dev`, SQLite and TiDB).
6. **Schedule dedup.** New files are imported once, changed files again, and duplicate registrations answer 409. Tests: Task 9.

## File structure

```
Cargo.toml                                   # + fork git deps (rev-pinned), parquet; member operon-durable
deny.toml                                    # allow-git for dina-kar/resonate; ignore RUSTSEC-2023-0071 with rationale
NOTICE                                       # + Resonate attribution (Apache-2.0)
crates/operon-durable/                       # new
  Cargo.toml                                 # features: mysql
  src/{lib.rs,config.rs,embed.rs,registry.rs,listen.rs,inproc.rs,runtime.rs,ops.rs,ids.rs,error.rs}
  src/import/{mod.rs,plan.rs,parquet.rs,ndjson.rs,slice.rs,schedule.rs}
  tests/{embed.rs,tidb.rs,inproc.rs,ops.rs,import.rs,import_crash.rs,schedule.rs}
crates/operon/
  Cargo.toml                                 # features durable, durable-mysql (default on)
  src/{server.rs,main.rs}                    # --durable-* flags, start/stop order
  src/api/{mod.rs,operations.rs,import.rs}   # operations and import routes
scripts/durable/{conformance.sh,examples.sh,porc-503.sh}
.github/workflows/ci.yml                     # + jobs durable (path-filtered), durable-tidb (nightly), durable-examples (nightly)
docs/plans/d1-dependency-spike.md            # Task 0
docs/plans/d1-exit-report.md                 # Task 10
docs/design/21-durable-execution.md  docs/design/13-decision-log.md  CHANGELOG.md  docs/plans/README.md
```

### Task 0: Reconcile and measure in the workspace

**Files:** read `crates/operon/src/{server.rs,main.rs,api/mod.rs}`, `crates/operon-query/src/{write.rs,flight_ingest.rs}`, `crates/operon-store/src/` as merged on `main`. Write `docs/plans/d1-dependency-spike.md` and fill this plan's "Rulings made during execution".

**Consumes** (each checked against the merged code; every difference is listed with its resolution):

```rust
// operon-query (M1.2)
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
1. Add the fork dependencies to a scratch branch of the workspace (never pushed) with `operon` depending on them. Record: the build time of `cargo build --release -p operon` with and without `--no-default-features --features <all but durable>`, at `CARGO_BUILD_JOBS=4 CARGO_INCREMENTAL=0`; the binary size, stripped and unstripped; the semver-incompatible duplicates (`cargo tree -d`); `cargo deny check` with the new `deny.toml`. The spike's stand-alone numbers are in §21 §11.4.
2. Confirm that one lockfile unifies: sqlx is 0.8.6, no second `libsqlite3-sys`, no `openssl-sys` in `cargo tree -e normal -i openssl-sys`, and no conflict with R1's `tikv-client` tree if R1 Task 0 has merged.
3. Confirm the Rust SDK builds with `reqwest` `default-features = false` and that `ResonateConfig.network` accepts a custom `Network` (compile a stub).
4. Confirm port 8001 is free in every documented configuration (`docs/`, `crates/operon/src/main.rs` defaults).
5. Confirm `promise.search` with a tag filter works on SQLite and TiDB (the progress rule, §21 §6.4), with an in-process call against the embedded server.
6. Record which Python and TypeScript SDK versions pass against server 0.10.1. Expected: the monorepo's Python 0.8.1 and the TypeScript SDK at the pinned revision; PyPI 0.7.x fails (§21 §4).

**Produces:** the spike doc and a filled "Rulings made during execution" table (no empty rows).

**Commit:** `docs: record the D1 dependency measurements`.

### Task 1: The fork and the pinned dependencies

**Files:** the fork (`dina-kar/resonate`, branch `loam/0.10.1`), `Cargo.toml`, `deny.toml`, `NOTICE`.

**Semantics:**
1. Create `loam/0.10.1` from upstream `28dfd01` with these commits, in order:
   - `deps: clear RustSec advisories, drop OpenSSL, version internal path deps`: PR 0c, the spike's patch, plus a `gcp-idtoken` feature on `resonate-transport-http-push` that gates `google-cloud-auth`, on by default upstream and off in Loam;
   - `server-mysql: run on TiDB, classify retryable errors by errno`: PR 0a + PR 1, the research commit `cdc1cbd`;
   - `sdk-rs: reqwest without default TLS`.
2. The fork's CI (`server-core-ci.yml`) must be green at the pinned revision for `check`, the engine and port differentials (SQLite, MySQL, TiDB leg from PR 1) and porcupine (SQLite, TiDB). Record the run URL in the spike doc.
3. Open the upstream issues and PRs for 0c and 0a (one concern each) and link them in the spike doc. Merging is not required for D1.
4. `Cargo.toml` `[workspace.dependencies]`: each crate as `{ git = "https://github.com/dina-kar/resonate", rev = "<sha>" }`; `parquet = { version = "58", default-features = false, features = ["arrow", "async", "object_store", "snap", "zstd", "lz4"] }`.
5. `deny.toml`: `[sources] allow-git = ["https://github.com/dina-kar/resonate"]`; `[advisories] ignore = [{ id = "RUSTSEC-2023-0071", reason = "rsa via sqlx-mysql: used only for RSA password exchange on non-TLS MySQL connections; Loam connects to TiDB over TLS or the cluster network with mysql_native_password (§21 §11.2)" }]`.

**Tests:** `cargo deny check` passes; `cargo tree -e normal -i openssl-sys` prints nothing.

**Commit:** `durable: pin the Resonate fork and extend the license policy`.

### Task 2: `operon-durable`: the embedded server

**Files:** `crates/operon-durable/{Cargo.toml,src/{lib.rs,config.rs,embed.rs,registry.rs,listen.rs,error.rs}}`, `crates/operon-durable/tests/embed.rs`.

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
   - `workers.transport_http_push.enabled = <push>`, `debug = <debug>`;
   - then each override, in order. An override of `abort_on_panic`, `bind` or `servers.active` is refused (`Config`).
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

### Task 3: The `operon` binary: flags, feature, lifecycle

**Files:** `crates/operon/{Cargo.toml,src/server.rs,src/main.rs}`, tests in `crates/operon/src/main.rs` (flag parsing) and `crates/operon/tests/durable.rs`.

**Semantics:**
1. Features: `durable = ["dep:operon-durable"]`, `durable-mysql = ["durable", "operon-durable/mysql"]`, both in `default`.
2. Flags on `dev`, `standalone` and `cluster`: `--durable-listen <addr>` (default `127.0.0.1:8001`), `--no-durable`, `--durable-store <sqlite:<path>|mysql://…>` (default on `dev` and `standalone`: `sqlite:<data_dir>/durable/default.db`; `cluster` has no default and refuses `sqlite:` with `the sqlite durable store is single-node; use --durable-store mysql://…`), `--durable-push`, `--durable-set key=value` (repeatable), and hidden `--durable-debug`. Without the feature, any `--durable-*` flag logs `this build has no durable execution (the durable feature is off)`, like the Qdrant flags.
3. `ServerConfig.durable: Option<DurableConfig>`.
4. Start order: metastore → durable → native API and the other listeners. Stop order: native API and the other listeners → durable → workers → metastore. A durable start failure is fatal, with its message.
5. `operon dev` prints the durable URL in its startup banner, next to the other listeners.

**Tests:** flag parsing (defaults, `--no-durable`, `cluster` refuses `sqlite:`, overrides); `dev_serves_durable_on_8001_style_port` (an ephemeral port through `--durable-listen`, `GET /ready` → 200); `stop_order_drains_durable_before_meta`.

**Commit:** `durable: serve the durable listener from operon dev, standalone and cluster`.

### Task 4: The TiDB backend

**Files:** `crates/operon-durable/src/config.rs` (MySQL URL and TLS), `crates/operon-durable/tests/tidb.rs`, `crates/operon/src/main.rs` (a `durable migrate` subcommand), `scripts/durable/tidb.sh`.

**Semantics:**
1. `mysql://user:pass@host:port/db?ssl-mode=required|disabled` maps to `servers.server_mysql.url`. `MysqlTls::Required` is the default except for 127.0.0.1 and `localhost`.
2. `operon durable migrate --durable-store mysql://…` runs Resonate's migrations once (it starts the server plugin with `migrate = true` and stops). `operon standalone|cluster` with a TiDB store never migrates. On a schema behind the binary it fails with Resonate's message, plus `run 'operon durable migrate' first`.
3. `scripts/durable/tidb.sh up|down` starts R1's playground with TiDB (or the fallback command in Tech Stack), creates the database `loam_durable_default` and prints `OPERON_TEST_TIDB`.
4. The fork's pessimistic pin is verified at connect time: `SELECT @@tidb_txn_mode` → `pessimistic` on every pooled connection (a debug assertion in tests).

**Tests** (`tests/tidb.rs`, skip without `OPERON_TEST_TIDB`): `migrate_then_serve`; `unmigrated_schema_names_the_command`; `state_survives_restart_on_tidb`; `two_servers_one_database_are_linearizable_smoke` (two `DurableServer`s on one database, concurrent `promise.create` and `settle` on 4 ids, and every final state is one the protocol allows); `optimistic_cluster_still_pessimistic_sessions` (`SET GLOBAL tidb_txn_mode='optimistic'` on a throwaway playground, then a session still reports `pessimistic`).

**Commit:** `durable: add the TiDB backend through the MySQL plugin`.

### Task 5: The conformance run

**Files:** `scripts/durable/{conformance.sh,porc-503.sh}`, `.github/workflows/ci.yml`.

**Semantics:**
1. `conformance.sh --store sqlite|tidb [--clients N --ops M --seed S]`:
   - builds `operon` (release) and, from the pinned fork, `conctrace` (`cargo build --release --example conctrace` in `impl/server/core`, target dir `~/.cache/cargo-target/durable-fork`);
   - starts `operon dev --durable-debug --durable-listen 127.0.0.1:<free> [--durable-store …]` on a fresh store;
   - runs `conctrace --url http://127.0.0.1:<port>/ --out <dir>/trace --clients N --ops M --seed S`;
   - runs `go run ./cmd/conccheck -partition=false < trace.history` in the fork's `spec/valid/porc`;
   - exits non-zero unless the output says LINEARIZABLE, and prints the status tally (2xx, 4xx, 5xx).
2. `porc-503.sh` rewrites a history's 503 responses as not applied (the research rule for F3) and reports how many there were. The TiDB leg runs the checker on the rewritten history until upstream PR 0b lands, and the job summary shows the count.
3. CI: job `durable` (PRs touching `crates/operon-durable/**`, `crates/operon/src/api/{operations,import}.rs`, `scripts/durable/**`): `cargo test -p operon-durable`, then `conformance.sh --store sqlite --clients 8 --ops 600`. Job `durable-tidb` (nightly): TiDB up, `cargo test -p operon-durable --test tidb`, `conformance.sh --store tidb` at 8 × 600 and at 16 × 400 with seeds 11, 12 and 13.

**Tests:** the script itself, plus `conformance.sh --self-test`: it must fail on a doctored history (one settle answered twice with different values).

**Commit:** `ci: run Resonate's linearizability check against the embedded server`.

### Task 6: The in-process network and Loam's durable runtime

**Files:** `crates/operon-durable/src/{inproc.rs,runtime.rs}`, `crates/operon-durable/tests/inproc.rs`.

**Produces:**

```rust
pub static PLUGIN: WorkerPlugin;            // id "worker_inproc" (crate name outside resonate-* keeps its whole name: set explicitly), scheme "inproc"
pub struct InProcNetwork;                   // implements resonate::Network over DurableServer::process
pub struct DurableRuntime;                  // the SDK instance for group "loam"
impl DurableRuntime {
    pub async fn start(server: &DurableServer, node_id: &str) -> Result<Self, DurableError>;
    pub fn sdk(&self) -> &resonate::Resonate;                    // register Loam functions here
    pub async fn stop(self);
}
```

**Semantics:**
1. Addresses: `unicast = inproc://uni@loam/<node_id>`, `anycast = inproc://any@loam/<node_id>`, `group = loam`, `pid = <node_id>`.
2. `worker_inproc` keeps `group → [callbacks]`. `process(address, msg)` parses the address like `PollAddress` (unicast: that node; anycast: prefer the named node, else any local subscriber). It serializes `msg` to exactly the JSON the poll transport writes in an SSE `data:` frame and invokes the callback on a spawned task. With no subscriber it returns `Unavailable`, and the task retry timeout recovers.
3. `InProcNetwork::send(req)` parses the request, calls `server.process`, and returns the response JSON. Transport errors map to the SDK's retryable error.
4. `DurableRuntime::start` builds `resonate::Resonate::new(ResonateConfig { network: Some(Arc::new(InProcNetwork…)), group: Some("loam"), ttl: Some(60_000), .. })` and starts it.
5. If the SDK cannot be driven this way, apply Ruling 5 and record it.

**Tests** (`tests/inproc.rs`):
- `two_step_function_runs` (step 1 then step 2, the result returned);
- `crash_between_steps_resumes_without_rerunning_step_1` (a counter per step; the runtime is stopped after step 1 settles and started again → step 1 ran once, step 2 once);
- `failed_branch_alone_retries` (four branches, one fails once → the other three ran once);
- `sleep_survives_restart` (`ctx.sleep(2 s)` across a runtime restart);
- `no_http_is_used` (the listener disabled through a test hook; the functions still run).

**Commit:** `durable: run Loam's own durable functions in process`.

### Task 7: The operations API

**Files:** `crates/operon-durable/src/{ops.rs,ids.rs}`, `crates/operon/src/api/operations.rs`, `crates/operon-durable/tests/ops.rs`.

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
2. **Submit.** The root promise is `promise.create { id, param: {kind, namespace, params, params_hash}, tags: {"loam:op": id, "loam:kind": kind, "loam:ns": ns, "resonate:target": "inproc://any@loam"}, timeoutAt: now + 7 d }`, and the workflow for `kind` starts on it through the SDK. With an `Idempotency-Key`, an existing root with the same `params_hash` returns `(id, false)`; a different hash is `OpsError::IdempotencyKeyReused` → 409 `idempotency_key_reused`.
3. **State** maps as in §21 §6.4. **Progress** comes from the kind's progress function (Task 8: tagged file branches), cached for 2 s.
4. **Cancel** settles the root `rejected_canceled`. Workflows check it between steps through a helper, `ops::check_canceled(ctx, id)`.
5. **Retention:** a worker task `durable-op-retention` (priority `Maintenance`) deletes finished operations older than 7 days (Ruling 8) through a backend-level delete of the operation's origin. SQLite and TiDB delete from the promise, task and callback tables where the id starts with `<origin>` or equals it. The engine exposes no such operation, so this is a direct SQL statement kept in `ops.rs` with a comment naming upstream PR 5.

**Tests** (`tests/ops.rs`): `submit_returns_202_location` (through the route); `same_key_same_operation`; `same_key_other_params_conflicts`; `state_mapping_covers_every_promise_state`; `cancel_stops_at_next_step`; `list_filters_by_state_and_pages`; `retention_prunes_finished_after_7_days` (debug clock); `unknown_id_is_404`.

**Commit:** `api: add the durable operations API`.

### Task 8: Bulk import from object storage

**Files:** `crates/operon-durable/src/import/{mod.rs,plan.rs,parquet.rs,ndjson.rs,slice.rs}`, `crates/operon/src/api/import.rs`, `crates/operon-durable/tests/{import.rs,import_crash.rs}`.

**Produces:** `POST /v1/namespaces/{ns}/collections/{c}/import` with the body of §21 §7.2, answering 202 with `Location` (and `200` for an idempotent repeat), and the workflow `collection.import`.

**Semantics:**
1. **Plan step** (`ctx.run("plan")`): list `source` with `operon-store` under the namespace's storage credentials. Filter by `pattern` (glob on the key's suffix after the prefix). Sort by key. Record `[(key, size, etag)]` and the totals. With zero files the operation succeeds at once with `files_total = 0`. A source outside the allowed schemes (`s3`, `gs`, `az`, `file` only on `dev`) → 400 before submit.
2. **File branches**: one child per file, tagged `loam:op=<id>`, `loam:kind=file`, `loam:file=<index>`, with at most `max_parallel_files` in flight (a semaphore inside the workflow). A branch opens the object with `If-Match: <etag>`; a mismatch fails the branch with `file_changed`.
3. **Slices**: Parquet → one row group per slice (`ParquetRecordBatchStreamBuilder` over the `object_store` reader, Ruling 6); NDJSON → 64 MiB ranges extended to the next `\n` (the first slice starts at 0, and each later slice skips its partial first line). Each slice is `ctx.run("slice-<n>")` → `{rows, bytes, token}`.
4. **Mapping and writing**: `CollectionBatchMapper::new(schema_of_slice, collection_schema, id_type)`, then `map(batch)`; rows without an id get Ruling 7's id; `CollectionService::write(ns, c, ops, WriteOptions::default())` in chunks of `put_chunk_rows`. A `ServiceError` for backpressure (the 429 of D86) or unavailability is retried inside the step with jittered backoff up to 5 minutes; a row error follows `on_error`.
5. **Fan-in**: `{files_total, files_done, files_failed, rows_written, bytes_read, token}`, where `token` merges every slice's consistency token (D76).
6. **Progress** for `GET /v1/operations/{id}`: `files_done` counts resolved file branches (a tag search), `rows_written` and `bytes_read` sum the settled slice values of running branches (a search by `loam:op`), and `files_total` comes from the plan value.
7. **Limits**: at most 100,000 files per operation (400 above that, with a hint to use several prefixes); `max_concurrent_operations` per namespace (Ruling 9) → 429 `too_many_operations`.

**Tests** (`tests/import.rs`, `tests/import_crash.rs`; sources are a `file://` bucket and the in-memory store with fault injection):
- `imports_parquet_and_ndjson` (row counts, a search finds the documents, and the result token makes a strong read see them);
- `rows_without_id_get_deterministic_ids`;
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

**Files:** `crates/operon-durable/src/import/schedule.rs`, `crates/operon/src/api/import.rs`, `crates/operon-durable/tests/schedule.rs`.

**Semantics:**
1. **Routes:**
   - `POST /v1/namespaces/{ns}/collections/{c}/import-schedules {name, cron, source, format, pattern?, mapping?}` → 201, or 409 when the name exists;
   - `GET …/import-schedules` → a list with each schedule's last run: time, files started, files already imported, and failures;
   - `DELETE …/import-schedules/{name}` → 204.
2. **Schedule**: `schedule.create { id: "isched-<hex26(sha256(ns‖c‖name))>", cron, promiseId: "isched-<…>.{{.timestamp}}", promiseTimeout: 24 h, promiseParam: {…}, promiseTags: {"resonate:target": "inproc://any@loam", "loam:kind": "import.run"} }`. The cron syntax is Resonate's (5 fields); an invalid expression is Resonate's 400, passed through.
3. **A run** (`import.run`): list the source (as in the plan step), then for each file start a **root** durable call `impf-<hex26(sha256(schedule id ‖ key ‖ etag))>` running `import.file`. That is a file branch without a parent, tagged `loam:schedule=<id>`. Existing ids return their memoized results and are counted as `already_imported`. The run's value is `{started, already_imported}`. A run whose previous run is still pending returns `{skipped: "previous run still listing"}`.
4. `import.file` is Task 8's file branch, parametrized with the collection, format and mapping, so file semantics are identical.

**Tests** (`tests/schedule.rs`, debug clock, a cron of every minute driven by `debug.tick`): `new_files_import_once` (two ticks, three files, then one new file → 4 file calls in total); `changed_etag_imports_again`; `duplicate_name_is_409`; `delete_stops_future_runs`; `invalid_cron_is_400`; `schedule_survives_restart`.

**Commit:** `import: add scheduled incremental import`.

### Task 10: The D1 gates and the exit report

**Files:** `scripts/durable/examples.sh`, `.github/workflows/ci.yml` (job `durable-examples`), `docs/plans/d1-exit-report.md`.

**Semantics:**
1. `examples.sh` (nightly) clones the pinned example repositories: `example-hello-world-py`, `example-fan-out-fan-in-py`, `example-human-in-the-loop-py`, `example-schedule-py`, `example-money-transfer-py`, `example-hello-world-ts` and `example-fan-out-fan-in-ts`, at recorded commits. It installs the SDK versions from Task 0 (Python: the fork's `impl/sdk/py` in editable mode; TypeScript: the fork's `impl/sdk/ts` or the npm version Task 0 recorded). Against `operon dev` it runs each example and checks its expected lines. For human-in-the-loop it runs `kill -9` on `operon` after the workflow blocks, restarts `operon` and resolves, then expects the root to be resolved.
2. **The import crash gate**: an import of 200 files (about 2 GB generated Parquet), with `operon dev` killed with `kill -9` at 10 random points and restarted each time. The final count is exact, no file is read more than once after it finished, and the total time is recorded.
3. **Exit report**:
   - the conformance results (SQLite and TiDB, the 503 counts);
   - the example suite;
   - the crash gates;
   - import throughput against Flight `DoPut` on the same data, measured on a quiet machine;
   - the binary and build-time delta (Task 0 against final);
   - the status of upstream PRs 0a, 0b, 0c and 1;
   - the rulings made during execution.

**Tests:** the gates themselves, plus `examples.sh --self-test` (an intentionally wrong expected line fails).

**Commit:** `ci: run the Resonate example suite against operon nightly`; `docs: add the D1 exit report`.

### Task 11: Documentation

**Files:** `docs/design/21-durable-execution.md` (as-built notes: rulings made during execution, the measured cost, the SDK versions), `docs/design/13-decision-log.md` (record new decisions; update D138–D147 statuses when the owner confirms them), `docs/design/01-architecture.md` §3 (the Resonate row: 127.0.0.1:8001, D1), `docs/design/10-operations.md` §2 (the `durable` listener block), `CHANGELOG.md`, `docs/plans/README.md` (D1 status), `crates/operon-durable/README.md` (dev setup: `operon dev`, a Python SDK example, the operations API, an import walkthrough).

**Commit:** `docs: record D1 as built`.

## PR grouping

One PR per group, stacked in order; each PR builds and passes CI on its own.

| PR | Tasks | Title |
|---|---|---|
| A | 0 | D1 (1/10): dependency measurements |
| B | 1 | D1 (2/10): the Resonate fork and the license policy |
| C | 2 | D1 (3/10): the embedded server on SQLite |
| D | 3 | D1 (4/10): the durable listener in `operon` |
| E | 4 | D1 (5/10): the TiDB backend |
| F | 5 | D1 (6/10): the linearizability run against `operon` |
| G | 6 | D1 (7/10): Loam's in-process durable runtime |
| H | 7 | D1 (8/10): the operations API |
| I | 8, 9 | D1 (9/10): bulk import and scheduled import |
| J | 10, 11 | D1 (10/10): gates, exit report, docs |

## Rulings made during execution

| # | Ruling | Why | Tasks |
|---|---|---|---|

## Self-review

| Check | Result |
|---|---|
| The embedded server behind the feature, public API only, no global side effects (D138) | Tasks 2, 3 |
| SQLite and TiDB backends (D139) | Tasks 2, 4 |
| The loopback listener on 127.0.0.1:8001, refusal of other addresses, push off (D138, D141) | Global Constraints, Tasks 2, 3 |
| The fork at a pinned revision, `allow-git`, the advisory ignore with a rationale (D140) | Task 1 |
| The conformance run of Resonate's harness against embedded Loam (D144) | Task 5 |
| Loam's own workflows on the Rust SDK in process (D141) | Task 6 |
| A submit-poll long-operation API for one real operation: bulk import (D145, D146) | Tasks 7, 8 |
| Fan-out with only failed branches re-run (pattern b) | Task 8 (`failed_branch_alone_retries` in Task 6, `idempotent_resubmit_resumes`) |
| One schedule: scheduled incremental import (D145) | Task 9 |
| Idempotency keys (pattern g, D146) | Tasks 7, 8 |
| No M1 code rewritten (D143) | Only routes and the listener are registered in `operon`; import uses M1.2's mapper and write path as they are |
| Other patterns on the roadmap (D143) | `docs/plans/README.md` track D rows; §21 §6, §14 |
| Review Focus → tests | 1: T8/T10 · 2: T7 · 3: T2/T3 · 4: T2 · 5: T5 · 6: T9 |
| Carried in | PR 0a/1 (research), PR 0c (spike) |
