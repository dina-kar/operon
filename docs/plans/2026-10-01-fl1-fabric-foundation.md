# FL1 — Event Fabric Foundation (Iggy, Fluss, the Envelope and the Bridges) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, ports, headers, constants), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: Planned** (2026-10-01). Track FL (§32 D351), beside M, R, D and J, interleaved on the one-build machine (D127). Branches `fl1-t<N>`, stacked; PRs target `main`. FL1 adds a new Cargo workspace `fabric/`, a dev stack under `deploy/fabric/`, two Iggy connector plugins (developed as upstream PRs) and one small change in the engine workspace (Task 2: a feature split in `loams-cloudevents`). It changes no engine code path.

**Goal:** Stand up the Event Fabric of [§32](../design/32-loams-flow-fabric-house.md) §5 end to end on one machine:
- the `fabric/` workspace and the `loams-fabric` binary with the `ingest` role;
- the CloudEvents envelope on Iggy messages and Fluss rows (D334), reusing `loams-cloudevents`;
- a dev stack with Apache Iggy 0.9.0, Apache Fluss 1.0.0 (with ZooKeeper), Lakekeeper 0.13.6, RustFS and a Flink session cluster running Fluss's tiering job;
- namespace provisioning across Iggy, Fluss, Lakekeeper and the bucket;
- `loams-fabric ingest`: CloudEvents over HTTP (binary, structured, batched) and gRPC into Iggy, deduplicated by `source` + `id` with a ledger in a Fluss PK table;
- the Iggy connector plugins `fluss_sink` (Iggy → Fluss) and `loams_sink` (Iggy → Loams streams and collections) (D336);
- one PK-table use case and one Log-table use case tiered to Iceberg and read back by an external reader;
- the FL1 gate: an event acknowledged by `ingest` is in Iggy, in the Fluss table and, after tiering, in Iceberg, through kills of every component, with no duplicate inside the dedup window.

**Architecture:**
- **`fabric/`** is a Cargo workspace of its own (own `Cargo.lock`, `deny.toml`, lints copied from the root), building into the shared target directory set by `~/Documents/.cargo/config.toml`. It depends on the engine workspace only through a path dependency on `crates/loams-cloudevents` with `default-features = false` (Task 2), so no engine crate, Lance, DataFusion or arrow 58 enters its graph.
- **`loams-fabric`** is one binary with roles (`ingest` in FL1; `house` in FL2; `flow` in CN1/FL3), selected by subcommand, all loopback-only until the unified auth plan (D111).
- **Iggy and Fluss run unmodified** from pinned images. Loams's code reaches Iggy through the `iggy` SDK 0.11.0 and Fluss through `fluss-rs` 1.0.0.
- **Plugins** `fluss_sink` and `loams_sink` are written in Iggy's connectors workspace (`core/connectors/sinks/`), on the fork `dina-kar/iggy` (a PR source only, like `ostrium-labs/resonate`, D140), and posted upstream only with the owner's go-ahead (§23 §2.2). CI builds them from a pinned revision of that branch.

**Tech Stack:**
- Rust 1.97.1, edition 2024. New dependencies (Task 0 verifies versions, licences and that they build together): `iggy` 0.11.0 (Apache-2.0), `fluss-rs` 1.0.0 (Apache-2.0; arrow 59), `arrow` 59 (the version `fluss-rs` uses), `connectrpc` 0.9.1 and `connectrpc-build` 0.9, `buffa` 0.9.2, `axum` 0.8, `tokio`, `reqwest` 0.12, `sha2`, `thiserror`, `tracing`, `proptest` 1; dev: `testcontainers`-free (the compose stack is the fixture), `tokio-test`.
- Services (images pinned by digest in Task 3): `apache/iggy:0.9.0`, `apache/fluss:1.0.0` (verify the image name), `zookeeper:3.9`, `quay.io/lakekeeper/catalog:v0.13.6`, `postgres:17` (Lakekeeper's catalog database), `rustfs/rustfs:1.0.x` (D61), `flink:1.20` with `fluss-flink-tiering-1.0.0.jar` and the Iceberg bundle (verify the Flink version the jar targets).
- Readers for the tiering check: Python 3.13 with `uv`, `pyiceberg` and `duckdb` (both Apache-2.0/MIT), in `scripts/fabric/`.

**Spec:**
- [`docs/design/32-loams-flow-fabric-house.md`](../design/32-loams-flow-fabric-house.md): §5 (all), §6.3, §7.3, §12, §13, §14.
- [`docs/design/02-stream-engine.md`](../design/02-stream-engine.md) §7.4 (D270: the CloudEvents mapping, validation, dedup protocol and answers that the Fabric mirrors).
- [`docs/design/13-decision-log.md`](../design/13-decision-log.md): D11, D61, D111, D127, D186, D189, D270; D330–D351 once merged.
- As built: `crates/loams-cloudevents` (`CloudEvent`, `Attr`, `dedup_key`, `http::{parse_binary, write_binary}`, `json`, `record`), PRs #170 and #171 (`ProduceCloudEvents` over HTTP and gRPC), `crates/loams-stream-grpc`.

## Global Constraints

- **The engine is untouched** except Task 2's feature split. `cargo build` in the root workspace must not change its lockfile because of FL1, except Task 2's `[features]` lines.
- **No forks of Iggy or Fluss in production.** Images and crates are released versions. Plugin code lives on `dina-kar/iggy` branches only until merged upstream.
- **Loopback only (D111).** `loams-fabric ingest --listen` accepts only loopback addresses; any other fails startup with `fabric ingest listen on <addr>: only loopback addresses are served until the unified auth plan (D111)`. The compose stack binds every published port to `127.0.0.1`.
- **Ports.** Other stacks run on this machine. The Fabric stack uses: Iggy TCP `127.0.0.1:18090`, HTTP `127.0.0.1:13000`, QUIC `127.0.0.1:18080`, WebSocket `127.0.0.1:18092`; Fluss coordinator `127.0.0.1:19123`, tablet server `127.0.0.1:19124`; ZooKeeper `127.0.0.1:12181`; Lakekeeper `127.0.0.1:18181`; RustFS S3 `127.0.0.1:19000`, console `127.0.0.1:19001`; Flink JobManager UI `127.0.0.1:18081`; `loams-fabric ingest` `127.0.0.1:7730` (HTTP and gRPC on one port, as Live's 7710 and jobs' 7720).
- **Memory.** The stack must fit in 8 GiB RSS (estimate; Task 0 measures). It is stopped before any cargo build and started after it, as the TiKV playground is (R1 Global Constraints).
- **Builds.** One cargo build at a time, the shared target, `-j 6`, lld. Never in `/tmp`. Stop and report if `/home` has under 8 GB free.
- **Tests skip without the stack.** Every test that needs a service calls `loams_fabric_testing::stack()`, which returns `None` and prints `skipped: <test> needs LOAMS_FABRIC_STACK` when the variable is unset; CI's `fabric` job sets it.
- **Names (owner rulings, 2026-10-01).** Crates here are `loams-*` (crates.io, PyPI when published) and npm packages `@loams/*`, Go modules `loams.dev/...`; Loams-defined CloudEvents types use `io.loams.dev.<domain>.<name>.v1`.
- **Commit areas:** `fabric`, `iggy`, `fluss`, `deploy`, `ci`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **A separate Cargo workspace `fabric/`** with its own lockfile, not members of the root workspace | `fluss-rs` 1.0 is on arrow 59 and FL2's libchdb is 180 MB; the engine is on arrow 58 (Lance 12); one workspace would force duplicate arrow versions into the engine's `cargo deny` and rebuild graphs | Two lockfiles to keep current; Dependabot gets a second entry |
| 2 | **Iggy names**: one Iggy stream per Loams namespace, `ns-<namespace_id>` (decimal); topics are the Fabric topic names (`[a-z0-9][a-z0-9._-]{0,62}`); default 4 partitions; `durability = persisted` and `consumer_offset_durability = persisted` for every Fabric-created topic | Namespaces map 1:1 to Iggy's tenant abstraction; `persisted` survives a full-cluster power loss | More fsyncs than `replicated`; Task 11 measures |
| 3 | **Iggy message id** = the first 16 bytes of `CloudEvent::dedup_key()` as a big-endian `u128` | One identity for an event across Loams streams (D270) and Iggy, so re-delivery is recognisable everywhere | None known; collisions at 2⁻¹²⁸ |
| 4 | **Fluss names**: database `ns_<namespace_id>`; tables `[a-z][a-z0-9_]{0,62}`; system tables in database `_fabric` | Fluss identifiers (verify allowed characters in Task 0) | A rename if Fluss refuses the leading underscore; Task 0 decides `fabric_sys` instead |
| 5 | **Dedup mirrors D270**: claim (2 min), append, complete (window 1 h default, 24 h max, `--dedup-window`), with the ledger in the Fluss PK table `_fabric.ce_dedup` (LastRow merge engine: a claim is updated in place when it completes) | The same semantics and answers as Loams streams, so a client can move between them | Fluss has no compare-and-set: two ingest processes claiming the same key could both append. **FL1 runs one ingest owner per (topic, partition)**; Ruling 6 |
| 6 | **One ingest owner per (topic, partition)** in FL1, by a static assignment in `loams-fabric.toml` (`ingest.owner = "all"` on a single node); any ingest node accepts a request and forwards it to the owner over loopback HTTP | Makes the ledger single-writer per key without CAS | Not HA; FL3 replaces the static map with metastore leases (the §09 §6 lease model over the network API) |
| 7 | **Fluss tiering runs as Fluss's own Flink job** in a Flink 1.20 session cluster in dev (§32 D333, Q332) | Owner-accepted v1 path; nothing to build | A JVM in dev; Task 0 measures its memory |
| 8 | **Lakekeeper with its own Postgres** in dev, one warehouse `loams` on RustFS bucket `loams-fabric`, path prefix `ns/<id>/fabric/iceberg/` | Fluss's Iceberg tiering speaks the Iceberg REST catalog; Lakekeeper is M4's catalog anyway (D6) | Q2 / Q335 decide production |
| 9 | **The PK-table event layout**: a route-declared schema's columns plus `_ce_source STRING, _ce_id STRING, _ce_type STRING, _ce_time TIMESTAMP_LTZ(3), _ce_subject STRING NULL, _ce_dataschema STRING NULL, _ce_ext MAP<STRING,STRING> NULL`; without a schema the data goes to `_ce_data BYTES` | §32 §5.4; queryable attributes without JSON parsing | Wide rows for tiny events; acceptable |
| 10 | **`ce_loamsop`** (`upsert` default, `delete`) selects the Fluss operation on PK tables in `fluss_sink` | CDC and tombstones without a second topic | None |

## Carried in

Nothing from earlier plans. The CloudEvents codec, validation and dedup protocol come from D270 as built in `crates/loams-cloudevents` and PRs #170/#171; if those PRs have not merged when Task 2 starts, Task 2 works on `main`'s `loams-cloudevents` (which already has the codec and `dedup_key`) and Task 10's stream target waits for #171.

## Review Focus

1. **Dedup is exactly D270's**: a retried request inside the window never appends twice; the answer to a retry names the original partition and offset; a `CommitUnknown`-like outcome (Iggy send timed out) keeps the claim. Tests: Task 6 (`retry_within_window_answers_duplicate`, `timed_out_send_keeps_claim`, `claim_lapses_after_two_minutes`).
2. **No acknowledged event is lost across kills** of Iggy, Fluss, the tiering job, the connectors runtime and `ingest`. Test: Task 11 (`fabric_kill_matrix`).
3. **The envelope round-trips byte for byte** through Iggy messages and Fluss rows (attribute names, values, order; extension types where `loams_ce_types` is present). Tests: Task 2.
4. **Loopback refusal and secret hygiene** (no credentials in logs or rendered configs). Tests: Task 5 (`non_loopback_is_refused`), Task 4 (`provision_never_logs_secrets`).

## File structure

```
crates/loams-cloudevents/Cargo.toml              # Task 2: feature "log" (default) around record.rs
crates/loams-cloudevents/src/{lib.rs,record.rs}
fabric/Cargo.toml  fabric/Cargo.lock  fabric/deny.toml  fabric/rust-toolchain.toml (symlink to ../)
fabric/crates/loams-fabric-envelope/src/{lib.rs,iggy.rs,fluss.rs,error.rs}
fabric/crates/loams-fabric-envelope/tests/{iggy_roundtrip.rs,fluss_roundtrip.rs}
fabric/crates/loams-fabric-ingest/src/{lib.rs,config.rs,http.rs,grpc.rs,ledger.rs,owner.rs,provision.rs,errors.rs}
fabric/crates/loams-fabric-ingest/tests/{http.rs,grpc.rs,ledger.rs,provision.rs}
fabric/crates/loams-fabric-testing/src/lib.rs      # stack() and fixtures
fabric/crates/loams-fabric/src/main.rs             # loams-fabric {ingest|provision|tables} …
fabric/crates/loams-fabric/tests/e2e.rs            # Task 9 and Task 11
fabric/proto/loams/fabric/v1/{fabric.proto,tables.proto}
deploy/fabric/{compose.yaml,iggy.toml,fluss/server.yaml,lakekeeper.env,flink/conf.yaml,README.md}
scripts/fabric/{up.sh,down.sh,wait.sh,tiering.sh,read_iceberg.py,kill_matrix.sh}
.github/workflows/fabric.yml                      # path-filtered: fabric/**, deploy/fabric/**, crates/loams-cloudevents/**
(dina-kar/iggy) core/connectors/sinks/{fluss_sink,loams_sink}/{Cargo.toml,src/lib.rs,README.md,config.toml}
docs/plans/fl1-dependency-spike.md  docs/plans/fl1-exit-report.md  docs/guides/fabric/{index.md,ingest.md}
```

### Task 0: Reconcile and measure

**Files:** read `crates/loams-cloudevents/**`, PRs #170/#171 (state and API), `crates/loams-stream-grpc`, `deploy/tikv/` (the playground conventions). Write `docs/plans/fl1-dependency-spike.md` and fill this plan's "Rulings made during execution".

**Checks** (record each with the command and output):
- Latest releases of Iggy (server, `iggy` SDK, connectors runtime), Fluss (server image, `fluss-rs`, the tiering jar and the Flink versions it supports), Lakekeeper, RustFS; the image names and digests.
- That `iggy` 0.11.0, `fluss-rs` 1.0.0, `connectrpc` 0.9.1 and `axum` 0.8 build together in a throwaway crate under `fabric/` (deleted after), with `cargo deny check` against a copy of the root `deny.toml`; the duplicate list (`cargo tree -d`).
- Fluss: identifier rules (Ruling 4), custom table properties (`loams.*`), PK-table merge engines and their option names in 1.0 (`table.merge-engine`, `table.merge-engine.versioned.ver-column`), Iceberg tiering options (`table.datalake.enabled`, freshness), the Iceberg REST catalog config for Lakekeeper, and whether `fluss-rs` exposes: append and upsert/delete writers with acknowledgements carrying bucket offsets, a PK lookup, log scans from offsets, admin (databases, tables, properties). Each missing API is a row in the spike's gap table (it becomes a §32 §5.9 contribution or a workaround).
- Iggy: the 0.9 dedup semantics (Ruling 3 relies only on the message id being stored; record whether the server also dedupes by message id), message header limits (count, size), `persisted` durability's write latency on this machine, the connectors runtime's plugin ABI (dynamic `cdylib` load) and how a plugin built from a fork revision is loaded.
- RSS of the full stack at idle and under Task 11's load (`docker stats`), against the 8 GiB budget.
- The decision numbers D330–D359 as merged.

**Commit:** `docs: reconcile FL1 with main and record the Fabric dependency spike`.

### Task 1: The `fabric/` workspace and its CI job

**Files:** `fabric/Cargo.toml`, `fabric/deny.toml`, `fabric/crates/loams-fabric/{Cargo.toml,src/main.rs}`, `fabric/crates/loams-fabric-testing/{Cargo.toml,src/lib.rs}`, `.github/workflows/fabric.yml`, `README.md` (one line under "Repository layout").

**Produces:**

```rust
// loams-fabric-testing
pub struct Stack { pub iggy_tcp: SocketAddr, pub iggy_http: Url, pub fluss_bootstrap: String,
                   pub lakekeeper: Url, pub s3_endpoint: Url, pub flink: Url }
pub fn stack() -> Option<Stack>;                 // from LOAMS_FABRIC_STACK=<path to stack.toml>; None prints the skip line
pub fn unique_namespace() -> u64;                // random per test, in 1_000_000..u32::MAX
```

**Semantics:** workspace lints identical to the root's (`[workspace.lints]` copied, with a test that diffs them, `lints_match_root`); `rust-version = "1.97"`; licence Apache-2.0. `loams-fabric --version` prints the binary version and the pinned Iggy, Fluss and (from FL2) chDB versions from a `versions.rs` table. CI job `fabric` (path-filtered, see File structure): `cargo fmt --check`, `cargo clippy -D warnings`, `cargo deny check`, unit tests; a second step brings up the stack (Task 3) and runs the integration tests with `LOAMS_FABRIC_STACK` set.

**Tests:** `lints_match_root`; `version_lists_pins`; `stack_absent_skips` (with the variable unset, `stack()` is `None`).

**Commit:** `fabric: add the fabric workspace and its CI job`.

### Task 2: The envelope on Iggy and Fluss

**Files:** `crates/loams-cloudevents/{Cargo.toml,src/lib.rs}` (engine workspace), `fabric/crates/loams-fabric-envelope/**`.

**Engine change:** `loams-cloudevents` gains `[features] default = ["log"]; log = ["dep:loams-log"]`; `record.rs` and its re-exports are behind `log`. Every existing dependent keeps the default. A test in the engine workspace (`cloudevents_builds_without_log`, a `cargo check -p loams-cloudevents --no-default-features` step in CI's existing job) keeps it honest.

**Produces:**

```rust
// loams-fabric-envelope::iggy
pub struct IggyEvent { pub id: u128, pub headers: Vec<(String, Vec<u8>)>, pub payload: Bytes }
pub fn message_id(ev: &CloudEvent) -> u128;                       // Ruling 3
pub fn to_iggy(ev: &CloudEvent) -> Result<IggyEvent, EnvelopeError>;  // ce_<attr>, content-type, loams_ce_types (D270 rules)
pub fn from_iggy(id: u128, headers: &[(String, Vec<u8>)], payload: &[u8]) -> FromIggy;
pub enum FromIggy { Event(CloudEvent), Synthesized(CloudEvent) }   // D270 consume rule 3 for messages without valid ce_ headers:
                                                                   // id "{stream}-{topic}-{partition}-{offset}", source "/fabric/{ns}/{topic}/{partition}",
                                                                   // type "io.loams.dev.fabric.message.v1"
// loams-fabric-envelope::fluss
pub const CE_COLUMNS: &[(&str, FlussType)];                       // Ruling 9, in this order
pub fn event_schema(data: Option<&arrow_schema::Schema>) -> arrow_schema::Schema;
pub fn to_rows(events: &[CloudEvent], data: Option<&arrow_schema::Schema>) -> Result<RecordBatch, EnvelopeError>; // data decoded from JSON when a schema is given
pub fn from_rows(batch: &RecordBatch) -> Result<Vec<CloudEvent>, EnvelopeError>;
pub enum Op { Upsert, Delete }                                     // from ce_loamsop (Ruling 10)
```

**Semantics:** header values are the attribute's string form as UTF-8, in attribute order; Iggy header keys are kept byte for byte (verify Iggy's allowed key characters in Task 0; if `ce_` keys are not allowed, the spike records the substitution). `to_rows` with a schema decodes JSON data into the columns by name and fails the event (not the batch) with `EnvelopeError::DataMismatch { index, column }` on a type error, so the caller can dead-letter it.

**Tests:** `tests/iggy_roundtrip.rs`: `binary_structured_and_protobuf_events_roundtrip` (proptest over random attributes and data kinds, 256 cases), `extension_types_survive_with_loams_ce_types`, `message_id_is_dedup_key_prefix`, `messages_without_ce_headers_are_synthesized`. `tests/fluss_roundtrip.rs`: `schemaless_events_roundtrip`, `schema_columns_decode_json_data`, `bad_data_fails_one_event`, `ce_columns_order_is_fixed`.

**Commit:** `fabric: map CloudEvents onto Iggy messages and Fluss rows`.

### Task 3: The dev stack

**Files:** `deploy/fabric/**`, `scripts/fabric/{up.sh,down.sh,wait.sh}`, `fabric/stack.toml.example`.

**Semantics:**
- `compose.yaml` services: `zookeeper` (1 node), `fluss-coordinator`, `fluss-tablet` (1 node, remote storage `s3://loams-fabric/fluss/` on RustFS), `iggy` (single node, `cluster.enabled = false`, data on a named volume, `persisted` durability default from `iggy.toml`, root credentials from `.env` generated by `up.sh` with mode 0600), `rustfs` (buckets `loams-fabric` created by an init container), `lakekeeper-db` (Postgres 17), `lakekeeper` (warehouse `loams` bootstrapped by `up.sh` through its management API), `flink-jobmanager` and `flink-taskmanager` (1 slot) with the tiering jar and Iceberg bundle in `lib/`. Every image pinned by digest. Every published port on `127.0.0.1` at Global Constraints' numbers.
- `up.sh` starts the stack, waits for health (`wait.sh`: Iggy `GET /ping`, Fluss coordinator port, Lakekeeper `/health`, RustFS `/minio/health/live` equivalent (verify), Flink `/overview`), bootstraps the warehouse, starts the tiering job (`scripts/fabric/tiering.sh`, Flink REST `jars/upload` + `run` with `--datalake.format iceberg` and the Lakekeeper REST options), and writes `fabric/stack.toml` for `LOAMS_FABRIC_STACK`. Idempotent: a second `up.sh` is a no-op.
- `down.sh` stops it; `down.sh --wipe` removes volumes.

**Tests:** `scripts/fabric/up.sh && scripts/fabric/wait.sh` in CI; `fabric/crates/loams-fabric-testing/tests/stack.rs`: `stack_services_answer` (each endpoint healthy), `tiering_job_is_running` (Flink REST lists one RUNNING job named like `fluss-tiering*`).

**Commit:** `deploy: add the Fabric dev stack (Iggy, Fluss, Lakekeeper, RustFS, Flink tiering)`.

### Task 4: Namespace provisioning

**Files:** `fabric/crates/loams-fabric-ingest/src/provision.rs`, `fabric/crates/loams-fabric/src/main.rs` (`loams-fabric provision`), `fabric/crates/loams-fabric-ingest/tests/provision.rs`.

**Produces:**

```rust
pub struct NamespaceSpec { pub id: u64, pub name: String }
pub struct Provisioned { pub iggy_stream: String, pub iggy_user: String, pub fluss_database: String,
                         pub lakekeeper_namespace: String, pub bucket_prefix: String }
pub async fn provision(stack: &StackClients, ns: &NamespaceSpec) -> Result<Provisioned, ProvisionError>; // idempotent
pub async fn deprovision(stack: &StackClients, ns: &NamespaceSpec, mode: Deprovision) -> Result<(), ProvisionError>; // Deprovision::{Keep, Drop}
pub async fn create_topic(stack: &StackClients, ns: u64, topic: &TopicSpec) -> Result<(), ProvisionError>;
pub struct TopicSpec { pub name: String, pub partitions: u32 /* 4 */, pub message_expiry: Duration /* 72 h, Q346 */,
                       pub max_topic_size: Option<u64> }
```

**Semantics:** creates (if absent) the Iggy stream `ns-<id>`, an Iggy user `ns-<id>` with permissions on that stream only and a personal access token stored in a secret file (`deploy/fabric/secrets/ns-<id>.token`, 0600, gitignored; Dapr secret stores later, D189), the Fluss database `ns_<id>`, the Lakekeeper namespace `ns_<id>` in warehouse `loams`, and the bucket prefix marker `ns/<id>/fabric/.keep`. The `_fabric` system database and `_fabric.ce_dedup` (Task 6's schema) are created once by `provision --system`. Existing objects with different settings are reported (`ProvisionError::Mismatch { object, field }`), never changed silently.

**Tests:** `provision_is_idempotent`; `user_sees_only_its_stream` (the namespace token cannot read another namespace's stream: Iggy answers unauthorized); `mismatch_is_reported`; `deprovision_keep_leaves_data`; `provision_never_logs_secrets` (captures `tracing` output and asserts the token never appears).

**Commit:** `fabric: provision namespaces across Iggy, Fluss, Lakekeeper and the bucket`.

### Task 5: `loams-fabric ingest` over HTTP

**Files:** `fabric/crates/loams-fabric-ingest/src/{lib.rs,config.rs,http.rs,owner.rs,errors.rs}`, `fabric/crates/loams-fabric/src/main.rs`, `fabric/crates/loams-fabric-ingest/tests/http.rs`.

**Produces:**

```rust
pub struct IngestConfig { pub listen: SocketAddr /* 127.0.0.1:7730 */, pub dedup_window: Duration /* 1 h, ≤ 24 h */,
                          pub claim_ttl: Duration /* 2 min */, pub max_body: usize /* 16 MiB */, pub max_batch: usize /* 10 000 */,
                          pub owners: OwnerMap /* Ruling 6 */ }
pub struct IngestHandle { pub addr: SocketAddr }
impl IngestHandle { pub async fn stop_within(self, grace: Duration); }
pub async fn serve(cfg: IngestConfig, clients: StackClients) -> Result<IngestHandle, IngestError>;
```

**Route:** `POST /v1/namespaces/{ns}/fabric/topics/{topic}/events`, with D270's three HTTP bindings (binary `ce-*` headers, `application/cloudevents+json`, `application/cloudevents-batch+json`) parsed by `loams_cloudevents`. 415 without a CloudEvents shape. Partition: `?partition=` if given, else murmur2(key) mod partitions (the Kafka default partitioner, as D270), else the dedup key's hash. Answer body per event `{status: appended|duplicate|in_flight, partition, offset}`; HTTP 200, or 409 with `Retry-After: 1` when any event is `in_flight`. Validation and batch coalescing exactly as D270 (one invalid event refuses the batch with its index; repeated `source`+`id` in one batch is coalesced). The send to Iggy is one `send_messages` per partition with `persisted` acknowledgement; offsets come from the acknowledgement (verify the 0.11 SDK returns them; if not, Task 0's gap table decides a fallback). Unknown namespace or topic: 404 with `{"error":"unknown_topic"}`. Requests for a partition this node does not own are forwarded to the owner (Ruling 6).

**Tests (over a real socket, against the stack):** `binary_mode_event_appends`; `structured_and_batched_modes_append`; `invalid_event_refuses_batch_with_index`; `repeated_id_in_batch_is_coalesced`; `partition_by_key_matches_kafka_murmur2` (fixtures from the Kafka partitioner); `unknown_topic_is_404`; `not_cloudevents_is_415`; `body_over_limit_is_413`; `non_loopback_is_refused`; `shutdown_drains_then_closes`.

**Commit:** `fabric: ingest CloudEvents over HTTP into Iggy`.

### Task 6: The dedup ledger on Fluss

**Files:** `fabric/crates/loams-fabric-ingest/src/ledger.rs`, `fabric/crates/loams-fabric-ingest/tests/ledger.rs`.

**Produces:**

```rust
// _fabric.ce_dedup: PK (topic_id BIGINT, dedup_key BYTES(32)), merge engine LastRow, with columns state TINYINT (1 pending, 2 done), partition INT, offset BIGINT, claimed_by STRING, until TIMESTAMP_LTZ(3)
pub struct Ledger { /* fluss PK writer + lookuper, owner-local cache */ }
pub enum Claim { Claimed, Duplicate { partition: u32, offset: u64 }, InFlight }
impl Ledger {
    pub async fn claim(&self, topic: u64, keys: &[[u8; 32]], now: SystemTime) -> Result<Vec<Claim>, LedgerError>;
    pub async fn complete(&self, topic: u64, done: &[([u8; 32], u32, u64)], now: SystemTime) -> Result<(), LedgerError>;
    pub async fn release(&self, topic: u64, keys: &[[u8; 32]]) -> Result<(), LedgerError>;   // a definitely failed send
    pub async fn prune(&self, now: SystemTime) -> Result<u64, LedgerError>;                  // entries past the window
}
```

**Scope.** Each Fabric topic is its own deduplication scope, the counterpart of a D270 stream: `topic_id` is a `u64` assigned once when the topic is created (Task 4's `create_topic`) and recorded in `_fabric.flow_objects` (`kind = 'topic'`), never reused after a drop, and independent of which Iggy stream holds the topic. The same `source` + `id` posted to two topics is two events, as on two Loams streams (D270); within one topic it is one event whatever the partition.

**Semantics:** D270's protocol, with Fluss as the store. The owner of `(topic, partition)` is the only writer of its keys (Ruling 6), so `claim` is lookup-then-upsert under an owner-local mutex per key shard; a key pending under a live claim answers `InFlight`; a lapsed claim (past `until`) is re-claimed. An Iggy send that **definitely** failed releases; one that timed out keeps the claims, which lapse after `claim_ttl`. `complete` writes `state = 2` with the offsets and `until = now + window`. `prune` deletes expired keys in batches of 10 000 every minute.

**Tests:** `retry_within_window_answers_duplicate`; `concurrent_retry_answers_in_flight`; `timed_out_send_keeps_claim`; `claim_lapses_after_two_minutes` (a test clock); `failed_send_releases`; `window_expiry_allows_reappend`; `prune_removes_only_expired`; `ledger_survives_fluss_tablet_restart` (stack).

**Commit:** `fabric: deduplicate ingest by source and id with a ledger on Fluss`.

### Task 7: gRPC ingest

**Files:** `fabric/proto/loams/fabric/v1/fabric.proto`, `fabric/crates/loams-fabric-ingest/{build.rs,src/grpc.rs}`, `fabric/crates/loams-fabric-ingest/tests/grpc.rs`.

**Produces:** package `loams.fabric.v1`, service `FabricService` with `rpc ProduceCloudEvents(ProduceCloudEventsRequest) returns (ProduceCloudEventsResponse)`, where the request carries `namespace`, `topic`, optional `partition` and an `io.cloudevents.v1.CloudEventBatch` (the CloudEvents protobuf format, vendored as in PR #171) and the response one `EventResult { status, partition, offset }` per event. Served by connect-rust (Connect, gRPC, gRPC-Web) on the same listener as HTTP (axum router), generated in `build.rs` with `connectrpc-build` like `loams-live-proto` (D128).

**Semantics:** the HTTP handler's core; errors: `INVALID_ARGUMENT` for invalid events (with the index), `NOT_FOUND` for an unknown topic, `UNAVAILABLE` when Iggy is down; `in_flight` per event, never a call failure.

**Tests:** `grpc_and_http_agree` (the same events through both give the same results and the same Iggy messages); `grpc_invalid_event_is_invalid_argument`; `connect_and_grpc_web_clients_work`.

**Commit:** `fabric: ingest CloudEvents over gRPC`.

### Task 8: The `fluss_sink` plugin (Iggy → Fluss)

**Files (in `dina-kar/iggy`, branch `loams/fluss-sink`, based on tag `server-0.9.0`):** `core/connectors/sinks/fluss_sink/{Cargo.toml,src/lib.rs,README.md,config.toml}`, the sink's tests in Iggy's connectors test layout; in this repository: `fabric/iggy-plugins.toml` (the pinned fork revision), `.github/workflows/fabric.yml` (a step that builds the plugin from that revision), `deploy/fabric/connectors/fluss_sink.toml`.

**Produces (plugin config):**

```toml
type = "sink"
key = "fluss"
name = "Fluss sink"
path = "target/release/libiggy_connector_fluss_sink"
plugin_config_format = "toml"
[[streams]]
stream = "ns-42"
topics = ["orders"]
schema = "raw"                      # the plugin reads ce_ headers itself
batch_length = 1000
poll_interval = "5ms"
consumer_group = "fluss_sink_orders"
[plugin_config]
bootstrap = "127.0.0.1:19124"
database = "ns_42"
table = "orders_current"
mode = "pk"                         # "log" | "pk"
schema_from = "table"               # columns decoded from JSON data by the table's schema
dead_letter_topic = "orders.dlq"
```

**Semantics:** per poll batch: decode each message with `loams-fabric-envelope` (vendored as a path dependency is not possible in Iggy's workspace, so the plugin depends on a published `loams-fabric-envelope` crate or carries a small copy of the mapping; Task 0 decides, defaulting to a copy with a test against the same fixtures), build Arrow batches with `to_rows`, write with `fluss-rs` (append for `log`, upsert or delete by `Op` for `pk`), wait for the acknowledgement, then let the runtime commit the consumer-group offset. An event that fails decoding goes to `dead_letter_topic` with `ce_loamserror`. A Fluss write error retries with backoff (1 s → 30 s) and never commits past it.

**Tests (Iggy's connector test harness, run in the fork's CI and ours):** `log_mode_appends_every_event`; `pk_mode_upserts_and_deletes`; `replay_after_crash_is_idempotent_for_pk`; `decode_failure_goes_to_dlq`; `fluss_outage_blocks_without_loss`.

**Upstream:** a PR to `apache/iggy` only after the owner's go-ahead; the PR description links §32 §5.9.

**Commit (this repo):** `iggy: build and configure the fluss_sink plugin`.

### Task 9: The use cases: a PK table and a Log table tiered to Iceberg

**Files:** `fabric/proto/loams/fabric/v1/tables.proto`, `fabric/crates/loams-fabric-ingest/src/tables.rs` (`loams-fabric tables create|describe|drop`), `fabric/crates/loams-fabric/tests/e2e.rs` (`tiering_*`), `scripts/fabric/read_iceberg.py`.

**Produces:**

```rust
pub struct TableSpec { pub namespace: u64, pub name: String, pub kind: TableKind, pub columns: Vec<Column>,
                       pub partition_by: Option<Partition>, pub buckets: u32 /* 4 */,
                       pub tiering: Tiering /* { enabled: true, freshness: 60 s } */, pub properties: BTreeMap<String, String> }
pub enum TableKind { Log, Pk { key: Vec<String>, merge: Merge } }
pub enum Merge { LastRow, FirstRow, Versioned { column: String }, Aggregation { functions: BTreeMap<String, AggFn> } }
pub enum AggFn { Sum, Min, Max, LastValue, LastValueIgnoreNulls, FirstValue, Rbm32, Rbm64, ListAgg, BoolAnd, BoolOr }  // Fluss 1.0's set (verify)
pub async fn create_table(clients: &StackClients, spec: &TableSpec) -> Result<(), TableError>;
```

**Use cases:** `user_profile` (PK `user_id`, Versioned on `updated_at`, CE columns plus `name STRING, plan STRING, updated_at TIMESTAMP_LTZ(3)`) fed by topic `profiles` through `fluss_sink`; `events` (Log, partitioned by `day(_ce_time)`) fed by topic `events`.

**Semantics:** Fluss tables with `table.datalake.enabled = true` and the freshness above; the tiering job writes Iceberg tables `ns_<id>.user_profile` and `ns_<id>.events` in Lakekeeper. `read_iceberg.py` reads them with pyiceberg (REST catalog, RustFS) and DuckDB's Iceberg extension, prints row counts and a checksum.

**Tests:** `tiering_log_table_reaches_iceberg` (10 000 events in; within 3 × freshness the Iceberg row count is 10 000 and the checksum matches); `tiering_pk_table_keeps_latest_version` (out-of-order versions in, the Iceberg table holds the max version per key, read with pyiceberg); `pk_lookup_returns_latest` (Fluss lookup before tiering); `table_properties_roundtrip` (`loams.*` properties survive, needed by FL2).

**Commit:** `fabric: create Fluss tables and tier them to Iceberg through Lakekeeper`.

### Task 10: The `loams_sink` plugin (Iggy → Loams)

**Files (in `dina-kar/iggy`, branch `loams/loams-sink`):** `core/connectors/sinks/loams_sink/**`; in this repository: `deploy/fabric/connectors/loams_sink.toml`, `fabric/crates/loams-fabric/tests/e2e.rs` (`loams_sink_*`).

**Semantics:** two targets, by config: `target = "stream"` calls the Loams engine's `StreamService.ProduceCloudEvents` (PR #171; gRPC on the engine's address, loopback) with the batch's events unchanged, so D270's ledger dedupes re-delivery; `target = "collection"` writes documents through the native REST write API (`POST /v1/namespaces/{ns}/collections/{c}/documents`), the primary key `"{_ce_source}|{_ce_id}"` unless `key_from = "<json pointer into data>"`, so replays upsert the same documents. Offsets commit after the engine's acknowledgement. Engine backpressure (HTTP 429 with `Retry-After`, D86) pauses polling for the advertised time.

**Tests (stack + a `loams dev` started by the test harness on a loopback port):** `stream_target_dedupes_replays` (replay the same Iggy batch twice; the Loams stream has each event once); `collection_target_upserts`; `backpressure_pauses_without_loss`; `engine_down_blocks_without_loss`.

**Commit (this repo):** `iggy: build and configure the loams_sink plugin`.

### Task 11: The FL1 gate: end-to-end correctness under kills

**Files:** `fabric/crates/loams-fabric/tests/e2e.rs` (`fabric_kill_matrix`), `scripts/fabric/kill_matrix.sh`, `docs/plans/fl1-exit-report.md`.

**Semantics:** a seeded load generator posts CloudEvents to `ingest` (mixed binary and batched, 5 % retries of already-acknowledged requests, 1 % invalid events) into `profiles` and `events` at a fixed rate for 10 minutes, while `kill_matrix.sh` kills, at seeded random times, one of: the Iggy container (`docker kill`, then start), the Fluss tablet server, the Fluss coordinator, the Flink taskmanager, the connectors runtime, `loams-fabric ingest`. After the load stops and the stack settles (tiering caught up), the checker reads: every acknowledged response, the Iggy topics (all messages), the Fluss tables (scan), the Iceberg tables (pyiceberg). It asserts:
1. every acknowledged event is in Iggy, in Fluss and in Iceberg;
2. no event id appears twice in Iggy among events acknowledged within one dedup window of each other except two documented cases, which the checker counts and reports separately: (a) node death, a pending claim lapsed after an `ingest` kill between append and complete; (b) commit-unknown, an Iggy send that timed out but had appended, whose claim lapsed and whose retry appended again (Task 6's `timed_out_send_keeps_claim`);
3. `user_profile` in Fluss and Iceberg equals the checker's model (max version per key);
4. invalid events never appear anywhere;
5. no event appears that was never sent.

The exit report records throughput, p50/p99 ingest latency, end-to-end freshness to Fluss and to Iceberg, the duplicate counts by cause, and peak RSS per service.

**Tests:** `fabric_kill_matrix` (seed corpus of 3 seeds on PRs, 20 nightly).

**Commit:** `fabric: add the FL1 kill-matrix gate and the exit report`.

### Task 12: Documentation

**Files:** `docs/guides/fabric/{index.md,ingest.md}`, `deploy/fabric/README.md`, `CHANGELOG.md`, `docs/plans/README.md` (the FL1 row's status).

**Semantics:** how to start the stack, provision a namespace, create a topic and a table, post events (curl examples for the three HTTP modes), read them from Iggy and Fluss, and see them in Iceberg; the dedup rules and limits; what is not in FL1 (auth, HA ingest owners, the House).

**Commit:** `docs: document the Event Fabric foundation`.

## PR grouping

One PR per group, stacked; each builds and passes CI on its own.

| PR | Tasks | Title | Size (estimate) |
|---|---|---|---|
| A | 0 | FL1 (1/9): dependency spike and reconciliation | docs only |
| B | 1 | FL1 (2/9): the `fabric/` workspace and CI | ~400 lines |
| C | 2 | FL1 (3/9): CloudEvents on Iggy and Fluss | ~900 lines |
| D | 3 | FL1 (4/9): the dev stack | ~500 lines (YAML, scripts) |
| E | 4 | FL1 (5/9): namespace provisioning | ~600 lines |
| F | 5, 6 | FL1 (6/9): HTTP ingest and the dedup ledger | ~1 500 lines |
| G | 7 | FL1 (7/9): gRPC ingest | ~500 lines |
| H | 8, 9 | FL1 (8/9): `fluss_sink`, tables and tiering | ~1 200 lines here, plus the fork branch |
| I | 10, 11, 12 | FL1 (9/9): `loams_sink`, the kill-matrix gate, docs | ~1 200 lines here, plus the fork branch |

## Rulings made during execution

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| — | (Task 0 fills this table) | | |
