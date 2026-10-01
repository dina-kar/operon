# CN1 — The Connector Registry and the ★ Connectors Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, event types, constants), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: Planned** (2026-10-01). Track CN, after FL1 (it needs the Fabric stack, `ingest` and the plugins); it can run beside FL2. Branches `cn1-t<N>`, stacked; PRs target `main`. CN1 adds the crate `loam-flow` to the `fabric/` workspace, the `flow` role of `loam-fabric`, a Java service `loam-connect` (Task 12) and registry data under `connectors/`. It changes no engine code.

**Goal:** Ship [§33](../design/33-connectors.md)'s registry, capability schema and the 21 ★ connectors of §33 §8:
- `loam.flow.v1` connector protos, the manifest loader and validator, and the 200-connector registry generated from one catalog file, with drift checks against the Camel and Kestra catalogs;
- connector instances, the `FlowService` API, the runtime supervisors (native, Iggy connectors runtime, Camel `loam-connect`, Debezium Server) and the contract-test kit;
- the ★ connectors in the précis' rollout order, each with a manifest, contract tests against a real service and docs;
- the CN1 gate of §33 §9.

**Architecture:**
- **`loam-flow`** holds the registry, instance validation, the runtime supervisors and the native connectors (`loam_flow::connectors::<id>`). `loam-fabric flow` serves `FlowService` over connect-rust on `127.0.0.1:7731` (loopback, D111) and supervises instances. One `flow` process supervises everything in CN1; leases for several `flow` processes come in FL3.
- **State** is the Fabric's system table `_fabric.flow_objects` (Fluss PK table, LastRow; key `(namespace BIGINT, kind STRING, name STRING)`; columns `version BIGINT, spec BYTES (postcard), updated TIMESTAMP_LTZ(3)`), as §32 D337 states. Native source positions are in `_fabric.flow_positions` (key `(namespace, instance, partition_key STRING)`, value `position BYTES`).
- **Every source writes to the Fabric** through `loam-fabric ingest`'s core (in-process for native connectors, HTTP for Debezium Server) or directly to Iggy with the envelope of FL1 Task 2; **every sink reads from Iggy** (native connectors as Iggy consumer groups; Iggy plugins inside the connectors runtime; Camel through `camel-iggy`).
- **Secrets** come from a `SecretStore` trait: `FileSecretStore` (dev, `deploy/fabric/secrets/`, mode 0600) in CN1; Dapr's secrets building block through `loam-dapr` (D189) when §24's F1 lands.

**Tech Stack:**
- Rust 1.97.1, the `fabric/` workspace. New dependencies (Task 0 verifies versions, licences, build cost): `jsonschema` 0.30+ (MIT), `serde_yaml_ng` or `serde_norway` (MIT/Apache; Task 0 picks a maintained YAML crate), `rdkafka` 0.39 with `cmake-build` and `ssl-vendored` (MIT; librdkafka BSD-2-Clause), `tokio-postgres` 0.7 (MIT/Apache), `mysql_async` 0.37 (MIT/Apache), `object_store` 0.14 (Apache-2.0), `iceberg` 0.10 (Apache-2.0), `parquet` 59, `arrow-ipc`/`arrow-flight` 59, `apache-avro` 0.22 (Apache-2.0), `adbc_core` and `adbc_driver_manager` 0.24 (Apache-2.0), `clickhouse` 0.15 (Apache-2.0), `aws-sdk-kinesis` 1.x (Apache-2.0), `redis` 1.7 (BSD-3-Clause), `reqwest` 0.12, `hmac`, `sha2`, `opentelemetry-proto` (Apache-2.0) for OTLP decoding.
- Services for tests (added to `deploy/fabric/compose.yaml` under profile `connectors`, pinned by digest): Apache Kafka 4.x in KRaft mode (Apache-2.0), Postgres 17 with `wal_level=logical`, MySQL 8.4 with row binlog, Debezium Server 3.7.0.Final, Elasticsearch-compatible target (Loam's own ES gateway from `operon dev`, plus OpenSearch 3.x (Apache-2.0) for a third-party check), ClickHouse server (the FL2 reference image), Redis-compatible Valkey 8 (BSD-3-Clause), floci 2.1.0 (MIT, D60) for Kinesis and S3 events, RustFS (S3), Lakekeeper.
- ADBC drivers: Snowflake and BigQuery drivers (Apache-2.0, `adbc-drivers/*`), Postgres, SQLite, DuckDB and Flight SQL drivers (Apache Arrow ADBC); shipped as shared libraries pinned by SHA-256 (Q349).
- Java (Task 12 only, **run, never written**): the `apache/camel` 4.22.x runtime image or Camel JBang (`camel run *.yaml`), JDK 21 inside it. **Java is deferred (owner, 2026-10-01): Loam writes no Java in CN1** — no Maven project, no Loam processor, no Java SDK; routes are YAML, transforms use Camel's built-in `jq`/`jsonata` languages and `camel-cloudevents`.
- Names: published packages `loams-*` (crates.io, PyPI) and `@loams/*` (npm); CloudEvents types `io.loams.dev.<domain>.<name>.v1` (owner rulings, 2026-10-01).

**Spec:**
- [`docs/design/33-connectors.md`](../design/33-connectors.md): all of it.
- [`docs/design/32-loam-flow-fabric-house.md`](../design/32-loam-flow-fabric-house.md): §5.4 (envelope), §5.5 (adapters), §5.7 (bridges), §6 (Flow).
- [FL1](2026-10-01-fl1-fabric-foundation.md) as built: `loam-fabric-envelope`, `loam-fabric-ingest`, provisioning, `TableSpec`, the plugins.
- [`docs/design/13-decision-log.md`](../design/13-decision-log.md): D11, D60, D111, D189, D210; D352–D359 once merged.

## Global Constraints

- **Buy first** (§33 D354): before writing a native connector, the task checks whether the Iggy plugin or the Camel component covers the declared capabilities; a native one is written only where §33 §8 says so.
- **Every manifest validates** against `connectors/schema/connector.schema.json`, and every declared capability of a ★ connector has a contract test (§33 D353 rule 3). CI fails otherwise.
- **Licence gate** (§33 D359): `connectors/licences.toml` lists every runtime component and driver with its SPDX id; CI refuses AGPL, BSL, SSPL, ELv2, `NOASSERTION` and unknown ids for anything in the image or the default compose.
- **No secret in a manifest, an instance spec, a rendered config, a log line or an error.** Instances reference secrets by name; rendered configs for Iggy, Camel and Debezium read them from files mounted at run time. A test greps every rendered config and captured log for the test secrets' values (Task 3).
- **Loopback only (D111)** for `FlowService` and every connector endpoint Loam serves (webhooks, OTLP).
- **Tests skip without services**, as FL1 (`loam_fabric_testing::stack()`, plus `LOAM_CONNECTORS_STACK` for the `connectors` profile).
- **The build machine.** `rdkafka`'s static librdkafka build is the heaviest addition (Task 0 measures); it is behind the feature `kafka` (on in CI and release, off by default for local builds of other tasks).
- **Commit areas:** `flow`, `connectors`, `connect` (YAML routes), `ci`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **The registry is generated from one CSV**, `connectors/registry/catalog.csv` (the columns of §33 Appendix A plus `id`, `category`, `runtime`, `ref`, `status`); per-connector YAML manifests for ★ connectors are hand-written and checked against the CSV; P2/P3 manifests are generated stubs with `status: planned` | One source for 200 rows; hand-written detail where it matters | A CSV edit can disagree with a hand-written manifest; the drift test catches it |
| 2 | **Event types**: `io.loams.dev.flow.<connector-id>.<event>.v1` with these events: Kafka `record`, Kinesis `record`, Redis `stream-entry`, S3 `object` and `object-deleted`, Iceberg `rows`, Postgres/MySQL batch `rows`, CDC `change` (with `loamop`), ADBC `rows`, HTTP poll `response-item`, webhook `delivery`, OTLP `log`, `span`, `metric` | One naming rule (§33 D355) | Renaming later is a manifest major bump |
| 3 | **Batch-of-rows events** carry Arrow IPC stream bytes (`datacontenttype: application/vnd.apache.arrow.stream`), 65 536 rows each by default (`batch_rows`), one event per batch; their `id` is `"<run-id>/<partition>/<first-row>-<last-row>"` and a run id is stable across retries of the same run | Bulk stays columnar end to end (§33 D356); `fluss_sink` appends Arrow batches without per-row decoding (FL1 Task 8 gains an Arrow fast path in this plan's Task 5) | Iggy's message size limit caps batch bytes; Task 0 records the limit and `batch_bytes` (default 8 MiB) splits batches |
| 4 | **Native source positions commit after the Fabric acknowledges** the batch (FL1's `ingest` core answers with offsets) | At-least-once with re-delivery recognisable by id (§33 §6) | None |
| 5 | **Native sinks are Iggy consumer groups** named `flow-<instance>`, committing offsets after the sink acknowledges | Same model as the Iggy plugins | None |
| 6 | **Debezium Server runs one container per CDC instance** with the HTTP sink to `loam-fabric ingest` (`debezium.sink.type=http`, CloudEvents structured format through Debezium's CloudEvents converter, verify the 3.7 property names in Task 0), offsets and schema history in files on a volume | Unmodified Debezium; one failure domain per source database | Many containers for many sources; Q351's Iggy `postgres_source` for small Postgres sources |
| 7 | **`loam-connect` is the unmodified Camel runtime in Main mode** (not Quarkus) in CN1, with YAML-DSL routes loaded from a mounted directory and reloaded on change; no Loam Java (Java deferred, owner 2026-10-01) | Least moving parts; Quarkus native is Q352 for CN2 | JVM start time and memory; measured in Task 12 |
| 8 | **JDBC drivers are not shipped**: `loam-connect` loads drivers from a mounted directory; the PostgreSQL (BSD-2-Clause) and MariaDB Connector/J (LGPL-2.1, loaded, not modified) drivers are documented; MySQL Connector/J (GPL-2.0 with FOSS exception) is the user's choice | Keeps the image's licence set clean (D359) | One more step for users of JDBC |
| 9 | **ADBC drivers load by path from an allowlist** in the instance's namespace config (`adbc.drivers = ["snowflake", "bigquery", …]`) and only from `/opt/loam/adbc/<name>/<version>/` with a SHA-256 check | A driver is native code in Loam's process | A compromised driver image still runs in-process; Q349 |

## Carried in

FL1's rulings during execution, its stack, `ingest` core (`loam_fabric_ingest::{IngestCore, append_events}` as built) and the plugins at their pinned fork revisions.

## Review Focus

1. **Capabilities are proven, not claimed**: each ★ manifest's capabilities against its contract tests. Test: Task 3 (`every_declared_capability_has_a_test`).
2. **No loss across restarts** of each runtime, and duplicates only within the declared delivery. Tests: each connector task's `kill_restart_*`.
3. **Secrets never leak**. Test: Task 3 (`no_secret_in_rendered_configs_or_logs`).
4. **Bulk stays columnar** (no per-row JSON on ADBC, Parquet, Iceberg and Postgres batch paths). Test: Task 11 (`bulk_path_has_no_row_decode`, a counter in `loam-fabric-envelope` that must stay 0).
5. **Licence gate**. Test: Task 2 (`licence_gate_refuses_flagged`).

## File structure

```
connectors/schema/connector.schema.json         connectors/licences.toml
connectors/registry/catalog.csv                 connectors/registry/<id>.yaml (21 hand-written ★, 179 generated stubs)
connectors/schemas/<id>.config.json             # JSON Schema per connector instance config
fabric/proto/loam/flow/v1/{connector.proto,instance.proto,flow.proto}
fabric/crates/loam-flow/src/{lib.rs,registry.rs,manifest.rs,validate.rs,instance.rs,store.rs,secrets.rs,service.rs,
                            runtime/{mod.rs,native.rs,iggy.rs,camel.rs,debezium.rs},
                            formats/{mod.rs,parquet.rs,avro.rs,arrow.rs},
                            connectors/{mod.rs,http.rs,webhook.rs,kafka.rs,postgres.rs,mysql.rs,cdc.rs,s3.rs,iceberg.rs,
                                        clickhouse.rs,adbc.rs,kinesis.rs,redis.rs,otlp.rs}}
fabric/crates/loam-flow/tests/{registry.rs,validate.rs,secrets.rs,runtime.rs,<connector>.rs …}
fabric/crates/loam-flow-conformance/src/{lib.rs,contract.rs,roundtrip.rs,kill.rs,dup.rs,bulk.rs,cdc.rs}
connect/{routes/templates/*.yaml.tmpl,application.properties,compose.fragment.yaml,README.md}   # YAML only; no Java (deferred)
deploy/fabric/compose.yaml (profile connectors)  deploy/fabric/connectors/**
scripts/connectors/{gen_registry.sh,camel_catalog.py,kestra_catalog.py,matrix.py}
.github/workflows/fabric.yml (jobs connectors-unit, connectors-it, connect-routes)
docs/guides/connectors/{index.md (generated), <id>.md for each ★}
docs/plans/cn1-dependency-spike.md  docs/plans/cn1-exit-report.md
```

### Task 0: Reconcile and measure

**Files:** read FL1 as built; write `docs/plans/cn1-dependency-spike.md`; fill "Rulings made during execution".

**Checks:** the versions and licences of every dependency in Tech Stack (crates.io, SPDX); `rdkafka` static build time and binary size delta (one measured build each way); ADBC driver availability per platform (Snowflake, BigQuery, Postgres, SQLite, DuckDB, Flight SQL), their download sources and SHA-256s, and a load test of each through `adbc_driver_manager` 0.24 with arrow 59; Debezium Server 3.7 image, the HTTP sink and CloudEvents converter properties, and whether its offset store can be other than a file; the Camel version to pin (latest 4.x LTS or 4.22.x; record Camel's LTS list), `camel-iggy`'s status and its Iggy client version against Iggy 0.9.0; the Iggy plugin list at FL1's pinned revision and each plugin's config keys for the ★ sinks used (`elasticsearch_sink`, `clickhouse_sink`, `postgres_sink`, `s3_sink`, `iceberg_sink`, `http_sink`); Iggy's maximum message and batch sizes (Ruling 3); the Kestra plugin list and Camel catalog JSON locations for the drift scripts.

**Commit:** `docs: reconcile CN1 with FL1 and record the connector dependency spike`.

### Task 1: Protos, manifests and validation

**Files:** `fabric/proto/loam/flow/v1/{connector.proto,instance.proto}`, `connectors/schema/connector.schema.json`, `fabric/crates/loam-flow/src/{lib.rs,manifest.rs,validate.rs,registry.rs}`, `fabric/crates/loam-flow/tests/{registry.rs,validate.rs}`.

**Produces:**

```rust
pub struct ConnectorSpec { pub id: String, pub name: String, pub spec_version: semver::Version, pub category: Category,
                           pub priority: Priority, pub starred: bool, pub status: Status, pub runtime: RuntimeRef,
                           pub licence: Licence, pub capabilities: Capabilities, pub auth: Vec<AuthMethod>,
                           pub config_schema: serde_json::Value, pub secrets: Vec<String>, pub envelope: EnvelopeDecl,
                           pub limits: Limits, pub conformance: Vec<Suite>, pub docs: Option<String> }
pub struct Capabilities { pub source: Option<SourceCaps>, pub sink: Option<SinkCaps>, pub delivery: Delivery,
                          pub ordering: Ordering, pub formats: Vec<Format>, pub schema: SchemaCaps, pub bulk: BulkCaps, pub backpressure: Backpressure }
pub struct SourceCaps { pub streaming: bool, pub batch: bool, pub cdc: bool, pub webhook: bool, pub resumable: bool, pub position: String }
pub struct SinkCaps { pub streaming: bool, pub batch: bool, pub transactional: bool, pub upsert: bool, pub delete: bool, pub idempotent: bool }
pub enum RuntimeRef { Native { module: String }, Iggy { plugin: String }, Camel { scheme: String }, Debezium { connector_class: String }, OpenApi { spec: String } }
pub struct Registry { /* by id, by category */ }
impl Registry { pub fn load(dir: &Path) -> Result<Self, RegistryError>; pub fn get(&self, id: &str) -> Option<&ConnectorSpec>; pub fn list(&self, filter: &Filter) -> Vec<&ConnectorSpec>; }
pub fn validate_manifest(m: &serde_json::Value) -> Result<ConnectorSpec, Vec<ManifestError>>;   // JSON Schema + semantic rules
pub fn check_use(spec: &ConnectorSpec, uses: &Uses) -> Result<(), CapabilityError>;              // §33 D353 rule 1
pub struct CapabilityError { pub connector: String, pub capability: String, pub spec_version: String }
```

**Semantic rules** beyond the JSON Schema: a sink with `upsert` declares a key in its config schema; `cdc: true` implies `source.streaming` and a `position`; `delivery.sink = exactly_once` requires `transactional` or `idempotent`; `starred` implies `priority = P1`; every auth method is in the enum of §33 Appendix A's legend; every secret name appears in the config schema with `"writeOnly": true`.

**Tests:** `schema_validates_kafka_example` (§33 §4's YAML verbatim); `semantic_rules_each_refuse` (one case per rule); `check_use_refuses_undeclared` (cdc on a polling source, upsert on an append sink, exactly-once on an at-least-once sink); `proto_and_yaml_agree` (round-trip through the protobuf form).

**Commit:** `flow: add connector manifests, their schema and validation`.

### Task 2: The registry data, the generator and the drift checks

**Files:** `connectors/registry/catalog.csv`, `connectors/registry/*.yaml`, `connectors/licences.toml`, `scripts/connectors/{gen_registry.sh,camel_catalog.py,kestra_catalog.py,matrix.py}`, `fabric/crates/loam-fabric/src/main.rs` (`loam-fabric connectors gen|list|describe|validate`), `fabric/crates/loam-flow/tests/registry.rs`.

**Semantics:** `catalog.csv` holds §33 Appendix A's 200 rows plus the extra columns of Ruling 1. `connectors gen` writes stub manifests for rows without a hand-written one, and renders Appendix A's Markdown tables (`scripts/connectors/matrix.py --check` compares them with `docs/design/33-connectors.md`'s appendix). `camel_catalog.py` reads `camel-catalog` 4.22.x's component JSON (Maven artifact, Apache-2.0) and checks every Camel cell names a real component and that its producer/consumer support agrees with Sink/Source; `kestra_catalog.py` checks Kestra cells against the plugin repository list; both run weekly and on demand, not on every PR (network). The licence gate reads `licences.toml` and each manifest's `licence`.

**Tests:** `registry_has_200_entries_and_21_starred`; `csv_and_manifests_agree`; `appendix_matches_csv`; `licence_gate_refuses_flagged` (fixtures with AGPL, BSL, ELv2, NOASSERTION); `connectors_cli_list_and_describe`.

**Commit:** `connectors: generate the 200-connector registry and its drift checks`.

### Task 3: Instances, `FlowService`, runtimes, secrets and the contract-test kit

**Files:** `fabric/proto/loam/flow/v1/flow.proto`, `fabric/crates/loam-flow/src/{instance.rs,store.rs,secrets.rs,service.rs,runtime/*.rs}`, `fabric/crates/loam-flow-conformance/**`, `fabric/crates/loam-flow/tests/{secrets.rs,runtime.rs}`.

**Produces:**

```rust
pub struct Instance { pub namespace: u64, pub name: String, pub connector: String, pub connector_major: u64,
                      pub config: serde_json::Value, pub secrets: BTreeMap<String, SecretRef>, pub direction: Direction,
                      pub fabric: FabricBinding /* topic or table to write (source) or consume (sink) */, pub enabled: bool }
pub trait SecretStore: Send + Sync { async fn resolve(&self, ns: u64, r: &SecretRef) -> Result<Secret, SecretError>; }
pub struct Secret(zeroize::Zeroizing<Vec<u8>>);                       // Debug prints "<redacted>"
#[async_trait] pub trait ConnectorRuntime: Send + Sync {
    fn kind(&self) -> RuntimeKind;
    async fn start(&self, inst: &Instance, spec: &ConnectorSpec) -> Result<RunHandle, RuntimeError>;
    async fn stop(&self, h: RunHandle, grace: Duration) -> Result<(), RuntimeError>;
    async fn status(&self, h: &RunHandle) -> RunStatus;              // Running { lag, last_error } | Stopped | Failed
}
// FlowService (connect-rust, 127.0.0.1:7731): ListConnectors, DescribeConnector, CreateInstance, UpdateInstance,
// ValidateInstance, DeleteInstance, StartInstance, StopInstance, InstanceStatus.
// loam-flow-conformance:
pub async fn contract(spec: &ConnectorSpec, harness: &dyn ConnectorHarness) -> SuiteResult;   // one check per declared capability
pub trait ConnectorHarness { /* seed the external system, read it back, kill and restart its runtime */ }
```

**Semantics:** instances are validated against the manifest's config schema and `check_use`; secrets are resolved only inside the runtime at start; runtimes: `NativeRuntime` runs the connector's `Source`/`Sink` impl (traits below) on tokio; `IggyRuntime` renders a connectors-runtime TOML per instance (secrets as file paths), runs `iggy-connectors` as a child process with a health probe on its HTTP API (verify the runtime's API), restarts with backoff; `CamelRuntime` writes a YAML route file into `loam-connect`'s watched directory (Task 12); `DebeziumRuntime` renders `application.properties` and runs a Debezium Server container through the local Docker API in dev (Kubernetes Deployment rendering is FL3's). Native connector traits:

```rust
#[async_trait] pub trait Source: Send { async fn open(&mut self, pos: Option<Position>) -> Result<(), ConnectorError>;
                                        async fn next(&mut self) -> Result<Option<SourceBatch>, ConnectorError>;
                                        async fn committed(&mut self, upto: &Position) -> Result<(), ConnectorError>; }
#[async_trait] pub trait Sink: Send { async fn write(&mut self, events: &[CloudEvent]) -> Result<SinkAck, ConnectorError>;
                                      async fn write_arrow(&mut self, batch: RecordBatch) -> Result<SinkAck, ConnectorError> { /* default: unsupported */ } }
pub struct SourceBatch { pub events: Vec<CloudEvent>, pub position: Position }
```

**Tests:** `instance_validation_uses_manifest`; `instance_versions_increment`; `no_secret_in_rendered_configs_or_logs` (every runtime renders a config with known test secrets; a grep over rendered files and captured logs finds none); `secret_debug_is_redacted`; `iggy_runtime_restarts_crashed_child`; `every_declared_capability_has_a_test` (for each ★ manifest, the conformance kit has a check registered for each declared capability; fails on a missing one).

**Commit:** `flow: add instances, FlowService, runtime supervisors and the contract-test kit`.

### Task 4: HTTP/REST ★ and Webhooks ★

**Files:** `fabric/crates/loam-flow/src/connectors/{http.rs,webhook.rs}`, `connectors/registry/{http,webhooks}.yaml`, `connectors/schemas/{http,webhooks}.config.json`, `fabric/crates/loam-flow/tests/{http.rs,webhook.rs}`, `docs/guides/connectors/{http,webhooks}.md`.

**Semantics:** **HTTP sink**: POST each event in CloudEvents binary mode (or structured, by config) with `Idempotency-Key: <ce_id>`, retries with exponential backoff (100 ms → 30 s, 10 attempts) on 408/425/429/5xx and connection errors, honouring `Retry-After`; 4xx other than those → DLQ. **HTTP polling source**: a request template, a cursor extracted with a JSON pointer (or a `Link` header), an items pointer, one event per item with `id` = `"<url>#<item key>"` and the cursor as the position; poll interval and rate limit from config. **Webhooks source**: routes `POST /v1/namespaces/{ns}/fabric/webhooks/{instance}` on `ingest`'s listener; verification schemes `hmac-sha256` (header and secret configurable), `github` (`X-Hub-Signature-256`), `stripe` (`Stripe-Signature` with tolerance 300 s), `slack` (`X-Slack-Signature` + timestamp), `shopify` (`X-Shopify-Hmac-Sha256`); a failed signature → 401 and a counter; each delivery becomes one CloudEvent (`type io.loams.dev.flow.webhooks.delivery.v1`, `id` the provider's delivery id header where one exists, else SHA-256 of the body and timestamp).

**Tests:** `http_sink_retries_and_dedupe_header`; `http_sink_4xx_goes_to_dlq`; `http_poll_follows_cursor_and_resumes`; `webhook_each_scheme_verifies` (fixtures from each provider's docs); `webhook_bad_signature_is_401`; `webhook_replay_is_deduplicated` (same delivery id twice → one event); contract suites for both manifests.

**Commit:** `connectors: HTTP sink, HTTP polling source and signed webhooks`.

### Task 5: Parquet ★, Avro ★ and Arrow IPC/Flight ★

**Files:** `fabric/crates/loam-flow/src/formats/*.rs`, `connectors/registry/{parquet,avro,arrow}.yaml`, `fabric/crates/loam-flow/tests/formats.rs`; in `dina-kar/iggy` `loam/fluss-sink`: an Arrow fast path (`datacontenttype = application/vnd.apache.arrow.stream` events appended as batches without per-row decoding).

**Semantics:** `formats::decode(format, bytes, schema) -> RecordBatch stream` and `encode(format, batch) -> bytes` for Parquet (files and row groups), Avro (object container files, single-object encoding, and the Confluent wire format with a registry client stub until Q344), Arrow IPC stream and file, and Flight (`DoGet` source with a ticket, `DoPut` sink, including against Loam's Flight SQL endpoint for collections and streams). Schema mapping Avro ↔ Arrow per the Arrow project's rules (logical types: timestamp-millis/micros, date, decimal, uuid).

**Tests:** `parquet_roundtrip_all_types`; `avro_container_and_confluent_framing`; `avro_logical_types_map`; `arrow_ipc_roundtrip`; `flight_doget_from_loam` (an `operon dev` collection scanned through Flight into Arrow events); `fluss_sink_arrow_fast_path` (in the fork's tests and ours: the decode counter stays 0).

**Commit:** `connectors: Parquet, Avro and Arrow codecs with a columnar fast path`.

### Task 6: Kafka ★

**Files:** `fabric/crates/loam-flow/src/connectors/kafka.rs`, `connectors/registry/kafka.yaml`, `connectors/schemas/kafka.config.json`, `fabric/crates/loam-flow/tests/kafka.rs`, `docs/guides/connectors/kafka.md`.

**Semantics:** **source**: an `rdkafka` `StreamConsumer` in group `loam-<ns>-<instance>`, `enable.auto.commit=false`; each record → one CloudEvent (records that already carry valid `ce_` headers are passed through as those events, D270's rule 1; others get `type io.loams.dev.flow.kafka.record.v1`, `id` `"<topic>/<partition>/<offset>"`, `subject` the key, headers as `kafkaheader_<name>` extensions); offsets committed after the Fabric acknowledges (Ruling 4); partitions map to Iggy partitions by key hash (Kafka's murmur2, so per-key order is kept). **sink**: an idempotent producer (`enable.idempotence=true`, `acks=all`), the CloudEvents Kafka binary layout (D270), key = `partitionkey` else `subject`; Avro values with a schema registry when configured. Auth: SASL PLAIN/SCRAM over TLS, mTLS, MSK IAM (verify `rdkafka`'s OAUTHBEARER callback route for MSK IAM in Task 0; else documented as Camel-only).

**Tests:** `source_reads_all_partitions_in_order_per_key`; `source_resumes_from_committed_offsets`; `ce_headers_pass_through`; `sink_idempotent_on_retry`; `sink_ce_binary_layout_matches_d270`; `kill_restart_source_no_loss`; `sasl_scram_and_mtls`; contract suite.

**Commit:** `connectors: Kafka source and idempotent sink`.

### Task 7: PostgreSQL ★ and MySQL ★ (batch and upsert)

**Files:** `fabric/crates/loam-flow/src/connectors/{postgres.rs,mysql.rs}`, `connectors/registry/{postgresql,mysql}.yaml`, `fabric/crates/loam-flow/tests/{postgres.rs,mysql.rs}`, docs.

**Semantics:** **Postgres batch source**: a snapshot (`REPEATABLE READ`, exported snapshot shared by parallel workers), table split by primary-key ranges into `partitions` (default 8) workers, each `COPY (SELECT … WHERE pk >= $1 AND pk < $2) TO STDOUT (FORMAT binary)` decoded into Arrow batches (Ruling 3); incremental mode by a `cursor_column` high-water mark. **Postgres sink**: upsert batches through a temporary table and `INSERT … ON CONFLICT (key) DO UPDATE` (or `COPY` for append), in one transaction per batch, so a batch is atomic; deletes for `loamop = d`. Streaming sinks may instead use Iggy's `postgres_sink` (manifest `runtime` alternative recorded). **MySQL**: the same with chunked `SELECT … WHERE pk > ? ORDER BY pk LIMIT n` and `INSERT … ON DUPLICATE KEY UPDATE`. Auth: password, TLS (verify-full), AWS RDS IAM tokens.

**Tests:** `pg_parallel_snapshot_is_consistent` (concurrent writes during the read do not appear); `pg_incremental_cursor`; `pg_upsert_batch_atomic`; `pg_delete_on_loamop_d`; `mysql_chunked_read_and_upsert`; `bulk_path_has_no_row_decode` (Postgres batch path); `kill_restart_*`; contract suites.

**Commit:** `connectors: Postgres and MySQL batch sources and upsert sinks`.

### Task 8: Debezium-Postgres ★ and Debezium-MySQL ★ (CDC)

**Files:** `fabric/crates/loam-flow/src/connectors/cdc.rs`, `fabric/crates/loam-flow/src/runtime/debezium.rs`, `connectors/registry/{debezium-postgres,debezium-mysql}.yaml`, `deploy/fabric/connectors/debezium/*.properties.tmpl`, `fabric/crates/loam-flow/tests/cdc.rs`, `docs/guides/connectors/cdc.md`.

**Semantics (§33 D357, §7):** an instance renders Debezium Server's properties (connector class, slot and publication names `loam_<instance>`, `snapshot.mode=initial`, incremental snapshots through a signal table, the HTTP sink URL `http://<ingest>/v1/namespaces/{ns}/fabric/topics/{topic}/events`, the CloudEvents structured format) and starts the container (Ruling 6). `ingest` receives Debezium's CloudEvents; `cdc.rs` adds `loamop` (from `op`) and `loamlsn` (Postgres `lsn` / MySQL `file:pos` + `gtid` when present) as extensions in `ingest`'s CDC mode (the route option `cdc = debezium`). A route template creates `<table>_current` (Fluss PK, Versioned on `_loam_lsn_order`, a monotonic numeric derived from the LSN or binlog position) and `<table>_history` (Fluss Log). Monitoring: slot lag (`pg_replication_slots` polled by the flow process with a read-only role), Debezium's metrics endpoint scraped; `flow_cdc_slot_lag_bytes` and `flow_cdc_behind_ms` gauges; an alert rule example.

**Tests:** `pg_cdc_inserts_updates_deletes_converge` (the `_current` table equals the source table after a random workload of 10 000 operations); `mysql_cdc_converges`; `debezium_restart_resumes_without_loss`; `incremental_snapshot_emits_r`; `ddl_add_column_evolves_table`; `incompatible_ddl_pauses_with_error`; `slot_lag_metric_moves`; contract suites.

**Commit:** `connectors: Postgres and MySQL CDC through Debezium Server`.

### Task 9: S3 ★ and Iceberg ★

**Files:** `fabric/crates/loam-flow/src/connectors/{s3.rs,iceberg.rs}`, `connectors/registry/{s3,iceberg}.yaml`, `deploy/fabric/connectors/iggy/{s3_sink,iceberg_sink}.toml.tmpl`, tests, docs.

**Semantics:** **S3 source**: `object_store` listing under a prefix with a high-water key (lexicographic) and, optionally, S3 event notifications through SQS (floci in CI) for low latency; per object either one `object` event (metadata, no data) or decoded rows (Parquet, CSV, NDJSON, Avro by suffix or config) as Arrow batch events; position = the last fully emitted key. **S3 sink**: Iggy's `s3_sink` (config rendered) for raw events; a native Parquet writer for Arrow batch events, files rolled by size (128 MiB) or time (5 min). **Iceberg source**: iceberg-rust incremental scans between snapshots (append-only snapshots; overwrite snapshots are re-scanned whole and flagged), Arrow batch events, position = the last snapshot id. **Iceberg sink**: for Fabric tables, Fluss tiering (nothing to run); for raw topics, Iggy's `iceberg_sink` with Lakekeeper.

**Tests:** `s3_list_resumes_at_high_water_key`; `s3_notifications_low_latency` (floci SQS); `s3_parquet_objects_as_arrow_batches`; `s3_sink_rolls_files`; `iceberg_incremental_append_snapshots`; `iceberg_overwrite_rescans_and_flags`; `kill_restart_*`; contract suites.

**Commit:** `connectors: S3 and Iceberg sources and sinks`.

### Task 10: Elasticsearch ★ and ClickHouse ★

**Files:** `connectors/registry/{elasticsearch,clickhouse}.yaml`, `deploy/fabric/connectors/iggy/{elasticsearch_sink,elasticsearch_source,clickhouse_sink}.toml.tmpl`, `fabric/crates/loam-flow/src/connectors/clickhouse.rs` (source only), tests, docs.

**Semantics:** Elasticsearch through Iggy's `elasticsearch_sink` and `elasticsearch_source` (configs rendered by `IggyRuntime`), checked against both OpenSearch and Loam's own ES gateway (`operon dev`), so a route into "Elasticsearch" can target a Loam collection unchanged. ClickHouse sink through Iggy's `clickhouse_sink`; native ClickHouse source over HTTP with the `clickhouse` crate, `SELECT … FORMAT ArrowStream` in key-range partitions, incremental by a cursor column; tested against the FL2 reference server and against Loam House when FL2 has merged.

**Tests:** `es_sink_to_opensearch_and_loam`; `es_source_scroll_resumes`; `clickhouse_sink_batches`; `clickhouse_source_arrow_partitions`; `clickhouse_source_against_house` (skipped until FL2); contract suites.

**Commit:** `connectors: Elasticsearch and ClickHouse through Iggy plugins and a native ClickHouse source`.

### Task 11: ADBC ★, Snowflake ★ and BigQuery ★

**Files:** `fabric/crates/loam-flow/src/connectors/adbc.rs`, `connectors/registry/{adbc,snowflake,bigquery}.yaml`, `deploy/fabric/adbc/drivers.toml` (names, versions, URLs, SHA-256), tests, docs.

**Semantics:** `adbc_driver_manager` loads an allowed driver (Ruling 9); **source**: `AdbcStatement::execute` → Arrow stream, split by `partitions` where the driver supports `ExecutePartitions` (Snowflake and BigQuery do, verify), else by key ranges; each batch an Arrow event (Ruling 3). **sink**: `bulk_ingest` (`adbc.ingest.target_table`, mode `append` or `create_append`) per Arrow batch, or staged `MERGE` for upserts where the driver supports it (Snowflake `MERGE` through a temporary table; BigQuery through the Storage Write API's default stream plus `MERGE`, verify). The précis' warehouse → Elasticsearch example ships as a route template: ADBC source with a projecting `SELECT` of the profile fields, ES sink.

**Tests:** `adbc_sqlite_and_duckdb_roundtrip` (local drivers, always run); `adbc_postgres_bulk_ingest`; `snowflake_fixture_replay` and `bigquery_fixture_replay` (recorded HTTP fixtures; real accounts nightly when `LOAM_SNOWFLAKE_*`/`LOAM_BIGQUERY_*` are set); `driver_outside_allowlist_is_refused`; `driver_checksum_mismatch_is_refused`; `bulk_path_has_no_row_decode`; `ten_million_rows_within_budget` (Postgres via ADBC into a Fluss Log table; the budget from Task 0); contract suites.

**Commit:** `connectors: ADBC bulk paths for Snowflake, BigQuery and other drivers`.

### Task 12: JDBC ★ through `loam-connect`

**Files:** `connect/**` (YAML route templates, `application.properties`, a compose fragment; no Java), `connectors/registry/jdbc.yaml`, `fabric/crates/loam-flow/src/runtime/camel.rs`, `.github/workflows/fabric.yml` (job `connect-routes`, path-filtered on `connect/**`), tests, docs.

**Semantics:** `loam-connect` is the unmodified Camel 4.22.x runtime (Camel JBang or the official image, Ruling 7: Camel Main mode) with `camel-iggy`, `camel-jdbc`, `camel-sql`, `camel-yaml-dsl`, `camel-cloudevents` and the `jq` language on its classpath, loading YAML routes from `/etc/loam-connect/routes/` with route reloading. **Loam writes no Java** (owner, 2026-10-01). JDBC source route: `timer` → `sql` with a high-water query (the high-water value kept in a Camel `caffeine` or file-backed idempotent repository, verify which survives restarts) → a `jq` transform producing the CloudEvents structured JSON (`type` `io.loams.dev.flow.jdbc.rows.v1`, `id` from the table and key) → `iggy:` producer with the `ce_` headers set by `setHeader` steps. JDBC sink route: `iggy:` consumer → `jq` to row maps → `sql` producer with batch inserts. Drivers mounted (Ruling 8). `CamelRuntime` renders the YAML from `connect/routes/templates/` and checks route status over Camel's health endpoint (verify Camel Main's HTTP health in 4.22). If a capability turns out to need Java (for example exact CloudEvents binary-mode headers), the connector declares the narrower capability and the gap is recorded for when Java is un-deferred.

**Tests:** `jdbc_source_postgres_high_water`; `jdbc_sink_batches`; `route_reload_without_restart`; `camel_iggy_roundtrip` (events written by Camel read back by `loam-fabric` with valid envelopes); `rendered_routes_are_valid_yaml_dsl` (Camel's `camel validate`/JBang check, verify the command); contract suite.

**Commit:** `connect: add loam-connect routes (Camel, YAML only) and the JDBC connector`.

### Task 13: Kinesis ★, Redis ★ and OpenTelemetry ★

**Files:** `fabric/crates/loam-flow/src/connectors/{kinesis.rs,redis.rs,otlp.rs}`, manifests, tests, docs.

**Semantics:** **Kinesis**: shard iterators with checkpointed sequence numbers per shard (position), resharding followed through parent/child shards, `PutRecords` sink with partial-failure retries; floci in CI. **Redis**: Streams source with `XREADGROUP` in group `loam-<instance>` and `XACK` after the Fabric acknowledges; sinks `XADD`, `SET`/`HSET` by key template, `PUBLISH`; Valkey in CI. **OpenTelemetry**: OTLP/HTTP (`/v1/logs`, `/v1/traces`, `/v1/metrics`, protobuf and JSON) and OTLP/gRPC receivers on `ingest`'s listener under `/v1/namespaces/{ns}/fabric/otlp/{instance}/…` (and a plain `:4318`/`:4317` loopback listener bound to one namespace for collectors that cannot set paths); one CloudEvent per log record, span or data point; `partial_success` for invalid records; OTLP logs meant for search still go to the engine's §02 §7.1 endpoint.

**Tests:** `kinesis_resharding_followed`; `kinesis_put_records_partial_failure`; `redis_streams_group_ack_after_fabric`; `redis_sink_templates`; `otlp_http_and_grpc_each_signal`; `otlp_partial_success`; `kill_restart_*`; contract suites.

**Commit:** `connectors: Kinesis, Redis Streams and OpenTelemetry`.

### Task 14: The CN1 gate, catalog page and exit report

**Files:** `fabric/crates/loam-fabric/tests/e2e.rs` (`cn1_*`), `docs/guides/connectors/index.md` (generated from the registry), `docs/plans/cn1-exit-report.md`, `CHANGELOG.md`.

**Semantics:** §33 §9: every ★ manifest validates and passes its suites; the end-to-end CDC test (Postgres → Debezium → `ingest` → Iggy → `fluss_sink` → `orders_current` and `loam_sink` → a Loam collection, with inserts, updates, deletes and a Debezium restart) equals the source; the 10 M-row ADBC test within budget. The catalog page lists all 200 connectors by category with status and runtime, generated by `loam-fabric connectors gen --docs`. The exit report: per connector, throughput and latency measured, duplicate counts in kill tests, gaps found upstream (Iggy plugins, Camel, Debezium) and the issues opened.

**Tests:** `cn1_cdc_end_to_end`; `cn1_adbc_bulk_end_to_end`; `catalog_page_matches_registry`.

**Commit:** `connectors: the CN1 gate, the catalog page and the exit report`.

## PR grouping

| PR | Tasks | Title | Size (estimate) |
|---|---|---|---|
| A | 0 | CN1 (1/11): dependency spike | docs only |
| B | 1 | CN1 (2/11): manifests and validation | ~1 000 lines |
| C | 2 | CN1 (3/11): the registry data and drift checks | ~800 lines + 200 manifests |
| D | 3 | CN1 (4/11): instances, FlowService, runtimes, contract kit | ~1 600 lines |
| E | 4 | CN1 (5/11): HTTP and webhooks | ~900 lines |
| F | 5 | CN1 (6/11): Parquet, Avro, Arrow | ~800 lines |
| G | 6 | CN1 (7/11): Kafka | ~800 lines |
| H | 7, 8 | CN1 (8/11): Postgres, MySQL, CDC through Debezium | ~1 500 lines |
| I | 9, 10 | CN1 (9/11): S3, Iceberg, Elasticsearch, ClickHouse | ~1 300 lines |
| J | 11, 12 | CN1 (10/11): ADBC (Snowflake, BigQuery) and JDBC via `loam-connect` | ~1 200 lines Rust + YAML route templates |
| K | 13, 14 | CN1 (11/11): Kinesis, Redis, OTLP, the gate and the catalog | ~1 300 lines |

## Rulings made during execution

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| — | (Task 0 fills this table) | | |
