# 33 — Loam Flow Connectors: Registry, Capabilities and the Catalog

Status: **Proposed** · 2026-10-01. Source: the owner's drafts "Précis (CDMP, Java stack)", "Top 200 connectors" and "Rollout" (`chatdump.md` lines 551–628), read as the design of Loam Flow's connector registry, and the owner's direction of 2026-10-01 that event ingestion runs on Apache Iggy and Apache Fluss ([§32](32-loam-flow-fabric-house.md) D331–D333). This document makes decisions **D352–D359** and asks **Q348–Q359**. It depends on §32 for the envelope (D334), the bridges (D336), Flow routes (D337–D339) and the `fabric/` workspace (D343). **No code is written by this document.**

Markers are §32's: **(verify)**, **(estimate)**, **(read 2026-10-01)**. "The précis" is the draft's "Précis (CDMP, Java stack)"; "CDMP" there is the draft's name for the canonical event contract, which in Loam is CloudEvents 1.0 (D334).

---

## 1. Summary

| # | Decision | Status |
|---|---|---|
| D352 | **A connector registry in `loam-flow`**: one versioned manifest per connector (`connectors/registry/<id>.yaml`, schema `loam.flow.v1.ConnectorSpec`), loaded at start, served by `FlowService.ListConnectors`/`DescribeConnector`, and checked by CI (schema, licence, capability tests). A connector **instance** (credentials, endpoints, tables) is a namespace object; a route (§32 D337) references instances | Proposed |
| D353 | **Every connector declares its capabilities** (§4): direction (source, sink), modes (streaming, batch, CDC, webhook, request-reply), delivery per direction, transactional and upsert/delete support, ordering, formats, schema handling, bulk Arrow support, backpressure, auth methods, config schema, secret fields and limits. A route that uses an undeclared capability is refused; a connector's contract tests prove each declared capability (the précis' change 1) | Proposed |
| D354 | **Runtimes, buy first** (§5): **native Rust** connectors in `loam-flow` for the hot path where Loam has the code or a good crate exists; **Iggy's connectors runtime** (Rust plugins) where Iggy has the connector; **Apache Camel** 4.22 for the long tail, run unmodified in a JVM service `loam-connect` whose routes end in `camel-iggy`; **Debezium Server** 3.7 (unmodified) for CDC of databases Loam does not bridge itself. **Kestra is not a runtime**: Resonate is Loam's orchestrator (D210), and Kestra is a companion with a Loam plugin (Q356). Kafka Connect arrives only through Iggy's Kafka gateway (§32 Q331) | Proposed |
| D355 | **One envelope and one delivery contract for every connector** (the précis' change 3): sources emit CloudEvents 1.0 with `type` = `dev.loam.flow.<connector>.<event>.v1`, `source` = `/connectors/<instance>/<resource>`, and an `id` stable across re-reads, so D334's dedup and PK tables make re-delivery harmless; sinks consume CloudEvents and deliver at least once with `ce_id` as the idempotency key | Proposed |
| D356 | **Bulk data moves as Arrow, never row by row through Camel** (the précis' "control plane vs data plane"): batches of 65 536 rows (default), partitioned by a declared key, written by parallel tasks; **ADBC** (`adbc_driver_manager` 0.24 with the Apache-2.0 Snowflake, BigQuery, Postgres, SQLite, Flight SQL and DuckDB drivers) for warehouse reads and bulk loads; native drivers for OLTP upserts; JDBC only inside `loam-connect`; **Avro only at Kafka and schema-registry boundaries**, Arrow inside | Proposed |
| D357 | **CDC is observed, not reinvented** (§7): Loam Postgres and WeSQL changes come from Loam's own bridges (D154, §29 D279); external Postgres, MySQL, MariaDB, SQL Server, Oracle, Db2 and MongoDB go through **Debezium Server** with its HTTP sink posting CloudEvents (Debezium's CloudEvents converter) to `loam-fabric ingest`; Iggy's `postgres_source` (logical replication) is the lighter option for Postgres. Current-state tables are Fluss PK tables (Versioned on the source LSN); history is the Iceberg log table; SCD2 is a House view. No triggers; replication-slot lag is monitored and alerted | Proposed |
| D358 | **The ★ set is 21 Loam-owned hot-path connectors**, each with a fixed runtime (§8), shipped in **CN1** in the précis' rollout order (Kafka, PostgreSQL, MySQL, Debezium, S3, Iceberg, Parquet/Arrow/Avro, Elasticsearch, ClickHouse, Snowflake and BigQuery via ADBC, HTTP/Webhooks, JDBC), then Kinesis, Redis and OpenTelemetry. Phase 2 (CN2) is the remaining P2 connectors through stock Camel and Iggy plugins; phase 3 (CN3) is the long tail through OpenAPI-generated connectors | Proposed |
| D359 | **A licence gate per connector**: each manifest names the licence of its runtime component and of every library or driver it loads; CI refuses AGPL, BSL, SSPL, ELv2 and unlicensed dependencies (D11) for anything Loam ships or runs by default; services that are used through their public API (SaaS) are not a licence question. **Airbyte** (ELv2 platform, mixed connector licences) and **Redpanda Connect** (Redpanda Community License on part of its connectors) are not runtimes | Proposed |

## 2. Goals and non-goals

### 2.1 Goals

1. **Breadth without writing hundreds of adapters** (the précis' first line): count unique connectors, reuse Camel's 300-plus components, Iggy's plugins and Debezium, and own only the hot path.
2. **Honest capabilities**: a user can see, before running a route, whether a connector is a source or sink, streaming or batch, transactional or not, and which auth it takes.
3. **One envelope** (D355) and **one bulk path** (D356).
4. **Testable**: every declared capability has a contract test; the starred set has end-to-end tests against real services in CI.

### 2.2 Non-goals

- **No Loam-written connector where a maintained Apache-2.0 one exists** in Iggy, Camel or Debezium.
- **No JVM in the engine or in `loam-fabric`**: Camel and Debezium run in their own pods.
- **No orchestration layer of Loam's own**: schedules, retries and batch flows around connectors are Resonate workflows (D210); Kestra users keep Kestra.
- **No exactly-once claims to external systems** that cannot deduplicate.

## 3. Reconciliation with the précis

| Précis | Loam |
|---|---|
| "Camel is the integration layer, Kestra the orchestration layer, CDMP the canonical event contract; everything is Java" | Camel is the long-tail runtime (D354), Resonate the orchestrator (D210), CloudEvents the contract (D355); Rust where Loam owns the hot path; Java only in `loam-connect` and Debezium Server |
| "Camel and Kestra as one catalog; count unique connectors" | The registry is the one catalog (D352); Camel and Kestra columns in Appendix A show who else covers each connector |
| "Control plane vs data plane: partitioned Arrow batches of ~64K rows" | D356 |
| "ADBC/Arrow for warehouses; JDBC or native drivers for OLTP; this includes the MySQL control plane" | D356; Loam's own control plane is TiKV (D260), not MySQL |
| "Arrow internal, Avro at Kafka and serialization boundaries" | D356 |
| "WAL/binlog → Debezium → Kafka → Iceberg history + current-state table, SCD2" | D357: Debezium → `loam-fabric ingest` (or Iggy's Kafka gateway later) → Iggy → Fluss PK (current) + Log (history) → Iceberg; SCD2 as a House view |
| "Warehouse → Elasticsearch: project ~15 fields, then bulk-ingest" | A route with a `map` step projecting the fields, an ADBC source and the Elasticsearch sink (or a Loam collection, which speaks the ES API) |
| Change 1: per-connector capability declarations | D353 |
| Change 2: only ★ connectors get owned wrappers | D358 |
| Change 3: every connector emits and consumes the canonical envelope | D355 |

## 4. The capability schema (D353)

```yaml
apiVersion: loam.flow/v1
kind: Connector
id: kafka                          # [a-z0-9-]{1,48}, unique
name: Apache Kafka
specVersion: 1.0.0                 # of this manifest; bumps on capability changes
category: messaging                # messaging | relational | nosql | search | vector | graph | warehouse | lakehouse
                                   # | object-storage | format | cdc | integration | saas | observability | infra | identity | ai | protocol
priority: P1                       # P1 = CN1 (★), P2 = CN2, P3 = CN3
starred: true
status: preview                    # planned | preview | stable | deprecated
runtime:
  kind: native                     # native | iggy | camel | debezium | openapi
  ref: loam_flow::connectors::kafka   # crate path | Iggy plugin name | Camel URI scheme | Debezium connector class | OpenAPI spec URL
  version: "rdkafka 0.39 / librdkafka 2.x (verify)"
licence:
  component: Apache-2.0
  dependencies: { librdkafka: BSD-2-Clause }
capabilities:
  source:
    streaming: true
    batch: false
    cdc: false
    webhook: false
    resumable: true                # restarts from a committed position
    position: kafka-offsets        # what the source checkpoints
  sink:
    streaming: true
    batch: true
    transactional: false
    upsert: false
    delete: false
    idempotent: true               # the sink itself dedupes retries (Kafka idempotent producer)
  delivery: { source: at_least_once, sink: at_least_once }
  ordering: per_partition          # none | per_key | per_partition | total
  formats: [cloudevents-binary, cloudevents-structured, json, avro, protobuf, bytes]
  schema: { registry: optional, evolution: backward }
  bulk: { arrow: false, max_batch_rows: 65536 }
  backpressure: pull               # pull | push-with-ack | push-rate-limited
auth: [none, sasl-plain, sasl-scram-256, sasl-scram-512, mtls, aws-msk-iam]
config:                            # JSON Schema 2020-12 for an instance's settings
  $ref: schemas/kafka.config.json
secrets: [sasl.password, tls.key_pem]   # resolved through Dapr secret stores (D189); never stored in the instance
envelope:
  emits: dev.loam.flow.kafka.record.v1
  consumes: "*"
limits: { max_record_bytes: 16777216 }
conformance: [contract, roundtrip, kill-restart, dup-check]
docs: docs/guides/connectors/kafka.md
```

Rules:

1. **Refusal.** `ValidateRoute` refuses a route whose `from` connector lacks the source mode it uses (for example `cdc` on a polling source), whose `to` connector lacks `upsert` when the route upserts, or whose delivery asks for more than the connector declares. The error names the connector, the capability and the manifest version.
2. **Runtime-specific truth.** The same system can have different capabilities per runtime (Camel's producer/consumer support, Iggy's source/sink plugins, Kestra's task/trigger split differ); the manifest describes the runtime Loam ships, and Appendix A shows the others.
3. **Tests per capability.** `conformance` lists the suites the connector passes: `contract` (each declared capability exercised), `roundtrip` (sink → source returns the events, where both exist), `kill-restart` (no loss after killing the runtime mid-batch), `dup-check` (duplicates bounded by the declared delivery), `bulk` (Arrow path at 1 M rows), `cdc` (inserts, updates, deletes, DDL, slot or binlog resume).
4. **Versioning.** A capability removed or narrowed bumps `specVersion`'s major; routes pinned to the old major keep running until migrated, and `ListConnectors` shows both.

The protobuf form (`proto/loam/flow/v1/connector.proto`, FL3/CN1) mirrors the YAML one to one; the YAML is the source and CI checks that both agree.

## 5. Runtimes (D354)

| Runtime | What runs | Where | Supervised by | Used for |
|---|---|---|---|---|
| **native** | Rust connector tasks in `loam-fabric flow` (`loam_flow::connectors::*`), leased per instance and partition | `loam-fabric` pods | `loam-flow` task leases (the worker-lease model of §09 §6, over the metastore's network API) | ★ hot path: Kafka, Kinesis, webhooks, HTTP, S3, Iceberg, formats, ADBC (Snowflake, BigQuery, Postgres bulk), MySQL/Postgres batch, Redis, OTLP |
| **iggy** | Iggy's connectors runtime (`iggy-connectors`) with Rust plugins, configured from a generated TOML per instance | its own pods, one runtime per namespace group | `loam-flow` (renders the config, restarts on change, reads its metrics) | Sinks Iggy already has (Elasticsearch, ClickHouse, Postgres, S3, Iceberg, Delta, Doris, InfluxDB, MongoDB, Meilisearch, Quickwit, RabbitMQ, Redshift, HTTP) and the Loam plugins `fluss_sink`, `loam_sink` (§32 D336) |
| **camel** | **`loam-connect`**: unmodified Apache Camel 4.22 (Camel Main or Camel Quarkus, decided in CN2 Task 0) running generated YAML-DSL routes, `<component> → camel-iggy` for sources and `camel-iggy → <component>` for sinks, with Kamelets as the parameter templates | its own pods (JVM), one per namespace group or per heavy instance | `loam-flow` (renders routes, applies them through Camel's route-reload or a restart) | The long tail (P2/P3) and JDBC ★ |
| **debezium** | Debezium Server 3.7.0.Final (unmodified), one per source database, HTTP sink → `loam-fabric ingest`, offsets in its file or Redis store on a PVC (verify the Iggy- or Fluss-backed offset store options) | its own pods | `loam-flow` (renders `application.properties`) | CDC of external databases (D357) |
| **openapi** | A native Rust connector generated from an OpenAPI 3.x spec (polling source with cursors, request sink), with a hand-written manifest for capabilities | `loam-fabric` | as native | CN3's long tail of SaaS APIs |

Why these and not others:

| Candidate | Licence | Verdict |
|---|---|---|
| Apache Camel 4.22.1 (319 component modules, including `camel-iggy` since 4.17, `camel-clickhouse` 4.22, `camel-cloudevents`, `camel-dapr`, `camel-debezium-*`) | Apache-2.0 | **Runtime** for the long tail |
| Iggy connectors runtime (`iggy_connector_sdk` 0.5.0, not published to crates.io) | Apache-2.0 | **Runtime**; Loam's plugins go upstream |
| Debezium 3.7.0.Final and Debezium Server (sinks include HTTP, Kafka, Kinesis, Pub/Sub, Pulsar, Redis, NATS, RabbitMQ, Fluss, Qdrant, Milvus, …) | Apache-2.0 | **Runtime** for CDC |
| Kestra 2.0.4 (about 190 plugin repositories) | Apache-2.0 core; Enterprise Edition proprietary | **Companion**, not a runtime: its scheduler, retries and flow state duplicate Resonate (D210, §26), and it needs its own database. Its catalog is a cross-check in Appendix A |
| Apache Camel Karavan 4.18.1 | Apache-2.0 | Designer reference for the Flow UI (§32 D340) |
| Kafka Connect | Apache-2.0 | Through Iggy's Kafka gateway when it supports consumer groups with offsets (§32 Q331) |
| Airbyte | ELv2 platform; connectors under mixed licences | **Rejected** (D359); its open connectors may be read as API references |
| Redpanda Connect | Apache-2.0 core plus the Redpanda Community License on part of the connectors | **Rejected** as a runtime (D359) |
| Fluvio connectors, Vector | Apache-2.0, MPL-2.0 | Not needed: narrower than Iggy's plugins plus Camel |

## 6. Envelope and delivery (D355)

- **Source events.** One CloudEvent per record, row change or file. `id` is derived from the source position so a re-read produces the same id: Kafka `"<topic>/<partition>/<offset>"`; Debezium the connector's `source.lsn`/binlog position plus table and key; S3 `"<bucket>/<key>@<etag>"`; ADBC batch reads `"<query-hash>/<partition>/<row-range>"` for batch-of-rows events (`datacontenttype: application/vnd.apache.arrow.stream`). `subject` is the table, key or path. Extensions: `loamconnector`, `loaminstance`, and for CDC `loamop` (`c`, `u`, `d`, `r`) and `loamlsn`.
- **Sinks.** A sink receives CloudEvents (or Arrow batches for bulk sinks) and maps them per its manifest; HTTP sinks send `ce-*` headers and `Idempotency-Key: <ce_id>`; database sinks upsert by the declared key when `upsert: true`.
- **Positions.** A native source commits its position after Iggy (or `ingest`) acknowledges the batch; Iggy plugins commit Iggy offsets after the sink acknowledges; Debezium commits its offsets after the HTTP sink's 2xx. Every runtime is therefore at-least-once, and D334's dedup key or the target's PK makes it effectively once where the target dedupes.

## 7. CDC (D357)

```
Postgres / MySQL / SQL Server / Oracle / Db2 / MongoDB
   │ logical replication · binlog · CDC tables · change streams
   ▼
Debezium Server 3.7 (CloudEvents converter; HTTP sink)  ──or──  Iggy postgres_source (CDC mode)
   │ POST /v1/namespaces/{ns}/fabric/topics/{topic}/events   (structured or batched CloudEvents)
   ▼
loam-fabric ingest (dedup by source + id)  →  Iggy topic <db>.<schema>.<table>
   │ fluss_sink
   ▼
Fluss PK table <table>_current (Versioned on loamlsn; deletes on loamop = d)   +   Fluss Log table <table>_history
   │ tiering
   ▼
Iceberg (current + history)  →  House: SELECT … FINAL, SCD2 view over history (valid_from = ts, valid_to = next ts per key)
```

- Loam Postgres (§28) and WeSQL (§29) use Loam's own change bridges (D154) into collections; a route can forward those changes to the Fabric through the `iggy` link target (§32 D336).
- **Snapshots** use Debezium's incremental snapshots (signal table), which emit `loamop = r`.
- **Monitoring**: replication-slot lag in bytes and seconds, Debezium's `MilliSecondsBehindSource`, and an alert when retained WAL passes a threshold (default 10 GiB, (estimate)), because an abandoned slot fills the source's disk.
- **Schema changes** flow as Debezium schema-change events into the DLQ-free `<db>.schema_changes` topic; the route either evolves the Fluss table (additive) or pauses with an error (incompatible), as links do (§09 §4).

## 8. The ★ set (D358)

| # | Connector | Direction | Runtime and implementation | CN1 task |
|---|---|---|---|---|
| 1 | Kafka | source, sink | native: `rdkafka` 0.39 (MIT; librdkafka BSD-2-Clause, static build) with consumer groups; sink with the idempotent producer | 6 |
| 2 | PostgreSQL | source (batch), sink (upsert) | native: `tokio-postgres` `COPY … TO STDOUT (FORMAT binary)` → Arrow for batch reads, ADBC Postgres driver for bulk loads; Iggy `postgres_sink` for streaming sinks | 7 |
| 3 | MySQL | source (batch), sink (upsert) | native: `mysql_async` (MIT/Apache-2.0) chunked `SELECT` by key range, `INSERT … ON DUPLICATE KEY UPDATE` batches | 7 |
| 4 | Debezium-Postgres | source (CDC) | debezium: Debezium Server → `ingest` | 8 |
| 5 | Debezium-MySQL | source (CDC) | debezium: Debezium Server → `ingest` | 8 |
| 6 | S3 | source, sink | native: `object_store` 0.14 listing with a high-water key and SQS/S3-event notifications; sink through Iggy `s3_sink` or native Parquet writer | 9 |
| 7 | Iceberg | source, sink | native source: iceberg-rust 0.10 incremental snapshot scans; sink: Fluss tiering for Fabric tables, Iggy `iceberg_sink` for raw topics | 9 |
| 8 | Parquet | format | native: `parquet` 58/59 | 5 |
| 9 | Avro | format | native: `apache-avro` (Apache-2.0); Confluent wire format (magic byte + schema id) when a registry is configured | 5 |
| 10 | Arrow IPC / Flight | format, source, sink | native: `arrow-ipc`, `arrow-flight` (Flight `DoGet` source, `DoPut` sink, including Loam's own Flight SQL) | 5 |
| 11 | Elasticsearch | source, sink | iggy: `elasticsearch_source`, `elasticsearch_sink` (also reaches Loam collections through their ES API) | 10 |
| 12 | ClickHouse | source, sink | iggy: `clickhouse_sink`; native source over HTTP with `clickhouse` (Apache-2.0) in `ArrowStream` format; Loam House is itself a ClickHouse endpoint | 10 |
| 13 | Snowflake | source, sink (bulk) | native: ADBC Snowflake driver (Apache-2.0, loaded by `adbc_driver_manager` 0.24) | 11 |
| 14 | BigQuery | source, sink (bulk) | native: ADBC BigQuery driver (Apache-2.0) | 11 |
| 15 | ADBC (generic) | source, sink (bulk) | native: any ADBC driver the namespace's admin allows (Flight SQL, SQLite, DuckDB, Postgres) | 11 |
| 16 | HTTP/REST | source (polling), sink | native: `reqwest` sink with retries and `Idempotency-Key`; polling source with cursors (Iggy `http_source` as an alternative) | 4 |
| 17 | Webhooks | source | native: `loam-fabric ingest` signed-webhook routes (HMAC SHA-256; GitHub, Stripe, Slack, Shopify schemes) | 4 |
| 18 | JDBC | source, sink | camel: `jdbc`/`sql` components in `loam-connect`; bulk reads through Arrow's JDBC adapter (verify the Java packaging) | 12 |
| 19 | Kinesis | source, sink | native: `aws-sdk-kinesis` (Apache-2.0), enhanced fan-out optional | 13 |
| 20 | Redis | source (Streams), sink | native: `redis` crate (BSD-3-Clause) with consumer groups on Streams; `SET`/`HSET`/`XADD` sinks | 13 |
| 21 | OpenTelemetry | source | native: OTLP/HTTP and OTLP/gRPC receiver in `loam-fabric ingest`, one CloudEvent per log record, span or metric point (`type` `dev.loam.otel.<signal>.v1`) | 13 |

Rollout: **CN1** the 21 above; **CN2** every P2 row of Appendix A through stock Camel components (`loam-connect`) or Iggy plugins, each with a manifest and contract tests; **CN3** the P3 rows, most through OpenAPI-generated native connectors. CN2 and CN3 are not planned yet.

## 9. CN1 exit gate

- Every ★ connector has a manifest that validates, a licence that passes D359, and its `conformance` suites green in CI against real services (containers for Kafka, Postgres, MySQL, Debezium, RustFS as S3, Lakekeeper, Elasticsearch, ClickHouse, Redis, LocalStack-free Kinesis through floci (D60), and recorded fixtures for Snowflake and BigQuery plus a nightly job against real accounts when credentials exist).
- End to end: Postgres CDC through Debezium into a Fluss PK table and a Loam collection, with inserts, updates, deletes and a Debezium restart, equals the source table; a 10 M-row Snowflake (fixture) or Postgres read through ADBC lands as Arrow batches in a Fluss Log table in under the budget measured in CN1 Task 0.
- `loam-fabric connectors list|describe|validate` and the generated catalog page `docs/guides/connectors/index.md`.

## 10. Where it lives (open core)

All of §33 is open source (D220): self-hosters need connectors. `loam-connect` (Java) lives in this repository under `connect/` with its own Maven build and a path-filtered CI job, or in its own repository (Q353). `loam-platform` adds the managed fleet: per-tenant connector pods, autoscaling, plan limits on connector count and throughput, the hosted secrets UI, connector metering (through the usage hooks, §27).

## 11. Risks

| # | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| CN-R1 | Capabilities in Appendix A drift from what the runtime really does | High | Medium | Capabilities are proven by contract tests (D353 rule 3); the Camel and Kestra columns are regenerated from their catalogs (CN1 Task 2) |
| CN-R2 | Camel's JVM footprint per tenant | Medium | Medium | `loam-connect` per namespace group; Camel Quarkus native images evaluated in CN2 |
| CN-R3 | Debezium Server per database multiplies pods | Medium | Low | Iggy `postgres_source` for small Postgres sources; one Debezium Server can host several connectors only through Kafka Connect, which waits for Q331 |
| CN-R4 | ADBC drivers are shared libraries built in Go or C++ that must match the platform | Medium | Medium | Pinned driver builds with SHA-256 in the image; a load test per platform (Q349) |
| CN-R5 | `camel-iggy` is a Preview component | Medium | Medium | Contract tests on every Camel bump; contribute fixes upstream; native fallback for P2 connectors that matter |
| CN-R6 | SaaS APIs change and rate-limit | High | Low | OpenAPI-generated connectors are regenerated; rate limits declared in manifests |
| CN-R7 | Secrets leak through connector logs or configs | Low | High | Secrets only through Dapr secret stores (D189), redacted in rendered configs; a test greps rendered configs for secret values |

## 12. Open questions

| # | Question | Owner | Needed by |
|---|---|---|---|
| Q348 | `rdkafka` (librdkafka, C build) or a pure-Rust client for the Kafka ★ connector; `rskafka` 0.6 lacks consumer groups | Eng | CN1 Task 0 |
| Q349 | Shipping ADBC drivers (Snowflake and BigQuery are Go builds, verify) inside the `loam-fabric` image: licences, NOTICE and platform builds | Eng | CN1 Task 0 |
| Q350 | Debezium Server's offset and schema-history stores: file on a PVC, Redis, or a Fluss/Iggy-backed store contributed upstream | Eng | CN1 Task 8 |
| Q351 | Iggy `postgres_source` (CDC mode) as the default for Postgres, with Debezium for the rest, or Debezium everywhere for one behaviour | Eng | CN1 Task 8 |
| Q352 | Camel Main or Camel Quarkus (JVM or native) for `loam-connect` | Eng | CN2 Task 0 |
| Q353 | `loam-connect`'s home: `connect/` in this repository, or its own repository | Founder | CN1 Task 12 |
| Q354 | Instance credentials: per namespace through Dapr secret components (D189), or Loam-vended short-lived credentials where the provider supports them (AWS STS, GCP WIF) | Eng | Unified auth plan |
| Q355 | Which P2 connectors move into CN1 if a launch customer needs them | Founder | CN1 start |
| Q356 | Publish a Kestra plugin for Loam (tasks for House queries, Fabric produce, route control) so Kestra users can drive Loam | Founder | After CN1 |
| Q357 | Contribute Loam's native connectors (Kafka, ADBC, Kinesis) to Iggy's connectors runtime as plugins, so one Rust connector set serves both | Founder | After CN1 |
| Q358 | OpenAPI-generated connectors (CN3): the generator (`progenitor`, Apache-2.0/MIT, or `openapi-generator`), and how cursors and webhooks are declared beside the spec | Eng | CN3 plan |
| Q359 | Connector metering hooks (§27): which counters (events, bytes, API calls) the platform needs per instance | Founder | Before the cloud beta |

## 13. Sources

All read on 2026-10-01: `github.com/apache/camel` tag `camel-4.22.1` (`components/` and the `camel-aws`, `camel-azure`, `camel-google`, `camel-debezium`, `camel-ai`, `camel-salesforce` module lists; `components/camel-iggy/src/main/docs/iggy-component.adoc`; `components/camel-clickhouse/src/main/docs/clickhouse-component.adoc`); `github.com/kestra-io` plugin repositories (193 `plugin-*` repositories; module lists of `plugin-jdbc`, `plugin-aws`, `plugin-gcp`, `plugin-azure`, `plugin-notifications`, `plugin-serdes`, `plugin-fs`); `github.com/kestra-io/kestra` v2.0.4 README; `github.com/debezium/debezium` v3.7.0.Final and `github.com/debezium/debezium-server` (module list); `github.com/apache/iggy` `core/connectors/{sdk,sinks,sources}` and `postgres_source/README.md`; crates.io: `adbc_core` and `adbc_driver_manager` 0.24.0 (arrow ≥ 58, < 60), `rdkafka` 0.39.0, `rskafka` 0.6.0, `sqlparser` 0.63.0; `github.com/adbc-drivers/{snowflake,bigquery}` (Apache-2.0); `github.com/supabase/etl` (Apache-2.0, a Rust Postgres replication library, a candidate for Q351); `github.com/airbytehq/airbyte` (licence `NOASSERTION`, ELv2 platform); `github.com/redpanda-data/connect` (no single SPDX licence). Loam: §02 §7.4, §09, §21, §24 (D184, D189), §26 (D210), §27, §28, §29, §32, `docs/open-core.md`.

---

## Appendix A. The connector matrix

Columns: **Source / Sink** — the connector reads from / writes to the system. **Streaming** — continuous, low-latency delivery. **Batch** — bulk or scheduled transfer. **CDC** — row-level change capture. **Webhook** — receives (as a source) or sends (as a sink) webhooks. **Auth** — the methods the Loam manifest will declare: `none`, `basic` (user and password), `key` (API key or token), `oauth2`, `jwt`, `hmac` (signed payloads), `mtls`, `sasl` (PLAIN/SCRAM), `iam` (AWS), `sa` (GCP service account or workload identity), `aad` (Entra ID), `ssh`, `kerb` (Kerberos), `cs` (connection string). **Camel** — the Camel 4.22.1 component(s) that cover it, or `·`. **Kestra** — the Kestra plugin (and sub-module) that covers it, or `·`. **Priority** — P1 = ★ (CN1), P2 = CN2 (stock Camel or Iggy plugins), P3 = CN3 (long tail, OpenAPI-generated). `Y` = yes, `·` = no.

The Camel and Kestra columns were filled from the component and plugin lists read on 2026-10-01; the capability cells are the planned manifest values and are **(verify)** until each connector's contract tests pass (D353 rule 3). CN1 Task 2 generates this table from the registry and fails CI if they disagree.

### A.1 Messaging and streaming (20)

| Connector | Source | Sink | Streaming | Batch | CDC | Webhook | Auth | Camel | Kestra | Priority |
|---|---|---|---|---|---|---|---|---|---|---|
| ★ Kafka | Y | Y | Y | · | · | · | sasl, mtls, iam | `kafka` | `plugin-kafka` | P1 |
| Redpanda | Y | Y | Y | · | · | · | sasl, mtls | `kafka` | `plugin-kafka` | P2 |
| RabbitMQ / AMQP | Y | Y | Y | · | · | · | basic, mtls | `amqp` | `plugin-amqp` | P2 |
| ActiveMQ / JMS | Y | Y | Y | · | · | · | basic, mtls | `activemq`, `jms`, `sjms2` | `plugin-jms` | P2 |
| NATS | Y | Y | Y | · | · | · | key, jwt, mtls | `nats` | `plugin-nats` | P2 |
| Pulsar | Y | Y | Y | · | · | · | jwt, oauth2, mtls | `pulsar` | `plugin-pulsar` | P2 |
| MQTT | Y | Y | Y | · | · | · | basic, mtls | `paho-mqtt5`, `paho` | `plugin-mqtt` | P2 |
| AWS SQS | Y | Y | Y | · | · | · | iam | `aws2-sqs` | `plugin-aws` (sqs) | P2 |
| AWS SNS | · | Y | Y | · | · | Y | iam | `aws2-sns` | `plugin-aws` (sns) | P2 |
| ★ Kinesis | Y | Y | Y | · | · | · | iam | `aws2-kinesis` | `plugin-aws` (kinesis) | P1 |
| EventBridge | Y | Y | Y | · | · | Y | iam | `aws2-eventbridge` | `plugin-aws` (eventbridge) | P2 |
| Google Pub/Sub | Y | Y | Y | · | · | Y | sa | `google-pubsub` | `plugin-gcp` (pubsub) | P2 |
| Azure Event Hubs | Y | Y | Y | · | · | · | aad, cs | `azure-eventhubs` | `plugin-azure` (eventhubs) | P2 |
| Azure Service Bus | Y | Y | Y | · | · | · | aad, cs | `azure-servicebus` | `plugin-azure` (servicebus) | P2 |
| Azure Event Grid | Y | Y | Y | · | · | Y | aad, key | `azure-eventgrid` | · | P2 |
| Redis Streams | Y | Y | Y | · | · | · | basic, mtls | `redis` | `plugin-redis` | P2 |
| WebSocket | Y | Y | Y | · | · | · | key, jwt | `vertx-websocket`, `atmosphere-websocket` | · | P2 |
| SSE | Y | · | Y | · | · | · | key, jwt | · | · | P3 |
| ★ Webhooks | Y | Y | Y | · | · | Y | hmac, key | `webhook`, `platform-http` | core (Webhook trigger) | P1 |
| gRPC | Y | Y | Y | · | · | · | mtls, jwt | `grpc` | · | P2 |

### A.2 Relational (16)

| Connector | Source | Sink | Streaming | Batch | CDC | Webhook | Auth | Camel | Kestra | Priority |
|---|---|---|---|---|---|---|---|---|---|---|
| ★ PostgreSQL | Y | Y | Y | Y | Y | · | basic, mtls, iam | `sql`, `jdbc`, `pg-replication-slot`, `debezium-postgres` | `plugin-jdbc` (postgres), `plugin-debezium` | P1 |
| ★ MySQL | Y | Y | Y | Y | Y | · | basic, mtls, iam | `sql`, `jdbc`, `debezium-mysql` | `plugin-jdbc` (mysql), `plugin-debezium` | P1 |
| MariaDB | Y | Y | Y | Y | Y | · | basic, mtls | `sql`, `jdbc` | `plugin-jdbc` (mariadb), `plugin-debezium` | P2 |
| SQL Server | Y | Y | Y | Y | Y | · | basic, aad | `sql`, `jdbc`, `debezium-sqlserver` | `plugin-jdbc` (sqlserver), `plugin-debezium` | P2 |
| Oracle | Y | Y | Y | Y | Y | · | basic, kerb | `sql`, `jdbc`, `debezium-oracle` | `plugin-jdbc` (oracle), `plugin-debezium` | P2 |
| SQLite | Y | Y | · | Y | · | · | none | `sql`, `jdbc` | `plugin-jdbc` (sqlite) | P3 |
| CockroachDB | Y | Y | Y | Y | Y | · | basic, mtls | `sql`, `jdbc` | `plugin-jdbc` (postgres) | P2 |
| TiDB | Y | Y | Y | Y | Y | · | basic, mtls | `sql`, `jdbc` | `plugin-jdbc` (mysql) | P3 |
| YugabyteDB | Y | Y | Y | Y | Y | · | basic, mtls | `sql`, `jdbc` | `plugin-jdbc` (postgres) | P3 |
| Aurora | Y | Y | Y | Y | Y | · | basic, iam | `sql`, `jdbc`, `debezium-postgres`, `debezium-mysql` | `plugin-jdbc` | P2 |
| AlloyDB | Y | Y | Y | Y | Y | · | basic, sa | `sql`, `jdbc`, `debezium-postgres` | `plugin-jdbc` (postgres) | P3 |
| Azure SQL | Y | Y | Y | Y | Y | · | basic, aad | `sql`, `jdbc`, `debezium-sqlserver` | `plugin-jdbc` (sqlserver) | P2 |
| Cloud SQL | Y | Y | Y | Y | Y | · | basic, sa | `sql`, `jdbc` | `plugin-jdbc` | P3 |
| Db2 | Y | Y | Y | Y | Y | · | basic | `sql`, `jdbc`, `debezium-db2` | `plugin-jdbc` (db2) | P3 |
| SingleStore | Y | Y | · | Y | · | · | basic | `sql`, `jdbc` | `plugin-jdbc` (mysql) | P3 |
| Vitess | Y | Y | Y | Y | Y | · | basic, mtls | `sql`, `jdbc` | `plugin-jdbc` (mysql) | P3 |

### A.3 NoSQL, search, vector and graph (20)

| Connector | Source | Sink | Streaming | Batch | CDC | Webhook | Auth | Camel | Kestra | Priority |
|---|---|---|---|---|---|---|---|---|---|---|
| MongoDB | Y | Y | Y | Y | Y | · | basic, mtls | `mongodb`, `debezium-mongodb` | `plugin-mongodb` | P2 |
| Cassandra | Y | Y | · | Y | · | · | basic, mtls | `cql` | `plugin-cassandra` | P2 |
| ScyllaDB | Y | Y | · | Y | · | · | basic, mtls | `cql` | `plugin-scylladb` | P3 |
| DynamoDB | Y | Y | Y | Y | Y | · | iam | `aws2-ddb`, `aws2-ddbstream` | `plugin-aws` (dynamodb) | P2 |
| ★ Redis | Y | Y | Y | Y | · | · | basic, mtls | `redis` | `plugin-redis` | P1 |
| Valkey | Y | Y | Y | Y | · | · | basic, mtls | `redis` | `plugin-redis` | P2 |
| Couchbase | Y | Y | Y | Y | · | · | basic | `couchbase` | `plugin-couchbase` | P3 |
| Firestore | Y | Y | Y | Y | · | · | sa | `google-firestore` | `plugin-gcp` (firestore) | P3 |
| Cosmos DB | Y | Y | Y | Y | Y | · | aad, key | `azure-cosmosdb` | · | P3 |
| ★ Elasticsearch | Y | Y | Y | Y | · | · | basic, key | `elasticsearch`, `elasticsearch-rest-client` | `plugin-elasticsearch` | P1 |
| OpenSearch | Y | Y | Y | Y | · | · | basic, iam | `opensearch` | `plugin-opensearch` | P2 |
| Solr | Y | Y | · | Y | · | · | basic | `solr` | · | P3 |
| Meilisearch | · | Y | Y | Y | · | · | key | · | `plugin-meilisearch` | P3 |
| Typesense | · | Y | · | Y | · | · | key | · | `plugin-typesense` | P3 |
| Qdrant | Y | Y | · | Y | · | · | key | `qdrant` | · | P2 |
| Pinecone | · | Y | · | Y | · | · | key | `pinecone` | `plugin-pinecone` | P3 |
| Weaviate | · | Y | · | Y | · | · | key | `weaviate` | `plugin-weaviate` | P3 |
| Milvus | · | Y | · | Y | · | · | basic, key | `milvus` | · | P3 |
| pgvector | Y | Y | · | Y | · | · | basic | `pgvector` | `plugin-jdbc` (postgres) | P2 |
| Neo4j | Y | Y | · | Y | · | · | basic | `neo4j` | `plugin-neo4j` | P2 |

### A.4 Warehouse, lakehouse, OLAP and compute (20)

| Connector | Source | Sink | Streaming | Batch | CDC | Webhook | Auth | Camel | Kestra | Priority |
|---|---|---|---|---|---|---|---|---|---|---|
| ★ Snowflake | Y | Y | · | Y | · | · | key, oauth2, basic | `sql`, `jdbc` | `plugin-jdbc` (snowflake) | P1 |
| ★ BigQuery | Y | Y | Y | Y | · | · | sa | `google-bigquery` | `plugin-gcp` (bigquery) | P1 |
| Redshift | Y | Y | · | Y | · | · | basic, iam | `aws2-redshift`, `sql` | `plugin-jdbc` (redshift) | P2 |
| Databricks | Y | Y | · | Y | · | · | oauth2, key | `sql`, `jdbc` | `plugin-databricks` | P2 |
| ★ ClickHouse | Y | Y | Y | Y | · | · | basic | `clickhouse`, `sql` | `plugin-jdbc` (clickhouse) | P1 |
| DuckDB | Y | Y | · | Y | · | · | none | `duckdb` | `plugin-jdbc` (duckdb) | P2 |
| Druid | Y | Y | Y | Y | · | · | basic | · | `plugin-jdbc` (druid) | P3 |
| Pinot | Y | Y | Y | Y | · | · | basic | · | `plugin-jdbc` (pinot) | P3 |
| StarRocks | Y | Y | Y | Y | · | · | basic | `sql`, `jdbc` | `plugin-jdbc` (mysql) | P3 |
| Doris | Y | Y | Y | Y | · | · | basic | `sql`, `jdbc` | `plugin-jdbc` (mysql) | P3 |
| Trino | Y | · | · | Y | · | · | basic, jwt, oauth2 | `sql`, `jdbc` | `plugin-jdbc` (trino) | P2 |
| Athena | Y | · | · | Y | · | · | iam | `aws2-athena` | `plugin-aws` (athena) | P3 |
| Synapse / Fabric | Y | Y | · | Y | · | · | aad | `sql`, `jdbc` | `plugin-azure` (synapse), `plugin-microsoft-fabric` | P3 |
| ★ Iceberg | Y | Y | Y | Y | · | · | iam, sa, oauth2 | · | `plugin-iceberg` | P1 |
| Delta Lake | Y | Y | · | Y | · | · | iam, sa | · | `plugin-databricks` | P2 |
| Hudi | Y | Y | · | Y | · | · | iam | · | · | P3 |
| Hive | Y | Y | · | Y | · | · | kerb, basic | `sql`, `jdbc` | · | P3 |
| Spark | Y | Y | · | Y | · | · | none | · | `plugin-spark` | P3 |
| Flink | · | Y | · | Y | · | · | none | `flink` | `plugin-flink` | P3 |
| Teradata | Y | Y | · | Y | · | · | basic | `sql`, `jdbc` | · | P3 |

### A.5 Object storage and files (10)

| Connector | Source | Sink | Streaming | Batch | CDC | Webhook | Auth | Camel | Kestra | Priority |
|---|---|---|---|---|---|---|---|---|---|---|
| ★ S3 | Y | Y | Y | Y | · | Y | iam | `aws2-s3` | `plugin-aws` (s3) | P1 |
| MinIO / RustFS | Y | Y | · | Y | · | Y | key | `minio`, `aws2-s3` | `plugin-minio` | P2 |
| Cloudflare R2 | Y | Y | · | Y | · | · | key | `aws2-s3` | `plugin-cloudflare` | P2 |
| GCS | Y | Y | Y | Y | · | Y | sa | `google-storage` | `plugin-gcp` (gcs) | P2 |
| Azure Blob | Y | Y | Y | Y | · | Y | aad, key | `azure-storage-blob` | `plugin-azure` (storage) | P2 |
| ADLS Gen2 | Y | Y | · | Y | · | · | aad | `azure-storage-datalake` | `plugin-azure` (storage) | P2 |
| SFTP | Y | Y | · | Y | · | · | ssh, basic | `sftp` | `plugin-fs` (sftp) | P2 |
| FTP / FTPS | Y | Y | · | Y | · | · | basic | `ftp`, `ftps` | `plugin-fs` (ftp, ftps) | P3 |
| SMB / NFS | Y | Y | · | Y | · | · | basic, kerb | `smb`, `file` | `plugin-fs` (smb, nfs) | P3 |
| Local FS / HDFS | Y | Y | · | Y | · | · | none, kerb | `file` | `plugin-fs` (local) | P3 |

### A.6 Formats (6)

Formats are codecs used by other connectors; "Source" and "Sink" mean decode and encode.

| Connector | Source | Sink | Streaming | Batch | CDC | Webhook | Auth | Camel | Kestra | Priority |
|---|---|---|---|---|---|---|---|---|---|---|
| ★ Parquet | Y | Y | · | Y | · | · | none | `parquet-avro` | `plugin-serdes` (parquet) | P1 |
| ★ Avro | Y | Y | Y | Y | · | · | none | `avro`, `jackson-avro` | `plugin-serdes` (avro) | P1 |
| ORC | Y | Y | · | Y | · | · | none | · | · | P3 |
| CSV / NDJSON | Y | Y | Y | Y | · | · | none | `csv`, `jackson` | `plugin-serdes` (csv, json) | P2 |
| ★ Arrow IPC / Flight | Y | Y | Y | Y | · | · | none, mtls, jwt | · | `plugin-jdbc` (arrow-flight) | P1 |
| Protobuf | Y | Y | Y | Y | · | · | none | `protobuf`, `jackson-protobuf` | `plugin-serdes` (protobuf) | P2 |

### A.7 CDC (6)

| Connector | Source | Sink | Streaming | Batch | CDC | Webhook | Auth | Camel | Kestra | Priority |
|---|---|---|---|---|---|---|---|---|---|---|
| ★ Debezium-Postgres | Y | · | Y | · | Y | · | basic, mtls | `debezium-postgres` | `plugin-debezium` | P1 |
| ★ Debezium-MySQL | Y | · | Y | · | Y | · | basic, mtls | `debezium-mysql` | `plugin-debezium` | P1 |
| Debezium-SQL Server / Oracle | Y | · | Y | · | Y | · | basic | `debezium-sqlserver`, `debezium-oracle` | `plugin-debezium` | P2 |
| Debezium-MongoDB | Y | · | Y | · | Y | · | basic | `debezium-mongodb` | `plugin-debezium` | P2 |
| Debezium Embedded engine | Y | · | Y | · | Y | · | basic | `debezium-*` (embedded) | `plugin-debezium` | P3 |
| DynamoDB Streams | Y | · | Y | · | Y | · | iam | `aws2-ddbstream` | `plugin-aws` (dynamodb) | P2 |

### A.8 Integration and orchestration (6)

| Connector | Source | Sink | Streaming | Batch | CDC | Webhook | Auth | Camel | Kestra | Priority |
|---|---|---|---|---|---|---|---|---|---|---|
| Camel (runtime) | Y | Y | Y | Y | · | Y | per component | (itself) | `plugin-camel` | P1 |
| Kestra (companion) | Y | Y | · | Y | · | Y | key, basic | · | (itself) | P2 |
| Kafka Connect | Y | Y | Y | · | Y | · | sasl, mtls | `kafka` | · | P3 |
| Airbyte (licence flag, D359) | Y | Y | · | Y | Y | · | key | · | `plugin-airbyte` | P3 |
| dbt | · | Y | · | Y | · | · | none | · | `plugin-dbt` | P3 |
| Airflow | Y | · | · | Y | · | Y | basic | · | `plugin-airflow` | P3 |

### A.9 CDP, marketing, analytics and ads (22)

| Connector | Source | Sink | Streaming | Batch | CDC | Webhook | Auth | Camel | Kestra | Priority |
|---|---|---|---|---|---|---|---|---|---|---|
| Segment | Y | Y | Y | · | · | Y | key | · | · | P2 |
| RudderStack | Y | Y | Y | · | · | Y | key | · | · | P3 |
| mParticle | Y | Y | Y | · | · | Y | key | · | · | P3 |
| Braze | Y | Y | Y | Y | · | Y | key | · | · | P3 |
| Iterable | Y | Y | Y | · | · | Y | key | · | · | P3 |
| Klaviyo | Y | Y | · | Y | · | Y | key | · | `plugin-klaviyo` | P3 |
| Mailchimp | Y | Y | · | Y | · | Y | key, oauth2 | · | · | P3 |
| SendGrid | Y | Y | Y | · | · | Y | key | · | `plugin-notifications` (sendgrid) | P2 |
| Amazon SES | Y | Y | Y | · | · | Y | iam | `aws2-ses` | · | P2 |
| Twilio SMS | Y | Y | Y | · | · | Y | key | `twilio` | `plugin-notifications` (twilio) | P2 |
| WhatsApp Business | Y | Y | Y | · | · | Y | oauth2 | `whatsapp` | `plugin-notifications` (whatsapp) | P3 |
| FCM | · | Y | Y | · | · | · | sa | · | · | P3 |
| APNs | · | Y | Y | · | · | · | jwt, mtls | · | · | P3 |
| OneSignal | · | Y | Y | · | · | · | key | · | · | P3 |
| Customer.io | Y | Y | Y | · | · | Y | key | · | · | P3 |
| Amplitude | Y | Y | Y | Y | · | · | key | · | · | P3 |
| Mixpanel | Y | Y | Y | Y | · | · | key, basic | · | · | P3 |
| PostHog | Y | Y | Y | Y | · | Y | key | · | `plugin-posthog` | P2 |
| GA4 | Y | Y | Y | Y | · | · | sa, key | · | · | P3 |
| Meta Ads / CAPI | Y | Y | Y | Y | · | Y | oauth2 | · | `plugin-meta` | P3 |
| Google Ads | Y | Y | · | Y | · | · | oauth2 | · | · | P3 |
| LinkedIn Ads | Y | Y | · | Y | · | · | oauth2 | · | `plugin-linkedin` | P3 |

### A.10 CRM, sales and support (12)

| Connector | Source | Sink | Streaming | Batch | CDC | Webhook | Auth | Camel | Kestra | Priority |
|---|---|---|---|---|---|---|---|---|---|---|
| Salesforce | Y | Y | Y | Y | Y | Y | oauth2 | `salesforce` | · | P2 |
| HubSpot | Y | Y | · | Y | · | Y | oauth2, key | · | `plugin-hubspot` | P2 |
| Pipedrive | Y | Y | · | Y | · | Y | key, oauth2 | · | `plugin-pipedrive` | P3 |
| Zoho | Y | Y | · | Y | · | Y | oauth2 | · | · | P3 |
| Dynamics 365 | Y | Y | · | Y | · | Y | aad | `olingo4` | · | P3 |
| Zendesk | Y | Y | · | Y | · | Y | key, oauth2 | `zendesk` | `plugin-zendesk` | P2 |
| Intercom | Y | Y | · | Y | · | Y | key | · | · | P3 |
| Freshdesk | Y | Y | · | Y | · | Y | key | · | · | P3 |
| ServiceNow | Y | Y | · | Y | · | Y | basic, oauth2 | `servicenow` | `plugin-servicenow` | P2 |
| Marketo | Y | Y | · | Y | · | · | oauth2 | · | · | P3 |
| Gainsight | Y | Y | · | Y | · | · | key | · | · | P3 |
| Gong | Y | · | · | Y | · | Y | key, oauth2 | · | · | P3 |

### A.11 Commerce and payments (8)

| Connector | Source | Sink | Streaming | Batch | CDC | Webhook | Auth | Camel | Kestra | Priority |
|---|---|---|---|---|---|---|---|---|---|---|
| Stripe | Y | Y | · | Y | · | Y | key, hmac | `stripe` | `plugin-stripe` | P2 |
| Shopify | Y | Y | · | Y | · | Y | oauth2, key, hmac | · | `plugin-shopify` | P2 |
| WooCommerce | Y | Y | · | Y | · | Y | key | · | · | P3 |
| Magento | Y | Y | · | Y | · | Y | oauth2 | · | · | P3 |
| BigCommerce | Y | Y | · | Y | · | Y | key | · | · | P3 |
| PayPal | Y | Y | · | Y | · | Y | oauth2 | · | · | P3 |
| Razorpay | Y | Y | · | Y | · | Y | key, hmac | · | · | P3 |
| Adyen | Y | Y | · | · | · | Y | key, hmac | · | · | P3 |

### A.12 Developer and collaboration (14)

| Connector | Source | Sink | Streaming | Batch | CDC | Webhook | Auth | Camel | Kestra | Priority |
|---|---|---|---|---|---|---|---|---|---|---|
| GitHub | Y | Y | · | Y | · | Y | oauth2, key, hmac | `github2` | `plugin-github` | P2 |
| GitLab | Y | Y | · | Y | · | Y | key, oauth2 | · | `plugin-gitlab` | P2 |
| Bitbucket | Y | Y | · | Y | · | Y | oauth2, key | · | · | P3 |
| Jira | Y | Y | · | Y | · | Y | basic, oauth2 | `jira` | `plugin-jira` | P2 |
| Linear | Y | Y | · | Y | · | Y | key, oauth2 | · | `plugin-linear` | P3 |
| Confluence | Y | Y | · | Y | · | Y | basic, oauth2 | · | `plugin-confluence` | P3 |
| Notion | Y | Y | · | Y | · | Y | key, oauth2 | · | `plugin-notion` | P3 |
| Slack | Y | Y | Y | · | · | Y | oauth2, hmac | `slack` | `plugin-slack`, `plugin-notifications` (slack) | P2 |
| Microsoft Teams | Y | Y | · | · | · | Y | aad | · | `plugin-notifications` (teams) | P3 |
| Discord | Y | Y | · | · | · | Y | key | · | `plugin-discord` | P3 |
| Google Workspace | Y | Y | · | Y | · | Y | oauth2, sa | `google-drive`, `google-sheets`, `google-mail`, `google-calendar` | `plugin-googleworkspace` | P2 |
| Microsoft 365 | Y | Y | · | Y | · | Y | aad | `mail-microsoft-oauth`, `olingo4` | `plugin-microsoft365` | P3 |
| Airtable | Y | Y | · | Y | · | Y | key | · | `plugin-airtable` | P3 |
| Telegram | Y | Y | Y | · | · | Y | key | `telegram` | `plugin-telegram` | P3 |

### A.13 Observability and incident (10)

| Connector | Source | Sink | Streaming | Batch | CDC | Webhook | Auth | Camel | Kestra | Priority |
|---|---|---|---|---|---|---|---|---|---|---|
| Prometheus | Y | Y | Y | · | · | · | basic, key | · | `plugin-prometheus` | P2 |
| Grafana | · | Y | · | · | · | Y | key | · | `plugin-grafana` | P3 |
| Datadog | Y | Y | Y | · | · | Y | key | · | · | P3 |
| New Relic | · | Y | Y | · | · | Y | key | · | · | P3 |
| ★ OpenTelemetry | Y | Y | Y | · | · | · | key, mtls | · | · | P1 |
| Loki | Y | Y | Y | · | · | · | basic | · | · | P3 |
| Splunk | Y | Y | Y | Y | · | · | key | `splunk`, `splunk-hec` | · | P2 |
| Sentry | Y | · | · | · | · | Y | key | · | `plugin-sentry` | P3 |
| PagerDuty | Y | Y | · | · | · | Y | key | · | `plugin-pagerduty` | P3 |
| CloudWatch | Y | Y | Y | Y | · | · | iam | `aws2-cw` | `plugin-aws` (cloudwatch) | P3 |

### A.14 Infrastructure and runtime (8)

| Connector | Source | Sink | Streaming | Batch | CDC | Webhook | Auth | Camel | Kestra | Priority |
|---|---|---|---|---|---|---|---|---|---|---|
| Kubernetes | Y | Y | Y | · | · | · | sa, mtls | `kubernetes` | `plugin-kubernetes` | P3 |
| Docker | Y | Y | Y | · | · | · | mtls | `docker` | `plugin-docker` | P3 |
| Terraform | · | Y | · | Y | · | · | none | · | `plugin-terraform` | P3 |
| Ansible | · | Y | · | Y | · | · | ssh | · | `plugin-ansible` | P3 |
| Lambda / Cloud Run / Azure Functions | Y | Y | Y | · | · | Y | iam, sa, aad | `aws2-lambda`, `google-functions`, `azure-functions` | `plugin-aws` (lambda), `plugin-gcp` (function), `plugin-azure` (function) | P2 |
| Vault | Y | Y | · | · | · | · | key | `hashicorp-vault` | · | P2 |
| Helm / Argo CD | · | Y | · | Y | · | Y | key | · | `plugin-helm`, `plugin-argocd` | P3 |
| SSH / Shell | Y | Y | · | Y | · | · | ssh | `ssh`, `exec` | `plugin-fs` (ssh), `plugin-scripts` | P3 |

### A.15 Identity (4)

| Connector | Source | Sink | Streaming | Batch | CDC | Webhook | Auth | Camel | Kestra | Priority |
|---|---|---|---|---|---|---|---|---|---|---|
| Okta | Y | Y | · | Y | · | Y | oauth2, key | · | · | P3 |
| Auth0 | Y | Y | · | Y | · | Y | oauth2 | · | · | P3 |
| Keycloak | Y | Y | · | Y | · | Y | oauth2 | `keycloak` | · | P2 |
| Entra ID | Y | Y | · | Y | · | Y | aad | · | · | P3 |

### A.16 AI (10)

AI connectors are mostly sinks used as enrichment steps (embedding, classification, extraction) inside a route, with the result written back into the event.

| Connector | Source | Sink | Streaming | Batch | CDC | Webhook | Auth | Camel | Kestra | Priority |
|---|---|---|---|---|---|---|---|---|---|---|
| OpenAI | · | Y | · | Y | · | · | key | `openai`, `langchain4j-*` | `plugin-openai` | P2 |
| Anthropic | · | Y | · | Y | · | · | key | `langchain4j-*` | `plugin-anthropic` | P2 |
| Gemini | · | Y | · | Y | · | · | key, sa | `google-vertexai`, `langchain4j-*` | `plugin-gemini` | P3 |
| Bedrock | · | Y | · | Y | · | · | iam | `aws-bedrock` | `plugin-aws` (bedrock) | P3 |
| Azure OpenAI | · | Y | · | Y | · | · | aad, key | `langchain4j-*` | `plugin-azure` (aifoundry) | P3 |
| Ollama | · | Y | · | Y | · | · | none | `langchain4j-*` | `plugin-ollama` | P3 |
| vLLM | · | Y | · | Y | · | · | key | `langchain4j-*` | · | P3 |
| Hugging Face | · | Y | · | Y | · | · | key | `huggingface` | `plugin-huggingface` | P3 |
| MCP | Y | Y | · | · | · | · | oauth2 | `mcp-server` | `plugin-ai` | P2 |
| A2A | Y | Y | Y | · | · | · | oauth2 | `a2a` | · | P3 |

### A.17 Generic protocols (8)

| Connector | Source | Sink | Streaming | Batch | CDC | Webhook | Auth | Camel | Kestra | Priority |
|---|---|---|---|---|---|---|---|---|---|---|
| ★ HTTP / REST | Y | Y | Y | Y | · | Y | none, basic, key, oauth2, jwt, mtls | `http`, `rest` | core (HTTP tasks) | P1 |
| GraphQL | Y | Y | · | Y | · | · | key, oauth2 | `graphql` | `plugin-graphql` | P2 |
| SOAP | Y | Y | · | Y | · | · | basic | `cxf`, `soap` | · | P3 |
| OpenAPI-generated | Y | Y | · | Y | · | Y | per spec | `rest-openapi` | · | P3 |
| ★ JDBC | Y | Y | · | Y | · | · | per driver | `jdbc`, `sql` | `plugin-jdbc` | P1 |
| ★ ADBC | Y | Y | · | Y | · | · | per driver | · | · | P1 |
| SMTP / IMAP | Y | Y | Y | · | · | · | basic, oauth2 | `mail` | `plugin-notifications` (mail), `plugin-email` | P3 |
| RSS / Atom | Y | · | Y | · | · | · | none | `rss`, `atom` | · | P3 |

Totals: 200 connectors; 21 ★ (P1), and the P2 and P3 counts are computed by CN1 Task 2's generator.
