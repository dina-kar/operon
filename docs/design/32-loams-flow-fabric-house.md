# 32 — Loams Flow, Event Fabric and House

Status: **Approved** (owner defaults, 2026-10-02: "do suggested for all") · 2026-10-01. Source: the owner's draft "Loams: Final Plan" (Loams Flow, Loams Event Fabric, Loams House), kept in `chatdump.md` lines 325–549, and two owner inputs of 2026-10-01: first, "Apache Iggy handles raw protocols well … the owner sees Iggy as the streaming house, the ingest and streaming layer"; then, in the owner's words, "our loams wal is optimized for postgres, mysql, text search, vector search, graph workloads, iggy and fluss for event ingestion so separate is good, and we see more contributions to iggy and fluss". This document turns that direction into decisions **D330–D351** and open questions **Q330–Q347**. The connector registry and catalog of Loams Flow are in [§33](33-connectors.md) (D352–D359, Q348–Q359). Everything here was a **proposal** until the owner confirmed it on 2026-10-02 ("do suggested for all"); where it reverses an approved decision (D45, and D74's Loams-side Kafka gateway), §14 says so and the owner decides. **No code is written by this document.**

**Names (owner rulings, 2026-10-01).** Published packages use the namespace `loams` (crates.io `loams-*`, PyPI `loams-*`, npm `@loams/*`), replacing `loamdb` (D33); Go modules use `loams.dev/...`; the domain is `loams.dev`; CloudEvents types Loams defines use the prefix `io.loams.dev.<domain>.<name>.v1`. The crate and binary names below (`loams-fabric`, `loams-chdb`, …) follow the same namespace as the engine's `loams-*` crates. **Java is deferred**: Loams writes no Java SDK or Java plugin in this track; the Camel layer runs unmodified Camel with generated YAML routes (§33 D354).

Markers:

- **(verify)** means not checked against a primary source, or checked only by reading docs or code that were not run. The plan task that depends on it checks it first.
- **(estimate)** means computed or reasoned, not measured.
- **(read 2026-10-01)** means read in the upstream repository or website on 2026-10-01 at the release named in §16.
- "The draft" is the owner's "Loams: Final Plan". Its status tags (**[verified]**, **[assumed]**, **[build]**) are re-checked here; §3 lists what changed.

---

## 1. Summary

| # | Decision | Status |
|---|---|---|
| D330 | **Three layers, named by role, over existing primitives and three new services.** Loams Flow (routes, connector registry, route compiler), Loams Event Fabric (event ingestion: Apache Iggy and Apache Fluss), Loams House (ClickHouse-dialect query and serving on chDB, plus the vector and graph surfaces Loams already has). The draft's "Trigger Engine / Loams Cells" are Loams Functions (§24) and durable functions (§21); its "Loams Gateway" is the House coordinator, so it does not clash with the runtime gateway (D184) | Approved (owner defaults, 2026-10-02) |
| D331 | **Two logs, chosen by workload** (owner direction). The Loams WAL and streams (§02, §28) stay the log for databases and retrieval: Postgres (§28), MySQL (§29), collections (text and vector), graphs, Loams-written tables and their implicit streams, OTLP logs, and low-rate CloudEvents triggers (D270). **Event ingestion** (high-rate events, telemetry, clickstreams, IoT, CDC fan-in) goes to the **Event Fabric** (Iggy + Fluss). A record lives in one log; bridges (D336) move data between them, never dual writes | Approved (owner defaults, 2026-10-02; owner direction) |
| D332 | **Apache Iggy is the Fabric's ingest log and protocol edge** (§5.2): QUIC, TCP, HTTP and WebSocket transports, SDKs in six languages, consumer groups, its Rust connectors runtime and its MCP server. It runs **unmodified, as a separate service**, pinned to server 0.9.0 (SDK 0.11.0). Loams accepts Iggy's local-disk storage in v1, with bounded retention, and **contributes S3 tiered storage upstream** (discussion apache/iggy#3312) so the edge can later hold long retention on the bucket. Kafka clients reach the Fabric through **Iggy's Kafka gateway**, to which Loams contributes (§5.9); this proposes to **move D74's Kafka gateway out of the Loams binary** (Q331) | Approved (owner defaults, 2026-10-02; owner direction; Q330, Q331) |
| D333 | **Apache Fluss is the Fabric's real-time table layer** (§5.3): Log tables (append-only) and Primary Key tables with the LastRow, FirstRow, Versioned and Aggregation merge engines, primary-key and prefix lookups, and tiering to Iceberg. It runs unmodified (1.0.0, Java, ZooKeeper, tablet-server disks with S3 remote storage). **v1 accepts Fluss's Flink tiering job**, run under the Flink Kubernetes Operator that §26 already plans (D212), writing Iceberg through Lakekeeper on RustFS; a Rust tiering service on `fluss-rs` and iceberg-rust is a contribution candidate, not a v1 dependency (Q332) | Approved (owner defaults, 2026-10-02; owner direction; Q332) |
| D334 | **One envelope: CloudEvents 1.0, laid out as in D270.** On Iggy, an event is one message whose headers are the Kafka binding's `ce_<attr>` headers plus `content-type`, whose payload is the data, and whose Iggy message id is the first 16 bytes of the D270 dedup key SHA-256(`source` ‖ 0x00 ‖ `id`). On Fluss, an event row carries the attributes as columns (§5.4). The draft's envelope fields map onto CloudEvents attributes; the tenant comes from the credential, never from an attribute | Approved (owner defaults, 2026-10-02) |
| D335 | **Schemas and dead letters**: the schema of a Fabric table is its Fluss table schema; Iggy topics carry `dataschema` URIs to schema documents (JSON Schema, Avro, protobuf) stored under `ns/<ns>/fabric/schemas/` in the bucket, with a version history in the Fabric's system table `_fabric.flow_objects`. A Confluent Schema Registry REST subset waits for Kafka clients (Q344). Each route has a dead-letter topic `<topic>.dlq` on Iggy with the error in `ce_loamserror` | Approved (owner defaults, 2026-10-02) |
| D336 | **Bridges, not dual writes** (§5.7): Iggy → Fluss through a Rust connector plugin `fluss_sink` (contributed to Iggy); Iggy → Loams collections and streams through a plugin `loams_sink` that calls `ProduceCloudEvents` (D270) or collection writes; Fluss → Iceberg through tiering; Loams → Fabric through a link target `iggy` (FL3). Exactly-once is defined per bridge in §5.7 | Approved (owner defaults, 2026-10-02) |
| D337 | **A Flow route is a declarative spec, `loams.flow.v1.Route`**, in YAML or JSON (§6.1): `from` (a connector instance or a Fabric topic or table), `steps` (filter, map, enrich, split, CloudEvents type routing), `to` (one or more targets), and delivery settings. The route API is connect-rust (D128); routes, connector instances and schema versions are stored in the Fabric's own system table `_fabric.flow_objects` (a Fluss PK table keyed by namespace, kind and name, with a version column), so `loams-fabric` needs no engine crate; leases for connector tasks, which need compare-and-set, come from the Loams metastore's network API in FL3 (§6.4) | Approved (owner defaults, 2026-10-02) |
| D338 | **The route compiler emits only existing primitives** (§6.2): links (§09) for stateless transforms and mergeable aggregates into Loams objects; Fluss tables and merge engines for keyed state; House materialized views (which are per-insert-block transforms, so they compile to the same link semantics); Loams Functions (§24) and durable functions (§21) for per-event reactions; Iggy connectors-runtime and Camel routes for external sinks (§33). Flow adds no execution engine | Approved (owner defaults, 2026-10-02) |
| D339 | **Where Flow runs, and the open-core line** (§6.4): the route API, compiler, registry, native connectors and the connector supervisors are open source in this repository, in a separate Cargo workspace `fabric/` and its binary `loams-fabric` (D343). The Camel runner `loams-connect` is open source too (an unmodified Camel runtime with generated YAML routes; no Loams-written Java while Java is deferred). The managed connector fleet, per-plan connector limits, hosted secrets UX and connector billing are `loam-platform` (D220) | Approved (owner defaults, 2026-10-02) |
| D340 | **The Flow designer UI is a console page** (§19) that edits `loams.flow.v1.Route`; it may embed Apache Camel Karavan's designer components (Apache-2.0, 4.18.1) where a route compiles to Camel, and is otherwise Loams's own (Q340). Not in FL1–FL2 | Approved (owner defaults, 2026-10-02) |
| D341 | **No Spark or Flink targets in Flow.** Stateful stream SQL (windowed joins with watermarks) goes to the RisingWave companion (D22), reading Fluss or Iggy. §26 keeps hosting users' own Spark (Sail) and Flink jobs (D211, D212), and Fluss's tiering job is infrastructure, not a Flow target. Amends §00 §7 only by naming the Fabric as RisingWave's source | Approved (owner defaults, 2026-10-02) |
| D342 | **The House SQL engine is chDB** (`chdb-core` 26.9.x, `libchdb`, Apache-2.0), driven through a Loams-owned FFI crate over `chdb.h` with `chdb-rust` 2.0 as the reference (Q337). Rejected: unmodified `clickhouse-server` as the engine (it cannot merge Fluss's unsettled tail or Loams's consistency tokens into a query, and its storage would be its own disks), and a ClickHouse dialect emulated on DataFusion (a second SQL engine's semantics rebuilt by hand) (§7.2) | Approved (owner defaults, 2026-10-02) |
| D343 | **The House runs in its own binary, in its own Cargo workspace**: `fabric/` holds `loams-fabric` (roles `ingest`, `house`, `flow`, later `coordinator`), never linked into `loams`/`loams`. libchdb is ~180 MB compressed and process-global; `fluss-rs` 1.0 is on arrow 59 and Grafeo on arrow 60, against the engine's arrow 58 (Lance 12 lockstep). **Amends D51**: D51 keeps the engine binary free of other engines; a Loams-owned service binary may link an engine library | Approved (owner defaults, 2026-10-02) |
| D344 | **ClickHouse table engines map onto Fluss and Iceberg** (§7.4): `MergeTree` → a Fluss Log table tiered to Iceberg; `ReplacingMergeTree(ver[, is_deleted])` → a Fluss PK table, Versioned (with `ver`) or LastRow; `SummingMergeTree([cols])` → a Fluss PK table, Aggregation with `sum`; `Replicated*` and `ON CLUSTER` accepted and ignored. `ORDER BY` becomes the PK or the sort order, `PARTITION BY` the Fluss and Iceberg partition when it is a supported transform. The original DDL is kept as a table property so `SHOW CREATE TABLE` returns it | Approved (owner defaults, 2026-10-02) |
| D345 | **Reads are a union of the lake snapshot and the Fluss tail**, pinned per query (§7.5): the worker reads the last tiered Iceberg snapshot through chDB's Iceberg reader and the Fluss log from that snapshot's offsets as Arrow streams, merged by key for PK tables. `FINAL` is accepted (Fluss has already merged); an `INSERT` returns an `X-Loams-Consistency-Token` of Fluss bucket offsets, and a read with it sees that write | Approved (owner defaults, 2026-10-02) |
| D346 | **Writes and DDL go to Fluss**, never to chDB's own storage (§7.6): DDL creates Fluss tables with tiering on; `INSERT` bodies in any declared input format are parsed by chDB into Arrow and written with `fluss-rs`; `INSERT … SELECT` runs the `SELECT` in chDB and writes its result | Approved (owner defaults, 2026-10-02) |
| D347 | **A declared ClickHouse surface replaces D45's "no ClickHouse surface"** (§8): the HTTP interface on port 8123 first (loopback until the auth plan, D111), the native protocol on 9000 in FL5, and a versioned list, `chsurface-1`, of statements, engines, formats, settings, functions, table functions, system tables and error codes. "100 % compatible" means 100 % of the declared surface's tests pass, with every deviation on the allowlist. **Reverses D45 and amends §00 §7 and §08**; owner approval needed (Q333) | Approved (owner defaults, 2026-10-02; Q333) |
| D348 | **The allowlist and the release gate** (§8.10): `conformance/clickhouse/allowlist.toml`, one entry per deviation with an id, the tests it covers, a category, the reason, an owner and a surface version; an allowlisted test that passes fails CI until its entry is removed; a regression on the declared surface blocks a release; semantic deviations need the owner's approval | Approved (owner defaults, 2026-10-02) |
| D349 | **Compatibility is proven differentially** (§10): the same workload on a pinned reference `clickhouse-server` (same ClickHouse version as `chdb-core`, CI-only container) and on Loams, diffing results, types, errors and wire bytes, in single-node, distributed (FL5) and S3-fault modes; corpora are the declared-surface corpus, the upstream stateless tests that fall inside the surface, ClickBench and TPC-H, and the official drivers | Approved (owner defaults, 2026-10-02) |
| D350 | **Graph stays native (§07, D44); Grafeo is not the House graph engine** (§7.7). Grafeo 0.5.43 (Apache-2.0, released 2026-09-27) has one maintainer in practice, a repository created on 2026-01-26 (eight months before this review), depends on arrow 60, stores locally, has no S3 or distribution, and brings its own HNSW. `grafeo-server` (GQL over gRPC, Bolt v5, Studio) may be a Flow sink for teams that want Cypher or GQL over a copy (Q339); one vector source of truth per collection stays Lance + qdrant-edge (§06) | Approved (owner defaults, 2026-10-02) |
| D351 | **Track FL** (§11): FL1 Fabric foundation; FL2 House SQL phase 1 (chDB, HTTP 8123, MergeTree, Replacing, Summing, the differential harness); FL3 Flow routes, MVs and sinks; FL4 graph alignment (M3 as planned, no Grafeo); FL5 distribution and the native protocol; FL6 hard engines. Interleaved with M, R, D and J on the one-build machine (D127), and built in the `fabric/` workspace so it never rebuilds the engine graph | Approved (owner defaults, 2026-10-02) |

## 2. Goals and non-goals

### 2.1 Goals

1. **Managed streaming and analytics without managed Spark or Flink as the default path** (the draft's goal 1). Flow compiles to Loams's own primitives, Iggy, Fluss and chDB.
2. **Separate logs for separate workloads** (owner direction): the Loams WAL keeps its database and retrieval optimizations (§28 Arm A, collections, graphs); event ingestion uses Iggy and Fluss, where Loams also contributes upstream.
3. **Buy, not build** (user memory): Iggy, Fluss, chDB, Debezium and Camel run unmodified; Loams writes envelopes, bridges, the compiler, the House shim and connectors where no good one exists, and upstreams what is general.
4. **ClickHouse compatibility you can test**: a declared, versioned surface with a published pass rate and an allowlist (the draft's §1).
5. **One envelope everywhere**: CloudEvents 1.0 on Loams streams (D270), Iggy messages and Fluss rows (D334).

### 2.2 Non-goals

- **No new storage engine.** Fluss tables, Iceberg on RustFS and Loams's own WAL are the storage; chDB never keeps state between queries.
- **No ClickHouse server inside Loams.** chDB executes queries; the House never runs ClickHouse's MergeTree storage, Keeper or replication.
- **No second graph or vector store.** §07 and §06 stay; Grafeo is not embedded.
- **No Flink or Spark as Flow targets**, and no stateful stream processing in Flow (D341).
- **No Loams fork of Iggy or Fluss.** Loams's changes go upstream as PRs; Loams pins released versions and carries at most a short-lived patch branch for a merged-but-unreleased fix.

## 3. What changed since the draft

| Draft claim | Status in the draft | Found on 2026-10-01 | Effect |
|---|---|---|---|
| Iggy "is not Kafka-protocol compatible, so Kafka ingestion needs a native adapter" | [verified] | Still true for releases: Iggy's `gateways/kafka` is "in development and not yet part of a release"; Produce/Fetch/ListOffsets/Metadata/CreateTopics work through a bridge, consumer-group membership works, offset commit and fetch are not implemented (read 2026-10-01) | Kafka ingest is a contribution to Iggy's gateway (D332, §5.9), not a Loams-built adapter |
| Iggy as "durable log and replay source" | [verified] | Iggy 0.9.0 (2026-09-18) stores on local disk; VSR clustering is in the server but documented as pre-production ("Iggy hasn't reached a 1.0 production release yet"); **tiered storage is not implemented**: issue #1419 was closed into discussion #3312 (2026-05-22), deferred until VSR landed, revived 2026-09-08; the README's "backups and archiving to S3" line has no matching code on `master` (no archiver module in the tree) | Iggy is the ingest log with bounded retention; long retention and replay from the bucket come from Fluss remote storage, Iceberg, or Iggy's tiering once it lands (D332) |
| Fluss is "Java and incubating" | — | Fluss **graduated to an Apache top-level project on 2026-07-16** (announced 2026-08-06) and released **1.0.0 on 2026-09-22**, with a Rust REST **Fluss Gateway** (preview: metadata, DDL and writes; no record reads, no caller authentication) and native Rust, Python and C++ clients in the main project | Lower project risk; `fluss-rs` 1.0.0 is the House's client (arrow 59, D343) |
| Fluss tiering "ships as a Flink job" | [verified] | Still true in 1.0: "The Tiering Service is a Flink job", needing a running Flink cluster (`fluss-flink-tiering` jar; Flink 1.18–1.20, 2.2, 2.3 modules) | Accepted for v1 (D333, Q332) |
| Grafeo v0.5.43, Apache-2.0, GQL/Cypher/Gremlin/GraphQL/SPARQL/SQL-PGQ, MVCC, HNSW | [verified] | Confirmed (release 2026-09-27). Also: repository created 2026-01-26; 90 of the last 100 commits by one author; arrow 60; `grafeo-server` **does** have a Bolt v5 listener (image `grafeo-server:bolt`) and a GQL wire protocol over gRPC, but its last release is v0.5.40 (2026-04-20) | Bolt is not "[build]" as the draft said; the maturity and lockstep findings still keep Grafeo out of the House (D350) |
| chDB Rust binding is experimental and needs `libchdb` | [verified] | `chdb-rust` 2.0.0 (2026-09-20) still says "Experimental"; it downloads `libchdb` at build time; it is thread-safe and streams Arrow through the C Data Interface; libchdb for linux-x86_64 is 180 MB compressed (`chdb-core` v26.9.0, 2026-09-28) | Own FFI crate, separate binary (D342, D343) |
| "Metastore: Raft or Postgres" (open decision 4) | open | Loams's `MetaStore` trait has openraft, Postgres, DynamoDB and TiKV backends (D47, D58, D124, D260) | Moot; Flow and House metadata use the `MetaStore` trait or Fluss table properties |
| "WAL → replay into qdrant-edge with snapshots to S3" for vectors | assumed | As built, Lance is the durable vector tier and qdrant-edge builds hot HNSW artifacts from manifests (§06 §5, M1.3) | §06 unchanged (D350) |
| "Kafka / S3Queue engines over Iggy adapters" | — | — | Kept as Tier 2 (§8.3), as Flow routes into Fluss tables |
| "S3, URL, File, Postgres, MySQL table functions pass through to chDB" | — | Passing them through lets any tenant read the worker's disk (`file`) or reach internal addresses (`url`, `remote`) | Off by default (§7.8) |

## 4. Architecture

```
              Kafka clients  MQTT / AMQP  Webhooks  OTLP  Debezium Server  SDKs (QUIC/TCP/HTTP/WS)
                    │            │           │        │          │              │
                    ▼            ▼           ▼        ▼          ▼              ▼
            Iggy Kafka gw    loams-connect  loams-fabric ingest (CloudEvents HTTP/gRPC)   (direct)
            (contributed)    (Camel → Iggy)          │                                   │
                    └────────────┴───────────────────┴───────────────┬───────────────────┘
                                                                     ▼
┌──────────────────────────── LOAMS EVENT FABRIC (event ingestion) ────────────────────────────┐
│  Apache Iggy (ingest log, VSR, local NVMe, bounded retention)  ── connectors runtime ──┐     │
│        │ fluss_sink (Rust plugin, upstreamed)                     sinks: ES, HTTP, S3,  │     │
│        ▼                                                          Postgres, ClickHouse… │     │
│  Apache Fluss (Log + PK tables, merge engines, remote storage on RustFS)               │     │
│        │ tiering (Flink job under the Flink operator, D212)                             │     │
│        ▼                                                                                │     │
│  Iceberg on RustFS through Lakekeeper ◄──────────────── shared catalog ───────────────┼──┐  │
└────────┬───────────────────────────────────────────────────────────────────────────────┘  │  │
         │ loams_sink (Iggy plugin → ProduceCloudEvents / collection writes)                 │  │
         ▼                                                                                   │  │
┌──────────────────── LOAMS ENGINE (loams binary; Loams WAL, §02/§28) ──────────────────────┐│  │
│ collections (Lance + Tantivy + qdrant-edge), graphs (§07), Loams tables (M4), Live, PG/MySQL││  │
└──────────────────────────────────────────────────────────────────────────────────────────┘│  │
                                                                                            │  │
┌─────────────────── loams-fabric (fabric/ workspace; separate binary) ─────────────────────┘  │
│ flow:   route API (connect-rust), compiler, registry (§33), supervisors (Iggy runtime,       │
│         loams-connect, Debezium Server)                                                      │
│ house:  ClickHouse HTTP 8123 → classifier → DDL/INSERT → Fluss;  SELECT → chDB session:      │
│         Iceberg snapshot (icebergS3) ∪ Fluss tail (ArrowStream) → output format             │
│ coordinator (FL5): scatter-gather, two-stage aggregation (-State / -Merge), native 9000      │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
```

## 5. Loams Event Fabric

### 5.1 Two logs, one rule (D331)

The Loams WAL is tuned for what it already carries: Postgres WAL through Arm A (§28 §7.2, D263–D272), MySQL binlog (§29, D276), collections and their implicit streams with consistency tokens (D76), graph sidecars, Loams Live's journal (§20). Event ingestion has other needs: many producers on raw transports, consumer groups, connectors to everything, and keyed real-time tables with merge semantics. The owner's direction is to give those to Iggy and Fluss and contribute there.

The rule: **every record has exactly one home log.** Writes through Loams's APIs (native, Flight, Qdrant, Elasticsearch, Postgres, MySQL, Live, `/events` at trigger rates) land in the Loams WAL. Writes through the Fabric's APIs (Iggy transports and SDKs, the Fabric ingest route, connectors, ClickHouse `INSERT`) land in Iggy or Fluss. A bridge (D336) copies records across when a route asks for it, with the target's own exactly-once mechanism, so no client ever writes both.

What stays in §02: the native stream API core (D72), OTLP logs (D73), CloudEvents on streams (D270, PRs #170 and #171) and changelog streams (D21) are unchanged; they serve the Loams objects and trigger-rate events. What moves: general-purpose, high-rate event streaming, which §02 §7.2 meant to reach through Loams's own Kafka gateway (D74). Q347 asks whether D72's explicit streams should then narrow.

### 5.2 Apache Iggy: evaluation and fit (D332)

**What Iggy is** (server 0.9.0, 2026-09-18; README and docs read 2026-10-01): a persistent append-only log in Rust, thread-per-core and shared-nothing on `io_uring` and `compio`; transports QUIC, TCP (its own binary protocol), HTTP (REST) and WebSocket, all with TLS; streams → topics → partitions; server-side consumer offsets and consumer groups; retention by `message_expiry` and `max_topic_size`; users, permissions and personal access tokens; optional AES-256-GCM encryption; per-topic `durability` (`replicated`: VSR quorum commit; `persisted`: stable storage on the quorum); clustering by Viewstamped Replication with one consensus group for metadata and one per partition, at least three replicas, documented as pre-production; SDKs for Rust, C#, Java, Python, Node.js and Go; a CLI, a web UI and a Helm chart; a **connectors runtime** that loads Rust plugins implementing `Source` and `Sink` (sources: Elasticsearch, HTTP, InfluxDB, PostgreSQL with polling and logical-replication CDC modes, random; sinks: ClickHouse, Delta Lake, Doris, Elasticsearch, HTTP, Iceberg, InfluxDB, Meilisearch, MongoDB, PostgreSQL, Quickwit, RabbitMQ, Redshift, S3, SurrealDB, stdout) with per-stream transforms and Avro support; an **MCP server**; and an in-development **Kafka gateway**. Apache-2.0; an Apache top-level project since 2026-08-19 (graduated from the Incubator; announced on the project blog 2026-08-24). Apache Camel 4.22 ships a `camel-iggy` component (producer and consumer, "Preview" since 4.17).

**Gaps that matter to Loams:**

| Gap | Evidence | Consequence |
|---|---|---|
| No object storage as the source of truth; no tiered storage | Discussion #3312; no archiver in `master` | Iggy's disks hold the data. Loams bounds retention (default 72 h, (estimate) sized to replay Fluss and connectors after an outage) and runs `persisted` durability on three replicas |
| Pre-1.0 wire and cluster protocol | "The cluster protocol and configuration … can still change between pre-release versions"; the VSR migration deleted the legacy wire format | Loams pins server and SDK together and upgrades as a tested pair (FL1 Task 0); Loams does not emulate Iggy's protocol |
| Kafka gateway unreleased; offset commit missing | `gateways/kafka/README.md` | Kafka clients wait for the gateway; Loams contributes offset commit/fetch and the produce path (§5.9) |
| Content-level deduplication | 0.9's dedup is per client session (`dedup_clients_max`, a retried produce is answered from the client table) **(verify)** | CloudEvents dedup by `source` + `id` happens in `loams-fabric ingest` (§5.4), as on Loams streams (D270) |
| Connector SDK not published (`iggy_connector_sdk` 0.5.0, `publish = false`) | `core/connectors/sdk/Cargo.toml` | Loams's plugins are developed as upstream PRs in Iggy's workspace and built from a pinned Iggy revision |
| Multi-tenancy | Streams group topics; permissions are per user | One Iggy stream per Loams namespace, one Iggy user per namespace credential (§5.8, Q343) |

**How Iggy and the Loams WAL can fit together:**

| Option | What it means | For | Against |
|---|---|---|---|
| **A. Iggy as the stream engine for events, Loams WAL for databases and retrieval (proposed)** | Iggy holds event topics; Fluss holds event tables; the Loams WAL keeps §02's internal roles; bridges connect them | The owner's direction; each log keeps its optimizations; Iggy's transports, SDKs, connectors and MCP server come for free; contributions grow both projects | Two durable systems to operate; Iggy's data on disks until tiering lands; consistency tokens (D76) do not cross the bridge (a Fabric write becomes visible in a collection when `loams_sink` has appended it) |
| B. Iggy only as a protocol edge in front of the Loams WAL | Iggy topics with short retention, drained into Loams streams | One durable tier (D1) | Double write for every event; Iggy's consumer groups and replay are wasted; contradicts "separate is good" |
| C. Loams speaks Iggy's protocol over its own WAL | Iggy SDKs connect to Loams directly | One log | The protocol is pre-1.0 and changing; it rebuilds what Iggy already is; nothing goes upstream |
| D. Iggy replaces the Loams WAL everywhere | — | One log | Breaks Arm A (§28), consistency tokens, the leaderless object-store write path (D1, D12) and every M0–M1 gate; not on the table after the owner's input |

**Decision (D332): option A.** Iggy runs as the Fabric's ingest log in its own pods (StatefulSet with local NVMe, three replicas, `persisted` durability for event topics that feed Fluss), deployed by its Helm chart under Loams's umbrella chart (D186). **Trade-offs accepted:** a second durable system with disks; cross-log visibility through bridges rather than tokens; Iggy's protocol changes absorbed by pinning. **Revisit triggers:** Iggy 1.0 freezing the protocol; tiered storage with object storage as the source of truth (then the 72 h bound lifts and option A gains D1's property); the Kafka gateway reaching consumer-group offsets (then Q331 can close D74).

### 5.3 Apache Fluss: evaluation and fit (D333)

**What Fluss is** (1.0.0, 2026-09-22; read 2026-10-01): streaming storage for real-time analytics. **Log tables** are append-only, columnar (Arrow) logs; **Primary Key tables** hold upserts and deletes and emit a changelog, with merge engines **LastRow** (default), **FirstRow**, **Versioned** (a version column) and **Aggregation** (per-column functions, including `rbm32`/`rbm64` bitmaps), primary-key and prefix lookups, and KV snapshots. Coordinator and tablet servers (Java) coordinate through ZooKeeper; 1.0 adds coordinator HA with epoch fencing. Recent data is on tablet-server local disks; older log segments and KV snapshots move to **remote storage** (S3 and others); **lake tiering** writes Paimon, Iceberg, Hudi or Lance through the Flink tiering job, and 1.0 uses Arrow batches for tiering and supports Iceberg REST catalogs. Clients: Java, Flink and Spark connectors; `fluss-rs` (Rust, 1.0.0, arrow 59), Python and C++; the Fluss Gateway (Rust, REST, preview). A `fluss-kafka` module exists in the tree (status (verify)).

**Fit.** Fluss is the table half of the Fabric: what ClickHouse's merge-tree engines and the draft's "real-time tables" need (§7.4). Loams's own keyed and aggregating tables (§08 §1, M4) stay for data written through Loams; the two meet in Iceberg, through the one Lakekeeper catalog.

| Concern | Handling |
|---|---|
| JVM, ZooKeeper, local disks | Run unmodified as a StatefulSet; remote storage on RustFS; ZooKeeper 3-node in production, 1 in dev; footprint measured in FL1 Task 0 (Q345) |
| Flink tiering job | v1 runs it as a `FlinkDeployment` under the Flink Kubernetes Operator 1.16.1 that §26 already plans (D212), one job per cluster; in dev, a Flink 1.20 session cluster in compose. Replacing it with a Rust tiering service is Q332 |
| Two dedup semantics (Fluss merges at write; ClickHouse merges lazily) | Documented and allowlisted under category `merge-timing` (§8.10): Loams returns what ClickHouse returns after a full merge, which ClickHouse also may return at any time |
| Catalog | Lakekeeper (Apache-2.0, 0.13.6) as M4 planned (D6), brought forward into FL1 as a dev and Helm service; its own Postgres in dev (Q2 stays open for production, Q335) |

### 5.4 The envelope on Iggy and Fluss (D334)

The draft's envelope (tenant, schema id, event time, idempotency key, payload) maps onto CloudEvents 1.0 exactly as Loams streams store it (§02 §7.4):

| Draft field | CloudEvents | Iggy message | Fluss row |
|---|---|---|---|
| tenant | none; the namespace comes from the credential (Iggy user, API key) | the Iggy stream `ns-<namespace_id>` | the Fluss database `ns_<namespace_id>` |
| schema id | `dataschema` (URI) | header `ce_dataschema` | column `_ce_dataschema` |
| event time | `time` | header `ce_time`; the message timestamp stays Iggy's | column `_ce_time` (`TIMESTAMP_LTZ(3)`) |
| idempotency key | `source` + `id` | the Iggy message id = first 16 bytes of SHA-256(`source` ‖ 0x00 ‖ `id`) (`loams_cloudevents::CloudEvent::dedup_key`) | columns `_ce_source`, `_ce_id` |
| payload | `data` / `data_base64`, `datacontenttype` | payload bytes; header `content-type` | the table's data columns when the route declares a schema, else column `_ce_data` (`BYTES`) |
| other attributes | `type`, `subject`, extensions | headers `ce_<attr>` and `loams_ce_types` (D270's rules) | `_ce_type`, `_ce_subject`, `_ce_ext` (`MAP<STRING,STRING>`) |

**Deduplication.** `loams-fabric ingest` (the HTTP and gRPC CloudEvents route of the Fabric, FL1) uses D270's claim, append, complete protocol with its ledger in a Fluss PK table `_fabric.ce_dedup` (LastRow merge engine, so a claim is updated in place when it completes; key `(topic_id, dedup_key)`, value `(partition, offset, state, claimed_until)`), with the same 1-hour default window and 24-hour maximum. A retried request inside the window gets the first write's partition and offset. Events that reach Iggy directly through its SDKs are not deduplicated by Loams (Iggy's own per-session retry handling applies); downstream PK tables keyed by `(_ce_source, _ce_id)` dedupe by construction.

### 5.5 Adapters: protocols into the Fabric

| Protocol | Path | Built by | Phase |
|---|---|---|---|
| Iggy TCP, QUIC, HTTP, WebSocket | Direct to Iggy with Iggy SDKs | Iggy | FL1 |
| CloudEvents over HTTP (binary, structured, batched) and gRPC | `loams-fabric ingest`: `POST /v1/namespaces/{ns}/fabric/topics/{topic}/events`, `FabricService.ProduceCloudEvents`; same bindings and answers as §02 §7.4 | Loams (reuses `loams-cloudevents`) | FL1 |
| Webhooks (signed) | `loams-fabric ingest` with per-route signature verification (HMAC-SHA256 header, Stripe/GitHub/Slack schemes) → CloudEvents | Loams | CN1 |
| Kafka | Iggy's Kafka gateway (contribution, §5.9); until it is released, Camel `camel-kafka` → `camel-iggy` routes, or the native Kafka connector (§33) | Iggy + Loams contributions | CN1 (connector), later (gateway) |
| MQTT, AMQP, JMS, NATS, Pulsar, SQS, Pub/Sub, Event Hubs… | Camel routes `<component> → camel-iggy` in `loams-connect` | Camel | CN2 |
| Postgres CDC | Debezium Server (unmodified) with its HTTP sink posting CloudEvents to `loams-fabric ingest`, or Iggy's `postgres_source` in CDC mode (§33 D357) | Debezium / Iggy | CN1 |
| MySQL CDC | Debezium Server → `loams-fabric ingest` | Debezium | CN1 |
| OTLP (metrics, traces, logs as events) | `loams-fabric ingest` OTLP/HTTP receiver mapping each record to one CloudEvent; OTLP logs for search stay on §02 §7.1 | Loams | CN1 |
| S3 objects (new-object notifications, Parquet drops) | Native S3 source (§33) | Loams | CN1 |

### 5.6 Schemas and dead letters (D335)

- A Fluss table's schema is authoritative for rows in it; `ALTER TABLE ADD COLUMN` follows Fluss's schema evolution (verify the operations Fluss 1.0 supports).
- An Iggy topic is schemaless; producers name a schema in `dataschema`. `loams-fabric flow` stores schema documents in the bucket (`ns/<id>/fabric/schemas/<subject>/<version>`) and their versions in `_fabric.flow_objects` (`kind = 'schema'`), and checks compatibility (backward by default) when a route declares a schema.
- Dead letters: a route that cannot decode, validate or deliver an event writes it to `<topic>.dlq` with `ce_loamserror` (code and message) and `ce_loamsroute`; the DLQ is an ordinary Iggy topic with the route's retention.

### 5.7 Bridges (D336)

| Bridge | Mechanism | Exactly-once |
|---|---|---|
| Iggy → Fluss | Iggy connectors-runtime **sink plugin `fluss_sink`** (Rust, `fluss-rs`), contributed to `apache/iggy` under `core/connectors/sinks/fluss_sink`. Log tables: append. PK tables: upsert, or delete when `ce_loamsop = delete` | Fluss writes are not transactional with Iggy's consumer offset; the plugin commits the Iggy offset after Fluss acknowledges, so a crash replays a batch. PK tables: replays are idempotent. Log tables: **at-least-once**; the plugin writes `_ce_source`/`_ce_id`, so a reader can deduplicate itself (`LIMIT 1 BY _ce_source, _ce_id`); a House read-time dedup option is not in `chsurface-1` |
| Iggy → Loams | **Sink plugin `loams_sink`**: CloudEvents to a Loams stream through `ProduceCloudEvents` (D270 dedup by `source` + `id`), or documents to a collection through the native write API keyed by `_ce_source`/`_ce_id` | Exactly-once in the target: D270's ledger for streams, PK upserts for collections |
| Fluss → Iceberg | Fluss tiering (Flink job) into Lakekeeper | Fluss's own tiering commit protocol |
| Loams → Fabric | A link target `iggy` (FL3): a link from a Loams stream or changelog stream (D21) appends to an Iggy topic with message ids from the record's dedup key | At-least-once into Iggy; Fluss PK tables dedupe |
| Fabric → House | Not a bridge: the House reads Fluss and Iceberg directly (§7.5) | — |

### 5.8 Tenancy, auth and deployment

- **Namespaces.** A Loams namespace gets an Iggy stream `ns-<id>`, a Fluss database `ns_<id>`, a Lakekeeper namespace `ns_<id>` in the cluster's warehouse and a prefix `ns/<id>/fabric/` in the bucket. `loams-fabric` provisions all four idempotently.
- **Credentials.** In FL1–FL2 everything binds loopback (D111). With the unified auth plan (Q30), a Loams API key maps to an Iggy user (or personal access token) per namespace and to Fluss ACLs (1.0 changed ACLs, verify), and `loams-fabric` checks OpenFGA (D66) for route and table operations. Q343 asks whether Iggy users are minted per key or per namespace.
- **Secrets** for connectors go through Dapr's secrets building block via `loams-dapr` (D189).
- **Deployment.** Dev: `deploy/fabric/compose.yaml` (Iggy, Fluss with ZooKeeper, Lakekeeper with Postgres, RustFS, a Flink session cluster with the tiering job, `loams-fabric`). Kubernetes: subcharts of the umbrella chart (D186): Iggy's chart, a Fluss chart (Fluss ships Helm support (verify)), Lakekeeper's chart, the Flink operator, `loams-fabric`.
- **Usage hooks** (§27): `loams-fabric` exports per-namespace metrics (events in, bytes, House query CPU from chDB's `system.query_log` equivalents); metering stays in `loam-platform` (D190, D202).

### 5.9 Upstream contributions (owner: "we see more contributions to iggy and fluss")

| Project | Contribution | Why Loams needs it | When |
|---|---|---|---|
| Iggy | `fluss_sink` connector | Iggy → Fluss bridge | FL1 |
| Iggy | `loams_sink` connector | Iggy → Loams collections and streams | FL1 |
| Iggy | CloudEvents helpers in the connectors SDK (the `ce_` header layout, dedup ids) | One envelope across connectors | FL1 |
| Iggy | Kafka gateway: offset commit/fetch, produce path hardening, SASL mapped to Iggy users | Kafka clients into the Fabric; Q331 | After FL2 |
| Iggy | Tiered storage on object storage (discussion #3312), through `object_store` or OpenDAL, compio-safe | Long retention on the bucket; D1 for the edge | After the Kafka gateway; design with the Iggy maintainers first |
| Fluss | Rust tiering service on `fluss-rs` and iceberg-rust (no Flink) | Removes the Flink job (Q332) | Candidate, FL5+ |
| Fluss | Gateway record reads and caller authentication | A REST read path for light clients | Candidate |
| Fluss | `fluss-rs` gaps found by the House (lake-snapshot offsets, union-read helpers, write offsets in acknowledgements) | §7.5 needs them | FL2 |

Upstream rules follow §23 §2.2: one concern per PR, nothing posted without the owner's go-ahead per project, Loams carries a fix only between its merge and the next release.

## 6. Loams Flow

### 6.1 Routes (D337)

```yaml
apiVersion: loams.flow/v1
kind: Route
metadata: { name: orders-to-house, namespace: shop }
spec:
  from: { connector: debezium-postgres, instance: orders-db, tables: [public.orders] }
  steps:
    - filter: "type == 'io.debezium.postgresql.datachangeevent' && data.op != 'r'"
    - map:   { sql: "SELECT data.after.id AS id, data.after.total AS total, data.after.updated_at AS ver" }
  to:
    - fabric: { table: orders_current, merge: versioned, version: ver }   # Fluss PK table
    - loams:   { collection: orders_search, key: id }                      # through loams_sink
  delivery: { mode: at_least_once, dlq: true, max_batch: 65536, max_delay: 2s }
```

Routes are rows of `_fabric.flow_objects` (`kind = 'route'`), versioned: an update writes version *n*+1 and keeps *n* until no task runs it. A route is valid only if every connector capability it uses is declared (§33 D353); the API answers `FailedPrecondition` naming the missing capability otherwise.

### 6.2 Compiler targets (D338)

| Draft target | Compiles to | Owner of execution |
|---|---|---|
| 1. Trigger Engine / Loams Cells | A Loams Function (`fetch` contract, §24 D181) subscribed to the topic, or a durable function (§21) when the reaction waits | §24 runtime, §21 |
| 2. Fluss tables and merge engines | Fluss table DDL plus a `fluss_sink` instance | Iggy connectors runtime, Fluss |
| 3. House SQL windows, aggregations, MVs | A House MV (§8.3 Tier 2) = a per-insert-block SQL transform run by `loams-fabric house` on each batch the route delivers, appended to its target table; tumbling-window aggregates by `toStartOfInterval` with a mergeable state; **no watermarks or stream joins** (those go to RisingWave, D341) | `loams-fabric house` |
| 4. Graph writes | Vertex and edge upserts into the source collections of a mapped graph (§07 §7) through `loams_sink` | Loams engine |
| 5. DEMUX sinks (HTTP, Kafka, MQTT, gRPC) | Iggy connectors-runtime sinks where they exist, else `loams-connect` Camel routes `camel-iggy → <component>`, else native connectors (§33) | Iggy, Camel, Loams |
| (Loams-internal) | Links (§09) when both ends are Loams objects | Loams workers |

### 6.3 Delivery

Into the Fabric and into Loams objects: exactly-once in the target where the target dedupes (D334's dedup key, PK tables, D270's ledger); into external sinks: at-least-once, with `ce_id` and an `Idempotency-Key` header (HTTP sinks) or the sink's own idempotent producer (Kafka) where available. Ordering: per Iggy partition, and per key when the route partitions by `partitionkey`.

### 6.4 Where it runs (D339)

`loams-fabric flow` is a stateless service: routes in the metastore, connector instances supervised as Iggy connectors-runtime processes, `loams-connect` routes or Debezium Server instances (one per instance, §33 D354), and native connectors as tasks in `loams-fabric`. Its state is in `_fabric.flow_objects` (D337); in CN1 one `flow` process supervises every instance, and FL3 adds task leases through the Loams metastore's network API (the §09 §6 lease model), so the `fabric/` workspace never links `loams-meta`. By open-core.md, all of this is open: a single organisation needs connectors to self-host. `loam-platform` adds the fleet (per-tenant connector pods, autoscaling, plan limits, billing).

### 6.5 UI (D340)

A console page lists routes, connector instances, lag and DLQ counts, and edits routes as YAML with a diagram. Embedding Karavan's designer for Camel-backed steps is Q340.

## 7. Loams House

### 7.1 Pattern

The draft's pattern, "WAL → immutable S3 segments → embedded engine → compat shim", holds with the Fabric as the WAL: Fluss's log and KV are the write-ahead state, tiered Iceberg files on RustFS are the immutable segments, chDB is the embedded engine, and the ClickHouse HTTP interface is the shim. House workers are stateless and disposable.

### 7.2 chDB (D342)

`chdb-core` packages ClickHouse as `libchdb` with a C ABI (`chdb.h`): connections and sessions, buffered and streaming queries, parameterized queries, Arrow output and Arrow input through the C Data Interface, and every ClickHouse input and output format. Versions track ClickHouse (26.9.0, 2026-09-28). Loams's crate `loams-chdb-sys` (bindgen over the pinned `chdb.h`) and `loams-chdb` (a safe wrapper: `Engine` as a process singleton, `Session`, `Query` streams, `register_arrow_stream`, cancellation, memory limits) are written against that ABI; `chdb-rust` 2.0 is the reference and may be used directly if FL2 Task 0 finds its API stable enough (Q337). `libchdb` is fetched by SHA-256 from the `chdb-core` release in the build script (or taken from `LIBCHDB_DIR`), never built from source.

Rejected alternatives:

| Alternative | Why not |
|---|---|
| Unmodified `clickhouse-server` reading Iceberg | It speaks 8123 and 9000 natively, but it cannot read Fluss's unsettled tail or honour a consistency token, so reads lag tiering by minutes; DDL and `INSERT` must still be intercepted; per-tenant isolation in one server is weak. It stays the **reference** for the differential harness (D349) |
| ClickHouse dialect on DataFusion | Rebuilds ClickHouse's function library, type rules and formats by hand; the differential harness would fail for years |
| DuckDB (MIT) | Not the ClickHouse dialect |

### 7.3 The binary and the workspace (D343)

```
fabric/                         # a Cargo workspace of its own; its own Cargo.lock; the shared target dir
  Cargo.toml                    # arrow 59 (fluss-rs), tokio, axum 0.8, connectrpc 0.9
  crates/loams-chdb-sys/         # bindgen over chdb.h; build.rs fetches libchdb by digest
  crates/loams-chdb/             # safe wrapper
  crates/loams-fabric-envelope/  # CloudEvents ↔ Iggy message, ↔ Fluss row (uses loams-cloudevents' core, §FL1 Task 2)
  crates/loams-fabric-ingest/    # CloudEvents HTTP/gRPC → Iggy, dedup ledger on Fluss
  crates/loams-house/            # ClickHouse HTTP, classifier, catalog, DDL/INSERT, read planner
  crates/loams-flow/             # routes, compiler, registry (§33), supervisors
  crates/loams-fabric/           # the binary: loams-fabric {ingest|house|flow|all} --config …
  crates/loams-house-conformance/# the differential harness (D349)
```

Iggy connector plugins live in Iggy's own workspace (upstream PRs) and are built from a pinned Iggy revision in CI. The engine workspace changes by one small PR: `loams-cloudevents` gets a default feature `log` around its record layout, so the Fabric can depend on its codec without `loams-log`.

### 7.4 Tables and engines (D344)

| ClickHouse DDL | Fluss table | Iceberg (tiered) | Notes |
|---|---|---|---|
| `ENGINE = MergeTree ORDER BY (a, b) PARTITION BY toYYYYMM(ts)` | Log table; bucket key from `ORDER BY`'s first column unless `SETTINGS loams_bucket_key` | Sort order `(a, b)`; partition `month(ts)` | `PARTITION BY` accepts `toYYYYMM`, `toYYYYMMDD`, `toDate`, `toStartOfHour`, identity of a column; anything else is refused with code 36 (`BAD_ARGUMENTS`) in `chsurface-1` |
| `ReplacingMergeTree(ver)` | PK table, PK = `ORDER BY` columns (or `PRIMARY KEY` if given), `table.merge-engine = versioned`, version column `ver` | PK table tiering (verify Fluss's Iceberg layout for PK tables: equality deletes or merge-on-read) | Equal versions: ClickHouse keeps the last inserted; Fluss Versioned keeps (verify); a difference is an allowlist entry |
| `ReplacingMergeTree(ver, is_deleted)` | As above; a row with `is_deleted = 1` is written as a Fluss delete | As above | ClickHouse keeps deleted rows until `FINAL` or `OPTIMIZE … CLEANUP`; Loams drops them at once → `merge-timing` |
| `ReplacingMergeTree` | PK table, `last_row` | As above | |
| `SummingMergeTree` / `SummingMergeTree((c1, c2))` | PK table, `aggregation` with `sum` on the listed (else all non-key numeric) columns, `last_value` on the others | As above | ClickHouse takes "an arbitrary value" for non-summed columns; corpus tests avoid relying on them |
| `Replicated…MergeTree('/path', '{replica}')`, `ON CLUSTER c` | As the unreplicated engine | — | Arguments accepted and ignored; replication is Fluss's |
| `Dictionary` (Tier 2) | A PK table read by key | — | `dictGet` rewritten to a join on the PK table |

The original `CREATE TABLE` text, the engine and the ClickHouse types go into Fluss table properties `loams.ch.ddl`, `loams.ch.engine`, `loams.ch.types` (custom properties, verify), so the House needs no catalog of its own. Types map ClickHouse → Fluss → Iceberg as in FL2 Task 5's table; types with no Fluss counterpart (`Enum8`, `LowCardinality`, `Nested`, `AggregateFunction`) are stored as their base type with the ClickHouse type kept in `loams.ch.types` and restored in query views.

### 7.5 Reads (D345)

For each query, the House worker:

1. Classifies the statement (§FL2 Task 4) and resolves its tables.
2. Pins, per table, the last tiered Iceberg snapshot and the Fluss log offsets that snapshot covers (Fluss records the lake snapshot's offsets per bucket, verify the `fluss-rs` API), plus the current Fluss high watermarks — or the offsets in the request's `X-Loams-Consistency-Token`, waiting up to `loams_consistency_wait_ms` (default 5 000) until the tail reaches them.
3. Registers in the chDB session, per table, an Arrow stream of the Fluss log between the snapshot offsets and the pinned high watermark, and defines a temporary view `db.t` = `icebergS3(<warehouse>, …, iceberg_metadata_file_path = '<pinned>')` `UNION ALL` the stream, with the ClickHouse types restored. For PK tables the view keeps, per key, the row with the highest `(_bucket, _offset)` and drops deletes (`… ORDER BY _offset DESC LIMIT 1 BY <pk>` over the union, where lake rows rank below tail rows).
4. Runs the user's query text unchanged against those views (within the supported surface of §8 and the deny list of §7.8), with `FINAL` accepted as a no-op, and streams chDB's output in the requested format.

Caching: chDB's own filesystem cache on local NVMe for S3 reads in FL2; sharing Loams's foyer cache is later work.

### 7.6 Writes and DDL (D346)

- `CREATE DATABASE d` creates a Fluss database `ns_<id>__d` (verify allowed characters) and maps `d` in the session; `default` always exists.
- `CREATE TABLE` maps per §7.4 and enables tiering (`table.datalake.enabled = true`, freshness from `SETTINGS loams_tiering_freshness`, default 60 s).
- `INSERT INTO t [(cols)] VALUES … | FORMAT <fmt> <data> | SELECT …`: the worker parses the body with chDB into Arrow batches of the table's schema (FL2 Task 6 fixes the mechanism), writes them with `fluss-rs` (append for Log tables, upsert or delete for PK tables), and answers once Fluss acknowledges; the summary header `X-ClickHouse-Summary` carries `written_rows` and `written_bytes`, and `X-Loams-Consistency-Token` the bucket offsets. `async_insert = 1` is accepted and served synchronously.
- `DROP TABLE`, `TRUNCATE TABLE` (Log tables: drop and recreate, verify Fluss), `ALTER TABLE ADD COLUMN` (Fluss schema evolution) are in `chsurface-1`; `ALTER … UPDATE/DELETE` (mutations), lightweight `DELETE FROM`, `OPTIMIZE` (accepted as no-op), `RENAME` and projections are not.

### 7.7 Graph (D350)

§07's native mapped graphs (D44) stay the graph surface: they are the GraphRAG workload's shape (seeds → 1–2 hops → rerank, in one planned query), they ship in M3, and they read the same collections that hold the vectors. Grafeo was checked against the draft's claims (§3): its feature list is real and broad, but its repository is eight months old (created 2026-01-26); it would add a second storage engine with local persistence, a second HNSW, arrow 60 beside Loams's 58, and a single-maintainer dependency. If owners of Neo4j-shaped apps ask for Cypher or GQL, the low-risk path is a Flow sink that keeps a `grafeo-server` (Bolt v5, GQL over gRPC) copy of a mapped graph, run as a separate service (Q339). Graph conformance suites in the draft (openCypher TCK, LDBC SNB, SPARQL, Gremlin) apply only to that companion, not to Loams.

### 7.8 Security of table functions

In `chsurface-1`, these chDB table functions and engines are **disabled** in House sessions (`SUPPORT_IS_DISABLED`, code 344): `file`, `url`, `urlCluster`, `remote`, `remoteSecure`, `cluster`, `mysql`, `postgresql`, `odbc`, `jdbc`, `hdfs`, `s3`, `s3Cluster`, `gcs`, `azureBlobStorage`, `iceberg*`/`deltaLake*`/`hudi` with user-supplied URLs, `executable`, `input` outside `INSERT`, `dictionary` sources outside Loams, `system.*` writes, and the `Kafka`, `RabbitMQ`, `NATS`, `S3Queue` engines. The worker itself uses `icebergS3` with Loams-generated arguments only. Allowed: `numbers`, `numbers_mt`, `zeros`, `values`, `generateRandom`, `format`, `null`, `view`, `merge` over Loams tables. A per-namespace allowlist for `s3` on the namespace's own prefix with vended credentials is later work. chDB's process-wide signal handlers are declined at engine start (`chdb-rust`'s runtime control, verify in the C ABI) so the worker owns its signals.

## 8. ClickHouse compatibility surface v1 (`chsurface-1`, D347)

The surface is a file in code (`fabric/crates/loams-house/src/surface.rs`, one table) from which `docs/guides/clickhouse-surface.md` is generated, with a test that fails on drift, as D88 does for limits. ClickHouse version: the version `chdb-core` carries (26.9 at FL2), which is also the reference server's.

### 8.1 Interface: HTTP (port 8123)

| Feature | v1 |
|---|---|
| `GET /` and `GET /ping` → `Ok.\n` | Yes |
| Query in `?query=` (GET: read-only), in the POST body, or `?query=` prefix + body (INSERT data) | Yes |
| `database`, `default_format`, `query_id`, `session_id`, `session_timeout`, `session_check`, `readonly`, settings as URL parameters | Yes |
| Auth: `X-ClickHouse-User`/`X-ClickHouse-Key`, HTTP Basic, `user`/`password` parameters | Yes (dev: a static map to namespaces; with Q30, API keys) |
| Compression: `enable_http_compression=1` with `Accept-Encoding: gzip, deflate, zstd, lz4, br` (verify the list in 26.9); request bodies with `Content-Encoding` | gzip, deflate, zstd in v1; lz4, br (verify) |
| `X-ClickHouse-Query-Id`, `X-ClickHouse-Format`, `X-ClickHouse-Timezone`, `X-ClickHouse-Server-Display-Name`, `X-ClickHouse-Summary`, `X-ClickHouse-Exception-Code` response headers | Yes |
| `send_progress_in_http_headers`, `wait_end_of_query`, `buffer_size` | Accepted; progress headers emitted at the end in v1 |
| `/play`, `/dashboard`, `/replicas_status`, predefined HTTP handlers, `/metrics` | No (`/metrics` is `loams-fabric`'s Prometheus endpoint) |
| Multi-statement in one request | No (as ClickHouse, which refuses them) |

### 8.2 Statements

`SELECT` (ClickHouse's SELECT syntax as chDB parses it, executed over Loams tables, §7.5, with the functions of §8.6 and the table-function restrictions of §7.8), `WITH`, `UNION`, `INSERT … VALUES | FORMAT | SELECT`, `CREATE DATABASE [IF NOT EXISTS]`, `CREATE TABLE [IF NOT EXISTS] … ENGINE = <Tier 1> …`, `CREATE TABLE … AS SELECT` (Tier 1 engines), `DROP DATABASE|TABLE [IF EXISTS]`, `TRUNCATE TABLE`, `ALTER TABLE … ADD COLUMN`, `SHOW DATABASES|TABLES|CREATE TABLE|COLUMNS`, `DESCRIBE TABLE`, `EXISTS TABLE`, `USE`, `SET`, `EXPLAIN` (AST, SYNTAX, PLAN; passed to chDB), `OPTIMIZE TABLE … [FINAL]` (no-op), `KILL QUERY WHERE query_id = …` (own session's queries). Everything else answers code 48 (`NOT_IMPLEMENTED`) with the statement kind named.

### 8.3 Engines

| Tier | Engine or feature | `chsurface-1` | Implementation |
|---|---|---|---|
| 1 | `MergeTree` | Yes | §7.4 |
| 1 | `ReplacingMergeTree` (0, 1 or 2 arguments) | Yes | §7.4 |
| 1 | `SummingMergeTree` (0 or 1 argument) | Yes | §7.4 |
| 1 | `Replicated*` prefixes, `ON CLUSTER` | Accepted, ignored | §7.4 |
| 2 | Materialized views (`CREATE MATERIALIZED VIEW mv TO t AS SELECT …`) | FL3 | A per-insert-block transform on the source table's writes, run by the House on each batch Fluss acknowledges for the source (a Fluss log subscription from the MV's committed offset) |
| 2 | `Kafka` / `S3Queue` engines | FL3 | A Flow route into the table, created from the DDL |
| 2 | `Distributed` | FL5 | The coordinator's scatter-gather (§9); in `chsurface-1`, accepted as an alias of its local table |
| 2 | `Dictionary`, `dictGet*` | FL3 | §7.4 |
| 2 | `Memory`, `Log`, `TinyLog`, `Null` | Session-local temporary tables only | chDB's own, dropped with the session |
| 3 | `AggregatingMergeTree`, `-State`/`-Merge` columns | FL6 | Fluss Aggregation cannot hold ClickHouse's opaque states; stored as `BYTES` and merged in a compaction job that runs chDB itself |
| 3 | `CollapsingMergeTree`, `VersionedCollapsingMergeTree` | FL6 | Sum aggregation on `sign` with dedicated tests; approximations are allowlisted |
| 3 | Projections, TTL moves, `ALTER … UPDATE/DELETE`, lightweight deletes | FL6 or refused | Emulated or refused explicitly |
| — | `ReplicatedMergeTree` Keeper paths, `system.replicas`, `SYSTEM SYNC REPLICA` | Never | Replication is Fluss's |

### 8.4 Formats

Output and input, through chDB: `TabSeparated` (and `WithNames`, `WithNamesAndTypes`, `Raw`), `CSV` (and `WithNames`, `WithNamesAndTypes`), `JSON`, `JSONCompact`, `JSONEachRow`, `JSONCompactEachRow` (and `WithNames…`), `JSONStringsEachRow`, `RowBinary` (and `WithNames`, `WithNamesAndTypes`; the Rust and Go HTTP clients use it), `Native` (`clickhouse-connect` uses it over HTTP), `Parquet`, `Arrow`, `ArrowStream`, `Values`, `Pretty*` (output only), `Null` (output only). Others answer code 73 (`UNKNOWN_FORMAT`, verify).

### 8.5 Settings

Passed to chDB when in the allowlist: query complexity (`max_execution_time`, `max_result_rows`, `max_result_bytes`, `result_overflow_mode`, `max_rows_to_read`, `max_bytes_to_read`, `max_memory_usage` capped by the namespace's quota), formats (`output_format_*`, `input_format_*`, `date_time_input_format`, `date_time_output_format`), `join_algorithm`, `max_threads` (capped), `session_timezone`, `allow_experimental_*` for analyzer features chDB enables by default, `insert_deduplicate` (accepted), `async_insert` (§7.6), `wait_for_async_insert`. Loams settings: `loams_consistency_wait_ms`, `loams_tiering_freshness`, `loams_bucket_key`. Unknown settings answer code 115 (`UNKNOWN_SETTING`); known but disallowed ones code 164 (`READONLY`).

### 8.6 Functions

Supported and allowlisted: chDB's scalar and aggregate functions, except the disabled table functions of §7.8 and functions that read the host (`hostName`, `getMacro`, `filesystem*`, `getSetting` of server settings; they return Loams values or code 344). `version()` returns the chDB ClickHouse version; `currentDatabase()` the mapped database.

### 8.7 System tables

Synthesized from the Fluss catalog and the session: `system.databases`, `system.tables` (with `engine` and `create_table_query` from `loams.ch.*` properties), `system.columns`, `system.parts` (one synthetic part per Iceberg data file, for tools that count rows), `system.settings`, `system.functions`, `system.formats`, `system.data_type_families`, `system.one`, `system.numbers`, `system.processes` (own namespace), `system.query_log` (own namespace, recent queries kept by `loams-fabric`), `system.build_options` (chDB's), `system.contributors` (chDB's). Others answer code 60 (`UNKNOWN_TABLE`).

### 8.8 Errors

Errors carry ClickHouse's text form `Code: <n>. DB::Exception: <message>. (<NAME>) (version <v>)`, the header `X-ClickHouse-Exception-Code`, and ClickHouse's HTTP status for the code (400 syntax, 401/403 auth, 404 unknown table or database, 500 otherwise; verify the mapping in 26.9). Errors from chDB pass through unchanged. Codes Loams raises itself: 36 `BAD_ARGUMENTS`, 48 `NOT_IMPLEMENTED`, 57 `TABLE_ALREADY_EXISTS`, 60 `UNKNOWN_TABLE`, 62 `SYNTAX_ERROR`, 81 `UNKNOWN_DATABASE`, 115 `UNKNOWN_SETTING`, 159 `TIMEOUT_EXCEEDED`, 164 `READONLY`, 202 `TOO_MANY_SIMULTANEOUS_QUERIES`, 241 `MEMORY_LIMIT_EXCEEDED`, 344 `SUPPORT_IS_DISABLED`, 372 `SESSION_NOT_FOUND`, 373 `SESSION_IS_LOCKED`, 394 `QUERY_WAS_CANCELLED`, 497 `ACCESS_DENIED`, 516 `AUTHENTICATION_FAILED` (each checked in `src/Common/ErrorCodes.cpp` at v26.9.8.3-stable, together with 73 `UNKNOWN_FORMAT` and 210 `NETWORK_ERROR`; recheck at the pinned version).

### 8.9 Out of `chsurface-1`

The native protocol (FL5), the MySQL and Postgres wire interfaces of ClickHouse, gRPC interface, `Distributed` fan-out (FL5), Tier 3 engines, mutations, row policies, users/roles/quotas DDL (`CREATE USER`, `GRANT`; Loams's auth applies), `SYSTEM` commands, backups, Keeper, `clickhouse-local` file semantics.

### 8.10 The allowlist process (D348)

- **Versioning.** The surface has a semver id: `chsurface-1.0` at FL2's exit. Adding statements, formats or engines is a minor bump; removing anything or changing a result is a major bump and needs the owner.
- **Entries.** `conformance/clickhouse/allowlist.toml`:

```toml
[[deviation]]
id        = "CH-0007"
tests     = ["corpus/replacing/equal_versions.sql", "stateless/01509_*"]
category  = "merge-timing"     # merge-timing | semantic | error-text | type-display | unsupported | performance
surface   = "1.0"
reason    = "Fluss Versioned keeps the first row on equal versions; ClickHouse keeps the last inserted"
owner     = "house"
issue     = "ostrium-labs/loams#NNN"
expires   = "chsurface-2.0"    # optional
approved  = "owner 2026-10-15" # required when category = "semantic"
```

- **Strictness.** The harness runs every allowlisted test; an allowlisted test that now passes fails CI with "unexpected pass: remove CH-0007" (strict xfail). A test not on the allowlist that fails blocks the PR.
- **Who approves.** `error-text`, `type-display` and `performance` entries: any reviewer. `merge-timing` and `unsupported`: the House owner. `semantic` (a different answer to a query inside the surface): the owner, recorded in `approved`.
- **Publication.** Each build publishes `house-compat-<sha>.json` and a Markdown summary (pass rate per category and per corpus, the allowlist size per category). The docs page shows the latest release's numbers.
- **Release gate.** A release is blocked if any declared-surface test regresses against the previous release, or the allowlist grows without an approved entry.

## 9. Distributed and S3-backed design

| Concern | Design |
|---|---|
| Metastore | Fluss's coordinator for tables; Loams's `MetaStore` (through the network API) for routes, schemas and House placement; the draft's "Raft or Postgres" is moot |
| Placement | House queries for a namespace go to the House workers that own `(ns, table)` by the rendezvous hashing with bounded load of D75, so chDB's filesystem cache warms per table |
| Reads at scale (FL5) | A coordinator role splits a query over the pinned file list and Fluss buckets: workers run the query's first stage with `-State` combinators (`sumState`, `uniqState`, …) over their share, the coordinator merges with `-Merge` in its own chDB session, ClickHouse's own two-stage aggregation; queries the splitter cannot decompose run on one worker |
| Writes | The Fluss acknowledgement is the commit point; tiering commits Iceberg snapshots atomically through Lakekeeper; House workers never write object storage |
| Cache | chDB filesystem cache on worker NVMe; Fluss and Iceberg files are immutable, so the cache needs no invalidation |
| Failure | Workers are disposable: a query on a lost worker fails with code 210 (`NETWORK_ERROR`, verify) and the client retries; nothing is lost because workers hold no state |
| Native protocol 9000 (FL5) | A Loams listener for the ClickHouse native protocol (handshake, Query, Data blocks in `Native` format produced by chDB, Progress, Exception, EndOfStream), so `clickhouse-client` and the native drivers connect |

## 10. Compatibility test plan (D349)

| Suite | What | Gate |
|---|---|---|
| Declared-surface corpus | `conformance/clickhouse/corpus/**.sql` with expected outputs generated by the reference server and checked in; each file runs on Loams and the reference | FL2 exit: 100 % minus allowlist |
| Upstream stateless tests | `tests/queries/0_stateless/*.sql` + `.reference` (Apache-2.0) at the pinned ClickHouse tag, filtered by a manifest to tests whose statements, engines and functions are inside the surface | Pass rate tracked from FL2; gated from FL3 |
| Engine semantics | Per Tier 1 engine: insert orders, versions, deletes, `FINAL`, `OPTIMIZE`, partitions; reference vs Loams with the reference also run after `OPTIMIZE TABLE … FINAL` so `merge-timing` entries are exact | FL2 |
| ClickBench | The 43 queries on `hits` partitions 0–9 (about 10 M rows, pinned by SHA-256); correctness gated, latency tracked; the queries are CC BY-NC-SA 4.0, fetched at test time from a pinned commit, credited, never vendored, for internal correctness and benchmarking only (D414) | FL2 correctness |
| TPC-H | SF1 22 queries, results equal to the reference | FL2 |
| Drivers | `clickhouse-rs` (Rust, in `cargo test`), `clickhouse-connect` (Python, `uv`), `clickhouse-go` v2 HTTP, `clickhouse-java` v2 (JDBC over HTTP, nightly; a third-party driver, so the test runs even while Loams's own Java work is deferred) | FL2 (Rust, Python), FL3 (Go, Java) |
| Wire formats | Byte equality of every declared output format on the corpus between Loams and the reference (after normalizing `X-ClickHouse-Server-Display-Name`, timings and query ids) | FL2 |
| Faults | RustFS through toxiproxy (latency, 503s, partial reads, connection resets); kill a House worker mid-query; kill Fluss tablet servers and the tiering job mid-write; every corpus test under every mode | FL2 (single node), FL5 (distributed) |
| Reference cluster | A 2-shard, 2-replica ClickHouse cluster vs Loams on identical data, for `Distributed` | FL5 |

## 11. Phases: track FL (D351)

| Milestone | Scope | Depends on | Exit gate |
|---|---|---|---|
| **FL0** | This document, §33 and their decision rows | — | Owner review |
| **FL1: Fabric foundation** ([plan](../plans/2026-10-01-fl1-fabric-foundation.md)) | `fabric/` workspace; dev compose (Iggy, Fluss, ZooKeeper, Lakekeeper, RustFS, Flink + tiering); the envelope on Iggy and Fluss; `loams-fabric ingest` with dedup; namespace provisioning; `fluss_sink` and `loams_sink` plugins (upstream PRs); one PK-table use case tiered to Iceberg; end-to-end fault gate | M1.2 (native API), PRs #170/#171 (D270) for `loams_sink` to streams | An event acknowledged by `ingest` is in Iggy, in the Fluss table and, after tiering, in Iceberg, through kills of each component, with no duplicates inside the dedup window |
| **FL2: House SQL phase 1** ([plan](../plans/2026-10-01-fl2-house-sql.md)) | `loams-chdb`; HTTP 8123; classifier; DDL and `INSERT` to Fluss; union reads; Tier 1 engines; system tables; errors; the differential harness, corpora and the allowlist; Rust and Python drivers; ClickBench/TPC-H correctness | FL1 | `chsurface-1.0`: 100 % of the declared corpus minus allowlist; Rust and Python driver suites; strict allowlist in CI |
| **FL3: Flow** | Route API and compiler (§6); MVs; Kafka/S3Queue engines as routes; Dictionary; DEMUX sinks; link target `iggy`; Go and Java drivers | FL2, CN1 | A route from Postgres CDC to a Fluss table and a Loams collection, and an MV, pass their tests; upstream stateless pass rate gated |
| **FL4: Graph alignment** | M3's native graph as planned (§07); a `grafeo-server` sink only if Q339 says so | M3 | M3's gates |
| **FL5: Distribution** | Coordinator scatter-gather; native protocol 9000; reference-cluster differential; Fluss tiering replacement if Q332 chooses it | FL2 | Distributed corpus equal to the reference cluster; `clickhouse-client` and native drivers pass |
| **FL6: Hard engines** | Aggregating, Collapsing, projections; mutations or explicit refusals | FL5 | Tier 3 corpus at its declared level |
| **CN1: ★ connectors** ([plan](../plans/2026-10-01-cn1-starred-connectors.md)) | §33's registry, capability schema and the starred set | FL1 | §33 §9 |

## 12. Licences

| Component | Licence | How used |
|---|---|---|
| Apache Iggy (server 0.9.0, SDK 0.11.0, connectors) | Apache-2.0 (ASF top-level project since 2026-08-19) | Separate service; plugins contributed upstream; SDK linked by `loams-fabric` |
| Apache Fluss 1.0.0, `fluss-rs` 1.0.0 | Apache-2.0 (ASF TLP) | Separate service; `fluss-rs` linked |
| ZooKeeper | Apache-2.0 | Separate service (Fluss) |
| Apache Flink + Flink Kubernetes Operator 1.16.1 | Apache-2.0 | Separate services (Fluss tiering) |
| Lakekeeper 0.13.6 | Apache-2.0 | Separate service |
| chDB (`chdb-core` 26.9.0, `libchdb`), `chdb-rust` 2.0.0 | Apache-2.0 | `libchdb` dynamically linked by `loams-fabric`; NOTICE carries ClickHouse's and chDB's notices |
| ClickHouse server (reference) | Apache-2.0 | CI-only container, never distributed |
| Grafeo, `grafeo-server` | Apache-2.0 | Not used (D350); possible companion |
| Apache Camel 4.22.1, Karavan 4.18.1 | Apache-2.0 | §33 |
| Kestra 2.0.4 (OSS core) | Apache-2.0; Enterprise Edition proprietary | §33: not a runtime |
| Debezium 3.7.0.Final, Debezium Server | Apache-2.0 | §33: separate service |

Nothing AGPL, BSL, SSPL or ELv2 is linked or required (D11).

## 13. Risks

| # | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| FL-R1 | Iggy's pre-1.0 protocol changes break SDKs and plugins between releases | High | Medium | Pin server, SDK and plugin revision together; upgrade as a tested set (FL1 Task 0 per release) |
| FL-R2 | Iggy data on local disks is lost with a replica set | Low | High | Three replicas, `persisted` durability, bounded retention with Fluss as the longer-lived copy; tiered storage contribution |
| FL-R3 | Fluss's JVM, ZooKeeper and Flink tiering job make small deployments heavy | High | Medium | Measure (Q345); a single-node dev profile; the Rust tiering candidate (Q332) |
| FL-R4 | `chdb-core` ABI changes or lags ClickHouse | Medium | Medium | Own FFI over a pinned `chdb.h`; ABI test in CI; the reference server pinned to the same ClickHouse version |
| FL-R5 | Union reads (Iceberg snapshot ∪ Fluss tail) need `fluss-rs` APIs that do not exist yet | Medium | High | FL2 Task 0 checks; contribute (§5.9); fallback: read only tiered data with a staleness bound and say so in a header |
| FL-R6 | Users read "ClickHouse compatible" as 100 % of ClickHouse | High | Medium | The declared surface page with pass rates; error code 48 naming what is out |
| FL-R7 | Two logs confuse users about where to write | Medium | Medium | §5.1's rule in the docs; Flow routes are the documented way to move data |
| FL-R8 | Merge-timing differences break apps that rely on duplicates before merge | Low | Low | Allowlisted and documented; such apps are already non-deterministic on ClickHouse |
| FL-R9 | libchdb's size and memory per process | Medium | Medium | One `house` process per node with sessions; memory caps per query; measured in FL2 Task 0 |
| FL-R10 | Table functions open SSRF or local file reads | Medium | High | §7.8 deny-by-default with a test per function |
| FL-R11 | Upstream PRs are slow, leaving Loams on patch branches | Medium | Medium | Small PRs, early design discussions with maintainers, the owner's go-ahead per project |

## 14. Conflicts with existing decisions, and how they are resolved

| Earlier | Conflict | Resolution |
|---|---|---|
| **D45** (no ClickHouse surface) and §00 §7 ("not a ClickHouse protocol emulator") | The draft adds a ClickHouse HTTP and native surface | **D347 reverses D45** for the House only, as a declared surface on a separate binary; §08's Iceberg-only engine stays for Loams tables; approved 2026-10-02 (Q333) |
| **D42** (protocol footprint) | Adds ClickHouse HTTP to the protocols Loams speaks | D347 amends D42; the engine binary's footprint is unchanged |
| **D74** (Kafka gateway in the Loams binary, M5) and D22 (RisingWave over it) | The owner puts event ingestion on Iggy; Iggy has a Kafka gateway in development | **Approved 2026-10-02 (Q331, D408):** event-ingestion Kafka clients use Iggy's gateway with Loams's contributions; D74's Loams-side gateway is deferred, not cancelled. RisingWave reads Fluss (its Fluss source, verify) or Iggy's gateway |
| **D1** (object storage is the only durable source of truth) | Iggy and Fluss keep recent data on local disks | Like Live (D130) and jobs (§26), the Fabric is a service whose store is not the bucket; Fluss remote storage and Iceberg put cold data on RustFS; Iggy tiering is a contribution |
| **D4** (the log is the spine) and **D5** (links replace connector glue) | Flow routes and connectors | Routes compile to links for Loams-to-Loams work (D338); connectors cover external systems, which links never did |
| **D44** (native graph, no Cypher/Bolt) vs the draft's Grafeo | The draft adds Grafeo with GQL/Cypher/Bolt | D350 keeps D44; Grafeo only as an optional companion (Q339) |
| **D51** (no embedded engines) | chDB and `fluss-rs` linked | D343 amends D51: the engine binary stays free; the separate `loams-fabric` binary may link libraries of other engines |
| **D6** (Iceberg via Lakekeeper) and Q2 | FL1 needs a catalog before M4 | Lakekeeper is brought forward as a dev and Helm service; Q2 stays open for its production backend |
| **D-SC-15** (OpenPanel's ClickHouse queries rewritten for DataFusion) | With the House, OpenPanel can run its ClickHouse queries unchanged | Proposed (Q334): the OpenPanel fork narrows to configuration once FL2 passes OpenPanel's query set, which becomes a corpus |
| **D-SC-13** (PostHog) | Withdrawn by D-SC-15 already | No change |
| §26 (D204, D211, D212: hosting Spark and Flink) vs the draft's "no Spark/Flink" | — | Compatible: Flow never targets them (D341); §26 hosts users' jobs; Fluss's tiering job reuses D212's operator |
| §09 ("no windowed aggregations with watermarks") vs "House SQL for windows" | — | House MVs are per-insert-block transforms, as ClickHouse's MVs are; watermark windows go to RisingWave (D341) |
| §06 (Qdrant layer; one vector source per collection) | The draft's "WAL replay into qdrant-edge" and Grafeo's HNSW | §06 unchanged; D350 |
| D184 (the runtime's Rust gateway) | The draft's "Loams Gateway (protocol shims, scatter-gather)" | Renamed the House coordinator (D330) |
| D260 (no TiDB) | None; the draft's metastore choice is moot | — |
| D270 (CloudEvents on streams) | The draft's own envelope | D334 uses D270's layout on Iggy and Fluss |
| D20 / D21 (ideas from Fluss: `arrow` encoding, changelog streams) | Fluss itself is now in the stack | Both stay for Loams's own objects; Fluss holds Fabric tables |
| D88 (limits page) | New limits on the House | `chsurface` page lists House limits the same way |

## 15. Open questions

| # | Question | Owner | Needed by |
|---|---|---|---|
| Q330 | Confirm option A (§5.2): Iggy as the event stream engine beside the Loams WAL, with bounded retention until Iggy's tiered storage lands | Founder | FL1 Task 0 |
| Q331 | ~~D74: defer Loams's own Kafka gateway and contribute to Iggy's (proposed), keep D74 in M5 as well, or cancel D74~~ Answered 2026-10-02 by the owner: the recommended default — defer D74's Loams-side Kafka gateway and contribute to Iggy's Kafka gateway instead; D74 is deferred, not cancelled (D408; §32 §14) | Founder | Resolved |
| Q332 | ~~Fluss tiering in v1: the Flink job under the operator (proposed), or a Rust tiering service first~~ Answered 2026-10-02 by the owner: the recommended default — the Fluss tiering Flink job under the Flink Kubernetes Operator in v1; a Rust tiering service stays an FL5+ candidate (D333) | Founder | Resolved |
| Q333 | ~~Approve reversing D45 for the House, and `chsurface-1`'s scope (§8)~~ Answered 2026-10-02 by the owner: the recommended default — approved: D347 reverses D45 for the House only, a declared surface on a separate binary, with `chsurface-1` scoped as §32 §8 lists it; §08's Iceberg engine stays for Loams tables | Founder | Resolved |
| Q334 | ~~D-SC-15: run OpenPanel's ClickHouse queries unchanged on the House (proposed) instead of rewriting them for DataFusion~~ Answered 2026-10-02 by the owner: the recommended default — run OpenPanel's ClickHouse queries unchanged on the House; the OpenPanel fork narrows to configuration once FL2 passes OpenPanel's query set (D-SC-15 amended) | Founder | Resolved |
| Q335 | ~~Lakekeeper's catalog database in production (its Postgres, or a backend on Loams's metastore; Q2)~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — Lakekeeper keeps its own Postgres in production, on CloudNativePG (D413); this also answers Q2; why: buy over build, and the same CNPG pattern as Authentik (Q447) | Eng | Resolved |
| Q336 | ~~House tenancy: one `house` process per node with per-namespace sessions and quotas (proposed), or a process per namespace for isolation~~ Answered 2026-10-02 by the owner: the recommended default — one `house` process per node with per-namespace sessions and quotas (FL2 Ruling 10) | Eng | Resolved |
| Q337 | ~~chDB binding: Loams's own `loams-chdb-sys` (proposed) or `chdb-rust` 2.0 directly~~ Answered 2026-10-02 by the owner: the recommended default — Loams's own `loams-chdb-sys` over the pinned `chdb.h`, with `chdb-rust` 2.0 as the reference (D342, FL2 Ruling 1) | Eng | Resolved |
| Q338 | ~~Native protocol 9000 timing: FL5 (proposed), or earlier if driver suites show HTTP is not enough for target apps~~ Answered 2026-10-02 by the owner: the recommended default — native protocol 9000 in FL5, earlier only if driver suites show HTTP is not enough (§32 §11) | Founder | Resolved |
| Q339 | ~~Is there demand for Cypher, GQL or Bolt (a `grafeo-server` companion), or does §07's native surface suffice~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — §07's native graph surface only (D44, D350); a `grafeo-server` companion only when a customer asks; why: the GraphRAG workload is native graphs, and Grafeo has one maintainer | Founder | Resolved |
| Q340 | ~~Flow UI: embed Karavan's designer for Camel-backed steps, or Loams's own editor only~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — embed Karavan's designer components (Apache-2.0) for Camel-backed steps, and Loams's own editor for the rest (D340); why: buy over build for the Camel half | Founder | Resolved |
| Q341 | ~~PK-table tiering layout in Fluss 1.0's Iceberg writer (equality deletes or merge-on-read) and whether chDB's Iceberg reader reads it correctly~~ Answered 2026-10-02 by the owner: the plan's default — not an owner decision: FL2 Task 0 checks the layout Fluss 1.0 writes for PK tiering and whether chDB reads it; the differential harness gates it | Eng | Resolved |
| Q342 | ~~`fluss-rs` APIs for lake-snapshot offsets and write offsets (§7.5): present, or contributions~~ Answered 2026-10-02 by the owner: the plan's default — not an owner decision: FL2 Task 0 checks `fluss-rs`; missing APIs are contributed upstream | Eng | Resolved |
| Q343 | ~~Iggy credentials: a user per namespace with PATs per API key, or a user per API key~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — an Iggy user per namespace with a personal access token per API key; why: revocation per key without multiplying users, and Iggy's per-user permissions match namespace scope | Eng | Resolved |
| Q344 | ~~A Confluent Schema Registry REST subset for Kafka clients of the Fabric: in `loams-fabric`, or upstream in Iggy's gateway~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — upstream in Iggy's Kafka gateway, with Loams's contributions, over D335's bucket schemas; not in `loams-fabric`; why: it follows Q331's contribute-not-build answer | Eng | Resolved |
| Q345 | ~~Fluss and Iggy footprint on small and self-hosted clusters (JVM heap, ZooKeeper, Flink), and a single-node profile~~ Answered 2026-10-02 by the owner: the plan's default — not an owner decision: FL1 Task 0 measures the footprint, and the single-node dev profile ships | Eng | Resolved |
| Q346 | ~~Iggy retention bound before tiering (72 h proposed) and the replay story when Fluss is down longer~~ Answered 2026-10-02 by the owner: the recommended default — 72 h, sized to replay Fluss and connectors after an outage (§32 §5.2); for a longer outage operators raise the topic's retention before it expires, with no second replay store (the chosen part, because Iggy's disks are the only copy until tiering) | Founder | Resolved |
| Q347 | ~~After D331, do D72's explicit streams (named consumers, subscribe) stay in M2 as planned, or narrow to trigger-rate and internal use~~ Answered 2026-10-02 by the owner: the recommended default — D72's explicit streams stay in M2 as planned, for Loams objects and trigger-rate events; high-rate event ingestion goes to the Fabric (§32 §5.1) | Founder | Resolved |

## 16. Sources

All read on 2026-10-01 unless stated.

- **Apache Iggy**: `github.com/apache/iggy` `master` (README, `core/server/config.toml`, `core/connectors/{sdk,sinks,sources}`, `gateways/kafka/README.md`), release `server-0.9.0` (published 2026-09-18) and its notes; `iggy.apache.org/blogs/2026/08/24/apache-iggy-top-level-project-tlp-graduation/` (graduated 2026-08-19); crates `iggy` 0.11.0 and `iggy_binary_protocol` 0.11.0 (2026-09-18); `iggy.apache.org/docs/clustering/vsr`; issue #1419 (closed 2026-05-22) and discussion #3312 "Tiered Storage for Iggy" (2026-05-22, comments to 2026-09-08).
- **Apache Fluss**: `github.com/apache/fluss` `main` (`website/docs/maintenance/tiered-storage/{lakehouse-storage,remote-storage}.md`, `website/docs/install-deploy/deploying-streaming-lakehouse.md`, `fluss-flink/*`, `fluss-rust/crates/fluss/Cargo.toml`, `fluss-kafka/`), release 1.0.0 (2026-09-22) notes; `incubator.apache.org/projects/fluss.html` (graduated 2026-07-16); crate `fluss-rs` 1.0.0.
- **chDB**: `github.com/chdb-io/chdb` v4.4.0 (2026-09-11); `github.com/chdb-io/chdb-core` v26.9.0 (2026-09-28, release assets); `github.com/chdb-io/chdb-rust` v2.0.0 (2026-09-20, README).
- **ClickHouse**: `github.com/ClickHouse/ClickHouse` v26.9.8.3-stable (2026-10-01), Apache-2.0; HTTP interface and formats documentation (to verify per row in FL2 Task 0).
- **Grafeo**: `github.com/GrafeoDB/grafeo` v0.5.43 (2026-09-27; README, `Cargo.toml`, commit history), created 2026-01-26; `github.com/GrafeoDB/grafeo-server` v0.5.40 (2026-04-20, README).
- **Camel**: `github.com/apache/camel` tag `camel-4.22.1` (`components/`, `components/camel-iggy/src/main/docs/iggy-component.adoc`, `components/camel-clickhouse`); `github.com/apache/camel-karavan` 4.18.1 (2026-04-03).
- **Kestra**: `github.com/kestra-io/kestra` v2.0.4 (2026-09-29), README; `github.com/orgs/kestra-io` plugin repositories.
- **Debezium**: `github.com/debezium/debezium` tag v3.7.0.Final; `github.com/debezium/debezium-server` (`debezium-server-http`, `debezium-server-fluss`, …).
- **Lakekeeper** v0.13.6 (2026-09-22); **Flink Kubernetes Operator** `release-1.16.1`; **iceberg-rust** `iceberg` 0.10.1 (arrow 58); **sqlparser** 0.63.0.
- **Loams**: §00 §7, §02 (§5, §7, §7.4, §8.1), §06 §5, §07, §08, §09, §11, §18 §5, §19, §21, §24, §26 (§10, §16), §27, §28 §7.2, §29 (PR #172), `docs/open-core.md`; `crates/loams-cloudevents` (`CloudEvent::dedup_key`), `crates/loams-worker`, `crates/loams-link`; decisions D1, D4–D6, D11, D20–D22, D42, D44, D45, D51, D66, D72–D76, D88, D111, D127, D128, D184, D186, D189, D190, D202, D204, D211, D212, D220, D260, D270, D-SC-13, D-SC-15.
- **The draft**: `chatdump.md` lines 325–628 (the owner's "Loams: Final Plan", "Précis (CDMP, Java stack)", "Top 200 connectors", "Rollout").
