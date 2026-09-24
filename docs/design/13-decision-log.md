# 13 — Decision Log

Living document. Newest decisions at the bottom of each table.

## Decisions

| ID | Date | Decision | Rationale | Status |
|---|---|---|---|---|
| D1 | 2026-09-22 | Object storage is the only durable source of truth; compute is stateless | turbopuffer/WarpStream/ClickHouse Cloud model; cost; operability | Approved |
| D2 | 2026-09-22 | **OLTP (Postgres) is out of scope** | Millisecond interactive transactions conflict with S3-native design; Neon needed a Paxos tier; users keep a small Postgres and stream CDC in | Approved (user) |
| D3 | 2026-09-22 | Scope = combined **Kafka + Elasticsearch + Qdrant + Neo4j + ClickHouse** | User's target pain point: AI apps deploying all of these | Approved (user) |
| D4 | 2026-09-22 | The log is the spine; every write lands in a stream; other objects are link-maintained materializations | Zero-ETL, consistency tokens, one durability mechanism | Approved (§01) |
| D5 | 2026-09-22 | Five object kinds: stream, table, collection, graph, link | Each replaces one system (links replace connector glue) | Approved (§01) |
| D6 | 2026-09-22 | **Iceberg for tables** (via Lakekeeper), **Lance for collections** | User requirement (Iceberg for analytics); Lance is the only Rust substrate with columns + vectors + versioning | Approved (§01) |
| D7 | 2026-09-22 | **Tantivy for full-text** (Quickwit storage/DSL/aggregation crates forked), not Lance FTS | Maturity, aggregations, ES DSL translation already exists in Quickwit | Approved (§01 amendment) |
| D8 | 2026-09-22 | Vectors: **Lance IVF durable tier + Qdrant-derived HNSW hot tier** | Lance wins storage/cost/versioning; Qdrant wins serving latency/filtered recall/freshness | Approved (user) |
| D9 | 2026-09-22 | **Hot-tier model applied uniformly**, including Iceberg + Lakekeeper (T0 file index, T1 Parquet cache, T2 hot projections, T3 tail) | User request; ClickHouse-like latency and freshness on open Iceberg | Approved (user, §04) |
| D10 | 2026-09-22 | Metastore = embedded Raft (openraft) by default; pluggable FoundationDB/Postgres | Kafka-rate metadata cannot run on S3 CAS | Approved (§01) |
| D11 | 2026-09-22 | Apache-2.0 license; no AGPL/BSL/SSPL/ELv2 dependencies | Big-company adoption | Approved |
| D12 | 2026-09-22 | WAL classes `standard` / `express` (2-of-3 zonal buckets) / `quorum` (Raft journals). Renames the `zonal` class shown in the approved §01 diagram to `express` and makes it multi-AZ durable | AutoMQ-grade reliability in OSS without stateful broker disks; AutoMQ OSS only has S3 WAL | Approved (§02) |
| D13 | 2026-09-22 | Compatibility scope defined by external conformance suites (client libs, framework integrations) | Prevents unbounded compat long tail | Approved |
| D14 | 2026-09-22 | Build order: M0 foundation → M1 collections → M2 graph → M3 Kafka → M4 analytics → M5 scale | Follows the AI-app pain point (ES+Qdrant+Neo4j first) | Approved (§12) |
| D15 | 2026-09-22 | DataFusion as the single query engine; datafusion-distributed (not Ballista) | Extensibility, ecosystem, interactive distributed execution | Approved (§05) |
| D16 | 2026-09-23 | Metastore Raft: openraft pinned to `=0.10.0-alpha.34`; the local Raft log, vote and snapshot pointer in **redb**; log entries and snapshot bodies encoded with **postcard** | openraft 0.10 alphas change APIs between releases; redb is pure Rust and ACID (RocksDB would add a C++ build) | Approved (M0.2 plan) |
| D17 | 2026-09-23 | Meta snapshots live only in object storage, one set per node: `meta/snapshots/<node_id>/<term>-<index>.snap` (amends §01 §6) | Nodes snapshot independently; per-node paths let each node delete its previous snapshot without breaking another node's restart | Approved (M0.2 plan) |
| D18 | 2026-09-23 | Metastore ids are dense `u64` counters allocated by the state machine; stream ids are unique across the cluster | `apply` must be deterministic (no random ULIDs); a cluster-unique stream id makes `(stream, partition)` a complete key | Approved (M0.2 plan) |
| D19 | 2026-09-24 | **Durable execution via the Resonate protocol** (§14): fork Resonate's Rust gateway + blob server (pinned git revision) and run it in the `gateway` role over `operon-store`, state under `ns/<ns>/durable/`, no metastore traffic. Phase A in M2, Phase B (change stream, search tables, execution graph, timer shards) in M4 | Agents need durable runs next to their memory; Resonate is Apache-2.0, formally specified, already S3-native on the same `object_store` crate, and plugin-based, so this is buy, not build | Approved (user) |
| D20 | 2026-09-24 | **`arrow` segment encoding** for schema'd streams (idea from Apache Fluss); the `encoding` field is reserved in the first WAL/segment format (M0.3), `arrow` ships in M4 | Column pruning and no decode for links and tails; reserving the field now avoids a format break | Approved (user) |
| D21 | 2026-09-24 | **Changelog streams** from keyed tables and collections (`upsert` / `full` with before images; idea from Apache Fluss), written by link apply with fenced appends; M3 | CDC out of Operon and incremental consumers that need deletes; the PK index already locates before images | Approved (user) |
| D22 | 2026-09-24 | **RisingWave is the supported companion stream processor**, run alongside Operon (not embedded): it reads topics and changelog streams (`upsert` / `debezium-json` wire formats) and writes Iceberg tables via Lakekeeper or Kafka topics; externally written keyed tables have one writer class | Keeps Operon out of stateful stream processing (§00 §7) while giving users joins/windows/MVs; RisingWave is Apache-2.0 Rust and already speaks Kafka + Iceberg REST | Approved (user) |
| D23 | 2026-09-24 | Operon as the **state plane for agent sandboxes** (§15): Git on the bucket with O(1) forks, copy-on-write environment images (nydus), package caches, registry proxy, MCP server; runtimes are integrated, never built | Sandboxes are stateless compute; their state (code, deps, checkpoints, memory, traces) fits Operon's bucket model, and agents already speak Git | **Proposed** |
| D24 | 2026-09-24 | Agent sessions: **every session is a Resonate durable execution and is stored in Operon** (workspace branch, harness session files, `sessions` stream → tables and a searchable collection); `operon-sandbox` runtimes behind a `Runtime` trait (microsandbox default, Firecracker fleet, gVisor for Kubernetes without KVM, process for dev); MCP surfaces target the **2026-07-28 stateless spec**, with a gateway that retrieves tool definitions via hybrid search + tool graph and exposes `find_tools` / `call_tool` | Crash-proof sessions without repeated model calls; Rust microVMs with fork; the stateless spec makes MCP horizontally scalable and forbids per-connection tool lists | **Proposed** (§15, §16) |

## Open questions

| # | Question | Owner | Needed by |
|---|---|---|---|
| Q1 | Final project name ("Operon" is the working name) | Founder | Before public repo |
| Q2 | Can Lakekeeper's catalog backend be implemented on Operon meta, or must we bundle Postgres? | Eng | M4 design |
| Q3 | `express` class semantics on GCS Rapid and Azure (conditional writes, append, zone redundancy) | Eng | M3 |
| Q4 | Depth of Kafka transactions required by target users | Product | M5 planning |
| Q5 | Exact Neo4j procedures/APOC functions used by Graphiti, LangChain, LlamaIndex, LightRAG | Eng | M2 |
| Q6 | Lance multivector support depth vs. implementing multivector in the hot tier | Eng | M1 Phase B |
| Q7 | Qdrant code boundary: `qdrant-edge` crate vs. forking `lib/segment` | Eng | M1 |
| Q8 | Governance path (company-led → LF AI & Data / CNCF) and commercial model | Founder | Before 1.0 |
| Q9 | Relationship with HelixDB (compete vs. collaborate on shared SlateDB/graph pieces) | Founder | M2 |
| Q10 | Elastic REST YAML spec test license compatibility for conformance use | Eng | M1 |
| Q11 | Resonate SDKs: per-namespace auth headers vs. base paths; S3 Express/GCS Rapid/Azure `If-Match` support for low-latency durable namespaces | Eng | M2 |
| Q12 | Contribute the Operon Resonate server plugin upstream, or keep it in-tree | Founder | M2 |
| Q13 | `arrow` encoding layout (IPC per chunk vs. per-segment file with column index) and whether the WAL writes it directly | Eng | M0.3 (field), M4 (layout) |
| Q14 | Approve §15 agent workspaces, and when: W0 (MCP) with M1, W1 after M2? | Founder | Before M1 plan |
| Q15 | Repos as a sixth object kind (with an implicit stream) or a service like durable execution | Eng | W1 design |
| Q16 | Approve the 100-agent demo (§16) as the agent-track launch demo, and its benchmark | Founder | Before W1 plan |
