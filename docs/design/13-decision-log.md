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
