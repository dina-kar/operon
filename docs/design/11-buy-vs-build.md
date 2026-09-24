# 11 — Buy vs Build

Status: **Approved** · research as of 2026-09-22 (Fluss and Resonate added 2026-09-24)

Rule: **buy (embed/fork) everything that is not the differentiator; build the serving layer that closed competitors keep closed.** Only Apache-2.0 / MIT / compatible permissive dependencies. No AGPL, BSL, SSPL or ELv2 code in the engine.

---

## 1. Embed (library dependencies)

| Component | Crate / project | License | Version (2026-09) | Role in Operon | Notes / risks |
|---|---|---|---|---|---|
| Query engine | **Apache DataFusion** | Apache-2.0 | 55.x | All planning/execution | Very active ASF project; extensibility proven by InfluxDB 3, GreptimeDB, LanceDB |
| Distributed exec | **datafusion-distributed** | Apache-2.0 | 4.0 | Multi-node stages over Flight | Young (datafusion-contrib); Ballista rejected (batch-oriented) |
| Collection format | **Lance** (`lance` crate) | Apache-2.0 | 12.0 (format 2.1) | Docs, vectors, IVF + scalar indexes | Vendor-led (LanceDB Inc.), fast API churn → pin + trait boundary; small-commit cost → batch via own WAL |
| Full-text | **Tantivy** | MIT | 0.26.1 | Inverted index, fast fields, aggregations | Healthy, 16k stars |
| KV / PK index / ID map | **SlateDB** | Apache-2.0 (Commonhaus) | 0.16 | PkIndex, vertex-ID maps | Pre-1.0 API; single writer per DB (fits our fencing model); used by HelixDB, Dropbox |
| Consensus | **openraft** | MIT/Apache-2.0 | 0.10.0-alpha.34 (pinned exactly) | Meta Raft, `quorum` journals | API unstable pre-1.0 (alphas break APIs); used in production by Databend |
| Local Raft log | **redb** | MIT/Apache-2.0 | 4.3 | Meta Raft log, vote, snapshot pointer | Pure Rust, ACID, fsync per commit; passes openraft's storage test suite |
| Binary encoding | **postcard** | MIT/Apache-2.0 | 1.1 | Raft log entries, meta snapshot bodies | Stable wire format since 1.0; snapshots add a versioned, checksummed envelope |
| Iceberg | **iceberg-rust** (+ RisingWave fork) | Apache-2.0 | 0.10.1 | Iceberg reads/appends; row deltas via fork | DV writer + RowDelta missing upstream → build and upstream |
| Iceberg compaction | **nimtable/iceberg-compaction** | Apache-2.0 | active | Compaction engine | RisingWave ecosystem |
| Iceberg catalog | **Lakekeeper** | Apache-2.0 | 0.13.6 | REST catalog for tables + external engines | Postgres backend today (verify pluggable backend); vendor Vakamo sells "Plus" |
| Object store I/O | **object_store** (apache/arrow-rs-object-store) | Apache-2.0 | 0.14 | All S3/GCS/Azure/local I/O | Alternative: OpenDAL — pick one (object_store: DataFusion-native) |
| Cache | **foyer** | Apache-2.0 | 0.22 | RAM+NVMe hybrid cache | Used by RisingWave |
| In-memory cache | **moka** | Apache-2.0/MIT | — | H0 metadata cache | — |
| Kafka wire | **kafka-protocol** | MIT/Apache-2.0 | 0.18 | Kafka codec (generated from Kafka 4.1 schemas) | — |
| gRPC / HTTP | tonic, axum, hyper | MIT | — | Gateways | — |
| Arrow Flight SQL | arrow-flight | Apache-2.0 | — | Native bulk results | — |
| SQL parsing | sqlparser-rs | Apache-2.0 | — | ClickHouse dialect, SQL | — |
| Qdrant API types | Qdrant OpenAPI + protobuf | Apache-2.0 | 1.19 | Qdrant gateway | — |
| Bitmaps | roaring-rs | Apache-2.0/MIT | — | Delete bitmaps, filter bitmaps | — |
| Tokenizers | lindera, jieba-rs, ICU4X | MIT/Apache | — | Analyzers | — |

## 2. Fork (take code, own the fork)

| Source | License | What we take | Why fork (not depend) |
|---|---|---|---|
| **Quickwit** | Apache-2.0 (since 2025) | `storage`, `directories` (split bundle + hotcache), ES DSL → Tantivy query crates, aggregation request/response mapping, `bitpacking` | Internal crates, no stable API; Datadog observability roadmap; we need upserts which Quickwit lacks |
| **Qdrant** `lib/segment` (or `qdrant-edge` 0.8) | Apache-2.0 | HNSW, filterable-HNSW links, quantization, payload-filter planner | Qdrant's storage is local-disk; we need the index code only, as a hot tier |
| **lance-graph** | Apache-2.0 | Cypher parser + planner lowering to DataFusion | Small project (slowing activity); we need extensions (MERGE, Bolt semantics, overlay) |
| **Nisshi** (formerly Tansu) | Apache-2.0 | Kafka broker structure, schema registry pieces, S3/Iceberg integration patterns | Bus factor 1 upstream; only selective borrowing |
| **RisingWave iceberg-rust fork** | Apache-2.0 | RowDelta/RewriteFiles, equality & position deletes | Upstream gaps; plan to converge on upstream |
| **Resonate** (`resonatehq/resonate`, `impl/server/core`) | Apache-2.0 | `resonate-core` (protocol types, `ResonateServer` trait), `resonate-plugin`, `resonate-gateway-http`, `resonate-server-blob` (object-storage backend on `object_store` 0.14), HTTP push/poll transports; the differential and linearizability test harness (§14) | Not on crates.io (git-only, workspace 0.10.1); vendor-led by a seed-stage company with fast protocol evolution → pin a git revision, keep Operon's code behind the `ResonateServer` trait, replace `resonate-auth` with Operon auth |

## 3. Reference designs only (no code)

| System | License | What we learn |
|---|---|---|
| turbopuffer | Closed | WAL-on-S3 + async indexing + tail merge; SPFresh ANN; FTS v2 posting blocks; namespace-affinity caching |
| WarpStream | Proprietary | Leaderless Kafka on S3, zone-aware routing, metadata-sequenced offsets, multi-zonal Express WAL (verify) |
| StreamNative Ursa | Proprietary (open-sourcing promised) | Leaderless log protocol (TLA+), WAL → Iceberg compaction |
| AutoMQ | Apache-2.0 (Java) | S3Stream WAL/cache split, EBS multi-attach failover + fencing, S3-proxied cross-AZ produce, Table Topic |
| ClickHouse / ClickHouse Cloud | Apache-2.0 / proprietary | MergeTree granules, sparse PK index, skip indexes, projections; SharedMergeTree, Shared Catalog, distributed cache |
| Kuzu / LadybugDB | MIT (C++) | Columnar graph storage, CSR, factorized execution, WCOJ |
| DuckPGQ | MIT | CSR built from edge tables inside a relational engine; SQL/PGQ |
| Apache GraphAr | Apache-2.0 | Chunked CSR/offset layout (also import/export) |
| HelixDB v3 | Apache-2.0 | Graph + vector on SlateDB/S3 with foyer + tantivy (closest OSS rival) |
| Neon | Apache-2.0 | Quorum WAL (safekeepers) + S3 pageserver split |
| Milvus 3.0 | Apache-2.0 (Go/C++) | Lake-native "external collections" over Lance/Iceberg/Parquet/Vortex; Woodpecker zero-disk WAL |
| GreptimeDB, RisingWave (Hummock), InfluxDB 3 | Apache-2.0 | Stateless frontends + object-store engines + metasrv patterns |
| Databend | Apache-2.0 + Elastic License 2.0 | Stateless warehouse on S3, meta-service on openraft (whose upstream it maintains). Since 2026 it has been repositioned as an "agent-ready" warehouse (analytics + full-text + vector search + sandboxed Python UDFs), which makes it a competitor for M1/M4 (see §12 risk 11). It is SQL-first, with no ES/Qdrant/Kafka/Neo4j wire compatibility |
| Apache Fluss (incubating) | Apache-2.0 (Java) | Columnar (Arrow) log with projection pushdown, primary-key tables that emit changelogs with before images, union reads of fresh log + lakehouse, tiering to Iceberg/Paimon/Lance. Taken as ideas: `arrow` segment encoding (§02 §5) and changelog streams (§02 §8.1). Not embeddable (JVM, ZooKeeper, tablet-server disks); a competitor for M3/M4 (§12 risk 11). Its Rust client (`fluss-rust` 0.1) is a possible interop target, not a dependency |
| Resonate specification | Apache-2.0 | Lean 4 executable abstract machine, TLA+ model, property catalogue and a trace checker that replays a real server's traffic against the model: a reference for M0.4's simulation and linearizability checks alongside Octopii |
| Octopii | Apache-2.0 | Deterministic simulation of openraft clusters (simulated time and RNG, VFS fault injection, partitioned in-memory network, cluster oracle): the reference for M0.4's simulation harness. Not adopted: it vendors a modified openraft, is not on crates.io, has a single maintainer, and pulls in `protobuf` 2.x (RUSTSEC-2024-0437) |
| DiskANN (Rust) | MIT | SSD-resident ANN for larger-than-RAM hot tier (Phase C evaluation) |
| Vortex | Apache-2.0 (LF AI & Data) | Future local/hot encoding option |
| Apache Iggy | Apache-2.0 | Thread-per-core io_uring design, VSR clustering, DST practices |

## 4. Avoid

| Project | License | Reason |
|---|---|---|
| pgrust | AGPL-3.0 | License; pre-production (and OLTP is out of scope) |
| ParadeDB (pg_search) | AGPL-3.0 | License (its Tantivy fork is MIT — use Tantivy directly) |
| ZeroFS | AGPL-3.0 | License |
| SurrealDB | BSL 1.1 | Blocks hosted offerings until 2030 |
| Memgraph | BSL 1.1 + enterprise | License |
| FalkorDB | SSPL | License |
| Moonlink (Mooncake) | BSL 1.1 | License; abandoned after Databricks acquisition |
| Databend `ee/` directories | ELv2 | License (Apache core is fine to study) |
| Bufstream, WarpStream | Proprietary | Closed |
| Kuzu forks as core (Ladybug, RyuGraph, Bighorn) | MIT | C++, local-disk, single-writer — wrong architecture for stateless S3 compute |
| Qdrant as storage | Apache-2.0 | Local-disk architecture (use its index code only) |
| Quickwit as a service | Apache-2.0 | Append-only; duplicates our metastore/ingest (use its crates) |
| S3 Vectors | AWS service | Top-k ≤ 100, ~1k writes/s per index, no hybrid search — cold tier at best |
| ClickHouse OSS text index | Apache-2.0 | No BM25/positions — filter, not relevance |

## 5. Build (the moat)

These are what turbopuffer, LanceDB Enterprise, AutoMQ commercial and ClickHouse Cloud keep closed — and what Operon ships open:

1. **Log engine** with `standard` / `express` (multi-zonal quorum) / `quorum` (Raft journals) WAL classes, leaderless sequencing, segmenting, `kafka`/`arrow` segment encodings, changelog streams with fenced appends, Kafka semantics (§02).
2. **Stateless serving fleet**: affinity routing, H0–H3 caching, hot-tier lifecycle (§04).
3. **Collection engine**: Lance + Tantivy under one manifest, upserts via PK index + delete bitmaps, tail indexes (§03, §06).
4. **Iceberg hot tier**: T0 file index, hot projections (MergeTree-on-NVMe), real-time tail (§04 §3).
5. **Hybrid planner**: vector + text + filter + graph + fusion in one DataFusion plan, consistency tokens (§05).
6. **Graph engine**: ID maps, chunked CSR/CSC sidecars, edge overlay, traversal operators, Cypher/Bolt surface (§07).
7. **Links**: exactly-once declarative materialization with transforms and `embed()` (§09).
8. **Compatibility gateways**: Kafka, ES subset, Qdrant, Bolt/Cypher, ClickHouse HTTP (§02, §06–§08).
9. **Iceberg DV writer / RowDelta** — contributed upstream to iceberg-rust.
10. **Durable-execution integration**: the Resonate surface on Operon's store, auth and routing; in Phase B the change stream, search index, execution graph and cluster-wide timer shards (§14).

## 6. Key sources

- Lance: github.com/lance-format/lance · lance.org/format/table/transaction · lance.org/format/table/mem_wal · docs.lancedb.com/enterprise
- Tantivy/Quickwit: github.com/quickwit-oss/tantivy · github.com/quickwit-oss/quickwit/releases/tag/v0.9.0 · quickwit.io/blog/quickwit-joins-datadog
- Qdrant: github.com/qdrant/qdrant · crates.io/crates/qdrant-edge
- turbopuffer: turbopuffer.com/docs/architecture · turbopuffer.com/blog/fts-v2-postings
- Iceberg: github.com/apache/iceberg-rust/issues/2269 · github.com/risingwavelabs/iceberg-rust · github.com/lakekeeper/lakekeeper · github.com/nimtable/iceberg-compaction
- Streams: docs.automq.com (WAL storage; licensing & enterprise features) · docs.warpstream.com (architecture) · vldb.org/pvldb/vol18/p5184-guo.pdf (Ursa) · github.com/nisshi-io/nisshi · github.com/kafka-protocol-rs/kafka-protocol-rs · KIP-1150/1163/1164
- Graph: github.com/lance-format/lance-graph · github.com/LadybugDB/ladybug · github.com/apache/incubator-graphar · github.com/HelixDB/helix-db · github.com/cwida/duckpgq-extension · opencypher.org
- Storage/infra: github.com/slatedb/slatedb · github.com/databendlabs/openraft · datafusion.apache.org · github.com/datafusion-contrib/datafusion-distributed · docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html
- ClickHouse: clickhouse.com/blog/clickhouse-cloud-stateless-compute · clickhouse.com/blog/full-text-search-ga-release
- Fluss: github.com/apache/fluss · fluss.apache.org/blog/releases/0.9 · github.com/apache/fluss-rust · jack-vanlightly.com/blog/2025/9/2/understanding-apache-fluss
- Resonate: github.com/resonatehq/resonate (`impl/server/core/crates/resonate-server-blob/README.md`, `impl/server/s3/docs/on-s3.md`, `spec/`) · resonatehq.io/durable-execution
