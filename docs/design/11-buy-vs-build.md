# 11 — Buy vs Build

Status: **Approved** · research as of 2026-09-22 (Fluss and Resonate added 2026-09-24; revised 2026-09-25 after the architecture review: dependencies of the dropped Kafka, Bolt/Cypher and ClickHouse surfaces removed, metastore backends and ADBC added; AI data ecosystem integrations added the same day, §17, D51–D56)

Rule: **buy (embed/fork) everything that is not the differentiator; build the serving layer that closed competitors keep closed.** Only Apache-2.0 / MIT / compatible permissive dependencies. No AGPL, BSL, SSPL or ELv2 code in the engine.

---

## 1. Embed (library dependencies)

| Component | Crate / project | License | Version (2026-09) | Role in Operon | Notes / risks |
|---|---|---|---|---|---|
| Query engine | **Apache DataFusion** | Apache-2.0 | 54.1 (lockstep with Lance 12 / arrow 58) | All planning/execution | Very active ASF project; extensibility proven by InfluxDB 3, GreptimeDB, LanceDB |
| Distributed exec | **datafusion-distributed** | Apache-2.0 | 3.0 (the last release on DataFusion 54; not used in M1) | Multi-node stages over Flight | Young (datafusion-contrib); Ballista rejected (batch-oriented) |
| Collection format | **Lance** (`lance` crate) | Apache-2.0 | 12.0 (file format 2.1 set explicitly; detached versions, M1.1) | Docs, vectors, IVF + scalar indexes | Vendor-led (LanceDB Inc.), fast API churn → pin + trait boundary; small-commit cost → batch via own WAL |
| Full-text | **Tantivy** | MIT | =0.26.2 (feature `quickwit`) | Inverted index, fast fields, aggregations | Healthy, 16k stars |
| KV / PK index / ID map | **SlateDB** | Apache-2.0 (Commonhaus) | 0.16 | PkIndex, vertex-ID maps | Pre-1.0 API; single writer per DB (fits our fencing model); used by HelixDB, Dropbox |
| Consensus | **openraft** | MIT/Apache-2.0 | 0.10.0-alpha.34 (pinned exactly) | Meta Raft (the default `MetaStore` backend), `quorum` journals | API unstable pre-1.0 (alphas break APIs); used in production by Databend |
| Local Raft log | **redb** | MIT/Apache-2.0 | 4.3 | Meta Raft log, vote, snapshot pointer | Pure Rust, ACID, fsync per commit; passes openraft's storage test suite |
| Binary encoding | **postcard** | MIT/Apache-2.0 | 1.1 | Raft log entries, meta snapshot bodies | Stable wire format since 1.0; snapshots add a versioned, checksummed envelope |
| Iceberg | **iceberg-rust** (+ RisingWave fork) | Apache-2.0 | 0.10.1 | Iceberg reads/appends; row deltas via fork | DV writer + RowDelta missing upstream → build and upstream |
| Iceberg compaction | **nimtable/iceberg-compaction** | Apache-2.0 | active | Compaction engine | RisingWave ecosystem |
| Iceberg catalog | **Lakekeeper** | Apache-2.0 | 0.13.6 | REST catalog for tables + external engines (M4) | Postgres backend today (verify pluggable backend; Q2), which can share the M2 Postgres metastore's server; vendor Vakamo sells "Plus"; its catalog-behind-a-trait design is the model for `MetaStore` (D47) |
| Object store I/O | **object_store** (apache/arrow-rs-object-store) | Apache-2.0 | 0.14 | All S3/GCS/Azure/local I/O | Alternative: OpenDAL — pick one (object_store: DataFusion-native) |
| Cache | **foyer** | Apache-2.0 | 0.22 | RAM+NVMe hybrid cache | Used by RisingWave |
| In-memory cache | **moka** | Apache-2.0/MIT | — | H0 metadata cache | — |
| gRPC / HTTP | tonic, axum, hyper | MIT | — | Gateways | — |
| Arrow Flight SQL | arrow-flight (`flight-sql-experimental`) | Apache-2.0 | 58.4 (lockstep with arrow) | Flight SQL server; `DoPut` bulk ingest into collections and streams (M1.2, D49) | The Flight SQL API is marked experimental in arrow-rs → pin with arrow |
| SQL parsing | sqlparser-rs | Apache-2.0 | — | SQL (through DataFusion) | — |
| Qdrant API types | Qdrant OpenAPI + protobuf | Apache-2.0 | 1.19 | Qdrant gateway | — |
| Bitmaps | roaring-rs (`roaring`) | Apache-2.0/MIT | 0.11 | Delete bitmaps, filter bitmaps | — |
| HNSW hot tier | **qdrant-edge** | Apache-2.0 | =0.8.0 (M1.3) | HNSW artifacts behind Operon's `HnswIndex` (R20) | Enables `serde_json/preserve_order`, which vendored Quickwit code must not see; M1.3 Task 0 resolves it (M1.1 ruling P5) |
| MCP server | **rmcp** | Apache-2.0 | 3.4.1 (M1.6) | The W0 MCP server (§15) | — |
| Tokenizers | lindera, jieba-rs, ICU4X | MIT/Apache | — | Analyzers | — |
| Postgres client (M2) | **tokio-postgres** (rust-postgres), with a pool such as deadpool-postgres; alternative **sqlx** | MIT/Apache-2.0 (all three) | tokio-postgres 0.7 (verify at M2) | `operon-meta-postgres` behind `MetaStore` (D47) | Choice made in the M2 plan; tokio-postgres is the smaller dependency, sqlx adds compile-time-checked queries and migrations |
| FoundationDB client (M6) | `foundationdb` crate (foundationdb-rs) over `libfdb_c` | MIT/Apache-2.0 (verify); `libfdb_c` Apache-2.0 | — | `operon-meta-fdb` behind `MetaStore` (D47) | Links a C client library whose version must match the cluster's API version |

### 1.1 Test and client-side dependencies

| Component | Project | License | Role | Notes |
|---|---|---|---|---|
| ADBC Flight SQL drivers | Apache Arrow ADBC: `adbc-driver-flightsql` (Python), the Go `flightsql` driver | Apache-2.0 | Test-only in the engine: the M1 exit gate runs their conformance tests against Operon (M1.7, D49); the Python SDK's optional `operon.flight` module uses the Python driver (M1.6) | Not linked into the `operon` binary |
| Lance Python bindings | **pylance** (`lance` on import) | Apache-2.0 | Python-SDK extra `loamdb[lance]`, pulled in by the Ray, Spark and torch extras: direct reads of the fragments of a pinned scan plan (§17 §3, D53) | Version tracks the engine's Lance release (12.x; verify at M2); whether it opens a detached version by id is Q20 |
| Distributed Lance I/O for Ray | **lance-ray** (lance-format/lance-ray) | Apache-2.0 | Python-SDK extra `loamdb[ray]`: the Ray Data datasource's per-fragment read tasks (§17 §5.3) | Version pinned in the M2 plan (verify) |
| Ray Data | **ray** (`ray[data]`) | Apache-2.0 (Anyscale platform and RayTurbo proprietary) | Python-SDK extra `loamdb[ray]`: datasource and datasink (M2, D54); M2 gate client | 2.58.0 (2026-08-23); PyTorch Foundation project since 2025-10 |
| Polars | **polars** (Python) | MIT | Python-SDK extra `loamdb[polars]`: `to_polars()` (M1.6) and the experimental `scan_loam()` IO plugin (M2) | 1.44.2; 2.0 in release candidates (streaming engine default). `register_io_source` is marked unstable upstream. Python package only, never the Rust crates (§5) |
| PySpark | **pyspark** ≥ 4.0 | Apache-2.0 | Python-SDK extra `loamdb[spark]`: the Python data source `format("loam")` on Apache Spark 4 and Sail (M2, D54); M2 gate client | Sail serves PySpark through Spark Connect (PySpark 4.2 per Sail 0.7.1; verify) |
| PyTorch | **torch** | BSD-3-Clause | Python-SDK extra `loamdb[torch]` (lower bound only; the user's build is used) | Lance's `lance.torch.data` reader is the base of `loamdb.torch` |
| Spice (test client) | spiceai/spiceai runtime | Apache-2.0 | Test-only: its Flight SQL connector runs against Operon in the M1.7 Flight SQL gate (D56) | Run as a separate process; never linked (§5) |

### 1.2 Candidates (evaluated, not adopted)

| Component | Crate | License | Version | Possible role | Notes |
|---|---|---|---|---|---|
| Query federation | **datafusion-federation** (from Spice) | Apache-2.0 | =0.5.5 (the last release on DataFusion 54; 0.5.6+ need 55) | M4: pushing whole sub-plans to remote SQL sources | Evaluate in the M4 plan; like every DataFusion extension it moves in lockstep (risk 21). `datafusion-table-providers` 0.13.1 (Apache-2.0, DataFusion ^54) is the companion crate if connectors are ever needed |

## 2. Fork (take code, own the fork)

| Source | License | What we take | Why fork (not depend) |
|---|---|---|---|
| **Quickwit** | Apache-2.0 (since 2025) | `storage`, `directories` (split bundle + hotcache, async directories, warmup), ES DSL → `QueryAst` → Tantivy (`quickwit-query`), the doc-mapper query builder, aggregation merge glue, `StableLogMergePolicy`, `quickwit-datetime`: vendored file by file from `af0591a3` into `operon-quickwit` (R21) | Internal crates, no stable API; Datadog observability roadmap; we need upserts which Quickwit lacks |
| **Qdrant** | Apache-2.0 | `qdrant-edge =0.8.0` behind `HnswIndex` (R20): HNSW, filterable-HNSW links, quantization, payload-filter planner; `lib/segment` is not vendored | Qdrant's storage is local-disk; we need the index code only, as a hot tier |
| **RisingWave iceberg-rust fork** | Apache-2.0 | RowDelta/RewriteFiles, equality & position deletes | Upstream gaps; plan to converge on upstream |
| **Resonate** (`resonatehq/resonate`, `impl/server/core`) | Apache-2.0 | `resonate-core` (protocol types, `ResonateServer` trait), `resonate-plugin`, `resonate-gateway-http`, `resonate-server-blob` (object-storage backend on `object_store` 0.14), HTTP push/poll transports; the differential and linearizability test harness (§14) | Not on crates.io (git-only, workspace 0.10.1); vendor-led by a seed-stage company with fast protocol evolution → pin a git revision, keep Operon's code behind the `ResonateServer` trait, replace `resonate-auth` with Operon auth |

## 3. Reference designs only (no code)

| System | License | What we learn |
|---|---|---|
| turbopuffer | Closed | WAL-on-S3 + async indexing + tail merge; SPFresh ANN; FTS v2 posting blocks; namespace-affinity caching |
| WarpStream | Proprietary | Leaderless Kafka on S3, zone-aware routing, metadata-sequenced offsets, multi-zonal Express WAL (verify) |
| StreamNative Ursa | Proprietary (open-sourcing promised) | Leaderless log protocol (TLA+), WAL → Iceberg compaction |
| AutoMQ | Apache-2.0 (Java) | S3Stream WAL/cache split, EBS multi-attach failover + fencing, S3-proxied cross-AZ produce, Table Topic |
| ClickHouse / ClickHouse Cloud | Apache-2.0 / proprietary | MergeTree granules, sparse PK index, skip indexes, projections (for the Iceberg hot tier, §04 §3); SharedMergeTree, Shared Catalog, distributed cache |
| StarRocks (shared-data mode) | Apache-2.0 | Stateless compute over Iceberg on S3 with a two-tier (RAM + NVMe) data cache: the pattern §04 applies to Lance, Tantivy and adjacency files as well |
| Nebula Graph | Apache-2.0 | Distributed graph database with three stateful daemons (`metad`, `graphd`, multi-Raft `storaged` over RocksDB): what a separate GraphRAG graph store costs to operate, and why §07 keeps expansion inside the query engine |
| Kuzu / LadybugDB | MIT (C++) | Columnar graph storage, CSR, factorized execution, WCOJ |
| DuckPGQ | MIT | CSR built from edge tables inside a relational engine; SQL/PGQ |
| Apache GraphAr | Apache-2.0 | Chunked CSR/offset layout (also import/export) |
| HelixDB v3 | Apache-2.0 | Graph + vector on SlateDB/S3 with foyer + tantivy (closest OSS rival) |
| Neon | Apache-2.0 | Quorum WAL (safekeepers) + S3 pageserver split |
| Milvus 3.0 | Apache-2.0 (Go/C++) | Lake-native "external collections" over Lance/Iceberg/Parquet/Vortex; Woodpecker zero-disk WAL |
| GreptimeDB, RisingWave (Hummock), InfluxDB 3 | Apache-2.0 | Stateless frontends + object-store engines + metasrv patterns |
| Databend | Apache-2.0 + Elastic License 2.0 | Stateless warehouse on S3, meta-service on openraft (whose upstream it maintains). Since 2026 it has been repositioned as an "agent-ready" warehouse (analytics + full-text + vector search + sandboxed Python UDFs), which makes it a competitor for M1/M4 (see §12 risk 11). It is SQL-first, with no Qdrant/ES compatibility and no graph expansion |
| Apache Fluss (incubating) | Apache-2.0 (Java) | Columnar (Arrow) log with projection pushdown, primary-key tables that emit changelogs with before images, union reads of fresh log + lakehouse, tiering to Iceberg/Paimon/Lance. Taken as ideas: `arrow` segment encoding (§02 §5) and changelog streams (§02 §8.1). Not embeddable (JVM, ZooKeeper, tablet-server disks); a competitor for M4/M5 (§12 risk 11). Its Rust client (`fluss-rust` 0.1) is a possible interop target, not a dependency |
| Resonate specification | Apache-2.0 | Lean 4 executable abstract machine, TLA+ model, property catalogue and a trace checker that replays a real server's traffic against the model: a reference for M0.4's simulation and linearizability checks alongside Octopii |
| Octopii | Apache-2.0 | Deterministic simulation of openraft clusters (simulated time and RNG, VFS fault injection, partitioned in-memory network, cluster oracle): the reference for M0.4's simulation harness. Not adopted: it vendors a modified openraft, is not on crates.io, has a single maintainer, and pulls in `protobuf` 2.x (RUSTSEC-2024-0437) |
| DiskANN (Rust) | MIT | SSD-resident ANN for larger-than-RAM hot tier (Phase C evaluation) |
| Vortex | Apache-2.0 (LF AI & Data) | Future local/hot encoding option |
| Apache Iggy | Apache-2.0 | Thread-per-core io_uring design, VSR clustering, DST practices |
| MosaicML Streaming | Apache-2.0 | Elastic determinism (a seeded shuffle over canonical partitions, `num_canonical_nodes`) and mid-epoch resume: the model for `loamdb.torch`'s sampler (§17 §5.5) |
| Sail (object-store shuffle) | Apache-2.0 | Stateless workers with blocking shuffle to object storage and checkpointing (0.7): a reference for M6's distributed shuffle |
| TileDB | MIT (core) | Timestamped fragments and named retained snapshots (the idea behind dataset tags, D52); a tensor column type (Q19) |

## 4. Companions (run alongside, not embedded)

| System | License | Role next to Operon | Integration surface | Why not embed |
|---|---|---|---|---|
| Lakekeeper | Apache-2.0 (Rust) | Iceberg REST catalog (bundled, M4) | Iceberg REST | Already listed in §1; separate process by design |
| Spice | Apache-2.0 (Spice.ai Enterprise proprietary) | Federation and acceleration for agent apps, with Operon as a Flight SQL source (M1 gate, D56); also a competitor (§12 risk 11) | Flight SQL | Ships forks of DataFusion and arrow-rs (§5) |
| Sail | Apache-2.0 (Rust) | Spark Connect compute for PySpark curation jobs: reads collections through `format("loam")` (M2) and tables through Iceberg REST (M4 gate, D55) | Python data source; Iceberg REST | DataFusion version lockstep (§5) |
| Ray | Apache-2.0 | Distributed curation, embedding backfills and batch inference over collections (M2) | Scan plans + Lance fragments; Flight `DoPut` | Python compute cluster, run by the user |

Stream-processor companions (RisingWave, Arroyo, Flink) connect over the Kafka surface, which is deferred past v1.0 together with the RisingWave integration (D43).

### 4.1 Candidates for agent workspaces (§15, proposed)

| Component | Project | License | Role | Notes |
|---|---|---|---|---|
| Git objects and packs | **gitoxide** (`gix-*`) | Apache-2.0 / MIT | Pack parsing, indexes, protocol, SHA-1/SHA-256 | Server-side upload-pack/receive-pack not implemented upstream → build the server loop |
| Lazy, deduplicated images | **nydus** (Dragonfly) | Apache-2.0 (Rust) | Environment images (RAFS v6 / EROFS), chunk dedup, lazy fetch | Verify its storage backend can use Operon's bucket or cache |
| Chunking / hashing | fastcdc, BLAKE3 | MIT / Apache-2.0 | Content-defined chunks for the namespace CAS | — |
| FUSE | fuser | MIT | Userspace mounts where virtiofs/EROFS are unavailable | — |
| Code parsing | tree-sitter | MIT | Symbol chunks and code graphs | — |
| MCP | Rust MCP SDK (`rmcp`) | Apache-2.0 | MCP server and gateway on the 2026-07-28 stateless spec | 3.4.1 supports 2026-07-28 (M1.6 plan; §1) |
| Session parsing and pricing | **tokscale-core** (tokscale) | MIT (Rust) | Parse Claude Code, Codex, opencode (and ~30 other harnesses') session files into `token_usage` (§16 §6) | Library crate of the tokscale CLI; parity check against the CLI |
| Build cache | sccache, bazel-remote | Apache-2.0 | Point at the bucket; no Operon code | — |
| Sandbox runtimes | microsandbox (libkrun), Firecracker, Cloud Hypervisor, Kata, gVisor, E2B, Anthropic `sandbox-runtime`, Codex | Apache-2.0 | `operon-sandbox` backends: microsandbox (default), Firecracker (fleet), gVisor (Kubernetes without KVM), process (dev only) (§15 §8) | Integrated through a `Runtime` trait, never forked |
| References | git-remote-object-store, awslabs/git-remote-s3, AgentFS, Jujutsu, mountpoint-s3, JuiceFS | Apache-2.0 / MIT | Designs to learn from | — |

## 5. Avoid

| Project | License | Reason |
|---|---|---|
| pgrust | AGPL-3.0 | License; pre-production (and OLTP is out of scope) |
| ParadeDB (pg_search) | AGPL-3.0 | License (its Tantivy fork is MIT — use Tantivy directly) |
| ZeroFS | AGPL-3.0 | License |
| SurrealDB | BSL 1.1 | Blocks hosted offerings until 2030 |
| Memgraph | BSL 1.1 + enterprise | License |
| FalkorDB | SSPL | License |
| Daytona | AGPL-3.0 | License; open-source repo reported unmaintained (2026-06) |
| Moonlink (Mooncake) | BSL 1.1 | License; abandoned after Databricks acquisition |
| Databend `ee/` directories | ELv2 | License (Apache core is fine to study) |
| Bufstream, WarpStream | Proprietary | Closed |
| Kuzu forks as core (Ladybug, RyuGraph, Bighorn) | MIT | C++, local-disk, single-writer — wrong architecture for stateless S3 compute |
| Qdrant as storage | Apache-2.0 | Local-disk architecture (use its index code only) |
| Quickwit as a service | Apache-2.0 | Append-only; duplicates our metastore/ingest (use its crates) |
| S3 Vectors | AWS service | Top-k ≤ 100, ~1k writes/s per index, no hybrid search — cold tier at best |
| ClickHouse OSS text index | Apache-2.0 | No BM25/positions — filter, not relevance |
| Spice runtime as a library | Apache-2.0 | Patches crates.io with forks of DataFusion 54 and arrow-rs (and Ballista, iceberg-rust, Vortex, mistral.rs forks); embedding it ties Operon to Spice's fork cadence (D51). Integrate over Flight SQL instead |
| Sail crates | Apache-2.0 | Main is on DataFusion 55.1 / arrow 59.2 against Operon's 54 / 58 (Lance 12); its maintainers declined Rust-level Lance integration for the same reason; no Spark Connect surface is planned (D42, D51) |
| Polars crates (`polars`, `polars-arrow`) | MIT (`polars-arrow`: MIT AND Apache-2.0) | A second Arrow implementation (arrow2 fork), so every boundary converts; Python users get zero copy through PyCapsule anyway (D51) |
| TileDB core, `tiledb-rs` | MIT; `tiledb-rs` has no license | `tiledb-rs` is unlicensed and early work; the core pins arrow 55 / DataFusion 47 in its Rust parts; activity slowing; its vector search depends on tiledb-cloud (D51) |
| Petastorm | Apache-2.0 | Maintenance mode; deprecated by Databricks in favour of Mosaic Streaming; requires pyspark; Lance's PyTorch reader covers it (D51) |

## 6. Build (the moat)

These are what turbopuffer, LanceDB Enterprise, AutoMQ commercial and ClickHouse Cloud keep closed — and what Operon ships open:

1. **Hybrid planner**: dense + sparse vector + BM25 text + filter + graph expansion + fusion in one DataFusion plan, with tail merge and consistency tokens (§05).
2. **Stateless serving fleet**: affinity routing, H0–H3 caching, hot-tier lifecycle — the StarRocks data-cache pattern extended to Lance pages, Tantivy splits and adjacency (§04).
3. **Collection engine**: Lance + Tantivy under one manifest, upserts via PK index + delete bitmaps, tail indexes (§03, §06).
4. **Graph engine for GraphRAG**: ID maps, chunked CSR/CSC sidecars, edge overlay, `ExpandExec` and shortest path, graph table functions, the `expand` search stage, and the LightRAG and LlamaIndex graph-store adapters (§07).
5. **Compatibility gateways**: Qdrant, the Elasticsearch subset scoped by the framework suites (D48), and Arrow Flight SQL with `DoPut` ingest (§05, §06).
6. **Log engine** with `standard` / `express` (multi-zonal quorum) / `quorum` (Raft journals) WAL classes, leaderless sequencing, segmenting, `kafka`/`arrow` segment encodings, changelog streams with fenced appends, and the native streaming API with idempotent producers and named consumers (§02).
7. **Links**: exactly-once declarative materialization with transforms and `embed()` (§09).
8. **Pluggable metastore**: the semantic `MetaStore` trait with openraft, Postgres and FoundationDB backends, held to one conformance and linearizability suite (§01 §3.2, D47).
9. **Iceberg hot tier**: T0 file index, hot projections (MergeTree-on-NVMe), real-time tail (§04 §3).
10. **Iceberg DV writer / RowDelta** — contributed upstream to iceberg-rust.
11. **Durable-execution integration**: the Resonate surface on Operon's store, auth and routing; in Phase B the change stream, search index, execution graph and cluster-wide timer shards (§14).
12. **AI data ecosystem surface**: scan pinning, retained dataset tags, credential vending for direct fragment reads, and thin Python adapters for Ray Data, Polars, PySpark/Sail and PyTorch, over open formats and protocols only (§17, D51–D54).

## 7. Key sources

- Lance: github.com/lance-format/lance · lance.org/format/table/transaction · lance.org/format/table/mem_wal · docs.lancedb.com/enterprise
- Tantivy/Quickwit: github.com/quickwit-oss/tantivy · github.com/quickwit-oss/quickwit/releases/tag/v0.9.0 · quickwit.io/blog/quickwit-joins-datadog
- Qdrant: github.com/qdrant/qdrant · crates.io/crates/qdrant-edge
- turbopuffer: turbopuffer.com/docs/architecture · turbopuffer.com/blog/fts-v2-postings
- Iceberg: github.com/apache/iceberg-rust/issues/2269 · github.com/risingwavelabs/iceberg-rust · github.com/lakekeeper/lakekeeper · github.com/nimtable/iceberg-compaction
- Streams: docs.automq.com (WAL storage; licensing & enterprise features) · docs.warpstream.com (architecture) · vldb.org/pvldb/vol18/p5184-guo.pdf (Ursa) · KIP-1150/1163/1164
- Graph: github.com/LadybugDB/ladybug · github.com/apache/incubator-graphar · github.com/HelixDB/helix-db · github.com/cwida/duckpgq-extension · github.com/vesoft-inc/nebula
- Storage/infra: github.com/slatedb/slatedb · github.com/databendlabs/openraft · datafusion.apache.org · github.com/datafusion-contrib/datafusion-distributed · docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html
- ClickHouse / StarRocks: clickhouse.com/blog/clickhouse-cloud-stateless-compute · clickhouse.com/blog/full-text-search-ga-release · docs.starrocks.io (data cache, shared-data mode)
- Metastore backends and Flight SQL clients: github.com/sfackler/rust-postgres · github.com/launchbadge/sqlx · github.com/foundationdb-rs/foundationdb-rs · github.com/apache/arrow-adbc
- Fluss: github.com/apache/fluss · fluss.apache.org/blog/releases/0.9 · github.com/apache/fluss-rust · jack-vanlightly.com/blog/2025/9/2/understanding-apache-fluss
- AI data ecosystem (§17 §9 has the full list): github.com/spiceai/spiceai · github.com/lakehq/sail (issue 2573) · docs.ray.io/en/latest/data/api/doc/ray.data.Datasource.html · github.com/lance-format/lance-ray · docs.pola.rs/api/python/stable/reference/api/polars.io.plugins.register_io_source.html · lance.org/integrations/pytorch · docs.mosaicml.com/projects/streaming/en/latest/distributed_training/elastic_determinism.html · docs.databricks.com/aws/en/archive/machine-learning/petastorm · github.com/TileDB-Inc/tiledb-rs
- Resonate: github.com/resonatehq/resonate (`impl/server/core/crates/resonate-server-blob/README.md`, `impl/server/s3/docs/on-s3.md`, `spec/`) · resonatehq.io/durable-execution
