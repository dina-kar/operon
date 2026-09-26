# Operon — Design Documents

**Operon** is an open-source (Apache-2.0) **unified hybrid retrieval engine on object storage** (D50): stateless compute over open formats in one bucket, with a RAM + NVMe hot tier, serving vector, full-text and GraphRAG retrieval as one planned query. It speaks its native REST/gRPC API, Arrow Flight SQL, the Qdrant API and a targeted Elasticsearch subset (D42); keeps analytics in Iceberg tables that any engine can read; reaches streams through a native streaming API; and runs durable agent workflows through the Resonate protocol. It replaces the Elasticsearch + Qdrant + Neo4j (+ Kafka + ClickHouse) stack that AI apps deploy today by role, not by wire protocol.

- **Status:** v0.2 — 2026-09-25 (revised after the 2026-09-25 [architecture review](../architecture-review-and-recommendations.md): decisions D42–D50)
- **Approved:** all documents (§00–§13), 2026-09-23; §14–§16 and the Fluss-derived stream features (`arrow` encoding, changelog streams), 2026-09-24; the architecture review's revisions (narrowed protocol footprint, native graph, Iceberg-only analytics, native streams, pluggable metastore, new build order), 2026-09-25; §17 AI data ecosystem (D51–D56), 2026-09-25

## Reading order

| # | Document | What it covers | Status |
|---|---|---|---|
| 00 | [Pitch](00-pitch.md) | Problem, what Operon is, why now, positioning (unified hybrid retrieval on object storage), competition, non-goals, governance/business model, launch demo | **Approved** |
| 01 | [Architecture](01-architecture.md) | Data model (streams, tables, collections, graphs, links), roles and protocol surfaces, consistency model, the `MetaStore` trait and its backends, S3 layout | **Approved** |
| 02 | [Stream engine](02-stream-engine.md) | The internal log and native streaming: WAL durability classes, write/read/failover protocols, the native streaming API and Flight ingest, changelog streams | **Approved** |
| 03 | [Storage formats](03-storage-formats.md) | Durable tier: Iceberg tables, Lance collections, Tantivy splits, adjacency sidecars, manifests, PK index | **Approved** |
| 04 | [Hot tier & caching](04-hot-tier.md) | Unified hot-tier model for every object type, incl. Iceberg + Lakekeeper hot tier | **Approved** |
| 05 | [Query engine](05-query-engine.md) | DataFusion embedding, custom operators, hybrid retrieval, consistency tokens, distributed execution, Flight SQL | **Approved** |
| 06 | [Search & vector](06-search-and-vector.md) | Qdrant and Elasticsearch-subset surfaces, Lance + hot HNSW tiers, API compatibility scope | **Approved** |
| 07 | [Graph](07-graph.md) | Native graph for GraphRAG: mapped graphs over collections and tables, CSR/CSC sidecars, `ExpandExec` and shortest path, graph table functions, the `expand` search stage, LightRAG and LlamaIndex adapters | **Approved** |
| 08 | [Analytics](08-analytics.md) | Iceberg analytics: tables via Lakekeeper, keyed tables, materialized views, SQL over Flight SQL and the native API, external engine access | **Approved** |
| 09 | [Links & workers](09-links-and-workers.md) | Declarative materialization (zero-ETL), exactly-once apply, background task scheduling | **Approved** |
| 10 | [Operations](10-operations.md) | Deployment, metastore backends, multi-tenancy, security, observability, DR, upgrades, cost model | **Approved** |
| 11 | [Buy vs build](11-buy-vs-build.md) | Every dependency with license, version, verdict; avoid list; what we build (the moat) | **Approved** |
| 12 | [Roadmap, testing, risks](12-roadmap-testing-risks.md) | Milestones M0–M6 and exit gates (v1.0 = M2), testing strategy, risk register | **Approved** |
| 13 | [Decision log](13-decision-log.md) | Decisions made so far and open questions | Living |
| 14 | [Durable execution](14-durable-execution.md) | Resonate protocol surface: durable promises, tasks and schedules on the bucket; phases, consistency, cost | **Approved** (direction) |
| 15 | [Agent workspaces](15-agent-workspaces.md) | Operon as the state plane for coding-agent sandboxes: Git on the bucket, copy-on-write environments, registry proxy, caches, sandbox runtimes, sessions as durable executions, MCP gateway with tool retrieval | **Approved** |
| 16 | [Agent fleet demo](16-agent-fleet-demo.md) | 100 Claude Code / Codex / opencode sessions on one host and one bucket: density, durability, tool-retrieval savings, analytics with tokscale parity | **Approved** |
| 17 | [AI data ecosystem](17-ai-data-ecosystem.md) | Operon as a source and sink for AI labs' data pipelines: integration map (Ray Data, Polars, PySpark on Spark 4 and Sail, PyTorch/JAX, Spice, Iceberg engines), scan pinning, retained dataset tags, Python SDK extras, what is not built | **Approved** |

## Glossary

| Term | Meaning |
|---|---|
| **Namespace** | Tenant/database boundary. Unit of isolation, quota, encryption key, and routing. |
| **Stream** | Partitioned, ordered, offset-addressed log. Every write in Operon lands in a stream (explicit or implicit); explicit streams are read and written through the native streaming API and Flight. |
| **Partition** | Ordered unit of a stream; offsets are dense per partition. |
| **Table** | Columnar analytical dataset, stored as an **Iceberg** table catalogued in Lakekeeper; queried through Flight SQL and the native API, and readable by any Iceberg engine. |
| **Collection** | Document set with vectors, full-text, and filters; stored as **Lance** dataset + **Tantivy** splits. Elasticsearch index / Qdrant collection equivalent. |
| **Graph** | Mapped graph whose vertex/edge types map onto tables or collections, plus CSR/CSC adjacency sidecars; traversed by 1–2 hop expansion and shortest path inside a query. |
| **Link** | Declared, continuously maintained materialization between objects (stream→table, stream→collection, table→collection, tables→graph). |
| **Applied offset** | Per link: the highest source offset reflected in the target's durable state. |
| **Consistency token** | Set of `(stream, partition, offset)` a reader requires to be visible. Returned by every write. |
| **Durable tier** | Open formats on object storage. The only source of truth. |
| **Hot tier** | Derived, node-local, rebuildable acceleration structures (caches, HNSW, projections, in-RAM CSR). Never the source of truth. |
| **Tail** | Data committed to the log but not yet in the durable indexed form; merged into every read. |
| **WAL class** | Durability/latency class for a stream: `standard`, `express`, `quorum` (see §02). |
| **Journal** | A 3-replica Raft group (one node per AZ) that hosts the `quorum` WAL for a set of partitions. |
| **Manifest** | Immutable description of an object's durable state at a version; the pointer to the current manifest lives in the metastore. |
| **MetaStore** | The semantic metastore trait in `operon-common` (D47): domain operations (WAL commit, catalog, manifest-pointer CAS, leases and fencing), implemented by embedded openraft (default), Postgres (M2) and FoundationDB (M6). |
| **Scan plan** | A collection resolved (at the current manifest, a manifest version, a consistency token or a dataset tag) into what an external reader needs: the manifest version, the Lance dataset URI and detached version id, the fragments with row counts, per-fragment Flight tickets, and the tail if one exists (D53, §17 §3). |
| **Dataset tag** | A named, immutable pointer to one collection manifest (from M4, one Iceberg snapshot) that GC retains until the tag is deleted; tagged reads never need the tail (D52, §17 §4). |
| **Split** | Immutable Tantivy index bundle with a hotcache footer (Quickwit design). |
| **Segment encoding** | How a stream's records are stored in WAL chunks and segments: `kafka` (the Kafka RecordBatch v2 layout, used internally) or `arrow` (columnar, for schema'd streams). |
| **Changelog stream** | A stream of row-level changes (`+I`, `-U`, `+U`, `-D`) of a keyed table or collection. |
| **Durable promise** | Resonate's unit of durable execution: a promise, keyed by a deterministic id, that survives process restarts and records a step's result. |
| **Origin** | The part of a Resonate id before the first `:`; all of one origin's promises and tasks are one document, committed atomically. |

## Conventions

- **(verify)** marks a claim that came from a single secondary source or could not be confirmed during research (2026-09-22). Resolve before relying on it.
- Latency/cost numbers are design targets or published figures from reference systems, not measured Operon results.
- Dependency versions are as of 2026-09-22; see §11.
