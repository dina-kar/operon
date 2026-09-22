# Operon — Design Documents

**Operon** is an open-source (Apache-2.0), object-storage-native, multi-model data engine for AI applications. One system, one bucket, one catalog, one log — replacing the **Kafka + Elasticsearch + Qdrant + Neo4j + ClickHouse** stack that AI apps deploy today.

- **Status:** v0.1 — 2026-09-23
- **Approved:** all documents (§00–§13), 2026-09-23

## Reading order

| # | Document | What it covers | Status |
|---|---|---|---|
| 00 | [Pitch](00-pitch.md) | Problem, why now, positioning, competition, governance/business model | **Approved** |
| 01 | [Architecture](01-architecture.md) | Data model (streams, tables, collections, graphs, links), roles, consistency model, metastore, S3 layout | **Approved** |
| 02 | [Stream engine](02-stream-engine.md) | Kafka replacement, WAL durability classes, write/read/failover protocols, consumer groups, transactions | **Approved** |
| 03 | [Storage formats](03-storage-formats.md) | Durable tier: Iceberg tables, Lance collections, Tantivy splits, adjacency sidecars, manifests, PK index | **Approved** |
| 04 | [Hot tier & caching](04-hot-tier.md) | Unified hot-tier model for every object type, incl. Iceberg + Lakekeeper hot tier | **Approved** |
| 05 | [Query engine](05-query-engine.md) | DataFusion embedding, custom operators, hybrid retrieval, consistency tokens, distributed execution | **Approved** |
| 06 | [Search & vector](06-search-and-vector.md) | Elasticsearch + Qdrant pillars, Lance + hot HNSW tiers, API compatibility scope | **Approved** |
| 07 | [Graph](07-graph.md) | Neo4j pillar: property graphs over tables/collections, CSR, Cypher subset, Bolt | **Approved** |
| 08 | [Analytics](08-analytics.md) | ClickHouse pillar: Iceberg tables, MergeTree-family semantics, materialized views, HTTP interface | **Approved** |
| 09 | [Links & workers](09-links-and-workers.md) | Declarative materialization (zero-ETL), exactly-once apply, background task scheduling | **Approved** |
| 10 | [Operations](10-operations.md) | Deployment, multi-tenancy, security, observability, DR, GC, cost model | **Approved** |
| 11 | [Buy vs build](11-buy-vs-build.md) | Every dependency with license, version, verdict; avoid list; what we build (the moat) | **Approved** |
| 12 | [Roadmap, testing, risks](12-roadmap-testing-risks.md) | Milestones and exit gates, testing strategy, risk register | **Approved** |
| 13 | [Decision log](13-decision-log.md) | Decisions made so far and open questions | Living |

## Glossary

| Term | Meaning |
|---|---|
| **Namespace** | Tenant/database boundary. Unit of isolation, quota, encryption key, and routing. |
| **Stream** | Partitioned, ordered, offset-addressed log. Kafka topic equivalent. Every write in Operon lands in a stream (explicit or implicit). |
| **Partition** | Ordered unit of a stream; offsets are dense per partition. |
| **Table** | Columnar analytical dataset, stored as an **Iceberg** table. ClickHouse table equivalent. |
| **Collection** | Document set with vectors, full-text, and filters; stored as **Lance** dataset + **Tantivy** splits. Elasticsearch index / Qdrant collection equivalent. |
| **Graph** | Property graph whose vertex/edge types map onto tables or collections, plus adjacency indexes. Neo4j database equivalent. |
| **Link** | Declared, continuously maintained materialization between objects (stream→table, stream→collection, table→collection, tables→graph). |
| **Applied offset** | Per link: the highest source offset reflected in the target's durable state. |
| **Consistency token** | Set of `(stream, partition, offset)` a reader requires to be visible. Returned by every write. |
| **Durable tier** | Open formats on object storage. The only source of truth. |
| **Hot tier** | Derived, node-local, rebuildable acceleration structures (caches, HNSW, projections, in-RAM CSR). Never the source of truth. |
| **Tail** | Data committed to the log but not yet in the durable indexed form; merged into every read. |
| **WAL class** | Durability/latency class for a stream: `standard`, `express`, `quorum` (see §02). |
| **Journal** | A 3-replica Raft group (one node per AZ) that hosts the `quorum` WAL for a set of partitions. |
| **Manifest** | Immutable description of an object's durable state at a version; the pointer to the current manifest lives in the metastore. |
| **Split** | Immutable Tantivy index bundle with a hotcache footer (Quickwit design). |

## Conventions

- **(verify)** marks a claim that came from a single secondary source or could not be confirmed during research (2026-09-22). Resolve before relying on it.
- Latency/cost numbers are design targets or published figures from reference systems, not measured Operon results.
- Dependency versions are as of 2026-09-22; see §11.
