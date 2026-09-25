# Implementation Plans

Each plan is a sequence of test-driven tasks that ends in working, tested software. Plans are written just before they're executed, so they match the real code that exists by then. The M0.1 and M0.2 plans contain code compiled and tested in a scratch copy before publication. From M0.3 on, plans specify the exact formats, interfaces, rulings and required tests; the code is written test-first during execution and checked by an independent whole-branch review (M0.3 plan, Ruling 1).

Execute a plan task by task with `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans`.

## Milestone M0: Foundation

Design references: [01 Architecture](../design/01-architecture.md), [02 Stream engine](../design/02-stream-engine.md), [03 Storage formats](../design/03-storage-formats.md), [04 Hot tier](../design/04-hot-tier.md), [09 Links & workers](../design/09-links-and-workers.md), [12 Roadmap](../design/12-roadmap-testing-risks.md).

| Plan | Scope | Status |
|---|---|---|
| [M0.1: Workspace & storage primitives](2026-09-23-m0.1-storage-primitives.md) | Cargo workspace, CI, license policy; `operon-store` (conditional writes, range reads, URL backends, fault injection); `operon-cache` (RAM + NVMe range cache with checksums) | **Done** |
| [M0.2: Metastore](2026-09-23-m0.2-metastore.md) | `operon-common` ids; `operon-meta` on openraft: namespaces, streams, sequencer and offset index, leases with epochs, fenced manifest-pointer CAS; Raft log in redb, snapshots in object storage; single-node and in-process 3-node clusters | **Done** |
| [M0.3: Log engine](2026-09-24-m0.3-log-engine.md) | WAL object and segment formats (with the `encoding` field reserved, D20); leaderless `standard` write path; fetch path; segmenter; native produce/fetch API; `operon dev` binary. In meta: segment index swap, WAL-commit pruning, retention trim, offset watch for long-poll | **Done** |
| [M0.4: Workers, links & M0 gates](2026-09-24-m0.4-workers-links-gates.md) | Worker leases and task framework; link framework with exactly-once apply; `PkIndex` on SlateDB; GC; seeded simulation harness with linearizability checks; M0 crash and fault exit gates | **Done** |

**M0 is done.** The exit gates and their results are in the [M0 exit report](m0-exit-report.md).

## Milestone M1: Collections (Qdrant + Elasticsearch subset + Flight SQL)

Design references: [03 Storage formats](../design/03-storage-formats.md) §3, [04 Hot tier](../design/04-hot-tier.md), [05 Query engine](../design/05-query-engine.md), [06 Search & vector](../design/06-search-and-vector.md), [09 Links & workers](../design/09-links-and-workers.md), [12 Roadmap](../design/12-roadmap-testing-risks.md), [15 Agent workspaces](../design/15-agent-workspaces.md) §10.1.

Start with the **[M1 overview](m1-overview.md)**. It fixes the contracts every M1 plan shares: crates, the collection catalog, the record format, the manifest, consistency tokens, the search IR and `CollectionService`, along with rulings R1–R22 and amendments A1–A32 (A26–A32 record the owner decisions of 2026-09-25: Qdrant sparse vectors in M1, the elasticsearch-py wipe endpoints, and the Loam rename after M1). Where a plan and the overview disagree, the overview wins. The [M1 dependency spike](m1-dependency-spike.md) records the dependency set that was verified to build together, including the fact that Lance 12 pins DataFusion 54 and arrow 58.

Only M1.1 and M1.2a were written against code that existed. Each later plan starts with a Task 0 that reconciles it with the as-built code of the plans it depends on; the rulings M1.1 made during execution (its plan's "Rulings made during execution") are the as-built reference for M1.2's Task 0.

| Plan | Scope | Depends on | Status |
|---|---|---|---|
| [M1.1: Collection storage](2026-09-24-m1.1-collection-storage.md) | Collection catalog, `DocOp` records, atomic multi-partition append, Lance (detached versions) + Tantivy splits under one manifest, upserts/deletes via the PK index, the collection link target, index builds, GC roots, gates | M0 | **Done** |
| [M1.2a: `MetaStore` trait](2026-09-25-m1.2a-metastore-trait.md) | `trait MetaStore` in `operon-common` (D47); shared metastore types moved (encoding unchanged); the openraft `MetaClient` as the first implementation; every downstream crate on `Arc<dyn MetaStore>`; the backend-agnostic conformance suite with linearizability checks; M0 and M1.1 gates re-run unchanged | M1.1 | **Done** |
| [M1.2: Query engine and native API](2026-09-24-m1.2-query-engine.md) | Search IR, DataFusion operators, tail index, strong reads, global BM25 statistics, `CollectionService`, native REST, SQL UDTFs, Flight SQL with `DoPut` bulk ingest (D49), scan pinning (D53), Spice-named SQL search functions (D56) | M1.2a | Planned |
| [M1.3: Hot tier, maintenance and affinity routing](2026-09-24-m1.3-hot-tier-routing.md) | Split merges, Lance compaction, qdrant-edge HNSW artifacts, pinned splits, the metastore over the network, node registry, rendezvous routing, the hot on/off differential harness | M1.2 | Planned |
| [M1.4: Qdrant API Phase A](2026-09-24-m1.4-qdrant-api.md) | REST 6333 + gRPC 6334 over `CollectionService`, sparse vectors included | M1.2 | Planned |
| [M1.5: Elasticsearch API Phase A](2026-09-24-m1.5-elasticsearch-api.md) | REST 9200, trimmed to what the LangChain and LlamaIndex suites and BEIR send (D48): document APIs, `_bulk`, `_search` and `_msearch` with the core DSL, `knn`, hybrid + RRF, `_delete_by_query`, minimal index admin | M1.2 | Planned |
| [M1.6: SDKs and MCP server](2026-09-24-m1.6-sdks-mcp.md) | Python and TypeScript SDKs (with `to_arrow()`/`to_polars()` and scan plans, D54), the W0 MCP server (2026-07-28 stateless spec) | M1.2 | Planned |
| [M1.7: M1 exit gates](2026-09-24-m1.7-exit-gates.md) | LangChain/LlamaIndex suites, ADBC Flight SQL drivers (D49), BEIR vs ES BM25, Recall@10 vs Qdrant, hot on/off identity at scale, the M1 exit report | M1.3–M1.6 | Planned |

M1.4, M1.5 and M1.6 can run in parallel once M1.2 is merged.

## Later milestones

The roadmap was revised on 2026-09-25 after the [architecture review](../architecture-review-and-recommendations.md) (D42–D50). **v1.0 is M1 plus M2, production hardening** (AuthN/Z, tenant quotas, Prometheus/OpenTelemetry, multi-node, the Postgres metastore backend, the Kubernetes operator, rolling upgrades). After v1.0 come M3 (native graph for GraphRAG and the Resonate durable-execution surface), M4 (analytics on Iceberg), M5 (native streams and changelog streams) and M6 (scale). The Kafka, Neo4j and ClickHouse protocol surfaces are no longer planned; the Kafka gateway is on the Phase C list (D43). Plans for M2 onward will be written once M1 is done. Their scope and exit gates are in [12-roadmap-testing-risks.md](../design/12-roadmap-testing-risks.md).
