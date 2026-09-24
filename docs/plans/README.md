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

## Milestone M1: Collections (Elasticsearch + Qdrant)

Design references: [03 Storage formats](../design/03-storage-formats.md) §3, [04 Hot tier](../design/04-hot-tier.md), [05 Query engine](../design/05-query-engine.md), [06 Search & vector](../design/06-search-and-vector.md), [09 Links & workers](../design/09-links-and-workers.md), [12 Roadmap](../design/12-roadmap-testing-risks.md), [15 Agent workspaces](../design/15-agent-workspaces.md) §10.1.

Start with the **[M1 overview](m1-overview.md)**. It fixes the contracts every M1 plan shares: crates, the collection catalog, the record format, the manifest, consistency tokens, the search IR and `CollectionService`, along with rulings R1–R21 and amendments A1–A18. Where a plan and the overview disagree, the overview wins. The [M1 dependency spike](m1-dependency-spike.md) records the dependency set that was verified to build together, including the fact that Lance 12 pins DataFusion 54 and arrow 58.

Only M1.1 is written against code that exists. Each later plan starts with a Task 0 that reconciles it with the as-built code of the plans it depends on.

| Plan | Scope | Depends on | Status |
|---|---|---|---|
| [M1.1: Collection storage](2026-09-24-m1.1-collection-storage.md) | Collection catalog, `DocOp` records, atomic multi-partition append, Lance (detached versions) + Tantivy splits under one manifest, upserts/deletes via the PK index, the collection link target, index builds, GC roots, gates | M0 | Planned |
| [M1.2: Query engine and native API](2026-09-24-m1.2-query-engine.md) | Search IR, DataFusion operators, tail index, strong reads, global BM25 statistics, `CollectionService`, native REST, SQL UDTFs, Flight SQL | M1.1 | Planned |
| [M1.3: Hot tier, maintenance and affinity routing](2026-09-24-m1.3-hot-tier-routing.md) | Split merges, Lance compaction, qdrant-edge HNSW artifacts, pinned splits, the metastore over the network, node registry, rendezvous routing, the hot on/off differential harness | M1.2 | Planned |
| [M1.4: Qdrant API Phase A](2026-09-24-m1.4-qdrant-api.md) | REST 6333 + gRPC 6334 over `CollectionService` | M1.2 | Planned |
| [M1.5: Elasticsearch API Phase A](2026-09-24-m1.5-elasticsearch-api.md) | REST 9200: document APIs, `_search` DSL, `knn`, aggregations, PIT, index admin | M1.2 | Planned |
| [M1.6: SDKs and MCP server](2026-09-24-m1.6-sdks-mcp.md) | Python and TypeScript SDKs, the W0 MCP server (2026-07-28 stateless spec) | M1.2 | Planned |
| [M1.7: M1 exit gates](2026-09-24-m1.7-exit-gates.md) | LangChain/LlamaIndex and client suites, BEIR vs ES BM25, Recall@10 vs Qdrant, hot on/off identity at scale, the M1 exit report | M1.3–M1.6 | Planned |

M1.4, M1.5 and M1.6 can run in parallel once M1.2 is merged.

## Later milestones

Plans for M2 (graph and the Resonate durable-execution surface), M3 (Kafka and changelog streams), M4 (analytics) and M5 (scale) will be written once M1 is done. Their scope and exit gates are in [12-roadmap-testing-risks.md](../design/12-roadmap-testing-risks.md).
