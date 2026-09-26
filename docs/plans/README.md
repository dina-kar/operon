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
| [M1.2: Query engine and native API](2026-09-24-m1.2-query-engine.md) | Search IR, DataFusion operators, tail index, strong reads, global BM25 statistics, `CollectionService`, native REST, SQL UDTFs, Flight SQL with `DoPut` bulk ingest (D49), scan pinning (D53), Spice-named SQL search functions (D56) | M1.2a | **Done** |
| [M1.3: Hot tier, maintenance and affinity routing](2026-09-24-m1.3-hot-tier-routing.md) | Split merges, Lance compaction, qdrant-edge HNSW artifacts, pinned splits, the metastore over the network, node registry, rendezvous routing, the hot on/off differential harness | M1.2 | Planned |
| [M1.4: Qdrant API Phase A](2026-09-24-m1.4-qdrant-api.md) | REST 6333 + gRPC 6334 over `CollectionService`, sparse vectors included | M1.2 | Planned |
| [M1.5: Elasticsearch API Phase A](2026-09-24-m1.5-elasticsearch-api.md) | REST 9200, trimmed to what the LangChain and LlamaIndex suites and BEIR send (D48): document APIs, `_bulk`, `_search` and `_msearch` with the core DSL, `knn`, hybrid + RRF, `_delete_by_query`, minimal index admin, multi-target aliases (D57) | M1.2 | Planned |
| [M1.6: SDKs and MCP server](2026-09-24-m1.6-sdks-mcp.md) | Python and TypeScript SDKs (with `to_arrow()`/`to_polars()` and scan plans, D54), the W0 MCP server (2026-07-28 stateless spec) | M1.2 | Planned |
| [M1.7: M1 exit gates](2026-09-24-m1.7-exit-gates.md) | LangChain/LlamaIndex suites, ADBC Flight SQL drivers (D49), Spice's Flight SQL connector (D56), BEIR vs ES BM25, Recall@10 vs Qdrant, hot on/off identity at scale, the M1 exit report | M1.3–M1.6 | Planned |

M1.4, M1.5 and M1.6 can run in parallel once M1.2 is merged.

## Later milestones

The roadmap was revised on 2026-09-25 after the [architecture review](../architecture-review-and-recommendations.md) (D42–D50), and on 2026-09-26 for the metastore backends, the namespace router, tenancy and erasure ([§18](../design/18-metastore-backends-and-router.md), D58–D70), then for streams, Kafka, routing and consistency tokens (D71–D76). **v1.0 is M1 plus M2, production hardening**, including the native stream API core and OTLP logs ingest (D72, D73). **v1.1 is M2.x, cloud and BYOC.** After them come M3 (native graph for GraphRAG and the Resonate durable-execution surface), M4 (analytics on Iceberg), M5 (the Kafka wire-protocol gateway with the RisingWave companion, changelog streams, `express` and Flight replay; D74) and M6 (scale). The Neo4j and ClickHouse protocol surfaces are no longer planned, and FoundationDB is dropped (D71). Plans for M2 onward will be written once M1 is done. Their scope and exit gates are in [12-roadmap-testing-risks.md](../design/12-roadmap-testing-risks.md).

Future plans, in their expected order (the split into plans is fixed when each milestone is planned):

| Milestone | Plan area | Design references |
|---|---|---|
| M2 | **The `MetaStore` contract amendment, first:** per-group `commit_wal`, bounded-skew stamps with GC claims, documented read orders (D59); the namespace on bare-id calls (D70); paginated lists and the scoped change feed (D63); conformance cases, `Backend::capabilities()`, the linearizability checker's relaxed models, and the fault-plan hook; the consistency-token conformance cases (D76) | §18 §3, §18 §10 |
| M2 | Catalog-scale fixes: an incremental catalog cache, dirty-set maintenance, snapshot builds off the apply path, snapshots past 5 GiB, bounded-load placement (D63); placement keys for stream partitions and consumer coordination (D75) | §18 §5.3, §18 §5.7 |
| M2 | `operon-meta-postgres` on Lakekeeper's patterns, with its fault matrix (D58, D60) | §18 §2.2, §18 §4 |
| M2 | `operon-meta-dynamodb`, with the floci and Alternator CI jobs, its fault matrix and the nightly AWS deployment job (D58, D60, D62) | §18 §2.3, §18 §4 |
| M2 | RustFS as the default self-hosted store; the `Store` provider suite and the S3 fault matrix over RustFS (D61) | §18 §4.4 |
| M2 | Tenancy: orgs and the `ControlStore`, API keys, the `Authorizer` trait with RBAC, quotas (D65, D66) | §18 §6–§7, §10 §3–§4 |
| M2 | The GDPR erasure path (D68, D69) | §18 §9, §10 §4.1 |
| M2 | The native stream API core: gRPC, idempotent producers, streaming subscribe, named consumers, stream admin, the plain-JSON produce body (D72); OTLP logs ingest (D73) | §02 §7, §02 §7.1, §02 §7.3 |
| M2 | Observability, multi-node membership, the Kubernetes operator, rolling upgrades, restore from bucket | §10 |
| M2 | The AI data ecosystem workstream: dataset tags, credential vending, the Python adapters (D52, D54) | §17 |
| M2.x | `operon-meta-remote`, the hosted `operon-control` and `ControlStore`, owned-namespace caches, both BYOC modes (D63, D64) | §18 §5.7, §18 §8 |
| M2.x | OpenFGA authorization (D67, default) and per-chunk envelope encryption (D69, default) | §18 §7, §18 §9 |
| M5 | The Kafka wire-protocol gateway, staged: produce and fetch, idempotent producers, consumer groups (D74); the RisingWave companion (D22); changelog streams, `express`, Flight replay | §02 §7.2, §02 §8.1 |
| M6 | `ShardedMetaStore`, namespace moves, size-class placement (D63); `operon-meta-tidb` (D58) | §18 §2.4, §18 §5 |
