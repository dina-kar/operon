# 07 — Graph (Neo4j Pillar)

Status: **Approved** · 2026-09-22

Graphs in Operon are **property graphs defined over tables and collections**, accelerated by dense vertex IDs and CSR/CSC adjacency sidecars. The graph is not a separate copy of the data: the same rows that are searchable and analyzable are traversable.

---

## 1. What AI apps actually need

From GraphRAG, Graphiti/Zep, Cognee, LightRAG and LangChain/LlamaIndex graph integrations, the dominant pattern is:

1. **Seed** entities via vector and/or BM25 search.
2. **Expand** 1–3 hops, filtered by edge type, time validity and properties.
3. **Upsert** entities/edges with `MERGE`-style entity resolution.
4. Occasionally **shortest path** between entities.
5. **Offline** PageRank / community detection (Leiden) for summaries.

Full Neo4j compatibility is not needed. What is needed is **Bolt + a Cypher subset** covering what these integrations send, plus shims for the Neo4j vector/full-text procedures they call.

## 2. Model and DDL

### 2.1 Mapped graphs (over existing data)
```sql
CREATE GRAPH kg
  VERTEX person   KEY (id)          FROM TABLE people
  VERTEX document KEY (doc_id)      FROM COLLECTION docs
  EDGE   KNOWS    SOURCE person(src) DESTINATION person(dst)     FROM TABLE knows
  EDGE   MENTIONS SOURCE document(doc_id) DESTINATION person(person_id) FROM COLLECTION mentions;
```
Aligned with SQL/PGQ `CREATE PROPERTY GRAPH` semantics (ISO/IEC 9075-16) where practical.

### 2.2 Native graphs (Cypher-first)
`CREATE GRAPH memory NATIVE` — Operon manages one collection per vertex label and per edge type (created on first use by Cypher `CREATE`/`MERGE`). Native graph data is still searchable/vector-indexed like any collection (e.g., `Entity.embedding`).

## 3. Storage and indexing

- **Vertex-ID map** per label (SlateDB): external key ↔ dense `u64` (§03 §4.1).
- **Adjacency sidecars** per edge-source segment: forward CSR + reverse CSC, chunked by vertex-ID range, delta-encoded and bitpacked, each edge pointing at its source row for properties (§03 §4.2).
- **Edge-delta overlay (tail):** edges added/removed since the last sidecar build, held in RAM on owning query nodes as small sorted adjacency maps; merged by `ExpandExec`.
- **Hot tier:** CSR/CSC chunks for hot vertex ranges resident in RAM; hot ID-map ranges cached (§04).

## 4. Execution

DataFusion physical operators (§05):

| Operator | Semantics |
|---|---|
| `ExpandExec` | For each input vertex, emit neighbors via CSR/CSC chunks ∪ overlay, filtered by edge type and pushed-down edge/vertex predicates |
| `VarExpandExec` | Bounded k-hop BFS with uniqueness modes (Cypher relationship-isomorphism semantics), depth bounds, path materialization on demand |
| `ShortestPathExec` | Bidirectional BFS (unweighted); Dijkstra for weighted (Phase B) |
| `PatternJoinExec` (Phase C) | Worst-case-optimal intersection for cyclic patterns (triangles, cliques), factorized intermediates — per Kuzu research |

Batch algorithms as table functions, executed on workers against a snapshot: `pagerank`, `wcc`, `louvain`, `leiden`, `label_propagation`, `k_core`, `triangle_count`, `betweenness` (sampled). Results can be written back as table/collection columns (e.g., `community_id`).

## 5. Cypher subset

Parser/planner forked from **lance-graph** (Apache-2.0, Rust, lowers Cypher to DataFusion), extended. Alternative parsers if needed: `decypher` (rowan-based), `open-cypher` (pest), GraphLite (GQL).

| Clause / feature | Phase A (M2) | Phase B | Out |
|---|---|---|---|
| `MATCH`, `OPTIONAL MATCH`, `WHERE`, `RETURN`, `WITH`, `ORDER BY`, `SKIP`, `LIMIT`, `DISTINCT` | ✓ | | |
| Patterns: fixed-length, variable-length `*1..3`, direction, multiple labels/types | ✓ | | |
| `CREATE`, `MERGE` (+ `ON CREATE/ON MATCH SET`), `SET`, `REMOVE`, `DELETE`, `DETACH DELETE` | ✓ | | |
| `UNWIND`, parameters, list/map literals, `CASE`, aggregation functions, `collect` | ✓ | | |
| `shortestPath`, `allShortestPaths` | shortestPath | allShortestPaths | |
| Path functions (`nodes`, `relationships`, `length`) | ✓ | | |
| Subqueries `CALL { … }`, `EXISTS { … }`, `COUNT { … }` | | ✓ | |
| `LOAD CSV`, `FOREACH` | | ✓ | |
| Schema: `CREATE INDEX/CONSTRAINT` (mapped to Operon indexes/PK) | ✓ (unique, range, vector, fulltext) | | |
| Quantified path patterns (GQL-style) | | ✓ | |
| APOC | | small shim subset required by target integrations (verify list) | general APOC |
| GDS library procedures | | `gds.pageRank`/`gds.louvain`-style shims onto table functions | full GDS |

**Neo4j procedure shims** (Phase A, verify exact names against Graphiti/LangChain sources): `db.index.vector.queryNodes`, `db.index.vector.queryRelationships`, `db.index.fulltext.queryNodes`, `db.index.fulltext.queryRelationships`, `db.labels`, `db.relationshipTypes`, `db.propertyKeys`, `db.schema.visualization`, `dbms.components` — mapped onto collection vector/text indexes and graph metadata.

Conformance: openCypher TCK (Apache-2.0) subset tracked as a pass-rate metric; Graphiti, LangChain `Neo4jGraph`, LlamaIndex `Neo4jPropertyGraphStore` and LightRAG test suites run unmodified.

## 6. Bolt server

- Bolt 5.x over TCP/WebSocket, PackStream encoding, from the published protocol spec; verified with the official Neo4j Python/JS/Java/Go drivers (Apache-2.0).
- Routing table responses advertise Operon gateways (single "cluster" view).
- Auth via Bolt basic/bearer → Operon RBAC.

## 7. Write and transaction semantics

| Case | Behavior |
|---|---|
| Single Cypher statement (auto-commit) | All its writes are appended as **one atomic batch** to the graph's implicit stream(s); visible to subsequent strong reads immediately via overlay/tail |
| `MERGE` uniqueness | Enforced by ID-map SlateDB transaction (SSI) keyed by label + key; conflicting concurrent merges resolve to one vertex |
| Explicit Bolt transaction (`BEGIN … COMMIT`) | Writes buffered in the gateway; reads inside the tx see a snapshot + the tx's own writes (overlay); `COMMIT` appends one atomic batch after validating `MERGE`/unique constraints; conflicts → retryable error |
| Isolation | Snapshot isolation per statement/transaction; no long-held locks |
| Limits | Transaction size bounded (default 64 MiB / 100k ops); not designed for OLTP-style high-contention updates on the same vertices |

## 8. GraphRAG patterns (first-class examples in docs/SDK)

- **Seed-and-expand:** hybrid search on entity/document collection → `graph_expand` 2 hops → fetch → rerank (single native query, §05 §4).
- **Community summaries:** `leiden` table function → write `community_id` → LLM summaries stored in a collection → retrievable by vector/text.
- **Temporal edges:** `valid_from`/`valid_to` edge properties with pushed-down time filters in `ExpandExec` (Graphiti-style bi-temporal memory).

## 9. Benchmarks and gates
- LDBC SNB Interactive short reads (IS1–IS7) and selected complex reads (IC1, IC2, IC9 bounded) on SF1/SF10.
- k-hop latency (1/2/3 hops) with and without hot tier.
- Graphiti/LightRAG end-to-end test suites.
