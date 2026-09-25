# 07 — Graph (native GraphRAG)

Status: **Approved** · 2026-09-22 · revised 2026-09-25 (native only, D44; M3)

Graphs in Operon are **property graphs mapped over collections and tables**, accelerated by dense vertex IDs and CSR/CSC adjacency sidecars. The graph is not a separate copy of the data: the same rows that are searchable and analyzable are traversable. Traversal is a DataFusion operator, so a GraphRAG retrieval (vector/BM25 seeds → 1–2 hops → rerank) is one planned query. Graphs are reached through SQL table functions, the `expand` stage of the native hybrid search API, and Operon-native graph-store adapters for the AI frameworks; there is no Cypher, Bolt or Neo4j procedure surface (D44).

---

## 1. What AI apps actually need

From GraphRAG, LightRAG, Cognee, Graphiti/Zep and the LangChain/LlamaIndex graph integrations, the dominant pattern is (architecture review §4.2):

1. **Seed** entities or chunks via vector and/or BM25 search.
2. **Expand** 1–2 hops, filtered by edge type, time validity and properties.
3. **Rerank** the seeds and their neighbors, and fetch the top results.
4. **Upsert** entities and relations keyed by a resolved entity id (latest wins per key).
5. Occasionally **shortest path** between two entities.
6. **Offline** PageRank / community detection (Leiden) for community summaries.

Arbitrary-depth traversal, cyclic pattern matching and a graph query language are not needed for this workload. The frameworks reach a graph store through a small storage interface (LightRAG `BaseGraphStorage`, LlamaIndex `PropertyGraphStore`), so Operon implements that interface natively (§6).

## 2. Model and DDL

### 2.1 Mapped graphs (over existing data)

A graph declares **vertex labels** over keyed sources and **edge types** over sources with `(src_key, dst_key)` columns. It owns no rows; writes go to its sources (§7).

```sql
CREATE GRAPH kg
  VERTEX entity   KEY (_pk)                                  FROM COLLECTION entities
  VERTEX chunk    KEY (_pk)                                  FROM COLLECTION chunks
  EDGE   RELATED  SOURCE entity(src) DESTINATION entity(dst) TYPE FROM (rel_type) FROM COLLECTION relations
  EDGE   MENTIONS SOURCE chunk(chunk_id) DESTINATION entity(entity_id)          FROM COLLECTION mentions;
```

- `CREATE GRAPH` is an Operon DDL extension to DataFusion SQL, aligned with SQL/PGQ `CREATE PROPERTY GRAPH` (ISO/IEC 9075-16) where practical. The native API has the same resource: `POST|GET /v1/namespaces/{ns}/graphs`, `GET|DELETE /v1/namespaces/{ns}/graphs/{g}`.
- A vertex key is the source's primary key or a keyword, integer or UUID field; an edge endpoint column holds a key of the endpoint label.
- `TYPE FROM (column)` maps one edge source that holds many relation types (the framework adapters' `relations` collection, §6); otherwise the declared name is the type of every row.
- Edge properties (weight, description, `valid_from`/`valid_to`, source chunk ids) are the edge source's fields; vertex properties are the vertex source's fields, embeddings included.
- Sources are collections in M3. Iceberg tables become vertex and edge sources when tables ship (M4).
- A graph's labels and edge types are also plain tables (`GraphVertexProvider`/`GraphEdgeProvider`, §05 §1), so anything the table functions do not cover is an ordinary SQL join.

## 3. Storage and indexing

- **Vertex-ID map** per label (SlateDB, §03 §4.1): canonical source key ↔ dense `u64`. IDs are assigned in batches by the graph-link worker (fenced by its lease epoch) when a key first appears as a vertex or as an edge endpoint, one ID per key (SlateDB transactions). Vertex uniqueness is the source's primary key; the map only keeps the key ↔ ID assignment stable.
- **Adjacency sidecars** per edge-source segment (a Lance fragment; an Iceberg data file from M4) and edge type: forward CSR + reverse CSC, chunked by vertex-ID range, delta-encoded and bitpacked, each edge carrying its source row address for properties (§03 §4.2). Sidecars live on S3 under the graph's prefix; the `collection → graph` link builds them after each source commit, and source compaction triggers a rebuild (§09).
- **Graph manifest:** the source versions covered, the sidecar set per source segment, the ID-map watermark and the applied offsets. It is CAS-committed in meta like a collection manifest and retained under the same time-travel policy (§03 §7).
- **Edge-delta overlay (tail):** edges added or removed in the edge sources since the last sidecar build (the sources' tails, and committed segments not yet covered by sidecars), held in RAM on owning query nodes as small sorted adjacency maps and merged by `ExpandExec`. Keys not yet in the ID map get node-local provisional IDs that never leave the operator (results carry source keys). Deleted or superseded edge rows are masked with the edge source's deletion bitmaps.
- **Hot adjacency cache:** CSR/CSC chunks and ID-map ranges go through the foyer cache (H0 RAM → H1 NVMe, §04). A pinned graph (`ALTER GRAPH kg SET HOT`, `PUT /v1/namespaces/{ns}/graphs/{g}/hot`) keeps the chunks of its vertex ranges resident in RAM on the owning nodes. Vertex-ID ranges are routed to query nodes by the same rendezvous affinity as collection split groups (§04 §5). Results are identical with the hot tier on and off.

## 4. Execution

DataFusion physical operators (§05 §2), vectorized over Arrow batches of vertex IDs:

| Operator | Semantics |
|---|---|
| `ExpandExec` | For each input vertex, emit neighbors via CSR/CSC chunks ∪ overlay, in direction `out`, `in` or `both`, filtered by edge type and pushed-down edge/vertex predicates; 1 or 2 hops, deduplicated per seed at the smallest hop; the edge and the previous vertex are emitted on demand |
| `ShortestPathExec` | Bidirectional BFS (unweighted) with a depth bound (default 6); Dijkstra for weighted edges (Phase B) |
| `PatternJoinExec` (Phase C) | Worst-case-optimal intersection for cyclic patterns, factorized intermediates (Kuzu research; §12 Phase C) |

- **Deterministic truncation:** `limit_per_seed` and `limit` keep neighbors ordered by hop ascending, then edge weight descending when the edge type declares a weight field, then canonical vertex-key bytes ascending. The order never depends on sidecar layout, the overlay or the hot tier, so truncated results are reproducible (the hot on/off gate, §9).
- **Late materialization:** `ExpandExec` emits `(seed, vertex_id, hop, edge_row_addr)`; vertex and edge properties are fetched only for surviving rows (`DocFetchExec`). Edge predicates that need properties are evaluated on the fetched edge rows before the second hop.
- **Supernodes:** a hub's CSR run is read chunk by chunk and bounded by `limit_per_seed` and the query's memory pool (§05 §7); expansion never materializes a hub's neighbor list beyond the limit.
- **Snapshot:** all operators of one query read the graph manifest and the source tails at the query's snapshot (§05 §5). A consistency token covering an edge source's stream makes its acknowledged edges visible through the overlay.

**Batch algorithms** run as table functions (§5.3) against a snapshot: `pagerank`, `wcc` and `leiden` in M3; `louvain`, `label_propagation`, `k_core`, `triangle_count` and sampled `betweenness` in Phase B. Small graphs run on the query node; large ones run as worker jobs over the CSR/CSC chunks. For Leiden, evaluate `graspologic-native` (Microsoft's Rust Leiden, the implementation behind GraphRAG's hierarchical Leiden; license and packaging to verify) before writing one.

## 5. Query surfaces

### 5.1 SQL table functions

Reached through DataFusion SQL over Flight SQL, ADBC and `POST /v1/namespaces/{ns}/sql`:

```sql
graph_expand(
  graph          => 'kg',
  seeds          => <key | ARRAY[keys] | lateral column>,
  seed_label     => 'entity',                     -- optional when one label fits the seed type
  hops           => 1,                            -- 1 or 2
  direction      => 'out',                        -- 'out' | 'in' | 'both'
  edge_types     => ARRAY['RELATED', 'MENTIONS'], -- default: all
  edge_filter    => 'valid_to IS NULL',           -- SQL predicate over edge-source columns
  vertex_filter  => 'entity_type = ''PERSON''',   -- SQL predicate over vertex-source columns
  limit_per_seed => 50
) → (seed, label, key, hop, edge_type, edge, via)
  -- seed: the seed key; (label, key): the reached vertex; edge: the edge-source PK of the last hop;
  -- via: the vertex it was reached from (the seed for hop 1)

graph_neighbors(graph => 'kg', keys => <key | ARRAY[keys] | lateral column>, label => 'entity',
                direction => 'both', edge_types => …, edge_filter => …, limit_per_key => …)
  → (key, direction, edge_type, edge, other_label, other_key)     -- one row per incident edge

graph_degree(graph => 'kg', keys => …, label => 'entity', direction => 'both', edge_types => …)
  → (key, degree)                                                  -- from CSR/CSC offsets ∪ overlay, no edge reads

graph_shortest_path(graph => 'kg', from => …, to => …, label => 'entity', direction => 'both',
                    edge_types => …, max_hops => 6)
  → (step, label, key, edge_type, edge)                            -- one row per vertex on the path; empty if none
```

- `graph_expand` returns distinct reached vertices; `graph_neighbors` returns incident edges (the adapters' `get_node_edges`, §6). Properties come from joining the vertex or edge source (`JOIN collections.entities e ON e._pk = n.key`), which the planner turns into `DocFetchExec`.
- The lateral form (`… JOIN LATERAL graph_expand('kg', m.entity_id, hops => 2) AS n ON true`, §05 §4) is rewritten by an Operon planner rule into `ExpandExec` over the left input (verify DataFusion's `LATERAL` support for table functions; fallback: a `seeds => 'SELECT …'` query argument).

### 5.2 The `expand` stage in the native hybrid search API

One request, one plan: retrievers → fusion → **expand** → rerank → limit (§05 §4). The search IR (M1 overview §6.6) gains two optional fields in M3; the Qdrant and Elasticsearch gateways never set them.

```rust
pub struct SearchRequest {
    /* … M1 fields (overview §6.6) unchanged … */
    pub expand: Option<Expand>,              // applied to the top `seeds` fused hits
    pub rerank: Option<Rerank>,              // scores the final candidate set (seeds and neighbors)
}
pub struct Expand {
    pub graph: String,
    pub from_field: Option<String>,          // hit field holding the seed vertex key; None: the hit's PK
    pub seed_label: Option<String>,          // required when several labels map onto the collection
    pub seeds: usize,                        // default 10
    pub hops: u8,                            // 1 | 2
    pub direction: Direction,                // Out | In | Both
    pub edge_types: Vec<String>,             // empty: all
    pub edge_filter: Option<Query>,          // over edge-source fields (§6.6 Query)
    pub vertex_filter: Option<Query>,        // over vertex-source fields
    pub limit_per_seed: usize, pub limit: usize,
    pub select: Projection,                  // vertex fields fetched for neighbors
    pub output: ExpandOutput,                // Nested: neighbors under each seed hit | Flat: seeds and neighbors in one ranked list
}
pub enum Rerank {
    Inherit { decay: f32 },                                  // neighbor score = max over its seeds of seed score × decay^hop
    Vector { field: String, query: Vec<f32> },               // exact similarity of each candidate's vector to `query`
    Model { endpoint: String, text_field: String, query: String, top_n: usize },  // external reranker UDF; off by default (§05 §4)
}
// Hit gains `neighbors: Vec<Neighbor>` (Nested) or `graph: Option<GraphHit { label, hop, seeds }>` (Flat).
pub struct Neighbor { pub label: String, pub key: PrimaryKey, pub hop: u8, pub via: PrimaryKey, pub edge_type: String,
                      pub edge: PrimaryKey, pub score: f32, pub source: Option<serde_json::Map<String, serde_json::Value>> }
```

JSON follows the §05 §4 example: `"expand": {"graph": "kg", "from_field": "entity_id", "hops": 2, "edge_types": ["MENTIONS", "RELATED"], "limit": 50}`, `"rerank": {"vector": {"field": "embedding", "query": [...]}}`. Plan: `FilterBitmapExec` → (`AnnExec` ‖ `SparseExec` ‖ `TantivySearchExec`) → `FusionExec` → `Limit(seeds)` → `ExpandExec` → `DocFetchExec` → rerank → `Limit`. Equal scores are ordered by canonical key bytes, as on every path (overview R10). The Python and TypeScript SDKs and the MCP server's `search` tool expose the same fields.

### 5.3 Graph algorithm table functions

```sql
pagerank(graph => 'kg', label => 'entity', edge_types => …, damping => 0.85, max_iterations => 20, tolerance => 1e-6)
  → (label, key, rank)
wcc(graph => 'kg', edge_types => …)
  → (label, key, component)
leiden(graph => 'kg', edge_types => …, weight => 'weight', resolution => 1.0, max_levels => 4, max_cluster_size => …, seed => 42)
  → (label, key, level, community, parent_community)               -- hierarchical, as GraphRAG consumes it
```

Results are deterministic for a fixed `seed` and snapshot. They are written back as vertex fields (e.g. `community_id`, `pagerank`) by a job that patches the vertex source: `POST /v1/namespaces/{ns}/graphs/{g}/jobs` with `{"algorithm": "leiden", "params": {…}, "write": {"field": "community_id"}}`, a worker task on a snapshot (§09 §5).

## 6. Framework adapters (LightRAG, LlamaIndex)

Operon-native graph-store adapters ship in the Python SDK and are the M3 gate (D44). They talk to the native API and Flight SQL. Each adapter creates, on first use, a mapped graph over two collections in its namespace: `entities` (vertex: PK = entity id; type, description, source chunk ids, embedding) and `relations` (edge: PK = `src ⟂ type ⟂ dst`, with the endpoint pair canonicalized for undirected stores; `TYPE FROM (rel_type)`; description, weight, source chunk ids).

| Adapter | Framework interface | Mapping |
|---|---|---|
| `operon.graph_stores.lightrag.OperonGraphStorage` | LightRAG `BaseGraphStorage` (verify the method set at the pinned version): `has_node`/`has_edge`, `node_degree`/`edge_degree`, `get_node`/`get_edge`, `get_node_edges`, the `*_batch` variants, `upsert_node`/`upsert_edge`, `delete_node`, `remove_nodes`/`remove_edges`, `get_all_labels`, `get_knowledge_graph(label, max_depth, max_nodes)`, `drop` | Upserts are PK writes; reads are `get` by PK, `graph_neighbors` and `graph_degree`; `get_knowledge_graph` is `graph_expand` with a node budget; LightRAG's graph is undirected, so every call uses `direction => 'both'` |
| `operon.graph_stores.llama_index.OperonPropertyGraphStore` | LlamaIndex `PropertyGraphStore`: `upsert_nodes`, `upsert_relations`, `get`, `get_triplets`, `get_rel_map(depth)`, `delete`, `vector_query`, `get_schema` | `get_rel_map` is `graph_expand` (depth ≤ 2); `vector_query` is a native hybrid search on `entities` (`supports_vector_queries = True`); `structured_query` accepts Operon SQL (verify that the framework's retrievers tolerate a non-Cypher dialect; otherwise `supports_structured_queries = False`) |

- Installed as extras (`operon[lightrag]`, `operon[llama-index]`); contributed upstream once stable (§12 risk 19).
- Deleting a vertex also deletes its incident relations (a delete-by-filter on `relations` for `src = key OR dst = key`), matching the frameworks' reference stores.
- Chunk and entity vector stores stay on Operon collections through the frameworks' Qdrant or Elasticsearch backends or the native SDK, so one namespace holds a deployment's graph, vectors and chunks.

## 7. Write semantics

Mapped graphs are written only through their sources: native API document ops, Flight `DoPut`, and the Qdrant and Elasticsearch gateways all apply.

| Case | Behavior |
|---|---|
| Vertex or edge upsert | A PK write to the source collection (latest wins per key, in partition order); an upsert of an existing entity id is the entity-resolution merge |
| One request with many ops | One atomic batch per source collection (collection `atomic` writes, M1 overview A24); vertices and edges in different collections are separate requests, and the adapters write vertices first |
| Visibility | The write's consistency token covers the source's stream; `ExpandExec` merges that tail through the overlay, so a strong expand sees the edge immediately, before the sidecar build |
| Dangling edges | An edge whose endpoint has no live vertex row is skipped by expand and counted in the graph's stats (verify against the adapter suites; LightRAG may upsert an edge before its nodes) |
| Deletes | Deleting an edge row masks it through the source's deletion bitmap; deleting a vertex row leaves its edges dangling until they are deleted |
| Isolation | Snapshot per query; no multi-statement transactions and no locks; concurrent upserts of one key resolve by partition order |

## 8. GraphRAG patterns (first-class examples in docs and SDK)

- **Seed-and-expand:** hybrid search on the entity or chunk collection → `expand` 1–2 hops → rerank, as one native query (§5.2), or `hybrid_search` joined with `graph_expand` in SQL (§05 §4). LightRAG's *local* mode (entity seeds → neighbors) and *global* mode (relation seeds → endpoints) are each one request.
- **Community summaries:** `leiden` → write `community_id` → LLM summaries stored in a collection → retrievable by vector/text (Microsoft GraphRAG's community reports).
- **Temporal edges:** `valid_from`/`valid_to` edge fields with pushed-down `edge_filter` time predicates (Graphiti-style bi-temporal memory).
- **Tool graphs:** MCP tools as vertices, `REQUIRES`/`CO_USED` as edges; tool retrieval is seed-and-expand over the catalog (§15 §10.2).
- **Agent execution graphs (§14 Phase B, M4):** durable promises as vertices and awaits as edges, so an agent run's call tree is a `graph_expand` from its root promise, next to the memory graph it read and wrote.

## 9. Benchmarks and gates (M3, §12)

- **Framework gate:** the LightRAG and LlamaIndex property-graph adapters (§6) pass those frameworks' storage-backend test suites (verify which suites at the pinned versions).
- **Differential:** `graph_expand`, `graph_neighbors`, `graph_degree` and `graph_shortest_path` results identical to a naive-adjacency reference (networkx) on fixture graphs, including overlay edges, deletes and truncation, with the hot tier on and off.
- **GraphRAG retrieval benchmark** (seed → 2 hops → rerank) tracked nightly: p50/p95 latency cold and hot, S3 GETs per query, and answer recall against the naive reference pipeline (dataset chosen in the M3 plan; license to verify).
- 1- and 2-hop expand latency with and without the hot tier, tracked.
