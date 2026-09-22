# 05 — Query Engine

Status: **Approved** · 2026-09-22

All reads — SQL, ClickHouse queries, ES `_search`, Qdrant `query`, Cypher, native hybrid requests — compile to **Apache DataFusion** logical plans and execute on `query` nodes. DataFusion is embedded as a library; Operon adds catalogs, table providers, physical operators, optimizer rules and a distributed layer.

---

## 1. Catalog integration

| DataFusion concept | Operon mapping |
|---|---|
| `CatalogProvider` | Namespace |
| `SchemaProvider` | Object kind: `tables`, `collections`, `streams`, `graphs` (plus user schemas for tables) |
| `TableProvider` | `IcebergTable` (hot-tier aware), `CollectionProvider`, `StreamProvider` (offset/timestamp-bounded scans), `GraphVertexProvider`/`GraphEdgeProvider` |
| Table functions (UDTF) | `vector_search`, `text_search`, `hybrid_search`, `graph_expand`, `graph_table` (SQL/PGQ-style), graph algorithms |

## 2. Custom physical operators

| Operator | Purpose | Inputs |
|---|---|---|
| `IcebergScanExec` | Pruned scan using T0 file index; chooses hot projection / T1 / cold per file group; merges T3 tail; applies DVs | Table snapshot + tail |
| `ProjectionScanExec` | Reads aggregate/sorted hot projections; sparse PK index + skip indexes | Hot projection |
| `TantivySearchExec` | BM25 top-k / boolean match / aggregation over splits (+ tail index); returns `(_pk, row_addr, score)` or agg results | Split set @ manifest |
| `AnnExec` | Vector top-k; hot HNSW if present, else Lance IVF with nprobes/refine; merges tail brute-force | Collection @ manifest |
| `FilterBitmapExec` | Builds roaring bitmaps from Tantivy/Lance scalar indexes for pre-filtering | Filter predicate |
| `FusionExec` | Combines ranked lists: RRF, weighted score, DBSF (Qdrant-compatible) | ≥2 ranked inputs |
| `DocFetchExec` | Fetches documents/columns by row address from Lance (coalesced range reads) | Row addresses |
| `ExpandExec` / `VarExpandExec` | 1-hop / k-hop traversal over CSR/CSC + edge-delta overlay, with edge-type/property filters | Frontier vertex ids |
| `ShortestPathExec` | Bidirectional BFS | Two vertex sets |
| `StreamScanExec` | Scans stream segments by offset/time range, decodes RecordBatch → Arrow | Stream partitions |
| `TailMergeExec` | Unions durable results with tail results honoring upsert/delete semantics | Durable + tail |

All operators emit Arrow `RecordBatch` streams and report metrics (rows, bytes, cache hit ratios per layer, S3 GETs).

## 3. Optimizer rules

- **Index pushdown:** predicates on indexed columns become `FilterBitmapExec` inputs; `ORDER BY distance(v, q) LIMIT k` → `AnnExec`; `WHERE match(text, 'q') ORDER BY score LIMIT k` → `TantivySearchExec`.
- **Pre- vs post-filter selection** for ANN: cost-based on estimated filter selectivity (bitmap cardinality); highly selective → pre-filter (bitmap-restricted search / brute force on small sets); broad → post-filter with over-fetch.
- **Projection matching** for aggregates (ClickHouse semantics) → `ProjectionScanExec`.
- **Late materialization:** retrieve `(_pk, row_addr, score)` first, fetch documents only for final top-k.
- **Dynamic filters** (DataFusion 55) propagate join/top-k bounds into scans, including across distributed stage boundaries.

## 4. Hybrid retrieval (native API)

One request, one plan:

```json
POST /v1/ns/acme/query
{
  "from": "collections.memories",
  "consistency": {"token": "c1:s7/p3@918273"},
  "retrieve": [
    {"vector": {"field": "embedding", "query": [0.12, …], "k": 100}},
    {"text":   {"field": "body", "query": "refund policy for enterprise", "k": 100}}
  ],
  "filter": {"and": [{"term": {"tenant": "t42"}}, {"range": {"ts": {"gte": "now-30d"}}}]},
  "fuse": {"method": "rrf", "k": 60},
  "expand": {"graph": "kg", "from_field": "entity_id", "hops": 2, "edge_types": ["MENTIONS", "RELATED_TO"], "limit": 50},
  "rerank": {"model": "endpoint:reranker-v2", "top_n": 20},
  "select": ["id", "body", "entity_id", "_score", "_neighbors"],
  "limit": 10
}
```

Plan: `FilterBitmapExec` → (`AnnExec` ‖ `TantivySearchExec`) → `FusionExec(RRF)` → `ExpandExec(2 hops)` → `DocFetchExec` → optional `RerankExec` (UDF calling an external model endpoint; pluggable, off by default) → `Limit`.

The same plan is reachable from SQL:

```sql
SELECT m.id, m.body, n.name
FROM hybrid_search('memories',
       vector => ('embedding', $q_vec, 100),
       text   => ('body', 'refund policy for enterprise', 100),
       fuse   => 'rrf') AS m
JOIN graph_expand('kg', m.entity_id, hops => 2, edge_types => ['MENTIONS']) AS n ON true
WHERE m.tenant = 't42' AND m.ts >= now() - INTERVAL '30 days'
LIMIT 10;
```

## 5. Consistency and snapshots

- At plan start, the coordinator resolves a **read snapshot** per referenced object: `(manifest_version | iceberg_snapshot_id, applied_offsets)`.
- If a consistency token is present, the required offsets define the tail range each object must merge: `(applied_offset, token_offset]`. Because the tail is read from the log, strong reads never wait for indexing.
- `consistency: "eventual"` skips tail merge (lowest latency; bounded staleness = link lag).
- All operators within one query use the same snapshot ⇒ repeatable results inside a query.

## 6. Distributed execution

- **Single-node** execution is the default for point/top-k queries routed to the owning node.
- **Distributed** execution via **datafusion-distributed** (Arrow Flight between stages) for large scans, joins and aggregations: the coordinator splits file groups/split groups/vertex ranges by ownership (hot-tier affinity) and streams partial results.
- Ballista is not used (batch/shuffle-to-disk oriented).
- Top-k across shards: two-phase (local top-k′ → global merge) with k′ = k × safety factor for ANN.

## 7. Resource management

- Per-query memory pools (DataFusion `MemoryPool`) with per-namespace limits; spill to NVMe.
- Admission control and priority classes: `interactive` (search/vector/graph), `analytical` (large scans), `background` (worker-internal). Interactive preempts analytical on shared nodes; large deployments separate pools.
- Timeouts and cancellation propagate across distributed stages.

## 8. SQL surfaces

| Surface | Parser/dialect | Notes |
|---|---|---|
| Operon SQL (native, Flight SQL, ADBC) | DataFusion SQL + Operon UDTFs | Primary SQL surface |
| ClickHouse HTTP | `sqlparser-rs` `ClickHouseDialect` + function-compat UDF library | §08 |
| Cypher | Forked `lance-graph` parser → logical plan | §07 |
| ES Query DSL | Quickwit-derived DSL → logical plan | §06 |
| Qdrant query API | Direct mapping → logical plan | §06 |
| Postgres wire (optional, later) | `pgwire` + DataFusion | For BI tools only; not OLTP |

**Arrow Flight SQL** is exposed from M1 for high-throughput result transfer (Python/pandas/Polars, BI via ADBC).
