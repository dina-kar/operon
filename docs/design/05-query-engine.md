# 05 — Query Engine

Status: **Approved** · 2026-09-22 · revised 2026-09-25 (frontends narrowed, D42/D44) · amended 2026-09-26 (M1.2 as built)

All reads — native hybrid requests (including the graph `expand` stage), SQL over the native API and Flight SQL, ES `_search`, Qdrant `query` — compile to **Apache DataFusion** logical plans and execute on `query` nodes. DataFusion is embedded as a library; Operon adds catalogs, table providers, physical operators, optimizer rules and a distributed layer.

---

## 1. Catalog integration

| DataFusion concept | Operon mapping |
|---|---|
| `CatalogProvider` | Namespace |
| `SchemaProvider` | Object kind: `tables`, `collections`, `streams`, `graphs` (plus user schemas for tables) |
| `TableProvider` | `IcebergTable` (hot-tier aware), `CollectionProvider`, `StreamProvider` (offset/timestamp-bounded scans), `GraphVertexProvider`/`GraphEdgeProvider` |
| Table functions (UDTF) | `vector_search`, `text_search`, `hybrid_search`, `rrf` (reciprocal-rank fusion; Spice's names, D56; `rerank` reserved for M3), `graph_expand`, `graph_neighbors`, graph algorithms (`leiden`, `pagerank`, `wcc`) |

## 2. Custom physical operators

| Operator | Purpose | Inputs |
|---|---|---|
| `IcebergScanExec` | Pruned scan using T0 file index; chooses hot projection / T1 / cold per file group; merges T3 tail; applies DVs | Table snapshot + tail |
| `ProjectionScanExec` | Reads aggregate/sorted hot projections; sparse PK index + skip indexes | Hot projection |
| `TantivySearchExec` | BM25 top-k / boolean match over the manifest's splits plus the tail index, with global live-only BM25 statistics (§06 §3); returns `(_rowid, _pk, _score, _sort)` | Split set @ manifest + tail |
| `AnnExec` | Vector top-k; hot HNSW if present, else Lance IVF with nprobes/refine; merges tail brute-force | Collection @ manifest |
| `FilterBitmapExec` | Builds roaring bitmaps from Tantivy/Lance scalar indexes for pre-filtering | Filter predicate |
| `FusionExec` | Combines ranked lists: RRF, weighted score, DBSF (Qdrant-compatible) | ≥2 ranked inputs |
| `DocFetchExec` | Fetches documents/columns by stable row id from Lance (coalesced `take_rows`), and tail documents from the tail | Row ids |
| `ExpandExec` | 1–2 hop traversal over CSR/CSC + edge-delta overlay, with edge-type/property filters | Frontier vertex ids |
| `ShortestPathExec` | Bidirectional BFS | Two vertex sets |
| `StreamScanExec` | Scans stream segments by offset/time range, decodes RecordBatch → Arrow | Stream partitions |
| `TailMergeExec` | Unions durable results with tail results honoring upsert/delete semantics | Durable + tail |

All operators emit Arrow `RecordBatch` streams and report metrics (rows, bytes, cache hit ratios per layer, S3 GETs).

Row ids are one `RoaringTreemap` space for masks, bitmaps, fusion and fetch: durable rows carry their Lance stable row id (below 2^63), and tail documents carry `2^63 + seq`, a per-tail counter never reused within a process (M1.2 Ruling 7). As built in M1.2, aggregations and highlighting run beside `TantivySearchExec` over the same splits and tail, and `SparseExec` scores sparse vectors (§06 §6).

## 3. Optimizer rules

- **Index pushdown:** predicates on indexed columns become `FilterBitmapExec` inputs; `ORDER BY distance(v, q) LIMIT k` → `AnnExec`; `WHERE match(text, 'q') ORDER BY score LIMIT k` → `TantivySearchExec`. As built (M1.2 plan row 10.2), `CollectionProvider` pushes down only what keeps DataFusion's `Inexact` filter a superset: comparisons, `IN`, ranges and `IS [NOT] NULL` on keyword, integer, float, boolean, date and UUID fields with a literal of the column's type, and `NOT` only over exact, never-null operands; text and JSON predicates, and every predicate while a split older than the current schema version exists, are left to DataFusion.
- **Pre- vs post-filter selection** for ANN: cost-based on estimated filter selectivity (bitmap cardinality); highly selective → pre-filter (bitmap-restricted search / brute force on small sets); broad → post-filter with over-fetch. As built (M1.2, `AnnConfig`): a filter allowing at most `max(1 000, full_scan_threshold_kb · 1024 / (4 · dim))` durable rows is scored by brute force; one allowing at most 10 % of the durable rows prefilters the Lance index search; a broader one post-filters, asking for `k · 1.5 / selectivity` candidates and retrying once with 4× more, then prefilters. Index searches default to `nprobes = max(20, ⌈partitions / 16⌉)` and refine factor 20 (M1.2 plan row 6.1). Exact search (`exact`, a metric override, Manhattan, or no index) never uses an index, and every returned vector score is Operon's own exact kernel (M1.2 Ruling 3).
- **Projection matching** for aggregates: a query whose grouping keys and aggregates a declared aggregate projection covers is rewritten to read it → `ProjectionScanExec`.
- **Late materialization:** retrieve `(_pk, row_addr, score)` first, fetch documents only for final top-k.
- **Dynamic filters** (DataFusion 54.1, M1.1) propagate join/top-k bounds into scans, including across distributed stage boundaries.

## 4. Hybrid retrieval (native API)

One request, one plan. `POST /v1/namespaces/{ns}/query` accepts this body (M1.2); consistency tokens are `v1:`-prefixed text:

```json
POST /v1/namespaces/acme/query
{
  "from": "collections.memories",
  "consistency": {"token": "v1:s7/p3@918273"},
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

`expand` and `rerank` are **M3 (D44)**: M1.2 refuses a body that carries either (`invalid_argument`).

The `expand` stage is the GraphRAG path (D44): vector/BM25 seeds → 1–2 hops over a mapped graph → rerank, in one planned query.

The same retrieval is reachable from SQL. DataFusion 54 accepts only positional arguments for table functions in `FROM` (M1.2 Rulings 8 and 23), so the functions take them in Spice's order:

```sql
SELECT m._id, m.body, m._score
FROM hybrid_search('memories', 'embedding', [0.12, 0.34, 0.56], 'body', 'refund policy for enterprise', 100) AS m
WHERE m.tenant = 't42'
LIMIT 10;

SELECT _id, body, _score
FROM rrf(vector_search('memories', [0.12, 0.34, 0.56], 'embedding', 100),
         text_search('memories', 'refund policy for enterprise', 'body', 100))
LIMIT 10;
```

`vector_search(collection, query_vector [, field [, k [, filter_json [, exact]]]])` and `text_search(collection, text [, field [, k [, filter_json]]])` default `k` to 1 000; `rrf(retriever, retriever [, …] [, k [, limit]])` fuses nested calls of one collection with `k` 60; every function outputs `_score`. DataFusion folds the nested calls into literals before `rrf` plans them (M1.2 plan row 10.1), so `rrf` also accepts a retriever's JSON text, and `SELECT rrf(…)` in a projection returns that JSON. SQL search functions use Spice's names (D56); `rerank` is reserved for M3. The graph join (`graph_expand`) arrives with graphs in M3.

## 5. Consistency and snapshots

- At plan start, the coordinator resolves a **read snapshot** per referenced object: `(manifest_version | iceberg_snapshot_id, applied_offsets)`.
- If a consistency token is present, the required offsets define the tail range each object must merge: `(applied_offset, token_offset]`. Because the tail is read from the log, strong reads never wait for indexing.
- **Levels** (M1.2): `Strong` (the default; a linearizable high-watermark read, then the tail caught up to it), `AtLeast(token)`, `Eventual` and `Pinned { manifest_version, token }`. A pin holds nothing in the metastore: it reads its manifest plus a range tail up to its token for as long as the manifest is retained, then fails with `NotFound { kind: "pin" }` (M1.2 Ruling 14, A6).
- **Tail overlay rule** (M1.2 Ruling 1): a tail entry for key *k* in partition *p*, the latest op on *k* at offset *o*, overrides the durable state iff `o >= manifest.applied[p]`; entries below `applied` are already in the manifest and are dropped. One live tail therefore serves every manifest at or after its base. Reads the live tail cannot serve (a `Pinned` upper bound below its head, or an overflowed tail) build a *range tail* from the log over exactly `(applied, upper]`, cached per `(manifest, upper)`.
- `consistency: "eventual"` reads the durable state plus whatever tail the node already holds (lowest latency; bounded staleness = link lag).
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

## 8. Frontends

| Frontend | Parser/mapping | Notes |
|---|---|---|
| Native REST/gRPC | Hybrid request (§4) → logical plan; SQL endpoint with DataFusion SQL + Operon UDTFs | Primary surface; hybrid, graph `expand` and SQL |
| Arrow Flight SQL (ADBC) | DataFusion SQL + Operon UDTFs | Queries, and `DoPut` bulk ingest into collections and streams (D49) |
| ES Query DSL | Quickwit-derived DSL → logical plan | §06 |
| Qdrant query API | Direct mapping → logical plan | §06 |

Graph queries have no language frontend: traversal runs as `ExpandExec` and `ShortestPathExec`, reached through the SQL table functions (§1) and the hybrid `expand` stage (§4) (D44).

**Arrow Flight SQL** is a core surface from M1.2 (D49): high-throughput result transfer (Python/pandas/Polars, BI via ADBC) and zero-copy bulk ingest; the ADBC Flight SQL drivers (Python and Go) are an M1 exit gate.

As built (M1.2):
- **Listener:** `native.flight_sql` (`--flight-sql-listen`; default `0.0.0.0:8082`, `127.0.0.1:8082` for `operon dev`; `--no-flight-sql` turns it off).
- **Queries:** read-only SQL (DDL, DML and statements are refused on every served surface), the same planner as the REST `…/sql` endpoint. The namespace is the request metadata `operon-namespace` (default `default`); `operon-consistency-token` reads `AtLeast` that token, else `Strong`. `GetCatalogs` lists namespaces, `GetDbSchemas`/`GetTables` list collections (not aliases, M1.2 plan row 12.2).
- **`DoPut` bulk ingest** into collections and streams (D49): a PATH descriptor `["collections", c]`, `["streams", s]` or `["streams", s, p]` answers one `PutAck` per record batch (`{batch, rows, token, offsets}`, the token covering the whole put so far); `CommandStatementIngest` serves ADBC's `adbc_ingest` (`create`, `append` and `create_append`; `replace` is refused) and answers the row count. Collection columns: `_id` (unsigned integers, strings, or 16-byte UUIDs; `operon-id-type: u64|uuid`, as field or request metadata, types string keys), `_source` (one JSON object, verbatim), one column per dense vector (`FixedSizeList<Float32, dim>`; `_vector` for the unnamed vector, and a collection created by ingest names such a column's vector `""`, M1.2 plan row 13.2) or sparse vector (`Struct<indices, values>`), `_seq_no`/`_partition`/`_score` ignored, and, without `_source`, any other column as a top-level source key. Stream columns: `key`, `value`, `headers`, `timestamp`, `partition`. Batches are written in chunks of 10 000 rows through the native write path, one chunk in flight, each whole or not at all.
- **Scan plans:** pinned reads of a scan plan (D53) use the metadata `operon-pin-manifest` with `operon-consistency-token`; §17 §3.7 describes the scan plan as built.
