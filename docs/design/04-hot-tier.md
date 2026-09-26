# 04 — Hot Tier & Caching

Status: **Approved** · 2026-09-22 · amended 2026-09-26 (M1.2 as built)

**Principle:** every object has a **durable tier** (open format on S3, source of truth) and a **hot tier** (derived, node-local, rebuildable acceleration + an in-memory tail). The hot tier can be lost at any time without affecting correctness; it only affects latency. The same model applies uniformly to streams, collections, tables (Iceberg) and graphs.

---

## 1. Layers

| Layer | Medium | Contents | Keyed by | Implementation |
|---|---|---|---|---|
| **H0 metadata** | RAM | Manifests, Iceberg metadata/manifests, offset indexes, split hotcaches, Parquet footers/page indexes | Immutable object id / snapshot id | `moka` or `foyer` in-memory |
| **H1 object cache** | RAM → NVMe | Byte ranges of any durable object (Parquet pages, Lance pages, postings, sidecar chunks, segments) | `(object_path, offset, len)` | `foyer` hybrid cache |
| **H2 hot structures** | RAM / NVMe | Derived acceleration structures per object type (table below) | `(object_id, source_version)` | Per-type |
| **H3 tail** | RAM | Data committed to the log but not yet in the durable indexed form | `(object_id, partition, offset range)` | Per-type in-memory index |
| Durable | Object storage | Source of truth | — | §03 |

**Coherence is trivial by construction:** durable objects are immutable, so H0/H1 never need invalidation. Only *pointers* (manifest pointer, Iceberg current snapshot) change; nodes learn about them through meta watch streams (Operon-written objects) or Lakekeeper change events/polling (externally written Iceberg tables). From M2 the watch is a scoped change feed (`changes_since(catalog_version)`, or one namespace's changes), so a node refreshes only what changed instead of re-reading the catalog (D63, §18 §5.4).

The layering is the pattern StarRocks' Data Cache established for Iceberg on S3 (stateless compute over open files, a RAM + NVMe cache), applied uniformly to Parquet, Lance pages, Tantivy splits and graph sidecars.

## 2. Hot structures per object type

| Object | Durable tier | H2 hot structure | H3 tail |
|---|---|---|---|
| Stream | Segments / WAL objects | Recent segments pinned in RAM/NVMe; per-partition read-ahead | Recent record batches (written-through at produce); Arrow batches for `arrow`-encoded streams, shared with the object's tail index without decoding |
| Collection — vectors | Lance IVF index + vectors | **HNSW** (Qdrant `lib/segment`-derived: HNSW, filterable-HNSW links, quantization) on NVMe/RAM | One RAM Tantivy index plus flat vectors per collection on the query node, folded latest-by-key over the durable state (M1.2); tail vectors are scored by brute force |
| Collection — text | Tantivy splits on S3 | Splits pinned on NVMe (whole files), hotcaches in RAM | The same RAM Tantivy index (M1.2) |
| Collection — docs | Lance fragments | Hot fragments on NVMe | The same index's latest document per key (M1.2) |
| **Table (Iceberg)** | Parquet + Iceberg metadata | **Hot projections** (sorted columnar parts + sparse PK index + skip indexes + aggregate projections) on NVMe | Arrow buffers of rows beyond last Iceberg commit |
| Graph | CSR/CSC sidecars | CSR/CSC chunks resident in RAM for hot vertex ranges; hot vertex-ID map | Edge-delta overlay (adds/deletes since last sidecar build) |
| Durable execution (§14) | Origin documents | Canonical document bytes cached per origin, bounded by count and weight, revalidated with `If-None-Match: <etag>` on every read | — (every transition is a durable write) |

## 3. The Iceberg + Lakekeeper hot tier (tables)

Goal: interactive analytical latency (§7) and sub-second freshness on hot data, while every byte at rest stays standard Iceberg readable by any engine.

### 3.1 T0 — metadata hot tier
- Lakekeeper `LoadTable` response, `metadata.json`, manifest lists and manifests are fetched **once per snapshot** and decoded into an in-memory **file index**: per data file → partition values, column min/max/null counts, record count, DV reference, sort-order id.
- Pruning runs against this index with zero object-store I/O. New snapshots are applied **incrementally** (only added/removed manifests are read).
- Operon's own commits notify query nodes directly via meta; for external writers, subscribe to Lakekeeper CloudEvents or poll with ETag (default 5 s).
- ⇒ Lakekeeper is contacted only on cold start or snapshot change, never per query.

### 3.2 T1 — Parquet data cache
- Footers + page indexes + bloom filters pinned in RAM (H0).
- Column-chunk pages in `foyer` (RAM → NVMe), keyed by `(file_path, offset, len)` — Iceberg data files are immutable, so no invalidation.
- Read coalescing: adjacent page requests merged into single range GETs (≥ 1 MiB) on cold reads.

### 3.3 T2 — hot projections
For tables/partitions that are **pinned** or **auto-promoted** (§4):

- **Local parts:** data files re-encoded into node-local columnar parts (Arrow IPC + LZ4/ZSTD initially; Vortex as a future option), sorted by the table's sort key.
- **Sparse primary index:** one entry per 8,192-row granule (ClickHouse model) → binary search on sort-key prefixes.
- **Skip indexes:** per-granule min/max, bloom (tokens/ngrams for LIKE), set indexes for low-cardinality columns — declared per table (§08).
- **Aggregate projections:** declared rollups (for example `count(*)` and `sum(cost)` grouped by `day, tenant`) maintained incrementally with mergeable aggregate states; the planner rewrites queries the projection covers to read it (§05 §3).
- **Incremental maintenance from snapshot diffs:** added data files → new local parts; added DVs/deletes → local delete masks; background local merges. A projection is tagged with the Iceberg snapshot id it reflects.
- **Optional publication:** a worker may build projection parts once and publish them under `…/hot/projection/<snapshot>/` so new/replacement nodes download instead of rebuilding (still derived; safe to delete).

### 3.4 T3 — real-time tail
- The `stream → table` link keeps, on the query nodes that own the table's shards, **Arrow buffers of rows whose offsets are beyond the last Iceberg commit's applied offset**.
- Keyed tables: tail is a latest-by-key map; tail rows also produce a *pending delete mask* over Iceberg rows they supersede (via PK index lookups done at link time).
- Scans = hot projection or Parquet (at snapshot S) ⊎ tail (offsets after S), with delete masks applied ⇒ sub-second freshness even with 30 s Iceberg commits.
- When the next Iceberg commit lands, the covered tail range is dropped.

### 3.5 Planner choice
For each table scan: `hot projection @ S'` if present and `S'` ≥ required snapshot (or delta small enough to patch from T1) → else Parquet via T1 cache → else cold S3. Tail is always merged unless the query is `eventual`.

## 4. Promotion, demotion and budgets

- **Pin API:** `ALTER TABLE t SET HOT (partitions => 'last 7 days')`, `PUT /collections/c/hot {vectors: true, text: true}`, `ALTER GRAPH g SET HOT`.
- **Auto-promotion:** per-object heat from access counters (TinyLFU sketches) over sliding windows; promote when sustained QPS or scanned-bytes/min exceeds thresholds and budget allows.
- **Budgets:** per node (RAM, NVMe) and per namespace (fair share, weighted by plan/priority). Demotion by lowest heat-per-byte first.
- **Build placement:** heavy builds (HNSW, projections) run on workers and publish artifacts; light builds (tail indexes, CSR residency) run on the owning query node.

## 5. Routing and affinity

- Objects (or shards of large objects: table partitions/file groups, collection split groups, graph vertex-ID ranges) are mapped to query nodes by **rendezvous hashing**, AZ-aware, with replication factor *r* (default 1; auto-raised to 2–3 for very hot objects).
- **Bounded load** (M2): when the top node is above its load threshold, the next rendezvous choice serves the request. **Size-class placement keys** (M6): small namespaces are placed by namespace, so one node warms a tenant's collections together; large collections by `(ns, cid)`; very large ones by `(ns, cid, shard)` (D63, §18 §5.3).
- Ownership is a **soft hint**: correctness never depends on the owner, and any node can serve any namespace, so a stale route is slow, never wrong.
- Gateways route to the owning node(s); large scans fan out across owners via distributed execution (§05).
- On node loss/scale-out, ownership moves with minimal churn; the new owner serves cold from S3 while warming, or downloads published hot-tier artifacts. (Peer-to-peer cache transfer between nodes is a later optimization.)
- Prewarm API: `operon warm <object>` for planned failovers and deploys.

## 6. Failure and correctness rules

1. A query must produce identical results with or without any hot structure (tests enforce this by randomly disabling hot tiers — §12).
2. Hot structures carry the source version they reflect; stale structures are used only with an explicit, correct delta patch or not at all.
3. Loss of a node's tail is safe: the tail is re-derivable from the log (offsets after the applied offset).
4. Cache corruption is detected by per-block checksums; a failed checksum evicts and refetches from S3.
5. Exact paths (text, filters, aggregations, fetch, scroll, counts, exact vectors) are identical with the hot tier on and off; approximate ANN returns exact scores (R12). M1.2 gates this with a fake hot tier (`hot_hooks`, `determinism`), and every returned vector score comes from Operon's own kernel, never from a hot artifact (M1.2 Ruling 3).

## 7. Latency targets (design goals, from reference systems)

| Path | Target |
|---|---|
| Warm vector/text query (hot tier) | p50 5–20 ms |
| Cold collection query (from S3) | p50 0.5–1 s |
| Warm analytical query on hot projection | p50 10–100 ms (ClickBench-class queries) |
| Cold analytical query (Iceberg from S3) | seconds, scan-bound |
| Freshness (write → visible to strong reads) | immediate (tail); external Iceberg readers: commit cadence |
