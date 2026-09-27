# 04 — Hot Tier & Caching

Status: **Approved** · 2026-09-22 · amended 2026-09-26 (M1.2 as built) · amended 2026-09-27 (M1.3 as built)

**Principle:** every object has a **durable tier** (open format on S3, source of truth) and a **hot tier** (derived, node-local, rebuildable acceleration + an in-memory tail). The hot tier can be lost at any time without affecting correctness; it only affects latency. The same model applies uniformly to streams, collections, tables (Iceberg) and graphs.

---

## 1. Layers

| Layer | Medium | Contents | Keyed by | Implementation |
|---|---|---|---|---|
| **H0 metadata** | RAM | Manifests, Iceberg metadata/manifests, offset indexes, split hotcaches, Parquet footers/page indexes | Immutable object id / snapshot id | `moka` or `foyer` in-memory |
| **H1 object cache** | RAM → NVMe | Byte ranges of any durable object (Parquet pages, Lance pages, postings, sidecar chunks, segments) | `(object_path, offset, len)` | `foyer` hybrid cache |
| **H2 hot structures** | RAM / NVMe | Derived acceleration structures per object type (table below) | `(object_id, source_version)` | Per-type. M1: HNSW artifacts (`qdrant-edge`), pinned splits |
| **H3 tail** | RAM | Data committed to the log but not yet in the durable indexed form | `(object_id, partition, offset range)` | Per-type in-memory index |
| Durable | Object storage | Source of truth | — | §03 |

Fragment prefetch fills H1 (the range cache that Lance reads through); it is not an H2 structure (M1.3 Ruling 9).

**Coherence is trivial by construction:** durable objects are immutable, so H0/H1 never need invalidation. Only *pointers* (manifest pointer, Iceberg current snapshot) change; nodes learn about them through meta watch streams (Operon-written objects) or Lakekeeper change events/polling (externally written Iceberg tables). From M2 the watch is a scoped change feed (`changes_since(catalog_version)`, or one namespace's changes), so a node refreshes only what changed instead of re-reading the catalog (D63, §18 §5.4).

The layering is the pattern StarRocks' Data Cache established for Iceberg on S3 (stateless compute over open files, a RAM + NVMe cache), applied uniformly to Parquet, Lance pages, Tantivy splits and graph sidecars.

## 2. Hot structures per object type

| Object | Durable tier | H2 hot structure | H3 tail |
|---|---|---|---|
| Stream | Segments / WAL objects | Recent segments pinned in RAM/NVMe; per-partition read-ahead | Recent record batches (written-through at produce); Arrow batches for `arrow`-encoded streams, shared with the object's tail index without decoding |
| Collection — vectors | Lance IVF index + vectors | **HNSW** built by `qdrant-edge` 0.8 behind `operon-hnsw` (R20), published under `hot/hnsw/<column>/<source_version:020>-<ulid>/`; views per manifest version add an appendable delta index and exclude rows deleted since (M1.3 Rulings 1–2) | One RAM Tantivy index plus flat vectors per collection on the query node, folded latest-by-key over the durable state (M1.2); tail vectors are scored by brute force |
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

**As built in M1.3 (collections):**
- **Pin:** `PUT /v1/namespaces/{ns}/collections/{c}/hot {"vectors", "text", "fragments"}` (booleans; absent keys are `false`) stores a `HotConfig` in the catalog (`SetCollectionHot`, M1.3 Ruling 7) and answers the hot status. `vectors` makes workers build and owners load HNSW artifacts; `text` pins every split of the live manifest on NVMe (verified by 64 KiB block checksums); `fragments` prefetches the Lance data, deletion and index files into H1. `--hot-pin-all` pins every collection on that process without a metastore write (Ruling 8); `--hot off` turns the tier off.
- **Warm:** `POST …/collections/{c}/warm` (and `operon warm <ns>/<collection>`) raises the collection's heat on its owner and marks it warm until the heat decays; it answers `202` with the status and never writes the metastore.
- **Status:** `GET …/collections/{c}` carries `hot` from the owner: the configuration, `pin_all`, `promoted`, the owner, and per structure `off | building | ready`, the effective `source_version` and `over_budget` (per vector column also the artifact's own source version, the delta's rows and the last load error).
- **Auto-promotion is off by default.** When on, a collection whose heat estimate (a TinyLFU-style count-min sketch, halved every `heat_window`) reaches `promote_min_hits` is promoted by its owner, which holds the lease `hot-promote/<ns>/<cid>` while it stays above `demote_below_hits`; workers treat a held lease like a pin, and demotion lets the lease go.
- **Budgets:** per node, NVMe (`--hot-nvme-bytes`), RAM (`--hot-ram-bytes`) and at most 32 open artifacts (`max_loaded_artifacts`, M1.3 Ruling 18). Each namespace's fair share is the budget divided by the namespaces with a resident or requesting structure. A candidate may evict a structure of a lower class (promoted below pinned), or of its own class with lower heat per byte that is in its own namespace or in a namespace over its share; evictions go by class, over-share first, then heat per byte. A pin that cannot fit is reported `over_budget`, and nothing is evicted for it. An evicted structure stays usable by the queries that hold it. A growing delta index is sized after each extension, so a column near the budget can be evicted and loaded again as its delta grows, until the rebuild lands.
- **Catalog reads:** the tier's reconcile and the maintenance and build sources read the whole catalog on each metastore change (at most every 100 ms), which M1 accepts because every node holds the catalog; M2's scoped change feed and dirty sets replace the triggers, not the reconcile logic (D63, M1.3 E68).

## 5. Routing and affinity

- Objects (or shards of large objects: table partitions/file groups, collection split groups, graph vertex-ID ranges) are mapped to query nodes by **rendezvous hashing**, AZ-aware, with replication factor *r* (default 1; auto-raised to 2–3 for very hot objects).
- **Bounded load** (M2): when the top node is above its load threshold, the next rendezvous choice serves the request. **Size-class placement keys** (M6): small namespaces are placed by namespace, so one node warms a tenant's collections together; large collections by `(ns, cid)`; very large ones by `(ns, cid, shard)` (D63, §18 §5.3).
- Ownership is a **soft hint**: correctness never depends on the owner, and any node can serve any namespace, so a stale route is slow, never wrong.
- Gateways route to the owning node(s); large scans fan out across owners via distributed execution (§05).
- On node loss/scale-out, ownership moves with minimal churn; the new owner serves cold from S3 while warming, or downloads published hot-tier artifacts. (Peer-to-peer cache transfer between nodes is a later optimization.)
- Prewarm API: `operon warm <object>` for planned failovers and deploys.

**As built in M1.3:**
- **Node registry:** each node holds the metastore lease `node/<node_id>` (TTL 10 s, renewed every 3 s) whose owner string is its descriptor `v1;<incarnation ulid>;<advertise addr>;<roles>;<zone>`; the live nodes are the leases that exist.
- **Rendezvous:** a placement key `(ns, kind, id[, shard])` (only collections in M1) scores each live `query` node as `xxh3_64(ns u64 BE ‖ cid u64 BE ‖ node_id u64 BE)` (other kinds and shards append the kind byte and the shard). The owners are the top *r* nodes by `(score desc, node_id asc)`, at most one per zone until every zone is used; *r* is `--replication` (default 1).
- **Suspects:** a node that failed a forward is skipped for 5 s; if every owner is suspect, suspects count again. The owner walk returns the whole ranking, so M2's bounded load is one more skip rule (M1.3 E62).
- **Addresses:** `--advertise` is an `ip:port`, or a host name resolved once at registration, because owners are addressed by `SocketAddr`; stable host names (Kubernetes, M2) revisit `Owner::Remote` (M1.3 E49).
- **Forwarding:** a gateway forwards a collection read to the first owner over `POST /internal/v1/reads/{op}` (JSON, carrying the request's consistency and token; the owner answers its own read token). A forwarded read is always executed where it lands, and the sender falls back to local execution when the owner answers `Unavailable` or `Timeout` or cannot be reached. Hot status and warm go to the owner the same way.

## 6. Failure and correctness rules

1. A query must produce identical results with or without any hot structure (tests enforce this by randomly disabling hot tiers — §12). M1 enforces it with the differential harness (`operon-hot` `differential`, M1.3) and at scale in M1.7; approximate ANN follows overview R12.
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
