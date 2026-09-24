# 03 — Storage Formats (Durable Tier)

Status: **Approved** · 2026-09-22

The durable tier is the **only source of truth**. It consists of open formats on object storage plus small metastore pointers. Every structure here must be (a) immutable once written, (b) addressable by range reads, and (c) openable cold in ≤ 3 sequential object-store round trips.

---

## 1. Format map

| Object | Primary format | Secondary structures | Pointer lives in |
|---|---|---|---|
| Stream | Operon WAL objects → segments (`kafka`: RecordBatch v2 bytes; `arrow`: Arrow IPC, §02 §5) | Sparse offset/timestamp index in footer; per-column ranges for `arrow` | Meta (offset index) |
| Table | **Apache Iceberg** v2/v3 (Parquet data, deletion vectors in Puffin) | PK index (SlateDB) for keyed tables | **Lakekeeper** (Iceberg REST catalog) |
| Collection | **Lance** dataset (docs, vectors, scalar + IVF indexes) | **Tantivy splits** + per-split deletion bitmaps; PK index | Meta → collection manifest (S3) |
| Graph | Source tables/collections | CSR/CSC adjacency sidecars; vertex-ID map (SlateDB) | Meta → graph manifest (S3) |
| Durable execution (§14) | Resonate blob documents: one canonical JSON-lines document per origin | Timer objects (zero-byte, name = record); schedule objects | None: the document itself is the state, replaced by conditional PUT |

## 2. Tables — Iceberg via Lakekeeper

### 2.1 Catalog
- **Lakekeeper** (Rust, Apache-2.0) is the Iceberg REST catalog. It is bundled in Operon deployments and is also the endpoint external engines (Spark, Trino, DuckDB, Snowflake) use.
- Lakekeeper stores catalog state in Postgres today (verify pluggability). Options, in order of preference:
  1. Implement Lakekeeper's catalog-backend trait on the Operon metastore (no extra dependency) — **verify the trait is pluggable**.
  2. Bundle a small managed Postgres for the catalog only (acceptable for large deployments).
- Lakekeeper is **not on the query hot path** (§04): Operon caches table metadata and learns about commits from its own workers or from Lakekeeper change events (CloudEvents).

### 2.2 Write path
- Workers apply `stream → table` links: batch records → Arrow → Parquet (sorted by the table's sort order, row groups 128 MiB target, page index + bloom filters on configured columns) → Iceberg commit through Lakekeeper with optimistic concurrency.
- **Commit cadence**: default every 30 s or 512 MiB per table (configurable 5 s–10 min). Freshness below cadence comes from the tail (§04), so commits can stay large and cheap.
- Snapshot summary records `operon.link.<link_id>.offsets = {partition: offset}` ⇒ exactly-once apply and restart safety.

### 2.3 Keyed tables (upsert/delete)
- Keyed tables maintain a **PK index**: SlateDB instance mapping `pk → (data_file, row_position)`, updated by the worker that writes each data file (it knows positions).
- Upsert = write new row + add old position to the **deletion vector** of its data file (Iceberg v3 Puffin DV). This keeps reads merge-on-read-cheap.
- Fallback for engines/versions without v3: equality deletes, converted to DVs/rewrites by compaction.
- **External upsert writers** (e.g., RisingWave's Iceberg upsert sink) commit equality deletes and bypass the PK index. A keyed table therefore has **one writer class**: either Operon links or an external engine. Externally written tables are read with equality-delete support and are never targets of Operon links; compaction converts their equality deletes to DVs without building a PK index.
- **Changelog:** because the PK index locates the old row, the apply worker can emit before/after images to the table's changelog stream (§02 §8.1) at the cost of one cached read per update.
- **Gap:** apache/iceberg-rust cannot yet write DVs or RowDelta commits (open PRs as of 2026-09). Plan: start from the **RisingWave iceberg-rust fork** (equality/position deletes, RewriteFiles), build the DV writer, and upstream it.

### 2.4 Maintenance
- Compaction (bin-pack + sort) via **nimtable/iceberg-compaction** (Rust, DataFusion-based), scheduled by workers (§09).
- Snapshot expiry, orphan-file removal, manifest rewrite — worker tasks with per-table policy.
- Partition spec and sort order map from ClickHouse `PARTITION BY` / `ORDER BY` (§08).

## 3. Collections — Lance + Tantivy under one manifest

### 3.1 Lance dataset
- Columns: `_pk`, `_row_version`, `_ingest_offset`, user fields, vector columns (FixedSizeList<f32|f16|u8>), JSON payload (`large_binary`/JSON), `_deleted` (logical).
- File format pinned to **Lance 2.1** initially (stable default); 2.2 adoption after evaluation.
- Indexes: IVF_RQ or IVF_PQ per vector column (IVF_HNSW_SQ for high-recall collections); BTREE/BITMAP/LABEL_LIST scalar indexes for filter columns.
- Operon does **not** use Lance's per-write commit path for small writes (expensive, contended). Workers write large fragments from batched stream data and commit once per batch.
- Lance FTS is **not** used; text is in Tantivy.

### 3.2 Tantivy splits
- One **split** per indexing batch: an immutable Tantivy index bundled into a single object with a **hotcache footer** (term dictionary skeleton, fast-field metadata, file offsets) — Quickwit's design, using Quickwit's `storage`/`directories`/split-bundle crates (forked, pinned).
- Opening a split cold = 1 GET for the footer/hotcache, then range reads for postings/positions/fast fields.
- Each Tantivy doc stores `_pk` and the **Lance row address** so hits join to documents without a lookup.
- Fast fields hold aggregation/sort columns (keywords, numerics, dates) for ES aggregations.
- **Deletes/upserts:** per-split **deletion bitmaps** (roaring), versioned, written as small objects; the PK index identifies which split/doc to mark. Merges (compaction) drop deleted docs and rewrite row addresses.
- Merge policy: log-structured tiers by doc count (Quickwit's `StableLogMergePolicy` as reference), bounded by split size (target 1–5 GiB for search-heavy collections).

### 3.3 Collection manifest
Immutable, protobuf-encoded, `collections/<id>/manifests/<version>.pb`:

```text
CollectionManifest {
  version:            u64
  parent_version:     u64
  schema_id:          u64
  lance_version:      u64                      // Lance dataset version
  splits:             [SplitRef { ulid, doc_count, size, min_offset, max_offset, delete_bitmap_version }]
  vector_indexes:     [{column, lance_index_uuid, trained_at_version}]
  hot_artifacts:      [{kind: HNSW, column, object_prefix, source_version}]   // optional, derived
  applied_offsets:    {stream_id/partition: offset}
  created_at:         timestamp
}
```

**Commit protocol:** worker writes Lance fragment(s) + Tantivy split + deletion bitmaps (all new objects) → commits Lance version → writes new manifest object → **CAS the manifest pointer in meta** (`expected = parent_version`). A reader that loads manifest *v* sees a mutually consistent Lance version, split set and delete bitmaps. Lance versions not referenced by any live manifest are cleaned up by GC.

## 4. Graph structures

### 4.1 Vertex-ID map
- Per vertex label, a SlateDB instance mapping `external_key → dense u64 vertex_id` (and reverse). Dense IDs make CSR arrays compact and cache-friendly.
- IDs are assigned by the graph-link worker in batches; `MERGE` uniqueness is enforced with SlateDB transactions (SSI).

### 4.2 Adjacency sidecars
For each edge source segment (an Iceberg data file or a Lance fragment), the graph link writes:

```text
<segment>.csr (forward, sorted by src)          <segment>.csc (reverse, sorted by dst)
  header { edge_type, src_label, dst_label, vertex_id_range, edge_count, source_ref }
  offsets:   delta-encoded, bitpacked u64[#src+1]   (chunked, GraphAr-style offset chunks)
  neighbors: delta-encoded, bitpacked u64[edge_count]
  edge_rows: row address in source segment (for edge properties)
  footer:    chunk index + crc
```

- Chunked layout means a k-hop expansion touches only the chunks for the frontier's ID ranges (few range GETs cold; RAM-resident when hot).
- Deleted edges are masked via the source's deletion vector / delete bitmap.
- Compaction of the source segments triggers rebuild of their sidecars.
- Apache GraphAr is used for **import/export** and as a layout reference, not as the mutable store.

## 5. Primary-key indexes

A single abstraction (`PkIndex`) backed by **SlateDB** (object-storage-native LSM, writer fencing, SSI transactions) per keyed object, located at `ns/<ns>/pk/<object_id>/`. Used by: keyed tables (row positions), collections (split doc + Lance row address), graph vertex-ID maps, and changelog streams in `full` mode (before-image lookup). Written only by the owning worker (single-writer per object shard, fenced by lease epoch).

## 6. Format versioning and compatibility

- Every Operon-defined format (WAL object, segment, split footer extensions, manifests, sidecars) carries `magic + format_version`; readers support N and N−1.
- WAL chunks and segments also carry an `encoding` field from their first version, so adding `arrow` (§02 §5) is not a format break.
- Durable-execution documents use the Resonate blob format (header `v`, currently 1) unchanged; Operon does not extend it, so upstream tools can read Operon's `durable/` prefix.
- Third-party formats are pinned: Lance file format 2.1, Iceberg spec v2 with v3 features enabled per table, Tantivy index version as shipped by the pinned fork.
- Upgrades that change formats are opt-in per object and rolled forward by compaction.

## 7. Garbage collection

- Reachability-based: an object is deletable when no live manifest/snapshot/offset-index entry references it **and** it is older than the grace period (default 1 h; ≥ longest query timeout).
- Time travel: manifests/snapshots retained per policy (default 24 h for collections/graphs, Iceberg snapshot policy for tables); GC respects retention.
- `durable/` is outside reachability GC: the Resonate server owns those objects and collects its own orphan timers. Settled-promise retention (deleting old origin documents) is a per-namespace policy run as a worker task.
