# 03 — Storage Formats (Durable Tier)

Status: **Approved** · 2026-09-22 · revised 2026-09-25 (dataset tags, D52; scan pinning, D53)

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
- **Lakekeeper** (Rust, Apache-2.0) is the Iceberg REST catalog. It is bundled in Operon deployments and is also the endpoint external engines (Spark, Trino, DuckDB, ClickHouse, Snowflake) use.
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
- **External upsert writers** (e.g., the Flink or RisingWave Iceberg upsert sinks) commit equality deletes and bypass the PK index. A keyed table therefore has **one writer class**: either Operon links or an external engine. Externally written tables are read with equality-delete support and are never targets of Operon links; compaction converts their equality deletes to DVs without building a PK index.
- **Changelog:** because the PK index locates the old row, the apply worker can emit before/after images to the table's changelog stream (§02 §8.1) at the cost of one cached read per update.
- **Gap:** apache/iceberg-rust cannot yet write DVs or RowDelta commits (open PRs as of 2026-09). Plan: start from the **RisingWave iceberg-rust fork** (equality/position deletes, RewriteFiles), build the DV writer, and upstream it.

### 2.4 Maintenance
- Compaction (bin-pack + sort) via **nimtable/iceberg-compaction** (Rust, DataFusion-based), scheduled by workers (§09).
- Snapshot expiry, orphan-file removal, manifest rewrite — worker tasks with per-table policy.
- Partition spec and sort order are set per table at creation and changed through Iceberg partition and sort-order evolution (§08).

## 3. Collections — Lance + Tantivy under one manifest

### 3.1 Lance dataset
- Columns: `_pk` (canonical PK bytes), `_source` (the document's JSON bytes), `_ingest_partition` and `_ingest_offset` (the record that last wrote the document; returned as its `seq_no`), one `_vector_<i>` per dense vector (`FixedSizeList<f32>`, nullable), one `_sparse_<j>` per sparse vector (`Struct<indices: List<u32>, values: List<f32>>`, nullable; M1.1 Ruling 27), and Lance's stable row id `_rowid` (stable row ids are on, so row ids survive compaction).
- Typed fields are **not** Lance columns (M1.1 Ruling 4): they live in Tantivy (indexed and fast fields) and are always re-derivable from `_source`. Schema evolution of fields therefore never touches Lance; vector columns are added lazily, all-null, by a metadata-only commit (M1.1 Ruling 5).
- File format pinned to **Lance 2.1** initially, set explicitly (`V2_1`); Lance 12 defaults to 2.2. 2.2 adoption after evaluation.
- Indexes: vector index segments per §06 §5.1 (IVF_PQ default, IVF_RQ, IVF_HNSW_SQ), and a scalar BTREE only on `_pk`.
- Operon does **not** use Lance's per-write commit path for small writes (expensive, contended). Workers write large fragments from batched stream data and commit once per batch.
- Lance FTS is **not** used; text is in Tantivy.

### 3.2 Tantivy splits
- One **split** per indexing batch: an immutable, single-segment Tantivy index bundled into a single object with a **hotcache footer** (term dictionary skeleton, fast-field metadata, file offsets) — Quickwit's design, using Quickwit's `storage`/`directories`/split-bundle code vendored into `operon-quickwit` (R21).
- Splits carry Quickwit's footer trailer; open = one GET of `footer_range`, recorded in the manifest's `SplitRef`. Then range reads for postings/positions/fast fields. The workspace builds Tantivy `=0.26.2` with its `quickwit` feature (sstable term dictionary), so a split is not readable by a Tantivy built without it (M1.1 Ruling 10).
- Each Tantivy doc stores `_pk` (canonical bytes, indexed and fast) and the Lance stable row id (`_rowid` fast field); `SplitRef.row_id_ranges` map row ids to doc ids. Documents are added in ascending row-id order, so doc id *i* is the *i*-th document and row id → (split, doc id) is a binary search (M1.1 Ruling 8).
- Fast fields hold aggregation/sort columns (keywords, numerics, dates) for ES aggregations. A JSON field `n` is five Tantivy fields: `n`, `_text.n`, `_date.n`, `_null.n`, `_count.n` (M1.1 Ruling 26).
- Sparse vectors: split fields `_sparse.<name>` (u64 postings, one term per index plus `u64::MAX` for every non-empty vector) and `_sparse_w.<name>` (bytes fast weights: `u32 LE nnz ‖ nnz × u32 LE indices ‖ nnz × f32 LE values`) (M1.1 Ruling 27).
- **Deletes/upserts:** one whole roaring bitmap per split at `text/deletes/<split_ulid>/<ulid>.bitmap` (format `OPDB` v1, §3.4), rewritten whole on each change; the PK index identifies which row, hence which split/doc, to mark. A split whose every doc is deleted leaves the manifest. Merges (compaction, M1.3) drop deleted docs and recompute `row_id_ranges`.
- Merge policy: log-structured tiers by doc count (Quickwit's `StableLogMergePolicy`, vendored), bounded by split size (target 1–5 GiB for search-heavy collections).

### 3.3 Collection manifest
Immutable, protobuf-encoded inside the `OPCM` v1 envelope (§3.4), at `collections/<cid>/manifests/<version:020>-<ulid>.pb`. The fields are defined by [`crates/operon-collection/proto/collection_manifest.proto`](../../crates/operon-collection/proto/collection_manifest.proto) (package `operon.collection.v1`): the version and its parent (by version and by full path), the schema version, `created_at_ms`, the detached `lance_version`, the `SplitRef`s (with footer range, row-id ranges and delete-bitmap path), the vector and scalar index segments, hot artifacts (M1.3), the `applied` offsets per partition of the implicit stream, the live doc count, the commit kind, and this commit's PK delta and dead-letter paths.

**Commit protocol** (the collection link-apply task, one per collection; plan M1.1 Task 10 rule 3):
1. The task loads the parent manifest *v* and reads the schema. A PK handle is reused only while its watermark equals the parent's `applied`; otherwise the index is reopened (fencing any other writer) and repaired before any key is resolved.
2. It decodes the batch's records (an undecodable, wrong-partition or schema-violating record is a dead letter), resolves every key through the PK index, and folds each key's ops latest-wins in partition order.
3. It writes the new rows and deletes the superseded ones in Lance. Each Lance commit is a *detached* version built from exactly the parent manifest's `lance_version`; the mainline holds only the empty version 1 (R7, M1.1 Ruling 1). A crashed or fenced writer's Lance version is never referenced and never blocks the next commit.
4. It writes the new split and the changed delete bitmaps, then the PK delta (`pkdelta/…pkd`) and the dead letters (`deadletters/…dlq`), then manifest *v+1* (all create-only, new ULID paths).
5. It **CASes the pointer `collection/<cid>` in meta** (`expected = v`, fenced by the task lease, and freshness-checked: refused if the commit started more than `max_commit_delay` ago, which is below GC's grace).
6. The PK index is updated after the CAS and repaired from per-commit PK deltas: it stores `pk → row id` plus a watermark (the manifest and `applied` offsets it reflects); a new handle replays the deltas of the live chain newer than its watermark, or rebuilds from Lance if one is gone (M1.1 Ruling 7).

A reader that loads manifest *v* sees a mutually consistent Lance version, split set and delete bitmaps. Lance's own cleanup is never run; Operon GC computes Lance reachability from the Lance manifests of the retained chain (M1.1 Ruling 2, §7).

**External readers** (Ray Data, Polars, PySpark, the torch loader, any Lance reader) get a pinned Lance version only through **scan pinning** (D53, §17 §3): the dataset's mainline is the empty version 1, so opening the dataset root shows no rows. A scan plan names the manifest version, the Lance dataset URI with the manifest's detached version id, the fragments with their row counts and whether a tail exists; the reader reads those fragments (which carry `_source`, system columns and vectors, not the typed fields) or falls back to Flight.

### 3.4 Operon-defined collection formats
Each Operon format below is `magic (4 bytes) ‖ u16 LE format version ‖ body ‖ crc32c (u32 LE) of every preceding byte` (M1.1 Ruling 19):

| Magic | Object | Body (version 1) |
|---|---|---|
| `OPCM` | collection manifest, `manifests/<version:020>-<ulid>.pb` | protobuf `operon.collection.v1.CollectionManifest` |
| `OPDB` | delete bitmap, `text/deletes/<split_ulid>/<ulid>.bitmap` | split ULID (u128 BE) ‖ split doc count (u32 LE) ‖ cardinality (u64 LE) ‖ roaring portable serialization |
| `OPPD` | PK delta, `pkdelta/<version:020>-<ulid>.pkd` | postcard `Vec<(key bytes, Option<row id u64>)>`, keys strictly ascending, `None` = deleted |
| `OPDL` | dead letters, `deadletters/<version:020>-<ulid>.dlq` | postcard `Vec<DeadLetter { partition: u32, offset: u64, key: Option<bytes>, value: Option<bytes>, reason: String }>` ([`deadletter.rs`](../../crates/operon-collection/src/deadletter.rs)) |

Not enveloped:
- **Implicit-stream records** (overview §6.2), defined in [`codec.rs`](../../crates/operon-collection/src/codec.rs):
  - The key is the canonical PK bytes (`0x01` ‖ u64 BE, `0x02` ‖ 16-byte UUID, or `0x03` ‖ UTF-8). A key's partition is `xxh3_64(canonical) mod partitions`.
  - The value is `0x01` (codec version) ‖ postcard `WireOp`, at most 16 MiB with the version byte. Variant order and field order are the format:
    - `WireOp`: variant 0 `Upsert(WireDoc)`; variant 1 `Delete(pk)`; variant 2 `Patch { pk, mode, source, delete_keys, vectors, sparse_vectors, upsert: Option<WireDoc> }`.
    - `WireDoc { pk, source, vectors, sparse_vectors }`.
    - `source` is a JSON object as UTF-8 bytes, because postcard cannot carry `serde_json::Value`. `vectors` maps a name to `[f32]`, and `sparse_vectors` maps a name to `{ indices: [u32], values: [f32] }`; both maps are in ascending name order. In a `Patch`, each map value is an `Option`, where `None` removes the vector. `mode` is `PatchMode` (0 `MergeDeep`, 1 `MergeTop`, 2 `Replace`), and `delete_keys` is a list of dot-separated paths.
    - The `pk` inside `WireOp` is `PrimaryKey`'s derived serde enum in postcard (0 `U64` varint, 1 `Uuid` 16 bytes with no length, 2 `Str` varint length ‖ UTF-8). It is not the canonical bytes (M1.1 ruling P23).
  - The golden test `every_record_variant_has_a_pinned_encoding` in [`tests/codec.rs`](../../crates/operon-collection/tests/codec.rs) pins one byte literal per variant (M1.1 ruling P24).
- **PK index values** (inside SlateDB, which checksums its blocks): `0x01 ‖ row id u64 BE`; the watermark under the key `0x00 "watermark"` is `0x01 ‖ postcard(PkWatermark { manifest_version, applied })`.
- **Splits**: Quickwit's bundle: the index files, then the footer = bundle metadata ‖ its length (u32 LE) ‖ hotcache ‖ its length (u32 LE) ‖ the 16-byte trailer (footer start u64 LE ‖ trailer version u32 LE = 1 ‖ `QWFT`). `SplitRef.footer_range` spans the whole footer, trailer included (M1.1 Ruling 9).

## 4. Graph structures

### 4.1 Vertex-ID map
- Per vertex label, a SlateDB instance mapping `external_key → dense u64 vertex_id` (and reverse). Dense IDs make CSR arrays compact and cache-friendly.
- IDs are assigned by the graph-link worker in batches; one ID per external key is enforced with SlateDB transactions (SSI).

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

A single abstraction (`PkIndex`) backed by **SlateDB** (object-storage-native LSM, writer fencing, SSI transactions) per keyed object, located at `ns/<ns>/pk/<object_id>/` (for example `ns/<ns>/pk/collection-<cid>/`). Used by: keyed tables (row positions), collections (the Lance stable row id; derived state with a watermark, §3.3), graph vertex-ID maps, and changelog streams in `full` mode (before-image lookup). Written only by the owning worker (single-writer per object shard, fenced by lease epoch).

## 6. Format versioning and compatibility

- Every Operon-defined format (WAL object, segment, split footer extensions, manifests, sidecars) carries `magic + format_version`; readers support N and N−1.
- WAL chunks and segments also carry an `encoding` field from their first version, so adding `arrow` (§02 §5) is not a format break.
- Durable-execution documents use the Resonate blob format (header `v`, currently 1) unchanged; Operon does not extend it, so upstream tools can read Operon's `durable/` prefix.
- Third-party formats are pinned: Lance file format 2.1, Iceberg spec v2 with v3 features enabled per table, Tantivy index version as shipped by the pinned fork.
- Upgrades that change formats are opt-in per object and rolled forward by compaction.

## 7. Garbage collection

- Reachability-based: an object is deletable when no live manifest/snapshot/offset-index entry or dataset tag (M2) references it **and** it is older than the grace period (default 1 h; ≥ longest query timeout).
- Time travel: manifests/snapshots retained per policy (default 24 h for collections/graphs, Iceberg snapshot policy for tables); GC respects retention.
- Collections keep every manifest for `time_travel_retention` (24 h) after it is superseded, plus the last `keep_manifests`; implicit streams are trimmed below the oldest retained manifest. GC keeps every object a retained manifest references (splits, delete bitmaps, PK deltas, dead letters and the files of its Lance version), and deletes a released manifest's objects no earlier than the manifest itself. A dropped collection's prefixes are retired and deleted once the drop is older than the grace period (M1.1 Rulings 12, 13).
- **Dataset tags** (M2, D52, §17 §4): a named tag pins one collection manifest until the tag is deleted (or expires). GC keeps a tagged manifest and the same objects a retained manifest keeps, but not its ancestors. A tag can be created only on a manifest that is still retained, and GC reads tags and retention from one metastore snapshot, so a manifest is never both tagged and collected. A tag is taken only on a manifest whose `applied` offsets cover the requested consistency token, so reading it never needs the stream tail and tags never hold back implicit-stream trimming. From M4 a tag on a table is an Iceberg tag ref without a maximum ref age, which Iceberg snapshot expiry already respects.
- `durable/` is outside reachability GC: the Resonate server owns those objects and collects its own orphan timers. Settled-promise retention (deleting old origin documents) is a per-namespace policy run as a worker task.
