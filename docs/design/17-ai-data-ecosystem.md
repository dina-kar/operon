# 17 — AI Data Ecosystem

Status: **Approved** · 2026-09-25 (decisions D51–D56) · amended 2026-09-26 (§3.7, M1.2 as built). Items marked (verify) are resolved in the implementation plans.

This document uses the product name **Loam** and the package name `loamdb` (D33). Everything here ships after the M1 rename except scan pinning (M1.2) and `to_arrow()` / `to_polars()` (M1.6), which land under the working names (`operon-*`, `operon-client`).

---

## 1. Goal and principle

Frontier AI labs run their data pipelines on a small set of tools: Ray Data for curation, deduplication and batch inference; PySpark (on Apache Spark, and increasingly on Sail) for large filters and joins; PyTorch and JAX loaders for training; Polars and DuckDB for local inspection. A lab adopts a new store when those tools read and write it without a copy step. Loam's retrieval data (documents, vectors, the eval sets built from them) must therefore be a **source and a sink for those tools**, alongside the Qdrant, Elasticsearch and Flight SQL surfaces that agent frameworks use (D42).

**Principle (D51): integrate over protocols and open formats, never by embedding other engines.** The integration surfaces are:

| Surface | Stable because | Used by |
|---|---|---|
| Arrow Flight SQL and Flight `DoGet`/`DoPut` (ADBC) | Wire protocol with a versioned protobuf schema | Spice, ADBC clients, the Flight fallback of every adapter, every sink |
| Lance files of a pinned version, handed out by **scan pinning** (§3, D53) | Lance file format 2.1 is pinned (§03 §6) | Ray Data, Polars, PySpark, `loamdb.torch`, Daft |
| Iceberg REST (Lakekeeper) | Open catalog protocol and table spec | DuckDB, Trino, Spark, Sail, ClickHouse, PyIceberg (M4) |
| Arrow C data interface (PyCapsule `__arrow_c_stream__`) | ABI-stable C structs | Polars, pandas, DuckDB, pyarrow in the same process |
| Native REST/gRPC API | Loam's own versioned API | Scan plans, dataset tags, search |

**Lockstep rationale.** A Rust binary links one version of DataFusion and one of arrow-rs, and engines built on them upgrade together. Lance 12 pins DataFusion 54 / arrow 58, which fixes Loam's versions (§11 §1). Sail's main branch moved to DataFusion 55.1 / arrow 59.2 on 2026-09-17; Spice patches crates.io with its own forks of DataFusion 54 and arrow-rs; Polars carries a second Arrow implementation (`polars-arrow`, forked from arrow2). Linking any of them would tie Loam's upgrade cadence to the slowest of Lance, Sail and Spice, or force a fork (risk 21). Sail's maintainers declined a Rust-level Lance integration for the same reason. Protocols and file formats do not move in lockstep, so every integration in this document crosses a process or format boundary, is pinned, and is tested in CI against the released tool.

## 2. Integration map

| Tool | Connects through | What an AI lab uses it for | Milestone | Gated |
|---|---|---|---|---|
| Python SDK `to_arrow()` / `to_polars()` | Arrow PyCapsule, zero-copy (§5.2) | Local inspection of search results and SQL output in notebooks; eval analysis | M1.6 | M1.6 plan tests |
| ADBC Flight SQL drivers (Python, Go) | Flight SQL queries and `DoPut` ingest | SQL and bulk loads from any language | M1.2 | **M1** (D49) |
| Spice | Spice's Flight SQL connector against Loam's Flight SQL frontend (§6.2) | Federated and accelerated SQL + search for agent apps | M1.2 | **M1** (D56) |
| Ray Data | `loamdb[ray]` datasource (scan plan + direct Lance fragment reads, built on `lance-ray`) and datasink (Flight `DoPut`) (§5.3) | Curation, deduplication, filtering; embedding backfills with `ray.data.llm`; batch inference | M2 | **M2** |
| Polars | `to_polars()` (M1.6); `loamdb[polars]` `scan_loam()`, an experimental IO plugin over the scan plan (§5.4) | Local and single-node inspection of collections and eval sets | M1.6 / M2 | **M2** |
| PySpark on Apache Spark 4 and on Sail | `loamdb[spark]` Python data source `format("loam")`: reads the scan plan (direct or Flight), writes through `DoPut` (§5.6) | Large curation and dedup jobs; joins of retrieval data with other lab data | M2 | **M2** |
| PyTorch / JAX | `loamdb[torch]` (`loamdb.torch`): Lance's PyTorch reader, an elastic deterministic sampler, mid-epoch resume, reads a tag (§5.5) | Training ingestion; reproducible eval sets | M2 | **M2** |
| DuckDB, Trino, Spark, Sail, ClickHouse | Iceberg REST catalog (Lakekeeper) over Loam tables | Eval results, usage and cost analytics, trace analysis | M4 | **M4** (D45, D55) |
| Ray Data, Polars, `loamdb.torch` over tables | Tagged Iceberg snapshots (`ray.data.read_iceberg(snapshot_id=…)`, `pl.scan_iceberg`) (§4.4) | Training and eval over analytical tables | M4 | **M4** |
| Daft | Its Lance reader with version pinning, given the scan plan's dataset URI and version (subject to Q20) | Curation on Daft's Rust engine | M1.2 (scan pinning) | Not gated |
| Hugging Face `datasets` | Reads Lance (the Hub hosts Lance datasets) and Iceberg, given a pinned version or a tagged snapshot (verify) | Dataset sharing and loading | M1.2 / M4 | Not gated |
| Agent frameworks (LangChain, LlamaIndex, LightRAG) | Qdrant API, Elasticsearch subset, native graph adapters (§06, §07) | Agent retrieval and memory | M1 / M3 | M1 / M3 |

Daft and Hugging Face `datasets` need nothing from Loam beyond scan pinning; they are listed so that the scan plan stays readable by any Lance reader that accepts a dataset URI and a version.

## 3. Scan pinning (D53, M1.2)

Loam commits Lance as **detached versions** (D34): the dataset's mainline holds only the empty version 1, and every collection manifest names its own detached `lance_version`. A plain `ray.data.read_lance(uri)` or `lance.dataset(uri)` therefore opens an empty dataset. Typed fields exist only in Tantivy (D36), and writes newer than the manifest are in the stream tail, not in Lance. External readers therefore get a pinned, consistent Lance version only by asking Loam for a **scan plan** (§03 §3.3).

### 3.1 Request and plan

`POST /v1/namespaces/{ns}/collections/{c}/scan-plans` (and the SDK's `collection.scan_plan(…)`) resolves the collection at one of:

| `at` | Resolves to | From |
|---|---|---|
| `current` (default) | The current manifest | M1.2 |
| `manifest_version` | That manifest, if it is still retained (§03 §7) | M1.2 |
| `consistency_token` | The current manifest, plus the tail up to the token (§3.2) | M1.2 |
| `tag` | The tagged manifest (§4); never has a tail | M2 |

The request may also carry `columns` (Lance columns, vector and sparse-vector names, or `_source` field paths), a `filter` in the native filter IR (§3.3), `tail` (§3.2) and, from M2, `credentials: true` (§3.4). A plan on a multi-target alias (D57) returns one pinned sub-plan per target collection.

The plan:

| Field | Content |
|---|---|
| `collection`, `manifest_version`, `resolved_from` | The collection id and name, the pinned Loam manifest version, and how it was resolved |
| `lance.uri` | The dataset root, `s3://<bucket>/ns/<ns>/collections/<cid>/lance` (§01 §6) |
| `lance.version` | The manifest's detached Lance version id (D34) |
| `lance.manifest_path` | `_versions/d<id>.manifest`, for readers that cannot open a detached version by id (§3.5) |
| `schema` | The Arrow schema of the Lance columns (`_pk`, `_source`, `_ingest_partition`, `_ingest_offset`, `_vector_<i>`, `_sparse_<j>`, `_rowid`), the map from each vector and sparse-vector name to its column, and the collection's field schema (kinds of the typed fields, for client-side projection from `_source`) |
| `fragments[]` | Per fragment: its id, physical row count, live row count (after its Lance deletion file), and a Flight ticket that streams the same rows through Loam |
| `tail` | Absent, or the tail descriptor of §3.2 |
| `row_filter` | Absent, or per-fragment row selections for the request's filter (§3.3) |
| `storage` | The object-store endpoint and region; from M2, vended credentials and their expiry (§3.4) |
| `valid_until` | When the pinned manifest may stop being retained (§3.6) |

The same plan is exposed through Flight: `GetFlightInfo` for a scan-plan command returns a `FlightInfo` whose endpoints are the fragments (plus one endpoint for the tail), so a Flight-native client parallelizes a scan without the REST call. Exact shapes are settled in the M1.2 plan.

Readers read each fragment directly from object storage with Lance (pylance, `lance-ray`), which applies the fragment's Lance deletion file, or through its Flight ticket when they have no object-store access. Both paths return identical rows. Lance deletion files are part of the pinned version (§03 §3.3 step 3), so a direct read of the pinned fragments sees exactly the live documents of the manifest.

### 3.2 The tail

A manifest reflects the implicit stream up to its `applied` offsets; later acknowledged writes are the tail (§05 §5). The request's `tail` field chooses how a plan treats it:

| `tail` | Plan | Use |
|---|---|---|
| `merge` (default with `consistency_token`) | Pinned manifest + a tail descriptor: the offset range, a Flight ticket that streams the tail's live documents in the Lance column layout (with a null `_rowid`), and the row ids in the pinned fragments that the tail supersedes (upserted or deleted keys, resolved through the tail index and the PK index at plan time) | Read-your-writes scans, e.g. a Ray job right after a sink |
| `wait` | Waits, up to a timeout, for a manifest whose `applied` offsets cover the token, and returns a tail-free plan | Jobs that want pure Lance reads and can wait for one link commit |
| `none` (default with `current`) | The pinned manifest only, no tail (eventual; staleness = link lag) | Most batch jobs |

A reader that merges the tail reads each fragment with `_rowid` excluded if it is in the superseded set, then appends the tail batch. The adapters do this themselves; a plain Lance reader given only `lance.uri` and `lance.version` must use a tail-free plan. A tag is always tail-free (D52), which is why training and eval jobs read tags.

### 3.3 Filters and projections

Lance holds `_source`, system columns and vectors; typed fields exist only in Tantivy (D36). A direct fragment read therefore sees `_source` (JSON bytes), `_pk`, `_ingest_partition`, `_ingest_offset`, `_rowid` and the vector columns, and no typed columns.

| Predicate or projection on | Where it runs |
|---|---|
| System columns (`_pk`, `_ingest_*`, `_rowid`) and vector nullness | In the reader, as a Lance scanner filter (`_pk` has a BTREE index) |
| Typed fields (keyword, numeric, date, boolean, JSON paths) | In Loam: the plan request carries the filter in the native filter IR; Loam evaluates it on the pinned manifest's splits (and on the tail, for `merge`) and returns `row_filter`, the matching rows per fragment as row offsets, so each read task receives only its own selection (applied with a fragment `take`; verify the pylance API) |
| Relevance (vector top-k, BM25, hybrid) | Not a scan: native search or Flight SQL, whose results carry `_rowid` and `_pk`; small result sets come back as Arrow directly (§5.2) |
| A predicate the adapter cannot translate to the filter IR | Post-filter on `_source` in the reader (JSON decode), after the pushed-down part |
| A projection of typed fields | Extracted from `_source` in the reader, typed by the plan's field schema (the adapters do this; `_source` stays available as a column) |

Server-side filtering keeps one filter evaluator (Tantivy, R5 of the M1 overview) for search, SQL and scans, so a filter means the same thing on every surface.

### 3.4 Credentials

| Stage | Direct fragment reads |
|---|---|
| M1 (`operon dev`, local filesystem or MinIO) | No vending: the reader uses the object-store configuration its operator already has; the plan carries only the endpoint |
| M2 (v1.0) | **Credential vending**: with `credentials: true` and the `read` permission on the collection (§10 §4), the plan carries short-lived, read-only object-store credentials scoped to the collection's `lance/` prefix: an S3 STS session with an inline session policy, a GCS downscoped token (Credential Access Boundaries), or an Azure user-delegation SAS (verify each), as Lakekeeper vends table credentials. The SDK refreshes them before expiry through a `refresh` call on the plan |

Direct reads bypass per-request authorization inside a collection, so credentials are vended only to principals whose read is unrestricted on that collection; a principal with field-level masking (Phase B) or any other restricted read gets Flight tickets only, and Loam enforces the restriction on the Flight path.

### 3.5 Opening a detached version (Q20)

**Answered (Q20, M1.2 Task 0):** pylance 12.0.0 and the Rust `lance` crate 12.0.0 open a detached version by id: `lance.dataset(uri, version=<detached id>)` and `DatasetBuilder::from_uri(uri).with_version(id).load()`, with Lance's default session and registry, both accept the u64 id (bit 63 set). The fallbacks below are not needed; the plan keeps `lance.manifest_path` anyway. Opening from the manifest file's bytes does not work (`serialized_manifest` expects the bare protobuf). The original question: whether pylance (and the Rust `lance` crate behind `lance-ray` and Daft) opens a detached version by id, e.g. `lance.dataset(uri, version=<detached id>)`. If it cannot, the fallbacks, in order: open the Lance manifest at `lance.manifest_path` through a Lance API that accepts a manifest location; have the Loam adapters build Lance fragment objects from the plan's fragment metadata; or, if Lance tags can reference detached versions, mirror each dataset tag (§4) as a Lance tag so that readers that know only Lance tags work unmodified (verify; Lance cleanup is never run, D35, so a Lance tag would not change GC). Daft and Hugging Face `datasets` work without a Loam adapter only under the first answer.

### 3.6 Validity

A plan pins a manifest without a metastore hold (D38): it stays readable while the manifest is retained, i.e. at least `time_travel_retention` (24 h) after the manifest is superseded, measured from the child manifest's creation. `valid_until` reports the earliest time the manifest may be released. A job that runs longer, or must be rerun later, pins a **tag** instead (§4). A read of a released manifest fails with a typed `SnapshotExpired` error, never with missing rows.

### 3.7 As built (M1.2)

M1.2 ships the core of §3.1–§3.6; the rest (filters, projections, Flight tickets per fragment, `tail: merge|wait|none`, credentials, multi-target aliases) stays with the SDK adapters (M1.6, M2) and D52–D57's milestones.
- **Route:** `POST /v1/namespaces/{ns}/collections/{c}/scan` (alias ok), body `{"at"?: …}`, answering the plan and the header `Operon-Consistency-Token: <pin.token>`. `at` is `"current"` (the default), `{"manifest_version": v}` or `{"token": "v1:…"}`; `{"tag": …}` is refused ("tags arrive in M2 (D52)"). Any node serves it: it reads the metastore, the manifest chain and one Lance manifest, never the tail.
- **The plan:** `namespace`, `collection`, `collection_id`, `manifest_version`, `schema_version`; `lance` (`null` before the first Lance version, else `{uri, version, manifest_path, storage_format, stable_row_ids}`, `version` the detached id); `fragments[]` in Lance's order (`id`, `physical_rows`, `deleted_rows`, `live_rows`, `files` as `data/…` paths, `deletion_file` as `{path, kind, deleted_rows}` or `null`, and `lance`, Lance's own `Fragment` JSON for `FragmentMetadata.from_json`); `live_rows` (equal to the manifest's live document count, checked); `columns` (the Lance version's schema with roles `pk`, `source`, `ingest_partition`, `ingest_offset`, `vector`, `sparse_vector`, and each vector's name, dim, distance or modifier); `pk_encoding` `operon_canonical_v1`; `tail`, `tail_records` and per-partition `offsets` (`applied` against `target`); `durable_token`; `pin`; `planned_at_ms` and `expires_at_ms`. The plan carries no credentials (D54, M2).
- **Lance deletion files are authoritative** (M1.2 Ruling 22): M1.1's commits delete every removed and superseded row in Lance itself, so the Lance version holds exactly the manifest's live rows, and the plan lists each fragment's deletion file for readers that bypass Lance; split delete bitmaps are not in the plan. What Lance cannot hold is the tail: the plan reports it (`tail`, `tail_records`, `offsets`) and carries a `pin` (`{manifest_version, token}`) that reads exactly the requested state, tail included, through the native API (`"consistency": {"pinned": …}`) or Flight SQL (metadata `operon-pin-manifest` with `operon-consistency-token`).
- **Token scans wait for durability, not for the tail** (M1.2 Ruling 24): `{"token": t}` waits up to `consistency_wait` (10 s) for a live manifest whose `applied` offsets cover `t`, then plans it (its target is `max(token, applied)` per partition, M1.2 plan row 14.2), else `Timeout`; `current` never waits and reports the tail; a manifest version is planned as it is, with no tail.
- **Expiry** (D38, A21): `expires_at_ms` is `planned_at_ms + time_travel_retention` for the live manifest, the child manifest's `created_at_ms + time_travel_retention` for a superseded one, and `null` for manifest version 0. Both are on the metastore clock, which advances only with time-stamped metastore writes, so `planned_at_ms` is 0 on a cluster that has had none (M1.2 plan row 15.5). A reader compares `expires_at_ms − planned_at_ms` with its own clock.
- **Verified** with the Rust `lance` crate over `file://` (the Lance version holds exactly the folded live documents); pylance, Daft, `FragmentMetadata.from_json` and `s3://` URIs are checked by M1.6 and M1.7.

## 4. Retained dataset tags (D52, M2)

A **dataset tag** is a named, immutable pointer from a collection to one manifest (from M4, from a table to one Iceberg snapshot) that is retained until the tag is deleted. Tags are the unit of reproducibility for training runs and eval sets, which outlive the 24 h time-travel window. The idea comes from TileDB's timestamped fragments and from Lance and Iceberg tags.

### 4.1 API

| Operation | REST (native API) | Semantics |
|---|---|---|
| Create | `POST /v1/namespaces/{ns}/collections/{c}/tags` `{"name", "at": {"current"} \| {"manifest_version"} \| {"consistency_token"}, "expires_at"?, "protect"?, "description"?}` | Returns `{name, manifest_version, lance_version, created_at}`. With a consistency token, the tag is taken on the first manifest whose `applied` offsets cover the token, waiting up to a timeout (default 30 s) for the link to commit one; otherwise it fails with `TokenNotCovered`. With a manifest version, it fails with `SnapshotExpired` if that manifest is no longer retained |
| List / get | `GET …/tags`, `GET …/tags/{name}` | Name, manifest version, creation and expiry, retained bytes (§4.3) |
| Delete | `DELETE …/tags/{name}` | Releases the root; GC reclaims objects no other root references |
| Read at a tag | `"snapshot": {"tag": "<name>"}` on native search and fetch; `at: {tag}` on scan plans (§3.1); a `tag => '<name>'` argument on the SQL table functions and a collection-snapshot table function (names settled in the M2 plan) | Reads the tagged manifest with no tail |

Tag names are unique per collection and never move: retagging is delete and create. The SDK mirrors the API (`collection.create_tag(name, at=token)`, `list_tags()`, `delete_tag(name)`, and `tag=` on every read and adapter). Tags live in the catalog behind `MetaStore` as domain operations (create, delete, list), so they are linearizable with manifest retention. The Qdrant and Elasticsearch surfaces do not expose tags. A tag created with `protect: true` makes a drop of its collection fail until the tag is deleted; otherwise dropping a collection deletes its tags.

### 4.2 GC and trimming

- A tag is a **GC root**: `CollectionGcRoots` keeps the tagged manifest and every object a retained manifest keeps (the files of its Lance version, its splits and delete bitmaps, its PK delta and dead letters; §03 §7). Manifests are self-contained, so a tag retains neither its manifest's ancestors nor their objects.
- Creating a tag and releasing a manifest are decided in the metastore: a tag can be created only on a manifest that is still retained, and GC reads tags and retention from one metastore snapshot. A manifest GC saw as retained is not deleted in that pass, and a manifest GC saw as released can no longer be tagged.
- Tags **never hold back implicit-stream trimming** (D38): a tag is taken only on a manifest whose `applied` offsets cover the requested token, so reading a tag never needs the tail, and streams are trimmed below the oldest time-travel-retained manifest exactly as before.
- Compaction and split merges after a tag leave the tagged manifest's old fragments and splits in place: a tag costs storage in proportion to how much of the collection has been rewritten since it was taken.
- Expired tags (`expires_at`) are deleted by a worker task, which then leaves their objects to GC.

### 4.3 Quotas

Tags are covered by the M2 tenant quotas: a per-namespace `max_tags_per_collection` (default 64) and the storage quota, which counts the bytes reachable only from tags. Each tag reports its **exclusive bytes** (objects no live or time-travel-retained manifest references) and the namespace metrics carry the total, so the storage cost of an old tag is visible.

### 4.4 Tags on tables (M4)

A tag on a table creates an Iceberg **tag ref** on the snapshot through Lakekeeper, with no `max-ref-age-ms`, so Iceberg snapshot expiry retains it for as long as the Loam tag exists. External engines read it with Iceberg's own syntax (Spark `VERSION AS OF '<tag>'`, Trino `FOR VERSION AS OF '<tag>'`; verify per engine), and Ray Data, Polars and `loamdb.torch` read it through the snapshot id the tag resolves to (M4 gate). Keyed tables read at a tag apply their deletion vectors as of that snapshot.

## 5. Python SDK extras (D54)

### 5.1 Packaging

The SDK's runtime dependencies stay minimal (the M1.6 plan's `httpx` and `anyio`); every integration is an optional extra, imported only by its module:

| Extra | Module | Adds | Milestone |
|---|---|---|---|
| `loamdb[flight]` | `loamdb.flight` | `adbc-driver-flightsql`, `adbc-driver-manager`, `pyarrow` (as in M1.6's `flight` extra) | M1.6 |
| `loamdb[polars]` | `loamdb.polars` | `polars`, `pyarrow` | M1.6 (`to_polars()`); M2 (`scan_loam()`) |
| `loamdb[lance]` | `loamdb.scan` | `pylance`, `pyarrow` (direct fragment reads; pulled in by the three extras below) | M2 |
| `loamdb[ray]` | `loamdb.ray` | `ray[data]`, `lance-ray` | M2 |
| `loamdb[torch]` | `loamdb.torch` | `torch` (declared as a lower bound only; the lab's own build is used) | M2 |
| `loamdb[spark]` | `loamdb.spark` | `pyspark` ≥ 4.0 (the client; Sail is reached through its Spark Connect endpoint) | M2 |

Versions are pinned in the M2 plan and each extra is tested in CI against the released tool (risk 21).

### 5.2 `to_arrow()` and `to_polars()` (M1.6)

Query results, search results and fetched documents expose `to_arrow()` (a `pyarrow.Table`) and `to_polars()` (a `polars.DataFrame`), and implement `__arrow_c_stream__`, so any PyCapsule consumer (Polars since 1.3, pandas, DuckDB, pyarrow) takes them without a copy. Results that arrive over Flight SQL are Arrow already; REST results are converted once in the SDK. Typed fields come from `_source` with the collection's field schema.

### 5.3 Ray Data datasource and datasink (M2)

```python
import ray
from loamdb.ray import read_loam, LoamDatasink

ds = read_loam(client, "web_docs", tag="dedup-2026-09-25",
               columns=["_pk", "text", "embedding"], filter={"term": {"lang": "en"}})
ds = ds.map_batches(Embedder, concurrency=16, num_gpus=1, batch_size=256)
sink = LoamDatasink(client, "web_docs", write="patch", vectors=["embedding_v2"])
ds.write_datasink(sink)
token = sink.consistency_token
```

- **Read.** `read_loam` requests a scan plan (§3) and builds a `ray.data.Datasource` whose `get_read_tasks(parallelism)` returns one read task per fragment (small fragments packed together up to `parallelism`), each with its live row count and size estimate as block metadata. Tasks read fragments directly with `lance-ray`'s fragment reader (Flight tickets when `mode="flight"`), apply their `row_filter` slice and the superseded-row set, and project typed fields from `_source`. Ray's projection and predicate pushdown hooks map onto `columns` and `filter`; the tail, when merged, is one extra read task.
- **Write.** `LoamDatasink` implements `on_write_start` (checks or creates the collection), `write` (each task streams its blocks through Flight `DoPut` with a path descriptor, `["collections", c]`, and returns the merged `PutAck` token of its puts; M1.2 Task 13) and `on_write_complete` (merges every task's token into one consistency token, exposed as `sink.consistency_token`, which a following `read_loam(…, at=token)` or tag creation uses). `on_write_failed` has nothing to roll back: acknowledged batches are visible, and writes are upserts keyed by `_id`, so a retried Ray task or a rerun job converges to the same documents.
- **Embedding backfills** write only the new vector column: the sink's `write="patch"` sends a `DoPut` write mode that maps each row to a `Patch` of the listed vectors instead of an `Upsert` (a `DoPut` option added in M2; verify against M1.2 Task 13's mapping). Large backfills run on the lab's own Ray cluster with `ray.data.llm`; Loam's `embed()` links (§09) serve continuous, smaller streams. Both are documented with an example.
- Streams become a Ray source in M5, over the native streaming subscribe.

### 5.4 Polars `scan_loam()` (M2, experimental)

`loamdb.polars.scan_loam(client, collection, tag=…)` returns a `LazyFrame` registered with `polars.io.plugins.register_io_source`, whose generator receives `with_columns`, `predicate`, `n_rows` and `batch_size`. It is marked experimental because Polars marks `register_io_source` unstable.

- `with_columns` becomes the plan's `columns`.
- The predicate is translated from Polars expressions to the native filter IR where it can be (comparisons, `is_in`, `is_null`, `&`, `|`, `~` over typed fields and system columns) and sent with the plan request (§3.3). The generator applies any untranslated remainder with `DataFrame.filter` before yielding each batch.
- `n_rows` stops the scan early; `batch_size` sizes the Lance reads.
- Tables (M4) use Polars' native `scan_iceberg` against Lakekeeper; `scan_loam()` is for collections. Polars' own `scan_lance` (a draft PR upstream) would still need a scan plan to find the version.

### 5.5 `loamdb.torch` (M2)

A thin training adapter, not a data-loading framework. It reads with Lance's PyTorch reader (`lance.torch.data.LanceDataset`) and replaces Lance's `ShardedFragmentSampler` / `ShardedBatchSampler`, which are deterministic for a fixed world size only, with a sampler that is **elastic**:

- **Pinned input.** A dataset is opened on a **tag** (or an explicit manifest version for short runs); the loader records the tag's manifest version and refuses to resume against a different one.
- **Deterministic order.** The epoch's sample order is a function of `(manifest, seed, epoch, P)` only. Samples, addressed by their position among the live rows of the plan's fragments in fragment order, are divided into **P canonical partitions** (contiguous fragment ranges; P is fixed for the whole run and stored in the checkpoint, by default the first run's world size, as Mosaic Streaming's `num_canonical_nodes`). Each partition is shuffled independently with a seeded block shuffle (shuffle fixed-size blocks, then samples within a window of blocks), and the global order interleaves the P partitions round-robin.
- **Elastic assignment.** Rank *r* of *W* takes global positions `consumed + r`, `consumed + r + W`, …; DataLoader workers stripe their rank's positions further. Changing *W* changes only the striding, never the global order, so every sample is seen exactly once per epoch at any GPU count. When *W* divides *P*, each rank reads from P/W partitions, which keeps reads local to few fragments.
- **Mid-epoch resume.** `state_dict()` returns `{format: 1, tag, manifest_version, seed, epoch, P, consumed}`, where `consumed` counts samples handed to the trainer (prefetched batches are not counted), and `load_state_dict()` continues from it on any world size; it fits a stateful DataLoader's checkpoint hook (e.g. torchdata's `StatefulDataLoader`; verify). The final step of an epoch may be uneven across ranks; `drop_last` is available but not used by the gate.
- **Columns.** `_source` fields named in `columns` are decoded into tensors or strings; vector columns are yielded as tensors; a user `transform` runs per batch.
- **JAX.** The sampler and batch iterator are framework-neutral; `loamdb.torch` also yields NumPy batches for JAX input pipelines.

### 5.6 PySpark data source `format("loam")` (M2)

A PySpark Python data source (`pyspark.sql.datasource.DataSource`, Spark 4) that runs unchanged on Apache Spark 4 and on Sail, which serves PySpark through Spark Connect and runs Python data sources in-process (verify the Sail version that supports them):

```python
from loamdb.spark import LoamDataSource
spark.dataSource.register(LoamDataSource)

df = (spark.read.format("loam")
      .option("endpoint", "https://loam.internal:8080")
      .option("namespace", "ml").option("collection", "web_docs")
      .option("tag", "dedup-2026-09-25")
      .load())
(df.write.format("loam").mode("append")
   .option("endpoint", "https://loam.internal:8080")
   .option("namespace", "ml").option("collection", "web_docs_clean")
   .save())
```

| Side | Behaviour |
|---|---|
| Read | The driver requests the scan plan; `DataSourceReader.partitions()` returns one partition per fragment (packed as in §5.3); `read(partition)` reads the fragment directly with pylance, or its Flight ticket, and yields Arrow record batches. Options: `tag`, `manifest_version` or `consistency_token`; `columns`; `filter` (native filter IR as JSON); `mode` (`direct`, `flight`, `auto`); `token` (M2 auth). Spark filters are pushed into `filter` where the Spark version's Python data source API offers filter pushdown (verify version) |
| Write | Each task streams its rows through Flight `DoPut` (path descriptor) and returns its merged token in its commit message; `commit` merges the tokens and logs the job's consistency token. Writes are upserts keyed by `_id` (or `patch`, as §5.3), so task retries are idempotent; there is no all-or-nothing job commit, and `abort` has nothing to roll back |

Tables (M4) are read by Spark and Sail through Iceberg REST (§6.1), not through `format("loam")`.

## 6. Spark and Sail (D55), Spice (D56)

### 6.1 Spark and Sail

| Path | Objects | How | Milestone |
|---|---|---|---|
| Python data source `format("loam")` | Collections | §5.6; the same code on Spark 4 and Sail | M2 |
| Iceberg REST | Tables (append-only and keyed) | Spark's Iceberg runtime or Sail's Iceberg support with a REST catalog pointed at Lakekeeper; credential vending by Lakekeeper (§10 §4) | M4 |

- **Sail is a named Iceberg reader in the M4 gate (D55).** Sail 0.7.1 writes Iceberg copy-on-write only and lists position deletes, equality deletes and deletion vectors as under construction. Loam's keyed tables carry deletion vectors (§03 §2.3), so M4 budgets an upstream contribution of DV and delete-file reads to Sail; the reader mirrors the DV writer M4 builds for iceberg-rust anyway.
- A table has one writer class (§03 §2.3): Spark and Sail may write tables that are not Loam link targets, never the keyed tables Loam's links maintain.
- Loam serves no Spark Connect endpoint (D42): Sail is the Spark Connect server; Loam is its source and sink.
- Sail's stateless workers with blocking shuffle to object storage and checkpointing (0.7) are the reference design for M6's distributed shuffle (§11 §3).

### 6.2 Spice

Spice (Apache-2.0 runtime; Spice.ai Enterprise proprietary) federates and accelerates data for agent apps on its own DataFusion fork. It is both a distribution channel and a competitor (risk 11).

- **Flight SQL source (M1 gate).** Spice's Flight SQL connector (beta) is a client in the M1.7 Flight SQL gate: a spicepod declares Loam collections as Flight SQL datasets, and the gate compares Spice's results with Loam's own, federated (no acceleration) and accelerated (Spice's Arrow accelerator). Spice's accelerators copy the data, so accelerated reads have Spice's refresh staleness, not Loam's consistency tokens. The namespace travels in the `operon-namespace` gRPC metadata (M1.6 W14); if Spice's connector cannot set custom metadata, the Flight SQL frontend also accepts namespace-qualified table names (verify in M1.7).
- **Function names (D56).** Loam's SQL search functions use Spice's names where they overlap:

  | Function | Loam | Spice |
  |---|---|---|
  | `vector_search` | Table function: dense or sparse top-k over a collection's vector (M1.2) | Vector search over an accelerated dataset |
  | `text_search` | Table function: BM25 over Tantivy (M1.2) | BM25 over Tantivy |
  | `rrf` | Reciprocal-rank fusion of two or more ranked table-function results (M1.2) | Reciprocal-rank fusion |
  | `rerank` | Reserved for M3's rerank stage | Model reranking |
  | `hybrid_search` | Loam's shorthand for filter + retrievers + fusion in one call (§05 §4); no Spice equivalent | — |

  Argument lists follow Spice's where the semantics match and extend them with named arguments (a vector literal, `k`, a filter); signatures are fixed in the M1.2 plan (verify against Spice's documentation).
- Whether Spice's federation pushes table-function calls down to a Flight SQL source is unverified; if it does not, Loam's search reaches Spice users through Loam views or direct Flight SQL queries.
- `datafusion-federation` (Apache-2.0, from Spice) is a candidate for M4, pinned at 0.5.5, the last release on DataFusion 54 (§11 §1.2). The Spice runtime itself is never embedded (§7).

## 7. What Loam does not build

- **Distributed Python compute.** Ray and Spark/Sail schedule the work; Loam hands out scan plans and accepts `DoPut` writes.
- **Dataframes.** Polars, pandas and DuckDB consume Arrow; Loam ships no dataframe API.
- **Training loaders beyond the thin adapter.** `loamdb.torch` is a sampler and resume state on top of Lance's reader; no prefetching framework, caching layer, sample-format codec or streaming shard format of its own.
- **GPU scheduling and model serving.** Embedding backfills and batch inference run on the lab's Ray or Spark cluster; `embed()` links and the M3 rerank stage call external model endpoints (§09).
- **A Spark Connect server, Lance or Iceberg forks, or engine plugins** in other engines' processes beyond the Python data source and IO plugin above.

Rejected dependencies:

| Project | License | Why not a dependency |
|---|---|---|
| Spice runtime (spiceai/spiceai) | Apache-2.0 (Enterprise edition proprietary) | Patches crates.io with forks of DataFusion 54 and arrow-rs (and forks of Ballista, iceberg-rust, Vortex, mistral.rs); embedding it would put Loam on Spice's fork cadence. A peer reached over Flight SQL (§6.2) |
| Sail crates (lakehq/sail) | Apache-2.0 | Main is on DataFusion 55.1 / arrow 59.2 while Loam is on 54 / 58 (Lance 12); its maintainers declined a Rust-level Lance integration for the same lockstep reason; Loam needs no Spark Connect server (D42) |
| Polars crates (`polars`, `polars-arrow`) | MIT (`polars-arrow`: MIT AND Apache-2.0) | A second Arrow implementation (forked from arrow2, no arrow-rs), so every boundary would convert; PyCapsule already gives Python users zero-copy exchange |
| TileDB core and `tiledb-rs` | MIT core; `tiledb-rs` has no license file | `tiledb-rs` is unusable without a license and "early wip"; the core embeds Rust pinned to arrow 55 / DataFusion 47; activity is slowing and TileDB Vector Search depends on tiledb-cloud. Its ideas (named retained snapshots, a tensor column type) are taken (§4, Q19) |
| Petastorm (uber/petastorm) | Apache-2.0 | Maintenance mode; Databricks marks it deprecated in favour of Mosaic Streaming; requires `pyspark`; Lance's PyTorch reader covers its role |

## 8. Gates and open questions

| Milestone | Deliverables (§12) | Exit gate (§12) |
|---|---|---|
| M1 | Scan pinning (D53) in the native API (M1.2); `to_arrow()` / `to_polars()` (M1.6) | The ADBC Flight SQL drivers (Python and Go) and Spice's Flight SQL connector pass against Loam (D49, D56). Proposed plan-level test for M1.2 (not an exit gate): a scan plan's fragments, read with pylance (after Q20) and through their Flight tickets, return the rows of a native scan of the same manifest |
| M2 (v1.0) | The AI data ecosystem workstream: dataset tags (D52), credential vending for direct fragment reads, the Ray Data datasource and datasink, Polars `scan_loam()` (experimental), `loamdb.torch`, the PySpark/Sail data source (D54) | A tagged collection read through Ray Data, Polars, PySpark (Spark 4 and Sail) and `loamdb.torch` returns identical rows, and a torch run resumed mid-epoch on a different GPU count sees each sample exactly once per epoch |
| M4 | Iceberg analytics; table tags as Iceberg tag refs (§4.4); Sail DV and delete-file reads upstream (D55) | Spark, Sail, Trino, DuckDB and ClickHouse read Loam tables (keyed tables with deletion vectors included) through Lakekeeper's Iceberg REST catalog; Ray Data, Polars and the torch loader read tagged table snapshots |

Risks: 11 (Spice, among others, competes for the same slot) and 21 (DataFusion/Arrow lockstep, mitigated by D51) in §12.

Open questions (§13):

| # | Question | Needed by |
|---|---|---|
| Q19 | A tensor column type for collections (Arrow `fixed_shape_tensor`; idea from TileDB) and how Lance stores it; relevant to training data beyond text and embeddings | M4 design |
| Q20 | Can pylance open a detached Lance version by id, or must Loam expose a manifest path or a custom opener (§3.5)? | M1.2 Task 0 |

## 9. Sources

- Spice: github.com/spiceai/spiceai (`v2.3.2/Cargo.toml`) · spiceai.org/blog/announcing-1.0-stable · spiceai.org/docs/reference/distributions · spice.ai/pricing · spiceai.org/docs/components/data-connectors · spiceai.org/docs/components/data-accelerators · spiceai.org/docs/features/search
- Sail: github.com/lakehq/sail (issue 2573; `v0.7.1/docs/guide/sources/iceberg/features.md`) · lakesail.com/blog/sail-0-7-blocking-shuffle-checkpoint · lakesail.com/product/spark-connect
- Ray: pypi.org/pypi/ray/json · pytorch.org/blog/pytorch-foundation-welcomes-ray-to-deliver-a-unified-open-source-ai-compute-stack · hpcwire.com/bigdatawire/2023/02/10/anyscale-bolsters-ray-the-super-scalable-framework-used-to-train-chatgpt · docs.ray.io/en/latest/data/api/doc/ray.data.Datasource.html · docs.ray.io/en/latest/data/api/doc/ray.data.Datasink.html · docs.ray.io/en/latest/data/working-with-llms.html · github.com/lance-format/lance-ray
- Daft: docs.daft.ai/en/stable/connectors/lance
- Polars: pola.rs/posts/announcing-polars-2 · docs.pola.rs/user-guide/misc/arrow · docs.pola.rs/api/python/stable/reference/api/polars.io.plugins.register_io_source.html · github.com/pola-rs/polars/pull/29413 · github.com/jonasdedden/polars-pylance
- PyTorch loaders: lance.org/integrations/pytorch · docs.mosaicml.com/projects/streaming/en/latest/distributed_training/elastic_determinism.html · docs.databricks.com/aws/en/archive/machine-learning/petastorm
- PySpark Python data sources: spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html
- TileDB: github.com/TileDB-Inc/TileDB · github.com/TileDB-Inc/tiledb-rs
