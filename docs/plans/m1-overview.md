# M1 — Collections (Elasticsearch + Qdrant): Overview and Shared Contracts

Status: **Planned** (2026-09-24). This document splits milestone M1 into seven plans and fixes the contracts between them. Each plan's tasks implicitly include this document's Global Constraints and must use the contracts here verbatim (§6). If a plan and this document disagree, this document wins until an amendment is recorded in its "Amendments" section.

Design references: [01 Architecture](../design/01-architecture.md), [03 Storage formats](../design/03-storage-formats.md) §3, §5, §7, [04 Hot tier](../design/04-hot-tier.md), [05 Query engine](../design/05-query-engine.md), [06 Search & vector](../design/06-search-and-vector.md), [09 Links & workers](../design/09-links-and-workers.md), [11 Buy vs build](../design/11-buy-vs-build.md), [12 Roadmap](../design/12-roadmap-testing-risks.md) §1 (M1 row), [15 Agent workspaces](../design/15-agent-workspaces.md) §10.1 (W0). Built on M0 as described in the [M0 exit report](m0-exit-report.md).

## 1. Goal

Ship collections: documents with text, keyword, numeric, date, boolean and JSON fields and named dense vectors, stored as one Lance dataset plus Tantivy splits under one manifest, written only through the log, read with strong consistency by a DataFusion-based hybrid engine, served through a native API, SQL and Arrow Flight SQL, the Qdrant and Elasticsearch APIs (Phase A), Python and TypeScript SDKs and an MCP server, accelerated by a hot tier that never changes results, and proven by the M1 exit gates.

## 2. Plans

| Plan | Branch | Scope | Depends on |
|---|---|---|---|
| [M1.1: Collection storage](2026-09-24-m1.1-collection-storage.md) | `m1.1-collection-storage` | Collection catalog in meta (collections, implicit streams, aliases, schema evolution, drop); `DocOp` record format; atomic multi-partition append; Lance dataset + Tantivy split writers; collection manifest and fenced, freshness-checked CAS; upserts, patches and deletes through the PK index and delete bitmaps; the `collection` link target; vector and scalar index builds; collection GC roots; carried-in M0 items | M0 |
| [M1.2: Query engine and native API](2026-09-24-m1.2-query-engine.md) | `m1.2-query-engine` | `operon-query`: the search IR, DataFusion catalog and operators (`TantivySearchExec`, `AnnExec`, `FilterBitmapExec`, `FusionExec`, `DocFetchExec`, `TailMergeExec`), the tail index, consistency tokens and strong reads, global BM25 statistics, `CollectionService`; native REST collection, document and hybrid query endpoints; SQL with `vector_search`/`text_search`/`hybrid_search`; Arrow Flight SQL | M1.1 |
| [M1.3: Hot tier, maintenance and affinity routing](2026-09-24-m1.3-hot-tier-routing.md) | `m1.3-hot-tier-routing` | Split merges and Lance compaction; Qdrant-derived HNSW hot artifacts (worker build, publish, load, appendable tail HNSW); pinned splits and fragments on NVMe; pin/warm APIs; the metastore over the network, node registry, rendezvous ownership and request forwarding; the hot-on/off differential harness | M1.2 |
| [M1.4: Qdrant API Phase A](2026-09-24-m1.4-qdrant-api.md) | `m1.4-qdrant-api` | `operon-qdrant`: REST (6333) and gRPC (6334) gateways per §06 §8 Phase A, over `CollectionService` | M1.2 (M1.3 for hot-tier params) |
| [M1.5: Elasticsearch API Phase A](2026-09-24-m1.5-elasticsearch-api.md) | `m1.5-elasticsearch-api` | `operon-es`: REST gateway (9200) per §06 §7 Phase A: document APIs, `_search` with the Phase A DSL, `knn`, aggregations, highlighting, PIT/`search_after`, index admin | M1.2 |
| [M1.6: SDKs and MCP server](2026-09-24-m1.6-sdks-mcp.md) | `m1.6-sdks-mcp` | Python and TypeScript SDKs for the native API (plus Flight SQL from Python); `operon-mcp`, the W0 MCP server on the 2026-07-28 stateless spec | M1.2 |
| [M1.7: M1 exit gates](2026-09-24-m1.7-exit-gates.md) | `m1.7-exit-gates` | Conformance: LangChain and LlamaIndex vector-store tests (ES and Qdrant backends), client-library suites; BEIR nDCG@10 vs ES BM25; Recall@10 vs Qdrant at equal latency; hot on/off identity at scale; the M1 exit report | M1.3, M1.4, M1.5, M1.6 |

M1.4, M1.5 and M1.6 are independent of each other and may run in parallel once M1.2 is merged. Branches stack on their dependency until it merges; PRs target `main`.

**Plans are reconciled before execution.** Only M1.1 is written against code that exists today. Each later plan starts with a Task 0 that reads the as-built code of the plans it depends on, lists every difference from that plan's "Consumes" block, and records the resolution in the plan's "Rulings made during execution" before any other task starts.

## 3. Scope and exit gates → plans

| Design §12 M1 item | Plan |
|---|---|
| Lance + Tantivy splits under one manifest | M1.1 |
| Upserts/deletes | M1.1 (write), M1.2 (read semantics) |
| Tail indexes | M1.2 |
| Native hybrid API + Python/TS SDK | M1.2 (API), M1.6 (SDKs) |
| Arrow Flight SQL | M1.2 |
| Qdrant API Phase A | M1.4 |
| ES API Phase A | M1.5 |
| Hot tier: pinned splits + Qdrant-derived HNSW artifacts | M1.3 |
| Affinity routing | M1.3 |
| W0: MCP server (§15 §13, "with M1") | M1.6 |

| Exit gate | Plan |
|---|---|
| LangChain + LlamaIndex vector-store tests (ES and Qdrant backends) pass unmodified | M1.7 (surface built in M1.4, M1.5) |
| BEIR nDCG@10 within 1 point of ES BM25 | M1.7 (analyzers and global statistics in M1.2) |
| Recall@10 within 1% of Qdrant at equal hot-tier latency | M1.7 (HNSW in M1.3) |
| Results identical with the hot tier on and off | M1.3 (harness), M1.7 (at scale); see Ruling R12 |

## 4. Dependencies

Verified together by the [M1 dependency spike](m1-dependency-spike.md) (compiled and exercised in a throwaway crate on 2026-09-24):

| Crate | Version | Notes |
|---|---|---|
| `lance`, `lance-index`, `lance-linalg`, `lance-table`, `lance-io`, `lance-file` | `=12.0.0`, `default-features = false` | Lance's defaults pull opendal and the AWS SDK |
| `datafusion` | `54.1` | **Not 55**: Lance 12 pins DataFusion 54 / arrow 58. DataFusion, arrow and Lance move in lockstep (design §11 amended by M1.1) |
| `arrow`, `arrow-array`, `arrow-schema`, `arrow-flight` (`flight-sql-experimental`) | `58.4` | |
| `tantivy` | `=0.26.2` | Quickwit's own tantivy fork rev is not used |
| `qdrant-edge` | `=0.8.0` | Only in `operon-hnsw` (≈171 extra packages) |
| `roaring` | `0.11` | One copy shared with Lance and qdrant-edge |
| `tonic` / `tonic-prost-build` / `prost` | `0.14` | Qdrant gRPC (vendored public protos), the collection manifest |
| `rmcp` | `3.4.1`, features `server`, `transport-streamable-http-server` | Supports MCP 2026-07-28 statelessly |
| `xxhash-rust` | `0.8`, feature `xxh3` | `partition_of` |
| Unchanged | `object_store 0.14.2`, `foyer 0.22`, `slatedb 0.16`, `openraft =0.10.0-alpha.34`, `axum 0.8`, `reqwest 0.12` | Lance 12 is on `object_store` 0.14, so `operon-store` (and `FaultyStore`) are handed to Lance directly |

Build requirements: system `protoc` (installed in CI; Lance's vendored-`protoc` feature is never enabled) and a C compiler. `deny.toml` allows `BSL-1.0` (the Boost licence, via `xxhash-rust`) and `bzip2-1.0.6`; its policy comment names the Business Source License by its SPDX id `BUSL-1.1`. `[profile.dev] debug = "line-tables-only"` keeps the debug target (≈13 GB in the spike) manageable.

## 5. Crate map

New crates (all `0.0.1`, Apache-2.0, workspace lints):

| Crate | Owns | Depends on |
|---|---|---|
| `operon-collection` | Schema, `PrimaryKey`, `DocOp` and its record codec, catalog helpers, `CollectionWriter` (append to the implicit stream), Lance and split writers, manifest codec, `CollectionTarget: LinkTarget`, PK usage, delete bitmaps, index-build tasks, `CollectionGcRoots` | meta, log, store, cache, pk, link, worker, `operon-text` |
| `operon-quickwit` | Vendored Quickwit files (split bundle and footer, hotcache, async storage directories, warmup, ES DSL → Tantivy AST, doc-mapper pieces, `StableLogMergePolicy`) with a small shim, built against crates.io Tantivy; per-file Datadog headers kept, Quickwit's NOTICE text added to ours (dependency spike §d) | tantivy |
| `operon-text` | Tantivy integration: analyzers, split writer/reader over `operon-store` + `operon-cache`, delete-bitmap application, the `Query` → Tantivy query builder, global statistics provider | quickwit, store, cache |
| `operon-query` | Search IR, DataFusion catalog/providers/operators, fusion, tail index, consistency tokens, `CollectionService`, SQL UDTFs, Flight SQL service | collection, text, log, meta |
| `operon-hnsw` (M1.3) | The `HnswIndex` trait and its `qdrant-edge` implementation (build, filtered search, publish and read-only open); the only crate that depends on `qdrant-edge`, behind the binary's `hnsw` feature | — |
| `operon-hot` (M1.3) | Hot artifacts, pinned-object manager, budgets, node registry and rendezvous ownership | collection, query, hnsw, cache |
| `operon-qdrant` (M1.4) | Qdrant REST + gRPC gateway | query |
| `operon-es` (M1.5) | Elasticsearch REST gateway | query, text |
| `operon-mcp` (M1.6) | MCP server | query |

The `operon` binary wires them; every gateway is behind a cargo feature (`qdrant`, `es`, `mcp`, `flight`, `hnsw`), all on by default. Non-Rust code lives in `sdks/python`, `sdks/typescript` (M1.6) and `conformance/` and `bench/` (M1.7).

## 6. Shared contracts

### 6.1 Ids and catalog (M1.1, in `operon-meta` / `operon-common`)

```rust
pub struct CollectionId(pub u64);   // operon-common (`id_type!`); dense, allocated by the state machine (D18)
// New Command variants go at the END of the enum (postcard-encoded Raft entries); snapshot format version 4 → 5 (codec.rs).

Command::CreateCollection { namespace: NamespaceId, name: String, schema: CollectionSchema, partitions: u32 }
    // -> Reply::CollectionCreated { id: CollectionId, stream: StreamId, link: LinkId }
    // Creates, in one command: the collection, its implicit stream `_collection.<name>.<id>` (WAL class standard,
    // `partitions` partitions, no retention: link apply and GC own trimming), and its link (TargetRef { kind: "collection", name })
    // Retry-safe: the same name with an identical schema returns CollectionExists(id); a different schema returns NameTaken.
Command::DropCollection { namespace: NamespaceId, name: String, now_ms: u64 }
    // -> Reply::CollectionDropped(Option<CollectionId>)  (None when absent; retry-safe)
    // Removes the name at once (it may be re-created immediately with a new id), drops the implicit stream and link,
    // and records the collection's prefixes as retired for GC.
Command::UpdateCollectionSchema { collection: CollectionId, expected_version: u64, schema: CollectionSchema }
    // -> Reply::SchemaUpdated { version: u64 }. Additive only (new fields, new vectors); anything else is ApplyError::IncompatibleSchema.
    // VersionMismatch when expected_version is stale; a retry that finds the same schema at expected_version + 1 succeeds.
Command::UpdateAliases { namespace: NamespaceId, actions: Vec<AliasAction> }   // atomic; AliasAction::{Create { alias, collection }, Delete { alias }}
    // -> Reply::AliasesUpdated
```

Queries on `MetaState`: `collection(id)`, `collection_by_name(ns, name)`, `collections(ns)`, `resolve_collection(ns, name_or_alias)`, `aliases(ns)`. Stream names starting with `_` are reserved for implicit streams; `CreateStream` refuses them.

### 6.2 Primary keys, partitioning and the record format (M1.1, `operon-collection`)

```rust
pub enum PrimaryKey { U64(u64), Uuid([u8; 16]), Str(String) }
// Canonical bytes: tag 0x01 + u64 big-endian | tag 0x02 + 16 bytes | tag 0x03 + UTF-8. Ordering of keys = ordering of canonical bytes.
pub fn partition_of(pk: &PrimaryKey, partitions: u32) -> u32;   // xxh3_64(canonical bytes) % partitions  (crate xxhash-rust, feature xxh3)

pub struct Document { pub pk: PrimaryKey, pub source: serde_json::Map<String, serde_json::Value>, pub vectors: BTreeMap<String, Vec<f32>> }
pub enum PatchMode { MergeDeep /* ES partial doc */, MergeTop /* Qdrant set_payload */, Replace /* Qdrant overwrite_payload */ }
pub enum DocOp {
    Upsert(Document),
    Delete(PrimaryKey),
    Patch { pk: PrimaryKey, mode: PatchMode, source: serde_json::Map<String, serde_json::Value>,
            delete_keys: Vec<String> /* JSON paths, dot-separated */, vectors: BTreeMap<String, Option<Vec<f32>>> /* None = delete vector */,
            upsert: Option<Document> /* used when the key does not exist; otherwise a patch of a missing key is a no-op */ },
}
```

One record per op on the implicit stream: Kafka record key = canonical PK bytes; value = `0x01` (codec version) followed by the postcard encoding of `DocOp` with `source` carried as UTF-8 JSON bytes (postcard cannot encode `serde_json::Value`); no headers; timestamp = the writer's clock. Records for one key always go to `partition_of(pk)`, so per-key order is the partition order. Sparse vectors and multivectors are rejected with `InvalidArgument` in M1 (Phase B).

**Atomic writes.** `LogWriter` gains `append_many(stream, Vec<(u32 /* partition */, Vec<Record>)>) -> Result<Vec<AppendAck>, LogError>`, which places every batch in the same WAL object and the same `CommitWal`, so one request is atomic across partitions (§01 §5). A collection write request is one `append_many`.

### 6.3 Schema (M1.1)

```rust
pub struct CollectionSchema {
    pub version: u64,                       // 1 at creation; +1 per UpdateCollectionSchema
    pub fields: Vec<FieldSpec>,             // unique names; order is stable
    pub vectors: Vec<VectorSpec>,           // unique names; "" is Qdrant's unnamed default vector
    pub dynamic: DynamicMapping,            // Strict | Ignore | Map
    pub max_fields: u32,                    // default 1000 (ES index.mapping.total_fields.limit)
}
pub struct FieldSpec { pub name: String /* dot path, e.g. "meta.author" or "title.keyword" */, pub source_path: String /* where the value comes from in _source */,
                       pub kind: FieldKind, pub indexed: bool, pub fast: bool }
pub enum FieldKind { Text { analyzer: String, positions: bool }, Keyword, I64, F64, Bool, Date, Uuid, Json }
pub struct VectorSpec { pub name: String, pub dim: u32, pub distance: Distance, pub element: VectorElement /* F32 in M1 */,
                        pub index: VectorIndexSpec, pub hnsw: HnswParams, pub quantization: Option<Quantization> }
pub enum Distance { Cosine, Dot, Euclid, Manhattan }
```

Every document keeps its `_source` verbatim (a Qdrant payload is its `_source`). Fields are values extracted from `_source` by `source_path` (arrays give multi-valued fields); they are what is indexed, filtered, sorted and aggregated. With `DynamicMapping::Map`, a gateway that sees unmapped paths proposes `UpdateCollectionSchema` with ES's dynamic rules before it appends the write (Ruling R17). With `Ignore` (the Qdrant default), unmapped paths live only in `_source`; Qdrant payload indexes add fields.

### 6.4 Durable layout, manifest and commit (M1.1)

```
ns/<ns>/collections/<cid>/
  lance/…                                                   # the Lance dataset root
  text/splits/<ulid>.split                                  # Tantivy split bundle with hotcache footer
  text/deletes/<split_ulid>/<ulid>.bitmap                   # roaring delete bitmap for one split, whole (not incremental)
  manifests/<version:020>-<ulid>.pb                         # immutable collection manifest
  hot/<kind>/<manifest_version:020>-<ulid>/…               # derived hot artifacts (M1.3)
ns/<ns>/pk/collection-<cid>/                                # PkIndex (SlateDB)
```

The manifest is protobuf (`prost`) inside Operon's standard envelope (magic `OPCM`, format version `1`, crc32c trailer; §03 §6). Fields every plan may rely on:

```text
CollectionManifest {
  version, parent_version, collection_id, schema_version, created_at_ms
  lance_version                                           // the Lance dataset version this manifest reads
  splits:   [SplitRef { ulid, doc_count, deleted_count, size_bytes, footer_range, row_id_ranges: [(start, end)], delete_bitmap: Option<path> }]
  vector_indexes: [VectorIndexRef { column, lance_index_uuid, indexed_row_ids_upto }]
  hot_artifacts:  [HotArtifactRef { kind, column, prefix, source_version }]   // written by M1.3; empty before
  applied:  { partition: next_offset }                    // exactly-once watermark (§09 §3)
  live_doc_count
}
```

Commit (§03 §3.3, exact order): write new Lance data/deletion files and the Lance version → write the new split and the changed delete bitmaps → write the manifest → `cas_pointer(ns, "collection/<cid>", expected = parent, fence = task lease, freshness = the oldest new object's creation time with max age = link `max_commit_delay`)`. Readers load the pointer, then the manifest, and read only the Lance version, splits and bitmaps it names.

### 6.5 Consistency tokens and read consistency (M1.2)

- A write returns `ConsistencyToken(Vec<(StreamId, u32 /* partition */, u64 /* next offset after the write */)>)`. Text form: `v1:` followed by `s<stream>/p<partition>@<offset>` items joined by `,`, e.g. `v1:s7/p3@918274`. HTTP header on every write response and accepted on every read request: `Operon-Consistency-Token`.
- `ReadConsistency::{Strong (default), Eventual, AtLeast(ConsistencyToken)}`. `Strong` reads the implicit stream's high watermarks with a linearizable metastore read at request start and merges the tail up to them; `AtLeast` merges up to the token's offsets (and at least the durable state); `Eventual` reads the durable state and whatever tail is already in memory.

### 6.6 The search IR (M1.2 implements; M1.4, M1.5, M1.6 compile to it)

```rust
pub struct SearchRequest {
    pub collection: String,                 // name or alias
    pub consistency: ReadConsistency,
    pub retrievers: Vec<Retriever>,         // empty: filter-only, ordered by `sort`
    pub fusion: Option<Fusion>,             // required when retrievers.len() > 1
    pub filter: Option<Query>,              // applied to every retriever (non-scoring)
    pub sort: Vec<SortKey>,                 // default: [Score desc]; always ends with the PK ascending as tie-break
    pub offset: usize, pub limit: usize,
    pub search_after: Option<Vec<SortValue>>,
    pub score_threshold: Option<f32>,
    pub select: Projection,                 // source (all | none | include/exclude paths), vectors (names), fields
    pub aggregations: Option<serde_json::Value>,   // Tantivy aggregation request JSON (ES-compatible, as Quickwit maps it)
    pub highlight: Option<Highlight>,
    pub group_by: Option<GroupBy>,          // Qdrant search_groups
    pub track_total_hits: TrackTotalHits,   // None | Exact | UpTo(u64)
}
pub enum Retriever {
    Vector { field: String, query: Vec<f32>, k: usize, params: AnnParams, filter: Option<Query> },
    Text { query: Query, k: usize },
    Fused { inputs: Vec<Retriever>, fusion: Fusion, k: usize },               // Qdrant nested prefetch
    Rescore { input: Box<Retriever>, field: String, query: Vec<f32>, k: usize },  // Qdrant prefetch + query
}
pub struct AnnParams { pub exact: bool, pub nprobes: Option<u32>, pub refine_factor: Option<u32>, pub ef: Option<u32>, pub oversampling: Option<f32> }
pub enum Fusion { Rrf { k: u32 /* default 60 */ }, Dbsf, WeightedSum { weights: Vec<f32> } }
pub enum Query {  // scoring when used by Retriever::Text, a bitmap when used as a filter
    MatchAll, MatchNone,
    Match { field: String, text: String, operator: BoolOperator, minimum_should_match: Option<String>, fuzziness: Option<Fuzziness>, analyzer: Option<String> },
    MatchPhrase { field: String, text: String, slop: u32 },
    MultiMatch { fields: Vec<(String, f32)>, text: String, kind: MultiMatchKind, operator: BoolOperator },
    Term { field: String, value: FieldValue }, Terms { field: String, values: Vec<FieldValue> },
    Range { field: String, gt: Option<FieldValue>, gte: Option<FieldValue>, lt: Option<FieldValue>, lte: Option<FieldValue> },
    Exists { field: String }, IsNull { field: String }, IsEmpty { field: String },
    ValuesCount { field: String, gt: Option<u64>, gte: Option<u64>, lt: Option<u64>, lte: Option<u64> },
    Prefix { field: String, value: String }, Wildcard { field: String, pattern: String },
    Fuzzy { field: String, value: String, fuzziness: Fuzziness },
    Ids(Vec<PrimaryKey>),
    QueryString { query: String, default_fields: Vec<String>, default_operator: BoolOperator },
    Bool { must: Vec<Query>, should: Vec<Query>, must_not: Vec<Query>, filter: Vec<Query>, minimum_should_match: Option<String> },
    Boost { query: Box<Query>, boost: f32 }, ConstantScore { query: Box<Query>, score: f32 },
}
pub struct SearchResponse { pub hits: Vec<Hit>, pub total: Option<TotalHits>, pub aggregations: Option<serde_json::Value>, pub groups: Option<Vec<HitGroup>>, pub read_token: ConsistencyToken }
pub struct Hit { pub pk: PrimaryKey, pub score: f32, pub sort_values: Vec<SortValue>, pub source: Option<serde_json::Map<String, serde_json::Value>>,
                 pub vectors: BTreeMap<String, Vec<f32>>, pub highlight: BTreeMap<String, Vec<String>> }
```

Score convention: larger is better. A vector retriever's score is cosine similarity (Cosine), dot product (Dot), or the negated distance (Euclid, Manhattan); gateways convert to their protocol's convention. Equal scores are ordered by canonical PK bytes ascending, on every path.

### 6.7 `CollectionService` (M1.2, `operon-query`) — the one facade every gateway uses

```rust
impl CollectionService {
    pub async fn create_collection(&self, ns: &str, name: &str, schema: CollectionSchema, partitions: Option<u32>) -> Result<CollectionInfo, ServiceError>;
    pub async fn drop_collection(&self, ns: &str, name: &str) -> Result<bool, ServiceError>;
    pub async fn get_collection(&self, ns: &str, name_or_alias: &str) -> Result<CollectionInfo, ServiceError>;
    pub async fn list_collections(&self, ns: &str) -> Result<Vec<CollectionInfo>, ServiceError>;
    pub async fn add_fields(&self, ns: &str, name: &str, fields: Vec<FieldSpec>, vectors: Vec<VectorSpec>) -> Result<CollectionSchema, ServiceError>;
    pub async fn update_aliases(&self, ns: &str, actions: Vec<AliasAction>) -> Result<(), ServiceError>;
    pub async fn write(&self, ns: &str, name: &str, ops: Vec<DocOp>, opts: WriteOptions /* report_existence */) -> Result<WriteResult /* token, per-op OpResult */, ServiceError>;
    pub async fn get(&self, ns: &str, name: &str, pks: &[PrimaryKey], select: &Projection, consistency: ReadConsistency) -> Result<Vec<Option<StoredDoc>>, ServiceError>;
    pub async fn search(&self, ns: &str, request: SearchRequest) -> Result<SearchResponse, ServiceError>;
    pub async fn count(&self, ns: &str, name: &str, filter: Option<Query>, consistency: ReadConsistency) -> Result<u64, ServiceError>;
    pub async fn scroll(&self, ns: &str, name: &str, filter: Option<Query>, after: Option<PrimaryKey>, limit: usize, select: &Projection, consistency: ReadConsistency) -> Result<(Vec<StoredDoc>, Option<PrimaryKey>), ServiceError>;
    pub async fn versions(&self, ns: &str, name: &str) -> Result<Vec<ManifestInfo>, ServiceError>;   // Qdrant snapshots = manifest versions
    pub fn sql_context(&self, ns: &str) -> datafusion::prelude::SessionContext;
}
pub enum ServiceError { NotFound { kind: &'static str, name: String }, AlreadyExists(String), InvalidArgument(String), SchemaViolation { field: String, message: String }, Unavailable(String) /* retryable */, Timeout, Internal(String) }
```

### 6.8 Native API additions (M1.2; routes follow the M0 style `/v1/namespaces/{ns}/…`, JSON error body unchanged)

`POST /v1/namespaces/{ns}/collections` · `GET|DELETE /v1/namespaces/{ns}/collections/{c}` · `POST /v1/namespaces/{ns}/collections/{c}/documents` (ops) · `POST /v1/namespaces/{ns}/collections/{c}/documents/get` · `POST /v1/namespaces/{ns}/query` (the §05 §4 hybrid request) · `POST /v1/namespaces/{ns}/sql` · M1.3 adds `PUT /v1/namespaces/{ns}/collections/{c}/hot` and `POST /v1/namespaces/{ns}/collections/{c}/warm`. Flight SQL listens on `native.flight_sql` (default `0.0.0.0:8082`).

### 6.9 Gateway namespaces

ES and Qdrant have no namespaces. Each gateway serves one namespace, `default` unless configured (`[gateways.qdrant] namespace = "…"`), created on first use; the `Operon-Namespace` header overrides it per request. Authentication is out of scope for M1 (listeners bind to localhost by default in `operon dev`).

## 7. Cross-cutting rulings

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| R1 | Seven plans (§2); plans after M1.1 start with a reconciliation Task 0 | A plan written against code that does not exist yet drifts; the contracts here are what must not drift | Some rework in later plans' Task 0 |
| R2 | Every collection mutation enters through the implicit stream as `DocOp` records; nothing writes Lance or splits except the collection link target and maintenance tasks | The log is the spine (D4); one path for exactly-once and consistency tokens | Write latency = WAL latency (fine: §02 targets) |
| R3 | PK canonical encoding and `partition_of` are fixed as in §6.2 | They are persisted in records, the PK index and split fields; changing either is a format break | None if kept |
| R4 | `_source` is stored verbatim; typed fields are derived by `source_path` | ES returns `_source` byte-for-byte in spirit and Qdrant returns payloads unchanged; one document model for both | Storage for both `_source` and typed columns |
| R5 | Tantivy is the engine for text scoring, filters and aggregations; Lance stores documents and vectors and runs ANN. A filter reaches ANN as a row-id allow-list (pre-filter) or a post-filter, chosen by estimated selectivity (§06 §4) | Tantivy handles multi-valued fields, JSON paths and ES aggregations natively; one filter evaluator for durable splits and the tail (a RAM Tantivy index) | Filter-heavy vector queries depend on the allow-list hand-off to Lance being cheap; M1.2 measures it |
| R6 | BM25 statistics (doc count, average field length, document frequencies) are global across a query's splits and tail, via Tantivy's statistics provider | ES scores a single-shard index with global statistics; per-split IDF would miss the BEIR gate | One extra statistics pass per query (cached per manifest) |
| R7 | The Operon manifest is the only lineage of the Lance dataset: a collection commit is based exactly on its parent manifest's `lance_version` and can never include fragments from a Lance version no live manifest references (a crashed or fenced writer's). M1.1 fixes the mechanism with the pinned Lance API and proves it with crash and zombie tests | Lance's own commit loop rebases concurrent appends; that would double-apply a zombie's batch | A Lance version is written per commit even if its CAS then fails (GC collects it) |
| R8 | The PK index is derived state: updated after the manifest CAS, carrying its own applied watermark, and repaired from committed objects on task start. Datasets are created with Lance **stable row ids** (`enable_stable_row_ids = true`, fixed at creation); the PK index, Tantivy docs (`_rowid` fast field) and `SplitRef.row_id_ranges` hold row ids, never row addresses | SlateDB is a separate commit and can never be atomic with the pointer CAS. Row addresses change on every Lance compaction (spike §g); row ids survive it, so compaction rewrites neither splits nor the PK index | Task start after a crash pays a repair scan of at most one batch. Stable row ids are marked experimental in Lance 12: M1.1 pins behaviour with tests |
| R9 | One link-apply task per collection (D30 stands). Compaction, split merges and index builds commit through the same pointer CAS and rebase on `Conflict` | One manifest per collection: parallel partition-range tasks would only contend on its CAS | Ingest per collection is bounded by one task; M5 revisits |
| R10 | Deterministic ordering everywhere: score desc, then canonical PK asc | Required by hot on/off identity and by paging (`search_after`, `scroll`) | None |
| R11 | Strong consistency is the default for every read on every surface (§01 §4.2); `eventual` is opt-in | ES `refresh`/Qdrant `wait` semantics become free, and conformance tests that write then read pass | One linearizable metastore read per request |
| R12 | **Hot on/off identity** (§04 §6 rule 1) is enforced exactly for text search, filters, aggregations, fetch, scroll, counts and exact vector search (`AnnParams.exact`, and any ANN whose candidate set falls under the brute-force threshold). Approximate ANN on the hot tier (HNSW) and on the durable tier (Lance IVF) are different approximations, so for them the gate is: every returned score is exact (rescored with full vectors), and Recall@10 against exact search is within the §12 bound on both tiers | Two approximate indexes cannot return identical top-k on every query; pretending otherwise would force brute force | If the user wants bit-identical ANN, hot HNSW must be restricted to exact rescoring of a durable-tier candidate set |
| R13 | The metastore over the network (openraft RPCs over HTTP, a remote `MetaClient`) and `operon --roles …` cluster mode are pulled from M5 into M1.3 | Affinity routing needs more than one process; the in-process `Router` cannot run a real multi-node deployment | M1.3 grows by one task; M5 keeps meta sharding |
| R14 | Qdrant and Elasticsearch servers are used only as external test oracles (Docker images in M1.7); no code, spec tests or resources from Elastic are vendored (Q10 resolved: not in M1) | License policy (D11) | Conformance relies on client-library and framework suites |
| R15 | Every gateway returns its protocol's error body; `ServiceError` maps to one status per variant, fixed in each gateway plan | Clients branch on those errors | — |
| R16 | Dropping a collection frees its name at once; re-creating it gets a new id and new prefixes | ES and Qdrant test suites drop and re-create names back to back | Old objects wait for GC's grace |
| R17 | Dynamic mapping is a schema update proposed by the gateway before it appends the write (`UpdateCollectionSchema`, CAS on the schema version); the link worker never changes the schema | `apply` stays deterministic, and a mapping is visible before any document that needs it | Two racing writers retry on `VersionMismatch` |
| R18 | Lance datasets use file format **2.1** explicitly (`data_storage_version = V2_1`; Lance 12 defaults to 2.2) | Design §03 §3.1 pins 2.1 until 2.2 is evaluated | A later move to 2.2 is per dataset, by compaction (§03 §6) |
| R19 | Lance is always given an explicit commit handler (never the `UnsafeCommitHandler` it silently picks for an unknown URL scheme), auto-cleanup is never enabled, and Operon's GC deletes Lance versions via `cleanup_with_policy(versions(..))` computed from live manifests | R7 and GC own lineage and deletion | — |
| R20 | Q7: **depend on `qdrant-edge =0.8.0`** behind Operon's own `HnswIndex` trait in `operon-hnsw`; do not vendor Qdrant `lib/segment` (≈200k LOC once its imports are followed). Allow-list filters are `has_id` sets; artifacts are built in a local directory, published as files and opened read-only (mmap) on the owning node | Buy over build; the trait keeps a later fork possible | 0.x API churn; heavy dependency tree, isolated by the feature |
| R21 | Quickwit code is vendored file by file into `operon-quickwit` from Quickwit `af0591a3`, adapted to Tantivy 0.26.2 (≈16k LOC + ≈1.2k LOC shim, spike §d); its S3 backend is not taken (our `Storage` impl sits on `operon-store`) | Quickwit's crates are coupled through `quickwit-config`/`-proto`/`-common`; files are not | Re-sync by diff on Tantivy bumps |

## 8. Global Constraints (every M1 task)

- Everything in the M0.3 and M0.4 Global Constraints still holds (toolchain Rust 1.97.1, edition 2024, Apache-2.0, `deny.toml`, `unsafe_code = "forbid"`, fmt/clippy/test/deny after every task, `unwrap()` only in tests, deterministic `apply`, retry-safe commands, magic + version + crc32c on every Operon-defined format, fenced tasks never change durable state, `/tmp` is a small tmpfs: never put a cargo target dir there).
- The hot tier is never a source of truth: deleting every hot artifact, cache file and tail at any moment changes only latency.
- Every freshness deadline (segmenter `swap_deadline`, link `max_commit_delay`, collection commit freshness) must be strictly below GC's `grace`; configuration that violates this is rejected at startup (carried from M0.4 re-review m1).
- Every new object a GC root can reference is named with a ULID (or has a `LastModified`) and is referenced only through a freshness-carrying CAS or command.
- No dependency with AGPL, SSPL, BSL or ELv2 licenses; vendored code keeps its license header and is listed in `NOTICE`.
- Gateways never reach storage directly: they call `CollectionService` (Rule §01 §1.6).
- Commit areas add `collection`, `text`, `query`, `hot`, `qdrant`, `es`, `mcp`, `sdk`, `conformance`, `bench`.

## 9. Carried in from M0

| From | Item | Plan |
|---|---|---|
| M0.4 re-review m1 | Validate that every freshness deadline is below GC's grace; reject violating configs | M1.1 |
| M0.4 re-review m2 | Bound `a_build_without_failpoints_refuses_to_arm_them` with a timeout and a clear failure message | M1.1 |
| M0.4 re-review m3 | Log a stale-object refusal with the proposer's clock lag | M1.1 |
| M0.4 re-review m4 | Collection manifests: ULID path, freshness-carrying CAS | M1.1 (§6.4) |
| M0.4 re-review m5 | Replace the zombie test's 300 ms sleep with an explicit synchronisation point | M1.1 |
| M0.4 review M9 | A link target must not read every data file per snapshot | M1.1 (the collection target reads one manifest) |
| M0 known limitations | Fetches read a whole WAL chunk even for a small range | M1.2 (tail reads use ranged fetches) |
| As built (M0 digest) | `LinkApplySource` always builds a `CounterTable` and only applies links of kind `"counter"`; there is no target registry | M1.1: a `LinkTargetFactory` registry keyed by `TargetRef.kind`; links of an unregistered kind are reported, not silently skipped |
| As built | No GC root covers `ns/<ns>/collections/` or `ns/<ns>/pk/`; `GcConfig` does not validate deadlines | M1.1 (`CollectionGcRoots`; pk prefixes deleted only for dropped collections, SlateDB collects its own files otherwise) |
| As built | `StoreError` has no retryable classification (only the fault-matrix test has one) | M1.1: `StoreError::is_retryable()`, used by the fault matrix and by M1 retry loops |
| As built | The HTTP API maps `VersionMismatch`, `Fenced` and `StaleObject` to 500; produce has no key partitioner | M1.2 (409 for conflicts, 503 for retryable; collection writes partition by `partition_of`) |
| As built | Worker sources cannot be added after `Worker::start()` | M1.1 (collection and index-build sources are registered at start and discover collections from meta) |
| As built | `operon-pk` is unused; its fencing is SlateDB's writer fencing, not the task lease | M1.1: the collection target opens the PK index under its task lease; a fenced SlateDB writer ends the task |

## 10. Open questions touched by M1

| # | Question | Resolution |
|---|---|---|
| Q6 | Lance multivector depth vs hot-tier multivector | Not in M1 (Phase B); multivectors rejected with a clear error |
| Q7 | `qdrant-edge` vs forking `lib/segment` | **Resolved:** depend on `qdrant-edge` (R20) |
| Q10 | Elastic REST YAML spec test license | Not vendored in M1 (R14) |

## Amendments

None yet.
