# M1 — Collections (Elasticsearch + Qdrant): Overview and Shared Contracts

Status: **Planned** (2026-09-24). This document splits milestone M1 into seven plans and fixes the contracts between them. Each plan's tasks implicitly include this document's Global Constraints and must use the contracts here verbatim (§6). If a plan and this document disagree, this document wins until an amendment is recorded in its "Amendments" section.

Design references: [01 Architecture](../design/01-architecture.md), [03 Storage formats](../design/03-storage-formats.md) §3, §5, §7, [04 Hot tier](../design/04-hot-tier.md), [05 Query engine](../design/05-query-engine.md), [06 Search & vector](../design/06-search-and-vector.md), [09 Links & workers](../design/09-links-and-workers.md), [11 Buy vs build](../design/11-buy-vs-build.md), [12 Roadmap](../design/12-roadmap-testing-risks.md) §1 (M1 row), [15 Agent workspaces](../design/15-agent-workspaces.md) §10.1 (W0). Built on M0 as described in the [M0 exit report](m0-exit-report.md).

## 1. Goal

Ship collections: documents with text, keyword, numeric, date, boolean and JSON fields, named dense vectors and named sparse vectors (Qdrant sparse vectors with exact dot-product scoring and the IDF modifier, A26), stored as one Lance dataset plus Tantivy splits under one manifest, written only through the log, read with strong consistency by a DataFusion-based hybrid engine, served through a native API, SQL and Arrow Flight SQL, the Qdrant and Elasticsearch APIs (Phase A), Python and TypeScript SDKs and an MCP server, accelerated by a hot tier that never changes results, and proven by the M1 exit gates.

## 2. Plans

| Plan | Branch | Scope | Depends on |
|---|---|---|---|
| [M1.1: Collection storage](2026-09-24-m1.1-collection-storage.md) | `m1.1-collection-storage` | Collection catalog in meta (collections, implicit streams, aliases, schema evolution, drop); `DocOp` record format (dense and sparse vectors); atomic multi-partition append; Lance dataset + Tantivy split writers (sparse vectors as Lance columns and split postings, A28); collection manifest and fenced, freshness-checked CAS; upserts, patches and deletes through the PK index and delete bitmaps; the `collection` link target; vector and scalar index builds; collection GC roots; carried-in M0 items | M0 |
| [M1.2: Query engine and native API](2026-09-24-m1.2-query-engine.md) | `m1.2-query-engine` | `operon-query`: the search IR, DataFusion catalog and operators (`TantivySearchExec`, `AnnExec`, `SparseExec`, `FilterBitmapExec`, `FusionExec`, `DocFetchExec`, `TailMergeExec`), the tail index, consistency tokens and strong reads, global BM25 statistics, `CollectionService`; native REST collection, document and hybrid query endpoints; SQL with `vector_search`/`text_search`/`hybrid_search`; Arrow Flight SQL | M1.1 |
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
| Sparse vectors (Qdrant; pulled from Phase B by the owner, 2026-09-25, A26–A30, R22) | M1.1 (storage), M1.2 (exact search, IDF), M1.3 (served from pinned splits), M1.4 (Qdrant API), M1.6 (SDK types) |
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
| LangChain + LlamaIndex vector-store tests (ES and Qdrant backends) pass unmodified: each suite runs with its own commands and only its server URL/environment pointed at Operon; every server-backed test is gated, the sparse and hybrid (dense + sparse) retrieval tests included (sparse vectors are in M1, A26); tests hard-wired to Qdrant's in-process `:memory:` mode never reach a server and are not counted (A14) | M1.7 (surface built in M1.4, M1.5) |
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
| `operon-collection` | Schema, `PrimaryKey`, `SparseVector`, `DocOp` and its record codec, catalog helpers, `CollectionWriter` (append to the implicit stream), Lance and split writers, manifest codec, `CollectionTarget: LinkTarget`, PK usage, delete bitmaps, index-build tasks, `CollectionGcRoots` | meta, log, store, cache, pk, link, worker, `operon-text` |
| `operon-quickwit` | Vendored Quickwit files (split bundle and footer, hotcache, async storage directories, warmup, ES DSL → Tantivy AST, doc-mapper pieces, `StableLogMergePolicy`) with a small shim, built against crates.io Tantivy; per-file Datadog headers kept, Quickwit's NOTICE text added to ours (dependency spike §d) | tantivy |
| `operon-text` | Tantivy integration: analyzers, split writer/reader over `operon-store` + `operon-cache`, delete-bitmap application, the `Query` → Tantivy query builder, global statistics provider | quickwit, store, cache |
| `operon-query` | Search IR, DataFusion catalog/providers/operators (sparse search included), fusion, tail index, consistency tokens, `CollectionService`, SQL UDTFs, Flight SQL service | collection, text, log, meta |
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
// The schema types of §6.3 live in `operon_common::schema` (operon-meta carries them in commands); operon-collection re-exports them (A17).
// Collection names: at most 222 bytes (A17).
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
    // ApplyError::SchemaVersionMismatch { collection, current } when expected_version is stale (A16); a retry that finds the same schema at expected_version + 1 succeeds.
Command::UpdateAliases { namespace: NamespaceId, actions: Vec<AliasAction> }   // atomic; AliasAction::{Create { alias, collection }, Delete { alias }}
    // -> Reply::AliasesUpdated
```

Queries on `MetaState`: `collection(id)`, `collection_by_name(ns, name)`, `collections(ns)`, `resolve_collection(ns, name_or_alias)`, `aliases(ns)`. Stream names starting with `_` are reserved for implicit streams; `CreateStream` refuses them.

### 6.2 Primary keys, partitioning and the record format (M1.1, `operon-collection`)

```rust
pub enum PrimaryKey { U64(u64), Uuid([u8; 16]), Str(String) }
// Canonical bytes: tag 0x01 + u64 big-endian | tag 0x02 + 16 bytes | tag 0x03 + UTF-8. Ordering of keys = ordering of canonical bytes.
pub fn partition_of(pk: &PrimaryKey, partitions: u32) -> u32;   // xxh3_64(canonical bytes) % partitions  (crate xxhash-rust, feature xxh3)

/// A sparse vector in canonical form (A27): indices strictly ascending, one finite value per index (zeros allowed).
/// Fields are private; every constructor, `Deserialize` included (`#[serde(try_from = "RawSparseVector")]`), goes through `new`.
/// JSON form: {"indices": [u32], "values": [f32]}.
pub struct SparseVector { indices: Vec<u32>, values: Vec<f32> }
impl SparseVector {
    pub fn new(indices: Vec<u32>, values: Vec<f32>) -> Result<Self, SparseVectorError>;   // sorts by index; unequal lengths, a duplicate index or a non-finite value is an error
    pub fn indices(&self) -> &[u32]; pub fn values(&self) -> &[f32]; pub fn len(&self) -> usize; pub fn is_empty(&self) -> bool;
}
pub struct Document { pub pk: PrimaryKey, pub source: serde_json::Map<String, serde_json::Value>, pub vectors: BTreeMap<String, Vec<f32>>,
                      pub sparse_vectors: BTreeMap<String, SparseVector> /* A27 */ }
pub enum PatchMode { MergeDeep /* ES partial doc */, MergeTop /* Qdrant set_payload */, Replace /* Qdrant overwrite_payload */ }
pub enum DocOp {
    Upsert(Document),
    Delete(PrimaryKey),
    Patch { pk: PrimaryKey, mode: PatchMode, source: serde_json::Map<String, serde_json::Value>,
            delete_keys: Vec<String> /* JSON paths, dot-separated */, vectors: BTreeMap<String, Option<Vec<f32>>> /* None = delete vector */,
            sparse_vectors: BTreeMap<String, Option<SparseVector>> /* None = delete sparse vector (A27) */,
            upsert: Option<Document> /* used when the key does not exist; otherwise a patch of a missing key changes nothing at apply time,
                                       and CollectionService reports it as OpResult::NotFound (A24) */ },
}
```

One record per op on the implicit stream: Kafka record key = canonical PK bytes; value = `0x01` (codec version) followed by the postcard encoding of `DocOp` with `source` carried as UTF-8 JSON bytes (postcard cannot encode `serde_json::Value`); no headers; timestamp = the writer's clock. Records for one key always go to `partition_of(pk)`, so per-key order is the partition order. Sparse vectors travel in `Document.sparse_vectors` and `Patch.sparse_vectors` inside the same codec version `0x01` body (A27; no record has been written yet, so this is not a format change). Multivectors are rejected with `InvalidArgument` in M1 (Phase B).

**Atomic writes.** `LogWriter` gains `append_many(stream, Vec<(u32 /* partition */, Vec<Record>)>) -> Result<Vec<AppendAck>, LogError>`, which places every batch in the same WAL object and the same `CommitWal`, so one request is atomic across partitions (§01 §5). A collection write request is one `append_many`.

### 6.3 Schema (M1.1)

```rust
pub struct CollectionSchema {
    pub version: u64,                       // 1 at creation; +1 per UpdateCollectionSchema
    pub fields: Vec<FieldSpec>,             // unique names; order is stable
    pub vectors: Vec<VectorSpec>,           // unique names; "" is Qdrant's unnamed default vector
    pub sparse_vectors: Vec<SparseVectorSpec>, // unique non-empty names, disjoint from `vectors`; fixed at creation in M1 (A26)
    pub dynamic: DynamicMapping,            // Strict | Ignore | Map
    pub max_fields: u32,                    // default 1000 (ES index.mapping.total_fields.limit)
    pub annotations: BTreeMap<String, String>,  // opaque gateway data, keys namespaced `es.*` / `qdrant.*`; preserved (A1)
}
pub struct FieldSpec { pub name: String /* dot path, e.g. "meta.author" or "title.keyword" */, pub source_path: String /* where the value comes from in _source */,
                       pub kind: FieldKind, pub indexed: bool, pub fast: bool,
                       pub ignore_malformed: bool /* a value of the wrong type is skipped, not a violation (A2) */ }
pub enum FieldKind { Text { analyzer: String, positions: bool }, Keyword, I64, F64, Bool, Date, Uuid, Json }
pub struct VectorSpec { pub name: String, pub dim: u32, pub distance: Distance, pub element: VectorElement /* F32 in M1 */,
                        pub index: VectorIndexSpec, pub hnsw: HnswParams, pub quantization: Option<Quantization> }
pub enum Distance { Cosine, Dot, Euclid, Manhattan }
pub struct SparseVectorSpec { pub name: String, pub modifier: SparseModifier }   // A26
pub enum SparseModifier { None, Idf }                                           // Qdrant's `modifier`; Idf reweights the query at search time
```

Every document keeps its `_source` verbatim (a Qdrant payload is its `_source`). Fields are values extracted from `_source` by `source_path` (arrays give multi-valued fields); they are what is indexed, filtered, sorted and aggregated. With `DynamicMapping::Map`, a gateway that sees unmapped paths proposes `UpdateCollectionSchema` with ES's dynamic rules before it appends the write (Ruling R17). With `Ignore` (the Qdrant default), unmapped paths live only in `_source`.

**JSON fields (A3).** A `FieldKind::Json` field with `source_path: ""` indexes the whole `_source`. `Query` field names address paths inside a Json field as `"<field>.<path>"` (dot-separated, arrays flattened, type-strict: a numeric range matches only numeric leaves). String leaves are indexed raw (exact match, fast) and, in a companion Tantivy field defined by M1.1, tokenized with the `standard` analyzer, so `Match`/`MatchPhrase` work on JSON paths. Every Qdrant collection has a Json field named `payload` with `source_path: ""`, and Qdrant filters always address `payload.<key>`; Qdrant payload indexes are recorded in `annotations`.

**No backfill (A4).** Fields added by `UpdateCollectionSchema` apply to documents written after the schema version that added them (ES put-mapping semantics). Each `SplitRef` records the `schema_version` it was written with.

**Sparse vectors (A26–A30, R22).** A sparse vector field holds, per document, at most one `SparseVector` (absent is allowed; an empty vector is stored and read back but is never a search candidate and never counts in statistics, as in Qdrant, `qdrant-edge-0.8.0/src/segment/index/sparse_index/sparse_vector_index/vector_index_impl.rs:177-190`). Search is exact (§6.6 `Retriever::Sparse`). `check_additive` requires `next.sparse_vectors == old.sparse_vectors`: sparse vectors are declared when the collection is created, and adding one later is Phase B. ES `sparse_vector` stays refused (M1.5 Ruling 13): its string-keyed token weights need a token → index dictionary and Lucene stores them at reduced precision, so it is not free on top of this.

**Analyzers (A5, M1.1 in `operon-text`).** `standard` (ES standard: UAX #29 word segmentation + lowercase, no stop words, max token length 255), `english` (Lucene's EnglishAnalyzer chain: standard tokenizer → English possessive filter → lowercase → Lucene English stop words → the original Porter stemmer, not Porter2/Snowball), `simple`, `whitespace`, `keyword`. The BEIR gate depends on `english` matching Lucene.

### 6.4 Durable layout, manifest and commit (M1.1)

```
ns/<ns>/collections/<cid>/
  lance/…                                                   # the Lance dataset root
  text/splits/<ulid>.split                                  # Tantivy split bundle with hotcache footer
  text/deletes/<split_ulid>/<ulid>.bitmap                   # roaring delete bitmap for one split, whole (not incremental)
  manifests/<version:020>-<ulid>.pb                         # immutable collection manifest
  pkdelta/<version:020>-<ulid>.pkd                          # the keys one commit changed; PK-index repair (R8)
  deadletters/<version:020>-<ulid>.dlq                      # records one commit dead-lettered
  hot/hnsw/<column>/<source_version:020>-<ulid>/{descriptor.bin,covered.bin,files/…}  # derived HNSW artifacts (M1.3)
ns/<ns>/pk/collection-<cid>/                                # PkIndex (SlateDB)
```

Sparse vectors add no object kind (A28): each sparse field is a Lance column `_sparse_<j>` (`Struct<indices: List<UInt32>, values: List<Float32>>`, nullable) and two hidden fields inside every Tantivy split, `_sparse.<name>` (the postings: one u64 term per index, plus `u64::MAX` for every non-empty vector) and `_sparse_w.<name>` (a bytes fast field with the vector), so split writes, merges, delete bitmaps, pinned splits and the tail cover them with no new mechanism.

The manifest is protobuf (`prost`) inside Operon's standard envelope (magic `OPCM`, format version `1`, crc32c trailer; §03 §6). Fields every plan may rely on:

```text
CollectionManifest {
  version, parent_version, collection_id, schema_version, created_at_ms
  lance_version                                           // the detached Lance version id this manifest reads (R7)
  splits:   [SplitRef { ulid, doc_count, deleted_count, size_bytes, footer_range, row_id_ranges: [(start, end)], delete_bitmap: Option<path>, schema_version }]
  vector_indexes: [VectorIndexRef { column, lance_index_uuid, indexed_row_ids_upto }]
  hot_artifacts:  [HotArtifactRef { kind, column, prefix, source_version }]   // written by M1.3; empty before
  applied:  { partition: next_offset }                    // exactly-once watermark (§09 §3)
  live_doc_count
}
```

Commit (§03 §3.3, exact order): write new Lance data/deletion files and the Lance version → write the new split and the changed delete bitmaps → write the manifest → `cas_pointer(ns, "collection/<cid>", expected = parent, fence = task lease, freshness = the oldest new object's creation time with max age = link `max_commit_delay`)`. Readers load the pointer, then the manifest, and read only the Lance version, splits and bitmaps it names.

**Retention for pinned reads (A6).** GC keeps every manifest superseded less than the collection's time-travel retention ago (A21: measured from the child manifest's `created_at_ms`, so a pin on a long-lived manifest does not expire at once) (default 24 h, §03 §7) plus the last `keep_manifests`, with everything they reference, and an implicit stream is trimmed only below the `applied` offsets of the oldest retained manifest. So a `Pinned` read (§6.5) stays valid for as long as its manifest is retained, with no metastore hold.

### 6.5 Consistency tokens and read consistency (M1.2)

- A write returns `ConsistencyToken(Vec<(StreamId, u32 /* partition */, u64 /* next offset after the write */)>)`. Text form: `v1:` followed by `s<stream>/p<partition>@<offset>` items joined by `,`, e.g. `v1:s7/p3@918274`. HTTP header on every write response and accepted on every read request: `Operon-Consistency-Token`.
- `ReadConsistency::{Strong (default), Eventual, AtLeast(ConsistencyToken), Pinned { manifest_version: u64, token: ConsistencyToken }}`. `Pinned` (A6; ES point in time, Qdrant snapshot reads) reads the durable state at `manifest_version` plus the tail from that manifest's `applied` offsets up to `token`, and fails with `NotFound` once the manifest is no longer retained (§6.4). `Strong` reads the implicit stream's high watermarks with a linearizable metastore read at request start and merges the tail up to them; `AtLeast` merges up to the token's offsets (and at least the durable state); `Eventual` reads the durable state and whatever tail is already in memory.

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
    Sparse { field: String, query: SparseVector, k: usize, filter: Option<Query>, params: SparseParams },   // A29; exact
}
#[derive(Default)] pub struct SparseParams { pub idf_corpus: Option<Query> /* IDF statistics over the docs matching this query (Qdrant `params.idf.corpus`); Idf fields only */ }
pub struct AnnParams { pub exact: bool, pub nprobes: Option<u32>, pub refine_factor: Option<u32>, pub ef: Option<u32>, pub oversampling: Option<f32>,
                      pub distance: Option<Distance> /* metric override, exact search only (ES script_score) (A7) */ }
pub enum Fusion { Rrf { k: u32 /* default 60 */ }, Dbsf, WeightedSum { weights: Vec<f32> } }
// Rrf: score(d) = Σ_lists 1 / (k + rank(d)), rank 1-based (ES convention; Qdrant's 0-based k=2 is sent as k = 1).
// Dbsf: Qdrant's distribution-based score fusion: each list's scores normalised with Qdrant's `distr_norm`, (s − (μ − 3σ)) / 6σ with f32 Welford mean and sample σ (a one-hit list or σ = 0 scores 0.5), not clamped, then summed (A20).
// WeightedSum: Σ weight_i · score_i over the lists a document appears in (ES query + knn semantics, boosts as weights).
pub enum Query {  // scoring when used by Retriever::Text, a bitmap when used as a filter
    MatchAll, MatchNone,
    Match { field: String, text: String, operator: BoolOperator, minimum_should_match: Option<String>, fuzziness: Option<Fuzziness>, analyzer: Option<String> },
    MatchPhrase { field: String, text: String, slop: u32 },
    MultiMatch { fields: Vec<(String, f32)>, text: String, kind: MultiMatchKind, operator: BoolOperator, tie_breaker: Option<f32> /* A8 */ },
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
                 pub vectors: BTreeMap<String, Vec<f32>>, pub sparse_vectors: BTreeMap<String, SparseVector> /* A29 */, pub highlight: BTreeMap<String, Vec<String>> }
```

**`Retriever::Sparse` semantics (A29, R22).** Candidates are the live documents of the read view that pass `request.filter ∧ filter` and share at least one index with `query` (a stored zero weight counts). The score is `Σ q'ᵢ · wᵢ` over the shared indices in ascending index order, accumulated in f32 (Qdrant's `score_vectors`, `qdrant-edge-0.8.0/src/sparse/common/sparse_vector.rs:66-90`), where `q'ᵢ = qᵢ` for `SparseModifier::None` and `q'ᵢ = qᵢ · idfᵢ` for `Idf`, with `idfᵢ = ln((N − dfᵢ + 0.5) / (dfᵢ + 0.5) + 1)` in f32 (Qdrant's `fancy_idf`, `qdrant-edge-0.8.0/src/segment/data_types/query_context.rs:276-299`). `N` is the number of live documents of the view (durable minus deleted and shadowed rows, plus the live tail) with a non-empty vector in the field, and `dfᵢ` the number of those containing index *i*; with `params.idf_corpus` both count only documents matching it. The top k are ordered by score descending, then PK (R10). An empty query returns no hits. `idf_corpus` on a `None` field is `InvalidArgument`. A sparse retriever may appear at the top level, inside `Fused` (Qdrant hybrid prefetch + RRF/DBSF) and as the input of `Rescore`; `Rescore`'s own query stays dense in M1.

`R4` exception (A9): a gateway may move mapped vector values out of `_source` into `Document.vectors` on write and restore them into `_source` on read (ES `dense_vector` fields).

Score convention: larger is better. A vector retriever's score is cosine similarity (Cosine), dot product (Dot), or the negated distance (Euclid, Manhattan); a sparse retriever's is the (IDF-weighted) dot product; gateways convert to their protocol's convention. Equal scores are ordered by canonical PK bytes ascending, on every path.

### 6.7 `CollectionService` (M1.2, `operon-query`) — the one facade every gateway uses

```rust
impl CollectionService {
    pub async fn ensure_namespace(&self, ns: &str) -> Result<(), ServiceError>;    // creates the namespace if absent (§6.9) (A10)
    pub async fn create_collection(&self, ns: &str, name: &str, schema: CollectionSchema, partitions: Option<u32>) -> Result<CollectionInfo, ServiceError>;  // creates a missing namespace
    pub async fn drop_collection(&self, ns: &str, name: &str) -> Result<bool, ServiceError>;
    pub async fn get_collection(&self, ns: &str, name_or_alias: &str) -> Result<CollectionInfo, ServiceError>;
    pub async fn list_collections(&self, ns: &str) -> Result<Vec<CollectionInfo>, ServiceError>;
    pub async fn add_fields(&self, ns: &str, name: &str, fields: Vec<FieldSpec>, vectors: Vec<VectorSpec>, annotations: BTreeMap<String, String>) -> Result<CollectionSchema, ServiceError>;  // no backfill (A4)
    pub async fn update_aliases(&self, ns: &str, actions: Vec<AliasAction>) -> Result<(), ServiceError>;
    pub async fn write(&self, ns: &str, name: &str, ops: Vec<DocOp>, opts: WriteOptions /* report_existence, atomic (A24) */) -> Result<WriteResult /* token, per-op OpResult and position (A24) */, ServiceError>;
    pub async fn get(&self, ns: &str, name: &str, pks: &[PrimaryKey], select: &Projection, consistency: ReadConsistency) -> Result<Vec<Option<StoredDoc>>, ServiceError>;
    pub async fn search(&self, ns: &str, request: SearchRequest) -> Result<SearchResponse, ServiceError>;
    pub async fn count(&self, ns: &str, name: &str, filter: Option<Query>, consistency: ReadConsistency) -> Result<u64, ServiceError>;
    pub async fn scroll(&self, ns: &str, name: &str, filter: Option<Query>, after: Option<PrimaryKey>, limit: usize, select: &Projection, consistency: ReadConsistency) -> Result<(Vec<StoredDoc>, Option<PrimaryKey>), ServiceError>;
    pub async fn versions(&self, ns: &str, name: &str) -> Result<Vec<ManifestInfo>, ServiceError>;   // Qdrant snapshots = manifest versions
    pub fn sql_context(&self, ns: &str) -> datafusion::prelude::SessionContext;
}
// StoredDoc carries `seq_no: u64`: the partition offset of the record that last wrote the document (ES `_seq_no`) (A11),
// and `sparse_vectors: BTreeMap<String, SparseVector>` (A30). `Projection.vectors` names dense or sparse vectors; each name is
// resolved against both lists of the schema. `add_fields` is unchanged: it adds fields and dense vectors only (sparse vectors are fixed at creation, A26).
pub enum ServiceError { NotFound { kind: &'static str, name: String }, AlreadyExists(String), InvalidArgument(String), SchemaViolation { field: String, message: String }, Unavailable(String) /* retryable */, Timeout, Internal(String) }
```

### 6.8 Native API additions (M1.2; routes follow the M0 style `/v1/namespaces/{ns}/…`, JSON error body unchanged)

`POST|GET /v1/namespaces/{ns}/collections` (create, list) · `GET|DELETE /v1/namespaces/{ns}/collections/{c}` · `POST /v1/namespaces/{ns}/collections/{c}/documents` (ops) · `POST /v1/namespaces/{ns}/collections/{c}/documents/get` · `POST /v1/namespaces/{ns}/query` (the §05 §4 hybrid request) · `POST /v1/namespaces/{ns}/sql` · M1.3 adds `PUT /v1/namespaces/{ns}/collections/{c}/hot` and `POST /v1/namespaces/{ns}/collections/{c}/warm`. Flight SQL listens on `native.flight_sql` (default `0.0.0.0:8082`). M1.2 also serves `…/collections/{c}/fields`, `…/versions`, `/v1/namespaces/{ns}/aliases`, `…/documents/scroll` and `…/documents/count`; `operon dev` binds Flight SQL on `127.0.0.1:8082` (A22). `PrimaryKey` in JSON: integer → `U64`, string → `Str`, `{"uuid": "…"}` → `Uuid` (A12). Sparse vectors need no new route: the IR's JSON carries `{"sparse": {...}}` retrievers, document ops carry `sparse_vectors`, and a schema carries `sparse_vectors` (A29, A30).

**Hot-tier controls (A13, A23).** M1.2 owns the per-request switch (`operon_query::hot::HotLayer`: the request header and metadata, `Operon-Hot-Used`) and the `--hot` flag; M1.3 owns the hot tier behind it, `PUT …/hot`, `POST …/warm`, the full hot status and `--hot-pin-all`. Every read listener (native REST with `/mcp`, Flight SQL, Qdrant REST and gRPC, Elasticsearch) is wrapped in `HotLayer`, and gRPC responses carry `operon-hot-used` as response metadata (A23). Request header `Operon-Hot: on|off` (gRPC metadata `operon-hot`) disables every hot structure for one request; responses carry `Operon-Hot-Used: <comma-separated subset of hnsw, splits; or none>` (fragment prefetch only fills the H1 cache and is never reported or bypassed); the server flag `--hot=on|off` sets the default and `--hot-pin-all` pins every collection (gates and benchmarks). `PUT …/hot` takes `{vectors, text, fragments}`. `GET …/collections/{c}` reports `manifest_version`, `link_lag_records` and the hot status per structure.

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
| R7 | The Operon manifest is the only lineage of the Lance dataset: a collection commit is based exactly on its parent manifest's `lance_version` and can never include fragments from a Lance version no live manifest references (a crashed or fenced writer's). **Mechanism (M1.1 Ruling 1): Lance detached versions.** The mainline holds only the empty version 1; every later commit is `CommitBuilder::with_detached(true)` on the dataset checked out at the parent's `lance_version`, through `LanceCommitter::commit`, the only code allowed to commit to Lance. No code path may call a Lance API that commits to the mainline (`append`, `delete`, `create_index`, `optimize_indices`, `commit_compaction`, `cleanup_*`) | Lance's own commit loop rebases concurrent appends; that would double-apply a zombie's batch | A Lance version is written per commit even if its CAS then fails (GC collects it) |
| R8 | The PK index is derived state: updated after the manifest CAS, carrying its own applied watermark, and repaired from committed objects on task start. Datasets are created with Lance **stable row ids** (`enable_stable_row_ids = true`, fixed at creation); the PK index, Tantivy docs (`_rowid` fast field) and `SplitRef.row_id_ranges` hold row ids, never row addresses | SlateDB is a separate commit and can never be atomic with the pointer CAS. Row addresses change on every Lance compaction (spike §g); row ids survive it, so compaction rewrites neither splits nor the PK index | Task start after a crash pays a repair scan of at most one batch. Stable row ids are marked experimental in Lance 12: M1.1 pins behaviour with tests |
| R9 | One link-apply task per collection (D30 stands). Compaction, split merges and index builds commit through the same pointer CAS and rebase on `Conflict` | One manifest per collection: parallel partition-range tasks would only contend on its CAS | Ingest per collection is bounded by one task; M5 revisits |
| R10 | Deterministic ordering everywhere: score desc, then canonical PK asc | Required by hot on/off identity and by paging (`search_after`, `scroll`) | None |
| R11 | Strong consistency is the default for every read on every surface (§01 §4.2); `eventual` is opt-in | ES `refresh`/Qdrant `wait` semantics become free, and conformance tests that write then read pass | One linearizable metastore read per request |
| R12 | **Hot on/off identity** (§04 §6 rule 1) is enforced exactly for text search, sparse vector search (A29), filters, aggregations, fetch, scroll, counts and exact vector search (`AnnParams.exact`, and any ANN whose candidate set falls under the brute-force threshold). Approximate ANN on the hot tier (HNSW) and on the durable tier (Lance IVF) are different approximations, so for them the gate is: every returned score is exact (rescored with full vectors), and Recall@10 against exact search is within the §12 bound on both tiers | Two approximate indexes cannot return identical top-k on every query; pretending otherwise would force brute force | If the user wants bit-identical ANN, hot HNSW must be restricted to exact rescoring of a durable-tier candidate set |
| R13 | The metastore over the network (openraft RPCs over HTTP; non-meta nodes run a non-voting learner replica, and `MetaClient` forwards writes and read-index requests to the leader) and `operon --roles …` cluster mode are pulled from M5 into M1.3 | Affinity routing needs more than one process; the in-process `Router` cannot run a real multi-node deployment | M1.3 grows by one task; M5 keeps meta sharding |
| R14 | Qdrant and Elasticsearch servers are used only as external test oracles (Docker images in M1.7); no code, spec tests or resources from Elastic are vendored (Q10 resolved: not in M1) | License policy (D11) | Conformance relies on client-library and framework suites |
| R15 | Every gateway returns its protocol's error body; `ServiceError` maps to one status per variant, fixed in each gateway plan | Clients branch on those errors | — |
| R16 | Dropping a collection frees its name at once; re-creating it gets a new id and new prefixes | ES and Qdrant test suites drop and re-create names back to back | Old objects wait for GC's grace |
| R17 | Dynamic mapping is a schema update proposed by the gateway before it appends the write (`UpdateCollectionSchema`, CAS on the schema version); the link worker never changes the schema | `apply` stays deterministic, and a mapping is visible before any document that needs it | Two racing writers retry on `SchemaVersionMismatch` |
| R18 | Lance datasets use file format **2.1** explicitly (`data_storage_version = V2_1`; Lance 12 defaults to 2.2) | Design §03 §3.1 pins 2.1 until 2.2 is evaluated | A later move to 2.2 is per dataset, by compaction (§03 §6) |
| R19 | Lance is always given an explicit commit handler (never the `UnsafeCommitHandler` it silently picks for an unknown URL scheme), auto-cleanup is never enabled, and Lance cleanup is never run: it sees only mainline manifests and would delete detached versions' files (A15). Operon's GC computes Lance reachability from retained collection manifests | R7 and GC own lineage and deletion | — |
| R20 | Q7: **depend on `qdrant-edge =0.8.0`** behind Operon's own `HnswIndex` trait in `operon-hnsw`; do not vendor Qdrant `lib/segment` (≈200k LOC once its imports are followed). Allow-list filters are `has_id` sets; artifacts are built in a local directory, published as files and opened read-only (mmap) on the owning node | Buy over build; the trait keeps a later fork possible | 0.x API churn; heavy dependency tree, isolated by the feature |
| R21 | Quickwit code is vendored file by file into `operon-quickwit` from Quickwit `af0591a3`, adapted to Tantivy 0.26.2 (≈16k LOC + ≈1.2k LOC shim, spike §d); its S3 backend is not taken (our `Storage` impl sits on `operon-store`) | Quickwit's crates are coupled through `quickwit-config`/`-proto`/`-common`; files are not | Re-sync by diff on Tantivy bumps |
| R22 | **The M1 sparse index rides in the Tantivy split, and scoring is exact** (owner decision 2026-09-25, A26–A30). Each sparse field is stored as a Lance column (the source of truth, read by fetch and by split merges) and, inside each split and the tail's RAM index, as a u64 postings field (one term per index) plus a bytes fast field holding the vector. `SparseExec` (M1.2) unions the postings of the query's indices, masks deleted, shadowed and filtered docs, reads each candidate's vector and scores it exactly; IDF statistics are counted over the same live postings. Sparse fields get no hot artifact: pinned splits serve them (M1.3). Not taken: qdrant-edge's sparse inverted index is private (`mod sparse;`, `qdrant-edge-0.8.0/src/lib.rs:13`; only `SparseVector` is re-exported, `edge/reexports.rs:63`) and reachable only through a whole `EdgeShard` on a local directory, so it cannot be the durable, object-storage-backed index, and a hot-only copy would still need this path; Qdrant's standalone `lib/sparse` (≈4.4k LOC at v1.19.1, Apache-2.0) is not published, depends on Qdrant's `common`, `blobstore` and mmap files, and would be a fork. The §06 §6 posting-file index with f16 weights, block-max metadata and MAXSCORE stays Phase B | Buy over build: Tantivy already gives postings, split bundling, hotcache, warmup, delete bitmaps, merges, the RAM tail and pinned splits, so sparse vectors need no new format, commit step, GC root or hot artifact, and exact scoring is the same computation hot or cold (R12) | Query cost is O(Σ posting lengths of the query's indices + candidates × nnz) with no pruning; a very common index makes most documents candidates. The Phase B index replaces the two split fields; splits are re-derived from Lance by merges (M1.3 Ruling 4), so the migration rewrites splits only |

## 8. Global Constraints (every M1 task)

- Everything in the M0.3 and M0.4 Global Constraints still holds (toolchain Rust 1.97.1, edition 2024, Apache-2.0, `deny.toml`, `unsafe_code = "forbid"`, fmt/clippy/test/deny after every task, `unwrap()` only in tests, deterministic `apply`, retry-safe commands, magic + version + crc32c on every Operon-defined format, fenced tasks never change durable state, `/tmp` is a small tmpfs: never put a cargo target dir there).
- The hot tier is never a source of truth: deleting every hot artifact, cache file and tail at any moment changes only latency.
- Every freshness deadline (segmenter `swap_deadline`, link `max_commit_delay`, collection commit freshness) must be strictly below GC's `grace`; configuration that violates this is rejected at startup (carried from M0.4 re-review m1).
- Every new object a GC root can reference is named with a ULID (or has a `LastModified`) and is referenced only through a freshness-carrying CAS or command.
- No dependency with AGPL, SSPL, BSL or ELv2 licenses; vendored code keeps its license header and is listed in `NOTICE`.
- Gateways never reach storage directly: they call `CollectionService` (Rule §01 §1.6).
- Commit areas add `common`, `quickwit`, `collection`, `text`, `query`, `hot`, `qdrant`, `es`, `mcp`, `sdk`, `conformance`, `bench`, `hnsw` (A25).

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
| Q1 | Final project name ("Operon" is the working name) | **Resolved 2026-09-25 (owner):** renamed Loam, package name `loamdb` on PyPI, npm and crates.io, after M1 completes. M1 keeps the working names (`operon-*` crates, `operon-client`, `@operon/client`) and publishes nothing; one mechanical rename PR follows M1 (A32) |
| Q6 | Lance multivector depth vs hot-tier multivector | Not in M1 (Phase B); multivectors rejected with a clear error. Sparse vectors are in M1 (A26) |
| Q7 | `qdrant-edge` vs forking `lib/segment` | **Resolved:** depend on `qdrant-edge` (R20) |
| Q10 | Elastic REST YAML spec test license | Not vendored in M1 (R14) |

## Amendments

Adopted 2026-09-25 from the M1.4, M1.5, M1.6 and M1.7 plans (their proposals are recorded there); already reflected in the text above. A23–A25 come from the cross-plan consistency review of the seven plans (2026-09-25). A26–A32 record the owner decisions of 2026-09-25 (sparse vectors in M1; the elasticsearch-py wipe endpoints; the package names).

| # | Change | From |
|---|---|---|
| A1 | `CollectionSchema.annotations` and the `annotations` argument of `add_fields` | M1.4 A3, M1.5 A1 |
| A2 | `FieldSpec.ignore_malformed` | M1.4 A2 |
| A3 | JSON fields addressable by path; the Qdrant `payload` catch-all field | M1.4 A1 |
| A4 | Schema additions are not backfilled; `SplitRef.schema_version` | Orchestrator (replaces M1.4 A5's "add_fields waits for indexing") |
| A5 | Analyzer definitions, `english` = Lucene chain with Porter | M1.7 amendment 2 |
| A6 | `ReadConsistency::Pinned` without a metastore hold, backed by manifest retention and trimming below the oldest retained manifest | Orchestrator (replaces M1.5 A3's metastore hold) |
| A7 | `AnnParams.distance` | M1.5 A5 |
| A8 | `MultiMatch.tie_breaker` | M1.7 amendment 2 |
| A9 | R4 exception for vectors in `_source` | M1.5 A4 |
| A10 | `ensure_namespace`; `create_collection` creates a missing namespace | M1.6 A2, M1.4 A5 |
| A11 | `StoredDoc.seq_no` | M1.5 A2 |
| A12 | `GET` collections list route; `PrimaryKey` JSON form | M1.6 A1, A3 |
| A13 | Hot-tier request header, response header, flags and status fields | M1.7 amendment 1 |
| A14 | Exit-gate wording for `:memory:` tests (never counted). **Decided 2026-09-25 (owner): sparse vectors are pulled into M1**, so the sparse and hybrid tests are gated like every other server-backed test and the earlier "run and reported but not gated" wording is withdrawn (A26–A30) | M1.4 A4, M1.7 amendment 5; owner decision 2026-09-25 |
| A15 | R19: Lance cleanup is never run (detached versions) | M1.1 A1 |
| A16 | `ApplyError::SchemaVersionMismatch` | M1.1 A2 |
| A17 | Schema types in `operon_common::schema`; `VectorIndexSpec`, `HnswParams`, `Quantization` defined by M1.1; collection names ≤ 222 bytes | M1.1 A5 |
| A18 | Layout: `pkdelta/` and `deadletters/`; `lance_version` is a detached id; R7's mechanism. Vector indexes grow by delta index segments with periodic full rebuilds (`optimize_indices` commits to the mainline and is not used) | M1.1 A3, A4 |
| A19 | M1.3: HNSW artifact path, `Operon-Hot-Used` values (`hnsw`, `splits`), `PUT …/hot` body, learner replicas on non-meta nodes (R13), commit area `hnsw` | M1.3 amendments 1–4 |
| A20 | DBSF exactly as Qdrant's `distr_norm` (no clamping) | M1.2 A19 |
| A21 | Manifest retention measured from supersession, not creation | M1.2 A20 |
| A22 | Extra native routes; Flight SQL bind address in `operon dev` | M1.2 A21 |
| A23 | §6.8: the per-request hot switch, `Operon-Hot-Used` and `--hot` are M1.2's (`HotLayer`, M1.2 Ruling 11); M1.3 owns the tier, the hot routes, the status and `--hot-pin-all`; every read listener, the Qdrant and ES gateways included, is wrapped in `HotLayer`; on gRPC `operon-hot-used` is response metadata, not a trailer | Consistency review (M1.2 Task 1 and M1.3 Task 8 both built on this split; the §6.8 text named M1.3 only) |
| A24 | §6.2/§6.7: `WriteOptions { report_existence, atomic }`, `WriteResult { token, results, positions }`, `OpResult` is an enum; a patch of a missing key without `upsert` changes nothing at apply time and is reported as `OpResult::NotFound` at request time | Consistency review (M1.2 Rulings 10, 16 are the producer's definition) |
| A25 | §8: commit areas `common` (M1.1's `operon_common::schema`) and `quickwit` (`operon-quickwit`) | Consistency review (M1.1 uses both) |
| A26 | §1, §3, §6.3: sparse vectors are in M1. `CollectionSchema.sparse_vectors: Vec<SparseVectorSpec>`, `SparseVectorSpec { name, modifier }`, `SparseModifier { None, Idf }`; names unique, non-empty and disjoint from `vectors`; fixed at creation (`check_additive` refuses any change); the exit gate counts the sparse and hybrid suite tests | Owner decision 2026-09-25 (M1.1 Task 3) |
| A27 | §6.2: `SparseVector` (canonical, private fields, `new` validates and sorts), `Document.sparse_vectors`, `DocOp::Patch.sparse_vectors` (`None` deletes); codec `0x01` body carries them | Owner decision 2026-09-25 (M1.1 Tasks 5–6) |
| A28 | §6.4: Lance column `_sparse_<j>` per sparse field; split fields `_sparse.<name>` (u64 postings + `u64::MAX` presence term) and `_sparse_w.<name>` (bytes fast); no new object kind, commit step or hot artifact | Owner decision 2026-09-25 (M1.1 Tasks 7–8, R22) |
| A29 | §6.6: `Retriever::Sparse { field, query, k, filter, params }`, `SparseParams { idf_corpus }`, `Hit.sparse_vectors`; exact scoring and live-only IDF statistics as specified under "`Retriever::Sparse` semantics"; R12's exact class includes sparse search | Owner decision 2026-09-25 (M1.2 Tasks 1, 6, 7) |
| A30 | §6.7, §6.8: `StoredDoc.sparse_vectors`; `Projection.vectors` resolves dense and sparse names; `add_fields` unchanged; the native JSON forms gain the sparse keys, no new routes | Owner decision 2026-09-25 (M1.2 Tasks 1, 9, 11; M1.6) |
| A31 | Scope: M1.5 serves the endpoints elasticsearch-py's `wipe_cluster` calls (M1.5 Task 3 rule 7), and M1.7 gates the `elasticsearch-py` client suite; wildcard and `_all` index deletes are served when `destructive_requires_name` is false, the gateway's default (M1.5 Ruling 21) | Owner decision 2026-09-25 (replaces M1.7 amendment 3's "not in M1.5") |
| A32 | §10 Q1: the product is renamed Loam (`loamdb` on PyPI, npm, crates.io) after M1; M1 keeps the working names and publishes nothing (M1.6 Ruling 16) | Owner decision 2026-09-25 |
