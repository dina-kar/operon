# M1 Dependency Spike Report

Date: 2026-09-24. Toolchain: rustc/cargo 1.97.1, edition 2024, resolver 3.
Spike crate: `target/m1-spike` (standalone `[workspace]`, git-ignored via `**/target/`), built with `CARGO_TARGET_DIR=target/m1-spike-target`. Both directories were deleted afterwards. No tracked file was changed and nothing was committed. `git status` still shows the untracked `docs/plans/m1-overview.md` and `website/`; this spike did not create them.
Registry source paths below are relative to `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/`.

**Result:** the version set in (a) resolves, compiles, and **runs**. `src/main.rs` exercised each crate:
- Lance: write through our own `object_store` 0.14 store, stage the data and commit once, an upsert-style `Update` commit with a deletion file, `take_rows`, BTREE/BITMAP/IVF_PQ/IVF_RQ indexes, kNN with a prefilter and an external row mask, checkout of v1, and cleanup.
- Tantivy: a RAM index plus a query.
- qdrant-edge: HNSW build, `has_id` filtered search, reload, and a read-only mmap follower over a copied directory.
- DataFusion, arrow-flight (flight-sql), tonic, roaring, foyer, slatedb, openraft and rmcp compiled and were type-checked.

---

## (a) Verified version table (these compiled and ran together)

| Crate | Version (resolved) | Features | License | Why |
|---|---|---|---|---|
| `lance` | `=12.0.0` | `default-features = false` (the defaults pull opendal and aws-sdk: aws, azure, gcp, oss, huggingface, tencent, tos, goosefs, geo) | Apache-2.0 | Collection storage and IVF/scalar indexes. Latest on crates.io (2026-09-17). Upstream main is `13.0.0-beta.14` and is still DataFusion 54 / arrow 58. |
| `lance-index`, `lance-linalg`, `lance-table`, `lance-io`, `lance-file` | `=12.0.0` | `lance-io` with `default-features = false` | Apache-2.0 | `IndexType`, `ScalarIndexParams`, `MetricType`, `ConditionalPutCommitHandler`, `ObjectStoreProvider` and `LanceFileVersion` are not all re-exported by `lance` |
| `datafusion` | `54.1.0` (**not 55.x**) | default | Apache-2.0 | Must equal Lance's (`^54.0.0`). DF 55 uses arrow 59, which is incompatible with Lance. |
| `arrow` / `arrow-array` / `arrow-schema` | `58.4.0` (**not 59/60**) | default | Apache-2.0 | Lance wants `^58.0.0` and DF 54.1 wants `^58.3.0` |
| `arrow-flight` | `58.4.0` | `flight-sql-experimental` (58.x also has a `flight-sql` alias) | Apache-2.0 | Flight SQL server; uses tonic/prost 0.14 |
| `tantivy` | `0.26.2` | default | MIT | Latest (2026-09-08) |
| `qdrant-edge` | `=0.8.0` | (no features; everything is unconditional) | Apache-2.0 | Hot-tier HNSW; see (e) |
| `roaring` | `0.11.5` | default | MIT OR Apache-2.0 | Same major as Lance and qdrant-edge (`^0.11.4`), so one copy in the tree |
| `tonic` / `tonic-build`(`tonic-prost-build`) / `prost` | `0.14.6` / `0.14.6` / `0.14.4` | — | MIT / MIT / Apache-2.0 | One version across arrow-flight, qdrant-edge, lance and qdrant-client |
| `object_store` | `0.14.2` | `aws, gcp, azure, fs` | MIT/Apache-2.0 | Unchanged. Lance 12 uses 0.14 (`^0.14.1`). |
| `foyer` | `0.22.6` | default | Apache-2.0 | Unchanged |
| `slatedb` | `0.16.0` | `default-features = false` | Apache-2.0 | Unchanged (object_store `^0.14.0`) |
| `openraft` | `=0.10.0-alpha.34` | `default-features=false, serde, tokio-rt` | MIT/Apache-2.0 | Unchanged |
| `rmcp` | `3.4.1` | `server, transport-streamable-http-server` | Apache-2.0 | MCP; see (f) |
| `axum` / `tokio` / `reqwest` | `0.8.9` / `1.53.1` / `0.12.28` | as the workspace | MIT | Unchanged |
| `datafusion-distributed` | `3.0.0` (fits; 4.0 does **not**) | — | Apache-2.0 | 4.0 needs DF 55 / arrow 59. 3.0.0 needs DF `^54`, arrow/arrow-flight `^58`, object_store `^0.13`. Not added for M1. |
| `qdrant-client` (optional, for generated gRPC stubs) | `1.19.0` | — | Apache-2.0 | Prebuilt tonic 0.14.6 client and server stubs; see (e) |

**Build facts** (clean debug, 14 cores, `cargo build`):
- Wall time: 5 m 52 s for all dependencies (1,656 s of CPU), plus about 3 min for the first link of `main.rs`. Later incremental builds took about 1 min, dominated by linking.
- Size: the target dir was about 13 GB. The debug binary was **3.0 GB**.
- Lockfile: 824 packages, against 419 in today's workspace (+405). qdrant-edge alone adds about 171 packages (824 against 653 without it).
- Release build time and size were not measured (unverified).
- Build requirements:
  - System `protoc` (6 lance crates run `prost-build`). Lance's `protoc` feature would vendor `protobuf-src` instead, which is a C++/cmake build, so avoid it.
  - A C compiler: aws-lc-sys, zstd-sys and lz4-sys are already in the workspace; qdrant-edge adds small `cc`-built SIMD C files (`cpp/quantization/{sse,avx2,neon}.c`).
  - No C++. No RocksDB.

**cargo-deny** (0.20.2, repo `deny.toml`, run against the spike manifest):
- `bans ok`, `sources ok`, `advisories ok`.
- `licenses FAILED` on two licenses, both permissive:
  - `BSL-1.0` (Boost Software License, **not** the Business Source License): `xxhash-rust` 0.8.18, via `lance-encoding` and via qdrant-edge's `ph`.
  - `bzip2-1.0.6`: `libbz2-rs-sys` 0.2.5, via `bzip2` 0.6, `async-compression` and `datafusion-datasource` 54.1.
  - Ruling: add both to `[licenses].allow` with a comment. The "BSL" in the deny.toml policy comment means Business Source License 1.1, whose SPDX id is `BUSL-1.1`. Boost's SPDX id is `BSL-1.0`.

**Duplicate majors that matter:**
- `object_store` 0.13.2 (pulled by DataFusion 54 only) alongside 0.14.2 (Lance, SlateDB, ours).
- `reqwest` 0.12 (ours, lance-namespace client) alongside 0.13 (object_store 0.14).
- `rand` 0.7/0.9/0.10.
- `sysinfo` 0.35 (slatedb) alongside 0.39 (qdrant-edge).
- Everything else is the usual small-crate duplication. `multiple-versions = "allow"`, so none of this fails the check.

## (b) Lance and object_store: how to hand Lance our own store

- **Lance 12 is on `object_store` 0.14**, the same as ours (lance, lance-core, lance-io, lance-table, lance-file and lance-index all use `^0.14.1`). So `operon-store` and `FaultyStore` can be passed straight in. Only DataFusion 54 (and 55) stays on 0.13. That affects us only if we use DataFusion's own `ObjectStoreRegistry` (ListingTable/Parquet). Lance does its own I/O and never uses it.
- **API (verified, and it ran):**
  - Implement `lance_io::object_store::ObjectStoreProvider` (`lance-io-12.0.0/src/object_store/providers.rs:42`, `async fn new_store(&self, base: Url, params: &ObjectStoreParams) -> lance::Result<lance::io::ObjectStore>`; also override `extract_path` and `calculate_object_store_prefix`).
  - Build the store with `lance::io::ObjectStore::new(inner: Arc<dyn object_store::ObjectStore /*0.14*/>, location: Url, block_size, wrapper: Option<Arc<dyn WrappingObjectStore>>, use_constant_size_upload_parts, list_is_lexically_ordered, io_parallelism, download_retry_count, storage_options)` (`lance-io-12.0.0/src/object_store.rs:1766`). Its struct fields are partly private (`:203-207`), so you cannot build a struct literal.
  - Register it:
    ```rust
    let reg = Arc::new(ObjectStoreRegistry::default()); // or ::empty() (providers.rs:132)
    reg.insert("operon", Arc::new(OperonProvider{..}));  // providers.rs:407
    let session = Arc::new(Session::new(idx_bytes, meta_bytes, reg)); // lance-12.0.0/src/session.rs:116
    ```
  - Pass `session` in `WriteParams { session: Some(..) }` (`dataset/write.rs:627`) and to `DatasetBuilder::with_session` (`dataset/builder.rs:527`) or `CommitBuilder::with_session` (`dataset/write/commit.rs:169`).
- **Commit handler.** For an unknown URL scheme, Lance silently falls back to `UnsafeCommitHandler` (`lance-table-12.0.0/src/io/commit.rs:1278`). Always pass `commit_handler: Some(Arc::new(ConditionalPutCommitHandler))` (`commit.rs:1629`; it needs `PutMode::Create` support in our store, which our S3 CAS already relies on). Alternatively, implement `ExternalManifestStore` (`commit/external_manifest.rs:163`) and wrap it in `ExternalManifestCommitHandler { external_manifest_store }` (`:563`), which puts the "latest Lance version" pointer in Operon meta.
- **Alternatives:**
  - `ObjectStoreParams { object_store_wrapper: Some(Arc<dyn WrappingObjectStore>) }` (`object_store.rs:352`, trait at `:269`, `wrap(&self, prefix, Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore>`) wraps whatever store Lance built. It is good for metering or fault injection over Lance's own S3 client. A fault-injecting wrapper must return `None` from `wrap_paginated` (doc at `:274-297`).
  - `ObjectStoreParams.object_store: Option<(Arc<DynObjectStore>, Url)>` (`:346`) and `DatasetBuilder::with_object_store` (`builder.rs:281`) still work but are `#[deprecated(note = "Implement an ObjectStoreProvider instead")]`.
- **Cache hook:** `Session::with_index_cache_backend(Arc<dyn lance_core::cache::CacheBackend>, ..)` (`session.rs:138`; trait at `lance-core-12.0.0/src/cache/backend.rs:55`) lets the Lance index cache sit on foyer (unverified beyond the signature).

## (c) DataFusion and arrow agreement

**DataFusion 54.1.0 + arrow 58.4.0 + arrow-flight 58.4.0 (+ datafusion-distributed 3.0.0 if needed later).**
- Lance 12.0.0 pins `datafusion ^54.0.0` and `arrow ^58.0.0`. Lance main is still on 54/58.
- DataFusion 55.x uses arrow 59, and arrow 60 is out, so the design doc's "55.x" is **not reachable** until Lance moves.
- Pin the arrow and datafusion families in `[workspace.dependencies]` to `58` and `54.1`, and let Lance bumps drive upgrades.

## (d) Tantivy pin and Quickwit vendoring

**Pin `tantivy = "=0.26.2"`** (crates.io, MIT, latest).

**Quickwit uses a tantivy fork rev.**
- Quickwit main is at `af0591a36e16831af9a2ad9484e12465f511ef84` (2026-09-24). The latest release is `v0.9.1` (`962685f5`, 2026-09-22), which pins tantivy rev `057458b`.
- Main pins `tantivy = { git = "https://github.com/quickwit-oss/tantivy/", rev = "e229de6db748c6b21747a99a18220446e76104e1" }`. That rev's Cargo.toml says `version = "0.27.0"`, and it adds the `jitexpr` feature.
- The rev is 271 commits past the point where 0.26.x split from main (`3abc137b`, 2026-03-31): +19.9k/−3.7k lines, mostly in aggregation, query and indexer.

**Licensing.**
- Apache-2.0. Only `quickwit-actors` is MIT, and we don't need it.
- Our NOTICE must carry `quickwit/NOTICE`: "Datadog Quickwit / Copyright 2021-Present Datadog, Inc. / This product includes software developed at Datadog".
- Keep the per-file `// Copyright 2021-Present Datadog, Inc.` headers.

**Compile check.** The Quickwit agent ran `cargo check` on `quickwit-storage`, `-directories`, `-query` and `-doc-mapper` with the dependency switched to crates.io tantivy `=0.26.2`. It passed after about 12 small edits:
- Drop `query_ast/calc_field_query.rs`, which uses `tantivy::jitexpr` and `DocPredicateQuery`.
- Drop `BucketResult::MultiTerms` in `aggregations.rs`.
- Remove the `FieldType::Custom` / `Type::Custom` / `OwnedValue::Custom` match arms: `range_query.rs:198`, `utils.rs:190`, `field_mapping_entry.rs:775`, `field_mapping_type.rs:142`, `tantivy_val_to_json.rs:218`.
- Change `set_fast(x)` to `set_fast(Some(x))` at `field_mapping_entry.rs:532,650`.
- Remove the `jitexpr::ast::infer_types` call in `query_builder.rs:129`.
- In `hot_directory.rs:462`, replace `IndexMeta::list_segment_files()` with `segments.iter().flat_map(|s| s.list_files())`.
- `quickwit-storage` needed no edits.

**Internal-crate coupling.** The coupling comes from Quickwit's own crates, not from tantivy: `quickwit-storage` → `quickwit-config` → `quickwit-doc-mapper` → `quickwit-query`, plus `quickwit-proto` (38k LOC, needs protoc) and `quickwit-common` (tonic, prometheus, opentelemetry, pnet). So **vendor by file**, with a shim of about 1.2k LOC covering `Uri`, `PathHasher`, the constants, the small config enums, `DocMappingUid` and `serde_multikey`. Vendor `quickwit-datetime` whole (870 LOC).

**Vendoring list.** All paths are under `quickwit/`. LOC is non-test.

| Piece | Paths | LOC | Gives us | Builds on 0.26.2 |
|---|---|---|---|---|
| Storage trait + RAM/local/prefix | `quickwit-storage/src/{storage,error,payload,ram_storage,local_file_storage,prefix_storage,stable_deref_bytes}.rs` | ~1,450 | async `Storage` (`get_slice`/`get_all`/`put`/`delete` → `OwnedBytes`) | yes. Better: write an `operon-store`-backed `Storage` impl instead of taking the S3 backend. The S3 backend is `object_storage/s3_compatible_storage.rs` (1,246 LOC); it uses aws-sdk-s3 **directly**, not object_store. Skip it. |
| Split bundle + footer | `quickwit-storage/src/{bundle_storage,split,versioned_component}.rs`, `quickwit-directories/src/bundle_directory.rs` | ~1,200 | `SplitPayloadBuilder` (writer), `BundleStorage`/`BundleFileRanges`/`BundleDirectory` (reader), 16-byte trailer `footer_start u64 LE, version u32=1, b"QWFT"` | yes |
| Hotcache | `quickwit-directories/src/hot_directory.rs` | 537 | `write_hotcache`, `HotDirectory`, `StaticDirectoryCache` (open a split in one GET) | yes, with 1 edit |
| Async directories + byte-range cache | `quickwit-directories/src/{caching_directory,storage_directory,union_directory,lib}.rs`, `quickwit-storage/src/cache/{byte_range_cache,slice_address,stored_item}.rs` | ~900 | tantivy `Directory` over async storage | yes |
| Warmup (required with `StorageDirectory`, whose sync `read_bytes` errors) | `quickwit-search/src/leaf.rs` lines 164–600 (`open_split_bundle`, `open_index_with_caches`, `warmup`, `warm_up_*`) | ~440 | async pre-fetch of term dicts, postings, fast fields and fieldnorms before sync search | the warm APIs exist in 0.26.2. Drop jitexpr lines 195–211. Isolated compile (unverified) |
| ES DSL → tantivy | `quickwit-query/src/{elastic_query_dsl/,query_ast/,tokenizers/,json_literal.rs,lib.rs,error.rs,not_nan_f32.rs}` | ~6,800 | 16 ES query types (bool, term(s), match*, multi_match, range, exists, prefix, wildcard, regexp, query_string, match_all/none), `BuildTantivyAst` | with edits. **Missing for M1 scope:** `fuzzy`, `ids`, `knn` (we add them) |
| Doc mapper | `quickwit-doc-mapper/src/{doc_mapper/,doc_mapping.rs,query_builder.rs,error.rs,lib.rs}` (skip `routing_expression/`, `tag_pruning.rs`) | ~4,700 | ES-style mapping → tantivy schema, doc ↔ JSON, `build_query` | with edits |
| ES aggregations | none: ES `aggs` JSON is deserialized straight into `tantivy::aggregation::agg_req::Aggregations`, and `AggregationResults` serializes to ES-shaped JSON (`quickwit-serve/src/elasticsearch_api/rest_handler.rs:522`, `quickwit-search/src/collector.rs:1042`). Cross-split merge glue: `quickwit-search/src/collector.rs` 870–910 | ~50 | terms/histogram/date_histogram/range/stats/percentiles/cardinality/top_hits | yes, minus `PruneMode`/`prune_intermediate_results` (not in 0.26.2; pruning behaviour may differ, unverified) |
| StableLogMergePolicy | `quickwit-indexing/src/merge_policy/{stable_log_merge_policy,mod}.rs`, config struct in `quickwit-config/src/merge_policy_config.rs` | ~580 | split-merge planner (pure logic over split metadata) | likely yes (only `TrackedObject`); not compiled on its own (unverified) |
| `bitpacking` | crates.io `0.9.3` (MIT) | — | used only by `query_ast/cache_node.rs` | depend on it; don't vendor |

**Total vendored:** about 16k LOC plus a ~1.2k LOC shim (non-test, excluding S3).

**Ruling:** vendor from `af0591a3` into one `operon-search-vendor` crate against tantivy 0.26.2, and re-sync by diff when we bump tantivy. Splits written by the fork rev are not needed: we write our own splits with 0.26.2. Whether fork-written splits are readable by 0.26.2 is unverified and doesn't matter to us.

## (e) qdrant-edge compared with vendoring `lib/segment`

**Verdict: depend on `qdrant-edge = "=0.8.0"` behind an `operon-hnsw` trait. Do not vendor `lib/segment`.**

**What qdrant-edge is:**
- It is an amalgamation of qdrant's segment, shard, quantization, common, wal, sparse and related crates, and corresponds to Qdrant v1.19.0 (inferred by diffing; the current Qdrant release is v1.19.1).
- Only the high-level `edge` API is public (`qdrant-edge-0.8.0/src/lib.rs:4-14`). `HNSWIndex`, `GraphLayers` and `VectorStorage` are private.

**Why not vendor `lib/segment`:**
- The HNSW code depends on `FilteredScorer`, `RawScorer`, `vector_storage`, `id_tracker` and `payload_index`, through about 1,100 `use crate::` imports.
- A usable vendor is about 200k LOC, which is what qdrant-edge already packages.
- Trimming to about 10–15k LOC (graph_layers, graph_links, point_scorer, spaces) means rewriting the scorer and filter layer: a fork, not a vendor.

**Concrete API (verified in the spike unless marked otherwise):**
- **Config:**
  ```rust
  EdgeConfigBuilder::new()
      .vector("v", EdgeVectorParamsBuilder::new(dim, Distance::Cosine)
          .quantization_config(..).build())
      .hnsw_config(HnswIndexConfig{ m, ef_construct, full_scan_threshold, max_indexing_threads, payload_m, .. })
      .optimizers(EdgeOptimizersConfig{ indexing_threshold: Some(kb), .. })
      .build()
  ```
- **Build:**
  - `EdgeShard::new(dir, cfg)` (`src/edge/edge_shard/mod.rs:55`)
  - `shard.update(UpdateOperation::PointOperation(PointOperations::UpsertPoints(PointInsertOperations::PointsList(Vec<PointStructPersisted{ id: PointId::NumId(u64), vector, payload }>))))` (`edge_shard/update.rs:14`)
  - Optional payload indexes for filterable-HNSW links: `UpdateOperation::FieldIndexOperation(..)`
  - `shard.optimize()` (`optimize.rs:28`, synchronous; it builds HNSW once a segment exceeds `indexing_threshold`, default 10,000 KB), then `shard.flush()` (`mod.rs:254`).
- **Search:**
  - `shard.search(SearchRequestBuilder::new(QueryEnum::Nearest(NamedQuery{query, using: Some("v")}), k).filter(Filter).params(SearchParams{hnsw_ef, quantization: Some(QuantizationSearchParams{rescore, oversampling, ..}), ..}).build())` (`edge_shard/shard_read.rs:47`)
  - `search` is deprecated in favour of `query(QueryRequest)` (`:51`).
- **Filters:**
  - Allow-list: `Filter{ must: [Condition::HasId(HasIdCondition{ has_id: AHashSet<PointId> })] }`, for example from a roaring bitmap translated to ids. Verified via JSON `{"must":[{"has_id":[..]}]}`.
  - Payload `FieldCondition` filters are supported.
  - There is **no arbitrary closure or bitmap filter**: `CustomIdCheckerCondition` is private (`segment/types.rs:4163`).
  - ACORN is available (`SearchParams.acorn`).
- **Persist / load:**
  - Storage is a local directory only (mmap). There is no load-from-bytes and no pluggable object-store backend.
  - The layout is `edge_config.json`, `wal/` (two preallocated 32 MiB files), and `segments/<uuid>/{segment.json, vector_storage-*/, vector_index-*/{graph.bin,links_compressed.bin,hnsw_config.json}, payload_*/, id_tracker.*}`.
  - `EdgeShard::load(dir, None)` (`mod.rs:110`) needs write access and takes an flock on the WAL.
  - **Read-only hot-tier load works (verified end to end):**
    1. Copy only the indexed segment dirs to a fresh directory.
    2. Write `segments_manifest.json` = `serde_json::to_vec(&SegmentsManifest::default().set(uuid, SegmentManifestState::Active))`. The leader does not write this file, because the `write_segment_manifest` flag is private and defaults to false.
    3. Call `ReadOnlyEdgeShard::open_mmap(dir)` (`read_only/lifecycle.rs:19`) and `EdgeShardRead::search`. This returned the same hits as the writer.
  - That is our "publish `hot/hnsw/<version>/` → materialize to NVMe → open" path.
- **Threading:**
  - The API is synchronous; call it from `spawn_blocking`.
  - Each shard owns a rayon pool ("edge-search", sized by `max_search_threads`), and HNSW build and quantization create their own rayon pools.
  - There are global `OnceLock` feature flags. It needs `tokio` "full" but creates no runtime.
- **Weight:**
  - About 171 extra packages. Everything is compiled unconditionally: tonic, hyper, rustls and ring (used only for `tonic::Status`), charabia with jieba-rs (embedded dictionaries), vaporetto (an 818 KB model via `include_bytes!`, MIT/Apache), geo, sysinfo 0.39, and on Linux `io-uring`, `procfs`, `thread-priority` and `cgroups-rs`, which pulls **zbus (D-Bus)**. It also pulls `docopt` and `env_logger`.
  - Licenses are all permissive: `self_cell` and `r-efi` are dual-licensed with a permissive option, and cargo-deny passed apart from the `BSL-1.0` xxhash dependency shared with Lance.
  - No RocksDB and no C++. No build-time downloads.
  - It is not Linux-only: fallbacks are cfg-gated and Qdrant CI checks macOS and Windows.
- **Risks:** the 0.x API churns (7 releases in 6 months), every vector is written twice (WAL plus segment) at build time, and there is no custom filter closure. Wrap it behind our own trait so a later fork stays possible.

**Qdrant gRPC/REST definitions:**
- Protos are in `lib/api/src/grpc/proto/*.proto` in qdrant/qdrant. There are 17 files. The public ones are `qdrant.proto`, `points.proto`, `points_service.proto`, `collections.proto`, `collections_service.proto`, `qdrant_common.proto`, `json_with_int.proto`, `snapshots_service.proto` and `health_check.proto`; the rest are internal.
- The files have no per-file headers; the repo is Apache-2.0 and `lib/api/Cargo.toml` says `license = "Apache-2.0"`.
- OpenAPI: `docs/redoc/master/openapi.json` (versioned copies in `docs/redoc/v1.19.x/`).
- Choice: generate with `tonic-build` 0.14 from vendored protos, or depend on `qdrant-client` 1.19.0, whose prebuilt `src/qdrant.rs` includes server traits such as `points_server::Points` and `collections_server` on tonic 0.14.6. Vendoring the 9 public protos (with a NOTICE entry) keeps the tree lighter.

## (f) rmcp

**Use `rmcp = { version = "3.4.1", features = ["server", "transport-streamable-http-server"] }`.**
- Apache-2.0, MSRV 1.88, released 2026-09-23. It is the official SDK (modelcontextprotocol/rust-sdk).
- **It supports 2026-07-28 statelessly:**
  - `ProtocolVersion::V_2026_07_28` is at `rmcp-3.4.1/src/model.rs:170` and is in `KNOWN_VERSIONS` (`:181-187`). `LATEST` is still `V_2025_11_25` (`:175`).
  - `service.rs:204-214` treats 2026-07-28 as "no initialize handshake; per-request metadata", and `handler/server.rs:58-100` does per-request version negotiation.
- **Streamable HTTP server config:**
  - Use `StreamableHttpServerConfig::default().with_legacy_session_mode(false).with_json_response(true)` (`transport/streamable_http_server/tower.rs:78-192`). With that, every POST is served one-shot, with no `Mcp-Session-Id` (`:1953-2030`).
  - For 2026-07-28 requests, stateless serving happens regardless of the setting (`:86-89`).
  - To serve strictly stateless, set `stateless_protocol_metadata_required = true` (`:176`) and override `supported_protocol_versions` to `[V_2026_07_28]`.
  - The default `allowed_hosts` is loopback only; set it for deployment.
- **Mounting:** `StreamableHttpService::new(factory, Arc::new(NeverSessionManager::default()), cfg)` (`:1134`) is a `Clone` tower `Service<http::Request<B>>` with `Error = Infallible`. Mount it with `axum::Router::new().nest_service("/mcp", svc)`, as rmcp's own tests do with axum 0.8.
- **Dependency note:** rmcp's `reqwest` 0.13 dependency is client-only and optional, so it is not pulled with server features.
- pyo3 and maturin are not needed.

## (g) Lance 12.0.0 API facts for M1 (checked in the source; items marked (ran) were exercised in the spike)

File paths are under `lance-12.0.0/src/` unless another crate is named.

**Format version.**
- The **default file format is 2.2, not 2.1**: `LanceFileVersion::V2_2` is `#[default]` and `stable_file_version()` returns V2_2 (`lance-file-12.0.0/src/version.rs:20-21, 44-45`).
- To stay on 2.1 as the design says, set `WriteParams.data_storage_version: Some(LanceFileVersion::V2_1)` (`dataset/write.rs:612`) (ran).

**Stage data, then commit once.**
- `InsertBuilder::new(uri).with_params(&wp).execute_uncommitted(Vec<RecordBatch>) -> Transaction` (`dataset/write/insert.rs:133`; the stream variant is at `:182`), then `CommitBuilder::new(uri | Arc<Dataset>).with_session(..).with_commit_handler(..).with_skip_auto_cleanup(true).execute(txn)` (`dataset/write/commit.rs:280`) (ran).
- Lower level: `FileFragment::create(uri, id, impl StreamingWriteSource, Option<WriteParams>) -> Fragment` (`dataset/fragment.rs:763`) and `create_fragments` (`:779`) write data files with no manifest. `lance::dataset::write_fragments` is also exported. Append fragments get final ids at commit (`lance-table-12.0.0/src/transaction/operation.rs:40-42`) (ran).
- Build the transaction with `TransactionBuilder::new(read_version, Operation).build()` (`lance-table/src/transaction/builder.rs:38`).
- `CommitBuilder::execute_batch` merges several transactions into one version, but **only Append** (`commit.rs:548-560`).

**Upsert batch in one version.**
- `Operation::Update { new_fragments, updated_fragments /* with new deletion files */, removed_fragment_ids, fields_modified: vec![], .. }` (`operation.rs:153-175`) (ran; 2048 − 4 + 256 = 2300 rows).
- `Operation::Delete { updated_fragments, deleted_fragment_ids, predicate }` (`:46`) is the delete-only equivalent.

**Deletes.**
- By predicate:
  - `Dataset::delete(&mut self, pred)` commits (`dataset.rs:2000`).
  - `DeleteBuilder::new(Arc<Dataset>, pred).execute_uncommitted() -> UncommittedDelete { transaction, affected_rows, num_deleted_rows }` (`dataset/write/delete.rs:40, 202`) stages it; commit it with `CommitBuilder::with_affected_rows`.
- By row offset (what the PK index gives us):
  - `FileFragment::extend_deletions(self, impl IntoIterator<Item=u32>) -> Option<FileFragment>` (`fragment.rs:2641`; `None` means the fragment is fully deleted, so put its id in `removed_fragment_ids`) (ran).
  - The by-predicate form is `FileFragment::delete(self, pred)` (`:2570`).
  - The internal `apply_deletions(RoaringTreemap)` is private (`delete.rs:52`).
- What is written: one new immutable object per touched fragment, at `_deletions/{fragment_id}-{read_version}-{random_u64}.{arrow|bin}`.
  - Sparse sets are written as an Arrow IPC file (ZSTD, a single `u32` column).
  - Dense sets are written as a serialized roaring bitmap (`.bin`) (`lance-table-12.0.0/src/io/deletion.rs:37-47, 65-125`).
  - Each deletion file holds the fragment's **full** vector: the old one is merged in and rewritten.

**Read by address or id.**
- Row address = `(fragment_id << 32) | offset`.
- `Dataset::take_rows(&[u64], projection)` takes **row ids** (`dataset.rs:1759`). With stable row ids off, row id == row address (ran).
- By address explicitly: `TakeBuilder::try_new_from_addresses(Arc<Dataset>, Vec<u64>, Arc<ProjectionPlan>)` (`dataset/take.rs:512`).
- `Dataset::take(&[u64])` takes dataset-global row *offsets* (`dataset.rs:1717`).
- Per fragment: `FileFragment::take(&[u32], &Schema)` (`fragment.rs:1664`).

**Stable row ids.**
- `WriteParams.enable_stable_row_ids` (`write.rs:618`, "Experimental") is fixed at dataset creation. Migrate later with `Dataset::migrate_to_stable_row_ids` (`dataset.rs:3226`).
- They survive compaction but not updates (`write.rs:614-617`).
- Without them, compaction rewrites addresses and remaps indexes (`optimize.rs:838`).
- **Implication:** the design's "Tantivy doc stores the Lance row address" breaks on every Lance compaction unless (i) our split merge re-maps addresses, or (ii) we enable stable row ids and store `_rowid`. We must choose one in M1.

**Indexes** (`DatasetIndexExt`, `index/api.rs:190`; `create_index(&mut self, cols, IndexType, name, &dyn IndexParams, replace)` at `index.rs:1689`; builder form at `index.rs:1679`) (ran unless marked):
- Vector:
  - IVF_PQ: `VectorIndexParams::ivf_pq(n_parts, 8, n_subvec, MetricType::L2, max_iters)` (`index/vector.rs:323`)
  - IVF_RQ: `ivf_rq(n_parts, num_bits, DistanceType)` (`:350`) or `with_ivf_rq_params` (`:408`)
  - IVF_HNSW_SQ: `with_ivf_hnsw_sq_params(metric, IvfBuildParams, HnswBuildParams, SQBuildParams)` (`:462`, not run)
  - All of these use `IndexType::Vector`. The concrete types are `IvfPq=103`, `IvfHnswSq=104`, `IvfRq=107` (`lance-index-core-12.0.0/src/lib.rs:74-81`).
  - Two named vector indexes on one column were accepted (ran).
- Scalar: `ScalarIndexParams::for_builtin(BuiltinIndexType::BTree | Bitmap | LabelList ..)` (`lance-index-core-12.0.0/src/scalar.rs:116`) with `IndexType::BTree=1`, `Bitmap=2`, `LabelList=3`. ZoneMap, BloomFilter and RTree also exist.
- Incremental maintenance: `optimize_indices(&OptimizeOptions)` (`index.rs:2274`).
- Each index build commits a new version (4 index builds gave 4 versions in the spike).

**Vector search** (`dataset/scanner.rs`):
- `scan()` (`dataset.rs:1683`), then `.nearest(col, &dyn Array, k)` (`:1911`), `.nprobes(n)` (`:2054`) or `.minimum_nprobes`/`.maximum_nprobes` (`:2086/2106`), `.ef(n)` for HNSW (`:2115`), `.refine(factor)` (`:2145`), `.distance_metric` (`:2153`), `.filter(sql)` (`:1642`) or `.filter_expr(datafusion Expr)` (`:1697`), `.prefilter(true)` (`:1559`), `.fast_search()` (`:2127`; skips the flat search of unindexed fragments), `.use_index(bool)` (`:2187`), `.with_row_id()` / `.with_row_address()` (`:2227/2234`), `.with_fragments(Vec<Fragment>)` (`:1461`), `.limit` (`:1877`), and `create_plan()` returns a DataFusion 54 `ExecutionPlan` (`:3082`) (ran).
- **External bitmap prefilter:** `.with_row_addr_prefilter(RowAddrMask::from_allowed(RowAddrTreeMap) | from_block(..))` (`:1600`; `lance-select-12.0.0/src/mask.rs:26`) is combined into both the index and the flat branch, keyed in `_rowid` space (ran). This is the hook for Tantivy-hit masks and for our own delete bitmaps.
- Lance also implements `TableProvider for Dataset` (`datafusion/logical_plan.rs:21`) and `LanceTableProvider` (`datafusion/dataframe.rs:123`).

**Versions and time travel.**
- `dataset.checkout_version(u64 | tag | (branch, ver))` (`dataset.rs:524`) (ran).
- `DatasetBuilder::from_uri(u).with_version(v).with_session(s).load()` (`builder.rs:240, 527, 635`).
- `with_serialized_manifest(&[u8])` (`builder.rs:295`) skips the manifest GET.
- Other calls: `versions()` (`dataset.rs:2613`), `latest_version_id()` (`:2687`), `manifest_location()` (`:1211`).
- `enable_v2_manifest_paths` defaults to true (`write.rs:714`).

**Cleanup (so our GC owns deletion).**
- Auto-cleanup is **off by default** (`WriteParams.auto_cleanup: None`, `write.rs:643, 716`; test at `:2403`). It triggers only if the manifest config has `lance.auto_cleanup.*` keys (`dataset/cleanup.rs:1519-1650`). Also pass `CommitBuilder::with_skip_auto_cleanup(true)` (`commit.rs:227`) defensively.
- Manual cleanup:
  - `Dataset::cleanup_old_versions(older_than: chrono::TimeDelta, delete_unverified, error_if_tagged)` (`dataset.rs:1484`)
  - `cleanup_with_policy(CleanupPolicy)` (`:1518`)
  - `CleanupPolicyBuilder::versions(Vec<u64>)` (`cleanup.rs:1422`, exact version set), `before_version`, `before_timestamp`, `delete_unverified`, `delete_rate_limit`, `clean_referenced_branches` (`:1345-1365`)
- What cleanup removes: old manifests plus the data, deletion, index and transaction files that are referenced by none of the kept manifests. The latest version is always kept. Files not referenced by any manifest ("unverified", for example an in-flight staged fragment) are kept unless `delete_unverified` is set (`dataset.rs:1474-1480`, `cleanup.rs:1495-1511`). Tagged versions are protected (with an error if `error_if_tagged_old_versions`).
- **Recommended:**
  - The Operon GC worker computes the set of Lance versions referenced by no live collection manifest and calls `cleanup_with_policy(versions(set))` with `delete_unverified=false`.
  - Orphaned staged fragments from failed batches are removed by our own orphan sweep, with a grace period.
  - Never set `lance.auto_cleanup.*`.

## (h) Blockers and recommended rulings

1. **DataFusion 55 cannot be used with Lance.** Ruling: pin DF `54.1` and arrow/arrow-flight `58` workspace-wide, and move in lockstep with Lance releases. Update doc 11's version column (DF 55.x → 54.1; datafusion-distributed 4.0 → 3.0 if used).
2. **Lance defaults to file format 2.2.** Ruling: set `data_storage_version: Some(V2_1)` explicitly as the design says, or re-decide to adopt 2.2, which is the new stable default. Record this in doc 03 §3.1.
3. **Row address is not stable across Lance compaction** (it affects doc 03 §3.2 "Tantivy stores the Lance row address"). Ruling needed in M1:
   - (a) `enable_stable_row_ids = true` from day one and store `_rowid` in Tantivy (recommended: compaction needs no remap, and the index is not remapped either), or
   - (b) keep addresses and make the Tantivy split rewrite part of the Lance compaction commit.
4. **cargo-deny licenses:** add `BSL-1.0` (Boost) and `bzip2-1.0.6` to `deny.toml` `[licenses].allow`, and fix the comment ("BSL" → "BUSL-1.1").
5. **Custom URL scheme silently gets `UnsafeCommitHandler`.** Ruling: `operon-collection` always sets `commit_handler` explicitly (ConditionalPut), or uses `ExternalManifestCommitHandler` backed by meta. Add a test that fails on unsafe commits (FaultyStore concurrent commit).
6. **qdrant-edge weight** (+171 packages, including zbus/D-Bus via cgroups-rs, a tokenizer stack and tonic). Not a blocker. Ruling: isolate it in its own crate (`operon-hnsw`) behind a feature so builds that do not need the hot tier skip it. Upstream a request for a lean feature set.
7. **Build cost:** 13 GB debug target and a 3 GB debug binary. Ruling: set `[profile.dev] debug = "line-tables-only"` (or `split-debuginfo`) and install `protoc` in CI (Lance build requirement). Do not enable Lance's `protoc` feature, which is a C++ build.
8. **Quickwit's tantivy rev (0.27-dev) is ahead of crates.io 0.26.2.** Not a blocker: the vendored files build on 0.26.2 with about 12 edits (see (d)). Ruling: stay on crates.io tantivy and own the edits.
9. **Two `object_store` majors** (0.13 in DataFusion only). This is not a blocker for M1, because Lance, SlateDB and ours all use 0.14. It becomes relevant once DataFusion's ListingTable or Parquet reads Iceberg over our store; that will need a 0.13 adapter or a DF release on 0.14.
