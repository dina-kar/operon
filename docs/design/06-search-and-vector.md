# 06 — Search & Vector (Elasticsearch + Qdrant Pillars)

Status: **Approved** · 2026-09-22

Collections replace both Elasticsearch indexes and Qdrant collections. Durable tier: **Lance** (documents, vectors, scalar + IVF indexes) + **Tantivy splits** (inverted index, fast fields). Hot tier: **Qdrant-derived HNSW**, pinned splits, in-memory tail indexes (§04).

---

## 1. Collection schema

A collection schema is derived from ES mappings or Qdrant collection config (or declared natively):

| Field kind | ES mapping | Qdrant | Stored in Lance | Indexed in Tantivy | Lance index |
|---|---|---|---|---|---|
| Primary key | `_id` | point id (u64/UUID) | `_pk` | stored `_pk` | BTREE |
| Full text | `text` (+ analyzer) | — (full-text payload index) | yes | TEXT with positions | — |
| Keyword | `keyword` | `keyword` payload index | yes | raw + fast field | BITMAP/BTREE |
| Numeric / date | `long`, `double`, `date` | `integer`, `float`, `datetime` | yes | fast field | BTREE |
| Boolean | `boolean` | `bool` | yes | fast field | BITMAP |
| Geo point | `geo_point` | `geo` | yes | fast field (lat/lon) | (Phase B) |
| Dense vector | `dense_vector` | named / default vector | FixedSizeList | — | IVF_RQ / IVF_PQ / IVF_HNSW_SQ |
| Sparse vector | `sparse_vector` | sparse vector | yes | custom sparse index (§6) | — |
| Multivector | — | multivector (ColBERT) | List<FixedSizeList> | — | hot tier (§5) |
| Object / nested | `object`, `nested` | JSON payload | JSON column | JSON field (flattened paths) | — |

Dynamic mapping follows ES defaults for unknown fields (string → text + keyword subfield), bounded by a per-collection field limit.

## 2. Write path

1. Gateway (`_bulk`, `_doc`, Qdrant `upsert`, native) validates and appends operations to the collection's implicit stream; responds with a consistency token (ES `refresh=wait_for` waits for tail visibility, which is immediate).
2. Collection-link worker consumes batches (target 16–128 MiB or 1–5 s):
   - Resolves upserts/deletes via the PK index.
   - Writes a Lance fragment + a Tantivy split from the same batch; updates deletion bitmaps for superseded docs.
   - If the collection has a changelog stream (§02 §8.1), appends the batch's change records with a fenced append first.
   - Commits Lance version → writes manifest → CAS pointer in meta (§03 §3.3).
3. Background: split merges, Lance compaction, vector index optimization (incremental add to IVF; periodic re-clustering when centroid drift exceeds threshold).

## 3. Read path and ranking

- **BM25 / boolean / phrase / fuzzy:** `TantivySearchExec` over the manifest's splits + tail index, block-max WAND top-k per split, global merge.
- **Aggregations:** Tantivy aggregation framework over fast fields (terms, histogram, date_histogram, range, stats/extended_stats, percentiles, cardinality, top_hits) — the same engine Quickwit uses for its ES-compatible aggregations.
- **ANN:** `AnnExec` (hot HNSW if present, else Lance IVF + refine) + tail brute force.
- **Hybrid:** RRF / weighted / DBSF fusion (§05).
- **Highlighting:** Tantivy snippet generator on stored/positions data.

## 4. Filtering strategy

| Filter selectivity | Strategy |
|---|---|
| Very selective (< ~1% of docs) | Pre-filter bitmap → exact brute-force distance over matching docs (cheap) |
| Moderate | Pre-filter bitmap passed into HNSW (filterable-HNSW links keep graph connectivity) or IVF (probe more partitions, restricted to bitmap) |
| Broad | Post-filter with over-fetch factor, retry with larger k if under-filled |

Selectivity is estimated from bitmap cardinalities (Tantivy/Lance scalar indexes) at plan time.

## 5. Vector tiers

### 5.1 Durable: Lance IVF
- Default `IVF_RQ` (RaBitQ) or `IVF_PQ` with full-vector refine; `IVF_HNSW_SQ` for high-recall collections.
- Cold path touches: centroids (cached H0) → selected partitions (range GETs) → refine vectors (range GETs) ⇒ ~3 round trips.
- Freshness: tail brute-force until the incremental index step; re-clustering in background. SPFresh-style incremental split/merge of partitions is a Phase C research item (reference: SPFresh paper, turbopuffer design; no production Rust implementation exists).

### 5.2 Hot: Qdrant-derived HNSW
- Fork of Qdrant's `lib/segment` components (Apache-2.0): HNSW graph construction/search, **payload-aware (filterable) links**, scalar/product/binary quantization, and the payload-filter planner. Evaluate `qdrant-edge` (0.8) as the packaging boundary before forking deeper.
- Built by workers from a manifest version, published as a hot artifact (`hot/hnsw/<version>/`), loaded by owning query nodes into RAM (quantized vectors) + NVMe (full vectors for rescoring).
- Incremental updates: new points go into a small appendable in-memory HNSW (Qdrant's appendable-segment model); periodic rebuild/merge into the main artifact; deletes via bitmap.
- Used when a collection is pinned or auto-promoted; otherwise Lance IVF serves.
- Larger-than-RAM alternative: **DiskANN** (MIT, Rust) on NVMe — evaluate in Phase C.

## 6. Sparse vectors
Qdrant sparse vectors / ES `sparse_vector` need float-weighted inverted lists and dot-product scoring. Tantivy stores integer term frequencies, so Phase B adds a **custom sparse index** stored as split-adjacent posting files (quantized f16 weights, block-max metadata) with a MAXSCORE scorer — informed by turbopuffer's FTS v2 posting-block design.

## 7. Elasticsearch compatibility scope

| Area | Phase A (M1) | Phase B | Out of scope |
|---|---|---|---|
| Document APIs | `_doc` index/get/delete, `_bulk`, `_mget`, `_update` (partial doc) | `_update_by_query`, `_delete_by_query` | `_reindex` from remote |
| Search | `_search`, `_count`, `_msearch`, `search_after`, PIT, `from/size`, `sort`, `_source` filtering, highlighting | `scroll`, `collapse`, suggesters (term/completion) | Percolator, scripts in queries |
| Query DSL | `match`, `match_phrase`, `multi_match`, `bool`, `term(s)`, `range`, `exists`, `prefix`, `wildcard`, `fuzzy`, `ids`, `query_string` (simple), `knn` | `nested`, `function_score` (field_value_factor, decay), `more_like_this`, `simple_query_string` | Painless scripting, `script_score` with arbitrary scripts |
| Aggregations | `terms`, `histogram`, `date_histogram`, `range`, `stats`, `avg/sum/min/max`, `cardinality`, `percentiles`, `top_hits` | `composite`, `filters`, `significant_terms`, pipeline aggs (subset) | `scripted_metric` |
| Index admin | create/delete index, mappings, aliases, `_cat/indices`, `_cluster/health` (synthetic) | index templates, analyzers config | ILM, snapshots API (use Operon versions), ingest pipelines, CCR/CCS |
| Tooling | elasticsearch-py/js/java clients; LangChain/LlamaIndex ES vector stores | OpenSearch clients (verify divergence) | Kibana |

Implementation starts from **Quickwit's ES-compatible API crates** (DSL parsing → Tantivy queries, aggregation request/response mapping), forked and extended with `_doc`-level CRUD, upserts and `knn`.

**Conformance:** client library integration tests + framework integration tests. Before vendoring Elastic's REST YAML spec tests, verify their license (Elastic relicensed in 2024: AGPL/SSPL/ELv2 options).

## 8. Qdrant compatibility scope

Qdrant's OpenAPI and protobuf definitions are Apache-2.0 and used directly (`tonic` for gRPC, generated REST types).

| Area | Phase A (M1) | Phase B | Accepted as no-op / out |
|---|---|---|---|
| Collections | create/delete/get/list, aliases, named vectors, distance metrics, HNSW/quantization params (mapped to hot-tier config) | collection update params, optimizer config (mapped) | shard/replica settings (no-op), cluster APIs |
| Points | upsert, delete, get, scroll, count, set/overwrite/delete payload, batch update | — | — |
| Search | `query` (universal API: prefetch, fusion RRF/DBSF, filters), `search`, `search_batch`, `recommend`, `discover`, `search_groups`, `with_payload`/`with_vectors`, score threshold | sparse vectors, multivector (hot tier), `order_by` | — |
| Payload indexes | keyword, integer, float, bool, datetime, uuid, full-text | geo | — |
| Snapshots | create/list → Operon manifest versions (restore = time travel) | download/upload | — |

**Conformance:** Qdrant Python/TS/Rust client test suites (subset), LangChain/LlamaIndex Qdrant vector store tests, recall parity vs. reference Qdrant (Recall@10 within 1% at equal latency budget on hot tier).

## 9. Analyzers and languages
Tantivy tokenizers (simple, whitespace, n-gram, stemmers), `lindera` (Japanese/Korean), `jieba-rs` (Chinese), ICU-based tokenizer; ES analyzer definitions mapped where equivalent, rejected with a clear error otherwise.

## 10. Benchmarks and gates
- Text relevance: BEIR subsets (nDCG@10 parity with Elasticsearch BM25 ± 1 point).
- Vector: VectorDBBench / ann-benchmarks subsets (recall/latency vs. Qdrant).
- Hybrid: BEIR hybrid (BM25 + dense) vs. ES + Qdrant composite pipeline.
