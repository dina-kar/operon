# 06 — Search & Vector (Elasticsearch + Qdrant Pillars)

Status: **Approved** · 2026-09-22 · revised 2026-09-25 (Elasticsearch Phase A trimmed to the framework suites, D48) · amended 2026-09-26 (M1.2 as built) · amended 2026-09-26 (turbopuffer gap analysis: backpressure, filter writes, ranking expressions, analyzers, vector types, recall; D86, D87, D91–D94)

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
| Dense vector | `dense_vector` (`element_type: byte` from M2) | named / default vector (`datatype: float16 \| uint8` from M2) | FixedSizeList of f32; f16, i8 and u8 from M2 (D94) | — | IVF_RQ / IVF_PQ / IVF_HNSW_SQ |
| Sparse vector | `sparse_vector` (Phase B) | sparse vector (M1) | `Struct<indices, values>` column | split postings + weights (M1), custom sparse index (§6, Phase B) | — |
| Multivector | — | multivector (ColBERT) | List<FixedSizeList> | — | hot tier (§5) |
| Object / nested | `object`, `nested` | JSON payload | JSON column | JSON field (flattened paths) | — |

As built in M1.1 (Ruling 4, §03 §3.1): typed fields are held in Lance only inside `_source` and are indexed in Tantivy; the only Lance columns are the system columns and the dense and sparse vector columns, and the only Lance scalar index is the BTREE on `_pk`. The table's per-field Lance columns and scalar indexes are not built.

Dynamic mapping follows ES defaults for unknown fields (string → text + keyword subfield), bounded by a per-collection field limit.

## 2. Write path

1. Gateway (`_bulk`, `_doc`, Qdrant `upsert`, native) validates and appends operations to the collection's implicit stream; responds with a consistency token (ES `refresh=wait_for` waits for tail visibility, which is immediate). While the collection's unapplied backlog is at its budget (records past `applied`, and their bytes), the write is refused with 429 or `RESOURCE_EXHAUSTED` and `Retry-After` (M1.3, D86).
2. Collection-link worker consumes batches (target 16–128 MiB or 1–5 s):
   - Resolves upserts/deletes via the PK index (latest-wins per key, in partition order).
   - Records that cannot be decoded, sit on the wrong partition or violate the current schema are **dead letters**: skipped, counted in the manifest's `dead_letters_total`, logged, and written to one `deadletters/…dlq` object per commit that lives as long as its manifest (M1.1 Ruling 11).
   - Writes a Lance fragment + a Tantivy split from the same batch; updates deletion bitmaps for superseded docs.
   - If the collection has a changelog stream (§02 §8.1), appends the batch's change records with a fenced append first.
   - Commits a **detached** Lance version built from exactly the parent manifest's Lance version (R7) → writes the split, bitmaps, PK delta, dead letters and manifest → fenced, freshness-checked CAS of the pointer in meta → updates the PK index after the CAS (§03 §3.3).
3. Background: split merges, Lance compaction, vector index optimization (incremental add to IVF; periodic re-clustering when centroid drift exceeds threshold).

**Filter writes (M1.5, D87).** `CollectionService::delete_by_filter` and `patch_by_filter` evaluate a filter at one pin, page the matching keys by primary key, and append plain deletes or patches in atomic batches of 1 000, each admitted like any write. The result carries `matched`, `affected`, `rows_remaining`, a cursor that continues at the same pin, and one consistency token. At most 5 000 000 rows are deleted or 50 000 patched per call (or `max_rows`, if smaller); a call matching more fails before writing unless `allow_partial`, which writes up to that ceiling and returns a cursor, never more. The native routes, ES `_delete_by_query` and `_update_by_query` and the Qdrant filter writes all call them. From M2 the filter is re-checked at apply through conditional ops, so a key that stopped matching is skipped (D89).

## 3. Read path and ranking

- **BM25 / boolean / phrase / fuzzy:** `TantivySearchExec` over the manifest's splits + tail index, block-max WAND top-k per split, global merge.
- **BM25 statistics are global and live-only** (M1.2 Ruling 2): over every split of the manifest plus the tail, `total_num_docs` counts live, unshadowed rows, `doc_freq` counts postings minus deleted and shadowed docs, and `total_num_tokens` sums the midpoint of each live doc's fieldnorm bucket. A document's score therefore does not depend on where its history lives (Lucene counts deleted docs until a merge). WAND prunes with a small slack, and every candidate is rescored with a canonical form of the query whose Boolean nodes each add two scores, so scores are bit-identical across split layouts (M1.2 plan row 15.1).
- **BM25 parameters (M2, D93):** per-field `k1` and `b` in the schema (defaults 1.2 and 0.75); the rescoring above computes them.
- **ANN:** `AnnExec` (hot HNSW if present, else Lance IVF + refine) + tail brute force.
- **Hybrid:** RRF / weighted / DBSF fusion (§05).
- **Ranking expressions (M2, D91):** one `rank` expression after fusion (field values, `saturate`, `decay`, recency, rank by filter, sums and products), which ES `function_score` and Qdrant `formula` compile to (§05 §4).
- **Aggregations (ES Phase B):** Tantivy aggregation framework over fast fields (terms, histogram, date_histogram, range, stats/extended_stats, percentiles, cardinality, top_hits) — the same engine Quickwit uses for its ES-compatible aggregations.
- **Highlighting (ES Phase B):** Tantivy snippet generator on stored/positions data.

## 4. Filtering strategy

| Filter selectivity | Strategy |
|---|---|
| Very selective (< ~1% of docs) | Pre-filter bitmap → exact brute-force distance over matching docs (cheap) |
| Moderate | Pre-filter bitmap passed into HNSW (filterable-HNSW links keep graph connectivity) or IVF (probe more partitions, restricted to bitmap) |
| Broad | Post-filter with over-fetch factor, retry with larger k if under-filled |

Selectivity is estimated from bitmap cardinalities (Tantivy/Lance scalar indexes) at plan time.

As built (M1.2, Lance IVF; the hot HNSW arrives in M1.3): *very selective* is at most `max(1 000, full_scan_threshold_kb · 1024 / (4 · dim))` allowed durable rows (Qdrant's full-scan rule), scored exactly by brute force; *moderate* is at most 10 % of the durable rows, a prefiltered Lance index search; *broad* post-filters with `k · 1.5 / selectivity` candidates, retries once with 4× more, then prefilters. Tail documents are always scored by brute force, and shadowed rows never leave the durable path.

## 5. Vector tiers

### 5.1 Durable: Lance IVF
- Default `IVF_PQ` (`VectorIndexSpec::Auto`; no index for Manhattan distance) with full-vector refine, or `IVF_RQ` (RaBitQ); `IVF_HNSW_SQ` for high-recall collections.
- Cold path touches: centroids (cached H0) → selected partitions (range GETs) → refine vectors (range GETs) ⇒ ~3 round trips.
- Element types (M2, D94): f32, f16, i8 and u8 columns; indexes and hot artifacts build at the stored width, and the exact kernel widens every element to f64 (D79).
- Freshness: tail brute-force until the incremental index step, then delta segments over unindexed fragments, full rebuild past `index_max_segments` (not `optimize_indices`, which commits to Lance's mainline; M1.1 Ruling 3). Each build is a worker task committed as a detached `CreateIndex` and a manifest CAS that rebases onto concurrent link-apply commits (R9); a first index waits for 256 rows with the vector (PQ training minimum). Re-clustering in background. SPFresh-style incremental split/merge of partitions is a Phase C research item (reference: SPFresh paper, turbopuffer design; no production Rust implementation exists).

### 5.2 Hot: Qdrant-derived HNSW
- Fork of Qdrant's `lib/segment` components (Apache-2.0): HNSW graph construction/search, **payload-aware (filterable) links**, scalar/product/binary quantization, and the payload-filter planner. Evaluate `qdrant-edge` (0.8) as the packaging boundary before forking deeper.
- Built by workers from a manifest version, published as a hot artifact (`hot/hnsw/<column>/<source_version:020>-<ulid>/`), loaded by owning query nodes into RAM (quantized vectors) + NVMe (full vectors for rescoring).
- Incremental updates: new points go into a small appendable in-memory HNSW (Qdrant's appendable-segment model); periodic rebuild/merge into the main artifact; deletes via bitmap.
- Used when a collection is pinned or auto-promoted; otherwise Lance IVF serves.
- Larger-than-RAM alternative: **DiskANN** (MIT, Rust) on NVMe — evaluate in Phase C.

## 6. Sparse vectors
Qdrant sparse vectors / ES `sparse_vector` need float-weighted inverted lists and dot-product scoring.

**M1 (owner decision 2026-09-25; M1 overview A26–A30, R22):** Qdrant sparse vectors ship in M1 with a simple, exact index. Each sparse field is a Lance column (`Struct<indices: List<u32>, values: List<f32>>`, the source of truth) and two hidden fields in every Tantivy split and in the tail's RAM index: a u64 postings field with one term per index (plus a presence term) and a bytes fast field holding the vector. A query unions the postings of its indices, masks deleted, shadowed and filtered documents, reads each candidate's vector and scores it exactly (Qdrant's dot product in f32, and Qdrant's IDF modifier `ln((N − df + 0.5)/(df + 0.5) + 1)` with `N` and `df` counted over the live documents of the read snapshot). No hot artifact: pinned splits serve it, so results are identical with the hot tier on and off. qdrant-edge's sparse index was not taken: it is private and local-directory-only. ES `sparse_vector` stays out of M1 (string token keys and Lucene's reduced-precision weights make it more than a mapping).

**As built (M1.2):** `SparseExec` scores every candidate exactly (M1.2 Ruling 21): the union of the query indices' postings in `_sparse.<name>`, minus deleted and shadowed docs, intersected with the filter; `Σ q'ᵢ · wᵢ` over shared indices in ascending index order with f32 accumulation (Qdrant's `score_vectors`), `q'ᵢ = qᵢ · idfᵢ` for `Idf` fields. IDF statistics are live-only, like BM25's: `N` counts the live documents with the vector and `dfᵢ` the live postings of index *i*, over the manifest's splits and the tail, or over the `idf_corpus` filter's rows when a request sets one. Results are identical hot or cold and across split layouts.

**Phase B:** a **custom sparse index** stored as split-adjacent posting files (quantized f16 weights, block-max metadata) with a MAXSCORE scorer — informed by turbopuffer's FTS v2 posting-block design — replaces the M1 split fields when collections outgrow exhaustive scoring; ES `sparse_vector` follows it.

## 7. Elasticsearch compatibility scope

Phase A is exactly what the gated LangChain and LlamaIndex Elasticsearch suites and BEIR send (D48; the M1.5 plan's conformance-surface table lists every construct and its sender). None of them sends aggregations, a point in time or highlighting, so those are Phase B; `_msearch` stays in Phase A because the BEIR harness batches its queries through it. There is no elasticsearch-py client-suite gate, and wildcard and `_all` index deletes are refused, as ES 8 does by default. Aliases may name several indices (D57): LangChain's cache tests put one alias on two indices with a write index.

| Area | Phase A (M1) | Phase B | Out of scope |
|---|---|---|---|
| Document APIs | `_doc` index/create/get/delete, `_create`, `_bulk`, `_mget`, `_update` (partial doc, upsert), `_delete_by_query` (LlamaIndex and LangChain delete through it), `_update_by_query` with recognised scripts (assignments from `params`, `remove`; D87) | `_update_by_query` with other scripts or none (it needs online index changes, D97) | `_reindex` from remote |
| Search | `_search`, `_count`, `_msearch`, `from/size`, `sort`, `search_after` (without PIT), `_source` filtering, `track_total_hits`, comma-list multi-index search | point in time, highlighting, `scroll`, `collapse`, suggesters (term/completion) | Percolator, scripts in queries |
| Query DSL | `match`, `match_phrase`, `multi_match`, `bool`, `term(s)`, `range`, `exists`, `prefix`, `wildcard`, `fuzzy`, `ids`, `query_string` (simple), `constant_score`, `knn` (top level and as a query), hybrid query + `knn` and RRF (`retriever.rrf`, legacy `rank.rrf`), the fixed LangChain/elasticsearch-py `script_score` vector scripts | `nested`, `function_score` (`weight`, `field_value_factor`, decay functions, per-function `filter`; **M2**, D91), `more_like_this`, `simple_query_string` | Painless scripting, `script_score` with arbitrary scripts |
| Aggregations | — | `terms`, `histogram`, `date_histogram`, `range`, `stats`, `avg/sum/min/max`, `cardinality`, `percentiles`, `top_hits`; then `composite`, `filters`, `significant_terms`, pipeline aggs (subset) | `scripted_metric` |
| Index admin | create/delete/exists/get index (concrete names and comma lists) with mappings and settings at creation, `_mapping` get/put, aliases over one or more indices with at most one write index (`is_write_index`; `_aliases` actions are atomic; reads fan out over every member, writes go to the write index; D57), `GET /_all`, `_refresh` (no-op), `GET /`, `_cluster/health` (synthetic), `_license` (synthetic), `_ml/trained_models/{id}/_infer` (404: Operon runs no models) | `_cat/indices`, `_flush`, `_settings` endpoints, wildcard deletes, alias filters and routing, index templates, analyzers config | ILM, snapshots API (use Operon versions), ingest pipelines, CCR/CCS |
| Tooling | LangChain and LlamaIndex ES vector stores (and LangChain's ES retrievers, chat history and caches), over elasticsearch-py 8.19; BEIR | elasticsearch-py/js/java client suites; OpenSearch clients (verify divergence) | Kibana |

`_delete_by_query` and `_update_by_query` call the native filter writes (D87). A write refused by backpressure answers 429 `es_rejected_execution_exception` with `Retry-After`, which elastic-transport retries (D86).

Implementation starts from **Quickwit's ES-compatible API crates** (DSL parsing → Tantivy queries; aggregation request/response mapping in Phase B), forked and extended with `_doc`-level CRUD, upserts and `knn`.

**Conformance:** the LangChain and LlamaIndex ES integration suites, run unmodified, and BEIR (M1 exit gates, §12). Before vendoring Elastic's REST YAML spec tests, verify their license (Elastic relicensed in 2024: AGPL/SSPL/ELv2 options).

## 8. Qdrant compatibility scope

Qdrant's OpenAPI and protobuf definitions are Apache-2.0 and used directly (`tonic` for gRPC, generated REST types).

| Area | Phase A (M1) | Phase B | Accepted as no-op / out |
|---|---|---|---|
| Collections | create/delete/get/list, aliases, named vectors, named sparse vectors (`modifier: idf`), distance metrics, HNSW/quantization params (mapped to hot-tier config) | collection update params, optimizer config (mapped), adding a sparse vector after creation | shard/replica settings (no-op), cluster APIs |
| Points | upsert, delete, get, scroll, count, set/overwrite/delete payload, batch update (dense and sparse vector values) | — | — |
| Search | `query` (universal API: prefetch, fusion RRF/DBSF, filters), dense and sparse nearest (with `params.idf`), `search`, `search_batch`, `recommend`, `discover`, `search_groups`, `with_payload`/`with_vectors`, score threshold | sparse recommend/discover/context/MMR, multivector (hot tier), `order_by`; `formula` (**M2**, D91) | — |
| Payload indexes | keyword, integer, float, bool, datetime, uuid, full-text | geo | — |
| Snapshots | create/list → Operon manifest versions (restore = time travel) | download/upload | — |

Filter writes (`delete`, the payload operations and `delete_vectors` with a `filter`) call the native filter writes (D87). `datatype: float16 | uint8` is served from M2 (D94); `update_filter` from M2 (conditional writes, D89).

**Conformance:** Qdrant Python/TS/Rust client test suites (subset), LangChain/LlamaIndex Qdrant vector store tests, recall parity vs. reference Qdrant (Recall@10 within 1% at equal latency budget on hot tier).

## 9. Analyzers and languages
**M1 analyzer set** (`operon-text`, M1.1 Ruling 25, overview A5), built to match Lucene because the BEIR gate compares rankings with ES:
- `standard`: UAX #29 word segmentation + lowercase, no stop words, tokens over 255 chars split at 255 (ES `standard`);
- `english`: Lucene's `EnglishAnalyzer` chain: standard tokenizer → English possessive filter → lowercase → Lucene's 33 English stop words → Porter stemmer;
- `simple` (letter tokenizer + lowercase), `whitespace` (case kept), `keyword` (the whole value as one token).

The Porter stemmer is the **original** 1980 Porter algorithm (what Lucene's `PorterStemFilter` implements), ported from Martin Porter's ANSI C reference into `operon-text` and checked against Porter's published 23 531-word vocabulary and output; it is not Porter2 (Snowball `english`, as in `rust-stemmers`).

**M2 (D93):** the ES language analyzers that Lucene builds from Snowball stemmers and stop-word lists, each checked token for token against ES `_analyze`; `asciifolding`, `lowercase` and `max_token_length` for custom analyzers; pre-tokenized text (an array of tokens indexed verbatim); per-field BM25 `k1` and `b`.

Later: n-gram and other Tantivy tokenizers, `lindera` (Japanese/Korean), `jieba-rs` (Chinese), ICU-based tokenizer; ES analyzer definitions mapped where equivalent, rejected with a clear error otherwise.

## 10. Benchmarks and gates
- Text relevance: BEIR subsets (nDCG@10 parity with Elasticsearch BM25 ± 1 point).
- Vector: VectorDBBench / ann-benchmarks subsets (recall/latency vs. Qdrant).
- Hybrid: BEIR hybrid (BM25 + dense) vs. ES + Qdrant composite pipeline.
- Recall in production: `POST …/collections/{c}/recall` compares ANN with the exact kernel on sampled stored vectors (M1.7, D92); M2 samples a fraction of live vector queries and exports recall per collection.
