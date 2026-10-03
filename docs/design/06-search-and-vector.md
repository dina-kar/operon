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
| Object / nested | `object`, `nested` | JSON payload: the catch-all `payload` Json field over the whole point payload (M1.4 Ruling 5) | JSON column | JSON field (flattened paths) | — |

As built in M1.1 (Ruling 4, §03 §3.1): typed fields are held in Lance only inside `_source` and are indexed in Tantivy; the only Lance columns are the system columns and the dense and sparse vector columns, and the only Lance scalar index is the BTREE on `_pk`. The table's per-field Lance columns and scalar indexes are not built.

In the ES gateway, `dense_vector` values leave `_source` for vector storage on write and are restored at their original paths on read (M1.5 Ruling 2, M1 overview A9).

Dynamic mapping follows ES defaults for unknown fields (string → text + keyword subfield), bounded by a per-collection field limit.

## 2. Write path

1. Gateway (`_bulk`, `_doc`, Qdrant `upsert`, native) validates and appends operations to the collection's implicit stream; responds with a consistency token (ES `refresh=wait_for` waits for tail visibility, which is immediate). While the collection's unapplied backlog is at its budget (records past `applied`, and their bytes), the write is refused with 429 or `RESOURCE_EXHAUSTED` and `Retry-After` (M1.3, D86). The bulk-load override (`Loams-Backpressure: off`, or `loams-backpressure: off` in Flight metadata) applies to these writes and admits them up to 4× the budget; above the budget, strong reads may use range tails or answer `Unavailable`.
2. Collection-link worker consumes batches (target 16–128 MiB or 1–5 s):
   - Resolves upserts/deletes via the PK index (latest-wins per key, in partition order).
   - Records that cannot be decoded, sit on the wrong partition or violate the current schema are **dead letters**: skipped, counted in the manifest's `dead_letters_total`, logged, and written to one `deadletters/…dlq` object per commit that lives as long as its manifest (M1.1 Ruling 11).
   - Writes a Lance fragment + a Tantivy split from the same batch; updates deletion bitmaps for superseded docs.
   - If the collection has a changelog stream (§02 §8.1), appends the batch's change records with a fenced append first.
   - Commits a **detached** Lance version built from exactly the parent manifest's Lance version (R7) → writes the split, bitmaps, PK delta, dead letters and manifest → fenced, freshness-checked CAS of the pointer in meta → updates the PK index after the CAS (§03 §3.3).
3. Background: split merges, Lance compaction, vector index optimization (incremental add to IVF; periodic re-clustering when centroid drift exceeds threshold). As built (M1.3): split merges re-index live docs from `_source` with each split's own schema (M1.3 Ruling 4); Lance compaction is Lance's rewrite committed as a detached version (Ruling 5); vector index freshness is M1.1's delta segments and full rebuilds.

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
- `qdrant-edge` 0.8 (R20; Apache-2.0), behind the `HnswIndex` traits of `loams-hnsw`, the only crate that names it: HNSW graph construction and search, **payload-aware (filterable) links**, scalar/product/binary quantization. Point ids are stable row ids, so an artifact stays valid across compaction and merges (M1.3 Ruling 1).
- **Filters** reach the index as `has_id` allow-lists built from `allow ∩ covered` (M1.2 passes an allow-list only for selective filters). Unless `payload_m` is 0, the build copies the first 8 indexed non-JSON `Keyword`, `I64`, `Bool` and `Uuid` fields into point payloads with payload indexes, so qdrant-edge adds its per-value links as Qdrant does; search never uses payload conditions (M1.3 Ruling 3).
- **The delta index:** a query at a manifest newer than the artifact uses a view: the artifact's points minus the rows deleted since (a `must_not has_id` filter), plus an appendable qdrant-edge index of the live rows inserted since (M1.3 Ruling 2). A view with more than 100 000 exclusions is not served until the rebuild lands; a column is rebuilt once the rows inserted since reach `max(10 000, 20 %)` of its points or it has been stale for 10 minutes (Ruling 16).
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
| Index admin | create/delete/exists/get index (concrete names and comma lists) with mappings and settings at creation, `_mapping` get/put, aliases over one or more indices with at most one write index (`is_write_index`; `_aliases` actions are atomic; reads fan out over every member, writes go to the write index; D57), `GET /_all`, `_refresh` (no-op), `GET /`, `_cluster/health` (synthetic), `_license` (synthetic), `_ml/trained_models/{id}/_infer` (404: Loams runs no models) | `_cat/indices`, `_flush`, `_settings` endpoints, wildcard deletes, alias filters and routing, index templates, analyzers config | ILM, snapshots API (use Loams versions), ingest pipelines, CCR/CCS |
| Tooling | LangChain and LlamaIndex ES vector stores (and LangChain's ES retrievers, chat history and caches), over elasticsearch-py 8.19; BEIR | elasticsearch-py/js/java client suites; OpenSearch clients (verify divergence) | Kibana |

`_delete_by_query` and `_update_by_query` call the native filter writes (D87). A write refused by backpressure answers 429 `es_rejected_execution_exception` with `Retry-After`, which elastic-transport retries (D86).

The gateway reports Elasticsearch 8.19.0 and sends `X-Elastic-Product: Elasticsearch` on every response (M1.5 Ruling 11). Its `script_score` parser recognises only the fixed cosine, dot-product and L2 vector sources used by the pinned client helpers (Ruling 1, Task 7).

**Phase A deviations from Elasticsearch 8.19:**

- Keyword `ignore_above`, replica count, refresh interval, routing and preference are accepted but not enforced; the shard count maps to collection partitions (Ruling 16). Supported date formats are `strict_date_optional_time`, `date_optional_time`, `strict_date_optional_time_nanos`, `epoch_millis` and their combinations; dynamic date detection has a smaller set (Ruling 17).
- `_seq_no` is the partition offset and `_version` is `_seq_no + 1`, so versions are not dense (Ruling 4). An unindexed `text` or `binary` field has a fast column, and an `enabled: false` object's contents are mapped dynamically and refused under `dynamic: strict`; stored-only fields and unmapped subtrees need the M2 schema change (O-M15-8/9).
- Numeric `range` bounds on a `flattened` field compare as numbers rather than keywords (E11). `query_string.lenient` is accepted but not applied; a fieldless `multi_match` or `query_string` searches fewer fields than ES (T7-5).
- A search across several indices fails as a whole when one index fails; ES returns hits from the other shards with `_shards.failed` (O-M15-10). A field sort key after `_score` is refused, and `_doc` sorts by `_id`; field tiebreaks after `_score` need M2 (O-M15-11).
- A `script_score` search with `min_score` counts matches within its top `from + size` window (T9-5). `_update_by_query` accepts only `params` assignments and `remove()`; its `scroll_size` is validated but the service chooses batch sizes (T9a-6). By-query `batches` are counted per index (T11-5).
- Parser locations and JSON syntax errors differ from Elasticsearch's; negative-boost and over-depth query errors, non-array vector errors and the dynamic field-limit count also retain Loams's wording (T11-5). The full deviation list is in the [`loams-es` crate documentation](../../crates/loams-es/src/lib.rs).

The gateway parses the ES DSL into the Loams search IR. Quickwit provides reference shapes and two small helpers with their original attribution (M1.5 Ruling 1); Phase B aggregation mapping may reuse further Quickwit code.

**Conformance:** the LangChain and LlamaIndex ES integration suites, run unmodified, and BEIR (M1 exit gates, §12). Before vendoring Elastic's REST YAML spec tests, verify their license (Elastic relicensed in 2024: AGPL/SSPL/ELv2 options).

## 8. Qdrant compatibility scope

Qdrant's protobuf definitions are Apache-2.0: nine of them are vendored and compiled with `tonic-prost-build`, and the REST types are a hand-written serde model (M1.4 Rulings 2 and 3, D133).

| Area | Phase A (M1) | Phase B | Accepted as no-op / out |
|---|---|---|---|
| Collections | create/delete/get/list, aliases, named vectors, named sparse vectors (`modifier: idf`), distance metrics, HNSW/quantization params (mapped to hot-tier config) | collection update params, optimizer config (mapped), adding a sparse vector after creation | shard/replica settings (no-op), cluster APIs |
| Points | upsert, delete, get, scroll, count, set/overwrite/delete payload, batch update (dense and sparse vector values) | — | — |
| Search | `query` (universal API: prefetch, nested prefetch, fusion RRF/DBSF, rescore, filters, `lookup_from`), `query/batch`, `query/groups` with `with_lookup`, dense and sparse nearest (with `params.idf`), sparse vectors in prefetches and fusion, `recommend` (`average_vector`, `best_score`, `sum_scores`), `discover`, `context`, MMR, the legacy `search`, `search/batch`, `search/groups`, `recommend`, `recommend/batch`, `recommend/groups`, `discover`, `discover/batch`, `with_payload`/`with_vectors`, score threshold, `offset` | sparse recommend/discover/context/MMR, multivector (hot tier), `order_by`, weighted RRF; `formula` (**M2**, D91) | — |
| Payload indexes | keyword, integer, float, bool, datetime, uuid, full-text | geo | — |
| Snapshots | create/list → Loams manifest versions (restore = time travel) | download/upload | — |

Filter writes (`delete`, the payload operations and `delete_vectors` with a `filter`) call the native filter writes (D87) once M1.5 Task 9a lands; until then the gateway resolves their ids with `scroll` and writes them in chunks (M1.4 Ruling 13). `datatype: float16 | uint8` is served from M2 (D94); `update_filter` from M2 (conditional writes, D89).

**Conformance:** Qdrant Python/TS/Rust client test suites (subset), LangChain/LlamaIndex Qdrant vector store tests, recall parity vs. reference Qdrant (Recall@10 within 1% at equal latency budget on hot tier).

**As built (M1.4).** `loams-qdrant` serves Qdrant 1.19.1's REST API on 6333 and its gRPC API on 6334 over `CollectionService`, including the legacy search, recommend and discover routes and methods that the 1.19 server still serves (Ruling 1, D132). The `loams` binary starts both listeners (flags `--qdrant-listen`, `--qdrant-grpc-listen`, `--qdrant-namespace`, `--no-qdrant`; 127.0.0.1 by default, D112). The official Python `qdrant-client` runs against `loams dev` in CI: 1.15.1 over REST with the legacy methods, and 1.19.1 with the query API over REST and gRPC, each result checked against brute force with Qdrant's formulas.
- **Payloads and filters.** Every collection created through the gateway has the catch-all `payload` Json field over the whole payload, and every filter reads it, text and datetime conditions included, so any key filters without an index and results never depend on when an index was created. Payload indexes (keyword, integer, float, bool, datetime, uuid, full-text) add lenient typed `payload_index.<key>` fields that filters never read; they are kept for `payload_schema` and the hot tier's payload links (Rulings 5 and 6, D134).
- **Sparse vectors.** Config (with `modifier: idf`), values, and nearest queries in `query`, prefetches and fusion (with `params.idf`) map one to one onto the IR's sparse vectors, scored exactly (Ruling 21, overview A26–A29, D136).
- **Gateway-side scoring.** `recommend` with `best_score` or `sum_scores`, `discover`, `context` and MMR are scored in the gateway with Qdrant's formulas over the union of IR candidate searches; groups follow Qdrant's collect-then-fill driver over the compiled query (Rulings 10 and 11, D135).
- **Snapshots** are manifest versions: create names the newest retained version, list shows them all; before its first commit a collection is version 0 (Ruling 19, row T10-3).
- **Unsupported (501, `Unsupported in Loams: <feature>`, gRPC `UNIMPLEMENTED`):** sparse `recommend`, `discover`, `context` and MMR, sparse rescoring, a sparse vector added after creation, multivectors, a `datatype` other than `float32`, inference objects, geo conditions and indexes, `nested`, `has_vector` and `slice` conditions, keys with `[n]` or quoted segments, `order_by`, `formula`, `sample`, `relevance_feedback`, MMR inside a prefetch, weighted RRF, a prefetch `score_threshold`, a leaf prefetch without a query, custom sharding and shard keys, `update_mode` other than `upsert`, `update_filter`, payload-index deletion and type changes, snapshot download, delete, recover and upload, full-storage snapshots, `facet`, `search/matrix/*`, deleting a named vector, and cluster mutations (Ruling 15).

**Divergences from Qdrant 1.19** (the same list, with the rulings, is the `loams-qdrant` crate documentation):
- **Filters.** A plain `a.b` also reaches `{"a": [{"b": …}]}` and nested arrays are flattened. An integer condition matches an integral float (`1` matches `1.0`, E8). A key holding no non-null leaf counts as missing for `is_empty` and `except`, and `except` on an array is true only when no element is listed (Qdrant: one element outside the list). Several sub-conditions of one `FieldCondition` are OR-ed. `match.text`, `text_any` and `phrase` use the `standard` analyzer over every string at the key (`text_any` matches whole tokens, `text` may find its tokens across an array's values), and a text index with other tokenizer options is refused. Datetime ranges match only strings in M1.1's date formats. A numeric `range` without bounds is `exists` (Ruling 6, E8, row T4-8). Filters on a collection not created through the Qdrant API are unsupported (row T4-3).
- **Writes.** Writes by filter are not atomic until D87 lands, and points inserted meanwhile may be missed (Ruling 13). `set_payload` and `overwrite_payload` with `key`, and `delete_payload` of indexed paths, read the point and rewrite its payload, so a concurrent write in between is lost (Ruling 12). Inside a batch, those reads and the filter writes' id lookups see the points as they were before the batch (row T5-4, owner ruling O2). A write request holds at most 10,000 listed operations; a larger one is 400, asking the client to split it (row T5-12, owner ruling O1, row T8-11). `wait` only changes the status string, because an acknowledged write is already visible; `ordering` and read `consistency` are ignored (Ruling 14).
- **Reads.** A scroll `limit` over the search window (100,000) is refused (row T7-11). `count` with `exact: false` is exact. `ScoredPoint.version` is 0, `segments_count` 1 and `indexed_vectors_count` equals `points_count` (Ruling 20).
- **Scoring.** DBSF over Euclid or Manhattan prefetches normalizes Loams's larger-is-better scores, where Qdrant normalizes raw distances (Ruling 9). `recommend` (`best_score`, `sum_scores`), `discover` and `context` score a bounded candidate set, one search of `min(max(4 × (offset + limit), 100), max_candidates)` points per example, so a relevant point outside every neighbourhood is missed (Ruling 10). With negatives only (`best_score` or `sum_scores`, accepted as Qdrant does; owner rulings on row T8-7), the searches go away from the negatives: on Cosine and Dot the nearest points to each negated negative and to their negated sum, which are exactly the least similar ones; on Euclid and Manhattan one exact scan by dot product with their negated sum (or the negated negative when there is one, or the sum is zero), which can miss a far point of small norm (review of PR #57; one scan per query, review of PR #60; accepted as approximate, M1.5 owner ruling O-M15-1). An empty `context` scores the first `candidate_k` points of the filter 0 each. The refusal text for missing examples is `No positive examples given` (row T8-7). MMR's `candidates_limit` is capped at `max_candidates` (10,000; Qdrant refuses over 16,384; row T8-8).
- **Request sizes.** A query batch holds at most `max_batch_queries` (1,000) requests and a list retrieve at most min(`max_point_ids` (10,000), native `max_get_keys`) ids; a longer list is 400 `Wrong input: … more than the limit of …`, independent of Qdrant's optional strict-mode batch limit and default 32 MiB REST body cap. Single-point GET retains its fixed one-id bound independently of the list limit (row T12-1, issue #298).
- **Groups.** A collect request leaves out every point holding a key of a full group; a fill request takes the unsatisfied groups' integer or string keys, where Qdrant requires both at once, which differs only for groups of mixed key types (owner ruling on row T9-2, D137); groups whose best hits tie are ordered by key; an integer key above `i64::MAX` voids its point; MMR's default `candidates_limit` under groups is `limit × group_size` (rows T9-2, T9-3).
- **Sparse vectors.** IDF statistics count live points only (Qdrant's server also counts deleted points until its optimizer runs); every sparse search is exact (`full_scan_threshold` is ignored); validation errors read `Wrong input: Sparse vector <name>: …` (Ruling 21).
- **Collections and snapshots.** Shard, replica, WAL, optimizer and strict-mode settings are stored and echoed but change nothing, and `PATCH /collections/{c}` is a no-op for them; cluster info is synthetic (Ruling 16). Snapshots are manifest versions, listed whether or not the API created them (Ruling 19). `ServiceError::Timeout` answers `Timeout: request timed out` without a duration (row T1-1). `GET /collections/aliases` answers 405 (row T3-9).

## 9. Analyzers and languages
**M1 analyzer set** (`loams-text`, M1.1 Ruling 25, overview A5), built to match Lucene because the BEIR gate compares rankings with ES:
- `standard`: UAX #29 word segmentation + lowercase, no stop words, tokens over 255 chars split at 255 (ES `standard`);
- `english`: Lucene's `EnglishAnalyzer` chain: standard tokenizer → English possessive filter → lowercase → Lucene's 33 English stop words → Porter stemmer;
- `simple` (letter tokenizer + lowercase), `whitespace` (case kept), `keyword` (the whole value as one token).

The Porter stemmer is the **original** 1980 Porter algorithm (what Lucene's `PorterStemFilter` implements), ported from Martin Porter's ANSI C reference into `loams-text` and checked against Porter's published 23 531-word vocabulary and output; it is not Porter2 (Snowball `english`, as in `rust-stemmers`).

**M2 (D93):** the ES language analyzers that Lucene builds from Snowball stemmers and stop-word lists, each checked token for token against ES `_analyze`; `asciifolding`, `lowercase` and `max_token_length` for custom analyzers; pre-tokenized text (an array of tokens indexed verbatim); per-field BM25 `k1` and `b`.

Later: n-gram and other Tantivy tokenizers, `lindera` (Japanese/Korean), `jieba-rs` (Chinese), ICU-based tokenizer; ES analyzer definitions mapped where equivalent, rejected with a clear error otherwise.

## 10. Benchmarks and gates
- Text relevance: BEIR subsets (nDCG@10 parity with Elasticsearch BM25 ± 1 point).
- Vector: VectorDBBench / ann-benchmarks subsets (recall/latency vs. Qdrant).
- Hybrid: BEIR hybrid (BM25 + dense) vs. ES + Qdrant composite pipeline.
- Recall in production: `POST …/collections/{c}/recall` compares ANN with the exact kernel on sampled stored vectors (M1.7, D92); M2 samples a fraction of live vector queries and exports recall per collection.
