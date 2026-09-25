# 08 — Analytics (Iceberg)

Status: **Approved** · 2026-09-22 · revised 2026-09-25 (Iceberg only, no ClickHouse surface, D45; M4)

Tables serve AI-app analytics (product analytics, LLM usage and cost, traces, evals, observability). Storage is **Apache Iceberg** through Lakekeeper, open to every lakehouse engine; speed comes from DataFusion and the Iceberg hot tier (§04 §3); freshness comes from the tail. Operon's SQL surface is DataFusion SQL over Arrow Flight SQL and the native API. External engines, ClickHouse among them, query the same tables through Lakekeeper's Iceberg REST catalog (§8); Operon has no ClickHouse HTTP interface, dialect or MergeTree-engine DDL (D45).

---

## 1. Table semantics

Every table is an Iceberg table. Its write semantics are declared natively:

| Kind | Declaration | Semantics | Implementation |
|---|---|---|---|
| Append-only | default | Rows are appended; duplicates are kept | Iceberg data files; sort order and partition spec from `SORTED BY` / `PARTITIONED BY` |
| Keyed | `PRIMARY KEY (…)` | One live row per key, latest write wins in log order; every read is deduplicated | PK index + deletion vectors on upsert (§03 §2.3) |
| Keyed, versioned | `PRIMARY KEY (…) VERSION BY (col)` | The row with the greatest `col` wins; equal versions resolve by log order. Late, older rows are dropped at apply | As keyed; the apply worker compares versions through the PK index |
| Aggregating | `PRIMARY KEY (…) WITH (merge = 'aggregate')` and state columns | Rows are partial aggregate states merged by key | State columns (§4) merged at read (`TailMergeExec` / aggregation) and during compaction |
| Retention | `WITH (retention = '180 days' ON ts)` | Rows older than the retention are removed | Worker job: partition drop when whole partitions expire, DVs otherwise |

Example:

```sql
CREATE TABLE llm_calls (
  ts TIMESTAMP(3) WITH TIME ZONE, tenant STRING, model STRING,
  prompt_tokens INT, completion_tokens INT, cost_usd DOUBLE, latency_ms INT,
  trace_id STRING
)
PARTITIONED BY (day(ts))
SORTED BY (tenant, model, ts)
WITH (retention = '180 days' ON ts, dictionary = 'tenant, model');
-- ⇒ Iceberg table: partition day(ts), sort order (tenant, model, ts), Parquet dictionary pages on tenant and model
```

`CREATE TABLE` with these clauses is an Operon DDL extension to DataFusion SQL; the native API has the same resource (`POST|GET /v1/namespaces/{ns}/tables`, `GET|DELETE /v1/namespaces/{ns}/tables/{t}`). A table has one writer class (§03 §2.3): Operon links and ingest, or an external engine (§8).

## 2. Types

Arrow/DataFusion types map to Iceberg types:

| Arrow / DataFusion | Iceberg | Notes |
|---|---|---|
| `Int8…Int64`, `UInt8…UInt32` | `int` / `long` | `UInt32` → `long` |
| `UInt64` | `decimal(20,0)`, or `long` with an overflow check | configurable |
| `Float32` / `Float64` | `float` / `double` | |
| `Decimal128(P,S)` | `decimal(P,S)` | P ≤ 38 |
| `Utf8`, `Binary`, `FixedSizeBinary(n)` | `string`, `binary`, `fixed(n)` | dictionary encoding is a Parquet hint, not a type |
| `Date32`, `Timestamp(µs[, tz])` | `date`, `timestamp` / `timestamptz` | ns precision needs an Iceberg v3 table |
| UUID (extension type) | `uuid` | |
| `List`, `Map`, `Struct` | `list`, `map`, `struct` | |
| JSON | `variant` | requires an Iceberg v3 table |
| Aggregate state (§4) | `binary` (serialized state) | Operon-readable only; external engines see opaque bytes |

## 3. Ingest paths

1. **Native API:** `POST /v1/namespaces/{ns}/tables/{t}/rows` with JSON, NDJSON or Arrow IPC → the table's implicit stream → link → Iceberg. The response carries a consistency token.
2. **Flight `DoPut`:** Flight SQL bulk ingest (`CommandStatementIngest`, what ADBC's ingest API sends) with a target in the `tables` schema, as for collections and streams (D49, §02 §7).
3. **SQL:** `INSERT INTO t VALUES …` and `INSERT INTO t SELECT …` over Flight SQL or the native API.
4. **Stream → table links** from explicit streams (native streaming API, §02 §7) and system streams such as agent telemetry (§16 §6.1).
5. **Materialized views** (§4) from other tables and streams.
6. **Bulk load:** register existing Parquet files into the table (add-files), or `INSERT … SELECT` from external Parquet on object storage (Phase B).

Implicit streams of tables use the `arrow` segment encoding (§02 §5): the link reads only the columns it writes and skips JSON decoding, and the T3 tail holds the same Arrow batches.

**CDC out:** keyed tables can expose a changelog stream (§02 §8.1, M5), read through the native streaming API, so downstream consumers and rollups see updates and deletes, not just inserts.

## 4. Materialized views

A materialized view is a link with a SQL transform (§09): per input batch it computes rows or partial aggregate states and appends them to its target table, exactly once via link offsets.

```sql
CREATE TABLE cost_daily (
  day DATE, tenant STRING, model STRING,
  cost  STATE sum(DOUBLE),
  calls STATE count(),
  p95   STATE quantile(0.95, INT)
) PRIMARY KEY (day, tenant, model) WITH (merge = 'aggregate');

CREATE MATERIALIZED VIEW cost_by_day TO cost_daily AS
SELECT date_trunc('day', ts) AS day, tenant, model,
       sum(cost_usd) AS cost, count(*) AS calls, quantile(0.95, latency_ms) AS p95
FROM llm_calls GROUP BY 1, 2, 3;

SELECT day, tenant, finalize(cost), finalize(calls), finalize(p95) FROM cost_daily;  -- merged states, finalized
```

- Supported (Phase A): stateless projections, filters and UDFs, and **mergeable aggregate states**: `count`, `sum`, `min`, `max`, `avg`, `approx_distinct` (HLL), bounded exact distinct, `quantile(s)` (t-digest/DDSketch), `arg_min`/`arg_max`, bounded `array_agg`.
- Semantics: an aggregate MV's target is an aggregating keyed table (§1); states are merged at read and during compaction, and `finalize` turns a merged state into its value.
- Out of scope: MV joins with mutable dimension tables beyond dictionary-style lookups (Phase B), window views, refreshable MVs (Phase B, as scheduled `INSERT … SELECT`).

## 5. SQL surface

- **Dialect:** DataFusion SQL with Operon's DDL extensions and UDFs (§05 §8). One dialect for tables, collections, streams and graphs.
- **Transports:** Arrow Flight SQL (ADBC drivers for Python, Go, Java and C; the Flight SQL JDBC driver), the native API (`POST /v1/namespaces/{ns}/sql`) and the Python/TypeScript SDKs.
- **BI and dashboards:** tools with a Flight SQL or ADBC connector (for example Grafana's Flight SQL data source, Superset and Metabase through the Flight SQL JDBC driver; verify per tool). Tools that only speak another engine's protocol use that engine over the Iceberg REST catalog (§8).
- **System tables:** `information_schema`, `system.tables`, `system.columns`, `system.files` (Iceberg data files and DVs of the current snapshot), `system.snapshots`, `system.query_log` (from Operon's query-log stream).
- Time travel: `SELECT … FROM t FOR SYSTEM_TIME AS OF <timestamp>` and `FOR SYSTEM_VERSION AS OF <snapshot_id>` read an older Iceberg snapshot (verify syntax against DataFusion's parser).

## 6. Mutations and deletes

- `DELETE FROM t WHERE …` → deletion-vector writes for matching rows (a worker job; synchronous for small predicates).
- `UPDATE t SET … WHERE …` → upserts on keyed tables; on append tables, a DV for the old rows plus appended new rows (merge-on-read).
- Deletion vectors are Iceberg v3 Puffin DVs written by Operon's DV writer (§03 §2.3); compaction folds them into rewritten files.
- Schema evolution: `ALTER TABLE … ADD/DROP/RENAME COLUMN`, type widening → Iceberg schema evolution (no rewrite). Partition and sort-order evolution for new data.

## 7. Performance strategy

1. **Layout:** sort by the table's sort key within files; partition pruning; Parquet page index and bloom filters on declared columns; 128–512 MiB files via compaction.
2. **T0 file index** for zero-I/O pruning (§04 §3.1).
3. **T1 Parquet data cache** in foyer (RAM → NVMe), with coalesced range reads (§04 §3.2).
4. **T2 hot projections** for pinned or hot partitions: sparse PK index, skip indexes, aggregate projections (§04 §3.3).
5. **T3 tail** for sub-second freshness (§04 §3.4).
6. **Distributed execution** across hot-tier owners for large scans (§05 §6).

Skip indexes and aggregate projections are declared per table and built only in the hot tier (Iceberg files are unchanged):

```sql
ALTER TABLE llm_calls ADD INDEX trace_bloom (trace_id) TYPE bloom;
ALTER TABLE llm_calls ADD PROJECTION cost_by_tenant AS
  SELECT date_trunc('day', ts) AS day, tenant, count(*), sum(cost_usd) FROM llm_calls GROUP BY 1, 2;
ALTER TABLE llm_calls SET HOT (partitions => 'last 7 days');
```

Targets: ClickBench (hot, on hot projections) median query within 2–3× of ClickHouse OSS on equal hardware in v1, with ClickHouse as a performance reference only; TPC-H SF100 for join-heavy workloads (DataFusion baseline).

## 8. External engine access

Every table is a standard Iceberg table in Lakekeeper. DuckDB, Trino, Spark, Sail (a named M4 gate reader; Operon contributes its deletion-vector reads upstream, D55), ClickHouse (through its Iceberg REST catalog support; verify version), Snowflake, StarRocks, PyIceberg, Ray Data and Polars read it (retained dataset tags are Iceberg tag refs, D52), and may write it; Operon's T0 cache detects external snapshots via Lakekeeper events or polling. External writers bypass Operon's tail and links; Operon treats their commits as new snapshots. A table has one writer class (§03 §2.3): tables written by an external engine, such as a Spark or Flink job, are not Operon link targets. They still get the Iceberg hot tier and Operon's SQL surface, with freshness equal to the external engine's commit cadence. Access control for external engines is Lakekeeper's (credential vending and its authorization model); how Operon's namespace RBAC maps onto it is settled in the M4 plan (verify).

## 9. Benchmarks and gates (M4, §12)

- ClickBench (hot) over Flight SQL: median query within 2–3× of ClickHouse OSS on equal hardware.
- TPC-H SF100 completes.
- Spark, Trino, DuckDB and ClickHouse read Operon tables through Lakekeeper's Iceberg REST catalog.
- Query results match DuckDB over the same Iceberg tables (differential, §12 §2).
