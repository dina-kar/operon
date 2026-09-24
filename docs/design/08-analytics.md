# 08 — Analytics (ClickHouse Pillar)

Status: **Approved** · 2026-09-22

Tables replace ClickHouse for AI-app analytics (product analytics, LLM usage/cost, traces, evals, observability). Storage is **Apache Iceberg** (open to every lakehouse engine); speed comes from DataFusion + the Iceberg hot tier (§04 §3); freshness comes from the tail.

---

## 1. Table semantics: MergeTree family → Iceberg

ClickHouse DDL is accepted and mapped:

| ClickHouse engine | Operon table semantics | Implementation |
|---|---|---|
| `MergeTree` | Append-only table, sort key = `ORDER BY`, partition spec = `PARTITION BY` | Iceberg sort order + partition spec; hot projections sorted by key |
| `ReplacingMergeTree(ver)` | Keyed table; latest version per key wins | PK index + deletion vectors on upsert (§03 §2.3); `FINAL` is implicit (always deduplicated) |
| `SummingMergeTree` / `AggregatingMergeTree` | Rows are partial aggregate states merged by key | Stored as aggregate-state columns; merged at read (`TailMergeExec`/aggregation) and during compaction |
| `CollapsingMergeTree` / `VersionedCollapsingMergeTree` | Phase B: sign-based collapse at read/compaction | — |
| `Kafka` engine + materialized view | **Link** `stream → table` with transform (§09) | Native; no polling engine table |
| `Distributed`, `ON CLUSTER`, `Replicated*` | Accepted; no-op (storage is shared) | — |
| `TTL` | Retention/transform policy executed by workers | Row-level delete via DVs or partition drop |

Example:

```sql
CREATE TABLE llm_calls (
  ts DateTime64(3), tenant LowCardinality(String), model LowCardinality(String),
  prompt_tokens UInt32, completion_tokens UInt32, cost_usd Float64, latency_ms UInt32,
  trace_id String
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (tenant, model, ts)
TTL ts + INTERVAL 180 DAY;
-- ⇒ Iceberg table: partition day(ts), sort (tenant, model, ts); retention 180 d
```

## 2. Type mapping (ClickHouse → Iceberg/Arrow)

| ClickHouse | Iceberg | Notes |
|---|---|---|
| `Int8…Int64`, `UInt8…UInt32` | `int`/`long` | UInt32 → long |
| `UInt64` | `decimal(20,0)` or `long` with overflow check | configurable |
| `Float32/64` | `float`/`double` | |
| `Decimal(P,S)` | `decimal(P,S)` | P ≤ 38 |
| `String`, `FixedString` | `string` / `binary`/`fixed` | |
| `LowCardinality(T)` | `T` + dictionary encoding hint | Parquet dictionary pages |
| `Date`, `Date32`, `DateTime`, `DateTime64(p, tz)` | `date`, `timestamp`/`timestamptz` (µs; ns in v3) | |
| `UUID` | `uuid` | |
| `Enum8/16` | `string` + check | |
| `Nullable(T)` | optional `T` | |
| `Array(T)`, `Map(K,V)`, `Tuple(...)` | `list`, `map`, `struct` | |
| `JSON` / `Object('json')` | `variant` (Iceberg v3) | requires v3 table |
| `IPv4/IPv6` | `int`/`fixed(16)` + logical annotation | |
| `AggregateFunction(f, T)` | `binary` (serialized state) | Operon-readable only; external engines see opaque bytes |

## 3. Ingest paths

1. **ClickHouse HTTP `INSERT`** (`INSERT INTO t FORMAT JSONEachRow|CSV|TSV|RowBinary|Parquet|Arrow`) → table's implicit stream → link → Iceberg.
2. **Kafka topic → table link** (replaces Kafka engine + MV).
3. **Materialized views** (§4) from other tables/streams.
4. **Bulk load**: register existing Parquet files into the Iceberg table (add-files) or `INSERT … SELECT` from `s3()`/`url()` table functions (Phase B).

Implicit streams of tables use the `arrow` segment encoding (§02 §5): the link reads only the columns it writes and skips JSON decoding, and the T3 tail holds the same Arrow batches.

**CDC out:** keyed tables (`ReplacingMergeTree`) can expose a changelog stream (§02 §8.1), so downstream consumers and rollups see updates and deletes, not just inserts.

## 4. Materialized views

ClickHouse MVs are insert-triggered transforms; Operon implements them as links with a SQL transform:

```sql
CREATE MATERIALIZED VIEW cost_by_day TO cost_daily AS
SELECT toDate(ts) AS day, tenant, model,
       sumState(cost_usd) AS cost, countState() AS calls, quantileState(0.95)(latency_ms) AS p95
FROM llm_calls GROUP BY day, tenant, model;
```

- Supported (Phase A): stateless projections/filters/UDFs, and **mergeable aggregate states**: `count`, `sum`, `min`, `max`, `avg`, `uniq` (HLL), `uniqExact` (bounded), `quantile(s)` (t-digest/DDSketch), `argMin/argMax`, `groupArray` (bounded).
- Semantics: per input batch, compute partial states → append to target (an AggregatingMergeTree-style table) → merged at read and compaction. Exactly-once via link offsets.
- Out of scope: MV joins with mutable dimension tables beyond dictionary-style lookups (Phase B), window views, refreshable MVs (Phase B, as scheduled `INSERT … SELECT`).

## 5. ClickHouse HTTP interface

- Endpoint compatible with port 8123 semantics: `GET/POST /?query=…`, body as query or data, `database`, `default_format`, `query_id`, `session_id` (temporary settings), `settings` params, `X-ClickHouse-*` headers, `/ping`, `/replicas_status` (synthetic).
- Formats (Phase A): `JSON`, `JSONEachRow`, `JSONCompact`, `TabSeparated(WithNames)`, `CSV(WithNames)`, `RowBinary(WithNamesAndTypes)`, `Parquet`, `Arrow`, `ArrowStream`, `Pretty` (subset). Phase B: `Native` format over HTTP.
- SQL: `sqlparser-rs` `ClickHouseDialect` → DataFusion, plus a **function-compat UDF library** prioritized by usage in target clients/dashboards: `toStartOfInterval`, `toDate`, `toStartOfHour`, `dateDiff`, `formatDateTime`, `if`, `multiIf`, `countIf`/`sumIf`/`avgIf`, `uniq`, `quantile(s)`, `arrayJoin`, `has`, `arrayMap` (lambdas), `JSONExtract*`, `splitByChar`, `match`/`extract` (regex), `any`, `argMax`, `groupArray`, `topK`, `runningDifference`, `neighbor`.
- System tables (subset): `system.tables`, `system.columns`, `system.databases`, `system.parts` (synthetic from Iceberg files), `system.query_log` (from Operon query log stream).
- **Native TCP protocol (9000):** Phase C, only if client demand justifies it (many tools — Grafana plugin, clickhouse-connect, Metabase driver, JDBC — work over HTTP; verify per tool).

Conformance: clickhouse-connect (Python) and clickhouse-js test subsets; Grafana ClickHouse datasource over HTTP; Metabase/Superset via HTTP drivers; ClickBench query set.

## 6. Mutations

- `ALTER TABLE … DELETE WHERE` / lightweight `DELETE FROM` → DV writes for matching rows (worker job; synchronous for small predicates).
- `ALTER TABLE … UPDATE` → rewrite affected rows as upserts (keyed tables) or copy-on-write file rewrites (append tables).
- Schema evolution: `ADD/DROP/RENAME COLUMN`, type widening → Iceberg schema evolution (no rewrite).

## 7. Performance strategy

1. **Layout:** sort by `ORDER BY` key within files; partition pruning; Parquet page index + bloom filters on declared columns; target 128–512 MiB files via compaction.
2. **T0 file index** for zero-I/O pruning (§04 §3.1).
3. **Hot projections** for pinned/hot partitions: sparse PK index, skip indexes, aggregate projections (§04 §3.3).
4. **Tail** for sub-second freshness (§04 §3.4).
5. **Distributed execution** across hot-tier owners for large scans (§05 §6).

Targets: ClickBench (hot, on hot projections) within 2–3× of ClickHouse OSS on equal hardware for the median query in v1; TPC-H SF100 for join-heavy workloads (DataFusion baseline).

## 8. External engine access

Every table is a standard Iceberg table in Lakekeeper: Spark, Trino, DuckDB, Snowflake, StarRocks, PyIceberg read it (and may write it; Operon's T0 cache detects external snapshots via Lakekeeper events/polling). External writers bypass Operon's tail and links; Operon treats their commits as new snapshots.
