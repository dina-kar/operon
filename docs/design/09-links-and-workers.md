# 09 — Links & Workers

Status: **Approved** · 2026-09-22

**Links** are Operon's zero-ETL mechanism: declared, continuously maintained materializations from streams into tables, collections and graphs. **Workers** are the stateless pool that executes links and all other background work.

---

## 1. Link types

| Link | Example | Target commit |
|---|---|---|
| `stream → table` | Kafka topic `llm_calls` → Iceberg table | Iceberg snapshot (via Lakekeeper) |
| `stream → collection` | Topic `support_tickets` → searchable, embedded collection | Collection manifest |
| `table → collection` | Search projection of `products(title, description)` | Collection manifest |
| `table/collection → graph` | Edge table `knows` → adjacency sidecars | Graph manifest |
| `table → table` | Materialized view / rollup (§08 §4) | Iceberg snapshot |
| `table/collection → changelog stream` | Row-level changes of `tickets` as a stream (§02 §8.1) | Fenced append to the changelog stream, before the target commit |
| `durable_events → table/graph` | Durable-execution search index and execution graph (§14 Phase B) | Iceberg snapshot / graph manifest |
| implicit | Every table/collection/graph's own implicit stream → itself | per target |

```sql
CREATE LINK tickets_search
  FROM STREAM support_tickets
  INTO COLLECTION tickets
  TRANSFORM (
    SELECT key AS _pk,
           json_get_str(value, '$.subject') AS subject,
           json_get_str(value, '$.body')    AS body,
           embed('endpoint:text-embedding-3', json_get_str(value, '$.body')) AS body_vec,
           timestamp AS ts
  )
  WITH (batch_bytes = '64MiB', batch_interval = '2s', on_error = 'dead_letter');
```

## 2. Transforms

- SQL (DataFusion) over the decoded record batch: projections, filters, JSON extraction, casts, scalar UDFs.
- Decoders: JSON, Avro/Protobuf (with schema registry), CSV, raw bytes.
- **`embed()` UDF (optional):** calls an external embedding endpoint (OpenAI-compatible HTTP, or a self-hosted model server) with batching, retries and rate limiting; results cached by content hash. Off by default; configured per namespace. (Operon does not host models.)
- Mergeable aggregate states for MV-style links (§08 §4).
- Not supported: stateful joins across streams, windowed aggregations with watermarks (use an external stream processor against the Kafka surface).

## 3. Exactly-once semantics

1. A link task leases `(link_id, source_partition_range)` in meta with an epoch.
2. It reads from the last **applied offset** recorded in the target's latest commit (Iceberg snapshot summary property / collection or graph manifest field).
3. It builds target files, then commits the target **including the new applied offsets**.
4. A zombie task (stale epoch) fails its commit: Iceberg optimistic concurrency / manifest-pointer CAS rejects it; leases are also fenced by epoch.
5. Restart = resume from the committed applied offset. Duplicate work after crashes is discarded, never double-applied.
6. A link with a changelog appends change records before the target commit, fenced by epoch and by the changelog's recorded `source_upto`; the retried task skips what is already appended (§02 §8.1).

## 4. Errors, dead letters, schema evolution

- `on_error`: `fail` (pause link, alert), `skip`, or `dead_letter` (write offending records + error to a DLQ stream).
- Source schema changes: additive fields auto-propagate if the target allows (Iceberg schema evolution, collection dynamic mapping); incompatible changes pause the link with a clear error.
- Link lag (offsets and seconds) exported as metrics and exposed in `system.links`.

## 5. Worker task catalog

| Task | Trigger | Output |
|---|---|---|
| Segmenter | WAL objects pending | Stream segments; WAL deletion |
| Stream compaction | Compacted stream dirty ratio | New segments |
| Retention | Policy schedule | Segment/file deletions |
| Link apply | Source lag > 0 | Target commits |
| Split merge | Split count/size tiers | Merged Tantivy splits |
| Lance compaction / vector index optimize | Fragment count, unindexed rows, centroid drift | New Lance version / index |
| Hot artifact build | Promotion or pin; new manifest version | HNSW / projection artifacts |
| Iceberg compaction | Small files, delete ratio | Rewritten data files (nimtable/iceberg-compaction) |
| Iceberg maintenance | Schedule | Snapshot expiry, orphan cleanup, manifest rewrite |
| Graph sidecar build | New/compacted edge segments | CSR/CSC sidecars |
| Graph algorithms | User job | Result tables/columns |
| GC | Schedule | Unreferenced object deletion (§03 §7) |
| Meta snapshot | Raft log size / schedule | Snapshot to S3 |
| Durable timer sweep (§14 Phase B) | Timer shard lease | Fires due promise/task/schedule deadlines |
| Durable retention (§14) | Policy schedule | Deletes settled origin documents past retention |

## 6. Scheduling

- Tasks are **leases in meta** `(task_key, epoch, owner, deadline)`; workers pull tasks, renew leases, and are fenced by epoch.
- **Priorities:** link apply (freshness SLO) > segmenting > merges/compaction > hot builds > maintenance > GC.
- **Fair share per namespace** with weights; per-namespace caps on concurrent tasks and bytes/s to prevent noisy neighbors.
- **Autoscaling signals:** total link lag (seconds), compaction debt (bytes), queue age per priority.
- **Resource budgets:** CPU/memory per task class; object-store request budgets (PUT/GET rate) per node to stay under prefix limits.

## 7. Backpressure

- If link lag exceeds `max_lag`, the source stream can be configured to **throttle producers** (Kafka quota semantics) or to keep accepting (log absorbs, tail grows).
- Tail memory is bounded per object on query nodes; when exceeded, strong reads fall back to reading the log range directly from segments (slower but correct).
