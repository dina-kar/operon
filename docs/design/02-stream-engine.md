# 02 — Stream Engine

Status: **Approved** · 2026-09-22 · revised 2026-09-25 (native streaming API, D43)

Goal: a partitioned log with **AutoMQ-grade reliability** (RPO 0 on node and AZ loss, seconds-level failover, no data on broker disks) and a choice of latency/cost per stream, reached through Operon's native streaming API — and it is the internal spine for every other object in Operon.

---

## 1. Requirements

| # | Requirement |
|---|---|
| R1 | Acknowledged writes survive loss of any node and any single AZ (RPO 0) for all default classes |
| R2 | Failover in seconds; no partition reassignment data copying (data lives in S3) |
| R3 | Selectable latency per stream: ~500 ms p99 (cheapest) → ~20–50 ms → < 10 ms |
| R4 | Avoid cross-AZ data transfer charges where the class allows |
| R5 | Native produce and consume over HTTP, gRPC and Arrow Flight: idempotent producers, long-poll and streaming reads, named consumers with committed offsets |
| R6 | Serve as the implicit log for tables/collections/graphs, with offsets usable as consistency tokens |

## 2. WAL durability classes

| Class | Mechanism | Ack after | p99 produce (target) | Survives AZ loss | Cross-AZ data $ | Best for |
|---|---|---|---|---|---|---|
| `standard` | Multi-partition WAL objects on **S3 Standard** (or GCS/Azure regional) | PUT success + meta offset commit | 400–600 ms | Yes | None | Bulk ingest, logs, collection/table ingest, cost-first streams |
| `express` | WAL objects written in parallel to **3 zonal buckets** (S3 Express One Zone / GCS Rapid) in 3 AZs; ack on **2 of 3** | 2 PUTs + meta commit | 20–50 ms | Yes | None (object-store writes, not VM-to-VM) | Latency-sensitive streams without running stateful disks |
| `quorum` | **Journal**: openraft group of 3 `log` nodes (1 per AZ), WAL on local NVMe, offloaded to S3 | Majority fsync | 3–10 ms | Yes | Yes (2 replica copies per byte) | Lowest latency; on-prem/MinIO; clouds without zonal object storage |

Notes:
- `express` is Operon's answer to AutoMQ's commercial EBS/Regional-EBS WAL: low latency and multi-AZ durability **without stateful broker disks**. A similar multi-zonal-bucket approach has been described by WarpStream (verify). Express storage is expensive ($0.11/GB-month) but WAL objects live only seconds before offload; Express PUTs are cheaper per request than Standard.
- On Azure, `express` may map to a single zone-redundant Premium block-blob account (verify). If no zonal/low-latency object store exists, `express` is unavailable and `quorum` is the low-latency option.
- A future `blockvol` class (AutoMQ-style EBS WAL with multi-attach failover) can be added behind the same trait; not planned for v1.

## 3. Leaderless write path (`standard`, `express`)

Modeled on WarpStream/Ursa: any `log` node may accept writes for any partition; ordering is assigned by the metastore at commit.

```
producer ──► log node (same AZ, via zone-aware discovery)
               │ 1. buffer batches from all partitions/namespaces
               │    until flush_interval (standard 250 ms / express 5 ms) or 8 MiB
               │ 2. PUT wal/<class>/<node>/<ulid>.wal   (If-None-Match: *)
               │    express: PUT to 3 zonal buckets in parallel, wait for 2
               │ 3. meta.CommitWal{object, chunks:[(partition, count, bytes, producer_seq…)]}
               │    → sequencer assigns base offsets, checks idempotence, appends index entries
               │ 4. ack each produce with its base offset (+ consistency token)
```

**WAL object format:** `header(magic, version, node_id, ulid, class) | chunk* | chunk_index | footer(crc32c, index_offset)`. Each chunk holds one partition's record batches and names its `encoding` (§5): Kafka's `RecordBatch` v2 byte format by default (a compact, CRC-checked, well-specified batch format), so the segmenter moves batches without re-encoding. Chunks are sorted by `(stream, partition)` (stream ids are cluster-unique). WAL objects live at the cluster level, `wal/<class>/<node_id>/<ulid>.wal`, since one object holds many namespaces (D25). The exact byte layout (format version 1) is in the [M0.3 plan](../plans/2026-09-24-m0.3-log-engine.md#task-2-records-kafka-recordbatch-v2-and-the-wal-object-format).

**Commit window:** `CommitWal` carries the WAL object's creation time (its ULID time). The sequencer dedupes a retried commit by object path, rejects a commit more than 15 minutes older than its clock (`StaleCommit`), and prunes dedupe records after 30 minutes; writers stop starting new commit attempts after 60 s. A retried commit therefore returns the first commit's offsets or is rejected; it is never committed twice (D27). A rejection proves nothing was committed only on a first attempt: after an attempt whose outcome was unknown, the first attempt may have committed the object and its dedupe record may since have been pruned, so the writer reports such a rejection as `CommitUnknown`. The meta leader refuses commands stamped more than 60 s ahead of its own clock, so one node with a fast clock cannot push the metastore clock forward and make every later commit stale (§10 §2).

**Sequencer (in meta):** per partition keeps `next_offset`, high watermark, producer-state table (last 5 batch sequences per producer id, as Kafka does), and an **offset index**: `(base_offset, count, object_ref, byte_range, max_timestamp)`. One Raft proposal per node flush (batched across partitions) keeps meta load proportional to *nodes × flush rate*, not partitions.

**Failure semantics:**

| Crash point | Outcome |
|---|---|
| Before PUT completes | Nothing durable; producer times out and retries |
| After PUT, before meta commit | Orphan WAL object (GC after grace period); producer retries; no duplicates |
| After meta commit, before ack | Data durable; producer retries → idempotent producers dedupe via sequence numbers; producers without ids may duplicate |

**Zone awareness:** any `log` node accepts produce and fetch for any partition, so routing only has to pick a node in the client's AZ. Nodes record their AZ in the node registry; the native API's node discovery (`GET /v1/cluster/nodes?zone=<az>`, used by the SDKs) returns same-AZ `log` nodes, and Kubernetes deployments can use topology-aware Service routing instead. The client's AZ comes from SDK configuration or the cloud instance metadata. Produce and fetch therefore never cross AZs for `standard`/`express`.

## 4. Quorum write path (`quorum`)

```
producer ──► journal leader (log node)
               │ 1. append RecordBatch to Raft log (openraft), replicate to 2 followers (other AZs)
               │ 2. commit on majority fsync → assign offsets deterministically in state machine
               │ 3. ack
               │ background: seal WAL range → write segment to S3 → commit segment index to meta
               │             → Raft log truncated up to sealed offset (snapshot = segment pointers)
```

- A **journal** hosts many partitions; partitions are assigned to journals by the placement controller in meta.
- Failover = Raft election (target 1–3 s). Acknowledged data is on ≥2 AZs ⇒ RPO 0.
- Followers serve fetches of committed data, so consumer reads stay AZ-local.
- Producers reach the leader, which is cross-AZ for ~2/3 of producers unless placement co-locates leaders with producer AZs (placement hint per stream). Document the cross-AZ cost clearly; it is the price of < 10 ms.
- Partition move between journals: seal + offload to S3, flip ownership in meta; no bulk copy.

## 5. Segmenting and storage

- **Segmenter** (worker task) rewrites WAL chunks into per-partition **segments** (64–256 MiB target), with a footer holding a per-batch offset and timestamp index. Commit = atomic, lease-fenced swap of index entries in meta: a segment becomes **one** index entry covering its data region (D26). The metastore retires WAL objects once no index entry references them, and GC deletes them after a grace period (D27). The segment byte layout (format version 1) is in the [M0.3 plan](../plans/2026-09-24-m0.3-log-engine.md#task-3-segment-format).
- `quorum` journals write segments directly on seal.
- Segments keep the WAL's batch bytes verbatim ⇒ segmenting never re-encodes, and fetch decodes straight from the cached byte range. This holds for both encodings below: the WAL writes each stream's `encoding` directly (an `arrow` stream's WAL chunks already hold Arrow IPC batches), so the segmenter only regroups batches and adds the footer.
- **Segment encodings** (idea from Apache Fluss's columnar log tables). WAL chunks and segments carry an `encoding` field:
  - `kafka` (default): the `RecordBatch` v2 byte format, as above. Explicit streams use it.
  - `arrow`: Arrow IPC record batches, for streams with a registered schema, which includes the implicit streams of tables and collections. The footer adds per-column byte ranges, so link apply and tail readers fetch only the columns they project (column pruning on the log itself) and skip JSON decoding. Record-oriented reads of an `arrow` stream (HTTP and gRPC fetch) build records on the fly, while Flight `DoGet` serves it without conversion, so the encoding is chosen per stream by who reads it most.
  - The field is reserved from the first format version (M0.3); `arrow` ships with stream → table links (M4), with an earlier evaluation for collection implicit streams (M1).
- **Retention:** time/size policies trim partitions metadata-first (the log start moves forward; wholly trimmed index entries are dropped and their objects retired), objects are deleted after grace. Size retention never trims a partition's newest index entry, as Kafka never deletes the active segment.
- **Compacted streams:** a compaction task per partition range keeps the latest record per key, honoring tombstone retention (`tombstone_retention`), producing new segments and swapping index entries.

## 6. Read path

Fetch `(partition, offset, max_bytes)` from any `log` or `query` node in the consumer's AZ:

1. Resolve via the node's cached offset index (kept current by meta watch streams).
2. Serve from, in order: in-memory tail cache → NVMe cache → WAL object (range GET) → segment object (range GET).
3. Long-poll: block on meta high-watermark notifications (the metastore's applied-index watch, M0.3) until records arrive or `max_wait_ms` passes. A gRPC `Subscribe` (§7) is the same loop held open.

Read amplification control: reads of recent data are coalesced per WAL object (one GET feeds many consumers/partitions), and the tail cache is populated at write time on the writing node and on AZ peers.

## 7. Native streaming API

The Kafka wire gateway is deferred past v1.0 (D43). Streams are reached through Operon's own API; the HTTP produce and long-poll fetch routes exist from M0.3, and M5 adds the rest. Streams and namespaces are addressed by name.

| Feature | Design | Phase |
|---|---|---|
| Stream admin | `POST /v1/namespaces/{ns}/streams` (name, partitions, retention), `GET /v1/namespaces/{ns}/streams/{stream}` (per-partition log start and high watermark, retention); adding partitions, compaction and retention changes and deletion are metastore operations | M0.3 (create, describe); M5 (rest) |
| Produce (HTTP) | `POST /v1/namespaces/{ns}/streams/{stream}/partitions/{p}/records` with JSON records (`key`/`value` base64, `headers`, `timestamp_ms`); the response carries `base_offset`, `last_offset` and a consistency token | M0.3 |
| Long-poll fetch (HTTP) | `GET …/partitions/{p}/records?offset=&max_bytes=&max_wait_ms=` (§6); at the high watermark it waits up to `max_wait_ms` (at most 60 s) and returns empty, not an error | M0.3 |
| gRPC | `Produce` (unary and client-streaming) and `Fetch` with the HTTP semantics and raw bytes instead of base64; `Subscribe` (server-streaming) pushes batches from a start position: an offset, `earliest`, `latest`, a timestamp, or a named consumer's committed offset | M5 |
| Idempotent producers | `InitProducer` (HTTP `POST /v1/namespaces/{ns}/producers`) returns a `producer_id` and an `epoch`; re-initializing under the same producer name bumps the epoch and fences the older instance. Each produce carries `(producer_id, epoch, sequence)` per partition. The sequencer (§3) dedupes by producer id and sequence: a retried batch returns its original offsets without appending, a sequence gap is rejected (`out_of_order_sequence`), a stale epoch is rejected (`fenced`). Producer state expires after an idle TTL (default 24 h) | M5 |
| Named consumers | A consumer is a namespace object whose committed offsets `(consumer, stream, partition) → offset` live in the metastore, committed and read through `…/consumers/{name}/offsets` (gRPC `CommitOffsets`, `GetOffsets`). Commits are coalesced per consumer and partition and batched into metastore proposals (§9). There is no membership or rebalance protocol: instances read the partitions they are given, and for exclusive ownership an instance takes a per-partition lease (the lease-and-epoch mechanism of worker tasks, §09 §6), which fences its offset commits. Lag (high watermark − committed offset) is exported as a metric | M5 |
| Flight bulk ingest | Flight SQL's bulk-ingest `DoPut` (`CommandStatementIngest`; the ADBC Flight SQL Go driver sends it for bulk ingest since ADBC Libraries 22, apache/arrow-adbc#3808, and the Python `adbc-driver-flightsql` wraps that driver; the driver's documentation page still says bulk ingest is not implemented) with a target in the `streams` schema appends rows as records: columns `key`, `value`, `headers`, `timestamp` and an optional `partition` (else by key hash) for `kafka` streams; `arrow` streams take batches of their registered schema as-is (from M4). `CommandStatementIngest` returns only a row count; a plain `DoPut` with the path descriptor `["streams", s(, p)]` returns one `PutResult` per batch whose `app_metadata` carries the consistency token and offsets (D49; M1.2 Task 13) | M1.2 |
| Flight replay | `DoGet` with a ticket naming a stream, partitions and an offset or timestamp range returns Arrow batches (`partition`, `offset`, `timestamp`, `key`, `value`, `headers`; the registered schema for `arrow` streams) | M5 |
| Auth | API tokens, TLS/mTLS and namespace-scoped RBAC apply to every route (§10 §4) | M2 |

Not offered: multi-partition transactions and server-side consumer-group assignment.

## 8. Streams as the internal spine

- Every table, collection and graph has an **implicit stream** (default class `standard`; configurable). Writes via the native API, Flight `DoPut` and the Qdrant and ES gateways are appended there, then materialized by links (§09).
- Explicit streams can be linked to tables/collections, so a stream *is* queryable without connectors.
- Offsets returned to writers form **consistency tokens** (§01 §5).

### 8.1 Changelog streams (idea from Apache Fluss)

Every keyed table and collection can expose a **changelog stream**: one record per row-level change, read through the native streaming API (§7) like any other stream.

```sql
CREATE STREAM tickets_changes AS CHANGELOG OF COLLECTION tickets
  WITH (mode = 'full');          -- or 'upsert'
```

| Mode | Records | Cost |
|---|---|---|
| `upsert` | `+U` (new row) and `-D` (key) | No extra reads |
| `full` | `+I`, `-U` (before image), `+U` (after image), `-D` (before image) | One read of the old row per update, located through the PK index (§03 §5) and usually cached |

- **Who writes it:** the link-apply worker that resolves upserts and deletes through the PK index already knows the old row, so it appends the batch's change records to the changelog stream before it commits the target.
- **Exactly-once:** the sequencer records, per changelog partition, the highest source offset already appended (`source_upto`). The append is **fenced**: it is accepted only if the worker's lease epoch is current and the batch covers source offsets starting at `source_upto + 1`. The changelog commits before the target, so a crash between the two re-runs the apply; the retried worker reads `source_upto`, appends only changes beyond it, and then commits the target. No change is lost or duplicated.
- **Ordering:** per key, changelog order equals commit order of the source partition. The changelog is partitioned like its source.
- **Uses:** syncing agent memory to caches and external systems, CDC out of Operon (any native-API or Flight client can consume it), and incremental consumers inside Operon (graph links, rollups) that need deletes and before images.
- **Record format:** rows with a row-kind column (`+I`, `-U`, `+U`, `-D`) and the primary key, as Arrow through Flight `DoGet` and links, or as JSON records (key = primary key) over HTTP and gRPC.
- Phase: M5 (collections and keyed tables), with the native streaming API.

## 9. Capacity and cost sketch

Example: 1 GiB/s ingest, 20 `log` nodes, `standard` class, 250 ms flush.

| Item | Estimate |
|---|---|
| WAL PUTs | 20 nodes × 4/s = 80 PUT/s ≈ 207 M/month ≈ **$1.0k/month** |
| Meta proposals | ~80/s WAL commits + offset commits (coalesced) — well within a 3-node Raft group |
| Storage | S3 Standard at $0.023/GB-month × retained bytes (no replication multiplier) |
| Cross-AZ | ~0 with zone-aware routing |

Tuning `flush_interval` trades PUT cost against latency; `express` trades storage/upload fees against latency; `quorum` trades cross-AZ transfer (≈ $0.02/GB for two replica copies, AWS pricing) against latency.

## 10. Open questions

1. Meta scaling beyond one Raft group: shard sequencer state by namespace range (multi-Raft) — design at M6.
2. `express` on GCS Rapid and Azure: confirm conditional-write and append semantics per provider.
3. Whether named consumers need server-side partition assignment beyond per-partition leases.
4. `arrow` encoding: Arrow IPC per chunk vs. one Arrow file per segment with a column index (either layout keeps the WAL's batch bytes, §5).
5. Changelog retention default (same as the source's implicit stream, or shorter) and whether `full` mode is allowed on collections with large documents.
