# 02 — Stream Engine (Kafka Replacement)

Status: **Approved** · 2026-09-22

Goal: a Kafka-compatible log with **AutoMQ-grade reliability** (RPO 0 on node and AZ loss, seconds-level failover, no data on broker disks) and a choice of latency/cost per stream — and it is the internal spine for every other object in Operon.

---

## 1. Requirements

| # | Requirement |
|---|---|
| R1 | Acknowledged writes survive loss of any node and any single AZ (RPO 0) for all default classes |
| R2 | Failover in seconds; no partition reassignment data copying (data lives in S3) |
| R3 | Selectable latency per stream: ~500 ms p99 (cheapest) → ~20–50 ms → < 10 ms |
| R4 | Avoid cross-AZ data transfer charges where the class allows |
| R5 | Kafka wire compatibility sufficient for librdkafka, Java client, franz-go, kcat, Kafka Connect, Flink, Spark |
| R6 | Serve as the implicit log for tables/collections/graphs, with offsets usable as consistency tokens |

## 2. WAL durability classes

| Class | Mechanism | Ack after | p99 produce (target) | Survives AZ loss | Cross-AZ data $ | Best for |
|---|---|---|---|---|---|---|
| `standard` | Multi-partition WAL objects on **S3 Standard** (or GCS/Azure regional) | PUT success + meta offset commit | 400–600 ms | Yes | None | Bulk ingest, logs, collection/table ingest, cost-first topics |
| `express` | WAL objects written in parallel to **3 zonal buckets** (S3 Express One Zone / GCS Rapid) in 3 AZs; ack on **2 of 3** | 2 PUTs + meta commit | 20–50 ms | Yes | None (object-store writes, not VM-to-VM) | Latency-sensitive topics without running stateful disks |
| `quorum` | **Journal**: openraft group of 3 `log` nodes (1 per AZ), WAL on local NVMe, offloaded to S3 | Majority fsync | 3–10 ms | Yes | Yes (2 replica copies per byte) | Lowest latency; on-prem/MinIO; clouds without zonal object storage |

Notes:
- `express` is Operon's answer to AutoMQ's commercial EBS/Regional-EBS WAL: low latency and multi-AZ durability **without stateful broker disks**. A similar multi-zonal-bucket approach has been described by WarpStream (verify). Express storage is expensive ($0.11/GB-month) but WAL objects live only seconds before offload; Express PUTs are cheaper per request than Standard.
- On Azure, `express` may map to a single zone-redundant Premium block-blob account (verify). If no zonal/low-latency object store exists, `express` is unavailable and `quorum` is the low-latency option.
- A future `blockvol` class (AutoMQ-style EBS WAL with multi-attach failover) can be added behind the same trait; not planned for v1.

## 3. Leaderless write path (`standard`, `express`)

Modeled on WarpStream/Ursa: any `log` node may accept writes for any partition; ordering is assigned by the metastore at commit.

```
producer ──► log node (same AZ, via zone-aware Metadata)
               │ 1. buffer batches from all partitions/namespaces
               │    until flush_interval (standard 250 ms / express 5 ms) or 8 MiB
               │ 2. PUT wal/<class>/<node>/<ulid>.wal   (If-None-Match: *)
               │    express: PUT to 3 zonal buckets in parallel, wait for 2
               │ 3. meta.CommitWal{object, chunks:[(partition, count, bytes, producer_seq…)]}
               │    → sequencer assigns base offsets, checks idempotence, appends index entries
               │ 4. ack each produce with its base offset (+ consistency token)
```

**WAL object format:** `header(magic, version, node_id, ulid, class) | chunk* | chunk_index | footer(crc32c, index_offset)`. Each chunk holds one partition's record batches and names its `encoding` (§5): Kafka `RecordBatch` v2 by default, so fetch can serve bytes without re-encoding. Chunks are sorted by `(namespace, stream, partition)`.

**Sequencer (in meta):** per partition keeps `next_offset`, high watermark, producer-state table (last 5 batch sequences per producer id, as Kafka does), and an **offset index**: `(base_offset, count, object_ref, byte_range, max_timestamp)`. One Raft proposal per node flush (batched across partitions) keeps meta load proportional to *nodes × flush rate*, not partitions.

**Failure semantics:**

| Crash point | Outcome |
|---|---|
| Before PUT completes | Nothing durable; producer times out and retries |
| After PUT, before meta commit | Orphan WAL object (GC after grace period); producer retries; no duplicates |
| After meta commit, before ack | Data durable; producer retries → idempotent producers dedupe via sequence numbers; non-idempotent producers may duplicate (same as Kafka) |

**Zone awareness:** Metadata responses advertise same-AZ `log` nodes as leaders for all partitions (using `client.rack` or an AZ-tagged client id), so produce and fetch never cross AZs for `standard`/`express`.

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
- Followers serve fetches of committed data (KIP-392-style follower fetch) to keep consumer reads AZ-local.
- Producers reach the leader, which is cross-AZ for ~2/3 of producers unless placement co-locates leaders with producer AZs (placement hint per stream). Document the cross-AZ cost clearly; it is the price of < 10 ms.
- Partition move between journals: seal + offload to S3, flip ownership in meta; no bulk copy.

## 5. Segmenting and storage

- **Segmenter** (worker task) rewrites WAL chunks into per-partition **segments** (64–256 MiB target), with a footer holding a sparse offset index and timestamp index. Commit = atomic swap of index entries in meta; WAL objects deleted after grace period.
- `quorum` journals write segments directly on seal.
- Segment format keeps Kafka `RecordBatch` v2 bytes verbatim ⇒ zero re-encoding on fetch, zero-copy into responses.
- **Segment encodings** (idea from Apache Fluss's columnar log tables). WAL chunks and segments carry an `encoding` field:
  - `kafka` (default): Kafka `RecordBatch` v2, as above. Explicit topics use it.
  - `arrow`: Arrow IPC record batches, for streams with a registered schema, which includes the implicit streams of tables and collections. The footer adds per-column byte ranges, so link apply and tail readers fetch only the columns they project (column pruning on the log itself) and skip JSON decoding. Kafka fetches of an `arrow` stream re-encode to `RecordBatch` on the fly, so the encoding is chosen per stream by who reads it most.
  - The field is reserved from the first format version (M0.3); `arrow` ships with stream → table links (M4), with an earlier evaluation for collection implicit streams (M1).
- **Retention:** time/size policies delete segments metadata-first, objects after grace.
- **Compacted streams:** a compaction task per partition range keeps the latest record per key, honoring tombstone retention (`delete.retention.ms`), producing new segments and swapping index entries.

## 6. Read path

Fetch `(partition, offset, max_bytes)` from any `log` or `query` node in the consumer's AZ:

1. Resolve via the node's cached offset index (kept current by meta watch streams).
2. Serve from, in order: in-memory tail cache → NVMe cache → WAL object (range GET) → segment object (range GET).
3. Long-poll: block on meta high-watermark notifications until `min_bytes`/`max_wait_ms`.

Read amplification control: reads of recent data are coalesced per WAL object (one GET feeds many consumers/partitions), and the tail cache is populated at write time on the writing node and on AZ peers.

## 7. Kafka protocol semantics

| Feature | Design | Phase |
|---|---|---|
| Produce / Fetch / ListOffsets / Metadata / ApiVersions | Native to the log | M3 |
| Topic admin (Create/Delete/CreatePartitions/Describe/AlterConfigs) | Meta operations | M3 |
| Idempotent producer (InitProducerId, sequences) | Producer state in sequencer / journal state machine | M3 |
| Consumer groups — classic (Join/Sync/Heartbeat/Leave) | Group coordinator runs in `gateway` nodes, state in meta | M3 |
| Consumer groups — KIP-848 (ConsumerGroupHeartbeat) | Server-side assignment; simpler, preferred | M3 |
| Offset commits | Coalesced per group/partition, batched Raft proposals | M3 |
| Compacted topics | §5 | M3 |
| SASL/PLAIN, SCRAM, OAUTHBEARER; mTLS; ACLs | Gateway auth mapped to Operon RBAC | M3 |
| Transactions (AddPartitionsToTxn, EndTxn, TxnOffsetCommit, read_committed/LSO) | Transaction coordinator in meta; control markers written as batches; LSO tracked by sequencer | M5 |
| DeleteRecords, OffsetForLeaderEpoch, DescribeCluster, quotas | Straightforward | M3–M5 |
| Schema Registry (Confluent REST) | Evaluate reuse of Nisshi's schema crate | M5 |
| Not planned | KRaft/ZooKeeper admin internals, MirrorMaker-specific APIs, tiered-storage KIP-405 APIs | — |

Wire encoding/decoding uses the **`kafka-protocol`** crate (generated from Kafka's own schemas). Broker structure borrows from **Nisshi (formerly Tansu)** where useful.

## 8. Streams as the internal spine

- Every table, collection and graph has an **implicit stream** (default class `standard`; configurable). Writes via ES/Qdrant/Cypher/ClickHouse gateways are appended there, then materialized by links (§09).
- Explicit Kafka topics can be linked to tables/collections, so a topic *is* queryable without connectors.
- Offsets returned to writers form **consistency tokens** (§01 §5).

### 8.1 Changelog streams (idea from Apache Fluss)

Every keyed table and collection can expose a **changelog stream**: one record per row-level change, readable through the native API and (from M3) as a Kafka topic.

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
- **Uses:** syncing agent memory to caches and external systems, CDC out of Operon (the Kafka surface makes it consumable by any Kafka client), incremental consumers inside Operon (graph links, rollups) that need deletes and before images, and stream processors such as RisingWave (§09 §8).
- **Wire formats** on the Kafka surface, chosen per changelog so external processors read it with their built-in formats:

  | `format` | Kafka record | Read by |
  |---|---|---|
  | `upsert` (default for `mode = 'upsert'`) | key = primary key; value = after image, or a tombstone (null value) for a delete | RisingWave `FORMAT UPSERT ENCODE JSON`, Flink `upsert-kafka`, compacted-topic consumers |
  | `debezium-json` (default for `mode = 'full'`) | key = primary key; value = Debezium envelope `{before, after, op: c\|u\|d, source, ts_ms}` | RisingWave `FORMAT DEBEZIUM ENCODE JSON`, Flink `debezium-json`, Debezium-aware sinks |
  | `native` | Arrow or JSON rows with a row-kind column (`+I`, `-U`, `+U`, `-D`) | Operon native API, links |

  Avro/Protobuf encodings follow the schema registry (M5).
- Phase: M3 (collections and keyed tables), with the Kafka surface.

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

1. Meta scaling beyond one Raft group: shard sequencer state by namespace range (multi-Raft) — design at M5.
2. `express` on GCS Rapid and Azure: confirm conditional-write and append semantics per provider.
3. Transactions depth required by target users (Flink exactly-once sinks need it; many AI pipelines don't).
4. Whether to vendor Nisshi as a starting point or only borrow patterns (bus factor 1 upstream).
5. `arrow` encoding: Arrow IPC per chunk vs. one Arrow file per segment with a column index; and whether the WAL writes `arrow` directly or the segmenter converts.
6. Changelog retention default (same as the source's implicit stream, or shorter) and whether `full` mode is allowed on collections with large documents.
