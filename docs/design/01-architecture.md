# 01 — Architecture: Data Model and System Shape

Status: **Approved** · 2026-09-22 (including amendments: Tantivy for text, Lance + hot HNSW for vectors, hot tier for Iceberg)

---

## 1. Design principles

1. **Object storage is the only durable source of truth.** The sole exception is the seconds-long WAL tail of `quorum`-class streams, which is 3-way replicated across AZs before acknowledgment (§02).
2. **All compute is stateless.** Any node can be killed at any time. Local RAM/NVMe hold only caches and derived structures that can be rebuilt from object storage.
3. **The log is the spine.** Every mutation enters through a stream. Tables, collections and graphs are materializations of streams, maintained by links.
4. **Open formats at rest.** Iceberg (tables), Lance (collection documents + vectors), Tantivy splits (text), Parquet-style sidecars (graph adjacency). External engines can read Operon's data without Operon.
5. **Namespace is the unit of everything.** Tenancy, quotas, encryption keys, cache affinity, routing and billing are all per namespace. A cold namespace costs only its S3 bytes.
6. **Compatibility is a gateway concern.** Protocol frontends translate into a small set of internal logical operations. No protocol leaks into the storage or query core.
7. **Every object has a durable tier and a hot tier** (§04). Correctness never depends on the hot tier.

## 2. Data model

A **namespace** contains five kinds of objects.

### 2.1 Stream (replaces Kafka)
- Partitioned, ordered, offset-addressed log of records `(key, value, headers, timestamp)`.
- Per-stream **WAL class**: `standard` | `express` | `quorum` (§02).
- Retention by time/size, or `compacted` (last value per key).
- **Explicit streams** are created by users (Kafka topics). **Implicit streams** back every table, collection and graph: a write to a collection is appended to that collection's implicit stream first.

### 2.2 Table (replaces ClickHouse)
- Columnar, schema'd, partitioned, with a sort key. Append-only or keyed (upsert/delete).
- Durable format: **Apache Iceberg** (Parquet data files, v3 deletion vectors), catalogued in **Lakekeeper** (Iceberg REST catalog).
- Readable directly by Spark, Trino, DuckDB, Snowflake, StarRocks, etc.

### 2.3 Collection (replaces Elasticsearch + Qdrant)
- Documents keyed by primary key, with any mix of: text fields, keyword/numeric/date fields, dense vectors (possibly several named), sparse vectors, JSON payload.
- Durable format: one **Lance** dataset (documents, vectors, scalar indexes, IVF vector index) **plus** a set of **Tantivy splits** (inverted index, fast fields for aggregations), bound together by a single **collection manifest**.
- Hot tier: Qdrant-derived HNSW on NVMe for pinned/hot collections; pinned splits; in-memory tail index.

### 2.4 Graph (replaces Neo4j)
- A property graph *defined over* tables and/or collections: vertex labels map to a keyed source, edge types map to a source with `(src_key, dst_key)` columns.
- Native Cypher writes land in Operon-managed vertex/edge collections.
- Durable acceleration: forward (CSR) and reverse (CSC) adjacency sidecars per source segment, plus a dense vertex-ID map.

### 2.5 Link (replaces Kafka Connect / Logstash / CDC jobs)
- Declared, continuously maintained materialization with optional stateless transform:
  - `stream → table` (e.g., topic → Iceberg table; ClickHouse "Kafka engine + MV" equivalent)
  - `stream → collection` (e.g., topic → searchable, embedded collection)
  - `table → collection` (search projection of selected columns + key)
  - `tables/collections → graph` (adjacency maintenance)
- Every link records its **applied offset** atomically with each target commit ⇒ exactly-once materialization and consistency tokens.

## 3. System shape

One binary, `operon`, runs any combination of five roles. All roles except `meta` are stateless; `meta` holds only metadata (Raft-replicated, snapshotted to S3).

```
 clients:  Kafka │ ES REST │ Qdrant REST/gRPC │ Bolt/Cypher │ ClickHouse HTTP │ native gRPC/REST/Flight SQL
                                          │
                              ┌──── gateway role ────┐   protocol → logical ops, auth, rate limits
                              ▼                      ▼
             log role                           query role
             ─ WAL writes (standard/express:    ─ DataFusion planning/execution
               leaderless; quorum: journals)    ─ RAM → NVMe cache (foyer)
             ─ offset sequencing via meta       ─ hot tier: HNSW, pinned splits,
             ─ tail cache for fetch               Iceberg projections, CSR, tails
                              │                      ▲   namespace/object-affinity routing
                              ▼                      │
          ┌───────────────────────── object storage bucket ─────────────────────────┐
          │ WAL objects · log segments · Iceberg (Parquet + metadata) · Lance ·     │
          │ Tantivy splits · adjacency sidecars · manifests · hot-tier artifacts    │
          └──────────────────────────────────────────────────────────────────────────┘
                              ▲
             worker role: segmenting, link apply, index build, compaction,
                          Iceberg commits (via Lakekeeper), GC, hot-artifact builds

             meta role:   embedded Raft (openraft) — namespaces, schemas, stream offsets
                          & segment index, consumer groups, leases, manifest pointers,
                          link state. Pluggable backend: FoundationDB / Postgres.
             catalog:     Lakekeeper (Iceberg REST) — for Iceberg tables; external engines use it too
```

### 3.1 Roles

| Role | Responsibility | State | Scales with |
|---|---|---|---|
| `gateway` | Protocol frontends, authN/Z, request routing, rate limiting | None | Connections, request rate |
| `log` | Accept writes, write WAL, request offset assignment, serve recent fetches; host `quorum` journals | `quorum` WAL tail only (replicated) | Ingest bandwidth |
| `query` | Execute reads/queries; own the hot tier for its routed objects | Cache + hot tier (derived) | Query load, hot data size |
| `worker` | Background tasks (§09) | None (leases in meta) | Ingest volume, index/compaction backlog |
| `meta` | Metadata state machine | Raft log + snapshots (→ S3) | Metadata op rate (sharded later) |

Small deployments run everything in one process (`operon dev` / `operon standalone`); large ones split roles into separately autoscaled pools.

### 3.2 Why the metastore is Raft, not S3 CAS

Streams generate high-rate metadata: offset assignment per flush, consumer offset commits, group membership, partition leases. S3 conditional PUT (tens to hundreds of ms, contended per key) cannot sustain that. The default is therefore a small embedded **openraft** group (KRaft / ClickHouse Keeper style) storing only metadata. Durable object data never flows through it. A backend trait allows FoundationDB or Postgres for very large or managed deployments. Manifest *bodies* stay immutable objects on S3; only *pointers* live in meta.

## 4. Data flow

### 4.1 Write (any protocol)
1. Gateway authenticates and translates the request into a logical write against a stream (explicit or implicit).
2. A `log` node appends to the WAL per the stream's class and obtains dense offsets from meta (or from its journal for `quorum`).
3. The client is acknowledged with a **consistency token** `{(stream, partition, offset)…}`.
4. Workers asynchronously apply links: build Lance fragments + Tantivy splits, append Iceberg data files, update adjacency — each commit atomically records its applied offset.

### 4.2 Read
1. Gateway translates into a logical plan (DataFusion).
2. Plan executes on `query` nodes chosen by affinity routing (§04). For each object, the read = **durable/hot state @ applied offset ∪ tail (applied offset, requested offset]**.
3. Default is **strong consistency** (read sees all acknowledged writes); `eventual` mode skips the tail for lower latency.

## 5. Consistency model

| Scope | Guarantee |
|---|---|
| Within a stream partition | Total order; acknowledged writes durable per WAL class (§02) |
| Single-object reads (default) | Strong: sees all writes acknowledged before the read began (via tail merge) |
| Cross-object reads | Snapshot per object; with a consistency token, guaranteed to reflect the token's offsets in every object that derives from those streams |
| Atomic multi-record writes | Atomic per request within one stream (a `_bulk`, a Cypher statement, a Kafka transaction — §02) |
| External Iceberg readers | See Iceberg snapshots at commit cadence (default 10–60 s); no tail |
| Not provided | Multi-object serializable transactions; interactive OLTP transactions |

## 6. Object storage layout

```
s3://<bucket>/<cluster_prefix>/
  meta/snapshots/<node_id>/<raft_term>-<index>.snap # metastore snapshots, one set per meta node (D17)
  ns/<namespace_id>/
    wal/<class>/<node_id>/<ulid>.wal                # standard/express WAL objects (multi-partition)
    streams/<stream_id>/<partition>/<base_offset>-<ulid>.seg
    collections/<collection_id>/
      lance/…                                        # Lance dataset (data/, _versions/, _indices/)
      text/splits/<ulid>.split                       # Tantivy split bundles (with hotcache footer)
      text/deletes/<split_ulid>/<version>.bitmap     # per-split deletion bitmaps
      manifests/<version>.pb                         # immutable collection manifests
      hot/<artifact_kind>/<source_version>/…         # optional prebuilt hot-tier artifacts (HNSW)
    graphs/<graph_id>/
      idmap/…                                        # SlateDB instance: external key → dense id
      adj/<source_ref>/<segment_ulid>.{csr,csc}
      manifests/<version>.pb
    pk/<object_id>/…                                 # SlateDB instance: primary key → row location
  warehouse/<namespace_id>/<table_id>/               # Iceberg table location (managed via Lakekeeper)
    metadata/…  data/…
```

All data objects are immutable and named by ULID/version. Only metastore pointers and Iceberg catalog pointers move.

## 7. Failure model (summary)

| Failure | Effect | Recovery |
|---|---|---|
| Any `gateway`/`query`/`worker` node | In-flight requests retried; hot tier for its objects goes cold | Re-route by rendezvous hashing; warm from S3 or prebuilt artifacts |
| `log` node (standard/express) | Unflushed, unacknowledged batches lost (producer retries) | Any node continues; orphan WAL objects GC'd |
| `log` node (quorum) | None for acknowledged data | Raft election in the journal (~1–3 s) |
| One AZ | `standard`/`express`(multi-bucket)/`quorum` survive with RPO 0 | Capacity in remaining AZs |
| Meta minority | None | Raft |
| Meta majority | Writes and metadata-dependent reads stall; cached reads continue | Restore from S3 snapshot + Raft log |
| Object store regional outage | Unavailable | Cross-region replication + meta restore (§10) |
