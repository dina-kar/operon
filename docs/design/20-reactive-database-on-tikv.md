# 20 — Loams Live: a Reactive Database on TiKV and the TiKV Metastore

Status: **Proposed** · 2026-09-27. The direction (D116–D118, D122–D124, D126–D128) was approved by the owner on 2026-09-27: "build convex like layer on TiKV, and use it for our metadata and control if possible, other than convex like api, can we provide mysql protocol for normal uses, build this as soon as possible, loams will a ai native cloud". The design choices this document makes on top of that direction (D119–D121, D125, D129, D131) are **proposals** until the owner confirms them. Open questions are Q31–Q38 in §16. Measurements and cluster facts marked **(spike)** come from the TiKV feasibility spike of 2026-09-27 on `tiup playground` v8.5.8 with `tikv-client` 0.4.0 (report: `.superpowers/research/tikv-spike.md` in the m1.2a worktree, not committed); its latencies were taken on a heavily loaded shared machine and are indicative only.

This document starts a second product line beside the retrieval engine (§00–§18). It amends D1 and D2 for that product line only (D130), supersedes D58's TiDB-over-sqlx clause (D124), and adds a parallel roadmap track, **R** (D127).

> **Amended 2026-09-29 by D260 (owner): TiKV only, no TiDB.** TiKV is the transactional store for Loams's own state in clusters, Loams cloud and self-hosted deployments: the metastore (`loams-meta-tikv`, §11), the control plane (`ControlStore` on `_control`, §9.5), Loams Live, job state ([§26](26-jobs-api.md)) and durable state ([§21](21-durable-execution.md) §3.3, D261). PD and TiKV run unmodified. (openraft stays the dev and standalone default, and the Postgres and DynamoDB metastore backends of D58 are unchanged.) No TiDB process, pool or keyspace is deployed, and TiCDC, TiProxy and TiFlash go with it. **D123 (MySQL through TiDB) is superseded**, and MySQL wire access is an open question (Q260, §10). The TiDB material below (§10, parts of §9, §13–§15 and §18) is kept as history and marked where it no longer applies. This follows D-SC-16, which dropped TiDB from the showcase suite ([§22](22-showcase-suite.md) §13b).

Markers: **(estimate)** is computed from code or specs, not measured. **(verify)** is not checked against a primary source; the plan that builds it resolves it (R1 Task 0 checks the ones R1 depends on). Paths of the form `tikv/…`, `pd/…`, `tidb/…`, `client-rust/…`, `ticdc/…`, `connect-rust/…` point into the reference clones under `~/Documents/research-clones/` as of 2026-09-26 (TiKV `548812e`, PD `9186d07`, TiDB `8936d7b`, client-rust `ab4be1c`, connect-rust `fb5f5aa`).

---

## 1. Summary

| # | Decision | Status | Track |
|---|---|---|---|
| D116 | Loams is an **AI-native cloud**: the retrieval engine, a reactive application database, SQL, streams and AI-gateway integration | Approved (owner) | — |
| D117 | **Loams Live** (working name "Loam Reactive"), a Convex-style reactive database built on TiKV: reactive queries, server functions, a protobuf sync API, a namespace router over keyspaces, and a bridge into Loams collections | Approved (owner); the name is proposed | R1–R4 |
| D118 | A mutation is **one TiKV optimistic transaction**, retried on conflict; isolation is snapshot isolation with point reads promoted to locks; serializable range reads are open (Q31) | Approved (owner) for the transaction model; the isolation detail is proposed | R1 |
| D119 | Invalidation comes from a **sharded, sequenced commit journal** written inside each mutation's transaction; TiKV CDC (validated from Rust with `kv_api=TiDB`) is a secondary path for consumers that tolerate ~1 s lag | Proposed | R1 |
| D120 | Server functions run in **QuickJS through `rquickjs`** in R1; V8 and wasmtime stay options (Q35) | Proposed | R1 |
| D121 | The sync API is **protobuf over connect-rust** (Connect, gRPC, gRPC-Web): a server-streamed session plus unary calls; clients are generated | Proposed (the transport was the owner's choice) | R1 |
| D122 | **Multi-tenancy by keyspace**, with size classes: large apps and every SQL tenant get their own keyspace; small apps share one under a key prefix | Approved (owner) for the router; the size classes are proposed; "every SQL tenant" is moot under D260 | R2 |
| D123 | ~~**MySQL protocol through unmodified TiDB**, one TiDB pool per SQL-enabled keyspace, on the same TiKV cluster; Loams builds no MySQL server~~ | **Superseded by D260** (2026-09-29): no TiDB; MySQL wire access is open (Q260) | — |
| D124 | **`loams-meta-tikv`**, a `MetaStore` backend over `tikv-client`, moves from M6 to R1 and becomes the backend for Loams cloud metadata; Postgres and DynamoDB stay in v1.0; openraft stays the default | Approved (owner) | R1 |
| D125 | The **control plane's `ControlStore` runs on Loams Live** (a system app in its own keyspace), not on TiDB SQL | Proposed | R2 |
| D126 | **PD, TiKV, TiDB and TiCDC run unmodified** from official releases; tidb-operator on Kubernetes; `tiup playground` in dev and CI; no forks | Approved (owner); amended by D260: PD and TiKV only (no TiDB, TiCDC or TiProxy) | R1, R4 |
| D127 | **Track R** runs beside M1, interleaved on the one-build machine; R1 = TiKV metastore + minimal reactive core ~~+ TiDB SQL in dev~~ | Approved (owner); the TiDB item is dropped by D260 | — |
| D128 | One **proto toolchain** (buffa + connect-rust, `buf` for clients) for Loams Live and the native stream API; M1.6's SDKs should reuse generated clients where possible (a proposed M1.6 amendment) | Approved (owner) for the shared toolchain; the M1.6 amendment is proposed | R1, M2 |
| D129 | The **collections bridge** tails the commit journal into a collection's implicit stream with exactly-once producer sequences, so Live tables become searchable | Proposed | R3 |
| D131 | **TiDB's object-storage, vector and full-text features:** the next-gen S3 kernel is not usable self-hosted; TiFlash is an optional add-on for SQL tenants; TiDB full-text is not used; **BR log backup (PITR) to object storage is mandatory for every Live cluster** | Proposed (owner question, 2026-09-27); the TiFlash add-on is dropped by D260, BR log backup stays | R2 (backup) |
| D260 | **TiKV only, no TiDB** anywhere in the engine or Loams cloud: metastore, control plane, Live, jobs and durable state on TiKV. Supersedes D123; MySQL wire access is open (Q260) | Approved (owner, 2026-09-29) | — |
| D130 | D1 (object storage is the only source of truth) and D2 (OLTP out of scope) **keep holding for the retrieval engine** and do not apply to Loams Live, whose source of truth is TiKV | Proposed | — |

## 2. Goals and non-goals

### 2.1 Goals

1. **Convex's developer model on infrastructure Loams can run and sell.** Documents in tables with indexes; queries that stay live and push new results; mutations that are transactions and retry themselves; actions for side effects. All of it runs on open-source components under Apache-2.0 or MIT (§15).
2. **One cluster for application data and metadata.** A TiKV cluster holds Loams Live apps and Loams's own metadata, control plane, job and durable state, each in its own keyspace (§9). (Until D260 it also held TiDB SQL databases.)
3. **App data becomes retrievable.** A Live table can be declared searchable, and the bridge keeps a Loams collection in step with it, so vector, full-text and hybrid search run over application data without an ETL job (§12). This is the differentiator: Convex has search indexes inside the database, but not Loams's hybrid retrieval, hot tier, Qdrant and Elasticsearch surfaces or scan plans.
4. **Clients on every platform from one contract.** Protobuf services generate clients for the web, iOS, Android, Python and Go (§7).
5. **The TiKV metastore early.** Loams cloud needs a scale-out, transactional metadata store with a managed option. TiKV gives the `MetaStore` contract in full (§11), and the same cluster then serves the control plane (§9.5).
6. **Ship fast.** R1 is small and reuses what exists: the `MetaStore` conformance suite and fault-matrix design (§18 §4), the M1.3 routing, the D111 loopback defaults.

### 2.2 Non-goals

- **No fork of PD or TiKV** (D126). Anything that needs a server change goes upstream or is not done.
- **No TiDB** (D260). ~~No MySQL server of our own (D123); TiDB is the MySQL surface.~~ Whether Loams offers MySQL wire access at all is open (Q260, §10).
- **No SQL over Live tables in R1–R3.**
- **No Convex compatibility.** Loams Live borrows concepts, not the wire protocol, the function API names or any code. Convex's backend is FSL-1.1 and is read for concepts only (§15).
- **Not a replacement for the retrieval engine's log and bucket model.** Collections, streams, tables and graphs keep object storage as the source of truth (D1, D130). Loams Live is the OLTP store beside them.
- **No auth in R1.** It follows D111, more strictly: the Live listener binds 127.0.0.1 and **refuses** any non-loopback address (§7.1); auth arrives with the unified auth plan (R3).
- **No offline-first sync.** Clients hold query results and optimistic updates, not a local replica with merge.

## 3. Architecture

```
   Web / iOS / Android / Python / Go clients
   (generated Connect / gRPC / gRPC-Web stubs
    + a thin reactive client per platform)
                  │ HTTP/1.1 or HTTP/2
                  ▼
 ┌──────────────────────────── loams binary, role `live` ──────────────────┐
 │  Sync API (connect-rust)                                                │
 │   Watch (server stream) · ModifyQuerySet · Query · Mutate               │
 │   · Deploy                                                              │
 │        │                                                                │
 │  Session manager ── query sets, transitions, backpressure               │
 │        │                                                                │
 │  Subscription manager ── read-set index (interval trees),               │
 │        │                  result cache, rerun scheduler                 │
 │        │  ▲ invalidations                                               │
 │  Function runner (QuickJS isolates) ── queries, mutations               │
 │        │                    │                                           │
 │  LiveTxn (read set, write set, retry) │  Journal tailer                 │
 │        │                    │                                           │
 │  Namespace router: app → keyspace (+ prefix)   ── ControlStore (R2) ─┐  │
 └────────┼─────────────────────────────────────────────────────────────┼──┘
          │ tikv-client (txn API v2, keyspace codec)                    │
          ▼                                                             ▼
 ┌──────────────────────────────── TiKV cluster (unmodified) ──────────────────────────────┐
 │ PD: TSO, region placement, keyspace metadata, GC states                                   │
 │ TiKV: Raft-replicated regions, Percolator transactions, MVCC                              │
 │  keyspace loams_meta     keyspace loams_control   keyspace app_…      keyspace loams_jobs,│
 │  (loams-meta-tikv)     (Live system app, R2)   (Live apps)         loams_durable (§21)     │
 └───────────────────────────────────────────────────────────────────────────────────────────┘
          ▲                                     │ journal tail (R3)
          │ MetaStore calls                     ▼
 ┌────────┴─────────────────────┐   ┌──────────────────────────────────────────────┐
 │ Retrieval engine (§01–§18):  │◀──│ Collections bridge: journal → DocOps → the     │
 │ gateways, log, query, worker │   │ collection's implicit stream (exactly once)   │
 │ roles; bucket = truth        │   └──────────────────────────────────────────────┘
 └──────────────────────────────┘
```

- **Loams builds** everything inside the `loams` box, the bridge and the metastore backend. **Loams runs unmodified** PD and TiKV, and no TiDB (D260).
- The Live role is stateless: sessions, subscriptions and the result cache are derived state, rebuilt when a client reconnects (§13).
- One TiKV cluster per region serves every kind of keyspace. Separate clusters per product (a metadata cluster, an app cluster) are an operational choice, not a design requirement.

### 3.1 Crates (R1)

| Crate | Owns |
|---|---|
| `loams-tikv` | The TiKV client layer every TiKV user shares: config and keyspace bootstrap through PD's HTTP API, the TSO clock, a transaction runner with error classification, retries and a `FaultPlan` hook, commit tokens, the order-preserving tuple codec, the keyspace GC loop, the test harness |
| `loams-meta-tikv` | `impl MetaStore` over `loams-tikv` (§11) |
| `loams-live-proto` | The `loams.live.v1` protos and the Rust code generated from them (buffa messages, connect-rust services) |
| `loams-live` | Data model and key layout, `LiveTxn`, the commit journal and tailer, the subscription and session managers, the sync service |
| `loams-live-js` | The QuickJS function runtime (`rquickjs`) and the host database API |
| `sdks/typescript/packages/live` | `@loams/live`: generated protobuf-es and Connect stubs plus the reactive client |

## 4. Data model

### 4.1 Apps, tables, documents

- An **app** is one Live database (one Convex deployment). In the §19 tenancy model an app belongs to an environment: environment = namespace, and a namespace may hold one Live app beside its collections and streams. The app's name is the namespace name.
- A **table** holds **documents**: maps of field name to value. Tables are created implicitly on first insert (dev) or by the deployed schema.
- **System fields:** `_id` (a document id, below) and `_creationTime` (ms since the epoch, from the TSO physical time of the inserting transaction's start timestamp).
- **Values:** `null`, `int64`, `float64`, `bool`, `string`, `bytes`, `array`, `object`. `int64` and `float64` are distinct types (as in Convex), so `1` and `1.0` differ. Limits in R1: 1 MiB per document, 1 024 fields, nesting depth 16, 8 192 array elements **(estimate; TiKV's `raft-entry-max-size` is 8 MiB and a transaction's per-entry limit must stay below it, verify)**.
- **Document ids:** 16 random bytes inside the table's key range. The text form is Crockford base32 of `varint(table_id) ‖ 16 bytes ‖ crc16`, so an id names its table and a client can check that an id belongs to the table it claims (Convex's `normalizeId` idea). Random ids spread writes over the table's regions; a time-ordered id would put every insert on one region.
- **Schema:** optional in R1. A deployed schema lists tables, their indexes and (from R2) validators. Adding an index to a non-empty table is refused in R1; online backfill arrives in R2.

### 4.2 Indexes

- Every table has two built-in indexes: `by_id` (the document key itself) and `by_creation_time`.
- A user index lists up to 16 fields. `_creationTime` and then `_id` are appended to every index, so entries are unique and ordered stably.
- A query on an index is an **index range**: equality on a prefix of the fields, then an optional lower and upper bound on the next field, in either direction, with a limit. Filters beyond the range are applied after the read and still count toward the read set as the whole range (§5.2).

### 4.3 Key encoding

All keys are inside the app's keyspace, which the client codec prefixes with `x` and the 3-byte keyspace id (`tikv/components/api_version/src/api_v2.rs:16-20`; `client-rust/src/request/keyspace.rs:11-13`). A shared keyspace (§7.2) adds an app prefix.

```
app prefix      = ""                              (dedicated keyspace)
                | 0xA0 ‖ app_id:u32 BE            (shared keyspace)

catalog         = prefix ‖ 0x01 ‖ kind:u8 ‖ name                 → TableDef | IndexDef | Deployment pointer | Schema
document        = prefix ‖ 0x02 ‖ table_id:u32 BE ‖ doc_id[16]    → DocumentRecord (protobuf)
index entry     = prefix ‖ 0x03 ‖ table_id:u32 BE ‖ index_id:u32 BE ‖ tuple(values…) ‖ creation_ms:u64 BE ‖ doc_id[16] → ""
journal head    = prefix ‖ 0x04 ‖ 0x00 ‖ shard:u16 BE             → last sequence:u64
journal entry   = prefix ‖ 0x04 ‖ 0x01 ‖ shard:u16 BE ‖ seq:u64 BE → JournalEntry (protobuf)
idempotency     = prefix ‖ 0x05 ‖ key_hash[16]                   → {commit_ts, result, expires_ms}   (key_hash = first 16 bytes of SHA-256(key))
scheduler (R2)  = prefix ‖ 0x06 ‖ …
```

**The tuple codec** is order-preserving (memcomparable), so a TiKV range scan returns index entries in value order:

| Tag | Type | Payload |
|---|---|---|
| `0x05` | null | — |
| `0x10` | int64 | 8 bytes big-endian with the sign bit flipped |
| `0x20` | float64 | 8 bytes: positive values with the sign bit set, negative values with every bit inverted (total order; NaN sorts last, `-0.0` equals `0.0` after normalisation) |
| `0x30` / `0x31` | false / true | — |
| `0x40` | string | UTF-8 bytes with `0x00` escaped as `0x00 0xFF`, terminated by `0x00 0x00` |
| `0x50` | bytes | same escaping as strings |
| `0x60` | array | encoded elements, terminated by `0x00 0x00` |

The order is null < int64 < float64 < bool < string < bytes < array. Objects are not indexable in R1. The codec is fuzzed for order preservation against a reference comparator (R1 Task 8).

**Document bodies** are a protobuf `DocumentRecord { format: 1, creation_ms, fields: map<string, Value> }` from the same `loams.live.v1` package the clients use, so a value means the same in storage, on the wire and in every generated client.

## 5. Transactions and isolation (D118)

### 5.1 The transaction model

- **A mutation is one TiKV optimistic transaction** (`client-rust/src/transaction/client.rs:174`, `begin_optimistic`). The function reads at the transaction's start timestamp, buffers writes, and commits. TiKV's Percolator two-phase commit makes the commit atomic across regions and keyspace ranges.
- **Retry on conflict.** A write conflict at prewrite aborts the transaction; the runner reruns the whole function at a new start timestamp, with jittered backoff, up to 8 attempts or the mutation's deadline. Functions are deterministic (§6.2), so a rerun is safe.
- **Queries** read a snapshot (`snapshot`, `client.rs:233`) at the tick timestamp of the subscription manager (§8.2), so every query in a session is evaluated at one timestamp.
- **Timestamps** are PD TSO values (physical ms << 18 | logical). They are Loams Live's version numbers: a query result is "valid at ts", a mutation returns its commit timestamp, and a client waits until its query set has reached that timestamp before it drops an optimistic update.
- **Limits per mutation (R1 defaults):** 8 MiB written, 16 000 documents written, 32 000 documents scanned, 4 096 index ranges, 1 s of JavaScript CPU, a 10 s wall-clock deadline. These follow Convex's published limits in shape, scaled down for R1, and are configurable.
- **Idempotency.** A `Mutate` call may carry an idempotency key. The runner writes the key's record inside the same transaction and, on a retry of the call, returns the recorded commit timestamp instead of running again. A lost acknowledgement therefore never applies a mutation twice.
- **Commit options.** **R1 defaults to async commit with 1PC** (`use_async_commit` and `try_one_pc`, `transaction.rs:1206,1213`). In the spike they cut commit p50 by roughly 30–50% against 2PC: about 3.5–5.3 ms against 7.1–7.3 ms for optimistic transactions under heavy host load **(spike)**. Two risks come with them:
  1. An async-commit timestamp can be larger than a start timestamp fetched later by another client unless `min_commit_ts` is seeded from a fresh TSO, as TiDB's `tidb_guarantee_linearizability` does.
  2. `tikv-client` 0.4.0's async commit `unwrap()`s `min_commit_ts` and never sets `max_commit_ts` (a FIXME in `transaction.rs`) **(spike)**.

  So the commit mode is a config switch (`commit_mode = async_1pc | two_pc`). The R1 gates (the linearizability histories of the metastore suite, and the reactive and transaction checkers) must pass with the default. If one fails, that component falls back to `two_pc` until the upstream fix lands (§11.4).

### 5.2 Isolation level

TiKV gives **snapshot isolation**. Convex promises **serializability**. The gap is write skew: two mutations read overlapping data, write disjoint keys, and both commit.

| What a mutation read | Protection in R1 | Result |
|---|---|---|
| A document by id (`db.get`) | The key is promoted into the transaction's lock set (`lock_keys`, `transaction.rs:615`), so a concurrent write to it is a write-write conflict and one side retries | Serializable |
| An index range (`db.query`) | None beyond snapshot isolation | Write skew is possible when two mutations each read a range the other writes into |

- R1 documents the difference on the limits page and in the function API docs. Most Convex-style mutations read what they write (read-modify-write of one document), which snapshot isolation plus promotion already makes safe.
- **Serializable range reads are Q31.** Two candidates:
  1. **Guard keys.** Each write to an index also writes a guard key for its equality-prefix bucket, and a range read in a mutation locks the buckets it spans. Correct and simple, but every insert into a bucket then conflicts with every other insert into it, which serializes, for example, all messages in one channel.
  2. **Validation against the journal.** After prewrite, fetch a TSO `v`, read the journal entries between the start timestamp and `v`, and abort if any of them wrote into the mutation's read ranges. The journal already carries the old and new index keys of every write (§5.3). This needs a proof that entries committing between `v` and the commit timestamp cannot create an anomaly, and a model check before it ships.

  R2 decides after R1's checker (R1 Task 16) measures how often the first option would conflict on realistic workloads.

### 5.3 The commit journal (D119)

Every mutation transaction also writes one **journal entry** into its app's journal. The entry lists what the transaction changed:

```
JournalEntry {
  commit_hint_ms,                        // TSO physical time of the start timestamp; diagnostics only
  writes: [ { table_id, doc_id, kind: insert | replace | delete,
              index_keys_removed: [bytes], index_keys_added: [bytes] } ],
  function, request_id                   // for tracing
}
```

**Sequencing.** The journal has `S` shards (16 by default, up to 1 024; set per app). A mutation picks a shard, reads the shard's head `h` at its start timestamp, and writes the entry at `seq = h + 1` and the head `= h + 1` in the same transaction. Two mutations that pick the same shard conflict on the head and one retries on another shard. So each shard's sequence is **dense and ordered by commit**, and an entry is visible at timestamp `T` exactly when its transaction committed at or before `T`.

**Why the journal, and where TiKV CDC fits.** TiKV's change feed **does** serve a txn-API keyspace. The spike subscribed to `cdcpb.ChangeData/EventFeed` from Rust and received Prewrite and Commit rows with values, 1PC `Committed` rows, deletes and resolved timestamps, both for a Rust-client keyspace (1 region) and for a keyspace-mode TiDB (62 regions) **(spike)**. It needs:

- **`kv_api = TiDB`, even for non-TiDB transactional keys.** `TxnKV` is refused with a misleading `Compatibility{required_version: "6.2.0"}` error. `validate_kv_api` accepts only `TiDB`, or `RawKV` on API v2, and the txn path handles any non-raw key (`tikv/components/cdc/src/service.rs:27-30`, `endpoint.rs:843-845`, `delegate.rs:1023-1029`).
- **Its own client stubs.** `tikv-client` ships generated `cdcpb` and `pdpb` code, but `mod proto` is private (`client-rust/src/lib.rs`). The spike copied `src/generated/` from the 0.4.0 crate, which builds with prost 0.12 and tonic 0.10 and needs no protoc. The alternative is running tonic-build over `client-rust/proto/*.proto`.
- **A hand-written, region-by-region subscriber.** The subscriber gets the cluster id from PD, calls `ScanRegions` over the keyspace's memcomparable-encoded range, finds each leader's store, and opens one `EventFeed` per store with one registration per region. It then matches Prewrite to Commit by `(start_ts, key)`, drops rollbacks and orders by `commit_ts`. Region splits, merges and leader moves arrive as per-region errors, and the subscriber must re-scan and re-register from the last resolved ts. That path was **not** exercised in the spike. TiCDC's Go log puller is the model (`ticdc/logservice/logpuller/`).
- **About 1 s of lag for commit order.** Raw events arrive within milliseconds, but store-level resolved ts advances about once a second **(spike)**. So a consumer that needs changes in commit order waits ~1 s, unless `cdc.min-ts-interval` is lowered, which costs TiKV CPU.

TiCDC itself stays TiDB-table oriented (spans keyed by table id, `ticdc/logservice/eventstore/event_store.go:218`), so capturing a Live keyspace would mean our own Rust subscriber.

**R1 keeps the in-transaction journal** for invalidation, because:

1. it invalidates within milliseconds of a commit, with no resolved-ts wait;
2. it needs no region tracking;
3. it carries the old index keys, which CDC gives only with `extra_op=ReadOldValue`;
4. its dense per-shard sequences serve both the journal-based serializability validation (Q31, option 2) and the bridge's exactly-once producer sequences.

**TiKV CDC is a validated secondary path.** It is the fallback if the journal's shard heads become a bottleneck, and a candidate feed for consumers where ~1 s is fine. The collections bridge (§12) is the first such candidate: search freshness of about a second is acceptable there, and CDC would remove the journal's retention coupling.

The journal costs two extra keys per mutation (the entry and the shard head). In exchange it is exactly once, ordered per shard, carries the old index keys the invalidation needs, and gives the bridge dense producer sequences (§12). TiKV CDC stays relevant for **TiDB tables** (through TiCDC, Q34) and as the secondary path above.

**Retention.** A janitor deletes entries once every consumer (each node's tailer, the bridge) has passed them and they are older than `journal_retention` (10 min). Consumers checkpoint per shard.

## 6. Server functions

### 6.1 Kinds

| Kind | Transactional | Side effects | Retried by Loams | Where |
|---|---|---|---|---|
| **Query** | Reads one snapshot | None | Rerun on invalidation | R1 |
| **Mutation** | One TiKV transaction | None | On conflict, up to 8 attempts | R1 |
| **Action** | No; each `runQuery`/`runMutation` inside it is its own transaction | `fetch` (allowlisted hosts), AI-gateway calls | Never automatically | R2 |
| **Scheduled function** | A mutation schedules it transactionally (a row in the scheduler range) | As its kind | Mutations exactly once; actions at most once | R2 |

Durable actions (steps that survive a crash and resume) run through the Resonate server embedded in the binary (§21 §6.6, Apache-2.0, with a Rust SDK at `resonate/impl/sdk/rs`), in track D's D3 (D145), not as an R4+ option.

### 6.2 Determinism

Queries and mutations must return the same result from the same snapshot:

- `Date.now()` returns the transaction's start timestamp in ms; `Math.random()` is a deterministic PRNG seeded from the start timestamp and the request id, and is documented as not suitable for secrets. **`crypto.getRandomValues()` and `crypto.randomUUID()` are never seeded**: in queries and mutations they throw (`DeterminismError: crypto randomness is not available in queries and mutations; use an action`), and in actions (R2) they draw from the OS CSPRNG. A deterministic value can then never be mistaken for a cryptographic one. Document ids are drawn by the host from the OS CSPRNG (§4.1), outside the function's view.
- No timers, no `fetch`, no network, no filesystem.
- Host calls (`db.get`, `db.query`, `db.insert`, `db.patch`, `db.replace`, `db.delete`) are the only I/O. Each read call adds to the read set.

### 6.3 The sandbox (D120, Q35)

| Option | License | For | Against |
|---|---|---|---|
| **V8 through `rusty_v8` (`v8` crate) or `deno_core`** | MIT (both) | Fastest (JIT); what Convex and Deno run; best npm compatibility | A prebuilt static library of ~100 MB or a from-source build of V8; long link times on a machine that builds one crate graph at a time; isolate snapshots and determinism need care |
| **QuickJS through `rquickjs`** (QuickJS-ng underneath) | MIT (both) | Small, compiles from C source in seconds; interrupt handler for CPU limits; per-runtime memory limit; async host functions; ES2023 modules | An interpreter: 10–50× slower than V8 on CPU-bound code **(estimate)**; smaller ecosystem |
| **WASM through `wasmtime`** | Apache-2.0 WITH LLVM-exception | Fuel metering; strong isolation; polyglot (Rust, Go, Python components) | TypeScript needs a JS engine compiled to WASM anyway (Javy embeds QuickJS, StarlingMonkey embeds SpiderMonkey); the component model tooling adds a build step for users |
| Boa (`boa_engine`) | MIT OR Unlicense | Pure Rust | Less complete and slower than QuickJS today |

**Recommendation for R1: QuickJS through `rquickjs` 0.14.** Server functions in Convex are mostly small, I/O-bound TypeScript that reads and writes a few documents, so an interpreter's speed is enough. QuickJS keeps the build small (it matters on a machine that builds one crate graph at a time) and gives CPU and memory limits out of the box. Users write TypeScript; the CLI bundles it to one ES module with esbuild (MIT) before `Deploy`. wasmtime comes back when users want functions in Rust or Go; V8 comes back if profiles show CPU-bound functions. The function API is engine-neutral, so switching engines does not change user code.

**Runtime model.** One QuickJS runtime per (node, app, deployment), with a pool of pre-initialised contexts; memory limit 64 MiB per runtime; the interrupt handler enforces the CPU limit. **A context serves exactly one invocation and is then discarded**, whether the call succeeded or threw: module-level variables, caches and patched globals can never carry state from one call to the next, which §6.2's determinism needs. The pool keeps `contexts` (4) fresh contexts warm, each with the bundle's module already evaluated, and refills in the background, so the evaluation cost stays off the request path. Built-in globals are frozen before the bundle is evaluated. Deployed bundles are stored in object storage (`live/<app>/deployments/<id>.js`) and the current deployment is a pointer in the app's catalog.

## 7. Sync protocol (D121)

### 7.1 Services

```proto
package loams.live.v1;

service LiveService {
  // Opens a session. The first Transition carries the whole query set's results;
  // later ones carry only changed queries. Heartbeats are empty Transitions every 15 s.
  rpc Watch(WatchRequest) returns (stream Transition);
  // Adds and removes queries in an open session. The next Transition reflects the change.
  rpc ModifyQuerySet(ModifyQuerySetRequest) returns (ModifyQuerySetResponse);
  rpc Query(QueryRequest) returns (QueryResponse);          // one-shot, at the latest tick or at a given ts
  rpc Mutate(MutateRequest) returns (MutateResponse);       // returns the commit ts and the result
  rpc Deploy(DeployRequest) returns (DeployResponse);       // admin: a bundle and a schema
}

message StateVersion { uint64 query_set = 1; uint64 identity = 2; uint64 ts = 3; }

message Transition {
  string session_id = 1;
  StateVersion start = 2;           // must equal the client's current version
  StateVersion end = 3;
  repeated QueryUpdate updates = 4; // per query id: a value, an error, or removed
  bool more = 5;                    // chunked: more Transition messages with the same end follow
}
```

- **Why a server stream plus unary calls, not bidi.** Browsers cannot do full-duplex streaming over fetch, and on HTTP/1.1 connect-rust sends no response until the request body is complete (`connect-rust/docs/guide.md:862-866`). A server stream works on HTTP/1.1, HTTP/2, Connect, gRPC and gRPC-Web, so one design serves every client. Native clients may later get a bidi variant over HTTP/2.
- **Versions.** A session's state is the triple (query-set version, identity version, ts), as in Convex's sync protocol. A client applies a Transition only if its `start` equals its current version; a gap means it resumes (below).
- **Consistency.** Every query in one session is evaluated at the same tick timestamp, so a client never shows two results from different moments (§8.3 gives the argument).
- **Mutations and optimistic updates.** `Mutate` returns `commit_ts`. The client keeps its optimistic update until its session's `ts` reaches `commit_ts`, then drops it; the server sends a ts-only Transition to a session with a pending mutation at most once per second, so the client does not wait for an unrelated change.
- **Resume.** A client that reconnects sends `WatchRequest { resume: { last_version, query_set } }`. The server reruns the set at a tick at or after `last_version.ts` and sends full results. R1 sends no diffs.
- **Session routing.** A session lives on the node that holds its `Watch` stream. `session_id` carries that node's id; another node that receives a `ModifyQuerySet` forwards it with M1.3's request forwarding.
- **Encoding.** Connect's JSON and binary codecs both work; `int64` values are strings in JSON and `bigint` in protobuf-es, so ids and counters stay lossless in TypeScript.
- **Listener: loopback only in R1.** `127.0.0.1:7710` by default (`--live-listen`). `LiveService` exposes `Query`, `Mutate` and the admin `Deploy` with no authentication in R1 (D111), so **a non-loopback `--live-listen` is refused at startup** (`loams: --live-listen <addr> is not a loopback address; the Live API has no authentication until the unified auth plan (D111)`) rather than only warned about, which is stricter than D111's default for the other listeners. A loopback bind still logs one startup line saying the Live API is unauthenticated. Remote access in R1 goes through an SSH tunnel or a reverse proxy the operator secures. The refusal is lifted when the unified auth plan covers the Live API (R3). The dev playground's TiDB (root without a password) binds 127.0.0.1, as `tiup playground` does by default.

### 7.2 Generated clients

| Platform | Generator (all Apache-2.0) | Hand-written layer |
|---|---|---|
| Web and Node | `protoc-gen-es` + `@connectrpc/connect-web` / `connect-node` | `@loams/live`: query set, transitions, resume, optimistic updates; React hooks in R2 |
| iOS | `connect-swift` | R3 |
| Android | `connect-kotlin` | R3 |
| Python | `connect-python` **(verify maturity)** | R2 |
| Go | `connect-go` | R2 |

The generated stubs are the contract; each platform's hand-written layer is small (session state machine, reconnect, optimistic updates) and shares one conformance fixture set (R1 Task 14 starts it).

## 8. Reactivity

### 8.1 Read sets

- A query's **read set** is the list of what it read: point keys (`db.get`) and index ranges (`db.query`), as encoded key ranges `[lo, hi)` in one index. A filter applied after the read does not narrow it: the whole range scanned is in the set. A range that stopped at its limit is recorded up to the last key returned, not to the range's end, so inserts past a full page do not invalidate it (Convex's pagination idea).
- Read sets are ranges, not document lists, so an insert that falls into a range is caught (no phantom misses).

### 8.2 Invalidation, fan-out and reruns

Per app, on each node with sessions for that app:

1. **Tick.** The tailer takes a TSO timestamp `T`, reads all shard heads at `T` (one `batch_get`), and scans the new entries of each shard whose head moved, at `T`. It wakes immediately after a local commit and otherwise polls with backoff from 20 ms to 200 ms.
2. **Match.** For every write in the new entries, the removed and added index keys and the document key are looked up in the app's **read-set index**: an interval tree per (table, index) and a hash set of point keys, mapping to subscription ids. A lookup is O(log n + matches).
3. **Rerun.** Each invalidated subscription is rerun at `T`, at most `rerun_concurrency` (16) at once per app. A rerun produces a new result and a new read set, which replaces the old one in the index. Identical subscriptions (same function, arguments and identity) share one cache entry, so a thousand clients watching one chat room cost one rerun.
4. **Push.** Every session gets one Transition to `T` with the queries whose result hash changed. Sessions with no changed query get nothing (except the ts-only Transition of §7.1).
5. **Advance.** All subscriptions of the app are now valid at `T`: the untouched ones because nothing in their read set changed between their last timestamp and `T`.

**Backpressure:**

- **Reruns.** If invalidations arrive faster than reruns finish, the next tick simply reruns at a newer timestamp. Intermediate results are skipped, never reordered: a client may see fewer versions, never an inconsistent one. A subscription is rerun at most once per `min_rerun_interval` (50 ms).
- **Sessions.** Each session has a bounded outbound queue (16 Transitions). When it is full, queued Transitions are merged into one from the client's acknowledged version to the newest. A session blocked for more than 30 s is closed, and the client resumes.
- **Quotas.** Subscriptions per session (1 000), sessions per app and reruns per second per app are limits; over a limit the server answers `RESOURCE_EXHAUSTED`, as D65's quotas do.
- **Safety net.** Every subscription is also rerun unconditionally every 5 minutes and compared; a difference is a bug, is logged with both read sets, and is counted in a metric that alerts.

### 8.3 Why no update is missed

Let a subscription's result `R` be computed at tick `t` with read set `S`, and let `T > t` be the next tick.

1. Every committed write to a key in `S`, or into a range in `S`, belongs to a transaction that also wrote a journal entry naming that key (the document key and its old and new index keys). This assumes that only Loams writes into a Live keyspace; the keyspace is never handed to another TiKV client, and the router enforces it.
2. A shard's entries visible at `T` but not at `t` are exactly those with `head(t) < seq ≤ head(T)`, because the head and the entry are written in one transaction and the head only grows.
3. So if no new entry touches `S`, no write between `t` and `T` changed what the query read, and `R` is also the result at `T`. Otherwise the subscription is rerun at `T`.

Every query in a session is therefore valid at the session's tick, and results never go back in time. R1 Task 16 checks exactly this: every pushed result equals a fresh snapshot evaluation at its timestamp.

## 9. Multi-tenancy and keyspaces (D122)

### 9.1 What TiKV and PD provide

- **API v2 keyspaces.** Keys carry a mode byte (`r` raw, `x` transactional) and a 3-byte keyspace id (`tikv/components/api_version/src/api_v2.rs:16-20,53-55`; `keyspace.rs:8`), so a cluster holds up to 2^24 keyspaces. PD splits regions at each keyspace's bounds when it creates one (`pd/pkg/keyspace/keyspace.go:653`).
- **Management.** PD creates keyspaces and changes their config and state over `/pd/api/v2/keyspaces` (`pd/server/apiv2/handlers/keyspace.go:39-48`). States go ENABLED ⇄ DISABLED → ARCHIVED → TOMBSTONE (`pd/pkg/keyspace/util.go:57-60`). Keyspaces can also be pre-created at bootstrap (`[keyspace] pre-alloc`, `pd/server/config/config.go:880`).
- **Cluster setting.** Every TiKV runs `storage.api-version = 2`, which requires `storage.enable-ttl = true` (`tikv/src/storage/config.rs:204-206`). A store that already holds RawKV or TxnKV data cannot switch from v1 (`tikv/src/server/raft_server.rs:280-330`), so **a Loams cluster is created on API v2 from the start**. Nothing found enforces the same setting on every store, so deployment tooling does (`tikv/src/storage/mod.rs:480-538` rejects mismatched requests per store).
- **Sharing.** Every transactional client writes `x` + keyspace-id keys, so Live, the metastore, jobs and durable state share one cluster safely as long as each uses its own keyspace. (A keyspace-mode TiDB would too; none is deployed under D260.)

### 9.2 Size classes

A keyspace costs at least one region (three Raft replicas) and PD metadata, so millions of apps cannot each own one. The router places apps by size class, as §18 §5.3 does for namespaces:

| Class | Placement | When |
|---|---|---|
| **Shared** | One of a pool of shared keyspaces, under the app prefix `0xA0 ‖ app_id` (§4.3) | Default for new and small apps |
| **Dedicated** | Its own keyspace | Large apps and apps that need their own GC or resource controls |

- The **directory** maps (org, app) → {keyspace, prefix, class, state}. It lives in the `ControlStore` (§9.5) and is pushed to Live nodes as a live query: the router's directory feed of §18 §5.2 becomes a Loams Live subscription.
- **Moving** an app from shared to dedicated copies its key range into the new keyspace under a write fence (the app's lease epoch), then flips the directory, as §18 §5.5 moves namespaces. Unlike a namespace move, this copies data, so it runs as a background job with a short write pause at the flip.
- **Resource isolation.** PD's resource manager has per-keyspace managers (`pd/pkg/mcs/resourcemanager/server/manager.go:98`); whether TiKV-side request units can be attributed to a txn-API client's keyspace is Q36.
- How many keyspaces one cluster sustains before region count dominates is Q36; the size-class thresholds come from that measurement.

### 9.3 MVCC garbage collection

TiKV keeps old versions until a GC safe point passes them. TiDB advances it for its own keyspace; **nobody advances it for a txn-API keyspace unless Loams does**, and versions would pile up forever.

- PD keeps per-keyspace GC state (txn safe point, GC safe point, GC barriers) and a keyspace's `gc_management_type` is `keyspace_level` or `unified` (`pd/pkg/gc/gc_state_manager.go`; `pd/pkg/keyspace/keyspace.go:51-75`). A `unified` keyspace needs a TiDB **without** `keyspace-name` to run the GC worker (`tidb/pkg/store/gcworker/gc_worker.go:108-117,334`).
- `client-rust`'s `gc()` resolves locks and updates the **cluster-level** safe point only (`client-rust/src/transaction/client.rs:268`, `src/pd/cluster.rs:88`). The keyspace-scoped RPCs (`AdvanceTxnSafePoint`, `AdvanceGCSafePoint`, `SetGCBarrier`, `GetGCState` with a `KeyspaceScope`) are in its vendored `pdpb.proto` (lines 82–112, kvproto `b41e863`), but not exposed.
- TiKV's own GC worker reads one safe point (`tikv/components/pd_client/src/client.rs:864`) and has no keyspace logic in `tikv/src/server/gc_worker/`; how PD combines keyspace states for it is unverified.
- **R1, as answered by R1 Task 0 (Q32, 2026-09-27):** PD v8.5.8 answers the keyspace GC-state RPCs with `Unimplemented`, and TiKV reads only the cluster safe point. So `loams-tikv`'s GC loop acts as the **cluster's GC worker for every keyspace**: it resolves locks below the target in each keyspace, then advances the cluster safe point (`UpdateServiceGCSafePoint` + `UpdateGCSafePoint`) to `now − gc_life_time` (10 min), held back by barriers registered as PD service safe points for open snapshots (the collections bridge, backups). No TiDB GC worker is used (D260). *(The original plan, keyspace-level safe points through generated `pdpb` stubs with a `unified` GC TiDB as fallback, is superseded.)*

### 9.4 Namespace router (the owner's item d)

- **R1:** one app, one dedicated keyspace, named on the command line.
- **R2:** the directory, shared keyspaces with app prefixes, app create/drop/rename, moves between classes, per-app quotas, and session placement by rendezvous on the app id (M1.3's hashing), so an app's subscriptions concentrate on few nodes and each app is tailed by few nodes.

### 9.5 The control plane on Loams Live (D125)

§19's control-plane data (orgs, members, teams, projects, environments, agents, service accounts, API keys, role bindings, quotas, usage rollups, the directory, billing accounts) needs a store in Loams cloud. Two TiKV-backed choices:

| | **Loams Live (a system app `_control`)** | **TiDB SQL** |
|---|---|---|
| Console updates | Live queries: member lists, agent tokens, usage and quota meters update without polling | Polling |
| Directory push to gateways (§18 §5.2) | It is a live query | A poller or TiCDC |
| Client code | The same Rust `LiveTxn` API; one TiKV client stack | `sqlx` with the MySQL dialect; a second data-access style; D58's dialect questions (Q24) return |
| Ad-hoc reporting | Weak: no SQL; usage is pre-aggregated rollups (D103) and exported to Iceberg for analysis (M4) | Strong |
| Maturity risk | The control plane depends on the youngest component | TiDB is mature |
| Transactions | TiKV transactions with the same retry model | TiDB pessimistic transactions |

(The TiDB SQL column is history: D260 removes TiDB.)

**Choice: Loams Live.** The console and the gateways are the two heaviest readers of control-plane data, and both want pushed changes. Dogfooding the reactive layer on Loams's own control plane is also the fastest way to harden it. Billing reports that need SQL read the usage rollups from the Iceberg export (or, until M4, from a SQL mirror outside TiKV). The `ControlStore` trait (D65) stays: OSS and single-node installs implement it on the metastore backend, and Loams cloud implements it on `_control`. The bootstrap app `_control` lives in a fixed keyspace (`loams_control`), so reading the directory never needs the directory. Lands in R2; the maturity risk is mitigated by R1's gates and by keeping the openraft `ControlStore` as a fallback.

## 10. TiDB SQL coexistence (D123): superseded by D260

> **Decided 2026-10-02** ([§31](31-loams-router-and-verification.md), Q314): OLTP MySQL wire access comes from vtgate in front of WeSQL (§29, PR #172; D320), so Q260 narrows to whether Loams also serves read-only MySQL wire access over DataFusion, which stays open.
>
> **Superseded 2026-09-29 by D260.** Loams deploys no TiDB, so there is no TiDB SQL beside Live. The subsections below are kept for their TiDB and keyspace facts. What replaces them is **open (Q260)**, with two candidates and no decision:
>
> 1. **Loams's own MySQL wire front end over DataFusion**: a read-only analytics listener over collections and tables, the MySQL counterpart of the Postgres wire listener (D-PG-1, `datafusion-postgres`). It would serve SELECTs to MySQL clients and BI tools, with no writes and no interactive transactions. It adds a protocol to D42's footprint and needs a MySQL wire library checked against D11.
> 2. **No MySQL surface.** SQL access stays on Flight SQL and the Postgres wire listener, and MySQL-only apps bring their own database (as Forgejo and Matomo do in the suite, D149, D-SC-14).
>
> Either way, Live tables are not exposed over MySQL, and transactional MySQL is out of scope.

### 10.1 What keyspace mode requires

- TiDB's `keyspace-name` (config key, or env `KEYSPACE_NAME`; `tidb/pkg/config/config.go:124,240`) switches its driver to API v2 with a keyspace codec (`tidb/pkg/store/driver/tikv_driver.go:190-202`).
- TiKV must run API v2 (§9.1), and the keyspace must exist in PD before TiDB starts, created with PD's `[keyspace] pre-alloc` or the HTTP API. **Verified on playground v8.5.8** (classic Community build): TiDB with `keyspace-name = "ks_tidb"` wrote keys prefixed `x 00 00 01`, served MySQL clients (CREATE, INSERT, UPDATE, BEGIN/COMMIT, SELECT), and ran beside a Rust client in keyspace `ks_rust`. A key written in `ks_rust` read back as absent from both `ks_tidb` and `DEFAULT` **(spike)**.
- A keyspace-mode TiDB runs its own keyspace-level GC worker; `unified` keyspaces need one TiDB without a keyspace (§9.3).

### 10.2 One TiDB per tenant, not a shared TiDB

- **A TiDB process serves exactly one keyspace**: its store holds a single keyspace name (`tikv_driver.go:252,494`). `tidb/pkg/domain/crossks` is internal plumbing for the next-gen SYSTEM keyspace, not multi-tenant serving; `tidb/pkg/keyspace/doc.go:15-38` describes keyspaces as logical clusters for next-gen and serverless.
- **So each SQL-enabled tenant gets its own TiDB pool** (one or more stateless TiDB pods with `keyspace-name` set), in its own dedicated keyspace, on the shared TiKV cluster. A shared TiDB with resource groups gives quotas but **no data isolation** between tenants and is not used for tenant SQL.
- **Cost.** An idle TiDB server takes several hundred MB of memory **(estimate)**, so SQL is opt-in per app, and idle pools scale to zero later (R4) behind a MySQL-protocol proxy that routes by user or database name. PingCAP's TiProxy (Apache-2.0) is the candidate **(verify its keyspace routing)**.
- **Next-gen only? No, for the binaries.** tidb-operator v2's `TiDB.spec.keyspace` says "For classic tidb, keyspace name is not supported" (`tidb-operator/api/core/v1alpha1/tidb_types.go:204-208`), but classic TiDB v8.5.8 runs in keyspace mode on `tiup playground` (above). Q33 is therefore narrowed. The binaries work; what remains is whether tidb-operator v2 accepts `keyspace` for classic clusters or needs the setting through its free-form config, and whether PingCAP supports the mode for classic deployments. That remaining question is checked with the operator in R4.

### 10.3 What SQL can and cannot see

- A tenant's TiDB sees its SQL keyspace only. Live tables are not visible from SQL, and SQL tables are not visible from Live functions, in R1–R3.
- Later, two one-way bridges are possible without forks: **Live → SQL** (the journal tailer writes rows into TiDB tables over MySQL) and **SQL → collections** (TiCDC in keyspace mode, `ticdc/pkg/config/changefeed.go:280`, into a Kafka or storage sink that Loams's native stream API or Kafka gateway reads). Neither is scheduled; the second depends on Q34.

### 10.4 TiDB's object-storage, vector and full-text features (D131)

The owner asked whether Loams can use these instead of, or beside, its own engine. Findings, checked in the clones where possible:

| Feature | Open source and self-hostable? | Evidence | Use in Loams |
|---|---|---|---|
| **Next-gen kernel: object storage as the single source of truth** | **No.** TiDB's side is open (a `nextgen` build tag, a separate binary; components of different kernel types cannot be mixed), but the matching shared-storage TiKV engine is not public: `tikv/tikv`'s `cloud-engine` branch was last touched on 2022-09-26, and master has no such engine (`tikv/components/cloud/` holds only the AWS, Azure and GCP clients for external storage) | `tidb/pkg/config/kerneltype/doc.go:15-39`; the coordinator's check of the tikv branches | **Not used.** Self-hosted TiKV keeps its row store on local disks with Raft. Loams Live's durability to object storage comes from BR log backup (below) |
| **TiFlash**: columnar replicas, disaggregated compute and storage on S3, the `VECTOR` type with HNSW vector indexes | **Yes.** pingcap/tiflash is Apache-2.0 and active **(not cloned; verify the S3 mode and vector index on the pinned release)**. TiDB builds vector indexes as *columnar indexes* backfilled on TiFlash (`tidb/pkg/ddl/index.go:997,1028-1115`) | As listed | **Optional add-on for SQL tenants** (R4) who want analytical or vector queries inside SQL. It is C++, heavy in memory, and replicates TiDB tables only, so it cannot see Live tables |
| **Full-text search** (`MATCH … AGAINST`, `FTS_MATCH_WORD`) | **Not in the open-source build, in practice.** TiDB parses it (`tidb/pkg/expression/builtin_fts.go`, `tidb/pkg/planner/core/fts_resolve_index.go`), but execution needs "the TiFlash FTS path", and the coordinator's search of public TiFlash found no full-text implementation. In a plain boolean `WHERE` position TiDB rewrites the match to `ILIKE '%term%'` predicates: no relevance score, no stop words, no word boundaries ("cat" matches "concatenate"), no index; phrases, `*`, `> < ~` and grouping are refused at plan time. A scoring position (`SELECT` list, `ORDER BY`, comparisons) keeps the native builtin and then needs TiFlash to execute (`tidb/pkg/planner/core/fulltext_to_like.go:19-70`, `tidb/pkg/expression/fts_to_like.go`) | As listed | **Not used.** Full-text over SQL data should go through a collection instead (SQL → collections, §10.3, Q34) |
| **`tiup playground --mode tidb-x` and `--mode tidb-cse`** (next-gen, S3-backed TiDB, with `--cse.s3_endpoint` and similar flags) | **Unknown.** The modes exist in tiup playground 1.17.1 **(spike; not run)**. Where their TiKV binaries come from and under what license is unverified, and no public source for a next-gen TiKV was found | tiup 1.17.1 `--help` | **Evaluate (Q38)** before any use. If the binaries are closed or unlicensed for self-hosting, they are not usable (D11, D126) |
| **`tici*` tiup components** (v0.1.0-alpha-nightly; probably TiDB's full-text and columnar indexing service) | **No.** Binary-only, with no public repository, and alpha | tiup component list **(spike)** | **Not usable:** no source, no license to check, alpha |
| **BR and log backup (PITR) to object storage** | **Yes.** TiKV's `backup-stream` component streams change logs to external storage (S3, GCS, Azure; `tikv/components/backup-stream`, `external_storage`, `cloud/{aws,azure,gcp}`); BR takes full snapshot backups and restores to a point in time | As listed | **Mandatory for every Live cluster** (below) |

**Position:**

1. **Loams's own engine is the vector and full-text engine for Live data**, through the collections bridge (§12). It is S3-native, gives hybrid retrieval with relevance scores, the hot tier and the Qdrant and Elasticsearch surfaces, and is the differentiator. TiDB's full-text is not a substitute, and TiFlash's vector index sees only SQL tables.
2. **TiFlash is an optional add-on for SQL tenants** (R4), run unmodified, for columnar or vector queries inside SQL. Loams does not depend on it.
3. **BR log backup (PITR) to object storage is mandatory for every Live cluster**, into the tenant's bucket (the bucket its namespace already uses) or the operator's bucket in multi-tenant keyspaces. A continuous log backup task plus periodic full snapshots gives a bounded recovery point (log backup's flush interval, minutes by default **(verify)**) and restore to any time in the retention window. A Live cluster does not serve external traffic until its backup task is running and a restore has been tested. This softens D130: TiKV stays Live's primary store, but a restorable copy of every Live keyspace is always in object storage. Whether BR's log backup and PITR restore cover a **txn-API keyspace** (not just TiDB tables), and per keyspace, is Q37; if they do not, the fallback is a journal-based export of each keyspace to the bucket, since the commit journal (§5.3) already records every write.

## 11. The TiKV metastore (D124)

### 11.1 Why now, and what changes

§18 §2.4 put TiDB over `sqlx` in M6 and rejected `tikv-client` because it "needs a real PD and TiKV cluster and would rebuild in KV what TiDB's SQL layer already provides". Both reasons change with Loams Live: the cluster exists anyway, and `loams-tikv` is shared with Live, so the KV layer is built once. **`loams-meta-tikv` replaces `loams-meta-tidb`** as the scale-out backend, and moves to R1. Postgres and DynamoDB stay v1.0 backends (D58). openraft + redb stays the default for `loams dev` and single node (D10).

### 11.2 Keys

One keyspace per Loams cluster (`loams_meta`, or `loams_meta_<cluster_id>` for each BYOC-managed cluster in the hosted control plane):

| Record | Key | Value |
|---|---|---|
| Namespace by name | `n/<org_id>/<name>` | `id` |
| Namespace | `N/<id:u64 BE>` | record, state |
| Id block | `c/<kind>` | next unallocated id; each node takes blocks of 1 000 (gaps allowed, D18) |
| Stream, link, collection by name | `s/`, `l/`, `k/` + `<ns>/<name>` | `id` |
| Stream, link, collection record | `S/`, `L/`, `K/` + `<id>` | record, `ns`, `state` |
| Aliases | `a/<ns>` | alias map, version |
| Partition head | `h/<stream>/<p>` | `next`, `log_start`, `bytes` |
| Index entry | `i/<stream>/<p>/<base:u64 BE>` | kind, records, object, byte range, max ts |
| WAL commit record | `w/<hash8(object)>/<object>/<group>` | base offsets, `created_at` |
| WAL live-chunk count | `W/<hash8(object)>/<object>` | `live` |
| Retired object | `r/<shard 0..63>/<path>` | `retired_at` |
| Object reference | `o/<hash8(path)>/<path>` | `refs`, `gc_claim` |
| Lease | `e/<scope>/<key>` | `owner`, `epoch`, `deadline_ms` |
| Pointer | `p/<ns>/<key>` | `version`, `value` |
| Commit token | `t/<token>` | `expires_ms` |
| Change counters | `v/<scope>` | `u64` |

WAL object names are ULIDs, which are time-ordered, so their keys get an 8-byte hash prefix to spread them over regions.

### 11.3 Mapping the contract

| Contract item | On TiKV |
|---|---|
| **One transaction per call** (D47) | Every trait method is one TiKV transaction: optimistic for single-record writes (leases, `cas_pointer`, creates), **pessimistic** (`begin_pessimistic`, `get_for_update` on partition heads in key order) for `commit_wal`, `swap_segment` and `trim_partition`, so hot heads queue instead of aborting in a loop |
| **`commit_wal` atomicity** (§18 §3.1) | **One transaction per partition group.** A group is at most 1 024 chunks and at most 4 MiB of metastore writes (index entries, heads, commit and reference rows), well under TiKV's per-request Raft entry limit (`raftstore.raft-entry-max-size`, 8 MiB by default) and the lock-holding time a pessimistic transaction should have **(verify the limits on the pinned release)**. A call that fits in one group, which is every flush the log writer produces today **(estimate)**, is **atomic across all its partitions and namespaces**, stronger than D59. A larger call is split into groups; **one stream's chunks are never split across groups** (D59's rule, §18 §3.1), and a single stream's chunks that alone exceed one group are refused with `InvalidArgument` so the writer splits the WAL object. Each group commits in its own transaction with its own commit record `w/<object>/<group>`, so it is idempotent. The call returns success only after every group has committed. After a crash or error some groups may be committed and others not; that WAL object is unacknowledged, and the writer's retry recommits only the missing groups (their records show which), or the stale-commit rule (D27) settles them. Visibility is atomic per group, as D59 requires |
| **Compare-and-swap** | Read the pointer at the start timestamp, compare, write. A concurrent writer is a write-write conflict at prewrite, so no update is lost; the loser re-reads and returns `VersionMismatch` when the version moved |
| **Fences and GC claims** (§18 §3.2) | Checked in the same transaction: the lease record's epoch, the collection's `live` state, and `gc_claim` on every `o/` row the command makes reachable. A claim and a new reference write the same `o/` row, so they conflict and serialize, with no clock |
| **Clock and stamps** (§18 §3.2, D113) | `clock_ms` is the TSO's physical part. The TSO is one cluster-wide monotonic clock with no hot row, so the TiKV backend keeps the strong "one monotonic clock" behaviour, and bounded skew holds trivially. Lease deadlines are judged against the transaction's start timestamp |
| **Leases** | A read-modify-write of `e/…` comparing owner, epoch and deadline |
| **Consistency tokens** (D76) | Offsets are assigned by `commit_wal` from the partition head inside the transaction, dense per partition. `Linearizable` reads use a fresh TSO timestamp, and any transaction acknowledged before that TSO fetch has a smaller commit timestamp, so a token's offsets are always visible. `Local` is served as `Linearizable` in R1; TiKV stale reads can serve it later |
| **Composite reads** (§18 §3.3) | Real snapshots: every read of one call uses one start timestamp. No read order is needed |
| **Unknown outcomes** | Each write transaction also writes `t/<token>`. After an error during commit, the backend reads the token at a fresh timestamp. TiKV's lock resolution either finds the transaction committed or rolls it back, so the answer is always known. Conflict, `KeyIsLocked` after backoff, region errors and TSO unavailability before prewrite are definitely-not-applied |
| **`watch_changes`** | No native primitive. Writers bump a change counter per scope (`v/catalog`, `v/ns/<id>`), and watchers poll counters every 100 ms and wake on the handle's own writes. `commit_wal` does not bump the catalog counter (D63's scoped feed) |
| **Pagination** (D99) | Range scans with `(prefix, after, limit)` |
| **Snapshots and restore** (D17, §10 §6) | Not applicable: TiKV replicates by Raft. Backups use BR full backups plus log backup (PITR) to object storage, mandatory as for Live (§10.4, D131; Q37) |

### 11.4 Gaps and risks

1. **MVCC GC** for the metastore keyspace needs Loams's GC loop (§9.3, Q32).
2. **`tikv-client` maturity.** Its README says 0.4.0 is "not suitable for production use - APIs are not yet stable" (`client-rust/README.md`).
   - **Dependency versions.** The crates.io 0.4.0 release pulls in prost 0.12 and tonic 0.10; the git master pulls in prost 0.13 and tonic 0.12 (`client-rust/Cargo.toml:39,49`). The workspace uses 0.14, so the tree carries two versions of each (build time and binary size).
   - **Client gotchas found in the spike:**
     - **TSO stream.** A PD stall (a 3 s etcd read) killed the client's TSO stream permanently (`TimestampRequest channel is closed`). Every later `begin` fails until the client is rebuilt, so `loams-tikv` wraps the client in a supervisor that rebuilds it on that error and on repeated TSO failures. *Fixed in Loams's fork (R1 plan row F1): the stream is reopened in place; the supervisor stays as defence in depth (row F3).*
     - **Pessimistic lock conflicts** surface as `PessimisticLockError{WriteConflict{reason: PessimisticRetry}}`. The client does not retry at a new `for_update_ts` as TiDB and client-go do; the runner restarts the whole transaction instead.
     - **Async commit** `unwrap()`s `min_commit_ts` and does not set `max_commit_ts` (a FIXME in `transaction.rs`). *Fixed in Loams's fork (row F1).*
     - **Keyspace required on API v2.** A client without a keyspace fails with `InvalidKeyMode` on an API v2 cluster, so every client must be configured with one.
     - **Error messages** include keyspace-prefixed raw keys, which must be scrubbed before they reach users.
   - **Upstream first.** Loams pins a version, runs its own conformance and fault suites against it, and contributes fixes upstream (D126). The **first upstream PR candidates** are (1) reconnecting the TSO stream after a PD stall and (2) exposing the generated proto modules (`cdcpb`, `pdpb`, `keyspacepb`) as a public module. Two follow-ups come after: setting `max_commit_ts` in async commit, and optional pessimistic lock retry at a new `for_update_ts`. A third is **resolving async-commit and 1PC locks on the read path** (`CheckSecondaryLocks` from the reader's lock resolver; today only GC's `cleanup_locks` checks secondaries, so a crashed async-commit writer blocks readers until GC). Until it lands, the metastore and Live commit with `two_pc` (R1 plan rows T6-5, T7-1).
   - **Loams's fork (R1 plan rows F1–F5).** On the owner's direction, `tikv-client` now comes from `https://github.com/ostrium-labs/client-rust`, branch `loam` (pinned by rev), which carries the TSO reconnect, the public proto modules, read-path async-commit lock resolution and `max_commit_ts`. Each fix is drafted as an upstream PR. Pessimistic lock retry stays the runner's job (row F5).
3. **Latency.** Each call costs a TSO fetch plus prewrite and commit round trips. In the spike, commit p50 was about 3.5–7 ms under heavy host load (1PC or async commit at the low end, 2PC at the high end). A 10-key pessimistic transaction took about 13–25 ms in total, dominated by ten sequential `get_for_update` round trips of about 1 ms each **(spike; indicative only)**. Batching locks (`batch_get_for_update`) matters for `commit_wal`. `commit_wal` sits on the write path; M2's write-latency budget must include it for TiKV deployments.
4. **Commit mode.** Async commit with 1PC was planned as the default here too (§5.1). As built, the metastore and Live commit with `two_pc` until the read-path lock resolution of item 2 is in the pinned `tikv-client` (R1 plan rows T6-5, T7-1); the `commit_mode` switch moves a component back to async commit then, by a ruling, once its linearizability histories and checkers pass with it. With the fork, the read-path resolution is in the pinned client; the metastore stays on `two_pc` pending an owner ruling (R1 plan row F4: async commit passed the matrix once but was less reliable on a hot partition head under load), and Live stays on `two_pc` until its checkers exist (Task 16).
5. **Hot keys.** A busy partition head is written by every flush that touches it. Pessimistic locking bounds the damage; TiKV splits regions by load but cannot split one key.

### 11.5 Where it lands

`loams-meta-tikv` passes the existing 49-case conformance suite with its linearizability histories, then its own fault matrix (§18 §4.2 columns, with TiKV rows: `BeforeSend` = TSO or prewrite refused; `AfterApply` = commit applied, response dropped; `Undetermined` = primary commit timed out; `Conflict` = `WriteConflict`; `Throttle` = `ServerIsBusy`; `Race`; `Delay`). It implements the trait as built when R1 starts; M2's contract amendment (§18 §3.4) then costs it little, because TiKV already meets the stronger contract.

## 12. The collections bridge (D129)

The owner's item (e), and the reason Loams Live is more than a Convex clone.

- **Declaring it.** A table in the deployed schema may say `searchable: { collection, fields, vectors, text }`. The bridge creates (or binds to) a Loams collection in the app's namespace with a matching schema.
- **Feeding it.** A bridge task per (app, table) tails the journal like a subscription tailer, but checkpoints durably. For each batch of entries visible at tick `T`, it reads the changed documents at `T`, maps each to a `DocOp` (upsert or delete by `_id`), and appends them to the collection's implicit stream through the log writer with an **idempotent producer id = (app, shard)** and **sequence = journal seq** (the D72 idempotent producers). A crash between append and checkpoint re-appends, and the producer sequence drops the duplicate: exactly once, end to end.
- **Alternative feed.** The spike showed that TiKV CDC (`kv_api=TiDB`) delivers a Live keyspace's changes to a Rust subscriber, with commit order about 1 s behind (§5.3). R3's plan decides between the journal and CDC as the bridge's source. CDC removes the journal-retention coupling; the journal gives ready-made dense producer sequences.
- **Embeddings** come from the collection's own ingest path (AI-gateway integration, D116), not from the mutation, so a mutation never waits on a model call.
- **Read-your-writes across the bridge.** A mutation returns `commit_ts`. The bridge records, per collection, the highest `T` it has fully appended and the consistency token of that append. A search from a Live query or action can pass `after_ts`; the service waits until the bridge's `T ≥ after_ts` and then reads with the matching token (D76). A query function that searches is re-run when the collection's manifest or tail advances past its token, which extends the read set with a "collection version" entry.
- **Back-pressure.** The bridge is an ordinary link-style task under the worker leases (§09); it honours the collection's unapplied-data budget (D86). A lagging bridge delays search freshness, never mutations.
- **GC.** The bridge's checkpoint holds the journal janitor and sets a GC barrier on the app keyspace while it reads documents at `T`.
- Lands in R3, with `ctx.search` in query and action functions.

## 13. Failure modes

| Failure | What happens |
|---|---|
| A TiKV store is lost | Raft (3 replicas) keeps each region available after a leader election (seconds); transactions retry on region errors |
| PD leader fails | TSO pauses until a new leader serves (seconds); new transactions and ticks wait; open sessions stay connected and receive nothing until ticks resume |
| A Live node crashes mid-mutation | Its prewrite locks expire after their TTL and the next reader resolves them (roll back or roll forward, per the primary's state). With an idempotency key, the client's retry is exactly once |
| A Live node crashes | Its sessions' clients reconnect elsewhere and resume (§7.1); results are rerun at a current tick. No state is lost because none is kept |
| The tailer falls behind | Ticks get further apart; results stay consistent (§8.2). A lag metric and alert fire past 1 s |
| A journal shard head is hot | Mutations conflict on it and retry on another shard; the app's shard count grows (a catalog change) |
| A slow client | Its Transitions are merged; after 30 s blocked it is disconnected (§8.2) |
| An invalidation is missed (a bug) | The 5-minute unconditional rerun finds the difference, repairs the result and alerts (§8.2) |
| GC safe point stalls (a stuck barrier, a long snapshot) | Old versions accumulate; an alert fires when the safe point is more than 1 h behind; barriers have TTLs |
| A TiKV cluster is lost (region or disk loss beyond Raft's quorum) | Restore from BR full backup plus log backup in object storage to the latest flushed point (D131); mutations committed after it are lost (the recovery point is the log flush interval) |
| Keyspace misconfiguration (a v1 store in a v2 cluster) | TiKV rejects requests with `ApiVersionNotMatched` (`tikv/src/storage/mod.rs:480-538`); the deployment check refuses to start |

## 14. Testing

1. **Conformance.** `loams-meta-conformance` runs against `loams-meta-tikv` (all 49 cases, linearizability histories). A new `loams-live` conformance suite covers the data model, codec order, index maintenance, journal density and the sync protocol's version rules.
2. **Fault matrix.** The in-process `FaultPlan` hook in `loams-tikv`'s transaction runner (`BeforeBegin`, `BeforePrewrite`, `BeforeCommit`, `AfterCommit`) drives a metastore fault matrix with a blessed `meta_fault_matrix.tikv.expected.md`, as §18 §4.2 does for the other backends, and a Live mutation fault matrix (every cell ends `Retried`, `SurfacedUnknown` or `NoEffect`; no acknowledged mutation lost; no idempotent mutation applied twice).
3. **Reactive correctness checker** (the R1 gate). A seeded workload of mutations and subscriptions over several sessions. For every Transition, the checker evaluates each updated query with a fresh snapshot read at the Transition's timestamp and requires equality; it also requires that versions strictly increase per session, that every committed mutation that touches a subscribed range is reflected by the first tick at or after its commit timestamp, and that a resumed session converges.
4. **Transaction checker.** A list-append workload over Live documents, checked for snapshot isolation (and, once Q31 lands, serializability) with an Elle-style cycle search implemented in `loams-sim`'s checker module; point-read promotion is checked by a write-skew workload that must show no anomaly on `db.get` reads.
5. **Jepsen-style nemesis** (nightly). On `tiup playground`: kill and restart TiKV stores and the PD leader, partition a Live node from TiKV with toxiproxy, pause processes; the workloads of items 3 and 4 run throughout and their checkers must pass.
6. **Where it runs.**
   - `tiup playground v8.5.8` with `--kv.config` (API v2 and TTL), `--pd.config` (pre-allocated keyspaces) and `--db 0` (no TiDB, D260); the spike also verified `--db.config` (`keyspace-name`). It is installed in CI by the tiup installer script, and the first run downloads about 500 MB. Every script uses `--tag` and `--port-offset`, because other playgrounds on the machine take the default ports.
   - Per PR: jobs touching `loams-tikv`, `loams-meta-tikv` or `loams-live*` start one playground (1 PD, 1 TiKV, no TiDB) and run the suites. Tests skip unless `LOAMS_TEST_PD` is set.
   - Nightly: 3 TiKV stores, the nemesis, the M1.1 gates over the TiKV metastore.
   - **Sizing.** In the spike, 1 PD + 1 TiKV + 1 TiDB peaked at about **3.2 GB RSS**: TiKV 2.62 GB, TiDB 428 MB, PD 114 MB, and the playground wrapper spiked to 1 GB at startup **(spike)**. TiKV sizes its memory from host RAM, and capping `storage.block-cache.capacity` at 1 GB still peaked at 2.56 GB. The dev and CI configs therefore also set `memory-usage-limit` explicitly, and CI runners need at least 8 GB. Locally the playground runs only when no cargo build is running (the build machine's limit); under memory pressure the kernel swapped out about 1.9 GB of TiKV.

## 15. Licensing

| Component | License | Use |
|---|---|---|
| TiKV | Apache-2.0 | Run unmodified |
| PD | Apache-2.0 | Run unmodified |
| TiDB | Apache-2.0 | ~~Run unmodified (SQL, and optionally a unified GC worker)~~ Not deployed (D260) |
| TiCDC | Apache-2.0 | ~~Later, for TiDB-table capture (Q34)~~ Not used (D260) |
| tidb-operator (TiDB Operator v2) | Apache-2.0 | Kubernetes deployment of PD and TiKV only (R4, D179) |
| tiup | Apache-2.0 | Dev and CI clusters |
| TiProxy | Apache-2.0 | ~~Candidate MySQL proxy (R4)~~ Not used (D260) |
| TiFlash | Apache-2.0 | ~~Optional columnar and vector add-on for SQL tenants (R4, D131)~~ Not used (D260) |
| BR (a tool in the TiDB repo; no TiDB server needed) and TiKV `backup-stream` | Apache-2.0 | Mandatory backup and PITR of Live and metastore keyspaces to object storage (D131) |
| kvproto (`pdpb`, `cdcpb`, `keyspacepb`) | Apache-2.0 | Vendored protos for the GC-state client |
| `tikv-client` (client-rust) 0.4.0, from Loams's fork `ostrium-labs/client-rust` (R1 plan row F1) | Apache-2.0 | Dependency |
| `connectrpc` 0.9 (connect-rust) | Apache-2.0 | Dependency |
| `buffa` 0.9 | Apache-2.0 | Dependency |
| `rquickjs` 0.14, QuickJS-ng | MIT | Dependency |
| `wasmtime` | Apache-2.0 WITH LLVM-exception | Option (Q35) |
| `v8` (rusty_v8), `deno_core` | MIT | Option (Q35) |
| `boa_engine` | MIT OR Unlicense | Considered, not chosen |
| protobuf-es, connect-es, connect-go, connect-swift, connect-kotlin, connect-python, `buf` | Apache-2.0 | Client generation |
| esbuild | MIT | Bundling user functions (CLI) |
| Resonate server and Rust SDK | Apache-2.0 | Later option for durable actions |
| Convex backend | FSL-1.1-Apache-2.0 | **Read for concepts only; no code copied** |
| convex-js | Apache-2.0 | Not used |

Every dependency is compatible with D11. Running PD and TiKV unmodified as separate processes creates no obligation beyond Apache-2.0 notices in distributed images.

## 16. Open questions

| # | Question | Needed by |
|---|---|---|
| Q31 | Serializable range reads in mutations: guard keys per equality-prefix bucket, or validation against the journal after prewrite (§5.2) | R2 plan |
| Q32 | ~~MVCC GC for txn-API keyspaces: does TiKV honour keyspace-level safe points set through PD's GC-state API, or must a `unified` GC TiDB run per cluster; and will `client-rust` accept a patch exposing keyspace GC (§9.3)~~ **Answered by R1 Task 0 (2026-09-27): no.** PD v8.5.8 has no GC-state RPCs and TiKV reads only the cluster safe point, so Loams's GC loop is the cluster's GC worker for every keyspace (§9.3; the decision log's Q32) | Answered |
| Q33 | ~~Do released classic TiDB binaries (v8.5.x) support `keyspace-name`?~~ **Verified on playground v8.5.8**: yes, with the keyspace pre-allocated in PD and TiKV on API v2 (§10.1). **Moot 2026-09-29 (D260: no TiDB); the tidb-operator follow-up is withdrawn** | Moot |
| Q34 | ~~Can TiCDC capture a keyspace-mode TiDB's tables on a classic cluster into a Kafka or storage sink, for SQL → collections (§10.3)~~ **Moot 2026-09-29 (D260: no TiDB)** | R3 plan |
| Q35 | The long-term function engine: QuickJS only, or V8 (`deno_core`) for CPU-bound functions and npm compatibility, or wasmtime for Rust and Go functions (§6.3) | R2 plan |
| Q36 | Keyspaces per cluster before region overhead dominates, and whether TiKV request units can be attributed to a txn-API keyspace; these set the size-class thresholds and per-app quotas (§9.2) | R2 plan |
| Q37 | Do BR's log backup and PITR restore cover a txn-API keyspace (not only TiDB tables), per keyspace, on a classic cluster; what recovery point does the default flush interval give (§10.4, D131) | R2 plan |
| Q260 | MySQL wire access after D260: Loams's own read-only MySQL wire front end over DataFusion, or no MySQL surface (§10). Not decided | Before any MySQL-protocol work |
| Q38 | What `tiup playground --mode tidb-x` / `tidb-cse` (next-gen, S3-backed TiDB) runs: where its TiKV binaries come from, under what license, and whether they can be self-hosted. If they are open, S3 could become the source of truth for Live keyspaces, removing the D130 tension (§10.4) | R2 plan |

## 17. Contradictions with earlier decisions, and how they are resolved

| Earlier | Conflict | Resolution |
|---|---|---|
| D1: object storage is the only source of truth; compute is stateless | TiKV keeps Live data on local disks with Raft replication | D130: D1 holds for the retrieval engine; Loams Live's source of truth is TiKV. The Live role itself stays stateless. Mandatory BR log backup puts a restorable copy of every Live keyspace in object storage (D131); TiDB's next-gen S3 kernel would remove the tension, but its TiKV engine is not open (§10.4) |
| D2: OLTP is out of scope | Loams Live is an OLTP database | D130: D2 holds for the retrieval engine; OLTP enters Loams as a separate product line on TiKV, not on the bucket |
| D58, §18 §2.4 and the avoid list of [11-buy-vs-build](11-buy-vs-build.md) (`tikv-client` rejected; TiDB over sqlx in M6) | D124 uses `tikv-client` in R1 | D124 supersedes D58's TiDB clause and the `tikv-client` entry on the avoid list |
| D42: a narrow protocol footprint | MySQL is a new protocol | ~~Loams does not implement it; TiDB does, unmodified (D123)~~ D260 removes TiDB; whether Loams adds a MySQL surface of its own is Q260 |
| D123: TiDB for MySQL | D-SC-16 and the owner's 2026-09-29 direction: TiKV only | D260 supersedes D123 |
| M1.6 Ruling 4: the TypeScript SDK has zero runtime dependencies | Generated Connect clients depend on `@bufbuild/protobuf` and `@connectrpc/connect` | The M1.6 SDKs keep REST and zero dependencies; the proposed M1.6 amendment (D128) applies only to protobuf surfaces (native gRPC, streams), where generated clients replace hand-written ones |
| The owner's note "moved up from M6 (D72)" | D72 is the native stream API; TiDB's M6 placement is D58 | D124 cites D58 |

## 18. Roadmap (D127)

| Milestone | Scope | Exit gate |
|---|---|---|
| **R1** | `loams-tikv`; `loams-meta-tikv` passing conformance and its fault matrix; keyspace GC loop; one Live app in one keyspace: documents, tables, indexes, built-in and QuickJS queries and mutations, the commit journal, reactive subscriptions, the sync API (`Watch`, `ModifyQuerySet`, `Query`, `Mutate`, `Deploy`) over connect-rust; the generated TypeScript client with a reactive layer; ~~TiDB SQL in a separate keyspace in the dev playground~~ (dropped, D260) | The reactive correctness checker and the transaction checker pass, including under the fault matrix; the metastore conformance suite passes on TiKV; a TypeScript client sees a live query update after a mutation |
| **R2** | The namespace router: directory, shared keyspaces, app lifecycle, moves, quotas; the `ControlStore` on `_control` (D125); actions and scheduled functions; online index backfill; the Q31 decision; multi-node sessions; generated Python and Go clients; **BR log backup (PITR) to object storage for every Live and metastore keyspace, with a tested restore** (D131); a `gc_blocked_seconds` gauge per keyspace and an alert on repeated cluster-GC failures of one keyspace, which hold the cluster safe point back for all (R1 plan row T3-12); `drop_collection` of a collection with a large index in batches rather than one transaction (R1 plan row T5-15) | 10 000 apps on one cluster; the nemesis suite green for 24 h; a console page served from live queries; a point-in-time restore of a Live keyspace from object storage passes the reactive checker's state comparison |
| **R3** | The collections bridge and `ctx.search` (D129); auth on the Live API through the unified auth plan (D111, Q30); Swift and Kotlin clients; React hooks | A searchable table stays in step under a crash loop, exactly once; search after a mutation with `after_ts` sees it |
| **R4** | Kubernetes: TiDB Operator v2 for PD and TiKV only (D179), Loams's Helm chart for the Live role; backup operations in the operator; BYOC for Live; durable actions (Resonate) as an option | A cluster deployed from the chart passes the nightly suite; restore from backup passes |

R runs beside M1 and M2. The build machine builds one crate graph at a time (shared target directory, 6 jobs), so R and M tasks interleave rather than run in parallel; the playground runs only between builds. The first plan is [`docs/plans/2026-09-27-r1-reactive-core.md`](../plans/2026-09-27-r1-reactive-core.md).

**Shared proto tooling (D128).** The native stream API's gRPC surface (D72, M2) and Loams Live use one toolchain: buffa messages, connect-rust services, `buf` for client generation. The proposed M1.6 amendment: where an SDK covers a protobuf service (the native gRPC protos of D101 and the stream API), it wraps the generated client instead of hand-writing transport code; the REST SDKs of M1.6 are unchanged. It is recorded here and in D128, and M1.6 is not rewritten.

## 19. Sources

- Convex (concepts only): stack.convex.dev/how-convex-works · docs.convex.dev/database/advanced/occ · docs.convex.dev/functions/actions · docs.convex.dev/scheduling/scheduled-functions · docs.convex.dev/database/document-ids · docs.convex.dev/database/reading-data/indexes · docs.convex.dev/database/pagination · docs.convex.dev/production/state/limits · docs.rs/convex_sync_types · github.com/get-convex/convex-backend/blob/main/LICENSE.md
- TiKV `548812e`: `components/api_version/src/{api_v2.rs,keyspace.rs}`, `src/storage/{mod.rs,config.rs}`, `src/server/raft_server.rs`, `components/cdc/src/{service.rs,endpoint.rs,delegate.rs}`, `components/cdc/tests/mod.rs`, `components/pd_client/src/client.rs`, `src/server/gc_worker/`
- PD `9186d07`: `pkg/keyspace/{keyspace.go,util.go}`, `pkg/gc/gc_state_manager.go`, `server/apiv2/handlers/keyspace.go`, `server/config/config.go`, `pkg/mcs/resourcemanager/server/manager.go`
- TiDB `8936d7b`: `pkg/config/config.go`, `pkg/store/driver/tikv_driver.go`, `pkg/store/gcworker/gc_worker.go`, `pkg/keyspace/doc.go`, `pkg/domain/crossks`, `pkg/resourcegroup/`
- client-rust `ab4be1c` (`tikv-client` 0.4.0): `Cargo.toml`, `README.md`, `src/lib.rs`, `src/config.rs`, `src/request/keyspace.rs`, `src/transaction/{client.rs,transaction.rs}`, `src/raw/client.rs`, `src/pd/cluster.rs`, `src/generated/cdcpb.rs`, `proto/{pdpb,cdcpb,keyspacepb}.proto`, `proto/VERSION`
- TiCDC: `logservice/logpuller/`, `logservice/eventstore/`, `downstreamadapter/sink/`, `pkg/config/changefeed.go`
- tidb-operator (v2, `main`): `api/core/v1alpha1/tidb_types.go`, `pkg/configs/{tidb,tikv}/config.go`
- connect-rust `fb5f5aa` (`connectrpc` 0.9.0): `README.md`, `docs/guide.md`; buffa 0.9.2: `README.md`
- Resonate: `README.md`, `impl/sdk/rs`
- Loams: §18 (the contract and backends), §19 on PR #39 (console and tenancy), `docs/plans/2026-09-25-m1.2a-metastore-trait.md`, `docs/plans/2026-09-24-m1.6-sdks-mcp.md`
