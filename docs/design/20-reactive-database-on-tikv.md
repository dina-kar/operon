# 20 — Loam Live: a Reactive Database on TiKV, TiDB SQL Access and the TiKV Metastore

Status: **Proposed** · 2026-09-27. The direction (D116–D118, D122–D124, D126–D128) was approved by the owner on 2026-09-27: "build convex like layer on TiKV, and use it for our metadata and control if possible, other than convex like api, can we provide mysql protocol for normal uses, build this as soon as possible, loam will a ai native cloud". The design choices this document makes on top of that direction (D119–D121, D125, D129, D131) are **proposals** until the owner confirms them. Open questions are Q31–Q38 in §16. Measurements and cluster facts marked **(spike)** come from the TiKV feasibility spike of 2026-09-27 on `tiup playground` v8.5.8 with `tikv-client` 0.4.0 (report: `.superpowers/research/tikv-spike.md` in the m1.2a worktree, not committed); its latencies were taken on a heavily loaded shared machine and are indicative only.

**R1 is built** (2026-09-28), except its TiDB task (R1 Task 15, parked until the owner decides about TiDB). Where R1 changed this design, the text below says what was built and cites the R1 plan's row (`T<task>-<n>`, in [`2026-09-27-r1-reactive-core.md`](../plans/2026-09-27-r1-reactive-core.md), "Rulings made during execution"). §20 lists the changes in one place, and the [R1 exit report](../plans/r1-exit-report.md) has the gate results and measurements.

This document starts a second product line beside the retrieval engine (§00–§18). It amends D1 and D2 for that product line only (D130), supersedes D58's TiDB-over-sqlx clause (D124), and adds a parallel roadmap track, **R** (D127).

Markers: **(estimate)** is computed from code or specs, not measured. **(verify)** is not checked against a primary source; the plan that builds it resolves it (R1 Task 0 checks the ones R1 depends on). Paths of the form `tikv/…`, `pd/…`, `tidb/…`, `client-rust/…`, `ticdc/…`, `connect-rust/…` point into the reference clones under `~/Documents/research-clones/` as of 2026-09-26 (TiKV `548812e`, PD `9186d07`, TiDB `8936d7b`, client-rust `ab4be1c`, connect-rust `fb5f5aa`).

---

## 1. Summary

| # | Decision | Status | Track |
|---|---|---|---|
| D116 | Loam is an **AI-native cloud**: the retrieval engine, a reactive application database, SQL, streams and AI-gateway integration | Approved (owner) | — |
| D117 | **Loam Live** (working name "Loam Reactive"), a Convex-style reactive database built on TiKV: reactive queries, server functions, a protobuf sync API, a namespace router over keyspaces, and a bridge into Loam collections | Approved (owner); the name is proposed | R1–R4 |
| D118 | A mutation is **one TiKV optimistic transaction**, retried on conflict; isolation is snapshot isolation with point reads promoted to locks; serializable range reads are open (Q31) | Approved (owner) for the transaction model; the isolation detail is proposed | R1 |
| D119 | Invalidation comes from a **sharded, sequenced commit journal** written inside each mutation's transaction; TiKV CDC (validated from Rust with `kv_api=TiDB`) is a secondary path for consumers that tolerate ~1 s lag | Proposed | R1 |
| D120 | Server functions run in **QuickJS through `rquickjs`** in R1; V8 and wasmtime stay options (Q35) | Proposed | R1 |
| D121 | The sync API is **protobuf over connect-rust** (Connect, gRPC, gRPC-Web): a server-streamed session plus unary calls; clients are generated | Proposed (the transport was the owner's choice) | R1 |
| D122 | **Multi-tenancy by keyspace**, with size classes: large apps and every SQL tenant get their own keyspace; small apps share one under a key prefix | Approved (owner) for the router; the size classes are proposed | R2 |
| D123 | **MySQL protocol through unmodified TiDB**, one TiDB pool per SQL-enabled keyspace, on the same TiKV cluster; Loam builds no MySQL server | Approved (owner) | R1 (dev), R2 (tenants) |
| D124 | **`operon-meta-tikv`**, a `MetaStore` backend over `tikv-client`, moves from M6 to R1 and becomes the backend for Loam cloud metadata; Postgres and DynamoDB stay in v1.0; openraft stays the default | Approved (owner) | R1 |
| D125 | The **control plane's `ControlStore` runs on Loam Live** (a system app in its own keyspace), not on TiDB SQL | Proposed | R2 |
| D126 | **PD, TiKV, TiDB and TiCDC run unmodified** from official releases; tidb-operator on Kubernetes; `tiup playground` in dev and CI; no forks | Approved (owner) | R1, R4 |
| D127 | **Track R** runs beside M1, interleaved on the one-build machine; R1 = TiKV metastore + minimal reactive core + TiDB SQL in dev | Approved (owner) | — |
| D128 | One **proto toolchain** (buffa + connect-rust, `buf` for clients) for Loam Live and the native stream API; M1.6's SDKs should reuse generated clients where possible (a proposed M1.6 amendment) | Approved (owner) for the shared toolchain; the M1.6 amendment is proposed | R1, M2 |
| D129 | The **collections bridge** tails the commit journal into a collection's implicit stream with exactly-once producer sequences, so Live tables become searchable | Proposed | R3 |
| D131 | **TiDB's object-storage, vector and full-text features:** the next-gen S3 kernel is not usable self-hosted; TiFlash is an optional add-on for SQL tenants; TiDB full-text is not used; **BR log backup (PITR) to object storage is mandatory for every Live cluster** | Proposed (owner question, 2026-09-27) | R2 (backup), R4 (TiFlash) |
| D130 | D1 (object storage is the only source of truth) and D2 (OLTP out of scope) **keep holding for the retrieval engine** and do not apply to Loam Live, whose source of truth is TiKV | Proposed | — |

## 2. Goals and non-goals

### 2.1 Goals

1. **Convex's developer model on infrastructure Loam can run and sell.** Documents in tables with indexes; queries that stay live and push new results; mutations that are transactions and retry themselves; actions for side effects. All of it runs on open-source components under Apache-2.0 or MIT (§15).
2. **One cluster for application data, SQL and metadata.** A TiKV cluster holds Loam Live apps, TiDB SQL databases and Loam's own metadata, each in its own keyspace (§9).
3. **App data becomes retrievable.** A Live table can be declared searchable, and the bridge keeps a Loam collection in step with it, so vector, full-text and hybrid search run over application data without an ETL job (§12). This is the differentiator: Convex has search indexes inside the database, but not Loam's hybrid retrieval, hot tier, Qdrant and Elasticsearch surfaces or scan plans.
4. **Clients on every platform from one contract.** Protobuf services generate clients for the web, iOS, Android, Python and Go (§7).
5. **The TiKV metastore early.** Loam cloud needs a scale-out, transactional metadata store with a managed option. TiKV gives the `MetaStore` contract in full (§11), and the same cluster then serves the control plane (§9.5).
6. **Ship fast.** R1 is small and reuses what exists: the `MetaStore` conformance suite and fault-matrix design (§18 §4), the M1.3 routing, the D111 loopback defaults.

### 2.2 Non-goals

- **No fork of PD, TiKV, TiDB or TiCDC** (D126). Anything that needs a server change goes upstream or is not done.
- **No MySQL server of our own** (D123). TiDB is the MySQL surface.
- **No SQL over Live tables in R1–R3.** TiDB tables and Live tables live in different keyspaces with different encodings; neither sees the other's data (§10.3).
- **No Convex compatibility.** Loam Live borrows concepts, not the wire protocol, the function API names or any code. Convex's backend is FSL-1.1 and is read for concepts only (§15).
- **Not a replacement for the retrieval engine's log and bucket model.** Collections, streams, tables and graphs keep object storage as the source of truth (D1, D130). Loam Live is the OLTP store beside them.
- **No auth in R1.** It follows D111, more strictly: the Live listener binds 127.0.0.1 and **refuses** any non-loopback address (§7.1); auth arrives with the unified auth plan (R3).
- **No offline-first sync.** Clients hold query results and optimistic updates, not a local replica with merge.

## 3. Architecture

```
   Web / iOS / Android / Python / Go clients              MySQL clients, ORMs, BI tools
   (generated Connect / gRPC / gRPC-Web stubs             │
    + a thin reactive client per platform)                │ MySQL protocol
                  │ HTTP/1.1 or HTTP/2                     │
                  ▼                                        ▼
 ┌──────────────────────────── operon binary, role `live` ───────┐   ┌────────────────────────┐
 │  Sync API (connect-rust)                                      │   │ TiDB (unmodified),      │
 │   Watch (server stream) · ModifyQuerySet · Query · Mutate     │   │ one pool per SQL        │
 │   · Deploy                                                    │   │ keyspace, keyspace-name │
 │        │                                                      │   └───────────┬────────────┘
 │  Session manager ── query sets, transitions, backpressure     │               │
 │        │                                                      │               │
 │  Subscription manager ── read-set index (interval trees),     │               │
 │        │                  result cache, rerun scheduler       │               │
 │        │  ▲ invalidations                                     │               │
 │  Function runner (QuickJS isolates) ── queries, mutations     │               │
 │        │                    │                                 │               │
 │  LiveTxn (read set, write set, retry) │  Journal tailer ──────┘               │
 │        │                    │                                                 │
 │  Namespace router: app → keyspace (+ prefix)   ── ControlStore (R2) ──┐       │
 └────────┼─────────────────────────────────────────────────────────────┼───────┘
          │ tikv-client (txn API v2, keyspace codec)                    │
          ▼                                                             ▼
 ┌──────────────────────────────── TiKV cluster (unmodified) ──────────────────────────────┐
 │ PD: TSO, region placement, keyspace metadata, GC states                                   │
 │ TiKV: Raft-replicated regions, Percolator transactions, MVCC                              │
 │  keyspace loam_meta     keyspace loam_control   keyspace app_…      keyspace sql_…         │
 │  (operon-meta-tikv)     (Live system app, R2)   (Live apps)         (TiDB, per tenant)     │
 └───────────────────────────────────────────────────────────────────────────────────────────┘
          ▲                                     │ journal tail (R3)
          │ MetaStore calls                     ▼
 ┌────────┴─────────────────────┐   ┌──────────────────────────────────────────────┐
 │ Retrieval engine (§01–§18):  │◀──│ Collections bridge: journal → DocOps → the     │
 │ gateways, log, query, worker │   │ collection's implicit stream (exactly once)   │
 │ roles; bucket = truth        │   └──────────────────────────────────────────────┘
 └──────────────────────────────┘
```

- **Loam builds** everything inside the `operon` box, the bridge and the metastore backend. **Loam runs unmodified** PD, TiKV and TiDB.
- The Live role is stateless: sessions, subscriptions and the result cache are derived state, rebuilt when a client reconnects (§13).
- One TiKV cluster per region serves all four kinds of keyspace. Separate clusters per product (a metadata cluster, an app cluster) are an operational choice, not a design requirement.

### 3.1 Crates (R1)

Crates keep working names until the rename (D33).

| Crate | Owns |
|---|---|
| `operon-tikv` | The TiKV client layer every TiKV user shares: config and keyspace bootstrap through PD's HTTP API, the TSO clock, a transaction runner with error classification, retries and a `FaultPlan` hook, commit tokens, the order-preserving tuple codec, the cluster MVCC GC loop (§9.3), the test harness |
| `operon-meta-tikv` | `impl MetaStore` over `operon-tikv` (§11) |
| `operon-live-proto` | The `loam.live.v1` protos and the Rust code generated from them (buffa messages, connect-rust services) |
| `operon-live` | Data model and key layout, `LiveTxn`, the commit journal and tailer, the subscription and session managers, the sync service, the Elle-style history checker (`operon_live::testing::elle`, T16-4) |
| `operon-live-js` | The QuickJS function runtime (`rquickjs`) and the host database API |
| `sdks/live-typescript` | `@operon/live` (renamed `@loamdb/live` with D33): generated protobuf-es and Connect stubs plus the reactive client |

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

All keys are inside the app's keyspace, which the client codec prefixes with `x` and the 3-byte keyspace id (`tikv/components/api_version/src/api_v2.rs:16-20`; `client-rust/src/request/keyspace.rs:11-13`). A shared keyspace (§9.2) adds an app prefix.

```
app prefix      = ""                              (dedicated keyspace)
                | 0xA0 ‖ app_id:u32 BE            (shared keyspace)

catalog         = prefix ‖ 0x01 ‖ kind:u8 ‖ name                 → counters | table id | TableDef | Deployment record | Schema | AppDef
document        = prefix ‖ 0x02 ‖ table_id:u32 BE ‖ doc_id[16]    → DocumentRecord (protobuf)
index entry     = prefix ‖ 0x03 ‖ table_id:u32 BE ‖ index_id:u32 BE ‖ tuple(values…) ‖ creation_ms:u64 BE ‖ doc_id[16] → ""
journal head    = prefix ‖ 0x04 ‖ 0x00 ‖ shard:u16 BE             → last sequence:u64
journal entry   = prefix ‖ 0x04 ‖ 0x01 ‖ shard:u16 BE ‖ seq:u64 BE → JournalEntry (protobuf)
checkpoint      = prefix ‖ 0x04 ‖ 0x02 ‖ shard:u16 BE ‖ consumer   → seq:u64 BE ‖ expires_ms:u64 BE
idempotency     = prefix ‖ 0x05 ‖ key_hash[16]                   → IdempotencyRecord (protobuf)   (key_hash = first 16 bytes of SHA-256(key))
scheduler (R2)  = prefix ‖ 0x06 ‖ …
```

**As built (R1).**
- **Catalog kinds** (T8-4, T10-1, T13-10): `0x00` counters (the next table id), `0x01` a table's name → its id, `0x02` a table by id → `TableDef`, `0x03` the deployment record, `0x04` the schema record, `0x05` the app record `AppDef { journal_shards }`. The records are in the internal `catalog.proto`.
- **The idempotency record** (T10-6, T11-13) is `IdempotencyRecord { format, result, expires_ms, function, start_ts, args_hash }` in the internal `idempotency.proto`. It holds **no commit timestamp**, because a transaction cannot write its own. A replay returns the timestamp of the read that found the record, which is at or after the commit. That is an upper bound, and it is safe for a client that waits for `commit_ts` before it drops an optimistic update. A key reused for another function, or with other arguments, is `INVALID_ARGUMENT`.
- **Indexes** (T8-5): `by_creation_time` entries have an empty tuple part, and `by_id` has no entries, because the document key is the index. A missing indexed field is indexed as `null`.

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

The order is null < int64 < float64 < bool < string < bytes < array. Objects are not indexable in R1. Order preservation is property-tested against a reference comparator: the tuple codec in R1 Task 2 (`tuple_order_matches_reference`), and index keys against value order in Task 8 (`index_keys_sort_in_value_order` without a cluster, `index_scan_order_matches_value_order` on TiKV). Arrays nest at most 64 deep (T3-10).

**Document bodies** are a protobuf `DocumentRecord { format: 1, creation_ms, fields: map<string, Value> }` from the same `loam.live.v1` package the clients use, so a value means the same in storage, on the wire and in every generated client.

## 5. Transactions and isolation (D118)

### 5.1 The transaction model

- **A mutation is one TiKV optimistic transaction** (`client-rust/src/transaction/client.rs:174`, `begin_optimistic`). The function reads at the transaction's start timestamp, buffers writes, and commits. TiKV's Percolator two-phase commit makes the commit atomic across regions and keyspace ranges.
- **Retry on conflict.** A write conflict at prewrite aborts the transaction; the runner reruns the whole function at a new start timestamp, with jittered backoff, up to the attempt budget or the mutation's deadline. Functions are deterministic (§6.2), so a rerun is safe. **As built:** Live mutations get **16 attempts** (`DEFAULT_MUTATION_ATTEMPTS`, owner ruling T10-3) and the metastore keeps 8. The runner scales its backoff pauses down so that a run's pauses fit half its deadline (T11-1). It also reruns on `NotApplied`, which means the transaction certainly did not commit (T2-4, T2-16).
- **Queries** read a snapshot (`snapshot`, `client.rs:233`) at the tick timestamp of the subscription manager (§8.2), so every query in a session is evaluated at one timestamp.
- **Timestamps** are PD TSO values (physical ms << 18 | logical). They are Loam Live's version numbers: a query result is "valid at ts", a mutation returns its commit timestamp, and a client waits until its query set has reached that timestamp before it drops an optimistic update.
- **Limits per mutation (R1 defaults):** 8 MiB written, 16 000 documents written, 32 000 documents scanned, 4 096 index ranges, 1 s of JavaScript CPU, a 10 s wall-clock deadline. These follow Convex's published limits in shape, scaled down for R1, and are configurable.
- **Idempotency.** A `Mutate` call may carry an idempotency key. The runner writes the key's record inside the same transaction. On a retry of the call it returns the recorded result, with the timestamp of the read that found the record, instead of running again (§4.3 as built, T10-6). A lost acknowledgement therefore never applies a mutation twice. A read-only mutation writes no record.
- **Commit options.** **As built, R1 commits with two-phase commit** (`commit_mode = two_pc`) for the metastore and for Live, and async commit with 1PC is a switch (owner ruling T7-1, after row T6-5). The pinned client resolves an async-commit lock on the read path with `CheckTxnStatus` only, which keeps the primary locked. So a crashed async-commit writer blocked readers until GC ran, about 10 minutes later. The plan's default, kept here as history: async commit with 1PC (`use_async_commit` and `try_one_pc`, `transaction.rs:1206,1213`). In the spike they cut commit p50 by roughly 30–50% against 2PC: about 3.5–5.3 ms against 7.1–7.3 ms for optimistic transactions under heavy host load **(spike)**. Two risks come with them:
  1. An async-commit timestamp can be larger than a start timestamp fetched later by another client unless `min_commit_ts` is seeded from a fresh TSO, as TiDB's `tidb_guarantee_linearizability` does.
  2. `tikv-client` 0.4.0's async commit `unwrap()`s `min_commit_ts` and never sets `max_commit_ts` (a FIXME in `transaction.rs`) **(spike)**.

  So the commit mode is a config switch (`commit_mode = async_1pc | two_pc`). The R1 gates (the linearizability histories of the metastore suite, and the reactive and transaction checkers) pass with `two_pc`. A component moves back to `async_1pc` by a ruling, once the pinned `tikv-client` resolves async-commit and 1PC locks on the read path (§11.4 item 2) and its gates pass with it. The exit report measured Live's commit p50 at 38–49 ms with `two_pc` and 27–32 ms with `async_1pc` on a loaded host.

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

- **As built (T10-4, T11-2):** R1 also has an opt-in, `RunnerOptions::serializable_ranges`, off by default. A mutation that read an index range and writes also locks every journal head, so it conflicts with every mutation that commits between its start and its commit. It is correct but coarse, and R2's journal validation (Q31) replaces it. The transaction checker (§14) found no anomaly at all, G2 included, because its workload reads by id (T16-8).

### 5.3 The commit journal (D119)

Every mutation transaction also writes one **journal entry** into its app's journal. The entry lists what the transaction changed:

```
JournalEntry {
  commit_hint_ms,                        // TSO physical time of the start timestamp; the janitor's age test (T9-4)
  writes: [ { table_id, doc_id, kind: insert | replace | delete,
              index_keys_removed: [bytes], index_keys_added: [bytes] } ],
  function, request_id                   // for tracing
}
```

**Sequencing.** The journal has `S` shards (**64 by default** as built, owner ruling T11-1; up to 1 024). The count is stored per app in its catalog (`AppDef`) and changes only while the journal is empty (T10-1). A mutation picks a shard, reads the shard's head `h` at its start timestamp, and writes the entry at `seq = h + 1` and the head `= h + 1` in the same transaction. Two mutations that pick the same shard conflict on the head and one retries on another shard: a rerun draws uniformly among the shards other than its previous attempt's (T10-2). An entry larger than 1 MiB is split over consecutive sequences of its shard, in the same transaction (T9-3). So each shard's sequence is **dense and ordered by commit**, and an entry is visible at timestamp `T` exactly when its transaction committed at or before `T`.

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

**Retention.** A janitor deletes entries once every consumer (each node's tailer, the bridge) has passed them and they are older than `journal_retention` (10 min). Consumers checkpoint per shard. **As built (T9-5, T11-11):** a node's tailer checkpoints with a 2-minute TTL, so a crashed node cannot hold the journal. A checkpoint without a TTL is for the bridge (R3). The janitor also deletes expired idempotency records (T10-6). At 64 shards, 32 writers and 2 000 mutations, the rerun rate was 0.25–0.28 per mutation (the exit report).

## 6. Server functions

### 6.1 Kinds

| Kind | Transactional | Side effects | Retried by Loam | Where |
|---|---|---|---|---|
| **Query** | Reads one snapshot | None | Rerun on invalidation | R1 |
| **Mutation** | One TiKV transaction | None | On conflict, up to 16 attempts (T10-3) | R1 |
| **Action** | No; each `runQuery`/`runMutation` inside it is its own transaction | `fetch` (allowlisted hosts), AI-gateway calls | Never automatically | R2 |
| **Scheduled function** | A mutation schedules it transactionally (a row in the scheduler range) | As its kind | Mutations exactly once; actions at most once | R2 |

Durable actions (steps that survive a crash and resume) can later run through the Resonate protocol (§14, Apache-2.0, with a Rust SDK at `resonate/impl/sdk/rs`), as an R4+ option.

### 6.2 Determinism

Queries and mutations must return the same result from the same snapshot:

- `Date.now()` returns the transaction's start timestamp in ms; `Math.random()` is a deterministic PRNG seeded from the start timestamp and the request id, and is documented as not suitable for secrets. **`crypto.getRandomValues()` and `crypto.randomUUID()` are never seeded**: in queries and mutations they throw (`DeterminismError: crypto randomness is not available in queries and mutations; use an action`), and in actions (R2) they draw from the OS CSPRNG. A deterministic value can then never be mistaken for a cryptographic one. Document ids are drawn by the host from the OS CSPRNG (§4.1), outside the function's view.
- No timers, no `fetch`, no network, no filesystem.
- Host calls (`db.get`, `db.query`, `db.insert`, `db.patch`, `db.replace`, `db.delete`) are the only I/O. Each read call adds to the read set.
- **As built (T13-8, T13-9):** `Math.random` is ChaCha8, seeded with SHA-256 of the start timestamp and the request id (the idempotency key, or empty). `setTimeout`, `fetch`, `WebAssembly` and the like are absent, and `console` is a no-op. Built-ins and `globalThis` are deep-frozen with override taming, so an Error subclass can still set `this.name`. A host call that fails with a storage error or an exceeded limit ends the call at once, and no JavaScript `catch` sees it (owner ruling T14-2).

### 6.3 The sandbox (D120, Q35)

| Option | License | For | Against |
|---|---|---|---|
| **V8 through `rusty_v8` (`v8` crate) or `deno_core`** | MIT (both) | Fastest (JIT); what Convex and Deno run; best npm compatibility | A prebuilt static library of ~100 MB or a from-source build of V8; long link times on a machine that builds one crate graph at a time; isolate snapshots and determinism need care |
| **QuickJS through `rquickjs`** (QuickJS-ng underneath) | MIT (both) | Small, compiles from C source in seconds; interrupt handler for CPU limits; per-runtime memory limit; async host functions; ES2023 modules | An interpreter: 10–50× slower than V8 on CPU-bound code **(estimate)**; smaller ecosystem |
| **WASM through `wasmtime`** | Apache-2.0 WITH LLVM-exception | Fuel metering; strong isolation; polyglot (Rust, Go, Python components) | TypeScript needs a JS engine compiled to WASM anyway (Javy embeds QuickJS, StarlingMonkey embeds SpiderMonkey); the component model tooling adds a build step for users |
| Boa (`boa_engine`) | MIT OR Unlicense | Pure Rust | Less complete and slower than QuickJS today |

**Recommendation for R1: QuickJS through `rquickjs` 0.14.** Server functions in Convex are mostly small, I/O-bound TypeScript that reads and writes a few documents, so an interpreter's speed is enough. QuickJS keeps the build small (it matters on a machine that builds one crate graph at a time) and gives CPU and memory limits out of the box. Users write TypeScript; the CLI bundles it to one ES module with esbuild (MIT) before `Deploy`. wasmtime comes back when users want functions in Rust or Go; V8 comes back if profiles show CPU-bound functions. The function API is engine-neutral, so switching engines does not change user code.

**Runtime model, as built (T13-5, owner ruling T14-1).** **Each pooled context has its own QuickJS runtime, on its own worker thread.** A deployment gets `contexts` of them: 4 by default, set with `--live-js-contexts 1–256`. The memory limit (64 MiB) is therefore per call, so one deployment can use up to `contexts` × 64 MiB. rquickjs runtimes are not `Send` without its `parallel` feature, and one runtime per thread keeps an interrupt or an out-of-memory error from hitting a neighbouring call. R2 revisits one shared runtime per deployment with `parallel`. The CPU limit counts the time a call runs JavaScript, excluding its waits on host calls (T13-6). A copied result is bounded at 2^21 parts and 64 MiB of strings and buffers (T14-11, T16-12). Functions are addressed as `module:export` (T13-7). The plan's model, kept as history: one QuickJS runtime per (node, app, deployment), with a pool of pre-initialised contexts; memory limit 64 MiB per runtime; the interrupt handler enforces the CPU limit. **A context serves exactly one invocation and is then discarded**, whether the call succeeded or threw: module-level variables, caches and patched globals can never carry state from one call to the next, which §6.2's determinism needs. The pool keeps `contexts` (4) fresh contexts warm, each with the bundle's module already evaluated, and refills in the background, so the evaluation cost stays off the request path. Built-in globals are frozen before the bundle is evaluated. Deployed bundles are stored in object storage (`live/<app>/deployments/<id>.js`) and the current deployment is a pointer in the app's catalog. **As built (T13-10, T14-3, T9-1):** `Deploy` validates the bundle, checks whether the schema changes the indexes of an existing table, and if so takes the app's gate, which refuses with `UNAVAILABLE` (busy) while a mutation is in flight. Only then does it store the bundle and commit the records. A refused deploy stores nothing. A running subscription moves to the new code at its next evaluation (T13-11).

## 7. Sync protocol (D121)

### 7.1 Services

```proto
package loam.live.v1;

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
- **Session routing.** A session lives on the node that holds its `Watch` stream. `session_id` carries that node's id; another node that receives a `ModifyQuerySet` forwards it with M1.3's request forwarding. **R1 has one node and no forwarding:** an unknown session is `NOT_FOUND` (T12-14).
- **Encoding.** Connect's JSON and binary codecs both work; `int64` values are strings in JSON and `bigint` in protobuf-es, so ids and counters stay lossless in TypeScript.
- **As built (T7-5, T12-4–T12-6, T13-18, T16-1).**
  - **Messages.** The `Transition` above, plus the messages row T7-5 lists: `Null {}`, `Resume`, `QuerySetChange`, `QueryUpdate { query_id; value | error | removed }`, `QueryRequest { function, args, optional ts }`, and `LiveError { code, message }` with `ErrorCode`.
  - **Heartbeats and ts-only Transitions.** A heartbeat is an empty Transition with `start == end`. A `Mutate` that carries the header `loam-session-id` marks its commit timestamp pending in that session, and ts-only Transitions follow at most once per second until the session reaches it.
  - **Resume.** A resumed session's first Transition starts at the client's `last_version` and carries every result.
  - **Errors.** They map to Connect codes: `FUNCTION_ERROR` → `unknown`, `FUNCTION_TIMEOUT` → `deadline_exceeded`, `FUNCTION_OUT_OF_MEMORY` and limits → `resource_exhausted`, busy → `unavailable`. **Every error also carries its `LiveError` as a Connect error detail of type `loam.live.v1.LiveError`** (`service::ERROR_DETAIL_TYPE`), so clients read the exact code. `@operon/live` reads the detail first and falls back to the Connect code.
  - **Query without `ts`.** It reads at the manager's current tick, which is `tick_read_lag` behind (§8.2).
- **Listener: loopback only in R1.** `127.0.0.1:7710` by default (`--live-listen`). `LiveService` exposes `Query`, `Mutate` and the admin `Deploy` with no authentication in R1 (D111), so **a non-loopback `--live-listen` is refused at startup** (`operon: --live-listen <addr> is not a loopback address; the Live API has no authentication until the unified auth plan (D111)`) rather than only warned about, which is stricter than D111's default for the other listeners. A loopback bind still logs one startup line saying the Live API is unauthenticated. Remote access in R1 goes through an SSH tunnel or a reverse proxy the operator secures. The refusal is lifted when the unified auth plan covers the Live API (R3). The dev playground's TiDB (root without a password) binds 127.0.0.1, as `tiup playground` does by default.

### 7.2 Generated clients

| Platform | Generator (all Apache-2.0) | Hand-written layer |
|---|---|---|
| Web and Node | `protoc-gen-es` + `@connectrpc/connect-web` / `connect-node` | `@operon/live`: query set, transitions, resume, optimistic updates; React hooks in R2 |
| iOS | `connect-swift` | R3 |
| Android | `connect-kotlin` | R3 |
| Python | `connect-python` **(verify maturity)** | R2 |
| Go | `connect-go` | R2 |

The generated stubs are the contract; each platform's hand-written layer is small (session state machine, reconnect, optimistic updates) and shares one conformance fixture set. **As built:** R1 ships `@operon/live` only (Connect and gRPC-Web over `fetch`, no `connect-node`, T14-5). The shared fixture set waits for a second client in R2, seeded from `session.test.ts`'s Transition scripts (owner ruling T16-2).

## 8. Reactivity

### 8.1 Read sets

- A query's **read set** is the list of what it read: point keys (`db.get`) and index ranges (`db.query`), as encoded key ranges `[lo, hi)` in one index. A filter applied after the read does not narrow it: the whole range scanned is in the set. A range that stopped at its limit is recorded up to the last key returned, not to the range's end, so inserts past a full page do not invalidate it (Convex's pagination idea).
- Read sets are ranges, not document lists, so an insert that falls into a range is caught (no phantom misses).

### 8.2 Invalidation, fan-out and reruns

Per app, on each node with sessions for that app:

1. **Tick.** The tailer takes a TSO timestamp `T`, reads all shard heads at `T` (one `batch_get`), and scans the new entries of each shard whose head moved, at `T`. It wakes immediately after a local commit and otherwise polls with backoff from 20 ms to 200 ms. **As built:** `T` is a fresh TSO timestamp moved back by `tick_read_lag`, **200 ms by default** (owner rulings T12-1, T13-1; `--live-tick-read-lag-ms`). Without the lag, a tick waited on in-flight two-phase-commit locks at the shard heads: p50 0.28–0.81 s under 32 writers, against 9.5–14.7 ms at 200 ms (T11-3, T16-9). New entries are read by batch gets of the known keys, and a tick reads at most 64 MiB, so a backlog takes several ticks and a subscription is evaluated at `T` only after a complete batch (T10-12).
2. **Match.** For every write in the new entries, the removed and added index keys and the document key are looked up in the app's **read-set index**: an interval tree per (table, index) and a hash set of point keys, mapping to subscription ids. A lookup is O(log n + matches). **As built (T11-4):** one augmented AVL tree for the whole app, because a read of a table that does not exist yet spans every future table's index range (T10-8).
3. **Rerun.** Each invalidated subscription is rerun at `T`, at most `rerun_concurrency` (16) at once per app. A rerun produces a new result and a new read set, which replaces the old one in the index. Identical subscriptions (same function, arguments and identity) share one cache entry, so a thousand clients watching one chat room cost one rerun.
4. **Push.** Every session gets one Transition to `T` with the queries whose result hash changed. Sessions with no changed query get nothing (except the ts-only Transition of §7.1).
5. **Advance.** All subscriptions of the app are now valid at `T`: the untouched ones because nothing in their read set changed between their last timestamp and `T`.

**Backpressure:**

- **Reruns.** If invalidations arrive faster than reruns finish, the next tick simply reruns at a newer timestamp. Intermediate results are skipped, never reordered: a client may see fewer versions, never an inconsistent one. A subscription is rerun at most once per `min_rerun_interval` (50 ms). **As built (T11-6):** the interval applies per tick for the whole app, so every subscription stays valid at the app's tick. A query that fails holds its error as its result and is rerun on every tick until it succeeds (T11-8).
- **Sessions.** Each session has a bounded outbound queue (16 Transitions). When it is full, queued Transitions are merged into one from the client's acknowledged version to the newest. A session blocked for more than 30 s is closed, and the client resumes.
- **Quotas.** Subscriptions per session (1 000), sessions per app and reruns per second per app are limits; over a limit the server answers `RESOURCE_EXHAUSTED`, as D65's quotas do.
- **Safety net.** Every subscription is also rerun unconditionally every 5 minutes and compared; a difference is a bug, is logged with both read sets, and is counted in a metric that alerts. **As built (T11-10, T13-3):** the counter is `SubsStats::missed_invalidations`. It is exported as `live_missed_invalidation_total` once M1.7 Task 1 adds the `/metrics` listener.

### 8.3 Why no update is missed

Let a subscription's result `R` be computed at tick `t` with read set `S`, and let `T > t` be the next tick.

1. Every committed write to a key in `S`, or into a range in `S`, belongs to a transaction that also wrote a journal entry naming that key (the document key and its old and new index keys). This assumes that only Loam writes into a Live keyspace; the keyspace is never handed to another TiKV client, and the router enforces it.
2. A shard's entries visible at `T` but not at `t` are exactly those with `head(t) < seq ≤ head(T)`, because the head and the entry are written in one transaction and the head only grows.
3. So if no new entry touches `S`, no write between `t` and `T` changed what the query read, and `R` is also the result at `T`. Otherwise the subscription is rerun at `T`.

Every query in a session is therefore valid at the session's tick, and results never go back in time. R1 Task 16 checks exactly this: every pushed result equals a fresh snapshot evaluation at its timestamp. The read lag of §8.2 changes nothing in the argument, because `T` is still a TSO timestamp and ticks never move back. **As built (T12-3):** the manager publishes each tick before it moves its current tick. A session drains its updates after subscribing and keeps each query's newer result, so a Transition never pairs results of two ticks. The reactive checker passed per PR (60 s) and under the nemesis (180 s), with 0 missed invalidations (the exit report).

## 9. Multi-tenancy and keyspaces (D122)

### 9.1 What TiKV and PD provide

- **API v2 keyspaces.** Keys carry a mode byte (`r` raw, `x` transactional) and a 3-byte keyspace id (`tikv/components/api_version/src/api_v2.rs:16-20,53-55`; `keyspace.rs:8`), so a cluster holds up to 2^24 keyspaces. PD splits regions at each keyspace's bounds when it creates one (`pd/pkg/keyspace/keyspace.go:653`).
- **Management.** PD creates keyspaces and changes their config and state over `/pd/api/v2/keyspaces` (`pd/server/apiv2/handlers/keyspace.go:39-48`). States go ENABLED ⇄ DISABLED → ARCHIVED → TOMBSTONE (`pd/pkg/keyspace/util.go:57-60`). Keyspaces can also be pre-created at bootstrap (`[keyspace] pre-alloc`, `pd/server/config/config.go:880`).
- **Cluster setting.** Every TiKV runs `storage.api-version = 2`, which requires `storage.enable-ttl = true` (`tikv/src/storage/config.rs:204-206`). A store that already holds RawKV or TxnKV data cannot switch from v1 (`tikv/src/server/raft_server.rs:280-330`), so **a Loam cluster is created on API v2 from the start**. Nothing found enforces the same setting on every store, so deployment tooling does (`tikv/src/storage/mod.rs:480-538` rejects mismatched requests per store).
- **Sharing.** A transactional client and a keyspace-mode TiDB both write `x` + keyspace-id keys, so they share one cluster safely as long as each uses its own keyspace.

### 9.2 Size classes

A keyspace costs at least one region (three Raft replicas) and PD metadata, so millions of apps cannot each own one. The router places apps by size class, as §18 §5.3 does for namespaces:

| Class | Placement | When |
|---|---|---|
| **Shared** | One of a pool of shared keyspaces, under the app prefix `0xA0 ‖ app_id` (§4.3) | Default for new and small apps |
| **Dedicated** | Its own keyspace | Large apps, apps that need their own GC or resource controls, and every app with SQL enabled |

- The **directory** maps (org, app) → {keyspace, prefix, class, state}. It lives in the `ControlStore` (§9.5) and is pushed to Live nodes as a live query: the router's directory feed of §18 §5.2 becomes a Loam Live subscription.
- **Moving** an app from shared to dedicated copies its key range into the new keyspace under a write fence (the app's lease epoch), then flips the directory, as §18 §5.5 moves namespaces. Unlike a namespace move, this copies data, so it runs as a background job with a short write pause at the flip.
- **Resource isolation.** PD's resource manager has per-keyspace managers (`pd/pkg/mcs/resourcemanager/server/manager.go:98`) and TiDB has resource groups (`tidb/pkg/resourcegroup/`); whether TiKV-side request units can be attributed to a txn-API client's keyspace is Q36.
- How many keyspaces one cluster sustains before region count dominates is Q36; the size-class thresholds come from that measurement.

### 9.3 MVCC garbage collection

TiKV keeps old versions until a GC safe point passes them. TiDB advances it for its own keyspace; **nobody advances it for a txn-API keyspace unless Loam does**, and versions would pile up forever.

- PD keeps per-keyspace GC state (txn safe point, GC safe point, GC barriers) and a keyspace's `gc_management_type` is `keyspace_level` or `unified` (`pd/pkg/gc/gc_state_manager.go`; `pd/pkg/keyspace/keyspace.go:51-75`). A `unified` keyspace needs a TiDB **without** `keyspace-name` to run the GC worker (`tidb/pkg/store/gcworker/gc_worker.go:108-117,334`).
- `client-rust`'s `gc()` resolves locks and updates the **cluster-level** safe point only (`client-rust/src/transaction/client.rs:268`, `src/pd/cluster.rs:88`). The keyspace-scoped RPCs (`AdvanceTxnSafePoint`, `AdvanceGCSafePoint`, `SetGCBarrier`, `GetGCState` with a `KeyspaceScope`) are in its vendored `pdpb.proto` (lines 82–112, kvproto `b41e863`), but not exposed.
- TiKV's own GC worker reads one safe point (`tikv/components/pd_client/src/client.rs:864`) and has no keyspace logic in `tikv/src/server/gc_worker/`; how PD combines keyspace states for it is unverified.
- **The plan (superseded):** `operon-tikv` runs a GC loop per Live and metastore keyspace with `keyspace_level` management. It generates tonic stubs for the needed `pdpb` RPCs from the same kvproto revision (Apache-2.0), resolves locks below the target, and advances the keyspace's txn and GC safe points to `now − gc_life_time` (10 min), held back by GC barriers for open snapshots (the collections bridge, backups). If R1 Task 0 finds that TiKV ignores keyspace-level safe points, the fallback is one `unified` GC TiDB per cluster. We also offer `client-rust` a patch that exposes keyspace GC. This is Q32.

**As built (Q32 answered by R1 Task 0: no keyspace-level GC on v8.5.8; rows R5–R7, T3-1–T3-13).** The bullets above describe PD's master branch. On the pinned release, PD v8.5.8 answers `Unimplemented` for `AdvanceTxnSafePoint` and `GetGCState`. TiKV v8.5.8 reads only the cluster safe point: a keyspace safe point set with `UpdateGCSafePointV2` was ignored for 90 s. A keyspace-mode TiDB neither computes the safe point nor resolves locks (`gc_worker.go:386-389`). So:

- **One Loam GC loop per cluster** (`operon_tikv::GcLoop`), leased at `e/cluster/gc` under its handle's root, acts as the cluster's GC worker. Each round:
  1. takes `min(now − life_time, UpdateServiceGCSafePoint("gc_worker", …))`;
  2. resolves locks below that point in **every** keyspace PD lists (all states but `TOMBSTONE`, `DEFAULT` included), renewing its lease after each keyspace;
  3. calls `UpdateGCSafePoint`;
  4. sweeps expired commit tokens and fences in the roots it was given (the metastore's and Live's).
- **The stubs.** It calls only `GetMembers`, `GetGCSafePoint`, `UpdateGCSafePoint` and `UpdateServiceGCSafePoint`, from `pdpb.proto` and its imports vendored byte for byte from kvproto `release-8.5` `07aa8c6` (`NOTICE`).
- **GC barriers are PD service safe points** (`GcBarrier`, service id `loam/<purpose>/<id>`). PD refuses a barrier below the current minimum.
- **Reads below the window are refused.** TiKV serves a read below the safe point without an error: `None` after GC, or the old value until compaction. So `operon-tikv` refuses snapshots older than `now − (life_time − 1 min)`, unless a barrier set through the same handle covers them.
- **One stuck keyspace blocks GC for all.** A keyspace whose locks cannot be resolved stops cluster GC for every keyspace (owner ruling T3-12: accepted; R2 adds a `gc_blocked_seconds` gauge and an alert).
- **No other GC worker.** No TiDB without `keyspace-name` may run beside Loam, since it would run its own GC worker.
- **Who runs it.** `operon dev --meta tikv://…` runs the loop on the metastore's handle. Live joins its sweep when both use the same PD, and otherwise runs its own (T6-9, T12-10).
- **Later.** Per-keyspace GC returns when a pinned PD release ships the GC-state API. The upstream patch that exposes GC safe points in `client-rust` is part of tikv/client-rust#564 (open).

### 9.4 Namespace router (the owner's item d)

- **R1:** one app, one dedicated keyspace, named on the command line.
- **R2:** the directory, shared keyspaces with app prefixes, app create/drop/rename, moves between classes, per-app quotas, and session placement by rendezvous on the app id (M1.3's hashing), so an app's subscriptions concentrate on few nodes and each app is tailed by few nodes.

### 9.5 The control plane on Loam Live (D125)

§19's control-plane data (orgs, members, teams, projects, environments, agents, service accounts, API keys, role bindings, quotas, usage rollups, the directory, billing accounts) needs a store in Loam cloud. Two TiKV-backed choices:

| | **Loam Live (a system app `_control`)** | **TiDB SQL** |
|---|---|---|
| Console updates | Live queries: member lists, agent tokens, usage and quota meters update without polling | Polling |
| Directory push to gateways (§18 §5.2) | It is a live query | A poller or TiCDC |
| Client code | The same Rust `LiveTxn` API; one TiKV client stack | `sqlx` with the MySQL dialect; a second data-access style; D58's dialect questions (Q24) return |
| Ad-hoc reporting | Weak: no SQL; usage is pre-aggregated rollups (D103) and exported to Iceberg for analysis (M4) | Strong |
| Maturity risk | The control plane depends on the youngest component | TiDB is mature |
| Transactions | TiKV transactions with the same retry model | TiDB pessimistic transactions |

**Choice: Loam Live.** The console and the gateways are the two heaviest readers of control-plane data, and both want pushed changes. Dogfooding the reactive layer on Loam's own control plane is also the fastest way to harden it. Billing reports that need SQL read the usage rollups from the Iceberg export (or, until M4, from a SQL mirror). The `ControlStore` trait (D65) stays: OSS and single-node installs implement it on the metastore backend, and Loam cloud implements it on `_control`. The bootstrap app `_control` lives in a fixed keyspace (`loam_control`), so reading the directory never needs the directory. Lands in R2; the maturity risk is mitigated by R1's gates and by keeping the openraft `ControlStore` as a fallback.

## 10. TiDB SQL coexistence (D123)

### 10.1 What keyspace mode requires

- TiDB's `keyspace-name` (config key, or env `KEYSPACE_NAME`; `tidb/pkg/config/config.go:124,240`) switches its driver to API v2 with a keyspace codec (`tidb/pkg/store/driver/tikv_driver.go:190-202`).
- TiKV must run API v2 (§9.1), and the keyspace must exist in PD before TiDB starts, created with PD's `[keyspace] pre-alloc` or the HTTP API. **Verified on playground v8.5.8** (classic Community build): TiDB with `keyspace-name = "ks_tidb"` wrote keys prefixed `x 00 00 01`, served MySQL clients (CREATE, INSERT, UPDATE, BEGIN/COMMIT, SELECT), and ran beside a Rust client in keyspace `ks_rust`. A key written in `ks_rust` read back as absent from both `ks_tidb` and `DEFAULT` **(spike)**.
- ~~A keyspace-mode TiDB runs its own keyspace-level GC worker; `unified` keyspaces need one TiDB without a keyspace (§9.3).~~ **As built (Q32, row R6):** on v8.5.8 a keyspace-mode TiDB neither computes the safe point nor resolves locks; Loam's cluster GC loop does both for every keyspace, TiDB's included, and no TiDB without `keyspace-name` may run beside Loam (§9.3). Q33 was re-confirmed on the pinned release in R1 Task 0 (row R8). R1 Task 15 (a keyspace-mode TiDB in the dev playground, with its SQL smoke test and coexistence tests) is **parked** until the owner decides about TiDB; `playground.sh --with-tidb` and `deploy/tikv/tidb.toml` exist from Task 1.

### 10.2 One TiDB per tenant, not a shared TiDB

- **A TiDB process serves exactly one keyspace**: its store holds a single keyspace name (`tikv_driver.go:252,494`). `tidb/pkg/domain/crossks` is internal plumbing for the next-gen SYSTEM keyspace, not multi-tenant serving; `tidb/pkg/keyspace/doc.go:15-38` describes keyspaces as logical clusters for next-gen and serverless.
- **So each SQL-enabled tenant gets its own TiDB pool** (one or more stateless TiDB pods with `keyspace-name` set), in its own dedicated keyspace, on the shared TiKV cluster. A shared TiDB with resource groups gives quotas but **no data isolation** between tenants and is not used for tenant SQL.
- **Cost.** An idle TiDB server takes several hundred MB of memory **(estimate)**, so SQL is opt-in per app, and idle pools scale to zero later (R4) behind a MySQL-protocol proxy that routes by user or database name. PingCAP's TiProxy (Apache-2.0) is the candidate **(verify its keyspace routing)**.
- **Next-gen only? No, for the binaries.** tidb-operator v2's `TiDB.spec.keyspace` says "For classic tidb, keyspace name is not supported" (`tidb-operator/api/core/v1alpha1/tidb_types.go:204-208`), but classic TiDB v8.5.8 runs in keyspace mode on `tiup playground` (above). Q33 is therefore narrowed. The binaries work; what remains is whether tidb-operator v2 accepts `keyspace` for classic clusters or needs the setting through its free-form config, and whether PingCAP supports the mode for classic deployments. That remaining question is checked with the operator in R4.

### 10.3 What SQL can and cannot see

- A tenant's TiDB sees its SQL keyspace only. Live tables are not visible from SQL, and SQL tables are not visible from Live functions, in R1–R3.
- Later, two one-way bridges are possible without forks: **Live → SQL** (the journal tailer writes rows into TiDB tables over MySQL) and **SQL → collections** (TiCDC in keyspace mode, `ticdc/pkg/config/changefeed.go:280`, into a Kafka or storage sink that Loam's native stream API or Kafka gateway reads). Neither is scheduled; the second depends on Q34.

### 10.4 TiDB's object-storage, vector and full-text features (D131)

The owner asked whether Loam can use these instead of, or beside, its own engine. Findings, checked in the clones where possible:

| Feature | Open source and self-hostable? | Evidence | Use in Loam |
|---|---|---|---|
| **Next-gen kernel: object storage as the single source of truth** | **No.** TiDB's side is open (a `nextgen` build tag, a separate binary; components of different kernel types cannot be mixed), but the matching shared-storage TiKV engine is not public: `tikv/tikv`'s `cloud-engine` branch was last touched on 2022-09-26, and master has no such engine (`tikv/components/cloud/` holds only the AWS, Azure and GCP clients for external storage) | `tidb/pkg/config/kerneltype/doc.go:15-39`; the coordinator's check of the tikv branches | **Not used.** Self-hosted TiKV keeps its row store on local disks with Raft. Loam Live's durability to object storage comes from BR log backup (below) |
| **TiFlash**: columnar replicas, disaggregated compute and storage on S3, the `VECTOR` type with HNSW vector indexes | **Yes.** pingcap/tiflash is Apache-2.0 and active **(not cloned; verify the S3 mode and vector index on the pinned release)**. TiDB builds vector indexes as *columnar indexes* backfilled on TiFlash (`tidb/pkg/ddl/index.go:997,1028-1115`) | As listed | **Optional add-on for SQL tenants** (R4) who want analytical or vector queries inside SQL. It is C++, heavy in memory, and replicates TiDB tables only, so it cannot see Live tables |
| **Full-text search** (`MATCH … AGAINST`, `FTS_MATCH_WORD`) | **Not in the open-source build, in practice.** TiDB parses it (`tidb/pkg/expression/builtin_fts.go`, `tidb/pkg/planner/core/fts_resolve_index.go`), but execution needs "the TiFlash FTS path", and the coordinator's search of public TiFlash found no full-text implementation. In a plain boolean `WHERE` position TiDB rewrites the match to `ILIKE '%term%'` predicates: no relevance score, no stop words, no word boundaries ("cat" matches "concatenate"), no index; phrases, `*`, `> < ~` and grouping are refused at plan time. A scoring position (`SELECT` list, `ORDER BY`, comparisons) keeps the native builtin and then needs TiFlash to execute (`tidb/pkg/planner/core/fulltext_to_like.go:19-70`, `tidb/pkg/expression/fts_to_like.go`) | As listed | **Not used.** Full-text over SQL data should go through a collection instead (SQL → collections, §10.3, Q34) |
| **`tiup playground --mode tidb-x` and `--mode tidb-cse`** (next-gen, S3-backed TiDB, with `--cse.s3_endpoint` and similar flags) | **Unknown.** The modes exist in tiup playground 1.17.1 **(spike; not run)**. Where their TiKV binaries come from and under what license is unverified, and no public source for a next-gen TiKV was found | tiup 1.17.1 `--help` | **Evaluate (Q38)** before any use. If the binaries are closed or unlicensed for self-hosting, they are not usable (D11, D126) |
| **`tici*` tiup components** (v0.1.0-alpha-nightly; probably TiDB's full-text and columnar indexing service) | **No.** Binary-only, with no public repository, and alpha | tiup component list **(spike)** | **Not usable:** no source, no license to check, alpha |
| **BR and log backup (PITR) to object storage** | **Yes.** TiKV's `backup-stream` component streams change logs to external storage (S3, GCS, Azure; `tikv/components/backup-stream`, `external_storage`, `cloud/{aws,azure,gcp}`); BR takes full snapshot backups and restores to a point in time | As listed | **Mandatory for every Live cluster** (below) |

**Position:**

1. **Loam's own engine is the vector and full-text engine for Live data**, through the collections bridge (§12). It is S3-native, gives hybrid retrieval with relevance scores, the hot tier and the Qdrant and Elasticsearch surfaces, and is the differentiator. TiDB's full-text is not a substitute, and TiFlash's vector index sees only SQL tables.
2. **TiFlash is an optional add-on for SQL tenants** (R4), run unmodified, for columnar or vector queries inside SQL. Loam does not depend on it.
3. **BR log backup (PITR) to object storage is mandatory for every Live cluster**, into the tenant's bucket (the bucket its namespace already uses) or the operator's bucket in multi-tenant keyspaces. A continuous log backup task plus periodic full snapshots gives a bounded recovery point (log backup's flush interval, minutes by default **(verify)**) and restore to any time in the retention window. A Live cluster does not serve external traffic until its backup task is running and a restore has been tested. This softens D130: TiKV stays Live's primary store, but a restorable copy of every Live keyspace is always in object storage. Whether BR's log backup and PITR restore cover a **txn-API keyspace** (not just TiDB tables), and per keyspace, is Q37; if they do not, the fallback is a journal-based export of each keyspace to the bucket, since the commit journal (§5.3) already records every write.

## 11. The TiKV metastore (D124)

### 11.1 Why now, and what changes

§18 §2.4 put TiDB over `sqlx` in M6 and rejected `tikv-client` because it "needs a real PD and TiKV cluster and would rebuild in KV what TiDB's SQL layer already provides". Both reasons change with Loam Live: the cluster exists anyway, and `operon-tikv` is shared with Live, so the KV layer is built once. **`operon-meta-tikv` replaces `operon-meta-tidb`** as the scale-out backend, and moves to R1. Postgres and DynamoDB stay v1.0 backends (D58). openraft + redb stays the default for `operon dev` and single node (D10).

### 11.2 Keys

One keyspace per Loam cluster (`loam_meta`, or `loam_meta_<cluster_id>` for each BYOC-managed cluster in the hosted control plane):

| Record | Key | Value |
|---|---|---|
| Namespace by name | `n/<name>` (as built, row R17: the trait's `create_namespace` takes no org; the org segment arrives with the router in R2) | `id` |
| Namespace | `N/<id:u64 BE>` | record, state |
| Id block | `c/<kind>` | next unallocated id; each node takes blocks of 1 000 (gaps allowed, D18) |
| Stream, link, collection by name | `s/`, `l/`, `k/` + `<ns>/<name>` | `id` |
| Stream, link, collection record | `S/`, `L/`, `K/` + `<id>` | record, `ns`, `state` |
| Aliases | `a/<ns>` | alias map, version |
| Hot configuration (as built, R17) | `H/<collection id>` | the collection's hot configuration, deleted with the collection |
| Partition head | `h/<stream>/<p>` | `next`, `log_start`, `bytes` |
| Index entry | `i/<stream>/<p>/<base:u64 BE>` | kind, records, object, byte range, max ts |
| WAL commit record | `w/<hash8(object)>/<object>/<group>` | base offsets, `created_at` |
| WAL live-chunk count | `W/<hash8(object)>/<object>` | `live` |
| Retired object | `r/<shard 0..63>/<path>` | `retired_at` |
| Object reference | `o/<hash8(path)>/<path>` | `refs`, `gc_claim` (as built, T5-5: read and locked, never written in R1) |
| Lease | `e/<scope>/<key>` (as built: `e/m/<key>`; the GC loop's `e/cluster/gc` is separate) | `owner`, `epoch`, `deadline_ms` |
| Pointer | `p/<ns>/<key>` | `version`, `value` |
| Commit token | `t/<token>` | `expires_ms` (or a fence, `expires_ms ‖ "F"`, T2-5) |
| ~~Change counters~~ | ~~`v/<scope>`~~ | Not built (T5-4, owner ruling T5-14; §11.3) |

WAL object names are ULIDs, which are time-ordered, so their keys get an 8-byte hash prefix to spread them over regions.

**As built (T4-4, T5-10).** Ids are 8-byte big-endian and partitions 4-byte big-endian, with names and paths last. Records are a format byte and postcard. Counters and stamps are u64 big-endian, and `W/` is a u32. `K/` has no state field: the record exists while the collection is live. A WAL commit record `w/…/<group BE4>` holds `{groups, created_at_ms, offsets: [(call position, base)]}`. Partition heads are written lazily: an absent head of an existing partition is an empty one (T4-5).

### 11.3 Mapping the contract

| Contract item | On TiKV |
|---|---|
| **One transaction per call** (D47) | Every trait method is one TiKV transaction: optimistic for single-record writes (leases, `cas_pointer`, creates), **pessimistic** (`begin_pessimistic`, `get_for_update` on partition heads in key order) for `commit_wal`, `swap_segment` and `trim_partition`, so hot heads queue instead of aborting in a loop. **As built (T5-2, T5-3, T6-5):** every write commits with `two_pc` (`operon_meta_tikv::COMMIT_MODE`). A pessimistic write locks its rows in one `batch_get_for_update` and reads index entries from a fresh snapshot taken after the locks. It gets 64 attempts, because a waiter woken by a commit gets `PessimisticRetry` and the runner restarts it: the pinned client never retries at a newer `for_update_ts` |
| **`commit_wal` atomicity** (§18 §3.1) | **One transaction per partition group.** A group is at most 1 024 chunks and at most 4 MiB of metastore writes (index entries, heads, commit and reference rows), well under TiKV's per-request Raft entry limit (`raftstore.raft-entry-max-size`, 8 MiB by default) and the lock-holding time a pessimistic transaction should have **(verify the limits on the pinned release)**. A call that fits in one group, which is every flush the log writer produces today **(estimate)**, is **atomic across all its partitions and namespaces**, stronger than D59. A larger call is split into groups; **one stream's chunks are never split across groups** (D59's rule, §18 §3.1), and a single stream's chunks that alone exceed one group are refused with `InvalidArgument` so the writer splits the WAL object. Each group commits in its own transaction with its own commit record `w/<object>/<group>`, so it is idempotent. The call returns success only after every group has committed. After a crash or error some groups may be committed and others not; that WAL object is unacknowledged, and the writer's retry recommits only the missing groups (their records show which), or the stale-commit rule (D27) settles them. Visibility is atomic per group, as D59 requires |
| **Compare-and-swap** | Read the pointer at the start timestamp, compare, write. A concurrent writer is a write-write conflict at prewrite, so no update is lost; the loser re-reads and returns `VersionMismatch` when the version moved |
| **Fences and GC claims** (§18 §3.2) | Checked in the same transaction: the lease record's epoch, the collection's `live` state, and `gc_claim` on every `o/` row the command makes reachable. A claim and a new reference write the same `o/` row, so they conflict and serialize, with no clock. **As built (T4-6, T4-7, T5-5):** a read that decides a write but is not overwritten by it is locked with `lock_keys` (the fence's lease, the collection record, the `o/` row), so write skew cannot pass a check. `o/` rows are read and locked, not written: no trait method claims an object in R1, and `W/` and the index already count references. The lock is what will make R2's claim protocol conflict with a new reference |
| **Clock and stamps** (§18 §3.2, D113) | `clock_ms` is the TSO's physical part. The TSO is one cluster-wide monotonic clock with no hot row, so the TiKV backend keeps the strong "one monotonic clock" behaviour, and bounded skew holds trivially. Lease deadlines are judged against the transaction's start timestamp. **As built (R2, T4-11, T5-7, T4-17):** the trait's `now_ms` is synchronous, so it is `max(previous, physical(latest TSO) + elapsed)`. A stamped write first waits, for at most 1 s, until a fresh TSO reaches `now_ms`, because PD advances the physical part in 50 ms steps. A TSO clock moves with time, not only with writes, so the suite's two clock-equality cases were relaxed to "not earlier" for every backend (owner ruling) |
| **Leases** | A read-modify-write of `e/…` comparing owner, epoch and deadline |
| **Consistency tokens** (D76) | Offsets are assigned by `commit_wal` from the partition head inside the transaction, dense per partition. `Linearizable` reads use a fresh TSO timestamp, and any transaction acknowledged before that TSO fetch has a smaller commit timestamp, so a token's offsets are always visible. `Local` is served as `Linearizable` in R1; TiKV stale reads can serve it later. As built, every read is one snapshot at a fresh TSO, retried up to 4 times when it meets a lock (T4-9) |
| **Composite reads** (§18 §3.3) | Real snapshots: every read of one call uses one start timestamp. No read order is needed |
| **Unknown outcomes** | Each write transaction also writes `t/<token>`. After an error during commit, the backend reads the token at a fresh timestamp. TiKV's lock resolution either finds the transaction committed or rolls it back, so the answer is always known. Conflict, `KeyIsLocked` after backoff, region errors and TSO unavailability before prewrite are definitely-not-applied. **As built (T2-5, T2-15, T4-3, T4-16):** a resolver that finds the token absent **writes a fence** at it and commits the fence before it reports "not applied", so a late prewrite of the lost commit meets a newer write and conflicts. A call whose attempt had an unknown outcome is rerun (at most 3 times) and reports `Tracked.earlier_unknown`, which keeps the trait's retry answers alike on every backend. Only `UndeterminedError` and a commit future dropped at the deadline are undetermined (row R9) |
| **`watch_changes`** | No native primitive. ~~Writers bump a change counter per scope (`v/catalog`, `v/ns/<id>`), and watchers poll counters every 100 ms and wake on the handle's own writes. `commit_wal` does not bump the catalog counter (D63's scoped feed)~~ **As built (T5-4, owner ruling T5-14): no `v/` counters.** The watch wakes at once on the handle's own writes, `commit_wal` included, and unconditionally every 100 ms. The trait has one unscoped watch whose consumers need other nodes' commits and pointer writes, so the poll must wake unconditionally anyway. Counters would then be unread writes, and on every commit a hot key. Scoped counters belong to D63's scoped change feed (M2.x) |
| **Pagination** (D99) | Range scans with `(prefix, after, limit)` |
| **Snapshots and restore** (D17, §10 §6) | Not applicable: TiKV replicates by Raft. Backups use BR full backups plus log backup (PITR) to object storage, mandatory as for Live (§10.4, D131; Q37) |
| **`drop_collection`** (as built, T4-8, T5-3, T5-15) | One transaction that deletes every partition's head (present or not), every index entry and the implicit link's pointer, releasing entries as the openraft state machine does. A collection with a large index is one large transaction; batching it is an R2 item |

### 11.4 Gaps and risks

1. **MVCC GC** for the metastore keyspace needs Loam's GC loop (§9.3, Q32). As built, the loop is cluster-wide and runs beside the metastore in `operon dev --meta tikv://…` (T6-9).
2. **`tikv-client` maturity.** Its README says 0.4.0 is "not suitable for production use - APIs are not yet stable" (`client-rust/README.md`).
   - **Dependency versions.** The crates.io 0.4.0 release pulls in prost 0.12 and tonic 0.10; the git master pulls in prost 0.13 and tonic 0.12 (`client-rust/Cargo.toml:39,49`). The workspace uses 0.14, so the tree carries two versions of each (build time and binary size).
   - **Client gotchas found in the spike:**
     - **TSO stream.** A PD stall (a 3 s etcd read) killed the client's TSO stream permanently (`TimestampRequest channel is closed`). Every later `begin` fails until the client is rebuilt, so `operon-tikv` wraps the client in a supervisor that rebuilds it on that error and on repeated TSO failures.
     - **Pessimistic lock conflicts** surface as `PessimisticLockError{WriteConflict{reason: PessimisticRetry}}`. The client does not retry at a new `for_update_ts` as TiDB and client-go do; the runner restarts the whole transaction instead.
     - **Async commit** `unwrap()`s `min_commit_ts` and does not set `max_commit_ts` (a FIXME in `transaction.rs`).
     - **Keyspace required on API v2.** A client without a keyspace fails with `InvalidKeyMode` on an API v2 cluster, so every client must be configured with one.
     - **Error messages** include keyspace-prefixed raw keys, which must be scrubbed before they reach users.
   - **The pin, as built (rows R4, X1).** crates.io 0.4.0 fails `cargo deny`'s advisories through its `tonic` 0.10, and misreports unknown outcomes of async-commit and 1PC prewrites. So Loam pins git `tikv/client-rust` `ab4be1c` with `default-features = false`, and `deny.toml` allows that git source. Crates depending on it are `publish = false` until a release carries the fixes; publishing waits for the Loam rename (owner ruling T8-1). The client's gRPC decoding limit is raised to 16 MiB, and reads also page from 256 keys, halving on `OutOfRange` (T2-12).
   - **Upstream first.** Loam pins a version, runs its own conformance and fault suites against it, and contributes fixes upstream (D126). The **first upstream PR candidates** are (1) reconnecting the TSO stream after a PD stall and (2) exposing the generated proto modules (`cdcpb`, `pdpb`, `keyspacepb`) as a public module. Two follow-ups come after: setting `max_commit_ts` in async commit, and optional pessimistic lock retry at a new `for_update_ts`. A third is **resolving async-commit and 1PC locks on the read path** (`CheckSecondaryLocks` from the reader's lock resolver; today only GC's `cleanup_locks` checks secondaries, so a crashed async-commit writer blocks readers until GC). Until it lands, the metastore and Live commit with `two_pc` (R1 plan rows T6-5, T7-1). **As built, four PRs are open upstream** (checked 2026-09-28): tikv/client-rust#563 (reopen the TSO stream), #564 (public kvproto modules), #565 (resolve async-commit locks on the read path) and #566 (bound the async-commit commit ts). dina-kar/operon#80 pins a `loam` fork that carries all four; it merges into `main` after #76 and reaches the R1 stack then (owner ruling T17-1).
3. **Latency.** Each call costs a TSO fetch plus prewrite and commit round trips. As built, `commit_wal` (one chunk, `two_pc`) measured p50 57–75 ms and p99 157–296 ms on a loaded host (the exit report); a quiet-machine run is an R2 item. In the spike, commit p50 was about 3.5–7 ms under heavy host load (1PC or async commit at the low end, 2PC at the high end). A 10-key pessimistic transaction took about 13–25 ms in total, dominated by ten sequential `get_for_update` round trips of about 1 ms each **(spike; indicative only)**. Batching locks (`batch_get_for_update`) matters for `commit_wal`. `commit_wal` sits on the write path; M2's write-latency budget must include it for TiKV deployments.
4. **Commit mode.** Async commit with 1PC was planned as the default here too (§5.1). As built, the metastore and Live commit with `two_pc` until the read-path lock resolution of item 2 is in the pinned `tikv-client` (R1 plan rows T6-5, T7-1); the `commit_mode` switch moves a component back to async commit then, by a ruling, once its linearizability histories and checkers pass with it.
5. **Hot keys.** A busy partition head is written by every flush that touches it. Pessimistic locking bounds the damage; TiKV splits regions by load but cannot split one key.

### 11.5 Where it lands

`operon-meta-tikv` passes the existing ~~49-case~~ **53-case** conformance suite with its linearizability histories (row R3: M1.3 added the hot cases and `leases_with_prefix_lists_only_that_prefix`), then its own fault matrix (§18 §4.2 columns, with TiKV rows: `BeforeSend` = TSO or prewrite refused; `AfterApply` = commit applied, response dropped; `Undetermined` = primary commit timed out; `Conflict` = `WriteConflict`; `Throttle` = `ServerIsBusy`; `Race`; `Delay`). It implements the trait as built when R1 starts; M2's contract amendment (§18 §3.4) then costs it little, because TiKV already meets the stronger contract.

**As built (T5-9, T6-2–T6-4, T6-8, T7-2, T7-3).**
- **The suite.** All 53 cases pass on TiKV, with `two_pc`.
- **The fault matrix.** It has 12 groups × 6 faults × 2 attempts = 144 cells, blessed in `meta_fault_matrix.tikv.expected.md`. The faults are `Refuse`, `LoseAck`, `Undetermined`, `Conflict`, `Delay` and `Race`. The groups include a two-group `commit_wal` failing in either group, and a drop of 64 partitions and 1 024 entries. It has a fifth outcome, `Rejected`, for a `Race` whose competitor legitimately won. `TikvMeta::check_invariants` checks the log's state after every cell.
- **The gates.** The kill -9 crash gate runs on TiKV nightly (`OPERON_GATE_META=tikv://…`). The object-store fault matrix and the simulation stay on openraft until M2.
- **The build feature.** `operon` selects the backend with `--meta tikv://<pd>/<keyspace>[?root=<hex>]` on `dev` and `standalone`, behind the off-by-default feature `tikv`, so the default build does not depend on the git pin.

## 12. The collections bridge (D129)

The owner's item (e), and the reason Loam Live is more than a Convex clone.

- **Declaring it.** A table in the deployed schema may say `searchable: { collection, fields, vectors, text }`. The bridge creates (or binds to) a Loam collection in the app's namespace with a matching schema.
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
| A TiDB pool dies | That tenant's SQL is down until the pod restarts; Live and other tenants are unaffected |
| Keyspace misconfiguration (a v1 store in a v2 cluster) | TiKV rejects requests with `ApiVersionNotMatched` (`tikv/src/storage/mod.rs:480-538`); the deployment check refuses to start |

## 14. Testing

1. **Conformance.** `operon-meta-conformance` runs against `operon-meta-tikv` (all 53 cases as built, linearizability histories). A new `operon-live` conformance suite covers the data model, codec order, index maintenance, journal density and the sync protocol's version rules.
2. **Fault matrix.** The in-process `FaultPlan` hook in `operon-tikv`'s transaction runner (`BeforeBegin`, `BeforePrewrite`, `BeforeCommit`, `AfterCommit`) drives a metastore fault matrix with a blessed `meta_fault_matrix.tikv.expected.md`, as §18 §4.2 does for the other backends, and a Live mutation fault matrix (every cell ends `Retried`, `SurfacedUnknown` or `NoEffect`; no acknowledged mutation lost; no idempotent mutation applied twice). **As built:** Live has no blessed matrix of its own. Its fault coverage is `idempotent_mutate_applies_once_across_lost_ack` (Task 10), plus the random `FaultPlan`s both checkers inject into the server's handle (T16-5, T16-8).
3. **Reactive correctness checker** (the R1 gate). A seeded workload of mutations and subscriptions over several sessions. For every Transition, the checker evaluates each updated query with a fresh snapshot read at the Transition's timestamp and requires equality; it also requires that versions strictly increase per session, that every committed mutation that touches a subscribed range is reflected by the first tick at or after its commit timestamp, and that a resumed session converges.
4. **Transaction checker.** A list-append workload over Live documents, checked for snapshot isolation (and, once Q31 lands, serializability) with an Elle-style cycle search implemented in `operon-sim`'s checker module; point-read promotion is checked by a write-skew workload that must show no anomaly on `db.get` reads.
5. **Jepsen-style nemesis** (nightly). On `tiup playground`: kill and restart TiKV stores and the PD leader, partition a Live node from TiKV with toxiproxy, pause processes; the workloads of items 3 and 4 run throughout and their checkers must pass.

**As built (T16-4–T16-10, owner ruling T17-2):**
- **The reactive checker** (`crates/operon-live/tests/reactive_checker.rs`) drives `LiveServer` through the generated connect-rust client. It uses HTTP/1.1 writers and HTTP/2 sessions, 4 sessions and 4 writers, with random faults in the server's handle. After **every** Transition, every held result must equal a fresh `Runner::query` at its `ts` on a second, fault-free handle, and the results must be exactly the version's query set. Sessions resume and modify their sets at random, and a resume must start at the last version. Run times: 60 s per PR and 30 min under the nemesis (`OPERON_CHECKER_*`). `checker_catches_injected_stale_result` shows it fails within 1.4 s once the server drops journal batches.
- **The transaction checker** (`txn_checker.rs`) uses **`operon_live::testing::elle`**, not `operon-sim`, which would pull the M0 simulation graph. It checks G0, G1a–c, lost update, G-single and G2, plus lost appends, duplicates and incompatible orders. **No anomaly is allowed, G2 included**, because the workload reads by id and point reads lock. `point_read_write_skew_never_happens` and `checker_catches_injected_lost_update` go with it.
- **The nemesis** (`scripts/tikv/nemesis.sh`) injects one fault every `--interval` seconds, round robin:
  - `tikv-kill` and `pd-kill`: SIGKILL, then restart from the same command line and data;
  - `pd-stall`: SIGSTOP of the PD leader for 15 s, past the client's 5 s timeout, which is the real TSO stream death (T2-17);
  - `live-pause`: SIGSTOP of the test binary, which serves Live.

  Toxiproxy partitions were not built. With `pd-stall`, the reactive checker fails unless the Live server's TSO supervisor rebuilt its client (it did, once per run). The nightly CI job `tikv-nemesis` runs **one TiKV store on the standard GitHub runner** (owner ruling T17-2). A 3-store nemesis on a larger runner is an R2 item.
6. **Where it runs.**
   - `tiup playground v8.5.8` with `--kv.config` (API v2 and TTL), `--pd.config` (pre-allocated keyspaces) and `--db.config` (`keyspace-name`), verified in the spike. It is installed in CI by the tiup installer script, and the first run downloads about 500 MB. Every script uses `--tag` and `--port-offset`, because other playgrounds on the machine take the default ports.
   - Per PR: jobs touching `operon-tikv`, `operon-meta-tikv` or `operon-live*` start one playground (1 PD, 1 TiKV, 1 TiDB when SQL tests run) and run the suites. Tests skip unless `OPERON_TEST_PD` is set.
   - Nightly: 3 TiKV stores, the nemesis, the M1.1 gates over the TiKV metastore. **As built:** the jobs are `tikv` (per PR, path-filtered inside the job), `tikv-nightly` (the crash gate on the TiKV metastore, nightly-only tests such as physical version removal, and the checkers at 60 s), `tikv-nemesis` (one store, above), `sdk-live-typescript` (the TypeScript client, with `live.test.ts` against a spawned `operon dev --features live`) and `live-protos` (`buf lint` and a regeneration diff). The playground uses `--port-offset 17000` everywhere (PD `127.0.0.1:19379`).
   - **Sizing.** 1 PD + 1 TiKV + 1 TiDB peaked at about **3.2 GB RSS**: TiKV 2.62 GB, TiDB 428 MB, PD 114 MB, and the playground wrapper spiked to 1 GB at startup **(spike)**. TiKV sizes its memory from host RAM, and capping `storage.block-cache.capacity` at 1 GB still peaked at 2.56 GB. The dev and CI configs therefore also set `memory-usage-limit` explicitly, and CI runners need at least 8 GB. Locally the playground runs only when no cargo build is running (the build machine's limit); under memory pressure the kernel swapped out about 1.9 GB of TiKV.

## 15. Licensing

| Component | License | Use |
|---|---|---|
| TiKV | Apache-2.0 | Run unmodified |
| PD | Apache-2.0 | Run unmodified |
| TiDB | Apache-2.0 | Run unmodified (SQL, and optionally a unified GC worker) |
| TiCDC | Apache-2.0 | Later, for TiDB-table capture (Q34) |
| tidb-operator | Apache-2.0 | Kubernetes deployment (R4) |
| tiup | Apache-2.0 | Dev and CI clusters |
| TiProxy | Apache-2.0 | Candidate MySQL proxy (R4) |
| TiFlash | Apache-2.0 | Optional columnar and vector add-on for SQL tenants (R4, D131) |
| BR (in the TiDB repo) and TiKV `backup-stream` | Apache-2.0 | Mandatory backup and PITR of Live and metastore keyspaces to object storage (D131) |
| kvproto (`pdpb`, `cdcpb`, `keyspacepb`) | Apache-2.0 | Vendored protos for the GC-state client. As built: `pdpb.proto` and its 13 imports from `release-8.5` `07aa8c6`; two of the imports that kvproto ships are not Apache-2.0: `gogoproto/gogo.proto` (BSD-3-Clause) and `rustproto.proto` (MIT), both listed in `NOTICE` (T3-2) |
| `tikv-client` (client-rust) 0.4.0 | Apache-2.0 | Dependency, as built at the git pin `ab4be1c` (row R4) |
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

Every dependency is compatible with D11. Running PD, TiKV and TiDB unmodified as separate processes creates no obligation beyond Apache-2.0 notices in distributed images.

## 16. Open questions

| # | Question | Needed by |
|---|---|---|
| Q31 | Serializable range reads in mutations: guard keys per equality-prefix bucket, or validation against the journal after prewrite (§5.2) | R2 plan |
| Q32 | ~~MVCC GC for txn-API keyspaces: does TiKV honour keyspace-level safe points set through PD's GC-state API, or must a `unified` GC TiDB run per cluster; and will `client-rust` accept a patch exposing keyspace GC (§9.3)~~ **Answered by R1 Task 0: no.** v8.5.8 has one cluster safe point, and Loam's GC loop is the cluster's GC worker (§9.3 as built). Still open: tikv/client-rust#564, which would expose the GC RPCs | Answered; upstream PR open |
| Q33 | ~~Do released classic TiDB binaries (v8.5.x) support `keyspace-name`?~~ **Verified on playground v8.5.8**: yes, with the keyspace pre-allocated in PD and TiKV on API v2 (§10.1). Narrowed to whether tidb-operator v2 accepts `keyspace` for classic clusters (§10.2). Re-confirmed on the pinned release in R1 Task 0 (row R8); R1 Task 15, which would run it beside Live, is parked | R4 plan |
| Q34 | Can TiCDC capture a keyspace-mode TiDB's tables on a classic cluster into a Kafka or storage sink, for SQL → collections (§10.3) | R3 plan |
| Q35 | The long-term function engine: QuickJS only, or V8 (`deno_core`) for CPU-bound functions and npm compatibility, or wasmtime for Rust and Go functions (§6.3) | R2 plan |
| Q36 | Keyspaces per cluster before region overhead dominates, and whether TiKV request units can be attributed to a txn-API keyspace; these set the size-class thresholds and per-app quotas (§9.2) | R2 plan |
| Q37 | Do BR's log backup and PITR restore cover a txn-API keyspace (not only TiDB tables), per keyspace, on a classic cluster; what recovery point does the default flush interval give (§10.4, D131) | R2 plan |
| Q38 | What `tiup playground --mode tidb-x` / `tidb-cse` (next-gen, S3-backed TiDB) runs: where its TiKV binaries come from, under what license, and whether they can be self-hosted. If they are open, S3 could become the source of truth for Live keyspaces, removing the D130 tension (§10.4) | R2 plan |

## 17. Contradictions with earlier decisions, and how they are resolved

| Earlier | Conflict | Resolution |
|---|---|---|
| D1: object storage is the only source of truth; compute is stateless | TiKV keeps Live data on local disks with Raft replication | D130: D1 holds for the retrieval engine; Loam Live's source of truth is TiKV. The Live role itself stays stateless. Mandatory BR log backup puts a restorable copy of every Live keyspace in object storage (D131); TiDB's next-gen S3 kernel would remove the tension, but its TiKV engine is not open (§10.4) |
| D2: OLTP is out of scope | Loam Live is an OLTP database | D130: D2 holds for the retrieval engine; OLTP enters Loam as a separate product line on TiKV, not on the bucket |
| D58, §18 §2.4 and the avoid list of [11-buy-vs-build](11-buy-vs-build.md) (`tikv-client` rejected; TiDB over sqlx in M6) | D124 uses `tikv-client` in R1 | D124 supersedes D58's TiDB clause and the `tikv-client` entry on the avoid list |
| D42: a narrow protocol footprint | MySQL is a new protocol | Loam does not implement it; TiDB does, unmodified (D123) |
| M1.6 Ruling 4: the TypeScript SDK has zero runtime dependencies | Generated Connect clients depend on `@bufbuild/protobuf` and `@connectrpc/connect` | The M1.6 SDKs keep REST and zero dependencies; the proposed M1.6 amendment (D128) applies only to protobuf surfaces (native gRPC, streams), where generated clients replace hand-written ones |
| The owner's note "moved up from M6 (D72)" | D72 is the native stream API; TiDB's M6 placement is D58 | D124 cites D58 |

## 18. Roadmap (D127)

| Milestone | Scope | Exit gate |
|---|---|---|
| **R1** (**done except Task 15, parked**, 2026-09-28; §20) | `operon-tikv`; `operon-meta-tikv` passing conformance and its fault matrix; ~~keyspace~~ cluster GC loop; one Live app in one keyspace: documents, tables, indexes, built-in and QuickJS queries and mutations, the commit journal, reactive subscriptions, the sync API (`Watch`, `ModifyQuerySet`, `Query`, `Mutate`, `Deploy`) over connect-rust; the generated TypeScript client with a reactive layer; TiDB SQL in a separate keyspace in the dev playground | The reactive correctness checker and the transaction checker pass, including under the fault matrix; the metastore conformance suite passes on TiKV; a TypeScript client sees a live query update after a mutation; a MySQL client and a Live client work against one playground without seeing each other's data |
| **R2** | The namespace router: directory, shared keyspaces, app lifecycle, moves, quotas; the `ControlStore` on `_control` (D125); actions and scheduled functions; online index backfill; the Q31 decision; multi-node sessions; generated Python and Go clients; per-tenant TiDB pools; **BR log backup (PITR) to object storage for every Live and metastore keyspace, with a tested restore** (D131); a `gc_blocked_seconds` gauge per keyspace and an alert on repeated cluster-GC failures of one keyspace, which hold the cluster safe point back for all (R1 plan row T3-12); `drop_collection` of a collection with a large index in batches rather than one transaction (R1 plan row T5-15); from R1's exit: the tick-latency re-measurement at `tick_read_lag` 0 and the async-commit/1PC decision, once #80's `tikv-client` fork is in (owner ruling T17-1); a 3-store nemesis on a larger runner (T17-2); a quiet-machine commit-latency run; one shared QuickJS runtime per deployment (T14-1); the shared client conformance fixtures (T16-2) | 10 000 apps on one cluster; the nemesis suite green for 24 h; a console page served from live queries; a point-in-time restore of a Live keyspace from object storage passes the reactive checker's state comparison |
| **R3** | The collections bridge and `ctx.search` (D129); auth on the Live API and TiDB users through the unified auth plan (D111, Q30); Swift and Kotlin clients; React hooks | A searchable table stays in step under a crash loop, exactly once; search after a mutation with `after_ts` sees it |
| **R4** | Kubernetes: tidb-operator for PD, TiKV and TiDB, Loam's Helm chart for the Live role; TiDB pools that scale to zero behind a proxy; TiFlash as an optional SQL add-on (D131); backup operations in the operator; BYOC for Live; durable actions (Resonate) as an option | A cluster deployed from the chart passes the nightly suite; restore from backup passes |

R runs beside M1 and M2. The build machine builds one crate graph at a time (shared target directory, 6 jobs), so R and M tasks interleave rather than run in parallel; the playground runs only between builds. The first plan is [`docs/plans/2026-09-27-r1-reactive-core.md`](../plans/2026-09-27-r1-reactive-core.md).

**Shared proto tooling (D128).** The native stream API's gRPC surface (D72, M2) and Loam Live use one toolchain: buffa messages, connect-rust services, `buf` for client generation. The proposed M1.6 amendment: where an SDK covers a protobuf service (the native gRPC protos of D101 and the stream API), it wraps the generated client instead of hand-writing transport code; the REST SDKs of M1.6 are unchanged. It is recorded here and in D128, and M1.6 is not rewritten.

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
- Operon: §18 (the contract and backends), §19 on PR #39 (console and tenancy), `docs/plans/2026-09-25-m1.2a-metastore-trait.md`, `docs/plans/2026-09-24-m1.6-sdks-mcp.md`

## 20. R1 as built

R1 was built from 2026-09-27 to 2026-09-28, in the stacked PRs #54–#101 and the one for this section. **Every task is done except Task 15 (TiDB SQL beside Live), which is parked until the owner decides about TiDB.** The R1 plan's "Rulings made during execution" has one row per change (R1–R19, X1–X10, T1-1–T17-3). The [exit report](../plans/r1-exit-report.md) has the gate results and measurements. This section lists what differs from the design above; the sections it names carry the detail.

### 20.1 Answers

- **Q32** (keyspace-level GC): **no**, on PD and TiKV v8.5.8. Loam runs one cluster-wide GC loop that resolves locks in every keyspace, with barriers as PD service safe points (§9.3, rows R5–R7, X2). Per-keyspace GC returns when a pinned PD release ships the GC-state API. Still open upstream: tikv/client-rust#564.
- **Q33** (keyspace-mode TiDB): re-confirmed on v8.5.8 (row R8). Its remaining part, tidb-operator v2 for classic clusters, stays with R4. Task 15, which would run TiDB beside Live in the dev playground, is parked.
- **Q31** (serializable ranges): still R2's decision. R1 has the coarse opt-in `serializable_ranges`, off by default (§5.2, T11-2).

### 20.2 What changed

| Area | Design said | As built | Rows |
|---|---|---|---|
| `tikv-client` | 0.4.0 from crates.io | Git pin `ab4be1c`, `default-features = false`; crates on it are `publish = false` until the Loam rename | R4, X1, T8-1 |
| Commit mode | Async commit with 1PC by default | **Two-phase commit** for the metastore and Live; `async_1pc` is a switch that comes back by a ruling after the read-path lock fix | T6-5, T7-1 |
| MVCC GC | A loop per keyspace, keyspace-level safe points | One cluster loop; reads below the window refused | R5–R7, T3-* |
| Metastore keys | `n/<org>/<name>`, `v/` counters, `o/` rows written by `commit_wal` | `n/<name>`, `H/<collection>`, no `v/` counters, `o/` rows read and locked only | R17, T5-4, T5-5 |
| Metastore suite | 49 cases | 53 cases; clock cases relaxed to "not earlier" | R3, T4-17, T5-9 |
| Unknown outcomes | Read the token at a fresh timestamp | The resolver fences an absent token before "not applied" | T2-5, T2-15 |
| Mutation attempts | Up to 8 | 16 for Live, 8 for the metastore; backoff within half the deadline | T10-3, T11-1 |
| Journal shards | 16, fixed at creation | 64 by default, stored per app, changed only while empty; a rerun draws another shard | T10-1, T10-2, T11-1 |
| Idempotency record | `{commit_ts, result, expires_ms}` | No commit timestamp; bound to the function and the arguments' digest | T10-6, T11-13 |
| Ticks | At a fresh TSO | 200 ms behind a fresh TSO (`tick_read_lag`), because in-flight 2PC locks on shard heads stalled lag-0 ticks | T11-3, T12-1, T13-1, T16-9 |
| Read-set index | An interval tree per (table, index) | One tree per app | T11-4 |
| QuickJS runtime | One per (node, app, deployment), 64 MiB per runtime | One per pooled context, on its own thread; 64 MiB per call; `--live-js-contexts` | T13-5, T14-1 |
| Errors | Connect codes | Connect codes plus a `loam.live.v1.LiveError` error detail | T16-1 |
| Session routing | Forward `ModifyQuerySet` between nodes | One node; an unknown session is `NOT_FOUND` | T12-14 |
| Client fixtures | A shared conformance fixture set from R1 | Deferred to R2, when a second client exists | T16-2 |
| Elle checker | In `operon-sim` | `operon_live::testing::elle` | T16-4 |
| Nemesis | 3 stores, toxiproxy partitions | Kills, a PD stall and a pause, no partitions; nightly on one store on the standard runner | T16-10, T17-2 |
| TiDB | A keyspace-mode TiDB in the dev playground | Parked (Task 15); the playground's `--with-tidb` exists | T16-3 |
| `operon` build | Live and the TiKV metastore in the default build | Features `tikv` and `live`, both off by default (`live` implies `tikv`) | T7-3, T8-2 |

### 20.3 Gates

These are the results in the exit report, on the owner's loaded build machine with one TiKV store:
- **The reactive checker** passed at 60 s per PR and at 180 s under the nemesis, with 0 missed invalidations. The TSO supervisor rebuilt its client once after the PD stall.
- **The transaction checker** passed at 30 s and in three 180 s nemesis runs, with no anomaly.
- **The broken-build tests** fail, as they must.
- **The metastore conformance suite and the fault matrix** pass on TiKV (Tasks 5–6).
- **Measurements.** The rerun rate at 64 shards was 0.25–0.28 per mutation. Live's commit p50 was 38–49 ms with `two_pc` and 27–32 ms with `async_1pc`. Tick p50 at the 200 ms lag was 9.5–14.7 ms.

**Not yet shown:** the nightly `tikv-nemesis` job has not run in CI. The exit gate's "a MySQL client and a Live client against one playground" waits for Task 15.

### 20.4 Carried to R2 and later

- **After #80.** The `loam` fork of `tikv-client` (dina-kar/operon#80) merges into `main` after #76, and the R1 stack takes it then (owner ruling T17-1). Then R2 does three things: re-measure tick latency at lag 0; decide whether `tick_read_lag` can drop and whether `async_1pc` returns; and repeat the commit latencies on a quiet machine.
- **A 3-store nemesis** on a larger runner (T17-2).
- **In the R2 row of §18 and `docs/plans/README.md`:**
  - the `gc_blocked_seconds` gauge and alert (T3-12);
  - batched large drops (T5-15);
  - a shared QuickJS runtime (T14-1);
  - the sweep of orphaned bundles (T14-3);
  - the client fixtures (T16-2);
  - Q31's journal validation (T11-2).
- **Upstream.** protobuf-es drops a map key named `__proto__` in `fromBinary` and `toJson` (T16-12).
