# 18 — Metastore Backends, Tenancy and the Namespace Router

Status: **Approved (owner)** · 2026-09-26 (decisions D58–D71; §3.5 and §5 amended by D75 and D76). Three parts are **defaults the owner has not yet confirmed** and may override: the OpenFGA timing (D67), the erasure policy (D69) and ids under sharding (D70). They are marked *(default)* below.

Loam is the product name (D33). API keys carry the `loam_` prefix from the start. Crates keep their working names (`operon-*`) until the rename.

Markers: **(estimate)** is computed from code or specs, not measured. **(verify)** is not checked against a primary source; the plan that builds it resolves it.

---

## 1. Summary

| # | Decision | Milestone |
|---|---|---|
| D58 | Postgres **and** DynamoDB metastore backends in v1.0; TiDB (MySQL protocol, sqlx) in M6 | M2, M6 |
| D59 | The `MetaStore` contract is relaxed: `commit_wal` is atomic per partition group; one monotonic clock becomes bounded-skew stamps plus GC claims; composite reads document a safe read order | M2 (first task) |
| D60 | Backend CI: floci runs the full DynamoDB suite, Alternator 6.2.3 the single-item subset, a fault matrix per backend, a nightly AWS deployment job | M2 |
| D61 | RustFS 1.0 replaces MinIO as the default self-hosted object store and becomes a per-PR CI target | M2 |
| D62 | Lambda workers are a CI harness only, for now | M2 |
| D63 | A router for millions of namespaces: directory, `ShardedMetaStore`, metadata-only moves, size-class placement with bounded load, scoped change feed, pagination, dirty sets | M2, M2.x, M6 |
| D64 | BYOC in both modes, with names in clear | M2.x (v1.1) |
| D65 | Orgs, a `ControlStore`, API keys and quotas | M2; hosted in M2.x |
| D66 | An `Authorizer` trait; RBAC in M2; OpenFGA through `openfga-client` with a transactional outbox | M2 |
| D67 | *(default)* OpenFGA moves from M6 to M2.x and shares one store with Lakekeeper | M2.x |
| D68 | A GDPR erasure path | M2 |
| D69 | *(default)* Erasure rewrites tags onto purged copies; 30-day deadline; crypto-shredding in M2.x | M2, M2.x |
| D70 | *(default)* Ids stay unsharded; calls that take a bare id gain the namespace in M2 | M2 |
| D71 | FoundationDB is dropped from the roadmap | — |
| D75 | One router for every resource kind: sharding by namespace whatever the kind; placement keys `(ns, kind, id[, shard])`; consumer-group coordination on the owner of `(ns, group)` | M2, M3, M4, M5, M6 |
| D76 | Consistency tokens are a hard guarantee on every backend, during moves and under stale routing, but not a required client input | M2, M2.x, M6 |

v1.0 stays M1 + M2 (D46), but M2 grows: a second new backend (DynamoDB), tenancy, erasure and the catalog-scale fixes. That growth is risk 25 in §12. A new milestone, **M2.x "cloud and BYOC" (v1.1)**, follows v1.0.

## 2. Metastore backends (D58)

### 2.1 Backends and milestones

| Backend | Crate | Milestone | Use |
|---|---|---|---|
| openraft + redb | `operon-meta` | Default (M0) | `operon dev`, standalone, clusters of 3 or 5 `meta` nodes |
| **Postgres** | `operon-meta-postgres` | **M2 (v1.0)** | Deployments that run managed Postgres (RDS, Aurora, Cloud SQL, Azure Database) |
| **DynamoDB** | `operon-meta-dynamodb` | **M2 (v1.0)** | AWS-native and serverless deployments; the natural store for the hosted control plane (M2.x), as WarpStream uses it |
| **TiDB** | `operon-meta-tidb` | **M6** | Metadata that outgrows one Postgres primary; TiDB Cloud is managed |

Every backend implements the same `MetaStore` trait (D47) under the relaxed contract (§3), and passes one conformance suite, the linearizability checks and its own fault matrix (§4).

FoundationDB was considered and dropped: TiDB, DynamoDB and Postgres plus the sharded metastore cover its roles; it needs native `libfdb_c` on every host and has no managed offering (D71).

### 2.2 Postgres: Lakekeeper's patterns

Lakekeeper (Apache-2.0, `lakekeeper-storage-postgres` at `b771dbf`) runs the same shape of commit as Operon's manifests: write the object, then compare-and-swap the pointer. The Postgres backend follows it:

- **Isolation.** READ COMMITTED, the Postgres default. No SERIALIZABLE. Explicit row locks only where needed, taken in a fixed order: `SELECT … FOR UPDATE` on partition heads in key order for `commit_wal`, `FOR UPDATE` on a namespace row when it is dropped.
- **Compare-and-swap.** One conditional `UPDATE … WHERE version = $expected` per pointer. The code checks the returned row count: no row is a `VersionMismatch`, a missing entity is `NotFound`. Batched pointer updates use `UPDATE … FROM (VALUES …)` as Lakekeeper's multi-table commit does.
- **Unknown outcomes.** Every write transaction inserts a **commit-token row** (Lakekeeper's idempotency record) in the same transaction. After a connection is lost during COMMIT, the client checks the token row, or `pg_xact_status(xid)` for the `pg_current_xact_id()` it recorded before COMMIT. Either turns an unknown outcome into a known one.
- **Retries.** SQLSTATE 40001 and 40P01 are rolled back and retryable. Constraint violations map to the trait's typed errors. Lakekeeper's `dbutils.rs` SQLSTATE mapping is the model.
- **Pools.** Separate read and write pools. Reads that decide idempotency use the write pool, to avoid replica lag.
- **Queries.** sqlx with compile-time-checked `query!` macros; CI builds with `SQLX_OFFLINE=true` from the checked-in `.sqlx/` data. Schema changes are `sqlx::migrate` migrations.
- **Change feed.** `LISTEN/NOTIFY` wakes `watch_changes`. Whether NOTIFY serializes commits at high commit rates is part of Q17.
- **Clock.** No single clock row. Commands carry bounded-skew stamps (§3.2), so no row is locked by every write.
- **Singletons.** Maintenance that must run once per cluster takes `pg_advisory_lock`, as Lakekeeper's `advisory_lock.rs` does, where a metastore lease does not already cover it.

What is copied from Lakekeeper, with its NOTICE, is listed in §11 §2. Lakekeeper's `CatalogStore` trait is not copied: it threads one transaction through many calls, while Operon's trait is one call, one transaction (D47). That is what makes Operon's trait servable from DynamoDB and over RPC.

### 2.3 DynamoDB

**Facts that shape the design** (AWS developer guide, checked 2026-09-26):

- `TransactWriteItems`: at most 100 distinct items, 4 MB in total, 400 KB per item. Every item is charged twice. A cancelled transaction lists a reason per item. SDKs do not retry it.
- `ClientRequestToken`: a repeat within 10 minutes of the first request finishing returns success without applying anything. The Rust SDK sets the token once per operation call, so its own retries reuse it; **application-level retries must set it explicitly**.
- A transaction is serializable against single-item operations and `TransactGetItems` (100 items, 4 MB), but only read-committed against `Query`, `Scan` and `BatchGetItem`.
- Strongly consistent reads exist on the base table and local secondary indexes, never on global secondary indexes.
- One partition serves at most 1,000 WCU/s and 3,000 RCU/s. One hot item therefore sustains about 500 transactional writes/s **(estimate)**.
- After a 500 or a timeout, a write may or may not have applied.
- TTL deletion is best effort ("typically within a few days"). Leases compare timestamps in condition expressions and never rely on TTL.

**Item layout.** One table, string keys `pk` and `sk`. Every lookup that must be linearizable is on the base table.

| Item | pk | sk | Attributes |
|---|---|---|---|
| Namespace by name | `nsname#<org_id>#<name>` | `-` | `id` |
| Namespace | `ns#<id>` | `-` | `name`, `state` |
| Id counter | `ctr#<kind>` | `-` | `n` (`ADD`; gaps allowed, D18) |
| Stream, link or collection by name | `ns#<id>` | `sname#`, `lname#`, `cname#<name>` | `id` |
| Stream, link or collection record | `stream#`, `link#`, `coll#<id>` | `-` | record, `ns`, `state` (`live` or `dropping`) |
| A namespace's aliases | `ns#<id>` | `aliases` | alias map, `ver` |
| Partition head | `part#<stream>#<p>` | `head` | `next`, `log_start`, `bytes`, `ver` |
| Index entry | `part#<stream>#<p>` | `e#<base:020>` | kind, records, object, byte range, max ts |
| WAL commit record | `walc#<object>#<group>` | `-` | base offsets, `created_at`, `ttl` |
| WAL live-chunk count | `wall#<object>` | `-` | `live` |
| Retired object | `ret#<shard 0..63>` | `<path>` | `retired_at` |
| Object reference, for GC | `obj#<path>` | `-` | `refs` |
| GC claim | `gcclaim#<path>` | `-` | `claimed_at`, `owner` (§3.2) |
| Lease | `lease#<key>` | `-` | `owner`, `epoch`, `deadline` |
| Pointer | `ptr#<ns>#<key>` | `-` | `version`, `value` |

**How the trait maps.** The notes that matter:

- **Creates** take an id from the counter, then one transaction: Put the name item if `attribute_not_exists(pk)`, Put the record, ConditionCheck the parent namespace. A failed name condition returns the existing item (`ReturnValuesOnConditionCheckFailure=ALL_OLD`), so `NamespaceExists(id)` needs no second read, and a retry after a lost acknowledgement reports the first attempt's id, as the trait requires.
- **`create_collection`** is one transaction of 7 items (namespace check, collection name and record, the implicit stream and link with their names). **Partition heads are created lazily**: a missing head means `next = 0`, because 10,000 partitions do not fit in a transaction.
- **`drop_collection`** marks the collection, stream and link `dropping`, deletes the name and the pointer, and retires the prefixes, in one transaction. An idempotent **janitor** deletes heads, index entries and records afterwards. Readers treat `dropping` as absent.
- **`update_aliases`** keeps a namespace's aliases in one item with a version, so up to 100 actions stay one transaction.
- **`commit_wal`** commits per group of at most 32 chunks, one transaction per group, each with its own `walc#<object>#<group>` record (§3.1).
- **`swap_segment`** touches about 2n + 3 items, so **at most ~45 WAL entries per swap**. The backend exposes that limit and the segmenter caps its runs.
- **`trim_partition`** moves `log_start` forward first, then deletes entries below it in chunks. Each chunk is idempotent.
- **`partition_index`** reads the head first with a consistent read, then queries entries in `[log_start, head.next)`. A concurrent trim can delete entries at the low end while the query runs, so if the result does not start at `log_start` or has a gap, the backend re-reads the head and retries. Only a contiguous range from the head's `log_start` is returned (§3.3).
- **Leases** are one conditional `UpdateItem` each, comparing owner, epoch and deadline in the condition.
- **`cas_pointer`** without a fence on a non-collection key is one conditional `UpdateItem`. Otherwise it is a transaction: the update, a ConditionCheck of the lease epoch, a ConditionCheck that the collection is `live`.
- **GC reads** query the `ret#` shards and batch-get `obj#` items instead of scanning index entries. The `obj#` counts are kept in the same transactions that change index entries.
- **`watch_changes`** has no native primitive: it wakes on this handle's own writes and polls (100–500 ms). DynamoDB Streams do not keep a transaction's records together.
- **`Consistency::Local`** is served as `Linearizable` (`ConsistentRead=true`). Eventually consistent reads can go backwards between replicas, which the trait forbids.
- **Unknown outcomes.** A transaction is retried with the **same `ClientRequestToken`** within 10 minutes; if the first attempt applied, the retry returns success, and the call reports success. A single-item write compares the returned item against a unique writer nonce.

**Throughput.** `commit_wal` runs at about nodes × flushes per second, far below the per-item limits (D25). A hot partition written by N gateways sees optimistic conflicts that grow with N; retries use jitter.

### 2.4 TiDB (M6)

- **Protocol.** MySQL, through `sqlx` (feature `mysql`). Not `tikv-client` 0.4: it needs a real PD and TiKV cluster and would rebuild in KV what TiDB's SQL layer already provides.
- **Transactions.** Pessimistic mode (the default since v3.0.8). REPEATABLE READ is snapshot isolation. `SELECT … FOR UPDATE` has no gap locks, so uniqueness comes from unique indexes.
- **Dense offsets** come from a counter row per partition, locked in key order. `AUTO_INCREMENT` leaves gaps and is used only for ids.
- **Errors.** 9007, 8022, 1213, 1205 and 8002 are rolled back and retryable. "Execution result undetermined", and any connection loss during COMMIT, is an unknown outcome, resolved by the commit-token row. The errno the client actually receives is Q24.
- **CI.** `pingcap/tidb:v8.5.x` with unistore per PR; `tiup playground` (real PD and TiKV) nightly.
- **Sharing with Postgres.** The schema design, the conformance and fault harness, and the commit-token logic are shared. The SQL is not: the dialects differ (`RETURNING`, upserts, error codes; Q24), and the crates stay separate.
- **Change feed.** No LISTEN/NOTIFY; TiCDC is too heavy. It polls, as DynamoDB does.

## 3. The relaxed `MetaStore` contract (D59)

The openraft backend gives every command one total order and one monotonic clock. DynamoDB cannot give either cheaply, and a sharded metastore (§5) cannot give them at all. D59 relaxes the contract in three places. **Callers may rely only on the relaxed contract.** The openraft backend may keep its stronger behaviour, but no caller and no test may depend on it.

### 3.1 `commit_wal` is atomic per partition group

- A WAL object holds chunks for many partitions and namespaces (D25), with no cap on the number of chunks. One busy flush can exceed DynamoDB's 100 items, and under sharding its partitions live on different shards.
- **New contract:** `commit_wal` is atomic per **partition group**: a DynamoDB transaction group, or a metastore shard. Each group commits idempotently through its own commit record keyed by `(object, group)`. The call returns success only when every group has committed.
- A crash can leave an **unacknowledged** WAL object committed for some partitions and not others. The writer's retry, or the stale-commit rule (D27), settles the rest. Acknowledged writes are unaffected.
- Visibility becomes atomic per partition. That is what Kafka promises for one produce request, and what Neki ships with (no atomic cross-shard commits in its preview). Elasticsearch `_bulk` promises less: each operation succeeds or fails on its own.
- **One caller relies on cross-partition atomicity today:** M1.1's `LogWriter::append_many` puts every partition's batch of one collection write in one WAL object and one `CommitWal`, and its tests expect all-or-nothing. The contract therefore also says: **a backend keeps one stream's chunks of one WAL object in one group** while they fit (32 chunks on DynamoDB; always, under sharding, because a stream lives in one namespace and so one shard). For a collection with more partitions than fit in one DynamoDB group, the M2 contract task decides between documenting per-group atomicity for `append_many` and giving such a write a transaction of its own, up to DynamoDB's 100 items. It checks every other caller the same way.

### 3.2 Bounded-skew stamps and GC claims

- Today every command's stamp raises one metastore clock, and `StaleCommit`, freshness (`StaleObject` against the GC grace) and lease expiry are judged against it. On DynamoDB, a clock item written by every transaction would be one hot item (about 500 transactions/s, all conflicting).
- **New contract:**
  1. Commands carry the proposer's stamp, `max(local clock, last observed stamp)`. The existing `ClockSkew` bound (§10 §2, default 5 min) keeps proposers within `max_clock_skew` of each other. The contract promises bounded skew, not one monotonic clock.
  2. WAL commit records are pruned only after `2·window + max_clock_skew` (with D27's 15-minute window: 35 minutes), so the "never committed twice" rule holds under skew.
  3. **Explicit GC claims** replace clock-ordered GC safety for objects the metastore references. Before deleting an unreferenced object, GC writes a claim record for its path. **The claim is written atomically with a check that the object is still unreferenced**: on DynamoDB, one transaction that puts `gcclaim#<path>` if absent and ConditionChecks that `obj#<path>` has no `refs` or `refs = 0`; on the SQL backends, the claim is a column of the object's reference row, set by an upsert that succeeds only while the count is zero, and every reference addition upserts the same row only while no claim is set, so the row lock orders them (no gap locks are needed, which TiDB lacks). **Every command that makes an object reachable checks, in the same transaction, that no claim exists** for each path it adds: WAL objects in `commit_wal`, segments in `swap_segment`, the manifest path in `cas_pointer`. The command fails with a retryable error if one does. A claim and a new reference therefore serialize: whichever commits first makes the other fail. This is correct without any clock, and costs one transaction per deleted object.
  4. Objects reachable only through a manifest (splits, Lance files, PK deltas) stay protected by the freshness check and the GC grace period. The grace's margin over `max_commit_delay` grows by `max_clock_skew`.
- Lease expiry is judged against the proposer's stamp inside the condition, so a lease can be taken over up to `max_clock_skew` early or late. Lease holders already fence every effect with the epoch, so that changes latency, not safety.

### 3.3 Composite reads document a safe read order

- `collection_heads`, `collection_roots`, `links_with_pointers`, `stream_state` and `partition_index` read many items. On DynamoDB, `TransactGetItems` gives a snapshot only up to 100 items, and `Query` is read-committed.
- **New contract:** each composite read documents a **safe read order**. Parts that only grow (high watermarks, log starts, pointer versions) are read after the parts they must not lag. Example: read the pointer, then the partition heads. The tail can then only look longer than the manifest's applied offsets, never shorter, and a reader merges it correctly.
- Each method's order is written in the trait docs, justified, and checked by the linearizability checker.
- Only DynamoDB needs this. Postgres (a single statement, or REPEATABLE READ) and TiDB (snapshot isolation) give real snapshots, and may keep using them.
- GC's rule that "tags and retention are read from one metastore snapshot" (§03 §7) becomes a documented order: **retention first, then tags**. GC reads the pointer and the clock that decide retention, then the tags, and evaluates retention at that pointer and at that clock minus `max_clock_skew` (§3.2). A tag is created only on a manifest that is still retained, and a manifest's retention only lapses as the pointer and the clock advance, so a manifest tagged after GC's tag read was already retained at GC's retention read, and a tag created before the tag read is seen by it. Reading tags first is unsafe: a tag can land between the two reads on a manifest whose retention lapses before the retention read, and GC would collect a tagged manifest.

### 3.4 Where it lands

The contract change is **the first task of M2**, before any new backend:

1. Amend the trait docs: the three relaxations above, the namespace on bare-id calls (D70, §18 §10), paginated list methods and the scoped change feed (§5.4).
2. Add conformance cases for each relaxation and for consistency tokens (§3.5), and a `Backend::capabilities()` flag (transactions or single-item only, snapshot reads or read orders).
3. Teach the linearizability checker the relaxed models: per-group atomicity of `commit_wal`, and the documented read orders.
4. Move the openraft backend and every caller onto the new signatures. The M0 and M1 gates run unchanged.

None of this changes an on-disk format. The openraft command encoding keeps its bare ids; the namespace is a call argument (§18 §10).

### 3.5 Consistency tokens hold on every backend (D76)

A consistency token `{(stream, partition, offset)}` means the same thing on openraft, Postgres, DynamoDB and TiDB, under the relaxations above, during namespace moves and under stale routing. A read that presents a token sees every write the token covers, on any node:

- **Offsets come from the metastore.** Only the metastore assigns offsets, and a write is acknowledged, with its token, only after every partition group it touched has committed (§3.1). Every offset in a token is committed before the client holds it.
- **Per-partition order is kept.** Each partition's offsets are dense and ordered on every backend, so "visible up to offset *o*" has one meaning. Bounded-skew stamps (§3.2) order nothing a token relies on.
- **Any serving node merges the tail up to the token.** The node reads the object's manifest (its applied offsets), then the partition heads in the documented read order (§3.3), and merges the log tail from the applied offsets up to the token. If a head it reads is below the token (a lagging replica, a cache, a stale shard), it re-reads it linearizably from the owning shard, or waits until the request's deadline. It never answers without the token's offsets.
- **Moves and stale routing.** Ownership is a hint (§5.3), so a stale route reaches a node that can still serve. A move fences writes before it copies and flips the directory after the copy (§5.5), so both shards hold every offset acknowledged before the fence. Once the old shard's copy is removed, it answers `NamespaceMoved`, and the node refreshes its directory and retries.
- **Clients may omit tokens.** Default single-object reads are strong without one, and `eventual` skips the tail. Tokens are needed only for reads through derived objects: tables, graphs, or another collection fed by a link.

**Gate** (the conformance suite and the router): a write acknowledged with a token, then read with that token on every other node and on every backend, during a namespace move and with a stale directory, must be visible. It is staged with the router (§5.7): every node and backend, including a node routing from a stale membership view, in M2; a stale directory in M2.x; a namespace move in M6.

## 4. Testing the backends (D60, D61, D62)

### 4.1 Conformance targets

`operon-meta-conformance` (49 cases and its linearizability histories, M1.2a) runs against every backend:

| Target | License | Runs | When |
|---|---|---|---|
| openraft, single-node and 3-node | MIT/Apache-2.0 | Full suite | Every PR |
| Postgres (`postgres:17` container) | PostgreSQL | Full suite | Every PR |
| **floci** (`floci/floci:2.1.0`, pinned ≥ 2.1.0) | MIT | **Full DynamoDB suite** | PRs touching the DynamoDB backend or the trait; nightly |
| **ScyllaDB Alternator** (`scylladb/scylla:6.2.3`) | AGPL-3.0, CI service only | **Single-item subset**, selected by `Backend::capabilities()` | PRs touching the DynamoDB backend or the trait |
| DynamoDB Local (`amazon/dynamodb-local:3.3.1`) | Proprietary; pulled in CI, never redistributed | Full suite, optional second opinion | On demand |
| Real DynamoDB (small on-demand table) | AWS | Smoke test, throttling | Nightly |
| TiDB (`pingcap/tidb:v8.5.x`, unistore) | Apache-2.0 | Full suite | PRs touching the TiDB backend (M6) |
| TiDB `tiup playground` | Apache-2.0 | Full suite | Nightly (M6) |

- **floci** is its own DynamoDB implementation, not DynamoDB Local. `TransactWriteItems` enforces 100 items and 4 MB and reports a reason per item, and `ClientRequestToken` is honoured (10-minute window, `IdempotentParameterMismatch` on a changed body), which is checked in its code. It never produces `TransactionConflictException` or throttling. Whether its `TransactGetItems` is isolated from concurrent writes is Q22. It is seven months old and releases twice a month: the version is pinned, and each new release is checked against the real-AWS nightly before the pin moves.
- **Alternator** has no transactions. It runs with `--alternator-write-isolation always_use_lwt`, the only mode with a correct compare-and-swap. The single-item subset covers leases, `cas_pointer` without a fence on non-collection keys, name uniqueness through `attribute_not_exists` plus `ALL_OLD`, `ConditionalCheckFailed` handling, and LWT timeouts as real unknown outcomes. 6.2.3 is the last AGPL release; 2025.1 and later are under a source-available license and are not used. Running it unmodified as a CI service creates no AGPL obligation (it is neither linked nor distributed), consistent with D11. It is **not** a production metastore.
- Container services run **one at a time**, to fit the build machine's memory.

### 4.2 A fault matrix per backend

Modelled on the S3 fault matrix (`crates/operon/tests/fault_matrix.rs` and its blessed `fault_matrix.expected.md`):

- **Cells** cross trait-method groups (catalog creates, `commit_wal`, `swap_segment`, trim, `cas_pointer` with and without a fence, leases, `drop_collection`), faults, and the attempt the fault hits (first or second).
- **Faults:**

  | Fault | Meaning | Postgres, TiDB (sqlx) | DynamoDB, Alternator (aws-sdk) |
  |---|---|---|---|
  | `BeforeSend` | Definitely not applied | Pool timeout or refusal before BEGIN | Interceptor fails in `read_before_transmit` |
  | `AfterApply` | Applied, acknowledgement lost | COMMIT, then an IO error | Interceptor replaces the success with a timeout in `modify_before_attempt_completion` |
  | `Undetermined` | The backend reports the outcome unknown | Connection lost during COMMIT; TiDB "result undetermined" | HTTP 500, timeout |
  | `Conflict` | Rolled back, retry | 40001, 40P01; TiDB 9007, 8022, 1213 | `TransactionCanceled{TransactionConflict}`, `TransactionConflictException` |
  | `Throttle` | Not applied, back off | 53300, pool exhaustion | `ProvisionedThroughputExceeded`, `ThrottlingException` |
  | `Race` | A competing write between our read and our write | Hook between SELECT and UPDATE | Hook between the consistent read and the transaction |
  | `Delay(d)` | Latency or a hang up to the retry budget | Sleep in the wrapper | Sleep in the interceptor |

- **Outcomes:** each cell ends `Retried`, `SurfacedUnknown` (`earlier_unknown` set, or `Timeout` after the retry budget), `SurfacedRetryable` or `NoEffect`, and must match `meta_fault_matrix.<backend>.expected.md`.
- **Invariants after every cell:** the suite's state checks pass; no WAL object is committed twice; no acknowledged write is lost; pointer versions stay dense.
- **Injection is in process and deterministic:** one `run_tx(op, |tx| …)` helper with a `FaultPlan` hook (`BeforeBegin`, `BeforeCommit`, `AfterCommit`) for the SQL backends; one SDK interceptor for DynamoDB and Alternator. The conformance crate's `Faults` trait gains `plan(FaultPlan)`.
- **Toxiproxy** (`ghcr.io/shopify/toxiproxy` 2.12.0, MIT) runs nightly end-to-end soaks against each container backend (`timeout`, `reset_peer`, `latency`, `down`), driven through its REST API. It is not reliable enough for per-cell tests.

**How `earlier_unknown` maps.** The rule as built: an attempt refused before it was proposed never sets it; every other retryable failure does.

| Backend | Definitely not applied | Unknown | Turned into known by |
|---|---|---|---|
| Postgres | Errors before COMMIT is sent; 40001, 40P01; constraint violations | Connection loss or cancel during COMMIT | The commit-token row, or `pg_xact_status` |
| DynamoDB | `ConditionalCheckFailed`; any `TransactionCanceled`; `TransactionConflictException`; throttling; validation; `IdempotentParameterMismatch` | 500, timeout, connection reset | Retry with the same `ClientRequestToken` (transactions); writer nonce in `ALL_OLD` (single items) |
| Alternator | `ConditionalCheckFailed` | LWT write timeout | Writer nonce only |
| TiDB | 9007, 8022, 1213, 1205, 8002 | "Result undetermined"; connection loss during COMMIT | The commit-token row |

### 4.3 The nightly AWS deployment job (floci)

Runs nightly and on PRs labelled `aws`, with floci started through `docker run` (Lambda needs the Docker socket):

| Piece | How it runs | Emulated service |
|---|---|---|
| Object store | `s3://loam-ci` at floci's endpoint | floci S3 |
| Metastore | `operon-meta-dynamodb` | floci DynamoDB |
| Gateway, query and log roles | `operon cluster` containers (they need a hot tier and long-lived connections) | — |
| Workers | A Lambda function (`provided.al2023` Rust `bootstrap`, or a container image); each invocation runs `run_once` for a bounded time, coordinated by the existing leases and fencing | floci Lambda |
| Triggers | EventBridge Scheduler `rate(1 minute)` for maintenance; SQS with an event-source mapping for on-demand work (hot builds, erasure purges) | floci Scheduler, SQS |
| Credentials | The IAM roles and policies Loam ships in its infrastructure-as-code module, with `FLOCI_SERVICES_IAM_ENFORCEMENT_ENABLED=true`; KMS keys | floci IAM, STS, KMS |

It checks: deployment from the shipped module; data ingested through the Qdrant, ES and native APIs comes back from queries; Lambda workers advance manifests and GC deletes only unreachable objects; a worker killed by its function timeout loses its lease and the next invocation takes over at the next epoch with nothing applied twice; the shipped least-privilege policy passes and a policy missing an action fails loudly; an erasure request completes end to end through SQS (§9).

It cannot check DynamoDB conflicts and throttling (the in-process fault layer covers them), S3 409 and 503 (`FaultyStore` covers them), real latency, cold starts, multi-AZ behaviour, or IAM behaviour floci does not implement. The real-AWS nightly stays the final check.

**Lambda workers are a CI harness (D62).** Production workers stay on containers and Kubernetes. Lambda becomes a supported production deployment only after the harness has proved it out, by a later decision.

### 4.4 RustFS and the `Store` provider suite (D61)

**RustFS 1.0.0** (Apache-2.0, released 2026-09-16) replaces MinIO as the default local and self-hosted object store in the docs, the docker-compose dev stack, the Helm chart and the agent-fleet demo. MinIO's community edition entered maintenance mode on 2025-12-03 and was later archived (AGPL-3.0). `operon dev` keeps `file://` and `memory://`; the compose file (or a `--with-rustfs` flag) starts RustFS next to it. The pin is `rustfs/rustfs:1.0.x`, moving to 1.0.1 once released for its two security fixes.

Checked against RustFS's source (`d8bf268`) and object_store 0.14.2: create-only PUT returns 412 under the commit write lock (atomic across endpoints in distributed mode), `If-Match` returns 412 on a mismatch and 404 on a missing key, and both map correctly to `StoreError`. RustFS never returns S3's 409 or `503 SlowDown`; `FaultyStore` keeps covering those.

A new **`Store` provider conformance suite** (`crates/operon-store/tests/provider.rs`, enabled by `OPERON_TEST_S3_URL`) checks:

1. 16 concurrent `put_if_absent` on one key: exactly one `Ok`, 15 `AlreadyExists`.
2. A `put_if_match` chain v1 → v2 → v3: a stale ETag and a missing key both get `PreconditionFailed`.
3. The ETag a PUT returns equals the ETag from HEAD and GET, **also with SSE on**.
4. Bounded ranges, ranges past the end, and invalid ranges.
5. **A list right after a put includes the new key, also across pages.**
6. DELETE of a missing key returns `Ok`.
7. A read right after an overwrite returns the new bytes.
8. **Objects survive a restart and a kill -9 of the store's container.**

Checks 3, 5 and 8 settle what is unverified on RustFS: ETag stability with SSE, list-after-write, and fsync durability in single-disk mode.

It runs against `memory` and `file`, **RustFS on every PR**, floci S3 in the AWS job, and real S3, GCS and Azure nightly. The **S3 fault matrix** gains a backend switch (in-memory or RustFS, with `FaultyStore` around either) and must produce the **same expected table** on both; only timing may differ. The kill -9 crash gate and the M1.1 gates also run over RustFS nightly. M2's provider list becomes **S3, GCS, Azure, RustFS**.

### 4.5 CI layout

- Every PR: openraft; Postgres; the provider suite and the S3 fault matrix over RustFS.
- PRs touching the relevant paths: floci (full DynamoDB suite); Alternator (single-item subset); TiDB unistore (M6).
- Nightly: real DynamoDB and S3, GCS, Azure; the AWS deployment job; toxiproxy soaks; `tiup playground` (M6); the crash gate and the M1.1 gates over RustFS.

## 5. A router for millions of namespaces (D63, D75)

### 5.1 What breaks first today

`MetaState` is one in-memory struct, and since M1.3 every node runs a learner replica of all of it (M1.3 Ruling 10). Per collection with one partition it costs about 2.5–3.5 KB in memory and 0.8–1.1 KB in a snapshot **(estimate)**: 3–4 GB of RAM per node and a 1 GB snapshot at 1M namespaces with one collection each, and 30–40 GB and 10 GB at ten collections each. In the order they break:

1. **Every metastore change re-reads the whole catalog on every node.** `CatalogCache` re-reads `namespaces()`, then each namespace's `collections()` and `aliases()`, on every `watch_changes` wake-up, and every `commit_wal` wakes it. That is O(N) per change per node, and it breaks long before memory does.
2. **Maintenance polls the whole catalog.** Link candidates call `links(None)` plus one `stream_state` per link; the segmenter and retention call `streams(None)`; trim and index builds call `collections(None)` and `collection_heads(None)`; GC calls `namespaces()`. Each pass is O(catalog) on every worker, although most namespaces are idle.
3. **The trait has no pagination.** Every list returns one `Vec`. DynamoDB and TiDB cannot serve a million rows per call.
4. **Building a snapshot stops apply.** `build_snapshot` encodes the whole state under the read guard, and `apply` needs the write guard: seconds of stall at 1–10 GB. The snapshot is also copied up to three times in memory.
5. **Snapshots stop at 5 GiB.** A snapshot is one PUT, and `Store` has no multipart upload (M1.3 Ruling 6): about 5M collections **(estimate)**.

The Raft write rate is not the problem: `commit_wal` scales with nodes × flushes, not with namespaces (D25).

### 5.2 Components

1. **Gateways**, stateless: authenticate, admit against quotas, resolve names through a directory cache, route.
2. **The directory**, a small global store: org → namespaces, and namespace name → `{id, shard, state, size class}`. It knows only namespaces; no stream, collection, table or graph has an entry of its own (D75). It is **versioned, replaced whole per change, and pushed or long-polled to gateways**, as Neki's data topology is (etcd, pushed to routers without restarts). It lives in the `ControlStore` (§6) on any backend.
3. **Metastore shards**, reached through **`ShardedMetaStore: MetaStore`**, which routes each call by namespace. All of one namespace's data-plane metadata lives in one shard, **whatever the resource kind** (D75): its streams, collections, Iceberg tables (M4) and graphs (M3), with their heads, index, pointers, consumers and leases. Operations across kinds in one namespace therefore stay atomic. A shard is an openraft group (M6's multi-Raft), a Postgres database, or a DynamoDB or TiDB deployment, which partition natively. Cluster-level state (WAL commit records, WAL live counts, retired WAL objects) is kept per shard: a WAL object is retired once every shard's live count for it reaches zero.
4. **Query, log and worker nodes**, stateless as today.

### 5.3 Placement for every resource kind (D75)

One router places every resource kind.

- M1.3's rendezvous hashing stays (xxh3, zone-aware, *r* replicas; M1.3 Ruling 13).
- **Placement key** `(ns, kind, id[, shard])`, on the same rendezvous hashing:

  | Kind | Key | What affinity buys | Milestone |
  |---|---|---|---|
  | Stream partition (fetch, subscribe) | `(ns, stream, partition)` | Tail cache and segment cache hits | M2 |
  | Collection | `(ns, collection)`; very large ones `(ns, collection, shard)` | Hot tier, pinned splits, HNSW artifacts | M1.3; `shard` in M6 |
  | Iceberg table | `(ns, table)` | Query and compaction affinity (T0–T3) | M4 |
  | Graph | `(ns, graph)` | Hot adjacency chunks | M3 |

- **Size classes still apply:** small namespaces place by `ns` alone, so one node warms all of a tenant's objects together; large namespaces place each object by its key. Hot keys raise *r* (§04 §5).
- **Bounded load:** when the top node is above its load threshold, the next rendezvous choice serves the request.
- **Writes need no routing.** The leaderless WAL lets any `log` node append for any partition (§02 §3), and the metastore orders the writes. A gateway sends a write to any `log` node in the client's zone.
- **Consumer-group coordination** (native named consumers from M2, Kafka groups from M5) runs on the rendezvous owner of `(ns, group)`. Committed offsets live in the metastore, and offset commits are conditional (on the consumer's lease epoch or the group's generation), so coordination is correct even when the owner is wrong: a stale owner's commit fails.
- **Workers own maintenance by the same keys:** segmenting and retention by `(ns, stream, partition)`, merges, compaction and index builds by `(ns, collection)`, Iceberg compaction by `(ns, table)`, sidecar builds by `(ns, graph)`. Leases still fence every task (§09).
- **Quotas are keyed the same way** (§6): a key's request-rate bucket lives at the key's rendezvous owner, and a namespace-wide limit is shared among its keys' owners, with the per-gateway fallback.
- **Ownership is a soft hint**, as in turbopuffer and M1.3 Ruling 14: correctness never depends on the owner, and any node can serve any namespace. A stale directory entry therefore routes suboptimally, never wrongly.

### 5.4 Metadata scaling

- **Paginated lists:** every list method takes `(after, limit)`.
- **A scoped change feed:** a monotonic `catalog_version` with `changes_since(version)` returning the namespaces whose catalog changed, and `watch_changes` scoped to one namespace or to the catalog. `CatalogCache` then updates incrementally, and a `commit_wal` no longer wakes it.
- **Dirty sets:** maintenance is driven by streams with new commits and collections with new manifests, not by full scans. It runs on the owning worker (rendezvous over worker nodes).
- **Nodes cache only the namespaces they own**, loaded lazily through a remote client and invalidated by the scoped feed. No node holds the whole catalog, and every-node learners become optional. This is the same client as BYOC's remote metastore (§8).
- **openraft's catalog leaves the monolithic snapshot**: either its applied state moves into redb with incremental snapshots, or each shard is its own openraft group (Q23).

### 5.5 Namespace moves are metadata-only

All bulk bytes are already on the bucket under `ns/<id>/`, and a namespace's metadata is small. A move therefore copies only metadata: fence the namespace (its lease epoch), copy its rows to the target shard, flip the directory entry, unfence. Gateways **buffer** the namespace's writes during the flip, as Neki's routers do during cutover, so writes pause for milliseconds instead of failing. The old shard serves reads for the namespace until its copy is removed, then answers `NamespaceMoved` (§3.5). Neki and PgDog move data through logical replication into new shards; Loam never copies data.

### 5.6 Failures

| Failure | What happens |
|---|---|
| A node is lost | Its registry lease expires (10 s); rendezvous moves only its keys; gateways mark it suspect for 5 s; queries fall back to local execution |
| One metastore shard is down | Only its namespaces' writes and linearizable reads stall; cached reads continue |
| The directory is down | Gateways route from their cached copy (safe: ownership is a hint); namespace creates and moves pause |
| A namespace is mid-move | Writes are fenced by the namespace lease's epoch; gateways retry after refreshing the directory |
| Routing is stale (an old membership view or directory entry) | A non-owner serves the request with colder caches; consistency tokens still hold (§3.5) |

### 5.7 Staging

| Stage | What | Milestone |
|---|---|---|
| 1. Remove the O(N) readers | Paginated lists; the scoped change feed and an incremental `CatalogCache`; dirty-set maintenance | **M2** |
| 2. openraft snapshot hygiene | Build snapshots without blocking apply and with at most one transient copy; snapshots past 5 GiB (a multipart `Store` put or a chunked snapshot; Q23); snapshot size and encode-time metrics | **M2** |
| 3. Bounded load and per-kind keys | On M1.3's rendezvous placement; placement keys for stream partitions and consumer coordination (D75; graphs in M3, tables in M4); the consistency-token gate across nodes and backends (§3.5) | **M2** |
| 4. Nodes stop holding everything | The remote client with per-namespace caches of owned namespaces; the directory pushed to gateways, served by the hosted `ControlStore` | **M2.x** |
| 5. Shard the metastore | `ShardedMetaStore`; metadata-only moves with gateway buffering; size-class placement keys; openraft's catalog out of the monolithic snapshot; per-shard GC state; the 1M-namespace gate | **M6** |

Stages 1–3 are what breaks first and are cheap, so they ship in v1.0. The trait changes stage 5 needs (per-group `commit_wal`, bounded-skew stamps, the namespace on every call) land in M2's first task (§3.4), so M6 changes no trait signature. M1.2, which is being implemented, is not changed: its `CatalogCache` is replaced in M2.

### 5.8 What Loam takes from the reference systems

- **Neki** (PlanetScale; proprietary, public docs only): a versioned topology document replaced whole and pushed; routers that buffer during cutover; lookup tables for secondary keys (Loam's name → id items already are); shipping without atomic cross-shard commits.
- **PgDog** (AGPL-3.0; **reference only, no code is copied**): shard routing with a direct/multi/all route; two-phase commit needing a coordinator WAL, which is why Loam avoids cross-shard atomicity; centroid-based vector sharding (`pgdog-vector`), relevant if very large collections are later sharded by IVF centroid.
- **turbopuffer**: routing as a soft cache-affinity hint by consistent hash of (org, namespace), about 100k namespaces per node, "unlimited (seen: 250M+)" namespaces, and at most 3 serial object-store round trips on a cold query. Loam does **not** adopt its per-namespace WAL (one entry per second per namespace, one PUT each): Loam's WAL objects span namespaces (D25) and are committed per shard (§3.1).
- **WarpStream**: a strongly consistent metadata database sequences writes and assigns offsets (DynamoDB on AWS, Spanner on GCP, Cosmos DB on Azure). Loam is already this model (D10: Kafka-rate metadata cannot run on S3 compare-and-swap).

**Cold start target.** A query on a cold namespace needs the directory entry (usually cached), `collection_head` (one metastore round trip), a manifest GET, and the Lance and Tantivy footers: at most 3 serial object-store round trips, p50 300–900 ms **(target)**. The prewarm API and pinned namespaces cover planned traffic.

## 6. Tenancy, API keys and quotas (D65)

- **Model:** org (tenant and billing unit) → namespaces (the existing unit of isolation) → collections. Each namespace belongs to one org, and namespace names are unique within an org, so name lookups are keyed by org and name (on DynamoDB, `nsname#<org_id>#<name>`, §2.3); the M2 plan adds the org to the trait's name lookups.
- **`ControlStore`**, a trait separate from the data-plane `MetaStore`, so it can be hosted remotely (BYOC, §8). It holds orgs, API keys, role bindings, quotas, usage rollups and, from M2.x, the directory (§5.2). In M2 it runs on the cluster's metastore backend; in M2.x the hosted control plane serves it.
- **API keys:** `loam_<key_id>_<secret>`. The store keeps `{key_id, org, sha256(secret), scopes, expiry, created_by, last_used}`, never the secret. Each surface's credential (§10 §4) resolves to a principal `{org, key or user, scopes}`. Gateways cache resolved keys for 30–60 s, and revocations are pushed through the `ControlStore`'s change feed. OIDC/JWT comes later.
- **Quotas:**

  | Quota | Enforced |
  |---|---|
  | Request rate per namespace, per surface | Token bucket at the rendezvous owner of the placement key (§5.3), which receives most of that key's traffic; fallback: a bucket per gateway sized quota ÷ gateways |
  | Ingest bytes/s | Token bucket at the gateway |
  | Concurrent queries | Semaphore at the gateway and at the owner |
  | Storage bytes | Soft limit at write admission, computed periodically from partition bytes and manifest sizes, including bytes held only by tags (§17 §4.3) |
  | Metadata operations (collection creates, alias updates, leases per namespace) | Rate limit, protecting the shared metastore; matters most on DynamoDB's per-item limits and on Postgres |

  A request over quota gets its surface's throttling error: HTTP 429 (native REST, Qdrant, ES) or gRPC `RESOURCE_EXHAUSTED` (native gRPC, Qdrant gRPC, Flight SQL).
- **Milestones:** orgs, API keys, RBAC and quotas in M2 (v1.0). The hosted `ControlStore` with BYOC in M2.x.

## 7. Authorization (D66, D67)

**An `Authorizer` trait** in `operon-common`, after Lakekeeper's, in Loam's terms:

- `check(principal, action, resource)`, `batch_check`, and `filter_visible(list)`;
- lifecycle hooks `on_created` and `on_deleted` for orgs, namespaces, collections and API keys;
- `bootstrap`.

Every surface maps each request to `(action, resource)` and calls it.

**Implementations:**

| Implementation | Milestone | Notes |
|---|---|---|
| `AllowAll` | M2 | Dev and single-user deployments |
| `Rbac` | M2 | Built in: roles grant `read`, `write`, `admin` over namespaces and collections; bindings in the `ControlStore` |
| `OpenFga` | M2.x *(default, D67)* | Fine-grained, delegated grants for multi-org tenancy and BYOC |

**OpenFGA:**

- **Dependency:** `openfga-client` 0.6 (Apache-2.0, maintained by Vakamo, gRPC through tonic and prost 0.14, protos vendored). The OpenFGA server is Apache-2.0 and runs as a separate service.
- **Model:** adapted from Lakekeeper's modular v4.12 model (`authz/openfga/v4.12/components/*.fga`): subjects `[user, api_key, role#assignee]`; a bare grant relation plus an inherited effective relation per privilege; Lakekeeper's server → `platform`, project → `org`, warehouse and namespace → `namespace`, table → `collection`. Object ids are global: `namespace:{org}/{ns_id}`, `collection:{org}/{ns_id}/{cid}`. Loam adds a **tenant fence** Lakekeeper's model lacks: a grant is effective only `and tenant_member`, so no grant can make a subject from another org effective.
- **Copied with attribution:** the `.fga` components and the patterns of Lakekeeper's `migration.rs` (model versions through `TupleModelManager`) and `reconcile.rs`, keeping Lakekeeper's `NOTICE` ("Copyright 2024-2026 Vakamo Inc.") under Apache-2.0 §4(d) and marking changes. Lakekeeper's `Authorizer` trait and `authorizer.rs` are reference only.
- **Tuple writes through a transactional outbox.** The tuple change is a row written in the same `ControlStore` or metastore transaction as the resource change. A worker drains the outbox with idempotent writes (ignoring duplicates on write and missing tuples on delete), and a `reconcile` job repairs drift. Lakekeeper writes tuples before its database commit and deletes them after it, which orphans tuples when the commit fails; the outbox cannot.
- **Reads:** a decision cache in the gateway with a TTL of at most 5 s, keyed by (principal, action, object). `MinimizeLatency` reads, and `HigherConsistency` right after the principal's own writes.
- *(default, D67)* **OpenFGA moves from M6 to M2.x**, with the control plane, and Loam and Lakekeeper (M4) **share one OpenFGA store**: Lakekeeper's types stay unchanged, and Loam's are added beside them as modules of a schema 1.2 model. Whether both products' migration managers can share one store is Q21.

## 8. BYOC (D64)

Both modes ship in **M2.x (v1.1)**, after v1.0.

| | **BYOC-managed-meta** (the WarpStream model) | **BYOC-local-meta** (the turbopuffer model) |
|---|---|---|
| Metastore | Remote: `operon-meta-remote` in the customer's VPC talks to the hosted metastore in `operon-control` | In the customer's VPC: openraft, or the customer's Postgres or DynamoDB |
| Control plane role | Serves the metastore and the `ControlStore`; a hard dependency for writes | A pull-based ops agent only (upgrades, scaling, telemetry, billing aggregates); not on the data path |
| Control plane down | Cached reads continue; writes stall | Nothing on the data path changes |

**`operon-meta-remote`**, a `MetaStore` client over gRPC (tonic):

- The trait is already shaped for RPC (semantic calls, owned arguments, one transaction per call), so the mapping is mechanical.
- Every write carries an **idempotency key**. The server keeps call results for `WAL_COMMIT_WINDOW_MS`, which turns most unknown outcomes into known ones, as `ClientRequestToken` does on DynamoDB.
- `watch_changes` is a server stream with a resume token. `Local` reads come from the client's per-namespace cache (§5.4).

**`operon-control`**, the hosted multi-tenant control plane:

- Serves many BYOC clusters, each a virtual cluster keyed by `cluster_id`, on DynamoDB, Postgres or (from M6) TiDB, with the sharded design of §5.
- Authenticates each data plane with per-cluster mTLS or an agent key and scopes every call to that cluster.
- Runs in the data plane's region: every `commit_wal` pays one round trip to it.

**The data boundary.** The control plane may see namespace and collection names, schemas, object paths, offsets, pointers and lease keys, **in clear**. It never sees documents, vectors, text or bucket credentials. GC lists and deletes objects inside the customer's VPC.

**Deferred:** a Ripcord-style degraded ingest (write the WAL while the control plane is unreachable, sequence it later). The Qdrant and ES surfaces acknowledge only after the commit, and acknowledging earlier would break read-your-writes.

## 9. GDPR erasure (D68, D69)

**Where a document's bytes survive a delete:** the implicit stream's records (WAL objects that span namespaces, then segments); Lance fragments (a delete writes a deletion file); Tantivy splits (external delete bitmaps); PK deltas and the PK index (keys may be personal data); dead letters; superseded manifests kept for `time_travel_retention` (D38); **dataset tags, which pin manifests indefinitely** (D52); hot HNSW artifacts; the NVMe and RAM caches; noncurrent object versions if bucket versioning is on; from M4 and M5, Iceberg snapshots and changelog streams.

**The path (M2):**

1. **`erase`**, by primary key or by filter, on the native API. It runs the normal delete, which hides the data at once, and records an **erasure request** in the metastore: `{ns, collection, key hashes, offset, deadline}`.
2. **Forced purge**, a worker task per affected collection: a compaction that materializes the deletions in the Lance fragments that hold the rows; a merge that re-indexes the affected splits; rewritten PK deltas, PK index entries and dead letters; rebuilt hot artifacts; and **time travel dropped before the erasure point** (older manifests released early, overriding D38's 24 h).
3. **Stream trim.** Once older manifests are released, the implicit stream is trimmed past the erasure offset. Segments below it are retired, and each WAL object is retired once all its chunks are segmented or trimmed.
4. **GC** deletes the retired objects after the grace period and **explicitly evicts their keys from the RAM and NVMe caches**, not only by LRU.
5. **Tags** *(default, D69)*: an erasure **rewrites a tagged manifest onto a purged copy**; the tag records that it was rewritten, by which erasure and from which manifest version. Erasure wins over bit-exact reproducibility.
6. **Proof:** an **erasure log** holds keyed key hashes, the request and completion times, and the objects rewritten or retired. A key hash is HMAC-SHA256 of the key's canonical encoding under a per-org erasure-log key, held in the `ControlStore` and wrapped by the deployment's KMS key where there is one. Each entry records its key version; rotation starts a new version, and destroying an org's keys makes its hashes unlinkable. The org can prove a key was erased by recomputing its HMAC, but a plain dictionary attack on guessable keys (email addresses) does not work. The hashes are pseudonymous, not anonymous: the log is readable only by the org's `admin` role and the operator's audit role, and is kept for a retention period the org configures (the M2 plan proposes the default).
7. **Deadline** *(default, D69)*: completion within **30 days**. The expected completion time is bounded by `max(compaction deadline, retention override) + segmenter lag + GC grace`.
8. **Crypto-shredding** *(default, D69)*: per-chunk envelope encryption moves forward from Phase B to **M2.x**. Because WAL objects span namespaces, this needs a data key per chunk (the note on D25). Destroying a namespace's key then makes its bytes unreadable everywhere at once, including WAL objects, noncurrent versions and backups.
9. **Versioned buckets:** S3 applies lifecycle expiry asynchronously, so the purge does not rely on it. Objects are never overwritten in place: a rewrite writes a new object and retires the old one. GC deletes every version of every object an erasure retires, current and noncurrent, and every delete marker for it, by version id (`ListObjectVersions`, then `DeleteObject` with `versionId`); a plain delete would only add a delete marker and keep the data. The erasure completes only after a version listing shows no version or delete marker of those objects remains. `object_store` has no versioned list or delete (verify), so this is a `Store` extension over the provider SDKs. With cross-region replication, the same deletion runs against the replica bucket, since deletes by version id are not replicated (verify). A lifecycle rule that expires noncurrent versions stays as a backstop (§10 §6).

The M2 gate: after an erasure completes, the key is unreadable through every surface, absent from every object and object version in the bucket (a byte scan), from every retained or tagged manifest and from the caches, and the erasure log records it. Iceberg tables (M4) and changelog streams (M5) extend the path in their milestones.

## 10. Ids under sharding (D70, default)

- **Ids stay unsharded.** Namespace, stream, collection and link ids stay dense `u64` counters (D18), with no shard bits and no format change.
- **Calls gain the namespace.** In M2's first task (§3.4), every `MetaStore` call that takes a bare `CollectionId`, `StreamId` or `LinkId` (for example `stream`, `stream_state`, `set_retention`, `trim_partition`, `partition_index`, `segment_referenced`, `collection`, `collection_head`, `collection_for_link`, `update_collection_schema`) also takes the `NamespaceId`. `WalCommit` and `SegmentSwap` carry the namespace per chunk or entry. Lease calls take a scope: a namespace, or the cluster for cluster-wide leases (GC, the node registry). `ShardedMetaStore` then routes every call without a lookup.
- **No M1.7 format change is needed.** Object paths already carry `ns/<namespace_id>/` (§01 §6); WAL objects are cluster-level (D25) and are committed per shard (§3.1); pointer keys are already scoped by namespace in `cas_pointer` and `pointer`. The openraft command encoding keeps its bare ids, and the namespace is only a call argument there.
- The alternative, ids that carry their shard, would change every id's meaning and every stored reference, and would make moving a namespace between shards rewrite ids.

## 11. Risks and open questions

Risks 22–26 in §12 cover the relaxed contract, emulator fidelity, router complexity, v1.0 scope and erasure completeness. Open questions Q21–Q25 and Q27 in §13 cover the shared OpenFGA store, floci's `TransactGetItems`, openraft snapshots at scale, TiDB dialect details, owner confirmation of the defaults, and metadata restores after an erasure.

## 12. Sources

- Lakekeeper: github.com/lakekeeper/lakekeeper @ `b771dbf` (`crates/lakekeeper-storage-postgres/src/{tabular/table/commit.rs,tasks.rs,idempotency.rs,dbutils.rs,advisory_lock.rs}`, `crates/authz-openfga/`, `authz/openfga/v4.12/`) · github.com/vakamo-labs/openfga-client @ `11ed87b` · openfga.dev
- DynamoDB: docs.aws.amazon.com/amazondynamodb/latest/developerguide/{WorkingWithItems,transaction-apis,HowItWorks.ReadConsistency,burst-adaptive-capacity,Programming.Errors,TTL}.html · docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html · `aws-sdk-dynamodb` 1.128.0 sources · aws.amazon.com/dynamodb/dynamodblocallicense
- ScyllaDB Alternator: docs.scylladb.com/manual/stable/alternator/compatibility.html · docs.scylladb.com/manual/stable/alternator/new-apis.html · scylladb.com/source-available-faq
- floci: github.com/floci-io/floci @ `df67d42` (`LICENSE`, `docs/configuration/storage.md`, `.github/workflows/compatibility.yml`) · floci.io · blog.localstack.cloud/2026-upcoming-pricing-changes
- RustFS: github.com/rustfs/rustfs @ `d8bf268` (`LICENSE`, `CLA.md`, `docs/architecture/s3-compatibility-matrix.md`, `crates/ecstore/`) · github.com/minio/minio/issues/21714
- TiDB: docs.pingcap.com/tidb/stable/{pessimistic-transaction,optimistic-transaction,transaction-isolation-levels,auto-increment,error-codes} · github.com/pingcap/tidb (`pkg/errno/errcode.go`, `pkg/parser/terror/terror.go`)
- Postgres: postgresql.org/docs/current/functions-info.html (`pg_xact_status`)
- Neki: planetscale.com/blog/{announcing-neki,the-architecture-of-neki,what-is-a-neki-router,what-is-a-data-topology} · planetscale.com/docs/neki/{vitess-and-postgres,data-migration,platform-preview-limitations,reference-tables-and-gsis}.md
- PgDog: github.com/pgdogdev/pgdog @ `c5e5c34` (AGPL-3.0; read, not copied)
- turbopuffer: turbopuffer.com/docs/{architecture,guarantees,limits,byoc} · turbopuffer.com/blog/{control-plane,object-storage-queue}
- WarpStream: docs.warpstream.com/warpstream/overview/architecture · warpstream.com/blog/the-art-of-being-lazy-log-lower-latency-and-higher-availability-with-delayed-sequencing · warpstream.com/blog/secure-by-default-how-warpstreams-byoc-deployment-model-secures-the-most-sensitive-workloads
- Toxiproxy: github.com/Shopify/toxiproxy
- Operon as built (M1.2a worktree): `crates/operon-meta/src/{state/mod.rs,state_machine.rs,codec.rs,client.rs}`, `crates/operon-query/src/catalog_cache.rs`, `crates/operon-meta-conformance/`, `crates/operon/tests/fault_matrix.rs`
