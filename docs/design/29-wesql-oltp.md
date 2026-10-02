# 29 — WeSQL as Loams's MySQL-on-the-Bucket OLTP Engine

Status: **Approved** (owner defaults, 2026-10-02: "do suggested for all") · 2026-10-01. This document comes from the owner's question, "will wesql become usable with proper transaction support, refer tidb (go) or starrocks (java), implement all starrocks features on wesql, both use rocksdb+iceberg on s3", and from the direction the owner approved in answer: verify the transaction model, then close WeSQL's gaps in four milestones (WS1–WS4), keep analytics on Iceberg beside it, and do not port StarRocks. Everything here was a **proposal**, approved by the owner on 2026-10-02 ("do suggested for all"; Q271 stays an owner action): decisions **D273–D280** and open questions **Q271–Q279**. It extends [§23](23-neon-and-wesql.md) (D148, D154, D156, Q50, Q51) and builds on [§28](28-loams-postgres.md) §7.2 (Arm A). It changes no code in Loams; the code work is in the fork `ostrium-labs/wesql` and, for WS2, in `loams-safekeeper`.

Markers, as in §23 and §28:

- **(verify)** means not checked against a primary source, or checked only by reading code that was not run. The PR that depends on it checks it first.
- **(estimate)** means computed or reasoned, not measured.
- **(spike)** means measured in the 2026-09-29 spike of §23 §9.
- Paths of the form `wesql/…` point into `ostrium-labs/wesql` branch `8.0` at `eef34f452` (2026-09-29, after the four merged fork PRs). Paths of the form `mysql/…` point into Oracle `mysql-8.0.46` (`976e5da9`). `tidb/…` points into `pingcap/tidb` `master` at `8936d7b` (2026-09-25). `starrocks/…` points into `StarRocks/starrocks` `main` read through the GitHub API on 2026-10-01.
- Nothing in this document is posted to `wesql`, `pingcap` or `StarRocks`. Upstream work needs the owner's explicit go-ahead (§23 §2.2).

---

## 1. Summary

| # | Decision | Status |
|---|---|---|
| D273 | **WeSQL (the fork `ostrium-labs/wesql`) is Loams's candidate MySQL OLTP engine whose storage lives entirely on the bucket, built as milestones WS1–WS4** (§11). It stays a separate, unmodified-by-Loams process (D148) and the choice against TiDB is made by the rule in D280. **Proposes to amend D156**: WeSQL stops being "dev compose only" only once WS1 and WS2 pass their acceptance tests; until then D156 and the Q50 gate stand | Approved (owner defaults, 2026-10-02) |
| D274 | **Transactions: claim only what the source verifies** (§4). Loams documents WeSQL as single-node ACID with READ COMMITTED and REPEATABLE READ (snapshot isolation with first-committer-wins on locked keys), point locks and no gap or next-key locks, no SERIALIZABLE, no `ROLLBACK TO SAVEPOINT` after a write, and user-level XA untested. Closing those gaps is backlog (T1–T4, §4.3), not a milestone, except where WS1 needs one | Approved (owner defaults, 2026-10-02) |
| D275 | **WS1: foreign keys are enforced in the SQL layer of the fork, not inside SmartEngine** (§5), in the handler wrappers (`ha_write_row`, `ha_update_row`, `ha_delete_row`), behind `wesql_enforce_foreign_keys` (default `OFF`, so today's strip-with-warning stays the default). Checks are current reads with a **shared lock on the parent's primary-key row**. Referential actions run as nested row operations that **are written to the row binlog**, and applier threads do not re-cascade. TiDB's implementation is the reference design | Approved (owner defaults, 2026-10-02) |
| D276 | **WS2: the binlog, not SmartEngine's redo, is the log that is made durable in the Loams WAL quorum before a commit is acknowledged** (§6). The shipping point is the sync stage of MySQL's ordered commit. SmartEngine's own WAL stays local and may be flushed lazily in this mode | Approved (owner defaults, 2026-10-02) |
| D277 | **The protocol boundary is a wire protocol, and the client in WeSQL is GPL-2.0-only code** (§6.4, §10). The acceptors are `loams-wal` (Apache-2.0, separate process, Arm A). The client lives in the fork, is written there, and copies no Apache-2.0 Loams source. The protocol is the safekeeper v3 message set (§28 §6.9) carrying a timeline kind `mysql-binlog` over TLS | Approved (owner defaults, 2026-10-02) |
| D278 | **WS3: failover is rebuilt on the same quorum** (§7): terms in the acceptors (metadata in TiKV, as in D268), a primary record in `x/<ns>/<db>` changed by compare-and-set, a lease that makes a deposed primary stop, and a replica that catches up from the snapshot, the archived binlog in the bucket and the WAL tail. Single writer; no multi-primary | Approved (owner defaults, 2026-10-02) |
| D279 | **WS4: analytics through Iceberg, not through a port of StarRocks** (§8). The binlog bridge (D154) writes a changelog stream per table, and links (§09) maintain a keyed Iceberg table in Lakekeeper on RustFS. StarRocks or Loams's own engine read that table as a sidecar. Porting StarRocks' features into the MySQL row executor is **rejected** | Approved (owner defaults, 2026-10-02) |
| D280 | **TiDB is the alternative, and the rule for choosing it is stated** (§9): WeSQL only if the requirement is that the OLTP engine's storage lives entirely on the bucket. TiDB on Loams's TiKV already has distributed transactions, foreign keys and HA, but D260 (no TiDB anywhere) is the owner's, and choosing TiDB reverses it. This document does not | Approved (owner defaults, 2026-10-02) |

## 2. Goals and non-goals

### 2.1 Goals

1. **A MySQL engine for apps that need one,** with real transactions, foreign keys, and a durability and failover story that holds when the local volume is lost.
2. **Storage entirely on the bucket.** Snapshots, SmartEngine's extents and the log live in RustFS, and no committed transaction depends on a local disk surviving (§6).
3. **Reuse Loams's pieces, build only the join.** Arm A's acceptors (§28 §7.2), the TiKV metadata and `x/` records (§23 §6.2), the binlog bridge (D154), Iceberg and Lakekeeper (§08).
4. **An honest record** of what SmartEngine, TiDB and StarRocks are (§3), because the owner's question rested on a mixed-up picture of the three.

### 2.2 Non-goals

- **Porting StarRocks or TiDB code into WeSQL** (§8.3).
- **Multi-primary or distributed transactions** in WeSQL. It is one writer at a time (D278).
- **Making WeSQL the default MySQL answer.** TiDB is better on every axis except the bucket-only storage (§9).
- **Linking WeSQL into Loams,** or patching it in a way that makes Loams's code GPL (D11, D148, §10).
- **Upstream contributions** without the owner's go-ahead.

## 3. Correcting the record

Each claim in the question, checked against primary sources on 2026-10-01.

| Claim | Finding | Source |
|---|---|---|
| "StarRocks is Java" | Half true. The frontend (FE) is Java: it holds metadata, plans queries and keeps its metadata in BDB JE. The backend (BE and, in shared-data mode, the compute nodes) is C++: it stores data and runs the vectorized executor. The repository is 76 MB of Java and 59 MB of C++ by GitHub's count | docs.starrocks.io/docs/introduction/Architecture/ ("BDB JE (Berkeley DB Java Edition) to store and maintain a complete copy of the metadata"); `starrocks/fe/`, `starrocks/be/src/`; GitHub languages API |
| "StarRocks uses RocksDB" | Not as its table storage. Tables are StarRocks' own columnar segment files ("Data is organized into segment files"). RocksDB appears in the BE as a local key-value store for tablet and rowset metadata (`be/src/storage/kv_store.{h,cpp}`, `tablet_meta_manager.cpp`), and the persistent primary-key index writes its *metadata* (`PersistentIndexMetaPB`) there (`be/src/storage/persistent_index.cpp`, "write PersistentIndexMetaPB in RocksDB"). The index itself is StarRocks' own structure (`class PersistentIndex` in `persistent_index.h`); the docs describe a hash map with an optional persistent index, and a `CLOUD_NATIVE` index on object storage in shared-data clusters. So "RocksDB only for the primary-key index" is **too narrow in one way and too broad in another**: RocksDB holds BE-local metadata, and the index data is not RocksDB (verify the last point by reading `persistent_index.h` beyond its class declaration) | docs.starrocks.io/docs/table_design/table_types/primary_key_table/; `starrocks/be/src/storage/` as listed |
| "StarRocks uses Iceberg on S3" | Iceberg is an **external catalog**: StarRocks queries Iceberg tables in place, without loading them ("query data from Iceberg without loading data into StarRocks or creating external tables"). Its own tables are separate. In shared-data mode its own tables also live on object storage, but in its own format | docs.starrocks.io/docs/data_source/catalog/catalog_overview/ and the Iceberg catalog page (found by search; the page's own URL moves between versions) |
| "TiDB uses RocksDB and S3" | TiDB stores data in **TiKV**, a distributed transactional key-value store (TiKV uses RocksDB inside each node). Classic TiDB keeps data on **local disks** with replicas, and "shared-nothing". **TiDB X** (TiDB Cloud Starter, Essential and Premium) is the object-storage architecture: "TiDB X uses object storage, such as Amazon S3, as the single source of truth for all data". Whether TiDB X is available outside TiDB Cloud is **not verified** | docs.pingcap.com/tidb/stable/tidb-architecture/; docs.pingcap.com/tidbcloud/tidb-x-architecture/ |
| "WeSQL is RocksDB plus Iceberg on S3" | WeSQL is MySQL 8.0 with **SmartEngine**, a RocksDB-derived LSM engine (its `LICENSE` is RocksDB's BSD text; `core/` keeps `db/`, `memtable/`, `write_batch/` and a RocksDB-style `transactions/`), on S3. Its README says all data, binlog and WAL are "**entirely** (**not partially!**)" in S3. There is **no Iceberg** anywhere in the repository outside vendored `extra/` (`git grep -il iceberg` finds nothing) | `wesql/README.md`; `wesql/storage/smartengine/core/LICENSE`; `wesql/storage/smartengine/core/` |
| "StarRocks and TiDB both use RocksDB+Iceberg on S3" | Neither does. **No one of the three combines RocksDB, Iceberg and S3 as one design.** WeSQL has the RocksDB lineage and S3, and no Iceberg. StarRocks has Iceberg (external) and its own storage. TiDB has TiKV and, in TiDB X, S3 | the rows above |

**What the corrected picture means.** Iceberg is where WeSQL, TiDB and StarRocks meet only if something *writes* Iceberg. None of the three does that for OLTP data. Loams does it for collections and tables (§08), which is why D279 puts the bridge in Loams.

## 4. Transactions in SmartEngine, verified in the source

Read on 2026-10-01 from `wesql/storage/smartengine/` and `wesql/mysql-test/suite/smartengine/`. The spike's observation (commit, rollback and `FOR UPDATE` work, §23 §9.2) holds. This section says exactly what is behind it. Nothing below was run beyond the spike and the recorded MTR results; where a conclusion depends on running code, it says (verify).

### 4.1 What is there

| Area | Behaviour | Evidence |
|---|---|---|
| **Atomicity and durability on one node** | A transaction buffers writes in the engine's transaction object, commits atomically, and the engine has its own WAL (`se_db->sync_wal()` on group commit, `smartengine_flush_log_at_trx_commit`: 1 syncs on commit, 0 and 2 defer) | `handler/se_hton.cc` (`se_commit`, `se_flush_wal`); `plugin/se_system_vars.cc` |
| **Two-phase commit with the binlog** | The engine registers `prepare`, `commit_by_xid`, `rollback_by_xid`, `recover` and `recover_prepared_in_tc`. Engine 2PC is on (`smartengine_enable_2pc` = 1, with the comment "2pc is not configurable in the near future, for atomic ddl"). A prepared transaction is a named transaction in the engine (`SetName`, `Prepare`) | `plugin/se_plugin.cc` lines 100–115; `transaction/se_transaction_impl.cc` (`prepare`); `plugin/se_system_vars.cc` |
| **Atomic DDL** | `HTON_SUPPORTS_ATOMIC_DDL` is set | `plugin/se_plugin.cc` |
| **Isolation levels** | **READ COMMITTED and REPEATABLE READ only.** At the first table access under any other level the statement fails with `SE only supports READ COMMITTED and REPEATABLE READ isolation levels. Please change from current isolation level …`. The MTR test `unsupported_tx_isolations` records this for `READ UNCOMMITTED` and `SERIALIZABLE`, for both `INSERT` and `SELECT` | `handler/ha_smartengine.cc` ~line 854; `mysql-test/suite/smartengine/r/unsupported_tx_isolations.result` |
| **READ COMMITTED** | The snapshot is released after every statement, so each statement sees the latest committed data. On a lock conflict the engine releases and re-acquires the snapshot and retries the locking read once | `se_hton.cc` (`se_commit`, `se_rollback`); `ha_smartengine.cc` `get_for_update` |
| **REPEATABLE READ is snapshot isolation** | The snapshot is acquired on the first statement (`SetSnapshotOnNextOperation`, "delayed") or by `START TRANSACTION WITH CONSISTENT SNAPSHOT` (RR only: any other level gets a warning and no snapshot). A write or locking read that touches a key **changed since the snapshot** fails: `ValidateSnapshot` returns `Busy`, which the handler maps to `HA_ERR_LOCK_DEADLOCK` (error 1213) and counts in `snapshot_conflict_errors`. That is first-committer-wins, which InnoDB's RR does not do (InnoDB updates the latest row) | `transaction/se_transaction_impl.cc` (`start_stmt`); `core/transactions/transaction_impl.cc` (`ValidateSnapshot`, `TryLock`); `transaction/se_transaction.cc` (`set_status_error`); `se_hton.cc` (`se_start_tx_and_assign_read_view`) |
| **Row locks** | **Point locks only.** The lock manager keys a striped hash map by `(index id, key)` (`LockInfo{index_id_, exclusive_, expiration_time_, trans_ids_}`); locks are shared or exclusive. A locking read of a key that does not exist locks that key (MTR `lock_rows_not_exist`: two `FOR UPDATE` reads of the same absent key block, a neighbouring key does not) | `core/transactions/transaction_lock_mgr.{h,cc}`; `mysql-test/suite/smartengine/r/lock_rows_not_exist.result` |
| **`SELECT … FOR UPDATE`, `LOCK IN SHARE MODE`, `NOWAIT`, `SKIP LOCKED`** | Present, with MTR tests (`select_lock_in_share_mode`, `select_for_update_skip_locked_nowait`) | `mysql-test/suite/smartengine/r/` |
| **Lock settings** | `smartengine_lock_wait_timeout` defaults to **1 second**, `smartengine_deadlock_detect` defaults to **OFF** (deadlocks end by timeout), `smartengine_lock_scanned_rows` defaults to OFF. A timeout rolls back the statement (1205), a deadlock or snapshot conflict the whole transaction (1213) | `plugin/se_system_vars.cc`; `transaction/se_transaction.cc` |
| **Statement atomicity** | A failed statement inside a transaction is undone through the engine's own savepoint taken at every `start_stmt` (`SetSavePoint`, `RollbackToSavePoint`) | `transaction/se_transaction_impl.cc` |
| **Binlog format** | Updates need `binlog_format = ROW` on a source (`Can't execute updates on master with binlog_format != ROW`), except for replication threads and `unsafe_for_binlog` | `ha_smartengine.cc` ~line 820 |

### 4.2 What is not there

| Gap | Precisely | Evidence |
|---|---|---|
| **No SERIALIZABLE, no READ UNCOMMITTED** | Rejected with the error above | as above |
| **No gap, next-key, range or predicate locks** | The lock manager has no range type. `SELECT … WHERE value > 0 FOR UPDATE` does not stop a concurrent insert of a matching row, so phantoms are possible for locking reads (the repository carries the upstream-heritage test `locking_issues_case4` for phantoms, but no test file in the 8.0 branch calls it, so the behaviour is established by the lock manager's design, not by a recorded result; verify by running it). The comment in `get_for_update`, "holds a gap lock if target key does not exist", means the point lock on the absent key | `core/transactions/transaction_lock_mgr.h`; `ha_smartengine.cc` ~line 3144 |
| **The gap-lock guard is inert** | `is_using_prohibited_gap_locks` has its body commented out (`//TODO implement this function like MySQL 8.0`). The recorded result `gap_lock_issue254.result` (error text about `gap_lock_raise_error`) is therefore stale for this branch (verify by running the test) | `ha_smartengine.cc` line 2106; `mysql-test/suite/smartengine/r/gap_lock_issue254.result` |
| **Write skew** | Snapshot isolation allows it. With no SERIALIZABLE there is no way to forbid it, short of explicit locking reads | follows from the two rows above |
| **`ROLLBACK TO SAVEPOINT` after a write** | `SAVEPOINT` itself is a no-op (`se_savepoint` returns success: "Dummy SAVEPOINT support … needed for long running transactions like mysqldump"). `ROLLBACK TO SAVEPOINT` succeeds only if the transaction has made **no modifications**; otherwise it fails with `SE currently does not support ROLLBACK TO SAVEPOINT if modifying rows` and the transaction becomes rollback-only | `se_hton.cc` (`se_savepoint`); `transaction/se_transaction.cc` (`rollback_to_savepoint`); MTR `rollback_savepoint` covers only the no-write case |
| **User-level XA is untested** | The engine hooks exist (§4.1), and they serve the server's internal 2PC with the binlog. But the XA tests are on the disabled list: `smartengine_main.xa_gtid`, `xa_debug`, `smartengine_binlog.binlog_xa_*`, `smartengine_rpl_basic.rpl_xa_*` ("BUG#000000 XA"). A stale comment in `se_hton.cc` still says "XA is not supported yet". Treat `XA START … XA PREPARE` as unsupported until a test passes | `mysql-test/suite/smartengine/disabled_smartengine.def`; `se_hton.cc` ~line 605 |
| **Foreign keys** | The 8.0.35 image rejects `FOREIGN KEY` at create with `Error 1235: SE currently doesn't support foreign key constraints` (`contains_foreign_key`, still defined in `ha_smartengine.cc`). On `8.0` (MySQL 8.0.46), the DDL compatibility layer from upstream PR 93 **removes the clause and warns** (`ER_WESQL_DDL_FK_STRIPPED`, `sql/wesql_ddl_compat.cc`), so the constraint is silently unenforced. The engine does not set `HTON_SUPPORTS_FOREIGN_KEYS` | `ha_smartengine.cc` lines 267 and 2831; `sql/wesql_ddl_compat.cc`; `storage/smartengine/plugin/se_plugin.cc` (flags) |
| **No temporary tables** | `HTON_TEMPORARY_NOT_SUPPORTED` | `plugin/se_plugin.cc` |
| **Single node** | There is no distributed transaction. HA is separate (§7) | §23 §4.2 |
| **Durability is a local-volume property between snapshots** | Fixed for the crash case by fork PR 3 (replay of the archived binlog after the snapshot), but the archive uploads slices about once a second, so the last second is still local-only (Q50, §6) | fork PR 3; §23 §9.2 |

**The two engines the owner pointed at, for comparison.** TiDB has optimistic and pessimistic transactions over TiKV's Percolator-style 2PC, snapshot isolation and a RR that is snapshot isolation, no SERIALIZABLE (it accepts the syntax and treats it as RR; **verify** against `docs.pingcap.com/tidb/stable/transaction-isolation-levels`), foreign keys from v6.6.0 (GA in v8.5.0), and distributed transactions. StarRocks has no OLTP transactions to speak of: it is an OLAP engine with load transactions (`docs.starrocks.io`; verify the current wording). **Neither is a reference for SmartEngine's locking.** TiDB is the reference for foreign keys (§5), because it implements them above a store that, like SmartEngine, has point locks and no gap locks.

### 4.3 What is proposed (D274)

- **Document the matrix above** in the product docs and in WeSQL's deployment notes, and gate every claim in them on a test.
- **T1, `smartengine_lock_wait_timeout` default.** One second is a bench default, not an OLTP default. The deployment sets it (for example 50 s, as InnoDB) and sets `smartengine_deadlock_detect = ON`.
- **T2, savepoints.** The engine already has `SetSavePoint`/`RollbackToSavePoint` per statement. Implementing `ROLLBACK TO SAVEPOINT` with writes on top is plausible (**estimate:** small, one PR), and ORMs use savepoints for nested transactions (Django `atomic()`, Hibernate). It is the first item after WS1 (Q276).
- **T3, SERIALIZABLE.** Not planned. Apps that need it use explicit `FOR UPDATE`, and the matrix says so.
- **T4, user XA.** Not planned; Loams needs the internal 2PC only.
- **WS1 depends on one transaction property:** a locking read that does **not** validate the snapshot (a *current read*). The engine has it: `TryLock` skips `ValidateSnapshot` when `validate_snapshot` is false or no snapshot exists (`core/transactions/transaction_impl.cc` ~line 590). Whether `ha_smartengine` can request it for an arbitrary key without going through a row read needs a short spike (verify, §11 WS1).

## 5. WS1: foreign keys (D275)

### 5.1 Why not inside SmartEngine

The fork's PR 2 listed what an in-engine implementation needs: the capability flag, checks in `write_row`/`update_row`/`delete_row`, recursive actions from inside the handler with a depth limit, and binlog behaviour for cascades. That is InnoDB's design, and InnoDB's code is not in this tree. Two facts favour the SQL layer instead:

- **MySQL 8.0 already prelocks foreign-key tables.** `process_table_fks` (`mysql/sql/sql_base.cc` ~line 4394) adds the parent and child tables of a DML statement to its prelocking set (for NO ACTION and RESTRICT rules it opens the other side for checks, for CASCADE, SET NULL and SET DEFAULT it opens it for writes). So the other table is already open and metadata-locked when the statement runs. Cascades from a handler would need that machinery anyway.
- **The data dictionary stores foreign keys for any engine that sets `HTON_SUPPORTS_FOREIGN_KEYS`** (`mysql/sql/sql_table.cc` ~line 8253), and PR 2 checked that the SQL layer's DDL paths need no engine help.

So the engine gets the capability flag, and the **enforcement sits in the handler wrappers** (`handler::ha_write_row`, `ha_update_row`, `ha_delete_row` in `sql/handler.cc`), which every DML path goes through (`INSERT … ON DUPLICATE KEY UPDATE`, `REPLACE`, `LOAD DATA`, multi-table `UPDATE` and `DELETE`). The code lives in the fork's overlay (`sql/wesql_fk.{h,cc}`) with a small hook in `handler.cc` in the patch, as `wesql_ddl_compat` does today. It is active only for tables whose engine carries a new WeSQL flag, and only when `wesql_enforce_foreign_keys = ON`. With the option `OFF`, PR 93's strip-with-warning behaviour is unchanged (and `serverless_honor_innodb_engine` from PR 2 remains the escape hatch for apps that cannot wait).

### 5.2 The reference: how TiDB does it

Read from `tidb/docs/design/2022-06-22-foreign-key.md` and `tidb/pkg/executor/foreign_key.go`; the user docs say the feature arrived in v6.6.0 and is GA from v8.5.0 (docs.pingcap.com/tidb/stable/foreign-key/).

| Aspect | TiDB | What WS1 takes |
|---|---|---|
| **Where** | In the SQL layer (executors `FKCheckExec` and `FKCascadeExec`), not in TiKV. TiKV has point locks and no gap locks, as SmartEngine does | Same: the SQL layer, over a store with point locks |
| **Metadata** | The child's `FKInfo` (id, name, parent schema and table, columns, `OnDelete`, `OnUpdate`, state, version); `ReferredFKInfo` on the parent is rebuilt while loading the schema, because a child can be created before its parent when `foreign_key_checks = 0` | MySQL's data dictionary already holds both directions (`dd::Foreign_key`, the `foreign_key_parent` share info used by `process_table_fks`). Nothing new to design |
| **Child insert or update** | Look for the parent: build the parent-index key from the new FK values and `Get`, or scan with batch size 2 for a non-unique index; a per-statement cache of checked keys; NULLs skip the check | The same probe, through the handler on the parent's referenced index |
| **Locking of the parent** | The parent row that is found is locked, **exclusively by default** in pessimistic transactions ("equivalent to `SELECT … FOR UPDATE`"), because TiDB had no shared locks when it was built; `tidb_foreign_key_check_in_shared_lock` switches to shared. In optimistic transactions the key is added as a lock record written during 2PC | **A shared lock on the parent's primary-key row**: SmartEngine's lock manager has shared locks, which TiDB's designers wanted and did not have. This is what lets two children of one parent insert concurrently |
| **Parent delete or update** | Look for children through the child's FK index, then apply the rule: RESTRICT and NO ACTION reject if a child exists, CASCADE and SET NULL build and run nested update or delete executors, SET DEFAULT behaves like RESTRICT | Same rules. SET DEFAULT: the parser accepts it and InnoDB rejects it, so WS1 refuses it at `CREATE` (verify against `process_table_fks`, which still handles the rule) |
| **The race: child insert vs parent delete** | Serialized by the parent row's lock: the child insert locks the parent, so a concurrent delete waits (the design doc's two-session table) | The same, and it needs **no gap lock**: the parent row exists when a child is valid, and it is a point lock. See §5.3 |
| **DDL** | `ADD FOREIGN KEY` is a multi-state schema change: write-only, then a reorg that checks every row (`SELECT 1 FROM child WHERE a IS NOT NULL AND a NOT IN (SELECT id FROM parent) LIMIT 1`), then public. While not public, DML checks but does not cascade | MySQL's `ALTER TABLE` with `ALGORITHM=COPY` semantics for the check. A table is rewritten or locked by the engine anyway; WS1 runs the check query under the DDL's metadata lock (§5.5) |
| **Index** | An index on the FK columns is created if absent; `DROP INDEX` of that index is refused even with `foreign_key_checks = 0`; no prefix indexes | MySQL already creates the index implicitly and refuses to drop it |
| **Limits** | No partitioned tables, temporary tables, BLOB/TEXT, or virtual generated columns; self-references allowed | Same set (SmartEngine already has no temporary tables) |
| **`foreign_key_checks`** | Honoured. `LOAD DATA` reports violations as warnings | Honoured; dumps and ORMs depend on it |
| **Plans** | `EXPLAIN` shows `Foreign_Key_Check` and `Foreign_Key_Cascade` operators, because the hidden work is a common slow-query cause | WS1 adds a status variable and an error-log trace for cascades; `EXPLAIN` extension is not planned |

### 5.3 The WS1 design

1. **Capability.** SmartEngine sets `HTON_SUPPORTS_FOREIGN_KEYS` only when `wesql_enforce_foreign_keys = ON` at server start (read-only variable). DDL then keeps the `FOREIGN KEY` clause instead of stripping it (PR 93's `strip_foreign_keys` is skipped).
2. **Checks are current reads.** InnoDB's checks read the latest committed parent, not the transaction's snapshot, and so must WS1's: a child insert under RR whose parent was created after the snapshot must succeed. The probe therefore takes the lock with snapshot validation off (§4.3), so a parent that changed after the snapshot is not a `1213` error. Which handler call carries that choice is the WS1 spike's first question.
3. **Locks.**
   - **Child insert or update of FK columns:** a shared lock on the matched parent's **primary-key row**, taken before the child row is written. If the FK references a unique secondary key, the probe resolves the key to the parent's primary key and locks that row, so the lock is always on the primary-key row (the row that a delete or an update of the referenced columns locks exclusively first).
   - **Parent delete, or update of referenced columns:** the exclusive lock on the parent row that the statement takes anyway, then a current-read scan of the child FK index prefix, then the referential action.
   - **Why that is enough.** Child insert and parent delete both go through the parent's primary-key row lock, so they serialize. Two parent deletes of different rows do not interact. Phantom children are impossible once the parent row is locked, because a new child must first take a shared lock on that row. No range lock is needed (there are none to take).
   - **What it does not give:** nothing here makes SmartEngine serializable for other predicates (§4.2); it makes the FK invariant hold.
4. **Timing.** Checks are immediate, per row (InnoDB semantics, not deferred to commit), so an application sees errors `1452` and `1451` at the offending statement, as MySQL clients expect.
5. **Referential actions** run as nested operations on the child table through the same handler wrappers, so the cascaded row changes are **ordinary row operations**: they take locks, run FK checks of their own, and appear in the binlog. Depth is limited to 15 as in InnoDB (`ER_FK_DEPTH_EXCEEDED`), with cycle detection through a per-statement set of `(table, primary key)`.
6. **The binlog contract (the part InnoDB does differently).** With InnoDB and row-based logging, cascaded changes are **not** logged as rows; a replica re-derives them by enforcing the same FKs. That breaks the binlog bridge (D154, D279): a consumer that sees only the logged rows would never see cascaded deletes, and its collection and Iceberg table would diverge from the database. So in WS1:
   - cascaded rows **are** logged as row events inside the parent statement's transaction;
   - **applier threads (replica, fork PR 3's replay, the WS3 replica) do not enforce or cascade**: they run with FK enforcement off, since the logged rows already contain everything. This is a deliberate difference from InnoDB. It means a WeSQL binlog applied by InnoDB would double-apply cascades; Loams does not do that (§23 §14, no mixed replication).
7. **DDL.**
   - `ADD FOREIGN KEY` on an existing table validates under a metadata lock with the check query above (a `COPY`-style path), failing with `1452`/`3780` as MySQL does. Concurrent DML is blocked for the duration, which is the cost of not having TiDB's online schema states (**estimate:** minutes for ten million rows; the deployment target is app metadata schemas, §5.6).
   - Dropping or renaming a referenced table or column, truncating a parent, and type changes of FK columns follow MySQL's existing dictionary rules (`3730`, `1553`, `3780`).
   - `foreign_key_checks = 0` skips checks and cascades for the session, for dumps and migrations.
8. **Bulk load.** `LOAD DATA` checks per row like any statement. Loads that need speed use `foreign_key_checks = 0` and a validating `ALTER` afterwards.

### 5.4 Risks specific to WS1

| Risk | Mitigation |
|---|---|
| The hook point in `handler.cc` misses a DML path (for example SmartEngine's bulk or blind-write fast paths, or `INSERT … SELECT` batching) | The test matrix in §11 WS1 includes every path; any engine path that bypasses `ha_write_row` is disabled for FK tables |
| Deadlocks between cascades and concurrent statements | Deadlock detection ON (T1), timeouts, and a stress test modelled on the ported `smartengine_deadlock_stress_*` tests |
| Snapshot conflict errors (`1213`) from RR first-committer-wins when two transactions touch one parent | The shared parent lock is a current read (§5.3, 2); child writes themselves still validate, as any SmartEngine write does. The test matrix shows the real rate under Forgejo's workload |
| Per-row probes are slow for bulk data | Per-statement cache of checked parent keys, as TiDB does |
| 8.0.46 is the base, and the SQL-layer foreign-key code changed between 8.0 minor releases | The hook is small and the patch is regenerated per base, as PRs 1–4 are |

### 5.5 Interim: `serverless_honor_innodb_engine`

PR 2 (merged) keeps an explicit `ENGINE=InnoDB` table on InnoDB, with real FKs, at the cost that InnoDB tables are copied whole into each snapshot and live on the local volume (PR 2's trade-off notes). It is the supported path for small relational metadata schemas until WS1, and after WS1 for anything WS1 does not cover. It does not help WS2: an InnoDB table is not on the bucket table by table.

### 5.6 Scope

WS1 is for **application metadata schemas** (Forgejo, Matomo-style apps, ORMs): thousands to millions of rows, modest write rates. It is not for bulk data with FK-heavy fan-out. That is the same scope PR 2 gave the InnoDB option.

## 6. WS2: durable commits in a Loams WAL quorum (D276, D277)

### 6.1 The gap

Q50: a commit after the last snapshot is lost when the local volume goes. Fork PR 3 fixed the crash case: recovery restores the last consistent snapshot, downloads the archived binlog and replays the window after the snapshot (`Consistent_recovery`, the temporary `wesql_snapshot_replay` channel). It cannot recover what was never uploaded. The binlog archive thread uploads slices about once a second (§23 §9.2), so on volume loss the most recent second or so of acknowledged commits is gone, and the spike lost a row written 20 s before the kill, which PR 3's analysis traced to the missing replay and not to the upload cadence. **After PR 3 the remaining loss window is the archive slice interval (estimate: up to a few seconds).** WS2 closes it to zero.

### 6.2 Which log is shipped (D276)

| Candidate | For | Against |
|---|---|---|
| **The binlog** | It is MySQL's commit order and its transaction-coordinator log; it is already the recovery input after PR 3 (the replay window is binlog); it is logical (ROW events), so replicas, the bridge (D154, D279) and the WS3 replica all consume it; transaction boundaries are explicit (GTID/Query-BEGIN … XID); a group commit already hands the log one contiguous batch | It is not the engine's redo; recovery replays SQL events, which is slower than redo |
| **SmartEngine's redo (its WAL)** | It is what the engine recovers from locally; physical, fast to apply | Engine-specific and tied to memtable and extent layout; unusable by replicas other than SmartEngine at the same version; carries no commit order across the SQL layer or DDL; nothing downstream can read it |

**The binlog is shipped.** SmartEngine's WAL stays local and keeps its job: process-crash recovery with the volume intact. In this mode `smartengine_flush_log_at_trx_commit` *can* be 0 or 2 (**estimate:** it removes one local fsync from the commit path), but that is an optimization behind its own test, not the default (§6.3, recovery of prepares). **WS2 runs with `gtid_mode = ON`** (with `enforce_gtid_consistency`), for the replay channel of fork PR 3 and for the WS3 and WS4 consumers. **`mysql.gtid_executed` is not the recovery marker.** That `gtid_executed` is on SmartEngine's supported system tables (`se_is_supported_system_table`) only says which engine stores the table; it does not say when rows are written. With binary logging on, MySQL 8.0 writes a transaction's GTID to the table at commit only if the engine persists it itself (`THD::se_persists_gtid()`, `sql/handler.cc` ~line 1577), and only InnoDB does (`Clone_persist_gtid`, `storage/innobase/clone/clone0repl.cc`); for every other engine the GTIDs reach the table when the binlog rotates or the server shuts down (`Gtid_state::save_gtids_of_last_binlog_into_table`; dev.mysql.com, "mysql.gtid_executed Table"). The fork's patch moves the table to SmartEngine and does not change that path. So after a power loss SmartEngine can hold a transaction whose GTID is not in the table, and replaying "every GTID not in `gtid_executed`" would apply it twice. WS2's marker is defined in §6.3.

### 6.3 The commit path

MySQL's ordered commit has a flush stage (write the transactions' binlog caches to the file), a sync stage (fsync the file, per `sync_binlog`) and a commit stage (engines commit). The engine's prepare (`se_prepare`) comes before all three, and `process_flush_stage_queue`, `sync_binlog_file` and `process_commit_stage_queue` are the functions in `mysql/sql/binlog.cc` (lines 8515, 8716, 8571).

```
 client COMMIT
   │ SE prepare (named transaction; local; not yet visible)
   ▼
 flush stage      leader writes the group's binlog caches to the local binlog file
   ▼
 sync stage       NEW: leader sends the group's bytes (whole transactions, contiguous) to the
                  quorum client; waits until a majority of acceptors has them durable
                  (optionally also fsyncs locally, sync_binlog=1, off by default in this mode)
   ▼
 commit stage     SE commit for every transaction in the group; ack to clients
```

- **One round trip per group, not per transaction.** The leader of the group already batches, so the RPC rate follows group commits.
- **The append unit is a flush-stage batch** of whole transactions. Acceptors therefore hold transaction-aligned prefixes.
- **LSN is a virtual stream offset**: the cumulative number of binlog bytes since the stream's origin, across files. The `(file, position)` of the MySQL world maps to it through the Rotate events that are part of the stream; the client keeps the map for each file's start.
- **Failure of the quorum:** the sync stage cannot complete. The group's transactions are prepared in the engine but not committed, and are not acknowledged. After a timeout the server goes read-only (`super_read_only = ON`) and reports the failure; it does not acknowledge a commit that is not on a majority.
- **Reconciliation at start.** Before serving, the server runs the equivalent of `sync-safekeepers`: it takes a new term (a vote round), learns the majority's highest flushed offset, and then:
  - **local binlog ahead of the quorum** (a crash between the local write and the quorum ack): the extra tail holds only transactions that are *prepared* in the engine and were never acknowledged. If it is the same lineage (the server holds the highest term), the tail is re-proposed; if another primary was elected meanwhile, the tail is truncated and MySQL's own crash recovery rolls the prepared transactions back, because they are not in the binlog;
  - **local binlog behind the quorum, or empty (volume lost):** the missing bytes come from the acceptors (and the bucket, below) into the replay window of fork PR 3, and `Consistent_recovery` applies them before the server opens for clients.
- **The applied-offset marker.** Each commit writes one extra key, `wesql_wal_applied = (term, end offset)` (the stream offset just past the transaction's XID event, with the term that appended it), into the transaction's own SmartEngine write batch, in a reserved system subtable, at `se_commit`. The marker and the data are therefore in one engine WAL record: after any crash, power loss included, both survive or neither does. WS2 requires `binlog_order_commits = ON`, so engine commits run in stream order and the engine WAL holds them in that order; a lost WAL tail drops a suffix of commits, never a commit in the middle. The committed engine state after a crash is therefore exactly the binlog prefix up to the surviving marker `M`. The marker travels in the engine snapshots, so a restore from the bucket brings its own `M`; it must agree with the binlog position PR 3 records for the snapshot, and recovery fails closed if they disagree. When WS2 is first enabled on a database, a fresh snapshot is taken and its position is the initial `M`.
- **Recovery uses the marker, not prepares and not `mysql.gtid_executed`.** In WS2 mode, XA recovery **rolls back every recovered engine prepare**, whether or not its XID is in the binlog, and the replay channel of fork PR 3 then applies every binlog transaction in `(M, quorum end]` before the server opens for clients. Rolling back is safe because a prepare is not visible and was not acknowledged unless it is on the quorum, in which case replay re-applies it from the binlog. `commit_by_xid` is not used: a lazily flushed engine WAL (`flush_log_at_trx_commit` 0 or 2) can keep a later transaction's prepare and lose an earlier one's (prepares are not in binlog order within a group), so committing recovered prepares would leave gaps below the highest committed offset. The rollback keeps the state an exact prefix. Replayed transactions commit through the same path and write the marker, so a crash during replay restarts from the new `M`: replay is idempotent at transaction granularity without `replica_exec_mode = IDEMPOTENT`, which would hide real errors. Recovered prepares whose XIDs are not on the quorum (the local tail above) are rolled back in the same step. WS2 moves the meaning of "in the binlog" from "in the local file" to "on a quorum".
- **Rejected alternative: make SmartEngine persist GTIDs at commit** as InnoDB does (`SE_GTID_PERSIST` and a GTID in the commit record). It is more patch in the SQL layer's GTID code, and a GTID set still has to be compared with the binlog transaction by transaction. One offset per commit says the same thing for an ordered stream.
- **Required acceptance tests (§11 WS2, 2b and 2c):** with `flush_log_at_trx_commit` 0, 1 and 2, a simulated power loss that discards the engine WAL's unsynced tail, including the case where **SmartEngine's data survives but `mysql.gtid_executed` lacks its GTIDs** (no binlog rotation and no clean shutdown since the commits). Recovery must lose no acknowledged row and apply no row twice.

### 6.4 The protocol boundary and licensing (D277)

| Side | What it is | License |
|---|---|---|
| **Acceptors** | `loams-wal` (`loams-safekeeper`, Arm A, §28 §7.2). Compio shards, a shared journal per shard, term and history metadata in TiKV (D268), offload to the bucket (D269) | Apache-2.0, a separate process |
| **Protocol** | The safekeeper v3 messages of §28 §6.9 (`ProposerGreeting`, `VoteRequest`, `ProposerElected`, `AppendRequest` and responses, `START_REPLICATION`), carrying a **timeline kind** (`pg-wal` or `mysql-binlog`) and, for `mysql-binlog`, a stream id derived from `SHA-256("wesql/" + ns + "/" + db)` | Specified in this repository (Apache-2.0); a wire format, not code |
| **Client** | A C++ module of the fork (`sql/wesql_wal_client.{h,cc}`): the proposer role for one stream, over TCP with TLS (OpenSSL, which MySQL already links), with a reader for the replica side (WS3) | **GPL-2.0-only**, written in the fork, as the rest of the tree |

Rules that keep D11 and D148 intact:

- **The client copies no Loams source.** It is written from the protocol specification. Apache-2.0 code cannot be combined into a GPL-2.0-only work (the FSF treats Apache-2.0 as compatible with GPL-3.0 and not with GPL-2.0), so nothing from `loams-safekeeper` is pasted or translated line by line into the fork. Where Neon's walproposer C code (PostgreSQL License, permissive and GPL-compatible) would help, it is read as a specification, not copied, to keep provenance simple (**verify** with the owner's counsel, Q271).
- **The acceptors do not link anything GPL.** They speak the protocol over a socket. A separate program communicating over a socket is not a combined work under the usual reading of the GPL (**verify**, Q271).
- **WeSQL is still never linked into Loams**, so Loams's code stays Apache-2.0 (D11). The fork's source is published as GPL-2.0 requires, from `ostrium-labs/wesql`.
- **Dependencies of the client** are limited to what the MySQL tree already carries (OpenSSL, zstd, protobuf if used), all GPL-2.0-compatible.

**Transport and authorization (required, not optional).** Term fencing orders writers; it does not say who may write. So:

- **Mutual TLS.** The client verifies the acceptor's certificate chain against a configured CA and **matches the endpoint's identity** (the configured name or SAN); it refuses the connection if either check fails. Acceptors require a client certificate. No plaintext mode, including on a cluster network (the rule §23 §6.3 sets for the MySQL hop).
- **Per-stream authorization.** The client certificate's identity (or a short-lived token bound to it) names the `(ns, db)` stream it may propose to. The acceptor checks that binding **before** it processes `START_WAL_PUSH`, votes or appends for that stream, and refuses other streams. The existing global `--auth-token` and `--trusted-network` options of `loams-wal` (§28 §7.2) do not bind a caller to a stream and are not enough for this kind (Q279).
- **Read side.** The replica and the bridge authenticate the same way, with read-only rights on the stream.
- **Credentials** come from the unified auth plan (Q30), rotate without a restart, and are never in the binlog or the metastore (§23 §6.2).

### 6.5 The bucket copy and trim

The acceptors offload committed bytes to the bucket (D269: `pgwal/…` objects for Postgres; for `mysql-binlog` a sibling prefix `mysqlbinlog/<ns>/<db>/<begin>-<end>.lwal` with the same segment conventions). Two logs then exist in the bucket: the acceptors' offload, and the fork's own binlog archive slices (`binlog_archive`). WS2 keeps the fork's archive (it feeds PR 3's recovery and the existing replica path) and lets the acceptors' offload serve as the **tail** between the last archived slice and the quorum's end. Trim follows D269's rule: a journal segment is recycled once every timeline in it has passed `min(backup_lsn, remote_consistent_lsn, commit_lsn)`, where for WeSQL `remote_consistent_lsn` is the fork's archive position, reported over the same feedback message the pageserver uses. Whether to keep two archives or let the acceptors' offload replace the fork's is Q272.

### 6.6 Cost (estimate)

The commit adds one quorum round trip to the path that today is a local binlog fsync. §28 §7.2's Arm A is designed to be at or better than the safekeepers' commit latency (which §28 §6.1 estimates at 1–3 ms p50 across AZs, an estimate, not a measurement); the Arm A gate runs are PRs #165 and #166, open when this was written, so this document treats Arm A's numbers as pending (verify). Arm B (TiKV on the commit path) failed the laptop gate in §28 §7.1 and is not an option here. **WS2's target is a p99 commit within the same budget as Loams Postgres** (the owner's < 5 ms p99 target, which is a server-hardware number, §28 §7.1). MySQL group commit amortizes the round trip across concurrent clients, so a single-client `INSERT` loop pays it in full and a 16-client one does not.

## 7. WS3: failover on the same quorum (D278)

Upstream removed Raft HA on 2026-08-22 (§23 §4.2); the single-node WeSQL has no leader election. WS3 rebuilds failover **without bringing consensus back into MySQL**: the quorum is the acceptors', the fencing is theirs, and MySQL stays single-writer.

### 7.1 Roles

| Role | What it is |
|---|---|
| **Primary** | One `mysqld`, writable, the proposer of the stream's current term |
| **Replica** | A `mysqld` in read-only mode that applies the binlog: from the snapshot and the bucket, then from the acceptors. It is a candidate for promotion |
| **Acceptors** | Three `loams-wal` processes (§28 §7.2). A majority fences and persists |
| **Control plane** | Loams: the `x/<ns>/<db>` record in the TiKV metastore (§23 §6.2) gains `primary` (node id, endpoint), `term` and `state`; changes are compare-and-set on its version; promotion runs as a Resonate saga (§21 §6.3) with deterministic step ids |

### 7.2 Fencing

- **Terms are the fence.** An acceptor refuses an append from a term lower than the one it has promised (§28 §7.2, D264). The term is made durable in the acceptor's TiKV metadata before the vote is acknowledged (D268). A deposed primary's next sync stage therefore fails, its group is not acknowledged, and it sets `super_read_only` and exits.
- **No acknowledged commit can be lost to a split brain.** A commit needs a majority at its term, and a new primary's election needs a majority at a higher term, so the two majorities intersect (the Paxos argument that walproposer already relies on, §28 §6.1).
- **Stale reads from a deposed primary** are not covered by terms. The primary keeps a **lease**: it sends a heartbeat (an empty append) every `T/3`, and if no majority has answered for `T` it sets itself read-only. The control plane waits `T` plus a margin after the old primary stops answering before it routes to a new one. `T` is a deployment setting (**estimate:** 5–10 s; failover time is bounded below by it unless the old primary is known dead).
- **The router follows the record.** Wire routing (§23 §6.3, D153) reads `x/<ns>/<db>`; after the compare-and-set the router sends new connections to the new primary and existing ones to the old one fail. *Proposed 2026-10-01 (D320, §31 §9.1):* where vtgate fronts WeSQL, the repoint is `vtctldclient TabletExternallyReparented <new-primary-tablet>` for the shard, beside the `x/` record change.

### 7.3 Promotion

1. **Decide.** The control plane marks the primary suspect (missed lease renewals in TiKV, a failed health check, an operator action) and picks the replica with the most applied offset.
2. **Fence and elect.** The candidate runs a vote round at term `n + 1` with a majority of acceptors and learns the highest flushed offset (the `sync-safekeepers` step of §6.3). From here the old primary cannot commit.
3. **Catch up.** The candidate applies the binlog up to that offset: what its relay log already holds, plus the tail read from the acceptors (`START_REPLICATION`). The replica read path queues events into the relay log with the function PR 3 already uses (`queue_event_from_objstore`).
4. **Open.** `read_only` and `super_read_only` go OFF, the status variable `Wesql_ready_for_write` (fork PR 4) turns ON, and the candidate starts proposing at term `n + 1`.
5. **Repoint.** The `x/` record changes `primary` and `term` by compare-and-set. The saga's steps are idempotent per term, so a crashed control plane re-runs from the record.

### 7.4 Replica bootstrap and steady state

- **Cold replica** (no local volume): restore the newest consistent snapshot from the bucket (the same code as PR 3's recovery), read the archived binlog slices after it, then switch to the acceptors' tail. Initialization from a clean bucket took 75–90 s in the spike (§23 §9.2, **spike**), with replay time on top (**estimate:** proportional to the binlog since the last snapshot; snapshot frequency is the lever).
- **Warm replica** keeps applying continuously, so promotion is election plus a short catch-up (**estimate:** seconds, bounded by the lease `T`).
- **Snapshots** continue to be taken by the primary only (`Consistent_archive`); replicas do not write to the bucket.
- **Replica reads** are eventually consistent. A client needing read-your-writes reads the primary. A consistency token (D76) carrying the binlog offset can let a replica wait until it has applied that offset (Q277).

### 7.5 What WS3 does not do

No multi-primary, no automatic split of read and write at the SQL layer (that is a router job, later), and no cross-region failover (acceptors are one region's AZs, as in §28).

## 8. WS4: analytics through Iceberg, not a StarRocks port (D279)

### 8.1 The bridge (extends D154)

D154 already specifies that the row binlog becomes `DocOp`s on a collection's implicit stream with an idempotent producer. WS4 adds the **Iceberg half** and says how the pieces connect:

```
 WeSQL ──binlog (ROW, FULL)──► bridge task ──► changelog stream per table (+I, -U, +U, -D; D21)
                                                  ├─► link ──► collection (search; D154's DocOps)
                                                  └─► link ──► keyed Iceberg table in Lakekeeper (RustFS)
                                                                 ▲                      ▲
                                                                 │                      │
                                                    StarRocks external catalog   Loams analytics (DataFusion)
                                                    (sidecar, reads in place)    Trino, DuckDB, Spark, …
```

- **Source.** Before WS2, the bridge reads from the primary as a replica (`COM_BINLOG_DUMP`, D154's N6). After WS2, it reads from the acceptors (the same `START_REPLICATION` as WS3's replica), so the bridge no longer loads the primary.
- **Sequence and exactly-once.** Producer id = (database, bridge), sequence = the virtual stream offset of §6.3 plus the ordinal inside the transaction. This is D154's `(file index, position)` made global, and it is the same contract as D129.
- **Mapping.** Each table maps to a keyed table by primary key. Insert, update and delete become changelog records; the link applies them as upserts and deletes to the Iceberg table with merge-on-read deletion vectors (§03, §08 §1). Cascaded deletes appear because WS1 logs them (§5.3, 6); without that property, this bridge would silently diverge, which is a reason to build WS1 before relying on the Iceberg copy for tables with FK actions.
- **DDL.** Binlog query events carry DDL. The bridge applies additive schema changes (new nullable column) to the Iceberg schema and halts the table's link on anything else, for the operator, as a changelog from Debezium-style tools usually does.
- **Initial copy, with an exact boundary.** `START TRANSACTION WITH CONSISTENT SNAPSHOT` alone does not say which binlog offset matches the snapshot: a commit can land between the snapshot and a later read of the position. The bridge therefore takes the boundary the way `mysqldump --single-transaction --source-data` does (dev.mysql.com, mysqldump): under a brief `FLUSH TABLES WITH READ LOCK` (which blocks commits through the global commit lock; verify that it blocks SmartEngine commits), it runs `START TRANSACTION WITH CONSISTENT SNAPSHOT` (REPEATABLE READ, the only level that takes a snapshot, §4.1), reads `SHOW MASTER STATUS` and `@@global.gtid_executed` (8.0.46 names; `SHOW BINARY LOG STATUS` is a later release), then `UNLOCK TABLES`. The copy reads the snapshot; streaming starts at the recorded position, mapped to the stream offset of §6.3, and the GTID set is the cross-check. Producer sequences drop any overlap. The WS4 test commits continuously during this handshake and compares checksums.
- **Freshness.** Seconds (**estimate:** the link tick plus an Iceberg commit), which is analytics freshness, not transactional.

### 8.2 The readers

- **Loams analytics** (§08) reads the table in place, with the hot tier of §04.
- **StarRocks**, if a deployment wants it, runs as a **sidecar**: `CREATE EXTERNAL CATALOG … iceberg` against Lakekeeper's REST catalog on RustFS, and queries the table in place (§3). Loams never links or embeds it. StarRocks is Apache-2.0 (GitHub license API), so the sidecar carries no license concern; whether the Iceberg catalog's deletion-vector reads match Loams's writes is a conformance test (**verify**, §11 WS4).
- Any other Iceberg engine reads the same table (§08 §8).

### 8.3 Why "implement all StarRocks features on WeSQL" is rejected

1. **Different engines.** StarRocks' speed comes from a vectorized C++ executor over its own columnar segments, with a cost-based optimizer and materialized views in a Java frontend. WeSQL's executor is MySQL's row-at-a-time executor over an LSM. Features such as vectorized joins, runtime filters, colocate groups and MPP exchange are not additions to MySQL's executor; they are another executor. That is a rewrite, not a port, in two languages and two licenses of code.
2. **Different storage.** StarRocks' features assume columnar segments with per-column indexes and delete vectors. SmartEngine's rows are LSM key-value pairs; the layouts do not match, and OLTP and OLAP access patterns pull the LSM's compaction in opposite directions.
3. **License.** WeSQL is GPL-2.0-only; StarRocks is Apache-2.0. Apache-2.0 code can be taken into a GPL-3.0 work, but not into a GPL-2.0-only one (§6.4). Porting StarRocks code into the fork would need either a separate Apache component (which is the sidecar again) or a re-implementation.
4. **Maintenance.** StarRocks ships frequent releases. A port would diverge from upstream on day one, and Loams would own the divergence, on top of the MySQL and SmartEngine forks already owned.
5. **The cheaper way exists and is already in the design.** Iceberg is the contract. OLTP writes stay in WeSQL, analytics reads come from an Iceberg table that StarRocks, Loams, DuckDB or Trino scan in place. That gives the owner the combination in the question (a row engine on the bucket and a columnar engine reading Iceberg on the bucket) with Loams writing the Iceberg half.

## 9. The alternative: TiDB on our TiKV (D280)

TiDB on Loams's TiKV already has what WS1–WS3 would add: **distributed transactions** (Percolator over TiKV), **foreign keys** (§5.2), and **HA** (TiKV's Raft and PD, TiDB servers are stateless). It speaks the MySQL protocol, and a TiCDC changefeed is the standard binlog-like bridge. Honestly stated:

| Question | WeSQL fork | TiDB on TiKV |
|---|---|---|
| Transactions | Single node, RC/RR (§4); SI with first-committer-wins; no SERIALIZABLE | Distributed, pessimistic or optimistic; SI; no real SERIALIZABLE (verify); scales out |
| Foreign keys | WS1 (to build); then enforced, with TiDB's design | Built in (v6.6.0, GA v8.5.0) |
| HA | WS2 + WS3 (to build) | Built in (Raft) |
| Scale-out writes | No (one writer) | Yes |
| **Where the data lives** | **Entirely on the bucket** (SmartEngine extents, snapshots, binlog), local disks are caches | **Local NVMe on TiKV nodes** in the self-hosted or classic form, replicated three ways; object storage only for backups. TiDB X is the object-storage form, in TiDB Cloud (§3), not verified as self-hostable |
| Cost shape | Storage at S3 price; compute for one primary and replicas | Three TiKV replicas on local disks (3× the data on NVMe) |
| Maturity | Beta, one vendor, long gaps in commits (§23 §4.2), plus a fork we own | A mature system under the Apache-2.0 license; Loams's TiKV is unmodified (D126) |
| Work for Loams | Four milestones (§11) | TiDB servers and operator pieces, and **a reversal of D260** |

**The rule (D280).** Choose WeSQL only if the requirement is *OLTP storage that lives entirely on the bucket*: the same property that D1 gives the retrieval engine, a stateless engine over S3 with disks as caches, for apps whose data is modest and whose operators want one durable place. Choose TiDB for anything that needs scale-out writes, more than one write node, distributed transactions across data, or mature HA, and accept that its data sits on TiKV disks.

**D260 is in the way, and this document does not remove it.** D260 (2026-09-29, owner) says "no TiDB anywhere in the engine or Loams cloud," and that it supersedes D123 (§23's "D123 is paused" is the weaker earlier statement). §23 §14 says WeSQL does not revive D123. If the owner wants TiDB for MySQL, that is a decision on D260 and Q260 (MySQL wire access), made there. WS1–WS4 are the cost of *not* reversing it for the one case where bucket-only storage is the requirement. The owner may reasonably conclude that the four milestones cost more than reversing D260 for a single use case; that is Q273. **Answered 2026-10-02 (D409): WS1–WS4 go ahead and D260 stands.**

## 10. Licensing (D277)

| Component | License | What the design does |
|---|---|---|
| WeSQL, SmartEngine (the fork) | **GPL-2.0-only** (`wesql/README.md` "Licensing": without the "any later version" clause); SmartEngine carries RocksDB's BSD notice | A separate process, never linked into Loams (D148, D11). Fork changes are published as GPL-2.0 requires, from `ostrium-labs/wesql` |
| The WAL client in the fork (§6.4) | GPL-2.0-only | Written in the fork. No Apache-2.0 Loams source copied in; OpenSSL, zstd and similar come from the MySQL tree |
| `loams-wal` acceptors (`loams-safekeeper`) | Apache-2.0 | A separate process that WeSQL reaches over a socket. Links nothing GPL |
| The protocol specification | Apache-2.0 (in this repository) | A wire format; both sides implement it independently |
| The bridge, links, Iceberg tables | Apache-2.0 (Loams) | Read WeSQL's binlog over MySQL's replication protocol, which is a protocol use, as D154 already does |
| StarRocks (sidecar) | Apache-2.0 | Unmodified, optional, a separate service |
| TiDB, TiKV (alternative) | Apache-2.0 | As in §20 |

D11 (no copyleft dependencies in Loams) and D148 (WeSQL is a separate process) are unchanged. The two questions that need a lawyer rather than an engineer are in Q271.

## 11. Milestones and acceptance tests

Each milestone is a small stack of PRs, in the fork (F) and in this repository (L). PR counts are estimates.

| Milestone | Scope | Depends on | Acceptance test (must pass to call it done) |
|---|---|---|---|
| **WS1: foreign keys** | F: `HTON_SUPPORTS_FOREIGN_KEYS` behind `wesql_enforce_foreign_keys`; `sql/wesql_fk.{h,cc}` and the handler-wrapper hook; shared-lock current-read parent checks; child probes; CASCADE, SET NULL, RESTRICT, NO ACTION; depth limit; cascaded rows logged and appliers not re-cascading; `ADD FOREIGN KEY` validation; `foreign_key_checks`. Spike first: a current read from `ha_smartengine` (§4.3). About 4–6 PRs | PR 3's fork baseline | (1) **The Forgejo migrations pass on WeSQL** (Forgejo 16.0.5 and the current release), from an empty database to the last migration, with `Error 1235` and the strip warning gone, and Forgejo's `make test-mysql` passes against the build. **First check Forgejo's minimum MySQL version: §23 §9.2 records that Forgejo documents 8.4+ while WeSQL is 8.0.x. If it is enforced, either the build is rebased to 8.4 or the version check is accepted as a documented deviation (Q274).** (2) The ported InnoDB foreign-key MTR suites (`innodb.foreign_key*`, replication variants) pass on SmartEngine. (3) A concurrency test: N sessions insert children of one parent while another deletes it, over 10 000 rounds; **no orphan** remains, and every failure is `1451`/`1452`/`1205`/`1213`. (4) A binlog test: a CASCADE delete is in the binlog as rows, and replaying the binlog with FK enforcement off reproduces the same table (hash comparison). (5) Deadlock stress with `deadlock_detect = ON`. (6) `ENGINE=InnoDB` honour option still works |
| **WS2: durability** | L: the `mysql-binlog` timeline kind in `loams-wal` (greeting, vote, elected, append, read, trim, offload prefix); the protocol specification; fault tests. F: `sql/wesql_wal_client.{h,cc}`; the sync-stage hook; start-time reconciliation; PR 3's replay extended to read the tail from the acceptors; read-only on quorum loss. About 5–7 PRs | Arm A gate passed (§28 §7.2, PRs #165/#166); WS1 not required | (1) **Volume loss survives with nothing committed lost:** a client commits rows in a loop and records each acknowledged row id; `kill -9` the primary, **delete its volume** (data directory and binlog), start a new `mysqld` against the same bucket and acceptors; after recovery **every acknowledged row is present**, with no extra rows beyond those that were in flight; repeated 200 times with kills at random points (before the quorum ack, after it, during the engine commit, during a snapshot). (2) Kill one acceptor: commits continue. Kill two: commits stall and are **not acknowledged**; they resume when one returns. (2b) With `smartengine_flush_log_at_trx_commit` = 0, 1 and 2, a simulated power loss (the engine WAL's unsynced tail discarded at random points, with and without then deleting the volume) loses no acknowledged row and applies no row twice; the recovered marker `M` (§6.3) equals the end offset of the last transaction visible in the engine, and every recovered prepare was rolled back and re-applied by replay or was not on the quorum. (2c) **Data survives, GTID marker does not:** commits with no binlog rotation and no clean shutdown since them, then power loss with the engine WAL intact; check first that `mysql.gtid_executed` lacks those GTIDs while SmartEngine holds the rows (the failure the marker exists for), then that recovery applies none of them twice. A crash during replay, repeated, converges to the same state. (3) The primary's local binlog is ahead of the quorum at the crash: after restart the unacknowledged tail is either re-proposed or truncated and no transaction is half-applied (checked with XA recovery logs). (4) Partition the primary from two acceptors: it stops acknowledging and goes read-only. (5) p50 and p99 commit latency, `commit-1` and `commit-16`-style runs, recorded against the same hardware as §28's gate (a report, with the budget of §6.6 as the target) |
| **WS3: HA** | L: `x/` record fields for primary and term; the promotion saga; router follows the record; lease heartbeats. F: replica mode that reads the acceptors' tail; fencing on lower terms; lease self-fence. About 4–5 PRs | WS2 | (1) **Failover after killing the primary under load:** a new primary is writable within the target (**estimate:** warm replica < 15 s with `T` = 5 s; the real number is the test's output) and **no acknowledged commit is lost**. (2) The deposed primary, resumed after a pause longer than `T`, cannot acknowledge any commit and stops serving. (3) A cold replica built from an empty volume catches up from the snapshot, the archive and the tail, and reaches the same checksum as the primary. (4) Two simultaneous promotion attempts: exactly one wins (compare-and-set and terms). (5) A crashed control plane mid-promotion resumes and finishes with one primary |
| **WS4: analytics** | L: the binlog-to-changelog bridge reading `START_REPLICATION`; links to a keyed Iceberg table in Lakekeeper; schema handling; the initial snapshot copy. No fork change unless the bridge needs one. About 3–4 PRs | WS2 for the acceptor source; WS1 for FK cascades to be complete; D154's N6 before that | (1) **The Iceberg table equals the MySQL table** (row-for-row checksum) after a mixed workload of inserts, updates, deletes and CASCADE deletes, with kills of the bridge between append and confirm (exactly-once, as D129's fault set). (2) StarRocks (a pinned release, as a sidecar) reads the table through an external Iceberg catalog on Lakekeeper and returns the same checksum, including after deletes. (3) Loams analytics returns the same checksum. (3b) Commits that race with the snapshot handshake are neither lost nor duplicated (a continuous writer during 100 initial copies; checksums equal). (4) A DDL adding a nullable column propagates; an incompatible change halts the table's link with an alert. (5) Bridge lag under a steady write rate is reported (the **estimate** in §8.1 is replaced by a number) |

**Ordering.** WS1 has no dependency on the Loams side and can start now. WS2 waits for the Arm A gate. WS3 follows WS2. WS4's first half can start from D154's N6 before WS2.

## 12. Risks

| # | Risk | Mitigation |
|---|---|---|
| 1 | **Four milestones in a fork of an unmaintained upstream** (§23 §4.2): Loams owns MySQL, SmartEngine and the patches | Each PR is small and regenerates the patch against a pinned base (PRs 1–4's method); D280 gives an exit to TiDB; nothing here is posted upstream |
| 2 | **WS1's hook misses a DML path** and a constraint silently fails | The test matrix (§11 WS1), and a startup self-test that creates FK tables and tries each path |
| 3 | **Snapshot isolation surprises apps** with `1213` where InnoDB would not fail | Documented (§4); the FK probe is a current read; T2 savepoints and a retry guide in the deployment notes |
| 4 | **Commit latency** from the quorum round trip is too high on cheap disks | WS2's acceptance test records it; group commit amortizes; acceptors on PLP NVMe (§28 §6.6) |
| 5 | **Two archives** (fork binlog slices and acceptors' offload) diverge | One position map, tests in WS2, Q272 |
| 6 | **The GPL client's license boundary is wrong** | Q271 before WS2; no Loams code in the fork; an interface-only protocol |
| 7 | **Replay is slow** for a long window after a snapshot | Snapshot cadence, measured in WS3; this is the lever PR 3's note flagged |
| 8 | **A stale lease** lets a deposed primary serve reads | The lease `T`, the router wait, and a documented bound (§7.2) |
| 9 | **The owner decides D260 is wrong** and the milestones are wasted | They are small, and WS4's Iceberg bridge works for any MySQL source. WS1 and WS3 are the part that would be wasted |
| 10 | **Recovery applies a transaction twice or skips one** because its marker is not atomic with its data | The marker is in the transaction's own engine write batch, not `mysql.gtid_executed` (§6.2, §6.3); recovered prepares are rolled back and replayed; tests 2b and 2c in §11 WS2 |

## 13. Open questions

| # | Question | Needed by |
|---|---|---|
| Q271 | **Legal review of the licensing boundary** (§6.4): is a GPL-2.0-only client in `mysqld` that speaks a documented protocol to an Apache-2.0 server a combined work under the GPL (we say no), and is writing the client from the specification, without copying Loams's Apache-2.0 code, enough? **Owner action pending (2026-10-02):** counsel reviews the GPL-2.0-only client / Apache-2.0 acceptor boundary (§29 §6.4, §10) | Owner action; owner and counsel, before the WS2 plan |
| Q272 | ~~One archive or two (§6.5): keep the fork's `binlog_archive` slices beside the acceptors' offload, or let the offload replace it and have recovery read only the acceptors' objects~~ Answered 2026-10-02 by the owner: the recommended default — two archives: the fork keeps its `binlog_archive` slices for recovery and replicas, the acceptors' offload is the tail, and one position map ties them (§29 §6.5) | Resolved |
| Q273 | ~~Given §9, do WS1–WS4 beat reversing D260 for the bucket-only requirement? (A decision, not a question for engineering.)~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — WS1–WS4 go ahead and D260 stands, no TiDB (D409); why: it is the direction the owner approved for §29, §31 builds on WeSQL (D301, D320), and bucket-only storage is D1's property | Resolved |
| Q274 | ~~Forgejo's minimum MySQL version versus WeSQL's 8.0.x base (§11 WS1): rebase to 8.4, or accept the documented deviation~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — accept the documented deviation for WS1 and rebase WeSQL to MySQL 8.4 once, before Vitess v24's support ends (about 2027-04), which also answers Q302 (D410); why: one rebase serves Forgejo and Vitess v25+ | Resolved |
| Q275 | ~~FK enforcement in the handler wrappers (D275) versus in the engine's own handler: the spike result on whether `ha_smartengine` can serve a current-read probe by key decides it~~ Answered 2026-10-02 by the owner: the recommended default — the engine-independent handler wrappers (D275); the WS1 spike only confirms that `ha_smartengine` serves a current-read probe by key, and reopens this if it cannot (§29 §5) | Resolved |
| Q276 | ~~Implement `ROLLBACK TO SAVEPOINT` with writes (T2) before or after WS1, given ORM usage~~ Answered 2026-10-02 by the owner: the recommended default — after WS1, as its first follow-up item (§29 §4.3) | Resolved |
| Q277 | ~~Consistency tokens for replica reads (§7.4): carry the binlog offset in D76's token~~ Answered 2026-10-02 by the owner: the recommended default — yes, D76's consistency token carries the binlog offset and a replica waits until it has applied it (§29 §7.4) | Resolved |
| Q278 | ~~Timeline kind encoding in the safekeeper v3 greeting: reuse and extend the greeting (compatible with the pageserver path), or a separate listener port for non-Postgres kinds~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — extend the safekeeper v3 greeting with a timeline kind, one listener; why: one acceptor protocol and port, compatible with the pageserver path | Resolved |
| Q279 | ~~Stream-scoped authentication for `loams-wal`'s `mysql-binlog` kind (§6.4): client certificates with the stream in the SAN, or tokens minted by the control plane. The Postgres path's global token stays as it is~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — client certificates with the `(ns, db)` stream in the SAN, issued by cert-manager; why: mTLS also authenticates the acceptor, `mysqld` already speaks TLS, and nothing new is built in the GPL client | Resolved |

## 14. Contradictions with earlier decisions, and how they are resolved

| Earlier | Conflict | Resolution |
|---|---|---|
| D156 (§23): WeSQL gated and lower priority; dev compose only | D273 | **Proposed amendment**: D156 and the Q50 gate stand until WS1 and WS2 pass their acceptance tests; then WeSQL may serve the apps it fits. WS2's design is the proposed answer to Q50, not its closure; Q51 (Matomo) is unchanged |
| D148 (§23): separate, unmodified services | The fork is modified | Unchanged in the sense D148 uses it: **Loams** never links or modifies it; the fork is a separate process, as §23 §7 says for "a patched WeSQL (which Loams does not plan)", now planned and GPL-compliant. §23 §7's sentence is updated by D273 |
| D154 (§23): binlog bridge into collections | WS4 adds Iceberg | **Extended**: the same bridge, one more link target |
| D11: no copyleft dependencies | A GPL client in the fork | The client is part of the GPL fork, which Loams does not link (D148, §10) |
| D260: no TiDB anywhere | D280's alternative | Not reversed here; §9 states the cost and Q273 asks the owner |
| D123, D-SC-14, §23 §14 | TiDB for MySQL | D123 is already superseded by D260. D-SC-14 (Matomo on MariaDB) stands until WS1–WS2 |
| D2 / D130: OLTP is out of scope for the retrieval engine | WeSQL is OLTP | As in §23 §14: a separate service. D130's exception for TiKV-backed OLTP does not cover WeSQL; WeSQL's own durability is WS2's quorum |
| D264 (§28): walproposer is the only sequencer | WeSQL's primary is a proposer too | The same Paxos shape, with a different client and timeline kind (D277). D264 is not changed for Postgres |
| D1: the bucket is the only durable source of truth | The quorum (local NVMe) holds the tail | Same as §28's Arm A: the acceptors' NVMe holds only the window not yet offloaded, as the safekeepers' disks do |

## 15. Sources

Read on 2026-10-01.

- **WeSQL fork** (`ostrium-labs/wesql`, branch `8.0`, `eef34f452`): `README.md`; `storage/smartengine/handler/{ha_smartengine.cc, se_hton.cc, se_hton.h}`; `storage/smartengine/plugin/{se_plugin.cc, se_system_vars.cc}`; `storage/smartengine/transaction/{se_transaction.cc, se_transaction_impl.cc}`; `storage/smartengine/core/transactions/{transaction_impl.cc, transaction_lock_mgr.h}`; `storage/smartengine/core/LICENSE`; `sql/wesql_ddl_compat.cc`; `docs/mysql-8.0.46-ddl-compat-design.md`; `patches/mysql-server-8.0.46.patch`; `mysql-test/suite/smartengine/` (`r/unsupported_tx_isolations.result`, `r/lock_rows_not_exist.result`, `r/rollback_savepoint.result`, `r/gap_lock_issue254.result`, `disabled_smartengine.def`, `include/locking_issues_case4.inc`); merged fork PRs 1–4 (`gh pr list -R ostrium-labs/wesql --state merged`): #1 path-style, #2 honour InnoDB, #3 replay after the snapshot, #4 readiness.
- **MySQL** (`mysql-8.0.46`, `976e5da9`): `sql/binlog.cc` (`ordered_commit`, `process_flush_stage_queue`, `sync_binlog_file`, `process_commit_stage_queue`), `sql/sql_base.cc` (`process_table_fks`), `sql/sql_table.cc` (`HTON_SUPPORTS_FOREIGN_KEYS`).
- **TiDB** (`pingcap/tidb` at `8936d7b`): `docs/design/2022-06-22-foreign-key.md`; `pkg/executor/foreign_key.go` (`FKCheckExec`, `FKCascadeExec`); `pkg/meta/model/table.go` (`FKInfo`, `ReferredFKInfo`); docs.pingcap.com/tidb/stable/foreign-key/, /tidb/stable/tidb-architecture/, /tidbcloud/tidb-x-architecture/.
- **StarRocks:** docs.starrocks.io (Architecture; Primary Key table; Catalog overview and the Iceberg catalog page, found by search); `StarRocks/starrocks` (`fe/`, `be/src/storage/{kv_store.h, tablet_meta_manager.h, persistent_index.cpp, persistent_index.h, local_primary_key_recover.h}`; GitHub license and languages APIs).
- **Loams:** §23 (D148–D157, Q50, Q51, §4.2, §9.2); §28 §6.1, §6.9, §7.1, §7.2 (D263–D271, Q261–Q264); §08, §09, §03, §21 §6.3, §20 §12 (D129); D1, D2, D11, D21, D76, D126, D130, D260; Q260.
