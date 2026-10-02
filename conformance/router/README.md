# Router compatibility inventory

What a router asks of the engine behind it, recorded as data (design
[§31](../../docs/design/31-loam-router-and-verification.md) §15, RT0 plan Tasks 5 and 6, D309). Two halves:

| Files | Router | Reference engine | Target |
|---|---|---|---|
| `pgdog-loampg-statements.tsv`, `pgdog-loampg-suites.tsv` | PgDog v0.1.60 | Postgres 17.11 | a Loams Postgres compute (P2b): **`pending-target`** |
| `vitess-wesql-statements.tsv`, `vitess-wesql-suites.tsv` | Vitess v24.0.4, unmanaged vttablets | MySQL 8.0.46 | WeSQL `8.0.35-0.1.0_beta5.40` (the published image) |

The capture scripts, the pins and the method of each step are in
[`scripts/router/inventory/`](../../scripts/router/inventory/README.md); `compat-replay`
([`crates/loams-compat`](../../crates/loams-compat)) does the replay and the classification.

## The method

1. **Static extraction** of the statements the router sends, from its source. For PgDog the output is `source_path:line`
   and a statement kind, never the line's text (PgDog is AGPL-3.0, D318).
2. **Dynamic capture**: the router's own integration scenarios, run from a pinned checkout against the reference engine
   with `pg_stat_statements` and replication-command logging.
3. **Merge** into one table keyed by digest, with the component that issues each statement.
4. **Replay and classify** each digest with its captured example and session state on the reference and on the target.
5. **Suite results**: the scenarios, with a result per suite and the number of inventory rows each touches.
6. **Bless** the tables as the TSVs here. A PR that changes a pinned version re-runs the inventory and shows the diff.

No PgDog code or text is in these files (`scripts/spec/provenance.sh` scans this directory in CI). Vitess is Apache-2.0,
so the MySQL half records statement text with its source path.

## `…-statements.tsv`

One row per digest. Fields escape tab, newline and backslash as `\t`, `\n`, `\\`, so every row is one line. Rows are
sorted by component, then digest.

| Column | Meaning |
|---|---|
| `digest` | First 16 hex digits of the SHA-256 of the statement's normalized text (lower-cased, whitespace collapsed; for MySQL also literals and bind variables replaced by `?`). |
| `component` | Postgres: `pool`, `health`, `schema-sync`, `copy`, `replication`, `2pc`, `query`. MySQL: `query`, `health`, `schema-engine`, `sidecar`, `vreplication`, `vdiff`, `2pc`, `onlineddl`, `reparent`. |
| `source` | `source_path:line` of the static scan (up to five, then `+N more`), and `dynamic:<suite>` for each scenario that sent it; notes in parentheses say how many similar statements a cap folded into the row. |
| `example` | The statement replayed. ` ;; ` separates commands sent one at a time on one connection. |
| `class` | `same` (identical result, tags, warnings or error code), `differs` (a result or its metadata differs), `error` (the target errors where the reference does not), `unsupported` (the target refuses by design; the note says why), `pending-target` (the target is not runnable; the reference result is recorded). |
| `ref_hash`, `target_hash` | The canonical result hashes: SHA-256 over column names, rows (sorted unless the statement has `ORDER BY`), command tags and warnings, or over the error code. `target_hash` is empty for `pending-target`. |
| `note` | What the replay saw: the error on either side, `volatile on the reference: compared by shape`, `replayed in an empty database`, and for `unsupported` the reason. |
| `issue` | A tracking reference, or the design's `C-n` item for the MySQL rows that carry one. |

`…-suites.tsv` has the columns `suite, test, result, rows`: `result` is `pass`, `fail`, `skip` or `pending-target`; `rows`
is the number of statement rows whose `source` names the suite. A suite that ran on the reference only reads
`[reference: <result>]` in `test` and `pending-target` in `result`.

## The gate (§31 §15 step 7)

RT3's gate for D302 (proposed): no `error` or `differs` row in the components Loams uses (query service, health, schema
engine, VReplication for MoveTables and Reshard), and the suite pass rates of §17's RT3 row. Rows in other components
are recorded, not gating.

## Postgres half, as built (2026-10-02)

**Pins:** `postgres:17.11`; PgDog v0.1.60 (`ghcr.io/pgdogdev/pgdog@sha256:25d19088…2f266`, source checkout at tag
`v0.1.60`, commit `0d040f92`); `pg_dump`/`psql` 18.6 on the host for the schema dumps and replication commands.

**Target:** the Loams Postgres compute from `deploy/neon` is not runnable (P2b has not merged), so every row is
`pending-target` (Ruling 7) and the reference result is recorded. RT1 Task 0 re-runs it with
`pg-replay.sh <capture-dir> postgres://…` once P2b is merged.

**Rows: 548, all `pending-target`.** What the reference did with them:

| Component | Rows | Reference ran | Reference error | Volatile (compared by shape) |
|---|---|---|---|---|
| `query` | 151 | 98 | 47 | 6 |
| `schema-sync` | 290 | 232 | 58 | 0 |
| `copy` | 54 | 24 | 30 | 0 |
| `replication` | 17 | 13 | 0 | 4 |
| `pool` | 21 | 21 | 0 | 0 |
| `2pc` | 10 | 8 | 2 | 0 |
| `health` | 5 | 5 | 0 | 0 |
| **Total** | **548** | **401** | **137** | **10** |

"Reference error" rows compare error codes on a target (replay runs on empty tables, so `NOT NULL` violations on NULL
parameters, missing `copy_test_types`, binary `COPY` with no header and `COPY FREEZE` outside the creating
transaction are expected); the codes are in each row's note.

**Scenarios** (suites table): `baseline`, `pgbench`, `schema_sync`, `data_sync` and `two_pc` pass on the reference.
`resharding` fails by timeout: PgDog's own scenario gives itself 16 minutes and the catch-up did not finish in 20 under
the pgbench load on this machine; the statements it sent before that are in the table. `rewrite`, `logical` and
`failover` are `skip` (configuration only; a dataset download; interactive). Rows per suite: `schema_sync` 266,
`data_sync` 170, `resharding` 129, `pgbench` 57, `baseline` 32, `two_pc` 14.

**Static scan:** 648 `path:line<TAB>kind` hits over 5 path groups. Kinds with a replayable statement became
canonical rows (`pg-static-kinds.tsv`: replication commands, 2PC commands, health queries, catalog queries). The
generic verbs (`SELECT` 126, `CREATE` 93, `DROP` 49, `BEGIN` 47, `UPDATE` 36, `INSERT` 34, …) and nine `pg_dump`
invocations have no single statement to replay; the dynamic capture covers them.

**What was left out on purpose:** the definitions PgDog installs in its own `pgdog` schema, the function, procedure and
trigger bodies of the scenarios' fixtures (588 statements; one canonical row stands for them), the replicated-schema DDL
beyond three statements per kind and scenario (1 542 folded), and workload statements beyond twelve per kind and scenario
(59 folded). Comments are stripped from every example. The scripts' README explains each rule.

**Limits of the replay:** schemas only (no rows); `$n` statements run with NULL parameters and compare types and
counts; `START_REPLICATION` is judged by the server accepting it. `pg_stat_statements` normalizes constants, so an
example is a statement the router sent, not necessarily the one with the most telling literals.

## MySQL half, as built (2026-10-02)

**Pins:** `mysql:8.0.46`; Vitess `vitess/lite:v24.0.4` (revision `967ff0b9`, 2026-10-01) with unmanaged vttablets, vtgate and
vtctld; `etcd:v3.7.2`; WeSQL `apecloud/wesql-server:8.0.35-0.1.0_beta5.40` on RustFS 1.0.0. The fork build with
`serverless_honor_innodb_engine` (commit `eef34f452`) was **not** built: `deploy/wesql/` runs the published image, and building a MySQL fork
is hours, not a step of an inventory. Both engines ran with `gtid_mode = ON`, ROW and FULL binlog; MySQL also with the semi-sync source plugin.

**Rows: 1 981** (356 from the static scan, 1 699 captured; 74 are both): `same` 1 822, `differs` 158, `error` 0, `unsupported` 1.

| Component | Rows | same | differs | error | unsupported |
|---|---|---|---|---|---|
| `query` | 1 263 | 1 262 | 1 | 0 | 0 |
| `vreplication` | 193 | 172 | 21 | 0 | 0 |
| `schema-engine` | 145 | 75 | 70 | 0 | 0 |
| `onlineddl` | 108 | 102 | 6 | 0 | 0 |
| `health` | 64 | 60 | 4 | 0 | 0 |
| `sidecar` | 57 | 28 | 29 | 0 | 0 |
| `vdiff` | 56 | 45 | 11 | 0 | 0 |
| `2pc` | 46 | 30 | 16 | 0 | 0 |
| `reparent` | 49 | 48 | 0 | 0 | 1 |

Of the 158 `differs`, 104 are `SHOW CREATE TABLE` (WeSQL prints `ENGINE=SMARTENGINE`) and 42 are `CREATE TABLE … ENGINE=InnoDB`
whose warnings differ (WeSQL substitutes SmartEngine and warns): C-1. The rest are `SHOW STATUS LIKE 'Rpl_semi_sync_source_status'`
(no row on WeSQL: C-2), `sql_mode` defaults, and a few schema-listing counts. Replay runs on schemas only, so the 1 262 `same`
`query` rows compare how the engines answer (parse, plan, errors, types), not what data says. Static statements with Go format verbs
or bind variables cannot run as written and compare by error code. Gate (§15 step 7): no `error` row and no `differs` row in
`query`, `vreplication`'s data path, `health` beyond `sql_mode`; the `differs` in the components Loams uses (`schema-engine`, `sidecar`,
`vreplication`) are all `SHOW CREATE TABLE` or CREATE-warning rows for the sidecar and vreplication tables, that is C-1, which the owner
has to rule on (Q304).

**Suites** (`vitess-wesql-suites.tsv`, the WeSQL run; the reference's result is in each test name): 28 steps, 24 pass, 3 fail, 1 skip.
On the reference (`mysql:8.0.46`) 26 pass and 1 skips. The 3 WeSQL failures: the first `TabletExternallyReparented` (C-2), `SERIALIZABLE`
through vtgate (C-4), and the two-phase-commit transaction (C-2). MoveTables (create, VDiff, SwitchTraffic, ReverseTraffic, SwitchTraffic,
Complete), Reshard `0 -> -80,80-` with VDiff, and an Online DDL `ALTER` all pass on WeSQL.

### C-1 to C-7 observed

| # | Observation |
|---|---|
| C-1 | Vitess's sidecar tables (`_vt`, `_vt_customer`; 40 per backend) are **SmartEngine** on WeSQL, `ENGINE = InnoDB` on MySQL. The published image has **no `serverless_honor_innodb_engine`** variable, so the "on" case could not be run. **vttablet issues no `ALTER TABLE … ENGINE` on its restart**: after restarting `commerce/0`, the only `ALTER TABLE _vt…` digests are `AUTO_INCREMENT` ones, the same as on MySQL, and the engines are unchanged. vttablet starts, serves and runs VReplication with its sidecar tables in SmartEngine (they live in the bucket). Rows carry `C-1`. |
| C-2 | Three findings. (a) WeSQL has **no semi-sync plugins**: `TabletExternallyReparented` under durability policy `semi_sync` fails with `VT09013: semi-sync plugins are not loaded`; the keyspaces need policy `none`. (b) Vitess prepares a 2PC transaction **only on a Unix socket** to MySQL (`dt_executor.go`: "We can only prepare on a Unix socket connection"); on a TCP connection (`--db-host`) it is refused with `VT10002`. An unmanaged vttablet therefore has to run next to its MySQL with a shared socket, which affects how D302's "one vttablet per WeSQL primary" is deployed. (c) With the socket, 2PC works on MySQL 8.0.46 even though `Rpl_semi_sync_source_status` is OFF (no replica); on WeSQL the transaction fails with `Deadlock found when trying to get lock` at `start_commit`, and in an earlier order of the steps the same transaction stayed prepared and an `ALTER` on that table blocked for 1 311 s. 2PC on WeSQL is therefore not usable as is. |
| C-3 | Vitess v24.0.4 ran against MySQL 8.0.46 (every step passes). The support end is a release-calendar fact, not testable here. |
| C-4 | WeSQL refuses `SERIALIZABLE` through vtgate (`SE only supports READ COMMITTED and REPEATABLE READ`); RC and RR pass. Pooled connections and vtgate's `SET` handling otherwise worked. One row is `unsupported` by rule; the isolation statements themselves run in the suites table. |
| C-5 | `CREATE TEMPORARY TABLE` works through vtgate (reserved connection) and directly on WeSQL (beta5.40); VReplication and Online DDL ran without a temporary-table failure. |
| C-6 | The `performance_schema`, `information_schema` and `SHOW` statements vttablet runs: 166+ rows tagged `C-6`; the differences are the `SHOW CREATE TABLE` ones above. |
| C-7 | VStream-based workflows (MoveTables, Reshard, VDiff) pass against WeSQL's binlog (ROW, FULL, GTID on). `SHOW BINARY LOGS`, `SHOW MASTER STATUS` and `gtid_executed` rows compare by column names (instance identity); `RESET MASTER` is refused (`Consensus Replication is enabled`) and counted `unsupported`. |

**What was adapted:** `vtgate` could not start while three overlapping shards all claimed to serve, so the `-80` and `80-` tablets start just before the Reshard; every
primary of a keyspace has its own backend because the sidecar database is per backend; table names `shopper` and `corder` replace `customer`, which the
planbuilder test schema already defines. MariaDB's flavor file is not scanned. `PlannedReparentShard` is `skip` (unmanaged tablets refuse it).
