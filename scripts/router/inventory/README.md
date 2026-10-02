# Compatibility inventory: capture scripts

The scripts that build the inputs of `crates/loams-compat` (`compat-replay`) for the router inventory
(design [§31](../../../docs/design/31-loam-router-and-verification.md) §15, RT0 plan Tasks 5 and 6). This directory
holds the **Postgres half** (PgDog v0.1.60 in front of Postgres 17.11) and the **MySQL half** (Vitess v24.0.4 in front
of MySQL 8.0.46 and WeSQL), the latter from `vitess-*`, `vt-*` and `compose.vitess.yml`. The blessed output lives in
[`conformance/router/`](../../../conformance/router/README.md).

## Rules

- **No PgDog code or text in this repository** (AGPL-3.0, D236, D318).
  - `pg-static.sh` prints `path:line<TAB>kind`, never the line.
  - `pg-static-kinds.tsv` holds example statements written for Loams.
  - PgDog's integration scenarios are run from the pinned checkout, never copied.
  - `pg-merge.py` drops the definitions PgDog installs in its own `pgdog` schema and every function, procedure or
    trigger body a scenario's fixtures define, strips SQL comments, and caps what a scenario's own workload and its
    replicated schema contribute (see "What the merge keeps").
  - `scripts/spec/provenance.sh` (run by CI) scans `conformance/`.
- **Vitess is Apache-2.0**, so `vitess-static.sh` records statement text with its source path.
- **Pinned checkouts live outside the repository**: `$HOME/.cache/loam/pgdog-v0.1.60` (`PGDOG_SRC`) and
  `$HOME/.cache/loam/vitess-v24.0.4` (`VITESS_SRC`), clones at the tags.
- **Containers use podman or docker.** With podman the scripts use `docker-compose` over podman's socket
  (`DOCKER_HOST=unix:///run/user/$UID/podman/podman.sock`, set by the scripts).
- **Machine limits** (15 GB of RAM, a small tmpfs `/tmp`): stop every container before a cargo build, never run the
  Postgres stack next to the Vitess stack, keep all output under `$HOME/.cache/loam/inventory/`. The
  scripts stop when `$HOME` has under 8 GB free.

## Image pins

| Image | Pin |
|---|---|
| Postgres | `docker.io/library/postgres:17.11` |
| PgDog | `ghcr.io/pgdogdev/pgdog@sha256:25d1908886f595a2e3b00ec59783326712e3c0f75f91d29f032ff712b30f2266`: the v0.1.60 image index (its amd64 manifest is `sha256:1bf5346dbeecdaea832a48e477294c66a2f284aba34863270b3aa96392191589`; the `org.opencontainers.image.revision` label is `0d040f92`, the v0.1.60 tag) |
| MySQL | `docker.io/library/mysql:8.0.46` (`sha256:62fb722c…2497b`) |
| Vitess | `docker.io/vitess/lite:v24.0.4` (`sha256:4b1f89a6…7b97`; vtctld, vtgate, vttablet, vtctldclient; Go 1.26.8, revision `967ff0b9`) |
| etcd | `quay.io/coreos/etcd:v3.7.2` (`sha256:3b705ec7…f543`) |
| WeSQL | `docker.io/apecloud/wesql-server:8.0.35-0.1.0_beta5.40` (`sha256:90d4c9e3…d4fc`), the image `deploy/wesql/` runs, on `docker.io/rustfs/rustfs:1.0.0`. The fork build (commit `eef34f452`, `serverless_honor_innodb_engine`) was not built here |


`pg-capture.sh` takes the PgDog binary out of that image (`podman create` and `cp`) because the two-phase-commit
scenario kills PgDog, which a `podman run` wrapper cannot do. The bits are the image's; `shims/pgdog` runs the image
itself when the extraction fails.

## Files

| File | Role |
|---|---|
| `pg-static.sh` | Static half: `source_path:line<TAB>statement-kind` for PgDog's `backend/{replication,schema,pool}/`, `frontend/client/query_engine/two_pc/` and `healthcheck.rs`. |
| `pg-static-kinds.tsv` | A canonical example, written for Loams, for each static kind that has a replayable statement (`kind`, `component`, `example`). Kinds without one (the generic verbs, `pg_dump` invocations) are listed in `static-unmapped.tsv` by `pg-merge.py`. |
| `compose.pg.yml` | `shard0` (127.0.0.1:5432, where PgDog's `integration/` layout expects a server), `shard1` (15433), `ref` (15440), all `postgres:17.11` with `pg_stat_statements`, `log_statement = all`, `log_replication_commands`, `max_prepared_transactions`, `wal_level = logical`; under the profile `baseline`, PgDog by digest with the two-shard config in `pgdog-two-shard/`. |
| `pg-capture.sh` | Dynamic half: runs the scenarios below, dumps `pg_stat_statements` and the schemas after each (`pg-dump-stats.sh`), resets the statistics between scenarios. Writes `steps.tsv`. |
| `pg-merge.py` | Merges static kinds and dynamic captures into `capture.jsonl`, one entry per digest. |
| `pg-prepare-ref.sh` | Restores the captured schemas onto the reference, one database per scenario and source database. |
| `pg-replay.sh` | Everything after the capture: build, merge, reference up, replay with `compat-replay`, suites table. |
| `suites.py` | Builds `…-suites.tsv` from `steps.tsv`. |
| `pg-baseline.sql`, `pgdog-two-shard/` | Our own two-shard workload through PgDog's container (DDL, cross-shard DML, 2PC, aggregates, COPY), and its configuration. |
| `shims/`, `pgdog-resharding.override.yaml` | Let PgDog's scripts run on podman: a `docker` wrapper, `pgbench` from the postgres image, the PgDog container wrapper, and a compose override that puts Postgres 17.11 with `pg_stat_statements` under PgDog's resharding stack. |

## Scenarios (Postgres)

PgDog's scenarios run from `$PGDOG_SRC/integration/`; the suites table records each one.

| Suite | What runs | Notes |
|---|---|---|
| `resharding` | `resharding/dev.sh`: COPY_DATA over four servers under pgbench write load, then replication catch-up | Own compose stack, Postgres 17.11 through the override. **Fails by timeout** at 20 minutes (PgDog's own limit is 16): the catch-up does not finish while pgbench keeps writing. The statements sent before that are captured. |
| `baseline` | `pg-baseline.sql` through PgDog's container with the two-shard config | Ours. |
| `pgbench` | `pgbench/run.sh`: pgbench simple, extended and prepared, COPY | Passes. |
| `schema_sync` | `schema_sync/dev.sh`: `pgdog schema-sync` (pre-data, post-data, cutover) with a publication | Passes. |
| `data_sync` | `copy_data/data_sync/run.sh`: 0 → 2 and 2 → 2 resharding under write load | Passes. |
| `two_pc` | `two_pc/crash_recovery.py`: PgDog killed during two-phase commit, WAL recovery | Passes (50 crash iterations). Needs `asyncpg` (a venv under the cache directory). |
| `rewrite`, `logical`, `failover` | not run | `rewrite` is configuration in this checkout (its specs are Ruby and Rust); `logical` downloads a dataset and walks a human through it; `failover` is interactive. Recorded as `skip`. |

## What the merge keeps

`pg-merge.py` builds `capture.jsonl` from four inputs and applies three rules, so that the table describes what the
router asks of the engine and does not reproduce its test fixtures:

- **Static kinds** with a canonical example become one row each, with the first five `path:line` hits as `source`.
- **Dynamic statements** come from `pg_stat_statements` of every server, per scenario (`dynamic:<suite>`), and from the
  server log's `received replication command` lines (replication commands never reach `pg_stat_statements`; each
  distinct shape becomes a row replayed with our canonical example for that command).
- **Component** is decided by statement text: `2pc`, `replication`, `copy`, `schema-sync`, `health`, `pool`, else `query`.
- **Definitions are dropped.** DDL in PgDog's `pgdog` schema, and every function, procedure or trigger definition (and
  anything with `$$`), is not recorded; one canonical row (`CREATE SCHEMA … CREATE TRIGGER`, written for Loams) stands
  for them, and its source says how many statements were dropped.
- **Replicated-schema DDL is capped** at three statements per statement kind and scenario (the schema is the
  scenario's fixture), and **workload statements** at twelve per kind and scenario; the source column records how many
  were seen. Router-originated statements (catalog queries, replication, COPY, 2PC, pool and health) are all kept.
- **Comments are stripped** from every example.

## Replay

`compat-replay` runs each example, with its session `SET`s, on the reference and the target in fresh connections, in a
transaction that is rolled back (a scratch database for transaction-control, 2PC and replication commands), and records
the canonical result hash: column names, rows (sorted unless the statement has `ORDER BY`), command tags and warnings,
or the error's SQLSTATE. Notes on the method:

- The reference runs twice; when the two hashes differ the statement is **volatile** (LSNs, clocks) and the row compares
  by shape (columns and row count), which the note says.
- Statements with `$n` (as `pg_stat_statements` stores them) are prepared and run with NULL parameters; the hash covers
  the inferred parameter and column types and the row count. Utility statements that cannot take parameters run with
  the literal `'1'` in place of each `$n`.
- `COPY … FROM STDIN` is sent and finished with no data; `COPY … TO STDOUT` is hashed. Replication commands go through
  `psql` with `replication=database` (tokio-postgres 0.7 cannot open one); `START_REPLICATION` counts as `copy-both
  started` when the server accepts it.
- A statement whose reference run fails because its object already exists is replayed in an empty database on both
  engines (the note says so).
- Replay runs on **schemas only**: the dumps restore tables without rows, so a `SELECT` compares how the engines
  answer (parse, plan, types, errors), not what the data says.
- With no `--target` every row is `pending-target` and the reference outcome is recorded in `ref_hash` and the note.

## Run it

```sh
export PGDOG_SRC=$HOME/.cache/loam/pgdog-v0.1.60
OUT=$HOME/.cache/loam/inventory/pg/$(date +%F)
scripts/router/inventory/pg-capture.sh "$OUT"        # about 45 minutes, most of it the resharding timeout
scripts/router/inventory/pg-replay.sh "$OUT"         # builds compat-replay, merges, replays on a fresh reference
cp "$OUT/statements.tsv" conformance/router/pgdog-loampg-statements.tsv
cp "$OUT/suites.tsv"     conformance/router/pgdog-loampg-suites.tsv
```

`SCENARIOS="pgbench two_pc" pg-capture.sh "$OUT"` re-runs a subset and keeps the other rows of `steps.tsv`.
`pg-replay.sh "$OUT" postgres://user:pw@host:port/db` replays against a target too. A PR that changes a pinned version
re-runs the inventory and shows the diff of the TSVs.

## MySQL half

`compose.vitess.yml` runs etcd, three backends (`my-a/b/c`, mysql:8.0.46, or `wesql-a/b/c` with RustFS), vtctld, vtgate and
four `--unmanaged` vttablets (Ruling 6): `commerce/0` and `customer/-80` on backend A, `customer/0` on B, `customer/80-` on C
(one backend per primary of a keyspace, because the sidecar database `_vt`, or `_vt_customer`, must not be shared). The MySQL
settings are those of the plan (`gtid_mode`, `enforce_gtid_consistency`, ROW and FULL binlog, `performance_schema`, the
semi-sync source plugin). The tablets connect over a Unix socket (a volume shared with the backend), because Vitess refuses to
prepare a two-phase-commit transaction on a TCP connection. Profiles: `ref` and `wesql`.

| Script | Role |
|---|---|
| `vitess-static.sh` | Static half: `source_path:line<TAB>statement` over the paths of §31 §15 step 1 plus the 2PC and Online DDL files. Drops log messages that start with a SQL verb. |
| `vitess-capture.sh ref\|wesql [dir]` | Brings the stack up, runs the scenario steps, snapshots `performance_schema.events_statements_summary_by_digest` around every step, dumps digests and schemas, writes `steps.tsv` and `observations.tsv`, stops the stack. |
| `vt-corpus.py` | The vtgate DML and SELECT corpus: 1 320 distinct statements from `go/vt/vtgate/planbuilder/testdata/*_cases.json`. |
| `vt-merge.py` | Static plus dynamic into `capture.jsonl`; the digests each step touched; the `C-n` tags (the `issue` column). |
| `vitess-replay.sh <dir>` | Merge, MySQL 8.0.46 and WeSQL up together, schemas restored on both, `compat-replay`, suites table. |
| `vitess-unsupported-rules.tsv`, `vitess-shape-only.txt` | WeSQL refusals that are by design (SmartEngine's isolation levels, foreign keys, consensus replication); statements that read an instance's identity (compared by column names). |

Steps of one run: bring-up (vtctld, tablets, `TabletExternallyReparented`, vtgate), `ApplySchema`/`ApplyVSchema`,
the corpus, a hand-written DML set, `MoveTables` (create, VDiff, SwitchTraffic, ReverseTraffic, SwitchTraffic, Complete),
`Reshard 0 -> -80,80-` (create, VDiff, SwitchTraffic, Complete), an Online DDL `ALTER` (`vitess` strategy), a repeated
`TabletExternallyReparented`, `PlannedReparentShard` (skipped: unmanaged tablets refuse it), a vttablet restart (C-1), isolation
levels and temporary tables through vtgate (C-4, C-5), and last a two-phase-commit transaction across the keyspaces (C-2; when
it fails it can leave a prepared transaction that blocks DDL on the table, which a first order of the steps showed).

Replay notes: statements that change the engine for good (DDL, globals, replication commands) run last; their side effects
(`read_only`, table locks) are undone; `SHUTDOWN`, `DROP DATABASE` and the like are never run. Static statements with Go format
verbs or bind variables cannot run as written; they compare by error code (usually 1064), which the row's note shows.

```sh
OUT=$HOME/.cache/loam/inventory/mysql/$(date +%F)
scripts/router/inventory/vitess-capture.sh ref   $OUT/ref
scripts/router/inventory/vitess-capture.sh wesql $OUT/wesql
scripts/router/inventory/vitess-replay.sh $OUT
cp $OUT/statements.tsv conformance/router/vitess-wesql-statements.tsv; cp $OUT/suites.tsv conformance/router/vitess-wesql-suites.tsv
```
