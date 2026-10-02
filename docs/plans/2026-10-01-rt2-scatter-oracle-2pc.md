# RT2 — Scatter, Merge and Aggregate with the Lean Oracle, Postgres 2PC and the Change Stream Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, flags, constants), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: Planned** (2026-10-01). Track RT, phase RT2 (design [§31](../design/31-loams-router-and-verification.md) §17). Branches `rt2-t<N>`, stacked; PRs target `main`. Depends on [RT1](2026-10-01-rt1-postgres-slice-and-sim.md) (the stack, adapters, models, `loams-detsim`). Tasks 4 and the Loams Postgres rows of Tasks 8–9 need §28's P2b (Loams computes started by Loams's control plane, `deploy/neon`); without it they run on `postgres:17.11` and their Loams Postgres rows stay open, which the exit report says. RT2 changes no M-track code path.

**Goal:** The chat dump's M2, as reconciled in §31:
- **The Lean kernels for cross-shard results** (k-way merge, `LIMIT`/`OFFSET` pushdown, aggregate decomposition) proved, compiled into the oracle, and mirrored in Rust (D312, §31 §12);
- **a three-way differential of cross-shard queries**: unsharded Postgres, PgDog over 2 and 4 shards, and the oracle over PgDog's per-shard results (§31 §12.2, §14.2);
- **Loams Postgres prepared transactions** proven durable across compute, pageserver and acceptor restarts, and **PgDog 2PC** under D306's deployment rule, with `CrossShardCommit.tla` checked and the in-doubt monitor (§31 §6.5, §8, §10);
- **the change stream and a snapshot-consistent copy with an exact boundary**, and an offline split verified by checksums (§31 §8).

**Architecture:**
- **Kernels**: `spec/lean/LoamsRouter/{Merge,Limit,Aggregate}.lean`; Rust mirror `loams_sqlrouter::reference` (pure). Neither is on a routing path: PgDog routes and merges; Loams's code checks it.
- **Differential harness** in `loams-sqlrouter-io/tests/it/cross_shard.rs`, using a query generator over a fixed schema family, the stack of RT1 with a 4-shard variant, and the oracle binary.
- **2PC**: PgDog's own (`two_phase_commit = true`) on a stack variant that satisfies D306; `CrossShardCommit.tla` models it; `InDoubtMonitor` (a machine) watches shards; `loams-detsim`'s PgDog model gains the 2PC protocol and its log.
- **Change stream**: tests and a small decoder over Postgres's SQL interface to logical slots; no streaming client is built in RT2 (D154's bridge stays §23's N4).

**Tech Stack:**
- As RT1. New: the CommunityModules `Json` (already pinned in RT1); no new Rust dependency is expected (Task 0 confirms; a pgoutput decoder is written in Task 8 over `bytes`).
- PgDog v0.1.60 with `two_phase_commit`; `postgres:17.11` with `max_prepared_transactions = 64`, `wal_level = logical`; Loams Postgres computes from `deploy/neon` (fork `dina-kar/neon`).

**Spec:**
- [`docs/design/31-loams-router-and-verification.md`](../design/31-loams-router-and-verification.md): §6.4–§6.5, §8, §10, §11.2 (`CrossShardCommit`), §12, §13.3–§13.4, §14.2.
- [`docs/design/28-loams-postgres.md`](../design/28-loams-postgres.md) §5.2 (the compute spec Loams serves), §7.2 (Arm A), §8.
- PgDog's documentation of cross-shard queries, partial aggregates and 2PC (docs.pgdog.dev), read for behaviour only.

## Global Constraints

Same as RT1, plus:
- **Allowlisted, never hidden.** A cross-shard result that differs from unsharded Postgres is either a bug (filed upstream with the owner's approval, §28 §2.2) or an entry in `conformance/router/pgdog-cross-shard-allowlist.toml` that cites PgDog's documentation for the behaviour. No test is weakened to pass.
- **The in-doubt monitor never resolves a transaction** (§31 §6.5). It has no code path that issues `COMMIT PREPARED` or `ROLLBACK PREPARED`; a test greps for those strings in its crate module.
- **2PC is off by default** in every rendered config; only a record with `two_phase = TwoPhase::Durable { node_id, deployment_id, wal_dir }` renders it on (D306).
- **Commit areas:** `router`, `sim`, `spec`, `lean`, `ci`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **The query generator covers a declared subset**: single-table `SELECT` with `WHERE` (equality, range, `IN`, `IS NULL`), `ORDER BY` on up to two columns with `NULLS FIRST/LAST`, `LIMIT`/`OFFSET`, `COUNT(*)`, `COUNT(col)`, `SUM`, `MIN`, `MAX`, `AVG`, `GROUP BY` on up to two columns, `DISTINCT` on one column. Joins, window functions and subqueries are out | The Lean model covers exactly this; the chat dump defers cross-shard joins | A PgDog bug outside the subset is not found by RT2; RT5's nemesis workloads widen it |
| 2 | **Results are compared as multisets unless `ORDER BY` is total** (includes the primary key); with a partial order, rows are compared as a sequence of tie groups: each group's sort key must match in order, complete groups compare as multisets, and a group cut by `LIMIT` or `OFFSET` at either boundary compares by its sort key and row count only, because which tied rows fall inside the window is unspecified | SQL leaves tie order unspecified | None |
| 3 | **`SUM` over integers compares exactly; `AVG` compares after normalizing the oracle's exact rational to Postgres's `numeric` result**: the oracle's `{num, den}` is rounded half away from zero to the scale of the `numeric` that unsharded Postgres returned for the same query (Postgres picks the division's display scale itself, `select_div_scale`), and the strings must be equal. A router that returns a float is a `differs` row unless allowlisted with PgDog's documentation | Postgres returns `numeric`, not an exact rational; comparing reduced rationals would fail on every recurring mean | An allowlist entry if PgDog documents float merging |
| 4 | **The shard-key column is `id bigint`** for the differential; text and uuid keys get one smoke test each | Hash coverage of other types is RT0's vectors and RT1's placement test | — |
| 5 | **The change-stream tests read slots through SQL** (`pg_logical_slot_get_binary_changes` with `pgoutput`), and create the exported-snapshot slot through a replication connection (`replication=database`, `CREATE_REPLICATION_SLOT … LOGICAL pgoutput (SNAPSHOT 'export')`) with `tokio-postgres`'s replication mode if Task 0 finds it supports the simple query on such a connection, else with `psql` from the test script | No streaming replication client to build or buy in RT2 | Slower tests; fine at test sizes |
| 6 | **The offline split runs PgDog's own `RESHARD` on one instance** (open-source single-instance cutover), then checks checksums; the multi-instance orchestrator is RT4 | Exercises the data path Loams relies on before Loams coordinates it | — |

## Review Focus

1. **The theorems state what the routers must do.** `kmerge_perm`, `limit_pushdown`, `decompose_correct`, with SQL NULL rules. Tests: Task 1 (`lake build`), Task 2 (`reference_matches_lean_oracle`).
2. **The differential cannot pass vacuously.** Every generated query returns rows on at least one shard in 90 % of cases (measured), and a mutant oracle is caught. Tests: Task 3 (`generator_hits_rows`, `mutant_oracle_is_caught`).
3. **2PC durability claims hold on Loams Postgres.** Tests: Task 4 (`prepared_survives_compute_restart` and the others).
4. **The monitor cannot decide.** Tests: Task 7 (`monitor_has_no_resolution_path`), Task 6's `MC_GuessingMonitor` variant.
5. **The snapshot boundary is exact.** Tests: Task 8 (`exported_snapshot_boundary_is_exact`).

## File structure

```
spec/lean/LoamsRouter/{Merge.lean,Limit.lean,Aggregate.lean,Value.lean}
spec/lean/LoamsRouter/Oracle.lean                 # new ops: merge, limit, aggregate
spec/tla/router/{CrossShardCommit.tla,MCCrossShardCommit.tla}
spec/tla/router/MCCrossShardCommit_{Small,LostLog,GuessingMonitor,TornLog,Nightly}.cfg
spec/tla/router/{CrossShardCommitTrace.tla,MCCrossShardCommitTrace.tla}
crates/loams-sqlrouter/src/reference/{mod.rs,value.rs,merge.rs,limit.rs,aggregate.rs}
crates/loams-sqlrouter/src/monitor.rs
crates/loams-sqlrouter/src/render/pgdog.rs      # two-phase settings
crates/loams-sqlrouter/tests/it/{reference.rs,lean_oracle.rs,monitor.rs}
crates/loams-sqlrouter/tests/sim/{two_phase.rs,corpus/*.seed}
crates/loams-detsim/src/models/pgdog.rs         # 2PC protocol and its log
crates/loams-detsim/src/checkers/{bank.rs,split.rs}
crates/loams-sqlrouter-io/src/{postgres.rs,pgoutput.rs}
crates/loams-sqlrouter-io/tests/it/{cross_shard.rs,gen.rs,two_phase.rs,loams_pg.rs,change_stream.rs,split.rs}
conformance/router/pgdog-cross-shard-allowlist.toml
scripts/router/{stack.sh,compose.stack.yml}      # variants: shards=4, twopc
docs/design/31-loams-router-and-verification.md  docs/plans/README.md  CHANGELOG.md
```

### Task 0: Reconcile with RT1 and the Postgres track

**Files:** read RT1's "Rulings made during execution", the as-built `loams-sqlrouter`, `loams-sqlrouter-io` and `loams-detsim`, `spec/tla/router/CrossShardCommit.tla` (RT0's skeleton), §28 P2b status and `deploy/neon`. Fill this plan's "Rulings made during execution" table.

**Checks:**
- Whether P2b merged, and how Loams's compute spec sets Postgres settings (where `max_prepared_transactions` goes).
- PgDog v0.1.60's documented cross-shard behaviour for the Ruling 1 subset (docs.pgdog.dev "cross-shard queries", "aggregates"): which aggregates it merges, how it merges `AVG`, `DISTINCT`, `OFFSET`; record each as an expected row so the allowlist starts from documentation.
- PgDog's 2PC settings and environment (`two_phase_commit`, `two_phase_commit_auto`, `PGDOG_TWO_PHASE_COMMIT_WAL_DIR`, `NODE_ID`, `DEPLOYMENT_ID`) in the v0.1.60 documentation, and whether a default WAL directory exists when the variable is unset (§31 §6.5 (verify)).
- `tokio-postgres` 0.7's replication-mode support (Ruling 5).
- Whether `pgoutput` binary changes through `pg_logical_slot_get_binary_changes` work on Loams Postgres computes (Neon's compute).

**Commit:** `docs: reconcile RT2 with RT1 and main`.

### Task 1: Lean kernels: merge, limit, aggregate

**Files:** `spec/lean/LoamsRouter/{Value.lean,Merge.lean,Limit.lean,Aggregate.lean}`.

**Produces:**
- `Value.lean`: `inductive SqlVal | null | int (i : Int) | text (s : String)`; `compareWith (nullsFirst : Bool) (desc : Bool)`; a `Row` as `List SqlVal`; `SortKey` (column index, direction, nulls position); `lexCompare`; `IsTotalPreorder` lemmas.
- `Merge.lean`: `kmerge (key : SortKey list) : List (List Row) → List Row` (by repeated two-way merge, stable by shard index); theorems `kmerge_sorted : (∀ xs ∈ shards, Sorted key xs) → Sorted key (kmerge key shards)` and `kmerge_perm : kmerge key shards ~ shards.join`.
- `Limit.lean`: `limit_pushdown : (∀ xs ∈ shards, Sorted key xs) → (kmerge key shards).drop o |>.take n = (kmerge key (shards.map (·.take (o + n)))).drop o |>.take n`.
- `Aggregate.lean`: `Agg := count_star | count (c) | sum (c) | min (c) | max (c) | avg (c)`; `partial : Agg → List Row → Partial`; `combine : Agg → List Partial → Result`; `final : Agg → List Row → Result`, with SQL NULL rules (`COUNT(col)` and `SUM`, `MIN`, `MAX`, `AVG` skip NULLs; `SUM`/`MIN`/`MAX`/`AVG` of no non-null rows is NULL; `AVG` as an exact rational sum/count); theorem `decompose_correct : combine a (shards.map (partial a)) = final a shards.join` for every `a`; `groupby_decompose` for `GROUP BY` on one or two columns (grouping by value equality where NULLs form one group, as SQL does).

**Tests:** `lake build` (theorems checked); Plausible `#test` properties for `kmerge` stability and for `DISTINCT` on one column (`dedup (kmerge …)` equals the distinct values of the union, sorted), which is a property, not a theorem, in RT2.

**PR size:** about 600 lines of Lean; split into Merge+Limit and Aggregate PRs if review asks.

**Commit:** `lean: prove k-way merge, limit pushdown and aggregate decomposition`.

### Task 2: The oracle's new operations, the Rust reference and the mirror properties

**Files:** `spec/lean/LoamsRouter/Oracle.lean`, `crates/loams-sqlrouter/src/reference/*`, `crates/loams-sqlrouter/tests/it/{reference.rs,lean_oracle.rs}`.

**Produces:**
- Oracle operations (JSON lines): `{"op":"merge","order":[{"col":0,"desc":false,"nulls_first":false}],"shards":[[[…row…],…],…]}` → `{"ok":true,"rows":[…]}`; `{"op":"limit","order":…,"offset":o,"limit":n,"shards":…}`; `{"op":"aggregate","aggs":[{"kind":"avg","col":2}],"group_by":[0],"shards":…}` → rows of `[group…, result…]` with `AVG` as `{"num":p,"den":q}` reduced. Values: `null`, integers as JSON numbers within ±2^53 or strings `"i:<decimal>"` beyond, text as `"t:<string>"`.
- Rust: `loams_sqlrouter::reference::{SqlVal, SortKey, kmerge, limit, Agg, Partial, partial, combine, final_}` with the same semantics and the same JSON codec (`reference::json`).

**Tests:** proptest mirrors `kmerge_is_sorted`, `kmerge_is_a_permutation`, `limit_pushdown_holds`, `decompose_matches_final` (each 1 000 cases); `reference_matches_lean_oracle` (10 000 cases per operation on PRs touching either side, 100 000 nightly; skips without the oracle as RT0).

**Commit:** `router: mirror the Lean kernels in Rust and diff them against the oracle`.

### Task 3: The three-way cross-shard differential

**Files:** `crates/loams-sqlrouter-io/tests/it/{cross_shard.rs,gen.rs}`, `scripts/router/{stack.sh,compose.stack.yml}` (variant `SHARDS=4`), `conformance/router/pgdog-cross-shard-allowlist.toml`.

**Produces:**
- Schema `t (id bigint PRIMARY KEY, g int, h text, n bigint, x bigint NULL, s text NULL)` on every shard and on `unsharded`; a seeded data generator (1 000–10 000 rows, NULL rate 10 %, skewed `g`).
- `gen.rs`: the Ruling 1 query generator from a seed, producing SQL text and, for each query, the oracle request that recomputes the answer from **per-shard** results (the harness runs the per-shard part directly on each shard with the shard-local form PgDog documents, e.g. `ORDER BY … LIMIT o+n` per shard, partial aggregates per shard).
- The allowlist format:

  ```toml
  [[deviation]]
  id = "pgdog-avg-float"
  matches = { agg = "avg" }
  behaviour = "PgDog returns AVG as double precision when merging partial averages"
  source = "docs.pgdog.dev/features/sharding/cross-shard/ (as read on <date>)"
  ```

**Semantics:** for each query: result through PgDog (2 shards and 4 shards), result on `unsharded`, oracle over per-shard results. All three must agree under Ruling 2 and 3, or the difference must match an allowlist entry. A disagreement between the oracle and `unsharded` is a harness bug and fails the test regardless of the allowlist.

**Tests:** `cross_shard_equals_unsharded_and_oracle` (proptest, 512 queries on PRs, 10 000 nightly, per shard count); `generator_hits_rows` (≥ 90 % of queries return rows from ≥ 1 shard, and ≥ 50 % from ≥ 2); `mutant_oracle_is_caught` (an oracle build with `limit_pushdown` deliberately off by one, feature `oracle-mutant`, fails the test within 512 queries); `oracle_agrees_with_unsharded` (sanity, every query); `avg_recurring_mean_matches_postgres` (`AVG` over `{1, 2, 2}` = 5/3 on two shards: the normalized oracle result equals Postgres's `1.6666666666666667`, and PgDog's result is compared the same way).

**Commit:** `router: compare cross-shard results through PgDog with Postgres and the oracle`.

### Task 4: Prepared transactions on Loams Postgres

**Files:** `crates/loams-sqlrouter-io/tests/it/loams_pg.rs`; the compute-spec setting in Loams's control plane (the file P2b added; Task 0 names it); `docs/design/28-loams-postgres.md` (one line under §5.2 noting the setting, marked D307).

**Produces:** `max_prepared_transactions = 64` in every compute spec Loams renders for a database whose record allows 2PC (and `0` otherwise, Postgres's default, so a non-2PC database refuses `PREPARE TRANSACTION`).

**Semantics:** each test prepares transactions with known gids (`loams_test_<n>`), then performs the disruption, then checks `pg_prepared_xacts`, commits or rolls back by gid, and checks the data.

**Tests** (skip with `skipped: needs LOAMS_PG_STACK` without `deploy/neon`): `prepared_survives_compute_restart`; `prepared_survives_pageserver_restart`; `prepared_survives_acceptor_kill` (Arm A, one of three `loams-wal` acceptors killed, if `loams-wal` is the compute's WAL; else with stock safekeepers, recorded); `prepared_on_branch_commits_independently` (a branch taken while a transaction is prepared sees it prepared; committing it on the branch leaves the parent's prepared, as Neon's `test_twophase.py` does); `prepare_refused_when_disabled` (`max_prepared_transactions = 0`).

**Commit:** `router: prove prepared transactions durable on Loams Postgres`.

### Task 5: PgDog 2PC under the deployment rule

**Files:** `crates/loams-sqlrouter/src/record.rs` (`TwoPhase` field), `crates/loams-sqlrouter/src/render/pgdog.rs`, `scripts/router/{stack.sh,compose.stack.yml}` (variant `TWOPC=1`), `crates/loams-sqlrouter-io/tests/it/two_phase.rs`.

**Produces:** `pub enum TwoPhase { Off, Durable { node_id: String, deployment_id: String, wal_dir: String } }` in `ShardMapRecord` (format byte stays `1` if RT1 left room through `serde(default)`, else `2` with a reader for `1`); rendering sets `two_phase_commit = true` and the environment for `Durable` only; the compose variant runs PgDog with `NODE_ID=0`, `DEPLOYMENT_ID=stack-kv`, `PGDOG_TWO_PHASE_COMMIT_WAL_DIR=/var/lib/pgdog/2pc` on a named volume.

**Tests** (stack, `TWOPC=1`): `cross_shard_transfer_is_atomic` (bank transfers between keys on different shards, 1 000 transfers, the total is conserved after each); `kill_between_phases_recovers_with_volume` (kill PgDog repeatedly at random times under the transfer load with `docker kill`; after restart, PgDog's recovery leaves no `__pgdog_2pc_` transaction prepared within 60 s, and the total is conserved); `wiped_volume_leaves_in_doubt_and_monitor_alerts` (kill, delete the volume, restart: prepared transactions remain, and Task 7's monitor reports them; the total is checked only after the test resolves each gid with knowledge only it has (the transfer it issued and the rows each shard holds), as an operator would with the data in view; this case documents why the monitor cannot resolve on its own: a gid absent from `pg_prepared_xacts` may have been committed or rolled back); `two_phase_off_by_default` (the default record renders `two_phase_commit` absent or `false`).

**Commit:** `router: run PgDog two-phase commit only with a durable coordinator log`.

### Task 6: `CrossShardCommit.tla`, checked

**Files:** `spec/tla/router/{CrossShardCommit.tla,MCCrossShardCommit.tla,MCCrossShardCommit_*.cfg,CrossShardCommitTrace.tla,MCCrossShardCommitTrace.tla}`, `specs.toml`, README.

**Produces:** RT0's skeleton completed. `CONSTANTS Participants, Txns, LogDurable, MonitorGuesses, TornTail`. Variables: `log` (a sequence of `[txn, phase ∈ {1, 2}]`; when `TornTail`, a crash may drop a suffix of unsynced records), `synced` (prefix length), `coord ∈ {"up", "down"}`, `pstate` (`[Txns × Participants -> {"working", "prepared", "committed", "aborted"}]`), `decided` (`[Txns -> {"none", "commit", "abort"}]`). Actions: `LogPhase1(t)`, `Prepare(t, p)`, `LogPhase2(t)` (only when every participant is prepared and phase 1 is synced), `Sync`, `CommitPrepared(t, p)`, `AbortPrepared(t, p)`, `Done(t)`, `CoordCrash` (drops unsynced records; with `LogDurable = FALSE` drops the whole log), `CoordRecover` (phase 1 → abort, phase 2 → commit, as PgDog's recovery), `ParticipantCrash(p)` (prepared state survives, working state aborts), `MonitorScan` (alerts; with `MonitorGuesses = TRUE` it may `AbortPrepared` an in-doubt transaction).

**Invariants and properties:** `Atomicity` (never one participant `committed` and another `aborted` for the same txn); `CommitOnlyAfterPhase2Synced`; `MonitorNeverDecides` (vacuous unless `MonitorGuesses`); liveness `NoForeverPrepared` (`<>[]` every txn has no participant `prepared`) under `WF(CoordRecover)`, `WF(CommitPrepared)`, `WF(AbortPrepared)`.

**Variants:** `Small` (2 participants, 2 txns, durable log, no torn tail): ok. `TornTail` (durable, torn tail): ok, because phase 2 is logged and synced before any `COMMIT PREPARED` (if TLC finds otherwise, that is a PgDog finding for the owner, not a spec edit). `LostLog` (`LogDurable = FALSE`): `expect = "violation:NoForeverPrepared"`. `GuessingMonitor`: `expect = "violation:Atomicity"`. `Nightly`: 3 participants, 3 txns.

**Trace validation:** the `two_phase` DST scenario (Task 7) emits the model coordinator's and the monitor's actions; 1 run in 20 is validated.

**Tests:** `check.sh CrossShardCommit` with every variant's `expect`; trace validation on a committed trace.

**Commit:** `spec: check PgDog-style two-phase commit and the in-doubt monitor`.

### Task 7: The in-doubt monitor, the 2PC model and the `two_phase` scenario

**Files:** `crates/loams-sqlrouter/src/monitor.rs`, `crates/loams-sqlrouter/tests/it/monitor.rs`, `crates/loams-detsim/src/models/pgdog.rs`, `crates/loams-detsim/src/checkers/bank.rs`, `crates/loams-sqlrouter/tests/sim/two_phase.rs`.

**Produces:**

```rust
pub struct InDoubtMonitor { shards: Vec<u32>, alert_after: Millis /* 300_000 */, seen: BTreeMap<String, Seen> }
pub enum MonitorInput { Scan { shard: u32, prepared: Vec<PreparedXact>, at: Millis }, Instances(Vec<InstanceState>), Tick }
pub enum MonitorOutput { ScanShard(u32), Alert(InDoubt), Clear(String) }
pub struct InDoubt { pub gid: String, pub deployment: Option<String>, pub instance: String, pub shards: Vec<u32>, pub age: Millis, pub owner: OwnerState /* Running | Restarted | Gone | Unknown */ }
pub fn parse_pgdog_gid(gid: &str) -> Option<PgDogGid>;   // "__pgdog_2pc_[<deployment>_]<instance>_<n>", per §31 §6.5
```

The PgDog model gains: phase-1 and phase-2 log records with a synced prefix, `PREPARE TRANSACTION`/`COMMIT PREPARED`/`ROLLBACK PREPARED` against Postgres models, recovery as PgDog documents it, crash with the log kept, torn, or wiped (by seed). `checkers::bank::check(final_balances, initial_total, transfers)`.

**Semantics:** the monitor scans every 30 s (`Tick`), groups by gid, alerts once per gid when `age ≥ alert_after` and the owner is not `Running`, clears when the gid disappears. It never emits a resolution (Global Constraints).

**Tests:** `parses_pgdog_gids` (with and without a deployment id; foreign gids ignored); `alerts_once_after_threshold`; `clears_when_resolved`; `monitor_has_no_resolution_path` (the module source contains neither `COMMIT PREPARED` nor `ROLLBACK PREPARED`); scenario `two_phase` (bank transfers through 1–3 PgDog models with 2PC, 2–4 Postgres models; faults: coordinator crash with log kept or torn, participant crash, partitions, delays; checkers: conservation after recovery, atomicity per transfer, `NoForeverPrepared` within 120 simulated seconds after heal when the log is kept, and with a wiped log the monitor alerts for every stuck gid); `mutant_monitor_guess_is_caught` (feature `sim-mutant-guessing-monitor` rolls back in-doubt gids; the bank checker catches it within 2 000 seeds).

**Commit:** `router: add the in-doubt monitor and the two-phase simulation`.

### Task 8: The change stream and the exact snapshot boundary

**Files:** `crates/loams-sqlrouter-io/src/pgoutput.rs`, `crates/loams-sqlrouter-io/tests/it/change_stream.rs`.

**Produces:** a minimal pgoutput decoder (protocol version 1: `Begin`, `Commit`, `Relation`, `Insert`, `Update`, `Delete`, `Truncate`, text tuple data) over the bytes returned by `pg_logical_slot_get_binary_changes(slot, NULL, NULL, 'proto_version', '1', 'publication_names', <pub>)`; `ChangeEvent { lsn, xid, kind, table, key, new, old }`. Test helpers to create a slot with an exported snapshot (Ruling 5) and copy a table inside `SET TRANSACTION SNAPSHOT '<name>'`. **The replication connection that created the slot stays open and sends nothing else until the copying transaction has run `SET TRANSACTION SNAPSHOT`**: Postgres keeps an exported snapshot valid only until that session runs another command or closes. The helper holds the connection in a guard that the copy step releases after the import succeeds; `snapshot_import_after_release_fails` checks the opposite order fails.

**Tests** (on `postgres:17.11` always; on Loams Postgres with `LOAMS_PG_STACK`): `decodes_insert_update_delete`; `slot_survives_compute_restart` (Loams Postgres: changes committed before and after a compute restart are all read once); `exported_snapshot_boundary_is_exact` (a writer commits continuously; the test creates the slot with an exported snapshot, copies the table at that snapshot, then reads the slot's changes from its consistent point; copy ∪ changes equals the final table, by primary key, with no row missing and no change applied to a row the copy already had at a later version; 50 repetitions).

**Commit:** `router: verify the change stream and the snapshot boundary on Loams Postgres`.

### Task 9: An offline split, verified by checksums

**Files:** `crates/loams-detsim/src/checkers/split.rs`, `crates/loams-sqlrouter-io/tests/it/split.rs`, `scripts/router/stack.sh` (variant `SPLIT=1`: a one-shard source database and a two-shard destination database in PgDog's config).

**Produces:** `checkers::split::check(source: &TableDigest, destinations: &[TableDigest], shard_fn) -> Result<(), SplitViolation>` (every source row is on exactly the destination shard the shard function names, no extra rows, equal row hashes); the test drives PgDog's `RESHARD <source> <destination> <publication>` and `CUTOVER` on the single instance (Ruling 6) through the admin adapter.

**Tests:** `split_one_to_two_preserves_every_row` (10 000 rows; writer active during the copy and stopped before cutover; checksums equal and placement matches `pg_partition_index`); `split_with_writes_during_catch_up` (writer active until PgDog's pause; every acknowledged write is on its destination shard); `reverse_stream_carries_post_cutover_writes` (writes after cutover appear on the old source through PgDog's reverse stream, which is what RT4's rollback relies on). On Loams Postgres computes as well when `LOAMS_PG_STACK` is set.

**Commit:** `router: verify a PgDog split by checksums and placement`.

### Task 10: Docs and the RT2 exit

**Files:** §31 (as-built: the allowlist's entries with sources, the 2PC rule as rendered, the change-stream results, measured times), `docs/plans/README.md`, `CHANGELOG.md`; the RT3–RT5 rows refreshed with what RT2 found.

**Exit criteria:**
- The Lean theorems build; the Rust mirror and the oracle agree on 10 000 cases per operation.
- The three-way differential is clean or allowlisted with documentation for 2 and 4 shards; the mutant oracle is caught.
- 2PC: the stack tests pass with the deployment rule; the wiped-volume case alerts; `CrossShardCommit` passes and fails its variants as expected; `two_phase` DST is clean on the corpus and 2 000 seeds, and its mutant is caught.
- Loams Postgres: the prepared-transaction and change-stream tests pass on `deploy/neon`, or the exit report says P2b had not merged and lists them as open.
- The split's checksums and placement match.
- Q305 has a proposed answer for the owner with the evidence.

**Commit:** `docs: record RT2 as built and close RT2`.

## Rulings made during execution

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| — | (Task 0 fills this table) | | |
