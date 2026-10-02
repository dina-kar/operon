# RT0 — Router Foundations, Specs and the Compatibility Inventory Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, flags, constants), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: Done** (2026-10-02: Tasks 0–4 #188, Task 8 #189, Task 7 #191, Task 5 #193/#194, Task 6 #249/#250, Task 9 this PR). Planned 2026-10-01. Track RT, phase RT0 (design [§31](../design/31-loams-router-and-verification.md) §17, D319). Branches `rt0-t<N>`, stacked; PRs target `main`. RT0 adds two crates (`loams-sqlrouter`, `loams-compat`), a `spec/` tree, scripts, inventory tables and two CI jobs. It changes no existing crate, no default feature and no M-track code path. It needs no cluster except the containers the inventory starts. Every decision here is a proposal until the owner answers Q300 (§31 §19); Tasks 1–4 and 7–8 are useful whatever the answer, Tasks 5–6 too.

**Goal:** Lay the foundations the chat dump's M0 asked for, as reconciled in §31:
- `spec/tla/router/` with **`ShardMap` and `ReshardCutover` checked by TLC at small bounds**, skeletons of `CrossShardCommit`, `PrimaryFailover` and `RouterSession` that parse, and the `tla` CI job (D310);
- **the compatibility inventory**, static and dynamic halves, for PgDog in front of Postgres and for Vitess v24 in front of MySQL 8.0.46, replayed against WeSQL and Loam Postgres where they are available (D309, §31 §15);
- **`loams-sqlrouter`**, the sans-I/O kernel crate: the shard-map types, key ranges with split and merge, the hash functions with reference vectors, the `Machine`/`Ctx`/`TraceSink` seams and the lints that keep the crate free of I/O (§31 §7.1);
- **the Lean 4 project** `spec/lean/` with the range-partition theorems proved and an oracle binary that the Rust tests call, with the `lean` CI job (D312).

**Architecture:**
- **Specs are their own tree.** `spec/tla/router/*.tla` with `MC*.tla` models and `.cfg` variants listed in `spec/tla/router/specs.toml`; `scripts/spec/check.sh` runs TLC and Apalache from pinned downloads. A variant may be marked `expect = "violation:<Invariant>"`, which must fail exactly that way.
- **The kernel crate does no I/O.** `crates/loams-sqlrouter` depends on `serde`, `postcard`, `thiserror`, `rand_core`, `des` and `xxhash-rust` only; its own `clippy.toml` bans clocks, threads, env reads and unseeded randomness; it has no `tokio`.
- **The inventory is data plus small tools.** `crates/loams-compat` (test-only, `publish = false`) holds the `compat-replay` binary; capture scripts live in `scripts/router/inventory/`; blessed TSVs in `conformance/router/`. Pinned PgDog and Vitess checkouts live outside the repository in `$HOME/.cache/loam/` and are never committed.
- **Lean builds an executable.** `spec/lean/` is a Lake package `LoamsRouter` with library modules and an executable `loams-router-oracle` speaking JSON lines.

**Tech Stack:**
- Rust 1.97.1, edition 2024, workspace lints. New dependencies (Task 0 checks versions and `cargo deny`): `des` 0.9 (MIT OR Apache-2.0), `postcard` and `serde` (workspace), `rand_core` (workspace through `rand` 0.9), `xxhash-rust` 0.8 (workspace). For `loams-compat` only: `tokio-postgres` 0.7 (already a dev-dependency of `operon`), `mysql_async` (MIT OR Apache-2.0, version checked in Task 0), `sha2`, `csv` (workspace if present; else MIT OR Unlicense, checked).
- TLA+: `tla2tools.jar` v1.7.4 (MIT, 2024-08-05), CommunityModules (MIT, the release Task 0 picks), Apalache v0.62.3 (Apache-2.0). Java 21 (Temurin) in CI.
- Lean: `leanprover/lean4:v4.34.1` through elan, Plausible (Apache-2.0) pinned in `lake-manifest.json`.
- Containers for the inventory: `postgres:17.11`, `mysql:8.0.46` (verify the tag exists; otherwise the closest 8.0 patch, recorded), the WeSQL image built by `deploy/wesql/` at fork commit `eef34f452` or later, `ghcr.io/pgdogdev/pgdog` v0.1.60 by digest, Vitess v24.0.4 images (`vitess/lite:v24.0.4`, verify the name), `quay.io/coreos/etcd:v3.7.2`.

**Spec:**
- [`docs/design/31-loams-router-and-verification.md`](../design/31-loams-router-and-verification.md): all of it; §6 (record, rendering, push, cutover, monitor), §7 (seams), §9.2 (C-1–C-7), §11 (specs), §12 (Lean), §15 (inventory method), §16 (licenses).
- [`docs/design/28-loam-postgres.md`](../design/28-loam-postgres.md) §8 (PgDog rules) and §11 (P-phases); [`docs/design/29-wesql-oltp.md`](../design/29-wesql-oltp.md) once PR #172 merges (until then `git show origin/wesql-oltp-design:docs/design/29-wesql-oltp.md`).
- Neon's spec layout as a reference: `neon/safekeeper/spec/` (`modelcheck.sh`, `MC*.tla`, `models/`).

## Global Constraints

Same as the M1 overview §8, plus:
- **No PgDog code or text in Loam.** PgDog (AGPL-3.0) is read as a reference and run as an unmodified container (D236, D318). Specs are written from its documentation and observed behaviour; inventory rows record source paths and captured statements, never PgDog source lines; no PgDog test is copied. A reviewer rejects any PR that pastes or translates PgDog code.
- **Vitess material only with its notice.** Test vectors copied from Vitess (`go/vt/vtgate/vindexes/hash_test.go`) carry the Apache-2.0 notice in the fixture's header; no other Vitess code is ported in RT0.
- **PostgreSQL's hash functions** are ported from PostgreSQL's `src/common/hashfn.c` and `src/backend/access/hash/hashfunc.c` (PostgreSQL License) with the notice in the module header, never from PgDog's `hashfn.c` copy.
- **Pinned external checkouts stay outside the repository**, in `$HOME/.cache/loam/{pgdog-v0.1.60,vitess-v24.0.4}` (shallow clones by tag). Scripts take the path as `PGDOG_SRC` and `VITESS_SRC`.
- **The build machine.** One cargo build at a time, the shared target directory, `-j 6`, lld. Containers are stopped before a cargo build. TLC runs with `-workers 2` locally. Stop and report if `/home` has under 8 GB free.
- **Commit areas:** `spec`, `router`, `compat`, `ci`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **TLC v1.7.4 (stable) is pinned**, not the rolling v1.8.0 pre-release | A rolling tag changes its jar under the same name; reproducible CI needs a fixed SHA-256 | If trace validation (RT1) needs a 1.8.0 feature, RT1 pins a dated 1.8.0 build by SHA-256 and records it |
| 2 | **PR bounds keep every TLC run under 5 minutes on a GitHub runner (estimate)**; nightly bounds are larger and listed per spec | The `tla` job must not dominate PR time | A bug that needs larger bounds is found nightly, not on the PR |
| 3 | **A spec that documents an unsafe configuration carries an `expect = "violation:<Inv>"` variant** | Keeps the reason for each rule (fence before ConfigMap, durable 2PC log) executable | None |
| 4 | **Postgres hash ground truth comes from real Postgres hash partitioning** (`CREATE TABLE … PARTITION BY HASH`, then which partition a key lands in), not from PgDog's code | PgDog documents that it matches Postgres's hash partitioning; Postgres is the primary source | If PgDog differs from Postgres for some type, RT1's routing test finds it, and the type is recorded as a deviation |
| 5 | **The kernel crate has no async and no I/O**, enforced by its `clippy.toml` and by a test that greps its `Cargo.toml` for forbidden dependencies (`tokio`, `reqwest`, `tikv-client`, `tokio-postgres`) | D313's determinism rests on it | None |
| 6 | **The inventory's dynamic half runs vttablet in unmanaged mode against MySQL 8.0.46**, the mode WeSQL will use (D302) | Managed-mode statements (mysqlctl, backups) are not what Loam runs | Statements only managed mode sends are missed; they are out of Loam's scope anyway |
| 7 | **An inventory row whose target is not yet runnable is `pending-target`**, not a failure | Loam Postgres computes need P2b; WeSQL's image exists today | RT0 exits with Postgres-side rows pending; RT1 Task 0 re-runs them if P2b merged |
| 8 | **`ShardMapRecord` is postcard-encoded with a leading format byte `1`** | The metastore's encoding (D16); a byte lets RT1 change the layout | A layout change needs a new format byte and a reader for `1` |

## Review Focus

1. **The invariants are the right ones.** `ShardMap`'s `SingleWriter`, `NoStrayWrite` and `ConfigMapSafe`; `ReshardCutover`'s `NoLostWrite` and `ReverseSafe`. The `expect = violation` variants must fail for the stated reason (read TLC's trace in the PR description). Tests: Tasks 2–3.
2. **No PgDog code anywhere.** Spec text, inventory rows, fixtures. Tests: the `license-provenance` check of Task 1 (a grep for PgDog's license header and for its identifiers in `spec/` and `conformance/`).
3. **Hash vectors are from primary sources.** Tests: Task 7 (`pg_hash_matches_postgres_partitioning`, `vitess_hash_matches_vitess_vectors`).
4. **The kernel crate cannot do I/O.** Tests: Task 7 (`kernel_crate_has_no_io_dependencies`, clippy with the crate's `clippy.toml`).
5. **Lean proves what the Rust checks.** The theorem statements of Task 8 match `loams_sqlrouter::ranges`' doc comments one to one.

## File structure

```
spec/README.md                                  # what lives here, how to run it, licenses
spec/tla/router/README.md                       # per-spec bounds, run times, action-to-code tables
spec/tla/router/specs.toml                      # name, model, cfg variants, bounds, expect
spec/tla/router/{ShardMap,ReshardCutover,CrossShardCommit,PrimaryFailover,RouterSession}.tla
spec/tla/router/MC{ShardMap,ReshardCutover}.tla
spec/tla/router/MCShardMap_{Small,UnsafeConfigMap,Nightly}.cfg
spec/tla/router/MCReshardCutover_{Small,NoFence,CrashSaga,Nightly}.cfg
spec/tla/selftest/{Selftest.tla,MCSelftest_Violation.cfg}
spec/lean/{lakefile.lean,lean-toolchain,lake-manifest.json,Main.lean}
spec/lean/LoamsRouter/{KeyRange.lean,ShardFn.lean,Oracle.lean,Json.lean}
scripts/spec/{check.sh,tools.lock,provenance.sh}
scripts/router/inventory/{pg-static.sh,pg-capture.sh,vitess-static.sh,vitess-capture.sh,compose.pg.yml,compose.vitess.yml,README.md}
scripts/router/gen-pg-hash-vectors.sh
crates/loams-sqlrouter/{Cargo.toml,clippy.toml}
crates/loams-sqlrouter/src/{lib.rs,record.rs,ranges.rs,hash/mod.rs,hash/pg.rs,hash/vitess.rs,machine.rs,trace.rs}
crates/loams-sqlrouter/tests/it/{main.rs,record.rs,ranges.rs,hash.rs,lean_oracle.rs,deps.rs}
crates/loams-sqlrouter/tests/fixtures/{pg_hash_vectors.json,vitess_hash_vectors.json}
crates/loams-compat/{Cargo.toml,src/main.rs,src/replay.rs,src/classify.rs,src/tsv.rs,tests/it/main.rs}
conformance/router/README.md
conformance/router/{pgdog-loampg,vitess-wesql}-statements.tsv
conformance/router/{pgdog-loampg,vitess-wesql}-suites.tsv
.github/workflows/ci.yml                        # jobs tla, lean; the changes filter gains spec and router
deny.toml                                       # only if Task 0 finds a license to allow (none expected)
docs/design/31-loams-router-and-verification.md  docs/plans/README.md  CHANGELOG.md
```

### Task 0: Reconcile and check the facts

**Files:** read §31, §28 §8, §29 (PR #172 or `main`), `crates/loams-safekeeper/` (as-built Arm A names), `.github/workflows/ci.yml` (the `changes` job's filters), `deny.toml`, `Cargo.toml`. Fill this plan's "Rulings made during execution" table.

**Checks** (record each result with the command):
- Whether PR #172 (§29) has merged, and §29's final decision numbers (D273–D280 expected).
- Whether §28's P2b and P3 have merged (Loam Postgres computes and PgDog routing in `deploy/`), which decides Ruling 7's `pending-target` rows.
- The latest PgDog release and the image digest for v0.1.60 (or the newer release, recorded, if the owner wants to move the pin); PgDog's license is still AGPL-3.0.
- Vitess: the latest v24 patch (v24.0.4 on 2026-10-01), the image names for vtgate, vttablet and vtctld at that tag, and whether vitess.io still says v24 is the last release with MySQL 8.0.
- `tla2tools.jar` v1.7.4 SHA-256; the CommunityModules release that works with it (its `Json` module is what RT1's trace validation needs; record the version); Apalache v0.62.3 SHA-256; Lean v4.34.1 still the latest stable, Plausible's tag for it.
- `cargo deny check` for `des` 0.9 and `mysql_async`; `cargo tree -d` for new duplicates.
- The final D and Q numbers (D300–D322, Q300–Q314 reserved).

**Commit:** `docs: reconcile RT0 with main`.

### Task 1: The spec tree, the checker script and the `tla` job

**Files:** `spec/README.md`, `spec/tla/router/{README.md,specs.toml}`, `spec/tla/selftest/*`, `scripts/spec/{check.sh,tools.lock,provenance.sh}`, `.github/workflows/ci.yml`.

**Produces:**
- `scripts/spec/tools.lock`: one line per tool, `name version url sha256`. `check.sh` downloads into `$HOME/.cache/loam/spec-tools/` and verifies the hash before use; a mismatch exits 2 with `checksum mismatch for <name>`.
- `scripts/spec/check.sh <Spec> [<variant>] [--parse-only] [--nightly]`: reads `specs.toml`, runs SANY (`--parse-only`), TLC (`java -XX:+UseParallelGC -cp tla2tools.jar:CommunityModules.jar tlc2.TLC -workers ${TLC_WORKERS:-2} -deadlock -config <cfg> <model>`) and, for variants with `apalache = ["Inv", …]`, `apalache-mc check --inv=<Inv> --length=<k>`. Exit 0 when every result matches `expect` (`"ok"` or `"violation:<Inv>"`), 1 otherwise, printing the variant, the expected and actual outcome, the states found and the time.
- `specs.toml` format:

  ```toml
  [[spec]]
  name = "ShardMap"
  model = "MCShardMap.tla"
  [[spec.variant]]
  cfg = "MCShardMap_Small.cfg"
  expect = "ok"
  pr = true
  apalache = ["SingleWriter"]
  apalache_length = 8
  ```

- `scripts/spec/provenance.sh`: fails if any file under `spec/`, `conformance/` or `crates/loams-sqlrouter/` contains `GNU AFFERO`, `pgdog::`, `use pgdog` or a line PgDog's source has verbatim (a list of 20 distinctive identifiers from PgDog's 2PC and resharding modules, kept in the script, chosen in this task); run in the `tla` job.
- CI: the `changes` job gains filters `spec` (`spec/**`, `scripts/spec/**`) and `router` (`crates/loams-sqlrouter/**`, `crates/loams-compat/**`, `conformance/**`). Job `tla` (needs `changes`; runs when `spec` or `router` changed, or on schedule): Temurin 21, cache `~/.cache/loam/spec-tools`, `scripts/spec/check.sh --all` (PR variants) or `--all --nightly` on schedule, then `provenance.sh`, which covers `spec/`, `conformance/` and `crates/loams-sqlrouter/`.

**Semantics:** `spec/tla/selftest/Selftest.tla` is a three-line counter with an invariant that the counter stays under 3; `MCSelftest_Violation.cfg` sets the bound so TLC must find the violation. It proves the job detects failures and is listed with `expect = "violation:Small"`.

**Tests:** `check.sh --self-test` (runs the self-test spec, then a corrupted copy of `tools.lock` and expects exit 2); the CI job itself on the PR.

**Commit:** `spec: add the TLA+ tree, the checker script and the tla job`.

### Task 2: `ShardMap.tla`

**Files:** `spec/tla/router/{ShardMap.tla,MCShardMap.tla,MCShardMap_Small.cfg,MCShardMap_UnsafeConfigMap.cfg,MCShardMap_Nightly.cfg}`, `spec/tla/router/README.md` (bounds, times, the action-to-code table of §31 §11.3).

**Produces** (the spec's interface; the TLA+ text is written in this task):
- `CONSTANTS Keys, Shards, Instances, MaxGen, MaxWrites, UnsafeConfigMapFirst` (the last is `FALSE` except in the unsafe variant).
- `VARIABLES record` (`[gen: 0..MaxGen, owner: [Keys -> Shards]]`), `history` (`[0..MaxGen -> [Keys -> Shards]]`, each generation's map), `configmap` (a generation), `applied` (`[Instances -> 0..MaxGen]`), `fenced` (`[Shards -> BOOLEAN]`), `caughtUp` (`[Shards -> SUBSET Keys]`, keys whose data the shard holds), `data` (`[Shards -> SUBSET Writes]`), `acked` (`SUBSET Writes`), `nextWrite`.
- **Actions:** `Publish(g, m)` (CAS: only from `record.gen = g - 1`); `CopyKeys(s, K)` (moves the old owner's writes for `K` into `s` and adds `K` to `caughtUp[s]`; models PgDog's copy plus stream); `Fence(s)` and `Unfence(s)`; `WriteConfigMap(g)`; `Reload(i)` (`applied[i] := configmap`); `Restart(i)` (same as `Reload`, separate action for traces); `ClientWrite(i, k)` (routes to `history[applied[i]][k]`; the shard accepts iff not fenced, it owns `k` in the newest generation in which it appears, and `k ∈ caughtUp[s]`; on accept the write enters `data` and `acked`); `Reject`.
- **Invariants:** `TypeOK`; `OneOwner` (each `history[g]` is a total function, by construction, checked); `SingleWriter` (for every key, at most one shard is unfenced and owns it in some generation not yet superseded on every instance); `NoStrayWrite` (every write in `data[s]` for key `k` was accepted while `s` owned `k`); `NoLostAck` (every acked write for `k` is in `data[record.owner[k]]` once `caughtUp[record.owner[k]]` contains `k`); `ConfigMapSafe` (`configmap`'s owner for every key is unfenced and caught up, or fenced only because a newer generation is mid-cutover).
- **Liveness** (`MCShardMap_Small` only, with `WF_vars(Reload(i))` for each instance and `SF_vars(ClientWrite)`): `<>[](\A i \in Instances: applied[i] = record.gen)`.
- **Ordering rule under test:** the safe protocol writes the ConfigMap for generation `g` only after every shard that loses keys in `g` is fenced and every shard that gains keys has them in `caughtUp`. With `UnsafeConfigMapFirst = TRUE` the ConfigMap may be written first.

**Bounds:** `Small`: `Keys = {k1, k2}`, `Shards = {s1, s2, s3}`, `Instances = {i1, i2}`, `MaxGen = 2`, `MaxWrites = 3`. `Nightly`: three keys, three instances, `MaxGen = 3`, `MaxWrites = 4`. `UnsafeConfigMap`: `Small` with `UnsafeConfigMapFirst = TRUE`, `expect = "violation:NoStrayWrite"` (a restarted instance routes to a gaining shard before it caught up) or `ConfigMapSafe`; the task records which one TLC reports and sets `expect` to it.

**Tests:** `check.sh ShardMap` passes `Small` and the expected violation; Apalache proves `SingleWriter` inductive with an `IndInv` conjunction written in this task (`apalache_length = 8`); the PR description pastes TLC's state counts and the unsafe trace.

**Commit:** `spec: model the shard map, its generations and the ConfigMap rule`.

### Task 3: `ReshardCutover.tla`

**Files:** `spec/tla/router/{ReshardCutover.tla,MCReshardCutover.tla,MCReshardCutover_{Small,NoFence,CrashSaga,Nightly}.cfg}`, README update.

**Produces:**
- `CONSTANTS Keys, Instances, Designated, MaxWrites, FenceEnabled, SagaCrashes`.
- Two clusters, `Src` and `Dst` (each a set of shards collapsed to one logical store per key, since §31 §6.4's safety argument is per key), a forward stream and a reverse stream (sequences of writes), `phase ∈ {"Copy", "CatchUp", "Paused", "Fenced", "CutOver", "Published", "Resumed", "Finalized", "RollingBack", "RolledBack"}`, `paused` (`[Instances -> BOOLEAN]`), `reachable` (`[Instances -> BOOLEAN]`, flipped by `Partition(i)` and `Heal(i)`), `routesTo` (`[Instances -> {"Src", "Dst"}]`), `srcFenced`, `acked`, `sagaStep` (survives `SagaCrash`, which restarts the saga from its recorded step).
- **Actions:** the seven steps of §31 §6.4 (`StartCopy`, `CatchUp`, `PauseAll` — only reachable instances pause; the saga waits for every reachable one and proceeds past unreachable ones after `PauseTimeout` —, `FenceSource`, `CutOverDesignated`, `PublishAndReload`, `ResumeAll`), `Finalize`, `RollBack` (the reverse path, allowed until `Finalize`), `StreamForward`, `StreamReverse`, `ClientWrite(i, k)` (to `routesTo[i]`; refused on a fenced source; queued while `paused[i]`), `Partition(i)`, `Heal(i)`, `SagaCrash`.
- **Invariants:** `TypeOK`; `NoLostWrite` (every acked write is in the store that owns the key after streams drain: `Dst` after `Published`, `Src` after `RolledBack`); `NoDuplicateEffect` (each write id occurs at most once per store; upserts make re-delivery idempotent, modelled as set union); `SingleWriterRange` (no state in which both `Src` and `Dst` accept client writes for a key); `ReverseSafe` (after `RollBack`, every write acked after `CutOver` is in `Src`).
- **Liveness:** with `WF` on the saga's steps and on `Heal`, `<>(phase \in {"Finalized", "RolledBack"})`.

**Bounds:** `Small`: `Keys = {k1}`, `Instances = {d, i2}` with `Designated = d`, `MaxWrites = 3`, `FenceEnabled = TRUE`, `SagaCrashes = 1`. `NoFence`: `Small` with `FenceEnabled = FALSE`, `expect = "violation:SingleWriterRange"` (an unreachable instance keeps writing to `Src` after the switch). `CrashSaga`: `SagaCrashes = 2`. `Nightly`: two keys, three instances, `MaxWrites = 4`.

**Tests:** `check.sh ReshardCutover` passes `Small` and `CrashSaga` and fails `NoFence` as expected; Apalache checks `SingleWriterRange` to length 10.

**Commit:** `spec: model the multi-instance cutover with a backend fence`.

### Task 4: Skeletons of the other specs

**Files:** `spec/tla/router/{CrossShardCommit.tla,PrimaryFailover.tla,RouterSession.tla}`, `specs.toml` entries with `parse_only = true`, README.

**Produces:** each skeleton declares its constants, variables, the full list of actions with their enabling conditions written as comments only where the behaviour is not yet fixed, the invariants and properties of §31 §11.2 as named definitions, and the header's action-to-code table with `code = "RT2"` or `"RT4"` placeholders. `CrossShardCommit` models the PgDog variant's phases (`LogPhase1`, `Prepare(p)`, `LogPhase2`, `CommitPrepared(p)`, `Done`, `CoordinatorCrash(keepLog)`, `Recover`, `ParticipantCrash(p)`, `MonitorScan`). `PrimaryFailover` cites `neon/safekeeper/spec/ProposerAcceptorStatic.tla` for the acceptor half and adds the lease and the record CAS. `RouterSession` is a header with the contract only (Q312).

**Tests:** `check.sh <Spec> --parse-only` (SANY) for each; the action-coverage list parses (RT1 Task 8 turns it into a code test).

**Commit:** `spec: add skeletons of the commit, failover and session specs`.

### Task 5: The compatibility inventory, Postgres half

**Files:** `scripts/router/inventory/{pg-static.sh,pg-capture.sh,compose.pg.yml,README.md}`, `crates/loams-compat/{Cargo.toml,src/{main.rs,replay.rs,classify.rs,tsv.rs},tests/it/main.rs}`, `conformance/router/{README.md,pgdog-loampg-statements.tsv,pgdog-loampg-suites.tsv}`.

**Produces:**
- `pg-static.sh $PGDOG_SRC`: greps `pgdog/src/backend/{replication,schema,pool}/`, `pgdog/src/frontend/client/query_engine/two_pc/` and `pgdog/src/healthcheck.rs` for SQL string literals and replication commands (`START_REPLICATION`, `CREATE_REPLICATION_SLOT`, `IDENTIFY_SYSTEM`, `PREPARE TRANSACTION`, `COMMIT PREPARED`, `ROLLBACK PREPARED`, `pg_prepared_xacts`, `pg_is_in_recovery`, `pg_current_wal_lsn`, `COPY … (FORMAT BINARY)`, `pg_dump` invocations) and prints `source_path:line<TAB>statement-kind` rows, **never the line's text**. A human classifies each kind into a digest row.
- `compose.pg.yml`: `postgres:17.11` ×3 (`shard0`, `shard1`, `ref`) with `shared_preload_libraries = pg_stat_statements`, `log_statement = all`, `max_prepared_transactions = 16`, `wal_level = logical`; PgDog v0.1.60 by digest with a two-shard config rendered by hand for this task (RT1 renders it from code).
- `pg-capture.sh`: starts the compose file, runs PgDog's integration scenarios `resharding`, `logical`, `failover`, `pgbench`, `rewrite` and the 2PC tests from `$PGDOG_SRC/integration/` against it (run, not copied), then dumps `pg_stat_statements` (query text normalized by Postgres, calls, rows) from every shard into `capture.jsonl` under `$HOME/.cache/loam/inventory/pg/<date>/`.
- `compat-replay` binary:

  ```text
  compat-replay --engine postgres|mysql --reference <url> --target <url> --input capture.jsonl --out statements.tsv
  ```

  For each digest it runs the captured example (session `SET`s first) on both URLs in a fresh connection, in a transaction rolled back at the end unless the statement is a transaction or replication command (those run in a scratch database per statement), and writes one TSV row: `digest, component, source, example, class, ref_hash, target_hash, note, issue`. `class ∈ {same, differs, error, unsupported, pending-target}`; `unsupported` needs a non-empty `note`.
- `conformance/router/README.md`: the method (§31 §15), the column meanings, how to re-run, and the rule that a pin bump re-runs the inventory.

**Semantics:** the target is a Loam Postgres compute from `deploy/neon` when P2b is merged (Task 0); otherwise every row is `pending-target` with the reference result recorded. `component` is one of `pool`, `health`, `schema-sync`, `copy`, `replication`, `2pc`, `query`.

**Tests:** `crates/loams-compat/tests/it`: `classify_same_differs_error` (fixtures over two in-process mock connectors: identical rows, a different row, an error), `tsv_round_trip`, `unsupported_needs_note`; a CI-less manual run whose TSVs are committed, with the run's date and pins in the README.

**PR size:** the tool and scripts (about 600 lines) in one PR, the blessed TSVs in a second.

**Commit:** `compat: inventory the statements PgDog sends to Postgres`.

### Task 6: The compatibility inventory, MySQL half

**Files:** `scripts/router/inventory/{vitess-static.sh,vitess-capture.sh,compose.vitess.yml}`, `conformance/router/{vitess-wesql-statements.tsv,vitess-wesql-suites.tsv}`, README.

**Produces:**
- `vitess-static.sh $VITESS_SRC` over the paths of §31 §15 step 1, printing `source_path:line<TAB>statement` (Vitess is Apache-2.0, so statement text may be recorded with the path).
- `compose.vitess.yml`: `etcd:v3.7.2`; `mysql:8.0.46` as the reference with `gtid_mode = ON`, `enforce_gtid_consistency = ON`, `binlog_format = ROW`, `binlog_row_image = FULL`, `performance_schema = ON`, the semi-sync source plugin enabled (for the 2PC rows); the WeSQL image from `deploy/wesql/` with the same settings plus WeSQL's bucket settings against a RustFS container; for each, one unmanaged vttablet (`--unmanaged --db-host … --init-keyspace commerce --init-shard 0`, later `-80`/`80-` for the two-shard run), vtctld and vtgate at v24.0.4.
- `vitess-capture.sh`: runs, against the reference, the scenario list `create keyspace and VSchema`, `vtgate DML and SELECT corpus` (Vitess's `go/vt/vtgate/planbuilder/testdata/*_cases.json` queries that are valid on the test schema, executed through vtgate), `MoveTables` (create, `SwitchTraffic`, `ReverseTraffic`, `Complete`), `Reshard 0 → -80,80-`, `VDiff`, `2PC with transaction_mode=twopc`, `OnlineDDL vitess strategy` (one `ALTER`), `PlannedReparentShard` skipped (unmanaged) and `TabletExternallyReparented`; then dumps `performance_schema.events_statements_summary_by_digest` (`DIGEST`, `DIGEST_TEXT`, `QUERY_SAMPLE_TEXT`, `COUNT_STAR`, `SCHEMA_NAME`) into `capture.jsonl`.
- `compat-replay --engine mysql` against the reference and WeSQL produces `vitess-wesql-statements.tsv`; running the same scenario list through vttablet in front of WeSQL produces `vitess-wesql-suites.tsv` (one row per scenario step: `pass`, `fail`, `skip`, with the inventory rows it touches).

**Semantics:** §31 §9.2's items C-1 to C-7 each get at least one row, with the observation recorded (for C-1: the engine the `_vt` tables end up in with `serverless_honor_innodb_engine` off and on, and whether vttablet issues an `ALTER … ENGINE` on its second start). `component` is one of `query`, `health`, `schema-engine`, `sidecar`, `vreplication`, `vdiff`, `2pc`, `onlineddl`, `reparent`.

**Tests:** `compat-replay`'s MySQL connector gets `mysql_classify_same_differs_error` against two in-process fixtures; the manual run's TSVs and the scenario log are committed with the pins.

**PR size:** compose and scripts in one PR, TSVs in a second; about 500 lines plus data.

**Commit:** `compat: inventory what Vitess v24 asks of MySQL and replay it on WeSQL`.

### Task 7: `loams-sqlrouter`: records, ranges, hashes and the machine seam

**Files:** `crates/loams-sqlrouter/{Cargo.toml,clippy.toml,src/*,tests/it/*,tests/fixtures/*}`, `scripts/router/gen-pg-hash-vectors.sh`, root `Cargo.toml` (workspace member and dependencies).

**Produces:**

```rust
// record.rs: §31 §6.1, exactly
pub const RECORD_FORMAT: u8 = 1;
pub struct ShardMapRecord { pub version: u64, pub generation: u64, pub engine: Engine, pub router: RouterKind,
    pub scheme: Scheme, pub shards: Vec<ShardEntry>, pub state: MapState }
impl ShardMapRecord {
    pub fn encode(&self) -> Vec<u8>;                                 // [RECORD_FORMAT] ++ postcard
    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError>;        // UnknownFormat(u8), Corrupt
    pub fn validate(&self) -> Result<(), RecordError>;               // shard count matches the scheme; ranges partition
    pub fn shard_for(&self, key: &KeyValue) -> Result<u32, RecordError>;
}
// ranges.rs
pub type KeyspaceId = u64;                                           // Vitess 8-byte ids, big-endian order = numeric order
pub struct KeyRange { pub lo: KeyspaceId, pub hi: Option<KeyspaceId> }   // [lo, hi)
pub fn parse_vitess_shard(name: &str) -> Result<KeyRange, RangeError>;   // "-80", "80-", "40-80", "-" ; case-insensitive hex
pub fn vitess_shard_name(r: &KeyRange) -> String;
pub fn validate_partition(ranges: &[KeyRange]) -> Result<(), PartitionError>; // Gap{at}, Overlap{at}, NotSorted, Empty
pub fn lookup(ranges: &[KeyRange], id: KeyspaceId) -> usize;         // requires a valid partition
pub fn split(ranges: &[KeyRange], index: usize, at: KeyspaceId) -> Result<Vec<KeyRange>, PartitionError>;
pub fn merge(ranges: &[KeyRange], index: usize) -> Result<Vec<KeyRange>, PartitionError>; // index with index+1
// hash/pg.rs: ported from PostgreSQL (notice in the header)
pub const HASH_PARTITION_SEED: u64 = 0x7A5B_2236_7996_DCFD;
pub fn hashint8extended(v: i64, seed: u64) -> u64;
pub fn hash_bytes_extended(bytes: &[u8], seed: u64) -> u64;
pub fn hash_combine64(a: u64, b: u64) -> u64;
pub fn pg_partition_index(key: &PgKey, modulus: u32) -> u32;          // what Postgres hash partitioning picks
// hash/vitess.rs
pub fn vitess_hash(id: u64) -> KeyspaceId;                           // null-key DES, big-endian
pub fn vitess_unhash(ksid: KeyspaceId) -> u64;
pub fn vitess_xxhash(bytes: &[u8]) -> KeyspaceId;
// machine.rs and trace.rs: §31 §7.1, exactly (Ctx, Machine, TraceSink, SpecEvent, SpecValue, Millis)
```

**Semantics:**
- `pg_partition_index` reproduces Postgres's `compute_partition_hash_value` for one key column (`hash_combine64(0, hashextended(value, HASH_PARTITION_SEED))`, then `% modulus`) for `int8`, `int4` (Postgres hashes `int4` with `hashint4extended`; included), `text` and `uuid`; other types return `RecordError::UnsupportedKeyType`.
- `vitess_hash` encrypts the big-endian bytes of `id` with DES under an all-zero key (`vitess/go/vt/vtgate/vindexes/hash.go`, `vhash`).
- `clippy.toml`: `disallowed-methods = ["std::time::Instant::now", "std::time::SystemTime::now", "std::thread::spawn", "std::env::var", "rand::rng", "rand::thread_rng"]`.

**Tests** (`tests/it`): `record_round_trips` (proptest), `unknown_format_byte_is_refused`, `validate_rejects_gap_and_overlap`; `vitess_shard_names_parse_and_print` (`-80`, `80-`, `40-80`, `-`); `split_then_merge_is_identity` (proptest), `lookup_is_total_and_unique` (proptest over random valid partitions); `pg_hash_matches_postgres_partitioning` (fixture `pg_hash_vectors.json`: 2 000 keys per type, moduli 2, 3, 4, 8, 16, generated by `gen-pg-hash-vectors.sh` from `postgres:17.11` by creating `PARTITION BY HASH` tables and reading `tableoid`); `vitess_hash_matches_vitess_vectors` (the vectors of Vitess's `hash_test.go`, with notice); `vitess_unhash_inverts`; `kernel_crate_has_no_io_dependencies` (reads `Cargo.toml`); `trace_sink_records_in_order`.

**Commit:** `router: add the sqlrouter kernel crate with records, ranges and hashes`.

### Task 8: The Lean project, the partition proofs and the oracle

**Files:** `spec/lean/{lakefile.lean,lean-toolchain,lake-manifest.json,Main.lean}`, `spec/lean/LoamsRouter/{KeyRange.lean,ShardFn.lean,Oracle.lean,Json.lean}`, `crates/loams-sqlrouter/tests/it/lean_oracle.rs`, `.github/workflows/ci.yml` (job `lean`).

**Produces:**
- `KeyRange.lean`: `abbrev KsId := Fin (2^64)`; `structure KeyRange where lo : Nat; hi : Option Nat`; `def IsPartition (rs : List KeyRange) : Prop` (non-empty, first `lo = 0`, each `hi = some (next lo)`, last `hi = none`, every `lo < hi`); `def lookup`; theorems `lookup_total_unique : IsPartition rs → id < 2^64 → ∃! i, i < rs.length ∧ inRange (rs.get i) id`, `split_preserves : IsPartition rs → validSplit rs i at → IsPartition (split rs i at)`, `merge_preserves`.
- `ShardFn.lean`: `shardOfModulo (n : Nat) (h : Nat) : Fin n` for `n > 0`, `modulo_partition`; `shardOfRange` through `lookup`; `shardOf_deterministic`.
- `Oracle.lean` and `Main.lean`: the `loams-router-oracle` executable reads JSON lines `{"op":"partition_check","ranges":[[lo,hi|null],…]}`, `{"op":"lookup","ranges":…,"id":n}`, `{"op":"split",…}`, `{"op":"merge",…}` and writes `{"ok":true,"result":…}` or `{"ok":false,"error":"Gap"|"Overlap"|"NotSorted"|"Empty","at":n}` per line, with the same error vocabulary as `PartitionError`.
- Plausible properties for split and merge on random partitions (`#test` in the modules, run by `lake test`).
- CI job `lean` (path-filtered on `spec/lean/**`, `crates/loams-sqlrouter/src/ranges.rs` and `crates/loams-sqlrouter/src/reference/**`, listed as three separate patterns): elan with `lean-toolchain`, cache `spec/lean/.lake` keyed by `lean-toolchain` and `lake-manifest.json`, `lake build`, `lake test`, then `cargo test -p loams-sqlrouter --test it lean_oracle` with the oracle on `PATH`.

**Tests:** `lake build` (the theorems are checked by building); `lake test`; `partition_matches_lean_oracle` (10 000 random cases, valid and invalid partitions, lookups, splits and merges; Rust and Lean must agree on result or error kind), skipped with `skipped: needs loams-router-oracle` when the binary is absent.

**Commit:** `spec: prove the key-range partition lemmas in Lean and wire the oracle`.

### Task 9: Docs and the RT0 exit

**Files:** `docs/design/31-loams-router-and-verification.md` (as-built notes: tool versions, bounds and run times, the inventory's headline numbers per class and component, C-1–C-7 observations), `docs/plans/README.md` (RT0 row status), `CHANGELOG.md`.

**Exit criteria:**
- `tla` job green: `ShardMap` and `ReshardCutover` pass at PR bounds and fail their unsafe variants as expected; the three skeletons parse.
- `lean` job green: the partition theorems build; 10 000 oracle cases agree.
- The inventory TSVs exist for both halves; every row is classified or `pending-target`; C-1–C-7 have observations.
- `cargo test -p loams-sqlrouter`, `cargo clippy -p loams-sqlrouter` (with its `clippy.toml`) and `cargo deny check` clean.
- Answers or updated wording recorded for Q304 (from Task 6's C-1 row) and Q308 (the `lean` job's measured time).

**Commit:** `docs: record RT0 as built and close RT0`.

## Rulings made during execution

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| E1 | **CommunityModules is not pinned in RT0.** Its 2026 releases (checked: `202609120237`) need TLC 1.8 (`NoClassDefFoundError: tlc2/value/impl/KSubsetValue` on 1.7.4), and no RT0 spec uses it | Ruling 1 keeps TLC 1.7.4; only RT1's trace validation needs the `Json` module | RT1 pins a dated TLC 1.8 build and a matching CommunityModules release by SHA-256 |
| E2 | **Apalache runs bounded checks in RT0** (`SingleWriter` to length 8 in 79 s, `SingleWriterRange` to length 10 in 14 s), not an inductive proof | A bounded check catches the same counterexamples at these bounds; writing a correct `IndInv` for `ShardMap` is real work that RT1 does with trace validation | Until RT1, `SingleWriter` is verified only to the bounds; a bug beyond them would be found by the nightly TLC run or not at all |
| E3 | **Shards accept any write unless fenced**: the specs do not give shards ownership knowledge, unlike the plan's wording of `ClientWrite` ("the shard accepts iff not fenced, it owns `k` …") | Postgres and WeSQL do not know the shard map; giving the model's shards that knowledge would make `SingleWriter` hold trivially | None: this is the stronger model |
| E4 | **`CaughtUp(s)` is computed from the data**, not a variable; `CopyKey(k)` replaces `CopyKeys(s, K)` | A write accepted after a copy must un-catch the shard; a derived predicate cannot drift | None |
| E5 | **`ReshardCutover` uses strong fairness on the saga's steps**; `CutOverDesignated` needs the designated instance reachable and paused | Under weak fairness TLC finds a run where the designated instance's partition flaps forever; the first model also let the saga switch an unreachable instance, which the real saga cannot do | If production partitions flap for longer than the pause timeout allows, the saga stalls and alerts, which is the intended behaviour |
| E6 | **`UnsafeConfigMap` expects `violation:SingleWriter`** (the plan allowed `NoStrayWrite` or `ConfigMapSafe`), and `NoFence`'s shortest counterexample is on the rollback path | TLC's shortest trace violates `SingleWriter` first; both are failures for the declared reason (no fence) | None |
| E7 | **The `ShardMap` nightly keeps two keys** (3 instances, 3 generations, 4 writes) | Three keys and three instances passed 37 million distinct states without finishing locally, and its disk queue filled a RAM `/tmp` | Three-key interleavings are covered only by Apalache's bounded runs |
| E8 | **TLC's working directories live under `~/.cache/loam/spec-work/`** | TLC spills its state queue to the metadir; on a tmpfs `/tmp` a nightly run can exhaust memory | None |
| E10 | **Lean without Plausible.** `lake test` runs `loams-router-proptest`, a seeded random driver (2 000 cases by default; `LOAMS_PROPTEST_CASES`) over the executable definitions; the package is `LoamsRouter` and the oracle `loams-router-oracle` (the product rename, D400) | Plausible's derive support for the structures here was not needed: the properties that matter are *proved* (`validate_iff`, `partition_total_unique`, `split_partition`, `merge_partition`, `shardOfRange_total`), and the random driver only exercises the executable code the oracle runs. No dependency to pin | If Lean-side shrinking is wanted later, add Plausible then |
| E11 | **The Lean theorems are stated over `validate`, the same algorithm as Rust's `validate_partition`**, and `validate_iff` proves it accepts exactly the partitions; the plan's `lookup_total_unique` is stated as "the filter of ranges containing `id` has length 1" | Ties the proofs to the code the oracle compares, instead of to a separate Prop the Rust never computes | None |
| E12 | **`KeyRange` is `{ lo: u64, hi: Option<u64> }`**, as this plan says, not §31 §6.1's `[u8; 8]` pair | Big-endian byte order is numeric order, so the two are equivalent; `u64` makes the arithmetic and the Lean oracle direct | None |
| E13 | **`lookup` returns `Option<usize>`**, and `split`/`merge` return a separate `EditError` (`BadIndex`, `NotInside`, `NotAdjacent { index }`) instead of `PartitionError` | Mirrors Lean's `lookup : … → Option Nat` and `split`/`merge : … → Option …` one to one; a bad edit is not a partition defect | None |
| E14 | **The Postgres port is the little-endian path of `hash_bytes_extended`**, with the PostgreSQL License notice in `hash/pg.rs` | PostgreSQL reads 32-bit words in host order; every server Loams runs is little-endian (x86-64, ARM64) | On a big-endian Postgres the hashes differ; out of scope |
| E15 | **`pg_hash_vectors.json` holds 2 000 int8, int4 and uuid keys and 1 915 distinct text keys** (the generator's lengths 0–40 repeat some prefixes), each with its remainder for moduli 2, 3, 4, 8 and 16 from `postgres:17.11`: 38 000+ checks, all equal | Ruling 4's ground truth, generated by `scripts/router/gen-pg-hash-vectors.sh` (docker or podman) | None |
| E16 | **The oracle generator draws half its lists from cut points below 16 and plants empty ranges.** A first version agreed with Lean on 10 000 cases while missing a planted `<` for `<=` bug in `validate_partition`; the strengthened one catches it at case 108 | A differential test is only as good as the inputs that reach the boundaries | None |
| E9 | **Task 0 facts (2026-10-02):** §29 merged (D273–D280); PgDog latest v0.1.60 (2026-09-24), still AGPL-3.0; Vitess latest v24.0.4 (2026-10-01); Lean v4.34.1 still the latest stable; TLC 1.7.4 `936a2620…0e88`, Apalache 0.62.3 `14482cc9…850e` (matches its release `sha256sum.txt`) | Recorded by command in this PR | — |
| E17 | **Tasks 5 and 6 as built (run of 2026-10-02).** Crate `loams-compat` (a `lib.rs` beside `main.rs` so the tests can reach the modules). Pins: `postgres:17.11`, PgDog v0.1.60 (`ghcr.io/pgdogdev/pgdog@sha256:25d19088…2f266`), `mysql:8.0.46` (exists), `vitess/lite:v24.0.4`, `etcd:v3.7.2`, WeSQL `apecloud/wesql-server:8.0.35-0.1.0_beta5.40` (the image `deploy/wesql/` runs; the fork build was not made). Postgres half: 548 rows, all `pending-target` (no Loams Postgres compute; Ruling 7): `query` 151, `schema-sync` 290, `copy` 54, `replication` 17, `pool` 21, `2pc` 10, `health` 5; 137 have a reference error. MySQL half: 1 981 rows, `same` 1 822, `differs` 158, `unsupported` 1, `error` 0: `query` 1 263, `vreplication` 193, `schema-engine` 145, `onlineddl` 108, `health` 64, `sidecar` 57, `vdiff` 56, `reparent` 49, `2pc` 46 | The plan asked for these numbers | None |
| E18 | **PgDog's scenarios that ran:** `baseline` (ours), `pgbench`, `schema_sync`, `data_sync`, `two_pc` pass; `resharding` fails by timeout at 20 minutes (its own limit is 16) under the pgbench load; `rewrite`, `logical`, `failover` are `skip`. The PgDog binary is taken out of the pinned image because the 2PC scenario kills PgDog | A `podman run` wrapper cannot be killed like a process | None |
| E19 | **PgDog's own SQL and fixtures stay out of `conformance/`.** The merge drops DDL in the `pgdog` schema and every function, procedure and trigger body (588 statements; one canonical row stands for them), strips comments, and folds replicated-schema DDL (to 3 per kind and scenario) and workload statements (12 per kind and scenario) | Ruling D318: captured statements are allowed, PgDog source and test fixtures are not | The table holds fewer statement shapes than a full capture |
| E20 | **Vitess 2PC needs a Unix socket to MySQL** (`dt_executor.go`); the unmanaged vttablets connect through a socket volume shared with the backend. WeSQL beta5.40 has no semi-sync plugins (keyspace durability policy `none`), fails the 2PC transaction with a deadlock at `start_commit`, refuses `SERIALIZABLE`, and puts the sidecar tables in SmartEngine; vttablet issues no `ALTER … ENGINE` on restart (C-1, C-2, C-4) | Ruling 6's unmanaged mode, observed | D302's one-vttablet-per-primary deployment needs the socket; Q304 gets the C-1 answer |
| E21 | **Three backends, not one, for the two-shard run**, and the `-80`/`80-` tablets start just before the Reshard | The sidecar database is per backend, and vtgate cannot rebuild a keyspace whose overlapping shards all serve | None |
| E22 | **Dependency follow-up #311:** use rand_core 0.10 Rng in Ctx and rand 0.10 ChaCha8Rng only in router dev tests. Retain rand_chacha 0.9 solely as the legacy seeded-stream oracle. | The kernel has no external Ctx callers yet. This fits a small isolated upgrade without changing workspace-wide rand 0.9 clients; compare 64 draws for three seeds against the prior generator to protect determinism. | New router drivers must supply a rand_core 0.10 generator. |
