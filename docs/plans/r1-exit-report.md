# R1 exit report: TiKV metastore and the reactive core

Date: 2026-09-28. Branch `r1-t16`, stacked on `r1-t14` (PR #97). R1 plan: [`2026-09-27-r1-reactive-core.md`](2026-09-27-r1-reactive-core.md), Task 16; its rows T16-1 to T16-14 record the rulings made here.

**Status: R1's local gates pass on the as-built tree; the nightly gate is pending, and there are two gaps.** The reactive checker, the transaction checker and the nemesis passed in local runs: 60 s and 30 s per PR, and 180 s under the nemesis on one TiKV store. The metastore conformance suite and fault matrix on TiKV passed as of Tasks 5–6. **Pending:** the nightly `tikv-nemesis` job (30-minute checkers, one store on the standard runner per owner ruling T17-2) has not run yet, so the nightly gate is not shown. The gaps are:
- **Task 15** (TiDB SQL beside Live) is **not done: it is parked** until the owner decides about TiDB. The question is open.
- **PR #80** (the `loam` fork of `tikv-client`) **is not in this branch.** So the re-measurement "with the fork's read-path lock resolution" (Task 16 semantics 5) measured the upstream pin `ab4be1c` again. It must be repeated once #80 lands.

All measurements below ran on the owner's build machine. It is not quiet: other agents' builds and a second TiKV playground (`loam-rtd`, port offset 20000) ran at the same time, with a load average of 8–10. The Loam playground `loam-t16` was dedicated: one PD and one TiKV at port offset 17000, and nothing else used it. Treat absolute times as indicative.

## Gates

| Gate | Result | Where |
|---|---|---|
| Reactive checker, 60 s per PR (4 sessions, 4 writers, random faults) | **pass**: 1 544 Transitions, 7 509 fresh comparisons, 40 resumes, 55 `ModifyQuerySet`s, 2 323 mutations, 373 faults injected, 0 missed invalidations | `crates/operon-live/tests/reactive_checker.rs` `reactive_checker` |
| Reactive checker under the nemesis, 180 s | **pass**: 4 080 Transitions, 24 306 comparisons, 115 resumes (0 fallbacks to a fresh session), 141 modifies, 5 451 mutations, 925 faults, 6 nemesis faults, **TSO client rebuilds 1** | same, with `scripts/tikv/nemesis.sh` |
| `checker_catches_injected_stale_result` | **pass**: the checker fails within 1.4 s once the server drops journal batches | same file |
| Transaction checker, 30 s per PR (list-append, 8 workers, 8 keys, faults) | **pass**: 501 transactions (499 committed), 676 reruns, 102 faults, 466/520/488 ww/wr/rw edges, **no anomaly** (G0, G1a–c, lost update, G-single and G2 all absent; also no lost or duplicated append) | `crates/operon-live/tests/txn_checker.rs` `txn_checker` |
| Transaction checker under the nemesis, 180 s | **pass** in three nemesis runs: 1 223, 935 and 1 844 transactions, up to 68 of unknown outcome, 0 anomalies | same |
| `checker_catches_injected_lost_update` | **pass**: the broken build (appends that write a list read before their transaction) shows `LostUpdate`, `LostAppend` and `IncompatibleOrder` | same |
| Point-read write skew | **pass**: 25 rounds × 8 pairs, 0 pairs where both mutations committed | `point_read_write_skew_never_happens` |
| Elle checker unit tests | 12 synthetic histories (a clean one, and one for each of G0, G1a, G1b, G1c, lost update, G-single, G2, a lost append, and duplicate/incompatible/internal/garbage reads), plus a 2 000-transaction history in well under 5 s | `operon_live::testing::elle` |
| Metastore conformance (53 cases, linearizability histories) and TiKV fault matrix | pass as of Tasks 5 and 6 (rows T5-*, T6-*); not re-run here (unchanged code) | `crates/operon-meta-tikv/tests/` |

**How the nemesis converged.** The first two nemesis runs found two bugs in the checker itself, not in Live (rows T16-6 and T16-7):
1. A retryable `UNAVAILABLE` result that the server published during a TiKV outage was reported as a wrong answer. Row T11-8 makes it a valid per-query result.
2. A resumed client kept the result of a query whose removal the old stream never delivered.

The third run passed with both fixes.

### `commit_mode` per component

| Component | Mode | Why |
|---|---|---|
| Metastore (`operon-meta-tikv`) | `TwoPc` (`COMMIT_MODE`, every write) | Row T6-5: a crashed async-commit writer blocks readers until GC, because the pinned client does not resolve those locks on reads. Owner ruling T7-1 |
| Live mutations (`RunnerOptions::commit_mode`) | `TwoPc` | Owner ruling T7-1 |
| Checkers | `TwoPc` (the defaults above) | Ruling 3 asked the checkers to run with the default; the default is now `TwoPc` |

`Async1pc` stays a switch. It flips back by ruling once the pinned `tikv-client` resolves async-commit and 1PC locks on the read path (tikv/client-rust #565, or the fork of PR #80).

## The nemesis (`scripts/tikv/nemesis.sh`)

The nemesis runs a command (the two checkers at once) and injects one fault every `--interval` seconds, in round-robin order:
1. `tikv-kill`: SIGKILL a TiKV store, then start it again from its own command line and data.
2. `pd-kill`: the same for the PD leader.
3. `pd-stall`: SIGSTOP the PD leader for 15 s, which is past the client's 5 s request timeout, then SIGCONT (owner ruling T2-17).
4. `live-pause`: SIGSTOP the command's processes for 5 s. The Live server runs inside the checker's test binary.

The command gets `OPERON_CHECKER_NEMESIS=1`. It also gets `OPERON_NEMESIS_EXPECT_REBUILD=1`, so the reactive checker fails unless the Live server's TSO supervisor rebuilt its client.

Local run (one store; 3 GB of RAM free with another playground up): three runs of 180 s, each with 5–6 faults (two TiKV kills, one PD kill, one PD stall, one pause). **The PD stall killed the real TSO stream, and the supervisor rebuilt the server's client once (`client_rebuilds: 1`).** Later runs and the final sync succeeded. The checker's own handle needed no rebuild; its requests recovered on the old stream.

The nightly CI job `tikv-nemesis` runs on 3 stores, with 30-minute checkers and a 60 s interval. It has not run yet. Carry: a GitHub runner with 7 GB of RAM may not fit three TiKV stores; if so, drop to `--stores 1` or use a larger runner.

## Measurements

### Tick latency (Task 16 semantics 5, owner ruling T12-1)

The load was `mutations_under_contention_complete_within_the_default_budget`: 32 writers, 2 000 inserts, 64 shards, and the tailer reading at `OPERON_TEST_TICK_READ_LAG_MS`. The client was `tikv-client` at the upstream pin `ab4be1c`, **without #80's fork**. The playground was dedicated to this run, but the machine was loaded.

| Tick read lag | Runs | p50 | p99 | max |
|---|---|---|---|---|
| 0 | 6 (1 failed a mutation) | 0.28–0.81 s | 0.68–4.14 s | 0.74–6.12 s |
| 50 ms | 2 | 0.56–0.58 s | 0.96–1.13 s | 1.01–1.80 s |
| **200 ms (default)** | 4 | **9.5–14.7 ms** | 38–556 ms | 0.54–1.31 s |

The lag-0 failure was one mutation out of 2 000 in a 45 s run; its error was not captured (row T16-9). In all runs the rerun rate was 0.25–0.28 per mutation, with at most 5–7 attempts.

**Ruling on the default: it stays 200 ms** (owner ruling T13-1). Without the read lag, ticks still wait on in-flight two-phase-commit locks at the shard heads, as in rows T11-3 and T12-1. Whether the fork's read-path lock resolution removes that wait is still unmeasured, because #80 is not in this branch. Carry: re-measure at lag 0 once #80 lands.

### Commit latency (the spike's interleaved method; not a quiet machine)

| Operation | Mode | p50 | p99 | max | Runs |
|---|---|---|---|---|---|
| Live `_system:insert` (one writer, 300 per mode, interleaved) | `two_pc` | 38–49 ms | 0.16–1.13 s | 0.71–3.99 s | 2 |
| Live `_system:insert` | `async_1pc` | 27–32 ms | 0.09–1.08 s | 0.29–1.58 s | 2 |
| Metastore `commit_wal` (one chunk, 300 sequential) | `two_pc` (fixed) | 57–75 ms | 157–296 ms | 0.63–0.67 s | 2 |

Async commit cut Live's p50 by about 30%, which matches the spike's 30–50%. The absolute times are 5–10 times the spike's, because the host was loaded. Tests: `commit_latency_interleaved` and `commit_wal_latency`, which run only with `OPERON_TEST_LATENCY=1`. Carry: repeat on a quiet machine.

### Journal shard conflicts (Q31, Ruling 4)

At 64 shards, with 32 writers and 2 000 mutations, the rerun rate was **0.25–0.28 per mutation**, with at most 5–7 of 16 attempts. That matches T11-1's 0.24–0.30, where 16 shards gave 0.94–0.96.

Under the checkers' own load, the rates were:
- the reactive checker: 134–397 restarts per 2 300–5 500 mutations, with faults;
- the transaction checker: about 1.1–1.35 reruns per transaction, on 8 deliberately hot keys.

### Playground RAM

This task did not re-measure RAM. The spike's figure stands: about 3.2 GB peak for a one-store playground, of which TiKV is 2.6 GB at startup (row R15). The local nemesis ran one store because only 3–4 GB was free beside another agent's playground.

## Runtime model (T13-5, owner ruling T14-1)

Each pooled QuickJS context has its own runtime, on its own worker thread. The pool holds `contexts` of them, 4 by default, set with `--live-js-contexts 1–256`. The memory limit (64 MiB) is therefore per call, so one deployment can use up to `contexts` × 64 MiB. R2 revisits one shared runtime per deployment, using rquickjs's `parallel` feature.

This task (the review of #97) also bounds the copy of a function's result:
- at 2^21 parts;
- at **64 MiB of strings and buffers**, which is new;
- a sparse array's length no longer reserves memory beyond the parts left.

A result getter that loops is reported as `FUNCTION_TIMEOUT`.

## Q32 and Q33

- **Q32** was answered in Task 0 (rows R5–R7): PD and TiKV v8.5.8 have no keyspace-level GC, so Loam runs cluster-wide GC. Still open: whether `client-rust` accepts the patch that exposes GC safe points. It is part of #564 (the public proto modules), which is still open.
- **Q33**: keyspace-mode TiDB v8.5.8 was verified in the spike (row R8). Task 15, which would have exercised it beside Live, is parked.

## Upstream `tikv-client` PRs (checked 2026-09-28)

| PR | Subject | State |
|---|---|---|
| [tikv/client-rust#563](https://github.com/tikv/client-rust/pull/563) | pd: reopen the TSO stream after it fails | open |
| [#564](https://github.com/tikv/client-rust/pull/564) | proto: make the generated kvproto modules public | open |
| [#565](https://github.com/tikv/client-rust/pull/565) | transaction: resolve expired async-commit locks on the read path (owner ruling T7-1) | open |
| [#566](https://github.com/tikv/client-rust/pull/566) | transaction: bound async-commit commit ts and handle TiKV's fallbacks | open |

The resonatehq PRs are unrelated to R1. [dina-kar/operon#80](https://github.com/dina-kar/operon/pull/80) (pin the `loam` fork that carries all four) is still open against `r1-t10` and is not in this stack.

## Task 15: not done, parked

TiDB SQL beside Live in the dev playground waits for the owner's decision about TiDB. The question is open. `playground.sh --with-tidb` and `deploy/tikv/tidb.toml` exist from Task 1. `sql_coexistence.rs`, `sql-smoke.sh` and the `operon dev` TiDB line were not written.

## Carries

- Repeat the tick-latency (lag 0 against 200 ms) and commit-latency measurements once #80 is in the stack, and on a quiet machine. Then decide whether `tick_read_lag` can drop and whether `Async1pc` can come back.
- Capture the error of the lag-0 contention run that failed one mutation (row T16-9).
- The first `tikv-nemesis` CI run: check the runner's RAM with 3 stores.
- protobuf-es's `fromBinary` and `toJson` drop a map key named `__proto__`. `@operon/live` now keeps the key as an own property on its own side (review of #97), but a round trip through protobuf-es still loses it. This is upstream (bufbuild/protobuf-es).
- R2: design §7.2's shared conformance fixture set, seeded from `session.test.ts`'s Transition scripts, once a second client exists (owner ruling T14-13).
