# Pending log for §31 (Loam Router and verification)

For the integrator. Source: [§31](../31-loam-router-and-verification.md), from `chatdump.md` lines 54–322 ("Loam SQL, Loam Postgres, and Loam Router: Build and Verification Plan"). Reserved ranges: **D300–D329** (used D300–D322) and **Q300–Q329** (used Q300–Q314). Branch `router-verification-design`.

## 1. Decision rows (append to the Decisions table of `13-decision-log.md`)

| ID | Date | Decision | Rationale | Status |
|---|---|---|---|---|
| D300 | 2026-10-01 | **"Loam Router" is Loam's sharding control plane over unmodified, bought routers** (§31 §1, §5 ADR-1/ADR-4): PgDog for Postgres (D236), Vitess for MySQL (D302). Loam builds the shard map, config rendering, cutover and failover orchestration, the in-doubt monitor and the verification program; **no SQL parser, planner or router data path** in RT0–RT5. Replaces the chat dump's "Rust rewrite" and "clean Postgres router" rows | PgDog and Vitess already parse, plan, scatter, merge, aggregate, pool and reshard; the owner prefers buying; DST, the chat dump's reason for Rust, applies to the code Loam writes (D313) | Proposed (Q300) |
| D301 | 2026-10-01 | **The MySQL shard is WeSQL** (§29, D273), not a new "InnoDB semantics on TiKV" engine; "Loam SQL" names the product (Vitess in front of WeSQL shards) (§31 §3) | InnoDB semantics over TiKV is TiDB rebuilt, which D260 rules out at TiDB's cost; WeSQL is a real `mysqld` with a row binlog and GTIDs | Proposed |
| D302 | 2026-10-01 | **Vitess v24.x (Apache-2.0) vtgate and vttablet in unmanaged mode front WeSQL primaries**, unmodified, as separate services, pinned to v24 (the last release supporting MySQL 8.0); adoption gated by the RT3 compatibility gate (§31 §9, §15). Replaces the chat dump's "implement the tablet contract natively" | WeSQL is a real `mysqld`, so the reason for a native tablet disappears; unmanaged tablets exist for externally managed MySQL (vitess.io/docs/24.0) | Proposed |
| D303 | 2026-10-01 | **Shard keys use each router's native scheme** (§31 §5 ADR-5, §6.1): Vitess vindexes (`hash` = null-key DES into keyspace-ID ranges; `xxhash`) for MySQL; PgDog's Postgres-compatible hash (`hashint8extended`, `hash_bytes_extended`, mod n) and range/list mappings for Postgres | Vitess tooling transfers only for Vitess keyspaces; PgDog matches Postgres's own hash partitioning | Proposed |
| D304 | 2026-10-01 | **The shard map record** (§31 §6.1): one postcard record per (namespace, database) in the TiKV metastore (proposed prefix `xs/<ns>/<db>`), CAS on `version`, monotonic routing `generation`; the source of truth for PgDog-routed databases, a mirror of Vitess's topology for Vitess keyspaces | One control plane for both engines without overriding Vitess's own workflows | Proposed |
| D305 | 2026-10-01 | **Multi-instance PgDog cutover is Loam's, with a backend fence** (§31 §6.4): copy and catch up on a designated instance (`RESHARD`), `PAUSE` everywhere, fence the source shards (`ALTER ROLE <app> NOLOGIN` and terminate), `CUTOVER` on the designated instance, publish the next generation and `RELOAD` the others, `RESUME`; a Resonate saga. Safety never depends on reaching every instance | PgDog's open-source `RESHARD` cuts over one instance only; coordinated cutover is its closed Enterprise Edition (`pgdog/docs/RESHARDING.md`) | Proposed (Q306, Q307) |
| D306 | 2026-10-01 | **Cross-shard atomic commit only with a durable coordinator log** (§31 §10): PgDog 2PC off by default; on only with PgDog as a StatefulSet (`NODE_ID` = ordinal, `DEPLOYMENT_ID`, `PGDOG_TWO_PHASE_COMMIT_WAL_DIR` on a volume) and after RT2's gate. MySQL cross-shard writes are non-atomic (`transaction_mode = multi`) until Q303. Never across engines | PgDog's 2PC log is local, unchecksummed, and absent without the variable; Vitess 2PC refuses without semi-sync (`dt_executor.go`) | Proposed |
| D307 | 2026-10-01 | **Loam Postgres becomes a shard by configuration and tests, not engine work** (§31 §8): `max_prepared_transactions` in the compute spec of 2PC databases; durability of prepared transactions, logical slots and the exported-snapshot boundary proven by RT2's tests | The pageserver stores two-phase state (`TWOPHASEDIR_KEY`) and Neon tests it (`test_twophase.py`); slots survive compute replacement (§23 §9.1) | Proposed |
| D308 | 2026-10-01 | **Verification by layer** (§31 §11–§14): TLA+ for protocols Loam builds or orchestrates; Lean 4 for pure kernels and as the cross-shard oracle; DST for Loam's control plane; contract, differential and nemesis tests for engines and routers. Bought routers are verified as black boxes | Proving third-party code is out of reach; their behaviour is testable | Proposed |
| D309 | 2026-10-01 | **The compatibility inventory method** (§31 §15): static extraction, dynamic capture, replay with classification (`same`, `differs`, `error`, `unsupported`, `pending-target`), suite pass rates, blessed TSVs in `conformance/router/`, re-run on every pin bump | Makes "what the router needs from the engine" a versioned spec | Proposed |
| D310 | 2026-10-01 | **TLA+ specs in `spec/tla/router/`**, TLC v1.7.4 and Apalache v0.62.3 pinned by SHA-256, CI job `tla`; variants marked `expect = "violation:<Inv>"` keep unsafe configurations documented and tested (§31 §11.1) | Reproducible model checking; the reason for each rule stays executable | Proposed |
| D311 | 2026-10-01 | **Trace validation links specs to code** (§31 §11.3): machines emit `SpecEvent`s (`tracing` target `loam::spec`); `<Spec>Trace.tla` checks JSON-lines traces from simulation and real runs; an action-coverage test fails on an action with no emitter | Specs drift from code otherwise | Proposed (Q309) |
| D312 | 2026-10-01 | **Lean 4 kernels in `spec/lean/`** (Lake package `LoamRouter`, Lean v4.34.1, Plausible): range partition, shard lookup, k-way merge, `LIMIT`/`OFFSET` pushdown, aggregate decomposition; compiled to the `loam-router-oracle` executable for differential tests and mirrored by `proptest`; hash functions checked by reference vectors only; plan rewrites out of scope (§31 §12) | Proves the semantics the routers must meet; the differential tests connect the model to code | Proposed (Q308) |
| D313 | 2026-10-01 | **Bit-exact DST for sans-I/O control-plane code** (§31 §7.1, §13): machines read time and randomness only from `Ctx`; `operon-detsim` (a new light crate) drives them with models under one seeded `ChaCha8Rng`, with trace hashes, shrinking, fault points and swarm runs. **Amends D28's scope**: D28's seeded simulation stays for the engine; madsim and turmoil are not used | Owned, I/O-free code is deterministic without porting a runtime; madsim (last release 2025-10-11) needs patched tokio crates | Proposed |
| D314 | 2026-10-01 | **Shared checkers** (§31 §13.4): the WGL checker from `operon-meta-conformance`; new bank, unique-key, Elle-style list-append, split and liveness checkers in `operon-detsim::checkers`, the one implementation §20 §14 item 4 also uses (re-exported by `operon-sim`); Elle (EPL-2.0) never a dependency | One checker per property across tracks | Proposed |
| D315 | 2026-10-01 | **A Rust nemesis harness** (`operon-nemesis`, test-only) for real-process fault tests with toxiproxy, kill/pause, `tc netem` and clock skew, running the DST workloads and checkers; Jepsen (EPL-1.0) optional and external; RT5 (§31 §14.3) | Same checkers in simulation and reality; no Clojure stack to own | Proposed (Q311) |
| D316 | 2026-10-01 | **Contract suites against model and reality** (`shard_backend_conformance!`, `router_fleet_conformance!`, `shard_map_store_conformance!`), the `metastore_conformance!` pattern (§31 §14.1) | Simulation models cannot drift silently | Proposed |
| D317 | 2026-10-01 | **A Loam-built Rust router is the recorded fallback** (§31 §5 ADR-1, §7.3), triggered by: a PgDog change config cannot replace and upstream refuses; Vitess failing the RT3 gate on WeSQL with no fork fix; or an unfixed correctness bug in a bought router. Apache-2.0, no PgDog code, Vitess code only with notices; the chat dump's `Frontend`/`Dialect`/`Planner`/`Executor`/`BackendPool` seams specified, not built | Keeps the build option open without paying for it | Proposed |
| D318 | 2026-10-01 | **Licenses** (§31 §16): Apache-2.0 for everything Loam links (D11); Vitess a service, forkable only by a recorded decision; PgDog an unmodified service read only as a reference, no code, text or tests copied into code or specs; PostgreSQL's hash functions ported under the PostgreSQL License, never from PgDog's copy. Answers the chat dump's "router license" row: Apache-2.0 | AGPL §13 and D11 | Proposed |
| D319 | 2026-10-01 | **Track RT (RT0–RT5)** replaces the chat dump's M0–M5 (§31 §17); runs beside M2, R, D and P on the one-build machine and changes no M-track code | Avoids a clash with Loam's M0–M6 | Proposed |
| D320 | 2026-10-01 | **vtgate is the MySQL front end for WeSQL**, sharded or not, replacing §23 §6.3's Loam-built handshake-and-splice proxy (N6); Loam renders vtgate's static auth file and VSchema. **Proposes to amend D153's MySQL half**; the splice is the fallback for unsharded WeSQL only, and sharded MySQL waits for D317 if RT3's gate fails (§31 §9.1) | Buy over build; one MySQL front end for both cases | Proposed (Q313) |
| D321 | 2026-10-01 | **The §18 namespace router is unchanged and separate** from the SQL routers; SQL resharding copies rows, the retrieval engine's moves stay metadata-only (§31 §1) | Different products, different data movement | Proposed |
| D322 | 2026-10-01 | **Isolation is promised per shard, never across shards** (§31 §10): Postgres semantics per Loam Postgres shard, D274's per WeSQL shard; cross-shard reads may be fractured; no global snapshot; checkers check exactly this | Honest promises the tests can hold | Proposed |

## 2. Open-question rows (append to the Open questions table)

| # | Question | Owner | Needed by |
|---|---|---|---|
| Q300 | Accept D300 (buy PgDog and Vitess; build only the control plane and the evidence) over the chat dump's Loam-built Rust router with MySQL and Postgres frontends (§31 §5) | Founder | RT1 plan start |
| Q301 | Vitess topology on a dedicated etcd (proposed) or on PD's embedded etcd (support for external etcd v3 clients not verified) (§31 §4.1) | Eng | RT3 plan |
| Q302 | WeSQL's rebase to MySQL 8.4 before Vitess v24's support ends (estimate: about 2027-04); joint with §29 Q274 (§31 §9.2 C-3) | Founder, Eng | RT3 plan |
| Q303 | Atomic MySQL cross-shard commits: a semi-sync replica so Vitess 2PC is allowed, or no MySQL cross-shard atomicity (§31 §9.2 C-2, §10) | Eng, Founder | RT4 plan |
| Q304 | Vitess `_vt` sidecar tables on SmartEngine or InnoDB (`serverless_honor_innodb_engine`), and whether vttablet's sidecar diff loops on the engine (§31 §9.2 C-1) | Eng | RT3 Task 0 |
| Q305 | When sharded Loam Postgres through PgDog leaves beta (§31 §18, risk 1) | Founder | RT2 exit |
| Q306 | Does `CUTOVER` on one PgDog plus `RELOAD` of the rendered swap on the others give identical routing, without PgDog's proprietary Enterprise Edition (§31 §6.4) | Eng | RT4 plan |
| Q307 | The Postgres fence: `ALTER ROLE <app> NOLOGIN` plus termination (proposed), or per-table write revokes (§31 §6.4) | Eng | RT1 Task 0 |
| Q308 | The Lean CI job on every relevant PR (proposed) or nightly only (§31 §12.3) | Eng | RT0 Task 8 |
| Q309 | Trace validation with TLC and the trace-spec method (proposed), Apalache, or a Rust port of each spec's `Next` (§31 §11.3) | Eng | RT1 Task 8 |
| Q310 | The nightly DST seed budget (1 000 000 proposed) against runner minutes (§31 §13.5) | Eng | RT1 exit |
| Q311 | The nemesis harness in Rust (proposed) or Jepsen as an external tool (§31 §14.3) | Eng | RT5 plan |
| Q312 | Keep `RouterSession` (a black-box contract of bought routers) or drop it (§31 §11.2) | Eng | RT4 plan |
| Q313 | vtgate in front of unsharded WeSQL too (D320), retiring §23's N6 splice (§31 §9.1) | Founder | RT3 plan |
| Q314 | Narrow Q260 to analytics: OLTP MySQL wire access is vtgate in front of WeSQL (§29, D320); whether Loam also serves read-only MySQL wire over DataFusion stays open (§31 §19) | Founder | With Q273 |

## 3. Rows for the READMEs and §12

### 3.1 `docs/plans/README.md`: a new section after "Track SC"

```markdown
## Track RT: the Loam Router and verification (parallel to M2, R, D and P)

Design reference: [31 Loam Router and verification](../design/31-loam-router-and-verification.md) (D300–D322). Track RT reconciles the chat dump's "Loam SQL, Loam Postgres, and Loam Router" plan with D260, D236, §28 and §29: Loam builds the sharding control plane (shard map, rendering, cutover with a backend fence, in-doubt monitor) over unmodified PgDog (Postgres) and Vitess v24 (MySQL, in front of WeSQL), and the evidence (TLA+, Lean 4, deterministic simulation, contract, differential and nemesis tests). The chat dump's M0–M5 are RT0–RT5 (D319). RT adds crates and CI jobs and changes no M-track code.

| Plan | Scope | Depends on | Status |
|---|---|---|---|
| [RT0: Router foundations, specs and the compatibility inventory](2026-10-01-rt0-foundations-and-specs.md) | `spec/tla/router` with `ShardMap` and `ReshardCutover` checked and skeletons of the others, the `tla` job; the compatibility inventory for PgDog→Postgres and Vitess v24→MySQL 8.0.46, replayed on WeSQL and Loam Postgres; `operon-sqlrouter` (records, ranges, Postgres and Vitess hashes with reference vectors, the sans-I/O `Machine` seam and its lints); the Lean project with the partition proofs, the oracle and the `lean` job | — | Planned |
| [RT1: Postgres single-shard slice and the deterministic simulator](2026-10-01-rt1-postgres-slice-and-sim.md) | `operon-detsim` (bit-exact scheduler, network model, fault points, shrinking); `ShardMapStore` on TiKV; PgDog rendering and admin adapter; the Postgres backend adapter; models with shared contract suites; the `ConfigPush` machine; trace validation of `ShardMap`; the `shardmap` DST scenario and `router-sim` job; the compose stack with a single-shard differential | RT0; §28 P3 for Loam Postgres computes (Postgres 17.11 until then) | Planned |
| [RT2: Scatter, merge and aggregate with the Lean oracle, Postgres 2PC and the change stream](2026-10-01-rt2-scatter-oracle-2pc.md) | Lean merge, limit and aggregate theorems and the oracle; the three-way cross-shard differential through PgDog; prepared transactions on Loam Postgres; PgDog 2PC under the durable-log rule; `CrossShardCommit` checked; the in-doubt monitor and the `two_phase` scenario; the change stream and the exact snapshot boundary; a split verified by checksums | RT1; §28 P2b for the Loam Postgres tests | Planned |
| RT3 | Vitess v24 with WeSQL: dynamic inventory against WeSQL and the RT3 gate; vtgate as the MySQL front end (D320); VSchema and auth rendering; unsharded then two-shard keyspaces; MySQL differential; Vitess end-to-end subset pass rates | RT0 inventory; §29 W2; Q301–Q304, Q313 | Not yet planned |
| RT4 | `PrimaryFailover` (Arm A and W3) and the Vitess 2PC variant checked; the multi-instance cutover orchestrator (D305) with trace validation; the full fault catalog in DST; the `RouterSession` contract; Q303 and Q306 decided | RT2, RT3; §28 P4c; §29 W3 | Not yet planned |
| RT5 | Resharding end to end on real clusters (PgDog across instances, Vitess `Reshard`); `operon-nemesis` (D315); performance baselines; the published compatibility matrix | RT4 | Not yet planned |
```

### 3.2 `docs/design/README.md`: a row in "Reading order" after row 30 (or after 29 if 30 is absent)

```markdown
| 31 | [Loam Router and verification](31-loam-router-and-verification.md) | Sharded Loam SQL and Loam Postgres: WeSQL as the MySQL shard (not InnoDB-on-TiKV, D260); Loam's sharding control plane over unmodified PgDog and Vitess v24 (shard map record, rendering, multi-instance cutover with a Postgres fence, the in-doubt monitor); cross-shard promises; TLA+ specs with trace validation, Lean 4 kernels and the differential oracle, bit-exact simulation of sans-I/O machines with a fault catalog, contract and nemesis tests; the compatibility inventory method; track RT (D300–D322) | **Proposed** |
```

### 3.3 `docs/design/12-roadmap-testing-risks.md`

**§1 Milestones, a row after "R"** (and after any other parallel-track rows):

```markdown
| **RT** | Loam Router and verification (§31), parallel track | RT0: specs (`ShardMap`, `ReshardCutover`), the compatibility inventory, `operon-sqlrouter` kernels, Lean partition proofs. RT1: the deterministic simulator (`operon-detsim`), the shard map on TiKV, PgDog rendering and adapters, trace validation, single-shard routing through PgDog. RT2: Lean merge/limit/aggregate and the oracle, the cross-shard differential, Postgres 2PC under D306, the in-doubt monitor, the change stream and a checked split. RT3: Vitess v24 with WeSQL (D302, D320). RT4: failover and commit specs, the multi-instance cutover, the full fault catalog. RT5: live resharding, nemesis runs, the compatibility matrix | Per phase in §31 §17; RT1 needs §28 P3, RT2 §28 P2b, RT3 §29 W2, RT4 §29 W3 and §28 P4c |
```

**§2 Testing strategy, amend item 1** (append): "*Amended 2026-10-01 (D313, §31 §13):* Loam's router control plane is written as sans-I/O machines and is simulated **bit-exactly** by `operon-detsim` (one seeded RNG, simulated time, network and fault models, trace hashes, shrinking, swarm runs): 2 000 fresh seeds per scenario plus the regression corpus on every PR that touches it, 1 000 000 nightly. D28's seeded simulation stays for the engine."

**§2 Testing strategy, a new item 13 at the end of the list:**

```markdown
13. **Formal specifications and kernels (from RT0, D308–D312):** TLA+ specs of the protocols Loam builds or orchestrates (`ShardMap`, `ReshardCutover`, `CrossShardCommit`, `PrimaryFailover`; `RouterSession` optional) in `spec/tla/router`, checked by TLC and Apalache on every PR that touches them, with expected-violation variants for unsafe configurations; trace validation of simulation and real-run traces against the specs, and a test that every spec action has a code emitter; Lean 4 proofs of the pure kernels (key-range partition, merge, `LIMIT`/`OFFSET`, aggregate decomposition) compiled into an oracle that the Rust mirror and the routers' cross-shard results are diffed against; compatibility inventories of what each bought router asks of its engine (`conformance/router/`, D309).
```

**§2 item 6 (Differential testing), append a bullet:** "Sharded SQL (§31 §14.2): the same SQL stream on an unsharded engine and through PgDog (2 and 4 shards) or vtgate, with the Lean oracle as the third opinion for merges and aggregates; documented router deviations live in an allowlist with their sources."

**§2 item 7 (Jepsen-style tests), append a bullet:** "Sharded SQL (RT5, D315): `operon-nemesis` runs the simulator's workloads and checkers (bank, list-append, split, liveness) against PgDog, Vitess, Loam Postgres, WeSQL, TiKV and RustFS with kills, pauses, partitions and clock skew."

**§3 Risk register, new rows** (numbers continue the table):

```markdown
| — | PgDog's v0.1.x maturity for sharded production databases (§31 row 1) | Medium | High | Differential and nemesis tests; beta until Q305; unsharded fallbacks |
| — | Vitess on WeSQL fails the compatibility gate, or Vitess drops MySQL 8.0 support after v24 (§31 rows 2–3) | Medium | High | RT3 inventory first; fixes in the WeSQL fork; pin v24; Q302 rebase to 8.4; D317 fallback |
| — | Specs or simulation models drift from code and engines (§31 rows 6–7) | Medium | Medium | Trace validation, the action-coverage test, contract suites against model and reality |
```

## 4. Conflicts with existing decisions

| # | Where | Chat dump (or §31) says | Existing decision | Proposed resolution |
|---|---|---|---|---|
| 1 | Chat §1, §4 | Loam SQL = MySQL engine with InnoDB semantics on TiKV | **D260** (no TiDB anywhere), **Q260** (MySQL wire open), §29 D273 (WeSQL proposed) | Rejected: the MySQL shard is WeSQL (**D301**). Q314 narrows Q260 to analytics |
| 2 | Chat §2 row 1, §6.3 | Rust router rewrite with MySQL and Postgres frontends and a shared planner | **D236** (PgDog unmodified routes Postgres); owner's buy-over-build preference | **D300**: buy PgDog and Vitess; Loam builds the control plane. **D317** records when to build a router. Owner decision **Q300** |
| 3 | Chat §2 row 4 | Clean Postgres router, PgDog as design reference | **D236** | D236 stands unchanged; PgDog is configured, never patched |
| 4 | Chat §2 row 2, §4 | Implement the vttablet contract natively | None directly; §29 (WeSQL is a real `mysqld`) | **D302**: real vttablet in unmanaged mode |
| 5 | Chat §2 row 3 | Router license AGPL or Apache | **D11** | Apache-2.0 (**D318**) |
| 6 | Chat §2 row 5 | Vitess-compatible hashing for both engines | None; PgDog's Postgres-compatible hashing (D236's router) | **D303**: native per router |
| 7 | Chat §7.1 | madsim/turmoil deterministic runtime, bit-exact replay | **D28** (seeded, not bit-exact, for the engine) | **D313** amends D28's scope: bit-exact for sans-I/O control-plane machines only; D28 unchanged for the engine |
| 8 | Chat §7.3 | Elle-style checker | §20 §14 item 4 (an Elle-style checker in `operon-sim`'s checker module) | **D314**: one implementation in `operon-detsim::checkers`, re-exported by `operon-sim`; §20 §14 item 4's wording updated when R-track work reaches it |
| 9 | Chat §5 | Build a logical change stream, durable prepared transactions, health endpoints, snapshot copy for Loam Postgres | §28 (Neon fork; D231), §23 §9.1 spike | **D307**: Neon already has them; configuration and tests only |
| 10 | §31 D320 | vtgate fronts WeSQL | **D153** MySQL half (§23 §6.3, N6: Loam's handshake-and-splice proxy) | Proposed amendment of D153's MySQL half; the splice stays the fallback for unsharded WeSQL only, sharded MySQL waits for D317 (Q313). A note is added to §23 §6.3 |
| 11 | §31 §9.1 | Failover repoint via `TabletExternallyReparented` | §29 §7.2 (PR #172): "the router follows the record (§23 §6.3, D153)" | Amend §29 §7.2's sentence once PR #172 merges; not edited here because §29 is not on `main` |
| 12 | §31 D306 | PgDog 2PC allowed for SQL databases under a rule | §18 §5.8 ("which is why Loam avoids cross-shard atomicity") | No conflict: §18 concerns the retrieval engine's metadata; clarified in §18 §5.8 with a note |
| 13 | §31 D321 | SQL resharding copies rows | §18 §5.5 ("Loam never copies data") | No conflict: scoped to the retrieval engine |
| 14 | §31 §9.1 | `gtid_mode = ON` for Vitess-fronted WeSQL | §23 §6.4 (the spike ran `gtid_mode = OFF`; the bridge reads `file:pos`) | `gtid_mode = ON` for Vitess-fronted WeSQL, as §29 W2 already requires; noted in §23 §6.3 |
| 15 | Chat §9 | Milestones M0–M5 | Loam's M0–M6 (§12) | Renamed RT0–RT5 (**D319**). Note: the `Q-RT-*` question ids of §24–§25 belong to the runtime track F, not to track RT |
| 16 | Chat §8 | Jepsen on real clusters | None (Jepsen is EPL-1.0) | **D315**: a Rust nemesis harness; Jepsen optional and external |
| 17 | Chat timelines (weeks) | 12–16 weeks per phase | Loam sizes work in PRs | Not adopted; §31 §17 and the plans size phases in PRs |
| 18 | D2, D130 | Sharded OLTP | OLTP out of scope for the retrieval engine | As §23 and §28: separate services beside the engine; no change |

## 5. Edits made on this branch to existing design docs

All are one-paragraph notes marked "Proposed 2026-10-01" with §31's D-numbers; none changes a decision.

- `docs/design/18-metastore-backends-and-router.md` §5.8: the PgDog bullet notes D306 and D321.
- `docs/design/20-reactive-database-on-tikv.md` §10: a note on Q314 above the D260 note.
- `docs/design/23-neon-and-wesql.md` §6.3: a note on D320 before the MySQL paragraph.
- `docs/design/28-loam-postgres.md` §8: a note on D304–D307 at the top of the section.

## 6. Later phases (not yet planned)

RT3, RT4 and RT5 are rows in §3.1 above with status "Not yet planned". Their scope and dependencies are in §31 §17.
