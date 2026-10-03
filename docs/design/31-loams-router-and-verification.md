# 31 — Loams Router and Verification: Sharded Loams SQL and Loams Postgres, Specified, Simulated and Tested

Status: **Approved** (owner defaults, 2026-10-02: "do suggested for all") · 2026-10-01. This document folds the chat-dump plan "Loams SQL, Loams Postgres, and Loams Router: Build and Verification Plan" (`chatdump.md`, lines 54–322, owner request of 2026-10-01) into Loams's design, and reconciles it with what has been decided since that plan was written: D260 (no TiDB), D236 (PgDog, unmodified), §28 (Loams Postgres, Arm A) and the pending §29 (WeSQL as the MySQL OLTP engine, PR #172, D273–D280). Decisions are **D300–D322** and open questions **Q300–Q314**, all approved or answered by the owner on 2026-10-02 ("do suggested for all"). It changes no code; the work is track **RT** (RT0–RT5, §17), with plans [RT0](../plans/2026-10-01-rt0-foundations-and-specs.md), [RT1](../plans/2026-10-01-rt1-postgres-slice-and-sim.md) and [RT2](../plans/2026-10-01-rt2-scatter-oracle-2pc.md).

The chat dump's own milestones M0–M5 are renamed **RT0–RT5**, so they do not clash with Loams's M0–M6 (D319).

Markers, as in §28 and §29:

- **(verify)** means not checked against a primary source, or checked only by reading code that was not run. The task that depends on it checks it first.
- **(estimate)** means computed or reasoned, not measured.
- **(source)** means read in the source on 2026-10-01, at the revisions below.
- Paths of the form `pgdog/…` point into `pgdogdev/pgdog` at `80d6059d` (`v0.1.60-30`, 2026-10-01), read as a reference only (AGPL-3.0, D236). `vitess/…` points into `vitessio/vitess` `main` read through the GitHub API on 2026-10-01 (latest release v24.0.4, 2026-10-01). `neon/…` points into `neondatabase/neon` at `fa504217c`, the base of `dina-kar/neon` (§28). `wesql/…` points into `ostrium-labs/wesql` branch `8.0` at `eef34f452` (§29).

---

## 1. Summary

| # | Decision | Status |
|---|---|---|
| D300 | **"Loams Router" is Loams's sharding control plane over unmodified, bought routers**: PgDog for Postgres (D236) and Vitess for MySQL (D302). Loams builds the shard map, config rendering, cutover and failover orchestration, the in-doubt monitor and the verification program. **Loams builds no SQL parser, planner or router data path** in RT0–RT5. Replaces the chat dump's ADR rows "Rust rewrite" and "clean Postgres router" (§5, ADR-1, ADR-4) | Approved (owner defaults, 2026-10-02) |
| D301 | **The MySQL shard is WeSQL** (§29, D273), not a new "InnoDB semantics on TiKV" engine. That engine would be TiDB rebuilt, which D260 rules out, at TiDB's cost. "Loams SQL" names the product (Vitess in front of WeSQL shards), not an engine | Approved (owner defaults, 2026-10-02) |
| D302 | **Vitess v24.x (Apache-2.0) vtgate and vttablet in unmanaged mode** front WeSQL primaries, unmodified, as separate Go services. Pinned to v24, the last Vitess release that supports MySQL 8.0 (WeSQL is 8.0.46); Vitess v25+ needs WeSQL on 8.4 (Q302). Adoption is gated by the RT3 compatibility gate (§15). Replaces the chat dump's "implement the tablet contract natively" (ADR-2) | Approved (owner defaults, 2026-10-02) |
| D303 | **Shard keys use each router's native scheme.** MySQL: Vitess vindexes (`hash`, null-key DES of the 64-bit id into keyspace-ID ranges; `xxhash`). Postgres: PgDog's Postgres-compatible hash (`hashint8extended`, `hash_bytes_extended`) modulo the shard count, and its range and list mappings. Loams's shard map represents both (§6.1). Replaces the chat dump's "Vitess-compatible hashing everywhere" (ADR-5) | Approved (owner defaults, 2026-10-02) |
| D304 | **The shard map record** (§6.1): one record per (namespace, database) in the TiKV metastore, versioned for compare-and-set, with a monotonic routing **generation**. It is the source of truth for PgDog-routed databases. For Vitess keyspaces, Vitess's topology is authoritative for its own workflows and Loams's record mirrors it | Approved (owner defaults, 2026-10-02) |
| D305 | **Cutover across many PgDog instances is Loams's job, with a backend fence** (§6.4). PgDog's open-source `RESHARD` cuts over one instance only (`pgdog/docs/RESHARDING.md`; coordinated cutover is its closed Enterprise Edition). Loams pauses every instance, fences writes on the source shards in Postgres itself, cuts over, reloads every instance, and resumes, as a Resonate saga. Safety never depends on reaching every PgDog instance | Approved (owner defaults, 2026-10-02) |
| D306 | **Cross-shard atomic commit only where its coordinator log is durable** (§10). Postgres: PgDog 2PC (`two_phase_commit`) is off by default, and may be enabled for a database only with PgDog as a StatefulSet (`NODE_ID` = ordinal, `DEPLOYMENT_ID` set, `PGDOG_TWO_PHASE_COMMIT_WAL_DIR` on a persistent volume) and after RT2's gate. MySQL: Vitess's 2PC requires semi-sync, which WS2's quorum is not, so MySQL cross-shard writes are non-atomic (`transaction_mode = multi`) until Q303 is answered. **Never a transaction across engines** | Approved (owner defaults, 2026-10-02) |
| D307 | **Loams Postgres needs configuration and verification, not engine work, to be a shard** (§8). Prepared transactions are stored by the pageserver (`neon/pageserver/src/pgdatadir_mapping.rs`, `TWOPHASEDIR_KEY`; `neon/test_runner/regress/test_twophase.py`), logical slots survive a compute replacement (§23 §9.1 spike), and a snapshot-consistent copy uses an exported snapshot. The chat dump's "Phase 2: hardening" becomes tests and compute-spec settings | Approved (owner defaults, 2026-10-02) |
| D308 | **Verification by layer** (§11–§14): TLA+ for the protocols Loams builds or orchestrates; Lean 4 for pure kernels and as the oracle for cross-shard results; deterministic simulation for Loams's control plane; contract, differential and nemesis tests for the real engines and routers. **Bought routers are verified as black boxes**, by their observable behaviour, never by proving their code | Approved (owner defaults, 2026-10-02) |
| D309 | **The compatibility inventory method** (§15): static extraction from the router's source, dynamic capture of the statements it sends to its backend, replay against Loams's engine with classification, and the router's own test suites with a tracked pass rate. Results are blessed tables in `conformance/router/` | Approved (owner defaults, 2026-10-02) |
| D310 | **TLA+ specs live in `spec/tla/router/`**, checked by TLC (MIT) at stated bounds and by Apalache (Apache-2.0) for inductive invariants, in a path-filtered CI job `tla` (§11) | Approved (owner defaults, 2026-10-02) |
| D311 | **Specs are linked to code by trace validation** (§11.3): Loams's control-plane machines emit structured events (`tracing` target `loams::spec`) that a trace spec checks against the protocol spec, on simulation and real runs. Each spec carries an action-to-code table, and a test fails when an action has no emitter | Approved (owner defaults, 2026-10-02) |
| D312 | **Lean 4 kernels in `spec/lean/`** (Lake package `LoamsRouter`, Lean v4.34.x, Plausible): range partition, shard lookup, k-way merge, `LIMIT`/`OFFSET` pushdown, aggregate decomposition. They compile to an executable oracle used by differential tests, mirrored by `proptest` properties in Rust. Hash functions are checked by reference vectors, not in Lean. Plan rewrites are out of scope (no Loams planner) (§12) | Approved (owner defaults, 2026-10-02) |
| D313 | **Bit-exact deterministic simulation for sans-I/O control-plane code** (§13). Loams's router control plane is written as state machines that do no I/O and read time and randomness only from their inputs; `loams-detsim` drives them with models of PgDog, Vitess, shards and the metastore under one seeded RNG, with replay, trace hashes, shrinking, fault points and swarm runs. **Amends D28's scope**: D28's seeded simulation stays for the engine; madsim and turmoil are not used | Approved (owner defaults, 2026-10-02) |
| D314 | **Checkers are shared, not duplicated** (§13.4): the Wing–Gong–Lowe checker from `loams-meta-conformance` (D28, M1.2a Ruling 14); new bank-conservation, unique-key, Elle-style list-append and liveness checkers in `loams-detsim::checkers`, the one implementation §20 §14 item 4 also uses. Elle (EPL-2.0) is never a dependency | Approved (owner defaults, 2026-10-02) |
| D315 | **Real-system fault tests use a Rust nemesis harness** (`loams-nemesis`, test-only) with toxiproxy, container kill and pause, `tc netem` and clock skew, running the simulator's workloads and checkers. Jepsen (Clojure, EPL-1.0) may be run as an external tool for cross-checks only. Planned in RT5 | Approved (owner defaults, 2026-10-02) |
| D316 | **One contract suite per seam runs against both the model and the real system** (`shard_backend_conformance!`, `router_fleet_conformance!`), the pattern of `metastore_conformance!`, so simulation models cannot drift from the engines silently (§14.1) | Approved (owner defaults, 2026-10-02) |
| D317 | **A Loams-built Rust router is the recorded fallback, with named triggers** (§5, ADR-1): a PgDog change that config cannot replace and upstream refuses (D236 forbids patching); Vitess failing the RT3 gate on WeSQL with no fix in the WeSQL fork; or a correctness bug in a bought router that upstream will not fix. The fallback would be Apache-2.0, take no PgDog code, and may port Vitess code with its notices. The seams of §7.3 keep it possible | Approved (owner defaults, 2026-10-02) |
| D318 | **Licenses** (§16): Loams's own code stays Apache-2.0, and every third-party crate it links must carry a permissive license (D11); no copyleft or source-available code is linked, and `xxhash-rust` (Boost, BSL-1.0) is the recorded permissive non-Apache case (§16). Vitess is Apache-2.0 and runs as a service (it may be forked under D302's rule); PgDog stays an unmodified service read only as a reference (D236), and no PgDog text, code or test is copied into Loams's code or specs. The chat dump's "router license: AGPL or Apache" question is answered: Apache-2.0 | Approved (owner defaults, 2026-10-02) |
| D319 | **Track RT** (RT0–RT5) replaces the chat dump's M0–M5 (§17). RT runs beside M2 and the R, D and P tracks on the one-build machine, and adds crates without changing M-track code | Approved (owner defaults, 2026-10-02) |
| D320 | **vtgate is the MySQL front end for WeSQL**, sharded or not, instead of the Loams-built handshake-and-splice proxy of §23 §6.3 (N6). Loams renders vtgate's static auth file and VSchema as it renders PgDog's files. **Proposes to amend D153's MySQL half**. If RT3's gate fails, the Loams splice is the fallback for **unsharded** WeSQL only; sharded MySQL is then unavailable until D317's Rust router exists | Approved (owner defaults, 2026-10-02) |
| D321 | **The §18 namespace router is unchanged and separate.** It places namespaces and resources of the retrieval engine and never copies data (§18 §5.5). The SQL routers sit outside the engine; moving SQL data between shards copies rows (PgDog's logical replication, Vitess's VReplication) | Approved (owner defaults, 2026-10-02) |
| D322 | **Isolation is promised per shard, never across shards** (§10): Postgres semantics on each Loams Postgres shard, §29 D274's semantics on each WeSQL shard; cross-shard reads may be fractured (Vitess documents this for its 2PC); no global snapshot. Every checker checks exactly these promises | Approved (owner defaults, 2026-10-02) |

## 2. Goals and non-goals

### 2.1 Goals

1. **Sharded MySQL and Postgres for apps that outgrow one primary**, on engines Loams already runs: Loams Postgres (§28) and WeSQL (§29).
2. **One shard map and one control plane** for both engines: placement, routing generations, cutover, failover and in-doubt monitoring, driven from Loams's metastore and Resonate sagas.
3. **Buy the routers.** PgDog and Vitess already parse, plan, route, scatter, merge, aggregate, pool, reshard and (with conditions) commit atomically. Loams builds what neither gives it: multi-instance coordination, a fence in the backend, and evidence.
4. **Evidence, not only tests**, as the chat dump asked: TLA+ for the protocols, Lean 4 for pure kernels, deterministic simulation for Loams's control plane, and fault tests on the real systems.

### 2.2 Non-goals

- **A new MySQL engine** (D301), **a new SQL router** (D300) or **a Vitess tablet re-implementation** (D302), unless D317's triggers fire.
- **Transactions across engines** (kept from the chat dump).
- **Cross-shard joins beyond what the bought routers do** (kept, and now defined by them).
- **Proving PgDog or Vitess correct.** Loams proves its own kernels and protocols, and tests the routers' behaviour.
- **Modifying PgDog** (D236), or **TiKV/PD** (D126).
- **Upstream contributions** without the owner's go-ahead (as §28 and §29).

## 3. Reconciling the chat dump

Each claim of the chat dump that the current design or the sources change, checked on 2026-10-01.

| Chat dump | Finding | Resolution |
|---|---|---|
| "Loams SQL: MySQL-compatible engine (InnoDB semantics on TiKV, binlog to S3)" | InnoDB semantics over a transactional KV store is what TiDB is (Percolator over TiKV, §29 §9). D260 (owner, 2026-09-29) forbids TiDB anywhere, and Q260 left MySQL wire access open. §29 (PR #172) proposes WeSQL, MySQL 8.0.46 with SmartEngine on the bucket, which is a real `mysqld` with a real row binlog and GTIDs (§29 §4, §6) | **D301: WeSQL is the MySQL shard.** A new engine would rebuild TiDB's executor, locking and DDL, years of work, and still not be InnoDB (TiKV has no gap locks, §29 §4.2) |
| "Implement the tablet gRPC contract natively; vttablet assumes a real mysqld" | WeSQL *is* a real `mysqld`, so the reason disappears. Vitess runs **unmanaged tablets** against externally managed MySQL (RDS, Aurora, CloudSQL) with `--unmanaged`, `--db-host`, `--db-port` (vitess.io/docs/24.0 "unmanaged tablet"). Vitess v24 supports MySQL 8.0 and 8.4 and is "the final release with support for MySQL 8.0" (vitess.io/docs/24.0 "supported databases") | **D302: real vttablet, unmanaged, pinned to v24.** The compatibility inventory (§15) is the spec of what WeSQL must answer |
| "Router language: Rust, because deterministic simulation is easier" | DST is easier for code Loams owns. The routers' hard parts (parsing, planning, merging, pooling, resharding) already exist in PgDog (Rust, AGPL-3.0, v0.1.60) and Vitess (Go, Apache-2.0, v24). The owner prefers buying (memory: buy over build) and approved PgDog unmodified (D236) | **D300**: no Loams-built router; DST applies to Loams's control plane (D313). **D317** records when to build one |
| "Postgres router: clean implementation, pgdog as design reference only" | D236 already decided: PgDog runs unmodified as a service. PgDog has sharding (hash, list, range, schema), cross-shard queries with partial aggregates, 2PC and online resharding (`pgdog/docs/SHARDING.md`, `RESHARDING.md`) | **D300 + D236**: PgDog routes. Loams adds multi-instance cutover (D305), which PgDog's open-source build lacks |
| "Router license: AGPL 3.0 vs Apache 2.0, decide with counsel" | D11: no AGPL in what Loams links. PgDog-derived code would make Loams's code AGPL | **D318**: Apache-2.0; no PgDog code |
| "Vitess-compatible `hash` and range keyspace IDs for both engines" | PgDog shards by Postgres's own partition hash modulo N (`pgdog/docs/SHARDING.md`, "the same shard as PostgreSQL's own hash partitioning would"); Vitess by keyspace-ID ranges (`vitess/go/vt/vtgate/vindexes/hash.go`, null-key DES) | **D303**: native scheme per router; one shard-map type for both |
| "Phase 2: build a logical change stream, prepared transactions with durable recovery, health endpoints, snapshot copy for Loams Postgres" | Neon stores `pg_twophase` in the pageserver (`TWOPHASEDIR_KEY`) and tests branching with prepared transactions (`test_twophase.py`); logical replication works and slots survive a compute replacement (§23 §9.1, spike); `compute_ctl` reports status | **D307**: configuration and tests; no engine change |
| "Deterministic async runtime (madsim or turmoil); ban wall clock in router crates" | D28 chose a seeded, not bit-exact, simulation for the engine because openraft, redb and `object_store` do real I/O. madsim's last release is 0.2.34 (2025-10-11) and it needs patched tokio-ecosystem crates; turmoil 0.7.2 (2026-04-24, MIT) simulates tokio's network only | **D313**: sans-I/O machines are bit-exact without any runtime port; lints ban clocks and unseeded randomness in the kernel crate; D28 stays for the engine |
| "Elle-style cycle detection" | Elle is EPL-2.0 (`jepsen-io/elle`); §20 §14 item 4 already plans an Elle-style checker in Loams | **D314**: one Loams implementation, shared |
| "Jepsen-style tests on real clusters" | Jepsen is Clojure, EPL-1.0 (`jepsen/project.clj`) | **D315**: a Rust nemesis harness with the same checkers; Jepsen optional and external |
| "Run a subset of Vitess's end-to-end tests against Loams SQL" | Still right, but against vttablet in front of WeSQL, not against a tablet re-implementation | §15, RT3 |
| "Lean: plan rewrites preserve semantics" | Loams has no planner under D300 | Out of scope (D312) |
| Milestones M0–M5 with week estimates | They clash with Loams's M0–M6, and Loams's plans size work in PRs, not weeks | **D319**: RT0–RT5, sized in PRs (§17) |

## 4. Architecture

```
 MySQL clients                                   Postgres clients
     │ mysql://<keyspace>                            │ postgres://…/<db>[__<branch>]
     ▼                                               ▼
 ┌─────────────────────────────┐            ┌─────────────────────────────────┐
 │ vtgate ×N (Vitess v24,      │            │ PgDog ×N (AGPL-3.0, unmodified, │
 │ Apache-2.0, unmodified)     │            │ StatefulSet when 2PC is on)     │
 │ VSchema, vindexes, scatter, │            │ hash/range/list sharding,       │
 │ merge, buffering            │            │ scatter, merge, partial aggs,   │
 └──────────┬──────────────────┘            │ 2PC (opt-in), RESHARD           │
            │ gRPC                          └──────────┬──────────────────────┘
            ▼                                          │ Postgres protocol (TLS)
 ┌─────────────────────────────┐                       ▼
 │ vttablet ×shard (unmanaged) │            ┌─────────────────────────────────┐
 │ query service, VReplication │            │ Loams Postgres computes per shard│
 └──────────┬──────────────────┘            │ (§28: pageserver, Arm A WAL)    │
            ▼                               │ Postgres 17.11 shards in CI     │
 ┌─────────────────────────────┐            └─────────────────────────────────┘
 │ WeSQL primaries + replicas  │
 │ (§29: WS2 quorum, WS3 HA)     │                 ▲ config + RELOAD, PAUSE/RESUME,
 └─────────────────────────────┘                 │ CUTOVER; fence via SQL
            ▲ vtctld gRPC: VSchema, Reshard,     │
            │ TabletExternallyReparented         │
 ┌──────────┴──────────────────────────────────────────────────────────────────┐
 │ Loams router control plane (in `loams`, D300)                                │
 │ loams-sqlrouter (sans-I/O machines: shard map, config push, cutover,         │
 │   in-doubt monitor; kernels: ranges, hashing, merge reference) ── traces ──► │
 │ loams-sqlrouter-io (adapters: TiKV records, PgDog admin, Postgres and        │
 │   WeSQL backends, vtctld, Kubernetes ConfigMaps); sagas on Resonate (§21)    │
 └──────────┬───────────────────────────────────────────────────────────────────┘
            ▼
 TiKV metastore: shard map record (D304), §23 `x/` records, §29 WS3 primary record

 Verification: spec/tla/router (TLC, Apalache) ◄── trace validation ── loams-detsim
               spec/lean (LoamsRouter oracle) ◄── differential tests ── routers' results
               loams-nemesis (RT5) on real processes, same workloads and checkers
```

### 4.1 Components

| Component | Source | License | Role |
|---|---|---|---|
| PgDog | `pgdogdev/pgdog` v0.1.60 (2026-09-24), image by digest | **AGPL-3.0** | Postgres router (D236). Unmodified service |
| Vitess vtgate, vttablet, vtctld | `vitessio/vitess` v24.0.4 (2026-10-01) | Apache-2.0 | MySQL router (D302) |
| etcd | `etcd-io/etcd` v3.7.2 (latest release as of 2026-10-01) | Apache-2.0 | Vitess topology store (Q301) |
| Loams Postgres | `dina-kar/neon` (§28) | Apache-2.0 | Postgres shards |
| WeSQL | `ostrium-labs/wesql` (§29) | GPL-2.0-only | MySQL shards. Separate process (D148) |
| `loams-sqlrouter` | new crate, sans-I/O | Apache-2.0 | Kernels and machines (§7.1) |
| `loams-sqlrouter-io` | new crate | Apache-2.0 | Adapters (§7.2) |
| `loams-detsim` | new test-only crate | Apache-2.0 | Deterministic scheduler and checkers (§13) |
| `loams-nemesis` | new test-only crate (RT5) | Apache-2.0 | Fault harness on real processes (D315) |
| TLC, CommunityModules | `tlaplus/tlaplus` v1.7.4 stable (2024-08-05); v1.8.0 is a rolling pre-release, updated 2026-10-01 | MIT | Model checking, trace validation |
| Apalache | `apalache-mc/apalache` v0.62.3 (2026-10-01) | Apache-2.0 | Symbolic checking |
| Lean 4, Plausible | `leanprover/lean4` v4.34.1 (2026-09-24); `leanprover-community/plausible` | Apache-2.0 | Kernels and oracle |

## 5. The ADRs of the chat dump's §2, decided

The chat dump asked for one ADR per row of its §2 before any code. Each is decided here, with the evidence of §3, and two are added.

**ADR-1. Router language: Go fork of vtgate, or a Rust rewrite.** *Decision:* neither. Loams runs vtgate (Go) and PgDog (Rust) unmodified and writes its control plane in Rust (D300). *Why:* both routers exist, are maintained (Vitess releases monthly; PgDog weekly) and carry years of compatibility work Loams would otherwise repeat. DST, the chat dump's argument for Rust, applies to the code Loams writes (D313), and Loams writes the coordination code. *Consequences:* Loams's evidence about routing correctness is black-box (differential tests, D308); a router bug is fixed upstream or worked around in config. *Revisit when* any trigger of D317 fires.

**ADR-2. Vitess compatibility level: real vttablet in front of the engine, or the tablet contract natively.** *Decision:* real vttablet, unmanaged, v24 (D302). *Why:* the engine is WeSQL, a real `mysqld` with binlog commands, GTIDs and `performance_schema`. *Consequences:* WeSQL's gaps surface as inventory rows (§9.2), not as Loams code; Vitess's MySQL 8.0 support ends after v24, so WeSQL's rebase to 8.4 becomes a dependency (Q302).

**ADR-3. Router license.** *Decision:* Apache-2.0 for everything Loams links (D11, D318). *Consequences:* no PgDog code anywhere; Vitess code may be ported only into an Apache-2.0 fallback with its notices (D317).

**ADR-4. Postgres router path: fork PgDog, or a clean implementation.** *Decision:* neither: PgDog unmodified (D236), with Loams's multi-instance cutover and fence (D305). *Why:* forking PgDog makes Loams's fork AGPL and a network service, which triggers AGPL §13 (§28 §8); a clean implementation duplicates what PgDog does. *Consequences:* PgDog's v0.1.x maturity is a product risk (Q305, risk 1 in this document's §18).

**ADR-5. Shard-key hashing.** *Decision:* native per router (D303). *Why:* Vitess tooling and docs transfer only for Vitess keyspaces; for Postgres, PgDog's choice is Postgres's own hash partitioning, so a sharded table matches a hash-partitioned one. *Consequences:* the shard map has two hash families (§6.1), and the Lean kernel covers both through the same partition lemma (§12).

**ADR-6 (new). The MySQL engine.** *Decision:* WeSQL (D301). *Why:* §3 row 1. *Consequences:* §29's WS1–WS4 become RT3's dependencies for anything beyond single-node durability (§17).

**ADR-7 (new). Simulation scope.** *Decision:* bit-exact DST for sans-I/O control-plane machines; seeded simulation (D28) for the engine; real-process nemesis tests for engines and routers (D313, D315). *Why:* §3 row "deterministic async runtime". *Consequences:* the control plane's machines must not do I/O; the lints of §7.1 enforce it.

## 6. The shard map and the control plane

### 6.1 The record (D304)

One record per mapped database, in the TiKV metastore beside §23's `x/<ns>/<db>` record (the prefix is fixed in RT1 Task 0 against the as-built key layout; proposed `xs/<ns>/<db>`), postcard-encoded, changed only by compare-and-set on `version`:

```rust
pub struct ShardMapRecord {
    pub version: u64,            // CAS version: every write increments it
    pub generation: u64,         // routing generation: increments when routing changes
    pub engine: Engine,          // Postgres | MySql
    pub router: RouterKind,      // PgDog { database: String } | Vitess { keyspace: String }
    pub scheme: Scheme,
    pub shards: Vec<ShardEntry>, // ordered; index = PgDog `shard` number
    pub state: MapState,         // Serving | Resharding { to: u64 } | CuttingOver { step: CutoverStep } | Fenced
}
pub enum Scheme {
    PgHash { column: String, data_type: PgKeyType, shards: u32 },          // hashint8extended / hash_bytes_extended, mod n
    PgRange { column: String, ranges: Vec<(Bound, Bound, u32)> },          // PgDog [[sharded_mappings]] kind = range
    PgList { column: String, lists: Vec<(Vec<Value>, u32)> },
    Vindex { vindex: VindexKind, ranges: Vec<KeyRange> },                  // hash (DES) | xxhash; keyspace-ID ranges
}
pub struct KeyRange { pub lo: [u8; 8], pub hi: Option<[u8; 8]> }           // [lo, hi), hi = None means the end; Vitess "-80", "80-"
pub struct ShardEntry { pub name: String, pub backend: BackendRef, pub fence: FenceState }
```

- **Generation semantics.** A router instance that has applied generation *g* routes by *g*'s shard list. A backend accepts router writes only while its fence state allows them (§6.4). Generations are never reused.
- **Vitess mirror.** For a Vitess keyspace the record mirrors `SrvKeyspace` (read through vtctld) after every Vitess workflow step, so Loams's checkers and the oracle know the current partition. Loams does not write Vitess's topology except through vtctld's API (VSchema apply, workflow commands).

### 6.2 Rendering

- **PgDog** (`pgdog.toml`, `users.toml`): one `[[databases]]` entry per shard (`name = <db>`, `shard = i`, `host`, `port`, `database_name`, `role`), `[[sharded_tables]]` with `column` and `data_type`, and `[[sharded_mappings]]` for range and list schemes. The output is byte-deterministic (golden-file tests), carries the generation in a ConfigMap annotation `loams.dev/router-generation`, and keeps §28 §8's rules (TLS both hops, SCRAM terminated in PgDog, `<db>__<branch>` names). `cutover_save_config = false`: Loams's record, not PgDog's disk, is the truth.
- **Vitess**: VSchema JSON per keyspace (vindexes, tables, sequences), and vtgate's static auth file (`mysql_auth_server_static_file`) rendered from the auth plan's credentials (Q30), as `users.toml` is for PgDog.

### 6.3 Pushing a generation

The `ConfigPush` machine (§7.1) moves every router instance to the record's generation: write the ConfigMap (or the compose file in dev), then `RELOAD` each PgDog instance's admin database, and record each instance's observed generation from the reply and from `SHOW CONFIG` (verify which admin command reports the loaded file). An instance that restarts reads the ConfigMap, so **the ConfigMap is written only when it is safe for any instance to route by it** (§6.4). That rule is the main thing `ShardMap.tla` checks (§11.2).

### 6.4 Cutover with a backend fence (D305)

PgDog's open-source `RESHARD` runs schema sync, parallel binary `COPY`, replication catch-up and a traffic swap on **one** instance (`pgdog/docs/RESHARDING.md`, "Traffic cutover via `RESHARD` is supported on single-node PgDog only"). Loams's orchestration for N instances, as a Resonate saga with deterministic step ids:

1. **Copy and catch up** on a designated instance *d*: `RESHARD <source> <destination> <publication>` against *d*'s admin database; poll `SHOW TASKS` and `SHOW REPLICATION` until lag is under the stop threshold. The destination is a second database entry (`<db>__reshard_<g>`) rendered into every instance at generation *g* but unused by clients.
2. **Pause** `<db>` on every instance (`PAUSE <db>`), so new queries queue (PgDog's own barrier).
3. **Fence the source in Postgres**, which is what makes the cutover safe when an instance is unreachable: on every source shard, `ALTER ROLE <app_role> NOLOGIN`, then `pg_terminate_backend` for that role's sessions. Sending the signal is not the same as the session ending: with no timeout argument `pg_terminate_backend` returns `true` whether or not the backend has actually terminated, and a positive timeout only bounds how long it waits ([PostgreSQL 17 §9.28.2](https://www.postgresql.org/docs/17/functions-admin.html)). Step 3 therefore ends only when `pg_stat_activity` on every source shard shows no backend still logged in as `<app_role>`. A signalled backend can still commit in the window before it acts on the signal, and a `CUTOVER` after that commit drops the write, which is what `ReshardCutover`'s `NoLostWrite` forbids. Any PgDog instance that still routes to a source shard can then no longer complete an operation: checked directly against Postgres, a new connection is refused with SQLSTATE 28000 and a terminated in-flight session ends with 57P01, while a routed query through PgDog surfaces PgDog's own checkout error (58000 in the pinned-image probe) rather than a forwarded SQLSTATE. The guarantee is that a write fails instead of being lost and a stale read fails instead of returning old data, not any particular error code. `default_transaction_read_only` is not a fence: a client's `BEGIN READ WRITE` or `SET` overrides it. The replication role PgDog uses for the reverse stream is a different role and is not fenced (Q307).
4. **Cut over** on *d* (`CUTOVER <task>`), which drains and swaps the two databases' identity in *d*'s routing table and starts the reverse stream (`pgdog/pgdog/src/admin/cutover.rs`, `RESHARDING.md` "Point of no return").
5. **Publish generation *g*+1** in the record (the destination is now `<db>`), write the ConfigMap, and `RELOAD` every other instance.
6. **Resume** `<db>` on every instance.
7. **Finalize or reverse.** Until finalize, the reverse stream keeps the old shards current; a rollback is steps 2–6 in the other direction. Finalize stops the reverse task (`STOP_TASK`), drops the old computes after a grace period, and clears the fence.

An instance unreachable at step 2 or 5 cannot write to the source (step 3) and, if it restarts, reads the ConfigMap that step 5 wrote. Whether `CUTOVER` on *d* plus `RELOAD` elsewhere gives every instance the same routing is the first thing RT4 verifies on a real PgDog (Q306). Vitess keyspaces use Vitess's own `Reshard` and `SwitchTraffic`, which buffer in vtgate and use tablet controls; Loams drives them and mirrors the result (§6.1).

### 6.5 The in-doubt monitor

PgDog names prepared transactions `__pgdog_2pc_[<DEPLOYMENT_ID>_]<instance>_<random>` (`pgdog/pgdog/src/frontend/client/query_engine/two_pc/transaction.rs`), where the instance id is `NODE_ID` or, without it, 8 random hex digits per process (`pgdog/pgdog/src/util.rs`). Its coordinator log is a local WAL directory with no checksums ("We didn't add checksums", `two_pc/wal/README.md`), and without `PGDOG_TWO_PHASE_COMMIT_WAL_DIR` there is no log at all (verify that no default directory applies). So:

- **Deployment rule (D306):** a database with 2PC on runs PgDog as a StatefulSet with `NODE_ID` = the pod ordinal, `DEPLOYMENT_ID = <cluster>-<db>` and the WAL directory on the pod's persistent volume.
- **The monitor** (a sans-I/O machine with a Postgres adapter) lists `pg_prepared_xacts` on every shard every 30 s, groups `__pgdog_2pc_` transactions by instance, and raises an alert for any older than `2pc_in_doubt_alert` (default 5 min) whose instance is gone or restarted. **It never commits or rolls back** a transaction: without the coordinator's log it cannot know the decision, and `CrossShardCommit.tla` shows that guessing violates atomicity (§11.2). An operator resolves with the shard data in view, through a documented runbook.

## 7. Trait seams

### 7.1 Sans-I/O machines (`loams-sqlrouter`)

The kernel crate has no dependency on tokio, sockets, files or clocks. Its `clippy.toml` (read from the crate directory) adds `disallowed-methods` for `std::time::{Instant, SystemTime}::now`, `rand::rng`, `rand::thread_rng`, `std::thread::spawn` and `std::env::var`, and the crate has no `tokio` in its `Cargo.toml`. Every protocol is a machine:

```rust
pub struct Ctx<'a> {
    pub now: Millis,                      // supplied by the driver: real clock or simulated clock
    pub rng: &'a mut dyn rand_core::Rng,   // supplied by the driver: seeded in simulation
    pub trace: &'a mut dyn TraceSink,     // spec events (D311)
}
pub trait Machine {
    type Input;
    type Output;
    /// Consume one input and return the commands to execute. Never blocks, never does I/O.
    fn on(&mut self, ctx: &mut Ctx<'_>, input: Self::Input) -> Vec<Self::Output>;
}
pub trait TraceSink { fn emit(&mut self, event: SpecEvent); }
pub struct SpecEvent { pub spec: &'static str, pub action: &'static str, pub fields: Vec<(&'static str, SpecValue)> }
```

Machines in RT1–RT2: `ConfigPush` (§6.3), `InDoubtMonitor` (§6.5), and the kernels `ranges` (partition, split, merge, lookup), `hash` (Postgres `hashint8extended`/`hash_bytes_extended` ported from PostgreSQL's `src/common/hashfn.c` under the PostgreSQL License with its notice, **never from PgDog's copy**; Vitess `hash` through the `des` crate; `xxhash` through `xxhash-rust`), `render` (PgDog and Vitess files) and `reference` (k-way merge, `LIMIT`/`OFFSET`, aggregate decomposition: the Rust mirror of the Lean oracle, used by checkers and differential tests, never on a routing path). RT4 adds `Cutover` (§6.4) and `FailoverRepoint`.

The chat dump's simulation seams map onto this: **Clock** and **Rng** are `Ctx` fields; **Network**, **Disk** and **Spawn** do not exist inside a machine, because outputs are commands that the driver executes (tokio adapters in production, the scheduler in simulation).

### 7.2 I/O seams (`loams-sqlrouter-io`)

```rust
#[async_trait]
pub trait ShardMapStore: Send + Sync {
    async fn read(&self, db: &DbRef) -> Result<Option<ShardMapRecord>, StoreError>;
    async fn cas(&self, db: &DbRef, expect: Option<u64>, next: &ShardMapRecord) -> Result<u64, StoreError>; // Conflict { current }
    async fn watch(&self, db: &DbRef, after: u64) -> Result<ShardMapRecord, StoreError>;                    // long poll
}
#[async_trait]
pub trait RouterInstance: Send + Sync {
    fn id(&self) -> &InstanceId;
    async fn apply(&self, generation: u64, files: &RenderedFiles) -> Result<Applied, RouterError>;
    async fn pause(&self, database: &str) -> Result<(), RouterError>;
    async fn resume(&self, database: &str) -> Result<(), RouterError>;
    async fn observe(&self) -> Result<Observed, RouterError>; // version, generation, peers, tasks
}
#[async_trait]
pub trait ShardBackend: Send + Sync {
    async fn status(&self) -> Result<BackendStatus, BackendError>;          // role, read-only, lag, LSN/GTID
    async fn fence_writes(&self, role: &str) -> Result<(), BackendError>;   // §6.4 step 3; returns only once no backend of `role` remains
    async fn unfence_writes(&self, role: &str) -> Result<(), BackendError>;
    async fn prepared(&self) -> Result<Vec<PreparedXact>, BackendError>;    // pg_prepared_xacts
    async fn checksum(&self, table: &str, by: &ShardFn) -> Result<TableDigest, BackendError>; // split verification
}
```

Implementations: `ShardMapStore` for memory and TiKV (`loams-tikv`'s `TxnRunner`); `RouterInstance` for PgDog's admin database (tokio-postgres, simple query protocol) and, in RT3, vtgate/vtctld; `ShardBackend` for Postgres (Loams Postgres computes and CNPG) and, in RT3, WeSQL. Each has a simulation model in `loams-detsim` and a contract suite (D316).

### 7.3 Fallback seams (D317), specified and not built

If D317's triggers fire, the chat dump's router architecture applies, behind these traits, sharing §7.1's kernels and §7.2's `ShardMapStore` and `ShardBackend`:

```rust
pub trait Frontend { type Conn; fn accept(&mut self, ctx: &mut Ctx<'_>, bytes: &[u8]) -> Vec<FrontendEvent>; }   // MySQL or Postgres wire, sans-I/O
pub trait Dialect  { fn parse(&self, sql: &str) -> Result<Statement, ParseError>; fn shard_keys(&self, stmt: &Statement, map: &ShardMapRecord) -> Route; }
pub trait Planner  { fn plan(&self, stmt: &Statement, route: &Route) -> Result<PlanIr, PlanError>; }
pub trait Executor: Machine<Input = ExecInput, Output = ExecCommand> {}   // route, scatter, merge, 2PC
pub trait BackendPool { fn checkout(&mut self, ctx: &mut Ctx<'_>, shard: u32, read: bool) -> PoolCommand; }
```

They exist on paper so that RT1's seams do not paint the fallback into a corner.

## 8. Loams Postgres as a shard (D307)

| Shard requirement (chat dump §5) | What Loams Postgres already has | What RT adds |
|---|---|---|
| Prepared transactions with durable recovery | The pageserver stores two-phase state (`neon/pageserver/src/pgdatadir_mapping.rs`: `TWOPHASEDIR_KEY`, `twophase_file_key`, `list_twophase_files`); Neon's regression tests prepare transactions, branch, and commit them on the branch (`neon/test_runner/regress/test_twophase.py`: `test_twophase`, `test_twophase_nonzero_epoch`, `test_twophase_at_wal_segment_start`) **(source)** | `max_prepared_transactions` in the compute spec Loams renders (§28 §5.2, P2b); RT2 tests: a prepared transaction survives a compute restart, a pageserver restart and an Arm A acceptor kill, and `COMMIT PREPARED` works after each |
| Logical change stream | `wal_level = logical`, pgoutput, slots that survive replacing the compute (§23 §9.1, spike); Neon's `test_logical_replication.py` (`test_restart_endpoint`, `test_slots_and_branching`) **(source)** | RT2: a pgoutput reader test over a compute restart, used by the split check and D154's bridge |
| Snapshot-consistent bulk copy with a start LSN | Postgres: `CREATE_REPLICATION_SLOT … LOGICAL pgoutput (SNAPSHOT 'export')` returns the slot's consistent point and a snapshot name for `SET TRANSACTION SNAPSHOT`; PgDog's copy uses a temporary slot inside the copy's transaction (`RESHARDING.md`, Step 3) | RT2: an exact-boundary test under a continuous writer (no row lost or doubled, checksums equal) |
| Health, role and replication status | `pg_is_in_recovery()`, `pg_stat_replication`, `compute_ctl`'s status endpoint | `ShardBackend::status` (§7.2) |
| Failover | Arm A: walproposer terms over `loams-wal` acceptors (D264); Loams's control plane starts computes (D232) | RT4: `PrimaryFailover.tla` covers it; the repoint is a generation push (§6.3) |

## 9. WeSQL as a shard behind Vitess (D301, D302)

### 9.1 Shape

- **One unmanaged vttablet per WeSQL primary** (and per replica, as `replica` tablets, once WS3 exists), with `--unmanaged`, `--db-host`, `--db-port`, the app and DBA users, `--init-keyspace`, `--init-shard`. Vitess does not manage WeSQL's process, backups or replication; Loams (§29) does.
- **Binlog requirements** that VReplication needs, which §29 already sets: `binlog_format = ROW`, `binlog_row_image = FULL`, and `gtid_mode = ON` with `enforce_gtid_consistency` (WS2 requires it, §29 §6.2). The §23 spike ran with `gtid_mode = OFF`, so the default changes for Vitess-fronted WeSQL.
- **Failover**: WS3 promotes a replica (§29 §7.3). Step 5, "repoint", becomes `vtctldclient TabletExternallyReparented <new-primary-tablet>` for the shard, plus the `x/` record change (D320 amends §29 §7.2's "router follows the record").
- **Front end**: vtgate, also for unsharded keyspaces (D320). Database name = keyspace.

### 9.2 What is already known to need checking

| # | Item | Evidence | Where decided |
|---|---|---|---|
| C-1 | **Vitess sidecar tables declare `ENGINE = InnoDB`** (`vitess/go/vt/sidecardb/schema/twopc/{dt_state,redo_state}.sql`, `vreplication/vreplication.sql`). WeSQL by default creates such tables in SmartEngine with a warning (`wesql/mysql-test/suite/smartengine_consistent_snapshot/t/default_storage_engine.test`); with `serverless_honor_innodb_engine = ON` they stay InnoDB on the local volume, outside the bucket. vttablet's sidecar check diffs the live schema against the desired one with `schemadiff` (`vitess/go/vt/sidecardb/sidecardb.go`, `findTableSchemaDiff`), which may issue `ALTER TABLE … ENGINE = InnoDB` at every start (verify) | (source) | RT3 inventory; Q304 |
| C-2 | **Vitess 2PC requires semi-sync**: `Prepare` refuses with "two-pc is enabled, but semi-sync is not" (`vitess/go/vt/vttablet/tabletserver/dt_executor.go`); the flag is cleared when a primary is promoted without semi-sync (`tx_engine.go`, `twopcAllowed`). Vitess's 2PC does not use MySQL XA (`vitess/doc/design-docs/AtomicDistributedTransaction.md`: "Although MySQL supports the XA protocol, it's been unusable due to bugs"), which avoids WeSQL's untested XA (§29 §4.2) | (source) | Q303 |
| C-3 | **MySQL 8.0 support ends after Vitess v24** (vitess.io/docs/24.0, "supported databases"). Vitess v24.0.0 was released 2026-04-30; at about a year of support per major (estimate), v24 ends around 2027-04 | (source for the dates; estimate for the support window) | Q302 |
| C-4 | **SmartEngine's isolation** (RC and RR only, first-committer-wins, point locks, §29 §4) under vtgate's pooled connections and its `SET` handling | §29 §4 | RT3 inventory |
| C-5 | **No temporary tables** in SmartEngine (`HTON_TEMPORARY_NOT_SUPPORTED`, §29 §4.2). VReplication's and Online DDL's use of temporary tables (verify which statements) | §29 | RT3 inventory |
| C-6 | **`performance_schema`, `information_schema` and `SHOW` queries** vttablet's schema engine and health checks run | §15 method | RT3 inventory |
| C-7 | **The binlog events** WeSQL dropped with its consensus code on 2026-08-23 (§23 §6.4), against Vitess's binlog parser | §23 | RT3 inventory |

## 10. Cross-shard semantics (D306, D322)

| Property | Postgres via PgDog | MySQL via Vitess |
|---|---|---|
| Single-shard transaction | The shard's isolation (Postgres) | The shard's isolation (§29 D274) |
| Multi-shard write, default | Non-atomic: each shard commits separately (PgDog with `two_phase_commit = false`) | Non-atomic (`transaction_mode = multi`) |
| Multi-shard write, atomic | PgDog 2PC, only under D306's deployment rule and after RT2's gate | Not available until Q303 |
| Cross-shard read | No global snapshot; may be fractured | Same; Vitess documents fractured reads under 2PC ("does not provide isolation in the traditional ACID sense") |
| Cross-engine transaction | Never | Never |

The DST and nemesis checkers check exactly this table: snapshot isolation per shard for the list-append workload, conservation only for 2PC-enabled bank transfers, and fractured reads allowed across shards.

## 11. Formal specifications (TLA+)

### 11.1 Tools and layout (D310)

- `spec/tla/router/<Spec>.tla`, a model file `MC<Spec>.tla` and one or more `MC<Spec>_<variant>.cfg` per spec, as Neon's `safekeeper/spec/` is laid out (Apache-2.0, used as the reference layout and, for the Postgres half of `PrimaryFailover`, as the model of walproposer's Paxos, cited and not copied).
- `scripts/spec/check.sh <Spec> [<variant>]` runs TLC (v1.7.4 by default, the jar pinned by SHA-256 in the script) with `-workers 2 -deadlock`, and Apalache v0.62.3 `check --inv=<Inv> --length=<k>` for the invariants marked inductive. Bounds and the time each run takes are recorded in the spec's header.
- CI job `tla` (path-filtered on `spec/tla/**`): every spec at its PR bounds; nightly at larger bounds. A variant marked `EXPECT_VIOLATION` must fail with the named invariant, which keeps known-unsafe configurations documented and tested.

### 11.2 The specs

| Spec | Models | Invariants (safety) | Properties (liveness, under fairness) | First checked |
|---|---|---|---|---|
| `ShardMap` | The record's CAS (§6.1), the ConfigMap, N router instances with `Reload` and `Restart`, per-shard fences, client writes routed by each instance's generation | `TypeOK`; `OneOwner` (every key maps to exactly one shard per generation); `SingleWriter` (no two unfenced shards accept a write for the same key); `NoStrayWrite` (every accepted write landed on a shard that owned its key at that shard's fence state); `ConfigMapSafe` (the ConfigMap's generation never routes a key to a fenced or not-yet-caught-up shard) | Every instance eventually applies the record's generation; every write eventually succeeds or fails | RT0 (TLC), RT1 (trace validation) |
| `ReshardCutover` | §6.4's seven steps, the designated instance, the forward and reverse streams, unreachable instances, a crash of the saga between steps | `NoLostWrite` (every acknowledged write is on the owner of its key in the current generation, once streams drain); `NoDuplicateEffect` (each acknowledged write is applied once per destination row; upserts by primary key make replay idempotent); `SingleWriterRange`; `ReverseSafe` (rolling back before finalize loses no write acknowledged after the switch) | A started cutover ends in `Finalized` or `RolledBack` | RT0 (TLC, small bounds); RT4 (trace validation against the orchestrator) |
| `CrossShardCommit` | PgDog 2PC as implemented: log Phase 1, `PREPARE TRANSACTION` on each participant, log Phase 2, `COMMIT PREPARED`, done; recovery rolls back Phase 1 and commits Phase 2; coordinator crash with the log kept or lost; participant crash; the in-doubt monitor. A Vitess variant (metadata manager in `_vt.dt_state`) in RT4 | `Atomicity` (no participant commits while another rolls back); `DecisionDurable` (with a kept log); `MonitorNeverDecides` | With a kept log and fair recovery, no transaction stays prepared | RT2. Variant `MC_LostLog` is `EXPECT_VIOLATION` of the liveness property; `MC_GuessingMonitor` is `EXPECT_VIOLATION` of `Atomicity` |
| `PrimaryFailover` | Per shard: proposer terms over acceptors (Arm A, D264; WS2/WS3, D276–D278), the lease *T* with clock drift bound ε, the primary record's CAS, the router repoint (§6.3, §9.1) | `OneWriterPerTerm`; `AckedSurvives` (an acknowledged commit is in every later primary's log); `NoStaleServe` (a deposed primary serves no read after *T* + ε) | After the old primary stops, a new primary is writable | RT4; skeleton in RT0 |
| `RouterSession` | The routers' observable contract only: a transaction stays on its shard set; a prepared statement is re-prepared or fails after a generation change | `PinnedTxn`; `NoSilentReplan` | — | RT4 (Q312) |

### 11.3 Trace validation and the action-to-code map (D311)

- **Emitting.** Every machine reports each transition it takes as a `SpecEvent` through `Ctx::trace`. Drivers write them as JSON lines (`{"spec":"ShardMap","action":"Reload","instance":"pgdog-1","gen":4,"ok":true,"t":1234}`): `loams-detsim` to its run directory, the production driver to `tracing` target `loams::spec` (off unless `LOAMS_SPEC_TRACE=<path>`).
- **Checking.** `spec/tla/router/<Spec>Trace.tla` reads the file with the CommunityModules `Json` module and constrains the spec's `Next` so each step matches the next event (the trace-validation method of Cirstea, Kuppe, Loillier, Merz and others, "Validating Traces of Distributed Programs Against TLA+ Specifications", 2024). TLC either finds a behaviour of the spec that explains the trace or reports the first event it cannot explain. Fields the code cannot observe (another instance's state) are left unconstrained.
- **Coverage.** A test in `loams-sqlrouter` parses each spec's action list from its header and fails if an action has no `SpecEvent` emitter in code, or an emitter names an action the spec does not have.

The map for `ShardMap` (the others follow the same form in their headers):

| TLA+ action | Code | Event |
|---|---|---|
| `Publish(g)` | `loams_sqlrouter::push::ConfigPush::on(Input::RecordChanged)` → `Output::Cas` | `map.publish {db, gen, version}` |
| `WriteConfigMap(g)` | `ConfigPush` → `Output::WriteConfigMap`; `loams_sqlrouter_io::kube::ConfigMapSink` | `map.configmap {db, gen}` |
| `Reload(i, g)` | `ConfigPush` → `Output::Reload`; `loams_sqlrouter_io::pgdog::PgDogInstance::apply` | `fleet.reload {instance, gen, ok}` |
| `Restart(i)` | Simulation: `PgDogModel::restart`; real: a changed process start time in `observe` | `fleet.restart {instance}` |
| `Fence(s)`, `Unfence(s)` | `Cutover` → `Output::Fence`; `PostgresShard::fence_writes` | `shard.fence {shard, on}` |
| `ClientWrite(i, k)`, `Accept(s, k)`, `Reject(s, k)` | Workload clients in `loams-detsim` and `loams-nemesis` | `client.write {key, instance, shard, result}` |

### 11.4 As-built tooling: `loams-specview`

`crates/loams-specview` (a dev tool, `publish = false`) shows the spec checks and the router's Rust tests in a browser while they run. `loams-specview serve` runs each `specs.toml` variant under TLC, parses the output (progress lines, counterexample states, `Back to state N` lassos, TLA+ values into JSON) and streams it over SSE with the Rust test results (`cargo nextest run --message-format libtest-json` when nextest is installed, else `cargo test`). The Leptos frontend lists every variant and test, and plays each counterexample step by step with the changed variables highlighted. `ShardMap` and `ReshardCutover` have their own views (owners by generation, instances and fences, write sets with the acknowledged ones marked; the saga phases, the instances' routes to the Src and Dst stores); any other spec gets a variable table. `--record` saves a run as JSON lines and `replay` plays it back. Its `SpecEvent` is a placeholder for the RT1 simulator's §7.1 events: when RT1 emits them as JSON lines the same player will show a simulated run. How to run it: `crates/loams-specview/README.md`.

## 12. Lean 4 kernels and the differential oracle (D312)

### 12.1 Scope

| Module (`spec/lean/LoamsRouter/`) | Definitions | Theorems (proved) | Properties (Plausible) |
|---|---|---|---|
| `KeyRange.lean` | Keyspace ids as `Nat` below 2^64; `KeyRange` with optional upper bound; `IsPartition` (sorted, contiguous, first `lo` = 0, last `hi` = none) | `lookup_total_unique` (every id is in exactly one range of a partition); `split_preserves`; `merge_preserves` | Random splits and merges keep `IsPartition` |
| `ShardFn.lean` | `shardOf` for modulo (PgDog) and range (Vitess) schemes over an abstract hash | `modulo_partition` (n shards partition the id space); `shardOf_deterministic` | — |
| `Merge.lean` | `kmerge` of sorted lists under a total order with SQL `NULLS FIRST/LAST` | `kmerge_sorted`; `kmerge_perm` (a permutation of the concatenation) | Stability of ties by shard index |
| `Limit.lean` | `pushdown o n` (each shard returns `take (o+n)`) | `limit_pushdown` (`take n (drop o (kmerge xs)) = take n (drop o (kmerge (map (take (o+n)) xs)))`) | — |
| `Aggregate.lean` | Partial states for `COUNT(*)`, `COUNT(col)`, `SUM`, `MIN`, `MAX`, `AVG` as (sum, count); SQL NULL rules; `GROUP BY` | `decompose_correct` (combining per-shard partials equals the aggregate of the union) for each; `groupby_decompose` | `AVG` over integers: the router's rounding against the oracle's exact rational (a reported difference, not a theorem) |

Hash functions (DES, Postgres's Jenkins hash, xxhash) are **not** modelled in Lean: they are checked by reference vectors in Rust (§7.1), generated from a real Postgres (`SELECT hashint8extended(k, seed)`) and from Vitess's own tests (`vitess/go/vt/vtgate/vindexes/hash_test.go`, Apache-2.0, vectors copied with the notice).

### 12.2 The oracle

- `lake build oracle` builds `loams-router-oracle`, which reads JSON lines on stdin (`{"op":"merge","order":[…],"shards":[[…],…]}`, `{"op":"aggregate",…}`, `{"op":"partition_check",…}`) and writes one result per line.
- **Rust side.** `loams_sqlrouter::reference` implements the same functions; `proptest` properties mirror the Lean theorems for fast CI; a differential test (`reference_matches_lean_oracle`, 10 000 cases on PRs that touch either side, 100 000 nightly) feeds both the same random inputs. Without the oracle binary on `PATH` the test prints `skipped: needs loams-router-oracle` (the cluster-test convention of R1).
- **Against the routers** (RT2): a query runs on unsharded Postgres, through PgDog over 2 and 4 shards, and its per-shard results go through the oracle; three answers are compared. Deviations a router documents are listed in `conformance/router/pgdog-cross-shard-allowlist.toml` with the source of the documentation.
- **The gap, stated as the chat dump asked.** Lean proves the model; the differential tests are what connect the model to Loams's Rust reference and to the routers' behaviour. Neither router's code is proved.

### 12.3 CI

Job `lean` (path-filtered on `spec/lean/**` and `crates/loams-sqlrouter/src/reference/**`): elan with the pinned `lean-toolchain`, `lake build`, `lake exe plausible-check`, then the differential test. Lake's build directory is cached by `lean-toolchain` and `lake-manifest.json` (Q308).

## 13. Deterministic simulation (D313, D314)

### 13.1 Tiers

| Tier | What runs | Deterministic | Where |
|---|---|---|---|
| **1. Control-plane DST** (new) | `loams-sqlrouter` machines, driven by `loams-detsim`'s scheduler, against models of PgDog instances, Vitess, shards, the TiKV record store and the saga runner | Bit-exact: same seed, same trace hash | `crates/loams-detsim`, scenarios in `crates/loams-sqlrouter/tests/sim/` |
| **2. Engine simulation** (D28, unchanged) | Metastore, log, links, collections on one single-threaded runtime with real I/O | Seeded, not bit-exact | `crates/loams-sim` |
| **3. Real-system nemesis** (RT5, D315) | PgDog, Vitess, Loams Postgres, WeSQL, TiKV, RustFS as processes | No | `crates/loams-nemesis` |

`loams-detsim` is its own small crate (rand_chacha, serde_json, `loams-meta-conformance` for the linearizability checker) so the router's tests do not build `loams-sim`'s Lance and DataFusion dependencies. `loams-sim` re-exports its checkers.

### 13.2 The scheduler

- **One RNG** (`ChaCha8Rng` from the seed) decides every choice: the next event among the ready ones, message delays, drops, duplicates, reorders, fault firings, workload operations.
- **Simulated time** advances only by events. Machines see it through `Ctx::now`.
- **Network model**: per directed link a queue with delay distribution, drop and duplicate probabilities, and partition state (symmetric and asymmetric); a "slow link" multiplies delays; a reset fails in-flight requests with an indeterminate outcome.
- **Fault points** (buggify): `detsim::fault_point!("pgdog.reload.after_parse")` in model code; each run enables a random subset of named points (swarm testing) and each enabled point fires with its own probability.
- **Trace hash**: SHA-256 over the canonical JSON of every scheduled event. CI runs each corpus seed twice and fails if the hashes differ.
- **Shrinking**: a failing run's schedule (the list of chosen events and fault firings) is minimized by delta debugging over fault firings and workload operations, replayed with the same seed, until no smaller schedule fails. The report prints the seed, the enabled faults and the shrunk schedule, in `SimReport`'s format (D28).

### 13.3 Fault catalog

The chat dump's catalog (§7.2), each fault mapped to where it is injected and when it arrives.

| Category | Fault | Tier 1 (`loams-detsim`) | Tier 2 / existing | Tier 3 (`loams-nemesis`) | RT |
|---|---|---|---|---|---|
| Network | Drop, delay, reorder, duplicate | Network model | `Router` isolate/heal (D28) | toxiproxy `latency`, `timeout`; `tc netem` | RT1 |
| Network | Partition, symmetric and asymmetric | Network model | `Isolate`, `Heal` | iptables per container | RT1 |
| Network | Slow link; reset mid-statement | Delay multiplier; indeterminate failure | — | toxiproxy `bandwidth`, `reset_peer` | RT1 |
| Node | Router instance crash and restart (reads ConfigMap) | `PgDogModel::restart` | — | `docker kill` / pod delete | RT1 |
| Node | Shard primary crash; replica crash | `ShardModel::crash` | Worker crash (D28) | `docker kill` the compute or `mysqld` | RT2 (Postgres), RT4 (WeSQL) |
| Node | Coordinator crash between prepare and commit, log kept or lost | `PgDogModel` 2PC with `wal_dir` kept or wiped | — | kill PgDog; delete its PVC | RT2 |
| Node | Saga crash during a cutover step | Saga-runner model restarts the machine from its record | Resonate's own tests (§21) | kill `loams` | RT4 |
| Time | Clock skew and jumps; lease expiry races | Per-node clock offsets in `Ctx::now` | — | `libfaketime`, chrony step | RT4 |
| Time | Timeout storms | Delay bursts beyond client timeouts | — | toxiproxy latency spike | RT2 |
| Storage | Torn writes; fsync failure; delayed durability | Model of PgDog's 2PC WAL (torn tail, lost unsynced records) | `FaultyStore::random` (D28); Arm A journal tests (§28 §7.2) | `dm-flakey` on the PVC (verify availability on CI) | RT2 |
| Storage | WAL or binlog segment lost or truncated; S3 errors and latency; eventual visibility | Shard model's change stream gaps | `FaultyStore` 5xx, 503, 412 (§12 §2 item 2) | RustFS with toxiproxy | RT4 |
| Topology | Stale map in a router; concurrent map updates | Generation lag per instance; concurrent CAS writers | — | Stale ConfigMap mount | RT1 |
| Topology | Failover during reshard; split during failover | Combined scenario | — | Combined nemesis | RT4 |
| Load | Connection exhaustion; large scatter; slow shard; head-of-line blocking | Pool-limit and slow-shard models | — | pgbench and sysbench drivers | RT2 |
| Protocol | Malformed packets; disconnect mid-transaction; prepared statement invalidated by DDL | Client model disconnects; DDL event invalidates | — | A fuzzing client (RT5) | RT4 |

### 13.4 Workloads and checkers

| Workload | Checker | Implementation |
|---|---|---|
| Single-key read/write registers per key | Linearizability per key | `loams_meta_conformance::linearizability` (WGL), reused |
| Bank transfers across shards (2PC on) | Conservation of the total after recovery; no partial transfer visible after quiescence | `loams_detsim::checkers::bank` (new) |
| Unique-key inserts | No duplicate, no lost acknowledged insert | `checkers::unique` (new) |
| Append-only lists per key | Snapshot isolation per shard; cross-shard fractured reads allowed (D322); G0, G1c, G-single cycles reported | `checkers::list_append` (new, Elle-style; the checker §20 §14 item 4 uses) |
| Writes during a cutover or split | `NoLostWrite`, `NoDuplicateEffect` against the final shards; table checksums | `checkers::split` (new) with `ShardBackend::checksum` |
| Any | Liveness watchdog: every operation completes or fails within *B* simulated seconds after faults heal | `checkers::liveness` (new) |
| Any | Trace validation against the spec (§11.3) | `scripts/spec/validate-trace.sh` on sampled runs |

Every run asserts that no acknowledged commit is lost and no committed transaction is partially visible where atomicity is promised (§10).

### 13.5 CI policy

- **Pull requests** (job `router-sim`, path-filtered on `crates/loams-sqlrouter*/**` and `crates/loams-detsim/**`): the regression corpus (`crates/loams-sqlrouter/tests/sim/corpus/*.seed`) twice each with trace-hash equality, plus 2 000 fresh seeds per scenario (`ROUTER_SIM_SEEDS`), within 10 minutes (estimate: a pure machine step costs microseconds; a 500-step run, a few milliseconds).
- **Nightly**: 1 000 000 seeds across scenarios with swarm configuration, split over the matrix (Q310); 100 sampled traces through trace validation; the Lean differential at 100 000 cases; Apalache at nightly bounds.
- **A failing nightly seed** files an issue (`gh issue create`, label `sim-failure`) with the seed, the enabled faults and the shrunk schedule. The fix's PR adds the seed to the corpus; a seed is never removed.

## 14. Real-system tests

### 14.1 Contract suites (D316)

`shard_backend_conformance!(Backend)` and `router_fleet_conformance!(Fleet)` expand the same cases against the model and the real thing: role and read-only reporting, fence, waiting for the role's backends to exit, then write fails with the documented error, unfence restores, prepared transactions listed with their gids, checksums equal for equal data, `RELOAD` applies a generation, `PAUSE` queues and `RESUME` releases. Real backends: Postgres 17.11 in CI, Loams Postgres computes from `deploy/neon` once P2b is merged, WeSQL in RT3.

### 14.2 Differential tests

The same seeded SQL stream against an unsharded engine and the sharded stack, results compared (row multiset, or order when `ORDER BY` is total), with the oracle as the third opinion for merges and aggregates (§12.2). Postgres in RT1 (single-shard) and RT2 (scatter, merge, aggregate); MySQL in RT3.

### 14.3 Nemesis runs (RT5)

`loams-nemesis` composes the stack (compose for the PR-sized run, k3d nightly), runs Tier 1's workloads through real clients (tokio-postgres, `mysql_async`), injects Tier 3 faults, and runs the same checkers on the recorded history. Online resharding under load checks table checksums at each step. Performance baselines (routing overhead, scatter latency, cutover duration, commit latency on the bucket-backed logs) are recorded, not gated, at first.

## 15. The compatibility inventory method (D309)

The inventory is the spec of what an engine must answer for a router. It is built the same way for both halves:

1. **Static extraction.** List every statement and command the router or its tablet sends to the backend, from its source: string constants and query builders, flavor files, sidecar DDL, health checks, replication and schema-engine queries. For Vitess: `go/mysql/flavor_mysql*.go`, `go/vt/vttablet/tabletserver/schema/`, `go/vt/vttablet/tabletmanager/`, `go/vt/vttablet/tabletserver/vstreamer/`, `go/vt/vttablet/tabletmanager/vreplication/`, `go/vt/sidecardb/schema/**`. For PgDog: `pgdog/src/backend/{replication,schema,pool}/` and the 2PC module (read as a reference; the inventory records statements and source paths, never PgDog code).
2. **Dynamic capture.** Run the router's own suites against the reference engine with statement logging: Vitess's `examples/local` and selected `go/test/endtoend/` packages against MySQL 8.0.46 with `performance_schema.events_statements_summary_by_digest`; PgDog's `integration/` suites (`resharding`, `logical`, `failover`, `pgbench`, `rewrite`) against Postgres 17.11 with `log_statement = 'all'` and `pg_stat_statements`. Normalize to digests.
3. **Merge** the static and dynamic lists into one table keyed by digest, with the component that issues each statement.
4. **Replay and classify** each digest (with its captured example and session state) against the target engine (WeSQL; Loams Postgres) and the reference: `same` (identical result, warnings and errors), `differs` (result or metadata differs), `error` (target errors), `unsupported` (target refuses by design, with the reason). Results compare by a canonical hash.
5. **Suite pass rates.** Run the selected suites through the router against the target; record pass, fail and skip per test, and link each failure to inventory rows.
6. **Bless** the tables as TSV files (`conformance/router/{vitess-wesql,pgdog-loamspg}-statements.tsv` with columns `digest, component, source, example, class, ref_hash, target_hash, note, issue`, where the two hashes are the canonical result hashes of step 4; `…-suites.tsv` with `suite, test, result, rows`). A PR that changes a pinned version re-runs the inventory and shows the diff.
7. **Gate.** RT3's gate for D302 (proposed): no `error` or `differs` row in the components Loams uses (query service, health, schema engine, VReplication for MoveTables and Reshard), and the suite pass rates in §17's RT3 row. Rows in components Loams does not use are recorded, not gating.

## 16. Licenses (D318)

| Component | License | Rule |
|---|---|---|
| Loams's crates, specs, oracle, inventory tables | Apache-2.0 | D11 |
| PgDog | AGPL-3.0 | Unmodified service; read as a reference; nothing copied, including into specs and inventory rows beyond statement text the engine receives (D236, §28 §8) |
| Vitess | Apache-2.0 | Service; may be forked or ported only by a recorded decision, with its NOTICE (D302, D317); test vectors copied with the notice (§12.1) |
| WeSQL | GPL-2.0-only | Separate process (D148, §29 §10) |
| PostgreSQL `hashfn.c` (ported hash functions) | PostgreSQL License | Ported with the notice into `loams-sqlrouter::hash` |
| etcd | Apache-2.0 | Service |
| TLC, CommunityModules | MIT | Tools, not linked |
| Apalache, Lean 4, Plausible | Apache-2.0 | Tools; the Lean oracle is Loams's code |
| Elle | EPL-2.0 | Not used as a dependency; optional external cross-check (D314) |
| Jepsen | EPL-1.0 (`jepsen/project.clj`) | Optional external tool (D315) |
| toxiproxy | MIT | Test tool |
| `des` 0.9 crate | MIT OR Apache-2.0 | Linked; `cargo deny` in RT0 Task 6 |
| `xxhash-rust` 0.8 | BSL-1.0, the **Boost** Software License: permissive, not the Business Source License (BUSL-1.1) that D11 and `deny.toml` forbid | Already a workspace dependency. This is the recorded exception to "Apache-2.0 only": D318 requires permissive licenses for linked crates, not Apache-2.0 for each one, so Boost is allowed as MIT is |

## 17. Roadmap: track RT (D319)

Each phase is small stacked PRs. RT adds crates and CI jobs and changes no M-track code path.

| Phase | Was (chat dump) | Scope | Depends on | Done when |
|---|---|---|---|---|
| **RT0** | M0 | This document's decisions; `spec/` scaffolding with `ShardMap` and `ReshardCutover` checked at small bounds and skeletons of the others; the compatibility inventory, static half for both routers and dynamic half against reference engines; `loams-sqlrouter` kernel types, ranges and hash vectors; the Lean project with the range-partition proofs and the oracle skeleton; the kernel crate's lints. About 9 PRs ([plan](../plans/2026-10-01-rt0-foundations-and-specs.md)) | — | Specs pass TLC at the stated bounds in CI; the partition lemma is proved; Rust and Lean agree on 10 000 partition checks; the inventory TSVs exist with every row classified or `pending-target` |
| **RT1** | M1 | `loams-detsim`; `ShardMapStore` (memory, TiKV); PgDog rendering; `RouterInstance` and `ShardBackend` with models and contract suites; the `ConfigPush` machine; trace validation of `ShardMap`; the DST scenario; the compose slice (PgDog, two Postgres shards); single-shard differential. About 10 PRs ([plan](../plans/2026-10-01-rt1-postgres-slice-and-sim.md)) | RT0; P3 for Loams Postgres computes (CI uses Postgres 17.11 until then) | Single-shard routing passes the differential; the DST scenario is clean on the PR corpus and 2 000 seeds, and replay is deterministic; traces validate |
| **RT2** | M2 | Lean merge, limit and aggregate kernels and the oracle; cross-shard differential through PgDog; Loams Postgres 2PC settings and tests; PgDog 2PC deployment rule; `CrossShardCommit` checked; the in-doubt monitor; the change stream and exact-boundary snapshot copy; an offline split verified by checksums. About 10 PRs ([plan](../plans/2026-10-01-rt2-scatter-oracle-2pc.md)) | RT1; P2b for the Loams Postgres tests | Theorems proved; differential clean or allowlisted; 2PC fault tests pass with the deployment rule; split checksums equal |
| **RT3** | M3 | Vitess v24 with WeSQL: dynamic inventory against WeSQL, the RT3 gate (§15 step 7), vtgate as the MySQL front end (D320), VSchema rendering, unsharded then two-shard keyspaces, MySQL differential, Vitess end-to-end subset pass rates (target: `vtgate/queries/*` and `vreplication` MoveTables/Reshard basic cases, numbers fixed in the plan) | RT0 inventory; §29 WS2 for durability claims; Q302–Q304 | Gate met or D317's trigger recorded |
| **RT4** | M4 | `PrimaryFailover` (Arm A and WS3) and the Vitess `CrossShardCommit` variant checked; the multi-instance cutover orchestrator with trace validation; the full fault catalog in DST; `RouterSession` contract; Q303 decided | RT2, RT3; §28 P4c; §29 WS3 | All specs pass at nightly bounds; DST clean across every category |
| **RT5** | M5 | Resharding end to end on real clusters (PgDog across instances, Vitess `Reshard`); `loams-nemesis`; performance baselines; the published compatibility matrix | RT4 | Nemesis runs clean for the agreed durations; a live reshard with zero lost or duplicated rows; the matrix published |

## 18. Risks

| # | Risk | Mitigation |
|---|---|---|
| 1 | **PgDog's maturity** (v0.1.x) for sharded production databases | Differential and nemesis tests (RT2, RT5); sharding offered as beta until Q305; PgBouncer and Neon's proxy remain the unsharded fallbacks (§28 §8) |
| 2 | **Vitess on WeSQL fails the gate** (C-1, C-4–C-7) | The inventory finds it in RT3 before any product commitment; fixes go into the WeSQL fork (GPL, its own repo); D317 if neither works |
| 3 | **Vitess drops MySQL 8.0 after v24** (C-3) | Pin v24; Q302 schedules WeSQL's 8.4 rebase, which §29 Q274 already raises for Forgejo |
| 4 | **Multi-instance PgDog cutover behaves differently than modelled** (Q306) | The backend fence makes safety independent of PgDog's routing table; RT4 trace-validates the orchestrator against the real PgDog |
| 5 | **PgDog's 2PC log** lacks checksums and lives on one pod's disk | D306's deployment rule; the monitor alarms and never decides; `CrossShardCommit`'s expected-violation variants document why |
| 6 | **Specs drift from code** | Trace validation in CI; the action-coverage test (D311) |
| 7 | **Models drift from engines and routers** | Contract suites against both (D316); nemesis runs (RT5) |
| 8 | **Formal-methods effort grows** | Strict scope: TLA+ for protocols Loams builds or orchestrates, Lean for pure kernels only (D308, D312); RouterSession may be dropped (Q312) |
| 9 | **License mistakes** (PgDog text in a spec, Vitess code without notice) | §16's rules; provenance in each spec header; review checklist in every RT plan |
| 10 | **Team bandwidth across three engines** | Postgres first (RT1–RT2) to validate the control plane before MySQL (RT3); RT runs only when the build machine is free |
| 11 | **The owner wants a Loams-built router** (Q300) | D317 keeps the seams; the RT0–RT2 work (specs, kernels, DST, inventory, control plane) is needed by either path |

## 19. Open questions

| # | Question | Owner | Needed by |
|---|---|---|---|
| Q300 | ~~Accept D300 (buy PgDog and Vitess, build only the control plane and the evidence) over the chat dump's Rust router with MySQL and Postgres frontends~~ Answered 2026-10-02 by the owner: the recommended default — accept D300: buy PgDog and Vitess; build only the control plane and the evidence (§31 §5) | Founder | Resolved |
| Q301 | ~~Vitess topology on a dedicated etcd (proposed), or on PD's embedded etcd (PD embeds etcd and serves its client port; whether external etcd v3 clients are supported is not verified)~~ Answered 2026-10-02 by the owner: the recommended default — a dedicated etcd for Vitess topology (§31 §4.1) | Eng | Resolved |
| Q302 | ~~WeSQL's rebase to MySQL 8.4 before Vitess v24's support ends (estimate: about 2027-04); joint with §29 Q274~~ Answered 2026-10-02 by the owner: the recommended default — yes, rebase WeSQL to MySQL 8.4 before Vitess v24's support ends, about 2027-04, together with Q274 (D410; §31 §9.2 C-3) | Founder, Eng | Resolved |
| Q303 | ~~Atomic MySQL cross-shard commits: run a semi-sync replica so Vitess 2PC is allowed, or ship without MySQL cross-shard atomicity~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — no MySQL cross-shard atomicity in RT4: vtgate runs `--transaction_mode=SINGLE`, so a transaction that spans shards is refused (D412); why: Vitess 2PC needs semi-sync replicas, which WS2's quorum is not, and D306 allows atomic commit only with a durable coordinator log | Eng, Founder | Resolved |
| Q304 | ~~Vitess `_vt` sidecar tables on SmartEngine (default substitution) or InnoDB (`serverless_honor_innodb_engine`), and whether vttablet's sidecar diff loops on the engine~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — SmartEngine (WeSQL's default substitution); RT3 Task 0 checks that vttablet's sidecar diff does not loop; why: sidecar state then lives on the bucket like the rest of the data | Eng | Resolved |
| Q305 | ~~When sharded Loams Postgres through PgDog leaves beta~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — sharded Loams Postgres through PgDog stays beta until RT2's exit gate and RT5's nemesis suite pass, with RT2's evidence recorded; why: PgDog is v0.1.x (§31 §18 risk 1) | Founder | Resolved |
| Q306 | ~~Does `CUTOVER` on one PgDog plus `RELOAD` of the rendered swap on the others give identical routing, or does it need PgDog's Enterprise Edition (proprietary, not usable in OSS Loams)~~ Answered 2026-10-02 by the owner: the plan's default — not an owner decision: RT4 verifies `CUTOVER` plus `RELOAD` on open-source PgDog; PgDog's Enterprise Edition is never used, and the backend fence keeps the cutover safe either way (§31 §6.4) | Eng | Resolved |
| Q307 | ~~The Postgres fence: `ALTER ROLE <app_role> NOLOGIN` plus termination (proposed; it also stops stale reads), or revoking write privileges per table (races with DDL)~~ Answered 2026-10-02 by the owner: the recommended default — `ALTER ROLE <app_role> NOLOGIN` plus termination; it also stops stale reads (§31 §6.4) | Eng | Resolved |
| Q308 | ~~The Lean job on every relevant PR (proposed) or nightly only, given its build time~~ Answered 2026-10-02 by the owner: the recommended default — the Lean job on every relevant PR, path-filtered (§31 §12.3) | Eng | Resolved |
| Q309 | ~~Trace validation with TLC and the trace-spec method (proposed), or Apalache, or a Rust port of each spec's `Next`~~ Answered 2026-10-02 by the owner: the recommended default — TLC with the trace-spec method (§31 §11.3) | Eng | Resolved |
| Q310 | ~~The nightly seed budget across runner minutes (1 000 000 proposed)~~ Answered 2026-10-02 by the owner: the recommended default — 1 000 000 seeds a night (§31 §13.5) | Eng | Resolved |
| Q311 | ~~The nemesis harness in Rust (proposed) or Jepsen as an external tool~~ Answered 2026-10-02 by the owner: the recommended default — the nemesis harness in Rust (§31 §14.3) | Eng | Resolved |
| Q312 | ~~Keep `RouterSession` (a black-box contract of bought routers) or drop it~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — keep `RouterSession`, limited to its two properties (`PinnedTxn`, `NoSilentReplan`); why: it is the only evidence that the bought routers keep transactions on their shard set | Eng | Resolved |
| Q313 | ~~vtgate as the front end for unsharded WeSQL too (D320), retiring §23's N6 splice~~ Answered 2026-10-02 by the owner: the recommended default — yes: vtgate fronts unsharded WeSQL too, and §23's N6 splice retires (D320, §31 §9.1) | Founder | Resolved |
| Q314 | ~~Narrow Q260 to analytics: OLTP MySQL wire access comes from vtgate in front of WeSQL (§29, D320); whether Loams also serves read-only MySQL wire over DataFusion stays open~~ Answered 2026-10-02 by the owner: the recommended default — yes: Q260 narrows to read-only analytics over DataFusion, and OLTP MySQL wire access is vtgate in front of WeSQL (§29, D320) | Founder | Resolved |

## 20. Contradictions with earlier decisions, and how they are resolved

| Earlier | Conflict | Resolution |
|---|---|---|
| D260 (no TiDB), Q260 | The chat dump's "Loams SQL, InnoDB semantics on TiKV" | Rejected (D301). WeSQL is the MySQL engine (§29); Q314 narrows Q260 |
| D236 (PgDog unmodified) | The chat dump's "clean Rust Postgres router, pgdog as reference" | D236 stands; D300; a Loams router only under D317 |
| D11 (Apache-2.0) | The chat dump's "router license AGPL or Apache" | Apache-2.0 (D318) |
| D28 (seeded, not bit-exact simulation) | The chat dump's madsim/turmoil DST | D313 adds a bit-exact tier for sans-I/O code; D28 unchanged for the engine |
| D153, MySQL half (§23 §6.3, N6: Loams's handshake-and-splice proxy) | D320: vtgate | Proposed amendment; the splice is the fallback for unsharded WeSQL only, and sharded MySQL waits for D317 |
| §29 §7.2 ("the router follows the record") | With Vitess, the repoint is `TabletExternallyReparented` | Amendment proposed by D320; a note is in §29 §7.2 |
| §18 §5.8 ("Loams avoids cross-shard atomicity") | D306 enables PgDog 2PC for SQL databases | No conflict: §18 is about the retrieval engine's metadata; SQL databases opt in under D306's rule |
| §18 §5.5 ("Loams never copies data") | SQL resharding copies rows | No conflict: D321 scopes §18 to the retrieval engine |
| §20 §14 item 4 (the Elle-style checker in `loams-sim`'s checker module) | D314 puts it in `loams-detsim::checkers` | One implementation in `loams-detsim`, re-exported by `loams-sim` |
| §23 §6.4 (WeSQL binlog with `gtid_mode = OFF`, spike) | Vitess and WS2 need GTIDs | `gtid_mode = ON` for Vitess-fronted WeSQL (§9.1), as §29 WS2 already requires |
| Chat dump milestones M0–M5 | Loams's M0–M6 | Renamed RT0–RT5 (D319). The `Q-RT-*` questions of §24–§25 are the runtime track's and are unrelated to track RT |
| D2, D130 (OLTP out of scope for the retrieval engine) | Sharded OLTP | As §23 and §28: separate services beside the engine |

## 21. Sources

Read on 2026-10-01.

- **Chat dump**: `chatdump.md` lines 54–322.
- **PgDog** (`pgdogdev/pgdog` at `80d6059d`, AGPL-3.0, read as a reference): `docs/{SHARDING.md,RESHARDING.md,REPLICATION.md}`; `pgdog/src/admin/{cutover.rs,pause.rs,reload.rs,show_peers.rs}`; `pgdog/src/frontend/client/query_engine/two_pc/{mod.rs,manager.rs,transaction.rs,wal/README.md}`; `pgdog/src/util.rs` (`instance_id`, `deployment_id`); `pgdog-config/src/general.rs` (`two_phase_commit*`); `integration/` directory list. GitHub API: license AGPL-3.0, release v0.1.60 (2026-09-24).
- **Vitess** (`vitessio/vitess`, Apache-2.0, through the GitHub API): `go/vt/sidecardb/schema/{twopc,vreplication}/*.sql`; `go/vt/sidecardb/sidecardb.go`; `go/vt/vttablet/tabletserver/{dt_executor.go,tx_engine.go}`; `go/vt/vtgate/vindexes/hash.go`; `doc/design-docs/AtomicDistributedTransaction.md`; releases v22.0.0 (2025-04-29), v23.0.0 (2025-11-04), v24.0.0 (2026-04-30), v24.0.4 (2026-10-01). vitess.io/docs/24.0: "Supported databases", "Distributed transactions", "Unmanaged tablet".
- **Neon** (`neondatabase/neon`, Apache-2.0): `pageserver/src/pgdatadir_mapping.rs`; `test_runner/regress/{test_twophase.py,test_logical_replication.py}`; `safekeeper/spec/{ProposerAcceptorStatic.tla,ProposerAcceptorReconfig.tla,readme.md}`.
- **WeSQL** (`ostrium-labs/wesql` at `eef34f452`): `mysql-test/suite/smartengine_consistent_snapshot/t/default_storage_engine.test`; semi-sync tests under `mysql-test/suite/smartengine_rpl_*`.
- **Tools**: GitHub API for `tlaplus/tlaplus` (MIT; v1.7.4 stable, v1.8.0 rolling pre-release), `apalache-mc/apalache` (Apache-2.0; v0.62.3), `leanprover/lean4` (v4.34.1), `leanprover-community/plausible` (Apache-2.0), `madsim-rs/madsim` (Apache-2.0), `tokio-rs/turmoil` (MIT), `jepsen-io/elle` (EPL-2.0); crates.io for `madsim` 0.2.34, `turmoil` 0.7.2, `proptest` 1.11.0, `des` 0.9.0, `xxhash-rust` 0.8.19. Trace validation: H. Cirstea, M. A. Kuppe, B. Loillier, S. Merz et al., "Validating Traces of Distributed Programs Against TLA+ Specifications" (2024).
- **Loams**: §12 §2; §18 §5; §20 §10, §14; §21; §23 §6.3, §6.4, §9; §28 §4, §5, §7.2, §8, §11; §29 (PR #172) §4, §6, §7, §9; D11, D28, D148, D153, D154, D236, D260, D264, D273–D280; Q260, Q274.

## 22. RT0 as built (2026-10-02)

RT0 is done: PRs #188 (specs), #189 (Lean), #191 (`loams-sqlrouter`), #193–#194 (Postgres inventory), #249–#250 (MySQL inventory), #190 (`loams-specview`, the browser view of runs) and this exit. The plan's execution rulings E1–E21 hold the details.

### 22.1 Tools

TLC 1.7.4 (`tla2tools.jar`, SHA-256 pinned), Apalache 0.62.3, Lean 4.34.1 through elan. CommunityModules is not pinned yet: its 2026 builds need TLC 1.8, and only RT1's trace validation needs its `Json` module (E1).

### 22.2 Specs: bounds and results

| Spec, variant | Result | States | Time (4 workers, local) |
|---|---|---|---|
| `ShardMap` Small, with `Converges` | ok | 655,107 | 14 s |
| `ShardMap` Small, Apalache `SingleWriter` to length 8 | ok | — | 79 s |
| `ShardMap` UnsafeConfigMap | violation `SingleWriter`, as declared | 108 | 1 s |
| `ShardMap` Nightly (2 keys, 3 instances, 3 generations, 4 writes) | ok | 57,401,019 | 247 s (6 workers) |
| `ReshardCutover` Small, with `Terminates` | ok | 8,257 | 1 s |
| `ReshardCutover` Small, Apalache `SingleWriterRange` to length 10 | ok | — | 14 s |
| `ReshardCutover` CrashSaga | ok | 13,541 | 1 s |
| `ReshardCutover` NoFence | violation `SingleWriterRange`, as declared | 772 | 1 s |
| `ReshardCutover` Nightly | ok | 791,265 | 24 s |
| `CrossShardCommit`, `PrimaryFailover`, `RouterSession` | parse | — | — |

The runs found and fixed one modelling bug before any code existed: the first cutover model let the saga switch a designated instance it could not reach, and needed strong fairness for a flapping partition (E5). Mutation runs confirm `SingleWriter` and `ConfigMapSafe` are not vacuous (`spec/tla/router/README.md`).

### 22.3 Lean and the kernel

Proved without `sorry`: `validate_iff`, `partition_total_unique`, `lookup_spec`, `lookup_isSome`, `split_partition`, `merge_partition`, `modulo_partition` and `shardOfRange_total`. A review found that `ShardFn` had never compiled, because the oracle did not import it. The library is now a default build target, so CI checks every theorem.

`loams-sqlrouter` implements the same algorithms. It agrees with the Lean oracle on 10,000 random cases, and a planted `<`-for-`<=` bug is caught at case 108 (E16). Its Postgres hash port matches real `PARTITION BY HASH` in `postgres:17.11` on 38,000+ key and modulus pairs (E15). Its Vitess `hash` and `xxhash` match Vitess's own vectors.

### 22.4 The compatibility inventory (§15)

| Half | Rows | Classes | Suites |
|---|---|---|---|
| PgDog v0.1.60 → Postgres | 548 | all `pending-target` (no Loams Postgres compute yet, P2b); the reference errored on 137 | PgDog's `pgbench`, `schema_sync`, `data_sync`, `two_pc` and a baseline pass; `resharding` timed out at 20 minutes under pgbench load; `rewrite`, `logical` and `failover` skipped |
| Vitess v24.0.4 → WeSQL | 1,981 | 1,822 `same`, 158 `differs` (`schema-engine` 70, `sidecar` 29, `vreplication` 21, `2pc` 16, `vdiff` 11, `onlineddl` 6, `health` 4, `query` 1), 1 `unsupported`, 0 `error` | WeSQL 24 pass, 3 fail, 1 skip; MySQL 8.0.46 reference 26 pass, 1 skip |

Observations on §9.2's items:
- **C-1:** Vitess's `_vt` sidecar tables land in SmartEngine on WeSQL (InnoDB on MySQL), and vttablet issues no `ALTER … ENGINE` on restart, so its sidecar diff does not loop. The `serverless_honor_innodb_engine` ON case was not run: the published `beta5.40` image lacks the variable (answers Q304).
- **C-2:** WeSQL has no semi-sync plugins, so keyspaces need durability policy `none`. Vitess prepares 2PC only over a Unix socket, which an unmanaged vttablet must share with its MySQL. **On WeSQL, a 2PC transaction deadlocks at `start_commit`** (D323).
- **C-4:** `SERIALIZABLE` is refused, consistent with D322's per-shard isolation promise.
- **C-5:** temporary tables work.
- **C-7:** MoveTables, Reshard, VDiff and Online DDL pass on WeSQL.

### 22.5 Decisions from RT0

- **D323. No Vitess 2PC on WeSQL keyspaces until the `start_commit` deadlock is understood.** vtgate's `transaction_mode` is `MULTI` (best-effort, with D307's monitor alerting on partial commits), never `TWOPC`, for WeSQL keyspaces. RT3 Task 0 reproduces the deadlock and decides between an upstream WeSQL fix and a Vitess setting.
- **D324. One vttablet per WeSQL primary, as a sidecar container in the primary's pod**, sharing the MySQL socket directory through an `emptyDir` volume, because Vitess prepares 2PC over the Unix socket. This refines D302 ("separate services") for the tablet. vtgate and vtctld stay separate services.
- **Q308 answered.** The `lean` job runs on every PR that touches `spec/lean/**` or the ranges code, not only nightly. A cold `lake build` of the package takes well under a minute locally after the toolchain is cached, and `lake test` takes seconds.
