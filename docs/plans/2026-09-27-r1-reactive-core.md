# R1 — TiKV Metastore and the Reactive Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, formats, constants), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: Planned** (2026-09-27). **Amended 2026-09-27 with the TiKV feasibility spike** (playground v8.5.8, `tikv-client` 0.4.0; design §20 notes marked *(spike)*): Task 1 has the exact playground command and configs; async commit with 1PC is the default (Ruling 3); the runner restarts on `PessimisticRetry` and supervises the TSO stream (Task 2); Q33 is verified for the binaries. **Task 0 done 2026-09-27** ([`r1-dependency-spike.md`](r1-dependency-spike.md)): rows R1–R19 at the end amend the task text, and where a row says "amended", the task text already carries the change (Q32: no keyspace-level GC on v8.5.8, so Task 3 runs cluster-wide GC; `tikv-client` is a git pin). **Task 1 done 2026-09-27:** rows T1-1–T1-8. **Task 2 done 2026-09-27:** rows T2-1–T2-17 (the owner's rulings on the gRPC limit, PR titles and DCO are T2-12–T2-14, and on rows T2-4, T2-5 and T2-7 they are T2-15–T2-17; row R10 amended; Task 16's nemesis gains a PD stall). **Task 3 done 2026-09-27:** rows T3-1–T3-11 (the owner's rulings on rows T3-11 and T3-7 are T3-12 and T3-13). **Task 4 done 2026-09-27:** rows T4-1–T4-15 (the owner's rulings on rows T4-3 and T4-14 are T4-16 and T4-17). **Task 5 done 2026-09-27:** rows T5-1–T5-13 (the owner's rulings on rows T5-4 and T5-3 are T5-14 and T5-15). **Task 6 done 2026-09-27:** rows T6-1–T6-12 (the metastore now commits with two-phase commit, T6-5). **Task 7 done 2026-09-27:** rows T7-1–T7-10 (the owner's rulings on rows T6-5, T6-8 and T6-1 are T7-1–T7-3: two-phase commit stays the default for the metastore and Live, the S3 fault matrix and simulation on TiKV move to M2, and `operon`'s TiKV metastore is behind the off-by-default feature `tikv`). Track R, beside M1 (D127). Branches `r1-t<N>`, stacked; PRs target `main`. R1 tasks interleave with M1 tasks on the one-build machine: never start an R1 build while an M1 build runs, and never run the TiKV playground during a build.

**Goal:** Ship the first slice of design §20 (D116–D131):
- `operon-tikv`, the TiKV client layer: config and keyspace bootstrap, the TSO clock, a transaction runner with error classification, retries, commit tokens and a fault hook, the order-preserving tuple codec, the cluster MVCC GC loop (keyspace-level GC does not exist on v8.5.8, row R6), the test harness;
- `operon-meta-tikv`, `impl MetaStore` over TiKV, passing the 53-case conformance suite with its linearizability histories and a TiKV fault matrix, selectable in `operon dev` and `operon standalone` (D124);
- `operon-live`, one Loam Live app in one keyspace: documents, tables, indexes, the commit journal, `LiveTxn` with read sets, built-in and QuickJS queries and mutations, reactive subscriptions, and the `loam.live.v1` sync API over connect-rust (`Watch`, `ModifyQuerySet`, `Query`, `Mutate`, `Deploy`), bound to 127.0.0.1:7710 (D117–D121);
- `@operon/live`, the generated TypeScript client with a reactive layer;
- a dev playground with TiKV on API v2, a Live keyspace, a metastore keyspace and a keyspace-mode TiDB for MySQL clients (D123);
- the R1 gates: the reactive correctness checker, the transaction checker, fault runs, and the exit report.

**Architecture:**
- **One TiKV layer.** `operon-tikv` wraps `tikv-client`'s `TransactionClient` with a keyspace. Every transaction in `operon-meta-tikv` and `operon-live` goes through `TxnRunner::run`, which classifies errors (`Conflict`, `NotApplied`, `Undetermined`, `Fatal`), retries with jittered backoff, writes a commit token when asked, and calls a `FaultPlan` hook at `BeforeBegin`, `BeforePrewrite`, `BeforeCommit` and `AfterCommit`. Tests isolate themselves by a random root prefix inside a shared test keyspace, so a case never needs a new keyspace.
- **The metastore** maps each `MetaStore` method onto one transaction with the key layout of §20 §11.2 and the mapping of §20 §11.3.
- **Loam Live** stores documents and index entries with the key layout of §20 §4.3. A mutation is a `LiveTxn`: reads go through it and are recorded in its read set; writes are buffered with index maintenance; commit writes one journal entry into a shard (§20 §5.3). The subscription manager ticks on TSO timestamps, tails the journal, matches written keys against an interval index of read sets, reruns invalidated queries and hands results to the session manager, which sends versioned Transitions over `Watch`.
- **Functions** are ES modules run by QuickJS (`rquickjs`); the host `db` API calls `LiveTxn`. Built-in system functions (`_system:get`, `_system:query`, `_system:insert`, `_system:patch`, `_system:replace`, `_system:delete`) use the same path, so R1 works end to end before any bundle is deployed.
- **The sync service** is generated by `connectrpc-build` from `proto/loam/live/v1/*.proto` and served through axum on its own listener in the `operon` binary (feature `live`).

**Tech Stack:**
- Rust 1.97.1, edition 2024, workspace lints.
- New dependencies (Task 0 verifies versions, licenses and that they build together in a throwaway crate, as the M1 dependency spike did):
  - `tikv-client` (Apache-2.0) **at the git pin `https://github.com/tikv/client-rust` rev `ab4be1c2cdd58d4e593202991fb520221c83bdfd`** (2026-09-03, `version = "0.4.0"`), `default-features = false` (amended, row R4): crates.io 0.4.0 fails `cargo deny`'s advisories through its `tonic` 0.10 and misses unknown outcomes of async-commit and 1PC prewrites. The pin brings `tonic` 0.12 and `prost` 0.13 beside the workspace's 0.14; `deny.toml` gains `allow-git = ["https://github.com/tikv/client-rust"]` in Task 1. The generated `pdpb` code is private: Task 3 generates its own stubs from vendored kvproto protos (Apache-2.0, noted in `NOTICE`) until the upstream PR that exposes it merges.
  - `connectrpc` 0.9.1 (features `axum`, plus `client` as a dev-dependency for Task 12's tests) and `connectrpc-build` 0.9 (Apache-2.0); `buffa` 0.9.2 (Apache-2.0).
  - `rquickjs` 0.14.0 (MIT), features `futures`, `loader`, `macro`, `array-buffer` (amended, row R13).
  - Workspace crates reused: `tonic` 0.14 and `tonic-prost-build` 0.14 (the PD GC-state stubs), `reqwest` 0.12 (PD HTTP API), `proptest` 1, `rand` 0.9, `rand_chacha` 0.9, `axum` 0.8, `tokio`, `async-trait`, `tracing`, `thiserror`.
- TypeScript (versions checked in Task 0): `@bufbuild/protobuf` 2, `@connectrpc/connect` 2, `@connectrpc/connect-web` 2 (runtime); `@bufbuild/protoc-gen-es` 2, `@bufbuild/buf` 1, `@connectrpc/connect-node` 2 (dev); plus M1.6's toolchain (`typescript` ~7.0.2, `@biomejs/biome` ~2.5.14, `pnpm` 11.13.0, Node ≥ 22), all Apache-2.0 or MIT.
- Cluster: `tiup` 1.17.1 (Apache-2.0) with `tiup playground v8.5.8` (PD, TiKV, TiDB, all Apache-2.0; verified in the spike). Local installs use the tiup installer script; CI installs it the same way. `mysql` client (MariaDB client package) for the SQL smoke test.
- System `protoc`, as for M1.

**Spec:**
- [`docs/design/20-reactive-database-on-tikv.md`](../design/20-reactive-database-on-tikv.md): all of it; §4 (data model), §5 (transactions, journal), §6 (functions), §7 (sync), §8 (reactivity), §9 (keyspaces, GC), §10 (TiDB), §11 (metastore), §13 (failures), §14 (testing).
- [`docs/design/13-decision-log.md`](../design/13-decision-log.md): D111, D113, D116–D131; Q31–Q36.
- [`docs/design/18-metastore-backends-and-router.md`](../design/18-metastore-backends-and-router.md): §3 (the contract and its relaxations), §4.2 (the fault matrix design).
- As built: [M1.2a](2026-09-25-m1.2a-metastore-trait.md) (the trait, the conformance suite), [M1.3](2026-09-24-m1.3-hot-tier-routing.md) (the network metastore, request forwarding), [M1.6](2026-09-24-m1.6-sdks-mcp.md) (the TypeScript toolchain).

## Global Constraints

Same as the M1 overview §8, plus:
- **No forks.** PD, TiKV and TiDB run unmodified from the pinned tiup release (D126). `tikv-client` is used as published or at a pinned upstream revision; a needed change goes upstream, and until it merges Loam generates its own stubs from vendored kvproto protos.
- **Cluster tests skip without a cluster.** Every test that needs TiKV calls `operon_tikv::testing::cluster()`, which returns `None` and prints `skipped: <test> needs OPERON_TEST_PD` when the variable is unset. CI's `tikv` job sets it, so nothing is skipped there.
- **Test isolation by prefix.** Tests use the keyspaces `loam_test_meta`, `loam_test_live` and `loam_test_sql`, created once by the playground script, and a random 8-byte root prefix per test. A test never creates or deletes a keyspace.
- **API v2 only.** Every TiKV config in the repo sets `storage.api-version = 2` and `storage.enable-ttl = true`. `operon-tikv` refuses to start against a cluster whose keyspace lookup fails, with a message naming the setting.
- **Loopback only (D111, stricter).** The Live listener binds 127.0.0.1:7710. `--live-listen` accepts only loopback addresses (127.0.0.0/8, `::1`, `localhost`); any other address fails startup with the error in §20 §7.1, because `Mutate`, `_system:*` writes and `Deploy` are unauthenticated in R1. A loopback bind logs one line saying the Live API is unauthenticated. The refusal is lifted only by the unified auth plan (R3).
- **Determinism in functions.** Queries and mutations never see wall-clock time, randomness, timers or I/O except through the host API (§20 §6.2).
- **The build machine.** One cargo build at a time, the shared target directory, `-j 6`, lld; the playground (about 3.2 GB peak RSS, TiKV 2.6 GB of it, measured in the spike) is stopped before a build and started after it.
- **Ports.** Other playgrounds run on this machine and grab the default ports. Every playground Loam starts uses `--tag loam-<purpose>` and `--port-offset 17000` (PD `127.0.0.1:19379`, TiDB `127.0.0.1:21000`); CI uses the same offset for uniformity.
- **Commit areas:** `tikv`, `meta`, `live`, `sdk`, `ci`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **Tests isolate by root prefix, not by keyspace.** `TikvMetaConfig` and `LiveConfig` take a `root: Vec<u8>` that prefixes every key inside the keyspace | Keyspace creation splits regions and is slow; the conformance suite starts a fresh metastore per case | If a GC or range operation ignores the prefix, tests interfere; Task 2 has a test that two prefixes never see each other |
| 2 | **`commit_wal`, `swap_segment` and `trim_partition` are pessimistic transactions** that lock partition heads in key order with `get_for_update`; every other write is optimistic | Hot heads queue instead of aborting in a loop (§20 §11.3) | If pessimistic transactions misbehave in `tikv-client` 0.4, fall back to optimistic with a larger retry budget; the fault matrix shows it |
| 3 | **Async commit with 1PC is the default** for the metastore and Live (`commit_mode = async_1pc`; `two_pc` is the switch back) | The spike measured commit p50 30–50% lower than 2PC (§20 §5.1) | `tikv-client` 0.4.0 does not set `max_commit_ts` and `min_commit_ts` may not come from a fresh TSO, so read-after-commit can break: the metastore linearizability histories (Task 5) and the reactive and transaction checkers (Task 16) run with the default, and a failing component switches to `two_pc` (recorded as a ruling) until the upstream fix |
| 4 | **The journal has 16 shards per app in R1**, fixed at app creation | Enough for R1's single app; changing the count is an R2 catalog operation | Shard-head conflicts under high concurrency; Task 16 measures the conflict rate |
| 5 | **Index changes on non-empty tables are refused** (`FailedPrecondition: index changes need an empty table in R1`) | Online backfill is R2 | Users must recreate tables to add indexes in R1 |
| 6 | **Live's tick timestamp is a TSO timestamp**, and every query of every session of an app is evaluated at the app's current tick | Gives one timestamp per session with no coordination (§20 §8.3) | None expected; the checker verifies it |
| 7 | **The Live service and the metastore use separate keyspaces** (`loam_live_<app>` and `loam_meta`), even in dev | Mirrors production and keeps GC and prefixes independent | One more keyspace in dev |
| 8 | **`@operon/live` is a separate package with runtime dependencies** (`@bufbuild/protobuf`, `@connectrpc/connect`, `@connectrpc/connect-web`); M1.6's `@operon/client` keeps Ruling 4's zero dependencies | Generated Connect clients need their runtime; D128's M1.6 amendment is only proposed | If the owner rejects runtime dependencies, the reactive layer must be hand-written over `fetch` streaming, which R1 Task 14 would then take on |
| 9 | **R1's resume sends full results**, never diffs | Simplest correct resume (§20 §7.1) | Larger reconnect payloads |
| 10 | **Function bundles are stored with `operon-store`** under `live/<app>/deployments/<deployment_id>.js`, and the current deployment is a catalog pointer in the app's keyspace | Reuses the bucket abstraction, its fault injection and its URL backends | None |

## Carried in

None from M0 or M1. From design §20: Q32 is answered by Task 0 (no: PD and TiKV v8.5.8 have no keyspace-level GC, so Loam runs cluster-wide GC; rows R6–R7). Q33 is re-confirmed on the pinned release (row R8).

## Review Focus

1. **No missed update.** Every committed write that touches a subscribed key or range must reach the subscription by the first tick at or after its commit; journal density per shard; old and new index keys both matched. Tests: Task 9 (`journal_is_dense_under_concurrent_mutations`, `entry_visible_iff_committed`), Task 11 (`insert_into_range_invalidates`, `update_moving_out_of_range_invalidates`, `limit_bounded_range_ignores_inserts_past_last_key`), Task 16 (the reactive checker).
2. **Unknown outcomes.** A commit whose acknowledgement is lost must resolve to a known outcome through the commit token, and an idempotent mutation must never apply twice. Tests: Task 2 (`undetermined_commit_resolves_by_token`), Task 6 (the fault matrix), Task 10 (`idempotent_mutate_applies_once_across_lost_ack`).
3. **The contract on TiKV.** CAS, fences, GC claims and dense offsets under concurrency. Tests: Task 5 (the conformance suite with linearizability histories), Task 6 (fault matrix invariants).
4. **Order preservation of the tuple codec.** Tests: Task 2 (`tuple_order_matches_reference` proptest), Task 8 (`index_scan_order_matches_value_order`).
5. **Session versions.** A Transition applies only from the client's current version; merged Transitions under backpressure stay consistent; resume converges. Tests: Task 12, Task 14, Task 16.
6. **Sandbox limits and determinism.** A function cannot read the clock, randomness or the network, and cannot exceed its CPU or memory limit. Tests: Task 13.

## File structure

```
Cargo.toml                                   # + tikv-client, connectrpc, connectrpc-build, buffa, rquickjs; members
deny.toml                                    # licenses for the new trees, if needed (Task 0)
NOTICE                                       # + kvproto proto attribution (Apache-2.0)
deploy/tikv/{pd.toml,tikv.toml,tidb.toml}    # API v2, enable-ttl, pre-allocated keyspaces, TiDB keyspace-name
scripts/tikv/{playground.sh,wait-ready.sh,nemesis.sh}
.github/workflows/ci.yml                     # + jobs tikv (path-filtered PRs), tikv-nightly, sdk-live-typescript
proto/loam/live/v1/{value.proto,live.proto,journal.proto}
buf.yaml  buf.gen.yaml
crates/operon-tikv/                          # new
  Cargo.toml  build.rs
  proto/kvproto/{pdpb.proto,…}               # the subset the cluster GC RPCs need, from kvproto release-8.5 07aa8c6 (R5)
  src/{lib.rs,config.rs,keyspace.rs,tso.rs,runner.rs,faults.rs,token.rs,codec.rs,gc.rs,testing.rs}
  tests/{runner.rs,codec.rs,gc.rs,keyspace.rs}
crates/operon-meta-tikv/                     # new
  Cargo.toml
  src/{lib.rs,keys.rs,catalog.rs,leases.rs,pointers.rs,log.rs,gc.rs,changes.rs,store.rs}
  tests/{conformance.rs,fault_matrix.rs,meta_fault_matrix.tikv.expected.md}
crates/operon-live-proto/                    # new: build.rs runs connectrpc-build over proto/loam/live/v1
crates/operon-live/                          # new
  Cargo.toml
  src/{lib.rs,value.rs,ids.rs,catalog.rs,keys.rs,docs.rs,journal.rs,txn.rs,system.rs,query.rs,
       readset.rs,subs.rs,session.rs,service.rs,limits.rs,deploy.rs,error.rs,config.rs}
  tests/{value.rs,docs.rs,journal.rs,txn.rs,subs.rs,session.rs,service.rs,sql_coexistence.rs,
         reactive_checker.rs,txn_checker.rs}
crates/operon-live-js/                       # new
  Cargo.toml
  src/{lib.rs,runtime.rs,prelude.js,host.rs,limits.rs}
  tests/{functions.rs,limits.rs,determinism.rs}
crates/operon/
  Cargo.toml                                 # features tikv (default off, T7-3), live (implies tikv, default off, T7-10)
  src/{server.rs,main.rs}                    # --meta tikv://…, --live-* flags, listener
sdks/live-typescript/                        # new
  package.json  pnpm-lock.yaml  tsconfig*.json  biome.json  LICENSE
  src/gen/                                   # generated by buf (checked in)
  src/{index.ts,client.ts,session.ts,optimistic.ts,values.ts}
  test/{session.test.ts,values.test.ts,live.test.ts}
docs/plans/r1-dependency-spike.md            # Task 0
docs/plans/r1-exit-report.md                 # Task 16
docs/design/20-reactive-database-on-tikv.md  docs/design/13-decision-log.md  CHANGELOG.md  docs/plans/README.md
```

### Task 0: Reconcile and check the cluster facts

**Files:** read `crates/operon-common/src/meta/`, `crates/operon-meta-conformance/`, `crates/operon/src/{server.rs,main.rs}` as merged on `main` (M1.2a done, M1.3 as far as merged). Write `docs/plans/r1-dependency-spike.md` and fill this plan's "Rulings made during execution".

**Consumes** (each checked against the merged code; every difference is listed with its resolution):

```rust
// operon-common::meta (M1.2a Task 3, as amended by M1.3)
#[async_trait] pub trait MetaStore: Send + Sync + fmt::Debug { /* the as-built surface */ }
pub enum Consistency { Linearizable, Local }
pub struct Tracked<T>;                         // earlier_unknown + result
// operon-meta-conformance (M1.2a Task 9)
pub struct Instance { pub clients: Vec<Arc<dyn MetaStore>>, pub faults: Option<Arc<dyn Faults>>, pub guard: Box<dyn Any + Send + Sync> }
#[async_trait] pub trait Backend: Send + Sync { async fn start(&self) -> Instance; }
#[async_trait] pub trait Faults: Send + Sync { fn lose_next_ack(&self, client: usize); async fn disturb(&self, seed: u64); /* … */ }
macro_rules! metastore_conformance { … }        // one #[tokio::test] per entry of CASES
```

**Checks on a live playground** (record each result, with the command, in the spike doc):
1. Task 1's playground command starts with API v2 (verified in the spike: `SHOW CONFIG` reports `storage.api-version=2`, `storage.enable-ttl=true`); creating a keyspace through `POST /pd/api/v2/keyspaces` at runtime works (not tried in the spike, which pre-allocated); `tikv-client` 0.4.0 with `Config::with_keyspace` reads and writes it (verified).
2. **Q33 (re-confirm):** a TiDB with `keyspace-name` from the pinned release serves MySQL and its keys stay inside its keyspace (verified on v8.5.8 in the spike). Only a regression on the pinned release stops the plan.
3. **Q32:** after writing and overwriting keys in a txn keyspace with `gc_management_type = keyspace_level`, advancing that keyspace's txn and GC safe points through `AdvanceTxnSafePoint` and `AdvanceGCSafePoint` makes TiKV drop old versions (a read at an old timestamp fails with the GC error, and the store's MVCC stats shrink after compaction). If TiKV ignores it, record the fallback (a `unified` GC TiDB) and change Task 3's semantics before it starts.
4. `tikv-client`, `connectrpc`, `buffa` and `rquickjs` build together with the workspace's pins; `cargo deny check` passes; record the duplicate `tonic`/`prost`/`hyper` versions and the added build time.
5. ~~Playground flags and RAM~~: verified in the spike (Task 1's command; 3.2 GB peak). Task 0 instead records the effect of `memory-usage-limit` on TiKV's peak RSS, since the block-cache cap alone did not lower it.
6. Whether `tikv-client`'s commit error types distinguish a failed prewrite from a lost primary commit (feeds Task 2's classification). Known from the spike: an optimistic conflict is `MultipleKeyErrors[KeyError{conflict: WriteConflict{reason: Optimistic}}]`; a pessimistic lock conflict is `PessimisticLockError{WriteConflict{reason: PessimisticRetry}}` with no client retry; an existing key on `insert` is `already_exist`; a PD stall can close the TSO stream for good (`TimestampRequest channel is closed`).
7. Pick the interval index for Task 11 (a hand-written centered interval tree, or `rust-lapper` (MIT) if it supports incremental insert and delete).

**Produces:** the spike doc and a filled "Rulings made during execution" table (no empty rows).

**Commit:** `docs: record the R1 dependency spike and cluster facts`.

### Task 1: Dev cluster and the `operon-tikv` skeleton

**Files:** `deploy/tikv/{pd.toml,tikv.toml,tidb.toml}`, `scripts/tikv/{playground.sh,wait-ready.sh}`, `.github/workflows/ci.yml`, `Cargo.toml`, `crates/operon-tikv/{Cargo.toml,src/{lib.rs,config.rs,keyspace.rs,tso.rs,testing.rs}}`, `crates/operon-tikv/tests/keyspace.rs`.

**Produces:**

```rust
pub struct TikvConfig { pub pd: Vec<String>, pub keyspace: String, pub root: Vec<u8>,
                        pub request_timeout: Duration /* 5 s */, pub pd_http: Option<String> /* default http://<pd[0]> */ }
pub struct Tikv;   // Clone; holds TransactionClient (keyspace-scoped), the PD HTTP client, the TSO clock, root
impl Tikv {
    pub async fn connect(config: TikvConfig) -> Result<Self, TikvError>;   // fails with KeyspaceMissing{name} or ApiVersion{hint}
    pub fn root(&self) -> &[u8];
    pub fn key(&self, suffix: &[u8]) -> Vec<u8>;                          // root ‖ suffix
    pub async fn now(&self) -> Result<Timestamp, TikvError>;             // a fresh TSO timestamp
    pub fn physical_ms(ts: &Timestamp) -> u64;
}
pub async fn ensure_keyspace(pd_http: &str, name: &str) -> Result<KeyspaceMeta, TikvError>;  // create if absent; idempotent
pub mod testing { pub async fn cluster() -> Option<TestCluster>; }   // OPERON_TEST_PD; random root per call
pub struct TestCluster { pub pd: Vec<String>, pub pd_http: String }
impl TestCluster { pub fn config(&self, keyspace: &str) -> TikvConfig; }   // keyspace from the three test keyspaces
```

**Semantics:**
1. **Configs** (the spike's working files, with Loam's keyspaces and an explicit memory limit):

   `deploy/tikv/tikv.toml`:
   ```toml
   memory-usage-limit = "3GB"          # TiKV otherwise sizes itself from host RAM (12 GB on a 16 GB host)
   [storage]
   api-version = 2
   enable-ttl = true
   [storage.block-cache]
   capacity = "1GB"
   ```
   `deploy/tikv/pd.toml`:
   ```toml
   [keyspace]
   pre-alloc = ["loam_meta", "loam_live_dev", "sql_dev", "loam_test_meta", "loam_test_live", "loam_test_sql"]
   ```
   `deploy/tikv/tidb.toml` (the CI variant `tidb-test.toml` names `loam_test_sql`):
   ```toml
   keyspace-name = "sql_dev"
   ```
2. **Install and start** (`playground.sh start [--with-tidb] [--stores N] [--tag T]`):
   ```sh
   # once; user-local under ~/.tiup (the installer edits ~/.zshrc only: add ~/.tiup/bin to PATH in fish yourself)
   curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
   export PATH="$HOME/.tiup/bin:$PATH"      # fish: fish_add_path ~/.tiup/bin
   # the first run downloads ~500 MB (the TiKV tarball is 402 MB)
   tiup playground v8.5.8 --tag loam-dev --port-offset 17000 --pd 1 --kv 1 --db 1 --tiflash 0 --without-monitor \
     --kv.config deploy/tikv/tikv.toml --pd.config deploy/tikv/pd.toml --db.config deploy/tikv/tidb.toml
   ```
   Without `--with-tidb` the script passes `--db 0`. It runs in the background with its pid in `target/tikv-playground/<tag>.pid`. `wait-ready.sh` then polls `curl -s http://127.0.0.1:19379/pd/api/v2/keyspaces` until every pre-allocated keyspace is listed, and with TiDB also runs `mysql -h127.0.0.1 -P21000 -uroot -e 'select 1'` (60 s limit). The script refuses to start if `cargo` or `rustc` is running (the build-machine rule), unless `--force`.
3. **Stop** (`playground.sh stop [--tag T]`): `kill -INT <pid>`, wait for exit, then `rm -rf ~/.tiup/data/<tag>`. `tiup clean <tag>` fails after an INT shutdown ("missing meta file"), so the script never uses it.
4. CI job `tikv` ("TiKV suites"): runs on PRs whose paths match `crates/operon-tikv/**`, `crates/operon-meta-tikv/**`, `crates/operon-live*/**`, `deploy/tikv/**`, `scripts/tikv/**`, `proto/loam/**`; installs tiup (caching `~/.tiup/components`) and appends `$HOME/.tiup/bin` to `$GITHUB_PATH`, starts the playground, sets `OPERON_TEST_PD=127.0.0.1:19379`, runs `cargo test -p operon-tikv -p operon-meta-tikv -p operon-live -p operon-live-js`. Job `tikv-nightly` runs the same plus later tasks' nightly suites on a schedule.

**Tests** (`tests/keyspace.rs`): `connect_to_missing_keyspace_names_it`; `ensure_keyspace_is_idempotent`; `two_roots_never_see_each_other` (writes under two random roots, scans each); `now_is_monotonic` (1 000 calls); `physical_ms_is_close_to_wall_clock` (within 1 s); `client_without_keyspace_is_refused_with_a_hint` (the `InvalidKeyMode` error on API v2 becomes `TikvError::ApiVersion` naming the keyspace setting).

**Commit:** `tikv: add the dev playground and the TiKV client skeleton`; `ci: run the TiKV suites against a tiup playground`.

### Task 2: The transaction runner, commit tokens, faults and the tuple codec

**Files:** `crates/operon-tikv/src/{runner.rs,faults.rs,token.rs,codec.rs}`, `crates/operon-tikv/tests/{runner.rs,codec.rs}`.

**Produces:**

```rust
pub enum Mode { Optimistic, Pessimistic }
pub struct TxnOptions { pub mode: Mode, pub max_attempts: u32 /* 8 */, pub deadline: Duration /* 10 s */,
                        pub commit_token: bool, pub op: &'static str /* for the fault plan and metrics */ }
pub enum TxnError { Conflict, NotApplied(String), Undetermined { token: Option<[u8; 16]> }, Fatal(String), Deadline }
impl Tikv {
    /// Runs `body` in a new transaction and commits it; reruns `body` on Conflict. With `commit_token`,
    /// writes t/<token> in the transaction and resolves an undetermined commit by reading it at a fresh ts.
    pub async fn run<T, F>(&self, opts: TxnOptions, body: F) -> Result<Committed<T>, TxnError>
        where F: FnMut(&mut Txn) -> BoxFuture<'_, Result<T, TxnError>>;
    pub async fn snapshot(&self, at: Timestamp) -> Snap;
}
pub struct Committed<T> { pub value: T, pub commit_ts: Timestamp, pub attempts: u32, pub earlier_unknown: bool }
pub struct Txn;    // get, batch_get, get_for_update, scan, scan_reverse, put, delete, lock_keys; start_ts(); all keys under root
pub struct Snap;   // get, batch_get, scan, scan_reverse at a timestamp
// faults.rs
pub enum FaultPoint { BeforeBegin, BeforePrewrite, BeforeCommit, AfterCommit }
pub enum Fault { Refuse, Conflict, LoseAck, Delay(Duration) }
pub trait FaultPlan: Send + Sync { fn at(&self, op: &str, point: FaultPoint, attempt: u32) -> Option<Fault>; }
impl Tikv { pub fn with_faults(self, plan: Arc<dyn FaultPlan>) -> Self; }   // feature "faults"
// codec.rs
pub mod tuple { pub enum Elem<'a> { Null, I64(i64), F64(f64), Bool(bool), Str(&'a str), Bytes(&'a [u8]), Array(Vec<Elem<'a>>) }
                pub fn encode(out: &mut Vec<u8>, e: &Elem); pub fn decode(buf: &[u8]) -> Result<(Elem<'static>, usize), CodecError>;
                pub fn successor(prefix: &[u8]) -> Vec<u8>; }        // the exclusive end of a prefix range
```

**Semantics:**
1. **Classification** (the pinned client's mapping, row R9; amended): `WriteConflict` (optimistic, and pessimistic `PessimisticRetry`, which the client does not retry itself) and `KeyIsLocked` after the client's own backoff → `Conflict`, and the runner restarts the whole transaction at a new start timestamp; `KeyError.already_exist` → the caller's typed "exists" error; `Error::UndeterminedError` and a commit future the runner drops at its deadline → `Undetermined` (the only two sources); `TimestampRequest channel is closed`, region errors, TSO unavailable, `ServerIsBusy` and every other error after which the transaction did not commit → `NotApplied`; invalid arguments and unknown error kinds → `Fatal`.
2. **Undetermined resolution:** with a token, read `t/<token>` at a fresh timestamp (TiKV resolves the lock or rolls it back); present → success with `earlier_unknown = true`; absent → `NotApplied`. Without a token, return `Undetermined`.
3. Token keys carry `expires_ms = now + 30 min`; a sweep in Task 3's GC loop deletes expired tokens.
4. **Commit mode.** `TxnOptions` gains `commit_mode: CommitMode { Async1pc, TwoPc }`, from config and defaulting to `Async1pc` (Ruling 3). It maps to `use_async_commit()` + `try_one_pc()`.
5. **TSO supervisor.** `Tikv` holds the `TransactionClient` behind a `std::sync::RwLock<Arc<_>>` (no new dependency). On `TimestampRequest channel is closed`, or on three consecutive TSO failures, it rebuilds the client (with backoff, one rebuild at a time) and retries the operation as `NotApplied`. The rebuild count is a metric.
5a. **Scan paging** (amended, row R10 and T2-12). A TiKV response over the client's gRPC decoding limit fails with `OutOfRange`. The limit is raised from gRPC's 4 MiB default to 16 MiB (`Config::with_grpc_max_decoding_message_size`, `TikvConfig.grpc_max_decoding_bytes`) as headroom; paging is the guarantee. `Txn::scan`, `Snap::scan` and `batch_get` page internally: 256 keys per request, halved on `OutOfRange` down to 1, and doubled back after a success; a caller's `limit` is the total. `Txn::put` refuses a value over 2 MiB (`TxnError::Fatal("value over 2 MiB")`), so a one-key page (key, value and framing) always stays under the 4 MiB limit; Live's 1 MiB document limit (§20 §4.1) is separate and stricter.
5b. **Reads below the GC safe point are refused** (amended, row R7). TiKV serves them without an error, returning versions GC may already have dropped. `Tikv::snapshot(at)` and `Txn` reads fail with `TxnError::Fatal("read below the GC safe point")` (`TikvError::GcSafePoint { at, safe_point }` from `Tikv`) when `at` is older than `now − (gc life_time − 1 min)` and no GC barrier covers it.
6. **Error scrubbing.** Keys in errors are shown with the keyspace prefix and root removed, and truncated to 64 bytes.
7. The tuple codec implements §20 §4.3's table exactly: tags, escaping, terminators, float transform, NaN last, `-0.0 == 0.0`.

**Tests:**
- `tests/runner.rs`: `conflicting_writers_both_finish_one_retries`; `pessimistic_lock_queues_second_writer`; `undetermined_commit_resolves_by_token` (fault `LoseAck` at `AfterCommit`); `refused_before_prewrite_is_not_applied`; `deadline_stops_retries`; `faults_fire_per_op_and_attempt`; `pessimistic_retry_restarts_the_transaction` (two `get_for_update` holders: the loser restarts and both finish); `tso_stream_loss_rebuilds_the_client` (a test hook closes the TSO stream; the next `run` succeeds after a rebuild); `commit_mode_two_pc_and_async_1pc_both_commit`; `scan_pages_stay_under_the_grpc_limit` (2 000 values of 64 KiB scanned in one call); `put_over_2_mib_is_refused`; `snapshot_below_the_safe_window_is_refused` (amended).
- `tests/codec.rs`: `tuple_order_matches_reference` (proptest: `encode(a) < encode(b)` iff `cmp_ref(a, b) == Less`, over nested arrays, strings with `0x00`, NaN, ±0, i64 extremes); `decode_inverts_encode`; `successor_bounds_every_extension`.

**Commit:** `tikv: add the transaction runner with retries, commit tokens and fault hooks`; `tikv: add the order-preserving tuple codec`.

### Task 3: The cluster MVCC GC loop

Amended by rows R5–R7: PD v8.5.8 has no keyspace GC-state RPCs and TiKV v8.5.8 reads only the cluster safe point (Q32), so Loam is the cluster's GC worker, doing what TiDB's GC worker does, for every keyspace. No TiDB without `keyspace-name` may run on a Loam cluster (it would be a second GC worker).

**Files:** `crates/operon-tikv/{build.rs,proto/kvproto/*.proto,src/gc.rs}`, `NOTICE`, `crates/operon-tikv/tests/gc.rs`.

**Produces:**

```rust
pub struct GcConfig { pub life_time: Duration /* 10 min */, pub interval: Duration /* 1 min */, pub lease_key: Vec<u8> /* e/cluster/gc in loam_meta */ }
pub struct GcLoop;
impl GcLoop {
    pub fn spawn(tikv: Tikv, config: GcConfig, shutdown: CancellationToken) -> GcHandle;   // tikv: a handle on loam_meta
    pub async fn run_once(&self) -> Result<GcReport, TikvError>;   // service safe point, resolve locks in every keyspace, UpdateGCSafePoint
}
pub struct GcReport { pub target: Timestamp, pub safe_point: Timestamp, pub keyspaces: u32, pub locks_resolved: u64,
                      pub tokens_swept: u64, pub held_by: Option<String> /* the service safe point that held it back */ }
pub struct GcBarrier;   // set(service_id, ts, ttl), delete(service_id): a PD service safe point; holds GC below ts
impl Tikv { pub async fn gc_safe_point(&self) -> Result<Timestamp, TikvError>; }   // GetGCSafePoint, cached ≤ 10 s
```

**Semantics:**
1. The vendored protos are the `pdpb.proto` subset (`GetMembers`, `GetGCSafePoint`, `UpdateGCSafePoint`, `UpdateServiceGCSafePoint`) and its imports from kvproto `release-8.5` at `07aa8c6a46fab0a4cd577119c249c9c8e718c553` (the revision TiKV v8.5.8 builds with), unchanged; `NOTICE` lists them as Apache-2.0 files from pingcap/kvproto. Stubs are generated with the workspace's `tonic-prost-build` 0.14. The GC-state RPCs of PD master (`AdvanceTxnSafePoint`, `AdvanceGCSafePoint`, `SetGCBarrier`, `GetGCState`) answer `Unimplemented` on v8.5.8 and are not used.
2. One loop per cluster: each run takes the lease `e/cluster/gc` in the `loam_meta` keyspace (epoch-fenced), computes `target = now − life_time`, calls `UpdateServiceGCSafePoint("gc_worker", ttl = i64::MAX, target)` and sets `safe_point = min(target, min_safe_point)` (the minimum covers every service safe point: Loam's barriers, BR, TiCDC). It lists the keyspaces through `GET /pd/api/v2/keyspaces`, runs `cleanup_locks(.., safe_point)` with a keyspace-scoped client for each keyspace, TiDB's included (a keyspace-mode TiDB resolves no locks, TiDB `gc_worker.go:386-389` at v8.5.8), then calls `UpdateGCSafePoint(safe_point)`. "Each keyspace" means every state but `TOMBSTONE` (whose range is gone): a lock in an `ENABLED`, `DISABLED` or `ARCHIVED` keyspace must be resolved before the cluster safe point passes it, and if a client cannot be built for one, the run fails without advancing the safe point. It also sweeps expired commit tokens in the keyspaces Loam owns.
3. `GcBarrier::set` is `UpdateServiceGCSafePoint(service_id, ttl, ts)` with `service_id = "loam/<purpose>/<id>"`; `delete` sends `ttl = 0`. PD drops a barrier whose TTL expires.
4. TiKV drops versions below the safe point only when RocksDB compacts (`gc.enable-compaction-filter = true`, the default), and serves reads below it without an error (row R7), so correctness rests on Task 2's read refusal, not on physical removal.

**Tests** (`tests/gc.rs`): `safe_point_advances_cluster_wide` (after `run_once`, `GetGCSafePoint` equals the report's `safe_point`, and TiKV's `tikv_gcworker_autogc_safe_point` on the status port reaches it within 30 s); `old_versions_dropped_after_gc` (nightly only: turns `gc.enable-compaction-filter` off through TiKV's `POST /config`, then a read at an old timestamp no longer sees the overwritten value; turns it back on); `barrier_holds_safe_point`; `only_one_loop_runs_per_cluster` (two loops, one lease); `locks_below_the_safe_point_are_resolved_in_every_keyspace`; `expired_tokens_are_swept`.

**Commit:** `tikv: run cluster MVCC GC through PD's service safe points`.

### Task 4: `operon-meta-tikv` (1/2): catalog, leases, pointers, clock

**Files:** `crates/operon-meta-tikv/{Cargo.toml,src/{lib.rs,keys.rs,catalog.rs,leases.rs,pointers.rs,store.rs}}`, `crates/operon-meta-tikv/tests/conformance.rs`.

**Produces:**

```rust
pub struct TikvMetaConfig { pub tikv: TikvConfig, pub id_block: u64 /* 1 000 */, pub poll: Duration /* 100 ms */ }
pub struct TikvMeta;                            // Clone + Debug
impl TikvMeta { pub async fn open(config: TikvMetaConfig) -> Result<Self, MetaError>; }
#[async_trait] impl MetaStore for TikvMeta { /* catalog, leases, pointers, clock now; the log and GC methods in Task 5 */ }
```

**Semantics:**
1. Keys exactly as §20 §11.2, under the root prefix.
2. `clock_ms` returns the physical time of a fresh TSO timestamp. `now_ms` is synchronous in the trait, so it cannot fetch one (amended, row R2): it returns `max(previous now_ms, physical(latest timestamp this handle obtained) + elapsed since)`, which is monotonic and never behind the TSO. Stamps written inside a transaction use its start timestamp's physical time; lease deadlines compare against it too.
3. Creates follow the trait's retry-safe rules: a create that finds its name record returns the existing id; ids come from per-node blocks of `id_block`.
4. `cas_pointer` checks the fence's lease epoch, the collection's `live` state and the new manifest path's `gc_claim` in the same transaction (§20 §11.3).
5. Methods of Task 5 return `MetaError::Unavailable("not implemented in R1 Task 4")` until then.

**Tests** (`tests/conformance.rs`): `metastore_conformance!` over `TikvBackend` (one `TikvMeta` per case on a random root, `clients` = 3 handles on the same root, `faults` = the `FaultPlan` adapter), restricted to the cases whose methods are all in Task 4's set (row R3: the macro has no `only` form, so Task 4 adds `metastore_conformance!(backend; cases = [..])` to `operon-meta-conformance`, a test-only change, amended). Task 4's methods are the clock and readiness, the catalog (including `links_with_pointers`), leases (including `leases_with_prefix`), pointers and collections (including `set_collection_hot` and `collection_hot`); `watch_changes` returns a watch that wakes every `poll` (spurious wake-ups are allowed) until Task 5. Plus `clock_is_tso_physical`, `id_blocks_leave_gaps_but_never_repeat`.

Amended by rows T4-1–T4-15: calls whose outcome was unknown are rerun (T4-3), partition heads are written lazily (T4-5), and `Backend::unavailable` lets cluster cases skip (T4-2).

**Commit:** `meta: add the TiKV metastore catalog, leases, pointers and clock`.

### Task 5: `operon-meta-tikv` (2/2): the log, GC reads and changes

**Files:** `crates/operon-meta-tikv/src/{log.rs,gc.rs,changes.rs,store.rs}`, `crates/operon-meta-tikv/tests/conformance.rs`.

**Produces:** the remaining trait methods: `commit_wal`, `swap_segment`, `trim_partition`, `partition_index`, `stream_state`, `retired_expired`, `forget_objects`, `prune_wal_commits`, `orphan_wal_objects`, `orphan_segments`, `segment_referenced`, `collection_roots`, `watch_changes`, and any others the as-built trait has.

**Semantics:**
1. `commit_wal`: pessimistic; `get_for_update` on every touched head in key order; dedupe on `w/…`; assign dense offsets; write index entries, heads, `w/`, `W/` and `o/` rows; check no `gc_claim` on the WAL object; one transaction per partition group of at most 1 024 chunks and 4 MiB of writes; a larger call is split without splitting one stream's chunks (a single stream over one group is refused with `InvalidArgument`), each group has its own `w/<object>/<group>` record, success only after every group commits, and a retry recommits only missing groups (D59, §20 §11.3).
2. `partition_index`: one snapshot; the head, then `[log_start, next)` entries by range scan.
3. `watch_changes`: per-scope counters `v/…` bumped by catalog writes (not by `commit_wal`, D63) plus in-process wake on the handle's own writes and a 100 ms poll; `commit_wal` wakes this handle's offset watchers directly.
4. GC reads scan `r/` shards and read `o/` rows; `forget_objects` checks and clears claims in one transaction per batch.

**Tests:** the full `metastore_conformance!` suite (all 53 cases, linearizability histories included; amended, row R3), plus `commit_wal_is_atomic_across_partitions` (one group: a crash fault leaves all or nothing), `oversized_commit_wal_commits_per_group_and_retry_completes` (a call over one group, fault `LoseAck` after the first group: the retry commits only the rest, no offset assigned twice), `one_stream_never_spans_groups`, `hot_head_commits_queue_without_abort_loops` (16 concurrent writers to one partition, every commit succeeds, offsets dense).

Amended by rows T5-1–T5-13: the pessimistic writes read through a snapshot taken after their locks (T5-3), no `v/` counters (T5-4), `o/` rows are checked and locked but not written (T5-5), and stamped writes wait for the TSO to reach `now_ms` (T5-7).

**Commit:** `meta: add the TiKV metastore log, GC and change-feed methods`.

### Task 6: The TiKV fault matrix and `operon dev --meta tikv://`

**Files:** `crates/operon-meta-tikv/tests/{fault_matrix.rs,meta_fault_matrix.tikv.expected.md}`, `crates/operon/{Cargo.toml,src/server.rs,src/main.rs}`, `crates/operon/tests/meta_tikv.rs`, `.github/workflows/ci.yml`.

**Produces:**

```rust
// operon: ServerConfig gains
pub enum MetaBackend { Raft /* default: the as-built MetaNode + MetaClient */, #[cfg(feature = "meta-tikv")] Tikv(operon_meta_tikv::TikvMetaConfig) }
pub meta: MetaBackend,                       // ServerConfig field (amended, row R16)
// Server: node: Option<MetaNode>, meta: Option<MetaClient>; Server::meta() -> Option<&MetaClient> (tests/it/http.rs updated)
// CLI (dev, standalone only; `operon cluster` refuses it): --meta tikv://<pd-host:port>[,<pd…>]/<keyspace>   (default: the embedded openraft store)
```

**Semantics:**
1. The matrix crosses the method groups of §18 §4.2 (catalog creates, `commit_wal`, `swap_segment`, trim, `cas_pointer` with and without a fence, leases, `drop_collection`) with the faults `Refuse` (`BeforeSend`), `LoseAck` (`AfterApply`), the undetermined path, `Conflict`, `Delay`, and a `Race` hook, at attempts 1 and 2. Each cell ends `Retried`, `SurfacedUnknown`, `SurfacedRetryable` or `NoEffect`, and the table must equal the blessed file (`BLESS=1` rewrites it).
2. Invariants after every cell: the suite's state checks, no WAL object committed twice, no acknowledged write lost, pointer versions dense.
3. `operon dev --meta tikv://127.0.0.1:2379/loam_meta` runs every role on the TiKV metastore; the GC loop of Task 3 starts with it.
4. Nightly: the M1.1 gates run with `--meta tikv://…`.

**Tests:** the matrix; `crates/operon/tests/meta_tikv.rs`: `dev_on_tikv_ingests_and_searches` (create a collection, write, read back strongly), `restart_keeps_state` (stop, start on the same keyspace and root).

Amended by rows T6-1–T6-12: `--meta` also takes `?root=<hex>` (T6-1), a fifth outcome `Rejected` for `Race` cells (T6-3), `TikvMeta::check_invariants` is the state check (T6-4), every metastore write commits with two-phase commit (T6-5), and the nightly M1.1 gates on TiKV are the kill -9 crash gate (T6-8).

**Commit:** `meta: add the TiKV metastore fault matrix`; `operon: select the TiKV metastore with --meta tikv://`.

### Task 7: The `loam.live.v1` protos and code generation

**Files:** `proto/loam/live/v1/{value.proto,live.proto,journal.proto}`, `buf.yaml`, `buf.gen.yaml`, `crates/operon-live-proto/{Cargo.toml,build.rs,src/lib.rs}`, `sdks/live-typescript/{package.json,src/gen/**}`, `.github/workflows/ci.yml`.

**Produces:**
- `value.proto`: `Value` (oneof `null_value`, `int64_value`, `double_value`, `bool_value`, `string_value`, `bytes_value`, `array_value`, `object_value`), `Array`, `Object` (`map<string, Value>`), `DocumentRecord { uint32 format = 1; uint64 creation_ms = 2; map<string, Value> fields = 3; }`.
- `live.proto`: `LiveService` with the five RPCs and messages of §20 §7.1 (`WatchRequest { oneof { QuerySet initial; Resume resume; } }`, `QuerySet { uint64 version; repeated QuerySpec queries; }`, `QuerySpec { uint32 query_id; string function; Value args; }`, `ModifyQuerySetRequest { string session_id; uint64 base_version; uint64 new_version; repeated QuerySetChange changes; }`, `QueryRequest`, `MutateRequest { string function; Value args; optional string idempotency_key; }`, `MutateResponse { uint64 commit_ts; Value result; }`, `DeployRequest { bytes bundle; Schema schema; }`, `Transition`, `StateVersion`, `QueryUpdate`, `LiveError { code; message; }`).
- `journal.proto`: `JournalEntry` and `WriteRecord` (§20 §5.3), internal.
- Rust: `operon_live_proto::loam::live::v1::*` (buffa messages, connect-rust service trait and client), generated in `build.rs` with `connectrpc-build` and the system `protoc`.
- TypeScript: `sdks/live-typescript/src/gen/` generated by `buf generate` (protoc-gen-es with its Connect service descriptors), checked in.

**Semantics:** `buf lint` passes with the `STANDARD` rules; `buf breaking` is not enforced in R1 (the API is unstable). CI regenerates the TypeScript code and fails if `git diff` is not empty.

**Tests:** `crates/operon-live-proto/tests/roundtrip.rs`: `value_binary_and_json_roundtrip` (every variant; int64 extremes as JSON strings); `document_record_roundtrip`. CI's regeneration check.

**Commit:** `live: add the loam.live.v1 protos and generated code`.

### Task 8: The Live data model

**Files:** `crates/operon-live/{Cargo.toml,src/{lib.rs,value.rs,ids.rs,catalog.rs,keys.rs,docs.rs,limits.rs,error.rs,config.rs}}`, `crates/operon-live/tests/{value.rs,docs.rs}`.

**Produces:**

```rust
pub enum LiveValue { Null, I64(i64), F64(f64), Bool(bool), Str(String), Bytes(Vec<u8>), Array(Vec<LiveValue>), Object(BTreeMap<String, LiveValue>) }
impl LiveValue { pub fn from_proto(v: pb::Value) -> Result<Self, LiveError>; pub fn to_proto(&self) -> pb::Value; pub fn index_elem(&self) -> Result<tuple::Elem<'_>, LiveError>; }
pub struct DocId { pub table: TableId, pub bytes: [u8; 16] }   // Display/FromStr: Crockford base32 of varint(table) ‖ bytes ‖ crc16
pub struct TableDef { pub id: TableId, pub name: String, pub indexes: Vec<IndexDef> }
pub struct IndexDef { pub id: IndexId, pub name: String, pub fields: Vec<String> }   // by_id and by_creation_time implicit
pub struct IndexRange { pub table: TableId, pub index: IndexId, pub eq: Vec<LiveValue>,
                        pub lower: Bound<LiveValue>, pub upper: Bound<LiveValue>, pub order: Order, pub limit: Option<u32> }
pub struct Doc { pub id: DocId, pub creation_ms: u64, pub fields: BTreeMap<String, LiveValue> }
pub struct Limits { /* §20 §5.1 R1 defaults, and §20 §4.1 document limits */ }
// docs.rs: operations inside a Txn (Task 2), used by LiveTxn in Task 10
pub async fn get(txn: &mut Txn, app: &AppKeys, id: DocId) -> Result<Option<Doc>, LiveError>;
pub async fn insert(txn: &mut Txn, app: &AppKeys, table: &TableDef, fields: BTreeMap<String, LiveValue>) -> Result<(DocId, WriteRecord), LiveError>;
pub async fn replace(…) -> Result<WriteRecord, LiveError>;  pub async fn patch(…) -> Result<WriteRecord, LiveError>;
pub async fn delete(…) -> Result<Option<WriteRecord>, LiveError>;
pub async fn scan(txn_or_snap: &mut impl Reads, app: &AppKeys, range: &IndexRange) -> Result<(Vec<Doc>, KeyRange /* read-set range */), LiveError>;
```

**Semantics:**
1. Keys exactly as §20 §4.3 (dedicated keyspace layout; the shared-keyspace prefix is reserved but unused in R1).
2. Every write returns a `WriteRecord` with the document key and the removed and added index keys (the journal needs both).
3. `_creationTime` is the physical time of the transaction's start timestamp; `_id` bytes come from the OS RNG (mutations stay deterministic because ids are not visible to reads until after commit, and a rerun draws new ids for its own inserts).
4. Limits are enforced with typed errors naming the limit.
5. Tables are created on first insert unless a deployed schema exists (Task 13); index changes on non-empty tables are refused (Ruling 5).

**Tests:** `tests/value.rs`: `int_and_float_are_distinct`, `index_elem_rejects_objects`, `doc_id_text_roundtrip_and_checksum`. `tests/docs.rs`: `index_scan_order_matches_value_order` (proptest over random documents); `patch_moves_index_entries`; `delete_removes_all_index_entries`; `write_record_lists_old_and_new_index_keys`; `limit_bounded_scan_reports_range_to_last_key`; `document_over_1_mib_is_refused`.

**Commit:** `live: add documents, tables, indexes and their key layout`.

### Task 9: The commit journal and its tailer

**Files:** `crates/operon-live/src/journal.rs`, `crates/operon-live/tests/journal.rs`.

**Produces:**

```rust
pub struct Journal { /* app keys, shard count (16) */ }
impl Journal {
    pub async fn append(&self, txn: &mut Txn, entry: JournalEntry, rng: &mut impl Rng) -> Result<(u16, u64), LiveError>;  // (shard, seq)
    pub async fn heads(&self, snap: &Snap) -> Result<Vec<u64>, LiveError>;                 // one batch_get
    pub async fn read(&self, snap: &Snap, from: &[u64], to: &[u64]) -> Result<Vec<(u16, u64, JournalEntry)>, LiveError>;
}
pub struct Tailer;   // positions per shard; tick(at: Timestamp) -> Batch { at, entries }
pub struct Janitor;  // consumer checkpoints (per consumer id, per shard); deletes entries below min(checkpoints) older than 10 min
```

**Semantics:** as §20 §5.3: read the head at the start timestamp, write `seq = head + 1` and the head; a conflict on the head is a normal `Conflict` and the rerun picks another shard. Read-only mutations write no entry. `Tailer::tick` reads heads at `at`, then entries in `(position, head]` per moved shard, and advances positions only after the caller acknowledges the batch.

**Tests:** `journal_is_dense_under_concurrent_mutations` (32 concurrent writers, 2 000 mutations, every shard's sequence has no gap); `entry_visible_iff_committed` (an aborted mutation leaves no entry; a committed one is visible exactly from its commit timestamp); `tailer_sees_each_entry_once_across_ticks`; `janitor_respects_slowest_consumer`.

**Commit:** `live: add the sharded, sequenced commit journal`.

### Task 10: `LiveTxn`, the mutation runner and the system functions

**Files:** `crates/operon-live/src/{txn.rs,system.rs,query.rs}`, `crates/operon-live/tests/txn.rs`.

**Produces:**

```rust
pub struct ReadSet { pub points: BTreeSet<Vec<u8>>, pub ranges: Vec<KeyRange> }   // KeyRange = [lo, hi) of one index
pub struct LiveTxn<'a> { /* Txn or Snap, ReadSet, pending WriteRecords, limits counters */ }
impl LiveTxn<'_> {
    pub async fn get(&mut self, id: DocId) -> Result<Option<Doc>, LiveError>;       // point read; promoted with lock_keys in mutations
    pub async fn query(&mut self, range: IndexRange) -> Result<Vec<Doc>, LiveError>;
    pub async fn insert(&mut self, table: &str, fields: BTreeMap<String, LiveValue>) -> Result<DocId, LiveError>;
    pub async fn patch(&mut self, id: DocId, fields: BTreeMap<String, LiveValue>) -> Result<(), LiveError>;
    pub async fn replace(&mut self, id: DocId, fields: BTreeMap<String, LiveValue>) -> Result<(), LiveError>;
    pub async fn delete(&mut self, id: DocId) -> Result<(), LiveError>;
    pub fn start_ts(&self) -> Timestamp;
}
pub trait Function: Send + Sync { fn kind(&self) -> FnKind; fn call<'a>(&'a self, txn: &'a mut LiveTxn<'_>, args: LiveValue) -> BoxFuture<'a, Result<LiveValue, LiveError>>; }
pub struct Runner;
impl Runner {
    pub async fn mutate(&self, f: &dyn Function, args: LiveValue, idempotency_key: Option<String>) -> Result<Mutated, LiveError>;  // commit_ts, result
    pub async fn query(&self, f: &dyn Function, args: LiveValue, at: Timestamp) -> Result<Queried, LiveError>;  // result, ReadSet
}
// system.rs: _system:get, _system:query, _system:insert, _system:patch, _system:replace, _system:delete as `Function`s
```

**Semantics:**
1. A mutation runs in an optimistic `Tikv::run` with `commit_token = true`; on `Conflict` the function reruns from scratch; the journal entry is appended just before commit when the write set is non-empty.
2. Point reads in mutations call `lock_keys` on the document key (§20 §5.2); range reads do not.
3. With an idempotency key, the record `0x05 ‖ hash(key)` is read first (a hit returns the recorded `commit_ts` and result without running the function) and written in the transaction with the result (results over 1 MiB are refused for idempotent calls); records expire after 24 h and the janitor sweeps them.
4. A query runs on a `Snap` at the given timestamp and returns its `ReadSet`.
5. Limits (§20 §5.1) are counted per attempt.

**Tests:** `rerun_on_conflict_sees_new_snapshot`; `point_read_promotion_prevents_write_skew` (two mutations that read each other's document by id and write their own: one retries, the invariant holds); `range_write_skew_is_possible_and_documented` (the same with range reads: asserts the anomaly can occur, pinning the documented behaviour until Q31); `idempotent_mutate_applies_once_across_lost_ack` (fault `LoseAck`); `read_set_covers_points_and_ranges`; `read_only_mutation_writes_no_journal_entry`; `scan_limit_is_enforced`.

**Commit:** `live: add LiveTxn with read sets, the mutation runner and system functions`.

### Task 11: The subscription manager

**Files:** `crates/operon-live/src/{readset.rs,subs.rs}`, `crates/operon-live/tests/subs.rs`.

**Produces:**

```rust
pub struct ReadSetIndex;   // per (table, index): a hand-written augmented interval tree over byte-string KeyRanges → SubId, with incremental insert and remove (row R14); hash map of point keys → SubId
impl ReadSetIndex { pub fn insert(&mut self, id: SubId, rs: &ReadSet); pub fn remove(&mut self, id: SubId);
                    pub fn stab(&self, w: &WriteRecord, out: &mut HashSet<SubId>); }
pub struct SubKey { pub function: String, pub args_digest: [u8; 32] }   // identity joins it in R3
pub struct Subscriptions;  // one per app on this node
impl Subscriptions {
    pub fn spawn(app: App, runner: Arc<Runner>, config: SubsConfig, shutdown: CancellationToken) -> Self;
    pub async fn subscribe(&self, key: SubKey, f: Arc<dyn Function>, args: LiveValue) -> Result<(SubId, Arc<SubResult>), LiveError>;
    pub fn unsubscribe(&self, id: SubId);
    pub fn updates(&self) -> broadcast::Receiver<Tick>;   // Tick { at, changed: Vec<(SubId, Arc<SubResult>)> }
}
pub struct SubsConfig { pub poll_min: Duration /* 20 ms */, pub poll_max: Duration /* 200 ms */, pub rerun_concurrency: usize /* 16 */,
                        pub min_rerun_interval: Duration /* 50 ms */, pub safety_rerun: Duration /* 5 min */ }
```

**Semantics:** §20 §8.2 steps 1–5: tick on a TSO timestamp (woken by local commits), tail the journal, stab every write record's document key and removed and added index keys, rerun invalidated subscriptions at the tick with bounded concurrency, replace read sets, publish changed results; identical `SubKey`s share one subscription with a reference count; the safety rerun compares and counts `live_missed_invalidation_total` on a difference.

**Tests:** `insert_into_range_invalidates`; `update_moving_out_of_range_invalidates`; `unrelated_write_does_not_rerun`; `limit_bounded_range_ignores_inserts_past_last_key`; `identical_subscriptions_share_one_rerun`; `burst_of_writes_coalesces_to_newest_tick`; `safety_rerun_detects_injected_miss` (a test hook drops one journal batch; the safety rerun repairs it and counts it).

**Commit:** `live: add read-set tracking, journal invalidation and query reruns`.

### Task 12: Sessions and the sync service

**Files:** `crates/operon-live/src/{session.rs,service.rs,deploy.rs}`, `crates/operon/{Cargo.toml,src/server.rs,src/main.rs}`, `crates/operon-live/tests/{session.rs,service.rs}`.

**Produces:**

```rust
pub struct LiveConfig { pub tikv: TikvConfig, pub app: String, pub listen: SocketAddr /* 127.0.0.1:7710 */,
                        pub store: Store /* bundles */, pub subs: SubsConfig, pub session_queue: usize /* 16 */,
                        pub heartbeat: Duration /* 15 s */, pub blocked_limit: Duration /* 30 s */ }
pub struct LiveServer;
impl LiveServer { pub async fn start(config: LiveConfig, shutdown: CancellationToken) -> Result<LiveHandle, LiveError>; }
pub struct LiveHandle { pub addr: SocketAddr }
// operon CLI (dev, standalone; feature "live"): --live-listen <ADDR>, --live-pd <ADDRS>, --live-keyspace <NAME> (loam_live_dev),
//                                                --live-app <NAME> (dev), --no-live
```

**Semantics:**
1. `Watch` creates a session (id = node id ‖ random), subscribes its query set, and sends the first Transition from `{0, 0, 0}` with every result; later Transitions carry changed queries only, `start` = the session's current version; chunks over 4 MiB split with `more = true`.
2. `ModifyQuerySet` applies adds and removes if `base_version` equals the session's query-set version (else `FailedPrecondition`), and the next Transition reflects it.
3. Backpressure: a full session queue merges queued Transitions into one; a session blocked longer than `blocked_limit` is closed with `ResourceExhausted`.
4. Heartbeats: an empty Transition every `heartbeat`; a ts-only Transition at most once per second while a mutation from the session is pending (the session learns it from `Mutate` calls that carry its `session_id` header).
5. `Resume` reruns the set at the current tick (≥ `last_version.ts`) and sends full results.
6. `Mutate` and `Query` run through `Runner`; `Deploy` is Task 13's (it returns `Unimplemented` until then).
7. Startup prints `operon live listening on http://<addr>` before the existing `operon listening on …` line. A non-loopback `--live-listen` fails startup with `ServerError::LiveListenNotLoopback { addr }` (message as §20 §7.1).

**Tests:** `tests/session.rs`: `transition_applies_only_from_current_version`; `merged_transitions_stay_consistent`; `blocked_session_is_closed`; `resume_sends_full_results_at_or_after_last_ts`. `tests/service.rs` (through a connect-rust client over HTTP/1.1 and HTTP/2): `watch_receives_update_after_mutate`; `two_sessions_share_one_tick`; `modify_query_set_adds_and_removes`; `heartbeats_arrive`; `non_loopback_bind_is_refused` (`0.0.0.0:0` and a LAN address fail startup; `127.0.0.1:0` and `[::1]:0` start).

**Commit:** `live: add sessions, transitions and the connect-rust sync service`; `operon: serve Loam Live on 127.0.0.1:7710`.

### Task 13: QuickJS server functions and `Deploy`

**Files:** `crates/operon-live-js/{Cargo.toml,src/{lib.rs,runtime.rs,prelude.js,host.rs,limits.rs}}`, `crates/operon-live/src/deploy.rs`, `crates/operon-live-js/tests/{functions.rs,limits.rs,determinism.rs}`.

**Produces:**

```rust
pub struct JsConfig { pub memory_limit: usize /* 64 MiB */, pub cpu_limit: Duration /* 1 s */, pub contexts: usize /* 4 */ }
pub struct Bundle;   // one ES module; exports built with `query({ args?, handler })` and `mutation({ args?, handler })` from "loam:server"
impl Bundle { pub fn load(source: &str, config: JsConfig) -> Result<Self, LiveError>;          // validates exports
              pub fn functions(&self) -> Vec<(String /* "module:export" */, FnKind)>;
              pub fn function(&self, path: &str) -> Option<Arc<dyn Function>>; }
```

JavaScript surface (`loam:server`): `query`, `mutation`, and in handlers `ctx.db.get(id)`, `ctx.db.query(table).withIndex(name, q => q.eq(…).gt(…).lt(…)).order("asc" | "desc").take(n)`, `.collect()`, `.first()`, `ctx.db.insert(table, doc)`, `ctx.db.patch(id, fields)`, `ctx.db.replace(id, doc)`, `ctx.db.delete(id)`. Values map to `LiveValue` (`bigint` ↔ `I64`, number ↔ `F64`, `ArrayBuffer` ↔ `Bytes`).

**Semantics:**
1. Each host call is an async host function that calls `LiveTxn`, so reads land in the read set.
2. Determinism: `Date.now()` = start timestamp's ms; `Math.random` is a deterministic PRNG seeded from (start timestamp, request id); `crypto.getRandomValues` and `crypto.randomUUID` throw `DeterminismError` (never seeded; actions in R2 get the OS CSPRNG); `setTimeout`, `setInterval`, `fetch` and `WebAssembly` are absent in queries and mutations.
2a. **One context per invocation.** A context runs exactly one call and is then dropped, on success or failure; the pool keeps `contexts` fresh contexts with the bundle already evaluated and refills in the background; built-in globals are frozen before evaluation.
3. Limits: the interrupt handler stops a handler past `cpu_limit` (`LiveError::FunctionTimeout`); the runtime's memory limit gives `FunctionOutOfMemory`; a context is recreated after either.
4. `Deploy` validates the bundle, stores it with `operon-store` at `live/<app>/deployments/<id>.js`, applies the schema (tables, indexes; Ruling 5), then swaps the catalog pointer in one transaction; running sessions keep their deployment until their next rerun, then move to the new one.

**Tests:** `functions.rs`: `query_and_mutation_run_and_record_read_sets`; `mutation_rerun_on_conflict_is_invisible_to_the_caller`; `unknown_function_is_not_found`; `deploy_swaps_functions_for_new_calls`. `limits.rs`: `busy_loop_times_out`; `allocation_bomb_hits_memory_limit`; `context_recovers_after_timeout`. `determinism.rs`: `module_state_does_not_leak_between_calls` (a handler that increments a module-level counter returns 1 on every call); `crypto_random_throws_in_queries_and_mutations`; `date_now_is_start_ts`; `random_is_repeatable_for_same_ts_and_request`; `no_fetch_no_timers`.

**Commit:** `live: run query and mutation functions in QuickJS`; `live: deploy function bundles and schemas`.

### Task 14: The TypeScript reactive client

**Files:** `sdks/live-typescript/{package.json,pnpm-lock.yaml,tsconfig.json,tsconfig.build.json,tsconfig.test.json,biome.json,LICENSE}`, `src/{index.ts,client.ts,session.ts,optimistic.ts,values.ts}`, `test/{session.test.ts,values.test.ts,live.test.ts}`, `.github/workflows/ci.yml`.

**Produces (exported from `src/index.ts`):**

```ts
export class LiveClient {
  constructor(opts: { baseUrl: string; transport?: "connect" | "grpc-web"; fetch?: typeof fetch });
  watch<T = LiveValue>(fn: string, args?: LiveValue): Subscription<T>;          // adds to the session's query set
  query<T = LiveValue>(fn: string, args?: LiveValue): Promise<T>;
  mutate<T = LiveValue>(fn: string, args?: LiveValue, opts?: { idempotencyKey?: string; optimistic?: (local: LocalStore) => void }): Promise<{ result: T; commitTs: bigint }>;
  close(): void;
}
export interface Subscription<T> { readonly value: T | undefined; readonly error: LiveError | undefined; onUpdate(cb: (v: T) => void): () => void; unsubscribe(): void; }
export type LiveValue = null | bigint | number | boolean | string | Uint8Array | LiveValue[] | { [k: string]: LiveValue };
```

**Semantics:** one `Watch` stream per client; query-set changes through `ModifyQuerySet` with versions; a Transition whose `start` differs from the current version triggers a resume; reconnect with exponential backoff (250 ms to 10 s, jittered); optimistic updates are layered over server results and dropped once the session's `ts ≥ commitTs`; `int64` ↔ `bigint` losslessly. Runs in Node ≥ 22 and browsers (no Node built-ins under `src/`).

**Tests:** `session.test.ts` (unit, a fake transport): `applies_transitions_in_version_order`, `resumes_on_version_gap`, `drops_optimistic_update_at_commit_ts`. `values.test.ts`: `int64_roundtrip_is_lossless`. `live.test.ts` (against a spawned `operon dev` with `--live-*` flags and the playground): `watch_sees_insert_from_another_client`, `mutate_returns_commit_ts_and_watch_catches_up`, `reconnect_after_server_restart_converges`. CI job `sdk-live-typescript` (needs the `operon` binary artifact and the playground).

**Commit:** `sdk: add the Loam Live TypeScript client over generated Connect stubs`; `ci: run the Live TypeScript client against operon dev`.

### Task 15: TiDB SQL beside Live in the dev playground

**Files:** `deploy/tikv/tidb.toml`, `scripts/tikv/playground.sh`, `crates/operon-live/tests/sql_coexistence.rs`, `scripts/tikv/sql-smoke.sh`, `.github/workflows/ci.yml`.

**Semantics:**
1. `playground.sh start --with-tidb` starts one TiDB with `keyspace-name = "sql_dev"` (tests: `loam_test_sql`) on `127.0.0.1:4000`; `operon dev --live-pd …` prints `operon sql (TiDB) at mysql://root@127.0.0.1:4000` when it finds the playground's TiDB.
2. Keyspace mode is verified on v8.5.8 (Q33). If Task 0 found a regression on the pinned release, this task would instead document a separate API v1 TiDB playground and the D123 revisit.

**Tests:** `sql-smoke.sh`: create a table, insert and select through the `mysql` client. `sql_coexistence.rs`: `live_keyspace_does_not_see_sql_keys` (after the smoke script, a `Tikv` on `loam_test_live` scans its whole range and finds no TiDB `t`/`m` keys); `sql_keyspace_does_not_see_live_keys` (a `Tikv` on `loam_test_sql` finds no Live key layout under its range beyond TiDB's own prefixes); `both_commit_concurrently` (a Live mutation loop and a SQL insert loop run together; both finish).

**Commit:** `tikv: run a keyspace-mode TiDB beside Loam Live in the playground`.

### Task 16: The R1 gates and the exit report

**Files:** `crates/operon-live/tests/{reactive_checker.rs,txn_checker.rs}`, `crates/operon-sim/src/elle.rs` (or `operon-live/src/testing/`, per Task 0), `scripts/tikv/nemesis.sh`, `docs/plans/r1-exit-report.md`, `.github/workflows/ci.yml`.

**Semantics:**
1. **Reactive checker** (seeded; per PR 60 s, nightly 30 min): N sessions subscribe to random index ranges and point reads over a small keyspace of tables; M writers run random mutations. For every Transition: each updated query equals a fresh `Runner::query` at the Transition's timestamp; versions strictly increase per session; every committed mutation that touches a subscribed range is reflected by the first Transition at or after its commit timestamp; resumed sessions converge. Faults from Task 2's plan are injected at random.
2. **Transaction checker:** a list-append workload over Live documents; the history is checked for snapshot isolation with an Elle-style dependency-cycle search (G0, G1a–c, lost update, and G-single must not occur; G2 write skew is allowed only on range reads, per D118); a point-read write-skew workload must show none.
3. **Nemesis** (nightly, 3 TiKV stores): `nemesis.sh` kills and restarts a TiKV store and the PD leader, pauses the Live process (SIGSTOP/SIGCONT), and **stalls PD** (SIGSTOP on the playground's PD process, held past the client's request timeout, then SIGCONT; owner ruling T2-17), while checkers 1 and 2 run; both must pass. The PD stall exercises the real death of `tikv-client`'s TSO stream (`TimestampRequest channel is closed`), which Task 2's tests only simulate: the nemesis asserts that the TSO supervisor rebuilt the client (`TikvStats::client_rebuilds` > 0) and that the runs after the stall succeed.
4. **Exit report:** results of every gate (with `commit_mode` recorded per component), the metastore conformance and fault matrix on TiKV, commit latencies measured on a quiet machine with the spike's interleaved method (metastore `commit_wal`, Live mutation p50/p99, `async_1pc` against `two_pc`), journal shard conflict rate (feeds Q31 and Ruling 4), playground RAM, the TSO rebuild count under the nemesis, the answer to Q32, and the status of the upstream `tikv-client` PRs (TSO stream reconnect, public proto modules, and resolving async-commit and 1PC locks on the read path, owner ruling T7-1).

**Tests:** the checkers themselves, plus `checker_catches_injected_stale_result` and `checker_catches_injected_lost_update` (each checker must fail on a deliberately broken build hook).

**Commit:** `live: add the reactive and transaction checkers`; `ci: run the R1 nemesis nightly`; `docs: add the R1 exit report`.

### Task 17: Documentation

**Files:** `docs/design/20-reactive-database-on-tikv.md` (as-built notes: the rulings made during execution, the answers to Q32 and Q33), `docs/design/13-decision-log.md` (close Q32 and Q33; record any new decisions), `CHANGELOG.md`, `docs/plans/README.md` (R1 status), `crates/operon-live/README.md` (dev setup: playground, `operon dev --live-*`, a TypeScript example).

**Commit:** `docs: record R1 as built`.

## PR grouping

One PR per group, stacked in order; each PR builds and passes CI on its own. The metastore line (C–G) and the Live line (H–P) both start from C and may interleave.

| PR | Tasks | Title |
|---|---|---|
| A | 0 | R1 (1/17): dependency spike and cluster facts |
| B | 1 | R1 (2/17): dev playground and the TiKV client skeleton |
| C | 2 | R1 (3/17): transaction runner, commit tokens, faults, tuple codec |
| D | 3 | R1 (4/17): cluster GC loop |
| E | 4 | R1 (5/17): TiKV metastore catalog, leases, pointers, clock |
| F | 5 | R1 (6/17): TiKV metastore log, GC and changes |
| G | 6 | R1 (7/17): TiKV metastore fault matrix and `--meta tikv://` |
| H | 7 | R1 (8/17): `loam.live.v1` protos and code generation |
| I | 8 | R1 (9/17): Live data model |
| J | 9 | R1 (10/17): commit journal |
| K | 10 | R1 (11/17): `LiveTxn`, mutation runner, system functions |
| L | 11 | R1 (12/17): subscription manager |
| M | 12 | R1 (13/17): sessions and the sync service |
| N | 13 | R1 (14/17): QuickJS functions and `Deploy` |
| O | 14 | R1 (15/17): TypeScript reactive client |
| P | 15 | R1 (16/17): TiDB SQL beside Live |
| Q | 16, 17 | R1 (17/17): gates, exit report, docs |

## Rulings made during execution

### Task 0: reconciliation with the as-built code and the cluster (2026-09-27)

Checked against `main` at `ca50a9f` (M1.2a merged, M1.3 merged, M1.4 through Task 3): `crates/operon-common/src/meta/`, `crates/operon-meta-conformance/`, `crates/operon-meta/tests/it/conformance*.rs` and `crates/operon/src/{server.rs,main.rs}`. Also checked: a throwaway workspace crate built with `tikv-client`, `connectrpc`, `buffa` and `rquickjs`, and `tiup playground v8.5.8` runs with `--tag loam-t0 --port-offset 17000`. Commands, versions and raw results are in [`r1-dependency-spike.md`](r1-dependency-spike.md). These rows amend the task text; where a row says "amended", the task text above already carries the change.

| # | Checked | As built / found | Ruling |
|---|---|---|---|
| R1 | Consumes: `MetaStore`, `Consistency`, `Tracked` | `pub trait MetaStore: Send + Sync + fmt::Debug + 'static` (`operon-common/src/meta/store.rs:150`). It has 3 synchronous methods (`now_ms`, `watch_changes -> MetaChanges`, `is_ready`) and 48 async ones. Only `commit_wal`, `swap_segment` and `cas_pointer` return `Tracked<T>` (`result`, `earlier_unknown`); the rest return `MetaResult<T>`. `Consistency { Linearizable, Local }` matches. The change feed is `MetaChanges` over a backend's `ChangeWait` (spurious wake-ups allowed; `MetaStopped` once stopped). Errors are `MetaError { Rejected(ApplyError), NotLeader, Timeout, Unavailable, ClockSkew, Storage, Config, UnexpectedReply }` with 20 `ApplyError` variants. Beyond the plan's list, the trait has `links_with_pointers`, `leases_with_prefix`, `collection_heads`, `set_collection_hot` and `collection_hot` (M1.3) | Tasks 4–5 implement the trait exactly as built. Task 4 takes the clock and readiness, the catalog, leases, pointers and collections (hot configuration included); Task 5 takes the rest (amended). An unknown outcome maps to `Tracked.earlier_unknown` on the three `Tracked` methods; on the others, the retry-safe rules of each method's docs apply (a create that finds its name record returns `NamespaceExists` and friends with the existing id) |
| R2 | Task 4 semantics 2 (`now_ms` = TSO physical) | `now_ms` is synchronous (`store.rs:155`), so it cannot fetch a TSO timestamp | `now_ms` = `max(previous, physical(latest TSO timestamp the handle obtained) + elapsed since)`, which is monotonic and never behind the TSO; in-transaction stamps use the start timestamp; `clock_ms` fetches a fresh timestamp (amended). `commit_wal`'s `ClockSkew` check compares `created_at_ms` against the start timestamp's physical time |
| R3 | Consumes: the conformance crate | `Instance { clients, faults, guard }` and `Backend::start` match. `Faults` has `lose_next_ack(client)`, `disturb(seed)` **and `heal()`**. `lose_next_ack`'s contract is "the next successful write through `clients[client]` reports an unknown outcome, and the implementation retries it". There are **53 cases**, not 49 (M1.3 added the hot cases and `leases_with_prefix_lists_only_that_prefix`). `metastore_conformance!($backend)` has no `only` form, and cases are generated from one list (`for_each_case!`) | Task 5 runs all 53 (amended). Task 4 adds a test-only macro arm `metastore_conformance!($backend; cases = [a, b, …])` expanding `__case_tests` over the given names (a wrong name fails to compile, because it calls `suite::$case`), and lists the cases whose methods are all in Task 4's set (amended). The TiKV `Faults` adapter turns `lose_next_ack(i)` into a one-shot `FaultPlan` `LoseAck` at `AfterCommit` for handle `i`, resolved through the commit token; `disturb` and `heal` are no-ops until Task 6's nemesis |
| R4 | Tech stack: `tikv-client` source (check 4) | crates.io 0.4.0 fails `cargo deny check` advisories: RUSTSEC-2026-0258 (`h2` 0.3.27) and RUSTSEC-2026-0098/0099/0104 (`rustls-webpki` 0.101.7), all through `tonic` 0.10. It also returns `UndeterminedError` only for `Error::Grpc` on a 2PC primary commit, so under Ruling 3 a lost async-commit or 1PC prewrite acknowledgement looks like "not applied". Master `ab4be1c` passes `cargo deny` (`advisories ok, bans ok, licenses ok, sources ok`) and marks both cases undetermined | Pin `tikv-client = { git = "https://github.com/tikv/client-rust", rev = "ab4be1c2cdd58d4e593202991fb520221c83bdfd", default-features = false }` (defaults would add `openssl`, `procfs` and `protobuf` 2.28 through `prometheus/push`). Task 1 adds `allow-git = ["https://github.com/tikv/client-rust"]` under `[sources]` in `deny.toml` (amended). Cost: crates depending on it cannot be published to crates.io until a release carries these fixes; the pin moves only by a PR that reruns Tasks 2 and 5's suites |
| R5 | Task 3 semantics 1 (vendored GC protos) | PD v8.5.8 answers `Unimplemented` for `AdvanceTxnSafePoint` and `GetGCState`; its GC RPCs are `GetGCSafePoint`, `UpdateGCSafePoint`, `UpdateServiceGCSafePoint` and the keyspace `…V2` family. TiKV v8.5.8 builds with kvproto `release-8.5` at `07aa8c6` | Vendor the `pdpb.proto` subset and imports from kvproto `07aa8c6`; call only `GetMembers`, `GetGCSafePoint`, `UpdateGCSafePoint` and `UpdateServiceGCSafePoint` (amended) |
| R6 | **Q32** (check 3) | Keyspace-level safe points are ignored: `UpdateGCSafePointV2(keyspace 4, ts)` was accepted and listed by `GetAllGCSafePointV2`, yet reads at an older timestamp kept their versions for 90 s with the compaction filter off. The cluster path (`UpdateServiceGCSafePoint("gc_worker")`, then `TransactionClient::gc`) dropped them within 10 s. TiDB v8.5.8's GC worker states that the cluster has one global safe point and that a keyspace-mode TiDB neither computes it nor resolves locks (`gc_worker.go:386-389`) | **Task 3 is rewritten** (amended): one Loam GC loop per cluster, leased in `loam_meta`. It takes `min(now − life_time, UpdateServiceGCSafePoint("gc_worker", …))`, resolves locks in every keyspace and calls `UpdateGCSafePoint`. GC barriers are PD service safe points. No unified-GC TiDB runs, and none without `keyspace-name` may run beside Loam. D122's "keyspace GC state" clause and design §9.3 are superseded for the pinned release; Q32 is closed in the decision log. Per-keyspace GC returns when PD's GC-state API ships in a release Loam pins |
| R7 | Task 3 tests (a read below the safe point "fails with the GC error") | TiKV served a read below the safe point without an error: `None` after GC with the compaction filter off, and the old value with it on (the default) until RocksDB compacts. `tikv-client` does not check either | Task 2 gains semantics 5b: reads older than `now − (life_time − 1 min)` without a covering barrier are refused by `operon-tikv`. Task 3's tests assert the PD and TiKV safe points and lock resolution; physical removal is a nightly test that turns the compaction filter off through TiKV's status API (amended) |
| R8 | **Q33** re-confirm (check 2) | TiDB v8.5.8 with `keyspace-name = "sql_dev"` served DDL, DML and a select through `mysql`; its 1 707 keys (`m`, `t`) are all in `sql_dev`, and `loam_test_sql`, `loam_meta` and `DEFAULT` hold none | No change; Task 15 as planned |
| R9 | Task 2 classification (check 6) | In the pinned source, `Error::UndeterminedError` covers a failed or unanswered async-commit or 1PC prewrite and a failed primary commit, and over-reports rather than under-reports. Every other `commit()` error means the transaction did not commit | Mapping (amended): `UndeterminedError` → `Undetermined`; `WriteConflict` (both reasons) and `KeyIsLocked` after backoff → `Conflict`; `already_exist` → the caller's typed error; `TimestampRequest channel is closed` → `NotApplied` plus a rebuild; region, TSO, `ServerIsBusy` and other pre-commit errors → `NotApplied`; invalid arguments and unknown kinds → `Fatal`. A commit future dropped at the runner's deadline → `Undetermined` |
| R10 | Tasks 2, 8, 10 (scans) | A scan page over 4 MiB fails with `OutOfRange: decoded message length too large` (gRPC's default limit); `tikv-client` neither raises it nor pages by bytes | Task 2 semantics 5a: reads page internally from 256 keys, halving on `OutOfRange`, and the test `scan_pages_stay_under_the_grpc_limit` (amended). Live documents are at most 1 MiB, so one key always fits. **Amended (owner, Task 2, T2-12):** do both: the client's decoding limit is raised to 16 MiB with `Config::with_grpc_max_decoding_message_size` (the pinned revision has it, T1-8), and paging stays, so paging is the guarantee and the raised limit is headroom. No upstream PR is needed for the limit |
| R11 | Check 1 and Task 1 `ensure_keyspace` | `POST /pd/api/v2/keyspaces` created a keyspace at runtime in 0.24 s and the client used it. A duplicate `POST` answers **500** with body `"keyspace already exists"`; `GET` of a missing keyspace answers **500** with `"keyspace does not exist"`. Pre-allocated keyspaces have an empty `config`. A client for a missing keyspace fails at connect with `InternalError{"… keyspace does not exist"}` | `ensure_keyspace` does a `GET`; on 500 with "does not exist" it `POST`s; on 500 with "already exists" it `GET`s again; any other status is an error. `Tikv::connect` maps the connect error to `KeyspaceMissing { name }` |
| R12 | Tech stack: connect-rust and buffa (check 4) | `connectrpc-build` 0.9 with the system `protoc` generated a server-streaming and a unary RPC. The service served through `into_axum_router()` on the workspace's `axum` 0.8 and answered a Connect JSON call; `uint64` goes as a JSON string | As planned. `connectrpc` features `axum` (runtime) and `client` (dev, Task 12's tests over HTTP/1.1 and HTTP/2) (amended). The TypeScript client reads 64-bit fields as `bigint` (Task 14 as planned) |
| R13 | Tech stack: `rquickjs` features (check 4) | 0.14.0 ships prebuilt `x86_64-unknown-linux-gnu` bindings (no bindgen). The memory limit and interrupt handler work: a busy loop stopped at the 200 ms deadline, and an allocation bomb stopped at the limit | Features `futures`, `loader`, `macro`, `array-buffer` (amended). `unsafe_code = "forbid"` is compatible |
| R14 | Check 7 (the interval index) | `rust-lapper` 1.3 needs unsigned integer coordinates, inserts in `O(n)` and cannot remove | Task 11 hand-writes an augmented interval tree over byte-string ranges with incremental insert and remove (amended) |
| R15 | Check 5 (`memory-usage-limit` and RAM) | TiKV's peak RSS reached 2.56–2.74 GB **during startup** under every setting (none, 3 GB/1 GB, 1.5 GB/256 MB). Steady-state RSS after a 200 000-key load was 0.5–2.2 GB, lower with limits but dominated by host memory pressure | Task 1 keeps `memory-usage-limit = "3GB"` and the 1 GB block cache (they stop TiKV from sizing itself to 12 GB). The 3.2 GB playground peak in the Global Constraints stands |
| R16 | Task 6 (`MetaBackend`, `Server`) | `ServerConfig` has no metastore selector. `Server` holds a concrete `node: MetaNode` and `meta: MetaClient` (`server.rs:374-377`). `Server::meta() -> &MetaClient` has 5 callers in `crates/operon/tests/it/http.rs`. `operon cluster` builds its metastore from `--peers` (`cluster.rs:39`) | `ServerConfig.meta: MetaBackend` (default `Raft`); `Server` keeps `node` and `meta` as `Option`, and `Server::meta()` returns `Option<&MetaClient>`, updating the 5 test calls. `--meta` exists on `dev` and `standalone` only (amended) |
| R17 | Design §11.2 keys against the trait | `create_namespace(name)` takes no org, so `n/<org_id>/<name>` has no org to use. The trait's per-collection hot configuration has no key in §11.2 | `n/<name>` in R1 (the org segment arrives with the router in R2); the hot configuration is `H/<collection id>`, deleted by `drop_collection`, so hot updates never conflict with schema writes to `K/` |
| R18 | Crate names and the TypeScript toolchain | `operon-tikv`, `operon-meta-tikv`, `operon-live`, `operon-live-proto` and `operon-live-js` are free; `operon-sim` and `operon-store` exist as Tasks 10, 13 and 16 assume. npm latest: `@bufbuild/protobuf` 2.15.0, `@connectrpc/connect(-web,-node)` 2.2.0, `@bufbuild/protoc-gen-es` 2.15.0, `@bufbuild/buf` 1.73.0, all Apache-2.0 (protobuf-es also BSD-3-Clause) | No change |
| R19 | Check 4 (build cost) | With the pin, the lockfile gains 32 packages. The duplicates are `tonic` 0.12, `prost` 0.13, `axum` 0.7, `tower` 0.4, `fail` 0.4, `itertools` 0.12 and `syn` 1. The throwaway crate built 86 units in 32 s wall at 4 jobs; its debug binary is 50 MB | No change. `multiple-versions = "allow"` |

| # | Ruling | Why | Tasks |
|---|---|---|---|
| X1 | `tikv-client` is the git pin `ab4be1c`, `default-features = false`, with `allow-git` for `tikv/client-rust` in `deny.toml` | crates.io 0.4.0 fails cargo-deny advisories and under-reports unknown outcomes (R4) | 1, 2 |
| X2 | Loam runs cluster-wide MVCC GC (service safe point `gc_worker`, locks resolved in every keyspace, `UpdateGCSafePoint`); barriers are PD service safe points; no TiDB without `keyspace-name` runs beside Loam | Q32: v8.5.8 has no keyspace-level GC (R5, R6) | 3, 6, 15 |
| X3 | `operon-tikv` refuses reads older than `now − (life_time − 1 min)` without a covering barrier | TiKV serves reads below the safe point without an error (R7) | 2, 3 |
| X4 | Reads page internally from 256 keys, halving on gRPC `OutOfRange` | 4 MiB gRPC response limit (R10) | 2, 8, 10 |
| X5 | `now_ms` is a TSO-anchored monotonic estimate; stamps use the transaction's start timestamp | `now_ms` is synchronous in the trait (R2) | 4 |
| X6 | The suite has 53 cases; Task 4 adds a `cases = [..]` arm to `metastore_conformance!` | As built (R3) | 4, 5 |
| X7 | `ServerConfig.meta: MetaBackend`, `Server::meta() -> Option<&MetaClient>`, `--meta` on `dev` and `standalone` only | As built (R16) | 6 |
| X8 | Task 11's interval index is hand-written | `rust-lapper` cannot index byte ranges or remove (R14) | 11 |
| X9 | `rquickjs` features `futures`, `loader`, `macro`, `array-buffer`; `connectrpc` `axum` plus dev `client` | R12, R13 | 7, 12, 13 |
| X10 | `ensure_keyspace` treats PD's HTTP 500 bodies "already exists" and "does not exist" as the idempotency signals | PD v8.5.8's keyspace API (R11) | 1 |

### Task 1: dev cluster and the `operon-tikv` skeleton (2026-09-27)

| # | Plan said | As built | Why |
|---|---|---|---|
| T1-1 | The Produces block | Also public: `TikvConfig::new`; `Tikv::{keyspace, pd_http, keyspace_meta, latest_timestamp}`; `Tikv::raw_client` (`#[doc(hidden)]`, for tests until Task 2's `Txn` exists); `testing::{PD_ENV, PD_HTTP_ENV, TEST_META, TEST_LIVE, TEST_SQL, TEST_KEYSPACES, ROOT_LEN, random_root}` and `TestCluster::connect`. `TikvError` is in `lib.rs` with the variants `KeyspaceMissing`, `ApiVersion`, `Config`, `Pd`, `Http`, `Timeout`, `Client` | `latest_timestamp` is row R2's anchor for Task 4's `now_ms`; the rest keeps tests short. Task 2 should hide `raw_client` behind its runner |
| T1-2 | `cluster()` returns `None` when `OPERON_TEST_PD` is unset | Also: when it is set, `cluster()` checks `GET /pd/api/v1/version` and panics if PD does not answer. `OPERON_TEST_PD_HTTP` overrides the HTTP base URL | A configured cluster that is down must fail CI, not skip every test |
| T1-3 | `client_without_keyspace_is_refused_with_a_hint` | An empty `TikvConfig.keyspace` connects without a keyspace and probes one read. TiKV answers `InvalidKeyMode { storage_api_version: V2 }` (checked on the playground), which becomes `ApiVersion`. If the read succeeds, the cluster is not on API v2, which is also `ApiVersion`. Every `connect` runs the probe, and `ApiVersionNotMatched` maps the same way | A handle *with* a keyspace on an API v1 cluster is not detected: keyspace-prefixed keys are valid v1 keys. Carry: check a store's `storage.api-version` through its status port if that matters before R2 |
| T1-4 | `ensure_keyspace_is_idempotent` | The live test covers the lookup path only (tests never create keyspaces). The create and race paths run against a fake PD HTTP API in the same file (`ensure_keyspace_creates_once_…`, `…_survives_a_concurrent_create_…`, `…_surfaces_other_errors_…`; `axum` dev-dependency), and they need no cluster | The Global Constraints forbid tests from creating keyspaces |
| T1-5 | `playground.sh start [--with-tidb] [--stores N] [--tag T]`, `stop [--tag T]` | Also `status`, `--tidb-config FILE` (CI's `deploy/tikv/tidb-test.toml`), `--timeout S` and `--force`. `start` refuses a tag that does not start with `loam-`, a running playground of the same tag, anything already answering on PD `127.0.0.1:19379`, and a leftover `~/.tiup/data/<tag>`. `stop` sends SIGINT to `tiup` and `tiup-playground`, waits up to 60 s, then SIGKILLs what is left and deletes the data. The log goes to `target/tikv-playground/<tag>.log`. `wait-ready.sh` reads the keyspace list from `deploy/tikv/pd.toml` | One Loam playground per host (the fixed port offset). The cargo/rustc refusal also triggers on other projects' builds, so `--force` is sometimes needed locally |
| T1-6 | CI job `tikv`, path-filtered on PRs; `tikv-nightly` on the schedule | The path filter is a step inside the job (`git diff HEAD^1 HEAD` on the merge commit), so the job always reports and a PR without TiKV changes passes in seconds. The list also includes `.github/workflows/ci.yml`. Pushes to `main` always run it. The `-p` list is built from the crates that exist (`operon-tikv` now; `operon-meta-tikv`, `operon-live` and `operon-live-js` join when their tasks add them). The test binaries are built before the playground starts, and the playground runs without TiDB (Task 15 adds `--with-tidb --tidb-config deploy/tikv/tidb-test.toml`). Its logs are uploaded on failure. tiup comes from the installer script, which installs the latest tiup rather than 1.17.1. `~/.tiup/components` is cached under the key `tiup-components-v8.5.8-<os>` | GitHub filters paths per workflow, not per job |
| T1-7 | — | `operon-tikv` has `publish = false` | crates.io refuses git dependencies (row R4) |
| T1-8 | Row R10: "`tikv-client` sets no larger decoding limit" | The pinned revision has `Config::with_grpc_max_decoding_message_size` (default 4 MiB). `operon-tikv` keeps the default in Task 1 | Owner question for Task 2: keep paging only (as planned), or also raise the limit. **Answered in T2-12: both** |

### Task 2: the transaction runner, commit tokens, faults and the tuple codec (2026-09-27)

| # | Plan said | As built | Why |
|---|---|---|---|
| T2-1 | `Elem<'a> { Str(&'a str), Bytes(&'a [u8]) }`, `decode -> Elem<'static>` | `Str(Cow<'a, str>)`, `Bytes(Cow<'a, [u8]>)`, plus `Elem::into_owned`. `PartialEq` is the encoding's equality (`-0.0 == 0.0`, `NaN == NaN`). `tuple::successor` returns an empty vector when no bound exists (an empty or all-`0xFF` prefix), read as "unbounded". The tag constants are public | `decode` unescapes, so it has to own its bytes; a `&'static str` cannot hold them |
| T2-2 | `Tikv::snapshot(at) -> Snap` | `-> Result<Snap, TikvError>`. The window check (`TikvError::GcSafePoint { at, safe_point }`, TSO versions) runs at creation against the TSO estimate (the latest timestamp plus the time since it arrived, or a fresh one before the first). A held `Snap` refuses its reads (`TxnError::Fatal("read below the GC safe point")`) once its timestamp leaves the window, and a `Txn` refuses reads once it is older than the window. `TikvConfig.gc_life_time` (default 10 min, must exceed 1 min) sets the window. **GC barriers do not exist yet**: Task 3 must let a snapshot that a barrier covers through | The refusal has to be reported somewhere, and creation is the first point that knows `at` |
| T2-3 | The Produces block | Also public: `TxnOptions::{new, pessimistic, with_token}` and `commit_mode: Option<CommitMode>` (`None` uses `TikvConfig.commit_mode`, default `Async1pc`); `TxnError::AlreadyExists(String)` and `Txn::insert`; `Txn::attempt()`; `Snap::ts()`; `Tikv::{stats, commit_mode}`; `TikvStats { client_rebuilds, page_halvings, restarts, unknown_outcomes }`; `MAX_VALUE_BYTES`, `PAGE_KEYS`, `Pair`; the `token` module (`TOKEN_PREFIX`, `TOKEN_TTL`, `token_key`, `token_value`, `fence_value`, `is_fence`, `token_expiry`, for Task 3's sweep); `TikvConfig.{commit_mode, gc_life_time, grpc_max_decoding_bytes}`. Reads and writes take root-relative keys: `scan(start, end: Option<&[u8]>, limit: usize) -> Vec<Pair>` (`None`: to the end of the root), and `batch_get` returns pairs sorted by key. `TikvError::Client` now holds scrubbed text instead of the `tikv-client` error. `raw_client` is gone (row T1-1's carry): `Tikv::client()` is `pub(crate)` for Task 3, and `TransactionClient` is no longer re-exported. The metrics are `Tikv::stats()` counters | "The caller's typed exists error" needs a carrier; the workspace has no metrics crate, and counters keep the tests exact |
| T2-4 | "reruns `body` on Conflict" | The runner reruns on `Conflict` **and** `NotApplied`, whether the body or the commit returned them, until `max_attempts` or the deadline, with a jittered backoff (10 ms doubling to 1 s, drawn from the upper half). A body that wants to reject without a retry returns `Ok` with its own error inside `T`, or `Fatal`. After exhausting the attempts the last error is returned; `Deadline` when the deadline passes before a commit starts | Both mean the transaction did not commit, so a rerun is safe; the TSO supervisor's "retries the operation as `NotApplied`" needs it |
| T2-5 | Semantics 2: read `t/<token>` at a fresh timestamp; absent → `NotApplied` | A resolving transaction reads the token at a fresh start timestamp. Present → success, `earlier_unknown = true`, and `commit_ts` is the resolver's start timestamp (an upper bound of the real one). Absent → the resolver writes a **fence** (`expires_ms ‖ "F"`) at the token and commits it; then the outcome is `NotApplied` and the runner retries (`earlier_unknown` stays false: the outcome is known). Unresolved within two request timeouts → `Undetermined { token: Some(token) }`. Task 3's sweep deletes fences with the tokens | A plain read cannot stop a request of the lost commit that is still in flight: an async-commit or 1PC prewrite that lands after the read commits above it, and the retry would then apply the body twice. The fence makes such a prewrite meet a newer write and conflict |
| T2-6 | Fault points `BeforeBegin`, `BeforePrewrite`, `BeforeCommit`, `AfterCommit` | `tikv-client` prewrites and commits in one call, so both middle points fire before any lock is written: `BeforePrewrite` after the body and before the token write, `BeforeCommit` right before `commit()`. `Refuse` → not applied (retried), `Conflict` → conflict (retried), `LoseAck` → rolled back and resolved as an unknown outcome (the fence makes it not applied), `Delay` sleeps. At `AfterCommit`, every fault but `Delay` is a lost acknowledgement; at `BeforeBegin`, `LoseAck` is a refusal. The table is in `faults.rs` | Reporting a conflict after a commit would make the runner replay a committed transaction |
| T2-7 | `tso_stream_loss_rebuilds_the_client`: "a test hook closes the TSO stream; the next `run` succeeds after a rebuild" | `Tikv::inject_tso_stream_loss()` (feature `faults`) makes the next TSO-dependent call (a begin or `Tikv::now`) fail with the client's own `TimestampRequest channel is closed` error; the supervisor path after it is the real one. The run that meets it succeeds on its second attempt, after the rebuild. Begin failures and `Tikv::now` failures count toward the three-failure rebuild; a failed rebuild backs off from 100 ms doubling to 5 s | `tikv-client` exposes no handle on its TSO stream |
| T2-8 | Semantics 5a | As planned, plus `scan_reverse` on both `Txn` and `Snap`. Every call starts at 256 keys. A single key whose response still fails with `OutOfRange` is `Fatal`. With the 16 MiB limit (T2-12), the test's first page of 256 × 64 KiB fails and is halved, which `TikvStats::page_halvings` shows | — |
| T2-9 | Semantics 1 (row R9) | The mapping of the kinds R9 leaves open: `KeyError` `locked`, `deadlock` or legacy `retryable`, and `ResolveLockError` → `Conflict`; `abort`, `commit_ts_expired`, `txn_not_found`, `txn_lock_not_found`, `commit_ts_too_large` and `primary_mismatch` → `NotApplied`; `assertion_failed` → `Fatal`; `InternalError` and `StringError` → `NotApplied` (with the TSO text: a rebuild); gRPC `OutOfRange` → the pager's signal (`Fatal` elsewhere). A compound error takes its strongest part. After a commit error that is not undetermined the runner rolls back (best effort) to clear prewrite locks; it never rolls back an undetermined commit | R9 lists the classes, not every kind |
| T2-10 | Semantics 6 | Errors built from structured fields (key errors, region errors) name the key without the `x` + keyspace-id prefix and the root, escaped and cut to 64 bytes; region errors name their kind only. Other `tikv-client` text goes through a scrubber that replaces every `Debug`-printed byte list of 4 or more bytes the same way | `tikv-client` prints keys as byte lists in its `Debug` text |
| T2-11 | Files `runner.rs, faults.rs, token.rs, codec.rs` | Also `classify.rs` (classification and scrubbing) and `txn.rs` (`Txn`, `Snap`, paging). The integration tests get the `faults` feature through a self dev-dependency (`operon-tikv = { path = ".", features = ["faults"] }`). Extra tests: `insert_of_an_existing_key_is_already_exists`, `the_type_order_is_the_design_order`, unit tests of classification, scrubbing, tokens, backoff and the codec's malformed input | — |
| T2-12 | Owner question T1-8 (gRPC decoding limit) | **Owner ruling (2026-09-27): do both.** `Tikv::connect` sets `Config::with_grpc_max_decoding_message_size(16 MiB)` (`TikvConfig.grpc_max_decoding_bytes`, at least 4 MiB), and scans and batch gets still page. Row R10 is amended | Paging is the guarantee; the raised limit is headroom |
| T2-13 | PR titles | **Owner ruling (2026-09-27):** PR titles follow the "PR grouping" table. #54 is now titled "R1 (1/17)"; this task's PR is "R1 (3/17)" | — |
| T2-14 | The PR template's "Commits signed off (DCO)" item | **Owner ruling (2026-09-27):** no DCO sign-off; the repository's history does not use it. The item stays unchecked and the PR body says why | — |
| T2-15 | Row T2-5 (fence-based token resolution) | **Owner ruling (2026-09-27): accepted.** A resolver that finds the token absent writes a fence and commits it before the outcome is `NotApplied`; this is the correct way to stop an in-flight lost prewrite from applying twice | A plain read at a fresh timestamp cannot stop a late async-commit or 1PC prewrite of the lost commit |
| T2-16 | Row T2-4 (the runner retries `NotApplied`) | **Owner ruling (2026-09-27): accepted**, because `NotApplied` means the transaction certainly did not commit, so a rerun is safe | — |
| T2-17 | Row T2-7 (simulated TSO stream loss) | **Owner ruling (2026-09-27): accepted** for the unit and integration tests. Task 16's nightly nemesis also stalls the real PD (SIGSTOP/SIGCONT on the playground's PD process) to exercise the real stream death; Task 16's semantics 3 carries it | The injected error is the client's own text, but only a real stall shows the stream actually dying |

### Task 3: the cluster MVCC GC loop (2026-09-27)

| # | Plan said | As built | Why |
|---|---|---|---|
| T3-1 | The Produces block | `GcLoop::new(tikv, config) -> Result<GcLoop, TikvError>` (validates the config) beside `GcLoop::spawn(tikv, config, shutdown) -> Result<GcHandle, TikvError>`; `GcHandle::{gc_loop, last, stopped}` (`last` is the latest run's outcome). `GcConfig` also has `sweep: Vec<Tikv>` (T3-6) and is `Default`; `GcReport` also has `sweep_error`. `GcBarrier::new(&Tikv)`, `GcBarrier::service_id(purpose, id)` = `loam/<purpose>/<id>`, `set(&self, service_id, &Timestamp, ttl: Duration)` and `delete(&self, service_id)`. New `TikvError` variants `PdGrpc`, `GcLease`, `GcKeyspace`, `BarrierBelowSafePoint` and `Txn` (from `TxnError`, for the loop's own transactions). Constants `GC_LEASE_KEY`, `DEFAULT_GC_INTERVAL`. `testing::{nightly, tikv_status, NIGHTLY_ENV, TIKV_STATUS_ENV, DEFAULT_TIKV_STATUS}`; `Tikv::locks_below(&Timestamp)` (feature `faults`) | A barrier needs PD and the handle's registry (T3-5), so it is built from a handle; the loop's lease and sweep are transactions whose errors need a carrier |
| T3-2 | Semantics 1: "the `pdpb.proto` subset … and its imports … unchanged" | `pdpb.proto` whole and its 13 transitive imports, byte-for-byte from kvproto `release-8.5` `07aa8c6`, flattened into `crates/operon-tikv/proto/kvproto/` with kvproto's `LICENSE`; `google/protobuf/descriptor.proto` comes from the system `protoc`. `build.rs` generates only the PD client (`tonic-prost-build` 0.14, comments off); `src/pd.rs` calls `GetMembers` (leader and cluster id), `GetGCSafePoint`, `UpdateGCSafePoint` and `UpdateServiceGCSafePoint`, reconnecting to the leader after a failed call. `NOTICE` lists the files: Apache-2.0 from pingcap/kvproto, except `gogoproto/gogo.proto` (BSD-3-Clause, The GoGo Authors) and `rustproto.proto` (MIT, rust-protobuf), which kvproto ships in its `include/` | Cutting a subset would have modified the files; "unchanged" wins. The plan's "all Apache-2.0" was inaccurate for the two option files (both licenses are allowed) |
| T3-3 | Semantics 2: "takes the lease `e/cluster/gc` … (epoch-fenced)" | The lease lives in `operon-tikv` (the metastore's leases arrive in Task 4): `holder (16 random bytes per loop) ‖ epoch ‖ expires_ms` at `GcConfig.lease_key` under the handle's root, TTL `max(3 × interval, 30 s)`. A new holder bumps the epoch. Before `UpdateGCSafePoint` the run confirms it still holds the lease at its epoch (and renews it); a run that finds the lease held returns `TikvError::GcLease` and the spawned loop just waits for the next interval | The confirmation is not atomic with the PD call; a second worker slipping in between is harmless, because each worker only sets a safe point below which it resolved every lock itself |
| T3-4 | Semantics 2–3 | `GcReport.safe_point` is PD's answer to `UpdateGCSafePoint` (PD never moves it back, so it can exceed what the run asked for); `held_by` names the minimum service safe point when it is below the target. PD saves a service safe point only if it is at or above the current minimum, so `GcBarrier::set` below it fails with `BarrierBelowSafePoint` (PD saved nothing). The barrier's TTL is whole seconds, at least 1 | Verified on the playground: `barrier_holds_safe_point`, `barrier_below_the_safe_point_is_refused` |
| T3-5 | Carry T2-2: "let a snapshot that a barrier covers through" | Each handle (with its clones) keeps a registry of the barriers set through it (service id → ts, local expiry measured from before the PD request). `Tikv::snapshot(at)` below the safe window succeeds when a live barrier of the handle is at or below `at` **and** `Tikv::gc_safe_point()` (cached ≤ 10 s) is at or below `at`; a held `Snap` past the window keeps reading while such a barrier lives and refuses once it is deleted or expires. Barriers set through another handle or process do not count | A handle can vouch only for barriers it knows are live; PD has no call to list service safe points |
| T3-6 | Semantics 2: "sweeps expired commit tokens in the keyspaces Loam owns"; carry T2-5 (fences too, by `token::token_expiry`) | The sweep covers the loop's own handle's root and every handle in `GcConfig.sweep` (Task 6 and Task 12 pass the metastore's and Live's handles). It scans `t/` in pages of 256 at a fresh snapshot, then deletes the expired tokens and fences of a page in one transaction that re-reads them; a value that is neither is kept. It runs after `UpdateGCSafePoint`, and a sweep failure does not fail the run: it is logged and reported in `GcReport.sweep_error` | The loop cannot discover roots; the safe point has already moved when the sweep runs |
| T3-7 | `GcConfig.life_time` (default 10 min) | `GcLoop::new` refuses a life time shorter than the handle's `TikvConfig::gc_life_time` (the read window would then trust versions GC drops), except with the feature `faults`, which the tests build with, so a test can use 2 s. It also refuses a zero interval and an empty lease key | Tests must see locks and versions fall below the safe point in seconds; production never enables `faults` |
| T3-8 | The tests of `tests/gc.rs` | All planned tests, plus `barrier_below_the_safe_point_is_refused` and `barrier_lets_old_snapshots_through` (T3-5). The tests share the cluster's one safe point, so they run one at a time under a static lock. `locks_below_the_safe_point_are_resolved_in_every_keyspace` leaves two prewrite locks in each test keyspace with `tikv-client`'s own failpoints (`after-prewrite` and `before-rollback`, `fail` 0.4 with `failpoints` as a dev-dependency, so only test builds have them) on a `TwoPc` run with one attempt, then waits 4 s (the locks' 3 s TTL), runs GC with a 2 s life time and checks `Tikv::locks_below` is 0 in every keyspace. `safe_point_advances_cluster_wide` reads TiKV's `tikv_gcworker_autogc_safe_point` from the status port (`OPERON_TEST_TIKV_STATUS`, default `127.0.0.1:37180`; the gauge is a float, so the comparison allows its rounding). `old_versions_dropped_after_gc` runs only when `OPERON_TEST_NIGHTLY` is set (the `tikv-nightly` job sets it): it turns `gc.enable-compaction-filter` off through `POST /config`, asserts through an MVCC read at the old timestamp (inside the handle's read window) that the overwritten value disappears, and turns the filter back on even on failure. Unit tests drive the round with a fake cluster: every keyspace state but `TOMBSTONE` is resolved, a keyspace without a client or a lost lease stops the round before `UpdateGCSafePoint`, and a service safe point holds it back | The spike found versions survive until compaction with the default filter (row R7), so the PR suite asserts safe points and locks, and physical removal is nightly |
| T3-9 | Semantics 2: "lists the keyspaces through `GET /pd/api/v2/keyspaces`" | Paged: `?limit=100&page_token=<id>`, following `next_page_token` (the next keyspace id; `""` on the last page, checked on v8.5.8), deduplicated by id. The list includes `DEFAULT` (id 0), whose locks are resolved too. `tikv-client`'s keyspace client loads a keyspace by name without checking its state, so `DISABLED` and `ARCHIVED` keyspaces get clients; clients are built once per keyspace and kept | PD answers at most one page per call |
| T3-10 | — | Review of #56 (CodeRabbit): `tuple::decode` refuses arrays nested deeper than 64 (`CodecError::TooDeep`); the `ApiVersion` hint is scrubbed before it is cut; the runner's TSO-failure bookkeeping and client rebuilds run in their own task and the run waits for them only until its deadline. Also `Supervisor::inject_tso_loss` is compiled only with `faults` (it failed `clippy -D warnings` on the library alone) | All three findings were valid |
| T3-11 | Semantics 2: "if a client cannot be built for one, the run fails without advancing the safe point" | As planned. Consequence: one keyspace whose client cannot be built (or whose locks cannot be resolved) stops cluster GC for every keyspace until it is fixed or tombstoned; each failed run logs the keyspace (`TikvError::GcKeyspace`) | Advancing past an unresolved lock would break snapshot reads in that keyspace. Owner question: accept, or alert on repeated failures in R2 |
| T3-12 | Row T3-11 (one keyspace blocks cluster GC) | **Owner ruling (2026-09-27): accepted.** R2 adds a `gc_blocked_seconds` gauge per keyspace (how long that keyspace has held the cluster safe point back) and an alert on repeated `GcKeyspace` failures; recorded in the R2 row of `docs/plans/README.md` and design §20 §18, since R2 has no plan yet | Advancing past an unresolved lock would break that keyspace's snapshot reads, so blocking is correct; the alert makes the block visible |
| T3-13 | Row T3-7 (the short-life-time exception) | **Owner ruling (2026-09-27): accepted.** `GcLoop::new` accepts a life time below the handle's `gc_life_time` only with the feature `faults`, which only test builds enable | — |

### Task 4: `operon-meta-tikv` (1/2): catalog, leases, pointers, clock (2026-09-27)

| # | Plan said | As built | Why |
|---|---|---|---|
| T4-1 | The Produces block | Also public: `TikvMeta::open_on(tikv, id_block, poll)` (a metastore on an existing handle; tests pass one with a fault plan), `TikvMeta::tikv()`, `TikvMetaConfig::new`, `DEFAULT_ID_BLOCK` (1 000), `DEFAULT_POLL` (100 ms). The crate has `publish = false` (row T1-7) | The conformance backend needs handles built with `Tikv::with_faults` |
| T4-2 | Row R3: "Task 4 adds `metastore_conformance!(backend; cases = [..])`, a test-only change" | As planned, plus a provided `Backend::unavailable() -> Option<String>` (default `None`) that every generated case checks first: an unavailable backend's case prints `skipped: <case> needs <reason>` and passes without `start`. The TiKV backend answers `Some("OPERON_TEST_PD")` when the variable is unset. The openraft single-node cases still pass (53) | The Global Constraints make cluster tests skip without a cluster, and `Backend::start` cannot skip |
| T4-3 | Semantics 3; row R3 ("resolved through the commit token") | Every write carries a commit token. When an attempt's outcome was unknown, the call is **rerun** (at most 3 times), both when the runner could not resolve it (`Undetermined`) and when it resolved it through the token as committed (`Committed.earlier_unknown`), and `Tracked.earlier_unknown` is set. The result is then the trait's retry result: `NamespaceExists` with the id, a `VersionMismatch` holding the caller's value, `create_collection`'s `CollectionExists` turned into the first attempt's ids (as the openraft client does) | The suite's `a_lost_ack_on_cas_reports_a_mismatch_with_the_callers_value` asserts the retry's `VersionMismatch`; returning the resolved `Ok(2)` would be more informative but fails it. Owner question: keep the rerun, or return the resolved value and relax that case |
| T4-4 | Semantics 1: "Keys exactly as §20 §11.2" | As §11.2 with row R17 (`n/<name>`, `H/<collection>`). Ids are 8-byte big-endian (id order is key order), partitions 4-byte big-endian, names and paths last. Leases are `e/m/<key>` (scope `m`; the GC loop's `e/cluster/gc` stays apart). Records are a format byte (1) and postcard; counters and stamps are u64 big-endian; `W/` is a u32. `a/<ns>` holds `{version, aliases}`, `h/` holds `{next, log_start, bytes}`, `o/` holds `{refs, gc_claim}`. `K/` has no state field: the record exists while the collection is live, and `drop_collection` deletes it | The design names the records, not their bytes |
| T4-5 | — | Partition heads are written lazily: `create_stream` and `create_collection` write none, and an absent head of an existing partition is an empty one. Task 5's `stream_state` must report such a partition as `Some` with zero bounds, as the openraft state machine does | A 10 000-partition stream would otherwise be a 10 000-key transaction, past async commit's key limits (the Task 2 carry on Async1pc and large mutations) |
| T4-6 | Semantics 4; §20 §11.3 ("checked in the same transaction") | A read that decides a write but is not overwritten by it is locked with `lock_keys`: the fence's lease record, the collection record of a `collection/<id>` pointer and of `set_collection_hot`, the CAS value's `o/` row, the alias map in `create_collection`, and the `k/` names `update_aliases` checked | Snapshot isolation lets two transactions that read each other's keys both commit (write skew); a lock makes them conflict |
| T4-7 | Semantics 4 (the new manifest path's `gc_claim`) | `cas_pointer` reads the value's `o/` row: a `gc_claim` refuses it with `StaleObject` (the request's freshness, or zeros without one), and the row is locked. Task 4 writes no `o/` rows and a pointer does not count as a reference; Task 5 defines when `forget_objects` claims and how `refs` count | The claim and the reference must conflict, and the rest of the `o/` design belongs to Task 5's GC methods |
| T4-8 | — | `drop_collection` removes the implicit stream's heads and index entries in the same transaction and releases each entry as the openraft state machine does (a WAL object whose `W/` count reaches zero, and every segment, is retired at `r/`), so Task 5 needs no change to it. Carry: a collection with a large index is dropped in one large transaction; Task 5 or 6 may batch it | The layout is fixed here, and a partial drop would leave dangling entries |
| T4-9 | "`Linearizable` reads use a fresh TSO timestamp" (§11.3) | Every read, `Local` included, is one snapshot at a fresh TSO timestamp, retried up to 4 times on `Conflict` (a lock it met) or `NotApplied` | As §11.3 says for R1 |
| T4-10 | — | Error mapping: `Deadline` → `MetaError::Timeout`; `Conflict`, `NotApplied` and an outcome still unknown after the reruns → `Unavailable`; `Fatal` (a corrupt record, a read below the safe window) → `Storage`; `AlreadyExists` → `UnexpectedReply` (writes use `put`, so it is a bug); `KeyspaceMissing`, `ApiVersion` and `Config` at open → `Config` | The trait's retryable errors say the outcome is unknown, which covers "certainly not applied" |
| T4-11 | Semantics 2; the Tests paragraph on `watch_changes` | `now_ms` is `max(previous, physical(latest TSO timestamp) + elapsed)`; `clock_ms` fetches a fresh timestamp and moves `now_ms`'s floor up to it; `open` fetches one timestamp, so `now_ms` has an anchor from the start. A change watch wakes on the handle's own writes at once (a `tokio::sync::watch` bumped after every write) and every `poll`. `is_ready` is true once opened. `stream_state` stays with Task 5 (its list) | As planned (row R2) |
| T4-12 | Semantics 3: "ids come from per-node blocks of `id_block`" | Per handle and per kind (`c/namespace`, `c/stream`, `c/link`, `c/collection`), each block taken in its own transaction. Arguments are validated before an id is taken, so an `InvalidArgument` costs none; a create rejected inside its transaction (exists, not found) leaves a gap. `create_collection` takes three ids | Gaps are allowed (D18) |
| T4-13 | Tests | 31 conformance cases: the catalog (6), leases (6), pointers (5), collections (5), hot (3), changes (3), the lost-ack cases on `create_collection` and `cas_pointer`, and `concurrent_cas_on_two_keys_is_linearizable` (with the default `Async1pc`, Ruling 3). The other 22 need Task 5's methods; `a_stale_cas_is_refused` because its `stamp` calls `prune_wal_commits`, `drop_frees_the_name_and_retires_both_prefixes` because it reads `retired_expired`, `takeover_bumps_the_epoch_and_fences_the_old_holder` because it commits and trims. Plus `clock_is_tso_physical`, `id_blocks_leave_gaps_but_never_repeat` and 7 unit tests of the key layout and records | — |
| T4-14 | Semantics 2: "`clock_ms` returns the physical time of a fresh TSO timestamp" | As planned. Consequence for Task 5: `collection_head_reads_pointer_bounds_and_clock` asserts that `collection_head`'s `clock_ms` equals a `clock_ms` read just before it with no write between (the trait: "the latest time stamp of any applied write"), and a TSO clock advances with wall time, so that case fails as written | Owner question: relax the case to `>=` for backends whose clock is a TSO, or have Task 5 keep a stored clock of the latest stamped write (a hot key every write touches) |
| T4-15 | — | Review of #59 (CodeRabbit, Task 3's code): the GC round confirms and renews its lease after every keyspace, not only before `UpdateGCSafePoint`, so a run longer than the lease TTL keeps the lease and a lost lease stops the run after the keyspace it was lost in (`a_lost_lease_stops_the_round_before_the_safe_point_moves` now checks that); the `BarrierBelowSafePoint` message loses a run of spaces left by an unescaped line break (`the_barrier_refusal_reads_as_one_sentence`) | Both findings were valid: two loops whose runs outlast the TTL would otherwise take the lease from each other and GC would never advance |
| T4-16 | Row T4-3 (the rerun after a token-resolved commit) | **Owner ruling (2026-09-27): keep the rerun.** It matches the trait's retry contract and the conformance case `a_lost_ack_on_cas_reports_a_mismatch_with_the_callers_value`, and keeps every backend's answers alike | — |
| T4-17 | Row T4-14 (the clock case) | **Owner ruling (2026-09-27): relax `collection_head_reads_pointer_bounds_and_clock` to "not earlier"**: the clock is non-decreasing, so the head's clock is at least a clock read before it. No hot clock key. Applied in Task 5, which also relaxes `collection_roots_lists_prefixes_under_a_path`'s identical equality (T5-9); the openraft suites still pass (53 × single node, three nodes and both HTTP clients) | A TSO clock advances with time, not only with writes |

### Task 5: `operon-meta-tikv` (2/2): the log, GC reads and changes (2026-09-27)

| # | Plan said | As built | Why |
|---|---|---|---|
| T5-1 | The Produces block | As planned; also public `MAX_GROUP_CHUNKS` (1 024), `MAX_GROUP_BYTES` (4 MiB) and `MAX_CLOCK_SKEW_MS` (300 000, the openraft default). `operon-tikv` gains `Txn::batch_get_for_update` (all keys locked in one request per region; latest values in a pessimistic transaction), with the live test `batch_get_for_update_reads_latest_values_and_locks` | Design §20 §11.4: batching the locks matters |
| T5-2 | Ruling 2 (pessimistic `commit_wal`, `swap_segment`, `trim_partition`) | The write helper takes a mode (carry T4). A pessimistic write gets 64 attempts within the runner's 10 s deadline. `tikv-client` sends `wait_timeout = 0` (TiKV's default lock wait, 1 s), so writers queue, but a waiter woken by a commit gets `PessimisticRetry` and the runner restarts it; `hot_head_commits_queue_without_abort_loops` (16 writers × 6 commits on one head) passes with every commit succeeding | The client never retries a lock at a newer `for_update_ts`, so waits end in restarts; the larger budget is the Ruling 2 fallback's "larger retry budget" |
| T5-3 | Semantics 1: "`get_for_update` on every touched head in key order" | Each pessimistic write locks its heads first (`commit_wal`: heads, the group's `w/` record and the object's `W/`, `r/` and `o/` rows in one batch), reads the rows it rewrites (`W/` counts, the fence's lease) with `get_for_update`, and reads stream records and index entries from **a snapshot at a fresh TSO taken after the locks**. `check_fence` now uses `get_for_update` in every write. `drop_collection` deletes **every** partition's head, present or not | A pessimistic transaction's plain reads use its start timestamp, which can predate a commit (a drop, a swap) that finished before its locks; TiKV does not report that. Every writer of a partition's index entries writes its head, so after the head lock a fresh snapshot is exact. A drop that deleted only the heads it saw would not conflict with a first commit creating a head, which could then index entries under a dropped stream (`a_drop_racing_first_commits_leaves_no_live_chunk`). Cost: one TSO fetch per pessimistic write, and a drop writes one key per partition (T4-8's large transaction grows) |
| T5-4 | Semantics 3: "per-scope counters `v/…` bumped by catalog writes … plus … a 100 ms poll" | **No `v/` counters.** The watch keeps Task 4's behaviour: it wakes at once on the handle's own writes (`commit_wal` included, so its offset watchers see its commits directly) and on every poll | The trait has one unscoped watch that must wake on every change, and its consumers need other nodes' commits and pointer writes (the log reader's long poll, `scan_token`); counters those writes bumped would be a hot key on every commit, and counters they did not bump cannot wake the watch for them, so the poll must wake unconditionally anyway and the counters would be unread writes. Scoped counters belong to D63's scoped change feed (M2.x). Owner question |
| T5-5 | Semantics 1 and 4 (`o/` rows: "write … `o/` rows", "`forget_objects` checks and clears claims"); row T4-7 | `o/` rows are read and locked, not written: `commit_wal` refuses a WAL object whose row has a `gc_claim` (or which is already retired, e.g. every chunk of an earlier group released) with `StaleCommit`; `swap_segment` refuses a claimed segment with `StaleObject`, as `cas_pointer` does. `forget_objects` deletes each forgotten path's `o/` row with its `r/` row. Nothing sets a claim in R1 | No trait method claims an object; `W/` already counts a WAL object's live chunks and the index names segments, so `refs` would duplicate them. The lock is what makes R2's claim protocol conflict with a new reference. `refs` stays unused |
| T5-6 | Row R2: "`commit_wal`'s `ClockSkew` check compares `created_at_ms` against the start timestamp's physical time" | Checked before any transaction against the handle's `now_ms` (never behind the TSO), bound 300 s, as the openraft leader checks before proposing. `StaleCommit` compares against the group transaction's start timestamp | `ClockSkew` is a `MetaError`, not an `ApplyError`, so the transaction body cannot return it; `now_ms` is the more lenient clock, so no false refusals |
| T5-7 | — | `swap_segment`, `trim_partition`, `prune_wal_commits` and `drop_collection` (the writes the trait says are "stamped with `now_ms`") first wait until a fresh TSO's physical time reaches the handle's `now_ms`, for at most 1 s | PD advances the TSO's physical part in steps (every 50 ms), so the extrapolated `now_ms` can run ahead of it; without the wait the metastore clock after a stamped write could be behind the caller's clock, which `orphan_wal_objects_skips_live_retired_and_young` caught. One extra TSO fetch per stamped write |
| T5-8 | Semantics 4: "GC reads scan `r/` shards and read `o/` rows" | `retired_expired` and `collection_roots` scan all of `r/` (sorted by path afterwards); `orphan_wal_objects` point-reads `W/` and `r/`; `orphan_segments` scans the index entries of every stream of the namespace, `segment_referenced` those of the partition; `forget_objects` runs one transaction per 256 objects and `prune_wal_commits` one per 256 commit records, each checking the fence (an empty `forget_objects` still checks it) | The trait's namespace-scoped semantics need the index; carry: `orphan_segments` and `retired_expired` cost O(entries) and O(retired set) per call |
| T5-9 | The suite | All 53 cases pass on TiKV. `collection_roots_lists_prefixes_under_a_path` also asserted `clock_ms == clock` and is relaxed to "not earlier" with `collection_head_reads_pointer_bounds_and_clock` (T4-17); the `collection_heads` comparisons in the latter compare the heads with their own clocks | The same TSO-clock reason |
| T5-10 | Semantics 1: groups | Streams are taken in the order of their first chunk; a new group starts when the next stream does not fit (1 024 chunks, 4 MiB by `object.len() + 128` bytes per chunk, so the chunk limit binds first for paths under ~4 KiB). A group record `w/<hash8>/<object>/<group BE4>` holds `{groups, created_at_ms, offsets: [(call position, base)]}`, so a retry returns every offset in call order. A call over one group validates every chunk from one snapshot before committing any group (unless a retry finds records), then commits the missing groups in order; a retry that groups differently from the first attempt is `InvalidArgument`. A group finding its record returns it, before the stale check, as the openraft state machine does | D59's rule, and a multi-group call must not commit half its groups on invalid input |
| T5-11 | Tests | As planned, plus `a_drop_racing_first_commits_leaves_no_live_chunk` (T5-3) and `drop_deletes_the_implicit_links_pointer` (T5-12). `commit_wal_is_atomic_across_partitions` uses 12 chunks over 3 streams: refused on every commit (nothing lands), then a lost acknowledgement (lands once). The oversized test commits 600 + 600 chunks, loses the first group's acknowledgement and refuses the rest (the call fails after the runner's 10 s deadline), then retries. `one_stream_never_spans_groups` commits a 1 024-chunk group plus a second group and refuses 1 025 chunks of one stream. The 1 024-entry group commits with the default `Async1pc` (the Task 2 carry on large async-commit transactions holds at this size) | — |
| T5-12 | — | Review of #61 (CodeRabbit, 1 actionable): `drop_collection` also deletes the implicit link's `link/<id>` pointer. Not applied to the openraft drop: nothing writes a pointer for a collection's implicit link (only counter targets write `link/<id>`, and `create_link` refuses the collection kind), and changing its apply would change the replicated state machine for no effect | Valid for the TiKV backend as cleanup |
| T5-13 | Carries | T4-8 (a drop with a large index, now also one key per partition, is one transaction) stays open for Task 6's fault matrix or R2. Design §20 §11.3's `v/` counters and "`o/` rows written by `commit_wal`" are superseded by T5-4 and T5-5; Task 17 records the as-built text | — |
| T5-14 | Row T5-4 (no `v/` counters) | **Owner ruling (2026-09-27): accepted.** `watch_changes` wakes on the handle's own writes and polls every 100 ms until D63's scoped change feed | — |
| T5-15 | Row T5-3 / T4-8 (a large drop is one transaction) | **Owner ruling (2026-09-27): batching large drops is deferred to R2**, recorded in the R2 row of `docs/plans/README.md` and design §20 §18 next to the `gc_blocked_seconds` item. Task 6's fault matrix covers a large drop failing midway: it is atomic, so nothing half-applies (T6-10) | — |

### Task 6: the TiKV fault matrix and `operon dev --meta tikv://` (2026-09-27)

| # | Plan said | As built | Why |
|---|---|---|---|
| T6-1 | The Produces block | As planned (row R16): `ServerConfig.meta: MetaBackend { Raft (default), Tikv(TikvMetaConfig) }`, `Server { node: Option<MetaNode>, meta: Option<MetaClient> }`, `Server::meta() -> Option<&MetaClient>` (the 5 callers in `tests/it/http.rs` take `.expect(..)`), `--meta` on `dev` and `standalone` only; `ServerConfig::validate` refuses a TiKV metastore with `cluster`. Also: `MetaBackend::parse` and `TIKV_SCHEME` (module `meta_backend.rs`), a `?root=<hex>` URL parameter (a key prefix inside the keyspace; tests and the gate isolate by it), `ServerError::Tikv`, and feature `meta-tikv` (default on) pulling `operon-meta-tikv` and `operon-tikv`. Without the feature a `tikv://` URL is refused with a message naming it. Consequence: `operon` now depends on the git-pinned `tikv-client` by default, so it cannot be published to crates.io while row R4's pin stands (as T1-7). **Superseded by T7-3:** the feature is `tikv`, off by default | Tests need a fresh root per case; the pin carries over |
| T6-2 | Semantics 1 | 12 groups × 6 faults × 2 attempts = 144 cells, each on a fresh root with a faulted and a clean handle: `create_namespace`, `create_collection`, `commit_wal`, `commit_wal` over two partition groups with the fault in group 1 or in group 2 (the first group has committed), `swap_segment`, `trim_partition`, `cas_pointer` without and with a fence, `acquire_lease`, `drop_collection`, and a drop of 64 partitions and 1 024 entries. Faults: `Refuse` = `Refuse` at `BeforeBegin`, `LoseAck` = `LoseAck` at `AfterCommit`, `Undetermined` = `LoseAck` at `BeforeCommit` (an unknown outcome the token resolution fences into "not applied"), `Conflict` and `Delay(500ms)` at `BeforeCommit`, `Race` at `BeforeCommit`. Attempt 2 is reached by a `Conflict` at the first `BeforeCommit`; attempts are counted by the fault points they reach, not by the runner's attempt number (T6-6). Six cells run at once; a cell with a 1 024-entry transaction runs alone (several at once outlast the runner's 10 s deadline on one playground store). `BLESS=1` rewrites `meta_fault_matrix.tikv.expected.md`; the blessed table held in three further runs | §18 §4.2's groups, with the carries of Task 5 (T5-2, T5-10, T5-3) as their own groups |
| T6-3 | Semantics 1: four outcomes | A fifth outcome, `Rejected`: a `Race` whose competitor legitimately won (the same namespace, collection, pointer version or lease) is refused. The expected file also has a second table with each `Race` competitor's outcome (`committed`, `committed after a restart`) | Calling a correct refusal `SurfacedRetryable` would hide the difference between "retry" and "someone else won" |
| T6-4 | Semantics 2: "the suite's state checks" | The conformance suite has no state check, so `TikvMeta::check_invariants()` (public, one snapshot, scans the root) checks what the openraft state machine's `check_invariants` checks for the log: heads and entries belong to existing partitions, entries tile `[first, next)`, the log start is in the first entry, byte counts, `W/` counts equal WAL entries, no retired object is referenced, each collection's implicit stream exists. Per cell also: a clean retry of `commit_wal` returns the same offsets and the watermarks count the records once, acknowledged writes are visible, pointer versions and lease epochs are 1 after one write | — |
| T6-5 | Ruling 3 (async commit with 1PC for the metastore) | **Every metastore write now commits with two-phase commit** (`operon_meta_tikv::COMMIT_MODE = TwoPc`, set in `write_in` and the id-block transaction, whatever the handle's default). Found by `a_large_drop_failing_after_its_prewrite_applies_all_or_nothing`: with `Async1pc`, a write that fails after its prewrite with its rollback skipped (`tikv-client`'s `after-prewrite` and `before-rollback` failpoints; in production, a crash or a lost connection) left locks that no reader resolved: the retried drop timed out after 10 s and every read of those keys failed with conflicts for at least 65 s. The pinned client's reader path (`resolve_locks`) sends only `CheckTxnStatus`, which keeps an async-commit primary locked; only `LockResolver::cleanup_locks` (the GC path) checks the secondaries, and it runs only below the safe point, about 10 min later. With `TwoPc` the locks expire after their 3 s TTL, the retry rolls them back and the drop completes in 3.6 s. Cost: the spike's 30–50% commit latency advantage is lost for the metastore until the upstream fix (reader-side async-commit resolution through `CheckSecondaryLocks`). The conformance binary (61) and the matrix pass with `TwoPc` | Ruling 3's "a failing component switches to `two_pc` (recorded as a ruling) until the upstream fix". Carry: Task 10 (Live's `LiveTxn`) has the same exposure; Task 16 records the upstream status. Owner question: keep `TwoPc` for the metastore, and for Live too? **Answered in T7-1: yes, both** |
| T6-6 | — | Observed under `Async1pc`: a two-group `commit_wal`'s second group almost always restarts its first attempt before its commit, because the first group's locks on the object's shared `W/`, `r/` and `o/` rows are still being resolved. Harmless (the runner restarts), but it made attempt-numbered cells nondeterministic, hence T6-2's counting | — |
| T6-7 | Carry T5-2 (pessimistic waits restart) | Confirmed by the `Race` competitors: a competitor of a pessimistic call (`commit_wal`, `swap_segment`, `trim_partition`, group 2 of a two-group commit) waits on the call's locks and ends `committed after a restart`, never in a fair queue; a competitor of an optimistic call commits first and the call is `Retried` (it reran over the new state: the drop, the fenced CAS against a renewed lease) or `Rejected` | — |
| T6-8 | Semantics 4: "the M1.1 gates run with `--meta tikv://…`" | The kill -9 crash gate (`tests/crash.rs`, all scenarios) runs on TiKV when `OPERON_GATE_META=tikv://<pd>/<keyspace>` is set: every `operon dev` gets `--meta` with a random root per test (kept in `<dir>/meta-url`), the in-process setup and checks open that root, and `check_meta` runs `check_invariants`. The two `meta.snapshot.*` rows are openraft-only and skip. The CI job `tikv-nightly` runs it. **The object-store fault matrix and the simulation stay on openraft**: both build `MetaNode`s in process (the matrix has a meta-snapshot component; the simulation isolates and restarts nodes), and M1.2a Ruling 13 left other backends for them to M2 | Only the crash gate runs `operon dev`, which is what `--meta` configures. Owner question: add a backend switch to the S3 fault matrix and the simulation in R1, or keep M2. **Answered in T7-2: M2** |
| T6-9 | Semantics 3; carry T3 | `operon dev --meta tikv://…` opens `TikvMeta`, starts `GcLoop::spawn` on its handle with `GcConfig::default()` (life time 10 min = the handle's `gc_life_time`), and stops it at shutdown before the metastore. `sweep` is empty: the loop already sweeps its own handle's `t/` tokens, and Live's handles join in Task 12. The GC lease is under the handle's root, so servers on different roots (tests, gates) each run a loop; they all move the one cluster safe point to at most now − 10 min, which is harmless | — |
| T6-10 | Owner ruling T5-15 | `a_large_drop_refused_on_every_attempt_leaves_everything` (the collection, stream, 1 024 entries and live count stay; a clean drop then removes all) and `a_large_drop_failing_after_its_prewrite_applies_all_or_nothing` (T6-5). They and the matrix share a lock, since `tikv-client`'s failpoints are process-wide; `fail` 0.4 with `failpoints` is a dev-dependency | — |
| T6-11 | — | Review of #63 (CodeRabbit): no actionable comments. Its summary's "later-group failure leaves earlier groups committed" is D59's documented behaviour (T5-10; the retry completes the rest), and its fence-authority notes are auth, skipped per D111 | — |
| T6-12 | Files: `.github/workflows/ci.yml` | The `tikv` job's path filter also matches `crates/operon/src/{server,meta_backend,main}.rs` and `crates/operon/tests/meta_tikv.rs`, and it runs `cargo test -p operon --test meta_tikv`. `tikv-nightly` runs that plus the crash gate on TiKV (T6-8) | — |

### Task 7: the `loam.live.v1` protos and code generation (2026-09-27)

| # | Plan said | As built | Why |
|---|---|---|---|
| T7-1 | Owner ruling on row T6-5 | **Two-phase commit stays the default for the metastore and for Live** (Task 10's `LiveTxn` commits with `TwoPc`) until `tikv-client` resolves async-commit and 1PC locks on the read path. The `commit_mode` switch stays, so a component can flip back to `Async1pc` by a ruling once the fix is in the pinned revision. An upstream contribution is added to design §20 §11.4's list and to Task 16's exit report: a fix in `tikv/client-rust` that resolves async-commit and 1PC locks on the read path (`CheckSecondaryLocks` from the reader's lock resolver, not only from GC's `cleanup_locks`) | Ruling 3's cost: a crashed async-commit writer blocks readers until GC, about 10 min (T6-5) |
| T7-2 | Owner ruling on row T6-8 | **The object-store fault matrix and the simulation on the TiKV metastore are deferred to M2.** No M2 plan exists yet, so the item is a row of the roadmap table in `docs/plans/README.md` (M2, beside the metastore backends' fault matrices). R1's nightly keeps the crash gate on TiKV only | Both build `MetaNode`s in process; M1.2a Ruling 13 already left other backends to M2 |
| T7-3 | Owner ruling on row T6-1 | **`operon` keeps a publishable default build.** Its feature `meta-tikv` (default on) became `tikv`, **off by default**: `--meta tikv://`, the `operon-meta-tikv` and `operon-tikv` dependencies and the GC loop start. Without it, `MetaBackend::parse` refuses a `tikv://` URL with `NO_TIKV_FEATURE` ("…this operon was built without the tikv feature; rebuild with `cargo build -p operon --features tikv`…"), tested at the parser (`a_tikv_url_without_the_feature_names_the_feature`) and the CLI (`meta_flag_without_the_tikv_feature_is_refused_naming_it`, `dev` and `standalone`). The `operon-tikv` dev-dependency was dropped: the TiKV tests are `cfg(feature = "tikv")` and use the optional dependency. `cargo tree -p operon -e normal \| rg tikv-client` is empty for the default build. CI: the `tikv` and `tikv-nightly` jobs build `operon` with `--features tikv` (the nightly crash gate with `failpoints,tikv`); the check job adds `cargo clippy -p operon --all-targets --features tikv` and fails if the default build's tree has `tikv-client`. Own commit | Owner question T6-1. Residual (owner question): `cargo publish` also needs every *optional* dependency on crates.io, so publishing `operon` still needs `operon-tikv` and `operon-meta-tikv` published (blocked by row R4's git pin) or the feature removed from the published manifest |
| T7-4 | Semantics: `buf lint` passes with `STANDARD` | It does, with one exception in `buf.yaml`: `RPC_RESPONSE_STANDARD_NAME` for `live.proto`, because `Watch` streams `Transition` (§20 §7.1) rather than a `WatchResponse` wrapper. Without the exception the only finding is that rule | `Transition` is the protocol's unit in the design, Task 12 and Task 14; a wrapper would add a level to every client |
| T7-5 | Produces: the messages of §20 §7.1 | As listed, plus what the list left open: `Null {}` (an empty message, JSON `{"nullValue": {}}`; not `google.protobuf.NullValue`, which buffa would need the well-known types for); `Resume { last_version, query_set }`; `QuerySetChange { oneof change { QuerySpec add; uint32 remove } }`; `ModifyQuerySetResponse {}`; `QueryUpdate { query_id; oneof update { Value value; LiveError error; Removed removed } }`; `QueryRequest { function, args, optional uint64 ts }` and `QueryResponse { ts, result }`; `DeployResponse { deployment_id }`; `Schema { repeated TableSchema }`, `TableSchema { name, repeated IndexSchema }`, `IndexSchema { name, repeated string fields }`; `LiveError { ErrorCode code; message }` with `ErrorCode` `UNSPECIFIED`, `INVALID_ARGUMENT`, `NOT_FOUND`, `FAILED_PRECONDITION`, `RESOURCE_EXHAUSTED`, `FUNCTION_ERROR`, `FUNCTION_TIMEOUT`, `FUNCTION_OUT_OF_MEMORY`, `UNAVAILABLE`, `INTERNAL`. `journal.proto`: `JournalEntry { commit_hint_ms, repeated WriteRecord writes, function, request_id }`, `WriteRecord { table_id, doc_id (16 bytes), WriteKind kind, repeated bytes index_keys_removed, index_keys_added }`, `WriteKind` `INSERT`, `REPLACE` (a patch is a replace), `DELETE`. All three files are package `loam.live.v1` (buf's directory rule) | Tasks 8–14 consume them; a later task that needs a change edits the proto and regenerates |
| T7-6 | Rust: `operon_live_proto::loam::live::v1::*` | As planned, through a private `generated` module re-exported at the root (the generated server types have no `Debug`). Oneof enums are at `loam::live::v1::__buffa::oneof::<message>::<Oneof>` (for example `…::oneof::value::Kind`) and views at `__buffa::view`. `LiveServiceClient` is behind the crate feature `client` (connectrpc-build's `gate_client_feature`, forwarding `connectrpc/client`), so Task 12's tests enable `operon-live-proto/client`. The generated JSON mapping derives serde, so `serde` is a direct dependency. Workspace dependencies: `buffa` 0.9.2, `connectrpc` 0.9.1 (default features: gzip, zstd, json; `axum` is Task 12's), `connectrpc-build` 0.9; the lockfile gains 8 packages, `cargo deny` passes | — |
| T7-7 | TypeScript: `buf generate` with protoc-gen-es, checked in | `sdks/live-typescript/src/gen/loam/live/v1/{value_pb.ts,live_pb.ts}` (options `target=ts`, `import_extension=js`); `journal.proto` is excluded (internal). `buf.gen.yaml` uses the local plugin from `node_modules`, and `package.json` (`@operon/live`, private, Node ≥ 22, `packageManager` pnpm 11.13.0) pins exact versions: `@bufbuild/protobuf` 2.15.0 (runtime), `@bufbuild/buf` 1.73.0 and `@bufbuild/protoc-gen-es` 2.15.0 (dev). `pnpm run generate` regenerates. **`pnpm-lock.yaml` and `pnpm-workspace.yaml` are added now** rather than in Task 14, so CI installs the pinned generator with `--frozen-lockfile`; `pnpm-workspace.yaml` sets `allowBuilds: {"@bufbuild/buf": false}` (pnpm 11 asks; buf's postinstall only swaps its JS shim for the binary). The Connect runtime dependencies, `tsconfig*` and `biome.json` stay Task 14's. `node_modules/` is in `.gitignore`. The generated code type-checks under `tsc --strict` (checked once locally, not in CI until Task 14) | A regeneration check needs the generator pinned |
| T7-8 | CI regenerates the TypeScript and fails on a diff | New job `live-protos`: pnpm 11.13.0 and Node 22, `pnpm install --frozen-lockfile`, `buf lint`, `pnpm run generate`, then `git add -N` and `git diff --exit-code` on `src/gen` (so a new generated file also fails). Checked locally both ways (a clean regeneration passes; an added message fails). The Rust code is generated by `build.rs`, so the check job's build covers it. The `tikv` job's path filter also matches `crates/operon/Cargo.toml` | — |
| T7-9 | Tests: `value_binary_and_json_roundtrip`, `document_record_roundtrip` | Both, plus `int64_extremes_are_json_strings` (i64 min, max and 2^53 + 1 as JSON strings, and the JSON forms clients see: `{"nullValue": {}}`, `"NaN"`, base64 bytes). The roundtrips compare doubles by bits (NaN, `-0.0`). proto3 JSON leaves out fields at their default, so `creationMs` is absent for 0 | — |
| T7-10 | File structure: `operon` feature `live` (default on) | Consequence of T7-3 for Task 12: `live` pulls `operon-live` and so `operon-tikv` and the git pin, so it must be **off by default and imply `tikv`**; the file structure line is amended. Owner to confirm | Otherwise the default build depends on the git pin again |

Review of #65 (CodeRabbit): no actionable comments. Its retained concerns are D59's per-group `commit_wal` boundary (T5-10: a retry completes the rest) and TiKV transport authentication (skipped per D111); its docstring-coverage warning is not a review comment.

## Self-review

| Check | Result |
|---|---|
| §20 §18 R1 scope: `operon-tikv`, `operon-meta-tikv` with conformance and fault matrix, the GC loop | Tasks 1–6 |
| One Live app in one keyspace: documents, tables, indexes (§20 §4) | Task 8 |
| Mutations as TiKV transactions retried on conflict, idempotency, isolation (§20 §5.1–§5.2, D118) | Tasks 2, 10 |
| The commit journal (§20 §5.3, D119) | Task 9 |
| Reactivity: read sets, invalidation, fan-out, backpressure (§20 §8) | Tasks 11, 12 |
| Server functions in QuickJS (§20 §6, D120) | Task 13 |
| Sync API over connect-rust: `Watch`, `ModifyQuerySet`, `Query`, `Mutate`, `Deploy` (§20 §7, D121) | Tasks 7, 12 |
| Generated TypeScript client with a reactive layer (D121, D128) | Tasks 7, 14 |
| TiDB SQL in its own keyspace in the dev setup (§20 §10, D123) | Tasks 1, 15 |
| Keyspaces on API v2, keyspace GC (§20 §9.1, §9.3, D122) | Tasks 0, 1, 3 |
| D111: loopback only, non-loopback refused | Global Constraints, Task 12 |
| Testing: conformance, fault matrices, reactive and transaction checkers, nemesis, playground in CI (§20 §14) | Tasks 1, 5, 6, 16 |
| Q32 answered, Q33 re-confirmed before dependent work | Task 0 (rows R6–R8; Task 3 rewritten), Task 17 |
| Spike findings: playground command and configs, async commit/1PC default, `PessimisticRetry` restarts, TSO supervisor, RAM and port sizing | Tasks 1, 2; Ruling 3; Global Constraints |
| Review Focus → tests | 1: T9/T11/T16 · 2: T2/T6/T10 · 3: T5/T6 · 4: T2/T8 · 5: T12/T14/T16 · 6: T13 |
| Carried in | None |
