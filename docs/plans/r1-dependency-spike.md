# R1 Dependency Spike and Cluster Facts

Date: 2026-09-27. R1 plan Task 0 ([`2026-09-27-r1-reactive-core.md`](2026-09-27-r1-reactive-core.md)). Toolchain: rustc/cargo 1.97.1, edition 2024, resolver 3; `protoc` 36.1; cargo-deny 0.20.2; tiup 1.17.1 with playground v8.5.8.

Method:
- A throwaway crate `crates/r1-spike` was added to the workspace, so it resolved against the workspace's pins and built in the shared target directory. It depended on `tikv-client`, `connectrpc` (with a `connectrpc-build` build script over a small proto that has a server-streaming and a unary RPC), `buffa` and `rquickjs`. It was also the cluster probe: it contained a copy of `client-rust`'s generated `pdpb` stubs to call PD's GC RPCs directly.
- The crate and its lockfile changes were deleted afterwards. No dependency was added to the workspace, and `Cargo.lock` and `deny.toml` are unchanged in this commit.
- The earlier feasibility spike is summarized in design §20 (the notes marked *(spike)*). This report re-checks the facts that spike left open and does not repeat the rest.
- Every playground in this report ran as `--tag loams-t0 --port-offset 17000`: PD at `127.0.0.1:19379`, TiDB at `127.0.0.1:21000`, and the TiKV status server at `127.0.0.1:37180`. Each was stopped with `kill -INT` on the `tiup-playground` process, and `~/.tiup/data/loams-t0` was deleted after every run.

**Result:**
- Everything builds together with the workspace's pins.
- `cargo deny check` passes **only with `tikv-client` pinned to `tikv/client-rust` master** (`ab4be1c`). The crates.io 0.4.0 release fails the advisories check (section (b)). (Since row F1 the pin is Loams's fork of that revision; section (i).)
- **Q32 is answered no.** PD v8.5.8 has no keyspace GC-state RPCs, and TiKV v8.5.8 ignores keyspace-level safe points. GC is cluster-wide, and Loams has to act as the cluster's GC worker (section (g) check 3).
- Q33 holds on the pinned release.
- `memory-usage-limit` lowers TiKV's steady-state RSS but not its startup peak of about 2.6 GB.

---

## (a) Versions

| Crate / tool | Version | Features | License | Notes |
|---|---|---|---|---|
| `tikv-client` | git `https://github.com/tikv/client-rust`, rev `ab4be1c2cdd58d4e593202991fb520221c83bdfd` (2026-09-03, `version = "0.4.0"`) | `default-features = false` | Apache-2.0 | Not crates.io 0.4.0: see (b). Has `Config::with_keyspace`, `gc`, `cleanup_locks`, `lock_keys`, `batch_get_for_update`, `scan_reverse` and `Error::UndeterminedError`. Brings `tonic` 0.12 and `prost` 0.13 |
| `connectrpc` | 0.9.1 | `axum` (defaults `gzip`, `zstd` and `json` kept; add `client` for Task 12's tests) | Apache-2.0 | Uses `hyper` 1, `http` 1 and the workspace's `axum` 0.8 |
| `connectrpc-build` | 0.9.0 | — | Apache-2.0 | Runs the system `protoc` (on `PATH` or `PROTOC`). Output goes in through `connectrpc::include_generated!()` |
| `buffa` | 0.9.2 | default | Apache-2.0 | `buffa-codegen` and `buffa-descriptor` 0.9.2 come in through `connectrpc-build` |
| `rquickjs` | 0.14.0 | `futures`, `loader`, `macro`, `array-buffer` | MIT | Ships prebuilt bindings for `x86_64-unknown-linux-gnu`, so no bindgen or libclang is needed. The QuickJS C sources are compiled with `cc` |
| `rust-lapper` | 1.3.0 (examined, not used) | — | MIT | Unsuitable for Task 11: see (g) check 7 |
| PD / TiKV / TiDB | v8.5.8 (TiDB commit `8b857efa`, built 2026-08-27) | — | Apache-2.0 | TiKV v8.5.8 is built from kvproto `release-8.5` at `07aa8c6a46fab0a4cd577119c249c9c8e718c553` (its `Cargo.lock`); PD v8.5.8 pins kvproto `v0.0.0-20260526084754-39498d6b17fc` |
| `@bufbuild/protobuf` | 2.15.0 | — | Apache-2.0 AND BSD-3-Clause | runtime |
| `@connectrpc/connect`, `@connectrpc/connect-web` | 2.2.0 | — | Apache-2.0 | runtime |
| `@bufbuild/protoc-gen-es` | 2.15.0 | — | Apache-2.0 | dev; Node ≥ 22 |
| `@bufbuild/buf` | 1.73.0 | — | Apache-2.0 | dev |
| `@connectrpc/connect-node` | 2.2.0 | — | Apache-2.0 | dev; Node ≥ 22 |

## (b) `tikv-client`: crates.io 0.4.0 or a git pin

Both have `Config::with_keyspace`, so the plan's tie-breaker did not decide. Two facts did.

**1. cargo-deny.** With crates.io 0.4.0 (`default-features = false`), `cargo deny check` reported `advisories FAILED, bans ok, licenses ok, sources ok`. Four advisories, all through 0.4.0's `tonic` 0.10:

| Advisory | Crate | Path | Fix |
|---|---|---|---|
| RUSTSEC-2026-0258 (unbounded empty DATA frames; low) | `h2` 0.3.27 | `tonic` 0.10 → `hyper` 0.14 → `h2` 0.3 | `h2` ≥ 0.4.16; no 0.3 fix |
| RUSTSEC-2026-0098, RUSTSEC-2026-0099 (name constraints) | `rustls-webpki` 0.101.7 | `tonic` 0.10 (`tls`) → `rustls` 0.21 | ≥ 0.103.12 |
| RUSTSEC-2026-0104 (panic in CRL parsing) | `rustls-webpki` 0.101.7 | same | ≥ 0.103.13 |

With the git pin, and `allow-git = ["https://github.com/tikv/client-rust"]` added under `[sources]` in `deny.toml`, the result was `advisories ok, bans ok, licenses ok, sources ok`. With default features, 0.4.0 also pulls in `openssl`/`native-tls`, `procfs` and `protobuf` 2.28 through `prometheus/push` and `prometheus/process`. `default-features = false` removes them.

**2. Unknown commit outcomes.** 0.4.0 returns `Error::UndeterminedError` only when a 2PC primary commit fails with `Error::Grpc` (`transaction.rs:1409`), and since the tonic migration that variant covers connection establishment only. A lost response on an async-commit or 1PC prewrite comes back as a plain error. Under Ruling 3 (`async_1pc` by default), the runner would classify that as not applied and replay the mutation. Master (`transaction.rs:1340-1400,1486-1545,1720-1733`) returns `UndeterminedError` in two cases:
- an async-commit or 1PC prewrite fails with any gRPC error or with `errorpb.UndeterminedResult`;
- a primary commit fails the same way.

Master over-reports rather than under-reports, which is the safe direction.

Costs of the pin:
- A git dependency cannot be published to crates.io. `loams-tikv` and everything that depends on it (the `loams` binary with the `live` and `meta-tikv` features on) stay unpublishable until a crates.io release carries these fixes. That is acceptable during R1.
- The duplicates listed in (c).

## (c) Build facts

- **Lockfile** (throwaway crate included): 844 packages on `main`. The git pin adds 32 and crates.io 0.4.0 with default features off would add 44. With the pin, the additions are:
  - `tikv-client`, `tonic` 0.12.3, `prost`/`prost-derive` 0.13.5, `axum` 0.7.9, `axum-core` 0.4.5, `tower` 0.4.13;
  - `prometheus` 0.13.4, `fail` 0.4, `syn` 1.0.109 (through `async-recursion` 0.3 and `derive-new` 0.5), `itertools` 0.12, `matchit` 0.7, `socket2` 0.5;
  - `async-stream`, `take_mut`, `rustls-pemfile` 2;
  - the four `rquickjs` crates, the six `connectrpc`/`buffa` crates, `convert_case`, `relative-path` and `smoothutf8`.

  The pin shares `hyper` 1, `h2` 0.4, `http` 1 and `rustls` 0.23 with the workspace. crates.io 0.4.0 would also add `hyper` 0.14, `h2` 0.3, `http` 0.2, `http-body` 0.4, `rustls` 0.21, `tokio-rustls` 0.24, `axum` 0.6 and `base64` 0.21.
- **Duplicate majors** against the workspace:
  - `tonic` 0.12 beside 0.14, `prost` 0.13 beside 0.14, `axum` 0.7 beside 0.8, `tower` 0.4 beside 0.5;
  - `fail` 0.4 beside 0.5, `itertools` 0.12 beside 0.14, `syn` 1 beside 2.

  `multiple-versions = "allow"`, so none of these fails the check.
- **Build time:** `cargo build -p r1-spike` compiled 86 units in **32 s wall** (4 jobs, while the machine was also running other agents' builds and two playgrounds). That count includes the base crates rebuilt for this crate's feature set. An incremental rebuild of the crate alone took 7–11 s. The debug binary is 50 MB.
- **Build requirements:** a C compiler for `rquickjs-sys` (QuickJS), and the system `protoc`, which M1 already needs. No libclang and no C++.

## (d) connect-rust and buffa

`connectrpc-build` compiled a proto with a `oneof`, a server-streaming RPC and a unary RPC in `build.rs`. The generated service trait was implemented with `ServiceResult<ServiceStream<T>>` for the stream and `Response::ok` for the unary call, then served through `connectrpc::Router::new().add_service(..).into_axum_router()` on the workspace's `axum` 0.8. A Connect JSON call over HTTP/1.1 (`POST /spike.v1.SpikeService/Mutate`) answered `200` with `{"commitTs":"42"}`: `uint64` fields are strings in JSON, as the Connect protocol specifies. The TypeScript client must read them as `bigint`; `protobuf-es` does this for 64-bit fields by default.

## (e) rquickjs

- `Runtime::set_memory_limit(64 MiB)` and `set_interrupt_handler` both work.
- An ES module declared with `Module::declare` evaluated and exported a function (`f(21) = 42`).
- A `for(;;){}` busy loop was interrupted at the 200 ms deadline.
- An allocation bomb stopped with an error at the memory limit.
- Task 13 also needs the `array-buffer` feature for `ArrayBuffer` ↔ `Bytes`.
- The workspace lint `unsafe_code = "forbid"` is not a problem: rquickjs's API is safe.

## (f) TypeScript

Versions and licenses are in (a). All are Apache-2.0, or Apache-2.0 AND BSD-3-Clause for `@bufbuild/protobuf`, which is compatible with D11. `protoc-gen-es` and `connect-node` need Node ≥ 22, matching M1.6's toolchain.

## (g) Cluster checks

Playground command (Task 1's, with the configs of Task 1 semantics 1 written to the scratchpad):

```sh
export PATH=$HOME/.tiup/bin:$PATH
tiup playground v8.5.8 --tag loams-t0 --port-offset 17000 --pd 1 --kv 1 --db 1 --tiflash 0 --without-monitor \
  --kv.config tikv.toml --pd.config pd.toml --db.config tidb.toml
```

It was ready (all six pre-allocated keyspaces listed, and `select 1` answered through TiDB) **12 s** after start with the components already downloaded. PD listed `DEFAULT`=0, `loams_meta`=1, `loams_live_dev`=2, `sql_dev`=3, `loams_test_meta`=4, `loams_test_live`=5, `loams_test_sql`=6, all `ENABLED`, all with an empty `config` (no `gc_management_type`).

**1. API v2 and runtime keyspaces.**
- `SHOW CONFIG` reported `storage.api-version=2`, `storage.enable-ttl=true`, `memory-usage-limit=3GiB` and `storage.block-cache.capacity=1GiB`.
- `curl -X POST http://127.0.0.1:19379/pd/api/v2/keyspaces -d '{"name":"loams_rt_created"}'` created keyspace 7 in 0.24 s. `tikv-client` with `Config::with_keyspace("loams_rt_created")` then wrote and read it back.
- A second identical `POST` answered **HTTP 500** with the body `"keyspace already exists"`.
- `GET /pd/api/v2/keyspaces/<missing>` also answers **HTTP 500**, with the body `"keyspace does not exist"`.
- A client for a missing keyspace fails at connect with `InternalError { "…/pd/cluster.rs:364: keyspace does not exist" }`.

**2. Q33 (re-confirmed).**
- The TiDB with `keyspace-name = "sql_dev"` reports `keyspace-name sql_dev`. Through `mysql -h127.0.0.1 -P21000 -uroot` it ran `create database`, `create table`, `insert`, `update` and `select` (rows `1 a`, `2 bb`).
- A `tikv-client` scan of the whole `sql_dev` keyspace found 1 707 keys, all TiDB's (`m`: 194, `t`: 1 513).
- Scans of `loams_test_sql`, `loams_meta` and `DEFAULT` found none.
- No regression.

**3. Q32: keyspace-level GC.**
- **PD v8.5.8 does not implement the GC-state API.** `AdvanceTxnSafePoint` and `GetGCState` with a `KeyspaceScope` (stubs from `client-rust` master's kvproto) answer `Unimplemented: unknown method AdvanceTxnSafePoint for service pdpb.PD`. The PD binary's GC methods are `GetGCSafePoint`, `UpdateGCSafePoint`, `UpdateServiceGCSafePoint`, `GetAllGCSafePointV2`, `UpdateGCSafePointV2`, `UpdateServiceSafePointV2` and `WatchGCSafePointV2`. `AdvanceGCSafePoint`, `SetGCBarrier` and `GetGCState` exist only on PD master, the `9186d07` design §20 read.
- **TiKV v8.5.8 reads only the cluster safe point.** Its only PD GC calls are `GetGCSafePoint` and `UpdateServiceGCSafePoint` (from the binary; the metrics show `get_gc_safe_point` polled about every 10 s).
- TiDB v8.5.8's GC worker says the same (`pkg/store/gcworker/gc_worker.go:386-389` at tag `v8.5.8`): "Gc safe point is not separated by keyspace now. The whole cluster has only one global gc safe point … at least one TiDB with `keyspace-name` not set is required … If `keyspace-name` is set, the TiDB node will only do its own delete range, and will not calculate gc safe point and resolve locks."
- **Experiment** (`gc.enable-compaction-filter` switched off at runtime with `POST http://127.0.0.1:37180/config`, so that the GC worker scans regions instead of waiting for compaction):
  1. Write `v1` to 200 keys in `loams_test_meta`, take timestamp `mid`, overwrite them with `v2`, take `after`.
  2. `UpdateGCSafePointV2(keyspace 4, after)` returned `new_safe_point = after`, and `GetAllGCSafePointV2` lists it. For **90 s** a read at `mid` still returned `v1` for 10 of 10 sampled keys: **TiKV ignored it**. The cluster safe point stayed 0.
  3. `UpdateServiceGCSafePoint("gc_worker", ttl = i64::MAX, after)` returned `min_safe_point = after`, and then `TransactionClient::gc(after)` returned `Ok(true)` (it resolves locks in its keyspace, then calls `UpdateGCSafePoint`). Within **10 s**, `tikv_gcworker_autogc_safe_point` reached `after`, and reads at `mid` returned **`None` for 10 of 10 keys, not an error**.
  4. With `gc.enable-compaction-filter` back at its default (`true`), the same cluster-level GC left `v1` readable at `mid` for the whole 90 s. Versions below the safe point are dropped only when RocksDB compacts.
- **Answer:** on the pinned release, TiKV does not honour keyspace-level safe points. MVCC GC is cluster-wide, so something must act as the cluster's GC worker.
- **Fallback:** Loams's GC loop does that job itself, in the same way TiDB's GC worker does (service safe point `gc_worker`, resolve locks, then `UpdateGCSafePoint`), across every keyspace. There is no unified-GC TiDB. The plan's rows R6 and R7 rewrite Task 3.
- A read below the safe point returns missing or old data **without an error**, and `tikv-client` does not check it either. `loams-tikv` must refuse such reads itself.
- Whether `client-rust` accepts a patch that exposes GC safe points is still open (upstream PR status goes in the exit report).

**4. Build and licenses:** (b) and (c).

**5. RAM and `memory-usage-limit`.** Same workload in each run: 20 000 optimistic async-commit/1PC transactions of 10 keys × 1 KiB each (200 000 keys, 16 concurrent writers), then a full read-back. The tables show TiKV only; PD (85–100 MB RSS, 131 MB peak) and TiDB (180–365 MB RSS, 234–379 MB peak) were similar in every run.

| `tikv.toml` | TiKV reports | idle RSS | after load | peak (VmHWM) |
|---|---|---|---|---|
| no memory settings | `memory-usage-limit=12187459583B`, block cache 7.31 GB | 2.36 GB | 2.19 GB | **2.74 GB** |
| `memory-usage-limit = "3GB"`, block cache 1 GB (Task 1), run A | as set | 1.39 GB | 0.82 GB (0.51 GB after read-back) | **2.56 GB**, already reached before the load |
| same, run B | as set | 0.52 GB | 0.54 GB | **2.61 GB** |
| `memory-usage-limit = "1536MB"`, block cache 256 MB | as set | 2.26 GB | 1.72 GB | **2.61 GB** |

- **The peak does not follow the memory settings.** Every run reached 2.56–2.74 GB during startup, before any load.
- **Steady-state RSS is noisy.** The machine was about 5 GB into swap with two other playgrounds running, so RSS moved with host pressure (0.5–2.4 GB for identical configs). It stayed lower with explicit limits.
- Task 1 keeps `memory-usage-limit = "3GB"` and the 1 GB block cache: they bound steady state and stop TiKV from sizing itself to 12 GB. The Global Constraints' **3.2 GB peak** budget for the whole playground still holds.
- The source of the startup peak was not found (unverified).

**6. Error classification (feeds Task 2),** from the pinned source (`transaction.rs:1340-1400,1486-1545,1720-1733`) plus the earlier spike's observations:
- `Error::UndeterminedError(_)` → `Undetermined`. It is produced for an async-commit or 1PC prewrite, and for a 2PC primary commit, when the RPC failed or the region reported `UndeterminedResult`.
- `MultipleKeyErrors`/`KeyError` with `conflict: WriteConflict{reason: Optimistic}`, and `PessimisticLockError{WriteConflict{reason: PessimisticRetry}}` → `Conflict`.
- `KeyError.already_exist` → the caller's typed "exists" error (not retried).
- Any other error from `commit()` means the transaction did not commit → `NotApplied`, or `Fatal` for invalid arguments or unknown kinds. The commit point is the 1PC or async-commit prewrite, or the primary commit, and every unknown outcome there is `UndeterminedError`.
- `TimestampRequest channel is closed` (an `internal_err!` string) → `NotApplied` plus a client rebuild (Task 2 semantics 5).
- One more case is Loams's own: a commit future the runner drops at its deadline must count as `Undetermined`, because the client never sees it.

**7. The interval index (Task 11).** `rust-lapper` 1.3 needs primitive unsigned integer coordinates (`I: PrimInt + Unsigned`), so it cannot index byte-string key ranges. Its `insert` is an `O(n)` vector insert into three sorted vectors, and it has no remove. **Task 11 hand-writes the interval index:** an augmented interval tree (max-end per subtree) keyed by byte strings, one per (table, index), with incremental insert and remove.

## (h) Other findings

- **gRPC 4 MiB response limit.** A `scan` whose page exceeds 4 MiB fails with `GrpcAPI(OutOfRange: "decoded message length too large: found 10510000 bytes, the limit is: 4194304 bytes")`. That page was 10 000 keys of 1 KiB. `tikv-client` sets no larger decoding limit and does not page by bytes, and the same limit applies to `batch_get`. See plan row R10.
- **Pre-allocated keyspaces carry no GC config.** PD v8.5.8 has no GC-state manager, so `gc_management_type` is irrelevant on the pinned release.
- `TransactionClient::gc` needs a keyspace-scoped client on API v2 and resolves locks only in that keyspace's range. A cluster-wide GC therefore calls `cleanup_locks` once per keyspace.

## (i) The client-rust fork

Added 2026-09-27 (R1 plan rows F1–F5), on the owner's direction "for both tikv and resonate ... change and merge".

**Where it lives.** The fork is `https://github.com/ostrium-labs/client-rust`. Its branch `loam` starts at upstream `ab4be1c` (upstream master on 2026-09-27) and merges one topic branch per fix, each with a merge commit. Loams pins `loam` by rev (`1f8962b00b33ebc6f34b72d6c52d08b8a8e1906e`). Every topic commit is signed off, because upstream's README asks for a DCO sign-off. Each fix is drafted as an upstream PR; none is posted yet.

| Branch | Commit | Fix | Tests |
|---|---|---|---|
| `fix/tso-stream-reconnect` | `7acba21` | The TSO oracle reopens its stream in place, with backoff from 50 ms doubling to 1 s. Requests waiting on a failed stream fail with `TSO stream failed: …`. The request channel stays open while the oracle exists, so `TimestampRequest channel is closed` no longer follows a PD stall | 4 unit tests (in-memory PD). Live: PD SIGSTOPped for 5 s and 12 s under a 500 ms begin/commit loop. Before the fix, two begins failed with `TimestampRequest channel is closed` after PD resumed. After it, none failed |
| `feat/public-proto` | `ddd1063` | `pub mod proto`, re-exporting the `tonic` 0.12 and `prost` 0.13 the generated clients use | `tests/proto_tests.rs`, which does not compile without the change; a doc test |
| `fix/resolve-async-commit-locks` | `f037ebb` | `resolve_locks` resolves an expired async-commit primary through `CheckSecondaryLocks`, as client-go does. Also fixed: `cleanup_locks` committed a transaction that had a rolled-back secondary, and a forced `CheckTxnStatus` returned a cached result | 7 unit tests (4 fail without the change). `txn_read_resolves_expired_async_commit_locks`: 512 locks left by `after-prewrite`; on `ab4be1c` the read fails with `ResolveLockError`, on `loams` it takes 0.43 s and leaves no locks |
| `fix/async-commit-max-commit-ts` | `6072d81` | Async-commit and 1PC prewrites carry `min_commit_ts` and `max_commit_ts` (start + elapsed + 2 s). TiKV's fallback to 2PC is committed through the primary instead of at a wrong timestamp, and a 1PC fallback commits instead of failing with `OnePcFailure` | 5 unit tests (3 fail without the change) |

**Verification of `loams` (`1f8962b`)** on `tiup playground v8.5.8` (tag `loams-cr`, one TiKV on API v2):
- client-rust: 118 unit tests, 2 proto tests, 9/9 failpoint tests, 27/27 integration tests and 28/28 sync tests pass. `cargo clippy --all-targets --features integration-tests -- -D clippy::all` and `cargo fmt --check` are clean.
- Loams on the fork: loams-tikv passes 29 unit tests plus the codec 4, gc 8, keyspace 9 and runner 14 suites. loams-meta-tikv passes 17 unit tests, conformance (61 tests, the 53-case suite) and the fault matrix (3 tests; the 144-cell matrix matches the blessed table) with `TwoPc`, and did once with `Async1pc`. Reruns under load (load average 4–11 from other builds) failed the matrix's two-group `commit_wal` cells with 1 025 records, in both modes. The unchanged `origin/r1-t10` on the upstream pin failed the same cells under the same load (row F4). loams-live passes 5 unit tests plus the docs 9, journal 13, txn 18 and value 13 suites. `loams --features tikv --test meta_tikv` passes 2. `cargo deny check` reports `advisories ok, bans ok, licenses ok, sources ok`.

**What it changed in Loams:**
- The vendored kvproto protos and `loams-tikv/build.rs` are gone. `pd.rs` uses `tikv_client::proto` (row F2).
- The TSO supervisor stays as defence in depth (row F3).
- The metastore and Live keep `TwoPc`. Switching the metastore back to `Async1pc` is an owner question (row F4).
- `PessimisticRetry` stays the runner's job (row F5).

**Moving the pin.** To pick up upstream, merge upstream master into `loams` (or rebase the topic branches and re-merge them), rerun the client-rust suites and the loams-tikv, loams-meta-tikv and loams-live suites on the playground, and move the `rev` in `Cargo.toml` by a PR. Once a fix merges upstream, its topic branch is dropped from `loams` at the next move.
