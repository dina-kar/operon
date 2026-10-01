# CF1 — The Cloudflare Spike Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first where the task produces code. Each task lists what it must produce, the measurements it must record and its exit criteria. Where this plan gives exact values (names, paths, flags, thresholds), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: Planned** (2026-10-01). Design: [§35](../design/35-cloudflare-target.md) (D380–D387) and [§36](../design/36-loam-git.md). **Needs from the owner before Task 2:** a Cloudflare account on the Workers Paid plan ($5/month; Containers need it), an API token with Workers Scripts, Workers KV, Durable Objects, R2 and Containers edit rights for one account, and an R2 API token pair for the S3 API; all supplied as environment variables (`CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_API_TOKEN`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`), never committed. Expected spend under $50 (estimate). Branches `cf1-t<N>`; PRs target `main`. CF1 is a **spike**: its product is a report and rulings, plus two pieces of code that stay (`operon-fs` and the Cloudflare `Fs` backends). It changes no existing crate.

**Goal:** Decide, per workload, what ships on Cloudflare (§35 §9's decision rule), by running the draft's six steps with measurements:
1. Build and deploy workers-rs's Emscripten examples; measure size, cold start, CPU and memory; answer JSPI vs `LocalEventLoop` (Q380).
2. `operon-fs` with `MemFs` and `NativeFs` (in the main workspace), `DoSqliteFs` and `R2Fs` (in the Cloudflare workspace), and one conformance suite over all four.
3. gitoxide in a Durable Object: fetch a small repository's pack over Smart HTTP and index it (thin and native modes); find the largest pack that fits in 128 MB.
4. S3 PUT and GET to R2 through `operon-store` and through the binding; §36 §4.2's conditional-create race; R2's per-key write limit; whether `object_store` runs in a Worker (Q399).
5. Serve a crate download through a mirror prototype.
6. The same operations on the native runners (k3d with `NativeFs` and RustFS), and the comparison.
Plus a `CloudflareRunner` thin-mode prototype with the Tail Worker usage mapping (D383, D385), and the credits recommendation (D387).

**Architecture:**
- **Two workspaces.** `crates/operon-fs` joins the main workspace (it is small and dependency-light). Everything that depends on workers-rs, wasm-bindgen or Emscripten lives in a **separate cargo workspace at `deploy/cloudflare/`**, with its own `Cargo.lock` and `rust-toolchain.toml`, so the main workspace's lockfile, `cargo deny` and CI are untouched. The main workspace's `members = ["crates/*"]` does not include it.
- **Measurement harness.** `deploy/cloudflare/measure/` (a native Rust CLI, `cf-measure`) drives requests, captures `wrangler tail --format json` events (`CPUTimeMs`, `WallTimeMs`, outcome), reads each Worker's self-reported peak linear memory, and writes one JSON line per (workload, mode, size, run) into `docs/plans/cf1-results/<date>/*.jsonl`. The report table is generated from those files.
- **Peak memory** inside Wasm is measured as `core::arch::wasm32::memory_size(0) × 64 KiB` (linear memory never shrinks, so the value after a request is that request's high-water mark on a fresh isolate), reported in a response header `Loam-Wasm-Memory-Bytes`. Isolate memory outside linear memory (JS heap) is not visible and is noted as such.

**Tech Stack:** Rust 1.97.1 for the main workspace; the Cloudflare workspace pins what workers-rs's Emscripten examples require (Task 0 records: the Rust toolchain, `worker` and `worker-build` 0.8.7 or the `main` revision with the Emscripten examples, wasm-bindgen, the Emscripten SDK version, wrangler 4.x). `gix-pack`, `gix-hash`, `gix-packetline`, `gix-protocol` (client request encoding only) from the `gix` 0.88 train. Node ≥ 22 for wrangler. k3d and RustFS (`rustfs/rustfs:1.0.x`) for Task 7.

**Spec:**
- [`docs/design/35-cloudflare-target.md`](../design/35-cloudflare-target.md): §3 (the workload map), §4 (the `Fs` trait verbatim), §5 (the runner), §6 (limits and prices to confirm), §7 (usage), §8 (credits), §9 (the spike and its decision rule).
- [`docs/design/36-loam-git.md`](../design/36-loam-git.md) §4.2 (the commit protocol the R2 race tests), §6.3 (the receive steps Task 4 exercises), §9 (the mirror).
- The Cloudflare blog post of 2026-09-28 and the workers-rs `examples/emscripten*` directories.
- §34 (another branch): the `Runner` trait, for Task 8.

## Global Constraints

- **Secrets never land in the repository**, in results files or in logs: the harness redacts `Authorization` and every value of the four variables above.
- **Resources are named `loam-cf1-<purpose>`** and deleted by `deploy/cloudflare/teardown.sh` at the end of each task that creates them; Task 9 checks the account is clean.
- **Costs are watched.** Before each task, record the account's current-month usage (Workers, Durable Objects, R2, Containers) from the dashboard or API; stop and report if CF1's spend passes $50.
- **The build machine.** Wasm builds follow the same rule as cargo builds: one at a time, never during another agent's build, `-j 6`. The Emscripten SDK lives under `~/.local/share/emsdk` (not `/tmp`). Stop and report if `/home` has under 8 GB free.
- **Everything measured is repeated 20 times** (cold-start measurements 10 times, each after a fresh deploy or a 30-minute idle) and reported as p50, p95 and max.
- **Commit areas:** `fs`, `cloudflare`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **The Cloudflare code lives in its own workspace** at `deploy/cloudflare/` | workers-rs, wasm-bindgen and pinned Emscripten patch sets must not enter the main lockfile or `cargo deny` before the spike decides | Two lockfiles to maintain; acceptable for a spike, and the decision in Task 9 says whether it stays |
| 2 | **"Clone a repository" means fetching its pack over Smart HTTP v2 from a public host with a hand-built request, then indexing it** | gitoxide has no Workers transport; the constraint the spike must measure is pack indexing and object reads in 128 MB, not transport code | The real fetch path (GT2) adds negotiation cost; it is CPU-light next to indexing |
| 3 | **Peak memory is linear-memory size**, from a fresh deployment per measured request; measurements that use unique Durable Object ids instead are reported as isolate-wide, since several Durable Objects can share an isolate | The only figure Wasm exposes; JS heap is small for Rust Workers | Under-reports isolate memory; noted in the report |
| 4 | **The decision rule is §35 §9's**, applied per workload and mode, and recorded as rulings | One rule for every workload | The owner may override per workload in Task 9 |

## Carried in

Q380, Q381, Q383 and Q399 (§35) are answered here; Q384 is partly answered (R2 latency) and finished in GT1 Task 10; Q398 gets its evidence.

## Review Focus

1. **The measurements are reproducible**: the harness, the commands and the raw JSON lines are committed; the report is generated, not typed. Check Task 1.
2. **The `Fs` conformance suite is the same for every backend.** Check Task 3 (`fs_conformance!` instantiations).
3. **Conditional-write semantics on R2 match §36 §4.2** over both the binding and the S3 API. Check Task 5.
4. **Nothing in the main workspace depends on the Cloudflare workspace.** Check `cargo tree` in Task 9.

## File structure

```
crates/operon-fs/
  Cargo.toml
  src/{lib.rs,path.rs,mem.rs,native.rs,conformance.rs}
  tests/conformance.rs
deploy/cloudflare/
  Cargo.toml  Cargo.lock  rust-toolchain.toml  README.md  teardown.sh
  patches/README.md                       # each pinned patch set with its upstream PR (Task 2)
  examples/{emscripten,emscripten-tokio}/  # copied from workers-rs at the pinned revision, with the memory header added
  operon-fs-cloudflare/{Cargo.toml,src/{lib.rs,do_sqlite.rs,r2.rs},tests/worker/…}
  spike-git/{Cargo.toml,wrangler.toml,src/{lib.rs,thin.rs,native.rs}}
  spike-store/{Cargo.toml,wrangler.toml,src/lib.rs}
  spike-mirror/{Cargo.toml,wrangler.toml,src/lib.rs}
  spike-runner/{Cargo.toml,src/{lib.rs,deploy.rs,tail.rs},tail-worker/…}
  measure/{Cargo.toml,src/main.rs}
deploy/cloudflare/native/{k3d.sh,rustfs.sh}   # Task 7's native baseline
docs/plans/cf1-results/<date>/*.jsonl
docs/plans/cf1-spike-report.md
```

### Task 0: Reconcile and pin

**Files:** read §35, §36, §34 on its branch (or on `main` if merged), `crates/operon-store`, and the workers-rs repository at its latest release and `main`. Fill "Rulings made during execution".

**Checks** (record each with its command):
1. The workers-rs revision whose `examples/emscripten`, `examples/emscripten-tokio` and `examples/emscripten-tcp` build with `worker-build --emscripten` (and `--tokio`), and the toolchain, Emscripten SDK, wasm-bindgen and wrangler versions they need.
2. Which Tokio, `libc`, `socket2` and `mio` patches the Tokio example pins (from its `Cargo.toml` `[patch]` section), with each patch's upstream PR and state on the day.
3. Whether §34's `Runner` trait is on `main`; if not, Task 8 implements against a local copy of §34's draft signature in `spike-runner/src/lib.rs`, marked as such.
4. The account's plan, and that Durable Objects with SQLite, R2 and Containers are enabled.
5. The current Workers limits and pricing pages' "last updated" dates against §35 §6 (any change is a ruling row).

**Commit:** `docs: reconcile CF1 and pin the Cloudflare toolchain`.

### Task 1: The measurement harness

**Files:** `deploy/cloudflare/{Cargo.toml,rust-toolchain.toml,README.md,teardown.sh}`, `deploy/cloudflare/measure/`.

**Produces:** `cf-measure run --target <url> --workload <name> --mode thin|native --size <bytes> --runs 20 [--cold]` → JSON lines `{date, workload, mode, size, run, wall_ms_client, cpu_ms, wall_ms, wasm_memory_bytes, outcome, class_a_ops, class_b_ops}`; `cf-measure report <dir>` → the Markdown tables of the report. `cpu_ms`/`wall_ms` come from matching the request's `cf-ray` against `wrangler tail --format json` events; R2 operation counts from the request's own response headers (`Loam-R2-Class-A`, `Loam-R2-Class-B`, counted by the spike Workers).

**Tests:** `report_from_fixture_lines` (a fixed `.jsonl` renders a fixed table); `tail_event_matching_by_ray`; `secrets_are_redacted`.

**Exit:** the harness measures a trivial "hello" Worker (deployed as `loam-cf1-hello`) and its numbers are in the results directory.

**Commit:** `cloudflare: add the CF1 measurement harness`.

### Task 2: The Emscripten examples

**Files:** `deploy/cloudflare/examples/{emscripten,emscripten-tokio}/`, `deploy/cloudflare/patches/README.md`.

**Measures:** for each example in release mode: Wasm size raw and gzip; deploy time; cold start (10 fresh deploys); warm CPU-ms and wall-ms per request (20); peak linear memory; whether the Tokio example works with JSPI on the hosted runtime and with `LocalEventLoop` (**Q380**: the result, the flag or compatibility date needed, and the error when it fails).

**Exit:** both examples deployed, measured and torn down; `patches/README.md` lists every pinned patch with its upstream PR.

**Commit:** `cloudflare: build and measure the Emscripten examples`.

### Task 3: `operon-fs` and its Cloudflare backends

**Files:** `crates/operon-fs/` (main workspace), `deploy/cloudflare/operon-fs-cloudflare/`.

**Produces:** §35 §4.2's `Fs`, `FsPath`, `WriteMode`, `FsMeta`, `FsCaps` and `FsError` verbatim; `MemFs`; `NativeFs { root: PathBuf, durable: bool }`; `DoSqliteFs` (over workers-rs's Durable Object SQL storage, the schema of §35 §4.3, 1 MiB chunks, one storage transaction per mutation); `R2Fs` (over the R2 binding, `CreateNew` with `onlyIf`); and the conformance macro:

```rust
#[macro_export] macro_rules! fs_conformance { ($name:ident, $make:expr) => { /* one test per case below */ } }
```

**Cases:** `read_after_write`; `create_new_twice_is_already_exists`; `concurrent_create_new_one_wins` (Native, Mem, DoSqlite; on R2 through two Worker requests); `range_read_boundaries` (0, 1, chunk boundary ± 1, end); `append_extends` (if `caps.append`); `rename_replaces_atomically` (if `caps.atomic_rename`); `remove_missing_is_ok`; `list_is_sorted_and_paginates`; `path_validation`; `too_large_is_refused`. On Cloudflare the suite runs in a test Worker under `wrangler dev` (local workerd with Durable Object SQLite and the R2 simulator), and once against the real account.

**Measures:** latency p50/p95 and $ per 1,000 operations for write/read at 4 KiB, 1 MiB and 64 MiB on `DoSqliteFs` and `R2Fs`; the same sizes through `std::fs` over `worker-fs-mount`'s `durable-object-fs` in native mode (Q: is direct `DoSqliteFs` faster, and by how much).

**Exit:** the suite green on all four backends locally and on the account; measurements recorded.

**Commit:** `fs: add the Fs trait with MemFs, NativeFs and its conformance suite`; `cloudflare: add DoSqliteFs and R2Fs`.

### Task 4: gitoxide in a Durable Object

**Files:** `deploy/cloudflare/spike-git/`.

**Semantics:** a Durable Object `GitSpike` with two entry points:
- **thin** (`wasm32-unknown-unknown`): POST a v2 `fetch` request built with `gix-packetline` to `https://github.com/<fixture>.git/git-upload-pack` via the Workers `fetch` API for a fixture repository at three sizes (about 1 MiB, 10 MiB and 50 MiB packs; Task 0 picks public repositories and pins their commits), stream the pack into `DoSqliteFs`, index it with `gix-pack` (writing the idx into `DoSqliteFs`), then read the root tree and 100 blobs.
- **native** (`wasm32-unknown-emscripten`, `--tokio`): the same through `std::fs` on a `worker-fs-mount` `durable-object-fs` mount, using `gix-pack`'s file-based bundle API.

**Measures:** per size and mode: CPU-ms, wall-ms, peak linear memory, Durable Object rows written; the largest pack size that indexes within 64 MB of linear memory and within the 30 s CPU default (bisect up to 200 MiB); failure modes past it (out of memory, CPU limit) with their error text.

**Exit:** the table, and the size threshold §36 §6.3 uses to route pushes to a Container.

**Commit:** `cloudflare: measure gitoxide pack indexing inside a Durable Object`.

### Task 5: R2 through `operon-store` and the binding

**Files:** `deploy/cloudflare/spike-store/`.

**Semantics and measures:**
1. **Q399:** build `operon-store` (with `object_store`'s `aws` feature) for `wasm32-unknown-unknown` inside a Worker; record whether it builds and works against R2's S3 endpoint. If not, record the error and what an R2-binding `Store` adapter needs (the methods `operon-git` uses: `put_if_absent`, `get`, `get_range`, `list`, `delete`).
2. Latency p50/p95/p99 of PUT (1 KiB, 64 KiB, 1 MiB, 8 MiB) and GET (full and 64 KiB range), through the binding and through the S3 API, from a Worker and from a native client in one region.
3. **The commit race** (§36 §4.2): 16 concurrent Worker requests create `loam-cf1/race/<run>/wal/00000000000000000001.lgw` with `If-None-Match: *` (S3 API) and with `onlyIf: { etagDoesNotMatch: "*" }` or the equivalent conditional headers (binding): exactly one succeeds in each of 100 runs, and never two; each loser gets `412` (S3 API) or `null` (binding), or `429` from R2's one-write-per-second limit on one key. Record the split of `412` and `429` and confirm that a `429` loser's follow-up GET finds the winner's object (§36 §4.2's resolution).
4. **The per-key limit:** write one key twice a second for 60 s; record the error code and rate at which R2 refuses (expected: the 1 write/s limit of the R2 limits page).
5. `If-Match` on overwrite with a stale ETag returns `412`.

**Exit:** the table; a ruling row confirming or amending §36 D389's premises on R2.

**Commit:** `cloudflare: measure R2 latency and conditional-write semantics`.

### Task 6: A crate download through a mirror prototype

**Files:** `deploy/cloudflare/spike-mirror/`.

**Semantics:** a thin Worker serving `config.json`, sparse index files (read-through from `index.crates.io` with ETag revalidation, cached in R2 and the Cache API) and `.crate` downloads by `cksum` (read-through from `static.crates.io`, SHA-256 checked, stored in R2), i.e. GT3 Task 5's routes without policy. `cargo fetch` of a 40-dependency fixture lockfile against it.

**Measures:** cold (upstream) and warm (R2, Cache API) latency per index file and per crate; CPU-ms; Class A and B operations per `cargo fetch`; **Q381:** separately, the cold start of a minimal Container (`lite` and `standard-2`) running `git --version`, for §36 §7's repack routing.

**Exit:** `cargo fetch` succeeds through the Worker; the table.

**Commit:** `cloudflare: prototype the crates mirror on Workers and R2`.

### Task 7: The native baseline

**Files:** `deploy/cloudflare/native/{k3d.sh,rustfs.sh}`, native builds of `spike-git`'s thin path, `spike-store` and `spike-mirror` (as plain binaries using `NativeFs` and `operon-store` on RustFS).

**Measures:** the same operations, sizes and runs as Tasks 3–6 on k3d with `NativeFs` and RustFS on the build machine (noting its hardware and load), CPU from `getrusage`, memory from RSS; and a per-operation cost estimate at beta volume using §35 §6's prices for Cloudflare and a stated $/vCPU-hour and $/GB-month for the native runner.

**Exit:** the comparison table: per workload and mode, Cloudflare vs native for CPU, wall time, memory and cost.

**Commit:** `cloudflare: measure the native-runner baseline`.

### Task 8: The `CloudflareRunner` thin prototype and usage mapping

**Files:** `deploy/cloudflare/spike-runner/`.

**Semantics:** an implementation of §34's `Runner` (or the local stand-in, Task 0 check 3) that deploys a Hono `fetch` bundle through the Workers scripts API with a `compatibility_date`, routes it, invokes it, and retires it (§35 §5.1's table); and `loam-tail`, a Tail Worker that maps trace events to `loam.meter.v1.Invocation` (§35 §7) and POSTs `HostReport`s to a local consumer exposed for the test (a tunnel or a public test endpoint the owner provides), resending until acknowledged; **Q383:** the Durable Object duration billed for a sequencer-shaped object that awaits R2 PUTs (from the account's usage after 10,000 such requests).

**Measures:** deploy-to-first-request time; `cpu_usec` from `loam-tail` against `CPUTimeMs` in `wrangler tail` (must match exactly); reports lost or duplicated across a forced consumer restart (duplicates allowed, deduped by `(host_id, seq)`; none lost).

**Exit:** the prototype deploys, invokes and retires a function; usage reaches the consumer.

**Commit:** `cloudflare: prototype the thin CloudflareRunner and the Tail Worker usage mapping`.

### Task 9: The report, the rulings and the credits recommendation

**Files:** `docs/plans/cf1-spike-report.md` (generated tables plus text), `docs/design/35-cloudflare-target.md` (an "After CF1" note per affected section: the limits confirmed or corrected, the size thresholds, the decisions), this plan's rulings.

**Contents of the report:**
1. Per workload of §35 §3 and mode: the measurements, §35 §9's rule applied, and the verdict (ship on Workers; ship on Containers; stay on Kubernetes).
2. Answers to Q380, Q381, Q383, Q399, and the evidence for Q398 (native mode on pinned patches).
3. The thresholds for routing Git work to Containers (Task 4) and the R2 premises of §36 D389 (Task 5).
4. **The credits recommendation** (D387): whether CF1's verdicts justify applying now, the tier Loam can apply for (the owner's answer to Q397), the expected first-year burn by product from the measurements, and whether the $10K R2 cap binds.
5. Whether `deploy/cloudflare/` stays a separate workspace, and the teardown check (no `loam-cf1-*` resources left in the account).

**Exit:** the report merged; the owner rules on each verdict (recorded as rulings), on Q398 and on the timing of the credits application.

**Commit:** `docs: report the Cloudflare spike and its verdicts`.

## Rulings made during execution

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| — | (Task 0 fills this table) | | |
