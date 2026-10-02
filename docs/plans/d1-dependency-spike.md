# D1 Dependency Spike: the Embed Measured in the Workspace

Date: 2026-09-27. D1 plan Task 0 ([`2026-09-27-d1-durable-execution.md`](2026-09-27-d1-durable-execution.md)). Toolchain: rustc/cargo 1.97.1, edition 2024, resolver 3, clang + lld (`~/.cargo/config.toml`); cargo-deny 0.20.2; tiup 1.17.1 with playground v8.5.8; Python 3.13.15 with uv 0.12.13; Node v26.8.2.

Method:
- A local scratch branch of the workspace (`d1-t0-scratch`, never pushed) added the fork's crates as git dependencies of the workspace, a throwaway crate `crates/loams-durable`, and the features `durable` and `durable-mysql` on `loams`, both default.
  - The throwaway crate built the registry (SQLite, MySQL, http-poll, http-push, the HTTP gateway), started it with `resonate_base::build` and `Running::start`, and stubbed Task 6's `InProcNetwork`.
  - `loams`'s `main` called it when `LOAMS_D1_PROBE` was set, so the whole embedded graph, SDK included, was reachable from `main` and linked into the measured binary.
  - None of it is in this commit: `Cargo.toml`, `Cargo.lock` and `deny.toml` are unchanged.
- The source is the fork `https://github.com/ostrium-labs/resonate` at **`c3f25b94301737f4bcfff503e25f2b0e36d57fb9`**. That is the branch `deps/advisories-rustls`: upstream `28dfd01` plus PR 0c (upstream #1164) only.
  - The TiDB fixes (PR 0a #1162, PR 1 #1163) change only `resonate-server-mysql`'s source, not its dependencies, so they do not move any number here.
  - The branch `loam/0.10.1` does not exist yet. Task 1 creates it and pins its own revision.
- Builds used `CARGO_BUILD_JOBS=4 CARGO_INCREMENTAL=0` and a dedicated target directory, `~/.cache/cargo-target/loams-d1`. That directory was wiped before each cold build.
- Other agents' builds and tests ran during every build (below), so the times are contended. Each build started only when no other cargo build was running, but builds that started later overlapped it.
- The TiDB playground ran as `--tag loams-d1-t0 --port-offset 27000` (TiDB `127.0.0.1:31000`), only while no D1 build ran. It was stopped with `kill -INT` and `~/.tiup/data/loams-d1-t0` was deleted.
- Scratch files (lockfiles, cargo-deny output, timings, probe output): `scratchpad/d1t0/` in the session scratch directory (not committed).

**Result:**
- Everything resolves into one lockfile with the workspace's pins and builds.
  - sqlx is 0.8.6, and `libsqlite3-sys` 0.30.1 is the only SQLite link.
  - There is no `openssl-sys` anywhere.
  - R1's `tikv-client` pin resolves beside it.
- `cargo deny check` fails on today's `deny.toml` (sources, and the `rsa` advisory). With the plan's two additions it passes: `advisories ok, bans ok, licenses ok, sources ok`.
- The embed adds **+13.0 MB stripped** (+18.5 MB unstripped) to a 292.6 MB stripped `loams`, and **+125 s** (+8 %) to a cold release build (1,622 s against 1,497 s, both contended).
- **Toggling `durable` rebuilds most of the tree.** `verus_syn` (through `resonate-timer-wheel`) turns on `proc-macro2/span-locations`, so every proc-macro and everything downstream of one compiles differently.
  - The incremental build that added `durable` to a warm `--no-default-features` tree recompiled 479 units and took 23 min 44 s. The cold build without `durable` took 24 min 57 s.
  - See (e).
- Check 5 passes on SQLite and on TiDB v8.5.8: `promise.search` by tags, in process.
- Check 6:
  - Python `resonate-sdk` 0.8.1, now on PyPI, passes. PyPI 0.7.4 is refused.
  - TypeScript `@resonatehq/sdk` 0.11.5 (npm, the same version as the monorepo) passes. npm 0.10.4, which the example repositories pin with `^0.10.0`, is refused.

---

## (a) Versions

| Crate / tool | Version | Source | License | Notes |
|---|---|---|---|---|
| `resonate-base`, `-plugin`, `-core`, `-server-sqlite`, `-server-mysql`, `-transport-http-poll`, `-transport-http-push`, `-gateway-http` (+ `-auth`, `-sql`, `-timer-wheel` transitively) | 0.10.1 | git `ostrium-labs/resonate` rev `c3f25b9` | Apache-2.0 | Path dependencies inside the fork carry versions (PR 0c), so cargo-deny's wildcard ban passes |
| Rust SDK | **package `resonate-sdk`** (lib `resonate_sdk`) 0.6.0, with `resonate-sdk-macros` 0.1.0 | same rev, `impl/sdk/rs/resonate` | Apache-2.0 | The package named `resonate` in that repository is the server binary (`impl/server/core`), not the SDK |
| `rusqlite` / `libsqlite3-sys` | 0.32.1 / 0.30.1 (bundled) | crates.io | MIT | The only `libsqlite3-sys`; no `sqlx-sqlite` in the graph |
| `sqlx` (`-core`, `-mysql`, `-macros`) | 0.8.6 | crates.io | Apache-2.0 OR MIT | `rsa` 0.9.10 through `sqlx-mysql` |
| `prometheus` / `protobuf` | 0.14.0 / 3.7.2 | crates.io | Apache-2.0 / MIT | R1's `tikv-client` adds `prometheus` 0.13.4 beside it (check 2) |
| `axum` | 0.7.9 (Resonate's gateway) beside the workspace's 0.8.9 | crates.io | MIT | Isolated: no type crosses (§21 §11.3) |
| `reqwest` | 0.12.28 (push transport, auth) and 0.13.5 (SDK, `google-cloud-auth`, and the workspace's `object_store`) | crates.io | Apache-2.0 OR MIT | Both on rustls. reqwest 0.13's default TLS is rustls (aws-lc-rs), so the SDK's default features bring no OpenSSL |
| `google-cloud-auth` | 1.17.0 | crates.io | Apache-2.0 | Unconditional in `resonate-transport-http-push` at `c3f25b9`. Task 1's `gcp-idtoken` feature removes it with `google-cloud-gax`, `-rpc`, `-wkt` and `jsonwebtoken` 11 |
| Verus: `vstd`, `verus_builtin`, `verus_builtin_macros`, `verus_syn`, `verus_prettyplease`, `verus_state_machines_macros` | exact pre-releases `0.0.0-2026-08-*` | crates.io | MIT | Through `resonate-timer-wheel`; see (e) |
| Python SDK | `resonate-sdk` 0.8.1 (PyPI) | PyPI | Apache-2.0 | Same version as the monorepo's `impl/sdk/py` |
| TypeScript SDK | `@resonatehq/sdk` 0.11.5 (npm `latest`) | npm | Apache-2.0 | Same version as the monorepo's `impl/sdk/ts` |

## (b) Check 1: build time, binary size, duplicates, cargo-deny

### Build time and size

```
CARGO_BUILD_JOBS=4 CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=~/.cache/cargo-target/loams-d1 \
  cargo build --release -p loams [--no-default-features --features flight,hnsw,qdrant] --timings
```

| Build | Units | Wall | Summed unit time | Binary unstripped / stripped |
|---|---|---|---|---|
| Cold, without `durable` (`--no-default-features --features flight,hnsw,qdrant`) | 1,004 | **1,497 s** (24 min 57 s) | 5,608 s | 422.2 MB / **292.6 MB** |
| Cold, with `durable` (default features) | 1,221 | **1,622 s** (27 min 2 s) | 6,024 s | 440.7 MB / **305.6 MB** |
| Incremental: `durable` added to the warm tree of the first row | 1,221 (479 crates recompiled) | 1,425 s (23 min 44 s) | 5,280 s | same as the row above |

- **Delta, cold:** +125 s wall (+8 %) and +416 s of summed unit time (the plan's estimate was +60–90 s). The Resonate-only units sum to 367 s. The largest are:

  | Unit | Time |
  |---|---|
  | `resonate-server-mysql` | 50.7 s |
  | `libsqlite3-sys` | 39.8 s |
  | `resonate-gateway-http` | 31.4 s |
  | `google-cloud-auth` | 29.6 s |
  | `protobuf` 3.7 | 26.8 s |
  | `resonate-sdk` | 22.7 s |
  | `resonate-server-sqlite` | 18.1 s |
  | `sqlx-mysql` | 11.3 s |

  At 4 jobs these overlap the workspace's long DataFusion and Lance chain.
- **Delta, size:** +18.5 MB unstripped and **+13.0 MB stripped** (+4.4 %), just above the plan's +8–12 MB estimate. The binary links the SDK and both server plugins.
- **Contention.** Samples every 30 s counted the other agents' cargo processes: 0 in 11 of 49 samples during the cold build without `durable`, and 3–8 in the rest. During the incremental build: 0 in 17 of 47, and 1–7 in the rest. During the cold build with `durable`: 0 in 6 of 53, and 3–6 in the rest, so it was more contended than the build without. The wall times compare only roughly. The summed unit times and unit counts are more stable.

### Duplicates (`cargo tree -d`)

`cargo tree -p loams -e normal,build --target x86_64-unknown-linux-gnu` was run with and without `durable` on the same lockfile:

| | Without `durable` | With `durable` |
|---|---|---|
| Packages | 724 | 809 (+85) |
| Names with more than one semver-incompatible version | 53 | 68 (+15) |

The 15 names that become duplicated:
- **11 against the workspace**, as in the spike: `axum` 0.7, `axum-core` 0.4, `matchit` 0.7, `sha2` 0.10, `md-5` 0.10, `const-oid` 0.9, `cpufeatures` 0.2, `spin` 0.9, `convert_case` 0.4, `synstructure` 0.13 and `untrusted` 0.7.
- **4 inside the new subgraph:**
  - `hmac` 0.12, through `sqlx-mysql`'s `hkdf`;
  - `hashlink` 0.9 (`rusqlite`) and 0.10 (`sqlx-core`);
  - `jsonwebtoken` 9 (`resonate-auth`) and 11 (`google-cloud-auth`);
  - `webpki-roots` 0.26 (`sqlx-core`) and 1.0.

The full lockfile gains 103 entries. 22 of them are new versions of existing names, 9 of which are `windows-*` 0.48 and 2 other non-Linux crates.

### cargo-deny

| `deny.toml` | Result |
|---|---|
| As on `main` | `advisories FAILED, bans ok, licenses ok, sources FAILED`: 13 × `source-not-allowed` (the fork's git source) and RUSTSEC-2023-0071 (`rsa` 0.9.10 through `sqlx-mysql` 0.8.6, "No safe upgrade is available") |
| With Task 1's additions: `[sources] allow-git = ["https://github.com/ostrium-labs/resonate"]` and `[advisories] ignore = [{ id = "RUSTSEC-2023-0071", reason = "rsa via sqlx-mysql: used only for RSA password exchange on non-TLS MySQL connections; Loams connects to TiDB over TLS or the cluster network with mysql_native_password (§21 §11.2)" }]` | **`advisories ok, bans ok, licenses ok, sources ok`**: no other advisory, no unmaintained warning, no license finding |

- `[graph] all-features = true` means the check covers `durable-mysql`.
- Git dependencies without a `version` do not trip `wildcards = "deny"`.

## (c) Check 2: one lockfile

| Question | Command | Result |
|---|---|---|
| sqlx version | `cargo tree -p loams -e normal -i sqlx` | 0.8.6 only, used by `resonate-sql`, `-server-mysql` and `-server-sqlite` (the last only for its migrator types; no `sqlx-sqlite`) |
| A second `libsqlite3-sys`? | `cargo tree -p loams -e normal -i libsqlite3-sys --target all` | No: one, 0.30.1, through `rusqlite` 0.32.1 |
| OpenSSL | `cargo tree -p loams -e normal -i openssl-sys --target all` | `package ID specification openssl-sys did not match any packages`: no `openssl`, `openssl-sys` or `native-tls` in the lockfile |
| R1's `tikv-client` | the pin `tikv-client = { git = "https://github.com/tikv/client-rust", rev = "ab4be1c", default-features = false }` added to the scratch crate, then removed | It resolves beside Resonate with no conflict and still no `openssl-sys`. It brings `prometheus` 0.13.4 beside Resonate's 0.14.0, which gives **two separate default registries**. A future `/metrics` has to gather both |

## (d) Check 3: the Rust SDK

- **A custom `Network` is accepted.** `ResonateConfig { network: Some(Arc::new(stub)), group: Some("loams"), pid: Some("probe"), ttl: Some(60_000), .. }` compiled.
  - The stub forwards `send` to `ResonateServer::process` in process and implements `recv` as a no-op.
  - `Resonate::new(config)` started.
  - `sdk.promises.get(<id>)` and `sdk.promises.search(None, Some({"loams:op": <id>}), …)` returned the root and 3 promises, over no socket.
- **The trait as built:** `#[async_trait] pub trait Network: Send + Sync`. The methods are `pid`, `group`, `unicast`, `anycast`, `start`, `stop`, `send(String) -> Result<String>`, `recv(Box<dyn Fn(String) + Send + Sync>)` and `target_resolver(&str) -> String`. The error type is `resonate_sdk::error::Error`, and transport failures use `Error::NetworkError(String)`.
- **`Resonate::new` returns `Self`, not a `Result`, and has no separate `start`.**
  - It spawns `network.start()` and only logs an error from it, so it needs a Tokio runtime.
  - It reads `RESONATE_TOKEN` from the environment even with a custom network. It reads `RESONATE_URL`/`HOST`/`PORT` only without one.
  - `Resonate::stop()` exists.
- **`reqwest` without default features.** The local copy of the SDK with `reqwest = { version = "0.13", default-features = false, features = ["json", "stream"] }` was patched in with `[patch."https://github.com/ostrium-labs/resonate"]`. `cargo check -p resonate-sdk` passed in 48 s.
  - OpenSSL is not the reason for the fork commit: reqwest 0.13's `default-tls` is rustls.
  - With the defaults, the SDK unifies `charset`, `default` and `system-proxy` into the workspace's `object_store` reqwest 0.13. With the patch those three are gone. `google-cloud-auth` still adds `default-tls`, `form`, `json` and `query`, until Task 1's `gcp-idtoken` feature removes it.

## (e) Feature unification: what `durable` changes in shared crates

`cargo tree -e normal,build -f '{p}|{f}'` was run with and without `durable`. The crates both graphs share whose features change:

| Crate | Features added by `durable` | From |
|---|---|---|
| `proc-macro2` 1.0.107 | `span-locations` | `verus_syn` (the Verus macros of `resonate-timer-wheel`) |
| `syn` 2.0.119 | `visit` | Resonate/sqlx macros |
| `zeroize` | `derive` | `rsa`/RustCrypto |
| `bitflags` 2, `either`, `serde_with` | `serde`, `base64` | sqlx, Resonate |
| `rand` 0.8, `rand_core` 0.6, `getrandom` 0.2 | `std`, `std_rng`, `getrandom` | `rsa`, sqlx |
| `digest` 0.10/0.11, `ring`, `aws-lc-rs`, `lazy_static` | small feature additions | RustCrypto, `jsonwebtoken`, `google-cloud-auth` |
| `tokio-stream` | `fs` | sqlx |
| `tower-http` 0.6 | `catch-panic`, `cors`, `trace` | `resonate-gateway-http` |
| `hyper-util` | `client-proxy-system` | reqwest 0.13 `system-proxy` |
| `hyper-rustls` 0.27 | `webpki-roots` | reqwest 0.12 `rustls-tls-webpki-roots` (push transport) |
| `reqwest` 0.12 | `rustls-tls`, `rustls-tls-webpki-roots` | push transport, `resonate-auth` |
| `reqwest` 0.13 | `default`, `default-tls`, `charset`, `form`, `json`, `query`, `system-proxy` | SDK defaults, `google-cloud-auth` |

- `proc-macro2/span-locations` changes the fingerprint of every proc-macro crate (`serde_derive`, `tokio-macros`, `thiserror-impl` and so on), and so of nearly every crate in the workspace.
- A build of `loams` with `durable` and a build without it therefore share few artifacts: adding `durable` to a warm tree recompiled 479 crates.
- A `-p <crate>` build that does not reach `loams` (such as `cargo test -p loams-query`) keeps its own artifacts, as before. With `durable` on by default, `-p loams` builds and `-p <library>` builds no longer share proc-macro artifacts. That costs roughly one more copy of the tree in a shared target directory, and one more cold compile of it.
- The verified timer wheel needs `verus!` at compile time. That macro erases the ghost code under plain rustc, so the dependency cannot simply be dropped. Options are in the plan's owner questions.
- `system-proxy` on Linux adds no proxy source: reqwest reads `HTTP(S)_PROXY` with or without it.
- `webpki-roots` makes the push transport trust Mozilla's roots instead of the OS store. Push is off by default.

## (f) Check 4: port 8001

- `grep -rnI -E '(^|[^0-9])8001([^0-9]|$)'` over the worktree (target and `.git` excluded) finds 8001 only where Resonate is meant: §01 §3, §10 §2, §13 D138, §21, the design README and the D1 plan.
- `crates/loams/src/main.rs` defaults are 8080 (HTTP), 8082 (Flight SQL), 6333/6334 (Qdrant); tests use ephemeral ports.
- R1's playgrounds use offset 17000; D1's uses 27000. Neither reaches 8001.

Port 8001 is free in every documented configuration.

## (g) Check 5: `promise.search` by tag, in process

```
LOAMS_D1_PROBE=sqlite:<dir>/default.db LOAMS_D1_BIND=127.0.0.1:18001 ./loams            # SQLite
LOAMS_D1_PROBE=mysql://root@127.0.0.1:31000/loams_durable_default LOAMS_D1_BIND=… ./loams  # TiDB v8.5.8
```

The probe created 4 promises through `ResonateServer::process`, all answered 200:
- the root `op-probe<pid>`, tagged `loams:op`, `loams:kind=collection.import`;
- two branches `op-probe<pid>:f0` and `:f1`, tagged `loams:op`, `loams:kind=file`, `loams:file`;
- one unrelated promise, tagged `loams:kind=file`.

It then settled `:f0` as resolved and searched:

| Search (`promise.search`) | SQLite | TiDB v8.5.8 |
|---|---|---|
| `tags: {loams:op}` | 200: root, `:f0`, `:f1` | 200: same |
| `tags: {loams:op, loams:kind: file}` | 200: `:f0`, `:f1` | 200: same |
| `tags: {loams:op, loams:kind: file}, state: resolved` | 200: `:f0` | 200: same |
| SDK `promises.search` over the stub network | 3 | 3 |

- The tag filter is containment: every given pair must be present.
- On SQLite it is a scan (`json_each(?) EXCEPT json_each(tags)`, no index), which confirms the 2 s progress cache (§21 §6.4).
- On TiDB the migration created `_sqlx_migrations`, `callbacks`, `listeners`, `promises` and `schedules`. `@@tidb_txn_mode` was `pessimistic`, the playground's default; PR 1's per-session pin is not in this revision.

## (h) Check 6: SDK versions against server 0.10.1

The embedded server was started with `LOAMS_D1_SERVE=1 LOAMS_D1_PROBE=sqlite:… LOAMS_D1_BIND=127.0.0.1:18001`, and each client ran with `RESONATE_URL=http://127.0.0.1:18001`:

| Client | Version | Program | Result |
|---|---|---|---|
| Python, PyPI | `resonate-sdk` 0.8.1 | `example-hello-world-py/main.py` (commit `8325aa5`) | **PASS**: `Hello World from foo! Hello World from bar! Hello World from baz!` |
| Python, PyPI | `resonate-sdk` 0.7.4 | same | FAIL: `400 Promise ID must be prefixed by resonate:origin` (as in the research) |
| TypeScript, npm | `@resonatehq/sdk` 0.11.5 | a hello-world with the same three functions (generator functions, `ctx.run`), under Node | **PASS**: the same greeting |
| TypeScript, npm | `@resonatehq/sdk` 0.10.4 (what `^0.10.0` in `example-fan-out-fan-in-ts` resolves to) | same | FAIL: `task.fence` answered 400, and the run returned `null` |

- PyPI now carries 0.8.1, the monorepo's version, so the example suite does not need an editable install of the fork's `impl/sdk/py`.
- The TypeScript example repositories must be run with `@resonatehq/sdk` 0.11.5 instead of their `^0.10.0`.
- They use `bun`, which is not installed on this machine. Task 10 either installs `bun` in CI or runs the examples with Node and `tsx`.

## (i) Upstream

| PR / issue | What | State (2026-09-27) |
|---|---|---|
| resonatehq/resonate#1162 | PR 0a: MySQL retryable errors by errno (fork branch `fix/mysql-retryable-errno`, `6968aa2`) | Open |
| #1163 | PR 1: TiDB support, stacked on 0a (`feat/tidb-backend`, `f8d7ef2`) | Draft |
| #1164 | PR 0c: dependency hygiene (`deps/advisories-rustls`, `c3f25b9`). It clears 9 of the 14 advisories in upstream's own graph, removes OpenSSL and pins rustls 0.23.45 | Open |
| #1165 | Issue: a public router constructor (PR 4) | Open |
| #1166 | Issue: settled-promise retention (PR 5) | Open |
| (0d, not yet opened) | `gcp-idtoken` feature on `resonate-transport-http-push` (fork branch `feat/push-gcp-idtoken-feature`, `849813f`) | Body ready for the owner (Task 1) |
| (0e, not yet opened) | SDK: reqwest defaults as the `reqwest-default` feature (`feat/sdk-rs-reqwest-default-feature`, `e5ddb8a`) | Body ready for the owner (Task 1) |

The fork's CI run for the pinned `loam/0.10.1` revision is Task 1's to record: see (j).

## (j) Task 1: the fork branch `loam/0.10.1`

`ostrium-labs/resonate` `loam/0.10.1` = **`e3606698e6e3f2502bb018bba1e618deb63f907a`**. It was built in the worktree `~/Documents/research-clones/resonate-loam`.

| Commit | What | Upstream |
|---|---|---|
| `c3f25b9` | `deps: clear RustSec advisories, drop OpenSSL, version internal path deps` | #1164 (0c), the same commit |
| `229a2f1` | `transport-http-push: gate the GCP ID token behind a gcp-idtoken feature` | 0d (branch `feat/push-gcp-idtoken-feature`, `849813f`, on `28dfd01`) |
| `c5dfe9e` | `server-mysql: classify retryable errors by MySQL error number` | #1162 (0a); cherry-pick of `6968aa2`, with the conflict in `resonate-server-mysql/Cargo.toml` resolved as 0c's versions plus 0a's `features = ["mysql"]` |
| `3ff482b` | `server-mysql: run on TiDB; xtask and CI legs for it` | #1163 (1); cherry-pick of `f8d7ef2` |
| `e360669` | `sdk-rs: reqwest without default TLS` (default-on SDK feature `reqwest-default`) | 0e (branch `feat/sdk-rs-reqwest-default-feature`, `e5ddb8a`, on `28dfd01`) |

**Fork CI.** Not run. GitHub keeps a fork's workflows disabled until they are enabled in its Actions tab. `gh api repos/ostrium-labs/resonate/actions/workflows` lists none, and `gh workflow run server-core-ci.yml --ref loam/0.10.1` answers 404. The workflows also trigger only on pushes to `main`, on PRs and on `workflow_dispatch`.

**Local verification.** It is scoped, because `cargo xtask check` covers the whole workspace, and that includes `resonate-server-scylladb`. The commands used `CARGO_BUILD_JOBS=4 CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=~/.cache/cargo-target/resonate`.

| Command (in `impl/server/core` unless noted) | Result |
|---|---|
| `cargo fmt -p <11 crates> -- --check`: `resonate-base`, `-plugin`, `-core`, `-sql`, `-timer-wheel`, `-auth`, `-server-sqlite`, `-server-mysql`, `-transport-http-poll`, `-transport-http-push`, `-gateway-http` | clean |
| `cargo clippy -p <the 11> --all-targets --all-features -- -D warnings` | clean |
| `cargo test -p <the 11> --all-features` | 150 passed, 0 failed, 10 ignored |
| `cargo test -p resonate-transport-http-push` with and without `--no-default-features`; `cargo clippy` the same, both ways | 12 passed each way; clippy clean each way |
| `impl/sdk/rs`: `cargo clippy -p resonate-sdk --all-targets [--no-default-features] -- -D warnings`; `cargo test -p resonate-sdk --no-default-features`; `cargo fmt --all --check` | clean both ways; 287 passed, 38 ignored (they need `RESONATE_URL`); clean |

The engine and port differentials, porcupine and the TiDB leg were not run locally. The fork's CI is where they run (Ruling 4).

**In Loams.** The committed `deny.toml` passes `cargo deny check` with two warnings (`unmatched-source`, `advisory-not-detected`), because no crate uses the pins until Task 2. A throwaway crate `zz-d1t1-probe` depended on all nine Resonate crates and `parquet`, and was then removed with `Cargo.lock` restored:

| Check | Result |
|---|---|
| `cargo deny check` | `advisories ok, bans ok, licenses ok, sources ok`, no warnings |
| `cargo tree -p zz-d1t1-probe -e normal -i {openssl-sys, native-tls, google-cloud-auth} --target all` | no match for any of them |
| `… -i sqlx`, `-i libsqlite3-sys`, `-i parquet`, `-i jsonwebtoken` | 0.8.6; 0.30.1 only; 58.4.0; 9.3.1 only (`resonate-auth`), since 11 went with `google-cloud-auth` |
| `cargo check -p zz-d1t1-probe` (target `~/.cache/cargo-target/loams-d1`) | 45 s, clean |
