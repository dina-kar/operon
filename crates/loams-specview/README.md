# loams-specview

A dev tool that shows the Loams router's distributed-system checks in a browser while they run: the TLA+ specs under TLC, with step-through counterexample traces, and the router's Rust tests. Design: [§31](../../docs/design/31-loam-router-and-verification.md) §11.4. It is not published and nothing else depends on it.

## Run it

You need Java 21+ and Python 3.11+ (the spec checker `scripts/spec/check.sh` downloads the pinned TLC and Apalache on first use), the `wasm32-unknown-unknown` target and [`trunk`](https://trunkrs.dev) to build the frontend once. `cargo-nextest` is optional.

```sh
rustup target add wasm32-unknown-unknown
cargo install trunk --locked            # or download a release binary from github.com/trunk-rs/trunk

cd crates/loams-specview
trunk build                             # writes dist/ (use `trunk watch` while editing the UI)
cargo run -p loams-specview -- serve    # then open http://127.0.0.1:7740
```

`serve` binds `127.0.0.1` only. It starts a run at launch; the page's **Run again** button starts another. Useful flags:

| Flag | Effect |
|---|---|
| `--port 7740` | Port on 127.0.0.1. |
| `--spec ShardMap` (repeatable) | Only these specs. |
| `--nightly` | The nightly variant set instead of the PR set. |
| `--apalache` | Also run the Apalache checks of `specs.toml` (no traces). |
| `--no-spec`, `--no-rust` | Skip the TLA+ part or the Rust tests. |
| `--rust-package loams-sqlrouter` (repeatable) | Which crates to test. Default: the router crate once `crates/loams-sqlrouter` exists, else none. |
| `--record run.jsonl` | Save the run's events, one JSON object per line. |
| `--no-autorun` | Wait for the button. |
| `--dist DIR`, `--repo DIR` | Where the built frontend and the repository are (found automatically). |

Replay a saved run (the page behaves as if it were live; **Run again** replays it):

```sh
cargo run -p loams-specview -- replay crates/loams-specview/examples/sample-run.jsonl --delay-ms 40
```

`examples/sample-run.jsonl` is a real run of every PR variant plus this crate's own tests. A deep link opens a trace directly: `http://127.0.0.1:7740/?variant=NoFence&step=8` (the first trace whose variant file name contains `NoFence`, or whose spec is named so, at state 8).

### Why trunk and not cargo-leptos

The tool needs no server-side rendering: the page is a client of one SSE stream. Leptos in client-side-rendered mode plus an Axum server that serves `dist/` is the smaller setup, and `trunk` is a single prebuilt binary. The frontend is behind the `web` cargo feature, so `cargo build`, `cargo test` and `cargo clippy --workspace` never compile Leptos or wasm; only `trunk build` does.

### Rust test output: nextest first, libtest as the fallback

If `cargo nextest --version` works, the run uses `cargo nextest run --no-fail-fast --message-format libtest-json` with `NEXTEST_EXPERIMENTAL_LIBTEST_JSON=1`: per-test start and finish events, timings, and captured output of failures. Otherwise it runs `cargo test --no-fail-fast` and parses libtest's human output (`test x ... ok`, the `---- x stdout ----` blocks, `test result:` lines): no start events or timings. Install nextest with `cargo install cargo-nextest --locked` or a release binary from get.nexte.st.

## What you see

*Header.* A status pill (waiting, running, all checks passed in N s, failures), connection state, **Run again** and a **Theme** toggle (light and dark follow the system until you press it).

*Suite panel (left).* One row per spec variant: a dot (amber and pulsing while TLC runs, green, red), spec and variant file, `expected violation:SingleWriter`, then states and seconds, or "N distinct, M queued" from TLC's progress lines while it runs. Variants with a counterexample carry a `trace` tag; click any row to open it. Below, one row per Rust test with its time and, for failures, the captured output expanded.

*Trace player (right).* For a counterexample: first, back, **Play**/**Pause**, forward and last buttons, a slider, a clickable timeline of `N Action` chips, the action name of the current state and the list of variables it changed. The last state shows `violates <Invariant>` and, for a lasso, `then back to state N`. Changed things are outlined in yellow in every view. Passing variants show their outcome and state counts instead.

*ShardMap view.* A grid of keys by generations whose cells are owner shards (coloured chips; a cell outlined in blue is a key that moved to a new owner in that generation; the column heads are tagged `record` and `ConfigMap` for the current record generation and the generation in the ConfigMap); router instances with their applied generation (dashed amber when behind the record); shards with `open` or `fenced` and their write sets, each write as `w3 k1 via i2`, with a green check when acknowledged to a client and `stale` when the shard lagged behind the key's history.

*ReshardCutover view.* The saga phases as a timeline (done, current, to come; the rollback phases on a second row); a diagram with each instance as a node (running, paused, unreachable) and an edge to the Src or Dst store it routes to (dashed when paused); the stores show fenced or unfenced and whether they can take writes, and when both can the edges turn red with "SingleWriterRange is violated"; below, each store's contents with acknowledged writes ticked, and the saga's up or crashed state.

*Other specs.* A table of variables and values (TLA+-style), changed rows highlighted. Every spec view also has an "All variables" section.

## Event stream

`GET /api/events` is Server-Sent Events: the current run from its first event, then live ones, each `data:` a JSON `Event` (`{"channel":"run","event":{"type":"State",...}}`; channels `run`, `test`, `spec`, `done`; see `src/event.rs`). An SSE event named `reset` means a new run replaced the history. `POST /api/run` starts a run (409 while one is running); `GET /api/status` says whether one is.

The `spec` channel carries `SpecEvent`, a placeholder shaped like §31 §7.1's `SpecEvent { spec, action, fields }` for the RT1 simulator. Nothing emits it yet and the views ignore it.

## Tests

```sh
cargo test -p loams-specview                      # parsers (golden TLC and nextest output), event round trips, run model, view models, replay over SSE
cargo test -p loams-specview -- --include-ignored # also runs TLC on the Selftest spec and checks the SSE stream: RunStarted ... Result ... Done
```

Fixtures in `tests/fixtures/` are real output: `tlc_unsafe.txt` (ShardMap UnsafeConfigMap), `tlc_nofence.txt` (ReshardCutover NoFence), `tlc_small.txt` (a passing run with progress lines), `nextest_sample.jsonl`, `libtest_sample.txt`. CI job `specview` (path-filtered on this crate, `scripts/spec/**` and `spec/tla/**`) runs clippy, all tests including the live one, and `trunk build --release`.

## Limits

* Spec views read variable names from the specs (`record`, `history`, `routesTo`, ...); a renamed variable drops that spec to the generic table.
* TLC prints progress about once a minute, so the progress line of a long run updates slowly.
* The runs are sequential and one run at a time; a second browser tab sees the same run.
* `--apalache` results have no traces.
