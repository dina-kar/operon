# Contributing to Loams

Thanks for your interest in Loams. The project is young, which makes this the best time to shape it. Every kind of contribution helps: bug reports, compatibility reports, docs, tests, reviews and code.

The crates are `loams-*` and the binary is `loams`. Packages will publish as `loams` on crates.io and PyPI and under the `@loams` npm scope.

## Ways to contribute

- **Pick up an issue.** Issues labelled [`good first issue`](../../issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) are small and come with pointers and acceptance criteria. [`help wanted`](../../issues?q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22) issues are larger. Comment on an issue before you start, so that two people don't do the same work.
- **Report compatibility gaps.** Point your Qdrant or Elasticsearch client, or your LangChain or LlamaIndex app, at `loams dev` and tell us what breaks.
- **Review the design.** The architecture is written down in [`docs/design`](docs/design/README.md). Open an issue for anything that is unclear, wrong or missing.
- **Improve the docs.** Fixes to the README, crate docs and design docs are always welcome.

## Ground rules

1. **Discuss before large changes.** Anything that changes a storage format, a wire protocol, or a decision in the [decision log](docs/design/13-decision-log.md) needs an issue or a design PR first.
2. **Tests come with behavior.** New behavior comes with tests. Storage, log and metastore code also needs deterministic-simulation or fault-injection coverage (see the testing strategy in [the roadmap](docs/design/12-roadmap-testing-risks.md)).
3. **Dependency licenses.** Only Apache-2.0, MIT, BSD, ISC, Zlib, Unicode, MPL-2.0 (unmodified, as a separate crate) or compatible licenses. **No AGPL, GPL, LGPL, BSL (Business Source), SSPL, ELv2 or proprietary code.** CI enforces this with `cargo-deny` ([`deny.toml`](deny.toml)).
4. **Attribute derived code.** Code taken from other projects (for example Quickwit, Tantivy or Qdrant) keeps its original copyright header, gets a line in [NOTICE](NOTICE), and is called out in the PR description.
5. **Keep PRs small.** One focused change per PR, with a description of what changed and why.

## Open-core boundary

The Multitenant BYOC Control Plane with GitOps, generic observability and quota
enforcement are open source. Billing-grade metering, billing and commercial APIs
belong in the private `loam-platform` repository. **Integrity** is the rule:
"could a charge depend on this value, and could a tenant or an agent profit from
forging it?" If so, its producer and validator belong in the private platform.

Run `scripts/ci/no-metering.sh` before submitting changes. The guard scans tracked
files and Cargo manifests/lockfiles; only the historical boundary documents named
in [MT4 Task 8](docs/plans/2026-10-02-mt4-byoc-control-plane.md) may mention the
private protocol's identifiers. New exceptions require a boundary review. The
open engine never depends on a private platform package. See
[open-core.md](docs/open-core.md) for the full boundary.

## Development setup

### Toolchain

| Tool | Why | Install |
|---|---|---|
| Rust (rustup) | The workspace. The version is pinned in [`rust-toolchain.toml`](rust-toolchain.toml) and installs itself on the first `cargo` command, with `rustfmt` and `clippy` | [rustup.rs](https://rustup.rs) |
| `protoc` | Lance, the Qdrant gateway and Loams Live compile protobufs | `apt install protobuf-compiler`, `brew install protobuf`, `pacman -S protobuf` |
| A C/C++ toolchain | Native dependencies (zstd, lz4, SQLite) | `build-essential`, Xcode command-line tools |
| Go 1.24 (optional) | Only for the durable-execution conformance checker in `scripts/durable` | [go.dev](https://go.dev/dl/) |
| [uv](https://docs.astral.sh/uv/) (optional) | Only for the Python client suites (`qdrant-client`, `elasticsearch-py`) | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| [tiup](https://docs.pingcap.com/tidb/stable/tiup-overview) (optional) | Only for the TiKV suites: a local PD + TiKV playground | see [`scripts/tikv/playground.sh`](scripts/tikv/playground.sh) |
| Node 22 and pnpm (optional) | Only for the web console in [`web`](web/README.md) and the SDKs in [`sdks`](sdks) | [pnpm.io](https://pnpm.io/installation) |

### Build without running out of memory

The workspace pulls in DataFusion, Lance, Tantivy and Arrow, so a full build is heavy: expect 10 to 30 minutes and several GB of RAM per parallel job the first time.

- **Limit parallel jobs** on machines with less than 32 GB of RAM: `cargo build -j 4`, or set `CARGO_BUILD_JOBS=4` in your shell.
- **Build only what you touch:** `cargo test -p loams-log` builds a fraction of the workspace; `cargo test --workspace` builds all of it.
- **Use a faster linker** if you have one: `lld` or `mold` (for example `RUSTFLAGS="-C link-arg=-fuse-ld=lld"`) cuts link times for the `loams` binary a lot.
- **Share a target directory** between checkouts and worktrees with `CARGO_TARGET_DIR`, so you don't rebuild everything per worktree.
- **Toggling a feature rebuilds a lot.** Turning on `durable` rebuilds about 480 crates. Stay on one feature set while you iterate.

### Cargo features

The `loams` crate's features:

| Feature | Default | What it adds |
|---|---|---|
| `es` | yes | The Elasticsearch REST API (`--es-listen`) |
| `flight` | yes | Arrow Flight SQL (`--flight-sql-listen`) |
| `qdrant` | yes | The Qdrant REST and gRPC APIs (`--qdrant-listen`) |
| `hnsw` | yes | The qdrant-edge HNSW engine for the hot tier |
| `tikv` | no | The TiKV metastore (`--meta tikv://…`); needs a TiKV playground to test |
| `durable` | no | The embedded Resonate server (`--durable-*`) on SQLite |
| `durable-mysql` | no | Resonate's MySQL store; deprecated, kept for tests until the TiKV store lands |
| `cluster-tests` | no | Builds the multi-process cluster tests |
| `failpoints` | no | Named failpoints for the crash gate; never in release builds |

Some library crates also have a `failpoints` feature (`loams-log`, `loams-link`, `loams-collection`, `loams-meta`, `loams-hot`).

### Forked dependencies

A few dependencies come from pinned forks under [ostrium-labs](https://github.com/ostrium-labs), because Loams needs patches that upstream has not released yet: [resonate](https://github.com/ostrium-labs/resonate) (the embedded durable-execution server), [client-rust](https://github.com/ostrium-labs/client-rust) (`tikv-client`), [neon](https://github.com/ostrium-labs/neon) and [sqlx](https://github.com/ostrium-labs/sqlx). Fixes to those go to the fork first and upstream where possible; bumping a pin is a PR of its own that updates `Cargo.toml`, `deny.toml` and [NOTICE](NOTICE) together.

## Running the checks

These are the checks CI runs on every pull request ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)). Run the ones for the crates you changed before you push:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked

# Scoped versions are much faster:
cargo clippy -p loams-log --all-targets -- -D warnings
cargo test -p loams-log

# Fault injection and the crash gate
cargo test -p loams-log --features failpoints
cargo test -p loams --features failpoints --test crash

# Multi-process cluster tests (one at a time)
cargo test -p loams --features cluster-tests --test cluster -- --test-threads=1

# Durable execution
cargo test -p loams-durable
cargo test -p loams --features durable --test durable --bin loams

# TiKV suites: start a playground first
scripts/tikv/playground.sh start
LOAMS_TEST_PD=127.0.0.1:19379 cargo test -p loams --features tikv --test meta_tikv
scripts/tikv/playground.sh stop

# License, advisory and source policy (install with `cargo install cargo-deny`)
cargo deny check
```

The client conformance suites live in [`crates/loams-qdrant/tests/python`](crates/loams-qdrant/tests/python) and [`conformance/es-client`](conformance/es-client). The web console has its own checks (`pnpm lint`, `pnpm typecheck`, `pnpm build`); see [`web/README.md`](web/README.md).

### What CI runs when

- **Always:** fmt, clippy and the workspace tests; the crash gate; the cluster tests; the Qdrant and Elasticsearch client suites; the simulation seed sweep; `cargo-deny`; the web console; and the Loams Live protos.
- **On pull requests, only when matching paths change:** the TiKV suites (TiKV crates, Loams Live, the metastore wiring, `deploy/tikv`, `scripts/tikv`, `proto/loams`) and the durable-execution suite (`loams-durable`, the server wiring, `Cargo.lock`, `scripts/durable`, or the Resonate pin). Any change to `ci.yml` runs both, and every push to `main` runs both.
- **Nightly:** the long simulation sweep, the tail-merge property test, the TiKV crash gate and more.

## Pull requests

1. **Fork and branch** from `dev`. Open your PR against `dev`; `main` is the release branch. Name the branch after the change, for example `log-retention-doc`.
2. **Keep it small.** A PR should do one thing. Split refactors from behavior changes, and stack PRs when a change is large.
3. **Fill in the template.** Say what changed, why, and how you tested it.
4. **Review.** [CodeRabbit](https://coderabbit.ai) reviews every PR automatically, and a maintainer reviews after it. Address or answer each comment; it is fine to disagree with a bot comment and say why. Small follow-ups can go in a follow-up PR if the reviewer agrees.
5. **CI must be green** before merge. If a failure looks unrelated to your change, say so in the PR.
6. **Merge.** Committers and maintainers merge into `dev` with a merge commit once the review is done and CI is green. Maintainers merge `dev` into `main` for releases. The contributor roles are described in [GOVERNANCE.md](GOVERNANCE.md), and the current maintainers are listed in [MAINTAINERS.md](MAINTAINERS.md).

## Commit messages

Use `<area>: <summary>` in the imperative mood, with a body that explains *why* when it is not obvious:

```
log: bound the segmenter's swap deadline
docs: clarify the express WAL quorum
qdrant: accept named vectors in query_points
```

The area is a crate name without the `loams-` prefix (`log`, `meta`, `query`, `qdrant`, `es`, `durable`, `live`, …), or `docs`, `ci`, `web` or `deps`.

## Developer Certificate of Origin (DCO)

Loams uses the [Developer Certificate of Origin](https://developercertificate.org/) (DCO). There is no contributor license agreement (CLA) and none will be added. A CI check on every pull request fails if a commit lacks a matching sign-off. Sign off every commit:

```sh
git commit -s -m "log: add a WAL object encoder"
```

This adds a `Signed-off-by: Your Name <you@example.com>` trailer, certifying that you wrote the change or otherwise have the right to submit it under the project's license. To sign off commits you already made, run `git rebase --signoff dev`.

## Code of Conduct

Everyone who takes part in the project agrees to the [Code of Conduct](CODE_OF_CONDUCT.md). Report security issues privately, as described in [SECURITY.md](SECURITY.md).
