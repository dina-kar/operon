# Building Loams on macOS

For building from source on **macOS**, because Loams ships **no macOS
artifacts** and does not intend to in the near term. Read
[the short version](#the-short-version) first; it is the whole answer if you
just want a binary.

The Windows page is [`windows.md`](windows.md). What is signed and what is not,
and why, is [`docs/release/signing.md`](../release/signing.md).

## How to read the markers

Every command and claim on this page carries one of three markers, and the
marker tells you how much it is worth:

| Marker | Meaning |
|---|---|
| **[verified]** | The command appears in this repository's own CI (`.github/workflows/ci.yml`, `release-crates.yml`) or in the top-level `README.md`, and the flags were checked against the manifests in this tree. CI runs it on Linux. |
| **[verified, platform-pending]** | The same command, correct for the pinned toolchain and this repository's layout — but **it has never been executed on macOS**, because no Mac took part in writing this page. |
| **[unverified]** | Reasoned from the pinned toolchain, a vendored crate's source, or Apple's documented behaviour. Never executed anywhere in this repository. |

Nothing on this page is presented as tested on macOS unless it is marked
**[verified]**, and the only commands that carry that marker are ones CI already
runs. This is deliberate: a build-from-source page that quietly invents flags is
worse than one that admits which parts are untested.

## The short version

**[verified, platform-pending]** — in a normal terminal, with the three
prerequisites below installed:

```sh
git clone https://github.com/ostrium-labs/loams.git
cd loams
cargo build --release -p loams --locked
./target/release/loams dev
```

That gives you a local server on `127.0.0.1:8080` with the Qdrant and
Elasticsearch surfaces on their default ports. `loams dev --help` lists every
listener and the flag that moves or disables it. **[verified]** (the flags and
defaults are the table in `README.md`).

The first build compiles DataFusion, Lance and Tantivy and takes a long time on
a cold cache. **[verified]** — `README.md` says the same thing and recommends
`-j 4` on machines with less than 32 GB of RAM:

```sh
cargo build --release -p loams --locked -j 4
```

## What you get, and what you do not

Loams today is a **server binary** and a **TypeScript web console**. That is
all. Concretely:

| Thing | In this repository? |
|---|---|
| The `loams` server binary (`crates/loams`) | **Yes.** This is what the instructions below build. |
| The web console and `@loams/ui` (`web/`) | **Yes.** Node/pnpm; see [the console](#the-web-console). |
| A Tauri desktop app | **No.** There is no Tauri crate in `Cargo.lock` and no Tauri dependency in `web/pnpm-lock.yaml`. The desktop app is a plan ([`docs/plans/2026-10-01-ap1-desktop-tauri.md`](../plans/2026-10-01-ap1-desktop-tauri.md)), not code. |
| Android or iOS apps | **No.** No `android/`, no `ios/`, no Swift Package, no Xcode project in the tree. Those live in a separate repository. |
| A `.app`, a `.dmg`, or an `.xcarchive` | **No.** Nothing here produces one. |
| A binary release pipeline | **No.** D292's cargo-dist pipeline is still planned, so **there are no Loams macOS binaries to download at all** — signed or unsigned. Building from source is currently the *only* way to get one. |

Two things people expect that are **not** needed for this:

- **A full Xcode install is not required.** The Command Line Tools are enough,
  because there is no Apple-platform project in this tree to open. See
  [prerequisite 1](#1-xcode-command-line-tools--unverified-install-verified-requirement).
- **WebView2 is a Windows concept and does not exist here.** It is Tauri's
  embedded browser engine on Windows. On macOS Tauri uses WKWebView, which is
  part of the OS. Neither applies, because there is no Tauri app. **[unverified]**
  — stated from the absence of any Tauri dependency in `Cargo.lock`,
  `web/package.json` and `web/pnpm-lock.yaml`, which was checked directly.

## Prerequisites

### 1. Xcode Command Line Tools **[unverified install, verified requirement]**

```sh
xcode-select --install
```

**[unverified]** — the command is Apple's documented installer for the Command
Line Tools. It is what the build needs: `clang` and the macOS SDK, used by the
C in `ring`, `aws-lc-sys`, `zstd-sys` and `lz4-sys`.

The full Xcode application is **not** needed, and this page will not tell you to
download 10 GB to compile a server binary. Xcode itself becomes necessary only
when this repository grows an Apple-platform project — the iOS app, or a Tauri
macOS build — and neither exists yet.

Confirm:

```sh
xcode-select -p
# expect: /Library/Developer/CommandLineTools
#   (or .../Developer.xctoolchain if full Xcode is installed and selected —
#    either works for this build)
clang --version
```

**[verified]** that the Command Line Tools are sufficient, by reading
`aws-lc-sys` 0.45.0's build script: its `get_builder` prefers a system
installation, then the CMake builder for FIPS or `NO_ASM`, then a `cc` builder
when bindgen is not required. `aws-lc-sys`'s default features are
`["all-bindings"]` and **not** `bindgen`, and pregenerated bindings ship with
the crate, so the `cc` builder is selected and `clang` is the only requirement.

### 2. Rust **[unverified install, verified toolchain]**

Install [rustup](https://rustup.rs), which on macOS is the same installer as
everywhere: <https://rustup.rs>. Do not use `brew install rust`; Homebrew's Rust
does not read `rust-toolchain.toml` and will not install the pinned toolchain.

You do not need to install a Rust version by hand.
[`rust-toolchain.toml`](../../rust-toolchain.toml) pins it:

```toml
[toolchain]
channel = "1.97.1"
components = ["rustfmt", "clippy"]
```

rustup reads that file on the first `cargo` invocation in the repository and
installs **1.97.1** with `rustfmt` and `clippy` automatically. **[verified]** —
this is what `README.md` relies on ("the toolchain pinned in
`rust-toolchain.toml` installs on first build") and what CI gets from
`actions/checkout`.

Check it:

```sh
rustc -V        # expect rustc 1.97.1
rustc -vV | grep host   # aarch64-apple-darwin, or x86_64-apple-darwin
```

No `rustup target add` is needed. A host build uses the host target, and this
page only builds for the host.

### 3. protoc **[verified requirement, unverified install]**

Six `build.rs` scripts in this workspace generate Rust code from `proto/`, and
every one of them shells out to the **`protoc` binary on `PATH`**. **[verified]**
— read directly:

| Crate | Generator |
|---|---|
| `loams-proto` | `connectrpc_build` (`loams/options`, `loams/errors`, `loams/instance`) |
| `loams-live-proto` | `connectrpc_build` (`loams/live/v1`) |
| `loams-apps-mock` | `connectrpc_build` (the app packages) |
| `loams-collection` | `prost_build` |
| `loams-stream-grpc` | `tonic_prost_build` |
| `loams-qdrant` | `tonic_prost_build` |

Each one panics with *"is protoc installed?"* when it is missing. CI installs
it with `sudo apt-get install -y protobuf-compiler libprotobuf-dev`, which is
Linux-specific and not usable here. **[verified]** — that line is in `ci.yml`
and `release-crates.yml`.

There is **no vendored `protoc`**: `protoc-bin-vendored` is not in `Cargo.lock`,
and no build script sets `PROTOC`. `buf` is not a substitute for `protoc` here,
because these are `prost`/`tonic` build scripts and they only look for `protoc`
or the `PROTOC` environment variable.

```sh
brew install protobuf
```

**[verified]** — `README.md` names `brew install protobuf` as one of the three
supported ways to get `protoc`. That needs [Homebrew](https://brew.sh)
**[unverified]**, which is Apple's-ecosystem-default package manager but not
preinstalled. `brew install protobuf` builds `protoc` **from source**, so expect
it to take a while the first time; a bottle exists but is not used if your
Homebrew or macOS version is older than the bottle's floor.

Confirm:

```sh
protoc --version
```

**[verified]** that a wide range of `protoc` versions build this tree: the Linux
machine that produced these pages has **36.1**, and `ci.yml` gets whatever
Ubuntu 24.04 ships (a 3.21-series release). Both build it.

If `protoc` is somewhere unusual, point the build at it:

```sh
PROTOC=/opt/homebrew/bin/protoc cargo build --release -p loams --locked
```

**[unverified]** — that `PROTOC` is honoured comes from reading `prost-build`
and `tonic-prost-build`, not from running it here.

### Not needed for the CLI build

| Prerequisite | Needed for |
|---|---|
| CMake | Only if `aws-lc-sys` falls back to its CMake builder, and for the optional `mysql-wire` feature. See [CMake](#cmake-is-not-required-by-default). |
| LLVM / libclang | Only for the optional `mysql-wire` feature (`mysql_common` → `bindgen`). **[verified]** from `Cargo.lock`. |
| Docker | Only for the Elasticsearch client conformance suite, which is a separate CI job. |
| `cargo-nextest` | Optional. CI uses it to partition the test suite; plain `cargo test` is equivalent for correctness. |

### CMake is not required by default **[verified by reading the build script]**

`aws-lc-sys` has two builders, chosen in this order: a system library if one is
detected, then `cmake` for FIPS or `NO_ASM`, then a `cc` builder when bindgen is
not needed. `aws-lc-sys`'s default features include `all-bindings` and **not**
`bindgen`, and pregenerated bindings ship with the crate, so `is_bindgen_required()`
is false and the **`cc` builder is used** — which needs only `clang`.

Install CMake if a build tells you it is missing, or if you enable `mysql-wire`
(`mysql_common` depends on both `cmake` and `bindgen`).

## Clone and build

**[verified, platform-pending]**

```sh
git clone https://github.com/ostrium-labs/loams.git
cd loams

# debug build, at target/debug/loams
cargo build -p loams --locked

# release build, at target/release/loams — what you probably want
cargo build --release -p loams --locked
```

`--locked` matters: it makes cargo fail rather than silently update
`Cargo.lock`. Every gate in the issue templates uses it, and CI does too.
**[verified]**

Useful variants:

```sh
# The exact build CI runs before the cluster and conformance suites.
cargo build -p loams --locked

# A build with an optional listener enabled. `durable` turns on the embedded
# Resonate server (127.0.0.1:8001); it rebuilds roughly 480 crates, so expect a
# long first build. The `mysql-wire` and `pgwire` features add a datafusion build.
cargo build --release -p loams --locked --features durable
```

The feature list is read from `crates/loams/Cargo.toml` **[verified]**;
`default = ["es", "flight", "hnsw", "qdrant"]`, and `durable`, `durable-mysql`,
`durable-tikv`, `mysql-wire`, `pgwire`, `stream-grpc`, `tikv`,
`cluster-tests` and `failpoints` are all off by default.

Run it:

```sh
./target/release/loams dev
```

### You built one architecture, not a universal binary

**[unverified]** — this follows from how Rust works and is stated here because
it surprises people who expect a Mac download to "just work on any Mac".

`cargo build` produces a Mach-O binary for **your** host: `arm64` on Apple
Silicon, `x86_64` on Intel. That is what you want for local use. A single
binary containing both is a *universal* binary and needs two builds plus a
`lipo`:

```sh
cargo build --release -p loams --locked --target aarch64-apple-darwin
cargo build --release -p loams --locked --target x86_64-apple-darwin
lipo -create -output loams-universal \
  target/aarch64-apple-darwin/release/loams \
  target/x86_64-apple-darwin/release/loams
file loams-universal
```

Nothing in this repository does this, and it is not on any roadmap for the
binary release path. It is here so the limitation is documented rather than
discovered later. **Unverified:** the `lipo` invocation has not been run here,
and a cross-built `x86_64` binary on Apple Silicon needs Rosetta 2 to execute.

## Test

**[verified, platform-pending]** — these are the commands CI runs, minus the
runner plumbing:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --doc --locked
cargo test --workspace --locked
```

Two notes on the fourth one.

**`cargo nextest` is what CI uses**, partitioned in two:

```sh
cargo nextest run --workspace --locked --partition hash:1/2
cargo nextest run --workspace --locked --partition hash:2/2
```

**[verified]** — `ci.yml`'s `rust-tests` job. `cargo nextest` is not a rustup
component; install it with
[`cargo-nextest`'s own instructions](https://nexte.st/book/installation.html)
**[unverified]**. It is not required: `cargo nextest run --workspace --locked`
and `cargo test --workspace --locked` run the same tests. The difference is that
nextest does not run doctests, which is why CI keeps a separate
`cargo test --workspace --doc --locked` step. **[verified]**

**Not every CI job is portable to macOS, and that is expected.** The
authoritative list is `ci.yml`. The jobs that need a Linux host or a service
are:

| CI job | Needs |
|---|---|
| `durable-tidb` | `tiup` and TiDB v8.5.8, driven by `scripts/durable/tidb.sh` (bash), plus the Go toolchain |
| `qdrant-client` | `crates/loams-qdrant/tests/python/run.sh` (bash) and `uv` |
| `es-client` | Docker running Elasticsearch 8.19.22, and `conformance/es-client/run.sh` (bash) |
| `specview` | Java 21, `trunk`, and the `wasm32-unknown-unknown` Rust target |
| `dapr-edge` | `kubectl` with kustomize |
| `tla` | Java 21 and the pinned TLA+ tools |
| `lean` | `elan` and `lake` |
| `tikv`, `tikv-nightly` | Linux-specific TiKV behaviour, driven by `scripts/tikv/playground.sh` |

**[verified]** — each of these is a separate job in `ci.yml` with its own setup
step; none of them is part of `cargo test --workspace`. The default-feature test
suite has no service dependency, because every service-backed suite is behind a
Cargo feature or lives in a non-Rust script.

## The web console

The console is a separate pnpm workspace under `web/`. It is a Node project, so
none of the Rust prerequisites apply to it.

**[verified, platform-pending]**

```sh
cd web
corepack enable
pnpm install --frozen-lockfile
pnpm lint        # biome check
pnpm typecheck   # tsc across every workspace package
pnpm test
pnpm build
```

**[verified]** — every one of those five scripts is a script in `web/package.json`,
and `ci.yml`'s `web` job runs exactly this sequence. Two details that come from
the same files:

- **Node 22 or newer.** `web/package.json` sets `"engines": { "node": ">=22" }`
  and `ci.yml` uses `actions/setup-node` with `node-version: 22`. **[verified]**
- **pnpm 11.27.1 exactly**, taken from the `"packageManager": "pnpm@11.27.1"`
  field, so `corepack enable` is what makes the version right. Do not
  `npm install -g pnpm`; that ignores the pin. **[verified]**

Install Node from <https://nodejs.org> **[unverified]** (or `brew install node`
**[unverified]**). `corepack` ships with Node, so no separate pnpm install is
needed.

There is a second, unrelated pnpm workspace at `sdks/live-typescript`. It holds
generated proto stubs for `@loams/live` and is not needed to build or run the
console. **[verified]**

## Gotchas

### `protoc` is the wrong architecture

On Apple Silicon, a `protoc` that answers `Bad CPU type in executable` is the
Intel build. Check what you have:

```sh
file "$(which -a protoc | head -1)"
uname -m
```

**[unverified]** — this is a diagnosis, not something exercised here. The usual
causes are a `protoc` copied from an Intel Homebrew prefix
(`/usr/local/bin/protoc` rather than `/opt/homebrew/bin/protoc`), or one dropped
in by hand from a `protoc-*-osx-x86_64.zip`. On Apple Silicon Homebrew installs
to `/opt/homebrew`; on Intel it installs to `/usr/local`. **[unverified]**

`ci.yml` sidesteps all of this by installing `protobuf-compiler` from apt.

### `xcode-select -p` may point at a full Xcode

Both are fine for this build. Apple's toolchain selection is a system-wide
setting, so if `xcode-select -p` names a full
`Xcode.app/Contents/Developer`, that is what `clang` will be, and it works. You
do not need to switch it back to the Command Line Tools. **[unverified]**

### APFS is case-insensitive by default, and this repository does not care

The default macOS volume is case-insensitive. Nothing here depends on
case-sensitive paths: the proto tree under `proto/loams/` is entirely lowercase,
and no build script includes a path differing from another only by case.
**[verified]** — the generator file lists in `loams-proto/build.rs`,
`loams-live-proto/build.rs` and `loams-apps-mock/build.rs` were read, and every
component is lowercase. This is worth stating because a case-collision is the
classic way a Linux-built repository breaks on macOS, and it does not apply
here.

### `cargo build` output is not quarantined, so Gatekeeper stays out of the way

This is the single most useful thing to know on this page. Gatekeeper's
com *does-not-run-this* behaviour is driven by a **quarantine attribute**
(`com.apple.quarantine`), which is attached to files **downloaded** by
`Safari`, `curl`, Mail, or most other clients. It is not attached to a file a
compiler produced.

So a binary you built yourself runs, with no prompt and no
`xattr -dr com.apple.quarantine`. **[unverified]** — from Apple's documented
behaviour rather than from a run here. If you have *already* hit a Gatekeeper
prompt on a `cargo build` product, something in your path added the attribute
(a download-and-extract step, an archive manager, or a sync client); clear it:

```sh
xattr -dr com.apple.quarantine ./target/release/loams
```

### Building in `/tmp` or a synced folder

Building under iCloud Drive, Dropbox, or a `Documents` folder that is
synchronised will be slow and can produce corrupted intermediates when the sync
agent rewrites files mid-build. Keep the checkout on the local volume. Also note
that `/tmp` is a RAM-backed tmpfs on macOS, so a 10 GB `target/` there will
disappear on reboot and can exhaust memory. **[unverified]** — advice.

### SIGTERM is handled, and unlike on Windows you can send it

`crates/loams/src/main.rs:shutdown_signal` registers `SIGTERM` alongside
`SIGINT` under `#[cfg(unix)]`. **[verified]** — read from the source. So
`kill -TERM` stops the server cleanly on macOS, and Ctrl-C in the terminal does
too.

### Disk space accounting

`crates/loams-hot/src/tier.rs` computes a directory's on-disk size from
`MetadataExt::blocks()` under `#[cfg(unix)]`, with a `#[cfg(not(unix))]`
fallback. **[verified]** — read from the source. On macOS the `#[cfg(unix)]`
branch is the one that compiles, so hot-tier capacity reporting uses allocated
block counts. Nothing else in the crate tree is platform-conditional: there is
not a single `cfg(target_os = "linux")` anywhere under `crates/`, which is why
this page is possible at all. **[verified]**

## macOS artifacts are unsigned

State this plainly to anyone who downloads something you built and runs it
elsewhere: **it has no code signature, no Developer ID, and no notarisation
ticket.**

On macOS that means **Gatekeeper**. Depending on how the file arrived, the first
launch shows one of:

- *"Apple cannot check it for malicious software"* — Gatekeeper has no
  notarisation ticket to look up and cannot reach Apple's notary service, or the
  file has no signature at all.
- *"The developer cannot be verified"* — there is a signature, but not one that
  chains to an Apple-issued Developer ID.

Neither means anything is wrong with the binary. There is no way to tell the
user "ignore this specific prompt", because there is no specific prompt — the
absence of a signature is the whole story. What the user does:

- Right-click (or Control-click) the file or app → **Open** → confirm in the
  dialog; this is remembered for that file. **or**
- Clear the quarantine attribute, which runs it without the prompt:

  ```sh
  xattr -dr com.apple.quarantine ./loams
  ```

`spctl --assess` will not accept it either, and there is no local change that
makes it: `spctl` consults Apple's records, and only an Apple-issued identity
appears there.

**A binary you built yourself does not trigger any of this** — see
[the quarantine note above](#cargo-build-output-is-not-quarantined-so-gatekeeper-stays-out-of-the-way).

There are currently no Loams macOS binaries to distribute — see
[what you do not get](#what-you-get-and-what-you-do-not) — so this is about the
day a release pipeline exists, and about copies you hand to someone else. The
decision to leave macOS unsigned is recorded as
[D620](../design/13-decision-log.md); `docs/release/signing.md` explains why: the
maintainers hold no Apple Developer account, and issue #264's original plan —
sign with a personal Apple ID in CI — was replaced by this page rather than
implemented.

### Signing your own build locally

Optional, and unrelated to the SignPath pipeline, which deliberately does not
touch macOS **[verified]** — `scripts/ci/signpath-artifacts.py` rejects `.dmg`,
`.pkg` and `.app` by name.

**Ad-hoc signing, free, no account.** A bare Mach-O executable can carry an
ad-hoc signature. It identifies nobody and is accepted by nothing but the
kernel, but it makes the binary self-consistent:

```sh
codesign --force --sign - ./target/release/loams
codesign --verify --verbose=2 ./target/release/loams
```

**[unverified]** — this is Apple's documented ad-hoc signing form; it was not
run here. Note the consequence honestly: **an ad-hoc signature does not satisfy
Gatekeeper.** It changes the message to *"the developer cannot be verified"*, and
the user still has to right-click → Open or clear the quarantine attribute. Use
it to keep a build tidy, not to get past Gatekeeper.

**Developer ID and notarisation need a paid account.** Both require membership
in the Apple Developer Program ($99/year, an organisation or an individual), so
neither is something this project can offer you. If you have one and want to sign
your own build — for example to send to a colleague on another Mac:

```sh
# Requires a "Developer ID Application" certificate in the login keychain,
# imported from a .p12 created in Xcode > Settings > Accounts > Manage Certificates.
codesign --force --options runtime --timestamp --sign "Developer ID Application: YOUR NAME (TEAMID)" \
  ./target/release/loams

# Requires an app-specific password or a stored notarytool profile.
xcrun notarytool submit ./target/release/loams \
  --apple-id you@example.com --team-id TEAMID --password @keychain-profile --wait
xcrun stapler staple ./target/release/loams
```

**[unverified]** — read from Apple's `codesign` and `notarytool` documentation,
not run here, and **never run by this repository's CI**: the owner's ruling of
2026-10-03 is that macOS ships unsigned and documented, with no Apple signing
workflow. There are no Apple secrets in
`ostrium-labs/loams`, and `gh secret list -R ostrium-labs/loams` is empty.

Note also that the *release* form of this would be a `.dmg` or a signed `.app`,
and this repository produces neither — it produces one Mach-O executable. If a
desktop app lands (AP1), that is the point at which Developer ID and
notarisation become a real question rather than a hypothetical one.

## See also

- [`windows.md`](windows.md) — the same page for Windows.
- [`docs/release/signing.md`](../release/signing.md) — what is signed, what is
  not, and the human steps that are still outstanding.
- [`CONTRIBUTING.md`](../../CONTRIBUTING.md) and
  [`GOVERNANCE.md`](../../GOVERNANCE.md) — the gates every change has to pass.
- [`README.md`](../../README.md) — what Loams is, and the listener table.