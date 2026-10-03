# Building Loams on Windows

For building from source on **Windows**, because Loams ships **no Windows
artifacts** and does not intend to in the near term. Read
[the short version](#the-short-version) first; it is the whole answer if you
just want a binary.

The macOS page is [`macos.md`](macos.md). What is signed and what is not, and
why, is [`docs/release/signing.md`](../release/signing.md).

## How to read the markers

Every command and claim on this page carries one of three markers, and the
marker tells you how much it is worth:

| Marker | Meaning |
|---|---|
| **[verified]** | The command appears in this repository's own CI (`.github/workflows/ci.yml`, `release-crates.yml`) or in the top-level `README.md`, and the flags were checked against the manifests in this tree. CI runs it on Linux. |
| **[verified, platform-pending]** | The same command, correct for the pinned toolchain and this repository's layout — but **it has never been executed on Windows**, because no Windows machine took part in writing this page. |
| **[unverified]** | Reasoned from the pinned toolchain, a vendored crate's source, or a vendor's documented behaviour. Never executed anywhere in this repository. |

Nothing on this page is presented as tested on Windows unless it is marked
**[verified]**, and the only commands that carry that marker are ones CI already
runs. This is deliberate: a build-from-source page that quietly invents flags is
worse than one that admits which parts are untested.

## The short version

**[verified, platform-pending]** — in PowerShell, from an ordinary (not
Administrator) prompt, with the four prerequisites below installed:

```powershell
git clone https://github.com/ostrium-labs/loams.git
cd loams
cargo build --release -p loams --locked
.\target\release\loams.exe dev
```

That gives you a local server on `127.0.0.1:8080` with the Qdrant and
Elasticsearch surfaces on their default ports. `loams dev --help` lists every
listener and the flag that moves or disables it. **[verified]** (the flags and
defaults are the table in `README.md`).

The first build compiles DataFusion, Lance and Tantivy and takes a long time on
a cold cache. **[verified]** — `README.md` says the same thing and recommends
`-j 4` on machines with less than 32 GB of RAM:

```powershell
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
| Android or iOS apps | **No.** No `android/`, no `ios/`, no Gradle or Xcode project in the tree. Those live in a separate repository. |
| A binary release pipeline | **No.** D292's cargo-dist pipeline is still planned, so **there are no Loams Windows binaries to download at all** — signed or unsigned. Building from source is currently the *only* way to get one. |

Two of the prerequisites people expect for a desktop app are therefore not
needed here, and this page will not pretend otherwise:

- **WebView2 is not a prerequisite.** WebView2 is the embedded browser engine
  Tauri uses to render a web frontend. There is no Tauri app in this tree, so
  there is nothing that loads it. If you are following a Tauri getting-started
  guide and it tells you to install the WebView2 runtime or the Evergreen
  bootstrapper, that guide is for a different project. **[unverified]** — stated
  from the absence of any Tauri dependency in `Cargo.lock`, `web/package.json`
  and `web/pnpm-lock.yaml`, which was checked directly.
- **An installer is not produced.** Nothing here emits `.msi`, `.msix` or a
  setup `.exe`. You get `target\release\loams.exe` and nothing else.

## Prerequisites

### 1. Rust, on the MSVC toolchain **[unverified install, verified toolchain]**

Install [rustup](https://rustup.rs) with the default `x86_64-pc-windows-msvc`
host. Do not switch to the `*-pc-windows-gnu` host: this workspace links against
the MSVC C runtime through several C dependencies, and nothing in this
repository has ever been built or tested against a GNU host.

You do not need to install a Rust version by hand.
[`rust-toolchain.toml`](../../rust-toolchain.toml) pins the toolchain:

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

```powershell
rustc -V     # expect rustc 1.97.1
cargo -V
```

No `rustup target add` is needed. A host build uses the host target, and this
page only builds for the host.

### 2. Visual Studio Build Tools **[unverified install, verified requirement]**

The MSVC target needs `cl.exe` (to compile the C in `ring`, `aws-lc-sys`,
`zstd-sys` and `lz4-sys`) and `link.exe` plus the Windows SDK (to link).

Install **Visual Studio Build Tools** and select the **Desktop development with
C++** workload. From
<https://visualstudio.microsoft.com/visual-cpp-build-tools/>:

| Component | Why |
|---|---|
| MSVC v143 x64/x86 build tools | `cl.exe`, `link.exe` |
| Windows 10 or 11 SDK | the import libraries (`kernel32.lib` and friends) |
| C++ CMake tools for Windows | **not needed** for the default build; see [CMake](#cmake-is-not-required-by-default). |

**[unverified]** — a `winget` one-liner that selects the workload:

```powershell
winget install --id Microsoft.VisualStudio.2022.BuildTools -e --override `
  "--quiet --wait --passive --add Microsoft.VisualStudio.Workload.VCTools --includeRecommended"
```

The `--override` string is the Visual Studio installer's own command-line syntax
and is documented by Microsoft, but it was not run against this repository. If
it does not do the right thing, use the installer UI and tick *Desktop
development with C++*; the result is identical.

After installing, **open a new shell**. An already-running shell will not see
`link.exe` on `PATH`.

### 3. NASM **[verified requirement, unverified install]**

This one surprises people, so here is the evidence rather than an assertion.

`aws-lc-sys` 0.45.0 is in the dependency graph of a **default** `cargo build -p
loams`. **[verified]** — `cargo tree -p loams -e normal -i aws-lc-sys --locked`
puts it there through `lance` → `object_store`/`lance-core` → `aws-lc-rs`. It is
not optional and not behind a feature.

That crate's build script selects a builder in
`builder/main.rs:get_builder`, and on `windows` + `x86_64` the resulting
`CcBuilder` drives a `NasmBuilder` (`builder/cc_builder.rs`, `apply_nasm` and
`nasm_builder.compile_intermediates()`). The only way it skips NASM is
`use_prebuilt_nasm()`, which returns true only when the crate's `prebuilt-nasm`
**feature** is enabled, or `AWS_LC_SYS_PREBUILT_NASM=1` is set. **[verified]** by
reading the vendored crate source.

`prebuilt-nasm` is **not** in `aws-lc-sys`'s default features (its
`default = ["all-bindings"]`), and nothing in this repository's dependency graph
enables it. **[verified]** — `cargo tree` shows no `aws-lc-rs/prebuilt-nasm`
edge. **So NASM is required.**

Install it:

- **[unverified]** `winget install NASM.NASM`
- **[unverified]** `choco install nasm`
- **[unverified]** Or download a `nasm-*-win64.zip` from <https://www.nasm.us/>
  and put `nasm.exe` on `PATH`.

Whichever you pick, check it:

```powershell
nasm -v
```

**The escape hatch**, if you would rather not install NASM: set the environment
variable and the crate uses its own pregenerated objects instead.

```powershell
$env:AWS_LC_SYS_PREBUILT_NASM = "1"
cargo build --release -p loams --locked
```

**[verified]** that the variable is read (`use_prebuilt_nasm()` calls
`allow_prebuilt_nasm()`, which reads `SYS_PREBUILT_NASM` from
`AWS_LC_SYS_PREBUILT_NASM`). **[unverified]** that this build then succeeds on
Windows — the crate's own comment warns that prebuilt objects "cannot apply
size-optimization definitions", so install NASM if it fails.

### 4. protoc **[verified requirement, unverified install]**

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

**[unverified]** install options, most reliable first:

1. Download `protoc-<version>-win64.zip` from the
   [protobuf releases page](https://github.com/protocolbuffers/protobuf/releases),
   extract it somewhere stable (`C:\protoc`), and add `C:\protoc\bin` to
   `PATH`. This is the recommended route because the version is explicit and you
   can see what you installed. Pick a **3.x** release; this page was written
   against both a 3.21-series `protoc` (what `ci.yml` gets from Ubuntu 24.04)
   and 36.1 (what the Linux machine used to write it), and both build this tree.
   **[verified]** that both work, on Linux.
2. `choco install protobuf`
3. `winget install protobuf` — **unverified**: I could not confirm a package with
   that exact identifier, so check `winget search protobuf` first.

Put it on `PATH` **for the shell that runs cargo**, and confirm:

```powershell
$env:Path += ";C:\protoc\bin"   # this session only
protoc --version
```

Persist it with the normal Windows *Environment Variables* UI, or
`[Environment]::SetEnvironmentVariable('Path', $env:Path, 'User')`.

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
is false and the **`cc` builder is used** — which needs only `cl.exe`.

Install CMake if a build tells you it is missing, or if you enable `mysql-wire`
(`mysql_common` depends on both `cmake` and `bindgen`).

## Clone and build

**[verified, platform-pending]**

```powershell
git clone https://github.com/ostrium-labs/loams.git
cd loams

# debug build, at target\debug\loams.exe
cargo build -p loams --locked

# release build, at target\release\loams.exe — what you probably want
cargo build --release -p loams --locked
```

`--locked` matters: it makes cargo fail rather than silently update
`Cargo.lock`. Every gate in the issue templates uses it, and CI does too.
**[verified]**

Useful variants:

```powershell
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

```powershell
.\target\release\loams.exe dev
```

## Test

**[verified, platform-pending]** — these are the commands CI runs, minus the
runner plumbing:

```powershell
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --doc --locked
cargo test --workspace --locked
```

Two notes on the fourth one.

**`cargo nextest` is what CI uses**, partitioned in two:

```powershell
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

**Not every CI job is portable to Windows, and that is expected.** The
authoritative list is `ci.yml`. The jobs that need a Unix host or a service are:

| CI job | Needs |
|---|---|
| `tikv`, `tikv-nightly` | `tiup` and a PD/TiKV v8.5.8 playground, driven by `scripts/tikv/playground.sh` (bash) |
| `durable-tidb` | `tiup` and TiDB v8.5.8, driven by `scripts/durable/tidb.sh` (bash), plus the Go toolchain |
| `qdrant-client` | `crates/loams-qdrant/tests/python/run.sh` (bash) and `uv` |
| `es-client` | Docker running Elasticsearch 8.19.22, and `conformance/es-client/run.sh` (bash) |
| `specview` | Java 21, `trunk`, and the `wasm32-unknown-unknown` Rust target |
| `dapr-edge` | `kubectl` with kustomize |
| `tla` | Java 21 and the pinned TLA+ tools |
| `lean` | `elan` and `lake` |

**[verified]** — each of these is a separate job in `ci.yml` with its own
setup step; none of them is part of `cargo test --workspace`. The default-feature
test suite has no service dependency, because every service-backed suite is
behind a Cargo feature or lives in a non-Rust script.

## The web console

The console is a separate pnpm workspace under `web/`. It is a Node project, so
none of the Rust prerequisites apply to it.

**[verified, platform-pending]**

```powershell
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

There is a second, unrelated pnpm workspace at `sdks/live-typescript`. It holds
generated proto stubs for `@loams/live` and is not needed to build or run the
console. **[verified]**

## Gotchas

### Long paths

Windows' `MAX_PATH` limit bites deep Cargo dependency trees. `cargo` and `cl`
both use extended-length paths for most of their work, but some tools in the
chain do not. If you see `The filename or extension is too long` or a
`file not found` on a path that obviously exists, the fix is the long-path
switch, which needs Administrator once:

```powershell
New-ItemProperty -Path 'HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem' `
  -Name 'LongPathsEnabled' -Value 1 -PropertyType DWORD -Force
```

**[unverified]** — the registry key and value are Microsoft's documented
setting, but this was not exercised here. A simpler alternative that avoids the
registry entirely: keep the clone near the root of a drive
(`C:\src\loams` rather than `C:\Users\you\Documents\work\loams`) and put
`CARGO_TARGET_DIR` on another volume.

### The shell you build in is the shell that needs `protoc`

`PATH` is per-process. A `protoc` reachable from your Explorer-launched terminal
is not necessarily reachable from the one VS Code or a CI emulator spawns. When
in doubt, set `PROTOC` to the absolute path of `protoc.exe` for the build; both
`prost-build` and `tonic-prost-build` honour it. **[unverified]** — this is the
documented behaviour of the two build helpers, read from their source rather
than tested here.

### Antivirus makes the first build unusable

Real-time scanning of every object file in `target\` can turn a first build from
tens of minutes into hours. Excluding the checkout (or just `target\`) from
Windows Defender is the standard fix. **[unverified]** — advice, not a
repository fact.

### `link.exe not found` after installing Build Tools

The shell predates the install. Open a new one. If it persists, `link.exe` is
not on `PATH` outside a *Developer Command Prompt*, which is normal — cargo
locates the MSVC toolchain through the registry, so a plain shell is fine as
long as Build Tools is installed.

### SIGTERM is not SIGINT on Windows

`crates/loams/src/main.rs:shutdown_signal` registers `SIGTERM` under
`#[cfg(unix)]` and falls back to `tokio::signal::ctrl_c()` elsewhere.
**[verified]** — read from the source. So on Windows, `Ctrl+C` stops the server
and there is no Unix-style `SIGTERM` to send. Anything scripted against
`kill -TERM` needs `Stop-Process` instead.

### Disk space accounting differs

`crates/loams-hot/src/tier.rs` computes a directory's on-disk size from
`MetadataExt::blocks()` under `#[cfg(unix)]`, with a `#[cfg(not(unix))]`
fallback that cannot see allocated-block rounding. **[verified]** — read from
the source. Hot-tier capacity reporting will therefore be less precise on
Windows. Nothing else in the crate tree is platform-conditional: there is not a
single `cfg(target_os = "linux")` anywhere under `crates/`, which is why this
page is possible at all. **[verified]**

## Windows artifacts are unsigned

State this plainly to anyone who downloads something you built and runs it
elsewhere: **it has no Authenticode signature.**

The consequence on Windows is **SmartScreen**, and it is worth being precise
about which warning it is. On first run of an unknown, unsigned binary Windows
may show:

> Windows protected your PC

That is an **unknown-publisher** warning. It is *not* a malware detection, it
does not appear because anything is wrong with the binary, and it clears
per-file as soon as the user unblocks it. What the user does:

- Click **More info**, then **Run anyway**; or
- Right-click the file → **Properties** → tick **Unblock** → **Apply**; or
- In PowerShell, for a file that carries the mark:

  ```powershell
  Get-Item .\loams.exe | Unblock-File
  ```

**A binary you built yourself does not trigger this.** The SmartScreen prompt is
driven by a *Mark-of-the-Web*, the `Zone.Identifier` alternate data stream that
Windows attaches to files **downloaded** from the internet. `cargo build` output
has no such stream, so a local build runs without any prompt. The section above
matters for a *distributed* copy of a build, not for your own.

There are currently no Loams Windows binaries to distribute — see
[what you do not get](#what-you-get-and-what-you-do-not) — so this is about the
day a release pipeline exists, and about copies you hand to someone else. The
decision to leave Windows unsigned is recorded as
[Q615](../design/13-decision-log.md) and
[D620](../design/13-decision-log.md); `docs/release/signing.md` explains why
shipping a binary signed in SignPath Foundation's name was judged the wrong
trade for a project with no Windows certificate of its own.

### Signing your own build locally

Optional, and unrelated to the SignPath pipeline, which deliberately does not
touch Windows **[verified]** — `scripts/ci/signpath-artifacts.py` rejects
`.exe`, `.msi`, `.msix` and the rest by name *and* by content.

For your own smoke tests, an ad-hoc-style Authenticode signature with a
self-signed certificate is enough to see SmartScreen behave differently. It is
**not** a trust anchor — nobody else will trust a certificate you minted
yourself.

**[unverified]** — none of the following was run here, and none of it is
supported by this repository:

```powershell
# 1. A code-signing certificate, in a PFX.
New-SelfSignedCertificate -Type CodeSigningCert -Subject "CN=Loams Local Dev" `
  -CertStoreLocation Cert:\CurrentUser\My

# 2. Sign. SignTool ships with the Windows SDK, which the Build Tools
#    workload installs.
& "${env:ProgramFiles(x86)}\Windows Kits\10\bin\x64\signtool.exe" sign `
  /fd SHA256 /f .\loams-dev.pfx /p $env:LOAMS_DEV_PW .\target\release\loams.exe

# 3. Confirm.
& "${env:ProgramFiles(x86)}\Windows Kits\10\bin\x64\signtool.exe" verify `
  /pa .\target\release\loams.exe
```

A real, publicly trusted Authenticode certificate cannot be obtained for free,
and this project's maintainers hold none. That is the whole of the reason
Windows is unsigned. If you want to change that, the options and what they cost
are [Q421](../design/13-decision-log.md) and
[Q615](../design/13-decision-log.md).

## See also

- [`macos.md`](macos.md) — the same page for macOS.
- [`docs/release/signing.md`](../release/signing.md) — what is signed, what is
  not, and the human steps that are still outstanding.
- [`CONTRIBUTING.md`](../../CONTRIBUTING.md) and
  [`GOVERNANCE.md`](../../GOVERNANCE.md) — the gates every change has to pass.
- [`README.md`](../../README.md) — what Loams is, and the listener table.