# CLI1 — The Local CLI, Stacks and the Stdio MCP Server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, flags, error codes, exit codes, file formats), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: Planned** (2026-10-01). Track CLI (D298), beside M1 and tracks R and D. Branches `cli1-t<N>`, stacked; PRs target `main`. CLI1 adds one crate (`operon-cli`) and three server flags (Task 3), and changes no M1 code path beyond `crates/operon/src/main.rs` and the `Native` flag group. **Working names throughout (D33):** the binary is `operon` and every user-facing string says `operon`. The rename PR changes `operon` to `loams` in strings; the env-var prefix stays `LOAM_` from the start (Ruling 2). Nothing is published.

**Goal:** Ship design §30's local half (D281–D285, D287–D291):
- `operon-cli`: the client command groups flattened into the `operon` binary, with the output and exit-code contract (D283), `LOAM_HOME` and profiles (D284);
- local **stacks** over `operon dev`/`standalone`: the engine registry, `stack.toml`, port allocation, a detached supervisor, health waits, logs (D285);
- **NVMe**: the H1 cache flags on the server, `storage inspect` and `storage prepare` (under `sudo`), and `--storage nvme:|mount:|dir:` (D287);
- **`env export`** and `.env.loam`, with the `Secret` type (D288);
- the **embedded docs and tested snippets** with `docs search` and `docs snippet` (D291);
- **`mcp serve`** over stdio with seven bootstrap tools and their safety rules, and **`mcp install`** for Claude Code, Codex, Cursor and Windsurf (D289, D290);
- `init`, `version`, `completions` and the CLI guide.

**Architecture:**
- `crates/operon-cli` is a library with no server dependencies (design §4.1). `crates/operon/src/main.rs` gains `#[command(flatten)] Client(operon_cli::ClientCommand)` beside `Dev`, `Standalone`, `Cluster`, `Warm` and `Durable`, plus the global flags, and calls `operon_cli::run`. The server commands are unchanged.
- The binary passes an `operon_cli::BuildInfo` (version, target, compiled-in features via `cfg!(feature = …)`, variant name from `option_env!("LOAM_VARIANT")`) into `run`, so `operon-cli` knows what the running binary can serve without depending on `operon`.
- Every side effect goes through a `Context` (home directory, environment, clock, TTY flags, HTTP client, process spawner, port prober), so unit tests run without touching `$HOME`, real ports or real processes. End-to-end tests drive the real binary with `CARGO_BIN_EXE_operon` from `crates/operon/tests/cli/`.
- A stack is `operon stack run --name <n>` → `exec` of `operon dev|standalone` with flags generated from `stack.toml` by the engine registry; `stack start` spawns that detached in its own process group.
- `mcp serve` is an rmcp `ServerHandler` on the stdio transport; tool handlers call the same functions the CLI commands call, and return redacted, structured results.

**Tech Stack:**
- Rust 1.97.1, edition 2024, workspace lints (`unsafe_code = "forbid"`: Ruling 9).
- New dependencies of `operon-cli` (Task 0 checks versions and `cargo deny`): `clap` 4 (workspace, `derive`, plus `env`, `wrap_help`), `clap_complete` 4.6, `serde` + `serde_json` (workspace), `toml_edit` 0.25 (MIT OR Apache-2.0; with `serde`), `schemars` 1 (MIT; the major rmcp uses), `rmcp` (workspace pin, features `server`, `transport-io`, `macros`), `tokio` (workspace), `reqwest` (workspace, rustls), `tantivy` (workspace pin `=0.26.2`), `zstd` 0.13 (workspace), `tar` 0.4 (MIT OR Apache-2.0), `comfy-table` 8 (MIT), `dialoguer` 0.12 (MIT; default features off, `password` not needed), `fs4` 1 (MIT OR Apache-2.0; `sync`), `nix` 0.31 (MIT; features `signal`, `process`, `fs`), `humantime` 2 (workspace lock), `thiserror` 2, `tracing`. Dev: `tempfile` (workspace), `assert_cmd` 2 (MIT OR Apache-2.0) in `crates/operon` only, `similar` 2 (Apache-2.0; golden diffs).
- System tools used at run time (never linked): `lsblk`, `blkid`, `mkfs.ext4`, `mount` (util-linux and e2fsprogs; Linux only, `storage prepare` only); `git` (optional, for `.gitignore`); `claude`, `codex` (optional, for `mcp install`).
- CI: Ubuntu runners; the `cli-nvme` job uses `sudo losetup`; the `snippets` job uses `uv` and Node 22.

**Spec:**
- [`docs/design/30-loam-cli.md`](../design/30-loam-cli.md): all of it; §5 (tree), §6 (output), §7 (home), §8 (stacks), §10 (NVMe), §11 (secrets), §12 (MCP), §13 (docs).
- [`docs/design/13-decision-log.md`](../design/13-decision-log.md): D281–D291, D295, D298; D29, D33, D65, D111, D262, D-PG-1.
- [`docs/plans/2026-09-24-m1.6-sdks-mcp.md`](2026-09-24-m1.6-sdks-mcp.md): Ruling 7 (protocol versions), Task 7 (the error shape `structured_error`), Ruling 19 (`--mcp-listen`), Task 9 (client configs).
- [`docs/design/19-console-identity-and-agents.md`](../design/19-console-identity-and-agents.md) §5.5 (keys are not for agents).
- [`docs/design/04-hot-tier.md`](../design/04-hot-tier.md) §1 (H1, H2); [`docs/design/10-operations.md`](../design/10-operations.md) §1, §2.
- As built: `crates/operon/src/main.rs` (`Command`, `Native`, `Tuning`), `crates/operon/src/server.rs` (`ServerConfig.cache: RangeCacheConfig`), `crates/operon-cache/src/range_cache.rs` (`RangeCacheConfig`, `DiskConfig`), `crates/operon/src/api/mod.rs` (`/health`, `/ready`).

## Global Constraints

Same as the M1 overview §8, plus:
- **No server dependency in `operon-cli`.** It must not depend on `operon`, `operon-server` (CLI2), `operon-query`, `operon-collection`, `operon-meta*`, `operon-log`, `operon-store` or `operon-mcp`. A CI step checks `cargo tree -p operon-cli -e normal --prefix none` contains none of them.
- **Loopback only.** Every address the CLI generates for a stack is on `127.0.0.1` (D111). There is no flag to change that in CLI1.
- **Stdout purity.** In `--output json` mode and under `mcp serve`, nothing but the documented JSON (or JSON-RPC) is written to stdout: tracing, notices and progress go to stderr. A test per command checks it.
- **No secret leaves through MCP or JSON** (D288): `Secret` has no `Serialize`/`Display`; `Secret::expose` is a `clippy::disallowed_methods` entry (root `clippy.toml`), allowed only in `env.rs` (the dotenv writer) and `http.rs` (the auth header), each with `#[allow(clippy::disallowed_methods)] // §30 §11.1`.
- **No destructive action without `--yes` or a typed TTY confirmation**; without a TTY, `confirmation_required` (exit 3).
- **No unsafe code**; detaching uses `std::os::unix::process::CommandExt::process_group(0)`, signals use `nix::sys::signal::killpg` (Ruling 9).
- **Hermetic tests.** Unit tests never read `$HOME`, `$PATH` or real ports: they use `Context::for_test(tempdir)`. End-to-end tests set `LOAM_HOME` to a tempdir under the shared target's `tmp` (never `/tmp`, user memory), `LOAM_NO_UPDATE_CHECK=1`, and pick port bases from `crates/operon/tests/cli/ports.rs` (a per-process atomic counter starting at 30000 + 1000 × (pid % 20)).
- **The build machine.** One cargo build at a time on the shared target, `-j 6`, lld. `operon-cli` must build in under 60 s from a warm target (Task 0 measures; it reuses tantivy and reqwest already in the tree).
- **Commit areas:** `cli`, `operon`, `cache`, `docs`, `ci`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **Client groups are flattened at the top level** (`operon stack create`, not `operon cli stack create`), and the server commands keep their names. A client group name may never equal a server command (`dev`, `standalone`, `cluster`, `warm`, `durable`); `tests/tree.rs` asserts it | Design §5; CI, the crash gate and every plan run `operon dev` | None |
| 2 | **Env vars use the `LOAM_` prefix from CLI1** (`LOAM_HOME`, `LOAM_OUTPUT`, `LOAM_PROFILE`, `LOAM_NO_INPUT`, `LOAM_NO_UPDATE_CHECK`, `LOAM_VARIANT`), and `LOAM_HOME` defaults to `~/.loam`, even under the working name | The rename must not move users' state or break their scripts; `.env.loam` variables are user-facing contracts (design §11.2) | A user who installs the pre-rename binary sees `~/.loam` with an `operon` binary in it; nothing is published before the rename, so only developers see it |
| 3 | **clap usage errors honour `--output json`.** `main` parses with `Cli::try_parse()`; on error it pre-scans argv for `--output json`/`-o json`/`--output=json` (or `LOAM_OUTPUT=json`) and, if found, prints `{"error": {"code": "usage", "message": <clap's first line>, "hint": "run with --help", "exit_code": 2}}` on stderr and exits 2; otherwise clap's own rendering. `--help` and `--version` stay clap's | Agents must parse every failure (design §6.2) | None |
| 4 | **`Render` is the only output path.** Every command returns `Result<T: Render, CliError>`; `output::emit` renders it in the chosen format. No command writes to stdout itself, except `completions`, `mcp serve` and `env export` in dotenv form, which return `RawOutput` (bytes for stdout) | One place enforces stdout purity and the error shape | None |
| 5 | **`stack.toml` is versioned** with `schema = 1` at the top; an unknown `schema` is refused (`config_unparseable`, exit 5) and never rewritten | Later CLIs read stacks made by earlier ones | None |
| 6 | **Ports are fixed at create time** and stored in `stack.toml`; `start` re-probes and fails with `port_in_use` naming the engine and port | SDK configs and `.env.loam` stay valid across restarts | A machine where something else took the port must `stack delete` and recreate the stack (a `stack set-port` command is a later addition) |
| 7 | **`stack create` writes `.env.loam` only when a project directory is found** (design §11.2: `--project-dir`, `$CLAUDE_PROJECT_DIR`, or an ancestor with `.git`, `package.json`, `pyproject.toml` or `Cargo.toml`), never in `$HOME` itself | Avoids secrets files in the home directory | A user in a bare directory runs `env export --write .env.loam` explicitly |
| 8 | **`stack_create` from MCP is allowed** (Q292's proposed answer), within design §12.1's limits: local object store, embedded metastore, `dir:`/`mount:` storage only, at most 8 stacks per `LOAM_HOME`, no download (CLI1 has no downloads at all) | The chat dump's "the agent provisions the stack" | If the owner answers Q292 "no", the tool is removed from the list; nothing else depends on it |
| 9 | **No `unsafe`, so no `setsid`.** Detach is `process_group(0)` + stdin `/dev/null` + stdout/stderr to `logs/server.log`; stop is `killpg(pgid, SIGTERM)` then `SIGKILL`. `stack run` is `CommandExt::exec` (safe) so the pid file names the server | The workspace forbids unsafe code; a separate process group already escapes the terminal's job control and SIGHUP of the foreground group | A server started from a terminal whose session leader sends SIGHUP to every member (rare) would die; `stack run` under systemd/launchd (CLI2) is the robust form |
| 10 | **The docs bundle is built by `operon-cli/build.rs`** from `docs/guides/**/*.md` and `docs/snippets/**` (paths relative to the workspace root via `CARGO_MANIFEST_DIR/../..`), with `cargo:rerun-if-changed` on both trees | Docs match the binary (D291) | Publishing `operon-cli` to crates.io later needs the trees copied into the crate (`include` in `Cargo.toml`); CLI2's publish step does it |
| 11 | **The H1 disk tier default stays off** on the server (`--cache-dir` absent → RAM only), and `--cache-disk-bytes` without `--cache-dir` is a clap error | Changing server defaults is out of CLI1's scope; stacks pass the flags explicitly (design §10.3) | None |
| 12 | **MCP tool names and argument names are snake_case and frozen** once Task 9 merges; changes are additive only, as for JSON outputs (design §6.1) | Agents' prompts and configs embed them | None |
| 13 | **`mcp install` prefers the agent's CLI** only for Claude Code and Codex, and only when `claude`/`codex` is on `PATH` and `<cli> --version` succeeds within 5 s; otherwise it edits the file. User scope for Claude Code without `claude` on `PATH` prints the command and exits 3 (`~/.claude.json` is Claude Code's private state and is not edited) | Design §12.2 | None |

## Review Focus

1. **Stdout purity and the error shape** on every command in `json` mode, including clap usage errors (Ruling 3). Tests: Task 1 `json_mode_errors_go_to_stderr_as_one_object`, `usage_error_in_json_mode_is_json`; every task's `*_json_output_matches_schema`.
2. **No secret reaches MCP output, JSON output or logs.** Tests: Task 6 `secret_debug_is_redacted` and `secret_is_not_serializable` (a `compile_fail` doc test, no `trybuild`), Task 9 `canary_never_appears_in_any_tool_output_or_log`.
3. **`storage prepare` cannot touch the wrong device.** Tests: Task 7's refusal fixtures (nine cases) and the loop-device CI job.
4. **Config edits never lose a user's other entries.** Tests: Task 10's golden files (`preserves_other_servers`, `unparseable_file_is_not_written`).
5. **MCP path confinement.** Tests: Task 9 `env_export_refuses_paths_outside_the_project`, `env_export_refuses_symlink_escape`.
6. **Engine flags match the server's actual clap tree.** Test: Task 4 `generated_server_args_parse_with_the_real_cli` (parses the generated argv with the `operon` binary's `Cli::try_parse_from`).

## File structure

```
Cargo.toml / Cargo.lock                      # operon-cli member; clap_complete, toml_edit, tar, comfy-table, dialoguer, fs4, nix, schemars (workspace deps)
clippy.toml                                  # disallowed-methods: operon_cli::secret::Secret::expose
crates/operon-cli/
  Cargo.toml  build.rs
  src/lib.rs  src/context.rs  src/output.rs  src/error.rs  src/home.rs  src/config.rs  src/configure.rs
  src/engines.rs  src/ports.rs
  src/stack/{mod.rs,spec.rs,resolve.rs,supervise.rs,health.rs,logs.rs}
  src/storage/{mod.rs,lsblk.rs,plan.rs,exec.rs}
  src/env.rs  src/secret.rs  src/project.rs  src/http.rs
  src/docs/{mod.rs,bundle.rs,index.rs,snippet.rs}
  src/mcp/{mod.rs,server.rs,tools.rs,install.rs,agents/{claude_code.rs,codex.rs,cursor.rs,windsurf.rs}}
  src/init.rs  src/version.rs  src/keys.rs
  tests/{tree.rs,output.rs,home.rs,config.rs,engines.rs,ports.rs,spec.rs,storage.rs,env.rs,docs.rs,agents.rs,mcp_tools.rs}
  tests/fixtures/{lsblk/*.json,blkid/*.txt,agents/**}  tests/golden/{help/*.txt,args/*.txt,agents/**}  tests/schemas/*.json
crates/operon/Cargo.toml                     # operon-cli dep; dev: assert_cmd
crates/operon/src/main.rs                    # Cli { globals, command }, Command::Client, BuildInfo, --cache-* flags in Native
crates/operon/tests/cli/{main.rs,ports.rs,stack.rs,env.rs,mcp.rs,init.rs}
docs/snippets/{native,qdrant,es,pg}/{python,typescript,shell}/{connect,upsert,search}.*
docs/guides/cli.md
scripts/snippets/run.sh
.github/workflows/ci.yml                     # jobs cli-nvme, snippets; cargo-tree guard for operon-cli
docs/design/04-hot-tier.md  docs/design/10-operations.md  CHANGELOG.md
```

### Task 0: Reconcile and measure

**Files:** read `crates/operon/src/main.rs`, `crates/operon/Cargo.toml`, `crates/operon/src/server.rs` (`ServerConfig::cache`, how `Native` maps into `ServerConfig`), `crates/operon-cache/src/range_cache.rs`, `crates/operon/src/api/mod.rs`, and whether M1.6 Task 7 (`operon-mcp`, `--mcp-listen`, `--no-mcp`) has merged. Fill this plan's "Rulings made during execution" table.

**Checks** (record each with the command used):
- Every server flag the engine registry (design §8.2) names exists on `dev` and `standalone` with that spelling, and which are `cfg(feature)`-gated (`--pg-listen`, `--mysql-listen`, `--stream-grpc-listen`) or unconditional (`--no-es`, `--no-qdrant`, `--no-durable`, `--no-flight-sql`). If M1.6 has not merged, the `mcp` engine is registered as `available: false` (Task 4) and its env var omitted.
- `rmcp`: the workspace pins `3.4.1`; crates.io has `3.5.0` (2026-09-28). Decide the pin with M1.6's owner (one rmcp in the tree), confirm `transport-io` exists in it, and that `rmcp::transport::stdio()` and `TokioChildProcess` (dev, `transport-child-process`) are the names to use.
- `cargo deny check` with the new dependencies; `cargo tree -d` duplicates added.
- A warm-target build time of `operon-cli` alone, and the stripped release size delta of `operon` with it (one measured build each way, deleted afterwards).
- Whether `/ready` returns 200 only after every listener is bound (the health wait depends on it); if not, Task 5 waits on `/ready` and then on each engine's port accepting a TCP connection.

**Commit:** `docs: reconcile CLI1 with main`.

### Task 1: The crate, the global flags and the output contract

**Files:** `crates/operon-cli/{Cargo.toml,src/{lib.rs,context.rs,output.rs,error.rs,version.rs,keys.rs}}`, `crates/operon-cli/tests/{tree.rs,output.rs}`, `crates/operon/{Cargo.toml,src/main.rs}`, `Cargo.toml`, `clippy.toml`. **PR size:** about 900 lines with tests.

**Produces:**

```rust
// crates/operon-cli/src/lib.rs
#[derive(Debug, Clone, clap::Args)]
pub struct Globals {
    #[arg(long, short = 'o', global = true, value_enum, env = "LOAM_OUTPUT")] pub output: Option<OutputFormat>,
    #[arg(long, global = true, env = "LOAM_PROFILE")] pub profile: Option<String>,
    #[arg(long, global = true, env = "LOAM_NO_INPUT")] pub no_input: bool,
    #[arg(long, short = 'q', global = true)] pub quiet: bool,
    #[arg(short = 'v', long = "verbose", global = true, action = clap::ArgAction::Count)] pub verbose: u8,
    #[arg(long, global = true, value_enum, default_value = "auto")] pub color: ColorChoice,
}
#[derive(Debug, clap::Subcommand)]
pub enum ClientCommand {
    /// Set up Loam on this machine: a profile, a first stack, .env.loam and your coding agents.
    Init(init::InitArgs),
    /// Show or change CLI settings.
    Configure(configure::ConfigureArgs),
    /// Create and manage local stacks.
    Stack { #[command(subcommand)] command: stack::StackCommand },
    /// Inspect and prepare a local disk for the cache (Linux).
    Storage { #[command(subcommand)] command: storage::StorageCommand },
    /// Export a stack's endpoints (and, with auth, credentials) as environment variables.
    Env { #[command(subcommand)] command: env::EnvCommand },
    /// API keys (needs the unified auth plan).
    Keys { #[command(subcommand)] command: keys::KeysCommand },
    /// Sign in to a Loam endpoint (needs the unified auth plan).
    Login(keys::LoginArgs),
    /// Sign out of the profile's endpoint (needs the unified auth plan).
    Logout,
    /// The MCP server for coding agents, and its installation.
    Mcp { #[command(subcommand)] command: mcp::McpCommand },
    /// Search the docs and SDK snippets built into this binary.
    Docs { #[command(subcommand)] command: docs::DocsCommand },
    /// Print the version, build features and output schema.
    Version,
}
#[derive(Debug, Clone)]
pub struct BuildInfo { pub version: &'static str, pub target: &'static str, pub git_sha: Option<&'static str>,
                       pub variant: &'static str /* option_env!("LOAM_VARIANT").unwrap_or("custom") */,
                       pub features: &'static [&'static str] /* the cfg!(feature) list, sorted */ }
pub async fn run(command: ClientCommand, globals: Globals, build: BuildInfo) -> std::process::ExitCode;
pub fn run_usage_error(err: &clap::Error, argv: &[std::ffi::OsString]) -> std::process::ExitCode; // Ruling 3
pub fn completions(shell: clap_complete::Shell, cmd: &mut clap::Command, bin: &str, out: &mut dyn std::io::Write);

// crates/operon-cli/src/output.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, serde::Deserialize, serde::Serialize)]
pub enum OutputFormat { Table, Json, Text }
pub trait Render: serde::Serialize + schemars::JsonSchema {
    fn table(&self, out: &mut dyn std::io::Write, style: &Style) -> std::io::Result<()>;
    fn text(&self, out: &mut dyn std::io::Write) -> std::io::Result<()>;     // TSV, no header
}
pub struct RawOutput(pub Vec<u8>);
pub fn emit<T: Render>(ctx: &Context, result: Result<T, CliError>) -> std::process::ExitCode;
pub fn emit_raw(ctx: &Context, result: Result<RawOutput, CliError>) -> std::process::ExitCode;
pub const OUTPUT_SCHEMA: u32 = 1;

// crates/operon-cli/src/error.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]   // as_str is hand-written
pub enum ErrorCode {
    Internal, Usage,
    ConfirmationRequired, RootRequired, SudoStepRequired, LoginRequired, DownloadConsentRequired,
    StackNotFound, KeyNotFound, DeviceNotFound, VariantNotFound, ProfileNotFound,
    StackExists, PortInUse, StackRunning, StackNotRunning, ConfigUnparseable, DeviceInUse, DeviceHasFilesystem, TooManyStacks,
    EngineUnsupported, PlatformUnsupported, AuthNotAvailable, ManagedInstall, FeatureNotInVariant,
    StackUnreachable, Network, Timeout, ServerError,
    Unauthenticated, Forbidden,
    ChecksumMismatch, SignatureInvalid, ManifestInvalid,
    Interrupted,
}
impl ErrorCode { pub const fn as_str(self) -> &'static str; pub const fn exit_code(self) -> u8; pub const ALL: &'static [ErrorCode]; }
#[derive(Debug, thiserror::Error)] #[error("{message}")]
pub struct CliError { pub code: ErrorCode, pub message: String, pub hint: Option<String>, pub details: serde_json::Map<String, serde_json::Value> }

// crates/operon-cli/src/context.rs
pub struct Context { pub build: BuildInfo, pub globals: Globals, pub home: home::LoamHome, pub env: Box<dyn Env>,
                     pub tty: Tty { stdin: bool, stdout: bool, stderr: bool }, pub out: Box<dyn Write + Send>, pub err: Box<dyn Write + Send>,
                     pub http: http::Http, pub procs: Box<dyn Spawner>, pub ports: Box<dyn PortProbe>, pub clock: Box<dyn Clock> }
impl Context { pub fn from_process(globals: Globals, build: BuildInfo) -> Result<Self, CliError>; pub fn for_test(dir: &std::path::Path) -> TestContext; pub fn output_format(&self) -> OutputFormat; pub fn can_prompt(&self) -> bool; }
```

```rust
// crates/operon/src/main.rs (shape)
#[derive(Debug, Parser)]
#[command(name = "operon", version, about = "Operon: an object-storage-native database")]
struct Cli { #[command(flatten)] globals: operon_cli::Globals, #[command(subcommand)] command: Command }
enum Command { Dev{…}, Standalone{…}, Cluster{…}, Warm{…}, Durable{…},
               /// Print shell completions.
               Completions { shell: clap_complete::Shell },
               #[command(flatten)] Client(operon_cli::ClientCommand) }
const FEATURES: &[&str] = &[/* "es" if cfg!(feature = "es"), …: built by a const fn filter over a (name, bool) table */];
```

**Semantics:**
- Exit codes and error codes exactly as design §6.3; `TooManyStacks` is exit 5 (`too_many_stacks`). `ErrorCode::ALL` lists every variant, and a test checks `as_str` is unique snake_case and `exit_code` matches the table.
- Output format resolution: `--output` > `LOAM_OUTPUT` > profile `output` (Task 2; until then skipped) > `table`. Never from TTY detection (design §6.1).
- `emit`: JSON → `serde_json::to_writer(out, &value)` + `\n`; table → `Render::table`; text → `Render::text`. Errors → stderr: JSON object in json mode, else `error: …` / `hint: …`. Colour (`Style`) only when `--color always`, or `auto` with stdout a TTY and `NO_COLOR` unset.
- `version`: `{"name": "operon", "version", "target", "git_sha", "variant", "features", "output_schema": 1, "docs_version"}` (`docs_version` = `version` until Task 8 sets the bundle's). Table: `operon 0.0.1 (custom; x86_64-unknown-linux-gnu)` then the features.
- `completions <shell>` writes `clap_complete::generate` output for the whole `Cli` (server commands included) as `RawOutput`. With `--output json` it instead goes through `emit` as `{"shell", "script"}` (design §6.1), so `RawOutput` is only the non-JSON path. A `Completions` type derives `Render` and `JsonSchema` for it.
- Ctrl-C during a command: exit 130 with `interrupted` (a `tokio::signal::ctrl_c` race in `run`).
- **Auth stubs** (D295): every `keys` verb (`create`, `list`, `rotate`, `revoke`, with design §5's flags so `--help` documents them), `login` (`--endpoint`) and `logout` parse their arguments and exit 6 with `auth_not_available` and the hint `this build has no auth (D111); local stacks are loopback-only`. `src/keys.rs` holds the clap types, so CLI3 replaces only the bodies.

**Tests:**
- `tests/tree.rs`: `command_tree_is_valid` (`Cli::command().debug_assert()` through a tiny test-only `Cli` mirror that flattens `ClientCommand`); `client_groups_never_shadow_server_commands` (Ruling 1); `help_goldens_match` (renders `--help` for every client command and compares with `tests/golden/help/<path>.txt`; `UPDATE_GOLDEN=1` rewrites).
- `tests/output.rs`: `error_codes_are_unique_snake_case_with_documented_exit_codes`; `json_mode_success_writes_one_document_to_stdout`; `json_mode_errors_go_to_stderr_as_one_object` (stdout empty, stderr parses, `exit_code` field equals the process code); `table_mode_error_prints_error_and_hint`; `output_resolution_order` (flag over env over default); `version_json_matches_schema` (against `tests/schemas/version.json`); `color_only_on_tty_without_no_color`; `auth_stubs_exit_6` (`keys create`, `keys list`, `keys rotate k1`, `keys revoke k1 --yes`, `login`, `logout`, each in `json` mode → exit 6, stderr `code: "auth_not_available"`).
- `crates/operon/tests/cli/main.rs`: `usage_error_in_json_mode_is_json` (`operon stack frobnicate -o json` → exit 2, stderr JSON with `code: "usage"`); `server_commands_still_parse` (`operon dev --help` and `operon cluster --help` succeed; `features_list_matches_cargo_toml` parses `crates/operon/Cargo.toml`'s `[features]` and checks every feature except `failpoints` and `cluster-tests` appears in the `FEATURES` table).

**Commit:** `cli: add operon-cli with the output and exit-code contract`.

### Task 2: `LOAM_HOME`, profiles and `configure`

**Files:** `crates/operon-cli/src/{home.rs,config.rs,configure.rs}`, `crates/operon-cli/tests/{home.rs,config.rs}`. **PR size:** about 600 lines.

**Produces:**

```rust
pub struct LoamHome { root: PathBuf }
impl LoamHome {
    pub fn resolve(env: &dyn Env) -> Result<Self, CliError>;   // LOAM_HOME, else $HOME/.loam; creates it 0700
    pub fn stacks(&self) -> PathBuf; pub fn stack(&self, name: &StackName) -> PathBuf;
    pub fn config(&self) -> PathBuf; pub fn credentials(&self) -> PathBuf; pub fn cache(&self) -> PathBuf; pub fn logs(&self) -> PathBuf;
    pub fn mcp_installs(&self) -> PathBuf; pub fn variants(&self) -> PathBuf; pub fn receipt(&self) -> PathBuf;
}
pub fn write_atomic(path: &Path, bytes: &[u8], mode: u32) -> std::io::Result<()>;  // temp in same dir, fsync, rename, fsync dir
pub struct DirLock(fs4::fs_std::FileExt-held File); impl DirLock { pub fn acquire(path: &Path, wait: Duration) -> Result<Self, CliError>; }
#[derive(Clone, Debug, PartialEq, Eq)] pub struct StackName(String); // ^[a-z][a-z0-9-]{0,30}$
pub struct Config { pub profiles: BTreeMap<String, Profile> } // toml_edit document kept for round-trips
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct Profile { pub endpoint: Endpoint /* "local" | Url */, pub default_stack: Option<StackName>, pub output: Option<OutputFormat>, pub update_check: bool /* true */ }
```

**Semantics:**
- `config.toml` as design §7; edits through `toml_edit` so comments and order survive `configure set`. Keys: `endpoint`, `default_stack`, `output`, `update_check`; unknown keys are kept on rewrite and reported by `configure list` as `unknown`. An unknown key in `configure set` is `usage` (exit 2).
- `configure` with no subcommand and a TTY asks for each key (dialoguer), showing the current value; without a TTY it prints the current profile (like `list`).
- `--profile` names a profile; a missing one is `profile_not_found` (exit 4) except in `configure set`, which creates it.
- `credentials.toml` is not read or written in CLI1 (CLI3), but `LoamHome` creates nothing there.
- `StackName` rules are enforced everywhere a name enters (`stack create --name`, MCP `name`).

**Tests:** `home_defaults_to_dot_loam_under_home`; `loam_home_env_overrides`; `home_is_created_0700`; `write_atomic_replaces_and_keeps_mode`; `write_atomic_leaves_no_temp_on_error`; `dir_lock_excludes_a_second_holder` (a second `acquire` with a 100 ms wait fails with `timeout`); `stack_names_are_validated` (`dev`, `a-1` ok; `Dev`, `-a`, `a_b`, 32 chars refused); `configure_set_preserves_comments_and_unknown_keys`; `configure_get_json_matches_schema`; `unknown_profile_is_not_found`; `configure_without_tty_lists`.

**Commit:** `cli: add LOAM_HOME, profiles and configure`.

### Task 3: The H1 cache flags on the server

**Files:** `crates/operon/src/main.rs` (`Native`), `crates/operon/src/server.rs` only if `ServerConfig.cache` needs a constructor, `crates/operon/tests/cli/main.rs` (flag parsing), `crates/operon/tests/cache_disk.rs` (new), `docs/design/04-hot-tier.md`, `docs/design/10-operations.md` (§2's flag table). **PR size:** about 250 lines.

**Produces** (flags on `dev`, `standalone` and `cluster`, design §10.1):

```rust
/// Memory for the object-range cache (H1), in bytes [default: 268435456].
#[arg(long)] cache_ram_bytes: Option<u64>,
/// Directory for the object-range cache's disk tier (H1, foyer); without it H1 is RAM only.
#[arg(long)] cache_dir: Option<PathBuf>,
/// Disk the object-range cache may use, in bytes [default: 100 GiB, capped at 80% of the filesystem's free space].
#[arg(long, requires = "cache_dir")] cache_disk_bytes: Option<u64>,
```

**Semantics:** map to `config.cache.memory_bytes` and `config.cache.disk = Some(DiskConfig { dir, capacity_bytes })`. The free-space cap uses `nix::sys::statvfs` (safe) on `cache_dir` at startup (created if absent), and logs the effective capacity once at `info`: `cache: H1 disk tier at <dir>, <bytes> bytes`. A `cache_dir` that is not writable fails startup with `cache: <dir> is not writable: <err>`.

**Tests:** `cache_flags_parse_on_dev_standalone_and_cluster`; `cache_disk_bytes_requires_cache_dir` (clap error); `cache_disk_capacity_is_capped_by_free_space` (unit test of the pure `effective_capacity(requested, free) -> u64`); `cache_dir_receives_files_after_reads` (in-process `Server` with `cache.disk` set and `memory_bytes` 1 MiB: write a collection, read it twice, assert files exist under `cache_dir`; skipped if foyer's direct-I/O open fails on the test filesystem, with a printed reason).

**Commit:** `operon: add --cache-dir, --cache-disk-bytes and --cache-ram-bytes`.

### Task 4: The engine registry, ports and the stack spec

**Files:** `crates/operon-cli/src/{engines.rs,ports.rs,stack/{mod.rs,spec.rs,resolve.rs}}`, `crates/operon-cli/tests/{engines.rs,ports.rs,spec.rs}`, `crates/operon-cli/tests/golden/args/*.txt`, `crates/operon/tests/cli/main.rs`. **PR size:** about 1 100 lines.

**Produces:**

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum Engine { Native, FlightSql, Mcp, Qdrant, Es, Pg, Durable, Mysql, StreamGrpc, Live, Jobs, Postgres, Tikv, Rustfs }
pub enum EngineKind { InBinary { feature: &'static str, on_by_default: bool, off_flag: Option<&'static str> }, Always, Companion }
pub struct PortSpec { pub key: &'static str, pub default: u16, pub offset: u16, pub flag: &'static str, pub scheme: &'static str }
pub struct EnvSpec { pub name: &'static str, pub template: &'static str /* "{scheme}://127.0.0.1:{port}{suffix}" */, pub secret: bool }
pub struct EngineSpec { pub engine: Engine, pub name: &'static str, pub aliases: &'static [&'static str], pub kind: EngineKind,
                        pub ports: &'static [PortSpec], pub env: &'static [EnvSpec], pub available: bool /* false until its plan merges */ }
pub static REGISTRY: &[EngineSpec];          // exactly design §8.2's rows, in that order
pub const DEFAULT_ENGINES: &[Engine] = &[Engine::Native, Engine::FlightSql, Engine::Mcp, Engine::Qdrant, Engine::Es];
pub fn parse_engines(list: &str) -> Result<BTreeSet<Engine>, CliError>;   // aliases; Native implied; Postgres/Tikv/Rustfs → engine_unsupported (CLI3)
pub fn required_features(engines: &BTreeSet<Engine>) -> BTreeSet<&'static str>;
pub fn missing_features(engines: &BTreeSet<Engine>, build: &BuildInfo) -> BTreeSet<&'static str>;

// ports.rs
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct Ports(pub BTreeMap<String, u16>);      // key per PortSpec.key: "native", "qdrant", "qdrant_grpc", …
pub trait PortProbe: Send + Sync { fn is_free(&self, port: u16) -> bool; }   // real: TcpListener::bind(127.0.0.1:port)
pub fn allocate(engines: &BTreeSet<Engine>, existing: &[Ports], probe: &dyn PortProbe, base: Option<u16>) -> Result<Ports, CliError>;

// stack/spec.rs
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct StackSpec { pub schema: u32 /* 1 */, pub name: StackName, pub version: String, pub variant: String, pub binary: PathBuf,
                       pub engines: BTreeSet<Engine>, pub metastore: Metastore, pub object_store: ObjectStore,
                       pub env_passthrough: Vec<String>, pub storage: Storage, pub ports: Ports, pub created_at: String }
pub enum Metastore { Embedded, Tikv { pd: Vec<String>, keyspace: String } }
pub enum ObjectStore { Local, Url(String) }      // s3://, gs://, az://, file:///
pub struct Storage { pub kind: StorageKind /* Dir | Mount | Nvme */, pub path: PathBuf, pub device: Option<PathBuf>,
                     pub cache_ram_bytes: u64, pub cache_disk_bytes: u64, pub hot_nvme_bytes: u64, pub hot_ram_bytes: u64 }
impl StackSpec { pub fn load(path: &Path) -> Result<Self, CliError>; pub fn save(&self, path: &Path) -> Result<(), CliError>;
                 pub fn server_command(&self, home: &LoamHome) -> (PathBuf, Vec<std::ffi::OsString>); }
pub fn resolve(args: &CreateArgs, ctx: &Context, existing: &[StackSpec]) -> Result<StackSpec, CliError>;
```

**Semantics:**
- The registry is design §8.2's table verbatim (ports, offsets, env names). `available` is `false` for `mcp` until M1.6 Task 7 merges (Task 0), and for `live` and `jobs` until their plans merge; requesting an unavailable engine is `engine_unsupported` with the hint naming the plan.
- `parse_engines`: comma-separated, trimmed, case-sensitive lower-case names or aliases (`elasticsearch` → `es`); unknown → `usage` with the list of names; `postgres` → `engine_unsupported` with design §8.2's hint; duplicates collapse.
- `missing_features` non-empty → `feature_not_in_variant` (exit 6) naming them (CLI2 replaces this with a variant download).
- `server_command`: `object_store = Local` → `[binary, "dev", "--data-dir", <stack>/data, "--listen", 127.0.0.1:<native>]`; else `[binary, "standalone", "--bucket", <url>, "--data-dir", <stack>/data, "--listen", …]`; then per engine its listen flags in registry order, then each `on_by_default` engine that was not requested gets its `off_flag`, then storage flags (`--cache-ram-bytes`, `--cache-dir` and `--cache-disk-bytes` when `cache_disk_bytes > 0`, `--hot-dir <path>/hot`, `--hot-nvme-bytes`/`--hot-ram-bytes` when non-zero), then `--meta tikv://…` for `Metastore::Tikv`. Deterministic order (golden files).
- `allocate`: if `existing` is empty and every default port is free, the defaults; else the first base in `20000, 20100, …, 29900` (or `base`) where `base + offset` is free for every needed port and not in any `existing` stack's ports; none → `port_in_use`.
- `resolve`: name taken → `stack_exists`; more than 8 stacks → `too_many_stacks`; `--storage` parsed (`dir:PATH`, `mount:PATH`, `nvme:DEVICE`; default `dir:<stack dir>`), sizing per design §10.3 using `statvfs` on the path (`nvme:` is resolved in Task 7; until then it is `usage` with the message `nvme: storage is not available yet; use dir: or mount:`, which Task 7 replaces); `--metastore raft` = `embedded`; `tikv://` requires feature `tikv`.
- `stack create --no-start` saves `stack.toml` (via `write_atomic`, 0600) and renders the spec; `stack list` renders `{stacks: [{name, state: "stopped", engines, created_at}]}` (state from Task 5 later); `stack describe` renders design §8.3's object with `state: "stopped"`.

**Tests:**
- `tests/engines.rs`: `registry_matches_the_design_table` (names, features, ports, env names, order); `every_in_binary_feature_exists_in_operon` (reads `crates/operon/Cargo.toml`; `mcp`, `live`, `jobs` allowed missing while `available: false`); `parse_engines_accepts_aliases_and_implies_native`; `postgres_is_unsupported_with_the_pg_hint`; `unknown_engine_is_usage_with_names`; `missing_features_against_build_info`.
- `tests/ports.rs`: `first_stack_gets_default_ports`; `taken_default_moves_to_the_first_free_block`; `second_stack_never_reuses_ports`; `explicit_base_is_used_or_refused`; `no_block_free_is_port_in_use`.
- `tests/spec.rs`: `stack_toml_round_trips`; `unknown_schema_is_refused` (Ruling 5); `server_args_golden_dev_default`, `…_standalone_s3`, `…_pg_and_durable`, `…_mount_storage`, `…_tikv_metastore` (against `tests/golden/args/*.txt`); `too_many_stacks_is_refused`; `existing_name_is_stack_exists`.
- `crates/operon/tests/cli/main.rs`: `generated_server_args_parse_with_the_real_cli` (for each golden, `Cli::try_parse_from` the argv minus the binary → `Ok`; the test is `#[cfg]`-aware: goldens needing `pgwire`/`tikv` run only with those features).

**Commit:** `cli: add the engine registry, port allocation and stack specs`.

### Task 5: The supervisor: `run`, `start`, `stop`, `restart`, `logs`, `delete`

**Files:** `crates/operon-cli/src/stack/{supervise.rs,health.rs,logs.rs,mod.rs}`, `crates/operon/tests/cli/{stack.rs,ports.rs}`. **PR size:** about 900 lines.

**Produces:**

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, schemars::JsonSchema)] #[serde(rename_all = "snake_case")]
pub enum StackState { Running, Starting, Unhealthy, Stopped, Crashed }
pub trait Spawner: Send + Sync {
    fn spawn_detached(&self, program: &Path, args: &[OsString], log: &Path) -> std::io::Result<u32>;   // process_group(0), stdin null
    fn is_alive(&self, pid: u32) -> bool;                       // kill(pid, None)
    fn signal_group(&self, pgid: u32, sig: nix::sys::signal::Signal) -> std::io::Result<()>;
}
pub async fn start(ctx: &Context, name: &StackName, wait: Duration) -> Result<StackStatus, CliError>;
pub async fn stop(ctx: &Context, name: &StackName, grace: Duration) -> Result<StackStatus, CliError>;
pub fn run_foreground(ctx: &Context, name: &StackName) -> Result<std::convert::Infallible, CliError>;  // exec
pub async fn state(ctx: &Context, spec: &StackSpec) -> StackState;
pub async fn delete(ctx: &Context, name: &StackName, keep_data: bool) -> Result<DeleteReport, CliError>;
#[derive(serde::Serialize, schemars::JsonSchema)] pub struct StackStatus { /* design §8.3's describe object */ }
```

**Semantics** (design §8.3):
- `start`: take `DirLock` (`lock`); if alive → `stack_running` (exit 5) unless already `Running` (then return the status, idempotent); re-probe ports (Ruling 6); rotate `logs/server.log` if ≥ 64 MiB (`.1` … `.3`); spawn `current_exe() stack run --name <n>` (so `stack run` does the exec); write `server.pid`; poll `GET http://127.0.0.1:<native>/ready` every 200 ms until 200 (then, per Task 0, each engine port accepts TCP) or `wait` → `timeout` (exit 7) with `details.log_tail` (last 20 lines).
- `run_foreground`: load the spec, check ports, `exec` `server_command()` with `env_passthrough` kept and the rest of the environment unchanged; never returns on success.
- `stop`: SIGTERM to the group, poll `is_alive` every 100 ms up to `grace`, then SIGKILL; remove `server.pid`; stopping a stopped stack returns `Stopped` (idempotent, exit 0).
- `state`: no pid file → `Stopped`; pid dead → `Crashed`; alive and `/ready` 200 → `Running`; alive and `/ready` connect refused within 2 s of the pid file's mtime → `Starting`, otherwise `Unhealthy`.
- `restart`: `stop` then `start`. `--upgrade` rewrites `binary` and `version` to the running executable when it covers the engines, then restarts.
- `logs`: last `--lines` (default 100) lines; `--follow` tails until Ctrl-C (exit 0, not 130, for `logs --follow`); `json` mode emits `{"lines": [...]}` and refuses `--follow` (`usage`).
- `delete --yes`: stop, remove `stacks/<name>/` (`--keep-data` moves `data/` to `stacks/.kept/<name>-<ts>/data` and says where); report `{name, removed: [paths], kept: [paths], left_in_place: ["s3://… (bucket objects)", "/mnt/loam-cache (mount)"]}`.
- `list` and `describe` now report live `state`.

**Tests** (`crates/operon/tests/cli/stack.rs`, real binary, tempdir `LOAM_HOME`, unique port bases):
- `create_starts_and_describe_reports_running` (default engines minus `mcp` if unavailable; `GET /ready` 200 on the reported port; `QDRANT_URL` port answers Qdrant's `GET /collections`).
- `stop_then_start_is_idempotent`; `start_running_stack_returns_status`; `crashed_stack_is_reported` (kill -9 the pid → `crashed`); `port_taken_after_create_is_port_in_use` (bind the qdrant port, `start` → exit 5, `details.port`).
- `start_timeout_reports_log_tail` (a spec whose `binary` is a script that sleeps and prints a line; exit 7, tail contains the line).
- `delete_needs_yes_without_tty` (exit 3, `confirmation_required`); `delete_removes_the_directory`; `delete_keep_data_moves_data`.
- `run_execs_the_server` (`stack run` in the foreground under `assert_cmd` with a 3 s timeout: the child pid serves `/ready`).
- `logs_rotate_at_64_mib` (unit test of the rotation function with a 64 MiB+1 sparse file).
- `json_outputs_match_schemas` (`describe`, `list`, `stop`, `delete`).

**Commit:** `cli: run local stacks under a detached supervisor`.

### Task 6: `env export`, `.env.loam` and `Secret`

**Files:** `crates/operon-cli/src/{env.rs,secret.rs,project.rs}`, `crates/operon-cli/tests/env.rs`, `crates/operon/tests/cli/env.rs`, `clippy.toml`. **PR size:** about 600 lines.

**Produces:**

```rust
pub struct Secret(String);   // no Serialize, no Display; Debug = "Secret(<redacted>)"
impl Secret { pub fn new(s: String) -> Self; pub fn expose(&self) -> &str; /* disallowed-methods */ }
pub enum EnvValue { Plain(String), Secret(Secret) }
pub struct EnvSet { pub stack: StackName, pub vars: Vec<(String, EnvValue)> }   // registry order, LOAM_STACK first
pub fn env_for(spec: &StackSpec) -> EnvSet;                                       // registry EnvSpec templates; secrets: none in CLI1
pub fn render_dotenv(set: &EnvSet, header: &str) -> String;                        // the only caller of expose() besides http.rs
pub fn write_env_file(path: &Path, set: &EnvSet, now: &str, version: &str) -> Result<(), CliError>;   // 0600, atomic
pub fn merge_env_block(existing: &str, set: &EnvSet) -> String;                    // marker block, idempotent
pub fn redacted(set: &EnvSet) -> BTreeMap<String, String>;                         // secrets → "<redacted: in .env.loam>"
pub fn ensure_gitignored(project: &Path, file: &str, git: &dyn GitProbe) -> Result<GitignoreAction, CliError>;
pub fn project_dir(explicit: Option<&Path>, env: &dyn Env, cwd: &Path) -> Option<PathBuf>;   // Ruling 7
```

**Semantics:** design §11.2 exactly: header line, `LOAM_STACK`, the engines' variables in registry order, and the D111 comment line when no secret exists. `env export` defaults to `dotenv` on stdout (`RawOutput`); `--format json` (or `--output json`) gives `{"stack", "variables": {…}}` with **secrets redacted** (the JSON form is what agents parse; humans who want the value use dotenv); `--format shell` gives `export K='v'` lines with single-quote escaping. `--write PATH` writes the file (0600) and prints `{path, written: [names]}`; `--merge PATH` rewrites only the marker block (creating the file 0600 if absent); both call `ensure_gitignored` unless `--no-gitignore`. `stack create` (Task 5's flow) calls `write_env_file(<project>/.env.loam)` when `project_dir` finds a project (Ruling 7) and prints the path.

**Tests:** `secret_debug_is_redacted`; `secret_is_not_serializable` (a `compile_fail` doc test: `serde_json::to_string(&Secret::new(..))`); `dotenv_golden_default_stack`; `dotenv_golden_with_pg`; `env_file_is_0600_and_atomic`; `merge_inserts_block_once_and_updates_in_place` (run twice, file identical; lines outside untouched); `merge_handles_missing_trailing_newline`; `shell_format_escapes_quotes`; `json_format_redacts_secrets` (a synthetic `EnvSet` with a secret); `gitignore_appended_when_not_ignored` (fake `GitProbe`); `gitignore_untouched_when_already_ignored`; `no_git_no_gitignore_change`; `project_dir_resolution_order` (explicit > `CLAUDE_PROJECT_DIR` > marker ancestor > none, never `$HOME`). End-to-end: `stack_create_in_a_project_writes_env_loam`; `env_export_merge_into_dot_env`.

**Commit:** `cli: export stack endpoints to .env.loam`.

### Task 7: `storage inspect`, `storage prepare` and NVMe stacks

**Files:** `crates/operon-cli/src/storage/{mod.rs,lsblk.rs,plan.rs,exec.rs}`, `crates/operon-cli/src/stack/resolve.rs`, `crates/operon-cli/tests/storage.rs`, `crates/operon-cli/tests/fixtures/{lsblk,blkid}/*`, `.github/workflows/ci.yml` (job `cli-nvme`). **PR size:** about 900 lines.

**Produces:**

```rust
#[derive(serde::Deserialize)] pub struct Lsblk { pub blockdevices: Vec<BlockDevice> }   // lsblk --json --bytes -o NAME,PATH,TYPE,SIZE,FSTYPE,MOUNTPOINTS,PKNAME,MODEL,SERIAL,ROTA,RO
#[derive(serde::Deserialize, Clone)] pub struct BlockDevice { pub name: String, pub path: PathBuf, #[serde(rename = "type")] pub kind: String,
    pub size: u64, pub fstype: Option<String>, pub mountpoints: Vec<Option<String>>, pub pkname: Option<String>, pub model: Option<String>,
    pub serial: Option<String>, pub rota: bool, pub ro: bool, #[serde(default)] pub children: Vec<BlockDevice> }
pub struct Blkid(BTreeMap<String, String>);   // blkid -p -o export
pub struct PrepareOptions { pub device: PathBuf, pub mount: PathBuf /* /mnt/loam-cache */, pub fs: Fs /* Ext4 | Xfs */, pub wipe: bool,
                            pub allow_virtual: bool, pub allow_hdd: bool, pub fstab: bool, pub owner: Option<(u32, u32)> /* SUDO_UID/GID */ }
pub enum Step { Mkfs { argv: Vec<String> }, Mkdir(PathBuf), Mount { argv: Vec<String> }, AppendFstab { line: String },
                Chown { path: PathBuf, uid: u32, gid: u32 }, MkdirOwned(PathBuf) }
pub enum Refusal { NotBlockDevice, Virtual, Mounted(String), SystemDevice(String), Member(String), ReadOnly, TooSmall(u64),
                   HasSignature(String), Rotational, NotFound }
pub fn plan_prepare(lsblk: &Lsblk, blkid: &Blkid, fstab: &str, opts: &PrepareOptions) -> Result<Vec<Step>, Refusal>;
pub fn inspect(ctx: &Context, device: Option<&Path>) -> Result<InspectReport, CliError>;   // read-only
pub fn execute(ctx: &Context, steps: &[Step]) -> Result<PrepareReport, CliError>;            // std::process::Command per step, no shell
```

**Semantics:** design §10.2 exactly. `Refusal` maps to error codes: `NotFound` → `device_not_found` (4); `Mounted`, `SystemDevice`, `Member` → `device_in_use` (5); `HasSignature` → `device_has_filesystem` (5) with `details.signature`; the rest → `device_in_use` with `details.reason`. Non-root → `root_required` (exit 3) before any step runs (`storage inspect` works without root). macOS and others → `platform_unsupported` (6). The confirmation: TTY → type the device path exactly; else `--yes`. `mkfs.ext4 -F -L loam-cache -m 0 -E lazy_itable_init=1,discard <dev>`; XFS: `mkfs.xfs -f -L loam-cache <dev>`. Fstab line `UUID=<uuid> <mount> <fs> noatime,nofail 0 2`, appended only if no line has that UUID or mount point; the UUID is read with `blkid -s UUID -o value` after mkfs. `stack create --storage nvme:/dev/X`: device mounted → its mount point (`<mp>/loam/<stack>`, writable check); not mounted → `sudo_step_required` (exit 3) whose `hint` is the two exact commands of design §10.2; running as root → `root_required` with "run `storage prepare` under sudo, then `stack create` as yourself". `mount:/path` must exist and be writable. `inspect` lists disks with `{path, size_bytes, model, rotational, mounted, fstype, eligible, reason}`.

**Tests** (`tests/storage.rs`, fixtures captured from real `lsblk`/`blkid` output and committed):
- Refusals, one fixture each: `refuses_a_mounted_device`; `refuses_the_root_disk` (a partition mounted at `/`); `refuses_swap`; `refuses_lvm_member`; `refuses_a_read_only_device`; `refuses_under_16_gib`; `refuses_existing_filesystem_without_wipe`; `refuses_loop_without_allow_virtual`; `refuses_rotational_without_allow_hdd`; `device_not_found`.
- `plans_ext4_happy_path` (exact steps and argv); `plans_xfs`; `fstab_line_not_duplicated`; `no_fstab_skips_append`; `owner_from_sudo_uid`.
- `nvme_storage_on_mounted_device_uses_its_mount`; `nvme_storage_unmounted_prints_the_sudo_step` (exit 3, hint text exact); `prepare_without_root_is_root_required`; `prepare_needs_yes_without_tty`.
- CI job `cli-nvme` (path-filtered on `crates/operon-cli/src/storage/**` and `crates/operon/src/main.rs`): `truncate -s 20G $RUNNER_TEMP/disk.img`, `sudo losetup -f --show`, `sudo operon storage prepare --device /dev/loopN --mount $RUNNER_TEMP/mnt --allow-virtual --no-fstab --yes -o json`, then `operon stack create --name nv --storage mount:$RUNNER_TEMP/mnt -o json`, write and read a collection over the native API, assert `find $RUNNER_TEMP/mnt/loam/nv/cache -type f | head -1` is non-empty, `stack delete --yes`, `sudo umount`, `sudo losetup -d`.

**Commit:** `cli: prepare NVMe disks for the cache and run stacks on them`.

### Task 8: Embedded docs, tested snippets, `docs search` and `docs snippet`

**Files:** `crates/operon-cli/{build.rs,src/docs/{mod.rs,bundle.rs,index.rs,snippet.rs}}`, `crates/operon-cli/tests/docs.rs`, `docs/snippets/**` (36 files: `{native,qdrant,es,pg}` × `{python,typescript,shell}` × `{connect,upsert,search}`), `docs/guides/cli.md` (stub, filled in Task 11), `scripts/snippets/run.sh`, `.github/workflows/ci.yml` (job `snippets`). **PR size:** about 900 lines, plus the snippets.

**Produces:**

```rust
pub struct Bundle { pub version: String, pub docs: Vec<Doc>, pub snippets: Vec<Snippet> }
pub struct Doc { pub path: String /* "guides/mcp.md" */, pub title: String, pub sections: Vec<Section /* {heading, anchor, body} */> }
#[derive(Clone, serde::Serialize, schemars::JsonSchema)]
pub struct Snippet { pub engine: Engine, pub language: Language, pub task: Task, pub code: String, pub packages: Vec<String>,
                     pub env: Vec<String>, pub doc_url: String, pub tested_with: String /* the bundle version */ }
pub enum Language { Python, Typescript, Rust, Shell }
pub enum Task { Connect, CreateCollection, Upsert, Search, HybridSearch, Sql }
pub fn bundle() -> &'static Bundle;                         // decoded once from include_bytes!(concat!(env!("OUT_DIR"), "/docs.tar.zst"))
pub fn search(query: &str, limit: usize) -> Result<Vec<Hit>, CliError>;   // in-RAM tantivy, built once (OnceLock)
pub fn snippet(engine: Engine, language: Language, task: Task) -> Result<&'static Snippet, CliError>;
```

**Semantics:**
- `build.rs` walks `docs/guides/**/*.md` and `docs/snippets/<engine>/<language>/<task>.<ext>` (ext: `py`, `ts`, `rs`, `sh`), rejects a snippet file whose path components are not valid names or whose front matter (the leading comment block: `# packages: …`, `# env: QDRANT_URL, …`, `# doc: guides/qdrant.md#connect`) is missing or names a variable that is not in the engine registry's env list, and writes `OUT_DIR/docs.tar.zst` (zstd level 19). Snippets never contain a URL literal: a `http://`/`grpc://`/`postgres://` substring fails the build (they read `os.environ`, `process.env`, `std::env` or `$VAR`).
- Section splitting at `##`/`###` headings; `search` indexes `title` (boost 2) and `body` with tantivy's `en_stem` tokenizer; a hit is `{title, path, section, excerpt (≤ 300 chars around the first match), url: "https://loams.dev/docs/<path>#<anchor>"}` (the URL form is documented; Q281 decides the host). Results capped at 32 KiB.
- `docs search QUERY` and `docs snippet --engine --language [--task]` render the same structures; table mode prints the code with a language header.
- `scripts/snippets/run.sh <operon-binary>`: starts a stack with engines `native,qdrant,es,pg` (pg only if the binary has `pgwire`), runs `operon env export --write $W/.env.loam`, sources it, and runs every snippet: Python ones in a `uv` venv with the in-repo SDK when published packages are missing, TypeScript with Node 22 type stripping, shell with `bash -euo pipefail`; each snippet exits non-zero on a wrong result. Job `snippets` runs it on PRs touching `docs/snippets/**`, `crates/operon/src/api/**`, `crates/operon-qdrant/**`, `crates/operon-es/**`, `crates/operon/src/pg/**`.

**Tests:** `bundle_contains_every_snippet_file`; `snippet_front_matter_is_validated` (`build.rs` logic is in `src/docs/bundle.rs::validate`, unit-tested on strings); `snippet_without_url_literals` (the same validator); `search_finds_the_mcp_guide_for_claude_code`; `search_limit_and_cap`; `missing_snippet_is_usage_listing_what_exists`; `docs_json_matches_schemas`. CI: the `snippets` job.

**Commit:** `cli: embed the docs and tested SDK snippets`.

### Task 9: `mcp serve` over stdio

**Files:** `crates/operon-cli/src/mcp/{mod.rs,server.rs,tools.rs}`, `crates/operon-cli/tests/mcp_tools.rs`, `crates/operon/tests/cli/mcp.rs`. **PR size:** about 1 000 lines.

**Produces:**

```rust
#[derive(Clone)] pub struct LoamBootstrap { ctx: Arc<Context>, project: Option<PathBuf>, tool_router: rmcp::handler::server::router::tool::ToolRouter<Self> }
#[rmcp::tool_router] impl LoamBootstrap {
  #[tool(name = "search_docs", description = "Search Loam's documentation for this installed version.", annotations(read_only_hint = true, idempotent_hint = true, open_world_hint = false))]
  async fn search_docs(&self, Parameters(SearchDocsArgs { query, limit }): Parameters<SearchDocsArgs>) -> Result<CallToolResult, rmcp::ErrorData>;
  #[tool(name = "get_sdk_snippet", …read-only…)] async fn get_sdk_snippet(&self, Parameters<SnippetArgs>) -> …;
  #[tool(name = "loam_info", …read-only…)] async fn loam_info(&self) -> …;
  #[tool(name = "stack_status", …read-only…)] async fn stack_status(&self, Parameters<StackStatusArgs>) -> …;
  #[tool(name = "stack_create", description = "Create and start a local Loam stack on this machine (loopback only). Never formats disks or downloads anything.", annotations(read_only_hint = false, destructive_hint = false, idempotent_hint = false, open_world_hint = false))]
  async fn stack_create(&self, Parameters<StackCreateArgs>) -> …;
  #[tool(name = "stack_start", annotations(read_only_hint = false, destructive_hint = false, idempotent_hint = true))] async fn stack_start(&self, Parameters<StackNameArgs>) -> …;
  #[tool(name = "env_export", description = "Write a stack's connection settings to .env.loam in the project. Secrets are written to the file and never returned.", annotations(read_only_hint = false, destructive_hint = false, idempotent_hint = true))]
  async fn env_export(&self, Parameters<EnvExportArgs>) -> …;
}
pub async fn serve_stdio(ctx: Context, project: Option<PathBuf>) -> Result<(), CliError>;   // rmcp::transport::stdio()
```

Argument structs (`serde` + `schemars::JsonSchema`, `deny_unknown_fields`): `SearchDocsArgs { query: String, limit: Option<u8> /* 1..=20, default 5 */ }`; `SnippetArgs { engine: Engine, language: Language, task: Option<Task> }`; `StackStatusArgs { name: Option<String> }`; `StackCreateArgs { name: String, engines: Vec<String>, storage: Option<String> /* "dir:…" | "mount:…" */, start: Option<bool> /* true */, allow_download: Option<bool> /* false; CLI1 has no downloads → must be false or absent */ }`; `StackNameArgs { name: String }`; `EnvExportArgs { name: String, path: Option<String> /* ".env.loam" */, merge_into: Option<String> }`.

**Semantics:** design §12.1 exactly.
- `serverInfo` `{name: "loams", version: build.version}` (the rename changes nothing here: the server already says `loams`); instructions text: one paragraph telling the model that the stack's data tools are on the `loams-<stack>` server and that destructive actions are for the user to run.
- Protocol versions as M1.6 Ruling 7 (stateless 2026-07-28 and legacy `initialize`); rmcp's stdio transport; no `Mcp-Session-Id` concept on stdio. Logging: a `tracing` subscriber to stderr at `warn` and to `<home>/logs/mcp.log` at `info` (never stdout).
- Results: `CallToolResult::structured(json)`; errors `CallToolResult::structured_error({"error": code, "message", "hint"})` with design §6.3 codes; argument deserialization failures are rmcp `invalid_params`.
- `stack_create`: Ruling 8's limits; `storage` with `nvme:` → `usage` ("only a person formats disks: ask the user to run `operon storage prepare`"); `allow_download: true` → `usage` in CLI1 (no downloads yet); object store forced `local`, metastore `embedded`; then the same `stack::create` + `start` path as the CLI; writes `.env.loam` per Ruling 7 using the server's `project`.
- `env_export`: `path` and `merge_into` are joined to `project`, canonicalized (the parent must exist), must stay under `project` after resolving symlinks, and the file name must start with `.env`; otherwise `usage` with "paths must stay inside <project>". Returns `{path, written, values}` with `redacted()` values. No project → `usage` ("start the agent in a project directory or pass --project-dir").
- `stack_status` and `loam_info` never call the network (update info only from `cache/update-check.json`, absent in CLI1 → `null`).
- `mcp tools` (CLI command) prints the tool list with input schemas (from the router) — the same JSON the server lists.

**Tests:**
- `tests/mcp_tools.rs` (direct handler calls on `Context::for_test`): one success and one error test per tool; `stack_create_refuses_nvme_and_downloads`; `stack_create_forces_local_and_embedded`; `env_export_refuses_paths_outside_the_project`; `env_export_refuses_symlink_escape`; `env_export_refuses_non_env_file_names`; `tool_list_is_frozen` (names, argument names and annotations against `tests/golden/mcp-tools.json`, Ruling 12).
- `crates/operon/tests/cli/mcp.rs` (rmcp client over `TokioChildProcess` running `operon mcp serve --project-dir <tmp>`): `lists_seven_tools_without_initialize` (2026-07-28 discover mode); `legacy_initialize_works` (2025-11-25); `server_info_reports_the_binary_version`; `stdout_carries_only_json_rpc` (raw pipe read: every line parses as JSON-RPC); `agent_creates_a_stack_and_exports_env` (`stack_create` → `stack_status` running → `env_export` → `.env.loam` exists 0600, tool output has no value marked secret); **`canary_never_appears_in_any_tool_output_or_log`** (seed `stacks/<n>/secrets/bootstrap` and a synthetic `EnvSet` secret with `loam_cnry_9f2b0c7e1d4a…`; call every tool with valid and invalid args and every CLI command with `-o json`; scan stdout, stderr, `mcp.log`, `server.log`).

**Commit:** `cli: serve the bootstrap MCP tools over stdio`.

### Task 10: `mcp install` and `mcp uninstall`

**Files:** `crates/operon-cli/src/mcp/{install.rs,agents/{claude_code.rs,codex.rs,cursor.rs,windsurf.rs}}`, `crates/operon-cli/tests/agents.rs`, `crates/operon-cli/tests/fixtures/agents/**`, `crates/operon-cli/tests/golden/agents/**`. **PR size:** about 900 lines.

**Produces:**

```rust
pub enum Agent { ClaudeCode, Codex, Cursor, Windsurf }
pub enum Scope { User, Project, Local }   // Local only for claude-code
pub struct Entry { pub name: String /* "loams" | "loams-<stack>" */, pub transport: Transport }
pub enum Transport { Stdio { command: PathBuf, args: Vec<String> }, Http { url: String } }
pub trait AgentConfig {
    fn cli(&self) -> Option<&'static str>;                                    // "claude" | "codex" | None
    fn cli_add_argv(&self, entry: &Entry, scope: Scope) -> Option<Vec<String>>;
    fn cli_remove_argv(&self, name: &str, scope: Scope) -> Option<Vec<String>>;
    fn config_path(&self, scope: Scope, home: &Path, project: Option<&Path>) -> Result<PathBuf, CliError>;
    fn upsert(&self, original: &str, entries: &[Entry]) -> Result<String, CliError>;   // pure; preserves everything else
    fn remove(&self, original: &str, names: &[String]) -> Result<String, CliError>;
}
pub struct InstallPlan { pub agent: Agent, pub actions: Vec<Action /* RunCli(argv) | EditFile { path, before, after } | PrintAndExit3 { command } */> }
pub fn plan_install(ctx: &Context, agent: Agent, scope: Scope, stack: Option<&StackSpec>, no_data: bool, protect_env: bool) -> Result<InstallPlan, CliError>;
pub fn apply(ctx: &Context, plan: &InstallPlan, dry_run: bool) -> Result<InstallReport, CliError>;
```

**Semantics:** design §12.2 table and rules, Ruling 13.
- Entries: `loams` → `Stdio { command: <abs current_exe>, args: ["mcp", "serve"] }` (plus `--stack <n>` when `--stack` is given); `loams-<stack>` → `Http { url: "http://127.0.0.1:<mcp>/mcp" }` when the stack has the `mcp` engine and `--no-data` is absent.
- File formats: Claude Code `.mcp.json` (stdio: `{"command", "args"}`; http: `{"type": "http", "url"}`); Codex TOML (`[mcp_servers.<name>]` with `command`/`args` or `url`); Cursor JSON (`{"type": "stdio", "command", "args"}`; http `{"url"}`); Windsurf JSON (`{"command", "args"}`; http `{"serverUrl"}`).
- JSON edited with `serde_json` (`preserve_order`) as a `Value`; the file's top-level key order, every non-Loam entry and unknown top-level keys survive; indentation 2 spaces, trailing newline. TOML through `toml_edit::DocumentMut`, preserving comments. An input that does not parse → `config_unparseable` (exit 5), file untouched.
- Before the first edit of a file: copy to `<file>.loam-backup` (only if no backup exists). Writes atomic, mode preserved (new files 0600 for user scope, 0644 for project scope).
- Records `{agent, scope, path_or_cli, entries, at}` in `mcp-installs.json`; `uninstall` uses it (and, for CLI-based installs, `claude mcp remove <name> --scope <s>` / `codex mcp remove <name>`).
- `--protect-env` (Claude Code, project scope): adds `"Read(./.env.loam)"` to `.claude/settings.json` `permissions.deny` (creating the array), with the same preservation rules; interactively, `install` asks.
- `--dry-run`: renders `actions` (argv, or a unified diff via `similar`) and writes nothing.

**Tests:** goldens per agent × scope: `claude_code_project_first_install`, `…_reinstall_is_identical`, `…_preserves_other_servers`, `…_unparseable_file_is_not_written`; `claude_code_uses_cli_when_on_path` (a fake `claude` script on a test `PATH` records argv; expect the two `mcp add` calls exactly as design §12.2); `claude_code_user_scope_without_cli_exits_3`; the same four goldens for `codex` (TOML, comments preserved), `cursor` (user and project) and `windsurf`; `codex_uses_cli_when_on_path`; `http_entry_skipped_without_mcp_engine`; `absolute_binary_path_is_written`; `backup_made_once`; `uninstall_removes_only_loam_entries`; `dry_run_writes_nothing`; `protect_env_adds_deny_rule_once`.

**Commit:** `cli: install the MCP servers into Claude Code, Codex, Cursor and Windsurf`.

### Task 11: `init`, the guide and the design amendments

**Files:** `crates/operon-cli/src/init.rs`, `crates/operon/tests/cli/init.rs`, `docs/guides/cli.md`, `docs/guides/mcp.md` (cross-link; created by M1.6 Task 9, or created here as a stub if M1.6 has not merged), `docs/design/30-loam-cli.md` (status of §19's CLI1 row; as-built notes), `docs/design/10-operations.md` (§1: a pointer to `operon stack` for laptops), `CHANGELOG.md`. The plans README and decision-log rows were integrated on 2026-10-02. **PR size:** about 500 lines plus docs.

**Semantics:**
- `init` (interactive): (1) profile `default` with `endpoint = "local"` if absent; (2) unless a stack exists, `stack create --name dev` with `--engines` chosen from a multi-select of available engines (default the registry's defaults) and storage `dir:` (if `storage inspect` finds an eligible, unmounted NVMe disk it prints the `sudo operon storage prepare …` line, never runs it); (3) `.env.loam` in the project directory if there is one; (4) detect agents (Ruling 13 and design §12.2) and offer `mcp install` for each. Non-interactive: `init --yes [--stack NAME] [--engines LIST] [--agent A]… [--no-agent] [--no-env]` does the same without prompts; with neither a TTY nor `--yes`, `confirmation_required`. Output: `{profile, stack: <describe>, env_file, agents: [<install report>]}`.
- `docs/guides/cli.md`: install (from source until CLI2), `init`, stacks, engines table (design §8.2), `.env.loam` and `--merge`, NVMe (`storage prepare` under sudo), MCP (`mcp install`, the two entries, the bootstrap tools and what is deliberately missing), the output and exit-code contract (tables from design §6), troubleshooting (`port_in_use`, `stack logs`), and the security notes (loopback, no auth until the auth plan, `.env.loam` and agents' file access).

**Tests:** `init_yes_creates_profile_stack_env_and_installs` (fake `claude` on `PATH`; asserts the stack is running, `.env.loam` exists, the fake recorded two `mcp add` calls); `init_without_tty_or_yes_is_confirmation_required`; `init_twice_reuses_the_stack`; `init_never_formats_a_disk` (an lsblk fixture with an eligible disk: output contains the sudo line, no `Step` executed).

**Commit:** `cli: add init and the CLI guide`.

## Rulings made during execution

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| — | (Task 0 fills this table) | | |

## Self-review

| Design §30 requirement | Task |
|---|---|
| D281 one binary, `operon-cli` crate, server commands unchanged | 1 |
| D282 the command tree (CLI1 part: init, configure, stack, storage, env, mcp, docs, version, completions; `keys`, `login` and `logout` stubs exiting 6) | 1 (the stubs and `auth_stubs_exit_6`), 2, 4–11 |
| D283 output, errors, exit codes, prompts | 1 (and every task's schema tests) |
| D284 `LOAM_HOME`, profiles, no telemetry | 2 (update check: CLI2) |
| D285 stacks, engine registry, `pg` vs `postgres`, loopback | 4, 5 |
| D287 NVMe and the H1 flags | 3, 7 |
| D288 secrets, `.env.loam`, `LOAM_API_KEY` naming | 6, 9 (canary) |
| D289 bootstrap MCP tools and safety rules | 9 |
| D290 `mcp install` for four agents | 10 |
| D291 embedded docs and tested snippets | 8 |
| D295 keys wait for auth (stub) | 1 |
