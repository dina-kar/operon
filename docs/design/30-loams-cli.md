# 30 — Loams CLI, Installer and Agent Bootstrap

Status: **Approved** (owner defaults, 2026-10-02: "do suggested for all") · 2026-10-01. The direction is the owner's. On 2026-10-01 the owner asked to fold the first draft in `chatdump.md` (an earlier chat) into the design docs, the decision log and the plans. The direction from that draft is: a one-line installer (`curl -fsSL https://loams.dev/install.sh | sh`); one `loams` binary that is both the CLI and an MCP server (`loams mcp serve`); AWS-style commands (`configure`, `stack create|describe|list|delete`, `keys`, `env export`, `mcp install --agent …`, `pkg add`, `self-update`); prebuilt feature-set variants instead of building on the user's laptop; NVMe set up for the foyer cache; secrets kept out of agent transcripts by writing them to `.env.loams`; and cargo-dist for releases. This document turns that direction into decisions **D281–D299** and open questions **Q281–Q294**. Every choice that goes beyond the direction (names, formats, safety rules, phasing) is a **proposal** until the owner confirms it. **No code is written by this document.**

Markers: **(verified 2026-10-01)** means checked against a primary source on that date (§23 lists the sources). **(verify)** means the plan that builds it checks it first. **(estimate)** means computed, not measured. Paths of the form `crates/…` point at `main` at `9eaddae` (2026-10-01).

**Naming.** This document writes `loams` for the binary and its commands, following the owner's rulings of 2026-10-01 and 2026-10-02 (D400, D401, D406): the binary is `loams` (answering Q284), packages are `loams` on crates.io, PyPI and npm (scope `@loams`; `loamdb` from D33 is superseded), Go modules are `loams.dev/...`, the domain is `loams.dev` and the repository becomes `ostrium-labs/loams`. The crates (`loams`, `loams-server`, `loams-cli`) follow the same namespace. The local state directory is `LOAMS_HOME` (default `~/.loams`), the variables are `LOAMS_*` and the env file is `.env.loams` (D407). Nothing is published before the repository moves to `ostrium-labs/loams` (D33, D406).

---

## 1. Summary

| # | Decision | Status |
|---|---|---|
| D281 | **One binary.** The `loams` binary holds the server commands (today's `loams dev`, `standalone`, `cluster`, `warm`, `durable`, unchanged), the client CLI and a stdio MCP server. The client CLI is a new library crate, `loams-cli`, with one module per command group, flattened into the binary's clap tree. It is not one crate per group (§4) | Approved (owner defaults, 2026-10-02) |
| D282 | **The command tree is `loams <group> <verb>`**, in the AWS CLI's shape, with one set of flag names across every group (§5) | Approved (owner defaults, 2026-10-02) |
| D283 | **An output and exit-code contract.** `--output table\|json\|text` (default `table`, also set by `LOAMS_OUTPUT` or the profile). In `json` mode stdout holds exactly one JSON document and errors go to stderr as one JSON object. There are ten stable exit codes (0–9, plus 130) and stable snake_case error codes. Prompts never appear without a TTY. Destructive commands need `--yes` (§6) | Approved (owner defaults, 2026-10-02) |
| D284 | **Local state lives in `LOAMS_HOME`** (default `~/.loams`): `bin/`, `config.toml` (profiles), `credentials.toml` (0600), `stacks/<name>/`, `variants/`, `receipt.json`. **The CLI sends no telemetry.** The only automatic network call is a daily update check, which can be turned off and never runs in `json` mode or under `mcp serve` (§7) | Approved (owner defaults, 2026-10-02) |
| D285 | **A stack is a supervised local server process**, described by `stack.toml`. An **engine registry** maps each engine to a cargo feature, server flags, a port and environment variables (§8.2). Engines that were not asked for are switched off with the server's existing `--no-*` flags. **`pg` is the Postgres wire over collections** (D-PG-1, analytics). **`postgres` is OLTP Postgres, a companion service (D299)**, and is the only engine that sets `DATABASE_URL` (§8) | Approved (owner defaults, 2026-10-02) |
| D286 | **Prebuilt variants, not builds on the user's machine.** There are two server variants, `standard` and `full`, plus a client-only `cli` variant once the server split lands (D297). Release targets are Linux x86_64 and aarch64 (glibc ≥ 2.35) and macOS aarch64. `failpoints`, `cluster-tests` and `durable-mysql` are never in a variant. If the installed variant lacks an engine, `stack create` downloads the smallest variant that has it. `--from-source` is a documented escape hatch (§9) | Approved (owner defaults, 2026-10-02) |
| D287 | **NVMe without a privileged helper.** `loams storage inspect` reads the device (read-only). `sudo loams storage prepare --device … --yes` formats (ext4), mounts by UUID, writes the fstab line and hands the mount to the user. Without root, `stack create --storage nvme:/dev/…` prints the exact `sudo` command and exits 3. The H1 range cache (foyer) and the hot tier are placed on the mount through **new server flags `--cache-dir`, `--cache-disk-bytes` and `--cache-ram-bytes`** (§10) | Approved (owner defaults, 2026-10-02) |
| D288 | **Secrets never pass through MCP.** Credentials are written to `.env.loams` (mode 0600, added to `.gitignore`) and a tool returns only names and redacted values. A `Secret` type has no `Serialize` and a redacted `Debug`. A canary test calls every tool and asserts the secret never appears. The credential variable is `LOAMS_API_KEY`, the whole `loams_<key_id>_<secret>` token (D65), not a separate `LOAMS_KEY_SECRET` (§11) | Approved (owner defaults, 2026-10-02) |
| D289 | **`loams mcp serve` is a stdio bootstrap server**: `search_docs`, `get_sdk_snippet`, `loams_info`, `stack_status`, `stack_create`, `stack_start`, `env_export` and `add_package` (CLI2). **The data tools stay on the stack's own MCP endpoint** (M1.6, `127.0.0.1:8083/mcp`, D111), so they are not proxied. No tool deletes, stops, formats, mints keys or updates the binary. File writes stay inside the project directory, packages come from an allow-list, and downloads need explicit consent (§12.1) | Approved (owner defaults, 2026-10-02) |
| D290 | **`loams mcp install --agent claude-code\|codex\|cursor\|windsurf`** uses the agent's own CLI when it is on `PATH` (`claude mcp add`, `codex mcp add`) and otherwise edits the agent's config file in place. It always writes an absolute binary path, never touches entries it did not write, backs the file up before the first edit, refuses to edit a file it cannot parse, and has a `--dry-run` mode. It registers two entries: `loams` (stdio) and `loams-<stack>` (HTTP, the data tools) (§12.2) | Approved (owner defaults, 2026-10-02) |
| D291 | **The docs and SDK snippets are embedded in the binary**, built from `docs/guides/**` and a new `docs/snippets/**` tree. CI runs every snippet against `loams dev`. `search_docs` searches them with an in-memory Tantivy index. The docs therefore always match the installed binary, and the MCP server reports its version in `serverInfo` and `loams_info` (§13) | Approved (owner defaults, 2026-10-02) |
| D292 | **The release pipeline is cargo-dist 0.33** (MIT OR Apache-2.0, verified 2026-10-01). It produces GitHub Releases on `ostrium-labs/loams`, SHA-256 checksums and GitHub artifact attestations (SLSA provenance), plus **a minisign-signed release manifest**, `loams-release.json`. Nothing is published before D33's rename and the move to `ostrium-labs` (§17.1) | Approved (owner defaults, 2026-10-02) |
| D293 | **`https://loams.dev/install.sh` is a thin POSIX `sh` script** that Loams owns, published as a release asset. `loams.dev` redirects to it. The script detects the platform, picks the variant, downloads the archive, verifies its SHA-256 (and the minisign signature when `minisign` is installed), installs to `~/.loams/bin`, writes a receipt, updates `PATH`, and offers to run `loams init` (§17.2) | Approved (owner defaults, 2026-10-02) |
| D294 | **`loams self-update`** fetches the signed manifest, verifies it with the public keys embedded in the binary, checks the archive's SHA-256, smoke-tests the new binary, replaces itself atomically (`self-replace`), keeps the previous binary for `--rollback`, and never restarts stacks. It refuses installs it did not make, such as `cargo install` or a package manager (§17.3) | Approved (owner defaults, 2026-10-02) |
| D295 | **Keys, login and agents follow §19, after the unified auth plan (D111, Q30).** Keys are API keys scoped to one environment, for apps and service accounts. **Agents never receive keys** (§19 §5.5). In CLI3, `loams mcp serve` acts as an agent principal with short-lived, vended tokens (§19 P5, P6). Until then, `keys` and `login` exit 6 (`unsupported`) and `.env.loams` holds endpoints only (§15) | Approved (owner defaults, 2026-10-02) |
| D296 | **`loams pkg add` resolves logical names** (`sdk`, `bullmq`, `celery`, `durable`, `live`) to the registry names of the 2026-10-02 ruling (`loams` on npm, PyPI and crates.io, the `@loams` npm scope; D400, which supersedes D33's `loamdb`). It pins the version that matches the CLI, finds the project's package manager from its lockfile, and refuses anything outside the allow-list when it is called from MCP (§14) | Approved (owner defaults, 2026-10-02) |
| D297 | **The server split.** The server library moves from `crates/loams` into a new `loams-server` crate. `crates/loams` keeps the binary, with `loams-server` behind a default feature `server`, and re-exports it so tests are unchanged. This makes the `cli` variant possible. It lands in the PR after D33's rename, in CLI2 (§4.2) | Approved (owner defaults, 2026-10-02) |
| D298 | **Track CLI, in three plans.** CLI1: the local CLI and the stdio MCP server, built before the rename. CLI2: the release pipeline, variants, the installer, self-update, the server split and `pkg add`, at or after the rename. CLI3: keys, login, agent tokens, companions and cloud stacks, after the unified auth plan (§19) | Approved (owner defaults, 2026-10-02) |
| D299 | **Companion services**: `postgres` (Postgres 17 in a container for local stacks; Loams Postgres images once track P publishes them, D231), `tikv` (the pinned tiup playground that R1 uses), and `rustfs` (the RustFS container, D61). They run through a container runtime the CLI detects (Docker or Podman). They are never linked or bundled into the binary (§16) | Approved (owner defaults, 2026-10-02) |

## 2. Goals and non-goals

### 2.1 Goals

1. **Paste one command and get a working stack.** After `curl … | sh` and `loams init`, a laptop runs Loams with the Qdrant, Elasticsearch, Flight SQL and MCP surfaces, has a `.env.loams` its app can read, and has its coding agent connected. No compiler is needed.
2. **Agents can drive it.** Every command has a stable JSON output and a stable exit code, so a model that already knows `aws <service> <verb>` can chain commands. The bootstrap MCP tools let an agent read the docs, provision a local stack and install the SDK itself.
3. **Credentials stay out of transcripts.** No tool output, error, log line or JSON document that an agent can read contains a secret.
4. **One artifact per platform and variant**, with checksums, provenance and a signature, and a self-update that verifies all three.
5. **Buy, not build** (user memory). Use clap, cargo-dist, rmcp, Tantivy, minisign-verify, self-replace, toml_edit and the agents' own CLIs. Loams writes only the glue: the engine registry, the stack supervisor, the safety rules and the installer script.

### 2.2 Non-goals

- **No build step on `stack create`.** Compiling the server takes tens of minutes and several GB of RAM (the M0–M1 build notes). `--from-source` exists but is never the default.
- **No privileged helper or setuid binary.** Root is needed only for `storage prepare`, and the user runs that explicitly under `sudo`.
- **No proxying of the data plane's MCP tools** through the stdio server (§12.1).
- **No telemetry**, crash reporting or usage pings.
- **No Windows server.** The engine's io_uring paths (§28 §7.2) and its test matrix are Linux and macOS. Windows gets the `cli` variant after D297 (Q285), and WSL2 otherwise.
- **No cloud provisioning in CLI1 or CLI2.** Cloud stacks go through Loams Cloud's public API in CLI3 (Q291). The CLI is open source (D220) and never depends on `loam-platform` code.

## 3. How this fits what exists

| Existing | What it is today | What this document does with it |
|---|---|---|
| The `loams` binary (`crates/loams/src/main.rs`) | clap 4 derive. Subcommands `dev`, `standalone`, `cluster`, `warm` and `durable migrate`. Listener flags are in the shared `Native` group (`--flight-sql-listen`, `--no-qdrant`, `--es-listen`, `--pg-listen`, `--durable-listen`, …) | Kept unchanged as top-level commands, so CI, the crash gate and every plan that runs `loams dev` keep working. The client groups are added beside them (D281). A local stack runs `loams dev` or `loams standalone` with flags the engine registry generates (D285) |
| Cargo features of `loams` | `default = ["es", "flight", "hnsw", "qdrant"]`. Opt-in: `tikv`, `durable` (D262), `durable-tikv`, `durable-mysql` (legacy, D260), `pgwire`, `mysql-wire`, `stream-grpc`, `failpoints`, `cluster-tests`. Planned: `mcp` (M1.6, default on), `live` (R1 Task 12), `console` (§19), `jobs` (§26) | The feature matrix and the variants (D286, §9) |
| M1.6's MCP server (`loams-mcp`, planned) | HTTP, stateless MCP 2026-07-28 on its own listener `127.0.0.1:8083` (D111, M1.6 Ruling 19). Five data tools: `search`, `sql`, `memory_write`, `list_collections`, `get_documents` | Unchanged. It is the stack's data endpoint. `mcp install` registers it as `loams-<stack>` next to the stdio bootstrap server (D289, D290). Both use rmcp |
| "Your existing docs MCP server" (in the chat dump) | **Does not exist.** Neither this repository nor `loam-cloud` has one (checked 2026-10-01) | Replaced by the embedded docs bundle and `search_docs` (D291) |
| §19 console and identity | API keys scoped to one environment, which "cannot be issued to agents" (§5.5). Agents are principals with short-lived tokens obtained by federation, delegation or vending (P5, P6) | Keys belong to apps. The agent path uses vending (D295, CLI3) |
| §10 operations | Deployment modes `dev`, `standalone` and `cluster`. M1 has no config file; flags only (§10 §2) | `stack.toml` is the CLI's own record. It is translated into flags, and the server never reads it (§8.1) |
| §04 hot tier | H1 is a foyer RAM and NVMe range cache (`loams-cache::RangeCacheConfig { memory_bytes, disk: Option<DiskConfig> }`). H2 is the hot tier (`--hot-dir`, `--hot-nvme-bytes`, `--hot-ram-bytes`). **No flag sets H1's disk tier today**, so `loams dev` caches 256 MiB in RAM only | D287 adds the H1 flags, and NVMe setup points H1 and H2 at the mount |
| D111 | No auth or TLS in M1; new gateways bind loopback | Local stacks bind loopback. `keys` waits for the auth plan (D295) |
| D33 | Renamed after M1 (`loamdb`, superseded by `loams` in D400); M1 publishes nothing | CLI1 was built before the rename (D407). Releases start after the move to `ostrium-labs/loams` (D292, D406) |

## 4. The binary and its crates (D281, D297)

### 4.1 CLI1 layout

```
crates/loams-cli/                 # new library crate, no server dependencies
  src/lib.rs                       # pub enum ClientCommand (clap Subcommand), pub async fn run(ClientCommand, &Globals) -> ExitCode
  src/output.rs                    # the D283 contract: Output, Render, CliError, ErrorCode, exit codes
  src/home.rs                      # LOAMS_HOME layout, locks, atomic writes
  src/config.rs  src/configure.rs  # profiles (D284)
  src/engines.rs                   # the engine registry (D285, §8.2)
  src/stack/{mod,spec,resolve,ports,supervise,health}.rs
  src/storage/{mod,lsblk,prepare}.rs
  src/env.rs  src/secret.rs        # .env.loams and Secret (D288)
  src/docs/{mod,bundle,index,snippets}.rs   # the embedded bundle (D291)
  src/mcp/{mod,tools,install,agents}.rs     # stdio server and install (D289, D290)
  src/init.rs  src/version.rs
  build.rs                         # packs docs/guides and docs/snippets into a zstd tarball
crates/loams/src/main.rs          # gains #[command(flatten)] Client(loams_cli::ClientCommand) and the global flags
```

`loams-cli` depends on `clap`, `serde`, `serde_json`, `toml_edit`, `reqwest` (rustls), `tokio`, `rmcp` (features `server`, `transport-io`), `schemars`, `tantivy` (already in the tree through `loams-text`), `zstd`, `tar`, `comfy-table`, `dialoguer`, `fs4`, `nix` (Unix process groups and signals) and `thiserror`. It does **not** depend on `loams`, `loams-query` or any storage crate. The `stack` commands start the server as a child process and talk to it over HTTP, the same way any client does.

**Why one crate with modules and not one crate per group** (the chat dump suggested the latter): there would be thirteen small crates, all sharing the output contract, `LOAMS_HOME` and the engine registry. That means thirteen `Cargo.toml` files to keep in step and more link units on a build machine where links are the bottleneck. The groups have no independent consumers. Modules give the same separation.

### 4.2 The server split (D297, CLI2)

The `cli` variant needs a binary that does not link the engine. Today `crates/loams` is both the server library and the binary, and its server dependencies are not optional. CLI2 Task 1 moves `crates/loams/src/{server.rs, cluster.rs, meta_backend.rs, api/, pg/, mysql_wire/}` into a new library crate, `loams-server`. `crates/loams` becomes:

- `[features] default = ["server", …]`, with `server = ["dep:loams-server"]` and every engine feature forwarded (`es = ["server", "loams-server/es"]`, …);
- `lib.rs`: `#[cfg(feature = "server")] pub use loams_server::*;`, so `crates/loams/tests/**` (which use `loams::{Server, ServerConfig}` and `CARGO_BIN_EXE_loams`) do not change;
- `main.rs`: the server subcommands behind `#[cfg(feature = "server")]`. Without the feature, `loams dev` exits 6 with `this is the cli variant; install the standard variant: loams self-update --variant standard`.

The split is mechanical but touches every in-flight `crates/loams` branch (pg, mysql_wire, durable). It therefore lands right after D33's rename PR, which already invalidates every branch, and not in CLI1.

## 5. The command tree (D282)

```
loams [--output table|json|text] [--profile NAME] [--no-input] [--quiet] [-v…] [--color auto|always|never]
├── init                         [--yes] [--stack NAME] [--engines LIST] [--agent AGENT…] [--no-agent] [--no-env]
├── configure                    (interactive: endpoint, default stack, output)
│   ├── get KEY | set KEY VALUE | unset KEY | list
├── stack
│   ├── create   --name NAME [--engines LIST] [--storage dir:PATH|mount:PATH|nvme:DEVICE]
│   │            [--metastore embedded|raft|tikv://PD/KS] [--object-store local|s3://…|gs://…|az://…|file:///…]
│   │            [--variant standard|full] [--port-base PORT] [--file stack.toml] [--no-start]
│   │            [--allow-download] [--from-source] [--wait-timeout 60s]
│   ├── describe [--name NAME]
│   ├── list
│   ├── start    [--name NAME] [--wait-timeout 60s]
│   ├── stop     [--name NAME] [--grace 30s]
│   ├── restart  [--name NAME] [--upgrade]
│   ├── run      --name NAME                           (foreground; for systemd, launchd and containers)
│   ├── logs     [--name NAME] [--follow] [--lines N]
│   └── delete   --name NAME [--keep-data] --yes
├── storage
│   ├── inspect  [DEVICE]
│   └── prepare  --device DEVICE [--mount /mnt/loams-cache] [--fs ext4] [--wipe] --yes     (run with sudo; Linux only)
├── env
│   └── export   [--stack NAME] [--format dotenv|json|shell] [--write PATH] [--merge PATH] [--no-gitignore]
├── keys                                                (CLI3; exits 6 before the auth plan)
│   ├── create   [--stack NAME] [--env ENV] [--name LABEL] [--expires 90d] [--write-env PATH]
│   ├── list     [--stack NAME]
│   ├── rotate   KEY_ID [--write-env PATH]
│   └── revoke   KEY_ID --yes
├── login [--endpoint URL] | logout                      (CLI3)
├── mcp
│   ├── serve    [--stack NAME] [--project-dir PATH]
│   ├── install  --agent claude-code|codex|cursor|windsurf [--scope user|project|local] [--stack NAME] [--no-data] [--dry-run]
│   ├── uninstall --agent AGENT [--scope …]
│   └── tools                                           (lists the bootstrap tools and their schemas)
├── pkg
│   └── add      PACKAGE… [--language typescript|python|rust] [--dev] [--dry-run]       (CLI2)
├── docs
│   ├── search   QUERY [--limit 5]
│   └── snippet  --engine ENGINE --language LANG [--task TASK]
├── self-update  [--check] [--version X.Y.Z] [--variant V] [--rollback] [--yes]        (CLI2)
├── version
├── completions  bash|zsh|fish|powershell|elvish
│
├── dev | standalone | cluster | warm | durable migrate  (the server commands, unchanged)
```

Rules shared by every group:

| Rule | Detail |
|---|---|
| Names | Kebab-case flags. One spelling for each concept: `--name` is the stack in `stack *`, and `--stack` is the stack everywhere else. `--yes` confirms. `--dry-run` writes nothing. `--output` sets the format. A list value is a comma-separated string (`--engines qdrant,es`). A duration is humantime (`30s`, `90d`) |
| Default stack | `--stack` defaults to the profile's `default_stack`. That is the first stack created, unless `configure set default_stack` changes it |
| Aliases | `es` = `elasticsearch`. `raft` = `embedded` (the dev metastore is single-node openraft, §10 §1). `ls` = `list`. `rm` = `delete`. There are no other aliases |
| Help | Every command has a one-line `about` and an example in `long_about`. `loams help <cmd>` and `-h` behave as clap's do |
| Server commands | `dev`, `standalone` and the others stay exactly as they are. The CLI never adds client flags to them |

## 6. Output, errors and exit codes (D283)

### 6.1 Formats

- **`table`** (the default). Human tables (comfy-table) and key/value blocks. Colour only when stdout is a TTY and `NO_COLOR` is unset. Progress and notices go to stderr.
- **`json`**. Exactly one JSON document on stdout, followed by a newline. Keys are snake_case. A list command returns an object (`{"stacks": [...]}`), never a bare array, so fields can be added. Times are RFC 3339 UTC, sizes are integer bytes, durations are integer milliseconds with an `_ms` suffix. Nothing else is written to stdout. Notices, progress and update checks are suppressed, and logs go to stderr only with `-v`.
- **`text`**. Tab-separated rows without headers, one record per line, for `cut` and `awk` (as the AWS CLI's `text` output).
- **Default selection.** `--output`, then `LOAMS_OUTPUT`, then the profile's `output`, then `table`. The format is never inferred from whether stdout is a TTY: a script must get what it asked for.
- **Commands that emit a file format** (`env export`, `completions`) emit that format unless `--output json` is given explicitly. With `--output json`, `env export` returns `{"stack", "path", "variables": {...}}`. With `--output json`, `completions` returns `{"shell", "script"}`, where `script` is the generated completion text as one JSON string.
- **`mcp serve` always writes JSON-RPC to stdout** (§12.1). `--output` does not change its transport. A startup error before the transport opens follows §6.2.

Every JSON output type derives `schemars::JsonSchema`. The schemas are snapshotted in `crates/loams-cli/tests/schemas/*.json` and a test fails on any change that is not additive. `loams version --output json` reports `"output_schema": 1`, which is bumped only by a breaking change, announced one minor release ahead.

### 6.2 Errors

In `json` mode an error is one object on **stderr**, stdout is empty, and the exit code is set:

```json
{"error": {"code": "stack_not_found", "message": "no stack named `dev`", "hint": "run `loams stack list`", "exit_code": 4, "details": {"name": "dev"}}}
```

In `table` and `text` modes the same error prints as `error: <message>` and `hint: <hint>` on stderr.

### 6.3 Exit codes

| Code | Meaning | Error codes (stable) |
|---|---|---|
| 0 | Success | — |
| 1 | Internal error (a bug) | `internal` |
| 2 | Usage error (clap's own code) | `usage` |
| 3 | Action required: a step the CLI will not take for you | `confirmation_required`, `root_required`, `sudo_step_required`, `login_required`, `download_consent_required` |
| 4 | Not found | `stack_not_found`, `key_not_found`, `device_not_found`, `variant_not_found`, `profile_not_found` |
| 5 | Conflict | `stack_exists`, `too_many_stacks`, `port_in_use`, `stack_running`, `stack_not_running`, `config_unparseable`, `device_in_use`, `device_has_filesystem` |
| 6 | Unsupported | `engine_unsupported`, `platform_unsupported`, `auth_not_available`, `managed_install`, `feature_not_in_variant` |
| 7 | Unavailable (retryable) | `stack_unreachable`, `network`, `timeout`, `server_error` |
| 8 | Permission denied | `unauthenticated`, `forbidden` |
| 9 | Integrity failure | `checksum_mismatch`, `signature_invalid`, `manifest_invalid` |
| 130 | Interrupted (SIGINT) | `interrupted` |

### 6.4 Prompts and confirmation

- A prompt appears only if stdin and stderr are both TTYs and none of `--no-input`, `LOAMS_NO_INPUT=1` or `CI=true` is set.
- **Destructive commands** (`stack delete`, `storage prepare`, `keys revoke`) need `--yes`. Interactively they ask instead: `y/N` for delete and revoke, and for `storage prepare` the user must type the device path. Without a TTY and without `--yes`, they exit 3 with `confirmation_required`.
- A command that needs a download it was not told to make (a variant, §9.3) asks interactively. Otherwise it exits 3 with `download_consent_required` and the hint `--allow-download`.

## 7. Local state (D284)

```
$LOAMS_HOME (default ~/.loams; mode 0700)
├── bin/loams                       # the installed binary (install.sh, self-update)
├── bin/loams.prev                   # the previous binary, kept by self-update for --rollback
├── env  env.fish                   # PATH snippets, sourced from shell rc files
├── receipt.json                    # {version, variant, target, installed_at, install_method: "install.sh", source_url}
├── config.toml                     # profiles (no secrets)
├── credentials.toml                # 0600; CLI3: login tokens per profile
├── variants/<version>/<variant>/loams  # downloaded server variants (D286)
├── cache/update-check.json         # {checked_at, latest}
├── mcp-installs.json               # what `mcp install` wrote, and where (D290)
└── stacks/<name>/
    ├── stack.toml                  # the resolved spec (§8.1)
    ├── lock                        # fs4 advisory lock: one start, stop or delete at a time
    ├── server.pid
    ├── data/                       # --data-dir (metastore, local bucket) unless --object-store points elsewhere
    ├── cache/  hot/                # H1 and H2 unless --storage points elsewhere
    ├── secrets/                    # 0700; CLI3: the stack's bootstrap key (local stacks only)
    └── logs/server.log             # rotated at 64 MiB, 3 files kept
```

```toml
# config.toml
[profile.default]
endpoint = "local"        # "local" (stacks under LOAMS_HOME) or https://… (CLI3: a remote or cloud endpoint)
default_stack = "dev"
output = "table"
update_check = true
```

- **Writes are atomic**: write a temp file in the same directory, `fsync`, then `rename`. Secret files are created with mode 0600 before anything is written to them.
- **The update check** happens at most once a day, in the background of an ordinary command. It reads the release manifest (§17.1) and prints `a newer loams (0.5.0) is available: loams self-update` on stderr in `table` mode only. It never runs in `json` mode, under `mcp serve`, with `CI=true`, or with `LOAMS_NO_UPDATE_CHECK=1` or `update_check = false`. It sends no identifiers beyond what an HTTP GET of a public file carries.

## 8. Stacks (D285)

### 8.1 The stack spec

```toml
# ~/.loams/stacks/dev/stack.toml (written by `stack create`; also accepted by `stack create --file`)
name = "dev"
version = "0.4.0"                 # the binary version that created it
variant = "standard"
binary = "/home/u/.loams/bin/loams" # or ~/.loams/variants/0.4.0/full/loams
engines = ["native", "flight-sql", "mcp", "qdrant", "es"]
metastore = "embedded"            # embedded | tikv://<pd-hosts>/<keyspace>
object_store = "local"            # local | s3://… | gs://… | az://… | file:///…
env_passthrough = []              # e.g. ["AWS_PROFILE", "AWS_REGION"] for an s3:// object store

[storage]
kind = "dir"                      # dir | mount | nvme
path = "/home/u/.loams/stacks/dev" # dir: the parent of cache/ and hot/; mount, nvme: the mount point
device = ""                       # nvme only: /dev/nvme1n1 (recorded; never re-formatted)
cache_ram_bytes = 1073741824
cache_disk_bytes = 0              # 0 = RAM only (the default for dir:; §10.3)
hot_nvme_bytes = 0                # 0 = the server default
hot_ram_bytes = 0

[ports]                           # resolved at create time, then fixed
native = 8080
flight_sql = 8082
mcp = 8083
qdrant = 6333
qdrant_grpc = 6334
es = 9200
```

The server never reads `stack.toml`. **`stack run` translates it into the server's command line**, for example:

```
loams dev --data-dir ~/.loams/stacks/dev/data --listen 127.0.0.1:8080 \
  --flight-sql-listen 127.0.0.1:8082 --mcp-listen 127.0.0.1:8083 \
  --qdrant-listen 127.0.0.1:6333 --qdrant-grpc-listen 127.0.0.1:6334 --es-listen 127.0.0.1:9200 \
  --no-durable --cache-dir ~/.loams/stacks/dev/cache --cache-ram-bytes 1073741824 \
  --hot-dir ~/.loams/stacks/dev/hot
```

`object_store = "local"` runs `dev`. Anything else runs `standalone --bucket <url>`. **Every address the CLI generates is loopback** (D111). Exposing a local stack is outside the CLI: the operator runs `loams standalone` or `cluster` directly.

### 8.2 The engine registry

| Engine | Cargo feature | Server flags the stack sets | Default port | Variables in `.env.loams` | In variant |
|---|---|---|---|---|---|
| `native` (always on) | — | `--listen` | 8080 | `LOAMS_URL` | all |
| `flight-sql` | `flight` | `--flight-sql-listen` / `--no-flight-sql` | 8082 | `LOAMS_FLIGHT_SQL_URL` (`grpc://…`) | all |
| `mcp` | `mcp` (M1.6) | `--mcp-listen` / `--no-mcp` | 8083 | `LOAMS_MCP_URL` | all, once M1.6 merges |
| `qdrant` | `qdrant` | `--qdrant-listen`, `--qdrant-grpc-listen` / `--no-qdrant` | 6333, 6334 | `QDRANT_URL`, `QDRANT_GRPC_URL` (+ `QDRANT_API_KEY`, CLI3) | all |
| `es` | `es` | `--es-listen` / `--no-es` | 9200 | `ELASTICSEARCH_URL` (+ `ELASTICSEARCH_API_KEY`, CLI3) | all |
| `pg` | `pgwire` | `--pg-listen` | 15432 | `LOAMS_PG_URL` (`postgres://127.0.0.1:15432/default`) | standard, full |
| `durable` | `durable` | `--durable-listen` / `--no-durable` | 8001 | `LOAMS_DURABLE_URL` | standard, full |
| `mysql` | `mysql-wire` | `--mysql-listen` | 13306 | `LOAMS_MYSQL_URL` | full |
| `stream-grpc` | `stream-grpc` | `--stream-grpc-listen` | 8085 | `LOAMS_STREAM_GRPC_URL` | full |
| `live` | `live` (R1) | `--live-listen` | 7710 | `LOAMS_LIVE_URL` | full, once R1 merges |
| `jobs` | `jobs` (§26 J1) | `--jobs-listen` | 7720 | `LOAMS_JOBS_URL` | full, once J1 merges |
| `postgres` | — (companion, D299) | — | 5432 | **`DATABASE_URL`** | CLI3 |
| `tikv` | — (companion, D299) | `--meta tikv://…` when `--metastore tikv` | 2379 (PD) | `LOAMS_TIKV_PD` | CLI3 |
| `rustfs` | — (companion, D299) | `standalone --bucket s3://…` | 9000 | `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | CLI3 |

- **`--engines` defaults to** `native,flight-sql,mcp,qdrant,es`. `native` is implied: it is the control surface the CLI health-checks.
- **Engines that are not asked for are switched off.** Engines that are on by default in the server (`flight-sql`, `qdrant`, `es`, `durable`, `mcp`) get their `--no-*` flag. Opt-in listeners get no flag. A variant can therefore serve any subset of its engines.
- **`pg` versus `postgres`.** `pg` is D-PG-1's Postgres wire over collections: analytics and ingest, read-only by default (PG1). It must not be mistaken for an OLTP database. Its variable is `LOAMS_PG_URL`, never `DATABASE_URL`, because ORMs and frameworks treat `DATABASE_URL` as their OLTP store. `postgres` is real Postgres (D299). This resolves the chat dump's `--engines postgres,qdrant,es`: CLI1 rejects `postgres` with exit 6 (`engine_unsupported`, hint: "`pg` is Postgres-wire analytics over collections; OLTP Postgres arrives with companions in CLI3").
- **Ports.** For the first stack, each engine takes its default port if it is free. Otherwise, and for every later stack, the CLI picks the first free block of 100 ports from 20000 (`--port-base` overrides). Each port in the block is base plus a fixed offset: native +80, flight-sql +82, mcp +83, stream-grpc +85, qdrant +33/+34, es +92, pg +32, mysql +06, durable +01, live +10, jobs +20. "Free" means a test `bind` on 127.0.0.1 succeeds at create time, and the ports are written into `stack.toml`. `stack start` re-checks them and exits 5 with `port_in_use`, naming the port, if another process holds one.
- **The metastore.** `embedded` (the alias `raft` is accepted) is the single-node openraft store in `data/`. `tikv://<pd-hosts>/<keyspace>` needs the `full` variant (`tikv` feature) and maps to `--meta`. Postgres and DynamoDB come when M2 ships them (D58). There is no TiDB (D260).

### 8.3 Lifecycle and the supervisor

- **`stack create`** resolves the spec (variant, ports, storage) and writes `stack.toml`. Unless `--no-start` is given, it then runs `stack start`, writes `.env.loams` in the current project directory if there is one (§11.2), and prints the endpoints. It refuses an existing name (exit 5).
- **`stack start`** takes the stack's `lock` and spawns `loams stack run --name <n>` detached: a new session (`setsid`), stdin from `/dev/null`, stdout and stderr appended to `logs/server.log`. It writes `server.pid`, then polls `GET http://<native>/ready` every 200 ms until it returns 200 or `--wait-timeout` passes. On timeout it exits 7 with the last 20 log lines in `details.log_tail`.
- **`stack run`** is the foreground form. It execs the server binary with the generated flags, so systemd units, launchd plists and containers use the same command line. A `--service systemd|launchd` generator is a CLI2 follow-up (Q288).
- **`stack stop`** sends SIGTERM to the process group, waits `--grace` (default 30 s, about the server's own `HTTP_GRACE` plus listener drains), then sends SIGKILL. It removes `server.pid`.
- **`stack describe`** reports one of `running` (the pid is alive and `/ready` returns 200), `starting`, `unhealthy` (alive but not ready), `stopped` or `crashed` (a pid file whose process is gone). It also reports the endpoints, the variable names, the binary and its version, and `binary_outdated` if the stack's binary is older than the installed one.
- **`stack delete --yes`** stops the stack and removes `stacks/<name>/`. It never deletes a remote bucket's objects or unmounts a disk; it prints what it left in place. `--keep-data` keeps `data/`.
- **`stack restart --upgrade`** switches `binary` to the installed binary (or the matching variant) and restarts. Data stays, under the format N/N−1 rules of §10 §7.

```json
{"name": "dev", "state": "running", "pid": 41233, "version": "0.4.0", "variant": "standard",
 "binary": "/home/u/.loams/bin/loams", "binary_outdated": false, "metastore": "embedded", "object_store": "local",
 "storage": {"kind": "nvme", "device": "/dev/nvme1n1", "path": "/mnt/loams-cache", "cache_disk_bytes": 858993459200},
 "engines": [
   {"engine": "qdrant", "endpoints": {"rest": "http://127.0.0.1:6333", "grpc": "http://127.0.0.1:6334"}, "env": ["QDRANT_URL", "QDRANT_GRPC_URL"]},
   {"engine": "es", "endpoints": {"rest": "http://127.0.0.1:9200"}, "env": ["ELASTICSEARCH_URL"]}],
 "auth": "none", "created_at": "2026-10-01T09:12:44Z"}
```

## 9. Variants and the feature matrix (D286)

### 9.1 Why prebuilt

Building `loams` with release settings takes tens of minutes and several GB of RAM on a laptop (estimate, from the M0–M1 build notes; this machine hangs on fully parallel builds). Toggling `durable` alone rebuilds about 480 crates (D262). "Paste one command" cannot include that. The chat dump also chose prebuilt variants for this reason. A variant can switch off engines at run time (§8.2), so two server variants cover every combination; neither is a per-stack build.

### 9.2 The matrix

| Variant | Cargo features (on `loams`) | Engines | Size (estimate) | Targets |
|---|---|---|---|---|
| `cli` (CLI2, after D297) | `--no-default-features` (no `server`) | none: client commands, `mcp serve`, `docs` | 15–25 MB | Linux x86_64 and aarch64, macOS aarch64; Windows x86_64 (Q285) |
| **`standard`** (the default install) | `es, flight, hnsw, qdrant, mcp, pgwire, durable` (+ `console` when §19 ships) | native, flight-sql, mcp, qdrant, es, pg, durable | 120–180 MB | Linux x86_64 and aarch64, macOS aarch64 |
| `full` | `standard` + `tikv, durable-tikv, live, stream-grpc, mysql-wire, jobs` (`live` and `jobs` once they merge) | all in-binary engines | 160–240 MB | Linux x86_64 and aarch64, macOS aarch64 |
| never | `failpoints` (D29), `cluster-tests`, `durable-mysql` (legacy, D260) | — | — | — |

- **Durable in `standard`.** D262 says release builds turn `durable` on, so the standard variant includes it. Stacks that do not ask for it pass `--no-durable`.
- **`pgwire` in `standard`.** PG1 Task 1 decides whether `pgwire` becomes a default feature. The variant includes it either way, because the listener is off unless `--pg-listen` is given.
- **Targets.** `x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu` are built on Ubuntu 22.04 runners, so the minimum glibc is 2.35 (verify that cargo-dist's `min-glibc-version` check agrees). `aarch64-apple-darwin` is built on a macOS 15 runner. Intel macOS servers are not built (Q286). musl is not built: the engine's allocator and DataFusion's performance on musl are untested.
- **A CI guard** (CLI2 Task 3) parses `release/variants.toml` and fails if a variant enables `failpoints`, `cluster-tests` or `durable-mysql`, or if a variant's feature list differs from `cargo metadata`'s view of the `loams` features.

### 9.3 Selecting a variant at `stack create`

1. Compute the features the requested engines need (§8.2).
2. If the running binary's compiled-in features (`env!`-baked by `build.rs` into `loams_cli::version::FEATURES`) cover them, the stack uses the running binary.
3. Otherwise, take the smallest variant in the release manifest that covers them, at the same version as the running binary. If it is already in `variants/<version>/<variant>/`, use it. If not, download it with the self-update verification path (§17.3), with consent (`--allow-download`, or a prompt).
4. If no variant covers them, exit 6 (`feature_not_in_variant`) and name the missing features. `--from-source` then runs `cargo install --locked --git https://github.com/ostrium-labs/loams --tag v<version> loams --no-default-features --features <list> --root ~/.loams/variants/<version>/src-<hash>/`, after warning about time and memory. It needs a Rust toolchain and is never chosen automatically.

## 10. NVMe and the cache (D287)

### 10.1 What gets placed on the device

§04 puts **H1** (byte ranges of durable objects: Lance pages, split blocks, segments) in foyer's hybrid RAM and NVMe cache, and **H2** (HNSW artifacts and pinned splits) on local disk. In code, H1 is `loams_cache::RangeCache` with `RangeCacheConfig { block_size, memory_bytes, disk: Option<DiskConfig { dir, capacity_bytes }> }`, and H2 uses `--hot-dir` and `--hot-nvme-bytes`. **H1's disk tier has no flag today**, so CLI1 Task 3 adds three flags to the shared `Native` group on `dev`, `standalone` and `cluster`:

| Flag | Maps to | Default |
|---|---|---|
| `--cache-ram-bytes <N>` | `RangeCacheConfig.memory_bytes` | 256 MiB (unchanged) |
| `--cache-dir <PATH>` | `RangeCacheConfig.disk = Some(DiskConfig { dir, … })` | none (RAM only, unchanged) |
| `--cache-disk-bytes <N>` | `DiskConfig.capacity_bytes`; requires `--cache-dir` | 100 GiB, capped to 80 % of the filesystem's free space at startup |

These are the M1-era flag forms of §10 §2's `[cache] nvme_path` and `nvme` keys, and §10 §2's flag table gains these rows.

### 10.2 The flow

```
loams stack create --name dev --storage nvme:/dev/nvme1n1
  ├─ device mounted already? ──yes──► use <mountpoint>/loams/<stack>/{cache,hot}; check that the user can write there
  └─ no ──► running as root? ──no──► exit 3 (sudo_step_required) and print:
                │                       sudo loams storage prepare --device /dev/nvme1n1 --mount /mnt/loams-cache --yes
                │                       loams stack create --name dev --storage mount:/mnt/loams-cache
                └─ yes ──► refuse: exit 3 ("run `storage prepare` under sudo, then `stack create` as yourself")
```

`stack create` never formats anything. Only `storage prepare` writes to a device, and only when the user ran it explicitly as root.

**`loams storage prepare --device D --yes`** (Linux only; elsewhere exit 6, `platform_unsupported`) does the following:

1. **Inspects** the device with `lsblk --json --bytes --output NAME,PATH,TYPE,SIZE,FSTYPE,MOUNTPOINTS,PKNAME,MODEL,SERIAL,ROTA,RO` and `blkid -p -o export D` (util-linux).
2. **Refuses** (exit 5, nothing written) if any of these holds:
   - D is not a block device of `TYPE` `disk` or `part` (loop and dm devices need `--allow-virtual`, which CI uses);
   - D or any of its partitions is mounted, holds swap or `/`, or is an LVM, MD-RAID or ZFS member;
   - D is read-only;
   - D is smaller than 16 GiB;
   - D has a filesystem or partition-table signature and `--wipe` was not given (`device_has_filesystem` names the signature);
   - D is rotational and `--allow-hdd` was not given.
3. **Confirms.** With a TTY, the user must type D's path. Without one, `--yes` is required.
4. **Formats** with `mkfs.ext4 -F -L loams-cache -m 0 -E lazy_itable_init=1,discard D`. ext4 is everywhere and needs no extra tools. XFS is an option (`--fs xfs`).
5. **Mounts** with `mount -o noatime D <mount>`, then appends `UUID=<uuid> <mount> ext4 noatime,nofail 0 2` to `/etc/fstab`. A line for that UUID is never written twice. `--no-fstab` skips this step.
6. **Hands the mount over**: `chown $SUDO_UID:$SUDO_GID <mount>`, then `mkdir <mount>/loams`.
7. **Prints** `--storage mount:<mount>` for the next command. With `--output json` it returns `{device, uuid, mount, fs, size_bytes, fstab: bool}`.

The step list is a pure function, `plan_prepare(&Lsblk, &Blkid, &Options) -> Result<Vec<Step>, Refusal>`, tested against fixture JSON. Execution runs `std::process::Command` per step with no shell. A CI job (`cli-nvme`) runs the whole flow as root against a loop device backed by a sparse file (CLI1 Task 7).

### 10.3 Sizing

| Storage | `cache_disk_bytes` | `hot_nvme_bytes` |
|---|---|---|
| `dir:` (default `stacks/<name>/`) | 0: RAM only. A laptop's home disk is not a cache device unless asked (`--cache-disk-bytes`) | the server default |
| `mount:` or `nvme:` | 50 % of the filesystem's size | 40 % of the filesystem's size |

The remaining 10 % is headroom for ext4 and for foyer's own overheads. Both values can be overridden with `stack create --cache-disk-bytes` and `--hot-nvme-bytes`.

## 11. Secrets (D288)

### 11.1 The rule

**A secret never appears in MCP tool output, MCP errors, `json` output that an agent might read, log lines or panic messages.** Secrets are written to files with mode 0600, and only names and redacted values are reported.

- `loams_cli::secret::Secret(String)` implements neither `Serialize` nor `Display`. Its `Debug` prints `Secret(<redacted>)`. The only way to read it is `expose(&self) -> &str`, called in two places: the dotenv writer, and the HTTP client's auth header. A `clippy.toml` `disallowed-methods` entry stops `expose` from being called anywhere else in the crate without an `#[allow]` that names this section.
- **The canary test** (CLI1 Task 9) creates a stack whose `secrets/` holds the canary `loams_cnry_9f2b…`, calls every MCP tool with valid and invalid arguments, runs every CLI command in `json` mode, scans the stdout, stderr and `server.log` it captured, and fails if the canary appears anywhere.
- **Humans still see their secrets** when they ask: `keys create` in a terminal prints the token once. `env export` prints it when the user runs it directly. Only the MCP path is restricted.

### 11.2 `.env.loams`

```dotenv
# Written by loams 0.4.0 for stack "dev" at 2026-10-01T09:12:44Z. Do not commit.
LOAMS_STACK=dev
LOAMS_URL=http://127.0.0.1:8080
LOAMS_FLIGHT_SQL_URL=grpc://127.0.0.1:8082
LOAMS_MCP_URL=http://127.0.0.1:8083/mcp
QDRANT_URL=http://127.0.0.1:6333
QDRANT_GRPC_URL=http://127.0.0.1:6334
ELASTICSEARCH_URL=http://127.0.0.1:9200
# No API key: this stack serves loopback without auth (D111). `loams keys create` arrives with the auth plan.
```

- **Loams owns `.env.loams` entirely.** Each write replaces it atomically, with mode 0600. Writing to it is idempotent.
- **`--merge PATH`** (for example `--merge .env`) updates the variables between `# >>> loams (stack dev) >>>` and `# <<< loams <<<` markers in place, inserting the block if it is absent. Lines outside the block are never changed. This replaces the chat dump's `loams env export >> .env`, which duplicates lines on every run. The plain `>>` form still works, and the docs recommend `--merge`.
- **`.gitignore`.** If the project is a git repository and `git check-ignore -q .env.loams` fails, the CLI appends `.env.loams` to `.gitignore` and says so on stderr. `--no-gitignore` skips this.
- **Variable names.** The chat dump's `LOAMS_KEY_ID` stays (it is not secret, and logs may print it). `LOAMS_KEY_SECRET` becomes **`LOAMS_API_KEY`**, the whole `loams_<key_id>_<secret>` token (D65), because the SDKs and gateways need the whole token. `QDRANT_API_KEY` and `ELASTICSEARCH_API_KEY` carry the same token, since the Qdrant `api-key` header and the ES `ApiKey` scheme accept it (§10 §4). `DATABASE_URL` is set only by the `postgres` companion (§8.2).
- **The project directory** is `--project-dir`, else `$CLAUDE_PROJECT_DIR` (which Claude Code sets for MCP servers, verified 2026-10-01), else the nearest ancestor of the working directory that holds `.git`, `package.json`, `pyproject.toml` or `Cargo.toml`, else the working directory.
- **Agents with file access can still read `.env.loams`.** The rule keeps the secret out of tool output, not out of an agent that runs `cat`. For Claude Code with project scope, `mcp install` therefore offers to add `"permissions": {"deny": ["Read(./.env.loams)"]}` to `.claude/settings.json` (only with consent, interactively or with `--protect-env`). The guide recommends the equivalent for other agents.

## 12. MCP

### 12.1 `loams mcp serve` (D289)

`loams mcp serve` serves MCP over stdio with rmcp (Apache-2.0; 3.5.0 on crates.io on 2026-10-01, and the workspace pins 3.4.1, so CLI1 Task 0 picks one) using the `transport-io` feature. It supports the same protocol versions as M1.6 Ruling 7: 2026-07-28 statelessly, and 2025-11-25, 2025-06-18 and 2025-03-26 with `initialize`. Stdout carries only JSON-RPC. Logs go to stderr at `warn` and to `~/.loams/logs/mcp.log` at `info`. `serverInfo` is `{"name": "loams", "version": "<binary version>"}`. The tool list is fixed for the life of the process; it does not depend on which stacks exist.

| Tool | Inputs | Returns | Annotations | Safety rules |
|---|---|---|---|---|
| `search_docs` | `query` (string), `limit` (1–20, default 5) | `{version, results: [{title, path, section, excerpt, url}]}` | read-only, idempotent, closed world | The bundle only (§13). Output capped at 32 KiB |
| `get_sdk_snippet` | `engine` (enum from §8.2), `language` (`python`, `typescript`, `rust`, `shell`), `task` (enum: `connect`, `create_collection`, `upsert`, `search`, `hybrid_search`, `sql`; default `connect`) | `{engine, language, task, code, packages, env, doc_url, tested_with}` | read-only, idempotent, closed world | Snippets read endpoints from environment variables and never inline a value (§13) |
| `loams_info` | none | `{version, variant, features, engines_available, docs_version, home, output_schema, update: {latest, checked_at} \| null}` | read-only, idempotent | Never fetches from the network: it reports the cached update check only |
| `stack_status` | `name` (optional) | `{stacks: [{name, state, engines, endpoints, env_names, binary_outdated}]}` | read-only, idempotent | Endpoints and variable names, never values from `secrets/` |
| `stack_create` | `name`, `engines` (array), `storage` (`dir:` or `mount:` only), `start` (default true), `allow_download` (default false) | the `stack describe` object | not read-only, not destructive, not idempotent | `object_store` is always `local` and `metastore` always `embedded` from MCP. `nvme:` is refused (only a human formats disks). An existing name returns `stack_exists`. A variant download needs `allow_download: true`, which the tool description tells the model to set only after the user agrees |
| `stack_start` | `name` | the `stack describe` object | not read-only, idempotent | Starts an existing stack; it never creates one |
| `env_export` | `name`, `path` (default `.env.loams`), `merge_into` (optional, e.g. `.env`) | `{path, written: [names], values: {name: value or "<redacted: in .env.loams>"}}` | not read-only, idempotent | Paths must resolve inside the project directory (§11.2), with no `..` or symlink escape, and the file name must match `.env*`. Secrets are redacted |
| `add_package` (CLI2) | `package` (a logical name or an allow-listed registry name), `dev` (bool), `dry_run` (bool) | `{ecosystem, command: [argv], manifest, exit_code, output_tail}` | not read-only, open world | Allow-list only (§14). The command runs in the project directory with a 5-minute timeout. `output_tail` is the last 4 KiB of output |

**Never MCP tools:** `stack stop`, `restart`, `delete`, `storage *`, `keys *`, `login`, `logout`, `configure`, `self-update`, `mcp install` and `uninstall`, and anything with `--from-source`. The chat dump allowed destructive tools if they ask for confirmation. MCP 2026-07-28 replaces server-initiated requests (elicitation) with multi-round-trip `input_required` results (§15 §10), and client support for them varies, so this document omits destructive tools instead of relying on confirmation (D289). An agent that needs one asks the user to run the command.

Errors are `CallToolResult::structured_error({"error": code, "message", "hint"})` with the same codes as §6.3, which follows M1.6 Task 7's error shape.

**The data tools** (`search`, `sql`, `memory_write`, `list_collections`, `get_documents`) are M1.6's HTTP server on the stack. They are not re-exported here, for four reasons:
- the tool list would otherwise change with stack state;
- the schemas would be duplicated;
- the stdio server would need `loams-mcp` and therefore the engine, which defeats the `cli` variant;
- one hop is better than two.

`mcp install` registers both servers instead (§12.2).

### 12.2 `loams mcp install` (D290)

| Agent | With its CLI on `PATH` (preferred) | Without it: the file the CLI edits | HTTP data entry |
|---|---|---|---|
| `claude-code` | `claude mcp add --scope <scope> loams -- <abs>/loams mcp serve`, then `claude mcp add --transport http --scope <scope> loams-<stack> http://127.0.0.1:<mcp>/mcp` (verified 2026-10-01) | project scope only: `.mcp.json` `{"mcpServers": {"loams": {"command": "<abs>/loams", "args": ["mcp", "serve"]}}}`. For user scope without `claude`, the CLI prints the command and exits 3 | `{"type": "http", "url": "…/mcp"}` (M1.6 Task 9) |
| `codex` | `codex mcp add loams -- <abs>/loams mcp serve` (verified 2026-10-01) | `~/.codex/config.toml` (user) or `.codex/config.toml` (project), edited with `toml_edit`: `[mcp_servers.loams] command = "<abs>/loams"`, `args = ["mcp", "serve"]` | `[mcp_servers.loams-<stack>] url = "…/mcp"` |
| `cursor` | none (verified 2026-10-01: no CLI, only a UI and an extension API) | `~/.cursor/mcp.json` (user) or `.cursor/mcp.json` (project): `{"mcpServers": {"loams": {"type": "stdio", "command": "<abs>/loams", "args": ["mcp", "serve"]}}}` | `{"url": "…/mcp"}` (verify the key) |
| `windsurf` | none | `~/.codeium/windsurf/mcp_config.json`: `{"mcpServers": {"loams": {"command": "<abs>/loams", "args": ["mcp", "serve"]}}}` (verified 2026-10-01) | `{"serverUrl": "…/mcp"}` |

Rules:
- **Absolute paths.** GUI agents may not inherit the shell's `PATH`, so the entry always names the absolute binary path (`~/.loams/bin/loams`, expanded).
- **Idempotent.** Installing twice gives the same file. The CLI updates its own entries (named `loams` or `loams-<stack>`) and leaves every other entry, key and key order alone (`serde_json` with `preserve_order`, or `toml_edit`).
- **Safe edits.** The file is copied to `<file>.loams-backup` before the CLI's first edit to it. A file that does not parse is never rewritten (exit 5, `config_unparseable`). The write is atomic.
- **Tracked.** `~/.loams/mcp-installs.json` records each entry written. `mcp uninstall` removes only those entries.
- **`--dry-run`** prints the command or a unified diff and writes nothing.
- **`--no-data`** skips the HTTP entry. The HTTP entry is also skipped, with a notice, when the stack has no `mcp` engine (before M1.6 merges, every stack).
- **`init`** detects agents by looking for `claude` and `codex` on `PATH` and for `~/.cursor/` and `~/.codeium/windsurf/`, and offers to install for each one it finds.

## 13. Docs and snippets in the binary (D291)

- **Sources.** `docs/guides/**/*.md` (user guides, starting with M1.6 Task 9's `docs/guides/mcp.md`, which this plan's Task 11 extends with a CLI guide) and **`docs/snippets/<engine>/<language>/<task>.<ext>`**. Each snippet starts with a front-matter comment block that lists `packages`, `env` and `doc`.
- **The bundle.** `loams-cli`'s `build.rs` packs both trees into a zstd tarball and embeds it with `include_bytes!` (estimate: under 1 MB compressed). On the first `search_docs` call, the process builds an in-memory Tantivy index: title and body fields, the English stemmer, BM25. The index is never written to disk.
- **The website and the bundle share one source.** The product site in `loam-cloud` renders the same `docs/guides` (Q281 covers hosting). The binary therefore carries the docs of its own version, offline, and `search_docs` never disagrees with the installed server.
- **Snippets are tested.** A CI job, `snippets` (CLI1 Task 8), starts `loams dev`, sources a generated `.env.loams`, and runs every `python` and `shell` snippet (uv venv) and every `typescript` snippet (node 22). Each snippet asserts its own result. A snippet that does not run fails the build. Snippets that need packages that are not yet published (the SDKs before D33) run against the in-repo SDK sources.
- **CLI1's first snippets:** `native`, `qdrant`, `es` and `pg` × `python`, `typescript` and `shell` × `connect`, `upsert` and `search`. The `mcp` snippet is the `.mcp.json` entry itself.

## 14. Packages (D296, CLI2)

`release/packages.toml`, embedded in the binary:

```toml
[sdk]
npm = "loams"            # D400; the `@loams` scope (D400) holds the other npm packages
pypi = "loams"
crates = "loams"
[bullmq]
npm = "@loams/bullmq"      # §26 §7.2
[celery]
pypi = "loams-celery"      # §26 §7.1
[durable]
npm = "@loams/durable"     # §26 §12.2
pypi = "loams"           # the helpers ship in the SDK (§26 §8.4)
[live]
npm = "@loams/live"      # R1 Task 14's package
```

- **The ecosystem** comes from `--language`, else from the project's files: `pnpm-lock.yaml` → `pnpm add`, `bun.lock` → `bun add`, `yarn.lock` → `yarn add`, `package-lock.json` or a bare `package.json` → `npm install`, `uv.lock` or a `pyproject.toml` with `[tool.uv]` → `uv add`, `poetry.lock` → `poetry add`, `Cargo.toml` → `cargo add`. A `requirements.txt` alone gets exit 6 with a hint (the CLI does not edit pip files). If more than one ecosystem matches, the CLI asks, or exits 3 without a TTY.
- **The version** is pinned to the CLI's minor version (`loams@~0.4.0`, `loams>=0.4,<0.5`, `loams = "0.4"`), so the SDK matches the server the CLI runs. `--version` overrides it.
- **Anything not in the allow-list** is refused from MCP (`add_package`). From the terminal, the CLI passes it through after a warning.

## 15. Keys, login and agents (D295, CLI3)

- **`keys create`** calls the gateway's key API (part of the unified auth plan, D111, Q30) for one environment (§19 §5.5). The key defaults to a 90-day expiry. The CLI prints `{key_id, token, expires_at}` once in a terminal. With `--write-env`, it writes `LOAMS_KEY_ID` and `LOAMS_API_KEY` to the file and prints only `key_id`. For a local stack, the bootstrap key `stack create` makes is also kept in `stacks/<name>/secrets/` (0600) so that `env export` can write it again. Keys for remote stacks are never stored by the CLI.
- **`login`** uses OAuth 2.1 authorization code with PKCE and a loopback redirect (RFC 8252) against the instance's authorization server (§19 §5.2, flow 2). Headless machines use the device flow (RFC 8628), if the auth plan adds it. Tokens go to `credentials.toml` (0600).
- **`mcp serve` as an agent principal.** `mcp install` registers an agent (§19 §5.1) owned by the user, with the policy `collections:read`, `collections:write`, `query`, `mcp:tools` on the profile's environment. `mcp serve` exchanges the user's credential for a vended agent token (§19 §5.2, flow 3), with a TTL of 15 minutes, on each call it makes to the stack. The user's credential stays in the CLI process and is never returned by a tool. Agents never hold API keys (§19 §5.5).
- **Reaching a self-hosted instance on a private network (D592, D593, [§43](43-private-networking.md)).** `loams login --endpoint https://loams.net.example.com` and `endpoint` in the config (§7) are ordinary HTTPS URLs; when the instance is on the user's tailnet they resolve to the tailnet while the official Tailscale client is connected. The CLI does not manage the tailnet, embed a client or take a network option; a failed connection adds the hint "is your tailnet connected?" when the host resolves into `100.64.0.0/10` (a hint on the existing network error of §6.2, not a new code). Credentials still require HTTPS (§43 §7.2).
- **Before the auth plan**, `keys` and `login` exit 6 with `auth_not_available` and the hint "this build has no auth (D111); local stacks are loopback-only". `.env.loams` holds endpoints only.

## 16. Companions (D299, CLI3)

| Companion | Runs | Pinned by | Notes |
|---|---|---|---|
| `postgres` | `postgres:17` with a volume under `stacks/<name>/companions/postgres`; Loams Postgres compute images once track P publishes them (D231, P2) | image digest in `release/companions.toml` | Sets `DATABASE_URL`. Not the pg wire (§8.2) |
| `tikv` | `tiup playground v8.5.8 --tag loams-<stack> --mode tikv-slim`, with the port offset R1 uses | tiup version (R1) | Needs `full`. Sets `--meta tikv://…` and, with `durable`, `--durable-store tikv://…` |
| `rustfs` | `rustfs/rustfs:1.0.x` (D61) | image digest | Switches the stack to `standalone --bucket s3://loams/<stack>` with the generated credentials |

The container runtime is Docker or Podman, whichever is found first; `LOAMS_CONTAINER_RUNTIME` overrides it. A companion's lifecycle follows its stack's (start, stop, delete). Companions are separate processes and never linked, so the license of the binary is unaffected.

## 17. Distribution

### 17.1 The release pipeline (D292, CLI2)

- **cargo-dist 0.33.0** (released 2026-09-11; MIT OR Apache-2.0; active on 2026-10-01; verified). The workspace's `dist-workspace.toml` declares the following:

  ```toml
  [dist]
  cargo-dist-version = "0.33.0"
  ci = "github"
  targets = ["x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu", "aarch64-apple-darwin"]
  installers = ["shell"]                 # cargo-dist's own installer, published as a fallback
  checksum = "sha256"
  github-attestations = true
  hosting = "github"
  install-path = "~/.loams/bin"
  global-artifacts-jobs = ["./release-manifest"]   # runs in the build-global-artifacts phase; signs loams-release.json (verify that its outputs upload)
  [dist.github-custom-runners]
  x86_64-unknown-linux-gnu = "ubuntu-22.04"
  aarch64-unknown-linux-gnu = "ubuntu-22.04-arm"
  aarch64-apple-darwin = "macos-15"
  ```

- **Variants.** cargo-dist builds per package, with `features` and `default-features` per package (verified 2026-10-01). It does not offer several feature sets of one package. CLI2 Task 0 is a spike that tries thin variant packages: `crates/loams-variant-standard` and `-full`, each a two-line `main.rs` calling `loams::main()` with its own feature list and `[[bin]] name = "loams"`. If cargo-dist's precise builds handle three packages that produce a binary of the same name, it builds all variants. If not, a Loams-owned matrix job in the same workflow builds the extra variants with `cargo build -p loams --profile dist --features …` and uploads `loams-<variant>-<target>.tar.xz` to the release (Q293).
- **The release manifest**, `loams-release.json`, is signed with minisign as `loams-release.json.minisig`:

  ```json
  {"schema": 1, "version": "0.4.0", "channel": "stable", "published_at": "2026-11-02T10:00:00Z",
   "output_schema": 1, "docs_version": "0.4.0",
   "artifacts": [{"variant": "standard", "target": "x86_64-unknown-linux-gnu",
     "url": "https://github.com/ostrium-labs/loams/releases/download/v0.4.0/loams-standard-x86_64-unknown-linux-gnu.tar.xz",
     "sha256": "…", "size": 0, "features": ["durable", "es", "flight", "hnsw", "mcp", "pgwire", "qdrant"]}]}
  ```

- **Signing.** The minisign secret key lives in the GitHub environment `release`, which has required reviewers (Q282). The public keys (current and next) are committed in `release/minisign.pub` and embedded in the binary and in `install.sh`. GitHub artifact attestations (Sigstore, SLSA build provenance) are verified out of band with `gh attestation verify <file> --repo ostrium-labs/loams`. A `SHA256SUMS` file is published for people who verify by hand.
- **When.** The pipeline runs on PRs as `dist plan` plus one `dist build` per target with publishing off. **Publishing starts only after D33's rename PR and the transfer to `ostrium-labs/loams`** (user memory). A release is cut by pushing a tag `v<semver>` on `main`.

### 17.2 `install.sh` (D293, CLI2)

The source is `release/install.sh`: POSIX `sh` and `set -eu`, about 250 lines, checked by shellcheck. Each release publishes it with the version stamped in. `https://loams.dev/install.sh` answers 302 to `https://github.com/ostrium-labs/loams/releases/latest/download/install.sh`, and `curl -fsSL` follows the redirect. The GitHub URL works without `loams.dev`. **`loams.dev` had no DNS record on 2026-10-01**; the owner bought it on Cloudflare on 2026-10-02 (D401, answering Q281).

```
curl -fsSL https://loams.dev/install.sh | sh
curl -fsSL https://loams.dev/install.sh | sh -s -- --variant full --version 0.4.0 --yes --no-init
```

1. **Platform.** `uname -s` and `uname -m` give Linux x86_64/aarch64 or Darwin arm64. Anything else prints a message naming WSL2, `cargo install`, or the `cli` variant once it exists.
2. **Tools.** It uses `curl` (else `wget`), `tar`, `xz` and `sha256sum` (else `shasum -a 256`).
3. **Manifest.** It fetches `loams-release.json` (latest, or `--version` / `LOAMS_VERSION`). If `minisign` is installed, it verifies the signature with the embedded public key. If not, it says the signature was not checked and how to check it. `--require-signature` makes a missing `minisign` an error.
4. **Archive.** It downloads the archive for the variant (`--variant`, `LOAMS_VARIANT`, default `standard`) and target, and checks its SHA-256 against the manifest. A mismatch exits 9.
5. **Install.** It writes `$LOAMS_HOME/bin/loams` (`LOAMS_HOME` default `~/.loams`) and the receipt `receipt.json`. It appends `. "$HOME/.loams/env"` to `~/.profile`, `~/.bashrc` and `~/.zshrc` (those that exist) and adds `~/.config/fish/conf.d/loams.fish`, unless `--no-modify-path` is given. A line is never added twice.
6. **Init.** If `/dev/tty` is readable (stdin is the pipe) and neither `--no-init` nor `--yes` was given, it asks "Run `loams init` now? [Y/n]" and reads the answer from `/dev/tty`.

**Verification policy.** The archive's SHA-256 check against the manifest is always mandatory. The manifest's **signature** is checked by `install.sh` only when `minisign` is installed, or always with `--require-signature`. This is the only verification path that may skip the signature, because a POSIX script has no portable Ed25519 verifier. Every download the binary makes (`self-update`, variant downloads) always verifies the signature (§17.3).

**Trust model, stated in the guide.** The first install trusts TLS and GitHub, which is the usual trust-on-first-use of `curl | sh`. From then on, the binary carries the public keys, so every `self-update` and every variant download is signature-checked whatever the host serves.

### 17.3 `loams self-update` (D294, CLI2)

1. **Refuse foreign installs.** Without `receipt.json`, or when `current_exe()` is not `$LOAMS_HOME/bin/loams`, it exits 6 (`managed_install`) with the right upgrade command: `cargo install loams` (`cargo install loam` names an unrelated crate, verified 2026-10-01), or the package manager.
2. **Fetch and verify** the manifest for `--version` or the latest, with the embedded public keys (`minisign-verify` 0.3, MIT). A failure exits 9.
3. **Check versions.** If the target is not newer, it prints "up to date" and exits 0. `--version` with an older version needs `--yes`.
4. **Download** the archive for the receipt's variant (`--variant` switches it), then check its SHA-256 (exit 9 on mismatch).
5. **Smoke-test.** It extracts the archive to `$LOAMS_HOME/bin/.loams-new` and runs `.loams-new version --output json`. The `version` and `features` it reports must match the manifest.
6. **Swap.** It copies the current binary to `bin/loams.prev`, then `self_replace::self_replace(".loams-new")` (self-replace 1.5, Apache-2.0) swaps the binary atomically. It then updates the receipt.
7. **Leave stacks alone.** Running stacks keep their binary. `stack describe` shows `binary_outdated` and the hint `loams stack restart --upgrade`.
8. **`--check`** stops after step 3 and returns `{current, latest, update_available}`. **`--rollback`** swaps `loams.prev` back in.

axoupdater (cargo-dist's updater library, MIT OR Apache-2.0, 0.10.2) was considered. It updates from cargo-dist's own install receipts and installers, so it does not support Loams's variants or the minisign manifest. Self-replace plus minisign-verify is about 150 lines of Loams code.

## 18. Testing

| Layer | What | Where |
|---|---|---|
| Command tree | `ClientCommand::command().debug_assert()`. A golden `--help` for every command, so a flag rename is visible in review | `crates/loams-cli/tests/tree.rs`, `tests/golden/help/*.txt` |
| Output contract | Each command's JSON against its schema snapshot. Every error code maps to its exit code. `json` mode writes nothing to stdout on error | `tests/output.rs`, `tests/schemas/` |
| Binary e2e | The built `loams` binary driven over a temporary `LOAMS_HOME`: `stack create`/`describe`/`stop`/`delete` against a real `loams dev`; `env export`; exit codes. Uses `CARGO_BIN_EXE_loams`, so these tests live in `crates/loams/tests/cli/` | `crates/loams/tests/cli/*.rs` |
| NVMe | `plan_prepare` against lsblk/blkid fixtures (nine refusal cases plus the happy path). The `cli-nvme` CI job: a loop device, `storage prepare --allow-virtual`, a stack on the mount, and a check that H1 files appear under `cache/` | `crates/loams-cli/tests/storage.rs`, CI |
| MCP | An rmcp client over `TokioChildProcess` running `loams mcp serve`: tool list, each tool's success and error paths, path confinement, the secret canary | `crates/loams/tests/cli/mcp.rs` |
| Agent configs | Golden files for each agent and scope: first install, re-install, unparseable file, uninstall, and other entries preserved. A fake `claude` or `codex` on `PATH` records its argv | `crates/loams-cli/tests/agents.rs` |
| Snippets | The `snippets` CI job (§13) | CI |
| Installer | bats-core (MIT) against a local HTTP server serving a release fixture signed with a test minisign key: happy path, checksum mismatch, a bad signature with `--require-signature`, an unknown platform, PATH edits made once, the receipt | `release/tests/install.bats`, CI job `installer` (Ubuntu, Debian, Fedora containers; macOS runner) |
| Self-update | The same fixture server: update, up to date, downgrade refused, rollback, foreign install refused, a tampered archive (exit 9) | `crates/loams/tests/cli/self_update.rs` |

## 19. Roadmap: track CLI (D298)

| Plan | Scope | Depends on |
|---|---|---|
| **CLI1** ([plan](../plans/2026-10-01-cli1-local-cli-and-mcp.md)) | `loams-cli`; the output contract; `LOAMS_HOME` and `configure`; the H1 cache flags; the engine registry; `stack create/describe/list/start/stop/restart/run/logs/delete`; `storage inspect/prepare`; `env export` and `.env.loams`; the docs bundle and snippets; `mcp serve` (seven tools) and `mcp install` for four agents; `init`; `version`, `completions`; the CLI guide | M1.2 (as built). M1.6 Task 7 only for the `mcp` engine row and the HTTP entry (both conditional) |
| **CLI2** ([plan](../plans/2026-10-01-cli2-release-and-install.md)) | The server split (`loams-server`, D297) and the `cli` variant; cargo-dist; variants; the signed manifest; `install.sh`; `self-update`; variant download in `stack create`; `pkg add` and `add_package`; `--from-source`; service-unit generators | CLI1; D33's rename PR; the transfer to `ostrium-labs/loams`; Q281 and Q282 for publishing |
| CLI3 (not yet planned) | `keys`, `login`, agent tokens for `mcp serve` (§15); companions (§16); remote and cloud stacks (Q291) | The unified auth plan (D111, Q30); §19 M2 work; P2 for Loams Postgres images |

CLI1 runs beside M1, like tracks R and D: it adds one crate and three server flags, and changes no M1 code path beyond `main.rs` and the `Native` flag group. Its builds interleave with other tracks on the one-build machine (D127).

## 20. Risks

| # | Risk | Mitigation |
|---|---|---|
| 1 | **The binary name collides** with another tool on users' machines. Shipping `loams` (D401, answering Q284) avoids the `loam` binary the Soroban `loam-cli` crate (2025) may install | `~/.loams/bin` goes first in `PATH` through `~/.loams/env`. `loams version` identifies itself (`loams (Loams database) 0.4.0`) |
| 2 | **Variant sizes** (estimate 120–240 MB) make the one-line install slow on poor links | The `cli` variant (D297). xz archives. `stack create` reuses the running binary whenever it covers the engines |
| 3 | **CI time** for 2–3 variants × 3 targets in release mode | Releases only on tags. PRs run `dist plan` plus one target. The Swatinem cache keys on the variant |
| 4 | **An agent misuses `stack_create`** (many stacks, port exhaustion) | At most 8 local stacks per `LOAMS_HOME` (exit 5 past that). Loopback only. No disks, buckets or downloads without consent. Q292 |
| 5 | **Formatting the wrong disk** | No formatting outside `storage prepare` under sudo. Nine refusal rules. A typed confirmation. A pure planner tested against fixtures. The loop-device CI job |
| 6 | **Config-file edits** break an agent's setup | A backup before the first edit. No writes to files that do not parse. Atomic writes. Golden tests. `--dry-run` |
| 7 | **Secrets leak** through a new code path | The `Secret` type, `disallowed-methods`, the canary test on every tool and command, and the `.claude/settings.json` deny rule |
| 8 | **Signing-key compromise or loss** | The `release` environment with required reviewers. Two public keys embedded (current and next) for rotation. Attestations as a second, independent check (Q282) |
| 9 | **cargo-dist cannot build same-named binaries from several packages** | The CLI2 Task 0 spike. A Loams-owned matrix job as the fallback (Q293) |
| 10 | **`curl \| sh` trust on first install** | Stated in the guide. Signature checks when `minisign` is present. Attestations. A pinned-version form. Every later update is signed |
| 11 | **Snippets drift from the APIs** | The `snippets` job runs every snippet on every PR that touches `docs/snippets/**`, `crates/loams/src/api/**` or the gateways |
| 12 | **MCP protocol churn** (2026-07-28 rolling out to clients) | The same version set and tests as M1.6 Ruling 7. Stdio has no session state to get wrong |

## 21. Open questions

| # | Question | Owner | Needed by |
|---|---|---|---|
| Q281 | ~~`loams.dev` has no DNS record (checked 2026-10-01). Who registers and hosts it?~~ Answered 2026-10-02 by the owner: `loams.dev` is bought on Cloudflare (D401). ~~Still open: the proposal is a redirect route in `loam-cloud` (Next.js) for `/install.sh` and `/releases/*`, pointing at GitHub Releases. Should the product docs' source move into this repository's `docs/guides` so that the site and the embedded bundle share it (§13)?~~ Answered 2026-10-02 by the owner: the recommended default — the `/install.sh` and `/releases/*` redirects live in `loam-cloud` on `loams.dev`, and the product docs' source moves to this repository's `docs/guides`, which the site and the embedded bundle share (§30 §13, §17.2) | Founder | Resolved |
| Q282 | Signing: who generates and holds the minisign key (offline, owner), and which reviewers gate the `release` environment? Or should Loams use Sigstore keyless only and drop minisign, which would mean no in-band verification without `gh` or `cosign`? **Owner action pending (2026-10-02):** generate the minisign release key offline, hold it, and name the `release` environment's reviewers; the design default stays minisign plus GitHub attestations (D292) | Owner action | CLI2 Task 4 |
| Q283 | ~~The `@loam` npm scope: `@loam/sdk` (chat dump), `@loam/bullmq` and `@loam/durable` (§26) all need it. Register the scope, or rename to `loamdb-*`~~ Answered 2026-10-02 by the owner: the npm scope is `@loams` (D400) | Founder | Resolved |
| Q284 | ~~The binary name `loam` against other tools that install a `loam` binary (Soroban's `loam-cli`). Keep `loam`, or ship `loamdb` with a `loam` alias?~~ Answered 2026-10-02 by the owner: the binary is `loams` (D401) | Founder | Resolved |
| Q285 | ~~Windows: the `cli` variant only, or WSL2 only?~~ Answered 2026-10-02 by the owner: the recommended default — Windows gets the `cli` variant (`x86_64-pc-windows-msvc`) and runs a local server through WSL2; no Windows server variant (§30 §2.2, D417); signing is Q421 | Founder | Resolved |
| Q286 | ~~Intel macOS server builds: skip (proposed), or build on cross-compiled runners?~~ Answered 2026-10-02 by the owner: the recommended default — skip Intel macOS server variants (§30 §9.2) | Eng | Resolved |
| Q287 | ~~The default install variant: `standard` (proposed, so one command gives a working stack) or `cli` (small, with a server downloaded on first `stack create`)?~~ Answered 2026-10-02 by the owner: the recommended default — `standard`, so one command gives a working stack (§30 §9.2, CLI2 Ruling 4) | Founder | Resolved |
| Q288 | ~~Local stacks as detached processes (proposed) or as systemd user units and launchd agents by default (they survive logout and reboot)?~~ Answered 2026-10-02 by the owner: the recommended default — detached processes by default; systemd user units and launchd agents as CLI2's opt-in (§30 §8.3) | Eng | Resolved |
| Q289 | ~~Container images per variant on GHCR (`ghcr.io/ostrium-labs/loams:<version>-<variant>`): in CLI2 or later?~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — after CLI2: one `standard` image with the first release that follows CLI2, per-variant tags only on demand; why: CLI2 stays scoped to the binary pipeline | Founder | Resolved |
| Q290 | ~~Homebrew tap, npm shim (`npx loams`) and PyPI shim distribution: which, and when?~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — a Homebrew tap and an npm installer (`npx @loams/cli`), both generated by cargo-dist, with CLI2's first release; no PyPI shim (D418); why: bought from cargo-dist, and `loams` on PyPI is the SDK (D400) | Founder | Resolved |
| Q291 | ~~Cloud stacks: the `loam-platform` public API the CLI calls for `stack create --target cloud`, and whether the cloud client lives in this repository (D220: the CLI is open)~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — the cloud client is open and lives here, against a published, versioned public API that `loam-platform` implements; CLI3's plan writes the contract; why: D220 keeps the CLI open and the API is the boundary | Founder | Resolved |
| Q292 | ~~May `stack_create` from MCP start a local process without a human confirming it? The proposal is yes: loopback only, local object store, no disks, no downloads without `allow_download`, at most 8 stacks~~ Answered 2026-10-02 by the owner: the recommended default — yes, within §30 §12.1's limits: loopback, a local object store, no disks, no downloads without `allow_download`, at most 8 stacks (CLI1 Ruling 8) | Founder | Resolved |
| Q293 | ~~The CLI2 Task 0 spike result: does cargo-dist build variant packages that each produce a binary named `loams`, or does Loams own a matrix job?~~ Answered 2026-10-02 by the owner: the plan's default — not an owner decision: thin variant packages if CLI2 Task 0's spike passes, else the Loams-owned matrix job (§30 §17.1) | Eng | Resolved |
| Q294 | ~~Default ports for the stack's pg (15432) and MySQL (13306) listeners, chosen to avoid local Postgres and MySQL. D-PG-1 gives the server no default port. Keep these, or use 5433 and 3307?~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — keep 15432 and 13306; why: they avoid local Postgres and MySQL and the 5433/3307 ports second instances and containers often take | Eng | Resolved |

## 22. Contradictions with earlier decisions, and how they are resolved

| Earlier | Conflict with the chat dump | Resolution |
|---|---|---|
| D33 (renamed Loams after M1; M1 publishes nothing) | The chat dump installs `loams` from `loams.dev` now | CLI1 was built before the rename PR (D407), which moved it to `loams` and `loams-cli`. Releases and the installer go live only after the rename PR and the move to `ostrium-labs/loams` (D292) |
| D111 (no auth or TLS in M1) | `stack create` "creates the first key"; `.env.loams` holds `LOAMS_KEY_SECRET` and `QDRANT_API_KEY` | Keys wait for the unified auth plan (D295, CLI3). Until then `.env.loams` holds endpoints only, and `keys` exits 6 |
| §19 §5.5 ("keys cannot be issued to agents") and P6 (agents hold no long-lived secrets) | The agent "provisions the stack" and the stack writes key secrets | Keys are created only by people, through the CLI, for apps. MCP tools never mint or return keys. In CLI3, `mcp serve` uses vended agent tokens (D295). The chat dump's own rule (secrets to `.env.loams`, the key id only through MCP) is kept and made stricter (D288) |
| D65 (the key is one token, `loams_<key_id>_<secret>`) | Separate `LOAMS_KEY_ID` and `LOAMS_KEY_SECRET` | `LOAMS_API_KEY` holds the whole token; `LOAMS_KEY_ID` stays as a non-secret label (D288) |
| D-PG-1, PG1, D2, D230, D231 (the pg listener is analytics; OLTP Postgres is CNPG or Loams Postgres, a separate service) | `--engines postgres` with `DATABASE_URL` implies OLTP Postgres inside the binary | `pg` is the analytics listener with `LOAMS_PG_URL`. `postgres` is a companion service (D299, CLI3) and the only engine that sets `DATABASE_URL` (D285) |
| D260 (no TiDB), D124 (TiKV metastore) | `--metastore raft` is the only backend named | `--metastore embedded` (alias `raft`) or `tikv://…`. Postgres and DynamoDB arrive with M2 (D58). No TiDB anywhere |
| M1.6 Ruling 19 and D111 (MCP is HTTP on its own loopback listener; five data tools) | `loams mcp serve` "runs your existing docs MCP server over stdio" and exposes `stack_status`, `env_export` and others | No docs MCP server exists. The stdio bootstrap server is new (D289). The data tools stay on M1.6's HTTP endpoint, and `mcp install` registers both (D290) |
| The chat dump's "MCP tools should omit [destructive ops] or require confirmation" | — | Omitted, not confirmed (D289), because elicitation support varies under 2026-07-28 |
| The chat dump's "Builds the single server binary with only those components enabled" | A per-stack build | Prebuilt variants, with run-time `--no-*` flags for subsets. `--from-source` is opt-in (D286); the chat dump itself recommended prebuilt variants |
| The chat dump's "one crate per command group" | — | One `loams-cli` crate with modules (§4.1) |
| The chat dump's "or use a small privileged helper" | — | Rejected. `storage prepare` under `sudo`, no setuid helper (D287) |
| D29 (release builds carry no failpoints) | — | Enforced by the variant guard (§9.2) |
| D138 vs D262 (whether durable is on by default) | — | D262 holds: `durable` is opt-in in the crate and on in the release variants |
| §10 §2 (no config file in M1) | `stack.toml` | `stack.toml` is the CLI's own record, translated into flags. The server's M2 configuration file is unaffected (§8.1) |
| The chat dump's `loams pkg add @loams/sdk` vs D33 (`loamdb`) | — | Superseded by the owner's 2026-10-02 ruling (D400): logical names map to `loams` packages and the `@loams` npm scope (D296) |
| The chat dump's `loams env export >> .env` | It duplicates lines on every run | `--merge .env` with marker blocks; `>>` still works (§11.2) |

## 23. Sources

Read on 2026-10-01.

- **This repository at `9eaddae`:**
  - `crates/loams/src/main.rs` (the clap tree and the `Native` flags), `crates/loams/Cargo.toml` (features), `crates/loams/src/api/mod.rs` (`/health`, `/ready`), `crates/loams-cache/src/range_cache.rs` (`RangeCacheConfig`, `DiskConfig`), `.github/workflows/ci.yml`, `Cargo.toml`;
  - docs §04, §10, §15 §10, §19, §21, §26, §28; plans M1.6 (Rulings 7, 8, 11, 19; Task 9's client configs), PG1, R1; `docs/open-core.md`;
  - D29, D33, D58, D61, D65, D111, D124, D138, D220, D230, D231, D260, D262, D-PG-1;
  - the pending §29 (PR #172, D273–D280) for numbering.
- **`chatdump.md`** lines 1–51 (the owner's 2026-10-01 source).
- **`loam-cloud` `origin/main`** (2026-10-01): no MCP server; the brand names `loams.dev`; `NEXT_PUBLIC_SITE_URL` example `https://loams.dev`.
- **cargo-dist:** GitHub `axodotdev/cargo-dist` (releases v0.33.0 of 2026-09-11 and v0.32.0; license MIT OR Apache-2.0 in `Cargo.toml`; not archived; last push 2026-10-01). The config reference (`features`, `default-features`, `checksum`, `install-path`, `installers`, `github-custom-runners`, `targets`, `hosting`, `min-glibc-version`, `github-attestations`) and the customizing-CI page (the config keys `plan-jobs`, `local-artifacts-jobs`, `global-artifacts-jobs`, `host-jobs`, `publish-jobs` and `post-announce-jobs`; `global-artifacts-jobs` adds jobs to the build-global-artifacts phase, verified in the config reference's anchors on 2026-10-01) at axodotdev.github.io/cargo-dist.
- **axoupdater:** GitHub `axodotdev/axoupdater` (v0.10.2, 2026-08-12, MIT OR Apache-2.0).
- **crates.io API** (2026-10-01): `minisign-verify` 0.3.0 (MIT), `self-replace` 1.5.0 (Apache-2.0), `clap` 4.6.7, `clap_complete` 4.6.11, `rmcp` 3.5.0 (Apache-2.0), `toml_edit` 0.25.15, `comfy-table` 8.0.1, `dialoguer` 0.12.0, `fs4` 1.1.0, `nix` 0.31.3 (MIT); `loams` (2026-08-04, "File-based tree storage") and `loams-cli` (2025-01-22, "Loams CLI for building smart contracts") are taken; `loams` is free.
- **npm registry and PyPI** (2026-10-01): `loams`, `@loams/sdk`, `@loams/bullmq` and `@loams/durable` return 404 on npm; the `@loams` org lists no packages; `loams` is free on PyPI and `loams` is taken.
- **DNS** (2026-10-01): `loams.dev` does not resolve.
- **Agent MCP configuration:** code.claude.com/docs/en/mcp (`claude mcp add [options] <name> -- <command>`, scopes `local`, `project` and `user`, `.mcp.json`, `CLAUDE_PROJECT_DIR`); learn.chatgpt.com/docs/extend/mcp (Codex: `~/.codex/config.toml` `[mcp_servers.<name>]` `command`, `args`, `env`; `codex mcp add <name> -- <command>`); cursor.com/docs/context/mcp (`.cursor/mcp.json`, `~/.cursor/mcp.json`, `type: "stdio"`; no CLI); Windsurf: `~/.codeium/windsurf/mcp_config.json` with `mcpServers`, `command`, `args`, `serverUrl` (from third-party guides at fast.io and grafana.com; verify against Windsurf's own docs in CLI1 Task 10).
