# CLI2 — Release Pipeline, Variants, Installer and Self-Update Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, file formats, keys, flags), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: Planned** (2026-10-01). Track CLI (D298). Branches `cli2-t<N>`, stacked; PRs target `main`. **Starts after** CLI1 and **D33's rename PR** (binary `loam`, crates renamed); Task 1 (the server split) is the first PR after the rename, because the rename already invalidates every open `crates/operon` branch. **Publishing** (Task 9) additionally waits for the transfer to `ostrium-labs/loam` (user memory), the `loams.dev` DNS (Q281) and the signing key (Q282). Everything before Task 9 runs in CI with publishing off. Names below are post-rename (`loam`, `crates/loam`, `loam-cli`, `loam-server`); if the rename chose other crate names, Task 0 maps them.

**Goal:** Ship design §30's distribution half (D286, D292–D294, D296, D297):
- the **server split** (`loam-server`) so a client-only `cli` variant builds without the engine;
- **prebuilt variants** `cli`, `standard`, `full` for Linux x86_64/aarch64 and macOS aarch64, with a CI guard on their feature sets;
- the **release pipeline** on cargo-dist 0.33: archives, SHA-256, GitHub artifact attestations, and a **minisign-signed release manifest**;
- **`install.sh`**, the `curl -fsSL https://loams.dev/install.sh | sh` installer, with its test suite;
- **`loam self-update`** with verification, rollback and the daily update check;
- **variant downloads** in `stack create` (and the MCP tool's `allow_download`), plus `--from-source`;
- **`loam pkg add`** and the `add_package` MCP tool;
- service-unit generators for `stack run` (Q288's opt-in form).

**Architecture:**
- `crates/loam` becomes a thin binary crate whose engine library is `loam-server` behind the default feature `server`; `crates/loam/src/lib.rs` re-exports `loam_server::*` so the existing tests are untouched.
- Variants are declared once in `release/variants.toml`. cargo-dist builds them (thin variant packages, if Task 0's spike shows that works) or a Loam-owned matrix job does (Q293). Either way every archive is `loam-<variant>-<target>.tar.xz` holding a binary named `loam`.
- A `build-global-artifacts` job (cargo-dist's custom-job hook) assembles `loam-release.json` from the built archives, signs it with minisign (`loam-release.json.minisig`), writes `SHA256SUMS`, stamps `install.sh` with the version and public keys, and uploads all four.
- `loam-cli::release` is the one module that knows the manifest: fetch, verify (embedded public keys, `minisign-verify`), pick an artifact, download, check SHA-256, extract. `self-update`, the variant download and `install.sh`'s Rust-side tests all use it.

**Tech Stack:**
- cargo-dist 0.33.0 (MIT OR Apache-2.0; installed in CI by its own installer, pinned by version in `dist-workspace.toml`).
- New Rust dependencies of `loam-cli`: `minisign-verify` 0.3 (MIT), `self-replace` 1.5 (Apache-2.0), `xz2` 0.1 (MIT OR Apache-2.0) or `lzma-rs` (MIT) for `.tar.xz` (Task 0 picks one: `xz2` links liblzma, `lzma-rs` is pure Rust), `sha2` 0.10 (MIT OR Apache-2.0; check it is already in the tree).
- Release tooling only (never linked): `minisign` 0.12 (ISC) in the signing job; `bats-core` 1.11 (MIT) and `shellcheck` 0.10 (GPL-3.0, run as a CI tool only, not distributed) for `install.sh`; `gh` for attestation checks.
- CI runners: `ubuntu-22.04` (x86_64, glibc 2.35), `ubuntu-22.04-arm` (aarch64), `macos-15` (aarch64).

**Spec:**
- [`docs/design/30-loam-cli.md`](../design/30-loam-cli.md): §4.2 (split), §9 (variants), §14 (packages), §17 (distribution), §18 (testing), §20 risks 1–3, 8–10.
- [`docs/design/13-decision-log.md`](../design/13-decision-log.md) (after the integrator merges [`_pending/30-log.md`](../design/_pending/30-log.md)): D286, D292–D294, D296, D297; D29, D33, D262.
- [`docs/plans/2026-10-01-cli1-local-cli-and-mcp.md`](2026-10-01-cli1-local-cli-and-mcp.md): as built, its "Rulings made during execution".
- cargo-dist book: config reference and "Customizing CI" (custom job hooks), read in Task 0 at the pinned version.

## Global Constraints

Same as the M1 overview §8 and CLI1's Global Constraints, plus:
- **Nothing is published** (no GitHub Release, no tag push from CI, no crates.io, npm or PyPI upload) until Task 9, and Task 9 runs only with the owner's go-ahead after Q281 and Q282 are answered. Every workflow added before Task 9 sets `publish = false` (or skips the host and publish jobs) on pull requests and branches.
- **Variants never contain** `failpoints`, `cluster-tests` or `durable-mysql` (D29, D260); the Task 2 guard enforces it.
- **Every download is verified** before use: manifest signature with an embedded key, then the archive's SHA-256 from the signed manifest. No code path skips verification, including tests (tests sign fixtures with a test key and build the binary with `LOAM_TEST_PUBKEY` compiled in only under `cfg(test)` or the `release-test-key` feature, never in release builds).
- **Secrets in CI:** the minisign secret key exists only in the GitHub environment `release` (required reviewers, Q282); no job outside that environment can read it; PR workflows sign with the committed test key.
- **The build machine:** release-profile builds of the server variants are CI-only. Locally, only `cargo build -p loam-cli` and the `cli` variant are built; never more than one cargo build at a time.
- **Commit areas:** `cli`, `release`, `server`, `ci`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **Archive names:** `loam-<variant>-<target>.tar.xz`, each containing `loam`, `LICENSE`, `NOTICE`, `README.md`; the binary inside is always `loam` | One name on `PATH` whatever the variant (design §9) | None |
| 2 | **The signed object is the manifest**, not each archive: `loam-release.json` lists every archive's SHA-256 and is signed once; `SHA256SUMS` is a convenience copy | One signature to verify, and it covers variant metadata (features) too | None |
| 3 | **Two public keys are embedded** (`release/minisign.pub` holds `current` and `next`); a manifest verifies if either key signed it | Key rotation without a flag day (design §20 risk 8) | Losing both keys needs a release signed with a new key that old binaries cannot verify: users reinstall with `install.sh` |
| 4 | **The default install variant is `standard`** (Q287's proposed answer) | One command gives a working stack | `cli` users pass `--variant cli`; changing the default later is a one-line installer change |
| 5 | **`self-update` keeps exactly one previous binary** (`bin/loam.prev`) | Enough for `--rollback`; disk use bounded | None |
| 6 | **The update check reads the same manifest URL** as `self-update` and caches `{checked_at, latest}` for 24 h; it runs only after a command finished, in a detached task bounded by 2 s, and never blocks or changes the exit code | Design §7 | None |
| 7 | **Release URLs:** the CLI uses `https://github.com/ostrium-labs/loam/releases/{latest/download,download/v<version>}/loam-release.json` directly; `LOAM_RELEASES_URL` overrides the base for mirrors and air-gapped installs; `loams.dev` is a redirect for humans (design §17.2) | Fewer hops and no dependency on the website's availability | None |
| 8 | **`pkg add` runs the package manager as a child process**, never edits manifests itself | The package manager owns lockfile semantics | None |

## Review Focus

1. **No unverified byte is executed.** `self-update`, the variant download and `install.sh` verify the signature and SHA-256 before extracting or running anything. Tests: Task 4 `tampered_archive_is_checksum_mismatch`, `manifest_signed_by_unknown_key_is_signature_invalid`; Task 5 `install_refuses_checksum_mismatch`; Task 6 `self_update_refuses_tampered_archive`.
2. **The split changes no behaviour.** Every `crates/loam/tests/**` test passes unchanged, and `cargo tree -p loam --no-default-features` contains no engine crate. Tests: Task 1.
3. **Variant feature sets are exactly design §9.2's.** Test: Task 2's guard.
4. **`install.sh` is POSIX and idempotent** (PATH lines once, receipt rewritten). Tests: Task 5's bats suite on dash, bash and zsh.

## File structure

```
crates/loam-server/                          # new: the engine library moved out of crates/loam (Task 1)
crates/loam/{Cargo.toml,src/{lib.rs,main.rs}}   # thin binary; `server` feature
crates/loam-cli/src/release/{mod.rs,manifest.rs,verify.rs,fetch.rs,extract.rs,receipt.rs}
crates/loam-cli/src/{self_update.rs,update_check.rs,pkg.rs,stack/variant.rs,stack/service.rs}
crates/loam-cli/tests/{release.rs,self_update.rs,pkg.rs,service.rs}  tests/fixtures/release/**
crates/loam-variant-{standard,full,cli}/     # only if Task 0's spike picks thin packages (Q293)
release/{variants.toml,packages.toml,minisign.pub,minisign-test.pub,minisign-test.key,install.sh,tests/install.bats,tests/fixture-server.py}
dist-workspace.toml
.github/workflows/{release.yml,release-manifest.yml,installer.yml}  ci.yml (variant guard, cli-variant build)
docs/guides/{install.md,cli.md}  docs/design/30-loam-cli.md  CHANGELOG.md
```

### Task 0: Reconcile, and the cargo-dist spike

**Files:** read the post-rename tree (`crates/loam/**`, `crates/loam-cli/**`, the CLI1 rulings table), `Cargo.toml`. A throwaway branch `cli2-spike` (never merged) for the spike. Fill "Rulings made during execution".

**Checks** (record each with the command):
- The rename's actual crate and binary names; whether the repository is already `ostrium-labs/loam`; Q281–Q284, Q287 and Q293 answers so far.
- **The spike (Q293):** on `cli2-spike`, add `crates/loam-variant-standard` and `crates/loam-variant-full` (each `[[bin]] name = "loam"`, `main.rs` = `fn main() -> std::process::ExitCode { loam::main() }`, features forwarded), run `dist init` with design §17.1's config, then `dist plan` and `dist build --artifacts=local --target x86_64-unknown-linux-gnu` on a CI runner (not locally: release builds of two server variants exceed this machine's budget). Record: whether precise builds keep the two `loam` binaries apart, the archive names cargo-dist produces (and whether `bin-aliases` or package-level archive naming can produce Ruling 1's names), whether `build-global-artifacts-jobs` outputs are uploaded to the release, and the unified checksum file's name. Decide: **thin packages** (cargo-dist builds every variant) or **matrix job** (cargo-dist builds `standard`; `release.yml` builds `full` and `cli` with `cargo build -p loam --profile dist`).
- `min-glibc-version` behaviour for `ubuntu-22.04` builds (expect 2.35).
- Whether `sha2` is already in the tree; `xz2` vs `lzma-rs` for extraction (size, license, `cargo deny`).

**Commit:** `docs: reconcile CLI2 with main and record the cargo-dist spike`.

### Task 1: The server split (D297)

**Files:** `crates/loam-server/**` (moved by `git mv`: `server.rs`, `cluster.rs`, `meta_backend.rs`, `api/`, `pg/`, `mysql_wire/`, and their unit tests), `crates/loam/{Cargo.toml,src/lib.rs,src/main.rs}`, `Cargo.toml`, `.github/workflows/ci.yml`. **PR size:** large in lines moved (renames), about 300 lines changed.

**Produces:**

```toml
# crates/loam/Cargo.toml
[features]
default = ["server", "es", "flight", "hnsw", "qdrant", "mcp"]
server = ["dep:loam-server"]
es = ["server", "loam-server/es"]          # every engine feature forwards and implies `server`
# … flight, hnsw, qdrant, mcp, tikv, durable, durable-tikv, durable-mysql, pgwire, mysql-wire, stream-grpc, live, jobs, console
failpoints = ["server", "loam-server/failpoints"]
cluster-tests = ["server"]
```

```rust
// crates/loam/src/lib.rs
#[cfg(feature = "server")] pub use loam_server::*;
pub fn main() -> std::process::ExitCode;   // the old main.rs body, callable by variant packages (Task 0)
```

**Semantics:** design §4.2. Without `server`, the `dev`, `standalone`, `cluster`, `warm` and `durable` commands are still parsed (so `loam dev --help` explains itself) and exit 6 with `feature_not_in_variant` and the hint `loam self-update --variant standard`. `BuildInfo.features` gains `server`.

**Tests:** every existing `crates/loam/tests/**` test passes unmodified (CI's existing jobs); new: `cli_variant_has_no_engine_crates` (CI step: `cargo tree -p loam --no-default-features -e normal --prefix none` contains none of `loam-server`, `loam-query`, `loam-collection`, `loam-meta`, `loam-log`, `loam-store`, `lance` or `datafusion`; `tantivy` is allowed, because `loam-cli`'s docs search uses it); `cli_variant_builds_and_runs_client_commands` (CI job `cli-variant`: `cargo build -p loam --no-default-features`, then `loam version -o json` lists no `server` and `loam dev` exits 6 with `feature_not_in_variant`); `server_flags_unchanged` (the CLI1 golden `--help` files for `dev`/`standalone`/`cluster` still match).

**Commit:** `server: move the engine library into loam-server`.

### Task 2: Variant definitions and the guard

**Files:** `release/variants.toml`, `crates/loam-cli/src/release/mod.rs` (the `Variant` type), `crates/loam-cli/tests/release.rs`, `.github/workflows/ci.yml` (step `variant guard`), and the thin packages if Task 0 chose them. **PR size:** about 300 lines.

**Produces:**

```toml
# release/variants.toml
[variant.cli]
features = []
default-features = false
targets = ["x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu", "aarch64-apple-darwin"]   # + x86_64-pc-windows-msvc if Q285 says yes
[variant.standard]
features = ["server", "es", "flight", "hnsw", "qdrant", "mcp", "pgwire", "durable"]
default-features = false
targets = ["x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu", "aarch64-apple-darwin"]
[variant.full]
features = ["server", "es", "flight", "hnsw", "qdrant", "mcp", "pgwire", "durable", "tikv", "durable-tikv", "stream-grpc", "mysql-wire"]   # + "live", "jobs" when merged
default-features = false
targets = ["x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu", "aarch64-apple-darwin"]
```

```rust
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
pub struct VariantDef { pub features: Vec<String>, pub default_features: bool, pub targets: Vec<String> }
pub fn variants() -> &'static BTreeMap<String, VariantDef>;      // include_str!("../../../../release/variants.toml")
pub fn smallest_covering(features: &BTreeSet<&str>, target: &str) -> Option<&'static str>;
pub const FORBIDDEN: &[&str] = &["failpoints", "cluster-tests", "durable-mysql"];
```

**Semantics:** the build passes `LOAM_VARIANT=<name>` in the environment so `BuildInfo.variant` is the variant name (CLI1's `option_env!`). `smallest_covering` orders variants `cli < standard < full` and returns the first whose features ⊇ the requested set and whose targets include `target`.

**Tests:** `variants_never_include_forbidden_features`; `every_variant_feature_exists_in_crates_loam` (parses `crates/loam/Cargo.toml`); `standard_matches_design_section_9_2` (exact list); `full_is_a_superset_of_standard`; `smallest_covering_picks_standard_for_pg_and_full_for_tikv`; CI `variant guard`: for each variant, `cargo metadata` resolves its features without error, and (thin packages) each package's features equal `variants.toml`'s.

**Commit:** `release: declare the cli, standard and full variants`.

### Task 3: cargo-dist and the release workflow (dry-run)

**Files:** `dist-workspace.toml`, `.github/workflows/release.yml` (generated by `dist generate`, then the custom job wired in), `Cargo.toml` (`[profile.dist]`). **PR size:** mostly generated YAML; about 150 hand-written lines.

**Semantics:** design §17.1's `[dist]` block, plus the per-variant setup Task 0 chose. `[profile.dist] inherits = "release", lto = "thin", codegen-units = 1, strip = "symbols"`. On `pull_request` (paths: `dist-workspace.toml`, `release/**`, `.github/workflows/release*.yml`, `crates/loam/Cargo.toml`): `dist plan` and one `dist build` for `x86_64-unknown-linux-gnu` × `standard`, uploads as workflow artifacts only. On a `v*` tag: the full matrix; **host and publish jobs are disabled until Task 9** (`publish-jobs = []`, and `release.yml` exits before `host` unless `vars.LOAM_PUBLISH == "true"`).

**Tests:** the PR run itself (green `plan` and one `build`); `release_workflow_is_generated_from_config` (CI step: `dist generate --check` reports no drift).

**Commit:** `release: build variants with cargo-dist (no publishing)`.

### Task 4: The release manifest, signing and verification

**Files:** `crates/loam-cli/src/release/{manifest.rs,verify.rs,fetch.rs,extract.rs}`, `crates/loam-cli/tests/release.rs`, `crates/loam-cli/tests/fixtures/release/**` (a fixture release signed with the test key), `release/{minisign.pub,minisign-test.pub,minisign-test.key}`, `.github/workflows/release-manifest.yml` (cargo-dist `build-global-artifacts-jobs` hook), `scripts/release/make-manifest.py`. **PR size:** about 900 lines.

**Produces:**

```rust
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
pub struct Manifest { pub schema: u32 /* 1 */, pub version: String, pub channel: String, pub published_at: String,
                      pub output_schema: u32, pub docs_version: String, pub artifacts: Vec<Artifact> }
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
pub struct Artifact { pub variant: String, pub target: String, pub url: String, pub sha256: String, pub size: u64, pub features: Vec<String> }
pub struct PublicKeys(Vec<minisign_verify::PublicKey>);
impl PublicKeys { pub fn embedded() -> Self; /* release/minisign.pub: current + next */ }
pub fn verify_manifest(bytes: &[u8], sig: &[u8], keys: &PublicKeys) -> Result<Manifest, CliError>;   // signature_invalid | manifest_invalid
pub async fn fetch_manifest(http: &Http, base: &Url, version: Option<&str>) -> Result<Manifest, CliError>;   // fetch + verify
pub fn pick<'m>(m: &'m Manifest, variant: &str, target: &str) -> Result<&'m Artifact, CliError>;           // variant_not_found
pub async fn download_verified(http: &Http, a: &Artifact, dest_dir: &Path) -> Result<PathBuf, CliError>;   // checksum_mismatch; size checked
pub fn extract_binary(archive: &Path, dest: &Path) -> Result<PathBuf, CliError>;                           // only `loam`; rejects path traversal
```

**Semantics:** Ruling 2 and 3. `make-manifest.py` (run in the hook job) reads cargo-dist's plan JSON plus the variant archives, computes SHA-256 and sizes, reads features from `variants.toml`, writes `loam-release.json` (keys sorted, `published_at` from the tag commit time), runs `minisign -S -s $KEY -m loam-release.json -t "loam <version>"`, writes `SHA256SUMS`, and stamps `release/install.sh` (version, both public keys) into `install.sh`. PR runs sign with `release/minisign-test.key` (committed; test only), tag runs with the `release` environment's `LOAM_MINISIGN_KEY`. `extract_binary` accepts only a regular file named `loam` at the archive root and refuses links, absolute paths and `..`.

**Tests:** `fixture_manifest_verifies_with_the_test_key`; `manifest_signed_by_unknown_key_is_signature_invalid`; `either_embedded_key_verifies` (current and next); `truncated_signature_is_signature_invalid`; `schema_2_manifest_is_manifest_invalid`; `pick_unknown_variant_is_variant_not_found`; `tampered_archive_is_checksum_mismatch`; `size_mismatch_is_checksum_mismatch`; `extract_refuses_traversal_and_symlinks`; `make_manifest_golden` (the script over a fixture plan → byte-identical `loam-release.json`); the hook job's PR run produces a manifest that `loam-cli`'s verifier accepts (CI step).

**Commit:** `release: sign a release manifest and verify it in the CLI`.

### Task 5: `install.sh`

**Files:** `release/install.sh`, `release/tests/{install.bats,fixture-server.py}`, `.github/workflows/installer.yml`. **PR size:** about 600 lines (script about 250).

**Semantics:** design §17.2 exactly. Flags: `--variant V`, `--version X.Y.Z`, `--yes`, `--no-init`, `--no-modify-path`, `--require-signature`, `--help`; env `LOAM_VERSION`, `LOAM_VARIANT`, `LOAM_HOME`, `LOAM_RELEASES_URL` (Ruling 7). Exit codes: 0; 1 (unexpected); 2 (usage); 6 (unsupported platform); 9 (checksum or signature). Messages on stderr; a final summary on stdout. `receipt.json` `{"version", "variant", "target", "installed_at", "install_method": "install.sh", "source_url"}`. PATH: `$LOAM_HOME/env` (`export PATH="$LOAM_HOME/bin:$PATH"` guarded against duplicates) and `env.fish`; rc edits append one line each, once. The init prompt reads `/dev/tty`. `minisign` verification uses `minisign -Vm loam-release.json -x loam-release.json.minisig -P <key>` for each embedded key.

**Tests** (`install.bats`, run under `dash`, `bash` and `zsh` as `/bin/sh`; `fixture-server.py` serves a fixture release over `http://127.0.0.1:<port>` with `LOAM_RELEASES_URL`; the fixture archives contain a stub `loam` script that prints a fixed `version -o json`):
`installs_standard_by_default`; `installs_a_pinned_version_and_variant`; `install_refuses_checksum_mismatch` (exit 9, nothing in `bin/`); `require_signature_without_minisign_fails`; `bad_signature_fails_with_minisign` (when `minisign` is installed in the job); `unknown_platform_exits_6` (`uname` shimmed); `path_lines_added_once` (run twice); `no_modify_path_leaves_rc_files`; `receipt_is_written`; `no_init_skips_prompt`; `yes_is_non_interactive`; `shellcheck_clean` (`shellcheck -s sh release/install.sh`). Workflow `installer.yml`: Ubuntu 22.04 and 24.04, Debian 12 and Fedora 41 containers, and a `macos-15` runner.

**Commit:** `release: add install.sh and its test suite`.

### Task 6: `loam self-update` and the update check

**Files:** `crates/loam-cli/src/{self_update.rs,update_check.rs,release/receipt.rs}`, `crates/loam-cli/src/lib.rs` (the `self-update` command), `crates/loam-cli/tests/self_update.rs`, `crates/loam/tests/cli/self_update.rs`. **PR size:** about 700 lines.

**Produces:**

```rust
#[derive(clap::Args, Debug)] pub struct SelfUpdateArgs { #[arg(long)] check: bool, #[arg(long)] version: Option<semver::Version>,
    #[arg(long)] variant: Option<String>, #[arg(long)] rollback: bool, #[arg(long)] yes: bool }
#[derive(serde::Serialize, schemars::JsonSchema)] pub struct SelfUpdateReport { pub previous: String, pub current: String, pub variant: String, pub action: Action /* updated | up_to_date | rolled_back | checked */, pub update_available: Option<bool> }
pub async fn self_update(ctx: &Context, args: &SelfUpdateArgs) -> Result<SelfUpdateReport, CliError>;
pub fn maybe_check_in_background(ctx: &Context);   // Ruling 6
```

**Semantics:** design §17.3's eight steps, Rulings 5–7. `managed_install` when the receipt is missing or `current_exe()` (canonicalized) is not `$LOAM_HOME/bin/loam`; the hint is `cargo install loamdb` when the exe is under `~/.cargo/bin`, else "update it with the package manager that installed it". The smoke test runs the new binary with a 10 s timeout and compares `version` and the sorted `features` with the manifest artifact. The swap is `self_replace::self_replace`; on any failure after the copy to `loam.prev`, the previous binary stays in place and `.loam-new` is removed. The update check (`update_check.rs`) obeys design §7's conditions and writes `cache/update-check.json`; the notice prints only in `table` mode, on stderr, after the command's own output.

**Tests:** unit (`Context::for_test`, the fixture release): `self_update_updates_and_keeps_prev`; `up_to_date_is_exit_0`; `downgrade_needs_yes`; `rollback_restores_prev`; `foreign_install_is_managed_install`; `self_update_refuses_tampered_archive` (exit 9, binary unchanged); `smoke_test_mismatch_aborts` (the new binary reports another version); `variant_switch_updates_receipt`; `check_does_not_download`; `update_check_skipped_in_json_mode_ci_and_mcp`; `update_check_runs_once_a_day`. End-to-end (`crates/loam/tests/cli/self_update.rs`): copy the built `loam` into a temp `LOAM_HOME/bin`, write a receipt, serve a fixture release whose archive holds a stub, run `loam self-update --yes -o json` → the stub is installed and `loam.prev` is the original.

**Commit:** `cli: add self-update with signature checks and rollback`.

### Task 7: Variant downloads in `stack create`, and `--from-source`

**Files:** `crates/loam-cli/src/stack/{variant.rs,resolve.rs}`, `crates/loam-cli/src/mcp/tools.rs` (`allow_download`), `crates/loam-cli/tests/release.rs`, `crates/loam/tests/cli/stack.rs`. **PR size:** about 500 lines.

**Semantics:** design §9.3. `resolve` replaces CLI1's `feature_not_in_variant` with: `smallest_covering` → if `variants/<version>/<variant>/loam` exists and its `version -o json` matches, use it; else, with consent (`--allow-download`, a TTY prompt, or MCP `allow_download: true`), `download_verified` + `extract_binary` into that directory, then smoke-test it; without consent `download_consent_required` (exit 3) with the size in `details.size_bytes`. No covering variant → `feature_not_in_variant` with the hint `--from-source`. `--from-source` (CLI only, never MCP): checks `cargo` on `PATH`, prints the warning (time, memory, the `-j` advice), runs design §9.3's `cargo install` command with `--root variants/<version>/src-<hash of feature list>/`, and uses the result. `stack restart --upgrade` reuses the same path for the new version.

**Tests:** `installed_variant_is_used_when_it_covers`; `download_needs_consent_without_tty`; `download_is_verified_and_cached`; `cached_variant_with_wrong_version_is_replaced`; `mcp_stack_create_with_allow_download_downloads`; `from_source_builds_the_cargo_command` (the argv only: a fake `cargo` on `PATH` records it); `no_covering_variant_hints_from_source`.

**Commit:** `cli: download the matching server variant for a stack`.

### Task 8: `loam pkg add` and the `add_package` tool

**Files:** `release/packages.toml`, `crates/loam-cli/src/pkg.rs`, `crates/loam-cli/src/mcp/tools.rs`, `crates/loam-cli/tests/pkg.rs`, `crates/loam-cli/tests/golden/mcp-tools.json` (additive). **PR size:** about 600 lines.

**Produces:**

```rust
pub enum Ecosystem { Npm(NodePm /* Pnpm | Npm | Yarn | Bun */), Python(PyPm /* Uv | Poetry */), Cargo }
pub struct PkgPlan { pub ecosystem: Ecosystem, pub argv: Vec<String>, pub manifest: PathBuf, pub packages: Vec<String> /* with version specs */ }
pub fn detect(project: &Path, language: Option<Language>) -> Result<Ecosystem, CliError>;
pub fn resolve(names: &[String], eco: &Ecosystem, cli_version: &semver::Version, from_mcp: bool) -> Result<Vec<String>, CliError>;
pub fn plan(project: &Path, args: &PkgAddArgs, cli_version: &semver::Version, from_mcp: bool) -> Result<PkgPlan, CliError>;
pub async fn run(ctx: &Context, plan: &PkgPlan, timeout: Duration) -> Result<PkgReport, CliError>;
```

**Semantics:** design §14 and Ruling 8. Version specs: npm `<name>@~<major>.<minor>.0`, PyPI `<name>>=<major>.<minor>,<<major>.<minor+1>` (quoted argv, no shell), crates `<name>@<major>.<minor>`. A logical name maps per `packages.toml`; a registry name in the allow-list (any value in `packages.toml`) passes; anything else from MCP → `usage` ("add_package installs Loam packages only"), from the terminal → passes through after a stderr warning. `--dry-run` returns the plan. `add_package` (design §12.1): `{package, dev, dry_run}` → `{ecosystem, command, manifest, exit_code, output_tail}`, 5-minute timeout, run in the MCP server's project directory.

**Tests:** `detects_each_package_manager_from_lockfiles` (fixture dirs for pnpm, npm, yarn, bun, uv, poetry, cargo); `requirements_txt_alone_is_unsupported`; `two_ecosystems_without_tty_is_confirmation_required`; `logical_names_map_per_ecosystem`; `version_specs_match_the_cli_minor`; `mcp_refuses_names_outside_the_allow_list`; `terminal_passes_unknown_names_with_a_warning`; `runs_the_package_manager_with_exact_argv` (fake `pnpm`/`uv`/`cargo` on `PATH`); `add_package_tool_output_tail_is_capped`; `tool_list_golden_gains_add_package_only`.

**Commit:** `cli: add pkg add and the add_package tool`.

### Task 9: Service units, the install guide and the first release

**Files:** `crates/loam-cli/src/stack/service.rs`, `crates/loam-cli/tests/service.rs`, `docs/guides/{install.md,cli.md}`, `docs/design/30-loam-cli.md` (as-built notes), `CHANGELOG.md`, `dist-workspace.toml` and `release.yml` (publishing on), and a PR to `loam-cloud` for the `loams.dev` redirects. **PR size:** about 500 lines in this repository.

**Semantics:**
- `loam stack service install --name N --kind systemd|launchd` writes `~/.config/systemd/user/loam-<n>.service` (`ExecStart=<abs loam> stack run --name <n>`, `Restart=on-failure`, `KillMode=control-group`, `TimeoutStopSec=35`) or `~/Library/LaunchAgents/dev.loams.stack.<n>.plist` (`KeepAlive` on crash), prints the `systemctl --user enable --now` / `launchctl bootstrap` command (never runs it), and records `service = "systemd"` in `stack.toml`; `stack start`/`stop` then call `systemctl --user`/`launchctl` instead of spawning (Q288's opt-in form). `service uninstall` reverses it.
- `docs/guides/install.md`: the one-liner, the pinned form, variants and their engines, verifying by hand (`minisign`, `gh attestation verify`), the trust model (design §17.2), uninstall (`rm -rf ~/.loam` and the rc lines), and `cargo install loamdb`.
- **The first release**, only with the owner's go-ahead: set `vars.LOAM_PUBLISH = "true"`, enable cargo-dist's `host` job, push `v<version>`; check the release page lists every archive, `SHA256SUMS`, `loam-release.json(.minisig)` and `install.sh`; run `curl -fsSL https://github.com/ostrium-labs/loam/releases/latest/download/install.sh | sh -s -- --yes --no-init` on a clean Ubuntu container and macOS runner; `gh attestation verify` on one archive. The `loam-cloud` PR adds `app/install.sh/route.ts` and `app/releases/[...path]/route.ts` returning 302 to the GitHub URLs (Q281).

**Tests:** `systemd_unit_golden`; `launchd_plist_golden`; `service_mode_start_calls_systemctl` (fake `systemctl` on `PATH`); `service_uninstall_removes_unit_and_record`; the release checklist above, recorded in this plan's "Rulings made during execution" table with links.

**Commit:** `cli: generate service units, document installation and publish the first release`.

## Rulings made during execution

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| — | (Task 0 fills this table) | | |

## Self-review

| Design §30 requirement | Task |
|---|---|
| D297 the server split and the `cli` variant | 1 |
| D286 variants, targets, forbidden features, download on `stack create`, `--from-source` | 2, 3, 7 |
| D292 cargo-dist, checksums, attestations, signed manifest, no publishing before the rename and move | 3, 4, 9 |
| D293 `install.sh` | 5 |
| D294 `self-update`, rollback, managed installs refused, update check | 6 |
| D296 `pkg add` and `add_package` | 8 |
| Q288 service units (opt-in) | 9 |
