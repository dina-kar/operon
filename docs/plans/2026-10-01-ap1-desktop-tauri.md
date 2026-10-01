# AP1 — Loams Desktop on Tauri 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (identifiers, command names, paths, exit codes), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: Planned** (2026-10-01). **Slot: track AP, after AP1a Task 3 (the cordis host) and CLI1 Tasks 1–6 (the CLI's output contract and stacks).** Branches `ap1-t<N>`, stacked; PRs target `main`. Tasks 1–6 need nothing from the auth plan. Tasks 7–9 (sign-in, keychain, approvals) need AP0 and, against a real server, the unified auth plan (D111, Q30); before it they run against `operon-apps-mock`. Publishing (Task 11) waits for D33's rename, the move to `ostrium-labs/loam`, signing identities (Q420, Q421) and the updater key.

**Goal:** Ship **Loams Desktop**: a Tauri 2 shell around the cordis console (AP1a) that (1) supervises local `loams` stacks through the CLI's stack layer (§30 D285), (2) signs in to any Loam instance with OAuth 2.1 + PKCE in the system browser and keeps credentials in the OS keychain, (3) carries every console request through a Rust network bridge so tokens never reach JavaScript, and (4) updates itself with signed releases. It borrows the harness desktop's patterns (loopback-only, port 0, a validated readiness line, zero shell capability in the webview) and fixes its gaps (no auth token, no supervision after readiness, no logs, hard kill, no single instance, no updater) (§37 §3.1).

**Architecture:**
- **`web/apps/desktop/`** (pnpm package `@loams/desktop`): the Vite entry that boots the cordis console host with the `desktop` plugin set (AP1a) plus `@loams/platform-tauri`, which provides the platform services over IPC.
- **`web/apps/desktop/src-tauri/`** (crate `loams-desktop`, its own Cargo workspace with its own `Cargo.lock`, **not a member of the engine workspace**): modules `cli` (runs `loams … --output json`, parses D283), `stacks` (status polling, restart policy), `net` (the fetch bridge), `auth` (PKCE, refresh, keychain), `envs` (profiles), `deeplink`, `tray`, `updates`, `logs`.
- **The sidecar is the `loams` binary** (the CLI's `standard` variant, D286), bundled as a Tauri `externalBin` per target triple. The desktop never spawns `loams dev` itself: it calls `loams stack start|stop|describe|logs` and the CLI owns the processes, so stacks created in a terminal appear in the app and the other way round (§37 §6.2, D429).
- **One window, one bundled origin, no remote content.** The webview loads only the bundled console. Its only network path is the `net_fetch` command (D430). Third-party plugin frames load from the `loams-plugin://` scheme with `connect-src 'none'` (AP1a Task 6).

**Tech Stack:** Rust 1.97.1 (the repository's toolchain), Tauri 2.12 (`tauri` 2.12.1, 2026-09-30) with `tauri-build` 2.x; plugins `tauri-plugin-single-instance` 2.5 (feature `deep-link`), `tauri-plugin-deep-link` 2.6, `tauri-plugin-updater` 2.13 (minisign signatures, which cannot be disabled), `tauri-plugin-notification`, `tauri-plugin-log`, `tauri-plugin-window-state`, `tauri-plugin-opener` (scoped); **not** `tauri-plugin-stronghold` (deprecated, removed in Tauri v3, plugins-workspace#3494) and **not** `tauri-plugin-shell` in any capability; `keyring` 4.2 (macOS Keychain, Windows Credential Manager, Linux Secret Service); `tauri-plugin-oauth` 2.1 for the loopback redirect listener if Task 0's licence check passes, else a 60-line listener of our own; `reqwest` (rustls) for the bridge and auth; `oauth2` 5.x for PKCE; `serde`, `tokio`. Frontend: the AP1a stack (React 19, Vite, cordis), `@connectrpc/connect` 2.x and `@connectrpc/connect-web` 2.x. `tauri-driver` + WebdriverIO for end-to-end tests on Linux and Windows (macOS has no WebDriver for WKWebView, verify), Playwright for the console in a browser. Task 0 confirms every version.

**Spec:**
- [§37](../design/37-desktop-and-mobile-apps.md) §5 (cordis console), §6 (desktop), §8 (protos), §11 (security), §12 (testing).
- [§30](../design/30-loam-cli.md) (on the `cli-design` branch until merged): D283 (output and exit codes), D284 (`LOAM_HOME`), D285 (stacks), D286 (variants), D294 (`self-update`), §8.3 (the supervisor).
- [§19](../design/19-console-identity-and-agents.md) §5.2–§5.4 (tokens, flow 2), §6 (sessions); D111.
- The harness desktop: `apps/desktop/src-tauri/src/lib.rs`, `capabilities/default.json`, `tauri.conf.json`, `scripts/prepare-runtime.mjs` (read, MIT; nothing copied unless Task 1 records it).

## Global Constraints

- **Not in the engine workspace.** `loams-desktop` has its own `[workspace]` and lockfile. It depends on no `operon-*` crate: it talks to the CLI through its JSON contract and to servers through Connect. `cargo build` in the repository root never builds it.
- **The build machine.** One cargo build at a time, the shared target (`/mnt/Projects/rust-cache/target`, set by `~/Documents/.cargo/config.toml`), `CARGO_BUILD_JOBS=4`. Tauri pulls webkit2gtk on Linux: run `tauri build` only for packaging tasks; unit tests use `cargo test -p loams-desktop --lib` with the `custom-protocol` feature off.
- **Capability lockdown (D430).** The `main` window gets exactly the permissions listed in Task 4; no `shell`, `fs`, `http`, `process`, `dialog` or unscoped `opener` permission, ever. A CI check fails if `capabilities/*.json` grants anything else.
- **Tokens never reach JavaScript.** No command returns an access or refresh token, a cookie or a key. A canary test (Task 7) asserts it.
- **Loopback only for local stacks** (D111). The bridge refuses a non-loopback origin for a profile of kind `local`.
- **No telemetry** (D284). The only automatic network calls are the update check (daily, can be turned off) and the requests the user's environments need.
- **Commit areas:** `desktop`, `web`, `ci`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **Identifiers:** product name `Loams`, bundle id `dev.loams.desktop`, URL scheme `loams`, keychain service `dev.loams.desktop`, app data dir from Tauri's `app_data_dir()`; `LOAM_HOME` stays the CLI's (`~/.loam`) and is shared | One reverse-DNS root for the domain `loams.dev`; sharing `LOAM_HOME` is what makes terminal and app stacks the same stacks | Renaming a bundle id after a store release loses updates; decided before Task 11 |
| 2 | **The desktop calls the CLI, it does not link it.** Every stack action is `loams <args> --output json`; stdout is one JSON document, stderr one error object, and the exit code is classified by D283's table. Unknown exit codes are `internal` | One supervisor of record (the CLI, §30 §8.3); the JSON contract is already snapshotted; the desktop needs no Rust dependency on `operon-cli` | A CLI change that breaks JSON breaks the app; the CLI's output-schema snapshots (D283) are the guard, and Task 2 pins `output_schema: 1` |
| 3 | **Which binary.** The bundled `loams` runs every CLI command. A stack keeps the binary recorded in its `stack.toml`; `binary_outdated` shows an **Upgrade** button that runs `stack restart --name <n> --upgrade` | Matches §30's model; never swaps a running stack's binary silently | None |
| 4 | **Stacks outlive the app.** Quitting the app leaves running stacks running (they are CLI-owned, `setsid`), and the tray shows them. A per-app setting `stop_stacks_on_quit` (default `false`) stops the stacks the app started. Q429 confirms the default | Developers' code and agents keep using the stack after the window closes; consistent with `loams stack start` from a terminal | A user surprised by a still-running stack; the tray makes it visible |
| 5 | **Restart policy, app-side.** A stack marked `keep_running` in the app's settings is restarted by the app when `stack describe` reports `crashed`: backoff 1 s, 2 s, 4 s … capped at 30 s, at most 5 restarts in 10 minutes, then it stays `crashed` with the log tail shown. Polling is every 5 s while the window is visible and every 30 s otherwise | The CLI has no restart loop (§30 §8.3); the harness had none at all | A crash loop wastes CPU for at most 10 minutes |
| 6 | **The network bridge (D430):** `net_fetch(request, channel)` performs the request in Rust and streams the body back as `Channel<FetchEvent>`; JavaScript gets a standard `Response` whose body is a `ReadableStream`. Origins are allowed only if they belong to the active environment's endpoint set (from `GetInstance` or the stack's `describe`). The bridge adds `Authorization: Bearer` (**bearer only until Q438 is decided**; Task 0 records Q438's outcome, and if the auth plan adds DPoP for desktop clients, Task 7 creates a non-exportable DPoP key in the OS keychain and the bridge signs a proof per request; Task 12's exit criteria require the decision recorded either way); it strips `Cookie`, `Authorization` and `Proxy-*` headers set by JavaScript | Connect-es takes a custom `fetch`; streaming works through a channel, which a custom URI scheme cannot do (its responder takes a complete body); tokens stay in Rust; servers need no CORS for app origins | IPC overhead on large bodies; measured in Task 5 (target: 50 MB/s, **estimate**) |
| 7 | **Sign-in is OIDC authorization code + PKCE at the instance's Authentik (open-source edition, the owner's 2026-10-01 ruling) in the system browser with a loopback redirect** (RFC 8252 §7.3: `http://127.0.0.1:<port 0>/callback`, `state` and `nonce` checked, the listener accepts one request and closes), then an RFC 8693 exchange of Authentik's token at the Loam gateway for Loam's tokens; Authentik's tokens are discarded. Public client id `loams-desktop`, an Authentik application per instance. No embedded webview sign-in, ever | RFC 8252's recommendation; whatever Authentik is configured with (passwords, TOTP, WebAuthn, upstream SSO) works; Loam keeps deciding environments and scopes (§19 §5.3); the loopback form needs no URL-scheme registration race | Corporate machines that block loopback listeners: Q-row fallback to the `loams://auth/callback` deep link with the same PKCE checks |
| 8 | **Credentials:** the refresh token in the OS keychain (`keyring`, one entry per environment: `dev.loams.desktop` / `<instance_id>:<principal_id>`), the access token in Rust memory only; refresh 5 minutes before `exp`, rotation on every refresh (OAuth 2.1 for public clients). Linux without a Secret Service: sign-in works for the session only, and the UI says so; never a plaintext file | Keychains are the platform's secret store; the harness kept credentials nowhere, the mobile harness kept a cookie in plaintext | Headless Linux users re-sign-in after restart |
| 9 | **Updates:** `tauri-plugin-updater` with a static `latest.json` per channel (`stable`, `beta`) on GitHub Releases, behind a redirect on `loams.dev/desktop/…` (like §30's `install.sh`, Q281). Its signing key is **separate from the CLI's minisign release key** (Q282) | Tauri's updater has its own signature format; two keys limit the blast radius of one leak | Two keys to guard |
| 10 | **Deep links are navigation only.** `loams://open/<env>/<path>`, `loams://approvals/<id>`, `loams://stacks/<name>`; parsed in Rust against an allowlist of patterns, and the console receives a typed route, never a raw URL. No deep link performs an action (no approve, start, delete) | Deep links are attacker-reachable from any web page or document | None |
| 11 | **Platforms.** macOS aarch64 and Linux x86_64/aarch64 with local stacks (the CLI's targets, D286); Windows x86_64 **remote-only** (no bundled server) until Q285/Q437 decide a Windows or WSL2 variant | No Windows server variant exists | Windows users cannot run local stacks from the app |

## Review Focus

1. **The webview cannot escalate.** Capability file, CSP, command allowlist, no remote content, plugin frames isolated. Tests: Task 4 (`capabilities_are_exactly_the_allowlist`, `csp_has_no_remote_sources`), Task 5 (`bridge_refuses_foreign_origin`, `bridge_strips_js_auth_headers`).
2. **Tokens never leave Rust.** Tests: Task 7 (`no_command_returns_a_secret` canary over every command's output and every log line).
3. **The CLI contract is the only stack interface.** Tests: Task 2 (`cli_error_is_classified_by_exit_code`, golden JSON fixtures).
4. **Deep links cannot trigger actions.** Tests: Task 10 (`deeplink_rejects_unknown_patterns`, `deeplink_never_dispatches_mutations`).

## File structure

```
web/apps/desktop/{package.json,index.html,vite.config.ts,src/{main.tsx,plugins.ts}}           # @loams/desktop
web/packages/platform-tauri/{package.json,src/{index.ts,fetch.ts,stacks.ts,auth.ts,deeplink.ts,notify.ts}}  # cordis plugin providing platform services
web/plugins/stacks/{package.json,src/**}                                                         # @loams/plugin-stacks (desktop only)
web/apps/desktop/src-tauri/{Cargo.toml,Cargo.lock,build.rs,tauri.conf.json,tauri.macos.conf.json,tauri.linux.conf.json,tauri.windows.conf.json}
web/apps/desktop/src-tauri/capabilities/main.json
web/apps/desktop/src-tauri/permissions/*.toml
web/apps/desktop/src-tauri/src/{main.rs,lib.rs,cli.rs,stacks.rs,net.rs,auth.rs,envs.rs,deeplink.rs,tray.rs,updates.rs,logs.rs,errors.rs}
web/apps/desktop/src-tauri/tests/{cli.rs,net.rs,auth.rs,deeplink.rs}  tests/fixtures/cli/*.json
web/apps/desktop/e2e/*.e2e.ts                                                                    # tauri-driver + WebdriverIO
scripts/desktop/{prepare-sidecar.mjs,check-capabilities.mjs}
.github/workflows/desktop.yml
docs/guides/desktop.md  THIRD_PARTY_NOTICES.md (desktop section)  docs/plans/README.md  CHANGELOG.md
```

### Task 0: Reconcile and spike

**Files:** read §30 and CLI1 as merged (or the `cli-design` branch), AP1a's as-built host, the harness desktop files listed under Spec. Fill "Rulings made during execution".

**Checks** (each recorded with its command):
- Current versions of Tauri, `tauri-build`, the plugins in the Tech Stack, `keyring`, `oauth2`; Tauri's minimum webkit2gtk on Linux; whether Tauri's bundler signs `externalBin` binaries on macOS (verify) and how sidecars are notarized.
- **Spike (throwaway, under the scratchpad, not `/tmp`):** a 40-line Tauri app that streams a 100 MB body and a 10-minute server stream through a `Channel` to a `ReadableStream`, measuring throughput and memory; whether Tauri serves `index.html` for unknown paths of the bundled frontend (decides hash vs browser router in AP1a Ruling 7).
- Whether CLI1 has merged `stack start|stop|describe|logs` and `--output json` (Tasks 2–3 need them); otherwise Tasks 2–3 run against fixtures only.

**Commit:** `docs: reconcile AP1 with main and record the Tauri spike`.

### Task 1: Scaffold, licences and notices

**Files:** `web/apps/desktop/**` scaffold, `src-tauri/{Cargo.toml,tauri.conf.json,build.rs,src/main.rs,src/lib.rs}`, `THIRD_PARTY_NOTICES.md` (desktop section), `web/pnpm-workspace.yaml` (no change if `apps/*` covers it).

**Semantics:** a window `main` (1280×820, min 900×600) loading the bundled `@loams/desktop` build. `tauri.conf.json`: `app.withGlobalTauri = false`, `app.security.csp` per Task 4, `bundle.targets` per platform file (no `"all"` with a conflicting macOS override, a harness bug), `bundle.externalBin = ["binaries/loams"]`. `build.rs` uses `tauri_build::Attributes::app_manifest` with the explicit command list so every app command needs a grant. Record in `THIRD_PARTY_NOTICES.md` that the desktop's structure follows `fendouai/deepseek-harness-desktop` (MIT, © 2026 DeepSeek) **only if** any code is copied; by default nothing is copied and the notice is not needed (§37 §3.3).

**Tests:** `cargo test -p loams-desktop --lib` (an empty smoke); `pnpm -C web --filter @loams/desktop build`.

**Commit:** `desktop: scaffold the Tauri 2 shell`.

### Task 2: The CLI bridge

**Files:** `src-tauri/src/{cli.rs,errors.rs}`, `src-tauri/tests/cli.rs`, `tests/fixtures/cli/*.json`, `scripts/desktop/prepare-sidecar.mjs`.

**Produces:**

```rust
pub struct Cli { binary: PathBuf, loam_home: PathBuf, timeout: Duration /* 60 s; `stack start` uses --wait-timeout + 10 s */ }
pub enum CliError { Usage(ErrorBody), ActionRequired(ErrorBody), NotFound(ErrorBody), Conflict(ErrorBody),
                    Unsupported(ErrorBody), Unavailable(ErrorBody), Permission(ErrorBody), Integrity(ErrorBody),
                    Internal(ErrorBody), Interrupted, Spawn(io::Error), Timeout, BadJson(String) }
pub struct ErrorBody { pub code: String, pub message: String, pub hint: Option<String>, pub details: serde_json::Value }
impl Cli {
    pub fn locate(app: &AppHandle) -> Result<Cli, CliError>;             // the bundled sidecar path; LOAM_HOME from env or ~/.loam
    pub async fn run<T: DeserializeOwned>(&self, args: &[&str]) -> Result<T, CliError>;   // appends --output json; no TTY; CI unset
    pub async fn version(&self) -> Result<VersionInfo, CliError>;       // checks output_schema == 1
}
```

**Semantics:** Ruling 2. Children get `stdin` null, `LOAM_NO_UPDATE_CHECK=1` and `LOAM_OUTPUT=json`; if the timeout passes they are killed with their whole tree: on Unix they run in their own process group (SIGTERM to the group, 5 s, SIGKILL); on Windows (only if Q437 ever bundles `loams` there, Ruling 11) each child is assigned to a Job Object with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` and the timeout terminates the job. `prepare-sidecar.mjs` copies the `loams` binary for the target triple into `src-tauri/binaries/loams-<triple>` from a CLI2 release archive, verifying the release manifest's SHA-256 (and its minisign signature when available, §30 D292), or from a local build with `--from-path` in development.

**Tests:** `cli_success_parses_json`; `cli_error_is_classified_by_exit_code` (each of 1–9 and 130 from fixtures, through a fake `loams` script); `cli_timeout_kills_process_group` (Unix); `cli_timeout_terminates_job_object_tree` (Windows CI, when a Windows `loams` exists); `cli_bad_json_is_internal`; `cli_never_inherits_tty`.

**Commit:** `desktop: run the loams CLI through its JSON contract`.

### Task 3: Stacks: status, actions, restart policy, logs

**Files:** `src-tauri/src/stacks.rs`, `src-tauri/tests/stacks.rs` (with the fake CLI), `web/packages/platform-tauri/src/stacks.ts`, `web/plugins/stacks/**`.

**Produces (commands):** `stacks_list() -> Vec<StackSummary>`, `stack_describe(name) -> StackDescription`, `stack_start(name)`, `stack_stop(name)`, `stack_restart(name, upgrade: bool)`, `stack_logs(name, follow: bool, channel: Channel<LogLine>)`, `stack_set_policy(name, keep_running: bool)`. `stack create` and `stack delete` are **not** commands in AP1: the plugin shows the exact CLI line to run, so destructive or disk-touching actions stay in a terminal with `--yes` (§30 D283, D287).

**Semantics:** Rulings 3–5. The `@loams/plugin-stacks` cordis plugin (`inject: ['platform.stacks']`, so it activates only on desktop) shows stacks, engines, endpoints and `.env.loam` variable names (never values, D288), a log view, and Start/Stop/Restart/Upgrade.

**Tests:** `crashed_stack_restarts_with_backoff`; `restart_budget_exhausted_stays_crashed`; `stop_on_quit_stops_only_app_started`; `logs_follow_streams_lines`; `env_values_never_returned`; a Vitest of the plugin with a fake `platform.stacks` service (`renders_running_and_crashed`, `upgrade_shown_when_binary_outdated`).

**Commit:** `desktop: show and supervise local stacks`.

### Task 4: Capabilities, CSP and the command allowlist

**Files:** `src-tauri/capabilities/main.json`, `src-tauri/permissions/*.toml`, `src-tauri/tauri.conf.json` (CSP), `scripts/desktop/check-capabilities.mjs`, `.github/workflows/desktop.yml` (job `capabilities`).

**Semantics:** `main.json` is `local: true`, `windows: ["main"]`, and grants: `core:default`, `core:window:allow-start-dragging`, `notification:default`, `deep-link:allow-get-current`, `log:default`, `window-state:default`, `opener:allow-open-url` scoped to `https://loams.dev/**` and the active environment's console origin, and the app commands of Tasks 3, 5, 7, 8 and 9 by name. CSP: `default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; font-src 'self'; connect-src ipc: http://ipc.localhost; frame-src loams-plugin:; object-src 'none'; base-uri 'none'; form-action 'none'`. The check script parses every capability and fails on any permission not in its allowlist, and on any `remote` block.

**Tests:** `capabilities_are_exactly_the_allowlist`; `csp_has_no_remote_sources`; an e2e `webview_cannot_invoke_unlisted_command` (calls `__TAURI_INTERNALS__.invoke('plugin:shell|execute')` and expects a refusal).

**Commit:** `desktop: lock down capabilities and CSP`.

### Task 5: The network bridge and the Connect transport

**Files:** `src-tauri/src/{net.rs,envs.rs}`, `src-tauri/tests/net.rs`, `web/packages/platform-tauri/src/fetch.ts`.

**Produces:**

```rust
#[tauri::command] async fn net_fetch(req: FetchRequest, body: tauri::ipc::Request<'_>, on_event: Channel<FetchEvent>, state: State<'_, Net>) -> Result<u64 /* request id */, NetError>;
#[tauri::command] fn net_abort(id: u64, state: State<'_, Net>);
pub enum FetchEvent { Head { status: u16, headers: Vec<(String, String)> }, Chunk(Vec<u8> /* sent as raw IPC */), End, Error { message: String } }
#[tauri::command] fn envs_list() -> Vec<EnvironmentSummary>;   #[tauri::command] fn envs_select(id: String) -> Result<(), NetError>;
```

```ts
// @loams/platform-tauri
export function tauriFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>;  // AbortSignal → net_abort
```

**Semantics:** Ruling 6. Redirects: same origin and scheme only, at most 3, re-checked against the allowlist; any cross-origin or `https`→`http` redirect is returned as an error and no credential is forwarded (§37 §6.4). HTTP/2 when the server offers it (reqwest + rustls with ALPN), HTTP/1.1 otherwise; TLS with the platform roots plus, per environment, a pinned SPKI when the environment came from a pairing QR (§37 §7.2). Timeouts: connect 10 s; no total timeout for streaming responses (Connect's own deadlines apply). The console's Connect services (AP1a Task 4) use `createConnectTransport({ baseUrl, fetch: tauriFetch, useBinaryFormat: true })`.

**Tests:** `bridge_refuses_cross_origin_redirect`; `bridge_refuses_https_downgrade_redirect`; `bridge_follows_same_origin_redirect_with_credentials`; `bridge_streams_server_stream` (against `operon-apps-mock`'s `WatchApprovals`); `bridge_refuses_foreign_origin`; `bridge_strips_js_auth_headers`; `bridge_abort_cancels_upstream`; `bridge_throughput` (records MB/s; fails under 10 MB/s, **estimate**); a Vitest of `tauriFetch` with a fake invoke.

**Commit:** `desktop: carry console requests through a Rust bridge`.

### Task 6: Single instance, tray, window state, logs

**Files:** `src-tauri/src/{tray.rs,logs.rs,lib.rs}`.

**Semantics:** `tauri-plugin-single-instance` (with `deep-link`) focuses the running window and forwards URLs. The tray lists stacks with state dots and pending-approval count, and has Open, Start/Stop per stack and Quit. `tauri-plugin-log` writes to the app log dir, rotating at 10 MB, 5 files; the startup failure page names that path (the harness told users to check logs that did not exist).

**Tests:** `second_instance_forwards_url` (unit, through the plugin's callback); `log_rotates_at_limit`.

**Commit:** `desktop: add single instance, tray and logs`.

### Task 7: Sign-in, refresh and the keychain

**Files:** `src-tauri/src/auth.rs`, `src-tauri/tests/auth.rs`, `web/packages/platform-tauri/src/auth.ts`.

**Produces (commands):** `auth_sign_in(env_id) -> SignedIn { principal: PrincipalSummary }`, `auth_sign_out(env_id)`, `auth_status(env_id) -> AuthStatus`. No command returns a token.

**Semantics:** Rulings 7–8. Discovery: `GetInstance` names the Authentik issuer (OIDC discovery at `/.well-known/openid-configuration`) and the Loam gateway's token endpoint for the exchange. Step-up for approvals sends `max_age=0` to Authentik. The loopback listener binds `127.0.0.1:0`, accepts one request with matching `state`, answers a static "You can close this tab" page, and closes. Refresh runs in the background; a refresh failure marks the environment `signed_out` and the console shows a sign-in banner. Before the auth plan, environments of kind `local` report `auth: none` (D111) and skip sign-in.

**Tests:** `pkce_round_trip_against_mock_issuer` (the mock plays Authentik's OIDC endpoints and the gateway's exchange); `exchange_discards_idp_tokens`; `state_mismatch_is_refused`; `loopback_accepts_one_request`; `refresh_rotates_and_stores`; `keychain_unavailable_is_session_only`; **`no_command_returns_a_secret`** (calls every command with the mock signed in and greps outputs and the log file for the mock's token strings).

**Commit:** `desktop: sign in with PKCE and keep credentials in the keychain`.

### Task 8: Approvals and desktop notifications

**Files:** `web/packages/platform-tauri/src/notify.ts`, AP1a's `@loams/plugin-approvals` (desktop surface), `src-tauri/src/tray.rs` (badge).

**Semantics:** the approvals plugin holds `WatchApprovals` (AP0) for every signed-in environment while the app runs (window hidden included), raises a native notification per new pending approval the user may decide (title and body from the server's `summary`, Rule 8 of AP0), and opens the approval on click. Deciding on desktop uses step-up `SESSION` (AP0 Ruling 7): a session older than 5 minutes triggers a re-sign-in in the system browser before the decision is sent.

**Tests:** against `operon-apps-mock` scenario `approvals-basic`: `new_approval_notifies_once`; `decided_elsewhere_clears_notification`; `stale_session_requires_step_up`.

**Commit:** `desktop: notify and decide approvals`.

### Task 9: Pairing a phone from the desktop

**Files:** AP1a's `@loams/plugin-devices` (desktop and browser), `web/packages/platform-tauri` (nothing new).

**Semantics:** a "Pair a phone" page calls `DeviceService.CreatePairing` and renders the QR payload (§37 §7.2.1) and the 8-digit code with a 5-minute countdown, then lists devices with revoke. Works the same in the browser console.

**Tests:** Vitest: `qr_payload_matches_schema_v1`; `countdown_expires_pairing`; e2e against the mock: `pairing_shows_device_after_claim`.

**Commit:** `console: pair phones from the desktop and the console`.

### Task 10: Deep links

**Files:** `src-tauri/src/deeplink.rs`, `src-tauri/tests/deeplink.rs`, `web/packages/platform-tauri/src/deeplink.ts`.

**Semantics:** Ruling 10. Registered at install on all platforms (and at runtime on Linux and Windows in development, as the plugin requires). The parser returns `DeepLink::{Open { env, path }, Approval { id }, Stack { name }}` or an error; the console's router receives it through a cordis event `desktop/deeplink`.

**Tests:** `deeplink_parses_allowlisted_patterns`; `deeplink_rejects_unknown_patterns`; `deeplink_rejects_path_traversal`; `deeplink_never_dispatches_mutations` (a property test over random URLs: the only effects are navigations).

**Commit:** `desktop: handle loams:// deep links as navigation`.

### Task 11: Packaging, signing, updates and the release job

**Files:** `.github/workflows/desktop.yml` (jobs `build` matrix macOS aarch64, Linux x86_64 and aarch64, Windows x86_64; `release` on tags `desktop-v*`), `src-tauri/tauri.*.conf.json` (bundles: macOS `app`+`dmg`; Linux `appimage`, `deb`, `rpm`; Windows `nsis`), `src-tauri/src/updates.rs`, `docs/guides/desktop.md`.

**Semantics:** Ruling 9. `tauri-apps/tauri-action` builds and uploads; macOS signs with a Developer ID Application identity, hardened runtime, and notarizes (secrets `APPLE_*`, Q420); Windows signs through `bundle.windows.signCommand` with the chosen service (Q421); the sidecar is signed with the app. The updater checks at start and daily, shows release notes, downloads, verifies the signature and installs on the next quit; `updates.channel` is `stable` or `beta`. "Add `loams` to PATH" (a menu item, macOS and Linux only: Windows bundles no `loams`, Ruling 11; if Q437 adds one, Windows gets a `loams.cmd` shim in `%LOCALAPPDATA%\Loams\bin` instead, since unprivileged symlinks need Developer Mode) writes `~/.loam/bin/loams` as a symlink to the bundled binary with `receipt.json` `install_method: "desktop"`, so `loams self-update` refuses it (§30 D294, amended by D429) and points to the app.

**Tests:** the CI build on every platform (unsigned on PRs); `update_manifest_signature_verifies` (a test key); `tampered_update_is_refused`; `path_install_writes_desktop_receipt`; e2e smoke per platform: launch, the console renders, the stacks page lists a fake stack (WebDriver on Linux and Windows; a launch-and-screenshot check on macOS).

**Commit:** `ci: package, sign and update Loams Desktop`.

### Task 12: Docs and exit

**Files:** `docs/guides/desktop.md`, `docs/design/37-desktop-and-mobile-apps.md` (as built), `docs/plans/README.md`, `CHANGELOG.md`.

**Exit criteria:** on macOS and Linux, a stack created with `loams stack create` in a terminal appears in the app within 5 s and can be stopped from it; a crashed `keep_running` stack restarts; sign-in against `operon-apps-mock`'s issuer stores a refresh token in the keychain and no secret appears in any command output or log; an approval in the mock raises one notification and can be decided after step-up; `capabilities_are_exactly_the_allowlist` passes; a signed update installs from the `beta` channel on a test machine.

**Commit:** `docs: document Loams Desktop and close AP1`.

## Rulings made during execution

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| — | (Task 0 fills this table) | | |
