# 37 — Loams Desktop and Mobile Apps: a cordis Console, a Tauri Shell, Native Phones

Status: **Approved** (owner defaults, 2026-10-02: "do suggested for all") · 2026-10-01, revised 2026-10-02 (the Authentik and D220 rulings). The direction is the owner's, given on 2026-10-01 in two messages:

1. "I downloaded the DeepSeek Harness desktop and mobile app repos. They serve as the base for the Loams desktop app and mobile app, with Connect-RPC. The harness desktop's Rust backend is Tauri, so adapt our control-plane React to Tauri. Mobile is Kotlin, so use Connect-RPC natively in Swift (iOS) and Jetpack Compose (Android)."
2. The same day's correction: "cordis" is the JavaScript meta-framework the harness is built on (contexts, services, a plugin lifecycle with scoped disposal and hot reload), not Tauri. The intent is to **adapt Loams's control-plane React to cordis so that any code can be loaded as a plugin**: console pages, panels, engine adapters, connectors, agent tools, and the integrations of §26, §30, §32–§34, each a cordis plugin with declared services and dependencies, loaded from a catalog like the harness's `cordis.yml`, in the browser and inside Tauri. **Tauri stays the desktop shell; cordis is the application architecture inside it.** "Native Connect-RPC" on mobile means connect-swift and connect-kotlin generated from the shared protos, with no web view or bridge, in native SwiftUI and Compose.

> **Amended 2026-10-02 (the zeron ruling).** The owner ruled: "instead of Tauri go native for desktop apps also: https://github.com/zeronsh/zeron". **The desktop is no longer a Tauri shell around the cordis console; it is a native app on a fork of zeron** (§18, D480–D499, plan [AP1n](../plans/2026-10-02-ap1n-native-desktop-zeron.md), which replaces AP1). Superseded by §18, kept below for history and marked in place: D420's "Tauri 2", D429–D432, D439's desktop layout, §3.1, §4's desktop half, §6 in full, §9's desktop rows, §12's `tauri-driver` tests, §13's AP1 row, risks 7, 9 and 10, Q428–Q430 and Q437 (as noted). **Unchanged:** the web console on cordis in the browser (D422–D428, AP1a), the phones and the app protos (D433–D438), AP0 and AP2/AP3. **Later the same day the owner added: "do not drop Tauri; add it as a bridge to control websites"**: Tauri returns, not as the desktop shell but as a separate web bridge that agents drive as MCP tools (§18.14, D500–D512, plan [AP1b](../plans/2026-10-02-ap1b-tauri-web-bridge.md)). The names are the owner's: **Loams Bot** and **Loams Software Factory** (§39).

The owner's standing rulings that apply: Connect-RPC everywhere (connect-es, connect-swift, connect-kotlin, all from the protos connect-rust serves, D128); the package namespace `loams` (crates.io, PyPI, npm `@loams`), Go paths `loams.dev/...`, the domain `loams.dev`, CloudEvents types `io.loams.dev.*`; the repository moving to the GitHub organisation `ostrium-labs`; mobile native per platform, not Kotlin Multiplatform UI. Two further rulings arrived while this document was written (2026-10-01 and 2026-10-02): **the identity provider is Authentik, open-source edition only** (Clerk and Keycloak are gone), so every app sign-in flow targets Authentik (§6.5, §7.2); and **D220's open-core split stands**: the console's multi-tenant, hosted and billing parts stay in the private `loam-cloud` and `loam-platform` repositories, loaded as private plugins from a private registry. This document designs only the open side and names the extension points the private side uses (§5.8); it contains no design for hosted or billing plugins.

This document turns that direction into decisions **D420–D439** and open questions **Q420–Q439**. Every choice beyond the direction (formats, trust tiers, flows, phasing) was a **proposal** until the owner confirmed it on 2026-10-02 ("do suggested for all"; Q420, Q421 and Q434 stay owner actions). **No code is written by this document.** The plans are [AP0](../plans/2026-10-01-ap0-app-protos.md), [AP1a](../plans/2026-10-01-ap1a-cordis-console.md), [AP1](../plans/2026-10-01-ap1-desktop-tauri.md), [AP2](../plans/2026-10-01-ap2-android-compose.md) and [AP3](../plans/2026-10-01-ap3-ios-swiftui.md).

Markers: **(verified 2026-10-01)** means checked against a primary source on that date (§17). **(verify)** means the plan that builds it checks it first. **(estimate)** means computed, not measured. Paths of the form `harness-desktop/…` point into `dina-kar/deepseek-harness-desktop` at `2d1b505` (2026-08-15); `harness-mobile/…` into `dina-kar/deepseek-harness-mobile` at `68b6c2f`.

**Naming.** This document writes `loams` for the CLI binary and `@loams/*` for npm packages, following the owner's namespace rulings (D400, D401; the binary `loams` answers §30's Q284). Until D33's rename PR, everything builds under the working names (`operon`, `@loam/console`, `@loam/ui`).

---

## 1. Summary

| # | Decision | Status |
|---|---|---|
| D420 | **Three apps, one contract.** Loams Desktop (Tauri 2, macOS, Linux, Windows), Loams for iOS (SwiftUI) and Loams for Android (Jetpack Compose). Every application call from an app to Loams is Connect-RPC from the protos connect-rust serves, through connect-es, connect-swift and connect-kotlin; the named exceptions are the console's OpenAPI `/api/v1` (REST until Q423), sign-in at Authentik and the Loams gateway's OAuth token endpoint (OIDC and OAuth over HTTP), the instance-to-gateway push API and APNs/FCM, the desktop's local CLI JSON contract (D283), and the updater's manifests (§2, §8) | Approved (owner defaults, 2026-10-02); the desktop half amended by D480 (native, §18) |
| D421 | **Borrow the harness repos' patterns; fork neither.** Both are MIT. The desktop's Rust host is about 160 lines and its value is the pattern; the mobile app talks a different protocol to a different server. Nothing is copied by default, so no attribution is owed; any copied file keeps its MIT notice in `THIRD_PARTY_NOTICES.md`. cordis itself is a direct MIT dependency (§3) | Approved (owner defaults, 2026-10-02) |
| D422 | **The console becomes a cordis v4 application**, in the browser (served by the engine at `/ui`, §19 P1) and inside Tauri. A small host boots a cordis `Context` and the cordis loader; everything else (layout, pages, engine views, connector forms, approval renderers, the RPC clients themselves) is a plugin. cordis's client half is used; the harness's Node host half is replaced by the Rust engine and the Tauri host, and its Typert RPC by Connect (§5) | Approved (owner defaults, 2026-10-02) |
| D423 | **The plugin manifest and the catalog.** A plugin is an ESM package whose `package.json` has a `loams.plugin` block (kind, entry, `inject`, `provides`, slots, permissions, trust tier, API requirements, editions). The catalog is a cordis v4 entry list, `loams.yml`, composed from a base file and edition patch files exactly as the harness composes bundles and profiles. **No `!!js` in any catalog** (§5.3) | Approved (owner defaults, 2026-10-02) |
| D424 | **Service contracts.** Plugins cooperate only through cordis services, never by importing each other's values. The host provides `transport`, one `rpc.<service>` per proto service (provided only when `GetInstance.api_versions` lists it, so dependents activate by themselves), `api` (the OpenAPI console client), `session`, `platform`, `slots`, `router`, `settings`, `flags` and `i18n` (§5.4) | Approved (owner defaults, 2026-10-02) |
| D425 | **Surfaces are typed slots**, adapted from the harness's `ui-slots`: `single`, `list` and `keyed` slots declared by a `SlotMap`, registered through `ctx.slots.register(...)` inside an effect, each entry in its own error boundary, components never touching `ctx`. The first slot catalog is fixed in §5.5 | Approved (owner defaults, 2026-10-02) |
| D426 | **Trust tiers and isolation.** `core` and `first-party` plugins run in the console's realm behind a guard proxy that exposes only their injected services. **`third-party` plugins always run in a sandboxed iframe** (opaque origin, `connect-src 'none'`) with a capability-checked bridge, and their calls carry a **vended, attenuated token** (§19 §5.2 flow 3) whose scopes are the manifest's permissions intersected with the user's, with the plugin as the actor. The server enforces it and audits it. Third-party plugins are off until the unified auth plan (§5.6) | Approved (owner defaults, 2026-10-02) |
| D427 | **Plugin sources and reload.** Five sources: bundled, npm `@loams/*` at build time (with npm provenance from `ostrium-labs`), a **private registry** configured at build time (how the private Cloud plugins arrive, §5.8), installed on an instance by an org owner at run time (served by the engine with integrity hashes), and a local path in development. Development reload is Vite HMR plus a cordis fiber refresh; a production instance pushes catalog changes and the host disposes and loads fibers without a page reload (§5.7) | Approved (owner defaults, 2026-10-02) |
| D428 | **Editions are plugin sets** (D220, which stands): `oss` and `desktop` in this repository; the hosted set is private plugins from `loam-cloud` and `loam-platform`, built into a hosted console from a private registry with a private catalog patch. The open host exposes extension points for it (a registry source, trusted publishers, catalog patches, slots, `flags.edition`) and knows nothing else about it; this repository never depends on it (§5.8, §10) | Approved (owner defaults, 2026-10-02) |
| D429 | **The desktop shell.** One Tauri 2 window loads the bundled console with the `desktop` plugin set. **Its sidecar is the `loams` CLI binary**, bundled per target; stacks are created, started, stopped and described through the CLI's JSON contract (§30 D283, D285), so terminal and app share `LOAMS_HOME` and the same stacks. Stacks outlive the app. The app restarts `keep_running` stacks with backoff. "Add `loams` to PATH" writes a `desktop` install receipt, which `self-update` refuses (amends D294) (§6.2) | **Superseded** by D488 (stacks through the CLI, from a native app) and D480 |
| D430 | **Desktop lockdown and the network bridge.** One local capability with an explicit command allowlist; no shell, fs, http or process permission; a CSP with no remote source. **Every console request goes through `net_fetch`**, a Rust command that streams the response over a Tauri `Channel` into a standard `Response`, adds the bearer token, strips JavaScript-set credentials, and only reaches the active environment's origins. Tokens never reach JavaScript (§6.3, §6.4) | **Superseded** by D489 (the credential rule for a native app) |
| D431 | **Desktop sign-in against Authentik**: OIDC authorization code with PKCE in the system browser and a loopback redirect (RFC 8252 §7.3) at the instance's Authentik (open-source edition), public client `loams-desktop`; the Authentik token is exchanged at the Loams gateway (RFC 8693) for Loams's own access and refresh tokens (§19 §5.3), so Loams still decides environment and scopes. The refresh token in the OS keychain through `keyring`, the access token in Rust memory only. Not Stronghold, which is deprecated (§6.5) | Approved (owner defaults, 2026-10-02); now implemented in Rust inside the app (D486, §18.7) |
| D432 | **Desktop updates, signing, platforms and deep links.** `tauri-plugin-updater` with static per-channel manifests and its own signing key, separate from the CLI's; Developer ID signing and notarization on macOS, Authenticode on Windows; macOS aarch64 and Linux with local stacks, Windows remote-only until a Windows server variant exists; `loams://` deep links are navigation only, parsed in Rust against an allowlist, forwarded by the single-instance plugin (§6.6, §6.7) | **Superseded** by D490 (zeron's updater with a signed manifest; same signing accounts) for updates and packaging; deep links stand |
| D433 | **Mobile is native on each platform, with no Kotlin Multiplatform.** SwiftUI with connect-swift over URLSession, and Compose with connect-kotlin over OkHttp; both use the Connect protocol with the binary codec. The shared parts are the protos, golden fixtures (canonical decision bytes, pairing payloads, sealed notifications) and the conformance scenarios run against one mock (§7.1, §7.8) | Approved (owner defaults, 2026-10-02) |
| D434 | **Pairing maps the harness's relay and pinned-key pattern onto §19's identity model.** A phone is a **device credential of a user principal**, not a new principal kind. A signed-in user creates a short-lived pairing in the console or desktop; the phone scans a QR (v1) holding the Loams gateway's URL, the instance id, the TLS SPKI pin set and the **instance key thumbprint**, and redeems the pairing at the Loams gateway's token endpoint with an extension grant and a DPoP proof. The alternatives sign in at **Authentik** (PKCE in the system browser, or Authentik's device-code flow, RFC 8628, for a phone without a camera) and exchange the result at the gateway. Loams tokens are DPoP-bound to a hardware key. The pinned anchor is the instance's token-signing key (§19 §5.3), under which TLS pins rotate. No relay in track AP (§7.2) | Approved (owner defaults, 2026-10-02) |
| D435 | **Approvals are a first-class service** over §21 §6.5's approval promises: `loams.approvals.v1` with list, get, watch and decide. **A decision carries a proof** signed by a user-presence key (Secure Enclave or StrongBox behind biometrics) or comes from a session younger than 5 minutes; the requester (or the user an agent acts for) cannot approve; there are no offline or queued decisions and no "always allow" (§7.3) | Approved (owner defaults, 2026-10-02) |
| D436 | **Push is a sealed wake-up.** The engine projects `io.loams.dev.*` CloudEvents into a per-user inbox, seals each notification with HPKE to the device's key, and hands it to a **push gateway** that holds the APNs and FCM credentials and sees only ciphertext. The gateway, `loams-push`, is open source; Loams runs the instance the store apps use, and self-hosters with their own app builds run their own. Android also supports UnifiedPush (§7.4) | Approved (owner defaults, 2026-10-02) |
| D437 | **Offline and background behaviour.** Phones cache approvals, operations and the inbox with freshness timestamps and show stale data read-only; no background sockets; push, `WorkManager` and `BGAppRefreshTask` catch up; decisions are never queued (§7.5) | Approved (owner defaults, 2026-10-02) |
| D438 | **The app proto surface (AP0)**: new packages `loams.instance.v1`, `loams.devices.v1`, `loams.approvals.v1`, `loams.operations.v1`, `loams.notifications.v1` and `loams.errors.v1`. Rules: unary and server-streaming only; watch streams send a snapshot, then changes, then a heartbeat every 15 s, and resume from a cursor; idempotent reads are marked for HTTP GET; every mutation takes an idempotency key; errors carry a stable `reason`. Served first by `loams-apps-mock` (§8) | Approved (owner defaults, 2026-10-02) |
| D439 | **Repository layout and track AP.** Desktop and console in this repository (`web/apps/console`, `web/apps/desktop` with its own Cargo workspace, `web/plugins/*`, `web/packages/*`). Mobile in one repository, `ostrium-labs/loams-mobile` (`android/`, `ios/`), generating from a pinned ref of this repository's `proto/`. Plans: AP0 (protos and mock), AP1a (cordis console), AP1 (desktop), AP2 (Android), AP3 (iOS); AP4 (the server side) is not yet planned (§9, §13) | Approved (owner defaults, 2026-10-02); the desktop layout **superseded** by D493 (its own repository), the plan by D499 |

## 2. Goals, non-goals and personas

### 2.1 Personas

| Persona | Device | Jobs to be done |
|---|---|---|
| **Dana, a developer with a local stack** | A laptop (macOS or Linux) | Start a `standard` stack without a terminal; see its endpoints and `.env.loams` variable names; read its logs; browse collections; watch jobs and durable runs while an agent works; approve what her agent asks for; switch to the team's staging environment; pair her phone |
| **Omar, an operator or on-call approver** | A phone, sometimes the desktop | Get an alert when a destructive operation or an agent action needs approval; read exactly what it does, who asked and for whom; approve or reject with Face ID or a fingerprint; follow a running restore or import; see failed jobs and dead-letter queues |
| **Priya, a platform engineer extending the console** | A browser or the desktop | Add a page for her team's connector, a renderer for a custom approval kind, or a panel on the environment overview, without forking the console; install it on her instance and have it reload live |

### 2.2 Goals

1. **The console is extensible without a fork.** Any page, panel, engine view, connector form, approval renderer or agent-tool view is a plugin with a manifest, declared services and declared permissions (D422–D427).
2. **One console in three places.** The engine's `/ui`, the desktop window and (when Q436 says so) Loams Cloud run the same host with different plugin sets (D428).
3. **A desktop app that is a better local stack manager than the terminal**, built on the CLI rather than beside it (D429).
4. **Phones that are safe approvers.** A decision proves a person on a known device; notifications reveal nothing to Apple, Google or the gateway (D434–D436).
5. **One contract.** Protos generate the server and all three clients; one mock and one scenario set test all of them (D433, D438).
6. **Open by default** (D220): every app, the plugin host, the first-party plugins and the push gateway's code are Apache-2.0 here or in `loams-mobile`.

### 2.3 Non-goals

- **No Electron, no React Native, no Flutter, no Tauri mobile, no KMP UI.** The owner chose Tauri for the desktop and native UI on phones. Tauri's own mobile targets shipped in 2.0 but its team calls the developer experience unfinished (verified 2026-10-01); they stay a fallback only.
- **No Node server.** §19 P8 rules out a Node service beside the binary. The harness's cordis host half (Node) is not used: Loams's host side is the Rust engine (Connect services) and, on the desktop, the Tauri host.
- **No model-written plugins at run time.** The harness's `cordis_define` lets an agent submit code that runs in the console after a click. Loams does not ship that: agents act through MCP and tokens (§15, §19 §5), not by injecting UI code.
- **No full console on phones.** Phones show environments, approvals, operations, jobs, runs and the inbox. Identity administration, keys and collection editing stay on the console and desktop.
- **No relay in track AP.** Phones reach instances that are reachable: Loams Cloud, or a self-hosted gateway exposed with TLS after the auth plan (Q425).
- **No offline decisions** (D435).

## 3. What we take from the harness repositories (D421)

### 3.1 The desktop harness

> **Superseded 2026-10-02:** the Tauri host pattern below is not used by the native desktop (§18.1, D480). Kept as a record of what was studied.

`harness-desktop` is upstream DeepSeek Harness (about 12 000 commits by its authors) plus a Tauri 2 host added on 2026-08-14 by `fendouai`. The host is `apps/desktop/src-tauri/src/lib.rs` (164 lines) and a capability file.

| Pattern | What it does there | Loams |
|---|---|---|
| A sidecar on loopback with port 0 | `sidecar("dsh-node").args([entry, "web", "--port", "0"])`; the URL comes from a stdout line `dsh web: http://127.0.0.1:<port>`, accepted only if the scheme is `http` and the host `127.0.0.1` | The CLI's stacks already choose and record ports (§30 §8.2) and expose `/ready`; the desktop reads `stack describe --output json` instead of parsing a log line (D429) |
| Zero IPC for the web UI | The loopback page gets no capability at all; only the splash page has `core:default` | The bundled console gets a small, explicit allowlist instead of a remote page with none, because tokens must stay in Rust and requests must go through the bridge (D430) |
| A Host/Origin/Fetch-Metadata fence on `/api` | Stops DNS rebinding and cross-site requests; "not an auth layer" | The engine's loopback listeners take the same fence before the auth plan (a §10 follow-up, not AP), and real auth after it |
| Separate data dir | `DSH_HOME = <app_data>/dsh` | The desktop deliberately **shares** `LOAMS_HOME` with the CLI (D429) |
| A checksum-verified runtime download per target triple | Node 24 + `SHASUMS256.txt` | The `loams` release archive, verified against the CLI's signed manifest (§30 D292) |

Gaps we fix rather than copy: no per-launch token, no supervision after readiness (a crash leaves a dead page), logs discarded after readiness, a hard kill with no process group, no single-instance guard, no CSP on the loopback UI, no updater, no signing or CI, and bundle targets that contradict each other (`"all"` with a macOS `["app"]` override).

### 3.2 The mobile harness

`harness-mobile` is an Android client (Kotlin 2.0, Compose, Hilt, OkHttp, one multiplexed WebSocket) by `sorsama` and contributors, talking to a harness through the `dsh-relay` plugin (TypeScript, in a separate repository not studied here).

| Pattern | Loams |
|---|---|
| Modules `core` (pure JVM), `app`, `mock-harness` (a scriptable fake server), `conformance` (tests against a real harness) | Kept: `:core`, `:data`, `:push`, `:app`, `:conformance` on Android, and the same split as Swift packages on iOS; the mock is `loams-apps-mock` in this repository, shared by all apps (§12) |
| A QR payload `{v, kind, url, fingerprint, code, expiresAt}` with strict version checks | Kept and extended (§7.2.1) |
| SPKI pinning that replaces CA validation, so self-signed servers work | Kept, with the hostname checked and pins rotating under a signed announcement (§7.2.3). The harness disabled hostname checks for typed-code pairing and rotated keys whenever the relay's addresses changed, forcing re-pairing |
| Honesty about QR pairing (pinned before the first byte) versus typed-code pairing (trust on first use) | Kept, with a 6-word fingerprint the user compares (§7.2.2) |
| Answers bound to a connection generation, with the host replaying pending requests on reconnect | Replaced by an approval `revision` in every decision and server-side idempotency keys (D435, AP0 Ruling 5) |
| A mock that ports the host's own validation rules (`QuestionAcceptance.kt`) | Kept: AP0 Task 7's `acceptance` module is shared by the mock and the server |
| An endpoint catalogue test where a typed "not found" counts as a pass | Kept (AP2 Task 9, AP3 Task 9) |
| A pinned protocol fixture tied to an upstream commit | Kept: `conformance/proto-ref.lock` and a descriptor-set hash test |

Gaps we fix: the relay terminates TLS and sees all plaintext; no token refresh; notifications only while a foreground service holds the socket, and no approving from a notification; approvals show only a tool name and a reason; `cleartextTrafficPermitted="true"` and user CAs trusted app-wide; a plaintext session cookie in DataStore with `allowBackup="true"`; a 2 000-line singleton store; no iOS.

### 3.3 Licences and attribution

| Source | Licence (verified 2026-10-01) | Use | Obligation |
|---|---|---|---|
| `dina-kar/deepseek-harness-desktop` (fork of `fendouai/deepseek-harness-desktop`, itself built on `deepseek-ai/deepseek-harness`) | MIT, "Copyright (c) 2026 DeepSeek"; the desktop additions carry no separate notice | Patterns only | None unless code is copied; then keep the MIT notice in `THIRD_PARTY_NOTICES.md`. Do not take its 18.5 MB VRM avatar (VRM Public License 1.0) or anything under `native/landlock-run` (BSD-3-Clause) |
| `dina-kar/deepseek-harness-mobile` (fork of `sorsama/deepseek-harness-mobile`) | MIT, "Copyright (c) 2026 DSH Mobile contributors" | Patterns only | As above |
| `cordiverse/cordis` 4.0.0-rc.10, `@cordisjs/plugin-loader` 1.0.0-rc.7 | MIT | A direct dependency of the console host | Ship the MIT notice in the console's third-party notices |
| The harness's vendored cordis (`@deepseek-ai/cordis` 4.0.1 with 18 logged modifications) | MIT | Read as a reference for fixes we may need (re-entrant fiber disposal, transactional config reload) | Port a fix as our own patch with a reference, not by copying the vendored tree |
| `@koishijs/plugin-console`, `@koishijs/client` | npm metadata says **AGPL-3.0** although the repository says MIT | **Not used** (and Vue-based) | — |

## 4. Architecture

> **Superseded 2026-10-02:** the right-hand "Tauri host" box below is replaced by the native app of §18.3 (zeron engine, `loams-link`, three seams); the console and the phones are as drawn.

```
                         ┌────────────────────── one console host (cordis v4) ─────────────────────────┐
                         │ boot: Context + Loader + browser module table + boot manifest (loams.yml)    │
  browser at /ui ───────▶│ services: transport · rpc.* (Connect) · api (OpenAPI) · session · platform   │
  (engine-served)        │           slots · router · settings · flags · i18n                           │
                         │ plugins:  shell · identity · collections · jobs · durable · approvals ·      │
  Tauri webview ────────▶│           devices · connectors · flow · gateway · live · (desktop) stacks ·  │
  (bundled, desktop set) │           mcp · (hosted: private plugins from a private registry, D220)     │
                         │ third-party plugins ──▶ sandboxed iframes ◀──bridge──▶ attenuated tokens     │
                         └───────────────┬───────────────────────────────┬──────────────────────────────┘
                                         │ fetch (browser)               │ net_fetch over IPC + Channel (desktop)
                                         ▼                               ▼
   ┌──────────── Tauri host (Rust, desktop only) ────────────┐   ┌──────── Loams instance (Rust) ─────────────────┐
   │ cli: `loams … --output json` (D283) ── stacks (D285)    │   │ connect-rust: Connect + gRPC + gRPC-Web       │
   │ net: bridge, origin allowlist, bearer injection         │──▶│ loam.instance/devices/approvals/operations/    │
   │ auth: PKCE loopback, keychain (keyring)                 │   │   notifications.v1 (AP0, AP4) · loams.jobs.v1   │
   │ tray · single instance · deep links · updater · logs    │   │   (J1) · loams.live.v1 · /api/v1 (OpenAPI, P9)  │
   └──────────────┬──────────────────────────────────────────┘   │ approvals = §21 approval promises             │
                  │ spawns via CLI (setsid, detached)            │ notifier: io.loams.dev.* → inbox → seal ──┐   │
                  ▼                                              └───────────────────────────────────────────┼───┘
         local stacks under ~/.loams/stacks/<name>                                                            │ sealed
                                                                                                             ▼
   ┌── Loams for iOS (SwiftUI) ──┐  ┌── Loams for Android (Compose) ──┐        ┌── loams-push gateway ─────────────┐
   │ connect-swift (URLSession)  │  │ connect-kotlin (OkHttp)         │◀──────▶│ APNs / FCM credentials of the     │
   │ Secure Enclave keys         │  │ StrongBox / TEE keys            │  push  │ store apps; sees ciphertext only  │
   │ NSE unseals HPKE payloads   │  │ FCM or UnifiedPush, Tink HPKE   │        │ (UnifiedPush: instance → endpoint)│
   └─────────────────────────────┘  └─────────────────────────────────┘        └───────────────────────────────────┘
```

## 5. The console as a cordis application (D422–D428)

### 5.1 Why cordis, and what it gives

cordis (MIT, `cordiverse/cordis`, by Shigma, the framework under Koishi and DeepSeek Harness) is a dependency-injection and lifecycle framework: a **Context** whose services are declared and injected, **plugins** with an `apply` function, an `inject` list and a config schema, and **fibers** (v4's scopes) whose side effects are all registered through `ctx.effect` or `ctx.on` and undone when the fiber is disposed. A plugin whose injected services are missing stays pending and activates when they appear; when a provider is replaced, its dependents reload. That is exactly what an extensible console needs: a page can depend on `rpc.jobs` and appear only on instances that serve `loams.jobs.v1`; disabling a plugin removes its routes, slot entries and streams without a page reload.

What exists today (verified 2026-10-01): `cordis` **4.0.0-rc.10** (2026-09-08; release candidates every 2–4 weeks; the README says the API "is not yet stable"), `@cordisjs/plugin-loader` 1.0.0-rc.7, `@cordisjs/plugin-include` 1.1.0, `@cordisjs/plugin-hmr` 1.1.0. The core imports no `node:` module, so it runs in browsers and in Tauri's webview; the harness runs the stock loader in the browser by stubbing `node:module` and `process` in Vite. **The maintainer bus factor is one** (548 of about 560 commits). §14 risk 1 covers it.

The harness proves the browser half at scale: about 40 client plugins in its web bundle, a boot manifest the server injects (`window.__DSH_BOOT__`), per-plugin bundles that may not import each other's values, a slot system for React, and fiber-by-fiber reload. Loams takes that half. The harness's Node host half (the loader on the server, Typert RPC over `POST /api/<ns>/<method>` and downlink WebSockets) does not apply: Loams's host side is Rust, and its RPC is Connect, which adds typed server streaming that Typert lacks.

### 5.2 The host

`@loams/console-host` (in `web/packages/console-host`) is the only code that is not a plugin. It:

1. reads the **boot manifest**: `GET /ui/plugins/manifest.json` from the engine in a browser, or the bundled `plugins/manifest.json` in Tauri. Each row is `{id, name, url, integrity, rev, tier, inject, slots}`;
2. creates `new Context()`, installs `@cordisjs/plugin-loader` with `loader.internal` set to a browser module table that resolves rows to `import(url)` with Subresource Integrity checks, and loads the catalog (`loams.yml`, §5.3);
3. provides the core services (§5.4) and renders the React root, which renders only the `root` slot;
4. finishes boot with an **all-fibers sweep**: any fiber still pending after 10 s is reported in a "plugins" diagnostics page with the services it waits for (the harness's boot check).

Shared platform modules (React, React DOM, React Router, cordis, `@loams/ui`, `@loams/slots`, `@bufbuild/protobuf`, `@connectrpc/connect`, `@loams/proto`) are provided once through an import map; plugin bundles mark them external. A plugin that bundles its own React fails the build (AP1a Task 2).

### 5.3 The manifest and the catalog (D423)

**A plugin's manifest** is a block in its `package.json`:

```json
{
  "name": "@loams/plugin-jobs",
  "version": "0.1.0",
  "type": "module",
  "exports": { "./client": "./dist/client.js" },
  "loams": {
    "plugin": {
      "kind": "console",
      "entry": "./client",
      "tier": "first-party",
      "inject": ["rpc.jobs", "router", "slots", "session"],
      "provides": [],
      "slots": ["console.nav", "console.page", "environment.overview.card", "engine.view"],
      "permissions": ["jobs:read", "jobs:admin"],
      "requires": { "console": "^1.0.0", "api": ["loams.jobs.v1"] },
      "editions": ["oss", "desktop", "cloud"],
      "config": "./dist/config.schema.json",
      "server": null
    }
  }
}
```

| Field | Meaning |
|---|---|
| `kind` | `console` (UI plugin). Reserved: `platform` (only the host's own `@loams/platform-*`), `theme` |
| `entry` | The ESM entry exporting `name`, `inject`, `Config` (a Standard Schema, from which `config.schema.json` is generated) and `apply(ctx, config)` |
| `tier` | `core`, `first-party` or `third-party` (§5.6). A package is `core` or `first-party` only if it is bundled or published with npm provenance by one of the build's trusted publishers (§5.6); the host decides this, not the manifest |
| `inject`, `provides` | cordis services it needs and offers. `provides` must be in its namespace (`<plugin-id>.*`) unless it is a `core` plugin |
| `slots` | The slots it registers into; registering into any other slot fails |
| `permissions` | §19 §5.1 actions (`collections:read`, `collections:write`, `query`, `documents:delete`, `streams:produce`, `mcp:tools`, `durable:invoke`, `durable:resolve`) plus `jobs:read`, `jobs:admin`, `approvals:decide`, `devices:manage`, `connectors:read`, `connectors:write`, and console-only `ui:notifications`, `ui:clipboard-write`. For `third-party`, they become the plugin's token scopes (§5.6) |
| `requires.api` | Proto packages that must be listed in `GetInstance.api_versions`; the matching `rpc.*` services gate activation anyway, so this is for the install-time check and the store listing |
| `editions` | Which plugin sets may include it (§5.8) |
| `server` | Optional link to a server half installed through another system: `{"kind": "function", "ref": …}` for a §24 function, `{"kind": "connector", "ref": …}` for a §33 connector. The console plugin never runs server code itself |

**The catalog** is a cordis v4 loader entry list, the format the harness's `cordis.yml` uses (`- id, name, config, group, disabled, inject`). Loams names it `loams.yml` and composes it the harness's way: a base list, then patch lists per bundle and per edition (`- id: x` patches replace a row's config; `- insert:` adds rows):

```yaml
# web/apps/console/catalog/base.yml   (the oss set)
- id: shell
  name: '@loams/plugin-shell'
- id: identity
  name: '@loams/plugin-identity'
- id: collections
  name: '@loams/plugin-collections'
- id: jobs
  name: '@loams/plugin-jobs'
  config: { pageSize: 50 }
- id: approvals
  name: '@loams/plugin-approvals'
- id: connectors
  name: '@loams/plugin-connectors'
# web/apps/desktop/catalog/desktop.patch.yml
- insert:
    - id: platform-tauri
      name: '@loams/platform-tauri'
    - id: stacks
      name: '@loams/plugin-stacks'
      inject: [platform.stacks]
```

**`!!js` is not allowed in any Loams catalog.** The harness evaluates `!!js` config with `new Function`, which needs `unsafe-eval` and is a code-execution path through configuration. Loams's console CSP has no `unsafe-eval` (§6.3), and the AP1a loader rejects a catalog that contains the tag. Dynamic values come from services (`flags`, `session`) inside `apply`, not from the catalog.

### 5.4 Service contracts (D424)

| Service | Provided by | What it is |
|---|---|---|
| `transport` | `@loams/console-host` (browser: `createConnectTransport({ baseUrl, useBinaryFormat: true })` with `credentials: 'include'`); `@loams/platform-tauri` (desktop: the same transport with `fetch: tauriFetch`, §6.4) | The Connect transport for the active environment. Replacing it (switching environments) reloads every `rpc.*` dependent |
| `rpc.instance`, `rpc.approvals`, `rpc.operations`, `rpc.devices`, `rpc.notifications`, `rpc.jobs`, `rpc.live`, `rpc.flow`, … | `@loams/plugin-rpc`: one sub-plugin per proto service, `inject: ['transport', 'flags']`, providing `createClient(Service, transport)` **only when** `GetInstance.api_versions` lists the package | Typed Connect clients. A server stream used by a plugin is wrapped in `ctx.effect`, so disposing the plugin aborts it |
| `api` | `@loams/plugin-rpc` | The `openapi-fetch` client for `/api/v1` (§19 P9), with the same base URL and, on desktop, the bridge's `fetch` |
| `session` | `@loams/plugin-identity` | The principal, org, project and environment selection, and sign-in state; emits `session/changed` |
| `platform` | `@loams/platform-web` or `@loams/platform-tauri` | `fetch`, `openExternal`, `notify`, `clipboard`, `kind: 'web' \| 'desktop'`; on desktop also `platform.stacks`, `platform.auth`, `platform.deeplink`, `platform.updates` |
| `slots` | `@loams/slots` (core) | §5.5 |
| `router` | `@loams/plugin-shell` | `ctx.router.page({ path, title, slot, nav? })` registers a route as an effect; React Router's data router underneath |
| `settings` | `@loams/plugin-shell` | Per-plugin config, rendered from the plugin's JSON Schema with the same form renderer as §33's connector config (`@loams/forms`) |
| `flags` | `@loams/console-host` | `GetInstance.features`, `edition`, `api_versions` |
| `i18n` | `@loams/plugin-shell` | Message catalogs per plugin |

The rule from the harness holds: **a value import from one plugin into another is a build error.** Plugins share types through `@loams/proto` and `declare module` merges on the `Context` and `SlotMap` interfaces, and behaviour only through services.

### 5.5 Slots (D425)

| Slot | Kind | Who registers | Props |
|---|---|---|---|
| `root` | single | `@loams/plugin-shell` | — |
| `console.nav` | list | any page plugin | `{ environment }` |
| `console.page` | keyed by route id | page plugins through `router.page` | `{ params, environment }` |
| `console.settings.section` | list | plugins with settings | `{}` |
| `environment.overview.card` | list | jobs, durable, live, collections, connectors | `{ environment }` |
| `collection.tab` | keyed | collections, search, analytics plugins | `{ collection }` |
| `engine.view` | keyed by engine id (`es`, `qdrant`, `flight-sql`, `pg`, `live`, `jobs`, `durable`, `mcp`) | engine adapter plugins | `{ environment, endpoints }` |
| `connector.config` | keyed by connector id or `runtime.kind` (§33 D352) | `@loams/plugin-connectors` registers the generic JSON-Schema form for every key; a connector-specific plugin overrides one key | `{ spec, instance, onChange }` |
| `flow.step.editor` | keyed by step type (§32 D337) | `@loams/plugin-flow` | `{ step, onChange }` |
| `approval.renderer` | keyed by approval `kind` | `@loams/plugin-approvals` (generic), others for richer views | `{ approval }` |
| `agent.tool.view` | keyed by MCP tool name | agent plugins | `{ call, result }` |
| `operation.detail` | keyed by operation `kind` (D146) | durable, jobs, import plugins | `{ operation }` |
| `palette.command` | list | any | `{}` |
| `shell.overlay` | list | notifications, update banner | `{}` |

The integrations of the other designs land as plugins in these slots:

| Design | Plugin | Slots | Needs |
|---|---|---|---|
| §26 jobs | `@loams/plugin-jobs` | `console.page` (queues, jobs, DLQs, schedules, flows, engine runs), `environment.overview.card`, `engine.view#jobs`, `operation.detail#jobs.*` | `rpc.jobs` (`Query`, `Watch`) |
| §30 CLI | `@loams/plugin-stacks` (desktop), `@loams/plugin-mcp` (desktop: `loams mcp install --agent …` through the CLI bridge); `@loams/plugin-cli-hints` (browser: copyable commands) | `console.page`, `palette.command` | `platform.stacks` (desktop only) |
| §32 Flow | `@loams/plugin-flow` | `console.page` (routes, lag, DLQ), `flow.step.editor` | `rpc.flow` |
| §33 connectors | `@loams/plugin-connectors` | `console.page` (catalog, instances), `connector.config` | `rpc.flow` (`ListConnectors`, `DescribeConnector`, `ValidateRoute`) |
| §34 gateway (moved to `loam-platform`, private, D440) | a private plugin, not designed here | `console.page` | `loam-platform` |
| §21 durable | `@loams/plugin-durable` | `console.page` (operations, runs), `operation.detail` | `rpc.operations` |
| §19 identity | `@loams/plugin-identity`, `-agents`, `-keys`, `-audit` | today's console pages, moved into plugins | `api` |
| §37 approvals and devices | `@loams/plugin-approvals`, `@loams/plugin-devices` | `console.page`, `approval.renderer`, `shell.overlay` | `rpc.approvals`, `rpc.devices` |

### 5.6 Trust tiers, isolation and permissions (D426)

cordis has no sandbox: its `isolate` and `intercept` only give scopes separate service names, and the harness says plainly that its plugins are as trusted as shell access. Loams needs a boundary for code its users did not write.

| Tier | Who | Where it runs | What it may call |
|---|---|---|---|
| `core` | The host, `@loams/slots`, `@loams/platform-*`, `@loams/plugin-rpc` | Console realm | Everything; ships only in the bundle |
| `first-party` | Bundled, or published with npm provenance by a **trusted publisher**: `github.com/ostrium-labs/*` in every build, plus the publishers a build adds (the hosted build adds the private repositories, §5.8) | Console realm, behind a **guard proxy**: `ctx` exposes only the services in its `inject` list (the harness client-runner pattern). This is hygiene, not a security boundary | Its injected services, with the user's credential |
| `third-party` | Anything else, from npm, an upload, or a local path | **A sandboxed iframe**: `sandbox="allow-scripts"` (no `allow-same-origin`, so an opaque origin), served with `default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src data: blob:; connect-src 'none'`. A second cordis context runs inside the frame; its services are proxies over `postMessage` to a host-side **bridge** that checks each call against the manifest's `permissions` and the plugin's granted set. Its slot entries are host-side placeholders that render the frame | Only through the bridge; no network of its own; never `platform.stacks`, `platform.auth` or `session` internals |

**Server-side enforcement.** A third-party plugin's calls do not carry the user's token. When the plugin activates, the host asks the gateway for a **vended token** (§19 §5.2 flow 3) with `scp` = the manifest's permissions ∩ the user's rights, `act` = `{"sub": "plugin:<id>@<version>"}` in the actor chain, one environment, and a 15-minute TTL, renewed while the plugin is active. The server's `Authorizer` decides as for any token, and the audit log shows the plugin as the actor. Client-side checks only shorten the path to a refusal.

Consequences:

- Before the unified auth plan (D111) there is nothing to vend, so **third-party plugins are disabled** in every build, and local-path plugins in development run as `first-party` with a red "unverified plugin" banner.
- A third-party plugin cannot render outside its frame (no overlays across the console, no access to other plugins' DOM), and cannot read the clipboard; `ui:clipboard-write` and `ui:notifications` go through the bridge with a visible host prompt the first time.
- On the desktop, third-party frames load from the `loams-plugin://` custom scheme, a different origin from the app's; AP1a Task 6 verifies that frames get no Tauri IPC (verify).
- Installing a plugin on an instance is an org-owner action, and in a `protected` environment it goes through an approval gate (D435). The install screen shows the permissions, the publisher, and whether npm provenance links it to a public repository.

### 5.7 Sources, distribution and reload (D427)

| Source | How | Tier |
|---|---|---|
| **Bundled** | Built into `web/apps/console/dist` (and the desktop bundle) by Vite from the catalog | `core`, `first-party` |
| **npm `@loams/*` at build time** | Self-hosters who build their own console (`pnpm loams-console build --catalog my.yml`) add packages to the catalog; the build checks provenance and records SRI hashes in the manifest | `first-party` if published from `ostrium-labs` with provenance, else `third-party` |
| **A private registry at build time** | A console build may point pnpm at a private registry for a scope (an `.npmrc` entry) and add that scope's publishers to `trustedPublishers`. This is how the hosted console receives its private plugins (§5.8); self-hosters can use it for their own internal plugins | `first-party` for the build's trusted publishers, else `third-party` |
| **Installed on an instance at run time** | An org owner installs a package (an npm name and version, or an uploaded tarball) through `loams.console.v1.PluginService` (AP1a Task 7, after the auth plan). The engine fetches it, checks its integrity and provenance, stores it under the system namespace in the bucket, and serves `/ui/plugins/<id>/<rev>/…` with `Cache-Control: immutable`. The boot manifest lists it | `third-party` unless its provenance makes it `first-party` |
| **A local path (development)** | `pnpm loams-console dev --plugin ../my-plugin` runs Vite with the plugin linked; the desktop's developer menu offers "Load plugin from folder" and watches it | `first-party` with the unverified banner (§5.6) |

**Reload.** In development, Vite's HMR replaces React components in place; a change to a plugin's `apply` triggers the harness's fiber refresh (invalidate the module, dispose the fiber, which undoes its slot entries, routes and streams, then load it again). React state inside that plugin is lost, as in the harness. In production, the host subscribes to `PluginService.WatchManifest`; an install, upgrade, enable or disable becomes a fiber add, replace or dispose, with no page reload. Desktop updates replace the whole bundle on restart (§6.6).

**Versioning.** The host exposes `console` (semver) and the plugin declares `requires.console`. Breaking changes to a core service or slot's props bump the host's major version; the slot catalog and `SlotMap` types are generated into docs (the harness's catalog generator pattern), and a CI check fails if a slot's props change without a version bump.

### 5.8 Editions as plugin sets (D428)

D220 stands: everything a single organisation needs to self-host is open, and the console's multi-tenant, hosted and billing parts stay in the private `loam-cloud` and `loam-platform` repositories. In plugin terms:

| Set | Where it lives | Contents |
|---|---|---|
| `oss` | This repository: `web/apps/console/catalog/base.yml` | Shell, identity (org, teams, projects, environments, members), agents, keys, audit (the short-retention open audit of D221), collections, engine views, jobs, durable, live, flow, connectors, approvals, devices, plugins management |
| `desktop` | This repository: `web/apps/desktop/catalog/desktop.patch.yml` | `oss` + `platform-tauri`, stacks, MCP install, desktop notifications, updates |
| hosted | The private repositories (not designed here) | `oss` + private plugins, built by the private repositories' CI from this repository's published host and plugins |

**The extension points the open host offers**, and the only things this document fixes about the hosted set:

| Extension point | What it is |
|---|---|
| A registry source | A build may resolve a package scope from another registry (§5.7) |
| `trustedPublishers` | A build option listing the provenance repositories whose packages count as `first-party` (§5.6) |
| Catalog patches | A build or an instance may apply further patch lists to `base.yml` (D423) |
| Slots | Every slot of §5.5 is open to any plugin set; the hosted set adds no slot the open host does not define |
| `flags.edition` and `flags.features` | From `GetInstance`; plugins gate on them as on `api_versions` |
| `platform` | A hosted build may provide its own `platform` service, as the desktop does |

The open host has no knowledge of the private plugins, and this repository never depends on them. Today's `loam-cloud` console is a separate Next.js app (with Clerk, which the Authentik ruling retires) and placeholder pages; whether it moves onto this host as a set of private plugins is Q436.

### 5.9 What the browser console needs to change

The console on `main` (`web/apps/console`, Vite 8, React 19, React Router 8, `openapi-fetch`) is already a static SPA, so it embeds in Tauri. AP1a turns it into the host plus plugins and fixes what blocks the desktop: `baseUrl: window.location.origin` in `src/api/client.ts` and the raw `fetch('/v1/…')` become the `api` and `transport` services; the hard-coded `/ui` in `window.location.assign` calls (`pages/auth.tsx`) becomes `router` navigation; the `/ui` basename and Vite `base` become build options; the cookie-and-CSRF session stays for the browser, and the desktop uses bearer tokens through the bridge (which needs `GET /api/v1/session` to accept a bearer and return the principal; an auth-plan item, §16).

## 6. The desktop shell (D429–D432)

> **Superseded 2026-10-02:** this whole section describes the Tauri design and is superseded by §18 (D480–D499). Stack supervision through the CLI (§6.2) survives as D488 (§18.3); sign-in (§6.5) as D486 (§18.7); the capability lockdown and `net_fetch` bridge (§6.3, §6.4) are retired by D489; updates and signing (§6.6) are replaced by D490 (§18.8); deep links (§6.7) stand, navigation only. Kept for history.

### 6.1 Shape

One Tauri 2.12 window (verified 2026-10-01) loads the bundled console with the `desktop` set. The Rust host (`loams-desktop`, its own Cargo workspace under `web/apps/desktop/src-tauri`, not part of the engine workspace) owns everything privileged: the CLI, the network, credentials, the tray, deep links, updates and logs. It exposes them to the console only as typed commands, which `@loams/platform-tauri` turns into cordis services.

### 6.2 Stacks through the CLI (D429)

The harness's host spawns one fixed runtime. Loams's desktop manages any number of stacks, and the CLI already does that well: `stack.toml`, the engine registry, port blocks, `/ready` polling, `setsid`, log rotation (§30 §8). Re-implementing it in the app would give two supervisors of the same directories. So:

- **The sidecar is the `loams` binary**, the `standard` variant (§30 D286), bundled as `externalBin` (`binaries/loams-<target-triple>`). Bundling costs tens of MB (**estimate**; Q428 asks whether to download it on first run instead).
- **Every stack action is `loams stack <verb> --output json`**, parsed by D283's contract: one JSON document on stdout, one error object on stderr, the exit code classified (0 ok … 9 integrity, 130 interrupted). Children run in their own process group with no TTY, `LOAMS_NO_UPDATE_CHECK=1` and a timeout.
- **`LOAMS_HOME` is shared** (default `~/.loams`), so a stack created in a terminal appears in the app within one poll, and the other way round.
- **Stacks outlive the app**, as they outlive a terminal. The tray lists running stacks. A setting stops the stacks the app started on quit (default off, Q429).
- **The app adds a restart policy** the CLI lacks: a `keep_running` stack reported `crashed` restarts with backoff 1 s … 30 s, at most 5 times in 10 minutes, then stays `crashed` with its log tail.
- **Creation and deletion stay in the terminal in AP1.** `stack create` (it can download variants and touch disks) and `stack delete --yes` are shown as exact commands to copy. Start, stop, restart, upgrade and logs are buttons.
- **"Add `loams` to PATH"** links `~/.loams/bin/loams` to the bundled binary and writes `receipt.json` with `install_method: "desktop"`. `loams self-update` refuses that install (exit 6 `managed_install`, hint "update Loams Desktop"), which **amends D294**.

Windows has no server variant (§30 Q285), so the desktop on Windows is **remote-only**: it signs in to Cloud or self-hosted instances and has no stacks page (Q437).

### 6.3 Capability lockdown (D430)

| Item | Setting |
|---|---|
| Windows | One, `main`; no remote URL ever loaded in it |
| Capability | `capabilities/main.json`, `local: true`, granting `core:default`, `notification:default`, `deep-link:allow-get-current`, `log:default`, `window-state:default`, `opener:allow-open-url` scoped to `https://loams.dev/**` and the active environment's console origin, and the app's own commands by name |
| App commands | Declared through `tauri_build`'s app manifest so each needs a grant: `stacks_*`, `net_fetch`, `net_abort`, `envs_*`, `auth_*`, `plugins_load_folder` (developer menu only) |
| Never granted | `shell:*` (the sidecar is spawned from Rust), `fs:*`, `http:*`, `process:*`, `dialog:*` beyond what Tauri's core needs, Stronghold |
| CSP | `default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; font-src 'self'; connect-src ipc: http://ipc.localhost; frame-src loams-plugin:; object-src 'none'; base-uri 'none'; form-action 'none'` (no `unsafe-eval`, which also forbids `!!js`, §5.3) |
| Check | A CI script fails on any permission outside the allowlist and on any `remote` capability block |

### 6.4 The network bridge (D430)

Three ways a webview can reach a Loams instance were considered:

| Option | Tokens | Streaming | Server changes | Verdict |
|---|---|---|---|---|
| The webview fetches the instance directly | In JavaScript | Yes | CORS for `tauri://localhost` and `http://tauri.localhost` on every instance; cookies are cross-site | Rejected: an XSS or a malicious plugin reads the token |
| A Tauri custom URI scheme that proxies | In Rust | **No**: a scheme handler's responder takes a complete body | None | Rejected for Connect server streams |
| **A `net_fetch` command with a `Channel`** | In Rust | Yes: head, chunks and end as channel events into a `ReadableStream` | None | **Chosen** |

`tauriFetch` implements the standard `fetch` signature, so connect-es (`createConnectTransport({ fetch: tauriFetch })`) and `openapi-fetch` use it unchanged. The Rust side allows only origins in the active environment's endpoint set, adds `Authorization: Bearer` (and a DPoP proof once Q438 lands), strips `Cookie`, `Authorization` and `Proxy-*` headers set by JavaScript, uses HTTP/2 when the server offers it, and pins the TLS key for environments that came from a pairing (§7.2.3). **Credentials travel only over HTTPS**: a request to a non-loopback origin over plain `http` is refused before any credential is attached; unauthenticated `http` to loopback (local stacks, D111) keeps working. **Redirects never carry credentials to a new origin**: a 3xx to the same origin and scheme is followed at most 3 times; a redirect to another origin, or from `https` to `http`, is not followed and reaches JavaScript as an error, so no bearer or DPoP proof is ever sent to a location the allowlist did not admit. The phones' clients do the same (`followRedirects(false)` on OkHttp, a refusing redirect delegate on URLSession). AP1 Task 0's spike measures throughput and memory.

### 6.5 Sign-in and credentials (D431)

- **Identity provider: Authentik**, open-source edition (owner ruling, 2026-10-01). `GetInstance` names the instance's Authentik issuer and the Loams gateway's token endpoint.
- **Flow:** OIDC authorization code with PKCE at Authentik, started from the desktop and completed in the **system browser**, so whatever Authentik is configured with (passwords, TOTP, WebAuthn, upstream SSO) works; redirect to `http://127.0.0.1:<port 0>/callback` (RFC 8252 §7.3), one request accepted, `state` and `nonce` checked. Public client id `loams-desktop`, registered as an Authentik application for the instance. The desktop then exchanges Authentik's token at the Loams gateway (RFC 8693, §19 §5.2) for Loams's access and refresh tokens; Authentik's own tokens are not kept. `tauri-plugin-oauth` 2.1 provides the loopback listener if its licence passes `deny.toml` (verify); otherwise it is about 60 lines.
- **Storage:** the refresh token in the OS keychain through `keyring` 4.2 (macOS Keychain, Windows Credential Manager, Linux Secret Service), one entry per `(instance_id, principal_id)`; the access token in memory; rotation on every refresh. `tauri-plugin-stronghold` is deprecated and will not exist in Tauri v3 (plugins-workspace#3494, verified 2026-10-01). Without a Secret Service on Linux, sign-in lasts the session only and the UI says so.
- **Local stacks before the auth plan** report `auth: none` (D111) and need no sign-in. After it, a local stack is signed in to like any instance.
- **Approvals on the desktop** use step-up `SESSION`: a session older than 5 minutes re-authenticates at Authentik in the browser (`max_age=0`) before a decision is sent (D435). A desktop device key with OS user presence (Touch ID through Keychain access control, Windows Hello) is a later option.

### 6.6 Updates, signing and packaging (D432)

| Item | Choice |
|---|---|
| Updater | `tauri-plugin-updater` 2.13: signatures are mandatory; static `latest.json` per channel (`stable`, `beta`) produced by `tauri-action` 1.0, on GitHub Releases behind a `loams.dev/desktop/{{target}}/{{arch}}/{{current_version}}` redirect (the §30 Q281 pattern) |
| Updater key | Its own key from `tauri signer generate`, held like the CLI's release key but **separate** from it (Q282) |
| macOS | `app` and `dmg`; Developer ID Application, hardened runtime, notarization through `notarytool` (secrets `APPLE_*`, Q420); the bundled `loams` is signed with the app (verify how Tauri signs `externalBin`) |
| Windows | `nsis`; Authenticode through `bundle.windows.signCommand` with Azure Artifact Signing or a Key Vault certificate (Q421) |
| Linux | `appimage` (the updater's format), `deb`, `rpm` (Q430: Flatpak) |
| Platforms with stacks | macOS aarch64, Linux x86_64 and aarch64 (the CLI's targets); Windows x86_64 remote-only |

### 6.7 Deep links, single instance, tray and logs (D432)

- **`loams://`** links: `open/<env>/<console path>`, `approvals/<id>`, `stacks/<name>`. Rust parses them against an allowlist and hands the console a typed route through the `desktop/deeplink` event. **No deep link performs an action**; it can only navigate. `tauri-plugin-deep-link` 2.6 registers the scheme at install (macOS has no runtime registration), and `tauri-plugin-single-instance` 2.5 with its `deep-link` feature forwards links to the running instance.
- **The tray** shows stacks with their state and the number of pending approvals.
- **Logs** go to the app log directory, rotating at 10 MB, 5 files, and the startup failure page names that path.

## 7. The mobile apps (D433–D437)

### 7.1 Shared rules

- **Native UI and native clients.** SwiftUI (iOS 17+) with connect-swift 1.2 (stable; `URLSessionHTTPClient`), and Compose with connect-kotlin 0.9 (beta; `ConnectOkHttpClient`, javalite). Neither app uses a web view except the system browser for sign-in.
- **Protocol:** Connect, binary codec, unary and server-streaming only (§8.3). gRPC would need trailers, which URLSession lacks (connect-swift's gRPC needs `ConnectNIO`); nothing requires it.
- **Module split** from the harness (§3.2): a pure core (pairing payload, decision canonicalization, watch resume, unsealing, error reasons) testable without a device; a data layer (clients, trust, keys, tokens, cache); push; UI; conformance.
- **Same screens on both:** environments, approvals (list and detail), operations, jobs and runs (when `loams.jobs.v1` is served), the inbox, devices and settings. No identity administration.

### 7.2 Pairing and identity (D434)

**Mapping the harness onto §19.** The harness's relay issued its own per-device bearer token and pinned its own TLS key. Loams already has an identity system: people sign in at the instance's **Authentik** (the identity provider, owner ruling 2026-10-01), and the Loams gateway issues Loams tokens signed by the instance's Ed25519 key, with a JWKS and revocation through the `ControlStore` change feed (§19 §5). So:

| Harness concept | Loams concept |
|---|---|
| A relay-issued device token | **A user's OAuth tokens, issued to a device**: the access token is §19 §5.3's JWT with `sub` = the user, a `dev` claim = the device id and `cnf.jkt` = the device's DPoP key (RFC 9449); the refresh token is bound to the same key and rotates |
| The pinned relay TLS key | **The instance key thumbprint (`jkt`)** of the token-signing key in the instance's JWKS, as the long-term anchor, plus a TLS SPKI pin set that rotates under signed announcements (§7.2.3) |
| "Sign out everywhere" on the relay | `DeviceService.RevokeDevice` and §19 §5.4's revocation set (by device id) |
| A new principal kind? | **No.** A device is a credential of a user. Agents never use phones; service accounts never pair |

#### 7.2.1 The QR payload (v1)

```json
{"v":1,"kind":"loams-pair","issuer":"https://loam.acme.example","instance_id":"01J9Z3…",
 "spki":["Rk9PQkFSLi4u…"],"jkt":"NzbLsXh8uDCcd-6MNwXF4W_7noWXFZAfHkxZsRGC9Xs",
 "code":"7JQ2KX4M2ZB6V3NAQ6E5RW2HCA","user_code":"48213977","exp":1790899500}
```

`issuer` is the Loams gateway (the authorization server for Loams tokens), not Authentik. `spki` is `null` for instances with publicly trusted certificates. `code` is 128 random bits, single use, valid 5 minutes, bound to the user who created it (`DeviceService.CreatePairing`); the example's `exp` is 2026-10-02 00:05 UTC. `user_code` is the typed alternative to `code` for the same pairing: the pairing grant accepts either `code` or `user_code` (never both), and a pairing is burned after 5 failed `user_code` attempts, so 8 digits within 5 minutes cannot be guessed (AP0 Task 2). A reader refuses `v` other than 1, another `kind`, an expired payload or a non-`https` issuer.

#### 7.2.2 Three ways to pair

1. **QR (primary).** The signed-in user opens "Pair a phone" in the console or desktop. The phone scans, pins `spki` before sending a byte, calls `GetInstance`, checks `instance_id` and that the JWKS contains `jkt`, creates two hardware keys (§7.6, §7.7), and redeems the pairing: `POST {issuer}/api/v1/oauth/token` with `grant_type=urn:loams:params:oauth:grant-type:pairing` (an extension grant, RFC 6749 §4.5), `code`, `client_id` (`loams-ios` or `loams-android`), `device_name`, `platform`, `decision_jwk` (the public key of the decision key), an optional key attestation, and a `DPoP` header. The answer is `{access_token, token_type: "DPoP", expires_in: 3600, refresh_token, device_id}`.
2. **Browser sign-in at Authentik.** `ASWebAuthenticationSession` or Custom Tabs with PKCE at the instance's Authentik, returning through `https://loams.dev/app/auth/callback` (universal and app links, Q281). The phone then exchanges Authentik's token at the Loams gateway (RFC 8693) with a DPoP proof and the same device fields, and receives the same DPoP-bound Loams tokens as the QR path.
3. **Device code at Authentik (no camera, or a phone that cannot open the browser flow).** The phone starts Authentik's device authorization flow (RFC 8628, verify the version that ships it) and shows the user code; the user enters it on the desktop or console, already signed in. Before that the phone connects trusting on first use, shows the gateway's key fingerprint as six words, and the console shows the same six words for the user to compare; the app says this pairing was not pinned in advance (the harness's honesty rule). The result is exchanged at the gateway as in path 2.

All three produce the same `Device` record. Authentik authenticates the person; the Loams gateway issues and binds the tokens, because DPoP binding, the `dev` and `env` claims and revocation are Loams's (§19 §5.3–§5.4). The unified auth plan implements the pairing grant, the exchange, the device record and DPoP binding (Q438); AP0 fixes the contract.

#### 7.2.3 Pins that rotate without re-pairing

The harness's relay minted a new TLS key whenever its addresses changed, which forced every phone to pair again. Loams separates the anchor from the transport key:

- The anchor is `jkt`. `GetInstance` returns `tls_pins`: a JWS signed by that key listing the current and next SPKI hashes. A phone accepts a new TLS key only if a pin set signed by the anchored key announced it.
- When the instance rotates its signing key, its JWKS lists both keys for an overlap period, and `GetInstance.key_rotation` carries the new key's thumbprint signed by the old key. A phone moves its anchor only along that chain.
- A mismatch is a hard stop ("this server's identity changed"), never a fallback to CA validation.

#### 7.2.4 No relay in track AP

`dsh-relay` terminates TLS on the user's network and sees everything. A hosted relay for Loams would have to be end-to-end (TLS passthrough by SNI, so the relay sees ciphertext only) and is a service with abuse and cost questions of its own. Phones in track AP reach instances that are reachable on the internet: Loams Cloud, or a self-hosted gateway with TLS after the auth plan (D111 keeps M1 listeners on loopback). Teams that keep instances private can use their VPN or a tunnel (Tailscale, Cloudflare Tunnel). Q425 asks whether to build a relay.

**Private instances on a tailnet ([§43](43-private-networking.md), D592–D594).** The recommended path is the user's own tailnet: the phone runs the official Tailscale app, signed in to their Tailscale account or, for a self-hosted Headscale, pointed at it (iOS: "Use custom coordination server"; Android: "Use an alternate server"), and the instance's DNS or MagicDNS name is the pairing `issuer`. The apps embed no tailnet client. The pairing payload gains one optional field, `"net": {"kind": "tailnet", "provider": "tailscale|headscale", "login_server": "https://..."}` (`login_server` only for Headscale) (`v` stays 1; readers ignore unknown optional fields; it never carries an auth key); an app that sees it asks the user to connect to the tailnet first instead of timing out. Credentials still require HTTPS: a tailnet instance uses a certificate for a name in the operator's DNS, or a private CA with the SPKI pin of §7.2.3. For tailnet users Q425 is answered: no relay.

### 7.3 Approvals (D435)

§21 §6.5 defines approval gates: a destructive operation (collection or namespace drop, erasure, restore over live data) or a `requires_approval` agent action becomes an operation whose first step waits on an approval promise, settled by an approver, timing out after 72 hours. `loams.approvals.v1` makes that a service every app shares:

- **What an approval shows:** a server-rendered `summary` and `detail_lines` in the user's locale (so iOS, Android, desktop and push show the same words and new kinds need no app release), the environment and whether it is `protected`, the requester and the actor chain (an agent acting for a user shows both), the risk, the policy's progress (1 of 2), and the time left.
- **Who may decide:** the org's approval policy through the `Authorizer` (§21 §6.5); **the requester, or the user an agent acts for, cannot approve their own request** by default (Q432).
- **A decision needs a proof.** `DecideApproval` carries `decision_proof`, a compact JWS (ES256 from the Secure Enclave or StrongBox; EdDSA allowed for software keys) over `{approval_id, revision, decision, iat, jti}`, signed by the device's decision key, which needs biometrics or the device passcode for every signature. The server verifies it against the key registered at pairing, checks `revision` (a stale card cannot be approved) and `jti` (no replay). A desktop or browser decision without a device key is accepted when the session is younger than 5 minutes, or when the policy says `step_up: none`.
- **Typed confirmation for the worst cases.** A `DESTRUCTIVE` approval requires typing the target's name, carrying §19 §4's protected-environment rule to the phone. Rejecting requires a reason.
- **Not offered:** "always allow" (policy changes are console actions, themselves approval-gated in protected environments), decisions from the lock screen without the app, and queued decisions.

### 7.4 Push notifications (D436)

**The constraint.** An APNs token-signing key belongs to one Apple developer team (Apple offers team-scoped and topic-specific keys), and a push is accepted only for a topic, the app's bundle id, that the key may send to. A team-scoped key can authorize pushes to every app of that team, so `loams-push` uses a **topic-specific key** limited to the Loams app's topics, held only by the gateway; FCM credentials belong to the app's Firebase project. Only the publisher of the store apps can push to them, so a self-hosted instance cannot push directly. Matrix (Sygnal), Mattermost (its push proxy), ntfy (upstream `poll_request`) and Home Assistant all solve this with a **push gateway** that the publisher runs (verified 2026-10-01). Loams does the same, with the gateway blind to content:

```
 engine: CloudEvent io.loams.dev.approval.requested.v1 (or …operation.failed.v1, …job.dead_lettered.v1)
   └─ notifier (a durable function in the engine): per-user projection, preferences, quiet hours
        ├─ inbox row (loams.notifications.v1, kept 30 days, estimate)
        └─ per device push target: seal(Notification) with HPKE to the device's X25519 key,
             AAD = instance_id ‖ notification_id  →  POST https://push.loams.dev/v1/notify
                                                      {target, app_id, sealed, collapse_id, priority, ttl}
 loams-push gateway: instance credential check, per-instance rate limit → APNs (token auth) / FCM HTTP v1
 device: NSE (iOS) or FirebaseMessagingService (Android) unseals → shows title and body → tap → app fetches details over Connect
```

- **What the gateway sees:** the device token, the app id, the instance id, sizes and timing. Not the title, body, environment or approval.
- **What Apple and Google see:** the same, plus a generic alert text ("New activity in Loams") that the extension replaces after unsealing. If unsealing fails, the generic text stays and the app syncs its inbox.
- **HPKE** (RFC 9180, DHKEM(X25519, HKDF-SHA256), HKDF-SHA256, ChaCha20-Poly1305): CryptoKit's `HPKE` on iOS 17 and Tink on Android. The sealed body stays under 2 KB to fit APNs' 4 KB limit; details are fetched.
- **The gateway is open source** (`loams-push`, a small Rust service in this repository, AP4) and **Loams operates the instance the store apps use** (an operational service in `loam-platform`, D220). Self-hosters who publish their own app builds run their own gateway with their own keys, as Mattermost requires. Instances register with the gateway for a credential; abuse limits are per instance (Q424).
- **Android without Google services:** a `unifiedpush` flavor; the instance posts the same sealed payload to the user's UnifiedPush endpoint (RFC 8030), with no gateway (Q439).
- **Desktop:** no push service; the running app holds `WatchApprovals` and raises native notifications (AP1 Task 8).
- **Categories:** approvals (high priority; actionable with **Review**, which opens the approval and asks for biometrics), operations, jobs, runs, security (device added, device revoked). Approvals may bypass quiet hours (a user preference, default on).

### 7.5 Offline and background (D437)

| Situation | Behaviour |
|---|---|
| Foreground, online | The visible screen holds its `Watch*` stream and writes through the cache |
| Foreground, offline | Cached lists with "updated 12 min ago"; Approve and Reject disabled; Cancel operation disabled |
| Background | No sockets. Push wakes the extension to display. Android: a `WorkManager` periodic sync (15 min, the platform's minimum) when push is unavailable. iOS: `BGAppRefreshTask`, opportunistic |
| A send fails mid-flight | Shown as "not sent" with Retry, which reuses the same `idempotency_key`; never retried silently |
| Sign-out or revocation | The instance's cache, keys and tokens are deleted |

### 7.6 Android specifics

Keystore keys per instance, EC P-256, StrongBox when present: `dpop-<instance>` without user authentication (background refresh works) and `decide-<instance>` with `setUserAuthenticationRequired(true)`, strong biometrics or device credential for every use, and invalidation on biometric enrolment changes; signing through `BiometricPrompt` with a `CryptoObject`. The refresh token is AES-GCM-encrypted with a Keystore key in DataStore (the harness's `RelayCredentialStore` pattern). `cleartextTrafficPermitted="false"`, no user CAs, `allowBackup="false"`, `FLAG_SECURE` on approval and pairing screens. Flavors `fcm` and `unifiedpush`. Details: AP2.

### 7.7 iOS specifics

Secure Enclave P-256 keys per instance: `dpop` (`.privateKeyUsage`) and `decide` (`.privateKeyUsage` + `.biometryCurrentSet`, or `.userPresence` without biometry), so DPoP and decision proofs are ES256. Keychain items `AfterFirstUnlockThisDeviceOnly`, never synchronizable, shared with the Notification Service Extension through an access group. ATS on with no exceptions outside Debug; pins evaluated in the `URLSession` delegate. Categories with a `REVIEW` action that opens the app (`.foreground`, `.authenticationRequired`). Details: AP3.

### 7.8 Kotlin Multiplatform: evaluated, not recommended (D433)

| For KMP shared logic | Against |
|---|---|
| One implementation of pairing parsing, decision canonicalization, watch resume and cache rules | **connect-kotlin does not support KMP** (connect-kotlin#140, open since 2023; it depends on OkHttp, `java.net` and `java.util.concurrent`, verified 2026-10-01). Shared Kotlin code could not use the generated client on iOS, so either the iOS app drops connect-swift's generated client, or shared code stops at plain functions |
| | Swift export is **Alpha**; Objective-C interop is Beta; SKIE 0.10.15 helps but is another tool |
| | The shared logic is small (a few thousand lines, **estimate**) and mostly pure functions with golden fixtures |
| | Two native toolchains are needed anyway (Xcode, Gradle); KMP adds a third build system to the iOS side |

**Recommendation:** no KMP. The protos are the shared contract; golden fixtures (canonical `DecisionClaims` bytes, pairing payloads, sealed notifications) and the conformance scenarios keep the two cores equal, and `LoamsCore` also builds on Linux so the fixtures run in Linux CI (AP3 Ruling 6). Revisit if connect-kotlin gains KMP support and the shared logic grows past roughly a third of either app.

## 8. The proto surface (D438)

### 8.1 What each app needs, and what exists

| Need | Desktop and console | Phones | On `main` today | Gap (plan) |
|---|---|---|---|---|
| Instance discovery, who am I, environments | ✓ | ✓ | REST `GET /api/v1/instance` and session (OpenAPI, §19 P9) | `loams.instance.v1` (AP0) |
| Identity administration (org, teams, projects, environments, agents, keys, audit) | ✓ | — | OpenAPI `/api/v1/*` | None; stays REST through the `api` service (Q423) |
| Collections | ✓ | — | REST `/v1/namespaces/{ns}/collections` (M1.6) | None for AP |
| Live | ✓ | — | `loams.live.v1.LiveService` (`Watch` server-streaming) | None |
| Jobs | ✓ | ✓ | `loams.jobs.v1` designed (D206), **not on `main`** | §26 J1 |
| Operations and durable runs | ✓ | ✓ | REST (D146) | `loams.operations.v1` (AP0) |
| Approvals | ✓ | ✓ | REST `POST /v1/operations/{id}/approve\|reject` (§21 §6.5, not built) | `loams.approvals.v1` (AP0) |
| Devices, pairing, push targets, preferences | ✓ (pairing) | ✓ | None | `loams.devices.v1` (AP0), the pairing grant (auth plan) |
| Inbox | ✓ | ✓ | None | `loams.notifications.v1` (AP0) |
| Connectors and routes (§33, §32) | ✓ | — | `loams.flow.v1` proposed on `flow-fabric-house-design` | None for AP |
| Console plugins | ✓ | — | None | `loams.console.v1.PluginService` (AP1a Task 7) |
| Local stacks | ✓ | — | The CLI's JSON (D283) | None: local only |

### 8.2 The new packages (AP0)

| Package | Service | RPCs (S = server-streaming) |
|---|---|---|
| `loams.instance.v1` | `InstanceService` | `GetInstance` (no auth; edition, versions, `api_versions`, features, issuer, JWKS URI, `tls_pins`, push config, minimum app versions), `WhoAmI` |
| `loams.devices.v1` | `DeviceService` | `CreatePairing`, `ListDevices`, `RenameDevice`, `RevokeDevice`, `RegisterPushTarget`, `UnregisterPushTarget`, `Get/SetNotificationPreferences`, `SendTestNotification` |
| `loams.approvals.v1` | `ApprovalService` | `ListApprovals`, `GetApproval`, `WatchApprovals` (S), `DecideApproval` |
| `loams.operations.v1` | `OperationsService` | `GetOperation`, `ListOperations`, `WatchOperations` (S), `CancelOperation` — the Connect face of D146 |
| `loams.notifications.v1` | `NotificationService` | `ListNotifications`, `WatchNotifications` (S), `MarkRead` |
| `loams.errors.v1` | — | `ErrorInfo { reason, metadata, hint }` |

Package names stay `loam.*` to match `loams.live.v1` and `loams.stream.v1`; whether the rename moves every proto package to `loams.*` before the first app release is Q422 (Connect URL paths contain the package, so it is cheap before a release and breaking after one).

### 8.3 Protocol choices

| Question | Choice | Why |
|---|---|---|
| Connect, gRPC-Web or gRPC | **Connect** for all three apps; connect-rust serves all three protocols on one port, so gRPC-Web stays available for proxies that need it | Works on HTTP/1.1 and HTTP/2; no trailers (URLSession has none); unary calls can be HTTP GET and cached; JSON is debuggable with curl |
| Codec | Binary protobuf in the apps; JSON in tests and debugging | Size and speed on mobile networks |
| Streaming | **Server streaming only**; no client or bidi streams anywhere | Browsers cannot stream requests; connect-rust answers nothing on HTTP/1.1 until the request body ends; half-duplex works through every proxy |
| Heartbeats | Every 15 s on every `Watch*` stream | AWS ALB's idle timeout defaults to 60 s and **ignores HTTP/2 PING frames**; Cloudflare's proxy read timeout is 125 s (verified 2026-10-01). Data frames keep both alive |
| Resume | Every stream response carries a cursor; a reconnect passes it and skips the snapshot, or gets `snapshot_reset` | Mobile networks drop streams; the harness re-sent full baselines on every reconnect |
| HTTP/2 on phones | Negotiated by ALPN (URLSession, OkHttp); HTTP/1.1 works too | Nothing depends on full duplex |
| Reads | `option idempotency_level = NO_SIDE_EFFECTS` | Connect clients send them as GET |
| Writes | `idempotency_key` on every mutation | Retries on flaky networks must not approve or revoke twice |
| Errors | Connect codes plus `ErrorInfo.reason` (`approval_expired`, `approval_already_decided`, `decision_proof_invalid`, `step_up_required`, `pairing_expired`, `pairing_used`, `device_revoked`, `push_target_unknown`) | Apps branch on reasons, as the CLI does on D283's codes |

### 8.4 Generation

`buf` generates every client from `proto/`: protobuf-es 2 (`@bufbuild/protobuf` 2.16, used by `@connectrpc/connect` 2.2) into `web/packages/proto` (`@loams/proto`, committed, checked for drift in CI); `buf.build/apple/swift` + `buf.build/connectrpc/swift` (`GenerateAsyncMethods`) and `buf.build/protocolbuffers/java` (lite) + `buf.build/connectrpc/kotlin` for the mobile repository, which generates at build time from a pinned git ref of this repository. The server keeps D128's generation (buffa and `connectrpc-build` in `build.rs`). `buf breaking` is enforced for the new packages.

## 9. Repository layout (D439)

> **Superseded 2026-10-02:** the desktop rows (`web/apps/desktop/`, `src-tauri`, `@loams/platform-tauri`) are superseded by D493: the desktop lives in `ostrium-labs/loams-desktop` (§18.9). Everything else in this layout stands.

**Recommendation: desktop and console in this repository; both phone apps in one new repository, `ostrium-labs/loams-mobile`.**

```
ostrium-labs/loams (this repository)
├── proto/loams/{instance,devices,approvals,operations,notifications,errors}/v1/   (AP0)
├── crates/operon-apps-mock/                       (AP0; scenarios shared by every app)
├── crates/loams-push/                             (AP4, not yet planned)
├── web/packages/{console-host,slots,forms,proto,platform-web,platform-tauri,ui}/
├── web/plugins/{shell,identity,agents,keys,audit,collections,jobs,durable,live,flow,connectors,gateway,approvals,devices,stacks,mcp,plugins}/
├── web/apps/console/                              (the browser build the engine embeds at /ui)
└── web/apps/desktop/ + src-tauri/                 (its own Cargo workspace and lockfile)

ostrium-labs/loams-mobile
├── android/  (core, proto, data, push, app, conformance)
├── ios/      (Packages/LoamsCore, LoamsProto, LoamsData; Loams app; LoamsNotificationService; LoamsConformance)
├── conformance/proto-ref.lock                     (this repository's git ref both apps generate from)
└── buf.gen.swift.yaml, buf.gen.kotlin.yaml        (copies of AP0's templates)
```

| Option | For | Against |
|---|---|---|
| **Desktop in the monorepo** (chosen) | It is the console plus a shell; it shares `web/` packages, the plugin set and the release of the `loams` binary it bundles; protos and the mock change in the same PR as the console | Tauri's dependency tree must not enter the engine's `Cargo.lock`: solved by its own workspace |
| **Mobile in its own repository** (chosen) | Xcode and Gradle toolchains, macOS CI runners, store release cadence and signing secrets stay out of the engine's CI; a contributor to the phones never builds the engine | Protos cross a repository boundary: solved by generating from a pinned git ref and the shared `proto-ref.lock` test |
| Mobile in the monorepo | One PR for a proto change and both apps | Every engine PR would carry mobile CI paths; signing secrets in the engine repository |
| Separate `loams-desktop` repository | Independent releases | Splits the console from its shell, and the `web/` workspace in two |
| Separate `loams-ios` and `loams-android` | Smaller repositories | Two copies of scenarios, fixtures and generation; the two cores drift |

## 10. Open-core placement (D220)

| Piece | Where | Licence |
|---|---|---|
| The console host, slots, forms, platform packages, every `oss` and `desktop` plugin | This repository | Apache-2.0 |
| Loams Desktop (shell, bridge, CLI integration, updater) | This repository | Apache-2.0 |
| Loams for iOS and Android | `ostrium-labs/loams-mobile` | Apache-2.0 |
| The app protos, `loams-apps-mock`, the server side of AP0's services (AP4) | This repository | Apache-2.0 |
| `loams-push` (the gateway's code) | This repository | Apache-2.0 |
| **Operating** the push gateway for the store apps, the store accounts, the signing identities | Loams (an operational service, run from `loam-platform`) | — |
| The console's multi-tenant, hosted and billing plugins and their catalog patch (D220; not designed here) | `loam-cloud`, `loam-platform`, through a private registry | Proprietary |

Nothing here makes this repository depend on `loam-platform`. A self-hoster gets every app, every open plugin and the gateway code; what they cannot get from us is our APNs key, which no one can share.

## 11. Security model

| Threat | Mitigation |
|---|---|
| A malicious or compromised console plugin steals credentials | Third-party plugins run in opaque-origin iframes with `connect-src 'none'` and attenuated, audited tokens (D426); on desktop, no token is ever in JavaScript (D430) |
| XSS in the console | Strict CSP without `unsafe-eval` or remote sources; no `!!js`; desktop tokens in Rust |
| A web page drives the desktop through `loams://` | Deep links only navigate; actions need a click in the app (D432) |
| A local process hijacks a stack | Loopback-only listeners (D111) and, before the auth plan, the Host/Origin fence (§3.1); after it, real auth |
| A stolen phone approves something | The decision key needs biometrics or the passcode for every signature and dies on enrolment change (D435); revocation from any other session (§7.2) |
| A stolen refresh token | DPoP-bound to a non-exportable key; rotation detects reuse |
| A swapped or MITM'd self-hosted server | QR pins before the first byte; `jkt` anchors identity even with public CAs; signed pin rotation (§7.2.3) |
| Apple, Google or the gateway read notifications | HPKE-sealed payloads; generic alert text (D436) |
| A replayed or stale approval decision | `revision` and `jti` in the signed claims; idempotency keys (AP0 Ruling 7) |
| Supply chain: plugins | SRI hashes, npm provenance for `first-party`, owner-only install, approval gates in protected environments (D427) |
| Supply chain: updates | Mandatory updater signatures with a dedicated key; notarization; Authenticode (D432) |
| cordis itself | Pinned exact versions, patches recorded, the option to vendor (§14 risk 1) |

## 12. Testing

> **Superseded 2026-10-02:** the `tauri-driver` and WebdriverIO desktop tests below are superseded by D495 (§18.11): native tests, a mock smoke test of the built binary, and upstream's suites.

The harness's three layers, shared across apps:

1. **Pure cores with golden fixtures.** Android `:core` and iOS `LoamsCore` test pairing payloads, error reasons, watch resume, decision canonicalization and unsealing against the same fixture files, which `loams-apps-mock` also uses. A descriptor-set hash test ties each app to `conformance/proto-ref.lock`.
2. **A shared, scriptable mock.** `loams-apps-mock` (AP0) serves every AP0 service over Connect, gRPC and gRPC-Web, is stateful, verifies decision proofs for real, and replays YAML scenarios (`approvals-basic`, `approval-expiry`, `stream-drop-and-resume`, `device-revoked`, `operation-progress`, `notification-burst`). Its validation is the server's `acceptance` module, so it refuses what the server will refuse.
3. **Conformance per app.** Each app runs every scenario and an endpoint catalogue (a typed business error counts as a pass) on the JVM or the macOS host, with no emulator or simulator for the network layer.

Plus, per surface: Vitest for plugins with fake services, Playwright for the browser console, `tauri-driver` with WebdriverIO on Linux and Windows for the desktop, Compose UI tests and screenshots, XCUITest. Security tests are named in each plan's Review Focus: the capability allowlist, the secret canary, deep-link property tests, pin bypass tests, and `iframe_cannot_reach_network`.

## 13. Roadmap: track AP (D439)

| Plan | Scope | Depends on | Status |
|---|---|---|---|
| [AP0](../plans/2026-10-01-ap0-app-protos.md) | The six packages, `loams-apps-mock` with scenarios, TypeScript generation and the Swift and Kotlin templates, the shared acceptance module | `main` only | Planned |
| [AP1a](../plans/2026-10-01-ap1a-cordis-console.md) | `@loams/console-host` on cordis v4, the catalog and manifest, slots, the `rpc.*` services, today's pages as first-party plugins, the trust tiers and iframe bridge, the plugin service and reload | AP0 Task 6 for `rpc.*`; the unified auth plan for Tasks 6–7 against a real server | Planned |
| [AP1](../plans/2026-10-01-ap1-desktop-tauri.md) | Loams Desktop: the CLI bridge and stacks, lockdown, the network bridge, sign-in and keychain, approvals, pairing, deep links, packaging and updates | AP1a Task 3; CLI1 (stacks, D283); D33 and the transfer for publishing | **Superseded** by [AP1n](../plans/2026-10-02-ap1n-native-desktop-zeron.md) (D499) |
| [AP1n](../plans/2026-10-02-ap1n-native-desktop-zeron.md) | Native Loams Desktop on a zeron fork: the scaffold (built), fork guards, environments and the credential rule, stack supervision, TLS pinning, Loams Bot over `loams.bot.v1`, approvals, the browser spike, Factory and collab panels, signed packaging and updates, rebase automation | AP0 for a published proto ref; SF2 and SF3 for Task 5; SF1 and SF4 for Task 8; the auth plan for the RFC 8693 exchange | Planned (Task 0 built) |
| [AP2](../plans/2026-10-01-ap2-android-compose.md) | Loams for Android | AP0; for a real server, the auth plan and AP4 | Planned |
| [AP3](../plans/2026-10-01-ap3-ios-swiftui.md) | Loams for iOS | AP0; the same as AP2 | Planned |
| AP4 | The server side: AP0's services in the gateway, the pairing grant and DPoP, decision-proof verification, the notifier, `loams-push`, `PluginService` | The unified auth plan (D111, Q30); §21 D2 (approval gates); §26 J1 for job events | Not yet planned |

Order: AP0 first; AP1a and the two phone plans in parallel; AP1 after AP1a's host. Everything runs against the mock until AP4; nothing is published before D33's rename and the move to `ostrium-labs`.

## 14. Risks

| # | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| 1 | **cordis has one maintainer and v4 is a release candidate** with an API "not yet stable" | Medium | High | Pin exact versions behind a thin `@loams/cordis` facade; record every patch (`pnpm patch`) in a modification log as the harness does; vendor when a patch is needed for more than 30 days or upstream goes quiet for 90 (Q427). The surface Loams uses is small: Context, plugins, services, effects, the loader |
| 2 | The browser-side loader depends on Node-oriented code stubbed in Vite | Medium | Medium | AP1a Task 1 proves it in a spike; fallback is a 200-line Loams loader over the same entry-list format |
| 3 | Iframe isolation for third-party plugins is clumsy for rich UI | Medium | Medium | Most extensions are first-party or self-built; the bridge offers forms, tables and charts from `@loams/ui` rendered host-side from data |
| 4 | connect-kotlin is still beta (0.9.0) | Medium | Medium | Thin use (unary and server streams over OkHttp); the conformance suite catches regressions; pin and upgrade deliberately |
| 5 | Store review rejects apps that need a self-hosted server | Medium | Medium | A bundled demo mode with seed data (Q435) |
| 6 | Push gateway abuse or cost | Low | Medium | Per-instance credentials and limits; sealed payloads make content abuse pointless; self-hosters can run their own (Q424) |
| 7 | _(superseded by §18.10)_ A bundled `loams` binary makes the desktop large | Medium | Low | Q428: download on first run instead |
| 8 | The auth plan slips, leaving the apps mock-only | Medium | High | AP0–AP3 deliver everything except real-server use; AP4 is small once the auth plan exists |
| 9 | _(superseded by §18.10)_ The Tauri `Channel` bridge is too slow for large exports | Low | Low | AP1 Task 0 measures; large downloads can go to a file through a separate command |
| 10 | _(superseded by §18.10)_ macOS has no WebDriver for WKWebView, so desktop e2e is weaker there | High | Low | Linux and Windows e2e; a launch-and-screenshot check on macOS (verify) |

## 15. Open questions

| # | Question | Owner | Needed by |
|---|---|---|---|
| Q420 | Store and signing accounts: an Apple Developer Program membership and a Google Play developer account for the `ostrium-labs` entity (D-U-N-S number, legal name), and who holds the Developer ID and upload keys. **Owner action pending (2026-10-02):** enrol the `ostrium-labs` entity in the Apple Developer Program (D-U-N-S, legal name) and Google Play, and decide who holds the Developer ID identity and the upload keys | Owner action | AP1 Task 11, AP2 Task 10, AP3 Task 10 |
| Q421 | Windows code signing: Azure Artifact Signing (needs organisation validation), a Key Vault certificate, or unsigned betas. **Owner action pending (2026-10-02):** choose and pay for Windows code signing (Azure Artifact Signing with organisation validation, or a Key Vault certificate); betas ship unsigned until then | Owner action | AP1 Task 11 |
| Q422 | Rename the proto packages `loam.*` to `loams.*` in D33's rename PR, before any app is published? | Founder | Before AP2/AP3 store releases |
| Q423 | ~~Keep the console's OpenAPI `/api/v1` (§19 P9) for identity administration, or move it to Connect (`loams.console.v1`) so that every console call is an `rpc.*` service? Proposed: keep REST for M2 and revisit~~ Answered 2026-10-02 by the owner: the recommended default — keep REST `/api/v1` for M2 and revisit; §37 does not recommend moving it to Connect now (§37 §8.1) | Founder | Resolved |
| Q424 | ~~The official push gateway at `push.loams.dev`: free for every self-hosted instance using the store apps? Limits, a privacy policy, and instance registration~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — free for every self-hosted instance, with per-instance rate limits, registration by instance key, and payload-free pushes the app resolves against its instance; why: adoption, and the gateway never sees content; the privacy policy's text is an owner action | Founder | Resolved |
| Q425 | ~~Reaching private instances from phones (a laptop stack, a self-hosted cluster behind a firewall): build an end-to-end relay, or document VPNs and tunnels only (proposed for track AP)?~~ Answered 2026-10-02 by the owner: the recommended default — VPNs and tunnels only for track AP; no relay (§37 §7.2.4) | Founder | Resolved |
| Q426 | ~~Third-party console plugins in OSS: allowed (proposed: yes, off by default, org owners install), and is npm provenance enough to call a package `first-party`, or only an `ostrium-labs` allowlist?~~ Answered 2026-10-02 by the owner: the recommended default — allowed, off by default, installed by org owners, always in a sandboxed iframe with attenuated tokens (D426), and disabled until the unified auth plan; `first-party` means npm provenance from a trusted publisher (`github.com/ostrium-labs/*` plus a build's own list), not provenance alone (§37 §5.6) | Founder | Resolved |
| Q427 | ~~cordis: depend on pinned npm `cordis@4.0.0-rc.10` with patches (proposed), vendor it now like the harness, or wait for 4.0?~~ Answered 2026-10-02 by the owner: the recommended default — pinned npm `cordis@4.0.0-rc.10` behind `@loams/cordis` with `pnpm patch` (§37 §14 risk 1) | Eng | Resolved |
| Q428 | ~~_(carries over to the native desktop; see Q488)_ Bundle the `standard` `loams` binary in the desktop app (proposed) or download it on first run through the CLI's variant mechanism?~~ Answered 2026-10-02 by the owner: the recommended default — bundle the `standard` binary (§37 §6.2); it carries over to the native desktop as Q488 (§37 §18.3) | Eng | Resolved |
| Q429 | ~~_(carries over to the native desktop, §18.3)_ On quit, leave stacks running (proposed) or stop the ones the app started?~~ Answered 2026-10-02 by the owner: the recommended default — leave stacks running on quit (§37 §6.2) | Founder | Resolved |
| Q430 | ~~_(superseded by Q492)_ Linux packages: AppImage, deb and rpm (proposed), plus Flatpak or Snap?~~ Answered 2026-10-02 by the owner: the recommended default — AppImage, deb and rpm; Flatpak or Snap only on demand (§37 §6.6) | Eng | Resolved |
| Q431 | ~~Minimum OS versions: iOS 17, Android 10 (API 29), macOS 13 (proposed)~~ Answered 2026-10-02 by the owner: the recommended default — iOS 17, Android 10 (API 29), macOS 13 (§37 §7) | Founder | Resolved |
| Q432 | ~~May a user approve an operation an agent requested on their behalf? Proposed: no by default, an org policy can allow it for non-protected environments~~ Answered 2026-10-02 by the owner: the recommended default — no by default; an org policy may allow it outside protected environments (§37 §7.3) | Founder | Resolved |
| Q433 | ~~Crash reporting in the apps: none (proposed, D284's no-telemetry rule) or opt-in Sentry, which `loam-cloud` already uses?~~ Answered 2026-10-02 by the owner: the recommended default — none, per D284's no-telemetry rule (§37 §16) | Founder | Resolved |
| Q434 | App names on the stores ("Loams", "Loams for iOS") and a trademark check. **Owner action pending (2026-10-02):** a trademark check of "Loams" (counsel) before the store names are registered | Owner action | Store releases |
| Q435 | ~~App review: a bundled demo mode (proposed) or a hosted demo instance and account?~~ Answered 2026-10-02 by the owner: the recommended default — a bundled demo mode with seed data (§37 §14 risk 5) | Founder | Resolved |
| Q436 | ~~Does Loams Cloud's console (today a Next.js app in `loam-cloud`, with Clerk, which the Authentik ruling retires) move onto the cordis host as private plugins from the private registry (§19 P1, proposed), or stay separate?~~ Answered 2026-10-02 by the owner: the recommended default — yes, Loams Cloud's console moves onto the cordis host as private plugins from the private registry (§19 P1, §37 §5.8) | Founder | Resolved |
| Q437 | ~~_(carries over, §18.3)_ Windows desktop: remote-only (proposed), stacks through WSL2, or a Windows server variant (§30 Q285)?~~ Answered 2026-10-02 by the owner: the recommended default — remote-only on Windows (§37 §6.2), consistent with Q285 | Founder | Resolved |
| Q438 | ~~Does the unified auth plan add, on the Loams gateway, the exchange of Authentik tokens for Loams tokens (RFC 8693), DPoP (RFC 9449) for user tokens issued to devices, device-bound rotating refresh tokens and the pairing extension grant? Which Authentik release provides the device-code flow and the step-up (`max_age`) the apps rely on? §19 §5.3 lists DPoP as a follow-up for agents only~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — yes: the unified auth plan adds the RFC 8693 exchange, DPoP for device tokens, device-bound rotating refresh tokens and the pairing grant, and MT1 Task 0 checks that the pinned Authentik (2026.8.x, D447) has the device-code flow and `max_age` step-up; why: the apps' designs (§37 §7.2) rely on all four | Founder, Eng | Resolved |
| Q439 | ~~Ship the Android `unifiedpush` flavor on F-Droid (reproducible builds), and when?~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — yes, after the first Play release (AP2 Task 10); why: adoption among users without Google services | Founder | Resolved |

## 16. Contradictions with earlier decisions, and how they are resolved

| Existing | What this document needs | Proposed resolution |
|---|---|---|
| **§19 P9** (the console contract is OpenAPI 3.1) and the owner's "Connect-RPC everywhere" | The apps use Connect | New surfaces are Connect (D438); the console's identity administration stays on the OpenAPI contract through the `api` service until Q423 decides |
| **§19 P1** (one console for OSS, Cloud and BYOC) | `loam-cloud` has a separate Next.js console with Clerk | The cordis host with the open sets and private hosted plugins realizes P1 within D220 (D428); converging `loam-cloud` is Q436 |
| **§19 P7, §6** (built-in passwords and TOTP, generic OIDC, Keycloak to broker SAML), §19 §3's "Clerk or Keycloak" for Cloud, and **D-SC-3** (Keycloak as the showcase suite's OIDC provider, §22 §4.4) | The owner's ruling: Authentik, open-source edition, is the identity provider; Clerk and Keycloak are gone | The apps sign in at Authentik and exchange at the Loams gateway (D431, D434); §19 needs the matching revision, which belongs to the identity work, not §37 |
| **§19 §6** (cookie sessions with CSRF) | The desktop sends bearer tokens through the bridge | Browsers keep cookies; the auth plan must accept a bearer on `/api/v1/*` and return the principal from `GET /api/v1/session` |
| **§19 §5.3** (DPoP is a follow-up for agents; users' refresh tokens are bound to the session) | Device-bound, DPoP-bound user tokens for phones | Q438 asks the auth plan to add both |
| **§19 §5** (three principal kinds) | Phones | No new kind: a device is a credential of a user (D434) |
| **§21 §6.5** (approve and reject over REST) | A shared approvals service with proofs | `loams.approvals.v1` (D435); the REST routes can remain as thin wrappers |
| **D146** (operations over REST) | Streams of operations for apps | `loams.operations.v1` is the Connect face of the same state (D438) |
| **D111** (no auth or TLS in M1; loopback listeners) | Phones need remote, authenticated instances | Phones are mock-only until the auth plan; desktop local stacks work on loopback now |
| **D33** (`loamdb` packages; working names) and the owner's 2026-10-01 `loams` ruling | npm `@loams/*`, the binary `loams` | This document follows the ruling, recorded as D400 (which amends D33) |
| **§30** (the binary, Q284; the npm scope, Q283) | The desktop's sidecar is `loams` | Consistent: Q284 and Q283 were answered on 2026-10-02 (D401, D400); §30 now writes `loams` |
| **§30 D294** (`self-update` refuses installs it did not make) | A desktop-provided binary on PATH | A new `install_method: "desktop"` that `self-update` refuses (D429) |
| **§30 D289** (no destructive MCP tools) | The desktop's stacks page | Consistent: creation and deletion stay CLI commands in AP1 |
| **D284** (no telemetry) | The apps | The apps send no telemetry by default (Q433) |
| **D128** (one protobuf toolchain: buffa and connect-rust on the server, buf with protobuf-es for clients) | Swift and Kotlin clients | Extended, not changed: the same `buf` inputs gain Swift and Kotlin templates |
| **§26 D206** (`loams.jobs.v1`, not on `main`) | Jobs screens | Feature-gated on `api_versions`; the apps ship before J1 |
| **§19 §3** (`@loam/console`, `@loam/ui`) | `@loams/*` plugins | Renamed in D33's rename PR with everything else |
| **§34 D364** (`io.loams.dev.` event and schema naming) | CloudEvents `io.loams.dev.*` per the owner's ruling | Consistent: Q361 was answered with `io.loams.dev.` (D402) and §34 D364 follows it |
| **§19 P8** (no Node service beside the binary) | cordis | Only cordis's browser half is used; there is no Node host (§2.3) |
| The harness itself: `cordis_define` (model-written plugins) | — | Not adopted (§2.3) |
| The harness itself: `!!js` in `cordis.yml` | — | Forbidden in Loams catalogs (D423) |

## 17. Sources

Read on 2026-10-01 and 2026-10-02.

- **Harness desktop** (`dina-kar/deepseek-harness-desktop` at `2d1b505`): `LICENSE`, `README.md`, `AGENTS.md`, `THIRD_PARTY_NOTICES.md`, `apps/desktop/src-tauri/{src/lib.rs,tauri.conf.json,tauri.macos.conf.json,capabilities/default.json,Cargo.toml}`, `apps/desktop/scripts/prepare-runtime.mjs`, `.agents/notes/implemented/architecture/2026-08-14-tauri-desktop-sidecar-host.md` and `2026-07-23-client-plugin-loading-model.md`, `packages/client/connection/src/api-request-trust.ts`, `packages/client/web/src/boot.tsx`, `packages/client/ui-slots`, `packages/bundle/web-app/cordis.patch.yml`, `packages/boot/app-boot/src/profile.ts`, `vendor/README.md`, `vendor/loader/src/config/{entry.ts,tree.ts,utils.ts,isolate.ts}`, `vendor/cordis/src/registry.ts`, `scripts/gen-cordis-catalog.ts`, `docs/api-gateway.md`, `tsconfig*.json`.
- **Harness mobile** (`dina-kar/deepseek-harness-mobile` at `68b6c2f`): `LICENSE`, `README.md`, `THIRD_PARTY_NOTICES.md`, `settings.gradle.kts`, `gradle/libs.versions.toml`, `docs/{PROTOCOL.md,SECURITY.md,COMPATIBILITY.md}`, `core/.../wire/{RelayPairing.kt,RelayTls.kt,ConnectionLoop.kt,RemoteStreamMux.kt}`, `app/.../data/{SessionStore.kt,RelayCredentialStore.kt,HarnessSessionStore.kt}`, `app/.../notify/*`, `app/src/main/res/xml/network_security_config.xml`, `mock-harness/`, `conformance/`, `.github/workflows/ci.yml`.
- **cordis:** github.com/cordiverse/cordis (licence, releases `v4.0.0-rc.7` to `rc.10`, contributors); npm `cordis`, `@cordisjs/plugin-loader`, `@cordisjs/plugin-include`, `@cordisjs/plugin-hmr`, `@cordisjs/plugin-webui`, `@koishijs/plugin-console` (licence metadata).
- **Connect:** github.com/connectrpc/connect-es (2.2.0), connectrpc.com/docs/{faq,node/using-clients,swift/using-clients,kotlin/using-clients,kotlin/getting-started}; github.com/connectrpc/connect-swift (1.2.3, `Package.swift`, conformance configs); github.com/connectrpc/connect-kotlin (0.9.0, issue #140); github.com/connectrpc/connect-rust and buf.build/blog/connect-rust-joins-the-connect-project; `connectrpc` 0.9.1 on crates.io; github.com/connectrpc/connect-query-es (2.3.1); npm `@bufbuild/protobuf` 2.16.0.
- **Tauri:** github.com/tauri-apps/tauri releases (2.12.1); v2.tauri.app/plugin/{updater,deep-linking,single-instance}, v2.tauri.app/develop/sidecar, v2.tauri.app/distribute/sign/{macos,windows}, v2.tauri.app/blog/tauri-20; tauri-apps/plugins-workspace#3494 (Stronghold); github.com/tauri-apps/tauri-action (1.0.0); github.com/FabianLars/tauri-plugin-oauth (2.1.0); crates.io `keyring` (4.2.0).
- **Mobile platform:** Apple's "Establishing a token-based connection to APNs", the Apple developer forum thread on URLSession trailers; github.com/square/okhttp; kotlinlang.org/docs/{components-stability,native-swift-export}; github.com/touchlab/SKIE releases; unifiedpush.org (spec).
- **Push gateways:** github.com/element-hq/sygnal; docs.mattermost.com/deploy/mobile/host-your-own-push-proxy-service; docs.ntfy.sh/config; companion.home-assistant.io/docs/notifications/{notification-details,notification-local}.
- **Proxies:** developers.cloudflare.com/fundamentals/reference/connection-limits and the 524 error page; docs.aws.amazon.com/elasticloadbalancing/latest/application/edit-load-balancer-attributes.
- **Loams:** §19, §21 (§6.4, §6.5), §26 (D206, §6.6), `docs/open-core.md` (D220, D221), §30 and its pending log (branch `cli-design`), §32–§33 and their pending log (branch `flow-fabric-house-design`), §34 (branch `gateway-runtime-design`); `web/` on `main` at `9eaddae` (`apps/console/{vite.config.ts,src/main.tsx,src/api/client.ts,src/session.tsx,src/pages/auth.tsx}`, `packages/ui`), `api/console/openapi.json`, `crates/operon-console-mock`, `buf.yaml`, `buf.gen.yaml`, `proto/loams/live/v1/live.proto`, `crates/operon-stream-grpc/proto/loams/stream/v1/stream.proto`; `loam-cloud` at `c738de7` (`next.config.*`, `proxy.ts`, `app/(console)/`, `lib/console.ts`, `lib/integrations.ts`; read only).


## 18. Native desktop on a zeron fork (supersedes the Tauri desktop)

Status: **Proposed** · 2026-10-02. The direction is the owner's, given on 2026-10-02: **"instead of Tauri go native for desktop apps also: https://github.com/zeronsh/zeron"**, with the product names **Loams Bot** and **Loams Software Factory**. This section turns that ruling into decisions **D480–D499** and open questions **Q480–Q499**, recorded in the [decision log](13-decision-log.md). Everything beyond the ruling is a **proposal** until the owner confirms it. The plan is [AP1n](../plans/2026-10-02-ap1n-native-desktop-zeron.md); the scaffold is the branch `loams-scaffold` of [`ostrium-labs/loams-desktop`](https://github.com/ostrium-labs/loams-desktop) (§18.12).

Markers: **(verified 2026-10-02)** means read in zeron's tree at `80b946b` (2026-10-01) or on GitHub that day. **(verify)** means the plan task that builds it checks it first. **(estimate)** means computed, not measured.

### 18.0 What this supersedes, and where it still points

| Of this document | Now | Read instead |
|---|---|---|
| D420's "Loams Desktop (Tauri 2, …)" | A native app on a zeron fork | D480, §18.1 |
| D421 (borrow the harness desktop's pattern) as it applies to the desktop shell | The Tauri host pattern is not used; the harness mobile patterns stand | §18.1 |
| §3.1 (the harness desktop's Tauri host) and risks 7, 9, 10 | Historical | — |
| D429 (the Tauri shell and CLI sidecar) | Stacks are still supervised through the CLI, from a native app | D488, §18.3 |
| D430 (capability lockdown, `net_fetch` bridge), §6.3, §6.4 | There is no webview holding JavaScript to defend; the credential rule is for agent subprocesses | D489, §18.7 |
| D431 (Authentik sign-in) | Unchanged in substance, now in Rust inside the app | D486, §18.7 |
| D432 (updater, signing, platforms, deep links), §6.6, §6.7 | Zeron's updater with a signed manifest; same signing accounts; deep links stay navigation-only | D490, §18.8 |
| D439 (desktop in the monorepo, `web/apps/desktop`) | Its own repository | D493, §18.9 |
| §9's `web/apps/desktop/` row and `@loams/platform-tauri` | Removed from AP1a's scope | §18.11 |
| §12's `tauri-driver` end-to-end tests | Native tests | D495, §18.11 |
| §13's AP1 row | Replaced by AP1n | D499 |
| Q428–Q430, Q437 | Q428, Q429 and Q437 carry over; Q430 becomes Q492 | §18.3, §18.8 |
| "Tauri is superseded" (D480, this table) | **Amended 2026-10-02 (the owner: "do not drop Tauri")**: Tauri is not the desktop shell, but returns as a separate **web bridge** that agents drive as MCP tools | §18.14, D500–D512 |

Unchanged: D422–D428 (the **web** console on cordis, in the browser at `/ui`), D433–D438 (the phones and the app protos), §10's open-core placement for everything except the desktop's repository.

### 18.1 The ruling, and what zeron is (verified 2026-10-02)

| Fact | Value |
|---|---|
| Repository, licence | `zeronsh/zeron`, **MIT**, "Copyright (c) 2026 Wing"; about 2 670 stars, 30 contributors, last push 2026-10-01 |
| What it says it is | "A native control plane for Claude Code, Codex, Cursor, Devin and other coding agents": every device runs a small engine that stores its sessions locally; multi-device sync is optional |
| Language and UI | Rust (edition 2024) on **GPUI**. Zeron pins `zeronsh/zui` (an Apache-2.0 extraction from Zed's GPUI with the GPL tracing crates removed and blur and edge-fade additions) and its own `gpui-component` fork. Its `ARCHITECTURE.md` states it uses none of Zed's GPL crates |
| Size | 17 crates plus the `zeron` app; `ui` 180 k lines of Rust, `engine` 79 k, `harness` 49 k, `client` 13 k, `doc` and `sync` 10 k each |
| Shape | **Engine** (sessions, run journals, repos and worktrees, diffs, terminals, agent accounts, device identity) and **UI** talk one typed RPC, in-process or over loopback IPC (`ws://127.0.0.1:27654`). `zeron` is headed; `zeron headless` is the engine alone; `zeron daemon` installs it as a service |
| Agents | Native drivers for Claude Code (stream-json), Codex (app-server), Cursor, Pi and opencode, and **ACP** (Agent Client Protocol, JSON-RPC over stdio) for Devin, Grok, Hermes and Antigravity. A mock harness exists for tests |
| Agent-facing surface | `zeron mcp`, an MCP server over the engine's IPC that lets an agent create, read and message other chats; the engine **injects it into every run** it drives |
| Data and sync | Sessions are Loro CRDT documents persisted in SQLite on the device. Optional sync runs through **zeron's own Cloudflare Durable Objects edge and WorkOS sign-in** (`edge/`, `edge.zeron.sh`). A clean install starts local-only with no account and no network |
| CLI | `zeron status`, `login`, `logout`, `sync`, `mcp`, `update`, `daemon install`, `start`, `stop`, `restart`, `status`, `headless` |
| Updates | The app and CLI check `{edge}/releases/manifest.json` (a version and a SHA-256 per artifact) hourly, download in the background, and swap a versioned managed install or an app bundle. Nothing is signed beyond TLS and the checksum |
| Packaging and CI | Installers for Linux (tarball and `install.sh`), macOS (dmg; signs and notarizes when secrets exist) and Windows (Inno Setup installer and a portable zip). Workflows: Linux UI and core tests, Windows tests and packaging, macOS and aarch64 Linux builds in the release workflow |
| Sidebar browser | **Linux:** a separate WebKitGTK helper process that sends offscreen frames to GPUI, with an **ephemeral** website-data store. **macOS:** `wry` (WKWebView). **Windows: none** (listed as remaining work in zeron's own Windows notes) |
| Telemetry | None in the Rust crates (searched on 2026-10-02) |
| Velocity | About 670 commits and 30 tagged releases in September 2026; 130 commits in the last eight days |

What zeron does **not** have: an extension or plugin runtime (its extension points are MCP servers, ACP agents, skills and themes), an organisation or identity model, an approvals concept, or any notion of a server. It is a very good single-user agent cockpit; Loams adds the server-connected half.

### 18.2 What to keep, change, add and drop (D481)

| Area | Decision | Why |
|---|---|---|
| Engine, run journals, repos, worktrees, diffs, terminals | **Keep** unchanged | The product; 79 k lines Loams does not want to own |
| Harness drivers (Claude Code, Codex, Cursor, Pi, opencode, ACP agents) | **Keep** | Loams Bot sits beside them; the coding agents are what people also use |
| gpui UI, themes, composer, transcript, sidebar | **Keep**; Loams adds a harness icon and, later, panels | |
| Local-only profile and the data directory (`~/.zeron`, `ZERON_*`) | **Keep** the names (Q489) | Renaming touches hundreds of lines and every rebase |
| `zeron-update` mechanism | **Keep**, **change** the feed and add a signature (D490) | |
| Voice dictation (`zeron-voice`, Parakeet model, optional) | **Keep**, off unless the user enables it | Not ours to remove |
| Name, window title, app id, icon, `.desktop` file | **Change** through `loams-brand` and `dist/loams/` | One reviewed place |
| Default edge URL and WorkOS client id | **Change**: both fail closed (§18.7) | They name zeron's private cloud |
| Sign-in | **Change**: Authentik (D486) | |
| `loams-brand`, `loams-link` (Connect client, mock, OIDC, A2A stub, ACP shim, CLI verbs) | **Add** | Everything Loams-specific that has no GPUI in it |
| Loams Bot harness (`HarnessId::LoamsBot`) | **Add**, in a separate file plus the match arms the compiler demands | D483 |
| `loams-panels` (native GPUI panels over `loams.collab.v1`, approvals, operations, stacks) | **Add** later (AP1n Tasks 3–8) | |
| `edge/` (Cloudflare Worker), `apps/landing`, `apps/www-redirect`, `apps/ios`, TestFlight and deploy workflows | **Drop** from the product (left in the tree, workflows disabled in repository settings, so rebases stay clean) | Zeron's cloud and iOS app |
| Zeron's sync crates (`zeron-sync`, the registry rooms) | **Keep compiled, unreachable** (D487) | Removing them is a large, conflict-prone patch |

### 18.3 The engines and the `loams` binary (D482, D488)

```
  ┌──────────────────────────── Loams Desktop (zeron fork, one binary `zeron`) ───────────────────────────┐
  │  gpui UI ──── typed RPC (in-process or ws://127.0.0.1:27654) ──── zeron engine                       │
  │   · transcript, composer, terminals, diffs                       · sessions, run journals, worktrees  │
  │   · harness picker: Claude Code, Codex, … , Loams Bot            · agent subprocesses (user's rights) │
  │   · loams-panels (later): approvals, stacks, operations, collab                                       │
  │                                                                                                      │
  │  loams-link (no GPUI)                                                                                 │
  │   · Connect clients (buf-generated)  · Authentik OIDC + keychain  · stack supervisor (CLI JSON)      │
  │   · `zeron loams bot-acp`: ACP agent ⇄ A2A (stub) / loams.bot.v1 (SF3)                               │
  └──────┬──────────────────────────┬────────────────────────────────────────────────┬───────────────────┘
         │ (1) supervise            │ (2) Connect-RPC                                │ (3) spawn, ACP on stdio
         ▼                          ▼                                                ▼
   `loams` CLI  ─ stacks      loams server (local stack or remote)            Loams Bot ACP shim (same binary)
   `loams stack … --output json`   loams.instance / approvals / operations /        └─ production: loams.bot.v1 (Connect) to the
                                                                                      server-side Loams Bot, which speaks A2A to
                                                                                      Plane, Zulip, Forgejo, GlitchTip, analytics
   shared LOAMS_HOME                 collab / bot .v1                                      (scaffold stub only)
```

**Three seams, and no fourth.**

1. **Supervise.** Stacks are managed by running `loams stack <verb> --output json` (§30 D283, D285) with no TTY, `LOAMS_NO_UPDATE_CHECK=1` and a timeout, parsing the one JSON document on stdout. `LOAMS_HOME` is shared with the terminal, so a stack created either way appears in the other. Stacks outlive the app. `stack create` and `stack delete` stay copy-paste commands in the first release. The restart policy AP1 specified (`keep_running`, backoff 1 s to 30 s, five tries in ten minutes) moves into `loams-link`. Windows stays remote-only (Q437), because no Windows server variant exists.
2. **Connect.** The app calls a running loams server with connect-rust clients generated by `buf` from the same protos the server, the console and the phones use (D128, D438). The scaffold has `loams.instance.v1` (`GetInstance`, `WhoAmI`); approvals, operations, notifications, collab and bot arrive as AP0 and the SF plans land. Remote HTTPS needs connect-rust's `client-tls` and the pinning of §7.2.3 (AP1n Task 4).
3. **Spawn.** Loams Bot is an ACP agent the zeron engine launches like any other (D483).

**Why not embed the `loams` engine in the app.** It is Rust, so it is tempting. Against it: the engine workspace is about 950 crates (DataFusion, Arrow, Lance, TiKV clients), and linking it beside GPUI in one binary would multiply link time and memory on a build machine already at its limit; an engine crash or a stuck compaction would take the window down; the app and the engine could no longer release independently; and the CLI already owns stack supervision (`stack.toml`, port blocks, readiness, `setsid`, log rotation). Embedding would also give two supervisors of one directory. **Why not run stacks inside zeron's engine:** it has no notion of one, and teaching it would be a large patch to a file set that changes daily.

Open: where the bundled `loams` binary lives and whether one updater may touch both it and zeron's `~/.zeron/app` layout (Q488); a native Stacks panel versus CLI-only buttons first (Q497, which turns on whether a stack is a zeron "space").

### 18.4 Loams Bot as zeron's chat and agents surface (D483, D497)

Zeron's chat surface already gives Loams Bot threads, an attention-sorted sidebar, drafts, a durable queue, steering, interrupts, attachments, tool cards, a question panel and local persistence. The cheapest faithful way to put Loams Bot on it is the way zeron puts Devin and Hermes on it: **as an ACP agent**.

| Zeron concept | Loams Bot behaviour |
|---|---|
| Harness | `HarnessId::LoamsBot`, wire name `loams-bot`, "Loams Bot" in the picker, running `zeron loams bot-acp`. It is the binary already running, so there is nothing to install or version-skew |
| `session/new` | A Loams Bot chat thread; its A2A `contextId` is derived from the session id (D-SF-8: the thread) |
| `session/prompt` | One A2A `SendMessage` (D-SF-6); the `messageId` is deterministic per turn so a replay deduplicates |
| An A2A task | An ACP **tool call**: `tool_call` when it starts, `tool_call_update` with `in_progress`, `completed` or `failed`. Zeron renders it as a tool card with live status, which is what §39 calls a subagent card |
| Agent text and artifacts | `agent_message_chunk` text; artifact cards (issue, PR, error, metric, run, approval) are a later native renderer (AP1n Task 5), not Markdown |
| `INPUT_REQUIRED` (a question) | Relayed as text now; as a zeron question panel (an ACP input request, which the engine already bridges) in AP1n Task 5, and the answer goes back as an A2A message in the same task: the **person** answers, never the bot |
| `INPUT_REQUIRED` (an approval, D435) | A card that opens the approval in the native approvals panel or the console; the decision needs a proof (a session younger than five minutes or a user-presence key) and is never made by the shim (**D497**, §39 D-SF-9) |
| `AUTH_REQUIRED` | Text with the link to the admin's "connect app" page |
| Cancel | `session/cancel` becomes `CancelTask` |
| Permission requests | Loams Bot sends none; zeron's auto-accept of an ACP agent's permission requests (it exists because zeron's sessions run unattended) is never exercised. Third-party coding agents keep zeron's behaviour, which the app should state plainly (Q485) |

**Who speaks A2A.** §39 D-SF-6 stands: clients speak Connect to Loams Bot, and Loams Bot speaks A2A to the five platform agents. In production the ACP shim therefore talks to `loams.bot.v1` (a Connect stream, SF3), not to agents. The **A2A client in `loams-link` is a stub**: a small JSON-RPC client (`SendMessage`, `CancelTask`) with a mock agent, so the chat path runs end to end before SF2 and SF3 exist, and so a developer can point `LOAMS_BOT_URL` at one agent. It follows A2A 1.0's JSON-RPC binding as §39 §5.3 records it and must be re-checked against `a2a.proto` in SF2 Task 0 (Q466).

**This amends D-SF-19 for the desktop only.** §39 planned to port the harness's conversation UI as cordis plugins (`@loams/plugin-bot`, `rpc.bot`). On the native desktop the conversation UI is zeron's, so that port is not built. Mobile is unchanged: native SwiftUI and Compose chat over `loams.bot.v1`. The server keeps the durable execution (a thread survives a locked phone or a closed laptop); the desktop keeps the transcript it shows. Which is the source of truth when they disagree is Q496. First-run default harness (Claude Code as upstream has it, or Loams Bot once an instance is configured) is Q484.

**The Loams Software Factory on the desktop** is the loop of §39 §10 seen from this chat: runs, stages, approvals and the kill switch are native panels over `loams.collab.v1` and `loams.factory.v1` (AP1n Task 8), plus Loams Bot's cards. Zeron's own `zeron mcp` server and its headless engine are an opportunity for the loop's coding step: a VPS running `zeron headless` could be the worker for the Forgejo agent's `propose_patch` sessions (§39 D-SF-18) instead of Loams' sandbox (Q498).

### 18.5 How the app UIs appear (D484)

§39 §3.2 fixed the rule: native, API-driven panels for the objects the loop reads and writes, and the app's own web UI, unmodified, for everything deeper. D-SF-5 implemented "deeper" as isolated Tauri child webviews. On a zeron fork that mechanism does not exist, and the verified state of zeron's browser changes the answer:

| Surface | Linux | macOS | Windows |
|---|---|---|---|
| Native GPUI panel (issue list, PR and CI status, error groups, metric tiles, approvals, runs) | yes | yes | yes |
| **System browser** (the person's own profile, Authentik session, passkeys, extensions) | yes | yes | yes |
| Zeron's sidebar browser tab | WebKitGTK helper, ephemeral store, no cookies kept between launches | `wry` (WKWebView) | **does not exist** |

The web bridge of §18.14 now supplies the third column's missing piece: a managed webview with persistent per-profile stores on all three operating systems ("Open in Loams Web"). So the defaults are: **panels first, "Open in browser" for the full app, and the sidebar browser as an opt-in that is on only where a spike proves it good enough.** Specifically:

- **Zulip, Plane, Forgejo, GlitchTip:** a native panel for the objects (from `loams.collab.v1`, SF1), and **Open in browser** to the app's own URL, where the person is already signed in at Authentik.
- **OpenPanel:** tiles in a panel, "Open" for the full UI. **Langfuse, OpenObserve:** links, never embeds (§39 D-SF-2).
- **The browser console itself** (`/ui` on the instance, with its cordis plugins) opens the same way: it is the place for administration, and it is where cordis plugins live (§18.6).
- **The sidebar browser** is a top-level browsing context, not a frame, so **D-SF-3's edge changes are not needed for it**: no `X-Frame-Options` removal and no `frame-ancestors` for `tauri://localhost`. A page that cannot be framed works fine here.
- **What the spike (AP1n Task 7, Q482) must prove:** a persistent data store on the Linux helper and on macOS, so that signing in to Authentik once lasts; passkeys and WebAuthn on each OS; a Windows browser on `wry` (WebView2); and whether `gpui-wry` (Apache-2.0, `longbridge/gpui-kit`, 0.7.0) can sit on zeron's pinned GPUI fork at all, since it targets Zed's own GPUI **(unverified; it may need a patch)**. Everything the spike produces is generally useful and goes upstream first (D491).
- **Mobile does not embed** (§39 D-SF-2, unchanged): native views, sealed push, deep links to the system browser.

**Amendment, 2026-10-03 — the sidebar browser ships, on Obscura over CDP (D620, superseding SF1 Task 0's E1 and closing Q482).** The row above is still accurate about zeron's browser, and E1 was right about what that browser could not do. What changed is the premise: the sidebar **embeds no webview at all**. `crates/loams-sidebar-browser` runs [Obscura](https://github.com/h4ckf0r0day/obscura) (Apache-2.0, Rust, headless, native rendering, no Chromium) as a pinned child process bound to `127.0.0.1`, and drives it over the DevTools protocol. That retires all three blockers at once, because all three were properties of the webview rather than of the feature:

- **Persistent store.** The engine has a real `--storage-dir`. Each `(environment, app)` pair gets its own directory, and Loams persists the app's scoped session cookies in its own ledger beside it, re-injecting on the next launch.
- **Passkeys.** Loams performs the OIDC/Authentik ceremony itself with its own OIDC client and hands the engine only post-auth, host-only scoped cookies. The engine never performs a WebAuthn ceremony — and cannot: the pinned engine has no WebAuthn implementation.
- **Windows.** There is no per-platform webview left to be missing. One command line covers Linux, macOS and Windows.

**The cost is fidelity, and it is stated in the product rather than discovered in it.** This is an **agent-driven surface**: frames arrive from a software rasteriser through `Page.startScreencast` and go into the GPUI panel as an image; interaction goes back as synthetic `Input.dispatchMouseEvent` / `dispatchKeyEvent`. There is no compositor, so no scroll momentum, no caret, no text selection and no 60 Hz animation, and a frame costs a full-page raster. It is not a general-purpose human-interactive browser. The fidelity risk is bounded because the embedded targets are Zulip, Plane and Forgejo — controlled, known applications whose login is a form, whose pages are server-rendered HTML with progressive enhancement, and whose scripts are not built against a Chromium-specific engine quirk; that is a real bound, not a guarantee, and the crate's README names what would falsify it.

### 18.6 The plugin story (D485)

Cordis no longer runs inside the desktop, because there is no webview. The extension story is therefore three tiers, each with a different trust level, and none of them needs a fork of zeron's UI to extend it.

| Tier | Mechanism | Runs in | For | Trust |
|---|---|---|---|---|
| **1. Now** | **MCP servers and ACP agents**, configured as zeron already does (the web bridge of §18.14 is one such MCP server). The engine injects its own `zeron mcp` into every run; Loams adds a `loams mcp serve` entry (§30 D289) beside it. Skills (`SKILL.md`) and themes (VS Code themes compile to zeron families) are also extension points | Separate processes | Tools, agents, prompts, themes: anything that fits "a process the engine talks to" | The user's own; third-party servers run with the user's rights, as in every MCP client |
| **2. First-party** | **Native panels**, Rust crates behind cargo features (`loams-panels`), reviewed in-tree | The UI process | Approvals, stacks, operations, collab, factory runs | `first-party` |
| **3. Later** | **WASM components** on wasmtime with a declarative UI interface: a plugin returns a view tree (lists, forms, tables, charts, text) that the **host** renders in GPUI; it never touches GPUI, the filesystem or the network directly. A permission manifest names the Loams services it may call, intersected with the user's, and calls carry the vended, attenuated token of D426 with the plugin as the actor | A wasmtime instance per plugin | Third-party panels and agent-tool views | `third-party`, always sandboxed |

Why not Zed's extension host: Zed's extension runtime is built into editor crates that are GPL; only the extension API crate is Apache-2.0, and zeron has deliberately kept every GPL crate out. A host of our own on wasmtime is small, because Loams already chose wasmtime for functions (§24), and a declarative UI keeps third parties out of the render loop. Whether tier 3 is worth building at all, and whether third-party desktop plugins are allowed in the open edition, are Q486 and Q487; tiers 1 and 2 need neither.

The **web console's plugins** (D422–D428) are unaffected: they run in the browser console, which the desktop opens like any other app. There is one console, in a browser, and one native cockpit beside it.

### 18.7 Sign-in, sync and credentials (D486, D487, D489)

- **Sign-in.** OIDC authorization code with PKCE (S256) at the instance's Authentik, in the **system browser**, redirect to `http://127.0.0.1:<ephemeral>/callback` (RFC 8252 §7.3), one request accepted, `state` and `nonce` checked, public client `loams-desktop`. The issuer and client id come from `GetInstance.sign_in_methods`. The refresh token is stored in the OS keychain (macOS Keychain, Windows Credential Manager, the Linux Secret Service) through the `keyring` crate, one entry per instance, and rotates on every refresh; the access token lives in memory only. Where no keychain exists, sign-in lasts the session and the app says so. The RFC 8693 exchange of the Authentik token for Loams tokens at the gateway (D431) is added with the unified auth plan (Q438). The scaffold implements the flow end to end against a fake provider in tests; it has not been run against a real Authentik.
- **Zeron's login is not used.** The fork's default WorkOS client id is a non-zeron placeholder and its default edge host cannot resolve (`edge.loams.invalid`), so zeron's `login` and sync paths are compiled but unreachable. Upstream's `zeron login` would otherwise send a Loams user to zeron's WorkOS tenant.
- **Sync (D487).** Zeron's multi-device sync is Loro documents through zeron's Cloudflare edge. Loams does not depend on another company's backend. The desktop is local-first on its device; continuity across devices is the server's: a Loams Bot thread is a durable execution visible from every device (§39 §13). Whether the Loro document layer is worth giving a Loams transport (streams over Connect) is Q483.
- **The credential rule (D489).** The Tauri design defended against JavaScript holding tokens. A native app has a different adversary: **the agents it launches**, which are subprocesses with the user's rights, and the sidebar browser. So: Loams tokens exist only in `loams-link`'s memory and the keychain; no agent subprocess and no browser receives one, not even through an inherited socket or pipe (calls an agent needs are made by `loams-link` on its behalf and only the result is returned); the injected `zeron mcp` server carries chat identity and nothing else; and when an agent must call Loams, it does so through the credential broker with an attenuated, audience-bound token (§39 §6), never the user's. Local stacks before the auth plan report `auth: none` (D111) and need no token.

### 18.8 Updates and signing (D490)

| Item | Choice |
|---|---|
| Mechanism | Zeron's: versioned managed installs behind a `current` link (Linux and macOS headless), an app-bundle swap (macOS), an in-place swap with `zeron-update.json` (Windows); a manifest of version and SHA-256 per artifact; check at start, hourly and on wake; download in the background; install on "Restart to update" or at quit; `ZERON_AUTO_UPDATE=0` to only report |
| Feed | GitHub Releases of `ostrium-labs/loams-desktop` for the first betas (Windows packaging already takes a releases URL), then a `loams.dev/desktop` redirect (Q493). **Until the feed exists the default host cannot resolve**, so a Loams build never installs zeron's binaries (the scaffold's `edge.loams.invalid`); the in-app "releases" links already point at the fork |
| Integrity | Zeron's manifest is protected by TLS and a checksum only. The fork adds a **detached Ed25519 signature over the manifest**, verified in `zeron-update` against a key compiled into the app, and **refuses an unsigned manifest**. The key is separate from the CLI's (Q282, Q494). This is also the best first upstream PR |
| macOS | Developer ID Application, hardened runtime, notarization and stapling: zeron's script does all of it when `MACOS_CERT_P12`, `MACOS_CERT_PASSWORD` and the App Store Connect API key secrets exist and falls back to ad-hoc signing without them (**owner action**, Q420, Q491) |
| Windows | Authenticode on the Inno Setup installer and `zeron.exe`; zeron has no signing today. Azure Artifact Signing or a certificate in a vault (**owner action**, Q421); unsigned betas trip SmartScreen |
| Linux | The tarball and `install.sh` (a per-user install under `~/.zeron/app` with a desktop entry); AppImage, deb, rpm and Flatpak are Q492 |
| Deep links | `loams://` stays navigation-only (D432). The desktop entry already claims `x-scheme-handler/loams`; macOS `CFBundleURLTypes` and the Windows protocol registration are AP1n Task 9 |

### 18.9 Upstream tracking, licensing and the repository (D491–D493)

**Cadence.** Zeron ships about two releases a day and moved 670 commits in September. Merging its `main` would be a permanent job, so the fork **rebases onto upstream release tags, not `main`**, at least every two weeks (Q480). A scheduled agent opens the rebase PR; CI is the judge. Conflicts are rare by construction.

**Patch discipline.** Loams changes are (a) **additive crates and files** (`loams-brand`, `loams-link`, `dist/loams`, `scripts/loams`, one workflow), (b) **one-line hooks** marked `// loams:` in inherited files, and (c) the match arms the compiler demands when `HarnessId` grows. `LOAMS.md` keeps a **patch ledger** of every inherited file touched and why; a CI job (AP1n Task 1) fails when an inherited file changes with no ledger line. The scaffold touches 20 inherited files (and `Cargo.lock`), almost all single lines (§18.12); the check itself is AP1n Task 1.

**What goes upstream first** (Q481): a config-driven custom ACP agent (which would delete the `HarnessId` patch), signed manifests, a persistent browser store, the Windows browser, an "ask" mode for permission requests. These are the changes zeron's other users also want, and each one that lands removes a patch from the ledger. **What stays in the fork:** anything that names Loams, Authentik or A2A.

**Licensing and attribution (D492).** `LICENSE` (MIT, "Copyright (c) 2026 Wing") is untouched. A root `NOTICE` records the fork, links upstream, and states that Loams-added crates are Apache-2.0 (D220). `THIRD_PARTY_NOTICES.md` stays upstream's (tree-sitter grammars, GPUI, `gpui-component`, Symbols icons) and gains nothing until Loams adds a bundled asset. A `cargo deny` licence job (AP1n Task 1) guards the dependency tree, and zeron's own statement that it uses no GPL Zed crate is kept true by it. The name "Zeron" is never used as a product name.

**Repository (D493).** `ostrium-labs/loams-desktop`, a public fork with upstream history, created with `gh repo fork zeronsh/zeron --org ostrium-labs --fork-name loams-desktop` on 2026-10-02. It vendors the protos it uses by pinned ref (`crates/loams-link/proto/PIN`, `scripts/loams/sync-protos.sh`), the pattern `loams-mobile` uses (§9). The monorepo keeps AP0, AP1a and the CLI. D439's layout row changes accordingly.

### 18.10 Risks

| # | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| 1 | **Upstream velocity and bus factor.** One fast-moving project (30 contributors, about 2 releases a day) is now our desktop; the maintainers' priorities are not ours | High | High | Additive patches, release-tag rebases, a patch ledger, upstream-first for generic changes; the exit is to freeze at a tag and carry the ledger |
| 2 | **A double fork of GPUI.** Zeron pins `zeronsh/zui`, itself extracted from Zed; its history and Zed's both move | Medium | High | We never touch GPUI; we rebase when zeron bumps its pin; no Loams crate depends on `gpui` except future `loams-panels` |
| 3 | **Accessibility.** GPUI has no screen-reader support today **(verify)**; an enterprise-facing desktop may need it | High | Medium | The browser console is the accessible surface; track GPUI's accessibility work; say so in the docs |
| 4 | **Windows maturity.** Zeron's own notes list authenticated provider runs, GPU and DPI coverage, accessibility and the browser as remaining Windows work | Medium | Medium | Windows is remote-only for stacks (Q437) and gets CI from day one; Windows panels use no browser |
| 5 | **Agent subprocesses run with the user's rights, and zeron auto-accepts ACP permission requests** | Medium | High | D489 and D497; Loams tokens never reach agents; an "ask" mode (Q485) |
| 6 | **Update supply chain.** Unsigned manifests; a wrong default host would install another project's binary | Medium | High | The unresolvable default host (done in the scaffold) and the signed manifest (D490) |
| 7 | **The sidebar browser may never be good enough** (ephemeral store, no Windows) | Medium | Low | It is optional; panels and the system browser carry the product |
| 8 | **GPU required.** GPUI needs Vulkan, Metal or Direct3D; remote desktops and some VMs have none | Medium | Medium | Document it; the CLI verbs (`zeron loams …`) and `zeron headless` need no GPU |
| 9 | **Name and brand confusion** with real zeron if both are installed (`zeron` on `PATH`, `~/.zeron`) | Medium | Low | The harness launches `current_exe`, never `zeron` from `PATH`; the data-directory and binary names are Q489 and Q490 |
| 10 | **The Authentik flow and the A2A shapes are unproven against the real servers** | Medium | Medium | Both are tested against fakes only; AP1n Tasks 2 and 5 run them against `loams-apps-mock` and a real Authentik before anything ships |
| 11 | **Connect-rust and buffa are pre-1.0** (0.9.x) | Medium | Low | Pinned; the generated code is committed; the server side already depends on them (D128) |

### 18.11 Testing, platforms and phasing (D494, D495, D499)

- **Platforms (D494):** Linux x86_64 and aarch64 (Wayland and X11), macOS on Apple silicon, Windows x86_64 (the set zeron's workflows build). Stacks on macOS and Linux; Windows is remote-only.
- **Tests (D495):** `loams-brand` and `loams-link` have no GPUI dependency and run in seconds on all three operating systems (the scaffold has 33 tests: PKCE against the RFC 7636 vector, the loopback listener, the full sign-in against a fake provider, the Connect client against the in-process mock, the A2A JSON-RPC client, and the ACP agent over an in-memory pipe). A smoke script drives `zeron loams status`, `bot` and `bot-acp` against the mock in the built binary on Linux and Windows. Generated code is regenerated and diffed in CI. Upstream's UI, engine and Windows suites run on every change because the shared crates changed. The Tauri plan's `tauri-driver` tests and risk 10 disappear.
- **Phasing (D499):** AP1n replaces AP1. AP1a drops its `desktop` catalog patch and `@loams/platform-tauri`. AP0 is unchanged (the desktop consumes its protos by pinned ref). SF1's Tauri child-webview tasks become panels and the spike of §18.5; SF3's desktop task becomes the harness shim of §18.4.
- **Stacks and approvals on the desktop:** approvals are a native panel over `loams.approvals.v1` with step-up through the system browser (`max_age=0`) and native notifications from zeron's notifier (D435, AP1n Task 6). Pairing a phone stays in the console first and gets a native QR panel in AP1n Task 8.

### 18.12 The scaffold (what the fork's first pull request contains)

Branch `loams-scaffold` of `ostrium-labs/loams-desktop`, on zeron `80b946b`, 2026-10-02. It is deliberately small and mostly additive.

| Piece | Where | What it does |
|---|---|---|
| Branding | `crates/loams-brand`, `dist/loams/`, four one-line hooks in `crates/ui/src/lib.rs` and `crates/update/src/lib.rs` | Product name, app id `dev.loams.desktop`, window title, the fork's releases links, a placeholder icon and desktop entry |
| Connect client and mock | `crates/loams-link` (`client`, `mock`, generated `proto` and `connect`) | `LoamsClient::get_instance` and `who_am_i` against a local `loams` server (`LOAMS_URL`, default `http://127.0.0.1:8080`); an in-process mock of `loams.instance.v1` for `LOAMS_MOCK=1`, tests and CI; code generated by `buf` from vendored protos and committed |
| Sign-in | `crates/loams-link/src/auth` | PKCE, OIDC discovery and token grants, loopback listener, system-browser launch, `KeyringStore` and `MemoryStore`; `zeron loams login` and `logout` |
| Loams Bot | `crates/loams-link/src/{a2a,acp}`, `crates/harness/src/acp/loams_bot.rs`, the match arms the compiler demanded in engine, harness, client and UI | The stub A2A client with a mock agent, the ACP agent (`zeron loams bot-acp`), and the harness registration that puts "Loams Bot" in zeron's picker |
| Fork hygiene | `LOAMS.md`, `NOTICE`, `.github/workflows/loams.yml`, `scripts/loams/` | Patch ledger and policy, attribution, CI (below), smoke test, proto generation and sync scripts |
| Fail-closed defaults | `apps/zeron/src/main.rs` | A placeholder WorkOS client id (not zeron's) and an unresolvable default edge host, so a Loams build never contacts zeron's cloud or installs its binaries |

CI added: the Loams crates on Linux, Windows and macOS; a build of the real binary with the mock smoke test on Linux and Windows; and the generated-code drift check. Zeron's own workflows already build the app on Linux and Windows for every `crates/**` change and on macOS in the release workflow, so they are not duplicated.

**Not in the scaffold** (each is an AP1n task): the stack supervisor, native panels, TLS to remote instances, the RFC 8693 exchange, `loams.bot.v1`, the signed update manifest and the feed, packaging overlays and signing, the sidebar-browser spike, the WASM host, deep links, and the rebase automation.

### 18.13 Contradictions with earlier decisions, and how they are resolved

| Existing | What this section needs | Resolution |
|---|---|---|
| **D420, D429–D432, D439, §6, §9, §12, §13's AP1 row** (Tauri) | A native desktop | Superseded as listed in §18.0; the old text stays for history |
| **§39 D-SF-5** (desktop embeds are isolated Tauri child webviews) and **§39 §3.4** | No Tauri | Replaced by §18.5 (panels, the system browser, an optional spiked sidebar browser); D-SF-3's edge changes are not needed for the desktop |
| **§39 D-SF-19** (`@loams/plugin-bot` ports the harness UI as cordis plugins on desktop) | Zeron's chat is the UI | Amended for desktop by D483; mobile unchanged |
| **§39 D-SF-2** ("Tauri child webviews") wording | | Superseded for the desktop by D484; the embed rule for the browser console stands |
| **D421** (fork neither harness repository; borrow patterns) | Fork zeron | The owner's ruling (D480) is a fork; D421 still holds for the harness repositories |
| **D431** (RFC 8693 exchange at the gateway) | Not available yet | Deferred to the unified auth plan (Q438); the Authentik token is the bearer until then |
| **§30 D294** (a desktop install receipt that `self-update` refuses) | Desktop no longer bundles into a Tauri app | Still needed if the `loams` binary is bundled (Q428, Q488) |
| **D284 and Q433** (no telemetry) | | Consistent: zeron's Rust crates have none |
| **D220** (open for adoption; commercial in `loam-platform`) | An MIT fork with Apache-2.0 additions | Consistent: both are open; nothing here depends on `loam-platform` |

### 18.14 The Tauri web bridge (D500–D512)

Status: **Proposed** · 2026-10-02. The direction is the owner's, given the same day, after the native ruling: **"do not drop Tauri; add it as a bridge to control websites, efficiently, like the Chrome MCP toolbox."** This subsection adds decisions **D500–D512** and questions **Q500–Q511**. The D480–D499 block is full, so the numbers continue at D500 (renumber at merge if another branch took them). They are recorded in the [decision log](13-decision-log.md). The plan is [AP1b](../plans/2026-10-02-ap1b-tauri-web-bridge.md). Markers are as in §18: **(verified 2026-10-02)** means read in the named source that day, **(verify)** means the plan task that builds it checks it first, **(estimate)** means computed, not measured.

**What changes and what does not.** D480 stands: the desktop is the native zeron fork, and Tauri does not come back as the desktop shell, so D429–D432 and D439 stay superseded. Tauri 2 returns in a **different role**: a separate **web bridge** process, a managed webview host that opens and keeps sessions for websites and that agents drive as MCP tools. It also answers the two gaps §18.5 found in zeron's sidebar browser: none on Windows, and no persistent store on Linux.

#### 18.14.1 Role and shape (D500–D502)

```
  agents: Loams Bot (ACP shim)  ·  Claude Code, Codex, Cursor … in zeron  ·  any MCP client
        │ MCP over stdio                                    │ (never a Loams token; a scoped session handle)
        ▼                                                   │
  `loams-web-bridge mcp`  (thin stdio shim, one per agent session)
        │ local IPC: Unix socket 0600 / Windows named pipe with an owner-only ACL
        ▼
  ┌──────────────── loams-web-bridge daemon (Tauri 2, one per user) ────────────────────────────┐
  │ MCP tool registry · policy and approvals · redaction · audit · profile manager                │
  │ windows: one webview per page, one data store per profile (WebView2 · WebKitGTK · WKWebView)  │
  │ injected script (isolated world) ⇄ eval · native hooks per OS · CDP in-process on Windows     │
  └───────▲──────────────────────────────────────────────▲───────────────────────────────────────┘
          │ launch, supervise, approvals                  │ OTLP (content-free; content to Langfuse only, §39 D-SF-11)
   Loams Desktop (zeron fork, loams-link)         collector → OpenObserve, Loams, Langfuse
```

- **D500, the role.** Loams Desktop stays native. **`loams-web-bridge`** is a separate Tauri 2 application whose job is to host webviews for websites (Zulip, Plane, Forgejo, GlitchTip, Langfuse, OpenObserve, any site) with persistent, isolated sessions, and to expose them as tools. A person can also use its windows by hand ("Open in Loams Web", §18.14.6).
- **D501, the repository.** Its own repository, **`ostrium-labs/loams-web-bridge`**, Apache-2.0, not a fork, with its own Cargo workspace. Reasons: Tauri's dependency tree and per-OS webview libraries (`webkit2gtk-4.1`, WebView2, WKWebView) must stay out of the engine's `Cargo.lock` (the reason D439 gave for a separate workspace); the three-OS CI and signing are heavy and independent of the engine; and any MCP client may use it without Loams Desktop. It vendors the few protos it needs by pinned ref, as `loams-desktop` does (D493). Q500 asks whether to keep it in the monorepo instead.
- **D502, processes and transport.** A **daemon** (single instance per user, a lock file) owns the webviews, profiles and policy. Agents never speak to it directly: each agent session spawns the thin stdio shim `loams-web-bridge mcp`, which proxies MCP over **local IPC** to the daemon, exactly as zeron's `zeron mcp` proxies to the engine (§18.1). Callers are identified by a **session handle** minted by Loams Desktop, or, for a standalone client with no Loams Desktop, by the user running `loams-web-bridge handle new --profile … --class … --origin … --ttl …` over the owner-only control channel (the same endpoint Desktop uses, authenticated by OS user identity), which prints the handle for the client's MCP configuration. A handle is random, scoped to a chat, a profile set, tool classes and an expiry, revocable, and passed in the shim's environment. **A handle is not a Loams token** and opens nothing off the machine, so D489 stands. A loopback Streamable-HTTP listener for clients that cannot spawn a process is off by default and needs a per-launch bearer.

#### 18.14.2 The toolbox (D503, D504)

Studied on 2026-10-02: **`ChromeDevTools/chrome-devtools-mcp`** (Apache-2.0 **(verified)**, about 53 k stars, 59 tools in `docs/tool-reference.md`, plus a three-tool `--slim` mode) and **`microsoft/playwright-mcp`** (Apache-2.0 **(verified)**, about 38 k stars, 72 `browser_*` tools, of which storage, routing, tracing and video sit behind capability flags). Their lists overlap on the core, which is the part Loams takes.

| Group | Loams tool (v1) | Modelled on | Notes |
|---|---|---|---|
| Pages | `list_pages`, `new_page`, `select_page`, `close_page`, `navigate_page` (url, back, forward, reload) | both | A page belongs to a profile; ids are small integers |
| **Observe** | **`take_snapshot`** (uids; `verbose`, `depth`, scope by uid, `filename`), **`find`** (text or regex over the snapshot), `take_screenshot` (on demand, a file path back), `list_console_messages` and `get_console_message`, `list_network_requests` and `get_network_request` (paged, redacted) | `take_snapshot`, `take_screenshot`, console and network tools of chrome-devtools-mcp; `browser_snapshot` and `browser_find` of Playwright MCP | Snapshot first; screenshots are never the default path |
| Act | `click`, `fill`, `fill_form`, `select_option`, `hover`, `press_key`, `type_text`, `drag`, `upload_file`, `handle_dialog` | both | By **uid** from the latest snapshot; `fill` also accepts a `secret_ref` (D507) |
| Wait | `wait_for` (text, selector, url, network idle, with a timeout) | both | Returns a short result, not a snapshot |
| Downloads | `list_downloads` (name, size, path) | Playwright MCP's download handling | Files stay on disk; the tool returns paths and hashes (D508 gate) |
| Script | `evaluate_script` | both | **Off by default**, dangerous class, org policy to enable (Q511) |
| Sessions | `list_profiles`, `profile_status` (signed in or not, never cookie values), **`request_human`** (shows the window and waits until the person finishes a login, a captcha or a second factor) | Loams | Replaces cookie and storage tools |
| Left out of v1 | performance traces, heap snapshots, Lighthouse, CSS inspection, extensions, PWA, WebMCP, emulation, video and recording, **cookie and storage get and set**, request routing | the rest of both lists | Not needed for operating apps; cookie and storage access would hand credentials to the model |

Capabilities group the tools as Playwright's `--caps` does: `core` (default, no script, no network detail), `network`, `script`, and a **slim** set (`navigate_page`, `take_snapshot`, `click`, `fill`) for small models, as chrome-devtools-mcp's `--slim` does with three tools.

**Efficiency rules (D504).** These are the token-saving tricks of the two projects, plus two of ours. Cited to their sources: chrome-devtools-mcp's `docs/design-principles.md` ("Token-optimized: return semantic summaries", "Reference over value: for heavy assets return a file path"), its `take_snapshot` with `verbose` and uids of the form `<snapshotId>_<n>` (`src/TextSnapshot.ts`), its `pageSize` and `pageIdx` on list tools; Playwright MCP's README ("accessibility snapshot … better than screenshot", `--snapshot-mode`, `depth`, `filename`, `browser_find`).

1. **Snapshot, not screenshot.** A compact accessibility-style text tree (role, name, state, uid) is the default way to see a page. A screenshot is an explicit tool call and returns a file path.
2. **Uids carry their snapshot id** (`s12_34`). A uid from an older snapshot returns a **self-healing error** ("snapshot s12 is stale; call `take_snapshot`"), never a click on the wrong element.
3. **Actions answer small.** After `click` or `fill` the result is a one-line outcome plus a **change summary** (navigated to, dialog opened, N nodes added or removed, a download started), with `snapshot: "none" | "diff" | "full"` per call and a server default of `diff`. Playwright MCP returns a full snapshot after every action by default (`--snapshot-mode full`); that is the main cost Loams avoids.
4. **Search before dumping.** `find` returns matching nodes with a few lines of context, as Playwright MCP's `browser_find` does, so an agent asks "the Save button" and does not read the page.
5. **Scope and depth.** `take_snapshot` takes a uid to scope to and a `depth`; the default prunes ignored and presentational nodes, collapses single-child chains, truncates text at 200 characters, and folds repeated siblings ("… 37 more similar rows").
6. **Paginate lists.** Console and network tools take `pageSize` and `pageIdx` and default to short, summarised rows; bodies come from `get_network_request` by id.
7. **Reference over value.** Screenshots, downloads, large snapshots (`filename`) and network bodies go to the profile's session directory and return a path.
8. **A token budget per result** (default 6 000 tokens, **an estimate** to be tuned in AP1b Task 11), with a clear truncation marker and the call to continue.
9. **Errors say what to do next** (design principle "self-healing errors").

None of this is measured yet. AP1b Task 11 benchmarks snapshot size and steps per task against both projects on the pages of the apps in §39.

#### 18.14.3 Mechanism per platform (D505)

What Tauri 2.12.1 exposes **(verified in `tauri-apps/tauri` at 2.12.1, 2026-09-30)**: `WebviewBuilder::initialization_script` and `initialization_script_for_all_frames` (run at document start, on every navigation); `Webview::eval` and `eval_with_callback` (the result comes back as a JSON string; a thrown exception is ignored on Windows, so scripts wrap their own `try`); `data_directory` (Windows and Linux), `data_store_identifier` (macOS 14 and later, 16 bytes), `incognito`, `proxy_url`; `Webview::cookies`, `cookies_for_url`, `set_cookie` (including HTTP-only cookies, http and https URLs only) and `clear_all_browsing_data`; `on_navigation`, `on_new_window`, `on_download`, `on_page_load`; `additional_browser_args` (Windows only); and `with_webview`, which hands over the platform's native webview handle. Tauri's own end-to-end tests drive Windows through `msedgedriver` and `--remote-debugging-port`, Linux through `WebKitWebDriver`, and macOS through a commercial CrabNebula driver and plugin, which Loams does not use.

| Layer | Windows (WebView2) | Linux (WebKitGTK) | macOS (WKWebView) |
|---|---|---|---|
| **Uniform: injected script and `eval_with_callback`** | yes | yes | yes |
| Isolated script world (so a page cannot tamper with the bridge's script) | CDP `Page.createIsolatedWorld` **(verify)** | `WebKitScriptWorld` through `with_webview` **(verify)** | `WKContentWorld` through `with_webview` **(verify)** |
| Accessibility tree | **CDP `Accessibility.getFullAXTree`**, in process | DOM-derived (below) | DOM-derived |
| Network and console | **CDP `Network` and `Runtime` events**, in process | injected `fetch`, XHR and console hooks, plus WebKitGTK resource-load signals **(verify)** | injected hooks, plus `WKNavigationDelegate` callbacks **(verify)** |
| Dialogs, downloads, permissions | CDP and WebView2 events | WebKitGTK signals through `with_webview` | `WKUIDelegate` and `WKDownloadDelegate` |
| Cookies (internal, `profile_status`) | Tauri `cookies` | Tauri `cookies` | Tauri `cookies` |
| Per-profile persistent store | `data_directory` | `data_directory` | `data_store_identifier` on 14 and later; **earlier: ephemeral profiles only** (Q502) |

- **Uniform layer.** The **accessibility-style snapshot is computed by an injected script from the DOM and ARIA attributes**, the way Playwright builds its aria snapshot in `packages/injected/src/ariaSnapshot.ts` (Apache-2.0 **(verified)**), which Loams may port with attribution (Q510). Results return by `eval_with_callback`; there is no page-to-Rust channel.
- **No Tauri IPC for remote pages.** The webviews load remote origins and are granted **no capability**: `window.__TAURI__` and `invoke` are unreachable from a website, so a page cannot call the bridge or the host. Control is Rust-initiated (eval and native callbacks) only. This is the property D430 had to engineer; here it is the default.
- **CDP, where the engine has it.** On Windows the bridge uses **in-process CDP** through the WebView2 handle (`CallDevToolsProtocolMethod` and the event receivers, reached through `with_webview` **(verify)**), which gives the real AX tree, network and console without opening a port. WebKit has no CDP; its inspector protocol is different and its remote inspector is a human tool, so Linux and macOS use the injected layer plus native hooks.
- **Remote debugging ports.** **No listening debug port in the default build.** A development flag (Windows only, loopback, off by default, a warning in the title) may expose WebView2's `--remote-debugging-port` so that `chrome-devtools-mcp --browser-url` can attach, but CDP has no authentication and any local process could then drive a signed-in session, so it is never used in production (Q504).
- **Degradations, stated.** On Linux and macOS the network list covers what the hooks and signals see, not every sub-resource; there is no heap, trace or Lighthouse tooling; and the accessibility names are an approximation of the browser's. Q509 asks whether that is acceptable.

#### 18.14.4 Security (D506–D510)

**Profiles (D506).** One **profile per site or tenant**, named `<environment>/<site>` (for example `acme-staging/zulip`). Each is a separate persistent store, so signing in to Authentik at one app does not leak into another, and an agent granted `acme-staging/plane` cannot read `acme-staging/zulip`. Persistence is what makes Authentik single sign-on last across restarts (the gap in zeron's Linux browser). Ephemeral profiles (`incognito`) exist for one-off visits. Clearing a store is a human-only action.

**Secrets (D507, the §30 D288 rule applied to the web).** No credential reaches an agent-visible output.
- `fill` takes a **`secret_ref`** (for example `loams:acme-staging/zulip#password`) that the bridge resolves from the OS keychain or the credential broker (§39 §6); the value never appears in a tool argument, result, snapshot, error or log.
- Snapshots **elide the values** of password, one-time-code and payment fields and of anything with an `autocomplete` credential token; network results **redact** `Authorization`, `Cookie` and `Set-Cookie` and the bodies of authentication endpoints; console text is scrubbed for token-shaped strings.
- **No cookie or storage tools** exist (Playwright MCP's `browser_cookie_*` and `browser_localstorage_*` are deliberately not taken).
- A login, a captcha or a second factor is **handed to the person**: `request_human` shows the window, the tool blocks, and the person finishes. The bridge never types a human secret on the model's behalf.

**Approvals and policy (D508).** Every tool has a class: **read** (list, snapshot, find, screenshot, console, network), **interact** (click, fill without submit, hover, select, press, type, wait), **commit** (a form submit, a non-GET request the page makes right after an agent action, `upload_file`, a download; **and any `click`, `press_key` or `fill_form` that may cause one, which is classified commit before it runs**: a click on a submit control, a control inside a form, a link or button the snapshot marks as submitting, or Enter in a form field, so approval is asked **before** the action starts and a request is never released late), and **dangerous** (`evaluate_script`, navigating off the allowlist, enabling a capability). Policy maps (agent, tool class, origin) to **allow**, **approve** or **deny**; **commit** and **dangerous** default to **approve**, as the factory policy of §39 §8 (D-SF-9) does for destructive skills. An approval is a Loams approval (§19, §21) raised through Loams Desktop and decided by a person with the proof of D435, **never auto-approved and never decided by the agent** (D497). The **origin allowlist** defaults to the origins of the environment's apps (from the instance and collab configuration); off-list navigation is denied unless approved. The origin check is enforced at `on_navigation`, `on_new_window` and the request hooks, and again at the tool; like Playwright's own `--allowed-origins`, it is a guard against mistakes, **not a security boundary against a hostile page**, which is why the controls below exist. **(verify)** how reliably a form submit can be intercepted before it leaves on each engine; the fallback is gating the click or key that causes it.

**Untrusted content (D509).** Everything derived from a page (text, names, console lines, network rows) is **untrusted data**. Results wrap it in a marker the harness and the policy can see (escaped so that page text cannot close it), the bridge never follows instructions found in a page, and Loams Bot's rule stands that an approval or a question it did not raise cannot be answered by page content (§39 D-SF-9). Pages cannot call the bridge (no IPC), cannot read other profiles, and run in webviews with the user's permissions prompts routed to the bridge window, not auto-granted.

**Audit and observability (D510).** Every tool call emits an OpenTelemetry span (`loams.web.tool`) with the tool, class, profile, **origin (path and query redacted)**, agent principal, chat or run id, approval id, snapshot id, result size and outcome, joined to the agent's trace by the W3C `traceparent` that the shim receives. Spans go to the collector of §39 §9.1: **content-free** to OpenObserve and Loams' own ingest; snapshots and page text **only to Langfuse 4**, under the masking and retention policy of §39 §9.2 (Q476). A **local hash-chained JSONL audit log** in the profile directory (0600) survives a collector outage. The §39 D-SF-14 kill switch maps to a **pause-all** command that closes sessions' handles and blocks new calls.

#### 18.14.5 Platform and licence notes

| Item | Windows | Linux | macOS |
|---|---|---|---|
| Engine | WebView2 (Evergreen runtime) | WebKitGTK 4.1 (`libwebkit2gtk-4.1`) | WKWebView |
| Passkeys and WebAuthn in the bridge windows | **(verify)** | not expected (WebKitGTK) | needs an entitlement for arbitrary sites **(verify)** |
| Per-profile stores | yes | yes | 14 and later |
| In-process CDP | yes | no | no |
| Signing | Authenticode (Q421) | none | Developer ID and notarization (Q420) |

Licences **(verified 2026-10-02)**: Tauri 2 and `wry`: Apache-2.0 or MIT; chrome-devtools-mcp, Playwright MCP and Playwright (the source of the aria snapshot): Apache-2.0. The bridge **does not ship either MCP server**: it reimplements the tool contract in Rust, and ports only the injected snapshot code, with attribution in `NOTICE`. This also avoids chrome-devtools-mcp's usage statistics, which are **on by default** (`--no-usage-statistics`), in conflict with D284.

#### 18.14.6 Integration with Loams Desktop and the Factory (D511, D512)

- **Launch and supervision (D511).** `loams-link` gains a `bridge` module: start the daemon on first use (`loams-web-bridge daemon`), health-check it over IPC, restart it with the policy of §18.3 (backoff, five tries in ten minutes), stop it on request. Version skew is a handshake (`bridge_api_version`), not a shared binary.
- **Agents.** Loams Bot's ACP shim gets the bridge through ACP `mcpServers` in `session/new` (zeron's engine already passes MCP servers to ACP agents); coding agents in zeron get it through the same stamping that adds `zeron mcp`, or through `loams mcp install` (§30 D290). Each gets its own handle.
- **Windows.** Bridge windows are **separate native windows** with a persistent banner ("controlled by Loams Bot", a Pause and a Stop button) and a taskbar entry. Embedding them into zeron's window (an `NSView`, an `HWND` or an X11 reparent) is deferred (Q508). Background profiles may run with the window minimized or hidden; some sites throttle hidden pages, so the bridge keeps pages visible but off-screen where it must.
- **Open in Loams Web.** The "Open in browser" action of §18.5's panels gains a second choice: open the app in a bridge window of its profile, with a persistent Authentik session on Windows and Linux too. The system browser stays the default where passkeys matter.
- **Relation to SF1 (collab panels).** Panels are API-driven and remain the primary view of Zulip, Plane, Forgejo and GlitchTip objects (§39 §3.2). The bridge is the fallback for what an app's API does not offer.
- **Relation to SF2 (agents).** The platform agents are **server-side** services and cannot reach a person's desktop. Two ways to give them web reach when an API lacks a feature are open (Q507): a **client-tool relay**, where Loams Bot on the server asks the person's Loams Desktop to run a bridge tool through `loams.bot.v1` and returns the result (the desktop executes, the server orchestrates), or a **headless bridge** on the factory host (a Playwright MCP container is the buy option, Apache-2.0). The first keeps credentials on the user's machine; the second suits unattended runs. Desktop-local agents use the bridge directly either way.
- **Q507 recommended default, 2026-10-02 ([§42](42-cloudflare-2026-betas.md) §4, D565 to D567).** Credentialed work uses the client-tool relay; unattended work (the public web, or an app reached with a service account) uses a `remote` provider with the same tool contract, implemented over Browser Run's CDP and Playwright endpoints (Kitesurf is optional: free beta, closed source, no licence yet). A remote browser is a third party, so it takes no user-credential `secret_ref` fills and keeps no persistent profile by default. WebMCP (a W3C Community Group draft, `document.modelContext`) joins the toolbox in v2 as `list_webmcp_tools` and `call_webmcp_tool` (D570); §18.14.2's "left out of v1: WebMCP" stands for v1.
- **D512, buy over build.** The toolbox is a small contract, so Loams builds it; the browser is the platform's. No Node runtime ships. Playwright MCP stays the choice for headless, server-side and CI automation.

#### 18.14.7 Risks

| # | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| B1 | **Prompt injection through page content** steers an agent | High | High | Untrusted-data marking, policy classes, approvals that the agent cannot decide, no secrets in output, profile isolation (D507–D509) |
| B2 | **A hostile page tampers with the injected script** in the page's world | Medium | Medium | Isolated worlds where the OS offers them, frozen closures and a per-session property name otherwise, and treating every result as untrusted (Q505) |
| B3 | **WebKit parity gaps** (no CDP, weaker network and a11y data) make tools behave differently per OS | High | Medium | A capability matrix in `list_tools` metadata; the same tool contract, honest degradation notes; conformance tests per OS |
| B4 | **macOS before 14 has no per-profile store** | Medium | Low | Only ephemeral profiles there (no shared store, so no cross-tenant access), or require 14 (Q502); the minimum macOS is already a question (Q431) |
| B5 | **Passkeys do not work in an embedded webview** on some OSes | Medium | Medium | `request_human` falls back to the system browser for sign-in; passkey results from the spike (AP1b Task 0) |
| B6 | **Submit interception is unreliable**, so a commit slips through unapproved | Medium | High | Gate at the tool as well as the page; default commit-class approvals; tests with real forms on every OS |
| B7 | **Token cost** of snapshots on heavy apps | Medium | Medium | The rules of D504, `find`, scoping, budgets, and a benchmark gate (AP1b Task 11) |
| B8 | **A second Tauri codebase** to maintain beside the native app | Medium | Low | Small, separate repository; no UI beyond a banner; Tauri's stable 2.x API |

Sources read for this subsection (2026-10-02): `ChromeDevTools/chrome-devtools-mcp` (`README.md`, `docs/{tool-reference,slim-tool-reference,design-principles,configuration}.md`, `src/TextSnapshot.ts`, `src/formatters/SnapshotFormatter.ts`); `microsoft/playwright-mcp` (`README.md`); `microsoft/playwright` (`packages/injected/src` listing); `tauri-apps/tauri` at 2.12.1 (`crates/tauri/src/webview/mod.rs`, `packages/api-e2e/README.md`); GitHub licence and star metadata for all of them. Platform behaviours marked **(verify)** are not yet tested.

### 18.15 Sources

Read on 2026-10-02. **Zeron** (`github.com/zeronsh/zeron`, `80b946b`, 2026-10-01): `README.md`, `ARCHITECTURE.md`, `CONTEXT.md`, `docs/mcp.md`, `docs/reference/{linux-browser,windows-development}.md`, `docs/PARITY.md`, `Cargo.toml`, `apps/zeron/src/{main,update_cli,auth_cli}.rs`, `crates/engine/src/{auth,registry,harness_updates}.rs`, `crates/harness/src/acp/{mod,normalize}.rs`, `crates/update/src/lib.rs`, `crates/ui/src/{lib,browser/mod,icons,pickers}.rs`, `crates/ui/Cargo.toml`, `.github/workflows/{release,windows,ui-tests}.yml`, `.github/actions/*`, `dist/`, `LICENSE`, `THIRD_PARTY_NOTICES.md`; GitHub metadata (stars, contributors, release and commit counts). **Loams:** §19, §30, §37 (this document), §38, §39 and its pending log (branch `software-factory-design`, PR #192), the AP0 branch's `proto/loams/{instance,errors}/v1` (commit `bc3e559`, not yet on `main`) and `loams-apps-mock`, `docs/open-core.md`. **Libraries:** connect-rust 0.9.1 (`connectrpc`, `connectrpc-codegen`), buffa 0.9.2, `keyring` 4.2.0, `gpui-wry` 0.7.0 (crates.io metadata only). **Protocols:** RFC 7636, RFC 8252, RFC 8693, A2A 1.0 as recorded in §39 §5.
