# 37 — Loams Desktop and Mobile Apps: a cordis Console, a Tauri Shell, Native Phones

Status: **Proposed** · 2026-10-01, revised 2026-10-02 (the Authentik and D220 rulings). The direction is the owner's, given on 2026-10-01 in two messages:

1. "I downloaded the DeepSeek Harness desktop and mobile app repos. They serve as the base for the Loams desktop app and mobile app, with Connect-RPC. The harness desktop's Rust backend is Tauri, so adapt our control-plane React to Tauri. Mobile is Kotlin, so use Connect-RPC natively in Swift (iOS) and Jetpack Compose (Android)."
2. The same day's correction: "cordis" is the JavaScript meta-framework the harness is built on (contexts, services, a plugin lifecycle with scoped disposal and hot reload), not Tauri. The intent is to **adapt Loams's control-plane React to cordis so that any code can be loaded as a plugin**: console pages, panels, engine adapters, connectors, agent tools, and the integrations of §26, §30, §32–§34, each a cordis plugin with declared services and dependencies, loaded from a catalog like the harness's `cordis.yml`, in the browser and inside Tauri. **Tauri stays the desktop shell; cordis is the application architecture inside it.** "Native Connect-RPC" on mobile means connect-swift and connect-kotlin generated from the shared protos, with no web view or bridge, in native SwiftUI and Compose.

The owner's standing rulings that apply: Connect-RPC everywhere (connect-es, connect-swift, connect-kotlin, all from the protos connect-rust serves, D128); the package namespace `loams` (crates.io, PyPI, npm `@loams`), Go paths `loams.dev/...`, the domain `loams.dev`, CloudEvents types `io.loams.dev.*`; the repository moving to the GitHub organisation `ostrium-labs`; mobile native per platform, not Kotlin Multiplatform UI. Two further rulings arrived while this document was written (2026-10-01 and 2026-10-02): **the identity provider is Authentik, open-source edition only** (Clerk and Keycloak are gone), so every app sign-in flow targets Authentik (§6.5, §7.2); and **D220's open-core split stands**: the console's multi-tenant, hosted and billing parts stay in the private `loam-cloud` and `loam-platform` repositories, loaded as private plugins from a private registry. This document designs only the open side and names the extension points the private side uses (§5.8); it contains no design for hosted or billing plugins.

This document turns that direction into decisions **D420–D439** and open questions **Q420–Q439**. Every choice beyond the direction (formats, trust tiers, flows, phasing) is a **proposal** until the owner confirms it. **No code is written by this document.** The plans are [AP0](../plans/2026-10-01-ap0-app-protos.md), [AP1a](../plans/2026-10-01-ap1a-cordis-console.md), [AP1](../plans/2026-10-01-ap1-desktop-tauri.md), [AP2](../plans/2026-10-01-ap2-android-compose.md) and [AP3](../plans/2026-10-01-ap3-ios-swiftui.md).

Markers: **(verified 2026-10-01)** means checked against a primary source on that date (§17). **(verify)** means the plan that builds it checks it first. **(estimate)** means computed, not measured. Paths of the form `harness-desktop/…` point into `dina-kar/deepseek-harness-desktop` at `2d1b505` (2026-08-15); `harness-mobile/…` into `dina-kar/deepseek-harness-mobile` at `68b6c2f`.

**Naming.** This document writes `loams` for the CLI binary and `@loams/*` for npm packages, following the owner's namespace rulings (D400, D401; the binary `loams` answers §30's Q284). The rename PR (D407) moved the code to these names (`@loams/console`, `@loams/ui`).

---

## 1. Summary

| # | Decision | Status |
|---|---|---|
| D420 | **Three apps, one contract.** Loams Desktop (Tauri 2, macOS, Linux, Windows), Loams for iOS (SwiftUI) and Loams for Android (Jetpack Compose). Every application call from an app to Loams is Connect-RPC from the protos connect-rust serves, through connect-es, connect-swift and connect-kotlin; the named exceptions are the console's OpenAPI `/api/v1` (REST until Q423), sign-in at Authentik and the Loams gateway's OAuth token endpoint (OIDC and OAuth over HTTP), the instance-to-gateway push API and APNs/FCM, the desktop's local CLI JSON contract (D283), and the updater's manifests (§2, §8) | Proposed |
| D421 | **Borrow the harness repos' patterns; fork neither.** Both are MIT. The desktop's Rust host is about 160 lines and its value is the pattern; the mobile app talks a different protocol to a different server. Nothing is copied by default, so no attribution is owed; any copied file keeps its MIT notice in `THIRD_PARTY_NOTICES.md`. cordis itself is a direct MIT dependency (§3) | Proposed |
| D422 | **The console becomes a cordis v4 application**, in the browser (served by the engine at `/ui`, §19 P1) and inside Tauri. A small host boots a cordis `Context` and the cordis loader; everything else (layout, pages, engine views, connector forms, approval renderers, the RPC clients themselves) is a plugin. cordis's client half is used; the harness's Node host half is replaced by the Rust engine and the Tauri host, and its Typert RPC by Connect (§5) | Proposed |
| D423 | **The plugin manifest and the catalog.** A plugin is an ESM package whose `package.json` has a `loams.plugin` block (kind, entry, `inject`, `provides`, slots, permissions, trust tier, API requirements, editions). The catalog is a cordis v4 entry list, `loams.yml`, composed from a base file and edition patch files exactly as the harness composes bundles and profiles. **No `!!js` in any catalog** (§5.3) | Proposed |
| D424 | **Service contracts.** Plugins cooperate only through cordis services, never by importing each other's values. The host provides `transport`, one `rpc.<service>` per proto service (provided only when `GetInstance.api_versions` lists it, so dependents activate by themselves), `api` (the OpenAPI console client), `session`, `platform`, `slots`, `router`, `settings`, `flags` and `i18n` (§5.4) | Proposed |
| D425 | **Surfaces are typed slots**, adapted from the harness's `ui-slots`: `single`, `list` and `keyed` slots declared by a `SlotMap`, registered through `ctx.slots.register(...)` inside an effect, each entry in its own error boundary, components never touching `ctx`. The first slot catalog is fixed in §5.5 | Proposed |
| D426 | **Trust tiers and isolation.** `core` and `first-party` plugins run in the console's realm behind a guard proxy that exposes only their injected services. **`third-party` plugins always run in a sandboxed iframe** (opaque origin, `connect-src 'none'`) with a capability-checked bridge, and their calls carry a **vended, attenuated token** (§19 §5.2 flow 3) whose scopes are the manifest's permissions intersected with the user's, with the plugin as the actor. The server enforces it and audits it. Third-party plugins are off until the unified auth plan (§5.6) | Proposed |
| D427 | **Plugin sources and reload.** Five sources: bundled, npm `@loams/*` at build time (with npm provenance from `ostrium-labs`), a **private registry** configured at build time (how the private Cloud plugins arrive, §5.8), installed on an instance by an org owner at run time (served by the engine with integrity hashes), and a local path in development. Development reload is Vite HMR plus a cordis fiber refresh; a production instance pushes catalog changes and the host disposes and loads fibers without a page reload (§5.7) | Proposed |
| D428 | **Editions are plugin sets** (D220, which stands): `oss` and `desktop` in this repository; the hosted set is private plugins from `loam-cloud` and `loam-platform`, built into a hosted console from a private registry with a private catalog patch. The open host exposes extension points for it (a registry source, trusted publishers, catalog patches, slots, `flags.edition`) and knows nothing else about it; this repository never depends on it (§5.8, §10) | Proposed |
| D429 | **The desktop shell.** One Tauri 2 window loads the bundled console with the `desktop` plugin set. **Its sidecar is the `loams` CLI binary**, bundled per target; stacks are created, started, stopped and described through the CLI's JSON contract (§30 D283, D285), so terminal and app share `LOAMS_HOME` and the same stacks. Stacks outlive the app. The app restarts `keep_running` stacks with backoff. "Add `loams` to PATH" writes a `desktop` install receipt, which `self-update` refuses (amends D294) (§6.2) | Proposed |
| D430 | **Desktop lockdown and the network bridge.** One local capability with an explicit command allowlist; no shell, fs, http or process permission; a CSP with no remote source. **Every console request goes through `net_fetch`**, a Rust command that streams the response over a Tauri `Channel` into a standard `Response`, adds the bearer token, strips JavaScript-set credentials, and only reaches the active environment's origins. Tokens never reach JavaScript (§6.3, §6.4) | Proposed |
| D431 | **Desktop sign-in against Authentik**: OIDC authorization code with PKCE in the system browser and a loopback redirect (RFC 8252 §7.3) at the instance's Authentik (open-source edition), public client `loams-desktop`; the Authentik token is exchanged at the Loams gateway (RFC 8693) for Loams's own access and refresh tokens (§19 §5.3), so Loams still decides environment and scopes. The refresh token in the OS keychain through `keyring`, the access token in Rust memory only. Not Stronghold, which is deprecated (§6.5) | Proposed |
| D432 | **Desktop updates, signing, platforms and deep links.** `tauri-plugin-updater` with static per-channel manifests and its own signing key, separate from the CLI's; Developer ID signing and notarization on macOS, Authenticode on Windows; macOS aarch64 and Linux with local stacks, Windows remote-only until a Windows server variant exists; `loams://` deep links are navigation only, parsed in Rust against an allowlist, forwarded by the single-instance plugin (§6.6, §6.7) | Proposed |
| D433 | **Mobile is native on each platform, with no Kotlin Multiplatform.** SwiftUI with connect-swift over URLSession, and Compose with connect-kotlin over OkHttp; both use the Connect protocol with the binary codec. The shared parts are the protos, golden fixtures (canonical decision bytes, pairing payloads, sealed notifications) and the conformance scenarios run against one mock (§7.1, §7.8) | Proposed |
| D434 | **Pairing maps the harness's relay and pinned-key pattern onto §19's identity model.** A phone is a **device credential of a user principal**, not a new principal kind. A signed-in user creates a short-lived pairing in the console or desktop; the phone scans a QR (v1) holding the Loams gateway's URL, the instance id, the TLS SPKI pin set and the **instance key thumbprint**, and redeems the pairing at the Loams gateway's token endpoint with an extension grant and a DPoP proof. The alternatives sign in at **Authentik** (PKCE in the system browser, or Authentik's device-code flow, RFC 8628, for a phone without a camera) and exchange the result at the gateway. Loams tokens are DPoP-bound to a hardware key. The pinned anchor is the instance's token-signing key (§19 §5.3), under which TLS pins rotate. No relay in track AP (§7.2) | Proposed |
| D435 | **Approvals are a first-class service** over §21 §6.5's approval promises: `loams.approvals.v1` with list, get, watch and decide. **A decision carries a proof** signed by a user-presence key (Secure Enclave or StrongBox behind biometrics) or comes from a session younger than 5 minutes; the requester (or the user an agent acts for) cannot approve; there are no offline or queued decisions and no "always allow" (§7.3) | Proposed |
| D436 | **Push is a sealed wake-up.** The engine projects `io.loams.dev.*` CloudEvents into a per-user inbox, seals each notification with HPKE to the device's key, and hands it to a **push gateway** that holds the APNs and FCM credentials and sees only ciphertext. The gateway, `loams-push`, is open source; Loams runs the instance the store apps use, and self-hosters with their own app builds run their own. Android also supports UnifiedPush (§7.4) | Proposed |
| D437 | **Offline and background behaviour.** Phones cache approvals, operations and the inbox with freshness timestamps and show stale data read-only; no background sockets; push, `WorkManager` and `BGAppRefreshTask` catch up; decisions are never queued (§7.5) | Proposed |
| D438 | **The app proto surface (AP0)**: new packages `loams.instance.v1`, `loams.devices.v1`, `loams.approvals.v1`, `loams.operations.v1`, `loams.notifications.v1` and `loams.errors.v1`. Rules: unary and server-streaming only; watch streams send a snapshot, then changes, then a heartbeat every 15 s, and resume from a cursor; idempotent reads are marked for HTTP GET; every mutation takes an idempotency key; errors carry a stable `reason`. Served first by `loams-apps-mock` (§8) | Proposed |
| D439 | **Repository layout and track AP.** Desktop and console in this repository (`web/apps/console`, `web/apps/desktop` with its own Cargo workspace, `web/plugins/*`, `web/packages/*`). Mobile in one repository, `ostrium-labs/loams-mobile` (`android/`, `ios/`), generating from a pinned ref of this repository's `proto/`. Plans: AP0 (protos and mock), AP1a (cordis console), AP1 (desktop), AP2 (Android), AP3 (iOS); AP4 (the server side) is not yet planned (§9, §13) | Proposed |

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
   ┌──────────── Tauri host (Rust, desktop only) ────────────┐   ┌──────── Loams instance (Rust) ────────────────┐
   │ cli: `loams … --output json` (D283) ── stacks (D285)    │   │ connect-rust: Connect + gRPC + gRPC-Web       │
   │ net: bridge, origin allowlist, bearer injection         │──▶│ loams.instance/devices/approvals/operations/   │
   │ auth: PKCE loopback, keychain (keyring)                 │   │   notifications.v1 (AP0, AP4) · loams.jobs.v1  │
   │ tray · single instance · deep links · updater · logs    │   │   (J1) · loams.live.v1 · /api/v1 (OpenAPI, P9) │
   └──────────────┬──────────────────────────────────────────┘   │ approvals = §21 approval promises             │
                  │ spawns via CLI (setsid, detached)            │ notifier: io.loams.dev.* → inbox → seal ──┐   │
                  ▼                                              └───────────────────────────────────────────┼───┘
         local stacks under ~/.loams/stacks/<name>                                                           │ sealed
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
{"v":1,"kind":"loams-pair","issuer":"https://loams.acme.example","instance_id":"01J9Z3…",
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

Package names are `loams.*`, matching `loams.live.v1` and `loams.stream.v1`: the rename PR moved every proto package from `loam.*` to `loams.*` before the first app release (Q422, answered by D407; Connect URL paths contain the package, so the move was cheap before a release and would break apps after one).

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

**Recommendation: desktop and console in this repository; both phone apps in one new repository, `ostrium-labs/loams-mobile`.**

```
ostrium-labs/loams (this repository)
├── proto/loams/{instance,devices,approvals,operations,notifications,errors}/v1/   (AP0)
├── crates/loams-apps-mock/                       (AP0; scenarios shared by every app)
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
| [AP1](../plans/2026-10-01-ap1-desktop-tauri.md) | Loams Desktop: the CLI bridge and stacks, lockdown, the network bridge, sign-in and keychain, approvals, pairing, deep links, packaging and updates | AP1a Task 3; CLI1 (stacks, D283); D33 and the transfer for publishing | Planned |
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
| 7 | A bundled `loams` binary makes the desktop large | Medium | Low | Q428: download on first run instead |
| 8 | The auth plan slips, leaving the apps mock-only | Medium | High | AP0–AP3 deliver everything except real-server use; AP4 is small once the auth plan exists |
| 9 | The Tauri `Channel` bridge is too slow for large exports | Low | Low | AP1 Task 0 measures; large downloads can go to a file through a separate command |
| 10 | macOS has no WebDriver for WKWebView, so desktop e2e is weaker there | High | Low | Linux and Windows e2e; a launch-and-screenshot check on macOS (verify) |

## 15. Open questions

| # | Question | Owner | Needed by |
|---|---|---|---|
| Q420 | Store and signing accounts: an Apple Developer Program membership and a Google Play developer account for the `ostrium-labs` entity (D-U-N-S number, legal name), and who holds the Developer ID and upload keys | Founder | AP1 Task 11, AP2 Task 10, AP3 Task 10 |
| Q421 | Windows code signing: Azure Artifact Signing (needs organisation validation), a Key Vault certificate, or unsigned betas | Founder | AP1 Task 11 |
| Q422 | ~~Rename the proto packages `loam.*` to `loams.*` in D33's rename PR, before any app is published?~~ Answered 2026-10-02 by the owner: yes, in the rename PR (D407) | Founder | Resolved |
| Q423 | Keep the console's OpenAPI `/api/v1` (§19 P9) for identity administration, or move it to Connect (`loams.console.v1`) so that every console call is an `rpc.*` service? Proposed: keep REST for M2 and revisit | Founder | AP1a Task 4 |
| Q424 | The official push gateway at `push.loams.dev`: free for every self-hosted instance using the store apps? Limits, a privacy policy, and instance registration | Founder | AP4 |
| Q425 | Reaching private instances from phones (a laptop stack, a self-hosted cluster behind a firewall): build an end-to-end relay, or document VPNs and tunnels only (proposed for track AP)? | Founder | After AP2/AP3 |
| Q426 | Third-party console plugins in OSS: allowed (proposed: yes, off by default, org owners install), and is npm provenance enough to call a package `first-party`, or only an `ostrium-labs` allowlist? | Founder | AP1a Task 6 |
| Q427 | cordis: depend on pinned npm `cordis@4.0.0-rc.10` with patches (proposed), vendor it now like the harness, or wait for 4.0? | Eng | AP1a Task 1 |
| Q428 | Bundle the `standard` `loams` binary in the desktop app (proposed) or download it on first run through the CLI's variant mechanism? | Eng | AP1 Task 2 |
| Q429 | On quit, leave stacks running (proposed) or stop the ones the app started? | Founder | AP1 Task 3 |
| Q430 | Linux packages: AppImage, deb and rpm (proposed), plus Flatpak or Snap? | Eng | AP1 Task 11 |
| Q431 | Minimum OS versions: iOS 17, Android 10 (API 29), macOS 13 (proposed) | Founder | AP2/AP3 Task 0 |
| Q432 | May a user approve an operation an agent requested on their behalf? Proposed: no by default, an org policy can allow it for non-protected environments | Founder | AP0 Task 3 |
| Q433 | Crash reporting in the apps: none (proposed, D284's no-telemetry rule) or opt-in Sentry, which `loam-cloud` already uses? | Founder | AP1, AP2, AP3 Task 10 |
| Q434 | App names on the stores ("Loams", "Loams for iOS") and a trademark check | Founder | Store releases |
| Q435 | App review: a bundled demo mode (proposed) or a hosted demo instance and account? | Founder | AP2/AP3 Task 10 |
| Q436 | Does Loams Cloud's console (today a Next.js app in `loam-cloud`, with Clerk, which the Authentik ruling retires) move onto the cordis host as private plugins from the private registry (§19 P1, proposed), or stay separate? | Founder | Before the cloud console's next phase |
| Q437 | Windows desktop: remote-only (proposed), stacks through WSL2, or a Windows server variant (§30 Q285)? | Founder | AP1 Task 11 |
| Q438 | Does the unified auth plan add, on the Loams gateway, the exchange of Authentik tokens for Loams tokens (RFC 8693), DPoP (RFC 9449) for user tokens issued to devices, device-bound rotating refresh tokens and the pairing extension grant? Which Authentik release provides the device-code flow and the step-up (`max_age`) the apps rely on? §19 §5.3 lists DPoP as a follow-up for agents only | Founder, Eng | The auth plan; AP4 |
| Q439 | Ship the Android `unifiedpush` flavor on F-Droid (reproducible builds), and when? | Founder | AP2 Task 10 |

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
| **D33** (`loamdb` packages) and the owner's 2026-10-01 `loams` ruling | npm `@loams/*`, the binary `loams` | This document follows the ruling, recorded as D400 (which amends D33) |
| **§30** (the binary, Q284; the npm scope, Q283) | The desktop's sidecar is `loams` | Consistent: Q284 and Q283 were answered on 2026-10-02 (D401, D400); §30 now writes `loams` |
| **§30 D294** (`self-update` refuses installs it did not make) | A desktop-provided binary on PATH | A new `install_method: "desktop"` that `self-update` refuses (D429) |
| **§30 D289** (no destructive MCP tools) | The desktop's stacks page | Consistent: creation and deletion stay CLI commands in AP1 |
| **D284** (no telemetry) | The apps | The apps send no telemetry by default (Q433) |
| **D128** (one protobuf toolchain: buffa and connect-rust on the server, buf with protobuf-es for clients) | Swift and Kotlin clients | Extended, not changed: the same `buf` inputs gain Swift and Kotlin templates |
| **§26 D206** (`loams.jobs.v1`, not on `main`) | Jobs screens | Feature-gated on `api_versions`; the apps ship before J1 |
| **§19 §3** (`@loams/console`, `@loams/ui`) | `@loams/*` plugins | Renamed by the rename PR (D407) with everything else |
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
- **Loams:** §19, §21 (§6.4, §6.5), §26 (D206, §6.6), `docs/open-core.md` (D220, D221), §30 and its pending log (branch `cli-design`), §32–§33 and their pending log (branch `flow-fabric-house-design`), §34 (branch `gateway-runtime-design`); `web/` on `main` at `9eaddae` (`apps/console/{vite.config.ts,src/main.tsx,src/api/client.ts,src/session.tsx,src/pages/auth.tsx}`, `packages/ui`), `api/console/openapi.json`, `crates/loams-console-mock`, `buf.yaml`, `buf.gen.yaml`, `proto/loams/live/v1/live.proto`, `crates/loams-stream-grpc/proto/loams/stream/v1/stream.proto`; `loam-cloud` at `c738de7` (`next.config.*`, `proxy.ts`, `app/(console)/`, `lib/console.ts`, `lib/integrations.ts`; read only).
