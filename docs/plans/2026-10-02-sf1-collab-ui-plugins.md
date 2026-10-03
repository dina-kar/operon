# SF1 — Collaboration App UIs as cordis Plugins (Zulip, Plane, Forgejo) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, flags, header values), use them verbatim. The code is not pre-written in this plan; the tests are the specification.

> **Status: In progress** (2026-10-02; Task 0 done, [spike notes](sf1-spike.md)). **Slot: track SF, first plan** (proposed; D-SF-1). Branches `sf1-t<N>`, stacked; **PRs target `dev`**. Depends on AP1a (the cordis console host and slots, for the browser console) being on `dev`; the **desktop** tasks (5b and the GPUI halves of 6 and 7) depend on the native desktop shell, a fork of zeron (§37, amended for a native desktop, D480–D499), and until it lands they build as standalone GPUI crates with a test harness. Tasks 0–2 depend on neither (a spike, a proto and edge configuration). Task 9 is the exit gate and needs Tasks 1–8: its browser-console checks need AP1a, its desktop checks run against the standalone GPUI crates until the fork lands, and its mobile check is an external prerequisite (the `loams-mobile` repository's own CI, pinned to this repository's ref, whose green run the exit report links). Mobile work (Task 8) depends on AP0's protos and AP2/AP3's app shells. **Phase 1 of D-SF-1: Zulip, Plane and Forgejo, in full, before any phase-2 app.**

**Goal:** Put Zulip, Plane and Forgejo into Loams in three ways, in the browser console, in the native desktop app and on phones, with no change to any app:
- **Full app UI.** Browser console: each app's own web UI in a sandboxed iframe, signed in by Authentik, framed through edge configuration only (design §3.3, §3.4). Desktop: the system browser (top-level, the person's Authentik session) and, if the spike allows, an optional in-app sidebar browser (design §3.4 tiers 2 and 3) — **Task 0 ruled no, see E1**.
- **Native panels** from `loams.collab.v1` (the typed read and write surface the loop uses): Zulip threads, Plane issues and cycles, Forgejo repositories, PRs and CI (§3.2, §3.6), with OpenFGA filtering, as cordis plugins in the browser console and native GPUI panels in the desktop app.
- **Mobile deep links and native views**: issue list and detail, PR list and checks, thread digest, plus `loams://app/…` links into the system browser (§3.4, §3.5).

**Architecture:**
- **`web/plugins/embed`** (`@loams/plugin-embed`): the `embed` service and the `embed.pane` slot for the **browser console**: the iframe host, the toolbar, the deep-link router.
- **`web/plugins/{zulip,plane,forgejo}`**: one plugin each, `first-party`, registering `console.page`, `app.panel`, `bot.card`, `embed.pane`, overview cards and palette commands (design §3.5).
- **`crates/loams-collab`**: the `loams.collab.v1` service (Connect, connect-rust), the app adapters (`ZulipApi`, `PlaneApi`, `ForgejoApi` traits with HTTP implementations and recorded-fixture fakes), the **credential broker** and the OpenFGA filter. SF2 reuses all three.
- **`crates/loams-apps-client`**: generated connect-rust clients for AP0's services and `loams.collab.v1` (later `loams.bot.v1` and `loams.factory.v1`), shared by the desktop fork and any Rust client.
- **Desktop (the zeron fork; the directory is named by §37's amendment, written `desktop/` below):** crate `loams-ui-collab` with GPUI panels and cards for the same objects, an `AppOpener` (registry-checked system-browser opens) and, spike-gated, `SidebarBrowser`.
- **`deploy/factory/edge/`**: the edge routes (headers, forward-auth hooks) as Envoy or Caddy snippets and Helm values, plus the CI harness that starts each pinned app image behind the edge.
- **`loams-mobile`** (separate repository): `Apps` screen, issue and PR views, deep-link handling.

**Tech Stack:** Rust 1.97.1, edition 2024, connect-rust and buffa (D128), `reqwest` (Apache-2.0 or MIT) for app APIs, `wiremock` for fakes, OpenFGA client from §22's `commons-control` work; TypeScript and cordis 4.0.0-rc.10 behind `@loams/cordis` (Q427), Vitest and Playwright for the browser console; GPUI at the revision the zeron fork pins, with GPUI's test context for views, and the `open` crate (MIT or Apache-2.0) for the system browser; `gpui-wry` or `wry` (MIT or Apache-2.0) only inside the spike (Task 0); SwiftUI with connect-swift, Compose with connect-kotlin. Docker Compose for the edge harness. No new native dependency in the Loams binary beyond `reqwest`.

**Spec:**
- [`docs/design/39-software-factory-and-loams-bot.md`](../design/39-software-factory-and-loams-bot.md): §3 (all), §4, §6.2, §7; D-SF-2–D-SF-5, D-SF-16, D-SF-17, D-SF-19.
- [`docs/design/37-desktop-and-mobile-apps.md`](../design/37-desktop-and-mobile-apps.md): §5 (slots, trust tiers, for the browser console), §7.4, §8.3; the desktop shell is **§37 as amended for a native desktop (D480–D499)**, not designed here. zeron (`github.com/zeronsh/zeron`, MIT): `ARCHITECTURE.md`, `crates/ui`.
- [`docs/design/22-showcase-suite.md`](../design/22-showcase-suite.md): §4 (licences), §5 (SSO per app), §7 (the OpenFGA model), §8.1.
- [`docs/design/38-knative-authentik-gitops.md`](../design/38-knative-authentik-gitops.md): Authentik, the edge.
- [`docs/plans/2026-10-01-ap1a-cordis-console.md`](2026-10-01-ap1a-cordis-console.md), [`…ap0-app-protos.md`](2026-10-01-ap0-app-protos.md).

## Global Constraints

Same as the AP plans, plus:
- **No app is modified, forked or patched** (D-SF-17). A needed behaviour is edge configuration, a panel or an adapter. A task that seems to need a patch stops and files an issue.
- **Embeds never hold a Loams token.** No Loams credential is passed to a frame, a webview, a URL or a spawned browser's command line. CI greps the plugin and desktop sources for `Authorization`, `Bearer` and `loams_` in embed and opener code paths.
- **The desktop opens only registry origins.** `AppOpener` refuses any URL whose origin is not in `ListApps` for the active environment, and never passes a token (SSO is the browser's).
- **A GPUI panel uses `loams-apps-client` only**; a lint (`cargo deny` bans plus a source grep) fails on an HTTP client aimed at an app origin from `loams-ui-collab`.
- **Native panels read through `loams.collab.v1` only.** A plugin never imports an app's API client. A lint (`eslint-plugin-boundaries` or a plain import test) fails on `fetch` of an app origin in `web/plugins/{zulip,plane,forgejo}`.
- **No app secret leaves the broker.** `Secret` has no `Serialize` and a redacted `Debug` (§30 D288); the canary test of Task 9 covers `loams-collab`.
- **Deep links navigate, never act** (D432). Parsed in Rust against an allowlist, and in Swift and Kotlin against the same golden table.
- **Pinned images by digest** in every compose file and chart; the digests are recorded in `deploy/factory/images.lock` with each app's licence and source URL (`LICENSES.md`, design §4).
- **The build machine.** One cargo build at a time, shared target; Docker harness runs one stack at a time; stop and report if `/home` has under 8 GB free.
- **Commit areas:** `collab`, `embed`, `web`, `desktop`, `edge`, `proto`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **Sibling subdomains of one domain** for the apps and the console (Q462's proposal) | Same-site cookies make framing work with `SameSite=Lax`; Plane, Forgejo and Zulip cannot all be served under a path prefix | A single-host install needs a wildcard DNS entry; `loams dev` uses `*.localhost` |
| 2 | **Desktop gets native panels plus the system browser; the sidebar browser is optional** (design §3.4) | zeron has no webview crate in its workspace, and the factory needs only the objects; the system browser gives SSO for free. **Corrected by Task 0:** zeron's `ui` crate *does* have a browser module (`crates/ui/src/browser`, macOS `wry`/WKWebView and a Linux WebKitGTK helper), but its website-data store is ephemeral on both and WebKitGTK has no WebAuthn, so tier 3 is impossible as it stands. See the execution rulings and [the spike](sf1-spike.md) | Users who want a docked Plane board wait for the spike's answer |
| 3 | **Native panels first for read, write only for comment, create and (gated) merge** | The loop needs these; every other write is the app's UI | Users ask for more; each is a small addition to `loams.collab.v1` |
| 4 | **Plane's API key is per agent identity, not per user** (Q463) | Plane CE has no per-user OAuth for third-party callers **(verify, Task 4)** | Audit in Plane names the agent, not the person; Loams' audit holds the person |
| 5 | **`loams.collab.v1` mirrors A2A's `Part` and artifact kinds where it can** | SF2 and SF3 render the same objects in chat | A little protobuf duplication until Q467 settles importing A2A's proto |

## Rulings made during execution

Task 0's findings, which win over the plan above where they disagree. Full evidence in
[the spike notes](sf1-spike.md).

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| E1 | ~~**The sidebar browser (tier 3) does not ship in SF1, on any OS.** The desktop is tiers 1 and 2 only: native panels and `AppOpener`~~ **SUPERSEDED by [D620](../design/13-decision-log.md) (2026-10-03): it ships, on Obscura over CDP, with no webview embedded.** The evidence in the why column is still true of zeron's browser; it is not true of the shipped sidebar | zeron's browser uses an ephemeral website-data store on both platforms (`macos.rs:41-45` `nonPersistentDataStore`, `linux/helper.c:619` `webkit_web_context_new_ephemeral`), so a sign-in never survives a launch, and WebKitGTK has no WebAuthn at all (WebKit bug 205350), so an Authentik passkey ceremony is impossible on Linux. There is no Windows browser at all | Users who want a docked Plane board wait for AP1n Task 7 (Q482) and D491, which send the work upstream |
| E2 | ~~**Task 5b's `SidebarBrowser` keeps its `sidebar-browser` feature but loses the per-(environment, app) data store requirement** until E1's upstream work lands; the feature builds only on the system browser fallback~~ **SUPERSEDED by D620: the `sidebar-browser` feature is on by default and the per-(environment, app) persistent store ships with it**, in `crates/loams-sidebar-browser` | A per-app persistent store is not reachable from zeron's browser without forking it, and the plan's "no native dependency beyond `reqwest`" rule argues against embedding `wry` here | The feature flag ships dead; dropping it later is a one-line change |
| E3 | **Plane CE needs no `UNIMPLEMENTED` fallback for cycles or modules.** Both are in the CE REST API v1 (`apps/api/plane/api/urls/cycle.py`, `module.py`), so `GetCycle` is a real call and Task 4's `plane_cycle_missing_is_unimplemented_with_reason` becomes a capability-probe test, not a known-unimplemented path | Task 4's contingency was written before CE was read | If a pinned CE image lags this, the probe still returns `APP_CAPABILITY` |
| E4 | **Plane CE's API key is a personal access token bound to a Plane *account*** (`APIToken.user`, `X-Api-Key`, expirable), with no OAuth issuer for third-party callers, so Ruling 4's "per agent identity" is realised as a dedicated Plane service account for the agent | `apps/api/plane/api/middleware/api_authentication.py` | Plane's audit names the agent's account; Loams' audit holds the person, exactly as Ruling 4 accepted |
| E5 | **Every app must lose `X-Frame-Options` at the edge: Zulip `DENY` (nginx), Plane `DENY` (Django's `XFrameOptionsMiddleware` default), Forgejo `SAMEORIGIN`. None of the three sends a `Content-Security-Policy`, so the edge appends one.** No `SameSite=None` anywhere, and none may be added | Source reads recorded in the spike; the edge-model check passes for all three | Nothing, if the Task 2 harness confirms |
| E6 | **Zulip's `__Host-` cookie prefix forbids any `Domain` attribute**, so the apps keep host-only sessions and the edge must not try to share one cookie domain across the subdomains | `zproject/computed_settings.py:485-486` | Sign-in per app per host, which is what the registry wants anyway |
| E7 | **The app embed frame is a different frame class from the console host's plugin frame.** `web/packages/console-host/src/sandbox.ts` pins `SANDBOX_FLAGS = 'allow-scripts'` and says "never `allow-same-origin`, `allow-top-navigation` or `allow-popups`"; Task 5's embed frame needs `allow-same-origin` (or the app session never applies) and `allow-top-navigation-by-user-activation` (the Authentik redirect). Task 5 must define its own allowlist, in `web/plugins/embed`, and must not reuse `SANDBOX_FLAGS` | The two frames have different jobs: one holds untrusted third-party code, the other holds a trusted app on a registry origin | A frame that can't complete the Authentik redirect, or a host whose plugin frame was loosened to fix it |
| E8 | **Plane CE's API-key rate limit is `60/minute` by default** (`API_KEY_RATE_LIMIT`, `apps/api/plane/settings/common.py:154`), so Task 4's "bound concurrency per app (default 8)" is right for reads but a 429 must be expected; `Retry-After` handling is not optional | The number is in CE's settings | Throttling during a factory run; the backoff test already covers it |

## Review Focus

1. **Framing is exactly as configured.** Tests: Task 2 (`frames_from_console_only`, `no_samesite_none`, `app_csp_otherwise_untouched`).
2. **The desktop opens only registry origins and never carries a token.** Tests: Task 5b (`opener_refuses_unlisted_origin`, `opener_passes_no_token`, `sidebar_browser_navigation_allowlist`).
3. **No path from an embed or a plugin to a secret or token.** Tests: Tasks 3, 4, 9.
4. **OpenFGA filtering is real.** Tests: Task 3 (`list_filters_by_openfga`, `unlisted_object_is_404_not_403`).
5. **Deep links cannot act.** Tests: Tasks 6 and 8 (`deeplink_table`, `unknown_path_is_dropped`).

## File structure

```
proto/loams/collab/v1/collab.proto                    # ListApps, threads, issues, cycles, repos, PRs, checks
crates/loams-collab/src/{lib.rs,service.rs,broker.rs,fga.rs,apps/{mod.rs,zulip.rs,plane.rs,forgejo.rs},fixtures.rs}
crates/loams-collab/tests/{main.rs,list.rs,write.rs,broker.rs,fga.rs,canary.rs}
crates/loams-collab/tests/fixtures/{zulip,plane,forgejo}/*.json   # recorded API responses
deploy/factory/edge/{envoy.yaml.tmpl,caddy.snippet,values.edge.yaml,README.md}
deploy/factory/images.lock   LICENSES.md
deploy/factory/harness/{compose.yml,run.sh,assert_frames.mjs}
web/plugins/embed/{package.json,src/{index.ts,service.ts,iframe.ts,toolbar.tsx,deeplink.ts},test/*}
web/plugins/{zulip,plane,forgejo}/{package.json,src/{index.ts,panels/*.tsx,cards/*.tsx,pages/*.tsx},test/*}
web/packages/slots/src/{embed.ts,app-panel.ts,bot.ts,factory.ts}   # new slot declarations
web/apps/console/catalog/base.yml
crates/loams-apps-client/{Cargo.toml,build.rs,src/lib.rs}
desktop/crates/loams-ui-collab/src/{lib.rs,opener.rs,sidebar.rs,panels/{zulip,plane,forgejo}.rs,cards/*.rs}   # zeron fork; path per §37's amendment
desktop/crates/loams-ui-collab/tests/*
docs/design/39-…  docs/design/37-… (slot catalog note)  docs/plans/README.md  CHANGELOG.md
```

### Task 0: Spike: the desktop browser question, the apps' headers and APIs

**Files:** `docs/plans/sf1-spike.md` (the results); a throwaway GPUI example under `desktop/spikes/sidebar/` (deleted at the end).

**Checks** (record each result with the command and OS, in the spike doc):
- **zeron's sidebar browser.** Its README mentions one; its `ARCHITECTURE.md` lists no webview crate and its `zeron-preview` manifest depends on `webrtc`. Read the code (`crates/ui`, `crates/preview`) and record what the sidebar browser actually is (an embedded webview, a remote-rendered preview, or something else) and whether Loams can reuse it for a top-level page.
- **A webview inside GPUI** on macOS (14+), Linux (X11 and Wayland) and Windows: does `gpui-wry` (or `wry` driven directly) host a child webview, at what GPUI revision, with what z-order, resize, focus and IME behaviour? Record per OS: works, works with caveats, or does not.
- **Separate data stores** for two webviews in one window on each OS that works (a named data store on macOS 14+, a data directory on Linux and Windows); cookies in one invisible to the other.
- **Navigation control**: can the host refuse redirects, form posts and new-window requests, and route them to the system browser?
- **WebAuthn and passkeys** inside each OS's webview against Authentik's sign-in (answers Q461).
- Which Authentik release's **proxy outpost** is in the open-source edition, and whether it injects basic auth or a trusted header.
- For each of Zulip, Plane and Forgejo, from the pinned image: the `X-Frame-Options` and `Content-Security-Policy` headers on `/`, `/login` and an API route (these matter to the browser console's iframes); the cookie flags; whether the app serves under a path prefix.
- Plane CE: the REST API v1 endpoints for workspaces, projects, issues, cycles, modules, comments, labels and webhooks (answers Q464), the API key model, and rate limits.

**Decision recorded by the spike:** per OS, whether the sidebar browser ships (tier 3) or the desktop has tiers 1 and 2 only. Nothing else in this plan depends on it.

**Done (2026-10-03).** Results: [the spike notes](sf1-spike.md); rulings E1–E8 above. The two
verdicts the rest of the plan needed: **tier 3 does not ship on any OS** (E1), and **all three
apps deny framing and send no CSP**, so Task 2's edge work is three header removals plus three
appended `frame-ancestors` policies (E5). The throwaway harness lived in `desktop/spikes/sidebar/`
and was deleted; the deliverable is the spike document. The header facts were read from upstream
source, not from a running container (no container runtime on the build machine) — Task 2's
`assert_frames.mjs` harness is where the live probe belongs.

**Commit:** `docs: SF1 spike results`.

### Task 1: Slots, the proto and the app registry

**Files:** `web/packages/slots/src/{embed.ts,app-panel.ts,bot.ts,factory.ts}`, `proto/loams/collab/v1/collab.proto`, `proto/loams/instance/v1/instance.proto` (the `apps` field), `docs/design/37-…` (§5.5 note), `web/packages/mock/*` (AP0's mock gains the services).

**Produces:**

```ts
// web/packages/slots/src/embed.ts
export interface EmbedPaneProps { app: AppId; path?: string; environment: EnvironmentRef }
export type AppId = "zulip" | "plane" | "forgejo" | "glitchtip" | "openpanel" | "langfuse" | "openobserve";
// slots: embed.pane (keyed by AppId), app.panel (keyed `<app>` or `<app>.<panel>`), bot.message.renderer, bot.card, factory.stage.detail
```

```proto
service CollabService {
  rpc ListApps(ListAppsRequest) returns (ListAppsResponse) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc ListThreads(ListThreadsRequest) returns (ListThreadsResponse) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc GetThread(GetThreadRequest) returns (Thread) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc PostMessage(PostMessageRequest) returns (PostMessageResponse);          // idempotency_key required
  rpc ListIssues(ListIssuesRequest) returns (ListIssuesResponse) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc GetIssue(GetIssueRequest) returns (Issue) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc CreateIssue(CreateIssueRequest) returns (Issue);                        // idempotency_key required
  rpc CommentIssue(CommentIssueRequest) returns (IssueComment);
  rpc GetCycle(GetCycleRequest) returns (Cycle) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc ListPullRequests(ListPullRequestsRequest) returns (ListPullRequestsResponse) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc GetPullRequest(GetPullRequestRequest) returns (PullRequest) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc ListChecks(ListChecksRequest) returns (ListChecksResponse) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc MergePullRequest(MergePullRequestRequest) returns (operations.v1.Operation); // always an approval operation
  rpc Watch(WatchRequest) returns (stream WatchEvent);                        // snapshot, changes, heartbeat 15 s, cursor
}
```

`App { id, kind, base_url, embed_url, api_capable, deep_link_templates[], phase }`. Rules of AP0 (D438) apply: stable `reason` in errors, mutations carry an idempotency key, watch streams resume from a cursor.

**Tests:** `buf lint` and `buf breaking`; the slots package's type tests (a plugin registering `embed.pane` with a wrong key type fails to compile); mock server tests: `list_apps_returns_registry`, `plugin_activates_only_when_app_listed` (a plugin whose app is not listed stays inactive and shows no nav entry), `mutations_require_idempotency_key`.

**Commit:** `proto: loams.collab.v1 and the embed, app and bot slots`.

### Task 2: The edge: framing, headers, forward-auth, and the harness

**Files:** `deploy/factory/edge/*`, `deploy/factory/harness/*`, `deploy/factory/images.lock`, `LICENSES.md`, `.github/workflows/factory-edge.yml` (path-filtered).

**Semantics (design §3.3):**
- Routes for `chat.`, `plane.`, `git.` (and, for SF5, `errors.`, `analytics.`, `llm.`, `obs.`). Per route: remove `X-Frame-Options`; replace `frame-ancestors` in any CSP with `'self' <console origin>`, appending when the app sends no CSP (a CSP with only `frame-ancestors`); leave every other directive; leave cookies alone.
- A route flag `forward_auth: authentik` adds Authentik's proxy outpost check before the app (used by SF5).
- `*.localhost` names for `loams dev`; wildcard DNS for a cluster; the Helm values map names to origins.
- `images.lock`: image, digest, licence, source URL, for Zulip, Plane CE, Forgejo, Forgejo Runner and (placeholders) the phase-2 apps. `LICENSES.md` renders the design §4 table from it; CI fails if an image is missing a licence line.

**Tests** (`assert_frames.mjs`, Playwright against the compose harness with the pinned images behind the edge):
`frames_from_console_only` (a page on the console origin frames each app and it renders; a page on `http://evil.localhost` is blocked); `app_csp_otherwise_untouched` (every non-`frame-ancestors` directive equals the app's own); `no_samesite_none`; `xfo_removed`; `login_redirects_to_authentik`; `api_route_not_framed`; `edge_assertion_fails_on_unpinned_image` (the CI script rejects a tag without a digest).

**Commit:** `edge: frame the collaboration apps from the console origin only`.

### Task 3: `loams-collab`: Zulip adapter, the broker and OpenFGA filtering

**Files:** `crates/loams-collab/src/{lib.rs,service.rs,broker.rs,fga.rs,apps/{mod.rs,zulip.rs},fixtures.rs}`, `crates/loams-collab/tests/{main.rs,list.rs,write.rs,broker.rs,fga.rs,canary.rs}`.

**Produces:**

```rust
#[async_trait]
pub trait ZulipApi: Send + Sync {
    async fn streams(&self, permit: CallPermit) -> Result<Vec<Stream>, AppError>;
    async fn topics(&self, permit: CallPermit, stream: StreamId) -> Result<Vec<Topic>, AppError>;
    async fn messages(&self, permit: CallPermit, narrow: Narrow, anchor: Anchor, limit: u16) -> Result<Vec<Message>, AppError>;
    async fn post(&self, permit: CallPermit, to: Target, content: &str, idem: &IdempotencyKey) -> Result<MessageId, AppError>;
}

/// Hands out per-call credential *handles*; the secret never leaves the broker.
pub struct CredentialBroker { /* secret store (Dapr secrets, §24), OpenFGA, clock */ }
impl CredentialBroker {
    pub async fn authorize(&self, cx: &Ctx, app: AppKind, action: Action, object: &FgaObject, approval: Option<ApprovalRef>)
        -> Result<CallPermit, BrokerError>;   // CallPermit performs the call; no accessor for the secret
}
```

**One flow:** the service calls `CredentialBroker::authorize(...)`, which returns a `CallPermit`; the service passes the permit to the adapter method; the adapter builds an `AppRequest` and calls `permit.execute(request)`, which attaches the secret inside the broker's boundary, performs the HTTP call and returns an `AppResponse`. A `CallPermit` is opaque and secret-free (an id, the app, the scope, the object, an expiry; single use or short-lived), has no accessor for the secret, and is the only way an adapter reaches an app, so adapter code cannot read, log or copy a secret. (There is no separate `AppCredential` type.) **Transport:** every credential-bearing hop is HTTPS (or the cluster's mTLS); `permit.execute` follows no redirect to another origin and never forwards the secret on a redirect (same-origin redirects only, the secret re-attached by the broker, other origins fail the call with `APP_REDIRECT`).

**Semantics:** Zulip over its REST API as the agent's bot user (design §6.1). `idem` becomes a Zulip `local_id`-style dedupe: the service keeps `(idempotency key → message id)` in the `ControlStore` for 24 h so a replay returns the first message. The OpenFGA filter (`fga.rs`) asks `ListObjects(user, can_read, zulip_stream)` with a seconds-long cache (as §22 §8.1) and filters streams, topics and messages; **an object the user cannot read is `NOT_FOUND`**, never `PERMISSION_DENIED`, so existence does not leak. Untrusted text (message content) is returned in a field marked `untrusted` and capped at 64 KiB.

**Tests:** fixtures-backed fakes (`wiremock`): `list_streams_and_topics`; `thread_read_paginates`; `post_is_idempotent_on_key`; `list_filters_by_openfga`; `unlisted_object_is_404_not_403`; `broker_refuses_unlisted_action`; `broker_requires_settled_approval_for_destructive`; `redirect_to_other_origin_never_carries_secret`; `plain_http_outside_loopback_is_refused`; `permit_has_no_secret_accessor` and `adapter_cannot_reach_app_without_permit` (compile-fail tests); `canary_secret_never_appears` (§30's canary extended: the `loam_cnry_…` value is the Zulip API key; every method, error and `tracing` output is scanned); `app_error_maps_to_stable_reason` (`APP_UNREACHABLE`, `APP_AUTH`, `APP_RATE_LIMITED`).

**Commit:** `collab: the Zulip adapter, the credential broker and OpenFGA filtering`.

### Task 4: Plane and Forgejo adapters

**Files:** `crates/loams-collab/src/apps/{plane.rs,forgejo.rs}`, fixtures, tests `list.rs`, `write.rs`, `broker.rs`.

**Produces:** `PlaneApi` (`projects`, `issues`, `issue`, `create_issue`, `comment`, `cycles`, `cycle`, `add_issue_to_cycle`) and `ForgejoApi` (`repos`, `branches`, `pulls`, `pull`, `checks` (commit statuses and Actions runs), `ci_log_tail`, `create_branch`, `open_pull`, `comment_pull`, `merge_pull`), as in Task 3.

**Semantics:**
- **Plane.** The API key of the agent's workspace member, from the broker. Issues carry the external id `factory:<run id>` (Plane's `external_id` and `external_source` **(verify, Task 0)**) so `create_issue` is create-if-absent. If Plane CE lacks an endpoint (cycles or modules), the panel reads what exists and `GetCycle` returns `UNIMPLEMENTED` with the reason `APP_CAPABILITY`, recorded in the spike doc.
- **Forgejo.** A fine-grained token limited to the factory policy's repositories. `merge_pull` is **never performed by the adapter directly**: `MergePullRequest` creates an approval operation, and only a settled approval's id lets the broker release the merge permit (design §8 item 2). `ci_log_tail` returns the last 200 lines, truncated at 64 KiB, `untrusted`.
- Both adapters send a `User-Agent: loams-collab/<version>`, honour `Retry-After`, and bound concurrency per app (default 8).

**Tests:** `plane_issues_list_and_detail`; `plane_create_issue_is_create_if_absent`; `plane_cycle_missing_is_unimplemented_with_reason`; `forgejo_pr_status_aggregates_checks`; `forgejo_ci_log_tail_is_capped_and_untrusted`; `merge_without_approval_is_refused_at_the_broker`; `merge_with_mismatched_approval_hash_is_refused`; `rate_limit_backs_off`; `canary_secret_never_appears` (Plane key and Forgejo token).

**Commit:** `collab: the Plane and Forgejo adapters`.

### Task 5: The browser embed plugin

**Files:** `web/plugins/embed/**`.

**Produces:**

```ts
export interface EmbedService {
  open(app: AppId, path?: string): EmbedHandle;                 // a sandboxed iframe in the browser console
  close(handle: EmbedHandle): void;
  onState(handle: EmbedHandle, cb: (s: "loading" | "ready" | "slow" | "error") => void): Dispose;
}
```

**Semantics (design §3.4, browser console):** `iframe.ts` sets the sandbox attribute of design §3.4 and the `allow` attribute to `clipboard-write; fullscreen` only; the URL must be in `ListApps`' `embed_url` set for the active environment; messages from frames are ignored; the toolbar offers back, reload, open in a new tab and copy link; the parent cannot read a cross-origin frame, so the states are only what it can observe: `loading` until the frame's `load` event, `ready` after it, `slow` if no `load` arrives within 8 s (the toolbar then says "If you see a sign-in page, finish signing in there, or open in a new tab"), and `error` on a failed registry check. No attempt is made to detect which origin the frame is on. **The embed frame is not the plugin frame (E7):** `web/packages/console-host/src/sandbox.ts` pins `SANDBOX_FLAGS = 'allow-scripts'` and documents "never `allow-same-origin`, `allow-top-navigation` or `allow-popups`" for *third-party plugin* code; the app embed is a trusted, registry-checked origin and needs `allow-same-origin` (or its session cookie never applies) and `allow-top-navigation-by-user-activation` for the Authentik redirect. `web/plugins/embed` declares its own allowlist constant and a test asserts it is not `SANDBOX_FLAGS` and that no token appears in the frame's URL or name.

**Tests (Vitest and Playwright against the Task 2 harness):** `service_opens_sandboxed_iframe`; `unlisted_app_is_refused`; `sandbox_attribute_is_exact`; `allow_attribute_is_exact`; `frame_messages_are_ignored`; `state_slow_after_8_seconds_without_load`; `state_ready_on_load`; `no_token_in_frame_url_or_name`.

**Commit:** `embed: the browser pane service and iframe host`.

### Task 5b: The desktop client, panel scaffolding, opener and (optional) sidebar browser

**Files:** `crates/loams-apps-client/**`, `desktop/crates/loams-ui-collab/{src/{lib.rs,opener.rs,sidebar.rs},tests/*}`.

**Produces:**

```rust
// crates/loams-apps-client: generated from AP0 and collab protos
pub struct AppsClient { /* connect-rust clients over the desktop's HTTP stack and token source */ }
impl AppsClient { pub fn collab(&self) -> CollabServiceClient; /* + instance, approvals; bot and factory arrive in SF3 and SF4 */ }

// desktop/crates/loams-ui-collab
pub struct AppOpener { /* the active environment's ListApps registry */ }
impl AppOpener { pub fn open(&self, url: &Url) -> Result<(), OpenError>; }   // refuses unlisted origins; no token, no extra args
pub struct SidebarBrowser;   // built with the `sidebar-browser` feature, which is **on by default** per D620. Under E1 it was off and these tests were `#[ignore]`d; D620 ships the engine integration in `crates/loams-sidebar-browser` and un-`ignore`s them
```

**Semantics:** the desktop's HTTP stack and token handling are §37's amendment; `AppsClient` takes them as trait objects (`HttpTransport`, `TokenSource`) so SF1 does not depend on that design. `AppOpener` resolves `loams://app/<env>/<app>/<path>` and panel "Open in browser" actions to registry URLs and opens them with the OS handler. `SidebarBrowser` (feature-gated, **on by default per D620**, which supersedes E1 and E2) loads a registry URL top-level with a per-(environment, app) data store, a navigation allowlist (the app's origin and Authentik's), new windows and downloads refused, no JavaScript bridge to the host, and an address bar that shows the origin. zeron's own browser already provides the allowlist (`crates/ui/src/browser/model.rs:131-138`), cancels downloads on Linux and denies permission requests, but its store is ephemeral. **D620 replaces that browser entirely**: the engine is Obscura over CDP (`crates/loams-sidebar-browser`), which has a persistent store, so the data-store requirement is met and the feature no longer falls back to `AppOpener`. What the replacement gives up is fidelity — see `crates/loams-sidebar-browser/README.md`, which states it plainly.

**Tests:** `opener_refuses_unlisted_origin`; `opener_passes_no_token` (the spawned command line and environment are scanned); `opener_resolves_deeplink_to_registry_url`; `apps_client_uses_injected_transport` (a fake transport sees every call); `apps_client_streams_watch` (collab `Watch` snapshot, changes, heartbeat); with the feature: `sidebar_browser_navigation_allowlist`, `sidebar_browser_data_store_is_per_app_and_env`, `sidebar_browser_refuses_downloads`, `sidebar_browser_has_no_host_bridge`.

**Commit:** `desktop: apps client, the opener and the optional sidebar browser`.

### Task 6: The Zulip plugin

**Files:** `web/plugins/zulip/**`, catalog entries.

**Produces:** `@loams/plugin-zulip`: `console.page` at `/chat` (tabs: Panel, Zulip) where the **Panel** tab is the factory triage stream's topics with a thread view and a reply box (read, post) and the **Zulip** tab is `embed.pane#zulip`; `environment.overview.card` (unread, mentions, triage topics open); `app.panel#zulip.threads`; `bot.card#zulip.thread` (title, last message, reply count, open-in-app link); a `palette.command` "Open Zulip thread…". **Desktop:** `loams-ui-collab/src/panels/zulip.rs`: the same triage-topics list, thread view and reply box as GPUI views, the thread card for the conversation, an "Open in Zulip" action through `AppOpener`. Deep links: `loams://app/<env>/zulip/#narrow/stream/<id>/topic/<name>` resolves through the router to the pane at that path.

**Tests:** Vitest with the AP0 mock: `panel_lists_triage_topics`, `thread_view_streams_new_messages`, `reply_posts_with_idempotency_key`, `untrusted_content_renders_as_text` (HTML in a message is shown escaped; links get `rel="noopener noreferrer"` and open externally), `overview_card_counts`, `deeplink_opens_pane_at_narrow`, `inactive_when_app_not_listed`. Playwright against the harness: `embed_signs_in_through_authentik_once`. GPUI (test context): `zulip_panel_lists_triage_topics`, `zulip_reply_posts_with_idempotency_key`, `zulip_untrusted_content_is_plain_text`, `zulip_open_in_browser_uses_opener`.

**Commit:** `zulip: the console plugin and the desktop panel`.

### Task 7: The Plane and Forgejo plugins

**Files:** `web/plugins/{plane,forgejo}/**`.

**Produces:**
- `@loams/plugin-plane`: `console.page` `/issues` (list with filters, detail drawer, create, comment), `/cycles` (the current cycle's progress and scope), the **Plane** tab as `embed.pane#plane`; `bot.card#plane.issue`; overview card (open factory issues, cycle progress). Deep links `loams://app/<env>/plane/<workspace>/projects/<id>/issues/<issue>`.
  **Desktop:** GPUI issue list, detail, create, comment and cycle views and the issue card.
- `@loams/plugin-forgejo`: `console.page` `/code` (repositories, PRs with status and checks, CI runs with a log tail), the **Forgejo** tab as `embed.pane#forgejo` (diffs open here, `loams://app/<env>/forgejo/<owner>/<repo>/pulls/<n>/files`), `bot.card#forgejo.pr`, `approval.renderer#forgejo.merge` (PR title, base and head, checks summary, the diffstat, "open the diff", typed confirmation for protected branches), overview card (open PRs, failing CI).

**Desktop:** GPUI repository, PR, checks and CI log-tail views, the PR card, and the merge approval view (the native approvals screen renders `forgejo.merge` summaries; "open the diff" goes through `AppOpener`).

**Tests:** Vitest: `plane_issue_list_filters`, `plane_create_issue_flow`, `plane_comment_is_idempotent`, `plane_cycle_card`, `forgejo_pr_list_and_checks`, `forgejo_merge_button_creates_approval_not_merge`, `merge_renderer_shows_hash_matched_summary`, `forgejo_log_tail_is_text_only`, `deeplinks_table` (shared golden file with Tasks 6 and 8). Playwright: `diff_opens_in_embed_pane`. GPUI: `plane_issue_view_renders_untrusted_as_plain`, `plane_create_issue_flow`, `forgejo_pr_view_shows_checks`, `forgejo_merge_action_creates_approval_not_merge`, `forgejo_diff_opens_in_browser_via_opener`.

**Commit:** `plane, forgejo: the console plugins and desktop panels`.

### Task 8: Mobile deep links and native views

**Files (in `ostrium-labs/loams-mobile`):** `ios/Sources/Apps/*`, `android/app/src/main/java/.../apps/*`, golden fixtures `conformance/fixtures/deeplinks.json` (the same file the web tests use, vendored at a pinned ref).

**Semantics (design §3.4, §3.5):**
- **Apps screen**: lists `ListApps`, opens each app's web UI in the system browser (Custom Tabs on Android; `UIApplication.open` on iOS), with the instance's console origin as the allowed base.
- **Native views**: Plane issue list and detail with comment and create; Forgejo PR list and detail with checks and the CI log tail; Zulip thread digest read-only with a reply field. Merge is an approval flow (opens the approval screen), never a button that merges.
- **Deep links**: `loams://app/<env>/<app>/<path>` (and universal links under the console origin `/l/…`) route to the native view when one exists for that path (an issue, a PR, a thread), else to the browser; unknown paths are dropped. **A link only navigates.** Push categories for these arrive in SF3 and SF4.
- Caching: lists cached with freshness timestamps, read-only when stale (D437).

**Tests:** Swift (XCTest) and Kotlin (JUnit, Robolectric/Compose test): `deeplink_table` (every row of the golden file), `unknown_path_is_dropped`, `issue_view_renders_untrusted_text_as_plain`, `merge_opens_approval_screen`, `stale_cache_is_read_only`, `browser_open_uses_custom_tabs` (Android) and `browser_open_uses_system_browser` (iOS); a UI snapshot test per screen against the mock.

**Commit (per repository):** `apps: native issue, PR and thread views and deep links`.

### Task 9: Hardening and the exit gate

**Files:** `crates/loams-collab/tests/canary.rs`, `web/plugins/*/test/security/*`, `docs/plans/sf1-exit-report.md`, `docs/design/39-…` (status and as-built notes), `CHANGELOG.md`.

**Checks:**
- **Canary:** every `loams.collab.v1` method, error, trace and log line of Tasks 3 and 4 scanned for the canary secrets (§30 D288's test, extended).
- **Prompt-injection fixtures:** an issue body, a Zulip message and a PR description containing `</script>`, markdown links with `javascript:` URLs, `[click](loams://…)` deep links, and "ignore previous instructions" text render as inert text in every plugin, card and mobile view; no deep link inside content is ever followed without a tap; the `untrusted` flag is present on every free-text field.
- **Exit gate (all must pass in CI):** the edge assertions of Task 2; the browser embed and desktop opener tests of Tasks 5 and 5b; plugin tests of Tasks 6–7; mobile tests of Task 8; the licence table check; an end-to-end Playwright run: sign in at Authentik, open Zulip, Plane and Forgejo panes, create a Plane issue from the native panel and see it in the embedded Plane, open a Forgejo PR's diff from the native PR panel; on the desktop build (the standalone GPUI harness until the fork lands), open the same issue and PR panels and the system-browser links; and on a simulator, open the issue on the phone through a deep link.
- Record outcomes, measured sizes (the plugins' bundle sizes, the desktop binary delta), and any API gaps found, in the exit report.

**Commit:** `docs: SF1 exit report`.
