# SF1 Task 0 — Spike: the desktop browser question, the apps' headers and APIs

> **The findings of [SF1](2026-10-02-sf1-collab-ui-plugins.md) Task 0.** Executed 2026-10-03 on
> `origin/dev` at `4197d53`, branch `docs/sf1-t0-spike`. Everything below is evidence, not design:
> the rulings it settles are in the plan's
> [Rulings made during execution](2026-10-02-sf1-collab-ui-plugins.md#rulings-made-during-execution)
> (E1–E8), and where the spike and the plan disagree, the spike wins.
>
> **No decision-log ID is assigned here.** Where a finding needs a new decision row in
> `docs/design/13-decision-log.md`, it is listed under [Needs a decision ID](#needs-a-decision-id)
> and left unassigned for the owner.

## Method, and what was *not* done

The spike had two halves. The desktop half was answered by **reading zeron's source**; the
apps half was answered by **reading upstream Zulip, Plane CE and Forgejo source**, not by running
them. The build machine has no container runtime:

```console
$ docker version
bash: docker: command not found
```

`/home` had 19 GB free (the plan's floor is 8 GB) and 15 GB RAM, so the block was Docker, not
resources. Consequences, stated plainly:

- **Not live-probed.** Every `X-Frame-Options`, `Content-Security-Policy` and cookie fact below is
  transcribed from source, with the file and line. A source read is strong evidence about the
  default configuration; it is *not* evidence about what a pinned image sends behind our edge.
  **Task 2's `assert_frames.mjs` harness is where the live probe belongs**, and it should assert
  these same values.
- **No GPUI compile.** Task 0 asked whether a webview can be hosted in GPUI per OS. The answer
  turned out to be a property of zeron's existing browser and of WebKitGTK, both readable without
  compiling. No crate was built; nothing in this document rests on a Rust build.
- **The throwaway harness is gone.** It lived in `desktop/spikes/sidebar/` (a header-rewrite model,
  a WebAuthn matrix, a transcription of Plane CE's v1 routes) and was deleted before the PR. The
  scripts' output is quoted below so the numbers can be re-derived.

---

## Decision 1 — Does the sidebar browser ship? (Q461, design §18.5, tier 3)

> **SUPERSEDED on 2026-10-03 by [D620](../design/13-decision-log.md).** The owner
> overrode this conclusion: the sidebar browser **ships**, on a different engine.
> The conclusion below is left exactly as the spike wrote it, because its
> evidence is still true — an ephemeral store, no WebAuthn on the GTK and WPE
> WebKit ports, and no Windows implementation really are the properties of the
> per-platform webview. What changed is the premise: **D620 embeds no webview
> at all**, so none of the three blockers is a property of the feature any more.
> What is not superseded is E5, E6, E7, E8 and Decision 2, 3 and 4.

**Conclusion: no, on any operating system. The desktop ships tiers 1 and 2 only — native GPUI
panels and `AppOpener` into the system browser.** Plan ruling E1.

### Question

Zeron's README advertises a sidebar browser. Design §37 §18.5 already records a verified table
(Linux: WebKitGTK helper with an *ephemeral* store; macOS: `wry`/WKWebView; Windows: does not
exist) and hands four items to a spike: a persistent data store, WebAuthn per OS, a Windows
browser, and whether `gpui-wry` sits on zeron's GPUI fork. Which of those block tier 3?

### How it was investigated

`gh api` over `zeronsh/zeron`'s tree: the workspace's 16 `crates/*`, then `crates/ui/Cargo.toml`,
then every file in `crates/ui/src/browser/` (`mod.rs`, `model.rs`, `view.rs`, `macos.rs`,
`linux/mod.rs`, `linux/helper.c`). Design §37 §18.5's markers are the reference point; this reads
the code behind them.

### Findings

The plan's Ruling 2 says "zeron has no webview crate in its workspace". That is **wrong** and is
corrected in the plan. There is no separate *crate*, but `crates/ui` has a `browser` module with a
real webview on two platforms:

| | macOS | Linux | Windows |
|---|---|---|---|
| Engine | WKWebView through `wry 0.56` (`features = ["os-webview"]`), parented with `objc2-web-kit` | WebKitGTK in a **separate helper process** (`helper.c`) that sends offscreen frames over a socket to GPUI | **none** |
| Website-data store | `WKWebsiteDataStore::nonPersistentDataStore` — `crates/ui/src/browser/macos.rs:41-45` | `webkit_web_context_new_ephemeral()` — `crates/ui/src/browser/linux/helper.c:619` | — |
| Persistent store possible? | not without patching the fork; the store is built once in `make_configuration` with no alternative branch | not without patching the C helper | — |
| WebAuthn / passkeys | **supported** (Apple WebKit port) | **not supported** (GTK/WPE WebKit ports) | n/a |
| Module gating | `crates/ui/src/browser/mod.rs:2-8` — `linux` and `macos` only; no `windows` arm anywhere in the module | | |

Evidence for the WebAuthn split, which is the load-bearing fact:

- **WebKit bug 205350**, "[WPE][GTK] Support WebAuthn" — *"WebAuthn is currently supported only on
  Apple ports."* Still open; the patch discussion (2019→2023) ends with the credentialsd portal API
  described as not ready. So WebKitGTK has no WebAuthn and no near-term plan for it.
- **webcompat/web-bugs#184312** (2025-10-21), triaged as `RESOLVED FIXED` with the maintainer
  writing: *"WebAuthn is not supported in the GTK and WPE WebKit ports."*

Navigation control, which is the part that turns out to be **already solved** in zeron's fork:

- `crates/ui/src/browser/model.rs:131-138` — `allowed_navigation()` admits `http`/`https` only,
  requires a host, and rejects any URL carrying userinfo. It has its own unit tests
  (`native_tabs_match_the_configured_navigation_keystrokes`, and assertions at lines 192-193 that
  `javascript:` and `https://user@example.com/` are refused).
- `crates/ui/src/browser/macos.rs:174-188` — `decidePolicyForNavigationAction` and
  `…NavigationResponse` consult that allowlist, require the main frame, and cancel anything else.
  Redirects and non-displayable responses are cancelled, not silently followed.
- `crates/ui/src/browser/linux/helper.c:184-213` — the same through `decide-policy`, and a
  user-gesture new window is sent to the host (`send_packet('N', …)`) and the decision ignored.
- `linux/helper.c:284-285` cancels every download; `linux/helper.c:215-217` denies every
  `WebKitPermissionRequest`. **There is no macOS counterpart for downloads**, so on macOS a
  download is not explicitly refused — a gap to close upstream, not one SF1 must solve.

### Conclusion and why it is decisive

A docked app needs two things at once: a store that survives the window closing, and a WebAuthn
ceremony for the Authentik passkey. The throwaway check encoded exactly that conjunction:

```console
$ node desktop/spikes/sidebar/webauthn-check.mjs
macos    webauthn=supported      store=false     tier 3 impossible
linux    webauthn=not supported  store=false     tier 3 impossible
windows  webauthn=n/a            store=false     tier 3 impossible

platforms clearing both bars: none
```

Zero platforms. An ephemeral store means every launch starts at the Authentik login page, and on
Linux the passkey path does not exist at all, so "sign in once" — the property that makes an
in-app browser worth having — cannot be delivered. Tier 3 is therefore off in SF1 on every OS
(E1), and `SidebarBrowser` keeps its `sidebar-browser` cargo feature but off by default, with the
persistent per-(environment, app) data store requirement explicitly parked on AP1n Task 7 (Q482)
and D491, which is where the work goes upstream (E2).

This **respects** the plan's Global Constraints rather than working around them: an ephemeral store
is structurally incapable of holding a Loams token or a session, so "embeds never hold a Loams
token" is easier to keep here, not harder.

> **What D620 changed about that last paragraph.** The reasoning above is sound
> and it is also the thing that made E1 reachable: the constraint was easy
> *because* the store retained nothing. D620 replaces the store with a
> persistent one, so the constraint stops being free and has to be enforced:
> Loams holds the credential, runs the OIDC ceremony itself, and hands the
> engine only host-only scoped session cookies, with `crates/loams-sidebar-browser/src/boundary.rs`
> auditing both the cookie jar and the whole profile directory for a Loams
> token. E1's line "`SidebarBrowser` keeps its `sidebar-browser` cargo feature
> but off by default" is superseded: under D620 the feature is **on** by
> default, and the persistent per-(environment, app) store ships with it, so
> E2's parking of that requirement on AP1n Task 7 / Q482 / D491 is superseded
> too. D491's upstream-first policy is unaffected.

### What SF1 inherits that is still open

The macOS download refusal is missing upstream, and `gpui-wry` compatibility with zeron's GPUI fork
was not tested (it needs a compile, which this spike deliberately avoided). Both belong to AP1n
Task 7, not to SF1. Under D620 the first of these is moot for the sidebar — there is no WKWebView to
miss the refusal — and the second is moot too, because `gpui-wry` is not on the sidebar's path.

---

## Decision 2 — What does each app send, and what must the edge change?

**Conclusion: all three deny framing, none sends a CSP, none may be given `SameSite=None`, and
Zulip's `__Host-` cookie prefix means the edge must not touch cookie domains.** Plan ruling E5,
plus E6.

### Question

Plan Task 2 says: remove `X-Frame-Options`, replace `frame-ancestors` in any CSP with
`'self' <console origin>` (appending when the app sends none), leave every other directive, leave
cookies alone. For which of the three apps is each of those branches actually taken?

### How it was investigated

Source reads, recorded in `app-headers.json` in the throwaway directory with file and line for each
value: `zulip/zulip` `puppet/zulip/files/nginx/zulip-include-common/headers` and
`zproject/computed_settings.py`; `makeplane/plane` `apps/api/plane/settings/common.py` and
`apps/api/plane/api/middleware/api_authentication.py`; Forgejo `custom/conf/app.example.ini`
(fetched from `codeberg.org/forgejo/forgejo`, branch `forgejo`) and the Forgejo config cheat sheet.
Then a pure-JS model of Task 2's rewrite was run against those values.

### Findings

| | Zulip | Plane CE | Forgejo |
|---|---|---|---|
| `X-Frame-Options` on `/`, login, API | **`DENY`** — `add_header X-Frame-Options DENY always` in the nginx include | **`DENY`** — `django.middleware.clickjacking.XFrameOptionsMiddleware` at `settings/common.py:129`, and `X_FRAME_OPTIONS` is never set, so Django's `DENY` default applies to *every* response | **`SAMEORIGIN`** — `[security] X_FRAME_OPTIONS` default (app.example.ini:1272; documented in the cheat sheet) |
| `Content-Security-Policy` | none — absent from the nginx header include and from `computed_settings.py` | none — no CSP middleware in `MIDDLEWARE` | none — no CSP setting in `app.example.ini`, none in `routers/common/middleware.go` |
| Session cookie | `__Host-sessionid`, `Secure`, `HttpOnly`, `SameSite=Lax` (Django default; only `LANGUAGE_COOKIE_SAMESITE` is set, to `"Lax"`) | `session-id`, `Secure` unless an allowed origin is `http:`, `HttpOnly`, `SameSite=Lax` (unset → Django default) | `session`, `SameSite=lax`, `Secure` when `ROOT_URL` is https |
| Serves under a path prefix? | no | no — no `SUB_PATH` handling in `settings/common.py` | **yes** — `ROOT_URL` plus shipped subpath reverse-proxy guides |
| Other headers the edge must not disturb | `Strict-Transport-Security`, `X-Content-Type-Options: nosniff`, `Referrer-Policy`, `Cross-Origin-Opener-Policy: same-origin` | `CORS_ALLOW_CREDENTIALS = True`, `CORS_ALLOW_HEADERS` includes `X-API-Key` | `[service] ENABLE_REVERSE_PROXY_AUTHENTICATION` exists — off by default, and Task 2 must not turn it on |

The header model, run against all nine route cases:

```console
$ node desktop/spikes/sidebar/edge-model.mjs
ok   zulip / xfo_removed
ok   zulip / frame_ancestors_is_self_plus_console — frame-ancestors 'self' https://console.loams.example
… (27 route checks) …
ok   existing_csp_non_frame_directives_preserved — default-src 'self'; script-src 'self' https://cdn.example; frame-ancestors 'self' https://console.loams.example; object-src 'none'
ok   no_samesite_none
ok   api_route_not_framed_not_decidable_from_headers — deferred to the Task 2 Playwright harness

all header-model checks passed
```

### Conclusions

- **The "append when the app sends no CSP" branch is the branch all three apps take.** Task 2's
  `app_csp_otherwise_untouched` is therefore vacuous against the real images; the throwaway model
  checked it against a synthetic CSP with `default-src`, `script-src` and `object-src` to prove the
  rewrite is not merely "clobber everything". Task 2's harness should keep that synthetic case.
- **`no_samesite_none` is satisfiable by not touching cookies.** All three are `Lax`, which works in
  a frame **only because of the plan's Ruling 1** — sibling subdomains of one registrable domain are
  same-site, so `Lax` is sent on the frame's subresource navigation. A cross-site deployment of the
  same apps would need `SameSite=None; Secure`, which the plan forbids. Ruling 1 is load-bearing.
- **Plane CE cannot be served under a path prefix** and neither can Zulip, so Ruling 1 is forced,
  not merely preferred. Forgejo could be; treating all three the same (sibling subdomains) keeps
  one rule and one wildcard DNS entry.
- **Zulip's `__Host-` prefix forbids any `Domain` attribute** (E6). The edge must not try to share
  one cookie domain across the app subdomains. Each app keeps a host-only session — which is what
  the `ListApps` registry assumes anyway.
- **`api_route_not_framed` cannot be decided from headers.** Left for Task 2's Playwright harness.

---

## Decision 3 — Plane CE's API surface, key model and limits (Q464, plan Rulings 4 and Task 4)

**Conclusion: every endpoint `PlaneApi` names exists in CE's REST API v1, including cycles and
modules, so `GetCycle` is a real call; the key is a per-account personal access token with no
OAuth issuer; the default key rate limit is 60/minute.** Plan rulings E3, E4, E8.

### Question

Task 4 says: if Plane CE lacks an endpoint (cycles or modules), `GetCycle` returns `UNIMPLEMENTED`
with reason `APP_CAPABILITY`. Ruling 4 says the API key is "per agent identity, not per user" and
flags it "verify, Task 0". Task 4 also wants the issue external id to ride on Plane's `external_id`
and `external_source`. What does CE actually ship?

### How it was investigated

`gh api` over `makeplane/plane`: `apps/api/plane/api/urls.py` (to confirm the `api/v1/` mount),
then every module in `apps/api/plane/api/urls/` — `project.py`, `cycle.py`, `module.py`,
`work_item.py`, `member.py`, `state.py`, `label.py`, `intake.py` — read verbatim. Then
`apps/api/plane/settings/common.py` and `apps/api/plane/api/middleware/api_authentication.py`.
Code search for `external_source` and `DEFAULT_THROTTLE_RATES`. The route dump is preserved in the
transcript of this document's method.

### Findings

Every route below is under `/api/v1/workspaces/<slug>/`:

| `PlaneApi` member | Route | Source |
|---|---|---|
| `projects` | `projects/`, `projects-lite/`, `projects/<uuid:pk>/`, `projects/<id>/archive/`, `projects/<id>/summary/` | `api/urls/project.py` |
| `issues` | `projects/<uuid:project_id>/issues/` and the `work-items/` alias, plus `issues/search/` and `work-items/search/` | `api/urls/work_item.py` |
| `issue` | `projects/<id>/issues/<uuid:pk>/` | `api/urls/work_item.py` |
| `comment` | `projects/<id>/issues/<issue_id>/comments/` and `…/comments/<uuid:pk>/` | `api/urls/work_item.py` |
| `cycles` | `projects/<id>/cycles/`, `cycles-lite/`, `cycles/<uuid:pk>/` (GET/PATCH/DELETE) | `api/urls/cycle.py` |
| `cycle` | same detail route | `api/urls/cycle.py` |
| `add_issue_to_cycle` | `cycles/<uuid:cycle_id>/cycle-issues/` (POST), plus `…/transfer-issues/` | `api/urls/cycle.py` |
| modules (not named by the trait) | `api/urls/module.py`, same shape | `api/urls/module.py` |
| workspace / members / states / labels / intake | `member.py`, `state.py`, `label.py`, `intake.py` | — |

- **Cycles and modules are present in CE.** Task 4's `UNIMPLEMENTED` contingency does **not** fire
  (E3). `plane_cycle_missing_is_unimplemented_with_reason` still earns its keep as a *capability
  probe* against a pinned image that might lag — but it must not be written as "CE has no cycles".
- **The key model is a personal access token bound to an account, with no OAuth issuer.**
  `api/middleware/api_authentication.py`: class `APIKeyAuthentication`, `auth_header_name =
  "X-Api-Key"`, validated as `APIToken.objects.get(Q(expired_at__gt=now) | Q(expired_at__isnull=True),
  token=…, is_active=True, user__is_active=True)`, returning `(api_token.user, api_token.token)`.
  Every CE view also sits behind `SessionAuthentication`. `apps/api/plane/urls.py` mounts only
  `api/` (app), `api/public/` (spaces), `api/instances/` (licence), `api/v1/` (the REST API) and
  `auth/` — and `plane/authentication/urls.py` lists sign-in, sign-up, sign-out, magic link, CSRF
  and the *social login callbacks* (Google, GitHub, GitLab), with **no authorization-server
  endpoints** (`/authorize`, `/token`) for third-party clients. So Ruling 4's premise holds —
  **there is no per-user OAuth for third-party callers** — and the way to get "per agent identity"
  is a **dedicated Plane service account whose `APIToken` the broker holds** (E4). Plane's audit
  will name that account; Loams' audit holds the person. That is the cost Ruling 4 already accepted.
- **Rate limits.** `settings/common.py:154`:
  `API_KEY_RATE_LIMIT = os.environ.get("API_KEY_RATE_LIMIT", "60/minute")`, with
  `DEFAULT_THROTTLE_RATES = {"anon": "30/minute", "asset_id": "5/minute"}` at lines 141-144. A
  DRF-format string, tunable by environment variable, **60 requests per minute per key** by default
  (E8). Task 4's "bound concurrency per app (default 8)" is not enough on its own: 8 in-flight
  reads at 60/minute will still see 429s on a panel that pages. `Retry-After` handling must be real
  and `rate_limit_backs_off` must be a genuine test, not a formality.
- **`external_id` / `external_source` are real CE fields on `Issue`**
  (`apps/api/plane/db/models/issue.py:162-163`, and again on the sub-models in the same file, with
  migrations for page, project and cycle). Task 4's create-if-absent plan is viable; it must key on
  `(external_source, external_id)`.
- **Licence.** Plane CE is **AGPL-3.0-only** (`SPDX-License-Identifier: AGPL-3.0-only` in every
  source header). It is a separately deployed service reached only over HTTP by our broker, which
  is the same posture the plan already takes for Zulip. But it means: no Plane source or text may be
  vendored into this repository, and `scripts/spec/provenance.sh` must pass on the recorded
  fixtures. Recorded JSON *responses* are interface facts, not code — the plan's fixture approach
  stands, and it needs a licence line in `deploy/factory/images.lock` (Task 2).

---

## Decision 4 — Authentik's proxy outpost in the open-source edition

**Conclusion: the OSS edition ships the embedded outpost, and the proxy provider injects trusted
headers by default; HTTP-Basic sending is an opt-in toggle we will not turn on.** No plan ruling
needed; it confirms D447/D448 and Task 2's `forward_auth: authentik`.

### Question

Task 0 asks which Authentik release's proxy outpost is in the open-source edition, and whether it
injects basic auth or a trusted header.

### How it was investigated

`docs/design/38-knative-authentik-gitops.md` for the pin (D447: **2026.8.3**, released 2026-09-17,
unmodified, no licence key, MIT outside `authentik/enterprise/`; the 2025.10 release notes that
moved the embedded outpost off Redis). Then the upstream proxy-provider and header-authentication
documentation, and `X-authentik-` search in `goauthentik/authentik`.

### Findings

- The proxy provider has three modes: **Proxy**, **Forward auth (single application)** and
  **Forward auth (domain level)**. Domain-level forward auth protects *multiple applications under
  one parent domain* — which is exactly the plan's Ruling 1 shape, so one Authentik provider can
  front `chat.`, `plane.` and `git.` together. Its documented limit is that it cannot enforce
  per-application authorisation rules, which is fine for SF1 because the per-object filtering is
  OpenFGA's job inside `loams-collab` (Task 3), not the edge's.
- **Headers, set by default:** `X-authentik-username`, `-groups` (pipe-separated), `-entitlements`,
  `-email`, `-name`, `-uid` (a hash), plus `X-authentik-meta-outpost` / `-provider` / `-app` /
  `-version`. The example value for the meta-outpost header is literally
  **`authentik Embedded Outpost`** — the OSS embedded outpost, answering Task 0's question.
- **Basic auth is not injected by default.** "Send HTTP-Basic Authentication" is a separate toggle
  requiring username and password *attribute keys* on a user or group. We will leave it off: a
  static shared credential per app is exactly the "app secret outside the broker" shape the plan's
  Global Constraints forbid. The default trusted-header mode is also what `forward_auth` gives us.
- `Intercept header authentication` is on by default: authentik takes the `Authorization` header,
  and **removes it** before the request reaches the app when the credentials check out. Relevant to
  the "no token in the frame" gates — the outpost, not the edge, is responsible for not leaking an
  `Authorization` header downstream.
- WebAuthn and passkeys are explicitly in the fixed usable feature list (D448: "flows and stages
  with TOTP, WebAuthn and passkeys"), so the passkey ceremony exists server-side; only the *client*
  engine's support is in question, which is Decision 1.

---

## Global Constraints check

Every finding above was tested against the plan's Global Constraints. Nothing contradicts them;
two findings make them easier to hold.

| Constraint | Finding |
|---|---|
| Embeds never hold a Loams token | Unaffected. Decision 1 makes the desktop's ephemeral store an ally: an ephemeral store cannot retain a token. Decision 2's `__Host-` prefix (E6) additionally removes the edge as a place a token could be smuggled. |
| Native panels read through `loams.collab.v1` only; a plugin never imports an app's API client | Unaffected, and Decision 3 sharpens it: with cycles and modules present in CE, the temptation is for a Plane plugin to reach `/api/v1/…` directly. The import/boundaries test must be in place *before* the panels exist. |
| No app secret leaves the broker; `Secret` has no `Serialize`, redacted `Debug` | Unaffected, and Decision 4's "leave basic-auth injection off" is the same rule at the edge. The Plane `APIToken` is a broker-held secret and nothing else. |
| `MergePullRequest` creates an approval operation, never a merge | Untouched by Task 0; no finding here touches it. |
| No app is modified, forked or patched (D-SF-17) | Respected. Tier 3 is switched **off** rather than patched into existence; the persistent-store and macOS-download work goes upstream to `zeronsh/zeron` (D491). The edge changes are header manipulation only. |

**One place where I could only go as far as the evidence allows, loudly:** the header facts are
source-derived, not probed from the pinned images. If a pinned image is configured with
`X_FRAME_OPTIONS = unset` or a CSP that upstream's defaults do not show, Task 2's assertions —
not this document — will find it. I did not want to state these as verified-against-the-image
because I did not run the images.

---

## Plugin contract reconciliation (not a Task 0 check, but load-bearing for Task 1)

Task 0 was pointed at `web/packages/console-host/plugin-manifest.schema.json` and
`src/manifest.ts` as the ground truth the plan must match. Three mismatches, recorded here rather
than fixed, because they belong to Task 1:

1. **Slot names.** The host's `SlotMap` on `dev` has exactly seven slots: `root`, `console.nav`,
   `console.page` (keyed), `console.settings.section`, `environment.overview.card`,
   `approval.renderer` (keyed), `shell.overlay`. The plan uses `console.page`,
   `environment.overview.card` and `approval.renderer` — good, those exist — but
   `embed.pane`, `app.panel`, `bot.card` and `palette.command` **do not exist yet** and are Task 1's
   to declare, exactly as the plan says. `bot.message.renderer` and `factory.stage.detail` appear
   only in a Task 1 code comment and belong to SF3/SF4; naming them in SF1's `bot.ts` would declare
   a slot this plan does not own.
2. **Permission enum.** `permissions` is a closed enum in both the schema and `isPermission`. There
   is no permission for embedding an app; an embed pane needs no new permission because it frames a
   registry origin and communicates nothing back (Task 5's "messages from frames are ignored"). That
   is a good outcome — the plan's embed design needs no permission-model change.
3. **The embed frame is not the plugin frame (E7).** `src/sandbox.ts:19-20` pins
   `SANDBOX_FLAGS = 'allow-scripts'` with the comment *"Only these tokens; never
   `allow-same-origin`, `allow-top-navigation` or `allow-popups`."* Task 5's embed frame needs
   `allow-same-origin` (otherwise the app's session cookie never applies) and
   `allow-top-navigation-by-user-activation` (the Authentik redirect). Task 5 must declare its own
   allowlist in `web/plugins/embed`, and must not weaken `SANDBOX_FLAGS`, which is load-bearing for
   third-party plugin code. Recorded as E7.

A first-party plugin's manifest on `dev` looks like `@loams/plugin-stack-status`: `kind: "console"`,
`entry: "."`, `tier: "first-party"`, `inject: ["flags","router","platform"]`,
`slots: ["console.page","console.nav","environment.overview.card"]`,
`permissions: ["instance:read"]`, `requires: { console: "^0.1.0", api: ["loams.instance.v1"] }`,
`editions: ["oss","desktop","cloud"]`. SF1's plugins follow it; note that `requires.api` must gain
`loams.collab.v1` and match `^loams\.[a-z]+\.v[0-9]+$`.

---

## Needs a decision ID

Nothing here is self-assigned. These findings want rows the owner may want to number:

1. **E1** — the sidebar browser does not ship in SF1 on any OS; the desktop is tiers 1 and 2.
   Related to D484 and D491; a decision ID would let SF3/SF4 cite it instead of re-deriving it.
   **Answered and superseded: [D620](../design/13-decision-log.md) (2026-10-03),
   which also closes Q482.** E1's evidence stands; its conclusion does not.
2. **E7** — the app embed frame is a distinct frame class from the console host's third-party
   plugin frame, with its own allowlist. This *amends* `web/packages/console-host/src/sandbox.ts`'s
   stated policy boundary, which is AP1a's, so it likely wants an ID and a note in design §37 §5.6.
3. **E6** — Zulip's `__Host-` cookie prefix forbids any `Domain` attribute, so the apps keep
   host-only sessions and the edge never sets a shared cookie domain. Small, but it constrains the
   edge's vocabulary for every app.

Not needing an ID: Decision 3's findings are corrections to plan text (E3, E4, E8) and Decision 2's
are what Task 2 was already going to implement (E5).

## Also noted, out of scope for this PR

- **Stale crate names elsewhere in the track.** `operon-*` paths remain in the SF2, SF3, SF4 and SF5
  plans and in parts of this plan's siblings (`docs/design/39-…` is already correct). Same
  `operon` → `loams` defect class this PR fixes in SF1; each is a one-line-per-path fix in the
  owning plan. Not edited here — those plans are not mine.
- **SF2's status line says "PRs target `main`** too. Same defect as SF1's. Not edited here (SF2's
  file), recorded so it is not lost.
- **`crates/loams-collab` does not exist yet** on `dev`; Task 3 creates it. Its member path is
  `crates/*`, so the path in the plan is right once the crate exists.