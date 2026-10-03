# 39 — Loams Software Factory and Loams Bot: Embedded Collaboration Apps, A2A Agents and the Closed Loop

Status: **Proposed** · 2026-10-02. The direction is the owner's, from 2026-10-02:

> "write a plan for integrating the UI of Plane, Forgejo, Zulip, GlitchTip, Langfuse 4 for agent tracing and OpenObserve for distributed tracing; for now pick the Slack, Jira, GitHub part [Zulip, Plane, Forgejo first] if you finish it fast; and in our deepseek fork harness in desktop and mobile apps, there should be a single chat interface called Loams bot, where we drive agents of each platform — Plane, Zulip, Forgejo, GlitchTip + product analytics — using A2A. It's a loop, called Loams Software Factory. Make it available as first in our Cloud marketplace page."

**Owner rulings later the same day, applied here:** (1) the names are **Loams Bot** and **Loams Software Factory** in prose, titles and decisions (the quotation above keeps the owner's original words); (2) **the desktop app is native, not Tauri**: a fork of zeron (`github.com/zeronsh/zeron`, MIT; Rust on GPUI; "a native control plane for Claude Code, Codex, Cursor, Devin and other coding agents", with a local engine daemon and optional multi-device sync), and Loams Bot on the desktop builds on zeron's agent and session engine. The browser console stays cordis; the phones stay native SwiftUI and Compose. This document cites **§37 (amended for a native desktop, D480–D499)** and does not design the desktop shell; it designs what the factory adds to it (§3.4, §13).

This document turns that direction into decisions **D-SF-1–D-SF-20** and open questions **Q460–Q479** (recorded in the canonical decision log). Everything beyond the quoted direction (the embed rules, the A2A mapping, the loop's gates, the open/commercial split) is a **proposal** until the owner confirms it. **No code is written by this document.** The hosted factory and the marketplace are commercial and are designed in the private `loam-platform` repository (its doc 05); this document only states the interface between the two.

**Numbering.** D-SF-1–D-SF-20 and Q460–Q479 are this document's blocks; both are in the [decision log](13-decision-log.md) since #319 (2026-10-03), so "renumber at merge if another branch took them" is settled and no longer applies. They collide with nothing: §38 holds D440–D459 and Q440–Q459, and the Elasticsearch gateway holds D460–D466, which leaves D467–D479 free for a future numeric block. When this document needs more numbers, take them above the log's current top rather than from §38's or the gateway's blocks: `dev`, the default branch, runs to D619 and Q614 as of 2026-10-03, while `main` still stops at D459 and Q454. The private repository has its own numbers (PD47 onward).

Markers: **(verified)** means read on the web on 2026-10-02 at the source in §17. **(verify)** means the plan that builds it checks it first. **(estimate)** means computed, not measured. "The harness" is the DeepSeek Harness (MIT) in its desktop and mobile forms (§37 §3); "our fork" is the harness code Loams adapts, per D421, never a copy-and-fork of the whole repository.

---

## 1. Summary

| # | Decision | Status |
|---|---|---|
| D-SF-1 | **Two products, one loop.** **Loams Software Factory** ("the factory") is a durable loop over five platform agents: a signal (an error or an analytics anomaly) is triaged in Zulip, becomes an issue in Plane, becomes a branch, a pull request and CI in Forgejo, is deployed, is observed in GlitchTip, OpenPanel, Langfuse and OpenObserve, and feeds the next signal. **Loams Bot** is the single chat in the desktop and mobile apps through which a person drives those agents. A new track, **SF**, has five plans (SF1–SF5). Phase 1 is Zulip, Plane and Forgejo, done in full; phase 2 adds GlitchTip, OpenPanel, Langfuse and OpenObserve | Proposed |
| D-SF-2 | **Embed rule: native panels for what the loop reads and writes, the app's own UI embedded for everything else.** Each app gets (a) API-driven native panels from `loams.collab.v1` (cordis plugins in the browser console, native GPUI panels in the desktop app, SwiftUI and Compose views on phones), for the objects the factory touches, and (b) its own web UI for depth: framed unmodified in the browser console, opened in the system browser or an optional in-app sidebar browser on the desktop. The per-app decision is in §3.2. **Mobile does not embed**: native views, sealed push and deep links that open the app's web UI in the system browser (§3.5) | Proposed |
| D-SF-3 | **Embedding is configuration, never a patch.** Every app runs unmodified behind the edge proxy (§22 §3, Envoy or Caddy per §38), on **sibling subdomains of one registrable domain**. The edge removes the app's `X-Frame-Options`, sets `Content-Security-Policy: frame-ancestors <the console origins>` and leaves cookies as the apps set them (the edge rewrites a `SameSite=None` to `Lax` only on a route whose image needs it, and `images.lock` lists the exception), and the console's `frame-src` lists only the instance's app origins (§3.3). Because no app is modified, AGPL §13's duty for modified, network-served versions is not triggered; redistributing an image and offering the apps as a paid hosted service carry their own obligations, which §4 lists and counsel reviews | Proposed |
| D-SF-4 | **Single sign-on through Authentik** (open-source edition, D404, §38). Native OIDC for Zulip, Forgejo, GlitchTip and Langfuse; **Plane** by the Forgejo-chained Gitea provider (§22 §5); **OpenPanel and OpenObserve have no free OIDC** and sit behind Authentik's proxy outpost (forward-auth). Embedded panes never receive Loams tokens: each is signed in at the app by Authentik's browser session (§3.4) | Proposed |
| D-SF-5 | **Desktop: native panels first, deep UIs in the system browser, an in-app sidebar browser optional.** Loams Desktop is a fork of zeron (§37, amended for a native desktop, D480–D499), so there is no Tauri webview, CSP or `net_fetch` here. App UIs reach the desktop as (a) native GPUI panels over `loams.collab.v1`, and (b) the app's own web UI opened as a **top-level page** in the system browser, which shares the person's Authentik session, or in an in-app sidebar browser **if** SF1's spike shows a wry-based webview works inside GPUI on the supported OSes. A top-level page is never framed, so the edge's frame headers matter only to the browser console, which uses sandboxed iframes (§3.4) | Proposed |
| D-SF-6 | **A2A v1.0 between Loams Bot and the platform agents; Connect-RPC from the apps to Loams Bot.** A2A 1.0.0 is a Linux Foundation project with official SDKs for Python, JavaScript, Java, .NET, Go and Rust **(verified)**; there are no official Kotlin or Swift SDKs. The phones and the desktop therefore never speak A2A: they use `loams.bot.v1` over Connect (D420), and Loams Bot is the A2A client. Loams Bot also has an A2A server, **disabled by default**: its route and Agent Card exist only when an operator starts the bot with `--bot-a2a` (Q469) (§5) | Proposed |
| D-SF-7 | **One agent per platform, each a separate A2A server with its own principal.** `plane`, `zulip`, `forgejo`, `glitchtip` and `analytics` (OpenPanel). Each serves a signed Agent Card at `/.well-known/agent-card.json`, runs as a Knative service (or in-process in `loams dev`) and holds no app credential: a credential broker injects it per call (§6, §7) | Proposed |
| D-SF-8 | **An A2A Task is a §21 operation.** `contextId` is the chat thread or the factory run, `taskId` is the operation id, A2A states map one-to-one onto operation states, `TASK_STATE_INPUT_REQUIRED` carries either a question for the user or an approval (D435), and push notifications reach the person as D436 sealed pushes (§5.3) | Proposed |
| D-SF-9 | **Approvals reuse §19 and §21 unchanged.** Every agent skill declares a risk (`read`, `write`, `destructive`) in its Agent Card; the org's policy decides which need an approval; the decision is a signed proof from the person, never from Loams Bot, never from the agent that asked (§8) | Proposed |
| D-SF-10 | **Identity: each agent is an `agent` principal** (§19 §5.1) with a policy, a 15-minute token cap and a suspend switch. Loams Bot exchanges the user's token for an attenuated, actor-chained token per agent (RFC 8693, `act` = `user → loams-bot → plane-agent`); Agent Cards declare an OAuth 2.0 scheme whose authorization server is the Loams gateway, and are signed with JWS (RFC 7515, canonicalised by RFC 8785) by the instance key (§6) | Proposed |
| D-SF-11 | **Every agent step is traced twice.** OTel GenAI spans (from the AI gateway and the durable steps, §21 §6.6) carry `a2a.task_id`, `a2a.context_id`, `factory.run_id` and `resonate.promise_id`; a collector sends the LLM-shaped spans, with content, to **Langfuse** and every span, log and metric, without content, to **OpenObserve**, with one W3C `traceparent` across A2A hops (§9) | Proposed |
| D-SF-12 | **MCP is for tools, A2A is for delegation.** A tool call is stateless and returns; a delegation is a stateful task with a lifecycle. Loams Bot talks only A2A to the platform agents. Each platform agent uses REST, and MCP where it helps, inside its own boundary. §30's MCP servers are unchanged; Loams Bot adds no MCP tool that writes (§5.6) | Proposed |
| D-SF-13 | **The factory loop is a Resonate workflow, `factory.run`**, one per signal: intake, triage, plan, fix, review, deploy, observe, close. Each stage is a durable step with an idempotency key, a budget check and a gate (§10) | Proposed |
| D-SF-14 | **Safety rails are in the engine, not the prompt**: per-run and per-org budgets (tokens, money, wall time, attempts, open PRs), a **kill switch** by scope (a run kill cancels that run's tasks and workflows and suspends nothing; an agent kill also suspends that agent's principal; an org kill suspends all of the org's agent principals), loop-depth and cooldown limits against feedback storms, and **no auto-merge and no auto-deploy by default**: every merge and every deploy needs an approval unless the policy lists the repository, path or service and the environment is unprotected (§10.1, §11) | Proposed |
| D-SF-15 | **A factory run is a record**: a Live table `factory_runs`, a stream `factory_events`, audit events as OTel logs (D100), and console pages (runs, run detail, approvals, agents, budgets). The record links every artifact: the Zulip thread, the Plane issue, the Forgejo PR, the deploy, the traces (§12) | Proposed |
| D-SF-16 | **Open-core split (D220 stands).** Open, in this repository: the A2A host and adapters, the five agents, the cordis plugins, the native desktop panels and harness, the Loams Bot service and clients, `loams.collab.v1`, and a **single-organisation factory you can self-host**. Commercial, in `loam-platform`: the hosted multi-tenant factory, managed agents and model costs, the marketplace and its listing, billing and metering (§14) | Proposed |
| D-SF-17 | **The apps run unmodified, from official images pinned by digest**, as separate services: Zulip (Apache-2.0), Plane Community Edition (AGPL-3.0), Forgejo (GPL-3.0-or-later), GlitchTip (MIT), OpenPanel (AGPL-3.0), Langfuse's MIT tree with `ee/` never enabled, OpenObserve's open-source edition (AGPL-3.0). Licences and what each demands are in §4 | Proposed |
| D-SF-18 | **Coding is a skill of the Forgejo agent**, not a sixth public agent: `propose_patch` starts a sandboxed coding session on a workspace branch (§15 D24, §36 Loams Git) and returns a patch the Forgejo agent turns into a branch and a PR. The model is configurable; the default routes DeepSeek through the AI gateway (§13) | Proposed |
| D-SF-19 | **Where it appears in the apps.** Desktop (a zeron fork): Loams Bot is a **new `Harness` in zeron's harness crate** over `loams.bot.v1`, so zeron's conversation, composer, sidebar and trajectory UI show Loams Bot threads, plus native panels for the collab objects, runs and approvals. Browser console: `@loams/plugin-bot` (the DeepSeek harness's conversation UI patterns ported to cordis) and one plugin per app. Phones: native chat, run, approval and issue views over `loams.bot.v1`, `loams.collab.v1` and `loams.factory.v1`. Deep links are `loams://app/…`, `loams://bot/…`, `loams://factory/…` and navigate only (§3.5, §13) | Proposed |
| D-SF-20 | **The marketplace is commercial and private** (`loam-platform` doc 05): Loams Software Factory is listing #1; the open repository ships only the installable **package** (a Helm chart, a catalog patch and a manifest) that the listing installs, so a self-hoster installs the same thing by hand (§14) | Proposed |

## 2. Goals and non-goals

### 2.1 Goals

1. **One place for a team's loop.** Someone using the console, the desktop app or a phone sees chat, issues, code review, errors and analytics without leaving Loams, and without Loams re-implementing any of those apps.
2. **One chat that does things.** "Why did checkout errors spike?" becomes a triage thread, an issue and a draft fix, each performed by the right agent, each visible, each stoppable.
3. **Autonomy with a short leash.** Agents act only through their own principals, inside budgets, behind approvals the person signs, with a kill switch and a complete record.
4. **Everything traced.** Any agent step can be opened in Langfuse (what the model saw and did) and in OpenObserve (how the request moved through the system).
5. **Buy, not build** (user preference). Plane, Zulip, Forgejo, GlitchTip, OpenPanel, Langfuse and OpenObserve are used as they are; the A2A SDKs are the official ones; the workflow engine is §21's Resonate.
6. **Self-hostable by one organisation** (D220), with the marketplace as a convenience for those who do not want to run it.

### 2.2 Non-goals

- **No patched apps, no forks** (§4, D-SF-17). A feature the apps lack is built as a panel or an agent skill.
- **No new chat product.** Loams Bot is the harness's chat with a different set of agents; it does not replace Zulip, which is the team's chat.
- **No A2A on the phones.** Kotlin and Swift have no official SDK, and D420 already gives the apps one protocol (§5.1).
- **No autonomous production changes by default** (D-SF-14). Removing the gates is an organisation policy that an owner sets, recorded in the audit log.
- **No second durability layer.** The loop's state is §21's; the apps keep their own data.
- **No analytics product of our own.** The analytics agent reads OpenPanel.
- **No mobile Langfuse, OpenObserve or Forgejo diff viewer.** Phones get status, summaries and deep links (§3.5).

## 3. Embedding the apps

### 3.1 The apps and how Loams reaches them

| App | Role in the loop | Licence | Free-edition SSO | API Loams uses | Webhooks out |
|---|---|---|---|---|---|
| Zulip ("Slack") | Triage and discussion; streams and topics (threads) | Apache-2.0 | Generic OIDC and SAML, free self-hosted | REST (`/api/v1`), bot users, event queues | Outgoing webhooks, or the event queue |
| Plane ("Jira") | Issues, cycles, modules | AGPL-3.0, Community Edition | Gitea, GitHub, GitLab, Google OAuth; generic OIDC and SAML are paid (§22 §5) | REST API v1 with API keys **(verify the cycle and module endpoints, SF2 Task 0)** | Webhooks |
| Forgejo ("GitHub") | Repositories, PRs, Actions CI | GPL-3.0-or-later | Native OIDC with group-to-team mapping | REST (`/api/v1`, Gitea-compatible), fine-grained tokens | Webhooks |
| GlitchTip (phase 2) | Errors | MIT | Generic OIDC | Sentry-compatible REST (`/api/0`) | Alerts to webhooks |
| OpenPanel (phase 2) | Product analytics | AGPL-3.0 | GitHub and Google only, so forward-auth | Read API with client credentials **(verify, SF5 Task 0)** | None needed (the agent polls) |
| Langfuse 4 (phase 2) | Agent and LLM traces, evaluations | MIT, `ee/` commercial **(verified)** | Generic OIDC through `AUTH_CUSTOM_*` **(verify)** | Public REST API, OTLP at `/api/public/otel` **(verify path, SF5 Task 0)** | None |
| OpenObserve (phase 2) | Distributed traces, logs, metrics | AGPL-3.0; enterprise tier is commercial **(verified)** | **None in the open-source edition** (SSO, RBAC and audit are enterprise) **(verified)** | OTLP at `/api/<org>/v1/traces` (and logs, metrics), search API | Alerts to webhooks |

**Langfuse 4.** A v4 line exists **(verified)**: Langfuse describes v4 as one open-source platform for tracing, prompts, evaluation and experiments, on a wide, immutable observations table in **ClickHouse** (ClickHouse acquired Langfuse in January 2026; Langfuse already ran on it); the SDKs are built on OpenTelemetry and the server accepts OTLP traces. The product features were all open-sourced under MIT on 2025-06-04, and what remains commercial in `ee/` includes SCIM, audit logs, data-retention policies, project-level RBAC, protected prompt labels, server-side data masking, UI customisation, organisation creators and the organisation- and instance-management APIs (per Langfuse's licence-key documentation; SF5 Task 0 re-reads the list). Server-side masking being commercial is why our masking runs in the collector (§9.1). A third-party page lists v4.38.0 as of 2026-09-17 **(verify the current release at SF5 Task 0)**. It needs Postgres, ClickHouse, Redis and an S3 bucket; RustFS serves the last. Phase 2 runs it as an optional profile.

### 3.2 Embed or API-driven native panels, per app (D-SF-2)

The test for each app: **does the loop read or write it, and is the app's UI too deep to rebuild?** The loop's objects get native panels, so Loams Bot's cards, the console's overview and the phones can show them without a web view. Everything deeper is the app's own UI, embedded.

| App | Native panels (`loams.collab.v1`: cordis in the browser, GPUI on the desktop) | Full app UI (browser console: framed; desktop: system browser or sidebar browser) | Mobile (native) | Why |
|---|---|---|---|---|
| **Zulip** | Unread and mention count, the factory triage stream's topics, a thread view with reply box (read, post) | The full web app, in an isolated pane | Notifications (sealed push, D436), thread digest cards in chat, deep link to Zulip's official mobile app or the browser | Chat is the app's whole value; its UI is the product. Native is only for the loop's own thread |
| **Plane** | Issues (list, detail, state, assignee, labels), cycles (progress, scope), create and comment | Full Plane for boards, modules, views, settings | Native issue list and detail, cycle summary, create and comment, deep link to the browser | Plane's UI is large; the loop needs the issue and the cycle. Reading through the API keeps one OpenFGA check (§3.6) |
| **Forgejo** | Repositories, branches, PRs (status, checks, reviewers), CI runs and logs tail, merge button (gated) | Full Forgejo for diffs, code browsing, review comments, settings | Native PR list, status and checks, summary, approve-and-merge through the approval service; the diff opens in the browser | A diff viewer is not worth rebuilding; the status of a PR is |
| **GlitchTip** | Issue list, issue detail (stack trace, breadcrumbs, release, first and last seen), resolve and ignore | Full GlitchTip for projects, releases, settings | Native issue list and detail, push on a new or regressed issue | The loop starts here; the native list is what Loams Bot cards link to |
| **OpenPanel** | Metric tiles (events, active users, a funnel) from the analytics agent's queries; no editor | The OpenPanel dashboard, behind forward-auth | Tiles only, deep link | OpenPanel's explorer is what people open; tiles are what the factory quotes |
| **Langfuse 4** | A trace summary per agent step (from Loams' own event stream: model, latency, cost, score), not a trace viewer | Langfuse's trace and session pages, opened at a given trace id | A step timeline inside the run view; deep link to the browser | Langfuse's tree and diff views are the point; Loams links into them |
| **OpenObserve** | Nothing native: a "traces for this run" link | The full UI at a search pre-filtered by `factory.run_id`, behind forward-auth | A deep link only | Its SPA is the value; there is no API surface the loop needs beyond ingest |

### 3.3 Edge, CSP and X-Frame-Options (D-SF-3)

All seven apps send headers that forbid framing by default or by convention (Forgejo's `[cors] X_FRAME_OPTIONS` defaults to `SAMEORIGIN` **(verified)**; the others are checked in SF1 Task 2 and SF5 Task 2). The apps stay unmodified, so the edge decides:

| Item | Setting |
|---|---|
| Domain layout | The instance's apps live at `chat.<domain>`, `plane.<domain>`, `git.<domain>`, `errors.<domain>`, `analytics.<domain>`, `llm.<domain>` and `obs.<domain>` (names are set by the Helm values), all siblings of `console.<domain>` and `auth.<domain>`, so every cookie is **same-site** (Q462 decides subdomains against path prefixes, which several of these apps cannot be served under) |
| Frame headers | For each app route, the edge removes `X-Frame-Options` and replaces any `Content-Security-Policy` `frame-ancestors` with `frame-ancestors 'self' <console origin>`. The rest of the app's CSP is left alone. This serves the **browser console** only: the desktop and phones load apps as top-level pages |
| Console `frame-src` | The browser console's CSP `frame-src` is the instance's app origins, from the app registry (below) |
| Cookies | Not rewritten by default (one rule, also in D-SF-3): sessions are the apps' own and same-site, so no cross-site cookie is needed; where an image emits `SameSite=None`, that route's edge rule rewrites it to `Lax` and the exception is recorded in `images.lock`. CI asserts that no cookie reaches the browser as `SameSite=None`. `Secure` and `HttpOnly` stay as the apps set them |
| Forward-auth apps | OpenPanel and OpenObserve are routed through Authentik's proxy outpost; the outpost sets a trusted header (`X-authentik-username`) or injects HTTP basic auth to the app's single service account (§3.4). **Trust boundary:** the ingress strips or overwrites any client-supplied `X-authentik-*` header before the outpost sets its own; OpenPanel and OpenObserve accept connections only from the outpost (a network policy leaves no other route to their ports); the outpost-to-app hop uses TLS (HTTPS or the cluster's mTLS) because it carries credentials |
| Webhooks and API | `/api` and webhook paths are not framed and are reached by Loams' services on the cluster network, never through the console |
| Verification | CI starts each app from its pinned image behind the edge and asserts: it frames from the console origin and from nowhere else, and loads top-level; `frame-ancestors` is exactly the configured list; no app cookie is `SameSite=None` |

**The app registry.** `GetInstance` (AP0) gains an `apps` field, and `loams.collab.v1.ListApps` returns, per app: `id`, `kind` (`zulip`, `plane`, `forgejo`, `glitchtip`, `openpanel`, `langfuse`, `openobserve`), `base_url`, `embed_url`, `api_capable`, `deep_link_templates` and `phase`. A plugin activates only if its app is listed, as `rpc.*` services activate by `api_versions` (D424).

### 3.4 Sessions, isolation and SSO per surface

**Browser console (cordis).** A sandboxed `<iframe sandbox="allow-scripts allow-same-origin allow-forms allow-popups allow-popups-to-escape-sandbox allow-downloads">` per app (`allow-same-origin` is the app's own origin, which is not the console's, so it cannot reach the console). The iframe loads `<app>/` and the app signs the user in by OIDC against Authentik, silently when the browser has an Authentik session (`prompt=none`), so there is one visible sign-in. The console never passes a Loams token to a frame. Messages from frames are ignored; the app registry decides what loads.

**Desktop (native, a zeron fork; §37 amended, D480–D499).** The desktop app has no web runtime of its own (zeron draws with GPUI and its architecture lists no webview crate; its README mentions a sidebar browser, which SF1 Task 0 inspects), so app UIs arrive in three tiers:

1. **Native panels** for the objects the factory touches (issues, PRs, threads, errors, tiles, runs, approvals), drawn in GPUI from `loams.collab.v1` and `loams.factory.v1` through a shared Rust client crate (`loams-apps-client`, §7). This is the default and the only tier the factory needs.
2. **System browser** for depth: "Open in browser" on every panel and card opens the app's URL top-level, where the person's Authentik session already exists, so SSO is the browser's and nothing is stored by the desktop app. Opened through the OS URL handler after the URL is checked against the app registry's origins.
3. **An in-app sidebar browser, optional and spike-gated.** If a wry-based webview can be hosted inside GPUI on macOS, Windows and Linux (the community `gpui-wry` crate is the candidate **(verify)**), the app offers a docked pane that loads an app's URL **top-level** (never in an iframe, so `X-Frame-Options` does not apply), with one data store per (environment, app), a navigation allowlist (the app's origin and Authentik's; everything else goes to the system browser), no native bridge exposed to the page, and downloads refused. Sign-in happens in the pane once per profile; WebAuthn inside embedded webviews varies by OS **(Q461)**. If the spike fails on an OS, that OS has tiers 1 and 2 only, and the plan is unchanged.

**Deep links** `loams://app/<env>/<app>/<path>` are registered by the native app (OS registration is §37's amendment), parsed against the app's allowlist of prefixes, and open the native panel for that object if one exists, else the system browser. **A deep link navigates, never acts** (D432).

**Mobile.** No embedded web views for the apps (D-SF-2). Links open in the system browser: Custom Tabs on Android, which share the browser's cookies, and the default browser on iOS (`UIApplication.open`), because `SFSafariViewController` does not share Safari's cookies **(verify, AP3)**. The user's Authentik session is the browser's, so one sign-in covers every app.

**Forward-auth apps.** For OpenPanel and OpenObserve the outpost authenticates the person against Authentik, checks membership in the OpenFGA-projected group (`factory-viewers`), and logs in to the app as one shared service user per app with a read-only role where the app has roles. This is honest about the gap: **those two apps have one user, not per-person identity** (Q475), so audit of "who looked" is Authentik's log, not the app's.

### 3.5 What each plugin provides

In the **browser console** all are cordis plugins in `web/plugins/*` (D422), `first-party`, Apache-2.0. The **desktop** has the same surfaces as native GPUI panels (below the table). New slots are added to §37 §5.5's catalog by SF1 Task 1.

| Plugin | Slots and services | Provides |
|---|---|---|
| `@loams/plugin-embed` | Provides the `embed` service and the `embed.pane` slot (keyed by app id, props `{ app, path, environment }`) | The iframe host (browser console), the registry, the toolbar, the deep-link router, the "unreachable or not signed in" states |
| `@loams/plugin-zulip` | `console.page` (`/chat`), `environment.overview.card`, `app.panel#zulip`, `bot.card#zulip.thread`, `embed.pane#zulip`, `palette.command` | Unread and mentions card, the triage thread panel, native thread cards |
| `@loams/plugin-plane` | `console.page` (`/issues`, `/cycles`), `app.panel#plane`, `bot.card#plane.issue`, `embed.pane#plane` | Issue and cycle lists and detail, create and comment, the cycle progress card |
| `@loams/plugin-forgejo` | `console.page` (`/code`), `app.panel#forgejo`, `bot.card#forgejo.pr`, `embed.pane#forgejo`, `approval.renderer#forgejo.merge` | Repository, PR and CI panels, the merge approval renderer, links into diffs |
| `@loams/plugin-glitchtip`, `-openpanel`, `-langfuse`, `-openobserve` (phase 2) | As above | §16 |
| `@loams/plugin-bot` | `console.page` (`/bot`), `shell.overlay` (a docked chat), `bot.message.renderer`, `bot.card`, `palette.command` | The chat (§13) |
| `@loams/plugin-factory` | `console.page` (`/factory/*`), `operation.detail#factory.run`, `environment.overview.card` | Runs, run detail, agents, budgets, the kill switch (§12) |

New slots: `embed.pane` (keyed by app id), `app.panel` (keyed by `<app>` or `<app>.<panel>`; props `{ environment, query }`), `bot.message.renderer` (keyed by part kind), `bot.card` (keyed by artifact kind; props `{ artifact, onAction }`), and `factory.stage.detail` (keyed by stage id).

**Desktop native surfaces (D-SF-2, D-SF-19).** Crates in the desktop fork, each a set of GPUI views registered in zeron's shell the way its own conversation and diff panes are: `loams-ui-collab` (Zulip thread, Plane issue and cycle, Forgejo PR and CI, GlitchTip error and OpenPanel tile panels, and the artifact cards), `loams-ui-bot` (the cards inside the conversation, the `@agent` mentions, the Loams Bot sidebar section), `loams-ui-factory` (runs, run detail with the stage graph, approvals, agents, policy and budgets, the kill switch), and `loams-harness-bot` (the `Harness`, §13). They read through `loams-apps-client`.

**Mobile native surfaces (D-SF-2, D-SF-19).** Each is a screen over Connect services, in SwiftUI and Compose: the chat; the run list and run timeline; approvals (existing `loams.approvals.v1`); the issue list and detail (Plane); the PR list and checks (Forgejo); the error list (GlitchTip, phase 2); analytics tiles (phase 2); an apps screen with deep links into each app's web UI. Phones show data the engine already fetched, cached with freshness timestamps (D437).

### 3.6 Native panels read through Loams, with OpenFGA

A native panel never calls an app directly. It calls `loams.collab.v1` (served by `loams-collab`, §6.3), which:

1. authenticates the user's Loams token and resolves the principal;
2. asks OpenFGA (the one model of §22 §7) which objects of that type the user can read (`ListObjects`, cached for seconds, as §22 §8.1 does);
3. calls the app's API with a **service credential** held by the credential broker (never visible to the browser, the phone or the model) and filters the result;
4. returns typed messages (`Issue`, `PullRequest`, `Thread`, `CheckRun`, `ErrorIssue`, `MetricSeries`).

Writes (comment, create, merge) take an idempotency key, go through the same check, and are audited. A write that §8's policy marks as needing approval returns an operation in `awaiting_approval`, not an error.

## 4. Licences, and what running each app unmodified means (D-SF-17)

Verified 2026-10-02 unless marked; §22 §4 has the earlier matrix and the reasoning, which stands.

| Component | Licence | Used as | What "unmodified" means for Loams |
|---|---|---|---|
| Zulip | Apache-2.0 (verified in §22) | Official image, own service | Nothing is owed beyond notices. Free self-hosted Zulip carries SSO and group sync; the paid plans add support and push-notification service limits, which we do not use (our push is D436) |
| Plane | **AGPL-3.0**, Community Edition; paid editions are a separate commercial build with OIDC and SAML SSO, among other features (§22) | Official CE image | **AGPL §13 binds whoever modifies Plane.** Unmodified, we owe only the source link Plane's own UI shows. Embedding through a frame and driving it through its API do not make Loams a derivative work (separate processes over HTTP). We never enable, copy or bypass paid-edition code or licence keys. Plane's marks are not ours: listings say "works with Plane" |
| Forgejo | **GPL-3.0-or-later** | Official image | Running and framing it triggers nothing. Redistributing an image (an air-gapped bundle) needs its licence text and source offer. Forgejo Runner is also GPL-3.0-or-later |
| GlitchTip | MIT | Official image | Notices only |
| OpenPanel | **AGPL-3.0** (§22 §13b) | Official image | As Plane. Forward-auth means no patch for SSO. The fork onto Loams's Iceberg store (D-SC-15) is a separate decision and would carry AGPL obligations |
| Langfuse | MIT, **except `ee/`** (commercial) (verified) | Official image; `ee/` features are never switched on | Nothing is owed under MIT beyond the notice. We neither use nor rely on any `ee/` feature (SCIM, audit logs, retention policies, project RBAC, protected prompt labels, server-side masking, UI customisation, organisation creators, the management APIs); audit comes from Loams and Authentik |
| OpenObserve | **AGPL-3.0** open-source edition; an Enterprise Edition under a commercial agreement adds SSO, RBAC, audit trail, extended retention and federated search (verified) | Official image, open-source edition | As Plane. Because the open edition has no SSO, we put it behind forward-auth (§3.4) and do not pretend it has per-user access control |
| A2A (spec and SDKs) | Apache-2.0; Linux Foundation project (verified) | Libraries linked into Loams (`a2a-lf` crates for Rust) | Compatible with D11 (no AGPL, BSL, SSPL or ELv2 in linked code). Pinned; the Rust SDK's maturity is a risk (§15) |
| Authentik | MIT, except `authentik/enterprise/` (§22 §4.4, §38) | Open-source edition (D404) | The proxy outpost used for forward-auth is in the open edition (verify in SF1 Task 2) |
| DeepSeek harness (desktop, mobile) | MIT | Patterns, not copied by default (D421): the server-side agent loop (SF3) and the browser console's chat UI | Any copied file keeps its notice in `THIRD_PARTY_NOTICES.md` |
| zeron | MIT (verified) | The base of the desktop app: a fork (§37, amended) | Notices kept; its GPUI fork is Apache-2.0 and Loro MIT (SF3 Task 0 checks the whole tree with `cargo deny`) |

**Offering the apps hosted (commercial).** AGPL and GPL allow running unmodified copies for paying customers when the source remains available; the hosted marketplace listing therefore links each app's source and licence. Whether any app's trademark or commercial terms restrict *reselling hosted instances* is a legal question we do not answer here (owner action in `loam-platform` doc 05).

## 5. Loams Bot and A2A

### 5.1 A2A as it stands (verified 2026-10-02)

| Item | Finding |
|---|---|
| Governance | Originally Google's, donated to the **Linux Foundation** in 2025; a Technical Steering Committee with AWS, Cisco, Google, IBM Research, Microsoft, Salesforce, SAP and ServiceNow; spec and SDK repositories under `a2aproject`, all Apache-2.0 |
| Version | **1.0.0** is current (stable since March 2026). Clients send an `A2A-Version` header; a server treats an empty header as 0.3 and answers `VersionNotSupportedError` for an unsupported one |
| Bindings | JSON-RPC 2.0 over HTTP, gRPC, and HTTP+JSON/REST, all defined to be functionally equivalent over one canonical protobuf data model |
| Operations | Send Message, Send Streaming Message, Get Task, List Tasks (cursor pagination), Cancel Task, Subscribe to Task, Create, Get, List and Delete Task Push Notification Config, Get Extended Agent Card |
| Task states | `TASK_STATE_SUBMITTED`, `_WORKING`, `_COMPLETED`, `_FAILED`, `_CANCELED`, `_REJECTED` (terminal) and `_INPUT_REQUIRED`, `_AUTH_REQUIRED` (interrupted). Enums are `SCREAMING_SNAKE_CASE` in v1; streaming events are wrapped (`statusUpdate`, `artifactUpdate`) and the `kind` and `final` fields are gone |
| Agent Card | At `/.well-known/agent-card.json`; identity, skills, capabilities (`streaming`, `pushNotifications`, `extendedAgentCard`), security schemes (API key, HTTP, OAuth 2.0, OpenID Connect, mutual TLS), interfaces with their own protocol versions, extensions, and an optional **JWS signature** (RFC 7515, canonicalised with RFC 8785) |
| Multi-tenancy | A `tenant` field on every request; an agent names its default tenant per interface |
| SDKs | Official: **Python, JavaScript, Java, .NET, Go, Rust**. The Rust repository (`a2aproject/a2a-rs`, Apache-2.0, crates suffixed `-lf`) targets v1, is built on axum for the REST and JSON-RPC bindings and on tonic for gRPC, and requires Rust 1.85+; a third-party summary calls Rust "in validation". **No official Kotlin or Swift SDK**: community ones exist (JetBrains Koog's A2A feature module; `a2a-swift` from Victory Apps) |
| Not to be confused with | **ACP**, the Agent Client Protocol, which the harness already implements (`packages/acp`, `subagent-acp`) for editor-to-agent automation. It is a different protocol with a different purpose; the harness's `subagent-acp` is the pattern for `subagent-a2a` (§5.2), not a substitute |

### 5.2 Shape (D-SF-6, D-SF-7)

```
 Loams Desktop (native, zeron fork)   browser console (cordis)    iOS / Android (native)               external A2A or MCP clients
 LoamsBotHarness ─┐                   @loams/plugin-bot ─┐       chat, runs, approvals ─┐            │ A2A 1.0 (JSON-RPC / REST + SSE)
   loams.bot.v1 (Connect, Rust client)  rpc.bot (Connect)     loams.bot.v1 (Connect)        │            │
                    ▼                                        ▼                      ▼
 ┌───────────────────────────────── Loams Bot (loams-bot) ─────────────────────────────────────────┐
 │ chat sessions as durable executions (§21, D24) · harness agent loop (headless) · model: DeepSeek  │
 │ default via the AI gateway · `subagent-a2a` provider on `ctx.subagents` · policy, budgets, trace   │
 │ exposes: loams.bot.v1 (Connect)  +  an A2A server card for "loams-bot" (Q469)                        │
 └──────┬───────────┬────────────┬─────────────┬──────────────────┬────────────────────────────────┘
        │ A2A       │ A2A        │ A2A         │ A2A              │ A2A      (client side: a2a-lf)
        ▼           ▼            ▼             ▼                  ▼
   zulip agent  plane agent  forgejo agent  glitchtip agent  analytics agent     each: its own principal,
   (SF2)        (SF2)        (SF2)          (SF5)            (SF5)               a signed Agent Card
        │           │            │             │                  │
        ▼           ▼            ▼             ▼                  ▼             credential broker injects the
   Zulip API    Plane API    Forgejo API   GlitchTip API    OpenPanel API         app credential per call
        └───────────┴────────────┴──────┬──────┴──────────────────┘
                                         ▼
        OTel GenAI spans ──► collector ──► Langfuse 4 (LLM view)  +  OpenObserve (all signals)  +  Loams (D73)
```

- **Loams Bot's agent loop is the harness's**, run headless on the server (the harness's SDK server half) rather than inside each app. It is a durable execution (D24), so a phone that locks or a laptop that sleeps loses nothing, and the same session is visible from every device.
- **Delegation is by natural language over A2A.** Loams Bot sends the plane agent a `Message` ("create an issue for the checkout 500s, label `factory`, link thread X"); the agent chooses its own skills. Loams Bot does not call a Plane tool. The agents' skills are visible to the model as tool descriptions derived from their Agent Cards, so routing is the model's choice; `@plane` in the composer forces it.
- **`subagent-a2a`** is a new provider for the harness's `ctx.subagents`, patterned on `subagent-acp`; it maps `start`, `continue` and `cancel` to `SendMessage` (with `taskId`), `SendMessage` follow-up, and `CancelTask`, and maps the harness's `userQuestions` and approval services to `INPUT_REQUIRED`.
- **Client protocol.** `loams.bot.v1` (Connect, AP0 rules: unary and server-streaming, snapshot-then-changes, heartbeat every 15 s, resume cursors, idempotency keys) mirrors the A2A `Message`, `Part`, `Task` and `Artifact` types by **importing A2A's own protobuf** where it can (Q467), so a client and an agent speak one vocabulary and A2A conformance is a property of the data, not a translation.
- **A2A bindings in our servers.** Agents serve **HTTP+JSON and JSON-RPC with SSE**, on axum (the same server stack as connect-rust, D128). gRPC is not served: it would add tonic beside connect-rust for no client that needs it. Loams Bot's client side uses the same two bindings.

### 5.3 Task lifecycle, streaming and push (D-SF-8)

| A2A | Loams |
|---|---|
| `contextId` | The chat thread id (or the factory run id, §10). One context spans many tasks |
| `taskId` | The §21 operation id (`op-…` from the idempotency key, D146). `GetTask` is `GetOperation` plus the status message; `ListTasks` pages over operations of the agent |
| `SUBMITTED` / `WORKING` | Operation `pending` / `running` |
| `INPUT_REQUIRED` | Operation `awaiting_input` (a question to the person, rendered as a card) or `awaiting_approval` (an approval promise; the status message's `data` part carries `{ approval_id, revision }`). Loams Bot relays the question to the person and answers only one the agent marked `answerable_by_orchestrator`; it never answers or decides an approval (D-SF-9, §8) |
| `AUTH_REQUIRED` | The agent's app account is not linked or its token was revoked; the status message carries a link to the console's "connect app" page. This is an owner/admin action, not the chat user's |
| `COMPLETED` | Operation `succeeded`; `artifacts` hold results (an issue key and URL, a PR, a summary), each with a `kind` that picks a card renderer |
| `FAILED`, `REJECTED`, `CANCELED` | `failed` (with the stable `reason`), `rejected` (policy or an approval denied; terminal), `canceled` |
| Streaming | Agent to Loams Bot: `SendStreamingMessage` or `SubscribeToTask` (SSE). Loams Bot to clients: `loams.bot.v1.Watch`, a Connect server stream. An agent that is mid-tool-call emits `statusUpdate` events with a short message, and artifacts stream as `artifactUpdate` chunks |
| Push notifications | An agent's tasks outlive any stream (a CI run, an approval). Loams Bot registers an A2A **push notification config** (`CreateTaskPushNotificationConfig`) on each long task, with a webhook at Loams Bot's receiver and the standard `TaskPushNotificationConfig.authentication` (scheme `Bearer`, credentials a random per-task secret that Loams Bot stores hashed against the registered `(agent, task)` pair; the agent sends it as the `Authorization` header and the receiver rejects any other agent, task or secret). A Loams-only JWS signature of the body (`X-Loams-Signature`) is an extension that no conformance claim depends on. A state change becomes the CloudEvent `io.loams.dev.bot.task.updated.v1`; the notifier turns it into D436's sealed push for the right devices |
| Resubscribe | A client that reconnects calls `Watch` with its cursor; Loams Bot re-subscribes to any task whose stream dropped (`SubscribeToTask`) |
| Idempotency | `messageId` is deterministic (`op id ‖ step`) so a replayed durable step resends the same message, and the agent deduplicates on it |

### 5.4 Agent cards (D-SF-7)

Each agent serves one card, signed by the instance key and carrying the instance JWKS key id. Example, the plane agent (abridged; `risk` is a Loams extension, URI `https://loams.dev/a2a/ext/risk/v1`):

```json
{
  "name": "plane-agent",
  "description": "Issues, cycles and modules in this instance's Plane.",
  "version": "1.0.0",
  "supportedInterfaces": [
    { "url": "https://agents.<domain>/plane/a2a", "protocolBinding": "JSONRPC", "protocolVersion": "1.0" },
    { "url": "https://agents.<domain>/plane/a2a/rest", "protocolBinding": "HTTP+JSON", "protocolVersion": "1.0" }
  ],
  "capabilities": { "streaming": true, "pushNotifications": true, "extendedAgentCard": true },
  "securitySchemes": { "loams": { "oauth2SecurityScheme": { "flows": { "clientCredentials": {
        "tokenUrl": "https://<gateway>/api/v1/oauth/token",
        "scopes": { "plane:read": "Read issues and cycles", "plane:write": "Create and update issues" } } } } } },
  "securityRequirements": [ { "schemes": { "loams": { "list": ["plane:read"] } } } ],
  "defaultInputModes": ["text/plain", "application/json"],
  "defaultOutputModes": ["text/plain", "application/json"],
  "skills": [
    { "id": "issues.search", "name": "Search issues", "description": "Find issues by text, label, state or cycle.",
      "tags": ["read"], "examples": ["open issues labelled factory"] },
    { "id": "issues.create", "name": "Create an issue", "description": "Create an issue, or return the existing one for the same run.",
      "tags": ["write"], "securityRequirements": [ { "schemes": { "loams": { "list": ["plane:write"] } } } ] },
    { "id": "issues.close_bulk", "name": "Close many issues", "description": "Close more than five issues at once (needs an approval).",
      "tags": ["destructive"], "securityRequirements": [ { "schemes": { "loams": { "list": ["plane:write", "plane:destructive"] } } } ] },
    { "id": "cycles.summary", "name": "Summarise a cycle", "description": "Progress, scope and blockers of a cycle.", "tags": ["read"] }
  ],
  "signatures": [ { "protected": "…", "signature": "…" } ]
}
```

Field names follow the A2A 1.0 protobuf JSON mapping (`oauth2SecurityScheme`, `securityRequirements` with a `schemes` map, a required skill `description`); SF2 Task 3 validates this example against the pinned `a2a.proto` and fixes any difference.

The **extended card** (after authentication) lists the instance's project ids and the principal's own limits. A client verifies the signature against `/.well-known/jwks.json` before trusting the card; an unsigned or unverified card is refused (SF2 Task 3).

### 5.5 The five agents

| Agent | Phase | Skills (risk) | Platform calls |
|---|---|---|---|
| `zulip` | 1 | `streams.list` (read), `threads.read` (read), `threads.post` (write), `threads.open_triage` (write; creates `#factory-triage > run-<id>`), `threads.summarise` (read) | Zulip REST as a bot user; the event queue for replies |
| `plane` | 1 | `issues.search`, `issues.get`, `cycles.summary` (read); `issues.create`, `issues.update`, `issues.comment`, `cycles.add_issue` (write); `issues.close_bulk` (destructive) | Plane REST API v1 with an API key of the agent's Plane member |
| `forgejo` | 1 | `repos.search`, `prs.get`, `ci.status`, `ci.logs_tail` (read); `branches.create`, `prs.open`, `prs.comment`, `propose_patch` (write); `prs.merge`, `branches.delete_protected` (destructive) | Forgejo REST with a fine-grained token of the agent's user; Actions run in the Forgejo Runner |
| `glitchtip` | 2 | `issues.search`, `issues.get`, `events.get`, `releases.list` (read); `issues.resolve`, `issues.ignore` (write) | GlitchTip's Sentry-compatible REST |
| `analytics` | 2 | `metrics.query`, `funnel.get`, `anomaly.check` (read) | OpenPanel's read API; no write skills |

### 5.6 MCP against A2A (D-SF-12)

| | MCP (§30, M1.6) | A2A |
|---|---|---|
| What it is | A model calls a **tool**: a function with a schema | An agent delegates a **task** to another agent that decides how |
| State | None (a call and a result); long jobs are an operation id the client polls | A task with a lifecycle, a context, interruptions and artifacts |
| Who is opaque | The tool is transparent: the model sees its schema | The remote agent is opaque: only its card and skills are visible |
| In Loams | Data tools on Loams (`search`, `sql`, `memory_write`, …), the bootstrap stdio server, and each platform agent's *own* tools | Loams Bot to platform agents; external A2A clients to Loams Bot |
| Rule | **Loams Bot never calls a platform tool.** A platform agent may use MCP or REST inside itself. Loams Bot's own model may use read-only Loams MCP tools (`search`, `sql`) to ground answers. No MCP tool writes to an app. §30 §12's rule that destructive tools are never MCP tools stands | Loams Bot talks A2A to platform agents only; external A2A clients (only with `--bot-a2a`) talk to Loams Bot; no write skill runs without the policy gate (§8) |

## 6. Identity, tokens and credentials (D-SF-10)

### 6.1 Principals

- **Loams Bot** is an `agent` principal (`loams-bot`) per organisation. It acts **for a user**: each chat session carries the user's token as the subject and Loams Bot as actor.
- **Each platform agent** is an `agent` principal with its own policy (the actions and the projects it may reach) and a 15-minute token cap (§19 §5.1). Suspending one revokes its tokens within the change feed's latency (§19 §5.4).
- **App identities.** The provisioning saga (§22 §7.3) creates, per agent, an app-native identity: a Zulip bot user, a Plane workspace member with an API key, a Forgejo user with a fine-grained token, a GlitchTip token, an OpenPanel client. They appear in each app as their own named users (`loams-plane-agent`), so every action in the app is attributable in the app too.

### 6.2 The token path

1. The user signs in at Authentik; the gateway issues Loams tokens (§37, amended for a native desktop; RFC 8693 exchange).
2. A chat message reaches Loams Bot with the user's access token.
3. For each A2A call, Loams Bot asks the gateway to **exchange** that token for one **audience-bound to the agent**, with `scp` the intersection of the agent's policy and the user's rights, a top-level `sub` that is the user, an `act` claim whose `sub` is the platform agent (the current actor) nesting `act` = `agent:loams-bot` (the prior actor), that is `act = { sub: agent:<name>, act: { sub: agent:loams-bot } }` (RFC 8693 §4.1), and an `aud` claim equal to the agent's interface URL (§19 §5.3 defines `env` as the environment audience; SF2 Task 4 adds `aud`, §18). Loams Bot is the exchange's client: the user's token is the `subject_token`, Loams Bot's own token the `actor_token`. TTL up to 15 minutes. It attenuates only (§19 §5.2 flow 3).
4. The agent verifies the JWT locally (signature, `aud`, `exp`, `env`), as every Loams gateway does, and the `Authorizer` still decides.
5. For the app call, the agent asks the **credential broker** for a short-lived call permit (a handle request that presents the agent token; it is not a second token exchange and mints no token); the broker holds the app secret (Dapr secret store, §24), checks OpenFGA, and performs or signs the call. **The app secret is never in the agent's memory, the model's context, a trace or a log** (§30's secret canary test is extended to the agents, SF2 Task 9).

### 6.3 Agent-to-agent authentication

Agent cards declare `oauth2` with the gateway as authorization server (§5.4). A2A's mutual-TLS scheme is available for in-cluster hops (Knative with the mesh's mTLS, §38), but the JWT is the contract, because the user chain must travel with the call.

## 7. Where the code lives (D-SF-16, D-SF-17)

| Piece | Where |
|---|---|
| `loams-a2a`: the A2A host library (server and client over `a2a-lf`, card signing and verification, task-to-operation mapping, push receiver) | This repository |
| `loams-agent-zulip`, `-plane`, `-forgejo`, later `-glitchtip`, `-analytics` | This repository |
| `loams-collab` (`loams.collab.v1`, the credential broker, OpenFGA filtering) | This repository |
| `loams-bot` (`loams.bot.v1`, the headless harness loop, `subagent-a2a`) | This repository |
| `loams-factory` (`loams.factory.v1`, `factory.run`, budgets, the kill switch) | This repository |
| `loams-apps-client` (generated connect-rust clients for AP0, `loams.collab.v1`, `loams.bot.v1`, `loams.factory.v1`), shared by the desktop and any Rust client | This repository |
| `web/plugins/{embed,zulip,plane,forgejo,bot,factory,…}` (browser console) | This repository |
| Native desktop: `loams-harness-bot`, `loams-ui-collab`, `loams-ui-bot`, `loams-ui-factory` in the zeron fork (the repository and directory are named by §37's amendment) | This repository or the desktop repository, as §37's amendment says |
| Mobile screens | `ostrium-labs/loams-mobile` (§37 D439) |
| The factory package (Helm chart `loams-factory`, catalog patch, manifest) | This repository, `deploy/factory/` |
| Crate names follow the current `operon-*` convention and the rename PR renames them (D33) | — |

## 8. Approvals and human-in-the-loop (D-SF-9)

1. **Risk is declared by the agent, enforced by Loams.** Each skill carries `read`, `write` or `destructive` in its card tags. The org's **factory policy** (a Live document, versioned, audited) maps `(agent, skill, environment)` to `allow`, `approve` or `deny`. Defaults: `read` allow; `write` allow in non-protected environments, approve in protected ones; `destructive` approve; `prs.merge` approve **in every environment**; deploy approve.
2. **The agent cannot skip the gate.** `approve` makes the agent's durable function wait on the approval promise (§21 §6.5); only a settled promise releases it. The *server-side* check is in the credential broker: a destructive call carries the approval id and the broker verifies it is settled and matches the call's hash. A prompt-injected agent that "decides" not to wait still cannot get the credential.
3. **The approval is §37's**: `loams.approvals.v1`, a server-rendered summary, a decision proof signed by the person's device key, the requester and the user an agent acts for unable to approve (Q432), a typed confirmation for destructive ones, 72-hour timeout, no always-allow. Loams Bot shows the approval as a card and opens the approval screen; it never carries the decision.
4. **Questions are not approvals.** `INPUT_REQUIRED` for a clarification is answered in chat; Loams Bot may answer a question itself only if the agent marked it `answerable_by_orchestrator` (for example "which project?" when the thread names one).
5. **Prompt-injection posture.** Text from apps (issue bodies, chat messages, error messages, PR descriptions) is untrusted data. Agents receive it in `data` parts tagged `untrusted`, the harness renders it in a quoted block, and no tool call is chosen *only* on it without the policy gate. Red-team fixtures are in SF2 Task 9 and SF4 Task 9.

## 9. Tracing and observability (D-SF-11)

| Layer | What is emitted | Where it goes |
|---|---|---|
| LLM calls | OTel GenAI spans from the AI gateway (aisix, §21 §6.6): model, tokens, latency, cost, and (policy-controlled) prompt and completion | Langfuse and OpenObserve (content only to Langfuse, §9.2) |
| Agent steps | One span per durable step, with `resonate.promise_id` and `resonate.origin` (§21), plus `a2a.task_id`, `a2a.context_id`, `a2a.agent`, `factory.run_id`, `factory.stage`, `enduser.id` (a hash of the principal id, never an email) | Both |
| A2A hops | W3C `traceparent` and `tracestate` as HTTP headers on every A2A request and push webhook, so a Loams Bot span parents the agent's | Both |
| App calls | A span per Zulip, Plane, Forgejo, GlitchTip and OpenPanel API call, with route and status, never bodies | OpenObserve |
| Services | OTLP logs and metrics from every Loams service (D73), Knative revisions, the apps' own OTLP where they emit it | OpenObserve and Loams |
| Factory events | CloudEvents `io.loams.dev.factory.*.v1` on the `factory_events` stream | Loams; the console reads them |

### 9.1 The collector

One OpenTelemetry Collector (per cluster; `loams dev` runs without) receives OTLP from every service and has two pipelines:

- `traces/llm` : a filter processor keeps every span that carries a `gen_ai.*` attribute **or** an agent-step attribute (`a2a.task_id` or `resonate.promise_id`), and their parents, so each agent step reaches Langfuse even when it made no model call; a **masking step** removes secret-canary matches and `untrusted` bodies over 2 KB before export, and the pipeline **fails closed** if masking cannot run (Langfuse's own server-side masking is an `ee/` feature we do not use); exporter `otlphttp/langfuse` receives only masked spans, at the Langfuse OTLP endpoint with the project's key pair as basic auth over TLS **(verify the path and headers, SF5 Task 3)**.
- `traces/all`, `logs`, `metrics`: a transform processor deletes `gen_ai.prompt`, `gen_ai.completion` and `gen_ai.*.content` attributes; exporter `otlphttp/openobserve` to `/api/<org>/v1/{traces,logs,metrics}` with basic auth (verified shape).

A third exporter sends the same stream to Loams's own OTLP ingest (D73, Q43) so the console's run view needs neither tool.

### 9.2 What content is captured (Q476)

Default: prompts and completions are captured **in Langfuse only**, in a project per environment, with **the collector as the only masking boundary** (§9.1: it drops values matching the secret canary's patterns and the `untrusted` data parts' bodies over 2 KB, and fails closed; Langfuse's own server-side masking is an `ee/` feature we do not use); **OpenObserve never receives content**. Production environments may turn content capture off per org. Either way the console's run view shows the step's summary from `factory_events`, which holds no model content.

## 10. The Loams Software Factory loop (D-SF-13)

### 10.1 Stages

```
        ┌──────────────────────────────────────────────────────────────────────────────────────────┐
        ▼                                                                                          │
 1 intake ──► 2 triage ──► 3 plan ──► 4 fix ──► 5 review ──► 6 deploy ──► 7 observe ──► 8 close ──┘
 signal       Zulip        Plane      Forgejo    approval     GitOps       GlitchTip,    summary,
 dedupe       thread       issue      branch,    (merge)      sync         OpenPanel,    new signal
 severity     + decision              PR, CI                              Langfuse,     if regressed
                                      (loop ≤N)                           OpenObserve
```

| # | Stage | Done by | Gate (default) | Durable step output |
|---|---|---|---|---|
| 1 | **Intake**: a signal arrives (a GlitchTip alert webhook; an analytics anomaly from the `analytics` agent's scheduled `anomaly.check`; a failing Langfuse evaluation score; a person in chat "start a run for…"). Deduplicate by fingerprint against open runs; apply severity | `loams-factory` | Severity below the org's threshold is logged, no run | `signal`, `fingerprint`, `severity`, a run record |
| 2 | **Triage**: open `#factory-triage > run-<id>` and post the evidence (counts, first and last seen, release, suspected commit, the OpenPanel metric); wait for a decision: `fix`, `ignore`, `escalate`, from a person's reply or reaction, or auto-`fix` by policy for severities and projects the org lists | `zulip`, `glitchtip`, `analytics` | A human reply by default (policy can auto-fix low-severity) | Thread URL, decision |
| 3 | **Plan**: create the Plane issue (title, evidence, links, label `factory`, run id), add to the active cycle if policy says | `plane` | None (write, non-protected) | Issue key and URL |
| 4 | **Fix**: `forgejo.propose_patch` (sandbox coding session on a workspace branch, §15, §36), `branches.create` `factory/<run>`, `prs.open` linking the issue; wait for CI (a promise settled by Forgejo's webhook); on CI failure feed the log tail back to the coder, up to `max_attempts` (default 3) | `forgejo` (+ coding sandbox) | None until merge | Branch, PR, CI status, attempt count |
| 5 | **Review**: post the PR link and a summary in the Zulip thread; wait for human review in Forgejo and a merge **approval** (D435) | `forgejo`, `zulip` | **Approval required** (D-SF-9) | Approval id, reviewer |
| 6 | **Deploy**: merge, then the org's deploy mechanism. For Loams' own GitOps layout (§38) merging to the environment branch makes Argo CD sync; the run waits on the rollout event (a CloudEvent from Argo's notifications). Other mechanisms are a webhook plus a wait on a callback (Q473) | `forgejo`; deploy by the org's GitOps | **Approval required for every deploy by default**, ordered by mechanism: with the §38 GitOps layout the merge to the environment branch *is* the deploy, so the merge approval and the deploy approval (covering the environment and the resulting commit) both settle before the merge; with a separate deploy step the order is merge approval, merge, then a deploy approval bound to the exact merge commit and the target, then the deploy; a policy may set `deploy.auto` for listed services in unprotected environments only (never protected ones), and the first deploy of any new service always needs one | Deploy ref, rollout state |
| 7 | **Observe**: for the observation window (default 30 min, configurable), check recurrence of the fingerprint in GlitchTip, the OpenPanel metric against its baseline, Langfuse evaluation scores for agent-facing changes, OpenObserve error rate and latency for the touched service. Verdict: `resolved`, `regressed`, `inconclusive` | `glitchtip`, `analytics`, `loams-factory` (Langfuse and OpenObserve through their APIs) | None | Verdict, evidence links |
| 8 | **Close**: `resolved` closes the Plane issue and posts a summary; `regressed` opens a **revert PR** (needs the same merge approval) and a new signal linked to the run (`generation + 1`); `inconclusive` extends the window once, then asks a person | `plane`, `zulip`, `forgejo` | Revert needs approval | Final record |

### 10.2 As a durable workflow (D-SF-13)

`factory.run` is a Resonate function on §21 (Rust SDK in-process, the embedded server). Its shape, in pseudocode that SF4 turns into tests:

```
fn factory_run(ctx, signal):
    run = ctx.run(open_run, signal)                       # idempotent on signal.fingerprint + generation
    guard = Guard::new(run, policy)                       # budgets, kill switch, cooldown
    guard.check(ctx)?                                     # before every stage and every A2A call: each stage helper below begins with it
    thread = a2a(ctx, "zulip",   open_triage(run, evidence(signal)))
    decision = ctx.promise(triage_decision(run.id), timeout=policy.triage_timeout)   # human or auto
    if decision != Fix { return close(run, Ignored) }
    guard.check(ctx)?
    issue = a2a(ctx, "plane",   create_issue(run, thread))
    let mut ci_green = false          # policy validation requires max_attempts >= 1, so `pr` is always set after the loop
    for attempt in 1..=policy.max_attempts:
        guard.check(ctx)?
        pr = a2a(ctx, "forgejo", propose_patch_and_open_pr(run, issue, attempt, last_ci_log))
        ci = ctx.promise(ci_result(pr.id))               # settled by the Forgejo webhook receiver
        if ci.ok { ci_green = true; break }
    if !ci_green { return fail_safe(ctx, run, pr) }      # draft PR, label factory:needs-human, thread and issue say so, no approval is requested
    guard.check(ctx)?
    approve(ctx, merge_approval(run, pr))                # waits on approvals promise (§21 §6.5)
    guard.check(ctx)?
    deploy = deploy_stage(ctx, run, pr)                  # approval order depends on the mechanism (§10.1 stage 6): GitOps (merge is the deploy) settles both approvals before the merge; a separate deploy step merges first, then asks a deploy approval bound to the merge commit
    guard.check(ctx)?
    verdict = observe(ctx, run, window=policy.observe_window)
    match verdict:
        Resolved      => close(run, Resolved)
        Regressed     => { revert_pr(ctx, run, pr); spawn_next(ctx, run.generation + 1) }
        Inconclusive  => extend_once_then_ask(ctx, run)
```

- **Idempotency.** Every `a2a(...)` call carries a deterministic `messageId` (promise id of the step); agents dedupe on it; the Forgejo and Plane skills are written as create-if-absent keyed by `run id` (a Plane issue with external id `factory:<run>`, a branch named `factory/<run>`).
- **Compensation.** A run that is **killed, ignored or abandoned** before merge closes its PR, deletes its branch, comments on the issue, and posts the outcome in the thread (a saga, §21 §6.3). **`fail_safe` (CI attempts exhausted) is a deliberate human handoff, not a failure to compensate**: it keeps the draft PR and its branch, labelled `factory:needs-human`, and the run ends `failed`; compensation does not touch it. After merge the compensation is the revert PR, which is itself gated.
- **Resumption.** A crash resumes the workflow from its last checkpoint; a model call is never paid twice (§21 §6.6).
- **Where the loop runs.** In the Loams process that holds Resonate for the org (single org: the one embedded server).

> **Cross-reference, 2026-10-02 ([§42](42-cloudflare-2026-betas.md) D574).** A factory run's coding workspace (stage 4, `forgejo.propose_patch`) is a pause and resume unit behind a `WorkspaceSnapshot` seam: a branch plus a volume snapshot on Knative; on Cloudflare Containers, filesystem snapshots (`snapshotContainer`, beta); that Cloudflare provider is private. Server-side web reach for the agents (Q507) is answered in §42 §4.

## 11. Safety: budgets, kill switch, loops, audit (D-SF-14)

| Rail | Definition | Enforced by |
|---|---|---|
| **Budgets** | Per run: tokens, money (micro-dollars at the AI gateway's price table), wall time (default 4 h), attempts (3), open PRs (1). Per org per day: runs (default 10), money, open factory PRs (3). Exceeding a budget pauses the run with a `budget_exceeded` card and a push; raising it is a person's action | `Guard::check` before every step, plus the AI gateway's own hard cap per principal (§19 §5.1 limits) so a bug in the guard cannot overspend |
| **Kill switch** | `loams.factory.v1.Kill(scope)`: scope is a run, an agent or the org. It sets a flag in Live (read by every step) and cancels the scope's workflows and sends `CancelTask` to its in-flight A2A tasks; **a run-scoped kill does not suspend any principal** (the agents are shared by unrelated runs); an agent-scoped kill also **suspends that agent's principal** (tokens die within the change-feed latency, §19 §5.4); an org-scoped kill suspends all of the org's agent principals. Needs the `factory:admin` action and a step-up session; one tap on the phone and one button in the console | `loams-factory`, the gateway |
| **Loop limits** | `generation` (a regression after a factory deploy) is capped at 2; a fingerprint has a cooldown (default 24 h) after a run closes; at most `N` concurrent runs per project; a signal that is caused by a factory deploy links to the causing run instead of creating a sibling | `factory.run` intake |
| **No auto-merge, no auto-deploy by default** | The policy may allow auto-merge only for configured repositories and paths, with CI and a minimum number of passing checks, and never in protected environments (Q472) | The policy and the credential broker |
| **Scope** | Agents see only their projects (OpenFGA tuples via the §22 sagas). The Forgejo agent's token is limited to repositories listed in the factory policy and cannot push to protected branches | The apps' own permissions (projected) and the broker |
| **Audit** | Every agent action, every approval, every policy change and every kill is an audit event emitted as OTel logs to a Loams stream (D100, open-core table), with the actor chain; the open repository ships the query API and CLI, the hosted audit UI is commercial (D220) | Engine |
| **Cost visibility** | The run record sums spend per stage, so a person sees what a fix cost | `factory_runs` |

## 12. The factory run record (D-SF-15)

**Live table `factory_runs`** (one row per run, updated by the workflow; subscribed by the console and phones through the Live sync API, §20 §7): `id`, `org`, `env`, `generation`, `parent_run`, `signal { source, fingerprint, title, link }`, `state` (`open`, `waiting`, `paused`, `succeeded`, `failed`, `killed`), `stage`, `stage_state`, `links { zulip_thread, plane_issue, forgejo_pr, deploy_ref, langfuse_trace_ids[], openobserve_query }`, `approvals[]`, `budget { limit, spent }`, `verdict`, `started_at`, `updated_at`.

**Stream `factory_events`**: one CloudEvent per transition (`io.loams.dev.factory.run.opened.v1`, `…stage.completed.v1`, `…approval.requested.v1`, `…run.completed.v1`, …), idempotent producer (D72), linked into a collection for search (§22 §8.1).

**Console pages** (`@loams/plugin-factory`): **Runs** (filter by state, project, severity; a lifetime-cost column); **Run detail** (the stage graph, each stage opening its artifacts: the thread, the issue, the PR, the CI log, the deploy, and "open trace in Langfuse" and "open in OpenObserve", which use the embed panes); **Approvals** (the org's queue); **Agents** (cards, health, last task, principal state, suspend); **Policy and budgets** (versioned, approval-gated in protected environments); **Kill switch**. Mobile has the run list and timeline, the approval screen and a kill button.

## 13. Loams Bot in the apps (D-SF-19)

**Desktop (a zeron fork).** zeron's engine drives coding agents through a **`Harness` trait** (Claude Code as a subprocess over stream-json, Codex over JSON-RPC, a mock; `zeron-harness`), and its UI (`zeron-ui`: sidebar, conversation, composer, terminal, diff pane) renders any harness's `AgentEvent` stream. Loams Bot is **a new harness, `loams-harness-bot`**, whose "agent" is a server-side Loams Bot thread: a `RunRequest` becomes `BotService.Send`, `Watch` events become `AgentEvent`s (text deltas; a subagent card per A2A task as a tool call with its state; a user question; an approval request), steering and interrupt become `Cancel` and follow-up sends, and the session appears in zeron's attention-sorted sidebar beside local coding sessions. The thread is authoritative on the server (a durable execution, SF3); zeron's session doc mirrors it for the UI, so a thread opened on a second device simply `Watch`es again. **zeron's own sync backend (Cloudflare Durable Objects, per its architecture notes) is not used**: the desktop runs in zeron's Local profile and the Loams instance is the sync (Q468 covers whether the fork keeps zeron's Claude Code and Codex harnesses and how their sessions sync). Artifact cards, `@agent` mentions and the slash commands are GPUI views in `loams-ui-bot`; `/kill` asks for a step-up and confirms; `/approvals` opens the native approvals view; none decides anything.

What is taken from zeron, and what is not (verified 2026-10-02 from its `ARCHITECTURE.md` and crate manifests): taken, the `Harness` trait, the session and transcript model, the UI crates, the theme registry, the local engine daemon and its headed or headless modes, and its `mcp` crate as a pattern; not taken, its Loro-over-Cloudflare sync, its voice crate, its WebRTC-based preview crate, and its own RPC (`zeron-rpc` stays between zeron's engine and UI; everything toward Loams is Connect, D420). zeron is MIT; it depends on a forked GPUI (Apache-2.0; "we do not use Zed's GPL crates", per its architecture notes) and Loro (MIT) **(licences checked at SF3 Task 0)**. Its `zeron-mobile` crate is a UniFFI core for iOS and Android with platform-painted UI; the phones here stay native over connect-swift and connect-kotlin (D433), and that crate is a pattern, not a dependency.

**Browser console (cordis).** `@loams/plugin-bot` ports the DeepSeek harness's conversation UI as cordis plugins (`ui-conversation`, `ui-tool`, `ui-subagent`, `ui-user-questions`, `ui-trajectory`, `ui-input-trigger`, `ui-commands`) over `rpc.bot` on Connect. The composer has `@agent` mentions, slash commands (`/run`, `/status`, `/kill`, `/approve` opens the approval, never decides), links as attachments and the model picker (default: DeepSeek through the AI gateway). Messages render as parts: text, subagent cards (agent, task state, streaming status) and **artifact cards** (`bot.card` keyed by kind: issue, PR, error, metric, run, approval, thread). It is a page and a docked overlay (`shell.overlay`).

**Mobile.** Native chat in SwiftUI and Compose, modelled on the mobile harness's chat screen (streamed turns, a glyph per tool, expandable tool cards, goal and question docks), over `loams.bot.v1` with the binary Connect codec; artifact cards are native views; push opens `loams://bot/threads/<id>`; transcripts are cached read-only offline; sending needs a connection (no queued sends, D437).

**Model and cost.** Loams Bot's default model is DeepSeek, through Loams's AI gateway, which meters tokens per principal and exports GenAI spans. A different model is a gateway route (Q468). **Managed model costs and managed agents are the commercial part** (§14); self-hosters bring their own key.

## 14. Open source and commercial (D-SF-16, D-SF-20)

| Piece | Open (this repository, Apache-2.0) | Commercial (`loam-platform`, private) |
|---|---|---|
| A2A | `loams-a2a`, the five agents, signed cards, push receiver | Hosted agent fleet operations, scaling and pre-warming |
| Chat | Loams Bot service, `loams.bot.v1`, desktop plugin, mobile screens | Managed model routes, the model price table and margins |
| Collaboration UI | All plugins, `loams.collab.v1`, the credential broker | Hosted per-tenant instances of the apps (provisioning at scale) |
| The loop | `loams-factory`, `loams.factory.v1`, the run record, budgets, the kill switch, audit events | Multi-tenant orchestration, cross-tenant dashboards, enforced plan limits, hosted audit UI and retention |
| Observability | Collector configs, Langfuse and OpenObserve wiring and embed panes | Hosted Langfuse and OpenObserve per tenant, retention tiers |
| Self-hosting | `deploy/factory/` chart, catalog patch, manifest: **a single-org factory a person installs with Helm** | The marketplace listing, install flow, entitlements, billing and metering (no metering in OSS, D403, D444) |
| Push | The sealed push path (D436) | Operating `push.loams.dev` |

OSS emits business events (`io.loams.dev.factory.run.completed.v1` with counts) as any consumer can read; the platform's metering consumes them. This repository never depends on `loam-platform`.

## 15. Risks

| # | Risk | Mitigation |
|---|---|---|
| 1 | **A2A's Rust SDK is young** ("validation" status; crates suffixed `-lf`) | Pin; SF2 Task 0 runs its conformance against the A2A inspector (`a2aproject/a2a-inspector`) and a Python agent; contribute fixes; the fallback is our own thin axum server over the spec's proto, which is small |
| 2 | **Embedding breaks on an app upgrade** (CSP, new framing headers, cookie flags) | CI runs the edge assertions of §3.3 against each pinned image and the next release; apps are pinned by digest |
| 3 | **A webview inside GPUI may not work on every OS** (no webview crate in zeron's workspace; `gpui-wry` is a community crate) | Native panels and the system browser are the plan; the sidebar browser is optional and spike-gated (SF1 Task 0) |
| 3a | **zeron is young and the fork drifts** (MIT, fast-moving, pinned to a Zed revision, no plugin mechanism beyond the `Harness` trait and themes) | Pin; keep Loams additions in separate crates behind the `Harness` and view registration seams; upstream what fits; D421's rule |
| 4 | **Plane Community Edition has no OIDC and a thinner API than the paid edition** | Chained SSO through Forgejo (§22 §5); SF2 Task 0 inventories the API; Q463 |
| 5 | **Prompt injection through issue, chat or error text** that steers an agent | §8 item 5: untrusted parts, server-side gates in the broker, no write without policy; red-team fixtures |
| 6 | **A runaway loop** (a fix that creates errors that start runs) | §11 limits; budgets enforced twice |
| 7 | **Langfuse v4 and OpenObserve are heavy or limited**: ClickHouse, Redis, Postgres and S3 for Langfuse; no SSO or RBAC in OpenObserve | Optional profiles; forward-auth; Loams's own OTLP store is the console's source (§9.1) |
| 8 | **AGPL and GPL obligations** on hosted offering | §4; legal review before the marketplace (platform doc 05) |
| 9 | **Cost**: a loop run is several model calls plus a coding session | Budgets, cost in the run record, a cheap triage model option |
| 10 | **Credentials in the model context** | Broker design (§6.2); the secret canary extended to agents and traces |
| 11 | **Two durable engines confusing operators** (§21's operations and A2A tasks) | One mapping (§5.3); the run view shows both ids |
| 12 | **The harness fork drifts** (MIT, third-party, fast-moving) | D421: patterns, not a fork; the A2A provider and UI plugins are ours |

## 16. Phase 2: the observability apps

Detailed in plan SF5; the decisions in brief:

- **GlitchTip**: native error list and detail (`/api/0`, §3.2), embedded UI behind OIDC, a webhook receiver that creates signals, the `glitchtip` agent.
- **OpenPanel**: native tiles through the `analytics` agent (AGPL, forward-auth for the dashboard embed), baseline and anomaly checks as a scheduled workflow.
- **Langfuse 4**: the collector pipeline (§9.1); embed panes that open a trace, session or score page by id; the `traces` card in the run view; Langfuse's own OIDC through Authentik; the `ee/` features never used.
- **OpenObserve**: the collector pipeline; an embed pane at a query pre-filtered by `factory.run_id`; forward-auth because the open edition has no SSO; the console's own run view does not depend on it.

## 17. Sources

- A2A: `a2a-protocol.org` (the overview, the specification, "What's new in v1.0"); `github.com/a2aproject` (the specification and SDK repositories, Apache-2.0, including `a2a-rs`); the Linux Foundation project announcement (2025); community SDKs: JetBrains Koog `agents-features-a2a-core`, `Victory-Apps/a2a-swift`. All read 2026-10-02.
- Langfuse: `langfuse.com/resources/engineering/clarifications` (v4, ClickHouse, OTLP, MIT and `ee/`), the open-sourcing post of 2025-06-04, third-party release notes for v4.38. Read 2026-10-02; the current release is a SF5 Task 0 check.
- OpenObserve: `github.com/openobserve/openobserve` (AGPL-3.0, the enterprise feature list, the OTLP path pattern). Read 2026-10-02.
- Forgejo: `forgejo.org/docs/latest/admin/config-cheat-sheet/` (`X_FRAME_OPTIONS`, default `SAMEORIGIN`). Read 2026-10-02.
- Not found on the web in this pass (so marked **verify**): Zulip's and Plane's framing headers, Plane CE's API coverage, OpenPanel's read API, Langfuse's OTLP path and headers.
- zeron: `github.com/zeronsh/zeron` (`README`, `ARCHITECTURE.md`, `crates/*/Cargo.toml`: engine, harness, proto, doc, sync, rpc, ui, mobile, mcp, preview). Read 2026-10-02 through the GitHub API; the sidebar browser's implementation was not found in the architecture notes (SF1 Task 0).
- This repository: §19, §21, §22, §24, §30, §36, §37, §38, `docs/open-core.md`, the decision log; the DeepSeek harness repositories at `~/Documents/research-clones/deepseek-harness-desktop` (`packages/client/ui-*`, `packages/subagent/*`, `packages/acp`, `packages/interaction/*`) and `deepseek-harness-mobile`.

## 18. Contradictions with earlier decisions, and how they are resolved

| Earlier text | This document | Resolution |
|---|---|---|
| §37 D429–D432 (a Tauri shell with a CSP and `net_fetch`) | The desktop is native, a zeron fork, and loads no web content in its own UI | §37 is being amended for a native desktop (D480–D499); this document needs from it only: Connect clients in Rust, OS deep-link registration, the keychain sign-in, and a place to register GPUI views and a `Harness`. The Tauri-specific rows here are removed |
| §37 D420: every app call is Connect-RPC | A2A is HTTP JSON | A2A is server-side only (Loams Bot to agents). The apps use Connect (`loams.bot.v1`). Added to the list of named exceptions: none needed |
| §22 D-SC-3 (Keycloak) | Authentik | §38 D447 already replaced it; this document follows §38 |
| §22 §8.5: agent actions go through `commons-control` sagas, never with app admin tokens | Agents hold app identities | The credential broker is `commons-control`'s successor for agent calls; app identities are provisioned by the same sagas; agents never hold secrets (§6.2) |
| §30 D289: destructive tools never MCP | The factory merges and deploys | Those are A2A skills behind approvals, not MCP tools (D-SF-12) |
| §22 §2.2: the suite does not modify apps | Embedding and header rewriting | Header rewriting is edge configuration, not modification (D-SF-3) |
| §19 §5.3 defines `env` as a token's audience and no `aud` claim | Agent tokens carry `aud` (the agent's interface URL) as well as `env` | SF2 Task 4 amends §19 §5.3: access tokens gain an optional `aud` claim, required for agent-to-agent tokens; `env` stays the environment audience |
| §19 §5.5: keys cannot be issued to agents | Agents use app tokens | The agent never holds them; the broker does (§6.2) |
| §22 `PostHog` as the analytics app | The analytics agent reads OpenPanel | §22 §13b already dropped PostHog; D-SC-15 stands |

## 19. Open questions

The questions are Q460–Q479 in the canonical decision log.
