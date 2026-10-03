# 42 — Cloudflare's Birthday Week 2026 Betas: Open Adapters for Loams

Status: **Proposed** · 2026-10-02. The owner's direction of 2026-10-02: "search the latest beta features in the Cloudflare blog and use them." This note folds the betas into the **open** side of Loams only. Hosted-service choices (Containers, Dynamic Workers, the Monetization Gateway, the Data Platform as a hosted analytics backend) are in the private `loam-platform` repository, docs 03 to 07, and follow [open-core.md](../open-core.md). Decisions **D560–D578** and questions **Q560–Q572** (the ranges D560–D579 and Q560–Q579 are reserved; IDs not listed are unused) are recorded in the [decision log](13-decision-log.md).

Rule for this note: nothing below is relied on until its status, limits, pricing and licence were read on Cloudflare's own blog or documentation. Every fact carries the date it was read (**verified 2026-10-02**); a fact a primary page did not state is marked **(unverified)**. Cloudflare's betas change fast: re-check the row before an adapter ships.

## 1. Summary

Four Cloudflare integrations (three betas or drafts, and one GA product) become open adapters, each behind a trait Loams already has or is about to have, so none of them is a dependency of self-hosting (D560):

| Beta | Open adapter | Default |
|---|---|---|
| Artifacts | `ArtifactsRemote` for Loams Git (§36): a mirror sink and a workspace host, **not** a `WalStore` or `RefLog` | The bucket WAL stays the default (D561, D562) |
| Browser Run, with Kitesurf as an option | A `remote` provider in the web-bridge toolbox (§37 §18.14), which **answers Q507** | Client-tool relay for credentialed sessions, `remote` for unattended public-web work (D565, D566) |
| WebMCP | Console and plugin actions registered as WebMCP tools; the bridge calls a page's tools | Progressive enhancement behind feature detection (D568, D569) |
| Basin Catalog (formerly R2 Data Catalog, GA 2026-10-01) | An external Iceberg REST catalog in `loams-iceberg` | Lakekeeper stays the default; read first (D571, D572) |

Two more betas are only cross-referenced here: Containers filesystem snapshots for factory runs (§39 SF4, D574) and Emscripten Rust for Workers (private, CF1, D576).

## 2. What was verified (2026-10-02)

| Feature | Status and date | Limits, pricing, licence | Flag |
|---|---|---|---|
| **Artifacts** ([blog](https://blog.cloudflare.com/next-git-platform-on-cloudflare/), [docs](https://developers.cloudflare.com/artifacts/)) | Open beta, post of 2026-10-01; Workers Paid plan only | $0.15 per 1,000 operations beyond 10,000 a month; $0.50 per GB-month beyond 1 GB. **Limits:** 1 GB per repository, 32 MB per blob, 1 TB per account (raisable), 2,000 control-plane requests per 10 s per namespace and 2,000 Git requests per 10 s per artifact, US or EU jurisdiction fixed at namespace creation. Closed source | **Beta.** The blog says billing starts **2026-10-15**, the docs pricing page says **2026-10-14**: treat 2026-10-14 as the safe date. Git protocol versions are not stated **(unverified)** |
| **Containers** ([blog](https://blog.cloudflare.com/faster-agent-sandboxes/)) | Post of 2026-09-30. `durable_object` scheduling policy and filesystem snapshots (`ctx.container.snapshotContainer`, `start({ containerSnapshot })`) are public beta; median start 648 ms (p95 910 ms, p99 1,129 ms) | Pricing and limits not stated in the post. The `cloudflare/debian-trixie` image has Node.js 24.20.0 | **Beta** (two parts). The `Container` and `Sandbox` classes are maintained only through **2026-12-31**; use `this.ctx.container` |
| **Dynamic Workers** ([blog](https://blog.cloudflare.com/dynamic-workers/)) | Open beta, 2026-03-24 (modified 2026-07-22); Workers Paid | $0.002 per unique Worker loaded per day, waived in beta; no stated global concurrency limit. V8 isolates | **Beta** |
| **Durable Object Facets** ([blog](https://blog.cloudflare.com/durable-object-facets-dynamic-workers/)) | Open beta, 2026-04-13; Workers Paid | Own SQLite database per facet; "subject to storage limits" | **Beta** |
| **Kitesurf** ([blog](https://blog.cloudflare.com/kitesurf-update/), [docs](https://developers.cloudflare.com/browser-run/kitesurf/), read 2026-10-03) | Free beta, 2026-09-28, per-account limits. A stateless Workers V8 isolate, selectable inside Browser Run with `browser=kitesurf`; WebMCP, CDP, Playwright, Puppeteer and MCP; `env.BROWSER.quickAction()`. Cloudflare rules it out for a long-running authenticated session, video/WebGL and bot-challenge TLS fingerprints; no tabs; `browser=kitesurf` must not be combined with `keep_alive`, `lab` or `recording`; **session guardrails are not supported on it** | Open-source release promised "soon", **no licence or date**. 3–7× less CPU and memory than the warm Chromium pool on their corpus, about 1.7× slower wall time; WPT coverage 97% DOM, 96% HTML | **Beta, closed source today.** Never design on its source, and never on its persistence |
| **Browser Run** ([docs](https://developers.cloudflare.com/browser-run/), read 2026-10-03) | Available on Free and Paid plans; Quick Actions and Sessions (Puppeteer, Playwright, CDP, Stagehand). **Not a beta**: answers Q565 | **Free:** 10 browser minutes/day, 3 concurrent browsers, one new browser per 20 s, a 60 s idle timeout, max 4 `keep_alive` sessions, 10 errors/min. **Paid:** 10 hours/month then $0.09/hour, 10 concurrent included then $2.00 each on a monthly average, 200 concurrent allowed | GA, and the stable base for the adapter. A paid account is a real dependency: never required for self-hosting, always off by default |
| **WebMCP** ([spec](https://webmachinelearning.github.io/webmcp/)) | **Draft Community Group Report, 2026-09-30**, W3C Web Machine Learning CG; "not a W3C Standard nor on the Standards Track" | The entry point is `document.modelContext` (`registerTool(tool, options)` with `exposedTo` and `signal`). Secondary articles report an origin trial in Chrome 149 to 156, Edge behind a flag, no commitment from Firefox or Safari **(unverified: not read on a primary Chrome page)** | **Experimental.** The older `navigator.modelContext` spelling is deprecated |
| **Monetization Gateway** ([blog](https://blog.cloudflare.com/monetization-gateway-beta/)) | **Closed beta**, 2026-09-30, eligible US-based sellers and buyers; request access through the dashboard or x402@cloudflare.com | HTTP 402 with the x402 protocol, USDC on Base through Coinbase's facilitator; Cloudflare's fee not stated | **Closed beta; owner action.** Private only (loam-platform doc 07) |
| **Data Platform, now Basin** ([2025 blog](https://blog.cloudflare.com/cloudflare-data-platform/), [Basin GA blog](https://blog.cloudflare.com/cloudflare-basin/)) | The 2025-09-25 post had Pipelines, R2 Data Catalog and R2 SQL in open beta. **On 2026-10-01 Cloudflare made it Basin, generally available**: Basin Pipelines, **Basin Catalog** (a managed Iceberg REST catalog; formerly R2 Data Catalog) and Basin SQL; old names still work | Usage-based pricing, no egress fees (rates not read; the 2025 plan was $9 per million catalog operations and $0.005 per GB compacted). Cloudflare says PyIceberg, DuckDB, Snowflake and Spark can read and write | **GA** (rename confirmed by the blog and two docs pages). The catalog's endpoint, token scopes and prices are not read **(unverified)** |
| **Rust on Workers via Emscripten** ([blog](https://blog.cloudflare.com/rust-workers-emscripten-target/)) | "First public **experimental** preview", 2026-09-28 | Tokio works two ways: JSPI, and a `LocalEventLoop` runtime; single-threaded; some patches **not yet upstream**; sockets through `-sNODERAWSOCKETS` | **Experimental.** Confirms §36 and CF1; see D576 |

## 3. Artifacts as a Loams Git provider (D561–D564)

Artifacts is "a versioned file system that speaks Git": repositories, forks, files, commits and repo-scoped Git tokens, through a Workers binding. That is a **Git-level** service, while §36's traits sit **below Git**:

| §36 trait | What it needs | Artifacts |
|---|---|---|
| `WalStore` | Fenced, monotonic, contiguous append of segments | Not offered; a repository is not an append log |
| `RefLog` | Atomic multi-ref transactions, linearizable, idempotent on a key | Not documented **(unverified)**; Git ref updates are per ref |
| `BlobStore` | Create-only content-named blobs, range reads | Git objects only, 32 MB per blob, 1 GB per repository: the limits alone rule it out for packs and the build cache (§36 §8) |
| `Materializer` | Scoped, on-demand blob fetch for agents | A fit: files and commits are readable through the binding |

**D561: the bucket WAL stays the default and the source of truth.** Artifacts cannot be a `WalStore` or `RefLog` provider. **D562: the open adapter is `ArtifactsRemote`, a crate `loams-git-artifacts` (Apache-2.0)** with two roles: (a) a **mirror sink**: after a Loams commit, push the same commit to an Artifacts repository so Cloudflare Workers Builds and any Git client on that platform see it (the WAL remains the writer; the mirror is rebuildable); (b) a **`Materializer` and workspace host** for agent workspaces (§15): fork a repository, hand an agent a repo-scoped token that §36 §6's scope model mints, and import the result back as a proposed patch. **D563: tokens** minted by Artifacts are never stored in the WAL; they are held by the credential broker like any other (§39 §6). **D564: the hosted use** (Loams Cloud repositories on Cloudflare, quotas, per-tenant namespaces and jurisdiction) is private, in loam-platform doc 04. The adapter needs a Workers binding or Cloudflare's REST API; it is compiled out of self-host builds unless enabled.

## 4. A remote provider for the web toolbox (D565–D567), and the answer to Q507

§37 §18.14.6 left **Q507** open: how do server-side agents (the Factory's, SF2) get web tools when their app lacks an API feature? The recommended default (**D565**):

1. **Credentialed work uses the client-tool relay** (the person's own desktop runs the bridge tool; credentials never leave their machine). 
2. **Unattended, public-web or app-with-service-account work uses a `remote` provider**: same tool contract as the local Tauri bridge (§18.14.2: snapshot with uids, `find`, `click`, `fill`, `wait_for`, no cookie or storage tools), implemented over a remote browser's CDP endpoint.

**D566: the adapter targets Browser Run's CDP and Playwright endpoints**, which are documented and stable, not Kitesurf's own API. Kitesurf is selectable *inside* Browser Run, so it works when the account has it, but it is closed source with an unspecified future licence, so nothing may depend on it (a Playwright MCP container remains the self-host provider, Apache-2.0, as §18.14.6 D512 already says). **D567: a remote browser is a third party**, so the provider refuses `secret_ref` fills of user credentials by default. Service-account `secret_ref` fills require a configured hostname allowlist, and the provider rejects `fill` on a non-allowlisted page; D507 redaction applies to these fills and to snapshots, logs, traces and console output. It keeps no persistent profile unless the organisation enables one, and records every call in the same audit and trace path (§39 §9). The provider is a trait implementation in `loams-web-bridge`'s core; running it as a hosted service is private (loam-platform doc 04).

**What AP1c added, from the documentation read on 2026-10-03** ([docs/remote-browser-provider.md](../remote-browser-provider.md)).
- **The endpoint**: `wss://api.cloudflare.com/client/v4/accounts/{account_id}/browser-run/devtools/browser` acquires a session and speaks CDP on the same socket, authenticated with `Authorization: Bearer <API_TOKEN>` (the token needs **Browser Rendering — Edit**). Some pages and the Puppeteer examples use the `browser-rendering` path, the same endpoint under its former product name.
- **Guardrails, which is the answer to egress.** An optional `cf-brapi-guardrails` header carries base64url JSON of `allowedDomains` (50 at most) and `allowedDomainSets` (four at most), and Browser Run blocks every request the session makes outside them: the sub-resources and redirects a caller never sees. **It is not supported on Kitesurf**, which is why the adapter refuses an allow-listed Kitesurf session by default instead of running with the enforcement layer missing.
- **The engine default, written down.** `engine` is `chromium` unless an operator writes otherwise. Kitesurf is offered for the stateless work — a one-shot scrape, a content extraction, an approximate screenshot of a **public** page — and refused for anything credentialed or long-lived, on Cloudflare's own statement that it cannot keep a durable authenticated session. Recommending Kitesurf for a logged-in flow would be actively wrong: the credentials would be typed into a browser that cannot keep the session state and cannot clear a bot challenge.
- **Cost controls**, because the adapter is optional but the invoice is not: a concurrency ceiling, a minimum gap between new sessions and a per-UTC-day local budget, all defaulting to the free plan's numbers. These are a guard against a runaway loop, not a meter; §10 §6 keeps metering private.
- **The `keep_alive` bound is ambiguous in Cloudflare's own pages** (Q567): the CDP table allows up to 1 200 000 ms, the FAQ and the limits page say ten minutes. The adapter sends at most ten minutes, which both readings accept.
- **Live verification is outstanding**: no Cloudflare credentials were available, so the adapter is implemented against the documented interface and its tests run against a scripted CDP connection. The unverified points are listed in the provider document §6.

## 5. WebMCP in the console and plugins (D568–D570)

WebMCP lets a page register tools with `document.modelContext.registerTool`, so an agent calls a function instead of driving the UI. Its status is a **Community Group draft**, so it is an enhancement, never the only path (**D568**).

- **D569, the cordis console and plugins.** The console registers one WebMCP tool per **plugin action** that is already an MCP tool or a Connect method, through a small `webmcp` module of the console: feature-detected (`document.modelContext` present), registered with `exposedTo` set to the console's own origin plus the paired agent origins, and unregistered on sign-out (the `signal` option). The tools are generated from the same action registry as the stdio MCP server (§30), so there is one description of each action. Each tool enforces the user's OpenFGA permissions and the approvals of §39 §8 exactly as the UI would.
- **D570, the bridge.** §18.14.2 lists WebMCP as "left out of v1". It joins v2 as two tools: `list_webmcp_tools` and `call_webmcp_tool`, which prefer a page's registered tools over snapshot-driven clicks when present. This also gives agents a lighter path into the framed apps of §39 once those apps expose WebMCP (none is known to; **unverified**).

## 6. R2 Data Catalog and Iceberg interop (D571–D573)

Basin Catalog (formerly R2 Data Catalog; generally available since 2026-10-01) is a managed Apache Iceberg REST catalog inside an R2 bucket. Loams's catalog is Lakekeeper (§08), which also speaks the Iceberg REST protocol, so interop is a catalog configuration, not a new format.

- **D571: `loams-iceberg` accepts an external REST catalog** (`catalog.kind = "iceberg-rest"` with URI, warehouse and a bearer token) beside Lakekeeper, using the Apache `iceberg-rust` REST client. **D572: read first.** Loams reads tables from R2 Data Catalog for DataFusion queries on day one; Cloudflare says external engines can write to it, but Loams **writes** only after a spike shows commits to that catalog pass the conformance tests (optimistic commit, snapshot expiry, compaction), and a table has exactly one writing catalog. **D573:** the provider is a vendor adapter and is open; the hosted analytics service built on Pipelines and Basin SQL is private (loam-platform doc 04). The catalog's REST endpoint, token scopes and prices were **not read (unverified)**; the spike confirms them.

## 7. Cross-references and what stays private

- **§39 SF4 (D574).** A factory run's coding workspace is a **pause and resume** unit. On the self-hosted Knative target, a workspace is a branch plus a volume snapshot. On Cloudflare Containers, it is `snapshotContainer` and `start({ containerSnapshot })` (beta). SF4 gets a `WorkspaceSnapshot` seam so the provider is chosen by deployment; the Cloudflare one is private.
- **The deprecated classes (D575).** A search of docs 00 to 41 on 2026-10-02 found no Loams design that names Cloudflare's `Container` or `Sandbox` classes (§36 uses a Durable Object for the sequencer). The private docs 03 and 04 are updated to `this.ctx.container`.
- **D576, Emscripten.** §36 and CF1 (private) already plan Rust on Workers. The 2026-09-28 post makes the Emscripten target an experimental preview with Tokio available through two paths; CF1 must treat Tokio on Workers as **experimental** and pin the patch set.
- **D577, Monetization Gateway.** Private. Charging agents per call for commercial Loams APIs is billing, and billing is not open (open-core.md). The open side exposes only the existing usage hooks (§27).
- **D578, Dynamic Workers and Facets.** The Cloudflare-target tier for untrusted agent code (§24) and the Factory's generated apps; private (loam-platform doc 03). The open tier is workerd under gVisor.

## 8. Implementation plans

Issues (label `codex-ready,plan`) each carry the gates of the M1.5 plan and are tracked in #237.

| Plan | Track | Scope | Depends on |
|---|---|---|---|
| **GT5: Artifacts provider** | `track:gt` | `loams-git-artifacts`: mirror sink, `Materializer`, token minting; conformance against a fake Artifacts server; a live test behind a flag | GT1 |
| **AP1c: remote browser provider** | `track:ap` | `RemoteProvider` over Browser Run CDP; the tool contract and its tests; the policy of D567; the Q507 relay and remote choice in SF2 | AP1b |
| **AP1d: WebMCP in the console and plugins** | `track:ap` | The console `webmcp` module, tool generation from the action registry, bridge v2 tools; feature detection and a Chrome-flag CI job | AP1a, AP1b |
| **FL3: external Iceberg REST catalog** | `track:fl` | `catalog.kind = "iceberg-rest"`, read path, conformance spike against R2 Data Catalog, write decision | FL2 |

## 9. Risks

1. Betas change or end; every adapter is optional and behind a trait. 2. Artifacts' billing date differs between two Cloudflare pages. 3. WebMCP may change again (it already moved from `navigator` to `document`). 4. A remote browser holds page content in a third party's cloud; D567 defaults to public-web work only. 5. The catalog's endpoint and token scopes may change with the Basin rename. 6. **A remote provider both bills and fetches.** A Cloudflare account is a prerequisite, and an agent that can navigate anywhere is an open proxy on someone else's card. The provider is off unless configured; it carries an egress policy (schemes, private and metadata addresses, deny and allow lists) enforced before every navigation **and** Browser Run session guardrails for what that check cannot see; guardrails are unavailable on Kitesurf, so an allow-listed Kitesurf session is refused; and concurrency, session spacing and a daily local budget bound the spend.

## 10. Sources (read 2026-10-02; Browser Run, Kitesurf, guardrails, limits and pricing re-read 2026-10-03)

The Cloudflare blog posts and documentation pages linked in §2, and the WebMCP draft at webmachinelearning.github.io/webmcp.
