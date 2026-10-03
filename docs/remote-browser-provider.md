# The remote browser provider: Cloudflare Browser Run

`loams-web-bridge` gives the web toolbox **one tool contract and two providers**:

| Provider | Where the browser runs | For |
|---|---|---|
| `LocalProvider` | the machine the agent already runs on, driven by the host | anything credentialed, anything long-lived |
| `BrowserRunProvider` | Cloudflare's network, over the Browser Run CDP endpoint | unattended public-web work: a scrape, a screenshot, a content extraction |

Which one runs is **configuration** (`provider = "local"` or `provider = "browser-run"`), not a fork. Both answer the same `BrowserProvider` trait, and one conformance suite runs against both (`tests/it/contract.rs`), so a deployment can change its mind without touching its callers.

Design references: [§42 Cloudflare 2026 betas](design/42-cloudflare-2026-betas.md) §4, [§37 Loams desktop and mobile apps](design/37-desktop-and-mobile-apps.md) §18.14. Decisions D565 (the client's own browser first, the remote provider for unattended public-web work), D566 (Browser Run CDP; Kitesurf optional and closed source), D567 (a remote browser is a third party: no user-credential fills, no persistent profile, full audit). Plan: [AP1c](plans/2026-10-03-ap1c-remote-browser-provider.md).

**Nothing in this document has been run against a live Cloudflare account.** See [What is verified and what is not](#what-is-verified-and-what-is-not).

---

## 1. Setup prerequisites

### 1.1 A Cloudflare account is a real dependency

The remote provider bills a Cloudflare account. It is **not** free, and it is not optional if you configure it:

| | Workers Free | Workers Paid |
|---|---|---|
| Browser hours | 10 minutes per day | 10 hours per month, then $0.09 per additional hour |
| Concurrent browsers (sessions only) | 3 | 10 (monthly average), then $2.00 per additional browser |
| Quick Actions | charged for browser hours only | charged for browser hours only |

Read on 2026-10-03 from [Browser Run pricing](https://developers.cloudflare.com/browser-run/pricing/). Read it again before you enable the provider in production: these are Cloudflare's numbers, not ours, and they are the numbers that decide whether a runaway agent loop is a nuisance or an invoice.

**Loams does not meter this.** The open-core boundary puts metering in the private platform (§41 D540–D559 and `open-core.md`). What the open crate does instead is a **local guard**: a per-UTC-day ceiling on the browser time this process spends, a concurrency ceiling, and a minimum gap between new sessions. It stops a runaway loop early; it is not a meter, it does not see other processes, and Cloudflare's dashboard remains the source of truth.

### 1.2 Per-account limits

From [Browser Run limits](https://developers.cloudflare.com/browser-run/limits/), read 2026-10-03:

- one new browser every 20 seconds (one every second on the paid plan);
- 3 concurrent browsers free, 200 paid (the pricing page bills 10 as included on the paid plan — the two pages answer different questions, and both are in the table above);
- a 60 second idle timeout free, 15 minutes paid;
- 3 requests per 10 seconds to Quick Actions free, 30 paid;
- error budget: 10 errors per minute free, 300 paid;
- max 4 `keep_alive` sessions per account, 10 paid.

`plan = "free"` in the configuration fills in the free numbers (3 concurrent, a 20 second gap, a 10 minute daily ceiling, a 60 second `keep_alive`); `plan = "paid"` fills in the paid ones. Every one of them can be overridden, because Cloudflare can raise an account's limits.

### 1.3 The beta caveat, and why Kitesurf is a dependency we can hold loosely

Browser Run itself is a **GA product available on Free and Paid plans** — not a beta. That answers Q565.

**Kitesurf is in beta**, and it is free during the beta behind the per-account limits above. Three things follow, and the design leans on all three:

- **It is not open source.** There is no licence to read, no source to audit, no commit to pin. D566 already records this as the reason Kitesurf is optional. A closed engine that renders the pages an agent acts on is a dependency we can use, and must be able to stop using, in one configuration line.
- **It is not the default pool.** `browser=kitesurf` opts in; omitting the parameter gets Chromium. We default to Chromium everywhere.
- **Its limits can change without notice** during the beta, which is one more reason it is not on the path that needs to work.

### 1.4 Credentials

The token is a **secret**, and the crate never holds one:

1. Put the token in whatever secret store the host already uses (§30 D288 and §37 §18.14.4: a `secret_ref` resolved from a file or the OS keychain, and §39 §6's credential broker on a hosted deployment).
2. Name it in configuration as a **reference**, never a value:

   ```toml
   [browser_run]
   account_id = "0123456789abcdef0123456789abcdef"
   token_secret_ref = "env:cloudflare#api_token"   # a reference, not a token
   ```

   The environment overlay accepts `LOAMS_WEB_BROWSER_RUN_TOKEN_SECRET_REF` — a reference — and deliberately has **no** variable that takes a token value.
3. The host implements `SecretResolver` over its store. The crate ships `MapSecretResolver` (tests and single-process runs) and `EnvSecretResolver`, which reads `LOAMS_WEB_BRIDGE_SECRET_<NAME>` from the environment and is for development only.

The token needs the **Browser Rendering — Edit** account permission. The account id is not a secret, but it must be configurable (it is: `account_id`, 32 hexadecimal characters, and the shape is checked at load time so that a token pasted into the wrong field is a startup error rather than a `403` from Cloudflare).

---

## 2. Chromium or Kitesurf

This is the decision the provider encodes, so it is worth being precise about why.

Cloudflare's own [Kitesurf page](https://developers.cloudflare.com/browser-run/kitesurf/) says Kitesurf is "not yet the right option" if you need to:

- start **a long-running, authenticated session that requires persistent state**,
- play video or render WebGL,
- negotiate a **bot-challenge handshake with real TLS fingerprints**.

Kitesurf is a Workers V8 isolate: ephemeral, stateless, cheap (Cloudflare measures 3–7× less CPU and memory than the warm Chromium pool for screenshots and HTML extraction), and about 1.7–1.8× slower in wall time.

| Need | Engine | Why |
|---|---|---|
| Anything signed in, any session that must remember | **chromium** | A session with state, and the TLS stack a bot challenge expects. Cloudflare rules Kitesurf out of this explicitly. |
| A site behind a bot challenge (WAF, managed challenge) | **chromium** | Kitesurf cannot present a real TLS fingerprint, so the challenge never clears. |
| Pixel-accurate screenshots | **chromium** | Kitesurf's rendering is explicitly not pixel-perfect. |
| Tabs | **chromium** | Kitesurf has no tabs. |
| An allow-listed hostname policy | **chromium** | Guardrails are not supported on Kitesurf (§4.3), so the policy could not be enforced for sub-resources and redirects. |
| One-shot scrape of a public page | **kitesurf** *or* chromium | Same answer either way; Kitesurf is 3.8× cheaper in CPU. |
| Screenshot of a public page, approximate rendering is fine | **kitesurf** | 3.1× less CPU than Chromium for the same screenshot. |
| Content extraction (text, HTML, markdown) of a public page | **kitesurf** | This is the operation Kitesurf is built for: 229 ms and 39 MiB against Chromium's 877 ms and 274 MiB on their corpus. |
| A bursty agent sweep, a fan-out of fetches, no state between them | **kitesurf** | Ephemeral and isolated by design; the per-session cost is minutes of a Workers isolate, not a browser hour. |

**The rule the crate enforces**: `engine` defaults to `chromium`, and configuring `engine = "kitesurf"` under a hostname allow list is a **configuration error**, not a warning, because guardrails cannot enforce the allow list there. An operator who really wants it sets `allow_unenforced_guardrails = true` and says so out loud.

**The answer to "where is Kitesurf right?"** is: ephemeral scraping, screenshots and content extraction of public pages, where statelessness is a feature rather than a limitation — that is, `extract`, an approximate `screenshot` on the Chromium pool, and a read-only scrape. It is **wrong** for a logged-in session, and recommending it there would be actively wrong: the credentials would be typed into a browser that cannot keep the resulting session state and cannot clear a bot challenge, so the flow would appear to work and then fail on the second request.

---

## 3. Configuration

```toml
[web_bridge]
provider = "browser-run"                    # "local" (the default) or "browser-run"

# Egress: what the browser may open. Empty means "any public host".
allowed_domains = ["*.example.com"]         # 50 at most (Cloudflare's limit)
allowed_domain_sets = ["common-cdns"]       # 4 at most
denied_domains = ["admin.example.com"]      # wins over the allow list
allow_private_hosts = false                 # loopback and RFC 1918 fixtures; development only
allow_unenforced_guardrails = false         # accept Kitesurf under an allow list

[browser_run]
account_id = "0123456789abcdef0123456789abcdef"   # not a secret, but configurable and checked
token_secret_ref = "env:cloudflare#api_token"    # a reference; never a token
engine = "chromium"                              # "chromium" (default) or "kitesurf"
plan = "free"                                    # fills in Cloudflare's per-plan limits
keep_alive_ms = 60000                            # chromium only; 10 s … 10 min
request_timeout_ms = 60000
max_concurrent_sessions = 3                      # defaults to the plan's ceiling
min_session_interval_ms = 20000                  # defaults to the plan's gap
daily_budget_ms = 600000                         # the local guard; 0 turns it off
artifact_dir = "/var/lib/loams/web-bridge"       # screenshots land here

[local]
default_profile = "acme-staging/zulip"
artifact_dir = "/var/lib/loams/web-bridge/local"

[web_bridge.credentials]
allow_secret_fills = false                       # see §4.2
allowed_fill_hosts = []                          # a service account only, only these hosts
```

Environment overlay: `LOAMS_WEB_BRIDGE_PROVIDER`, `LOAMS_WEB_BROWSER_RUN_ACCOUNT_ID`,
`LOAMS_WEB_BROWSER_RUN_ENGINE`, `LOAMS_WEB_BROWSER_RUN_TOKEN_SECRET_REF`,
`LOAMS_WEB_BROWSER_RUN_KEEP_ALIVE_MS`, `LOAMS_WEB_BROWSER_RUN_MAX_SESSIONS`,
`LOAMS_WEB_BROWSER_RUN_DAILY_BUDGET_MS`, `LOAMS_WEB_BRIDGE_ARTIFACT_DIR`,
`LOAMS_WEB_BRIDGE_ALLOWED_DOMAINS`, `LOAMS_WEB_BRIDGE_ALLOWED_DOMAIN_SETS`,
`LOAMS_WEB_BRIDGE_DENIED_DOMAINS` (comma-separated).

Refused at load time, with a message that says why:

| Refused | Why |
|---|---|
| `provider = "browser-run"` with no `account_id` or no `token_secret_ref` | a missing credential must be a startup error, not a failed session |
| an `account_id` that is not 32 hexadecimal characters | the usual mistake is pasting the API token where the account goes |
| `engine = "kitesurf"` with `keep_alive_ms` | Cloudflare documents that `browser=kitesurf` must not be combined with `keep_alive`, `lab` or `recording` |
| `keep_alive_ms` outside 10 000 … 600 000 | Cloudflare's floor, and the ten-minute ceiling both readings of their docs accept (Q567) |
| `engine = "kitesurf"` under an allow list | guardrails are unavailable there, so the policy cannot be enforced (§2) |
| more than 50 `allowed_domains` or more than 4 domain sets | Cloudflare's own limits, checked locally so a mistake is not a `400` |
| an unknown key anywhere in `[web_bridge]` | a typo must not become a silent default |
| `allow_secret_fills = true` with no `allowed_fill_hosts` | "allowed everywhere" is the setting this design exists to prevent |

---

## 4. Security model

### 4.1 The API token

- BYOK: the crate holds a **reference**, and the host's `SecretResolver` supplies the value at the moment a session opens.
- `SecretValue` renders `<redacted>` from `Debug`, `Display` and every error carrying it. `expose()` is the only way out, and its callers are the handshake builder and nothing else.
- Every resolved value is registered with `redact::register`, which rewrites it to `<redacted>` in any later string that carries it — an error, a `Debug`, a `tracing` field. Values are also scrubbed by shape even when they were never registered: bearer headers, `api_key=`-style assignments, JWTs, and long base64 or hex runs.
- The handshake headers have a hand-written `Debug`, so `{headers:?}` cannot print the token.
- `tests/it/secrets.rs` plants a canary token, drives every error path (a refused connection whose message quotes the token, a refused fill, a refused navigation, a missing secret, a browser that echoes its input) and asserts the canary appears **nowhere** in the output, while `<redacted>` appears in it.

### 4.2 Credentials typed into a remote browser

D567: a remote browser is a third party.

- No user-credential `secret_ref` fill on the remote provider. `allow_secret_fills` is off by default and the error message says why.
- A service-account secret may be typed into a host in `credentials.allowed_fill_hosts`, and nowhere else. The typed value is registered for scrubbing before it reaches the socket, so a browser error that echoes it cannot leak it.
- The local provider keeps a profile on the person's machine, which is where a personal login belongs. That is the whole reason it exists.
- A password or one-time-code field is marked sensitive from the accessibility tree (`protected`, a credential `autocomplete` token, or a credential-shaped name in a field role) and its **value is elided from every snapshot** — the field is visible, its content is not.

### 4.3 Egress and server-side request forgery

The remote browser fetches arbitrary URLs **from our Cloudflare account's network**. Left alone, Loams would be an open proxy paid for by someone else's card. Two layers, one of which only exists on one engine:

**Layer 1 — in the provider, before every navigation** (`EgressPolicy::check`):

1. `http` and `https` only. `file:`, `data:`, `javascript:`, `about:`, `ws:` and `ftp:` are refused.
2. Private, loopback, link-local, carrier-grade NAT, documentation, unspecified and broadcast addresses are refused as literals, in IPv4 and IPv6, including IPv4-mapped forms (`::ffff:127.0.0.1`).
3. `localhost`, `*.localhost`, `*.internal`, `*.local`, `*.lan`, `metadata.google.internal`, `metadata.goog`, `169.254.169.254` and `100.100.100.200` are refused by name.
4. The deny list is checked, and wins over the allow list.
5. With an allow list configured, a host outside it is refused.

Cloudflare's own hostname-pattern rules are mirrored exactly, because Cloudflare enforces them: a bare hostname with at most one `*`; `*.example.com` matches subdomains and **not** the apex; `*example.com` matches the apex and, as Cloudflare documents, lookalikes such as `evilexample.com`. A pattern with a scheme, a port or a path is a configuration error.

**Layer 2 — in Browser Run itself, for the whole session**: the `cf-brapi-guardrails` header carries `allowedDomains` (50 at most) and `allowedDomainSets` (four at most) as base64url JSON, and Browser Run blocks every request the session makes outside them. This is the layer that covers what layer 1 cannot see: sub-resources, redirects, and DNS that resolves a public name to a private address after our check.

**What layer 1 deliberately does not do: resolve names.** A hostname that resolves to `127.0.0.1` passes a syntactic check. Resolving and pinning would mean Loams doing its own DNS with its own rebinding window. Layer 2 is the answer on the Chromium pool; **and guardrails are not supported on Kitesurf**, which is exactly why an allow-listed Kitesurf session is refused by default rather than run with one layer missing.

**Loopback fixtures.** `allow_private_hosts = true` re-opens loopback and private addresses so a test suite can drive a fixture site. It is a development switch; it is off by default, and it is the setting that turns the provider into an SSRF primitive against whatever the host can reach.

### 4.4 Cost, denial of service and audit

- The remote provider is **off unless configured**: `provider` defaults to `local`, and the default `WebBridgeConfig` carries no account and no token reference.
- Every action is a change summary; snapshots are text with uids; nothing is logged with a credential in it; screenshots are written to `artifact_dir` with a SHA-256 so a caller can tell two runs apart.
- The concurrency ceiling and the minimum gap between sessions are the free plan's, so a free account cannot be throttled into `429`s by our own client behaviour. When the ceiling is hit the error is `SessionLimit`, not a queue.
- The daily budget guard refuses new sessions once the local ceiling is spent and says when it resets (00:00 UTC).

---

## 5. What the tools do

Both providers answer the same seven calls:

| Call | Chromium pool | Kitesurf | Notes |
|---|---|---|---|
| `open(url)` | session + tab | session | `Target.createTarget`, `Target.attachToTarget`, then `Page`/`Runtime`/`DOM`/`Accessibility` |
| `navigate(url)` | `Page.navigate` | same | refused by the egress policy first |
| `snapshot()` | `Accessibility.getFullAXTree` | same | text with `s7_12` uids, never a screenshot; budgeted, with a marker naming the way to continue |
| `find()` | over the last snapshot | same | matches with context lines, not the page |
| `click(uid)` | `DOM.resolveNode` + `Runtime.callFunctionOn` | same | a stale uid is refused before anything is sent |
| `fill(uid, value)` | same, with the prototype's value setter and `input`/`change` events | same | a secret is resolved, scrubbed and never echoed |
| `wait_for()` | polling the tree, or `Runtime.evaluate` for a selector | same | a selector wait is refused on the local provider, which cannot query CSS |
| `screenshot()` | `Page.captureScreenshot` | **refused** | not pixel-perfect; the provider says so |
| `extract()` | `Runtime.evaluate` reading `innerText` | same | the Kitesurf-shaped operation |
| `close()` | `Browser.close`, then the socket | same | closing twice is not an error |

Efficiency rules implemented once, in `tool.rs`, so both providers answer identically (§18.14.2 D504): uids carry their snapshot id, so a stale uid is a self-healing error that names `take_snapshot`; an action answers in one line plus a change summary; `find` returns matches with context; names are truncated at 200 characters; identical sibling runs are folded; the result is capped by a token budget; a URL appears as host and path with the query redacted; page text is escaped so it cannot close a field.

---

## 6. What is verified and what is not

**Verified against Cloudflare's documentation, read 2026-10-03** (the CDP, Kitesurf, guardrails, limits and pricing pages): the endpoint path and its bearer authentication; `keep_alive`; `browser=kitesurf` and the `keep_alive` incompatibility; guardrails' header name, shape, limits and **non-support on Kitesurf**; Kitesurf's statelessness and the three things Cloudflare says it cannot do; the per-plan limits; the pricing; that Kitesurf is closed source; the WPT conformance figures.

**Verified in this repository, with no account**: the URL and headers this crate builds; the exact CDP conversation the provider holds (asserted call by call in `tests/it/remote.rs`); the accessibility-tree parsing, including credential-field detection; the uid scheme and the stale-uid refusal; the screenshot hash; the session ceiling and the daily budget; the refusal combinations; the credential never appearing in any output; the same conformance suite passing for both providers.

**Not verified — no Cloudflare credentials were available, so none of this ran against the live service:**

1. that the WebSocket handshake accepts the headers as sent here (`cf-brapi-guardrails` in particular);
2. that `Target.createTarget` and `Target.attachToTarget` behave on a Kitesurf session — Cloudflare documents Kitesurf as CDP-compatible but tabless, and we assume one target per session;
3. the exact accessibility-tree shape Kitesurf returns, and whether `backendDOMNodeId` is stable there, which the click and fill paths depend on;
4. the real latency and cost of each tool call, which is what would justify Kitesurf on a specific workload;
5. whether a `429` from the daily free-tier ceiling arrives as we expect, and how our own budget guard interacts with Cloudflare's;
6. the `keep_alive` ceiling, where Cloudflare's own pages disagree (Q567).

**Task 0's spike** (the first thing to do with credentials in hand): open one Chromium session against a public page, take a snapshot, click, extract, screenshot, close; then the same on Kitesurf with `extract` only; record the tree shape, the wall time and the `X-Browser-Ms-Used` accounting. Everything above is written to be corrected by that spike rather than defended.

---

## 7. Related

- **Distribution.** The crate is `publish = false` for now: it is not in the release workflow's declared crate list, which is a reviewed owner decision (issue #254). AP1b's repository takes it as a path or git dependency until it is published.
- The desktop's own browser (AP1b, `ostrium-labs/loams-web-bridge`) is the **local** provider and a different concern: it is where credentialed work happens. Kitesurf does not belong there, and nothing in this crate's design asks it to.
- Design [§42](design/42-cloudflare-2026-betas.md) §4 (the betas as optional adapters) and §9 (risks).
- [§37](design/37-desktop-and-mobile-apps.md) §18.14.2–§18.14.6: the tool contract, the profiles, the credentials, the remote provider, the MCP surface.
