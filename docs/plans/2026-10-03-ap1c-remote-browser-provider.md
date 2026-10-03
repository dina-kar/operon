# AP1c — The Remote Browser Provider (Browser Run) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, config keys, method names), use them verbatim. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Status: Implemented (unverified against a live account)** (2026-10-03). Issue [#272](https://github.com/ostrium-labs/loams/issues/272). Design [§42](../design/42-cloudflare-2026-betas.md) §4 and [§37 §18.14](../design/37-desktop-and-mobile-apps.md); decisions D565–D567. Shipped as the crate **`loams-web-bridge`** in this repository (a library: the provider contract, the local provider and the Browser Run provider), branched from `dev`. Task 0's live spike could not run: no Cloudflare credentials were available, so **nothing here has been exercised against the live service** — see Task 0 and §"What is verified and what is not" in [docs/remote-browser-provider.md](../remote-browser-provider.md). The owner ruling of 2026-10-03 fixes the engine decision: Browser Run is the remote provider and **Kitesurf is an engine option on it**; it is not part of the desktop sidebar, which is a separate worktree on a different stack.

**Goal:** one `BrowserProvider` trait with at least two implementations behind it — a local (agent-driven) provider and a Cloudflare Browser Run provider — selected by configuration, with the credentials handled as secrets, the egress decided by an enforced policy, and the Chromium-versus-Kitesurf choice written down rather than assumed.

**Architecture** (as implemented, `crates/loams-web-bridge`):
- **`provider.rs`** — the trait (`open`, `navigate`, `snapshot`, `find`, `click`, `fill`, `wait_for`, `screenshot`, `extract`, `close`), `PageRef` (opaque; says only its provider and an id), `OpenRequest`, `Capabilities`.
- **`tool.rs`** — the tool contract and the D504 efficiency rules, implemented once: `s7_12` uids, snapshot budgets, `find` with context, the change summary, escaping, URL redaction, credential-field elision.
- **`page.rs`** — the shared page state and the parts both providers share: snapshot numbering, uid resolution with the self-healing stale error, scope and depth selection, the action answer.
- **`local.rs`** — the local provider over a host-supplied `PageDriver`. The policy, the snapshot and the redaction are here; the webview is the host's.
- **`browser_run/`** — `endpoint.rs` (the endpoint, the headers, the guardrails encoding, the refusal checks), `cdp.rs` (the transport seam and the client), `ws.rs` (the real WebSocket transport), `ax.rs` (accessibility-tree parsing and credential-field detection), `budget.rs` (the local daily guard), `mod.rs` (the provider).
- **`egress.rs`**, **`config.rs`**, **`secret.rs`**, **`redact.rs`**, **`artifact.rs`**, **`error.rs`** — the policy, the configuration surface, BYOK secrets, scrubbing, artifacts, the error type.

**Boundaries.** This crate does not own secret storage (it takes a `SecretResolver`), does not own a webview (it takes a `PageDriver`), does not meter, and does not know about MCP or the desktop host. AP1b's daemon and shim consume this contract; nothing here changes AP1b or the native desktop.

---

## Milestone AP1c: the remote browser provider

Design references: [§42 §4](../design/42-cloudflare-2026-betas.md), [§37 §18.14](../design/37-desktop-and-mobile-apps.md), [§30](../design/30-loams-cli.md) D288 (secrets by reference), [§39](../design/39-software-factory-and-loams-bot.md) §6 (the credential broker), and the [remote browser provider document](../remote-browser-provider.md).

- [x] **Task 0: Read the interface, and spike it live.** Read Cloudflare's CDP, Kitesurf, guardrails, limits and pricing pages and record the facts in §42 §2 and in the provider document.
  - **Done (2026-10-03):** the endpoint, the bearer authentication, `keep_alive`, `browser=kitesurf` and its `keep_alive` incompatibility, the guardrails header and **its non-support on Kitesurf**, Kitesurf's three documented limitations, the per-plan limits, the pricing, and the answer to Q565 (Browser Run is GA on Free and Paid plans; Kitesurf is in beta and closed source).
  - **Not done:** the live spike. With credentials: one Chromium session (open, snapshot, click, extract, screenshot, close), then the same on Kitesurf with `extract` only; record the tree shape, `backendDOMNodeId` stability, the wall time and the `X-Browser-Ms-Used` accounting. Until then the six unverified points in the provider document stand.
- [x] **Task 1: The provider contract, shared and tested.** The trait, the page state, the snapshot formatter with uids and budgets, `find`, the change summary, the stale-uid error, URL redaction, credential elision. *Tests:* uid ordering, a stale uid naming `take_snapshot`, a password value never rendering, the query never rendering, page text unable to close a field, the budget capping with a marker, scope and depth narrowing.
- [x] **Task 2: The local (agent-driven) provider.** Over `PageDriver`, with the same egress, credential, snapshot and redaction rules. *Tests:* the conformance suite over the local provider; a secret fill refused without the policy; the driver's own action log carrying only a length.
- [x] **Task 3: The Browser Run provider.** Endpoint and handshake, the CDP lifecycle (`Target.createTarget`, `Target.attachToTarget`, four `*.enable`), navigate, snapshot, click, fill, wait, screenshot, extract, close, the session ceiling and the daily budget. *Tests:* the handshake URL and headers per engine, the guardrails header, the exact CDP conversation call by call, a click resolving to a backend node and releasing its handle, a stale uid sending nothing, the screenshot path and hash, the ceiling of three on the free plan, Kitesurf refusing a pixel screenshot.
- [x] **Task 4: The configuration surface, the egress policy and the secrets.** `[web_bridge]` TOML with the environment overlay; the refusal matrix; the egress policy with Cloudflare's hostname-pattern rules and limits; `SecretRef`/`SecretResolver`/`SecretValue`; the scrubber. *Tests:* the refusal matrix, the policy matrix (schemes, private and metadata addresses, wildcards, deny over allow, Cloudflare's limits), the scrubber, and the credential-never-leak test.
- [x] **Task 5: The documentation.** The [provider document](../remote-browser-provider.md): prerequisites, the account and billing dependency, the per-plan limits, the beta caveat, the Chromium-versus-Kitesurf decision table, the security model including the SSRF stance, the configuration reference, and what is unverified. §42 §2 and §4 updated with what was read today; new open questions in the decision log.

---

## Verification (gates)

Gates are satisfied when each has a command and a recorded result.

1. `cargo fmt --all --check`
2. `cargo clippy -p loams-web-bridge --all-targets --locked -- -D warnings`
3. `cargo test -p loams-web-bridge --locked` — 34 unit tests and 42 integration tests, including the credential-never-leak test, the two-provider conformance suite and the refusal matrix.
4. `cargo deny check` — the new dependencies (`tokio-tungstenite`, `tungstenite` and their transitive crates) are MIT or Apache-2.0.
5. `bash scripts/ci/no-metering.sh` — the open crate meters nothing.
6. A human reads the credential test and confirms it would fail if a `SecretValue` were printed anywhere.
7. **Not satisfied here:** the live spike (Task 0). It needs the owner's Cloudflare credentials. Stated plainly rather than implied.

## Definition of done

- [x] The provider abstraction, the Browser Run implementation and the configuration surface, with tests.
- [x] A test proving credentials never appear in logs or errors.
- [x] Documentation: prerequisites, account and limits and beta caveats, the Chromium-versus-Kitesurf decision table, the security model including the SSRF stance.
- [x] Branched from `dev`; merge commits only; `git commit -s`; no force-push; PR into `dev` with auto-merge.
- [ ] The live spike (Task 0). Needs credentials; tracked on the issue.

---

## Rulings made during execution

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | The crate lives in **this** repository as `crates/loams-web-bridge`, not in AP1b's `ostrium-labs/loams-web-bridge`. | AP1b is a separate repository (D501) whose daemon will consume this as a library; the provider contract, the policy and the remote adapter belong with the engine they serve, and the desktop sidebar work (a different worktree, a different stack) must not be touched. | Low: publishing or vendoring the crate elsewhere is a dependency change, not a rewrite. |
| 2 | **Chromium is the default engine** and `engine = "kitesurf"` under a hostname allow list is a **configuration error**. | Cloudflare's guardrails are not supported on Kitesurf, so an allow-listed session could not be enforced; and Cloudflare states Kitesurf cannot start a long-running authenticated session. | Low: `allow_unenforced_guardrails = true` exists as the explicit override. |
| 3 | The local provider takes a host-supplied `PageDriver` rather than embedding a webview. | There is no webview in this repository, and the testable half of a browser provider is the policy, the snapshot and the redaction — not the engine. | Low: a headless local driver implements the same trait. |
| 4 | The egress policy refuses `*.internal`, `*.local`, `*.lan` and the metadata names by default. | A remote browser behind a syntactic check is still an SSRF primitive; the private and metadata cases are the ones that pay. | Low: the deny names are configuration, not code paths. |
| 5 | `keep_alive` is capped at 10 minutes even though Cloudflare's CDP table says 20. | Their table and their FAQ disagree; ten minutes is accepted by both readings (Q567). | None: a shorter session is always valid. |
| 6 | The local daily budget is a **guard**, not a meter, and says so. | The open-core boundary puts metering in the private platform (D540–D559); the guard exists to stop a runaway loop early. | Low, but it must never be described as metering. |
| 7 | The handle id is ours (`remote-<target>-<n>`), not the browser's target id. | A fake and, more importantly, a buggy server reporting the same target id twice would otherwise collide in the session map and leak a concurrency permit. | None. |
| 8 | A text search walks the whole accessibility tree, not its roots. | Found by the conformance suite: the text an agent waits for is almost always on a leaf, so a roots-only search silently never matches. | None. |
| 9 | Kitesurf **refuses** `screenshot` in this provider rather than returning a low-fidelity image. | Cloudflare documents Kitesurf as not pixel-perfect; returning a screenshot an agent may compare against a design would be a false answer. `extract` is the Kitesurf-shaped call. | Low: a `best_effort` flag is a small change. |
