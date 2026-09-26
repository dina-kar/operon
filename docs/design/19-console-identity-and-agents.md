# 19 — Console, Identity and Agents

Status: **Proposed** · 2026-09-26. Decision numbers are assigned at merge; until then they are P1–P10. This document builds on §18 §6–§8 (D64–D66) and amends one line of D65 (the tenancy model, P2).

Markers: **(verify)** is not checked against a primary source; the plan that builds it resolves it.

---

## 1. Summary

| # | Proposal | Milestone |
|---|---|---|
| P1 | **One console, three editions.** A React single-page app serves OSS, Loam Cloud and BYOC. The engine embeds its build and serves it at `/ui`; the hosted control plane serves the same build | Mock-backed now; real with M2 |
| P2 | **Org → projects → environments.** A project groups environments; an environment is exactly one namespace. Amends D65's "org → namespaces" | M2 |
| P3 | **One org per OSS install.** The schema keeps `org_id` everywhere; Cloud runs many orgs on the same model | M2 |
| P4 | **Teams** hold members and receive project roles; OIDC groups can map to teams | M2 |
| P5 | **Agents are principals**, beside users and service accounts: owned by a project, with a capability policy, a token TTL cap and their own audit trail | M2 |
| P6 | **Agents hold no long-lived secrets.** They get short-lived, scoped access tokens by workload-identity federation, user delegation (OAuth 2.1 + PKCE, for MCP clients) or vending from a parent token | M2 (federation, delegation); W1 (vending, §15 §8) |
| P7 | **Human sign-in in OSS:** first-run setup token, email and password (argon2id) with TOTP, and generic OIDC SSO. Passkeys follow. SAML and SCIM are brokered by an IdP (Keycloak, Dex, Authentik), not built in | M2; passkeys M2.x |
| P8 | **Auth is Rust, in the gateway**, on the `ControlStore`: no Node service beside the binary. Built from primitives, not a framework | M2 |
| P9 | **The console API contract** is OpenAPI 3.1 at `api/console/openapi.json`, the single source for the console's types, the mock server and the gateway's implementation | Now |
| P10 | **`operon-console-mock`**, an httpmock server with seed data, serves the contract before the gateway does | Now |

## 2. Why

§18 decided orgs, API keys, quotas and an `Authorizer` (D65, D66) but not how people and agents sign in, how a company organizes many applications, or what the console is. Three facts shape the answer:

1. **Self-hosters run one binary.** Anything that needs a second runtime (a Node auth server, Redis, a SQL database the operator did not choose) breaks the single-binary and air-gapped story (§10 §1).
2. **Most traffic will come from agents, not people.** Coding agents and application agents call the query, MCP and document APIs with credentials someone pasted into an environment variable. A long-lived key in a sandbox is the weakest point of the system (§15 §1, "Scoped credentials").
3. **A company has many applications and stages.** "One namespace per tenant" (§01 principle 5) is right for isolation, but a team with a search feature in staging and production needs those two namespaces grouped, with shared access and separate keys.

## 3. Editions and the console (P1)

| | OSS | Loam Cloud | BYOC |
|---|---|---|---|
| Console | Embedded, `/ui` on the native REST listener | Hosted | Hosted, managing the customer's cluster |
| Orgs | Exactly one | Many | One per customer |
| Identity | Built in (§6) | Built in, or an external IdP (Clerk or Keycloak) through OIDC | The customer's IdP through OIDC |
| Billing | None | Plans and metered usage | Contract |

The console discovers what it can show from `GET /api/v1/instance` (`edition`, enabled sign-in methods, feature flags), so one build serves all three. Cloud-only pages (billing) are hidden, not forked.

**Build and serving.** The console lives in `web/` (a pnpm workspace: `apps/console` and `packages/ui`, the shared `@loam/ui` design system). The `operon` crate embeds `web/apps/console/dist` behind a `console` feature and serves it with an SPA fallback at `/ui`; without the feature the binary builds with no Node toolchain. Fonts and assets are bundled: an air-gapped console loads nothing from the internet.

## 4. Tenancy (P2, P3, P4)

```
org ─┬─ members (users)            roles: owner, admin, member
     ├─ teams ── members
     ├─ projects ─┬─ environments ── namespace (1:1) ── collections, streams, links
     │            ├─ access: team or user → project role (admin, developer, viewer)
     │            ├─ agents
     │            └─ service accounts
     └─ audit log
```

- **Environment = namespace.** Isolation, quotas, encryption keys, routing and billing stay per namespace (§01 principle 5, §18 §6); an environment adds a human name (`production`), a slug and a `protected` flag. The namespace name is `<project>-<environment>` by default and is shown in the console.
- **Protected environments** (production, by default) require the project `admin` role for destructive actions: dropping a collection, deleting documents in bulk, creating API keys, and widening an agent's policy.
- **Roles map onto D66's `Rbac`**: a project role expands to `read`, `write` or `admin` grants on the project's namespaces. The `Authorizer` interface does not change; bindings gain a project scope.
- **OSS has one org** (P3), created at first-run setup. Multi-org self-hosting (agencies, holding companies) is a Cloud or BYOC concern.

## 5. Principals and agents (P5, P6)

Every request resolves to a principal. There are three kinds:

| Kind | Who | Authenticates with |
|---|---|---|
| `user` | A person | A console session (§6), or an access token from OAuth 2.1 |
| `service_account` | A non-human workload that is not an agent: a batch job, a CI pipeline | Workload-identity federation, or an API key |
| `agent` | An AI agent: a coding agent, an application's assistant, an MCP client | Short-lived access tokens only |

### 5.1 An agent

An agent is registered in a project and has:

- **an owner** (a user or team) accountable for it, and a description;
- **a policy**: the environments it may reach, and per environment the actions (`collections:read`, `collections:write`, `query`, `documents:delete`, `streams:produce`, `mcp:tools`) and optionally the collections;
- **a TTL cap** for its tokens: 15 minutes by default, at most 1 hour;
- **trust policies** that say which external identities may act as it (§5.2);
- **limits**: its own request-rate and concurrency quotas, below the namespace's (§18 §6);
- **a status**: `active` or `suspended`. Suspension revokes every token at once (§5.4).

Every token an agent holds and every call it makes is attributed to it in the audit log, including the user it acts for.

### 5.2 How an agent gets a token

There are three flows, and none of them stores a secret in the agent's environment.

1. **Workload-identity federation** (RFC 8693 token exchange). The agent's runtime already issues it an OIDC token: GitHub Actions, a Kubernetes service account, AWS, GCP or Azure workload identity, or a sandbox runtime. The agent posts that token to `POST /api/v1/oauth/token`. The gateway verifies it against a **trust policy** on the agent (issuer, audience, a subject pattern such as `repo:acme/search:ref:refs/heads/main`), then issues a Loam access token.
2. **User delegation** (OAuth 2.1 authorization code with PKCE). An MCP client such as Claude Code or Codex sends the user to the console's consent screen, which shows the agent, the environments and the actions. The resulting token carries the user as the subject and the agent as the actor, and it can never exceed either one's rights. This is the flow the MCP authorization spec expects (verify: the 2026-07-28 revision). The console serves RFC 8414 metadata at `/.well-known/oauth-authorization-server`.
3. **Vending** from a parent token (§15 §8). A sandbox supervisor holding a token asks for a narrower one for a sandbox: fewer actions, one environment, a shorter TTL. Vended tokens can only attenuate.

### 5.3 Access tokens

- **Format:** a JWT signed by the instance's Ed25519 key, published at `/.well-known/jwks.json`. Claims: `sub` (the principal), `act` (the actor chain, RFC 8693 §4.1), `org`, `env` (audience: one environment), `scp` (actions), `col` (optional collection list), `exp`, `iat` and `jti`.
- **Lifetime:** the agent's TTL cap (15 minutes by default, 1 hour at most). There are no refresh tokens for agents: they exchange again. Users' access tokens last 1 hour, with refresh tokens bound to the session.
- **Verification** is local in every gateway (signature, `exp`, `env` against the request's namespace, `scp` against the action), so it adds no round trip. The `Authorizer` still decides; the token only narrows what the principal may do.
- **Sender-constrained tokens** (DPoP, RFC 9449) are a follow-up for agents that run outside a sandbox.

### 5.4 Revocation

Short lifetimes are the main defence. On top of that, the gateways keep a revocation set of `jti`s and suspended principals, pushed through the `ControlStore` change feed, as §18 §6 does for API keys. Suspending an agent takes effect within the change feed's latency, not the token's TTL.

### 5.5 API keys

API keys stay for SDK users and service accounts that cannot federate (§18 §6: `loam_<key_id>_<secret>`, hashed). Proposed additions:

- every key is scoped to one environment;
- keys default to a 90-day expiry;
- the console marks keys that were not used for 30 days;
- keys cannot be issued to agents.

## 6. Human sign-in in OSS (P7)

| Method | In OSS | Notes |
|---|---|---|
| First-run setup | Yes | The server prints a one-time setup token to its log; `/ui/setup` creates the org and its owner with it, and the token then stops working |
| Email and password | Yes | argon2id (`argon2` 0.6); lockout after repeated failures; can be turned off once SSO works |
| TOTP two-factor | Yes | `totp-rs` 6, with recovery codes; an org setting can require it |
| OIDC single sign-on | Yes, several providers | Keycloak, Okta, Entra ID, Google Workspace, Authentik, Dex, GitHub (through Dex). Just-in-time provisioning, allowed email domains, group-to-team mapping. `openidconnect` 4 |
| Passkeys (WebAuthn) | M2.x | `webauthn-rs` 0.5 (MPL-2.0, used unmodified as a separate crate) |
| Magic links | No | They need SMTP, which many self-hosters do not run |
| SAML, SCIM | No | Put an IdP in front: Keycloak brokers SAML to OIDC (product doc 08). SCIM is a Cloud and BYOC feature |

**Sessions** are an `HttpOnly`, `Secure`, `SameSite=Lax` cookie holding an opaque id; the session lives in the `ControlStore` (12 hours idle, 30 days absolute). Mutating requests carry a CSRF token from `GET /api/v1/session`.

Sign-in methods do not depend on the edition: OSS gets SSO and two-factor. Loam charges for running Loam (Cloud, BYOC, support), not for security features.

## 7. Implementation: Rust primitives on the `ControlStore` (P8)

Auth runs in the gateway role, stores its state in the `ControlStore` (§18 §6), and decides through the `Authorizer` (§18 §7). That rules out the Rust auth frameworks we reviewed on 2026-09-26, because each one owns its storage:

| Framework | Why not |
|---|---|
| `better-auth` 0.10 (better-auth-rs) | 1.0 is in alpha; storage is SeaORM (SQL only), with a Redis cache; its org and session tables would sit outside the `ControlStore`, and openraft- or DynamoDB-backed installs have no SQL database |
| `torii` 0.5 | Last release 2025-12-23; SQL storage (SQLite, Postgres, MySQL) |
| `auth-framework` 0.4 | Young, one maintainer, and it bundles a whole authorization server whose security we would have to audit |

Loam builds the flows itself (sessions, exchange, consent, vending) on audited primitives:

| Need | Crate |
|---|---|
| OIDC client (SSO, and verifying federated tokens) | `openidconnect` 4 (MIT) |
| JWT signing and verification (EdDSA) | `jsonwebtoken` 11 (MIT) |
| Password hashing | `argon2` 0.6 (MIT or Apache-2.0) |
| TOTP | `totp-rs` 6 (MIT) |
| Passkeys | `webauthn-rs` 0.5 (MPL-2.0) |

better-auth's plugin boundaries and its organization, team and invitation shapes are a useful reference for naming; no code is copied.

## 8. The contract and the mock (P9, P10)

- **`api/console/openapi.json`** (OpenAPI 3.1) defines every `/api/v1` operation the console uses, plus the OAuth endpoints and the well-known documents. The console generates its TypeScript types from it (`openapi-typescript`); the gateway's handlers are tested against it in M2.
- **The data API is not in it.** The console reads collections through the existing native API (`/v1/namespaces/{ns}/collections`, the M1.6 wire contract W6–W9), using the environment's namespace.
- **`crates/operon-console-mock`** runs an `httpmock` server (MIT; used as a library, the `remote` feature) on a fixed port with **seed data**: one org (Acme), three teams, projects with development, staging and production environments, agents with trust policies and live tokens, API keys, audit events, usage series, and collections in the seeded namespaces. `cargo run -p operon-console-mock` serves it; `pnpm dev` in `web/` proxies `/api` and `/v1` to it.
- **Two tests keep the three in step:** every contract operation has a mock, and every seed record carries its schema's required properties.
- The mock is static: a `POST` returns a plausible created object, but the next `GET` does not show it. That is enough to build and review the console; state belongs to the real gateway.

## 9. Risks and open questions

1. **Amending D65** (P2) adds a level between org and namespace. The M2 plan must check that the directory (§18 §5.2) and name lookups keyed by `(org, name)` need nothing else: environments are metadata over namespaces, not a new routing key.
2. **Token exchange is security-critical code we write.** Mitigations: RFC test vectors, a fuzz target for token parsing, and a review by someone outside the team before M2 ships.
3. **MCP authorization** is still moving; the 2026-07-28 revision's requirements (resource indicators, protected-resource metadata) need checking (verify).
4. **Clock skew** matters for 15-minute tokens: verification allows 60 seconds.
5. **Q:** should service accounts be able to federate in OSS in M2, or only agents? The proposal says both.
6. **Q:** is one org per OSS install right for large companies that want separate orgs per business unit? The alternative is Cloud or BYOC.

## 10. Sources

- RFC 8693 (token exchange), RFC 8414 (authorization server metadata), RFC 7636 (PKCE), RFC 9449 (DPoP), OAuth 2.1 draft.
- MCP authorization specification, 2026-07-28 revision (verify).
- crates.io metadata for `better-auth`, `torii`, `auth-framework`, `openidconnect`, `jsonwebtoken`, `argon2`, `totp-rs`, `webauthn-rs` and `httpmock`, read 2026-09-26.
