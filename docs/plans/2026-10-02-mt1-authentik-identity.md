# MT1 — Authentik as the Identity Provider Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, claims, scopes, metric names, defaults), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: Planned** (2026-10-02). **Track MT** (design [§38](../design/38-knative-authentik-gitops.md) §4–§5, D447–D452, D458; amends [§19](../design/19-console-identity-and-agents.md) §6 and [§22](../design/22-showcase-suite.md) D-SC-3). Narrows D111 (D451): MT1 is the identity half of the unified auth plan for the native API, the console API and MCP. Depends on §19's M2 identity work being merged up to the token endpoint and sessions (`POST /api/v1/oauth/token`, the session cookie, the `ControlStore` principal records) and on D66's tuple outbox. If they have not merged, Task 0 records which tasks wait. Branches `mt1-t<N>`, stacked; PRs target `main`. Authentik runs only in CI containers and in the k3d e2e job; nothing Authentik-specific is linked into `loams`.

**Goal:**
- Authentik 2026.8.x, open-source edition only, configured entirely by Loams's blueprints, with a CI guard that fails if any Enterprise feature or licence appears (D452, D458).
- The gateway signs people in through Authentik (authorization code with PKCE for the console; device code for `loams login`) and issues Loams access tokens by RFC 8693 exchange of the Authentik token (D449).
- The `groups` claim becomes Loams teams and OpenFGA `team#member` tuples at sign-in and refresh.
- The showcase suite moves from Keycloak to Authentik (D-SC-3 superseded).

**Architecture:**
- **The IdP is a trusted issuer, not a library.** The gateway talks OIDC to Authentik through `openidconnect` 4 (already chosen in §19 §6). Nothing in the gateway names Authentik except the default discovery URL in the chart's values; any OIDC provider passes the same tests (D450).
- **One verifier.** Every listener verifies Loams access tokens only (§19 §5.3). Authentik tokens are accepted at exactly one place: the token endpoint's RFC 8693 grant with `subject_token_type=urn:ietf:params:oauth:token-type:access_token` or `…:id_token`, from an issuer in the trusted-issuer list.
- **Agents are unchanged.** Workload federation, user delegation and vending on Loams's token endpoint stay as §19 §5.2 specifies; Authentik is not in the agent path.
- **Configuration is data in Git.** `deploy/authentik/blueprints/loams.yaml` is the single source of Authentik's Loams objects; the e2e job loads it into a fresh Authentik and asserts the result.

**Tech Stack:** Rust 1.97.1, edition 2024, workspace lints. Workspace crates: `openidconnect` 4, `jsonwebtoken` (or the crate §19 chose for Ed25519 JWTs), `axum`, `reqwest`, `serde`, `tokio`, `tracing`, `proptest`. Test tools: Authentik `ghcr.io/goauthentik/server:2026.8.3` with Postgres 17 in `docker compose` for integration tests, pinned by digest; `kind` or k3d for the e2e job; Python 3.13 with `uv` for the blueprint lint. Task 0 checks the image digest, the licence files and that the compose stack starts in under 2 GiB RSS on the CI runner.

**Spec:**
- [§38](../design/38-knative-authentik-gitops.md) §4 (the licence split, the usable features, the integration shape, deployment), §5, §8 rows 1–5, §12 Q440, Q441, Q448.
- [§19](../design/19-console-identity-and-agents.md) §4 (teams, P4), §5.2–§5.4 (token endpoint, access tokens, revocation), §6 (sign-in).
- [§22](../design/22-showcase-suite.md) §4.4, §5, §7.3 (the suite's SSO and provisioning).
- D66 (the `Authorizer` and outbox), D111, D221.

## Global Constraints

Same as the M1 overview §8, plus:
- **Open-source edition only (D447, D458).** No task sets `AUTHENTIK_ENTERPRISE__*`, installs a licence, or uses a model from `authentik/enterprise/`. The guard of Task 1 runs on every PR that touches `deploy/authentik/` and on every Authentik bump.
- **No Authentik code in this repository.** Images are pulled; the chart is referenced (D452); blueprints are Loams's own YAML.
- **Loams tokens only on listeners.** No listener other than the token endpoint accepts a token whose `iss` is not the instance's own.
- **Loopback until this plan's Task 7.** Listeners keep D111's loopback defaults until the verifier is wired; Task 7 is the first that lets the native API, the console API and MCP bind beyond loopback, and only with TLS configured.
- **The build machine.** One cargo build at a time, the shared target, `-j 6`, lld. Authentik containers run only in the `identity` CI job and the e2e job, never during a cargo build on the build machine.
- **Commit areas:** `auth`, `deploy`, `ci`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **The Authentik application is `loams`**, with two OAuth2 providers: `loams-console` (confidential, authorization code + PKCE, redirect `https://<console-host>/api/v1/auth/oidc/authentik/callback`) and `loams-cli` (public, device code, no redirect). Scopes: `openid`, `profile`, `email`, `groups`, `offline_access` | One application per product; the CLI cannot hold a secret | Renaming later breaks existing installs' redirect URIs; documented in the upgrade notes |
| 2 | **The `groups` scope mapping** emits `groups` as a list of Authentik group names prefixed `loams-` (`[g.name for g in request.user.ak_groups.all() if g.name.startswith("loams-")]`) | Only groups meant for Loams reach the token; a company's other groups do not leak | Teams named without the prefix are invisible to Loams; documented |
| 3 | **Group → team mapping**: Authentik group `loams-<project>-<role>` maps to the Loams team `<project>-<role>` with project role `<role>` ∈ {`admin`, `developer`, `viewer`}; `loams-org-owner` and `loams-org-admin` map to org roles. Anything else is ignored and counted (`loams_auth_unmapped_groups_total`) | Deterministic, no mapping table to keep in sync | A company with existing group names writes them as `loams-` aliases in Authentik |
| 4 | **Exchange grant**: `grant_type=urn:ietf:params:oauth:grant-type:token-exchange`, `subject_token` = the Authentik access token, `subject_token_type=urn:ietf:params:oauth:token-type:access_token`, `audience` = a Loams environment id. The gateway validates the token by the issuer's JWKS (cached 10 minutes), `aud` = the `loams-console` or `loams-cli` client id, `exp`, and `iss` in `[auth] trusted_issuers`; it then issues a Loams access token with `sub` = the Loams user id (JIT-created on first sight, keyed by `(iss, sub)`), `org`, `env`, `scp` from the user's grants, and **no** `act` | §19 §5.2 flow 1 already specifies the exchange for workload identities; people use the same endpoint | None beyond §19's |
| 5 | **Tuple writes at sign-in**: the team set from the token is diffed against the user's current `team#member` tuples; additions and removals go through D66's outbox in the same `ControlStore` transaction that updates the session | Groups are only as fresh as the last sign-in or refresh; doing both in one transaction keeps them consistent | Removal waits for the next refresh (1 hour) unless Q448 gives back-channel logout |
| 6 | **The single binary keeps built-in sign-in** (D450). `[auth] builtin_password = true` is the default when no `trusted_issuers` are configured, `false` otherwise; the owner may change this (Q441) | Air-gapped and laptop installs must still work | One config flag |
| 7 | **The CI guard** is a Python script `scripts/authentik/guard.py` with three checks: (a) no blueprint entry has a `model` whose app label starts with `authentik_enterprise` or is one of the enterprise providers' and stages' app labels listed in `scripts/authentik/enterprise-apps.txt` (generated in Task 1 from the image's `INSTALLED_APPS` filtered to modules under `authentik.enterprise`); (b) the chart values set no licence and no `AUTHENTIK_TENANTS__ENABLED`; (c) against the running e2e instance, `GET /api/v3/enterprise/license/summary/` reports zero licences | A list derived from the image cannot go stale when Authentik moves a feature | If the API path moves, check (c) fails loudly and Task 1 updates it |

## Carried in

From §19: the token endpoint, the session model and JIT user records as merged by M2's identity work. From §22: the suite's app list and Keycloak realm export (`showcase/keycloak/realm.json`), whose clients and groups Task 6 ports.

## Review Focus

1. **No Enterprise surface.** Tests: Task 1 (`guard_rejects_enterprise_model`, `guard_rejects_licence_values`, the e2e `no_licence_installed`).
2. **Only Loams tokens on listeners.** Tests: Task 3 (`authentik_token_rejected_on_native_api`, `exchange_requires_trusted_issuer`, `exchange_checks_audience_and_expiry`).
3. **Groups cannot escalate.** Tests: Task 4 (`unprefixed_groups_are_ignored`, `unknown_role_suffix_is_ignored`, `removed_group_removes_tuple_on_refresh`).
4. **The device flow cannot be phished into another client.** Tests: Task 5 (`device_code_bound_to_loams_cli_client`).

## File structure

```
deploy/authentik/
  blueprints/loams.yaml              # application, providers, scope mappings, groups, flows (Ruling 1–2)
  values.yaml                       # Loams's values for the upstream chart (D452)
  compose.yaml                      # CI: authentik server + worker + postgres, pinned by digest
scripts/authentik/
  guard.py, enterprise-apps.txt     # Ruling 7
  e2e.sh                            # k3d: install, load blueprint, sign in headlessly, exchange, call the API
crates/loams-auth/src/oidc/        # (or wherever §19's M2 put sign-in) issuer config, JWKS cache, exchange grant
crates/loams-auth/tests/authentik.rs
crates/loams-cli/src/login.rs      # `loams login` device flow
showcase/authentik/                 # the suite's blueprint (Task 6)
docs/guides/identity-authentik.md
```

## Tasks

### Task 0: Reconcile and check

**Produces:** a short note in this plan's "Rulings made during execution": which §19 M2 pieces are merged (token endpoint, sessions, JIT users, outbox); the Authentik image digest for 2026.8.3 (or the latest 2026.8 patch) and its `LICENSE` and `authentik/enterprise/LICENSE` hashes; the owner's answers to Q441 and Q440 if given.

**Tests:** none (a reading task). **Commit:** `docs: MT1 task 0 findings`.

### Task 1: Blueprints, compose stack and the guard

**Files:** `deploy/authentik/{blueprints/loams.yaml,compose.yaml,values.yaml}`, `scripts/authentik/{guard.py,enterprise-apps.txt}`, `.github/workflows/identity.yml`.

**Produces:** the blueprint of Rulings 1–2 (application `loams`; providers `loams-console`, `loams-cli`; scope mapping `loams-groups`; groups `loams-org-owner`, `loams-org-admin`; the default enrolment flow with TOTP or WebAuthn required; a **device-code flow** `loams-device-code` (designation Stage Configuration, with a consent stage) assigned as the active Brand's device-code flow, which Authentik requires for the device grant and does not create by default); the guard of Ruling 7; a CI job `identity` that starts the compose stack, waits for `/-/health/ready/`, applies the blueprint and runs the guard.

**Tests:** `guard_rejects_enterprise_model` (a fixture blueprint with `authentik_providers_google_workspace.googleworkspaceprovider` fails); `guard_rejects_licence_values`; `guard_accepts_loams_blueprint`; `brand_has_device_code_flow` (the default Brand's `flow_device_code` is `loams-device-code`); `blueprint_applies_cleanly` (the worker's blueprint instance reports `successful`); `no_licence_installed`.

**Commit:** `deploy: Authentik blueprints and the open-source-edition guard`.

### Task 2: Trusted issuers and the JWKS cache

**Files:** `crates/loams-auth/src/oidc/issuer.rs`, `…/jwks.rs`, config `[auth] trusted_issuers = [{ issuer, audiences, jwks_ttl_s = 600 }]`.

**Produces:** `pub struct TrustedIssuers { … }` with `fn verify(&self, token: &str) -> Result<ExternalClaims, AuthError>`; discovery from `<issuer>/.well-known/openid-configuration`; a JWKS cache refreshed on TTL and on an unknown `kid` (at most once per 30 s per issuer).

**Tests:** `unknown_kid_triggers_one_refresh`; `refresh_is_rate_limited`; `wrong_issuer_rejected`; `audience_must_match`; `expired_rejected_with_60s_leeway`; property test `any_tampered_payload_rejected`.

**Commit:** `auth: trusted OIDC issuers with a JWKS cache`.

### Task 3: The exchange grant

**Files:** `crates/loams-auth/src/oidc/exchange.rs`; the token endpoint's grant dispatch.

**Produces:** Ruling 4's grant; JIT users keyed by `(iss, sub)`; the `loams_auth_exchanges_total{issuer,result}` counter.

**Tests:** `exchange_issues_loams_token_for_authentik_user` (against the compose stack, a user created by the blueprint test fixture); `exchange_requires_trusted_issuer`; `exchange_checks_audience_and_expiry`; `authentik_token_rejected_on_native_api` (the raw Authentik token on `GET /v1/namespaces` → 401); `loams_token_has_no_act_for_people`.

**Commit:** `auth: RFC 8693 exchange of IdP tokens for Loams tokens`.

### Task 4: Groups to teams to tuples

**Files:** `crates/loams-auth/src/oidc/groups.rs`.

**Produces:** Ruling 3's mapping and Ruling 5's diff-and-outbox write; `loams_auth_unmapped_groups_total`.

**Tests:** `prefixed_groups_map_to_teams`; `unprefixed_groups_are_ignored`; `unknown_role_suffix_is_ignored`; `removed_group_removes_tuple_on_refresh`; `tuples_and_session_commit_together` (fault injected between them leaves neither). Q448: if Authentik sends back-channel logout (Task 0 checks), add `backchannel_logout_revokes_session`; otherwise record the 1-hour bound in the guide.

**Commit:** `auth: map IdP groups to teams and OpenFGA tuples at sign-in`.

### Task 5: Console sign-in and `loams login`

**Files:** the console API's existing `GET /api/v1/auth/oidc/{provider}/start` (already in `api/console/openapi.json`) and a new `GET /api/v1/auth/oidc/{provider}/callback`, added to the OpenAPI contract, to `loams-console-mock` and to the contract tests (§19 P9, P10); `crates/loams-cli/src/login.rs`.

**Produces:** authorization code with PKCE (S256) and `state` and `nonce` checks for the console; the device-code flow for the CLI (`loams login --issuer <url>`), which polls Authentik, then exchanges at Loams's token endpoint and stores the Loams refresh state the way §30 stores credentials.

**Tests:** `callback_is_in_openapi_and_mock` (the contract test covers both operations); `pkce_s256_required`; `state_mismatch_rejected`; `nonce_replay_rejected`; `device_code_bound_to_loams_cli_client`; `cli_login_end_to_end` (compose stack, headless approval through Authentik's flow executor API).

**Commit:** `auth: console and CLI sign-in through an OIDC IdP`.

### Task 6: The showcase moves to Authentik

**Files:** `showcase/authentik/blueprints/*.yaml` (one OAuth2 or SAML provider per suite app, groups), removal of `showcase/keycloak/`, §22's app SSO table.

**Produces:** every suite app §22 §5 signs in to with OIDC (Forgejo, Zulip, GlitchTip, Loams) gets an Authentik OAuth2 provider; Plane keeps its chained SSO through Forgejo; groups move from the Keycloak realm export. The suite's group sync (§22 §7.3) reads Authentik's API instead of Keycloak admin events.

**Tests:** the SC1 e2e sign-in checks, re-run against Authentik; `guard_accepts_showcase_blueprints`.

**Commit:** `showcase: move the suite's IdP from Keycloak to Authentik`.

### Task 7: Leave loopback for the native API, console API and MCP

**Files:** listener config; `docs/guides/identity-authentik.md`.

**Produces:** non-loopback binds allowed for these three listeners only when TLS is configured and the verifier is on; startup error otherwise: `<listener> listen on <addr>: non-loopback addresses need [tls] and [auth] (MT1)`.

**Tests:** `non_loopback_without_tls_is_refused`; `non_loopback_with_tls_and_auth_starts`; the e2e `api_call_with_exchanged_token`.

**Commit:** `auth: allow non-loopback listeners behind TLS and token verification`.

### Task 8: Docs and close

**Files:** §38 (as built), §19 §6, the guide, `CHANGELOG.md`.

**Tests:** `identity` and e2e jobs green; `cargo deny check`.

**Commit:** `docs: record MT1 as built`.

## What MT1 leaves to others

| Item | Where |
|---|---|
| SCIM provisioning into Loams | `loam-platform` (D221), unless the owner moves it (Q440) |
| The other listeners (Qdrant, Elasticsearch, Postgres, Flight SQL) leaving loopback | Each listener's plan adopts MT1's verifier |
| Hosted Loams Cloud identity (Clerk or Authentik) | `loam-platform` and `loam-cloud` (Q442) |
| Authentik in the GitOps waves | MT3 |

## PR sizes

| Task | Expected size |
|---|---|
| 1 | ~500 lines (YAML, Python, CI) |
| 2 | ~600 lines |
| 3 | ~700 lines |
| 4 | ~600 lines |
| 5 | ~900 lines |
| 6 | ~600 lines (YAML) |
| 7 | ~300 lines |
| 8 | docs |

## Rulings made during execution

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| — | (Task 0 fills this table) | | |
