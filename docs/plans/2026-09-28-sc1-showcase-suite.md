# SC1 — Loams Commons: the Open-Source Showcase Suite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists what it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, versions, settings), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Amended 2026-09-28 (design §22 §13b):** PostHog is replaced by **OpenPanel** (FOSS, AGPL-3.0; upstream ClickHouse in SC1, behind forward-auth until OIDC lands) and **Matomo** (GPL-3.0; MariaDB; the LoginOIDC plugin). **Task 7 (TiDB) is dropped**, and every PostHog step below applies to OpenPanel and Matomo instead. The OpenPanel-on-Iceberg fork (D-SC-15) is a later SC plan.

> **Status: Not started** (written 2026-09-28, ahead of time). SC1 runs **after the Loams release (v1.0 = M1 + M2)**. It depends on the unified auth plan (Q30) being implemented, and it starts with a Task 0 that reconciles this plan with what then exists. The work lives in a new repository, **`dina-kar/loam-commons`** (D-SC-1); this plan stays in the engine repository beside the design it implements. Branches `sc1-t<N>`, stacked; PRs target `main` of `loams-commons`. SC1 changes no engine code: every engine gap it finds is filed as an engine issue and worked in the engine's own milestones (D-SC-10).

**Goal:** Ship design §22 (D-SC-1 … D-SC-10):
- a license-clean suite repository (Apache-2.0 glue, `NOTICE`, `LICENSES.md`, a license-check CI job);
- Keycloak as the one OIDC provider for Loams, Forgejo, Zulip, GlitchTip and (through Forgejo) Plane; PostHog behind forward-auth;
- one OpenFGA model across Loams and every app, projected into each app by durable provisioning sagas on Loams Durable, with a scheduled reconcile;
- every app's object storage on RustFS; Forgejo's issue indexer on Loams's Elasticsearch API; every service's logs in Loams over OTLP;
- the showcase features: unified hybrid search with OpenFGA filtering, a live activity feed on Loams Live, and an MCP assistant with delegated user tokens;
- Compose for development and an umbrella Helm chart;
- the dogfooding cutover: Loams's own team works in the suite, and the SC1 exit report.

**Architecture:** design §22 §3. `commons-control` (the suite's service) holds the outbox consumer, the app connectors, the sagas and the group sync, and runs as Resonate SDK workers against Loams Durable. The Commons portal is a small web app on `@loams/ui` and the Loams SDKs.

**Tech Stack:**
- Apps, official images pinned by digest (Task 0 fixes the versions): Plane Community Edition, Forgejo and Forgejo Runner, Zulip (`docker-zulip`), PostHog (FOSS build, hobby topology, optional profile), GlitchTip.
- Platform: the released `loams` binary (with `durable`), Keycloak, OpenFGA (≥ 1.21), RustFS 1.0.x, Caddy 2, oauth2-proxy, Postgres 17, Valkey, RabbitMQ, the OpenTelemetry Collector (contrib).
- `commons-control` and the portal: language per Q-SC-4 (TypeScript with the Resonate TypeScript SDK and the Loams TypeScript SDK is the default proposal); OpenFGA's official SDK for that language; `fga` CLI for model tests.
- Deployment: Docker Compose v2 with profiles; Helm 3 with an umbrella chart `loams-commons`.

**Spec:**
- [`docs/design/22-showcase-suite.md`](../design/22-showcase-suite.md): all of it.
- [`docs/design/19-console-identity-and-agents.md`](../design/19-console-identity-and-agents.md) (PR #39): §4 (projects, teams), §5.2 (user delegation for MCP), §6 (OIDC login).
- [`docs/design/18-metastore-backends-and-router.md`](../design/18-metastore-backends-and-router.md) §7 (the `Authorizer`, the OpenFGA model, the outbox).
- [`docs/design/21-durable-execution.md`](../design/21-durable-execution.md) §5 (tenancy), §6.1 (schedules), §6.3 (sagas), §6.4 (operations API), §6.5 (approval gates).
- [`docs/design/20-reactive-database-on-tikv.md`](../design/20-reactive-database-on-tikv.md) §7 (sync API), §10 (TiDB).
- [`docs/design/02-stream-engine.md`](../design/02-stream-engine.md) §7.1 (OTLP logs), [`06-search-and-vector.md`](../design/06-search-and-vector.md) §7 (the ES subset).
- [`docs/design/13-decision-log.md`](../design/13-decision-log.md): D11, D60, D61, D66, D67, D72, D73, D100, D111, D129, D142, D146; D-SC-1 … D-SC-10; Q30, Q43, Q-SC-1 … Q-SC-8.

## Depends on

| Dependency | Why | If it is late |
|---|---|---|
| **Loams v1.0** (M1 + M2) | The released binary, the stream API (D72), OTLP logs ingest (D73), RustFS as the default store (D61), audit events (D100) | SC1 does not start |
| **The unified auth plan (Q30), implemented** | Loams must accept Keycloak-issued OIDC tokens (or exchange them, §19 §5.2) on the native API, the ES API, MCP and the durable listener, and must listen beyond loopback with auth (D111) | SC1 does not start |
| **§19 OIDC login and teams** (M2) | Loams's console signs in with Keycloak; groups map to teams | Task 2 waits |
| **The `OpenFga` authorizer** (D66, D67, M2.x) | Loams checks the shared store natively | Tasks 3 and 9 use Loams's built-in RBAC for Loams resources and OpenFGA only in the portal until it lands |
| **D2** (durable tenancy, D142) | Sagas in a `commons` namespace, not `default` | Task 5 runs in `default` on a dedicated Loams instance |
| **R1/R2** (Loams Live sync API) | The activity feed | Task 10 moves to the end |
| **ES Phase B** | Forgejo's code indexer | The code indexer stays on bleve; not a gate |
| **Q43** (OTLP traces) | Traces | Logs only; not a gate |

## Global Constraints

- **Apps unmodified.** Official images by digest; no patched image in the default profiles. A patch that must ship lives in a public fork `dina-kar/<app>-commons` with its license's source offer (§22 §4.2), and needs an owner ruling first.
- **Glue talks to apps only through public APIs, OIDC and webhooks**, never by importing app code or writing to an app's database.
- **Licenses.** Suite code is Apache-2.0 with SPDX headers. `LICENSES.md` lists every image: name, version, digest, license, source URL, and whether it is copyleft; the `license-check` job fails if an image in the compose file or chart is missing from it, or if a pinned app's upstream license at that tag differs from the row.
- **Secrets.** No secret in the repository; compose reads `.env` generated by `commons-init`; Helm reads Kubernetes secrets. App admin tokens are held only by `commons-control`.
- **Idempotency.** Every saga step is idempotent and keyed by its durable promise id (D146); every step has a compensation or is documented as safe to leave.
- **Build machine.** Compose profiles are started one heavy app at a time on the development machine (PostHog alone needs about 16 GB); never during a cargo build.
- **Commit areas:** `compose`, `helm`, `idp`, `fga`, `control`, `portal`, `search`, `obs`, `ci`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **GlitchTip, not Sentry** (D-SC-2) | Sentry is FSL-1.1, not open source; GlitchTip is MIT and takes Sentry SDKs | Missing replays and profiling; Sentry self-hosted can be an optional non-OSS profile later |
| 2 | **Keycloak** as the IdP (D-SC-3) | Apache-2.0, no enterprise split, already §19's broker | Keycloak's weight (Java); Authentik is the fallback |
| 3 | **Plane signs in through Forgejo's OAuth2 provider** with Plane's Gitea provider | Generic OIDC is a paid Plane feature; Forgejo serves the Gitea endpoints Plane calls | If a Plane release drops or changes the Gitea provider, Plane falls back to forward-auth plus provisioned accounts |
| 4 | **PostHog is an optional profile**, behind forward-auth, FOSS build | SSO and RBAC are `ee/`; the hobby deploy is heavy and unsupported | The showcase without product analytics; documented |
| 5 | **OpenFGA is the source of truth; app ACLs are projections** (D-SC-4) | Most apps cannot call OpenFGA | Drift between reconcile runs; bounded by the schedule interval and reported |
| 6 | **Forgejo's issue indexer on Loams's ES API; the code indexer stays on bleve** | The issue indexer's queries are in ES Phase A; the code indexer needs highlighting and aggregations (Phase B) | None |

## Review Focus

1. **Authorization correctness.** No user sees, in any app or in search results, an object whose OpenFGA check denies them, after the reconcile interval. Tests: Task 3 (`fga model test` fixtures including the tenant fence), Task 5 (`grant_then_revoke_converges_in_every_app`, `drift_is_repaired_and_audited`), Task 9 (`search_never_returns_unreadable_objects`).
2. **Saga safety.** A crash at any step of project creation leaves either a complete project or a compensated one, never a half-provisioned app. Tests: Task 5 (`create_project_survives_kill_at_every_step`).
3. **License compliance.** Tests: Task 1 (`license-check`).
4. **Offboarding.** A disabled Keycloak user loses access to every app and to Loams. Tests: Task 5 (`offboarding_removes_access_everywhere`).

## File structure (in `dina-kar/loam-commons`)

```
LICENSE  NOTICE  LICENSES.md  README.md
compose.yaml  compose.dev.yaml  .env.example
init/                         # commons-init: Keycloak realm, OpenFGA store+model, RustFS buckets, Loams namespaces
  keycloak/realm-commons.json
  fga/model.fga  fga/model.fga.yaml   # model + tests
caddy/Caddyfile
otel/collector.yaml
control/                      # commons-control: outbox consumer, connectors, sagas, group sync, webhooks
  src/connectors/{forgejo,plane,zulip,posthog,glitchtip,loams}.ts
  src/sagas/{create_project,grant,revoke,offboard,reconcile,backfill}.ts
  src/webhooks/  src/groupsync/  test/
portal/                       # search, activity feed, assistant setup
charts/loams-commons/          # umbrella chart
e2e/                          # Playwright SSO flows, provisioning and search e2e
.github/workflows/{ci.yml,license-check.yml,e2e.yml}
docs/{architecture.md,licenses.md,operations.md,dogfooding.md}
```

### Task 0: Reconcile and verify

**Files:** read the engine as released (the auth plan's decisions, §19 as merged, the `OpenFga` authorizer, D2, the OTLP endpoint, the ES gateway's refusals); write `docs/plans/sc1-reconciliation.md` in the engine repo and fill this plan's "Rulings made during execution".

**Checks** (record each with its source or command):
1. Re-read each component's license and edition split at the version to pin (§22 §4.1): `gh api repos/<o>/<r> --jq .license`, the `LICENSE` file at the tag, `ee/` or enterprise directories; Forgejo and Forgejo Runner on Codeberg/code.forgejo.org.
2. Re-check SSO per app (§22 §5): Plane's Gitea provider against Forgejo end to end; PostHog FOSS login options; Zulip OIDC group sync on the pinned version; GlitchTip generic OIDC.
3. Resolve Q-SC-2 (a PostHog FOSS image), Q-SC-4 (the language of `commons-control`), Q-SC-6 (PostHog on Apache Kafka).
4. Measure the suite's memory and CPU on Compose, per profile.
5. Confirm what the released Loams provides: OIDC token acceptance on each surface, non-loopback listeners with auth, durable namespaces other than `default`, OTLP logs, the ES API's answers to Forgejo's issue-indexer requests (a quick run), Live sync API availability.

**Produces:** the reconciliation doc; pinned versions and digests; every "(verify)" in §22 resolved or carried with an owner.

**Commit:** `docs: reconcile SC1 with the released Loams`.

### Task 1: The suite repository and the license policy

**Files:** `LICENSE` (Apache-2.0), `NOTICE`, `LICENSES.md`, `README.md`, `.github/workflows/license-check.yml`, `compose.yaml` with Postgres 17, RustFS, Caddy and Loams only.

**Semantics:** `LICENSES.md` has one row per image (§22 §4.1 columns plus digest and source URL) and a section per copyleft app stating that it runs unmodified and linking its source at the pinned tag. `license-check` parses `compose.yaml` and `charts/**/values.yaml` for images and fails on any image without a row, and on any app whose upstream license at the pinned tag differs from its row.

**Tests:** `license-check` passes; a fixture with an unlisted image fails it; `docker compose config` validates.

**Commit:** `ci: add the suite skeleton and the license check`.

### Task 2: Keycloak, the realm and Loams's OIDC login

**Files:** `init/keycloak/realm-commons.json`, the `commons-init` container, compose services `keycloak` and `commons-init`.

**Semantics:** realm `commons` with one OIDC client per app and one for Loams, a `groups` claim (Group Membership mapper, full path off), TOTP required for admins, and a seeded group per suite project (`proj-<slug>-{admin,maintainer,developer,viewer}`). Loams's console and API accept the realm's tokens as the auth plan specifies.

**Tests:** `e2e/sso_loams.spec.ts`: sign in to Loams's console through Keycloak; a user in `proj-loams-engine-developer` sees that project's environments and not others.

**Commit:** `idp: add the Keycloak realm and Loams sign-in`.

### Task 3: The OpenFGA model and the group sync

**Files:** `init/fga/model.fga` (§22 §7.2), `init/fga/model.fga.yaml` (tests), `control/src/groupsync/`.

**Semantics:** the model is written into the store Loams uses (D67), beside Loams's and Lakekeeper's types; Keycloak admin events for group membership become `group#member` tuples, and a full resync runs at start.

**Tests:** `fga model test` with fixtures: every relation of §22 §7.2, the tenant fence (`other_org_member_never_reads`), public repositories, org-visible channels. `groupsync_adds_and_removes_members`, `groupsync_full_resync_is_idempotent`.

**Commit:** `fga: add the cross-app model and the Keycloak group sync`.

### Task 4: The apps, with SSO and RustFS

**Files:** compose services and profiles for Forgejo (+ runner), Zulip, GlitchTip, Plane, PostHog; `caddy/Caddyfile`; per-app init in `commons-init` (buckets, OIDC clients, admin tokens into `.env`).

**Semantics:**
- Forgejo: OIDC login source to Keycloak with `--group-claim-name groups` and a group-team map; storage on RustFS (LFS, attachments, packages, actions artifacts); Forgejo also registered as an OAuth2 application provider for Plane.
- Zulip: `SOCIAL_AUTH_OIDC_ENABLED_IDPS` to Keycloak, `SOCIAL_AUTH_SYNC_ATTRS_DICT` for groups; uploads on RustFS.
- GlitchTip: allauth OIDC to Keycloak; uploads on RustFS.
- Plane: `GITEA_HOST` = the Forgejo URL, client from Forgejo; uploads on RustFS; telemetry export pointed at the suite's collector or turned off.
- PostHog (profile `posthog`): FOSS build, object storage on RustFS, behind Caddy forward-auth with oauth2-proxy against Keycloak.

**Tests:** `e2e/sso_*.spec.ts` for each app: one Keycloak sign-in reaches Forgejo, Zulip, GlitchTip and Plane with no second password; PostHog is unreachable without the edge session. `storage_*` checks: an upload in each app lands in its RustFS bucket.

**Commit:** `compose: add the apps with single sign-on and RustFS storage`.

### Task 5: `commons-control`: connectors, sagas and reconcile

**Files:** `control/src/{connectors,sagas,outbox}/`, `control/test/`.

**Semantics:**
- The outbox consumer reads tuple changes (from the control plane's outbox, §18 §7) and starts one durable workflow per change, with the change id as the idempotency key.
- `create_project`: Loams environments → Forgejo team and repositories → Plane project → Zulip channel → PostHog project (if enabled) → GlitchTip project and team → tuples; each step idempotent; on a permanent failure, compensations run in reverse (archive, never delete). Deleting a project's repositories needs an approval gate (§21 §6.5).
- `grant` / `revoke`: per-app membership calls from `ListUsers` on the affected objects.
- `offboard`: removes the user from every app and revokes personal tokens where the app's API allows.
- `reconcile`: a durable schedule (every 10 minutes by default) comparing each app's members with OpenFGA, fixing drift per Q-SC-8's policy, one audit event per fix.

**Tests:** connector unit tests against recorded app responses; `create_project_survives_kill_at_every_step` (kill `commons-control` and `loams` at each step; the operation completes or compensates); `grant_then_revoke_converges_in_every_app`; `drift_is_repaired_and_audited`; `offboarding_removes_access_everywhere`.

**Commit:** `control: add the connectors, provisioning sagas and reconcile`.

### Task 6: Forgejo's issue indexer on Loams's Elasticsearch API

**Files:** Forgejo `[indexer] ISSUE_INDEXER_TYPE = elasticsearch`, `ISSUE_INDEXER_CONN_STR` pointing at Loams's ES listener with a Loams credential; `e2e/forgejo_search.spec.ts`.

**Semantics:** Forgejo writes and searches issues through Loams; the code indexer stays on bleve (Ruling 6).

**Tests:** Forgejo's own ES issue-indexer integration tests run against Loams (a CI job that builds Forgejo's test binary at the pinned tag and points `TEST_INDEXER_CODE_ES_URL`/the issue equivalent at Loams; exact variable in Task 0); e2e: create, edit, close and search issues with label, milestone and assignee filters. Every refusal becomes an engine issue; the task is done when the suite passes or every failure has an engine issue and a documented fallback.

**Commit:** `search: run Forgejo's issue search on Loams`.

### Task 7: TiDB experiments (optional; resolves Q-SC-3 and Q-SC-5)

**Files:** `compose.tidb.yaml` (R1's playground or tidb-operator), `docs/tidb.md`.

**Semantics:** run Forgejo's `make test-mysql` and OpenFGA's MySQL datastore tests against TiDB; record failures. Adopt TiDB for either only if its suite passes.

**Tests:** the two suites' results recorded in `docs/tidb.md`.

**Commit:** `docs: record Forgejo and OpenFGA on TiDB`.

### Task 8: Observability over OTLP

**Files:** `otel/collector.yaml`, compose and chart wiring.

**Semantics:** the collector reads container logs (filelog receiver) and receives each app's OTLP where it exists, and exports logs to Loams's OTLP endpoint (D73) with `loams-namespace: commons-obs` and `loams-stream: <service>`; a link materializes them into the collection `commons.logs`. Traces wait for Q43.

**Tests:** `logs_from_every_service_arrive` (one known log line per service is searchable in `commons.logs` within 30 s).

**Commit:** `obs: ship every service's logs to Loams`.

### Task 9: Unified search and the portal

**Files:** `control/src/webhooks/`, `control/src/sagas/backfill.ts`, `portal/`.

**Semantics:** webhooks from Forgejo, Plane, GlitchTip and Zulip (outgoing webhooks or the event queue) become records in per-app streams; links materialize `commons.issues`, `commons.messages`, `commons.errors`, `commons.code` (repository files through Forgejo's API, batch) with text, filters, embeddings and `acl_object`; backfill runs as a D1 bulk import operation. The portal's search takes the caller's readable objects from OpenFGA (`ListObjects`, cached ≤ 5 s) as a `terms` filter and runs one hybrid query with RRF.

**Tests:** webhook signature verification per app; `search_never_returns_unreadable_objects` (property test over random grants); `revoked_access_disappears_within_cache_ttl`; backfill idempotency.

**Commit:** `portal: add unified search with OpenFGA filtering`.

### Task 10: The activity feed on Loams Live

**Files:** `portal/src/feed/`, a Live app `commons` with a table `activity` and its mutation.

**Semantics:** `commons-control` writes one activity document per webhook event through a Live mutation (idempotency key = the event's delivery id); the portal subscribes through the Live sync API, filtered by the caller's projects.

**Tests:** `feed_updates_live` (an issue created in Plane appears in a subscribed browser within 2 s); `feed_respects_projects`.

**Commit:** `portal: add the live activity feed`.

### Task 11: The MCP assistant

**Files:** `docs/assistant.md`, portal page for connecting an MCP client.

**Semantics:** a user connects an MCP client to Loams's MCP server with §19's user-delegation flow; the tools search the commons collections with the user's rights. Actions that change apps (open an issue, post a message) are exposed only through `commons-control` operations with an approval gate.

**Tests:** `assistant_sees_only_callers_objects`; `assistant_action_needs_approval`.

**Commit:** `portal: connect the MCP assistant with delegated access`.

### Task 12: The Helm chart

**Files:** `charts/loams-commons/` (umbrella), values for dev and production.

**Semantics:** upstream charts where they exist (Forgejo, OpenFGA, Keycloak or its operator, Zulip, Plane, GlitchTip; Task 0 lists them), Loams's chart and RustFS's chart; `commons-init` as a Helm hook job; PostHog optional; Postgres external in production values.

**Tests:** `helm lint`, `helm template` against kubeconform; the e2e suite (Tasks 2, 4, 5, 9) on a kind cluster, nightly.

**Commit:** `helm: add the umbrella chart`.

### Task 13: Dogfooding cutover and the exit report

**Files:** `docs/dogfooding.md`; `docs/plans/sc1-exit-report.md` in the engine repo.

**Semantics:** staged adoption by the Loams team: GlitchTip (loam-cloud's Sentry SDKs re-pointed by DSN), then Forgejo as a mirror of the GitHub repositories with CI on Forgejo Runner, then Zulip, then Plane for one milestone, then PostHog (if kept). Each stage has a rollback. GitHub stays the source of truth for code until the owner decides otherwise.

**Exit gates:** one sign-in reaches every app except PostHog's documented gap; a grant and a revoke converge in every app within one reconcile interval; project creation survives a crash at every step; unified search never leaks; the license check passes; every service's logs are in Loams; the team used the suite for one full milestone, with every incident recorded.

**Commit:** `docs: record the SC1 exit report`.

### Task 14: Documentation

**Files:** `docs/{architecture,licenses,operations}.md` in `loams-commons`; in the engine repo: §22 as-built notes, decision statuses, `docs/plans/README.md` (SC1 status).

**Commit:** `docs: record SC1 as built`.

## PR grouping

| PR | Tasks | Title |
|---|---|---|
| A | 0 | SC1 (1/12): reconcile with the released Loams |
| B | 1 | SC1 (2/12): suite skeleton and license check |
| C | 2 | SC1 (3/12): Keycloak and Loams sign-in |
| D | 3 | SC1 (4/12): the OpenFGA model and group sync |
| E | 4 | SC1 (5/12): the apps with SSO and RustFS |
| F | 5 | SC1 (6/12): connectors, sagas and reconcile |
| G | 6, 7 | SC1 (7/12): Forgejo search on Loams; TiDB experiments |
| H | 8 | SC1 (8/12): logs to Loams over OTLP |
| I | 9 | SC1 (9/12): unified search |
| J | 10, 11 | SC1 (10/12): activity feed and assistant |
| K | 12 | SC1 (11/12): the Helm chart |
| L | 13, 14 | SC1 (12/12): dogfooding, exit report, docs |

## Rulings made during execution

None yet.

## Self-review

- Every §22 decision has a task: D-SC-1 (Task 1), D-SC-2 (Task 4), D-SC-3 (Task 2), D-SC-4 (Task 3), D-SC-5 (Task 5), D-SC-6 (Task 1), D-SC-7 (Tasks 4, 6, 7, 8), D-SC-8 (Tasks 9–11), D-SC-9 (Tasks 1, 12), D-SC-10 (Global Constraints, Task 0).
- Every open question has an owning task: Q-SC-1, Q-SC-2, Q-SC-4, Q-SC-6 (Task 0), Q-SC-3, Q-SC-5 (Task 7), Q-SC-7 (Task 4), Q-SC-8 (Task 5).
