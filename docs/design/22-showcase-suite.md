# 22 — Loams Commons: an Open-Source Showcase Suite on Loams

Status: **Proposed** · 2026-09-27; amended 2026-09-28 by the owner (§13a, §13b: PostHog and Sentry dropped, OpenPanel and Matomo added, TiDB dropped); amended 2026-09-29 by the owner ([§28](28-loams-postgres.md) D230: the apps' Postgres is plain Postgres 17 on CloudNativePG, §6.4). The direction is the owner's: after a Loams release, assemble an open-source suite of project management (Plane), a git forge (Forgejo), chat (Zulip), product analytics (PostHog) and error tracking (Sentry), deploy it on Loams behind a Loams control plane with single sign-on over OIDC and OpenFGA authorization across every app, and use it to build Loams itself: "it's a loop". This document turns that direction into decisions **D-SC-1 … D-SC-10** and open questions **Q-SC-1 … Q-SC-8**. Final numbers are assigned at merge, because other open PRs hold placeholders. Everything this document chooses on top of the owner's list is a **proposal** until the owner confirms it. The milestone is **SC1**, planned in [`docs/plans/2026-09-28-sc1-showcase-suite.md`](../plans/2026-09-28-sc1-showcase-suite.md). Nothing here is built before Loams's release (v1.0, M1 + M2) and the unified auth plan (Q30).

> **Amended 2026-10-02** by [§38](38-knative-authentik-gitops.md) (proposed): **D-SC-3 is superseded by D447**. The suite's IdP is Authentik's open-source edition (MIT tree only, no licence key, a CI guard, D458), configured by blueprints (D452), on CloudNativePG. §4.4's concern about Authentik's enterprise split is answered by the guard and by §38 §4.2's feature list. Plan MT1 Task 6 moves the suite.

Markers: **(verify)** is not checked against a primary source, or was checked only against a secondary one; the SC1 task that depends on it resolves it first. **(estimate)** is computed, not measured. Every license and edition claim was read on 2026-09-27 from the source named in §15; editions and licenses change, so SC1 Task 0 re-reads each one.

---

## 1. Summary

| # | Proposal | Status |
|---|---|---|
| D-SC-1 | **The suite is named Loams Commons** (repository `dina-kar/loam-commons`); it is a separate repository, not part of the engine workspace | Proposed |
| D-SC-2 | **The apps are Plane, Forgejo, Zulip, PostHog and GlitchTip**, each deployed **unmodified from its official images** as a separate service. **GlitchTip replaces Sentry**, because Sentry is under the Functional Source License (FSL-1.1), which is not an OSI open-source license; GlitchTip is MIT and speaks Sentry's SDK protocol | Proposed |
| D-SC-3 | **Keycloak is the identity provider**: every app and Loams itself are OIDC clients of one realm; Loams's own gateway stays an OIDC relying party, as §19 P7 already plans | Proposed |
| D-SC-4 | **One OpenFGA model spans every app** (§7): a suite `project` fans out to a Loams namespace, a Plane project, Forgejo repositories, Zulip channels, a PostHog project and a GlitchTip project. OpenFGA is the source of truth; each app's native permissions are a **projection** that a reconciler writes through the app's admin API | Proposed |
| D-SC-5 | **Enforcement by provisioning sagas on Loams Durable** (§21): tuple changes leave the control plane's outbox (§18 §7) and run as durable, compensating workflows that call each app's API; a scheduled reconcile repairs drift. OIDC group claims are a second, coarse layer where an app supports them | Proposed |
| D-SC-6 | **The suite's own code is Apache-2.0**; the apps keep their licenses as separate, unmodified services; the repository ships `NOTICE`, `LICENSES.md` (the matrix of §4) and a written-offer and source-link page for the copyleft services | Proposed |
| D-SC-7 | **Loams replaces what it honestly can** (§6): object storage on RustFS for every app, Forgejo's issue search on Loams's Elasticsearch API, cross-app search, activity and logs in Loams, Forgejo's and OpenFGA's databases considered for TiDB. **Postgres stays** for Plane, Zulip, PostHog, GlitchTip and OpenFGA; **ClickHouse and Kafka stay** for PostHog | Proposed |
| D-SC-8 | **The showcase features are Loams's**: unified hybrid search over every app, an activity feed on Loams Live, provisioning sagas, OTLP logs and traces, and an MCP assistant over all of it, each enforcing OpenFGA at query time (§8) | Proposed |
| D-SC-9 | **Docker Compose for development, Helm for Kubernetes** (§9); the upstream Helm charts are used where they exist, and an umbrella chart wires them | Proposed |
| D-SC-10 | **SC1 is a post-release milestone** after v1.0, the unified auth plan (Q30), M2's OTLP logs ingest (D73) and Loams's OIDC client (§19 P7); it changes no engine code, and every engine gap it finds becomes an engine issue, not a suite patch | Proposed |

## 2. Goals and non-goals

### 2.1 Goals

1. **Dogfooding: the loop.** Loams's own team plans in Plane, hosts code and CI on Forgejo, talks in Zulip, reads product analytics in PostHog and triages errors in GlitchTip, all on Loams. Every day of work exercises Loams's search, streams, Live, durable execution, auth and storage under a real, mixed load, and every failure lands in the suite it broke. The loam-cloud repository currently uses hosted Sentry and PostHog (Clerk for auth); SC1's exit moves the Loams team's own use onto the suite.
2. **A showcase that sells the AI-native cloud (D116).** One login, one permission model, one search box over issues, code, chat, analytics and errors, an activity feed that updates live, and an assistant that answers from all of it while respecting who is asking. Each of those is a Loams feature (§8), visible in a product people already know.
3. **A reference deployment.** Compose and Helm that a company can run as its own open-source workspace, with Loams as the data and identity plane.
4. **License-clean.** Every component is open source under an OSI license, used as its license allows, with the obligations written down (§4).

### 2.2 Non-goals

- **Replacing the apps' OLTP databases with Loams.** Plane, Zulip, GlitchTip, OpenPanel and OpenFGA need Postgres (PostHog was dropped, §13b; Matomo needs MariaDB), or for some MySQL, with interactive multi-statement transactions. Loams's retrieval engine keeps D2 (OLTP is out of scope), and its planned Postgres wire surface is analytical and autocommit. TiDB (§20 §10) speaks MySQL, which only Forgejo and OpenFGA can use (§6).
- **Forking the apps.** Patches go upstream first. A fork is a last resort, with its obligations in §4.2.
- **Replacing ClickHouse under PostHog.** PostHog's query layer is ClickHouse SQL (HogQL compiles to it); Loams's analytics (M4, Iceberg) is not a drop-in (§6.4).
- **A new chat, forge or tracker.** The suite composes existing products.
- **Multi-org hosting of the suite.** One org per install, as in §19 P3.

## 3. Architecture

```
                         browser / CLI / agents (MCP)
                                   │  one domain, TLS
                          ┌────────┴─────────┐
                          │  edge proxy      │  Caddy (Apache-2.0): TLS, routing by host,
                          │                  │  forward-auth only for apps without OIDC
                          └────────┬─────────┘
       ┌──────────────┬────────────┼─────────────┬──────────────┬─────────────┐
       │              │            │             │              │             │
  ┌────┴────┐   ┌─────┴────┐  ┌────┴───┐   ┌─────┴────┐   ┌─────┴────┐  ┌─────┴──────┐
  │ Plane   │   │ Forgejo  │  │ Zulip  │   │ PostHog  │   │GlitchTip │  │ Commons    │
  │         │   │ + runner │  │        │   │ (hobby)  │   │          │  │ portal     │
  └──┬──┬───┘   └──┬───┬───┘  └─┬───┬──┘   └──┬───┬───┘   └──┬───┬───┘  │ (search,   │
     │  │ webhooks │   │        │   │ events  │   │          │   │      │ activity,  │
     │  └──────────┼───┼────────┼───┼─────────┼───┼──────────┘   │      │ assistant) │
     │  OIDC       │   │        │   │         │   │   OIDC       │      └─────┬──────┘
     ▼             ▼   ▼        ▼   ▼         ▼   ▼              ▼            │
 ┌──────────────────────────────────────────────────────────────────────┐     │
 │ Loams control plane                                                  │◄────┘
 │  Keycloak (IdP, realm "commons")  ── OIDC ──►  every app, Loams      │
 │  OpenFGA (one store, model §7)                                       │
 │  commons-control: outbox → provisioning sagas (Loams Durable, §21)   │
 │                   reconcile schedule, group sync, app connectors     │
 └───────────────┬──────────────────────────────────────────────────────┘
                 │ native API, ES API, OTLP, Live sync API, MCP
 ┌───────────────┴──────────────────────────────────────────────────────┐
 │ Loams: collections (hybrid search), streams (webhooks, activity,     │
 │ OTLP logs), Loams Live (activity feed, control state), Durable       │
 │ (sagas), MCP server; TiKV/TiDB where used (§6)                       │
 └───────────────┬──────────────────────────────────────────────────────┘
                 │ S3
 ┌───────────────┴────────────┐   ┌───────────────────────────────────┐
 │ RustFS (Apache-2.0)        │   │ Postgres 17, Valkey, RabbitMQ,    │
 │ one bucket per app + Loams │   │ ClickHouse, Kafka/Redpanda        │
 └────────────────────────────┘   │ (what Loams does not replace, §6) │
                                  └───────────────────────────────────┘
```

### 3.1 Components

| Component | Role | Source |
|---|---|---|
| **Keycloak** | The OIDC provider for every app and for Loams; users, groups, MFA, social and SAML brokering; SCIM out through an extension if needed (§5.2) | Official image, unmodified |
| **OpenFGA** | The authorization store and checker; one store, one model (§7) | Official image, unmodified; Postgres datastore (MySQL/TiDB is Q-SC-3) |
| **commons-control** | The suite's own service (Apache-2.0): the tuple outbox, the app connectors, the provisioning sagas and the reconcile schedule, the group sync from Keycloak to OpenFGA, and the webhook receiver that turns app events into Loams stream records | New, in the suite repo. Language: TypeScript or Python on the Resonate SDK against Loams Durable (Q-SC-4) |
| **Commons portal** | A small web app: one search box, the activity feed, the assistant; built on `@loams/ui` (§19 §3) and the Loams SDKs | New, in the suite repo |
| **Loams** | Search, streams, Live, Durable, MCP, OTLP, Flight SQL; its own OIDC login (§19 P7) and `OpenFga` authorizer (§18 §7) | The released `loams` binary |
| **RustFS** | The object store for every app and for Loams (D61) | Official image |
| **Caddy** | TLS and host routing; forward-auth (with oauth2-proxy, MIT) only for an app without free OIDC | Official image |
| **Apps** | Plane, Forgejo (+ Forgejo Runner), Zulip, PostHog, GlitchTip | Official images, unmodified (D-SC-2) |

### 3.2 The control plane is Loams's, extended

The owner's "Loams control plane in front of all of them" is the §19 control plane plus two things it does not yet have: an IdP to federate *other* applications (Loams's gateway is an OIDC client, not a provider, §19 P7: "SAML and SCIM are brokered by an IdP"), and connectors that project OpenFGA grants into applications that do not call OpenFGA. The suite adds both outside the engine: Keycloak as the provider, and `commons-control` as the projector. Loams's console stays the place where Loams's own projects, environments and agents live; the suite's `project` type (§7) maps one-to-one onto a §19 project, so the console and the suite share the same unit.

## 4. Licenses (D-SC-6)

### 4.1 The matrix

| Component | License (verified 2026-09-27) | OSI open source | Edition caveat | Used as |
|---|---|---|---|---|
| Plane | AGPL-3.0 | Yes | No `ee/` directory in the repository; the paid plans ("One", Pro, Business) are a separate commercial build, and OIDC/SAML SSO and LDAP are among their features | Unmodified service, Community Edition |
| Forgejo | GPL-3.0-or-later (since v9; earlier releases MIT) | Yes | None; one edition | Unmodified service |
| Forgejo Runner | GPL-3.0-or-later | Yes | — | Unmodified service |
| Zulip | Apache-2.0 | Yes | None in code: SAML, OIDC, SCIM and group sync are "Self-managed" on the free self-hosted plan; paid plans add vendor support and push notifications past 10 users | Unmodified service |
| PostHog | MIT, **except `ee/`**, under the PostHog Enterprise License (production use needs a subscription); `PostHog/posthog-foss` is the MIT mirror "with all proprietary code removed" | Yes (outside `ee/`) | Self-hosting is the hobby compose only, "officially unsupported"; "all paid-plan features are Cloud-only" | Unmodified service, **FOSS build** (Q-SC-2), optional profile |
| Sentry (and `getsentry/self-hosted`) | FSL-1.1-Apache-2.0 (each release becomes Apache-2.0 two years later; BUSL before late 2023) | **No** | — | **Not used** (D-SC-2) |
| GlitchTip | MIT | Yes | None | Unmodified service |
| OpenFGA | Apache-2.0 | Yes | — | Unmodified service |
| Keycloak | Apache-2.0 | Yes | — | Unmodified service |
| RustFS | Apache-2.0 | Yes | — | Unmodified service |
| Caddy, oauth2-proxy | Apache-2.0, MIT | Yes | — | Unmodified services |
| Postgres, Valkey, RabbitMQ, ClickHouse, Redpanda/Kafka, SeaweedFS, Temporal | PostgreSQL, BSD-3-Clause, MPL-2.0, Apache-2.0, **Redpanda: BSL-1.1** core, with Redpanda Community License parts (`redpanda-data/redpanda/licenses/{bsl,rcl}.md`) / Kafka: Apache-2.0, SeaweedFS: Apache-2.0, Temporal: MIT | Yes, except Redpanda | PostHog's hobby compose uses Redpanda; use Apache Kafka if PostHog runs on it (Q-SC-6) | Unmodified services |
| Loams | Apache-2.0 | Yes | — | The platform |
| Suite glue (`commons-control`, portal, charts, compose) | **Apache-2.0** | Yes | — | Ours |

### 4.2 What the licenses require of the suite

**Unmodified, separate services** (the plan of record):

- **Loams's own rule (D11)** forbids AGPL, BSL, SSPL and ELv2 in *linked* dependencies. The suite links none: each app is a separate process reached over HTTP, OIDC or SQL, as ScyllaDB Alternator is in Loams's CI (D60) and TiDB is in Loams cloud (D126). The suite repository documents this in `LICENSES.md`.
- **Mere aggregation.** Shipping GPL and AGPL services beside Apache-2.0 glue in one compose file or umbrella chart is aggregation (GPL-3.0 §5, last paragraph); the glue keeps its license. The glue talks to the apps only through their public APIs, never by importing their code.
- **Redistribution.** The suite references official images by digest and does not rebuild them. If it ever redistributes an image or binary (an air-gapped bundle), it must pass on the license texts and the corresponding source (GPL-3.0 §6, AGPL-3.0 §6) for the copyleft ones; `LICENSES.md` links each pinned version's source.
- **AGPL network use (Plane).** AGPL-3.0 §13 obliges whoever *modifies* the program to offer the modified source to its network users. Running unmodified Plane triggers nothing beyond keeping Plane's own source-link notice intact. **FSL (Sentry)** would allow internal use and even this showcase's non-competing use, but it is not open source, and the owner's rule is an open-source suite; so Sentry is out (§4.3).
- **PostHog's `ee/`.** The default images include `ee/` code, whose production use needs a PostHog subscription under the Enterprise License. To keep the suite open source by construction, the suite pins a FOSS build (the `posthog-foss` mirror, active as of 2026-09-27, or an image built from it) and documents what the FOSS build lacks: SAML, OIDC and Google SSO, SCIM, and role-based access control all live in `ee/` (§5, §7.3). Q-SC-2 decides whether a FOSS image exists or the suite must build one.

**If the suite ever patches an app:**

- **Plane (AGPL-3.0):** the patched source must be offered to every user who interacts with the patched Plane over the network, under AGPL-3.0; the Plane UI's source link must point at the patched tree. The patch itself is AGPL-3.0.
- **Forgejo (GPL-3.0-or-later):** serving a patched Forgejo over a network triggers nothing; distributing the patched binary or image requires its source under GPL-3.0. Forgejo accepts contributions under its DCO and license.
- **Zulip, GlitchTip, OpenFGA, Keycloak (Apache-2.0/MIT):** keep notices; mark changed files (Apache-2.0 §4(b)).
- **PostHog:** patches outside `ee/` are MIT. Never modify `ee/`.

**Rule for the suite (proposed in D-SC-6):** patches go upstream; a patch that must ship before upstream accepts it lives in a public fork named `dina-kar/<app>-commons`, with the license's source offer, and is removed when upstream releases the change.

### 4.3 Sentry or GlitchTip

| | Sentry self-hosted | GlitchTip |
|---|---|---|
| License | FSL-1.1-Apache-2.0: source-available, converts to Apache-2.0 two years after each release; not OSI | MIT |
| SDK protocol | Native | Sentry's ingest API: official Sentry SDKs work by DSN (errors and performance transactions; per-SDK coverage to verify) |
| Features | Full: issues, performance, profiling, replays, crons, uptime, feedback | Errors, performance (transactions), uptime monitors, releases, source maps, OTLP logs ingest (`GLITCHTIP_ENABLE_LOGS`); no replays or profiling (verify) |
| SSO | SAML2, Google, GitHub (free); no generic OIDC | Generic OIDC through django-allauth (free) |
| Infrastructure | Postgres + pgbouncer, Redis, Memcached, Kafka, ClickHouse, SeaweedFS, Snuba (API plus about 20 consumers), Relay, Symbolicator, taskbroker, Vroom, uptime-checker, nginx: dozens of containers | Django web and worker, Postgres 14+; Valkey/Redis optional (falls back to Postgres); 256–512 MB RAM |
| On Loams | Little to replace; ClickHouse and Kafka stay | Postgres stays; uploads on RustFS; its events can also be mirrored into Loams (§8.4) |

**Recommendation (D-SC-2): GlitchTip.** It keeps the suite open source, it runs in a few containers, and because it speaks Sentry's protocol the suite loses nothing that matters for dogfooding error triage. The loam-cloud repository's Sentry SDKs keep working by changing the DSN. If profiling or session replay become important, Sentry self-hosted can run as an optional, clearly labelled non-OSS profile outside the suite's default, the way Alternator runs in CI.

### 4.4 The identity provider

| Candidate | License | OIDC provider | Groups in tokens | SCIM | Fit |
|---|---|---|---|---|---|
| **Keycloak** | Apache-2.0 (CNCF incubating) | Yes, mature | Yes (Group Membership mapper) | A native SCIM Realm API, Preview in 26.7 behind `--features=scim-api` (not yet for production) | Full admin REST API for automation; brokers SAML and social logins to OIDC, which §19 P7 already names; Java, one container plus Postgres |
| Ory Kratos + Hydra | Apache-2.0 | Hydra is an OAuth2/OIDC server; Kratos is identity | No native groups claim (kratos#2528); claims come from a login/consent app we write | SCIM, SAML and B2B organizations need Ory Network or the Ory Enterprise License | Needs a login and consent UI we build; the useful extras are not open source |
| Authentik | MIT, except `authentik/enterprise/` under the authentik EE License (production use needs a subscription) | Yes | Yes | Yes, in the MIT tree (a SCIM provider); the enterprise tree adds Google Workspace and Entra sync, among others | Good UX and more built in than Keycloak; the enterprise split means the suite must keep to the MIT tree and check each feature |
| Dex | Apache-2.0 | Yes, as a federating shim | Passed through from connectors | No | No user store; better as a connector than the IdP |
| ZITADEL | AGPL-3.0-only (v3 onward, verify the date), with a commercial option; protos Apache-2.0, login app and clients MIT | Yes | Yes | Inbound SCIM for users only | Rejected for Loams's own code (D11). As a separate, unmodified service it would be allowed (§4.2), but any patch carries AGPL obligations, so it is the second choice, not the default |

**Historical rationale; superseded by §38 D447 (Authentik), 2026-10-02.** **Recommendation (D-SC-3): Keycloak.** It is Apache-2.0 with no enterprise split, it is the IdP §19 already documents as the broker for SAML, its admin REST API lets `commons-control` create clients and groups declaratively (realm export and `keycloak-config-cli`), and its group mapper puts `groups` into every app's ID token. Its weakness, SCIM (Preview only), matters little here because the suite provisions apps through their own APIs (§7.3). Loams's §19 login (`openidconnect` 4) treats Keycloak as one more OIDC provider; the unified auth plan (Q30) must accept Keycloak-issued tokens for Loams's API, or exchange them (RFC 8693, §19 §5.2) for Loams tokens.

## 5. Single sign-on per app

| App | OIDC | In the free/open edition? | Group claims → roles | Gap and workaround |
|---|---|---|---|---|
| Forgejo | Native OpenID Connect login source; Forgejo is also an OAuth2/OIDC provider itself | Yes | Yes, natively: a groups claim plus a JSON map from group to organization teams (`{"developer":{"Org":["Team1"]}}`, patterns such as `group-{org}-{team}`), with optional removal (`--group-team-map`, `--group-team-map-removal`) | None |
| Zulip | Native generic OIDC (`SOCIAL_AUTH_OIDC_ENABLED_IDPS`), SAML, LDAP, JWT | Yes: SAML, OIDC, SCIM (beta, users only) and group sync are all in free self-hosted Zulip | Yes: `SOCIAL_AUTH_SYNC_ATTRS_DICT` syncs groups on login (SAML since 11.0, OIDC since 13.0) | None. SCIM group sync is documented as unsupported while the pricing table lists it (verify); the suite does not need it |
| GlitchTip | Native generic OIDC (django-allauth) | Yes | No | None for login; roles by API (§7.3) |
| Plane | Community Edition: GitHub, GitLab, Google and **Gitea** OAuth. Generic OIDC and SAML are paid ("One" plan and up) | **Gap** for generic OIDC | No | **Workaround without a patch: chained SSO through Forgejo.** Plane's Gitea provider takes a `GITEA_HOST` and calls `/login/oauth/access_token` and `/api/v1/user`, which Forgejo serves; Forgejo in turn signs in with Keycloak. The user clicks "Sign in with Gitea" once and never types a password in Plane (verify end to end, SC1 Task 4). Longer term: upstream generic OIDC to the Community Edition (Q-SC-1) |
| PostHog | FOSS: GitHub and GitLab login only. SAML, OIDC and Google live in `ee/`, and PostHog documents "Google SSO is not available for open source deployments"; JIT provisioning and enforced SSO are paid | **Gap** | No | No protocol trick: PostHog's GitLab login expects GitLab's API, which neither Forgejo nor Keycloak serves. Options: (1) forward-auth at the edge plus PostHog accounts created by API with random passwords; (2) upstream a generic OIDC backend outside `ee/` (MIT), which PostHog may decline since OIDC is an `ee/` feature today; (3) a small GitLab-userinfo shim over Keycloak (rejected: fragile, and it impersonates another product's API). SC1 takes (1) (Q-SC-1) |
| Loams | §19 P7 generic OIDC | Yes (§19: "OSS gets SSO") | §19 P4 maps OIDC groups to teams | Needs M2 (§19) |
| OpenFGA, RustFS consoles | Operator-only; not exposed to users | — | — | — |

**Where the gaps leave SC1.** Forgejo, Zulip, GlitchTip and Loams get real OIDC SSO against Keycloak; Plane gets it through Forgejo with no patch; **PostHog is the one real gap**. Until PostHog accepts generic OIDC outside `ee/`, it sits behind forward-auth at the edge (so one sign-in still gates it) with accounts that `commons-control` creates, and shows its own login once per session. The suite documents this as a known gap instead of hiding it.

## 6. Infrastructure per app, and what Loams replaces (D-SC-7)

### 6.1 The table

| App | Needs | Loams replaces now (after v1.0) | Later | Stays |
|---|---|---|---|---|
| Plane | Postgres 15, Valkey 7.2, RabbitMQ 3.13, S3 (MinIO by default); services web, admin, space, api, worker, beat-worker, live, proxy | **S3 → RustFS** | Search over Plane issues in the portal (§8.1); Plane's own search box stays on Postgres | Postgres, Redis, RabbitMQ |
| Forgejo | Postgres, MySQL or SQLite; optional Redis/Valkey for cache, queue and sessions; S3 for LFS, attachments, packages, artifacts; issue indexer (bleve, db, Elasticsearch or Meilisearch); code indexer (bleve, Elasticsearch or zoekt); databases: PostgreSQL 14+, MySQL 8.4+, MariaDB 10.6+, SQLite | **S3 → RustFS**; **issue indexer → Loams's Elasticsearch API** (the queries Forgejo sends, §6.2, fall in M1.5's Phase A) | **Code indexer → Loams's ES API** once ES Phase B adds highlighting and `terms` aggregations; **database → TiDB** over MySQL if the Forgejo test suite passes on it (Q-SC-5) | Postgres until then |
| Zulip | Postgres, RabbitMQ, memcached, Redis, nginx, Tornado, Smokescreen (outbound proxy); S3 optional for uploads; full-text search is Postgres FTS (optionally PGroonga) | **S3 → RustFS** (uploads) | Cross-app search over messages in the portal (§8.1) | Postgres (and its FTS inside Zulip: Zulip has no pluggable search backend) |
| PostHog (hobby) | Postgres 15, ClickHouse, Zookeeper, Redpanda (as Kafka), Redis 7 and Valkey, SeaweedFS (object storage; it replaced MinIO), Temporal, Elasticsearch (Temporal visibility, verify), Caddy, the Rust capture and feature-flag services, ingestion workers, livestream and more; about 4 vCPU and 16 GB RAM, under about 300k events a month | **Object storage → RustFS** in place of SeaweedFS (verify the S3 settings PostHog exposes) | A copy of events into a Loams stream for the portal and the assistant (§8.4); PostHog's own queries stay on ClickHouse | Postgres, ClickHouse, Kafka/Redpanda, Redis, Temporal and its Elasticsearch (Temporal visibility needs aggregations and scroll, beyond Loams's ES Phase A) |
| GlitchTip | Postgres 14+; Valkey/Redis optional (falls back to Postgres); S3, Azure or GCS for uploads | **S3 → RustFS** | Its events mirrored into Loams for search (§8.4); GlitchTip can itself receive OTLP logs, but the suite sends logs to Loams | Postgres |
| OpenFGA | Postgres, MySQL or SQLite | — | **TiDB** over MySQL (Q-SC-3) | Postgres until then |
| Keycloak | Postgres (or MySQL, MariaDB) | — | TiDB is not a supported Keycloak database (verify); not planned | Postgres |
| All | Logs, traces | **Logs → Loams over OTLP** (D73) via the OpenTelemetry Collector | **Traces** when Q43 lands: Forgejo (`[opentelemetry]`), Plane (`OTLP_ENDPOINT`; it otherwise exports to `telemetry.plane.so`, which the suite turns off) and PostHog's Node and Rust services emit OTLP; Zulip emits none | — |

### 6.2 Forgejo on Loams's Elasticsearch API

Forgejo talks to Elasticsearch through `olivere/elastic/v7` with sniffing off and a health check (`GET /_cluster/health`). Its **issue indexer** creates a versioned index, writes by `Index`, `Delete` and `_bulk`, and searches with `bool` of `term`, `terms`, `range` and `multi_match`, sorted, paged by `from/size`. Every construct is in M1.5's Phase A. Its **code indexer** additionally asks for highlighting with the fast-vector highlighter and a `terms` aggregation on `language`; both are ES Phase B (§06 §7), so the code indexer stays on bleve until then. SC1 runs Forgejo's own indexer integration tests against Loams as the gate (Task 6), and every refusal it finds becomes an engine issue. Meilisearch is not needed: Loams covers the issue indexer, and the code indexer waits for Phase B.

### 6.3 Forgejo, OpenFGA and GlitchTip on TiDB

Forgejo supports MySQL (`SupportedDatabaseTypes = {"mysql", "postgres"}`), but TiDB is not in its documented list (MariaDB 10.6+, MySQL 8.4+, PostgreSQL 14+, SQLite), and TiDB diverges from MySQL in DDL and some SQL. Running Forgejo's integration suite (`make test-mysql`) against R1's TiDB playground is cheap and would make Forgejo's metadata a TiKV tenant (D123). GlitchTip requires Postgres 14+, so it stays on Postgres; the same holds for Plane, Zulip and PostHog, whose Postgres use (JSONB, array fields, `pg_trgm`, PGroonga) has no MySQL path.

### 6.4 What Loams does not replace, said plainly

- **Postgres** under Plane, Zulip, GlitchTip, OpenPanel, Keycloak and OpenFGA (PostHog was dropped, §13b). These are OLTP workloads with multi-statement transactions and Postgres-specific SQL. Loams's planned Postgres wire access is read-only analytics with autocommit, and D2 keeps OLTP out of the retrieval engine. The suite runs one Postgres 17 with a database per app. **Amended 2026-09-29 (owner, D230, [§28](28-loams-postgres.md) §4):** in Kubernetes that Postgres is a **CloudNativePG** `Cluster` (`commons-pg`, Postgres 17.11, a primary and a replica) with backups and PITR to RustFS through the Barman Cloud plugin; the dev compose keeps a plain `postgres:17.11` container. Zulip's PGroonga needs an extended image or its own Cluster (Q111). OpenPanel's compose pins Postgres 14; it runs on this Postgres 17 once its migrations pass there. **Matomo's MariaDB is separate** (§13b): its own MariaDB service, not part of the CloudNativePG cluster.
- **ClickHouse and Kafka under PostHog.** PostHog's ingestion pipeline and HogQL are built on them. Loams's streams could carry the event firehose in principle, but PostHog's code expects Kafka topics and ClickHouse tables; replacing them means forking PostHog, which is out of scope. The hobby compose uses Redpanda (BSL-1.1 and the Redpanda Community License); the suite uses Apache Kafka instead if PostHog runs on it (to verify), or accepts Redpanda as an unmodified, separately licensed service and says so in `LICENSES.md` (Q-SC-6).
- **Redis/Valkey, RabbitMQ, memcached** as caches and queues. Loams is not a cache.

What this leaves is still a real showcase: every app's files live on RustFS, the forge's search runs on Loams, every log line lands in Loams, and the cross-app features of §8 run only on Loams.

## 7. The OpenFGA model and its enforcement (D-SC-4, D-SC-5)

### 7.1 Principles

- **One store.** The suite adds its types to the store Loams uses (D67 already shares it with Lakekeeper). Loams's own types (`org`, `namespace`, `collection`, §18 §7) are reused, not duplicated.
- **The suite `project` is the unit.** A team works on a project; the project fans out to one resource per app. Granting a team `developer` on a project grants write on its repositories, membership of its Plane project and Zulip channels, and read on its analytics and errors.
- **Effective relations are fenced to the org**, as §18 §7's tenant fence requires.
- **Apps do not call OpenFGA.** OpenFGA holds the intent; each app's own ACL is a projection kept in step by the reconciler. Loams and the portal *do* call OpenFGA at request time.

### 7.2 The model (schema 1.1, proposed)

```
model
  schema 1.1

type user

type group                      # Keycloak groups, mirrored by the group sync
  relations
    define member: [user, group#member]

type org                        # one per install (§19 P3); Loams's type, shared
  relations
    define owner: [user]
    define admin: [user, group#member] or owner
    define member: [user, group#member] or admin

type project                    # the suite unit; one-to-one with a §19 project
  relations
    define org: [org]
    define admin: [user, group#member] or admin from org
    define maintainer: [user, group#member] or admin
    define developer: [user, group#member] or maintainer
    define viewer: [user, group#member, org#member] or developer
    define can_admin: admin and member from org          # tenant fence
    define can_write: developer and member from org
    define can_read: viewer and member from org

type namespace                  # Loams: environment = namespace (§19 P2)
  relations
    define project: [project]
    define reader: [user, group#member] or can_read from project
    define writer: [user, group#member] or can_write from project
    define admin: [user, group#member] or can_admin from project

type collection                 # Loams
  relations
    define namespace: [namespace]
    define reader: [user, group#member] or reader from namespace
    define writer: [user, group#member] or writer from namespace

type plane_project
  relations
    define project: [project]
    define admin: can_admin from project
    define member: can_write from project
    define guest: [user] or can_read from project

type plane_issue                # only for search-time checks (§8.1)
  relations
    define plane_project: [plane_project]
    define reader: guest from plane_project or member from plane_project or admin from plane_project

type forgejo_org
  relations
    define org: [org]
    define owner: admin from org

type forgejo_repo
  relations
    define project: [project]
    define forgejo_org: [forgejo_org]
    define public: [user:*]
    define admin: can_admin from project or owner from forgejo_org
    define write: can_write from project or admin
    define read: public or can_read from project or write

type zulip_channel
  relations
    define project: [project]
    define org_visible: [org#member]                   # a public channel
    define administrator: can_admin from project
    define subscriber: [user, group#member] or can_write from project or administrator
    define reader: org_visible or subscriber

type analytics_project          # PostHog project
  relations
    define project: [project]
    define admin: can_admin from project
    define member: can_read from project or admin

type errors_project             # GlitchTip project
  relations
    define project: [project]
    define admin: can_admin from project
    define member: can_write from project or admin
    define viewer: can_read from project or member
```

Object ids carry the app and its native id: `forgejo_repo:{org}/{forgejo_repo_id}`, `zulip_channel:{org}/{stream_id}`, `plane_project:{org}/{workspace_slug}/{uuid}`. The model is tested with `fga model test` fixtures in SC1 (Task 3), including the fence (a user outside the org never gets `can_read` through `org#member` of another org).

### 7.3 Enforcement per app

| App | What the app's model offers | Projection |
|---|---|---|
| Loams | `Authorizer` with `OpenFga` (§18 §7) | **Native**: Loams checks OpenFGA itself; no projection |
| Portal (search, feed, assistant) | Ours | **Native**: `ListObjects` or `BatchCheck` per request; results filtered by an ACL field (§8.1) |
| Forgejo | Organizations, teams with unit permissions, repository collaborators; REST API (`/orgs/{org}/teams`, `PUT/DELETE /teams/{id}/members/{user}`, `PUT /repos/{o}/{r}/collaborators/{u}`); native OIDC group → team mapping with removal; webhooks for repositories and organizations, none for team membership | `project.developer` → a team per project with write on its repositories; `maintainer` → admin team; `viewer` → read team. Teams are created and filled by API; the OIDC group-team map is a coarse fallback when the sync lags |
| Plane | Workspaces, projects with roles (admin, member, guest); public API `GET/POST workspaces/{slug}/projects/{id}/members/` and `PATCH/DELETE .../members/{pk}/`; workspace membership is read-only in the public API (changes go through invites); webhooks for projects, issues, modules, cycles and comments, none for membership | Project members and roles by the project-members API; workspace membership by invite, accepted on the user's first (chained-SSO) login; Q-SC-7 decides whether to upstream a workspace-members write API |
| Zulip | Channels (public, private, web-public), user groups, channel permission groups; REST API: subscriptions with `principals`, `/user_groups` and their members, roles by `PATCH /users/{id}`; outgoing-webhook bots and the event queue (`/register`, `/events`); group sync on OIDC login | `subscriber` → channel subscriptions; `administrator` → channel admin permission group; Keycloak groups → Zulip user groups |
| PostHog | Organizations, projects/environments, members with levels; roles and per-project access control are `ee/` (`ee/api/rbac`), so not in the FOSS build; REST API with personal API keys (endpoint shapes to verify) | Organization membership and level by API; per-project restriction only if available outside `ee/` (Q-SC-2); otherwise PostHog is org-wide and the model marks `analytics_project` as coarse |
| GlitchTip | Organizations, teams, projects, member roles; a Sentry-compatible `/api/0/` REST API (endpoint details to verify); alert webhooks only | Teams per project and members by API |

**The mechanism.**

1. **Writes.** A grant or revoke in the portal or the Loams console writes a tuple change to the control plane's outbox in the same transaction as the change (§18 §7). On Loams Live the outbox is a table; the Live commit journal (D119) makes it tail-able.
2. **Sagas.** `commons-control` turns each outbox row into a durable workflow on Loams Durable (§21 §6.3): *create a project* runs "create the Loams environments → the Forgejo team and repos → the Plane project → the Zulip channel → the PostHog project → the GlitchTip project → write the tuples", each step idempotent (the step's promise id is its idempotency key, D146) and each with a compensation (archive what was created) if a later step fails permanently. Grants and revokes are small sagas of per-app membership calls.
3. **Group sync.** Keycloak group changes (admin events) become `group#member` tuples, so a person added to the `loams-engine` group in Keycloak gets everything that group is granted.
4. **Reconcile.** A durable schedule (§21 §6.1) compares each app's actual members with `ListUsers` results from OpenFGA and fixes drift, reporting each fix as an audit event (D100). Direct changes made in an app's own UI are reverted or, by setting, adopted as tuples (Q-SC-8).
5. **Offboarding.** Disabling a user in Keycloak ends sessions at the IdP; the offboarding saga then removes the user from every app and revokes the user's personal tokens where each app's API allows (Forgejo, Zulip, GlitchTip).

## 8. Loams integrations: the showcase (D-SC-8)

### 8.1 Unified search

- **Indexer.** `commons-control` receives each app's webhooks (Forgejo, Plane, GlitchTip; Zulip outgoing webhooks or the event queue API; PostHog annotations and actions) and writes them as records to Loams streams, one per app; links materialize them into collections (`commons.issues`, `commons.code`, `commons.messages`, `commons.errors`, `commons.docs`) with text fields, keyword filters and embeddings. Backfill uses each app's list API through D1's bulk import.
- **Every document carries its FGA object** (`acl_object: "forgejo_repo:acme/42"`). At query time the portal asks OpenFGA for the caller's readable objects per type (`ListObjects`, cached for seconds as §18 §7 does) and adds them as a `terms` filter; for large result sets it post-filters with `BatchCheck`.
- **Hybrid retrieval**: BM25 plus vectors with RRF (§05), one query over issues, code, messages and errors; the Qdrant and Elasticsearch APIs let third-party tools reuse the same collections.

### 8.2 Activity feed on Loams Live

The webhook stream feeds a Live table `activity` through a small mutation; the portal subscribes with the Live sync API (§20 §7), filtered by the caller's projects, so a push, a new issue, a chat mention and a new error appear live in one feed. It also shows the D129 bridge in the other direction when it lands (Live → collections).

### 8.3 Provisioning sagas on Loams Durable

§7.3's sagas are the showcase for §21: a visible, resumable operation per project creation (`/v1/operations/{id}`, D146), with progress, compensation after a failed step and a human approval gate (§21 §6.5) for destructive steps such as deleting a project's repositories.

### 8.4 Observability over OTLP

Every service runs with an OpenTelemetry Collector sidecar or DaemonSet that ships container logs (and the OTLP traces that Forgejo, Plane and PostHog's services emit, once Q43 gives Loams a traces endpoint) to Loams's OTLP endpoint (D73) into per-app streams, linked into a `commons.logs` collection for search. Traces follow Q43. GlitchTip and PostHog events can be mirrored into Loams streams so the assistant can relate an error to the deploy and the chat thread about it.

### 8.5 The assistant over MCP

Loams's MCP server (M1.6) exposes the commons collections; a user connects Claude Code or another MCP client with §19's user-delegation flow (OAuth 2.1 + PKCE), so the assistant sees exactly what the user may see: "what broke after yesterday's deploy?" searches errors, commits, issues and chat together. Agent actions that change apps (open an issue, post a message) go through `commons-control`'s sagas with approval gates, never with app admin tokens held by the agent.

### 8.6 Analytics

Flight SQL over the commons streams and collections gives cross-app metrics (lead time from issue to merge to first error) without an ETL job; M4's Iceberg tables add history.

### 8.7 The dogfooding loop

| Loams feature | Exercised by |
|---|---|
| Collections, hybrid search, ES API | Portal search; Forgejo's issue indexer |
| Streams, links | Webhook and OTLP ingest |
| Loams Live, sync API | Activity feed; control-plane state (D125) |
| Durable (Resonate) | Provisioning, grants, offboarding, reconcile, backfill |
| OIDC login, OpenFGA authorizer, agent tokens | Every request to Loams and the portal; the MCP assistant |
| TiDB (if Q-SC-5 passes) | Forgejo's database |
| RustFS | Every app's files and Loams's bucket |
| Erasure (D68) | A user's right-to-erasure request, run across the Loams collections that index the user's messages and issues |

## 9. Deployment (D-SC-9)

- **Compose (development and small teams).** One `compose.yaml` with profiles per app (`--profile plane`, `--profile posthog`), because PostHog alone needs several GB of RAM (estimate: the whole suite needs 16 GB or more; SC1 Task 0 measures it). Keycloak's realm, OpenFGA's model and the RustFS buckets are bootstrapped by a one-shot `commons-init` container. TLS by Caddy with a local CA.
- **Helm (Kubernetes).** An umbrella chart `loams-commons` depending on the upstream charts where they exist (Forgejo's official chart; Plane's chart (verify); Zulip's `docker-zulip` Helm chart (verify); Keycloak through the Keycloak operator or a maintained chart; OpenFGA's official chart; GlitchTip's chart (verify)) and on Loams's chart (M2) and RustFS's chart. PostHog supports only its hobby Docker Compose deploy ("officially unsupported"); the chart runs that topology as an optional component or leaves PostHog as an external dependency. Managed Postgres is an input, not a sub-chart, in production values.
- **Pins.** Every image by digest, with its license and source link in `LICENSES.md`; Renovate proposes bumps; each bump re-runs the SSO and provisioning end-to-end tests.
- **Loams cloud.** A hosted demo instance is a later decision; nothing is deployed by this document.

## 10. Security

- Keycloak is the only place with passwords; apps use OIDC where possible, and their local-account fallback (Plane, PostHog until the gaps close) uses random passwords held only by `commons-control` and never shown.
- `commons-control` holds admin tokens for every app; it is the most sensitive component. Tokens live in the Kubernetes secret store, are scoped as narrowly as each app allows, and every call it makes is an audit event (D100).
- OpenFGA and Keycloak admin endpoints stay on the internal network.
- The webhook receiver verifies each app's signature (Forgejo, Plane, GlitchTip HMAC; Zulip token).

## 11. Risks

| # | Risk | Mitigation |
|---|---|---|
| 1 | **SSO gaps**: generic OIDC is paid in Plane and `ee/` in PostHog, so "one login" is partial | Plane: chained SSO through Forgejo's Gitea-compatible OAuth2 provider (no patch). PostHog: forward-auth plus provisioned accounts; upstream generic OIDC. Documented, not hidden |
| 2 | **Editions move.** Features shift between free and paid tiers, and licenses change (Forgejo moved to GPL at v9; Sentry moved from BSL to FSL) | Task 0 re-verifies; pins by digest; `LICENSES.md` checked by a CI job that reads each repo's license at the pinned tag |
| 3 | **Projection drift and partial failure**: an app changes permissions outside OpenFGA, or a saga step fails between apps | Durable sagas with compensation; scheduled reconcile with audit; idempotent steps |
| 4 | **App API gaps** for membership (Plane, PostHog per-project access) | Q-SC-7, Q-SC-2; coarse mapping where fine-grained is impossible |
| 5 | **PostHog's weight and support status**: the hobby compose is "officially unsupported", needs about 4 vCPU and 16 GB, and runs ClickHouse, Redpanda, Temporal and Elasticsearch | An optional profile, off by default; an external PostHog allowed; measured in Task 0; if it proves too heavy, the showcase keeps PostHog out and Q-SC-2 records why |
| 6 | **Loams's ES Phase A may refuse something Forgejo sends** (index settings, `_cluster/health` details, versioned index names) | Forgejo's indexer tests as the gate; engine issues, not suite patches |
| 7 | **Copyleft handling mistakes** if a patch ships without its source offer | The fork rule of §4.2; a release checklist item |
| 8 | **Dogfooding risk**: if the suite is down, Loams's own team is blocked | Backups (Postgres PITR, RustFS versioning, BR for TiKV, D131); keep GitHub as a mirror of the code; staged adoption in SC1 (chat and issues last) |
| 9 | **Scope creep** into building app features | D-SC-10: engine gaps become engine issues; suite code is glue only |
| 10 | **Keycloak token acceptance by Loams** depends on the unified auth plan | Q30 must list external-IdP tokens (or exchange) as a requirement; SC1 depends on it |

## 12. Open questions

| # | Question | Needed by |
|---|---|---|
| Q-SC-1 | Plane: is chained SSO through Forgejo enough, or do we upstream generic OIDC to Plane's Community Edition? PostHog: forward-auth plus provisioned accounts, or an upstream OIDC backend outside `ee/` (PostHog may decline)? | SC1 Task 0 |
| Q-SC-2 | PostHog FOSS: is there a maintained `posthog-foss` image, or must the suite build one; what does the FOSS build lose (SAML, per-project access, others) | SC1 Task 0 |
| Q-SC-3 | OpenFGA on TiDB through its MySQL datastore: does its migration and test suite pass | SC1 Task 7 |
| Q-SC-4 | `commons-control`'s language: TypeScript or Python (the Resonate SDKs both work against Loams Durable, §21 §4), or Rust with the Resonate Rust SDK | SC1 Task 0 |
| Q-SC-5 | Forgejo on TiDB: does `make test-mysql` pass against R1's TiDB playground, and with which settings | SC1 Task 7 |
| Q-SC-6 | PostHog's hobby deploy with Apache Kafka instead of Redpanda (BSL-1.1), or Redpanda documented as a separately licensed service | SC1 Task 0 |
| Q-SC-7 | Plane's public API for project membership: enough for the projection, or upstream work | SC1 Task 4 |
| Q-SC-8 | Drift policy: revert changes made in an app's own UI, or adopt them as tuples | SC1 Task 5 |
| Q-SC-9 | Postgres write compatibility (D-SC-12): which store backs it (a Postgres front end over TiKV transactions, or a layer in front of TiDB), and which Postgres features are in scope first. Proposed answer: Neon behind Loams's pg listener ([§23](23-neon-and-wesql.md) §5, D149), pending Q45. Updated 2026-09-29 (owner, D230): plain Postgres 17 on CloudNativePG for the suite ([§28](28-loams-postgres.md) §4); the TiKV front end stays long-term | The engine's Postgres-write design doc |
| Q-SC-10 | The PostHog fork (D-SC-13): the staged scope, keeping HogQL's semantics on DataFusion, and how far the fork may drift from upstream before rebases stop being practical | The PostHog-on-Loams design doc |

## 13. Contradictions with earlier decisions

| Earlier | Here | Resolution |
|---|---|---|
| D11: no AGPL/BSL/SSPL/ELv2 dependencies | Plane (AGPL), Forgejo (GPL), possibly Redpanda (BSL) run in the suite | D11 is about Loams's linked dependencies; the suite runs these as separate, unmodified services, as D60 does for Alternator. The suite repository is not the engine |
| D2: OLTP out of scope | The suite needs Postgres | The suite runs Postgres beside Loams; Loams does not claim to replace it (§6.4) |
| §19 P7: Loams is an OIDC client; SAML/SCIM brokered by an IdP | Keycloak is the suite's IdP | Consistent: §19 already names Keycloak as the broker |

## 13a. Owner direction of 2026-09-28 (amends §2.2, §6.4, D-SC-7)

The owner reviewed the first draft and gave three directions: "it is a separate project, add postgres write compatibility, fork posthog and replace clickhouse with our iceberg backed". They change the plan as follows. Each change is a **proposal** (D-SC-11 … D-SC-13) until the owner confirms the wording.

1. **A separate project (D-SC-11).** Loams Commons is its own product, with its own repository (`dina-kar/loam-commons`, already D-SC-1), its own roadmap and its own releases. It is not an engine milestone. SC1 stays the first plan of that project, and the engine roadmap lists it only as a consumer with dependencies.
2. **Postgres write compatibility in Loams (D-SC-12).** Loams gains a Postgres wire surface that accepts writes, and not only the analytical, autocommit reads now planned. This is an **engine** decision that the suite motivates, and it goes through its own design doc and plan. It amends D2 ("OLTP out of scope") and this document's §2.2 and §6.4. This document records the direction and the questions the engine design must answer before any app's database moves:
   - **Which store backs transactional Postgres writes.** The retrieval engine's log and collections are not an OLTP store (D1, D2). The natural candidate is **TiKV** (§20): Loams Live already runs ACID transactions on it, and TiDB proves that SQL over TiKV works. The candidates are a Postgres dialect front end over TiKV transactions written by Loams, or a Postgres-compatible layer placed in front of TiDB. Q-SC-9.
   - **How much Postgres the suite's apps need.** Plane, Zulip, GlitchTip, Keycloak and OpenFGA use multi-statement transactions, `SELECT … FOR UPDATE`, sequences, JSONB, arrays, `pg_trgm` and, for Zulip, PGroonga. An app moves off Postgres only when its own test suite passes against Loams's Postgres wire, one app at a time. Order of attempt: OpenFGA and GlitchTip (small schemas), then Keycloak and Plane. Zulip goes last, because it depends on PGroonga and Postgres full-text search.
   - **Until then, Postgres stays** in the suite. §6.4 stays true for SC1, and each app's move becomes a later SC plan.
   - **Proposed amendment (2026-09-29, [§23](23-neon-and-wesql.md) §5, D149, pending the owner's confirmation Q45):** serve this Postgres OLTP with **Neon** (Apache-2.0, storage on the same RustFS bucket store), run unmodified beside Loams and reached through Loams's pg listener by database name, and narrow the Postgres front end over TiKV to Loams Live's reactive API. The spike ran OpenFGA's and GlitchTip's migrations on Neon unchanged. §23 §4.1 records that Neon's public repository has been nearly dormant since August 2025, and D151 keeps plain Postgres as the exit.
   - **Decided 2026-09-29 (owner, D230, [§28](28-loams-postgres.md)):** the amendment above is **not** taken. The suite's apps run on **plain Postgres 17 on CloudNativePG** with PITR to RustFS. Neon becomes **Loams Postgres**, a fork owned by Loams (D231) for serverless and branching use (agent workspaces, D155), not the suite's database. The Postgres front end over TiKV stays the long-term D-SC-12 path.
3. **Fork PostHog onto Loams's Iceberg analytics (D-SC-13).**
   - **The fork.** A public fork, `dina-kar/posthog-loams`, built from `posthog-foss` (MIT; `ee/` is never taken). Its event store and query layer move from ClickHouse and Kafka to Loams: ingestion goes through Loams's native stream API (D72) into Iceberg tables (M4, §08), and HogQL is compiled to DataFusion SQL instead of ClickHouse SQL.
   - **License.** A fork of the MIT tree is allowed. It stays MIT, keeps PostHog's notices and must not use PostHog's trademarks as its product name.
   - **Size.** HogQL's printer targets ClickHouse (functions, `arrayJoin`, `argMax`, sampling, materialized columns), and the ingestion pipeline, persons and cohorts, funnels, retention and session replay all assume ClickHouse tables. This is the largest item in the suite. It needs its own design doc and plan, with a staged scope. Stage 1 covers events and trends, stage 2 persons, funnels and retention, and replay comes later or never. It also needs **M4 (Iceberg analytics)** in the engine.
   - **What it proves.** It is the strongest showcase in the suite: a well-known analytics product running on Loams's Iceberg tables and query engine, with no ClickHouse.
   - **Until it exists,** SC1 runs upstream PostHog FOSS as an optional profile (§5, §6), or leaves PostHog out.

## 13b. Owner direction of 2026-09-28, second (supersedes the PostHog, Sentry and TiDB parts)

The owner then said: "drop posthog and sentry, implement for [GlitchTip], openpanel, [Matomo] which is maria + php … drop tidb, use only tikv with our tikv rust client". Where this section and earlier ones disagree, this section wins (D-SC-14 … D-SC-16).

1. **The app set (D-SC-14).** The apps are **Plane, Forgejo, Zulip, GlitchTip, OpenPanel and Matomo**. PostHog and Sentry are dropped entirely: no optional profile and no fork. D-SC-13 (the PostHog fork) is withdrawn, and its intent moves to OpenPanel (item 2).

   | App | License (verified 2026-09-28) | Stack | SSO | Loams's role |
   |---|---|---|---|---|
   | **OpenPanel** (`Openpanel-dev/openpanel`) | **AGPL-3.0** | Caddy, Postgres 14, Redis 7.2, **ClickHouse 25.10**, the api, dashboard and worker images (`self-hosting/docker-compose.template.yml`) | GitHub and Google login through `arctic` (`packages/auth/src/oauth.ts`); **no generic OIDC**: a gap | The ClickHouse replacement target (item 2) |
   | **Matomo** (`matomo-org/matomo`) | **GPL-3.0** | PHP with **MariaDB/MySQL** only (`adapter = PDO\MYSQL` in `config/global.ini.php`); Redis optional for queued tracking | Only through the third-party **LoginOIDC** plugin (`dominik-th/matomo-plugin-LoginOIDC`, GPL-3.0, last pushed 2024-06: maintenance risk) | Logs over OTLP; RustFS for archives and backups. **MariaDB stays**: TiDB is dropped, and Loams's Postgres write surface (D-SC-12) does not speak MySQL |
   | **GlitchTip** | MIT | Postgres, optional Valkey | Generic OIDC (free) | As in §4.3 and §6 |

   **SSO after the change:** Forgejo, Zulip, GlitchTip and Loams work natively. Plane signs in through Forgejo. Matomo uses the LoginOIDC plugin, a separately licensed GPL add-on installed unmodified; the fallback is forward-auth. **OpenPanel is the new gap.** Either we upstream a generic OIDC provider (it already uses `arctic`, which ships generic OAuth2 clients, so the patch is small, and AGPL-3.0 obliges us to publish it if we run it patched), or it sits behind forward-auth.
2. **OpenPanel on Loams instead of ClickHouse (D-SC-15, replaces D-SC-13).** The owner's "replace ClickHouse with our Iceberg backend" now targets OpenPanel.
   - **Why OpenPanel is the better target.** Its ClickHouse use is one service's event store, far smaller than PostHog's HogQL, ingestion pipeline and persons model. It is AGPL-3.0, so a fork (`dina-kar/openpanel-loams`) must offer its source to every network user; as an open-source showcase it does that anyway.
   - **The work.** Events go through Loams's stream API (D72) into Iceberg tables (M4). OpenPanel's ClickHouse queries (in its `packages/db`, to inventory) are rewritten as DataFusion SQL over Flight SQL.
   - **Ownership and gates.** The work gets its own design doc and needs M4.
3. **No TiDB anywhere (D-SC-16).** TiDB leaves the suite and the SC plans: no Forgejo or OpenFGA on TiDB (Q-SC-3 and Q-SC-5 are withdrawn, and SC1 Task 7 is dropped). Loams's transactional storage is **TiKV only, through Loams's own `tikv-client` fork**. The upstream client's README says it is not production-ready, which is why Loams carries the fork. This also points D-SC-12's Postgres write surface at a Postgres front end over TiKV transactions, not a layer in front of TiDB (it narrows Q-SC-9). What this means for the engine's existing TiDB decisions (D123 TiDB SQL, D139 Resonate on TiDB) belongs to the Resonate + TiKV + Dapr work, which the owner has made the main workstream. It is not decided in this document.

   **Proposed amendment (2026-09-29, [§23](23-neon-and-wesql.md) §5, D149, pending Q45; superseded the same day by D230, [§28](28-loams-postgres.md): the apps use plain Postgres on CloudNativePG, and D-SC-16 gains that exception instead):** keep "no TiDB", but let Neon, not TiKV, back Postgres-wire OLTP for the apps; TiKV stays the store for Loams Live, the metastore and durable state. WeSQL (GPL-2.0-only, MySQL on object storage) was also tested for Forgejo and Matomo and is not proposed for either yet: Forgejo's migrations fail on WeSQL's missing foreign-key support (D156).

4. **Resonate + TiKV + Dapr is validated in the forks first (owner, 2026-09-28).** In the owner's words: "build this in our forks, do not touch loams for now, once we validated the Dapr + tikv + resonate we apply for ours, embed for dev and test with separate binaries using the tidb v2 kubernetes operator". The work happens in `ostrium-labs/resonate` (branch `loam/tikv-dapr`, from `loam/0.10.1`) and, if fixes are needed, in `ostrium-labs/client-rust` (branch `loam`). No upstream PR is changed, and Loams is not changed until the validation passes. There are two deployment modes. **Dev is embedded:** one process holds Resonate with an in-process TiKV store or a local backend. **Test and validation use separate binaries:** Resonate server pods, Dapr sidecars and the Rust gRPC stream server, with TiKV deployed by TiDB Operator v2 (`core.pingcap.com/v1alpha1`) on a local kind or k3d cluster. The validation bar is Resonate's own `cargo xtask differential` and porcupine across seeds, run both embedded and on the cluster. Loams Commons adopts the result only after that; until then, SC1's durable sagas assume D1's engine as merged.

## 14. Roadmap

SC1 follows the release (v1.0) and needs: the unified auth plan (Q30) implemented for OIDC tokens and the `OpenFga` authorizer (D66, D67, M2.x), §19's OIDC login (M2), M2's OTLP logs ingest (D73) and stream API (D72), D2's durable tenancy (D142) for sagas outside the `default` namespace, and R1's Live sync API for the feed. Items that arrive later extend SC1 without blocking it: ES Phase B (Forgejo's code indexer), Q43 (traces), D129 (the Live bridge).

## 15. Sources

Read on 2026-09-27 unless noted.

- Loams: §02 §7.1 (OTLP), §06 §7 (ES Phase A and B), §18 §6–§7, §19 (PR #39), §20 §7, §10, §21 §6; D2, D11, D60, D61, D66, D67, D72, D73, D100, D111, D116, D123, D125, D126, D129, D142, D146; Q30, Q43.
- Forgejo: `LICENSE` at `codeberg.org/forgejo/forgejo` (branch `forgejo`): GPL-3.0; `go.mod` (`olivere/elastic/v7` v7.0.32, `meilisearch-go`, `go-sql-driver/mysql`, `pgx/v5`, `minio-go/v7`, `go-redis/v9`); `modules/indexer/internal/elasticsearch/{indexer.go,util.go}` (sniff off, `ClusterHealth`); `modules/indexer/issues/elasticsearch/elasticsearch.go` (bool, term(s), range, multi_match); `modules/indexer/code/elasticsearch/elasticsearch.go` (fvh highlighting, `terms` aggregation on `language`); `modules/setting/database.go` (`SupportedDatabaseTypes`). Forgejo Runner: `code.forgejo.org/forgejo/runner` `LICENSE`: GPL-3.0.
- OpenFGA: `openfga/openfga` license Apache-2.0; latest release v1.21.0 (2026-09-20); `CHANGELOG.md` (ListUsers).
- Plane: `makeplane/plane` license (AGPL-3.0) and `LICENSE.txt`; `apps/api/plane/authentication/provider/oauth/{github,gitlab,google,gitea}.py` (the Gitea provider's `GITEA_HOST`, `/login/oauth/access_token`, `/api/v1/user`); `packages/constants/src/subscription.ts` (OIDC + SAML in paid plans); `docker-compose.yml`; `apps/api/plane/api/urls/{member.py,invite.py}`; `apps/api/plane/db/models/webhook.py`; `plane/utils/otlp_endpoints.py`.
- Forgejo docs: https://forgejo.org/2024-08-gpl/, https://forgejo.org/2024-10-release-v9-0/, https://forgejo.org/docs/latest/admin/advanced/oidc-group-mappings/, https://forgejo.org/docs/latest/admin/installation/database-preparation/, https://forgejo.org/docs/latest/admin/config-cheat-sheet/, https://forgejo.org/docs/latest/user/oauth2-provider/.
- Zulip: `zulip/zulip` license (Apache-2.0); https://zulip.readthedocs.io/en/latest/production/authentication-methods.html, https://zulip.readthedocs.io/en/latest/production/scim.html, https://zulip.readthedocs.io/en/latest/production/requirements.html; `templates/corporate/comparison_table_integrated.html`; https://zulip.com/plans/?showSelfHosted.
- PostHog: `LICENSE` and `ee/LICENSE` in `PostHog/posthog`; `PostHog/posthog-foss` (README, pushed 2026-09-27); https://posthog.com/docs/self-host; https://posthog.com/docs/settings/sso; `ee/settings.py`, `ee/api/scim`, `ee/api/rbac`; `docker-compose.hobby.yml`.
- Sentry: `getsentry/sentry` and `getsentry/self-hosted` `LICENSE.md` (FSL-1.1-Apache-2.0); self-hosted `docker-compose.yml`; https://develop.sentry.dev/self-hosted/sso/.
- GlitchTip: https://gitlab.com/glitchtip/glitchtip-backend/-/raw/master/LICENSE (MIT); https://glitchtip.com/documentation/install.
- IdPs: `keycloak/keycloak` (Apache-2.0), https://www.keycloak.org/2026/07/keycloak-2670-released (SCIM Preview); `ory/kratos`, `ory/hydra` (Apache-2.0), https://www.ory.com/docs/kratos/manage-identities/scim, https://github.com/ory/kratos/issues/2528; `goauthentik/authentik` (`authentik/enterprise/` and its license); `dexidp/dex` (Apache-2.0); https://github.com/zitadel/zitadel/blob/main/LICENSING.md, https://zitadel.com/docs/guides/manage/user/scim2.
