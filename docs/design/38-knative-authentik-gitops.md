# 38 — Knative, Authentik and GitOps for Self-Hosted Loams

Status: **Approved** (owner defaults, 2026-10-02: "do suggested for all") · 2026-10-02. Source: three owner rulings. On 2026-10-01 the owner wrote "loams multitenant also fully opensource using knative and for gitops clever cloud, for auth Authentik so no paid plan" and "all run on cloudflare using byoc". On 2026-10-02 the owner narrowed them: "keep loams cloud and loams-cloud private; may add Knative in OSS but no metering; I want adoption and also to raise money from VCs; move Cloudflare, OpenRTB etc. commercial to private repos." This document applies the 2026-10-02 ruling. It does **not** reopen the open-core boundary (D220, [open-core.md](../open-core.md)), which stays as approved on 2026-09-29.

It turns the ruling into decisions **D440–D459** and open questions **Q440–Q453** (Q454–Q459 are reserved and unused). The owner approved them on 2026-10-02 ("do suggested for all"). Plans: [MT1](../plans/2026-10-02-mt1-authentik-identity.md) (Authentik identity), [MT2](../plans/2026-10-02-mt2-knative.md) (Knative Serving and Eventing) and [MT3](../plans/2026-10-02-mt3-gitops-clever.md) (GitOps).

**Amends** [§19](19-console-identity-and-agents.md) (the IdP in front of Loams), [§22](22-showcase-suite.md) (D-SC-3: the suite's IdP), [§24](24-cpu-time-runtime.md) (a Knative runner for the `http-port` contract), [§25](25-clever-cloud-stack.md) (what "GitOps from Clever Cloud" means; new sync waves) and [§27](27-usage-hooks.md) (Knative pods under the hooks contract, with no meter). **Narrows** D111 (the unified auth plan) and D221 (SAML is brokered through Authentik, not Keycloak). The private side of the same ruling (the hosted Loams Cloud on Cloudflare, the protocol gateway, the Cloudflare target, the metering ledger) is designed in `loam-platform`. This repository does not depend on it.

> **Amended 2026-10-02 (later the same day) by [§41](41-multitenant-byoc-control-plane.md) (D540, D541, D548, owner open-core ruling).** The owner ruled that the **Loams Multitenant BYOC Control Plane with GitOps** (multi-tenancy, Knative, Argo CD GitOps with Clever Cloud's operator fork, Authentik, BYOC) is **open source**, and that metering and the commercial APIs are private because **integrity** is the security principle. This document's non-goals that left "multi-org control planes and BYOC management" to `loam-platform` (§2.2, D440's first sentence, §3.3's last clause, MT3's last table, §7) are **superseded**: those are goals of §41. "No metering in this repository" (D444) **stands**, with integrity as the reason. §27's references to a CloudEvents form and a usage reporter are superseded by D548.

Markers: **(verify)** means not checked against a primary source; the task that depends on it checks it first. **(estimate)** means computed, not measured. Every version, licence and status claim with a date was read on 2026-10-02 from the source named in §13.

**Numbering.** D440–D459 and Q440–Q459 are this document's reserved ranges; Q440–Q453 are used.

---

## 1. Summary

| # | Decision | Status |
|---|---|---|
| D440 | **The open-core boundary** (D220, reconfirmed 2026-10-02; **revised later that day by D540, §41**). Open source: self-hosting, and the Multitenant BYOC Control Plane with GitOps (multi-tenancy, BYOC management, Knative, Authentik, the GitOps layout). Private (`loam-platform`): metering, billing, commercial APIs and hosted-only operations (D548, D550). **No metering in this repository**: only §27's generic hooks stay. The protocol gateway (§34) and the Cloudflare target (the former §35) move to `loam-platform` (private), and §34 becomes a stub | Approved (owner defaults, 2026-10-02) · owner ruling 2026-10-02 |
| D441 | **Knative Serving is an optional compute layer** for the `http-port` contract (§24 D181, tier T2) in self-hosted clusters. `KnativeRunner` implements the `Runner` trait (D375): one Knative `Service` per function, one `Revision` per version, scale to zero, gVisor through `runtimeClassName`. The node supervisor stays the only runner for `fetch` (T0 workerd) and Wasm (T1), whose many-tenants-per-process model Knative cannot express | Approved (owner defaults, 2026-10-02) |
| D442 | **Knative's ingress is Kourier, inside the cluster, behind Loams's edge.** Envoy stays the edge (D184); the gateway routes a function's traffic to Kourier's internal service with the tenant already checked. Knative's own domains are cluster-local (`svc.cluster.local`), so no function is reachable except through the gateway | Approved (owner defaults, 2026-10-02) |
| D443 | **Tenancy on Knative: one Kubernetes namespace per Loams namespace** (`loams-ns-<namespace>`), with a default-deny `NetworkPolicy`, a `ResourceQuota` and a `LimitRange` set by `loams-operator` from the namespace's limits. The quota is **enforced** here (§41 §9); who **sets** it per plan is `loam-platform`'s concern, through the open limits API (D220, D546). Every pod carries §27 §3.2's labels | Approved (owner defaults, 2026-10-02) |
| D444 | **No meter on Knative.** `KnativeRunner` returns `usage: None` and writes no host reports, like the supervisor. Usage is visible only through the open hooks: §27 §3.2's pod labels on the pod cgroup, Knative's queue-proxy and activator Prometheus metrics, and the edge's access logs. Aggregating, rating and billing them is `loam-platform` (D190, D202) | Approved (owner defaults, 2026-10-02) · owner ruling 2026-10-02 |
| D445 | **Knative Eventing is an adapter, not Loams's event log.** Loams streams (D270) and the Event Fabric (§32 D331) stay the logs. Loams ships `loams-knative-source`, which reads a stream consumer group and delivers binary-mode CloudEvents to any Knative sink, and documents `POST /v1/namespaces/{ns}/streams/{stream}/events` as a Knative sink URI. The broker is the in-memory channel for development; the production broker is an open question (Q443) | Approved (owner defaults, 2026-10-02) |
| D446 | **Knative is installed by the Knative Operator** (Apache-2.0), as `KnativeServing` and `KnativeEventing` resources pinned to 1.23 with the features Loams needs turned on (`kubernetes.podspec-runtimeclassname`, `kubernetes.podspec-securitycontext`). It is off by default in the umbrella chart (`knative.enabled: false`) | Approved (owner defaults, 2026-10-02) |
| D447 | **Authentik, open-source edition, is the default identity provider** of the Kubernetes distribution and the showcase suite. It replaces Keycloak in D-SC-3 (§22) and in D221's "SAML brokered through Keycloak". It runs as an **unmodified, separate service** (Python, Postgres only since 2025.10), version 2026.8.3, and **only code outside `authentik/enterprise/` is used**: no licence key is ever installed | Approved (owner defaults, 2026-10-02) |
| D448 | **The usable feature list is fixed** (§4.2): OAuth2/OIDC provider (authorization code with PKCE, client credentials with JWT federation, device code, refresh, **token exchange (RFC 8693)**, dynamic client registration (RFC 7591)); SAML, SCIM (static token), LDAP, RADIUS (PAP), Proxy and RAC providers; OAuth, SAML, LDAP, Kerberos and SCIM sources; flows and stages with TOTP, WebAuthn and passkeys; RBAC; brands; blueprints; outposts. Everything listed as Enterprise on 2026-10-02 is excluded, multi-tenancy (`tenants`) included | Approved (owner defaults, 2026-10-02) |
| D449 | **Loams's gateway stays the resource server and the authority for Loams tokens** (§19 §5). Authentik authenticates **people**: the console and the CLI sign in through it (OIDC authorization code with PKCE; device code for the CLI), and the gateway exchanges the Authentik token for a Loams access token. **Agents stay Loams principals** (§19 P5), with federation, delegation and vending on Loams's token endpoint; Authentik's "agent accounts" are Enterprise and are not used. Biscuit stays inside the runtime (D188), OpenFGA stays the authority (D66, D67), and Authentik groups reach OpenFGA as `team` tuples at sign-in | Approved (owner defaults, 2026-10-02) |
| D450 | **The single binary keeps its built-in sign-in** (§19 P7: setup token, password with argon2id, TOTP, generic OIDC). Authentik is the default only where Kubernetes is (the Helm chart, the operator, the showcase). A self-hoster may point Loams at any OIDC provider; Authentik is the documented and tested one | Approved (owner defaults, 2026-10-02) |
| D451 | **D111 is narrowed, not dropped.** User identity, MFA, SSO and SAML brokering are Authentik's. What remains of the unified auth plan is Loams's side: token verification on every listener, API keys, agent tokens, TLS and leaving loopback. MT1 is that plan for the native API, the console and MCP; the other listeners follow it in their own plans | Approved (owner defaults, 2026-10-02) |
| D452 | **Authentik is configured by blueprints in Git and installed from its upstream chart.** Loams's blueprints (the Loams application and providers, the `loams-*` groups, the enrolment and MFA flows) are Apache-2.0 YAML under `deploy/authentik/blueprints/`. The upstream Helm chart (`goauthentik/helm`) is **GPL-3.0**, so it is referenced by an Argo CD `Application`, never copied or vendored into this repository | Approved (owner defaults, 2026-10-02) |
| D453 | **"GitOps from Clever Cloud" means Clever's open-source operator and infrastructure tooling, not a Clever deployer.** Clever Cloud publishes no GitOps reconciler or deployer (checked 2026-10-02; its git-push deployer is closed). So: `loams-operator` is the fork of `clever-kubernetes-operator` (MIT, D185); `terraform-provider-clevercloud` and `karpenter-provider-clever-cloud` (Apache-2.0) are used unmodified on Clever Kubernetes Engine; `clever-tools` is the CLI reference. **Argo CD is not replaced** (D186 stands) | Approved (owner defaults, 2026-10-02) |
| D454 | **The GitOps layout stays consumable by Flux.** Argo CD is the default and the tested path; a Flux layout (`deploy/gitops/flux/`) with the same order through `dependsOn` is documented for the single-node k3s profile, where Argo CD's footprint matters (Q-RT-11) | Approved (owner defaults, 2026-10-02) |
| D455 | **New sync waves** (§6.3): CloudNativePG and the Knative Operator join wave −1; Authentik's Postgres joins wave 1; Authentik and its blueprints join wave 2; `KnativeServing` and `KnativeEventing` join wave 3; `loams-knative-source` joins wave 5. Lua health checks are added for `authentik` blueprint instances, `KnativeServing` and `KnativeEventing` | Approved (owner defaults, 2026-10-02) |
| D456 | **The reference small cluster is k3s** (Apache-2.0, v1.37.1+k3s1) or k3d, started with `--disable traefik` so that Envoy and Kourier own ingress. MT2 and MT3 test on k3d in CI and on a single k3s node by hand | Approved (owner defaults, 2026-10-02) |
| D457 | **Licences** (§9): Knative (Serving, Eventing, Operator, Kourier, `func`) Apache-2.0; Authentik MIT outside `authentik/enterprise/`, unmodified; the Authentik chart GPL-3.0, referenced only; CloudNativePG Apache-2.0; Argo CD and Flux Apache-2.0; the operator fork MIT with its notice kept. No enterprise or copyleft code is linked or vendored | Approved (owner defaults, 2026-10-02) |
| D458 | **A CI guard keeps Authentik free of the Enterprise edition**: the chart values never set a licence, a blueprint lint rejects models from `authentik_enterprise*` and `authentik_providers_*` apps that live in the enterprise tree, and the MT1 e2e job asserts that `GET /api/v3/enterprise/license/summary/` reports no licence | Approved (owner defaults, 2026-10-02) |
| D459 | **Track MT** (§10): MT1 Authentik identity, MT2 Knative, MT3 GitOps. MT1 and MT2 are independent; MT3 wires both into the waves. Each is small, stacked PRs; none adds a dependency to `loams`'s default build | Approved (owner defaults, 2026-10-02) |

## 2. Goals and non-goals

### 2.1 Goals

1. **Adoption.** A self-hoster gets a serverless layer (scale to zero), a real identity provider (SSO, MFA, SAML) and a GitOps install from open source, with no paid plan anywhere in the stack.
2. **No new runtime in the engine.** Knative and Authentik are separate services. The engine binary links neither, and the single-binary install works without them.
3. **The boundary holds.** Nothing here meters, rates or invoices, or sets quotas per plan (D220). *Amended 2026-10-02 (D540): provisioning orgs across clusters is open and designed in §41; metering, rating and invoicing stay private (D548).*
4. **Every feature is checked against its licence.** Authentik is open core, so each feature Loams uses is checked against the directory it lives in.

### 2.2 Non-goals

- **Metering and billing.** These stay in `loam-platform` (D190, D440, D548). *Superseded 2026-10-02 (D540): multi-org control planes and BYOC management, listed here as non-goals, are now goals of [§41](41-multitenant-byoc-control-plane.md) and open source.*
- **Running Loams on Cloudflare.** The Cloudflare target is a commercial component in `loam-platform` (D440). The `Fs` trait's portable part stays here, in §36.
- **Replacing the node supervisor** with Knative for T0 and T1 (D441).
- **Replacing Loams streams with Knative Eventing** (D445).
- **Authentik multi-tenancy.** It is Enterprise and alpha (§4.2). A self-hosted Loams has one org (§19 P3), so one Authentik tenant is enough.

## 3. Knative (D441–D446)

### 3.1 Where Knative fits in §24

| §24 contract | Tier | Runner | Why |
|---|---|---|---|
| `fetch` | T0 workerd, one process per tenant, isolates per version | `SupervisorRunner` | Knative scales pods; T0 packs many function versions into one per-tenant process. A pod per function would waste the isolate model |
| Wasm component | T1 wasmtime, many tenants per host | `SupervisorRunner` | Same: one host process serves many components |
| `http-port` | T2, a container under gVisor | **`KnativeRunner`** (when `knative.enabled`), else the supervisor's T2 | A server listening on `$PORT` is exactly a Knative `Service`; Knative adds scale to zero, concurrency-based autoscaling, revisions and traffic splitting |
| `static` | object store and edge | none | Unchanged |

So Knative replaces the hand-written T2 scheduler that F2 would otherwise need. F2 keeps the gVisor node setup and the `gvisor` `RuntimeClass`; the pod lifecycle becomes Knative's.

### 3.2 `KnativeRunner`

`crates/loams-runner-knative` (MT2) implements D375's trait over `kube` 3:

| `Runner` method | Knative operation |
|---|---|
| `deploy(cx, artifact)` | Server-side apply of a `serving.knative.dev/v1` `Service` named `fn-<function_id>` in `loams-ns-<namespace>`, with the image by digest, `runtimeClassName: gvisor`, `containerConcurrency` from the manifest, `autoscaling.knative.dev/min-scale: "0"`, `max-scale` from the namespace's limits, and the §27 §3.2 labels. Idempotent by digest: an unchanged digest creates no new revision |
| `invoke(cx, dep, req)` | HTTP/2 over TLS to Kourier's internal service (Knative's internal TLS on; no cleartext fallback, because the Biscuit is a bearer token; MT2 Ruling 4) with `Host: fn-<id>.loams-ns-<ns>.svc.cluster.local`, the request's `traceparent`, and the runtime's Biscuit (D182) in `x-loams-sandbox-token`. `usage: None` (D444) |
| `undeploy(cx, dep)` | Delete the `Service`; Knative garbage-collects its revisions |
| `health()` | `KnativeServing` `Ready`, Kourier reachable |
| `capabilities()` | contracts `[http-port]`, `suspend: false`, cold start: Knative's (seconds, image-dependent) **(estimate)** |

A function's traffic still enters through the gateway: the gateway authenticates, authorizes (OpenFGA), applies the namespace's rate limits, and only then calls `Runner::invoke`. Knative's activator buffers requests while a revision scales from zero.

**Scale to zero.** Knative's defaults (`enable-scale-to-zero: "true"`, `scale-to-zero-grace-period: "30s"`, read in `config/core/configmaps/autoscaler.yaml` at 1.23) apply. A namespace can set `scale-to-zero-pod-retention-period` through its manifest, within the operator's bound.

**gVisor.** Knative rejects `runtimeClassName` unless `kubernetes.podspec-runtimeclassname` is `enabled` in `config-features` (it is `disabled` by default at 1.23); D446 turns it on. `kubernetes.podspec-securitycontext` is turned on so the operator can set `runAsNonRoot`, `readOnlyRootFilesystem` and a seccomp profile. Kata is not used (D174).

### 3.3 Tenancy on Knative (D443)

- **One Kubernetes namespace per Loams namespace**, `loams-ns-<namespace>`, created by `loams-operator` when the namespace is created and deleted with it. A Loams namespace is one environment (§19 P2), so a project's `staging` and `production` never share a Kubernetes namespace.
- **Network.** A default-deny `NetworkPolicy`; ingress only from Kourier's pods; egress only to the gateway and `loams-dapr` (functions reach Loams through them, §24 §5), DNS, and whatever the namespace's egress policy allows.
- **Quotas.** A `ResourceQuota` (CPU, memory, pod count, `count/services.serving.knative.dev`) and a `LimitRange` per namespace, computed by the operator from the namespace's limits record (D65). Knative's `max-scale` is capped by the same record.
- **Identity.** Each function's pod runs with a service account that has no Kubernetes API rights (`automountServiceAccountToken: false`). Its Loams credential is the per-invocation Biscuit, never a long-lived secret (§19 P6).

### 3.4 Usage without a meter (D444)

Knative pods are T2 sandboxes in §27's terms, so §27 §3.2 already covers them: the pod cgroup with the labels `loams.dev/org`, `loams.dev/namespace`, `loams.dev/function` and `loams.dev/tier` (`t2`). Two more open hooks come for free and are documented, not built:

- **queue-proxy and activator metrics** (Prometheus): request counts, latencies and concurrency per revision, which carry the revision's labels;
- **the edge's access logs**, with `x-loams-tenant` set by the gateway (§27 §3.4).

`KnativeRunner` writes no billing-grade host report; Q-UH-3 (the final cgroup reading for pods) applies unchanged. Whoever wants usage per tenant, a self-hoster's dashboard or `loam-platform`, reads these hooks.

### 3.5 Knative Eventing (D445)

Knative Eventing delivers CloudEvents from sources to sinks through `Broker`s and `Trigger`s. Loams already has two logs: streams for databases, retrieval and trigger-rate events (D270), and the Event Fabric (Iggy and Fluss) for high-rate ingestion (§32 D331, D332). Eventing is neither; it is a **delivery layer** for teams that already use it.

| Direction | How | Delivery |
|---|---|---|
| Knative → Loams | Any Knative `Trigger` or `Subscription` can use `http://loams-gateway.<ns>/v1/namespaces/{ns}/streams/{stream}/events` as its `subscriber.uri`. Knative delivers binary-mode CloudEvents over HTTP, which is §02 §7.4's HTTP binary mode | D270's dedupe on `source` + `id` makes Knative's retries safe |
| Loams → Knative | `loams-knative-source`, a small Rust service (one `Deployment` per `LoamsSource` resource, a `SinkBinding`-style `sink` reference): it reads a stream through a consumer group and POSTs each record as a binary-mode CloudEvent to the sink, committing the offset after a 2xx | At least once; the sink dedupes on `id` |
| Fabric → Knative | `loams-knative-source` with an Iggy topic instead of a stream (after FL1) | Same |

The broker for development is `InMemoryChannel` (not durable). The production broker is open (Q443): Knative's Kafka broker over Loams's Kafka gateway (M5, D74), a Loams broker class over streams, or no broker (sources deliver to Services directly, which covers most uses). Loams's own triggers (functions subscribed to streams, §24) do not need Eventing at all.

Event types Loams defines use the owner's prefix `io.loams.dev.<domain>.<name>.v1` (ruling of 2026-10-01).

### 3.6 What Knative does not change

- **§26 jobs.** Queues, leases and schedules stay `loams-jobs` and Resonate (D205, D210). A Knative function can enqueue through `loams.jobs.v1`; Knative does not run job workers.
- **§21 durable execution.** Long waits still go through Resonate (D173). A scaled-to-zero Knative revision is not a suspended durable function.
- **The edge.** Envoy stays the edge (D184), and Kourier is internal (D442).

## 4. Authentik (D447–D452, D458)

### 4.1 The licence split, checked

The repository's `LICENSE` (read at `version/2026.8.3`, released 2026-09-17):

> "All content that resides under the "authentik/enterprise/" directory of this repository, if that directory exists, is licensed under the license defined in "authentik/enterprise/LICENSE". … Content outside of the above mentioned directories or restrictions above is available under the "MIT" license."

`authentik/enterprise/LICENSE` is the **authentik Enterprise Edition (EE) license**:

> "This software … may only be used in production, if you … have agreed to, and are in compliance with, the Authentik Subscription Terms of Service … and otherwise have a valid authentik Enterprise Edition subscription for the correct number of user seats. … you may copy and modify the Software for development and testing purposes, without requiring a subscription."

So the rule for Loams is: **use only what is outside `authentik/enterprise/`, and never install a licence key** (D447, D458). The enterprise code ships in the same image, but it is inactive without a licence.

### 4.2 What Loams may use

Checked against the source tree at 2026.8.3 (`authentik/providers`, `authentik/sources`, `authentik/stages`, `authentik/enterprise/*`) and the "Enterprise features" page (docs.goauthentik.io/enterprise/enterprise-features, read 2026-10-02):

| Feature | Where it lives | Loams |
|---|---|---|
| OAuth2/OIDC provider: authorization code + PKCE, refresh, device code, client credentials | `providers/oauth2` (MIT) | **Use**: console, CLI and showcase apps |
| Client credentials with **JWT federation** (a JWT from a configured provider authenticates a service account) | `providers/oauth2` (MIT) | Use for CI and automation that signs in to Authentik |
| **Token exchange (RFC 8693)**, impersonation and delegation with an `act` claim; since 2026.8.0 | `providers/oauth2/views/token.py`, `common/oauth/constants.py` (`GRANT_TYPE_TOKEN_EXCHANGE`) (MIT) | Available; Loams's own token endpoint still does agent exchange (D449) |
| Dynamic client registration (RFC 7591) | `providers/oauth2/views/dcr.py` (MIT) | Not needed: MCP clients register with Loams's authorization server (§19 §5.2) |
| SAML provider | `providers/saml` (MIT) | Use: SAML apps in the showcase |
| SCIM provider (outbound), static token auth | `providers/scim` (MIT) | Showcase apps only (§22 §7.3). Loams's own SCIM endpoint is `loam-platform` (D221, Q440) |
| SCIM provider with **OAuth authentication** | `enterprise/providers/scim/auth_oauth2.py` | **Excluded** |
| LDAP, Proxy and RAC providers, with outposts | `providers/ldap`, `providers/proxy`, `providers/rac` (MIT) | LDAP and Proxy for apps without OIDC; RAC not used |
| RADIUS provider (PAP) | `providers/radius` (MIT) | Not used |
| RADIUS **EAP-TLS** | `enterprise/providers/radius` | **Excluded** |
| Sources: OAuth (GitHub, Google, any OIDC), SAML, LDAP, Kerberos, SCIM, Plex, Telegram | `sources/*` (MIT) | Use OAuth, SAML and LDAP sources so a company's own IdP federates in |
| The **source stage** (an external IdP embedded in a flow) | `enterprise/stages/source` | **Excluded** |
| Flows and stages: identification, password, TOTP, WebAuthn and passkeys, Duo, email, SMS, static recovery codes, consent, invitation, captcha, prompt | `stages/*` (MIT) | Use: enrolment, MFA and recovery flows |
| Client-certificate (mTLS) stage, account lockdown, password-history policy | `enterprise/stages/mtls`, `enterprise/stages/account_lockdown`, `enterprise/policies/unique_password` | **Excluded** |
| RBAC (roles, object permissions) | `rbac` (MIT) | Use for Authentik's own admin |
| Brands (per-domain branding; called tenants before 2024.2) | `brands` (MIT) | Use: one brand per install |
| **Multi-tenancy** (`tenants`, a Postgres schema per tenant) | the `tenants` app, gated: "an Enterprise feature … in alpha", one licence per additional tenant (docs.goauthentik.io/sys-mgmt/tenancy) | **Excluded**; not needed (one org per install, §19 P3) |
| Blueprints (declarative YAML) | `blueprints` (MIT) | **Use**: all of Loams's Authentik configuration (D452) |
| Google Workspace and Microsoft Entra ID sync, Shared Signals Framework, WS-Federation, agent accounts | `enterprise/providers/*`, `enterprise/agents` | **Excluded** |
| Enhanced audit (before and after values), event maps, CSV exports, reports, object lifecycle management, privileged access management, endpoint and device connectors | `enterprise/audit`, `enterprise/reports`, `enterprise/lifecycle`, `enterprise/endpoints` | **Excluded**. Loams's own audit events (D221) cover Loams's actions |

The "Enterprise features" page also lists "External OAuth and SAML sources embed an external identity provider in a flow". That is the source stage only: plain OAuth and SAML sources (federated login) are in `sources/` under MIT.

**Runtime.** Since 2025.10 Authentik needs only Postgres: cache, sessions, WebSockets and the embedded outpost moved off Redis, with about 50% more Postgres connections (2025.10 release notes; goauthentik.io blog "We removed Redis", 2025-11-13). Its Postgres is a CloudNativePG cluster (D230), like the other showcase databases.

### 4.3 How Loams uses it (D449)

```
 person ── browser / loams CLI ─► Authentik (OIDC: code + PKCE, or device code; MFA, passkeys, SAML/LDAP/OAuth sources)
                                     │ ID token + access token (groups claim)
                                     ▼
                           loams-gateway  POST /api/v1/oauth/token
                           (RFC 8693: subject_token = Authentik token, trusted issuer)
                                     │ Loams access token (JWT, Ed25519, §19 §5.3)
                                     ▼
                  native API · console API · MCP · gateways  ──►  Authorizer (OpenFGA, D66)
 agent ── federation / delegation / vending on Loams's token endpoint (§19 §5.2, unchanged)
 sandbox ── Biscuit minted and attenuated by the supervisor (D188, unchanged)
```

- **People sign in through Authentik.** The console uses the authorization-code flow with PKCE; `loams login` uses the device-code flow (`providers/oauth2/views/device_*`). Loams's gateway is an OIDC relying party (`openidconnect` 4, §19 §6) and keeps the session.
- **Loams issues Loams tokens.** The gateway exchanges the Authentik token for a Loams access token on its own token endpoint (RFC 8693, §19 §5.2 flow 1, with Authentik registered as a trusted issuer). Every listener verifies only Loams tokens, so the verification code does not change with the IdP.
- **Agents are not Authentik users.** An agent is a Loams principal with a trust policy (§19 §5.1). Authentik's agent accounts are Enterprise and are not used.
- **Groups reach OpenFGA at sign-in.** The `groups` claim maps to Loams teams (§19 P4, "OIDC groups can map to teams"); the gateway writes `team#member` tuples through the outbox (D66) when a person signs in or refreshes. Removal takes effect at the next refresh (1 hour) or on session revocation. Continuous provisioning through SCIM stays a `loam-platform` feature (D221) unless the owner moves it (Q440).
- **SAML, LDAP and social logins** are Authentik sources. Loams itself only ever speaks OIDC (D221 as amended).

### 4.4 Deployment (D452)

- **Chart.** The upstream chart `goauthentik/helm` (`authentik-2026.8.3`) is GPL-3.0. Loams's umbrella chart does not include it; Loams's GitOps layout points an Argo CD `Application` at `https://charts.goauthentik.io` with Loams's values file. Nothing from the chart is copied into this repository.
- **Configuration as code.** `deploy/authentik/blueprints/loams.yaml` creates the `loams` application, its OAuth2 provider (the console's and the CLI's clients, redirect URIs, the `groups` scope mapping), the `loams-admins` and `loams-developers` groups, and the enrolment and MFA flows. It is mounted into the worker through the chart's `blueprints.configMaps`, so Argo CD owns it.
- **Secrets.** The bootstrap token, the secret key and the client secrets come through the same secret path as everything else (D189); none is committed.

### 4.5 What this replaces

| Before | After |
|---|---|
| §22 D-SC-3: Keycloak is the suite's IdP; §22 §4.4 preferred it for having "no enterprise split" | Authentik (D447), restricted to the MIT tree with a CI guard (D458). Keycloak's advantage (no split) is answered by the guard; Authentik's are blueprints, built-in passkeys and outposts, and a smaller footprint after Redis was removed |
| §19 P7 and §19 §6: "SAML … brokered by an IdP (Keycloak, Dex, Authentik)"; "Keycloak brokers SAML to OIDC" | Authentik is the documented and tested broker; any OIDC IdP still works (D450) |
| D221: "plain OIDC SSO, with SAML brokered through Keycloak by self-hosters" | "… brokered through Authentik" (D447). The rest of D221 is unchanged |
| D111: one unified auth plan after M1 | Narrowed (D451): MT1 is the identity half; listeners leave loopback as their plans adopt MT1's verifier |
| `loam-cloud`'s Clerk (hosted console) | **Not changed here.** Hosted identity is `loam-platform`'s and `loam-cloud`'s decision (D440) |

## 5. Reconciling with §19, D67 and D188

| Mechanism | Answers | Changed? |
|---|---|---|
| Authentik (OIDC) | Who is this person; which groups | New, in front of the gateway (D449) |
| Loams access tokens (JWT, §19 §5.3) | This bearer is principal X with scopes S until T | No; issued after the exchange |
| Agent federation, delegation, vending (§19 §5.2) | How an agent gets a token without a secret | No |
| Biscuit (D188) | What this sandbox may do, attenuated offline | No |
| OpenFGA (D66, D67) | Who may do what | No; groups become `team` tuples at sign-in |

## 6. GitOps (D453–D456)

### 6.1 What Clever Cloud publishes

Checked on 2026-10-02 (GitHub organisation `CleverCloud`, and §25 §2's inventory of 2026-09-29):

| Project | Licence · release | Role in Loams's GitOps |
|---|---|---|
| `clever-kubernetes-operator` | MIT · v0.8.0 (2026-06-09), pushed 2026-09-04 | **Forked** as `loams-operator` (D185): reconciles `Loams`, `Function`, `RuntimePool`, `ObjectStore`, and now the per-namespace Knative tenancy (D443) |
| `terraform-provider-clevercloud` | Apache-2.0 · v2.3.0 (2026-09-28) | Infra under GitOps on Clever Kubernetes Engine (CKE) |
| `karpenter-provider-clever-cloud` | Apache-2.0 · v0.13.0 (2026-10-01) | Node autoscaling on CKE |
| `clever-tools` | Apache-2.0 · 5.0.2 (2026-09-16) | Reference for the `loams` CLI's deploy UX (§30) |
| A GitOps reconciler, deployer or git-push build system | **none published** | Clever's own deployer is closed (§25 §1) |

So Clever supplies the operator skeleton and the infrastructure layer for CKE, and **a GitOps engine is still needed**. Argo CD (Apache-2.0, v3.5.3, 2026-09-14) stays that engine (D186); Flux (Apache-2.0, v2.9.6, 2026-10-01) is the documented alternative for the smallest profile (D454).

### 6.2 What the owner's ruling changes in §25

Nothing in D185–D188 is reversed. The forked operator gains work (namespaces, `NetworkPolicy`, `ResourceQuota` and Knative `Service`s for D443), the waves gain components (D455), and `infra/clever-cke/` stays the place for Clever's Terraform provider.

### 6.3 Sync waves (amends §25 §6.3)

| Wave | §25 today | Added by D455 |
|---|---|---|
| −2 | CRDs | Knative Operator CRDs, CloudNativePG CRDs |
| −1 | operators, gVisor node setup, Karpenter on CKE | **Knative Operator**, **CloudNativePG operator** |
| 0 | RustFS | — |
| 1 | PD and TiKV | **`authentik-db`** (a CNPG `Cluster`) |
| 2 | Resonate | **Authentik** (upstream chart, Loams values, blueprints) |
| 3 | `loams-dapr`, gateway, Envoy | **`KnativeServing`** (with Kourier) and **`KnativeEventing`**, when `knative.enabled` |
| 4 | `Loams` | — |
| 5 | runtime tiers | **`loams-knative-source`** instances, when enabled |

Health: Argo CD Lua checks for `KnativeServing` and `KnativeEventing` (`status.conditions[type=Ready]`), the CNPG `Cluster` (`status.phase == "Cluster in healthy state"`) and Authentik (the worker's blueprint status through the server's `/-/health/ready/`). Flux's layout (D454) expresses the same order with `dependsOn`.

### 6.4 The small profile (D456)

k3s v1.37.1+k3s1 (2026-09-30) or k3d, started with `--disable traefik`. On it: Argo CD (or Flux), RustFS single-node, one PD and one TiKV, Authentik with one CNPG instance, and Knative Serving with Kourier. Footprints are measured in MT3 Task 6 and recorded beside Q-RT-11; until then they are unknown.

## 7. Moves out of this repository (D440)

| What | Was | Now |
|---|---|---|
| The protocol gateway, OpenRTB and Google adapters, the canonical `loams.rtb.v1`, partner negotiation, the ad-tech conformance suite (D366–D371, D373, D377, D379) and plans GW1–GW4 | §34, merged in #177 | `loam-platform` (private). §34 is a stub that keeps the vendor-neutral decisions (the standards charter, the narrow waist, the CloudEvents profile, the high-rate path, state rules, the `Runner` trait, usage hooks from runners) |
| The Cloudflare target (`CloudflareRunner`, Workers, Durable Objects, R2, Containers placement, the startup credits plan) and plan CF1 | the former §35 (PR #179) | `loam-platform` (private). §36 (Loams Git) and GT1–GT3 stay; the `Fs` trait and `NativeFs` move into §36 |
| The usage CloudEvents form and its Arrow mapping (RN1 Task 6), and any ledger | RN1, §34 §12 | `loam-platform`. RN1 keeps the `Runner` trait, `RunnerHost`, the process and Lambda runners. *Amended 2026-10-02 (D548): §27's host-report emitter (RN1 Tasks 1 and 2) moved to `loam-platform` too; RN1 gains the open `InvocationObserver`.* |

## 8. Contradictions with earlier decisions, and how they are resolved

| # | Earlier | This document | Resolution |
|---|---|---|---|
| 1 | **D-SC-3** (§22): Keycloak is the suite's IdP | Authentik | Superseded by D447 |
| 2 | **D221**: SAML brokered through Keycloak | Through Authentik | Amended by D447; the rest of D221 stands |
| 3 | **D111**: one unified auth plan after M1 | MT1 plus each listener's plan | Narrowed by D451 |
| 4 | **§19 P7, §6** name Keycloak as the SAML broker | Authentik | Amended (D447, D450); built-in sign-in kept |
| 5 | **§19 §3**: Cloud identity "Clerk or Keycloak" | — | Not changed here (hosted is `loam-platform`, D440) |
| 6 | **D379** (§34): the adapters, negotiation and ad-tech conformance are Apache-2.0 here | `loam-platform` | Superseded by D440 (owner ruling 2026-10-02) |
| 7 | **The former §35 §2** (PR #179): `CloudflareRunner` and the Worker crates are Apache-2.0 here | `loam-platform` (private) | Superseded by D440 before merge |
| 8 | **D376 item 4, RN1 Task 6**: usage as CloudEvents, built here | Moved | The record spec (§27 §3.6) stays; the event form is `loam-platform`'s (D444) |
| 9 | **D186**: Argo CD | "GitOps from Clever Cloud" | No conflict: Clever has no GitOps engine (D453) |
| 10 | **§24 §11 F2**: Loams schedules T2 sandboxes | Knative schedules them when enabled | Refined by D441; F2's gVisor setup stays |
| 11 | **§22 §4.4** rejected Authentik's split as a risk | A CI guard (D458) | Risk accepted with a guard |

## 9. Licences (D457)

| Component | Licence (file, release) | How used |
|---|---|---|
| Knative Serving, Eventing | Apache-2.0 · knative-v1.23.0 (2026-07-28/29) | Unmodified services |
| Knative Kafka broker | Apache-2.0 · knative-v1.23.1 (2026-08-25) | Only if Q443 picks it |
| Kourier | Apache-2.0 · knative-v1.23.0 | Unmodified |
| Knative `func` | Apache-2.0 · knative-v1.23.3 (2026-09-03) | Optional developer tool |
| Knative Operator | Apache-2.0 · knative-v1.23.1 (2026-09-01) | Unmodified (D446) |
| Authentik | MIT outside `authentik/enterprise/` (EE licence inside) · 2026.8.3 (2026-09-17) | Unmodified image, no licence key (D458) |
| Authentik Helm chart | **GPL-3.0** (`goauthentik/helm`) · authentik-2026.8.3 | Referenced by URL; not vendored (D452) |
| CloudNativePG | Apache-2.0 · v1.30.1 (2026-09-23) | Unmodified operator (D230) |
| Argo CD · Flux | Apache-2.0 · v3.5.3 · v2.9.6 | Unmodified |
| k3s | Apache-2.0 · v1.37.1+k3s1 (2026-09-30) | Reference cluster (D456) |
| `clever-kubernetes-operator` | MIT · v0.8.0 | Forked, notice kept (D185) |

Knative graduated in the CNCF on 2025-10-08 (CNCF announcement).

## 10. Track MT (D459)

| Plan | Scope | Depends on |
|---|---|---|
| [MT1](../plans/2026-10-02-mt1-authentik-identity.md) | Authentik blueprints and the CI guard; the gateway's OIDC sign-in against Authentik; the RFC 8693 exchange for Loams tokens; groups → teams → OpenFGA tuples; `loams login` with device code; the showcase moves from Keycloak | §19's M2 identity work (the token endpoint, sessions); D66's outbox |
| [MT2](../plans/2026-10-02-mt2-knative.md) | `loams-runner-knative`; per-namespace tenancy in `loams-operator`; Kourier routing from the gateway; `loams-knative-source`; the Knative sink docs; usage-hook conformance with no meter | RN1 Tasks 1–3 (the trait); `loams-operator` (D185) |
| [MT3](../plans/2026-10-02-mt3-gitops-clever.md) | New waves and health checks; the Authentik and Knative `Application`s; the Flux layout; the k3s small profile; CKE variant | MT1 Task 1, MT2 Task 1; §25's layout |

MT, like tracks R, D, J and GT, interleaves on the one-build machine: one cargo build at a time. MT1 and MT3 are mostly YAML and e2e scripts; MT2 adds two crates outside `loams`'s default features.

## 11. Risks

| Risk | Mitigation |
|---|---|
| A future Authentik release moves a feature Loams uses into `authentik/enterprise/` | D458's guard runs on every Authentik bump; the pin moves only after MT1's e2e passes; the gateway's OIDC side is IdP-agnostic, so Keycloak remains a fallback |
| Authentik's monthly releases and security fixes | Pin a minor (`2026.8.x`), take patch releases promptly, record each bump (Q453) |
| Knative's cold start (image pull plus pod start) is far from T0's milliseconds | Knative is for `http-port` only (D441); `min-scale` per function for latency-sensitive services, charged by nothing in OSS |
| Two schedulers (supervisor and Knative) for one runtime | They own different contracts (§3.1); the `Runner` trait hides which one runs a function |
| Kourier and Envoy both in the path | Kourier is internal and small; Q446 asks whether `net-gateway-api` on Envoy Gateway removes Kourier |
| Usage under Knative is coarser than the supervisor's (pod cgroup, not per invocation) | Accepted: no metering in OSS (D444); per-invocation precision for billing is `loam-platform`'s problem |
| Removing §34 and §35 leaves references dangling in other branches | §34 stays as a stub at the same path; §36 was edited to match; the decision log records what moved (one row per moved range) |

## 12. Open questions

| # | Question | Owner | Needed by |
|---|---|---|---|
| Q440 | ~~Authentik's SCIM provider is free, and Loams could accept SCIM in OSS. Keep SCIM provisioning in `loam-platform` (D221), or move it to OSS for adoption~~ Answered 2026-10-02 by the owner: the recommended default — keep SCIM provisioning into Loams in `loam-platform` (D221); Authentik's own SCIM provider is used only for the showcase apps (§38 §4.3) | Founder | Resolved |
| Q441 | ~~Keep the single binary's built-in password and TOTP (§19 P7), or make an external OIDC IdP mandatory once MT1 lands~~ Answered 2026-10-02 by the owner: the recommended default — keep built-in password and TOTP: on by default when no `trusted_issuers` are configured, off otherwise (D450, MT1 Ruling 6) | Founder | Resolved |
| Q442 | Hosted Loams Cloud identity: Clerk (in `loam-cloud` today) or Authentik, given the 2026-10-01 "no paid plan" ruling. Decided in `loam-platform`, recorded here only for the cross-reference | Founder | Before the hosted beta |
| Q443 | ~~Knative Eventing's production broker: the Kafka broker over Loams's Kafka gateway (M5), a Loams broker class over streams, or none~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — no broker in MT2: sources deliver to Services directly, and the in-memory channel stays for development; a Kafka broker over Iggy's Kafka gateway is revisited after Q331's work; why: §38 §3.5 says direct delivery covers most uses | Eng | Resolved |
| Q444 | ~~gVisor mandatory for every `KnativeRunner` function, or optional for trusted single-org code~~ Answered 2026-10-02 by the owner: the recommended default — gVisor is mandatory for every `KnativeRunner` function (D441, MT2 Ruling 3) | Founder | Resolved |
| Q445 | ~~Should `KnativeRunner` emit per-invocation request and wall-time reports (no CPU) for showback dashboards, or stay at `usage: None` (D444)~~ Answered 2026-10-02 by the owner: the recommended default — stay at `usage: None`; no metering or showback in OSS (D444) | Founder | Resolved |
| Q446 | ~~Kourier, or `net-gateway-api` on Envoy Gateway so Envoy is the only proxy~~ Answered 2026-10-02 by the owner: the recommended default — Kourier, internal behind Envoy (D442); `net-gateway-api` is not adopted | Eng | Resolved |
| Q447 | ~~Authentik's Postgres: CloudNativePG now (D230), Loams Postgres (§28) later~~ Answered 2026-10-02 by the owner: the recommended default — CloudNativePG now (D230), Loams Postgres (§28) later | Eng | Resolved |
| Q448 | ~~Does Authentik's OIDC provider send back-channel logout, so a removed user's Loams session ends before its refresh **(verify)**~~ Answered 2026-10-02 by the owner: the plan's default — not an owner decision: MT1 Task 4 checks it; without back-channel logout the guide records the one-hour bound | Eng | Resolved |
| Q449 | ~~Flux as the default for the single-node profile, if MT3 measures Argo CD as too heavy (merges Q-RT-11)~~ Answered 2026-10-02 by the owner: the recommended default — Argo CD stays the default (D186, D454); Flux becomes the single-node default only if MT3 Task 6 measures Argo CD too heavy (this also answers Q-RT-11) | Eng | Resolved |
| Q450 | ~~Give §34's retained vendor-neutral decisions (D360–D365, D372, D374, D378) their own OSS document, and split GW1's vendor-neutral tasks (`buf breaking`, the CloudEvents profile) into an OSS plan~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — §34 itself is that document (it now holds only the retained vendor-neutral decisions), and GW1's vendor-neutral tasks (`buf breaking`, the CloudEvents profile, the event Arrow mapping) become an open plan here, written when GW1 starts; why: D220 keeps standards open, with no new file | Founder | Resolved |
| Q451 | ~~The `loam.dev/*` pod labels of §27 §3.2 under the `loams` rename: keep, or move to `loams.dev/*` with the rename PR~~ Answered 2026-10-02 by the owner: `loams.dev/*`, in the rename PR (D407) | Eng | Resolved |
| Q452 | ~~`loams-knative-source` for Iggy topics (§32): in MT2 or with FL1~~ Answered 2026-10-02 by the owner: the recommended default — with FL1 (MT2's deferred list) | Eng | Resolved |
| Q453 | ~~Authentik upgrade cadence and who takes security patches for self-hosters (chart values pinned in Loams's layout)~~ Answered 2026-10-02 by the owner: the recommended default — pin a minor (`2026.8.x`), take patch releases promptly, and record each bump in the layout; self-hosters follow Loams's pinned chart values (§38 §11) | Eng | Resolved |

## 13. Sources

Read on 2026-10-02 unless a date is given.

- The owner's rulings of 2026-10-01 and 2026-10-02 (quoted in the status line).
- Authentik: `github.com/goauthentik/authentik` at `version/2026.8.3` (released 2026-09-17): `LICENSE`; `authentik/enterprise/LICENSE` (the EE licence); directory listings of `authentik/providers` (`ldap oauth2 proxy rac radius saml scim`), `authentik/sources` (`kerberos ldap oauth plex saml scim telegram`), `authentik/stages`, `authentik/enterprise/providers` (`google_workspace microsoft_entra radius scim ssf ws_federation`), `authentik/enterprise/stages` (`account_lockdown authenticator_endpoint_gdtc mtls source`), `authentik/enterprise/{agents,audit,endpoints,lifecycle,policies,reports}`; `authentik/common/oauth/constants.py` (`GRANT_TYPE_TOKEN_EXCHANGE`); `authentik/providers/oauth2/views/` (`token.py`, `dcr.py`, `device_*`). Docs: docs.goauthentik.io/enterprise/enterprise-features; docs.goauthentik.io/sys-mgmt/tenancy ("This feature is in alpha"; Enterprise; a licence per additional tenant); docs.goauthentik.io/add-secure-apps/providers/oauth2/token_exchange (2026.8.0+, `act` claim); docs.goauthentik.io/add-secure-apps/providers/oauth2/client_credentials (JWT federation); docs.goauthentik.io/releases/2025.10 and goauthentik.io/blog/2025-11-13-we-removed-redis. Chart: `github.com/goauthentik/helm` (GPL-3.0, `authentik-2026.8.3`).
- Knative: `knative/serving` and `knative/eventing` releases knative-v1.23.0 (2026-07-29, 2026-07-28), Apache-2.0; `knative/serving` `config/core/configmaps/features.yaml` (`kubernetes.podspec-runtimeclassname: "disabled"`) and `config/core/configmaps/autoscaler.yaml` (`enable-scale-to-zero: "true"`, `scale-to-zero-grace-period: "30s"`); `knative-extensions/eventing-kafka-broker` knative-v1.23.1 (2026-08-25); `knative-extensions/net-kourier` knative-v1.23.0; `knative/func` knative-v1.23.3 (2026-09-03); CNCF announcement "Cloud Native Computing Foundation Announces Knative's Graduation" (2025-10-08).
- Clever Cloud: `CleverCloud/clever-kubernetes-operator` (MIT, v0.8.0, pushed 2026-09-04); `CleverCloud/terraform-provider-clevercloud` (Apache-2.0, v2.3.0, 2026-09-28); `CleverCloud/clever-tools` (Apache-2.0, 5.0.2, 2026-09-16); a repository search of the organisation for GitOps, deploy and operator projects (no reconciler or deployer found); §25 §2.
- Argo CD v3.5.3 (2026-09-14), Flux v2.9.6 (2026-10-01), k3s v1.37.1+k3s1 (2026-09-30): GitHub releases, Apache-2.0.
- This repository: §02 §7.4, §19, §21, §22 §4.4, §24 (§4, §7, §16), §25 (§1, §4, §6), §26, §27 (§3.2, §3.6), §32 (branch `flow-fabric-house-design`, D331–D332), §34, `docs/open-core.md`; D65, D66, D67, D111, D184–D188, D190, D202, D220, D221, D230, D270, D375, D376.
