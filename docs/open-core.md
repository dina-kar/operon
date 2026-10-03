# Loams — Open-Core Boundary

Status: **Revised by the owner, 2026-10-02** ([D540 to D559](design/13-decision-log.md), design [§41](design/41-multitenant-byoc-control-plane.md)). Earlier rulings: approved 2026-09-29 (D220, D221), reconfirmed 2026-10-02 (D403, D440). The engine, all gateways and the operator stay Apache-2.0, and reliability and performance features are never withheld from open source.

## The rule

> "In open-core, multi-tenant Knative and GitOps using Argo CD is fully open source, so name it as **Multitenant BYOC Control Plane with GitOps**, and move the commercial API and metering to private, because they may be used to abuse by agents. Integrity is the security principle of Loams." (owner, 2026-10-02)

1. **Open source, in this repository, Apache-2.0:** everything needed to run Loams as a multi-tenant, bring-your-own-cloud deployment. That is the **Loams Multitenant BYOC Control Plane with GitOps**: multi-tenancy (orgs, namespaces, isolation, quota enforcement), Knative Serving and Eventing for tenant compute, Argo CD GitOps with Clever Cloud's operator fork and tooling, Authentik identity, and BYOC install and management into a customer's own cluster or cloud. It is designed in [§41](design/41-multitenant-byoc-control-plane.md).
2. **Private, in `loam-platform`:** **metering** (the meter record, the collector and usage reporter whose output feeds billing, the ledger), **billing**, and the **commercial APIs** (the hosted Loams Cloud's paid APIs, the marketplace install and billing APIs, partner and commercial integrations).
3. **This repository never depends on `loam-platform`:** no crate, package, build step, test or default configuration may require it. The platform consumes this repository's open extension points (the control plane's operations and limits APIs, the tenants Git repository, the `InvocationObserver` trait, OpenTelemetry, the cgroup layout and pod labels), the same ones any self-hoster can use.

## The security principle: integrity

**Usage figures that set what a tenant pays, and APIs that spend money, must not have their producers or validators open to manipulation by agents or tenants.**

When the wire format, the trust rules and the acknowledgement protocol of a meter are public, and its code runs in a tenant's own cluster, an attacker has a map of what to forge: usage reported for the wrong tenant, **replayed** host reports, **under-reported** CPU, a spoofed usage header, duplicated or reordered records. The same holds for a paid API: a published, scriptable validator is a target for agents that loop on it. So:

- The open repository exposes **generic observability** (OpenTelemetry metrics, logs and traces; cgroup labels; Envoy access logs) and **quota enforcement**. Nothing in it claims to be billing-grade, and nothing in it is the source of truth for a charge.
- The private platform reads those signals only as hints, and bills from sources a tenant cannot reach.
- The open control plane has an operations API (tenants, clusters, limits, upgrades). It has **no endpoint that creates a charge, changes a plan or reads billing-grade usage**. On the hosted cloud it is never exposed to tenants; the private commercial API stands in front of it.

## The boundary

| Area | Open source (this repository) | Private (`loam-platform`) | Why |
|---|---|---|---|
| Engine | Retrieval (vector, full-text, graph), streams, Iceberg analytics, every wire API (Qdrant, the Elasticsearch subset, Flight SQL, Postgres read, MySQL) | — | Self-hosting |
| Live, metastore, durable, jobs | Reactive database on TiKV, TiKV metastore, change-feed bridges, Resonate, `loams-jobs`, `@loams/bullmq` | — | Self-hosting |
| Runtime | Rust Dapr server, workerd and wasmtime hosting, gVisor, secrets and state wiring, the gateway, the `Runner` trait and the `InvocationObserver` extension point | The observer implementation that feeds metering | The trait carries no wire format and no storage; the implementation is the meter |
| **Multi-tenancy** | Orgs, namespaces, per-namespace Kubernetes isolation (`NetworkPolicy`, `ResourceQuota`, `LimitRange`), namespace router and directory, OIDC and API-key auth, OpenFGA | — | Needed to run any multi-tenant deployment |
| **Knative** | Serving and Eventing as the tenant compute and event layer, `KnativeRunner`, `loams-knative-source` | — | Already open (§38); stays |
| **GitOps** | Argo CD app-of-apps and ApplicationSets, sync waves, the Flux layout, tenant onboarding through a tenants repo, upgrade waves, Clever Cloud's operator fork (`loams-operator`), `terraform-provider-clevercloud` and Karpenter on CKE | — | The way a multi-tenant deployment is installed and changed |
| **Identity** | Authentik open-source edition, blueprints, plain OIDC SSO, SAML through Authentik, **SCIM provisioning through Authentik, enforced org-wide SSO and cross-org operator admin** (moved to open, D553) | Hosted-cloud customer identity choices (Clerk beta, doc 04) | A multi-tenant operator needs them; Authentik's open edition supplies them |
| **BYOC** | Both modes of D64 (managed-meta and local-meta), `loams-meta-remote`, the BYOC agent, enrolment, health, pull-based upgrades | The commercial BYOC contract and its support tooling | Everything to run BYOC is open |
| **Control plane** | `loams-control`: operations API, `ControlStore`, directory, tenants-repo writer, release channels and upgrade rings, quota **limits** API | Plans, entitlements, and the plan-to-limits mapping | The limits are mechanism; the plan table is commercial |
| **Quotas** | The limits record, **enforcement** at the gateway, the owner of the placement key, the operator (`ResourceQuota`, Knative `max-scale`), HTTP 429 and `RESOURCE_EXHAUSTED`, and read-only enforcement state (`used` against `limit`, approximate) | **Setting** limits per plan | Enforcement without a meter (§41 §9) |
| Observability | Prometheus and OTel metrics, logs and traces, cgroup layout and pod labels per sandbox, Envoy access logs with the gateway-set `x-loams-tenant`, operational dashboards (not billing-grade) | Reads them as hints; cross-checks against its own trusted sources | Useful to every operator; not a charge |
| **Metering** | Nothing billing-grade | The meter record (`loams.meter.v1`), host reports and their delivery and acknowledgement rules, the usage reporter and node collector, the final-read guarantee, provider reconciliation, the ledger | Integrity |
| **Billing** | — | Pricing, invoices, credits, Clerk and Stripe integration | Integrity; commercial |
| **Commercial APIs** | — | The hosted Loams Cloud's paid APIs (signup with a plan, plan changes, entitlements, billing-grade usage API), the marketplace install and billing APIs, partner and commercial integrations (the protocol gateway, the Cloudflare target) | Integrity; abuse by agents |
| Audit | Event emission as OTel logs to a Loams stream, query API and CLI, short default retention (D221) | Hosted audit UI, long tamper-evident retention and legal hold, SIEM export, compliance packs | Operational, hosted |
| Hosted operations | — | Predictive pre-warming and capacity for the hosted fleet, hosted Neon/WeSQL fleet automation, abuse and trust and safety, the internal admin console, support tooling, runbooks | Only exist to sell and operate the hosted cloud |
| Console | A single-cluster admin UI and an **operator view** of tenants, clusters and BYOC (D555) | The hosted console with billing pages (`loam-cloud`) | |
| Clients and docs | SDKs, the CLI, generated clients, engine and control-plane design docs | Platform design docs | |

Quotas show the split: the engine and operator enforce whatever limits they are given; the open control plane stores and distributes them; the platform decides what they are for each paid plan. Audit and SSO follow the same idea: **no SSO tax** on SAML, OIDC or SCIM.

## What each earlier ruling now means

| Earlier ruling | Now |
|---|---|
| **D220** (2026-09-29): the multi-tenant control plane, BYOC management and fleet operations are private | **Amended.** The control plane, BYOC management and multi-cluster GitOps are open. Hosted-only fleet operations (pre-warming, capacity, hosted databases) stay private |
| **D221**: SCIM, enforced org-wide SSO and cross-org admin are private | **Amended** (D553): open, through Authentik. Hosted audit UI, long retention, SIEM and compliance packs stay private |
| **D403 and D440** (2026-10-02, earlier): Knative open with no metering; platform private | **Kept** for Knative and for "no metering in OSS"; the multi-tenant control plane is now open too |
| **D190, D202, D444**: billing and metering private; no meter on Knative | **Kept and strengthened**: the reason is now stated as integrity (D541) |
| **D200 to D201** ([§27](design/27-usage-hooks.md)): a billing-grade usage contract with host reports | **Superseded in part** (D548): the record, socket and reporter moved to `loam-platform`; generic metrics, cgroup labels and access logs stay |

## Applying it

- A feature goes here if a person running Loams for **many tenants on their own clusters or clouds** needs it. It goes to `loam-platform` only if it **produces or validates a figure that sets a charge, or is an API that spends money**, or exists solely to sell or operate the hosted cloud.
- When the platform needs something from the engine or the control plane, add an open extension point here (an API, a trait, a label), not platform-specific code. A new open extension point must not carry a billing-grade wire format.
- CI keeps the boundary: `scripts/ci/no-metering.sh` (planned: MT4 Task 8, issue #258; not present until it lands) will fail the build if a billing-grade metering name (`loams.meter`, `meter.sock`, `HostReport`, `x-loams-usage`) appears outside the allowlist of historical documents.
- Before adding a metric, ask: could a tenant or an agent profit from forging it? If a charge could depend on it, it does not belong here.
