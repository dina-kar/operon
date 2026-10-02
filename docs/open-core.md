# Loams — Open-Core Boundary

Status: **Approved** by the owner, 2026-09-29 (D220, D221 for audit and SSO; [decision log](design/13-decision-log.md)). Refines the monetization note in [§00 §8](design/00-pitch.md): the engine, all gateways and the operator stay Apache-2.0, and reliability and performance features are never withheld from open source.

## The rule

Anything needed to **self-host Loams as a single organisation** stays open source in this repository, under Apache-2.0. Anything needed **only to run Loams as a multi-tenant paid cloud** lives in the managed Loams Cloud platform (proprietary, separate repository), `loam-platform`.

This repository must never depend on `loam-platform`: no crate, package, build step, test or default configuration may require it. The platform consumes this repository's open hooks and APIs (metrics, usage events, quota enforcement, the operator and the admin APIs), the same ones any self-hoster can use.

## Open source (this repository)

| Area | What is open |
|---|---|
| Engine | Retrieval (vector, full-text, graph), streams, Iceberg analytics, and every wire API: Qdrant, the Elasticsearch subset, Flight SQL, Postgres read, MySQL |
| Live and metastore | The reactive database on TiKV, the TiKV metastore, and the change-feed bridges (Postgres logical replication, MySQL binlog) |
| Durable and jobs | The embedded Resonate server, the durable patterns, `loams-jobs`, the Celery transport and result backend, and `@loams/bullmq` |
| Runtime | The Rust Dapr API server, workerd and wasmtime hosting, gVisor sandboxing, Dapr secrets and state wiring, and the gateway |
| Tenancy and access | Namespaces, OIDC and API-key auth, plain OIDC SSO (self-hosters broker SAML through Authentik's open-source edition, §38 D447, or any other IdP), OpenFGA checks, and **enforcing** quotas and limits |
| Audit | Audit events for every admin, auth and data-access action, emitted as OTel logs to a Loams stream (the same pattern as the usage hooks); an audit query API and CLI, with a short default retention set by the operator. Extends D100's admin and security events and record fields (never document contents) |
| Observability and usage hooks | Prometheus and OTel metrics, cgroup labels per sandbox (`loams.slice/tenant-<org>.slice/fn-<id>.scope`), Envoy access logs, OTLP spans |
| Self-hosting | The Helm umbrella chart, the Loams operator, the Argo CD layout, RustFS defaults, backup and restore |
| Clients and docs | SDKs, the CLI, generated clients, and the engine design docs |

## Managed cloud only (`loam-platform`)

- Metering and billing.
- The multi-tenant control plane: provisioning, plans, entitlements, and **setting** quotas per plan.
- Fleet and multi-region operations, autoscaling policy and pre-warming.
- Hosted Neon/WeSQL fleet automation.
- BYOC management.
- Abuse, trust and safety.
- Hosted audit: the audit UI (search, filters, per-user and per-org timelines), long retention (1 year or more), tamper-evident storage and legal hold, continuous SIEM export (Splunk, Datadog) and compliance report packs.
- SCIM provisioning, org-wide enforced SSO, and cross-org admin.
- The internal admin console, support tooling and runbooks.

Quotas show the split: the engine enforces whatever limits it is given; the platform decides what those limits are for each plan. Audit and SSO follow the same principle: **no SSO tax** on SAML or OIDC, and the paid features are the operational ones at scale or across tenants.

## Borderline calls

| Topic | Open source | `loam-platform` |
|---|---|---|
| Neon/WeSQL | Routing, the change feed, basic branch creation | Fleet automation |
| Console | A basic single-cluster admin UI | The multi-tenant console |

## Applying it

- A new feature goes here if a single organisation running its own cluster needs it. It goes to `loam-platform` only if it exists solely to sell, bill or operate Loams for many tenants.
- When the platform needs something from the engine, add an open hook or API here (a metric, an event, an admin endpoint) instead of platform-specific code.
- Design docs 24 (CPU-time runtime) and 26 (jobs API), in review, follow this boundary; D190 on the §24 branch already moves billing to `loam-platform`.

## Reconfirmed 2026-10-02

The owner reconfirmed this boundary on 2026-10-02 ("keep loams cloud and loams-cloud private; may add Knative in OSS but no metering; I want adoption and also to raise money from VCs; move Cloudflare, OpenRTB etc. commercial to private repos"), withdrawing the ruling of 2026-10-01 that would have opened the multi-tenant platform. Recorded as [D403](design/13-decision-log.md) (approved) and in [§38](design/38-knative-authentik-gitops.md) (D440, proposed). Authentik's open-source edition replacing Keycloak and Clerk for OSS is D404:

- **Stays as above.** Metering and billing, the multi-tenant control plane, fleet operations, hosted databases, BYOC management and the hosted console remain `loam-platform` (and `loam-cloud` for the site and console).
- **Added to the open column**, as self-hosting features: Knative Serving and Eventing as an optional compute and delivery layer, **with no metering** (only §27's hooks); Authentik's open-source edition as the default IdP of the Kubernetes distribution, which takes Keycloak's place as the SAML broker in the Tenancy row and in D221 (D447); the GitOps layout's new waves (D453–D455).
- **Moved to `loam-platform`** as commercial components: the ad-tech protocol gateway with the OpenRTB and Google adapters (not to be confused with Loams's API gateway and its wire gateways, Qdrant, Elasticsearch, Postgres, MySQL and Flight SQL, which stay open as stated above) ([§34](design/34-protocol-gateway-and-standards.md) is now a stub keeping the vendor-neutral charter, CloudEvents profile and `Runner` trait), the Cloudflare deployment target and its startup-credits plan (formerly §35, now in `loam-platform`, private), the hosted Loams Cloud on Cloudflare, and the usage-event form and any metering ledger.
