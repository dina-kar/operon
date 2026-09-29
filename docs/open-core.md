# Loam — Open-Core Boundary

Status: **Approved** by the owner, 2026-09-29 (D220, [decision log](design/13-decision-log.md)). Refines the monetization note in [§00 §8](design/00-pitch.md): the engine, all gateways and the operator stay Apache-2.0, and reliability and performance features are never withheld from open source.

## The rule

Anything needed to **self-host Loam as a single organisation** stays open source in this repository, under Apache-2.0. Anything needed **only to run Loam as a multi-tenant paid cloud** lives in the managed Loam Cloud platform (proprietary, separate repository), `loam-platform`.

This repository must never depend on `loam-platform`: no crate, package, build step, test or default configuration may require it. The platform consumes this repository's open hooks and APIs (metrics, usage events, quota enforcement, the operator and the admin APIs), the same ones any self-hoster can use.

## Open source (this repository)

| Area | What is open |
|---|---|
| Engine | Retrieval (vector, full-text, graph), streams, Iceberg analytics, and every wire API: Qdrant, the Elasticsearch subset, Flight SQL, Postgres read, MySQL |
| Live and metastore | The reactive database on TiKV, the TiKV metastore, and the change-feed bridges (Postgres logical replication, MySQL binlog) |
| Durable and jobs | The embedded Resonate server, the durable patterns, `operon-jobs`, the Celery transport and result backend, and `@loam/bullmq` |
| Runtime | The Rust Dapr API server, workerd and wasmtime hosting, gVisor sandboxing, Dapr secrets and state wiring, and the gateway |
| Tenancy and access | Namespaces, OIDC and API-key auth, OpenFGA checks, and **enforcing** quotas and limits |
| Observability and usage hooks | Prometheus and OTel metrics, cgroup labels per sandbox (`loam.slice/tenant-<org>.slice/fn-<id>.scope`), Envoy access logs, OTLP spans |
| Self-hosting | The Helm umbrella chart, the Loam operator, the Argo CD layout, RustFS defaults, backup and restore |
| Clients and docs | SDKs, the CLI, generated clients, and the engine design docs |

## Managed cloud only (`loam-platform`)

- Metering and billing.
- The multi-tenant control plane: provisioning, plans, entitlements, and **setting** quotas per plan.
- Fleet and multi-region operations, autoscaling policy and pre-warming.
- Hosted Neon/WeSQL fleet automation.
- BYOC management.
- Abuse, trust and safety.
- The internal admin console, support tooling and runbooks.

Quotas show the split: the engine enforces whatever limits it is given; the platform decides what those limits are for each plan.

## Borderline calls

| Topic | Open source | `loam-platform` |
|---|---|---|
| Neon/WeSQL | Routing, the change feed, basic branch creation | Fleet automation |
| Console | A basic single-cluster admin UI | The multi-tenant console |
| SSO | Plain OIDC | Enterprise SSO/SCIM: a separate call, not yet decided |

## Applying it

- A new feature goes here if a single organisation running its own cluster needs it. It goes to `loam-platform` only if it exists solely to sell, bill or operate Loam for many tenants.
- When the platform needs something from the engine, add an open hook or API here (a metric, an event, an admin endpoint) instead of platform-specific code.
- Design docs 24 (CPU-time runtime) and 26 (jobs API), in review, follow this boundary; D190 on the §24 branch already moves billing to `loam-platform`.
