# 41 — Loams Multitenant BYOC Control Plane with GitOps

Status: **Proposed** · 2026-10-02. Source: the owner's open-core ruling of 2026-10-02: "in open-core, multi-tenant Knative and GitOps using Argo CD is fully open source, so name it as Multitenant BYOC Control Plane with GitOps, and move the commercial API and metering to private, because they may be used to abuse by agents — integrity is the security principle of Loams." The boundary is in [open-core.md](../open-core.md); this document is the architecture of the open half. Decisions **D540–D559** and questions **Q540–Q559** are recorded in the [decision log](13-decision-log.md). Plan: [MT4](../plans/2026-10-02-mt4-byoc-control-plane.md).

**Amends** D220 and D221 (the multi-tenant control plane, BYOC management, SCIM, enforced SSO and cross-org admin are open), D403 and D440 (the parts that kept the multi-tenant control plane private), [§18](18-metastore-backends-and-router.md) §8 (D64: BYOC is open), [§24](24-cpu-time-runtime.md) §7 and §16, [§27](27-usage-hooks.md) (billing-grade metering moved to `loam-platform`), [§38](38-knative-authentik-gitops.md) §2.2 (its non-goals) and RN1. **Reaffirms** D190, D202 and D444 (no metering in this repository), with a new reason.

Markers: **(verify)** means not checked against a primary source; the task that depends on it checks it first.

---

## 1. Summary

| # | Decision | Status |
|---|---|---|
| D540 | **The Loams Multitenant BYOC Control Plane with GitOps is open source.** Multi-tenancy, Knative, Argo CD GitOps with Clever Cloud's operator fork and tooling, Authentik identity, BYOC install and management, and quota enforcement. Everything needed to run a multi-tenant BYOC deployment is Apache-2.0 here | Proposed · owner ruling |
| D541 | **Integrity is the security principle** for what stays private: figures that set what a tenant pays, and APIs that spend money, must not have their producers or validators open to manipulation by agents or tenants | Proposed · owner ruling |
| D548 | **Billing-grade metering moves to `loam-platform`**: the meter record, host reports and their delivery rules, the usage reporter, the final-read guarantee. Generic observability stays (§10) | Proposed · owner ruling |
| D550 | **Commercial APIs are private**; the open operations API has no endpoint that spends money (§12) | Proposed · owner ruling |

The full list, D540 to D559, is in the canonical decision log. This document designs the open half; the private half is in `loam-platform` docs 06 and 07.

### 1.1 The picture

```
                      +------------------------------------------------+
   operators, CLI --> |  Loams Control Plane (hub)                     |
   console (operator  |   loams-control: operations API, limits API    |
   view), Authentik   |   ControlStore + directory (§18 §5)            |
                      |   release channels, upgrade rings              |
                      |   tenants-repo writer (opens commits / PRs)    |
                      +----------------+-------------------------------+
                                       | commits
                              +--------v---------+
                              | tenants Git repo |  (Forgejo or GitHub; the source of truth
                              |  + release repo  |   for every tenant and cluster)
                              +--------+---------+
                          pull / sync  |
              +------------------------+---------------------------+
              |                        |                           |
     +--------v-------+     +----------v---------+      +----------v---------+
     | managed cluster|     | BYOC cluster A     |      | BYOC cluster B     |
     | Argo CD        |     | Argo CD (or Flux)  |      | Argo CD (or Flux)  |
     | loams-operator |     | loams-operator     |      | loams-operator     |
     | Knative, Authentik   | Knative, Authentik |      | + loams-byoc-agent |
     | engine, gateway|     | engine, gateway    |      |   (outbound only)  |
     +----------------+     +--------------------+      +--------------------+
```

Argo CD runs in the cluster it deploys to (hub-and-spoke is optional, §8.3); a BYOC cluster pulls, so the control plane never needs credentials into a customer's cluster.

## 2. Goals and non-goals

### 2.1 Goals

1. **One open way to run Loams for many tenants** on a cluster you own or a customer owns: onboarding, isolation, identity, compute, upgrades.
2. **GitOps is the only write path to a cluster.** The control plane and the console change desired state by committing to Git; Argo CD applies it. Nothing in the control plane holds a long-lived `kubectl` credential for a tenant cluster.
3. **BYOC without trust in the control plane's reach.** A customer's data plane keeps working if the control plane disappears (D64's local-meta mode), and the control plane never sees documents, vectors, text or bucket credentials (D64's data boundary).
4. **Enforce quotas without metering.** Limits are enforced from counters that exist only for enforcement (§9).
5. **Nothing here can be used to bill.** This is a design goal, not an accident (§11).

### 2.2 Non-goals

- **Metering, billing, invoices, credits, plans and entitlements.** `loam-platform` (D548, D550).
- **Commercial APIs**: the hosted Loams Cloud's paid APIs, the marketplace install and billing APIs, partner and commercial integrations (§12).
- **A replacement for Argo CD or Flux.** Clever Cloud publishes no GitOps engine (§38 §6.1); Argo CD stays (D186).
- **Authentik multi-tenancy.** It is Enterprise and alpha (§38 §4.2). A deployment has one Authentik tenant; Loams orgs are Loams's concept, mapped from Authentik groups (§38 D449).
- **Hosted-fleet operations** (predictive pre-warming, capacity management, hosted Neon/WeSQL fleet automation, abuse handling for the paid cloud): private.

## 3. Components, and where each is designed

This document consolidates; it does not repeat. Each row links to the design that holds the detail.

| Component | Role in the control plane | Designed in |
|---|---|---|
| `loams-control` | The hub: operations API, limits API, `ControlStore` server, directory, release channels, tenants-repo writer | §18 §6, §8 (D64, D65); this document §4 to §9 |
| `ControlStore` and the directory | Orgs, API keys, role bindings, quota limits, the pushed directory (org → namespaces → shard) | §18 §5.2, §6 (D63, D65) |
| `loams-meta-remote` | A `MetaStore` client for BYOC-managed-meta | §18 §8 (D64) |
| `loams-byoc-agent` | The outbound-only agent in a customer cluster: enrolment, health, commands | §7 (new) |
| `loams-operator` | The fork of `clever-kubernetes-operator`; reconciles `Loams`, `Function`, `RuntimePool`, `ObjectStore`, `Tenant`, per-namespace Knative tenancy | §25 §5 (D185), §38 §3.3 (D443) |
| Argo CD (default), Flux (small profile) | Applies the desired state; sync waves | §25 §6 (D186), §38 §6 (D453 to D456) |
| Clever Cloud tooling | `terraform-provider-clevercloud` and `karpenter-provider-clever-cloud` on CKE; `clever-tools` as UX reference | §25 §2 (D187), §38 §6.1 |
| Knative Serving and Eventing, Kourier | Tenant compute (T2 `http-port`) and the event adapter | §38 §3 (D441 to D446) |
| Authentik | Identity for people: OIDC, SAML, SCIM, MFA; blueprints in Git | §38 §4 (D447 to D452), MT1 |
| Gateway, namespace router, OpenFGA | Admission, quota enforcement, routing, authorization | §18 §5 to §7, §24 §5 |
| `loams-net` (optional) | `NetProvider`, Tailscale and Headscale clients, private connectivity | [§43](43-private-networking.md) |
| Console, operator view | The tenants, clusters and BYOC pages | §19; this document §12 |

## 4. The control plane

`loams-control` is a Connect-RPC service (D362) with two APIs and no billing surface:

- **Operations API** (`loams.control.v1`): `Org`, `Namespace`, `Cluster` (managed or BYOC), `Enrolment`, `ReleaseChannel`, `Rollout`, and the Git writer's status. Mutations are idempotent by client token.
- **Limits API**: `PUT/GET /v1/orgs/{org}/limits` and `/namespaces/{ns}/limits`, the quota record of §18 §6 (D65, D98) extended with the Kubernetes-side limits of §38 D443. Writers are the operator-held admin credential and, on the hosted cloud, the private plan mapping (D546, D550). Reads include **enforcement state** (`limit`, approximate `used`, `window`), documented as not billing-grade (D547).

Its state is the `ControlStore` (§18 §6) on any metastore backend (TiKV by default, D124 and D260). It is horizontally scalable and stateless apart from that store; the directory is replaced whole per change and pushed to gateways (§18 §5.2).

**Authorization.** Callers are people through Authentik (OIDC), agents (Loams `agent` principals, §19 P5) and services (mTLS plus an attenuated Biscuit, D188). Every call is checked in OpenFGA (D66, D67) against the object it names: `org`, `cluster`, `rollout`. A cross-org operator role (**cross-org admin**, D553) is a relation on the control plane's own root object, held only by the deployment's operators. Agent principals cannot hold it.

**What it does not do.** It never calls a billing system, never stores a price, plan name or invoice, and never reads usage. A hosted deployment adds those in front (§12), not inside.

## 5. Tenancy

The isolation layers are fixed by existing decisions; the control plane only provisions them.

| Layer | Mechanism | Source |
|---|---|---|
| Data | Org → namespaces → collections; one namespace per environment; all of a namespace's metadata in one shard; paths and keys carry the namespace | §18 §5 to §6 (D63, D65, D75) |
| Compute | One Kubernetes namespace per Loams namespace, `loams-ns-<namespace>`; default-deny `NetworkPolicy`; `ResourceQuota`; `LimitRange`; gVisor `RuntimeClass`; Knative cluster-local | §38 §3.3 (D443), §24 §4 |
| Identity | Authentik for people; Loams tokens and Biscuit for agents and calls; OpenFGA as the authority | §38 §4 (D449), §19 |
| Secrets | Dapr secrets, tenant-scoped; per-invocation Biscuit, no long-lived secret in a pod | §24 §5, §38 §3.3 |
| Cluster | Per-cluster mTLS and agent key for BYOC; nothing shared between clusters except what the directory names | §18 §8 |

## 6. Tenant onboarding through GitOps (new)

A tenant is a Git commit. The control plane never applies Kubernetes objects to a cluster itself.

**Repositories.** A *tenants repository* holds one directory per org, rendered from the operations API's records; a *release repository* (the umbrella chart `deploy/helm/loams-stack` and the GitOps layout) holds versions. They may be one repository. Forgejo (open, §39) is the default in a self-hosted deployment; GitHub and GitLab are supported (Q558).

```
tenants/
  <org>/
    org.yaml                      # Org: display name, identity group mapping, contact
    clusters.yaml                 # which clusters host this org (placement)
    namespaces/
      <namespace>.yaml            # Namespace: shard hint, limits, egress policy, features
    kustomization.yaml
```

**Flow** (create an org and its first namespace):

1. A caller (a person through the console, the CLI, an agent with the right relation, or the hosted cloud's commercial API) calls `CreateOrg` and `CreateNamespace`. The control plane checks OpenFGA, validates, and writes the records to the `ControlStore` in one transaction with an outbox row (D66's outbox pattern).
2. The *Git writer* reads the outbox and renders `tenants/<org>/…` deterministically from the records (same records, same bytes). It commits to a branch and, per the deployment's policy, either merges directly (self-service) or opens a pull request that a person approves (regulated).
3. An Argo CD `ApplicationSet` with a Git directory generator over `tenants/*` creates one `Application` per org on each cluster in the org's placement. The generator is the only thing that has to know the layout.
4. On sync, `loams-operator` reconciles `Namespace` objects into: the Loams namespace in the directory (idempotent upsert), the Kubernetes namespace `loams-ns-<namespace>` with `NetworkPolicy`, `ResourceQuota`, `LimitRange` and labels `loams.dev/org` and `loams.dev/namespace`, the Authentik group and role bindings (through the blueprint of MT1), and OpenFGA tuples for the org's teams.
5. The operator writes status back (`Ready`, `Degraded`, the observed generation). The control plane watches it and flips the namespace to `active` in the directory, which gateways receive on their next push (§18 §5.2). Requests before `active` get `404` for the namespace, never a half-provisioned one.

**Properties.**

- **Source of truth.** The `ControlStore` is authoritative for tenant records; Git holds the rendered desired state and is the audit trail of changes. If they disagree (someone edited Git by hand), the Git writer re-renders from the store on its next run and the manual edit is overwritten and reported as a `Drift` event; a protected branch with required review is the way to make Git edits legitimate (they go through the API, not around it).
- **Idempotent and replayable.** Rebuilding the tenants repository from the `ControlStore` yields identical bytes.
- **Drift.** Argo CD's self-heal reverts out-of-band edits; the operator reports a `Drift` condition for objects it does not own.
- **Offboarding.** `DeleteNamespace` marks the namespace `deleting` in the directory (gateways stop routing), commits the removal, the operator deletes the Kubernetes namespace after the data-plane erasure of §18 §9 (GDPR, D68 and D69) has completed, then the record. Argo CD's `prune` with a finalizer prevents data loss from an accidental directory deletion: removing `tenants/<org>/` does not delete data without the erasure workflow's receipt (the operator refuses to delete a namespace whose erasure receipt is absent).
- **Failure.** A failed sync leaves the previous state in force; the control plane reports the namespace as `provisioning` with the Argo CD condition; nothing is retried by hand.
- **Scale.** One `Application` per org, not per namespace, keeps Argo CD's object count at O(orgs). At the millions-of-namespaces scale of §18 §5, namespaces without a Kubernetes footprint (no functions) are directory records only and have no file in Git; only namespaces that need cluster resources (functions, Knative, dedicated limits) get `namespaces/<namespace>.yaml` (D544).

## 7. BYOC

Both modes of D64 are open and ship in M2.x. This section adds how a BYOC cluster connects, which §18 §8 left open.

| | BYOC-managed-meta | BYOC-local-meta |
|---|---|---|
| Metastore | `loams-meta-remote` in the customer's VPC against the control plane's metastore | In the customer's VPC (openraft, Postgres, DynamoDB or TiKV) |
| Control plane on the data path | Yes, for writes | No |
| When the control plane is down | Cached reads continue; writes stall | Nothing on the data path changes |

### 7.1 The BYOC agent and how it connects (D543)

`loams-byoc-agent` is a small Rust Deployment installed with the chart's BYOC profile. It is **outbound-only**: the customer opens no inbound port, and the control plane never holds credentials for the customer's cluster.

1. **Enrolment.** An operator creates an `Enrolment` in the control plane (`loams cluster enrol --name acme-prod`), which returns a one-time token (single use, 1 hour). The customer installs the chart with the token. The agent generates a key pair, sends a CSR with the token over TLS, and receives a client certificate for its `cluster_id` (24-hour validity, auto-renewed; the CA is the control plane's). The token is then dead.
2. **Channels.** Over mTLS HTTP/2 (D361) the agent opens (a) a *heartbeat stream* to the control plane with health, Argo CD application conditions, versions and enforcement counters' summaries; and (b) a *command stream* with a small closed set of commands (§7.2). Everything else is pull: the cluster's Argo CD (or Flux) pulls the tenants and release repositories directly from Git. For BYOC-managed-meta the same certificate authenticates `loams-meta-remote`.
3. **Scoping.** Every control-plane call from a cluster is scoped to its `cluster_id` and can touch only the orgs placed on it. A stolen certificate reaches one cluster's records, expires within a day and can be revoked in the `ControlStore`; revocation is pushed on the heartbeat stream and checked at the gateway.
4. **Air-gapped mode** (Q550). With no outbound path, the cluster uses a customer-run Git mirror of the release repository and tenants repository and no agent; upgrades are a bundle the customer carries in. There are no heartbeats, and the cluster is shown as `disconnected` by design.

### 7.2 What the agent may do

The command set is closed and typed. Nothing is a shell, a `kubectl` passthrough or a YAML apply.

| Command | Effect | Authorization |
|---|---|---|
| `Pause` / `ResumeRollout` | Stop or continue syncing a rollout wave on this cluster (suspends the ApplicationSet's auto-sync) | The cluster's own policy decides whether to accept it |
| `Report` | Send a diagnostics bundle (versions, conditions, logs the customer allows) | Customer-allowed scopes |
| `RotateCertificate` | Rotate the client certificate now | — |
| `SetLimits` | Apply limits for an org on this cluster, as written by the limits API | Signed by the control plane; limits can only be set from the control plane's record |

A cluster can be configured to run in *observe-only* mode, where it accepts none of these and the customer applies changes by merging the pull request themselves.

### 7.3 The data boundary

Unchanged from D64: the control plane may see namespace and collection names, schemas, object paths, offsets, pointers and lease keys, in clear. It never sees documents, vectors, text or bucket credentials. The heartbeat carries conditions and versions, never data.

### 7.4 Connectivity modes: outbound agent or tailnet (D587)

BYOC has two connectivity modes. The default is the outbound agent of §7.1. The optional mode joins the customer's cluster to a **tailnet** (the operator's, or the customer's own Tailscale or Headscale through the provider knob of §43 §6.4; §43, `43-private-networking.md`, provider-neutral): the agent runs `tailscaled` (userspace) as a sidecar, joins with a single-use key tagged `tag:byoc-<tenant>` that the control plane issues, and reaches the control plane's tailnet name. Neither mode opens an inbound port. In tailnet mode the policy lets a tenant reach `tag:control:443` only; the control plane never dials in; the customer may enable a time-boxed support grant that the control plane expires. Tenant isolation is a generated policy on one operator Headscale, with a dedicated Headscale per tenant on request (D586). The tenant's own users and east-west traffic are the tenant's own network. Cites §43 §6.

## 8. Upgrade waves (new)

Two different orderings exist and must not be confused.

- **Sync waves** order components *within one cluster* (§25 §6.3 and §38 §6.3: CRDs, operators, RustFS, PD and TiKV, Resonate and Authentik, the gateway and Knative, `Loams`, runtime tiers).
- **Upgrade rings** order *clusters and tenants* for a new release (this section).

### 8.1 Channels and rings (D545)

A release is a version of the umbrella chart plus a set of operator and image digests, published to a **channel**: `canary`, `early`, `stable`. A cluster follows one channel (the BYOC customer chooses; the default is `stable`). Within a channel, the control plane orders clusters into **rings**:

| Ring | Contents | Soak before next | Gate |
|---|---|---|---|
| 0 | The deployment's own canary cluster (and the showcase suite) | 30 minutes | All Argo CD apps `Healthy`; the e2e smoke suite and the conformance suites of D60 and D63 pass |
| 1 | Clusters that opted into `early` | 24 hours | No new `Degraded` conditions; error budget not burned |
| 2 | `stable` clusters in batches of at most 10% | 1 hour per batch | Same, plus no `Pause` from a cluster |
| BYOC with `maintenance window` | Applied inside the customer's window only | — | The customer's window and approval policy |

### 8.2 Mechanism

- The release repository has one branch per channel. The control plane promotes a release by fast-forwarding the next channel's branch **only after the previous ring's gate passes**; a gate failure stops promotion and opens an incident record.
- Rings are implemented with the ApplicationSet **progressive sync** (`RollingSync`, **(verify)**: alpha in Argo CD 3.x, checked in MT4 Task 0) over cluster labels `loams.dev/ring`, or, if that is not stable enough, with one ApplicationSet per ring and the control plane flipping `targetRevision` ring by ring. The second form is the fallback and needs nothing alpha.
- **Data-plane compatibility.** A release declares the minimum version it can run beside (`compatibleWith`), because rings run mixed versions. The operator refuses a rollout whose version skew exceeds that declaration. Migrations follow the expand-then-contract rule: a release in `stable` never contains the contract step of a migration whose expand step has not been in `stable` for one full ring cycle.
- **Rollback** is a revert of the channel branch; Argo CD syncs it. Stateful components with no safe downgrade (TiKV, PD, the metastore's on-disk format) carry a `noDowngrade` marker, and the operator refuses the downgrade and says so.
- **Pause.** A cluster's `Pause` (manual, or the agent's automatic one on a failed health check, Q548) stops its ring; the control plane does not override it.

### 8.3 Hub-and-spoke or per cluster (Q541, D186)

The default is Argo CD in each cluster, pulling (it works for BYOC and avoids hub credentials). Where the operator owns all clusters and wants one view, a hub Argo CD can manage spokes through registered cluster secrets. Hub credentials into a customer's cluster are not allowed for BYOC.

## 9. Quota enforcement without metering (new)

Quotas are limits and the counters that enforce them. They are **not** usage records, and are designed so that they cannot become one.

### 9.1 Where each limit is enforced

| Limit | Enforced by | State kept | Source |
|---|---|---|---|
| Request rate per namespace and surface | Token bucket at the rendezvous owner of the placement key; fallback per-gateway bucket sized quota ÷ gateways | In-memory bucket, refilled by time; no per-request record | §18 §6 |
| Ingest bytes per second | Token bucket at the gateway | In-memory | §18 §6 |
| Concurrent queries | Cost-weighted semaphore per collection (D98); namespace semaphore at the gateway | In-memory | D98 |
| Unapplied data | At write admission from the collection's backlog | The collection's own backlog counters | D86 |
| Storage bytes | Soft limit at write admission from partition bytes and manifest sizes, computed periodically | A periodically refreshed figure per namespace | §18 §6 |
| Metadata operations | Rate limit | In-memory | §18 §6 |
| Kubernetes compute | `ResourceQuota` and `LimitRange` per Kubernetes namespace; Knative `max-scale` | The API server's own quota status | §38 D443 |
| Function CPU time ceiling per invocation | The supervisor's per-invocation limits (D173) | In-process | §24 |

A request over quota gets HTTP 429 or gRPC `RESOURCE_EXHAUSTED` (§18 §6).

### 9.2 Counters for enforcement versus usage records (D546, D547)

| Property | Enforcement counter (open) | Billing-grade usage record (private) |
|---|---|---|
| Purpose | Decide admit or reject now | Decide what is charged |
| Accuracy | Approximate; may over- or under-admit within a window | Exact, reconciled |
| Durability | In memory or a periodically refreshed figure; lost on restart without consequence | Spooled, acknowledged, replayed, deduplicated |
| Identity | Keyed by namespace in the owner process | Per tenant, per invocation, with idempotency keys |
| Shared with the cluster operator | `used` against `limit`, approximate, in the limits API and the console, labelled "enforcement state, not billing-grade" | The platform's ledger |

If an enforcement counter is wrong, a tenant is throttled too early or too late for a window; nobody is charged wrongly. This is the whole point of keeping them apart: **an agent that games an enforcement counter gains at most a little throughput, and cannot lower a bill.**

### 9.3 Who sets limits

The limits record has one writer interface (the limits API). In a self-hosted deployment the operator sets limits through the CLI, the console or the tenants repository. On the hosted cloud, the private plan mapping writes them (D546). The engine and operator enforce whatever they are given and cannot tell the difference. If the writer is down, the last limits stay in force (enforcement does not depend on the platform).

## 10. Observability: what stays open and what moves (D547, D548)

The test applied to each item: could a charge depend on this value, and could a tenant or an agent profit from forging it? If yes, the producer and validator are private.

| Item | Decision | Why |
|---|---|---|
| Prometheus and OTLP metric families of §27 §3.1 (`loams_namespace_*`, `loams_function_*`, `loams_gateway_*`, `loams_dapr_*`, `loams_durable_*`) | **Stay open** | Capacity, dashboards and alerts for any operator; they carry no acknowledgement or exactly-once claim, and the platform never bills from them alone |
| OTel traces and logs, audit events as OTel logs (D221) | **Stay open** | Debugging and audit |
| Cgroup layout and pod labels `loams.dev/*` (§27 §3.2) | **Stay open** (without the final-read acknowledgement) | They are also the isolation and scheduling labels (NetworkPolicy, quotas, Knative); a platform node agent reads cgroup files directly from outside the sandbox, so the layout is a stable interface, not a meter |
| Envoy access logs and the gateway-set `x-loams-tenant` header (§27 §3.4) | **Stay open** | Routing and authorization use the header; the sink is off by default |
| Quota counters and enforcement state (§9) | **Stay open** | Enforcement |
| Cgroup retention window after a sandbox finishes | **Stays open** (10 minutes default) | Garbage collection |
| `SandboxFinished` notice and "remove only after the consumer acknowledges the final read" | **Moves** | A delivery guarantee that exists to make totals exact for billing |
| `loams.meter.v1` (`HostReport`, `Invocation`, acks), `/run/loams/meter.sock`, buffering, resend and dedupe rules | **Moves** | The billing-grade record and its protocol |
| The usage reporter (`loams-meter`, RN1 Tasks 1 and 2) | **Moves** | The producer of the record |
| Lambda usage header `x-loams-usage`, the billed-duration cap, the `REPORT` line parser, `provider_billed_ms` | **Move** | A tenant-reachable value that sets a charge |
| The CloudEvents meter form `io.loams.dev.meter.usage.v1` | **Moves** (already, §38 D440) | The ledger's input |
| `GET /v1/orgs/{org}/usage` (rollups) | **Moves** | Billing-grade |
| `D103`'s logical-bytes usage rows in the `ControlStore` | **Stay open** as the engine's own view; renamed "namespace statistics" in the API so that no one mistakes them for billing | D103 |
| Per-invocation observer extension point | **New and open** (D549): `InvocationObserver` in `loams-runner`, a Rust trait called with a plain struct (`org`, `namespace`, `function`, `runner`, wall and CPU as measured, outcome) at the end of each invocation; no wire format, no buffer, no persistence | The platform's private implementation plugs in here, link-time; the trait is generic, like a tracing layer, and also feeds the open `loams_runner_*` metrics |

The platform may not need the observer at all: cgroups are authoritative for T0 and T2 totals and are read from outside the sandbox. T1's shared wasmtime host, where one process serves many tenants, is where per-invocation data matters; the supervisor already measures CPU per poll (§27 §3.3), and exposes it to the observer.

## 11. Security: integrity, and the control plane's own threats

### 11.1 The principle applied

| Attack | Where it would land | Why the open design is safe |
|---|---|---|
| Forged usage for another tenant | A published meter wire format with a node-local socket | No such format or socket in the open repository; the private reporter authenticates the peer and signs the epoch (`loam-platform` doc 06 PD63 and PD64) |
| Replayed or duplicated reports to inflate a rival's bill | Published dedupe rules | Private (doc 06) |
| Under-reported CPU to avoid a charge | A tenant-reachable usage header or an in-sandbox rusage | Private; billed from cgroup totals and provider figures (doc 06 PD63, PD66, PD67) |
| Scripted loops on a paid endpoint | A public validator for plan changes, entitlements or credits | Those endpoints are commercial and private; the open operations API spends nothing (§12) |
| An agent raising its own quota | The limits API | Writers are operator credentials or the private plan mapping; agent principals cannot hold `limits:write` |
| An agent minting tenants to exhaust a cluster | The operations API | `CreateOrg` and `CreateNamespace` are rate limited per principal and bounded by a per-deployment tenant cap, both enforced here (limits, not charges). **Fail closed:** if the limits store is unavailable, tenant creation is refused, while gateways keep enforcing the last pushed limits on existing tenants |

### 11.2 The control plane's own threats

- **Git is the write path, so Git access is the control.** Branch protection on the tenants and release repositories; the control plane's bot identity is the only writer to `main` in self-service mode; signed commits are required (Sigstore gitsign or SSH signing, Q556, **(verify)** in MT4 Task 0) and Argo CD verifies them (`signatureKeys`).
- **Blast radius of the hub.** A hub compromise can write tenant files but cannot reach a BYOC cluster directly (pull model); a cluster's own policy (`observe-only`, sync windows, signature verification) bounds what a malicious commit can do.
- **Authentik hygiene.** No licence key ever (D458). Outposts and the control plane's service identities use short-lived tokens (RFC 8693 exchange, D449).
- **Supply chain.** Images pinned by digest, `cargo deny`, provenance for the operator fork (MIT notice kept, D185), `buf breaking` on `proto/loams/control`.
- **Tailnet policy.** Changes are reviewed commits and refused on failing tests (D585); NET1 is optional and independent of MT4.
- **Audit.** Every control-plane mutation is an audit event (D221) naming the principal, including the agent chain (`act`) when an agent acted for a person.

## 12. Console, hosted cloud and the dependency direction

- **Console.** The open console gains an *operator view* (D555): orgs, namespaces, clusters, BYOC enrolments, rollouts, rings and enforcement state. It has no billing pages. The hosted console with billing pages is `loam-cloud` (private).
- **The hosted Loams Cloud** is this control plane plus a private commercial layer (`loam-platform` doc 07, PD69). A tenant's request reaches a commercial API, which checks identity, plan and entitlement and then calls the operations API with a service credential. The operations API is not exposed to tenants on the hosted cloud.
- **Extension points the platform uses**, all open: the operations API, the limits API, the tenants Git repository, `InvocationObserver`, OTel, the cgroup layout and pod labels. None carries a billing-grade format.
- **The dependency runs one way** (D202, D551): no crate, test, chart value or default requires `loam-platform`. CI guards it (D552, MT4 Task 8).

## 13. Supersessions

| Earlier text | Now |
|---|---|
| D220 and [open-core.md](../open-core.md): the multi-tenant control plane, BYOC management and fleet operations are private | **Amended** (D540). Control plane, BYOC and multi-cluster GitOps are open; hosted-only fleet operations stay private |
| D221: SCIM, enforced SSO and cross-org admin are private | **Amended** (D553): open through Authentik; hosted audit UI, long retention, SIEM and compliance packs stay private |
| D403, D440: "keep loams cloud private; Knative in OSS but no metering" | **Kept** except that the multi-tenant control plane is now open (D540) |
| §18 §8 (D64): the hosted `loams-control` | Now the open `loams-control` (this document §4) |
| §24 §7 (metering hooks table) and §16 (D376: usage from every runner reaches §27's contract) | **Superseded** by D548: observability hooks stay; the contract moved. Notes added |
| §27 §3.3 (host reports), §3.6 (external runner usage, additive fields, CloudEvents form), the `SandboxFinished` acknowledgement in §3.2, §4 | **Moved** to `loam-platform` doc 06; one-paragraph stubs remain |
| RN1 Tasks 1 and 2 (`loams.meter.v1`, `loams-meter`), the one-reporter rule in Task 3, Task 5's usage header and cap | **Moved or dropped** (D556); RN1 keeps the trait, `RunnerHost` with the observer, `ProcessRunner` and a usage-free `LambdaRunner` |
| §38 §2.2 non-goals: "Metering, billing, multi-org control planes and BYOC management … stay in `loam-platform`" | **Amended**: only metering and billing do. Multi-org control plane and BYOC management are goals of this document |
| §38 D444 (no meter on Knative) | **Kept**; the reason is now integrity |
| §38 §3.3: "who sets [quota] per plan is `loam-platform`'s concern" | **Kept**; the limits API is the seam (D546) |
| MT2 Task 4 (hooks without a meter) | **Amended**: the test now asserts no billing-grade names and that `KnativeRunner` calls the observer with no usage payload |
| MT3 "What MT3 leaves to others": hub-and-spoke for the hosted cloud, BYOC fleet automation | **Moved into MT4** (the open parts) |
| `loam-platform` README: "Tenant control plane", "BYOC management agent" as private | **Superseded** in `loam-platform` PR (README boundary) |

## 14. Plan and exit

[MT4](../plans/2026-10-02-mt4-byoc-control-plane.md) builds what is new here: `loams-control` and `loams.control.v1`, the tenants-repo Git writer and ApplicationSet layout, the BYOC agent and enrolment, release channels and rings, limits API and enforcement state, the operator view, the `InvocationObserver` and the no-metering CI guard. MT1 to MT3 remain as written, with the notes in their plans.

**Exit (D558).** On k3d in CI: create an org and a namespace through the API and see the commit, the `Application`, the Kubernetes namespace with its `NetworkPolicy` and `ResourceQuota`, and the directory entry `active`; enrol a second k3d cluster as BYOC through the agent and onboard a tenant there; promote a release through ring 0 to ring 1 and fail a gate on purpose to see promotion stop; exceed a limit and receive 429 with no usage record written anywhere; and `scripts/ci/no-metering.sh` passes.

## 15. Risks

| # | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| 1 | Open code is forked and hosted by a third party as a competing cloud | Medium | Medium | Apache-2.0 is the owner's choice for adoption; the moat is the hosted service, the marketplace and the private billing stack (Q544) |
| 2 | Argo CD's progressive sync stays alpha | Medium | Low | Fallback: one ApplicationSet per ring (§8.2) |
| 3 | The observer trait becomes a de facto meter in the open code | Low | High | A CI guard on names; review rule in CONTRIBUTING; the trait carries no wire format, buffer or persistence |
| 4 | Git as the write path adds latency to onboarding | Medium | Low | Directory-only namespaces for the common case (D544); sync is seconds on a healthy cluster |
| 5 | A BYOC customer's agent certificate is stolen | Low | Medium | 24-hour validity, per-cluster scope, revocation on the heartbeat, `observe-only` mode |
| 6 | Mixed-version rings break a data-plane contract | Medium | High | `compatibleWith`, expand-then-contract, conformance suites per ring gate |

## 16. Sources

Read on 2026-10-02: [open-core.md](../open-core.md) (previous revision); §18 §5 to §9; §24 §4, §5, §7, §16; §25 §5 to §6; §27 in full; §34's stub; §38 in full; the plans RN1, MT1, MT2, MT3; decisions D63, D64, D65, D86, D98, D103, D185 to D188, D190, D200 to D202, D220, D221, D403, D404, D440 to D459; `loam-platform` docs 01 to 05 and README. External claims marked **(verify)** (Argo CD ApplicationSet progressive sync status in 3.5, commit signature verification options) are checked by MT4 Task 0.
