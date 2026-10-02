# 25 — Clever Cloud's Open-Source Stack and the GitOps Deployment

Status: **Proposed** · 2026-09-29. This is the companion to [§24](24-cpu-time-runtime.md). The owner's direction (2026-09-29): "use rustfs and clever cloud opensource stack to gitops, evaluate all the tools that are we adopt from clever cloud". This document does four things: it inventories every relevant open-source project from Clever Cloud (§2), compares Sōzu with Loams's planned edge (§3), compares Biscuit with the planned auth (§4), and designs the GitOps layout (§5–§6). Its decisions are **D185–D188** in §24's table, and its questions are **Q-RT-8 … Q-RT-14**.

> **Amended 2026-10-02** by [§38](38-knative-authentik-gitops.md) (proposed): "GitOps from Clever Cloud" means Clever's open-source operator and infrastructure tooling (D185, the CKE Terraform and Karpenter providers); Clever publishes no GitOps engine, so **Argo CD stays** (D186, D453), with a Flux layout for the smallest profile (D454). §6.3 gains waves for CloudNativePG, the Knative Operator, Authentik and Knative (D455); the operator also reconciles Knative tenancy per namespace (D443). Plan MT3.

Markers are the same as in §24. Every license was read from the repository's `LICENSE` file (or, where there is none, from the crate manifest, as noted). Activity dates come from the GitHub API on 2026-09-29.

**License rule applied throughout.** A **linked dependency** must not be AGPL, BSL, SSPL or ELv2. An **AGPL service** may run only unmodified, as a separate process, and the risk is flagged even then. LGPL linked into a Rust binary is not banned by the rule, but static linking brings relinking obligations. It is flagged wherever it appears.

---

## 1. Summary

- **Adopt as linked dependencies:** `biscuit-auth` (Apache-2.0) for sandbox tokens (D188), and `clevercloud-sdk` (MIT) behind an optional Cellar provider feature.
- **Fork:** `clever-kubernetes-operator` (MIT), as the skeleton of `loams-operator` (D185). What carries over is small, a kube-rs controller registry, finalizers, event recording, metrics and a hardened Helm chart, because its CRDs provision Clever Cloud add-ons through Clever's API rather than running workloads in the cluster.
- **Adopt as unmodified tools or services, only when deploying on Clever Cloud:** `terraform-provider-clevercloud` and `karpenter-provider-clever-cloud` (Apache-2.0), plus `biscuit-cli` (BSD-3-Clause) as a developer tool.
- **Reject as the edge:** Sōzu (AGPL-3.0) and `sozu-gateway`. Envoy stays the edge, and Pingora is the library if a Rust L7 component is ever needed.
- **Reference only:** clever-tools (CLI UX), clever-components (UI), kawa, nlrs, poule, simple-vmm, cellar-migration, kaniop (AGPL-3.0, not Clever's).
- **Not published:** Clever Cloud's hypervisor, deployer, log pipeline and Materia KV engine. No open repository was found in `CleverCloud`, `sozu-proxy` or `eclipse-biscuit`; only `simple-vmm`, a 2021 example VMM, touches virtualization.
- **GitOps: Argo CD** (D186), with an app-of-apps and sync waves over one umbrella chart. RustFS is the object store (D178), TiDB Operator v2 deploys PD and TiKV (D179), and the forked operator reconciles `Loams` and `Function` resources.

## 2. Inventory (D187)

Searched on 2026-09-29: every public, non-archived, non-fork repository of `github.com/CleverCloud` (about 250), `github.com/sozu-proxy` and `github.com/eclipse-biscuit` (where Biscuit moved from Clever Cloud), and `biscuit-auth`. About 150 of Clever's repositories are `*-example` deployment samples, most with no license. They are grouped as one row. Everything with a plausible place in Loams's GitOps, runtime, edge or auth stack is listed individually.

### 2.1 GitOps, operators and infrastructure

| Project | What it does | License (file) | Last commit · release | Lang | Fit in Loams | Verdict | Rationale |
|---|---|---|---|---|---|---|---|
| **CleverCloud/clever-kubernetes-operator** | Exposes Clever Cloud add-ons (PostgreSQL, MySQL, Redis, MongoDB, Pulsar, Cellar, Elasticsearch, Keycloak, Matomo, Metabase, Otoroshi, KV, Azimutt, ConfigProvider) as CRDs in group `api.clever-cloud.com/v1`, provisioned through Clever's API | **MIT** (`LICENSE`, © 2021 Clever Cloud) | 2026-09-04 · v0.8.0 (2026-06-09) | Rust (kube 3.1, k8s-openapi 0.27, edition 2024) | Skeleton of `loams-operator` | **Fork** (D185) | Its reusable part is `crates/core` (a `Controller` trait, a registry and sync strategies; 212 lines), `svc/k8s` (finalizer, event recorder, secret and resource helpers) and `svc/http` (metrics server), about 1,300 lines, plus a Helm chart with NetworkPolicy, PDB and CA bundle. The `svc/clevercloud` client and the 16 add-on CRDs are dropped. The README says "under development … may have bugs". The MIT notice is kept |
| **CleverCloud/clevercloud-sdk-rust** | Rust client and types for Clever Cloud's API | **MIT** | 2026-06-04 · v1.0.1 | Rust | Provisioning Cellar buckets and keys in the Cellar `ObjectStoreProvider` | **Adopt, linked**, behind the optional feature `provider-cellar` | License is fine. It is only needed to *provision* on Clever; reading and writing Cellar is plain S3 |
| **CleverCloud/terraform-provider-clevercloud** | Terraform provider for Clever resources | Apache-2.0 | 2026-09-28 · v2.3.0 | Go | Infra layer below GitOps when the target is Clever Kubernetes Engine (CKE): cluster, Cellar, network groups | **Adopt as a tool** (unmodified), Clever targets only | Active and Apache-2.0. Runs outside the cluster, before Argo CD |
| **CleverCloud/karpenter-provider-clever-cloud** | Karpenter `CloudProvider` over CKE's `nodegroups.api.clever-cloud.com/v1` | Apache-2.0 | 2026-08-12 · v0.12.0 | Go | Node autoscaling for runtime tiers on CKE | **Adopt as an unmodified service**, Clever targets only | README says "under development", validated end to end on a live CKE cluster. It is irrelevant off Clever |
| CleverCloud/pulumi-clevercloud | Pulumi provider | Apache-2.0 | 2026-01-26 · v0.0.19 | Go | Alternative to Terraform | Reference only | Pre-1.0, slower cadence |
| CleverCloud/clever-autoscaler-operator-example | Sample node autoscaler for CKE | **no LICENSE file** | 2025-12-04 · none | TS | — | Reject | Unlicensed; superseded by the Karpenter provider |
| CleverCloud/clever-cloud-review-app | GitHub Action for per-PR review apps on Clever | **no LICENSE file** | 2025-08-07 · v2.0.2 | HTML/shell | Preview environments | Reject | Unlicensed; previews are a non-goal (§24 §2.2) |
| pando85/kaniop *(not Clever)* | Kanidm operator in Rust | **AGPL-3.0** (`LICENSE.md`) | 2026-09-26 · v0.16.4 | Rust | Operator design | **Reference only** | AGPL: no code copied (as the draft already says) |

### 2.2 CLI, SDKs and UI

| Project | What it does | License | Last commit · release | Lang | Fit | Verdict | Rationale |
|---|---|---|---|---|---|---|---|
| **CleverCloud/clever-tools** | Official Clever CLI: `clever deploy`, logs, add-ons (Node 22+) | Apache-2.0 | 2026-09-28 · 5.0.2 (2026-09-16) | JavaScript | The `loams deploy` / `loams functions` UX | **Reference only** | Loams's CLI is the Rust `loams` binary; clever-tools talks only to Clever's API |
| CleverCloud/clever-client.js | JS client for Clever's API | Apache-2.0 | 2026-09-28 · v12.6.5 | TS | — | Reject | Clever-API specific |
| **CleverCloud/clever-components** | Web Components used in Clever's consoles, with Storybook | Apache-2.0 | 2026-09-23 · 26.5.0 | JavaScript | Loams console | **Reference only** | The console has its own stack (§19, `loam-cloud`). Useful for patterns (log viewer, metrics widgets) |
| CleverCloud/clevercloud-sdk-go, -python, clevercloud-client-go | API SDKs | Apache-2.0 | 2026-09 | Go/Python | — | Reject | Clever-API specific |
| CleverCloud/mcp-simple-server | MCP server over Clever's API | Apache-2.0 text (GitHub: NOASSERTION) | 2026-04-14 · none | TS | — | Reject | Clever-API specific |
| CleverCloud/oauth10a-rust | OAuth 1.0a (Clever's API auth) | MIT | 2025-05-28 · v3.0.0 | Rust | — | Reject | Only pulled in by `clevercloud-sdk`, transitively |

### 2.3 Edge and proxy (Sōzu family)

| Project | What it does | License | Last commit · release | Lang | Fit | Verdict | Rationale |
|---|---|---|---|---|---|---|---|
| **sozu-proxy/sozu** | Hot-reconfigurable reverse proxy: HTTP/1.1 (kawa), HTTP/2 mux, TLS (rustls, aws-lc-rs, FIPS), TCP, UDP, per-IP limits, zero-downtime upgrade | **AGPL-3.0** (`LICENSE`; `sozu` and `sozu-lib` crates AGPL-3.0; **`sozu-command-lib` LGPL-3.0**) | 2026-09-29 · 2.2.1 (2026-08-28) | Rust | Edge / ingress | **Reject** (§3). If ever used, only unmodified as a separate process, with the AGPL risk flagged | No HTTP/3 in its README or docs; no `GRPCRoute`, header or query matching or weighted splits (sozu-gateway `docs/features.md` lists these as "not supported by Sōzu"). Envoy covers all of them under Apache-2.0 |
| CleverCloud/sozu-gateway | Kubernetes Ingress + Gateway API controller driving Sōzu over its command socket; compiles to an IR and applies deltas | Apache-2.0, but **statically links `sozu-command-lib` (LGPL-3.0)** | 2026-09-23 · v0.5.0 | Rust | K8s edge controller | Reject (with Sōzu). Reference for its IR-and-delta design | Tied to Sōzu. Pod-IP backends from EndpointSlices and idempotent global reconcile are good ideas to copy by design, not code |
| CleverCloud/kawa | Zero-copy HTTP/1 and HTTP/2 representation used by Sōzu | Apache-2.0 | 2026-09-28 · v0.7.2 | Rust | `loams-gateway` parsing | Reference only | The gateway uses hyper, the ecosystem default; kawa is optimized for Sōzu's buffer model |
| CleverCloud/sozu-client, sozu-prometheus-connector | Async client for Sōzu's command socket; metrics exporter | Apache-2.0 (sozu-client links LGPL `sozu-command-lib`) | 2026-07-16 · v0.5.0 / v0.4.0 | Rust | — | Reject | Only useful with Sōzu |
| CleverCloud/sozu-pulsar-connector, sozu-pki-connector | Pulsar → Sōzu config; certificates from a directory | Apache-2.0 | 2023–2024 · tags v0.1.1 | Rust | — | Reject | Stale; Sōzu-only |
| sozu-proxy/poule, circular | Growable object pool; nom stream buffer | MIT | 2026-05 / 2023-07 | Rust | Buffer pools | Reference only | Loams has `bytes` and its own pools |

### 2.4 Authorization (Biscuit)

| Project | What it does | License | Last commit · release | Lang | Fit | Verdict | Rationale |
|---|---|---|---|---|---|---|---|
| **eclipse-biscuit/biscuit** | The Biscuit specification: public-key-verified, offline-attenuable capability tokens with Datalog checks | Apache-2.0 | 2025-10-21 · v3.3 (2024-12-17) | spec | Sandbox token format | **Adopt (spec)** | Moved from Clever Cloud to the Eclipse Foundation; stable v3 format |
| **eclipse-biscuit/biscuit-rust** (`biscuit-auth`) | Reference Rust implementation | **Apache-2.0**: no root `LICENSE`; `biscuit-auth/LICENSE` and `license = "Apache-2.0"` in `biscuit-auth/Cargo.toml` | 2026-08-17 · biscuit-auth 6.0.0 (2025-07-16) | Rust | Minting in the node supervisor; verifying in `loams-dapr` and the gateway | **Adopt, linked** (D188) | Apache-2.0, maintained, Rust. §4 has the comparison. `cargo deny` check at adoption (Q-RT-10) |
| eclipse-biscuit/biscuit-wasm, biscuit-python, biscuit-java, biscuit-go, biscuit-swift | Bindings | Apache-2.0 | 2026-07 to 2026-09 | various | Letting functions attenuate their own token before calling a sub-function | Reference; adopt later if functions need it | F1 attenuates in the supervisor only |
| eclipse-biscuit/biscuit-cli | Generate and inspect tokens | **BSD-3-Clause** | 2026-06-19 · 0.6.0 | Rust | Developer and debugging tool | **Adopt as a tool** (unmodified) | Permissive, not linked |
| CleverCloud/biscuit-pulsar | Biscuit auth plugins for Apache Pulsar | Apache-2.0 | 2026-09-19 · 4.0.1 | Java | — | Reject | Loams has no Pulsar |
| CleverCloud/biscuit-wasm-shim, biscuit-wasm-go | Experimental wasm shims | **no LICENSE file** | 2025-10 · none | Rust/Go | — | Reject | Unlicensed |

### 2.5 Messaging, metrics, logs and storage tooling

| Project | What it does | License | Last commit · release | Lang | Fit | Verdict | Rationale |
|---|---|---|---|---|---|---|---|
| CleverCloud/magnetar | Sans-io Apache Pulsar client | Apache-2.0 | 2026-09-21 · v1.7.2 | Rust | — | Reject | Loams's streams are native and Kafka-compatible (D72, D74); no Pulsar |
| CleverCloud/pulsar4s, logstash-output-pulsar, pulsar-addon-migration-tool, node-pulsar-rust-backed, warp10-ext-pulsarwriter | Pulsar clients and tools | Apache-2.0 / MIT | mixed, most stale | Scala/Ruby/JS/Java | — | Reject | No Pulsar |
| CleverCloud/fdbexporter | FoundationDB → Prometheus exporter | Apache-2.0 | 2026-09-24 · v2.5.0 | Rust | — | Reject | FoundationDB was dropped (D71) |
| CleverCloud/warp10.rs, telegraf-output-warp10, clevercloud-warp10-datasource, warp10-* | Warp 10 time-series clients and plugins | BSD-3-Clause / Apache-2.0 | mixed | Rust/Go/TS/Java | — | Reject | Loams's telemetry is OTLP (D73) and Iceberg |
| CleverCloud/cellar-migration | Copies an S3-compatible store into Cellar | Apache-2.0 | 2025-09-23 · v2.1.0 (2023-12-13) | Rust | Migrating a tenant's bucket to Cellar | Reference only | One direction, no release since 2023; `rclone` or Loams's own bulk import (§21 §7) covers this |
| CleverCloud/testcontainers-ceph | Ceph testcontainer (Cellar is Ceph-based **(verify)**) | MIT | 2026-05-10 | Java | Cellar-compatibility CI | Reject | CI uses RustFS (D61); a Rust Ceph harness would be written separately if Q-RT-9 needs it |
| CleverCloud/stream-dns | DNS server updated from Kafka | MIT | 2020-01-22 | Go | — | Reject | Stale |
| CleverCloud/nlrs | Minimal Netlink requests | MIT | 2026-08-20 · v0.2.0 | Rust | Supervisor network namespaces (veth, routes, egress allowlist) | Reference; candidate linked dependency in F1 against `rtnetlink` | Small and permissive; the choice is made on API fit |
| CleverCloud/simple-vmm | Example VMM on rust-vmm | MIT | 2021-03-01 · none | Rust | T3 (later) | Reference only | Stale teaching example |
| CleverCloud/CleverCloud-exheres | Exherbo packages for Clever's images | **no LICENSE file** | 2026-09-28 | — | — | Reject | Unlicensed; distribution-specific |
| CleverCloud/rust-guidelines, guidelines | Clever's Rust guidelines | no license | 2018 | — | — | Reference only | Loams has its own lints |
| ~150 `CleverCloud/*-example` repos | Deployment samples per framework (Astro, SvelteKit, Bun, FrankenPHP, Django, n8n, GlitchTip, …) | mostly **no license** | 2026-03 to 2026-09 | various | Framework support matrix (§24 §4.3) | Reference only | Read to learn build commands and ports; nothing copied |

**Not open source.** Clever Cloud's hypervisor and VM images, its deployer, its log and metrics pipeline and the Materia KV engine are not published. Only clients and demos for Materia exist (`mkv-*`, MIT). Nothing from those systems can be adopted.

## 3. The edge: Sōzu vs Envoy vs Pingora or River

| | **Envoy** (planned edge) | **Sōzu** | **Pingora** | **River** |
|---|---|---|---|---|
| License | Apache-2.0 | **AGPL-3.0** (command lib LGPL-3.0) | Apache-2.0 | Apache-2.0 / MIT |
| Form | Proxy binary | Proxy binary | **Library** (framework) | Proxy binary on Pingora |
| Activity | 2026-09-29 · v1.39.1 | 2026-09-29 · 2.2.1 | 2026-09-11 · 0.9.0 | **2024-09-06 · v0.5.0 (stalled)** |
| HTTP/3 (QUIC) | Yes **(verify maturity label)** | Not found in README or docs | Not in Pingora 0.9 **(verify)** | No |
| gRPC routing / gRPC-Web | Yes (`GRPCRoute` via Envoy Gateway **(verify)**, gRPC-Web filter) | No `GRPCRoute` | You build it | No |
| Header/query match, weighted split, mirroring | Yes | **No** | You build it | Partial |
| WebSocket / SSE | Yes | WebSocket upgrade **(verify)**; SSE passes as HTTP | Yes | Yes |
| Hot reconfiguration | xDS | Command socket, no restart | In code | Config reload |
| Rate limits, ext_authz | Built-in filters and ext_authz (useful for Biscuit/OIDC checks at the edge) | Per-IP connection caps | You build it | Basic |
| Kubernetes | Envoy Gateway (Gateway API) **(verify version)** | sozu-gateway (Apache-2.0, v0.5.0) | — | — |

**Verdict.** Envoy stays the edge (D184). Sōzu is well engineered and fast, and its hot reconfiguration is appealing. But D176 puts HTTP/3 and gRPC in phase 1, and Sōzu has neither HTTP/3 nor gRPC routes. It would also bring an AGPL-3.0 binary into Loams's default install. As an unmodified separate process that is allowed by the rule, but every distributor of the self-hosted bundle would then carry the AGPL source-offer duty for it. **Pingora** is the right tool if Loams ever needs a Rust L7 component of its own (for example, a gateway that terminates HTTP/3 next to the supervisor). **River** is not an option while it is stalled.

## 4. Biscuit vs OpenFGA + OIDC vs the sandbox tokens (D182, D188)

These mechanisms answer different questions, so the comparison is about which job each one does.

| | **OpenFGA + OIDC** (D66, D67, §19) | **JWT access tokens** (§19 §5.3) | **Biscuit** |
|---|---|---|---|
| Answers | *Who may do what* (the relationship graph); OIDC says *who you are* | *This bearer is principal X with scopes S until T* | *This bearer holds these capabilities, narrowed by every holder along the way* |
| Verification | A network check (OpenFGA) | Local, Ed25519 signature | Local, Ed25519 (or P-256) signature chain |
| Attenuation (narrowing) | Write new tuples | Only by asking the token endpoint again (vending, §19 §5.2 item 3) | **Offline**: any holder appends a block that can only restrict |
| Delegation chains | Via relations | The `act` claim, set by the issuer | Native: each block is a hop; third-party blocks carry attestations from another key |
| Revocation | Immediate (delete tuples) | Short TTL plus `jti` deny list | Short TTL plus revocation ids checked against a list (the spec leaves revocation to external state) |
| Policy language | OpenFGA DSL | None (claims) | Datalog checks inside the token |
| Size | — | Hundreds of bytes | Hundreds of bytes to a few KB, growing per block **(estimate)** |
| Ecosystem | CNCF, OIDC everywhere | Universal | Smaller; Rust, Go, Java, Python, Wasm, Swift implementations |

**Recommendation.**

1. **OpenFGA stays the authority** for every decision on Loams data (the `Authorizer`, D66). Biscuit Datalog is not allowed to become a second policy store. Tokens carry only *narrowing* facts: tenant, namespace, function, invocation, operations and expiry.
2. **JWTs stay for external clients** (§19 §5.3). They are universal, and MCP clients and OIDC federation expect them.
3. **Biscuit is used inside the runtime** (D188), where offline attenuation pays off:
   - The control plane issues each node supervisor a Biscuit whose authority allows minting for the tenants scheduled on that node, with a short TTL and regular renewal.
   - For each invocation, the supervisor appends a block that narrows the token to one tenant, function, version and invocation id and an expiry of at most the invocation's wall limit. It adds a further block per call with the allowed operations (`state:get`, `pubsub:publish`, `retrieval:query`, …). No round trip is needed per invocation.
   - `loams-dapr` and the gateway verify the chain locally with the control plane's public key, take the namespace from the token, then ask OpenFGA.
   - A function that calls a sub-function passes an attenuated copy. This is §19 §5.2's "vended tokens can only attenuate", enforced cryptographically rather than by the token endpoint.
   - Suspension of an agent (§19 §5.4) adds its revocation ids to a TiKV-backed deny list that verifiers cache.
4. **Risk:** two token formats. The gateway accepts JWTs from outside and Biscuits from sandboxes, and never the reverse (a Biscuit is not valid on public routes). Q-RT-10 asks whether §19's vending flow should also switch to Biscuit, so that there is one attenuation mechanism.

## 5. The forked operator and the `ObjectStoreProvider` trait (D178, D185)

**Fork plan.** Fork `CleverCloud/clever-kubernetes-operator` to `dina-kar/loam-operator` and keep the MIT `LICENSE` with Clever Cloud's copyright plus a `NOTICE`. Rename the API group from `api.clever-cloud.com` to Loams's (placeholder `loams.<domain>/v1alpha1`, Q-RT-8). Keep `crates/core`, `svc/k8s`, `svc/http` and the Helm chart. Delete `svc/clevercloud` and the add-on CRDs. Add:

| CRD | Reconciles | Notes |
|---|---|---|
| `Loams` | A Loams cluster: roles (`meta`, `log`, `query`, `worker`, `gateway`) as StatefulSets or Deployments, the metastore connection (TiKV PD endpoints, D179), the bucket (via an `ObjectStore` reference), the Resonate endpoint | Replaces §10's planned `loams-operator` (M2) |
| `ObjectStore` | A bucket and credentials from a provider | Status carries the endpoint, bucket and a Secret reference |
| `Function` | A function version: contract, tier, bundle digest, routes, limits | Written by `loams deploy`; the operator programs the gateway and warms nodes |
| `RuntimePool` | Per-node tier capacity (T0, T1, T2) and RuntimeClass | Maps to the supervisor DaemonSet's config |

```rust
/// Implemented by `rustfs` (default), `s3` (AWS, R2, GCS interop, any S3) and `cellar` (feature `provider-cellar`).
#[async_trait::async_trait]
pub trait ObjectStoreProvider: Send + Sync {
    fn kind(&self) -> &'static str;
    /// What the backend supports; Loams refuses a provider that lacks conditional writes (D1's WAL needs them).
    fn capabilities(&self) -> ProviderCapabilities; // if_none_match_put, if_match_put, max_object, ...
    async fn ensure_bucket(&self, spec: &BucketSpec) -> Result<BucketRef, ProviderError>;
    async fn issue_credentials(&self, bucket: &BucketRef, scope: &CredentialScope) -> Result<S3Credentials, ProviderError>;
    async fn revoke_credentials(&self, bucket: &BucketRef, key_id: &str) -> Result<(), ProviderError>;
    /// The `object_store` configuration Loams's roles receive (endpoint, region, path-style, TLS).
    fn store_config(&self, bucket: &BucketRef) -> ObjectStoreConfig;
}
```

- **`rustfs`**: RustFS is deployed by its upstream Helm chart (`rustfs/rustfs`, `helm/rustfs`, Apache-2.0, 1.0.0 on 2026-09-16), and the provider creates buckets and access keys through RustFS's S3 and admin APIs **(verify admin API shape)**.
- **`s3`**: static credentials or IRSA / workload identity; no bucket creation unless allowed.
- **`cellar`**: buckets and keys through `clevercloud-sdk`; data over S3. Whether Cellar honours `If-None-Match` and `If-Match` on `PUT` is unverified, and it decides whether Cellar can hold Loams's WAL or only static assets (Q-RT-9).

## 6. GitOps layout (D186)

### 6.1 Argo CD or Flux

Both are Apache-2.0 and CNCF-graduated. The latest releases are Argo CD v3.5.3 (2026-09-14; v3.6.0-rc1 on 2026-09-16) and Flux v2.9.5 (2026-08-31). **Argo CD is chosen**, for these reasons:

1. **The draft and this design are written in sync waves.** Argo CD's `argocd.argoproj.io/sync-wave` orders resources inside an Application and, in an app-of-apps, orders the child Applications. Flux expresses the same thing with `dependsOn` between Kustomizations and HelmReleases. That works, but it is a different model from the one the owner wrote.
2. **Custom health checks for CRDs.** Waves only wait for *healthy* resources. Argo CD's Lua `resource.customizations.health` lets the TiDB Operator v2 groups (`PDGroup`, `TiKVGroup`), RustFS, `Loams` and `Function` report real readiness.
3. **Hub and spoke for Loams cloud and BYOC.** One Argo CD with ApplicationSets (cluster generator) can manage many clusters, and it has a UI for operators.

**Costs, stated plainly.** Argo CD is heavier than Flux: application controller, repo server, Redis, server and optional Dex **(verify RSS on k3d)**. That matters on the dev machine and in small BYOC clusters. And since Argo CD 1.8, an `Application`'s own health is **not** assessed by default. Without adding the documented `argoproj.io/Application` health customization, a parent's waves do not wait for child Applications to become healthy **(verify against the v3.5 docs)**. The umbrella chart has no Argo-specific templates, so Flux can consume it unchanged if a BYOC customer requires Flux (Q-RT-11).

### 6.2 Repository layout

```
deploy/
  helm/loams-stack/                 # umbrella chart (Chart.yaml dependencies, all toggleable)
    charts/                        # rustfs (upstream), tidb-operator (upstream), loams-operator (fork),
                                   # resonate, loams-dapr, loams-gateway, envoy-gateway, dapr (long tail, off by default),
                                   # runtime (supervisor DaemonSet, RuntimeClasses)
    values.yaml
  gitops/
    bootstrap/argocd/              # Argo CD install + argocd-cm health customizations (Application, PDGroup, TiKVGroup, Loams, Function)
    root.yaml                      # the root Application (app-of-apps)
    apps/                          # one Application per wave, each rendering loams-stack with one component enabled
    envs/{dev-k3d,selfhosted,clever-cke,cloud}/values.yaml
  infra/clever-cke/                # Terraform (terraform-provider-clevercloud) — outside Argo CD
```

### 6.3 Sync waves

| Wave | Application | Contents | Health gate | Clever piece |
|---|---|---|---|---|
| **-2** | `crds` | CRDs: TiDB Operator v2 (`core.pingcap.com`), `loams-operator`, Gateway API | CRDs established | `loams-operator` CRDs (fork) |
| **-1** | `operators` | TiDB Operator v2; `loams-operator`; gVisor node setup (a DaemonSet installing `runsc` and the containerd shim, plus the `gvisor` RuntimeClass); Karpenter on CKE | Deployments available | **fork of clever-kubernetes-operator**; **karpenter-provider-clever-cloud** (CKE only) |
| **0** | `object-store` | RustFS (upstream chart) and `ObjectStore` resources for the system buckets | RustFS ready; `ObjectStore` `Ready` | `cellar` provider via **clevercloud-sdk-rust** when `envs/clever-cke` selects Cellar instead of RustFS |
| **1** | `tikv` | `Cluster` + `PDGroup` + `TiKVGroup` (no `TiDBGroup`), TiKV with `storage.api-version = 2` and `enable-ttl = true` (D122) | Lua health on both groups | — |
| **2** | `durable` | Resonate server Deployment (the fork, TiKV store) + HPA | Deployment available | — |
| **3** | `edge` | `loams-dapr` (with **biscuit-auth**), `loams-gateway`, Envoy (Envoy Gateway), optional shared `daprd` for the long tail | Ready; Gateway `Programmed` | **biscuit-auth** (Eclipse, started at Clever) |
| **4** | `loams` | `Loams` resource → the engine roles | `Loams` `Ready` | — |
| **5** | `runtime` | Supervisor DaemonSet, `RuntimePool`s, T0 workerd and T1 wasmtime, T2 when F2 lands | DaemonSet rolled out | — |

`envs/dev-k3d` sets one replica everywhere and small TiKV memory (§20's playground peaked at 3.2 GB RSS for PD, TiKV and TiDB). It can use RustFS in single-node mode.

### 6.4 TiDB Operator v2 check (D179)

- **Status.** `main` is v2 ("The `main` branch is now the default branch and hosts the v2 version", README). **v2.0.0 was released on 2025-12-18 and v2.0.1 on 2026-03-26** (not pre-releases). v2.1.0-beta.6 and v2.2.0-alpha.11 (2026-09-16) are pre-releases. v1.6.6 (2026-08-12, `release-1.x`) is v1's latest. The v2 CRDs are still `core.pingcap.com/v1alpha1` even in the GA line, which is an API-stability risk to pin against.
- **PD + TiKV only.** v2 splits a cluster into a `Cluster` and one resource per component group. `examples/pdms` deploys `Cluster`, `PDGroup`, `TSOGroup`, `SchedulingGroup` and `TiKVGroup` with **no `TiDBGroup`**, and `examples/basic` puts TiDB in its own file (`03-tidb.yaml`). So a PD-and-TiKV cluster is a supported shape. In v1, `TidbClusterSpec.TiDB` is an optional pointer (`json:"tidb,omitempty"`, `release-1.6` `types.go`), but whether v1's controllers run cleanly without TiDB was not checked **(verify)**.
- **Still to verify in F0:** that `storage.api-version = 2` passes through `TiKVGroup.spec.template.spec.config`; that PD keyspace pre-allocation (D122) can be set in `PDGroup` config; and Q33's question of whether `keyspace` is accepted for classic clusters.

## 7. Risks

| Risk | Mitigation |
|---|---|
| The forked operator's reusable core is small, so the fork buys less than it seems | Accepted: it saves the skeleton and the chart hardening; track upstream `crates/core` changes by hand |
| Argo CD's footprint on small clusters | Measure on k3d (Q-RT-11); the chart stays Flux-consumable |
| TiDB Operator v2 CRDs are `v1alpha1` | Pin the operator version per Loams release; conversion tested in upgrade CI |
| RustFS 1.0 is new | Pin; S3 provider fallback (D178) |
| LGPL `sozu-command-lib` pulled in by accident (through sozu-client or sozu-gateway) | `cargo deny` license policy lists LGPL-3.0 as needing review |
| Biscuit adds a second token format | §4 item 4; Q-RT-10 |

## 8. Open questions

| # | Question | Owner | Needed by |
|---|---|---|---|
| Q-RT-8 | The operator's API group and domain (`loams.<domain>`), and whether the fork lives at `dina-kar/loam-operator` or in the engine workspace | Founder | Operator fork |
| Q-RT-9 | Does Cellar honour `If-None-Match` / `If-Match` on `PUT`, so that it can hold the WAL, or is it only for static assets and bundles | Eng | Cellar provider |
| Q-RT-10 | Biscuit for §19's vending flow too (one attenuation mechanism), or Biscuit only inside the runtime; `cargo deny` result for `biscuit-auth` 6.0 | Founder | F1 plan |
| Q-RT-11 | Argo CD's RSS on k3d and in the smallest BYOC profile; whether BYOC-local-meta (§10 §1) needs a Flux option | Eng | F1 |
| Q-RT-12 | Envoy Gateway or plain Envoy with a static xDS from `loams-gateway` | Eng | F1 plan |
| Q-RT-13 | Is Clever Kubernetes Engine a first-class target (CI on CKE, the `clever-cke` env), or only documented | Founder | Before cloud beta |
| Q-RT-14 | Should Loams contribute back to `clever-kubernetes-operator`'s `crates/core` (for example, a generic `Controller` registry release on crates.io) rather than carry a diverging fork | Founder | After the fork |

## 9. Sources

Read on 2026-09-29 through the GitHub API (`repos/<owner>/<repo>`, `/license`, `/commits`, `/releases`) and the files named.

- CleverCloud org listing (all public, non-archived, non-fork repositories); sozu-proxy, eclipse-biscuit and biscuit-auth org listings.
- `CleverCloud/clever-kubernetes-operator`: `LICENSE` (MIT), `README.md`, `Cargo.toml` (v0.8.0, workspace `crates/core`, `crates/operator`), `crates/operator/Cargo.toml` (kube 3.1, `clevercloud-sdk` 1.0.1), `crates/core/src/{controller,registry,strategy}.rs`, `crates/operator/src/svc/crd/postgresql.rs` (`group = "api.clever-cloud.com"`), `deployments/kubernetes/helm/`.
- `sozu-proxy/sozu`: `LICENSE` (AGPL-3.0), `README.md`, `lib/Cargo.toml` (AGPL-3.0), `command/Cargo.toml` (LGPL-3.0), `bin/Cargo.toml` (AGPL-3.0), `doc/`. `CleverCloud/sozu-gateway`: `README.md`, `docs/features.md`, `Cargo.toml` (`sozu-command-lib = "=2.2.1"`). `CleverCloud/sozu-client`: `Cargo.toml`.
- `eclipse-biscuit/biscuit-rust`: `README.md`, `biscuit-auth/Cargo.toml` (6.0.0, Apache-2.0), `biscuit-auth/LICENSE`. `eclipse-biscuit/biscuit` `LICENSE`. `eclipse-biscuit/biscuit-cli` `LICENSE` (BSD-3-Clause).
- `CleverCloud/karpenter-provider-clever-cloud` `README.md`; `CleverCloud/clever-tools` `README.md`; `CleverCloud/clever-components` `README.md`; `LICENSE` files of every repository in §2.
- `pando85/kaniop` `LICENSE.md` (AGPL-3.0).
- `pingcap/tidb-operator`: `README.md`, `examples/{basic,pdms}/`, `api/core/v1alpha1/`, releases and tags (v2.0.0, v2.0.1, v2.1.0-beta.6, v2.2.0-alpha.11, v1.6.6), `release-1.6:pkg/apis/pingcap/v1alpha1/types.go`.
- `rustfs/rustfs`: `LICENSE` (Apache-2.0), `helm/`, release 1.0.0. `cloudflare/pingora` (Apache-2.0, 0.9.0), `memorysafety/river` (Apache-2.0/MIT, v0.5.0, last commit 2024-09-06), `envoyproxy/envoy` (Apache-2.0, v1.39.1), `argoproj/argo-cd` (Apache-2.0, v3.5.3), `fluxcd/flux2` (Apache-2.0, v2.9.5).
- Loams: §10 §1, §18, §19 §5, §20 §9 and §15, §21, §22 §13b; D1, D61, D66, D67, D71, D72, D73, D74, D122, D-SC-16; Q33.
