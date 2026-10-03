# 24 — Loams Functions: a CPU-Time Serverless Runtime

Status: **Proposed** · 2026-09-29; amended the same day by two owner decisions (D189: secrets through Dapr; D190: billing in `loam-platform`). Source: the owner's draft "CPU-Time Serverless Runtime: Consolidated Plan" v2 (2026-09-28). On 2026-09-29 the owner approved ten changes to that draft in conversation. This document is built on those changes, and they override the draft wherever the two differ. They are decisions **D170–D179**, marked *approved in conversation 2026-09-29*. **D180–D188** are this document's own proposals and the parts of the draft that the changes left untouched; the owner has not yet ruled on them. **D189** and **D190** are owner decisions made on 2026-09-29, after the draft. The deployment and the Clever Cloud evaluation are in the companion document, [§25](25-clever-cloud-stack.md). **Amended 2026-10-01** by the owner's "Loams Serverless Runtime — Consolidated Plan" v1 (2026-09-30), folded in by [§34](34-protocol-gateway-and-standards.md): the `Runner` trait (D375) and usage from external runners (D376), §16 below; both proposed. **Amended 2026-10-02** by [§38](38-knative-authentik-gitops.md) (D440, D441, D444, proposed): the trait now lives in §16, `KnativeRunner` runs the `http-port` contract on Knative Serving, no meter runs in this repository, and the Cloudflare runner moved to `loam-platform`.

Markers: **(verify)** means the claim was not checked against a primary source, or was checked only against a secondary one; the task that depends on it resolves it first. **(estimate)** means computed, not measured. **(draft)** means the figure comes from the owner's draft and was not re-checked here. Every license, release and pricing claim was read on 2026-09-29 from the source named in §15.

Numbering: `main` ends at D147 and Q44. Doc 23 (Neon and WeSQL) is being written at the same time on another branch and will take numbers after D147. This document starts at **D170**, a gap of about 20, and names its questions **Q-RT-n** so that nothing collides.

---

## 1. Summary

| # | Decision | Status |
|---|---|---|
| D170 | **Positioning.** The product is for agent and backend functions that spend most of their time waiting, running next to Loams's data: retrieval, Loams Live on TiKV (§20), durable workflows (§21) and Neon branches (§23, in progress on another branch). Frontends get static hosting plus the workerd framework preset, not a Vercel clone | Proposed · approved in conversation 2026-09-29 |
| D171 | **JavaScript runs on workerd** (Cloudflare's runtime, Apache-2.0), replacing rquickjs plus homemade polyfills. It reuses the existing Cloudflare Workers presets (Hono, Nitro, Astro, SvelteKit, React Router/Remix, OpenNext). workerd's README says it is not a hardened sandbox, so there is **one workerd process per tenant, under gVisor or seccomp** | Proposed · approved in conversation 2026-09-29 |
| D172 | **T1 is wasmtime**, with no WasmEdge benchmark. Before building a host, evaluate **Spin** and **wasmCloud** (both Apache-2.0) as the T1 host | Proposed · approved in conversation 2026-09-29 |
| D173 | **Suspend-on-await is scoped.** Short waits keep the instance resident (zero CPU, a few MB). Long waits go through Resonate. Only code written with the Resonate SDK (deterministic and replayable) is evicted and resumed | Proposed · approved in conversation 2026-09-29 |
| D174 | **T3 (Firecracker snapshots) is deferred.** Kata does not restore Firecracker snapshots (its Firecracker driver's pause, save and resume are no-ops). That feature needs firecracker-containerd or orchestration of our own | Proposed · approved in conversation 2026-09-29 |
| D175 | **Metering sources:** Wasm fuel or epochs for T1, per-process CPU time for workerd, cgroup `cpu.stat` for gVisor. **eBPF comes last, as a cross-check only.** Exposed only through open hooks (D190) | Proposed · approved in conversation 2026-09-29 |
| D176 | **Phase-1 protocols:** HTTP/1.1, HTTP/2 and HTTP/3, gRPC, WebSocket and SSE. Kafka arrives with Loams's Kafka gateway (M5, D74). NATS, MQTT and AMQP go through the shared Dapr runtime | Proposed · approved in conversation 2026-09-29 |
| D177 | **Revised phase 1 (F1):** the manifest; tenant identity in the Rust Dapr server; workerd and wasmtime with Hono; Resonate suspend and resume; fuel or per-process CPU metering exposed through open hooks (D190; the draft said "written to the WAL"). **F2** adds gVisor. **Later:** Firecracker, eBPF and more protocols | Proposed · approved in conversation 2026-09-29 |
| D178 | **RustFS is the default object store** for self-hosted and GitOps deployments. Cellar and every other S3 store are **providers behind an `ObjectStoreProvider` trait** | Proposed · approved in conversation 2026-09-29 |
| D179 | **The metastore is TiKV**, not "raft or postgres". PD and TiKV are deployed by **TiDB Operator v2**, which supports a cluster of PD and TiKV only; its upstream example `examples/pdms` has no TiDB | Proposed · approved in conversation 2026-09-29 |
| D180 | **Billing is CPU time, not wall time**, plus requests. The draft's meter events in the WAL move to `loam-platform` with the rest of billing (D190) | Proposed (owner's draft); amended by D190 |
| D181 | **Three app contracts:** `static`, `fetch` (a Hono-style `fetch(Request) → Response` handler) and `http-port` (a server that listens on `$PORT`). The manifest picks a contract, and the contract picks a tier (§4) | Proposed (owner's draft, narrowed by D171/D174) |
| D182 | **Tenant identity is checked in the Rust Dapr server**, from mTLS (a SPIFFE ID) or a sandbox token. The sandbox token is a **Biscuit** that the supervisor mints and attenuates for each invocation (§25 §4) | Proposed |
| D183 | **Go `daprd` is used only for the long tail** of bindings and pub/sub brokers (NATS, MQTT, AMQP and the like), as one shared runtime per cluster, not a sidecar per function | Proposed (owner's draft) |
| D184 | **The gateway is Rust, and its envelope is CloudEvents 1.0.** Envoy is the edge in front of it. Sōzu is rejected as the edge (§25 §3) | Proposed |
| D185 | **The operator is a fork of `CleverCloud/clever-kubernetes-operator`** (MIT, Rust, kube-rs), with the API group renamed. It adds a `Loams` CRD and a `Function` CRD, and holds the `ObjectStoreProvider` trait. kaniop (AGPL-3.0) is a design reference only (§25 §2, §25 §5) | Proposed (owner's draft) |
| D186 | **GitOps with Argo CD**, an umbrella chart and sync waves in the order RustFS → PD/TiKV → Resonate → Rust Dapr/RPC server and gateway → Loams → runtime tiers (§25 §6) | Proposed |
| D187 | **Clever Cloud adoption verdicts** as listed in §25 §2 | Proposed |
| D188 | **Biscuit carries sandbox tokens and delegation inside the runtime.** The JWT access tokens of §19 §5.3 stay as they are for external clients, and OpenFGA stays the authority (§25 §4) | Proposed |
| D189 | **Secrets through Dapr's secrets building block** for every store in `components-contrib` (AWS Secrets Manager and Parameter Store, Azure Key Vault, GCP Secret Manager, Vault/OpenBao, Kubernetes, …): the shared Go `daprd` off the hot path, one component per tenant and store with the tenant's own credentials, scoping enforced by `loams-dapr`, a TTL cache, and delivery by loopback (workerd, gVisor) or host import (wasmtime) (§5.1) | Proposed · owner decision 2026-09-29 |
| D190 | **Billing and metering move to the private `loam-platform` repository**; this repository exposes only open hooks: Prometheus metrics, cgroup labels, Envoy access logs, OTLP spans (§7) | Proposed · owner decision 2026-09-29 |

## 2. Goals and non-goals

### 2.1 Goals

- **Waiting costs no CPU.** An agent that spends 10 ms computing and 20 s waiting on a model is billed for 10 ms of CPU.
- **Functions run next to Loams's data.** A function reaches retrieval, Live, durable promises and streams over a loopback or node-local gRPC surface, not the public internet.
- **Existing code runs unchanged.** Hono apps and the output of the Cloudflare Workers framework adapters run on workerd without a Loams-specific port. Resonate SDK code runs as it does against any Resonate server.
- **One deployment path.** The same Git repository and charts deploy a laptop k3d cluster, a self-hosted cluster on RustFS and Loams cloud (§25 §6).

### 2.2 Non-goals

- **A Vercel or Netlify clone** (D170). There are no preview-deploy workflows, image optimization or edge-network CDN. Frontends get static hosting and the workerd preset.
- **Transparent suspension of arbitrary code** (D173). A Node or Python process waiting on a socket is not snapshotted. Only Resonate SDK code is evicted.
- **Microsecond cold starts from VM snapshots in F1** (D174).
- **A Loams implementation of Cloudflare's product bindings.** Loams provides KV and R2-shaped bindings over its own storage (§4.2); D1, Queues, Analytics Engine and Hyperdrive are out of scope until someone asks.

## 3. Architecture

```
            client ── HTTP/1.1·2·3, gRPC, WS/SSE
                               │
                     ┌─────────▼─────────┐
                     │  Envoy (edge, L7) │  TLS, HTTP/3, rate limits
                     └─────────┬─────────┘
                     ┌─────────▼─────────┐
                     │ loams-gateway (Rust)│  route → (tenant, function, version)
                     │ CloudEvents 1.0   │  admission, quotas, placement
                     └───┬──────────┬────┘
            ┌────────────▼──┐   ┌───▼────────────────┐
            │ node runtime  │   │ node runtime       │  one per node (DaemonSet)
            │ supervisor    │   │ supervisor         │
            │  ├ workerd/tenant (gVisor|seccomp)     │  T0  JS
            │  ├ wasmtime host (in-process, fuel)    │  T1  Wasm components
            │  └ runsc sandbox (Bun/Node/native)     │  T2  F2
            └──────┬────────┘   └─────┬──────────────┘
                   │ loopback gRPC (Dapr API subset + Loams APIs), sandbox token
            ┌──────▼──────────────────────────────────┐
            │ loams-dapr (Rust): tenant identity, secrets, metrics hooks, Dapr API → Loams │
            └──────┬───────────────┬──────────────┬───┘
         Resonate (durable)   Loams (retrieval, streams, Live)   shared daprd (long tail)
                   │               │
                  TiKV (PD+TiKV) ──┴── object store (RustFS by default; S3/Cellar providers)
```

### 3.1 What exists today, and what does not

The draft says that "a Rust gRPC server translating the Dapr API to the internal streams gRPC already exists". The repository shows something narrower:

- **`crates/loams-stream-grpc`** is a native tonic service, `loams.stream.v1.StreamService` with a single `Produce` RPC (`proto/loams/stream/v1/stream.proto`). It is not the Dapr API.
- **`deploy/dapr/edge`** (`loams-trigger-edge`) is a Rust **Dapr app**. It uses the `dapr` 0.19 crate as a client of a Go `daprd` sidecar, receives pub/sub CloudEvents from Kafka, agent topics and webhooks, and calls `StreamService.Produce` through Dapr's gRPC proxy. Its init check refuses to start unless the Dapr Workflow APIs are denied, because Resonate owns durable execution.
- Both exist only as **uncommitted files on the `design-neon-wesql` working tree**, not on `main`.

A Rust server that implements Dapr's own gRPC service (`dapr.proto.runtime.v1.Dapr`), which the draft calls "the Dapr API in Rust", **does not exist yet**. F1 builds it (§6). It is called `loams-dapr` here; the name is open (Q-RT-1).

### 3.2 Components

| Component | Language / source | Role | Tier or phase |
|---|---|---|---|
| Envoy | C++, Apache-2.0, v1.39.1 | Edge: TLS, HTTP/3 (QUIC), L7 rate limits, gRPC-Web | F1 |
| `loams-gateway` | Rust (new) | Routes a request to `(tenant, function, version)`, wraps async events in CloudEvents 1.0, admission control and quotas (§18 §6), picks a node | F1 |
| Runtime supervisor | Rust (new), one per node | Starts and stops tier instances, holds the resident pool, mints sandbox tokens (D182), exposes metering hooks (D175, D190), evicts and resumes SDK code (D173) | F1 |
| workerd | C++, Apache-2.0, `v1.20260929.1` | T0: JavaScript and TypeScript, one process per tenant | F1 |
| wasmtime host | Rust, Apache-2.0, wasmtime v49.0.1 (or Spin or wasmCloud as the host, D172) | T1: Wasm components (WASI 0.2 `wasi:http`) | F1 |
| gVisor (`runsc`) | Go, Apache-2.0, `release-20260921.0` | T2: Bun, Node, Python and native binaries in the `http-port` contract; also the outer sandbox around workerd | F2 |
| `loams-dapr` | Rust (new) | The Dapr API subset (§5) plus tenant identity, secrets (D189) and metering hooks, over Loams's internal gRPC | F1 |
| Resonate | Rust, Apache-2.0; fork `ostrium-labs/resonate` | Durable promises for long waits (D173); embedded in dev, a Deployment in clusters (§21, §22 §13b item 4) | F1 |
| Shared `daprd` | Go, Apache-2.0, v1.18.4 | Long-tail bindings only (D183) | F1 (optional) |
| Firecracker | Rust, Apache-2.0 | T3, deferred (D174) | Later |

## 4. Tiers and app contracts (D171, D172, D174, D181)

### 4.1 Tiers

| Tier | Runtime | Isolation | Cold start target | Meter (D175) | Phase |
|---|---|---|---|---|---|
| **T0** | workerd, one process per tenant, many isolates per process (one per function version) | gVisor `runsc` around the process, or seccomp-bpf plus a user namespace and cgroup where gVisor is not available | isolate: ms; process: tens of ms **(estimate)** | per-process CPU time (cgroup `cpu.stat` for the tenant's workerd cgroup, cross-checked with `getrusage`) | F1 |
| **T1** | wasmtime (pooling allocator), components per request | Wasm's memory safety plus the host's process and cgroup | µs to ms **(estimate)** | fuel (exact, deterministic) or epoch interruption (cheaper, coarse) | F1 |
| **T2** | Bun, Node, Python or native, in `runsc` | gVisor (`RuntimeClass: gvisor`) | 100s of ms **(estimate)** | cgroup `cpu.stat` | F2 |
| **T3** | Firecracker microVM restored from a snapshot | KVM | deferred | cgroup of the VMM | Later (D174) |

Why workerd rather than rquickjs (D171). With rquickjs, Loams would write and maintain the Web platform itself: `fetch`, streams, `crypto.subtle`, `URL`, `TextEncoder`, the `nodejs_compat` surface and more. The frameworks' Cloudflare adapters already target workerd's API, so it runs their output unchanged. workerd's README is explicit about the cost:

> "WARNING: `workerd` is not a hardened sandbox … `workerd` on its own does not contain suitable defense-in-depth against the possibility of implementation bugs. When using `workerd` to run possibly-malicious code, you must run it inside an appropriate secure sandbox, such as a virtual machine." (cloudflare/workerd `README.md`, read 2026-09-29)

So isolates are never shared across tenants. Each tenant gets its own workerd process inside its own sandbox, and isolates separate one tenant's functions and versions only. workerd's open-source config (`src/workerd/server/workerd.capnp`) has no per-request CPU limit, which is why metering and limits act on the process (D175).

**Loams Live keeps QuickJS for now.** D120 (rquickjs in R1 for Live's deterministic functions) is not changed by this document. Live needs a deterministic `Date.now()` and `Math.random()` and one runtime per `(node, app, deployment)`, which is a different problem. Q35 gains workerd as a candidate.

### 4.2 Bindings on workerd

workerd's `kvNamespace` and `r2Bucket` bindings are turned into **HTTP requests to a named service** (`workerd.capnp`, lines 449–456). Loams implements those services in the node supervisor over Loams storage. KV maps to a Loams Live table or a TiKV keyspace, and R2 maps to the tenant's prefix in the object store. Durable Objects use workerd's `localDisk` storage (`durableObjectStorage`) only in dev; in clusters they are out of scope for F1 (Q-RT-4). A `wrapped` or service binding exposes `loams-dapr` and the Loams APIs to the function as `env.LOAMS`.

### 4.3 App contracts and frameworks

| Framework | Contract | Tier | Notes |
|---|---|---|---|
| Hono | `fetch` | T0 (workerd). T1 only later, once a Wasm JS engine is chosen | The reference contract. "With Hono" in D177 and F1 (§11) means the Hono reference app on T0; T1 in F1 runs Wasm components, not Hono |
| Nitro / Nuxt | `fetch` (Nitro's Cloudflare module preset) | T0 | **(verify)**: which Nitro storage drivers assume Cloudflare KV/R2 |
| Astro (SSR) | `fetch` (`@astrojs/cloudflare`) | T0 | Static-only Astro uses `static` |
| SvelteKit | `fetch` (`adapter-cloudflare`) | T0 | |
| React Router 7 / Remix | `fetch` (Cloudflare template) | T0 | |
| Next.js | `fetch` (OpenNext `@opennextjs/cloudflare`, MIT, 1.20.6) | T0 | OpenNext's incremental cache and tag cache use R2, KV or Durable Objects; Loams must provide those bindings (§4.2) **(verify)** |
| Vite SPA, Docusaurus, Hugo | `static` | object store + edge | No compute |
| Express, Fastify, NestJS | `http-port` | T2 (F2) | Or a `fetch` port where the app allows it |
| Rust (`wasi:http`), Go (TinyGo), Python (componentize-py) | `fetch` as a component | T1 | |
| Django, Rails, Spring | `http-port` | T2 (F2) | |

Cloudflare publishes Workers guides for Astro, React Router, Next.js (OpenNext), Vue, SvelteKit, TanStack Start, Nuxt, Hono and others (developers.cloudflare.com/workers/framework-guides/web-apps). Those adapters build for Cloudflare's hosted runtime through `wrangler`. Loams takes their bundle and writes its own workerd capnp config. The compatibility gaps between hosted Workers and open-source workerd are a spike item (Q-RT-3).

## 5. Dapr surface (D182, D183)

`loams-dapr` implements the subset of `dapr.proto.runtime.v1.Dapr` that maps onto Loams:

| Dapr building block | Loams backing | Phase |
|---|---|---|
| Service invocation | the gateway → another function | F1 |
| Pub/sub (publish, subscribe) | Loams streams (D72) through `StreamService` | F1 |
| State (get, save, query, transactions) | Loams Live / TiKV keyspace (§20) | F1 (get/save), later (query) |
| Bindings (input and output) | native for HTTP, cron and Loams streams; everything else forwarded to the shared `daprd` (D183) | F1 |
| Workflow | **denied**: Resonate owns durable execution (the rule that `deploy/dapr/edge` already enforces) | never |
| Secrets | **Dapr's secrets building block** through the shared `daprd`, with per-tenant secret-store components (§5.1, D189) | F1 |
| Configuration | the tenant's environment in the `ControlStore` (§19) | F1 |
| Actors, distributed lock | out of scope | — |

**Tenant identity (D182).** Every call to `loams-dapr` carries one of two things:

1. an **mTLS client certificate** with a SPIFFE ID (`spiffe://<trust-domain>/tenant/<org>/ns/<ns>/fn/<fn>`), for T2 and system components; or
2. a **sandbox token**, a Biscuit minted by the node supervisor for the invocation. Its authority block names the org, namespace, function, version and invocation id and carries a short expiry. The supervisor attenuates it with each call's allowed operations before passing it into the isolate or component (§25 §4).

`loams-dapr` derives the namespace from the credential, never from a request field, which is the rule §21 §5.2 already sets for the durable dispatcher. It then asks the `Authorizer` (OpenFGA, D66/D67) and emits the metering hooks of §7 (its own call metrics; it records no meter events, D200). **The long tail (D183)** is one shared `daprd` per cluster, running its components with Dapr's own scoping. `loams-dapr` forwards binding calls to it after the tenant check, so no Go sidecar runs per function.

### 5.1 Secrets from every cloud (D189)

The owner decided on 2026-09-29 to adopt **Dapr's secrets building block** as the one path for tenant secrets. Loams then speaks to every store in `dapr/components-contrib` (Apache-2.0, v1.18.5, 2026-09-25) without writing a client for each. The components listed under `secretstores/` in that repository, with the maturity given on docs.dapr.io's supported-stores page, are:

| Component `type` | Store | Status |
|---|---|---|
| `secretstores.aws.secretmanager` | AWS Secrets Manager | Beta |
| `secretstores.aws.parameterstore` | AWS SSM Parameter Store | Alpha |
| `secretstores.azure.keyvault` | Azure Key Vault | Stable |
| `secretstores.gcp.secretmanager` | GCP Secret Manager | Alpha |
| `secretstores.hashicorp.vault` | HashiCorp Vault (and OpenBao, listed separately in the docs) | Stable |
| `secretstores.kubernetes` | Kubernetes Secrets | Stable |
| `secretstores.local.env`, `secretstores.local.file` | environment, file (dev only) | Stable |
| `secretstores.alicloud.parameterstore`, `secretstores.tencentcloud.ssm`, `secretstores.huaweicloud.csms` | Alibaba OOS, Tencent SSM, Huawei CSMS | Alpha |

Every component implements the same Go interface (`secretstores/secret_store.go`): `Init`, `GetSecret`, `BulkGetSecret` and `Features`. The runtime exposes them as the gRPC calls `GetSecret` and `GetBulkSecret` in `dapr/proto/runtime/v1/dapr.proto`, and as `GET /v1.0/secrets/{store}/{key}` and `/bulk` over HTTP. A component is an ordinary Dapr `Component` (`apiVersion: dapr.io/v1alpha1`, `spec.type`, `spec.version`, `spec.metadata`, optional `scopes`), and its own credentials come from a `secretKeyRef`.

**The practical path, while the Rust Dapr server is incomplete.**

1. **The shared Go `daprd` serves secrets, off the hot path** (D183). Secrets are read at instance start and on cache miss, never once per request.
2. **One component per tenant and store.** Components are named `t-<org>-<ns>-<store>`, carry **the tenant's own credentials** (a `secretKeyRef` to a Kubernetes Secret owned by the tenant's resources, or the tenant's Vault role), and are generated by `loams-operator` from the tenant's settings in the `ControlStore`. Loams's own cloud credentials are never put in a tenant's component, and no two tenants share a component.
3. **Loams enforces the scoping, not the caller.** `loams-dapr` takes the tenant from the credential (§5), rewrites the caller's logical store name (`aws`, `vault`, …) to that tenant's component name, checks the key against the tenant's allow list and OpenFGA, and then calls `daprd`'s `GetSecret`. **Bulk reads are never forwarded to the shared `daprd`**: `GetBulkSecret` returns every secret the component can see, which a check on one key does not authorize. `loams-dapr` answers a bulk request itself, with one `GetSecret` per key that is on the tenant's allow list and granted by OpenFGA, and drops every other key. A request that names another tenant's component cannot be expressed. With one shared `daprd` there is one Dapr app id, so Dapr's per-app `secrets.scopes` (`storeName`, `defaultAccess`, `allowedSecrets`, `deniedSecrets`) and component `scopes` cannot tell tenants apart. They are still set as a second, coarse layer: `defaultAccess: deny` for every store the platform itself does not need. Tenants that need Dapr-enforced separation too get a **dedicated `daprd`** whose app id is the tenant, with component `scopes` and `secrets.scopes` naming that id (Q-RT-15).
4. **Adding a tenant's store must not restart the shared runtime.** This depends on `daprd` reloading components when their resources change **(verify: Dapr's component hot-reload feature and its maturity in v1.18)**. The fallback is a small pool of `daprd` replicas rolled one at a time.
5. **Workload identity per tenant is limited.** An AWS IRSA or Azure workload identity belongs to the `daprd` pod, not to a tenant, so tenant components use their own static credentials, Vault auth or cross-account role assumption where the component supports it **(verify per component)**.

**Cache.** `loams-dapr` caches values per `(tenant, store, key)` for a TTL (default 60 s, capped by the tenant's setting) plus a short negative TTL. It drops a tenant's entries when that tenant is suspended or rotates a secret through the console, and zeroes values on eviction. The cache is per node and in memory only, never written to disk, the WAL or logs.

**How each tier receives secrets.** Secrets never enter the bundle or the manifest, only their names.

| Tier | Delivery | Rotation |
|---|---|---|
| T0 workerd | A service binding `env.SECRETS` whose `get(name)` becomes a loopback HTTP call from the isolate to the node supervisor, which forwards it to `loams-dapr` with the invocation's sandbox token. Names declared in the manifest can also be bound as `text` bindings at process start **(verify the binding type in `workerd.capnp`)** | Loopback picks up rotation within the TTL; `text` bindings need an instance restart |
| T1 wasmtime | A host import: the component imports `wasi:config/store` (or a `loams:secrets` WIT interface if `wasi:config` does not fit, **verify its status**), and the host implements it over the same cache | Within the TTL |
| T2 gVisor | Loopback Dapr API inside the sandbox's network namespace: `GET http://127.0.0.1:3500/v1.0/secrets/{store}/{key}` (or gRPC `GetSecret`) to a supervisor bridge that adds the sandbox token. The standard Dapr SDKs work unchanged. Environment injection at start is allowed but discouraged | Loopback within the TTL; environment variables need a restart |

The loopback endpoint of a sandbox is bound to its own network namespace, so one tenant cannot reach another's bridge.

## 6. Suspend-on-await (D173)

| Wait | Examples | What happens | Billed CPU | Memory held |
|---|---|---|---|---|
| **Short** (below the resident threshold, default 30 s, Q-RT-2) | `fetch` to a model, a DB query, `setTimeout` | The instance stays resident and idle; the event loop is parked | none while idle | a few MB (isolate or Wasm instance) |
| **Long, in Resonate SDK code** | `ctx.sleep("1h")`, awaiting a human approval, awaiting another function's promise | The SDK records a durable promise and returns; the supervisor **evicts** the instance; when the promise settles, Resonate hands the task to a worker and the function **replays** from its start, reading recorded results | only the replay's CPU | none |
| **Long, in non-SDK code** | a plain `await` on a slow socket | Stays resident up to the invocation's wall-clock limit (Q-RT-2), then is cancelled | none while idle | a few MB, bounded by the wall limit |

Only SDK code is evicted, because only SDK code is replayable. The Resonate SDKs record each step's result as a durable promise, so a re-invocation skips completed steps. Arbitrary JavaScript that holds a socket or a closure cannot be resumed without a memory snapshot, and T3 (D174) is deferred. The Resonate TypeScript SDK (Apache-2.0) has to run inside workerd. Whether it runs unchanged, or needs a `fetch`-based network and no Node built-ins, is a spike item (Q-RT-5).

**Waking up.** The supervisor is registered with Resonate as the worker for the tenant's function group. Resonate's http-poll transport (D141) delivers the task, and the supervisor starts an instance and re-invokes the function with the task. In clusters Resonate runs as its own Deployment on the TiKV store from the fork (`loam/tikv-dapr`, §22 §13b item 4). In `loams dev` it is embedded (§21).

## 7. Metering hooks (D175, D190)

> **Amended 2026-10-02 ([§41](41-multitenant-byoc-control-plane.md), D541, D548, owner ruling): superseded in part.** The table below is **generic observability**, not metering. The per-invocation host reports and the usage reporter that §27 §3.3 and §3.6 and §16 below described are **moved to `loam-platform`** (doc 06, private, authoritative) because integrity is the security principle: billing-grade producers and validators are not published. What stays here is the metrics, the cgroup layout and pod labels, Envoy access logs, OTLP spans and the `InvocationObserver` extension point (§27 §3.7).

**Billing and metering live in the private `loam-platform` repository** (owner decision, 2026-09-29; D190). That covers the aggregation pipeline, meter events, rating, invoices and pricing. This open repository only **exposes hooks** that `loam-platform`, or a self-hoster's own tooling, reads:

| Hook | Where | What it carries |
|---|---|---|
| **Prometheus metrics** | node supervisor, `loams-dapr`, the gateway | per `(tenant, fn, version, tier)`: invocations, CPU µs, wall ms, resident instances, wasmtime fuel consumed, secret-cache hits |
| **cgroup labels** | the supervisor creates `loams.slice/tenant-<org>.slice/fn-<id>.scope` (T0 per tenant, T2 per sandbox) | anything reading cgroup v2 `cpu.stat` and `memory.*` can attribute usage to a tenant without Loams code |
| **Envoy access logs** | the edge | request count, bytes and status per tenant (`x-loams-tenant` set by the gateway) |
| **OTLP spans** | supervisor and `loams-dapr` | invocation spans with CPU time as an attribute (D73) |

The exact contract (metric families and labels, the cgroup layout and pod labels, the host-report socket and the tenant header) is [§27](27-usage-hooks.md) (D200–D202). The CPU sources behind those hooks are unchanged (D175): wasmtime fuel or epochs for T1; the per-tenant cgroup's `cpu.stat` for T0 (workerd, cross-checked with `getrusage`); the sandbox cgroup's `cpu.stat` for T2. eBPF (aya, `sched_switch`) comes last, as a cross-check, because a per-switch BPF map update taxes every tenant's hot path. Inside one workerd process, CPU is exact per tenant but only estimated per invocation (Q-RT-6). Runners outside Loams's nodes (Lambda, Workers) report plain measurements to the `InvocationObserver` (§16; the billing-grade usage contract of D376 moved to `loam-platform`, 2026-10-02).

## 8. Storage and the data plane

- **Metadata: TiKV** (D179, D124). Function versions, routes and the resident-pool directory live in a TiKV keyspace. PD and TiKV are deployed by TiDB Operator v2 (§25 §6). D-SC-16 dropped TiDB from the suite, and this runtime needs none.
- **Bundles and static assets: the object store.** RustFS by default (D178, D61), reached through `ObjectStoreProvider`. Durable writes use Loams's WAL group commit to S3 (§02).
- **Cache: foyer** (Apache-2.0, v0.22.6), a hybrid RAM and NVMe cache for bundles and hot static assets on each node, as in §04.
- **io_uring only in the host data plane.** Loams's own storage paths may use it. Tenant sandboxes are not given it: gVisor's io_uring support is limited and off by default **(verify)**, and the seccomp profile for T0 denies it.

## 9. Cost model (checked 2026-09-29)

**Published prices.**

| Provider | Price | Wall time billed? | Source |
|---|---|---|---|
| Cloudflare Workers Standard | $5/month; 10M requests + 30M CPU-ms included; **$0.30 per extra million requests; $0.02 per extra million CPU-ms**; CPU up to 5 min per invocation (default 30 s) | **No** ("No charge or limit for duration") | developers.cloudflare.com/workers/platform/pricing |
| Vercel Fluid compute | **Active CPU $0.128/h** (iad1, pdx1, cle1; up to $0.221/h in gru1); Provisioned Memory $0.0106/GB-h (iad1); invocations $0.60/M | CPU no; **memory yes** for the instance's whole life | vercel.com/docs/functions/usage-and-pricing |
| AWS Lambda (x86) | **$0.0000166667 per GB-s**; $0.20 per million requests; free tier 1M requests + 400,000 GB-s | **Yes** (duration × memory) | aws.amazon.com/lambda/pricing |
| EC2 floor | **~$0.047 per vCPU-hour** at 100% utilization | — | (draft; not re-checked) |

**Worked example:** 100M requests a month, 10 ms CPU and 200 ms wall time per request.

| Provider | Calculation | Monthly |
|---|---|---|
| Cloudflare | $5 + 90M × $0.30/M = $27 + (1,000M − 30M) CPU-ms × $0.02/M = $19.40 | **$51.40** |
| Lambda, 256 MB | 100M × 0.2 s × 0.25 GB = 5M GB-s × $0.0000166667 = $83.33 + $20 requests (free tier ignored) | **$103.33** |
| Vercel, iad1 | CPU 277.8 h × $0.128 = $35.56 + $60 invocations + memory from $0 (full concurrency) to $58.89 per GB (none) | **$96–155** at 1 GB |
| Loams's cost floor | 277.8 vCPU-h × $0.047 | **$13.06** at 100% utilization; **$26** at 50% **(estimate)** |

**What the numbers say.** Cloudflare's CPU price is **$0.072 per CPU-hour** (3.6M CPU-ms per hour × $0.02 per million). That is only about 1.5× the EC2 floor at 100% utilization, and it is below Loams's cost at realistic utilization (40–60%, $0.078–0.118 per vCPU-hour, **estimate**). In Cloudflare's model the margin is in the request fee, not the CPU price. So Loams cannot win on CPU price alone. It wins on **placement**: a function next to the data does not pay egress, extra round trips or a separate database bill, and **suspension** (D173) means long-waiting agents hold no memory. This supports D170, and it makes the request price a pricing decision to settle with the owner (Q-RT-7).

## 10. Security

- **Per-tenant processes** for T0 (D171), and gVisor for T0 and T2 where the node supports it. seccomp-bpf, user namespaces and cgroups are the fallback, and the fallback is reported in the node's status.
- **No ambient credentials.** Functions see only a sandbox token (D182) bound to the invocation and its expiry. An egress allowlist per tenant is enforced by the supervisor's network namespace (the same rule as D141's outbound allowlist).
- **Spectre and timing.** workerd's hosted defenses (Cloudflare's security-model post) are not all present in open-source workerd **(verify)**. Tenant-per-process under gVisor is the answer, not isolate tricks.
- **Resource limits** per tier: memory (cgroup `memory.max`, Wasm memory limits), CPU per invocation (fuel/epoch for T1, a supervisor watchdog on the cgroup delta for T0 and T2), wall-clock per invocation (Q-RT-2).

## 11. Roadmap: track F

| Phase | Scope | Exit gate |
|---|---|---|
| **F0 (spikes)** | workerd in gVisor on k3d: cold start, per-tenant RSS and capnp generation from a wrangler bundle (Q-RT-3); Spin vs wasmCloud vs raw wasmtime as the T1 host (D172); the Resonate TS SDK inside workerd (Q-RT-5); TiDB Operator v2 with PD+TiKV only and `storage.api-version = 2` (§25 §6.4) | each spike's written result |
| **F1** | manifest (`loams.toml` `[functions]`); `loams-dapr` with tenant identity; T0 workerd with Hono (the reference app) and T1 wasmtime for Wasm components; Resonate suspend and resume; fuel and cgroup metering hooks (§7); secrets via Dapr (§5.1); HTTP/1.1, HTTP/2, HTTP/3 (at Envoy), gRPC, WebSocket and SSE; umbrella chart and Argo CD (§25) | a Hono agent that sleeps 1 h through the SDK and is billed only for its CPU; per-tenant isolation tests; hook CPU totals within 2% of cgroup totals; a tenant cannot read another tenant's secret |
| **F2** | T2 under gVisor, `http-port` contract | Express and Django samples pass |
| **Later** | T3 Firecracker via firecracker-containerd or our own orchestration (D174); the eBPF cross-check; Kafka triggers with M5; more protocols through `daprd` | — |

Track F depends on the unified auth plan (Q30) for API keys and OpenFGA, on D72 (the stream API) and on the Resonate + TiKV validation in the forks (§22 §13b item 4). Like tracks R and D (D127, D145), it interleaves on the one-build machine.

## 12. Contradictions with earlier decisions

| Earlier | This document | Resolution |
|---|---|---|
| D120: rquickjs for Live functions | workerd for the function runtime (D171) | Both stand. D120 is Live's deterministic mutations; D171 is general functions. Q35 gains workerd as a candidate |
| D139: Resonate on TiDB for cloud | Resonate on TiKV in clusters | Follows D-SC-16 and the owner's forks-first plan (§22 §13b item 4). D261 (2026-09-29) now supersedes D139's TiDB clause: durable state on the native TiKV backend |
| D47/§10: openraft default metastore | TiKV for the runtime's metadata (D179) | D179 covers this runtime and the GitOps deployment. `loams dev` and `loams standalone` keep openraft |
| The draft's rquickjs T0, WasmEdge, Kata-driven T3, eBPF-first metering, "raft or postgres" | D171, D172, D174, D175, D179 | Superseded by the owner's 2026-09-29 changes |
| The consolidated plan v1 (2026-09-30): "Resonate on TiDB", four co-equal runner targets, a Dapr sidecar where needed, metering CloudEvents into Iceberg in this repository | D261, D170, D183, D190, D202 | Resolved in §16 (the reconciliation was §34 §15, now in `loam-platform`): Resonate on TiKV; `SupervisorRunner` is the default and the others are options (D375); one shared `daprd`; the record spec is open and the ledger is `loam-platform` (D376) |

## 13. Risks

| Risk | Mitigation |
|---|---|
| workerd releases daily (`v1.2026MMDD.n`) and its compatibility dates move | Pin a release per Loams version; set `compatibilityDate` per function from its manifest |
| Hosted-Workers features absent from open-source workerd break adapters | F0 spike (Q-RT-3); publish a support matrix per framework |
| Per-tenant workerd processes cost RSS at high tenant counts | The resident pool evicts idle tenants; measure RSS in F0 |
| The Resonate SDK does not run in workerd | Fall back to an SDK shim over `fetch` to the Resonate protocol (§21 §4) |
| Meter attribution disputes in T0 | Publish the method (§7); totals are exact |
| The shared `daprd` holds many tenants' secret-store credentials | Loams-side scoping (§5.1), per-tenant components, a dedicated `daprd` on request (Q-RT-15), `daprd` off the public network |
| The RustFS 1.0 line is weeks old (1.0.0 on 2026-09-16) | Pin; the S3 providers are the fallback (D178) |

## 14. Open questions

| # | Question | Owner | Needed by |
|---|---|---|---|
| Q-RT-1 | Name and home of the Rust Dapr server (`loams-dapr` in the engine workspace, or its own repository like the trigger edge), and whether `loams-stream-grpc` and `deploy/dapr` are merged to `main` first | Founder | F1 plan |
| Q-RT-2 | The resident threshold for short waits (default 30 s?) and the wall-clock limit for non-SDK code; whether resident memory beyond a free allowance is billed | Founder | F1 plan |
| Q-RT-3 | What the Cloudflare adapters' output needs that open-source workerd lacks; whether Loams generates capnp from `wrangler.toml` or ships its own builder | Eng | F0 |
| Q-RT-4 | Durable Objects in clusters: out of scope, or backed by a TiKV keyspace with single-owner placement through the router (§18) | Eng | F2 |
| Q-RT-5 | Does the Resonate TypeScript SDK run inside workerd unchanged | Eng | F0 |
| Q-RT-6 | Per-invocation CPU attribution inside a shared workerd process: the apportioning rule, or one process per function for tenants that need exact per-invocation meters | Eng | F1 plan |
| Q-RT-7 | Pricing: per-request fee and CPU price given §9's numbers | Founder | Before cloud beta |
| Q-RT-15 | Secrets: does `daprd` v1.18 hot-reload per-tenant components, and which tenants get a dedicated `daprd` (§5.1, D189) | Eng | F1 plan |

Open questions about the deployment and Clever Cloud are in §25 §8.

## 15. Sources

Read on 2026-09-29.

- The owner's draft "CPU-Time Serverless Runtime: Consolidated Plan" v2 (2026-09-28) and the owner's approved changes (conversation, 2026-09-29).
- Repository: `crates/loams-stream-grpc/{Cargo.toml,proto/loams/stream/v1/stream.proto,src/lib.rs}`, `deploy/dapr/{README.md,edge/Cargo.toml,edge/src/main.rs}` (uncommitted, `design-neon-wesql` working tree); §02, §04, §10 §1, §14, §18, §19 §5, §20, §21, §22 §13b; D47, D61, D66, D67, D72, D74, D111, D120, D124, D127, D139, D141, D145, D-SC-16; Q30, Q35.
- workerd: `cloudflare/workerd` `README.md` ("WARNING: `workerd` is not a hardened sandbox"), `LICENSE` (Apache-2.0), `src/workerd/server/workerd.capnp` (bindings `kvNamespace`, `r2Bucket`, `wrapped`, `durableObjectStorage.localDisk`; no CPU limit field); latest release `v1.20260929.1` (2026-09-29).
- wasmtime: `bytecodealliance/wasmtime` `LICENSE` (Apache-2.0), release v49.0.1 (2026-09-24). Spin: `spinframework/spin` `LICENSE` (Apache-2.0), `README.md` (built on Wasmtime and the component model), `crates/` (factors, trigger-http), release v4.1.0 (2026-08-26). wasmCloud: `wasmCloud/wasmCloud` `LICENSE` (Apache-2.0), `README.md` (CNCF Incubating; `crates/wash-runtime` "the embeddable Rust runtime … custom embedded hosts"; the runtime operator schedules over NATS), release v2.10.1 (2026-09-24).
- Kata: `kata-containers/kata-containers` `src/runtime/virtcontainers/fc.go` (`PauseVM`, `SaveVM` and `ResumeVM` return `nil` without acting), release 4.2.0; firecracker-containerd `LICENSE` Apache-2.0, last commit 2026-07-16, no releases.
- gVisor `google/gvisor` Apache-2.0, `release-20260921.0`. Dapr `dapr/dapr` Apache-2.0, v1.18.4. Resonate `resonatehq/resonate` Apache-2.0, v0.9.8; `resonate-sdk-ts` Apache-2.0. Hono MIT, v4.13.10. OpenNext Cloudflare MIT, 1.20.6. foyer Apache-2.0, v0.22.6. aya Apache-2.0/MIT. Envoy Apache-2.0, v1.39.1.
- Cloudflare framework guides: developers.cloudflare.com/workers/framework-guides/web-apps.
- Dapr secrets: `dapr/components-contrib` `secretstores/` (directory listing; `secret_store.go`), release v1.18.5; `dapr/dapr` `dapr/proto/runtime/v1/dapr.proto` (`rpc GetSecret`, `rpc GetBulkSecret`); docs.dapr.io/reference/components-reference/supported-secret-stores (status per store); docs.dapr.io/developing-applications/building-blocks/secrets/secrets-scopes (`secrets.scopes`).
- Pricing: developers.cloudflare.com/workers/platform/pricing; vercel.com/docs/functions/usage-and-pricing (updated 2026-06-16); aws.amazon.com/lambda/pricing.

## 16. Amendments from the consolidated plan (2026-10-01; D375, D376)

The owner's "Loams Serverless Runtime — Consolidated Plan" v1 (2026-09-30) restates this document's premise and adds runners outside Loams's nodes. The full reconciliation was §34; since the owner's ruling of 2026-10-02 (§38 D440) the protocol gateway and the Cloudflare runner are commercial components in `loam-platform`, and [§34](34-protocol-gateway-and-standards.md) is a stub. The `Runner` trait therefore lives here.

**The draft's corrections, checked.**

| # | The draft's correction | Here |
|---|---|---|
| 1 | Fargate, Cloud Run and Container Apps bill allocated resources over wall time; Knative adds no CPU billing; only Cloudflare Workers bills CPU time; CPU billing is something Loams meters and charges | Agrees with §9 and D180. Cloudflare's prices are unchanged on 2026-10-01 ($0.02 per million CPU-ms, $0.30 per million requests, no charge for duration) |
| 2 | "Resonate is not backed by TiKV … use TiDB" | Withdrawn: Loams's fork has the native TiKV store, on `main` since PR #114 (D261) |
| 3 | The sample code was not durable (raw get/put race, completion written after side effects, `f64` money, no tenant prefix) | Agrees: TiKV only through transactions or CAS, integer money, tenant scope on every key (D374) |
| 4 | Dapr subscriptions need registration; handlers answer `SUCCESS`/`RETRY`/`DROP`; CloudEvent attributes are preserved | Already so in `deploy/dapr/edge` and §02 §7.4 (D270) |
| 5 | `tikv-client` does not build for `wasm32-unknown-unknown`; Workers stay thin | Agrees with §5: functions reach Loams through `loams-dapr` and the gateway, never TiKV |

**Runners (D375).** §3's architecture is one `Runner` among several: the node supervisor and its tiers are `SupervisorRunner`, the default and the only runner with D170's placement next to the data. `KnativeRunner` (§38 D441) runs the `http-port` contract on Knative Serving in self-hosted clusters. `LambdaRunner` (Rust, arm64, `provided.al2023`) and, on demand, Cloud Run and Container Apps runners (Q367) are options for burst and BYOC. Runners outside this repository (the commercial Cloudflare runner in `loam-platform`) implement the same trait as `RunnerKind::External`. The trait is built by [RN1](../plans/2026-10-01-rn1-runner-usage.md); `SupervisorRunner` is built with F1. External runners run no Dapr (D183 is unchanged inside Loams's clusters).

```rust
// crates/loams-runner/src/lib.rs (RN1)
#[async_trait]
pub trait Runner: Send + Sync + fmt::Debug + 'static {
    fn kind(&self) -> RunnerKind;                                   // Supervisor, Process, Lambda, Knative, …
    fn capabilities(&self) -> &RunnerCapabilities;                  // contracts (fetch, http-port), limits, suspend support
    async fn deploy(&self, cx: &TenantCx, artifact: &Artifact) -> Result<Deployment, RunnerError>; // idempotent by digest
    async fn invoke(&self, cx: &InvocationCx, dep: &DeploymentRef, req: InvokeRequest) -> Result<InvokeResponse, RunnerError>;
    async fn undeploy(&self, cx: &TenantCx, dep: &DeploymentRef) -> Result<(), RunnerError>;
    async fn health(&self) -> RunnerHealth;
}
pub struct InvokeResponse { pub response: http::Response<Bytes>, pub usage: Option<Usage> } // None: the runner's usage reaches the hooks another way
```

| Runner | Contract | Isolation | CPU source for the hooks | Status |
|---|---|---|---|---|
| `SupervisorRunner` | `fetch`, `http-port`, `static` (D181) | this document's tiers | the supervisor's hooks (D175, D201); `usage: None` | F1 builds the supervisor; RN1 the adapter |
| `KnativeRunner` | `http-port` | gVisor pods (§38 §3) | the pod cgroup and its labels (§27 §3.2); `usage: None`, no meter (§38 D444) | MT2 |
| `ProcessRunner` (dev and tests) | `fetch` over a child process | cgroup v2 when delegated, else none | child `cpu.stat` or `wait4` rusage | RN1 |
| `LambdaRunner` | `fetch` through Loams's Lambda bootstrap | Firecracker (AWS) | **Q366 moved to `loam-platform` (2026-10-02, D548); no billing figure here.** Formerly: either the in-process `getrusage(RUSAGE_SELF)` delta capped by the billed duration × `memory_mb / 1 769`, or the billed duration itself | RN1 (Task 5, no longer waits for Q366) |
| Cloud Run, Container Apps | `http-port` | gVisor or VM (provider) | in-container cgroup `cpu.stat` **(verify)** | not planned (Q367) |

**Placement.** D170's advantage is placement next to the data; a function on Lambda loses it and pays the round trips and egress. External runners are for burst capacity and BYOC accounts that want their own cloud bill, not the default.

**Usage (D376; superseded 2026-10-02 by D548).** The billing-grade usage rules for external runners (the host report on a node-local socket, the Lambda `getrusage` delta and its billed-duration cap, Q366, the additive private meter-record fields) moved to `loam-platform` doc 06 and are no longer specified here. In this repository every runner, the supervisor included, calls `InvocationObserver` ([§27 §3.7](27-usage-hooks.md)) with plain measurements at the end of each invocation; nothing aggregates, rates or bills them (§38 D444, §41).

**Track F.** F1 is unchanged. RN1 adds `loams-runner` (the trait, `RunnerHost` and `InvocationObserver`, which F1's supervisor then calls), `ProcessRunner` and `LambdaRunner`; it does not build the supervisor. The reporter crate `loams-meter` that this paragraph once listed moved to `loam-platform` (D548).

**Knative (2026-10-02, §38 D441).** When `knative.enabled`, the `http-port` contract (T2) runs on Knative Serving through `KnativeRunner`, which takes over F2's pod scheduling; F2 keeps the gVisor node setup and the `gvisor` `RuntimeClass`. T0 and T1 stay on the supervisor.
