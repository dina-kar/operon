# 27 — Usage Hooks: Generic Observability the Engine Exposes

Status: **Proposed** · 2026-09-29. Builds on D190 (billing and metering move to the private `loam-platform` repository; §24 §7 lists the open hooks) and turns §24 §7's list into a contract. Refines D182 and D190. Decisions **D200–D202**; questions **Q-UH-n**. **Amended 2026-10-01** by [§34](34-protocol-gateway-and-standards.md) (D376, proposed): usage from runners outside Loams's nodes (§3.6; moved to `loam-platform` on 2026-10-02). **Amended 2026-10-02** by [§38](38-knative-authentik-gitops.md) (D440, D444, proposed): Knative pods under §3.2 with no meter (§3.5a).

> **Amended 2026-10-02 by [§41](41-multitenant-byoc-control-plane.md) (D541, D547, D548, D549, owner ruling).** Billing-grade metering moved to the private `loam-platform` repository because **integrity is the security principle**: figures that set what a tenant pays must not have their producers or validators open to manipulation by agents or tenants. What stays here is **generic observability**: the metric families (§3.1), the cgroup layout and pod labels (§3.2, without the final-read acknowledgement), Envoy access logs (§3.4), quota enforcement (§3.5) and the `InvocationObserver` extension point (§3.7). **Moved to `loam-platform` doc 06 (authoritative):** the per-invocation host report and its socket, buffering and acknowledgement rules (was §3.3), the external-runner usage rules, additive fields and CloudEvents form (was §3.6), and the usage reporter. Nothing in this repository claims to be billing-grade.

Numbering: `main` ends at D147. The highest number on any `design-*` branch is D190 (§24, `design-cpu-time-runtime`). Docs 23 and 26 are being written on other branches and may take numbers after D190, so this document starts at **D200**.

---

## 1. Summary

| # | Decision | Status |
|---|---|---|
| D200 | **The Rust Dapr server does not meter.** `loams-dapr` checks tenant identity and authorizes (D182) and records no meter events; like any component it exports its own call metrics as hooks (§3.1). Refines D182 and D190 | Proposed |
| D201 | **The usage-hooks contract** (§3): the metric families and labels, the cgroup layout and pod labels for every sandbox, and Envoy access logs with a gateway-set tenant header. **Amended 2026-10-02 (D548):** the final-read guarantee and the per-invocation host reports (`loams.meter.v1.HostReport`) moved to `loam-platform`; what remains is generic observability, versioned like an API. Refines D190 | Proposed · amended |
| D202 | **The dependency runs one way.** Metering and billing (D190) are a Loams Cloud component that is neither in the engine nor in its chart, and the engine never depends on it. Quota **enforcement** stays in the engine (D65, D98); the limits come from an operator's configuration or from the open control plane's limits API (§41 §9). Reaffirmed 2026-10-02 (D551) | Proposed |

## 2. Why

§24 as first drafted put metering in `loams-dapr` (D182: "meter events are recorded there"), and D190 then moved billing and metering to `loam-platform`. This document settles what the open engine still owes. Two problems ruled out metering in `loams-dapr`:

1. **The server does not exist yet** (§24 §3.1). Only `loams-stream-grpc` (one `Produce` RPC) and a Dapr app behind a Go sidecar exist, both uncommitted. Billing would wait for it.
2. **It meters from the wrong place.** `loams-dapr` sees Dapr API calls, not CPU. CPU and memory are node-level data (cgroups, `/proc`), and in T0 and T1 one process serves many isolates or tenants. Only the runtime host knows the per-invocation split, and only something outside the sandbox can read the cgroup totals without tenants being able to interfere.

A self-hosting organisation needs to *see* usage (dashboards, capacity, per-namespace quotas), but it does not need billing. A hosted, multi-tenant, paid service does. So the engine's job is to make usage observable, precisely and with the tenant attached, through standard interfaces. Turning that into invoices is the cloud's job (D202).

## 3. The hooks (D201)

### 3.1 Metrics

| Source | Series (Prometheus names; OTLP uses the same names with dots) | Labels |
|---|---|---|
| Engine, per namespace (D103's units) | `loams_namespace_logical_bytes_written_total`, `…_logical_bytes_stored`, `…_bytes_queried_total`, `…_queries_total`, `…_hot_gb_hours_total` | `org`, `namespace` |
| Runtime supervisor, per function | `loams_function_invocations_total`, `loams_function_cpu_seconds_total` (host-measured), `loams_function_wall_seconds_total`, `loams_function_resident_bytes` | `org`, `namespace`, `function`, `tier` |
| `loams-dapr`, per Dapr API call (D200) | `loams_dapr_calls_total`, `loams_dapr_call_duration_seconds` (histogram), `loams_dapr_secret_cache_hits_total`, `loams_dapr_secret_cache_misses_total` | `org`, `namespace`, `api` (`secrets`, `state`, `pubsub`, `invoke`, …), `code` (calls only) |
| `loams-gateway`, per request (§3.4) | `loams_gateway_requests_total`, `loams_gateway_request_bytes_total`, `loams_gateway_response_bytes_total`, `loams_gateway_request_duration_seconds` (histogram) | `org`, `namespace`, `route`, `code` (requests only) |
| Durable (Resonate, §21) | `loams_durable_promises_created_total`, `loams_durable_timers_scheduled_total` (where the count is a counter on the create path, not a scan) | `org`, `namespace` |

Millions of namespaces make per-namespace labels expensive in a Prometheus scrape. Each node therefore exports only the namespaces active on it, and the per-namespace families can be turned off on the Prometheus endpoint and sent instead as **OTLP metrics with delta temporality**, which a collector can aggregate without holding every series (Q-UH-1).

### 3.2 Cgroup layout and sandbox labels (open)

The runtime places every tenant workload in a cgroup whose path and labels are documented and stable:

| Tier | Cgroup | Tenant from |
|---|---|---|
| T0 workerd (one process per tenant, D171) | `loams.slice/tenant-<org>.slice/workerd.scope` under the supervisor's delegated subtree | the path; the split by namespace and function from the per-function metrics (§3.1) |
| T1 wasmtime host | `loams.slice/wasm-host.scope` (shared) | the per-function metrics and the `InvocationObserver` (§3.7), not the cgroup |
| T2 gVisor sandbox (a pod, `RuntimeClass: gvisor`) | the pod's cgroup (`kubepods-…-pod<uid>.slice` or `pod<uid>`) | pod labels `loams.dev/org`, `loams.dev/namespace`, `loams.dev/function`, `loams.dev/tier`, set by the operator |

This refines §24 §7's `loams.slice/tenant-<org>.slice/fn-<id>.scope`: T0 has one process per tenant, not per function, and T2 sandboxes are pods whose cgroups the kubelet creates. With these, a node agent can read `cpu.stat`, `memory.current`, `memory.peak` and `cgroup.events` for every sandbox and attribute them without any engine API. `populated 0` in `cgroup.events` means the sandbox has finished. The supervisor keeps the cgroup of a finished T0 or T1 sandbox for a retention window (default 10 minutes, configurable) and then removes it, so that any node agent that reads cgroups from outside the sandbox can take a final reading. T2 pod cgroups belong to the kubelet, which removes them when it cleans up the terminated pod (Q-UH-3). *The acknowledged final read (`SandboxFinished` and `SandboxReadAck`, "remove only after the consumer acknowledges") moved to `loam-platform` with the record (2026-10-02, D548).*

### 3.3 Per-invocation reports from runtime hosts (moved to `loam-platform`)

**Superseded 2026-10-02 (D548).** The per-invocation host report, its protobuf package `loams.meter.v1`, the node-local socket, the delivery, buffering and acknowledgement rules and the CPU-accuracy contract are billing-grade metering input. They are specified in `loam-platform` `docs/design/06-meter-record-and-usage-reporter.md` (private, authoritative) and are not published here, so that their producers and validators cannot be studied and forged by tenants or agents. This repository measures CPU per invocation (T1 reads the thread CPU clock around each poll; T0 apportions the tenant's cgroup CPU) only to feed the open per-function metrics of §3.1 and the `InvocationObserver` of §3.7.

### 3.4 Envoy access logs

`loams-gateway` sets the header `x-loams-tenant: <org>/<namespace>` (§24 §7) on every request after resolving the route, and Envoy strips any client-supplied value first, so the tenant never comes from the client. The same values are in route metadata `filter_metadata["loams"]` for access-log formats that read metadata. The chart's Envoy configuration has an access-log sink (gRPC ALS or OpenTelemetry) that is **off by default** and points at any consumer. Each entry gives a request, bytes in and bytes out, with the tenant.

### 3.5 What the engine keeps

- **Quota enforcement** (D65, D98): the engine enforces request rate, ingest bytes, concurrency and storage quotas per namespace. The limits come from configuration or from a control plane through the `ControlStore`.
- **D103's usage records** in the `ControlStore` remain, as the engine's own view of logical bytes.
- **eBPF** stays last, as a cross-check only (D175).

### 3.5a Knative pods (2026-10-02, §38 D444)

Pods that `KnativeRunner` creates (§38 §3) are T2 sandboxes under §3.2: the pod cgroup with the labels `loams.dev/org`, `loams.dev/namespace`, `loams.dev/function`, `loams.dev/tier` (`t2`), plus `loams.dev/version` and `loams.dev/runner` (`knative`). `KnativeRunner` writes no host reports. Knative's queue-proxy and activator metrics and the edge's access logs are further open hooks. **No meter runs in this repository** (owner ruling of 2026-10-02).

### 3.6 Usage from external runners (D376; moved to `loam-platform`)

**Superseded 2026-10-02 (D548).** The rules for usage from runners outside Loams's nodes (one reporter per invocation, where Lambda and Workers CPU comes from, the additive `Invocation` fields 12 to 16, the CloudEvents form `io.loams.dev.meter.usage.v1`, reconciliation against provider invoices, and the Lambda CPU question Q366) are specified in `loam-platform` doc 06 and doc 01 §13. External runners in this repository implement the `Runner` trait ([§24 §16](24-cpu-time-runtime.md)), report plain measurements to `InvocationObserver` (§3.7), and carry no usage header and no billing figure.

### 3.7 `InvocationObserver` (new, D549)

A Rust trait in `loams-runner`, called at the end of every invocation by the supervisor's runner adapter and by `RunnerHost` with a plain struct: `org`, `namespace`, `function`, `version`, runner kind, wall time, CPU time as measured (and whether estimated), outcome. It has **no wire format, no buffer and no persistence**, like a tracing layer. The open implementation turns it into the `loams_runner_*` metrics. `loam-platform`'s private implementation plugs in at link time. `KnativeRunner` calls it with the pod-level facts it has and no CPU figure it did not measure (§38 D444).

## 4. What is not in this repository (D202, D548)

The node agent that reads usage for billing, the meter record, the reporter, aggregation per tenant, pricing, invoices and credits, the export to a billing provider, and the billing-grade usage API are part of Loams Cloud and live in `loam-platform`. This repository does not depend on them, and its chart does not deploy them. Anyone can build observability dashboards on the hooks in §3, but nothing here is, or claims to be, the source of truth for a charge. The reason is integrity: [open-core.md](../open-core.md), [§41](41-multitenant-byoc-control-plane.md) §10 to §11.

## 5. Changes to §24

- §24 §5: "emits the metering hooks of §7" means `loams-dapr`'s own call metrics (§3.1); it records no meter events (D200).
- §24 §7: the hook table stands as a summary; §3 here is the contract, and the cgroup row is refined as in §3.2.
- §24 §11: F1's exit gate "billed only for its CPU" is now checked in `loam-platform` (track MB, MB4): the host-reported CPU for the test function, summed, matches its cgroup's `cpu.stat` within 2%. F1's open exit checks per-function CPU metrics against the cgroup's `cpu.stat` (an aggregate check, not billing).

## 6. Open questions

| # | Question | Owner | Needed by |
|---|---|---|---|
| Q-UH-1 | Per-namespace metric cardinality: OTLP delta metrics only, or a Prometheus endpoint limited to the namespaces active on a node | Eng | F1 plan |
| Q-UH-2 | How the hooks contract is versioned (the metric names, the cgroup layout and the labels; `loams.meter.v1` moved to `loam-platform`), and where its conformance tests live | Eng | F1 plan |
| Q-UH-3 | The final cgroup reading for T2 pods, whose cgroups the kubelet removes: a delay on pod cleanup, or the sandbox's own accounting sent as a final report | Eng | F2 plan |
| Q366 | ~~Lambda CPU attribution~~ Moved to `loam-platform` (doc 06 PD66, default: billed duration). No longer gates RN1 Task 5 | Founder | Resolved here |

## 7. Sources

Read on 2026-09-29: §24 (§3.1, §5, §7, §11), §21, D65, D73, D98, D103, D171, D175, D182; Linux `Documentation/admin-guide/cgroup-v2.rst` (`cpu.stat usage_usec`, `memory.current`, `memory.peak`, `cgroup.events populated`); Envoy `envoy.service.accesslog.v3.AccessLogService` and the OpenTelemetry access-log sink.
