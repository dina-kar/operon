# 27 — Usage Hooks: How the Engine Exposes Usage for Metering

Status: **Proposed** · 2026-09-29. Builds on D190 (billing and metering move to the private `loam-platform` repository; §24 §7 lists the open hooks) and turns §24 §7's list into a contract. Refines D182 and D190. Decisions **D200–D202**; questions **Q-UH-n**. **Amended 2026-10-01** by [§34](34-protocol-gateway-and-standards.md) (D376, proposed): usage from runners outside Loam's nodes, additive `loam.meter.v1` fields and a CloudEvents form of the host report (§3.6).

Numbering: `main` ends at D147. The highest number on any `design-*` branch is D190 (§24, `design-cpu-time-runtime`). Docs 23 and 26 are being written on other branches and may take numbers after D190, so this document starts at **D200**.

---

## 1. Summary

| # | Decision | Status |
|---|---|---|
| D200 | **The Rust Dapr server does not meter.** `loam-dapr` checks tenant identity and authorizes (D182) and records no meter events; like any component it exports its own call metrics as hooks (§3.1). Refines D182 and D190 | Proposed |
| D201 | **The usage-hooks contract** (§3): the metric families and labels, the cgroup layout and pod labels for every sandbox with a final-read guarantee, per-invocation host reports on a node-local socket (`loam.meter.v1.HostReport`), and Envoy access logs with a gateway-set tenant header. Any metering system can consume them. They are part of the engine's public contract and are versioned like an API. Refines D190 | Proposed |
| D202 | **The dependency runs one way.** Metering and billing (D190) are a Loam Cloud component that is neither in the engine nor in its chart, and the engine never depends on it. Quota **enforcement** stays in the engine (D65, D98); the limits come from an operator's configuration or from a control plane (§4) | Proposed |

## 2. Why

§24 as first drafted put metering in `loam-dapr` (D182: "meter events are recorded there"), and D190 then moved billing and metering to `loam-platform`. This document settles what the open engine still owes. Two problems ruled out metering in `loam-dapr`:

1. **The server does not exist yet** (§24 §3.1). Only `operon-stream-grpc` (one `Produce` RPC) and a Dapr app behind a Go sidecar exist, both uncommitted. Billing would wait for it.
2. **It meters from the wrong place.** `loam-dapr` sees Dapr API calls, not CPU. CPU and memory are node-level data (cgroups, `/proc`), and in T0 and T1 one process serves many isolates or tenants. Only the runtime host knows the per-invocation split, and only something outside the sandbox can read the cgroup totals without tenants being able to interfere.

A self-hosting organisation needs to *see* usage (dashboards, capacity, per-namespace quotas), but it does not need billing. A hosted, multi-tenant, paid service does. So the engine's job is to make usage observable, precisely and with the tenant attached, through standard interfaces. Turning that into invoices is the cloud's job (D202).

## 3. The hooks (D201)

### 3.1 Metrics

| Source | Series (Prometheus names; OTLP uses the same names with dots) | Labels |
|---|---|---|
| Engine, per namespace (D103's units) | `loam_namespace_logical_bytes_written_total`, `…_logical_bytes_stored`, `…_bytes_queried_total`, `…_queries_total`, `…_hot_gb_hours_total` | `org`, `namespace` |
| Runtime supervisor, per function | `loam_function_invocations_total`, `loam_function_cpu_seconds_total` (host-measured), `loam_function_wall_seconds_total`, `loam_function_resident_bytes` | `org`, `namespace`, `function`, `tier` |
| `loam-dapr`, per Dapr API call (D200) | `loam_dapr_calls_total`, `loam_dapr_call_duration_seconds` (histogram), `loam_dapr_secret_cache_hits_total`, `loam_dapr_secret_cache_misses_total` | `org`, `namespace`, `api` (`secrets`, `state`, `pubsub`, `invoke`, …), `code` (calls only) |
| `loam-gateway`, per request (§3.4) | `loam_gateway_requests_total`, `loam_gateway_request_bytes_total`, `loam_gateway_response_bytes_total`, `loam_gateway_request_duration_seconds` (histogram) | `org`, `namespace`, `route`, `code` (requests only) |
| Durable (Resonate, §21) | `loam_durable_promises_created_total`, `loam_durable_timers_scheduled_total` (where the count is a counter on the create path, not a scan) | `org`, `namespace` |

Millions of namespaces make per-namespace labels expensive in a Prometheus scrape. Each node therefore exports only the namespaces active on it, and the per-namespace families can be turned off on the Prometheus endpoint and sent instead as **OTLP metrics with delta temporality**, which a collector can aggregate without holding every series (Q-UH-1).

### 3.2 Cgroup layout and sandbox labels

The runtime places every tenant workload in a cgroup whose path and labels are documented and stable:

| Tier | Cgroup | Tenant from |
|---|---|---|
| T0 workerd (one process per tenant, D171) | `loam.slice/tenant-<org>.slice/workerd.scope` under the supervisor's delegated subtree | the path; the split by namespace and function from the metrics and host reports |
| T1 wasmtime host | `loam.slice/wasm-host.scope` (shared) | per-invocation reports (§3.3), not the cgroup |
| T2 gVisor sandbox (a pod, `RuntimeClass: gvisor`) | the pod's cgroup (`kubepods-…-pod<uid>.slice` or `pod<uid>`) | pod labels `loam.dev/org`, `loam.dev/namespace`, `loam.dev/function`, `loam.dev/tier`, set by the operator |

This refines §24 §7's `loam.slice/tenant-<org>.slice/fn-<id>.scope`: T0 has one process per tenant, not per function, and T2 sandboxes are pods whose cgroups the kubelet creates. With these, a node agent can read `cpu.stat`, `memory.current`, `memory.peak` and `cgroup.events` for every sandbox and attribute them without any engine API. `populated 0` in `cgroup.events` means the sandbox has finished; it does not by itself allow cleanup. For T0 and T1, whose cgroups the supervisor owns, the supervisor then sends a `SandboxFinished` notice with the cgroup path on the host-report socket (§3.3) and **removes the cgroup only after the consumer acknowledges that it has read `cpu.stat` and `memory.peak`**. With no consumer connected, the cgroup is kept for a retention window (default 10 minutes, configurable) and then removed, since nobody is reading. T2 pod cgroups belong to the kubelet, which removes them when it cleans up the terminated pod; how the final reading is taken before that is open (Q-UH-3).

### 3.3 Per-invocation reports from runtime hosts

Where many tenants share a process (T1, and T0's per-request split), the host reports each invocation on a node-local Unix socket, `/run/loam/meter.sock`, as length-delimited protobuf. The socket is created by the consumer and the host connects to it; no consumer means no reports and no cost. Tenants cannot reach it.

```protobuf
syntax = "proto3";
package loam.meter.v1;

message HostReport {
  string host_id = 1;          // stable per host process
  uint64 seq = 2;              // per host_id, monotonic; consumers dedupe on (host_id, seq)
  repeated Invocation invocations = 3;
}

message Invocation {
  string org = 1;
  string namespace = 2;
  string function = 3;
  string version = 4;
  string invocation_id = 5;
  uint64 cpu_usec = 6;         // see "CPU accuracy" below; exact only when cpu_estimated is false
  uint64 fuel = 7;             // 0 when fuel is off
  uint64 epochs = 8;           // epoch ticks consumed
  int64 start_unix_ms = 9;
  int64 end_unix_ms = 10;
  bool cpu_estimated = 11;     // true when cpu_usec was apportioned (T0), not measured
}

message HostReportAck { uint64 seq = 1; }   // cumulative: every report with seq <= this one
```

**Delivery.** An ack for `seq` means the consumer has durably recorded every report of that `host_id` up to and including `seq`. The host keeps unacknowledged reports in a bounded in-memory buffer (default 64 MiB) and, after a socket disconnect, resends them from the oldest unacknowledged `seq` when a consumer reconnects; consumers dedupe on `(host_id, seq)`. When the buffer is full the host drops the oldest reports and counts them in `loam_meter_reports_dropped_total`. A host restart loses its buffer and starts a new `host_id`, so sequence numbers never collide. CPU in lost or dropped reports is not lost from the totals: it is still in the cgroup's `cpu.stat` (§3.2), which is authoritative for totals, and a consumer reconciles the sum of host-reported CPU against it per window.

**CPU accuracy.** On T1 `cpu_usec` is measured: the host reads the thread CPU clock (`CLOCK_THREAD_CPUTIME_ID`) around each poll of the invocation, and `cpu_estimated` is false. On T0 one workerd process serves all of a tenant's invocations, so the per-invocation value is **an estimate**: the tenant's cgroup CPU for each interval is apportioned across the invocations that were running in it (the rule is Q-RT-6), and `cpu_estimated` is true. Per-invocation accuracy on T0 is not bounded by this contract. The 2% check in §5 compares aggregate totals only, not individual invocations.

### 3.4 Envoy access logs

`loam-gateway` sets the header `x-loam-tenant: <org>/<namespace>` (§24 §7) on every request after resolving the route, and Envoy strips any client-supplied value first, so the tenant never comes from the client. The same values are in route metadata `filter_metadata["loam"]` for access-log formats that read metadata. The chart's Envoy configuration has an access-log sink (gRPC ALS or OpenTelemetry) that is **off by default** and points at any consumer. Each entry gives a request, bytes in and bytes out, with the tenant.

### 3.5 What the engine keeps

- **Quota enforcement** (D65, D98): the engine enforces request rate, ingest bytes, concurrency and storage quotas per namespace. The limits come from configuration or from a control plane through the `ControlStore`.
- **D103's usage records** in the `ControlStore` remain, as the engine's own view of logical bytes.
- **eBPF** stays last, as a cross-check only (D175).

### 3.6 Usage from external runners (D376)

§24 §16 adds runners outside Loam's nodes (D375): `LambdaRunner`, a thin `WorkersRunner`, and later Cloud Run and Container Apps. They have no cgroup the supervisor owns and cannot reach `/run/loam/meter.sock`. The contract still has one consumer interface:

- **One reporter per invocation.** The supervisor reports its own tiers as above. For an external runner, the **runner host** (the gateway process that called `Runner::invoke`) writes the `HostReport`, with its own `host_id`, from the `Usage` the runner returned. A runner never reports and is never reported twice.
- **Where the CPU comes from.** Lambda: Loam's bootstrap reads `getrusage(RUSAGE_SELF)` before and after each invocation and returns the delta to the runner in a response header the bootstrap owns (`x-loam-usage`); the runner host caps it at the billed duration × the function's fractional CPU share (`memory_mb / 1 769`, the memory at which Lambda allocates one vCPU) and sets `cpu_estimated` when the cap applies; whether Lambda is metered this way or by billed duration is the owner's decision (Q366). Workers: the Tail Worker's `CPUTimeMs` (Cloudflare changelog 2025-04-09). It arrives after the response, so the handoff is asynchronous: `Runner::invoke` returns a `Usage` marked *pending* with the invocation's join key, `InvocationCx.invocation_id`: the host generates it, it is unique per invocation, and `WorkersRunner` sends it to the Worker as the request header `x-loam-invocation-id`. The tenant's Tail Worker, deployed by Loam with the function, reads that header from the trace event and posts the event to the runner host's authenticated usage endpoint; the host joins each event to its pending entry by that id and only then writes the one `HostReport` (`cpu_estimated` false). An entry with no event after a timeout (default 5 minutes) is reported once with `cpu_usec` 0, `cpu_estimated` true and `loam_runner_usage_missing_total` incremented; an event that arrives after its entry was reported, or matches no entry, is dropped and counted (`loam_runner_usage_late_total`), never reported a second time. The pending table is in memory and bounded; on a host restart its entries are lost and counted as missing. The `WorkersRunner` plan fixes the endpoint and timeout (Q373).
- **Additive fields** in `loam.meter.v1.Invocation`; consumers that do not know them ignore them:

```protobuf
  string runner = 12;              // "supervisor", "process", "lambda", "workers", …; empty means "supervisor"
  string region = 13;              // the provider's region for external runners; empty on Loam's nodes
  uint64 provider_billed_ms = 14;  // the provider's billed duration (Lambda's platform report), 0 when none
  uint64 compile_usec = 15;        // CPU Loam spent compiling for this tenant (Cranelift, script compile), not in cpu_usec
  uint64 overhead_usec = 16;       // runtime CPU attributable to the invocation but not to tenant code, where measured
```

- **A CloudEvents form.** Each `Invocation` can also be emitted as the CloudEvent `dev.loam.meter.usage.v1` (`id` = `<host_id>:<seq>:<index>`, `source` = `/hosts/<host_id>`, `tenantid` = `<org>/<namespace>`, `data` = the `Invocation` in protobuf), through the high-rate stream path of §34 §4.3. It is off unless a consumer configures the target stream, so the engine keeps no default dependency (D202). The socket stays the authoritative, acknowledged path.
- **Totals.** For Lambda and Workers there is no cgroup total to reconcile against; the provider's own figures (billed duration, CPU-ms on Cloudflare's invoice) play that role, and the reconciliation is `loam-platform`'s (D190, D202).

## 4. What is not in this repository (D202)

The node agent that reads these hooks, the aggregation of usage per tenant, pricing, invoices and credits, and the export to a billing provider are part of Loam Cloud and live outside this repository. This repository does not depend on them, and its chart does not deploy them. Anyone can build the same thing on the hooks in §3, or use an open-source metering service.

## 5. Changes to §24

- §24 §5: "emits the metering hooks of §7" means `loam-dapr`'s own call metrics (§3.1); it records no meter events (D200).
- §24 §7: the hook table stands as a summary; §3 here is the contract, and the cgroup row is refined as in §3.2.
- §24 §11: F1's exit gate "billed only for its CPU" is checked through the hooks: the host-reported CPU for the test function, summed, matches its cgroup's `cpu.stat` within 2% (an aggregate check, not a per-invocation one).

## 6. Open questions

| # | Question | Owner | Needed by |
|---|---|---|---|
| Q-UH-1 | Per-namespace metric cardinality: OTLP delta metrics only, or a Prometheus endpoint limited to the namespaces active on a node | Eng | F1 plan |
| Q-UH-2 | How the hooks contract is versioned (the metric names, the cgroup layout, the labels and `loam.meter.v1`), and where its conformance tests live | Eng | F1 plan |
| Q-UH-3 | The final cgroup reading for T2 pods, whose cgroups the kubelet removes: a delay on pod cleanup, or the sandbox's own accounting sent as a final report | Eng | F2 plan |
| Q366 | Lambda CPU attribution: the in-process `getrusage` delta capped by billed duration, or billed duration as the meter on Lambda (§3.6, §34 Q366) | Founder | RN1 Task 5 |

## 7. Sources

Read on 2026-09-29: §24 (§3.1, §5, §7, §11), §21, D65, D73, D98, D103, D171, D175, D182; Linux `Documentation/admin-guide/cgroup-v2.rst` (`cpu.stat usage_usec`, `memory.current`, `memory.peak`, `cgroup.events populated`); Envoy `envoy.service.accesslog.v3.AccessLogService` and the OpenTelemetry access-log sink.
