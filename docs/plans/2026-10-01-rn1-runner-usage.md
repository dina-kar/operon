# RN1 — The `Runner` Trait, `InvocationObserver` and the Process and Lambda Runners Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, headers, field numbers, metric names, defaults), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: Planned** (2026-10-01; amended 2026-10-02 by [§38](../design/38-knative-authentik-gitops.md) D440: the protocol gateway, the Cloudflare runner and the usage-event form moved to `loam-platform`, and this plan builds hooks only). **Track RN** (D375, D376; design [§24](../design/24-cpu-time-runtime.md) §16 and [§27](../design/27-usage-hooks.md) §3.6; [§34](../design/34-protocol-gateway-and-standards.md) is now a stub). Build-order item 7, and the open half of item 6. **Not covered by track F:** §24's F1 builds the node supervisor and its tiers but no runner abstraction, no external runner and no host-report emitter crate; RN1 builds those, and F1's supervisor then calls RN1's `InvocationObserver` (D549) at the end of each invocation. RN1 does **not** build the metering ledger: rating, aggregation and reconciliation are `loam-platform`'s (D190, D202; §34 §15 row 5). Tasks 1–5 depend on nothing new. The former Task 6 (usage as CloudEvents and Arrow) moved to `loam-platform` (private) with GW1 (D440). Branches `rn1-t<N>`, stacked; PRs target `main`. RN1 adds crates only; nothing is linked into `loams`'s default build. **Amended 2026-10-02 by [§41](../design/41-multitenant-byoc-control-plane.md) (D548, D549, D556, owner open-core ruling):** the usage reporter, the `loams.meter.v1` record and the Lambda usage header moved to `loam-platform` (doc 06, private, authoritative) because **integrity is the security principle**: billing-grade producers and validators are not published. This plan now builds the `Runner` trait, `RunnerHost` with the open `InvocationObserver`, `ProcessRunner` and a usage-free `LambdaRunner`. **Tasks 1 and 2 are moved**; Task 5 no longer waits for Q366.

**Goal:**
- `loams-runner`: the `Runner` trait (D375), `RunnerHost` that calls the `InvocationObserver` (D549) for every invocation, and a runner conformance kit.
- `ProcessRunner` (development and tests) and `LambdaRunner` with Loams's Lambda bootstrap (`loams-lambda-bootstrap`), both **without any usage header or billing figure**, tested locally against the AWS Lambda Runtime Interface Emulator.
- **Not here any more:** `loams.meter.v1` and `loams-meter` (the former Tasks 1 and 2), the usage header, the billed-duration cap and the `REPORT` parser. They are built in `loam-platform` (doc 06).

**Architecture:**
- **One observation point.** `RunnerHost` and the supervisor's runner adapter call `InvocationObserver::observed(&Observation)` at the end of every invocation. `Observation` is a plain struct (org, namespace, function, version, runner kind, wall time, CPU time as measured and whether it is estimated, outcome). The trait has no wire format, no buffer and no persistence. The open implementation feeds the `loams_runner_*` metrics; the private platform plugs in its own at link time (§27 §3.7).
- **Runners are thin.** A runner deploys an artifact, invokes it with an HTTP request and returns an HTTP response plus, when it has one, `Usage`: plain measurements (wall, CPU, whether CPU is estimated), used for metrics and passed to the observer. Routing, quotas and tenant checks stay in the gateway and `loams-dapr` (§24 §5).
- **Heavy dependencies are opt-in.** `aws-sdk-lambda` lives in its own crate, `loams-runner-lambda`, behind its own CI job.

**Tech Stack:** Rust 1.97.1, edition 2024, workspace lints. Workspace crates: `tokio` (net, `UnixStream`), `bytes`, `buffa`, `connectrpc-build` (messages only), `http` 1, `hyper` 1 and `hyper-util` (the UDS client), `async-trait`, `thiserror`, `tracing`, `rand`, `proptest`. New (Task 0 checks versions, licences, `cargo deny`, and a cold build-time delta for the Lambda crate): `aws-sdk-lambda` and `aws-config` (Apache-2.0), `lambda_runtime` and `lambda_http` (Apache-2.0, from `awslabs/aws-lambda-rust-runtime`), `rustix` (Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT, for `getrusage`; or `libc` if already in the tree). Test tool: the AWS Lambda Runtime Interface Emulator (`aws/aws-lambda-runtime-interface-emulator`, Apache-2.0), pinned release, downloaded by the test script.

**Spec:**
- [§24](../design/24-cpu-time-runtime.md) §16 (runners, the trait); [§27](../design/27-usage-hooks.md) §3.7 (`InvocationObserver`); [§41](../design/41-multitenant-byoc-control-plane.md) §10 (what stays open); [§38](../design/38-knative-authentik-gitops.md) D440, D444 (no metering in OSS).
- As built: `loams-cloudevents`.


## Global Constraints

Same as the M1 overview §8, plus:
- **Exactly one observation per invocation.** A test in Task 3 enforces it for every runner kind, so the open metrics never double count.
- **The engine never depends on the platform (D202, D551).** With the default observer nothing blocks, buffers or persists; the call is synchronous and must be cheap, and Task 3 tests that a panicking observer cannot fail an invocation.
- **No AWS account in CI.** Lambda tests run against the Runtime Interface Emulator; a real-AWS job is optional, manual, and needs the owner's credentials decision (Task 5).
- **The build machine.** One cargo build at a time, the shared target, `-j 6`, lld; `loams-runner-lambda` is built only in its own job and when its task is worked on.
- **Commit areas:** `runner`, `ci`, `docs`.
- **No billing-grade names (D552).** No identifier, file or string in this plan's crates may contain `loams.meter`, `meter.sock`, `HostReport` or `x-loams-usage`; the guard `scripts/ci/no-metering.sh` (MT4 Task 8) enforces it.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1–5 | **Moved to `loam-platform` (doc 06, 2026-10-02, D548):** the socket envelope, `host_id` and `seq`, reporter batching and the buffer, the usage response header, and the Lambda billed-duration cap. They were billing-grade; their text is no longer published here | Integrity (D541) | — |
| 6 | **`LambdaRunner` splits control from invocation**: `LambdaControl` (deploy, undeploy) with `AwsLambdaControl` (CreateFunction / UpdateFunctionCode / PublishVersion / alias `loams-<version>`) and `StaticLambdaControl` (a pre-deployed function name; used with the emulator); invocation always goes through `aws-sdk-lambda`'s `Invoke`, with `endpoint_url` overridden for the emulator | The emulator only implements invoke | Deploy paths are tested only by the optional real-AWS job |
| 7 | **Lambda invocations carry the HTTP request as an API Gateway v2 (HTTP API) event**, so the tenant's handler is an ordinary `lambda_http` handler, and the response is the matching v2 response | `lambda_http` already maps v2 events to `http::Request`; Loams's `fetch` contract is an HTTP request (§24 D181) | Binary bodies are base64 in the event, which costs ~33% on large payloads; documented |
| 8 | **`ProcessRunner` measures CPU per process** (cgroup v2 `cpu.stat` when the runner has a delegated subtree, else `/proc/<pid>/stat` `utime + stime`), and apportions it across invocations that overlapped, setting `cpu_estimated = true` whenever more than one was in flight | Development parity with T0's apportioning (estimated, never exact) | Development only; never used for billing |
| 9 | **`SupervisorRunner` is not built here**; it comes with F1 (it implements the trait in F1's plan). `KnativeRunner` is MT2's. A Cloudflare Workers runner is part of the commercial Cloudflare target in `loam-platform` and plugs in as `RunnerKind::External` (D440), observed through the same trait | Scope | None |

## Carried in

From §24 §16: the `Runner` trait sketch, refined here. From §41 §10: `InvocationObserver`.


## Review Focus

1. **Exactly one observation.** Tests: Task 3 (`supervisor_kind_is_observed_once`, `external_runner_is_observed_once`).
2. **The observer cannot hurt an invocation.** Tests: Task 3 (`panicking_observer_does_not_fail_invoke`, `no_observer_costs_nothing`).
3. **The open Lambda path carries no usage value.** Tests: Task 5 (`bootstrap_adds_no_headers`, `runner_returns_no_billing_figure`).
4. **No billing-grade names.** `scripts/ci/no-metering.sh` passes on this plan's crates.


## File structure

```
crates/loams-runner/                        # new (Tasks 3–4)
  src/{lib.rs,types.rs,error.rs,host.rs,observer.rs,registry.rs,process.rs,cpu.rs,conformance.rs}
  tests/{host.rs,process.rs,conformance_process.rs}
crates/loams-lambda-bootstrap/              # new (Task 5), no usage code
  src/lib.rs
  examples/echo.rs
  tests/passthrough.rs
crates/loams-runner-lambda/                 # new (Task 5)
  src/{lib.rs,control.rs,invoke.rs,event.rs}
  tests/rie.rs
scripts/runner/{rie.sh,build-lambda-example.sh}
.github/workflows/ci.yml                     # jobs runner (path-filtered), runner-lambda (path-filtered, RIE)
docs/design/27-usage-hooks.md  docs/design/24-cpu-time-runtime.md  CHANGELOG.md
```


### Task 0: Reconcile and check

**Files:** read §24 and §27 as merged, the status of F1 (is a supervisor on `main`?), `Cargo.toml`. Fill "Rulings made during execution".

**Checks:**
- Whether any code on `main` already defines a metering record or writes to a meter socket; if so, it is removed or moved to `loam-platform` (D548) and the difference is listed.
- `aws-sdk-lambda`, `aws-config`, `lambda_runtime`, `lambda_http` latest versions and licences; `cargo deny check` with them; **one measured cold build of `loams-runner-lambda`** (time and target-dir growth), recorded and the artifacts deleted.
- The Runtime Interface Emulator's latest release, its arm64 and x86 binaries, and that `Invoke` works against it.
- AWS Lambda's current payload and timeout limits, for the runner's capabilities.
- Q367's status. Q366 moved to `loam-platform` (doc 06 PD66) and does not gate anything here.

**Commit:** `docs: reconcile RN1 with main`.

### Task 1: moved to `loam-platform`

**Moved 2026-10-02 (D548).** The billing-grade usage record and its codec are built in `loam-platform` (doc 06). This repository has no `proto/loams/meter/`. The task number is kept so that references to Tasks 3 to 7 stay valid.

### Task 2: moved to `loam-platform`

**Moved 2026-10-02 (D548).** The host-side usage reporter and its test consumer are built in `loam-platform` (doc 06). In their place Task 3 defines the open `InvocationObserver`.


### Task 3: `loams-runner`: the trait, `RunnerHost` and the conformance kit

**Files:** `crates/loams-runner/src/{lib.rs,types.rs,error.rs,host.rs,registry.rs,conformance.rs}`, `crates/loams-runner/tests/host.rs`.

**Produces:**

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RunnerKind { Supervisor, Process, Lambda, Knative, CloudRun, ContainerApps, External(&'static str) } // External: runners outside this repository
impl RunnerKind { pub fn as_str(&self) -> &'static str; }       // "supervisor", "process", "lambda", …
pub struct RunnerCapabilities { pub contracts: Vec<Contract> /* Fetch, HttpPort, Static */, pub max_cpu: Duration, pub max_wall: Duration, pub max_body: usize, pub streaming: bool, pub websockets: bool, pub suspend: bool }
pub struct TenantCx { pub org: String, pub namespace: String }
pub struct InvocationCx { pub tenant: TenantCx, pub function: String, pub version: String, pub invocation_id: String, pub deadline: Instant, pub trace: Option<String> }
pub struct Artifact { pub function: String, pub version: String, pub digest: [u8; 32], pub contract: Contract, pub bytes: ArtifactBytes /* Path | Bytes */, pub memory_mb: u32, pub env: BTreeMap<String, String> }
pub struct Deployment { pub r#ref: DeploymentRef, pub runner: RunnerKind, pub created_unix_ns: i64 }
pub struct DeploymentRef { pub function: String, pub version: String, pub digest: [u8; 32], pub runner_handle: String }
pub struct InvokeRequest(pub http::Request<Bytes>);
pub struct Usage { pub cpu_usec: Option<u64> /* None = not measured */, pub cpu_estimated: bool, pub wall_usec: u64, pub region: Option<String>, pub start_unix_ms: i64, pub end_unix_ms: i64 } // plain measurements for metrics and the observer; not a billing record
pub struct InvokeResponse { pub response: http::Response<Bytes>, pub usage: Option<Usage> }
pub enum RunnerError { NotFound(DeploymentRef), Unsupported(Contract), DeadlineExceeded, Throttled { retry_after: Option<Duration> }, Provider(String), Artifact(String), Internal(String) }
#[async_trait] pub trait Runner { /* exactly §24 §16 */ }
pub struct RunnerHost { /* Arc<dyn Runner>, ObserverSet */ }
impl RunnerHost { pub async fn invoke(&self, cx: &InvocationCx, dep: &DeploymentRef, req: InvokeRequest) -> Result<http::Response<Bytes>, RunnerError>; }
#[macro_export] macro_rules! runner_conformance { ($factory:expr) => { … } }   // one #[tokio::test] per case
```

**Semantics:** `observer.rs` defines `pub trait InvocationObserver: Send + Sync { fn observed(&self, o: &Observation); }`, `pub struct Observation { org, namespace, function, version, runner: RunnerKind, wall_usec: u64, cpu_usec: Option<u64>, cpu_estimated: bool, outcome: Outcome /* Ok, Error, Timeout, Throttled */ }`, `NoopObserver`, `MetricsObserver` (exports `loams_runner_invocations_total`, `loams_runner_wall_seconds_total`, `loams_runner_cpu_seconds_total` with labels `org`, `namespace`, `function`, `runner`), and `ObserverSet` (calls each registered observer; a panic in one is caught and counted, never propagated). `RunnerHost::invoke` calls the runner and then calls the observer **exactly once**, even on failure; the supervisor adapter (F1) does the same for the supervisor's tiers. A runner's `usage`, when present, fills `cpu_usec` (which stays `None` if the runner did not measure it); when absent, the observation has `cpu_usec: None`. `RunnerCapabilities` has no reporting flag. Conformance cases: `deploy_is_idempotent_by_digest`; `invoke_returns_handler_response`; `undeploy_then_invoke_is_not_found`; `deadline_is_enforced`; `concurrent_invokes_complete`; `unsupported_contract_is_refused`; `health_reports_ready`.

**Tests:** `supervisor_kind_is_observed_once`; `external_runner_is_observed_once`; `failed_invoke_is_still_observed`; `panicking_observer_does_not_fail_invoke`; `no_observer_costs_nothing`; `runner_label_is_kind`; a `FakeRunner` passes `runner_conformance!`.

**Commit:** `runner: add the Runner trait, RunnerHost, InvocationObserver and the runner conformance kit`.

### Task 4: `ProcessRunner`

**Files:** `crates/loams-runner/src/{process.rs,cpu.rs}`, `crates/loams-runner/tests/{process.rs,conformance_process.rs}`, `crates/loams-runner/tests/fixtures/echo-server/` (a tiny binary built by the test, serving HTTP on `$LOAMS_SOCKET`).

**Produces:** `pub struct ProcessRunner { /* root dir, optional delegated cgroup */ }` with `ProcessRunner::new(root: PathBuf, cgroup: Option<PathBuf>)`; `Contract::Fetch` only. Deploy writes the artifact under `root/<function>/<version>-<digest-hex8>/` and starts it with `LOAMS_SOCKET=<dir>/sock`; invoke sends the request over the Unix socket (hyper client); undeploy sends SIGTERM, then SIGKILL after 5 s. `cpu.rs`: `CpuSource::{Cgroup(path), Proc(pid)}` with `read_usec()`; per-invocation apportioning per Ruling 8.

**Tests:** `conformance_process` (`runner_conformance!(ProcessRunner)`); `single_invocation_cpu_is_exact` (a handler that spins ~50 ms of CPU reports 40–80 ms, `cpu_estimated = false`); `overlapping_invocations_are_estimated`; `crash_restarts_on_next_invoke`; `cgroup_source_used_when_delegated` (skipped with a message when no delegated cgroup is available).

**Commit:** `runner: add the process runner for development and tests`.

### Task 5: The Lambda bootstrap and `LambdaRunner` (without usage)

**Moved out (2026-10-02, D548):** the usage response header, the `getrusage` read around the handler, the billed-duration cap, the `REPORT` line parser and `provider_billed_ms`. They are a tenant-reachable value that sets a charge, and are built in `loam-platform` (doc 06 PD66) as a wrapper around this runner. **Q366 no longer gates this task.**

**Files:** `crates/loams-lambda-bootstrap/{Cargo.toml,src/lib.rs,examples/echo.rs,tests/passthrough.rs}`, `crates/loams-runner-lambda/{Cargo.toml,src/lib.rs,src/control.rs,src/invoke.rs,src/event.rs,tests/rie.rs}`, `scripts/runner/{rie.sh,build-lambda-example.sh}`, `.github/workflows/ci.yml` (job `runner-lambda`).

**Produces:**

```rust
// loams-lambda-bootstrap
pub async fn run<F, Fut>(handler: F) -> Result<(), lambda_runtime::Error>
where F: Fn(http::Request<lambda_http::Body>) -> Fut, Fut: Future<Output = Result<http::Response<lambda_http::Body>, lambda_http::Error>>;
// loams-runner-lambda
pub struct LambdaRunner { /* aws_sdk_lambda::Client, Arc<dyn LambdaControl>, region */ }
#[async_trait] pub trait LambdaControl: Send + Sync { async fn deploy(&self, a: &Artifact) -> Result<String /* qualified ARN or name:alias */, RunnerError>; async fn undeploy(&self, handle: &str) -> Result<(), RunnerError>; }
pub struct AwsLambdaControl { /* role ARN, arch arm64, runtime provided.al2023 */ }
pub struct StaticLambdaControl { pub function: String }
```

**Semantics:** Rulings 6 and 7. The bootstrap wraps the tenant's handler and adds nothing to the response; the private wrapper in `loam-platform` (doc 06) is what strips tenant-set headers that could pose as platform values and adds its measurements. `LambdaRunner::invoke` builds the API Gateway v2 event from the request, calls `Invoke` (`InvocationType::RequestResponse`), maps the v2 response back, and returns `Usage { wall_usec, cpu_usec: None, cpu_estimated: false, region: Some(region), .. }` (plain measurements for the open metrics). Function errors (`FunctionError` set) answer 502 to the caller. `AwsLambdaControl::deploy` zips the artifact as `bootstrap`, creates or updates the function, publishes a version and points alias `loams-<version>` at it; idempotent by the artifact digest stored in the function's tags (`loams.dev/digest`).

**Tests:** `bootstrap_adds_no_headers` (the response equals the handler's); `runner_returns_no_billing_figure` (`Usage.cpu_usec` is `None` for Lambda); `rie_invoke_returns_response` and `rie_function_error_is_502` (the job starts the emulator with the `echo` example built for the runner's architecture, `StaticLambdaControl`, `endpoint_url` → the emulator); `runner_conformance!` against the emulator for the cases that do not need deploy. **Optional, manual:** a `workflow_dispatch` job `runner-lambda-aws` that deploys and invokes the example in a real account, only if the owner provides credentials and a budget (record the decision in Task 0).

**Commit:** `runner: add the Lambda runner and Loams's Lambda bootstrap`.


### Task 7: Docs and close

**Files:** `docs/design/27-usage-hooks.md` (§3.7 as built), `docs/design/24-cpu-time-runtime.md` (§16 as built), `CHANGELOG.md`.

**Tests:** the `runner` and `runner-lambda` jobs green; `cargo deny check`; `scripts/ci/no-metering.sh` once MT4 Task 8 has landed.

**Commit:** `docs: record RN1 as built`.

## What RN1 leaves to others

| Item | Where |
|---|---|
| `SupervisorRunner` (the node supervisor implementing `Runner`, calling the observer) | F1 plan (§24 §11) |
| `KnativeRunner` | MT2 |
| `loams.meter.v1`, the usage reporter, the Lambda usage header and cap, usage as CloudEvents and Arrow (the former Tasks 1, 2, 5-usage and 6) | `loam-platform` (doc 06; D440, D548) |
| A Cloudflare Workers runner | `loam-platform` (D440) |
| Cloud Run and Container Apps runners | on demand (Q367) |
| The consumer that aggregates, rates, bills and reconciles | `loam-platform` (D190, D202) |
| Showback in money or metering of any kind | not in this repository (D444, D541); operational dashboards over open metrics only (Q546) |


## PR sizes

| Task | Expected size |
|---|---|
| 1, 2 | moved (none) |
| 3 | ~700 lines |
| 4 | ~700 lines |
| 5 | ~800 lines across two crates plus scripts |
| 7 | docs |


## Rulings made during execution

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| — | (Task 0 fills this table) | | |
