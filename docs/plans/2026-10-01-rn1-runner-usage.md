# RN1 — The `Runner` Trait, External Runners and the Usage Reporter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, headers, field numbers, metric names, defaults), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: Planned** (2026-10-01). **Track RN** (design [§34](../design/34-protocol-gateway-and-standards.md) §11–§12, D375, D376; amends [§24](../design/24-cpu-time-runtime.md) §16 and [§27](../design/27-usage-hooks.md) §3.6). Build-order item 7, and the open half of item 6. **Not covered by track F:** §24's F1 builds the node supervisor and its tiers but no runner abstraction, no external runner and no host-report emitter crate; RN1 builds those, and F1's supervisor then uses RN1's `operon-meter` to report. RN1 does **not** build the metering ledger: rating, aggregation and reconciliation are `loam-platform`'s (D190, D202; §34 §15 row 5). Tasks 1–5 depend on nothing new; Task 6 depends on GW1 Tasks 3–5. Branches `rn1-t<N>`, stacked; PRs target `main`. RN1 adds crates only; nothing is linked into `operon`'s default build.

**Goal:**
- `loam.meter.v1`, the §27 usage contract, as a protobuf package with the §27 §3.6 additions and the socket envelope, in `proto/loam/meter/v1/` (D201, D376).
- `operon-meter`: the host-side reporter of §27 §3.3 (sequence numbers, bounded buffer, resend until acknowledged, drop counting) and a test consumer.
- `operon-runner`: the `Runner` trait (D375), `RunnerHost` with the one-reporter rule, and a runner conformance kit.
- `ProcessRunner` (development and tests) and `LambdaRunner` with Loam's Lambda bootstrap (`operon-lambda-bootstrap`), tested locally against the AWS Lambda Runtime Interface Emulator.
- The CloudEvents form of usage (`dev.loam.meter.usage.v1`) and its Arrow schema (D376 item 4), off by default.

**Architecture:**
- **One consumer contract.** Every usage record reaches the consumer as a `HostReport` on `/run/loam/meter.sock` (§27 §3.3). The supervisor (F1) reports its own tiers; `RunnerHost` reports for every other runner. A runner implementation never writes reports itself.
- **Runners are thin.** A runner deploys an artifact, invokes it with an HTTP request and returns an HTTP response plus, when it is not the supervisor, a `Usage`. Routing, quotas and tenant checks stay in the gateway and `loam-dapr` (§24 §5).
- **Lambda's CPU comes from inside the sandbox.** Loam's bootstrap wraps the tenant's handler (`lambda_runtime`, which serves one invocation at a time per sandbox), measures `getrusage(RUSAGE_SELF)` around each invocation and returns the delta in a response header the bootstrap owns. The runner caps it by the billed duration from the invocation's log tail (Ruling 5, Q366).
- **Heavy dependencies are opt-in.** `aws-sdk-lambda` lives in its own crate, `operon-runner-lambda`, behind its own CI job.

**Tech Stack:** Rust 1.97.1, edition 2024, workspace lints. Workspace crates: `tokio` (net, `UnixStream`), `bytes`, `buffa`, `connectrpc-build` (messages only), `http` 1, `hyper` 1 and `hyper-util` (the UDS client), `async-trait`, `thiserror`, `tracing`, `rand`, `proptest`. New (Task 0 checks versions, licences, `cargo deny`, and a cold build-time delta for the Lambda crate): `aws-sdk-lambda` and `aws-config` (Apache-2.0), `lambda_runtime` and `lambda_http` (Apache-2.0, from `awslabs/aws-lambda-rust-runtime`), `rustix` (Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT, for `getrusage`; or `libc` if already in the tree). Test tool: the AWS Lambda Runtime Interface Emulator (`aws/aws-lambda-runtime-interface-emulator`, Apache-2.0), pinned release, downloaded by the test script.

**Spec:**
- [§34](../design/34-protocol-gateway-and-standards.md) §11 (runners), §12 (metering), §15 rows 5, 6, 16–18, §18 Q362, Q366, Q367, Q373.
- [§27](../design/27-usage-hooks.md) §3.3 (the socket, delivery, CPU accuracy) and §3.6 (external runners); [§24](../design/24-cpu-time-runtime.md) §4 (contracts), §7, §16.
- As built: `operon-cloudevents`, and after GW1 `operon_cloudevents::{profile, proto_data}` and `operon-events-arrow`.

## Global Constraints

Same as the M1 overview §8, plus:
- **Exactly one reporter per invocation** (D376). A test in Task 3 enforces it for every runner kind.
- **The engine never depends on a consumer** (D202). No consumer connected means no reports and no cost beyond the bounded buffer; nothing blocks on the socket.
- **No AWS account in CI.** Lambda tests run against the Runtime Interface Emulator; a real-AWS job is optional, manual, and needs the owner's credentials decision (Task 5).
- **The build machine.** One cargo build at a time, the shared target, `-j 6`, lld; `operon-runner-lambda` is built only in its own job and when its task is worked on.
- **Commit areas:** `meter`, `runner`, `ci`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **The socket envelope** is two messages, `HostMessage { oneof { HostReport report = 1; SandboxFinished finished = 2; } }` (host → consumer) and `ConsumerMessage { oneof { HostReportAck ack = 1; SandboxReadAck read_ack = 2; } }` (consumer → host), each frame a protobuf varint length prefix then the message (the standard delimited encoding). §27 names `HostReport`, `HostReportAck` and the `SandboxFinished` notice; RN1 fixes their framing | §27 §3.2 and §3.3 need both directions on one socket | Consumers written against bare `HostReport` frames break; none exist yet |
| 2 | **`host_id`** is 16 random bytes in lowercase hex, drawn at process start; `seq` starts at 1 | §27: a restart starts a new `host_id` so sequences never collide | None |
| 3 | **Reporter batching:** one `HostReport` per 1 s or per 1 000 invocations, whichever first; buffer 64 MiB of encoded reports (§27's default); full buffer drops the oldest report and counts `loam_meter_reports_dropped_total` | §27 §3.3's defaults; batching keeps frames and acks few | Configurable in `ReporterConfig` |
| 4 | **The usage header** returned by Loam's Lambda bootstrap is `x-loam-usage: v=1;cpu_usec=<n>;rss_peak_kib=<n>`. The bootstrap removes any `x-loam-usage` the tenant's handler set before adding its own; the runner removes the header before returning the response to the caller | The header is the only channel out of the sandbox on a synchronous invoke | A tenant handler that forges memory state in its own process can still lie; Ruling 5 caps the damage (Q366) |
| 5 | **The Lambda cap:** `cpu_usec ≤ billed_ms × 1 000 × vcpus`, where `vcpus = max(1, memory_mb / 1 769)` **(verify the 1 769 MB per vCPU figure in Task 0 against AWS's Lambda memory documentation)**; when the cap applies, `cpu_usec` is the cap and `cpu_estimated = true`. `billed_ms` comes from the `REPORT` line in the log tail (`Invoke` with `LogType::Tail`); without a tail (the emulator), `provider_billed_ms = 0` and no cap applies | The provider's own measurement bounds what the sandbox can claim | If the log tail is truncated before `REPORT`, the cap is skipped and `cpu_estimated` stays false; counted as `loam_runner_lambda_no_report_total` |
| 6 | **`LambdaRunner` splits control from invocation**: `LambdaControl` (deploy, undeploy) with `AwsLambdaControl` (CreateFunction / UpdateFunctionCode / PublishVersion / alias `loam-<version>`) and `StaticLambdaControl` (a pre-deployed function name; used with the emulator); invocation always goes through `aws-sdk-lambda`'s `Invoke`, with `endpoint_url` overridden for the emulator | The emulator only implements invoke | Deploy paths are tested only by the optional real-AWS job |
| 7 | **Lambda invocations carry the HTTP request as an API Gateway v2 (HTTP API) event**, so the tenant's handler is an ordinary `lambda_http` handler, and the response is the matching v2 response | `lambda_http` already maps v2 events to `http::Request`; Loam's `fetch` contract is an HTTP request (§24 D181) | Binary bodies are base64 in the event, which costs ~33% on large payloads; documented |
| 8 | **`ProcessRunner` measures CPU per process** (cgroup v2 `cpu.stat` when the runner has a delegated subtree, else `/proc/<pid>/stat` `utime + stime`), and apportions it across invocations that overlapped, setting `cpu_estimated = true` whenever more than one was in flight | Development parity with T0's apportioning (§27 §3.3) | Development only; never used for billing |
| 9 | **`WorkersRunner` and `SupervisorRunner` are not built here.** The supervisor comes with F1 (it implements the trait in F1's plan); the thin Workers runner waits for the unified auth plan and the Cloudflare document (Q373) | Scope | None |

## Carried in

From §27: the `HostReport`/`Invocation`/`HostReportAck` fields 1–11 as published, unchanged, plus §3.6's fields 12–16. From §34 §11: the `Runner` trait sketch, refined here.

## Review Focus

1. **Exactly one reporter.** Tests: Task 3 (`supervisor_kind_is_never_reported_by_host`, `external_runner_is_reported_once`).
2. **Delivery survives disconnects.** Tests: Task 2 (`resends_unacked_after_reconnect`, `ack_is_cumulative`, `dedupe_key_is_host_seq`).
3. **The engine never blocks on the meter.** Tests: Task 2 (`no_consumer_costs_nothing`, `stalled_consumer_drops_oldest_and_counts`).
4. **A tenant cannot forge the usage header past the cap.** Tests: Task 5 (`tenant_usage_header_is_replaced`, `cpu_is_capped_by_billed_duration`).
5. **Additive contract.** Tests: Task 1 (`fields_1_to_11_match_section_27`, `buf breaking`).

## File structure

```
proto/loam/meter/v1/meter.proto
crates/operon-meter/                         # new (Tasks 1–2, 6)
  Cargo.toml  build.rs
  src/{lib.rs,codec.rs,reporter.rs,buffer.rs,testing.rs,events.rs,metrics.rs}
  tests/{codec.rs,reporter.rs,events.rs}
crates/operon-runner/                        # new (Tasks 3–4)
  src/{lib.rs,types.rs,error.rs,host.rs,registry.rs,process.rs,cpu.rs,conformance.rs}
  tests/{host.rs,process.rs,conformance_process.rs}
crates/operon-lambda-bootstrap/              # new (Task 5)
  src/{lib.rs,usage.rs}
  examples/echo.rs
  tests/usage.rs
crates/operon-runner-lambda/                 # new (Task 5)
  src/{lib.rs,control.rs,invoke.rs,report_line.rs,event.rs}
  tests/{report_line.rs,rie.rs}
scripts/runner/{rie.sh,build-lambda-example.sh}
.github/workflows/ci.yml                     # jobs runner (path-filtered), runner-lambda (path-filtered, RIE)
docs/design/27-usage-hooks.md  docs/design/34-protocol-gateway-and-standards.md  CHANGELOG.md
```

### Task 0: Reconcile and check

**Files:** read §24 and §27 as merged, the status of F1 (is a supervisor or any `/run/loam/meter.sock` consumer on `main`?), GW1's state, `Cargo.toml`. Fill "Rulings made during execution".

**Checks:**
- Whether any code on `main` already defines `loam.meter.v1` or writes to `/run/loam/meter.sock`; if so, RN1 adopts it and lists the differences.
- `aws-sdk-lambda`, `aws-config`, `lambda_runtime`, `lambda_http` latest versions and licences; `cargo deny check` with them; **one measured cold build of `operon-runner-lambda`** (time and target-dir growth), recorded and the artifacts deleted.
- The Runtime Interface Emulator's latest release, its arm64 and x86 binaries, and whether `Invoke` with `LogType::Tail` returns a `LogResult` from it (expected: no).
- AWS Lambda's memory-to-vCPU rule (Ruling 5) and whether the `REPORT` line appears in the 4 KB log tail of a synchronous `Invoke`.
- Q362, Q366 and Q367's status.

**Commit:** `docs: reconcile RN1 with main`.

### Task 1: `loam.meter.v1` and its codec

**Files:** `proto/loam/meter/v1/meter.proto`, `crates/operon-meter/{Cargo.toml,build.rs,src/lib.rs,src/codec.rs,tests/codec.rs}`.

**Produces:** `meter.proto` with `HostReport` (fields 1–3), `Invocation` (fields 1–11 exactly as §27 §3.3, then 12 `runner`, 13 `region`, 14 `provider_billed_ms`, 15 `compile_usec`, 16 `overhead_usec` as §27 §3.6), `HostReportAck`, `SandboxFinished { string cgroup_path = 1; string org = 2; string namespace = 3; int64 finished_unix_ms = 4; }`, `SandboxReadAck { string cgroup_path = 1; }`, `HostMessage`, `ConsumerMessage` (Ruling 1); `Invocation` carries `option (loam.events.v1.event) = { type: "dev.loam.meter.usage.v1" };` once GW1 Task 3 has merged (until then a comment and Task 6 adds it). `codec::{write_frame(&mut impl AsyncWrite, &impl buffa::Message), read_frame::<M>(&mut impl AsyncRead, max: usize) -> Result<Option<M>, CodecError>}` with a 16 MiB frame limit.

**Tests:** `fields_1_to_11_match_section_27` (names, numbers and types against a table copied from §27); `frame_roundtrip`; `oversize_frame_is_refused`; `truncated_frame_is_an_error_not_a_hang`; `buf lint` and `buf breaking` (the package joins GW1 Task 1's gate).

**Commit:** `meter: add the loam.meter.v1 usage contract and its framing`.

### Task 2: The reporter and the test consumer

**Files:** `crates/operon-meter/src/{reporter.rs,buffer.rs,testing.rs,metrics.rs}`, `crates/operon-meter/tests/reporter.rs`.

**Produces:**

```rust
pub struct ReporterConfig { pub socket: PathBuf /* /run/loam/meter.sock */, pub flush_every: Duration /* 1 s */, pub flush_invocations: usize /* 1 000 */, pub buffer_bytes: usize /* 64 MiB */, pub reconnect_backoff: (Duration, Duration) /* 100 ms .. 10 s */ }
pub struct Reporter { /* host_id, seq, buffer, background task */ }
impl Reporter {
    pub fn start(cfg: ReporterConfig) -> Self;                 // spawns the connection task; never fails
    pub fn host_id(&self) -> &str;
    pub fn record(&self, inv: Invocation);                     // non-blocking
    pub fn sandbox_finished(&self, notice: SandboxFinished) -> oneshot::Receiver<()>; // resolves on SandboxReadAck
    pub async fn shutdown(self, grace: Duration);              // flushes, waits for acks up to grace
}
pub mod testing { pub struct Consumer { /* binds the socket, records frames, acks */ }
  impl Consumer { pub fn bind(path: &Path) -> Self; pub fn stall(&self, on: bool); pub fn disconnect(&self); pub fn received(&self) -> Vec<HostReport>; pub fn ack_policy(&self, p: AckPolicy); } }
```

**Semantics:** §27 §3.3 exactly: the consumer creates the socket and the reporter connects to it; without a consumer the reporter keeps up to `buffer_bytes` and retries connecting with backoff; on connect it resends from the oldest unacknowledged `seq`; an ack for `seq` frees every report up to it; a full buffer drops the oldest and counts it. Metrics: `loam_meter_reports_total`, `loam_meter_reports_dropped_total`, `loam_meter_buffer_bytes`, `loam_meter_connected` (0/1).

**Tests:** `reports_flow_and_are_acked`; `ack_is_cumulative`; `resends_unacked_after_reconnect`; `dedupe_key_is_host_seq` (a consumer that sees a resend can dedupe on `(host_id, seq)`); `no_consumer_costs_nothing` (no socket: `record` stays O(1), memory bounded); `stalled_consumer_drops_oldest_and_counts`; `restart_gets_new_host_id`; `sandbox_finished_waits_for_read_ack`; `shutdown_flushes_within_grace`.

**Commit:** `meter: add the host-report emitter of §27 §3.3`.

### Task 3: `operon-runner`: the trait, `RunnerHost` and the conformance kit

**Files:** `crates/operon-runner/src/{lib.rs,types.rs,error.rs,host.rs,registry.rs,conformance.rs}`, `crates/operon-runner/tests/host.rs`.

**Produces:**

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RunnerKind { Supervisor, Process, Lambda, Workers, CloudRun, ContainerApps }
impl RunnerKind { pub fn as_str(&self) -> &'static str; }       // "supervisor", "process", "lambda", …
pub struct RunnerCapabilities { pub contracts: Vec<Contract> /* Fetch, HttpPort, Static */, pub max_cpu: Duration, pub max_wall: Duration, pub max_body: usize, pub streaming: bool, pub websockets: bool, pub suspend: bool }
pub struct TenantCx { pub org: String, pub namespace: String }
pub struct InvocationCx { pub tenant: TenantCx, pub function: String, pub version: String, pub invocation_id: String, pub deadline: Instant, pub trace: Option<String> }
pub struct Artifact { pub function: String, pub version: String, pub digest: [u8; 32], pub contract: Contract, pub bytes: ArtifactBytes /* Path | Bytes */, pub memory_mb: u32, pub env: BTreeMap<String, String> }
pub struct Deployment { pub r#ref: DeploymentRef, pub runner: RunnerKind, pub created_unix_ns: i64 }
pub struct DeploymentRef { pub function: String, pub version: String, pub digest: [u8; 32], pub runner_handle: String }
pub struct InvokeRequest(pub http::Request<Bytes>);
pub struct Usage { pub cpu_usec: u64, pub cpu_estimated: bool, pub wall_usec: u64, pub provider_billed_ms: u64, pub rss_peak_bytes: Option<u64>, pub region: Option<String>, pub start_unix_ms: i64, pub end_unix_ms: i64 }
pub struct InvokeResponse { pub response: http::Response<Bytes>, pub usage: Option<Usage> }
pub enum RunnerError { NotFound(DeploymentRef), Unsupported(Contract), DeadlineExceeded, Throttled { retry_after: Option<Duration> }, Provider(String), Artifact(String), Internal(String) }
#[async_trait] pub trait Runner { /* exactly §34 §11 */ }
pub struct RunnerHost { /* Arc<dyn Runner>, Reporter */ }
impl RunnerHost { pub async fn invoke(&self, cx: &InvocationCx, dep: &DeploymentRef, req: InvokeRequest) -> Result<http::Response<Bytes>, RunnerError>; }
#[macro_export] macro_rules! runner_conformance { ($factory:expr) => { … } }   // one #[tokio::test] per case
```

**Semantics:** `RunnerHost::invoke` calls the runner; if `kind() == Supervisor`, a returned `usage` is a bug (logged at error, dropped, counted `loam_runner_double_report_total`); otherwise a missing `usage` is a bug the same way, and a present one becomes one `Invocation` with `runner = kind().as_str()`, recorded on the `Reporter`. A failed invocation that consumed CPU still reports it. Conformance cases: `deploy_is_idempotent_by_digest`; `invoke_returns_handler_response`; `usage_present_iff_not_supervisor`; `undeploy_then_invoke_is_not_found`; `deadline_is_enforced`; `concurrent_invokes_complete`; `unsupported_contract_is_refused`; `health_reports_ready`.

**Tests:** `supervisor_kind_is_never_reported_by_host`; `external_runner_is_reported_once`; `failed_invoke_still_reports_usage`; `runner_label_is_kind`; a `FakeRunner` passes `runner_conformance!`.

**Commit:** `runner: add the Runner trait, RunnerHost and the runner conformance kit`.

### Task 4: `ProcessRunner`

**Files:** `crates/operon-runner/src/{process.rs,cpu.rs}`, `crates/operon-runner/tests/{process.rs,conformance_process.rs}`, `crates/operon-runner/tests/fixtures/echo-server/` (a tiny binary built by the test, serving HTTP on `$LOAM_SOCKET`).

**Produces:** `pub struct ProcessRunner { /* root dir, optional delegated cgroup */ }` with `ProcessRunner::new(root: PathBuf, cgroup: Option<PathBuf>)`; `Contract::Fetch` only. Deploy writes the artifact under `root/<function>/<version>-<digest-hex8>/` and starts it with `LOAM_SOCKET=<dir>/sock`; invoke sends the request over the Unix socket (hyper client); undeploy sends SIGTERM, then SIGKILL after 5 s. `cpu.rs`: `CpuSource::{Cgroup(path), Proc(pid)}` with `read_usec()`; per-invocation apportioning per Ruling 8.

**Tests:** `conformance_process` (`runner_conformance!(ProcessRunner)`); `single_invocation_cpu_is_exact` (a handler that spins ~50 ms of CPU reports 40–80 ms, `cpu_estimated = false`); `overlapping_invocations_are_estimated`; `crash_restarts_on_next_invoke`; `cgroup_source_used_when_delegated` (skipped with a message when no delegated cgroup is available).

**Commit:** `runner: add the process runner for development and tests`.

### Task 5: The Lambda bootstrap and `LambdaRunner`

**Files:** `crates/operon-lambda-bootstrap/{Cargo.toml,src/lib.rs,src/usage.rs,examples/echo.rs,tests/usage.rs}`, `crates/operon-runner-lambda/{Cargo.toml,src/lib.rs,src/control.rs,src/invoke.rs,src/report_line.rs,src/event.rs,tests/report_line.rs,tests/rie.rs}`, `scripts/runner/{rie.sh,build-lambda-example.sh}`, `.github/workflows/ci.yml` (job `runner-lambda`).

**Produces:**

```rust
// operon-lambda-bootstrap
pub async fn run<F, Fut>(handler: F) -> Result<(), lambda_runtime::Error>
where F: Fn(http::Request<lambda_http::Body>) -> Fut, Fut: Future<Output = Result<http::Response<lambda_http::Body>, lambda_http::Error>>;
pub mod usage { pub const HEADER: &str = "x-loam-usage"; pub fn encode(cpu_usec: u64, rss_peak_kib: u64) -> String; pub fn decode(v: &str) -> Result<(u64, u64), UsageHeaderError>; }
// operon-runner-lambda
pub struct LambdaRunner { /* aws_sdk_lambda::Client, Arc<dyn LambdaControl>, region */ }
#[async_trait] pub trait LambdaControl: Send + Sync { async fn deploy(&self, a: &Artifact) -> Result<String /* qualified ARN or name:alias */, RunnerError>; async fn undeploy(&self, handle: &str) -> Result<(), RunnerError>; }
pub struct AwsLambdaControl { /* role ARN, arch arm64, runtime provided.al2023 */ }
pub struct StaticLambdaControl { pub function: String }
pub mod report_line { pub struct Report { pub duration_ms: f64, pub billed_ms: u64, pub memory_mb: u32, pub max_memory_mb: u32, pub init_ms: Option<f64> } pub fn parse_tail(base64_tail: &str) -> Option<Report>; }
```

**Semantics:** Rulings 4–7. The bootstrap reads `getrusage(RUSAGE_SELF)` (user + system) and `ru_maxrss` before and after the handler, strips any tenant `x-loam-usage`, and adds its own. `LambdaRunner::invoke` builds the API Gateway v2 event from the request, calls `Invoke` (`InvocationType::RequestResponse`, `LogType::Tail`), maps the v2 response back, removes `x-loam-usage` from it, parses the header and the `REPORT` line, applies the cap, and returns `Usage { region: Some(region), provider_billed_ms, … }`. Function errors (`FunctionError` set) answer 502 to the caller and still return usage. `AwsLambdaControl::deploy` zips the artifact as `bootstrap`, creates or updates the function, publishes a version and points alias `loam-<version>` at it; idempotent by the artifact digest stored in the function's tags (`loam.dev/digest`).

**Tests:** `tenant_usage_header_is_replaced` (bootstrap unit test with a handler that sets the header); `usage_header_roundtrip`; `report_line_parses` (a table of real-format `REPORT` lines, including with `Init Duration`, without, and truncated); `cpu_is_capped_by_billed_duration`; `no_report_line_skips_cap_and_counts`; `rie_invoke_returns_response_and_usage` and `rie_function_error_is_502_with_usage` (the job starts the emulator with the `echo` example built for the runner's architecture, `StaticLambdaControl`, `endpoint_url` → the emulator); `runner_conformance!` against the emulator for the cases that do not need deploy. **Optional, manual:** a `workflow_dispatch` job `runner-lambda-aws` that deploys and invokes the example in a real account, only if the owner provides credentials and a budget (record the decision in Task 0).

**Commit:** `runner: add the Lambda runner and Loam's Lambda bootstrap`.

### Task 6: Usage as CloudEvents and Arrow (needs GW1 Tasks 3–5)

**Files:** `proto/loam/meter/v1/meter.proto` (the event option on `Invocation`), `crates/operon-meter/src/events.rs`, `crates/operon-meter/tests/events.rs`.

**Produces:** `pub struct UsageEventSink { /* EventSink-style bounded queue over a StreamProducer, off unless configured */ }` with `UsageEventSink::new(target: (String, String), producer: Arc<dyn StreamProducer>, cfg)`; `pub fn usage_event(host_id: &str, seq: u64, index: usize, inv: &Invocation) -> CloudEvent` (§27 §3.6: `id` `<host_id>:<seq>:<index>`, `source` `/hosts/<host_id>`, `tenantid` `<org>/<namespace>`, `type` `dev.loam.meter.usage.v1`, `dataschema` `urn:loam:proto:loam.meter.v1.Invocation`, `datacontenttype` `application/protobuf`, `time` from `end_unix_ms`). The `Reporter` gains `with_events(UsageEventSink)`: every report is also emitted as events after it is recorded, never instead of the socket. `StreamProducer` is GW2's trait; if GW2 has not merged, RN1 defines the same trait in `operon-meter` and GW2 re-exports it (recorded as a ruling).

**Tests:** `usage_event_is_profile_valid` (GW1's `profile::validate`); `usage_event_ids_are_unique_and_stable` (a resend produces the same ids, so the event table dedupes); `usage_arrow_schema_matches_golden` (GW1's `event_schema` over `loam.meter.v1.Invocation`); `events_off_by_default`.

**Commit:** `meter: emit usage reports as CloudEvents when configured`.

### Task 7: Docs and close

**Files:** `docs/design/27-usage-hooks.md` (§3.3's framing as built, Ruling 1; §3.6 as built), `docs/design/34-protocol-gateway-and-standards.md` (§11–§12 as built), `CHANGELOG.md`.

**Tests:** the `runner` and `runner-lambda` jobs green; `cargo deny check`; `buf breaking`.

**Commit:** `docs: record RN1 as built`.

## What RN1 leaves to others

| Item | Where |
|---|---|
| `SupervisorRunner` (the node supervisor implementing `Runner`, reporting through `operon-meter`) | F1 plan (§24 §11) |
| `WorkersRunner` (thin) | after D111; the Cloudflare document's spike (Q373) |
| Cloud Run and Container Apps runners | on demand (Q367) |
| The consumer that aggregates reports, rating, the ledger, invoices, reconciliation against provider invoices | `loam-platform` (D190, D202) |
| Showback dashboards over the hooks | proposed open (Q362); after Q362 |

## PR sizes

| Task | Expected size |
|---|---|
| 1 | ~400 lines |
| 2 | ~900 lines |
| 3 | ~900 lines |
| 4 | ~700 lines |
| 5 | ~1 300 lines across two crates plus scripts |
| 6 | ~400 lines |
| 7 | docs |

## Rulings made during execution

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| — | (Task 0 fills this table) | | |
