# AP0 — App Protos, the Apps Mock and Code Generation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (package names, field names, error codes, paths), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: In progress** (2026-10-02: scaffolded, see Rulings E1–E7; planned 2026-10-01). **Slot: track AP, first; it gates AP1 Tasks 7–9, AP1a Task 5, AP2 and AP3.** Branches `ap0-t<N>`, stacked; PRs target `main`. AP0 adds protos, one mock crate, code generation and docs. It changes no engine code path. The real server implementations of these services belong to the unified auth plan (D111, Q30) and §21's D2 milestone; AP0 only fixes the contract and serves it from a mock.

**Goal:** Fix the Connect-RPC contract that the desktop app, the console's cordis services and the two mobile apps share (§37 §8, D438), serve it from a scriptable mock with seed data and scenarios (the harness `mock-harness` pattern, §37 §3.2), and generate TypeScript, Swift and Kotlin clients from it with `buf`, beside the existing connect-rust server generation (D128).

**Architecture:**
- **Four new packages under `proto/loams/`**, beside `proto/loams/live/v1/`: `instance/v1`, `devices/v1`, `approvals/v1`, `operations/v1`, plus the shared `notifications/v1` messages. Package names are `loams.*.v1`, matching what is on `main` (Q422, answered by D407; §37 §16).
- **Only unary and server-streaming RPCs.** Every `Watch*` RPC sends a snapshot, then changes, then a heartbeat every 15 s, and accepts a resume cursor (Ruling 3). No client or bidi streaming anywhere (browsers, URLSession and HTTP/1.1 proxies cannot carry it; connect-rust answers nothing on HTTP/1.1 until the request body ends).
- **`crates/loams-apps-mock`**: a connect-rust server of the four services with seed data, plus a scenario runner. It runs beside `loams-console-mock` (which stays the OpenAPI mock of §19 P10) on `127.0.0.1:8084`.
- **Generation:** the root `buf.gen.yaml` gains TypeScript (protobuf-es v2 for `@loams/proto`, consumed by `web/`), and two new templates, `buf.gen.swift.yaml` and `buf.gen.kotlin.yaml`, which the mobile repository runs against a pinned git ref of this repository (§37 §9). Generated Swift and Kotlin are not committed here.

**Tech Stack:** `buf` CLI 1.x (pinned in `web/package.json` devDependencies as `@bufbuild/buf`); `protoc-gen-es` 2.x (`@bufbuild/protobuf` 2.x); connect-rust `connectrpc` 0.9 and `connectrpc-build` for the mock (D128); buffa messages. Remote plugins for mobile: `buf.build/apple/swift` + `buf.build/connectrpc/swift`; `buf.build/protocolbuffers/java` (`lite`) + `buf.build/connectrpc/kotlin` (`generateCallbackMethods=false`, `generateCoroutineMethods=true`). Task 0 confirms every version (§37 §17 lists the ones read on 2026-10-01).

**Spec:**
- [§37 Desktop and mobile apps](../design/37-desktop-and-mobile-apps.md): §8 (the proto surface), §7.2 (pairing), §7.3 (approvals), §7.4 (push), §12 (testing).
- [§19](../design/19-console-identity-and-agents.md) §5 (principals, tokens, vending), §6 (sessions); [§21](../design/21-durable-execution.md) §6.4 (operations, D146) and §6.5 (approval gates); D111, D128, D146.
- `proto/loams/live/v1/live.proto` (style: comments, heartbeats, no bidi), `buf.yaml` (STANDARD lint), `crates/loams-live-proto/build.rs` (how connect-rust code is generated in this workspace).

## Global Constraints

Same as the M1 overview §8, plus:
- **Docs and protos first, no engine code.** No crate under `crates/loams*` other than the new `loams-apps-mock` changes.
- **The build machine.** One cargo build at a time, the shared target, `CARGO_BUILD_JOBS=4`. `loams-apps-mock` must build without the `loams` crate (it depends on `connectrpc`, buffa, `tokio`, `axum` 0.8, `serde`, `serde_yaml`, `ulid` and nothing from the engine).
- **`buf lint` (STANDARD) passes with no new `ignore_only` entry** except `RPC_RESPONSE_STANDARD_NAME` for `Watch*` RPCs, if Ruling 2 keeps a bare event type (it does not by default).
- **`buf breaking` (FILE) is enforced for the new packages from Task 6 on**, against `main`. The Live exception (not enforced in R1) is unchanged.
- **No secrets in the mock.** Tokens are fixed strings documented as fake (`mock-access-<principal>`), and the mock refuses to bind a non-loopback address (D111).
- **Commit areas:** `proto`, `mock`, `web`, `ci`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **Package names `loams.<area>.v1`**, file layout `proto/loams/<area>/v1/<area>.proto` | Matches `loams.live.v1` and `loams.stream.v1` on `main`; Connect URL paths contain the package, so the rename PR moved them from `loam.*` before the first release (Q422, D407) | A rename after release breaks every installed app |
| 2 | **Every RPC has its own `<Rpc>Request` and `<Rpc>Response`**, including `Watch*` (`WatchApprovalsResponse` with a `oneof event`) | STANDARD lint without exceptions; room to add fields | None |
| 3 | **Watch streams: snapshot, changes, heartbeat, cursor.** The first response carries `snapshot` (every matching object) and a `cursor`; later ones carry `upsert` or `remove` with a new cursor; an empty `heartbeat` every 15 s. A request with `resume_cursor` skips the snapshot when the cursor is still valid, and otherwise the server answers with a fresh snapshot and `snapshot_reset = true` | Mobile networks and proxies drop idle streams (§37 §8.3); the harness's "new generation, full baseline" reconnect is kept as the fallback | A cursor store per stream on the server; retention is the stream's (7 days for `_jobs/*`, §26 §6.6) |
| 4 | **Idempotent reads are marked `option idempotency_level = NO_SIDE_EFFECTS`**, so Connect clients may send them as HTTP GET | Cacheable, and CDN- and proxy-friendly | None |
| 5 | **Every mutating RPC takes `idempotency_key`** (a client ULID). The server maps it as D146 does for REST (`op-` + 26 hex chars of SHA-256(namespace ‖ key) where an operation results) | Mobile retries over flaky networks must not double-approve or double-revoke | None |
| 6 | **Errors are Connect codes plus a `loams.errors.v1.ErrorInfo` detail** with a stable snake_case `reason` (`approval_expired`, `approval_already_decided`, `decision_proof_invalid`, `step_up_required`, `pairing_expired`, `pairing_used`, `device_revoked`, `push_target_unknown`), matching §30 D283's error codes in spirit | Apps branch on `reason`, not on message text | None |
| 7 | **A decision on an approval needs a `decision_proof`**: a compact JWS (EdDSA or ES256) over the canonical `DecisionClaims` (approval id, decision, the approval's `revision`, `iat`, `jti`), signed by the device's user-presence key and verified against the key registered at pairing (§37 §7.3). Desktop and console decisions without a device key are accepted only when the session is younger than 5 minutes or the approval policy allows `step_up: none` | Approving a destructive operation from a lock screen must prove a person was present on a known device; a stolen bearer token alone cannot approve | Approvals from a fresh console session stay possible; the policy can require a device proof |
| 8 | **Human-readable text is rendered by the server** (`Approval.summary`, `Approval.detail_lines`), in the user's locale from `Accept-Language`, beside structured fields | Apps then show the same words on three platforms and in push notifications, and new approval kinds need no app release | Translation lives server-side |
| 9 | **The mock is scriptable from YAML scenarios** under `crates/loams-apps-mock/scenarios/`, each a timeline of `at: <ms>` steps (`emit`, `expire`, `fail_next`, `drop_stream`, `revoke_device`) | Both mobile apps, the desktop and the console run the same scenarios; the harness's `mock-harness` proved the pattern | Scenario drift from the real server; Task 7's acceptance port keeps the mock's validation equal to the server's |

## Review Focus

1. **The approval decision cannot be replayed or forged.** `revision` in `DecisionClaims`, `jti` single use, key binding. Tests: Task 3 (`decision_without_proof_is_step_up_required`, `decision_proof_replay_is_refused`, `stale_revision_is_failed_precondition`).
2. **Watch resumes lose nothing and duplicate nothing.** Tests: Task 4 (`resume_after_drop_delivers_missed`, `expired_cursor_resets_snapshot`).
3. **Pairing codes are single use, short-lived and bound to their creator.** Tests: Task 2 (`pairing_code_single_use`, `pairing_code_expires`).
4. **The mock refuses what the server will refuse** (ported validation, Ruling 9). Tests: Task 7.

## File structure

```
proto/loams/instance/v1/instance.proto
proto/loams/devices/v1/devices.proto
proto/loams/approvals/v1/approvals.proto
proto/loams/operations/v1/operations.proto
proto/loams/notifications/v1/notifications.proto
proto/loams/errors/v1/errors.proto
buf.yaml  buf.gen.yaml  buf.gen.swift.yaml  buf.gen.kotlin.yaml
crates/loams-apps-mock/{Cargo.toml,build.rs,src/{lib.rs,main.rs,seed.rs,scenario.rs,services/*.rs},seed/seed.yaml,scenarios/*.yaml,tests/*.rs}
web/packages/proto/{package.json,src/gen/**}        # @loams/proto (generated, committed)
.github/workflows/ci.yml                            # job protos (buf lint, buf breaking, buf generate diff)
docs/design/37-desktop-and-mobile-apps.md  docs/plans/README.md  CHANGELOG.md
```

### Task 0: Reconcile and pin

**Files:** read `buf.yaml`, `buf.gen.yaml`, `crates/loams-live-proto/build.rs`, `api/console/openapi.json` as on `main`; whether §26 J1 (`loams.jobs.v1`) has merged; the final numbers of D420–D439. Fill this plan's "Rulings made during execution" table.

**Checks** (record each with its command in the PR description):
- The current versions of `buf`, `protoc-gen-es`, `@connectrpc/connect`, `@connectrpc/connect-web`, connect-swift, swift-protobuf, connect-kotlin and protobuf-javalite, and the remote plugin names on buf.build. Update §37 §17 if they moved.
- Q422 (the `loams.*` proto rename) is answered (D407): use the `loams.*` prefix everywhere in this plan.
- Whether the unified auth plan has fixed claim names for device binding (`cnf.jkt`, RFC 9449). If not, AP0 uses `cnf.jkt`.

**Commit:** `docs: reconcile AP0 with main`.

### Task 1: `loams.instance.v1` and `loams.errors.v1`

**Files:** `proto/loams/instance/v1/instance.proto`, `proto/loams/errors/v1/errors.proto`, `buf.yaml` (if needed).

**Produces:**

```proto
service InstanceService {
  // No auth: what this instance is and how to sign in to it.
  rpc GetInstance(GetInstanceRequest) returns (GetInstanceResponse) { option idempotency_level = NO_SIDE_EFFECTS; }
  // The calling principal, its org, and the environments it can reach.
  rpc WhoAmI(WhoAmIRequest) returns (WhoAmIResponse) { option idempotency_level = NO_SIDE_EFFECTS; }
}
// GetInstanceResponse: instance_id (ULID, stable for the install), edition (OSS|CLOUD|BYOC),
//   server_version, api_versions (repeated string, e.g. "loams.approvals.v1"), features (map<string,bool>),
//   issuer (the OAuth authorization server URL), jwks_uri, sign_in_methods,
//   push (PushConfig: gateway_url, app_ids per platform; absent when push is off),
//   min_app_versions (map<string,string>: "ios", "android", "desktop").
// WhoAmIResponse: principal (Principal: id, kind USER|SERVICE_ACCOUNT|AGENT, display_name, email),
//   actor_chain (repeated Principal, RFC 8693 act), org (id, name),
//   environments (repeated Environment: id, project, name, namespace, protected), device (optional DeviceRef).
// errors.proto: message ErrorInfo { string reason = 1; map<string,string> metadata = 2; string hint = 3; }
```

**Semantics:** `GetInstance` mirrors the console's `GET /api/v1/instance` (§19 §3) and adds what the apps need (issuer, JWKS, push, minimum app versions). Field names follow the OpenAPI schema where both exist.

**Tests:** `buf lint`; `buf build`; a golden JSON of a `GetInstanceResponse` in `crates/loams-apps-mock/tests/golden/instance.json` matched in Task 5.

**Commit:** `proto: add loams.instance.v1 and loams.errors.v1`.

### Task 2: `loams.devices.v1`

**Files:** `proto/loams/devices/v1/devices.proto`.

**Produces:**

```proto
service DeviceService {
  // A signed-in user starts pairing a phone; returns the QR payload (§37 §7.2).
  rpc CreatePairing(CreatePairingRequest) returns (CreatePairingResponse);
  // Lists the caller's devices (an admin may list a member's).
  rpc ListDevices(ListDevicesRequest) returns (ListDevicesResponse) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc RenameDevice(RenameDeviceRequest) returns (RenameDeviceResponse);
  // Revokes the device's refresh token, its push targets and its decision key, at once.
  rpc RevokeDevice(RevokeDeviceRequest) returns (RevokeDeviceResponse);
  // Registers where to push and the HPKE key to seal payloads to.
  rpc RegisterPushTarget(RegisterPushTargetRequest) returns (RegisterPushTargetResponse);
  rpc UnregisterPushTarget(UnregisterPushTargetRequest) returns (UnregisterPushTargetResponse);
  rpc GetNotificationPreferences(GetNotificationPreferencesRequest) returns (GetNotificationPreferencesResponse) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc SetNotificationPreferences(SetNotificationPreferencesRequest) returns (SetNotificationPreferencesResponse);
  rpc SendTestNotification(SendTestNotificationRequest) returns (SendTestNotificationResponse);
}
// CreatePairingRequest: device_name_hint, environments (optional narrowing), idempotency_key.
// CreatePairingResponse: pairing_id, qr_payload (string: the JSON of §37 §7.2.1), user_code (8 digits), expires_at (5 min).
// Device: id, name, platform (IOS|ANDROID|DESKTOP), model, app_version, created_at, last_seen_at,
//   decision_key_thumbprint (RFC 7638 JWK thumbprint), push_targets (repeated PushTargetRef), revoked_at.
// RegisterPushTargetRequest: provider (APNS|FCM|UNIFIEDPUSH|WEBPUSH), token_or_endpoint,
//   app_id (bundle id / package name), environment (APNS sandbox|production),
//   hpke_public_key (bytes, X25519, RFC 9180 suite DHKEM(X25519)+HKDF-SHA256+ChaCha20Poly1305), idempotency_key.
// NotificationPreferences: per category (APPROVALS, OPERATIONS, JOBS, RUNS, SECURITY) on/off,
//   environments filter, quiet_hours (start, end, tz), approvals_bypass_quiet_hours (default true).
```

**Semantics:** **`user_code` redemption:** the pairing grant accepts exactly one of `code` (from the QR) or `user_code` (typed), both identifying the same pairing; `user_code` is valid only during the pairing's 5 minutes, and 5 failed `user_code` attempts against an instance within that window burn the pairing (`pairing_used`) and are rate-limited per client address. The pairing exchange itself is **not** an RPC here: the phone redeems the pairing at the OAuth token endpoint with the extension grant `urn:loams:params:oauth:grant-type:pairing` (RFC 6749 §4.5) and a DPoP proof (RFC 9449), so tokens keep one issuance path (§19 §5.2). This task documents that grant in `devices.proto`'s header comment and in §37 §7.2; the unified auth plan implements it.

**Tests:** `buf lint`; `buf build`.

**Commit:** `proto: add loams.devices.v1`.

### Task 3: `loams.approvals.v1`

**Files:** `proto/loams/approvals/v1/approvals.proto`.

**Produces:**

```proto
service ApprovalService {
  rpc ListApprovals(ListApprovalsRequest) returns (ListApprovalsResponse) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc GetApproval(GetApprovalRequest) returns (GetApprovalResponse) { option idempotency_level = NO_SIDE_EFFECTS; }
  // Snapshot of pending approvals the caller may decide, then changes (Ruling 3).
  rpc WatchApprovals(WatchApprovalsRequest) returns (stream WatchApprovalsResponse);
  rpc DecideApproval(DecideApprovalRequest) returns (DecideApprovalResponse);
}
// Approval: id, revision (uint64, bumps on every change), operation_id (D146), promise_id (§21 §6.5),
//   kind (string: "collection.drop", "namespace.drop", "erasure", "restore", "agent.action", "key.create", ...),
//   environment (EnvironmentRef), requested_by (Principal), actor_chain (repeated Principal),
//   summary (string), detail_lines (repeated string), target (map<string,string>), risk (LOW|MEDIUM|HIGH|DESTRUCTIVE),
//   policy (ApprovalPolicy: required_approvals, approver_roles, requester_may_approve=false, step_up DEVICE|SESSION|NONE),
//   decisions (repeated Decision: by, decision, reason, at, device), state (PENDING|APPROVED|REJECTED|EXPIRED|CANCELED),
//   created_at, expires_at (default 72 h, §21 §6.5).
// DecideApprovalRequest: approval_id, revision, decision (APPROVE|REJECT), reason (required for REJECT and for DESTRUCTIVE),
//   decision_proof (string, compact JWS, Ruling 7), idempotency_key.
// WatchApprovalsRequest: environments (filter), states (default PENDING), resume_cursor.
// WatchApprovalsResponse: oneof event { Snapshot snapshot; Approval upsert; string remove; Heartbeat heartbeat; },
//   cursor, snapshot_reset.
```

**Semantics:** an approval is the approval promise of §21 §6.5 plus its policy; deciding it settles the promise (approve → resolve, reject → reject) once the policy's count is met. `requester_may_approve` defaults to false: the principal (or the user an agent acts for) who requested the operation cannot approve it (Q432 confirms).

**Tests:** `buf lint`; `buf build`. Behaviour tests are in Task 5 against the mock: `decision_without_proof_is_step_up_required`, `decision_proof_replay_is_refused`, `stale_revision_is_failed_precondition`, `requester_cannot_approve`, `expired_is_failed_precondition_approval_expired`, `second_decision_is_already_decided`.

**Commit:** `proto: add loams.approvals.v1`.

### Task 4: `loams.operations.v1` and `loams.notifications.v1`

**Files:** `proto/loams/operations/v1/operations.proto`, `proto/loams/notifications/v1/notifications.proto`.

**Produces:**

```proto
service OperationsService {          // the Connect face of D146's REST operations API
  rpc GetOperation(GetOperationRequest) returns (GetOperationResponse) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc ListOperations(ListOperationsRequest) returns (ListOperationsResponse) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc WatchOperations(WatchOperationsRequest) returns (stream WatchOperationsResponse);
  rpc CancelOperation(CancelOperationRequest) returns (CancelOperationResponse);
}
// Operation: the JSON of §21 §6.4 as a message (id, kind, namespace, target, state, progress, created_at,
//   updated_at, result (google.protobuf.Struct), error (code, message)), plus approval_id when awaiting_approval.
service NotificationService {        // the in-app inbox; push is only a wake-up (§37 §7.4)
  rpc ListNotifications(ListNotificationsRequest) returns (ListNotificationsResponse) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc WatchNotifications(WatchNotificationsRequest) returns (stream WatchNotificationsResponse);
  rpc MarkRead(MarkReadRequest) returns (MarkReadResponse);
}
// Notification: id (ULID), category, cloudevent_type ("io.loams.dev.approval.requested", ...), subject (string),
//   title, body (server-rendered), ref (oneof approval_id | operation_id | job_ref | run_ref), environment, created_at, read_at.
```

**Semantics:** both packages are views over state that already exists: operations are durable promises (D146), and notifications are a per-user projection of CloudEvents (`io.loams.dev.*` types, §37 §7.4) kept for 30 days (**estimate**). Job and durable-run events reference `loams.jobs.v1` objects by id only, so AP0 does not depend on J1.

**Tests:** `buf lint`; `buf build`; `buf breaking --against '.git#branch=main'` passes (new files only).

**Commit:** `proto: add loams.operations.v1 and loams.notifications.v1`.

### Task 5: `loams-apps-mock`

**Files:** `crates/loams-apps-mock/**`, workspace `Cargo.toml` (member), `deny.toml` only if a new licence appears (none expected).

**Produces:**

```rust
pub struct MockConfig { pub listen: SocketAddr /* 127.0.0.1:8084 */, pub seed: Seed, pub scenario: Option<Scenario>, pub clock: MockClock }
pub struct MockHandle { pub addr: SocketAddr, /* ... */ }
impl MockHandle { pub async fn stop(self); pub fn scenario_done(&self) -> impl Future<Output = ()>; }
pub async fn serve(config: MockConfig) -> anyhow::Result<MockHandle>;   // refuses non-loopback
pub fn load_seed(yaml: &str) -> anyhow::Result<Seed>;
pub fn load_scenario(yaml: &str) -> anyhow::Result<Scenario>;
// main.rs: `loams-apps-mock [--listen 127.0.0.1:8084] [--scenario <name|path>] [--speed 1.0]`
```

**Semantics:** stateful, unlike `loams-console-mock` (approvals change state when decided, so the apps' flows can be tested end to end). Fake bearer tokens map to seed principals. Decision proofs are verified for real against seed device keys (Ed25519 test keys checked into `seed/keys/`, labelled test-only). Serves Connect, gRPC and gRPC-Web on one listener (connect-rust). CORS allows `http://localhost:5173`, `tauri://localhost` and `http://tauri.localhost` for the console and desktop dev builds. Scenarios: `approvals-basic`, `approval-expiry`, `stream-drop-and-resume`, `device-revoked`, `operation-progress`, `notification-burst`.

**Tests** (`tests/*.rs`, real sockets, a connect-rust client): every RPC of Tasks 1–4 answers over Connect (JSON and binary), gRPC and gRPC-Web; Task 3's six behaviour tests; `resume_after_drop_delivers_missed`; `expired_cursor_resets_snapshot`; `heartbeat_every_15s` (with `MockClock`); `pairing_code_single_use`; `pairing_code_expires`; `user_code_redeems_same_pairing`; `user_code_burns_after_5_failures`; `code_and_user_code_together_is_invalid_argument`; `revoke_device_ends_its_streams`; `non_loopback_is_refused`.

**Commit:** `mock: add loams-apps-mock for the app protos`.

### Task 6: Code generation and the CI job

**Files:** `buf.gen.yaml` (add the new packages to the TypeScript output `web/packages/proto/src/gen`, keep the Live output unchanged), `buf.gen.swift.yaml`, `buf.gen.kotlin.yaml`, `web/packages/proto/package.json` (`@loams/proto`, private until publishing starts, D406), `.github/workflows/ci.yml` (job `protos`, path-filtered on `proto/**`, `buf.*.yaml`).

**Semantics:** the CI job runs `buf lint`, `buf breaking --against "https://github.com/${{ github.repository }}.git#branch=main"` for the new packages, `buf generate` and fails on a diff in `web/packages/proto/src/gen`, and runs `buf generate --template buf.gen.swift.yaml` and `--template buf.gen.kotlin.yaml` into a temporary directory (outside `/tmp` on the dev machine; the runner's temp is fine) and compiles nothing: it only checks that the templates still generate. The mobile repository compiles them (AP2 Task 1, AP3 Task 1).

**Tests:** the CI job; `pnpm -C web typecheck` with a tiny `web/packages/proto/test/smoke.ts` that builds a client for each service with `createClient` from `@connectrpc/connect`.

**Commit:** `ci: generate and check the app protos`.

### Task 7: Acceptance port and docs

**Files:** `crates/loams-apps-mock/src/acceptance.rs`, `docs/design/37-desktop-and-mobile-apps.md` (as-built notes), `docs/plans/README.md`, `CHANGELOG.md`.

**Semantics:** collect every validation rule the services apply (required fields, `reason` rules, revision checks, proof checks, pairing limits) into one `acceptance` module that the mock uses and that the real server will call too, so the mock refuses exactly what the server refuses (the harness's `QuestionAcceptance.kt` lesson, §37 §3.2). Write the as-built section.

**Tests:** `acceptance_table_is_exhaustive` (every request message field marked required in comments has a rule); the behaviour tests from Task 5 rerun through `acceptance`.

**Commit:** `docs: document the app protos and close AP0`.

## Rulings made during execution

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| E1 | **Scaffold first (2026-10-02, owner: "do suggested for all").** Tasks 1–4 and 6 are complete; Task 5's mock serves every RPC with seed data, real decision rules, revisions, idempotency keys and resumable watch streams; decision-proof verification, push targets, notification preferences, operation cancel and the YAML scenario runner answer `unimplemented` with reason `not_implemented` and are the next AP0 steps (see `crates/loams-apps-mock/README.md`) | Unblocks AP1a, AP1 and the mobile scaffold now; the stubs refuse rather than accept, so nothing trusts an unverified proof | The remaining Task 5 tests (`decision_proof_replay_is_refused`, the pairing-code limits, `heartbeat_every_15s` with a mock clock, the scenarios) land in a follow-up PR |
| E2 | **The seed is Rust code** (`Seed::demo`), not `seed/seed.yaml` | The workspace has no YAML crate (`serde_yaml` is unmaintained); a typed seed cannot drift from the protos | Scenarios still want YAML (Ruling 9); the follow-up picks a maintained YAML crate through `deny.toml` |
| E3 | **A separate template, `buf.gen.apps.yaml`, writes `@loams/proto`**; the root `buf.gen.yaml` is narrowed to `proto/loams/live/v1` | A buf v2 template's inputs apply to every plugin, so one template cannot send live to `sdks/live-typescript` and the app packages to `web/packages/proto`; with `clean: true` each output directory has one owner | None |
| E4 | **`buf breaking` runs over the whole module against `main`, with `proto/loams/live` in `breaking.ignore`** | `--path` filters fail when the base has none of the files (the first PR); the ignore keeps R1's "not enforced for Live" | A live breaking change is still not caught, as before |
| E5 | **How `loams-mobile` consumes the protos: a git tag of this repository, not a BSR module.** Tags `app-protos/v<semver>` on `main`; the mobile repo pins the tag and its commit SHA in `conformance/proto-ref.lock` and runs `buf generate "https://github.com/<org>/<repo>.git#tag=app-protos/v0.1.0" --template buf.gen.swift.yaml` (or `buf.gen.kotlin.yaml`, copied from this repository) with `--path proto/loams/{instance,devices,approvals,operations,notifications,errors}`. GitHub redirects the URL across the move to `ostrium-labs` | Needs no BSR account or token (an owner action) and the repository is public; a tag is immutable enough with the SHA in the lock file | Moving to a BSR module later (`buf.build/ostrium-labs/loams`) is a template change in the mobile repo |
| E6 | **New reasons:** `approval_stale_revision`, `reason_required`, `invalid_decision`, `not_implemented` join Ruling 6's list (in `errors.proto`) | Each refusal the acceptance module makes has its own stable reason | None |
| E7 | **The Swift and Kotlin templates are smoke-generated in CI with `continue-on-error`** | buf.build's remote plugins are rate-limited without a token (seen locally: `resource_exhausted`) | A template error shows only in the step log until the mobile repo's build catches it |
