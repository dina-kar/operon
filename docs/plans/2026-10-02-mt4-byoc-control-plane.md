# MT4 — The Open Multitenant BYOC Control Plane with GitOps Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, ports, defaults), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: Task 8 guard implemented; remaining Tasks 0–7 and 9 planned** (2026-10-02). **Track MT** (design [§41](../design/41-multitenant-byoc-control-plane.md), D540–D559, recorded in the [decision log](../design/13-decision-log.md); the owner's open-core ruling of 2026-10-02). Adds what MT1 to MT3 leave out: the control plane itself, tenant onboarding through Git, the BYOC agent, upgrade rings, the limits API and the no-metering guard. Depends on MT3 Task 1 (waves and `Application`s), RN1 Task 3 (`InvocationObserver`), `loams-operator` (D185) and §18's `ControlStore` (M2). **No metering, billing, plan or commercial API is built here, and none may be (D541, D550).** Branches `mt4-t<N>`, stacked; PRs target `dev`. New crates are outside `loams`'s default build.

**Goal:**
- `loams-control`: the hub's operations API and limits API (`loams.control.v1`) over the `ControlStore`, with OpenFGA authorization (§41 §4).
- Tenant onboarding as a Git commit: the Git writer, the `tenants/<org>/` layout, an Argo CD `ApplicationSet`, and the operator's `Tenant` reconcile (§41 §6).
- `loams-byoc-agent` and the enrolment flow (§41 §7).
- Release channels and upgrade rings (§41 §8).
- The limits API with approximate enforcement state (§41 §9).
- The operator view in the console and the CLI commands.
- `scripts/ci/no-metering.sh` and the exit-gate e2e (D552, D558).

**Architecture:**
- **Git is the write path.** The control plane writes records to the `ControlStore` and an outbox; the Git writer renders files; Argo CD or Flux applies them; the operator reconciles and reports. Nothing here runs `kubectl apply` against a tenant cluster.
- **Pull, not push, for BYOC.** The agent connects outbound; the cluster's own Argo CD pulls Git. No hub credential into a customer's cluster.
- **Enforcement is not metering.** Counters exist to admit or reject; they are approximate, in memory or refreshed, and never exported as usage (§41 §9.2).
- **One seam to the private platform.** The limits API, the operations API, the tenants repository and `InvocationObserver`; nothing else (D551).

**Tech Stack:** Rust 1.97.1, edition 2024, workspace lints. `connectrpc` for services (D362), `tokio`, `rustls` and `rcgen` (the agent CA), `gix` for the Git writer (**or** a `git` subprocess behind a trait; Task 0 decides on a measured basis), `kube` (the operator's), `openfga` client from MT1, `serde_yaml`. Test tools: k3d, Argo CD (pinned by MT3), Forgejo for the Git host in CI, `kubeconform`. Task 0 checks versions and licences, and `cargo deny` must pass.

**Spec:**
- [§41](../design/41-multitenant-byoc-control-plane.md) (all); [§18](../design/18-metastore-backends-and-router.md) §5 to §8 (D63 to D65, D98, D103); [§38](../design/38-knative-authentik-gitops.md) §3.3, §4, §6; [§25](../design/25-clever-cloud-stack.md) §5, §6; [§27](../design/27-usage-hooks.md) §3.7.
- [open-core.md](../open-core.md).

## Global Constraints

Same as the M1 overview §8, plus:
- **No billing, no plan, no price, no invoice, no usage record** in any type, table, proto field, metric or endpoint added here. Review rejects a field named `plan`, `price`, `invoice`, `credit`, `meter`, `billable` or `usage` (the limits API's `used` is *enforcement state* and is documented as such). `scripts/ci/no-metering.sh` (Task 8) enforces the names.
- **No endpoint that spends money, changes a plan or reads billing-grade usage** (D550).
- **The dependency runs one way** (D551). No crate may depend on anything named `loam-platform*`; a `cargo deny` ban and the guard check it.
- **Idempotent mutations.** Every mutating RPC takes a `client_token` and replays return the first result.
- **Deterministic rendering.** The Git writer produces byte-identical files from the same records (a golden test per resource).
- **Agents cannot hold operator roles.** The cross-org operator relation cannot be granted to an `agent` principal; a test in Task 2 enforces it.
- **The build machine.** One cargo build at a time, the shared target, `-j 6`, lld; k3d and Forgejo run only in CI jobs, never alongside a cargo build on the build machine.
- **Commit areas:** `control`, `byoc`, `gitops`, `operator`, `console`, `cli`, `ci`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **Crates:** `loams-control` (service + store access), `loams-control-proto` (generated types for `loams.control.v1`), `loams-gitops` (the renderer and the Git writer, no network service), `loams-byoc-agent`; the operator's `Tenant` CRD is added to the existing `loams-operator`. Protos live in `proto/loams/control/v1/` under `buf breaking` | D542; separable build and CI | Splitting later is mechanical |
| 2 | **Resources:** `Org`, `Namespace`, `Cluster` (`kind`: `managed` or `byoc`), `Enrolment`, `ReleaseChannel`, `Release`, `Rollout`, `Limits`. Ids are ULIDs; names are unique within the parent | §41 §4 | Renames are breaking; settle in Task 1 |
| 3 | **Repository layout** exactly as §41 §6 (`tenants/<org>/{org.yaml,clusters.yaml,namespaces/<namespace>.yaml,kustomization.yaml}`); a namespace gets a file only when it needs cluster resources (D544) | Argo CD object count stays O(orgs) | A later change re-renders everything once |
| 4 | **ApplicationSet:** a Git directory generator over `tenants/*` with a `clusters` matrix from `clusters.yaml`; sync policy automated with `prune: true` and `selfHeal: true`; `syncOptions: [PrunePropagationPolicy=foreground]`; a `resources-finalizer` | §41 §6 | Wrong prune behaviour is a values change |
| 5 | **Namespace deletion guard:** the operator refuses to delete a Kubernetes namespace unless the namespace's erasure receipt (§18 §9) exists in the `ControlStore`; the condition `ErasurePending` is reported | An accidental directory delete must not lose data | A stuck namespace needs a manual receipt |
| 6 | **Agent CA:** an in-process CA (`rcgen`, ECDSA P-256) whose key is in the deployment's secret store (Dapr secrets, D189); client certificates last 24 hours and renew at 12; enrolment tokens are 32 random bytes, single use, 1 hour; only a hash is stored | §41 §7.1 | Shorter or longer lifetimes are a config change |
| 7 | **Agent commands** are the closed set of §41 §7.2 as a protobuf `oneof`; an unknown command is refused and logged; `SetLimits` carries a control-plane signature (Ed25519 over the canonical bytes) the agent verifies | No shell, no passthrough | Adding a command is a deliberate proto change |
| 8 | **Release channels** `canary`, `early`, `stable`; rings 0 to 2 and the BYOC window ring as §41 §8.1; promotion = fast-forward of the next channel branch; gates are named checks recorded as `GateResult`s in the `ControlStore`; the first implementation uses **one ApplicationSet per ring** (the fallback of §41 §8.2) and switches to `RollingSync` only if Task 0 finds it stable | Nothing alpha on the critical path | A later switch is a generator change |
| 9 | **Limits record:** `Limits { requests_per_s, ingest_bytes_per_s, concurrent_queries, storage_bytes, unapplied_records, unapplied_bytes, metadata_ops_per_s, k8s_cpu_millis, k8s_memory_bytes, k8s_pods, knative_max_scale }` with unset meaning "deployment default"; `EnforcementState { limit, used_approx, window_s, as_of }` per limit | §41 §9.1, D65, D98, D86, D443 | Adding a field is additive |
| 10 | **Cross-org operator admin** is the OpenFGA relation `operator` on the object `deployment:<id>`; SCIM and enforced SSO are Authentik blueprints (`deploy/authentik/blueprints/scim.yaml`, `enforced-sso.yaml`) under MT1's guard (D458, D553) | Reuses MT1 | Moving the relation later is a model migration |

## Carried in

From §18 §6: the `ControlStore` and D65's quota record. From MT1: the OpenFGA model and the Authentik blueprints. From MT2: the operator's per-namespace tenancy (D443). From MT3: the waves, `Application`s and health checks. From RN1 Task 3: `InvocationObserver`.

## Review Focus

1. **Nothing billing-grade.** Tests: Task 8 (`guard_rejects_meter_names`, `guard_rejects_platform_dependency`); the field-name review rule above.
2. **Git is the only write path.** Tests: Task 4 (`control_plane_never_applies_to_cluster`: the service has no `kube` client; the e2e shows the namespace appears only through Argo CD).
3. **Deterministic, replayable rendering.** Tests: Task 4 (`render_is_deterministic`, `rebuild_from_store_matches_repo`).
4. **A stolen BYOC certificate is bounded.** Tests: Task 3 (`cert_scoped_to_cluster`, `revoked_cert_is_refused_at_gateway`, `enrolment_token_is_single_use`).
5. **Promotion stops on a failed gate.** Tests: Task 6 (`failed_gate_blocks_promotion`, `pause_stops_ring`).
6. **Enforcement survives the platform being down.** Tests: Task 5 (`limits_remain_when_writer_absent`).

## File structure

```
proto/loams/control/v1/{control.proto,limits.proto,agent.proto}
crates/loams-control-proto/
crates/loams-control/                       # Tasks 1–2, 5, 6
  src/{lib.rs,store.rs,authz.rs,orgs.rs,namespaces.rs,clusters.rs,limits.rs,releases.rs,outbox.rs,server.rs}
  tests/
crates/loams-gitops/                        # Task 4
  src/{lib.rs,render.rs,layout.rs,writer.rs,sign.rs}
  tests/{golden/,render.rs}
crates/loams-byoc-agent/                    # Task 3
  src/{lib.rs,enrol.rs,streams.rs,commands.rs,observe_only.rs}
  tests/
crates/loams-operator/src/tenant.rs         # Task 4 (existing crate)
deploy/gitops/appset-tenants.yaml  deploy/gitops/appset-rings/  deploy/authentik/blueprints/{scim.yaml,enforced-sso.yaml}
deploy/helm/loams-stack/charts/control/     # chart for loams-control and the BYOC profile
web/apps/console/src/pages/operator/        # Task 7
scripts/ci/no-metering.sh                   # Task 8
.github/workflows/ci.yml                    # jobs control (path-filtered), mt4-e2e (k3d + Forgejo)
docs/design/41-multitenant-byoc-control-plane.md  docs/design/13-decision-log.md  CHANGELOG.md
```

## Tasks

### Task 0: Reconcile and check

**Checks:** what is on `main` of `loams-operator`, the `ControlStore` (M2), MT1 to MT3 and RN1 Task 3; Argo CD's current `ApplicationSet` `RollingSync` status in the pinned version (**D545 verify**); Argo CD commit signature verification options (GPG, SSH, gitsign) in the pinned version (Q556); `gix` push support versus a `git` subprocess (measure: a commit and push of 1 000 files on the build machine); Forgejo's API for opening pull requests and its OIDC with Authentik; licences of every new dependency (`cargo deny`).

**Produces:** the findings, recorded in "Rulings made during execution", and decisions on Q540, Q541, Q556, Q558, Q559 at their defaults unless a finding changes them.

**Tests:** none. **Commit:** `docs: MT4 task 0 findings`.

### Task 1: `loams.control.v1`, the crates and the store

**Files:** `proto/loams/control/v1/*.proto`, `crates/loams-control-proto/`, `crates/loams-control/{Cargo.toml,src/{lib.rs,store.rs,outbox.rs}}`.

**Produces:** the resources of Ruling 2 as protobuf messages; `ControlStore` extensions (tables or keys for `Cluster`, `Enrolment`, `ReleaseChannel`, `Release`, `Rollout`, `GateResult`, `Limits`) on every metastore backend that supports `ControlStore`; the **outbox** (a row per mutation that the Git writer consumes, with a monotonic sequence and `processed` marker, as D66's outbox does).

**Tests:** `proto_roundtrip` for every message; `buf lint` and `buf breaking`; `store_conformance` (the `ControlStore` conformance suite extended with the new tables, run on the in-memory backend and on the TiKV backend job); `outbox_is_ordered_and_idempotent`; `names_unique_within_parent`.

**Commit:** `control: add loams.control.v1 and the control store tables`.

### Task 2: The operations API and authorization

**Files:** `crates/loams-control/src/{authz.rs,orgs.rs,namespaces.rs,clusters.rs,server.rs}`, `deploy/authentik/blueprints/{scim.yaml,enforced-sso.yaml}`.

**Produces:** `OrgService`, `NamespaceService`, `ClusterService` (Create, Get, List with pagination, Update, Delete; `client_token` idempotency); OpenFGA checks per call with the relations `org#admin`, `org#member`, `cluster#operator`, `deployment#operator` (Ruling 10); per-principal rate limits on `CreateOrg` and `CreateNamespace` and a per-deployment tenant cap, both configurable; service credentials as mTLS plus an attenuated Biscuit (Q542); audit events for every mutation (D221) naming the `act` chain; the SCIM and enforced-SSO blueprints (open, D553, Q545).

**Tests:** `create_is_idempotent_by_token`; `unauthorized_is_denied_per_object`; `agent_cannot_hold_operator` (granting `deployment#operator` to an `agent` principal fails); `tenant_cap_and_rate_limit_enforced`; `audit_event_names_actor_chain`; `scim_and_sso_blueprints_pass_the_enterprise_guard` (MT1's lint: no `authentik_enterprise*` models); `no_endpoint_named_plan_or_billing` (a reflection test over the service descriptors).

**Commit:** `control: add the operations API with authorization`.

### Task 3: The BYOC agent and enrolment

**Files:** `crates/loams-byoc-agent/*`, `crates/loams-control/src/clusters.rs` (the CA and the streams), `proto/loams/control/v1/agent.proto`, `deploy/helm/loams-stack/charts/control/` (the BYOC profile).

**Produces:** `Enrolment` creation (token per Ruling 6); the agent's CSR flow and certificate renewal; the heartbeat stream (versions, Argo CD application conditions, enforcement summaries) and the command stream with the closed command set (Ruling 7); `observe-only` mode; revocation list pushed on the heartbeat and checked at the gateway; the air-gapped bundle format (a tarball of the release and tenants repositories plus a manifest) and `loams cluster bundle` (Q550); `loams-meta-remote` is **not** built here (M2.x plan, Q551).

**Tests:** `enrolment_token_is_single_use`; `token_expires_after_one_hour`; `cert_scoped_to_cluster` (a cluster cannot read another cluster's orgs); `cert_renews_before_expiry`; `revoked_cert_is_refused_at_gateway`; `unknown_command_is_refused`; `set_limits_requires_valid_signature`; `observe_only_accepts_no_commands`; `agent_has_no_inbound_listener` (the agent binary opens no listening socket); `airgap_bundle_roundtrip`.

**Commit:** `byoc: add the outbound-only agent, enrolment and the command set`.

### Task 4: Tenant onboarding through Git

**Files:** `crates/loams-gitops/*`, `crates/loams-control/src/outbox.rs` (the consumer), `crates/loams-operator/src/tenant.rs`, `deploy/gitops/appset-tenants.yaml`.

**Produces:** the deterministic renderer for the layout of Ruling 3; the Git writer (commit to a branch, then merge directly or open a pull request per policy, with signed commits per Q556 and the bot identity); the `ApplicationSet` of Ruling 4; the operator's `Tenant` and `Namespace` reconcile (Kubernetes namespace `loams-ns-<namespace>` with `NetworkPolicy`, `ResourceQuota`, `LimitRange`, labels, the Authentik group binding through a blueprint instance, OpenFGA tuples, the directory upsert) with status conditions `Ready`, `Degraded`, `Drift`, `ErasurePending` (Ruling 5); the directory entry flips to `active` only on `Ready`; offboarding as in §41 §6.

**Tests:** `render_is_deterministic` (golden files); `rebuild_from_store_matches_repo`; `control_plane_never_applies_to_cluster`; `namespace_active_only_after_ready`; `delete_waits_for_erasure_receipt`; `directory_only_namespace_has_no_file`; `drift_is_reported_not_silently_fixed_for_unowned_objects`; **e2e `onboard_org_on_k3d`** (Forgejo + Argo CD + the operator on k3d: create an org and a namespace by API, see the commit, the `Application`, the Kubernetes namespace with `NetworkPolicy` and `ResourceQuota`, and the directory entry `active`).

**Commit:** `gitops: onboard tenants through the tenants repository`.

### Task 5: The limits API and enforcement state

**Files:** `crates/loams-control/src/limits.rs`, `proto/loams/control/v1/limits.proto`, the gateway and operator limits consumers (existing crates, small changes).

**Produces:** `LimitsService` (`PutLimits`, `GetLimits`, `GetEnforcementState`) per org and per namespace (Ruling 9); distribution: the directory push carries limits to gateways; the operator derives `ResourceQuota`, `LimitRange` and Knative `max-scale` from the record (D443); `EnforcementState.used_approx` read from the enforcement counters of §41 §9.1 (token-bucket fill, semaphore occupancy, the periodic storage figure, the API server's quota status); documentation that the figure is approximate and not billing-grade. Writers: operator credentials and a service credential for an external plan mapping; **agent principals cannot write limits**.

**Tests:** `limits_roundtrip`; `gateway_enforces_new_limit_within_one_push_interval`; `operator_derives_resource_quota`; `limits_remain_when_writer_absent` (stop the writer; the last limits keep enforcing); `agent_cannot_write_limits`; `over_quota_gets_429_and_writes_no_usage_record` (the test asserts no new `ControlStore` row besides enforcement state); `enforcement_state_is_labelled_not_billing_grade` (the field description and the console label exist).

**Commit:** `control: add the limits API and enforcement state`.

### Task 6: Release channels and upgrade rings

**Files:** `crates/loams-control/src/releases.rs`, `deploy/gitops/appset-rings/`, gate definitions under `deploy/gitops/gates/`.

**Produces:** `ReleaseService` (`PublishRelease` with `compatibleWith` and `noDowngrade` markers, `Promote`, `Pause`, `Resume`, `Rollback`, `GetRollout`); the channel branches; one `ApplicationSet` per ring selecting clusters by label `loams.dev/ring`; the gate runner (Argo CD health, the e2e smoke suite, the conformance suites of D60 and D63, error-budget check from open metrics) recording `GateResult`s; promotion by fast-forward only after the previous ring's gate and soak time pass; automatic `Pause` on a failed health check for a cluster in a ring (Q548); the operator's version-skew refusal; BYOC maintenance windows (the BYOC ring syncs only inside the window).

**Tests:** `failed_gate_blocks_promotion`; `soak_time_is_enforced`; `pause_stops_ring`; `rollback_is_a_revert`; `no_downgrade_marker_blocks_downgrade`; `skew_beyond_compatible_with_is_refused`; `byoc_ring_syncs_only_in_window`; `ring_batches_are_at_most_ten_percent`.

**Commit:** `control: add release channels and upgrade rings`.

### Task 7: The operator view and the CLI

**Files:** `web/apps/console/src/pages/operator/*`, `crates/loams-cli/src/{org,cluster,rollout,limits}.rs` (existing CLI crate, new subcommands).

**Produces:** console pages for orgs and namespaces, clusters and BYOC enrolments (with the one-time token shown once), rollouts and rings, and enforcement state with the label "enforcement state, not billing-grade"; CLI commands `loams org create|list|delete`, `loams cluster enrol|list|bundle`, `loams rollout promote|pause|resume|rollback`, `loams limits get|put`. No page shows money, a plan or an invoice. The operator view requires `deployment#operator`.

**Tests:** console component tests (empty, loading, error, the token shown once); a snapshot test that no string in the operator view matches `/plan|price|invoice|billing|credit/i`; CLI golden-output tests; `operator_view_requires_operator_relation`.

**Commit:** `console: add the operator view and the control-plane CLI`.

### Task 8: The no-metering guard and `InvocationObserver` conformance

**Files:** `scripts/ci/no-metering.sh`, `.github/workflows/ci.yml`, `deny.toml`, `CONTRIBUTING.md` (a short section).

**Produces:** the guard of D552: it fails if `loams.meter`, `meter.sock`, `HostReport`, `x-loams-usage` or `loams_meter_` appears in tracked files outside the allowlist (Q554: `docs/design/13-decision-log.md`, `docs/design/_pending/`, `docs/design/27-usage-hooks.md`, `docs/design/41-*.md`, `docs/open-core.md`, `docs/plans/2026-10-0*-rn1-*.md`, `docs/plans/2026-10-02-mt4-*.md`, `CHANGELOG.md`); it fails if any `Cargo.toml` or lockfile names a package starting with `loam-platform`; a `cargo deny` ban on the same; a conformance test in `loams-runner` (if RN1 Task 3 has merged) that `InvocationObserver`'s data type has no buffer, socket or serialization derive (`Observation` is not `Serialize`).

**Tests:** `guard_rejects_meter_names` (a fixture tree with a forbidden name); `guard_allows_allowlisted_docs`; `guard_rejects_platform_dependency`; `observation_is_not_serializable` (a compile-fail test with `trybuild`).

**Commit:** `ci: add the no-metering guard`.

### Task 9: The exit gate, docs and close

**Files:** `.github/workflows/ci.yml` (job `mt4-e2e`), `docs/design/41-multitenant-byoc-control-plane.md` (as built), the decision log fold (the integrator, #235, does the merge of the canonical decision log), `CHANGELOG.md`.

**Produces:** the exit gate of D558 as one CI job: create an org and a namespace (Task 4 e2e); enrol a second k3d cluster as BYOC through the agent and onboard a tenant there; promote a release through ring 0, fail a gate on purpose and see promotion stop; exceed a limit, receive 429 and see no usage record; run the guard.

**Tests:** the `mt4-e2e` job green; `cargo deny check`; `buf breaking`; docs links resolve.

**Commit:** `docs: record MT4 as built`.

## What MT4 leaves to others

| Item | Where |
|---|---|
| Metering, billing, plans, entitlements, the plan-to-limits mapping, invoices, credits | `loam-platform` (docs 01, 06, 07) |
| The hosted cloud's commercial APIs and the marketplace install and billing APIs | `loam-platform` (docs 05, 07); the install saga calls Task 4's Git writer (Q552) |
| Hosted-fleet operations: pre-warming, capacity, hosted Neon/WeSQL automation, abuse handling | `loam-platform` (D554) |
| `loams-meta-remote` (BYOC-managed-meta) | the M2.x plan (D64, Q551) |
| A real CKE apply, a real BYOC customer | manual, after the owner's account decisions |
| An external security review | the owner's action (Q557) |

## PR sizes

| Task | Expected size |
|---|---|
| 1 | ~900 lines |
| 2 | ~1 200 lines |
| 3 | ~1 500 lines |
| 4 | ~1 600 lines plus YAML and the e2e |
| 5 | ~900 lines |
| 6 | ~1 300 lines plus YAML |
| 7 | ~1 200 lines (TypeScript and Rust) |
| 8 | ~300 lines |
| 9 | docs and CI |

## Rulings made during execution

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| T8-1 | Implement Task 8 first under #258, independently of the control-plane dependencies. No `loams-runner` crate or RN1 Task 3 exists on `dev`; `observation_is_not_serializable` remains a required RN1 follow-up before its Task 3 merge | Protect later runtime work now; no nonexistent observer type can be tested | Add the compile-fail conformance alongside the real type |
| T8-2 | Keep Q554's allowlist unchanged. Historical protocol literals in §24, §34, §38, §44 and the plans README become plain descriptions referring to the private record; no decisions or protocol semantics change | The guard must pass without broadening the boundary | More boundary explanations may need the same wording |
| T8-3 | The stdlib Python guard parses every tracked Cargo manifest and lockfile, including aliases and target-specific dependencies, and rejects all private package prefixes. `cargo-deny` separately bans the exact root package because its documented package selectors do not support prefix globs | Fail before resolving any private dependency; no network, new package or printed file contents | Python 3.11+ is required (available on the CI runner) |
| T8-4 | Run the guard on every CI event and require it in `CI required`; fixture tests use real tracked Git trees and cache-backed temporary directories | Docs, fixtures and lockfiles can all introduce forbidden protocol names | Full repository scan is linear in tracked bytes |
| T8-5 | Reject symlinked Cargo manifests and lockfiles before parsing. Scan other symlink targets without opening their destination | CodeRabbit identified a bypass where the link target string is valid TOML but Cargo reads different dependency bytes; the regression failed before the fix | A manifest symlink must be replaced with a regular tracked manifest |
