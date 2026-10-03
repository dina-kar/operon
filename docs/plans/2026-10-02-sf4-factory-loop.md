# SF4 — The Factory Loop: a Durable, Gated, Single-Organisation Workflow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, event types, states, defaults, limits), use them verbatim. The code is not pre-written in this plan; the tests are the specification.

> **Status: Planned** (2026-10-02). **Slot: track SF, fourth plan** (proposed; D-SF-13–D-SF-15, D-SF-18). Branches `sf4-t<N>`, stacked; PRs target `main`. Depends on SF2 (agents, A2A client, risk policy evaluation, token exchange), SF3 Task 4 (approvals relay) and §21's embedded Resonate (D1). Stages 1–2 need the Zulip agent; the observation stage's GlitchTip and analytics parts use SF5's agents and, until SF5 lands, **recorded fakes** (the fake agents of Task 0). The **hosted multi-tenant factory is not here**: it is `loam-platform` doc 05. Loopback only until the unified auth plan (D111).

**Goal:** Loams Software Factory's loop, for **one organisation that self-hosts it**:
- the `factory.run` **Resonate workflow** of design §10: intake, triage, plan, fix, review, deploy, observe, close, with `generation` for regressions;
- **gates, budgets, a kill switch, loop limits and audit** (design §11), enforced in the engine;
- the **run record** (Live table `factory_runs`, stream `factory_events`, audit events) and the `loams.factory.v1` service;
- the **console pages and mobile views** for runs, approvals, agents, policy and the kill switch;
- the **`deploy/factory/` package** (chart, catalog patch, manifest) that a single organisation installs with Helm.

**Architecture:**
- **`crates/operon-factory`**: `FactoryService` (Connect, `loams.factory.v1`), the workflow (`factory_run`, one function per stage, a shared `StageCtx`), `Guard` (budgets, kill flag, cooldowns, concurrency), the intake receivers (`/hooks/glitchtip`, `/hooks/forgejo`, `/hooks/zulip`, `/hooks/plane`), `PolicyStore` (versioned Live document), `RunStore`, and the CloudEvent publisher.
- **Durable shape:** one Resonate function per run, keyed by `fingerprint ‖ generation`; every stage a `ctx.run` step with the step's promise id as idempotency key; waits (a human decision, CI, an approval, a rollout, the observation window) are promises or durable sleeps. A2A calls go through `loams-a2a`'s client with a deterministic `messageId`.
- **State:** runs and policy on Live (TiKV in cloud, the local store in `loams dev`); events on a Loams stream; artifacts live in the apps.
- **`web/plugins/factory`**: pages, cards, the kill switch for the browser console; **`desktop/crates/loams-ui-factory`**: the same views as native GPUI panels in the zeron fork; **`loams-mobile`**: run list and timeline, kill button.
- **`deploy/factory/chart`**: the Helm chart of the single-org factory (agents, the factory service, the collector configs of SF5, the edge routes of SF1) and `loams-factory.yaml`, the catalog patch.

**Tech Stack:** Rust 1.97.1, edition 2024, `loams-durable` (Resonate Rust SDK, in-process), `loams-a2a` and `loams-collab`, connect-rust and buffa, Loams Live's client crate, `cloudevents-sdk`, `tokio-test` and a paused clock for time. TypeScript and cordis 4, Vitest, Playwright; Helm 3 and `helm unittest`; `kind` or `k3d` for the install test; SwiftUI and Compose for the phone views. No new durable engine, no new queue.

**Spec:**
- [`docs/design/39-software-factory-and-loams-bot.md`](../design/39-software-factory-and-loams-bot.md): §8, §10, §11, §12, §14; D-SF-13–D-SF-16.
- [`docs/design/21-durable-execution.md`](../design/21-durable-execution.md) §3.5, §6.3 (sagas), §6.5 (approval gates), §6.6, §6.7 (idempotency), §8; [`19-console-identity-and-agents.md`](../design/19-console-identity-and-agents.md) §5.4 (revocation); [`20-reactive-database-on-tikv.md`](../design/20-reactive-database-on-tikv.md) (Live tables); [`22-showcase-suite.md`](../design/22-showcase-suite.md) §7.3 (provisioning sagas); [`38-knative-authentik-gitops.md`](../design/38-knative-authentik-gitops.md) (GitOps and rollout events); [`docs/open-core.md`](../open-core.md).
- SF2's cards and skills; SF3's approvals relay.

## Global Constraints

- **The gates are in the engine.** A stage that needs an approval cannot proceed without a settled approval whose hash covers what it will do (design §8). No prompt, no agent output and no flag in a run record bypasses this.
- **Defaults are conservative:** no auto-fix without a human decision for severity `high` and above; **no auto-merge and no auto-deploy ever** unless the org policy lists the repository and path (Q472), and never into a protected environment.
- **Every step is idempotent.** A replay of any step creates nothing twice: Zulip topic by name, Plane issue by `external_id`, Forgejo branch by name, PR by branch.
- **Budgets are checked before every stage and every A2A call**, and the AI gateway's per-principal hard cap exists independently (design §11).
- **The kill switch does not depend on the loop or the model route.** It needs exactly: the Live flag write (TiKV, which the control plane already needs), the principals store (suspend), the A2A `CancelTask` call to each in-flight agent (a plain HTTP call that does not use the model) and the Resonate cancel of the workflows. It runs in the `FactoryService` request handler, not inside any workflow, so a stuck loop cannot block it; if an agent is unreachable the flag and principal suspension still stop it (the agent's next call is refused), and the unreachable `CancelTask` is retried durably. A **run-scoped kill suspends no principal** (agents are shared by unrelated runs); an agent-scoped kill suspends that agent; an org-scoped kill suspends all of the org's agent principals.
- **No metering in this repository** (D403, D444). The factory emits business events only.
- **No model content in the run record, events or audit logs.** Summaries come from agents' status text; the canary scan of Task 9 covers it.
- **Loopback only until D111.**
- **Time in tests is a paused clock;** no test sleeps.
- **The build machine.** One cargo build at a time, shared target; `helm`/`kind` tests one at a time; stop and report if `/home` has under 8 GB free.
- **Commit areas:** `factory`, `policy`, `web`, `ios`, `android`, `deploy`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **One run per `(fingerprint, generation)`**; a regression is a new run with `generation + 1` and `parent_run` set; the cap is 2 | Each run stays a simple linear workflow, and a loop cannot run forever | A bug that needs three deploys to fix needs a person after generation 2, which is the point |
| 2 | **Triage waits for a human by default** (`triage.auto_fix = false`); a reply `fix`, `ignore` or `escalate` in the thread (or a console button) settles it | The cheapest control with the most value | Slower runs; auto-fix is a policy for low severity and listed projects |
| 3 | **CI is awaited by promise, settled by Forgejo's webhook** | No polling; works across restarts | A lost webhook stalls a run; a durable timer polls `ci.status` once a minute after 10 minutes of silence |
| 4 | **Deploy is a Forgejo merge plus a GitOps rollout event** (§38); other deploy mechanisms are a configured webhook and a callback (Q473) | Argo CD is already the layout's deploy path; one mechanism is testable | Orgs with other CD systems wire the callback |
| 5 | **The observation window is a durable sleep with early exit**: it ends early on `regressed` (recurrence over the threshold) and runs the full window on `resolved` | Fast revert, no wasted wait | Early exit needs a streaming check; polled each minute |
| 6 | **Policy is a versioned Live document**, edited through `loams.factory.v1`, and edits in protected environments are approval-gated | Auditable and revertable; the policy is itself a risk | The policy editor is a form over JSON Schema (the connector form of §37); a raw JSON editor is available |
| 7 | **Revert is a PR like any other**, gated by the merge approval | No special unsafe path | A broken main waits for a human; the console's "revert now" button opens the approval at once |
| 8 | **The Plane and Forgejo agents write; the loop reads artifacts back through `loams.collab.v1`** | One audited read path | A little more latency per stage |

## Review Focus

1. **No ungated merge or deploy.** Tests: Tasks 5, 6 (`merge_without_approval_never_happens`, `approval_for_old_head_is_void`), Task 9.
2. **Idempotent and resumable.** Tests: Tasks 4–7 (`replay_creates_nothing_twice`, `crash_at_each_stage_resumes`).
3. **Budgets and loop limits hold.** Tests: Task 2.
4. **The kill switch stops everything.** Tests: Task 9 (`kill_org_suspends_principals_and_cancels_tasks`, `kill_works_with_model_down`).
5. **The record is complete and has no model content.** Tests: Tasks 1, 9.
6. **The single-org package installs.** Tests: Task 10.

## File structure

```
proto/loams/factory/v1/factory.proto                   # FactoryService, Run, Stage, Policy, Budget, KillRequest
crates/operon-factory/src/{lib.rs,service.rs,run.rs,stages/{intake,triage,plan,fix,review,deploy,observe,close}.rs,guard.rs,policy.rs,store.rs,hooks.rs,events.rs,fakes.rs}
crates/operon-factory/tests/{main.rs,service.rs,guard.rs,intake.rs,stages.rs,e2e.rs,kill.rs,canary.rs,crash.rs}
web/plugins/factory/{package.json,src/{index.ts,pages/{Runs,RunDetail,Approvals,Agents,Policy,Kill}.tsx,cards/*},test/*}
ios/Sources/Factory/*  android/app/src/main/java/.../factory/*      # loams-mobile
deploy/factory/{chart/Chart.yaml,chart/values.yaml,chart/templates/*,loams-factory.yaml,README.md}
docs/design/39-…  docs/plans/README.md  CHANGELOG.md
```

### Task 0: Reconcile, and build the fakes

**Files:** read SF1–SF3 as merged; `crates/operon-durable` (workflow, promise and sleep APIs); `crates/operon-factory/src/fakes.rs` (new).

**Produces:** **fake agents** for all five roles that speak the real A2A bindings through `A2aServer` with scripted behaviour (state transitions, artifacts, delays on the paused clock, injected failures and `INPUT_REQUIRED`), and a fake Forgejo webhook sender, so every later task runs the real client and the real tasks against scripted agents. Confirm: how a Resonate Rust function declares a promise, a durable sleep and a timeout; the promise id conventions (D146); the Live table and stream APIs; Argo CD's notification events (the rollout event payload) **(verify against §38 as built)**.

**Tests:** `fake_agent_passes_a2a_conformance` (SF2's inspector run); `fake_scripted_failure_surfaces_as_failed_task`.

**Commit:** `factory: fake agents and reconcile with main`.

### Task 1: `loams.factory.v1` and the run record

**Files:** `proto/loams/factory/v1/factory.proto`, `crates/operon-factory/src/{service.rs,run.rs,store.rs,events.rs}`, `tests/{main.rs,service.rs}`.

**Produces:**

```proto
service FactoryService {
  rpc ListRuns(ListRunsRequest) returns (ListRunsResponse) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc GetRun(GetRunRequest) returns (Run) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc WatchRuns(WatchRunsRequest) returns (stream RunEvent);               // snapshot, changes, heartbeat 15 s, cursor
  rpc StartRun(StartRunRequest) returns (Run);                             // idempotency_key; manual signal
  rpc DecideTriage(DecideTriageRequest) returns (Run);                     // fix | ignore | escalate (a person's decision, not an approval)
  rpc PauseRun(PauseRunRequest) returns (Run);  rpc ResumeRun(ResumeRunRequest) returns (Run);
  rpc Kill(KillRequest) returns (KillResponse);                            // scope: run | agent | org; needs factory:admin and step-up
  rpc GetPolicy(GetPolicyRequest) returns (Policy) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc UpdatePolicy(UpdatePolicyRequest) returns (operations.v1.Operation); // approval-gated in protected environments
  rpc ListAgents(ListAgentsRequest) returns (ListAgentsResponse) { option idempotency_level = NO_SIDE_EFFECTS; }
}
```

`Run`: the fields of design §12. Events: `io.loams.dev.factory.run.opened.v1`, `…stage.started.v1`, `…stage.completed.v1`, `…approval.requested.v1`, `…budget.exceeded.v1`, `…run.paused.v1`, `…run.completed.v1` (with `verdict`, `cost`, `duration`, counts), `…kill.executed.v1`. `GetRun` returns links, never contents.

**Semantics:** `factory_runs` rows are written only by the workflow (the service has no direct write except `Pause`, `Resume`, `DecideTriage`, `Kill`, `UpdatePolicy`, each an audited command). `ListRuns` is filtered by OpenFGA (`factory#viewer`).

**Tests:** `list_filters_by_state_and_project`; `get_run_has_links_not_content`; `watch_sends_snapshot_then_changes`; `start_is_idempotent_on_key`; `decide_triage_only_when_waiting`; `kill_requires_admin_and_step_up`; `events_golden` (every CloudEvent type against the CloudEvents profile of §34's open charter); `non_viewer_gets_not_found`.

**Commit:** `factory: loams.factory.v1 and the run record`.

### Task 2: Policy and the guard

**Files:** `crates/operon-factory/src/{policy.rs,guard.rs}`, `tests/guard.rs`.

**Produces:**

```rust
pub struct Policy { pub version: u64, pub triage: TriagePolicy, pub rules: Vec<Rule> /* (agent, skill, env) -> Allow|Approve|Deny */,
                    pub budgets: Budgets, pub limits: Limits, pub repos: Vec<RepoScope>, pub observe: ObservePolicy, pub deploy: DeployPolicy }
pub struct Budgets { pub run: BudgetSet, pub org_day: BudgetSet }           // tokens, usd_micro, wall_secs, attempts, open_prs, runs
pub struct Limits  { pub max_generation: u8 /* 2 */, pub fingerprint_cooldown: Duration /* 24h */, pub max_concurrent_per_project: u8 /* 2 */ }
pub struct Guard<'a> { /* policy snapshot, run record, kill flag, clock */ }
impl Guard<'_> { pub async fn check(&self, cx: &StageCtx, next: &StageKind) -> Result<(), GuardStop>; pub fn charge(&self, spent: Spend); }
pub enum GuardStop { BudgetExceeded(BudgetKind), Killed, Paused, LoopLimit(LimitKind), PolicyDeny }
```

**Validation:** a policy is rejected unless `max_attempts >= 1`, every budget is positive, and `max_generation <= 2` unless the owner raises it.

**Defaults (verbatim):** run: `wall = 4 h`, `attempts = 3`, `open_prs = 1`, `usd = $5.00` (a placeholder the org sets), tokens set by the model route; org per day: `runs = 10`, `open factory PRs = 3`, `usd = $50.00`. `triage.auto_fix = false`; `triage.timeout = 72 h` then the run closes `abandoned`. `observe.window = 30 min`, `observe.regression_threshold` = any recurrence of the fingerprint above `max(2, baseline × 1.5)`. `deploy.auto = false`.

**Semantics:** `check` runs before every stage and every A2A call; it reads the kill flag from Live (read-your-writes, so a kill takes effect on the next check), applies budgets from the run's `spent`, loop limits (generation, cooldown, concurrency), and the rule table. Only `BudgetExceeded` pauses the run with a `budget.exceeded` event and a card; the other stops are `Killed` (the `kill.executed` event and state `killed`), `Paused` (a `run.paused` event, resumed by a person), `LoopLimit` (the run closes with verdict `limited`, linked to the earlier run, and a `run.completed` event) and `PolicyDeny` (paused with reason `policy_denied` and a card); a person raising a budget (a policy edit or `ResumeRun` with a one-time extension, itself audited) resumes it. The policy is read as a **snapshot at run start** and re-read at gates; edits do not rewrite history.

**Tests:** `budget_stops_before_overspend` (each kind); `wall_time_uses_clock_not_steps`; `generation_cap_stops_third_run`; `fingerprint_cooldown_links_not_creates`; `concurrency_cap_queues_extra_runs`; `kill_flag_is_seen_on_next_check`; `rules_table_deny_stops`; `policy_edit_does_not_change_running_snapshot`; `raising_budget_resumes_run`; `default_policy_is_conservative` (golden: `auto_fix = false`, `deploy.auto = false`).

**Commit:** `factory: policy and the guard`.

### Task 3: Intake: signals, receivers and dedupe

**Files:** `crates/operon-factory/src/{hooks.rs,stages/intake.rs}`, `tests/intake.rs`.

**Semantics:** receivers on the factory's **webhook listener**, which every receiver authenticates itself (HMAC or token, below), so it may bind a cluster-internal address and is **never routed through the public gateway until D111**; on a laptop it is loopback and the apps must be on the same host. Webhook provisioning (SF4 Task 10, SF5 Task 4) therefore runs only when the receiver is routable from the app: in the same cluster, or on one host. Otherwise the saga reports `webhook_unreachable` and the factory runs on manual signals only. The receivers: `/hooks/glitchtip` (alert webhook; verifies a shared secret header), `/hooks/forgejo` (HMAC-signed, `X-Forgejo-Signature`), `/hooks/zulip` (outgoing webhook token), `/hooks/plane` (HMAC); each normalises to a `Signal { source, fingerprint, severity, title, link, evidence_refs }`. GlitchTip: the fingerprint is the issue id; severity from level and event count against the policy's thresholds. Analytics anomalies arrive as `Signal`s published by the analytics agent's scheduled `anomaly.check` (SF5; here, the fake). Dedupe: a signal whose fingerprint has an open run attaches to it (an event, no new run); a fingerprint inside its cooldown is recorded and linked to the closed run; a signal caused by a factory deploy (the issue's first-seen release equals a factory-deployed ref, `caused_by_run`) is linked to the cause's run and starts `generation + 1` only through the observe stage (Task 7), not here. Webhook secrets come from the broker's secret store.

**Tests:** `glitchtip_alert_becomes_signal`; `bad_signature_is_401_and_creates_nothing`; `duplicate_fingerprint_attaches_to_open_run`; `cooldown_links_to_closed_run`; `low_severity_is_logged_not_run`; `factory_caused_signal_is_linked_to_cause`; `manual_start_from_chat_makes_run`; `webhook_replay_is_idempotent`; `unknown_source_is_404`.

**Commit:** `factory: signals, receivers and dedupe`.

### Task 4: The workflow skeleton, triage and plan

**Files:** `crates/operon-factory/src/{run.rs,stages/{triage,plan}.rs}`, `tests/stages.rs`, `tests/crash.rs`.

**Produces:**

```rust
pub async fn factory_run(ctx: &durable::Ctx, signal: Signal, gen: u8) -> Result<RunOutcome, FactoryError>;
pub struct StageCtx<'a> { pub run: &'a RunRecord, pub guard: &'a Guard<'a>, pub a2a: &'a A2aClient, pub store: &'a RunStore, pub events: &'a EventSink }
// each stage: async fn stage(cx: &StageCtx, input: StageInput) -> Result<StageOutput, StageStop>
```

**Semantics:** `factory_run` is the pseudocode of design §10.2. Triage: `zulip.threads.open_triage` with the evidence (the `glitchtip` and `analytics` reads, via SF2/SF5 agents or the fakes), then waits on the promise `triage:<run id>` settled by `DecideTriage`, a Zulip reply parsed by the receiver (`fix`, `ignore`, `escalate` as the first word of a message by an authorised member) or policy auto-fix; the wait has `triage.timeout`. `ignore` closes the run `ignored`; `escalate` posts to the escalation stream and waits for a new decision. Plan: `plane.issues.create` with `external_id = factory:<run id>`; the issue key is stored. Every A2A call carries the step's promise id as `messageId`. The guard checks before each step.

**Tests:** `intake_to_plan_happy_path` (fakes); `triage_waits_for_a_human_by_default`; `zulip_reply_fix_settles_triage`; `reply_from_non_member_is_ignored`; `auto_fix_only_when_policy_allows`; `ignore_closes_run`; `triage_timeout_abandons`; `replay_creates_nothing_twice` (the whole prefix replayed 3 times: one topic, one issue); `crash_at_each_stage_resumes` (a failpoint before and after every `ctx.run` in stages 1–3); `guard_stop_pauses_with_card`.

**Commit:** `factory: the workflow, triage and plan`.

### Task 5: Fix: patch, PR, CI and the attempt loop

**Files:** `crates/operon-factory/src/stages/fix.rs`, `tests/stages.rs`.

**Semantics:** for `attempt` in `1..=max_attempts`: `forgejo.propose_patch` (the evidence, the issue, the previous CI log tail if any; the budget passed along), then `branches.create`/commit/`prs.open` (create-if-absent: **one stable branch `factory/<run>` for the whole run**; each attempt is a new commit on it, so there is one PR per run, updated by later attempts, and branch-keyed idempotency holds), then wait on the promise `ci:<run>:<head sha>` settled by `/hooks/forgejo` (commit status or Actions run completion). A failing CI feeds the log tail (`untrusted`, capped) into the next attempt; after the last attempt the run **fails safe**: the PR is converted to draft and labelled `factory:needs-human`, the thread and issue say so, and the run is `failed` (not killed). A durable timer polls `ci.status` after 10 minutes without a webhook. Open PR count is charged to the budget. **The stage never merges.**

**Tests:** `fix_happy_path_opens_one_pr`; `ci_failure_feeds_next_attempt`; `max_attempts_fails_safe_with_draft_pr_and_label`; `ci_webhook_settles_promise_once`; `lost_webhook_is_polled_after_10_minutes`; `pr_is_updated_not_duplicated_on_retry` (same branch, new head commit); `open_prs_budget_stops_second_pr`; `patch_agent_failure_is_a_failed_attempt_not_a_crash`; `stage_never_calls_merge` (the fake Forgejo records zero merge calls); `head_sha_changes_invalidate_ci_result`.

**Commit:** `factory: patch, PR and CI`.

### Task 6: Review and deploy

**Files:** `crates/operon-factory/src/stages/{review,deploy}.rs`, `tests/stages.rs`, `tests/e2e.rs`.

**Semantics:** **Review:** post the PR link and a summary in the thread; request the merge approval (`loams.approvals.v1`, kind `forgejo.merge`, a hash over repo, PR number, head SHA, base branch); wait on the approval promise (72 h timeout then `abandoned` with the PR left open); a push to the PR head after the request **voids** the approval and re-requests it. **Deploy:** on a settled approval, `prs.merge` (the broker releases the permit only for the matching approval); then the deploy mechanism: **policy auto-merge**, when the org policy lists the repository and path and the required checks pass, replaces the human wait with a recorded **policy authorization** (a record of kind `policy`, which is an authorization under the org's policy and not an approval by a person, hash-bound to the same repo, PR, head SHA and base, created by the factory under the policy version in force and audited); the broker accepts it only for listed repositories and paths and **never in a protected environment**, where the human approval is required. For the §38 layout the merge lands on the environment branch and the run waits on the rollout promise `rollout:<run>:<sha>` settled by the Argo CD notification receiver; for the callback mechanism the promise is settled by the configured webhook. **Every deploy needs an approval by default**: when the policy lists the service with `deploy.auto` in an unprotected environment, the merge approval is the only one; in protected environments, for the first deploy of a service, and whenever `deploy.requires_approval` is set, **two approvals (the merge approval and a deploy approval covering the environment and the sha) both settle before `prs.merge` is called**. Rollout failure marks the run `failed` and opens the revert flow of Task 7. The merge approval is also visible in chat and on phones (SF3).

**Tests:** `merge_without_approval_never_happens` (every path, including a replayed step); `approval_for_old_head_is_void`; `approval_timeout_abandons_leaves_pr_open`; `deploy_waits_for_rollout_event`; `rollout_failure_starts_revert`; `second_approval_for_protected_env_settles_before_merge`; `deploy_requires_approval_by_default`; `policy_rejects_zero_max_attempts`; `auto_merge_only_for_listed_repo_and_path_with_checks`; `auto_merge_never_in_protected_env`; `merge_approval_requester_cannot_be_the_approver` (SF3's rule, end to end); `e2e_signal_to_deployed_with_fakes` (the paused clock runs the whole loop in under 5 seconds of wall time).

**Commit:** `factory: review, approval-gated merge and deploy`.

### Task 7: Observe and close

**Files:** `crates/operon-factory/src/stages/{observe,close}.rs`, `tests/stages.rs`.

**Semantics:** observation runs as a durable loop: every minute until the window ends, the stage asks the `glitchtip` agent for the fingerprint's events since the deploy ref, the `analytics` agent for the watched metric against its baseline, and the factory's own readers for the Langfuse score and OpenObserve error rate and latency of the touched service (SF5 Task 6 gives the readers; here, behind a `Observer` trait with fakes). Verdict: **regressed** the moment the recurrence threshold of Task 2's policy is crossed (early exit); **resolved** when the window ends clean; **inconclusive** if a required source is unavailable (extended once, then the thread asks a person). Close: *resolved* closes the Plane issue, posts a summary (what changed, the evidence, the cost) and ends; *regressed* opens a **revert PR** (the same merge approval, a `revert` fast-path button in the console that opens the approval immediately) and publishes a new signal with `parent_run` and `generation + 1`, unless `generation` is at the cap, in which case it escalates to a human and ends `failed`. `inconclusive` after the extension asks a person and waits.

**Tests:** `window_resolves_clean`; `recurrence_regresses_early`; `missing_source_is_inconclusive_then_asks`; `regressed_opens_revert_pr_needing_approval`; `generation_cap_escalates`; `new_signal_links_parent_run`; `summary_has_cost_and_links_no_content`; `observer_fakes_cover_all_four_sources`; `crash_during_window_resumes_with_correct_deadline` (the deadline is a durable timestamp, not a counter).

**Commit:** `factory: observe, verdict and close`.

### Task 8: Console, desktop and mobile views

**Files:** `web/plugins/factory/**` (browser console), `desktop/crates/loams-ui-factory/**` (the native desktop, a zeron fork; path per §37's amendment, D480–D499), `loams-mobile` `Factory` modules, `web/apps/console/catalog/*`.

**Desktop (GPUI):** `loams-ui-factory` renders the same Runs, Run detail (stage graph), Approvals, Agents, Policy and budgets, and Kill views from `loams-apps-client`'s `FactoryService` client and registers them in zeron's shell sidebar; "Open trace in Langfuse" and "Open in OpenObserve" go through SF1's `AppOpener` (system browser, or the sidebar browser where it ships); `/kill` and the Kill view need a step-up and a confirmation naming the in-flight tasks. Tests (GPUI test context, fake `FactoryService`): `runs_view_updates_live`, `run_detail_stage_graph_golden`, `trace_links_use_opener_and_are_hidden_when_apps_absent`, `kill_requires_step_up_and_shows_in_flight_count`, `policy_view_shows_diff`, `viewer_cannot_see_kill`.

**Produces:** `@loams/plugin-factory`: **Runs** (table, filters, a cost column, live updates through `WatchRuns`); **Run detail** (the stage graph with each stage's state, duration and cost; artifact links that open the SF1 panes and cards; "Open trace in Langfuse" and "Open in OpenObserve" as `embed.pane` actions from SF5, hidden when those apps are not listed; the approvals and the policy snapshot used); **Approvals** (the org's queue, reusing `approval.renderer`); **Agents** (cards, health, last task, principal state, suspend); **Policy and budgets** (a JSON-Schema form plus a raw editor, a diff against the previous version, approval for protected environments); **Kill switch** (scoped, step-up, a confirmation that names the number of in-flight tasks). `operation.detail#factory.run`. Mobile: run list, run timeline, approvals (existing), and a **Kill** action with biometric step-up; push categories `factory.run` (a run needs a decision, paused, completed, regressed) and `factory.budget`.

**Tests:** Vitest: `runs_table_updates_live`; `run_detail_shows_stage_graph_from_golden_run`; `trace_links_hidden_when_apps_absent`; `policy_edit_shows_diff_and_requires_approval_in_protected_env`; `kill_requires_step_up_and_shows_in_flight_count`; `viewer_cannot_see_kill`. Swift and Kotlin: `run_list_golden`, `timeline_golden`, `kill_requires_biometric`, `push_payload_golden`, `tap_navigates_only`. Playwright: `kill_from_console_then_runs_show_killed`.

**Commit:** `factory: console, desktop and mobile views`.

### Task 9: Kill switch end to end, audit, injection and canary

**Files:** `crates/operon-factory/tests/{kill.rs,canary.rs}`, audit event definitions.

**Semantics:** `Kill(scope)`: (1) set the Live flag (run, agent or org); (2) suspend the affected agent principals through §19's suspend (tokens die with the change-feed latency); (3) `CancelTask` on every in-flight A2A task of the scope; (4) cancel the workflows; (5) post a notice to each run's thread; (6) emit `kill.executed`. A killed run's compensation (close its PR, delete its branch, comment) runs with the **factory service's own** principal, which is not suspended. Every command, policy change, gate and kill emits an audit event (OTel logs to a Loams stream, D100) with the actor chain and **no model content**.

**Tests:** `kill_org_suspends_principals_and_cancels_tasks`; `kill_run_leaves_other_runs_running`; `kill_works_with_model_down` (the AI gateway returns 503; kill still completes in under 2 s of paused-clock time); `kill_mid_stage_stops_before_the_next_step`; `killed_run_compensates_pr_and_branch`; `audit_has_actor_chain_for_every_command`; `canary_never_appears_in_runs_events_audit` (secret canary and a model-content canary); `injection_in_issue_body_cannot_start_merge` (end to end with the scripted model); `budget_exceeded_card_has_no_content`.

**Commit:** `factory: kill switch, audit and hardening`.

### Task 10: The single-organisation package and the exit gate

**Files:** `deploy/factory/**`, `docs/plans/sf4-exit-report.md`, `docs/design/39-…`, `CHANGELOG.md`.

**Semantics:** the chart installs the factory service, the five agents (phase 1: three), the edge routes (SF1), the collector (SF5), and webhooks provisioning jobs (a saga on §21 that registers each app's webhook to the factory receivers and creates the agents' app identities, per §22 §7.3); `loams-factory.yaml` is the catalog patch that enables the plugins. `values.yaml` holds the placeholders (`domain`, the model route, budgets). Values default to **gates on, budgets low, auto-fix off**. The README documents the install in under 20 commands and links the licence table (design §4).

**Tests:** `helm unittest` (values render; defaults are conservative; no secret in a ConfigMap); the **install test** on `kind`: install the chart with the three phase-1 apps from SF1's harness, run the provisioning saga, assert the webhooks exist, the principals exist and are scoped, then run `e2e_signal_to_deployed` with the real apps and a fake model; uninstall leaves no orphaned principals.

**Exit gate (all in CI):** Tasks 1–10 tests; the loop on the kind install, signal to closed, including a forced CI failure, a rejected approval, a regression with a revert, a budget stop, a kill and a crash at each stage (killing the factory pod); the canary; record the run's cost and wall time per stage on the paused clock and on the real stack, and the memory of the factory service.

**Commit:** `docs: SF4 exit report`.
