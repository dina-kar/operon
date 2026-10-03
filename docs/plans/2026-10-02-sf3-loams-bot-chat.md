# SF3 — Loams Bot: One Chat in the Desktop and Mobile Apps Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, event names, deep links, push categories), use them verbatim. The code is not pre-written in this plan; the tests are the specification.

> **Status: Planned** (2026-10-02). **Slot: track SF, third plan** (proposed; D-SF-6, D-SF-8, D-SF-9, D-SF-19). Branches `sf3-t<N>`, stacked; PRs target `main` (mobile: `ostrium-labs/loams-mobile`). Depends on SF2 (the A2A client, agents and token exchange), AP0 (protos, the mock), AP1a (the cordis browser console), the **native desktop shell, a zeron fork** (§37 amended for a native desktop, D480–D499; Tasks 5 and 6's desktop halves wait for its skeleton and build as standalone crates with GPUI's test context until then) and AP2/AP3 for the phone shells; the push path (D436) is AP4 server work and the phone push tasks (Task 8) use AP0's mock notifier until it lands. Loopback only until the unified auth plan (D111).

**Goal:** One chat, **Loams Bot**, in the native desktop app (a zeron fork), in the browser console (a cordis page and overlay) and on iOS and Android (native), driving the platform agents over A2A:
- a server, **`loams-bot`**, that runs the harness agent loop headless, as a durable execution per chat thread, with a `subagent-a2a` provider that delegates to the agents of SF2;
- **`loams.bot.v1`** over Connect for every client (threads, send, watch, cancel, answer, approve-handoff);
- **the desktop as a new `Harness` in zeron's engine** (`loams-harness-bot`) so zeron's own conversation, composer, sidebar and trajectory UI show Loams Bot threads, plus GPUI cards and approvals views; and the browser console's `@loams/plugin-bot`, a cordis port of the DeepSeek harness's conversation UI;
- native chat screens on both phones, with push that opens the right thread, run or approval;
- Loams Bot as an **A2A server** too, so external A2A clients can drive it: the server, its route and its card are **disabled by default** and exist only with `--bot-a2a` (Q469).

**Architecture:**
- **`crates/operon-bot`**: the Connect service (`BotService`), the thread store (Live table `bot_threads`, stream `bot_events`), the **harness host manager** (spawns and supervises the harness SDK server process, speaks its JSON-RPC over stdio or a Unix socket), the A2A client wiring (SF2's `A2aClient`, token exchange per call), the push mapping, and the optional A2A server card for `loams-bot`.
- **`packages/subagent-a2a`** (TypeScript, in the harness-host bundle `web/host-bot/`): a provider on `ctx.subagents` patterned on `subagent-acp`: `start`, `continue`, `cancel`, `list`, over a small `A2aTransport` that calls back to `loams-bot` (the Rust side owns tokens, signing and tracing; the TS side never sees a bearer).
- **`web/plugins/bot`** (`@loams/plugin-bot`): the **browser console's** cordis page and overlay: `bot.message.renderer`, `bot.card`, composer, slash commands, mentions, trajectory.
- **Desktop (the zeron fork; the directory is named by §37's amendment, written `desktop/`):** `loams-harness-bot` (implements zeron's `Harness` trait over `loams-apps-client`'s `BotService` client), `loams-ui-bot` (artifact cards, `@agent` mentions, slash commands, the Loams Bot sidebar section, the approvals handoff) and, from SF4, `loams-ui-factory`.
- **`ostrium-labs/loams-mobile`**: `Bot` module in each app: chat list, thread, composer, artifact cards, deep links, notification handlers.

**Tech Stack:** Rust 1.97.1, edition 2024, connect-rust and buffa (D128), `loams-a2a` (SF2), `loams-durable`; the harness core packages (MIT; pinned commit recorded in `THIRD_PARTY_NOTICES.md`; D421: patterns, with any copied file keeping its notice) running on Node 22 or Bun (Task 0 picks), cordis 4 behind `@loams/cordis` for the browser console; TypeScript, React, Vitest, Playwright; for the desktop, Rust with the zeron fork's GPUI revision and its `zeron-harness`, `zeron-proto` and `zeron-ui` crates (MIT; licences of the whole tree checked with `cargo deny` at Task 0); SwiftUI with connect-swift (iOS 17), Jetpack Compose with connect-kotlin (API 29), XCTest, JUnit and Compose UI tests.

**Spec:**
- [`docs/design/39-software-factory-and-loams-bot.md`](../design/39-software-factory-and-loams-bot.md): §5 (all), §8, §13; D-SF-6–D-SF-9, D-SF-19.
- [`docs/design/37-desktop-and-mobile-apps.md`](../design/37-desktop-and-mobile-apps.md) (the desktop parts as **amended for a native desktop, D480–D499**): §3 (what is taken from the harnesses), §5.4–§5.5 (browser console), §7.3 (approvals), §7.4 (push), §7.5, §8.3.
- The harness repositories (read-only references): `packages/client/{ui-conversation,ui-tool,ui-subagent,ui-user-questions,ui-trajectory,ui-input-trigger,ui-commands,ui-slots}`, `packages/subagent/{subagent,subagent-acp,tool-subagent}`, `packages/interaction/*`, `packages/sdk` (the JSON-RPC server half), `packages/acp`, `docs/subsystems/subagent.md`; the mobile harness's chat module.
- [`docs/plans/2026-10-01-ap0-app-protos.md`](2026-10-01-ap0-app-protos.md), [`…ap1a-cordis-console.md`](2026-10-01-ap1a-cordis-console.md), [`…ap2-android-compose.md`](2026-10-01-ap2-android-compose.md), [`…ap3-ios-swiftui.md`](2026-10-01-ap3-ios-swiftui.md).

## Global Constraints

- **Clients speak Connect only** (D420). No A2A on the phones or the desktop UI.
- **Loams Bot never decides an approval, and never answers a question that an agent has not marked `answerable_by_orchestrator`** (design §8). The client UI offers "Review", which opens `loams.approvals.v1`'s screen.
- **No bearer in the harness host process.** Tokens, signing and tracing live in `loams-bot`. The TS side sends `a2a.call` requests over the host channel and receives results.
- **A thread is a durable execution.** Closing the app, a locked phone and a crashed host process lose nothing; the thread resumes (§21, D24).
- **No queued sends offline** (D437). A send needs a connection; the composer says so.
- **Untrusted content renders as inert text** in every client (design §8 item 5).
- **Mobile uses the binary Connect codec** (D433); the desktop uses connect-rust clients through the transport and token source of §37's amendment; the browser console uses JSON over fetch.
- **zeron's sync is not used.** The desktop runs in zeron's Local profile for Loams threads; the Loams instance is the source of truth (design §13).
- **Loopback only until D111**, as `loams-bot`'s listener and the host channel.
- **The build machine.** One cargo build at a time; Node and Gradle builds one at a time; Xcode builds on the Mac only.
- **Commit areas:** `bot`, `subagent-a2a`, `web`, `desktop`, `ios`, `android`, `proto`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **The agent loop is the harness core, run as a supervised child process; `loams-bot` is Rust** | Buy, not build: the harness's loop, sessions, compaction, subagent seam and user-question seam are mature MIT code; a Rust port is a large rewrite with no user-visible gain | A second runtime (Node or Bun) in the server image. Q468 asks whether to port the small loop to Rust later. The channel is narrow so a port is possible |
| 2 | **`loams.bot.v1` imports A2A's `Message`, `Part`, `Task` and `Artifact` protos if Task 0 shows it works** (Q467); otherwise it mirrors them field for field | One vocabulary from agent to screen | Mirroring needs a conversion layer; fixtures pin it |
| 3 | **One thread = one A2A `contextId`**; each user message that delegates is one or more A2A tasks | A2A's context model fits chat; multi-turn clarification stays within a context | A thread that touches five agents has five task chains under one context; the UI shows them as subagent cards |
| 4 | **Routing is the model's, with `@agent` as an override**; the card skills are the model's tool descriptions | No separate router to maintain; the cards are the truth | Wrong routing is possible; `/route` shows the choice and the trajectory view shows why |
| 5 | **The desktop is a `Harness` in zeron, not a second chat UI** | Reuses zeron's conversation, composer, sidebar and diff pane; Loams Bot threads sit beside local coding sessions | The fork must keep the `Harness` seam stable across zeron upgrades; Loams code is in separate crates and upstreamed where it fits |
| 5a | **The browser console keeps a cordis page and an overlay of the same component** | One code path in the browser | The overlay needs layout care in narrow windows |
| 6 | **Push opens a screen, never acts** (D432, D436) | An attacker who spoofs a notification gains nothing | "Approve from the lock screen" is not offered (D435) |
| 7 | **The external A2A card for Loams Bot ships off by default** (Q469) | Exposing the orchestrator on a network is an organisation decision | A flag, `--bot-a2a`, and a card at `/.well-known/agent-card.json` of the bot listener |

## Review Focus

1. **Loams Bot cannot approve or answer for a person.** Tests: Task 4 (`approval_decision_through_bot_is_refused`, `unmarked_question_goes_to_the_user`).
2. **The host process never sees a token.** Tests: Task 3 (`host_channel_carries_no_bearer`, `canary_token_never_reaches_host`).
3. **A thread survives crashes and reconnects.** Tests: Task 2 (`thread_resumes_after_host_crash`, `watch_resumes_from_cursor`).
4. **Clients render identical states.** Tests: golden fixtures in Tasks 5, 7 and 8 (`thread_states_golden`).
5. **Inert untrusted text.** Tests: Tasks 5, 7, 8.
6. **Push goes to the right device and opens the right screen.** Tests: Task 8.

## File structure

```
proto/loams/bot/v1/bot.proto                            # BotService, Thread, Event, Part, Card, TaskRef
crates/operon-bot/src/{lib.rs,service.rs,threads.rs,host.rs,channel.rs,a2a.rs,approvals.rs,push.rs,card.rs}
crates/operon-bot/tests/{main.rs,service.rs,threads.rs,host.rs,approvals.rs,push.rs,canary.rs,a2a_server.rs}
web/host-bot/{package.json,src/{main.ts,channel.ts,provider.ts,transport.ts},test/*}   # harness host bundle + subagent-a2a
web/plugins/bot/{package.json,src/{index.ts,page.tsx,overlay.tsx,composer.tsx,renderers/*,cards/*,commands.ts},test/*}
conformance/fixtures/bot/{threads.json,events.json,deeplinks.json,push.json}          # golden files shared by all clients
ios/Sources/Bot/*  ios/Tests/BotTests/*   android/app/src/main/java/.../bot/*  android/app/src/test/…/bot/*   # loams-mobile
docs/design/39-…  docs/plans/README.md  THIRD_PARTY_NOTICES.md  CHANGELOG.md
```

### Task 0: Reconcile and study the harness

**Files:** read the harness repositories' packages named under Spec at the pinned commit; `crates/operon-a2a` (SF2 as merged); AP0's mock. Record results in `docs/plans/sf3-spike.md`.

**Checks:**
- **The harness SDK server half** (`packages/sdk`): its JSON-RPC methods for creating a session, sending a turn, streaming events, answering a user question and approval, cancelling, and resuming; whether it runs headless on Node 22 and on Bun; its memory per session (estimate); its licence list (`THIRD_PARTY_NOTICES.md`).
- **The `ctx.subagents` provider contract** (`docs/subsystems/subagent.md`, `subagent-acp`): `start`, `continue`, `cancel`, live runs; how a provider surfaces user questions and approvals.
- **Which `ui-*` plugins** are reusable as they are, which need Typert replaced by Connect, which hard-code harness concepts that Loams lacks (workspaces, goals, plans).
- **zeron** at the pinned commit: the `Harness` trait (`zeron-harness`), `RunRequest` and `AgentEvent` (`zeron-proto`), how `zeron-ui` registers views and sidebar sections, how steering and interrupt reach a harness, how a harness reports questions and approvals, which crates the fork must keep, the licence of every dependency (`cargo deny check`, including the forked GPUI, Loro and `webrtc`), and what sync would do if a Loams build left it on (it must be off).
- **The mobile harness chat module:** the message model, tool-card model, goal dock, and what is Android-specific; the shape SwiftUI needs.
- **A2A proto import** (Q467): does importing the spec's `a2a.proto` into buffa and connect-rust work for `loams.bot.v1`, and does connect-swift and connect-kotlin generation handle it.
- Which Node or Bun, and the host image size delta (estimate).

**Commit:** `docs: reconcile SF3 with main`.

### Task 1: `loams.bot.v1` and the mock

**Files:** `proto/loams/bot/v1/bot.proto`, `conformance/fixtures/bot/*.json`, AP0's mock server (`loams-apps-mock`) additions.

**Produces:**

```proto
service BotService {
  rpc ListThreads(ListThreadsRequest) returns (ListThreadsResponse) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc CreateThread(CreateThreadRequest) returns (Thread);                      // idempotency_key required
  rpc GetThread(GetThreadRequest) returns (Thread) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc Send(SendRequest) returns (SendResponse);                                // idempotency_key = the client message id
  rpc Watch(WatchRequest) returns (stream WatchEvent);                         // snapshot, changes, heartbeat 15 s, cursor
  rpc Cancel(CancelRequest) returns (CancelResponse);                          // a task or the whole turn
  rpc AnswerQuestion(AnswerQuestionRequest) returns (AnswerQuestionResponse);  // for INPUT_REQUIRED questions only
  rpc ListAgents(ListAgentsRequest) returns (ListAgentsResponse) { option idempotency_level = NO_SIDE_EFFECTS; }
}
// WatchEvent: ThreadSnapshot | MessageAdded | MessageDelta | TaskUpdated | ArtifactAdded | QuestionAsked | ApprovalRequested | Heartbeat
// Part: text | data (json, `untrusted` flag) | url | file_ref ; Artifact.kind picks a card (issue, pr, error, metric, run, approval, thread)
// TaskRef { agent, task_id, state, status_text, approval_id? }   // states are A2A's, as strings
```

`ApprovalRequested` carries `approval_id` and `revision` only; the details come from `loams.approvals.v1`. **There is no `Approve` or `Decide` RPC in `BotService`** (a compile-level guarantee that Task 4 tests).

**Tests:** `buf lint`, `buf breaking`; golden JSON for every event kind round-trips through the generated TS, Swift and Kotlin types (`events_golden`); mock tests: `watch_sends_snapshot_then_changes_then_heartbeat`, `watch_resumes_from_cursor`, `send_is_idempotent_on_key`, `no_decide_rpc_exists` (a reflection test over the service descriptor).

**Commit:** `proto: loams.bot.v1`.

### Task 2: `loams-bot`: threads as durable executions

**Files:** `crates/operon-bot/src/{lib.rs,service.rs,threads.rs}`, `tests/{main.rs,service.rs,threads.rs}`.

**Produces:**

```rust
#[async_trait]
pub trait ThreadStore: Send + Sync {
    async fn create(&self, cx: &Ctx, idem: &IdempotencyKey, title: Option<String>) -> Result<Thread, BotError>;
    async fn append(&self, cx: &Ctx, thread: &ThreadId, ev: ThreadEvent) -> Result<Cursor, BotError>;   // idempotent on event id
    async fn read(&self, cx: &Ctx, thread: &ThreadId, from: Option<Cursor>) -> BoxStream<'static, ThreadEvent>;
    async fn list(&self, cx: &Ctx, page: Page) -> Result<(Vec<ThreadSummary>, Option<Cursor>), BotError>;
}
```

**Semantics:** threads belong to a user and an environment (Live table `bot_threads`, events in the stream `bot_events`, kept 90 days, a per-org setting; the transcript holds no secret). `Send` appends the user message, starts or continues the thread's durable execution (a Resonate function keyed by the thread id and message id) and returns immediately; `Watch` streams. A thread has one running turn at a time; a second `Send` while a turn runs is queued as the harness's follow-up. Cancel cancels the turn and every in-flight A2A task. A user sees only their threads (OpenFGA `bot_thread#owner`); org admins cannot read content (Q476 sets whether a legal-hold read exists).

**Tests:** `create_is_idempotent`; `send_appends_and_returns`; `second_send_queues_as_followup`; `watch_streams_events_in_order`; `watch_resumes_from_cursor`; `thread_resumes_after_host_crash` (failpoint kills the host process mid-turn; the turn resumes from its last checkpoint and a model call already made is not made twice); `other_users_thread_is_not_found`; `cancel_cancels_tasks` (fake A2A client records `CancelTask`); `retention_prunes_old_events`.

**Commit:** `bot: durable chat threads and the Connect service`.

### Task 3: The harness host and `subagent-a2a`

**Files:** `crates/operon-bot/src/{host.rs,channel.rs,a2a.rs}`, `web/host-bot/**`, tests `host.rs`, `canary.rs`, `web/host-bot/test/*`.

**Produces:**

```rust
pub struct HostManager { /* spawns `node web/host-bot/dist/main.js` (or bun) per pool slot; health, restart with backoff */ }
// Channel (newline-delimited JSON-RPC over stdio): host -> bot: `a2a.send`, `a2a.stream`, `a2a.cancel`, `a2a.list_agents`, `ask_user`, `request_approval`, `emit`;
//                                                    bot -> host: `turn.start`, `turn.followup`, `turn.cancel`, `answer`, `resume`.
```

```ts
// web/host-bot/src/provider.ts — registers on ctx.subagents
export const a2aSubagentProvider: SubagentProvider = {
  id: "a2a",
  async start(req) { /* req.agent = "plane" etc.; sends a2a.send via the channel; returns a live run */ },
  async continue(run, msg) { /* A2A SendMessage with taskId and contextId */ },
  async cancel(run) { /* a2a.cancel */ },
};
```

**Semantics:** `loams-bot` spawns the host with no environment secrets, passes a per-thread **host token** that is a channel capability, not a bearer: 256 random bits, delivered over an inherited file descriptor (never argv or the environment), bound to one thread id and to the host process's life, never valid on the Connect listener or against any other thread, and authorising exactly these channel calls for that thread: `a2a.send`, `a2a.stream`, `a2a.cancel`, `a2a.list_agents`, `ask_user`, `request_approval` (which creates an approval and can never decide one) and `emit`. A replayed or foreign-thread use is refused and logged. It grants no network access. `a2a.*` requests from the host are executed by `loams-bot`'s `A2aClient`: it exchanges the user's token for an agent-audience token (SF2 Task 4), signs nothing the host can see, forwards `traceparent`, and streams results back as `emit` events. The harness `userQuestions` and approval services are bound to `ask_user` and `request_approval`: they create `INPUT_REQUIRED` handling in `loams-bot` (Task 4). Agent skills from verified cards become tool descriptions (`delegate_to_<agent>` with the skills in its description); `@agent` in a message forces `delegate_to_<agent>` as the first call. The default model route is DeepSeek through the AI gateway; the host calls the gateway through a channel method that attaches the thread's token, so the host never holds a gateway key.

**Tests:** Rust: `host_restarts_with_backoff`; `host_channel_carries_no_bearer`; `canary_token_never_reaches_host` (a canary bearer is planted and the host token is a second canary; every byte on the channel and the host's stdout, stderr and environment is scanned for the bearer, and the host token must appear only in the channel handshake, never in a log or span); `a2a_send_adds_traceparent`; `a2a_call_uses_audience_bound_token`; `host_crash_does_not_lose_thread` (see Task 2). TS (Vitest): `provider_start_sends_a2a_send_over_channel`; `provider_continue_uses_task_and_context`; `provider_cancel_propagates`; `skills_become_tool_descriptions`; `at_mention_forces_first_call`; `injected_text_in_data_part_does_not_change_tool_choice` (scripted-model corpus as in SF2 Task 9, over the full path with fake agents).

**Commit:** `bot: the harness host and the A2A subagent provider`.

### Task 4: Questions, approvals and the hand-off

**Files:** `crates/operon-bot/src/approvals.rs`, `tests/approvals.rs`.

**Semantics (design §5.3, §8):** an agent task entering `TASK_STATE_INPUT_REQUIRED` with a question `data` part becomes a `QuestionAsked` event (a card with options if given); the user's `AnswerQuestion` becomes the A2A follow-up message. One rule (design §5.3 and §8): Loams Bot relays every `INPUT_REQUIRED` question to the person, and the model may answer one **only** if the agent marked it `answerable_by_orchestrator`; otherwise the model is told it cannot, and the thread waits. Approvals are never answered by the model. An approval (`data: { approval_id, revision }`) becomes `ApprovalRequested`; the client opens `loams.approvals.v1`'s review screen. Loams Bot **does not call `DecideApproval`**; the service has no code path to it, and `loams-bot`'s service account has no `approvals:decide` scope. When the approval is settled the agent's own durable function resumes; the A2A push or the stream then reports the state change, which becomes a `TaskUpdated` event and a push (Task 8). `AUTH_REQUIRED` is an administrator matter and must not expose a transcript: the thread owner sees only a card saying that an administrator must reconnect the app; admins (`factory:admin`) get a separate **admin notice**, a `loams.factory.v1.ListAgents` health state `AUTH_REQUIRED` and a push category `admin.agent_auth` carrying only the agent and app names and a link to the connect-app page, never thread content.

**Tests:** `question_becomes_card_and_answer_becomes_followup`; `unmarked_question_goes_to_the_user`; `marked_question_may_be_answered_by_model`; `approval_requested_event_has_id_and_revision_only`; `approval_decision_through_bot_is_refused` (every route and the host channel; the service account lacks the scope); `settled_approval_resumes_task`; `rejected_approval_ends_task_rejected`; `stale_revision_card_is_updated_not_decided`; `auth_required_owner_sees_no_link_and_admin_notice_has_no_transcript`; `admin_push_carries_only_agent_and_app`.

**Commit:** `bot: questions and approvals without a decision path`.

### Task 5: The desktop harness and views, and the browser console plugin

**Files:** `desktop/crates/loams-harness-bot/**`, `desktop/crates/loams-ui-bot/**` (paths per §37's amendment); `web/plugins/bot/**`, `web/apps/console/catalog/base.yml`.

**5a. Desktop (zeron fork).** `loams-harness-bot` implements `Harness`: `RunRequest` becomes `BotService.Send` (idempotency key = the request id); `Watch` events map to `AgentEvent`s: `MessageDelta` to text deltas, a `TaskUpdated` of an A2A task to a tool call whose name is the agent (`plane`, `forgejo`, …) with its state and status text as the call's progress, `QuestionAsked` to zeron's question event, `ApprovalRequested` to a tool call of kind `approval` that `loams-ui-bot` renders with a **Review** action opening the native approvals view (never a decision), `ArtifactAdded` to a result part that picks a GPUI card. Steering and interrupt map to follow-up `Send` and `Cancel`. The harness appears in zeron's harness and model pickers as "Loams Bot"; its sessions are marked remote and survive app restarts by `Watch`ing from the stored cursor. `loams-ui-bot` adds the cards (issue, PR, error, metric, run, approval, thread: the views of SF1, SF4 and SF5 register by artifact kind), `@agent` mention completion fed by `ListAgents`, the slash commands `/run`, `/status`, `/kill` (a confirm dialog and step-up), `/approvals` (opens the approvals view) and `/route`, and a Loams Bot section in the sidebar with attention sorting by pending questions and approvals.

**5b. Browser console.** `@loams/plugin-bot`: `console.page` at `/bot`, `shell.overlay`, `bot.message.renderer` for `text`, `data` and `url` parts, `bot.card` renderers by kind, `palette.command` ("Ask Loams Bot…", "New thread"). A port, not a copy: the DeepSeek harness's `ui-conversation`, `ui-tool`, `ui-subagent`, `ui-user-questions`, `ui-trajectory`, `ui-input-trigger` and `ui-commands` are adapted to read `rpc.bot` instead of Typert, with their slots mapped onto the console's slot catalog (D425).

**Tests (desktop, GPUI test context and a fake `BotService`):** `harness_maps_events_to_agent_events` (golden: every `WatchEvent` kind); `run_request_sends_with_idempotency_key`; `interrupt_maps_to_cancel`; `restart_resumes_from_cursor_without_duplicates`; `approval_event_renders_review_not_decide`; `question_event_round_trips_answer`; `mention_completion_lists_agents`; `slash_kill_requires_confirmation_and_step_up`; `slash_approvals_opens_view_and_does_not_decide`; `untrusted_parts_render_as_plain_text`; `cards_resolve_by_artifact_kind`; `sync_is_off_in_loams_build` (the profile is Local and no sync client is constructed); `loams_sessions_sort_by_attention`; `harness_trait_conformance` (zeron's own harness test suite, run against `loams-harness-bot` with the fake service).

**Tests (browser console, Vitest, AP0 mock):** `thread_states_golden` (the shared fixture renders to the shared DOM snapshots, ignoring styles); `streaming_appends_deltas`; `reconnect_resumes_without_duplicates`; `mention_forces_agent`; `slash_kill_requires_confirmation`; `slash_approvals_opens_queue_and_does_not_decide`; `approval_card_opens_review_not_decide`; `untrusted_parts_render_as_text`; `card_slot_keys_resolve`; `overlay_and_page_share_state`; `inactive_agents_are_not_offered`. Playwright: `chat_roundtrip_with_fake_agents`; `overlay_toggle_shortcut`.

**Commits:** `desktop: Loams Bot harness and views`, `bot: the browser console chat plugin`.

### Task 6: Artifact cards and chat deep links

**Files:** `web/plugins/{plane,forgejo,zulip}/src/cards/*` (finishing SF1's `bot.card` registrations), `web/plugins/bot/src/deeplink.ts`, `conformance/fixtures/bot/deeplinks.json`.

**Semantics:** each card shows the artifact's essentials and offers **navigation actions only** (open in the pane, open in the browser, open the approval). Deep links: `loams://bot/threads/<id>`, `loams://bot/threads/<id>#task=<task id>`, `loams://factory/runs/<id>` (SF4), `loams://approvals/<id>` (existing). The desktop (Rust), the browser console, Swift and Kotlin parse against the same allowlist; the golden file is shared by all four.

**Tests:** `card_actions_are_navigation_only` (the card API has no mutating callback); `deeplink_table` (every row); `unknown_path_is_dropped`; `deeplink_opens_thread_at_task`.

**Commit:** `bot: artifact cards and deep links`.

### Task 7: iOS chat

**Files (`loams-mobile`):** `ios/Sources/Bot/{BotView.swift,ThreadView.swift,Composer.swift,Cards/*.swift,BotClient.swift,Deeplinks.swift}`, `ios/Tests/BotTests/*`.

**Semantics:** SwiftUI over connect-swift with the binary codec. The thread view streams `Watch`, reconnects with the cursor, shows subagent cards, question cards (tap an option or type) and approval cards with **Review**, which opens the existing approval screen and its biometric proof (D435). Artifact cards are native views (issue, PR, error, metric, run) with "Open in browser" buttons that call `UIApplication.open`. Transcripts are cached with a freshness stamp, read-only offline; the composer is disabled offline with a message. VoiceOver labels on every card; Dynamic Type; the keyboard avoids the composer.

**Tests (XCTest and snapshot tests):** `events_golden` (decodes the shared fixtures); `thread_states_golden`; `reconnect_resumes_from_cursor`; `offline_is_read_only`; `approval_card_opens_review_screen`; `question_option_sends_answer`; `untrusted_text_is_plain`; `deeplink_table`; `voiceover_labels_present` (accessibility audit on each card).

**Commit:** `ios: Loams Bot chat`.

### Task 8: Android chat, and push on both

**Files (`loams-mobile`):** `android/app/src/main/java/.../bot/{BotScreen.kt,ThreadScreen.kt,Composer.kt,cards/*.kt,BotClient.kt,Deeplinks.kt}`, push handlers on both platforms, `conformance/fixtures/bot/push.json`.

**Semantics:** as Task 7 in Compose with connect-kotlin over OkHttp. **Push** (D436) categories added by SF3: `bot.task` (an agent finished or needs input), `bot.question` (INPUT_REQUIRED with a question), `bot.approval` (reuses `approvals`), each a sealed notification with a title, a short body, and a deep link to the thread (`loams://bot/threads/<id>#task=<task id>`) or the approval; the engine projects the CloudEvent `io.loams.dev.bot.task.updated.v1` (from SF2's push receiver) through the notifier. Quiet hours and per-category preferences are D436's; `bot.question` and `bot.approval` may bypass quiet hours by the user's setting. **A tap opens the screen and does nothing else.** Android: a `FirebaseMessagingService` and the UnifiedPush flavour; iOS: the notification service extension. If the sealed payload cannot be opened, the generic text stays and the inbox syncs.

**Tests:** Kotlin: `events_golden`; `thread_states_golden`; `compose_snapshot_per_card`; `reconnect_resumes_from_cursor`; `offline_is_read_only`; `push_payload_golden` (the `push.json` rows open sealed payloads to the expected deep links); `tap_navigates_only`; `quiet_hours_respect_category`; `deeplink_table`; TalkBack labels. Swift: `push_payload_golden`, `tap_navigates_only`. Server (Rust, in `loams-bot`): `task_update_becomes_cloudevent`; `notifier_targets_threads_owner_devices_only`; `sealed_notification_has_no_content_beyond_title_and_short_body`.

**Commit (per repository):** `android: Loams Bot chat and push`, `ios: bot push`.

### Task 9: Loams Bot as an A2A server (off by default)

**Files:** `crates/operon-bot/src/card.rs`, `tests/a2a_server.rs`.

**Semantics (Q469):** with `--bot-a2a`, the bot listener also serves SF2's `A2aServer` for an agent `loams-bot`: skills `ask` (read, write by delegation), `status` (read), `start_run` (write); the card is signed and requires a token whose subject is a user or a service account with `bot:invoke`; each external call becomes a thread owned by the token's principal: a user for a user token, and for a service-account token the **service account itself** (there is no user mapping; its threads are readable only by it and by holders of `bot:admin`, who see metadata, not content; user-scoped delegation is unavailable to it and its agent tokens carry the service account as `sub`), with the same policy and gates as a chat (a delegated destructive action still needs a human's approval, and the external caller cannot decide it). Rate limits per principal.

**Tests:** `disabled_by_default`; `card_is_signed_and_schema_valid`; `external_call_creates_owned_thread`; `external_caller_cannot_decide_approvals`; `rate_limit_per_principal`; `python_client_interop`.

**Commit:** `bot: serve Loams Bot over A2A when enabled`.

### Task 10: Exit gate

**Files:** `docs/plans/sf3-exit-report.md`, `docs/design/39-…`, `THIRD_PARTY_NOTICES.md`, `CHANGELOG.md`.

**Exit gate (all in CI):** Tasks 1–9 tests; **a full-path scenario** on the SF1/SF2 harness: from the desktop (the zeron fork, or the standalone GPUI harness if the fork has not landed), `@plane create an issue for the checkout 500s`, see the subagent card stream, the Plane issue card, open it in the embedded pane; `@forgejo open a PR from the canned patch`, get an approval card, open it in the approvals screen, approve with a session proof, see the task complete and the PR card update; on a simulator, the same thread resumes and a push opens it; kill the host process mid-turn and see the thread resume; the canary token scan; the prompt-injection corpus; a 30-minute soak with 20 threads and a reconnect storm (record memory and CPU). Record host image size and per-session memory.

**Commit:** `docs: SF3 exit report`.
