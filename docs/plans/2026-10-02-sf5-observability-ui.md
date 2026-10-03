# SF5 — Observability UIs and Agents: GlitchTip, OpenPanel, Langfuse 4, OpenObserve Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, headers, attributes, endpoints), use them verbatim, except where marked **(verify)**: Task 0 checks those against the pinned images and records the answer. The code is not pre-written in this plan; the tests are the specification.

> **Status: Planned** (2026-10-02). **Slot: track SF, fifth plan, phase 2** (proposed; D-SF-1, D-SF-2, D-SF-11, D-SF-17). Branches `sf5-t<N>`, stacked; PRs target `main`. Depends on SF1 (embed plugin, edge, `loams-collab`, the broker), SF2 (A2A host, agent pattern) and SF4's `Observer` trait and run record. Tasks 1–3, 5 and Task 6's panes and readers need no SF4; **Task 4 needs SF4 Task 3** (the GlitchTip receiver and `Signal` type); Task 6's real `Observer` is built against the trait as SF4 Task 7 specifies it (a stable interface) and merges after SF4 Task 7. Loopback only until the unified auth plan (D111).

**Goal:** Phase 2 of D-SF-1, complete:
- **GlitchTip** and **OpenPanel** in the console (native panels plus embed) and as the **`glitchtip`** and **`analytics`** A2A agents (the loop's signal sources and its observation tools).
- **Langfuse 4** for agent and LLM tracing and **OpenObserve** for distributed tracing, logs and metrics: the OpenTelemetry collector pipelines of design §9.1, the embed panes that open a trace or a pre-filtered search, and the readers the factory's observation stage uses.
- Every agent step traceable end to end, with prompts and completions in Langfuse only.

**Architecture:**
- **`web/plugins/{glitchtip,openpanel,langfuse,openobserve}`**: each `first-party`, with panels (GlitchTip, OpenPanel), embed panes (all four), and cards.
- **`crates/operon-collab`**: GlitchTip and OpenPanel adapters (`GlitchTipApi`, `OpenPanelApi`) and the read-only `LangfuseReader` and `OpenObserveReader`, all behind the broker (SF1).
- **`crates/operon-agent-glitchtip`**, **`crates/operon-agent-analytics`**: A2A agents like SF2's.
- **`deploy/factory/otel/`**: the collector config (two pipelines plus Loams's own OTLP exporter), the Langfuse and OpenObserve profiles (images pinned, S3 on RustFS, Postgres, ClickHouse and Redis for Langfuse), and the Authentik outpost config for forward-auth apps.
- **`crates/operon-factory`** gains the real `Observer` implementation (SF4 Task 7).
- **Desktop (the zeron fork; path per §37's amendment):** `loams-ui-collab` gains GlitchTip error panels and OpenPanel tiles (GPUI), and the trace and log buttons open Langfuse and OpenObserve through SF1's `AppOpener` (system browser, or the sidebar browser where it ships); no embedding of either app is attempted natively.
- **`loams-mobile`**: error list and detail, analytics tiles, the run view's trace timeline.

**Tech Stack:** Rust 1.97.1, edition 2024, `reqwest`, `wiremock`, the OpenTelemetry Collector (`otelcol-contrib`, Apache-2.0; a distribution built with only the needed components by `ocb`, Task 0) with the `filter`, `transform`, `batch` processors and the `otlphttp` exporter; `opentelemetry` and `tracing-opentelemetry` in the Rust services; TypeScript and cordis 4; Vitest, Playwright; Docker Compose and Helm for the profiles. The pinned images: GlitchTip (MIT), OpenPanel (AGPL-3.0), Langfuse web and worker (MIT, `ee/` never enabled), OpenObserve open-source edition (AGPL-3.0). Task 0 records digests and licences into `deploy/factory/images.lock`.

**Spec:**
- [`docs/design/39-software-factory-and-loams-bot.md`](../design/39-software-factory-and-loams-bot.md): §3 (apps, embed, edge, sessions), §4 (licences), §9 (tracing), §10 stage 7, §16; D-SF-2, D-SF-4, D-SF-11, D-SF-17.
- [`docs/design/22-showcase-suite.md`](../design/22-showcase-suite.md): §4.3 (GlitchTip), §5 (SSO), §8.4 (OTLP), §13b (OpenPanel); [`21-durable-execution.md`](../design/21-durable-execution.md) §6.6 (agent traces); D73 and Q43 (OTLP ingest into Loams).
- [`docs/plans/2026-10-02-sf1-collab-ui-plugins.md`](2026-10-02-sf1-collab-ui-plugins.md), [`2026-10-02-sf2-a2a-agents.md`](2026-10-02-sf2-a2a-agents.md), [`2026-10-02-sf4-factory-loop.md`](2026-10-02-sf4-factory-loop.md).
- Upstream documentation, re-read at Task 0: GlitchTip's API and alert webhooks, OpenPanel's API and its self-hosting compose, Langfuse's OpenTelemetry and public API pages and self-hosting guide, OpenObserve's OTLP ingestion pages.

## Global Constraints

Same as SF1 and SF2, plus:
- **Unmodified apps** (D-SF-17). **Langfuse's `ee/` features are never switched on**; CI greps the Langfuse environment for enterprise licence keys. **OpenObserve runs the open-source edition only.**
- **Model content goes to Langfuse only.** The OpenObserve and Loams pipelines delete `gen_ai.prompt`, `gen_ai.completion` and `gen_ai.*.content`; a test proves it with a content canary.
- **No per-person identity is claimed for OpenPanel or OpenObserve** (forward-auth, one service user). The UI says so on the pane's toolbar tooltip.
- **Both credential-bearing hops use TLS** (HTTPS or the cluster's mTLS): the collector to Langfuse and OpenObserve, and the Authentik outpost to OpenPanel and OpenObserve. A test fails on an `http://` endpoint outside loopback.
- **Readers are read-only** and use their own, minimally scoped credentials from the broker; they cannot ingest or delete.
- **Collector config is declarative and tested** with `otelcol validate` and golden pipelines; no hand-edited file in a chart.
- **The build machine.** One cargo build at a time; the Langfuse profile (ClickHouse, Redis, Postgres, S3) and OpenObserve run one at a time in tests; stop and report if `/home` has under 8 GB free.
- **Commit areas:** `glitchtip`, `openpanel`, `langfuse`, `openobserve`, `otel`, `collab`, `factory`, `web`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **Langfuse is an optional profile**, off in `loams dev`; the console run view works from Loams's own OTLP store and the factory's events without it | Langfuse 4 needs ClickHouse, Redis, Postgres and S3 | Without Langfuse there is no LLM trace viewer; the run view still shows step summaries |
| 2 | **OpenObserve is an optional profile**, behind forward-auth | AGPL, no SSO or RBAC in the open edition | Users reach all signals or none; per-person audit is Authentik's |
| 3 | **Traces open in Langfuse by trace id through its UI path; sessions by `contextId`** | One stable link per run and per chat thread | Langfuse's UI paths may change; `embed_url` templates are in the app registry, not code |
| 4 | **OTLP to Langfuse over HTTP with its project key pair as basic auth; to OpenObserve over HTTP with basic auth** | Both document it | gRPC is not used; HTTP is enough at this volume |
| 5 | **The collector is the only exporter.** Services never export to Langfuse or OpenObserve directly | One place to filter content and rotate credentials | A collector outage drops spans after the batch queue; the queue is persistent (file storage) |
| 6 | **GlitchTip alerts start signals through webhooks; OpenPanel anomalies are found by a scheduled agent call** | OpenPanel has no alert webhooks we rely on **(verify)** | A polling lag of one interval (default 10 min) |

## Review Focus

1. **Content only in Langfuse.** Tests: Task 3 (`content_never_reaches_openobserve_or_loam`).
2. **One trace across the whole path.** Tests: Task 5 (`trace_spans_chat_a2a_agent_app_call`).
3. **Forward-auth and framing are right.** Tests: Task 2.
4. **Readers cannot write.** Tests: Task 6.
5. **Signals dedupe and map to the factory.** Tests: Task 4.

## File structure

```
crates/operon-collab/src/apps/{glitchtip.rs,openpanel.rs,langfuse.rs,openobserve.rs}   # the last two are readers
crates/operon-agent-glitchtip/src/*   crates/operon-agent-analytics/src/*
crates/operon-factory/src/observer.rs
deploy/factory/otel/{collector.yaml,collector.golden/*.yaml,README.md}
deploy/factory/profiles/{langfuse.compose.yml,openobserve.compose.yml,openpanel.compose.yml,glitchtip.compose.yml}  deploy/factory/chart/templates/{langfuse,openobserve,openpanel,glitchtip}.yaml
deploy/factory/edge/{authentik-outpost.yaml,routes.phase2.*}   deploy/factory/images.lock   LICENSES.md
web/plugins/{glitchtip,openpanel,langfuse,openobserve}/{package.json,src/*,test/*}
ios/Sources/{Errors,Analytics}/*   android/app/src/main/java/.../{errors,analytics}/*
docs/design/39-…  docs/plans/README.md  CHANGELOG.md
```

### Task 0: Reconcile, verify and pin

**Files:** `docs/plans/sf5-spike.md`, `deploy/factory/images.lock`, `LICENSES.md`.

**Checks (record each with the command, image digest and date):**
- **Langfuse 4:** the current release; its self-hosting requirements (Postgres, ClickHouse, Redis, S3) and the minimal compose; the OTLP endpoint path and the headers it needs (**verify** `/api/public/otel/v1/traces`, basic auth with the public and secret key, and any ingestion-version header); how generic OIDC is configured (`AUTH_CUSTOM_*`) and whether it is in the MIT tree; that no `ee/` code path activates without a licence key; how sessions and users map from OTLP attributes (`langfuse.session.id`, `langfuse.user.id`, `session.id`, `user.id` **verify**); the URL paths for a trace and a session page.
- **OpenObserve:** the current release; the OTLP HTTP paths for traces, logs and metrics (`/api/<org>/v1/traces` and the rest) and gRPC port; the open-edition auth model (root user, basic auth, API tokens); the search API for queries by attribute; the UI URL parameters for a pre-filled query; that SSO is absent in the open edition.
- **GlitchTip:** the `/api/0` endpoints for organisations, projects, issues, events and releases; token model; alert webhook payload (project, issue id, level, count, release, URL); OIDC settings; the framing headers.
- **OpenPanel:** the read API (project, client id and secret, events, funnels, metrics endpoints), rate limits, whether alert or webhook features exist; the framing headers; the share-dashboard feature; the exact forward-auth need.
- **OTel collector:** which components are needed (`otlp` receiver, `filter`, `transform`, `batch`, `otlphttp`, `file_storage`), the `ocb` build, size, and the `gen_ai.*` attribute names in the semantic conventions as of the pinned `aisix` and OpenTelemetry versions (**verify**; they have changed between releases).
- **Authentik outpost:** the proxy provider settings for forward-auth in single-application mode, and how to pass a trusted header or inject basic auth.
- The licence line and source URL of each image for `LICENSES.md`.

**Commit:** `docs: reconcile SF5 and pin the phase-2 apps`.

### Task 1: GlitchTip adapter, plugin and the `glitchtip` agent

**Files:** `crates/operon-collab/src/apps/glitchtip.rs`, `crates/operon-agent-glitchtip/**`, `web/plugins/glitchtip/**`, tests in each.

**Produces:** `GlitchTipApi` (`projects`, `issues`, `issue`, `events`, `releases`, `resolve`, `ignore`); `loams.collab.v1` additions `ListErrorIssues`, `GetErrorIssue`, `ResolveErrorIssue` (write, idempotent), `ErrorIssue` messages (id, title, level, count, first and last seen, release, culprit, `top_frames` capped at 20, all free text `untrusted`); `@loams/plugin-glitchtip` (`console.page` `/errors`: list, filters, detail with stack trace and breadcrumbs, resolve and ignore; `embed.pane#glitchtip`; `bot.card#glitchtip.issue`; overview card with new and regressed counts); the **`glitchtip` agent**: `issues.search`, `issues.get`, `events.get`, `releases.list` (read), `issues.resolve`, `issues.ignore` (write).

**Tests:** `list_and_detail_from_fixtures`; `stack_trace_truncates_at_20_frames`; `resolve_is_idempotent`; `free_text_is_untrusted`; `agent_card_schema_and_signature`; `agent_search_by_release`; `canary_secret_never_appears`; `framing_headers_replaced_by_edge` (harness); `plugin_inactive_when_not_listed`; `deeplink_opens_pane_at_issue`.

**Desktop:** a GPUI error list and detail panel and the error card, with "Open in GlitchTip" through `AppOpener`; tests (GPUI test context): `error_panel_lists_and_filters`, `stack_trace_is_plain_text`, `open_in_glitchtip_uses_opener`.

**Commit:** `glitchtip: adapter, plugin, desktop panel and agent`.

### Task 2: OpenPanel adapter, plugin and the `analytics` agent; the forward-auth edge

**Files:** `crates/operon-collab/src/apps/openpanel.rs`, `crates/operon-agent-analytics/**`, `web/plugins/openpanel/**`, `deploy/factory/edge/{authentik-outpost.yaml,routes.phase2.*}`.

**Produces:** `OpenPanelApi` (`events`, `metrics`, `funnel`); `MetricSeries` messages; `@loams/plugin-openpanel` (tiles on the console overview and in a page: events, active users, a configured funnel; `embed.pane#openpanel` behind forward-auth; `bot.card#analytics.metric`); the **`analytics` agent**: `metrics.query`, `funnel.get` (read) and `anomaly.check` (read; compares a series to a baseline: the median and the median absolute deviation of the same weekday-hour over the previous 4 weeks (estimate), flagging above `3 × MAD`; thresholds are policy). A scheduled durable function (every 10 min, per watched metric in the policy) calls `anomaly.check` and publishes a `Signal` to the factory (SF4 Task 3). The Authentik outpost routes `analytics.` and `obs.`: authenticate against Authentik, require group `factory-viewers`, inject basic auth for the app's service user.

**Tests:** `metrics_from_fixtures`; `anomaly_flags_3_mad`; `anomaly_ignores_seasonal_pattern` (a recorded weekly cycle); `scheduler_publishes_signal_once_per_window`; `forward_auth_denies_without_group` (harness: Authentik container, a user outside the group gets 403); `forward_auth_injects_service_credentials_server_side` (the browser never sees them); `framing_allowed_only_for_console_origin`; `plugin_states_no_per_user_identity` (the tooltip text); `agent_has_no_write_skills` (the card).

**Desktop:** GPUI metric tiles and the metric card; tests: `tiles_render_from_series`, `tiles_state_no_per_user_identity`, `open_in_openpanel_uses_opener`.

**Commit:** `openpanel: adapter, plugin, desktop tiles, analytics agent and forward-auth`.

### Task 3: The collector and the two pipelines

**Files:** `deploy/factory/otel/**`, `deploy/factory/profiles/{langfuse,openobserve}.compose.yml`, chart templates, tests under `deploy/factory/otel/test/`.

**Semantics (design §9.1):** receivers: OTLP gRPC and HTTP on loopback in `loams dev`, cluster-internal otherwise. Pipelines: `traces/llm` (the Filter Processor cannot keep a parent because a child matches, so selection is **trace-aware**: a `tail_sampling` stage (or a `groupbytrace` plus a policy) keeps every whole trace that contains a span with a `gen_ai.*` attribute or an agent-step attribute (`a2a.task_id`, `resonate.promise_id`), with a decision wait of 30 s (estimate) and a span cap, then a masking step that removes secret-canary matches and `untrusted` bodies over 2 KB and fails closed; `batch`; exporter `otlphttp/langfuse`); `traces/all` (a `transform` processor deleting `gen_ai.prompt`, `gen_ai.completion` and keys matching `gen_ai.*.content`, and truncating attribute values over 1 KiB; `batch`; exporters `otlphttp/openobserve` and `otlphttp/loam`); `logs` and `metrics` (the same deletion; OpenObserve and Loams). `file_storage` for the sending queue. Credentials come from the broker's secret store as environment variables of the collector pod, never in the config.

**Tests:** `otelcol validate` on every config; golden pipelines (`collector.golden/*.yaml`); with the compose harness and a synthetic span set: `llm_spans_reach_langfuse_with_content`; `selected_trace_includes_parent_spans` (a step span with no model call arrives with all its ancestors and siblings); `content_never_reaches_openobserve_or_loam` (a content canary in `gen_ai.prompt` and in an attribute named `gen_ai.tool.content`; scan OpenObserve's stored data and Loams's stream); `non_llm_spans_do_not_reach_langfuse`; `collector_restart_drains_queue`; `bad_credentials_drop_with_a_metric_not_a_crash`; `credentials_not_in_config_or_logs`; `langfuse_ee_not_enabled` (the container env has no licence key and the instance reports no EE features).

**Commit:** `otel: the collector, two pipelines and the Langfuse and OpenObserve profiles`.

### Task 4: Signals, and the GlitchTip webhook into the factory

**Files:** `crates/operon-factory/src/hooks.rs` additions, `crates/operon-factory/tests/intake.rs`, provisioning saga additions (`deploy/factory/chart`).

**Semantics:** a provisioning saga (§22 §7.3 style) creates, in GlitchTip, a project alert rule with the factory webhook for each project in the policy; in OpenPanel, the watched-metric list in the policy; both idempotent and reconciled on a schedule. The GlitchTip receiver (SF4 Task 3) maps level and count to severity; the fingerprint is the GlitchTip issue id; the signal's evidence references are issue and event ids, which the triage stage reads through the agents. Fixtures use recorded real payloads from the pinned image.

**Tests:** `alert_rule_is_created_once`; `saga_reconciles_deleted_rule`; `recorded_payload_maps_to_signal` (golden); `severity_thresholds_follow_policy`; `anomaly_signal_has_metric_and_window`; `signals_end_to_end_with_real_glitchtip` (harness: send an event with a Sentry SDK DSN, see a run open).

**Commit:** `factory: GlitchTip and analytics signals`.

### Task 5: Tracing wired through every layer

**Files:** `crates/operon-a2a/src/trace.rs` (SF2) extended, `crates/operon-bot`, `crates/operon-factory`, `crates/operon-collab`, tests.

**Semantics:** every service uses the OTel SDK with the collector as the exporter; resource attributes `service.name`, `loams.instance`, `loams.env`; span attributes as design §9: `a2a.task_id`, `a2a.context_id`, `a2a.agent`, `factory.run_id`, `factory.stage`, `resonate.promise_id`, `resonate.origin`, `enduser.id` (a hash); for Langfuse, the attributes it maps to session and user (`session.id` = `contextId`, `user.id` = the hash; exact names per Task 0) so a chat thread is a Langfuse session and a factory run is a trace group. The AI gateway's `gen_ai.*` spans nest under the step span. The run record stores the root trace id of each stage, and `embed_url` templates build the Langfuse trace and OpenObserve search URLs from it.

**Tests:** `trace_spans_chat_a2a_agent_app_call` (one trace id from `loams.bot.v1.Send` through `A2aClient`, the agent, the broker and the app call; parentage correct); `a2a_hops_forward_traceparent_and_tracestate`; `session_id_equals_context_id`; `user_id_is_a_hash_not_an_identifier`; `app_call_spans_have_no_bodies`; `run_record_holds_root_trace_ids`; `langfuse_shows_session_for_thread` and `openobserve_search_by_run_id_finds_all_spans` (harness, with the real apps).

**Commit:** `otel: one trace across chat, A2A, agents and apps`.

### Task 6: Langfuse and OpenObserve panes, readers and the factory `Observer`

**Files:** `web/plugins/{langfuse,openobserve}/**`, `crates/operon-collab/src/apps/{langfuse.rs,openobserve.rs}`, `crates/operon-factory/src/observer.rs`.

**Produces:** `@loams/plugin-langfuse` (`embed.pane#langfuse` at a trace, session or score page by id; `bot.card#langfuse.trace` with model, latency, cost and score from the run record, not from a Langfuse query; a "Traces for this run" button in run detail) and `@loams/plugin-openobserve` (`embed.pane#openobserve` at a search pre-filtered by `factory.run_id`, behind forward-auth; a "Logs and traces for this run" button); `LangfuseReader` (`scores(trace_ids)`, read-only key) and `OpenObserveReader` (`error_rate(service, window)`, `p95_latency(service, window)`, read-only credentials); the real **`Observer`** for SF4's observe stage combining the GlitchTip and analytics agents with the two readers.

**Tests:** `langfuse_pane_opens_trace_by_id`; `openobserve_pane_prefilters_by_run_id`; `trace_card_uses_run_record_only`; `readers_are_read_only` (the credentials are rejected on every write endpoint of the apps; the reader has no write method); `observer_combines_four_sources`; `observer_missing_source_is_inconclusive`; `readers_use_minimal_scope_credentials`; `buttons_hidden_when_apps_absent`; `embed_signs_in_via_oidc_langfuse` and `embed_via_forward_auth_openobserve` (harness).

**Commit:** `langfuse, openobserve: panes, readers and the factory observer`.

### Task 7: Mobile surfaces

**Files (`loams-mobile`):** `ios/Sources/{Errors,Analytics,Factory/Trace}/*`, `android/.../{errors,analytics,factory/trace}/*`, golden fixtures.

**Semantics:** native error list and detail (GlitchTip) with push on a new or regressed issue (category `errors`, sealed, deep link `loams://app/<env>/glitchtip/<project>/issues/<id>`); analytics tiles on the home screen (series, sparkline, last updated; read-only); the run timeline gains a **step trace** list built from `factory_events` (agent, duration, cost, model name) with "Open in browser" buttons for Langfuse and OpenObserve (system browser, SSO via the browser session). No embed, no editor.

**Tests (Swift and Kotlin):** `errors_golden`; `stack_trace_renders_as_plain_text`; `tiles_golden`; `stale_tiles_are_marked`; `trace_list_golden`; `open_in_browser_uses_system_browser`; `push_payload_golden` (errors); `tap_navigates_only`; `deeplink_table` (the phase-2 rows).

**Commit:** `apps: error, analytics and trace views`.

### Task 8: Exit gate

**Files:** `docs/plans/sf5-exit-report.md`, `docs/design/39-…`, `LICENSES.md`, `CHANGELOG.md`.

**Exit gate (all in CI):** Tasks 1–7 tests on the harness with the four phase-2 apps plus phase 1; a full scenario: send an error to GlitchTip with a Sentry SDK, a run opens, the thread shows the evidence, a chat command `@analytics funnel checkout` returns a tile card, the fix PR and approval proceed through SF4's fakes for the model, and after deploy the observe stage reads GlitchTip, OpenPanel, Langfuse and OpenObserve; open the run's trace in Langfuse (an embedded pane) and its logs in OpenObserve (a pane behind forward-auth); the content-canary and secret-canary scans; the licence table check including "no `ee/` activated"; the forward-auth denial test. Record the resource use of each profile (memory and disk after one hour at the test load), the collector's CPU, the image sizes, and every gap in the apps' APIs found.

**Commit:** `docs: SF5 exit report`.
