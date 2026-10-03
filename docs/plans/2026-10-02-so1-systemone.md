# SO1 — Loams SystemOne: Engine, API and Backends Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, flags, field names, limits), use them verbatim. The code is not pre-written in this plan.

> **Status: Planned** (2026-10-02). **Slot: track SO, first plan** (proposed; D520–D539). Branches `so1-t<N>`, stacked; PRs target `main`. Task 0 gates Tasks 5 and 12's in-process backend only; Tasks 1 to 4 and 6 to 11 do not depend on it. The desktop half is [SO2](2026-10-02-so2-systemone-desktop.md).

**Goal:** Ship Loams SystemOne's engine side: one typed-decision API (`choice`, `score`, `noul`) in front of interchangeable backends, chosen from the host:
- **`loams.systemone.v1`** (Connect) and **`POST /v1/systemone`** (a superset of the Jev and `laya-serve` JSON), with validation limits set to the largest any backend accepts and per-backend caps applied by routing;
- the crate **`loams-systemone`**: canonical types and wire, the `DecisionBackend` trait, a registry, `HostProfile`, a pure routing function, calibration, the self-test, a model manager;
- adapters: **`LayaServeBackend`** (a supervised loopback `laya-serve` sidecar with a generated key), **`JevBackend`** (off by default, bring your own key), a `FakeBackend`, and, if Task 0's spike passes, **`LayaNativeBackend`** (kevala, in-process);
- the server route, `loams systemone …` CLI verbs and a read-only MCP tool, the local decision log and export, and the docs.

**Architecture:**
- **`proto/loams/systemone/v1/systemone.proto`**: the service and messages of design §40 §4.1, `buf lint` STANDARD and `buf breaking` FILE (the app-protos CI job).
- **`crates/loams-systemone/`** (no DataFusion, no storage, no GPUI; tokio, serde, reqwest, axum only for the test fake): `types.rs` (canonical request/response), `wire.rs` (JSON in Jev/Laya shape), `limits.rs`, `backend.rs` (trait, `Capabilities`, `BackendStatus`), `registry.rs`, `host.rs` (`HostProfile`), `route.rs` (`plan`), `calibrate.rs`, `selftest.rs` (+ `selftest_cases.json`), `models/` (lock, downloader, cache), `sidecar.rs` (supervisor), `backends/{laya_serve.rs,jev.rs,fake.rs,native.rs}`, `declog.rs`.
- **`crates/loams/`**: `systemone` module (Connect service impl, the axum route on the gateway role, config), CLI verbs.
- **`crates/loams-link`** is untouched here (SO2).

**Tech Stack:** Rust 1.97.1, edition 2024, workspace lints; connect-rust and buffa (D128); `reqwest` (rustls) for HTTP clients, `tokio`, `serde_json`, `sha2`, `async-trait` (as the workspace already uses it; Task 3 confirms), `thiserror`. Dev: `axum` for the fake sidecar, `wiremock` for Jev, `proptest`. Python side (Task 5): `uv` and `laya[serve]==<pin>` installed by the CLI, not a build dependency. kevala only if Task 0 passes (git dependency pinned to a commit, Apache-2.0).

**Spec:**
- [`docs/design/40-loams-systemone.md`](../design/40-loams-systemone.md): all of it, especially §2 (what was verified and corrected), §4, §5, §6, §9; [decision log](../design/13-decision-log.md) (D520–D539, Q520–Q539).
- [`docs/design/13-decision-log.md`](../design/13-decision-log.md): D111 (loopback), D128 (connect-rust), D220 (open core), D284 (no telemetry), D-SF-11 (no content in spans).
- [`docs/design/39-software-factory-and-loams-bot.md`](../design/39-software-factory-and-loams-bot.md): D-SF-9 (approvals), D-SF-11.
- Upstream, re-read in Task 0: Laya `docs/http-api.md`, `docs/security.md`, `README.md` (Honest Limits); `bvolpato/kevala` `docs/architecture.md`; docs.typesafe.ai primitives.

## Global Constraints

- **Local by default.** Locality is `LOCAL_ONLY` unless the request says `ALLOW_CLOUD` and the instance switch and a key exist. A forced cloud backend with local-only data is `CLOUD_NOT_ALLOWED`, never a silent fallback.
- **Loopback only for sidecars (D111).** The adapter refuses a non-loopback address. The sidecar is started with `LAYA_HOST=127.0.0.1` and a generated key in its environment; the key is never on a command line, never logged, never written to disk.
- **Policy above the trait.** Calibration, abstention, `act_probability` stripping and provenance are applied in `Registry::decide`, never in an adapter.
- **A decision is advisory (D537).** No code path in this plan lets a SystemOne answer approve, merge, delete, deploy or grant.
- **No content in logs, spans or errors.** State, questions and answers appear nowhere except the response and the opt-in local decision log.
- **Never copy** `nvkudva/laya-web` (no licence) or any code without a licence. kevala, Laya, FluidUse and `laya-ts` are Apache-2.0: copied files keep their notice in `THIRD_PARTY_NOTICES.md`.
- **Weights are verified before they are parsed** (SHA-256), and a revision directory is immutable.
- **The build machine.** One cargo build at a time, the shared target, `CARGO_BUILD_JOBS=4`. Python installs and model downloads only in Tasks 0, 5 and 9, by hand or in the labelled runner, never in unit tests. Stop and report if `/home` has under 8 GB free.
- **Commit areas:** `systemone`, `proto`, `cli`, `docs`, `ci`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **The HTTP shape is Jev's and `laya-serve`'s, plus optional fields.** We accept and ignore unknown fields; we never rename one | `laya-client` and TypeSafe clients work unchanged | A future Jev field we do not model is dropped silently; mitigated by passing unknown response fields through in `extra` for Jev |
| 2 | **`confidence` is passed through; `calibrated_confidence` is ours.** Gating uses the latter | The two vendors define confidence differently (D536) | None |
| 3 | **`criteria` order is the order sent.** Maps are parsed with `serde_json`'s `preserve_order` (and a custom visitor in the proto JSON path) | Option order feeds the prompt; `HashMap` would reorder | Wrong answers that look random; test `option_order_survives_roundtrip` |
| 4 | **Core ML's 32-option bucket is a routing rule, not a validation rule** | The same request is valid on another backend | A user with 40 options on a Mac gets the sidecar or an error instead of a 422 |
| 5 | **No table of calibration ships.** First table comes from a labelled set (Task 7) | A table fitted to someone else's data lies | Raw over-confidence until the user calibrates |
| 6 | **The Jev adapter ships behind a build-time-default-off flag and a runtime key** | Terms unknown (Q520) | None |
| 7 | **Language detection is conservative:** English only when explicit `lang` says so or the detector is sure; otherwise multilingual | The English checkpoint is confidently wrong outside English | English text sometimes takes the weaker multilingual path (0.657 vs 0.783, upstream) |

## Carried in

Design §40 and the pending log. The verified facts of §2 are inputs: re-verify any that a task depends on in Task 0 and record the result in the spike doc.

## Review Focus

1. **Cloud never happens by accident.** Tests: Task 4 (`cloud_requires_all_three_consents`, `forced_jev_with_local_only_fails_closed`, `fallback_respects_locality`).
2. **Sidecar hygiene.** Loopback, key not leaked, group kill. Tests: Task 5 (`non_loopback_is_refused`, `key_not_in_argv_or_logs`, `child_group_killed_on_drop`).
3. **The wire is compatible.** Golden fixtures from Laya's and Jev's docs. Tests: Task 2.
4. **Routing rules exactly as §6.3.** Tests: Task 4's table.
5. **Weights are verified.** Tests: Task 9 (`digest_mismatch_deletes_part`, `verify_before_rename`).
6. **No content leaks.** Tests: Task 10 (`no_state_in_spans_or_errors`).

## File structure

```
proto/loams/systemone/v1/systemone.proto
crates/loams-systemone/Cargo.toml  src/{lib,types,wire,limits,backend,registry,host,route,calibrate,selftest,sidecar,declog}.rs
crates/loams-systemone/src/selftest_cases.json  src/models/{mod,lock,download,cache}.rs  models.lock.toml
crates/loams-systemone/src/backends/{mod,laya_serve,jev,fake,native}.rs
crates/loams-systemone/tests/{wire,route,registry,sidecar,jev,calibrate,selftest,models}.rs  tests/fixtures/{laya_*.json,jev_*.json}
crates/loams/src/systemone/{mod,service,http,config,cli}.rs   crates/loams/tests/systemone/{main.rs,http.rs,connect.rs,cli.rs}
scripts/systemone/{install_laya.sh,bench.sh,parity.py}
docs/plans/so1-systemone-spike.md   docs/design/40-loams-systemone.md   docs/design/13-decision-log.md   docs/plans/README.md   CHANGELOG.md   THIRD_PARTY_NOTICES.md
```

### Task 0: Reconcile and measure (the spike)

**Files:** read `crates/loams/src/{server.rs,main.rs}`, the connect-rust service examples under `crates/loams/src`, `crates/loams-link/src`, `deny.toml`; write `docs/plans/so1-systemone-spike.md`. Fill this plan's "Rulings made during execution" table.

**Checks** (record each with the command and date in the spike doc):
- Re-verify design §2.1's licences on the day (Laya `LICENSE`, HF card, `laya-coreml`, FluidUse, kevala, `laya-ts`), record the commit SHAs you pin, and confirm `nvkudva/laya-web` still has no licence.
- Re-read Laya's `docs/http-api.md` and `docs/security.md`; diff the request/response fields and limits against §4.2; record any change (the pin is `laya==0.3.23`).
- On the build machine, install `laya[serve]` into a throwaway venv under `/mnt/Projects` (never `/tmp`), run `LAYA_HOST=127.0.0.1 LAYA_API_KEY=… laya-serve` on CPU, and measure p50/p95 for 1, 5 and 10 questions of a short state (English and Hindi), cold start, resident memory, and the disk use of the venv. Record `/health` before and after a request.
- **kevala:** build `kevala-cli` at a pinned commit (`CARGO_BUILD_JOBS=4`), convert or fetch the Laya pack, run the same inputs; compare probabilities with `laya-serve` (target: max abs difference under 0.02 on 200 decisions from the golden set), measure native CPU latency and memory, and answer: is there a multilingual pack or converter; does `cargo deny check` pass with it as a git dependency; does `torchpt.rs` run on untrusted input anywhere in the inference path.
- **`ort`:** record the current release (2.0.0-rc.13 on 2026-07-28 or later), whether `load-dynamic` works on the build machine, and a 1-day estimate of the template-parity port (not done unless the owner approves, Q523).
- Whether `FluidUse`'s `LayaManager` can be built as a SwiftPM dependency alone (feeds SO2 Task 0; Mac not required here, read `Package.swift` and the repository).
- Whether an `axum` version and `async-trait` match the workspace's.
- The final decision numbers.

**Output:** the spike doc with a one-line verdict per gate and, for the in-process backend, one of **kevala / ort-with-approval / none**. Task 12 is skipped if the verdict is none.

**Commit:** `docs: SO1 spike results and reconciliation`.

### Task 1: `loams.systemone.v1` and the mock

**Files:** `proto/loams/systemone/v1/systemone.proto`, `crates/loams-apps-mock/src/systemone.rs` (the mock returns canned, deterministic answers), buf config, `docs/design/40-loams-systemone.md` §4.1 if the proto changes.

**Produces:** the proto exactly as design §4.1 (service `SystemOneService` with `Decide`, `DecideBatch`, `ListBackends`, `SelfTest`). The mock implements all four against `FakeBackend` semantics so SO2 and the console can build before the engine exists.

**Semantics:** field numbers as in §4.1 and never reused; `reserved` ranges documented; `Answer.value` is a `oneof`; `Provenance.warnings` are stable strings listed in a comment (`state_truncated`, `options_over_20`, `noul_via_choice_ab`, `fell_back_to:<id>`, `self_test_failed`, `uncalibrated`).

**Tests:** `buf lint` and `buf breaking` clean; `mock_decide_roundtrips` (Connect JSON and binary); `proto_json_matches_http_json` (the canonical JSON of a `DecideResponse` equals the Task 2 wire fixture after the documented key mapping).

**Commit:** `proto: loams.systemone.v1 and the apps mock`.

### Task 2: Canonical types, wire format and limits

**Files:** `crates/loams-systemone/src/{lib,types,wire,limits}.rs`, `tests/wire.rs`, `tests/fixtures/*`.

**Produces:**

```rust
pub enum QuestionType { Choice, Score, Noul }
pub struct Question { pub ty: QuestionType, pub instructions: String, pub criteria: Criteria }
pub enum Criteria { None, Choice(Vec<(String, String)> /* key, description; order kept */), Score(Vec<String>) }
pub struct DecideRequest { pub state: serde_json::Value, pub questions: indexmap::IndexMap<String, Question>,
    pub backend: Option<BackendId>, pub lang: Option<String>, pub min_confidence: Option<f64>,
    pub budget: Budget, pub locality: Locality, pub extra: serde_json::Map<String, Value> /* ignored fields */ }
pub struct Answer { /* type, value, probabilities, legend, confidence, answer_confidence, calibrated_confidence, low_confidence */ }
pub struct DecideResponse { pub model: String, pub answers: IndexMap<String, Answer>, pub usage: Usage, pub loams: Provenance,
    pub extra: serde_json::Map<String, Value> /* unknown backend fields; rendered back only in Compat::Jev, never invented */ }
pub fn parse_request(body: &[u8]) -> Result<DecideRequest, SystemOneError>;       // enforces limits first
pub fn render_response(r: &DecideResponse, compat: Compat) -> serde_json::Value; // Compat::Jev drops `loams` and `calibrated_*` if asked
pub const MAX_BODY: usize = 2 * 1024 * 1024;       // 2 MiB
pub const MAX_STATE_CHARS: usize = 50_000;
pub const MAX_QUESTIONS: usize = 64;
pub const MAX_OPTIONS_PER_CHOICE: usize = 255;    // the largest any backend accepts (Jev); laya-serve's 100 and Core ML's 32 are routing rules
pub const MAX_LEVELS_PER_SCORE: usize = 32;
pub const MAX_OPTIONS_TOTAL: usize = 512;
```

**Semantics:** `criteria` for `choice` is a map or a list of keys (normalised to key plus empty description); for `score` a list; `noul` takes none. Missing or null `state` is `400`. State length is measured as the string itself or `serde_json::to_string` of an object/array with `ensure_ascii=false` semantics. A lone UTF-16 surrogate escape anywhere in the body is `400` (Laya's behaviour). Limit failures are `413`, invalid questions `422` with the question id in the message. Option order is preserved.

**Tests:** `parses_laya_doc_request` and `renders_laya_doc_response` (golden files copied from Laya's `http-api.md` sample, with attribution in the fixture header); `parses_jev_doc_request` (`state`, `model: jev-latest`, choice question); `criteria_list_equals_map_with_empty_descriptions`; `option_order_survives_roundtrip`; `unknown_fields_ignored`; `missing_state_is_400`; `lone_surrogate_is_400`; `limits_413_each` (one case per constant); `locality_unspecified_is_local_only` (HTTP omitted and proto zero value); `http_wire_maps_to_proto_and_back` (the translation of §40 §4.1 in both directions); `choice_list_forms_equal` (map, key list and `{key, description}` list give the same canonical question); `hook_arguments_are_422`; `extra_fields_roundtrip_in_jev_compat_mode`; `noul_has_no_criteria_and_extra_criteria_is_422`; `score_levels_over_32_is_413`; `object_instructions_is_422_unsupported_shape`; `noul_criteria_optional_forwarded_to_jev`; `usage_options_roundtrips`; `act_probability_never_rendered`; `calibrated_confidence_rendered_only_in_loams_mode`.

**Commit:** `systemone: canonical types, Jev/Laya wire and limits`.

### Task 3: The backend trait, the registry and the fake

**Files:** `crates/loams-systemone/src/{backend,registry}.rs`, `src/backends/{mod,fake}.rs`, `tests/registry.rs`.

**Produces:** `Capabilities`, `BackendStatus`, `DecisionBackend`, `BackendRequest`, `RawDecision`, `BackendError`, `Registry::{register, list, decide}` exactly as design §5. `FakeBackend` is deterministic (probabilities from a hash of state and option key), configurable capabilities, injectable failures, latency and truncation.

**Semantics:** `Registry::decide` validates (Task 2), asks `route::plan` (stub returning the only ready backend until Task 4), calls the backend with a deadline (default 10 s, `LOAMS_SYSTEMONE_TIMEOUT_MS`), then applies the policy stage: strip `act_probability`, apply calibration if a table is loaded (Task 7), compute `calibrated_confidence`, mark `low_confidence` against `min_confidence`, fill `Provenance`. A backend error never includes request content.

**Tests:** `registry_lists_status_and_reason`; `policy_strips_act_probability`; `low_confidence_keeps_answer`; `backend_error_has_no_state`; `timeout_maps_to_unavailable`; `batch_preserves_order`; `uncalibrated_warning_when_no_table`.

**Commit:** `systemone: DecisionBackend, registry and the fake backend`.

### Task 4: Host detection and routing

**Files:** `crates/loams-systemone/src/{host,route}.rs`, `tests/route.rs`.

**Produces:** `HostProfile::detect()` (os, arch, `apple_silicon`, NVIDIA via NVML if loadable else `nvidia-smi -L` with a 2 s timeout, threads, RAM, Python, container, role) and `route::plan` (pure) with the selection order and rules of §6.2 and §6.3, `RoutingConfig { backend: Option<BackendId>, fallback: Option<BackendId>, allow_cloud: bool, allow_cloud_fallback: bool, on_truncate: OnTruncate }` read from `LOAMS_SYSTEMONE_BACKEND`, `LOAMS_SYSTEMONE_FALLBACK`, `LOAMS_SYSTEMONE_ALLOW_CLOUD`, `LOAMS_SYSTEMONE_ALLOW_CLOUD_FALLBACK`, `LOAMS_SYSTEMONE_ON_TRUNCATE`, and the matching config keys.

**Semantics:** exactly §6.3, in order. Language: `detect_lang(state, hint) -> {English, Other, Undecided}` using script analysis plus a small, permissively-licensed language detector chosen in this task (candidates `whatlang` MIT, `lingua` Apache-2.0; record the choice and the licence); `Undecided` routes multilingual. `plan` returns `NO_BACKEND` with the list of candidates and why each was excluded.

**Tests** (table-driven; each row is a host profile, request and expected plan): `apple_silicon_defaults_to_coreml`; `nvidia_defaults_to_torch_cuda`; `cpu_only_prefers_native_if_ready_else_torch`; `browser_role_uses_server_route`; `env_backend_beats_auto`; `request_backend_beats_env`; `over_32_options_excludes_coreml`; `over_100_options_excludes_torch_leaves_jev_if_allowed`; `over_255_options_is_413`; `score_over_10_levels_excludes_jev`; `no_backend_error_lists_each_exclusion`; `over_20_options_warns_and_widens_budget`; `over_20_prefers_jev_only_when_allowed`; `omitted_locality_never_selects_cloud`; `non_english_goes_multilingual`; `undecided_latin_goes_multilingual`; `english_only_backend_excluded_for_hindi`; `truncation_warns_by_default`; `on_truncate_widen_retries_once`; `cloud_requires_all_three_consents`; `forced_jev_with_local_only_fails_closed`; `fallback_respects_locality`; `fallback_only_on_retryable`; `degraded_backend_skipped_unless_only`; `gpu_host_with_cpu_device_is_degraded`; `host_detect_does_not_load_models` (no process spawned except the bounded `nvidia-smi`).

**Commit:** `systemone: host detection and routing`.

### Task 5: The `laya-serve` adapter and sidecar supervisor

**Files:** `crates/loams-systemone/src/{sidecar,backends/laya_serve}.rs`, `scripts/systemone/install_laya.sh`, `tests/sidecar.rs` (against a fake `laya-serve` implemented in the test with axum, obeying the documented limits, status codes and `/health` rules).

**Produces:** `LayaServeBackend { base: LoopbackUrl, key: Secret, transport: Tcp | Uds }`, `SidecarSupervisor::{ensure_installed, start, stop, health}`, `loams systemone install laya` logic (a function; the CLI wiring is Task 10).

**Semantics:**
- Install: `uv` pinned, Python 3.12, venv at `$LOAMS_HOME/systemone/venv`, `pip install --require-hashes -r requirements.lock` (generated and reviewed per release, committed under `scripts/systemone/`), CUDA vs CPU torch from `HostProfile`; only with explicit consent; a dry-run prints the plan and sizes.
- Start: environment as design §7.1 (`LAYA_HOST=127.0.0.1`, `LAYA_PORT`, `LAYA_API_KEY`, `LAYA_DEVICE`, `LAYA_PRELOAD=1`, `LAYA_MODELS`, `LAYA_THREADS` = physical cores, `LAYA_MAX_CONCURRENT=16`, `LAYA_REVISION`, `USE_TF=0`, `HF_HUB_OFFLINE=1` once cached); own process group; wait up to 120 s for `/health` to list the checkpoints; backoff 1 s to 30 s, five tries in ten minutes, then `Failed`.
- Requests: map the canonical request to `laya-serve` JSON, forwarding `lang`, `max_len`, `head_max_len` and `min_confidence` (we use our own gate but forward it so `low_confidence` stays consistent), reading `Server-Timing`. Map statuses: 422 to `Invalid`, 413 to `TooLarge`, 503 to `Unavailable{retry_after}`, 401 to `Internal` plus a restart (the key is ours; a 401 means a stale process), 500 to `Internal`. A response with `usage.truncated` becomes the `state_truncated` warning.
- `noul_mode`: native, or `choice_ab` (Task 8 sets it): the adapter sends a two-option choice with keys `A` (yes) and `B` (no) and returns `noul = P(A)`.
- The adapter refuses any non-loopback base URL at construction.

**Tests:** `non_loopback_is_refused`; `key_not_in_argv_or_logs` (spawn a probe child, scan argv, env of logs and tracing output); `child_group_killed_on_drop`; `health_without_key_shows_only_status`; `restart_backoff_then_failed`; `truncation_becomes_warning`; `status_codes_map` (422, 413, 503, 401, 500); `queue_depth_16_then_unavailable`; `widened_budget_forwarded`; `noul_choice_ab_roundtrip`; `device_cpu_on_cuda_host_is_degraded` (fake `/health` says cpu); `installer_dry_run_prints_sizes_and_changes_nothing`; and an `#[ignore]` `real_laya_serve_cpu_smoke` (needs the venv) that runs the Task 8 self-test.

**Commit:** `systemone: supervised laya-serve sidecar and adapter`.

### Task 6: The Jev adapter

**Files:** `crates/loams-systemone/src/backends/jev.rs`, `tests/jev.rs`, `tests/fixtures/jev_*.json`.

**Produces:** `JevBackend { base: Url /* default https://api.typesafe.ai */, key: Secret, model: String /* default "jev-latest" */ }` with a circuit breaker; locality `Cloud`; capabilities from docs.typesafe.ai (all three types native; 255 options per `choice`, 2 to 10 levels per `score`, `noul` criteria optional; 401, 422, 429 and 529 mapped).

**Semantics:** `POST {base}/v1/systemone` with `Authorization: Bearer`, body `{model, state, questions}` only; decode `answers`, `model`, `usage` (unknown fields kept in `extra`, Ruling 1). 429, 529 and 5xx: exponential backoff with jitter, at most 2 retries, honouring `Retry-After`; 401/403 disable the backend with a clear status; the request body is never logged. The base URL must be `https` unless the host is loopback; redirects are not followed with the key attached (a redirect to another host or scheme fails the request). The key comes from the secret store or `LOAMS_JEV_API_KEY`; absent key means `NotInstalled { how: "set a key" }`. Compiled in by default but **inert without a key and the instance switch** (Ruling 6).

**Tests (wiremock):** `sends_only_model_state_questions`; `bearer_header_set_and_key_not_logged`; `decodes_choice_score_noul` (shapes from docs.typesafe.ai: `noul` has no confidence); `unknown_fields_kept`; `retry_after_honoured`; `breaker_opens_after_failures_and_recovers`; `auth_error_disables_backend`; `no_key_is_not_installed`; `locality_is_cloud`; `body_never_in_error`; `non_https_remote_base_refused`; `loopback_http_base_allowed`; `redirect_to_other_host_does_not_forward_key`; `jev_outputs_never_enter_decision_log_labels`.

**Commit:** `systemone: Jev cloud adapter (off by default)`.

### Task 7: Calibration

**Files:** `crates/loams-systemone/src/calibrate.rs`, `tests/calibrate.rs`.

**Produces:**

```rust
pub struct CalibrationTable { pub id: String, pub revision: String, pub entries: Vec<CalEntry> }
pub struct CalEntry { pub checkpoint: String, pub ty: QuestionType, pub bucket: OptionBucket, pub temperature: f64, pub n: u32, pub ece_before: f64, pub ece_after: f64 }
pub enum OptionBucket { B2, B3_4, B5_8, B9_20, B21_32, B33Plus }
pub fn fit(rows: &[LabelledDecision]) -> Result<CalibrationTable, CalError>;       // NLL by 1-D bounded search
pub fn apply(table: &CalibrationTable, ans: &mut Answer, ctx: &CalContext);        // p_i ∝ p_i^(1/T)
pub fn ece(rows: &[(f64 /* confidence */, bool /* correct */)], bins: usize) -> f64;
```

**Semantics:** needs at least 100 labelled decisions per bucket else that bucket is `uncalibrated` (reported, not fitted); tables are written to `$LOAMS_HOME/systemone/calibration/cal-<date>-<hash>.json` and loaded by revision; applying a table for a different revision is refused. For `score`, calibrate the distribution over levels and recompute the expected value from the calibrated probabilities. For `noul`, a two-class temperature. `calibrated_confidence = max(p)` (`max(p, 1-p)` for `noul`). Compare `confidence` and `answer_confidence` as gating signals by AUROC on the labelled set and write the result in the report (Q527).

**Tests:** `fit_reduces_ece_on_synthetic_overconfident_data` (generated with a known temperature of 2.5; recovered within 10%); `buckets_under_100_are_uncalibrated`; `table_for_other_revision_refused`; `apply_preserves_argmax`; `score_expectation_recomputed`; `noul_two_class`; `ece_known_value`; `auroc_known_value`; `uncalibrated_answers_carry_warning`.

**Commit:** `systemone: temperature calibration per type and option count`.

### Task 8: The self-test, gating and the `noul` quirk

**Files:** `crates/loams-systemone/src/selftest.rs`, `selftest_cases.json`, `tests/selftest.rs`.

**Produces:** the 16 cases of design §9.2 (ids `s01`..`s16`), `run(backend) -> SelfTestReport`, `Registry` wiring (run after `start` and on revision change; `Starting` until it passes), the `noul` stuck-label check that flips `noul_mode` to `choice_ab` per checkpoint, and `min_confidence` handling.

**Semantics:** each case declares `requires` (for example `min_options: 33` on the 40-option case), and a backend that cannot serve it reports `not_applicable`, which is neither pass nor fail; the empty-options case is a validation test (Task 2), not a backend case. Structure always (probabilities sum to 1 within 1e-3, `output_tokens == 0`, option sets equal the request's); argmax only for cases carrying `expect: "strict"`, which Task 0's measurements populate (an allowlist, with `informational` for the rest); the 40-option case is informational and reports the widened-budget outcome (Q528); the Hindi and German cases assert the multilingual route; the truncation case asserts the warning. A failure makes the backend `Degraded` with the case id in the reason.

**Tests:** `fake_backend_passes`; `structure_violation_degrades`; `strict_case_failure_degrades_informational_does_not`; `stuck_noul_switches_to_choice_ab_with_warning`; `non_english_cases_route_multilingual`; `truncation_case_warns`; `report_contains_no_state` (case ids only); `selftest_runs_under_two_seconds_on_fake`; `coreml_like_backend_skips_40_option_case_as_not_applicable`.

**Commit:** `systemone: self-test, noul workaround and gating`.

### Task 9: The model manager

**Files:** `crates/loams-systemone/src/models/{mod,lock,download,cache}.rs`, `models.lock.toml`, `tests/models.rs` (a local HTTP server with range support).

**Produces:** `ModelLock` (artefacts: source repo, commit SHA, files with SHA-256 and size, licence, notice, mirror URLs), `ModelManager::{status, plan, pull, verify, remove, list}`, the cache layout of design §9.5. The initial lock covers the Laya checkpoints used by `laya-serve` (English, multilingual, typed-decisions) and, as inactive entries, the Core ML buckets for SO2.

**Semantics:** consent callback before any byte is fetched (the CLI prompts; the desktop shows a dialog); resumable ranges to `*.part`; SHA-256 verified before the atomic rename and before any parser sees the file; read-only after; mirror fallback in order; `LOAMS_OFFLINE=1` never downloads; revisions are immutable; `remove` only on request; the lock is the only source of URLs (no URL from a response is followed).

**Tests:** `pull_resumes_after_interrupt`; `digest_mismatch_deletes_part`; `verify_before_rename`; `offline_never_fetches`; `mirror_used_when_primary_fails`; `consent_denied_downloads_nothing`; `revision_directory_immutable`; `lock_parses_and_urls_are_https`; `disk_full_leaves_no_partial_revision`; `list_shows_sizes_and_licences`.

**Commit:** `systemone: verified, resumable model cache`.

### Task 10: The server route, Connect service, CLI and MCP tool

**Files:** `crates/loams/src/systemone/{mod,service,http,config,cli}.rs`, `crates/loams/src/{server.rs,main.rs}`, `crates/loams/tests/systemone/{main.rs,http.rs,connect.rs,cli.rs}`.

**Produces:** the Connect `SystemOneService` and the axum route `POST /v1/systemone` (+ `POST /v1/systemone/batch`) on the gateway role, both over one `Registry`; `GET /v1/systemone/backends`; CLI `loams systemone {status, decide, selftest, install laya, calibrate, export, model list|pull|rm|add}` with `--output json`; and a read-only `systemone_decide` tool in the stdio MCP server (CLI1). Auth uses the normal gateway auth; the loopback desktop/dev listener allows a local key. Usage events: `io.loams.dev.systemone.decided.v1` (counts, backend, latency, locality; never content). Per-principal rate limit and per-org quota as configured (Q538, Q539: off by default).

**Semantics:** HTTP errors per §4.2; no internals in 500; `Server-Timing: inference;dur=<ms>` set. Enablement: in `loams dev` the route is on by default; on a server it is off unless started with `--systemone`, and stays off by default until Q539 is answered. `loams systemone status` prints the `HostProfile`, each backend's status with the one-line fix, the active route reason, and the calibration table id.

**Tests:** `http_decide_matches_laya_fixture_shape`; `jev_client_compat` (the TypeSafe request shape is accepted); `connect_decide_equals_http_decide`; `auth_required_on_gateway`; `no_state_in_spans_or_errors` (a tracing subscriber captures everything during a failing request); `batch_endpoint_ordering`; `cli_status_json_schema`; `cli_decide_stdin`; `mcp_tool_is_read_only_and_local_only` (refuses `ALLOW_CLOUD`); `usage_event_has_no_content`; `route_disabled_by_default_on_server`.

**Commit:** `systemone: serve the decision API, CLI verbs and MCP tool`.

### Task 11: The decision log and export (fine-tuning hooks)

**Files:** `crates/loams-systemone/src/declog.rs`, `crates/loams/src/systemone/cli.rs`, `tests/declog.rs`.

**Produces:** `DecisionLog` (JSONL by month in `$LOAMS_HOME/systemone/decisions/`), `loams systemone export --format laya --out dir` (typed-decisions JSONL plus a held-out split), `loams systemone label <decision-id> --choice …`, and `model add` (Task 9's lock accepts a custom entry with a digest).

**Semantics:** off by default (`systemone.log_decisions = off | local`); state stored only when `log_state = true`, else a salted hash; retention 90 days enforced on open; file mode 0600; labels are append-only rows; a label whose source is a Jev answer is refused (TypeSafe's agreement forbids using Output to train a model). Export refuses decisions without a stored state.

**Tests:** `off_by_default_writes_nothing`; `hash_only_without_log_state`; `retention_prunes`; `file_is_0600`; `label_rows_join_by_id`; `export_requires_state`; `export_format_matches_fixture` (a sample of upstream's typed-decisions format, copied with attribution); `custom_checkpoint_needs_digest`.

**Commit:** `systemone: opt-in decision log, export and custom checkpoints`.

### Task 12: The in-process Rust backend (only if Task 0 chose kevala or an approved `ort` port)

**Files:** `crates/loams-systemone/Cargo.toml` (feature `native`, off by default), `src/backends/native.rs`, `tests/native.rs`, `THIRD_PARTY_NOTICES.md`.

**Semantics:** wrap kevala's `Model` (pinned git commit) as `LayaNativeBackend`; models come from Task 9's cache; inference runs on a dedicated blocking pool with a bounded queue; capabilities report the checkpoints actually present; the self-test is the gate; the feature stays off by default until the owner decides (as with `pgwire`).

**Tests:** `native_matches_laya_serve_within_0_02` (`#[ignore]`, needs both); `native_passes_selftest`; `native_refuses_untrusted_torch_pickles`; `native_bounded_queue`; `cargo deny check` with the feature.

**Commit:** `systemone: in-process Laya backend behind a feature`.

### Task 13: Documentation and the exit gate

**Files:** `docs/design/40-loams-systemone.md` (as built), `docs/design/13-decision-log.md` (paste the pending log), `docs/plans/README.md`, `CHANGELOG.md`, `THIRD_PARTY_NOTICES.md`, a user page `docs/systemone.md`.

**Exit gate (all in CI or on the labelled runner):** Tasks 1 to 11 tests; `loams dev` serves `POST /v1/systemone` with the fake backend; on a runner with the venv, the real `laya-serve` CPU smoke passes the self-test; the Jev adapter passes against wiremock; `cargo deny check`; the licence table of design §2.1 matches `THIRD_PARTY_NOTICES.md`; a chaos run (kill the sidecar mid-request, corrupt a downloaded file, forbid cloud) passes.

**Commit:** `docs: document SystemOne and close SO1`.

## Rulings made during execution

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| — | (Task 0 fills this table) | | |
