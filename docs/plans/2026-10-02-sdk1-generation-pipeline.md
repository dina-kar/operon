# SDK1 — The SDK Generation Pipeline: buf, the Facade Generator, Runtime Contract and Conformance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, headers), use them verbatim. The code is not pre-written in this plan; the tests are the specification.

> **Status: Planned** (2026-10-02). **Slot: track SDK, second plan** (proposed; D604–D611, D615–D619). Branches `sdk1-t<N>`, stacked; PRs target `dev`. Depends on API1 Tasks 1–2 (options proto and the first services); can start on AP0's protos.

**Goal:** Everything the per-language SDKs (SDK2) share:
- the layout `sdks/<lang>/` with a pinned `buf.gen.yaml` per language (remote plugins) and drift checks;
- the facade generator `protoc-gen-loams-facade` and its template contract;
- the **runtime contract** (§44 §7.4) written as a language-neutral spec with executable fixtures: auth, consistency tokens, retries, idempotency keys, errors, pagination, streaming resume;
- the shared fixture corpus, the conformance runner and the test-server launcher;
- the release workflows (tag `sdk-<lang>-v<semver>`, trusted publishing, mirrors), tied to issue #254;
- the generated reference docs.

**Architecture:**
- `proto/` is the single source. `buf.yaml` stays the module; `loams.internal.v1` is excluded by a second module.
- `crates/loams-facade-gen`: a Rust `protoc` plugin (reads a `CodeGeneratorRequest`, the options of `loams.options.v1`) writing one facade per language from `sdks/templates/<lang>/`. It is run by buf as a `local` plugin.
- `docs/sdk/runtime-contract.md`: the normative behaviours with numbered clauses (R1 auth refresh, R2 retry classes, ...). Each clause maps to fixtures; a language's tests cite clause numbers.
- `sdks/fixtures/` (JSON + proto cases) and `sdks/conformance/run.sh` (starts `loams dev --listen 127.0.0.1:0` with fixtures, optionally `loams-apps-mock` for fault injection, exports `LOAMS_TEST_ENDPOINT`).
- Release: `.github/workflows/sdk-release.yml` (matrix by language), `sdk-mirror.yml`, dry-run on PRs.

**Tech Stack:** `buf` (pinned), remote plugins pinned per `buf.gen.yaml`, Rust for the generator (`prost-types`/`protobuf` descriptor decoding as the repo already uses), GitHub Actions with OIDC (`id-token: write`).

**Spec:** [§44](../design/44-unified-api-and-sdks.md) §7, §9–§11; issue #254; [CLI2](2026-10-01-cli2-release-and-install.md); [AP0](2026-10-01-ap0-app-protos.md) Task on generation; D128.

## Global Constraints

- **Remote plugins are pinned** to a version; a bump is its own PR with regenerated output.
- **Generated code is never hand-edited.** CI regenerates and fails on a diff where output is committed (TS, Go, PHP, Swift mirrors).
- **A language is not published until it passes 100% of the required fixtures** (D617).
- **No long-lived registry secret where trusted publishing exists** (D615); Maven Central keeps a token and a GPG key.
- Names: `loams`; no Operon/Loam strings in generated packages.

## Tasks (one PR each)

- [ ] **Task 0: Verify the toolchain (dated).** For each of the 13 languages: confirm the remote plugin names and versions on buf.build, the Connect library's latest release and maintenance state, and the registry's trusted-publishing support. Write `docs/sdk/toolchain-2026-10.md` (a table with dates and links). Output decides the Connect/gRPC column of §44 §9 (D612's 12-month rule). *Tests:* `toolchain_table_has_row_per_language`.
- [ ] **Task 1: Layout and `buf` templates.** `sdks/<lang>/buf.gen.yaml` for all 13; fold `buf.gen.yaml`, `.apps`, `.kotlin`, `.swift` into it; `scripts/sdk/gen.sh <lang>`; CI job `sdk-gen-drift`. Resolves Q605 and Q609 in a ruling row. *Tests:* `gen_all_languages_clean` (CI), `drift_check_fails_on_manual_edit`.
- [ ] **Task 2: The runtime contract.** `docs/sdk/runtime-contract.md` with clauses R1 (TokenSource and refresh), R2 (retry classes and backoff numbers), R3 (idempotency-key lifecycle), R4 (consistency token merge and attachment), R5 (error mapping and `reason` registry), R6 (pagination), R7 (streaming: heartbeat, cursor, reconnect), R8 (timeouts and cancellation), R9 (version check via `GetInstance`). *Tests:* `every_clause_has_a_fixture`.
- [ ] **Task 3: The facade generator.** `loams-facade-gen` with templates for **TypeScript, Python and Go first** (the spike that decides Q604) and the template interface for the rest. Golden-file tests. *Tests:* `generates_module_per_service_option`, `vector_and_search_share_one_rpc`, `retry_class_follows_idempotency_level`, `pagination_iterator_for_list_rpcs`, `golden_typescript`, `golden_python`, `golden_go`. If the spike fails, record Ruling "hand-written facades" and keep the generator for the module index only.
- [ ] **Task 4: Fixture corpus and conformance runner.** `sdks/fixtures/` (cases for auth, search, writes with idempotency, pagination, errors with reasons, streaming resume, consistency); a Rust test running every fixture against the real server; `sdks/conformance/run.sh`; mock fault-injection endpoints (retryable `UNAVAILABLE`, `RetryInfo`, mid-stream disconnect, `token_expired`). *Tests:* `fixtures_pass_against_real_server`, `mock_injects_retryable_errors`.
- [ ] **Task 5: Bulk data helpers contract.** Chunking rule (10 000 rows or 4 MiB), Flight-backed `bulk` module contract for Python, Go, Java, C++, Rust, C#, and the `QueryArrow` fallback (Q608). Fixtures `bulk_*`. *Tests:* `chunking_whole_or_nothing`.
- [ ] **Task 6: Release workflows and mirrors.** `sdk-release.yml` (matrix; per-registry jobs with OIDC; dry-run on PR using `--dry-run` equivalents), `sdk-mirror.yml` (subtree split for `loams-go`, `loams-swift`, `loams-php`), tag rules, changelog per SDK. Coordinate with #254; the owner-account checklist in `docs/sdk/publishing.md`. *Tests:* `workflow_lint` (actionlint), `dry_run_all_languages`.
- [ ] **Task 7: Reference docs.** `buf generate` doc plugin to `docs/api/reference/`; snippet extraction from fixtures into `docs/api/snippets/`; the page list for `loam-cloud` (a follow-up issue there). *Tests:* `every_rpc_documented`, `snippets_match_fixtures`.
- [ ] **Task 8: Versioning gates.** `buf breaking` in CI for SDK-relevant packages, SDK `LOAMS_PROTO_REV` file, the `GetInstance` version check clause (R9) fixture, CONTRIBUTING section "adding a community SDK". *Tests:* `breaking_change_fails_ci` (on a fixture proto).
