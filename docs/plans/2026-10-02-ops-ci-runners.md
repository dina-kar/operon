# OPS CI runners (#232)

**Status:** In progress — Loams workflow changes implemented; PR CI and the companion repositories remain.

## Global constraints

- One repository task per PR. The Loams PR targets `dev`; companion repositories need their own PRs.
- Preserve path filters and all existing test coverage. Use signed commits and wait for CI and CodeRabbit before `needs-opus-review`.
- Runner labels come from organization variables. Use safe defaults so a missing variable cannot strand CI jobs.

## Task 0: reconciliation

Issue #232 has no linked implementation plan or named tests. At the start of this task, Loams had 16 jobs in `ci.yml` plus DCO; `changes` filtered PR jobs by path, but all jobs used `ubuntu-latest`. The crash, cluster and TiKV suites still ran on matching dev PRs. The owner added workflow concurrency in the issue comment on 2026-10-02. The cited design decision log contains no CI runner decision for this issue.

## Tasks

- [ ] **Task 1 — Loams CI:** route main to Blacksmith and dev Rust jobs to Depot; retain light jobs on GitHub-hosted runners; partition workspace tests with nextest; run crash, cluster and TiKV suites on promotion and nightly; add concurrency to PR workflows; measure before and after.
- [ ] **Task 2 — loams-mobile:** apply runner policy to its Linux jobs, keeping macOS and iOS on GitHub-hosted macOS runners.
- [ ] **Task 3 — loams-desktop:** apply runner policy to its Linux jobs, keeping macOS and iOS on GitHub-hosted macOS runners.

## Rulings made during execution

| # | Ruling | Reason |
|---|---|---|
| 1 | Include `github.workflow` in the issue's concurrency group. | CI and DCO run for the same PR; identical groups would cancel each other. Each workflow should cancel only its own superseded run. |
| 2 | Use the issue's named labels as fallbacks for `RUNNER_MAIN`, `RUNNER_DEV_HEAVY` and `RUNNER_LIGHT`. | The organization variables cannot be read with the current token, and an unset variable must not leave a job queued with an empty runner label. |
| 3 | Partition only the workspace test pass with nextest; retain Cargo doctests and feature-specific test passes. | nextest does not run doctests, while existing feature passes exercise distinct configurations. |
| 4 | Keep the existing Rust cache action on both providers and defer sccache until a measured gain exists. | Depot routes GitHub cache API actions to Depot Cache automatically; the issue asks for sccache only where measured to help. |

## Measurement

Use the GitHub Actions run and job timestamps for a comparable full CI run before and after this change. Report both queue delay and elapsed wall time in the PR; a cached light or docs-only run is not a comparable baseline.

Baseline: [main run 36966002471](https://github.com/ostrium-labs/loams/actions/runs/36966002471), created 2026-10-02 04:46:16 UTC, finished 06:17:51 UTC, **91.6 minutes wall time**. The `fmt, clippy, test` job started 0.1 minutes after run creation and ran 91.5 minutes; it was the critical path. All 12 non-skipped jobs passed. This is a main push baseline; compare main pushes separately from dev PRs because their test sets differ.
