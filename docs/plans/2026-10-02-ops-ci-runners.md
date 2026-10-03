# OPS CI runners (#232)

**Status:** Task 1 landed on `dev` (runner routing, nextest partitioning, promotion-only heavy suites, concurrency, and the before/after numbers below). Tasks 2 and 3 remain.

## Global constraints

- One repository task per PR. The Loams PR targets `dev`; companion repositories need their own PRs.
- Preserve path filters and all existing test coverage. Use signed commits and wait for CI and CodeRabbit before `needs-opus-review`.
- Runner labels come from organization variables. Use safe defaults so a missing variable cannot strand CI jobs.

## Task 0: reconciliation

Issue #232 has no linked implementation plan or named tests. At the start of this task, Loams had 16 jobs in `ci.yml` plus DCO; `changes` filtered PR jobs by path, but all jobs used `ubuntu-latest`. The crash, cluster and TiKV suites still ran on matching dev PRs. The owner added workflow concurrency in the issue comment on 2026-10-02. The cited design decision log contains no CI runner decision for this issue.

## Tasks

- [x] **Task 1 — Loams CI:** route main to Blacksmith and dev Rust jobs to Depot; retain light jobs on GitHub-hosted runners; partition workspace tests with nextest; run crash, cluster and TiKV suites on promotion and nightly; add concurrency to PR workflows; measure before and after.
- [ ] **Task 2 — loams-mobile:** apply runner policy to its Linux jobs, keeping macOS and iOS on GitHub-hosted macOS runners.
- [ ] **Task 3 — loams-desktop:** apply runner policy to its Linux jobs, keeping macOS and iOS on GitHub-hosted macOS runners.

## Rulings made during execution

| # | Ruling | Reason |
|---|---|---|
| 1 | Include `github.workflow` in the issue's concurrency group. | CI and DCO run for the same PR; identical groups would cancel each other. Each workflow should cancel only its own superseded run. |
| 2 | Use the issue's named labels as fallbacks for `RUNNER_MAIN`, `RUNNER_DEV_HEAVY` and `RUNNER_LIGHT`. | The organization variables cannot be read with the current token, and an unset variable must not leave a job queued with an empty runner label. |
| 3 | Partition only the workspace test pass with nextest; retain Cargo doctests and feature-specific test passes. | nextest does not run doctests, while existing feature passes exercise distinct configurations. |
| 4 | Keep the existing Rust cache action on both providers and defer sccache until a measured gain exists. | Depot routes GitHub cache API actions to Depot Cache automatically; the issue asks for sccache only where measured to help. |
| 5 | Pin the Rust cache action and nextest installer to verified upstream commits. | CodeRabbit found the new mutable action references; the v2.9.2 rust-cache tag resolves to `6323deb102c322ba6fcbdcafc7e3dddab59af2b6` and the nextest installer tag to `badb8c3638e0b773c6c17a6025c2306a8f1c4303` on 2026-10-02. |
| 6 | Route the five jobs that still pinned `ubuntu-latest` (`docs`, `tla`, `lean`, `no-metering`, `required`) through the same light expression as `changes`. | #268 landed the routing for 19 of the 24 jobs. The remaining five ignored `RUNNER_MAIN`, so changing that one organization variable would have moved 19 jobs off the default pool and left these five behind — and on `main` they would have stayed on GitHub-hosted runners, which contradicts the issue's main-branch ruling. On `dev` the expression resolves to `RUNNER_LIGHT`, so this is behaviour-neutral there. |
| 7 | Keep the workspace test pass at 2 nextest partitions. | The issue allows 2–4. Measured on dev PR [37052445546](https://github.com/ostrium-labs/loams/actions/runs/37052445546) the two partitions took 12.8 and 11.1 minutes, so they are already balanced. Four partitions would move the critical path from `Rust tests (1/2)` to `Rust feature tests` (11.8 minutes) — about 1 minute — at the cost of two more full workspace compiles on every Rust PR. Raising it needs a measurement that justifies the extra runner minutes, not a guess. |
| 8 | Leave the heavy suites running on `dev` pushes, not only on promotion and nightly. | The issue's complaint in Task 0 reconciliation is about dev *pull requests*, which are now skipped. A `dev` push is the merge of many reviewed PRs; dropping the crash, cluster and TiKV gates there would let a broken merge sit on `dev` with no gate until the promotion to `main`, which conflicts with the global constraint to preserve all existing test coverage. |

## Measurement

Use the GitHub Actions run and job timestamps for a comparable full CI run before and after this change. Report both queue delay and elapsed wall time in the PR; a cached light or docs-only run is not a comparable baseline.

Baseline: [main run 36966002471](https://github.com/ostrium-labs/loams/actions/runs/36966002471), created 2026-10-02 04:46:16 UTC, finished 06:17:51 UTC, **91.6 minutes wall time**. The `fmt, clippy, test` job started 0.1 minutes after run creation and ran 91.5 minutes; it was the critical path. All 12 non-skipped jobs passed. This is a main push baseline; compare main pushes separately from dev PRs because their test sets differ.

### Before and after, dev pull requests

`dev` is the branch that received the routing (commit 15e7d59, merged as #268 at 2026-10-02 08:24 UTC), so the comparable pair is dev pull requests either side of that commit. Figures are `updated_at - created_at` on successful `ci.yml` runs longer than 5 minutes, read from the Actions API; a docs-only or cached run is not counted.

| | runs | median wall | min | max |
|---|---|---|---|---|
| Before (`ubuntu-latest`) | 189 | **64.7 min** | 5.2 | 858.4 |
| After (Depot heavy, GitHub-hosted light) | 25 | **14.7 min** | 9.9 | 136.6 |

Queue delay, taken as `run_started_at - created_at`, was the other half of the wait on GitHub-hosted runners and is now gone:

| run | branch | queue delay | wall |
|---|---|---|---|
| [37010902971](https://github.com/ostrium-labs/loams/actions/runs/37010902971) | `ap1a-cordis-console` (before) | 67.1 min | 105.0 min |
| [37052445546](https://github.com/ostrium-labs/loams/actions/runs/37052445546) | `rand_core` bump (after) | 0.0 min | 13.6 min |
| [37121487118](https://github.com/ostrium-labs/loams/actions/runs/37121487118) | `ops/no-metering-258` (after) | 0.0 min | 10.5 min |
| [37119070102](https://github.com/ostrium-labs/loams/actions/runs/37119070102) | `api1-t0` (after) | 0.0 min | 10.3 min |

The critical path of a typical dev PR after the change is [37052445546](https://github.com/ostrium-labs/loams/actions/runs/37052445546): `Rust tests (1/2)` 12.8 min, `Rust feature tests` 11.8, `durable execution` 11.2, `Rust tests (2/2)` 11.1, `fmt and clippy` 8.9. `kill -9 crash gate`, `multi-process cluster` and `TiKV suites` are all skipped on that PR.

### Main pushes are not measured after the change

`main` has not received the routing — `15e7d59` is an ancestor of `dev` only, and every main run since the baseline still resolves to `ubuntu-latest`. The most recent comparable main push, [37020276624](https://github.com/ostrium-labs/loams/actions/runs/37020276624), took **94.5 minutes** wall with `fmt, clippy, test` at 93.2 minutes, i.e. no change against the 91.6-minute baseline. The main-branch numbers stay at their pre-change values until `dev` is promoted; re-measure then and record the result here. The 3% gap between the two main runs is within the run-to-run noise of an unpartitioned GitHub-hosted pass and is not attributable to any change on `dev`.
