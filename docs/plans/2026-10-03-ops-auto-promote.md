# OPS — Auto-promote contributors to committers after their first merged PR (#231)

**Status:** Implemented; the credential is an owner action, and until it exists the workflow posts the
by-hand hand-off comment instead of promoting anyone.

## Global constraints

- One task per PR against `dev`, signed off. No new runtime dependency: the decision logic is Python
  standard library, in the shape of `no-metering.py`.
- **No repository protection changes.** Team membership is not governed by a branch ruleset, so
  nothing in `rulesets` is touched by this task (see Task 0).
- **The narrowest possible grant.** The workflow adds exactly one account — the author of the merged
  PR — to exactly one team, `committers`. It never touches `maintainers`, never changes a repository
  permission, a branch protection or a ruleset, and never runs for a bot.
- **Reversible.** Every promotion leaves a comment on the merged PR naming the change and how to undo
  it, and removing the account from `committers` restores the previous state exactly.
- The credential is the owner's to create. The workflow is designed to be useful before it exists:
  with no secret it posts the comment that asks a maintainer to do it by hand.

## Task 0: reconciliation

Issue #231 specifies the mechanism in full: a workflow on `pull_request` closed and merged into `dev`
that invites the author to the organisation, adds them to `committers`, comments a welcome, skips
bots and authors already in the team, is idempotent, and degrades to a by-hand request when the
credential is missing. No design or plan document predates it, and `docs/design/13-decision-log.md`
records no decision for it.

State inspected on 2026-10-03, before this change:

- `GOVERNANCE.md` already says a first merged PR makes a contributor eligible for `committers` and
  that "a team maintainer adds them". That sentence is what this task automates; it is updated here to
  say the promotion is automatic and still reversible.
- Five active rulesets exist. `dev: team updates only` (24352963) and `main: team updates only`
  (24352956) are **branch**-target rulesets holding a single `update` rule: they restrict who may push
  to a branch, and despite the names they say nothing about team membership. `dev: PR, CI and branch
  safety` (24352959), `main: PR, CI and branch safety` (24352955) and `main: require DCO sign-off`
  (24272886) govern merges. **None of them is affected by adding a user to a team**, so this task
  changes no ruleset and carries no lock-out risk. A full copy of all five, taken before any work, is
  held at `Operon/.claude/auto-promote-ruleset-backup.json`.
- The one real permission consequence is inherited from #230's governance model rather than added
  here: `committers` is a bypass actor on the `dev` `update` rule, so a committer may push to `dev`.
  `committers` is **not** a bypass actor on `main`, and `main` additionally requires a maintainers'
  review, `CI required` and `DCO sign-off`, so a promoted committer cannot merge or push to `main`.
  The promotion therefore grants exactly what `GOVERNANCE.md` already promises the role, and no more.
- The `committers` team's own repository permission is unchanged by this task. Promotion adds a team
  member; it does not alter team settings, collaborator permissions or ruleset bypass lists.

## Tasks

- [x] **Task 1 — the decision logic.** `scripts/ci/auto-promote.py`: skip bots; skip an author already
  an active `committers` member before any write; read organisation membership; invite when absent;
  add to `committers`; comment the welcome with the reversal instructions. Every step is a PUT, so a
  re-run converges rather than duplicating.
- [x] **Task 2 — the credential.** Prefer a GitHub App (`ORG_APP_ID` plus `ORG_APP_PRIVATE_KEY`,
  minted with `actions/create-github-app-token`); fall back to the fine-grained `ORG_MEMBERS_TOKEN`.
  With neither, post the by-hand comment naming the missing secret. A credential that can read but
  not write, or that cannot read team membership, is reported as a failure or a hand-off — never as a
  silent success.
- [x] **Task 3 — the tests.** `scripts/ci/test-auto-promote.py` runs the real script over HTTP against
  a mock GitHub API: new contributor, existing organisation member, second merged PR, bot author, no
  credential, dry run, refused write, unreadable team membership, and the split between the
  organisation credential and the commenting credential.
- [x] **Task 4 — the workflow.** `.github/workflows/auto-promote.yml`, gated on `merged` and base
  `dev`, with a `workflow_dispatch` dry-run mode for testing against a real account.
- [x] **Task 5 — the docs.** This plan, and the `GOVERNANCE.md` sentence it automates.

## Rulings made during execution

| # | Ruling | Reason |
|---|---|---|
| 1 | Trigger on `pull_request_target`, not `pull_request`. | A `pull_request` run from a fork is given a read-only `GITHUB_TOKEN` and no repository secrets at all. That breaks both halves of the issue for exactly the population it targets — external first-time contributors, who are by definition not organisation members and so almost always on forks. `pull_request_target` runs in the base repository with a write token and the org secrets available. It is safe here only because the workflow checks out the **base** branch to read the script, never the PR head, and executes nothing from the PR. |
| 2 | Two credentials, not one. | Organisation membership needs Organisation → Members; the PR comment needs repository `pull-requests: write`. The App is given the membership scope only, and the comment is posted as the job's own `GITHUB_TOKEN`. A single credential would have meant asking the App for repository rights it should not hold. |
| 3 | No ruleset change of any kind. | The issue asks for automation of a team addition, which no branch ruleset governs. Editing `dev: team updates only` would have widened push access on `dev` to every member of a new team, and touching anything on `main` risked the maintainer's own access. Recorded as no-op with the full pre-change state kept as a restore point. |
| 4 | A second merged PR from an existing committer is a no-op: one read, no write, no comment. | The issue's "skip authors already in the team". The check runs first so a busy contributor's later PRs generate no noise on the repository. |
| 5 | Failure to read team membership is a failure, not a "not a member" verdict. | A credential missing the `Members: read` scope returns 403, and treating that as "not in the team" would invite every committer to the organisation and re-add them on each merge. It exits non-zero instead. |
| 6 | Reversal is documented in the comment, not automated. | GitHub offers no undo for an organisation invitation, and an unattended "undo" job would be able to remove a maintainer. The comment names the team and says a maintainer can remove the account from it; that is the whole of the reversal. |
| 7 | The credential is not created by this task, and the workflow is not gated on it. | Creating a GitHub App and a private key is an owner action the issue names explicitly. The workflow ships first and does the useful half of the job — telling the contributor and the maintainer what happened — from the moment it merges. |
| 8 | The test is not wired into `ci.yml` in this PR. | `.github/workflows/ci.yml` is being edited concurrently for #232; adding a job here would conflict. The test runs standalone with `python3 scripts/ci/test-auto-promote.py` and should be added to the `check` job once #232 lands. |
| 9 | `actions/checkout` pinned to a SHA, and the App's private key scoped to the token-minting step rather than the job. | This workflow runs on `pull_request_target`, so the `GITHUB_TOKEN` has write access and the job can see the org secret. Pinning removes the moved-tag vector, and scoping the key to the one step that needs it keeps it out of every other step's environment. A configured `ORG_APP_ID` with a missing key fails loudly rather than falling back silently. |

## Owner action

Until this exists, every promotion is a comment asking a maintainer to do it by hand:

1. Create a GitHub App in `ostrium-labs` with **Organisation → Members: Read & write**, installed on
   `ostrium-labs` only.
2. Store `ORG_APP_PRIVATE_KEY` as the organisation secret of that name, and `ORG_APP_ID` as an
   organisation variable (or a secret).
3. Or skip the App and store a fine-grained personal access token with organisation member
   read/write as the organisation secret `ORG_MEMBERS_TOKEN`.

A GitHub App is preferred: a fine-grained token belongs to a person, and the promotion runs unattended.
