# OPS — Dependabot DCO validation

Status: Implemented; CI and review pending.

## Global constraints and Task 0

Every source commit must carry its author's DCO sign-off. No force-push,
rewritten bot authorship, unsigned-commit exemptions or skipped bot checks.
PR #309 exposed the existing check's email mismatch after a signed-off update
from dev: GitHub's Dependabot author uses its noreply identity, while its
standard sign-off uses support@github.com. This is documented in upstream
https://github.com/dependabot/dependabot-core/issues/3480.

## Tasks

- [x] Add fixtures: signed and unsigned human, standard signed bot, unsigned bot, human with
  bot sign-off and bot name with wrong author email. Demonstrate signed bot fails.
- [x] Recognize only the exact bot name/email and full standard sign-off;
  preserve human validation and run the check for bot-triggered PRs too.
- [ ] Review full diff, obtain green CI/DCO and CodeRabbit, then merge.

## Rulings made during execution

| # | Ruling | Reason |
|---|---|---|
| 1 | Accept Dependabot's exact support-address DCO only for its exact GitHub author identity; remove actor-level job skipping. | Standard GitHub bot commits are already signed off. Checking all actors strengthens enforcement without requiring rewritten shared history. |

## Verification

The signed-bot fixture failed with the original email-only check. All six
fixtures pass with the fix; unsigned and mismatched identities fail. The check
also passes the actual #309 base/head range that failed CI. Bash syntax and
workflow YAML validation pass. No dependency or crate changes.
