# OPS — Integrate the pending decision logs (#235)

Status: Fold implemented and locally verified; CI/review pending.

## Global constraints

One documentation task per PR against `dev`, signed off. Preserve every existing
decision and every pending decision/question, including answered defaults and
owner actions. Do not invent approvals, renumber established IDs, copy external
code or change implementation plans' tests. Keep commercial metering private.
Source: §37 amendments, §39–§44 and their pending logs; the central decision log,
design/plans indexes and §12 are the integration targets.

## Task 0: reconciliation

The named worktree has no partial commits or changes. Seven pending files remain:
37b, 39, 40, 41, 42, 43 and 44, with 285 decision/question rows. Existing central
D460–D466 are unrelated approved ES rulings. Four pending questions already have
short owner-action references in the central log (Q479, Q491, Q494, Q499).
The indexes have no §39, §40, §42–§44 or native-desktop/web-bridge/SF/SO/API/SDK
plan rows. §43's staged §41 tailnet cross-reference is still missing.

## Task 1: fold and reconcile

- [x] Fold all source rows losslessly into the central decisions/questions tables.
- [x] Disambiguate the still-pending Software Factory decision IDs and update
  their references; leave established ES decisions intact.
- [x] Update both indexes and §12, native/Tauri supersessions and §39's stale
  D440-series pointer; apply §43's staged cross-reference to §41.
- [x] Replace all staging pointers with canonical decision-log pointers and
  remove the integrated pending files only after preservation verification.
- [x] Reconcile verified merge status for #301, #307 and #311; keep the live
  Cloudflare deployment blocked on the owner (#314).
- [ ] Verify source-row preservation, unique IDs, no staging references, local
  links, provenance, green CI/DCO and CodeRabbit; review the full diff and merge.

## Rulings made during execution

| # | Ruling | Reason |
|---|---|---|
| 1 | Fold every remaining pending file, including later §40–§44 blocks, in this integration task. | Removing `_pending` after only the originally named blocks would discard newer work. |
| 2 | Move the still-pending §39 Software Factory decisions D460–D479 to D-SF-1–D-SF-20; update SF-specific references. Preserve established numeric ES D460–D466 verbatim. | These are different decisions with colliding IDs. Scoped IDs already pass the repository's uniqueness guard, and cannot consume another track's reserved numeric range. |
| 3 | Fold full pending Q479/Q491/Q494/Q499 into the question table, retaining the old brief owner-action entries as explicitly labelled references. | Preserve both details without duplicate declarations. |
| 4 | Preserve source proposal/owner-ruling/default statuses. Newer supersessions are annotations, never erased historical decisions. | Integration must not silently approve proposals or overturn owner rulings. |
| 5 | Reconcile completed merge-plan checkboxes only after checking GitHub PR state/merge OIDs for #301, #307 and #311. Cloudflare live deployment stays open under #314. | Status must describe verified results, not stale pending checks or an unperformed deployment. |

## Local verification

All 285 pending rows (152 decisions, 133 questions) match the integrated rows
byte for byte after the documented SF ID/reference mapping. All seven existing
ES decisions D460–D466 remain byte-identical. Unique-ID validation and its
planted-duplicate self-test pass. No link points at a removed pending file; all
local Markdown links in changed documents resolve. Provenance and diff checks
pass. No implementation or dependency changed, so Rust builds are not required.
