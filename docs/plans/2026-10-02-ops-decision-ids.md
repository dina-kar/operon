# OPS — Unique decision declarations (#304)

Status: Implemented; CI and review pending.

## Global constraints

Keep every owner's decision, question, action and reference. Change no design
ruling or ID merely to make a checker pass. Use no dependencies or Rust build.
The central log owns canonical declarations; design sections may repeat their
own scoped decisions. A reference to an ID is not another declaration.

## Task 0: Reconciliation

Compare every duplicate Q271, Q282, Q420, Q421 and Q434. In each case the
later owner-action summary restates the canonical open question, with shorter
wording and a Needed by column. There is no conflicting decision or separate
question to renumber. All canonical details and references remain under their
existing IDs; preserve the later action text as references in a list.

## Tasks

- [x] Add duplicate D/Q and unique/reference fixtures; demonstrate the checker
  rejects both the planted fixture and the original log's five duplicates.
- [x] Keep canonical question rows and convert the repeated owner-action
  summaries to references without losing their text or changing IDs.
- [x] Run the checker and its self-test in a required docs CI job.
- [ ] Obtain green CI/DCO and CodeRabbit review, merge and close #304.

## Rulings made during execution

| # | Ruling | Reason |
|---|---|---|
| 1 | The five later rows are abbreviated action references to the same open questions, not new decisions; preserve them as a list. | Their action and deadline match the canonical rows. Renumbering would create spurious independent questions and break the existing design references. |
| 3 | Recognize rows with or without the optional leading Markdown pipe; fixtures mix both forms. | CodeRabbit identified that valid GFM rows without a leading pipe otherwise escaped the checker. |
| 2 | Check bare D/Q identifiers in the first Markdown table cell of the central log, not every textual reference or every design file. | References and scoped design excerpts legitimately reuse canonical IDs; duplicate declarations do not. |
| 4 | Check numeric and scoped hyphenated declarations, stripping only their parenthetical status annotation. Disable persisted checkout credentials in the docs job. | CodeRabbit identified scoped IDs omitted by the first matcher and unnecessary credentials in a read-only validator. |

## Verification

The planted D1/Q2 fixture is rejected with both original line numbers. A
unique fixture containing D1/D10, Q2/Q20 and repeated textual references passes.
Before the docs change the check rejected all five IDs listed in #304. After
it the log and self-test pass. Workflow YAML parses and git diff --check passes.
