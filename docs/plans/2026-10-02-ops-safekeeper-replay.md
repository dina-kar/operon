# OPS — Safekeeper journal replay correction

> **Status: Complete; merged in #308, CI run 37039007140 green** (2026-10-02).
> Issues: #303 and #278. Design: [§28 §7.2](../design/28-loams-postgres.md), D265–D267.

## Global constraints

Keep the existing journal format, CRC validation, durability acknowledgements,
and torn-tail behavior. Never skip or weaken existing tests. No dependencies
or borrowed source. Use a signed-off commit and a PR to `dev`; merge only after
CI, DCO, and review pass. Serialize Cargo commands with the shared build lock;
use only `loams-safekeeper` for compilation and stop below 15 GB free disk.

## Task 0: Reconciliation

The journal already waits for each acknowledged flush unit to become durable.
Recovery decodes records from 4 KiB-aligned units. A record may leave exactly
eight zero padding bytes; reading the kind byte at offset eight then reads the
next unit. Force that boundary without relying on scheduling or filesystem
speed, verify the failure before changing the parser, and check both buffered
and direct-write tiers. The fix must also preserve a valid record that crosses
a block boundary, including a zero-length record whose CRC is zero.

## Task 1: Fix and validate replay (one PR)

- [x] Add and demonstrate failing `eight_byte_padding_does_not_hide_the_next_durable_unit` and `eight_padding_bytes_before_an_append_skip_to_the_next_block`.
- [x] Correct padding recognition while preserving CRC-validated records and torn-tail handling.
- [x] Run fmt, clippy, and the crate tests with `server,nvme`; retain both original failing tests.
- [x] Require 20 consecutive successful runs of `pipelined_appends_are_acknowledged_when_durable_and_survive_a_restart` in CI, with no retries or failure suppression.
- [x] Record the proven cause and verification, obtain green CI and DCO, address CodeRabbit, and close #303 and #278.

## Rulings made during execution

| # | Ruling | Reason |
|---|---|---|
| 1 | Force a 4088-byte first unit, wait for durability, then append a second unit. | This leaves exactly eight padding bytes and separates grouping from recovery behavior. |
| 2 | Keep full record validation before treating an invalid eight-byte zero prefix at a block end as padding. | A real zero-length record with CRC zero must still decode normally. |
| 3 | The observed loss is in recovery, not the durable-write acknowledgement path: an eight-byte pad makes the parser inspect the following record's length as its kind, then stop at an invalid header. | The new two-unit regression returned one record before the fix and two after it, on the same filesystem and write path. |

## Verification before opening the PR

Both new boundary regressions failed before the parser change. Afterward,
`cargo fmt --all --check`, `cargo clippy -p loams-safekeeper --all-targets
--features server,nvme --locked -- -D warnings`, and `cargo test -p
loams-safekeeper --features server,nvme --locked` passed (78 unit tests and
seven service integration tests). CI repeats both original replay/restart
regressions 20 times, with `TMPDIR` on the runner's disk; every invocation must
pass. No test was weakened, and no on-disk format or acknowledgement rule changed.

Both original replay/restart regressions also passed 20 consecutive local runs
with test files under `~/.cache` (btrfs). The default-feature crate tests passed
as well. The deterministic pre-fix failure used tmpfs; CI uses the runner disk.

CI run 37039007140 passed all 20 consecutive iterations of both original
regressions. DCO, CodeQL, and CodeRabbit review passed; #308 merged as
`1da3ca9e71c181f0a4fa2027b7f3b311d686ae50`, closing #303 and #278.
The CI follow-up #306 checks exact test names before the loop so a later
rename cannot silently remove repeated regression coverage.
