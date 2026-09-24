# M0 Exit Report

Date: 2026-09-24 · Branch `m0.4-workers-links-gates` · Plans: [M0.1](2026-09-23-m0.1-storage-primitives.md), [M0.2](2026-09-23-m0.2-metastore.md), [M0.3](2026-09-24-m0.3-log-engine.md), [M0.4](2026-09-24-m0.4-workers-links-gates.md)

M0 (Foundation, design [§12 §1](../design/12-roadmap-testing-risks.md)) is done. All three exit gates pass, and every scope item has a test. Every command below was run with `--locked` on a 14-core Linux laptop.

## Exit gates

| Exit gate (design §12 §1) | What proves it | Result |
|---|---|---|
| **kill -9 at every step of the write, commit and segment paths**: no acknowledged-data loss, no torn state | `crates/operon/tests/crash.rs` (`cargo test -p operon --features failpoints --test crash`). One test per failpoint aborts a child `operon dev` process at that point (`std::process::abort()`, D29): `wal.after_put`, `wal.after_commit`, `seg.after_put`, `seg.after_swap`, `link.after_data_put`, `link.after_manifest_put`, `link.after_cas`, `gc.after_delete`, `meta.snapshot.after_put`, `meta.snapshot.after_pointer`, `retention.after_trim`. `random_sigkills_under_load_lose_nothing` adds 20 SIGKILLs at random times under load (`CRASH_KILLS`). After each restart the test checks: offsets are dense; every acknowledged record is readable exactly once at its offset; `CounterTable` sums match the log exactly once; and `MetaState::check_invariants` holds | **Pass.** 3 runs × 12 tests: 36/36 passed, 33 failpoint aborts and 60 random SIGKILLs |
| **Object-store PUT/GET/412/409 fault matrix passes** | `crates/operon/tests/fault_matrix.rs`. It crosses 7 component operations (writer flush, reader fetch, segmenter swap, retention trim, link commit, GC pass, meta snapshot) with every pair of `Put`/`PutCreate`/`PutIfMatch`/`Get`/`Delete`/`List` × `Error`/`ErrorAfterApply`/`Precondition` (409 create-only, 412 CAS)/`Delay(2 s)`, on the first and the second call. Every cell's outcome must equal the committed table `crates/operon/tests/fault_matrix.expected.md`; a surfaced error must be a retryable kind, and an error that never reached the fault fails the gate. After every cell the test checks: the segmenter runs without failures; no acknowledged record lost or changed; no definitely failed value visible; meta invariants hold; `CounterTable` exact | **Pass.** 336 cells, all asserted: 35 `Retried`, 9 `Deferred`, 60 `SurfacedRetryable`, 232 `NoEffect`. Table below |
| **Linearizability check on the sequencer and the manifest-pointer CAS** | `operon-sim`. `tests/checker.rs` runs 12 hand-written histories through the Wing–Gong–Lowe checker: linearizable ones, a stale read, a lost update, a duplicate offset, a gap, and indeterminate operations taken as applied or as not applied. `tests/sim.rs` runs the seeded cluster simulation (D28): 3 meta nodes over `Router`, 2 writers, a reader, a worker with the segmenter, retention, link apply and GC, `FaultyStore::random`, node isolation and healing, and worker crashes. It checks linearizability per partition sequencer and per CAS register, that every acknowledged append is readable, that `CounterTable` equals the model, meta invariants on every node, and that no node has a fatal Raft error | **Pass.** Checker 12/12. CI default sweep: 32 seeds × 300 steps. Extra sweep: 64 seeds × 300 steps with 5 849 acknowledged appends and 79 indeterminate operations, 0 violations. The implementer's earlier 128-seed sweep: 11 582 appends and 179 indeterminate operations, 0 violations |

## M0 scope items

| Scope item (design §12 §1) | Tests |
|---|---|
| `operon dev` / `operon standalone` | `crates/operon/tests/http.rs` (11); the crash gate drives `operon dev` |
| Meta on openraft | `crates/operon-meta` (95 tests: single node, 3-node cluster, snapshots, leases, pointers, sequencer, segments, links, state-machine property tests) |
| `object_store` abstraction with fault-injection wrapper | `crates/operon-store/tests/*`, including `fault.rs` for the injector itself |
| Internal stream engine (`standard` class, leaderless sequencing, segmenter) | `crates/operon-log/tests/*` (writer, reader, segmenter, retention, e2e, WAL and segment formats) |
| foyer cache (H0/H1) | `crates/operon-cache/tests/range_cache.rs` |
| Worker leases + task framework | `crates/operon-worker/tests/worker.rs` (7): mutual exclusion, takeover cancels and fences, an expired lease re-taken at the same epoch, priority order, namespace fair share, a dropped worker's lease expiring, `run_once` |
| Link framework (exactly-once apply) | `crates/operon-link/tests/link.rs` (6): exactly once across crashes at every commit step, a zombie task cannot double-apply, stale-version conflicts, apply over a stream being segmented and trimmed, dead letters |
| PkIndex on SlateDB | `crates/operon-pk/tests/pk.rs` (7): a second open fences the first writer, lost PUT acknowledgements, reader refresh, 100 000 keys in the time budget |
| GC | `crates/operon-log/tests/gc.rs` (5, including the review I1 reproduction `a_swap_delayed_past_its_deadline_is_refused_after_gc_deleted_the_segment`); `crates/operon-link/tests/gc.rs` (2), including the property test `gc_never_breaks_reads_links_or_the_index` |
| DST harness | `operon-sim`, a seeded simulation (D28, see Known limitations) |

## Gate runs

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | pass |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| `cargo clippy -p operon -p operon-link -p operon-log -p operon-meta --all-targets --features failpoints --locked -- -D warnings` | pass |
| `cargo test --workspace --locked` | pass: 254 tests, 0 failed (after the review fixes) |
| `cargo test -p operon --features failpoints --test crash --locked` | pass: 3 of 3 runs before the review fixes and 2 of 2 after (12 tests each) |
| `cargo test -p operon-sim --release --locked` (32 seeds) | pass |
| `SIM_SEEDS=64 cargo test -p operon-sim --release --locked --test sim` | pass, 64 of 64 seeds (after the fixes: 5 898 acknowledged appends, 76 indeterminate operations; the sweep now also checks that no index entry or link pointer dangles) |
| `cargo deny check` | advisories, bans, licenses, sources ok |
| Stress: 3 rounds × 4 parallel copies of the `operon-worker` worker, `operon-link` link and gc, `operon-log` gc and e2e, and `operon` http test binaries (24 processes per round) | 72 of 72 processes exited 0 (384 tests) |
| Stress after the review fixes: 2 rounds × the same 6 binaries × 4 copies | 48 of 48 processes exited 0 (264 tests) |

The GC property test's `gc.proptest-regressions` holds 6 workloads, all from one earlier stress loop that failed with "a young WAL orphan was deleted". The machine suspended for 1 h 57 min in the middle of that loop (systemd-logind, 19:01 to 20:58). The wall clock jumped past the orphan's age limit (twice the 15-minute commit window plus the grace period), so GC was right to delete it. The same loop's two link "never caught up" failures and its one cluster "log was never purged" failure have the same cause. It was not a GC bug. The six seeds pass alone, under 4 and 12 parallel copies, and in the stress loop above.

## Fault matrix

As asserted by `fault_matrix.rs` against `crates/operon/tests/fault_matrix.expected.md` (and written to `target/fault-matrix.md`). A cell's outcome:
- `Retried`: the fault was reached and the operation succeeds after a retry or rides out the delay.
- `Deferred`: GC only: the fault was reached, the pass completed and left that object for the next pass.
- `SurfacedRetryable`: the caller gets a retryable error and nothing is acknowledged.
- `NoEffect`: the operation never issues that store call, or the call's failure changes nothing.

<details><summary>All 336 cells</summary>

| Component | Op | Fault | 1st call | 2nd call |
|---|---|---|---|---|
| WriterFlush | Put | Error | SurfacedRetryable | SurfacedRetryable |
| WriterFlush | Put | ErrorAfterApply | SurfacedRetryable | SurfacedRetryable |
| WriterFlush | Put | Precondition | SurfacedRetryable | SurfacedRetryable |
| WriterFlush | Put | Delay(2s) | Retried | Retried |
| WriterFlush | PutCreate | Error | SurfacedRetryable | SurfacedRetryable |
| WriterFlush | PutCreate | ErrorAfterApply | SurfacedRetryable | SurfacedRetryable |
| WriterFlush | PutCreate | Precondition | SurfacedRetryable | SurfacedRetryable |
| WriterFlush | PutCreate | Delay(2s) | Retried | Retried |
| WriterFlush | PutIfMatch | Error | NoEffect | NoEffect |
| WriterFlush | PutIfMatch | ErrorAfterApply | NoEffect | NoEffect |
| WriterFlush | PutIfMatch | Precondition | NoEffect | NoEffect |
| WriterFlush | PutIfMatch | Delay(2s) | NoEffect | NoEffect |
| WriterFlush | Get | Error | NoEffect | NoEffect |
| WriterFlush | Get | ErrorAfterApply | NoEffect | NoEffect |
| WriterFlush | Get | Precondition | NoEffect | NoEffect |
| WriterFlush | Get | Delay(2s) | NoEffect | NoEffect |
| WriterFlush | Delete | Error | NoEffect | NoEffect |
| WriterFlush | Delete | ErrorAfterApply | NoEffect | NoEffect |
| WriterFlush | Delete | Precondition | NoEffect | NoEffect |
| WriterFlush | Delete | Delay(2s) | NoEffect | NoEffect |
| WriterFlush | List | Error | NoEffect | NoEffect |
| WriterFlush | List | ErrorAfterApply | NoEffect | NoEffect |
| WriterFlush | List | Precondition | NoEffect | NoEffect |
| WriterFlush | List | Delay(2s) | NoEffect | NoEffect |
| ReaderFetch | Put | Error | NoEffect | NoEffect |
| ReaderFetch | Put | ErrorAfterApply | NoEffect | NoEffect |
| ReaderFetch | Put | Precondition | NoEffect | NoEffect |
| ReaderFetch | Put | Delay(2s) | NoEffect | NoEffect |
| ReaderFetch | PutCreate | Error | NoEffect | NoEffect |
| ReaderFetch | PutCreate | ErrorAfterApply | NoEffect | NoEffect |
| ReaderFetch | PutCreate | Precondition | NoEffect | NoEffect |
| ReaderFetch | PutCreate | Delay(2s) | NoEffect | NoEffect |
| ReaderFetch | PutIfMatch | Error | NoEffect | NoEffect |
| ReaderFetch | PutIfMatch | ErrorAfterApply | NoEffect | NoEffect |
| ReaderFetch | PutIfMatch | Precondition | NoEffect | NoEffect |
| ReaderFetch | PutIfMatch | Delay(2s) | NoEffect | NoEffect |
| ReaderFetch | Get | Error | SurfacedRetryable | SurfacedRetryable |
| ReaderFetch | Get | ErrorAfterApply | SurfacedRetryable | SurfacedRetryable |
| ReaderFetch | Get | Precondition | SurfacedRetryable | SurfacedRetryable |
| ReaderFetch | Get | Delay(2s) | Retried | Retried |
| ReaderFetch | Delete | Error | NoEffect | NoEffect |
| ReaderFetch | Delete | ErrorAfterApply | NoEffect | NoEffect |
| ReaderFetch | Delete | Precondition | NoEffect | NoEffect |
| ReaderFetch | Delete | Delay(2s) | NoEffect | NoEffect |
| ReaderFetch | List | Error | NoEffect | NoEffect |
| ReaderFetch | List | ErrorAfterApply | NoEffect | NoEffect |
| ReaderFetch | List | Precondition | NoEffect | NoEffect |
| ReaderFetch | List | Delay(2s) | NoEffect | NoEffect |
| SegmenterSwap | Put | Error | SurfacedRetryable | SurfacedRetryable |
| SegmenterSwap | Put | ErrorAfterApply | SurfacedRetryable | SurfacedRetryable |
| SegmenterSwap | Put | Precondition | SurfacedRetryable | SurfacedRetryable |
| SegmenterSwap | Put | Delay(2s) | Retried | Retried |
| SegmenterSwap | PutCreate | Error | SurfacedRetryable | SurfacedRetryable |
| SegmenterSwap | PutCreate | ErrorAfterApply | SurfacedRetryable | SurfacedRetryable |
| SegmenterSwap | PutCreate | Precondition | SurfacedRetryable | SurfacedRetryable |
| SegmenterSwap | PutCreate | Delay(2s) | Retried | Retried |
| SegmenterSwap | PutIfMatch | Error | NoEffect | NoEffect |
| SegmenterSwap | PutIfMatch | ErrorAfterApply | NoEffect | NoEffect |
| SegmenterSwap | PutIfMatch | Precondition | NoEffect | NoEffect |
| SegmenterSwap | PutIfMatch | Delay(2s) | NoEffect | NoEffect |
| SegmenterSwap | Get | Error | SurfacedRetryable | SurfacedRetryable |
| SegmenterSwap | Get | ErrorAfterApply | SurfacedRetryable | SurfacedRetryable |
| SegmenterSwap | Get | Precondition | SurfacedRetryable | SurfacedRetryable |
| SegmenterSwap | Get | Delay(2s) | Retried | Retried |
| SegmenterSwap | Delete | Error | NoEffect | NoEffect |
| SegmenterSwap | Delete | ErrorAfterApply | NoEffect | NoEffect |
| SegmenterSwap | Delete | Precondition | NoEffect | NoEffect |
| SegmenterSwap | Delete | Delay(2s) | NoEffect | NoEffect |
| SegmenterSwap | List | Error | NoEffect | NoEffect |
| SegmenterSwap | List | ErrorAfterApply | NoEffect | NoEffect |
| SegmenterSwap | List | Precondition | NoEffect | NoEffect |
| SegmenterSwap | List | Delay(2s) | NoEffect | NoEffect |
| RetentionTrim | Put | Error | NoEffect | NoEffect |
| RetentionTrim | Put | ErrorAfterApply | NoEffect | NoEffect |
| RetentionTrim | Put | Precondition | NoEffect | NoEffect |
| RetentionTrim | Put | Delay(2s) | NoEffect | NoEffect |
| RetentionTrim | PutCreate | Error | NoEffect | NoEffect |
| RetentionTrim | PutCreate | ErrorAfterApply | NoEffect | NoEffect |
| RetentionTrim | PutCreate | Precondition | NoEffect | NoEffect |
| RetentionTrim | PutCreate | Delay(2s) | NoEffect | NoEffect |
| RetentionTrim | PutIfMatch | Error | NoEffect | NoEffect |
| RetentionTrim | PutIfMatch | ErrorAfterApply | NoEffect | NoEffect |
| RetentionTrim | PutIfMatch | Precondition | NoEffect | NoEffect |
| RetentionTrim | PutIfMatch | Delay(2s) | NoEffect | NoEffect |
| RetentionTrim | Get | Error | NoEffect | NoEffect |
| RetentionTrim | Get | ErrorAfterApply | NoEffect | NoEffect |
| RetentionTrim | Get | Precondition | NoEffect | NoEffect |
| RetentionTrim | Get | Delay(2s) | NoEffect | NoEffect |
| RetentionTrim | Delete | Error | NoEffect | NoEffect |
| RetentionTrim | Delete | ErrorAfterApply | NoEffect | NoEffect |
| RetentionTrim | Delete | Precondition | NoEffect | NoEffect |
| RetentionTrim | Delete | Delay(2s) | NoEffect | NoEffect |
| RetentionTrim | List | Error | NoEffect | NoEffect |
| RetentionTrim | List | ErrorAfterApply | NoEffect | NoEffect |
| RetentionTrim | List | Precondition | NoEffect | NoEffect |
| RetentionTrim | List | Delay(2s) | NoEffect | NoEffect |
| LinkCommit | Put | Error | SurfacedRetryable | SurfacedRetryable |
| LinkCommit | Put | ErrorAfterApply | SurfacedRetryable | SurfacedRetryable |
| LinkCommit | Put | Precondition | SurfacedRetryable | SurfacedRetryable |
| LinkCommit | Put | Delay(2s) | Retried | Retried |
| LinkCommit | PutCreate | Error | SurfacedRetryable | SurfacedRetryable |
| LinkCommit | PutCreate | ErrorAfterApply | SurfacedRetryable | SurfacedRetryable |
| LinkCommit | PutCreate | Precondition | SurfacedRetryable | SurfacedRetryable |
| LinkCommit | PutCreate | Delay(2s) | Retried | Retried |
| LinkCommit | PutIfMatch | Error | NoEffect | NoEffect |
| LinkCommit | PutIfMatch | ErrorAfterApply | NoEffect | NoEffect |
| LinkCommit | PutIfMatch | Precondition | NoEffect | NoEffect |
| LinkCommit | PutIfMatch | Delay(2s) | NoEffect | NoEffect |
| LinkCommit | Get | Error | SurfacedRetryable | SurfacedRetryable |
| LinkCommit | Get | ErrorAfterApply | SurfacedRetryable | SurfacedRetryable |
| LinkCommit | Get | Precondition | SurfacedRetryable | SurfacedRetryable |
| LinkCommit | Get | Delay(2s) | Retried | Retried |
| LinkCommit | Delete | Error | NoEffect | NoEffect |
| LinkCommit | Delete | ErrorAfterApply | NoEffect | NoEffect |
| LinkCommit | Delete | Precondition | NoEffect | NoEffect |
| LinkCommit | Delete | Delay(2s) | NoEffect | NoEffect |
| LinkCommit | List | Error | NoEffect | NoEffect |
| LinkCommit | List | ErrorAfterApply | NoEffect | NoEffect |
| LinkCommit | List | Precondition | NoEffect | NoEffect |
| LinkCommit | List | Delay(2s) | NoEffect | NoEffect |
| GcPass | Put | Error | NoEffect | NoEffect |
| GcPass | Put | ErrorAfterApply | NoEffect | NoEffect |
| GcPass | Put | Precondition | NoEffect | NoEffect |
| GcPass | Put | Delay(2s) | NoEffect | NoEffect |
| GcPass | PutCreate | Error | NoEffect | NoEffect |
| GcPass | PutCreate | ErrorAfterApply | NoEffect | NoEffect |
| GcPass | PutCreate | Precondition | NoEffect | NoEffect |
| GcPass | PutCreate | Delay(2s) | NoEffect | NoEffect |
| GcPass | PutIfMatch | Error | NoEffect | NoEffect |
| GcPass | PutIfMatch | ErrorAfterApply | NoEffect | NoEffect |
| GcPass | PutIfMatch | Precondition | NoEffect | NoEffect |
| GcPass | PutIfMatch | Delay(2s) | NoEffect | NoEffect |
| GcPass | Get | Error | Deferred | NoEffect |
| GcPass | Get | ErrorAfterApply | Deferred | NoEffect |
| GcPass | Get | Precondition | Deferred | NoEffect |
| GcPass | Get | Delay(2s) | Retried | NoEffect |
| GcPass | Delete | Error | Deferred | Deferred |
| GcPass | Delete | ErrorAfterApply | Deferred | Deferred |
| GcPass | Delete | Precondition | Deferred | Deferred |
| GcPass | Delete | Delay(2s) | Retried | Retried |
| GcPass | List | Error | SurfacedRetryable | SurfacedRetryable |
| GcPass | List | ErrorAfterApply | SurfacedRetryable | SurfacedRetryable |
| GcPass | List | Precondition | SurfacedRetryable | SurfacedRetryable |
| GcPass | List | Delay(2s) | Retried | Retried |
| MetaSnapshot | Put | Error | Retried | Retried |
| MetaSnapshot | Put | ErrorAfterApply | Retried | Retried |
| MetaSnapshot | Put | Precondition | Retried | Retried |
| MetaSnapshot | Put | Delay(2s) | Retried | Retried |
| MetaSnapshot | PutCreate | Error | NoEffect | NoEffect |
| MetaSnapshot | PutCreate | ErrorAfterApply | NoEffect | NoEffect |
| MetaSnapshot | PutCreate | Precondition | NoEffect | NoEffect |
| MetaSnapshot | PutCreate | Delay(2s) | NoEffect | NoEffect |
| MetaSnapshot | PutIfMatch | Error | NoEffect | NoEffect |
| MetaSnapshot | PutIfMatch | ErrorAfterApply | NoEffect | NoEffect |
| MetaSnapshot | PutIfMatch | Precondition | NoEffect | NoEffect |
| MetaSnapshot | PutIfMatch | Delay(2s) | NoEffect | NoEffect |
| MetaSnapshot | Get | Error | NoEffect | NoEffect |
| MetaSnapshot | Get | ErrorAfterApply | NoEffect | NoEffect |
| MetaSnapshot | Get | Precondition | NoEffect | NoEffect |
| MetaSnapshot | Get | Delay(2s) | NoEffect | NoEffect |
| MetaSnapshot | Delete | Error | Retried | NoEffect |
| MetaSnapshot | Delete | ErrorAfterApply | Retried | NoEffect |
| MetaSnapshot | Delete | Precondition | Retried | NoEffect |
| MetaSnapshot | Delete | Delay(2s) | Retried | NoEffect |
| MetaSnapshot | List | Error | NoEffect | NoEffect |
| MetaSnapshot | List | ErrorAfterApply | NoEffect | NoEffect |
| MetaSnapshot | List | Precondition | NoEffect | NoEffect |
| MetaSnapshot | List | Delay(2s) | NoEffect | NoEffect |

Cells: 336 ({"Deferred": 9, "NoEffect": 232, "Retried": 35, "SurfacedRetryable": 60}).

</details>

## Known limitations

- **The simulation is seeded, not deterministic (D28).** openraft, redb and `object_store` do real I/O on real time. A failing seed prints its full schedule but may not replay exactly. A madsim/turmoil port stays an option.
- **The crash gate kills a single-node `operon dev`.** Multi-node metastore failures (isolation, restarts) are covered by the in-process simulation, not by kill -9 of separate processes.
- **The fault matrix injects one fault per cell.** The fault kinds are `Error`, `ErrorAfterApply`, `Precondition` and a 2 s `Delay`. Partial reads and a distinct 503 SlowDown kind (§12 §2 item 2) are not modelled separately: a 503 is an `Error`. Combined and random faults come from the simulation (`FaultyStore::random`).
- **GC safety depends on time bounds, now enforced by the metastore.** After the M0.4 review (I1), `SwapSegment` and a link's pointer CAS carry the new object's creation time and deadline, and the metastore refuses them once its clock is past the deadline. GC measures ages against the metastore clock. So the only requirement left is configuration: the segmenter's `swap_deadline` and the link's `max_commit_delay` must be below GC's `grace`. The server clamps both to half the grace; `GcConfig` does not check it. The metastore clock is the latest stamped time, so a proposer whose clock is behind other nodes' (within the 5-minute skew bound) gets its swaps and commits refused sooner. Collection manifests in M1 must follow the same rule: a ULID in the name and a fresh CAS.
- **Time comes from wall clocks.** Leases, retention, the WAL commit window and object ages use proposers' wall-clock stamps, which the metastore clock takes the maximum of. A host suspend makes objects old and leases expire all at once. That cannot make GC delete a referenced object, but timing-based tests fail if the host sleeps mid-run.
- **GC lists whole prefixes on every run.** `wal/`, `ns/<ns>/streams/` and `ns/<ns>/links/` are listed in full each time; `list_page` bounds deletes per pass, not the listing.
- **Scheduling is minimal (§09 §6 as built).** Priority order plus round-robin across namespaces, under global and per-namespace caps. There are no weights, byte-rate caps, autoscaling signals or per-class resource budgets.
- **One link task per link (D30).** A link does not split into partition ranges yet.
- **The toy target is the only link target.** `CounterTable` is a test target. `PkIndex` has no production user until M1 keyed collections.
- **Carried from M0.3.** Segmenting by age uses client-supplied record timestamps. The segmenter and retention scan every partition on each run. Fetches read a whole WAL chunk even for a small range.
- **Leader clock bound.** `max_clock_skew` (5 min) bounds proposers against the leader's clock. A leader whose own clock is wrong by more than that refuses correct writers. The race-free bound (the leader stamps each entry) is planned for M5.
- **One `target/debug/operon` for two feature sets.** `cargo test -p operon --features failpoints` rebuilds the shared `target/debug/operon` with failpoints. Running the `http` test binary directly after that hangs `a_build_without_failpoints_refuses_to_arm_them`, until `cargo build -p operon` rebuilds it. CI runs the two in separate jobs, and `cargo test` rebuilds the binary itself.
- **Simulation gaps (review M4).** One worker runs at a time, so takeover by a live successor is covered only by the worker and link unit tests, and the simulated `events` stream has no retention.
- **`CounterTable` cost (review M9).** Every manifest lists every data file, and `snapshot()` reads all of them. That is fine for a test target, but not a design for M1 targets.
