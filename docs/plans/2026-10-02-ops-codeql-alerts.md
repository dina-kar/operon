# OPS — CodeQL alerts (#298)

Status: Task 1 implemented; native-ceiling and single-point fixes pass all local gates. Bounded vector growth follow-up awaiting local checks and CI; Tasks 2–3 pending.

## Global Constraints

Keep Qdrant compatibility adapters and D132–D137: one executor for REST
and gRPC, Qdrant envelopes, HTTP 400 / gRPC InvalidArgument for bad input,
and documented deliberate divergences in design 06 §8 and the crate docs.
No new native REST surface, dependencies, licenses or storage calls.
Preserve all existing tests. Build touched crates under the shared lock.
One task per PR; required CI, DCO and CodeRabbit must pass before merge.

## Task 0 — Reconciliation

The stopped codeql-298 agent left configurable query-batch and retrieve
count guards, three regressions, and two separate panic redactions.
Its complete patch is preserved outside the repository. The allocation
patch is applied to current dev; redaction follows in its own PR.
Existing request byte limits do not bound executor amplification tightly.
The stopped retrieve default of 100,000 conflicts with native max_get_keys
10,000; use 10,000 so a boundary request can be served unchanged.
D133 requires both protocols to reach the same core. gRPC conversion also
allocates lists, so reject excessive counts before conversion there.
Qdrant has optional strict-mode search_max_batchsize (not an unconditional
count ceiling); its default REST request cap is 32 MiB. Loams's limits
are a documented divergence and remain independent of its no-op strict
mode config. Do not claim Qdrant has no count controls.
The current alert set is #1, #2, #4, #5, #19 and #20. Older test alerts
already left the open set. D303 requires Vitess null-key DES routing;
its crypto alerts need a documented non-security-use audit, not a hash
change that would move keys to other shards.

## Tasks

### Task 1 — Bound batch and retrieve executor allocations (one PR)

- [x] Tests first: check_request_len_refuses_huge_lengths,
  query_batch_refuses_more_than_max_batch_queries,
  retrieve_refuses_more_ids_than_max_point_ids,
  grpc_batch_counts_are_checked_before_conversion,
  grpc_retrieve_count_is_checked_before_conversion,
  retrieve_limit_tracks_native_configuration,
  retrieve_limit_does_not_block_single_point_get.
- [x] Reject batch counts above 1,000 and retrieve IDs above 10,000 by
  default before executor allocations; configurable limits accept zero
  and their exact boundary. Cap list retrieval by native max_get_keys;
  single-point GET uses its own fixed one-ID bound. Test shared REST/gRPC
  behavior and legacy search/recommend/discover batches, including malformed oversized gRPC.
- [x] Document defaults, error text and compatibility divergence.
- [ ] fmt, strict clippy, touched-crate tests; required CI/DCO/CodeQL,
  CodeRabbit and complete diff review; merge and update #237.

### Task 2 — Redact unreachable query values (one PR)

Audit the two flagged compile branches. Test private panic paths with
user-controlled values; replace value formatting with constant messages.
Keep normal query semantics and existing compile tests. Record exact test
names in this task before writing them. Run touched-query-crate gates,
CI/CodeQL, CodeRabbit and full review before merging and updating #237.

### Task 3 — Audit and reconcile alert state (one PR if docs change)

Verify historical test-alert dismissals. Audit #19/#20 against D303 and
Vitess attribution; dismiss only verified non-security hash use with a
specific reason. Confirm no open dev alerts after the merged CodeQL run,
mark all tasks complete and close #298; update #237.

## Rulings made during execution

| # | Ruling | Reason |
|---|---|---|
| 1 | Keep the stopped agent's batch default of 1,000 and use a retrieve default of 10,000 IDs as configurable gateway limits, separate from strict mode. | Bounded executor amplification; the native get ceiling is 10,000, so the retrieve boundary must fit it. These are Loams limits, not claims of Qdrant parity. |
| 2 | Check gRPC list counts before converting malformed entries and again in the shared core. | Conversion itself allocates; both protocol paths must enforce the same limits. Oversized lists consistently report count errors. |
| 3 | Separate allocation fixes, privacy fixes and final alert audit. | One plan task per PR and individually reviewable security changes. |
| 4 | Replace the stopped retrieve default with 10,000 IDs. | Before-fix tests exposed the native max_get_keys ceiling of 10,000; increasing native limits is outside this security task. |
| 5 | REST discover batches check count before per-entry validation. | The shared legacy path converts later; consistent oversized-request errors must precede discovery validation too. |
| 6 | Use min(max_point_ids, native max_get_keys) for REST and gRPC list retrieval. | CodeRabbit identified that the native ceiling can be configured below the gateway ceiling; reject before allocation/conversion consistently. |
| 7 | Single-point GET has a fixed one-ID executor bound, independent of the configurable list limit. | A zero list limit must not disable a separately documented single-point API; the native service still enforces its own limit. |
| 8 | Retain count checks and grow initially empty result/key vectors within their enforced bounds instead of eagerly allocating from client lengths. | CodeQL still flags the configuration-capped preallocation expressions; bounded incremental growth removes the eager client-sized allocation and also avoids capacity allocation for empty requests. |

## References

- [Design 06 §8](../design/06-search-and-vector.md#8-qdrant-compatibility-scope)
- [Decision log D132–D137 and D303](../design/13-decision-log.md)
- [Original M1.4 reconciliation](2026-09-24-m1.4-qdrant-api.md)
- [Qdrant v1.19.1 request size default](https://github.com/qdrant/qdrant/blob/v1.19.1/config/config.yaml#L289-L291)
- [Qdrant v1.19.1 optional strict-mode limits](https://github.com/qdrant/qdrant/blob/v1.19.1/lib/segment/src/types.rs)

## Verification

With executor and gRPC guards absent, both REST regressions and both gRPC
count regressions failed. The first retrieve run also exposed that the
stopped 100,000 default exceeded native max_get_keys=10,000. After aligning
the default and guarding REST discovery before entry validation, all 161
Qdrant integration tests initially passed; both native-limit and single-point
GET review regressions then failed before their fixes. With the shared
effective list limit and fixed single-point bound, all 163 pass. The malformed query fixture explicitly
asserts that direct gRPC conversion fails; the guarded oversized batch
returns the count error instead. Existing tests are retained unchanged.

Workspace fmt, strict all-targets clippy for loams-qdrant and loams,
provenance and decision-ID checks pass. Full default-feature tests for both
touched crates run with CARGO_PROFILE_TEST_DEBUG=0 to limit disk use; this
changes debug symbols only, preserving assertions. CI uses its normal
profile. Both touched crates' full default-feature tests pass, including
the complete fault matrix after providing its local target report directory
through an untracked symlink to the shared cache. No test source, assertion
or expectation changes. No dependency change requires cargo deny for this task.
