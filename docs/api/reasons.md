# The `ErrorInfo.reason` registry

Status: **API1 Task 1** (2026-10-03). Design [§44](../design/44-unified-api-and-sdks.md) §7.4, decision **D611**; plan [API1](../plans/2026-10-02-api1-unified-connect.md).

Every failed RPC carries a Connect code (`unimplemented`, `not_found`, ...) and one `loams.errors.v1.ErrorInfo` in its details. Callers branch on **`reason`**, a stable `snake_case` string, never on `code` alone and never on `message` (which may change). This page is the registry of every `reason` the server can return, so a caller can look one up instead of guessing a string.

The rules, in the order they bite:

- A reason is added here **before** an RPC returns it.
- Within an API major version a reason is **never renamed and never removed**. A bug fix is a new reason; a deprecated one keeps working and stops being returned.
- `snake_case`, `[a-z0-9_]` only, unique. The test `reasons_are_snake_case_and_unique` (`crates/loams/tests/connect_api.rs`) reads this page and fails on a duplicate, on a name outside `snake_case`, and on a registry that has lost one of the reasons it promised.
- The mapping from `reason` to a typed error class is **generated** by SDK1 from this page plus the protos; until then each SDK maps the codes of its own language.

`metadata` carries structured context (`{"variant": "standard"}`), never secrets. `hint` is a short next step in the caller's locale.

## Registry

| reason | Connect code | Raised by | Metadata |
|---|---|---|---|
| `approval_expired` | failed_precondition | DecideApproval | |
| `approval_already_decided` | failed_precondition | DecideApproval | |
| `approval_stale_revision` | failed_precondition | DecideApproval | |
| `requester_cannot_approve` | permission_denied | DecideApproval | |
| `decision_proof_invalid` | permission_denied | DecideApproval | |
| `step_up_required` | unauthenticated | DecideApproval, CreatePairing | |
| `reason_required` | invalid_argument | DecideApproval | |
| `invalid_decision` | invalid_argument | DecideApproval | |
| `pairing_expired` | failed_precondition | the pairing grant (MT) | |
| `pairing_used` | failed_precondition | the pairing grant (MT) | |
| `device_revoked` | unauthenticated | every RPC from a revoked device | |
| `push_target_unknown` | not_found | UnregisterPushTarget | |
| `not_implemented` | unimplemented | a stub handler whose service has not landed yet | |
| `feature_not_in_variant` | unimplemented | any RPC of a catalogue package whose engine is not in this build variant (§44 §4, §30 §9) | `variant` |
| `invalid_argument` | invalid_argument | any RPC: a malformed field, an unparseable value | `field` |
| `not_found` | not_found | any RPC: the named resource does not exist | |
| `already_exists` | already_exists | any RPC that creates a named resource | |
| `permission_denied` | permission_denied | any RPC the caller's role may not make | |
| `token_expired` | unauthenticated | a rejected access token | `hint` says whether to refresh or sign in again |
| `unauthenticated` | unauthenticated | any RPC with no or an unusable credential | |
| `failed_precondition` | failed_precondition | any RPC whose preconditions do not hold | |
| `resource_exhausted` | resource_exhausted | a backpressure or quota refusal | `retry_after_ms` |
| `unavailable` | unavailable | a dependency is down or this node cannot serve the read | |
| `deadline_exceeded` | deadline_exceeded | the caller's deadline passed | |
| `aborted` | aborted | a concurrent write won; the caller retries | |
| `internal` | internal | a bug; the message and `request_id` go to the log | |

The generic rows (`invalid_argument` and below) are the code-to-class mapping of D611's error hierarchy: they are what an SDK turns into its own `InvalidArgument`, `NotFound`, ... types. They carry no Loams-specific semantics, so they were already the native REST API's `error` values (`crates/loams/src/api/errors.rs`) and become the RPC `reason` unchanged. The rows above them are the specific causes AP0 registered; the specific causes API1 Tasks 2–8 add are appended as their services land, and `unavailable_service_reports_reason` covers the variant case.

## Not reasons

- **`cursor_expired`** is not an error: a watch stream whose cursor the server no longer has resets with a fresh snapshot instead (§37 §8.3).
- A **`Deprecated`** RPC (a removed REST route behind `--legacy-rest`, §44 §5.3) answers `308`/`410` with `{"error":"moved","rpc":…}`, not an `ErrorInfo`.

## Adding one

1. Add the row here, with the Connect code it is raised under and the RPC that raises it.
2. Raise it through the one helper that builds the error (`crates/loams/src/api/connect.rs`), so the code and the `reason` cannot drift apart.
3. Run `cargo test -p loams --test connect_api`. `reasons_are_snake_case_and_unique` fails on a duplicate or a name that is not `snake_case`.