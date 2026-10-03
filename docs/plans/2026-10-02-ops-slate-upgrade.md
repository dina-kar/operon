# OPS — SlateDB 0.17 upgrade (#307)

Status: Complete; merged #307 after green CI/DCO and addressed CodeRabbit.

## Global constraints and Task 0

Keep design §03 §5: one fenced writer per scoped primary-key database,
acknowledged writes survive reopen, and databases sharing a cache remain
isolated. Preserve dependencies with default features disabled. The only
production adaptation is the cache API; build only loams-pk under the lock.

## Tasks

- [x] Add a cache scope/ownership regression before the adapter change;
  verify the old adapter fails to compile against 0.17.
- [x] Require an explicit caller-assigned scope alongside a shared cache.
- [x] Verify reopen durability, distinct cached scopes, cache ownership,
  default existing tests, fmt/clippy and dependency/license checks.
- [x] Obtain green CI/DCO and CodeRabbit review, merge #307.

## Rulings made during execution

| # | Ruling | Reason |
|---|---|---|
| 1 | Accept cache as an optional (Arc<dyn DbCache>, u64) pair; caller IDs are unique per database and stable on reopen. | Upstream 0.17 requires explicit scope IDs. A constant collides across databases; a process-local counter cannot preserve persistent scope identity across restarts. No in-repo caller configures a shared cache yet. |
| 2 | The caller closes an injected cache after every database using it closes. | Upstream 0.17 preserves injected-cache ownership instead of closing it with the first database. |

## Verification

The original adapter failed E0061 because with_db_cache now needs a u64 ID.
The regression flushes two indexes to SSTs, reads the same key with different
values, closes/reopens one while the other stays usable, and observes both
actual cached scope IDs without the database closing the shared cache.
A constant-ID mutation failed with cached scopes {0} instead of {11,22}.
After restoring the adapter, all 2 unit and 9 integration tests pass;
workspace fmt, strict crate clippy and cargo deny pass. The additional
async-trait dev edge uses the existing resolved dependency, with no new package.
Production adapter changes are seven lines; the remainder is test coverage.
Upstream API: https://github.com/slatedb/slatedb/blob/v0.17.0/slatedb/src/db/builder.rs.

Merged commit verified through the GitHub PR API: `e370217dc248d8b65b08513bfbe053e05b4af0ee`.
