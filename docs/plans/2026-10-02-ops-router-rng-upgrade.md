# OPS — Router RNG 0.10 upgrade (#311)

Status: Complete; merged #311 after actual Lean agreement, green CI/DCO and addressed CodeRabbit.

## Global constraints and Task 0

Keep design §31 §7.1 and RT0 Task 7's sans-I/O machine seam: time and seeded
randomness come only from the driver. Preserve the trace, range, hash and
Lean-oracle tests. Build only loams-sqlrouter under the shared lock.
The standalone rand_core bump breaks its old ChaCha driver and deprecates
RngCore. There are no Ctx consumers outside this crate in dev.

## Tasks

- [x] Add driver_randomness_preserves_the_legacy_seeded_stream before the
  adaptation; show the original 0.10-only upgrade fails compilation.
- [x] Migrate the kernel trait and only this crate's test driver to 0.10.
- [x] Verify three seeds and 64 draws per seed against the original 0.9
  generator; preserve all existing tests, kernel lints and licenses.
- [x] Trigger Lean validation for oracle-driver/manifest/toolchain changes.
- [x] Obtain actual 10,000-case Lean agreement on CI, green CI/DCO and
  CodeRabbit, review the full diff, and merge #311.

## Rulings made during execution

| # | Ruling | Reason |
|---|---|---|
| 1 | Use rand_core::Rng and router-only rand 0.10 with default features disabled and chacha enabled in dev tests; workspace rand 0.9 remains. | Only the isolated kernel and its drivers need to agree here, so the change fits the small-upgrade limit. Other crates have independent generators and no Ctx calls. |
| 2 | Keep rand_chacha 0.9 as the legacy stream oracle in one regression. | Protects seeded reproducibility across the upgrade without new packages or hard-coded generated values. |
| 3 | Lean CI also watches the oracle test, crate manifest and shared toolchain inputs. | Its old filter skipped the changed driver, so local tests without an oracle binary could not establish actual agreement. |

| 4 | Retain legacy unseeded RNG bans with allow-invalid for unavailable symbols and add the new make_rng ban. | Rand 0.10 with default features disabled exposes no thread-RNG functions; invalid-symbol configuration warnings do not justify removing the policy. The new API must remain forbidden if enabled later. |

## Verification

The original upgrade failed the old driver's rand_core trait bound (E0277)
and lacks the new ChaCha type (E0433); CI also caught deprecated RngCore.
After adaptation, the three-seed legacy comparison passes. Workspace fmt,
strict crate clippy, existing Rust tests, provenance and cargo deny pass.
The local Lean binary is absent: its existing test reports that it cannot
execute the oracle. Actual 10,000-case agreement is a CI gate, not claimed
from the local fallback. RT0 E22 records this dependency follow-up; the
§31 machine example reflects the new trait. No tests or lints are weakened.
Primary migration guide: https://rust-random.github.io/book/update-0.10.html.

Merged commit verified through the GitHub PR API: `c11e093f66d91efef5613698ef1742c9ac6a329b`.
Actual Lean agreement passed in [CI run 37052445546, job 110989089920](https://github.com/ostrium-labs/loams/actions/runs/37052445546/job/110989089920): 10,000 cases, no local-oracle skip.
