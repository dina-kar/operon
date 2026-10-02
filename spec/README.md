# Formal specifications

Formal models of the Loams protocols where a design bug would be expensive (design §31 §11–§12, D310, D312).

| Path | What | Checked by |
|---|---|---|
| [`tla/router/`](tla/router/README.md) | The router's shard map, resharding cutover, cross-shard commit, primary failover and session contract | TLC and Apalache, CI job `tla` |
| `tla/selftest/` | A spec that must fail, so the `tla` job proves it notices failures | `tla` |
| `lean/` (RT0 Task 8) | Lean 4 proofs of the key-range partition lemmas, and the oracle the Rust tests call | CI job `lean` |

Run everything with `scripts/spec/check.sh --all`. The tools are pinned by SHA-256 in [`scripts/spec/tools.lock`](../scripts/spec/tools.lock): TLC (`tla2tools.jar`, MIT) and Apalache (Apache-2.0).

**Licenses and provenance.** The specs are Apache-2.0 like the rest of the repository. They are written from the designs and from the documented and observed behaviour of the systems they model. PgDog (AGPL-3.0) is never copied. `scripts/spec/provenance.sh` fails the `tla` job if its license text or distinctive identifiers appear here (D318). Neon's `safekeeper/spec/` (Apache-2.0) is the reference for the layout and for the Paxos half of `PrimaryFailover`: cited, not copied.
