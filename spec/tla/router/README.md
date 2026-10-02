# Router specs (TLA+)

The formal specifications of the Loams router's sharding control plane (design [§31](../../../docs/design/31-loams-router-and-verification.md) §11, D310). Plan: [RT0](../../../docs/plans/2026-10-01-rt0-foundations-and-specs.md) Tasks 1–4.

Run them with `scripts/spec/check.sh` (needs Java 21+ and Python 3.11+; it downloads the pinned tools into `~/.cache/loam/spec-tools/` and checks their SHA-256):

```sh
scripts/spec/check.sh --all              # every PR variant
scripts/spec/check.sh --all --nightly    # the nightly bounds
scripts/spec/check.sh ShardMap MCShardMap_UnsafeConfigMap.cfg
scripts/spec/check.sh RouterSession --parse-only
scripts/spec/check.sh --self-test        # the job notices failures and bad tool hashes
```

`TLC_WORKERS` sets TLC's worker count (default 2). TLC's state queue spills to its metadir, which `check.sh` keeps under `~/.cache/loam/spec-work/` (override with `LOAMS_SPEC_WORK`), never in `/tmp`.

## Specs and variants

Variants and their expected outcome live in [`specs.toml`](specs.toml). An `expect = "violation:<Invariant>"` variant documents an unsafe configuration and must fail for exactly that reason.

| Spec | Variant | Bounds | Expect | Distinct states | Time (4 workers, local) |
|---|---|---|---|---|---|
| `ShardMap` | `Small` (PR) | 2 keys, 3 shards, 2 instances, 2 generations, 3 writes; with `Converges` | ok | 655,107 | 14 s |
| `ShardMap` | `Small`, Apalache `SingleWriter` | length 8 | ok | — | 79 s |
| `ShardMap` | `UnsafeConfigMap` (PR) | `Small` with the ConfigMap written first | `violation:SingleWriter` | 108 | 1 s |
| `ShardMap` | `Nightly` | 2 keys, 3 shards, 3 instances, 3 generations, 4 writes | ok | see the nightly job | — |
| `ReshardCutover` | `Small` (PR) | 1 key, instances `d` (designated) and `i2`, 3 writes, 1 saga crash; with `Terminates` | ok | 8,257 | 1 s |
| `ReshardCutover` | `Small`, Apalache `SingleWriterRange` | length 10 | ok | — | 14 s |
| `ReshardCutover` | `CrashSaga` (PR) | `Small` with 2 saga crashes | ok | 13,541 | 1 s |
| `ReshardCutover` | `NoFence` (PR) | `Small` with no backend fence | `violation:SingleWriterRange` | 772 | 1 s |
| `ReshardCutover` | `Nightly` | 2 keys, 3 instances, 4 writes | ok | 791,265 | 24 s |
| `CrossShardCommit`, `PrimaryFailover`, `RouterSession` | parse only | — | parses | — | — |

A first `ShardMap` nightly at three keys and three instances passed 37 million distinct states without finishing, so the nightly bounds keep two keys and add the third instance and generation.

## What the counterexamples show

- **`ShardMap` / `UnsafeConfigMap`.** Publish generation 1, which moves `k1` from `s2` to `s1`, then write the ConfigMap at once. Now generation 0 (still loaded by both instances) and generation 1 (in the ConfigMap, so loaded by any instance that restarts) both route `k1` to an unfenced shard: two writers. The safe order fences `s2` and catches `s1` up first.
- **`ReshardCutover` / `NoFence`.** The cutover finishes on both instances, then the designated instance is partitioned and the saga rolls back: the reachable instance returns to the source while the partitioned one still writes to the destination. Without the fence nothing stops either. With the fence the same schedule is safe, because `RollBack` fences the destination and the forward path fences the source.

## Mutation checks

Run by hand when an invariant or guard changes, to confirm the invariants are not vacuous (each removes one guard from the safe spec):

| Mutation | Result |
|---|---|
| `ShardMap`: `Unfence` once the ConfigMap is current, before every instance reloads | `SingleWriter` violated |
| `ShardMap`: `WriteConfigMap` without the catch-up check | `ConfigMapSafe` violated |
| `ShardMap`: `CopyKey` without its fence guard | no violation: `WriteConfigMap` re-checks the fence and the catch-up, so the guard is redundant (kept, and commented) |

## Modelling choices

- **Shards do not know ownership.** A Postgres or WeSQL shard accepts any write that reaches it unless fenced (`ALTER ROLE … NOLOGIN`). The fence is the only guard; the specs check that the protocol's order makes it enough.
- **Catch-up is a predicate, not a flag.** `CaughtUp(s)` is "s holds every acknowledged write for the key", computed from the data, so a write accepted after a copy un-catches the shard automatically.
- **Writes are upserts with unique ids**, so stream redelivery is a set union and `NoDuplicateEffect` holds by construction; it is stated so a later layout change (per-row versions) must keep it.
- **`ReshardCutover` collapses each cluster to one store per key**, since §31 §6.4's argument is per key.
- **Strong fairness on the saga.** A designated instance whose partition flaps keeps `CutOverDesignated` only intermittently enabled; under weak fairness TLC finds a run that never finishes. The saga needs the network to be stable often enough, which strong fairness expresses.
- **Apalache runs bounded checks** (`--length` 8 and 10) in RT0. Proving `SingleWriter` inductive with an `IndInv` is deferred to RT1 (ruling E2 in the RT0 plan).

## Action-to-code map

Every action is emitted as a `SpecEvent` by the code that performs it (§31 §11.3); RT1's trace validation checks recorded runs against these specs. Code names are the RT1 targets.

### `ShardMap`

| TLA+ action | Code (RT1) | Event |
|---|---|---|
| `Publish(m)` | `loams_sqlrouter::push::ConfigPush` → `Output::Cas` | `map.publish {db, gen, version}` |
| `Fence(s)`, `Unfence(s)` | `Cutover` → `Output::Fence`; `PostgresShard::fence_writes` | `shard.fence {shard, on}` |
| `CopyKey(k)` | PgDog's copy and stream, observed by `PgDogModel` | `shard.caught_up {shard, key}` |
| `WriteConfigMap` | `ConfigPush` → `Output::WriteConfigMap`; `ConfigMapSink` | `map.configmap {db, gen}` |
| `Reload(i)` | `ConfigPush` → `Output::Reload`; `PgDogInstance::apply` | `fleet.reload {instance, gen, ok}` |
| `Restart(i)` | simulation: `PgDogModel::restart`; real: a changed process start time in `observe` | `fleet.restart {instance}` |
| `ClientWrite(i, k)`, `Reject(i, k)` | workload clients in the simulator and the nemesis | `client.write {key, instance, shard, result}` |

### `ReshardCutover`

| TLA+ action | Code (RT1–RT4) | Event |
|---|---|---|
| `StartCopy`, `PauseAll`, `FenceSource`, `CutOverDesignated`, `PublishAndReload`, `ResumeAll`, `Finalize` | `loams_sqlrouter::cutover::Cutover`, one step each | `cutover.step {db, step}` |
| `RollBack`, `RolledBack` | `Cutover` reverse path | `cutover.step {db, step}` |
| `StreamForward`, `StreamReverse` | PgDog's logical replication, observed | `stream.apply {dir, write}` |
| `ClientWrite(i, k)` | workload clients | `client.write {key, instance, store, result}` |
| `Partition(i)`, `Heal(i)` | the simulator's network and the nemesis | `net.partition {instance, on}` |
| `SagaCrash`, `SagaRestart` | the nemesis; the durable saga's restart | `saga.crash {}`, `saga.restart {step}` |

The skeletons' headers carry their own tables, with `code = "RT2"` or `"RT4"` placeholders.
