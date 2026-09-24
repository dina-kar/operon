# Implementation Plans

Each plan is a sequence of test-driven tasks that ends in working, tested software. Plans are written just before they're executed, so they match the real code that exists by then. The M0.1 and M0.2 plans contain code compiled and tested in a scratch copy before publication. From M0.3 on, plans specify the exact formats, interfaces, rulings and required tests; the code is written test-first during execution and checked by an independent whole-branch review (M0.3 plan, Ruling 1).

Execute a plan task by task with `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans`.

## Milestone M0: Foundation

Design references: [01 Architecture](../design/01-architecture.md), [02 Stream engine](../design/02-stream-engine.md), [03 Storage formats](../design/03-storage-formats.md), [04 Hot tier](../design/04-hot-tier.md), [09 Links & workers](../design/09-links-and-workers.md), [12 Roadmap](../design/12-roadmap-testing-risks.md).

| Plan | Scope | Status |
|---|---|---|
| [M0.1: Workspace & storage primitives](2026-09-23-m0.1-storage-primitives.md) | Cargo workspace, CI, license policy; `operon-store` (conditional writes, range reads, URL backends, fault injection); `operon-cache` (RAM + NVMe range cache with checksums) | **Done** |
| [M0.2: Metastore](2026-09-23-m0.2-metastore.md) | `operon-common` ids; `operon-meta` on openraft: namespaces, streams, sequencer and offset index, leases with epochs, fenced manifest-pointer CAS; Raft log in redb, snapshots in object storage; single-node and in-process 3-node clusters | **Done** |
| [M0.3: Log engine](2026-09-24-m0.3-log-engine.md) | WAL object and segment formats (with the `encoding` field reserved, D20); leaderless `standard` write path; fetch path; segmenter; native produce/fetch API; `operon dev` binary. In meta: segment index swap, WAL-commit pruning, retention trim, offset watch for long-poll | **Done** |
| M0.4: Workers, links & M0 gates | Worker leases and task framework; link framework with exactly-once apply; `PkIndex` on SlateDB; GC; deterministic simulation harness; M0 crash and fault exit gates | **Next** |

## Later milestones

Plans for M1 (collections), M2 (graph and the Resonate durable-execution surface), M3 (Kafka and changelog streams), M4 (analytics) and M5 (scale) will be written once M0 is done. Their scope and exit gates are in [12-roadmap-testing-risks.md](../design/12-roadmap-testing-risks.md).
