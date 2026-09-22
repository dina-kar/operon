# Implementation Plans

Each plan is a sequence of test-driven tasks that ends in working, tested software. Plans are written just before they're executed, so their code matches the real code that exists by then. Code in a plan has been compiled and tested in a scratch copy before the plan is published.

Execute a plan task by task with `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans`.

## Milestone M0: Foundation

Design references: [01 Architecture](../design/01-architecture.md), [02 Stream engine](../design/02-stream-engine.md), [03 Storage formats](../design/03-storage-formats.md), [04 Hot tier](../design/04-hot-tier.md), [09 Links & workers](../design/09-links-and-workers.md), [12 Roadmap](../design/12-roadmap-testing-risks.md).

| Plan | Scope | Status |
|---|---|---|
| [M0.1: Workspace & storage primitives](2026-09-23-m0.1-storage-primitives.md) | Cargo workspace, CI, license policy; `operon-store` (conditional writes, range reads, URL backends, fault injection); `operon-cache` (RAM + NVMe range cache with checksums) | **Ready** |
| M0.2: Metastore | `operon-common` ids; `operon-meta` on openraft: namespaces, streams, sequencer, leases, manifest-pointer CAS; single-node and 3-node in-process clusters; snapshots to object storage | Next, written after M0.1 lands |
| M0.3: Log engine | WAL object and segment formats; leaderless `standard` write path; fetch path; segmenter; native produce/fetch API; `operon dev` binary | Planned |
| M0.4: Workers, links & M0 gates | Worker leases and task framework; link framework with exactly-once apply; `PkIndex` on SlateDB; GC; deterministic simulation harness; M0 crash and fault exit gates | Planned |

## Later milestones

Plans for M1 (collections), M2 (graph), M3 (Kafka), M4 (analytics) and M5 (scale) will be written once M0 is done. Their scope and exit gates are in [12-roadmap-testing-risks.md](../design/12-roadmap-testing-risks.md).
