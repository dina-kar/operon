# Loam WAL vs safekeepers: the P4b benchmark

The harness behind the merge gate in [§28 §7](../../docs/design/28-loam-postgres.md). It runs
pgbench through a Neon compute whose WAL goes either to **stock safekeepers** (the baseline)
or to **Loam's WAL service on TiKV** (the candidate), on the same host and topology. The Loam
WAL replaces the safekeepers only if, for every workload:

- its p99 commit latency is at or below the safekeepers' (within the baseline's run-to-run
  noise), and
- its throughput is no worse.

Derived from [`deploy/neon`](../neon) (Apache-2.0, from `neondatabase/neon` `docker-compose/`).

## Topology

| Tier | Baseline (`--variant safekeepers`) | Candidate (`--variant loam`) |
|---|---|---|
| Compute | `compute-node-v16`, `shared_buffers = 2GB`, one per run on a fresh timeline | same |
| WAL | `safekeeper1` (`--replicas 1`) or `safekeeper1..3` (`--replicas 3`), fsync on, each on its own volume | `loam-wal --store tikv` + a TiKV playground with 1 or 3 stores ([`tikv.toml`](tikv.toml)) |
| Pageserver feed | the safekeepers | `feeder-safekeeper`: a stock safekeeper with `--no-sync` that `loam-wal` streams committed WAL to, off the commit path |
| Storage | pageserver, storage broker, RustFS | same |
| Client | pgbench inside the compute container | same |

`loam-wal` refuses to listen beyond loopback unless it has `--auth-token` and `--trusted-network`
(it has no TLS yet, so the token is cleartext; the benchmark uses
loopback only). Everything uses host networking, so the compute reaches containers and host processes the
same way.

The **feeder** exists because the pageserver only ingests Neon's *interpreted* WAL protocol,
which the WAL service does not speak yet (§28 Q112; see `crates/operon-safekeeper/src/feeder.rs`).
The feeder safekeeper stands in for that decoder. It adds disk and CPU work to the candidate
that the final design does not have, so it can only make the candidate look worse.

## Run

Build `loam-wal` once, with podman's socket (or Docker) and tiup available (see
[`scripts/tikv`](../../scripts/tikv)):

```sh
cargo build --release -p operon-safekeeper --features server,tikv --bin loam-wal
# One run of one variant: writes bench/results/<date>-<sha>-<variant>-rf<n>-<label>.json
scripts/loam-pg-bench/run.sh --variant safekeepers --replicas 3
scripts/loam-pg-bench/run.sh --variant loam --replicas 3
# The gate: baseline and candidate interleaved three times, then the comparison.
scripts/loam-pg-bench/gate.sh --replicas 3 --repeats 3 --duration 300 --warmup 60 \
  --scale 50 --workloads "commit-1 commit-16 tpcb-16 tpcb-64 bulk"
```

`run.sh` waits while `cargo` or `rustc` runs on the host (pass `--force` to skip this), because
a build ruins p99s. The manual workflow [`loam-pg-bench.yml`](../../.github/workflows/loam-pg-bench.yml)
runs the gate on a dedicated self-hosted runner. Shared CI runners are too noisy for p99s.

## Workloads

These are defined in [`scripts/loam-pg-bench/workload.sh`](../../scripts/loam-pg-bench/workload.sh).
Each one has a warm-up, then `pgbench -l` per-transaction logs. Percentiles come from those
logs, not from pgbench's averages.

| Name | What | Why |
|---|---|---|
| `commit-1` | One single-row `INSERT` per transaction, 1 client | The pure commit round trip |
| `commit-16` | The same, 16 clients | Group commit |
| `tpcb-16` / `tpcb-64` | Built-in TPC-B at `--scale` | A realistic OLTP mix; saturation |
| `bulk` | One transaction inserting about 250 MB | WAL throughput (MB/s) |

## Results

Each run writes one JSON file with:

- topology and versions;
- settings;
- per workload: TPS and p50, p90, p99, p99.9 and max.

`scripts/loam-pg-bench/compare.py --baseline … --candidate …` prints the gate table. The
single-host results in [`bench/results`](../../bench/results) are laptop data points, not the
gate. The gate needs server hardware:

- NVMe with power-loss protection;
- the three-AZ topology of §7, with `tc netem` delays or real zones.

## Not modelled yet

- **Cross-AZ delays.** Both variants run with zero injected delay.
- **The fault run.** This means killing a TiKV leader, or a safekeeper for the baseline, at
  minute 2.
- **TiKV leader placement.**

## P4b results, raw TiKV store (2026-10-01)

Three interleaved repeats per configuration (baseline, raw depth 1, 8, 32, then
the transactional store, in that order each round), on an exclusive host, for 1
and 3 TiKV stores (3 stores: leaders pinned to the compute's zone with
`place-leaders.sh`). 60 s per workload after 10 s warm-up, scale 10, compute
fsync off. Raw JSON is in `bench/results/2026-10-01-raw/`.

Each cell is p99 ms, median (min-max) over the 3 repeats, then median TPS.
The run-to-run spread is the noise estimate: for the baseline's commit-1 p99 it
is up to 40% at rf1 and 25% at rf3; commit-16 and tpcb-16 up to 70% / 15% at
rf1 and rf3, and a single rf3 baseline commit-16 outlier of 398 ms. Differences
under those bands are not results.

| run (p99 ms median (min-max) / TPS) | safekeepers | Loam txn | raw d1 | raw d8 | raw d32 |
|---|---|---|---|---|---|
| rf1 commit-1 | 16.6 (16.5-23.6) / 197 | 45.6 (36.0-73.9) / 117 | 46.2 (24.1-48.9) / 138 | 44.0 (36.1-44.3) / 120 | 44.6 (40.3-45.1) / 133 |
| rf1 commit-16 | 31.2 (22.0-38.8) / 2129 | 69.7 (68.8-124.8) / 731 | 72.3 (44.9-75.0) / 919 | 40.7 (33.4-49.7) / 1361 | 57.6 (44.9-59.5) / 1388 |
| rf1 tpcb-16 | 116.0 (90.9-116.8) / 851 | 184.1 (175.6-337.5) / 465 | 193.8 (184.7-195.9) / 483 | 134.7 (122.5-144.5) / 543 | 128.3 (111.5-129.0) / 599 |
| rf1 bulk MB/s | 86.6 (45.2-101.2) | 20.7 (19.6-24.4) | 34.2 (27.4-68.7) | 49.6 (38.7-72.1) | 35.4 (34.0-35.6) |
| rf3 commit-1 | 39.7 (36.1-45.3) / 111 | 48.6 (48.3-64.8) / 76 | 54.6 (34.4-61.5) / 88 | 49.0 (48.1-66.5) / 71 | 47.4 (36.1-71.3) / 74 |
| rf3 commit-16 | 77.5 (65.5-398.0) / 767 | 74.2 (71.8-80.2) / 606 | 73.8 (56.1-77.0) / 746 | 77.9 (75.5-96.5) / 569 | 86.6 (77.7-102.5) / 592 |
| rf3 tpcb-16 | 213.7 (185.5-214.0) / 434 | 297.0 (292.8-358.6) / 263 | 243.3 (238.4-299.8) / 322 | 302.4 (223.2-313.3) / 291 | 271.2 (238.0-293.6) / 344 |
| rf3 bulk MB/s | 44.3 (32.8-57.0) | 9.3 (8.2-11.5) | 18.1 (9.0-18.1) | 11.4 (10.0-15.0) | 13.7 (10.0-14.8) |

Verdict: **the gate fails** in every configuration; Loam is behind the stock
safekeepers on commit-1 p99 by 2.7x at rf1 (44 vs 17 ms) and about 1.25x at rf3.
What the raw store and pipelining do buy, outside the noise at rf1: commit-16
p99 70 -> 41 ms and TPS 731 -> 1361 (txn -> raw d8), tpcb-16 TPS 465 -> 543-599,
bulk 21 -> 50 MB/s. At rf3 the raw store is within noise of the transactional
store on most rows (rf3 bulk and tpcb-16 improve, 9 -> 11-18 MB/s and 263 ->
291-344 TPS) and the 3-replica baseline is itself slow (p99 40 ms, noisy).
Depth 8 and 32 are indistinguishable (32 is not better), depth 1 loses the
concurrent workloads, so the default stays 8.

commit-1 and the Raft fsync: this host's NVMe (Samsung BM9C1a, no PLP) does an
8 KiB write + fdatasync in p50 2.65 ms, p90 9.9 ms, p99 48 ms, max 395 ms
(measured idle). commit-1 p50 is about 6 ms and its p99 about 44 ms, in line
with one fsync per commit plus the gRPC and Raft hops, so single-commit latency
is mostly bound by the fsync (and its tail), not by the extra read or TSO the
transactional store had (raw is no faster on commit-1). It is not purely the
fsync: the safekeepers fsync the same disk and keep a 17 ms p99, so TiKV's
extra work (raft-engine plus apply, region leader hop) roughly doubles the tail.
Only a PLP NVMe, or the local-NVMe WAL of Arm A, can remove that floor.

Caveats: one laptop (14 CPUs, 16 GB, one shared consumer SSD), Neon and TiKV
and loam-wal all on it; n=3; the baseline's own spread is large; compute fsync
off; one TiKV store at rf1 (no replication).

Bug found by the gate: with a backlog (pgbench -i at depth 32) the feeder read
8 MiB scan pages, over tonic's 4 MiB decode limit, and reconnected forever, so
the compute stalled (the 25-minute hang). Fixed in #169, not a lost or unacked
pipelined append. `run.sh` now bounds each workload with a timeout.
