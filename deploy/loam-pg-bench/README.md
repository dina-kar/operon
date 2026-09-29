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

`loam-wal` refuses to listen beyond loopback unless it has `--auth-token` (the benchmark uses
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
