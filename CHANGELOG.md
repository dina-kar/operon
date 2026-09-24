# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- Design documents (`docs/design`) and repository governance files.
- Design: durable execution through the Resonate protocol (§14); `arrow` segment encoding and changelog streams, adapted from Apache Fluss (§02).
- Cargo workspace, CI (fmt, clippy, tests, cargo-deny license policy).
- `operon-store`: object storage access with create-only and compare-and-swap writes, range reads, S3/GCS/Azure/local/in-memory backends from URLs, and `FaultyStore` fault injection.
- `operon-cache`: read-through RAM + NVMe byte-range cache with per-block crc32c verification.
- `operon-common`: namespace and stream id types.
- `operon-meta`: the metastore (namespaces, streams, the stream sequencer and offset index, leases with epochs, fenced pointer compare-and-swap), replicated with openraft, with the Raft log in a local redb database, snapshots in object storage, and in-process multi-node clusters.
- `operon-meta`: segment swaps (`SwapSegment`, lease-fenced), partition trimming and log start offsets (`TrimPartition`), per-stream retention (`SetRetention`), a 15-minute WAL commit window with pruning of commit records (`PruneWalCommits`), deterministic tracking of live WAL chunks and retired objects (`ForgetObjects`), an applied-index watch (`MetaNode::watch_applied`), and `MetaClient`, which follows the leader and retries.
- `operon-log`: the internal log on the `standard` WAL class: Kafka `RecordBatch` v2 records, the WAL object and segment formats (version 1, with the `encoding` field reserved), the leaderless write path (`LogWriter`), fetch with long-poll (`LogReader`), and the segmenter and retention loops.
- `operon` binary: `operon dev` and `operon standalone` run the metastore, the log and a native HTTP/JSON produce/fetch API in one process.
- `operon-worker`: the lease-based task framework: task sources, priorities, round-robin fair share across namespaces, per-task leases `task/<key>` renewed every third of their TTL and re-taken at the same epoch after a late renewal, fences and cancellation, and `run_once` for one deterministic pass.
- `operon-link`: the link framework with exactly-once apply (`LinkTarget`, `LinkApplySource`, one task per link) and the `CounterTable` test target, whose commits carry data and applied offsets under a fenced pointer CAS; `LinkGcRoots` for garbage collection.
- `operon-pk`: `PkIndex`, a fenced single-writer SlateDB per keyed object, and the read-only `PkReader`.
- `operon-log`: garbage collection (`gc::GcSource`) of retired objects, never-committed WAL objects, unreferenced segments and unreachable link objects, after a grace period.
- `operon-meta`: the link catalog (`CreateLink`), `ReacquireLease`, fences on `TrimPartition`, `PruneWalCommits` and `ForgetObjects`, `MetaState::check_invariants`, and `MetaNode::fatal_error`.
- `operon`: link endpoints (`POST /v1/namespaces/{ns}/links`, `GET /v1/namespaces/{ns}/links/{link}`); the server runs link apply and garbage collection; with the `failpoints` feature, `OPERON_FAILPOINTS` / `OPERON_FAILPOINT_HIT` abort the process at named failpoints.
- `operon-sim` (test-only): a seeded in-process cluster simulation and a Wing–Gong–Lowe linearizability checker.
- M0 exit gates: the kill -9 crash gate (`crates/operon/tests/crash.rs`), the object-store fault matrix (`crates/operon/tests/fault_matrix.rs`) and the simulation seed sweep, all in CI.

### Changed
- `operon-log` (M0.4): the segmenter and retention run as worker tasks (`SegmenterSource`, `RetentionSource`); their background loops and `BackgroundTask` are gone. The segmenter deletes a segment it wrote only when its swap was definitely not applied. `SegmenterConfig` loses `interval` and gains `swap_deadline`.
- `operon-meta` (M0.4): the snapshot envelope is format version 4 (link catalog, incremental byte counts); `PartitionState::bytes` is O(1); `max_clock_skew` defaults to 5 min; `MetaNode::shutdown` waits for the database file to close; a snapshot upload is bounded by a total deadline (`MetaConfig::snapshot_io_budget`).
- `operon-store` (M0.4): `ObjectInfo` carries the modification time; `FaultyStore` classifies PUTs by mode, adds `Precondition` and `Delay` faults, random seeded faults (`FaultyStore::random`) and `inject_nth`.
- `operon` (M0.4): `405 Method Not Allowed` uses the JSON error body.
- `operon-meta` (M0.4 review): `SwapSegment` carries the segment's `Freshness`, and `CasPointer` an optional one. The metastore refuses a reference to an object older than its deadline with `ApplyError::StaleObject`. GC ages objects by the metastore clock. `CounterTable` manifests are named `<version:020>-<ulid>.man`, and orphaned manifests are no longer adopted.
- `operon-store`: `file://` stores fsync each written file and its directory before a write returns, so a write that returned survives power loss.
- `operon-meta`: the snapshot envelope is format version 2; version-1 snapshots are rejected. `CommitWal` carries the WAL object's creation time.
- `operon-meta`: the leader refuses commands stamped more than `MetaConfig::max_clock_skew` (default 60 s) ahead of its own clock (`MetaError::ClockSkew`). `CreateStream` carries the stream's retention. `MetaClient::inject_lost_ack` needs the `test-util` feature.
- `operon-log`: a commit rejected after an attempt whose outcome was unknown fails its appends with `CommitUnknown` (they may be committed). `LogWriter::start` validates its config and returns a `Result`. Size retention keeps each partition's newest entry. The segmenter renews its lease before swapping and never deletes a segment the metastore references or retired.
- `operon`: every error, including malformed paths and queries and bodies over 16 MiB (413), uses the JSON error body; fetch `max_bytes` is capped at 16 MiB.
- `operon-meta`: a node with no local state refuses to start when the snapshot store already holds metastore snapshots, since its data directory was most likely lost (override with `MetaConfig::allow_fresh_start_with_existing_snapshots`). Transient object-storage failures while writing or reading a snapshot are retried instead of stopping the node.
