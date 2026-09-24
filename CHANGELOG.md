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

### Changed
- `operon-store`: `file://` stores fsync each written file and its directory before a write returns, so a write that returned survives power loss.
- `operon-meta`: the snapshot envelope is format version 2; version-1 snapshots are rejected. `CommitWal` carries the WAL object's creation time.
- `operon-meta`: the leader refuses commands stamped more than `MetaConfig::max_clock_skew` (default 60 s) ahead of its own clock (`MetaError::ClockSkew`). `CreateStream` carries the stream's retention. `MetaClient::inject_lost_ack` needs the `test-util` feature.
- `operon-log`: a commit rejected after an attempt whose outcome was unknown fails its appends with `CommitUnknown` (they may be committed). `LogWriter::start` validates its config and returns a `Result`. Size retention keeps each partition's newest entry. The segmenter renews its lease before swapping and never deletes a segment the metastore references or retired.
- `operon`: every error, including malformed paths and queries and bodies over 16 MiB (413), uses the JSON error body; fetch `max_bytes` is capped at 16 MiB.
- `operon-meta`: a node with no local state refuses to start when the snapshot store already holds metastore snapshots, since its data directory was most likely lost (override with `MetaConfig::allow_fresh_start_with_existing_snapshots`). Transient object-storage failures while writing or reading a snapshot are retried instead of stopping the node.
