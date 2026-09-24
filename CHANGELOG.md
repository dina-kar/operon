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

### Changed
- `operon-store`: `file://` stores fsync each written file and its directory before a write returns, so a write that returned survives power loss.
- `operon-meta`: a node with no local state refuses to start when the snapshot store already holds metastore snapshots, since its data directory was most likely lost (override with `MetaConfig::allow_fresh_start_with_existing_snapshots`). Transient object-storage failures while writing or reading a snapshot are retried instead of stopping the node.
