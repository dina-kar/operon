# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- Design documents (`docs/design`) and repository governance files.
- Cargo workspace, CI (fmt, clippy, tests, cargo-deny license policy).
- `operon-store`: object storage access with create-only and compare-and-swap writes, range reads, S3/GCS/Azure/local/in-memory backends from URLs, and `FaultyStore` fault injection.
- `operon-cache`: read-through RAM + NVMe byte-range cache with per-block crc32c verification.
