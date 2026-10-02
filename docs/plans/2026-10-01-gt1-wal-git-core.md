# GT1 — The WAL Git Core and `git-remote-loams` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, formats, constants, error messages), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: Planned** (2026-10-01). Design: [§36](../design/36-loams-git.md) (D388–D399). **Slot: §15's W1, after M3** (Q395, answered 2026-10-02: D411); track GT then interleaves on the one-build machine beside the other tracks. Branches `gt1-t<N>`, stacked; PRs target `main`. GT1 adds two crates and one proto package; it changes no existing code path except one small, additive `loams-store` method if Task 0 finds it missing.

**Goal:** Ship §36's bucket-native Git core and a serverless remote helper:
- `loams-git`: the `loams.git.v1` formats (segments, checkpoints, `.lpk` pack objects), `BlobStore`, `WalStore`, a pack-cache `Odb`, the ref state machine, `BucketRefLog` (the per-repository sequencer with group commit, fencing, unknown-outcome resolution and idempotency), checkpoints, forks and segment GC;
- `git-remote-loams` (crate `loams-git-remote`): stock git clones, fetches and pushes through `loams::<store-url>` addresses straight to the bucket, with no server;
- the GT1 gates: fault runs, linearizable `RefLog` histories, concurrent pushers through stock git, and the first pushes/s and latency numbers per store.

**Architecture:**
- **One core, two callers.** `BucketRefLog` is the sequencer of §36 §4.4. In GT1 its callers are the helper (each `git push` process is its own short-lived sequencer) and the tests (many sequencers on one store, the worst case). GT2 adds the gateway; a commercial Cloudflare target (`loam-platform`) may add a Durable Object. Every caller is fenced by the create-only segment PUT (§36 §4.2), so none needs a lease for correctness.
- **Storage through `loams-store`.** `StoreBlobStore` and `StoreWalStore` take a `loams_store::Store` (already a `PrefixStore` when opened from a URL with a prefix) and a `RepoPaths`. Fault injection is `loams_store::FaultyStore`, unchanged.
- **gitoxide only behind `Odb` and `ids`.** `gix-hash` (object ids), `gix-validate` (ref names), `gix-pack` (reading packs and indexes in tests and in the pack cache). Pack writing in the helper uses the `git` binary (`pack-objects`, `index-pack`), as every remote helper does.
- **CloudEvents through `loams-cloudevents`.** Events are `loams_cloudevents::CloudEvent`; the protobuf format comes from where PR #171 (`ProduceCloudEvents`, D270) put it, or a vendored `io.cloudevents.v1` proto if it has not merged (Task 0).

**Tech Stack:**
- Rust 1.97.1, edition 2024, workspace lints.
- New dependencies (Task 0 checks versions, licences and `cargo deny`): `gix-hash`, `gix-validate`, `gix-pack` (and the `gix-features`/`gix-object` versions they pull) from the `gix` 0.88.0 release train (2026-09-25; MIT OR Apache-2.0), with `default-features = false` and only the `sha1` feature; `crc32c` (already used by `loams-log`), `prost` 0.14 and `prost-build` 0.14 (workspace), `sha2` (workspace, if present; else Task 0 adds it, MIT OR Apache-2.0).
- Reused: `loams-store`, `loams-cloudevents`, `loams-meta-conformance` (dev: its `linearizability` checker), `tokio`, `bytes`, `futures`, `async-trait`, `thiserror`, `tracing`, `proptest`, `rand`, `rand_chacha`, `tempfile`.
- System: `git` ≥ 2.45 on the build machine and CI (the helper protocol and `pack-objects --stdin-packs` behaviour used by Task 9; Task 0 records the versions), system `protoc` as for M1, Docker or Podman for RustFS in Task 10 (`rustfs/rustfs:1.0.x`, D61).

**Spec:**
- [`docs/design/36-loams-git.md`](../design/36-loams-git.md): §4 (the WAL), §5.1–§5.3 (the traits), §6.2 (the helper), §6.3 (steps 1–5 as the helper uses them), §7 (checkpoints and segment GC; repack is GT2), §12 (cost), §14 (risks).
- [`docs/design/15-agent-workspaces.md`](../design/15-agent-workspaces.md) §3 (amended by D388–D390), §11.
- [`docs/design/02-stream-engine.md`](../design/02-stream-engine.md) §7.4 (D270, the CloudEvents mapping), [`docs/design/03-storage-formats.md`](../design/03-storage-formats.md) §6–§7.
- As built: `crates/loams-store/src/{store.rs,fault.rs,error.rs}`, `crates/loams-cloudevents/src/lib.rs`, `crates/loams-meta-conformance/src/linearizability.rs`, `crates/loams-log/src/wal.rs` (framing conventions).

## Global Constraints

Same as the M1 overview §8, plus:
- **Docs and code stay in step.** Any constant this plan names (`max_group_txns` 64, `max_segment_bytes` 1 MiB, `idempotency_window` 1 h, `checkpoint_every_segments` 256, `checkpoint_every_bytes` 8 MiB, `max_refs_per_txn` 4096, `exactly_retention` 24 h, `gc_grace` 1 h) lives in one `loams_git::limits` module with a doc comment pointing at §36.
- **No server, no listener.** GT1 opens no port. The helper is a process git starts.
- **Object storage is the source of truth.** No state outside the bucket is read for correctness. Local caches (`Odb`'s pack cache) are keyed by content and may be deleted at any time.
- **Never link copyleft code.** The `git` binary is run as a process by the helper and by tests only (D394, D396); `git2`/libgit2 is not a dependency of any crate.
- **The build machine.** One cargo build at a time, the shared target directory, `-j 6`, lld. RustFS runs only between builds. Stop and report if `/home` has under 8 GB free.
- **Commit areas:** `git`, `store`, `proto`, `ci`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **Content-named blobs over 64 MiB are uploaded with an unconditional multipart PUT**; under 64 MiB, `put_if_absent`. **On RustFS, blobs over 64 MiB are supported only once Q385 confirms atomic multipart completion**; until then `StoreBlobStore` refuses them there with `BlobError::TooLarge` (a 64 MiB push limit on RustFS), and Task 3 records the finding | Two writers of one content-derived name write identical bytes, so overwriting is harmless, and a conditional multipart completion is not available on every `object_store` backend; S3, R2, GCS and Azure document atomic completion | A lower push-size limit on RustFS until Q385 is answered |
| 2 | **The helper is its own sequencer** (a `BucketRefLog` per `git push` process), fenced by the segment PUT | GT1 has no server; the protocol is safe for any number of writers | Many direct pushers to one hot repository retry on 412 a lot; GT2's server path group-commits them |
| 3 | **The helper uses `fetch` and `push` capabilities, not `stateless-connect`** | `stateless-connect` needs a v2 upload-pack, which is GT2 | No partial clone in GT1; documented |
| 4 | **Fetch downloads whole packs.** The helper keeps the set of pack checksums it has in `.git/loams/<remote>/packs` and downloads every live pack it lacks; each `.lpk` is split into `pack-<checksum>.pack` and `.idx` in `.git/objects/pack/`, with no `index-pack` run | Packs are immutable and content-named, and the stored idx is the pack's own idx (verified at push) | Over-fetching after repacks (GT2's compaction rewrites packs); acceptable for GT1's agent-sized repositories, and GT2's v2 fetch replaces it for large ones |
| 5 | **Fast-forward checks happen in the pushing process** (`git merge-base --is-ancestor <old> <new>` in the helper) | The helper has the full local object graph; the sequencer checks only object ids (§36 §4.4) | A modified helper can force-push without `+`. Protections (Task 6) are checked by every sequencer, the helper's included, but a modified helper with bucket credentials can skip both checks: direct bucket writers are trusted with their credentials in GT1, and GT2's server path enforces both for everyone else |
| 6 | **Idempotency keys for helper pushes** are `sha256(principal ‖ pack checksum ‖ sorted updates)` (§36 §4.5). `principal` is the authenticated credential subject: the access key id of the bucket credentials the helper uses (or a vended token's subject), the same value in the WAL event and the key. `git config user.email` is used only when `LOAMS_GIT_UNAUTHENTICATED_TESTS=1` (local tests on `file://` and `memory://` stores) | git retries a push only by the user re-running it; the derived key makes a re-run after a lost answer a replay. A caller-chosen email would let two credential holders share or spoof one audit and idempotency identity | None beyond §36 §4.5 |
| 7 | **The repository is created by its first push** (checkpoint 0 with no refs, then segment 1), or explicitly by `loams_git::create_repo`; a clone of a missing repository fails with `fatal: repository '<url>' not found` | Mirrors git hosting behaviour without a control plane | A typo in a URL creates a repository on push; GT2's server requires an explicit create |
| 8 | **SHA-1 only in GT1.** The object format is a field in every message and checkpoint; a SHA-256 repository is refused with `object format sha256 is not supported yet (Q388)` | gitoxide's SHA-256 parity is an open item | A format change later is additive |

## Carried in

From §36: Q384 (latency per store) is measured in Task 10; Q385 (RustFS above 1 MiB) is checked in Task 3; Q393 (pack bytes inside the WAL) is decided by Task 10's numbers.

## Review Focus

1. **No acknowledged push is lost, and none is applied twice.** Tests: Task 4 (`append_is_exclusive_across_writers`, `retry_after_lost_ack_is_committed`), Task 7 (`lost_ack_resolves_to_committed`, `fenced_group_revalidates`, the linearizability histories), Task 9 (`concurrent_pushes_never_lose_an_acked_push`).
2. **Fencing holds with many sequencers.** Tests: Task 7 (`two_sequencers_one_store_linearizable`).
3. **Formats are exact and versioned.** Tests: Task 2 (golden bytes, `corrupt_crc_is_error`, `n_minus_one_is_read`).
4. **Readers never see a partial state.** Tests: Task 7 (`snapshot_is_a_prefix_of_commits`), Task 8 (`checkpoint_plus_replay_equals_state`).
5. **Idempotency window.** Tests: Task 6 (`key_reuse_with_other_digest_is_mismatch`, `replay_inside_window_returns_receipt`, `after_window_old_oid_decides`).

## File structure

```
Cargo.toml / Cargo.lock                      # gix-hash, gix-validate, gix-pack (sha1), members unchanged (crates/*)
deny.toml                                    # only if Task 0 needs an exception
proto/loams/git/v1/wal.proto                  # §36 §4.3, verbatim
proto/io/cloudevents/v1/cloudevents.proto    # only if PR #171 has not merged (Task 0)
crates/loams-git/
  Cargo.toml  build.rs                       # prost-build over proto/loams/git/v1
  src/{lib.rs,limits.rs,ids.rs,paths.rs,error.rs,
       format/{mod.rs,segment.rs,checkpoint.rs,lpk.rs,event.rs},
       blob.rs,wal.rs,odb.rs,state.rs,reflog.rs,sequencer.rs,checkpointer.rs,fork.rs,gc.rs,testing.rs}
  tests/{format.rs,blob.rs,wal.rs,state.rs,reflog.rs,linearizable.rs,checkpoint.rs,fork.rs,gc.rs}
  tests/golden/{segment_v1.lgw,checkpoint_v1.lgc,lpk_v1_footer.bin}
crates/loams-git-remote/
  Cargo.toml                                 # [[bin]] name = "git-remote-loams"
  src/{main.rs,protocol.rs,url.rs,fetch.rs,push.rs,local.rs}
  tests/{helper.rs,concurrent.rs,faults.rs}
crates/loams-git/benches/ or bench/git-wal/ # Task 10 (whichever matches bench/ conventions on main)
scripts/git/{rustfs.sh,bench.sh}
.github/workflows/ci.yml                     # job git (path-filtered)
docs/design/36-loams-git.md  CHANGELOG.md
```

### Task 0: Reconcile and check the facts

**Files:** read `crates/loams-store/src/*`, `crates/loams-cloudevents/src/*`, `crates/loams-meta-conformance/src/linearizability.rs`, `crates/loams-log/src/wal.rs`, `bench/` and `.github/workflows/ci.yml` as on `main`. Fill this plan's "Rulings made during execution" table.

**Checks** (record each result with its command):
1. `loams_store::Store` has `put_if_absent`, `put_if_match`, `get`, `get_range`, `put`, `list` (with an offset or prefix), `delete` and a multipart put. Missing ones become a small additive PR to `loams-store` first (commit area `store`), with tests in its own suite.
2. `FaultyStore` has `Fault::ErrorAfterApply` (lost acknowledgement) for `PutCreate` (it does on `main` as of 2026-10-01: `crates/loams-store/src/fault.rs`). Record how a test targets one path (rules by path prefix or op).
3. Where the CloudEvents protobuf format lives after PR #171 (`crates/loams-stream-grpc/src/events.rs` and `proto/loams/stream/v1/stream.proto` on its branch). If merged, `loams-git` depends on the proto definitions it exposes (or on `loams-cloudevents` if the codec moved there); if not, vendor `io/cloudevents/v1/cloudevents.proto` from cloudevents/spec v1.0.2 (Apache-2.0, noted in `NOTICE`) and record that the two must be merged when #171 lands.
4. The `gix-*` crate versions of the `gix` 0.88.0 train, their features with `default-features = false`, `cargo deny check` and `cargo tree -d` deltas, and the cold build time of `loams-git` alone (one measured build).
5. `git --version` locally and on the CI image; whether `git index-pack --stdin --fix-thin` and `git pack-objects --revs --stdout --thin` behave as Task 9 uses them.
6. Whether RustFS runs in CI already (D61) and the image tag; otherwise Task 10 adds a path-filtered service container.
7. The decision numbers on `main` (D388–D399 reserved by §36's PR).

**Commit:** `docs: reconcile GT1 with main`.

### Task 1: The crate, ids and paths

**Files:** `crates/loams-git/{Cargo.toml,src/{lib.rs,limits.rs,ids.rs,paths.rs,error.rs}}`, `crates/loams-git/tests/format.rs` (ids part).

**Produces:**

```rust
// ids.rs
pub struct RepoId(String);         // [a-z0-9][a-z0-9._-]{0,99}; parse() refuses anything else
pub struct NamespaceId(String);    // the string form used in ns/<ns>/ paths
pub use gix_hash::ObjectId;
pub struct RefName(String);        // gix_validate::reference::name; "HEAD" handled as a symref only
pub struct IdemKey(String);        // 1..=128 bytes of [A-Za-z0-9._:-]
pub struct Principal(String);
pub struct Seq(pub u64);           // Ord; Seq::ZERO; next(); Display as 20 digits
pub struct TraceParent(String);    // W3C traceparent, validated
// paths.rs
pub struct RepoPaths { root: String }   // "ns/<ns>/repos/<repo_id>"
impl RepoPaths {
    pub fn new(ns: &NamespaceId, repo: &RepoId) -> Self;
    pub fn head(&self) -> String;                       // <root>/head
    pub fn segment(&self, seq: Seq) -> String;          // <root>/wal/<seq:020>.lgw
    pub fn checkpoint(&self, seq: Seq) -> String;       // <root>/checkpoints/<seq:020>.lgc
    pub fn pack(&self, checksum: &ObjectId) -> String;  // <root>/packs/<hex>.lpk
    pub fn wal_prefix(&self) -> String;  pub fn checkpoint_prefix(&self) -> String;  pub fn pack_prefix(&self) -> String;
    pub fn parse_segment(path: &str) -> Option<Seq>;    pub fn parse_checkpoint(path: &str) -> Option<Seq>;
}
// limits.rs: the constants of the Global Constraints, as `pub const`
```

**Tests:** `refname_validation_matches_git_check_ref_format` (a corpus of 40 valid and invalid names, each also run through `git check-ref-format` when `git` is present); `repo_id_charset`; `seq_formats_as_twenty_digits_and_sorts_lexically`; `paths_round_trip`; `limits_are_documented` (every `pub const` in `limits.rs` has a doc comment naming a §36 section).

**Commit:** `git: add the loams-git crate with ids and object paths`.

### Task 2: Formats

**Files:** `proto/loams/git/v1/wal.proto` (§36 §4.3 verbatim), `crates/loams-git/build.rs`, `crates/loams-git/src/format/{mod.rs,segment.rs,checkpoint.rs,lpk.rs,event.rs}`, `crates/loams-git/tests/{format.rs,golden/*}`.

**Produces:**

```rust
pub mod pb { /* prost-generated loams.git.v1 */ }
// segment.rs
pub const SEGMENT_MAGIC: &[u8; 8] = b"LGITWAL\0";   pub const SEGMENT_END: &[u8; 8] = b"LGITWEND";
pub const SEGMENT_VERSION: u16 = 1;
pub fn encode_segment(seq: Seq, batch: &WalBatch) -> Result<Bytes, FormatError>;      // TooLarge over limits::MAX_SEGMENT_BYTES
pub fn decode_segment(expected_seq: Seq, bytes: &[u8]) -> Result<WalSegment, FormatError>; // Corrupt{reason}, SeqMismatch, UnknownVersion
// checkpoint.rs
pub const CHECKPOINT_MAGIC: &[u8; 8] = b"LGITCKPT";  pub const CHECKPOINT_END: &[u8; 8] = b"LGITCEND";
pub fn encode_checkpoint(c: &pb::Checkpoint) -> Bytes;  pub fn decode_checkpoint(bytes: &[u8]) -> Result<pb::Checkpoint, FormatError>;
// lpk.rs
pub const LPK_MAGIC: &[u8; 8] = b"LGITLPK\0";  pub const LPK_FOOTER_LEN: usize = 32;
pub struct LpkFooter { pub pack_len: u64, pub idx_len: u64 }
pub fn encode_footer(pack: &[u8], idx: &[u8]) -> [u8; 32];                // crc32c over pack ‖ idx
pub fn decode_footer(tail: &[u8; 32]) -> Result<LpkFooter, FormatError>;
pub fn pack_checksum(pack: &[u8]) -> Result<ObjectId, FormatError>;        // the pack's 20-byte trailer
// event.rs
pub struct TxnEvent { pub ns: NamespaceId, pub repo: RepoId, pub tenant: String, pub key: IdemKey,
                      pub trace: Option<TraceParent>, pub time_unix_ms: i64, pub body: TxnBody }
pub enum TxnBody { RefTxn(pb::RefTransaction), PackSet(pb::PackSetChange), Config(pb::ConfigChange) }
pub fn to_cloudevent(e: &TxnEvent, seq: Seq, index: u32) -> CloudEvent;   // §36 §4.3's attribute table exactly
pub fn from_cloudevent(ce: &CloudEvent) -> Result<TxnEvent, FormatError>;  // refuses a wrong type, a `dataschema` that does not match the type, an `id` that is not a key, a missing extension (D415)
```

**Semantics:** the header, body and trailer layouts of §36 §4.3, little-endian; CRC32C (Castagnoli) over header and body; a reader accepts format versions 1 and (once a version 2 exists) 1–2; an unknown higher version is `UnknownVersion { found }`. The segment body is the `CloudEventBatch` protobuf. `loamsseq` is the decimal string of the segment's seq and must equal the segment header's seq. The CloudEvents `id` is the transaction's `IdemKey` in lowercase hex and `dataschema` is `urn:loams:proto:loams.git.v1.<Message>`; there are no `idempotencykey` or `schemaversion` extensions (§36 §4.3, D364, D415).

**Tests:** `segment_round_trip` (proptest over random batches up to the limit); `segment_over_one_mib_is_too_large`; `corrupt_crc_is_error`; `truncated_segment_is_error`; `seq_mismatch_is_error`; `golden_segment_v1` (a checked-in file of a fixed batch decodes, and encoding the batch reproduces it byte for byte); `checkpoint_round_trip`; `golden_checkpoint_v1`; `lpk_footer_round_trip`; `pack_checksum_reads_trailer` (a pack made by `git pack-objects`); `event_attributes_match_design_table`; `event_without_tenantid_is_refused`; `n_minus_one_is_read` (a stub: version 0 bytes are refused with `UnknownVersion`, and the test documents where version 1 readers must keep working when version 2 arrives).

**Commit:** `git: add the loams.git.v1 formats (segments, checkpoints, pack objects)`.

### Task 3: `BlobStore`

**Files:** `crates/loams-git/src/blob.rs`, `crates/loams-git/tests/blob.rs`.

**Produces:** the `BlobStore` trait of §36 §5.3 verbatim, plus:

```rust
pub struct StoreBlobStore { store: Store, paths: RepoPaths }
impl StoreBlobStore { pub fn new(store: Store, paths: RepoPaths) -> Self; }
pub struct LpkWriter;   // builds pack ‖ idx ‖ footer from a pack and its idx (bytes or files in a temp dir)
impl LpkWriter {
    pub fn from_parts(pack: Bytes, idx: Bytes) -> Result<(BlobId, pb::PackRef, Bytes), BlobError>;   // checks idx matches pack (object count, trailer)
}
pub async fn read_idx(blobs: &dyn BlobStore, pack: &pb::PackRef) -> Result<Bytes, BlobError>;     // one range read
pub async fn read_pack(blobs: &dyn BlobStore, pack: &pb::PackRef) -> Result<Bytes, BlobError>;
```

**Semantics:** Ruling 1. `put` under 64 MiB is `put_if_absent`; `AlreadyExists` becomes `Put::Existed` after a `head` length check (`Mismatch { id, stored, offered }` otherwise). Over 64 MiB, `put_stream` is an unconditional multipart upload. `get_range` refuses a range past the blob's end (`OutOfRange`). Retryable store errors are retried three times with jittered backoff (50, 200, 800 ms) inside the store adapter, then surfaced.

**Tests:** `put_is_create_only_and_idempotent`; `put_existing_with_other_length_is_mismatch`; `lost_ack_put_returns_existed_on_retry` (`FaultyStore` `ErrorAfterApply` on the pack path); `get_range_reads_idx_section`; `range_past_end_is_out_of_range`; `lpk_from_git_pack_round_trips` (a pack from `git pack-objects`, read back through `read_pack`/`read_idx`, verifies with `git verify-pack`); `large_blob_uses_multipart` (a 65 MiB blob on the in-memory store). **Q385** is recorded by a manual run of `large_blob_uses_multipart` against RustFS with a concurrent reader polling the object (no partial object observed in 200 tries, or the finding).

**Commit:** `git: add BlobStore over loams-store with one-object pack bundles`.

### Task 4: `WalStore`

**Files:** `crates/loams-git/src/wal.rs`, `crates/loams-git/tests/wal.rs`.

**Produces:** the `WalStore` trait, `Seq`, `WalBatch`, `WalSegment`, `Hint` and `Appended` of §36 §5.1 verbatim, plus:

```rust
pub struct StoreWalStore { store: Store, paths: RepoPaths }
pub enum WalError { TooLarge, Corrupt { seq: Seq, reason: String }, Store(StoreError) }
impl Hint { pub fn empty() -> Self; }
```

**Semantics:**
1. `append(expect_seq, batch)`: encode (Task 2); `put_if_absent(segment(expect_seq))`. `Ok` → `Committed`. `AlreadyExists` → `get` and decode it: the same `batch_id` → `Committed`; another → `Fenced { existing }`. A retryable error → `get`: present with our `batch_id` → `Committed`, present with another → `Fenced`, absent → resend (at most 5 attempts), then the error.
2. `read_from(from, max)`: GETs `from`, `from+1`, … issued up to 8 in parallel and returned in order, stopping at the first `NotFound`.
3. `hint`: GET `head` (a small JSON object `{"seq":…,"checkpoint":…,"written_unix_ms":…}`); missing → `Hint::empty()`. `publish_hint` is an unconditional `put`.
4. `latest_checkpoint(at_or_below)`: if the hint's checkpoint ≤ `at_or_below`, read it; else LIST `checkpoints/` and take the greatest ≤ `at_or_below`.
5. `put_checkpoint`: `put_if_absent`; `AlreadyExists` with equal bytes is `Ok`, different bytes is `Corrupt`.

**Tests:** `append_is_exclusive_across_writers` (32 tasks race on `Seq(1)` with distinct batches: exactly one `Committed`, 31 `Fenced` naming the winner); `retry_after_lost_ack_is_committed` (`ErrorAfterApply` on the segment PUT); `retry_after_lost_ack_fenced_by_other` (the first attempt failed before apply, another writer took the seq); `read_stops_at_gap` (segments 1, 2, 4 → reads 1–2); `corrupt_segment_is_error_not_stop`; `hint_missing_is_empty`; `latest_checkpoint_uses_hint_then_list`; `checkpoint_put_is_idempotent_on_equal_bytes`; `store_faults_never_produce_two_owners` (proptest: random `FaultyStore` seeds and rates, 4 writers × 50 appends, then every seq read back has exactly the batch its `Committed` writer got).

**Commit:** `git: add the bucket WAL with fenced create-only segments`.

### Task 5: The pack-cache `Odb`

**Files:** `crates/loams-git/src/odb.rs`, `crates/loams-git/tests/fork.rs` (lookup part).

**Produces:**

```rust
pub struct PackCache { dir: PathBuf, max_bytes: u64 }   // content-keyed files <checksum>.pack/.idx; LRU by atime; safe to delete
pub struct Odb { blobs: Arc<dyn BlobStore>, cache: Arc<PackCache>, packs: Vec<pb::PackRef>, parent: Option<Box<Odb>> }
impl Odb {
    pub async fn open(blobs: Arc<dyn BlobStore>, cache: Arc<PackCache>, snapshot: &RefSnapshot,
                      parent: Option<Odb>) -> Result<Self, OdbError>;
    pub async fn contains(&self, oid: &ObjectId) -> Result<bool, OdbError>;
    pub async fn read(&self, oid: &ObjectId) -> Result<(gix_object::Kind, Bytes), OdbError>;   // delta-resolved
    pub async fn missing_from_closure(&self, tips: &[ObjectId], extra: Option<&Odb>) -> Result<Vec<ObjectId>, OdbError>;
}
```

**Semantics:** GT1's `Odb` downloads whole packs into `PackCache` on first use and reads them with `gix-pack` (`gix_pack::Bundle`). Lookup order: the repository's packs (newest first), then the fork parent's at its seq (recursively, at most 8 levels; deeper is `ForkTooDeep`). `missing_from_closure` walks commits, trees and tags from the tips and returns objects found in neither `self` nor `extra` (the pushed pack): the connectivity check of §36 §6.3 step 3. GT2 replaces the whole-pack cache with range reads for serving; this type stays for compaction and tests.

**Tests:** `reads_objects_written_by_git`; `delta_objects_resolve`; `fork_falls_through_to_parent_at_seq`; `fork_depth_is_bounded`; `missing_from_closure_finds_a_missing_blob`; `cache_eviction_never_breaks_reads` (cache of 1 byte forces re-download every read).

**Commit:** `git: add a pack-cache object database with fork fall-through`.

### Task 6: The ref state machine and idempotency

**Files:** `crates/loams-git/src/state.rs`, `crates/loams-git/tests/state.rs`.

**Produces:**

```rust
pub struct RepoState {                      // the state after `seq`
    pub seq: Seq, pub format: pb::ObjectFormat,
    pub refs: BTreeMap<RefName, ObjectId>, pub symrefs: BTreeMap<RefName, RefName>,
    pub protections: Vec<pb::Protection>, pub packs: Vec<pb::PackRef>, pub parent: Option<pb::ForkParent>,
    idem: IdemIndex,
}
pub struct IdemIndex;   // key → (request_digest, seq, index, expires_unix_ms); pruned by time
pub enum Check { Accept, Replay(Receipt), Reject(Vec<(RefName, RejectReason)>), Mismatch }
impl RepoState {
    pub fn empty(format: pb::ObjectFormat) -> Self;
    pub fn from_checkpoint(c: pb::Checkpoint) -> Result<Self, StateError>;
    pub fn to_checkpoint(&self, now_unix_ms: i64) -> pb::Checkpoint;          // idempotency entries still in the window only
    pub fn check(&self, txn: &RefTxn, now_unix_ms: i64) -> Check;              // pure
    pub fn apply_txn(&mut self, txn: &pb::RefTransaction, key: &IdemKey, digest: [u8; 32], seq: Seq, index: u32, now: i64);
    pub fn apply_segment(&mut self, seg: &WalSegment) -> Result<(), StateError>;   // seq must be self.seq + 1
    pub fn snapshot(&self) -> RefSnapshot;
}
```

**Semantics:** `check` in this order: idempotency (`Replay` if the key and digest match an unexpired entry; `Mismatch` if the key matches with another digest); every update's `Expect` against the refs (`Stale { current }`, `Exists`); `InvalidName`; protections (`deny_delete`, `deny_force` with `Expect::Any` counting as force); the scope (`OutOfScope`, GT4: in GT1 every `scope` is `None`). A transaction with two updates to one ref is `Reject` with `InvalidName` on that ref. `apply_segment` applies events in index order; a `PackSetChange` replaces packs; a `ConfigChange` replaces symrefs and protections.

**Tests:** `model_matches_reference` (proptest: random transactions applied through `check`/`apply_txn` agree with a 30-line reference model over `HashMap`); `group_sees_earlier_txns` (txn 2 expecting txn 1's new oid is accepted in the same state clone); `replay_inside_window_returns_receipt`; `key_reuse_with_other_digest_is_mismatch`; `after_window_old_oid_decides`; `protected_ref_refuses_delete_and_force`; `checkpoint_round_trip_keeps_window_entries_only`; `apply_segment_refuses_a_gap`.

**Commit:** `git: add the repository state machine with idempotency windows`.

### Task 7: `BucketRefLog`: the sequencer and group commit

**Files:** `crates/loams-git/src/{reflog.rs,sequencer.rs,testing.rs}`, `crates/loams-git/tests/{reflog.rs,linearizable.rs}`.

**Produces:** the `RefLog` trait, `RefTxn`, `Receipt`, `RefError`, `ReadAt`, `RefSnapshot` and `CommittedTxn` of §36 §5.2 verbatim, plus:

```rust
pub struct SequencerConfig { pub max_group_txns: usize /* 64 */, pub max_segment_bytes: usize /* 1 MiB */,
                             pub group_linger: Duration /* 0 */, pub hint_every: Duration /* 1 s */,
                             pub tenant: String, pub ns: NamespaceId, pub repo: RepoId }
pub struct BucketRefLog;   // Clone; one sequencer task per instance
impl BucketRefLog {
    pub async fn open(wal: Arc<dyn WalStore>, blobs: Arc<dyn BlobStore>, config: SequencerConfig,
                      clock: Arc<dyn Clock>) -> Result<Self, RefError>;      // loads hint → checkpoint → replay
    pub async fn shutdown(self);                                             // answers queued txns Unavailable
}
pub trait Clock: Send + Sync { fn now_unix_ms(&self) -> i64; }
pub mod testing { pub struct ManualClock; pub fn mem_reflog(store: Store) -> BucketRefLog; }
```

**Semantics:** §36 §4.4 exactly:
1. `commit` checks every `txn.packs` entry with `BlobStore::head` before queuing; a missing pack fails the call with `RefError::Wal(WalError::Store(NotFound))` and nothing is queued (the receive path never hits this, because it PUTs the pack first).
2. The loop takes a group (by count and encoded size), checks each transaction against a clone of the state with `RepoState::check`, answers `Replay`, `Reject` and `Mismatch` at once, and appends the accepted ones at `state.seq + 1` with a fresh random `batch_id`.
3. `Committed` → apply, answer `Receipt { seq, index, replayed: false }`, mark the hint dirty. `Fenced` → apply the existing segment and every later one (`read_from`), then re-check the group's accepted transactions against the new state from scratch, answer the ones that now fail, and append the rest at the new tail with a new `batch_id`. A transaction is retried across at most 16 fences before `Unavailable`.
4. One `append` in flight at a time. The hint is published at most every `hint_every`, and on `shutdown`.
5. `snapshot(Latest)`: `read_from(state.seq + 1, 64)` until empty, applying what it finds, then the state's snapshot. `AtLeast(s)` skips the read when `state.seq ≥ s`. `Exactly(s)`: a checkpoint ≤ `s` plus replay to `s` (24 h retention is enforced by GC, Task 8).
6. `watch(from)` streams `CommittedTxn` from the WAL (catch-up) and then from the sequencer's own commits and fence reads, without gaps.

**Tests** (`tests/reflog.rs`): `commit_then_snapshot_sees_it`; `group_commit_batches_concurrent_txns` (200 concurrent commits with a 50 ms injected PUT delay produce fewer than 60 segments); `group_rejects_only_the_stale_txn`; `atomic_multi_ref_all_or_none`; `lost_ack_resolves_to_committed`; `fenced_group_revalidates` (a second `BucketRefLog` on the same store commits a conflicting update in between); `replay_returns_original_receipt`; `snapshot_is_a_prefix_of_commits`; `watch_has_no_gaps_across_fences`; `missing_pack_is_refused_before_queueing`; `shutdown_answers_unavailable`.

**Tests** (`tests/linearizable.rs`, with `loams_meta_conformance::linearizability`): `single_sequencer_linearizable`, `two_sequencers_one_store_linearizable` and `four_sequencers_under_faults_linearizable` — histories of `commit` (CAS on 3 refs) and `snapshot(Latest)` from 4 clients over 300 operations each, with `FaultyStore::random` (5% errors, 5% `ErrorAfterApply`, delays up to 20 ms), seeds 0..32 on PRs and 0..1024 nightly; the model is a map of 3 refs with CAS semantics.

**Commit:** `git: add the bucket RefLog with group commit, fencing and idempotency`.

### Task 8: Checkpoints, forks and segment GC

**Files:** `crates/loams-git/src/{checkpointer.rs,fork.rs,gc.rs}`, `crates/loams-git/tests/{checkpoint.rs,fork.rs,gc.rs}`.

**Produces:**

```rust
pub async fn create_repo(wal: &dyn WalStore, format: pb::ObjectFormat) -> Result<(), RefError>;          // checkpoint 0
pub async fn fork_repo(src: &dyn RefLog, dst_wal: &dyn WalStore, src_repo: &RepoId, at: ReadAt) -> Result<Seq, RefError>;
pub struct CheckpointPolicy { pub every_segments: u64 /* 256 */, pub every_bytes: u64 /* 8 MiB */ }
// the sequencer writes a checkpoint after a commit that crosses the policy, off the ack path
pub struct SegmentGcPlan { pub delete: Vec<String>, pub keep_from: Seq }
pub async fn plan_segment_gc(wal: &dyn WalStore, store: &Store, paths: &RepoPaths, now_unix_ms: i64,
                             exactly_retention: Duration, grace: Duration) -> Result<SegmentGcPlan, RefError>;
pub async fn run_segment_gc(store: &Store, plan: &SegmentGcPlan) -> Result<u64, RefError>;
```

**Semantics:** checkpoints are written by the sequencer task after the acknowledgement of the commit that crossed the policy, never before it. A fork writes the destination's checkpoint 0 with `parent = {src_repo, seq}`, the source's refs at that seq and no packs, through `put_checkpoint` (one PUT). Segment GC keeps every segment above the oldest checkpoint younger than `exactly_retention` and deletes the rest older than `grace`; segments above a gap (unreachable) are deleted after `grace`. Pack GC is GT2.

**Tests:** `checkpoint_plus_replay_equals_state` (proptest over random commit sequences with random checkpoint points); `checkpoint_never_delays_ack` (a stalled checkpoint PUT does not delay the next commit's receipt); `fork_is_one_put` (store op count); `fork_sees_parent_refs_and_objects`; `fork_of_fork_falls_through`; `gc_keeps_exactly_retention`; `gc_deletes_unreachable_above_gap_after_grace`; `gc_never_deletes_segments_a_reader_needs` (a reader opened before GC at an old checkpoint finishes its replay).

**Commit:** `git: add checkpoints, O(1) forks and segment GC`.

### Task 9: `git-remote-loams`

**Files:** `crates/loams-git-remote/{Cargo.toml,src/{main.rs,protocol.rs,url.rs,fetch.rs,push.rs,local.rs}}`, `crates/loams-git-remote/tests/{helper.rs,concurrent.rs,faults.rs}`.

**Produces:** the binary `git-remote-loams`, invoked by git as `git-remote-loams <remote-name> <address>` for `loams::<address>` URLs.

**Semantics** (gitremote-helpers):
1. **Address.** `<address>` is an `object_store` URL whose path ends in `ns/<ns>/repos/<repo_id>`; the helper opens `Store::from_url` at that prefix (credentials from the environment). Anything else exits with `fatal: loams: expected <store-url>/ns/<ns>/repos/<repo_id>, got <address>`.
2. **`capabilities`** → `fetch`, `push`, `option`, then a blank line.
3. **`option`** → `verbosity`, `progress`, `atomic`, `push-option`, `dry-run` (`ok`), anything else `unsupported`.
4. **`list` / `list for-push`** → `RefLog::snapshot(Latest)`; one line `<oid> <ref>` per ref, `@<target> HEAD` for the symref; a missing repository lists nothing for `for-push` and fails `list` with `fatal: repository '<address>' not found` (Ruling 7).
5. **`fetch <oid> <ref>` lines, then a blank line** → Ruling 4: download every live pack (and the fork parent's, at its seq) not in `.git/loams/<remote>/packs`, write `pack-<checksum>.pack`/`.idx` into `.git/objects/pack/` atomically (write to `tmp_pack_*`, then rename), record the checksums, answer a blank line.
6. **`push <src>:<dst>` lines, then a blank line** → for each refspec: resolve `<src>` (empty = delete); FF check (Ruling 5) unless `+`; collect `<new>` tips and the remote's tips; `git pack-objects --revs --stdout --thin` with `<new>` and `^<remote tip>` lines on stdin; complete the thin pack with `git index-pack --stdin --fix-thin` into a temp dir (giving pack and idx); `LpkWriter::from_parts` → `BlobStore::put` → one `RefTxn` (all refspecs if `option atomic true`, else one per refspec, committed concurrently so they share segments) through a `BucketRefLog` opened for this process; answer `ok <dst>` or `error <dst> <reason>` per ref (`fetch first` for `Stale`, `non-fast-forward`, `protected`, `already exists`), then a blank line; `shutdown` the log (publishes the hint).
7. Progress lines go to stderr when `progress` is on. Exit code 0 unless the protocol itself failed.

**Tests** (`tests/helper.rs`, driving the real `git` binary with `PATH` pointing at the built helper and a `file://` store in a temp dir): `clone_of_missing_repo_fails`; `first_push_creates_repo`; `clone_fetch_push_round_trip`; `non_fast_forward_is_rejected_with_fetch_first`; `force_push_with_plus_succeeds`; `delete_ref`; `atomic_multi_ref_push_all_or_none`; `push_options_are_recorded` (visible in the WAL event); `fork_then_clone` (a fork made with `fork_repo` clones with the parent's objects); `fetch_after_other_push_gets_new_pack`. **Tests** (`tests/concurrent.rs`): `concurrent_pushes_to_distinct_branches_all_land` (8 `git push` processes); `concurrent_pushes_to_one_branch_one_wins_per_round` (8 processes, 5 rounds, each round exactly one `ok` and the rest `fetch first`); `concurrent_pushes_never_lose_an_acked_push` (every `ok` ref is in the final snapshot or superseded by a later `ok`). **Tests** (`tests/faults.rs`, through a `LOAMS_GIT_FAULTS=<seed>:<rate>` test hook compiled only with the `faults` feature): `push_survives_lost_acks`, `repeated_push_after_lost_answer_is_replayed`.

**Commit:** `git: add git-remote-loams, a serverless remote helper over the bucket`.

### Task 10: Benchmarks, RustFS and the CI job

**Files:** the bench (in `bench/git-wal/` or `crates/loams-git/benches/`, per `bench/`'s conventions on `main`), `scripts/git/{rustfs.sh,bench.sh}`, `.github/workflows/ci.yml`, `docs/design/36-loams-git.md` (§4.4 measured numbers).

**Semantics:**
- `git-wal-bench --store <url> --repos <n> --pushers <p> --duration <s> --pack-bytes <b> [--put-delay-ms <d>]` drives `BucketRefLog` directly (no git processes): reports commits/s, p50/p99 commit latency, mean group size, store PUTs and GETs per commit, as one JSON line.
- Runs recorded in §36 §4.4 (as a table, with the machine and date): in-memory with `--put-delay-ms` 20, 50, 100, 150 (the latency model); RustFS on the machine (via `scripts/git/rustfs.sh start`, `rustfs/rustfs:1.0.x`); and, when the owner provides credentials, R2, S3 Standard and S3 Express One Zone in one region, which answers **Q384** (including whether an Express directory bucket honours `If-None-Match: *`: the `append_is_exclusive_across_writers` test run against it).
- The CI job `git` (path-filtered on `crates/loams-git*/**`, `proto/loams/git/**`): `cargo test -p loams-git -p loams-git-remote` with RustFS as a service container for the store-backed suites (`LOAMS_TEST_S3_URL`), skipping them with a printed reason when unset; seeds 0..32. The nightly runs seeds 0..1024 and the bench against RustFS.

**Exit:** §36's GT1 gate (§13): every test above green; ≥ 30 commits/s on one hot repository at `--put-delay-ms 100` with p99 commit latency under 1 s, or the measured shortfall recorded with the owner's ruling.

**Commit:** `bench: measure the bucket WAL's push rate and latency per store`; `ci: run the loams-git suites against RustFS`.

### Task 11: Docs and close

**Files:** `docs/design/36-loams-git.md` (an "As built (GT1)" note under §4, the measured table, any ruling that changed the design), `CHANGELOG.md`, and this plan's "Rulings made during execution".

**Commit:** `docs: record GT1 as built and close the plan`.

## Rulings made during execution

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| — | (Task 0 fills this table) | | |
