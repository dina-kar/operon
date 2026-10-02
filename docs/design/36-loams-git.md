# 36 — Loams Git: a WAL on the Bucket, Smart HTTP, Agent Scopes and a Build Cache

Status: **Approved** (owner defaults, 2026-10-02: "do suggested for all") · 2026-10-01. Source: §14 "Loams Git (hosting, WAL, build cache)" and the related open questions in §15 of the owner's draft "Loams Serverless Runtime — Consolidated Plan" (2026-09-30, `chatdump.md` lines 868–946). The owner asked on 2026-10-01 to fold that draft into the design docs, the decision log and the plans. This document **extends §15 §3 (repos on the bucket)**; it does not fork that design. Where it changes an approved part of §15, the change is marked with its D-number and listed in §15 "Conflicts". The `Fs` trait is §17 below. Running Loams Git on Cloudflare (Workers, Durable Objects, Containers) is a commercial Cloudflare target (`loam-platform`): since the owner's ruling of 2026-10-02 ("move Cloudflare, OpenRTB etc. commercial to private repos"; §38 D440 on PR #182) the former §35 lives there (private). This repository's Git core stays portable to it but does not depend on it.

Decisions **D388–D399**; open questions **Q384–Q395** (the range D380–D399 / Q380–Q399 was shared with the former §35; D381 and D382 stay here, in §17, and D380, D383–D387 moved to `loam-platform`, private). Plans: [GT1](../plans/2026-10-01-gt1-wal-git-core.md) (the WAL git core and `git-remote-loams`), [GT2](../plans/2026-10-01-gt2-smart-http.md) (Smart HTTP for stock git) and [GT3](../plans/2026-10-01-gt3-build-cache-and-mirror.md) (the sccache backend and the crates mirror). GT4 and GT5 are not yet planned.

Markers: **(source)** means read in the upstream repository or documentation on 2026-10-01, at the URL in §18. **(verify)** means the plan that builds it checks it first. **(estimate)** means computed, not measured. **(draft)** means the figure comes from the owner's draft and was not re-checked.

---

## 1. Summary

| # | Decision | Status |
|---|---|---|
| D388 | **Loams Git extends §15 §3.** A repository's refs move from §15's single CAS'd `refs` document to a **per-repository write-ahead log of create-only segments plus periodic checkpoints**, under the same `ns/<ns>/repos/<repo_id>/` prefix. Packs stay immutable and content-addressed. Forks stay O(1). Repos stay a service with their own bucket WAL, not a sixth object kind and not a Loams stream (proposed answer to Q15). Track **GT** (GT1–GT5) delivers W1's repository scope and parts of W2 (§13) | Approved (owner defaults, 2026-10-02) |
| D389 | **The commit point is the create-only PUT of the next segment**, `wal/<seq:020>.lgw` with `If-None-Match: *`. There is no separately CAS'd head pointer: `head` is a hint, written at most once a second. Exactly one batch can own a sequence number on every supported store (S3, R2, GCS `ifGenerationMatch=0`, Azure `If-None-Match: *`, RustFS) | Approved (owner defaults, 2026-10-02) |
| D390 | **Formats.** A WAL record is a **CloudEvent 1.0** in the protobuf format (D270) whose `id` is the transaction's idempotency key and whose version is the `type` suffix plus `dataschema` (D364's profile; amended 2026-10-02 by D415), with the extensions `tenantid`, `traceparent` and `loamsseq`, whose data is a `loams.git.v1` message; a segment frames one `CloudEventBatch` with magic, version and CRC32C (§03 §6) and is at most 1 MiB; a checkpoint holds the full ref state, the live pack set, the fork parent and the idempotency window; a push is stored as **one object**, `packs/<checksum>.lpk` (pack ‖ index ‖ footer), named by the pack's trailer checksum | Approved (owner defaults, 2026-10-02) |
| D391 | **One sequencer per repository, with group commit.** The rendezvous owner of `(ns, repo)` (D75) under a lease (on a commercial Cloudflare target (`loam-platform`), a Durable Object per repository). One segment PUT in flight at a time; every transaction that arrives meanwhile joins the next segment. **Correctness never depends on there being one sequencer**: two writers for one repository fence each other on the segment name. Target **30 pushes/s per hot repository** (draft) | Approved (owner defaults, 2026-10-02) |
| D392 | **Four traits in `loams-git`, with exact semantics** (§5): `WalStore` (fenced, monotonic append, idempotent on the batch id; contiguous reads), `RefLog` (atomic multi-ref transactions, linearizable commits and reads, idempotent on the idempotency key within a 1 h window), `BlobStore` (create-only, content-named blobs; range reads) and `Materializer` (scoped, on-demand blob fetch for agents) | Approved (owner defaults, 2026-10-02) |
| D393 | **Usage is exposed through §27's hooks only.** Git, the build cache and the package mirror export metric families per namespace (§11) and append a CloudEvent per committed transaction to the repository's event stream. No meter events and no ledger in this repository. Amends the draft's "CloudEvents feeding the metering ledger" (D190, D200–D202) | Approved (owner defaults, 2026-10-02) |
| D394 | **Smart HTTP.** `git-upload-pack` speaks **protocol v2** (`ls-refs`, `fetch` with `filter`, `shallow` and `wait-for-done`, `object-info`); a v0/v1 upload-pack is added in GT2 only if a client in W1's matrix lacks v2. `git-receive-pack` speaks v0/v1, since protocol v2 has no push (`report-status`, `report-status-v2`, `atomic`, `delete-refs`, `side-band-64k`, `ofs-delta`, `push-options`, `quiet`). The server loop is Loams's, on gitoxide primitives. **Stock `git` runs only as an unmodified separate process**: the test oracle, the repack worker (D396) and, on the client, the pack steps of `git-remote-loams`'s pushes (D395). Answers §15 Q1 and the draft's last open question | Approved (owner defaults, 2026-10-02) |
| D395 | **`git-remote-loams`.** In GT1 it is a serverless helper that reads and writes the bucket directly through the core (capabilities `fetch`, `push`, `option`; `loams::<store-url>` addresses). In GT2 it adds `stateless-connect` to tunnel protocol v2 to an in-process upload-pack, which is what partial clone and lazy fetch need, and `loams://<host>/<ns>/<repo>` addresses that reach a Loams server | Approved (owner defaults, 2026-10-02) |
| D396 | **Compaction is never on the push path.** Checkpoints every 256 segments or 8 MiB of replay; geometric repack with bitmaps and a multi-pack index by **stock `git repack`** on a worker's local mirror (gitoxide cannot yet write deltas or bitmaps); pack-set changes are committed through the WAL like pushes; one compactor per repository, by lease; GC by fork-family reachability after the §03 §7 grace period | Approved (owner defaults, 2026-10-02) |
| D397 | **`loams-vfs` is §15 §5.1's `/workspace` lower layer** (GT4). A scope is a list of cone-mode sparse-checkout paths carried in the agent's vended token; blobs are fetched on first read; **write admission** refuses a commit that changes paths outside the scope; a commit is one WAL record whatever the folders it touches. **No per-scope WAL partitions** until GT4 measures that the per-repository sequencer is the bottleneck (Q391) | Approved (owner defaults, 2026-10-02) |
| D398 | **Build cache: sccache** (Apache-2.0, v0.18.0) is the primary cache. Loams serves it two ways: **direct** (sccache's S3 backend against R2, RustFS or S3 with vended prefix-scoped credentials; no Loams code on the path) and **through the gateway** (sccache's WebDAV backend against a Loams endpoint that meters hits and misses, refreshes entries on hit for approximate LRU and enforces trust). **Trust model:** trusted branches write; forks, pull requests and agent sandboxes read only, with an optional private scratch prefix. BuildCache (zlib) only if a toolchain sccache cannot handle needs it | Approved (owner defaults, 2026-10-02) |
| D399 | **Package mirror: a crates.io sparse-index read-through** in the `gateway` role (§15 §6): index files cached with ETag revalidation, `.crate` files content-addressed by their SHA-256 `cksum` in the public-packages namespace, the §15 §6 policy (allowlists, quarantine, audit records). The index lives in the object store, **not** in a Durable Object or D1 | Approved (owner defaults, 2026-10-02) |

## 2. Goals and non-goals

### 2.1 Goals

1. **Object storage is the source of truth for Git** (D1). Node disks, Durable Object storage and NVMe mirrors are caches. Any node, or a client with bucket credentials, can rebuild a repository's state from the bucket.
2. **Pushes are durable when acknowledged.** A push is acknowledged only after its pack and its ref update are in the bucket (the rule Continuity and Spokes share, §3).
3. **Throughput for agent fleets.** Thousands of small repositories with a few pushes a minute each, and hot repositories (a monorepo with an agent fleet) at 30 pushes/s (draft), without a stateful replica fleet.
4. **Stock clients.** `git` over Smart HTTP, and gitoxide, libgit2 and JGit, pass §15 W1's client matrix.
5. **Agents read only what they touch** (§15 principle 4), per folder scope, and cannot write outside it.
6. **One build cache and one package mirror**, with tenant isolation and a trust model that stops cache poisoning.
7. **Portable.** The same core runs on Kubernetes (NVMe, RustFS or S3) and on a commercial Cloudflare target (`loam-platform`); nothing in the core assumes either.

### 2.2 Non-goals

- A GitHub replacement: no pull-request UI, issues or CI (§15 §14). Webhooks and mirroring only.
- A distributed POSIX filesystem or concurrent multi-writer workspaces (§15 §14).
- Gossip, any-node writes and NVMe replica fleets in GT1–GT3. They are GT5, after measurement (D391).
- SHA-256 repositories in GT1–GT2. The object format is a field from the first format version (Q393).
- Git LFS beyond §15 §3.2's plan (batch API on the namespace CAS, W1).

## 3. Reference systems

| System | What it does | What Loams takes |
|---|---|---|
| **Cursor Continuity** ("Git at any scale", Vicent Martí, cursor.com, 2026-08; InfoQ, Leela Kumili, 2026-09-30) | An S3-backed WAL is the source of truth; NVMe repositories are warm caches; "we never acknowledge a push until it has been fully persisted"; batching hides PUT latency; rendezvous hashing picks preferred nodes; compare-and-swap on S3 lets any server accept a push; UDP gossip is a hint and conditional reads verify state; only the primary compacts, replicas download packs. **Reported, synthetic, not independently verified** (InfoQ's words): about 120 pushes/s on S3 Standard and over 300 on S3 Express One Zone, with compaction the bottleneck at the top rate (source) | The WAL as truth, ack-after-persist, batching, rendezvous placement, primary-only compaction. Not gossip or any-node writes yet (GT5) |
| **GitHub Spokes** (DGit, 2016; "Stretching Spokes", 2017, updated 2025-06-03) | Three replicas on three servers; writes stream to all three and commit when a quorum confirms; updates use a three-phase commit that doubles as a distributed lock; four round trips to distant replicas (source) | The contrast: Loams keeps no stateful replicas; the bucket is the quorum |
| **Delta Lake's transaction log** | Commits are files `_delta_log/<version>.json` created with put-if-absent; the next version number is the fence | The commit protocol of D389: a sequential, create-only object name is the compare-and-swap |
| **§15 §3** (approved) | One CAS'd `refs` document per repository, packs create-only, forks by alternates, Smart HTTP v2 in the `gateway` role, repo-affinity group commit | Everything except the single document (D388) |
| `git-remote-object-store` (Apache-2.0), `awslabs/git-remote-s3` (Apache-2.0, last push 2026-08-20), Cloudflare Artifacts (closed) | Client-side helpers that use a bucket as a serverless Git remote; a Git server on Durable Objects and R2 | GT1's serverless helper (D395); the Durable Object sequencer of a commercial Cloudflare target (`loam-platform`) |
| **go-git** (Apache-2.0, v5.19.2 2026-07-29) | Server-side `upload-pack` and `receive-pack` with a pluggable `storage.Storer`; its v6 `main` has v2 code while its compatibility table still lists v2 as unsupported (source) | A reference for the server loop. Not a dependency (Go; Loams's core is Rust and must run in Workers) |

## 4. The WAL on the bucket (D388–D391)

### 4.1 Layout

Extends §15 §3.1 and §15 §11:

```
ns/<ns>/repos/<repo_id>/
  head                          # hint: {seq, checkpoint_seq, written_unix_ms}; best effort, ≤ 1 write/s
  wal/<seq:020>.lgw             # create-only segments; segment n's PUT is the commit point of its batch
  checkpoints/<seq:020>.lgc     # create-only; the full state after segment seq
  packs/<checksum:hex>.lpk      # create-only, content-named: pack ‖ idx ‖ footer (D390)
  midx/<seq:020>.midx           # multi-pack index over the pack set at seq (compaction output, D396)
  commit-graph/<seq:020>.graph  # written by compaction
```

§15's `refs` document is replaced by `head`, `wal/` and `checkpoints/` (D388). §15's `packs/<ulid>.pack` + `.idx` become one `.lpk` object named by the pack checksum (D390), so a retried upload writes the same name and a push costs one PUT for its data.

### 4.2 The commit protocol (D389)

A writer that holds the state after segment `n` commits a batch by:

```
PUT wal/<n+1>.lgw   If-None-Match: *     body = segment(n+1, batch_id, events)
  200 → committed at n+1
  412 → fenced: segment n+1 exists. GET it, apply it, re-validate the batch's transactions
        against the new state, try n+2
  timeout / 5xx after the request was sent, or 429 (R2's one-write-per-second limit when writers race one seq)
        → unknown: GET wal/<n+1>.lgw
        own batch_id → committed;  another batch_id → as 412;  404 → resend the same bytes
```

- **Exactly one batch per sequence number.** Every store Loams supports refuses a second create of the same name: S3 (`If-None-Match`, 2024-08-20), R2 (S3 API and the Workers binding's `onlyIf`), GCS (`ifGenerationMatch=0`), Azure (`If-None-Match: *`) and RustFS (412 under a write lock; source). D178's `ObjectStoreProvider` already refuses a provider without conditional writes.
- **Why no head pointer.** The draft advanced a head pointer by conditional write after each segment. That costs a second PUT per batch, adds a second linearization point, and contends on one key. **R2 limits writes to the same key to one per second** (R2 limits page, updated 2026-06-08), which would cap a CAS'd head at one commit a second. Sequential segment names are written once each by a successful writer, so the limit applies only when fenced writers race one name, and their `429` is resolved by the GET above; `head` is coalesced to at most one write a second and only speeds up readers.
- **Contiguity.** A writer appends `n+1` only after it has read or written segment `n`. Readers read forward from a checkpoint and stop at the first missing number, so a segment written above a gap (which a correct writer never does) is unreachable, and GC deletes it after the grace period.
- **Strong reads.** S3 and R2 give read-after-write and list consistency (R2 consistency page), so "GET `wal/<n+1>` returns 404" proves that `n` is the tail at that instant. A linearizable read costs one GET past the state a node holds.
- **RustFS atomicity.** RustFS's conditional PUT is atomic for bodies up to 1 MiB (its PR #6798, 2026-08-28; source). Segments are capped at 1 MiB on every store (D390). Packs are larger, but a pack's name is its content, so two racing creators write identical bytes and either outcome is correct.

### 4.3 Formats (D390)

**Segment** `wal/<seq:020>.lgw`, format version 1:

```
header   magic "LGITWAL\0" (8) | format_version u16 LE = 1 | flags u16 = 0 | seq u64 LE
         | batch_id [16] (random per batch; recognises one's own segment after an unknown outcome)
         | created_unix_ms i64 LE | body_len u32 LE
body     io.cloudevents.v1.CloudEventBatch (protobuf format, the encoding D270 uses on gRPC)
trailer  crc32c u32 LE over header ‖ body | magic "LGITWEND" (8)
```

Readers accept format versions N and N−1 (§03 §6). A body over 1 MiB is refused at encode.

**Events.** Each event in a batch is one transaction:

| Attribute | Value |
|---|---|
| `specversion` | `1.0` |
| `id` | the transaction's idempotency key (§4.5) in lowercase hex; a ULID the sequencer generates for events without a client key (compaction, config changes). Dedup is on (`source`, `id`), as D270's. The event's position is `loamsseq` plus its index in the batch (D364, D415) |
| `source` | `/ns/<ns>/repos/<repo_id>` |
| `type` | `io.loams.dev.git.reftxn.v1`, `io.loams.dev.git.packset.v1` (compaction, D396), `io.loams.dev.git.config.v1` (`HEAD`, protection rules), under the owner's prefix `io.loams.dev.<domain>.<name>.v1` (ruling of 2026-10-01; the draft's §6 had `io.loams.<domain>.<name>.v1`) |
| `time` | the sequencer's clock at commit, RFC 3339 |
| `datacontenttype` / `dataschema` | `application/protobuf` / `urn:loams:proto:loams.git.v1.RefTransaction` (or `PackSetChange`, `ConfigChange`); with the `type` suffix `.v1` this is the version, so there is no `schemaversion` extension (D364) |
| `tenantid` | `<org>/<ns>`, from the credential, never from the request (D182's rule) |
| `traceparent` | the receive request's W3C trace context, if any |
| `loamsseq` | the segment's sequence number, as a decimal string (CloudEvents integers are 32-bit) |

**Data messages** (`proto/loams/git/v1/wal.proto`):

```protobuf
syntax = "proto3";
package loams.git.v1;

enum ObjectFormat { OBJECT_FORMAT_UNSPECIFIED = 0; SHA1 = 1; SHA256 = 2; }

message RefUpdate {
  string name = 1;                       // "refs/heads/main"; validated as git check-ref-format
  oneof expect {
    bytes old_oid = 2;                   // must currently point here
    bool must_not_exist = 3;             // create
    bool any = 4;                        // only for force updates the policy allows
  }
  bytes new_oid = 5;                     // empty = delete
}

message PackRef {
  string checksum = 1;                   // hex of the pack trailer checksum; names packs/<checksum>.lpk
  uint64 object_count = 2;
  uint64 pack_len = 3;                   // bytes of the pack section
  uint64 idx_offset = 4;                 // the idx section's offset in the .lpk
  uint64 idx_len = 5;
  bool has_bitmap = 6;
}

message RefTransaction {
  repeated RefUpdate updates = 1;        // applied atomically, all or none
  repeated PackRef packs = 2;            // packs this transaction adds (usually 0 or 1)
  string principal = 3;                  // who pushed (credential subject)
  repeated string push_options = 4;
  ObjectFormat object_format = 5;
}

message PackSetChange { repeated PackRef added = 1; repeated string removed = 2; string midx = 3; string commit_graph = 4; }
message SymRef { string name = 1; string target = 2; }
message Protection { string pattern = 1; bool deny_delete = 2; bool deny_force = 3; }
message ConfigChange { repeated SymRef symrefs = 1; repeated Protection protections = 2; }

message ForkParent { string repo_id = 1; uint64 seq = 2; }
message IdemEntry { string key = 1; bytes request_digest = 2; uint64 seq = 3; uint32 index = 4; int64 expires_unix_ms = 5; }
message Checkpoint {
  uint64 seq = 1;
  ObjectFormat object_format = 2;
  map<string, bytes> refs = 3;
  repeated SymRef symrefs = 4;
  repeated Protection protections = 5;
  repeated PackRef packs = 6;            // the live pack set
  ForkParent parent = 7;                 // set on forks: packs fall through to the parent at its seq
  repeated IdemEntry idempotency = 8;    // entries still inside the window
}
```

**Checkpoint** `checkpoints/<seq:020>.lgc`: header magic `LGITCKPT`, version u16 = 1, seq u64; body `Checkpoint`; trailer CRC32C and `LGITCEND`. No size cap (a repository with a million refs has a large checkpoint); it is written with a multipart upload above 64 MiB.

**Pack object** `packs/<checksum>.lpk`: the pack bytes exactly as received or generated, then the version 2 `.idx` bytes, then a 32-byte footer `magic "LGITLPK\0" | pack_len u64 | idx_len u64 | crc32c u32 | reserved u32`. Range reads fetch the footer, the idx's fan-out table and object entries without downloading the pack. A local mirror (D396, GT5) splits it into `pack-<checksum>.pack` and `.idx`.

### 4.4 The sequencer and group commit (D391)

One sequencer task serves each active repository. It holds the state after the last segment it has seen (refs, symrefs, protections, the pack set, the idempotency index) and a queue of validated transactions.

```
loop:
  wait until the queue is non-empty (and no PUT is in flight)
  group = take up to max_group_txns (64) or max_segment_bytes (1 MiB) from the queue
  state' = state
  for txn in group (arrival order):
      if idempotency hit: answer the original receipt; drop from group
      check every update's expectation against state'   (earlier txns of this group included)
      check protections (deny_delete, deny_force) and the token's scope (D397)
      pass → apply to state', keep;   fail → answer Rejected{per ref reasons}; drop
  PUT wal/<state.seq+1> (§4.2)
      committed → state = state'; answer every kept txn {seq, index}; schedule hint and stream mirror
      fenced    → apply the foreign segments; re-validate the kept txns from scratch; retry
```

- **One PUT in flight.** Transactions that arrive during a PUT form the next group, so the group size grows with load and no fixed linger is needed (`group_linger`, default 0 ms, exists for benchmarks).
- **What the sequencer does not do.** Pack upload, pack indexing, fast-forward checks and connectivity checks happen in the receive path *before* a transaction is queued (§6.3). Fast-forward is a property of the old and new commits, not of the ref state: if `old_oid` still matches at commit, a new commit that descends from it is a fast-forward. The sequencer therefore checks only object ids, protections and scopes, in memory.
- **Throughput (estimate).** With a segment PUT latency `L`, the sequencer commits `1/L` groups a second. At `L` = 50–150 ms that is 7–20 groups/s, so 30 pushes/s needs an average group of 2–5 transactions, which forms on its own at that load. A push's acknowledgement waits for its pack PUT, up to one in-flight PUT, and its own segment PUT: 3 × `L` plus transfer time. GT1 measures `L` on R2, S3 Standard, S3 Express One Zone and RustFS (Q384).
- **Placement.** The sequencer runs on the rendezvous owner of the placement key `(ns, repo, repo_id)` (D75) under the metastore lease `task/git-seq/<ns>/<repo_id>`, and nodes that receive a push for a repository they do not own forward the transaction to the owner, as §15 §3.2's repo-affinity routing describes. On a commercial Cloudflare target (`loam-platform`) the sequencer is a Durable Object instead. A wrong owner, a zombie after a lease loss, or a `git-remote-loams` client writing the bucket directly is fenced by §4.2, never trusted.
- **Idle repositories** keep no sequencer. The first push loads the hint, the checkpoint and the segments after it.

### 4.5 Idempotency

- **Keys.** A client may send `Idempotency-Key` (API pushes). For a git push, the receive path derives `sha256(principal ‖ pack checksum ‖ sorted (name, expect, new) triples)`, so git's own retry of the same push over a flaky connection is answered with the first push's result.
- **Window.** Keys are kept for `idempotency_window` (1 h, the same default as D270's CloudEvents dedup window) in the sequencer's index and in each checkpoint (`IdemEntry`). A retry inside the window returns the original receipt (`replayed: true`). A key reused with a different request digest is refused with `IdempotencyMismatch`, as Live's idempotency records are bound to their arguments (R1 review of #76).
- **After the window** a retry is a new transaction, and its old-oid expectations decide it: a duplicate push of an already-applied fast-forward finds `old_oid` changed and is rejected, never applied twice.

### 4.6 Reads

| Read | How | Cost |
|---|---|---|
| `Latest` on the sequencer's node | in-memory state, then one GET of `wal/<seq+1>` (404 → current) | 1 GET |
| `Latest` elsewhere | `head` → the checkpoint it names → segments after it → probe until 404 | 1 + 1 + k GETs; cached checkpoints make it k + 2 |
| `AtLeast(seq)` | as `Latest`, but a node whose state is at or past `seq` answers without the probe | 0–1 GET |
| `Exactly(seq)` | the latest checkpoint at or below `seq` plus segments up to `seq` (retained 24 h, as collections keep manifests, D38) | — |

A push returns its sequence number as a consistency token, `git:<repo_id>:<seq>`, so an agent that pushes and then reads (`ls-refs` with `server-option=loams-at-least=<seq>`, or a code-index search, §15 §4) sees its own push.

### 4.7 Forks

Forking repository A at sequence `s` writes repository B's `checkpoints/00000000000000000000.lgc` with `parent = {A, s}`, A's refs at `s` and an empty pack set: one PUT, no data copy (§15 §3.1's alternates rule, kept). Object lookup falls through to the parent's pack set at `s`, recursively. GC computes reachability across a fork family (§15 §3.3).

### 4.8 The event-stream mirror

After a segment commits, the sequencer appends each transaction's CloudEvent to the namespace stream `_git` (partition key `repo_id`) through `ProduceCloudEvents` (D270), deduplicated by `source` + `id`. This is §15 §3.2's "append a record to the repo's event stream", at least once with a repair sweep that compares `head` with the stream's last `loamsseq`. Code-index links (§15 §4) and usage consumers (§11) read it. On a commercial Cloudflare target (`loam-platform`) the sequencer posts to a Queue whose consumer calls `ProduceCloudEvents`.

## 5. The traits (D392)

All four live in `crates/loams-git` and are object-safe (`Arc<dyn …>`). On `wasm32` targets they use `async_trait(?Send)`, because Workers futures are not `Send` (§17).

> **Cross-reference, 2026-10-02 ([§42](42-cloudflare-2026-betas.md) D561, D562).** Cloudflare Artifacts (open beta; 1 GB per repository, 32 MB per blob) is a Git-level service and cannot implement `WalStore`, `RefLog` or `BlobStore`. The bucket WAL stays the default. An open adapter, `loams-git-artifacts`, mirrors commits to Artifacts and implements `Materializer` for agent workspaces; hosted use is private.

### 5.1 `WalStore`

```rust
/// Sequence numbers start at 1; Seq(0) is the empty log.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Seq(pub u64);

pub struct WalBatch { pub batch_id: [u8; 16], pub events: Vec<CloudEvent> }   // loams_cloudevents::CloudEvent
pub struct WalSegment { pub seq: Seq, pub batch: WalBatch, pub created_unix_ms: i64 }
pub struct Hint { pub seq: Seq, pub checkpoint: Seq, pub written_unix_ms: i64 }

pub enum Appended {
    /// The batch owns `seq`, now or from an earlier attempt with the same batch id.
    Committed { seq: Seq },
    /// Another batch owns `seq`; the caller applies it and re-validates.
    Fenced { existing: WalSegment },
}

#[async_trait]
pub trait WalStore: Send + Sync + fmt::Debug {
    /// Writes `batch` as segment `expect_seq` with a create-only PUT.
    /// Precondition (caller's): segment `expect_seq - 1` exists or `expect_seq == Seq(1)`.
    /// Fenced: at most one batch is ever stored under a seq, across every writer.
    /// Idempotent: a retry with the same `batch_id` after an unknown outcome returns `Committed`.
    /// Errors: `TooLarge` (encoded body > 1 MiB), `Store(StoreError)` (retryable per StoreError::is_retryable).
    async fn append(&self, expect_seq: Seq, batch: &WalBatch) -> Result<Appended, WalError>;

    /// Segments `from, from+1, …` up to `max` of them, stopping at the first missing number.
    /// A CRC or magic mismatch is `Corrupt { seq }`, never a silent stop.
    async fn read_from(&self, from: Seq, max: usize) -> Result<Vec<WalSegment>, WalError>;

    /// The hint, or `Hint::empty()` if none was written. Never ahead of the true tail.
    async fn hint(&self) -> Result<Hint, WalError>;
    /// Best effort and coalesced by the caller (≤ 1 write/s); failures are logged, not returned to pushers.
    async fn publish_hint(&self, hint: &Hint) -> Result<(), WalError>;

    /// Create-only; an existing checkpoint at the same seq is `Ok` if its bytes are equal.
    async fn put_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), WalError>;
    /// The checkpoint with the greatest seq ≤ `at_or_below`, using the hint, else a LIST of `checkpoints/`.
    async fn latest_checkpoint(&self, at_or_below: Seq) -> Result<Option<Checkpoint>, WalError>;
}
```

The draft's `append(expect_seq, record)` "idempotent on `idempotencykey`" (the draft's extension; the key is the CloudEvents `id` here, D415) is split in two: `WalStore` is idempotent on the batch (a store-level retry), and `RefLog` is idempotent on each transaction's key (a client-level retry, §4.5). A batch holds many keys, so the store cannot be idempotent per key without reading every segment.

### 5.2 `RefLog`

```rust
pub enum Expect { Oid(ObjectId), Absent, Any }
pub struct RefUpdate { pub name: RefName, pub expect: Expect, pub new: Option<ObjectId> }   // None = delete
pub struct RefTxn {
    pub key: IdemKey,
    pub request_digest: [u8; 32],
    pub updates: Vec<RefUpdate>,          // 1..=max_refs_per_txn (4096)
    pub packs: Vec<PackRef>,              // must already exist in the BlobStore
    pub principal: Principal,
    pub scope: Option<Scope>,             // write admission (D397); None = whole repository
    pub trace: Option<TraceParent>,
    pub push_options: Vec<String>,
}
pub struct Receipt { pub seq: Seq, pub index: u32, pub replayed: bool }
pub enum RejectReason { Stale { current: Option<ObjectId> }, Exists, Protected, OutOfScope { path: RepoPath }, InvalidName }
pub enum RefError {
    Rejected(Vec<(RefName, RejectReason)>),   // nothing applied
    IdempotencyMismatch,
    TooLarge,                                 // the transaction alone would exceed a segment
    Unavailable,                              // the sequencer is shutting down or lost its lease; retry
    Wal(WalError),
}
pub enum ReadAt { Latest, AtLeast(Seq), Exactly(Seq) }

#[async_trait]
pub trait RefLog: Send + Sync + fmt::Debug {
    /// Applies every update or none, at one sequence number, through group commit.
    /// Linearizable: the transaction takes effect at its segment's PUT, between call and return.
    /// Acknowledged only after the segment is durable.
    async fn commit(&self, txn: RefTxn) -> Result<Receipt, RefError>;

    /// The draft's `cas_ref(name, old, new)`: one update, through `commit`.
    async fn cas_ref(&self, name: RefName, old: Expect, new: Option<ObjectId>, key: IdemKey)
        -> Result<Receipt, RefError>;

    /// `Latest` is linearizable; `AtLeast(s)` reflects every commit up to `s`; `Exactly(s)` is retained 24 h.
    async fn snapshot(&self, at: ReadAt) -> Result<Arc<RefSnapshot>, RefError>;

    /// Committed transactions from `from`, in order, without gaps; resumable by seq.
    fn watch(&self, from: Seq) -> BoxStream<'static, Result<CommittedTxn, RefError>>;
}
```

`RefSnapshot` holds `seq`, the refs (`BTreeMap<RefName, ObjectId>`), symrefs, protections, the live pack set and the fork parent. Implementations: `BucketRefLog` (the sequencer of §4.4 over a `WalStore`, used by the gateway, the Durable Object and the GT1 helper in single-writer mode) and `MemRefLog` (tests).

### 5.3 `BlobStore`

```rust
/// Content-derived names: a pack checksum (`packs/<hex>.lpk`) or, from GT4, a scope cache chunk.
pub struct BlobId(String);
pub enum Put { Created, Existed }

#[async_trait]
pub trait BlobStore: Send + Sync + fmt::Debug {
    /// Create-only. If the name exists, returns `Existed` after checking that the stored length matches
    /// (`Mismatch` otherwise: two contents under one content-derived name is corruption).
    async fn put(&self, id: &BlobId, bytes: Bytes) -> Result<Put, BlobError>;
    /// Same, as a multipart upload for bodies over 64 MiB; the parts are buffered in an `Fs` (§17).
    async fn put_stream(&self, id: &BlobId, body: BoxStream<'static, io::Result<Bytes>>, len: u64)
        -> Result<Put, BlobError>;
    /// `range` must lie inside the blob; reads go through the H1 cache (§04) where one exists.
    async fn get_range(&self, id: &BlobId, range: Range<u64>) -> Result<Bytes, BlobError>;
    async fn head(&self, id: &BlobId) -> Result<Option<u64>, BlobError>;
}
```

There is no `delete` on the trait: only GC deletes, through `Store`, after the grace period and a GC claim (§03 §7). Objects are located by an `Odb` built on `BlobStore`: pack idx sections (cached in H0/H1) or the compaction's multi-pack index map an object id to `(pack, offset)`, and gitoxide's `gix-pack` resolves deltas from range reads.

### 5.4 `Materializer` (implemented in GT4)

```rust
/// Cone-mode sparse-checkout paths ("src/api", "docs"); empty = the whole tree.
pub struct Scope { pub repo: RepoId, pub commit: ObjectId, pub cones: Vec<RepoPath> }
pub struct AgentId(pub String);
pub struct Touched { pub tree: Arc<TreeManifest>, pub fetched_blobs: u64, pub fetched_bytes: u64, pub cached_blobs: u64 }

#[async_trait]
pub trait Materializer: Send + Sync + fmt::Debug {
    /// Fetches every tree of `scope.commit` (the full path listing) and the blobs inside `scope.cones`
    /// that `agent`'s cache lacks. Idempotent. Reads are grouped by pack and coalesced into ranges of
    /// at least 1 MiB: never one GET per blob (§15 §12).
    async fn touch(&self, scope: &Scope, agent: &AgentId) -> Result<Touched, MatError>;
    /// One blob, fetched on demand; `OutOfScope` for a path outside the cones.
    async fn read(&self, scope: &Scope, agent: &AgentId, path: &RepoPath) -> Result<Bytes, MatError>;
}
```

The agent cache is an `Fs` (§17): `NativeFs` on a sandbox host's NVMe (other backends belong to a commercial Cloudflare target (`loam-platform`)). It is shared by every agent on a host (§15 principle 4); the `agent` argument scopes metering and quotas, not storage.

## 6. Access paths

### 6.1 Smart HTTP (D394, GT2)

Routes in the `gateway` role, loopback-only until the unified auth plan (D111, Q30; Q389):

| Route | Protocol |
|---|---|
| `GET /git/<ns>/<repo>.git/info/refs?service=git-upload-pack` with `Git-Protocol: version=2` | v2 capability advertisement: `agent=loams/<version>`, `ls-refs=unborn`, `fetch=shallow wait-for-done filter`, `server-option`, `object-format=sha1`, `object-info` |
| `POST /git/<ns>/<repo>.git/git-upload-pack` | v2 commands `ls-refs`, `fetch`, `object-info` (gitprotocol-v2) |
| `GET …/info/refs?service=git-receive-pack` | v0 advertisement with `report-status report-status-v2 delete-refs side-band-64k quiet atomic ofs-delta push-options object-format=sha1 agent=loams/<version>` |
| `POST …/git-receive-pack` | commands, then the pack (gitprotocol-pack, gitprotocol-http) |

HTTP/2 is accepted where the client negotiates it (git over libcurl with `http.version=HTTP/2`). `packfile-uris` (pre-signed bucket URLs for whole-pack reuse on clone) is a GT2 option, worthwhile on R2 because R2 egress is free.

**Fetch.** Negotiation walks commits with a commit graph (the compaction's `commit-graph`, or one built on demand and cached in H2). The pack is assembled from stored entries with `gix-pack`'s output pipeline, reusing deltas whose base is also sent ("pack copy" mode), and whole stored packs are streamed when the wants cover them. `filter=blob:none` and `tree:0` drop blobs or trees. `shallow`/`deepen` follow gitprotocol-v2.

**Push.** §6.3.

**Why gitoxide primitives and not `git upload-pack` on every request.** The serving path must run on nodes with no local repository (and stay portable to Workers, for a commercial Cloudflare target (`loam-platform`)), with object reads that go through range GETs and the H1 cache. `gix-pack` builds for `wasm32-unknown-unknown` in gitoxide's CI; the `git` binary does not run in Workers. gitoxide's server-side upload-pack/receive-pack plumbing is still unchecked in its `crate-status.md` (read 2026-10-01), as §15 §3.4 found, so the loop is Loams's. Pack *encoding* without delta compression and *bitmap writing* are also unchecked there, so repacking uses stock `git` (D396), which is GPL-2.0 and runs as an unmodified separate process, never linked, as WeSQL (D148) and PgDog (D236) do.

### 6.2 `git-remote-loams` (D395)

| Phase | Address | Capabilities | What it does |
|---|---|---|---|
| GT1 | `loams::s3://bucket/prefix/ns/<ns>/repos/<repo_id>` (any `object_store` URL; git's `<transport>::<address>` form runs `git-remote-loams`) | `fetch`, `push`, `option` | **Serverless**: lists refs from `RefLog::snapshot(Latest)`; fetches the live packs the local repository lacks and writes each with its stored index into `.git/objects/pack/`; pushes by running `git pack-objects` for the refspecs, writing one `.lpk`, and committing a `RefTxn` through its own `BucketRefLog` (the client is its own sequencer, fenced by §4.2) |
| GT2 | adds `loams://<host>/<ns>/<repo>` | adds `stateless-connect` | Tunnels protocol v2 to the server (or to an in-process upload-pack over the bucket for `loams::` addresses), so `--filter=blob:none`, sparse checkout and lazy fetch of promisor objects work. Whether git drives lazy fetch through a helper's `stateless-connect` (documented as "experimental; for internal use only") is checked first (Q386) |

Credentials: the `object_store` environment (`AWS_*`, `GOOGLE_*`, `AZURE_*`), or a vended token from `loams` (§15 §8). The helper links only Apache-2.0/MIT code and runs `git` subcommands as processes, as every remote helper does.

### 6.3 The receive path (GT2; the helper's push in GT1 uses the same steps)

1. Parse the commands (old, new, ref) and capabilities. Refuse more than `max_refs_per_push` (4096) or a pack over `max_pack_bytes` (2 GiB, configurable).
2. Stream the pack into a spill file in the node's `Fs`, indexing it with `gix-pack` as it arrives (memory bounded by the index, not the pack). Resolve thin-pack bases from the repository's pack set at the snapshot the advertisement came from.
3. Verify: every object hashes to its id; every new tip's closure is present in the new pack plus the pack set (connectivity); fast-forward unless the update is forced and allowed; ref names valid; protections; the token's scope (D397).
4. PUT `packs/<checksum>.lpk` (create-only, D390). An empty push (deletes only) has no pack.
5. Queue a `RefTxn`: one for an `atomic` push, else one per ref, all in the same group, so each ref gets its own status.
6. Answer `report-status`/`report-status-v2` after the segment commits, with the sequence number in a `loams-seq=<n>` progress line on side-band 2.

A pack older than the GC grace period (1 h, §03 §7) at commit is refused (`Stale`), so a long push cannot rely on a pack that compaction retired meanwhile.

### 6.4 `loams-vfs` and per-folder scopes (D397, GT4, not yet planned)

`loams-vfs` is the lazy lower layer of §15 §5.1's `/workspace`, not a second filesystem design:

- **Mount.** A FUSE (or virtiofs, §15 Q3) filesystem over a `Materializer`: the tree of the agent's commit is listed at mount, blobs are fetched on first read or write, and writes go to the local upper layer (overlay).
- **Scopes.** The agent's vended token (§15 §8) names its cones. Paths outside the cones are listed but unreadable (`EACCES`), and the receive path and `RefLog` refuse a commit that changes a path outside them (`OutOfScope`). This is write admission for agents, enforced on the server, not trust in the client.
- **Commit.** A checkpoint (§15 §9) hashes the upper layer into blobs and trees, writes one `.lpk` with the new objects and commits one `RefTxn` on the agent's branch. A commit that touches several folders is still one WAL record and one sequence number: the draft's "cross-folder commits use one head-pointer swap" holds with no head pointer.
- **Contention.** Agents work on their own branches (§15 principle 2: one writer per workspace), so they contend only in the sequencer, which group-commits them. The draft's per-scope WAL partitions would split one repository's ordering, and with it atomic cross-folder commits, to remove a contention point that group commit already removes. They are not built until GT4 measures the sequencer as the bottleneck (Q391). A server-side merge of disjoint-path commits into a shared branch (a merge queue) is a separate question (Q392).

## 7. Compaction and GC (D396)

| Job | When | How |
|---|---|---|
| **Checkpoint** | every 256 segments, or when replay since the last checkpoint exceeds 8 MiB | The sequencer writes `checkpoints/<seq>.lgc` from its state; the hint names it |
| **Repack** | more than 16 packs above the geometric progression (git's `--geometric=2`), or on demand | A worker task under the lease `task/git-compact/<ns>/<repo_id>` (D30's pattern) hydrates a local mirror (the `.lpk` files split into pack and idx), runs `git repack --geometric=2 -d --write-midx --write-bitmap-index` and `git commit-graph write --reachable`, uploads the new packs as `.lpk`, the `midx/` and `commit-graph/` objects, then commits a `PackSetChange` through the sequencer. Readers see the old or the new pack set, never a mix |
| **Segment GC** | daily | Segments below the oldest checkpoint still needed for `Exactly` reads (24 h) are deleted; segments above a gap after the grace period |
| **Pack GC** | daily | A pack removed by a `PackSetChange` is deleted once no checkpoint within retention and no fork at a seq that still names it references it, after the grace period, with a GC claim (§03 §7, D59) |

Only one compactor runs per repository, and never on the push path. The sequencer's owner schedules it as a worker task (§09), because repacking is CPU-heavy (a commercial Cloudflare target (`loam-platform`) runs it in a container). Continuity reports compaction as its bottleneck at the top rate (§3), so GT2 measures repack time per pushed MiB.

## 8. Build cache (D398, GT3)

### 8.1 Tools

| Tool | Licence, version | Verdict |
|---|---|---|
| **sccache** | Apache-2.0, v0.18.0 (2026-09-14) | **Primary.** Wraps C/C++, Rust and CUDA compilers. Backends: local disk, S3, R2, Redis, Memcached, GCS, Azure, GitHub Actions, WebDAV, Alibaba OSS, Tencent COS. A multi-level cache (`SCCACHE_MULTILEVEL_CHAIN`, docs/MultiLevel.md, added 2026-04-17, first released in v0.16.0 (verify)); read-only modes per backend (`SCCACHE_S3_RW_MODE`, `SCCACHE_WEBDAV_RW_MODE`, `SCCACHE_LOCAL_RW_MODE`, `rw_mode`); `SCCACHE_BASEDIRS` since v0.14.0 (source) |
| **BuildCache** | zlib, v0.33.1 (2026-09-23; moved to gitlab.com/bits-n-bites/buildcache) | Optional. C/C++ and rustc; local cache plus an `http`, `redis` or `s3` second level. Overlaps sccache; adopt only if a toolchain sccache cannot handle needs it |
| **mozilla-actions/sccache-action** | Apache-2.0, v0.0.11 | Wires sccache to GitHub's cache on GitHub-hosted runners only; Loams's endpoint replaces it elsewhere |
| bazel-remote, REAPI | Apache-2.0 | §15 §7 stands: bazel-remote on the bucket now, Loams's REAPI CAS in W3 |

Known sccache limits, from its README: crates that invoke the linker (`bin`, `dylib`, `cdylib`, `proc-macro`) and incrementally compiled crates are not cached; absolute paths must match unless `SCCACHE_BASEDIRS` is set.

### 8.2 Two ways in

| Path | sccache config | Loams code on the path | What it gives |
|---|---|---|---|
| **Direct** | S3 backend: `SCCACHE_BUCKET`, `SCCACHE_ENDPOINT` (R2: `https://<account>.r2.cloudflarestorage.com`, region `auto`), `SCCACHE_S3_KEY_PREFIX=ns/<ns>/cache/sccache/<repo>/trusted/`, vended credentials; `SCCACHE_S3_RW_MODE=READ_ONLY` for untrusted builds | none (§15 §7's "no Loams code" path) | Isolation by credential scope: R2 temporary credentials take `prefixes` and `object-read-only`, up to 7 days (Cloudflare API, read 2026-10-01); RustFS and S3 through `ObjectStoreProvider::issue_credentials` (§25 §5). Eviction by age since write only |
| **Gateway** | WebDAV backend: `SCCACHE_WEBDAV_ENDPOINT=https://<gateway>/cache/sccache/<ns>/<repo>/`, a token | `loams-buildcache` in the `gateway` role | Hit, miss and put counts (§11); quotas; trust enforced on the server; **approximate LRU**: a hit on an entry older than `refresh_after` (7 days) rewrites its timestamp with an in-place copy, at most one copy per entry per week. The WebDAV subset sccache needs is checked first (Q394) |

CI templates (GT3) set `RUSTC_WRAPPER=sccache`, `CARGO_INCREMENTAL=0`, `SCCACHE_BASEDIRS=<workspace root>` and either path's variables. With `SCCACHE_MULTILEVEL_CHAIN=disk,webdav` (or `disk,s3`), a local disk cache sits in front, and on Cloudflare R2 is the edge level; a regional S3 level is a third link only where a deployment has one.

### 8.3 Trust model

**Never share a writable cache between untrusted builds.** A poisoned entry is a compiled object that any later build links.

| Trust class | Who | Reads | Writes |
|---|---|---|---|
| `trusted` | CI on protected branches of the repository; release builds | `trusted/` | `trusted/` |
| `untrusted` | fork and pull-request builds, agent sandboxes, local developer machines | `trusted/` | nothing, or `scratch/<principal>/` when the repository enables it; scratch is never read by another principal |

Keys are `ns/<ns>/cache/sccache/<repo>/<class>/<sccache key>`: per tenant and per repository, so no cross-tenant reads exist to poison, and dedup across repositories is given up deliberately (§15 principle 5). The class comes from the credential: the vending flow (§15 §8) maps a CI job's identity (for example a GitHub OIDC token's `ref` and `repository` claims) to a class. Eviction: a TTL (default 30 days since write or refresh) and a size quota per repository (default 50 GiB), enforced by a sweeper that deletes the oldest entries first.

## 9. Package mirror (D399, GT3)

The crates.io part of §15 §6's registry proxy; PyPI, npm and Go stay in W2.

- **Routes** (gateway role): `GET /registry/crates/<ns>/index/config.json`, `GET /registry/crates/<ns>/index/<path>` (the sparse layout: `1/`, `2/`, `3/<c>/`, `<ab>/<cd>/<name>`), `GET /registry/crates/<ns>/dl/<crate>/<version>/<cksum>`.
- **`config.json`:** `{"dl": "https://<gateway>/registry/crates/<ns>/dl/{crate}/{version}/{sha256-checksum}", "auth-required": true}` and no `api` (no publishing). The `{sha256-checksum}` marker lets downloads go straight to content-addressed storage.
- **Index files** are fetched from `https://index.crates.io/<path>` with the cached ETag (`If-None-Match`), cached under `ns/_public/packages/crates/index/<path>` with a freshness TTL (60 s), and filtered per namespace at serve time.
- **`.crate` files** are fetched once from `static.crates.io`, checked against the index's `cksum` (SHA-256), and stored create-only at `ns/_public/packages/crates/sha256/<cksum>`. The designated public-packages namespace is §15 principle 5's exception to per-namespace dedup.
- **Policy** (§15 §6): allowlists and denylists, version pinning, a quarantine window that hides versions younger than N days (needs a publish time per version: the index's `pubtime` field where present (verify), else the crates.io API), and an audit record per download to the stream `_packages`.
- **Where state lives.** Index files and crates are objects; policy is namespace configuration. The draft's "index in a Durable Object or D1" is not needed: the object store plus H1 serve the index (D399).
- **Buy vs build.** kellnr (Apache-2.0, v6.9.0, 2026-09-23) is a private registry; panamax (Apache-2.0, last release 2024-06-06) and ktra (Apache-2.0) are mirrors or registries with their own storage. None runs in Workers or stores into a Loams namespace, and the proxy is three routes, so it is built; kellnr is the documented choice for a self-hosted private registry with publishing (GT3 Task 0 checks its crates.io proxy and S3 storage).

## 10. Security

- **Tenant from the credential.** The namespace comes from the token, never from the URL alone (D182's rule); a URL naming another namespace is `404`.
- **Scoped, short-lived tokens** (§15 §8): `(namespace, repo, branch prefix, scope cones, cache class, registry read)`.
- **Pack limits** (§6.3) and fsck-light verification bound what a push can store; malformed packs never reach the WAL.
- **No ambient bucket credentials** in sandboxes: agents reach Git, the cache and the mirror through the gateway, or hold vended prefix credentials.
- **Cache poisoning**: §8.3. **Supply chain**: §9's quarantine and audit.

## 11. Usage hooks (D393)

Exported per §27 (Prometheus, or OTLP delta metrics for per-repository cardinality, Q-UH-1), labels `org` and `namespace`:

| Family | Meaning |
|---|---|
| `loams_git_pushes_total{result}` | committed, rejected, replayed |
| `loams_git_received_bytes_total`, `loams_git_sent_bytes_total` | pack bytes in and out |
| `loams_git_stored_bytes` | live pack set, per namespace (gauge, from checkpoints) |
| `loams_git_wal_segments_total`, `loams_git_group_txns` (histogram) | sequencer activity; group size |
| `loams_git_cpu_seconds_total{op}` | `receive`, `upload`, `compact`, from the thread CPU clock around each request (as §27 §3.3 measures T1) |
| `loams_buildcache_requests_total{result}` | `hit`, `miss`, `put`, `denied` (gateway path only) |
| `loams_buildcache_bytes_total{direction}`, `loams_buildcache_stored_bytes` | |
| `loams_packages_requests_total{ecosystem,source}` | `cache`, `upstream`, `denied` |
| `loams_packages_bytes_total{ecosystem}` | |

Storage bytes for the direct cache path come from the namespace's logical-bytes accounting (D103). Turning these into invoices is `loam-platform`'s job (D190, D202).

## 12. Cost sketch (estimate)

R2 prices read 2026-10-01: storage $0.015/GB-month, Class A $4.50 per million, Class B $0.36 per million, no egress fee. One push with a pack costs one `.lpk` PUT plus its share of a segment PUT (1/k at group size k) and a hint write at most once a second: **about $5 per million pushes** on R2, before storage. A clone of a compacted repository is a handful of GETs plus egress, which R2 does not charge. On S3 Standard the same push costs about $5 per million (PUT $0.005 per 1,000, from a search summary; verify) plus egress.

## 13. Phases

| Phase | Scope | Exit gate | Plan |
|---|---|---|---|
| **GT1** | `loams-git`: formats, `BlobStore`, `WalStore`, `Odb`, `BucketRefLog` with group commit and idempotency, checkpoints, forks; `git-remote-loams` (serverless `fetch`/`push`) | Stock git clones, fetches and pushes through `loams::` on RustFS and in-memory; 8 concurrent pushers never lose an acknowledged push; the linearizability checker passes RefLog histories under store faults; pushes/s and push latency measured on RustFS, R2 and S3 | [GT1](../plans/2026-10-01-gt1-wal-git-core.md) |
| **GT2** | Smart HTTP v2 upload-pack, v0/v1 receive-pack, the receive path, negotiation and pack assembly, compaction with stock git, `stateless-connect` in the helper (partial clone, lazy fetch) | §15 W1's client matrix (git, gitoxide, libgit2, JGit): clone, fetch, push, partial clone, shallow; differential tests against `git upload-pack` on the same objects | [GT2](../plans/2026-10-01-gt2-smart-http.md) |
| **GT3** | `loams-buildcache` (WebDAV subset, trust classes, refresh-on-hit, sweeper), the direct-path credential recipe, CI templates; `loams-registry` crates mirror | A cold and a warm `cargo build` of a fixture workspace through each path (the Loams workspace nightly), with the warm hit rate reported; an untrusted build cannot write `trusted/`; `cargo fetch` of the workspace through the mirror with egress limited to Loams | [GT3](../plans/2026-10-01-gt3-build-cache-and-mirror.md) |
| **GT4** | `loams-vfs`, `Materializer`, scopes and write admission (§6.4) | Not yet planned | — |
| **GT5** | Continuity-style NVMe replica caches, gossip as a hint, any-node writes, compaction at scale | Not yet planned | — |

GT1–GT2 are §15 W1's repository scope; GT3 is W2's sccache wiring and the crates part of the registry proxy; GT4 is W2's lazy workspace mount. §15 §13 places W1 after M3. Starting GT1 earlier, as its own track beside M and R like tracks R, D and J, was an owner decision (Q395); on 2026-10-02 the owner kept §15's slot, so GT1 starts with W1 after M3 (D411).

## 14. Risks

| # | Risk | Mitigation |
|---|---|---|
| 1 | **The server-side Smart HTTP effort** is larger than planned (negotiation, shallow, filters, thin packs, every client's quirks) | v2-only upload-pack first; stock `git upload-pack` as the differential oracle; go-git's server as a reference; the client matrix as the gate |
| 2 | **Small-object request costs** on S3 (Class A $5 per million) | One PUT per push for data (`.lpk`), one PUT per group for the WAL, hints coalesced; request counts per operation tracked in CI (§12 §2 item 11: cost regressions are bugs) |
| 3 | **Pack GC and repacking** bugs lose objects | Removal only through `PackSetChange` after the replacement pack is durable; deletion only after grace with GC claims; a nightly `git fsck` of mirrors of sampled repositories |
| 4 | **gitoxide memory in Workers** (128 MB per isolate) | Relevant only to a commercial Cloudflare target (`loam-platform`), which measures it |
| 5 | **Cache poisoning** | §8.3 trust classes, enforced by credential scope or the gateway, never by client configuration |
| 6 | **Unverified third-party throughput** (Continuity's 120 and 300 pushes/s are synthetic) | Loams's own target (30/s) is measured in GT1 on each store |
| 7 | **Segment PUT latency** on R2 or S3 Standard is too high for 30 pushes/s at small group sizes | Group commit grows with load; S3 Express One Zone as an `express`-like class for hot repositories (§02 §2), if its conditional create is confirmed (Q384) |
| 8 | **A direct-writing `git-remote-loams` client** and the server race on one repository | Both are fenced by §4.2; the server re-reads and re-validates; tested in GT1 Task 9 |
| 9 | **Stock git as a process dependency** (GPL-2.0) | Never linked or vendored; invoked as a binary only in the repack worker and tests; recorded in the licence check like PgDog (D236) |
| 10 | **gitoxide API churn** (0.x releases every few weeks) | Pin `gix-*` versions; the core touches gix only behind `Odb` and the protocol module |

## 15. Conflicts with existing decisions, and how they are resolved

| Earlier | The draft or this document | Resolution |
|---|---|---|
| §15 §3.1 (approved): one CAS'd `refs` document per repository | D388, D389: WAL segments plus checkpoints | **Amends §15 §3.1–§3.2** (marked there). The single document is the special case "a checkpoint after every segment"; forks and alternates are unchanged. Needs the owner's approval because §15 is approved |
| §15 §3.1: `packs/<ulid>.pack` + `.idx` | D390: `packs/<checksum>.lpk` | Amends §15 §3.1: one PUT per push and idempotent retries |
| The draft §14.3: "head pointer advances via conditional write" | D389: the segment create is the commit point | R2's one-write-per-second-per-key limit and one fewer PUT per batch |
| The draft §14.4: `append` idempotent on `idempotencykey` | D392: batch-level idempotency in `WalStore`, key-level in `RefLog` | Same guarantee, at the layer that can check it |
| The draft §14.5: per-scope WAL partitions | D397: one WAL per repository; scopes are read and write admission | §15 principle 2 (one writer per workspace) and atomic cross-folder commits; Q391 reopens it on measurement |
| The draft §14.7: metering CloudEvents feed the ledger | D393: §27 hooks only | D190, D200–D202: billing is in `loam-platform` |
| The draft §13.3: package index "in a Durable Object or D1" | D399: in the object store | D1 (object storage is the source of truth); fewer services |
| §15 §7: sccache with "no Loams code" | D398 adds an optional gateway path | The direct path stays code-free; the gateway path is for metering, LRU and server-enforced trust |
| D11: no copyleft dependencies | D394, D396: stock `git` (GPL-2.0) | An unmodified separate process, never linked, as D148 (WeSQL) and D236 (PgDog) |
| §15 §13: W1 after M3 | GT1–GT3 could start earlier | §15's slot kept (Q395, D411) |
| Q15: repos as a sixth object kind | D388: a service with its own bucket WAL | Proposed answer to Q15; a repo's WAL is not a Loams stream, and its events are mirrored into one (§4.8) |
| §15 §16 Q1: protocol v2 only? | D394 | Answered: v2 upload-pack, v0/v1 receive-pack, v0 upload-pack only if a matrix client needs it |
| The draft §2/§3/§4.3: Resonate on TiDB | Not used by Git | D260/D261: Resonate on TiKV; Git needs no durable-execution store |
| The draft §6: event types `io.loams.<domain>.<name>.v1` | `io.loams.dev.git.*.v1` | The owner's ruling of 2026-10-01: the prefix is `io.loams.dev.<domain>.<name>.v1` |

## 16. Open questions

| # | Question | Owner | Needed by |
|---|---|---|---|
| Q384 | ~~Segment PUT latency (p50, p99) on R2, S3 Standard, S3 Express One Zone and RustFS, and whether S3 Express directory buckets honour `If-None-Match: *` on PutObject (AWS documents conditional writes without excluding them; not confirmed)~~ Answered 2026-10-02 by the owner: the plan's default — not an owner decision: GT1 Task 10 measures it; S3 Express is used only if its conditional create is confirmed | Eng | Resolved |
| Q385 | ~~RustFS conditional PUT above 1 MiB: does a create-only PUT of a large `.lpk` stay atomic, or can a reader see a partial object (D390 relies only on equal content, but a torn read must be impossible)~~ Answered 2026-10-02 by the owner: the plan's default — not an owner decision: GT1 Task 3 checks it; until then RustFS refuses content-named blobs over 64 MiB (GT1 Ruling 1) | Eng | Resolved |
| Q386 | ~~Does current git (2.5x) drive partial clone and lazy fetch of promisor objects through a remote helper's `stateless-connect`, or only through native transports~~ Answered 2026-10-02 by the owner: the plan's default — not an owner decision: GT2 Task 0 checks it; `stateless-connect` is the design, and the result goes into GT2 | Eng | Resolved |
| Q387 | ~~Do libgit2 and JGit speak protocol v2 for fetch, or does W1's client matrix require a v0/v1 upload-pack~~ Answered 2026-10-02 by the owner: the recommended default — upload-pack v2 only, unless GT2 Task 0 finds a matrix client without v2, in which case GT2 Task 10 adds v0/v1 (D394) | Eng | Resolved |
| Q388 | ~~SHA-256 repositories: when, given gitoxide's open SHA-256 parity item and that GitHub mirrors are SHA-1~~ Answered 2026-10-02 by the owner: the recommended default — SHA-1 only until gitoxide's SHA-256 parity lands; the object format is already a field in every record (GT1 Ruling 8) | Eng | Resolved |
| Q389 | ~~Authentication before the unified auth plan (D111): loopback only in the open-source gateway until MT1 (§38 D451, PR #182); the Cloudflare variant is a commercial Cloudflare target (`loam-platform`)~~ Answered 2026-10-02 by the owner: the recommended default — loopback only in the open-source gateway until MT1 (§38 D451) | Founder | Resolved |
| Q390 | ~~Is age-since-write eviction good enough for the direct cache path, or must quotas require the gateway path~~ Answered 2026-10-02 by the owner: the plan's default — not an owner decision: GT3 Task 4 measures it; quotas apply on the gateway path | Eng | Resolved |
| Q391 | ~~Per-scope WAL partitions: needed once GT4 measures sequencer contention on a hot monorepo~~ Answered 2026-10-02 by the owner: the recommended default — not now: one WAL per repository (D397), reopened only if GT4 measures sequencer contention | Eng | Resolved |
| Q392 | ~~A server-side merge queue that rebases disjoint-path agent commits onto a shared branch: in GT4, a later phase, or never~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — a later phase after GT4, on demand; why: it keeps GT4's scope, and agents work on their own branches (§15 principle 2) | Founder | Resolved |
| Q393 | ~~Whether the WAL should carry pack bytes for very small pushes (one PUT per push instead of two)~~ Answered 2026-10-02 by the owner: the plan's default — not an owner decision: GT1 Task 10's numbers decide; the default is a separate `.lpk` PUT | Eng | Resolved |
| Q394 | ~~The WebDAV subset sccache's backend (OpenDAL `webdav`) needs: `PROPFIND`, `MKCOL`, `HEAD`, `GET`, `PUT`~~ Answered 2026-10-02 by the owner: the plan's default — not an owner decision: GT3 Task 0 records the subset; the default is `PROPFIND`, `MKCOL`, `HEAD`, `GET` and `PUT` | Eng | Resolved |
| Q395 | ~~Timing: start GT1–GT3 now as track GT, beside M, R, D and J, or keep §15's W1 after M3~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — keep §15's slot: GT1 starts with W1, after M3 (D411); why: the doc gives no reason to start early, and the one-build machine already carries tracks M, R, D, J, CLI, FL, AP and RT | Founder | Resolved |

## 17. The `Fs` trait and object-store providers (D381, D382; moved from the former §35)

When the Cloudflare target moved to `loam-platform` (2026-10-02, §38 D440), its two vendor-neutral decisions stayed here, because Loams Git needs them.

**D381: no Loams-built S3 service.** RustFS is the store where Loams runs its own storage (D61, D178); any S3-compatible service (S3, R2, Cellar, MinIO) is a provider behind `ObjectStoreProvider` (§25 §5), and `loams-store` stays the client. Tenants are isolated by **vended, prefix-scoped credentials** (`ObjectStoreProvider::issue_credentials`); on R2 these are temporary credentials with `prefixes` and `object-read-only` or `object-read-write`, up to 7 days, which GT3's `r2` vendor requests. D178's provider list gains `r2`.

**D382: an `Fs` trait for warm, rebuildable state and scratch.** Loams code that keeps spill buffers (a pack arriving on a push), local caches (pack indexes, scope caches, §5.4) or small mutable metadata (a sequencer's idempotency index) uses `Fs`, so the same code can run where there is no `std::fs`. **Durable truth stays in the object store behind conditional writes (D1)**; an `Fs` is a cache and scratch, never the source of truth.

### 17.1 Where it lives

`crates/loams-fs` (Apache-2.0, no cloud dependency) holds the trait, `NativeFs` (`tokio::fs` under a root directory; `rename(2)`; fsync of file and directory when `survives_restart`; Kubernetes, Lambda `/tmp` (not surviving), laptops) and `MemFs` (tests, and the conformance suite's reference). Cloudflare backends (Durable Object SQLite, R2 blobs) are part of the commercial Cloudflare target in `loam-platform`. One conformance suite, `loams-fs::conformance`, runs against every backend, with capability flags selecting the append and rename cases: read-after-write, `CreateNew` races, range reads at boundaries, list pagination, rename atomicity under a crash, and the limits.

### 17.2 The trait

`crates/loams-fs` (Apache-2.0, no Cloudflare dependency):

```rust
/// Relative, '/'-separated, no empty, '.' or '..' segments, at most 1,024 bytes (R2's key limit).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FsPath(String);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteMode { Overwrite, CreateNew }

#[derive(Clone, Debug)]
pub struct FsMeta { pub len: u64, pub modified_unix_ms: i64 }

#[derive(Clone, Copy, Debug)]
pub struct FsCaps {
    pub append: bool,          // NativeFs, MemFs (and the commercial Durable Object backend)
    pub atomic_rename: bool,   // NativeFs (rename(2)), MemFs
    pub max_file: u64,         // NativeFs: disk; MemFs: configured
    pub survives_restart: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum FsError {
    #[error("not found: {0}")] NotFound(FsPath),
    #[error("already exists: {0}")] AlreadyExists(FsPath),
    #[error("unsupported on this backend: {0}")] Unsupported(&'static str),
    #[error("too large: {0} bytes")] TooLarge(u64),
    #[error("backend: {0}")] Backend(String),
}

#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]   // Workers futures are !Send
pub trait Fs: fmt::Debug {
    fn caps(&self) -> FsCaps;
    async fn read(&self, path: &FsPath) -> Result<Bytes, FsError>;
    async fn read_range(&self, path: &FsPath, range: Range<u64>) -> Result<Bytes, FsError>;
    /// `CreateNew` fails with `AlreadyExists`; a returned write is visible to every later read.
    async fn write(&self, path: &FsPath, data: Bytes, mode: WriteMode) -> Result<(), FsError>;
    /// Returns the new length. `Unsupported` on blob-only backends.
    async fn append(&self, path: &FsPath, data: Bytes) -> Result<u64, FsError>;
    /// Atomic replace of `to`. `Unsupported` on blob-only backends.
    async fn rename(&self, from: &FsPath, to: &FsPath) -> Result<(), FsError>;
    async fn remove(&self, path: &FsPath) -> Result<(), FsError>;   // NotFound is Ok
    async fn stat(&self, path: &FsPath) -> Result<Option<FsMeta>, FsError>;
    /// Paths under `dir`, sorted, strictly after `after`, at most `limit`.
    async fn list(&self, dir: &FsPath, after: Option<&FsPath>, limit: usize)
        -> Result<Vec<(FsPath, FsMeta)>, FsError>;
}
```


## 18. Sources

Read on 2026-10-01 unless noted.

- The owner's draft "Loams Serverless Runtime — Consolidated Plan" (2026-09-30), §13–§15 (`chatdump.md` lines 814–946).
- Repository: §01 §5, §02 §2–§3 and §7.4 (D270), §03 §6–§7, §04, §09, §15 (all), §18 §5.3 (D75), §24, §25 §5 (`ObjectStoreProvider`), §27; `crates/loams-store/src/store.rs` (`put_if_absent`, `put_if_match`, `get_range`), `crates/loams-cloudevents`, `crates/loams-meta-conformance/src/linearizability.rs`, `crates/loams-log/src/wal.rs` (format conventions).
- Cursor: "Git at any scale", Vicent Martí, https://cursor.com/blog/git-at-any-scale (2026-08). InfoQ: "Cursor Uses S3 WAL to Scale Git Storage to More than 300 Pushes per Second", Leela Kumili, https://www.infoq.com/news/2026/09/cursor-continuity-git-storage/ (2026-09-30).
- GitHub: "Introducing DGit", https://github.blog/news-insights/the-library/introducing-dgit/ (2016-04-05); "Stretching Spokes", https://github.blog/engineering/infrastructure/stretching-spokes/ (2017-10-13, updated 2025-06-03).
- Git: https://git-scm.com/docs/protocol-v2 (`ls-refs`, `fetch` with `filter`, `packfile-uris`, `wait-for-done`; `object-info`; no push command), https://git-scm.com/docs/gitprotocol-http, https://git-scm.com/docs/gitremote-helpers (`connect`, `stateless-connect` "experimental; for internal use only", `fetch`, `push`, `option from-promisor`).
- gitoxide: https://github.com/GitoxideLabs/gitoxide `crate-status.md` (server-side upload-pack/receive-pack plumbing, delta compression in pack encoding, bitmap writing, commit-graph writing and partial clone unchecked; MIDX read/write done), `.github/workflows/ci.yml` (`wasm` job: `gix-pack`, `gix-commitgraph`, `gix-hash` on `wasm32-unknown-unknown`); `gix` 0.88.0 (2026-09-25), MIT OR Apache-2.0.
- go-git: https://github.com/go-git/go-git (Apache-2.0; `plumbing/transport/{upload_pack.go,receive_pack.go}`; v5.19.2, 2026-07-29).
- Object stores: AWS "Amazon S3 now supports conditional writes" (2024-08-20) and "… functionality for conditional writes" (2024-11-25); https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html; https://developers.cloudflare.com/r2/api/s3/api/ (`If-Match`, `If-None-Match` on PutObject; updated 2026-07-31), https://developers.cloudflare.com/r2/platform/limits/ ("Maximum concurrent writes to the same object name (key): 1 per second"; updated 2026-06-08), https://developers.cloudflare.com/r2/reference/consistency (strong read-after-write and listing), https://developers.cloudflare.com/r2/pricing/ (updated 2026-10-01), Cloudflare API `POST /accounts/{account_id}/r2/temp-access-credentials` (`permission`, `prefixes`, `objects`, `ttlSeconds` ≤ 604800); https://docs.cloud.google.com/storage/docs/request-preconditions (updated 2026-09-30); Microsoft "Specifying conditional headers for Blob service operations" (updated 2026-01-20); RustFS https://github.com/rustfs/rustfs (Apache-2.0; 1.0.1-preview.14, 2026-09-30; PRs #6421, #6798, #6801).
- sccache: https://github.com/mozilla/sccache (Apache-2.0; v0.18.0, 2026-09-14; README limits; docs/MultiLevel.md; docs/Configuration.md, docs/S3.md, docs/Webdav.md). mozilla-actions/sccache-action (Apache-2.0, v0.0.11). BuildCache: https://gitlab.com/bits-n-bites/buildcache (zlib; v0.33.1, 2026-09-23).
- Cargo: https://doc.rust-lang.org/cargo/reference/registry-index.html (sparse layout, `config.json` `dl` markers including `{sha256-checksum}`, `auth-required`, `cksum`). kellnr (Apache-2.0, v6.9.0, 2026-09-23), panamax (Apache-2.0, v1.0.14, 2024-06-06), ktra (Apache-2.0).
