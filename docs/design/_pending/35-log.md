# Pending log for §36 (Loam Git); the former §35 (Cloudflare target) moved to `loam-platform`

For the integrator. Branch `cloudflare-git-design`. **Amended 2026-10-02:** the owner ruled "move Cloudflare, OpenRTB etc. commercial to private repos" (§38 D440, PR #182). §35 and plan CF1 were removed from this branch and are designed in `loam-platform`; their rows (D380, D383–D387, Q380–Q383, Q396–Q399) are removed below and must not be integrated. D381 and D382 stay, re-homed in §36 §17. The numbers below as originally written: Reserved numbers: **D380–D399** and **Q380–Q399**; all twenty decision numbers and all twenty question numbers are used (D380–D387 and Q380–Q383, Q396–Q399 for §35; D388–D399 and Q384–Q395 for §36). Paste the rows below into `docs/design/13-decision-log.md`, `docs/design/README.md`, `docs/plans/README.md`, `docs/design/12-roadmap-testing-risks.md` and `docs/design/11-buy-vs-build.md`. Owner rulings of 2026-10-01 relayed by the orchestrator are applied in the docs: the domain is `loams.dev`, packages are `loams` (`@loams` on npm), Go modules are `loams.dev/...` (a `go-import` vanity responder on `loams.dev` answering `?go-get=1`; how it is hosted is the private site's concern), Java is deferred, and CloudEvents types use `io.loams.dev.<domain>.<name>.v1`.

## 1. Decision rows

Append to the Decisions table, after the last row on `main` at merge time:

| ID | Date | Decision | Rationale | Status |
|---|---|---|---|---|
| D381 | 2026-10-01 | **No Loam-built S3 service** (§36 §17): the draft's "Rust S3-compatible service" is RustFS where Loam runs its own storage (D61, D178) and R2 on Cloudflare; `operon-store` stays the client; tenants are isolated by vended, prefix-scoped credentials (`ObjectStoreProvider::issue_credentials`, §25 §5), on R2 by temporary credentials (`POST /accounts/{id}/r2/temp-access-credentials` with `prefixes` and `object-read-only`/`object-read-write`, `ttlSeconds` ≤ 604800). D178's provider list gains `r2`. Extends D178 | R2 already speaks the S3 API; a facade would duplicate SigV4, multipart and conditional writes for no gain; buy over build | Proposed |
| D382 | 2026-10-01 | **The `Fs` trait** (§36 §17, crate `operon-fs`): `read`, `read_range`, `write` (`Overwrite`/`CreateNew`), `append`, `rename`, `remove`, `stat`, `list`, `caps`; `?Send` on `wasm32`. Backends `NativeFs`, `MemFs`, `DoSqliteFs` (Durable Object SQLite, 1 MiB chunks, one transaction per mutation) and `R2Fs` (immutable blobs), with one conformance suite. `Fs` holds warm, rebuildable state and scratch only: **Durable Object storage is a cache and a lease, never the source of truth** | D1; code that runs in Workers and natively needs one file abstraction; thin mode has no `std::fs` | Proposed |
| D388 | 2026-10-01 | **Loam Git extends §15 §3** (§36 §1, §4): a repository's refs move from one CAS'd `refs` document to a per-repository WAL of create-only segments plus checkpoints under `ns/<ns>/repos/<repo_id>/`; packs stay immutable and content-addressed; forks stay O(1) (checkpoint 0 names the parent and seq). Repos are a service with their own bucket WAL, not a sixth object kind and not a Loam stream (proposed answer to Q15). **Amends §15 §3.1–§3.2.** Track GT (GT1–GT5) carries W1's repository scope and parts of W2 | A single document rewritten per push costs O(refs) and contends on one key; agent fleets create thousands of branches | Proposed (amends an approved doc; needs the owner) |
| D389 | 2026-10-01 | **The commit point is the create-only PUT of the next segment**, `wal/<seq:020>.lgw` with `If-None-Match: *` (§36 §4.2); no CAS'd head pointer; `head` is a hint written ≤ 1/s; readers stop at the first missing number; a linearizable read is one GET past the reader's state. Holds on S3, R2, GCS (`ifGenerationMatch=0`), Azure and RustFS (atomic ≤ 1 MiB, so segments are capped at 1 MiB) | R2 allows one write per second per key (R2 limits, 2026-06-08), which would cap a CAS'd head at one commit a second; one fewer PUT per batch; Delta Lake's log uses the same protocol | Proposed |
| D390 | 2026-10-01 | **Git formats** (§36 §4.3): a WAL record is a CloudEvent 1.0 in protobuf (D270) with `tenantid`, `idempotencykey`, `traceparent`, `schemaversion`, `loamseq`, types `io.loams.dev.git.{reftxn,packset,config}.v1`, data `loam.git.v1.{RefTransaction,PackSetChange,ConfigChange}`; segments frame one `CloudEventBatch` (`LGITWAL\0`, version 1, seq, batch id, CRC32C, `LGITWEND`), ≤ 1 MiB; checkpoints (`LGITCKPT`) hold refs, symrefs, protections, the pack set, the fork parent and the idempotency window; a push is one object `packs/<checksum>.lpk` (pack ‖ idx ‖ 32-byte footer). Amends §15 §3.1's `packs/<ulid>.pack` + `.idx` | The draft's CloudEvents envelope; §03 §6's magic-and-version rule; one data PUT per push and idempotent retries | Proposed |
| D391 | 2026-10-01 | **One sequencer per repository with group commit** (§36 §4.4): the rendezvous owner of `(ns, repo, repo_id)` (D75) under the lease `task/git-seq/<ns>/<repo_id>` elsewhere; one segment PUT in flight, arrivals form the next group (≤ 64 transactions, ≤ 1 MiB); validation of object ids, protections and scopes in memory; pack upload, fast-forward and connectivity checks before queueing. Correctness never depends on one sequencer: the segment name fences every writer, including direct-to-bucket helpers. Target 30 pushes/s per hot repository (draft) | Continuity's batching and primary-only writes, without gossip or replicas | Proposed |
| D392 | 2026-10-01 | **Four traits in `operon-git`** (§36 §5): `WalStore` (fenced, monotonic, contiguous; idempotent on the batch id), `RefLog` (`commit` atomic and linearizable, `cas_ref`, `snapshot(Latest/AtLeast/Exactly)`, `watch`; idempotent per key within a 1 h window, `IdempotencyMismatch` on a reused key with another digest), `BlobStore` (create-only content-named `put`, `put_stream`, `get_range`, `head`; deletion only by GC) and `Materializer` (`touch(scope, agent)`, `read`; cone-mode scopes; coalesced range reads, never a GET per blob) | The draft §14.4, with idempotency split between the layer that can check it: batches in the store, keys in the ref log | Proposed |
| D393 | 2026-10-01 | **Git, build-cache and mirror usage through §27's hooks only** (§36 §11): metric families `loam_git_*`, `loam_buildcache_*`, `loam_packages_*` per `org` and `namespace`; a CloudEvent per committed transaction mirrored to the namespace stream `_git` (at least once, repair sweep). No meter events or ledger in this repository. Amends the draft §14.7 | D190, D200–D202 | Proposed |
| D394 | 2026-10-01 | **Smart HTTP** (§36 §6.1): upload-pack speaks protocol v2 (`ls-refs`, `fetch` with `filter`/`shallow`/`wait-for-done`, `object-info`); v0/v1 upload-pack only if a W1 matrix client lacks v2 (Q387); receive-pack speaks v0/v1 (v2 has no push) with `report-status`, `report-status-v2`, `atomic`, `delete-refs`, `side-band-64k`, `ofs-delta`, `push-options`, `quiet`. The server loop is Loam's on gitoxide primitives (`gix-pack` builds for `wasm32-unknown-unknown`); stock `git` (GPL-2.0) runs only as an unmodified separate process: as the test oracle, the repack worker, and the `git pack-objects`/`index-pack` steps of `git-remote-loam`'s pushes on the client (D395). Answers §15 Q1 | gitoxide's server-side upload-pack/receive-pack plumbing, delta compression and bitmap writing are unchecked in its `crate-status.md` (read 2026-10-01); the serving path must run in Workers | Proposed |
| D395 | 2026-10-01 | **`git-remote-loam`** (§36 §6.2): GT1 is a serverless helper over the bucket (`fetch`, `push`, `option`; `loam::<store-url>/ns/<ns>/repos/<repo_id>` addresses; whole-pack fetch with stored indexes; `git pack-objects`/`index-pack` for pushes; each push process is its own fenced sequencer). GT2 adds `stateless-connect` (v2 to an in-process upload-pack or a server) and `loam://<host>/<ns>/<repo>` addresses, for partial clone and lazy fetch | Phase 1 of the draft needs no server; `stateless-connect` is how v2 features reach a helper | Proposed |
| D396 | 2026-10-01 | **Compaction off the push path** (§36 §7): checkpoints every 256 segments or 8 MiB of replay; `git repack --geometric=2 -d --write-midx --write-bitmap-index` and `git commit-graph write` on a worker's NVMe mirror under the lease `task/git-compact/<ns>/<repo_id>`, committed as a `PackSetChange` through the sequencer; segment GC after 24 h `Exactly` retention; pack GC by fork-family reachability after the §03 §7 grace with GC claims. On Cloudflare a Durable Object alarm schedules it in a Container | Continuity reports compaction as its bottleneck; gitoxide cannot yet write deltas or bitmaps | Proposed |
| D397 | 2026-10-01 | **`loam-vfs` is §15 §5.1's `/workspace` lower layer** (§36 §6.4, GT4): cone-mode scopes in the vended token; fetch on first read; server-side write admission refuses commits outside the scope (`OutOfScope`); a commit is one WAL record whatever folders it touches; **no per-scope WAL partitions** until GT4 measures the sequencer as the bottleneck (Q391) | §15 principle 2 (one writer per workspace); per-scope partitions would break atomic cross-folder commits to fix contention group commit already removes | Proposed |
| D398 | 2026-10-01 | **Build cache: sccache** (Apache-2.0, v0.18.0) (§36 §8): a **direct** path (sccache's S3 backend on R2, RustFS or S3 with vended prefix credentials; no Loam code) and a **gateway** path (sccache's WebDAV backend against `operon-buildcache`: hit/miss/put metering, refresh-on-hit approximate LRU, TTL and quota sweeper, server-enforced trust). Keys `ns/<ns>/cache/sccache/<repo>/<class>/`; **trusted** classes (protected-branch CI) write `trusted/`; **untrusted** (forks, PRs, agent sandboxes) read only, with an optional private `scratch/<principal>/`. BuildCache (zlib) only on demand; sccache-action only on GitHub-hosted runners. Amends §15 §7 | Cache poisoning across trust boundaries; R2 temporary credentials scope by prefix and read-only permission; sccache has multi-level caches and read-only modes | Proposed |
| D399 | 2026-10-01 | **Crates mirror** (§36 §9, `operon-registry`): a crates.io sparse-index read-through in the gateway role; `config.json` with `dl` using `{sha256-checksum}` and `auth-required: true`; index files cached with ETag revalidation (60 s TTL) and filtered per namespace at serve time; `.crate` files verified by `cksum` and stored once in `ns/_public/packages/crates/sha256/`; §15 §6 policy (allow/deny, pins, quarantine) and an audit CloudEvent per download (`io.loams.dev.packages.download.v1`) to `_packages`. The index lives in the object store, not a Durable Object or D1 | D1; three routes are smaller than adapting kellnr, panamax or ktra, none of which runs in Workers or stores into a Loam namespace | Proposed |

## 2. Open-question rows

Append to the Open questions table:

| # | Question | Owner | Needed by |
|---|---|---|---|
| Q384 | Segment PUT latency on R2, S3 Standard, S3 Express One Zone and RustFS, and whether S3 Express directory buckets honour `If-None-Match: *` (§36 §4.4) | Eng | GT1 Task 10 |
| Q385 | RustFS conditional PUT above 1 MiB: can a reader ever see a partial large object (§36 §4.2) | Eng | GT1 Task 3 |
| Q386 | Does current git drive partial clone and lazy fetch through a remote helper's `stateless-connect` (§36 §6.2) | Eng | GT2 Task 0 |
| Q387 | Do libgit2 and JGit fetch over protocol v2, or does W1's client matrix need a v0/v1 upload-pack (§36 §6.1, D394) | Eng | GT2 Task 0 |
| Q388 | When to support SHA-256 repositories, given gitoxide's open SHA-256 parity item (§36 §2.2) | Eng | After GT2 |
| Q389 | Git authentication before the unified auth plan (D111): loopback only in the open-source gateway until MT1 (§38 D451, PR #182) (§36 §6.1) | Founder | GT2 Task 0 |
| Q390 | Is age-since-write eviction enough for the direct cache path, or must quotas require the gateway path (§36 §8.2) | Eng | GT3 results |
| Q391 | Per-scope WAL partitions, if GT4 measures sequencer contention on a hot monorepo (§36 §6.4, D397) | Eng | GT4 plan |
| Q392 | A server-side merge queue that rebases disjoint-path agent commits onto a shared branch: GT4, later, or never (§36 §6.4) | Founder | GT4 plan |
| Q393 | Should the WAL carry pack bytes for very small pushes, saving the separate `.lpk` PUT (§36 §16) | Eng | GT1 results |
| Q394 | The WebDAV subset sccache's backend needs (`PROPFIND`, `MKCOL`, `HEAD`, `GET`, `PUT`) (§36 §8.2) | Eng | GT3 Task 0 |
| Q395 | Start GT1–GT3 now as track GT beside M, R, D and J, or keep §15's W1 after M3 (§36 §13) | Founder | Before GT1 |

Existing questions to annotate (edit in place):

- **Q15** (repos as a sixth object kind): append "Proposed answer 2026-10-01 (D388, §36): a service with its own bucket WAL; events mirrored into the `_git` stream."

## 3. README and roadmap rows

### `docs/design/README.md`, Reading order table (after the §34 row)

| 36 | [Loam Git](36-loam-git.md) | Extends §15 §3: a per-repository WAL of create-only segments on the bucket (no head pointer; R2's one-write-per-second-per-key limit), CloudEvents records, checkpoints, one-object packs, a group-committing sequencer per repository (Durable Object or rendezvous owner); `WalStore`, `RefLog`, `BlobStore`, `Materializer`; Smart HTTP (v2 upload-pack, v0/v1 receive-pack) on gitoxide primitives with stock git as oracle and repacker; `git-remote-loam`; `loam-vfs` scopes with write admission; the sccache cache (direct and gateway paths, trust classes); the crates mirror; track GT (D388–D399) | **Proposed** (amends §15 §3, approved) |

### `docs/plans/README.md`, a new section (after Track J or the last track section)

```markdown
## Track GT: Loam Git, the build cache and the crates mirror

Design references: [36 Loam Git](../design/36-loam-git.md) (D388–D399), extending [15 Agent workspaces](../design/15-agent-workspaces.md) §3, §6 and §7; [36 §17](../design/36-loam-git.md) (D381, D382). Track GT carries §15's W1 repository scope and parts of W2. Whether it starts now or in W1's slot after M3 is the owner's decision (Q395). Like tracks R, D and J it interleaves on the one-build machine.

| Plan | Scope | Depends on | Status |
|---|---|---|---|
| [GT1: The WAL git core and `git-remote-loam`](2026-10-01-gt1-wal-git-core.md) | `operon-git`: `loam.git.v1` formats, `BlobStore`, `WalStore`, a pack-cache `Odb`, the ref state machine with idempotency, `BucketRefLog` with group commit and fencing, checkpoints, forks, segment GC; `git-remote-loam` (serverless `fetch`/`push`); linearizability and fault runs; push-rate benchmarks per store | — | Planned |
| [GT2: Smart HTTP for stock git](2026-10-01-gt2-smart-http.md) | v2 upload-pack (`ls-refs`, negotiation, shallow, filters, `object-info`) on a range-read object database; v0/v1 receive-pack with streaming verification; owner forwarding; compaction with stock `git repack` and pack GC; `stateless-connect` in the helper; the git/gitoxide/libgit2/JGit matrix and the upload-pack differential suite | GT1 | Planned |
| [GT3: The sccache backend and the crates mirror](2026-10-01-gt3-build-cache-and-mirror.md) | `operon-buildcache` (WebDAV subset, trust classes, refresh-on-hit, sweeper, hooks), the direct path's credential vending (R2 temporary credentials), CI templates; `operon-registry` (sparse-index read-through, content-addressed crates, policy, audit); cargo and sccache end to end; (Worker variants moved to `loam-platform`) says so | GT1 Task 1 | Planned |
| GT4 | `loam-vfs` and the `Materializer`: per-folder scopes, fetch on first read, write admission, checkpoints as commits | GT2, W2's sandbox agent | Not yet planned |
| GT5 | Continuity-style NVMe replica caches, gossip as a hint, any-node writes, compaction at scale | GT2 results | Not yet planned |
```

### `docs/design/12-roadmap-testing-risks.md`

1. **§1 Milestones**, a new row after **W**:

| **GT** | Loam Git, build cache and crates mirror (§36), parallel track | GT1 (bucket WAL git core with group commit, `git-remote-loam`); GT2 (Smart HTTP v2 for stock clients, compaction, partial clone); GT3 (sccache direct and gateway paths with trust classes, the crates.io mirror); GT4 (`loam-vfs` scopes) and GT5 (replica caches, gossip) not yet planned. Carries W1's repository scope and parts of W2 | GT1: no lost acknowledged push under faults and 8 concurrent pushers, linearizable `RefLog` histories, ≥ 30 pushes/s per hot repository at 100 ms PUT latency; GT2: the git/gitoxide/libgit2/JGit matrix and the upload-pack differential; GT3: warm hit rate ≥ 90% of cacheable compilations, untrusted builds cannot write the trusted cache, `cargo fetch` with egress limited to Loam |

2. **§1, row W**: append "W1's repositories and W2's sccache wiring, crates proxy and lazy workspace mount are delivered by track GT (§36); W1's remaining scope (code-index links, credential vending, agent telemetry, the MCP gateway, session workflows) is unchanged."

3. **§3 Risk register**, new rows:

| 33 | Scope: track GT adds a Git server, a WAL, a build cache and a registry mirror | Medium | High | Buy sccache and stock git (as processes); one WAL protocol for every writer; GT1 ships a serverless helper before any server; GT4–GT5 stay unplanned until GT2's measurements |

4. **§2 Testing strategy**, item 2 (object-store fault injection): append "Track GT's `WalStore` and `RefLog` run under the same `FaultyStore`, with the linearizability checker over ref-log histories (GT1)."

### `docs/design/11-buy-vs-build.md`

- Row "Git objects and packs" (gitoxide): append "Pack encoding without delta compression and bitmap writing are also unchecked upstream (2026-10-01): repacking uses stock `git` as a process (D396)."
- Row "Build cache": replace "Point at the bucket; no Operon code" with "sccache v0.18.0: direct S3/R2 path with no Loam code, or a Loam WebDAV gateway path for metering, LRU and trust (D398)".

## 4. Conflicts with existing decisions

| Existing | What the chat dump (or the new docs) says | Proposed resolution |
|---|---|---|
| **§15 §3.1 (approved)**: one CAS'd `refs` document per repository | A WAL on S3 as the source of truth, a head pointer advanced by conditional write | **D388/D389** amend §15 §3.1–§3.2 (marked inline there): create-only segments plus checkpoints, no head pointer. Needs the owner's approval because §15 is approved |
| **§15 §3.1**: `packs/<ulid>.pack` + `.idx` | — | **D390**: one `packs/<checksum>.lpk` per push |
| **D1**: object storage is the only source of truth | A Durable Object per tenant "for coordination and metadata"; package index "in a Durable Object or D1" | **D382, D391, D399**: Durable Object storage is a cache and a lease; the index lives in the object store |
| **D61, D178**: RustFS is the self-hosted store | "Rust S3-API service fronting R2 … with a per-tenant Durable Object" | **D381**: no Loam S3 service; RustFS self-hosted, R2 on Cloudflare, vended credentials; `r2` joins D178's providers |
| **D190, D200–D202**: metering and billing outside the engine, hooks only | §13.3 "natural metering unit for CPU-time billing"; §14.7 "CloudEvents … feeding the metering ledger in section 7" | **D393**: §27 hooks only, no ledger (the Tail Worker path belongs to the commercial Cloudflare target in `loam-platform`) |
| **D260, D261**: no TiDB; Resonate on TiKV | §2 item 2, §3, §4.3: "Resonate on TiDB (our fork)" | The thin runner calls Resonate's HTTP gateway whatever its store; Git needs no durable store. No change to D260/D261 (other agents' sections own the Resonate text) |
| **D11**: no copyleft dependencies | §14.8 "build on git's own upload-pack/receive-pack?" | **D394, D396**: stock `git` (GPL-2.0) only as an unmodified process (oracle, repack), as D148 (WeSQL) and D236 (PgDog) |
| **§15 §7**: sccache with "no Operon code" | §14.6 "Loam provides an S3-compatible (or WebDAV) endpoint … LRU via a metadata index" | **D398**: the direct path stays code-free; the gateway path is optional; LRU is approximate (refresh-on-hit), no metadata index |
| **§15 principle 2**: one writer per workspace; share by commit | §14.5 "per-scope WAL partitions so agents in different folders do not contend" | **D397**: no per-scope partitions; scopes are read and write admission; Q391 reopens it on measurement |
| **§15 §13**: W1 after M3 | The draft's phases imply starting Loam Git now | **Q395**: owner decision; plans are written so GT can start at any time |
| **D33** (`loamdb`) | — | Superseded by the owner's ruling of 2026-10-01 (packages `loams`); §36 names no packages beyond crates, which keep `operon-*` names until the rename |
| The draft §6: event types `io.loam.<domain>.<name>.v1` | — | The owner's ruling of 2026-10-01: `io.loams.dev.<domain>.<name>.v1` (§36 §4.3, D390, D399) |
| The draft §14.2: Continuity's 120/300 pushes/s | InfoQ: synthetic, "have not been independently verified" | §36 §3 records them as reported; Loam's own target is measured in GT1 |
| The draft §14.6: "Known sccache limits … `SCCACHE_BASEDIRS`" | sccache README (v0.18.0) | Confirmed: `SCCACHE_BASEDIRS` exists since v0.14.0; linker-invoking and incremental crates are not cached |

## 5. Verification notes (web, 2026-10-01)

| Claim in the draft | Finding | Source (date) |
|---|---|---|
| R2 and S3 conditional writes | R2: `If-Match`/`If-None-Match` on PutObject and binding `onlyIf`; **one write per second per key**; strong consistency. S3: `If-None-Match` 2024-08-20, `If-Match` 2024-11-25, both on CompleteMultipartUpload; Express not explicitly confirmed | R2 S3 API (2026-07-31), limits (2026-06-08), consistency; AWS announcements |
| InfoQ 2026-09-30 on Cursor's Continuity | Confirmed: S3 WAL as truth, NVMe warm caches, ack after persist, batching, rendezvous hashing, S3 CAS, UDP gossip as hint, primary-only compaction, 120/300+ pushes/s synthetic and unverified | infoq.com/news/2026/09/cursor-continuity-git-storage (2026-09-30); cursor.com/blog/git-at-any-scale (2026-08) |
| GitHub Spokes three-phase commit | Confirmed for Spokes; DGit is three replicas, two-of-three commit | github.blog (2016-04-05; 2017-10-13, updated 2025-06-03) |
| sccache backends and limits | v0.18.0 (2026-09-14), Apache-2.0; local, S3, R2, Redis, Memcached, GCS, Azure, GHA, WebDAV, OSS, COS; multi-level (2026-04-17); read-only modes; `SCCACHE_BASEDIRS` | github.com/mozilla/sccache |
| gitoxide server-side status | Server-side upload-pack/receive-pack still unchecked; delta compression, bitmap and commit-graph writing unchecked; MIDX done; `gix` 0.88.0 (2026-09-25), MIT OR Apache-2.0 | GitoxideLabs/gitoxide `crate-status.md` |
| Licences | gitoxide MIT OR Apache-2.0; sccache Apache-2.0; BuildCache zlib (v0.33.1, moved to GitLab); workers-rs Apache-2.0 (v0.8.7, 2026-09-25) | the repositories |
