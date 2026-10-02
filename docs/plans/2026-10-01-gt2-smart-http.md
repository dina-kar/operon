# GT2 — Smart HTTP for Stock Git, Compaction and Partial Clone Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, capabilities, messages), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: Planned** (2026-10-01). Design: [§36](../design/36-loams-git.md) §6 and §7 (D394–D396). Depends on [GT1](2026-10-01-gt1-wal-git-core.md) merged. Track GT; branches `gt2-t<N>`, stacked; PRs target `main`. GT2 adds a feature and routes to the `loams` binary's `gateway` role, a module tree in `loams-git`, and a compaction task in `loams-worker`'s registry; it changes no M-track code path.

**Goal:** Serve §36's repositories to stock clients over Smart HTTP and keep them compact:
- `git-upload-pack` over **protocol v2** (`ls-refs`, `fetch` with negotiation, `shallow`/`deepen`, `filter`, `wait-for-done`, `object-info`) on a range-read object database;
- `git-receive-pack` over v0/v1 (`report-status`, `report-status-v2`, `atomic`, `delete-refs`, `side-band-64k`, `ofs-delta`, `push-options`, `quiet`) with streaming pack indexing and verification;
- compaction (`git repack` by a worker on a local mirror, `PackSetChange`, pack GC);
- `git-remote-loams` with `stateless-connect` and `loams://` addresses, so partial clone and lazy fetch work;
- the gates: §15 W1's client matrix (git, gitoxide, libgit2, JGit) and differential tests against `git upload-pack`.

**Architecture:**
- **Routes** in `crates/loams/src/api/git.rs` under the off-by-default feature `git`, mounted in the `gateway` role, **loopback only** until the unified auth plan (D111; Q389): `--git-listen` refuses any non-loopback address with `git listen on <addr>: only loopback addresses are served until the unified auth plan (D111)`.
- **Protocol** in `crates/loams-git/src/protocol/` (`pktline.rs`, `advertise.rs`, `lsrefs.rs`, `fetch.rs`, `negotiate.rs`, `assemble.rs`, `receive.rs`, `report.rs`): transport-free, over `AsyncRead`/`AsyncWrite` of pkt-lines, so the gateway, the helper's in-process server (Task 8) and the Worker of a commercial Cloudflare target (`loam-platform`) can share it.
- **Reads** through `RangeOdb` (Task 3): pack idx sections and objects by range GET through the H1 cache (`loams-cache`), never whole-pack downloads on the serving path.
- **Writes** through GT1's `BlobStore` and `BucketRefLog`. The gateway node runs a `BucketRefLog` per active repository it owns (rendezvous owner of `(ns, repo, repo_id)`, D75) and forwards pushes for other repositories to their owner (§36 §4.4).
- **Stock `git`** runs only in the compaction worker and in tests (D394, D396).

**Tech Stack:** Rust 1.97.1. New: `gix-packetline` (async feature), `gix-negotiate` is client-side and not used; `gix-pack` output pipeline (`gix_pack::data::output`), `gix-commitgraph` (read), `gix-revwalk` if Task 0 finds it useful; all from the `gix` 0.88 train, MIT OR Apache-2.0. Reused: `axum` 0.8, `tokio`, `loams-cache` (H1), `loams-worker` (leases). Test-only tools, run as processes: `git` (two versions: the CI image's and the newest release), `gix` CLI (gitoxide, `cargo install gitoxide` pinned), `pygit2` (libgit2, GPL-2.0 with linking exception; run from a `uv` venv in a script, never linked), JGit's CLI jar (EDL-1.0/BSD-3-Clause, `org.eclipse.jgit.pgm`).

**Spec:**
- [`docs/design/36-loams-git.md`](../design/36-loams-git.md) §6.1, §6.2 (GT2 row), §6.3, §7, §10, §11, §14.
- Git: https://git-scm.com/docs/protocol-v2, https://git-scm.com/docs/gitprotocol-http, https://git-scm.com/docs/gitprotocol-pack, https://git-scm.com/docs/gitprotocol-capabilities, https://git-scm.com/docs/gitremote-helpers.
- As built: GT1's `loams-git` and `loams-git-remote`; `crates/loams/src/{server.rs,main.rs,api/}`; `crates/loams-worker`; `crates/loams-cache`.

## Global Constraints

Same as the M1 overview §8 and GT1's, plus:
- **Loopback only** (D111), as above. A request whose namespace does not exist answers `404` with no body detail.
- **Limits** (in `loams_git::limits`, documented on the limits page per D88): `max_pack_bytes` 2 GiB, `max_refs_per_push` 4096, `max_wants` 65 536, `max_haves_per_round` 256, `max_negotiation_rounds` 64, `receive_idle_timeout` 60 s, `upload_deadline` 30 min. Each has a test at the limit and one past it.
- **Every response is streamed.** No handler buffers a whole pack in memory; spill goes to the node's `Fs` (`loams-fs`'s `NativeFs`, §36 §17) under a per-request directory removed on completion.
- **Commit areas:** `git`, `api`, `worker`, `ci`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **Upload-pack v2 only, unless Task 0 finds a matrix client without v2.** A request without `Git-Protocol: version=2` gets `400` with `loams: git protocol v2 is required (git ≥ 2.26; set protocol.version=2)` | §36 D394; git has defaulted to v2 for years (Task 0 records the exact release) | Old clients fail; Task 10 adds v0/v1 upload-pack if Q387 says a matrix client needs it |
| 2 | **Ref advertisement in v2 is served from `snapshot(Latest)`**, one GET past the node's state (§36 §4.6); `server-option=loams-at-least=<seq>` selects `AtLeast` | Linearizable `ls-refs` after a push on any node | One GET per `ls-refs`; cheap |
| 3 | **Pack assembly reuses stored deltas whose base is sent or already on the client**, and otherwise sends the object whole; no new delta search in GT2 | gitoxide's pack encoding has no delta compression yet (`crate-status.md`, 2026-10-01); stored packs are already delta-compressed by the pusher and by repack | Larger fetch packs for objects stored as deltas against unsent bases; measured in Task 5 against `git upload-pack` |
| 4 | **Non-atomic pushes become one `RefTxn` per ref, committed concurrently** so they share a segment and each ref reports its own status | Matches git's per-ref semantics without a separate code path | None |
| 5 | **Compaction mirrors repositories on the worker's NVMe** (`<data>/git-mirror/<ns>/<repo_id>.git`), hydrated from `.lpk` objects and kept as a cache between runs | Stock `git repack` needs a local repository; Continuity's "only the primary compacts" (§36 §3) | Disk use on workers; the mirror is evicted by LRU and rebuilt on demand |
| 6 | **`packfile-uris` is off in GT2** | It needs pre-signed URL issuance per provider and client opt-in (`fetch.uriProtocols`); worth it on R2 later | Clones of large repositories stream through the gateway |

## Carried in

From §36: Q386 and Q387 (Task 0), Q389 (loopback until the auth plan, MT1 on PR #182). From GT1: its as-built rulings and measured latencies.

## Review Focus

1. **A push is acknowledged only after its pack and its segment are durable**, and a malformed or disconnected pack never reaches the WAL. Tests: Task 6 (`ack_after_segment_commit`, `malformed_pack_is_refused`, `missing_base_is_refused`, `connectivity_gap_is_refused`).
2. **Fetch returns exactly the objects needed**, with filters and shallow boundaries honoured. Tests: Task 5 and Task 9 (the differential suite against `git upload-pack`).
3. **Compaction never loses an object or exposes a mixed pack set.** Tests: Task 7 (`repack_preserves_every_reachable_object`, `readers_see_old_or_new_pack_set`, `pack_gc_respects_grace_and_forks`).
4. **No unbounded memory.** Tests: Task 6 (`two_gib_push_bounded_rss`), Task 5 (`large_clone_bounded_rss`).
5. **Loopback refusal and limits.** Tests: Task 1, and the limit tests of every task.

## File structure

```
crates/loams-git/src/protocol/{mod.rs,pktline.rs,advertise.rs,lsrefs.rs,objectinfo.rs,fetch.rs,negotiate.rs,
                                assemble.rs,filter.rs,shallow.rs,receive.rs,verify.rs,report.rs}
crates/loams-git/src/{range_odb.rs,commit_graph.rs,owner.rs,compact.rs,pack_gc.rs}
crates/loams-git/tests/{protocol.rs,lsrefs.rs,fetch.rs,receive.rs,compact.rs,differential.rs}
crates/loams/Cargo.toml                       # feature git
crates/loams/src/api/git.rs  crates/loams/src/{server.rs,main.rs}   # --git-listen
crates/loams-worker/src/…                     # register the git-compact task kind (as built)
crates/loams-git-remote/src/{connect.rs,url.rs}
crates/loams/tests/git/{main.rs,http.rs,matrix.rs}
scripts/git/{matrix.sh,pygit2_probe.py,jgit_probe.sh,gix_probe.sh}
.github/workflows/ci.yml                       # job git-matrix (path-filtered), nightly differential
docs/design/36-loams-git.md  docs/guides/limits.md (via the limits table)  CHANGELOG.md
```

### Task 0: Reconcile and check the clients

**Files:** read GT1's crates as merged, `crates/loams/src/{server.rs,main.rs,api/mod.rs}`, `crates/loams-worker/src/*`, `crates/loams-cache/src/*`. Fill "Rulings made during execution".

**Checks** (record each with its command and output in this plan's rulings table):
1. **Q386:** with git (CI version and newest), a test helper that implements only `stateless-connect` to a local `git upload-pack --stateless-rpc` proxy: does `git clone --filter=blob:none loams::…` succeed, and does a later `git checkout` lazily fetch missing blobs through the helper? Record the git versions where it works. If it does not, Task 8's design falls back to `git clone` over Smart HTTP for partial clone and the helper keeps `fetch`/`push` only.
2. **Q387:** do pygit2's libgit2 (the version `uv` installs) and JGit's CLI fetch over protocol v2 against `git http-backend` with `protocol.version=2`? If either does not, Task 10 (v0/v1 upload-pack) is in scope.
3. The git release that made v2 the default over HTTP (for Ruling 1's message).
4. `gix-pack`'s output pipeline: the counting and entry-iteration options that reuse stored deltas ("pack copy" of entries), whether it emits `ofs-delta`, and whether it can write a thin pack; `gix-commitgraph` read API for generation numbers.
5. How `loams-worker` registers a new task kind with a lease key (`task/git-compact/<ns>/<repo_id>`), and how the gateway learns the rendezvous owner of a placement key (M1.3's routing as built).
6. The owner's answer to Q389 (recorded only; a Cloudflare Worker belongs to a commercial Cloudflare target (`loam-platform`)).

**Commit:** `docs: reconcile GT2 with main and record the client checks`.

### Task 1: pkt-lines, the listener and advertisements

**Files:** `protocol/{mod.rs,pktline.rs,advertise.rs}`, `crates/loams/src/api/git.rs`, `crates/loams/src/{server.rs,main.rs}`, `crates/loams/Cargo.toml`, `crates/loams/tests/git/{main.rs,http.rs}`, `crates/loams-git/tests/protocol.rs`.

**Produces:**

```rust
pub enum Pkt<'a> { Data(&'a [u8]), Flush, Delim, ResponseEnd }
pub struct PktReader<R>;  pub struct PktWriter<W>;              // gix-packetline underneath; max 65 520 bytes per line
pub fn advertise_v2(agent: &str, caps: &ServerCaps) -> Bytes;  // §36 §6.1's capability list
pub fn advertise_receive_v0(snapshot: &RefSnapshot, agent: &str) -> Bytes;   // first line carries capabilities after NUL
pub struct GitConfig { pub listen: SocketAddr, pub data_dir: PathBuf /* spill and mirrors */ }
// routes: GET /git/{ns}/{repo}.git/info/refs?service=…, POST …/git-upload-pack, POST …/git-receive-pack
```

**Semantics:** content types per gitprotocol-http (`application/x-git-<service>-advertisement`, `-request`, `-result`); `Cache-Control: no-cache`; `# service=<name>` preface for `info/refs`; `Git-Protocol` parsed; gzip request bodies accepted (`Content-Encoding: gzip`); the repository is resolved from `{ns}` and `{repo}` with `RepoId::parse` (invalid → `404`).

**Tests:** `pktline_round_trip_and_limits`; `advertise_v2_matches_design`; `advertise_receive_has_capabilities_after_nul`; `non_loopback_is_refused`; `v0_upload_request_is_400_with_hint` (Ruling 1); `unknown_namespace_is_404`; `git_ls_remote_against_empty_repo` (stock `git ls-remote http://127.0.0.1:<port>/git/default/r.git`).

**Commit:** `git: serve Smart HTTP advertisements on a loopback listener`.

### Task 2: `ls-refs` and `object-info`

**Files:** `protocol/{lsrefs.rs,objectinfo.rs}`, `crates/loams-git/tests/lsrefs.rs`.

**Semantics:** `ls-refs` with `symrefs`, `peel` (annotated tags peeled through `RangeOdb` once Task 3 lands; until then from a peeled-tag cache in the checkpoint, written at commit by the receive path), `ref-prefix` (multiple), `unborn` (HEAD pointing at a missing branch); Ruling 2. `object-info` with `size` for listed oids.

**Tests:** `ls_refs_prefix_filters`; `ls_refs_symrefs_and_unborn`; `ls_refs_peels_tags`; `ls_refs_at_least_seq_reads_own_push` (push on node A, `ls-refs` with `loams-at-least` on node B sees it); `object_info_sizes`.

**Commit:** `git: answer ls-refs and object-info`.

### Task 3: The range-read object database

**Files:** `crates/loams-git/src/range_odb.rs`, `crates/loams-git/tests/fetch.rs` (odb part).

**Produces:**

```rust
pub struct RangeOdb;   // over BlobStore + H1 (loams-cache), per RefSnapshot's pack set (+ fork parents)
impl RangeOdb {
    pub async fn open(blobs: Arc<dyn BlobStore>, cache: Arc<loams_cache::Cache>, snap: &RefSnapshot, parents: Vec<RangeOdb>) -> Result<Self, OdbError>;
    pub async fn locate(&self, oid: &ObjectId) -> Result<Option<Location>, OdbError>;      // (pack, offset) via idx fan-out + binary search, or MIDX
    pub async fn header(&self, loc: &Location) -> Result<EntryHeader, OdbError>;           // kind, size, delta base
    pub async fn raw_entry(&self, loc: &Location) -> Result<Bytes, OdbError>;              // compressed bytes, for pack copy
    pub async fn read(&self, oid: &ObjectId) -> Result<(gix_object::Kind, Bytes), OdbError>; // delta-resolved, base chain cached
}
```

**Semantics:** an idx section is read lazily: fan-out table (1 KiB) once, then the oid table pages it needs (4 KiB-aligned ranges), then the offset tables; the MIDX from compaction replaces per-pack lookups when present. Entry reads fetch at least 64 KiB (an entry's compressed size is unknown until inflated; the next entry's offset bounds it). Every range goes through H1 keyed by `(path, offset, len)` (§04); objects are immutable, so nothing is invalidated.

**Tests:** `locate_matches_git_cat_file` (every object of a fixture repository); `read_resolves_ofs_and_ref_deltas`; `midx_lookup_matches_per_pack`; `range_reads_are_coalesced` (store GET count for reading 1 000 objects of one pack stays under 50); `fork_parent_lookup`.

**Commit:** `git: read objects by range from stored packs`.

### Task 4: Negotiation, commit graph and shallow

**Files:** `protocol/{fetch.rs,negotiate.rs,shallow.rs}`, `crates/loams-git/src/commit_graph.rs`, `crates/loams-git/tests/fetch.rs`.

**Semantics:** v2 `fetch` arguments `want`, `have`, `done`, `thin-pack`, `no-progress`, `include-tag`, `ofs-delta`, `shallow`, `deepen`, `deepen-relative`, `deepen-since`, `deepen-not`, `filter`, `want-ref`, `wait-for-done`, `sideband-all` (per protocol-v2). Server side: mark `have`s that exist as common; answer `acknowledgments` (`ACK`/`NAK`, `ready` when the common set covers every want's history by the commit-graph's generation numbers, as git's `ok_to_give_up`); bounded by `max_haves_per_round` and `max_negotiation_rounds`. The commit graph is the compaction's `commit-graph/<seq>.graph` when present, else built per request from `RangeOdb` and cached in H2 per pack-set seq. Shallow and deepen compute the boundary per protocol-v2 and send `shallow-info`.

**Tests:** `clone_negotiates_with_no_haves`; `fetch_after_push_sends_only_new_commits`; `ready_ends_negotiation_early`; `deepen_1_matches_git`; `deepen_since_and_not`; `want_ref_resolves`; `negotiation_round_limit_is_enforced`.

**Commit:** `git: negotiate fetches with a commit graph, shallow and deepen`.

### Task 5: Pack assembly and filters

**Files:** `protocol/{assemble.rs,filter.rs}`, `crates/loams-git/tests/fetch.rs`.

**Semantics:** Ruling 3. The object set is `reachable(wants) − reachable(common)`, minus filtered objects: `blob:none`, `blob:limit=<n>`, `tree:<depth>`, `sparse:oid` refused with `ERR filter sparse:oid is not supported`. Entries are written by `gix-pack`'s output pipeline in recency order with `ofs-delta`; a stored delta whose base is in the set or (with `thin-pack`) in `common` is copied raw; otherwise the object is inflated, resolved and re-deflated whole. Whole stored packs are streamed verbatim when the set equals a pack's object set (a clone right after repack). Side-band-64k: data on 1, progress on 2 (`Counting objects`, `Enumerating`), errors on 3. `include-tag` adds annotated tags pointing into the set.

**Tests:** `clone_round_trips_through_git_fsck`; `partial_clone_blob_none_has_no_blobs`; `blob_limit_filters_by_size`; `tree_zero_has_only_commits`; `thin_pack_resolves_on_client`; `whole_pack_reuse_after_repack` (store GET count ≈ the pack's range count); `large_clone_bounded_rss` (a 1 GiB fixture repository, RSS under 256 MiB); `pack_size_within_1_5x_of_git` (against `git upload-pack` on the same objects; the ratio recorded).

**Commit:** `git: assemble fetch packs with delta reuse and partial-clone filters`.

### Task 6: `receive-pack`

**Files:** `protocol/{receive.rs,verify.rs,report.rs}`, `crates/loams-git/src/owner.rs`, `crates/loams-git/tests/receive.rs`.

**Semantics:** §36 §6.3 steps 1–6 exactly, with Ruling 4. Streaming: the pack body is copied to a spill file and fed to `gix-pack`'s index writer as it arrives; thin bases are resolved through `RangeOdb`; memory is bounded by the index. Verification: object hashes (index-pack), connectivity (GT1's `missing_from_closure` semantics over `RangeOdb` + the new pack), fast-forward (commit ancestry through the commit graph), names, protections. Report: `unpack ok`/`unpack <error>` and `ok <ref>`/`ng <ref> <reason>` with reasons `fetch first`, `non-fast-forward`, `protected`, `already exists`, `stale info`, `missing necessary objects`; `report-status-v2` adds `option refname`/`option old-oid`/`option new-oid` lines; `loams-seq=<n>` on side-band 2. `owner.rs`: the repository's rendezvous owner runs the `BucketRefLog`; a non-owner forwards the verified `RefTxn` (after its own pack PUT) to the owner over the internal API, or, if the owner is unreachable for 2 s, commits itself (safe by fencing; counted as `loams_git_owner_fallbacks_total`).

**Tests:** `push_new_branch`; `push_ff_and_reject_non_ff`; `atomic_push_all_or_none`; `non_atomic_push_reports_per_ref`; `delete_ref_with_delete_refs`; `push_options_reach_the_wal`; `malformed_pack_is_refused`; `missing_base_is_refused`; `connectivity_gap_is_refused`; `protected_branch_refuses_force_and_delete`; `ack_after_segment_commit` (a stalled segment PUT holds the report); `two_gib_push_bounded_rss` (nightly); `max_refs_and_max_pack_limits`; `forwarded_push_commits_on_owner`; `owner_down_fallback_is_fenced`.

**Commit:** `git: receive pushes with streaming verification and group commit`.

### Task 7: Compaction and pack GC

**Files:** `crates/loams-git/src/{compact.rs,pack_gc.rs}`, the `loams-worker` task registration, `crates/loams-git/tests/compact.rs`.

**Semantics:** §36 §7. The task `git-compact` under the lease `task/git-compact/<ns>/<repo_id>` runs when the pack count exceeds the geometric threshold (more than 16 packs above `--geometric=2`) or on `POST /git/<ns>/<repo>.git/loams/compact` (loopback). Ruling 5's mirror is hydrated (missing `.lpk` → `pack-<checksum>.pack`/`.idx`), then `git repack --geometric=2 -d --write-midx --write-bitmap-index` and `git commit-graph write --reachable --split=no`; new packs are uploaded as `.lpk`, plus `midx/<seq>.midx` and `commit-graph/<seq>.graph`; then a `PackSetChange { added, removed, midx, commit_graph }` is committed through the owner's sequencer, fenced on the pack set it started from (if the pack set changed meanwhile, the removed list keeps only packs still present, and packs pushed meanwhile stay). Pack GC (daily): a removed pack is deleted once no checkpoint inside `exactly_retention` and no fork's parent seq names it, after `gc_grace`, with a GC claim (§03 §7). Metrics: `loams_git_cpu_seconds_total{op="compact"}` from the child process's rusage.

**Tests:** `repack_preserves_every_reachable_object` (`git fsck --full` on a fresh clone after compaction); `readers_see_old_or_new_pack_set` (fetches during compaction always succeed); `concurrent_push_during_compaction_keeps_new_pack`; `pack_gc_respects_grace_and_forks`; `compaction_lease_is_exclusive`; `repack_time_per_mib_recorded` (bench output, §36 §7's measurement).

**Commit:** `git: compact repositories with git repack and collect retired packs`.

### Task 8: `stateless-connect` and `loams://` in the helper

**Files:** `crates/loams-git-remote/src/{connect.rs,url.rs}`, `crates/loams-git-remote/tests/helper.rs`.

**Semantics:** per Task 0's Q386 result. `capabilities` gains `stateless-connect`; `stateless-connect git-upload-pack` → for `loams::` addresses, an in-process v2 upload-pack (Tasks 2–5) over the bucket; for `loams://<host>/<ns>/<repo>`, a v2 tunnel to `https://<host>/git/<ns>/<repo>.git/git-upload-pack` (plain `http://` only for loopback hosts). Pushes keep GT1's `push` capability for `loams::`; for `loams://` they go to the server's receive-pack. `option from-promisor` and `option no-dependents` are accepted.

**Tests:** `partial_clone_through_helper`; `lazy_fetch_of_missing_blob_through_helper`; `sparse_checkout_fetches_only_cone_blobs`; `loams_url_reaches_server`; `push_to_loams_url_uses_receive_pack`.

**Commit:** `git: tunnel protocol v2 through git-remote-loams for partial clone`.

### Task 9: The client matrix and the differential suite

**Files:** `crates/loams/tests/git/matrix.rs`, `scripts/git/{matrix.sh,pygit2_probe.py,jgit_probe.sh,gix_probe.sh}`, `crates/loams-git/tests/differential.rs`, `.github/workflows/ci.yml`.

**Semantics:** the matrix (§15 W1): for each client (git CI version, git newest, `gix` CLI, pygit2/libgit2, JGit CLI) run clone, fetch after a push, push (new branch, fast-forward, rejected non-fast-forward, delete), shallow clone `--depth 1`, and (git and gix only) `--filter=blob:none`. The differential suite: for 200 seeded random histories (merges, renames, tags, large blobs), the same `fetch` requests are sent to Loams and to `git upload-pack --stateless-rpc` on a local mirror of the same objects; both packs, indexed by `git index-pack`, must contain the same object set (modulo filters), and `git fsck` must pass. CI: job `git-matrix` path-filtered; the differential nightly.

**Tests:** the matrix rows and `differential_object_sets_match`.

**Commit:** `ci: run the git client matrix and the upload-pack differential suite`.

### Task 10: v0/v1 upload-pack (only if Q387 says a matrix client needs it)

**Files:** `protocol/{advertise.rs,fetch.rs}`.

**Semantics:** stateless v0 upload-pack over HTTP with `multi_ack_detailed`, `no-done`, `side-band-64k`, `ofs-delta`, `shallow`, `deepen-since`, `deepen-not`, `filter`, `include-tag`, `thin-pack`, `allow-tip-sha1-in-want`; Ruling 1's refusal is lifted for `info/refs` without `Git-Protocol`.

**Tests:** the matrix rows of the client that needed it, plus `v0_clone_and_fetch` with `git -c protocol.version=0`.

**Commit:** `git: serve protocol v0 upload-pack for older clients`.

### Task 11: Docs and close

**Files:** `docs/design/36-loams-git.md` ("As built (GT2)" notes in §6 and §7, the measured repack time, the client matrix result), the limits table entries, `CHANGELOG.md`, this plan's rulings.

**Commit:** `docs: record GT2 as built and close the plan`.

## Rulings made during execution

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| — | (Task 0 fills this table) | | |
