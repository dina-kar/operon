# 15 — Agent Workspaces (Sandboxes on Operon)

Status: **Approved** (user) · 2026-09-24. Items marked (verify) are unconfirmed and are resolved in the W-phase plans.

Coding agents such as Claude Code and Codex run inside **sandboxes**: an isolated process or microVM, a checkout of a repository, installed dependencies and a network policy. The runtime (the VM or namespace jail) is compute. Everything else — code, branches, checkpoints, dependencies, caches, transcripts, memory — is state that must be fast to materialize, cheap to fork and must survive the sandbox. That is Operon's model: stateless compute over a bucket.

**Operon is the state plane for sandboxes, not the sandbox runtime.** It never executes agent code in its own processes.

---

## 1. What a coding-agent sandbox needs

| Need | Typical today | Operon piece |
|---|---|---|
| Isolation (process, microVM) | Firecracker, gVisor, bubblewrap, Landlock | **Not Operon** — integrate runtimes (§8) |
| Source checkout, a branch per agent, commits, diffs, rewind | GitHub + a full clone per sandbox | **Repos**: Git on the bucket, O(1) forks, partial clone (§3) |
| Dependencies and toolchains ready at start | Reinstall per sandbox, or a fat image per project | **Environment images**: content-addressed, lazily loaded, copy-on-write (§5) |
| Package downloads under egress control | Direct internet access, or a proxy allowlist | **Registry proxy** in the gateway (§6) |
| Build and test caches | Local, lost with the sandbox | Shared caches in the bucket (§7) |
| Crash recovery of a long run | Start over | Every session is a **durable execution** (§14) with workspace and session-state checkpoints (§9) |
| Search over code, docs and past runs | Separate vector DB + grep | **Code-index links** into collections and graphs (§4) |
| Transcripts, tool calls, token cost | Log files, vendor dashboards | Streams → tables (§9) |
| Tools for the agent | Ad-hoc MCP servers | **Operon MCP server** (§10) |
| Scoped credentials | Long-lived tokens in the sandbox | Credential vending per sandbox (§8) |

## 2. Principles

1. **State plane, not runtime.** Operon stores and serves; sandbox runtimes execute.
2. **One writer per workspace; share by commit.** Agents exchange work as commits and branches (the Git model), not through a shared POSIX filesystem. This removes distributed locking and cache coherence from the design.
3. **Immutable, content-addressed lower layers + a local writable upper layer.** Code trees and environment images are immutable and deduplicated; each sandbox writes to a local copy-on-write layer. A fork is a new pointer, never a copy.
4. **Lazy and cached.** Nothing is downloaded before it is read; every read goes through the node-local H1 cache (§04), so sandboxes on one host share it.
5. **Tenancy by namespace.** Deduplication happens within a namespace. Cross-namespace sharing is only for a designated public-packages namespace, to avoid content-existence side channels.

## 3. Repos: Git on the bucket

**Why Git:** agents already know it from training data (`git diff`, branches, worktrees), Claude Code and Codex operate on Git repositories, and a commit is exactly the checkpoint an agent needs: a durable snapshot with identity, parent and message. Cloudflare Artifacts and Freestyle made the same bet for agent storage.

### 3.1 Layout

```
ns/<ns>/repos/<repo_id>/
  refs                       # one canonical document: all refs, live packs, fork parent — replaced by conditional PUT
  packs/<ulid>.pack          # immutable packfiles (create-only)
  packs/<ulid>.idx           # pack index (+ .bitmap, .rev after repack)
  commit-graph/<ulid>        # built by repack
```

- **Refs are a CAS'd document, not metastore state.** A deployment may hold tens of millions of repos (Cloudflare's stated target is tens of millions per namespace), and pushes are user data. The per-repo document pattern is the one the Resonate blob server uses (§14): one conditional PUT commits a ref transaction atomically.
- **Fork = a new `refs` document naming its parent.** Reads fall through to the parent's packs (Git "alternates"), so a fork costs one PUT and no data copy. Per-agent forks or per-agent branches both work.

### 3.2 Protocol

- **Smart HTTP, protocol v2**, in the `gateway` role, for stock `git`, gitoxide, libgit2 and JGit clients.
- **Push (receive-pack):** stream the pack → verify it (index, connectivity, object limits) → PUT pack + idx (create-only) → CAS the `refs` document with per-ref old-oid checks, fast-forward rules and branch protection → append a record to the repo's event stream → acknowledge. The event record is written after the commit, at least once, with a repair sweep, so links (§4) see every push.
- **Fetch/clone (upload-pack):** negotiation uses the commit-graph cached on query nodes; existing packs are reused whole when the wants cover them, otherwise a pack is generated. **Partial clone** (`--filter=blob:none`, `tree:0`) and shallow clones let sandboxes start with trees only and fetch blobs on demand.
- **Contention:** pushes to one repo contend on its `refs` document. Repo-affinity routing (§04) lets one node group-commit concurrent pushes, as Resonate does per origin.
- **Git LFS:** batch API with objects in the namespace CAS (§5.2).
- **Import and mirror:** import from GitHub/GitLab, periodic sync, optional push-back of agent branches.

### 3.3 Maintenance

A worker task repacks small packs into larger ones with bitmaps and a commit-graph. GC runs by reachability across a fork family (a parent's packs stay while any fork references them), after the §03 §7 grace period.

### 3.4 Buy vs build

- **gitoxide** (`gix-pack`, `gix-protocol`, `gix-packetline`, `gix-hash` with SHA-1 and SHA-256, `gix-commitgraph`; Apache-2.0/MIT, very active) provides the object and pack machinery. Its own status page lists **server-side upload-pack/receive-pack plumbing as not implemented**, so Operon builds the server loop on gix primitives. That is the build part.
- References: `git-remote-object-store` (Apache-2.0, Rust: `bundle` and `packchain` engines on S3/Azure with GC and compaction), `awslabs/git-remote-s3` (Apache-2.0, Python: bundles + LFS on S3), Cloudflare Artifacts (closed; a Zig/WASM Git server on Durable Objects + R2).
- **Later option — Jujutsu:** `jj` (Apache-2.0, Rust) has pluggable backends for commits, the operation log, op heads and the index (Google runs it against cloud storage). `jj` snapshots the working copy on every command and keeps an undoable operation log, which suits agents. An Operon backend is Phase C.

## 4. Code intelligence: why this belongs in Operon

A Git host stores code; Operon also makes it searchable the moment it is pushed.

- **`repo → collection` link:** each push event → changed blobs → tree-sitter (MIT) parse → chunks by symbol → Tantivy text + `embed()` vectors → a collection keyed by `(repo, ref, path, symbol)`. The default branch and branches with open agent work are indexed.
- **`repo → graph` link:** files, symbols, imports and calls as a graph, so "who calls `parse_config`" is one Cypher query (§07).
- **Read-your-writes:** a push returns a consistency token; an agent that pushes and then searches with that token sees its own change (§05 §5).

## 5. Environments: dependencies without reinstalling

### 5.1 The sandbox filesystem

```
/workspace   upper: local NVMe (overlayfs)      lower: repo tree @ commit (lazy, from Operon)
/env         upper: local NVMe                  lower: environment image @ env_key (lazy, CAS)
/cache       upper: local, written back async   lower: namespace package caches (CAS)
/tmp         local only
```

- **Fork** = a new upper layer over the same lower layers: O(1), nothing copied.
- **Checkpoint** = the `/workspace` upper layer hashed into Git objects and committed to the agent's branch (§9). Only changed files are uploaded.

### 5.2 Environment images

- **Key:** `env_key = hash(lockfiles, toolchain versions, platform, setup script)`. Identical projects on identical lockfiles share one image.
- **Format:** content-defined chunks (FastCDC, BLAKE3 digests) packed into 16–64 MiB pack objects with an index, plus a filesystem manifest. Small files are never one PUT each. This is the **nydus RAFS** model.
- **Buy: nydus** (Apache-2.0, Rust, CNCF Dragonfly): RAFS v6 images (EROFS-compatible), cross-layer chunk dedup, lazy fetch through FUSE, virtiofs or in-kernel EROFS + fscache. Operon stores the chunk packs and serves them through its cache; nydus builds and mounts images (verify that nydus's storage backend can point at Operon's bucket or cache endpoint).
- **Build once:** a sandbox that misses its `env_key` triggers an **env build** worker task that installs into a builder sandbox, converts the result to an image and publishes it. Concurrent misses on one key share one build through a metastore lease on the key.
- **Why lazy:** a sandbox touches a small fraction of its environment at start. The SOCI paper (arXiv 2607.06868) reports 7.4–9.3× lower cold-start pull time for lazy loading versus full pulls, and Mintlify reports session creation dropping from about 46 s to about 100 ms with a virtual filesystem.

### 5.3 Package caches

Mount the namespace's caches for `uv` (`UV_CACHE_DIR`), pnpm (`store-dir`), Cargo (`CARGO_HOME/registry`), Go (`GOMODCACHE`) and pip wheels as a CAS-backed lower layer. New entries land in the local upper layer and are published back asynchronously; since entries are content-addressed, concurrent publishes cannot conflict. uv and pnpm already hardlink from a content-addressed store on one host; this extends the same store across hosts and past the sandbox's lifetime.

## 6. Registry proxy: egress control

- Read-through endpoints in the `gateway` role for **PyPI** (simple API, PEP 503/691), **npm**, **crates.io** (sparse index), **Go** (`GOPROXY`). Artifacts are fetched from upstream once, stored immutably in the CAS and served from cache.
- **Policy:** allowlists, version pinning, a quarantine window for newly published versions (supply-chain defense), and an audit record per download to a stream.
- A sandbox's network policy then needs only Operon and the model API. That matches how Claude Code's sandbox runtime (a proxy with a domain allowlist) and Codex (network off by default) already work.
- **OCI images:** run an existing registry (`distribution` or `zot`, both Apache-2.0) with its S3 driver on the bucket instead of implementing OCI distribution.

## 7. Build and test caches

- **sccache** (Apache-2.0) has an S3 backend: point it at `ns/<ns>/cache/sccache/` with vended credentials. No Operon code.
- **Bazel / Buck2 / Pants:** `bazel-remote` (Apache-2.0) with its S3 backend now; Operon's own REAPI CAS + ActionCache on the namespace CAS is Phase C.
- **Turborepo / Nx** remote-cache HTTP APIs: small, Phase B.

## 8. Sandbox runtimes: integrate, do not build

Coding agents need a full Linux userland (Node or Bun, Git, a shell, compilers), so the isolation unit is a **microVM running an OCI image** or, where KVM is unavailable, a user-space kernel. Function-level sandboxes (Hyperlight, WebAssembly) cannot host Claude Code, Codex or opencode.

| Runtime | License | Language | Isolation | Snapshot / fork | Needs | Role |
|---|---|---|---|---|---|---|
| **microsandbox** | Apache-2.0 | Rust (libkrun) | microVM per sandbox, OCI images | Snapshot, restore and fork of live sandboxes in core (2026-06) | KVM (Linux), Apple Silicon, WHP (Windows) | **Default backend**: embeddable SDK, no daemon, per-sandbox egress allowlists |
| **Firecracker** | Apache-2.0 | Rust | microVM | Memory + disk snapshots; restore with copy-on-write memory | KVM | **Fleet backend** for large hosts (the E2B model) |
| Cloud Hypervisor | Apache-2.0 / BSD-3 | Rust | microVM (virtiofs, hotplug) | Snapshot/restore | KVM | Alternative VMM |
| libkrun | Apache-2.0 | Rust | microVM library | via embedder | KVM / HVF | Under microsandbox |
| crosvm | BSD-3 | Rust | microVM (ChromeOS) | — | KVM | Reference |
| Kata Containers | Apache-2.0 | Rust (runtime-rs) | microVM per pod | — | KVM | Kubernetes RuntimeClass option |
| youki | Apache-2.0 | Rust | OCI containers (namespaces) | — | — | Not a security boundary for untrusted code alone |
| gVisor | Apache-2.0 | Go | User-space kernel | `runsc checkpoint` | Nothing (systrap) | **Kubernetes backend without KVM** |
| E2B | Apache-2.0 (infra) | Go | Firecracker | Yes | KVM | Managed or self-hosted provider adapter |
| Anthropic `sandbox-runtime` | Apache-2.0 | TypeScript | bubblewrap / Seatbelt + network proxy | — | — | Laptop/dev mode; what Claude Code uses for its sandboxed bash tool |
| Codex CLI | Apache-2.0 | Rust | Landlock + seccomp | — | — | Laptop/dev mode; sandboxing on by default |
| Hyperlight | Apache-2.0 | Rust | micro-VM per function call | — | KVM / Hyper-V | Not suitable (no OS for agent harnesses) |
| Daytona | AGPL-3.0 | — | — | — | — | **Avoid**: license; open-source repo reported unmaintained (2026-06) |

**Decision (proposed): `operon-sandbox` is a Rust crate with a `Runtime` trait and four backends**, chosen per deployment:

| Backend | When |
|---|---|
| `microsandbox` (default) | Single hosts, laptops (macOS/Linux/Windows), demos; fork of live sandboxes |
| `firecracker` | Dense production fleets on KVM hosts; copy-on-write memory across restored snapshots |
| `gvisor` | Kubernetes clusters without KVM or nested virtualization |
| `process` (Landlock + seccomp, or bubblewrap) | Trusted local development only; never multi-tenant |

**`operon-sandbox`** is a small agent (sidecar or in-VM binary) that runtimes call with a spec:

```toml
repo    = "ns/acme/repos/api"      # fork or branch to work on
ref     = "agent/run-8f2c"
env_key = "auto"                    # derived from lockfiles
caches  = ["uv", "pnpm", "cargo"]
egress  = "operon-only"             # registry proxy + model API
token   = "<vended, 1h, scoped to this repo branch, env read, cache write>"
```

It mounts the layers (§5), points package managers at the proxy (§6), ships telemetry, and implements `checkpoint`, `fork`, `suspend` and `resume`. Adapters: microsandbox, E2B templates, Kubernetes pods. Inside microVMs, virtiofs or EROFS + fscache avoids needing FUSE privileges in pods (verify per runtime).

**Credential vending:** tokens are short-lived and scoped to `(namespace, repo, branch prefix, env read, cache write, MCP tools)`, the way Lakekeeper vends table credentials (§10 §4).

**VM memory snapshots** (fork a *running* VM, as Morph's Infinibranch does) are a runtime feature. Operon can store Firecracker snapshot files (memory + disk diff) in the CAS with chunk dedup for suspend-to-bucket and resume-anywhere. Phase C.

## 9. Agent sessions: stored in Operon, run as durable executions

**Every agent session is a Resonate durable execution (§14), and every session is stored in Operon.** A crash of the sandbox, the host or the orchestrator never loses a session; it resumes mid-conversation on another host.

### 9.1 What a session is

| Part | Where it lives | Written when |
|---|---|---|
| Session workflow | Resonate origin `session-<id>` (`ns/<ns>/durable/wf/…`) | Every step transition |
| Workspace | Branch `agent/<session>` in an Operon repo (§3) | Every checkpoint |
| Harness session state (`~/.claude/projects/…/*.jsonl`, `~/.codex/sessions/`, `~/.local/share/opencode/opencode.db`) | The `/agent-home` layer, checkpointed with the workspace as a tree in the namespace CAS | Every checkpoint |
| Session log (messages, tool calls, results, token usage) | Stream `sessions` (one partition key per session) → table `agent_sessions` + collection `session_history` | Continuously, from telemetry and from parsed session files |
| Traces and metrics | OTLP → streams → tables (§16 §6) | Continuously |

### 9.2 The session workflow

```
session(id, task):                                   # a Resonate durable function
  ws   = ctx.run(fork_branch, task.repo, id)         # step: create agent/<id>
  env  = ctx.run(resolve_env, task.repo)             # step: env_key → image (build once)
  turn = 0
  loop:
    r = ctx.run(agent_turn, id, turn, ws, env)       # step: one harness turn in a sandbox
        # value = {commit, home_tree, stop_reason, tokens, consistency_token}
    if r.stop_reason in (done, failed, budget): break
    if r.needs_human: ctx.promise("approve-"+id).await   # durable human-in-the-loop wait
    turn += 1
  ctx.run(finish, id, r.commit)                      # push branch / open review
```

- **`agent_turn`** starts (or reuses) a sandbox mounted at the previous step's `commit` and `home_tree`, runs the harness non-interactively in resume mode (`claude -p --resume <session>`, `codex exec resume <session>`, `opencode run --session <session>`; verify exact flags per version) for one turn or a bounded number of tool calls, then checkpoints and returns. Settled steps are never re-run on replay, so model calls already paid for are not repeated.
- **Crash recovery:** if a sandbox or host dies mid-turn, the Resonate task lease expires, another worker acquires the task (fenced by its version), replays settled steps from their memoized values and re-runs only the interrupted turn from the last checkpoint.
- **Fork and rewind:** any settled step's `{commit, home_tree}` is a restore point: fork N sessions from turn *k* to explore alternatives (tree-of-thought, parallel evals), each sharing every lower layer.
- **Human in the loop:** approvals are durable promises; a session can wait days at no compute cost (no sandbox runs while it waits).

### 9.3 Storing sessions

- The `sessions` stream receives one record per message, tool call and turn result (from OTLP spans and events, and from the harness's session files parsed with `tokscale-core`, §16 §6).
- A link keeps `agent_sessions` (one row per turn: harness, model, tokens, cost, tools used, duration, outcome) and `session_history`, a collection over transcripts, so agents can search past sessions as memory (hybrid search, §05 §4).
- Session files in `/agent-home` are the harness's own resume state; the stream is Operon's queryable copy. Both survive the sandbox.
- **Result:** merge within the Operon repo, or push the branch to the GitHub mirror.

## 10. MCP: Operon's server and the MCP gateway

Target spec: **MCP 2026-07-28**, which makes the protocol stateless: no `initialize` handshake and no `Mcp-Session-Id`; every request carries its protocol version and client capabilities in `_meta`; `server/discover` advertises versions; `Mcp-Method` / `Mcp-Name` headers let gateways route without parsing JSON; list results carry `ttlMs` and `cacheScope`; server-initiated requests are replaced by multi-round-trip `input_required` results; long-running work uses the Tasks extension; OpenTelemetry context travels in `_meta` (`traceparent`).

### 10.1 Operon MCP server (W0)

- Tools: `search` (hybrid over collections, including code), `sql`, `cypher`, `memory_write`, `repo_read` / `repo_diff` / `repo_log` at a ref, `session_search` (past sessions).
- Stateless by the spec, so any gateway node answers any request; OAuth maps to a namespace. Library: the official Rust MCP SDK (`rmcp`; license and 2026-07-28 support to verify).
- Small and immediately useful to every Claude Code, Codex and opencode user, so it is proposed for M1, independent of the rest of this document.

### 10.2 MCP gateway with tool retrieval (W1)

Agents with many MCP servers pay for every tool definition in every request. The gateway fronts all of a namespace's MCP servers and sends the model **only the tool definitions a task needs**.

- **Catalog:** the gateway reads `tools/list` from each registered server (cached for `ttlMs`) and upserts every tool into a collection (name, description, parameter names; BM25 + embedding) and a **tool graph**: `(server)-[:PROVIDES]->(tool)`, `(tool)-[:REQUIRES]->(tool)` (from schemas and docs, e.g. `create_pr` needs `push_branch`), and `(tool)-[:CO_USED {weight}]->(tool)` learned from session traces.
- **Retrieval (Graph RAG-Tool Fusion, arXiv 2502.07223):** hybrid search over the catalog seeds candidates, a 1–2 hop expansion over `REQUIRES` / `CO_USED` adds their dependencies, and a rerank keeps the top *k*. One native Operon query (§05 §4).
- **Delivery, spec-compliant:** under 2026-07-28 `tools/list` must not vary per connection, so the gateway exposes a fixed pair of meta-tools, `find_tools(query, k)` → matching tool definitions, and `call_tool(name, arguments)` → validated against the tool's JSON Schema and proxied with the caller's credentials. Works with any client (Codex, opencode, Claude Code).
- **Clients with native deferral:** Claude Code defers MCP tool definitions by default (`ENABLE_TOOL_SEARCH`) and the Anthropic API offers a tool search tool with `defer_loading`; the gateway can also serve the full catalog to them and let the client search. The demo compares both (§16).
- **Older servers:** for servers on 2025-11-25 the gateway probes with `server/discover`, holds the upstream session itself and presents a stateless face to agents.
- **Tracing:** the gateway propagates `traceparent` from `_meta`, so an agent's LLM span, the MCP call and the Operon query that served it share one trace.

## 11. Object layout additions

```
ns/<ns>/repos/<repo_id>/{refs, packs/, commit-graph/}
ns/<ns>/cas/packs/<ulid>.{pack,idx}          # chunks: env images, LFS, package caches, artifacts
ns/<ns>/envs/<env_key>/manifest               # environment image manifest (nydus bootstrap)
ns/<ns>/cache/<tool>/…                       # sccache, bazel-remote, turbo
```

GC: reachability from `refs` documents and env manifests (env images retained by last use), cache entries by age (§03 §7).

## 12. Design targets (not measurements)

| Operation | Target |
|---|---|
| Fork a repo or workspace | One conditional PUT, or none (overlay only) |
| Sandbox start, warm env image, warm node cache | < 1 s to first command |
| Sandbox start, cold node | Manifest GETs + lazy reads of only what is touched |
| Checkpoint | Upload of changed files only, as one pack + one `refs` CAS |
| Small files | Never one object per file |

## 13. Phasing

| Phase | When | Scope | Exit gates |
|---|---|---|---|
| **W0** | With M1 | Operon MCP server on the 2026-07-28 stateless spec | Claude Code, Codex and opencode use Operon tools over MCP |
| **W1** | After M2 | Repos (smart HTTP v2, forks, partial clone, repack/GC, LFS, GitHub import); code-index links; credential vending; OTLP ingest of agent telemetry; MCP gateway with graph-based tool retrieval; session workflows on Resonate with stored sessions (§9) | Client matrix (git, gitoxide, libgit2, JGit) passes clone/fetch/push/partial clone; 10k concurrent forks; Claude Code and Codex complete a task end to end with Operon as the remote |
| **W2** | After W1 | `operon-sandbox`: `Runtime` trait with microsandbox, Firecracker, gVisor and process backends; lazy workspace mount, env images via nydus, package-cache layer, registry proxy (PyPI, npm, crates, Go), sccache wiring; the 100-agent fleet demo (§16) | Warm-env start target met; installs work with egress limited to Operon; kill a sandbox mid-task and resume on another host from the last checkpoint with an identical workspace |
| **W3** | Phase C | `jj` backend, REAPI CAS/AC, VM snapshot storage, Turborepo/Nx caches | — |

## 14. Non-goals

- Not a sandbox runtime or VM host; no untrusted code in Operon processes.
- Not a distributed POSIX filesystem and no concurrent multi-writer workspaces. For shared POSIX datasets use JuiceFS (Apache-2.0) or Amazon S3 Files.
- Not a GitHub replacement: no pull-request UI, issues or CI (webhooks and mirroring only).
- Not a secrets manager.

## 15. Alternatives considered

| Option | License | Verdict |
|---|---|---|
| AgentFS (Turso) | MIT (Rust) | SQLite-backed copy-on-write overlay + KV + tool-call audit. Its object-storage ("disaggregated") version is described by its author as "a direction, not a finished system". Reference; possible upper-layer format |
| Amazon S3 Files | AWS service (GA 2026-04) | NFS mount of a bucket with "stage and commit" roughly every 60 s. AWS-only, no forks or versions; complementary |
| JuiceFS | Apache-2.0 (Go) | POSIX on S3 with a separate metadata engine and `clone`; heavy and unversioned for per-agent workspaces |
| mountpoint-s3 | Apache-2.0 (Rust) | Key-per-file mapping, read-mostly; reference for FUSE-on-S3 performance |
| Cloudflare Artifacts, Freestyle Git | Closed | Validate "Git as the agent filesystem"; references |
| aggit | MIT (Rust) | Small S3-backed, Git-versioned store for agents; reference |
| ZeroFS | AGPL-3.0 | **Avoid** (license) |
| Daytona | AGPL-3.0 | **Avoid** (license, maintenance) |

## 16. Open questions

1. Scope of the Git server Operon must build on gitoxide (protocol v2 only? v0/v1 for old clients?).
2. Whether nydus can read chunks through `object_store` or needs an S3-compatible endpoint on Operon's cache.
3. FUSE vs virtiofs vs EROFS + fscache per runtime, and privileges in Kubernetes pods.
4. A cross-ecosystem definition of `env_key` (lockfile sets, native build steps, CPU architecture).
5. Cross-namespace dedup policy for public packages.
6. Rust MCP SDK license; OpenTelemetry coverage in Claude Code and Codex.
7. Whether repos become a sixth object kind (with an implicit stream) or stay a service like durable execution.
