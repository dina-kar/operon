# 14 — Durable Execution (Resonate Protocol)

Status: **Approved** (direction, user) · 2026-09-24. Items marked (verify) are resolved in the implementation plan.

AI agents need more than memory and retrieval. A multi-step agent run calls models and tools for minutes or hours, waits for humans, fans out sub-tasks and must survive crashes without redoing paid work. Today that means a sixth system (Temporal, or a queue + cron + Postgres). Operon serves the **Resonate protocol** instead, so agent workflows run durably against the same bucket that holds their memory, search indexes and traces.

---

## 1. What Resonate is

[Resonate](https://github.com/resonatehq/resonate) (Apache-2.0, Rust server, SDKs in TypeScript, Python, Rust, Go and Java) implements *distributed async/await*: ordinary async functions whose every step is recorded as a **durable promise** on a server, keyed by a deterministic id. After a crash the function replays; settled steps return their memoized value, pending ones suspend.

Protocol vocabulary (from the Lean specification, `spec/spec/01-protocol/types.lean`):

| Entity | Fields | States / operations |
|---|---|---|
| Promise | `id`, `param`, `value`, `tags`, `timeoutAt`, `createdAt`, `settledAt` | `pending` → `resolved` \| `rejected` \| `rejected_canceled` \| `rejected_timedout`; `get`, `create`, `settle`, `register_callback` (awaiter ↔ awaited), `register_listener` (address), `search` |
| Task | `id` (= its promise id), `version` (fencing token), `pid`, `ttl`, `resumes` | `pending` → `acquired` → `suspended` \| `halted` \| `fulfilled`; `create`, `acquire`, `fence`, `heartbeat`, `suspend`, `fulfill`, `release`, `halt`, `continue`, `search` |
| Schedule | `id`, `cron`, promise template | `create`, `get`, `delete`, `search`; fires promises on the cron |
| Timeouts | — | promise timeout, task lease timeout, task retry timeout, schedule timeout |

**Origin:** everything before the first `:` of an id. Every protocol operation except searches is **single-origin** (an awaiter and its awaited must share an origin), so one atomic write of one origin's state commits any transition.

Workers receive tasks through transports: HTTP push (Resonate calls the worker), HTTP long-poll / SSE (the worker holds a connection) and Google Pub/Sub.

## 2. Why this fits Operon

1. **It already runs on a bucket.** Resonate's `resonate-server-blob` crate stores each origin as one canonical document at `wf/<origin>` and commits every transition with one conditional PUT (`If-None-Match: *` / `If-Match: <etag>`). Deadlines are zero-byte timer objects `t/<NN>/<deadline>_<target>@<token>`. It needs no log, lock or consensus, and it is built on **`object_store` 0.14** — the same crate as `operon-store`.
2. **It is a plugin architecture.** A Resonate server is assembled from *server* (storage), *worker* (transport) and *gateway* (edge) plugins behind the `ResonateServer` trait (`resonate-core`). Operon registers its own plugins; it does not fork the protocol.
3. **It is formally specified and differentially tested.** Every storage engine is compared step by step against an executable oracle on randomized traffic, with a linearizability checker and a trace checker against the Lean/TLA+ models. An Operon backend inherits that harness as its conformance gate.
4. **Its task leases match Operon's model.** Task `version` is a fencing token, exactly like the metastore's lease epochs (§09 §3), so zombie workers are rejected the same way.

## 3. Architecture

```
 Resonate SDK (TS/Py/Rust/Go/Java) ──HTTP──► gateway role: resonate-gateway-http (axum)
                                                   │ auth → namespace
                                                   ▼
                                     ResonateServer (per namespace)
                                     = resonate-server-blob kernel over operon-store
                                                   │ one conditional PUT per origin batch
                                                   ▼
             s3://<bucket>/<cluster_prefix>/ns/<ns>/durable/{wf,sched,t}/…
                                                   │
                        transports: HTTP push / HTTP poll ──► agent workers
```

- **Role:** the Resonate gateway runs in the `gateway` role, enabled per cluster like any other surface (§10 §2). It is stateless; all state is in the bucket.
- **Tenancy:** an API key is bound to a namespace, so a request's credentials select the namespace; no URL rewriting is needed. A per-namespace path prefix is the fallback if an SDK cannot send auth headers (verify SDK support for base paths and headers).
- **Storage:** each namespace's durable state lives under `ns/<ns>/durable/` (§01 §6). A `ResonateServer` instance per active namespace is created on first use and evicted when idle.
- **Routing:** requests for one origin are routed to one gateway node by rendezvous hashing on `(namespace, origin)` (the §04 affinity scheme), so Resonate's per-origin actor and group commit batch that origin's burst into one PUT. Correctness does not depend on routing: the blob server validates every cached read with `If-None-Match: <etag>`, so several nodes serving one origin stay linearizable (they only contend).
- **Metastore:** not used in Phase A. Durable-execution traffic is user data and must not load the Raft group (§01 §3.2).

## 4. Phases

### Phase A — the Resonate surface on the blob backend (M3)

- Fork, pinned to a git revision (the crates are not on crates.io): `resonate-core`, `resonate-plugin`, `resonate-gateway-http`, `resonate-server-blob`, `resonate-transport-http-push`, `resonate-transport-http-poll`. Workspace version at research time: 0.10.1.
- Hand the blob server an `object_store` built by `operon-store` (so fault injection, provider conformance and credentials are shared) with the namespace prefix.
- Replace `resonate-auth` with Operon's authN/Z (§10 §4); keep the protocol and error codes byte-compatible.
- Promise and task search keep the blob backend's semantics: a scan of the namespace's documents, correct but not atomic and not fast. Off by default for large namespaces.

**Exit gates:** Resonate's TypeScript and Python SDK test suites pass unmodified against Operon; Resonate's differential and linearizability harness passes against a 3-gateway Operon deployment over one bucket, including object-store fault injection (412/409/5xx, lost responses).

### Phase B — Operon-native value (M4)

1. **Search and observability via a change stream.** After each committed origin write, the server appends the changed promises and tasks to the namespace's `durable_events` stream (at-least-once, idempotent by `(origin, generation)`). A link maintains a keyed table `system.durable_promises` and `system.durable_tasks`, so SQL and the Resonate `search` operations run against an index instead of a scan. A worker repair sweep re-emits documents whose generation is ahead of the index. These reads are eventually consistent, as Resonate's searches already are.
2. **Execution graphs.** The same events feed a mapped Operon graph (§07): promises as vertices, callbacks as edges, so a call tree is a `graph_expand` from its root promise (Resonate's Neo4j backend does the same in Neo4j).
3. **Cluster-wide timer shards.** Phase A keeps timers per namespace, which is fine for thousands of active namespaces but makes timer scanning grow with namespace count. Phase B moves timer objects to cluster-level shards (`durable/t/<NN>/<deadline>_<ns>_<target>@<token>`), each shard leased to one worker through meta leases, so one sweeper per shard lists only due deadlines.
4. **Low-latency namespaces.** Place a namespace's `durable/` prefix on the `express` zonal buckets (§02 §2) for single-digit-ms transitions (verify that S3 Express One Zone supports `If-Match` on PUT).
5. **Workers on streams (M5+).** A transport plugin that publishes tasks to an Operon stream, so named consumers of the native streaming API (§02 §7) can serve as a worker pool.

## 5. Consistency and failure model

| Scope | Guarantee |
|---|---|
| One origin | Linearizable: each transition is one conditional write, decided against the latest document (Resonate's checker verifies this) |
| Across origins | Independent; the protocol never asks for cross-origin atomicity |
| Searches | Surveys, not atomic (Phase A: document scan; Phase B: eventually consistent index) |
| Workflow step + Operon data write | Not atomic, but idempotent: write data keyed by the step's promise id (an upsert), and return the write's consistency token as the step's value so later steps read their own writes |

Failure behavior comes from the blob backend's effect order — **arm deadline → commit document → disarm old deadline → send messages → answer** — which leaves every crash window in a state a timer or a client retry repairs (see `impl/server/s3/docs/on-s3.md` upstream). A lost gateway node loses no state; requests are retried against another node.

## 6. Cost

Each transition costs one conditional PUT ($0.005 per 1,000 on S3 Standard) plus validated GETs (no body when unchanged). A workflow step is typically 2–3 transitions (create, acquire, settle); group commit folds a burst on one origin into one PUT. One million steps per day is therefore on the order of $10–15/day in PUTs before batching. Timer objects add one PUT and one DELETE per armed deadline.

## 7. Non-goals

- Not a general workflow product: no workflow DSL, no visual designer beyond Resonate's own console (`resonate-gateway-web`, optional).
- Not a replacement for Resonate's SDKs: Operon ships no SDK of its own for durable execution.
- No cross-origin transactions and no transactional coupling with collection/table writes (see §5 for the idempotent pattern).

## 8. Open questions

1. SDK support for per-namespace auth headers or base paths (verify per SDK).
2. `If-Match` support on S3 Express One Zone, GCS Rapid and Azure for the low-latency option.
3. Whether Phase B's change stream should be emitted by a fork of the blob kernel or by a wrapper `ResonateServer` that diffs documents before and after `process`.
4. Upstream relationship: contribute the Operon server plugin back, or keep it in-tree.
