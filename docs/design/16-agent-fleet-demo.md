# 16 — Demo: 100 Coding Agents on Operon

Status: **Proposed** · 2026-09-24. Builds on §14 (durable execution) and §15 (agent workspaces). Every number below is a design target to be measured, not a result.

**The claim:** one host and one bucket run 100 concurrent coding-agent sessions (Claude Code, Codex and opencode), each in its own microVM. Every session is a Resonate durable execution stored in Operon. Every MCP server speaks the stateless 2026-07-28 spec through Operon's gateway, which sends each model only the tool definitions it needs. Every trace, token and dollar lands in Operon's analytics, cross-checked against tokscale.

---

## 1. What the demo shows

| Property | How it is shown |
|---|---|
| **Density** | 100 microVM sandboxes on one KVM host, forked from warm templates, sharing all read-only layers |
| **Durability** | Kill sandboxes, a worker and a gateway mid-run; every session still completes, and no settled step (no paid model call) is repeated |
| **Context efficiency** | Tool-definition tokens per request with all tools loaded, with client-side deferral, and with Operon's graph-based tool retrieval |
| **One system for the whole loop** | Code, environments, sessions, memory, traces and analytics all live in one bucket, queried with SQL, search and Cypher |
| **Honest accounting** | Operon's token and cost tables match tokscale's totals per session |

## 2. Setup

| Item | Choice |
|---|---|
| Host | One bare-metal KVM host (e.g. 64 cores, 256 GiB RAM, 2 TB NVMe); Operon `standalone` on the same host or a second one |
| Storage | One S3 bucket (MinIO for a local run) |
| Sandboxes | `operon-sandbox` with the `microsandbox` backend; `firecracker` for the fleet variant (§15 §8) |
| Harnesses | 34 Claude Code, 33 Codex, 33 opencode sessions, installed from their official channels at env-build time |
| Tasks | 100 tasks from a fixed, public benchmark of real repository issues (e.g. a SWE-bench Verified subset over about 10 repositories; license to verify) |
| Orchestrator | One Resonate workflow per session, plus a parent workflow that fans out 100 sessions (§15 §9) |
| Models | Each harness's default provider; API keys **never enter a sandbox**: the sandbox egress proxy injects them for the allowed model host only (microsandbox supports per-host secret injection) |

## 3. Why 100 fit on one host

1. **Warm templates, forked.** For each `(harness, env_key)`, boot one template sandbox with the harness installed and the environment mounted, snapshot it and fork one sandbox per session. Forks share the template's memory copy-on-write (Firecracker snapshot restore; microsandbox fork), so a sandbox's unique memory is only the pages it dirties.
2. **Shared read-only layers.** Repo trees and environment images are immutable, content-addressed and lazily fetched (§15 §5). All sandboxes on the host read through one NVMe chunk cache, so a dependency is fetched from S3 once per host, not once per sandbox.
3. **Agents mostly wait.** A coding agent spends most wall time waiting on model APIs, so vCPUs are overcommitted (2 vCPUs per sandbox on 64 cores) and memory is ballooned.
4. **No idle sandboxes.** A session waiting on a human is a pending durable promise; its sandbox is stopped and resumed later from its checkpoint (§15 §9.2).

**Budget (targets):** ≤ 512 MiB unique memory per active sandbox; p50 < 1 s from fork to first command with a warm template and warm host cache; ≤ 1 S3 PUT per checkpoint for the workspace pack plus one `refs` CAS.

## 4. Session lifecycle

Each session is the durable workflow in §15 §9.2: fork a branch, resolve the environment, then loop `agent_turn` steps (one resumed harness invocation per step: `claude -p --resume`, `codex exec resume`, `opencode run --session`; verify flags per version), checkpointing the workspace and the harness's session files after each step.

**Chaos script** (run during the demo):

1. Kill 10 random sandboxes at random points in their turns.
2. `kill -9` one Resonate worker and one Operon gateway process.
3. Expire 5 sandboxes' leases by pausing their VMs.

**Assertions:** all 100 sessions reach a terminal state for task reasons only (solved, failed tests, budget), none for infrastructure; per session, the trace shows each settled step's model calls exactly once; every resumed session's workspace equals its last checkpoint byte for byte.

## 5. MCP servers and tool retrieval

- **All MCP traffic** goes through Operon's MCP gateway on the 2026-07-28 spec (§15 §10): stateless requests, `Mcp-Method` / `Mcp-Name` routing headers, cacheable `tools/list` (`ttlMs`, `cacheScope`), `traceparent` in `_meta`.
- **Catalog:** about 20 servers and 300 tools: Operon (search, SQL, Cypher, memory, repo, sessions), a Git hosting mock (issues, pull requests, reviews), documentation search, a database, a browser, a ticketing mock, a chat mock and a set of distractor servers with overlapping tool names.
- **Three arms**, each run over the same 100 tasks:

  | Arm | What the model sees |
  |---|---|
  | A. All tools | Every tool definition in every request |
  | B. Client deferral | Claude Code's built-in MCP tool search (`ENABLE_TOOL_SEARCH`); the other harnesses fall back to A |
  | C. Operon retrieval | Only `find_tools` and `call_tool`; `find_tools` returns the top *k* definitions from hybrid search + tool-graph expansion (Graph RAG-Tool Fusion) |

- **Metrics:** tool-definition tokens per request (measured at the gateway for C, from request payload size for A/B), total input tokens per turn, task success rate, tool-selection recall (did the agent obtain the tool its successful trajectory used), and added latency of `find_tools`.
- **Closed loop:** `CO_USED` edges in the tool graph are recomputed nightly from the sessions table, so retrieval improves with use.

## 6. Traces, sessions and analytics

### 6.1 Ingest

| Source | Path into Operon |
|---|---|
| Claude Code | OpenTelemetry (`CLAUDE_CODE_ENABLE_TELEMETRY=1`, `OTEL_*` exporters; traces via its beta flag) → Operon OTLP/HTTP endpoint |
| Codex | `[otel]` in `config.toml` with an `otlp-http` exporter → Operon OTLP endpoint |
| opencode | OpenTelemetry if available (verify); otherwise its session database, parsed from the checkpointed `/agent-home` |
| All harnesses | Session files in `/agent-home`, parsed by **`tokscale-core`** (MIT, Rust library: session parsing, aggregation, pricing) in a worker link on every checkpoint |
| MCP gateway, Resonate, `operon-sandbox` | Native spans and metrics (tool calls, retrieval, step transitions, fork time, memory, bytes fetched) |

OTLP arrives on streams (`otel_spans`, `otel_logs`, `otel_metrics`, `arrow` encoding) and links materialize Iceberg tables. The tail makes a turn visible in queries within seconds of its end.

### 6.2 Tables

```sql
token_usage   (ts, session_id, turn, harness, model, input_tokens, output_tokens,
               cache_read_tokens, cache_write_tokens, cost_usd, source)      -- source: otel | tokscale
agent_spans   (trace_id, span_id, parent_id, session_id, turn, kind,         -- kind: llm | tool | mcp | query | step
               name, start_ts, duration_ms, attrs)
agent_sessions(session_id, task_id, harness, model, arm, turns, outcome,
               tokens_total, cost_usd, wall_s, resumed_count)
sandbox_events(ts, sandbox_id, session_id, event, fork_ms, rss_mib, bytes_fetched)
```

### 6.3 Analyses (ClickHouse surface, Grafana dashboards)

```sql
-- cost and success per harness and arm
SELECT harness, arm, count() AS sessions, avg(outcome = 'solved') AS solve_rate,
       sum(cost_usd) AS cost, sum(cost_usd) / nullIf(countIf(outcome = 'solved'), 0) AS cost_per_solve
FROM agent_sessions GROUP BY harness, arm ORDER BY harness, arm;

-- prompt-cache effectiveness per model
SELECT model, sum(cache_read_tokens) / sum(input_tokens + cache_read_tokens) AS cache_hit
FROM token_usage GROUP BY model;
```

Other panels: tool-definition tokens by arm, p50/p95 turn latency, tokens per solved task, top tools and co-usage, sandbox fork time and memory, S3 requests per session.

- **Session memory:** transcripts are in the `session_history` collection, so a new session can search how earlier sessions solved similar issues through the Operon MCP server.
- **Parity with tokscale:** for every session, `token_usage` totals from `tokscale-core` must equal `tokscale --json` run over the restored `/agent-home`, and the OpenTelemetry-derived totals are reported beside them, with any gap explained.

## 7. Demo script

1. `operon standalone --bucket s3://demo`; import the task repositories; build the environment images and harness templates (shown once, then cached).
2. Start the parent workflow: 100 sessions fan out; the dashboard shows forks, tokens and cost live.
3. Run the chaos script (§4) while sessions are in flight.
4. Open a session's trace: LLM span → `find_tools` → MCP call → Operon query, one trace.
5. Fork session 42 at turn 5 into 3 alternatives; watch them diverge from shared layers.
6. Approve a session waiting on a human; it resumes on a different host from its checkpoint.
7. Show the three-arm comparison and the tokscale parity table.
8. Show the bucket: repos, environment images, durable documents, streams and Iceberg tables, all under one prefix, readable by DuckDB directly.

## 8. Success criteria (targets)

| Criterion | Target |
|---|---|
| Sessions lost to infrastructure | 0 of 100 |
| Settled steps re-executed after crashes | 0 |
| Fork to first command (warm template, warm cache) | p50 < 1 s |
| Unique memory per active sandbox | ≤ 512 MiB |
| Tool-definition tokens, arm C vs arm A | Reported (measured) reduction, with success rate not lower than arm A |
| Analytics freshness | A finished turn is queryable within 5 s |
| Token parity with tokscale | Exact, per session |

## 9. Roadmap dependencies

| Needs | From |
|---|---|
| Streams, links, workers, leases | M0 |
| Collections (tool catalog, session history, code index) | M1 |
| Graph (tool graph) and the Resonate surface | M2 |
| Iceberg tables and the ClickHouse surface | M4 |
| MCP server, gateway, repos, OTLP ingest, session workflows | W0–W1 (§15) |
| `operon-sandbox`, environment images | W2 (§15) |

Staging: **Demo α** after M2 + W1: 100 sessions on the `microsandbox` backend with Resonate, the MCP gateway and tool retrieval, traces on streams queried with native SQL over the tail. **Demo β** after M4 + W2: the full demo with Iceberg tables, ClickHouse dashboards, environment images and the Firecracker fleet variant.

## 10. Open questions

1. Per-harness headless resume semantics and flags, and how to bound one "turn" per step (`--max-turns` or equivalent).
2. opencode OpenTelemetry support; Codex telemetry coverage in `exec` mode (an upstream issue reports gaps).
3. Harness licensing: Codex (Apache-2.0) and opencode (MIT) may be baked into images; Claude Code is installed from its official channel under its own terms, not redistributed.
4. Model API rate limits for 100 concurrent sessions per provider.
5. Benchmark choice and license, and whether the task set is published with the demo for reproducibility.
