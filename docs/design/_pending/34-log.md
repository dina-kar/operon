# Pending log entries for §34 (branch `gateway-runtime-design`)

For the integrator. Reserved ranges: **D360–D379**, **Q360–Q379** (Q360–Q374 used). **Amended 2026-10-02** ([§38](../38-knative-authentik-gitops.md) D440): the protocol gateway moved to `loam-platform`, so D366–D371, D373, D377, D379 and Q360, Q363–Q365, Q370, Q371, Q373, Q374 are removed from this log (they are `loam-platform`'s, under its own numbering); §34 on `main` is a stub that keeps the rows below. Merged in #177 but not yet integrated into the decision log. Source: the owner's "Loam Serverless Runtime — Consolidated Plan" v1 (2026-09-30), `chatdump.md` §1–§12 and §15, folded in on 2026-10-01. §13 (Cloudflare) and §14 (Loam Git) of that draft belong to another document.

## 1. Decision rows (paste at the bottom of the Decisions table in `13-decision-log.md`)

| ID | Date | Decision | Rationale | Status |
|---|---|---|---|---|
| D360 | 2026-10-01 | **The standards charter** (§34 §1): every external standard Loam speaks is pinned to a spec version behind one crate (HTTP/2 and mTLS, Connect/gRPC/gRPC-Web, CloudEvents 1.0.2, Arrow/Parquet/Iceberg, protobuf with Avro only at schema-registry sinks, OTel and W3C Trace Context, the Resonate protocol, `loam.meter.v1`). Longevity is replaceability: one crate per standard, a decision-log row per standards choice, deprecation as announce → dual-run (at least one minor release) → sunset, open formats only in stored data and no cloud-specific ids, key and algorithm ids on every signature, `cargo deny`/`cargo vet`/SBOMs/reproducible releases/MSRV, restore drills, a dependency review every 3–5 years | The owner's draft (§1, §11): "built for 50 years" means the wire contracts, data formats and semantics outlive every implementation | Proposed |
| D361 | 2026-10-01 | **Transports** (§34 §1): HTTP/2 with mTLS (ALPN `h2`) between Loam components, h2c only on loopback; HTTP/3 only at the Envoy edge (D176, D184) until an internal-mesh flag (Q372); external HTTP/1.1 traffic is re-originated as h2 by Envoy | Dapr and Rust's `h3` are behind h2; edge providers already terminate h3. Consistent with D176, whose HTTP/3 is at Envoy | Proposed |
| D362 | 2026-10-01 | **Connect-RPC through connect-rust for every new service** (§34 §1): one handler serves Connect, gRPC and gRPC-Web. Checked 2026-10-01: `connectrpc` 0.9.1 (2026-09-21, Apache-2.0) is the official Connect project's Rust implementation (Connect RFC 007, now `connectrpc/connect-rust`) and passes the full Connect conformance suite (3,600 server tests). The draft's `tonic` + hand-written-codec fallback is not needed. Extends D128 and D206 | The draft asked to verify maturity; the workspace already uses it for `loam.live.v1` | Proposed |
| D363 | 2026-10-01 | **The narrow waist is `loam.stream.v1.StreamService`** (`Produce` and D270's `ProduceCloudEvents`, PR #171): Dapr pub/sub, HTTP push, runner hosts and runtime hooks are adapters into it (commercial adapters in `loam-platform` use the same service). `buf breaking` (`FILE`) is enforced in CI on `proto/loam/{stream,events,meter}` and `crates/operon-stream-grpc/proto`; `loam.live.v1` joins when R2 freezes it; evolution is compatible-only (§34 §1) | One owned internal contract; `buf.yaml` already names `FILE`, unenforced since R1 | Proposed |
| D364 | 2026-10-01 | **The Loam CloudEvents profile** (§34 §2): CloudEvents 1.0 plus required `tenantid` (`<org>/<namespace>`, stamped by the gateway or runner host from the credential; a client value is overwritten and counted) and `traceparent` (W3C, generated at the first hop if missing; `tracestate` optional). `id` is the idempotency key (D270's `source` + `id`), so no `idempotencykey` extension; versions are the `type` suffix `.v<major>` plus `dataschema` = `urn:loam:proto:<message full name>`, so no `schemaversion` extension; types are `io.loams.dev.<domain>.<name>.v<major>` (owner ruling 2026-10-01, answering Q361) | Two idempotency keys would disagree; CloudEvents already has `dataschema`; §02 §7.4's `dev.loam.stream.record` moves to the owner's prefix with the rename PR | Proposed |
| D365 | 2026-10-01 | **High-rate events bypass the dedup ledger** (§34 §3): usage events and other per-request events are batched plain produce in D270's Kafka binary-mode record layout (`ce_` headers; flush every 50 ms or 1 MiB; bounded queue of 65 536 events or 64 MiB that drops the oldest and counts `loam_events_dropped_total`); deduplication by `(source, id)` happens in the keyed event table, not at ingest | D270's ledger costs two metastore proposals per request; §02 §7.4 already says bulk ingest should use plain produce | Proposed |
| D372 | 2026-10-01 | **Events → Arrow** (§34 §4): one Arrow schema per event type, derived from the data message's protobuf descriptor (`buffa-descriptor`); attribute columns, `time` as `Timestamp(Nanosecond, "UTC")`, `ext` as a string map, `data` as a typed struct, `data_raw` as the original bytes (on by default); proto path and tag in field metadata; Iceberg field ids assigned by the catalog and matched by proto path; additive-only evolution checked in CI; event tables keyed on `(source, id)`, partitioned by `day(time)`, sorted by `(type, time)`; ns timestamps need Iceberg v3, else a `time_ns` column (Q368) | No official CloudEvents Arrow format exists (v1.0.2 defines JSON, protobuf and Avro; drafts add Avro compact, CBOR and XML) | Proposed |
| D374 | 2026-10-01 | **State rules** (§34 §1): the tenant scope on every stored key in Loam's existing forms (`ns/<ns>/` in the bucket, keyspace plus prefix in TiKV), not a literal `tenant/{id}/`; money as `int64` micros plus ISO 4217 currency; `int64` ns UTC in new protobuf contracts and Arrow (existing versioned contracts keep their units); TiKV only through transactions or atomic CAS | The draft's rules, mapped onto Loam's layout | Proposed |
| D375 | 2026-10-01 | **One `Runner` trait** (§24 §16; amends §24 §3): `kind`, `capabilities`, `deploy` (idempotent by digest), `invoke` (returns the response and, except for the supervisor, a `Usage`), `undeploy`, `health`. `SupervisorRunner` (Loam's nodes, §24's tiers, built with F1) is the default and the only runner with D170's placement advantage; `ProcessRunner` (dev and tests) and `LambdaRunner` (Rust, arm64, `provided.al2023`, Loam's bootstrap) in RN1; `KnativeRunner` in MT2 (§38 D441); runners outside this repository as `RunnerKind::External` (the commercial Cloudflare runner, `loam-platform`); Cloud Run and Container Apps on demand (Q367). External runners run no Dapr; D183's one shared `daprd` is unchanged | The draft's four co-equal targets would give up placement next to the data, which is the product's advantage (§24 §9) | Proposed |
| D376 | 2026-10-01 | **Usage from every runner reaches §27's contract** (§27 §3.6): exactly one reporter per invocation (the supervisor for its tiers, `RunnerHost` for external runners); Lambda CPU from the bootstrap's `getrusage(RUSAGE_SELF)` delta in a bootstrap-owned `x-loam-usage` header, capped by the `REPORT` line's billed duration × the fractional CPU share `memory_mb / 1 769` (Q366, which gates RN1 Task 5); the Tail Worker join for the commercial Workers runner is specified in §27 §3.6 and built in `loam-platform`; `loam.meter.v1.Invocation` gains `runner` (12), `region` (13), `provider_billed_ms` (14), `compile_usec` (15), `overhead_usec` (16); the socket gets a framing envelope (`HostMessage`/`ConsumerMessage`, RN1 Ruling 1); the CloudEvent form `dev.loam.meter.usage.v1` is specified but built in `loam-platform` (§38 D440). Platform CPU (supervisor, `loam-dapr`, `daprd`, Envoy) never lands in tenant cgroups; compile CPU is reported apart. Rating, the ledger and reconciliation against provider invoices stay `loam-platform` (D190, D202). Refines D201 | The draft's metering section, reconciled with D190/D202 and the owner's 2026-10-02 ruling: hooks only in this repository, one contract for every runner | Proposed |
| D378 | 2026-10-01 | **Languages and embedding** (§34 §1): Rust for the data plane; Java (Quarkus) and Go through `buf`-generated Connect clients; in-process embedding only after profiling (Java FFM, final since JDK 22, with `jextract` over a `cbindgen` header; Go cgo, whose baseline overhead Go 1.26 cut by ~30%; the Arrow C Data Interface; Wasm under Chicory or wazero). No core path depends on Go or Java SIMD: Go 1.27 (August 2026) adds a portable `simd` package and arm64/Wasm `simd/archsimd`, both still behind `GOEXPERIMENT=simd`; the Java Vector API is in its eleventh incubator in JDK 26 (JEP 529), waiting on Valhalla | Checked 2026-10-01 against the Go 1.26 and 1.27 release notes and JEP 529; JIT and GC CPU work against CPU-billed tenants | Proposed |

## 2. Open-question rows (paste at the bottom of the Open questions table)

| # | Question | Owner | Needed by |
|---|---|---|---|
| Q361 | Event type prefix: `dev.loam.` (as §02 §7.4 builds) or `dev.loams.` (the registered domain), for §02's synthesized type and D364's types alike (§34 §2) | Founder | Answered by the owner 2026-10-01: `io.loams.dev.` (§34 §2) |
| Q362 | Showback and single-organisation billing in the open repository or `loam-platform` only. **Answered by the owner 2026-10-02: no metering in OSS** (§38 D440, D444) | Founder | Resolved |
| Q366 | Lambda CPU attribution: the bootstrap's `getrusage` delta capped by billed duration × `memory_mb / 1 769`, or billed duration as the meter on Lambda (§24 §16, §27 §3.6); gates RN1 Task 5 | Founder | RN1 Task 5 |
| Q367 | Cloud Run and Container Apps runners: build or document only (§24 §16) | Founder | After RN1 |
| Q368 | Iceberg v3 `timestamptz_ns` on the pinned iceberg-rust and Lakekeeper by M4, or a `time_ns` column (§34 §4) | Eng | Event-table plan |
| Q369 | Move `operon-stream-grpc` from tonic/prost to connect-rust/buffa (D128), and when (§34 §5) | Eng | M2 stream API plan |
| Q372 | Internal HTTP/3: the condition that enables it (§34 §3) | Eng | Later |

## 3. README and roadmap rows

### 3.1 `docs/design/README.md`, reading-order table (after row 33, or after the last row present)

| 34 | [Standards charter and the narrow waist](34-protocol-gateway-and-standards.md) | Stub since 2026-10-02 (§38 D440): the protocol gateway is a commercial component in `loam-platform`. Keeps the vendor-neutral decisions: the standards charter, transports, Connect-RPC, `loam.stream.v1` as the narrow waist, the Loam CloudEvents profile, the high-rate path, the events → Arrow mapping, state rules; the `Runner` trait lives in §24 §16 and runner usage in §27 §3.6 | **Proposed** |

### 3.2 `docs/plans/README.md`: a new section after "Track D" (or after the last track section)

```markdown
## Track RN: runners and the usage hooks

Design reference: [24 CPU-time runtime](../design/24-cpu-time-runtime.md) §16 (the `Runner` trait, D375) and [27 Usage hooks](../design/27-usage-hooks.md) §3.6 (D376); [34](../design/34-protocol-gateway-and-standards.md) keeps the decision rows. Hooks only: no metering in this repository (§38 D444).

| Plan | Scope | Depends on | Status |
|---|---|---|---|
| [RN1: Runner trait, external runners, usage reporter](2026-10-01-rn1-runner-usage.md) | `loam.meter.v1` with §27 §3.6's fields and the socket framing; `operon-meter` (host-report emitter, test consumer); `operon-runner` (`Runner`, `RunnerHost` with one reporter per invocation, conformance kit); `ProcessRunner`; `LambdaRunner` with Loam's Lambda bootstrap | — | Planned |
```

### 3.3 `docs/design/12-roadmap-testing-risks.md`

No milestone row: RN1 is a small track beside F (§24 §11). Risk register row 33 of the earlier version of this log (ad-tech scope) is withdrawn with the gateway.

## 4. Conflicts with existing decisions

| # | The draft says | Existing | Proposed resolution |
|---|---|---|---|
| 1 | "Durable orchestration: Resonate on TiDB"; §4.3 "Resonate authoritative store: TiDB (our fork)"; build order item 6 "(TiDB)" | **D260** (no TiDB anywhere), **D261** (Resonate on the native TiKV backend; `mysql://` legacy) | D261 stands; the TiKV store is on `main` (PR #114, `--durable-store tikv://…`, feature `durable-tikv`) |
| 2 | Correction 2: "Resonate is not backed by TiKV … Use TiDB" | **D261**, PR #114 | True of upstream Resonate only; Loam's fork has `resonate-server-tikv`. Correction withdrawn (§24 §16) |
| 3 | §4.3 metastore backends "redb, Postgres, TiDB" | **D124**, **D179**, **D260**, **D58** | openraft/redb (dev, standalone), TiKV (clusters), Postgres and DynamoDB (M2); no TiDB backend |
| 4 | §4 diagram: event log "NATS/S3" | **D4**, **D72**, **D270**; **D176**, **D183** | Loam streams are the event log; NATS only as a long-tail binding through the shared `daprd` |
| 5 | §7 metering CloudEvents into Iceberg tables and reconciliation against provider invoices, in this repository | **D190**, **D202**, **D220** | The record spec and collectors are open (D376); the ledger, Iceberg meter tables and reconciliation are `loam-platform` |
| 6 | §10 licensing table: "Metering agent" and "Self-hosted single-org billing" open | **D202** (the node agent reading the hooks is Loam Cloud), **D220** | Collectors that produce the hooks are open; the aggregating agent and billing stay in `loam-platform`; showback over open dashboards proposed open; owner decides (Q362) |
| 7 | §10 "Open decision: keep the multi-tenancy layer closed …?" | **D220** (approved 2026-09-29) | Answered by D220, reconfirmed by the owner on 2026-10-02 (§38 D440) |
| 8 | §4.1 required extension `idempotencykey` | **D270** (dedupe on `source` + `id`) | Not adopted; `id` is the idempotency key (D364) |
| 9 | §4.1 required extension `schemaversion` | CloudEvents `dataschema`; **D270** | Not adopted; `type` suffix plus `dataschema` URN (D364) |
| 10 | §6 event types `io.loam.<domain>.<name>.v1` | §02 §7.4 (**D270**) synthesizes `dev.loam.stream.record` | The owner ruled `io.loams.dev.` on 2026-10-01; both move with the rename PR (§34 §2) |
| 11 | §4.3 "every key is prefixed `tenant/{id}/…`" | **D25**, §03 (`ns/<ns>/`), §20, §26 §6.8 (**D214**) | Kept in Loam's existing forms (D374) |
| 13 | §4.3 "timestamps are i64 nanoseconds" | §27's `HostReport` (ms, µs; **D201**); §08 §2 (ns needs Iceberg v3) | New contracts use ns; versioned contracts unchanged; Iceberg v3 or `time_ns` (D372, Q368) |
| 15 | §3 "Connect-RPC in Rust: verify … fallback is tonic plus a small Connect codec" | **D128**, **D206** | Verified; no fallback (D362). `operon-stream-grpc` is still tonic (Q369) |
| 16 | §4.4 "Dapr runs as a sidecar where needed" | **D183** | One shared `daprd` per cluster; none on external runners (D375) |
| 17 | §4.4 Workers "call Resonate HTTP gateway" | **D138**, **D111** (loopback-only listeners until the unified auth plan) | The Workers runner is a commercial component in `loam-platform` (§38 D440) |
| 18 | §4.4 four co-equal runner targets | **D170** (placement next to data is the advantage) | `SupervisorRunner` default; others are options (D375) |
| 19 | §3 "HTTP/3 opt-in" | **D176** (HTTP/3 in phase 1) | No conflict: D176 is at the Envoy edge; inside, h2 (D361) |
| 21 | The brief's pointer "D262: TiKV backend" | **D261** is the TiKV backend; **D262** makes the `durable` feature opt-in | Cited as D261 throughout §34 |

## 5. Web checks recorded in §34 (2026-10-01)

| Claim in the draft | Finding | Source |
|---|---|---|
| Rust Connect-RPC maturity unclear | `connectrpc` 0.9.1 (2026-09-21), official Connect project, full conformance pass, Apache-2.0; the workspace already pins it | Connect RFC 007; `connectrpc/connect-rust` releases |
| Go `simd/archsimd` amd64-only in 1.26; 1.27 RC1 portable package — "check whether 1.27 shipped and whether the flag is gone" | Go 1.27 shipped August 2026; portable `simd` plus arm64 Neon and Wasm in `archsimd`; **still `GOEXPERIMENT=simd`** | go.dev/doc/go1.27 |
| Java Vector API "still incubating in Java 25" | Tenth incubator in JDK 25 (JEP 508), eleventh in JDK 26 (JEP 529), incubating until Valhalla previews | openjdk.org/jeps/529 |
| Go 1.26 cgo overhead dropped ~30% | Confirmed: "The baseline runtime overhead of cgo calls has been reduced by ~30%" | go.dev/doc/go1.26 |
| Cloudflare Workers CPU pricing (re-verify) | Unchanged since §24's check: $5/month, 10 M requests and 30 M CPU-ms included, $0.30/M requests, $0.02/M CPU-ms, no duration charge, 5 min CPU max | developers.cloudflare.com/workers/platform/pricing |
| "No official Arrow mapping for CloudEvents" | Confirmed | `cloudevents/spec` v1.0.2 `formats/`, `working-drafts/` |
| Workers CPU visible per invocation | Tail Workers and trace events carry `CPUTimeMs` and `WallTimeMs` (since 2025-04-09) | Cloudflare changelog |
