# 34 — The Standards Charter and the Narrow Waist (the protocol gateway moved)

Status: **Stub** · 2026-10-02. The protocol gateway is a commercial component, designed in the private `loam-platform` repository (owner ruling of 2026-10-02: "move Cloudflare, OpenRTB etc. commercial to private repos"; [§38](38-knative-authentik-gitops.md) D440). The OpenRTB and Google adapters, the canonical `loams.rtb.v1` model, per-partner version negotiation, the bid hot path, match/merge for identity profiles, the ad-tech conformance suite and the plans GW1–GW4 left this repository for `loam-platform` (private) with that ruling; D366–D371, D373, D377 and D379 are `loam-platform`'s now, under its own numbering. This repository does not depend on any of it. What stays is the vendor-neutral part that the open engine needs, kept below so that the decisions D360–D365, D372, D374, D375, D376 and D378 keep a home. They are still **proposals** until the owner rules on them (Q450 asks whether they get a document of their own).

---

## 1. What stays open, and where

| # | Decision | Where it lives now |
|---|---|---|
| D360 | **The standards charter**: every external standard Loams speaks is pinned to a spec version behind one crate, so replacing it touches one crate (HTTP/2 and mTLS, Connect/gRPC/gRPC-Web, CloudEvents 1.0.2, Arrow/Parquet/Iceberg, protobuf with Avro only at schema-registry sinks, OTel and W3C Trace Context, the Resonate protocol; the meter record moved to `loam-platform` on 2026-10-02, D548). A decision-log row per standards choice; deprecation is announce → dual-run for at least one minor release → sunset; open formats only in stored data; crypto agility through key and algorithm ids; `cargo deny`, `cargo vet` on new data-plane crates, CycloneDX SBOMs, reproducible release builds | Here |
| D361 | **Transports**: HTTP/2 with mTLS (ALPN `h2`) between Loams components, h2c only on loopback; HTTP/3 only at the Envoy edge (D176, D184) until an internal-mesh flag | Here |
| D362 | **Connect-RPC through connect-rust** (`connectrpc` 0.9.1, Apache-2.0, the official Connect project's Rust implementation) for every new service (D128, D206) | Here |
| D363 | **The narrow waist is `loams.stream.v1.StreamService`**: `Produce` and `ProduceCloudEvents` (D270) are the one internal ingress for async events; Dapr pub/sub, HTTP push, runner hosts and runtime hooks are adapters into it. `buf breaking` (`FILE`) on `proto/loams/{stream,events}` (`meter` moved to `loam-platform`, D548; `control` added by §41) | Here |
| D364 | **The Loams CloudEvents profile** | §2 below |
| D365 | **High-rate events bypass the dedup ledger** | §3 below |
| D372 | **Events → Arrow**: one Arrow schema per event type, derived from the `data` message's protobuf descriptor | §4 below |
| D374 | **State rules**: the tenant scope on every stored key in Loams's existing forms (`ns/<ns>/` in the bucket, keyspace plus prefix in TiKV); money as integer micros plus an ISO 4217 currency; `int64` ns UTC in new protobuf contracts; TiKV only through transactions or atomic CAS | Here |
| D375 | **One `Runner` trait** (`SupervisorRunner` default; process, Lambda and Knative runners) | [§24 §16](24-cpu-time-runtime.md); plan [RN1](../plans/2026-10-01-rn1-runner-usage.md) |
| D376 | **Usage from every runner reaches §27's contract**: one reporter per invocation, fields 12–16 of the private invocation record. **Superseded 2026-10-02 (D548):** the billing-grade record and the reporter moved to `loam-platform` (doc 06); every runner calls the open `InvocationObserver` instead | [§27 §3.7](27-usage-hooks.md), [§41 §10](41-multitenant-byoc-control-plane.md) |
| D378 | **Languages**: Rust for the data plane; other languages through `buf`-generated Connect clients; in-process embedding only after profiling; no core path depends on Go or Java SIMD | Here |

## 2. The Loams CloudEvents profile (D364)

| Attribute | Required | Rule |
|---|---|---|
| `specversion`, `id`, `source`, `type` | yes | CloudEvents 1.0 (D270 validates them) |
| `id` | yes | **The idempotency key.** Producers keep it stable across retries; D270 dedupes on SHA-256(`source` ‖ 0x00 ‖ `id`). There is no `idempotencykey` extension |
| `type` | yes | `<prefix>.<domain>.<name>.v<major>`; a breaking change of `data` is a new major, so a new type. The owner ruled on 2026-10-01 that the prefix is `io.loams.dev` (Q361 answered); §02's `io.loams.dev.stream.record` and §27's `io.loams.dev.meter.usage.v1` moved with the rename PR (D407) |
| `dataschema` | yes for Loams-defined types | `urn:loams:proto:<full message name>`; minor evolution is additive (§4), so there is no `schemaversion` extension |
| `time` | yes | RFC 3339 with nanoseconds; the producer's clock |
| `tenantid` | yes | `<org>/<namespace>`, the same value as `x-loams-tenant` (§27 §3.4). **Set by the gateway or the runner host from the credential**; a client-supplied value is replaced and counted (`loams_events_tenantid_overwritten_total`) |
| `traceparent` | yes (may be generated) | W3C Trace Context; `tracestate` optional; generated at the first Loams hop if missing |
| `partitionkey` | optional | D270: becomes the record key |
| `datacontenttype` | optional | `application/protobuf` for Loams-defined types on the high-rate path, `application/json` at the edge |

## 3. The high-rate path (D365)

D270's ledger costs two metastore proposals per request and about 80 bytes per event for the window, so producers far above webhook rates (runtime usage events, and any adapter that emits an event per request) use plain produce instead:

1. buffer events per `(namespace, stream)` in a bounded queue (default 65 536 events or 64 MiB, whichever first);
2. flush every 50 ms or at 1 MiB as one `Produce` of records in D270's Kafka binary-mode layout (`ce_` headers, `content-type`, key from `partitionkey`, value the protobuf `data`);
3. drop the oldest batch when the queue is full and count it (`loams_events_dropped_total{reason="queue_full"}`), never blocking the producer's response;
4. do not deduplicate at ingest; the event table (§4) is keyed on `(source, id)` and removes duplicates.

Readers see the same CloudEvents either way: §02 §7.4's consume path rebuilds any record whose `ce_` headers validate. High-rate **ingestion** from outside (telemetry, clickstreams, IoT) is the Event Fabric's job (§32 D331), not this path's.

## 4. Events → Arrow (D372)

No official Arrow mapping for CloudEvents exists (`cloudevents/spec` v1.0.2 defines JSON, protobuf and Avro; its working drafts add Avro compact, CBOR and XML; read 2026-10-01). Loams's:

| Column | Arrow type | From |
|---|---|---|
| `id`, `source`, `type`, `subject`, `dataschema`, `datacontenttype`, `tenantid`, `traceparent`, `tracestate` | `Utf8` (dictionary-encoded for `source`, `type`, `tenantid`) | the attributes |
| `time` | `Timestamp(Nanosecond, "UTC")` | `time` |
| `ext` | `Map<Utf8, Utf8>` | other extensions, string form |
| `ext_types` | `Map<Utf8, Utf8>` | extension name → CloudEvents type for every non-string extension (D270's `loams_ce_types`) |
| `data` | `Struct` derived from the `data` message's descriptor | `data` |
| `data_raw` | `Binary` | the original `data` bytes, for lossless replay (on by default) |

The `data` struct is derived at build time from the protobuf descriptor (scalars to scalars, `repeated` to `List`, messages to `Struct`, `map` to `Map`); each Arrow field carries its proto path and tag in field metadata; Iceberg field ids are assigned by the catalog and matched by path, and only additive evolution passes CI. An event stream is linked (§09) to an Iceberg table (§08, M4) keyed on `(source, id)`, partitioned by `day(time)`, sorted by `(type, time)`; until Iceberg v3 is on the pinned stack, `time` is `timestamptz` (µs) plus a `time_ns` `long` column (Q368).

## 5. Open questions kept here

| # | Question | Owner | Needed by |
|---|---|---|---|
| Q362 | Showback and single-organisation billing in the open repository, or `loam-platform` only. **Answered by the owner on 2026-10-02: no metering in OSS** (D440, D444); open dashboards over the hooks remain possible for anyone to build | Founder | Resolved |
| Q366 | ~~Lambda CPU attribution~~ Moved to `loam-platform` (doc 06 PD66, default: billed duration) on 2026-10-02 (D548); no longer an RN1 Task 5 dependency | Founder | Resolved here |
| Q367 | ~~Cloud Run and Container Apps runners: build or document only~~ Answered 2026-10-02 by the owner: the recommended default — document only; build on demand (§24 §16, RN1) | Founder | Resolved |
| Q368 | ~~Iceberg v3 `timestamptz_ns` by M4, or the `time_ns` column (§4)~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — the `time_ns` column until Iceberg v3 `timestamptz_ns` is on the pinned iceberg-rust and Lakekeeper (§34 §4); why: it works on today's stack and migrates additively | Eng | Resolved |
| Q369 | ~~Move `loams-stream-grpc` from tonic/prost to connect-rust/buffa (D128), and when~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — yes, move to connect-rust/buffa in the M2 stream API plan (D128, D419); why: one RPC stack for the console, the apps (§37) and streams | Eng | Resolved |
| Q372 | ~~Internal HTTP/3: the condition that enables it~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — off; internal traffic stays HTTP/2 until a cross-region link measures loss where head-of-line blocking costs latency; why: no cross-region links exist yet | Eng | Resolved |
| Q450 | ~~Give §1's decisions their own document, and split GW1's vendor-neutral tasks (`buf breaking`, the CloudEvents profile, the event Arrow mapping) into an open plan~~ Answered 2026-10-02 by the owner: the chosen default (the doc gives no recommendation) — §34 itself is that document (it now holds only the retained vendor-neutral decisions), and GW1's vendor-neutral tasks (`buf breaking`, the CloudEvents profile, the event Arrow mapping) become an open plan here, written when GW1 starts; why: D220 keeps standards open, with no new file | Founder | Resolved |

Q360, Q363–Q365, Q370, Q371, Q373 and Q374 moved to `loam-platform` with the gateway and the Cloudflare runner.
