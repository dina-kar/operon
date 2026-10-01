# 34 — The Protocol Gateway, the Standards Charter and the Narrow Waist

Status: **Proposed** · 2026-10-01. Source: the owner's draft "Loam Serverless Runtime — Consolidated Plan" v1 (2026-09-30, §1–§12 and §15; `chatdump.md` lines 631–812 and 937–947). The owner asked on 2026-10-01 to fold that draft into the design docs, the decision log and the plans. §13 (Cloudflare) and §14 (Loam Git) of the draft are covered by another document and are not part of this one. This document turns the rest into decisions **D360–D379** and open questions **Q360–Q374**. They are **proposals** until the owner rules on them. Where the draft repeats something already decided (D128, D170–D190, D200–D202, D220, D260, D261, D270), this document cites the decision and does not restate it. Where the draft contradicts a decision, §15 lists the conflict and the proposed resolution. No code is written by this document; the plans are GW1–GW4 and RN1 (§14).

**Amends** [§24](24-cpu-time-runtime.md) (the `Runner` trait, D375; the draft's corrections, §24 §16) and [§27](27-usage-hooks.md) (usage from runners outside Loam's nodes, D376). **Extends** [§02 §7.4](02-stream-engine.md) (CloudEvents, D270) with an envelope profile (D364) and a high-rate path (D365), and [§08](08-analytics.md) with an event-table mapping (D372, D373).

Markers: **(verify)** means the claim was not checked against a primary source, and the task that depends on it checks it first. **(estimate)** means computed, not measured. **(draft)** means the figure or claim comes from the owner's draft and was not re-checked. Every version, licence and status claim marked with a date was read on 2026-10-01 from the source named in §17.

**Numbering.** D360–D379 and Q360–Q374 are this document's reserved ranges. D273–D280 belong to §29 (PR #172), D270–D272 to the CloudEvents and Arm A work.

---

## 1. Summary

| # | Decision | Status |
|---|---|---|
| D360 | **The standards charter** (§3): every external standard Loam speaks is pinned to a spec version and sits behind one crate, so replacing it touches one crate. "Built for 50 years" means **replaceability**: the durable assets are wire contracts, data formats and semantics. Each standards choice gets a decision-log row; deprecation is announce → dual-run → sunset | Proposed |
| D361 | **Transports**: HTTP/2 with mTLS (ALPN `h2`) between Loam components; HTTP/3 only at the Envoy edge (D176, D184) until an internal-mesh flag lands (build order 8, Q372); exchange traffic arrives as HTTP/1.1 and is terminated and re-originated as h2 by Envoy | Proposed |
| D362 | **Connect-RPC through connect-rust** for every new service: one handler serves Connect, gRPC and gRPC-Web (D128, D206). Checked 2026-10-01: `connectrpc` 0.9.1 (2026-09-21, Apache-2.0) is the official Connect project's Rust implementation (Connect RFC 007) and passes the full Connect conformance suite. The draft's `tonic` + hand-written-codec fallback is not needed | Proposed |
| D363 | **The narrow waist is `loam.stream.v1.StreamService`**: `Produce` and `ProduceCloudEvents` (D270, PR #171) are the one internal ingress for async events. Dapr pub/sub, HTTP push, Cloudflare Queue consumers, Lambda triggers and the protocol gateway are adapters into it. `buf breaking` (rule set `FILE`) is enforced in CI on `proto/loam/{stream,rtb,events,meter}` from GW1; evolution is compatible-only | Proposed |
| D364 | **The Loam CloudEvents profile** (§4.2): CloudEvents 1.0 plus two required extensions, `tenantid` (`<org>/<namespace>`, **stamped by the gateway from the credential**; a client value is overwritten) and `traceparent` (W3C Distributed Tracing extension; `tracestate` optional). **`id` is the idempotency key** (D270's `source` + `id` dedupe), so there is no `idempotencykey` extension. The schema version is the `type` suffix `.v<major>` plus `dataschema` = `urn:loam:proto:<message full name>`, so there is no `schemaversion` extension. Types are `dev.loam.<domain>.<name>.v<major>` (§02's `dev.loam.stream.record` already uses `dev.loam.`; Q361) | Proposed |
| D365 | **High-rate events bypass the dedup ledger.** Bid-path and metering events are written by plain produce in the Kafka binary-mode layout (`ce_` headers, D270's record mapping), in batches, with no claim/complete proposals. They are deduplicated by `(source, id)` in the keyed event table (§08), not at ingest. D270's ledger stays for trigger, webhook and agent rates, as §02 §7.4 already says | Proposed |
| D366 | **The bid hot path is not durable execution.** An auction is decoded, decided and answered in process inside `tmax` (about 100 ms, **draft**). Events leave through a bounded in-memory queue that drops and counts under pressure and never blocks a response. Resonate (on TiKV, D261) orchestrates campaigns, identity merges, syncs and reporting | Proposed |
| D367 | **`ProtocolAdapter`** (§5): `id`, `capabilities`, `detect`, and two directions, inbound (`decode_request`, `encode_response`) and outbound (`encode_request` with a loss report, `decode_response`). Codecs are synchronous. Crates `operon-protocol` (trait, registry), `operon-openrtb` (2.x in JSON, and from GW4 the IAB and Google protobuf adapters), `operon-openrtb-proto` (the vendored IAB and Google protos); the gateway service, negotiation and outbound client in `operon-rtb-gateway`, mounted in `loam-gateway` (D184) and, for development, in `operon` behind the feature `rtb`, loopback only until D111 | Proposed |
| D368 | **The canonical model `loam.rtb.v1`** (§6): protocol-neutral protobuf modelled on the OpenRTB 2.6-202606 object model, with every field that older versions carried in `ext` promoted to a first-class field, removed attributes kept in per-object `Legacy` messages, money as `Money{int64 micros, string currency}`, times as `int64` ns UTC, AdCOM 1.0 lists as `int32` (unknown values preserved), per-object `ext` as raw JSON bytes, and a `Provenance` (protocol, version, partner) on every message | Proposed |
| D369 | **Version negotiation is per partner, never per request** (§7): start at the partner record's highest version; on a structural rejection (HTTP 400/422, `nbr` 2 "Invalid Request", or three consecutive undecodable responses), downgrade, cache the result, guard it with a circuit breaker and probe upward on a timer; a response `x-openrtb-version` lower than the request's downgrades the next request directly; timeouts and 5xx never downgrade. Decoders ignore unknown fields and record them as warnings | Proposed |
| D370 | **OpenRTB adapters**: 2.6 (pinned to the IAB release `2.6-202606`), 2.5 and 2.4, as one superset serde model plus per-version rule tables generated from the spec's Appendix B; 2.3 on demand (Q370); **OpenRTB 3.0/AdCOM-as-transport is out of scope** (AdCOM 1.0's enumerated lists are used, since 2.6 points to them) | Proposed |
| D371 | **Google means OpenRTB.** Google sunset the Authorized Buyers real-time bidding protocol on 2025-04-30; OpenRTB is the only protocol for Authorized Buyers, Open Bidding and SDK Bidding. The Google adapter is OpenRTB 2.6 in JSON and in protobuf (`openrtb.proto`, package `com.google.openrtb`, proto2, Apache-2.0) with Google's `openrtb-adx.proto` extensions. DV360 is not an RTB surface and is out of scope. Answers the draft's §15 question | Proposed |
| D372 | **Events → Arrow** (§9): one Arrow schema per event type, derived at build time from the protobuf descriptor of its data message; context attributes become columns, `data` a typed struct, `data_raw` the original bytes for lossless replay; `Timestamp(Nanosecond, "UTC")`; Iceberg field ids assigned by the catalog and matched by proto field path; additive-only evolution checked in CI | Proposed |
| D373 | **Match/merge on §08's tables**, not `MERGE INTO`: identity resolution is a DataFusion sort-merge join on identity keys; profiles are `PRIMARY KEY … VERSION BY` tables (§08 §1) fed by stream → table links (§09). Iceberg arrives in M4 | Proposed |
| D374 | **State rules** (§10): every stored key carries the tenant scope in Loam's existing forms (`ns/<ns>/` in the bucket, keyspace plus prefix in TiKV), not a literal `tenant/{id}/`; money is integer (micros of the currency unit); new protobuf contracts use `int64` ns UTC; TiKV is reached only through transactions or atomic CAS, never a raw get-then-put | Proposed |
| D375 | **One `Runner` trait** (§11): `deploy`, `invoke`, `undeploy`, `health`, `capabilities`. `SupervisorRunner` (Loam's nodes, §24's tiers) is the default and the only one with D170's placement advantage; `LambdaRunner` (Rust, arm64, `provided.al2023`) and a thin `WorkersRunner` are options for burst, edge and BYOC; Cloud Run and Container Apps runners wait for demand (Q367). Dapr stays one shared `daprd` per cluster (D183), never a sidecar per function. **Amends §24 §3** | Proposed |
| D376 | **Usage from every runner reaches §27's contract** (§12): exactly one reporter per invocation; the supervisor reports for its own tiers (D201), the runner host reports for external runners from the runner's `Usage` (Lambda: in-process `getrusage` delta, capped by the billed duration; Workers: Tail Worker `CPUTimeMs`). `loam.meter.v1.Invocation` gains fields 12–16 (`runner`, `region`, `provider_billed_ms`, `compile_usec`, `overhead_usec`), and a host report is also a CloudEvent `dev.loam.meter.usage.v1`. Rating, invoices and reconciliation against provider invoices stay in `loam-platform` (D190, D202). **Amends §27** | Proposed |
| D377 | **The conformance suite `operon-conformance`** (renamed `loam-conformance` with the Loam rename; §13): golden corpora per protocol and version, round-trip properties, nightly fuzzing, the cross-protocol differential, negotiation and transport tests, CloudEvents and Arrow checks, latency gates. Fixtures: IAB examples under CC BY 3.0 with attribution, Google protos under Apache-2.0, nothing from real traffic in this repository | Proposed |
| D378 | **Languages** (§14): Rust for the data plane; Java (Quarkus) and Go for control-plane and customer extension points through `buf`-generated Connect clients; in-process embedding only after profiling shows the RPC hop is the bottleneck (FFM, cgo, the Arrow C Data Interface, or Wasm under Chicory or wazero). **No core path depends on Go or Java SIMD**: Go 1.27's `simd` and `simd/archsimd` still need `GOEXPERIMENT=simd`, and the Java Vector API is in its eleventh incubator in JDK 26 | Proposed |
| D379 | **The licensing split follows D220** (§16), and the draft's "open decision" is answered by it: the proto contract, CloudEvents profile, Arrow schemas, adapters, negotiation, conformance harness with the public corpus, the metering record spec and the runner usage collectors are Apache-2.0 here; the ledger, rating, invoicing, provider reconciliation, private partner-quirk corpora, hosted conformance and the "Loam Certified" programme are `loam-platform`. **Track GW** (GW1–GW4, then GW5–GW7) and **RN1** carry the build order (§14) | Proposed |

## 2. Goals and non-goals

### 2.1 Goals

1. **One canonical model, many dialects.** Any supported version of OpenRTB, and Google's OpenRTB, decode to the same canonical message for the same logical auction, and encode back without losing anything but documented `ext` moves.
2. **Adding a protocol is one adapter plus fixtures.** No change to the gateway, the bidder interface, the event schemas or the tables.
3. **Every dependency is replaceable.** The charter (§3) names the standard, the version and the one crate behind which it sits.
4. **Waiting is never billed.** The draft's premise, already §24's goal (D170, D173, D180): Loam meters CPU and charges its tenants for it, on every runner (§11, §12).
5. **Conformance is public.** A support claim ("speaks OpenRTB 2.5") is a passing, published test suite (D13's rule).

### 2.2 Non-goals

- **A full ad server or DSP.** Loam hosts the decision code (a tenant function or an in-process bidder) and the data around it; it does not ship campaign logic, pacing or creative review.
- **Durable execution on the bid path** (D366).
- **OpenRTB 3.0 as a transport** (D370), **DV360 APIs** (D371), and **Google's retired proprietary RTB protocol** (D371).
- **Billing in this repository** (D190, D202, D379).

## 3. The standards charter (D360)

| Concern | Standard and version | Loam rule | Crate behind which it sits | Decision |
|---|---|---|---|---|
| Internal transport | HTTP/2 (RFC 9113), TLS 1.3, mTLS with SPIFFE IDs | ALPN `h2`; h2c only on loopback | `hyper` via axum and connect-rust | D361 |
| Edge | HTTP/1.1, HTTP/2, HTTP/3 (RFC 9114) at Envoy | Exchanges send HTTP/1.1; Envoy terminates and re-originates h2 | Envoy config in the chart | D176, D184, D361 |
| RPC | Connect protocol, gRPC, gRPC-Web, protobuf | One connect-rust handler per service | `connectrpc` + `buffa` | D128, D362 |
| Async events | CloudEvents 1.0.2: protobuf format inside, JSON at the edges, batch formats on the wire | The Loam profile (§4.2); `source` + `id` dedupe | `operon-cloudevents` | D270, D364, D365 |
| Columnar | Arrow (IPC in memory and on the wire), Parquet (at rest), Iceberg (tables) | Arrow Flight for bulk transfer (D49); DataFusion for joins (D15) | `arrow` 58, `parquet` 58, `operon-events-arrow` | D6, D372 |
| Row serialization | Protobuf; Avro only at schema-registry sinks (the M5 Kafka gateway) | Arrow is columnar and is not used as a row format | `buffa`; Avro crate chosen in M5 | D372; answers the draft's §15 question |
| Telemetry | OpenTelemetry (OTLP), W3C Trace Context | `traceparent` in every event (D364) | `opentelemetry` crates | D73, D364 |
| Durable orchestration | Resonate protocol `2026-04-01` | Promise id = idempotency key; durable state on TiKV | `operon-durable` (`resonate-server-tikv`) | D19, D261 |
| Ad-tech | OpenRTB 2.6-202606, 2.5, 2.4; AdCOM 1.0 lists; Google OpenRTB protobuf and `openrtb-adx` | Per-partner negotiation (§7) | `operon-openrtb`, `operon-openrtb-proto` | D369–D371 |
| Usage | `loam.meter.v1` (§27), Prometheus, cgroup v2 | One reporter per invocation | `operon-meter` (RN1) | D201, D376 |

**Longevity practices** (the draft's §11, adopted as part of D360):

- **One crate per standard.** Replacing a standard, or adding a version of it, touches that crate and its fixtures.
- **A decision-log row per standards choice**, with the pinned version. Deprecation: announce in the changelog and docs; dual-run both versions for at least one minor Loam release; then sunset.
- **Open formats only in stored data**: Iceberg, Parquet, Lance, protobuf, OTel. No cloud-specific identifiers in stored data (an AWS request id is a log attribute, never a key).
- **Crypto agility**: every signature and key carries a key id and an algorithm id (Biscuit's root key ids, D188, already do); hybrid post-quantum key exchange is planned with the TLS stack's support **(verify: rustls' X25519MLKEM768 status at GW2)**.
- **Supply chain**: `cargo deny` (in CI today), `cargo vet` for new third-party crates on the data plane, SBOMs (CycloneDX) per release, reproducible release builds, and the MSRV policy in `rust-toolchain.toml`.
- **Backups and drills**: object-storage backups with scheduled restore drills (§10); a dependency review every 3–5 years, recorded in this charter.

## 4. The narrow waist (D363, D364, D365)

### 4.1 Owned contracts

The draft says the streams gRPC protocol is the canonical internal contract. On `main` that is `loam.stream.v1.StreamService` in `crates/operon-stream-grpc` (tonic, `Produce` only). PR #171 adds `ProduceCloudEvents`, which takes the CloudEvents protobuf batch format (`io.cloudevents.v1.CloudEventBatch`, vendored unchanged from CloudEvents 1.0.2) or a JSON batch kept as sent. D363 makes that service the one ingress:

| Producer | Adapter into the waist |
|---|---|
| Dapr pub/sub and bindings | `deploy/dapr/edge`, passing CloudEvents through (D270, §02 §7.4) |
| HTTP push and webhooks | `POST …/streams/{stream}/events` (PR #170) |
| The protocol gateway | high-rate path (D365), plain produce with `ce_` headers |
| Lambda, Cloudflare Queue consumers | the runner host (§11) calls `ProduceCloudEvents` with the runner's credential |
| Runtime hooks | `dev.loam.meter.usage.v1` events (D376), high-rate path |

`operon-stream-grpc` is tonic and `prost`, while D128 chose connect-rust and buffa for every protobuf service. Moving it is not part of this document (Q369); the proto file is the contract and does not change when the server library does.

**Breaking checks.** `buf.yaml` already declares `breaking: use: [FILE]`, but R1 did not enforce it because the Live API was unstable. GW1 enforces it for `loam.stream.v1`, `loam.rtb.v1`, `loam.events.v1` and `loam.meter.v1`, comparing against `main`; `loam.live.v1` joins when R2 freezes it.

### 4.2 The Loam CloudEvents profile (D364)

| Attribute | Required | Rule |
|---|---|---|
| `specversion`, `id`, `source`, `type` | yes | CloudEvents 1.0 (D270 validates them) |
| `id` | yes | **The idempotency key.** Producers keep it stable across their retries; D270 dedupes on SHA-256(`source` ‖ 0x00 ‖ `id`) |
| `type` | yes | `dev.loam.<domain>.<name>.v<major>`, for example `dev.loam.rtb.auction.v1`. A breaking change of `data` is a new major (`.v2`), a new type |
| `dataschema` | yes for Loam-defined types | `urn:loam:proto:<full message name>`, for example `urn:loam:proto:loam.events.v1.AuctionEvent`. The schema's minor evolution is additive (D372), so the URN carries no minor version |
| `time` | yes | RFC 3339 with nanoseconds; the producer's clock |
| `tenantid` | yes | `<org>/<namespace>`, the same value as `x-loam-tenant` (§27 §3.4). **Set by the gateway or the runner host from the credential**; a client-supplied value is replaced and its replacement counted (`loam_events_tenantid_overwritten_total`) |
| `traceparent` | yes (may be generated) | W3C Trace Context `traceparent`; `tracestate` optional. A missing one is generated at the first Loam hop |
| `partitionkey` | optional | D270: becomes the record key |
| `datacontenttype` | optional | `application/protobuf` for Loam-defined types on the high-rate path, `application/json` at the edge |

The draft's `idempotencykey` and `schemaversion` extensions are not adopted (§15 rows 8 and 9): two idempotency keys on one event would disagree, and CloudEvents already has `dataschema`. An application that needs a business key distinct from `id` (a bid id, an order number) keeps it in `data`.

### 4.3 The high-rate path (D365)

D270's ledger costs two metastore proposals per request and about 80 bytes per event for the window; §02 §7.4 already says bulk ingest should use plain produce. An exchange at 50 000 requests per second would emit 50 000 auction events per second **(estimate)**, far above webhook rates. So the gateway's `EventSink`:

1. buffers events per `(namespace, stream)` in a bounded queue (default 65 536 events or 64 MiB, whichever first);
2. flushes every 50 ms or at 1 MiB as one `Produce` of records laid out by D270's mapping (`ce_` headers, `content-type`, key from `partitionkey`, value the protobuf `data`);
3. drops the oldest batch when the queue is full and counts it (`loam_rtb_events_dropped_total{reason="queue_full"}`), never blocking the auction;
4. does not deduplicate: a gateway retry after an unknown produce outcome can duplicate; the event table (§9) is keyed on `(source, id)` and removes them.

Readers see the same CloudEvents either way: §02 §7.4's consume path rebuilds any record whose `ce_` headers validate.

## 5. The protocol gateway (D366, D367)

### 5.1 Where it runs

```
exchange / SSP / SDK ──HTTP/1.1──► Envoy (TLS, h1→h2, rate limits)
                                        │ h2, x-loam-tenant stamped by the route
                              ┌─────────▼──────────────────────────────┐
                              │ operon-rtb-gateway (in loam-gateway)    │
                              │  detect → ProtocolAdapter.decode_request│
                              │  canonical loam.rtb.v1.Auction          │
                              │  Bidder (in-process, deadline = tmax−ε) │
                              │  ProtocolAdapter.encode_response        │
                              │  EventSink (bounded, async) ────────────┼──► StreamService.Produce (D365)
                              └─────────┬──────────────────────────────┘
                                        │ outbound fan-out (when Loam is the exchange side)
                              PartnerClient + Negotiator (§7) ──► DSP endpoints
```

- **Inbound** (Loam hosts a bidder): a request on `/rtb/v1/{partner}/bid` is decoded with the adapter the partner record names (or `detect` picks), decided by the `Bidder`, encoded in the request's own version, and answered with `x-openrtb-version`. 204 is the no-bid answer.
- **Outbound** (Loam fans an auction out to partners): `PartnerClient` encodes the canonical auction in each partner's negotiated version and decodes the answers. Whether Loam is the bidder side, the exchange side or both decides which half GW2 and GW3 build first (Q363). The adapter trait has both halves either way.
- **Canonical service**: `loam.rtb.v1.AuctionService/Bid` over connect-rust, for internal callers and SDKs that already hold a canonical auction.
- **The bidder** is a trait (§5.3). GW2 ships a rule-based bidder for tests and a forwarding bidder that calls a function over loopback; whether the production hot path runs tenant code as a T1 wasmtime component in process (§24 D172) is Q371.
- **Listeners**: in development, `operon` behind the feature `rtb` with `--rtb-listen` (loopback only until D111, the same rule as every other listener); in clusters, the same router mounted in `loam-gateway` behind Envoy.

### 5.2 The adapter trait (D367)

```rust
// crates/operon-protocol/src/adapter.rs
pub trait ProtocolAdapter: Send + Sync + 'static {
    fn id(&self) -> ProtocolId;                          // OpenRtb(V2_6), OpenRtb(V2_5), GoogleOpenRtb, …
    fn capabilities(&self) -> &Capabilities;             // versions, encodings, directions, limits
    fn detect(&self, head: &WireHead) -> Detection;      // header, content type, path, partner hint
    // Inbound: an exchange calls Loam.
    fn decode_request(&self, msg: &WireMessage, cx: &DecodeCx) -> Result<Decoded<Auction>, DecodeError>;
    fn encode_response(&self, resp: &AuctionResponse, cx: &EncodeCx) -> Result<WireMessage, EncodeError>;
    // Outbound: Loam calls a partner.
    fn encode_request(&self, auction: &Auction, cx: &EncodeCx) -> Result<Encoded, EncodeError>;
    fn decode_response(&self, msg: &WireMessage, cx: &DecodeCx) -> Result<Decoded<AuctionResponse>, DecodeError>;
}
pub struct Decoded<T> { pub value: T, pub warnings: Vec<Warning> }   // unknown fields, coerced values
pub struct Encoded { pub msg: WireMessage, pub loss: LossReport }    // fields moved to ext, dropped
pub enum Detection { Match { version: ProtocolVersion, confidence: Confidence }, NoMatch }
```

Codecs are synchronous and allocation-bounded (a request body over `Capabilities::max_body` is refused before parsing). The registry (`AdapterRegistry`) holds one adapter per `ProtocolId`; `detect` runs only when a partner record does not name the protocol, and the first `Match` with `Confidence::Certain` (the `x-openrtb-version` header, a protobuf content type on a Google route) wins over `Likely` (field sniffing).

### 5.3 The bidder

```rust
#[async_trait]
pub trait Bidder: Send + Sync + 'static {
    async fn bid(&self, cx: &BidCx, auction: &Auction) -> BidDecision;  // never errors: NoBid carries a reason
}
pub struct BidCx { pub tenant: Tenant, pub partner: PartnerId, pub deadline: Instant, pub trace: TraceContext }
pub enum BidDecision { Bids(AuctionResponse), NoBid(NoBidReason) }
```

The deadline is `tmax − decode − encode − network margin` (margin default 10 ms). A bidder that misses it gets a no-bid answered for it (`NoBidReason::Timeout`, counted), because a late bid is worthless.

## 6. The canonical model (D368)

`proto/loam/rtb/v1/` (package `loam.rtb.v1`), compiled by `connectrpc-build` into `operon-rtb-proto`. It follows the OpenRTB 2.6-202606 object model because that is the industry's shared vocabulary and Google's OpenRTB is a superset of it. Message and field names follow the IAB's own protobuf, `openrtb.proto` in `openrtb2.x/proto` (package `com.iabtechlab.openrtb.v2`, edition 2023, Apache-2.0, "expected to be an exact representation of the OpenRTB standard", kept in lockstep with the spec). That file is not the canonical model itself: it carries prices as `double`, `ext` as proto extensions, OpenRTB's integer flags as `bool`, and none of the removed fields. Field numbers are Loam's own. The canonical model is **protocol-neutral** in four ways:

1. **Normalized locations.** Fields that 2.5 and 2.4 carried in `ext` by convention and 2.6 promoted are first-class (`regs.gdpr`, `user.consent`, `user.eids`, `source.schain`, `regs.us_privacy`, `regs.gpp`, `regs.gpp_sid`). A 2.5 request with `regs.ext.gdpr = 1` and a 2.6 request with `regs.gdpr = 1` give the same canonical message.
2. **Integer money.** `message Money { int64 micros = 1; string currency = 2; }`: micros of the currency's major unit (a CPM of 1.5 USD is `1_500_000`). Minor units would round sub-cent CPMs; Google's own protocols use micros. JSON prices are parsed from their decimal text, never through `f64` (§8.2).
3. **Nanosecond time.** `int64 *_unix_ns` everywhere (D374).
4. **Lossless legacy.** Attributes 2.6 removed (`banner.wmax/hmax/wmin/hmin`, `video.protocol`, `content.videoquality`) and deprecated (`video.placement`, deprecated in 2.6-202303 for `plcmt`; `video.sequence`, `device.didsha1/didmd5/dpidsha1/dpidmd5/macsha1/macmd5`, `user.yob/gender`, `bid.api`) live in a per-object `Legacy` message, so a 2.4 request round-trips.

```protobuf
syntax = "proto3";
package loam.rtb.v1;

message Auction {                      // OpenRTB BidRequest
  string id = 1;
  repeated Impression imp = 2;
  oneof distribution { Site site = 3; App app = 4; Dooh dooh = 5; }
  Device device = 6;
  User user = 7;
  int32 test = 8;
  AuctionType at = 9;                  // first price, second price plus, deal-defined
  int64 tmax_ms = 10;
  repeated string wseat = 11; repeated string bseat = 12;
  bool allimps = 13;
  repeated string cur = 14;
  repeated string wlang = 15; repeated string wlangb = 16;
  int32 cattax = 17; repeated string bcat = 18; repeated string badv = 19; repeated string bapp = 20;
  Source source = 21;
  Regs regs = 22;
  bytes ext = 50;                      // the original ext object, raw JSON text
  Provenance provenance = 60;
}
message Provenance { string protocol = 1; string version = 2; string partner = 3; int64 received_unix_ns = 4; }
message Money { int64 micros = 1; string currency = 2; }
// Impression, Banner, Video (with Pod), Audio, Native, Format, Pmp, Deal, Metric, Site, App, Dooh, Publisher,
// Content, Producer, Network, Channel, Data, Segment, Device, UserAgent, BrandVersion, Geo, User, Eid, Uid,
// Source, SupplyChain, SupplyChainNode, Regs, Qty; AuctionResponse (BidResponse), SeatBid, Bid, NoBidReason:
// field-by-field in GW1 Task 2, each with `bytes ext = 50`, `Legacy legacy = 51` where 2.6 removed fields.
```

**Enumerations.** AdCOM 1.0 lists (2.6 points to them; 2.5 and 2.4 define the same values in their own §5) are `int32` fields, not proto `enum`s, so a value added in a monthly 2.6 release passes through without a schema change. Named constants live in `operon-rtb-proto::adcom`.

**Extensions.** `ext` is the original JSON text of the object's `ext`, kept as bytes. Google's AdX extensions are typed: field 52, `google_ext`, on each object Google extends, has the type of Google's own extension message from the vendored `openrtb-adx.proto` (`BidRequestExt`, `ImpExt`, `BidExt`, `BidResponseExt`, …), so Loam never re-models them. Field 53, `ext_proto`, keeps the unknown-field bytes of an object decoded from protobuf, so a protobuf round trip is lossless.

## 7. Version negotiation (D369, D370)

### 7.1 Inbound

The version is the request's `x-openrtb-version` header (`<major>.<minor>`, OpenRTB 2.6 §2.5). Without it, the partner record's configured version is used; without that, `detect` sniffs (a `dooh` or `imp.rwdd` field means at least 2.6, `source` at least 2.5). The response is encoded in the request's version and carries `x-openrtb-version` with that version. A decode failure answers 400 with a short reason and counts it; it never retries at another version.

### 7.2 Outbound

The partner capability record (`PartnerRecord`: id, protocol, versions offered in preference order, encoding, compression, `tmax`, QPS ceiling) comes from configuration in GW2 and from a `CapabilityStore` later (the `ControlStore`, R2; Q374). For each partner the `Negotiator` keeps:

| State | Meaning | Transition |
|---|---|---|
| `Current(v)` | send at `v` | start at the record's highest version |
| downgrade | `v → next lower offered` | on a **structural rejection**: HTTP 400 or 422; or a 200 whose body decodes to `nbr = 2` ("Invalid Request"); or three consecutive undecodable responses at `v` (a single malformed response is a no-bid, OpenRTB 2.6 §4). Also on a response `x-openrtb-version` lower than `v` (the partner says what it implements): move to that version directly |
| breaker | per `(partner, version)`: closed, open, half-open | opens after 5 structural rejections in 30 s at a version (defaults); an open version is skipped; half-open after the probe interval |
| probe upward | one request in 1 000, at most one a second, at the next higher version once its breaker is half-open, every `probe_interval` (default 1 h) | a valid answer (a bid or 204) at the higher version closes its breaker and promotes it |

Timeouts, connection errors and 5xx never downgrade (they are not version problems). There are no in-request retries at a lower version: the latency budget has no room, so the request that triggered a downgrade is lost and the next one uses the new version. State is per gateway node in memory, persisted to the `CapabilityStore` on change so a new node starts from the learned version; nodes do not gossip.

### 7.3 Downgrade rules

One superset serde model covers 2.4–2.6 (§8.1). A per-version rule table, generated from the specs' Appendix B change logs and checked by GW3 Task 0 against the three spec texts, says what each field does when encoded at a lower version:

| Rule | Example (2.6 → 2.5) | Example (2.5 → 2.4) |
|---|---|---|
| `MoveToExt` (lossless) | `regs.gdpr` → `regs.ext.gdpr`; `user.consent` → `user.ext.consent`; `user.eids` → `user.ext.eids`; `source.schain` → `source.ext.schain`; `regs.gpp`, `regs.gpp_sid` → `regs.ext.*` | `request.source` → `request.ext.source`; `bseat`, `wlang` → `ext` |
| `Drop` (documented) | `imp.rwdd`, `imp.ssai`, `imp.qty`, `device.sua` (to `ext` where the partner record opts in), `content.network`, `content.channel`, `cattax` | `imp.metric`, `banner.vcm`, `video.placement`, `video.playbackend`, `format.wratio/hratio/wmin` |
| `Map` | `video.plcmt` → `video.placement` where the IAB guidance gives a one-to-one value **(verify the mapping table)** | — |
| `NotRepresentable` | a `dooh`-only auction; an ad-pod auction whose meaning needs `podid`/`slotinpod` (pods go to `video.ext` only if the partner record opts in) | an impression whose formats use only ratios |

`NotRepresentable` means the auction is not sent to that partner at that version: the partner is skipped for this auction, counted (`loam_rtb_partner_skipped_total{reason="not_representable"}`), and not downgraded further. Decoding at any version promotes `ext` locations back to their canonical fields, so **downgrade then upgrade never invents data**: the result is the original with `Drop`ped fields absent (GW3's property test).

## 8. The adapters (D370, D371)

### 8.1 OpenRTB 2.x

`operon-openrtb` holds one serde model, the superset of 2.4, 2.5 and 2.6-202606 (fields removed in 2.6 included), with `ext` as `Box<RawValue>` and unknown fields ignored and listed in `Decoded::warnings`. A version is a `Profile` over that model: which fields are valid, where conventions put promoted fields, and the downgrade rules. The adapters are `OpenRtb26`, `OpenRtb25` and `OpenRtb24` over one implementation; 2.3 is another profile if a partner needs it (Q370).

**Buy before build.** `iab-specs` 0.5.1 (Apache-2.0, 2026-05-11) has serde models for OpenRTB 2.5, 2.6 and 3.0 and AdCOM 1.0, but it represents prices as `f64`, has no 2.4 and no removed 2.6 fields, and holds `ext` as `Vec<u8>`. GW2 Task 0 confirms; the proposal is to use it as a **test oracle** (decode every corpus file with both and compare) and for AdCOM constant names, not as the model. `openrtb2` 0.3.0 (MIT OR Apache-2.0) has had no release since 2022-12.

### 8.2 Money in JSON

`serde_json` in the workspace has `float_roundtrip` and `preserve_order` but not `arbitrary_precision`, and enabling that would change every crate's `serde_json::Number` through feature unification. Price fields therefore deserialize through `&RawValue` (the `raw_value` feature, which changes nothing else) and a decimal parser into micros, rounding half to even beyond six decimals and counting the rounding as a warning; encoding writes the shortest decimal text for the micros. Round-trip equality compares JSON values with numbers compared by decimal value.

### 8.3 OpenRTB in protobuf, and Google (D371)

The IAB names protobuf "the standard binary encoding for OpenRTB" (`openrtb2.x/proto/README.md`). Its `openrtb.proto` reserves extension ranges per organisation on every extensible object (1–499 IAB, 500–999 prototype, 1000–1999 Google, 2000–2999 Amazon, …). GW4 adds an `OpenRtb26Proto` adapter over that file beside the Google adapter, since both are the same object model in protobuf.


Google's Authorized Buyers proprietary RTB protocol was sunset on 2025-04-30; OpenRTB is the only protocol for Authorized Buyers, Open Bidding and SDK Bidding (Google Ads Developer Blog, 2024-03 and 2025-01-24). Google publishes `openrtb.proto` (OpenRTB 2.6 as proto2, package `com.google.openrtb`, Apache-2.0) and `openrtb-adx.proto` (AdX extensions as proto2 `extend` fields; version v.205 on 2026-03-13, beta v.213 **(verify at GW4)**). The Google adapter (`operon-openrtb`, module `proto::google`, GW4):

- decodes and encodes JSON (through `operon-openrtb`'s 2.6 profile plus typed AdX `ext` members) and protobuf (buffa over the vendored protos);
- keeps proto unknown fields, which buffa preserves by default, so a field Google adds round-trips;
- carries AdX extensions in `google_ext` with Google's own message types (§6);
- answers in the encoding of the request.

buffa states proto2 and extension support and unknown-field preservation; whether its generated code exposes proto2 `extend` fields, or only `buffa-descriptor`'s `DynamicMessage` does, is GW4 Task 0's check, with `DynamicMessage` for the AdX extensions as the fallback.

## 9. Events, Arrow and the event tables (D372)

**Event types** come with the adapters: `dev.loam.rtb.auction.v1` (the canonical auction, the decision, the outcome and latencies), `dev.loam.rtb.win.v1`, `dev.loam.rtb.loss.v1`, `dev.loam.rtb.billing.v1` (from `nurl`, `lurl`, `burl` notifications), and `dev.loam.meter.usage.v1` (D376). Their `data` messages live in `proto/loam/rtb/v1/events.proto` (and `loam.meter.v1.Invocation` for usage), each marked with the custom message option `(loam.events.v1.event)` from `proto/loam/events/v1/options.proto`, which names its CloudEvents type; the Arrow derivation and the profile check read that option.

**No official Arrow mapping for CloudEvents exists** (the draft's claim, checked on 2026-10-01: `cloudevents/spec` v1.0.2 defines the JSON, protobuf and Avro formats; its working drafts add Avro compact, CBOR and XML; none is Arrow). Loam defines one:

| Column | Arrow type | From |
|---|---|---|
| `id`, `source`, `type`, `subject`, `dataschema`, `datacontenttype`, `tenantid`, `traceparent`, `tracestate` | `Utf8` (dictionary-encoded for `source`, `type`, `tenantid`) | the attributes |
| `time` | `Timestamp(Nanosecond, "UTC")` | `time` |
| `ext` | `Map<Utf8, Utf8>` | other extensions, string form (types via D270's `loam_ce_types`) |
| `data` | `Struct` derived from the `data` message's descriptor | `data` |
| `data_raw` | `Binary` | the original `data` bytes, for lossless replay (on by default; a table may turn it off) |

The `data` struct is derived at build time from the protobuf descriptor (proto scalar → Arrow scalar, `repeated` → `List`, message → `Struct`, `map` → `Map`, `Money` → `Struct{micros: Int64, currency: Dictionary<Utf8>}`, `bytes` raw JSON `ext` → `Utf8`). Each Arrow field carries its proto field path and tag in field metadata. Iceberg field ids are assigned by the catalog (they must be unique across nested fields, which proto tags are not) and matched to the proto by path, so a field added in proto is an added Iceberg column, and `buf breaking` and the additive-only check (`operon-events-arrow::evolution::check_additive`) reject everything else.

**Storage.** An event stream is linked (§09) to an Iceberg table (§08, M4) keyed on `(source, id)` (D365's dedupe), partitioned by `day(time)` and sorted by `(type, time)`. Nanosecond timestamps need an Iceberg v3 table (§08 §2); until v3 is available on the pinned stack, `time` is stored as `timestamptz` (µs) with an extra `time_ns` `long` column (Q368). Arrow IPC is the batch format between the gateway and the sink and for Flight `DoGet`.

## 10. Match/merge and state (D373, D374)

**Match/merge.** The draft's "DataFusion sort-merge joins on identity keys; Iceberg `MERGE INTO` for profile upserts" maps onto what §08 already designs: a profile is a `PRIMARY KEY (profile_id) VERSION BY (updated_unix_ns)` table; identity matches (`user.eids`, `user.buyeruid`, device ids) are a DataFusion `SortMergeJoinExec` over the identity edges; the merged rows are written by a link or a durable job (§26) as upserts, which §08 §5 already turns into deletion vectors plus appends. No `MERGE INTO` statement is needed for this; it could come later as SQL sugar over the same writes. Identity merges that span many profiles run as Resonate workflows (D366) with the merge id as promise id.

**State rules (D374).**

| Rule | Loam's form |
|---|---|
| Tenant prefix on every key | Bucket keys under `ns/<ns>/` (the WAL is cluster-level by D25 and carries the namespace per chunk); TiKV keyspace per system plus a tenant prefix (§20, §26 §6.8); never a literal `tenant/{id}/` alongside them |
| Money | `int64` micros plus ISO 4217 currency in every new contract and table; invoices may render minor units, computed from micros |
| Time | `int64` ns UTC in new protobuf contracts and Arrow; existing contracts keep their units (`loam.meter.v1` uses ms and µs; §27 is versioned and is not changed for this) |
| TiKV | through `operon-tikv`'s transaction runner or atomic CAS only; a raw get-then-put is a race (the draft's correction 3) |

## 11. Runners (D375; amends §24 §3)

§24 places every function on Loam's own nodes, under the node supervisor (T0 workerd, T1 wasmtime, T2 gVisor). The draft adds clouds Loam does not own. A `Runner` is where a function version runs; the supervisor's tiers are inside one runner:

```rust
// crates/operon-runner/src/lib.rs (RN1)
#[async_trait]
pub trait Runner: Send + Sync + fmt::Debug + 'static {
    fn kind(&self) -> RunnerKind;                                   // Supervisor, Process, Lambda, Workers, …
    fn capabilities(&self) -> &RunnerCapabilities;                  // contracts (fetch, http-port), limits, suspend support
    async fn deploy(&self, cx: &TenantCx, artifact: &Artifact) -> Result<Deployment, RunnerError>; // idempotent by digest
    async fn invoke(&self, cx: &InvocationCx, dep: &DeploymentRef, req: InvokeRequest) -> Result<InvokeResponse, RunnerError>;
    async fn undeploy(&self, cx: &TenantCx, dep: &DeploymentRef) -> Result<(), RunnerError>;
    async fn health(&self) -> RunnerHealth;
}
pub struct InvokeResponse { pub response: http::Response<Bytes>, pub usage: Option<Usage> } // None: the runner reports itself
```

| Runner | Contract | Isolation | CPU meter | Status |
|---|---|---|---|---|
| `SupervisorRunner` (Loam's arm64 or x86 nodes) | `fetch`, `http-port`, `static` (§24 D181) | §24's tiers | the supervisor's hooks (D175, D201); `usage: None` | F1 builds the supervisor; RN1 the adapter |
| `ProcessRunner` (dev and tests) | `fetch` over a child process | cgroup v2 when delegated, else none | child `cpu.stat` or `wait4` rusage | RN1 |
| `LambdaRunner` (Rust, arm64, `provided.al2023`) | `fetch` through Loam's Lambda bootstrap | Firecracker (AWS) | in-process `getrusage(RUSAGE_SELF)` delta per invocation, capped by the billed duration × vCPUs (Q366); billed duration from the platform report | RN1 |
| `WorkersRunner`, thin (`workers-rs`) | validate, then call Loam's ingress | V8 isolates (Cloudflare) | Tail Worker `CPUTimeMs` (Cloudflare changelog 2025-04-09) | design here; the Cloudflare document owns its spike |
| Cloud Run, Container Apps | `http-port` | gVisor or VM (provider) | in-container cgroup `cpu.stat` **(verify)** | not planned (Q367) |

**Placement.** D170's advantage is placement next to the data; a function on Lambda or Workers loses it and pays the round trips and egress. External runners are for burst capacity, edge validation and BYOC accounts that want their own cloud bill, not the default. **Workers** never talk to TiKV (the draft's correction 5 still holds; `tikv-client` does not build for `wasm32-unknown-unknown`); they call Loam's ingress, which is loopback-only until the unified auth plan (D111), so `WorkersRunner` waits for D111 (Q373). **Dapr** runs as one shared `daprd` per cluster (D183); on external runners there is no Dapr at all, and their calls into Loam go through the runner host with the runner's credential.

## 12. Metering (D376; amends §27)

The draft's corrections stand and agree with §24: Fargate, Cloud Run and Container Apps bill allocated resources over wall time, Knative adds no CPU billing, and only Cloudflare Workers bills CPU time (checked 2026-10-01: Workers Standard $5 a month, 10 M requests and 30 M CPU-ms included, then $0.30 per million requests and $0.02 per million CPU-ms, "no charge or limit for duration", up to 5 minutes of CPU per invocation). **CPU-time billing is something Loam meters and charges its tenants.** What this document changes:

1. **One reporter per invocation.** The supervisor reports its tiers on `/run/loam/meter.sock` (§27 §3.3). For an external runner the **runner host** (the gateway process that called `Runner::invoke`) turns `InvokeResponse::usage` into a `HostReport` on the same socket. Nothing is reported twice, and the consumer contract is unchanged.
2. **`loam.meter.v1.Invocation` gains fields** (additive, §27 §3.6): `string runner = 12`, `string region = 13`, `uint64 provider_billed_ms = 14`, `uint64 compile_usec = 15`, `uint64 overhead_usec = 16`.
3. **Overhead is separated, not hidden.** CPU of the supervisor, `loam-dapr`, the shared `daprd` and Envoy never lands in a tenant's cgroup (it is platform overhead by construction). JIT and compilation that Loam does for a tenant (wasmtime's Cranelift compile at deploy, workerd's script compile at first load) is reported in `compile_usec`, GC inside a tenant's own process stays tenant CPU, and how each is priced is `loam-platform`'s decision (Q-RT-7).
4. **A usage report is also a CloudEvent** `dev.loam.meter.usage.v1` (`id` = `<host_id>:<seq>:<index>`, `source` = `/hosts/<host_id>`, `data` = the `Invocation`), so a consumer that prefers streams can read the high-rate path (D365) instead of the socket. The engine does not write it unless a consumer configures the stream (D202: no default dependency).
5. **Rating, the ledger, invoices and reconciliation against provider invoices** are `loam-platform` (D190, D202). The draft's "emit metering CloudEvents into Iceberg tables" is what that platform does with item 4.

The sources stay those of D175: fuel or epochs, cgroup `cpu.stat`, `getrusage`; eBPF last, as a cross-check.

## 13. The conformance suite (D377)

`crates/operon-conformance` (the draft's `loam-conformance`; renamed with the Loam rename):

| # | Area | What | Where it runs |
|---|---|---|---|
| 1 | Golden corpora | per protocol and version, valid and invalid, each with its expected canonical JSON; partner quirks as named fixtures | every PR (path-filtered) |
| 2 | Round trips | `proptest`: wire → canonical → wire equal modulo `ext` moves; downgrade then upgrade never invents data | every PR |
| 3 | Fuzzing | `cargo-fuzz` targets per decoder (2.6, 2.5, 2.4 JSON; Google protobuf; CloudEvents JSON and protobuf) | nightly CI only, never on the build machine |
| 4 | Cross-protocol differential | one logical auction as 2.6 JSON, 2.5 JSON and Google protobuf, over HTTP/1.1 at the edge and Connect, gRPC and gRPC-Web on the canonical service: identical canonical output | every PR |
| 5 | Negotiation | a partner that rejects 2.6, fallback, breaker open and recovery, header mismatch | every PR |
| 6 | Transport | h2 multiplexing, GOAWAY, connection reuse; h3 at Envoy when enabled; the official Connect conformance runner against the pinned connect-rust | nightly |
| 7 | CloudEvents and Arrow | the Loam profile, Parquet and IPC round trips, the additive-evolution check | every PR |
| 8 | Latency | `criterion` benches; the gate: p99 decode + canonicalize + encode overhead ≤ 1 ms (1% of a 100 ms `tmax`) on the reference corpus on the CI runner (Q364) | a `latency` CI job |

**Fixture licences.** The OpenRTB 2.x specification is licensed CC BY 3.0 (IAB Tech Lab, `openrtb2.x` README, read 2026-10-01); its examples may be copied with attribution, which `corpus/openrtb/NOTICE.md` gives with the release tag (`2.6-202606`, 2026-06-11). AdCOM 1.0 and OpenRTB 3.0 are CC BY 3.0 as well. The IAB's `openrtb.proto` is Apache-2.0 (Copyright 2020 IAB Tech Lab); Google's `openrtb.proto` is Apache-2.0 (Copyright 2014 Google); `openrtb-adx.proto`'s header is checked when it is vendored (GW4 Task 0). Specs are **vendored with provenance** (tag, commit, SHA-256 in `corpus/SOURCES.toml`) by a fetch script, not as git submodules, so a cargo build never needs the network. **No fixture comes from real traffic in this repository**: bid requests carry personal data (IP, device ids, consent strings); private partner corpora are `loam-platform`'s (Q365).

The suite combines with the TLA+, Lean and deterministic-simulation work planned for the orchestration layer; this document adds no formal spec.

## 14. Languages, embedding and the build order (D378, D379)

**Languages.** Rust for the data plane: the gateway, codecs, Arrow match/merge, metering. Java (Quarkus) and Go for control-plane services and customer extension points, through `buf`-generated Connect clients (`connect-java`/`connect-kotlin` and `connect-go`), exactly as D128 does for TypeScript and Python. In-process embedding only after profiling shows that the RPC hop is the bottleneck:

| Path | Status (checked 2026-10-01) | Notes |
|---|---|---|
| Java FFM (`java.lang.foreign`) + `jextract` over a `cbindgen` header | final since JDK 22 (JEP 454) | a native frame pins a virtual thread's carrier; bridge tokio to Mutiny with callbacks, not blocking calls |
| Go cgo | Go 1.26 cut cgo's baseline call overhead by about 30% (release notes) | batch calls; one call per Arrow batch, not per row |
| Arrow C Data Interface | stable | zero-copy batches across Rust, Java and Go |
| Wasm in the JVM (Chicory) or Go (wazero) | both Apache-2.0 **(verify current releases at the time)** | slower than native, sandboxed and crash-isolated |

**SIMD is not a foundation.** Go 1.27 shipped in August 2026: it adds a portable `simd` package and extends `simd/archsimd` to arm64 Neon and Wasm, and both still need `GOEXPERIMENT=simd` (Go 1.27 release notes); experiments are outside the Go 1 compatibility promise. The Java Vector API is in its eleventh incubator in JDK 26 (JEP 529, with no substantial change since JDK 25) and stays incubating until Valhalla's value classes reach preview. A warmed JIT can approach Rust on tight loops, but GC pauses, footprint, and JIT and GC CPU, which a CPU-billed tenant pays for, keep the data plane in Rust.

**The build order** (the draft's §12, mapped to tracks):

| # | Draft item | Plan | Depends on |
|---|---|---|---|
| 1 | Proto contract, CloudEvents mapping, Arrow schema | **GW1** ([plan](../plans/2026-10-01-gw1-proto-events-arrow.md)) | PRs #170/#171 merged |
| 2 | Gateway skeleton, OpenRTB 2.6 adapter, golden corpus | **GW2** ([plan](../plans/2026-10-01-gw2-gateway-openrtb26.md)) | GW1 |
| 3 | Fallback and negotiation; 2.5 and 2.4 adapters | **GW3** ([plan](../plans/2026-10-01-gw3-negotiation-openrtb25-24.md)) | GW2 |
| 4 | Google adapter, cross-protocol differential | **GW4** ([plan](../plans/2026-10-01-gw4-google-differential.md)) | GW3 |
| 5 | Arrow/Iceberg sink with the merge pipeline | GW5, not yet planned | GW1, M4 (Iceberg tables, links into them) |
| 6 | Resonate orchestration flows and the metering ledger | flows: GW5/D2; the ledger: `loam-platform` (D190) | D261 (on `main`), RN1 |
| 7 | `Runner` implementations | **RN1** ([plan](../plans/2026-10-01-rn1-runner-usage.md)) for the trait, process and Lambda runners and the usage reporter; `SupervisorRunner` with F1 | GW1 (`loam.meter.v1` additions) |
| 8 | HTTP/3 on the internal mesh | GW6, behind a flag (Q372) | — |
| 9 | Java and Go SDKs, optional embedding | GW7 | GW2 |

Track GW, like tracks R, D and J, interleaves on the one-build machine: one cargo build at a time, GW crates kept out of `operon`'s default features (`rtb` is off by default).

## 15. Contradictions with earlier decisions

| # | The draft says | Earlier | Resolution |
|---|---|---|---|
| 1 | "Durable orchestration: Resonate on TiDB"; build order item 6 "(TiDB)"; §4.3 "Resonate authoritative store: TiDB (our fork)" | D260 (no TiDB anywhere), D261 (Resonate on the native TiKV backend) | D261 stands. The native TiKV store is on `main` (PR #114, `--durable-store tikv://…`, feature `durable-tikv`). Resonate runs on TiKV |
| 2 | Correction 2: "Resonate is not backed by TiKV … Use TiDB" | D261, PR #114 | True of upstream Resonate, not of Loam's fork, which adds `resonate-server-tikv`. Correction withdrawn |
| 3 | §4.3: "Loam metastore … (redb, Postgres, TiDB)" | D124, D179, D260 (TiKV; no TiDB backend); D58 (Postgres and DynamoDB in M2) | The metastore backends are openraft/redb (dev, standalone), TiKV (clusters), Postgres and DynamoDB (M2) |
| 4 | §4 diagram: "Event log (NATS/S3)" | D4, D72, D270 (Loam streams are the log); D176, D183 (NATS through the shared `daprd`) | The event log is Loam streams on the bucket; NATS is a long-tail binding only |
| 5 | §7: "Emit metering CloudEvents into Iceberg tables … Reconcile against provider invoices" | D190, D202, D220 | The record spec and the runner collectors are open (D376); the ledger and reconciliation are `loam-platform` |
| 6 | §10: "Metering agent" and "Self-hosted single-org billing" in the open column | D202 (the node agent that reads the hooks is Loam Cloud), D220 (metering and billing are `loam-platform`) | Collectors that produce the hooks are open; the aggregating agent and billing stay `loam-platform`. Showback for a single organisation through open dashboards over the hooks is proposed as open; billing is Q362 for the owner |
| 7 | §10 "Open decision: … keep the multi-tenancy layer closed …" | D220 (approved 2026-09-29) | Answered by D220: the multi-tenant control plane stays in `loam-platform`. Reopening it is the owner's call (Q362) |
| 8 | §4.1: required extension `idempotencykey` | D270 (dedupe on `source` + `id`) | Not adopted: `id` is the idempotency key (D364) |
| 9 | §4.1: required extension `schemaversion` | CloudEvents `dataschema`; D270 | Not adopted: `type` suffix plus `dataschema` URN (D364) |
| 10 | §6: types `io.loam.<domain>.<name>.v1` | §02 §7.4 synthesizes `dev.loam.stream.record` | `dev.loam.` for consistency with what is built; the owner may move both to `dev.loams.` (Q361) |
| 11 | §4.3: "Every key is prefixed `tenant/{id}/…`" | D25, §03 (`ns/<ns>/`), §20, §26 §6.8 (keyspace plus prefix) | The rule is kept in Loam's existing forms (D374) |
| 12 | §4.3: "Money is integer minor units" | — | Integer, but micros, because CPM prices are sub-cent (D368, D374) |
| 13 | §4.3: "Timestamps are i64 nanoseconds" | §27's `HostReport` uses ms and µs; §08 §2 (ns needs Iceberg v3) | New contracts use ns; versioned contracts keep their units; Iceberg v3 or a `time_ns` column (D372, Q368) |
| 14 | §6: "Iceberg `MERGE INTO` for profile upserts" | §08 §1 keyed and versioned tables, §09 links | Profiles are `VERSION BY` tables written as upserts (D373) |
| 15 | §3: "Connect-RPC in Rust: verify current crate maturity. Fallback is tonic plus a small Connect codec" | D128, D206 (connect-rust) | Verified; no fallback (D362). `operon-stream-grpc` is still tonic (Q369) |
| 16 | §4.4: "Dapr runs as a sidecar where needed" | D183 (one shared `daprd` per cluster, no sidecar per function) | D183 stands |
| 17 | §4.4: Workers "call Resonate HTTP gateway" | D138, D111 (the Resonate listener is loopback-only until the unified auth plan) | Workers call Loam's authenticated ingress after D111 (Q373) |
| 18 | §4.4: four co-equal runner targets | D170 (placement next to data is the advantage) | `SupervisorRunner` is the default; the others are options (D375) |
| 19 | §3: "HTTP/3 opt-in" | D176 (HTTP/3 in phase 1) | No conflict: D176's HTTP/3 is at the Envoy edge; inside, h2 (D361) |
| 20 | §5.2: "Google Authorized Buyers real-time-bidding protobufs (`BidRequest`/`BidResponse`)" | — | That protocol was sunset on 2025-04-30; Google is OpenRTB (D371) |
| 21 | §10 table row "Protocol adapters and version-fallback logic" open; "Private partner-quirk corpora" paid | D220 | Consistent with D220 (D379) |

## 16. Licensing (D379)

| Open (Apache-2.0, this repository) | `loam-platform` (proprietary) |
|---|---|
| `loam.rtb.v1`, `loam.events.v1`, the `loam.meter.v1` additions, the CloudEvents profile, the Arrow mapping | the multi-tenant control plane: isolation policy, quotas per plan, invoicing (D220) |
| `operon-protocol`, `operon-openrtb`, `operon-openrtb-proto`, `operon-rtb-gateway`, negotiation | rating, the ledger, reconciliation against provider invoices (D190, D202) |
| `operon-conformance` and the public corpus | private partner-quirk corpora from real traffic (Q365) |
| `operon-runner`, `operon-meter` (the report emitter and test consumer) | hosted conformance-as-a-service and the "Loam Certified" programme |
| showback dashboards over the hooks (proposed, Q362) | SLA, support, air-gapped installs |

Keep conformance open: a closed suite makes support claims unverifiable. Apache-2.0 lets anyone, a hyperscaler included, host the code; the defences are the trademark, certification and being the reference implementation. FSL or BSL would block that but contradicts D11 and the open-standards position, so it is not proposed.

## 17. Risks

| Risk | Mitigation |
|---|---|
| Ad-tech is a new product direction beside the retrieval engine (D42–D57) | Owner decision before GW2 (Q360); GW1 is useful without it (the waist, the profile, the Arrow mapping, the meter additions) |
| OpenRTB's monthly 2.6 releases add fields | `int32` lists and raw `ext`; a corpus refresh per release; unknown fields are warnings, not errors |
| Downgrade rules wrong for a partner | Rule tables generated from the specs and reviewed; partner-record overrides; `NotRepresentable` instead of a guess |
| p99 overhead eats `tmax` | Synchronous codecs, bounded allocation, the latency gate (§13 row 8) |
| High-rate events overwhelm streams | Batching, the bounded queue with counted drops (D365); sampling per partner record if needed |
| Personal data in fixtures or events | No real traffic in the repository; events carry what the auction carries, under the tenant's retention and erasure (§18 §9) |
| Lambda CPU attribution is in the tenant's process | Capped by billed duration × vCPUs and flagged `cpu_estimated` (Q366) |
| connect-rust and buffa are pre-1.0 | Pinned; both are in the workspace already and pass conformance; the protos are the contract |

## 18. Open questions

| # | Question | Owner | Needed by |
|---|---|---|---|
| Q360 | Is ad-tech (OpenRTB, Google RTB) a Loam product direction, beside the retrieval engine's D42–D57 scope? GW1 does not depend on it; GW2–GW4 do | Founder | Before GW2 |
| Q361 | Event type prefix: `dev.loam.` (what §02 §7.4 builds) or `dev.loams.` (the registered domain), for both §02's synthesized type and D364's types | Founder | GW1 Task 3 |
| Q362 | Showback and single-organisation billing in the open repository (the draft) or `loam-platform` only (D220); does the owner reopen D220's multi-tenant boundary | Founder | Before RN1 Task 6 |
| Q363 | Loam's role in the auction: bidder host (inbound first), exchange side (outbound first), or both; it orders GW2 and GW3 | Founder | GW2 Task 0 |
| Q364 | The latency gate: p99 overhead ≤ 1% of a 100 ms `tmax` on the CI runner, or a different budget | Eng | GW2 Task 8 |
| Q365 | Private partner corpora: anonymisation rules, retention and where they live (`loam-platform`) | Founder, counsel | Before any real-traffic fixture |
| Q366 | Lambda CPU attribution: trust the in-process `getrusage` delta capped by billed duration, or bill billed duration on Lambda | Founder | RN1 Task 5 |
| Q367 | Cloud Run and Container Apps runners: build, or document only | Founder | After RN1 |
| Q368 | Iceberg v3 `timestamptz_ns` on the pinned iceberg-rust and Lakekeeper by M4, or the `time_ns` column | Eng | GW5 plan |
| Q369 | Move `operon-stream-grpc` from tonic and prost to connect-rust and buffa (D128), and when | Eng | M2 stream API plan |
| Q370 | OpenRTB 2.3 and 3.0 adapters: the demand that triggers them | Founder | After GW3 |
| Q371 | The production bidder: an in-process Rust trait object, a T1 wasmtime component in the gateway process, or a forwarded function call | Eng | F1 plan |
| Q372 | Internal HTTP/3: the condition that enables it (cross-region links, measured loss) | Eng | GW6 |
| Q373 | `WorkersRunner` before the unified auth plan: wait for D111, or a scoped token for the ingress only | Eng | With the Cloudflare document's spike |
| Q374 | Partner capability records: the `ControlStore` (R2) or a TiKV keyspace of the gateway's own; who edits them (console, API) | Eng | GW3 Task 5 |

## 19. Sources

Read on 2026-10-01 unless a date is given.

- The owner's draft "Loam Serverless Runtime — Consolidated Plan" v1 (2026-09-30), `chatdump.md` lines 631–812, 937–947.
- Repository (`origin/main` at `9eaddae`): `Cargo.toml` (`connectrpc` 0.9.1, `buffa` 0.9.2, `serde_json` features, `arrow`/`parquet` 58), `buf.yaml`, `crates/operon-cloudevents/src/lib.rs`, `crates/operon-stream-grpc/proto/loam/stream/v1/stream.proto`, `crates/operon/Cargo.toml` (`durable-tikv`), `crates/operon/src/main.rs` (`--durable-store tikv://`); PR #171 branch `cloudevents-grpc` (`ProduceCloudEvents`, `io.cloudevents.v1`); PR #114 (merged). Design §02 §7.4, §08 §1–§2, §20, §21, §24, §25, §26, §27, `docs/open-core.md`; D4, D6, D11, D13, D15, D25, D49, D58, D72, D73, D111, D124, D128, D138, D170–D190, D200–D202, D206, D220, D260, D261, D270.
- connect-rust: Connect RFC 007 "Rust Implementation" (connectrpc.com/docs/governance/rfc/rust-implementation); buf.build blog "connect-rust joins the Connect project"; `github.com/connectrpc/connect-rust` (Apache-2.0; releases v0.9.1 and v0.8.2 on 2026-09-21). buffa: `github.com/anthropics/buffa` README (proto2 and proto3 as editions presets, unknown-field preservation, `buffa-descriptor` with `DynamicMessage` and extensions; Apache-2.0).
- Go: go.dev/doc/go1.26 ("The baseline runtime overhead of cgo calls has been reduced by ~30%"); go.dev/doc/go1.27 (August 2026; `simd` and `simd/archsimd` behind `GOEXPERIMENT=simd`; arm64 Neon and Wasm). Java: openjdk.org/jeps/529 (Vector API, Eleventh Incubator, JDK 26); JEP 454 (FFM, final in JDK 22).
- Cloudflare: developers.cloudflare.com/workers/platform/pricing; developers.cloudflare.com/changelog/2025-04-09-workers-timing (CPU and wall time in Tail Workers and trace events).
- CloudEvents: `github.com/cloudevents/spec` tag v1.0.2, `cloudevents/formats/` (JSON, protobuf, Avro) and `cloudevents/extensions/` (`distributed-tracing.md`, `partitioning.md`); `cloudevents/working-drafts/` on the default branch (Avro compact, CBOR, XML).
- OpenRTB: `github.com/InteractiveAdvertisingBureau/openrtb2.x` (README licence statement CC BY 3.0; `2.6.md` §2.5 `x-openrtb-version`, §2.6 versioning behaviour, Appendix B change logs 2.4→2.5 and 2.5→2.6; release `2.6-202606` on 2026-06-11); `github.com/InteractiveAdvertisingBureau/openrtb` (3.0) and `AdCOM` (CC BY 3.0).
- IAB protobuf: `openrtb2.x/proto/src/main/com/iabtechlab/openrtb/v2/openrtb.proto` (edition 2023, Apache-2.0 header, extension ranges by organisation) and `openrtb2.x/proto/README.md`.
- Google: ads-developers.googleblog.com 2024-03 "Migration to OpenRTB: deprecation of the Authorized Buyers RTB protocol" and 2025-01-24 "Extending deprecation period" (sunset 2025-04-30); developers.google.com/authorized-buyers/rtb/downloads/openrtb-proto (OpenRTB 2.6 proto2, `com.google.openrtb`) and the downloads page for `openrtb-adx.proto`.
- crates.io: `iab-specs` 0.5.1 (Apache-2.0, 2026-05-11; `github.com/remysaissy/iab-specs`), `openrtb2` 0.3.0 (MIT OR Apache-2.0, 2022-12-14), `openrtb` 0.2.1 (2023-01-04).
