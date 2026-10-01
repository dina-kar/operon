# GW2 — The Protocol Gateway Skeleton, the OpenRTB 2.6 Adapter and the Golden Corpus Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, headers, status codes, metric names), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: Planned** (2026-10-01). **Track GW** (design [§34](../design/34-protocol-gateway-and-standards.md)), build-order item 2. **Gated on Q360** (the owner confirms ad-tech as a Loam direction) and ordered by **Q363** (bidder side first, as written here, or exchange side first, which moves Task 4's outbound half and Task 6's `PartnerClient` ahead of the bidder). Depends on GW1 merged. Branches `gw2-t<N>`, stacked; PRs target `main`. GW2 adds crates and an off-by-default feature `rtb` on `operon`; it changes no existing code path except the server's listener wiring.

**Goal:**
- `operon-protocol`: the `ProtocolAdapter` trait, the wire types, versions, the adapter registry and the error and warning types (D367).
- `operon-openrtb`: the superset OpenRTB 2.x serde model, exact decimal prices, the 2.6 profile and canonicalization, and the `OpenRtb26` adapter (D368, D370).
- `operon-conformance`: the crate, the OpenRTB 2.6 golden corpus with provenance, the golden harness, the 2.6 round-trip property and an `iab-specs` differential oracle (D377 rows 1, 2).
- `operon-rtb-gateway`: the inbound auction route, the `Bidder` trait with a rule bidder and a forwarding bidder, the canonical `AuctionService` over connect-rust, the bounded `EventSink` (D365, D366), metrics; mounted in `operon` behind `rtb`, loopback only (D111).
- Transport tests (HTTP/1.1, h2c, multiplexing, GOAWAY, gzip) and the latency gate (D377 rows 6 and 8).

**Architecture:**
- **Codecs are synchronous** (`ProtocolAdapter` methods are plain functions). The gateway runs them inline on the request task; at 1 ms p99 (the gate) they do not need `spawn_blocking`.
- **One superset model, many profiles.** `operon-openrtb::model` covers 2.4–2.6-202606 including fields 2.6 removed; GW2 implements the 2.6 profile only, GW3 adds 2.5 and 2.4 as profiles over the same model.
- **Prices never touch `f64`.** Price fields deserialize through `&serde_json::value::RawValue` into `operon_rtb_proto::Money` (GW1 Task 2's decimal parser).
- **The gateway** is an axum `Router` built by `operon-rtb-gateway`, so `operon` (dev) and later `loam-gateway` (clusters, behind Envoy) mount the same code. The connect-rust `AuctionService` is mounted on the same router.
- **Events** leave through `EventSink`, which batches `dev.loam.rtb.auction.v1` events (GW1 Task 3) into plain produce calls on a `StreamProducer` (D365). In `operon` the producer is the in-process log writer; elsewhere it is the `loam.stream.v1` gRPC client.

**Tech Stack:** Rust 1.97.1, edition 2024, workspace lints. Workspace crates: `axum` 0.8, `connectrpc` 0.9.1 (`axum` feature; `client` as a dev-dependency), `buffa` 0.9.2, `serde`, `serde_json` (adds the `raw_value` feature, Ruling 2), `bytes`, `http`, `tokio`, `tower`, `tracing`, `thiserror`, `proptest`, `criterion`, `reqwest` 0.12 (forwarding bidder and tests). New, checked in Task 0: `flate2` (MIT OR Apache-2.0, request gzip; already in the tree through other crates, verify), `hdrhistogram` 7 (MIT OR Apache-2.0, the latency gate), and **dev-dependency** `iab-specs` 0.5 (Apache-2.0, the oracle).

**Spec:**
- [§34](../design/34-protocol-gateway-and-standards.md): §5 (the gateway, the trait, the bidder), §6 (the canonical model), §7.1 (inbound versions), §8.1–§8.2 (the adapters, money), §13 (conformance), §15 rows 10–12.
- OpenRTB 2.6-202606 (`openrtb2.x` tag `2.6-202606`): §2.4–§2.6 (encoding, compression, the version header), §3 (request objects), §4 (response objects), §6 (examples).
- As built after GW1: `operon-rtb-proto` (`loam::rtb::v1`, `adcom`, `money`), `operon_cloudevents::{profile, proto_data}`, `operon-events-arrow`.

## Global Constraints

Same as the M1 overview §8, plus:
- **Loopback only (D111).** `--rtb-listen` accepts only loopback addresses. Any other address fails startup with `rtb listen on <addr>: only loopback addresses are served until the unified auth plan (D111)`. No default port; when given, the server prints `operon rtb listening on http://<addr>`.
- **Feature `rtb`, off by default.** `operon`'s default features do not change. CI adds a path-filtered `rtb` job.
- **No real traffic** in fixtures, logs or test output (D377). Corpus files come from the specification's examples or are written by hand.
- **The build machine.** One cargo build at a time, the shared target, `-j 6`, lld. `cargo-fuzz` never runs locally. Benchmarks for the gate run with `CARGO_BUILD_JOBS=4 CARGO_INCREMENTAL=0`.
- **Commit areas:** `rtb`, `conformance`, `ci`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **Own the serde model; `iab-specs` is a test oracle only.** `iab-specs` 0.5.1 is Apache-2.0 and current, but its prices are `f64`, it has no 2.4 and no fields 2.6 removed, and `ext` is `Vec<u8>` | D374 and GW3's 2.4 profile need the superset; the oracle still catches field-name mistakes | A second model to maintain; Task 0 re-checks whether `iab-specs` has changed, and if it now meets all three, Task 2 wraps it instead |
| 2 | **`serde_json`'s `raw_value` feature is added at the workspace level**; `arbitrary_precision` is not | `raw_value` only adds `RawValue`; `arbitrary_precision` would change every crate's `Number` through feature unification | None known; Task 0 builds the workspace's `serde_json` users once with the feature to confirm |
| 3 | **Unknown fields are not collected on the hot path.** `DecodeCx { collect_unknown: bool }` (default false) re-parses into `serde_json::Value` and diffs against the model's known field set only when true (conformance, debugging, a sampled 1-in-10 000 production check reported as `loam_rtb_unknown_fields_total{object}`) | Collecting with `#[serde(flatten)]` would allocate on every request and does not combine with `RawValue` | Unknown-field warnings are sampled in production; the corpus run collects them all |
| 4 | **Inbound route:** `POST /rtb/v1/{partner}/bid`, `Content-Type: application/json` (charset ignored), optional `Content-Encoding: gzip` on the request, `x-openrtb-version` on request and response. A bid answers 200 with the encoded `BidResponse`; a no-bid answers **204** with no body (OpenRTB 2.6 §2 and §4.2.1: an empty 204 is the most bandwidth-friendly no-bid); a decode failure answers **400** with `text/plain` reason (at most 200 bytes) and the header `x-loam-rtb-error: <code>`; an unknown partner answers **404**; over the body limit **413**; an unsupported version **400** with code `unsupported_version` | The spec's HTTP conventions; error bodies short because exchanges log them | Partners that expect `nbr` in a 200 instead of 204 get it through the partner record's `no_bid: "nbr"` option (Task 6) |
| 5 | **Partner records come from a TOML file** (`--rtb-partners <path>`), reloaded on `SIGHUP`; the `CapabilityStore` of GW3 replaces it later (Q374) | Enough for one-node development and the conformance suite | Multi-node configuration waits for GW3 |
| 6 | **The bidder deadline** is `min(tmax, partner.tmax_ms, 1000 ms) − decode − margin` (margin `--rtb-deadline-margin-ms`, default 10); a missed deadline answers the no-bid of Ruling 4 and counts `loam_rtb_bidder_timeouts_total` | A late bid is worthless (§34 §5.3) | Bidders that need more time configure a larger partner `tmax_ms` |
| 7 | **The event sink never blocks the auction**: a full queue drops the oldest batch and counts it (D365); `--rtb-events-stream <ns>/<stream>` names the target, and without it no events are produced | §34 §4.3 | Events are lossy under overload, by design |
| 8 | **Event ids** are `<auction id>:<node id>:<per-node sequence>` and `source` is `/rtb/<partner>` | Unique without coordination; the table dedupes on `(source, id)` (D365) | None |
| 9 | **The corpus is vendored with provenance, not a submodule** (§34 §13): `scripts/conformance/fetch-openrtb.sh` downloads `2.6.md` at the pinned tag, checks its SHA-256 against `corpus/SOURCES.toml`, extracts fenced JSON examples between marker headings into `corpus/openrtb/2.6/valid/spec-<section>-<n>.json`, and is run by a person, never by `cargo` | Builds stay offline; licences stay visible | A spec refresh is a deliberate PR |

## Carried in

From GW1: the as-built names of `operon-rtb-proto`, the profile module and `event_from_proto`; GW1's "Rulings made during execution". From §34: Q363 decides whether Task 4's outbound half moves first.

## Review Focus

1. **Exact money.** No price ever passes through `f64`. Tests: Task 2 (`price_is_parsed_from_decimal_text`, `price_rounding_is_half_even`, `no_f64_in_price_paths` — a grep test over `src/model/` for `f64` on price fields).
2. **Lossless 2.6 round trip modulo ext.** Tests: Task 5 (`golden_26_*`), Task 8 (proptest `wire26_canonical_wire26`).
3. **The tenant comes from the route, never the body.** Tests: Task 6 (`event_tenantid_is_the_routes`).
4. **The auction never waits on events.** Tests: Task 6 (`full_event_queue_does_not_delay_bids`).
5. **Loopback refusal.** Tests: Task 7 (`non_loopback_is_refused`).

## File structure

```
Cargo.toml                                    # members; serde_json raw_value; dev iab-specs, hdrhistogram
crates/operon-protocol/                       # new (Task 1)
  src/{lib.rs,adapter.rs,wire.rs,version.rs,registry.rs,error.rs,warning.rs}
  tests/{version.rs,registry.rs}
crates/operon-openrtb/                        # new (Tasks 2–4)
  src/{lib.rs,model/{mod.rs,request.rs,response.rs,price.rs,ext.rs},profile/{mod.rs,v2_6.rs},
       canon/{mod.rs,to_canonical.rs,from_canonical.rs},adapter.rs,unknown.rs,gzip.rs}
  tests/{model.rs,price.rs,canon.rs,adapter.rs}
  benches/codec.rs
crates/operon-conformance/                    # new (Task 5)
  corpus/SOURCES.toml  corpus/openrtb/NOTICE.md
  corpus/openrtb/2.6/{valid,invalid}/*.json  corpus/openrtb/2.6/{valid,invalid}/*.expected.json
  src/{lib.rs,golden.rs,corpus.rs}
  tests/{openrtb26.rs,oracle.rs}
scripts/conformance/fetch-openrtb.sh
crates/operon-rtb-gateway/                    # new (Task 6)
  src/{lib.rs,router.rs,partner.rs,bidder.rs,deadline.rs,events.rs,service.rs,metrics.rs,error.rs}
  tests/{route.rs,bidder.rs,events.rs,service.rs,transport.rs}
crates/operon/{Cargo.toml,src/server.rs,src/main.rs}   # feature rtb; --rtb-listen, --rtb-partners, --rtb-events-stream
.github/workflows/ci.yml                      # job rtb (path-filtered), job latency
docs/design/34-protocol-gateway-and-standards.md  CHANGELOG.md
```

### Task 0: Reconcile, and check the open questions

**Files:** read GW1's crates as merged, `crates/operon/src/{server.rs,main.rs}` (how `pgwire`'s listener is wired, to copy it), `Cargo.toml`. Fill "Rulings made during execution".

**Checks:**
- Q360 answered yes, and Q363's answer (if exchange side first, reorder: Task 4's `encode_request`/`decode_response` and a minimal `PartnerClient` before Task 6's bidder; record it).
- `iab-specs`'s latest version and whether it still has `f64` prices, no 2.4 and opaque `ext` (Ruling 1).
- The workspace builds with `serde_json/raw_value` (one `cargo check --workspace`, Ruling 2).
- `flate2`, `hdrhistogram`, `iab-specs` against `deny.toml`.
- Whether `connectrpc`'s axum integration lets one `Router` carry both a plain axum route and a Connect service (it does for `operon-live`; confirm the as-built pattern).
- The list of fenced JSON examples in 2.6-202606's §6 and their section anchors, for Task 5's script.

**Commit:** `docs: reconcile GW2 with main`.

### Task 1: `operon-protocol`

**Files:** `crates/operon-protocol/src/{lib.rs,adapter.rs,wire.rs,version.rs,registry.rs,error.rs,warning.rs}`, `crates/operon-protocol/tests/{version.rs,registry.rs}`.

**Produces:**

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProtocolVersion { pub major: u16, pub minor: u16 }
impl ProtocolVersion { pub fn parse_header(v: &str) -> Result<Self, VersionError>; }   // "2.6", " 2.6 ", refuses "2", "2.6.1", "v2.6"
impl fmt::Display for ProtocolVersion;                                               // "2.6"
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ProtocolId { OpenRtb(ProtocolVersion), OpenRtbProto(ProtocolVersion), GoogleOpenRtb }
pub enum Encoding { Json, Protobuf }
pub struct WireHead { pub content_type: Option<String>, pub content_encoding: Option<String>, pub openrtb_version: Option<String>, pub path_partner: Option<String> }
pub struct WireMessage { pub head: WireHead, pub encoding: Encoding, pub body: Bytes }
pub enum Direction { Inbound, Outbound }
pub struct Capabilities { pub versions: Vec<ProtocolVersion>, pub encodings: Vec<Encoding>, pub directions: Vec<Direction>, pub max_body: usize /* default 512 KiB */ }
pub enum Confidence { Certain, Likely }
pub enum Detection { Match { id: ProtocolId, confidence: Confidence }, NoMatch }
pub struct DecodeCx { pub partner: String, pub collect_unknown: bool, pub received_unix_ns: i64 }
pub struct EncodeCx { pub partner: String, pub version: ProtocolVersion }
pub struct Decoded<T> { pub value: T, pub warnings: Vec<Warning> }
pub struct Encoded { pub msg: WireMessage, pub loss: LossReport }
pub struct LossReport { pub moved_to_ext: Vec<FieldPath>, pub dropped: Vec<FieldPath> }
pub struct FieldPath(pub String);                                   // "imp[0].video.podid"
pub enum Warning { UnknownField(FieldPath), CoercedValue { path: FieldPath, detail: String }, PromotedFromExt(FieldPath), ConflictingLocations(FieldPath), PriceRounded(FieldPath) }
pub enum DecodeError { BodyTooLarge { limit: usize }, BadEncoding(String), Syntax { offset: usize, detail: String }, MissingRequired(FieldPath), InvalidValue { path: FieldPath, detail: String }, UnsupportedVersion(ProtocolVersion) }
pub enum EncodeError { NotRepresentable { path: FieldPath, version: ProtocolVersion }, Internal(String) }
pub trait ProtocolAdapter { /* exactly §34 §5.2 */ }
pub struct AdapterRegistry { /* by ProtocolId */ }
impl AdapterRegistry { pub fn register(&mut self, a: Arc<dyn ProtocolAdapter>); pub fn get(&self, id: ProtocolId) -> Option<&Arc<dyn ProtocolAdapter>>; pub fn detect(&self, head: &WireHead) -> Option<ProtocolId>; }
impl DecodeError { pub fn code(&self) -> &'static str; }   // "body_too_large", "bad_encoding", "syntax", "missing_required", "invalid_value", "unsupported_version"
```

**Semantics:** `detect` asks every adapter and returns the first `Certain` match, else the first `Likely` match in registration order. Registering two adapters with the same `ProtocolId` panics (a programming error).

**Tests:** `header_parse_cases`; `version_ordering`; `registry_prefers_certain_over_likely`; `duplicate_registration_panics`; `error_codes_are_stable` (a table).

**Commit:** `rtb: add the ProtocolAdapter trait and the adapter registry`.

### Task 2: The superset OpenRTB 2.x model and exact prices

**Files:** `crates/operon-openrtb/src/{lib.rs,model/{mod.rs,request.rs,response.rs,price.rs,ext.rs},unknown.rs,gzip.rs}`, `crates/operon-openrtb/tests/{model.rs,price.rs}`.

**Produces:** `model::request::{BidRequest, Source, Regs, Imp, …}` and `model::response::{BidResponse, SeatBid, Bid}` with `#[derive(Deserialize, Serialize)]`, every field `Option<T>` (or `Vec<T>` with `#[serde(default, skip_serializing_if = "Vec::is_empty")]`), field names exactly the OpenRTB attribute names, including 2.6-removed attributes (`banner.wmax`, …) and 2.6-deprecated ones; `ext: Option<Box<RawValue>>` on every extensible object; `model::price::Price(Money)` with a `Deserialize` impl that reads `&RawValue` and calls `Money::from_decimal_str` with the object's currency resolved later (Price holds micros only; the currency is attached in canonicalization); `unknown::collect(raw: &[u8], known: &KnownFields) -> Vec<FieldPath>`; `gzip::inflate(body, limit) -> Result<Bytes, DecodeError>` (refuses output over the limit, so a gzip bomb cannot expand past it).

**Semantics:** Integer flags stay integers (GW1 Ruling 6); string arrays stay strings; a JSON `null` is treated as absent. A price that is a JSON string (`"1.5"`, seen from non-conforming senders) is accepted with `Warning::CoercedValue`.

**Tests:** `deserializes_every_spec_example` (the files Task 5 vendors; until then, two inline examples from §6); `price_is_parsed_from_decimal_text` (`1.5`, `0.15`, `12.000001`, `1e-3` refused); `price_rounding_is_half_even`; `price_string_is_coerced_with_warning`; `null_is_absent`; `ext_is_kept_byte_exact`; `gzip_bomb_is_refused`; `no_f64_in_price_paths` (reads `src/model/*.rs` and fails if any field named `price`, `bidfloor` or `*floor*` has type `f64`).

**Commit:** `rtb: add the OpenRTB 2.x superset model with exact prices`.

### Task 3: The 2.6 profile and canonicalization

**Files:** `crates/operon-openrtb/src/{profile/mod.rs,profile/v2_6.rs,canon/mod.rs,canon/to_canonical.rs,canon/from_canonical.rs}`, `crates/operon-openrtb/tests/canon.rs`.

**Produces:**

```rust
pub struct Profile { pub version: ProtocolVersion, pub fields: &'static [FieldRule] }
pub struct FieldRule { pub path: &'static str, pub since: ProtocolVersion, pub removed_in: Option<ProtocolVersion>, pub ext_before: Option<&'static str>, pub downgrade: Downgrade }
pub enum Downgrade { Keep, MoveToExt(&'static str), Drop, Map(fn(&mut serde_json::Value)), NotRepresentable }
pub static V2_6: Profile;
pub fn to_canonical(req: model::BidRequest, profile: &Profile, cx: &DecodeCx) -> Result<Decoded<Auction>, DecodeError>;
pub fn from_canonical(a: &Auction, profile: &Profile) -> Result<(model::BidRequest, LossReport), EncodeError>;
pub fn response_to_canonical(resp: model::BidResponse, profile: &Profile, cx: &DecodeCx) -> Result<Decoded<AuctionResponse>, DecodeError>;
pub fn response_from_canonical(r: &AuctionResponse, profile: &Profile) -> Result<(model::BidResponse, LossReport), EncodeError>;
```

**Semantics:**
- Required fields per 2.6: `BidRequest.id`, at least one `imp` with `id`; exactly one of `site`, `app`, `dooh` (2.6 §3.2.1: a DOOH request must not contain site or app); `BidResponse.id`; `SeatBid.bid` non-empty; `Bid.id`, `Bid.impid`, `Bid.price`. Missing → `DecodeError::MissingRequired`.
- **Ext promotion in 2.6:** 2.6 senders that still use the 2.5 convention (`regs.ext.gdpr`, `regs.ext.us_privacy`, `user.ext.consent`, `user.ext.eids`, `source.ext.schain`) are promoted when the first-class field is absent (`Warning::PromotedFromExt`); when both are present, the first-class value wins (`Warning::ConflictingLocations`) and the `ext` member stays in `ext` unchanged.
- Currency: `imp.bidfloorcur` (default `USD`) attaches to `imp.bidfloor`; `deal.bidfloorcur` (default `USD`) to the deal's; `BidResponse.cur` (default `USD`) to every `bid.price` in it.
- `Provenance { protocol: "openrtb", version: "2.6", partner, received_unix_ns }`.
- `from_canonical` at 2.6 is lossless: `LossReport` is empty unless the canonical message carries `Legacy` fields 2.6 removed (those are written back only for 2.4/2.5 in GW3; at 2.6 they are dropped and reported).

**Tests:** `required_fields_are_enforced`; `dooh_with_site_is_invalid`; `promotes_25_style_ext_in_26`; `first_class_wins_over_ext`; `currency_defaults_to_usd`; `deal_floor_currency_is_its_own`; `canonical_back_to_26_is_identity_on_spec_examples`; `legacy_fields_are_reported_at_26`.

**Commit:** `rtb: canonicalize OpenRTB 2.6 requests and responses`.

### Task 4: The `OpenRtb26` adapter

**Files:** `crates/operon-openrtb/src/adapter.rs`, `crates/operon-openrtb/tests/adapter.rs`, `crates/operon-openrtb/benches/codec.rs`.

**Produces:** `pub struct OpenRtbAdapter { profile: &'static Profile }` with `OpenRtbAdapter::v2_6()`; `impl ProtocolAdapter` for it: `id() = OpenRtb(2.6)`; `capabilities()` (versions `[2.6]`, `Json`, both directions, `max_body` 512 KiB); `detect` (`Certain` when `x-openrtb-version` is `2.6`; `Likely` when the content type is JSON and no header is present); the four codec methods over Tasks 2–3; responses written with `x-openrtb-version: 2.6`.

**Semantics:** decode = (gzip inflate if `content-encoding: gzip`) → size check → `serde_json::from_slice` into the model → `to_canonical`; `collect_unknown` per Ruling 3. Encode writes compact JSON with fields in model order, omitting absent fields; a JSON consumer that compares by value sees equality with the input.

**Tests:** `decode_encode_spec_examples`; `header_version_mismatch_is_unsupported` (a `2.5` header to the 2.6 adapter gives `UnsupportedVersion`, which the gateway maps to 400 until GW3 registers 2.5); `outbound_encode_then_decode_response`; `oversize_body_is_refused_before_parse`; criterion benches `decode_26_spec_example_banner`, `decode_26_spec_example_video`, `encode_26_response` (run by Task 8's job).

**Commit:** `rtb: add the OpenRTB 2.6 adapter`.

### Task 5: `operon-conformance` and the 2.6 golden corpus

**Files:** `scripts/conformance/fetch-openrtb.sh`, `crates/operon-conformance/{Cargo.toml,corpus/SOURCES.toml,corpus/openrtb/NOTICE.md,corpus/openrtb/2.6/**,src/{lib.rs,golden.rs,corpus.rs},tests/{openrtb26.rs,oracle.rs}}`.

**Produces:**
- `SOURCES.toml`: per source, `name`, `url`, `tag` (`2.6-202606`), `commit`, `sha256`, `licence` (`CC-BY-3.0`), `fetched` (date).
- `NOTICE.md`: "Examples in `openrtb/2.6/valid/spec-*` are from the OpenRTB 2.6 specification by IAB Tech Lab, licensed under CC BY 3.0 (https://creativecommons.org/licenses/by/3.0/), at tag `2.6-202606`; changes: extracted from the document, whitespace normalized."
- Corpus layout: `valid/<name>.json` with `valid/<name>.expected.json` (the canonical message in proto3 JSON, `Provenance.received_unix_ns` fixed at 0 by the harness); `invalid/<name>.json` with `invalid/<name>.expected.json` = `{"error": "<code>", "path": "<path or null>"}`. Hand-written files: at least `banner-minimal`, `video-pod-26`, `native-12`, `audio`, `dooh`, `app-with-eids`, `site-with-schain`, `gdpr-first-class`, `gdpr-in-ext-25-style`, `multi-currency-floors`, `deal-floors`; invalid: `no-imp`, `site-and-dooh`, `price-exponent`, `truncated-json`, `wrong-type-imp`, `gzip-bomb` (generated by the test, not checked in).
- `golden::run(dir, adapter) -> GoldenReport` and `UPDATE_GOLDEN=1` rewriting `.expected.json`; `corpus::files(protocol, version, kind)`.

**Tests:** `golden_26_valid` (each valid file decodes to its expected canonical JSON and re-encodes equal to the input by JSON value); `golden_26_invalid` (each invalid file gives its expected code and path); `corpus_sources_are_recorded` (every corpus file under `spec-*` names a source in `SOURCES.toml`); `oracle_iab_specs_agrees` (every valid file also decodes with `iab_specs`' 2.6 model; for each field both models set, values agree, prices compared as decimals; disagreements listed, the test fails on any).

**Commit:** `conformance: add the OpenRTB 2.6 golden corpus and harness`.

### Task 6: `operon-rtb-gateway`

**Files:** `crates/operon-rtb-gateway/src/{lib.rs,router.rs,partner.rs,bidder.rs,deadline.rs,events.rs,service.rs,metrics.rs,error.rs}`, `crates/operon-rtb-gateway/tests/{route.rs,bidder.rs,events.rs,service.rs}`.

**Produces:**

```rust
pub struct GatewayConfig { pub partners: PartnerBook, pub deadline_margin: Duration /* 10 ms */, pub events: Option<EventsConfig>, pub tenant: Tenant, pub node_id: String }
pub struct PartnerRecord { pub id: String, pub protocol: String /* "openrtb" */, pub versions: Vec<ProtocolVersion>, pub encoding: Encoding, pub tmax_ms: Option<u32>, pub no_bid: NoBidStyle /* Http204 | Nbr */, pub bidder: String }
pub struct PartnerBook { /* id → PartnerRecord */ } impl PartnerBook { pub fn load(path: &Path) -> Result<Self, ConfigError>; }
#[async_trait] pub trait Bidder { async fn bid(&self, cx: &BidCx, auction: &Auction) -> BidDecision; }   // §34 §5.3
pub struct RuleBidder { /* rules: match on imp media type, size, deal id → fixed Money price */ }
pub struct ForwardBidder { /* POST application/protobuf `loam.rtb.v1.BidRequest` to a URL; reqwest with the deadline */ }
pub struct EventsConfig { pub stream: (String, String), pub max_events: usize /* 65 536 */, pub max_bytes: usize /* 64 MiB */, pub flush_every: Duration /* 50 ms */, pub flush_bytes: usize /* 1 MiB */ }
#[async_trait] pub trait StreamProducer: Send + Sync { async fn produce(&self, ns: &str, stream: &str, records: Vec<Record>) -> Result<(), ProduceError>; }
pub struct EventSink { /* bounded queue + flusher task */ } impl EventSink { pub fn try_push(&self, ev: CloudEvent); }
pub fn router(cfg: GatewayConfig, registry: AdapterRegistry, bidders: BidderBook, sink: Option<EventSink>) -> axum::Router;
```

- `service.rs`: the connect-rust `AuctionService` implementation over the same `Bidder`s (`Bid` takes a canonical `BidRequest` with `partner`).
- Metrics (Prometheus names): `loam_rtb_requests_total{partner,version,code}`, `loam_rtb_request_duration_seconds{partner}` (histogram), `loam_rtb_codec_duration_seconds{adapter,op}` (histogram), `loam_rtb_bidder_timeouts_total{partner}`, `loam_rtb_events_dropped_total{reason}`, `loam_rtb_unknown_fields_total{object}`, `loam_events_tenantid_overwritten_total`.

**Semantics:** Ruling 4 (status codes), Ruling 6 (deadline), Ruling 7 (sink), Ruling 8 (event ids). The route resolves the partner, picks the adapter (the record's protocol and version list, or `detect`), decodes, calls the bidder under `tokio::time::timeout`, encodes, answers, then `try_push`es one `dev.loam.rtb.auction.v1` event built with `event_from_proto`, `tenantid` from `GatewayConfig::tenant` (in clusters: from the route's credential), `traceparent` from the request's header or generated. `NoBidStyle::Nbr` answers 200 with `{"id": <auction id>, "nbr": <code>}`.

**Tests:** `bid_answers_200_with_version_header`; `no_bid_answers_204`; `no_bid_nbr_style`; `unknown_partner_is_404`; `bad_json_is_400_with_code`; `oversize_is_413`; `version_25_is_400_until_gw3`; `bidder_timeout_answers_no_bid`; `forward_bidder_sends_canonical_protobuf`; `event_is_emitted_per_auction`; `event_tenantid_is_the_routes` (a body that smuggles `ext.tenantid` changes nothing); `full_event_queue_does_not_delay_bids` (a blocked producer, 10 000 auctions, p99 unchanged within noise, drops counted); `auction_service_over_connect_grpc_and_grpcweb` (connect-rust client in all three protocols).

**Commit:** `rtb: add the protocol gateway with bidders and the event sink`.

### Task 7: Wiring into `operon`, and transport tests

**Files:** `crates/operon/{Cargo.toml,src/server.rs,src/main.rs}`, `crates/operon-rtb-gateway/tests/transport.rs`, `.github/workflows/ci.yml` (job `rtb`, path-filtered on `crates/operon-{protocol,openrtb,rtb-gateway,conformance}/**` and `proto/loam/rtb/**`).

**Produces:** feature `rtb` on `operon` (off by default) pulling in the gateway crates; `RtbConfig { listen, partners, events_stream, deadline_margin }` in `ServerConfig` behind `#[cfg(feature = "rtb")]`; flags `--rtb-listen`, `--rtb-partners` (required with `--rtb-listen`), `--rtb-events-stream`, `--rtb-deadline-margin-ms`; the in-process `StreamProducer` over the log writer; the listener bound before the HTTP line and drained on shutdown like `pgwire`'s.

**Tests:** `non_loopback_is_refused`; `partners_flag_requires_listen`; `feature_off_warns_on_flags`; transport: `http11_keepalive_reuses_connection`; `h2c_prior_knowledge_multiplexes_100_streams`; `graceful_shutdown_sends_goaway_and_finishes_in_flight`; `gzip_request_body_is_accepted`; `events_reach_the_stream_and_read_back_as_cloudevents` (through `operon dev`, consumed by §02 §7.4's `GET …/events`).

**Commit:** `rtb: serve the protocol gateway from operon behind the rtb feature`.

### Task 8: The 2.6 round-trip property, the latency gate and docs

**Files:** `crates/operon-conformance/tests/openrtb26.rs` (property), `.github/workflows/ci.yml` (job `latency`), `crates/operon-openrtb/benches/codec.rs`, `scripts/conformance/latency-gate.sh`, `docs/design/34-protocol-gateway-and-standards.md`, `CHANGELOG.md`.

**Semantics:**
- Proptest strategy over canonical `Auction`s valid at 2.6 (bounded sizes: ≤ 8 impressions, ≤ 4 formats, ≤ 4 deals, ext either absent or a small JSON object): canonical → 2.6 JSON → canonical is identity; and for the spec examples, wire → canonical → wire is JSON-value identity.
- The gate (`latency-gate.sh`): runs the benches' reference corpus 10 000 times per file through decode + canonicalize + bid (a no-op bidder) + encode, records an `hdrhistogram`, and fails if p99 exceeds **1 ms** (Q364's proposed budget). It runs on the CI runner on PRs that touch the codec crates and nightly; the result is posted as a job summary.

**Tests:** `wire26_canonical_wire26` (512 cases); `canonical_wire26_canonical` (512 cases); the `latency` job.

**Commit:** `rtb: add the 2.6 round-trip property and the latency gate`.

## PR sizes

| Task | Expected size |
|---|---|
| 1 | ~500 lines |
| 2 | ~1 800 lines (the model is long and flat) |
| 3 | ~1 200 lines |
| 4 | ~400 lines |
| 5 | ~600 lines plus corpus files |
| 6 | ~1 500 lines |
| 7 | ~600 lines |
| 8 | ~400 lines |

## Rulings made during execution

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| — | (Task 0 fills this table) | | |
