# GW4 — The Google and OpenRTB-Protobuf Adapters and the Cross-Protocol Differential Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (paths, content types, versions, job names), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: Planned** (2026-10-01). **Track GW** (design [§34](../design/34-protocol-gateway-and-standards.md)), build-order item 4. Depends on GW3 merged; gated on Q360. Branches `gw4-t<N>`, stacked; PRs target `main`. GW4 vendors three third-party protos, adds two adapters, and adds the nightly fuzzing, Connect-conformance and edge jobs; it changes GW1's canonical protos only by giving the reserved fields 52 and 53 their types.

**Goal:**
- **`OpenRtb26Proto`**: OpenRTB 2.6 in the IAB's standard protobuf encoding (`com.iabtechlab.openrtb.v2`, edition 2023) ↔ canonical (§34 §8.3).
- **`GoogleOpenRtb`**: Google's OpenRTB (`com.google.openrtb`, proto2) with the AdX extensions (`openrtb-adx.proto`), in protobuf and JSON ↔ canonical (D371).
- **The cross-protocol differential**: one logical auction through 2.6 JSON, 2.5 JSON, IAB protobuf and Google protobuf, over HTTP/1.1, h2c, Connect, gRPC and gRPC-Web, decodes to one canonical message (D377 row 4).
- **Nightly**: `cargo-fuzz` on every decoder (row 3), the official Connect conformance runner against the pinned connect-rust (row 6), and an Envoy edge test (HTTP/1.1 in, h2 to the gateway).

**Architecture:**
- **Vendored protos** under `proto/vendor/` (excluded from `buf lint` and `buf breaking`, never edited), compiled by a new crate `operon-openrtb-proto`, so the gateway crates compile them only when they use them.
- **The canonical model references Google's extension messages** (`google_ext`, field 52) instead of re-modelling them; `ext_proto` (field 53) keeps unknown-field bytes (§34 §6).
- **Doubles at the protobuf boundary.** The IAB and Google protos carry prices as `double`. A double is converted to `Money` through its shortest round-trip decimal string (the `ryu` algorithm in `std`'s `Display` for `f64`) and then GW1's decimal parser, so `1.23` stays exactly 1 230 000 micros. This is the only place an `f64` price exists, inside the adapter, and it is never stored (D374).

**Tech Stack:** as GW3. System `protoc` ≥ 27 (the IAB proto uses edition 2023; Task 0 checks CI's). `buffa` with proto2, editions and extension support (Task 0 checks the generated-code API for `extend` fields; fallback `buffa-descriptor`'s `DynamicMessage`). Nightly CI only: `cargo-fuzz` (MIT OR Apache-2.0) with a nightly toolchain pinned in the job; `connectconformance` from `connectrpc/conformance` (Apache-2.0, pinned release); Envoy v1.39.1 (Apache-2.0) as a container.

**Spec:**
- [§34](../design/34-protocol-gateway-and-standards.md) §6, §8.3, §13 rows 3, 4, 6, 8.
- `openrtb2.x/proto/src/main/com/iabtechlab/openrtb/v2/openrtb.proto` and `proto/README.md` (bools for integer flags; integers for enumerations; extension ranges, 1000–1999 Google).
- Google Authorized Buyers: `openrtb.proto` (OpenRTB 2.6, proto2), `openrtb-adx.proto` (v.205, 2026-03-13; beta v.213) from developers.google.com/authorized-buyers/rtb/downloads; the OpenRTB migration guide's field mappings.
- As built after GW3.

## Global Constraints

Same as GW2 and GW3, plus:
- **Vendored files are byte-identical to their source**, recorded in `corpus/SOURCES.toml` (`url`, `version`, `sha256`, `licence`, `fetched`) and in `NOTICE`. An update is its own PR.
- **Nightly jobs never run on the build machine.** Fuzzing, the Connect runner and the Envoy test run in CI only.
- **Commit areas:** `rtb`, `proto`, `conformance`, `ci`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **A separate crate `operon-openrtb-proto`** compiles `proto/vendor/iab/openrtb/v2/openrtb.proto`, `proto/vendor/google/openrtb/openrtb.proto` and `proto/vendor/google/openrtb/openrtb-adx.proto` with `connectrpc-build` (messages only, no services) | Keeps edition 2023 and proto2 out of `operon-rtb-proto`'s build, and lets `operon-rtb-proto` depend on it only for field 52's types | One more crate; `operon-rtb-proto` gains a dependency in Task 2 |
| 2 | **Integer flags ↔ bools:** IAB and Google protos carry some OpenRTB integer flags as `bool`. Decoding maps `true`/`false` to `1`/`0`; encoding maps `0` to `false`, `1` to `true`, and any other value to `true` with `Warning::CoercedValue` | The canonical model keeps integers (GW1 Ruling 6); protobuf cannot carry a `2` | A non-0/1 flag from a JSON partner is coerced when re-sent as protobuf; reported |
| 3 | **Enumerations are integers in both protos** (as the IAB README says), so they pass through unchanged | No mapping to maintain | None |
| 4 | **Google's content types:** protobuf requests and responses use `application/octet-stream`; JSON uses `application/json` (**verify** against Google's current RTB docs in Task 0); a partner record with `protocol = "google"` selects the Google adapter on `/rtb/v1/{partner}/bid`, and the answer uses the request's encoding | One route for every partner; the encoding follows the request | If Google uses another content type, only `detect` and one constant change |
| 5 | **`google_ext` is typed with Google's messages; unknown extensions and fields go to `ext_proto`** as raw bytes and are written back unchanged when re-encoding to the same protocol; they are dropped (reported in `LossReport`) when encoding to JSON or another protobuf schema | Lossless round trip within a protocol, honest loss across protocols | None |
| 6 | **The differential compares canonical messages after normalization**: `Provenance` cleared, warnings ignored, `ext` compared as JSON values, `ext_proto` and `google_ext` excluded (they are protocol-specific) | The claim is "the same logical auction", not the same bytes | A bug that only shows in `ext` is caught by the per-protocol goldens, not the differential |
| 7 | **The differential's generator is the intersection** of what all four encodings represent: no DOOH (2.5 lacks it), prices with at most six decimals whose shortest `f64` decimal equals their decimal text, flags 0 or 1, no 2.6-only fields that 2.5 drops | Otherwise the differential tests loss, which GW3's properties already do | Fields outside the intersection are covered by the per-version goldens and GW3's properties |

## Carried in

From GW1 Ruling 3: fields 52 (`google_ext`) and 53 (`ext_proto`) are kept free on every extensible message. From GW2/GW3: the adapters, the registry, the corpus harness, `mock_partner`, the latency gate.

## Review Focus

1. **Protobuf round trips are lossless within a protocol**, unknown fields included. Tests: Task 3 (`iab_proto_roundtrip_keeps_unknown_fields`), Task 4 (`google_roundtrip_keeps_unknown_extensions`).
2. **Doubles never reach state.** Tests: Task 3 (`double_price_is_shortest_decimal`, `no_f64_outside_proto_boundary`).
3. **The differential is identical across every protocol and transport.** Tests: Task 5.
4. **Fuzzing finds no panic.** Nightly job; every crash becomes a regression file (Task 6).

## File structure

```
proto/vendor/iab/openrtb/v2/openrtb.proto
proto/vendor/google/openrtb/{openrtb.proto,openrtb-adx.proto}
buf.yaml                                            # exclude proto/vendor from lint and breaking
NOTICE                                              # the three vendored protos
crates/operon-openrtb-proto/{Cargo.toml,build.rs,src/lib.rs}
proto/loam/rtb/v1/{request.proto,response.proto}    # fields 52 and 53 typed (Task 2)
crates/operon-openrtb/src/proto/{mod.rs,iab.rs,google.rs,double.rs,flags.rs}
crates/operon-openrtb/tests/{iab_proto.rs,google.rs}
crates/operon-conformance/corpus/{openrtb-proto/2.6,google}/{valid,invalid}/*.bin (+ .expected.json, + .txtpb sources)
crates/operon-conformance/tests/{iab_proto.rs,google.rs,differential.rs}
fuzz/{Cargo.toml,fuzz_targets/*.rs}                 # its own workspace, excluded from the main one
crates/operon-conformance/corpus/fuzz-regressions/<target>/*
deploy/conformance/envoy-edge.yaml
.github/workflows/nightly.yml                       # jobs fuzz, connect-conformance, edge
docs/design/34-protocol-gateway-and-standards.md  CHANGELOG.md
```

### Task 0: Reconcile and fetch the Google and IAB protos

**Files:** read GW3's crates as merged. Fill "Rulings made during execution".

**Checks:**
- Download the three protos; record version, SHA-256 and licence header of each (`openrtb-adx.proto`'s header is not yet checked: if it is not Apache-2.0 or compatible, stop and ask the owner).
- `protoc --version` in CI (≥ 27 for edition 2023); if older, the job installs a pinned `protoc` release.
- buffa's generated code for proto2 `extend` fields: are extensions accessible as typed getters on the extended message, or only through `buffa-descriptor`? Record the API, or Ruling 1's fallback (`DynamicMessage` for `openrtb-adx` extensions).
- Google's current RTB HTTP conventions: content types for protobuf and JSON, compression, whether Google sends `x-openrtb-version`, and the default `tmax` (Ruling 4).
- The `connectconformance` release that matches connect-rust 0.9.x's own CI, and how connect-rust's repository runs its conformance server (the `conformance/` directory and its config).

**Commit:** `docs: reconcile GW4 and record the vendored protos`.

### Task 1: Vendor the protos and `operon-openrtb-proto`

**Files:** `proto/vendor/**`, `buf.yaml`, `NOTICE`, `crates/operon-conformance/corpus/SOURCES.toml`, `crates/operon-openrtb-proto/{Cargo.toml,build.rs,src/lib.rs}`, workspace `Cargo.toml`.

**Produces:** `operon_openrtb_proto::{iab::v2, google::openrtb, google::adx}` (module names per the generated packages), `publish = false`.

**Tests:** `vendored_files_match_recorded_hashes` (reads `SOURCES.toml`, hashes the files); `iab_and_google_messages_decode_empty`; `buf lint` and `buf breaking` still clean (vendor excluded).

**Commit:** `proto: vendor the IAB and Google OpenRTB protos`.

### Task 2: Type the canonical fields 52 and 53

**Files:** `proto/loam/rtb/v1/{request.proto,response.proto}`, `crates/operon-rtb-proto/{Cargo.toml,build.rs}`, `crates/operon-rtb-proto/tests/proto.rs`.

**Semantics:** on each canonical message whose OpenRTB object Google extends in `openrtb-adx.proto`, field 52 becomes `<Google's extension message> google_ext = 52;` (for example `com.google.doubleclick.ImpExt google_ext = 52;` on `Impression`; the exact list is the `extend` blocks in the vendored file). Field 53 becomes `bytes ext_proto = 53;` on every extensible message. GW1 kept both numbers free by a test, not by `reserved` (which `buf breaking` would refuse to delete), so adding them is an additive change; GW1's `field_numbers_follow_ruling_3` is updated to allow exactly these two.

**Tests:** `google_ext_types_match_adx_extends` (every `extend` block in the vendored adx proto has a `google_ext` on the matching canonical message); `ext_proto_on_every_extensible_message`.

**Commit:** `proto: type google_ext and ext_proto on the canonical model`.

### Task 3: The `OpenRtb26Proto` adapter (IAB protobuf)

**Files:** `crates/operon-openrtb/src/proto/{mod.rs,iab.rs,double.rs,flags.rs}`, `crates/operon-openrtb/tests/iab_proto.rs`, `crates/operon-conformance/corpus/openrtb-proto/2.6/**`, `crates/operon-conformance/tests/iab_proto.rs`.

**Produces:** `pub struct IabProtoAdapter;` with `ProtocolId::OpenRtbProto(2.6)`; `double::money_from_f64(v: f64, currency: &str) -> Result<Money, MoneyError>` (refuses NaN, infinities, negatives) and `money_to_f64(&Money) -> f64`; `flags::{to_bool, from_bool}` per Ruling 2.

**Semantics:** decode = buffa decode of `com.iabtechlab.openrtb.v2.BidRequest` → map each field to canonical (names match by construction, GW1 Ruling 3) → unknown fields of each object into `ext_proto` → `Provenance { protocol: "openrtb-proto", version: "2.6" }`. Encode reverses, writing `ext_proto` back. `detect` is `Certain` for `application/x-protobuf` or `application/protobuf` with `x-openrtb-version: 2.6`. Corpus: `.txtpb` sources (text format, readable in review) compiled to `.bin` by a test helper (`UPDATE_GOLDEN=1`), with `.expected.json`.

**Tests:** `iab_proto_golden_valid`, `iab_proto_golden_invalid`; `iab_proto_roundtrip_keeps_unknown_fields`; `double_price_is_shortest_decimal` (`1.23` → 1 230 000; `0.1 + 0.2` as a double → 300 000 with `PriceRounded`); `flags_map_to_bool_and_back`; `no_f64_outside_proto_boundary` (grep test: `f64` appears in price paths only under `src/proto/`); `iab_proto_equals_json_on_spec_examples` (each 2.6 JSON spec example, re-encoded as IAB protobuf and decoded, gives the same canonical message under Ruling 6's normalization).

**Commit:** `rtb: add OpenRTB 2.6 in the IAB protobuf encoding`.

### Task 4: The Google adapter

**Files:** `crates/operon-openrtb/src/proto/google.rs`, `crates/operon-openrtb/tests/google.rs`, `crates/operon-conformance/corpus/google/**`, `crates/operon-conformance/tests/google.rs`, `crates/operon-rtb-gateway/src/router.rs` (register; partner `protocol = "google"`).

**Produces:** `pub struct GoogleAdapter;` with `ProtocolId::GoogleOpenRtb`; both encodings: protobuf (`com.google.openrtb.BidRequest` with AdX extensions) and JSON (GW2's 2.6 profile, with the `ext` members Google defines parsed into `google_ext` and removed from canonical `ext`).

**Semantics:** decode maps Google's proto like Task 3 and its AdX extensions into `google_ext` (typed); other extensions and unknown fields into `ext_proto`. JSON decode parses `ext` once, moves Google's known members into `google_ext` and keeps the rest in `ext`. Encode writes the request's encoding. `Provenance { protocol: "google-openrtb", version: "2.6" }`. Corpus: hand-written `.txtpb` and JSON requests covering banner, video, native, app with `ImpExt` billing ids and `BidRequestExt` fields, plus responses with `BidExt`.

**Tests:** `google_golden_proto_valid`, `google_golden_json_valid`, `google_golden_invalid`; `google_roundtrip_keeps_unknown_extensions`; `google_json_and_proto_agree` (the same request in both encodings gives the same canonical message, `google_ext` included); `google_response_in_request_encoding`; the gateway route test `google_partner_route_serves_both_encodings`.

**Commit:** `rtb: add the Google OpenRTB adapter with AdX extensions`.

### Task 5: The cross-protocol differential

**Files:** `crates/operon-conformance/tests/differential.rs`, `crates/operon-conformance/src/{differential.rs,normalize.rs,transports.rs}`.

**Produces:** `differential::generate(seed) -> Auction` (Ruling 7's intersection); `normalize(Auction) -> Auction` (Ruling 6); `transports::{Http11Json, H2cJson, ConnectJson, ConnectProto, Grpc, GrpcWeb}` clients against an in-process gateway (`operon-rtb-gateway::router` on `127.0.0.1:0`), each implementing `async fn roundtrip(&self, encoded: WireMessage) -> Auction` through a `RecordingBidder` that captures the canonical auction the gateway decoded.

**Semantics:** for each generated auction: encode as 2.6 JSON, 2.5 JSON, IAB protobuf and Google protobuf (and Google JSON); send each over every transport that carries it (the four exchange encodings over HTTP/1.1 and h2c on `/rtb/v1/{partner}/bid`; the canonical message over Connect JSON, Connect protobuf, gRPC and gRPC-Web on `AuctionService/Bid`); collect the recorded canonical auctions; all must equal `normalize(original)`. Also checks that the bid each bidder returns encodes back to each protocol and decodes to the same canonical response.

**Tests:** `differential_same_canonical_everywhere` (proptest, 256 cases per PR, 10 000 nightly by `PROPTEST_CASES`); `differential_responses_agree`; `differential_reports_first_divergence` (a failure names the protocol, transport and first differing field path).

**Commit:** `conformance: add the cross-protocol, cross-transport differential`.

### Task 6: Fuzzing, the Connect runner and the edge test (nightly)

**Files:** `fuzz/{Cargo.toml,fuzz_targets/{openrtb_json_26.rs,openrtb_json_25.rs,openrtb_json_24.rs,openrtb_proto_iab.rs,google_proto.rs,google_json.rs,cloudevents_json.rs,cloudevents_proto.rs}}`, `crates/operon-conformance/corpus/fuzz-regressions/**`, `crates/operon-conformance/tests/fuzz_regressions.rs`, `deploy/conformance/envoy-edge.yaml`, `.github/workflows/nightly.yml`.

**Semantics:**
- Each fuzz target feeds arbitrary bytes to one decoder (and, when it decodes, re-encodes and decodes again, asserting equality). Seeds: the corpus files. The nightly `fuzz` job runs each target for 10 minutes (`-max_total_time=600`), uploads crashes as artifacts and fails the job; a person turns each crash into a file under `fuzz-regressions/<target>/`, which `fuzz_regressions.rs` replays on every PR.
- `connect-conformance`: runs the pinned `connectconformance` against connect-rust's own conformance server at the workspace's `connectrpc` version (the server is built from the connect-rust repository at the matching tag in the job). It guards the pinned dependency, not Loam's handlers.
- `edge`: starts Envoy v1.39.1 with `envoy-edge.yaml` (HTTP/1.1 listener → h2 upstream to the gateway, request gzip passthrough) and the gateway, and replays the 2.6 corpus through Envoy; every answer must match the direct answer.

**Tests:** `fuzz_regressions_replay` (every PR); the three nightly jobs.

**Commit:** `ci: add nightly fuzzing, the Connect conformance runner and the Envoy edge test`.

### Task 7: The latency gate for protobuf, and close

**Files:** `crates/operon-openrtb/benches/codec.rs` (IAB and Google protobuf benches), `scripts/conformance/latency-gate.sh` (adds the protobuf corpora), `docs/design/34-protocol-gateway-and-standards.md` (§8.3 as built; §13 rows 3, 4, 6), `CHANGELOG.md`.

**Tests:** the `latency` job with the protobuf corpora under the same 1 ms p99 budget.

**Commit:** `docs: record GW4 as built`.

## PR sizes

| Task | Expected size |
|---|---|
| 1 | vendored files plus ~200 lines |
| 2 | ~150 lines |
| 3 | ~1 200 lines plus corpus |
| 4 | ~1 200 lines plus corpus |
| 5 | ~900 lines |
| 6 | ~700 lines plus YAML |
| 7 | ~200 lines |

## Rulings made during execution

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| — | (Task 0 fills this table) | | |
