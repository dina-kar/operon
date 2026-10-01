# GW1 — The Proto Contract, the CloudEvents Profile and the Event Arrow Schema Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, field numbers, URNs, metric names), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: Planned** (2026-10-01). **Track GW** (design [§34](../design/34-protocol-gateway-and-standards.md), D360–D379), build-order item 1. Branches `gw1-t<N>`, stacked; PRs target `main`. GW1 is useful whether or not the owner takes Loam into ad-tech (Q360): the `buf breaking` gate, the CloudEvents profile and the event-to-Arrow mapping serve every event producer. Tasks 2 and 3's `loam.rtb.v1` protos are the only ad-tech part; if Q360 is answered "no" before Task 2 starts, Tasks 2–3 are skipped and Task 5 uses `loam.meter.v1.Invocation` (RN1 Task 1) as its reference message instead. GW1 adds crates and a CI gate; it changes no existing code path except `operon-cloudevents` (a new module) and `buf.yaml`.

**Goal:**
- `buf breaking` enforced in CI for `loam.stream.v1` and the new packages (D363).
- `loam.rtb.v1`, the canonical auction model, and its event messages, compiled by connect-rust into `operon-rtb-proto` (D368).
- `loam.events.v1`, a custom message option that names an event's CloudEvents type (D364, D372).
- `operon_cloudevents::profile`: the Loam CloudEvents profile (D364) and the high-rate record layout helper (D365).
- `operon-events-arrow`: protobuf descriptor → Arrow schema, CloudEvents ⇄ `RecordBatch`, Parquet and IPC round trips, and the additive-evolution check (D372).

**Architecture:**
- Protos live under `proto/` at the workspace root (one `buf` module). `operon-stream-grpc`'s proto stays where it is and becomes a second `buf` module (Ruling 2).
- `operon-rtb-proto` follows `operon-live-proto`'s pattern exactly (D128): `build.rs` runs `connectrpc-build` with the system `protoc`, the client behind a `client` feature, `publish = false`.
- `operon-events-arrow` reads message descriptors at run time from an embedded `FileDescriptorSet` through `buffa-descriptor`'s `DescriptorPool` (Ruling 4), so one mapping function serves every event type, including RN1's usage events.
- Nothing here is linked into the `operon` binary by default; the crates are libraries that GW2 and RN1 use.

**Tech Stack:** Rust 1.97.1, edition 2024, workspace lints. Workspace crates reused: `connectrpc` 0.9.1, `connectrpc-build` 0.9, `buffa` 0.9.2, `arrow`/`arrow-array`/`arrow-schema` 58.4, `parquet` 58 (workspace features), `bytes`, `serde`, `serde_json`, `proptest` 1, `thiserror`, `rand` 0.9. New (Task 0 checks version, licence and `cargo deny`): `buffa-descriptor` (Apache-2.0, the `reflect` feature line matching `buffa` 0.9.2). Tools: `buf` (the version `sdks/live-typescript` pins), system `protoc` ≥ 27 (editions; CI's `protobuf-compiler` package is checked in Task 0).

**Spec:**
- [§34](../design/34-protocol-gateway-and-standards.md): §3 (charter), §4 (the narrow waist and the profile), §6 (canonical model), §9 (events and Arrow), §10 (state rules), §15 rows 8–13.
- [§02 §7.4](../design/02-stream-engine.md) (D270): the record mapping, validation and the consume path's rebuild.
- [§08 §2](../design/08-analytics.md): Arrow → Iceberg types (ns needs v3).
- As built: `crates/operon-cloudevents` (`CloudEvent`, `json`, `http`, `record`, `partition`), `crates/operon-live-proto/{Cargo.toml,build.rs}`, `buf.yaml`, `.github/workflows/ci.yml` (the `protos` path filter), and PR #171's `crates/operon-stream-grpc/proto/` if merged.

## Global Constraints

Same as the M1 overview §8, plus:
- **The build machine.** One cargo build at a time, the shared target, `-j 6`, lld. GW1's crates build without DataFusion; never add `operon-query` or `datafusion` to them. Stop and report if `/home` has under 8 GB free.
- **Compatible-only protos (D363).** After Task 1 merges, a change to `proto/loam/{stream,rtb,events,meter}/` that `buf breaking` rejects needs a new package version (`v2`), never a waiver.
- **Field numbers are permanent.** Removed fields are `reserved` (number and name), never reused.
- **No `f64` money (D374).** No price, floor or amount field in any new message is `float` or `double`.
- **Commit areas:** `proto`, `events`, `ci`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **`buf breaking` uses the `FILE` rule set against `.git#branch=origin/main`**, on the `protos` path filter; `loam.live.v1` is excluded (`breaking.ignore`) until R2 freezes it | `FILE` is the strictest set and the one `buf.yaml` already names; Live is declared unstable (R1) | A legitimate move of a message between files is refused; it needs a new version, which is acceptable for public contracts |
| 2 | **`crates/operon-stream-grpc/proto` becomes a second `buf` module**, with `io/cloudevents/v1/` excluded from lint (it is vendored unchanged) but included in breaking checks | Moving the file would change `build.rs` paths in an in-flight PR (#171) for no wire benefit | Two modules to keep in `buf.yaml`; trivial |
| 3 | **Canonical field numbering:** within each message, fields take numbers 1–49 in the order of the OpenRTB 2.6 object table (§3.2.x of the spec); `ext = 50` (raw JSON bytes), `legacy = 51`, `google_ext = 52` (left unused in GW1; GW4 adds it with Google's own AdX extension message type), `ext_proto = 53` (left unused in GW1; GW4 adds it: the unknown-field bytes of a protobuf-origin object, for a lossless protobuf round trip), 54–59 kept free for other organisations' typed extensions, `provenance = 60` on top-level messages; 61+ for later additions. Names are the IAB `openrtb.proto` names | One rule a reviewer can check against the spec; room for typed extensions without renumbering | Spec order changes in later 2.6 releases do not renumber anything: new fields take the next free number |
| 4 | **Descriptors at run time through `buffa-descriptor`**, from a `FileDescriptorSet` that `build.rs` writes (`protoc --descriptor_set_out --include_imports`) and the crate embeds with `include_bytes!` | One generic Arrow mapping instead of generated code per message; RN1's events reuse it | If `buffa-descriptor` is missing or too young (Task 0), decode the set with `prost-types` (in the workspace) and walk `DescriptorProto` directly; `DynamicMessage` decode then comes from `prost-reflect` (MIT OR Apache-2.0) — Task 0 records which |
| 5 | **Prices are `Money { int64 micros; string currency }`**, the currency folded in: `imp.bidfloor` + `imp.bidfloorcur` → `Money bidfloor`; `deal.bidfloor` + `deal.bidfloorcur` → `Money bidfloor`; `durfloors[].bidfloor` → `Money bidfloor` in the impression's currency; `bid.price` → `Money price` (currency from the response's `cur`, default `USD` per the spec) | D368, D374 | A partner whose floors are in several currencies per request still fits (per-object currency) |
| 6 | **AdCOM lists are `int32`**, never proto `enum`; flags that OpenRTB sends as `0/1` integers stay `int32` (not `bool`), so a value of `2` from a non-conforming partner survives the round trip | Monthly 2.6 releases add list values; Loam's adapters must be lossless where partners misbehave | Typed constants live in `operon_rtb_proto::adcom`, so code is still readable |
| 7 | **`time` in Arrow is `Timestamp(Nanosecond, Some("UTC"))`**; Parquet writes it as `TIMESTAMP(NANOS, isAdjustedToUTC=true)`. The Iceberg mapping (v3 or the `time_ns` column, Q368) is GW5's | D372; Parquet supports ns today | None for GW1 |
| 8 | **The event type of a message is declared in the proto** by `option (loam.events.v1.event) = { type: "dev.loam.rtb.auction.v1" };`, extension number **50360** on `google.protobuf.MessageOptions` (the 50000–99999 in-house range) | The type and the schema then cannot drift apart; `dataschema` is derived from the message's full name | If `connectrpc-build` or `buffa` cannot carry custom options, a `const EVENT_TYPE: &str` per message in hand-written code, checked by a test against a table |
| 9 | **The type prefix is `dev.loam.`** (§02 §7.4 as built), held in one constant `operon_cloudevents::profile::TYPE_PREFIX`. If the owner answers Q361 with `dev.loams.`, the constant, §02's synthesized type and the options change in one PR before any external producer exists | Consistency with what is built | One renaming PR |

## Carried in

From §34: D362–D365, D368, D372, D374. From D270 (PRs #170, #171): `operon-cloudevents`'s `CloudEvent` keeps attributes as exact strings in arrival order with extension types; `record` lays an event out as a Kafka binary-mode record. If #170/#171 have not merged by Task 3, Task 3 rebases on whichever has.

## Review Focus

1. **The gate catches breaking changes.** Tests: Task 1 (`scripts/proto/breaking-selftest.sh` must fail on each of its five canned breaking patches and pass on its additive one).
2. **No float money, no reused numbers.** Tests: Task 2 (`no_float_money_fields`, `field_numbers_follow_ruling_3`).
3. **The profile cannot be bypassed.** A client `tenantid` never survives `stamp_tenant`. Tests: Task 3 (`client_tenantid_is_overwritten`, `missing_traceparent_is_generated_once`).
4. **Lossless Arrow round trip.** CloudEvents → `RecordBatch` → Parquet → `RecordBatch` → CloudEvents is byte-identical on attributes and `data`. Tests: Task 6 (`parquet_roundtrip_is_lossless`, proptest `arrow_roundtrip_any_event`).
5. **Additive evolution only.** Tests: Task 6 (`check_additive_*`).

## File structure

```
buf.yaml                                          # second module; breaking ignore for loam/live (Task 1)
.github/workflows/ci.yml                          # job `protos`: + buf breaking (Task 1)
scripts/proto/{breaking-selftest.sh,patches/*.patch}
proto/loam/events/v1/options.proto                # Task 3
proto/loam/rtb/v1/{common.proto,request.proto,response.proto,service.proto,events.proto}
crates/operon-rtb-proto/                          # new (Task 2)
  Cargo.toml  build.rs
  src/{lib.rs,adcom.rs,money.rs,descriptor.rs}
  tests/{proto.rs,money.rs,descriptor.rs}
crates/operon-cloudevents/src/profile.rs          # new module (Task 3)
crates/operon-cloudevents/src/proto_data.rs       # new module (Task 4)
crates/operon-cloudevents/tests/{profile.rs,high_rate.rs}
crates/operon-events-arrow/                       # new (Tasks 5–6)
  Cargo.toml
  src/{lib.rs,schema.rs,builder.rs,reader.rs,evolution.rs,error.rs}
  tests/{schema.rs,roundtrip.rs,evolution.rs,golden/*.schema.json}
docs/design/34-protocol-gateway-and-standards.md  CHANGELOG.md
```

### Task 0: Reconcile and check the toolchain

**Files:** read `crates/operon-cloudevents/src/*.rs`, `crates/operon-live-proto/{Cargo.toml,build.rs}`, `crates/operon-stream-grpc/{build.rs,proto/}`, `buf.yaml`, `.github/workflows/ci.yml` as on `main`. Fill this plan's "Rulings made during execution".

**Checks** (record each with its command and output summary in the table):
- Whether PRs #170 and #171 have merged, and the as-built public API of `operon-cloudevents` (`CloudEvent`'s accessors and constructors, `record::{to_record, from_record}` or their actual names). Every later task uses the as-built names.
- `buffa-descriptor`: does a release matching `buffa` 0.9.2 exist; does it give `DescriptorPool::decode(&[u8])`, `MessageDescriptor` with fields, `DynamicMessage::decode`, and custom-option access; licence; `cargo deny check` with it. If not, Ruling 4's fallback, recorded.
- `connectrpc-build` 0.9: can one `Config` compile two packages where one imports the other (`loam/rtb/v1/events.proto` imports `loam/events/v1/options.proto` and `google/protobuf/descriptor.proto`)? Does it emit a descriptor set, or must `build.rs` call `protoc --descriptor_set_out` itself?
- `protoc --version` on the build machine and in CI; whether CI's `protobuf-compiler` package is ≥ 27 (needed only if a vendored proto uses editions; GW4's IAB proto does).
- `buf --version` (the pinned one) and whether `buf breaking --against '.git#branch=origin/main'` works in CI's shallow checkout (`fetch-depth: 0` may be needed for the job).
- Q360 and Q361's status.

**Commit:** `docs: reconcile GW1 with main`.

### Task 1: The breaking-change gate (D363)

**Files:** `buf.yaml`, `.github/workflows/ci.yml` (job `protos`), `scripts/proto/breaking-selftest.sh`, `scripts/proto/patches/{01-renumber.patch,02-retype.patch,03-remove-field.patch,04-rename-package.patch,05-change-rpc.patch,06-additive.patch}`.

**Produces:**
- `buf.yaml` v2 with `modules: [{path: proto}, {path: crates/operon-stream-grpc/proto, excludes: [], lint: {ignore: [crates/operon-stream-grpc/proto/io/cloudevents]}}]` (exact syntax per the pinned `buf`), `breaking: {use: [FILE], ignore: [proto/loam/live]}`.
- The `protos` CI job gains `buf breaking --against '.git#branch=origin/main'` after `buf lint`, on PRs only, with the checkout depth it needs.
- `breaking-selftest.sh`: copies the repo's protos to a scratch directory under the target dir (never `/tmp`), applies each patch to the copy, runs `buf breaking` against the unpatched copy, and exits non-zero unless patches 01–05 fail and 06 passes.

**Tests:** `scripts/proto/breaking-selftest.sh` in the `protos` job; `buf lint` clean.

**Commit:** `ci: enforce buf breaking on the stream and new protobuf packages`.

### Task 2: `loam.rtb.v1`, the canonical model (D368)

**Files:** `proto/loam/rtb/v1/{common.proto,request.proto,response.proto,service.proto}`, `crates/operon-rtb-proto/{Cargo.toml,build.rs,src/lib.rs,src/adcom.rs,src/money.rs,src/descriptor.rs,tests/proto.rs,tests/money.rs,tests/descriptor.rs}`, workspace `Cargo.toml` (member, `operon-rtb-proto` in `[workspace.dependencies]`).

**Produces:**
- `common.proto`: `Money`, `Provenance { string protocol = 1; string version = 2; string partner = 3; int64 received_unix_ns = 4; }`, and the `Legacy*` messages.
- `request.proto`: `Auction` (BidRequest) and one message per OpenRTB 2.6-202606 request object: `Source`, `SupplyChain`, `SupplyChainNode`, `Regs`, `Impression` (Imp), `Metric`, `Banner`, `Video`, `Audio`, `Native`, `Format`, `Pmp`, `Deal`, `Site`, `App`, `Dooh`, `Publisher`, `Content`, `Producer`, `Network`, `Channel`, `Device`, `UserAgent`, `BrandVersion`, `Geo`, `User`, `Eid`, `Uid`, `Data`, `Segment`, `Qty`, `Refresh`, `RefSettings`, `DurFloors` (the 35 request objects of 2.6-202606 §3.2.1–§3.2.35; Task 0 re-checks the list against the pinned release). Each has `bytes ext = 50`. `Legacy` messages on `Banner` (`wmax`, `hmax`, `wmin`, `hmin`), `Video` (`protocol`, `sequence`, `placement`), `Audio` (`sequence`), `Content` (`videoquality`), `Device` (`didsha1`, `didmd5`, `dpidsha1`, `dpidmd5`, `macsha1`, `macmd5`), `User` (`yob`, `gender`).
- `response.proto`: `AuctionResponse` (BidResponse: `id`, `seatbid`, `bidid`, `cur`, `customdata`, `nbr`), `SeatBid`, `Bid` (with `Money price`, `apis`, `api` in `Legacy`, `mtype`, `dur`, `slotinpod`, `cattax`, `cat`, `burl`, `lurl`, `nurl`, `adm`, …), `NoBidReason` as `int32 nbr`.
- `service.proto`: `service AuctionService { rpc Bid(BidRequest) returns (BidResponse); }` where `BidRequest { Auction auction = 1; string partner = 2; }` and `BidResponse { oneof result { AuctionResponse response = 1; int32 nbr = 2; } }` (names per `buf lint`'s `RPC_REQUEST_STANDARD_NAME`).
- `operon_rtb_proto`: the generated modules re-exported as `loam::rtb::v1`; `adcom` constants (`pub mod no_bid_reason { pub const INVALID_REQUEST: i32 = 2; … }`, plus the lists the adapters need: device type, connection type, API frameworks, placement subtypes, creative attributes, auction type, delivery methods), each list with a doc link to its AdCOM 1.0 section; `money::{Money::from_decimal_str(&str, &str) -> Result<Money, MoneyError>, Money::to_decimal_string(&self) -> String}` (half-to-even beyond six decimals, `MoneyError::{Empty, NotDecimal, Overflow, BadCurrency}`; currency is three uppercase ASCII letters); `descriptor::FILE_DESCRIPTOR_SET: &[u8]`.

**Semantics:** Ruling 3 for numbering; Ruling 5 for prices; Ruling 6 for lists and flags. Every comment on a field names the OpenRTB attribute and section it comes from (`// OpenRTB 2.6 §3.2.4 imp.bidfloor + imp.bidfloorcur`).

**Tests:** `roundtrip_binary_and_json` (a fully populated `Auction` and `AuctionResponse` through buffa binary and proto3 JSON); `no_float_money_fields` (walks the descriptor set: no `float`/`double` field whose name contains `price`, `floor`, `bid`, `cpm` or `amount`); `field_numbers_follow_ruling_3` (every message's `ext` is 50, `legacy` 51, `provenance` 60 where present, no field in 52–59). These numbers are kept free by this test, **not** by `reserved` statements: `buf breaking`'s `FILE` rules refuse deleting a reserved range, so a `reserved 52;` could never be turned into a field; `every_openrtb26_object_has_a_message` (a checked-in list of the 2.6-202606 objects, from Task 0, each mapped to its message); `money_parse_cases` (`"1.5"` → 1 500 000; `"0.0000005"` → 0 with half-even; `"0.0000015"` → 2; `"1e2"` → 100 000 000 and `"1.5E-3"` → 1 500 (exponent forms of the JSON number grammar, RFC 8259, are parsed exactly as decimal mantissa and exponent, never through `f64`); `"1e400"` refused as `Overflow`; `"0x10"` and `"1."` refused as `NotDecimal`; `"-0.5"` refused unless negative amounts are allowed, which they are not for prices; 19-digit overflow refused); `money_display_is_shortest` (1 500 000 → `"1.5"`).

**Commit:** `proto: add loam.rtb.v1, the canonical auction model`.

### Task 3: `loam.events.v1` and the CloudEvents profile (D364)

**Files:** `proto/loam/events/v1/options.proto`, `proto/loam/rtb/v1/events.proto`, `crates/operon-rtb-proto/build.rs` (both packages), `crates/operon-cloudevents/src/{lib.rs,profile.rs}`, `crates/operon-cloudevents/tests/profile.rs`.

**Produces:**

```protobuf
// proto/loam/events/v1/options.proto
syntax = "proto3";
package loam.events.v1;
import "google/protobuf/descriptor.proto";
message EventOption { string type = 1; }            // the CloudEvents `type`, e.g. "dev.loam.rtb.auction.v1"
extend google.protobuf.MessageOptions { EventOption event = 50360; }
```

```protobuf
// proto/loam/rtb/v1/events.proto (each message carries option (loam.events.v1.event))
message AuctionEvent {      // dev.loam.rtb.auction.v1
  Auction auction = 1; AuctionResponse response = 2; Outcome outcome = 3;
  int64 decode_ns = 4; int64 bid_ns = 5; int64 encode_ns = 6; string bidder = 7; string adapter = 8;
}
enum Outcome { OUTCOME_UNSPECIFIED = 0; OUTCOME_BID = 1; OUTCOME_NO_BID = 2; OUTCOME_TIMEOUT = 3; OUTCOME_DECODE_ERROR = 4; }
message WinEvent { string auction_id = 1; string imp_id = 2; string bid_id = 3; Money clearing_price = 4; int64 at_unix_ns = 5; }      // dev.loam.rtb.win.v1
message LossEvent { string auction_id = 1; string imp_id = 2; string bid_id = 3; int32 loss_reason = 4; Money min_to_win = 5; int64 at_unix_ns = 6; } // dev.loam.rtb.loss.v1
message BillingEvent { string auction_id = 1; string imp_id = 2; string bid_id = 3; Money price = 4; int64 at_unix_ns = 5; }        // dev.loam.rtb.billing.v1
```

(`Outcome` is a proto `enum` because it is Loam's own closed set, not an AdCOM list.)

```rust
// crates/operon-cloudevents/src/profile.rs
pub const TYPE_PREFIX: &str = "dev.loam.";
pub const DATASCHEMA_PREFIX: &str = "urn:loam:proto:";
pub const REQUIRED_EXTENSIONS: [&str; 2] = ["tenantid", "traceparent"];
#[derive(Clone, Debug, PartialEq, Eq)] pub struct Tenant { org: String, namespace: String }    // private fields; Display: "<org>/<namespace>"
impl Tenant { pub fn new(org: &str, namespace: &str) -> Result<Self, ProfileError>; pub fn parse(s: &str) -> Result<Self, ProfileError>; pub fn org(&self) -> &str; pub fn namespace(&self) -> &str; }
#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub struct TraceParent { trace_id: [u8; 16], parent_id: [u8; 8], flags: u8 } // private fields
impl TraceParent { pub fn parse(s: &str) -> Result<Self, ProfileError>; pub fn generate(rng: &mut impl rand::RngCore) -> Self; pub fn trace_id(&self) -> [u8; 16]; pub fn parent_id(&self) -> [u8; 8]; pub fn flags(&self) -> u8; }
pub enum Stamp { Set, Overwritten { previous: String } }
pub fn stamp_tenant(ev: &mut CloudEvent, tenant: &Tenant) -> Stamp;
pub fn ensure_traceparent(ev: &mut CloudEvent, rng: &mut impl rand::RngCore) -> bool; // true if generated
pub fn validate(ev: &CloudEvent) -> Result<(), ProfileError>;       // D270's rules plus the profile
pub fn validate_loam_type(ty: &str) -> Result<(), ProfileError>;     // ^dev\.loam\.[a-z0-9]+(\.[a-z0-9_]+)+\.v[1-9][0-9]*$
pub fn dataschema_for(message_full_name: &str) -> String;            // "urn:loam:proto:" + name
#[derive(Debug, thiserror::Error)] pub enum ProfileError { MissingExtension(&'static str), BadTraceParent(String), BadTenant(String), BadType(String), DataschemaMismatch { expected: String, got: String } }
```

**Semantics:** `validate` requires `tenantid` (`<org>/<namespace>`, both non-empty, no further `/`) and a valid `traceparent` (W3C Trace Context level 1: version `00`, 32 lowercase hex trace id not all zero, 16 hex parent id not all zero, 2 hex flags; versions above `00` accepted if the first four fields parse, as the spec asks). For a `type` under `TYPE_PREFIX`, the suffix rule applies and `dataschema` must be present and start with `DATASCHEMA_PREFIX`. `stamp_tenant` replaces any value and reports it; the caller counts `loam_events_tenantid_overwritten_total`. Attribute order is preserved (D270): a stamped or generated attribute is appended at the end if new, replaced in place if present.

**Tests:** `client_tenantid_is_overwritten`; `tenantid_is_appended_when_absent`; `missing_traceparent_is_generated_once` (a second call keeps the first); `traceparent_vectors` (the W3C spec's valid and invalid examples, checked in as a table); `loam_type_names` (accepts `dev.loam.rtb.auction.v1`, refuses `io.loam.x.v1`, `dev.loam.rtb.auction`, `dev.loam.rtb.auction.v0`); `dataschema_required_for_loam_types`; `profile_keeps_attribute_order`; `every_event_message_declares_its_type` (in `operon-rtb-proto`: each message in `events.proto` has the option, and its type matches the table in this task).

**Commit:** `events: add the Loam CloudEvents profile and the event type option`.

### Task 4: Events from messages, and the high-rate record layout (D365)

**Files:** `crates/operon-cloudevents/src/proto_data.rs`, `crates/operon-cloudevents/tests/high_rate.rs`, `crates/operon-cloudevents/Cargo.toml` (dev-dependency on `operon-rtb-proto`).

**Produces:**

```rust
pub struct EventMeta { pub id: String, pub source: String, pub time_unix_ns: i64, pub subject: Option<String>, pub partition_key: Option<String> }
/// Builds a profile-valid event whose data is `data` in protobuf, datacontenttype "application/protobuf".
pub fn event_from_proto(meta: EventMeta, event_type: &str, message_full_name: &str, data: Bytes, tenant: &Tenant, trace: &TraceParent) -> Result<CloudEvent, ProfileError>;
/// Formats `time` as RFC 3339 UTC with nine fractional digits.
pub fn rfc3339_nanos(unix_ns: i64) -> String;
```

**Semantics:** `Tenant` and `TraceParent` have private fields and only the validating constructors of Task 3 (`Tenant::new`/`parse`, `TraceParent::parse`/`generate`), so a value of either type is valid by construction and `event_from_proto` takes no unvalidated tenant or trace input. The remaining inputs are checked: `event_type` by `validate_loam_type`, `message_full_name` as a dotted protobuf name, `meta.id` and `meta.source` non-empty; a failure returns `Err(ProfileError)`, so the signature is `-> Result<CloudEvent, ProfileError>`. The record of an event built here is the as-built `record` layout (D270): `ce_` headers in attribute order, `content-type: application/protobuf`, the key from `partitionkey`, the value the protobuf bytes. This module adds no new layout; it exists so producers never hand-assemble attributes. `id` uniqueness is the producer's: GW2's gateway uses `<auction id>:<node id>:<sequence>`.

**Tests:** `bad_event_type_is_refused`; `empty_id_or_source_is_refused`; `tenant_new_refuses_slash_and_empty`; `high_rate_record_reads_back_as_cloudevent` (event → record → the as-built consume rebuild → an equal event, attribute order included); `time_has_nanoseconds`; `partition_key_becomes_record_key`; `data_is_protobuf_bytes_unchanged`.

**Commit:** `events: build profile events from protobuf messages`.

### Task 5: Descriptor → Arrow schema (D372)

**Files:** `crates/operon-events-arrow/{Cargo.toml,src/lib.rs,src/schema.rs,src/error.rs,tests/schema.rs,tests/golden/auction_event.schema.json,tests/golden/win_event.schema.json}`.

**Produces:**

```rust
pub const META_PROTO_PATH: &str = "loam.proto.path";      // Arrow field metadata: "auction.imp.banner.w"
pub const META_PROTO_TAG: &str = "loam.proto.tag";        // "1"
pub struct EventSchema { pub event_type: String, pub message: String, pub arrow: SchemaRef, pub raw: bool }
pub fn event_schema(pool: &DescriptorPool, message_full_name: &str, opts: &SchemaOptions) -> Result<EventSchema, ArrowMapError>;
pub struct SchemaOptions { pub raw: bool /* default true */, pub max_depth: usize /* default 16 */ }
```

**Semantics:** the column order and types of §34 §9's table (including `ext_types`, the extension-type map that carries D270's `loam_ce_types`), then `data` and `data_raw`. The `data` struct's mapping: `int32`/`sint32`/`sfixed32` → `Int32`; `int64`/`sint64`/`sfixed64` → `Int64`; `uint32`/`fixed32` → `UInt32`; `uint64`/`fixed64` → `UInt64`; `bool` → `Boolean`; `string` → `Utf8`; `bytes` → `Binary`, except fields named `ext` → `Utf8` (raw JSON); `float`/`double` → `Float32`/`Float64`; proto `enum` → `Int32` with the enum's full name in field metadata `loam.proto.enum`; `repeated T` → `List<T>` (non-null items); `map<K, V>` → `Map<K, V>`; a message → `Struct`; `loam.rtb.v1.Money` → `Struct{micros: Int64, currency: Dictionary<Int32, Utf8>}`; `google.protobuf.Timestamp` → `Timestamp(Nanosecond, UTC)`. Every scalar field is nullable (proto3 presence is not tracked for scalars; `optional` fields are null when unset, others hold their default). Recursion deeper than `max_depth` is an error naming the path. Each field carries `META_PROTO_PATH` and `META_PROTO_TAG`.

**Tests:** `auction_event_schema_matches_golden` and `win_event_schema_matches_golden` (serialized schema JSON; `UPDATE_GOLDEN=1` rewrites); `money_maps_to_micros_struct`; `enum_maps_to_int32_with_name`; `every_field_has_path_and_tag`; `recursion_limit_is_enforced` (a test-only recursive message); `attribute_columns_come_first`.

**Commit:** `events: derive Arrow schemas from event protobuf descriptors`.

### Task 6: CloudEvents ⇄ `RecordBatch`, Parquet and IPC, and the evolution check

**Files:** `crates/operon-events-arrow/{src/builder.rs,src/reader.rs,src/evolution.rs,tests/roundtrip.rs,tests/evolution.rs}`.

**Produces:**

```rust
pub struct EventBatchBuilder { /* per-column builders */ }
impl EventBatchBuilder {
    pub fn new(schema: &EventSchema, pool: &DescriptorPool, capacity: usize) -> Self;
    pub fn push(&mut self, ev: &CloudEvent) -> Result<(), ArrowMapError>;   // type must match; data decoded with DynamicMessage
    pub fn len(&self) -> usize;
    pub fn finish(&mut self) -> Result<RecordBatch, ArrowMapError>;
}
pub fn to_cloudevents(batch: &RecordBatch, schema: &EventSchema) -> Result<Vec<CloudEvent>, ArrowMapError>; // needs data_raw
pub fn check_additive(old: &Schema, new: &Schema) -> Result<(), EvolutionError>;
pub enum EvolutionError { Removed { path: String }, Retyped { path: String, from: String, to: String }, Renumbered { path: String }, NullabilityTightened { path: String } }
```

**Semantics:** `push` refuses an event whose `type` differs from the schema's, or whose `data` is not `application/protobuf` (JSON data for these types is converted at the edge, not here). `to_cloudevents` rebuilds attributes from the columns in the canonical attribute order of `operon_cloudevents::CONTEXT_ATTRIBUTES` then extensions in `ext` order, and `data` from `data_raw`; each extension takes its type from `ext_types` (absent: string), the same rule as D270's `loam_ce_types` header, so an integer, boolean, URI, URI-reference, timestamp or binary extension comes back typed; it refuses a batch without `data_raw`. `push` fills `ext_types` from the event's extension types. Because attribute order in the original event can differ from the canonical order, the round trip is byte-identical on attribute **values** and on `data`, and order-identical only for events produced by `event_from_proto` (which writes canonical order); the test names say which. `check_additive` matches fields by `META_PROTO_PATH`: a field may be added anywhere; nothing may be removed, retyped, given a new tag or made non-nullable.

**Tests:** `ipc_roundtrip_is_lossless`; `parquet_roundtrip_is_lossless` (written with the workspace `parquet` features, read back, compared); proptest `arrow_roundtrip_any_event` (256 cases, events from `event_from_proto` over arbitrary `AuctionEvent`s); `typed_extension_roundtrip` (an event with one extension of each non-string type survives IPC and Parquet with its types); `push_refuses_other_type`; `to_cloudevents_needs_raw`; `check_additive_accepts_new_field`, `check_additive_refuses_removed`, `check_additive_refuses_retyped`, `check_additive_refuses_new_tag`; `batch_of_10k_auction_events_under_budget` (a timing smoke test, `#[ignore]` by default, run in Task 7's notes).

**Commit:** `events: map CloudEvents to Arrow batches and check schema evolution`.

### Task 7: Docs and close

**Files:** `docs/design/34-protocol-gateway-and-standards.md` (as-built notes in §4, §6, §9 where names differ), `CHANGELOG.md`, `docs/design/_pending/` is not touched (the integrator owns the README and log).

**Semantics:** record the as-built crate APIs, the rulings made during execution and the timing smoke test's result (10 000 `AuctionEvent`s per batch, build and Parquet write time) in §34 §9.

**Tests:** `cargo test -p operon-rtb-proto -p operon-cloudevents -p operon-events-arrow`, `buf lint`, `buf breaking`, `cargo deny check`.

**Commit:** `docs: record GW1 as built`.

## PR sizes

| Task | Expected size | Notes |
|---|---|---|
| 0 | docs only | |
| 1 | ~150 lines (YAML, script, patches) | |
| 2 | ~1 500 lines, most of it proto | split into `request.proto` and `response.proto`+`service.proto` PRs if review asks |
| 3 | ~600 lines | |
| 4 | ~250 lines | |
| 5 | ~700 lines plus goldens | |
| 6 | ~900 lines | |
| 7 | docs | |

## Rulings made during execution

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| — | (Task 0 fills this table) | | |
