# GW3 — Version Negotiation, Fallback, and the OpenRTB 2.5 and 2.4 Adapters Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (thresholds, intervals, metric names, field paths), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: Planned** (2026-10-01). **Track GW** (design [§34](../design/34-protocol-gateway-and-standards.md)), build-order item 3. Depends on GW2 merged; gated on Q360 like GW2. Branches `gw3-t<N>`, stacked; PRs target `main`. GW3 adds two profiles, the outbound client, the negotiator and its store; it changes GW2 only by registering the new adapters and adding the outbound path to the gateway.

**Goal:**
- The OpenRTB **2.5** and **2.4** profiles over GW2's superset model: field tables checked against the specs, ext promotions on decode (upgrade) and the downgrade rules on encode, with a `LossReport` (D370, §34 §7.3).
- **Per-partner negotiation** for outbound requests: the `Negotiator`, the circuit breaker, the upward probe, the `CapabilityStore` with memory and file backends (D369, §34 §7.2).
- **`PartnerClient` and `Exchange`**: outbound fan-out of one canonical auction to many partners, each at its negotiated version, inside the auction's deadline.
- Conformance: the 2.5 and 2.4 corpora, "downgrade then upgrade never invents data", and the negotiation scenarios (D377 rows 1, 2, 5).

**Architecture:**
- **Profiles are data.** `profile::{v2_4, v2_5, v2_6}` are static `FieldRule` tables (GW2 Task 3's types). One generic `upgrade` and one generic `downgrade` walk the model's JSON form with a table; there is no per-version code path except `Downgrade::Map` functions.
- **Negotiation state is per gateway node**, in memory, written through to a `CapabilityStore` on every change; a starting node loads it. Nodes do not gossip (§34 §7.2).
- **Time is injected.** The negotiator and breaker take a `Clock` (`operon-protocol::clock::{Clock, SystemClock, ManualClock}`), so every scenario test is deterministic.

**Tech Stack:** as GW2, plus `reqwest` 0.12 with `http2` for the outbound pool (already in the workspace; Task 0 checks its features). No new third-party crates are expected.

**Spec:**
- [§34](../design/34-protocol-gateway-and-standards.md) §7 (all of it), §8.1, §13 rows 1, 2, 5, §18 Q370, Q374.
- OpenRTB 2.6-202606 Appendix B (the change logs 2.4 → 2.5 and 2.5 → 2.6); the OpenRTB 2.5 and 2.4 specifications (IAB Tech Lab PDFs; Task 0 records their URLs, SHA-256 and licence).
- As built after GW2: `operon-protocol`, `operon-openrtb` (`model`, `profile`, `canon`), `operon-conformance`, `operon-rtb-gateway`.

## Global Constraints

Same as GW2, plus:
- **No in-request retries at another version** (§34 §7.2). The request that triggers a downgrade is not resent.
- **Timeouts, connection errors and 5xx never change a partner's version.**
- **Every dropped field is reported.** An encode that drops or moves a field and does not list it in `LossReport` is a bug (Review Focus 2).
- **Commit areas:** `rtb`, `conformance`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **A structural rejection** is: HTTP 400 or 422; a 200 whose body decodes to a `BidResponse` with `nbr = 2` (Invalid Request) and no bids; or a 200 whose body fails to decode at the version sent, three times in a row from that partner at that version. Nothing else is | 400/422 and `nbr` 2 are the only explicit signals; OpenRTB 2.6 §4 says "a malformed response … will also be interpreted as no-bid", so a single bad body is not a version signal | A partner that answers 500 to a version it does not parse never downgrades; its record must name its version, which Task 0 documents for operators |
| 2 | **Breaker defaults:** open after **5** structural rejections within **30 s** at one `(partner, version)`; half-open after `probe_interval` (**1 h**); in half-open, **one request in 1 000, at most one per second**, is sent at the higher version | The draft's shape; conservative so that a flapping partner does not lose auctions | Configurable per partner record (`breaker_threshold`, `breaker_window_s`, `probe_interval_s`, `probe_ratio`) |
| 3 | **Response header downgrade:** a response `x-openrtb-version` lower than the request's moves the partner to that version immediately if it is in the record's offered list, with reason `header`; a higher one is ignored | OpenRTB 2.6 §2.5: bidders may answer with the version they implement | A partner that sends a wrong header downgrades itself; it shows in `loam_rtb_downgrades_total{reason="header"}` |
| 4 | **`NotRepresentable` skips the partner for that auction only** and never downgrades further | A DOOH-only auction cannot be expressed in 2.5; trying 2.4 does not help | Partners that would accept a lossy form get it only through an explicit record option (`lossy_pods`, `lossy_dooh`: not offered in GW3) |
| 5 | **`CapabilityStore` backends in GW3: `MemoryCapabilityStore` and `FileCapabilityStore`** (one JSON file, written by write-to-temp-and-rename in the same directory). The `ControlStore` backend waits for R2 (Q374) | Development and single-node deployments need persistence now; multi-node needs the control plane | Multi-node clusters relearn versions per node until R2 |
| 6 | **The 2.5 and 2.4 field lists are checked-in text files extracted by hand from the PDFs** (`corpus/specs/openrtb-2.5-fields.txt`, `openrtb-2.4-fields.txt`: one `object.attribute type` per line, with the PDF's SHA-256 in `SOURCES.toml`); 2.6's list is parsed from the vendored `2.6.md` tables | The older specs exist only as PDFs; a hand extraction reviewed once is cheaper than a PDF parser | An extraction mistake; Task 1's test cross-checks the lists against Appendix B's change logs, which catches most |
| 7 | **2.3 is not built** (Q370); the profile type admits it later without code changes outside `profile/v2_3.rs` | The draft lists 2.3, the build order lists 2.5 and 2.4 only | A partner on 2.3 is refused with `unsupported_version` until then |

## Carried in

From GW2: its "Rulings made during execution"; the `FieldRule` and `Downgrade` types; `PartnerRecord` (gains the breaker fields of Ruling 2 and `offered: Vec<ProtocolVersion>` in preference order). From §34 §7.3: the draft downgrade table, which Task 0 verifies row by row.

## Review Focus

1. **Never invent data.** `upgrade(downgrade(x)) ⊑ x` for every canonical auction and every lower version. Tests: Task 7 (`downgrade_then_upgrade_never_invents_data_25`, `…_24`).
2. **Loss reports are complete.** Tests: Task 7 (`loss_report_is_complete`).
3. **Only structural rejections downgrade.** Tests: Task 6 (`timeouts_never_downgrade`, `server_errors_never_downgrade`, `single_bad_body_does_not_downgrade`).
4. **The breaker recovers.** Tests: Task 5 (`breaker_opens_after_threshold`, `probe_promotes_after_recovery`, `probe_rate_is_bounded`).
5. **Deadlines hold under fan-out.** Tests: Task 6 (`fanout_respects_deadline`).

## File structure

```
crates/operon-protocol/src/clock.rs                       # Clock, SystemClock, ManualClock (Task 5)
crates/operon-openrtb/src/profile/{v2_5.rs,v2_4.rs,upgrade.rs,downgrade.rs,maps.rs}
crates/operon-openrtb/tests/{profiles.rs,upgrade.rs,downgrade.rs}
crates/operon-conformance/corpus/specs/{openrtb-2.5-fields.txt,openrtb-2.4-fields.txt}
crates/operon-conformance/corpus/openrtb/{2.5,2.4}/{valid,invalid}/*.json (+ .expected.json)
crates/operon-conformance/tests/{openrtb25.rs,openrtb24.rs,downgrade_props.rs,negotiation.rs}
crates/operon-conformance/tests/support/mock_partner.rs
crates/operon-rtb-gateway/src/{negotiator.rs,breaker.rs,capability.rs,partner_client.rs,exchange.rs}
crates/operon-rtb-gateway/tests/{negotiator.rs,exchange.rs}
docs/design/34-protocol-gateway-and-standards.md  CHANGELOG.md
```

### Task 0: Reconcile and verify the downgrade table

**Files:** read GW2's crates as merged. Fill "Rulings made during execution" and add an "As verified" column to §34 §7.3.

**Checks** (each with its source and result):
- The OpenRTB 2.5 and 2.4 PDFs: URLs, SHA-256, and their licence statements (CC BY 3.0 expected, **verify**); record in `SOURCES.toml`.
- Every row of §34 §7.3 against the specs: in which version each field became first-class, and where the 2.5-era convention put it in `ext` (`regs.ext.gdpr`, `user.ext.consent`, `user.ext.eids`, `source.ext.schain`, `regs.ext.us_privacy`; the IAB's GDPR, CCPA, EIDs and SupplyChain guidance documents are the sources for the `ext` locations).
- The `video.plcmt` → `video.placement` value mapping: whether the IAB published a one-to-one guide (2.6-202303 release notes); if not, `plcmt` is `Drop` at 2.5 and `placement` is sent only when the canonical `Legacy.placement` holds one.
- When `regs.us_privacy`, `regs.gpp` and `regs.gpp_sid` became first-class (2.6 base or a monthly release) and whether any 2.5 convention exists for GPP.
- Q370 and Q374's status.

**Commit:** `docs: reconcile GW3 and verify the OpenRTB downgrade rules`.

### Task 1: The 2.5 and 2.4 profiles

**Files:** `crates/operon-openrtb/src/profile/{mod.rs,v2_5.rs,v2_4.rs}`, `crates/operon-conformance/corpus/specs/*.txt`, `crates/operon-openrtb/tests/profiles.rs`.

**Produces:** `pub static V2_5: Profile; pub static V2_4: Profile;` and `profile::for_version(v) -> Option<&'static Profile>`. Each `FieldRule` names `since`, `removed_in`, the `ext_before` location where a convention existed, and the `Downgrade` rule for encoding below `since`.

**Tests:** `profile_fields_match_spec_lists` (for each version, the set of `path`s with `since ≤ v < removed_in` equals the spec's field list, modulo `ext`); `appendix_b_deltas_match_profiles` (every field Appendix B says was added between two versions has `since` equal to the later one); `every_26_field_has_a_downgrade_rule` (no field newer than 2.4 lacks a rule).

**Commit:** `rtb: add the OpenRTB 2.5 and 2.4 profiles`.

### Task 2: Upgrade on decode

**Files:** `crates/operon-openrtb/src/profile/upgrade.rs`, `crates/operon-openrtb/src/canon/to_canonical.rs` (profile-generic), `crates/operon-openrtb/tests/upgrade.rs`.

**Produces:** `pub fn upgrade(value: &mut serde_json::Value, from: &Profile) -> Vec<Warning>`, applied to the parsed request before `to_canonical`; `to_canonical` takes any profile.

**Semantics:** for each rule with `ext_before = Some(loc)` and `since > from.version`, move the value at `loc` to `path` if `path` is absent (`Warning::PromotedFromExt`); if both are present keep `path` and leave `loc` (`Warning::ConflictingLocations`). Fields removed in 2.6 (`banner.wmax`, …) fill the canonical `Legacy` messages. `Provenance.version` is the profile's.

**Tests:** `gdpr_in_regs_ext_is_promoted_from_25`; `eids_in_user_ext_are_promoted_from_25`; `schain_in_source_ext_is_promoted_from_25`; `banner_wmax_fills_legacy_from_24`; `conflict_keeps_first_class`; `upgrade_is_idempotent`.

**Commit:** `rtb: promote older OpenRTB conventions on decode`.

### Task 3: Downgrade on encode

**Files:** `crates/operon-openrtb/src/profile/{downgrade.rs,maps.rs}`, `crates/operon-openrtb/src/canon/from_canonical.rs`, `crates/operon-openrtb/tests/downgrade.rs`.

**Produces:** `pub fn downgrade(value: &mut serde_json::Value, to: &Profile) -> Result<LossReport, EncodeError>`; `maps::plcmt_to_placement` (per Task 0's finding) and any other `Map` functions.

**Semantics:** walk the 2.6 JSON form; for each present field whose rule says it is not valid at `to`: `MoveToExt(loc)` writes it at `loc` (creating `ext` objects; never overwriting an existing `ext` member, which is a `Warning::ConflictingLocations` and a `Drop` of the moved value, reported); `Drop` removes and reports; `Map` transforms; `NotRepresentable` returns `EncodeError::NotRepresentable` with the path. `Legacy` fields valid at `to` are written back. Rules apply deepest path first so `ext` creation never collides with a moved parent.

**Tests:** one test per §34 §7.3 row as verified in Task 0 (`regs_gdpr_moves_to_ext_at_25`, `user_eids_moves_to_ext_at_25`, `source_schain_moves_to_ext_at_25`, `rwdd_is_dropped_at_25`, `source_moves_to_ext_at_24`, `metric_is_dropped_at_24`, `dooh_only_is_not_representable_at_25`, `ratio_only_format_is_not_representable_at_24`, `plcmt_maps_or_drops_per_task0`); `existing_ext_member_is_not_overwritten`; `legacy_fields_are_written_at_24`.

**Commit:** `rtb: downgrade canonical auctions to OpenRTB 2.5 and 2.4 with loss reports`.

### Task 4: The 2.5 and 2.4 adapters and corpora

**Files:** `crates/operon-openrtb/src/adapter.rs` (`v2_5()`, `v2_4()`), `crates/operon-rtb-gateway/src/router.rs` (register both), `crates/operon-conformance/corpus/openrtb/{2.5,2.4}/**`, `crates/operon-conformance/tests/{openrtb25.rs,openrtb24.rs}`.

**Semantics:** inbound requests at 2.5 and 2.4 now decode (GW2's `version_25_is_400_until_gw3` is inverted to `version_25_is_served`); responses are encoded at the request's version. `detect` gives `Likely(2.5)` for a body with `source` but no 2.6-only field, `Likely(2.4)` without `source`, never `Certain` without the header. Corpora: hand-written files covering the same cases as GW2's 2.6 list where the version allows them, plus `gdpr-in-ext` (2.5), `video-placement` (2.5), `audio-24`, `banner-wmax-24`; the spec examples of the 2.5 PDF where their licence allows (Task 0).

**Tests:** `golden_25_valid`, `golden_25_invalid`, `golden_24_valid`, `golden_24_invalid`; `version_25_is_served`; `response_version_matches_request`; `oracle_iab_specs_agrees_25` (iab-specs has 2.5).

**Commit:** `rtb: serve OpenRTB 2.5 and 2.4`.

### Task 5: The negotiator, the breaker and the capability store

**Files:** `crates/operon-protocol/src/clock.rs`, `crates/operon-rtb-gateway/src/{negotiator.rs,breaker.rs,capability.rs}`, `crates/operon-rtb-gateway/tests/negotiator.rs`.

**Produces:**

```rust
pub trait Clock: Send + Sync { fn now(&self) -> Instant; fn unix_ns(&self) -> i64; }
pub struct ManualClock { /* advance(Duration) */ }
pub enum Signal { Accepted, NoBid, StructuralRejection { status: u16, nbr: Option<i32> }, BadBody, HeaderVersion(ProtocolVersion), Transient }
pub enum BreakerState { Closed, Open { since: Instant }, HalfOpen }
pub struct PartnerState { pub current: ProtocolVersion, pub breakers: BTreeMap<ProtocolVersion, BreakerState>, pub learned_unix_ns: i64 }
pub struct Negotiator { /* per partner: PartnerState, rejection windows, bad-body streaks */ }
impl Negotiator {
    pub fn new(book: &PartnerBook, store: Arc<dyn CapabilityStore>, clock: Arc<dyn Clock>) -> Self;
    pub fn version_for(&self, partner: &str) -> Choice;                  // Choice { version, probe: bool }
    pub fn observe(&self, partner: &str, sent: ProtocolVersion, signal: Signal);
}
#[async_trait] pub trait CapabilityStore: Send + Sync { async fn load(&self) -> Result<BTreeMap<String, PartnerState>, StoreError>; async fn save(&self, partner: &str, state: &PartnerState) -> Result<(), StoreError>; }
pub struct MemoryCapabilityStore; pub struct FileCapabilityStore { pub path: PathBuf }
```

**Semantics:** Rulings 1–3 and 5. `version_for` returns the current version, or, when the next higher offered version's breaker is half-open and the probe budget allows (Ruling 2), that version with `probe: true`. `observe` on a probe's `Accepted` or `NoBid` closes that breaker and promotes; on a probe's rejection reopens it. Saves happen off the request path (a `tokio::spawn` with the latest state; a failed save is logged and retried on the next change). Metrics: `loam_rtb_partner_version{partner}` (gauge, `major*100+minor`), `loam_rtb_downgrades_total{partner,from,to,reason}` (`reason` ∈ `rejection`, `nbr`, `bad_body`, `header`), `loam_rtb_breaker_state{partner,version}` (0 closed, 1 open, 2 half-open), `loam_rtb_probes_total{partner,version,result}`.

**Tests:** `starts_at_highest_offered`; `downgrades_after_threshold_rejections`; `nbr_2_counts_as_rejection`; `single_bad_body_does_not_downgrade`; `three_bad_bodies_downgrade`; `header_lower_downgrades_directly`; `header_higher_is_ignored`; `breaker_opens_after_threshold`; `probe_promotes_after_recovery`; `probe_rate_is_bounded` (10 000 calls in one second at half-open yield exactly one probe); `learned_version_survives_restart` (file store); `file_store_write_is_atomic` (a crash between write and rename leaves the old file).

**Commit:** `rtb: negotiate OpenRTB versions per partner with a breaker and a probe`.

### Task 6: `PartnerClient` and `Exchange` (outbound)

**Files:** `crates/operon-rtb-gateway/src/{partner_client.rs,exchange.rs}`, `crates/operon-conformance/tests/support/mock_partner.rs`, `crates/operon-rtb-gateway/tests/exchange.rs`.

**Produces:**

```rust
pub struct PartnerEndpoint { pub partner: String, pub url: Url, pub gzip: bool }
pub struct PartnerClient { /* reqwest::Client with http1 keep-alive and h2, per-host pool */ }
impl PartnerClient { pub async fn send(&self, ep: &PartnerEndpoint, auction: &Auction, choice: Choice, deadline: Instant) -> PartnerResult; }
pub enum PartnerResult { Bids(Decoded<AuctionResponse>), NoBid { nbr: Option<i32> }, Skipped(SkipReason), Rejected { status: u16 }, Failed(FailReason) }
pub enum SkipReason { NotRepresentable(FieldPath), BreakerOpen }
pub enum FailReason { Timeout, Connect, Status(u16), BadBody(String) }
pub struct Exchange { /* registry, negotiator, client, sink */ }
impl Exchange { pub async fn run(&self, cx: &BidCx, auction: &Auction, partners: &[PartnerEndpoint]) -> Vec<(String, PartnerResult)>; }
```

**Semantics:** `send` encodes at `choice.version` with the registry's adapter, sets `x-openrtb-version`, `content-type: application/json`, optional gzip, posts with a timeout of `deadline − now`, classifies the response into a `Signal` for the negotiator, and decodes bids at the version sent. `run` sends to all partners concurrently and returns when all answer or the deadline passes (late answers are dropped and counted as `Timeout`). It emits one `dev.loam.rtb.auction.v1` event per auction with the per-partner results in `AuctionEvent` (GW1's message gains no fields: per-partner detail goes in the event's `subject` = partner and one event per partner, ids `<auction id>:<partner>:<node>:<seq>`).

`mock_partner`: an axum server with a programmable behaviour per test: accepted versions, rejection style (400, 422, `nbr` 2, bad body), response header version, latency, 5xx.

**Tests:** `fanout_respects_deadline`; `partner_rejects_26_falls_back_to_25` (the next auction goes at 2.5; the rejecting one is not retried); `timeouts_never_downgrade`; `server_errors_never_downgrade`; `breaker_open_partner_is_skipped`; `not_representable_skips_partner`; `version_header_is_sent`; `gzip_is_sent_when_configured`; `events_per_partner_are_emitted`.

**Commit:** `rtb: fan auctions out to partners at their negotiated versions`.

### Task 7: Properties and the negotiation scenarios

**Files:** `crates/operon-conformance/tests/{downgrade_props.rs,negotiation.rs}`.

**Semantics:**
- `downgrade_then_upgrade_never_invents_data_25` and `…_24`: for arbitrary canonical auctions valid at 2.6 (GW2's strategy), encode at the lower version (skipping `NotRepresentable` cases, counted) and decode back; every field in the result equals the original's, and every original field absent from the result is in the `LossReport`. 1 024 cases each.
- `loss_report_is_complete`: the set of paths that differ between original and round trip equals the `LossReport`'s `dropped` set.
- The scenario suite of §34 §13 row 5 end to end through `Exchange` and `mock_partner` with `ManualClock`: a partner that rejects 2.6 (fallback engages); that rejects 2.6 for an hour then accepts it (the breaker opens, then the probe promotes); that answers a lower header; that times out for a minute (no downgrade); and a restart in between (the file store restores the learned version).

**Tests:** as named; all deterministic (`ManualClock`, seeded proptest).

**Commit:** `conformance: add downgrade properties and negotiation scenarios`.

### Task 8: Docs and close

**Files:** `docs/design/34-protocol-gateway-and-standards.md` (§7.3's "As verified" column, as-built names), `CHANGELOG.md`, operator notes on partner records in `docs/` (where GW2's notes live).

**Tests:** the `rtb` job green with the new tests; `cargo deny check`.

**Commit:** `docs: record GW3 as built`.

## PR sizes

| Task | Expected size |
|---|---|
| 1 | ~900 lines (tables) plus the field lists |
| 2 | ~500 lines |
| 3 | ~800 lines |
| 4 | ~400 lines plus corpus files |
| 5 | ~1 000 lines |
| 6 | ~1 000 lines |
| 7 | ~600 lines |
| 8 | docs |

## Rulings made during execution

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| — | (Task 0 fills this table) | | |
