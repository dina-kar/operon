# API1 — The Unified Connect API: Consolidate Services, Remove REST and OpenAPI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (names, paths, headers), use them verbatim. The code is not pre-written in this plan; the tests are the specification.

> **Status: In progress** (2026-10-03): Tasks 0–1 done ([route map](../api/route-map.md), [reason registry](../api/reasons.md)). **Slot: new track SDK, first plan** (proposed; D600–D611). Branches `api1-t<N>`, stacked; PRs target `dev`. Depends on AP0 (app protos, `loams-apps-mock`), R1 (`loams.live.v1`) and D128's toolchain (connect-rust, buffa). Does not depend on SDK1 except Task 9's docs.

**Goal:** One Connect/gRPC API in `loams.<service>.v1` on the one port of the `loams` binary (design [§44](../design/44-unified-api-and-sdks.md) §4–§8):
- the protos and handlers for collections, documents, query, SQL, streams, links, admin and auth, plus the options file the SDK facade generator reads;
- the M1.2 native REST, the console OpenAPI and the other bespoke JSON routes replaced by those RPCs, each with a migration entry, and deleted after (optionally) one release of shims;
- gRPC compat services (Flight SQL, Qdrant gRPC) also mounted on the main port; `GetInstance.services[]`; health and reflection;
- the compatibility surfaces untouched and still green.

**Architecture:**
- **Protos** in `proto/loams/{options,collection,sql,link,stream,admin,auth,internal}/v1`. Conventions are AP0's: `NO_SIDE_EFFECTS` on reads, `idempotency_key` on mutations, `ErrorInfo.reason`, server-streaming only, cursors on streams, AIP-158 pagination. `loams.internal.v1` is a separate buf module (not in SDK generation).
- **Handlers** are thin: they call the same service traits (`CollectionService`, query, SQL, stream, meta) the REST handlers call and the compat adapters call. Task 0 maps every REST handler to its trait call. The REST crates' logic moves, it is not rewritten.
- **One router** (`crates/loams/src/server.rs`): the connect-rust service router plus the protocol endpoints (§44 §4) and the opt-in legacy shim router.
- **Tests** reuse the REST handlers' existing test bodies, re-targeted at the RPC, so behaviour is pinned before the old route is deleted.

**Tech Stack:** Rust 1.97.1, edition 2024, connect-rust (`connectrpc`), buffa, `connectrpc-build`, `buf` (lint STANDARD, breaking FILE), the `loams-apps-mock` crate. Versions recorded in the Task 0 reconciliation.

**Spec:** [§44](../design/44-unified-api-and-sdks.md); [§05](../design/05-query-engine.md) §4, §5, §8; [§19](../design/19-console-identity-and-agents.md) §5, P9; [§30](../design/30-loams-cli.md) §9; [§37](../design/37-desktop-and-mobile-apps.md) §8; `docs/plans/2026-09-24-m1.2-query-engine.md` (API tables, Task 14 scan); `docs/plans/2026-10-01-ap0-app-protos.md`; D101, D111, D128, D420.

## Global Constraints

- **Compatibility surfaces are not touched** (D603): the Qdrant, ES, Flight, Postgres, MCP, Git and Resonate suites stay green on every PR. A PR that changes their wire output is wrong.
- **Behaviour first, deletion last.** A REST route is deleted only after an RPC with the same tests passes (Tasks 3–7), and only in Task 9.
- **Names:** `loams.*` packages, `LOAMS_*` env vars. No resource name in a URL path: names are message fields.
- **`buf lint` clean**; `buf breaking` is not enforced until the first release tag (Task 10 turns it on).
- **No new dependency without `cargo deny`**; the build machine rule: one cargo build at a time.
- Commit areas: `proto`, `api`, `loams`, `console`, `docs`, `ci`.

## Rulings made while writing this plan

| # | Ruling | Why |
|---|---|---|
| 1 | `QueryService/Search` is the single RPC behind `loams.search` and `loams.vector` | One IR (§05 §4); two facade names (§44 §7.3) |
| 2 | `ScrollDocuments` is server-streaming of pages with a cursor field (provisional default for Q614; Task 4 may switch to unary pagination if Q614 is answered so) | Matches Watch conventions; unary pagination also offered via `page_token` |
| 3 | `/health`, `/ready`, OAuth, OIDC and well-known stay HTTP (D602) | RFCs and orchestrators |
| 4 | Resource names are request fields: `namespace`, `collection` (alias ok), `stream`, `partition` | Connect URLs are `/<package>.<Service>/<Method>` |

## Tasks (one PR each)

- [x] **Task 0: Reconcile with the as-built code.** Produce `docs/api/route-map.md`: every route in `crates/loams/src/api/*.rs` and `api/console/openapi.json` with its target RPC (start from §44 §5), the trait call it makes, and its existing tests. Record connect-rust/buffa versions. *Tests:* `route_map_covers_every_route` (a test parses the router and fails if a route is missing from the map).
- [x] **Task 1: Options, errors and server plumbing.** `loams/options/v1/options.proto` (§44 §7.3), `reason` registry `docs/api/reasons.md` and its test, the one-port router, `grpc.health.v1`, reflection (Q603 default), `InstanceService.GetInstance.services[]`, variant mapping `UNIMPLEMENTED` + `feature_not_in_variant`. *Tests:* `connect_json_unary_via_curl_shape`, `grpc_and_grpc_web_on_same_port`, `health_rpc_ok`, `unavailable_service_reports_reason`, `reasons_are_snake_case_and_unique`.
- [ ] **Task 2: `loams.collection.v1` Namespace and Collection services.** Rows of §44 §5.1 for namespaces, collections, fields, versions, aliases, hot/warm, scan (with the pin token and header `loams-consistency-token`). *Tests:* each existing REST collections test ported by its name with an `_rpc` suffix; `create_collection_repeat_is_safe`, `scan_returns_pin_token`.
- [ ] **Task 3: `DocumentService`.** Write, get, count, delete/patch by filter, with `idempotency_key` (dedupe window equals the REST one) and consistency tokens. *Tests:* ported REST tests; `write_idempotency_key_replays_same_token`; `consistency_at_least_waits`.
- [ ] **Task 4: `QueryService/Search`, `ScrollDocuments`, filter IR messages.** The native hybrid IR as proto (dense, sparse, text, filters, expand, fusion, `performance`). The §05 §4 body is accepted as the JSON mapping. *Tests:* the hybrid-query fixtures from M1.2 run through the RPC and equal the REST results; `scroll_streams_pages_with_cursor`.
- [ ] **Task 5: `loams.sql.v1`.** `Query` (rows, truncated), `QueryArrow` (IPC bytes with a cap; provisional until Q608 is answered). Flight SQL unchanged. *Tests:* ported SQL tests; `query_arrow_roundtrips`; `flight_sql_still_serves_8082`.
- [ ] **Task 6: `loams.stream.v1` full surface and `loams.link.v1`.** Move `StreamService` to connect-rust if not yet (D128), add `CreateStream`, `DescribeStream`, `Fetch`, `FetchCloudEvents`, links. Kafka gateway and CloudEvents bindings still call the same traits. *Tests:* ported stream/link tests; `produce_without_key_is_not_retried_by_server`; `cloudevents_http_binding_unchanged`.
- [ ] **Task 7: `loams.admin.v1` and `loams.auth.v1`.** Org, project, agent, key, audit services and `AuthService` per §44 §5.2; `WatchAuditEvents` stream. Tests are the console mock's. *Tests:* each console-mock route test ported; `session_cookie_and_bearer_both_work`; `oidc_start_remains_http_redirect`.
- [ ] **Task 8: Mount compat gRPC on the main port; `loams.internal.v1`.** Flight and Qdrant gRPC by service name; the cluster listener's internal service replaces `/internal/*`. *Tests:* `qdrant_grpc_client_works_on_main_port`, `flight_client_works_on_main_port`, `cluster_tests_use_internal_rpc` (the cluster test suite passes).
- [ ] **Task 9: Remove REST and OpenAPI.** Console moves to `@loams/proto` clients (AP1a); delete the native REST routes, `api/console/openapi.json` and the `openapi-typescript` step; optional `--legacy-rest` shim router (Q600) with the "moved" JSON, `Deprecation` and `Sunset` headers; migration page `docs/api/migrate.md` generated from the route map; amend M1.6 and AP1a status lines. *Tests:* `no_native_rest_route_remains` (router inventory equals the allowed protocol endpoints), `legacy_shim_points_to_rpc_when_enabled`, `compat_suites_green`, the console's tests.
- [ ] **Task 10: Contract gates.** `buf breaking` against the release tag for non-`unstable` packages, CI job `api-protos`, curl examples in `docs/api/curl.md` executed by a test (`curl_examples_run`). Update the plans README and the decision log pointers.

## Rulings made during execution

| # | Task | Ruling | Why |
|---|---|---|---|
| 0.1–0.4 | 0 | Recorded in [`docs/api/route-map.md`](../api/route-map.md) §"Rulings made in this reconciliation": 27 `/v1` method/path pairs as built (the 25 `api::router` registers plus `hot` and `warm`, which `hot::routes()` registers separately); the console contract is served only by `loams-console-mock`; the metastore Raft transport is mapped to `loams.internal.v1` (moved in Task 8 if openraft's wire types allow, else a follow-up); hot and warm are `SetHot` and `WarmCollection`. | As built on `dev`, 2026-10-02 |
| 1.1 | 1 | The Connect paths are registered as their own axum routes, **not** as `api::router`'s fallback. A fallback would take the native API's `404`/`405` away, which `it/http.rs::framework_rejections_use_the_json_error_body` pins and which Task 9's `no_native_rest_route_remains` still expects of the paths that survive it. The two sets cannot collide: a Connect path is `/<package>.<Service>/<Method>`, every native path is under `/v1`, `/internal`, `/health` or `/ready`. | Task 0's route map already records that the native `404` stays for unknown paths; this is how |
| 1.2 | 1 | The public packages get a new crate, `loams-proto`, which generates `loams.options.v1`, `loams.errors.v1` and `loams.instance.v1` and emits their descriptor set for `grpc.reflection.v1`. **`loams.live.v1` is not in it**: R1's `loams-live-proto` already generates those files, and a proto package's Rust types are generated exactly once per workspace. Tasks 2–8 append their packages here and the server reaches for them through it. | The precedent is `loams-apps-mock` and `loams-live-proto`; SDK1 ships this crate as the Rust SDK's generated surface (§44 §11) |
| 1.3 | 1 | **`feature_not_in_variant` is demonstrated on `loams.live.v1`.** The live sync engine is a `full`-variant engine (§30 §8.2) and R1's `live` cargo feature has not merged, so no variant serves it yet; its `LiveService` is registered with a stub that refuses every RPC with `unimplemented` + reason `feature_not_in_variant` + `metadata.variant`, which is §44 §4's rule applied to the one catalogue package whose engine is genuinely absent. R1 Task 12 flips the catalogue row on. | §44 §4; §30 §8.2 |
| 1.4 | 1 | **Reflection is on in `loams dev` and off everywhere else**, via a new `ServerConfig.reflection` (default false) that `main.rs`'s `Dev` arm sets. That is Q603's proposed answer, adopted while Q603 is still open; no plan task owns the `--reflection` flag, and `Server::start` callers (every test) get the default. | Q603's proposed option; §44 §4 |
| 1.5 | 1 | The variant name is derived from the cargo features (`full` when `tikv`, `mysql-wire` or `stream-grpc` is compiled in, else `standard`) and published as `loams::api::connect::VARIANT`. CLI2 Task 3 replaces this with `release/variants.toml`; until then the features are the variant, and `unavailable_service_reports_reason` asserts against `VARIANT` so a feature change cannot break it. | §30 §8.2's engine table; §44 §4 |
| 1.6 | 1 | `docs/api/reasons.md` is the single registry, and `proto/loams/errors/v1/errors.proto`'s own reason table was replaced by a pointer to it (so the page and the proto cannot disagree). It holds AP0's reasons plus the generic code-to-reason mapping D611's error hierarchy implies; the REST `error` values in `api/errors.rs` become the RPC `reason` unchanged, so Tasks 2–8 reuse `ApiError`'s codes rather than re-deriving them. The specific reasons Tasks 2–8 need are appended as their services land. | D611; `api/errors.rs` |
| 1.7 | 1 | `loams.live.v1` is **not** annotated with `loams.options.v1` options yet, though it is the one `unstable` package. Annotating it changes the descriptor `sdks/live-typescript/src/gen/` was generated from, and AP0's `buf.gen.apps.yaml` drift check would fail until R1 regenerates it. The `module` option lands with R1 Task 12, which is when the server-side service lands. | §44 §10.3; `buf.gen.apps.yaml` |
