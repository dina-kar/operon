# Route map: every HTTP route and its Connect RPC

Status: **API1 Task 0** (2026-10-02), reconciled with `dev` at the start of the API1 plan ([plan](../plans/2026-10-02-api1-unified-connect.md), design [§44](../design/44-unified-api-and-sdks.md) §5).

This page lists every route the `loams` binary serves over plain HTTP, and every path in the console contract `api/console/openapi.json`, with the RPC that replaces it, the internal call the handler makes (the call the RPC handler makes too: logic moves, it is not rewritten), and the tests that pin its behaviour today. The test `route_map_covers_every_route` (`crates/loams/tests/route_map.rs`) reads the routers' source and the contract and fails if a route is missing from this page, if this page lists a route that no longer exists, or if the number of `/v1` rows here is not the number of `/v1` method+path pairs the routers serve.

Format rules the test relies on: each route is one table row whose first cell is one HTTP method and whose second cell is the path in backticks, written as axum writes it (`{param}`). Rows whose first cell is not a method are ignored.

Kinds: **app** is application API, replaced by the RPC and deleted in Task 9; **protocol** is a protocol endpoint that stays HTTP (D602); **internal** is node-to-node, moved to `loams.internal.v1` on the cluster listener (Task 8); **raft** is the metastore's Raft transport (see ruling 0.3).

## Toolchain (recorded for Task 1)

| Component | Version | Where |
|---|---|---|
| `connectrpc` (connect-rust) | 0.9.1 | workspace `Cargo.toml`, `Cargo.lock` |
| `connectrpc-build` / `connectrpc-codegen` | 0.9.0 / 0.9.0 | build dependency of the proto crates |
| `buffa` / `buffa-types` / `buffa-codegen` | 0.9.2 / 0.9.2 / 0.9.2 | messages and well-known types (JSON feature) |
| `connectrpc-health`, `connectrpc-reflection` | 0.9.0 (crates.io) | `grpc.health.v1`, `grpc.reflection.v1`, added in Task 1 |
| `axum` | 0.8.9 | the one HTTP router; connect-rust mounts as an axum service |
| `tonic` | 0.14 | the tonic-era `loams-stream-grpc` (moves to connect-rust in Task 6, D128) and the Qdrant and Flight gRPC compat servers (stay tonic: compat) |

Existing Connect precedent: `loams-apps-mock` (AP0) and `loams-live-proto` (R1) generate buffa messages and connect-rust service traits from `proto/` in `build.rs` with the system `protoc`; the mock serves them with `Router::into_axum_service()` as an axum fallback.

## Native REST (`crates/loams/src/api/mod.rs`, `collections.rs`, `query.rs`, `sql.rs`, `events.rs`, `hot.rs`)

Served by `api::router` on `--listen` (gateway role). Tests are in `crates/loams/tests/it/` unless noted.

| Method | Route | Kind | Target RPC | Internal call | Existing tests |
|---|---|---|---|---|---|
| GET | `/health` | protocol | kept; also `grpc.health.v1.Health/Check` | none (200) | `it/http.rs` (startup helpers poll it) |
| GET | `/ready` | protocol | kept; also `grpc.health.v1.Health/Check` | `MetaStore::is_ready` | `it/http.rs`, `cluster.rs` |
| POST | `/v1/namespaces` | app | `loams.collection.v1.NamespaceService/CreateNamespace` | `MetaStore::create_namespace` | `http.rs::namespaces_and_streams_are_created_once` |
| POST | `/v1/namespaces/{ns}/streams` | app | `loams.stream.v1.StreamService/CreateStream` | `MetaStore::create_stream` | `http.rs::namespaces_and_streams_are_created_once` |
| GET | `/v1/namespaces/{ns}/streams/{stream}` | app | `loams.stream.v1.StreamService/DescribeStream` | `MetaStore::stream` | `http.rs::namespaces_and_streams_are_created_once` |
| POST | `/v1/namespaces/{ns}/streams/{stream}/partitions/{partition}/records` | app | `loams.stream.v1.StreamService/Produce` | `api::produce_records` (`LogWriter::append`) | `http.rs::produced_records_are_fetched_back`, `acknowledged_records_survive_a_restart`, `a_record_for_the_wrong_partition_of_an_implicit_stream_is_rejected` |
| GET | `/v1/namespaces/{ns}/streams/{stream}/partitions/{partition}/records` | app | `loams.stream.v1.StreamService/Fetch` | `LogReader::fetch` (long-poll on the offset watch) | `http.rs::produced_records_are_fetched_back`, `a_long_poll_fetch_wakes_on_produce`, `out_of_range_offsets_get_416_with_both_bounds` |
| POST | `/v1/namespaces/{ns}/streams/{stream}/events` | app | `loams.stream.v1.StreamService/ProduceCloudEvents` | `events::ingest` (dedup ledger, `LogWriter::append`) | `it/events.rs` (10 tests), `stream_grpc_events.rs::cloudevents_over_grpc_share_the_ledger_with_http` |
| GET | `/v1/namespaces/{ns}/streams/{stream}/partitions/{partition}/events` | app | `loams.stream.v1.StreamService/FetchCloudEvents` | `LogReader::fetch` and the envelope synthesis | `events.rs::a_record_that_is_not_an_event_is_read_as_a_synthesized_envelope`, `binary_mode_maps_headers_and_serves_binary_mode_back` |
| POST | `/v1/namespaces/{ns}/links` | app | `loams.link.v1.LinkService/CreateLink` | `MetaStore::create_link` | `http.rs::a_link_is_created_once_and_sums_its_stream` |
| GET | `/v1/namespaces/{ns}/links/{link}` | app | `loams.link.v1.LinkService/DescribeLink` | `MetaStore::link`, `CounterTable::load`, `TargetRegistry` | `http.rs::a_link_is_created_once_and_sums_its_stream`, `the_link_endpoint_shows_version_and_applied_for_collections` |
| POST | `/v1/namespaces/{ns}/collections` | app | `loams.collection.v1.CollectionService/CreateCollection` | `CollectionService::create_collection` | `native_collections.rs::collection_routes_speak_the_documented_json` |
| GET | `/v1/namespaces/{ns}/collections` | app | `loams.collection.v1.CollectionService/ListCollections` | `CollectionService::list_collections` | `native_collections.rs::collection_routes_speak_the_documented_json` |
| GET | `/v1/namespaces/{ns}/collections/{c}` | app | `loams.collection.v1.CollectionService/GetCollection` | `CollectionService::get_collection`, `hot::hot_status_value` | `native_collections.rs::collection_routes_speak_the_documented_json`, `hot_http.rs::get_collection_reports_hot_status_per_structure` |
| DELETE | `/v1/namespaces/{ns}/collections/{c}` | app | `loams.collection.v1.CollectionService/DropCollection` | `CollectionService::drop_collection` | `native_collections.rs::collection_routes_speak_the_documented_json` |
| POST | `/v1/namespaces/{ns}/collections/{c}/fields` | app | `loams.collection.v1.CollectionService/AddFields` | `CollectionService::add_fields` | `native_collections.rs::collection_routes_speak_the_documented_json` |
| GET | `/v1/namespaces/{ns}/collections/{c}/versions` | app | `loams.collection.v1.CollectionService/ListVersions` | `CollectionService::versions` | `native_collections.rs::collection_routes_speak_the_documented_json` |
| POST | `/v1/namespaces/{ns}/collections/{c}/scan` | app | `loams.collection.v1.CollectionService/Scan` | `CollectionService::scan_plan` | `native_scan.rs::scan_route_speaks_the_documented_json` |
| POST | `/v1/namespaces/{ns}/aliases` | app | `loams.collection.v1.CollectionService/UpdateAliases` | `CollectionService::update_aliases` | `native_collections.rs::collection_routes_speak_the_documented_json`, `hot_http.rs::hot_routes_accept_aliases` |
| POST | `/v1/namespaces/{ns}/collections/{c}/documents` | app | `loams.collection.v1.DocumentService/WriteDocuments` | `CollectionService::write` | `native_collections.rs::write_get_scroll_count_round_trip_over_http`, `a_rejected_atomic_write_is_400_with_the_op_index`, `write_responses_carry_the_consistency_token_header`, `backpressure.rs` (4 tests) |
| POST | `/v1/namespaces/{ns}/collections/{c}/documents/get` | app | `loams.collection.v1.DocumentService/GetDocuments` | `CollectionService::get_with_token` | `native_collections.rs::write_get_scroll_count_round_trip_over_http` |
| POST | `/v1/namespaces/{ns}/collections/{c}/documents/scroll` | app | `loams.collection.v1.DocumentService/ScrollDocuments` | `CollectionService::scroll_with_token` | `native_collections.rs::write_get_scroll_count_round_trip_over_http` |
| POST | `/v1/namespaces/{ns}/collections/{c}/documents/count` | app | `loams.collection.v1.DocumentService/CountDocuments` | `CollectionService::count_with_token` | `native_collections.rs::write_get_scroll_count_round_trip_over_http` |
| POST | `/v1/namespaces/{ns}/collections/{c}/documents/delete_by_filter` | app | `loams.collection.v1.DocumentService/DeleteByFilter` | `CollectionService::delete_by_filter` | `filter_write_http.rs::delete_by_filter_route_speaks_the_documented_json`, `over_the_limit_is_400_with_matched_and_limit`, `the_token_header_covers_the_filter_write` |
| POST | `/v1/namespaces/{ns}/collections/{c}/documents/patch_by_filter` | app | `loams.collection.v1.DocumentService/PatchByFilter` | `CollectionService::patch_by_filter` | `filter_write_http.rs::patch_by_filter_route_applies_the_patch` |
| PUT | `/v1/namespaces/{ns}/collections/{c}/hot` | app | `loams.collection.v1.CollectionService/SetHot` | `MetaStore::set_collection_hot`, `hot::local_status` | `hot_http.rs::put_hot_sets_the_catalog_and_returns_status`, `put_hot_on_a_missing_collection_is_404`, `a_malformed_hot_body_is_400`, `hot_routes_accept_aliases` |
| POST | `/v1/namespaces/{ns}/collections/{c}/warm` | app | `loams.collection.v1.CollectionService/WarmCollection` | `hot::warm_local` or the owner's `/internal/v1/hot/warm` | `hot_http.rs::warm_prefetches_an_unpinned_collection`, `loams_warm_posts_the_warm_request` |
| POST | `/v1/namespaces/{ns}/query` | app | `loams.collection.v1.QueryService/Search` | `CollectionService::search` | `native_query.rs` (6 tests), `hot_http.rs::the_hot_off_header_gives_identical_results_and_reports_none` |
| POST | `/v1/namespaces/{ns}/sql` | app | `loams.sql.v1.SqlService/Query` | `CollectionService::sql_context_with`, `loams_query::sql::run_read_only` | `native_sql.rs::sql_over_http_returns_columns_and_rows`, `sql_ddl_is_400` |

Router-level behaviour pinned by `http.rs::framework_rejections_use_the_json_error_body` (the `no_route` fallback and `method_not_allowed`) is replaced by connect-rust's error envelope for RPC paths; the fallback stays for unknown paths.

As built in Task 1 (2026-10-03): none of these rows is gone. Every route above still answers as it did, **and** the same port now serves the Connect protocol, gRPC and gRPC-Web beside them, plus `grpc.health.v1` and (in `loams dev`) `grpc.reflection.v1`. The RPC paths are registered as their own axum routes rather than as the router's fallback, precisely so the `no_route` fallback keeps its JSON body for a path neither API knows (ruling 1.1 of the [plan](../plans/2026-10-02-api1-unified-connect.md)). One application RPC exists so far, `loams.instance.v1.InstanceService/GetInstance`, and its sibling `WhoAmI` answers `unimplemented` with reason `not_implemented` because there is no authentication on this port yet. The one catalogue package whose engine is not in this build, `loams.live.v1`, answers `unimplemented` with reason `feature_not_in_variant`. Tasks 2–8 add one RPC per row above and Task 9 deletes the row.

## Internal node routes (`crates/loams/src/api/internal.rs`, `loams_hot::READS_PATH`)

Merged outside `HotLayer` on every node (`api::router` and `api::internal_router`). Unauthenticated today (M1 overview §6.9).

| Method | Route | Kind | Target RPC | Internal call | Existing tests |
|---|---|---|---|---|---|
| POST | `/internal/v1/reads/{op}` | internal | `loams.internal.v1.ForwardService/Read` | `loams_hot::serve_forwarded` | `cluster.rs` (forwarded reads), `loams-hot` `remote` tests |
| GET | `/internal/v1/node/stats` | internal | `loams.internal.v1.NodeService/GetNodeStats` | `ForwardStats::snapshot`, `HotTierImpl::counters`, `NodeInfo::meta` | `cluster.rs` |
| POST | `/internal/v1/hot/status` | internal | `loams.internal.v1.HotService/GetHotStatus` | `hot::local_status` | `cluster.rs`, `hot_http.rs` (owner path) |
| POST | `/internal/v1/hot/warm` | internal | `loams.internal.v1.HotService/Warm` | `hot::warm_local` | `cluster.rs`, `hot_http.rs` (owner path) |

## Cluster bootstrap (`crates/loams/src/server.rs`)

| Method | Route | Kind | Target RPC | Internal call | Existing tests |
|---|---|---|---|---|---|
| GET | `/health` | protocol | kept | none (200, before the late router is ready) | `cluster.rs` |

## Metastore Raft transport (`crates/loams-meta/src/rpc.rs`)

Served on the cluster listener before the API router (`meta_rpc::router`). See ruling 0.3.

| Method | Route | Kind | Target RPC | Internal call | Existing tests |
|---|---|---|---|---|---|
| POST | `/internal/v1/raft/append` | raft | `loams.internal.v1.RaftService/AppendEntries` | `MetaNode` openraft `append_entries` | `loams-meta` network tests, `cluster.rs` |
| POST | `/internal/v1/raft/vote` | raft | `loams.internal.v1.RaftService/Vote` | openraft `vote` | `loams-meta` network tests |
| POST | `/internal/v1/raft/pre-vote` | raft | `loams.internal.v1.RaftService/PreVote` | openraft pre-vote | `loams-meta` network tests |
| POST | `/internal/v1/raft/snapshot` | raft | `loams.internal.v1.RaftService/InstallSnapshot` | openraft `install_full_snapshot` | `loams-meta` network tests |
| POST | `/internal/v1/meta/write` | raft | `loams.internal.v1.MetaService/Write` | `MetaNode::client_write` | `loams-meta` network tests, `cluster.rs` |
| POST | `/internal/v1/meta/read-index` | raft | `loams.internal.v1.MetaService/ReadIndex` | `MetaNode::read_index` | `loams-meta` network tests |
| POST | `/internal/v1/meta/join` | raft | `loams.internal.v1.MetaService/Join` | `MetaNode::join` | `cluster.rs` |
| POST | `/internal/v1/meta/leave` | raft | `loams.internal.v1.MetaService/Leave` | `MetaNode::leave` | `cluster.rs` |
| POST | `/internal/v1/meta/status` | raft | `loams.internal.v1.MetaService/Status` | `MetaNode` metrics | `cluster.rs` |

## Console contract (`api/console/openapi.json`, served by `loams-console-mock`)

The real server does not serve `/api/v1` yet (the gateway work is AP4/MT1); only `loams-console-mock` does, from the contract. Tests: `crates/loams-console-mock/tests/contract.rs` (`every_operation_has_a_mock_and_every_mock_an_operation`, `every_mocked_body_matches_its_response_schema`, `the_server_answers_from_the_seed`) and the console's `web/apps/console` tests over `openapi-fetch`. Services are in `loams.admin.v1` unless noted.

| Method | Route | Kind | Target RPC | Internal call | Existing tests |
|---|---|---|---|---|---|
| GET | `/api/v1/instance` | app | `loams.instance.v1.InstanceService/GetInstance` (exists, AP0) | console mock seed | console mock contract tests |
| POST | `/api/v1/setup` | app | `loams.auth.v1.AuthService/CompleteSetup` | console mock seed | console mock contract tests |
| GET | `/api/v1/session` | app | `loams.auth.v1.AuthService/GetSession` | console mock seed | console mock contract tests |
| POST | `/api/v1/session` | app | `loams.auth.v1.AuthService/CreateSession` | console mock seed | console mock contract tests |
| DELETE | `/api/v1/session` | app | `loams.auth.v1.AuthService/DeleteSession` | console mock seed | console mock contract tests |
| GET | `/api/v1/auth/oidc/{provider}/start` | protocol | kept (browser redirect, D602); `loams.auth.v1.AuthService/ListProviders` lists providers | console mock seed | console mock contract tests |
| GET | `/api/v1/org` | app | `OrgService/GetOrg` | console mock seed | console mock contract tests |
| PATCH | `/api/v1/org` | app | `OrgService/UpdateOrg` | console mock seed | console mock contract tests |
| GET | `/api/v1/org/members` | app | `OrgService/ListMembers` | console mock seed | console mock contract tests |
| DELETE | `/api/v1/org/members/{user}` | app | `OrgService/RemoveMember` | console mock seed | console mock contract tests |
| GET | `/api/v1/org/invitations` | app | `OrgService/ListInvitations` | console mock seed | console mock contract tests |
| POST | `/api/v1/org/invitations` | app | `OrgService/CreateInvitation` | console mock seed | console mock contract tests |
| GET | `/api/v1/teams` | app | `OrgService/ListTeams` | console mock seed | console mock contract tests |
| POST | `/api/v1/teams` | app | `OrgService/CreateTeam` | console mock seed | console mock contract tests |
| GET | `/api/v1/teams/{team}` | app | `OrgService/GetTeam` | console mock seed | console mock contract tests |
| POST | `/api/v1/teams/{team}/members` | app | `OrgService/AddTeamMember` | console mock seed | console mock contract tests |
| GET | `/api/v1/projects` | app | `ProjectService/ListProjects` | console mock seed | console mock contract tests |
| POST | `/api/v1/projects` | app | `ProjectService/CreateProject` | console mock seed | console mock contract tests |
| GET | `/api/v1/projects/{project}` | app | `ProjectService/GetProject` | console mock seed | console mock contract tests |
| GET | `/api/v1/projects/{project}/access` | app | `ProjectService/ListAccess` | console mock seed | console mock contract tests |
| POST | `/api/v1/projects/{project}/access` | app | `ProjectService/GrantAccess` | console mock seed | console mock contract tests |
| GET | `/api/v1/projects/{project}/environments` | app | `ProjectService/ListEnvironments` | console mock seed | console mock contract tests |
| POST | `/api/v1/projects/{project}/environments` | app | `ProjectService/CreateEnvironment` | console mock seed | console mock contract tests |
| GET | `/api/v1/projects/{project}/environments/{environment}` | app | `ProjectService/GetEnvironment` | console mock seed | console mock contract tests |
| GET | `/api/v1/projects/{project}/environments/{environment}/usage` | app | `ProjectService/GetEnvironmentUsage` | console mock seed | console mock contract tests |
| GET | `/api/v1/projects/{project}/agents` | app | `AgentService/ListAgents` | console mock seed | console mock contract tests |
| POST | `/api/v1/projects/{project}/agents` | app | `AgentService/CreateAgent` | console mock seed | console mock contract tests |
| GET | `/api/v1/agents/{agent}` | app | `AgentService/GetAgent` | console mock seed | console mock contract tests |
| PATCH | `/api/v1/agents/{agent}` | app | `AgentService/UpdateAgent` | console mock seed | console mock contract tests |
| POST | `/api/v1/agents/{agent}/suspend` | app | `AgentService/SuspendAgent` | console mock seed | console mock contract tests |
| POST | `/api/v1/agents/{agent}/resume` | app | `AgentService/ResumeAgent` | console mock seed | console mock contract tests |
| GET | `/api/v1/agents/{agent}/tokens` | app | `AgentService/ListAgentTokens` | console mock seed | console mock contract tests |
| GET | `/api/v1/agents/{agent}/trust-policies` | app | `AgentService/ListTrustPolicies` | console mock seed | console mock contract tests |
| POST | `/api/v1/agents/{agent}/trust-policies` | app | `AgentService/CreateTrustPolicy` | console mock seed | console mock contract tests |
| DELETE | `/api/v1/tokens/{jti}` | app | `KeyService/RevokeToken` | console mock seed | console mock contract tests |
| GET | `/api/v1/projects/{project}/service-accounts` | app | `AgentService/ListServiceAccounts` | console mock seed | console mock contract tests |
| POST | `/api/v1/projects/{project}/service-accounts` | app | `AgentService/CreateServiceAccount` | console mock seed | console mock contract tests |
| GET | `/api/v1/projects/{project}/environments/{environment}/keys` | app | `KeyService/ListKeys` | console mock seed | console mock contract tests |
| POST | `/api/v1/projects/{project}/environments/{environment}/keys` | app | `KeyService/CreateKey` | console mock seed | console mock contract tests |
| DELETE | `/api/v1/keys/{key}` | app | `KeyService/RevokeKey` | console mock seed | console mock contract tests |
| GET | `/api/v1/audit` | app | `AuditService/ListAuditEvents` (and the `WatchAuditEvents` stream) | console mock seed | console mock contract tests |
| POST | `/api/v1/oauth/token` | protocol | kept (RFC 6749/8693, D602) | console mock seed | console mock contract tests |
| GET | `/api/v1/oauth/authorize` | protocol | kept (RFC 6749/7636, D602) | console mock seed | console mock contract tests |
| POST | `/api/v1/oauth/consent` | app | `loams.auth.v1.AuthService/DecideConsent` | console mock seed | console mock contract tests |
| GET | `/.well-known/oauth-protected-resource` | protocol | kept (RFC 9728, D602) | console mock seed | console mock contract tests |
| GET | `/.well-known/oauth-authorization-server` | protocol | kept (RFC 8414, D602) | console mock seed | console mock contract tests |
| GET | `/.well-known/jwks.json` | protocol | kept (RFC 7517, D602) | console mock seed | console mock contract tests |

## Out of scope: compatibility surfaces (D603)

Not mapped and not changed: the Qdrant REST and gRPC gateway (`loams-qdrant`), the Elasticsearch gateway (`loams-es`), Flight SQL (`server.rs`, `--flight-sql-listen`), Postgres wire (`pg`), MySQL wire (`mysql_wire`), the Resonate HTTP protocol (`loams-durable`), and the tonic `loams.stream.v1` listener for Dapr adapters (`loams-stream-grpc`, `--stream-grpc-listen`), which Task 6 moves onto connect-rust without changing its wire contract.

## Rulings made in this reconciliation

| # | Ruling | Why |
|---|---|---|
| 0.1 | §44 §3 counts 22 native routes; the as-built router has 27 `/v1` method/path pairs: the 25 `api::router` registers itself, plus `hot` and `warm`, which `hot::routes()` registers separately and `api::router` merges (so they are counted here too, not added on top). `/health`, `/ready` and the `/internal/*` routes are not app routes and are not in the 27. The table above is authoritative, and `route_map_covers_every_route` asserts the count so it cannot drift. | Counted from `api::router` and `hot::routes` |
| 0.2 | The console contract is served only by `loams-console-mock`; there is no `/api/v1` handler in the server to port. Task 7 builds `loams.admin.v1`/`loams.auth.v1` against the mock's seed (as the plan says: "tests are the console mock's") and the console moves to Connect in Task 9. | `grep -r /api/v1 crates` finds only the mock |
| 0.3 | The metastore Raft transport (`/internal/v1/raft/*`, `/internal/v1/meta/*`) is node-to-node HTTP like the other `/internal` routes, so it is mapped to `loams.internal.v1`. Task 8 moves the four `api/internal.rs` routes first; the Raft transport moves in the same task only if `loams-meta`'s `HttpTransport` can switch without changing openraft's wire types, else it is a follow-up issue (it is never on the public port and never in an SDK, D607). | §44 §5.1 row `/internal/*`; keep Task 8 one PR |
| 0.4 | Hot and warm become `CollectionService/SetHot` and `WarmCollection`, as §44 §5.1; the native `/v1/namespaces/{ns}/collections/{c}/hot` GET does not exist (the hot status is part of `GetCollection`). | As built |
