# AP1a — The Console as a cordis Application Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Execute task by task, test first. Each task lists the interfaces it must produce and the tests that must exist and pass before it is done. Where this plan gives exact values (package names, slot names, manifest fields, CSP strings), use them verbatim. The code is not pre-written in this plan (M0.3 Ruling 1).

> **Status: In progress** (2026-10-02: scaffolded, see Rulings E1–E9; planned 2026-10-01). **Slot: track AP, after AP0 Task 6 for the `rpc.*` services; Tasks 0–3 need nothing but `main`.** Branches `ap1a-t<N>`, stacked; PRs target `main`. AP1 starts after Task 3. Tasks 6–7 are complete against `loams-apps-mock`; against a real server they need the unified auth plan (D111, Q30) for vended tokens and AP4 for `PluginService`. Packages use the `@loams/*` scope (D400).

**Goal:** Turn `web/apps/console` into a **cordis v4 application** (§37 §5, D422–D428): a small host that boots a cordis `Context` and loader from a catalog, provides core services (transport, typed Connect clients, the OpenAPI client, session, platform, slots, router, settings, flags, i18n), and loads everything else as plugins with manifests, declared services, slots and permissions. The same host runs at `/ui` in the browser and inside the Tauri shell (AP1). Today's pages become first-party plugins with no change in behaviour; approvals, operations, devices, jobs and connectors arrive as new plugins; third-party plugins run in sandboxed iframes behind a permission-checked bridge.

**Architecture:**
- **`web/packages/cordis`** (`@loams/cordis`): a facade that re-exports the pinned `cordis` and `@cordisjs/plugin-loader` and owns the patch log (§37 §14 risk 1). Every other package imports cordis only from here.
- **`web/packages/console-host`** (`@loams/console-host`): boot (manifest, module table with SRI, loader, catalog), the core services, the all-fibers sweep and the diagnostics page.
- **`web/packages/slots`** (`@loams/slots`): the slot service, `SlotMap` typing, the React renderer (`useSyncExternalStore`), per-entry error boundaries.
- **`web/packages/forms`** (`@loams/forms`): one JSON-Schema form renderer for plugin settings and §33 connector configs.
- **`web/packages/platform-web`** (`@loams/platform-web`): the browser's `platform` service. (`@loams/platform-tauri` is AP1's.)
- **`web/plugins/*`**: one package per plugin (`@loams/plugin-shell`, `-rpc`, `-identity`, `-agents`, `-keys`, `-audit`, `-collections`, `-approvals`, `-operations`, `-devices`, `-jobs`, `-connectors`, `-plugins`, `-sandbox`).
- **`web/apps/console`**: the browser build; `catalog/base.yml`; Vite config producing the host bundle, one ESM bundle per plugin, the import map and `plugins/manifest.json`.

**Tech Stack:** pnpm 11, Node ≥ 22, TypeScript 5.9, Vite 8, React 19.3, React Router 8 (data router), Biome; `cordis` **4.0.0-rc.10** and `@cordisjs/plugin-loader` **1.0.0-rc.7** pinned exactly (MIT; Q427); Standard Schema with zod 4 for plugin `Config` and JSON Schema output; `@bufbuild/protobuf` 2.16, `@connectrpc/connect` 2.2, `@connectrpc/connect-web` 2.2; `openapi-fetch` 0.17 (as on `main`); Vitest, Testing Library, Playwright. Task 0 confirms every version.

**Spec:**
- [§37](../design/37-desktop-and-mobile-apps.md) §5 (all of it), §8 (protos), §10 (open core), §11.
- [AP0](2026-10-01-ap0-app-protos.md) (the services `rpc.*` wraps; the mock).
- §19 §3 (one console, `/ui`, `GET /api/v1/instance`), §19 §5.1–§5.2 (actions; vending), §19 P9 (OpenAPI); §33 D352 (connector manifests and their JSON Schema `config`); §26 D206.
- The harness's client half (MIT), read as reference: `packages/client/web/src/boot.tsx`, `packages/client/ui-slots/`, the `web-react` renderer, `.agents/notes/implemented/architecture/2026-07-23-client-plugin-loading-model.md`, `vendor/loader/src/config/*.ts`, `vendor/README.md` (its modification log). Nothing copied unless Task 1 records it in `THIRD_PARTY_NOTICES.md`.

## Global Constraints

- **Behaviour parity first.** After Task 5 the browser console does exactly what it does on `main` against `loams-console-mock`; Playwright parity tests prove it before any new page lands.
- **CSP without `unsafe-eval` or remote sources** in both the engine's `/ui` responses and the Tauri bundle: `script-src 'self'` (plus the import map's hash). No `!!js` in any catalog; the loader refuses it.
- **No value imports between plugins.** Shared code lives in `web/packages/*`; plugins cooperate through services. A build check fails otherwise.
- **Platform externals are shared, never bundled**: `react`, `react-dom`, `react-router`, `@loams/cordis`, `@loams/slots`, `@loams/ui`, `@loams/forms`, `@bufbuild/protobuf`, `@connectrpc/connect`, `@loams/proto`.
- **Air-gapped** (§19 §3): every asset bundled; nothing fetched from the internet at run time.
- **Never use** `@koishijs/plugin-console` or `@koishijs/client` (AGPL-3.0 per npm metadata, and Vue).
- **Commit areas:** `web`, `proto`, `mock`, `ci`, `docs`.

## Rulings made while writing this plan

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| 1 | **Depend on npm `cordis` pinned exactly, behind `@loams/cordis`, with `pnpm patch` for fixes**, each logged in `web/packages/cordis/PATCHES.md` (upstream issue or PR, reason, removal condition). Vendor into `web/vendor/cordis` only when a patch lives longer than 30 days or upstream is silent for 90 (Q427) | Bus factor one and an rc API: the facade makes vendoring a one-package change; the harness's 18-entry modification log is the model | A breaking rc bump costs a facade update; pinned versions mean we choose when |
| 2 | **The catalog is `loams.yml` in cordis v4 loader format** (`id`, `name`, `config`, `group`, `disabled`, `inject`), composed as base + patch lists (`- id:` replaces a row's config; `- insert:` adds rows), the harness's profile composition. Composition runs at build time for bundled sets and at boot for instance-installed rows | One format the stock loader understands; editions are patch files (D428) | None |
| 3 | **Manifests live in `package.json` under `loams.plugin`** with the fields of §37 §5.3; a JSON Schema for the block is at `web/packages/console-host/plugin-manifest.schema.json` and every plugin's build validates against it | One place, readable by npm tooling and the install screen | None |
| 4 | **Bundles are native ESM loaded with `import()`, platform externals through an import map**, not the harness's classic-script factory table | Simpler with Vite; works in the browser and in Tauri's webview; SRI on every module | Import maps need a CSP hash; Task 0's spike verifies Tauri's webviews accept it |
| 5 | **Slot kinds `single`, `list`, `keyed`** (the harness's `chain` is not needed yet); scopes are `root` and `environment` (the harness's `session` maps to Loams's selected environment) | The §37 §5.5 catalog needs no more | Adding a kind later is additive |
| 6 | **`rpc.<service>` providers gate on `GetInstance.api_versions`**; a provider for a package the instance does not serve is never provided, so dependents stay pending silently (not reported as failures by the sweep) | Pages appear by themselves on instances that serve their API (jobs before and after J1) | None |
| 7 | **Router:** `createBrowserRouter` with `basename: '/ui'` in the browser; in Tauri, the browser router with `basename: '/'` if AP1 Task 0's spike shows Tauri serves `index.html` for unknown paths, otherwise `createHashRouter`. Pages register through `ctx.router.page(...)`, never by editing a route table | Deep links and reloads must work in both shells | A hash router changes URLs in the desktop only |
| 8 | **Trust tier is decided by the host, not claimed by the manifest**: `core` = in the bundle and in a fixed list; `first-party` = bundled, or a package whose npm provenance attestation names one of the build's `trustedPublishers` (always `github.com/ostrium-labs/*`; a build may add more, which is how the private hosted plugins of D220 count as first-party in the hosted build without this repository knowing them); everything else `third-party` | A manifest cannot elevate itself; the open/private split (D220, which stands) needs an extension point, not knowledge of the private set | Q426 may narrow first-party to an explicit allowlist |
| 9 | **Third-party plugins are disabled until vended tokens exist** (the auth plan); in development a local-path plugin runs as `first-party` under a red "unverified plugin" banner | Without attenuation there is no server-side boundary | Plugin authors test as first-party before the auth plan |

## Review Focus

1. **A plugin cannot exceed its declaration.** Guard proxy and bridge. Tests: Task 6 (`first_party_sees_only_injected_services`, `third_party_call_outside_permissions_is_refused`, `iframe_cannot_reach_network`, `iframe_cannot_read_parent_dom`).
2. **Disposal is complete.** Disabling a plugin removes its routes, slot entries, streams and listeners. Tests: Task 3 (`dispose_removes_slot_entries_and_routes`), Task 4 (`dispose_aborts_server_streams`).
3. **No eval path.** Tests: Task 2 (`catalog_with_js_tag_is_refused`), Task 0's CSP check.
4. **Parity with today's console.** Tests: Task 5 (the Playwright parity suite).

## File structure

```
web/packages/cordis/{package.json,src/index.ts,PATCHES.md}
web/packages/console-host/{package.json,src/{boot.ts,modules.ts,manifest.ts,catalog.ts,services/*.ts,sweep.ts,diagnostics.tsx},plugin-manifest.schema.json,test/**}
web/packages/slots/{package.json,src/{service.ts,types.ts,render.tsx,boundary.tsx},test/**}
web/packages/forms/{package.json,src/**,test/**}
web/packages/platform-web/{package.json,src/index.ts}
web/plugins/<name>/{package.json,src/client.ts(x),src/config.ts,test/**}      # shell, rpc, identity, agents, keys, audit, collections,
                                                                               # approvals, operations, devices, jobs, connectors, plugins, sandbox
web/apps/console/{vite.config.ts,catalog/base.yml,index.html,scripts/{build-plugins.mjs,check-imports.mjs,gen-slot-catalog.mjs},e2e/**}
proto/loams/console/v1/console.proto                                            # PluginService (Task 7)
crates/loams-apps-mock/src/services/console.rs                                # Task 7
docs/guides/console-plugins.md  docs/design/37-desktop-and-mobile-apps.md  THIRD_PARTY_NOTICES.md  docs/plans/README.md  CHANGELOG.md
```

### Task 0: Reconcile and spike

**Files:** read `web/` as on `main`, AP0 as merged, the harness files under Spec. Fill "Rulings made during execution".

**Spike (throwaway, in the scratchpad):** a Vite app that boots `cordis@4.0.0-rc.10` with `@cordisjs/plugin-loader@1.0.0-rc.7` in the browser using a custom `loader.internal` over `import()` and an import map, loads two plugins (one injects a service the other provides, provided after a delay), disposes one, and reloads it. Run it under the CSP of §37 §6.3 (no `unsafe-eval`) in Chromium, Firefox, WebKit (Playwright) and a Tauri 2 webview on Linux. Record: which Node shims the loader needs, whether anything evaluates strings, bundle sizes, and whether `ctx.effect` disposal ran for every registration.

**Checks:** the latest cordis rc and its changelog since rc.10; the loader's entry-list format at that version; zod 4's JSON Schema output for Standard Schema configs.

**Commit:** `docs: reconcile AP1a with main and record the cordis spike`.

### Task 1: `@loams/cordis` and the host's boot

**Files:** `web/packages/cordis/**`, `web/packages/console-host/src/{boot.ts,modules.ts,manifest.ts,sweep.ts,diagnostics.tsx}`, tests.

**Produces:**

```ts
// @loams/console-host
export interface BootManifestRow { id: string; name: string; url: string; integrity: string; rev: string;
  tier: 'core' | 'first-party' | 'third-party'; inject: string[]; slots: string[] }
export interface BootManifest { console: string /* semver */; catalog: CatalogEntry[]; rows: BootManifestRow[] }
export interface BootOptions { manifestUrl: string; platform: PlatformPlugin; root: HTMLElement; sweepAfterMs?: number /* 10_000 */ }
export async function boot(options: BootOptions): Promise<ConsoleHandle>;
export interface ConsoleHandle { ctx: Context; dispose(): Promise<void>; pending(): PendingReport[] }
```

**Semantics:** §37 §5.2. The module table resolves a row's `name` to `import(url)`, and **the integrity check is bound to the bytes the browser executes**: every plugin module URL is listed in the import map's `integrity` section, which the browser enforces on the module fetch itself, dynamic imports included. Hashing a separate `fetch` and then calling `import(url)` is forbidden (the two reads can differ). Task 0 records import-map integrity support in each target engine (Chromium, Firefox, WebKit, WebKitGTK, WKWebView, WebView2); where it is missing, the host imports the verified bytes through a `blob:` URL created from the hashed response, and that build alone adds `blob:` to `script-src`. In Tauri, bundled modules are local assets of the signed app. The sweep lists fibers still pending with the services they wait for, excluding Ruling 6's silent gates.

**Tests:** `boot_loads_rows_in_dependency_order`; `integrity_mismatch_refuses_row`; `different_bytes_on_second_read_are_refused` (a test server returns the verified bytes once and different bytes after); `pending_fiber_reported_with_missing_services`; `rpc_gate_is_not_reported`; `dispose_disposes_every_fiber`.

**Commit:** `web: boot the console as a cordis context`.

### Task 2: Plugin builds, manifests and the catalog

**Files:** `web/apps/console/{vite.config.ts,scripts/build-plugins.mjs,scripts/check-imports.mjs}`, `web/packages/console-host/src/catalog.ts`, `plugin-manifest.schema.json`, tests.

**Produces:**

```ts
export function composeCatalog(base: CatalogEntry[], ...patches: CatalogPatch[]): CatalogEntry[];   // Ruling 2
export function parseCatalog(yaml: string): CatalogEntry[];                                         // refuses !!js and unknown keys
export function validateManifest(pkg: unknown): PluginManifest;                                     // the schema of §37 §5.3
```

**Semantics:** each `web/plugins/*` package builds to one ESM bundle with platform externals; `build-plugins.mjs` writes `dist/plugins/<id>/<rev>/client.js`, the import map, and `dist/plugins/manifest.json` (rows with SRI hashes, from the composed catalog). `check-imports.mjs` fails on a value import between plugins and on a bundled copy of any platform external.

**Semantics (registries):** `build-plugins.mjs` takes `--trusted-publisher <repo-glob>` (repeatable) and honours scoped registries from `.npmrc`, so a build can pull a scope from a private registry (§37 §5.7–§5.8). Nothing in this repository names a private scope.

**Tests:** `scoped_private_registry_resolves_from_npmrc` (a local Verdaccio in the test); `compose_patch_replaces_config`; `compose_insert_adds_rows`; `catalog_with_js_tag_is_refused`; `manifest_schema_rejects_unknown_tier`; `cross_plugin_value_import_fails_build`; `bundled_react_fails_build`.

**Commit:** `web: build plugins, manifests and the catalog`.

### Task 3: Slots, the shell and the router

**Files:** `web/packages/slots/**`, `web/plugins/shell/**`, tests.

**Produces:**

```ts
// @loams/slots
export interface SlotMap { root: { kind: 'single'; props: {} }; 'console.nav': { kind: 'list'; props: { environment: EnvironmentRef } };
  'console.page': { kind: 'keyed'; props: { params: Record<string,string>; environment: EnvironmentRef } }; /* §37 §5.5, extended by declaration merging */ }
export interface SlotService {
  register<N extends keyof SlotMap>(spec: { name: N; id?: string; key?: string; order?: number; children?: ChildSlots }, component: ComponentFor<N>): () => void;
  render<N extends keyof SlotMap>(name: N, props: SlotMap[N]['props'], key?: string): ReactNode;
}
// @loams/plugin-shell provides `router`, `settings`, `i18n`
export interface RouterService { page(spec: { id: string; path: string; title: string; nav?: { group: string; order: number; icon?: string } }, component: ComponentType<PageProps>): () => void; navigate(to: string): void }
```

**Semantics:** §37 §5.5. `register` is called inside `ctx.effect`, so disposal removes the entry. Each entry renders inside its own error boundary that shows the plugin id and a reload button. Components receive props only, never `ctx` (the harness rule). `@loams/plugin-shell` renders the layout into `root`, the navigation from `console.nav`, and the routes registered through `router.page` (Ruling 7). The slot catalog is generated into `docs/guides/console-plugins.md` by `gen-slot-catalog.mjs`, and CI fails if a slot's props type changes without a host version bump.

**Tests:** `register_and_render_list_in_order`; `keyed_slot_renders_one_key`; `entry_error_is_contained`; `dispose_removes_slot_entries_and_routes`; `components_never_receive_ctx` (a type test); `slot_props_change_requires_version_bump` (the CI check against a fixture).

**Commit:** `web: add slots, the shell and the router`.

### Task 4: Core services: transport, rpc, api, session, flags, settings, platform

**Files:** `web/packages/console-host/src/services/*.ts`, `web/plugins/rpc/**`, `web/plugins/identity/src/session.ts`, `web/packages/forms/**`, `web/packages/platform-web/**`, tests.

**Produces:**

```ts
export interface PlatformService { kind: 'web' | 'desktop'; fetch: typeof fetch; openExternal(url: string): Promise<void>;
  notify(n: { title: string; body: string; route?: string }): Promise<void>; clipboardWrite(text: string): Promise<void> }
// transport: Transport (connect-es); replaced when the environment changes
// rpc.<name>: Client<typeof Service> for instance, approvals, operations, devices, notifications, jobs, live, flow, console
// api: openapi-fetch Client<paths> over platform.fetch
export interface SessionService { principal(): Principal | undefined; environment(): EnvironmentRef | undefined; select(env: string): Promise<void> }
export interface FlagsService { edition: 'oss' | 'cloud' | 'byoc'; features: Record<string, boolean>; apiVersions: string[]; has(api: string): boolean }
```

**Semantics:** §37 §5.4, Ruling 6. `transport` uses `createConnectTransport({ baseUrl, useBinaryFormat: true, fetch: ctx.platform.fetch })`. A server stream opened by a plugin goes through `ctx.rpc.watch(fn)`, a helper that ties an `AbortController` to the calling fiber's disposal. `settings` renders each plugin's `Config` (JSON Schema) with `@loams/forms`, which also renders §33 connector configs (Task 9).

**Tests:** `rpc_service_provided_only_when_api_listed`; `environment_switch_reloads_rpc_dependents`; `dispose_aborts_server_streams` (against `loams-apps-mock` `WatchApprovals`); `api_client_uses_platform_fetch`; `settings_form_round_trips_schema_defaults`; `forms_render_connector_schema_fixture` (a §33 manifest fixture).

**Commit:** `web: provide transport, Connect clients and core services`.

### Task 5: Today's pages as first-party plugins, with parity

**Files:** `web/plugins/{identity,agents,keys,audit,collections}/**`, `web/apps/console/catalog/base.yml`, `web/apps/console/e2e/parity/*.e2e.ts`; delete the moved files from `web/apps/console/src/pages`.

**Semantics:** move each page of `web/apps/console/src/pages` into its plugin, registering routes and nav entries through the services; replace `window.location.origin` and the hard-coded `/ui` with `api`, `transport` and `router.navigate`; keep the cookie session and CSRF in the browser (`@loams/platform-web`). No visual or behavioural change.

**Tests:** the Playwright parity suite, recorded on `main` before the move and replayed after it against `loams-console-mock`: sign-in, setup, consent, projects, environments, agents (create, suspend, tokens), access, teams, members, audit, settings, collections listing. Each test asserts the same requests and the same rendered text.

**Commit:** `web: move the console's pages into first-party plugins`.

### Task 6: Trust tiers, the guard proxy and the iframe sandbox

**Files:** `web/packages/console-host/src/{tiers.ts,guard.ts,bridge.ts}`, `web/plugins/sandbox/**` (the in-frame cordis runtime), `crates/loams-apps-mock` (a token-exchange endpoint for vending, `POST /api/v1/oauth/token` with `grant_type=urn:ietf:params:oauth:grant-type:token-exchange`), tests.

**Produces:**

```ts
export function tierOf(row: BootManifestRow, provenance: ProvenanceInfo | undefined, trustedPublishers: string[]): Tier;   // Ruling 8
export function guard(ctx: Context, inject: string[]): Context;                                         // exposes only injected services
export interface BridgePolicy { pluginId: string; version: string; permissions: Permission[]; services: string[] }
export function mountSandboxed(row: BootManifestRow, policy: BridgePolicy, host: HostServices): SandboxHandle;
```

**Semantics:** §37 §5.6, Ruling 9. The frame is `<iframe sandbox="allow-scripts" src=…>` served with `Content-Security-Policy: default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src data: blob:; connect-src 'none'`; on desktop from `loams-plugin://` (AP1). The bridge is a typed `postMessage` RPC (one `MessageChannel` per frame); each call names a service and method, is checked against `services` and `permissions`, and is performed by the host with the plugin's **vended token** (scopes = permissions ∩ the user's rights, `act` = `plugin:<id>@<version>`, TTL 15 min, renewed while active). Slot registrations from the frame become host placeholders that render the frame at the registered size. Third-party rows are skipped unless `flags.features['console.third_party_plugins']` is on (it is off until the auth plan).

**Tests:** `first_party_sees_only_injected_services`; `manifest_cannot_claim_first_party`; `third_party_call_outside_permissions_is_refused`; `third_party_token_is_attenuated` (decode the mock's issued token: `scp`, `act`, `exp`); `iframe_cannot_reach_network` (Playwright: a `fetch` in the frame fails under CSP); `iframe_cannot_read_parent_dom`; `third_party_disabled_without_flag`.

**Commit:** `web: isolate third-party plugins in sandboxed frames`.

### Task 7: The plugin service and live reload

**Files:** `proto/loams/console/v1/console.proto`, `crates/loams-apps-mock/src/services/console.rs`, `web/plugins/plugins/**` (the management page), `web/packages/console-host/src/live.ts`, `web/apps/console/scripts/dev.mjs` (`pnpm loams-console dev --plugin <path>`), tests.

**Produces:**

```proto
service PluginService {
  rpc ListPlugins(ListPluginsRequest) returns (ListPluginsResponse) { option idempotency_level = NO_SIDE_EFFECTS; }
  rpc InstallPlugin(InstallPluginRequest) returns (InstallPluginResponse);     // npm name@version or an uploaded tarball ref; owner only
  rpc SetPluginEnabled(SetPluginEnabledRequest) returns (SetPluginEnabledResponse);
  rpc UninstallPlugin(UninstallPluginRequest) returns (UninstallPluginResponse);
  rpc WatchManifest(WatchManifestRequest) returns (stream WatchManifestResponse);  // snapshot, changes, heartbeat (AP0 Ruling 3)
}
// Plugin: id, name, version, tier, permissions, publisher, provenance (repo, workflow, verified), enabled, installed_by, installed_at.
```

**Semantics:** §37 §5.7. The host applies `WatchManifest` changes as fiber add, replace or dispose without a page reload. Installing in a `protected` environment returns an operation awaiting approval (D435). The development command runs Vite with the plugin linked; a change to a component is HMR; a change to `apply` refreshes the plugin's fiber (dispose, re-import with a cache-busting `rev`, load), the harness's driver.

**Tests:** `install_appears_without_reload`; `disable_disposes_fiber`; `upgrade_replaces_fiber_and_dependents_reload`; `install_in_protected_env_needs_approval` (mock); `dev_apply_change_refreshes_fiber`; `buf lint` for `console.proto`.

**Commit:** `web: install, enable and reload plugins live`.

### Task 8: New first-party plugins: approvals, operations, devices, jobs

**Files:** `web/plugins/{approvals,operations,devices,jobs}/**`, tests.

**Semantics:** `approvals` (inject `rpc.approvals`, `slots`, `router`, `platform`): the list and detail of §37 §7.3, the generic `approval.renderer`, a `shell.overlay` badge, step-up `SESSION` before deciding (AP0 Ruling 7), and `platform.notify` for new approvals while the console is open. `operations` (`rpc.operations`): lists, detail with progress and cancel, `operation.detail` keyed by kind. `devices` (`rpc.devices`): pairing QR and code (§37 §7.2.1), the device list, revoke, notification preferences. `jobs` (`rpc.jobs`, gated by Ruling 6): queues, counts, jobs, DLQs, schedules, flows, engine runs from `Query` and `Watch` (§26 §6.11), plus an `environment.overview.card`.

**Tests:** per plugin, Vitest with fake services and Playwright against `loams-apps-mock` scenarios: `approval_appears_live_and_decides_once`; `stale_session_requires_step_up`; `operation_progress_updates`; `pairing_qr_matches_schema_v1`; `revoke_device_removes_row`; `jobs_plugin_absent_without_api`.

**Commit:** `web: add the approvals, operations, devices and jobs plugins`.

### Task 9: Connectors, engine views and docs

**Files:** `web/plugins/connectors/**`, `web/plugins/collections` (engine views), `docs/guides/console-plugins.md` (authoring guide, slot catalog, manifest reference, trust tiers), `THIRD_PARTY_NOTICES.md` (cordis MIT), `docs/design/37-desktop-and-mobile-apps.md` (as built), `docs/plans/README.md`, `CHANGELOG.md`.

**Semantics:** `connectors` (inject `rpc.flow` once §33's `FlowService` exists, gated): the connector catalog from `ListConnectors`, a generic `connector.config` entry for every connector id that renders its JSON Schema with `@loams/forms`, `ValidateRoute` before save, and capability badges (§33 D353). Engine views register `engine.view` entries for `es`, `qdrant`, `flight-sql` and `pg` showing endpoints and client snippets. The authoring guide walks through a third-party plugin from `pnpm create` to install.

**Exit criteria:** the parity suite passes; disabling any first-party plugin removes its pages and streams with no reload and no console error; the jobs plugin appears only when the instance lists `loams.jobs.v1`; a third-party sample plugin (in `web/examples/plugin-hello`) runs in a frame, cannot fetch, and its calls carry an attenuated token in the mock; the same host bundle boots in the browser at `/ui` and in AP1's Tauri shell.

**Commit:** `docs: document console plugins and close AP1a`.

## Rulings made during execution

| # | Ruling | Why | Cost if wrong |
|---|---|---|---|
| E1 | **Scaffold first (2026-10-02).** Built: `@loams/cordis`, `@loams/slots`, `@loams/console-host` (catalog, manifest, tiers, guard, permissions, bridge, sandbox, boot, sweep), `@loams/platform-web`, the core plugins `shell`, `rpc`, `identity`, the first-party plugins `stack-status`, `namespaces`, `approvals`, the in-frame runtime `@loams/plugin-sandbox` and the third-party sample `web/examples/plugin-hello`. The cordis console is served at `/ui/cordis.html` beside today's console; Task 5 (today's pages as plugins, the parity suite) moves it to `/ui/` | Unblocks AP1 now without touching today's console | Two entries in one build until Task 5 |
| E2 | **`@cordisjs/plugin-loader` is not used; the host loads the same entry-list format itself** (§37 §14 risk 2's fallback) | The loader imports `node:module`, `node:fs`, `node:path` and evaluates catalog expressions with `new Function` (read in 1.0.0-rc.7's `lib/index.js` and `Entry.evaluate`); cordis core itself imports no Node module and evaluates nothing | A later loader release that drops Node may replace `boot.ts`'s row loop |
| E3 | **Bundled plugins come from a module table of dynamic imports** (`web/apps/console/src/cordis/modules.ts`), which Vite code-splits; the per-plugin ESM bundles, import map with SRI and `manifest.json` rows of Task 2 replace the table without changing `boot()`'s API | No build pipeline is needed for bundled plugins in the scaffold | `integrity_mismatch_refuses_row` and `different_bytes_on_second_read_are_refused` wait for Task 2 |
| E4 | **`session` is provided by `@loams/plugin-identity` as a core plugin**, and `CORE_PLUGINS` lists it | A first-party plugin may provide only `<plugin-id>.*` services (§37 §5.3); `session` is a core contract every page uses | None |
| E5 | **Five read permissions added to the §37 §5.3 list:** `instance:read`, `approvals:read`, `operations:read`, `operations:cancel`, `notifications:read`, and `METHOD_PERMISSIONS` maps every bridged method to one | The bridge needs a permission per method; the AP0 packages have reads that §5.3 did not name | The auth plan's scope names may differ; the map is the one place to change |
| E6 | **Hash routing** (`#/approvals`) in `@loams/plugin-shell`'s router; Ruling 7's browser router with `basename: '/ui'` follows | Works with no server fallback | URLs change once when the browser router lands |
| E7 | **The sandbox frame's CSP is a meta tag in `frame.html` (SANDBOX_CSP, checked by a test), not the iframe `csp` attribute** | Chrome's CSP Embedded Enforcement refuses a frame whose response lacks `Allow-CSP-From` (seen in a headless Chrome smoke run) | The engine must also send the CSP as a header when it serves `/ui/sandbox/` |
| E8 | **The bridge performs third-party calls with the console's own transport until the auth plan can vend attenuated tokens** (TODO in `bridge.ts`); third-party plugins stay off unless the instance sets `console.third_party_plugins`, which only the in-browser demo mock does (Ruling 9) | There is no token to vend before the auth plan | `third_party_token_is_attenuated` waits for the auth plan |
| E10 | **Owner rulings, 2026-10-02: AP1 (the Tauri shell around this console) is dropped.** Loams Desktop is redesigned separately, and Tauri returns as a web bridge, a webview host that agents drive through an MCP toolbox (plan AP1b, being designed). The cordis console stays a browser application; this plan's references to running the same host inside Tauri (Rulings 4 and 7, Tasks 0, 1 and 9) are superseded by those designs | The owner's direction | None for the browser console |
| E9 | **Vitest only (jsdom), no Playwright yet**: the repository had no Playwright setup. `iframe_cannot_reach_network` is covered by the CSP equality test plus a manual headless-Chrome check (the sample plugin's `fetch` fails, `createPairing` is refused, `listApprovals` works) | Keeps the scaffold's CI light | The Playwright parity suite of Task 5 adds the real-browser tests |
| E11 | **Review of #244 (Opus): the frame runtime loads the plugin only after the host has handed over the port, and only when its origin is opaque and it is framed** (`self.origin === 'null'`, `window.parent !== window`); the bridge's method lookup takes own keys only. The console's CSP keeps `frame-src 'self'`, which stops a sandboxed frame from navigating itself to another origin (an exfiltration path `connect-src 'none'` does not cover). When the engine serves `/ui/sandbox/`, it sends SANDBOX_CSP plus `sandbox allow-scripts` as a response header, so the frame is opaque even when opened directly | Before this, plugin code ran before the port was posted (with `targetOrigin '*'`), so a plugin that navigated its frame first could have the port posted to the next document; and `frame.html` opened top-level ran plugin code with the console's origin | A same-origin navigation of the frame is still possible; it reaches only the console's own server |
| E12 | **Review of #244 (Codex): `enable()` rejects third-party rows as well as rows that were not disabled.** Enabling a third-party plugin through the first-party loader stays unavailable until the sandbox lifecycle supports it. | The disabled-status guard introduced during review had dropped the third-party check. A disabled third-party row could load and execute in the host realm, even with the instance feature off; `enable_does_not_run_a_disabled_third_party_plugin_in_the_host` fails before this fix. | Third-party enablement remains a Task 7 sandbox lifecycle feature |
