# web

The Loams console and its design system (design [§19](../docs/design/19-console-identity-and-agents.md)).

| Package | What |
|---|---|
| `packages/ui` (`@loams/ui`) | Tokens, the mark and wordmark, grain textures and React primitives: buttons, status tags, cards, empty states, fields, tables, stats, notices, dialogs, snippets, meters, avatars, formatters. Plain CSS with `loams-` classes; works in Vite and Next.js |
| `apps/console` (`@loams/console`) | One single-page app for OSS, Loams Cloud and BYOC. It reads `GET /api/v1/instance` to know its edition and sign-in methods |

### The cordis console (design §37 §5, AP1a)

The console is becoming a [cordis](https://github.com/cordiverse/cordis) v4 application: a small host boots a cordis context from a plugin catalog, and everything else is a plugin with a manifest, declared services, slots and permissions. Until today's pages move into plugins (AP1a Task 5) it is served beside the current console at `/ui/cordis.html`.

| Package | What |
|---|---|
| `packages/cordis` (`@loams/cordis`) | The only import of cordis (pinned `4.0.0-rc.10`) and its patch log |
| `packages/slots` (`@loams/slots`) | Typed `single`, `list` and `keyed` slots, the React renderer, per-entry error boundaries |
| `packages/console-host` (`@loams/console-host`) | Boot, the `loams.yml` catalog (no YAML tags, so no `!!js`), manifests (`loams.plugin` in package.json, `plugin-manifest.schema.json`), trust tiers, the guard proxy, permissions, the sandbox bridge; `./testing` has an in-memory app-protos mock |
| `packages/platform-web` (`@loams/platform-web`) | `platform` and `transport` in a browser |
| `packages/proto` (`@loams/proto`) | Generated clients for the app protos (AP0) |
| `plugins/*` | `shell` (layout, nav, `router`), `rpc` (`rpc.*` clients, gated on `api_versions`), `identity` (`session`), `stack-status`, `namespaces`, `approvals`, and `sandbox` (the in-frame runtime) |
| `examples/plugin-hello` | A third-party plugin that runs in a sandboxed frame |
| `apps/console/catalog/base.yml` | The `oss` plugin set |

```bash
pnpm dev                                     # then open http://localhost:5173/ui/cordis.html (demo mode, no server)
cargo run -p loams-apps-mock                 # or against the app-protos mock on :8084:
VITE_LOAMS_APPS_URL=http://127.0.0.1:8084 VITE_LOAMS_DEV_BEARER=mock-access-usr_omar pnpm dev
pnpm test                                    # Vitest, every package
```

Third-party plugins are off unless the instance sets the `console.third_party_plugins` feature (the demo mock does): until the unified auth plan can vend attenuated tokens there is no server-side boundary for them. `window.loamsConsole` in the browser console shows the plugin table and `pending()`.

The console speaks the contract in [`api/console/openapi.json`](../api/console/openapi.json). Until the gateway implements it (M2), [`loams-console-mock`](../crates/loams-console-mock) serves it with seed data.

## Develop

```bash
cargo run -p loams-console-mock        # the API mock on :8081 (add --signed-out for the sign-in screens)
cd web && pnpm install && pnpm dev       # the console on http://localhost:5173/ui/
```

`pnpm dev` proxies `/api`, `/v1`, `/.well-known`, `/health` and `/ready` to the mock. To use a real engine instead, set `LOAMS_API=http://127.0.0.1:8080`.

After changing the contract, regenerate the console's types:

```bash
pnpm gen:api
```

## Check and build

```bash
pnpm lint          # Biome
pnpm typecheck     # tsc, every package
pnpm test          # Vitest, every package
pnpm build         # @loams/ui to packages/ui/dist, the console to apps/console/dist
```

The console builds for the base path `/ui`, bundles its fonts and assets, and loads nothing from the internet, so an air-gapped install works. The engine will embed `apps/console/dist` behind a `console` feature (design §19 §3).

## `@loams/ui` outside this workspace

```ts
import '@loams/ui/styles.css';
import { Button, Card, Logo } from '@loams/ui';
```

Put the class `dark` on `<html>` for dark mode, and set `--loams-font-sans` and `--loams-font-mono` to your loaded Archivo and Martian Mono. Inside the workspace the package resolves to its TypeScript sources; `pnpm --filter @loams/ui publish` publishes the built `dist` (`publishConfig`).

## Fonts

The console bundles Archivo and Martian Mono from Fontsource, both under the SIL Open Font License 1.1 (see `NOTICE`).

## Deploy to Cloudflare

[console.loams.dev](https://console.loams.dev) is the console on Cloudflare Workers static assets: the assets-only Worker `loams-console` on the free plan, configured in [`apps/console/wrangler.jsonc`](apps/console/wrangler.jsonc). It is the same build the engine embeds, staged under `/ui/`:

```bash
pnpm build
node apps/console/deploy/stage.mjs      # dist/ -> dist-cloudflare/: ui/, a root index.html for the SPA fallback, _headers, _redirects
cd apps/console && wrangler deploy
node deploy/smoke.mjs https://console.loams.dev
```

The stage step adds a Content-Security-Policy `<meta>` (inline scripts by hash, no eval, fetches only to itself and the configured server), security headers, long caching for hashed assets and `/` → `/ui/`. Set `LOAMS_CONSOLE_SERVER=https://...` to write both `/config.json` and `/ui/config.json`: both console entries read the server's origin from it at runtime, so nothing is baked into the bundle. The engine serves the default `{}` (same origin).

`.github/workflows/console-deploy.yml` deploys on every push to `main` that touches `web/`, and on demand. It needs `LOAMS_CONSOLE_SERVER` plus `CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ACCOUNT_ID` in the `production` environment; without them it builds and stages, then skips the deploy. `node --test apps/console/deploy/*.test.mjs` tests the stage and smoke scripts and the runtime config. The live smoke check verifies that deployed runtime configuration matches the configured API origin.

The staging step replaces the cordis build CSP with the configured HTTPS API
origin. Sandbox frames receive their own response CSP with `sandbox allow-scripts`;
HTML paths stay exact so their security headers apply without canonical redirects.
