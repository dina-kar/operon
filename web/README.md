# web

The Loams console and its design system (design [§19](../docs/design/19-console-identity-and-agents.md)).

| Package | What |
|---|---|
| `packages/ui` (`@loams/ui`) | Tokens, the mark and wordmark, grain textures and React primitives: buttons, status tags, cards, empty states, fields, tables, stats, notices, dialogs, snippets, meters, avatars, formatters. Plain CSS with `loams-` classes; works in Vite and Next.js |
| `apps/console` (`@loams/console`) | One single-page app for OSS, Loams Cloud and BYOC. It reads `GET /api/v1/instance` to know its edition and sign-in methods |

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
pnpm typecheck     # tsc, both packages
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

The stage step adds a Content-Security-Policy `<meta>` (inline scripts by hash, no eval, fetches only to itself and the configured server), security headers, long caching for hashed assets and `/` → `/ui/`. Set `LOAMS_CONSOLE_SERVER=https://...` to write `ui/config.json`: the console reads the server's origin from it at runtime, so nothing is baked into the bundle. The engine serves the default `{}` (same origin).

`.github/workflows/console-deploy.yml` deploys on every push to `main` that touches `web/`, and on demand. It needs `CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ACCOUNT_ID` in the `production` environment; without them it builds and stages, then skips the deploy. `node --test apps/console/deploy/` tests the stage and smoke scripts and the runtime config.
