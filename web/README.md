# web

The Loam console and its design system (design [§19](../docs/design/19-console-identity-and-agents.md)).

| Package | What |
|---|---|
| `packages/ui` (`@loam/ui`) | Tokens, the mark and wordmark, grain textures and React primitives: buttons, status tags, cards, empty states, fields, tables, stats, notices, dialogs, snippets, meters, avatars, formatters. Plain CSS with `loam-` classes; works in Vite and Next.js |
| `apps/console` (`@loam/console`) | One single-page app for OSS, Loam Cloud and BYOC. It reads `GET /api/v1/instance` to know its edition and sign-in methods |

The console speaks the contract in [`api/console/openapi.json`](../api/console/openapi.json). Until the gateway implements it (M2), [`operon-console-mock`](../crates/operon-console-mock) serves it with seed data.

## Develop

```bash
cargo run -p operon-console-mock        # the API mock on :8081 (add --signed-out for the sign-in screens)
cd web && pnpm install && pnpm dev       # the console on http://localhost:5173/ui/
```

`pnpm dev` proxies `/api`, `/v1`, `/.well-known`, `/health` and `/ready` to the mock. To use a real engine instead, set `LOAM_API=http://127.0.0.1:8080`.

After changing the contract, regenerate the console's types:

```bash
pnpm gen:api
```

## Check and build

```bash
pnpm lint          # Biome
pnpm typecheck     # tsc, both packages
pnpm build         # @loam/ui to packages/ui/dist, the console to apps/console/dist
```

The console builds for the base path `/ui`, bundles its fonts and assets, and loads nothing from the internet, so an air-gapped install works. The engine will embed `apps/console/dist` behind a `console` feature (design §19 §3).

## `@loam/ui` outside this workspace

```ts
import '@loam/ui/styles.css';
import { Button, Card, Logo } from '@loam/ui';
```

Put the class `dark` on `<html>` for dark mode, and set `--loam-font-sans` and `--loam-font-mono` to your loaded Archivo and Martian Mono. Inside the workspace the package resolves to its TypeScript sources; `pnpm --filter @loam/ui publish` publishes the built `dist` (`publishConfig`).

## Fonts

The console bundles Archivo and Martian Mono from Fontsource, both under the SIL Open Font License 1.1 (see `NOTICE`).
