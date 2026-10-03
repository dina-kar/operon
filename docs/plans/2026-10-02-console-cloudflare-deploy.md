# Console Cloudflare deployment — #269 Task 3

Status: Merged #301; live deployment awaits owner setup (#314).

## Global constraints

Workers Free static assets only; no paid products, Worker request code or
embedded secrets. Keep the engine's /ui/ build paths. Read server configuration
at runtime. Authentik remains the identity provider. No new API endpoints.
Use a signed-off PR to dev. The separate Vite+ migration waits for #244.

## Task 0: Reconciliation

The existing #301 deploy stages the legacy console, whose runtime configuration
must remain compatible with same-origin engine deployments. #244 adds a second
cordis entry: after it merges, verify that runtime configuration and CSP also
work for that entry. Its sandbox requires a distinct response CSP. #269's
Vite+ migration and sign-in work are outside this single deployment task.

## Tasks

- [x] Stage the /ui/ assets with SPA fallback and security headers.
- [x] Load runtime server config before rendering; add stage/smoke tests.
- [x] Bound config fetches, reject invalid/insecure hosted server origins,
  hash whitespace-terminated inline scripts, and pin third-party actions.
- [x] Integrate the merged cordis entry and verify its runtime config/CSP.
- [x] Obtain green CI/DCO, address CodeRabbit, and merge #301.
- [ ] Deploy and smoke-check console.loams.dev once owner secrets exist.

## Rulings made during execution

| # | Ruling | Reason |
|---|---|---|
| 1 | Preserve /ui/ assets under the Cloudflare staging directory and use a root fallback page. | The engine build and sandbox resolve /ui/ paths; copying dist to the root returned HTML for JavaScript. |
| 2 | Put console CSP in each page; apply a separate sandbox response CSP. Do not enable COEP without a SharedArrayBuffer consumer. | A global console CSP would restrict sandbox isolation differently; current code does not need cross-origin isolation. |
| 3 | Hosted server URLs must be HTTPS; runtime parsing retains HTTP for local engine development. Validate before replacing staged files. | An HTTPS hosted page cannot fetch a remote HTTP server; invalid schemes must not destroy a previous stage. |
| 4 | Abort config fetches after three seconds and keep the existing same-origin fallback. | A stalled configuration request otherwise leaves the page blank indefinitely. |

| 5 | Load the same runtime config before cordis startup; use the build-time override only in development. | The production cordis entry ignored config.json, unlike the legacy entry. The new entry test failed before this change. |
| 6 | Replace the existing cordis meta CSP during staging and preserve exact HTML paths with assets.html_handling=none. | The old meta CSP blocked the configured server; Cloudflare canonical redirects bypassed the sandbox-specific response header path. |
| 7 | Keep Vitest and Node deploy suites in the test command, excluding deploy files only from Vitest discovery. | Their distinct test APIs cannot share a runner; both suites remain mandatory. |

| 8 | Use explicit deploy test globs on Node 22 and accept browser-recognized script end tags with attributes. | CI Node 22 does not expand test directories; CodeQL identified another end-tag form missed by the original hash matcher. The broader fixture failed before the correction. |

| 9 | Require LOAMS_CONSOLE_SERVER for credentialed hosted deployment while keeping same-origin engine builds. | The static-only Worker has no API handler. Deploying an empty config would direct API requests to its HTML fallback. The new staging regression failed before this guard. |

| 10 | Pass the configured server to the live smoke check and compare normalized deployed origins. | Missing, malformed or wrong API configuration previously passed the HTTP/content-type-only check. The new wrong-origin fixture failed before the comparison. |

## Verification

The whitespace-terminated script and invalid-origin tests failed before their
fixes. The stalled-config test exceeded the external three-second test deadline
before an abort was added. All 20 deploy/config tests and 66 Vitest tests pass after the corrections.
Frozen pnpm install, lint, typecheck, existing tests, build and stage are checked
locally. Cloudflare production environment is absent (GitHub environments API
returned an empty list); owner credentials are needed before live deployment.

Chromium against local Wrangler proved production API requests target the
runtime-configured HTTPS origin. The sample plugin executes in an opaque frame,
cannot read the parent DOM or fetch, and the directly opened sandbox document
has an opaque origin from its response CSP. Cloudflare HTML behavior was checked
against https://developers.cloudflare.com/workers/static-assets/routing/advanced/html-handling/.

Merged commit verified through the GitHub PR API: `a8c799dd8df5f197cd9f30f50b0d708a51d1ed17`.
Live account/environment configuration is tracked in [owner blocker #314](https://github.com/ostrium-labs/loams/issues/314).
