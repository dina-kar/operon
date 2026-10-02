// Stages the console build for Cloudflare Workers static assets (issue #269).
//
//   node deploy/stage.mjs            # dist/ -> dist-cloudflare/
//   LOAMS_CONSOLE_SERVER=https://loams.example.com node deploy/stage.mjs
//
// The build is the same one the engine embeds (base path /ui/), so it lands
// under dist-cloudflare/ui/. The root gets a copy of index.html, which the
// `single-page-application` fallback in wrangler.jsonc serves for unknown
// paths, and a redirect from / to /ui/. Asset paths therefore resolve to real
// files and come back with their own MIME type (text/javascript for .js),
// never as the HTML fallback.
import { createHash } from 'node:crypto';
import { cpSync, existsSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

/** The base path the console is built for (vite.config.ts `base`). */
export const BASE = '/ui/';

/** The `sha256-...` CSP sources of every inline `<script>` in a page. */
export function inlineScriptHashes(html) {
  const hashes = [];
  for (const m of html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script\b[^>]*>/gi)) {
    if (/\bsrc\s*=/i.test(m[1] ?? '')) continue;
    const body = m[2] ?? '';
    hashes.push(`'sha256-${createHash('sha256').update(body, 'utf8').digest('base64')}'`);
  }
  return hashes;
}

/** The console's Content-Security-Policy: no eval, no remote code; fetches to itself and the configured server. */
export function contentSecurityPolicy({ scriptHashes = [], server } = {}) {
  const connect = ["'self'", ...(server ? [new URL(server).origin] : [])];
  return [
    "default-src 'self'",
    `script-src ${["'self'", ...scriptHashes].join(' ')}`,
    "style-src 'self' 'unsafe-inline'",
    "img-src 'self' data: blob:",
    "font-src 'self'",
    `connect-src ${connect.join(' ')}`,
    "frame-src 'self'",
    "object-src 'none'",
    "base-uri 'none'",
    "form-action 'self'",
  ].join('; ');
}

/** Adds a CSP once; staging replaces the build's policy with its runtime server policy. */
export function injectCsp(html, csp, replaceExisting = false) {
  const meta = `<meta http-equiv="Content-Security-Policy" content="${csp.replaceAll('"', '&quot;')}" />`;
  const existing = /<meta\b[^>]*http-equiv=["']Content-Security-Policy["'][^>]*>/i;
  if (existing.test(html)) return replaceExisting ? html.replace(existing, meta) : html;
  const charset = /<meta charset="utf-8"\s*\/?>/i;
  if (!charset.test(html)) throw new Error('index.html has no <meta charset="utf-8" />');
  return html.replace(charset, (m) => `${m}\n    ${meta}`);
}

/** Response headers for every path (`_headers`); the CSP rides in the page's `<meta>`. */
export function headersFile() {
  return `/*
  X-Content-Type-Options: nosniff
  Referrer-Policy: strict-origin-when-cross-origin
  Cross-Origin-Opener-Policy: same-origin
  X-Frame-Options: SAMEORIGIN
  Permissions-Policy: camera=(), microphone=(), geolocation=(), payment=()
  Strict-Transport-Security: max-age=31536000; includeSubDomains

${BASE}sandbox/frame.html
  Content-Security-Policy: default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src data: blob:; connect-src 'none'; form-action 'none'; base-uri 'none'; sandbox allow-scripts

${BASE}assets/*
  Cache-Control: public, max-age=31536000, immutable

${BASE}config.json
  Cache-Control: no-store

/config.json
  Cache-Control: no-store
`;
}

/** `_redirects`: the bare host opens the console. */
export function redirectsFile() {
  return `/ ${BASE} 302\n`;
}

/** Fails when the build was made for another base path (the assets would 404 into the HTML fallback). */
export function checkBase(html) {
  const srcs = [...html.matchAll(/<script\b[^>]*\bsrc="([^"]+)"/gi)].map((m) => m[1] ?? '');
  if (srcs.length === 0) throw new Error('index.html loads no script');
  for (const src of srcs) {
    if (!src.startsWith(`${BASE}assets/`)) {
      throw new Error(
        `index.html loads ${src}, expected ${BASE}assets/...: rebuild with base ${BASE}`,
      );
    }
  }
}

/** Stages `dist` into `out`. `server` is the Loams server origin for config.json, if any. */
export function stage({ dist, out, server, requireServer = false }) {
  if (requireServer && !server) {
    throw new Error('LOAMS_CONSOLE_SERVER is required for the hosted console deployment');
  }
  if (server) {
    let url;
    try {
      url = new URL(server);
    } catch {
      throw new Error('hosted console server must be a valid HTTPS URL');
    }
    if (url.protocol !== 'https:') {
      throw new Error('hosted console server must use HTTPS');
    }
    server = url.origin;
  }
  const index = join(dist, 'index.html');
  if (!existsSync(index)) throw new Error(`${index} is missing: run the console build first`);
  checkBase(readFileSync(index, 'utf8'));

  rmSync(out, { recursive: true, force: true });
  const ui = join(out, BASE.replaceAll('/', ''));
  cpSync(dist, ui, { recursive: true });

  if (server) {
    const origin = new URL(server).origin;
    writeFileSync(join(ui, 'config.json'), `${JSON.stringify({ server: origin }, null, 2)}\n`);
  }

  for (const name of readdirSync(ui)) {
    if (!name.endsWith('.html')) continue;
    const file = join(ui, name);
    const html = readFileSync(file, 'utf8');
    const csp = contentSecurityPolicy({ scriptHashes: inlineScriptHashes(html), server });
    writeFileSync(file, injectCsp(html, csp, true));
  }
  cpSync(join(ui, 'index.html'), join(out, 'index.html'));
  cpSync(join(ui, 'config.json'), join(out, 'config.json'));

  writeFileSync(join(out, '_headers'), headersFile());
  writeFileSync(join(out, '_redirects'), redirectsFile());
  // Source maps stay out of the public deploy.
  writeFileSync(join(out, '.assetsignore'), '*.map\n');
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
  const server = process.env.LOAMS_CONSOLE_SERVER?.trim() || undefined;
  stage({
    dist: join(root, 'dist'),
    out: join(root, 'dist-cloudflare'),
    server,
    requireServer: process.argv.includes('--require-server'),
  });
  console.log(`staged dist-cloudflare/ (server: ${server ?? 'same origin'})`);
}
