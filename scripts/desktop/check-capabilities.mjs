#!/usr/bin/env node
// Fails when a capability grants anything outside the allowlist, or when a
// capability is remote (AP1 Task 4, D430; capabilities_are_exactly_the_allowlist).
// Also fails when the CSP allows a remote source or `unsafe-eval`
// (csp_has_no_remote_sources).

import { readdirSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const tauri = join(root, 'web/apps/desktop/src-tauri');

/** Exactly what main.json may grant. */
const ALLOWED = new Set([
  'core:default',
  'deep-link:default',
  'opener:allow-open-url',
  'allow-sidecar-status',
  'allow-sidecar-start',
  'allow-sidecar-stop',
  'allow-sidecar-restart',
  'allow-net-fetch',
  'allow-net-abort',
  'allow-envs-list',
  'allow-envs-active',
  'allow-envs-select',
  'allow-auth-sign-in',
  'allow-auth-sign-out',
  'allow-auth-status',
  'allow-deeplink-take',
]);
const FORBIDDEN_PREFIXES = ['shell:', 'fs:', 'http:', 'process:', 'dialog:', 'stronghold:'];
const OPENER_SCOPE = new Set(['https://loams.dev/**', 'https://github.com/ostrium-labs/**']);

const errors = [];
const dir = join(tauri, 'capabilities');
const files = readdirSync(dir).filter((f) => f.endsWith('.json'));
if (files.length !== 1 || files[0] !== 'main.json') {
  errors.push(`expected exactly capabilities/main.json, found ${files.join(', ')}`);
}
for (const file of files) {
  const cap = JSON.parse(readFileSync(join(dir, file), 'utf8'));
  if (cap.remote) errors.push(`${file}: remote capabilities are not allowed`);
  if (cap.local !== true) errors.push(`${file}: must be local`);
  if (JSON.stringify(cap.windows) !== '["main"]') errors.push(`${file}: windows must be ["main"]`);
  const seen = new Set();
  for (const permission of cap.permissions ?? []) {
    const id = typeof permission === 'string' ? permission : permission.identifier;
    seen.add(id);
    if (FORBIDDEN_PREFIXES.some((p) => id.startsWith(p))) errors.push(`${file}: forbidden ${id}`);
    else if (!ALLOWED.has(id)) errors.push(`${file}: ${id} is not in the allowlist`);
    if (id === 'opener:allow-open-url') {
      const urls = (permission.allow ?? []).map((a) => a.url);
      if (urls.length === 0 || urls.some((u) => !OPENER_SCOPE.has(u))) {
        errors.push(`${file}: opener:allow-open-url must be scoped to ${[...OPENER_SCOPE].join(', ')}`);
      }
    }
  }
  if (typeof (cap.permissions ?? []).find((p) => p === 'opener:allow-open-url') === 'string') {
    errors.push(`${file}: opener:allow-open-url must be scoped, not granted bare`);
  }
  for (const id of ALLOWED) if (!seen.has(id)) errors.push(`${file}: missing ${id}`);
}

const conf = JSON.parse(readFileSync(join(tauri, 'tauri.conf.json'), 'utf8'));
const csp = conf.app?.security?.csp ?? '';
if (!csp) errors.push('tauri.conf.json: app.security.csp is empty');
if (/unsafe-eval/.test(csp)) errors.push('CSP allows unsafe-eval');
for (const token of csp.split(/[\s;]+/)) {
  if (/^(https?:|wss?:)\/\//.test(token) && !token.startsWith('http://ipc.localhost')) {
    errors.push(`CSP allows a remote source: ${token}`);
  }
  if (token === '*' || token === 'https:' || token === 'http:') {
    errors.push(`CSP allows a wildcard source: ${token}`);
  }
}
if (conf.app?.withGlobalTauri !== false) errors.push('app.withGlobalTauri must be false');

if (errors.length > 0) {
  console.error(errors.map((e) => `✗ ${e}`).join('\n'));
  process.exit(1);
}
console.log(`capabilities and CSP are exactly the allowlist (${ALLOWED.size} permissions)`);
