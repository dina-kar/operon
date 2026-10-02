#!/usr/bin/env node
// Puts the `loams` sidecar where Tauri's `externalBin` expects it:
// web/apps/desktop/src-tauri/binaries/loams-<target-triple>[.exe] (AP1 Task 2).
//
//   node scripts/desktop/prepare-sidecar.mjs --from-path <path to loams>   # a local build
//   node scripts/desktop/prepare-sidecar.mjs --placeholder                 # CI: compile-only stub
//   node scripts/desktop/prepare-sidecar.mjs --from-release <url> --sha256 <hex>
//
// --target <triple> overrides the host triple (from `rustc -vV`).
// The placeholder is a shell script that exits 1 with a message, so a CI
// build links and bundles without building the engine; the app then shows
// the sidecar as crashed with that message. Releases use --from-release,
// which verifies the CLI release manifest's SHA-256 (and, once §30 D292's
// minisign signatures exist, the signature: TODO).

import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { chmodSync, copyFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const outDir = join(root, 'web/apps/desktop/src-tauri/binaries');

function arg(name) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : undefined;
}

const triple =
  arg('--target') ??
  /host: (\S+)/.exec(execFileSync('rustc', ['-vV'], { encoding: 'utf8' }))?.[1];
if (!triple) throw new Error('could not read the host target triple from rustc -vV');
const exe = triple.includes('windows') ? '.exe' : '';
const out = join(outDir, `loams-${triple}${exe}`);
mkdirSync(outDir, { recursive: true });

if (process.argv.includes('--placeholder')) {
  if (exe) {
    // Windows bundles no sidecar (remote-only, Q437); nothing to write.
    console.log('windows: no sidecar is bundled');
    process.exit(0);
  }
  writeFileSync(
    out,
    '#!/bin/sh\necho "this Loams Desktop build has no bundled loams (CI placeholder); set LOAMS_DESKTOP_SIDECAR" >&2\nexit 1\n',
  );
  chmodSync(out, 0o755);
  console.log(`placeholder: ${out}`);
} else if (arg('--from-path')) {
  const from = resolve(arg('--from-path'));
  if (!existsSync(from)) throw new Error(`no loams binary at ${from}`);
  copyFileSync(from, out);
  chmodSync(out, 0o755);
  console.log(`copied ${from} → ${out}`);
} else if (arg('--from-release')) {
  const url = arg('--from-release');
  const expected = arg('--sha256');
  if (!expected) throw new Error('--from-release needs --sha256 (from the CLI release manifest)');
  const response = await fetch(url);
  if (!response.ok) throw new Error(`download failed: HTTP ${response.status}`);
  const bytes = Buffer.from(await response.arrayBuffer());
  const actual = createHash('sha256').update(bytes).digest('hex');
  if (actual !== expected.toLowerCase()) {
    throw new Error(`checksum mismatch: expected ${expected}, got ${actual}`);
  }
  // TODO(§30 D292): verify the manifest's minisign signature first.
  writeFileSync(out, bytes);
  chmodSync(out, 0o755);
  console.log(`downloaded and verified → ${out}`);
} else {
  console.error(readFileSync(fileURLToPath(import.meta.url), 'utf8').split('\n').slice(1, 12).join('\n'));
  process.exit(2);
}
