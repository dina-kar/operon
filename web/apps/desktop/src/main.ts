// Loams Desktop's frontend (§37 §6, AP1). Boots the cordis console with the
// desktop plugin set and @loams/platform-tauri, whose fetch goes through
// the Rust bridge. Outside Tauri (`pnpm dev:web` in a browser) it runs the
// console in demo mode on the in-browser mock.

import '@fontsource-variable/archivo/standard.css';
import '@fontsource-variable/martian-mono';
import '@loams/ui/styles.css';
import '@loams/console/cordis.css';

import { startConsole } from '@loams/console/cordis';
import { type PluginModule, parsePatch } from '@loams/console-host';
import { createMockTransport } from '@loams/console-host/testing';
import { createTauriPlatform, type EnvView, type SidecarStatus } from '@loams/platform-tauri';
import { createWebPlatform } from '@loams/platform-web';
import stacksPkg from '@loams/plugin-stacks/package.json';
import { invoke, isTauri } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';
import desktopPatch from '../catalog/desktop.patch.yml?raw';

const DEMO_URL = 'demo:';

/** Waits briefly for the bundled sidecar, so the first load can use it. */
async function activeEnvironment(): Promise<EnvView | null> {
  for (let i = 0; i < 25; i++) {
    const status = await invoke<SidecarStatus>('sidecar_status');
    if (status.state !== 'starting') break;
    await new Promise((r) => setTimeout(r, 200));
  }
  return invoke<EnvView | null>('envs_active');
}

async function platformPlugin(): Promise<PluginModule> {
  const demo = new URLSearchParams(location.search).has('demo');
  if (!isTauri()) return createWebPlatform({ transport: createMockTransport() });
  const active = demo ? null : await activeEnvironment();
  if (!active?.base_url) {
    // No environment yet (no sidecar, or it is still starting): the demo
    // mock, with the stacks page available to start one.
    return createTauriPlatform({ baseUrl: DEMO_URL, transport: createMockTransport() });
  }
  return createTauriPlatform({ baseUrl: active.base_url });
}

async function main() {
  const root = document.getElementById('root');
  if (!root) return;
  const handle = await startConsole({
    platform: await platformPlugin(),
    root,
    base: '/',
    patches: [parsePatch(desktopPatch)],
    extraModules: { '@loams/plugin-stacks': () => import('@loams/plugin-stacks') },
    extraManifests: [stacksPkg],
  });
  Object.assign(globalThis, { loamsConsole: handle });
  if (isTauri()) {
    // Deep links are navigation only: Rust parsed them against the
    // allowlist and hands over a route (deeplink.rs).
    const go = (route: string) => {
      location.hash = `#${route}`;
    };
    const first = await invoke<{ route: string } | null>('deeplink_take');
    if (first) go(first.route);
    await listen<{ route: string }>('desktop/deeplink', (event) => go(event.payload.route));
  }
}

main();
