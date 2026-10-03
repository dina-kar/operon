// The browser entry of the cordis console (§37 §5, AP1a). Served at
// /ui/cordis.html beside today's console until AP1a Task 5 moves today's
// pages into plugins and this becomes /ui/.
//
//   pnpm dev                                    → demo mode (an in-browser mock)
//   VITE_LOAMS_APPS_URL=http://127.0.0.1:8084 \
//   VITE_LOAMS_DEV_BEARER=mock-access-usr_omar pnpm dev
//                                               → against `cargo run -p loams-apps-mock`

import '@fontsource-variable/archivo/standard.css';
import '@fontsource-variable/martian-mono';
import '@loams/ui/styles.css';
import './console.css';

import { THIRD_PARTY_FLAG } from '@loams/console-host';
import { createMockTransport } from '@loams/console-host/testing';
import { createWebPlatform } from '@loams/platform-web';
import { loadRuntimeConfig } from '../runtime-config.js';
import { startConsole } from './start.js';

const appsUrl = import.meta.env.DEV
  ? (import.meta.env.VITE_LOAMS_APPS_URL as string | undefined)
  : undefined;
const demo =
  new URLSearchParams(globalThis.location.search).has('demo') || (import.meta.env.DEV && !appsUrl);

const root = document.getElementById('root');
if (root) {
  loadRuntimeConfig(import.meta.env.BASE_URL)
    .then((config) => {
      const platform = createWebPlatform(
        demo
          ? { transport: createMockTransport({ features: { [THIRD_PARTY_FLAG]: true } }) }
          : {
              baseUrl: appsUrl ?? config.server,
              devBearer: import.meta.env.DEV
                ? (import.meta.env.VITE_LOAMS_DEV_BEARER as string | undefined)
                : undefined,
            },
      );

      return startConsole({
        platform,
        root,
        // Demo only: grant a sandboxed plugin what it declares (start.ts).
        grant: demo ? (manifest) => manifest.permissions : undefined,
      });
    })
    .then((handle) => {
      // For debugging in the browser console: the plugin table and the sweep.
      Object.assign(globalThis, { loamsConsole: handle });
      setTimeout(() => {
        const stuck = handle.pending().filter((p) => !p.silent);
        if (stuck.length > 0) console.warn('loams console: plugins still pending', stuck);
      }, 10_000);
    });
}
