import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import react from '@vitejs/plugin-react';
import { defineConfig, type Plugin } from 'vite';
import { configDefaults } from 'vitest/config';

// The engine serves the build at /ui (design §19 §3). In development the
// console API comes from loams-console-mock (`cargo run -p
// loams-console-mock`, port 8081) unless LOAMS_API points at an engine.
const api = process.env.LOAMS_API ?? 'http://127.0.0.1:8081';

const require = createRequire(import.meta.url);

/**
 * Static files the cordis console serves beside itself (AP1a Task 6): the
 * sandbox frame and its runtime, and the sample third-party plugin. In dev
 * they are served from their packages; in a build they are emitted as assets.
 */
const SANDBOX_FILES: Record<string, string> = {
  'sandbox/frame.html': require.resolve('@loams/plugin-sandbox/frame.html'),
  'sandbox/runtime.js': require.resolve('@loams/plugin-sandbox/runtime.js'),
  'plugins/hello/client.js': require.resolve('@loams/example-plugin-hello/client'),
};

function sandboxAssets(base: string): Plugin {
  return {
    name: 'loams-sandbox-assets',
    configureServer(server) {
      server.middlewares.use((req, res, next) => {
        const path = req.url?.split(/[?#]/)[0] ?? '';
        const file = path.startsWith(base) ? SANDBOX_FILES[path.slice(base.length)] : undefined;
        if (!file) return next();
        res.setHeader(
          'content-type',
          file.endsWith('.html') ? 'text/html; charset=utf-8' : 'text/javascript; charset=utf-8',
        );
        res.end(readFileSync(file));
      });
    },
    generateBundle() {
      for (const [fileName, file] of Object.entries(SANDBOX_FILES)) {
        this.emitFile({ type: 'asset', fileName, source: readFileSync(file) });
      }
    },
  };
}

/**
 * The console's CSP, in production builds only (Vite's dev server injects
 * inline scripts): no `unsafe-eval` and no remote source (§37 §6.3).
 */
const CSP =
  "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; " +
  "img-src 'self' data: blob:; font-src 'self'; connect-src 'self'; frame-src 'self'; " +
  "object-src 'none'; base-uri 'none'; form-action 'self'";

function cspMeta(): Plugin {
  return {
    name: 'loams-csp',
    apply: 'build',
    transformIndexHtml: {
      order: 'post',
      handler(html, ctx) {
        if (!ctx.filename.endsWith('cordis.html')) return html;
        return html.replace(
          '<meta charset="utf-8" />',
          `<meta charset="utf-8" />\n    <meta http-equiv="Content-Security-Policy" content="${CSP}" />`,
        );
      },
    },
  };
}

export default defineConfig({
  base: '/ui/',
  plugins: [react(), sandboxAssets('/ui/'), cspMeta()],
  server: {
    port: 5173,
    proxy: {
      '/api': api,
      '/v1': api,
      '/.well-known': api,
      '/health': api,
      '/ready': api,
    },
  },
  build: {
    outDir: 'dist',
    // Everything is bundled: an air-gapped console loads nothing remote.
    assetsInlineLimit: 0,
    sourcemap: true,
    rollupOptions: {
      // Today's console, and the cordis console beside it (AP1a).
      input: { main: 'index.html', cordis: 'cordis.html' },
    },
  },
  test: {
    environment: 'jsdom',
    // Deployment tests use node:test and run in the package test command.
    exclude: [...configDefaults.exclude, 'deploy/**'],
  },
});
