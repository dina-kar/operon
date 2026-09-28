import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

// The engine serves the build at /ui (design §19 §3). In development the
// console API comes from operon-console-mock (`cargo run -p
// operon-console-mock`, port 8081) unless LOAM_API points at an engine.
const api = process.env.LOAM_API ?? 'http://127.0.0.1:8081';

export default defineConfig({
  base: '/ui/',
  plugins: [react()],
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
  },
});
