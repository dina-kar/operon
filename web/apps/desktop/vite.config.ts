import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

// Loams Desktop's frontend: the cordis console with the desktop plugin set,
// bundled into the app (no remote content, §37 §6.3). `tauri dev` runs this
// dev server on 5174; `tauri build` embeds dist/.
export default defineConfig({
  base: '/',
  plugins: [react()],
  clearScreen: false,
  server: { port: 5174, strictPort: true },
  build: {
    outDir: 'dist',
    assetsInlineLimit: 0,
    sourcemap: true,
    target: 'es2022',
  },
});
