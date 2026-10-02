import { createWebPlatform } from '@loams/platform-web';
import { waitFor } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { startConsole } from '../src/cordis/start.js';

vi.mock('@loams/platform-web', () => ({ createWebPlatform: vi.fn(() => ({ name: 'test' })) }));
vi.mock('../src/cordis/start.js', () => ({
  startConsole: vi.fn(async () => ({ pending: () => [] })),
}));

const timers = vi.spyOn(globalThis, 'setTimeout');
afterEach(() => {
  for (const result of timers.mock.results) {
    if (result.type === 'return') clearTimeout(result.value);
  }
  timers.mockClear();
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
  vi.clearAllMocks();
  document.body.innerHTML = '';
});

it('production_cordis_console_uses_runtime_server_configuration', async () => {
  vi.stubEnv('DEV', false);
  vi.stubEnv('BASE_URL', '/ui/');
  vi.stubEnv('VITE_LOAMS_APPS_URL', 'https://baked.example');
  const fetcher = vi.fn(async () => Response.json({ server: 'https://runtime.example' }));
  vi.stubGlobal('fetch', fetcher);
  document.body.innerHTML = '<div id="root"></div>';
  await import('../src/cordis/main.js');
  await waitFor(() => expect(startConsole).toHaveBeenCalledOnce());
  expect(fetcher).toHaveBeenCalledWith('/ui/config.json', expect.any(Object));
  expect(createWebPlatform).toHaveBeenCalledWith(
    expect.objectContaining({ baseUrl: 'https://runtime.example', devBearer: undefined }),
  );
});
