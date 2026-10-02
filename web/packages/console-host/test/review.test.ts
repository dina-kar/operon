// Regressions for the review of #244.

import { type Context, Context as Root } from '@loams/cordis';
import { describe, expect, it, vi } from 'vitest';
import {
  boot,
  GuardError,
  guard,
  ManifestError,
  mountSandboxed,
  validateManifest,
} from '../src/index.js';
import { createMockControl } from '../src/testing/index.js';

const platform = (transport: unknown) => ({
  name: 'p',
  apply(ctx: Context) {
    ctx.provide('platform', { kind: 'web' });
    ctx.provide('transport', transport);
  },
});

const pkg = (name: string, plugin: Record<string, unknown>) => ({
  name,
  version: '0.1.0',
  loams: { plugin: { kind: 'console', entry: '.', tier: 'first-party', inject: [], ...plugin } },
});

describe('review fixes', () => {
  it('guarded plugins provide only in their namespace', async () => {
    const root = new Root();
    const errors: unknown[] = [];
    await root.plugin({
      name: 'jobs',
      apply(c: Context) {
        const g = guard(c, [], 'jobs');
        g.provide('jobs.queues', { ok: true });
        try {
          g.provide('session', {});
        } catch (e) {
          errors.push(e);
        }
      },
    });
    expect(root.get('jobs.queues')).toEqual({ ok: true });
    expect(errors[0]).toBeInstanceOf(GuardError);
  });

  it('a plugin that fails to load fails its row, not boot', async () => {
    const mock = createMockControl();
    const handle = await boot({
      catalog: [
        { id: 'broken', name: '@loams/plugin-broken' },
        { id: 'empty', name: '@loams/plugin-empty' },
      ],
      manifests: [pkg('@loams/plugin-broken', {}), pkg('@loams/plugin-empty', {})],
      modules: {
        '@loams/plugin-broken': async () => {
          throw new Error('chunk 404');
        },
        '@loams/plugin-empty': async () => ({}) as never,
      },
      platform: platform(mock.transport),
    });
    const rows = Object.fromEntries(handle.plugins().map((p) => [p.id, p]));
    expect(rows.broken).toMatchObject({
      status: 'failed',
      reason: expect.stringContaining('chunk 404'),
    });
    expect(rows.empty).toMatchObject({
      status: 'failed',
      reason: expect.stringContaining('no apply'),
    });
    await handle.dispose();
  });

  it('enable refuses a row that failed a boot-time check', async () => {
    const mock = createMockControl();
    const apply = vi.fn();
    const handle = await boot({
      catalog: [{ id: 'wide', name: '@loams/plugin-wide', inject: ['platform'] }],
      manifests: [pkg('@loams/plugin-wide', { inject: [] })],
      modules: { '@loams/plugin-wide': async () => ({ apply }) },
      platform: platform(mock.transport),
    });
    await handle.enable('wide');
    expect(apply).not.toHaveBeenCalled();
    expect(handle.plugins()[0]?.status).toBe('failed');
    await handle.dispose();
  });

  it('a claimed core tier does not widen provides, and server/requires are validated', () => {
    expect(() =>
      validateManifest(pkg('@evil/plugin', { tier: 'core', provides: ['session'] })),
    ).toThrow(/namespace/);
    for (const bad of [
      { server: 'x' },
      { server: { kind: 'eval', ref: 'x' } },
      { server: { kind: 'function', ref: 'x', extra: 1 } },
      { requires: { console: '^1', api: ['jobs'] } },
      { requires: { other: true } },
    ]) {
      expect(() => validateManifest(pkg('@loams/plugin-x', bad))).toThrow(ManifestError);
    }
    expect(
      validateManifest(pkg('@loams/plugin-x', { server: { kind: 'connector', ref: 'kafka' } }))
        .server,
    ).toEqual({ kind: 'connector', ref: 'kafka' });
  });

  it('the script must be the frame base path for the policy plugin', () => {
    const mount = (frameUrl: string, scriptUrl: string, pluginId = 'hello') =>
      mountSandboxed(document.createElement('div'), {
        frameUrl,
        scriptUrl,
        policy: { pluginId, version: '1', services: [], permissions: [] },
        invoke: async () => undefined,
      });
    expect(() => mount('/ui/sandbox/frame.html', '/custom/plugins/hello/client.js')).toThrow(
      /refusing/,
    );
    expect(() => mount('/ui/sandbox/frame.html', '/ui/plugins/hello/client.js', 'other')).toThrow(
      /refusing/,
    );
    expect(() => mount('/ui/other.html', '/ui/plugins/hello/client.js')).toThrow(/refusing/);
    mount('/ui/sandbox/frame.html', '/ui/plugins/hello/client.js').dispose();
  });

  it('a frame that navigates away never gets a port again', () => {
    const container = document.createElement('div');
    const handle = mountSandboxed(container, {
      frameUrl: '/ui/sandbox/frame.html',
      scriptUrl: '/ui/plugins/hello/client.js',
      policy: { pluginId: 'hello', version: '0.1.0', services: [], permissions: [] },
      invoke: async () => undefined,
    });
    const posted = vi.fn();
    Object.defineProperty(handle.iframe, 'contentWindow', { value: { postMessage: posted } });
    handle.iframe.dispatchEvent(new Event('load'));
    expect(posted).toHaveBeenCalledTimes(1);
    expect(handle.bridge()).toBeDefined();
    handle.iframe.dispatchEvent(new Event('load'));
    expect(posted).toHaveBeenCalledTimes(1);
    expect(handle.bridge()).toBeUndefined();
    handle.dispose();
  });
});
