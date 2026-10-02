import type { Transport } from '@connectrpc/connect';
import { type Context, FiberStates } from '@loams/cordis';
import type { SlotRegistry } from '@loams/slots';
import { describe, expect, it, vi } from 'vitest';
import {
  boot,
  type ModuleTable,
  type PluginModule,
  service,
  THIRD_PARTY_FLAG,
  watch,
} from '../src/index.js';
import { createMockControl } from '../src/testing/index.js';

function platformWith(transport: Transport): PluginModule {
  return {
    name: 'platform-test',
    apply(ctx: Context) {
      ctx.provide('platform', {
        kind: 'web',
        fetch: globalThis.fetch,
        baseUrl: 'mock:',
        openExternal: async () => {},
        notify: async () => {},
        clipboardWrite: async () => {},
      });
      ctx.provide('transport', transport);
    },
  };
}

function manifest(name: string, inject: string[], extra: Record<string, unknown> = {}) {
  return {
    name,
    version: '0.1.0',
    loams: {
      plugin: { kind: 'console', entry: '.', tier: 'first-party', inject, ...extra },
    },
  };
}

const log: string[] = [];

/** A provider of `rpc.approvals` gated on flags, like @loams/plugin-rpc. */
const rpcLike: PluginModule = {
  inject: ['transport', 'flags'],
  apply(ctx) {
    const flags = service(ctx, 'flags');
    if (flags.has('loams.approvals.v1')) ctx.provide('rpc.approvals', { gated: true });
    if (flags.has('loams.jobs.v1')) ctx.provide('rpc.jobs', {});
  },
};

const consumer: PluginModule = {
  inject: ['rpc.approvals', 'slots'],
  apply(ctx) {
    log.push('consumer active');
    const slots = service(ctx, 'slots') as SlotRegistry;
    ctx.effect(() => slots.register({ name: 'shell.overlay', plugin: 'consumer' }, () => null));
    ctx.effect(() => () => log.push('consumer disposed'));
  },
};

const jobsPage: PluginModule = {
  inject: ['rpc.jobs'],
  apply() {
    log.push('jobs active');
  },
};

const modules: ModuleTable = {
  '@loams/plugin-rpc': async () => rpcLike,
  '@loams/plugin-consumer': async () => consumer,
  '@loams/plugin-jobs': async () => ({ default: jobsPage }),
};

const manifests = [
  manifest('@loams/plugin-rpc', ['transport', 'flags'], { tier: 'core' }),
  manifest('@loams/plugin-consumer', ['rpc.approvals', 'slots']),
  manifest('@loams/plugin-jobs', ['rpc.jobs']),
  manifest('@acme/plugin-third', ['rpc.approvals'], {
    tier: 'third-party',
    permissions: ['approvals:read'],
  }),
];

const catalog = [
  { id: 'rpc', name: '@loams/plugin-rpc' },
  { id: 'consumer', name: '@loams/plugin-consumer' },
  { id: 'jobs', name: '@loams/plugin-jobs' },
  { id: 'third', name: '@acme/plugin-third' },
];

const settle = () => new Promise((r) => setTimeout(r, 20));

describe('boot', () => {
  it('boot_loads_rows_in_dependency_order and gates rpc on api_versions', async () => {
    log.length = 0;
    const mock = createMockControl();
    const handle = await boot({
      catalog,
      manifests,
      modules,
      platform: platformWith(mock.transport),
    });
    await settle();
    const status = Object.fromEntries(handle.plugins().map((p) => [p.id, p.status]));
    expect(status).toMatchObject({
      rpc: 'active',
      consumer: 'active',
      jobs: 'pending',
      third: 'skipped',
    });
    expect(log).toContain('consumer active');
    expect(log).not.toContain('jobs active');
    expect(handle.slots.entries('shell.overlay')).toHaveLength(1);
    await handle.dispose();
  });

  it('rpc_gate_is_not_reported and pending_fiber_reported_with_missing_services', async () => {
    const mock = createMockControl();
    const handle = await boot({
      catalog: [...catalog, { id: 'needy', name: '@loams/plugin-needy' }],
      manifests: [...manifests, manifest('@loams/plugin-needy', ['router'])],
      modules: { ...modules, '@loams/plugin-needy': async () => ({ apply() {} }) },
      platform: platformWith(mock.transport),
    });
    await settle();
    const pending = handle.pending();
    expect(pending).toContainEqual({ id: 'jobs', waitingFor: ['rpc.jobs'], silent: true });
    expect(pending).toContainEqual({ id: 'needy', waitingFor: ['router'], silent: false });
    await handle.dispose();
  });

  it('dispose_removes_slot_entries when a plugin is disabled, and enable restores it', async () => {
    log.length = 0;
    const mock = createMockControl();
    const handle = await boot({
      catalog,
      manifests,
      modules,
      platform: platformWith(mock.transport),
    });
    await settle();
    await handle.disable('consumer');
    expect(log).toContain('consumer disposed');
    expect(handle.slots.entries('shell.overlay')).toHaveLength(0);
    expect(handle.plugins().find((p) => p.id === 'consumer')?.status).toBe('disabled');
    await handle.enable('consumer');
    await settle();
    expect(handle.slots.entries('shell.overlay')).toHaveLength(1);
    await handle.dispose();
  });

  it('a catalog row cannot inject beyond its manifest', async () => {
    const mock = createMockControl();
    const handle = await boot({
      catalog: [
        { id: 'consumer', name: '@loams/plugin-consumer', inject: ['rpc.approvals', 'platform'] },
      ],
      manifests,
      modules,
      platform: platformWith(mock.transport),
    });
    expect(handle.plugins()[0]).toMatchObject({
      status: 'failed',
      reason: expect.stringContaining('platform'),
    });
    await handle.dispose();
  });

  it('third_party_disabled_without_flag, and granted permissions never exceed the manifest', async () => {
    const mock = createMockControl({ features: { [THIRD_PARTY_FLAG]: false } });
    const handle = await boot({
      catalog,
      manifests,
      modules,
      platform: platformWith(mock.transport),
      sandboxScripts: { '@acme/plugin-third': '/ui/plugins/third/client.js' },
      grant: () => ['approvals:read', 'approvals:decide'],
    });
    await settle();
    const third = handle.plugins().find((p) => p.id === 'third');
    expect(third).toMatchObject({
      tier: 'third-party',
      status: 'skipped',
      granted: ['approvals:read'],
    });
    await handle.dispose();
  });

  it('dispose_disposes_every_fiber', async () => {
    const mock = createMockControl();
    const handle = await boot({
      catalog,
      manifests,
      modules,
      platform: platformWith(mock.transport),
    });
    await settle();
    const fibers = handle.plugins().flatMap((p) => (p.fiber ? [p.fiber] : []));
    await handle.dispose();
    // Active fibers end DISPOSED; a disposed pending fiber keeps its state
    // but leaves the registry. Either way, nothing is left running.
    expect(
      fibers.every((f) => f.state === FiberStates.disposed || f.state === FiberStates.pending),
    ).toBe(true);
    const live = [...handle.ctx.registry.values()].flatMap((r) => [...r.fibers]);
    expect(live.filter((f) => f.state === FiberStates.active)).toEqual([]);
  });

  it('enable_does_not_run_a_disabled_third_party_plugin_in_the_host', async () => {
    const mock = createMockControl({ features: { [THIRD_PARTY_FLAG]: false } });
    const apply = vi.fn();
    const load = vi.fn(async () => ({ apply }));
    const handle = await boot({
      catalog: [{ id: 'third', name: '@acme/plugin-third', disabled: true }],
      manifests,
      modules: { '@acme/plugin-third': load },
      platform: platformWith(mock.transport),
    });
    try {
      expect(handle.plugins()[0]).toMatchObject({ tier: 'third-party', status: 'disabled' });
      await handle.enable('third');
      await settle();
      expect(load).not.toHaveBeenCalled();
      expect(apply).not.toHaveBeenCalled();
      expect(handle.plugins()[0]).toMatchObject({ tier: 'third-party', status: 'disabled' });
    } finally {
      await handle.dispose();
    }
  });
});

describe('watch', () => {
  it('dispose_aborts_server_streams', async () => {
    const mock = createMockControl();
    const { createClient } = await import('@connectrpc/connect');
    const { approvals } = await import('@loams/proto');
    const client = createClient(approvals.ApprovalService, mock.transport);
    const seen: string[] = [];
    let signal: AbortSignal | undefined;
    const { Context } = await import('@loams/cordis');
    const root = new Context();
    const fiber = root.plugin({
      name: 'watcher',
      apply(ctx: Context) {
        watch(
          ctx,
          (s) => {
            signal = s;
            return client.watchApprovals({}, { signal: s });
          },
          (m) => seen.push(m.event.case ?? 'none'),
        );
      },
    });
    await fiber;
    await settle();
    expect(seen).toEqual(['snapshot']);
    mock.addApproval({ id: 'apr_new', revision: 1n, state: approvals.ApprovalState.PENDING });
    await settle();
    expect(seen).toEqual(['snapshot', 'upsert']);
    await fiber.dispose();
    expect(signal?.aborted).toBe(true);
    mock.addApproval({ id: 'apr_later', revision: 1n, state: approvals.ApprovalState.PENDING });
    await settle();
    expect(seen).toEqual(['snapshot', 'upsert']);
  });
});
