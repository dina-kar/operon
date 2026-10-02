import { createMockControl } from '@loams/console-host/testing';
import type { Context } from '@loams/cordis';
import { Context as Root } from '@loams/cordis';
import { describe, expect, it } from 'vitest';
import rpc, { RPC_SERVICES } from '../src/index.js';

describe('@loams/plugin-rpc', () => {
  it('rpc_service_provided_only_when_api_listed', async () => {
    const mock = createMockControl({ apiVersions: ['loams.instance.v1', 'loams.approvals.v1'] });
    const root = new Root();
    await root.plugin({
      name: 'deps',
      apply(ctx: Context) {
        ctx.provide('transport', mock.transport);
        ctx.provide('flags', { has: (api: string) => ['loams.approvals.v1'].includes(api) });
      },
    });
    await root.plugin({ name: 'rpc', inject: rpc.inject, apply: rpc.apply });
    const provided = RPC_SERVICES.map((s) => s.name).filter((n) => root.get(n) !== undefined);
    expect(provided).toEqual(['rpc.instance', 'rpc.approvals']);
    const client = root.get('rpc.approvals') as {
      listApprovals(r: object): Promise<{ approvals: unknown[] }>;
    };
    expect((await client.listApprovals({})).approvals.length).toBe(2);
  });
});
