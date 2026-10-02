import { Context } from '@loams/cordis';
import { describe, expect, it } from 'vitest';
import frameHtml from '../../../plugins/sandbox/static/frame.html?raw';
import {
  createBridge,
  decideCall,
  GuardError,
  guard,
  type HostMessage,
  invokeOn,
  isLocalScript,
  mountSandboxed,
  SANDBOX_CSP,
} from '../src/index.js';

describe('guard', () => {
  it('first_party_sees_only_injected_services', async () => {
    const root = new Context();
    await root.plugin({
      name: 'provider',
      apply(c: Context) {
        c.provide('rpc.approvals', { list: () => 'ok' });
        c.provide('platform', { kind: 'web' });
      },
    });
    const seen: unknown[] = [];
    await root.plugin({
      name: 'jobs',
      inject: ['rpc.approvals'],
      apply(c: Context) {
        const g = guard(c, ['rpc.approvals'], 'jobs');
        seen.push((g as unknown as Record<string, { list(): string }>)['rpc.approvals']?.list());
        expect(() => (g as unknown as Record<string, unknown>).platform).toThrow(GuardError);
        expect(() => g.get('platform')).toThrow(GuardError);
        expect(() => g.registry).toThrow(GuardError);
        expect(() => g.plugin).toThrow(GuardError);
        expect(() => g.root).toThrow(GuardError);
        expect(() => (g.emit as (name: string) => void)('approvals/decided')).toThrow(/only emit/);
        // The lifecycle API works.
        g.effect(() => () => seen.push('disposed'));
      },
    });
    expect(seen).toEqual(['ok']);
  });
});

describe('permissions', () => {
  const policy = { services: ['rpc.approvals', 'rpc.devices'], permissions: ['approvals:read'] };

  it('third_party_call_outside_permissions_is_refused', () => {
    expect(decideCall(policy, 'rpc.approvals', 'listApprovals')).toEqual({
      ok: true,
      permission: 'approvals:read',
    });
    expect(decideCall(policy, 'rpc.approvals', 'decideApproval')).toMatchObject({ ok: false });
    expect(decideCall(policy, 'rpc.devices', 'createPairing')).toMatchObject({
      ok: false,
      reason: expect.stringContaining('devices:manage'),
    });
    expect(decideCall(policy, 'rpc.operations', 'listOperations')).toMatchObject({
      ok: false,
      reason: expect.stringContaining('inject'),
    });
    expect(decideCall(policy, 'rpc.approvals', 'watchApprovals')).toMatchObject({ ok: false });
    // A method cannot borrow another service's entry, or an inherited key.
    const rpc = { services: ['rpc'], permissions: ['approvals:read'] };
    expect(decideCall(rpc, 'rpc', 'approvals.listApprovals')).toMatchObject({ ok: false });
    expect(decideCall(rpc, 'rpc', 'constructor')).toMatchObject({ ok: false });
  });

  it('never bridges session, transport or desktop internals', () => {
    const all = { services: ['session', 'transport', 'platform.stacks'], permissions: [] };
    for (const service of all.services) {
      expect(decideCall(all, service, 'anything')).toMatchObject({
        ok: false,
        reason: expect.stringContaining('never'),
      });
    }
  });
});

function nextMessage(port: MessagePort): Promise<HostMessage> {
  return new Promise((resolve) => {
    port.onmessage = (e) => resolve(e.data as HostMessage);
  });
}

describe('bridge', () => {
  it('performs allowed calls and refuses the rest', async () => {
    const channel = new MessageChannel();
    const calls: string[] = [];
    const bridge = createBridge(
      channel.port1,
      {
        pluginId: 'hello',
        version: '0.1.0',
        services: ['rpc.approvals'],
        permissions: ['approvals:read'],
      },
      invokeOn((name) =>
        name === 'rpc.approvals'
          ? {
              listApprovals: (input: unknown) => {
                calls.push(`list ${JSON.stringify(input)}`);
                return { approvals: [] };
              },
              decideApproval: () => calls.push('decide'),
            }
          : undefined,
      ),
    );
    const frame = channel.port2;
    let reply = nextMessage(frame);
    frame.postMessage({
      t: 'call',
      id: 1,
      service: 'rpc.approvals',
      method: 'listApprovals',
      input: {},
    });
    expect(await reply).toEqual({ t: 'result', id: 1, ok: true, value: { approvals: [] } });

    reply = nextMessage(frame);
    frame.postMessage({ t: 'call', id: 2, service: 'rpc.approvals', method: 'decideApproval' });
    expect(await reply).toMatchObject({ t: 'result', id: 2, ok: false, refused: true });
    expect(calls).toEqual(['list {}']);
    expect(bridge.refusals).toHaveLength(1);
    bridge.close();
    frame.close();
  });
});

describe('sandbox', () => {
  it('mounts an opaque-origin frame with no network', () => {
    const container = document.createElement('div');
    const handle = mountSandboxed(container, {
      frameUrl: '/ui/sandbox/frame.html',
      scriptUrl: '/ui/plugins/hello/client.js',
      policy: { pluginId: 'hello', version: '0.1.0', services: [], permissions: [] },
      invoke: async () => undefined,
    });
    const frame = container.querySelector('iframe');
    expect(frame?.getAttribute('sandbox')).toBe('allow-scripts');
    expect(frame?.getAttribute('sandbox')).not.toContain('allow-same-origin');
    expect(frame?.src).toContain('/ui/sandbox/frame.html#plugin=hello');
    handle.dispose();
    expect(container.querySelector('iframe')).toBeNull();
  });

  it('refuses plugin scripts from other origins', () => {
    for (const url of [
      'https://evil.example/x.js',
      '//evil.example/x.js',
      '/ui/../x.js',
      'data:x',
    ]) {
      expect(isLocalScript(url)).toBe(false);
    }
    expect(() =>
      mountSandboxed(document.createElement('div'), {
        frameUrl: '/f.html',
        scriptUrl: 'https://evil.example/x.js',
        policy: { pluginId: 'x', version: '1', services: [], permissions: [] },
        invoke: async () => undefined,
      }),
    ).toThrow(/non-local/);
  });

  it('frame.html carries the same CSP as SANDBOX_CSP (iframe_cannot_reach_network)', () => {
    const html = frameHtml;
    const meta = /http-equiv="Content-Security-Policy"\s+content="([^"]+)"/.exec(html)?.[1];
    expect(meta).toBe(SANDBOX_CSP);
    expect(SANDBOX_CSP).toContain("connect-src 'none'");
    expect(SANDBOX_CSP).not.toContain('unsafe-eval');
  });
});
