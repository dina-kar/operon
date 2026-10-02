import { describe, expect, it } from 'vitest';
import runtime from '../static/runtime.js?raw';

declare global {
  // eslint-disable-next-line no-var
  var loams: {
    call(service: string, method: string, input?: unknown): Promise<unknown>;
    ready(): Promise<{ plugin: string; version: string }>;
  };
}

describe('sandbox runtime', () => {
  it('connects through the host port and resolves calls', async () => {
    document.body.innerHTML = '<div id="root"></div>';
    window.location.hash = '#plugin=https%3A%2F%2Fevil.example%2Fx.js';
    // Evaluate the classic script as the frame would.
    new Function(runtime)();
    expect(document.getElementById('root')?.textContent).toContain('Refused');
    const channel = new MessageChannel();
    channel.port1.onmessage = (e) => {
      const m = e.data as { id: number; service: string };
      channel.port1.postMessage(
        m.service === 'rpc.approvals'
          ? { t: 'result', id: m.id, ok: true, value: { approvals: [] } }
          : { t: 'result', id: m.id, ok: false, error: 'needs devices:manage', refused: true },
      );
    };
    const call = globalThis.loams.call('rpc.approvals', 'listApprovals', {});
    window.dispatchEvent(
      new MessageEvent('message', {
        data: { t: 'loams/init', plugin: 'hello', version: '0.1.0' },
        source: window.parent,
        ports: [channel.port2],
      }),
    );
    expect(await globalThis.loams.ready()).toEqual({ plugin: 'hello', version: '0.1.0' });
    expect(await call).toEqual({ approvals: [] });
    await expect(globalThis.loams.call('rpc.devices', 'createPairing')).rejects.toMatchObject({
      message: 'needs devices:manage',
      refused: true,
    });
    channel.port1.close();
  });
});
