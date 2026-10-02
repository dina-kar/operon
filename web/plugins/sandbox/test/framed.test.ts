// The runtime in an opaque-origin, framed document, as the console mounts it.
import { describe, expect, it } from 'vitest';
import runtime from '../static/runtime.js?raw';

declare global {
  // eslint-disable-next-line no-var
  var loams: { ready(): Promise<{ plugin: string; version: string }> };
}

describe('sandbox runtime in a sandboxed frame', () => {
  it('loads the plugin only after the host hands over the port', async () => {
    const parent = {} as Window;
    Object.defineProperty(window, 'origin', { value: 'null', configurable: true });
    Object.defineProperty(window, 'parent', { value: parent, configurable: true });
    document.body.innerHTML = '<div id="root"></div>';
    window.location.hash = '#plugin=hello';
    new Function(runtime)();
    const scripts = () => [...document.head.querySelectorAll('script')].map((s) => s.src);
    // No plugin code before the port: it could navigate the frame first.
    expect(scripts()).toEqual([]);

    // A message from anything but the embedding console is ignored.
    const stray = new MessageChannel();
    window.dispatchEvent(
      new MessageEvent('message', {
        data: { t: 'loams/init', plugin: 'hello', version: '0.1.0' },
        source: window,
        ports: [stray.port2],
      }),
    );
    expect(scripts()).toEqual([]);

    const channel = new MessageChannel();
    window.dispatchEvent(
      new MessageEvent('message', {
        data: { t: 'loams/init', plugin: 'hello', version: '0.1.0' },
        source: parent,
        ports: [channel.port2],
      }),
    );
    expect(await globalThis.loams.ready()).toEqual({ plugin: 'hello', version: '0.1.0' });
    expect(scripts()).toHaveLength(1);
    expect(new URL(scripts()[0] ?? '').pathname).toBe('/plugins/hello/client.js');
    channel.port1.close();
    stray.port1.close();
  });
});
