// frame.html opened directly (or framed without `sandbox`) has the console's
// own origin: the runtime must not run plugin code there.
import { describe, expect, it } from 'vitest';
import runtime from '../static/runtime.js?raw';

describe('sandbox runtime outside a sandboxed frame', () => {
  it('iframe_cannot_read_parent_dom: plugin code never runs with the console origin', () => {
    expect(window.origin).not.toBe('null');
    document.body.innerHTML = '<div id="root"></div>';
    window.location.hash = '#plugin=hello';
    new Function(runtime)();
    expect(document.getElementById('root')?.textContent).toContain('outside a sandboxed frame');
    const channel = new MessageChannel();
    window.dispatchEvent(
      new MessageEvent('message', {
        data: { t: 'loams/init', plugin: 'hello', version: '0.1.0' },
        source: window.parent,
        ports: [channel.port2],
      }),
    );
    expect(document.head.querySelectorAll('script')).toHaveLength(0);
    channel.port1.close();
  });
});
