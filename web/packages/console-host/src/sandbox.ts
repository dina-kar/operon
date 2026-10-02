// Mounting a third-party plugin in a sandboxed frame (§37 §5.6, AP1a Task 6).
//
// The frame is `<iframe sandbox="allow-scripts">` (no `allow-same-origin`,
// so its origin is opaque: no cookies, no storage, no access to the
// console's DOM) loading `sandbox/frame.html`, which carries
// SANDBOX_CSP in a meta tag (and the engine will send it as a header). The
// iframe `csp` attribute (CSP Embedded Enforcement) is not used: Chrome then
// refuses any frame whose response does not send `Allow-CSP-From`. The in-frame runtime (@loams/plugin-sandbox) loads the
// plugin's script and talks to the host only through the MessagePort it
// receives here.

import { type Bridge, type BridgePolicy, createBridge, type Invoke } from './bridge.js';

/** The CSP every sandbox frame is served with (the frame's own meta tag too). */
export const SANDBOX_CSP =
  "default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; " +
  "img-src data: blob:; connect-src 'none'; form-action 'none'; base-uri 'none'";

/** Only these tokens; never `allow-same-origin`, `allow-top-navigation` or `allow-popups`. */
export const SANDBOX_FLAGS = 'allow-scripts';

export interface SandboxOptions {
  /** The frame document, for example "/ui/sandbox/frame.html". */
  frameUrl: string;
  /** The plugin's script, a path on the console's own origin. */
  scriptUrl: string;
  policy: BridgePolicy;
  invoke: Invoke;
  title?: string;
}

export interface SandboxHandle {
  iframe: HTMLIFrameElement;
  bridge(): Bridge | undefined;
  dispose(): void;
}

/** A same-origin path only: a plugin script is never fetched from elsewhere. */
export function isLocalScript(url: string): boolean {
  return url.startsWith('/') && !url.startsWith('//') && !url.includes('..') && !url.includes(':');
}

export function mountSandboxed(container: HTMLElement, options: SandboxOptions): SandboxHandle {
  if (!isLocalScript(options.scriptUrl)) {
    throw new Error(`refusing a non-local plugin script: ${options.scriptUrl}`);
  }
  const iframe = document.createElement('iframe');
  iframe.setAttribute('sandbox', SANDBOX_FLAGS);
  iframe.setAttribute('referrerpolicy', 'no-referrer');
  iframe.setAttribute('allow', '');
  iframe.title = options.title ?? `Plugin ${options.policy.pluginId}`;
  iframe.className = 'loams-sandbox-frame';
  iframe.dataset.plugin = options.policy.pluginId;
  iframe.src = `${options.frameUrl}#script=${encodeURIComponent(options.scriptUrl)}`;
  let bridge: Bridge | undefined;
  let loaded = false;
  const onLoad = () => {
    if (loaded) {
      // The frame navigated away from frame.html: never hand the new
      // document a port, and cut the old one off.
      bridge?.close();
      bridge = undefined;
      return;
    }
    loaded = true;
    const channel = new MessageChannel();
    bridge = createBridge(channel.port1, options.policy, options.invoke);
    // The frame's origin is opaque, so the target origin can only be "*";
    // the port is what carries trust, and only this frame receives it.
    iframe.contentWindow?.postMessage(
      { t: 'loams/init', plugin: options.policy.pluginId, version: options.policy.version },
      '*',
      [channel.port2],
    );
  };
  iframe.addEventListener('load', onLoad);
  container.append(iframe);
  return {
    iframe,
    bridge: () => bridge,
    dispose() {
      iframe.removeEventListener('load', onLoad);
      bridge?.close();
      iframe.remove();
    },
  };
}
