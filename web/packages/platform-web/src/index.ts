// @loams/platform-web: `platform` and `transport` in a browser (§37 §5.4).
//
// The browser console keeps the cookie session (§19 §6): requests go with
// `credentials: 'include'`. For local development against
// `loams-apps-mock`, `devBearer` adds the mock's fake token; it is never set
// in a production build. `transport` lets the demo mode pass an in-memory
// mock instead of a network transport.

import type { Transport } from '@connectrpc/connect';
import { createConnectTransport } from '@connectrpc/connect-web';
import type { PlatformService, PluginModule } from '@loams/console-host';
import type { Context } from '@loams/cordis';

export interface WebPlatformOptions {
  /** The Connect base URL; default: this page's origin. */
  baseUrl?: string;
  /** Development only: `Authorization: Bearer <devBearer>` on every request. */
  devBearer?: string;
  /** Use this transport instead of the network (the demo mode). */
  transport?: Transport;
}

export function webFetch(devBearer?: string): typeof globalThis.fetch {
  return (input, init) => {
    const headers = new Headers(init?.headers);
    if (devBearer) headers.set('authorization', `Bearer ${devBearer}`);
    return globalThis.fetch(input, { ...init, headers, credentials: 'include' });
  };
}

export function createWebPlatform(options: WebPlatformOptions = {}): PluginModule {
  return {
    name: 'platform-web',
    apply(ctx: Context) {
      const baseUrl = options.baseUrl ?? globalThis.location?.origin;
      if (!baseUrl) throw new Error('platform-web: no baseUrl and no page origin');
      const fetch = webFetch(options.devBearer);
      const platform: PlatformService = {
        kind: 'web',
        fetch,
        baseUrl,
        async openExternal(url) {
          // Web links only: never `javascript:`, `data:` or a custom scheme.
          const { protocol } = new URL(url, baseUrl);
          if (protocol !== 'https:' && protocol !== 'http:') {
            throw new Error(`openExternal refuses ${protocol} URLs`);
          }
          globalThis.open?.(url, '_blank', 'noopener,noreferrer');
        },
        async notify({ title, body }) {
          if (typeof Notification === 'undefined') return;
          if (Notification.permission === 'default') await Notification.requestPermission();
          if (Notification.permission === 'granted') new Notification(title, { body });
        },
        async clipboardWrite(text) {
          await navigator.clipboard.writeText(text);
        },
      };
      ctx.provide('platform', platform);
      ctx.provide(
        'transport',
        options.transport ?? createConnectTransport({ baseUrl, useBinaryFormat: true, fetch }),
      );
    },
  };
}
