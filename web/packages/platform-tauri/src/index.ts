// @loams/platform-tauri: the desktop's `platform`, `transport` and
// `platform.stacks` (§37 §5.4, §6). Provided only inside Loams Desktop, so
// plugins that inject `platform.stacks` appear only there.

import type { Transport } from '@connectrpc/connect';
import { createConnectTransport } from '@connectrpc/connect-web';
import type { PlatformService, PluginModule } from '@loams/console-host';
import type { Context } from '@loams/cordis';
import { invoke as tauriInvoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';
import { openUrl } from '@tauri-apps/plugin-opener';
import { createTauriFetch, type Invoke } from './fetch.js';

export { createTauriFetch, type FetchEvent, type Invoke } from './fetch.js';

/** `sidecar_status` (lib.rs `SidecarStatus`, sidecar.rs `SidecarState`). */
export interface SidecarStatus {
  state: 'stopped' | 'starting' | 'running' | 'restarting' | 'crashed' | 'unavailable';
  url?: string;
  pid?: number | null;
  port?: number;
  reason?: string;
  exit?: number | null;
  attempt?: number;
  delay_ms?: number;
  log_tail: string[];
  binary?: string | null;
}

/** `envs_list` (lib.rs `EnvView`). */
export interface EnvView {
  id: string;
  name: string;
  kind: 'local' | 'remote';
  base_url: string | null;
  active: boolean;
  signed_in: boolean;
}

/** The `platform.stacks` service: the local sidecar and the environments. */
export interface StacksService {
  status(): Promise<SidecarStatus>;
  start(): Promise<SidecarStatus>;
  stop(): Promise<SidecarStatus>;
  restart(): Promise<SidecarStatus>;
  environments(): Promise<EnvView[]>;
  /** Selects an environment and reloads the console onto it. */
  select(id: string): Promise<void>;
  signIn(id: string): Promise<void>;
  /** Calls `listener` when the sidecar's state changes. */
  onChange(listener: () => void): Promise<() => void>;
}

declare module '@loams/console-host' {
  interface Services {
    'platform.stacks': StacksService;
  }
}

export interface TauriPlatformOptions {
  /** The active environment's Connect base URL. */
  baseUrl: string;
  /** Use this transport instead of the bridge (the demo mode). */
  transport?: Transport;
  invoke?: Invoke;
  reload?: () => void;
}

export function createStacks(invoke: Invoke, reload: () => void): StacksService {
  return {
    status: () => invoke('sidecar_status'),
    start: () => invoke('sidecar_start'),
    stop: () => invoke('sidecar_stop'),
    restart: () => invoke('sidecar_restart'),
    environments: () => invoke('envs_list'),
    async select(id) {
      await invoke('envs_select', { id });
      reload();
    },
    async signIn(envId) {
      await invoke('auth_sign_in', { envId });
      reload();
    },
    onChange: (listener) => listen('desktop/sidecar', () => listener()),
  };
}

export function createTauriPlatform(options: TauriPlatformOptions): PluginModule {
  const invoke = options.invoke ?? tauriInvoke;
  const reload = options.reload ?? (() => globalThis.location.reload());
  return {
    name: 'platform-tauri',
    apply(ctx: Context) {
      const fetch = createTauriFetch(invoke);
      const platform: PlatformService = {
        kind: 'desktop',
        fetch,
        baseUrl: options.baseUrl,
        // Scoped by capabilities/main.json (opener:allow-open-url).
        openExternal: (url) => openUrl(url),
        // TODO(AP1 Task 8): native notifications (tauri-plugin-notification).
        notify: async () => {},
        clipboardWrite: (text) => navigator.clipboard.writeText(text),
      };
      ctx.provide('platform', platform);
      ctx.provide(
        'transport',
        options.transport ??
          createConnectTransport({ baseUrl: options.baseUrl, useBinaryFormat: true, fetch }),
      );
      ctx.provide('platform.stacks', createStacks(invoke, reload));
    },
  };
}
