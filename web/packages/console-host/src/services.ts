// The service contracts plugins cooperate through (§37 §5.4, D424). A value
// import from one plugin into another is a build error; plugins share types
// through this module and @loams/proto, and behaviour only through services.
//
// Plugins add their own services to `Services` by declaration merging:
//
//     declare module '@loams/console-host' {
//       interface Services { 'approvals.inbox': InboxStore }
//     }

import type { Client, Transport } from '@connectrpc/connect';
import type { Context } from '@loams/cordis';
import type { approvals, devices, instance, notifications, operations } from '@loams/proto';
import type { EnvironmentRef, SlotRegistry } from '@loams/slots';
import type { ComponentType } from 'react';

/** What differs between the shells the console runs in (today: the browser). */
export interface PlatformService {
  kind: 'web' | 'desktop';
  /** The fetch every request goes through. */
  fetch: typeof globalThis.fetch;
  /** The Connect base URL of the active environment. */
  baseUrl: string;
  openExternal(url: string): Promise<void>;
  notify(n: { title: string; body: string; route?: string }): Promise<void>;
  clipboardWrite(text: string): Promise<void>;
}

/** `GetInstance`'s answer, as plugins gate on it (AP1a Ruling 6). */
export interface FlagsService {
  edition: 'oss' | 'cloud' | 'byoc' | 'unknown';
  instanceName: string;
  serverVersion: string;
  features: Record<string, boolean>;
  apiVersions: string[];
  has(api: string): boolean;
}

export interface PageSpec {
  /** The route id, also the `console.page` key. */
  id: string;
  /** A path pattern, for example "/approvals/:id". */
  path: string;
  title: string;
  nav?: { group: string; order: number; label?: string };
  /** The registering plugin's id, for the error boundary and diagnostics. */
  plugin?: string;
}

export interface PageProps {
  params: Record<string, string>;
  environment?: EnvironmentRef;
}

export interface Location {
  path: string;
  pageId?: string;
  params: Record<string, string>;
}

/** The `router` service (@loams/plugin-shell). */
export interface RouterService {
  /** Registers a page; returns its disposer (call inside `ctx.effect`). */
  page(spec: PageSpec, component: ComponentType<PageProps>): () => void;
  navigate(to: string): void;
  current(): Location;
  subscribe(listener: () => void): () => void;
}

export interface SessionService {
  principal(): instance.Principal | undefined;
  environment(): EnvironmentRef | undefined;
  environments(): EnvironmentRef[];
  select(environmentId: string): void;
  /** When the session last authenticated the person (step-up, AP0 Ruling 7). */
  authenticatedAt(): Date | undefined;
  subscribe(listener: () => void): () => void;
}

/** The typed Connect clients `@loams/plugin-rpc` provides, gated on `api_versions`. */
export interface RpcServices {
  'rpc.instance': Client<typeof instance.InstanceService>;
  'rpc.approvals': Client<typeof approvals.ApprovalService>;
  'rpc.operations': Client<typeof operations.OperationsService>;
  'rpc.devices': Client<typeof devices.DeviceService>;
  'rpc.notifications': Client<typeof notifications.NotificationService>;
}

export interface Services extends RpcServices {
  platform: PlatformService;
  transport: Transport;
  flags: FlagsService;
  slots: SlotRegistry;
  router: RouterService;
  session: SessionService;
}

export type ServiceName = keyof Services;

/**
 * A typed service read: `service(ctx, 'rpc.approvals')`. cordis throws if
 * the plugin did not inject the service, and the guard throws for anything
 * outside the plugin's `inject` list.
 */
export function service<K extends ServiceName>(ctx: Context, name: K): Services[K] {
  return (ctx as unknown as Record<string, Services[K]>)[name] as Services[K];
}

/**
 * Opens a server stream tied to the calling fiber: disposing the plugin
 * aborts the stream (Review Focus 2, `dispose_aborts_server_streams`).
 * `open` receives the abort signal to pass as `{ signal }` to the client.
 */
export function watch<T>(
  ctx: Context,
  open: (signal: AbortSignal) => AsyncIterable<T>,
  onMessage: (message: T) => void,
  onError?: (error: unknown) => void,
): void {
  ctx.effect(() => {
    const controller = new AbortController();
    (async () => {
      try {
        for await (const message of open(controller.signal)) onMessage(message);
      } catch (error) {
        if (!controller.signal.aborted) onError?.(error);
      }
    })();
    return () => controller.abort();
  }, 'watch');
}
