// The host side of the sandbox bridge (§37 §5.6, D426, AP1a Task 6).
//
// A third-party plugin runs in an iframe with an opaque origin and
// `connect-src 'none'`, so its only way out is this typed RPC over one
// MessageChannel per frame. Each call names a service and a method; the
// bridge checks it against the plugin's policy (`decideCall`) and performs
// it with the host's services.
//
// TODO(AP1a Task 6, after the unified auth plan): calls must carry a vended,
// attenuated token (scopes = manifest permissions ∩ the user's rights,
// `act` = plugin:<id>@<version>, 15-minute TTL) instead of the user's
// credential. Until then third-party plugins are off unless the instance
// sets `console.third_party_plugins` (only the demo mock does).

import { decideCall } from './permissions.js';

export interface BridgePolicy {
  pluginId: string;
  version: string;
  /** The manifest's `inject` list. */
  services: readonly string[];
  /** The manifest's permissions ∩ what the user granted. */
  permissions: readonly string[];
}

export type FrameMessage =
  | { t: 'call'; id: number; service: string; method: string; input?: unknown }
  | { t: 'ready' };

export type HostMessage =
  | { t: 'result'; id: number; ok: true; value: unknown }
  | { t: 'result'; id: number; ok: false; error: string; refused?: boolean };

/** Performs an allowed call with the host's services. */
export type Invoke = (service: string, method: string, input: unknown) => Promise<unknown>;

export interface Bridge {
  /** Every refusal, for the diagnostics page and tests. */
  readonly refusals: { service: string; method: string; reason: string }[];
  close(): void;
}

export function createBridge(port: MessagePort, policy: BridgePolicy, invoke: Invoke): Bridge {
  const refusals: Bridge['refusals'] = [];
  const reply = (message: HostMessage) => port.postMessage(message);
  port.onmessage = async (event: MessageEvent<FrameMessage>) => {
    const message = event.data;
    if (!message || typeof message !== 'object' || message.t !== 'call') return;
    const { id, service, method } = message;
    if (typeof id !== 'number' || typeof service !== 'string' || typeof method !== 'string') {
      return;
    }
    const decision = decideCall(policy, service, method);
    if (!decision.ok) {
      refusals.push({ service, method, reason: decision.reason });
      reply({ t: 'result', id, ok: false, error: decision.reason, refused: true });
      return;
    }
    try {
      reply({ t: 'result', id, ok: true, value: await invoke(service, method, message.input) });
    } catch (error) {
      reply({
        t: 'result',
        id,
        ok: false,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  };
  port.start?.();
  return {
    refusals,
    close() {
      port.onmessage = null;
      port.close();
    },
  };
}

/** Resolves `service.method` on an object of host services. */
export function invokeOn(resolve: (service: string) => unknown): Invoke {
  return async (service, method, input) => {
    const target = resolve(service) as Record<string, unknown> | undefined;
    const fn = target?.[method];
    if (typeof fn !== 'function') throw new Error(`${service}.${method} is not available`);
    return (fn as (input: unknown) => unknown).call(target, input ?? {});
  };
}
