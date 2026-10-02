// The guard proxy for first-party plugins (§37 §5.6): `ctx` exposes only
// the services in the plugin's `inject` list plus the lifecycle API. This is
// hygiene, not a security boundary (first-party code runs in the console's
// realm); third-party code runs in a sandboxed frame instead.
//
// cordis already refuses `ctx.<service>` for a service that was not
// injected; the guard also closes the side doors: `ctx.get(name)`, the
// registry, `ctx.plugin` (a sub-plugin could inject anything), `ctx.root`.

import type { Context } from '@loams/cordis';

/** The cordis API a guarded plugin may use besides its services. */
const LIFECYCLE = new Set(['effect', 'on', 'fiber', 'logger', 'emit']);

export class GuardError extends Error {
  override name = 'GuardError';
}

export function guard(ctx: Context, inject: readonly string[], pluginId: string): Context {
  const allowed = new Set(inject);
  return new Proxy(ctx, {
    get(target, prop, receiver) {
      if (typeof prop === 'symbol') return Reflect.get(target, prop, receiver);
      if (prop === 'provide') {
        // Non-core plugins provide only `<plugin-id>.*` services (§37 §5.3).
        return (name: string, ...args: unknown[]) => {
          if (!name.startsWith(`${pluginId}.`)) {
            throw new GuardError(`${pluginId} may only provide "${pluginId}.*" services`);
          }
          return (target.provide as (n: string, ...a: unknown[]) => unknown)(name, ...args);
        };
      }
      if (prop === 'emit') {
        // Plugins emit only their own events, `<plugin-id>/...`.
        return (name: string, ...args: unknown[]) => {
          if (!name.startsWith(`${pluginId}/`)) {
            throw new GuardError(`${pluginId} may only emit "${pluginId}/..." events`);
          }
          return (target.emit as (n: string, ...a: unknown[]) => unknown)(name, ...args);
        };
      }
      if (LIFECYCLE.has(prop)) {
        const value = Reflect.get(target, prop, target);
        return typeof value === 'function' ? value.bind(target) : value;
      }
      if (allowed.has(prop)) return Reflect.get(target, prop, target);
      throw new GuardError(`${pluginId} did not inject "${prop}"`);
    },
    set(_target, prop) {
      throw new GuardError(`${pluginId} cannot set ctx.${String(prop)}`);
    },
  });
}
