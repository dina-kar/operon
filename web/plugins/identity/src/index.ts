// @loams/plugin-identity: the `session` service from WhoAmI (§37 §5.4).
//
// Scaffold: the browser's sign-in pages (§19, today's console) move here in
// AP1a Task 5.
// This plugin only reads who is signed in and keeps the environment
// selection.

import { timestampDate } from '@bufbuild/protobuf/wkt';
import { type PluginModule, type SessionService, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
import type { instance } from '@loams/proto';
import type { EnvironmentRef } from '@loams/slots';

export function createSession(me: instance.WhoAmIResponse | undefined): SessionService {
  const listeners = new Set<() => void>();
  const environments: EnvironmentRef[] = (me?.environments ?? []).map((e) => ({
    id: e.id,
    name: e.name,
    namespace: e.namespace,
    protected: e.protected,
  }));
  let selected = environments[0]?.id;
  return {
    principal: () => me?.principal,
    environments: () => environments,
    environment: () => environments.find((e) => e.id === selected),
    select(id) {
      if (!environments.some((e) => e.id === id)) throw new Error(`unknown environment ${id}`);
      selected = id;
      for (const l of listeners) l();
    },
    authenticatedAt: () => (me?.authenticatedAt ? timestampDate(me.authenticatedAt) : undefined),
    subscribe(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
  };
}

const plugin: PluginModule = {
  name: 'identity',
  inject: ['rpc.instance'],
  async apply(ctx: Context) {
    const client = service(ctx, 'rpc.instance');
    let me: instance.WhoAmIResponse | undefined;
    try {
      me = await client.whoAmI({});
    } catch {
      // Signed out (or an instance without auth, D111): an empty session.
    }
    ctx.provide('session', createSession(me));
  },
};

export default plugin;
