// @loams/plugin-namespaces: the environments the signed-in principal can
// reach, one engine namespace each (§19), from the `session` service.

import { type PluginModule, type SessionService, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
import type { EnvironmentRef } from '@loams/slots';
import { Badge, Card, Empty, StatusTag, Table } from '@loams/ui';
import { useSyncExternalStore } from 'react';

export function NamespacesPage({ session }: { session: SessionService }) {
  useSyncExternalStore(session.subscribe, () => session.environment()?.id);
  const environments = session.environments();
  const selected = session.environment()?.id;
  const who = session.principal();
  return (
    <div className="lc-page">
      <header className="lc-page-head">
        <h1>Namespaces</h1>
        <p>{who ? `Signed in as ${who.displayName}` : 'Not signed in'}</p>
      </header>
      <Card title="Environments" flush>
        <Table<EnvironmentRef>
          caption="Environments and their namespaces"
          rows={environments}
          rowKey={(e) => e.id}
          empty={
            <Empty title="No environments">This principal cannot reach any environment.</Empty>
          }
          columns={[
            { key: 'name', header: 'Environment', cell: (e) => e.name },
            { key: 'namespace', header: 'Namespace', cell: (e) => <Badge>{e.namespace}</Badge> },
            {
              key: 'protected',
              header: 'Protection',
              cell: (e) =>
                e.protected ? <StatusTag status="progress">protected</StatusTag> : null,
            },
            {
              key: 'select',
              header: '',
              cell: (e) =>
                e.id === selected ? (
                  <StatusTag status="done">selected</StatusTag>
                ) : (
                  <button
                    type="button"
                    className="loams-btn loams-btn-quiet loams-btn-sm"
                    onClick={() => session.select(e.id)}
                  >
                    Select
                  </button>
                ),
            },
          ]}
        />
      </Card>
    </div>
  );
}

const plugin: PluginModule = {
  name: 'namespaces',
  inject: ['session', 'router'],
  apply(ctx: Context) {
    const session = service(ctx, 'session');
    const router = service(ctx, 'router');
    ctx.effect(() =>
      router.page(
        {
          id: 'namespaces',
          path: '/namespaces',
          title: 'Namespaces',
          plugin: 'namespaces',
          nav: { group: 'Instance', order: 10 },
        },
        () => <NamespacesPage session={session} />,
      ),
    );
  },
};

export default plugin;
