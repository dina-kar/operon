// @loams/plugin-shell: the layout (the `root` slot), the navigation (from
// `console.nav`), the page outlet (`console.page`) and the `router` service
// (§37 §5.4–§5.5). Components get props and closures, never `ctx`.

import { type PluginModule, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
import { Slot, type SlotRegistry, useSlot } from '@loams/slots';
import { Empty, Logo } from '@loams/ui';
import { useEffect, useSyncExternalStore } from 'react';
import { HashRouter } from './router.js';

export { compile, HashRouter, type HashSource, windowHash } from './router.js';

function Nav({ router }: { router: HashRouter }) {
  const entries = useSlot('console.nav');
  const location = useSyncExternalStore(router.subscribe, () => router.current());
  const groups = new Map<string, typeof entries>();
  for (const entry of entries) {
    const group = entry.meta?.group ?? 'Console';
    groups.set(group, [...(groups.get(group) ?? []), entry]);
  }
  return (
    <nav className="lc-nav" aria-label="Console">
      {[...groups].map(([group, items]) => (
        <div key={group} className="lc-nav-group">
          <p className="lc-nav-heading">{group}</p>
          <ul>
            {items.map((entry) => {
              const href = entry.meta?.href ?? '/';
              const active =
                location.path === href || (href !== '/' && location.path.startsWith(`${href}/`));
              return (
                <li key={entry.id}>
                  <a href={`#${href}`} aria-current={active ? 'page' : undefined}>
                    {entry.meta?.label}
                  </a>
                </li>
              );
            })}
          </ul>
        </div>
      ))}
    </nav>
  );
}

function Outlet({ router }: { router: HashRouter }) {
  const location = useSyncExternalStore(router.subscribe, () => router.current());
  const title = location.pageId ? router.title() : undefined;
  useEffect(() => {
    document.title = title ? `${title} · Loams` : 'Loams';
  }, [title]);
  if (!location.pageId) {
    return (
      <Empty title="Nothing here">
        No plugin serves <code>{location.path}</code>. It may be disabled, or waiting for an API
        this instance does not serve.
      </Empty>
    );
  }
  return (
    <Slot
      name="console.page"
      slotKey={location.pageId}
      props={{ params: location.params }}
      fallback={<Empty title="Loading…" />}
    />
  );
}

function Layout({ router }: { router: HashRouter }) {
  return (
    <div className="lc-shell">
      <aside className="lc-side">
        <a className="lc-brand" href="#/">
          <Logo />
        </a>
        <Nav router={router} />
      </aside>
      <main className="lc-main">
        <div className="lc-overlays">
          <Slot name="shell.overlay" props={{}} />
        </div>
        <Outlet router={router} />
      </main>
    </div>
  );
}

const plugin: PluginModule = {
  name: 'shell',
  inject: ['slots'],
  apply(ctx: Context) {
    const slots = service(ctx, 'slots') as SlotRegistry;
    const router = new HashRouter(slots);
    ctx.effect(() => () => router.dispose(), 'router');
    ctx.provide('router', router);
    ctx.effect(() =>
      slots.register({ name: 'root', plugin: 'shell' }, () => <Layout router={router} />),
    );
  },
};

export default plugin;
