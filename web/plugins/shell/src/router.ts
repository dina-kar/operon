// The `router` service (§37 §5.4, AP1a Ruling 7).
//
// Pages register through `router.page(...)`, never by editing a route
// table: a page is a keyed `console.page` slot entry (key = the page id) and,
// with `nav`, a `console.nav` entry. Disposing the registering plugin
// removes both, so its routes disappear with it.
//
// Scaffold: hash routing (`#/approvals/apr_1`) in both shells, which needs no
// server fallback and survives reloads. A browser
// history router with `basename: '/ui'` follows AP1 Task 0's spike.

import type { Location, PageProps, PageSpec, RouterService } from '@loams/console-host';
import type { SlotRegistry } from '@loams/slots';
import type { ComponentType } from 'react';

interface Route {
  spec: PageSpec;
  pattern: RegExp;
  keys: string[];
}

/** Compiles "/approvals/:id" into a matcher. */
export function compile(path: string): { pattern: RegExp; keys: string[] } {
  const keys: string[] = [];
  const source = path
    .split('/')
    .map((part) => {
      if (part.startsWith(':')) {
        keys.push(part.slice(1));
        return '([^/]+)';
      }
      return part.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    })
    .join('/');
  return { pattern: new RegExp(`^${source}/?$`), keys };
}

export interface HashSource {
  get(): string;
  set(path: string): void;
  listen(listener: () => void): () => void;
}

/** `window.location.hash`, as the default source. */
export const windowHash: HashSource = {
  // The raw hash: decoding happens once, per parameter, in the router.
  get: () => globalThis.location?.hash.replace(/^#/, '') || '/',
  set: (path) => {
    globalThis.location.hash = path;
  },
  listen: (listener) => {
    globalThis.addEventListener?.('hashchange', listener);
    return () => globalThis.removeEventListener?.('hashchange', listener);
  },
};

export class HashRouter implements RouterService {
  #routes: Route[] = [];
  #listeners = new Set<() => void>();
  #current: Location;
  readonly #unlisten: () => void;

  constructor(
    private readonly slots: SlotRegistry,
    private readonly source: HashSource = windowHash,
    private readonly plugin = 'shell',
  ) {
    this.#current = this.#resolve(source.get());
    this.#unlisten = source.listen(() => this.#update());
  }

  page(spec: PageSpec & { plugin?: string }, component: ComponentType<PageProps>): () => void {
    if (this.#routes.some((r) => r.spec.id === spec.id)) {
      throw new Error(`page ${spec.id} is already registered`);
    }
    const plugin = spec.plugin ?? this.plugin;
    const route = { spec, ...compile(spec.path) };
    this.#routes = [...this.#routes, route];
    const disposers = [
      this.slots.register({ name: 'console.page', plugin, key: spec.id }, component),
    ];
    // A parameterized page (`/approvals/:id`) has no concrete link to show.
    if (spec.nav && route.keys.length === 0) {
      disposers.push(
        this.slots.register(
          {
            name: 'console.nav',
            plugin,
            order: spec.nav.order,
            meta: { label: spec.nav.label ?? spec.title, href: spec.path, group: spec.nav.group },
          },
          () => null,
        ),
      );
    }
    this.#update();
    return () => {
      for (const dispose of disposers) dispose();
      this.#routes = this.#routes.filter((r) => r !== route);
      this.#update();
    };
  }

  navigate(to: string): void {
    this.source.set(to);
    this.#update();
  }

  current(): Location {
    return this.#current;
  }

  /** The title of the current page, for the document title. */
  title(): string | undefined {
    return this.#routes.find((r) => r.spec.id === this.#current.pageId)?.spec.title;
  }

  readonly subscribe = (listener: () => void): (() => void) => {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  };

  dispose(): void {
    this.#unlisten();
    this.#listeners.clear();
  }

  #resolve(path: string): Location {
    for (const route of this.#routes) {
      const match = route.pattern.exec(path);
      if (!match) continue;
      const params: Record<string, string> = {};
      try {
        route.keys.forEach((key, i) => {
          params[key] = decodeURIComponent(match[i + 1] ?? '');
        });
      } catch {
        // A malformed escape in a user-supplied hash (`#/approvals/%`):
        // no route matches, and the router keeps working.
        return { path, params: {} };
      }
      return { path, pageId: route.spec.id, params };
    }
    return { path, params: {} };
  }

  #update(): void {
    const next = this.#resolve(this.source.get());
    const same =
      next.path === this.#current.path &&
      next.pageId === this.#current.pageId &&
      JSON.stringify(next.params) === JSON.stringify(this.#current.params);
    if (same) return;
    this.#current = next;
    for (const listener of this.#listeners) listener();
  }
}
