import { SlotRegistry } from '@loams/slots';
import { describe, expect, it } from 'vitest';
import { compile, HashRouter, type HashSource } from '../src/router.js';

function memory(start = '/'): HashSource & { go(path: string): void } {
  let path = start;
  const listeners = new Set<() => void>();
  return {
    get: () => path,
    set: (p) => {
      path = p;
    },
    listen: (l) => {
      listeners.add(l);
      return () => listeners.delete(l);
    },
    go(p) {
      path = p;
      for (const l of listeners) l();
    },
  };
}

describe('router', () => {
  it('compiles path patterns', () => {
    const { pattern, keys } = compile('/approvals/:id');
    expect(keys).toEqual(['id']);
    expect(pattern.exec('/approvals/apr_1')?.[1]).toBe('apr_1');
    expect(pattern.test('/approvals')).toBe(false);
  });

  it('registers pages as keyed slots with nav entries, and resolves params', () => {
    const slots = new SlotRegistry();
    const source = memory('/approvals/apr_9');
    const router = new HashRouter(slots, source);
    const seen: string[] = [];
    router.subscribe(() => seen.push(router.current().path));
    const dispose = router.page(
      {
        id: 'approval',
        path: '/approvals/:id',
        title: 'Approval',
        nav: { group: 'Operate', order: 1 },
      },
      () => null,
    );
    expect(router.current()).toEqual({
      path: '/approvals/apr_9',
      pageId: 'approval',
      params: { id: 'apr_9' },
    });
    expect(slots.keys('console.page')).toEqual(['approval']);
    // A parameterized page gets no nav link (it would link to `:id`).
    expect(slots.entries('console.nav')).toEqual([]);
    router.page(
      { id: 'list', path: '/approvals', title: 'Approvals', nav: { group: 'Operate', order: 1 } },
      () => null,
    );
    expect(slots.entries('console.nav')[0]?.meta).toEqual({
      label: 'Approvals',
      href: '/approvals',
      group: 'Operate',
    });
    source.go('/elsewhere');
    expect(router.current().pageId).toBeUndefined();
    router.navigate('/approvals/apr_2');
    expect(router.current().params).toEqual({ id: 'apr_2' });
    expect(seen).toEqual(['/approvals/apr_9', '/elsewhere', '/approvals/apr_2']);
    // dispose_removes_slot_entries_and_routes
    dispose();
    expect(slots.keys('console.page')).toEqual(['list']);
    expect(router.current().pageId).toBeUndefined();
  });

  it('refuses a second page with the same id', () => {
    const router = new HashRouter(new SlotRegistry(), memory());
    router.page({ id: 'a', path: '/a', title: 'A' }, () => null);
    expect(() => router.page({ id: 'a', path: '/b', title: 'B' }, () => null)).toThrow(/already/);
  });
});

describe('router hardening', () => {
  it('a malformed escape in the hash matches no route instead of throwing', () => {
    const slots = new SlotRegistry();
    let path = '/approvals/%';
    const router = new HashRouter(slots, {
      get: () => path,
      set: (p) => {
        path = p;
      },
      listen: () => () => {},
    });
    router.page({ id: 'approval', path: '/approvals/:id', title: 'A' }, () => null);
    expect(router.current().pageId).toBeUndefined();
    router.navigate('/approvals/a%2Fb');
    expect(router.current().params).toEqual({ id: 'a/b' });
  });
});
