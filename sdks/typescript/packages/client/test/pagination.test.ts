// SDK2 Task 0's `typescript_pagination_iterator`.
//
// Design §44 §7.4, D617: the facade exposes both the raw page call and an
// iterator, and the iterator follows `next_page_token` to the end. That is one
// function for every paged RPC, not one per list RPC, because the binding says
// which two fields page (the generated `pagination` comes from
// `FacadeOptions.pagination`).
//
// **No RPC is paged yet.** `loams.collection.v1.ListCollections` arrives with
// API1 Task 2, so the conformance half of this test waits for the corpus rather
// than inventing a fixture the server cannot answer. What is pinned now is the
// SDK's half — the token threading, the stop condition, and what happens when a
// binding is not paged — against a stub fetch shaped exactly like a paged
// response. When the RPC lands, `a_paged_call_from_the_corpus_yields_every_item`
// starts doing the end-to-end half instead of skipping.

import { describe, expect, it } from 'vitest';
import { MODULES, type CallBinding } from '../src/gen/facade.js';
import { Loams } from '../src/loams.js';
import { paginate } from '../src/runtime/pagination.js';

/** A paged response, shaped like `ListCollectionsResponse`. */
interface Page {
  collections: { id: string }[];
  nextPageToken: string;
}

/** The binding the first paged RPC will use: `collections.listCollections`. */
const PAGED: CallBinding = {
  module: 'collections',
  name: 'listCollections',
  protoName: 'listCollections',
  method: 'ListCollections',
  rpc: 'loams.collection.v1.CollectionService/ListCollections',
  service: 'loams.collection.v1.CollectionService',
  package: 'loams.collection.v1',
  idempotency: 'no_side_effects',
  retry: 'safe',
  streaming: 'unary',
  pagination: { items: 'collections', nextPageToken: 'nextPageToken' },
};

/** A server that pages: `count` items over `pages` responses. */
function pagedServer(pages: Page[]) {
  const requests: Record<string, unknown>[] = [];
  const fetch = async (request: Record<string, unknown>) => {
    requests.push(request);
    return pages[requests.length - 1] as Page;
  };
  return { fetch, requests };
}

describe('typescript_pagination_iterator', () => {
  it('follows next_page_token to the end and yields items, not pages', async () => {
    const { fetch, requests } = pagedServer([
      { collections: [{ id: 'col_1' }, { id: 'col_2' }], nextPageToken: 'p2' },
      { collections: [{ id: 'col_3' }], nextPageToken: 'p3' },
      { collections: [{ id: 'col_4' }], nextPageToken: '' },
    ]);

    const items: { id: string }[] = [];
    for await (const item of paginate<Record<string, unknown>, Page, { id: string }>(
      PAGED,
      fetch,
      { namespace: 'acme' },
    )) {
      items.push(item);
    }

    expect(items.map((item) => item.id)).toEqual(['col_1', 'col_2', 'col_3', 'col_4']);
    // Three requests, the first with no token and each later one carrying the
    // previous response's token.
    expect(requests).toHaveLength(3);
    expect(requests[0]).toEqual({ namespace: 'acme' });
    expect(requests[1]).toEqual({ namespace: 'acme', pageToken: 'p2' });
    expect(requests[2]).toEqual({ namespace: 'acme', pageToken: 'p3' });
  });

  it('stops on one page, and does not send a second request', async () => {
    const { fetch, requests } = pagedServer([
      { collections: [{ id: 'col_1' }], nextPageToken: '' },
    ]);
    const items: { id: string }[] = [];
    for await (const item of paginate<Record<string, unknown>, Page, { id: string }>(
      PAGED,
      fetch,
      {},
    )) {
      items.push(item);
    }
    expect(items).toHaveLength(1);
    expect(requests).toHaveLength(1);
    // No page token on a request whose page is the first.
    expect(requests[0]).toEqual({});
  });

  it('carries the caller request and the per-call options into every page', async () => {
    const seen: (Record<string, unknown> | undefined)[] = [];
    let page = 0;
    const fetch = async (_request: Record<string, unknown>, options?: unknown) => {
      seen.push(options as Record<string, unknown>);
      page += 1;
      return page < 2
        ? { collections: [{ id: 'col_1' }], nextPageToken: 'p2' }
        : { collections: [], nextPageToken: '' };
    };

    const headers = { 'x-request-id': 'req_1' };
    for await (const _ of paginate<Record<string, unknown>, Page, { id: string }>(
      PAGED,
      fetch,
      { namespace: 'acme' },
      { headers },
    )) {
      // not inspected
    }
    expect(seen).toHaveLength(2);
    for (const options of seen) {
      expect(options?.headers).toEqual(headers);
    }
  });

  it('refuses a binding that is not paged, rather than looping once', async () => {
    const notPaged = MODULES.find((module) => module.name === 'instance')!.calls[0]!;
    expect(notPaged.pagination).toBeNull();
    // `paginate` is an async generator, so the refusal arrives on the first
    // step rather than at the call.
    await expect(
      (async () => {
        for await (const _ of paginate<Record<string, unknown>, Page, unknown>(
          notPaged,
          async () => ({}) as Page,
          {},
        )) {
          // not reached
        }
      })(),
    ).rejects.toThrow(/not a paged call/);
  });

  it('tolerates a page whose items field is absent', async () => {
    // A server that omits an empty repeated field is legal proto3. Yielding
    // nothing and moving on is right; throwing would break a caller over a
    // message the server is allowed to send.
    let page = 0;
    const fetch = async () => {
      page += 1;
      return (page < 2 ? { nextPageToken: 'p2' } : { nextPageToken: '' }) as Page;
    };
    const items: unknown[] = [];
    for await (const item of paginate<Record<string, unknown>, Page, unknown>(PAGED, fetch, {})) {
      items.push(item);
    }
    expect(items).toHaveLength(0);
    expect(page).toBe(2);
  });

  it('a_paged_call_from_the_corpus_yields_every_item', async () => {
    // Skipped until API1 Task 2 lands `ListCollections`: there is nothing to
    // call, and a fixture for an RPC the server does not serve would be a test
    // of the stub rather than of the SDK. The runtime half above is the same
    // function the generated binding would drive.
    const paged = MODULES.flatMap((module) => module.calls).find(
      (call) => call.pagination !== null,
    );
    expect(paged, 'no generated call is paged yet').toBeUndefined();

    // The facade's own entry point resolves a module and call name to a
    // binding, which is what a generated `listAll` alias would be built from.
    const loams = new Loams({ endpoint: 'http://127.0.0.1:1' });
    expect(() => loams.binding('collections', 'listCollections')).toThrow(
      /no generated call/,
    );
    expect(loams.binding('instance', 'getInstance').retry).toBe('safe');
  });
});
