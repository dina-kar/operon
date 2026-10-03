// Pagination (design §44 §7.4; runtime contract R6).
//
// AIP-158: `page_size` and `page_token` in, `next_page_token` out. A generated
// binding says which two fields those are (`FacadeOptions.pagination` is
// `"<items>:<next page token>"`), so the iterator is one function for every
// paged RPC rather than one per list RPC.

import type { CallBinding } from '../gen/facade.js';
import type { CallOptions } from './call.js';

/** The field a paged call is given the page token in, on the request. */
export interface PageRequestOptions {
  /** The request field carrying `page_size`; defaults to `pageSize`. */
  pageSizeField?: string;
  /** The request field carrying `page_token`; defaults to `pageToken`. */
  pageTokenField?: string;
}

/** One page request, as the iterator makes it. */
export type PageFetcher<Request, Response> = (
  request: Request,
  options?: CallOptions,
) => Promise<Response>;

/**
 * Every item of a paged call, following the tokens (D617's "the paging
 * iterator").
 *
 * The caller gets items, not pages: `for await (const c of loams.paginate(...))`.
 * The raw page call is still available on the module, so a caller that wants
 * pages, or wants to stop after one, does not have to use this.
 */
export async function* paginate<Request extends object, Response extends object, Item>(
  binding: CallBinding,
  fetch: PageFetcher<Request, Response>,
  request: Request,
  options: CallOptions & PageRequestOptions = {},
): AsyncGenerator<Item, void, undefined> {
  const pagination = binding.pagination;
  if (pagination === null) {
    throw new Error(
      `${binding.module}.${binding.name} is not a paged call: the proto's facade options name no pagination`,
    );
  }
  const pageSizeField = options.pageSizeField ?? 'pageSize';
  const pageTokenField = options.pageTokenField ?? 'pageToken';
  let pageToken: string | undefined;
  for (;;) {
    const page = (await fetch(
      {
        ...request,
        ...(pageToken === undefined ? {} : { [pageTokenField]: pageToken }),
      } as Request,
      options,
    )) as unknown as Record<string, unknown>;
    const items = page[pagination.items];
    if (Array.isArray(items)) {
      for (const item of items as Item[]) {
        yield item;
      }
    }
    const next = page[pagination.nextPageToken];
    if (typeof next !== 'string' || next === '') {
      return;
    }
    pageToken = next;
  }
}

/** The request field names a paged call uses, from its binding. */
export function pageFields(binding: CallBinding): {
  pageSizeField: string;
  pageTokenField: string;
} {
  if (binding.pagination === null) {
    throw new Error(`${binding.module}.${binding.name} is not a paged call`);
  }
  return { pageSizeField: 'pageSize', pageTokenField: 'pageToken' };
}
