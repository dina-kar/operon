import createClient, { type Middleware } from 'openapi-fetch';
import type { components, paths } from './schema';

/** The contract's schemas (api/console/openapi.json). */
export type Schemas = components['schemas'];
export type ApiError = Schemas['Error'];

let csrf: string | undefined;

/** Set from the session; sent on every mutating request (design §19 §6). */
export function setCsrfToken(token: string | undefined) {
  csrf = token;
}

const csrfHeader: Middleware = {
  onRequest({ request }) {
    if (csrf && request.method !== 'GET') request.headers.set('X-CSRF-Token', csrf);
    return request;
  },
};

/** Typed client for the console API. Paths are absolute: `/api/v1/...`. */
export const api = createClient<paths>({ baseUrl: window.location.origin, credentials: 'include' });
api.use(csrfHeader);

/** The engine's collection description (M1.6 wire contract W7, W8), the fields the console reads. */
export type CollectionInfo = {
  id: number;
  name: string;
  namespace: string;
  partitions: number;
  aliases: string[];
  manifest_version: number;
  live_doc_count: number;
  size_bytes: number;
  created_at_ms: number;
  link_lag_records: number;
  hot: Record<
    'vectors' | 'text' | 'fragments',
    { state: 'off' | 'building' | 'ready'; source_version: number | null }
  >;
  schema: {
    version: number;
    fields: { name: string; kind: unknown }[];
    vectors: { name: string; dim: number; distance: string | null }[];
    sparse_vectors: { name: string; modifier: string }[];
  };
};

/** The data API is outside the console contract, so it is fetched directly. */
export async function listCollections(namespace: string): Promise<CollectionInfo[]> {
  const res = await fetch(`/v1/namespaces/${encodeURIComponent(namespace)}/collections`, {
    credentials: 'include',
  });
  if (!res.ok) throw new Error(`collections: ${res.status}`);
  const body = (await res.json()) as { collections: CollectionInfo[] };
  return body.collections;
}

export function message(error: unknown): string {
  if (error && typeof error === 'object' && 'message' in error) return String(error.message);
  return 'The request failed. Check that the API is reachable and try again.';
}
