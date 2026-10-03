/**
 * Runtime configuration, read from `<base>config.json` when the console
 * boots (issue #269). Nothing here is baked in at build time: the engine
 * serves its own `config.json` (an empty object, so the console talks to
 * the origin it came from), and a hosted deploy such as console.loams.dev
 * publishes one that names a Loams server.
 */
export type RuntimeConfig = {
  /** The Loams server's origin, for example `https://loams.example.com`. Unset means same-origin. */
  server?: string;
};

/** Validates a parsed `config.json`. Unknown keys are ignored; a bad `server` is dropped. */
export function parseRuntimeConfig(raw: unknown): RuntimeConfig {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return {};
  const server = (raw as Record<string, unknown>).server;
  if (typeof server !== 'string' || server.trim() === '') return {};
  let url: URL;
  try {
    url = new URL(server.trim());
  } catch {
    return {};
  }
  if (url.protocol !== 'https:' && url.protocol !== 'http:') return {};
  // Only the origin: API paths are absolute (`/api/v1/...`, `/v1/...`).
  return { server: url.origin };
}

/** Fetches and parses `<base>config.json`; any failure means the defaults. */
export async function loadRuntimeConfig(
  base: string,
  fetcher: typeof fetch = fetch,
  deadlineMs = 3_000,
): Promise<RuntimeConfig> {
  const controller = new AbortController();
  const deadline = setTimeout(() => controller.abort(), deadlineMs);
  try {
    const res = await fetcher(`${base}config.json`, {
      cache: 'no-store',
      signal: controller.signal,
    });
    if (!res.ok) return {};
    const type = res.headers.get('content-type') ?? '';
    // An SPA fallback answers a missing file with index.html.
    if (!type.includes('json')) return {};
    return parseRuntimeConfig(await res.json());
  } catch {
    return {};
  } finally {
    clearTimeout(deadline);
  }
}
