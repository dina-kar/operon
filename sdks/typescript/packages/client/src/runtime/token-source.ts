// Token sources (design §44 §7.4, D608; runtime contract R1).
//
// A `TokenSource` returns a bearer and can be asked for a new one. Tokens
// travel in `Authorization: Bearer` and never in the query string. A `401`
// carrying `reason = token_expired` triggers one refresh and one retry; that
// logic lives in the call path, not here, so a source stays a source.

/**
 * Where a call's bearer comes from.
 *
 * `token()` is called once per attempt, so a source may return a different
 * token each time. `refresh()` is called only after the server said the token
 * expired; a source without one is a source whose token does not expire
 * (an API key), and the runtime then does not retry.
 */
export interface TokenSource {
  /** The bearer to send, or undefined to send no credential at all. */
  token(): Promise<string | undefined>;
  /** Fetch a new token after the server reported the current one expired. */
  refresh?(): Promise<void>;
}

/** A Loams API key. The key does not expire, so there is nothing to refresh. */
export function apiKey(key: string): TokenSource {
  if (key === '') {
    throw new Error('apiKey: the key is empty');
  }
  return { token: () => Promise.resolve(key) };
}

/** A token that is already valid, for a caller that manages its own. */
export function staticToken(token: string): TokenSource {
  if (token === '') {
    throw new Error('staticToken: the token is empty');
  }
  return { token: () => Promise.resolve(token) };
}

/**
 * `LOAMS_API_KEY`, then `LOAMS_TOKEN`, then nothing.
 *
 * The environment is read on every call rather than once at construction, so a
 * process that receives its credentials after the client is built (a
 * sidecar, a test) still authenticates.
 */
export function envToken(
  environment: Record<string, string | undefined> = processEnvironment(),
): TokenSource {
  return {
    token: () =>
      Promise.resolve(environment.LOAMS_API_KEY ?? environment.LOAMS_TOKEN ?? undefined),
  };
}

/**
 * A source that caches a token and calls `fetch` when it is asked to refresh.
 *
 * This is the shape every refreshing source has: one in-flight refresh shared
 * by concurrent callers, so a burst of `401`s produces one token exchange
 * rather than one per request.
 */
export function refreshing(fetchToken: () => Promise<string>): TokenSource {
  let cached: string | undefined;
  let inFlight: Promise<void> | undefined;
  const source: TokenSource = {
    // The first `token()` fetches: a source whose cache starts empty would send
    // no credential at all, and an instance that requires one answers
    // `unauthenticated`, which the call path treats as "the token expired" and
    // retries — with still no credential.
    token: () => {
      if (cached !== undefined) {
        return Promise.resolve(cached);
      }
      return (source.refresh?.() ?? Promise.resolve()).then(() => cached);
    },
    refresh: () => {
      inFlight ??= fetchToken()
        .then((next) => {
          cached = next;
        })
        .finally(() => {
          inFlight = undefined;
        });
      return inFlight;
    },
  };
  return source;
}

/** The RFC 8693 token exchange a person signed in through Authentik needs
 * (design §44 §7.4, D608; §19 §5.2).
 *
 * The instance's `/oauth/token` protocol endpoint takes the identity token and
 * answers with a Loams access token, which is then cached until it expires.
 *
 * Not covered by the conformance suite: the instance serves no OAuth endpoint
 * yet (the auth plan, MT, and API1 Task 7 build it), so this path is written
 * to the documented request and response and cannot be exercised against a
 * live server. `typescript_token_source_refresh` covers the caching and
 * refresh-once-and-retry behaviour that `refreshing()` implements, which is the
 * part the SDK owns.
 */
export function oidcExchange(options: {
  /** The instance's `/oauth/token` endpoint. */
  endpoint: string;
  /** The public OAuth client id (§44 §7.4: the gateway exchanges the token). */
  clientId: string;
  /** Mints the current identity token, from the host's OIDC session. */
  subjectToken: () => Promise<string>;
  /** Injected for tests and for hosts with their own `fetch`. */
  fetch?: typeof globalThis.fetch;
}): TokenSource {
  const doFetch = options.fetch ?? globalThis.fetch;
  const inner = refreshing(async () => {
    const response = await doFetch(options.endpoint, {
      method: 'POST',
      headers: { 'content-type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        grant_type: 'urn:ietf:params:oauth:grant-type:token-exchange',
        subject_token_type: 'urn:ietf:params:oauth:token-type:id_token',
        requested_token_type: 'urn:ietf:params:oauth:token-type:access_token',
        subject_token: await options.subjectToken(),
        client_id: options.clientId,
      }).toString(),
    });
    if (!response.ok) {
      throw new Error(`the token exchange answered ${response.status} ${response.statusText}`);
    }
    const body = (await response.json()) as { access_token?: string };
    if (body.access_token === undefined) {
      throw new Error('the token exchange answered no access_token');
    }
    return body.access_token;
  });
  return {
    token: () => inner.token(),
    refresh: async () => {
      await inner.refresh?.();
    },
  };
}

/** `process.env` where there is one. In a browser there is not, and an
 * `envToken()` there resolves to no credential rather than throwing at import
 * time. */
function processEnvironment(): Record<string, string | undefined> {
  const candidate = (globalThis as { process?: { env?: Record<string, string | undefined> } })
    .process;
  return candidate?.env ?? {};
}
