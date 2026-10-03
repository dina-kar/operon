// SDK2 Task 0's `typescript_token_source_refresh`.
//
// Design §44 §7.4, D608 and runtime contract R1: a `401` carrying
// `reason = token_expired` gets **exactly one** refresh and **one** retry.
// Both halves matter. Refreshing more than once turns an auth outage into a
// refresh storm; retrying without refreshing replays a token the server has
// already rejected.
//
// What is pinned here is the SDK's half: the refresh-once-and-retry loop, and
// the sharing of one in-flight refresh across concurrent callers. The token
// exchange itself (`oidcExchange`) is written to the documented protocol and is
// not exercised, because the instance serves no OAuth endpoint yet; that is
// stated in `token-source.ts` and in the PR.
//
// The loop is pinned against a real HTTP server, because the interesting part
// is what goes out on the wire: one call with the stale bearer, then one with
// the fresh one.

import { Code, ConnectError } from '@connectrpc/connect';
import { create } from '@bufbuild/protobuf';
import { toBinary } from '@bufbuild/protobuf';
import { ErrorInfoSchema } from '@loams/proto/errors';
import { createServer, type Server } from 'node:http';
import { describe, expect, it } from 'vitest';
import { InstanceService } from '@loams/proto/instance';
import { MODULES } from '../src/gen/facade.js';
import { CallInvoker, callWithRetry } from '../src/runtime/call.js';
import { TokenExpiredError } from '../src/runtime/errors.js';
import {
  apiKey,
  envToken,
  oidcExchange,
  refreshing,
  staticToken,
  type TokenSource,
} from '../src/runtime/token-source.js';

const GET_INSTANCE = MODULES.find((module) => module.name === 'instance')!.calls.find(
  (call) => call.name === 'getInstance',
)!;

/** A server that answers `401 token_expired` while the bearer is stale, and
 * `GetInstance` once it is fresh. It records every bearer it saw. */
async function withAuthServer(
  run: (endpoint: string, bearers: (string | undefined)[]) => Promise<void>,
): Promise<void> {
  const bearers: (string | undefined)[] = [];
  const server: Server = createServer((request, response) => {
    const bearer = request.headers.authorization;
    bearers.push(bearer);
    if (bearer !== 'Bearer fresh') {
      const info = create(ErrorInfoSchema, { reason: 'token_expired' });
      response.writeHead(401, { 'content-type': 'application/json' });
      response.end(
        JSON.stringify({
          code: 'unauthenticated',
          message: 'the access token expired',
          details: [
            {
              type: 'loams.errors.v1.ErrorInfo',
              value: Buffer.from(toBinary(ErrorInfoSchema, info)).toString('base64'),
            },
          ],
        }),
      );
      return;
    }
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end(JSON.stringify({ instanceId: '01', name: 'Loams', apiVersions: [] }));
  });
  await new Promise<void>((resolvePromise) => server.listen(0, '127.0.0.1', resolvePromise));
  const address = server.address();
  const port = typeof address === 'object' && address !== null ? address.port : 0;
  try {
    await run(`http://127.0.0.1:${port}`, bearers);
  } finally {
    await new Promise<void>((resolvePromise) => server.close(() => resolvePromise()));
  }
}

describe('typescript_token_source_refresh', () => {
  it('refreshes once and retries once when the server says the token expired', async () => {
    await withAuthServer(async (endpoint, bearers) => {
      let stale = true;
      let exchanges = 0;
      // The shape every refreshing source has: a token, and a way to get a new
      // one. `stale` flips on refresh, which is what makes the second attempt
      // carry a token the server accepts.
      const source: TokenSource = {
        token: () => Promise.resolve(stale ? 'stale' : 'fresh'),
        refresh: () => {
          exchanges += 1;
          stale = false;
          return Promise.resolve();
        },
      };

      const { createConnectTransport } = await import('@connectrpc/connect-web');
      const invoker = new CallInvoker(
        createConnectTransport({ baseUrl: endpoint, useBinaryFormat: false }),
        source,
        1,
        undefined,
      );
      invoker.register(GET_INSTANCE.service, InstanceService);

      const info = (await invoker.unary(GET_INSTANCE, {})) as { name?: string };
      expect(info.name).toBe('Loams');
      // Two attempts, one refresh: the stale bearer, then the fresh one.
      expect(bearers).toEqual(['Bearer stale', 'Bearer fresh']);
      expect(exchanges).toBe(1);
    });
  });

  it('refreshes once for a burst of concurrent callers', async () => {
    let exchanges = 0;
    const source = refreshing(async () => {
      exchanges += 1;
      // Long enough that every concurrent caller is waiting on the same one.
      await new Promise((resolvePromise) => setTimeout(resolvePromise, 10));
      return `token-${exchanges}`;
    });

    await Promise.all([
      source.token(),
      source.refresh?.(),
      source.refresh?.(),
      source.refresh?.(),
      source.token(),
    ]);

    // Three refreshes asked for, one exchange made: an instance whose token
    // endpoint is slow must not see one request per 401.
    expect(exchanges).toBe(1);
    expect(await source.token()).toBe('token-1');
  });

  it('refreshes once and does not loop when the fresh token is refused too', async () => {
    // The refresh is offered once per logical call. A second `token_expired`
    // after that is reported: an instance whose token endpoint is broken must
    // not be asked to exchange again on every attempt.
    let refreshes = 0;
    const expired = () => {
      const info = create(ErrorInfoSchema, { reason: 'token_expired' });
      const refused = new ConnectError('the access token expired', Code.Unauthenticated);
      refused.details.push({
        type: ErrorInfoSchema.typeName,
        value: toBinary(ErrorInfoSchema, info),
      });
      return refused;
    };
    let attempts = 0;
    const error = await callWithRetry(
      {},
      async () => {
        attempts += 1;
        throw expired();
      },
      {
        retrySafe: false,
        maxRetries: 3,
        onRefresh: async () => {
          refreshes += 1;
        },
      },
      GET_INSTANCE.rpc,
    ).then(
      () => undefined,
      (thrown: unknown) => thrown,
    );

    expect(refreshes).toBe(1);
    expect(attempts).toBe(2);
    expect(error).toBeInstanceOf(TokenExpiredError);
  });

  it('maps token_expired to TokenExpiredError, which is what drives the refresh', async () => {
    const info = create(ErrorInfoSchema, { reason: 'token_expired' });
    const refused = new ConnectError('expired', Code.Unauthenticated);
    refused.details.push({
      type: ErrorInfoSchema.typeName,
      value: toBinary(ErrorInfoSchema, info),
    });
    expect(refused).toBeInstanceOf(ConnectError);
    const { toLoamsError } = await import('../src/runtime/errors.js');
    expect(toLoamsError(refused)).toBeInstanceOf(TokenExpiredError);
  });

  it('reads credentials from the environment, preferring an API key', async () => {
    const environment = { LOAMS_API_KEY: 'key-1', LOAMS_TOKEN: 'token-1' };
    expect(await envToken(environment).token()).toBe('key-1');
    expect(await envToken({ LOAMS_TOKEN: 'token-1' }).token()).toBe('token-1');
    // No credential is not an error: `GetInstance` needs none, and a client
    // that cannot see `process` (a browser) must still construct.
    expect(await envToken({}).token()).toBeUndefined();
    // The ambient environment is read at call time, so a process that has no
    // Loams credentials resolves to none rather than throwing.
    expect([undefined, 'key', 'token']).toContain(await envToken().token());
  });

  it('refuses an empty credential rather than sending a bare bearer', () => {
    expect(() => apiKey('')).toThrow();
    expect(() => staticToken('')).toThrow();
  });

  it('exchanges an identity token for a Loams access token (RFC 8693)', async () => {
    // The request shape is what the protocol documents; the instance has no
    // OAuth endpoint yet, so this asserts the request rather than a round trip.
    const seen: { url: string; body: URLSearchParams }[] = [];
    const fakeFetch = (async (url: string, init: RequestInit) => {
      seen.push({ url, body: new URLSearchParams(String(init.body)) });
      return new Response(JSON.stringify({ access_token: 'loams-token' }), { status: 200 });
    }) as unknown as typeof globalThis.fetch;

    const source = oidcExchange({
      endpoint: 'https://acme.loams.dev/oauth/token',
      clientId: 'console',
      subjectToken: () => Promise.resolve('id-token'),
      fetch: fakeFetch,
    });

    // The exchange is driven by a refresh, because that is when the runtime
    // decides the current token is no good.
    await source.refresh?.();
    expect(await source.token()).toBe('loams-token');
    expect(seen).toHaveLength(1);
    expect(seen[0]?.url).toBe('https://acme.loams.dev/oauth/token');
    expect(seen[0]?.body.get('grant_type')).toBe(
      'urn:ietf:params:oauth:grant-type:token-exchange',
    );
    expect(seen[0]?.body.get('subject_token')).toBe('id-token');
    expect(seen[0]?.body.get('requested_token_type')).toBe(
      'urn:ietf:params:oauth:token-type:access_token',
    );

    // A second refresh is a second exchange; a plain read is not.
    await source.refresh?.();
    expect(seen).toHaveLength(2);
  });

  it('reports a token endpoint that refuses', async () => {
    const fakeFetch = (async () =>
      new Response('{}', { status: 403 })) as unknown as typeof globalThis.fetch;
    const source = oidcExchange({
      endpoint: 'https://acme.loams.dev/oauth/token',
      clientId: 'console',
      subjectToken: () => Promise.resolve('id-token'),
      fetch: fakeFetch,
    });
    await expect(source.refresh?.()).rejects.toThrow(/403/);
  });
});
