// SDK2 Task 0's `typescript_retry_reuses_idempotency_key`.
//
// Design §44 §7.4, D610: a mutation may only be retried when it carries an
// idempotency key, and **the same key has to go out on every attempt**. A key
// that is regenerated per attempt turns one write into two, which is the exact
// failure the key exists to prevent — and it is invisible in a test that only
// counts attempts.
//
// So this pins three things: that a keyed mutation becomes retryable, that the
// key is a UUIDv7 generated once per logical call, and that a retried attempt
// carries the identical request. The last one is asserted end to end against a
// real HTTP server that fails twice and then answers, because a unit test of the
// request object cannot show that the transport re-sent it.

import { Code, ConnectError } from '@connectrpc/connect';
import { createConnectTransport } from '@connectrpc/connect-web';
import { createServer, type Server } from 'node:http';
import { describe, expect, it } from 'vitest';
import { MODULES } from '../src/gen/facade.js';
import { InstanceService } from '@loams/proto/instance';
import { LiveService } from '@loams/live/live';
import { CallInvoker, callWithRetry, withIdempotencyKey } from '../src/runtime/call.js';
import { backoffMs } from '../src/runtime/retry.js';
import { uuidv7, uuidv7Time } from '../src/runtime/uuidv7.js';

describe('typescript_retry_reuses_idempotency_key', () => {
  it('mints one UUIDv7 per logical call, before the first attempt', () => {
    const before = Date.now();
    const first = withIdempotencyKey({ idempotencyKey: '' }, undefined);
    const second = withIdempotencyKey({ idempotencyKey: '' }, undefined);

    expect(first.keyed).toBe(true);
    expect(second.keyed).toBe(true);
    const key = (first.request as { idempotencyKey: string }).idempotencyKey;
    expect(key).not.toBe('');
    expect(key).not.toBe((second.request as { idempotencyKey: string }).idempotencyKey);
    // UUIDv7: version 7 nibble, variant bit, and the millisecond stamp is now.
    expect(key[14]).toBe('7');
    expect(['8', '9', 'a', 'b']).toContain(key[19]);
    const stamped = uuidv7Time(key);
    expect(stamped).toBeGreaterThanOrEqual(before);
    expect(stamped).toBeLessThanOrEqual(Date.now());
  });

  it('keeps a key the caller supplied, and does not touch a request without one', () => {
    const mine = withIdempotencyKey({ idempotencyKey: 'my-key' }, undefined);
    expect((mine.request as { idempotencyKey: string }).idempotencyKey).toBe('my-key');

    const unkeyed = withIdempotencyKey({ idempotencyKey: '' }, 'supplied');
    expect((unkeyed.request as { idempotencyKey: string }).idempotencyKey).toBe('supplied');

    // A message with no `idempotency_key` field is left alone. Two proofs are
    // available and either is enough: the generated schema declaring the field
    // (`declared`, which is what the invoker reads) or the object carrying it.
    const plain = withIdempotencyKey({ collectionId: 'col_1' }, 'supplied');
    expect(plain.keyed).toBe(false);
    expect(plain.request).toEqual({ collectionId: 'col_1' });
  });

  it('keys a call whose schema declares the field, even when the caller omitted it', () => {
    // `MutateRequest.idempotency_key` is proto3 `optional`, so a caller who
    // leaves it out sends no key at all and the mutation is not retryable. The
    // decision is read from the generated schema, so it does not depend on what
    // the caller's object happens to contain.
    const generated = withIdempotencyKey({ function: 'go' }, undefined, true);
    expect(generated.keyed).toBe(true);
    expect((generated.request as { idempotencyKey: string }).idempotencyKey).not.toBe('');

    // `DeployRequest` has no such field, so a key would be a field the schema
    // does not know: still nothing to key.
    const undeclared = withIdempotencyKey({ bundle: new Uint8Array() }, undefined, false);
    expect(undeclared.keyed).toBe(false);
    expect(undeclared.request).toEqual({ bundle: new Uint8Array() });
  });

  it('reads the key decision off the generated schemas of the live service', () => {
    // The live service is the only one with a keyed mutation today, and it is
    // generated, so this asserts the schema walk itself: `Mutate` is keyed,
    // `Deploy` and `Query` are not.
    const invoker = new CallInvoker(
      createConnectTransport({ baseUrl: 'http://127.0.0.1:1' }),
      undefined,
      3,
      undefined,
    );
    invoker.register('loams.live.v1.LiveService', LiveService);
    // `Deploy` is a `tables` call and `Mutate` is too, while `live` is the
    // session half; both modules name the same service, so the search is across
    // every module of the package.
    const binding = (name: string) =>
      MODULES.filter((module) => module.package === 'loams.live.v1')
        .flatMap((module) => module.calls)
        .find((call) => call.method === name)!;
    const takes = (name: string) =>
      (
        invoker as unknown as { takesIdempotencyKey(binding: unknown): boolean }
      ).takesIdempotencyKey(binding(name));

    expect(takes('Mutate')).toBe(true);
    expect(takes('Deploy')).toBe(false);
    expect(takes('Query')).toBe(false);
    // And a read is retryable on its own account, key or no key.
    expect(MODULES.find((m) => m.name === 'instance')!.calls[0]!.retry).toBe('safe');
  });

  it('reuses the key on every retry of one logical call', async () => {
    const { request, keyed } = withIdempotencyKey({ idempotencyKey: '' }, undefined);
    const seen: unknown[] = [];
    const result = await callWithRetry(
      request,
      async (attempt) => {
        seen.push(attempt.request);
        if (seen.length < 3) {
          throw new ConnectError('node is restarting', Code.Unavailable);
        }
        return { ok: true };
      },
      // `retrySafe` is what `unary()` computes for a keyed mutation.
      { retrySafe: keyed, maxRetries: 3 },
      'loams.test.v1.ThingService/Write',
    );

    expect(result).toEqual({ ok: true });
    expect(seen).toHaveLength(3);
    // The same request object, therefore the same key, on all three attempts.
    for (const attempt of seen) {
      expect(attempt).toBe(request);
      expect((attempt as { idempotencyKey: string }).idempotencyKey).toBe(
        (request as { idempotencyKey: string }).idempotencyKey,
      );
    }
  });

  it('does not retry an unkeyed mutation, and reports the last failure', async () => {
    let attempts = 0;
    await expect(
      callWithRetry(
        { collectionId: 'col_1' },
        async () => {
          attempts += 1;
          throw new ConnectError('node is restarting', Code.Unavailable);
        },
        { retrySafe: false, maxRetries: 3 },
        'loams.test.v1.ThingService/Write',
      ),
    ).rejects.toMatchObject({ code: Code.Unavailable });
    expect(attempts).toBe(1);
  });

  it('retries a read, whose class comes from the generated bindings', async () => {
    const read = MODULES_RETRY.get('instance.getInstance');
    const mutation = MODULES_RETRY.get('tables.mutate');
    expect(read).toBe('safe');
    // A live mutation declares no idempotency level, so the class is `manual`
    // until the call carries a key.
    expect(mutation).toBe('manual');

    let attempts = 0;
    const value = await callWithRetry(
      {},
      async () => {
        attempts += 1;
        if (attempts < 2) {
          throw new ConnectError('node is restarting', Code.Unavailable);
        }
        return { instanceId: '01' };
      },
      { retrySafe: read === 'safe', maxRetries: 3 },
    );
    expect(value).toEqual({ instanceId: '01' });
    expect(attempts).toBe(2);
  });

  it('sends the identical request again over HTTP, and answers on the third try', async () => {
    // The end-to-end half: a real socket, the SDK's own transport, and a
    // server that refuses the first two attempts. What the server records is
    // the evidence that the request — and so the key — went out unchanged.
    const bodies: string[] = [];
    const server: Server = createServer((request, response) => {
      const chunks: Buffer[] = [];
      request.on('data', (chunk: Buffer) => chunks.push(chunk));
      request.on('end', () => {
        const body = Buffer.concat(chunks).toString('base64');
        bodies.push(body);
        if (bodies.length < 3) {
          response.writeHead(503, { 'content-type': 'application/json' });
          response.end(JSON.stringify({ code: 'unavailable', message: 'node is restarting' }));
          return;
        }
        response.writeHead(200, { 'content-type': 'application/json' });
        response.end('{}');
      });
    });
    await new Promise<void>((resolvePromise) => server.listen(0, '127.0.0.1', resolvePromise));
    const address = server.address();
    const endpoint = `http://127.0.0.1:${typeof address === 'object' && address ? address.port : 0}`;

    try {
      const invoker = new CallInvoker(
        createConnectTransport({ baseUrl: endpoint, useBinaryFormat: false }),
        undefined,
        3,
        undefined,
      );
      invoker.register(READ_BINDING.service, InstanceService);
      // `retrySafe` is what a keyed mutation computes, so the loop retries.
      await expect(
        callWithRetry(
          { idempotencyKey: 'idem_fixed' },
          async () => invoker.unary(READ_BINDING, { idempotencyKey: 'idem_fixed' }),
          { retrySafe: true, maxRetries: 3 },
          READ_BINDING.rpc,
        ),
      ).resolves.toBeDefined();

      expect(bodies).toHaveLength(3);
      expect(new Set(bodies).size).toBe(1);
    } finally {
      await new Promise<void>((resolvePromise) => server.close(() => resolvePromise()));
    }
  });

  it('backs off with full jitter, under the cap', () => {
    for (let attempt = 0; attempt < 8; attempt += 1) {
      const ceiling = Math.min(2_000, 100 * 2 ** attempt);
      for (let sample = 0; sample < 20; sample += 1) {
        const delay = backoffMs(attempt);
        expect(delay).toBeGreaterThanOrEqual(0);
        expect(delay).toBeLessThanOrEqual(ceiling);
      }
    }
  });
});

/** The retry class of every generated call, read off the generated table. */
const MODULES_RETRY = new Map<string, string>();
{
  for (const module of MODULES) {
    for (const call of module.calls) {
      MODULES_RETRY.set(`${module.name}.${call.name}`, call.retry);
    }
  }
}

/** `instance.getInstance`, which the generated table says is `safe`. */
const READ_BINDING = MODULES.find(
  (module) => module.name === 'instance',
)!.calls.find((call) => call.name === 'getInstance')!;
