// The runtime's guarantees that the other six tests lean on: a stream is
// authenticated, the session's consistency token goes out as well as being
// recorded, a stream that makes progress earns a fresh retry budget, and a
// token source with an empty cache fetches before its first call.
//
// Each of these was a real defect found in review, so each has a test that
// fails without the fix. They are in their own file rather than folded into the
// clause tests because they are about the call path itself, not about one
// clause's behaviour.

import { Code, ConnectError } from '@connectrpc/connect';
import { create } from '@bufbuild/protobuf';
import { toBinary } from '@bufbuild/protobuf';
import { ErrorInfoSchema } from '@loams/proto/errors';
import { InstanceService } from '@loams/proto/instance';
import { LiveService } from '@loams/live/live';
import { describe, expect, it } from 'vitest';
import { MODULES } from '../src/gen/facade.js';
import { CallInvoker } from '../src/runtime/call.js';
import { ConsistencySession } from '../src/runtime/consistency.js';
import { LoamsError, TokenExpiredError } from '../src/runtime/errors.js';
import { refreshing } from '../src/runtime/token-source.js';
import { watch } from '../src/runtime/streams.js';

const GET_INSTANCE = MODULES.find((module) => module.name === 'instance')!.calls[0]!;
const WATCH = MODULES.find((module) => module.name === 'live')!.calls.find(
  (call) => call.name === 'watch',
)!;
const TABLES_QUERY = MODULES.find((module) => module.name === 'tables')!.calls.find(
  (call) => call.name === 'query',
)!;

/** An invoker whose transport records what it was asked and answers `answer`. */
function invokerWith(
  transport: ConstructorParameters<typeof CallInvoker>[0],
  rest: {
    tokenSource?: ConstructorParameters<typeof CallInvoker>[1];
    maxRetries?: number;
    consistency?: ConstructorParameters<typeof CallInvoker>[3];
  } = {},
): CallInvoker {
  const invoker = new CallInvoker(
    transport,
    rest.tokenSource,
    rest.maxRetries ?? 0,
    rest.consistency,
  );
  invoker.register(GET_INSTANCE.service, InstanceService);
  invoker.register('loams.live.v1.LiveService', LiveService);
  return invoker;
}

/**
 * A transport that records the headers of every call and answers with
 * `answer`.
 *
 * connect-es's `Transport` takes positional arguments, not a context object:
 * `(method, signal, timeoutMs, header, input)`. The fourth is the request
 * headers, which is what these tests read.
 */
function recordingTransport(answer: (open: number) => unknown) {
  const seen: Record<string, string>[] = [];
  let opens = 0;
  const record = (header: HeadersInit | undefined) => {
    const headers: Record<string, string> = {};
    for (const [name, value] of Object.entries(
      (header ?? {}) as Record<string, string>,
    )) {
      headers[name.toLowerCase()] = value;
    }
    seen.push(headers);
    opens += 1;
    return opens;
  };
  return {
    seen,
    opens: () => opens,
    transport: {
      unary(
        _method: unknown,
        _signal: unknown,
        _timeoutMs: number | undefined,
        header: HeadersInit | undefined,
        _input: unknown,
      ) {
        record(header);
        // The response message is the answer, so a caller can read a
        // consistency token off it. The shape is loose because the stub is a
        // plain object, not a decoded proto.
        return Promise.resolve({ message: answer(opens) }) as never;
      },
      stream(
        _method: unknown,
        _signal: unknown,
        _timeoutMs: number | undefined,
        header: HeadersInit | undefined,
        input: AsyncIterable<unknown>,
      ) {
        record(header);
        return Promise.resolve({ message: input }) as never;
      },
    },
  };
}

describe('the call path', () => {
  it('authenticates a server stream, not just a unary call', async () => {
    // A stream that never sends the bearer fails the moment the instance
    // requires credentials, and `for await (const t of loams.live.watch(..))`
    // is the shape the design asks for — so the credential has to be attached
    // on the same path as every other call.
    const { seen, transport } = recordingTransport(async function* () {
      yield { sessionId: 's1' };
    });
    const invoker = invokerWith(transport, { tokenSource: { token: () => Promise.resolve('bearer') } });

    const messages: unknown[] = [];
    for await (const message of invoker.stream(WATCH, {})) {
      messages.push(message);
    }
    expect(messages).toHaveLength(1);
    expect(seen).toHaveLength(1);
    expect(seen[0]?.authorization).toBe('Bearer bearer');
  });

  it('refreshes once and re-opens a stream whose token expired', async () => {
    // R1 on a stream. The refusal is thrown by the *transport* here rather than
    // by the message stream, so what is pinned is the invoker's re-open and the
    // single refresh; the message-level path is `watch()`'s, pinned in
    // `stream-resume.test.ts`.
    let refreshes = 0;
    let bearer = 'stale';
    const expired = new ConnectError('expired', Code.Unauthenticated);
    const info = create(ErrorInfoSchema, { reason: 'token_expired' });
    expired.details.push({
      type: ErrorInfoSchema.typeName,
      value: toBinary(ErrorInfoSchema, info),
    });

    const seen: Record<string, string>[] = [];
    let opens = 0;
    const transport = {
      unary(
        _m: unknown,
        _s: unknown,
        _t: number | undefined,
        header: HeadersInit | undefined,
      ) {
        opens += 1;
        seen.push({ ...(header as Record<string, string>) });
        if (opens === 1) {
          return Promise.reject(expired);
        }
        return Promise.resolve({ message: {} });
      },
      stream(
        _m: unknown,
        _s: unknown,
        _t: number | undefined,
        header: HeadersInit | undefined,
        input: AsyncIterable<unknown>,
      ) {
        opens += 1;
        seen.push({ ...(header as Record<string, string>) });
        return opens === 1
          ? Promise.reject(expired)
          : Promise.resolve({ message: input });
      },
    };
    const invoker = invokerWith(transport as never, {
      tokenSource: {
        token: () => Promise.resolve(bearer),
        refresh: () => {
          refreshes += 1;
          bearer = 'fresh';
          return Promise.resolve();
        },
      },
      maxRetries: 3,
    });

    await invoker.unary(GET_INSTANCE, {});
    expect(refreshes).toBe(1);
    expect(opens).toBe(2);
    expect(seen[0]?.authorization).toBe('Bearer stale');
    expect(seen[1]?.authorization).toBe('Bearer fresh');
  });

  it('does not re-open a stream that had already yielded messages', async () => {
    // Once messages are flowing the caller holds a position, and replaying from
    // the start would duplicate everything it has seen. `watch()` resumes from
    // the cursor instead (R7); this layer stops and reports.
    let refreshes = 0;
    let opens = 0;
    const expired = new ConnectError('expired', Code.Unauthenticated);
    const info = create(ErrorInfoSchema, { reason: 'token_expired' });
    expired.details.push({
      type: ErrorInfoSchema.typeName,
      value: toBinary(ErrorInfoSchema, info),
    });
    const transport = {
      unary() {
        return Promise.resolve({ message: {} }) as never;
      },
      stream(
        _m: unknown,
        _s: unknown,
        _t: number | undefined,
        _header: HeadersInit | undefined,
        _input: AsyncIterable<unknown>,
      ) {
        opens += 1;
        return Promise.resolve({
          message: (async function* () {
            yield { cursor: 'c1' };
            throw expired;
          })(),
        }) as never;
      },
    };
    const invoker = invokerWith(transport as never, {
      tokenSource: {
        token: () => Promise.resolve('bearer'),
        refresh: () => {
          refreshes += 1;
          return Promise.resolve();
        },
      },
    });

    const seen: unknown[] = [];
    await expect(
      (async () => {
        for await (const message of invoker.stream(WATCH, {})) {
          seen.push(message);
        }
      })(),
    ).rejects.toBeInstanceOf(TokenExpiredError);
    expect(seen).toHaveLength(1);
    expect(opens).toBe(1);
    expect(refreshes).toBe(0);
  });

  it('sends the session consistency token on a later read (R4)', async () => {
    // A session that only records is a session that does nothing. The token a
    // write answered with has to go out on the next read, or read-your-writes
    // is a promise the SDK does not keep.
    const session = new ConsistencySession();
    session.record('v1:stream-1.7');
    const { seen, transport } = recordingTransport(() => ({ instanceId: '01' }));
    const invoker = invokerWith(transport, { consistency: session });

    await invoker.unary(GET_INSTANCE, {}, { consistency: { session: true } });
    expect(seen[0]?.['loams-consistency']).toBe('v1:stream-1.7');
  });

  it('folds the token a write answered with into the session', async () => {
    const session = new ConsistencySession();
    const { transport } = recordingTransport(() => ({ consistencyToken: 'v1:stream-2.1' }));
    const invoker = invokerWith(transport, { consistency: session });

    await invoker.unary(TABLES_QUERY, {}, { consistency: { session: true } });
    expect(session.current()).toBe('v1:stream-2.1');
  });

  it('does not fail a committed write because the session cannot merge a token', async () => {
    // The token's encoding is still opaque, so two different tokens meet and the
    // session refuses to merge them. That refusal must not become the caller's
    // error: the write is done, and a caller that retried on the error would
    // write twice.
    const session = new ConsistencySession();
    session.record('v1:stream-1.1');
    const { transport } = recordingTransport(() => ({ consistencyToken: 'not-a-token' }));
    const invoker = invokerWith(transport, { consistency: session });

    await expect(
      invoker.unary(TABLES_QUERY, {}, { consistency: { session: true } }),
    ).resolves.toBeDefined();
    expect(session.conflicts).toBe(1);
  });

  it('gives a stream that made progress a fresh retry budget (R7)', async () => {
    // `maxRetries` bounds the disconnects in one *run*, not for the life of the
    // stream. A watch that recovers from a node restart and then runs for days
    // must not spend its budget on the first failure of each of those days.
    let opens = 0;
    const open = async function* () {
      opens += 1;
      yield { cursor: `c${opens}` };
      if (opens < 4) {
        throw new ConnectError('node is restarting', Code.Unavailable);
      }
    };

    const seen: { cursor: string }[] = [];
    for await (const message of watch<{ cursor: string }, Record<string, never>>(
      WATCH,
      open,
      {},
      { resume: () => ({}), maxRetries: 1 },
    )) {
      seen.push(message);
    }
    // Four failures' worth of reconnection with a budget of one: only possible
    // because each open made progress first.
    expect(seen.map((message) => message.cursor)).toEqual(['c1', 'c2', 'c3', 'c4']);
    expect(opens).toBe(4);
  });

  it('fetches on first use, so a source with an empty cache still authenticates', async () => {
    // `oidcExchange` caches nothing until something refreshes it. If `token()`
    // did not fetch, the first call would go out with no credential, the
    // instance would answer `unauthenticated`, and the call path would read that
    // as an expired token and retry — with still no credential.
    let exchanges = 0;
    const source = refreshing(async () => {
      exchanges += 1;
      return `token-${exchanges}`;
    });
    expect(await source.token()).toBe('token-1');
    expect(await source.token()).toBe('token-1');
    expect(exchanges).toBe(1);
  });

  it('maps a failure from below the API on a stream too', async () => {
    const open = async function* () {
      throw new TypeError('fetch failed');
    };
    await expect(
      (async () => {
        for await (const _ of watch<unknown, Record<string, never>>(WATCH, open, {}, {
          resume: () => ({}),
        })) {
          // not reached
        }
      })(),
    ).rejects.toBeInstanceOf(LoamsError);
  });
});