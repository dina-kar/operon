// SDK2 Task 0's `typescript_stream_resume_with_cursor`.
//
// Design §44 §7.4, D610 and runtime contract R7: a server stream is the one
// call where "retry it" is not enough. The server hands out cursors; a
// reconnect has to resume from the last one the client applied, or the client
// silently misses everything that changed in between — which is worse than an
// error, because a sync UI that is quietly stale looks like a sync UI that
// works.
//
// So the assertions are about the *cursor*: that it is read off every message,
// that it is the one carried into the re-open, that the messages already
// yielded are not re-yielded, and that a failure the retry class does not
// cover is reported rather than swallowed.

import { Code, ConnectError } from '@connectrpc/connect';
import { describe, expect, it } from 'vitest';
import type { CallBinding } from '../src/gen/facade.js';
import { LoamsError } from '../src/runtime/errors.js';
import { watch } from '../src/runtime/streams.js';

/** The live watch binding, which the generated table says is a server stream. */
const WATCH = (await import('../src/gen/facade.js')).MODULES.find(
  (module) => module.name === 'live',
)!.calls.find((call) => call.name === 'watch')! as CallBinding;

/** A message shaped like a live `Transition`: it carries a cursor. */
interface Message {
  sessionId: string;
  cursor: string;
}

/** A stream that yields `messages` and then fails with `error`, if given. */
async function* stream(messages: Message[], error?: ConnectError): AsyncGenerator<Message> {
  for (const message of messages) {
    yield message;
  }
  if (error !== undefined) {
    throw error;
  }
}

/** The request a re-open carries, as `loams.live`'s `WatchRequest.resume` does. */
interface Request {
  start?: { case: string; value: { version: bigint } };
  resume?: { lastCursor: string };
}

describe('typescript_stream_resume_with_cursor', () => {
  it('the generated binding says watch is a server stream, not a unary call', () => {
    expect(WATCH.streaming).toBe('server');
    expect(WATCH.module).toBe('live');
    expect(WATCH.method).toBe('Watch');
  });

  it('re-opens from the last cursor and carries on without repeating a message', async () => {
    const opens: Request[] = [];
    const seen: Message[] = [];
    // Two sessions: the first dies after two messages, the second runs to
    // completion. The second must resume from the second message's cursor.
    const batches = [
      stream(
        [
          { sessionId: 's1', cursor: 'c1' },
          { sessionId: 's1', cursor: 'c2' },
        ],
        new ConnectError('node is restarting', Code.Unavailable),
      ),
      stream([
        { sessionId: 's1', cursor: 'c3' },
        { sessionId: 's1', cursor: 'c4' },
      ]),
    ];
    const open = async function* (request: Request) {
      opens.push(request);
      yield* batches[opens.length - 1]!;
    };

    for await (const message of watch<Message, Request>(
      WATCH,
      open,
      { start: { case: 'initial', value: { version: 0n } } },
      {
        resume: (cursor) => ({ resume: { lastCursor: cursor ?? '' } }),
      },
    )) {
      seen.push(message);
    }

    expect(seen.map((message) => message.cursor)).toEqual(['c1', 'c2', 'c3', 'c4']);
    expect(opens).toHaveLength(2);
    expect(opens[0]).toEqual({ start: { case: 'initial', value: { version: 0n } } });
    // Resumed from the last cursor applied, not from the beginning and not
    // from the first message of the session.
    expect(opens[1]).toEqual({ resume: { lastCursor: 'c2' } });
  });

  it('resumes from the start when the failure came before any cursor', async () => {
    const opens: Request[] = [];
    const batches = [
      (async function* () {
        throw new ConnectError('connection reset', Code.Unavailable);
      })(),
      stream([{ sessionId: 's1', cursor: 'c1' }]),
    ];
    const open = async function* (request: Request) {
      opens.push(request);
      yield* batches[opens.length - 1]!;
    };

    const seen: Message[] = [];
    for await (const message of watch<Message, Request>(WATCH, open, {}, {
      resume: (cursor) => ({ resume: { lastCursor: cursor ?? 'zero' } }),
    })) {
      seen.push(message);
    }
    expect(seen).toHaveLength(1);
    expect(opens).toHaveLength(2);
    expect(opens[1]).toEqual({ resume: { lastCursor: 'zero' } });
  });

  it('reports a failure the retry class does not cover', async () => {
    // A refusal is not a hiccup: retrying an `unimplemented` stream would spin
    // forever against an instance that will never serve it. This is the
    // unavailable-service path arriving on a stream, and it has to surface as
    // a typed error.
    const open = async function* () {
      throw new ConnectError(
        'loams.live.v1.LiveService/Watch is not in the standard variant',
        Code.Unimplemented,
      );
    };
    await expect(
      (async () => {
        for await (const _ of watch<Message, Request>(WATCH, open, {}, { resume: () => ({}) })) {
          // not reached
        }
      })(),
    ).rejects.toBeInstanceOf(LoamsError);
  });

  it('gives up after maxRetries and reports the last failure', async () => {
    let opens = 0;
    const open = async function* () {
      opens += 1;
      throw new ConnectError('node is restarting', Code.Unavailable);
    };
    await expect(
      (async () => {
        for await (const _ of watch<Message, Request>(WATCH, open, {}, {
          resume: () => ({}),
          maxRetries: 2,
        })) {
          // not reached
        }
      })(),
    ).rejects.toMatchObject({ code: Code.Unavailable });
    // One first attempt plus two retries.
    expect(opens).toBe(3);
  });

  it('reports each cursor as it goes, for a caller saving its own position', async () => {
    const cursors: (string | undefined)[] = [];
    const open = async function* () {
      yield* stream([
        { sessionId: 's1', cursor: 'c1' },
        { sessionId: 's1', cursor: 'c2' },
      ]);
    };
    for await (const _ of watch<Message, Request>(WATCH, open, {}, {
      resume: () => ({}),
      onCursor: (cursor) => cursors.push(cursor),
    })) {
      // not inspected
    }
    expect(cursors).toEqual(['c1', 'c2']);
  });
});
