// Server streams (design §44 §7.4, D610; runtime contract R7).
//
// The API has server streams only (D420): no client streaming, no bidi, because
// a browser cannot do it over fetch and half-duplex works through every proxy.
// In TypeScript a server stream is an `AsyncIterable` (design §44 §7.1).
//
// A stream is the one call where a retry is not just "send it again": the
// server hands out cursors, and a reconnect has to resume from the last one or
// the client silently misses the changes in between. So `watch` tracks the
// cursor of every message and, on a retryable failure, re-opens the stream from
// it. The request that resumes is the stream's own business — `loams.live`'s
// `WatchRequest.resume`, for instance — so the caller passes a `resume`
// function and the runtime supplies the cursor.

import type { CallBinding } from '../gen/facade.js';
import type { CallOptions } from './call.js';
import { toLoamsError } from './errors.js';
import { DEFAULT_MAX_RETRIES, backoffMs, shouldRetry, sleep } from './retry.js';

/** Opens a stream. The runtime owns retries, so this is the raw call. */
export type StreamOpener<Message, Request = unknown> = (
  request: Request,
  options?: CallOptions,
) => AsyncIterable<Message>;

/** The cursor a stream message carries, when it carries one (R7). */
export type CursorOf<Message> = (message: Message) => string | undefined;

/** How a stream re-opens from a cursor. */
export interface ResumeOptions<Message, Request> {
  /**
   * The request to re-open with, given the last cursor seen. Return the same
   * request to reconnect from the beginning (which is correct, and loses
   * nothing but time, for a stream whose snapshot is complete).
   */
  resume: (lastCursor: string | undefined, request: Request) => Request;
  /** Reads the cursor off a message. Defaults to a `cursor` field. */
  cursor?: CursorOf<Message>;
  /** Retries after the first failure. Defaults to the client's. */
  maxRetries?: number;
  /** Called after each message, with the cursor it carried. */
  onCursor?: (cursor: string | undefined, message: Message) => void;
}

/** The default cursor reader: a `cursor` field, which is what every stream in
 * the API uses (§44 §7.4). */
function cursorField<Message>(message: Message): string | undefined {
  const value = (message as { cursor?: unknown }).cursor;
  return typeof value === 'string' && value !== '' ? value : undefined;
}

/**
 * A server stream that reconnects from its cursor.
 *
 * `loams.live.watch()` returns one of these. It yields until the stream ends;
 * if the stream fails with a code a retry may answer and the call is retryable,
 * it re-opens from the last cursor and carries on, so a node restart is a
 * hiccup rather than a gap.
 */
export async function* watch<Message, Request extends object>(
  binding: CallBinding,
  open: StreamOpener<Message, Request>,
  request: Request,
  options: CallOptions & ResumeOptions<Message, Request>,
): AsyncGenerator<Message, void, undefined> {
  const cursorOf = options.cursor ?? cursorField;
  const maxRetries = options.maxRetries ?? DEFAULT_MAX_RETRIES;
  const retrySafe = options.retrySafe ?? true;
  let current = request;
  let cursor: string | undefined;
  let attempt = 0;
  for (;;) {
    try {
      for await (const message of open(current, options)) {
        // Progress earns a fresh budget: `maxRetries` bounds the retries in
        // one *run* of disconnects, not for the life of the stream. A watch
        // that recovers from a node restart and then runs for days must not
        // spend its budget on the first failure of each of those days.
        attempt = 0;
        cursor = cursorOf(message) ?? cursor;
        options.onCursor?.(cursor, message);
        yield message;
      }
      return;
    } catch (thrown) {
      const error = toLoamsError(thrown, binding.rpc);
      if (!shouldRetry(error, retrySafe, attempt, maxRetries)) {
        throw error;
      }
      current = options.resume(cursor, request);
      await sleep(backoffMs(attempt), options.signal);
      attempt += 1;
    }
  }
}
