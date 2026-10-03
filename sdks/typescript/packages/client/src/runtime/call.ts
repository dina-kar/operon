// The call path: the one place a facade method becomes an RPC (design §44 §7.4;
// runtime contract R1-R4).
//
// It does four things the generated facade cannot, and nothing else:
//
// - attaches the bearer from the client's `TokenSource`;
// - retries on the call's class from the generated bindings, with M1.6's
//   backoff numbers, and refreshes the token once on `token_expired`;
// - gives a mutating call an idempotency key once per logical call and reuses
//   it on every retry, so a retried write is the same write (D610);
// - turns whatever is thrown into the typed `LoamsError` hierarchy, so a
//   caller branches on `reason` and never on a message.

import type { DescService } from '@bufbuild/protobuf';
import {
  Code,
  type CallOptions as ConnectCallOptions,
  createClient,
  type Transport,
} from '@connectrpc/connect';
import type { CallBinding } from '../gen/facade.js';
import { LoamsError, TokenExpiredError, toLoamsError } from './errors.js';
import { DEFAULT_MAX_RETRIES, backoffMs, shouldRetry, sleep } from './retry.js';
import type { TokenSource } from './token-source.js';
import { uuidv7 } from './uuidv7.js';

/**
 * Per-call overrides. Everything here is optional: the client's defaults apply,
 * and the generated binding supplies the retry class.
 *
 * The instance's URL is *not* here: in connect-es it belongs to the transport,
 * so a `Loams` built without a `transport` makes one from its `endpoint`, and
 * one built with a `transport` expects that transport to know the URL.
 */
export interface CallOptions extends ConnectCallOptions {
  /** Retries after the first attempt. `0` disables retrying for this call. */
  maxRetries?: number;
  /** Extra request headers. `Authorization` is set by the client and wins. */
  headers?: Record<string, string>;
  /**
   * The idempotency key for a mutating call. Supply your own to make a retry
   * yours rather than the SDK's; omit it and the SDK mints one UUIDv7 per
   * logical call and reuses it on every retry.
   */
  idempotencyKey?: string;
  /** Overrides the call's retry class for this call only. */
  retrySafe?: boolean;
  /** The consistency token to read at, and the store to record writes in
   * (§44 §7.4, D609). */
  consistency?: ConsistencyOptions;
}

/** How consistency tokens are carried (D609). */
export interface ConsistencyOptions {
  /** A `ConsistencyToken` from a previous write, or `'strong'` (the default)
   * / `'eventual'` to read without waiting. */
  token?: string;
  /** Record the consistency token every response carries, so later reads are
   * read-your-writes without the caller threading tokens by hand. */
  session?: ConsistencyTokenStore | true | false;
}

/** A session's merged consistency token (D609). Off unless a call opts in. */
export interface ConsistencyTokenStore {
  /** The token to attach to the next read, or undefined for none. */
  current(): string | undefined;
  /** Folds a token the server returned into the session's. */
  record(token: string | undefined): void;
}

/** One attempt of a call, as the transport sees it. */
export type Send = (attempt: Attempt) => Promise<unknown>;

/** What one attempt carries: the request (with the idempotency key already
 * set) and the bearer to send, if any. */
export interface Attempt {
  readonly request: unknown;
  readonly headers: Record<string, string>;
  readonly attempt: number;
  readonly refreshed: boolean;
}

/** Everything the retry loop needs, so it can be tested without a transport. */
export interface RetryPlan {
  /** The call's retry class from the generated binding. */
  readonly retrySafe: boolean;
  readonly maxRetries: number;
  readonly signal?: AbortSignal;
  readonly onRefresh?: () => Promise<void>;
}

/**
 * Runs `send` until it answers or the plan says stop, and returns whatever it
 * last threw as a `LoamsError`.
 *
 * Exported so the tests can drive it with a stub `send`: the retry policy,
 * the idempotency-key lifecycle and the refresh-once behaviour are the SDK's
 * own logic and are pinned without a server.
 */
export async function callWithRetry(
  request: unknown,
  send: Send,
  plan: RetryPlan,
  rpc?: string,
): Promise<unknown> {
  let refreshed = false;
  for (let attempt = 0; ; attempt += 1) {
    try {
      return await send({ request, headers: {}, attempt, refreshed });
    } catch (thrown) {
      const error = toLoamsError(thrown, rpc);
      // R1: a 401 whose reason is `token_expired` gets exactly one refresh and
      // one retry. A source that cannot refresh (`apiKey`) makes this a no-op,
      // and a second expiry is reported rather than looped on.
      if (error instanceof TokenExpiredError && !refreshed && plan.onRefresh !== undefined) {
        refreshed = true;
        await plan.onRefresh();
        attempt -= 1;
        continue;
      }
      if (!shouldRetry(error, plan.retrySafe, attempt, plan.maxRetries)) {
        throw error;
      }
      await sleep(backoffMs(attempt), plan.signal);
    }
  }
}

/**
 * Gives a mutating call its idempotency key, once per logical call.
 *
 * D610: a mutation may only be retried when it carries one, and the same key
 * has to go out on every attempt or the retry is a second write. So the key is
 * decided here, before the first attempt, and the request object is not
 * rewritten afterwards.
 *
 * A message without an `idempotency_key` field is left alone: proto3 messages
 * are plain objects here, so the field's presence is the test.
 */
export function withIdempotencyKey(
  request: unknown,
  supplied: string | undefined,
  /** Whether the request *schema* declares an `idempotency_key` field. */
  declared = false,
): { request: unknown; keyed: boolean } {
  if (typeof request !== 'object' || request === null) {
    return { request, keyed: false };
  }
  const message = request as Record<string, unknown>;
  // Either the schema declares the field or the object carries it; without one
  // of those two there is nothing to key, and a request that has no key field
  // must be left exactly as the caller wrote it.
  if (!declared && !('idempotencyKey' in message)) {
    return { request, keyed: false };
  }
  if (typeof message.idempotencyKey === 'string' && message.idempotencyKey !== '') {
    return { request, keyed: true };
  }
  return {
    request: { ...message, idempotencyKey: supplied ?? uuidv7() },
    keyed: true,
  };
}

/**
 * A stream method's return value, as one async iterable.
 *
 * connect-es types a server stream as an iterable that may be awaited; the SDK
 * hands callers a plain `AsyncIterable`, which is what `for await` wants
 * (design §44 §7.1).
 */
async function* streamOf(source: AsyncIterable<unknown> | AsyncIterable<Promise<unknown>>) {
  yield* source as AsyncIterable<unknown>;
}

/** The request-message field a mutation is keyed by (D610). Read from the
 * generated schema rather than guessed from the object a caller built. */
const IDEMPOTENCY_KEY = 'idempotencyKey';

/**
 * The session a call asked for: its own store if it brought one, the client's
 * if it asked for the session's, none otherwise.
 */
function resolveSession(
  options: CallOptions['consistency'],
  clientSession: ConsistencyTokenStore | undefined,
): ConsistencyTokenStore | undefined {
  const session = options?.session;
  if (session === undefined || session === false) {
    return undefined;
  }
  return session === true ? clientSession : session;
}

/**
 * The client's call path: a generated `CallBinding` plus a request becomes a
 * response message, with everything the SDK owns applied on the way.
 */
export class CallInvoker {
  private readonly clients = new Map<string, Record<string, unknown>>();
  private readonly descriptors = new Map<string, DescService>();
  /** Whether a call's request schema declares `idempotency_key`, per RPC. */
  private readonly keyed = new Map<string, boolean>();

  constructor(
    private readonly transport: Transport,
    private readonly tokenSource: TokenSource | undefined,
    private readonly maxRetries: number,
    private readonly consistency: ConsistencyTokenStore | undefined,
  ) {}

  /**
   * A module's Connect client, built once from the service descriptor
   * `protoc-gen-es` generated. This is what makes the facade generated rather
   * than hand-written: the SDK never writes a client, it wraps the stubs.
   */
  register(service: string, descriptor: DescService): void {
    if (this.clients.has(service)) {
      return;
    }
    this.descriptors.set(service, descriptor);
    for (const method of Object.values(descriptor.method)) {
      // Whether this RPC takes an idempotency key is a property of its request
      // message, so it is read from the generated schema rather than inferred
      // from the object a caller happened to build. A caller who omits an
      // `optional` field still gets a key, and `MutateRequest` (which has one)
      // is never confused with `DeployRequest` (which does not).
      this.keyed.set(
        `${service}/${method.name}`,
        method.input.field[IDEMPOTENCY_KEY] !== undefined,
      );
    }
    this.clients.set(service, createClient(descriptor, this.transport) as unknown as Record<
      string,
      unknown
    >);
  }

  /** Whether this call's request message declares an `idempotency_key`. */
  private takesIdempotencyKey(binding: CallBinding): boolean {
    return this.keyed.get(`${binding.service}/${binding.method}`) ?? false;
  }

  /** The connect-es method a binding names, or a clear internal error. */
  private method(binding: CallBinding): (request: unknown, options: CallOptions) => Promise<unknown> {
    const client = this.clients.get(binding.service);
    const method = client?.[binding.name];
    if (typeof method !== 'function') {
      throw new LoamsError(
        `the generated client for ${binding.service} has no method ${binding.name}`,
        { code: Code.Internal, rpc: binding.rpc },
      );
    }
    return method as (request: unknown, options: CallOptions) => Promise<unknown>;
  }

  /** One unary call: the whole runtime contract, applied to one binding. */
  async unary(binding: CallBinding, request: unknown, options: CallOptions = {}) {
    const { request: keyed, keyed: hasKey } = withIdempotencyKey(
      request,
      options.idempotencyKey,
      this.takesIdempotencyKey(binding),
    );
    const maxRetries = options.maxRetries ?? this.maxRetries;
    // D610: a `safe` call always retries; a mutation retries once it carries an
    // idempotency key, which `withIdempotencyKey` has just decided.
    const retrySafe = options.retrySafe ?? (binding.retry === 'safe' || hasKey);
    const session = resolveSession(options.consistency, this.consistency);
    // An explicit token wins; otherwise the session's, which is what makes
    // `consistency: { session: true }` read-your-writes rather than record it
    // and never send it (R4).
    const consistency = options.consistency?.token ?? session?.current();
    const method = this.method(binding);

    const result = (await callWithRetry(
      keyed,
      async () => method(keyed, { ...options, headers: await this.headers(options, consistency) }),
      {
        retrySafe,
        maxRetries,
        signal: options.signal,
        onRefresh: this.tokenSource?.refresh?.bind(this.tokenSource),
      },
      binding.rpc,
    )) as Record<string, unknown> | undefined;
    this.recordConsistency(session, result);
    return result;
  }

  /** The headers a call carries: the caller's, plus the bearer and the
   * consistency token the runtime owns. */
  private async headers(
    options: CallOptions,
    consistency: string | undefined,
  ): Promise<Record<string, string>> {
    const headers: Record<string, string> = { ...options.headers };
    const bearer = await this.tokenSource?.token();
    if (bearer !== undefined) {
      headers.authorization = `Bearer ${bearer}`;
    }
    if (consistency !== undefined) {
      headers['loams-consistency'] = consistency;
    }
    return headers;
  }

  /**
   * One server stream, with the errors mapped: no resume, no cursor tracking.
   *
   * The mapping is not optional here. A refusal on a stream arrives inside the
   * Connect envelope, not as an HTTP status, so a caller that iterated the raw
   * iterable would see a `ConnectError` with no `reason` — the one place in the
   * SDK where `reason` could go missing. `watch()` adds the resume on top.
   */
  stream(binding: CallBinding, request: unknown, options: CallOptions = {}): AsyncIterable<unknown> {
    const method = this.method(binding) as unknown as (
      request: unknown,
      options: CallOptions,
    ) => AsyncIterable<unknown>;
    const session = resolveSession(options.consistency, this.consistency);
    const consistency = options.consistency?.token ?? session?.current();
    const self = this;
    const refresh = this.tokenSource?.refresh?.bind(this.tokenSource);
    // Synchronous, because `loams.live.watch(...)` has to hand back an
    // `AsyncIterable` rather than a promise of one (design §44 §7.1). The
    // bearer is fetched when the caller first steps the stream rather than when
    // the call is made: a stream that never authenticates is not a stream.
    const invoke = async function* (): AsyncGenerator<unknown, void, undefined> {
      let refreshed = false;
      let yielded = false;
      for (;;) {
        const headers = await self.headers(options, consistency);
        const source = self.mapped(
          streamOf(method(request, { ...options, headers })),
          binding.rpc,
        );
        try {
          for await (const message of source) {
            yielded = true;
            yield message;
          }
          return;
        } catch (thrown) {
          const error = toLoamsError(thrown, binding.rpc);
          // R1 on a stream: one refresh and one re-open, and only if nothing
          // has been yielded yet. Once messages are flowing the caller is
          // holding a position in the stream, and re-opening it is `watch()`'s
          // job (it resumes from the cursor); replaying from the start here
          // would duplicate everything the caller has already seen.
          if (
            error instanceof TokenExpiredError &&
            !refreshed &&
            refresh !== undefined &&
            !yielded
          ) {
            refreshed = true;
            await refresh();
            continue;
          }
          throw error;
        }
      }
    };
    return invoke();
  }

  /** Wraps an iterable so every throw becomes a `LoamsError`. */
  private mapped(source: AsyncIterable<unknown>, rpc: string): AsyncIterable<unknown> {
    const invoke = async function* (): AsyncGenerator<unknown, void, undefined> {
      try {
        yield* source;
      } catch (thrown) {
        throw toLoamsError(thrown, rpc);
      }
    };
    return invoke();
  }

  /** Merges the consistency token a write answered with into the session. */
  private recordConsistency(
    session: ConsistencyTokenStore | undefined,
    result: Record<string, unknown> | undefined,
  ): void {
    if (session === undefined || result === undefined) {
      return;
    }
    const token = result.consistencyToken;
    try {
      session.record(typeof token === 'string' && token !== '' ? token : undefined);
    } catch {
      // The RPC already succeeded. A token the session cannot merge — the
      // encoding is still opaque, see `ConsistencySession` — must not turn a
      // committed write into a thrown error, because a caller that retries on
      // that error performs the write twice. `ConsistencySession.conflicts`
      // counts it instead.
    }
  }

}
