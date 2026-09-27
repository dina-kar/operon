// A fake Live server behind connect's in-memory router transport: tests script
// each Watch stream's Transitions and answer the unary calls.
import { create } from "@bufbuild/protobuf";
import { Code, ConnectError, createRouterTransport, type Transport } from "@connectrpc/connect";

import {
  LiveClient,
  LiveService,
  type LiveValue,
  pb,
  toValue,
  type Version,
} from "../dist/index.js";

/** An async queue: `push` values, `end` or `fail` it, iterate it once. */
export class Channel<T> implements AsyncIterable<T> {
  readonly #items: T[] = [];
  #waiting: ((r: IteratorResult<T>) => void) | undefined;
  #failWaiting: ((e: unknown) => void) | undefined;
  #done = false;
  #error: unknown;

  push(item: T): void {
    const w = this.#waiting;
    if (w !== undefined) {
      this.#waiting = undefined;
      w({ value: item, done: false });
    } else {
      this.#items.push(item);
    }
  }

  end(): void {
    this.#done = true;
    this.#waiting?.({ value: undefined, done: true });
  }

  fail(error: unknown): void {
    this.#error = error;
    this.#failWaiting?.(error);
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: () => {
        const item = this.#items.shift();
        if (item !== undefined) return Promise.resolve({ value: item, done: false });
        if (this.#error !== undefined) return Promise.reject(this.#error);
        if (this.#done) return Promise.resolve({ value: undefined, done: true });
        return new Promise((resolve, reject) => {
          this.#waiting = resolve;
          this.#failWaiting = reject;
        });
      },
    };
  }
}

export function v(querySet: bigint, ts: bigint, identity = 0n): Version {
  return { querySet, identity, ts };
}

/** A Transition from `start` to `end` with values by query id. */
export function transition(
  start: Version,
  end: Version,
  values: Record<number, LiveValue> = {},
  opts: { more?: boolean; removed?: number[]; errors?: Record<number, string> } = {},
): pb.Transition {
  const updates = [
    ...Object.entries(values).map(([id, value]) => ({
      queryId: Number(id),
      update: { case: "value" as const, value: toValue(value) },
    })),
    ...(opts.removed ?? []).map((id) => ({
      queryId: id,
      update: { case: "removed" as const, value: {} },
    })),
    ...Object.entries(opts.errors ?? {}).map(([id, message]) => ({
      queryId: Number(id),
      update: {
        case: "error" as const,
        value: { code: pb.ErrorCode.FUNCTION_ERROR, message },
      },
    })),
  ];
  return create(pb.TransitionSchema, {
    sessionId: "s-1",
    start,
    end,
    updates,
    more: opts.more ?? false,
  });
}

/** One Watch call the fake received: its request and the stream it answers with. */
export interface WatchCall {
  request: pb.WatchRequest;
  stream: Channel<pb.Transition>;
}

const clients = new Set<LiveClient>();

/** Closes every client the fakes made (an `afterEach` hook: runs after a timeout too). */
export function closeClients(): void {
  for (const c of clients) c.close();
  clients.clear();
}

export class FakeServer {
  readonly watches: WatchCall[] = [];
  readonly modifies: pb.ModifyQuerySetRequest[] = [];
  readonly mutates: { request: pb.MutateRequest; session: string | null }[] = [];
  readonly deploys: pb.DeployRequest[] = [];
  #watchWaiters: ((c: WatchCall) => void)[] = [];
  /** What `Mutate` answers next; a function may throw. */
  mutateAnswer: () => { commitTs: bigint; result: LiveValue } = () => ({
    commitTs: 1n,
    result: null,
  });
  deployAnswers: (() => string)[] = [];

  readonly transport: Transport = createRouterTransport((router) => {
    router.service(LiveService, {
      watch: (request) => {
        const call: WatchCall = { request, stream: new Channel() };
        this.watches.push(call);
        const waiter = this.#watchWaiters.shift();
        waiter?.(call);
        return call.stream;
      },
      modifyQuerySet: (request) => {
        this.modifies.push(request);
        return {};
      },
      query: (request) => ({ ts: request.ts ?? 7n, result: request.args ?? toValue(null) }),
      mutate: (request, context) => {
        this.mutates.push({ request, session: context.requestHeader.get("loam-session-id") });
        const answer = this.mutateAnswer();
        return { commitTs: answer.commitTs, result: toValue(answer.result) };
      },
      deploy: (request) => {
        this.deploys.push(request);
        const answer = this.deployAnswers.shift();
        if (answer === undefined) throw new ConnectError("no answer scripted", Code.Internal);
        return { deploymentId: answer() };
      },
    });
  });

  /** The `n`th Watch call (0-based), once it arrives. */
  watch(n: number): Promise<WatchCall> {
    const call = this.watches[n];
    if (call !== undefined) return Promise.resolve(call);
    return new Promise((resolve) => {
      const check = (c: WatchCall): void => {
        if (this.watches.length > n) resolve(this.watches[n] ?? c);
        else this.#watchWaiters.push(check);
      };
      this.#watchWaiters.push(check);
    });
  }

  client(): LiveClient {
    const c = new LiveClient({
      baseUrl: "http://fake",
      transport: this.transport,
      reconnect: { minMs: 1, maxMs: 4 },
    });
    clients.add(c);
    return c;
  }
}

/** Resolves once `predicate` holds, polling each macrotask; fails after 2 s. */
export async function until(predicate: () => boolean, what: string): Promise<void> {
  const deadline = Date.now() + 2000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 1));
  }
}
