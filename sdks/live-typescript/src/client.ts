// The Loam Live client (R1 plan Task 14; design §20 §7.1): one `Watch` stream
// per client, query-set changes through `ModifyQuerySet`, resume on a version
// gap or a lost stream, reconnect with jittered backoff, and optimistic updates
// layered over the server's results. Nothing here needs Node: it runs in
// browsers and in Node ≥ 22 over `fetch`.
import { create } from "@bufbuild/protobuf";
import { type Client, Code, ConnectError, createClient, type Transport } from "@connectrpc/connect";
import { createConnectTransport, createGrpcWebTransport } from "@connectrpc/connect-web";

import { LiveError, toLiveError } from "./errors.js";
import {
  LiveService,
  MutateRequestSchema,
  QueryRequestSchema,
  type QuerySpec,
  QuerySpecSchema,
  type WatchRequest,
  WatchRequestSchema,
} from "./gen/loam/live/v1/live_pb.js";
import type { Value } from "./gen/loam/live/v1/value_pb.js";
import { OptimisticLayer, type OptimisticUpdate } from "./optimistic.js";
import {
  type QueryDef,
  QuerySetState,
  SessionState,
  sameVersion,
  type Version,
  VersionGap,
  ZERO,
} from "./session.js";
import { fromValue, type LiveValue, toValue } from "./values.js";

/** The request header that ties a `Mutate` to the client's session (row T12-6). */
export const SESSION_HEADER = "loam-session-id";

/** Reconnect delays: 250 ms doubling to 10 s, jittered (plan Task 14). */
export const RECONNECT_MIN_MS = 250;
export const RECONNECT_MAX_MS = 10_000;
/** A stream silent this long is dead: the server heartbeats every 15 s. */
export const IDLE_TIMEOUT_MS = 45_000;
/** `Deploy` answered `UNAVAILABLE` (busy, row T13-10) is retried this many times. */
export const DEPLOY_ATTEMPTS = 8;

export interface LiveClientOptions {
  /** The Live listener, for example `http://127.0.0.1:7710`. */
  baseUrl: string;
  /** The protocol, or a ready transport (tests pass an in-memory one). */
  transport?: "connect" | "grpc-web" | Transport;
  /** The `fetch` the transport calls; the global one by default. */
  fetch?: typeof fetch;
  /** Reconnect backoff bounds in ms. */
  reconnect?: { minMs?: number; maxMs?: number };
  /** How long a silent stream is kept before reconnecting, in ms. */
  idleTimeoutMs?: number;
  /** Attempts for a `Deploy` answered `UNAVAILABLE`. */
  deployAttempts?: number;
}

export interface MutateOptions {
  /** Makes a retried mutation apply at most once (the server keeps the key 24 h). */
  idempotencyKey?: string;
  /** Edits query results at once; dropped when the server's results show the commit. */
  optimistic?: OptimisticUpdate;
}

export interface Mutated<T> {
  readonly result: T;
  /** The commit's TSO timestamp; a session at `ts ≥ commitTs` shows the write. */
  readonly commitTs: bigint;
}

/** A table and its indexes, for `deploy`. */
export interface TableInput {
  name: string;
  indexes?: { name: string; fields: string[] }[];
}

/** One live query of a client. */
export interface Subscription<T> {
  /** The latest result (with optimistic updates), `undefined` until one arrives. */
  readonly value: T | undefined;
  /** The query's error, if its latest result is one. */
  readonly error: LiveError | undefined;
  /** Calls `cb` with each new value; returns the unsubscriber. */
  onUpdate(cb: (value: T) => void): () => void;
  /** Calls `cb` with each new error; returns the unsubscriber. */
  onError(cb: (error: LiveError) => void): () => void;
  /** Stops the query; the client drops it from the session's query set. */
  unsubscribe(): void;
}

/** What the client keeps of a subscription. */
interface Watcher {
  readonly def: QueryDef;
  notify(): void;
}

class Sub<T> implements Subscription<T>, Watcher {
  readonly #client: LiveClient;
  readonly def: QueryDef;
  readonly #updates = new Set<(value: T) => void>();
  readonly #errors = new Set<(error: LiveError) => void>();
  #closed = false;

  constructor(client: LiveClient, def: QueryDef) {
    this.#client = client;
    this.def = def;
  }

  get value(): T | undefined {
    return this.#client.valueOf(this.def) as T | undefined;
  }

  get error(): LiveError | undefined {
    return this.#client.errorOf(this.def);
  }

  onUpdate(cb: (value: T) => void): () => void {
    this.#updates.add(cb);
    return () => this.#updates.delete(cb);
  }

  onError(cb: (error: LiveError) => void): () => void {
    this.#errors.add(cb);
    return () => this.#errors.delete(cb);
  }

  unsubscribe(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#updates.clear();
    this.#errors.clear();
    this.#client.release(this);
  }

  notify(): void {
    const error = this.error;
    if (error !== undefined) {
      for (const cb of [...this.#errors]) cb(error);
      return;
    }
    const value = this.value;
    if (value === undefined) return;
    for (const cb of [...this.#updates]) cb(value);
  }
}

function transportOf(opts: LiveClientOptions): Transport {
  const t = opts.transport;
  if (t !== undefined && typeof t === "object") return t;
  const common = {
    baseUrl: opts.baseUrl,
    useBinaryFormat: true,
    ...(opts.fetch === undefined ? {} : { fetch: opts.fetch }),
  };
  return t === "grpc-web" ? createGrpcWebTransport(common) : createConnectTransport(common);
}

/** A client of one Loam Live app. */
export class LiveClient {
  readonly #rpc: Client<typeof LiveService>;
  readonly #minMs: number;
  readonly #maxMs: number;
  readonly #idleMs: number;
  readonly #deployAttempts: number;

  readonly #queries = new QuerySetState();
  readonly #state = new SessionState();
  readonly #subs = new Map<number, Set<Watcher>>();
  readonly #optimistic = new OptimisticLayer();
  #overrides = new Map<string, LiveValue | undefined>();

  #sessionId: string | undefined;
  #stream: AbortController | undefined;
  #running: Promise<void> | undefined;
  #restartNow = false;
  #syncing = false;
  #syncAgain = false;
  #wake: (() => void) | undefined;
  #closed = false;

  constructor(opts: LiveClientOptions) {
    this.#rpc = createClient(LiveService, transportOf(opts));
    this.#minMs = opts.reconnect?.minMs ?? RECONNECT_MIN_MS;
    this.#maxMs = opts.reconnect?.maxMs ?? RECONNECT_MAX_MS;
    this.#idleMs = opts.idleTimeoutMs ?? IDLE_TIMEOUT_MS;
    this.#deployAttempts = opts.deployAttempts ?? DEPLOY_ATTEMPTS;
  }

  /** The session's version: its `ts` is the timestamp all results are valid at. */
  get version(): Version {
    return this.#state.version;
  }

  /** The open session's id, while the stream is up. */
  get sessionId(): string | undefined {
    return this.#sessionId;
  }

  /** Subscribes to a query function; equal (fn, args) share one query. */
  watch<T = LiveValue>(fn: string, args: LiveValue = null): Subscription<T> {
    if (this.#closed) throw new LiveError("CANCELED", "the client is closed");
    toValue(args); // refuse what has no wire form here, not in the stream
    const { def, created } = this.#queries.add(fn, args);
    const sub = new Sub<T>(this, def);
    let set = this.#subs.get(def.id);
    if (set === undefined) {
      set = new Set();
      this.#subs.set(def.id, set);
    }
    set.add(sub);
    if (created) {
      this.#running ??= this.#run();
      this.#requestSync();
    }
    return sub;
  }

  /** Runs a query once, at the latest tick or at `ts` (a commit timestamp, say). */
  async query<T = LiveValue>(
    fn: string,
    args: LiveValue = null,
    opts: { ts?: bigint } = {},
  ): Promise<T> {
    const init: { function: string; args: Value; ts?: bigint } = {
      function: fn,
      args: toValue(args),
    };
    if (opts.ts !== undefined) init.ts = opts.ts;
    try {
      const res = await this.#rpc.query(create(QueryRequestSchema, init));
      return fromValue(res.result) as T;
    } catch (e) {
      throw toLiveError(e);
    }
  }

  /** Runs a mutation; its optimistic update shows until the session shows the commit. */
  async mutate<T = LiveValue>(
    fn: string,
    args: LiveValue = null,
    opts: MutateOptions = {},
  ): Promise<Mutated<T>> {
    const init: { function: string; args: Value; idempotencyKey?: string } = {
      function: fn,
      args: toValue(args),
    };
    if (opts.idempotencyKey !== undefined) init.idempotencyKey = opts.idempotencyKey;
    const token =
      opts.optimistic === undefined ? undefined : this.#optimistic.push(opts.optimistic);
    if (token !== undefined) this.#notify(this.#refreshOptimistic());
    const session = this.#sessionId;
    const headers = session === undefined ? undefined : { [SESSION_HEADER]: session };
    try {
      const res = await this.#rpc.mutate(
        create(MutateRequestSchema, init),
        headers === undefined ? {} : { headers },
      );
      if (token !== undefined) {
        if (this.#sessionId === undefined || this.#state.version.ts >= res.commitTs) {
          this.#optimistic.drop(token);
          this.#notify(this.#refreshOptimistic());
        } else {
          this.#optimistic.committed(token, res.commitTs);
        }
      }
      return { result: fromValue(res.result) as T, commitTs: res.commitTs };
    } catch (e) {
      if (token !== undefined && this.#optimistic.drop(token)) {
        this.#notify(this.#refreshOptimistic());
      }
      throw toLiveError(e);
    }
  }

  /**
   * Deploys a function bundle (one ES module) and, optionally, a schema. A
   * deploy refused as busy (`UNAVAILABLE`: an index change while mutations
   * run) is retried with backoff.
   */
  async deploy(
    bundle: string | Uint8Array,
    schema?: { tables: TableInput[] },
  ): Promise<{ deploymentId: string }> {
    const bytes = typeof bundle === "string" ? new TextEncoder().encode(bundle) : bundle;
    const init = {
      bundle: bytes,
      ...(schema === undefined
        ? {}
        : {
            schema: {
              tables: schema.tables.map((t) => ({ name: t.name, indexes: t.indexes ?? [] })),
            },
          }),
    };
    for (let attempt = 0; ; attempt++) {
      try {
        const res = await this.#rpc.deploy(init);
        return { deploymentId: res.deploymentId };
      } catch (e) {
        const err = toLiveError(e);
        if (!err.retryable || attempt + 1 >= this.#deployAttempts || this.#closed) throw err;
        await this.#sleep(this.#backoff(attempt));
      }
    }
  }

  /** Closes the stream and drops every subscription. */
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#stream?.abort();
    this.#wake?.();
    this.#subs.clear();
    this.#optimistic.clear();
    this.#overrides.clear();
  }

  // ---- used by subscriptions ----

  /** @internal */
  valueOf(def: QueryDef): LiveValue | undefined {
    if (this.#overrides.has(def.key)) return this.#overrides.get(def.key);
    const r = this.#state.results.get(def.id);
    return r?.kind === "value" ? r.value : undefined;
  }

  /** @internal */
  errorOf(def: QueryDef): LiveError | undefined {
    if (this.#overrides.has(def.key)) return undefined;
    const r = this.#state.results.get(def.id);
    return r?.kind === "error" ? r.error : undefined;
  }

  /** @internal */
  release(sub: Watcher): void {
    const set = this.#subs.get(sub.def.id);
    if (set === undefined) return;
    set.delete(sub);
    if (set.size > 0) return;
    this.#subs.delete(sub.def.id);
    this.#queries.remove(sub.def.id);
    this.#requestSync();
  }

  // ---- the stream ----

  async #run(): Promise<void> {
    let attempt = 0;
    let gaps = 0;
    while (!this.#closed) {
      const ctl = new AbortController();
      this.#stream = ctl;
      const set = this.#queries.start();
      const sent = new Set(set.queries.map((q) => q.id));
      const querySet = { version: set.version, queries: set.queries.map((q) => this.#spec(q)) };
      const resuming = !sameVersion(this.#state.version, ZERO);
      this.#state.restart(this.#state.version);
      const request: WatchRequest = create(WatchRequestSchema, {
        start: resuming
          ? { case: "resume", value: { lastVersion: this.#state.version, querySet } }
          : { case: "initial", value: querySet },
      });
      let first = true;
      let failure: unknown;
      let idle: ReturnType<typeof setTimeout> | undefined;
      const arm = (): void => {
        clearTimeout(idle);
        idle = setTimeout(() => {
          this.#restartNow = true;
          ctl.abort();
        }, this.#idleMs);
      };
      arm();
      try {
        for await (const t of this.#rpc.watch(request, { signal: ctl.signal })) {
          arm();
          if (t.sessionId !== "") this.#sessionId = t.sessionId;
          const applied = this.#state.apply(t);
          if (!applied.complete) continue;
          attempt = 0;
          gaps = 0;
          const changed = new Set(applied.changed);
          if (first) {
            first = false;
            for (const id of this.#state.retain(sent)) changed.add(id);
            this.#requestSync();
          }
          this.#afterTransition(changed);
        }
      } catch (e) {
        failure = e;
      } finally {
        clearTimeout(idle);
        ctl.abort();
      }
      this.#sessionId = undefined;
      this.#stream = undefined;
      if (this.#closed) break;
      if (failure instanceof VersionGap) {
        // Resume from the client's version; a second gap in a row starts over.
        gaps++;
        if (gaps > 1) this.#state.restart(ZERO);
        continue;
      }
      if (resuming && first && isCode(failure, Code.FailedPrecondition)) {
        // The server refused the resume: start a fresh session.
        this.#state.restart(ZERO);
        continue;
      }
      if (this.#restartNow) {
        this.#restartNow = false;
        continue;
      }
      await this.#sleep(this.#backoff(attempt++));
    }
    this.#running = undefined;
  }

  #spec(def: QueryDef): QuerySpec {
    return create(QuerySpecSchema, { queryId: def.id, function: def.fn, args: toValue(def.args) });
  }

  /** Sends `ModifyQuerySet` until the server holds the wanted set; one at a time. */
  #requestSync(): void {
    if (this.#syncing) {
      this.#syncAgain = true;
      return;
    }
    this.#syncing = true;
    void (async () => {
      try {
        do {
          this.#syncAgain = false;
          const session = this.#sessionId;
          if (session === undefined) return; // the next stream sends the whole set
          const d = this.#queries.diff();
          if (d === undefined) continue;
          try {
            await this.#rpc.modifyQuerySet({
              sessionId: session,
              baseVersion: d.baseVersion,
              newVersion: d.newVersion,
              changes: [
                ...d.add.map((q) => ({ change: { case: "add" as const, value: this.#spec(q) } })),
                ...d.remove.map((id) => ({ change: { case: "remove" as const, value: id } })),
              ],
            });
          } catch {
            // The session is gone or disagrees: a new stream sends the wanted set.
            if (this.#sessionId === session) {
              this.#restartNow = true;
              this.#stream?.abort();
            }
            return;
          }
          if (this.#sessionId === session) this.#queries.accepted(d);
          else return;
        } while (this.#syncAgain || this.#queries.diff() !== undefined);
      } finally {
        this.#syncing = false;
      }
    })();
  }

  #afterTransition(changed: Set<number>): void {
    if (this.#optimistic.size > 0 || this.#overrides.size > 0) {
      this.#optimistic.prune(this.#state.version.ts);
      for (const id of this.#refreshOptimistic()) changed.add(id);
    }
    this.#notify(changed);
  }

  /** Recomputes the optimistic view; returns the query ids whose shown value may have changed. */
  #refreshOptimistic(): Set<number> {
    const before = this.#overrides;
    const after =
      this.#optimistic.size === 0
        ? new Map<string, LiveValue | undefined>()
        : this.#optimistic.view((key) => {
            const def = this.#queries.byKey(key);
            const r = def === undefined ? undefined : this.#state.results.get(def.id);
            return r?.kind === "value" ? r.value : undefined;
          });
    this.#overrides = after;
    const ids = new Set<number>();
    for (const key of [...before.keys(), ...after.keys()]) {
      const def = this.#queries.byKey(key);
      if (def !== undefined) ids.add(def.id);
    }
    return ids;
  }

  #notify(ids: Iterable<number>): void {
    for (const id of ids) {
      for (const sub of [...(this.#subs.get(id) ?? [])]) sub.notify();
    }
  }

  #backoff(attempt: number): number {
    const base = Math.min(this.#maxMs, this.#minMs * 2 ** Math.min(attempt, 30));
    return base / 2 + (Math.random() * base) / 2;
  }

  #sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const done = (): void => {
        clearTimeout(timer);
        this.#wake = undefined;
        resolve();
      };
      const timer = setTimeout(done, ms);
      this.#wake = done;
    });
  }
}

function isCode(e: unknown, code: Code): boolean {
  return e instanceof ConnectError && e.code === code;
}
