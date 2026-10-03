// The `Loams` object: one client with namespaced modules (design §44 §7.1).
//
//   const loams = new Loams({ endpoint, auth: apiKey(process.env.LOAMS_API_KEY) });
//   const info = await loams.instance.getInstance({});
//   for await (const t of loams.live.watch({ start: { case: 'initial', value: querySet } })) {}
//
// What is generated and what is hand-written, once more, because it decides
// where a change goes. The **module surface** is generated: `gen/facade.ts` has
// one interface per annotated service and one method per `FacadeOptions`
// call, with the generated message types, and `MODULES` says which service and
// which retry class each call has. The **runtime** behind those methods is
// hand-written, once, in `runtime/`: transport, credentials, retry, errors,
// tokens, pagination, streams. This file is the thin join — it reads the
// generated table, builds one object per module out of it, and hands every call
// to the same invoker. It contains no method names and no RPC paths, which is
// why annotating a proto is enough to add an SDK method.

import { Code } from '@connectrpc/connect';
import type { Transport } from '@connectrpc/connect';
import {
  MODULES,
  PROTO_PACKAGES,
  PROTO_REV,
  SERVICE_DESCRIPTORS,
  type CallBinding,
  type InstanceModule,
  type LiveModule,
  type ModuleBinding,
  type TablesModule,
} from './gen/facade.js';
import { CallInvoker, type CallOptions, type ConsistencyTokenStore } from './runtime/call.js';
import { ConsistencySession } from './runtime/consistency.js';
import { LoamsError } from './runtime/errors.js';
import { paginate, type PageRequestOptions } from './runtime/pagination.js';
import { SystemApi, type VersionReport } from './system.js';
import { DEFAULT_MAX_RETRIES } from './runtime/retry.js';
import { apiKey, type TokenSource } from './runtime/token-source.js';
import { createLoamsTransport, type TransportOptions } from './runtime/transports.js';
import { watch, type ResumeOptions } from './runtime/streams.js';

/** How to build a `Loams`. */
export interface LoamsOptions {
  /** The instance's base URL, for example `https://acme.loams.dev`. A
   * loopback stack is `http://127.0.0.1:8080`. */
  endpoint: string;
  /**
   * The bearer source. A string is taken as an API key, which is what a
   * script or a CI job has; pass `envToken()` or `oidcExchange()` for the
   * rest. Omitted means an unauthenticated client, which is what
   * `loams.instance.getInstance` needs anyway.
   */
  auth?: TokenSource | string;
  /**
   * The transport. Defaults to Connect over `fetch` pointed at `endpoint`,
   * which is the browser, Node, Deno and Bun path (see `runtime/transports.ts`).
   * `@loams/client/node` exports `createNodeTransport` for HTTP/2 gRPC.
   *
   * A transport carries its own base URL, so a transport passed here must be
   * built for the same instance as `endpoint`.
   */
  transport?: Transport;
  /** Transport-level options, used when `transport` is not given. They win
   * over `endpoint` where they overlap, so `baseUrl` here replaces it. */
  transportOptions?: TransportOptions;
  /** Retries after the first attempt, for every call. `0` disables them. */
  maxRetries?: number;
  /**
   * Hold a session consistency token across calls (D609). Off by default:
   * every read is then `STRONG` on its own, which is correct but does not give
   * read-your-writes across processes.
   */
  sessionConsistency?: boolean;
  /** The client's deadline, when the transport does not set one. */
  timeoutMs?: number;
}

export type { Catalogue, VersionReport } from './system.js';

/** The generated modules, keyed by name. A module the generator has not seen
 * does not appear, so `loams.vector` is absent until `QueryService/Search`
 * carries its facade options (API1 Task 2). */
export interface LoamsModules {
  readonly instance: InstanceModule;
  readonly live: LiveModule;
  readonly tables: TablesModule;
  readonly [module: string]: unknown;
}

/** One SDK, over one instance. */
export class Loams {
  /** `loams.instance` — what this instance is, and who the caller is. */
  readonly instance: InstanceModule;
  /** `loams.live` — the live sync session half. Its package is `unstable`. */
  readonly live: LiveModule;
  /** `loams.tables` — the table half of the same service (design §44 §7.2). */
  readonly tables: TablesModule;
  /** The module catalogue, feature detection and the version check. */
  readonly system: SystemApi;
  /** Every generated module, by name: the catalogue a caller iterates. */
  readonly modules: Readonly<Record<string, unknown>>;

  readonly #invoker: CallInvoker;
  readonly #consistency: ConsistencySession | undefined;

  constructor(options: LoamsOptions) {
    // The endpoint lives on the transport, which is where connect-es reads it.
    const transport =
      options.transport ??
      createLoamsTransport({ baseUrl: options.endpoint, ...options.transportOptions });
    const tokenSource =
      options.auth === undefined
        ? undefined
        : typeof options.auth === 'string'
          ? apiKey(options.auth)
          : options.auth;
    // The session store is built first so the invoker can hold it: a call
    // that asks for the session's consistency token reads and records through
    // the same store the client exposes.
    this.#consistency = options.sessionConsistency === true ? new ConsistencySession() : undefined;
    this.#invoker = new CallInvoker(
      transport,
      tokenSource,
      options.maxRetries ?? DEFAULT_MAX_RETRIES,
      this.#consistency,
    );

    this.modules = buildModules(this.#invoker);
    this.instance = this.modules.instance as InstanceModule;
    this.live = this.modules.live as LiveModule;
    this.tables = this.modules.tables as TablesModule;
    this.system = new SystemApi(
      (request, callOptions) => this.instance.getInstance(request, callOptions),
      {
        endpoint: options.endpoint,
        maxRetries: options.maxRetries,
        timeoutMs: options.timeoutMs,
      },
    );
  }

  /** The session consistency token store, when `sessionConsistency` is on. */
  get consistency(): ConsistencyTokenStore | undefined {
    return this.#consistency;
  }

  /** The generated module bindings, as the SDK sees them. */
  get bindings(): readonly ModuleBinding[] {
    return MODULES;
  }

  /** Every item of a paged call (D617's iterator).
   *
   * `loams.paginate('collections', 'listCollections', { namespace })`. The
   * per-call alias §44 §7.4 sketches (`loams.collections.listAll`) arrives with
   * the first paged RPC, when there is a generated signature to hang it on;
   * until then this is the same iterator under its module and call name.
   */
  paginate<Request extends object, Response extends object, Item>(
    module: string,
    call: string,
    request: Request,
    options: CallOptions & PageRequestOptions = {},
  ): AsyncGenerator<Item, void, undefined> {
    const binding = this.binding(module, call);
    const method = this.modules[module] as Record<string, unknown> | undefined;
    const fn = method?.[call];
    if (typeof fn !== 'function') {
      throw new LoamsError(`loams.${module} has no call ${call}`, { code: Code.Internal });
    }
    return paginate<Request, Response, Item>(
      binding,
      fn as (request: Request, options?: CallOptions) => Promise<Response>,
      request,
      options,
    );
  }

  /** A server stream that reconnects from its cursor (runtime contract R7). */
  stream<Message, Request extends object>(
    module: string,
    call: string,
    request: Request,
    options: CallOptions & ResumeOptions<Message, Request>,
  ): AsyncGenerator<Message, void, undefined> {
    const binding = this.binding(module, call);
    const method = this.modules[module] as Record<string, unknown> | undefined;
    const fn = method?.[call];
    if (typeof fn !== 'function') {
      throw new LoamsError(`loams.${module} has no call ${call}`, { code: Code.Internal });
    }
    return watch<Message, Request>(
      binding,
      fn as (request: unknown, options?: CallOptions) => AsyncIterable<Message>,
      request,
      options,
    );
  }

  /** The binding a module and call name identify, or a clear error. */
  binding(module: string, call: string): CallBinding {
    const found = MODULES.find((entry) => entry.name === module)?.calls.find(
      (entry) => entry.name === call,
    );
    if (found === undefined) {
      throw new LoamsError(`loams.${module} has no generated call ${call}`, {
        code: Code.Internal,
      });
    }
    return found;
  }

  /** The proto revision this SDK declares (§44 §10.3). */
  get protoRev(): string {
    return PROTO_REV;
  }

  /** The proto packages this SDK speaks. */
  get protoPackages(): readonly string[] {
    return PROTO_PACKAGES.filter((name) => name.startsWith('loams.'));
  }

  /** Forgets the cached service catalogue, so the next check calls again. */
  invalidateCatalogue(): void {
    this.system.invalidate();
  }
}

/**
 * Builds one object per generated module: one function per generated call,
 * each delegating to the same invoker.
 *
 * A server-streaming call returns the `AsyncIterable` directly rather than a
 * promise of one, so `for await (const t of loams.live.watch(...))` reads the
 * way design §44 §7.1 says it should.
 */
function buildModules(invoker: CallInvoker): Record<string, unknown> {
  const modules: Record<string, unknown> = {};
  for (const module of MODULES) {
    const descriptor = SERVICE_DESCRIPTORS[module.service];
    if (descriptor === undefined) {
      continue;
    }
    invoker.register(module.service, descriptor);
    const calls: Record<string, unknown> = {
      module: module.name,
      service: module.service,
      unstable: module.unstable,
    };
    for (const binding of module.calls) {
      // A server-streaming call returns the `AsyncIterable` directly rather
      // than a promise of one, so `for await (const t of loams.live.watch(..))`
      // reads the way design §44 §7.1 says it should. `loams.stream()` wraps
      // the same call with cursor resume.
      calls[binding.name] =
        binding.streaming === 'server'
          ? (request: unknown, options?: CallOptions) => invoker.stream(binding, request, options)
          : (request: unknown, options?: CallOptions) => invoker.unary(binding, request, options);
    }
    modules[module.name] = calls;
  }
  return modules;
}
