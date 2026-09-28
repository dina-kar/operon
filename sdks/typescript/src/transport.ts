/** Sends a `WireRequest` with `fetch`, the retry policy and error mapping (plan M1.6 Task 5 rule 4). */
import { TransportError } from "./errors.js";
import { encode } from "./json.js";
import { RetryPolicy } from "./retry.js";
import type { ClientOptions } from "./types.js";
import { errorFromResponse, type WireRequest, type WireResponse } from "./wire.js";

export const DEFAULT_BASE_URL = "http://127.0.0.1:8080";

/** Waits `ms`, rejecting with the signal's reason as soon as it aborts. */
export function abortableSleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason);
      return;
    }
    const onAbort = (): void => {
      clearTimeout(timer);
      reject(signal?.reason);
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

function describe(error: unknown): string {
  if (error instanceof Error) return `${error.name}: ${error.message}`;
  return String(error);
}

export class Transport {
  readonly baseUrl: string;
  readonly timeoutMs: number;
  private readonly headers: Record<string, string>;
  private readonly policy: RetryPolicy;
  private readonly fetchFn: typeof globalThis.fetch;
  private readonly sleep: (ms: number, signal?: AbortSignal) => Promise<void>;

  constructor(options: ClientOptions = {}) {
    const timeoutMs = options.timeoutMs ?? 30_000;
    if (!(timeoutMs > 0) || !Number.isFinite(timeoutMs)) {
      throw new RangeError(`timeoutMs must be a positive number, got ${timeoutMs}`);
    }
    this.baseUrl = (options.baseUrl ?? DEFAULT_BASE_URL).replace(/\/+$/, "");
    this.timeoutMs = timeoutMs;
    this.headers = { ...(options.headers ?? {}) };
    this.policy = new RetryPolicy(
      options.maxRetries ?? 3,
      options.backoffBaseMs ?? 100,
      options.backoffMaxMs ?? 2_000,
      options.random ?? Math.random,
    );
    // Resolved per call, so a fetch installed later is used, and never called unbound.
    this.fetchFn = options.fetch ?? ((input, init) => globalThis.fetch(input, init));
    this.sleep = options.sleep ?? abortableSleep;
  }

  private url(request: WireRequest): string {
    const url = this.baseUrl + request.path;
    const entries = Object.entries(request.query ?? {});
    if (entries.length === 0) return url;
    return `${url}?${new URLSearchParams(entries.map(([k, v]) => [k, String(v)])).toString()}`;
  }

  /** Sends `request`, retrying per Ruling 5; resolves with a 2xx answer or rejects with a typed error. */
  async send(request: WireRequest, signal?: AbortSignal): Promise<WireResponse> {
    const url = this.url(request);
    const headers: Record<string, string> = { ...this.headers, ...(request.headers ?? {}) };
    const init: RequestInit = { method: request.method, headers };
    if (request.body !== undefined) {
      // Encoded once, before anything is sent: a non-finite number throws here.
      init.body = encode(request.body);
      headers["content-type"] = "application/json";
    }
    const timeoutMs = this.timeoutMs + (request.extraTimeoutMs ?? 0);
    let retry = 0;
    for (;;) {
      signal?.throwIfAborted();
      retry += 1;
      const timeout = AbortSignal.timeout(timeoutMs);
      init.signal = signal === undefined ? timeout : AbortSignal.any([signal, timeout]);
      let response: Response;
      let text: string;
      try {
        response = await this.fetchFn(url, init);
        text = await response.text();
      } catch (error) {
        // The caller's abort wins and is never retried.
        if (signal?.aborted) throw signal.reason;
        const retryable = error instanceof TypeError || timeout.aborted;
        const delay = retryable ? this.policy.afterFailure(request.idempotent, retry) : null;
        if (delay === null) throw new TransportError(describe(error), error);
        await this.sleep(delay, signal);
        continue;
      }
      if (response.status >= 200 && response.status < 300) {
        return { status: response.status, headers: response.headers, text };
      }
      const delay = this.policy.afterStatus(
        response.status,
        response.headers,
        request.idempotent,
        retry,
      );
      if (delay === null) throw errorFromResponse(response.status, text);
      await this.sleep(delay, signal);
    }
  }
}
