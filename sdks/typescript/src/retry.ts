/**
 * The retry policy (plan M1.6 Ruling 5, Task 5 rule 4).
 *
 * A 503 is retried on idempotent requests. A network failure or a per-attempt
 * timeout is retried only on idempotent requests too: `fetch` cannot tell a
 * failure before sending from one after it, so stream produce is never resent.
 */
import { retryAfterSeconds } from "./wire.js";

export const MAX_RETRY_AFTER_S = 30;

export class RetryPolicy {
  readonly maxRetries: number;
  readonly backoffBaseMs: number;
  readonly backoffMaxMs: number;
  readonly random: () => number;

  constructor(
    maxRetries: number,
    backoffBaseMs: number,
    backoffMaxMs: number,
    random: () => number,
  ) {
    if (!Number.isSafeInteger(maxRetries) || maxRetries < 0) {
      throw new RangeError(`maxRetries must be a non-negative integer, got ${maxRetries}`);
    }
    if (!(backoffBaseMs >= 0) || !(backoffMaxMs >= 0)) {
      throw new RangeError("backoffBaseMs and backoffMaxMs must be non-negative");
    }
    this.maxRetries = maxRetries;
    this.backoffBaseMs = backoffBaseMs;
    this.backoffMaxMs = backoffMaxMs;
    this.random = random;
  }

  /** The wait before retry number `retry` (1-based), with jitter. */
  backoff(retry: number): number {
    const capped = Math.min(this.backoffMaxMs, this.backoffBaseMs * 2 ** (retry - 1));
    return capped * (0.5 + 0.5 * this.random());
  }

  /** The wait before resending after a failed attempt, or `null` to give up. */
  afterFailure(idempotent: boolean, retry: number): number | null {
    if (!idempotent || retry > this.maxRetries) return null;
    return this.backoff(retry);
  }

  /** The wait before resending after a non-2xx answer, or `null` to give up. */
  afterStatus(status: number, headers: Headers, idempotent: boolean, retry: number): number | null {
    if (status !== 503 || !idempotent || retry > this.maxRetries) return null;
    let delay = this.backoff(retry);
    const retryAfter = retryAfterSeconds(headers);
    if (retryAfter !== null) {
      if (retryAfter > MAX_RETRY_AFTER_S) return null;
      delay = Math.max(delay, retryAfter * 1000);
    }
    return delay;
  }
}
