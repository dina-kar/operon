// Retry classes and backoff (design §44 §7.4, D610; runtime contract R2).
//
// The numbers are M1.6 Ruling 5's and are the same in every SDK: base 100 ms,
// doubling, capped at 2 s, 3 retries, full jitter. Full jitter rather than
// exponential backoff alone, because every client retrying at the same instant
// after a node restart is how a recovering node gets knocked over again.

import { Code } from '@connectrpc/connect';
import { LoamsError } from './errors.js';

/** The first backoff, and the multiplier's base. */
export const BASE_DELAY_MS = 100;
/** The ceiling on one backoff. */
export const MAX_DELAY_MS = 2_000;
/** Retries after the first attempt, when nothing overrides it. */
export const DEFAULT_MAX_RETRIES = 3;
/** `RetryInfo.retry_delay` is honoured up to this, when the server sends one. */
export const MAX_SERVER_DELAY_MS = 30_000;

/** The codes a retry may answer (D610). */
const RETRYABLE: ReadonlySet<Code> = new Set<Code>([
  Code.Unavailable,
  Code.DeadlineExceeded,
  Code.ResourceExhausted,
]);

/** Whether the code is one a retry may answer. */
export function isRetryableCode(code: Code): boolean {
  return RETRYABLE.has(code);
}

/**
 * The backoff before retry number `attempt` (0 for the first retry), with full
 * jitter: uniform over `[0, min(cap, base × 2^attempt)]`. A server-sent
 * `RetryInfo.retry_delay` replaces it, up to {@link MAX_SERVER_DELAY_MS}.
 */
export function backoffMs(attempt: number, serverDelayMs?: number): number {
  if (serverDelayMs !== undefined && serverDelayMs > 0) {
    return Math.min(serverDelayMs, MAX_SERVER_DELAY_MS);
  }
  const ceiling = Math.min(MAX_DELAY_MS, BASE_DELAY_MS * 2 ** attempt);
  return Math.floor(Math.random() * ceiling);
}

/** Sleeps, for the retry loop and for the tests that would otherwise wait. */
export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (ms <= 0) {
    return Promise.resolve();
  }
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal?.reason ?? new Error('aborted'));
    };
    if (signal?.aborted === true) {
      clearTimeout(timer);
      reject(signal.reason ?? new Error('aborted'));
      return;
    }
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

/**
 * Whether one more attempt is allowed.
 *
 * `retrySafe` is the call's class from the generated bindings: `safe` for
 * reads and idempotent RPCs, `manual` for a mutation. A mutation becomes
 * retryable once it carries an idempotency key, because the key is what makes
 * the repeat safe — see `callWithRetry` in `call.ts`, which sets one before the
 * first attempt and reuses it on every retry.
 */
export function shouldRetry(
  error: LoamsError,
  retrySafe: boolean,
  attempt: number,
  maxRetries: number,
): boolean {
  if (attempt >= maxRetries) {
    return false;
  }
  return retrySafe && isRetryableCode(error.code);
}
