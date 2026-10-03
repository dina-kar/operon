// Consistency tokens (design §44 §7.4, D609; runtime contract R4).
//
// A write answers with a `consistency_token`; a read accepts one, so a caller
// that just wrote can read its own write. Threading those by hand is the
// caller's job today. The session store is the alternative §44 §7.4 asks for:
// off by default, and when a call opts in, every response's token is merged
// into the session and attached to later reads.
//
// **The token's encoding is not in the protos yet.** §05 §5 defines the
// semantics (offsets per stream and partition, `STRONG` as the default,
// `EVENTUAL`, `AT_LEAST{token}`) and API1's write paths carry it as an opaque
// `v1:` string; §44 §7.4 says it merges by "max offset per stream and
// partition", which needs the encoding to be parsed. Until that lands this
// store keeps the token it was given, refuses to merge two different tokens
// into a wrong one, and says so — a silently-wrong consistency token reads
// stale data, which is worse than an error.

import { LoamsError } from './errors.js';
import { Code } from '@connectrpc/connect';

/** The prefix every token carries (§44 §7.4). */
export const TOKEN_PREFIX = 'v1:';

/** Whether a string looks like a consistency token. */
export function isConsistencyToken(value: string): boolean {
  return value.startsWith(TOKEN_PREFIX) && value.length > TOKEN_PREFIX.length;
}

/**
 * A session's consistency token.
 *
 * Merging is deliberately conservative while the encoding is opaque: the store
 * keeps what it has, and a second, different token is a conflict the caller has
 * to resolve, not something to merge blindly.
 */
export class ConsistencySession {
  #token: string | undefined;
  /** How many times two different tokens met. Surfaced so the limitation is
   * visible rather than silent. */
  #conflicts = 0;

  /** The token to attach to the next read. */
  current(): string | undefined {
    return this.#token;
  }

  /** Folds a token the server returned in. */
  record(token: string | undefined): void {
    if (token === undefined || token === '') {
      return;
    }
    if (!isConsistencyToken(token)) {
      this.#conflicts += 1;
      throw new LoamsError(`not a consistency token: ${token}`, { code: Code.Internal });
    }
    if (this.#token === undefined || this.#token === token) {
      this.#token = token;
      return;
    }
    this.#conflicts += 1;
    throw new LoamsError(
      'two different consistency tokens met and the encoding cannot merge them yet; ' +
        'the session keeps the first. Merging by stream and partition offset arrives with ' +
        'the write paths that carry offsets (§44 §7.4, D609).',
      { code: Code.FailedPrecondition },
    );
  }

  /** How many unmergeable pairs the session has seen. */
  get conflicts(): number {
    return this.#conflicts;
  }

  /** Forgets the token, so the next read is not held to it. */
  clear(): void {
    this.#token = undefined;
    this.#conflicts = 0;
  }
}
