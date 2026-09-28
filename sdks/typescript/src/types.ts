/** The SDK's own value types (the wire JSON lives only in `wire.ts`). */
import type { ConsistencyToken } from "./token.js";

/** A document id: a u64 (`number` up to 2^53 − 1, `bigint` above), a string, or a UUID. */
export type Id = number | bigint | string | { uuid: string };

/** How fresh a read must be: `"strong"` (default), `"eventual"`, or at least a token. */
export type Consistency = "strong" | "eventual" | ConsistencyToken | string;

export interface ClientOptions {
  /** Default `"http://127.0.0.1:8080"`. */
  baseUrl?: string;
  /** Per attempt; a fetch's `maxWaitMs` is added to it. Default 30 000. */
  timeoutMs?: number;
  /** Default 3. */
  maxRetries?: number;
  /** Default 100. */
  backoffBaseMs?: number;
  /** Default 2 000. */
  backoffMaxMs?: number;
  /** Sent with every request. */
  headers?: Record<string, string>;
  /** Test hook or a custom agent; default the global `fetch`. */
  fetch?: typeof globalThis.fetch;
  /** Test hook: waits `ms` before a retry, rejecting when `signal` aborts. */
  sleep?: (ms: number, signal?: AbortSignal) => Promise<void>;
  /** Test hook: jitter in `[0, 1)`; default `Math.random`. */
  random?: () => number;
}

export interface RequestOptions {
  /** Aborts the request and any retry wait; the promise rejects with the signal's reason. */
  signal?: AbortSignal;
}

export interface ProduceRecord {
  key?: Uint8Array | string;
  value?: Uint8Array | string;
  headers?: Array<[string, Uint8Array | string | null]>;
  timestampMs?: number;
}

export interface FetchedRecord {
  offset: number;
  key: Uint8Array | null;
  value: Uint8Array | null;
  headers: Array<[string, Uint8Array | null]>;
  timestampMs: number;
}

export interface ProduceResult {
  baseOffset: number;
  lastOffset: number;
  token: ConsistencyToken;
}

export interface FetchResult {
  records: FetchedRecord[];
  nextOffset: number;
  highWatermark: number;
  logStartOffset: number;
}

export interface PartitionInfo {
  partition: number;
  logStartOffset: number;
  highWatermark: number;
}

export interface StreamInfo {
  id: number;
  partitions: PartitionInfo[];
  maxAgeMs: number | null;
  maxBytes: number | null;
}
