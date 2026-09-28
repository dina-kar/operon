/** The SDK's own value types (the wire JSON lives only in `wire.ts`). */
import type { SchemaInput } from "./schema.js";
import type { ConsistencyToken } from "./token.js";

/** A document id: a u64 (`number` up to 2^53 − 1, `bigint` above), a string, or a UUID. */
export type Id = number | bigint | string | { uuid: string };

/**
 * How fresh a read must be: `"strong"` (default), `"eventual"`, at least a token
 * (or its text), or exactly a scan plan's `Pin`.
 */
export type Consistency = "strong" | "eventual" | ConsistencyToken | string | Pin;

/** A pinned read (a scan plan's `pin`): one manifest version and its token. */
export interface Pin {
  /** A u64: a `bigint` above 2^53 − 1. */
  manifestVersion: number | bigint;
  token: ConsistencyToken;
}

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

export interface ReadOptions extends RequestOptions {
  /** Default `"strong"`. */
  consistency?: Consistency;
}

// ---------------------------------------------------------------- documents

/** A dense vector: every element a finite number. */
export type VectorLike = number[] | Float32Array | Float64Array;

/**
 * A sparse vector (overview A27), checked before sending: equal lengths, unique
 * integer indices in 0..2^32 − 1, finite values. The order is kept: the server sorts.
 */
export interface SparseVector {
  indices: number[];
  values: number[];
}

/** A document to upsert: its id, JSON source and vectors by field name. */
export interface DocumentInput {
  id: Id;
  source?: Record<string, unknown>;
  vectors?: Record<string, VectorLike>;
  sparseVectors?: Record<string, SparseVector>;
}

export type PatchMode = "merge_deep" | "merge_top" | "replace";

/** A partial update; `null` in `vectors`/`sparseVectors` removes that vector. */
export interface PatchInput {
  id: Id;
  source?: Record<string, unknown>;
  /** Default `"merge_deep"`. */
  mode?: PatchMode;
  deleteKeys?: string[];
  vectors?: Record<string, VectorLike | null>;
  sparseVectors?: Record<string, SparseVector | null>;
  /** Written instead when the document does not exist. */
  upsert?: DocumentInput;
}

export type Op = { upsert: DocumentInput } | { delete: Id } | { patch: PatchInput };

export interface WriteResult {
  token: ConsistencyToken;
  /** Per op: `created`, `updated`, `deleted`, `not_found`, `noop` or `accepted`. */
  results: string[];
}

export interface StoredDoc {
  id: Id;
  /** `{}` when the read selected no source. */
  source: Record<string, unknown>;
  vectors: Record<string, number[]>;
  sparseVectors: Record<string, SparseVector>;
}

// ---------------------------------------------------------------- collections

export interface CollectionInfo {
  id: number;
  name: string;
  schema: SchemaInput & { version?: number };
  partitions: number | null;
  liveDocCount: number | null;
  /** The whole answer. */
  raw: Record<string, unknown>;
}

// ---------------------------------------------------------------- search and SQL

export interface Hit {
  id: Id;
  score: number;
  /** As returned (ending with the id tie-break); pass them to `searchAfter`. */
  sortValues: unknown[];
  source: Record<string, unknown> | null;
  vectors: Record<string, number[]>;
  sparseVectors: Record<string, SparseVector>;
  highlight: Record<string, string[]>;
}

export interface TotalHits {
  value: number;
  relation: "eq" | "gte";
}

export interface SearchResponse {
  hits: Hit[];
  total: TotalHits | null;
  aggregations: Record<string, unknown> | null;
  groups: Record<string, unknown>[] | null;
  readToken: ConsistencyToken;
}

export interface Column {
  name: string;
  /** The Arrow type name, e.g. `Int64`, `Utf8`, `Timestamp(µs, "UTC")`. */
  type: string;
}

export interface SqlResult {
  columns: Column[];
  /** Integers above 2^53 − 1 are `bigint`. */
  rows: unknown[][];
  /** Whether the server stopped at its row cap. */
  truncated: boolean;
  /** One object per row, keyed by column name. */
  toObjects(): Record<string, unknown>[];
}

// ---------------------------------------------------------------- scan plans (W15)

export interface LanceVersion {
  uri: string | null;
  /** A u64 (a detached version is above 2^63): a `bigint` above 2^53 − 1. */
  version: number | bigint;
  manifestPath: string;
}

export interface ScanFragment {
  id: number;
  physicalRows: number;
  deletedRows: number;
  liveRows: number;
  /** The data files' paths. */
  files: string[];
  deletionFile: string | null;
  /** Lance's own fragment JSON. */
  lance: Record<string, unknown>;
}

export interface ScanColumn {
  name: string;
  dataType: string;
  role: string;
  vector?: string;
  dim?: number;
}

export interface ScanPlan {
  collection: string;
  collectionId: number;
  /** A u64: a `bigint` above 2^53 − 1. */
  manifestVersion: number | bigint;
  lance: LanceVersion | null;
  fragments: ScanFragment[];
  liveRows: number;
  columns: ScanColumn[];
  /** The requested state holds writes the Lance version lacks: read them through `pin`. */
  tail: boolean;
  tailRecords: number;
  durableToken: ConsistencyToken;
  pin: Pin;
  plannedAtMs: number;
  expiresAtMs: number | null;
  raw: Record<string, unknown>;
}

/** `"current"`, a manifest version (`number` or `bigint`), or a token (another string is parsed as one). */
export type ScanAt = "current" | number | bigint | ConsistencyToken | string;
