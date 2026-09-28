/** Typed errors mapped from the native API's error body `{"error": code, "message": text, …}`. */

export interface ErrorInit {
  code: string;
  status: number;
  body?: Record<string, unknown>;
  cause?: unknown;
}

function intOrUndefined(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isSafeInteger(value)) return value;
  if (
    typeof value === "bigint" &&
    value >= BigInt(Number.MIN_SAFE_INTEGER) &&
    value <= BigInt(Number.MAX_SAFE_INTEGER)
  ) {
    return Number(value);
  }
  return undefined;
}

/** An error answered by the server (or, for `TransportError`, no answer at all). */
export class OperonError extends Error {
  readonly code: string;
  readonly status: number;
  readonly body: Record<string, unknown>;

  constructor(message: string, init: ErrorInit) {
    super(message, init.cause === undefined ? undefined : { cause: init.cause });
    this.name = new.target.name;
    this.code = init.code;
    this.status = init.status;
    this.body = { ...(init.body ?? {}) };
  }

  /** Whether the same request may succeed when sent again (a 503). */
  get retryable(): boolean {
    return this.status === 503;
  }
}

/** 400 `invalid_argument`. */
export class InvalidArgumentError extends OperonError {}

/** 400 `schema_violation`; `field` names the offending field when the server says. */
export class SchemaViolationError extends InvalidArgumentError {
  readonly field: string | undefined;

  constructor(message: string, init: ErrorInit) {
    super(message, init);
    const field = this.body.field;
    this.field = typeof field === "string" ? field : undefined;
  }
}

/** 404 `not_found`. */
export class NotFoundError extends OperonError {}

/** 409 `already_exists`; `id` is set on namespace, stream and link creates only. */
export class AlreadyExistsError extends OperonError {
  readonly id: number | undefined;

  constructor(message: string, init: ErrorInit) {
    super(message, init);
    this.id = intOrUndefined(this.body.id);
  }
}

/** 409 `conflict`. */
export class ConflictError extends OperonError {}

function offsetExtras(body: Record<string, unknown>): [number, number, number] | undefined {
  const offset = intOrUndefined(body.offset);
  const start = intOrUndefined(body.log_start_offset);
  const high = intOrUndefined(body.high_watermark);
  if (offset === undefined || start === undefined || high === undefined) return undefined;
  return [offset, start, high];
}

/** 416 `offset_out_of_range`, with the partition's bounds. */
export class OffsetOutOfRangeError extends OperonError {
  readonly offset: number;
  readonly logStartOffset: number;
  readonly highWatermark: number;

  constructor(message: string, init: ErrorInit) {
    super(message, init);
    const extras = offsetExtras(this.body);
    if (extras === undefined) {
      throw new RangeError("offset_out_of_range needs offset, log_start_offset, high_watermark");
    }
    [this.offset, this.logStartOffset, this.highWatermark] = extras;
  }
}

/** 429 `resource_exhausted`; `retryAfterMs` is the server's suggested wait. */
export class ResourceExhaustedError extends OperonError {
  readonly retryAfterMs: number | undefined;

  constructor(message: string, init: ErrorInit) {
    super(message, init);
    this.retryAfterMs = intOrUndefined(this.body.retry_after_ms);
  }
}

/** 503 `unavailable`: retryable (the outcome of a write may be unknown). */
export class UnavailableError extends OperonError {}

/** 504 `timeout`. */
export class OperonTimeoutError extends OperonError {}

/** 500 `internal` and other 5xx answers. */
export class InternalError extends OperonError {}

/** No usable answer: `code` is `"transport"`, `status` 0, `cause` the fetch error. */
export class TransportError extends OperonError {
  constructor(message: string, cause?: unknown) {
    super(message, { code: "transport", status: 0, cause });
  }
}

type ErrorClass = new (message: string, init: ErrorInit) => OperonError;

const BY_CODE: Record<string, ErrorClass> = {
  invalid_argument: InvalidArgumentError,
  schema_violation: SchemaViolationError,
  not_found: NotFoundError,
  already_exists: AlreadyExistsError,
  conflict: ConflictError,
  offset_out_of_range: OffsetOutOfRangeError,
  resource_exhausted: ResourceExhaustedError,
  internal: InternalError,
  unavailable: UnavailableError,
  timeout: OperonTimeoutError,
};

function byStatus(status: number, body: Record<string, unknown>): ErrorClass {
  switch (status) {
    case 400:
      return InvalidArgumentError;
    case 404:
      return NotFoundError;
    case 409:
      return ConflictError;
    case 416:
      return offsetExtras(body) === undefined ? OperonError : OffsetOutOfRangeError;
    case 429:
      return ResourceExhaustedError;
    case 503:
      return UnavailableError;
    case 504:
      return OperonTimeoutError;
    default:
      return status >= 500 && status <= 599 ? InternalError : OperonError;
  }
}

/** Maps a JSON error body (with a string `error`) to its typed error. */
export function errorFromBody(
  status: number,
  body: Record<string, unknown> & { error: string },
): OperonError {
  const code = body.error;
  const text = typeof body.message === "string" ? body.message : code;
  let cls = Object.hasOwn(BY_CODE, code) ? (BY_CODE[code] as ErrorClass) : byStatus(status, body);
  if (cls === OffsetOutOfRangeError && offsetExtras(body) === undefined) cls = OperonError;
  return new cls(text, { code, status, body });
}
