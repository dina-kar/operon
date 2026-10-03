// The typed error hierarchy of design §44 §7.4, decision D611.
//
// A failed RPC carries a Connect code and one `loams.errors.v1.ErrorInfo` in
// its details. The `reason` is what callers branch on: it is a stable
// `snake_case` string, registered in `docs/api/reasons.md` and generated into
// this package's `Reason` union, so `error.reason === 'feature_not_in_variant'`
// narrows the type and a reason the registry has lost stops compiling. The
// `message` is for people and may change; nothing in an SDK branches on it.

import { Code, ConnectError } from '@connectrpc/connect';
import { ErrorInfoSchema } from '@loams/proto/errors';
import { REASONS, type Reason } from '../gen/facade.js';

/** The registry, as a runtime guard: a reason off the wire that the generated
 * union does not have is a server newer than this SDK, and is surfaced rather
 * than dropped. */
const KNOWN_REASONS: ReadonlySet<string> = new Set<string>(REASONS);

/** What every Loams failure carries. */
export class LoamsError extends Error {
  /** The Connect code, the canonical classification. */
  readonly code: Code;
  /** The stable cause, when the server sent an `ErrorInfo`. Undefined means
   * the failure came from below the API (a socket, a timeout, a CORS
   * rejection), not from a Loams service. */
  readonly reason: Reason | undefined;
  /** Structured context the server sent. Never secrets. */
  readonly metadata: Readonly<Record<string, string>>;
  /** A short next step in the caller's locale, when the server sent one. */
  readonly hint: string | undefined;
  /** The RPC that failed, as `package.Service/Method`. */
  readonly rpc: string | undefined;
  /** True when the reason is not in this SDK's registry: the server is newer
   * than the SDK, so the reason is carried as text and the union is not. */
  readonly unknownReason: string | undefined;

  constructor(
    message: string,
    init: {
      code: Code;
      reason?: Reason;
      unknownReason?: string;
      metadata?: Record<string, string>;
      hint?: string;
      rpc?: string;
      cause?: unknown;
    },
  ) {
    super(message, init.cause === undefined ? undefined : { cause: init.cause });
    this.name = new.target.name;
    this.code = init.code;
    this.reason = init.reason;
    this.unknownReason = init.unknownReason;
    this.metadata = init.metadata ?? {};
    this.hint = init.hint;
    this.rpc = init.rpc;
  }
}

export class InvalidArgumentError extends LoamsError {}
export class NotFoundError extends LoamsError {}
export class AlreadyExistsError extends LoamsError {}
export class PermissionDeniedError extends LoamsError {}
export class UnauthenticatedError extends LoamsError {}
export class FailedPreconditionError extends LoamsError {}
export class ResourceExhaustedError extends LoamsError {}
export class UnavailableError extends LoamsError {}
export class DeadlineExceededError extends LoamsError {}
export class AbortedError extends LoamsError {}
export class InternalError extends LoamsError {}
export class UnimplementedError extends LoamsError {}

/**
 * A package this build variant does not carry (design §44 §4, D600).
 *
 * The server answers `unimplemented` with `ErrorInfo.reason =
 * feature_not_in_variant` and names the variant in `metadata.variant`, which
 * is what this class reads. A caller usually never gets here: `loams.system`
 * feature-detects from `GetInstance.services[]` before calling, so an
 * unavailable module throws `FeatureNotInVariantError` from
 * `loams.system.guard()` with no request spent. This class is the path for a
 * caller who skipped the guard, or whose instance changed variant.
 */
export class FeatureNotInVariantError extends UnimplementedError {
  /** The build variant that was asked for, from `metadata.variant`. */
  readonly variant: string | undefined;

  constructor(
    message: string,
    init: ConstructorParameters<typeof UnimplementedError>[1] & { variant?: string },
  ) {
    super(message, init);
    this.variant = init.variant;
  }
}

/** A token the server rejected as expired: `unauthenticated` with reason
 * `token_expired`. The runtime refreshes once and retries once (D608, R1). */
export class TokenExpiredError extends UnauthenticatedError {}

/** The code-to-class mapping of D611. Codes D611 does not name (`canceled`,
 * `out_of_range`, `data_loss`) fall through to the base `LoamsError`. */
const BY_CODE: Readonly<Record<Code, new (message: string, init: never) => LoamsError>> = {
  [Code.Canceled]: LoamsError,
  [Code.Unknown]: LoamsError,
  [Code.InvalidArgument]: InvalidArgumentError,
  [Code.DeadlineExceeded]: DeadlineExceededError,
  [Code.NotFound]: NotFoundError,
  [Code.AlreadyExists]: AlreadyExistsError,
  [Code.PermissionDenied]: PermissionDeniedError,
  [Code.ResourceExhausted]: ResourceExhaustedError,
  [Code.FailedPrecondition]: FailedPreconditionError,
  [Code.Aborted]: AbortedError,
  [Code.OutOfRange]: LoamsError,
  [Code.Unimplemented]: UnimplementedError,
  [Code.Internal]: InternalError,
  [Code.Unavailable]: UnavailableError,
  [Code.DataLoss]: LoamsError,
  [Code.Unauthenticated]: UnauthenticatedError,
} as unknown as Readonly<Record<Code, new (message: string, init: never) => LoamsError>>;

/** Whether a value is a Loams error rather than a raw `ConnectError`. */
export function isLoamsError(value: unknown): value is LoamsError {
  return value instanceof LoamsError;
}

/**
 * The `ErrorInfo` a Connect error carries, if any.
 *
 * The detail is looked up by its type URL rather than by position, so a
 * service that adds a detail of its own does not move `reason` out from under
 * a caller.
 */
export function errorInfo(error: ConnectError): ErrorInfoShape | undefined {
  const found = error.findDetails(ErrorInfoSchema);
  return found.length > 0 ? found[0] : undefined;
}

/** The fields of `loams.errors.v1.ErrorInfo` this package reads. */
export interface ErrorInfoShape {
  readonly reason: string;
  readonly metadata: { readonly [key: string]: string };
  readonly hint: string;
}

/**
 * Turns any thrown value into the typed hierarchy: a `ConnectError` becomes the
 * class its code names, with `reason` and `metadata` lifted out of the
 * `ErrorInfo` detail; anything else (a fetch rejection, an abort, a bug in
 * the SDK) becomes a `LoamsError` with `Code.Unknown`.
 */
export function toLoamsError(value: unknown, rpc?: string): LoamsError {
  if (value instanceof LoamsError) {
    return value;
  }
  if (!(value instanceof ConnectError)) {
    return new LoamsError(value instanceof Error ? value.message : String(value), {
      code: Code.Unknown,
      rpc,
      cause: value,
    });
  }
  const info = errorInfo(value);
  const raw = info?.reason ?? '';
  const init = {
    code: value.code,
    reason: KNOWN_REASONS.has(raw) ? (raw as Reason) : undefined,
    unknownReason: raw !== '' && !KNOWN_REASONS.has(raw) ? raw : undefined,
    metadata: info?.metadata ?? {},
    hint: info?.hint === '' ? undefined : info?.hint,
    rpc,
    cause: value,
  };
  if (init.reason === 'feature_not_in_variant') {
    return new FeatureNotInVariantError(value.rawMessage, {
      ...init,
      variant: init.metadata.variant,
    });
  }
  if (init.code === Code.Unauthenticated && init.reason === 'token_expired') {
    return new TokenExpiredError(value.rawMessage, init);
  }
  const Constructor = BY_CODE[value.code] ?? LoamsError;
  return new Constructor(value.rawMessage, init as never);
}
