// SDK2 Task 0's `typescript_error_reason_mapping`.
//
// Design §44 §7.4, D611: a failed RPC carries a Connect code and one
// `loams.errors.v1.ErrorInfo`, and **`reason` is what callers branch on**. The
// message is for a person and may change.
//
// What this pins:
//
// - every reason in the registry reaches the SDK as a class, not as a string
//   comparison, and each one is raised under the code the registry says;
// - the class comes from the code, so a caller can also branch on the coarse
//   category (`NotFoundError` for `not_found`);
// - a reason from a *newer* server, which this SDK's registry does not have,
//   is surfaced rather than dropped — that is what `unknownReason` is for;
// - a failure from below the API (a socket, a CORS rejection, an abort) is a
//   `LoamsError` with no reason at all, which is a different thing from a
//   Loams service refusing.
//
// The corpus in `sdks/fixtures` covers the two reasons a `loams dev` produces
// (`not_implemented` and `feature_not_in_variant`); this covers all twenty-five.

import { Code, ConnectError } from '@connectrpc/connect';
import { create, toBinary } from '@bufbuild/protobuf';
import { ErrorInfoSchema } from '@loams/proto/errors';
import { describe, expect, it } from 'vitest';
import { REASONS, REASON_CODES } from '../src/gen/facade.js';
import {
  AlreadyExistsError,
  DeadlineExceededError,
  FeatureNotInVariantError,
  FailedPreconditionError,
  InternalError,
  InvalidArgumentError,
  LoamsError,
  NotFoundError,
  PermissionDeniedError,
  ResourceExhaustedError,
  TokenExpiredError,
  UnauthenticatedError,
  UnavailableError,
  UnimplementedError,
  isLoamsError,
  toLoamsError,
} from '../src/runtime/errors.js';

/** The code each Connect code maps to, for the assertions below. */
const CLASS_OF: Partial<Record<Code, new (...args: never[]) => LoamsError>> = {
  [Code.InvalidArgument]: InvalidArgumentError,
  [Code.NotFound]: NotFoundError,
  [Code.AlreadyExists]: AlreadyExistsError,
  [Code.PermissionDenied]: PermissionDeniedError,
  [Code.Unauthenticated]: UnauthenticatedError,
  [Code.FailedPrecondition]: FailedPreconditionError,
  [Code.ResourceExhausted]: ResourceExhaustedError,
  [Code.Unavailable]: UnavailableError,
  [Code.DeadlineExceeded]: DeadlineExceededError,
  [Code.Unimplemented]: UnimplementedError,
  [Code.Internal]: InternalError,
};

/** A `ConnectError` shaped the way the server shapes one: the code, the
 * message, and the `ErrorInfo` in its details, exactly as connect-rust sends
 * it over the wire (API1 Task 1). */
function refuse(
  reason: string,
  code: Code,
  metadata: Record<string, string> = {},
  hint = '',
): ConnectError {
  const info = create(ErrorInfoSchema, { reason, metadata, hint });
  const error = new ConnectError(`loams.test.v1 refused: ${reason}`, code);
  // The Connect JSON error body carries the bare message name in `type`, which
  // is what connect-es matches a schema against; the gRPC path carries a full
  // `google.rpc.Status` in the trailers instead.
  error.details.push({
    type: ErrorInfoSchema.typeName,
    value: toBinary(ErrorInfoSchema, info),
  });
  return error;
}

describe('typescript_error_reason_mapping', () => {
  it('turns every reason in the registry into its typed class', () => {
    // Every row is exercised, so a reason added to `docs/api/reasons.md`
    // without a code in the SDK cannot pass unnoticed.
    expect(REASONS.length).toBeGreaterThanOrEqual(25);
    for (const reason of REASONS) {
      const code = REASON_CODES[reason] as unknown as Code;
      const error = toLoamsError(refuse(reason, code), 'loams.test.v1.ThingService/Read');
      expect(isLoamsError(error), reason).toBe(true);
      expect(error.reason, reason).toBe(reason);
      expect(error.code, reason).toBe(code);
      expect(error.rpc, reason).toBe('loams.test.v1.ThingService/Read');
      const Expected = CLASS_OF[code] ?? LoamsError;
      expect(error, reason).toBeInstanceOf(Expected);
    }
  });

  it('reads the variant out of the metadata for the unavailable-service path', () => {
    const error = toLoamsError(
      refuse('feature_not_in_variant', Code.Unimplemented, { variant: 'full', package: 'loams.live.v1' }),
      'loams.live.v1.LiveService/Query',
    );
    expect(error).toBeInstanceOf(FeatureNotInVariantError);
    expect(error).toBeInstanceOf(UnimplementedError);
    expect(error.reason).toBe('feature_not_in_variant');
    expect((error as FeatureNotInVariantError).variant).toBe('full');
    expect(error.metadata.package).toBe('loams.live.v1');
    // The branch a caller writes is on the reason and the typed class; the
    // message is whatever prose the server sent and is not asserted on.
  });

  it('separates an expired token from any other unauthenticated failure', () => {
    const expired = toLoamsError(refuse('token_expired', Code.Unauthenticated));
    expect(expired).toBeInstanceOf(TokenExpiredError);
    expect(expired).toBeInstanceOf(UnauthenticatedError);

    const refused = toLoamsError(refuse('permission_denied', Code.Unauthenticated));
    expect(refused).toBeInstanceOf(UnauthenticatedError);
    expect(refused).not.toBeInstanceOf(TokenExpiredError);
  });

  it('surfaces a reason this SDK does not know instead of dropping it', () => {
    // A server newer than the SDK can raise a reason this registry has not
    // heard of. Losing it would leave a caller unable to tell "not supported
    // here" from "not supported at all", so it is carried as text and flagged.
    const error = toLoamsError(refuse('quota_exceeded_for_this_tenant', Code.ResourceExhausted));
    expect(error.reason).toBeUndefined();
    expect(error.unknownReason).toBe('quota_exceeded_for_this_tenant');
    expect(error.code).toBe(Code.ResourceExhausted);
  });

  it('treats a failure from below the API as reasonless', () => {
    // A socket reset, a CORS rejection, a DNS failure: nothing a Loams service
    // said, so there is no reason to report.
    const error = toLoamsError(new TypeError('fetch failed'), 'loams.instance.v1.InstanceService/GetInstance');
    expect(error).toBeInstanceOf(LoamsError);
    expect(error.code).toBe(Code.Unknown);
    expect(error.reason).toBeUndefined();
    expect(error.unknownReason).toBeUndefined();
    expect(error.rpc).toBe('loams.instance.v1.InstanceService/GetInstance');
    expect(error.cause).toBeInstanceOf(TypeError);

    // A raw `ConnectError` with no detail at all is the same shape.
    const bare = toLoamsError(new ConnectError('connection refused', Code.Unavailable));
    expect(bare).toBeInstanceOf(UnavailableError);
    expect(bare.reason).toBeUndefined();
  });

  it('keeps the hint, which is the server’s next step in the caller’s locale', () => {
    const error = toLoamsError(
      refuse('step_up_required', Code.Unauthenticated, {}, 'Re-authenticate to approve this.'),
    );
    expect(error.hint).toBe('Re-authenticate to approve this.');
  });

  it('is idempotent: mapping a mapped error returns it unchanged', () => {
    const once = toLoamsError(refuse('not_found', Code.NotFound));
    expect(toLoamsError(once)).toBe(once);
  });
});
