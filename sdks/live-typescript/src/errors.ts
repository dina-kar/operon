// Errors of the Live API (R1 plan Task 14, rows T7-5 and T13-18).
import { Code, ConnectError } from "@connectrpc/connect";

import {
  ErrorCode,
  LiveErrorSchema,
  type LiveError as WireError,
} from "./gen/loam/live/v1/live_pb.js";

/** The wire codes of `loam.live.v1.ErrorCode`, without their prefix. */
export type LiveErrorCode =
  | "UNSPECIFIED"
  | "INVALID_ARGUMENT"
  | "NOT_FOUND"
  | "FAILED_PRECONDITION"
  | "RESOURCE_EXHAUSTED"
  | "FUNCTION_ERROR"
  | "FUNCTION_TIMEOUT"
  | "FUNCTION_OUT_OF_MEMORY"
  | "UNAVAILABLE"
  | "INTERNAL"
  | "CANCELED";

/** An error of a Live call or of one query in a session. */
export class LiveError extends Error {
  override readonly name = "LiveError";
  readonly code: LiveErrorCode;
  /** Whether the same call may succeed later (`UNAVAILABLE`). */
  readonly retryable: boolean;

  constructor(code: LiveErrorCode, message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.code = code;
    this.retryable = code === "UNAVAILABLE";
  }
}

const WIRE: Record<ErrorCode, LiveErrorCode> = {
  [ErrorCode.UNSPECIFIED]: "UNSPECIFIED",
  [ErrorCode.INVALID_ARGUMENT]: "INVALID_ARGUMENT",
  [ErrorCode.NOT_FOUND]: "NOT_FOUND",
  [ErrorCode.FAILED_PRECONDITION]: "FAILED_PRECONDITION",
  [ErrorCode.RESOURCE_EXHAUSTED]: "RESOURCE_EXHAUSTED",
  [ErrorCode.FUNCTION_ERROR]: "FUNCTION_ERROR",
  [ErrorCode.FUNCTION_TIMEOUT]: "FUNCTION_TIMEOUT",
  [ErrorCode.FUNCTION_OUT_OF_MEMORY]: "FUNCTION_OUT_OF_MEMORY",
  [ErrorCode.UNAVAILABLE]: "UNAVAILABLE",
  [ErrorCode.INTERNAL]: "INTERNAL",
};

/** A query's error as a Transition carries it. */
export function fromWireError(e: WireError): LiveError {
  return new LiveError(WIRE[e.code] ?? "UNSPECIFIED", e.message);
}

// Row T13-18: the server maps its codes onto Connect's; the reverse is exact
// except that RESOURCE_EXHAUSTED also carries FUNCTION_OUT_OF_MEMORY. Servers
// attach the exact code as a `loam.live.v1.LiveError` detail (row T14-12), so
// this mapping is the fallback for errors without one.
const CONNECT: Partial<Record<Code, LiveErrorCode>> = {
  [Code.Canceled]: "CANCELED",
  [Code.InvalidArgument]: "INVALID_ARGUMENT",
  [Code.NotFound]: "NOT_FOUND",
  [Code.FailedPrecondition]: "FAILED_PRECONDITION",
  [Code.ResourceExhausted]: "RESOURCE_EXHAUSTED",
  [Code.Unknown]: "FUNCTION_ERROR",
  [Code.DeadlineExceeded]: "FUNCTION_TIMEOUT",
  [Code.Unavailable]: "UNAVAILABLE",
  [Code.Internal]: "INTERNAL",
};

/** Any error of a Connect call as a `LiveError`. */
export function toLiveError(e: unknown): LiveError {
  if (e instanceof LiveError) return e;
  const c = ConnectError.from(e);
  // The first detail with a code this client knows; an unusable one before it
  // must not hide it.
  for (const detail of c.findDetails(LiveErrorSchema)) {
    const code = detail.code === ErrorCode.UNSPECIFIED ? undefined : WIRE[detail.code];
    if (code !== undefined) return new LiveError(code, c.rawMessage, { cause: e });
  }
  let code = CONNECT[c.code] ?? "INTERNAL";
  if (code === "RESOURCE_EXHAUSTED" && /out of memory/i.test(c.rawMessage)) {
    code = "FUNCTION_OUT_OF_MEMORY";
  }
  return new LiveError(code, c.rawMessage, { cause: e });
}
