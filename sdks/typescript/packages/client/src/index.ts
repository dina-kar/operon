// @loams/client: the Loams SDK for TypeScript and JavaScript.
//
//   import { Loams, apiKey } from '@loams/client';
//
//   const loams = new Loams({ endpoint: 'https://acme.loams.dev', auth: apiKey(key) });
//   const info = await loams.instance.getInstance({});
//   await loams.system.guard('live');            // no RPC once cached
//   for await (const t of loams.live.watch(request, { resume })) {}
//
// Design §44 §7: one client object with namespaced modules, generated from the
// `loams.options.v1` annotations on the protos, over the unified Connect API
// on one port. `@loams/client/node` adds the HTTP/2 gRPC transport.
//
// What a caller imports from here: the `Loams` client, the module types the
// generator produced, the typed error hierarchy, the token sources, and the
// reason registry as a type.

export { Loams, type LoamsModules, type LoamsOptions } from './loams.js';
export {
  SystemApi,
  type Catalogue,
  type SystemConfig,
  type VersionReport,
} from './system.js';

// The generated facade: the module interfaces, the binding table, the proto
// revision, and the reason registry as a type. Everything here is written by
// `buf generate`; do not hand-edit it.
export {
  FEATURE_NOT_IN_VARIANT,
  MODULES,
  PROTO_PACKAGES,
  PROTO_REV,
  REASON_CODES,
  REASONS,
  SERVICE_DESCRIPTORS,
  type CallBinding,
  type InstanceModule,
  type LiveModule,
  type ModuleBinding,
  type Reason,
  type TablesModule,
} from './gen/facade.js';

// The runtime, for a caller who composes rather than uses.
export {
  CallInvoker,
  callWithRetry,
  withIdempotencyKey,
  type Attempt,
  type CallOptions,
  type ConsistencyOptions,
  type ConsistencyTokenStore,
  type RetryPlan,
  type Send,
} from './runtime/call.js';
export { ConsistencySession, TOKEN_PREFIX, isConsistencyToken } from './runtime/consistency.js';
export {
  AlreadyExistsError,
  AbortedError,
  DeadlineExceededError,
  FailedPreconditionError,
  FeatureNotInVariantError,
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
  errorInfo,
  isLoamsError,
  toLoamsError,
  type ErrorInfoShape,
} from './runtime/errors.js';
export { paginate, pageFields, type PageFetcher, type PageRequestOptions } from './runtime/pagination.js';
export {
  BASE_DELAY_MS,
  DEFAULT_MAX_RETRIES,
  MAX_DELAY_MS,
  MAX_SERVER_DELAY_MS,
  backoffMs,
  isRetryableCode,
  shouldRetry,
  sleep,
} from './runtime/retry.js';
export { watch, type CursorOf, type ResumeOptions, type StreamOpener } from './runtime/streams.js';
export {
  apiKey,
  envToken,
  oidcExchange,
  refreshing,
  staticToken,
  type TokenSource,
} from './runtime/token-source.js';
export { createLoamsTransport, type TransportOptions } from './runtime/transports.js';
export { uuidv7, uuidv7Time } from './runtime/uuidv7.js';
