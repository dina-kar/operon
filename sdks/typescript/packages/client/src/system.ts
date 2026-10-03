// Feature detection and the version check (design §44 §4 and §7.4; runtime
// contract R9).
//
// The unavailable-service path has two halves, and an SDK needs both:
//
// 1. **Without calling.** `GetInstance.services[]` says which packages this
//    binary carries and which it does not (D600). One call, no auth, cheap, so
//    an application asks first and hides a feature it cannot use. `loams.system`
//    wraps it: `available`, `served`, `guard`.
// 2. **When the caller calls anyway.** A call to a package the variant does not
//    carry answers `unimplemented` with `ErrorInfo.reason =
//    feature_not_in_variant` and the variant in `metadata.variant` (D600). The
//    runtime turns that into a `FeatureNotInVariantError`, so the branch is on
//    `err instanceof FeatureNotInVariantError` or on
//    `err.reason === 'feature_not_in_variant'` — never on a message, and never
//    on the package name, which is a proto detail.
//
// Half 1 is the one to use. Half 2 is the safety net for a caller who skipped
// it, or whose instance changed variant underneath a long-lived client.

import type { GetInstanceResponse, ServiceStatus } from '@loams/proto/instance';
import { MODULES, PROTO_PACKAGES, PROTO_REV } from './gen/facade.js';
import { Code } from '@connectrpc/connect';
import type { CallOptions } from './runtime/call.js';
import { FeatureNotInVariantError, LoamsError } from './runtime/errors.js';

/** `GetInstance.services[]`, sorted by what an SDK can use. */
export interface Catalogue {
  /** Every package the instance knows, served or not. */
  readonly services: readonly ServiceStatus[];
  /** The packages this binary serves. */
  readonly served: readonly string[];
  /** The packages this binary knows but does not serve, which is what makes
   * `unavailable` a plan rather than a surprise. */
  readonly unavailable: readonly string[];
}

/** What `loams.version()` reports (runtime contract R9). */
export interface VersionReport {
  /** The proto revision this SDK was generated from. */
  readonly protoRev: string;
  /** The server's own semver. */
  readonly serverVersion: string;
  /** The proto packages the server says it serves. */
  readonly apiVersions: readonly string[];
  /** The SDK's packages the server does not serve. */
  readonly missing: readonly string[];
  /** True when every package the SDK speaks is served. */
  readonly compatible: boolean;
}

/** What a `Loams` hands to its `system`. */
export interface SystemConfig {
  /** The instance's URL, for error messages and for a caller reading `endpoint`. */
  readonly endpoint: string;
  readonly maxRetries?: number;
  /** The client's deadline for the catalogue call. */
  readonly timeoutMs?: number;
}

/** The catalogue, the version check and the guard. */
export class SystemApi {
  #catalogue: Catalogue | undefined;
  #inFlight: Promise<Catalogue> | undefined;

  constructor(
    /** `loams.instance.getInstance`, which is the one RPC the system API uses.
     * Injected rather than reached for, so the system API is testable without
     * a client. */
    private readonly getInstance: (
      request: object,
      options?: CallOptions,
    ) => Promise<GetInstanceResponse>,
    private readonly config: SystemConfig,
  ) {}

  /**
   * The service catalogue, cached.
   *
   * Cached because it is asked on a cold start, when a UI is deciding which
   * features to show, and again on every feature check: the instance's services
   * do not change while a process runs. `invalidate()` drops it.
   */
  async catalogue(): Promise<Catalogue> {
    this.#catalogue ??= await this.fetch();
    return this.#catalogue;
  }

  private async fetch(): Promise<Catalogue> {
    // One in-flight fetch shared by concurrent callers, so a cold start with
    // twenty availability checks makes one call, not twenty.
    this.#inFlight ??= this.getInstance({}, this.callOptions())
      .then((info) => toCatalogue(info.services ?? []))
      .finally(() => {
        this.#inFlight = undefined;
      });
    return this.#inFlight;
  }

  /** Drops the cached catalogue. */
  invalidate(): void {
    this.#catalogue = undefined;
  }

  /** The client's retry and deadline settings, applied to the catalogue call. */
  private callOptions(): CallOptions {
    return {
      ...(this.config.maxRetries === undefined ? {} : { maxRetries: this.config.maxRetries }),
      ...(this.config.timeoutMs === undefined ? {} : { timeoutMs: this.config.timeoutMs }),
    };
  }

  /**
   * Whether this binary serves a module or a proto package.
   *
   * Takes either, so a caller holding `loams.live` can pass `'live'` and a
   * caller reading a `ServiceStatus` can pass `'loams.live.v1'`.
   */
  async available(moduleOrPackage: string): Promise<boolean> {
    const name = packageOf(moduleOrPackage);
    const { served } = await this.catalogue();
    return served.includes(name);
  }

  /** The packages this binary serves. */
  async served(): Promise<readonly string[]> {
    return (await this.catalogue()).served;
  }

  /** The packages it knows but does not serve. */
  async unavailable(): Promise<readonly string[]> {
    return (await this.catalogue()).unavailable;
  }

  /**
   * Throws `FeatureNotInVariantError` unless this binary serves the module.
   *
   * This is the guard to put in front of a feature: the error is the same
   * class the server's own refusal maps to, so one `catch` covers both "the
   * instance does not have this" and "the guard said no", and it costs no RPC
   * once the catalogue is cached.
   */
  async guard(moduleOrPackage: string): Promise<void> {
    const name = packageOf(moduleOrPackage);
    if (await this.available(name)) {
      return;
    }
    throw new FeatureNotInVariantError(
      `${name} is not in this instance's build variant`,
      {
        code: Code.Unimplemented,
        reason: 'feature_not_in_variant',
        metadata: { package: name, variant: 'standard' },
        rpc: 'loams.instance.v1.InstanceService/GetInstance',
        variant: 'standard',
      },
    );
  }

  /**
   * The proto revision check (R9).
   *
   * A missing package is a warning, not an exception: the SDK still works for
   * the modules that are there, and the caller decides what a missing one means.
   */
  async version(): Promise<VersionReport> {
    const info = await this.getInstance({}, this.callOptions());
    const apiVersions = info.apiVersions ?? [];
    const spoken = PROTO_PACKAGES.filter((name) => name.startsWith('loams.'));
    const missing = spoken.filter((name) => !apiVersions.includes(name));
    return {
      protoRev: PROTO_REV,
      serverVersion: info.serverVersion ?? '',
      apiVersions,
      missing,
      compatible: missing.length === 0,
    };
  }
}

/** The proto package behind a module name or a package name. */
function packageOf(moduleOrPackage: string): string {
  if (moduleOrPackage.startsWith('loams.')) {
    return moduleOrPackage;
  }
  const found = MODULES.find((module) => module.name === moduleOrPackage);
  if (found === undefined) {
    throw new LoamsError(`no generated module named ${moduleOrPackage}`, {
      code: Code.Internal,
    });
  }
  return found.package;
}

/** Splits `GetInstance.services[]` into served and not. */
function toCatalogue(services: readonly ServiceStatus[]): Catalogue {
  const sorted = [...services].sort((left, right) =>
    left.package.localeCompare(right.package),
  );
  return {
    services: sorted,
    served: sorted.filter((service) => service.available).map((service) => service.package),
    unavailable: sorted
      .filter((service) => !service.available)
      .map((service) => service.package),
  };
}
