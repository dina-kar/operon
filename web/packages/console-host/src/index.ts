// @loams/console-host: the console's host (§37 §5.2–§5.6, D422–D426).

export {
  type BootOptions,
  boot,
  type ConsoleHandle,
  flagsPlugin,
  type ModuleTable,
  type PendingReport,
  type PluginModule,
  type PluginRecord,
  type PluginStatus,
  THIRD_PARTY_FLAG,
} from './boot.js';
export {
  type Bridge,
  type BridgePolicy,
  createBridge,
  type FrameMessage,
  type HostMessage,
  invokeOn,
} from './bridge.js';
export {
  type CatalogEntry,
  CatalogError,
  type CatalogPatch,
  composeCatalog,
  parseCatalog,
  parsePatch,
} from './catalog.js';
export { GuardError, guard } from './guard.js';
export {
  type Edition,
  ManifestError,
  type PluginManifest,
  pluginId,
  type Tier,
  validateManifest,
} from './manifest.js';
export {
  decideCall,
  isPermission,
  METHOD_PERMISSIONS,
  NEVER_BRIDGED,
  PERMISSIONS,
  type Permission,
} from './permissions.js';
export {
  isLocalScript,
  mountSandboxed,
  SANDBOX_CSP,
  SANDBOX_FLAGS,
  type SandboxHandle,
  sandboxScriptId,
} from './sandbox.js';
export {
  type FlagsService,
  type Location,
  type PageProps,
  type PageSpec,
  type PlatformService,
  type RouterService,
  type RpcServices,
  type ServiceName,
  type Services,
  type SessionService,
  service,
  watch,
} from './services.js';
export { CORE_PLUGINS, DEFAULT_TRUSTED_PUBLISHERS, type PluginSource, tierOf } from './tiers.js';

/** The console host's semver; plugins declare `requires.console` against it. */
export const CONSOLE_VERSION = '0.1.0';
