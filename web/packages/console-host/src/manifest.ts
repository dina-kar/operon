// A plugin's manifest: the `loams.plugin` block of its package.json
// (§37 §5.3, D423, AP1a Ruling 3). The JSON Schema of the block is
// `plugin-manifest.schema.json` beside this package; this validator is the
// one the host and the build run, and the tests keep the two in step.

import { isPermission, type Permission } from './permissions.js';
import { CORE_PLUGINS } from './tiers.js';

export type Tier = 'core' | 'first-party' | 'third-party';
export type Edition = 'oss' | 'desktop' | 'cloud';

export interface PluginManifest {
  /** The package name. */
  package: string;
  version: string;
  kind: 'console';
  /** The ESM export that holds the plugin, for example "./client". */
  entry: string;
  /** What the package claims; the host decides the real tier (`tierOf`). */
  tier: Tier;
  inject: string[];
  provides: string[];
  slots: string[];
  permissions: Permission[];
  requires: { console: string; api: string[] };
  editions: Edition[];
  config?: string;
  server?: { kind: 'function' | 'connector'; ref: string } | null;
}

export class ManifestError extends Error {
  override name = 'ManifestError';
}

const KEYS = new Set([
  'kind',
  'entry',
  'tier',
  'inject',
  'provides',
  'slots',
  'permissions',
  'requires',
  'editions',
  'config',
  'server',
]);
const TIERS = new Set<string>(['core', 'first-party', 'third-party']);
const EDITIONS = new Set<string>(['oss', 'desktop', 'cloud']);

function strings(value: unknown, field: string, pkg: string): string[] {
  if (value === undefined) return [];
  if (!Array.isArray(value) || !value.every((v) => typeof v === 'string')) {
    throw new ManifestError(`${pkg}: "${field}" must be a list of strings`);
  }
  return value as string[];
}

/** The id a package's services and events are namespaced under. */
export function pluginId(pkg: string): string {
  return pkg.replace(/^@loams\/(plugin-|platform-|example-plugin-)?/, '').replace(/^@[^/]+\//, '');
}

/** Validates `package.json`'s `loams.plugin` block. */
export function validateManifest(pkg: unknown): PluginManifest {
  if (typeof pkg !== 'object' || pkg === null) throw new ManifestError('not a package.json');
  const p = pkg as Record<string, unknown>;
  const name = typeof p.name === 'string' ? p.name : '';
  if (!name) throw new ManifestError('package.json has no name');
  const loams = p.loams as Record<string, unknown> | undefined;
  const block = loams?.plugin as Record<string, unknown> | undefined;
  if (typeof block !== 'object' || block === null) {
    throw new ManifestError(`${name}: no "loams.plugin" block`);
  }
  for (const key of Object.keys(block)) {
    if (!KEYS.has(key)) throw new ManifestError(`${name}: unknown manifest key "${key}"`);
  }
  if (block.kind !== 'console') throw new ManifestError(`${name}: "kind" must be "console"`);
  if (typeof block.entry !== 'string') throw new ManifestError(`${name}: "entry" is required`);
  if (typeof block.tier !== 'string' || !TIERS.has(block.tier)) {
    throw new ManifestError(`${name}: unknown tier "${String(block.tier)}"`);
  }
  const tier = block.tier as Tier;
  const id = pluginId(name);
  const provides = strings(block.provides, 'provides', name);
  // The claimed tier is untrusted: only the host's own core packages may
  // provide services outside their namespace.
  if (tier !== 'core' || !CORE_PLUGINS.has(name)) {
    for (const service of provides) {
      if (!service.startsWith(`${id}.`)) {
        throw new ManifestError(
          `${name}: provides "${service}" outside its namespace "${id}.*" (only core plugins may)`,
        );
      }
    }
  }
  const permissions = strings(block.permissions, 'permissions', name);
  for (const permission of permissions) {
    if (!isPermission(permission)) {
      throw new ManifestError(`${name}: unknown permission "${permission}"`);
    }
  }
  const editions = strings(block.editions, 'editions', name);
  for (const edition of editions) {
    if (!EDITIONS.has(edition)) throw new ManifestError(`${name}: unknown edition "${edition}"`);
  }
  const requires = (block.requires ?? {}) as Record<string, unknown>;
  if (typeof requires !== 'object' || requires === null || Array.isArray(requires)) {
    throw new ManifestError(`${name}: "requires" must be an object`);
  }
  for (const key of Object.keys(requires)) {
    if (key !== 'console' && key !== 'api') {
      throw new ManifestError(`${name}: unknown "requires" key "${key}"`);
    }
  }
  if (requires.console !== undefined && typeof requires.console !== 'string') {
    throw new ManifestError(`${name}: "requires.console" must be a semver range`);
  }
  const api = strings(requires.api, 'requires.api', name);
  for (const pkg of api) {
    if (!/^loams\.[a-z]+\.v[0-9]+$/.test(pkg)) {
      throw new ManifestError(
        `${name}: "requires.api" entry "${pkg}" is not a loams.<area>.v<N> package`,
      );
    }
  }
  const server = block.server ?? null;
  if (server !== null) {
    const ok =
      typeof server === 'object' &&
      !Array.isArray(server) &&
      Object.keys(server).every((k) => k === 'kind' || k === 'ref') &&
      ((server as Record<string, unknown>).kind === 'function' ||
        (server as Record<string, unknown>).kind === 'connector') &&
      typeof (server as Record<string, unknown>).ref === 'string';
    if (!ok) {
      throw new ManifestError(
        `${name}: "server" must be null or {kind: "function" | "connector", ref}`,
      );
    }
  }
  return {
    package: name,
    version: typeof p.version === 'string' ? p.version : '0.0.0',
    kind: 'console',
    entry: block.entry,
    tier,
    inject: strings(block.inject, 'inject', name),
    provides,
    slots: strings(block.slots, 'slots', name),
    permissions: permissions as Permission[],
    requires: {
      console: typeof requires.console === 'string' ? requires.console : '*',
      api,
    },
    editions: editions as Edition[],
    ...(typeof block.config === 'string' ? { config: block.config } : {}),
    server: server as PluginManifest['server'],
  };
}
