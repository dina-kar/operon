// Booting the console (§37 §5.2, AP1a Task 1).
//
// The host is the only code that is not a plugin. It creates a cordis
// Context, provides `slots`, loads the platform plugin (`platform` and
// `transport`), reads GetInstance into `flags`, and loads every catalog row
// as a fiber: core plugins with the raw context, first-party plugins behind
// the guard proxy, third-party plugins as sandboxed frames (only when the
// instance allows them). Then it renders the `root` slot.
//
// Scaffold note (AP1a Rulings E1–E3): bundled plugins come from a module
// table of dynamic imports that Vite code-splits; per-plugin ESM bundles with
// an import map and SRI (Task 2) replace the table without changing this
// API.

import { createClient, type Transport } from '@connectrpc/connect';
import { Context, type Fiber, FiberStates } from '@loams/cordis';
import { instance } from '@loams/proto';
import { SlotRegistry } from '@loams/slots';
import { type CatalogEntry, CatalogError } from './catalog.js';
import { guard } from './guard.js';
import { type PluginManifest, type Tier, validateManifest } from './manifest.js';
import type { Permission } from './permissions.js';
import type { FlagsService } from './services.js';
import { type PluginSource, tierOf } from './tiers.js';

/** What a plugin module exports (its default export, or the module itself). */
export interface PluginModule<C = unknown> {
  name?: string;
  inject?: string[];
  apply(ctx: Context, config: C): unknown;
}

export type ModuleTable = Record<string, () => Promise<PluginModule | { default: PluginModule }>>;

export type PluginStatus = 'active' | 'pending' | 'loading' | 'failed' | 'disabled' | 'skipped';

export interface PluginRecord {
  entry: CatalogEntry;
  id: string;
  manifest?: PluginManifest;
  tier?: Tier;
  status: PluginStatus;
  reason?: string;
  fiber?: Fiber;
  /** Third-party only: the script the sandbox frame loads. */
  scriptUrl?: string;
  /** Third-party only: the permissions the bridge enforces. */
  granted?: Permission[];
}

export interface PendingReport {
  id: string;
  /** Injected services that are not available. */
  waitingFor: string[];
  /** Pending only on `rpc.*` services the instance does not serve (Ruling 6). */
  silent: boolean;
}

export interface BootOptions {
  catalog: CatalogEntry[];
  /** The package.json of every plugin the catalog may name. */
  manifests: unknown[];
  /** Bundled plugin code, by package name. */
  modules: ModuleTable;
  /** Provides `platform` and `transport` (@loams/platform-web, or another shell's). */
  platform: PluginModule;
  /** Where each package came from; default: bundled if in `modules`. */
  sources?: Record<string, PluginSource>;
  /** Third-party scripts by package name (paths on the console's origin). */
  sandboxScripts?: Record<string, string>;
  /** The sandbox frame document; default "sandbox/frame.html" beside the page. */
  frameUrl?: string;
  /** Provenance repositories counted as first-party (D428). */
  trustedPublishers?: string[];
  /** The permissions the user grants a third-party plugin; default: none. */
  grant?: (manifest: PluginManifest) => Permission[];
  /** Renders the `root` slot into this element. */
  root?: HTMLElement;
}

export interface ConsoleHandle {
  ctx: Context;
  slots: SlotRegistry;
  plugins(): PluginRecord[];
  /** Plugins still waiting for services (the all-fibers sweep). */
  pending(): PendingReport[];
  disable(id: string): Promise<void>;
  enable(id: string): Promise<void>;
  dispose(): Promise<void>;
}

/** The instance feature that turns sandboxed third-party plugins on (Ruling 9). */
export const THIRD_PARTY_FLAG = 'console.third_party_plugins';

function unwrap(mod: PluginModule | { default: PluginModule }): PluginModule | undefined {
  if (!mod || typeof mod !== 'object') return undefined;
  return 'apply' in mod ? mod : mod.default;
}

/** Reads GetInstance into the `flags` service (a core plugin). */
export const flagsPlugin: PluginModule = {
  name: 'flags',
  inject: ['transport'],
  async apply(ctx) {
    const transport = (ctx as unknown as { transport: Transport }).transport;
    const client = createClient(instance.InstanceService, transport);
    let flags: FlagsService;
    try {
      const info = await client.getInstance({});
      const editions = { 1: 'oss', 2: 'cloud', 3: 'byoc' } as const;
      flags = {
        edition: editions[info.edition as 1 | 2 | 3] ?? 'unknown',
        instanceName: info.name,
        serverVersion: info.serverVersion,
        features: { ...info.features },
        apiVersions: [...info.apiVersions],
        has: (api) => info.apiVersions.includes(api),
      };
    } catch (error) {
      console.warn('loams console: GetInstance failed; no API is available', error);
      flags = {
        edition: 'unknown',
        instanceName: '',
        serverVersion: '',
        features: {},
        apiVersions: [],
        has: () => false,
      };
    }
    ctx.provide('flags', flags);
  },
};

export async function boot(options: BootOptions): Promise<ConsoleHandle> {
  const ctx = new Context();
  const slots = new SlotRegistry();
  const records = new Map<string, PluginRecord>();
  const manifests = new Map<string, PluginManifest>();
  for (const pkg of options.manifests) {
    const manifest = validateManifest(pkg);
    manifests.set(manifest.package, manifest);
  }

  await ctx.plugin({
    name: 'slots',
    apply(c: Context) {
      c.provide('slots', slots);
    },
  });
  await ctx.plugin({ name: options.platform.name ?? 'platform', apply: options.platform.apply });
  ctx.plugin({ name: 'flags', inject: flagsPlugin.inject, apply: flagsPlugin.apply });

  const thirdParty: PluginRecord[] = [];

  const load = async (record: PluginRecord) => {
    const { entry, manifest, tier } = record;
    if (!manifest || !tier) return;
    const loader = options.modules[entry.name];
    if (!loader) {
      record.status = 'failed';
      record.reason = `no bundled module for ${entry.name}`;
      return;
    }
    let mod: PluginModule | undefined;
    try {
      mod = unwrap(await loader());
    } catch (error) {
      // A chunk that fails to load or throws at evaluation fails its row only.
      record.status = 'failed';
      record.reason = `could not load ${entry.name}: ${error instanceof Error ? error.message : String(error)}`;
      return;
    }
    if (typeof mod?.apply !== 'function') {
      record.status = 'failed';
      record.reason = `${entry.name} exports no plugin (no apply)`;
      return;
    }
    const plugin = mod;
    const inject = entry.inject ?? manifest.inject;
    record.status = 'loading';
    record.fiber = ctx.plugin(
      {
        name: entry.id,
        inject,
        apply: (c: Context, config: unknown) =>
          plugin.apply(tier === 'core' ? c : guard(c, inject, record.id), config),
      },
      entry.config ?? {},
    );
  };

  for (const entry of options.catalog) {
    const manifest = manifests.get(entry.name);
    const record: PluginRecord = { entry, id: entry.id, manifest, status: 'pending' };
    records.set(entry.id, record);
    if (!manifest) {
      record.status = 'failed';
      record.reason = `no manifest for ${entry.name}`;
      continue;
    }
    const source = options.sources?.[entry.name] ?? {
      bundled: entry.name in options.modules,
    };
    record.tier = tierOf(manifest, source, options.trustedPublishers);
    const widened = (entry.inject ?? []).filter((s) => !manifest.inject.includes(s));
    if (widened.length > 0) {
      record.status = 'failed';
      record.reason = new CatalogError(
        `catalog row ${entry.id} injects ${widened.join(', ')} beyond its manifest`,
      ).message;
      continue;
    }
    if (entry.disabled) {
      record.status = 'disabled';
      continue;
    }
    if (record.tier === 'third-party') {
      record.scriptUrl = options.sandboxScripts?.[entry.name];
      record.granted = (options.grant?.(manifest) ?? []).filter((p) =>
        manifest.permissions.includes(p),
      );
      record.status = 'skipped';
      record.reason = `third-party plugins need the "${THIRD_PARTY_FLAG}" instance feature`;
      thirdParty.push(record);
      continue;
    }
    await load(record);
  }

  if (thirdParty.length > 0) {
    const { sandboxHostPlugin } = await import('./sandbox-host.js');
    ctx.plugin(sandboxHostPlugin, {
      records: thirdParty,
      frameUrl: options.frameUrl ?? 'sandbox/frame.html',
      resolve: (name: string) => ctx.get(name, true),
    });
  }

  let reactRoot: { unmount(): void } | undefined;
  if (options.root) {
    const { renderRoot } = await import('./root.js');
    reactRoot = renderRoot(options.root, slots, () => handle);
  }

  const statusOf = (record: PluginRecord): PluginStatus => {
    const state = record.fiber?.state;
    if (state === undefined) return record.status;
    if (state === FiberStates.active) return 'active';
    if (state === FiberStates.failed) return 'failed';
    if (state === FiberStates.disposed) return 'disabled';
    if (state === FiberStates.loading) return 'loading';
    return 'pending';
  };

  const handle: ConsoleHandle = {
    ctx,
    slots,
    plugins: () => [...records.values()].map((r) => ({ ...r, status: statusOf(r) })),
    pending: () =>
      [...records.values()]
        .filter((r) => statusOf(r) === 'pending')
        .map((r) => {
          const inject = r.entry.inject ?? r.manifest?.inject ?? [];
          const waitingFor = inject.filter((name) => ctx.get(name, true) === undefined);
          return {
            id: r.id,
            waitingFor,
            silent: waitingFor.length > 0 && waitingFor.every((n) => n.startsWith('rpc.')),
          };
        }),
    async disable(id) {
      const record = records.get(id);
      if (!record?.fiber) return;
      await record.fiber.dispose();
      record.fiber = undefined;
      record.status = 'disabled';
    },
    async enable(id) {
      const record = records.get(id);
      // Only a disabled row can be enabled: a row that failed a boot-time
      // check (for example an inject list wider than its manifest) stays off.
      if (
        !record ||
        record.fiber ||
        record.status !== 'disabled' ||
        record.tier === 'third-party'
      ) {
        return;
      }
      record.entry = { ...record.entry, disabled: false };
      await load(record);
    },
    async dispose() {
      reactRoot?.unmount();
      for (const record of [...records.values()].reverse()) {
        await record.fiber?.dispose();
      }
      for (const runtime of [...ctx.registry.values()]) {
        for (const fiber of [...runtime.fibers]) await fiber.dispose();
      }
    },
  };
  return handle;
}
