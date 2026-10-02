// Starts the cordis console with a platform plugin and optional catalog
// patches. The browser entry (main.tsx) and Loams Desktop
// (web/apps/desktop) both call this; they differ in the platform plugin and
// the desktop patch (§37 §5.8: editions are plugin sets).

import {
  boot,
  type CatalogPatch,
  type ConsoleHandle,
  composeCatalog,
  type Permission,
  type PluginManifest,
  type PluginModule,
  parseCatalog,
} from '@loams/console-host';
import baseCatalog from '../../catalog/base.yml?raw';
import { manifests, modules, sandboxScripts } from './modules.js';

export interface StartOptions {
  platform: PluginModule;
  root: HTMLElement;
  /** Edition or bundle patches applied to base.yml in order. */
  patches?: CatalogPatch[];
  /** Extra bundled plugins (the desktop's), by package name. */
  extraModules?: typeof modules;
  extraManifests?: unknown[];
  /** Where the console's assets live, for example "/ui/" or "/". */
  base?: string;
  /** The permissions granted to a sandboxed plugin; default: none. */
  grant?: (manifest: PluginManifest) => Permission[];
}

export function startConsole(options: StartOptions): Promise<ConsoleHandle> {
  const base = options.base ?? import.meta.env.BASE_URL;
  return boot({
    catalog: composeCatalog(parseCatalog(baseCatalog), ...(options.patches ?? [])),
    manifests: [...manifests, ...(options.extraManifests ?? [])],
    modules: { ...modules, ...options.extraModules },
    platform: options.platform,
    sandboxScripts: sandboxScripts(base),
    frameUrl: `${base}sandbox/frame.html`,
    // Nothing is granted to a sandboxed plugin by default: the install
    // screen (AP1a Task 7) asks the user. Only the demo grants what a
    // plugin declares, so the sample can show the bridge.
    grant: options.grant ?? (() => []),
    root: options.root,
  });
}
