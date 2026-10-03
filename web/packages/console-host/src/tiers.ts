// Trust tiers are decided by the host, never claimed by a manifest
// (§37 §5.6, AP1a Ruling 8).

import type { PluginManifest, Tier } from './manifest.js';

/** The host's own plugins: the only `core` tier. */
export const CORE_PLUGINS = new Set([
  '@loams/plugin-shell',
  '@loams/plugin-rpc',
  '@loams/plugin-identity',
  '@loams/platform-web',
]);

/** Always trusted for `first-party`, in every build. */
export const DEFAULT_TRUSTED_PUBLISHERS = ['github.com/ostrium-labs/*'];

/** Where a package came from, as the build or the instance recorded it. */
export interface PluginSource {
  /** Built into this console bundle. */
  bundled: boolean;
  /** The npm provenance attestation's source repository, if verified. */
  provenanceRepo?: string;
}

function matches(glob: string, repo: string): boolean {
  if (glob.endsWith('/*')) return repo.startsWith(glob.slice(0, -1));
  return glob === repo;
}

/**
 * `core` = bundled and in CORE_PLUGINS; `first-party` = bundled, or with
 * provenance from a trusted publisher (a build may add publishers, which is
 * how a hosted build's private plugins count as first-party without this
 * repository knowing them, D428); anything else `third-party`. A manifest
 * may claim a lower tier than it would get, never a higher one.
 */
export function tierOf(
  manifest: Pick<PluginManifest, 'package' | 'tier'>,
  source: PluginSource,
  trustedPublishers: readonly string[] = DEFAULT_TRUSTED_PUBLISHERS,
): Tier {
  let earned: Tier = 'third-party';
  if (source.bundled && CORE_PLUGINS.has(manifest.package)) earned = 'core';
  else if (source.bundled) earned = 'first-party';
  else if (
    source.provenanceRepo &&
    [...DEFAULT_TRUSTED_PUBLISHERS, ...trustedPublishers].some((g) =>
      matches(g, source.provenanceRepo as string),
    )
  ) {
    earned = 'first-party';
  }
  const rank: Record<Tier, number> = { core: 0, 'first-party': 1, 'third-party': 2 };
  return rank[manifest.tier] > rank[earned] ? manifest.tier : earned;
}
