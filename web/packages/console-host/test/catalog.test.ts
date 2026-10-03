import { describe, expect, it } from 'vitest';
import {
  CatalogError,
  composeCatalog,
  ManifestError,
  parseCatalog,
  parsePatch,
  tierOf,
  validateManifest,
} from '../src/index.js';

const BASE = `
- id: shell
  name: '@loams/plugin-shell'
- id: jobs
  name: '@loams/plugin-jobs'
  config: { pageSize: 50 }
`;

describe('catalog', () => {
  it('parses the cordis v4 entry list', () => {
    expect(parseCatalog(BASE)).toEqual([
      { id: 'shell', name: '@loams/plugin-shell' },
      { id: 'jobs', name: '@loams/plugin-jobs', config: { pageSize: 50 } },
    ]);
  });

  it('catalog_with_js_tag_is_refused', () => {
    const evil = `
- id: jobs
  name: '@loams/plugin-jobs'
  config:
    pageSize: !!js/function "() => fetch('https://evil.example')"
`;
    expect(() => parseCatalog(evil)).toThrow(CatalogError);
    expect(() => parseCatalog(evil)).toThrow(/tags are not allowed/);
    expect(() => parseCatalog('- id: a\n  name: !custom b\n')).toThrow(/tags/);
    expect(() => parsePatch("- id: a\n  config: !!js 'x'\n")).toThrow(/tags/);
  });

  it('refuses unknown keys, bad ids, duplicates and groups', () => {
    expect(() => parseCatalog('- id: a\n  name: b\n  url: x\n')).toThrow(/unknown key "url"/);
    expect(() => parseCatalog('- id: Bad\n  name: b\n')).toThrow(/"id"/);
    expect(() => parseCatalog('- id: a\n  name: b\n- id: a\n  name: c\n')).toThrow(/duplicate/);
    expect(() => parseCatalog('- id: a\n  name: b\n  group: true\n')).toThrow(/groups/);
  });

  it('compose_patch_replaces_config', () => {
    const patch = parsePatch('- id: jobs\n  config: { pageSize: 10 }\n  disabled: true\n');
    const composed = composeCatalog(parseCatalog(BASE), patch);
    expect(composed[1]).toEqual({
      id: 'jobs',
      name: '@loams/plugin-jobs',
      config: { pageSize: 10 },
      disabled: true,
    });
  });

  it('compose_insert_adds_rows', () => {
    const patch = parsePatch(`
- insert:
    - id: stacks
      name: '@loams/plugin-stacks'
      inject: [platform.stacks]
`);
    const composed = composeCatalog(parseCatalog(BASE), patch);
    expect(composed.map((e) => e.id)).toEqual(['shell', 'jobs', 'stacks']);
    expect(composed[2]?.inject).toEqual(['platform.stacks']);
  });

  it('a patch naming an unknown row fails', () => {
    expect(() => composeCatalog(parseCatalog(BASE), parsePatch('- id: nope\n'))).toThrow(
      /unknown row/,
    );
  });
});

function pkg(plugin: Record<string, unknown>, name = '@loams/plugin-jobs') {
  return { name, version: '0.1.0', loams: { plugin: { kind: 'console', entry: '.', ...plugin } } };
}

describe('manifest', () => {
  it('validates the §37 §5.3 block', () => {
    const m = validateManifest(
      pkg({
        tier: 'first-party',
        inject: ['rpc.jobs', 'router'],
        provides: ['jobs.queues'],
        permissions: ['jobs:read'],
        requires: { console: '^1.0.0', api: ['loams.jobs.v1'] },
        editions: ['oss'],
      }),
    );
    expect(m.inject).toEqual(['rpc.jobs', 'router']);
    expect(m.requires.api).toEqual(['loams.jobs.v1']);
  });

  it('manifest_schema_rejects_unknown_tier, permissions and keys', () => {
    expect(() => validateManifest(pkg({ tier: 'trusted' }))).toThrow(ManifestError);
    expect(() => validateManifest(pkg({ tier: 'first-party', permissions: ['root'] }))).toThrow(
      /unknown permission/,
    );
    expect(() => validateManifest(pkg({ tier: 'first-party', eval: true }))).toThrow(
      /unknown manifest key/,
    );
  });

  it('non-core plugins provide only inside their namespace', () => {
    expect(() => validateManifest(pkg({ tier: 'first-party', provides: ['session'] }))).toThrow(
      /namespace/,
    );
    expect(
      validateManifest(pkg({ tier: 'core', provides: ['session'] }, '@loams/plugin-identity'))
        .provides,
    ).toEqual(['session']);
  });
});

describe('tiers', () => {
  const m = (name: string, tier: 'core' | 'first-party' | 'third-party' = 'core') => ({
    package: name,
    tier,
  });

  it('manifest_cannot_claim_first_party', () => {
    expect(tierOf(m('@evil/plugin', 'core'), { bundled: false })).toBe('third-party');
    expect(tierOf(m('@loams/plugin-jobs', 'core'), { bundled: true })).toBe('first-party');
    expect(tierOf(m('@loams/plugin-shell', 'core'), { bundled: true })).toBe('core');
  });

  it('trusts provenance from trusted publishers only', () => {
    expect(
      tierOf(m('@loams/plugin-x', 'first-party'), {
        bundled: false,
        provenanceRepo: 'github.com/ostrium-labs/loams',
      }),
    ).toBe('first-party');
    expect(
      tierOf(m('@acme/plugin', 'first-party'), {
        bundled: false,
        provenanceRepo: 'github.com/acme/console-plugins',
      }),
    ).toBe('third-party');
    expect(
      tierOf(
        m('@acme/plugin', 'first-party'),
        { bundled: false, provenanceRepo: 'github.com/acme/console-plugins' },
        ['github.com/acme/*'],
      ),
    ).toBe('first-party');
  });

  it('a manifest may claim a lower tier', () => {
    expect(tierOf(m('@loams/plugin-jobs', 'third-party'), { bundled: true })).toBe('third-party');
  });
});
