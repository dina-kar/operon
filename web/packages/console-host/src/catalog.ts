// The plugin catalog, `loams.yml` (§37 §5.3, D423, AP1a Ruling 2).
//
// The format is cordis v4's loader entry list, as the harness's `cordis.yml`
// uses it: `- id, name, config, group, disabled, inject`. A catalog is a base
// list plus patch lists per bundle and edition: a `- id: x` row in a patch
// replaces that row's `config` (and `disabled`, `inject` if given); an
// `- insert: [...]` row adds rows.
//
// **No YAML tags at all**, so no `!!js`: the harness evaluates `!!js` config
// with `new Function`, a code path through configuration that the console's
// CSP (no `unsafe-eval`) forbids anyway. Dynamic values come from services
// inside `apply`, never from the catalog.

import { isMap, isScalar, isSeq, parseDocument, visit } from 'yaml';

export interface CatalogEntry {
  /** Unique within the composed catalog: [a-z][a-z0-9-]*. */
  id: string;
  /** The plugin's package name, for example "@loams/plugin-approvals". */
  name: string;
  /** The plugin's config, validated by the plugin's own schema. */
  config?: unknown;
  /** cordis groups are reserved; the console does not nest catalogs yet. */
  group?: boolean;
  disabled?: boolean;
  /** Must be a subset of the manifest's `inject` (it can narrow, never widen). */
  inject?: string[];
}

export type CatalogPatchRow =
  | { id: string; config?: unknown; disabled?: boolean; inject?: string[] }
  | { insert: CatalogEntry[] };

export type CatalogPatch = CatalogPatchRow[];

export class CatalogError extends Error {
  override name = 'CatalogError';
}

const ENTRY_KEYS = new Set(['id', 'name', 'config', 'group', 'disabled', 'inject']);
const PATCH_KEYS = new Set(['id', 'config', 'disabled', 'inject']);
const ID = /^[a-z][a-z0-9-]*$/;

/** Parses YAML and refuses any explicit tag (`!!js`, `!!str`, `!custom`, ...). */
function load(text: string, what: string): unknown {
  const doc = parseDocument(text, { prettyErrors: false, uniqueKeys: true });
  if (doc.errors.length > 0) {
    throw new CatalogError(`${what}: ${doc.errors[0]?.message ?? 'invalid YAML'}`);
  }
  visit(doc, {
    Node(_key, node) {
      if ((isScalar(node) || isMap(node) || isSeq(node)) && node.tag) {
        throw new CatalogError(
          `${what}: YAML tags are not allowed in a Loams catalog (found ${node.tag})`,
        );
      }
    },
  });
  // Aliases are resolved to plain values; nothing executes.
  return doc.toJS({ maxAliasCount: 50 }) ?? [];
}

function entryOf(raw: unknown, where: string): CatalogEntry {
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
    throw new CatalogError(`${where}: a catalog row must be a mapping`);
  }
  const row = raw as Record<string, unknown>;
  for (const key of Object.keys(row)) {
    if (!ENTRY_KEYS.has(key)) throw new CatalogError(`${where}: unknown key "${key}"`);
  }
  if (typeof row.id !== 'string' || !ID.test(row.id)) {
    throw new CatalogError(`${where}: "id" must match ${ID}`);
  }
  if (typeof row.name !== 'string' || row.name.length === 0) {
    throw new CatalogError(`${where}: "name" must be a package name`);
  }
  const entry: CatalogEntry = { id: row.id, name: row.name };
  if ('config' in row) entry.config = row.config;
  if (row.group !== undefined) {
    if (typeof row.group !== 'boolean') throw new CatalogError(`${where}: "group" is a boolean`);
    if (row.group) throw new CatalogError(`${where}: catalog groups are not supported yet`);
    entry.group = row.group;
  }
  if (row.disabled !== undefined) {
    if (typeof row.disabled !== 'boolean') {
      throw new CatalogError(`${where}: "disabled" is a boolean`);
    }
    entry.disabled = row.disabled;
  }
  if (row.inject !== undefined) entry.inject = injectOf(row.inject, where);
  return entry;
}

function injectOf(raw: unknown, where: string): string[] {
  if (!Array.isArray(raw) || !raw.every((s) => typeof s === 'string')) {
    throw new CatalogError(`${where}: "inject" is a list of service names`);
  }
  return raw as string[];
}

/** Parses a base catalog (a list of entries). */
export function parseCatalog(text: string): CatalogEntry[] {
  const data = load(text, 'catalog');
  if (!Array.isArray(data)) throw new CatalogError('catalog: expected a list of rows');
  const entries = data.map((row, i) => entryOf(row, `catalog row ${i + 1}`));
  assertUnique(entries);
  return entries;
}

/** Parses a patch list (`- id:` replacements and `- insert:` rows). */
export function parsePatch(text: string): CatalogPatch {
  const data = load(text, 'patch');
  if (!Array.isArray(data)) throw new CatalogError('patch: expected a list of rows');
  return data.map((raw, i): CatalogPatchRow => {
    const where = `patch row ${i + 1}`;
    if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
      throw new CatalogError(`${where}: a patch row must be a mapping`);
    }
    const row = raw as Record<string, unknown>;
    if ('insert' in row) {
      if (Object.keys(row).length !== 1 || !Array.isArray(row.insert)) {
        throw new CatalogError(`${where}: "insert" takes a list of rows and nothing else`);
      }
      return { insert: row.insert.map((r, j) => entryOf(r, `${where}, insert ${j + 1}`)) };
    }
    for (const key of Object.keys(row)) {
      if (!PATCH_KEYS.has(key)) throw new CatalogError(`${where}: unknown key "${key}"`);
    }
    if (typeof row.id !== 'string') throw new CatalogError(`${where}: "id" is required`);
    const patch: CatalogPatchRow = { id: row.id };
    if ('config' in row) patch.config = row.config;
    if (row.disabled !== undefined) {
      if (typeof row.disabled !== 'boolean') {
        throw new CatalogError(`${where}: "disabled" is a boolean`);
      }
      patch.disabled = row.disabled;
    }
    if (row.inject !== undefined) patch.inject = injectOf(row.inject, where);
    return patch;
  });
}

/**
 * Applies patch lists to a base catalog in order (AP1a Ruling 2). A `- id:`
 * row replaces the whole `config` of the row it names, as the harness's
 * profiles do, so a patch restates every key it owns.
 */
export function composeCatalog(base: CatalogEntry[], ...patches: CatalogPatch[]): CatalogEntry[] {
  const rows = base.map((e) => ({ ...e }));
  for (const patch of patches) {
    for (const row of patch) {
      if ('insert' in row) {
        rows.push(...row.insert.map((e) => ({ ...e })));
        continue;
      }
      const target = rows.find((e) => e.id === row.id);
      if (!target) throw new CatalogError(`patch names an unknown row "${row.id}"`);
      if ('config' in row) target.config = row.config;
      if (row.disabled !== undefined) target.disabled = row.disabled;
      if (row.inject !== undefined) target.inject = row.inject;
    }
  }
  assertUnique(rows);
  return rows;
}

function assertUnique(entries: CatalogEntry[]): void {
  const seen = new Set<string>();
  for (const e of entries) {
    if (seen.has(e.id)) throw new CatalogError(`duplicate catalog id "${e.id}"`);
    seen.add(e.id);
  }
}
