import type { SlotComponent, SlotEntry, SlotKind, SlotName, SlotSpec } from './types.js';

/**
 * Each slot's kind. A slot outside this table cannot be registered into;
 * `extend` adds plugin-declared slots (with their `SlotMap` merge).
 */
export const SLOT_KINDS: Record<string, SlotKind> = {
  root: 'single',
  'console.nav': 'list',
  'console.page': 'keyed',
  'console.settings.section': 'list',
  'environment.overview.card': 'list',
  'approval.renderer': 'keyed',
  'shell.overlay': 'list',
};

/**
 * The `slots` service: where plugins register UI, and what the shell renders.
 *
 * `register` returns its disposer, so a plugin calls it inside `ctx.effect`
 * and disposing the plugin removes the entry (Review Focus 2).
 */
export class SlotRegistry {
  #entries: SlotEntry[] = [];
  #listeners = new Set<() => void>();
  #next = 1;
  #version = 0;
  readonly #kinds: Record<string, SlotKind>;

  constructor(kinds: Record<string, SlotKind> = SLOT_KINDS) {
    this.#kinds = { ...kinds };
  }

  /** Declares a plugin-defined slot. */
  extend(name: string, kind: SlotKind): void {
    const known = this.#kinds[name];
    if (known && known !== kind) {
      throw new Error(`slot ${name} is already declared as ${known}`);
    }
    this.#kinds[name] = kind;
  }

  register<N extends SlotName>(spec: SlotSpec<N>, component: SlotComponent<N>): () => void {
    const kind = this.#kinds[spec.name];
    if (!kind) throw new Error(`unknown slot ${String(spec.name)}`);
    if (kind === 'keyed' && !spec.key) {
      throw new Error(`slot ${String(spec.name)} is keyed: a key is required`);
    }
    if (kind === 'single' && this.#entries.some((e) => e.name === spec.name)) {
      throw new Error(`slot ${String(spec.name)} is single and already taken`);
    }
    const entry = {
      ...spec,
      id: this.#next++,
      order: spec.order ?? 0,
      component,
    } as SlotEntry;
    this.#entries = [...this.#entries, entry];
    this.#changed();
    return () => {
      const before = this.#entries.length;
      this.#entries = this.#entries.filter((e) => e.id !== entry.id);
      if (this.#entries.length !== before) this.#changed();
    };
  }

  /** The entries of a slot in render order; for a keyed slot, one key's. */
  entries<N extends SlotName>(name: N, key?: string): SlotEntry<N>[] {
    return (this.#entries as SlotEntry<N>[])
      .filter((e) => e.name === name && (key === undefined || e.key === key))
      .sort((a, b) => a.order - b.order || a.id - b.id);
  }

  /** Every key registered in a keyed slot. */
  keys(name: SlotName): string[] {
    return [...new Set(this.entries(name).flatMap((e) => (e.key ? [e.key] : [])))];
  }

  /** Every entry registered by one plugin (the diagnostics page). */
  byPlugin(plugin: string): SlotEntry[] {
    return this.#entries.filter((e) => e.plugin === plugin);
  }

  /** For `useSyncExternalStore`: bumps on every change. */
  readonly getVersion = (): number => this.#version;

  readonly subscribe = (listener: () => void): (() => void) => {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  };

  #changed(): void {
    this.#version++;
    for (const listener of this.#listeners) listener();
  }
}
