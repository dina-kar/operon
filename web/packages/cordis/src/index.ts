// @loams/cordis: the console's only import of cordis (AP1a Ruling 1).
//
// cordis (MIT, cordiverse/cordis) is pinned exactly at 4.0.0-rc.10: its API
// is a release candidate with one maintainer (§37 §14 risk 1). Every other
// package imports from here, so a breaking rc bump, a patch or vendoring the
// library is a change to this package alone. Patches are logged in
// PATCHES.md.
//
// The surface the console uses is small: Context, plugins with `inject`,
// services through `ctx.provide`, `ctx.effect` and `ctx.on` for disposable
// side effects, and fibers. `@cordisjs/plugin-loader` is not used: it imports
// `node:module`, `node:fs` and `node:path` and evaluates catalog expressions
// with `new Function`, which the console's CSP forbids; @loams/console-host
// loads the same entry-list format itself (§37 §14 risk 2's fallback).

export { Context, type Fiber, type Plugin } from 'cordis';

/**
 * cordis's `FiberState` values. cordis declares them as an ambient `const
 * enum`, which `isolatedModules` cannot read, so the numbers are restated
 * here (checked by @loams/console-host's tests).
 */
export const FiberStates = {
  pending: 0,
  loading: 1,
  active: 2,
  failed: 3,
  disposed: 4,
  unloading: 5,
} as const;

/** The cordis version this facade pins; shown on the diagnostics page. */
export const CORDIS_VERSION = '4.0.0-rc.10';
