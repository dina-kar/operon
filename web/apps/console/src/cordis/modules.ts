// The bundled plugins: code by package name, and every manifest the catalog
// may name (first-party and third-party). Vite splits each dynamic import
// into its own chunk. Per-plugin ESM bundles with an import map and SRI
// replace this table in AP1a Task 2 without changing the host's API.

import type { ModuleTable } from '@loams/console-host';
import helloPkg from '@loams/example-plugin-hello/package.json';
import approvalsPkg from '@loams/plugin-approvals/package.json';
import identityPkg from '@loams/plugin-identity/package.json';
import namespacesPkg from '@loams/plugin-namespaces/package.json';
import rpcPkg from '@loams/plugin-rpc/package.json';
import shellPkg from '@loams/plugin-shell/package.json';
import stackStatusPkg from '@loams/plugin-stack-status/package.json';

export const modules: ModuleTable = {
  '@loams/plugin-shell': () => import('@loams/plugin-shell'),
  '@loams/plugin-rpc': () => import('@loams/plugin-rpc'),
  '@loams/plugin-identity': () => import('@loams/plugin-identity'),
  '@loams/plugin-stack-status': () => import('@loams/plugin-stack-status'),
  '@loams/plugin-namespaces': () => import('@loams/plugin-namespaces'),
  '@loams/plugin-approvals': () => import('@loams/plugin-approvals'),
};

export const manifests: unknown[] = [
  shellPkg,
  rpcPkg,
  identityPkg,
  stackStatusPkg,
  namespacesPkg,
  approvalsPkg,
  helloPkg,
];

/** Third-party scripts, served beside the console (see vite.config.ts). */
export function sandboxScripts(base: string): Record<string, string> {
  return { '@loams/example-plugin-hello': `${base}plugins/hello/client.js` };
}
