// The plugin permission model (§37 §5.3, §5.6; D426).
//
// A plugin declares `permissions` in its manifest. For `first-party` plugins
// they document intent and are checked at install; their calls carry the
// user's credential. For `third-party` plugins they are enforced twice: the
// host-side bridge refuses any call whose method needs a permission the
// plugin was not granted, and (after the auth plan) the server enforces the
// vended token whose scopes are the manifest's permissions intersected with
// the user's rights. Client-side checks only shorten the path to a refusal.

/** Every permission a manifest may declare. */
export const PERMISSIONS = [
  // §19 §5.1 actions.
  'collections:read',
  'collections:write',
  'query',
  'documents:delete',
  'streams:produce',
  'mcp:tools',
  'durable:invoke',
  'durable:resolve',
  // §37 §5.3.
  'jobs:read',
  'jobs:admin',
  'approvals:decide',
  'devices:manage',
  'connectors:read',
  'connectors:write',
  // Proposed with the AP1a scaffold (reads of the AP0 packages).
  'instance:read',
  'approvals:read',
  'operations:read',
  'operations:cancel',
  'notifications:read',
  // Console-only, granted through a visible host prompt.
  'ui:notifications',
  'ui:clipboard-write',
] as const;

export type Permission = (typeof PERMISSIONS)[number];

const KNOWN = new Set<string>(PERMISSIONS);

export function isPermission(value: string): value is Permission {
  return KNOWN.has(value);
}

/**
 * The permission each bridged method needs, by `<service>.<method>`. A
 * method that is not listed cannot be called through the bridge at all.
 */
export const METHOD_PERMISSIONS: Readonly<Record<string, Permission>> = {
  'rpc.instance.getInstance': 'instance:read',
  'rpc.instance.whoAmI': 'instance:read',
  'rpc.approvals.listApprovals': 'approvals:read',
  'rpc.approvals.getApproval': 'approvals:read',
  'rpc.approvals.decideApproval': 'approvals:decide',
  'rpc.operations.getOperation': 'operations:read',
  'rpc.operations.listOperations': 'operations:read',
  'rpc.operations.cancelOperation': 'operations:cancel',
  'rpc.devices.listDevices': 'devices:manage',
  'rpc.devices.createPairing': 'devices:manage',
  'rpc.devices.revokeDevice': 'devices:manage',
  'rpc.notifications.listNotifications': 'notifications:read',
  'rpc.notifications.markRead': 'notifications:read',
  'platform.notify': 'ui:notifications',
  'platform.clipboardWrite': 'ui:clipboard-write',
};

/** Services a third-party plugin may never reach, whatever it declares. */
export const NEVER_BRIDGED = new Set(['platform.stacks', 'platform.auth', 'session', 'transport']);

export type Decision = { ok: true; permission: Permission } | { ok: false; reason: string };

/**
 * Decides one bridged call: the service must be in the plugin's `inject`
 * list, the method must have a known permission, and the plugin must hold
 * that permission (the manifest's, intersected with what the user granted).
 */
export function decideCall(
  policy: { services: readonly string[]; permissions: readonly string[] },
  service: string,
  method: string,
): Decision {
  if (NEVER_BRIDGED.has(service) || [...NEVER_BRIDGED].some((s) => service.startsWith(`${s}.`))) {
    return { ok: false, reason: `${service} is never available to sandboxed plugins` };
  }
  if (!policy.services.includes(service)) {
    return { ok: false, reason: `${service} is not in the plugin's inject list` };
  }
  const key = `${service}.${method}`;
  // Own keys only, and the method must be one name (no `.`), so
  // `rpc` + `approvals.listApprovals` cannot borrow another service's entry.
  const permission =
    !method.includes('.') && Object.hasOwn(METHOD_PERMISSIONS, key)
      ? METHOD_PERMISSIONS[key]
      : undefined;
  if (!permission) return { ok: false, reason: `${service}.${method} cannot be bridged` };
  if (!policy.permissions.includes(permission)) {
    return { ok: false, reason: `${service}.${method} needs ${permission}` };
  }
  return { ok: true, permission };
}
