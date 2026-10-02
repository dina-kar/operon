// @loams/plugin-rpc: one cordis service per app proto service (§37 §5.4).
//
// `rpc.<name>` is provided only when GetInstance.api_versions lists its
// package (AP1a Ruling 6), so a page that injects `rpc.jobs` appears by
// itself on instances that serve loams.jobs.v1 and stays silently pending
// elsewhere. Replacing `transport` (switching environments) reloads this
// plugin and so every `rpc.*` dependent.

import type { DescService } from '@bufbuild/protobuf';
import { createClient, type Transport } from '@connectrpc/connect';
import { type PluginModule, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
import { approvals, devices, instance, notifications, operations } from '@loams/proto';

/** The services this plugin provides, by cordis service name. */
export const RPC_SERVICES: readonly { name: string; api: string; service: DescService }[] = [
  { name: 'rpc.instance', api: 'loams.instance.v1', service: instance.InstanceService },
  { name: 'rpc.approvals', api: 'loams.approvals.v1', service: approvals.ApprovalService },
  { name: 'rpc.operations', api: 'loams.operations.v1', service: operations.OperationsService },
  { name: 'rpc.devices', api: 'loams.devices.v1', service: devices.DeviceService },
  {
    name: 'rpc.notifications',
    api: 'loams.notifications.v1',
    service: notifications.NotificationService,
  },
];

const plugin: PluginModule = {
  name: 'rpc',
  inject: ['transport', 'flags'],
  apply(ctx: Context) {
    const transport = service(ctx, 'transport') as Transport;
    const flags = service(ctx, 'flags');
    for (const entry of RPC_SERVICES) {
      // GetInstance needs no auth and is how flags were read, so
      // rpc.instance is always there.
      if (entry.name !== 'rpc.instance' && !flags.has(entry.api)) continue;
      ctx.provide(entry.name, createClient(entry.service, transport));
    }
  },
};

export default plugin;
