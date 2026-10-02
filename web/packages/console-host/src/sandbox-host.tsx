// Third-party plugins as console pages that render their sandbox frame
// (AP1a Task 6). Loaded only when the catalog has third-party rows; the
// pages appear only when the instance sets THIRD_PARTY_FLAG.

import type { Context } from '@loams/cordis';
import { useEffect, useRef } from 'react';
import { type PluginRecord, THIRD_PARTY_FLAG } from './boot.js';
import { invokeOn } from './bridge.js';
import { mountSandboxed } from './sandbox.js';
import { type PageProps, service } from './services.js';

interface Config {
  records: PluginRecord[];
  frameUrl: string;
  resolve: (name: string) => unknown;
}

function SandboxPage({
  record,
  frameUrl,
  resolve,
}: { record: PluginRecord } & Omit<Config, 'records'>) {
  const box = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const container = box.current;
    if (!container || !record.scriptUrl || !record.manifest) return;
    const handle = mountSandboxed(container, {
      frameUrl,
      scriptUrl: record.scriptUrl,
      policy: {
        pluginId: record.id,
        version: record.manifest.version,
        services: record.manifest.inject,
        permissions: record.granted ?? [],
      },
      invoke: invokeOn(resolve),
    });
    return () => handle.dispose();
  }, [record, frameUrl, resolve]);
  return (
    <div className="loams-sandbox">
      <div className="loams-notice loams-notice-warn" role="status">
        <div>
          <strong>Unverified plugin: {record.manifest?.package}</strong>
          <p>
            It runs in an isolated frame with no network access. It can call only{' '}
            {(record.granted ?? []).join(', ') || 'nothing'} through the console.
          </p>
        </div>
      </div>
      <div ref={box} className="loams-sandbox-box" />
    </div>
  );
}

export const sandboxHostPlugin = {
  name: 'sandbox-host',
  inject: ['flags', 'router'],
  apply(ctx: Context, config: Config) {
    const flags = service(ctx, 'flags');
    if (!flags.features[THIRD_PARTY_FLAG]) return;
    const router = service(ctx, 'router');
    config.records.forEach((record, i) => {
      if (!record.scriptUrl) {
        record.reason = 'no sandbox script';
        return;
      }
      record.status = 'active';
      record.reason = undefined;
      ctx.effect(() =>
        router.page(
          {
            id: `plugin-${record.id}`,
            path: `/plugins/${record.id}`,
            title: record.manifest?.package ?? record.id,
            nav: { group: 'Plugins', order: 100 + i, label: record.id },
          },
          (_props: PageProps) => (
            <SandboxPage record={record} frameUrl={config.frameUrl} resolve={config.resolve} />
          ),
        ),
      );
    });
  },
};
