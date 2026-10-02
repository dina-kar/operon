// @loams/plugin-stacks: the desktop's local stack (§37 §6.2, AP1 Task 3).
//
// Scaffold: the bundled `loams` sidecar the desktop supervises itself
// (start, stop, restart, the log tail) and the environment switcher. When
// CLI1's `loams stack … --output json` lands, this page lists every stack
// under LOAMS_HOME through the CLI; `stack create` and `stack delete` stay
// commands to copy into a terminal (§30 D287).

import { type PluginModule, service } from '@loams/console-host';
import type { Context } from '@loams/cordis';
import type { EnvView, SidecarStatus, StacksService } from '@loams/platform-tauri';
import { Badge, Button, Card, Notice, Snippet, StatusTag } from '@loams/ui';
import { useCallback, useEffect, useState } from 'react';

const TONE: Record<SidecarStatus['state'], 'done' | 'progress' | 'failed' | 'neutral' | 'planned'> =
  {
    running: 'done',
    starting: 'progress',
    restarting: 'progress',
    crashed: 'failed',
    stopped: 'neutral',
    unavailable: 'planned',
  };

export function StacksPage({ stacks }: { stacks: StacksService }) {
  const [status, setStatus] = useState<SidecarStatus>();
  const [envs, setEnvs] = useState<EnvView[]>([]);
  const [error, setError] = useState<string>();
  const [busy, setBusy] = useState(false);
  const refresh = useCallback(async () => {
    try {
      setStatus(await stacks.status());
      setEnvs(await stacks.environments());
    } catch (e) {
      setError(String(e));
    }
  }, [stacks]);
  useEffect(() => {
    refresh();
    const timer = setInterval(refresh, 5000);
    let unlisten: (() => void) | undefined;
    stacks.onChange(refresh).then((u) => {
      unlisten = u;
    });
    return () => {
      clearInterval(timer);
      unlisten?.();
    };
  }, [stacks, refresh]);
  const act = async (fn: () => Promise<SidecarStatus>) => {
    setBusy(true);
    setError(undefined);
    try {
      setStatus(await fn());
      setEnvs(await stacks.environments());
    } catch (e) {
      setError(String(e));
      await refresh();
    } finally {
      setBusy(false);
    }
  };
  const state = status?.state ?? 'stopped';
  const unavailable = state === 'unavailable';
  return (
    <div className="lc-page">
      <header className="lc-page-head">
        <h1>This computer</h1>
        <p>The loams server Loams Desktop runs on 127.0.0.1</p>
      </header>
      {error && (
        <Notice tone="danger" title="That did not work">
          {error}
        </Notice>
      )}
      <Card title="Local stack" actions={<StatusTag status={TONE[state]}>{state}</StatusTag>}>
        {unavailable ? (
          <p>{status?.reason}</p>
        ) : (
          <>
            <p className="lc-muted">
              {status?.url ? (
                <>
                  Serving at <Badge>{status.url}</Badge>
                  {status.pid ? <> · pid {status.pid}</> : null}
                </>
              ) : status?.reason ? (
                status.reason
              ) : (
                'Not running.'
              )}
            </p>
            <div className="lc-actions">
              <Button
                variant="primary"
                disabled={busy || state === 'running'}
                onClick={() => act(stacks.start)}
              >
                Start
              </Button>
              <Button disabled={busy || state !== 'running'} onClick={() => act(stacks.restart)}>
                Restart
              </Button>
              <Button disabled={busy || state === 'stopped'} onClick={() => act(stacks.stop)}>
                Stop
              </Button>
            </div>
          </>
        )}
        {status && status.log_tail.length > 0 && (
          <figure className="lc-log">
            <figcaption>Log tail</figcaption>
            <pre>{status.log_tail.join('\n')}</pre>
          </figure>
        )}
      </Card>
      <Card title="Environments" flush>
        <ul className="lc-api-list">
          {envs.map((env) => (
            <li key={env.id}>
              <strong>{env.name}</strong> <Badge>{env.kind}</Badge>{' '}
              {env.base_url ? (
                <Badge>{env.base_url}</Badge>
              ) : (
                <span className="lc-muted">no address yet</span>
              )}{' '}
              {env.active ? (
                <StatusTag status="done">active</StatusTag>
              ) : (
                <Button
                  size="sm"
                  variant="quiet"
                  disabled={!env.base_url}
                  onClick={() => stacks.select(env.id)}
                >
                  Use
                </Button>
              )}{' '}
              {env.kind === 'remote' && !env.signed_in && (
                <Button
                  size="sm"
                  onClick={() => stacks.signIn(env.id).catch((e) => setError(String(e)))}
                >
                  Sign in
                </Button>
              )}
            </li>
          ))}
        </ul>
      </Card>
      <Card title="Stacks from the terminal">
        <p>
          Creating and deleting stacks stays in a terminal (they can download variants and touch
          disks):
        </p>
        <Snippet>loams stack create dev</Snippet>
      </Card>
    </div>
  );
}

const plugin: PluginModule = {
  name: 'stacks',
  inject: ['platform.stacks', 'router'],
  apply(ctx: Context) {
    const stacks = service(ctx, 'platform.stacks');
    const router = service(ctx, 'router');
    ctx.effect(() =>
      router.page(
        {
          id: 'stacks',
          path: '/stacks',
          title: 'This computer',
          plugin: 'stacks',
          nav: { group: 'Instance', order: 5, label: 'This computer' },
        },
        () => <StacksPage stacks={stacks} />,
      ),
    );
  },
};

export default plugin;
