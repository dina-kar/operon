// @loams/plugin-stack-status: the console's home page. What this instance
// is (GetInstance), which app APIs it serves, which shell the console runs
// in, and the `environment.overview.card` slot other plugins fill.

import {
  type FlagsService,
  type PlatformService,
  type PluginModule,
  service,
} from '@loams/console-host';
import type { Context } from '@loams/cordis';
import { APP_API_PACKAGES } from '@loams/proto';
import { Slot } from '@loams/slots';
import { Badge, Card, Notice, StatusTag } from '@loams/ui';

export function StatusPage({
  flags,
  platform,
}: {
  flags: FlagsService;
  platform: PlatformService;
}) {
  const reachable = flags.edition !== 'unknown';
  return (
    <div className="lc-page">
      <header className="lc-page-head">
        <h1>{flags.instanceName || 'Loams'}</h1>
        <p>
          {reachable ? (
            <>
              <Badge>{flags.edition}</Badge> <Badge>v{flags.serverVersion}</Badge>{' '}
              <Badge>{platform.kind === 'desktop' ? 'Loams Desktop' : 'browser'}</Badge>
            </>
          ) : null}
        </p>
      </header>
      {!reachable && (
        <Notice tone="danger" title="This instance did not answer">
          GetInstance failed at {platform.baseUrl}. Is it running? For development, start the mock
          with <code>cargo run -p loams-apps-mock</code>.
        </Notice>
      )}
      <Card title="App APIs" flush>
        <ul className="lc-api-list">
          {APP_API_PACKAGES.map((api) => (
            <li key={api}>
              <code>{api}</code>{' '}
              {flags.has(api) ? (
                <StatusTag status="done">served</StatusTag>
              ) : (
                <StatusTag status="planned">not served</StatusTag>
              )}
            </li>
          ))}
        </ul>
      </Card>
      <div className="lc-cards">
        <Slot name="environment.overview.card" props={{}} />
      </div>
    </div>
  );
}

const plugin: PluginModule = {
  name: 'stack-status',
  inject: ['flags', 'router', 'platform'],
  apply(ctx: Context) {
    const flags = service(ctx, 'flags');
    const platform = service(ctx, 'platform');
    const router = service(ctx, 'router');
    ctx.effect(() =>
      router.page(
        {
          id: 'status',
          path: '/',
          title: 'Status',
          plugin: 'stack-status',
          nav: { group: 'Instance', order: 0 },
        },
        () => <StatusPage flags={flags} platform={platform} />,
      ),
    );
  },
};

export default plugin;
