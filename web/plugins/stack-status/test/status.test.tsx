import type { FlagsService, PlatformService } from '@loams/console-host';
import { SlotProvider, SlotRegistry } from '@loams/slots';
import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import { StatusPage } from '../src/index.js';

afterEach(cleanup);

const platform = { kind: 'web', baseUrl: 'http://127.0.0.1:8084' } as PlatformService;

function flags(apiVersions: string[], edition: FlagsService['edition'] = 'oss'): FlagsService {
  return {
    edition,
    instanceName: 'Loams (mock)',
    serverVersion: '0.0.1',
    features: {},
    apiVersions,
    has: (api) => apiVersions.includes(api),
  };
}

describe('StatusPage', () => {
  it('shows which app APIs the instance serves', () => {
    render(
      <SlotProvider registry={new SlotRegistry()}>
        <StatusPage
          flags={flags(['loams.instance.v1', 'loams.approvals.v1'])}
          platform={platform}
        />
      </SlotProvider>,
    );
    expect(screen.getByRole('heading', { name: 'Loams (mock)' })).toBeTruthy();
    expect(screen.getAllByText('served')).toHaveLength(2);
    expect(screen.getAllByText('not served')).toHaveLength(3);
  });

  it('says so when the instance does not answer', () => {
    render(
      <SlotProvider registry={new SlotRegistry()}>
        <StatusPage flags={flags([], 'unknown')} platform={platform} />
      </SlotProvider>,
    );
    expect(screen.getByText('This instance did not answer')).toBeTruthy();
  });
});
