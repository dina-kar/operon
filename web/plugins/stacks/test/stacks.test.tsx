import type { EnvView, SidecarStatus, StacksService } from '@loams/platform-tauri';
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import { StacksPage } from '../src/index.js';

afterEach(cleanup);

function fake(initial: SidecarStatus, envs: EnvView[] = []) {
  let status = initial;
  const calls: string[] = [];
  const stacks: StacksService = {
    status: async () => status,
    start: async () => {
      calls.push('start');
      status = { state: 'running', url: 'http://127.0.0.1:49152/', pid: 42, log_tail: ['serving'] };
      return status;
    },
    stop: async () => {
      calls.push('stop');
      status = { state: 'stopped', log_tail: [] };
      return status;
    },
    restart: async () => status,
    environments: async () => envs,
    select: async (id) => {
      calls.push(`select ${id}`);
    },
    signIn: async () => {},
    onChange: async () => () => {},
  };
  return { stacks, calls };
}

describe('StacksPage', () => {
  it('renders_running_and_crashed', async () => {
    const { stacks } = fake({
      state: 'crashed',
      reason: 'loams exited before it was ready (Some(1))',
      log_tail: ['boom'],
    });
    render(<StacksPage stacks={stacks} />);
    await screen.findByText('crashed');
    expect(screen.getByText(/exited before it was ready/)).toBeTruthy();
    expect(screen.getByText('Log tail').closest('figure')?.textContent).toContain('boom');
  });

  it('starts the sidecar and switches environments', async () => {
    const env = (id: string, active: boolean): EnvView => ({
      id,
      name: id,
      kind: 'local',
      base_url: 'http://127.0.0.1:8084/',
      active,
      signed_in: false,
    });
    const { stacks, calls } = fake({ state: 'stopped', log_tail: [] }, [
      env('local', true),
      env('apps-mock', false),
    ]);
    render(<StacksPage stacks={stacks} />);
    fireEvent.click(await screen.findByRole('button', { name: 'Start' }));
    await screen.findByText('running');
    expect(screen.getByText('http://127.0.0.1:49152/')).toBeTruthy();
    fireEvent.click(screen.getByRole('button', { name: 'Use' }));
    await waitFor(() => expect(calls).toEqual(['start', 'select apps-mock']));
  });

  it('says why there is no sidecar on this platform', async () => {
    const { stacks } = fake({
      state: 'unavailable',
      reason: 'Loams Desktop on Windows is remote-only',
      log_tail: [],
    });
    render(<StacksPage stacks={stacks} />);
    await screen.findByText(/remote-only/);
    expect(screen.queryByRole('button', { name: 'Start' })).toBeNull();
  });
});
