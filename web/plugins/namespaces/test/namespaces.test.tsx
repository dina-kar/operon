import { create } from '@bufbuild/protobuf';
import type { SessionService } from '@loams/console-host';
import { instance } from '@loams/proto';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import { NamespacesPage } from '../src/index.js';

afterEach(cleanup);

describe('NamespacesPage', () => {
  it('lists environments with their namespaces and switches the selection', () => {
    // A fake session (plugins never import each other's values).
    const listeners = new Set<() => void>();
    const environments = [
      { id: 'env_dev', name: 'development', namespace: 'search-dev', protected: false },
      { id: 'env_prod', name: 'production', namespace: 'search-prod', protected: true },
    ];
    let selected = 'env_dev';
    const session: SessionService = {
      principal: () => create(instance.PrincipalSchema, { id: 'usr_omar', displayName: 'Omar' }),
      environments: () => environments,
      environment: () => environments.find((e) => e.id === selected),
      select: (id) => {
        selected = id;
        for (const l of listeners) l();
      },
      authenticatedAt: () => undefined,
      subscribe: (l) => {
        listeners.add(l);
        return () => listeners.delete(l);
      },
    };
    render(<NamespacesPage session={session} />);
    expect(screen.getByText('Signed in as Omar')).toBeTruthy();
    expect(screen.getByText('search-prod')).toBeTruthy();
    expect(screen.getByText('protected')).toBeTruthy();
    fireEvent.click(screen.getByRole('button', { name: 'Select' }));
    expect(session.environment()?.id).toBe('env_prod');
  });
});
