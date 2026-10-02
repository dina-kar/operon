import { create } from '@bufbuild/protobuf';
import { timestampFromDate } from '@bufbuild/protobuf/wkt';
import { instance } from '@loams/proto';
import { describe, expect, it } from 'vitest';
import { createSession } from '../src/index.js';

describe('createSession', () => {
  it('selects the first environment and switches on request', () => {
    const at = new Date('2026-10-02T09:00:00Z');
    const session = createSession(
      create(instance.WhoAmIResponseSchema, {
        principal: { id: 'usr_omar', displayName: 'Omar' },
        environments: [
          { id: 'env_dev', name: 'development', namespace: 'search-dev' },
          { id: 'env_prod', name: 'production', namespace: 'search-prod', protected: true },
        ],
        authenticatedAt: timestampFromDate(at),
      }),
    );
    let changes = 0;
    session.subscribe(() => changes++);
    expect(session.principal()?.id).toBe('usr_omar');
    expect(session.environment()?.id).toBe('env_dev');
    session.select('env_prod');
    expect(session.environment()).toEqual({
      id: 'env_prod',
      name: 'production',
      namespace: 'search-prod',
      protected: true,
    });
    expect(changes).toBe(1);
    expect(session.authenticatedAt()?.toISOString()).toBe(at.toISOString());
    expect(() => session.select('env_nope')).toThrow(/unknown environment/);
  });

  it('is empty when signed out', () => {
    const session = createSession(undefined);
    expect(session.principal()).toBeUndefined();
    expect(session.environments()).toEqual([]);
  });
});
