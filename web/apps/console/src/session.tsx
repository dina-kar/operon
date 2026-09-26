import { createContext, type ReactNode, useContext, useEffect, useState } from 'react';
import { Navigate, useLocation } from 'react-router';
import { api, type Schemas, setCsrfToken } from './api/client';

type Ctx = {
  instance: Schemas['Instance'];
  session: Schemas['Session'];
  projects: Schemas['Project'][];
  signOut: () => Promise<void>;
};

const SessionContext = createContext<Ctx | null>(null);

export function useSession(): Ctx {
  const ctx = useContext(SessionContext);
  if (!ctx) throw new Error('useSession outside <SessionGate>');
  return ctx;
}

type State =
  | { kind: 'loading' }
  | { kind: 'setup' }
  | { kind: 'signed-out' }
  | { kind: 'error'; message: string }
  | {
      kind: 'ready';
      instance: Schemas['Instance'];
      session: Schemas['Session'];
      projects: Schemas['Project'][];
    };

/**
 * Loads the instance and the session before any console page renders. A
 * fresh install goes to setup, a signed-out visitor to sign-in.
 */
export function SessionGate({ children }: { children: ReactNode }) {
  const [state, setState] = useState<State>({ kind: 'loading' });
  const location = useLocation();

  useEffect(() => {
    let live = true;
    (async () => {
      const instance = await api.GET('/api/v1/instance');
      if (!instance.data) throw new Error('The console API did not answer /api/v1/instance.');
      if (instance.data.setup_required) return setState({ kind: 'setup' });
      const session = await api.GET('/api/v1/session');
      if (session.response.status === 401 || !session.data) return setState({ kind: 'signed-out' });
      setCsrfToken(session.data.csrf_token);
      const projects = await api.GET('/api/v1/projects');
      if (live)
        setState({
          kind: 'ready',
          instance: instance.data,
          session: session.data,
          projects: projects.data?.projects ?? [],
        });
    })().catch(
      (e: unknown) =>
        live && setState({ kind: 'error', message: e instanceof Error ? e.message : String(e) }),
    );
    return () => {
      live = false;
    };
  }, []);

  if (state.kind === 'loading') return <div className="boot" aria-busy="true" />;
  if (state.kind === 'setup') return <Navigate to="/setup" replace />;
  if (state.kind === 'signed-out')
    return <Navigate to={`/sign-in?next=${encodeURIComponent(location.pathname)}`} replace />;
  if (state.kind === 'error')
    return (
      <div className="boot-error">
        <h1>The console can't reach its API</h1>
        <p>{state.message}</p>
        <p>
          In development, start the mock with <code>cargo run -p operon-console-mock</code>.
        </p>
      </div>
    );

  const signOut = async () => {
    await api.DELETE('/api/v1/session');
    setCsrfToken(undefined);
    setState({ kind: 'signed-out' });
  };
  return (
    <SessionContext.Provider
      value={{
        instance: state.instance,
        session: state.session,
        projects: state.projects,
        signOut,
      }}
    >
      {children}
    </SessionContext.Provider>
  );
}
