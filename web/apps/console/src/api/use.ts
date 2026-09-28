import { type DependencyList, useCallback, useEffect, useRef, useState } from 'react';
import { message } from './client';

export type Loaded<T> = {
  data?: T;
  error?: string;
  loading: boolean;
  reload: () => void;
};

type Result<T> = { data?: T; error?: unknown; response: Response };

/** Runs an openapi-fetch call (or any promise) and tracks its state. */
export function useLoad<T>(load: () => Promise<Result<T> | T>, deps: DependencyList): Loaded<T> {
  const [state, setState] = useState<{ data?: T; error?: string; loading: boolean }>({
    loading: true,
  });
  const [tick, setTick] = useState(0);
  const lastDeps = useRef<DependencyList | undefined>(undefined);
  // biome-ignore lint/correctness/useExhaustiveDependencies: the caller lists the deps
  useEffect(() => {
    let live = true;
    // A reload keeps what is on screen; new deps (another project or
    // environment) must not show the previous resource while loading.
    const prev = lastDeps.current;
    const same =
      prev !== undefined &&
      prev.length === deps.length &&
      prev.every((d, i) => Object.is(d, deps[i]));
    lastDeps.current = deps;
    setState((s) => (same ? { ...s, loading: true } : { loading: true }));
    Promise.resolve(load())
      .then((r) => {
        if (!live) return;
        if (r && typeof r === 'object' && 'response' in r) {
          const res = r as Result<T>;
          if (res.error !== undefined || !res.response.ok)
            setState({ error: message(res.error), loading: false });
          else setState({ data: res.data, loading: false });
        } else setState({ data: r as T, loading: false });
      })
      .catch((e: unknown) => live && setState({ error: message(e), loading: false }));
    return () => {
      live = false;
    };
  }, [...deps, tick]);
  const reload = useCallback(() => setTick((t) => t + 1), []);
  return { ...state, reload };
}

/** Re-renders every `ms`, for countdowns. */
export function useNow(ms = 1000): number {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), ms);
    return () => clearInterval(t);
  }, [ms]);
  return now;
}
