import { Notice } from '@loam/ui';
import { createContext, type ReactNode, useContext, useEffect, useState } from 'react';

const Crumb = createContext<{ tail?: string; setTail: (t: string | undefined) => void }>({
  setTail: () => {},
});

export function CrumbProvider({ children }: { children: ReactNode }) {
  const [tail, setTail] = useState<string>();
  return <Crumb.Provider value={{ tail, setTail }}>{children}</Crumb.Provider>;
}

export function useCrumbTail(): string | undefined {
  return useContext(Crumb).tail;
}

/** Sets the tab title and, for detail pages, the last breadcrumb. */
export function usePageTitle(title: string | undefined, crumb?: string) {
  const { setTail } = useContext(Crumb);
  useEffect(() => {
    if (title) document.title = `${title} · Loam console`;
    setTail(crumb);
    return () => setTail(undefined);
  }, [title, crumb, setTail]);
}

export function PageHead({
  title,
  eyebrow,
  children,
  actions,
}: {
  title: ReactNode;
  eyebrow?: ReactNode;
  children?: ReactNode;
  actions?: ReactNode;
}) {
  return (
    <div className="page-head">
      <div>
        {eyebrow && <div className="page-eyebrow">{eyebrow}</div>}
        <h1>{title}</h1>
        {children && <div className="page-intro">{children}</div>}
      </div>
      {actions && <div className="page-actions">{actions}</div>}
    </div>
  );
}

export function Loading({ label = 'Loading' }: { label?: string }) {
  return (
    <div className="loading" aria-busy="true">
      <span className="sr-only">{label}</span>
      <span className="skeleton" />
      <span className="skeleton" />
      <span className="skeleton short" />
    </div>
  );
}

export function LoadError({ error, retry }: { error: string; retry?: () => void }) {
  return (
    <Notice tone="danger" title="This didn't load">
      {error}{' '}
      {retry && (
        <button type="button" className="link-btn" onClick={retry}>
          Try again
        </button>
      )}
    </Notice>
  );
}
