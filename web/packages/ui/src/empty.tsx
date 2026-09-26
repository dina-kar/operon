import type { ReactNode } from 'react';
import { Grain, type GrainKind } from './grain';

/** An empty state says what goes here and how to add it. */
export function Empty({
  title,
  children,
  actions,
  grain = 'sand',
  seed = 13,
}: {
  title: string;
  children?: ReactNode;
  actions?: ReactNode;
  grain?: Exclude<GrainKind, 'bloom'>;
  seed?: number;
}) {
  return (
    <div className="loam-empty">
      <Grain kind={grain} seed={seed} />
      <strong>{title}</strong>
      {children && <p>{children}</p>}
      {actions && <div className="loam-empty-actions">{actions}</div>}
    </div>
  );
}
