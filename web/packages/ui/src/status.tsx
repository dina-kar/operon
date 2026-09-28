import type { ReactNode } from 'react';
import { cx } from './cx';

export type Status = 'done' | 'progress' | 'planned' | 'failed' | 'neutral';

const labels: Record<Status, string> = {
  done: 'Built',
  progress: 'In progress',
  planned: 'Planned',
  failed: 'Failed',
  neutral: '',
};

/** A state, in its one colour: sprout built, ochre in progress, gley planned, oxide failed. */
export function StatusTag({ status, children }: { status: Status; children?: ReactNode }) {
  return (
    <span className={cx('loam-status', `loam-status-${status}`)}>{children ?? labels[status]}</span>
  );
}

/** A quiet mono label for ids, scopes and versions. */
export function Badge({ children, title }: { children: ReactNode; title?: string }) {
  return (
    <span className="loam-badge" title={title}>
      {children}
    </span>
  );
}
