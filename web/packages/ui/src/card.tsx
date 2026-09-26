import type { CSSProperties, ReactNode } from 'react';
import { cx } from './cx';

/** A square panel with a raised head. `flush` drops the body padding, for tables. */
export function Card({
  title,
  actions,
  children,
  flush,
  className,
  style,
  headingLevel = 2,
}: {
  title?: ReactNode;
  actions?: ReactNode;
  children: ReactNode;
  flush?: boolean;
  className?: string;
  style?: CSSProperties;
  headingLevel?: 2 | 3;
}) {
  const H = headingLevel === 2 ? 'h2' : 'h3';
  return (
    <section className={cx('loam-card', className)} style={style}>
      {(title || actions) && (
        <div className="loam-card-head">
          {title ? <H>{title}</H> : <span />}
          {actions && <div>{actions}</div>}
        </div>
      )}
      <div className={cx('loam-card-body', flush && 'loam-card-flush')}>{children}</div>
    </section>
  );
}
