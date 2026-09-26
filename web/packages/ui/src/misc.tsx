import { type ReactNode, useEffect, useId, useRef, useState } from 'react';
import { Button } from './button';
import { cx } from './cx';

export function Stats({ children }: { children: ReactNode }) {
  return <div className="loam-stats">{children}</div>;
}

export function Stat({
  label,
  value,
  detail,
}: {
  label: string;
  value: ReactNode;
  detail?: ReactNode;
}) {
  return (
    <div className="loam-stat">
      <span>{label}</span>
      <strong>{value}</strong>
      {detail && <small>{detail}</small>}
    </div>
  );
}

export type NoticeTone = 'info' | 'warn' | 'danger' | 'success';

export function Notice({
  tone = 'info',
  title,
  children,
}: {
  tone?: NoticeTone;
  title: ReactNode;
  children?: ReactNode;
}) {
  return (
    <div
      className={cx('loam-notice', tone !== 'info' && `loam-notice-${tone}`)}
      role={tone === 'danger' ? 'alert' : 'status'}
    >
      <div>
        <strong>{title}</strong>
        {children && <p>{children}</p>}
      </div>
    </div>
  );
}

/** A command or a value to copy. */
export function Snippet({
  children,
  prompt = true,
  copy = true,
}: {
  children: string;
  prompt?: boolean;
  copy?: boolean;
}) {
  const [copied, setCopied] = useState(false);
  useEffect(() => {
    if (!copied) return;
    const t = setTimeout(() => setCopied(false), 1600);
    return () => clearTimeout(t);
  }, [copied]);
  return (
    <div className="loam-snippet">
      <code>
        {prompt && <span className="loam-snippet-prompt">$ </span>}
        {children}
      </code>
      {copy && (
        <Button
          variant="quiet"
          size="sm"
          onClick={() => navigator.clipboard?.writeText(children).then(() => setCopied(true))}
        >
          {copied ? 'Copied' : 'Copy'}
        </Button>
      )}
    </div>
  );
}

/** A usage bar; ochre above 80 percent, oxide at the limit. */
export function Meter({ value, max, label }: { value: number; max: number; label: string }) {
  const ratio = max > 0 ? Math.min(1, value / max) : 0;
  const level = ratio >= 1 ? 'full' : ratio >= 0.8 ? 'high' : undefined;
  return (
    <div className="loam-meter" data-level={level}>
      <meter className="sr-only" value={value} min={0} max={max} aria-label={label} />
      <span aria-hidden="true" style={{ width: `${ratio * 100}%` }} />
    </div>
  );
}

export type AvatarKind = 'user' | 'team' | 'agent' | 'service_account' | 'system';

/** Initials in a disc for people, a square for agents, teams and service accounts. */
export function Avatar({
  name,
  kind = 'user',
  size = 24,
}: {
  name: string;
  kind?: AvatarKind;
  size?: number;
}) {
  const initials = name
    .split(/[\s\-_]+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((w) => w[0]?.toUpperCase())
    .join('');
  return (
    <span
      className="loam-avatar"
      data-kind={kind}
      style={{ width: size, height: size }}
      aria-hidden="true"
    >
      {initials}
    </span>
  );
}

/** A modal on the native <dialog>: focus moves in, Escape closes, the page behind is inert. */
export function Dialog({
  open,
  onClose,
  title,
  children,
  footer,
}: {
  open: boolean;
  onClose: () => void;
  title: ReactNode;
  children: ReactNode;
  footer?: ReactNode;
}) {
  const ref = useRef<HTMLDialogElement>(null);
  const titleId = useId();
  // The native close event also fires when `open` turns false and the effect
  // closes the dialog; only a dismissal while `open` is true reports onClose.
  const openRef = useRef(open);
  openRef.current = open;
  useEffect(() => {
    const d = ref.current;
    if (!d) return;
    if (open && !d.open) d.showModal();
    if (!open && d.open) d.close();
  }, [open]);
  return (
    <dialog
      ref={ref}
      className="loam-dialog"
      onClose={() => openRef.current && onClose()}
      aria-labelledby={titleId}
    >
      <div className="loam-dialog-head">
        <h2 id={titleId}>{title}</h2>
        <Button variant="quiet" size="icon" aria-label="Close" onClick={onClose}>
          ×
        </Button>
      </div>
      <div className="loam-dialog-body">{children}</div>
      {footer && <div className="loam-dialog-foot">{footer}</div>}
    </dialog>
  );
}
