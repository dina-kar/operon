import {
  type InputHTMLAttributes,
  type ReactNode,
  type SelectHTMLAttributes,
  type TextareaHTMLAttributes,
  useId,
} from 'react';
import { cx } from './cx';

/** A label, a control, and a hint or an error. The control gets the ids. */
export function Field({
  label,
  hint,
  error,
  children,
}: {
  label: ReactNode;
  hint?: ReactNode;
  error?: ReactNode;
  children: (props: {
    id: string;
    'aria-describedby'?: string;
    'aria-invalid'?: boolean;
  }) => ReactNode;
}) {
  const id = useId();
  const noteId = hint || error ? `${id}-note` : undefined;
  return (
    <div className="loam-field">
      <label htmlFor={id}>{label}</label>
      {children({ id, 'aria-describedby': noteId, 'aria-invalid': error ? true : undefined })}
      {error ? (
        <span id={noteId} className="loam-field-error">
          {error}
        </span>
      ) : hint ? (
        <span id={noteId} className="loam-field-hint">
          {hint}
        </span>
      ) : null}
    </div>
  );
}

export function Input({ className, ...rest }: InputHTMLAttributes<HTMLInputElement>) {
  return <input className={cx('loam-input', className)} {...rest} />;
}

export function Select({ className, ...rest }: SelectHTMLAttributes<HTMLSelectElement>) {
  return <select className={cx('loam-input', className)} {...rest} />;
}

export function Textarea({ className, ...rest }: TextareaHTMLAttributes<HTMLTextAreaElement>) {
  return <textarea className={cx('loam-input', className)} {...rest} />;
}

export function Checkbox({
  label,
  ...rest
}: InputHTMLAttributes<HTMLInputElement> & { label: ReactNode }) {
  return (
    <label className="loam-check">
      <input type="checkbox" {...rest} />
      {label}
    </label>
  );
}
