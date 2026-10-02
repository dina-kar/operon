import type { ButtonHTMLAttributes } from 'react';
import { cx } from './cx';

export type ButtonVariant = 'primary' | 'secondary' | 'quiet' | 'danger';
export type ButtonSize = 'sm' | 'md' | 'icon';

/** The class list for a button, for links styled as buttons. */
export function buttonClass({
  variant = 'secondary',
  size = 'md',
  className,
}: {
  variant?: ButtonVariant;
  size?: ButtonSize;
  className?: string;
} = {}): string {
  return cx(
    'loams-btn',
    `loams-btn-${variant}`,
    size === 'sm' && 'loams-btn-sm',
    size === 'icon' && 'loams-btn-icon',
    className,
  );
}

export type ButtonProps = ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: ButtonVariant;
  size?: ButtonSize;
};

/** A pill. `type` defaults to "button", so a button never submits by accident. */
export function Button({ variant, size, className, type = 'button', ...rest }: ButtonProps) {
  return <button type={type} className={buttonClass({ variant, size, className })} {...rest} />;
}
