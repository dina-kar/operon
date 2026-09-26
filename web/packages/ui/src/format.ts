/** Formatting shared by the console and the site. */

const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];

export function formatBytes(bytes: number): string {
  let v = bytes;
  let i = 0;
  while (v >= 1000 && i < units.length - 1) {
    v /= 1000;
    i++;
  }
  return `${v >= 100 || i === 0 ? Math.round(v) : v.toFixed(1)} ${units[i]}`;
}

export function formatNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(n >= 10_000_000 ? 0 : 1)}M`;
  if (n >= 10_000) return `${Math.round(n / 1000)}k`;
  return n.toLocaleString('en-US');
}

const rtf = new Intl.RelativeTimeFormat('en', { numeric: 'auto', style: 'short' });
const steps: [Intl.RelativeTimeFormatUnit, number][] = [
  ['second', 60],
  ['minute', 60],
  ['hour', 24],
  ['day', 30],
  ['month', 12],
  ['year', Number.POSITIVE_INFINITY],
];

/** "3 min. ago", "in 12 min.", from an RFC 3339 time. */
export function formatRelative(iso: string, now: number = Date.now()): string {
  let delta = (new Date(iso).getTime() - now) / 1000;
  for (const [unit, size] of steps) {
    if (Math.abs(delta) < size) return rtf.format(Math.round(delta), unit);
    delta /= size;
  }
  return iso;
}

export function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  });
}

export function formatDuration(seconds: number): string {
  if (seconds % 3600 === 0) return `${seconds / 3600} h`;
  if (seconds % 60 === 0) return `${seconds / 60} min`;
  return `${seconds} s`;
}
