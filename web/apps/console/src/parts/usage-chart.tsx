import { formatNumber } from '@loam/ui';
import { useState } from 'react';
import type { Schemas } from '../api/client';

/** Daily queries as bars, the newest in ochre. Hover or focus a bar for its numbers. */
export function UsageChart({ series }: { series: Schemas['Usage']['series'] }) {
  const [hover, setHover] = useState<number>();
  const max = Math.max(1, ...series.map((d) => d.queries));
  const w = 100 / series.length;
  const shown = hover !== undefined ? series[hover] : series[series.length - 1];
  return (
    <div className="chart">
      <div className="chart-legend" aria-live="polite">
        {shown && (
          <>
            <strong>{formatNumber(shown.queries)}</strong> queries, {formatNumber(shown.writes)}{' '}
            writes on{' '}
            {new Date(`${shown.date}T00:00:00Z`).toLocaleDateString('en-US', {
              month: 'short',
              day: 'numeric',
              timeZone: 'UTC',
            })}
          </>
        )}
      </div>
      <svg
        viewBox="0 0 100 40"
        preserveAspectRatio="none"
        className="chart-svg"
        role="img"
        aria-label="Queries per day"
      >
        {series.map((d, i) => {
          const h = (d.queries / max) * 38;
          return (
            <rect
              key={d.date}
              x={i * w + w * 0.15}
              y={40 - h}
              width={w * 0.7}
              height={h}
              className={i === series.length - 1 ? 'bar latest' : i === hover ? 'bar on' : 'bar'}
              style={{ animationDelay: `${i * 14}ms` }}
              onPointerEnter={() => setHover(i)}
              onPointerLeave={() => setHover(undefined)}
            />
          );
        })}
      </svg>
      <div className="chart-axis">
        <span>30 days ago</span>
        <span>today</span>
      </div>
    </div>
  );
}
