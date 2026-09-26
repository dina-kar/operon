import { useEffect, useRef } from 'react';
import { fit, onThemeChange, readTokens, rgba, rng } from './canvas';
import { cx } from './cx';

export type GrainKind = 'sand' | 'silt' | 'clay' | 'bloom';

/**
 * Loam's texture: stipple instead of gradients. Sand is coarse and sparse,
 * silt even, clay fine and packed toward the bottom, and bloom is ochre
 * rising into sprout, for covers and calls to action only.
 */
export function Grain({
  kind,
  seed = 5,
  className,
}: {
  kind: GrainKind;
  seed?: number;
  className?: string;
}) {
  const ref = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = ref.current;
    if (!canvas) return;

    const draw = () => {
      const f = fit(canvas);
      if (!f) return;
      const { ctx, w, h } = f;
      const t = readTokens();
      const r = rng(seed);
      const area = (w * h) / 10000;
      ctx.clearRect(0, 0, w, h);

      if (kind === 'sand') {
        for (let i = 0; i < area * 18; i++) {
          ctx.fillStyle = rgba(t.ink, 0.15 + r() * 0.4);
          ctx.beginPath();
          ctx.arc(r() * w, r() * h, 0.8 + r() * 2.4, 0, 7);
          ctx.fill();
        }
      } else if (kind === 'silt') {
        const step = 6;
        for (let y = step / 2; y < h; y += step) {
          for (let x = step / 2; x < w; x += step) {
            ctx.fillStyle = rgba(t.ink, 0.12 + r() * 0.24);
            ctx.fillRect(x + (r() - 0.5) * 3, y + (r() - 0.5) * 3, 1.4, 1.4);
          }
        }
      } else if (kind === 'clay') {
        for (let i = 0; i < area * 320; i++) {
          const y = r() ** 0.5 * h;
          ctx.fillStyle = rgba(t.ink, 0.1 + (y / h) * 0.6 * r());
          ctx.fillRect(r() * w, y, 1.2, 1.2);
        }
      } else {
        // An ordered dot grid; dots grow toward two corners, ochre at the
        // bottom left and sprout at the top right.
        const g = Math.max(5, Math.round(w / 90));
        for (let y = 0; y < h; y += g) {
          for (let x = 0; x < w; x += g) {
            const u = x / w;
            const v = y / h;
            const d = Math.min(
              Math.hypot(u - 0.05, v - 1.05) * 1.1,
              Math.hypot(u - 1.02, v + 0.05),
            );
            const s = Math.max(0, 1 - d * 1.55) * (0.75 + r() * 0.25);
            if (s < 0.06) continue;
            ctx.fillStyle = rgba((u - v + 1) / 2 < 0.5 ? t.accent : t.grow, 0.9);
            const sz = s * g * 0.62;
            ctx.fillRect(x + (g - sz) / 2, y + (g - sz) / 2, sz, sz);
          }
        }
      }
    };

    draw();
    const ro = new ResizeObserver(draw);
    ro.observe(canvas);
    const off = onThemeChange(draw);
    return () => {
      ro.disconnect();
      off();
    };
  }, [kind, seed]);

  return (
    <span aria-hidden="true" className={cx('loam-grain', className)}>
      <canvas ref={ref} />
    </span>
  );
}
