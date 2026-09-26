import { useEffect, useState } from 'react';

type Toast = { id: number; text: string; tone: 'success' | 'danger' | 'info' };
let next = 1;
const listeners = new Set<(t: Toast) => void>();

/** Shows a short confirmation at the bottom right. */
export function toast(text: string, tone: Toast['tone'] = 'success') {
  const t = { id: next++, text, tone };
  for (const l of listeners) l(t);
}

export function Toaster() {
  const [items, setItems] = useState<Toast[]>([]);
  useEffect(() => {
    const add = (t: Toast) => {
      setItems((xs) => [...xs, t]);
      setTimeout(() => setItems((xs) => xs.filter((x) => x.id !== t.id)), 4200);
    };
    listeners.add(add);
    return () => {
      listeners.delete(add);
    };
  }, []);
  return (
    <div className="toasts" aria-live="polite">
      {items.map((t) => (
        <div key={t.id} className="toast" data-tone={t.tone}>
          {t.text}
        </div>
      ))}
    </div>
  );
}
