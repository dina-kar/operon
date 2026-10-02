import { Slot, SlotProvider, type SlotRegistry } from '@loams/slots';
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import type { ConsoleHandle } from './boot.js';

function Booting({ handle }: { handle: () => ConsoleHandle | undefined }) {
  const failed = handle()
    ?.plugins()
    .filter((p) => p.status === 'failed');
  return (
    <main className="loams-boot" aria-busy="true">
      <p>Starting the Loams console…</p>
      {failed && failed.length > 0 && (
        <ul>
          {failed.map((p) => (
            <li key={p.id}>
              {p.id}: {p.reason}
            </li>
          ))}
        </ul>
      )}
    </main>
  );
}

/** Renders the `root` slot; until the shell registers it, a boot screen. */
export function renderRoot(
  element: HTMLElement,
  slots: SlotRegistry,
  handle: () => ConsoleHandle | undefined,
): { unmount(): void } {
  const root = createRoot(element);
  root.render(
    <StrictMode>
      <SlotProvider registry={slots}>
        <Slot name="root" props={{}} fallback={<Booting handle={handle} />} />
      </SlotProvider>
    </StrictMode>,
  );
  return root;
}
