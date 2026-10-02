import {
  Component,
  createContext,
  type ErrorInfo,
  type ReactNode,
  useContext,
  useSyncExternalStore,
} from 'react';
import type { SlotRegistry } from './registry.js';
import type { SlotEntry, SlotName, SlotProps } from './types.js';

const SlotContext = createContext<SlotRegistry | null>(null);

/** Makes a registry available to `<Slot>` and `useSlot` below it. */
export function SlotProvider({
  registry,
  children,
}: {
  registry: SlotRegistry;
  children: ReactNode;
}) {
  return <SlotContext.Provider value={registry}>{children}</SlotContext.Provider>;
}

export function useSlotRegistry(): SlotRegistry {
  const registry = useContext(SlotContext);
  if (!registry) throw new Error('useSlotRegistry outside <SlotProvider>');
  return registry;
}

/** The live entries of a slot; re-renders when any registration changes. */
export function useSlot<N extends SlotName>(name: N, key?: string): SlotEntry<N>[] {
  const registry = useSlotRegistry();
  useSyncExternalStore(registry.subscribe, registry.getVersion, registry.getVersion);
  return registry.entries(name, key);
}

/**
 * Renders a slot: every entry of a list slot, the one entry of a single
 * slot, or a keyed slot's entries for `slotKey`. Each entry renders inside
 * its own error boundary, so one plugin's crash stays in its box.
 */
export function Slot<N extends SlotName>({
  name,
  props,
  slotKey,
  fallback,
}: {
  name: N;
  props: SlotProps<N>;
  slotKey?: string;
  fallback?: ReactNode;
}) {
  const entries = useSlot(name, slotKey);
  if (entries.length === 0) return <>{fallback ?? null}</>;
  return (
    <>
      {entries.map((entry) => {
        const Entry = entry.component;
        return (
          <SlotBoundary key={entry.id} plugin={entry.plugin}>
            <Entry {...props} />
          </SlotBoundary>
        );
      })}
    </>
  );
}

interface BoundaryState {
  error: Error | null;
}

/** Contains a slot entry's render errors and names the plugin. */
export class SlotBoundary extends Component<
  { plugin: string; children: ReactNode },
  BoundaryState
> {
  override state: BoundaryState = { error: null };

  static getDerivedStateFromError(error: Error): BoundaryState {
    return { error };
  }

  override componentDidCatch(error: Error, info: ErrorInfo): void {
    console.error(`plugin ${this.props.plugin} failed to render`, error, info.componentStack);
  }

  override render() {
    if (this.state.error) {
      return (
        <div className="loams-notice loams-notice-danger" role="alert" data-plugin-error>
          <div>
            <strong>Plugin {this.props.plugin} failed</strong>
            <p>{this.state.error.message}</p>
            <button
              type="button"
              className="loams-btn loams-btn-secondary loams-btn-sm"
              onClick={() => this.setState({ error: null })}
            >
              Retry
            </button>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}
