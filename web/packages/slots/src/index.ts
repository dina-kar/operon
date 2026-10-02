// @loams/slots: typed UI slots for console plugins (§37 §5.5, D425).

export { SLOT_KINDS, SlotRegistry } from './registry.js';
export { Slot, SlotBoundary, SlotProvider, useSlot, useSlotRegistry } from './render.js';
export type {
  EnvironmentRef,
  SlotComponent,
  SlotEntry,
  SlotKind,
  SlotMap,
  SlotMeta,
  SlotName,
  SlotProps,
  SlotSpec,
} from './types.js';
