// components_never_receive_ctx (AP1a Task 3): a type-level check. A slot
// component's props are exactly the slot's props; nothing named `ctx` fits.
import type { ComponentProps } from 'react';
import type { SlotComponent } from './types.js';

// A props type may index every key (Record<string, never>), but then `ctx` can
// only be `never`: nothing can be passed in it.
type NoCtx<P> = P extends { ctx: infer T } ? ([T] extends [never] ? true : never) : true;

const _overlay: NoCtx<ComponentProps<SlotComponent<'shell.overlay'>>> = true;
const _page: NoCtx<ComponentProps<SlotComponent<'console.page'>>> = true;
void _overlay;
void _page;
