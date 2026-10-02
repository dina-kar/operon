import { act, cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { Slot, SlotProvider, SlotRegistry } from '../src/index.js';

afterEach(cleanup);

describe('SlotRegistry', () => {
  it('register_and_render_list_in_order', () => {
    const slots = new SlotRegistry();
    slots.register({ name: 'shell.overlay', plugin: 'b', order: 2 }, () => <p>second</p>);
    slots.register({ name: 'shell.overlay', plugin: 'a', order: 1 }, () => <p>first</p>);
    render(
      <SlotProvider registry={slots}>
        <Slot name="shell.overlay" props={{}} />
      </SlotProvider>,
    );
    expect(screen.getAllByRole('paragraph').map((p) => p.textContent)).toEqual(['first', 'second']);
  });

  it('keyed_slot_renders_one_key', () => {
    const slots = new SlotRegistry();
    slots.register({ name: 'console.page', plugin: 'a', key: 'one' }, () => <p>one</p>);
    slots.register({ name: 'console.page', plugin: 'b', key: 'two' }, () => <p>two</p>);
    render(
      <SlotProvider registry={slots}>
        <Slot name="console.page" slotKey="two" props={{ params: {} }} />
      </SlotProvider>,
    );
    expect(screen.queryByText('one')).toBeNull();
    expect(screen.getByText('two')).toBeTruthy();
    expect(slots.keys('console.page').sort()).toEqual(['one', 'two']);
  });

  it('refuses unknown slots, keyless keyed entries and a second single entry', () => {
    const slots = new SlotRegistry();
    // @ts-expect-error: not in SlotMap
    expect(() => slots.register({ name: 'nope', plugin: 'a' }, () => null)).toThrow(/unknown slot/);
    expect(() => slots.register({ name: 'console.page', plugin: 'a' }, () => null)).toThrow(
      /keyed/,
    );
    slots.register({ name: 'root', plugin: 'shell' }, () => null);
    expect(() => slots.register({ name: 'root', plugin: 'other' }, () => null)).toThrow(/single/);
  });

  it('entry_error_is_contained', () => {
    const spy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const slots = new SlotRegistry();
    slots.register({ name: 'shell.overlay', plugin: 'broken' }, () => {
      throw new Error('boom');
    });
    slots.register({ name: 'shell.overlay', plugin: 'fine' }, () => <p>still here</p>);
    render(
      <SlotProvider registry={slots}>
        <Slot name="shell.overlay" props={{}} />
      </SlotProvider>,
    );
    expect(screen.getByRole('alert').textContent).toContain('Plugin broken failed');
    expect(screen.getByText('still here')).toBeTruthy();
    spy.mockRestore();
  });

  it('dispose_removes_slot_entries (live)', () => {
    const slots = new SlotRegistry();
    const dispose = slots.register({ name: 'shell.overlay', plugin: 'a' }, () => <p>here</p>);
    render(
      <SlotProvider registry={slots}>
        <Slot name="shell.overlay" props={{}} fallback={<p>empty</p>} />
      </SlotProvider>,
    );
    expect(screen.getByText('here')).toBeTruthy();
    act(() => dispose());
    expect(screen.getByText('empty')).toBeTruthy();
    expect(slots.byPlugin('a')).toEqual([]);
  });
});
