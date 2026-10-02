// The approvals inbox state, fed by WatchApprovals (AP0 Ruling 3): a
// snapshot replaces everything, an upsert replaces one approval, a remove
// drops one, a heartbeat only moves the cursor. Reconnects resume from the
// last cursor.

import type { ConnectError } from '@connectrpc/connect';
import { approvals, errors } from '@loams/proto';

export interface InboxState {
  approvals: approvals.Approval[];
  cursor: string;
  /** Set while the stream is down; the list is then possibly stale. */
  error?: string;
  connected: boolean;
}

export class Inbox {
  #state: InboxState = { approvals: [], cursor: '', connected: false };
  #listeners = new Set<() => void>();
  /** Called once per approval id the first time it appears pending. */
  onNew?: (approval: approvals.Approval) => void;
  #seen = new Set<string>();
  #primed = false;

  readonly getSnapshot = (): InboxState => this.#state;

  readonly subscribe = (listener: () => void): (() => void) => {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  };

  apply(message: approvals.WatchApprovalsResponse): void {
    const event = message.event;
    let list = this.#state.approvals;
    switch (event.case) {
      case 'snapshot':
        list = [...event.value.approvals];
        break;
      case 'upsert': {
        const next = event.value;
        list = list.some((a) => a.id === next.id)
          ? list.map((a) => (a.id === next.id ? next : a))
          : [...list, next];
        break;
      }
      case 'remove':
        list = list.filter((a) => a.id !== event.value);
        break;
      default:
        // A heartbeat only moves the cursor: it proves the stream is open,
        // not that the list has resynced, so a stale warning stays up.
        this.#set({ ...this.#state, cursor: message.cursor || this.#state.cursor });
        return;
    }
    for (const a of list) {
      if (this.#seen.has(a.id)) continue;
      this.#seen.add(a.id);
      // The first snapshot is not news; later arrivals are.
      if (this.#primed && a.state === approvals.ApprovalState.PENDING) this.onNew?.(a);
    }
    this.#primed = true;
    this.#set({ approvals: list, cursor: message.cursor || this.#state.cursor, connected: true });
  }

  failed(error: unknown): void {
    this.#set({ ...this.#state, connected: false, error: String(error) });
  }

  /** Applies a decision's answer before the stream reports it. */
  replace(approval: approvals.Approval): void {
    const pending = approval.state === approvals.ApprovalState.PENDING;
    this.#set({
      ...this.#state,
      approvals: pending
        ? this.#state.approvals.map((a) => (a.id === approval.id ? approval : a))
        : this.#state.approvals.filter((a) => a.id !== approval.id),
    });
  }

  #set(state: InboxState): void {
    this.#state = state;
    for (const listener of this.#listeners) listener();
  }
}

/** The stable reason of a failed call (AP0 Ruling 6). */
export function reasonOf(error: unknown): string | undefined {
  const e = error as ConnectError & { findDetails?: ConnectError['findDetails'] };
  const info = e?.findDetails?.(errors.ErrorInfoSchema)?.[0];
  if (info?.reason) return info.reason;
  // The in-memory mock puts the reason in the message.
  return /\[([a-z_]+)\]$/.exec(e?.rawMessage ?? e?.message ?? '')?.[1];
}

/** What the user is told for each reason. */
export const REASON_TEXT: Record<string, string> = {
  step_up_required: 'Sign in again (your session is older than 5 minutes), then decide.',
  requester_cannot_approve: 'You requested this (or an agent did for you); someone else decides.',
  approval_stale_revision: 'The approval changed while you looked at it. Review it again.',
  approval_already_decided: 'Someone already decided this approval.',
  approval_expired: 'This approval expired.',
  reason_required: 'Give a reason.',
  not_implemented: 'This server cannot do that yet.',
};
