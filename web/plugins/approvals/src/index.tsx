// @loams/plugin-approvals: the approvals inbox (§37 §7.3, AP1a Task 8).
//
// Holds WatchApprovals while the plugin is active (disposing it aborts the
// stream), lists pending approvals with the server-rendered summary and
// detail lines, and decides them. The rules are the server's (AP0 Ruling
// 7): a reason to reject or for anything destructive, typed confirmation of
// the target for DESTRUCTIVE, and step-up when the session is older than 5
// minutes. The generic `approval.renderer` is registered under "*"; other
// plugins may register richer renderers for their own kinds.

import { timestampDate } from '@bufbuild/protobuf/wkt';
import { type PlatformService, type PluginModule, service, watch } from '@loams/console-host';
import type { Context } from '@loams/cordis';
import { approvals } from '@loams/proto';
import { Badge, Button, Card, Empty, Field, Input, Notice, StatusTag } from '@loams/ui';
import { useRef, useState, useSyncExternalStore } from 'react';
import { Inbox, REASON_TEXT, reasonOf } from './store.js';

export { Inbox, REASON_TEXT, reasonOf } from './store.js';

type Client = ReturnType<typeof service<'rpc.approvals'>>;

const RISK_TEXT: Record<number, string> = {
  [approvals.Risk.LOW]: 'low risk',
  [approvals.Risk.MEDIUM]: 'medium risk',
  [approvals.Risk.HIGH]: 'high risk',
  [approvals.Risk.DESTRUCTIVE]: 'destructive',
};

export function ApprovalCard({
  approval,
  client,
  onDecided,
}: {
  approval: approvals.Approval;
  client: Client;
  onDecided: (a: approvals.Approval) => void;
}) {
  const destructive = approval.risk === approvals.Risk.DESTRUCTIVE;
  const confirmName = approval.target.name ?? '';
  const [reason, setReason] = useState('');
  const [typed, setTyped] = useState('');
  const [busy, setBusy] = useState(false);
  // One idempotency key per decision for this revision (the card remounts on
  // a new revision), so a retry after a lost answer is deduplicated.
  const keys = useRef(new Map<approvals.DecisionKind, string>());
  const [error, setError] = useState<string>();
  const requester = approval.requestedBy?.displayName ?? 'someone';
  // An agent acting for a user: the chain is [agent, user, ...].
  const onBehalfOf = approval.actorChain
    .slice(1)
    .map((p) => p.displayName)
    .join(' for ');
  const expires = approval.expiresAt ? timestampDate(approval.expiresAt) : undefined;

  const decide = async (decision: approvals.DecisionKind) => {
    setBusy(true);
    setError(undefined);
    try {
      const res = await client.decideApproval({
        approvalId: approval.id,
        revision: approval.revision,
        decision,
        reason,
        idempotencyKey:
          keys.current.get(decision) ??
          (keys.current.set(decision, crypto.randomUUID()).get(decision) as string),
      });
      if (res.approval) onDecided(res.approval);
    } catch (e) {
      const why = reasonOf(e);
      setError((why && REASON_TEXT[why]) ?? String(e));
    } finally {
      setBusy(false);
    }
  };

  const reasonNeeded = destructive;
  const canApprove =
    !busy && (!reasonNeeded || reason.trim()) && (!destructive || typed === confirmName);
  return (
    <Card
      title={approval.summary}
      actions={
        <>
          {approval.environment?.protected && <StatusTag status="progress">protected</StatusTag>}{' '}
          <Badge>{RISK_TEXT[approval.risk] ?? 'unknown risk'}</Badge>
        </>
      }
    >
      <p className="lc-muted">
        {approval.environment?.name} · requested by {requester}
        {onBehalfOf ? ` for ${onBehalfOf}` : ''}
        {expires ? ` · expires ${expires.toLocaleString()}` : ''}
      </p>
      <ul className="lc-detail-lines">
        {approval.detailLines.map((line, i) => (
          // Server-rendered lines may repeat; the list is fixed per revision.
          // biome-ignore lint/suspicious/noArrayIndexKey: see above
          <li key={i}>{line}</li>
        ))}
      </ul>
      <Field
        label="Reason"
        hint={reasonNeeded ? 'Required for destructive approvals' : 'Required to reject'}
      >
        {(props) => <Input {...props} value={reason} onChange={(e) => setReason(e.target.value)} />}
      </Field>
      {destructive && (
        <Field label={`Type ${confirmName} to confirm`}>
          {(props) => <Input {...props} value={typed} onChange={(e) => setTyped(e.target.value)} />}
        </Field>
      )}
      {error && (
        <Notice tone="danger" title="Not decided">
          {error}
        </Notice>
      )}
      <div className="lc-actions">
        <Button
          variant="primary"
          disabled={!canApprove}
          onClick={() => decide(approvals.DecisionKind.APPROVE)}
        >
          Approve
        </Button>
        <Button
          variant="danger"
          disabled={busy || !reason.trim()}
          onClick={() => decide(approvals.DecisionKind.REJECT)}
        >
          Reject
        </Button>
      </div>
    </Card>
  );
}

export function InboxPage({ inbox, client }: { inbox: Inbox; client: Client }) {
  const state = useSyncExternalStore(inbox.subscribe, inbox.getSnapshot);
  return (
    <div className="lc-page">
      <header className="lc-page-head">
        <h1>Approvals</h1>
        <p>{state.connected ? 'Live' : 'Connecting…'}</p>
      </header>
      {state.error && (
        <Notice tone="warn" title="The approvals stream dropped">
          The list may be out of date: {state.error}
        </Notice>
      )}
      {state.approvals.length === 0 ? (
        <Empty title="Nothing to approve">
          Destructive operations and agent actions that need a person show up here.
        </Empty>
      ) : (
        state.approvals.map((a) => (
          <ApprovalCard
            key={`${a.id}@${a.revision}`}
            approval={a}
            client={client}
            onDecided={(d) => inbox.replace(d)}
          />
        ))
      )}
    </div>
  );
}

function PendingChip({ inbox }: { inbox: Inbox }) {
  const state = useSyncExternalStore(inbox.subscribe, inbox.getSnapshot);
  const n = state.approvals.length;
  if (n === 0) return null;
  return (
    <a className="lc-chip" href="#/approvals">
      {n} approval{n === 1 ? '' : 's'} waiting
    </a>
  );
}

export interface ApprovalsConfig {
  /** Raise a platform notification for each new pending approval. */
  notify?: boolean;
}

function notifier(platform: PlatformService) {
  return (a: approvals.Approval) => {
    platform
      .notify({ title: 'Approval needed', body: a.summary, route: `/approvals` })
      .catch(() => {});
  };
}

const plugin: PluginModule<ApprovalsConfig> = {
  name: 'approvals',
  inject: ['rpc.approvals', 'router', 'slots', 'platform'],
  apply(ctx: Context, config: ApprovalsConfig) {
    const client = service(ctx, 'rpc.approvals');
    const router = service(ctx, 'router');
    const slots = service(ctx, 'slots');
    const inbox = new Inbox();
    if (config?.notify) inbox.onNew = notifier(service(ctx, 'platform'));
    watch(
      ctx,
      (signal) => client.watchApprovals({ resumeCursor: inbox.getSnapshot().cursor }, { signal }),
      (message) => inbox.apply(message),
      (error) => inbox.failed(error),
    );
    ctx.effect(() =>
      router.page(
        {
          id: 'approvals',
          path: '/approvals',
          title: 'Approvals',
          plugin: 'approvals',
          nav: { group: 'Operate', order: 20 },
        },
        () => <InboxPage inbox={inbox} client={client} />,
      ),
    );
    ctx.effect(() =>
      slots.register({ name: 'shell.overlay', plugin: 'approvals' }, () => (
        <PendingChip inbox={inbox} />
      )),
    );
  },
};

export default plugin;
