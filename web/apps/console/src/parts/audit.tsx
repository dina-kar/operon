import { Avatar, Badge, formatRelative, StatusTag, Table } from '@loam/ui';
import type { Schemas } from '../api/client';

type Event = Schemas['AuditEvent'];

/** The audit log as a table: who, acting for whom, did what, to what, where. */
export function AuditTable({
  events,
  showProject = true,
}: {
  events: Event[];
  showProject?: boolean;
}) {
  return (
    <Table<Event>
      rows={events}
      rowKey={(e) => e.id}
      caption="Audit events"
      empty={<p className="pad muted">No events yet.</p>}
      columns={[
        {
          key: 'at',
          header: 'When',
          width: '120px',
          cell: (e) => (
            <time dateTime={e.at} title={new Date(e.at).toLocaleString()} className="muted nowrap">
              {formatRelative(e.at)}
            </time>
          ),
        },
        {
          key: 'actor',
          header: 'Actor',
          cell: (e) => (
            <span className="who">
              <Avatar name={e.actor.name} kind={e.actor.kind} size={22} />
              <span>
                {e.actor.name}
                {e.acting_for && <span className="muted"> for {e.acting_for.name}</span>}
              </span>
            </span>
          ),
        },
        {
          key: 'action',
          header: 'Action',
          cell: (e) => <code className="action">{e.action}</code>,
        },
        {
          key: 'target',
          header: 'Target',
          cell: (e) => (
            <span>
              {e.target.name} <span className="muted">{e.target.kind.replace('_', ' ')}</span>
            </span>
          ),
        },
        ...(showProject
          ? [
              {
                key: 'where',
                header: 'Where',
                cell: (e: Event) =>
                  e.project ? (
                    <Badge>
                      {e.project}
                      {e.environment ? `/${e.environment}` : ''}
                    </Badge>
                  ) : (
                    <span className="muted">org</span>
                  ),
              },
            ]
          : []),
        {
          key: 'outcome',
          header: 'Outcome',
          cell: (e) =>
            e.outcome === 'denied' ? (
              <StatusTag status="failed">Denied</StatusTag>
            ) : (
              <StatusTag status="done">OK</StatusTag>
            ),
        },
      ]}
    />
  );
}
