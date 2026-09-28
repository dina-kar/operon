import { Badge, Card, formatBytes, formatNumber, Stat, Stats } from '@loam/ui';
import { Bot, Lock } from 'lucide-react';
import { Link } from 'react-router';
import { api, type Schemas } from '../api/client';
import { useLoad } from '../api/use';
import { LoadError, Loading, PageHead, usePageTitle } from '../page';
import { AuditTable } from '../parts/audit';
import { useSession } from '../session';

/** The org's front page: every project, its environments, and what just happened. */
export function Home() {
  const { session, projects } = useSession();
  usePageTitle(session.org.name);

  const envs = useLoad(
    () =>
      Promise.all(
        projects.map((p) =>
          api
            .GET('/api/v1/projects/{project}/environments', {
              params: { path: { project: p.slug } },
            })
            .then((r) => [p.slug, r.data?.environments ?? []] as const),
        ),
      ).then((pairs) => new Map<string, Schemas['Environment'][]>(pairs)),
    [projects],
  );
  const agents = useLoad(
    () =>
      Promise.all(
        projects.map((p) =>
          api
            .GET('/api/v1/projects/{project}/agents', { params: { path: { project: p.slug } } })
            .then((r) => r.data?.agents ?? []),
        ),
      ).then((lists) => lists.flat()),
    [projects],
  );
  const audit = useLoad(() => api.GET('/api/v1/audit', { params: { query: { limit: 8 } } }), []);

  const all = [...(envs.data?.values() ?? [])].flat();
  const sum = (f: (e: Schemas['Environment']) => number) => all.reduce((n, e) => n + f(e), 0);
  const liveTokens = (agents.data ?? []).reduce((n, a) => n + a.active_tokens, 0);

  return (
    <>
      <PageHead title={session.org.name} eyebrow="Organization">
        {projects.length} projects, {all.length || '…'} environments. Each environment is one
        namespace in your bucket.
      </PageHead>

      <Stats>
        <Stat
          label="Documents"
          value={envs.data ? formatNumber(sum((e) => e.stats.documents)) : '…'}
          detail="Across every environment"
        />
        <Stat
          label="Storage"
          value={envs.data ? formatBytes(sum((e) => e.stats.storage_bytes)) : '…'}
          detail="In object storage"
        />
        <Stat
          label="Queries, 24 h"
          value={envs.data ? formatNumber(sum((e) => e.stats.queries_24h)) : '…'}
        />
        <Stat
          label="Agent tokens live"
          value={agents.data ? liveTokens : '…'}
          detail={
            agents.data
              ? `${agents.data.filter((a) => a.status === 'active').length} active agents`
              : undefined
          }
        />
      </Stats>

      <h2 className="section-title">Projects</h2>
      {envs.error && <LoadError error={envs.error} retry={envs.reload} />}
      <div className="project-grid">
        {projects.map((p, i) => (
          <Link
            key={p.slug}
            to={`/projects/${p.slug}`}
            className="project-card"
            style={{ '--i': i } as React.CSSProperties}
          >
            <span className="project-card-head">
              <strong>{p.name}</strong>
              <span className="project-agents">
                <Bot size={14} aria-hidden="true" /> {p.agent_count}
              </span>
            </span>
            <span className="project-desc">{p.description}</span>
            <span className="project-envs">
              {(envs.data?.get(p.slug) ?? []).map((e) => (
                <span key={e.slug} className="env-chip">
                  <span className="env-dot" data-slug={e.slug} aria-hidden="true" />
                  {e.name}
                  {e.protected && <Lock size={11} aria-label="protected" />}
                  <span className="env-chip-n">{formatNumber(e.stats.documents)}</span>
                </span>
              ))}
            </span>
          </Link>
        ))}
      </div>

      <Card
        title="Recent activity"
        actions={
          <Link to="/audit" className="card-link">
            Audit log
          </Link>
        }
        flush
        style={{ marginTop: 28 }}
      >
        {audit.loading && !audit.data ? (
          <Loading />
        ) : audit.error ? (
          <div className="pad">
            <LoadError error={audit.error} retry={audit.reload} />
          </div>
        ) : (
          <AuditTable events={audit.data?.events ?? []} />
        )}
      </Card>
      <p className="footnote">
        Signed in as {session.user.name} <Badge>{session.role}</Badge>
      </p>
    </>
  );
}
