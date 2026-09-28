import {
  Avatar,
  Badge,
  Button,
  Card,
  Checkbox,
  Dialog,
  Field,
  formatBytes,
  formatNumber,
  formatRelative,
  Input,
  Primary,
  StatusTag,
  Table,
} from '@loam/ui';
import { Lock, Plus } from 'lucide-react';
import { useState } from 'react';
import { Link, useNavigate, useParams } from 'react-router';
import { api, message, type Schemas } from '../api/client';
import { useLoad } from '../api/use';
import { LoadError, Loading, PageHead, usePageTitle } from '../page';
import { AuditTable } from '../parts/audit';
import { useSession } from '../session';
import { toast } from '../toast';

export function ProjectPage() {
  const { project = '' } = useParams();
  const { projects } = useSession();
  const navigate = useNavigate();
  const p = projects.find((x) => x.slug === project);
  usePageTitle(p?.name);
  const envs = useLoad(
    () => api.GET('/api/v1/projects/{project}/environments', { params: { path: { project } } }),
    [project],
  );
  const agents = useLoad(
    () => api.GET('/api/v1/projects/{project}/agents', { params: { path: { project } } }),
    [project],
  );
  const audit = useLoad(
    () => api.GET('/api/v1/audit', { params: { query: { project, limit: 6 } } }),
    [project],
  );
  const [creating, setCreating] = useState(false);

  if (!p) return <LoadError error={`There is no project ${project} in this org.`} />;

  return (
    <>
      <PageHead
        title={p.name}
        eyebrow="Project"
        actions={
          <Button variant="primary" onClick={() => setCreating(true)}>
            <Plus size={15} aria-hidden="true" /> New environment
          </Button>
        }
      >
        {p.description}
      </PageHead>

      <Card title="Environments" flush>
        {envs.loading && !envs.data ? (
          <Loading />
        ) : envs.error ? (
          <div className="pad">
            <LoadError error={envs.error} retry={envs.reload} />
          </div>
        ) : (
          <Table<Schemas['Environment']>
            rows={envs.data?.environments ?? []}
            rowKey={(e) => e.slug}
            onRowClick={(e) => navigate(`/projects/${project}/environments/${e.slug}`)}
            columns={[
              {
                key: 'name',
                header: 'Environment',
                cell: (e) => (
                  <Link to={`/projects/${project}/environments/${e.slug}`} className="row-link">
                    <span className="env-dot" data-slug={e.slug} aria-hidden="true" />
                    <Primary
                      title={
                        <>
                          {e.name} {e.protected && <Lock size={12} aria-label="protected" />}
                        </>
                      }
                      detail={e.region}
                    />
                  </Link>
                ),
              },
              { key: 'ns', header: 'Namespace', cell: (e) => <Badge>{e.namespace}</Badge> },
              {
                key: 'cols',
                header: 'Collections',
                numeric: true,
                cell: (e) => e.stats.collections,
              },
              {
                key: 'docs',
                header: 'Documents',
                numeric: true,
                cell: (e) => formatNumber(e.stats.documents),
              },
              {
                key: 'size',
                header: 'Storage',
                numeric: true,
                cell: (e) => formatBytes(e.stats.storage_bytes),
              },
              {
                key: 'q',
                header: 'Queries, 24 h',
                numeric: true,
                cell: (e) => formatNumber(e.stats.queries_24h),
              },
            ]}
          />
        )}
      </Card>

      <div className="grid-2" style={{ marginTop: 16 }}>
        <Card
          title="Agents"
          actions={
            <Link to={`/projects/${project}/agents`} className="card-link">
              All agents
            </Link>
          }
          flush
        >
          {agents.error ? (
            <div className="pad">
              <LoadError error={agents.error} retry={agents.reload} />
            </div>
          ) : (
            <ul className="agent-list">
              {(agents.data?.agents ?? []).map((a) => (
                <li key={a.id}>
                  <Link to={`/projects/${project}/agents/${a.id}`}>
                    <Avatar name={a.name} kind="agent" size={28} />
                    <Primary
                      title={a.name}
                      detail={
                        a.last_active_at
                          ? `active ${formatRelative(a.last_active_at)}`
                          : 'never active'
                      }
                    />
                    {a.status === 'suspended' ? (
                      <StatusTag status="failed">Suspended</StatusTag>
                    ) : (
                      <Badge>{a.active_tokens} live</Badge>
                    )}
                  </Link>
                </li>
              ))}
            </ul>
          )}
        </Card>
        <Card
          title="Recent activity"
          actions={
            <Link to="/audit" className="card-link">
              Audit log
            </Link>
          }
          flush
        >
          {audit.error ? (
            <div className="pad">
              <LoadError error={audit.error} retry={audit.reload} />
            </div>
          ) : (
            <AuditTable events={audit.data?.events ?? []} showProject={false} />
          )}
        </Card>
      </div>

      <NewEnvironment
        project={project}
        open={creating}
        onClose={() => setCreating(false)}
        onCreated={(e) => {
          setCreating(false);
          toast(`Created ${e.name}, namespace ${e.namespace}`);
          envs.reload();
        }}
      />
    </>
  );
}

function NewEnvironment({
  project,
  open,
  onClose,
  onCreated,
}: {
  project: string;
  open: boolean;
  onClose: () => void;
  onCreated: (e: Schemas['Environment']) => void;
}) {
  const [name, setName] = useState('');
  const [protect, setProtect] = useState(false);
  const [error, setError] = useState<string>();
  const [busy, setBusy] = useState(false);
  const slug = name
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');

  const submit = async (ev: React.FormEvent) => {
    ev.preventDefault();
    if (!slug) return setError('Give the environment a name.');
    setBusy(true);
    const r = await api.POST('/api/v1/projects/{project}/environments', {
      params: { path: { project } },
      body: { name: name.trim(), slug, protected: protect },
    });
    setBusy(false);
    if (r.data) onCreated(r.data);
    else setError(message(r.error));
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title="New environment"
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button variant="primary" type="submit" form="new-env" disabled={busy}>
            Create environment
          </Button>
        </>
      }
    >
      <form id="new-env" onSubmit={submit} className="form">
        <Field
          label="Name"
          hint={slug ? `Namespace ${project}-${slug}` : 'For example Preview or QA'}
          error={error}
        >
          {(p) => <Input {...p} value={name} onChange={(e) => setName(e.target.value)} autoFocus />}
        </Field>
        <Checkbox
          label="Protected: destructive actions need the project admin role"
          checked={protect}
          onChange={(e) => setProtect(e.target.checked)}
        />
      </form>
    </Dialog>
  );
}
