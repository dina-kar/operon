import {
  Badge,
  Button,
  Card,
  Checkbox,
  Dialog,
  Empty,
  Field,
  formatBytes,
  formatDate,
  formatNumber,
  formatRelative,
  Input,
  Notice,
  Primary,
  Select,
  Snippet,
  Stat,
  Stats,
  StatusTag,
  Table,
} from '@loam/ui';
import { KeyRound, Lock } from 'lucide-react';
import { useState } from 'react';
import { useParams } from 'react-router';
import { api, type CollectionInfo, listCollections, message, type Schemas } from '../api/client';
import { useLoad } from '../api/use';
import { LoadError, Loading, PageHead, usePageTitle } from '../page';
import { ACTIONS } from '../parts/actions';
import { UsageChart } from '../parts/usage-chart';
import { toast } from '../toast';

export function EnvironmentPage() {
  const { project = '', environment = '' } = useParams();
  const path = { project, environment };
  const env = useLoad(
    () => api.GET('/api/v1/projects/{project}/environments/{environment}', { params: { path } }),
    [project, environment],
  );
  const usage = useLoad(
    () =>
      api.GET('/api/v1/projects/{project}/environments/{environment}/usage', { params: { path } }),
    [project, environment],
  );
  const keys = useLoad(
    () =>
      api.GET('/api/v1/projects/{project}/environments/{environment}/keys', { params: { path } }),
    [project, environment],
  );
  const ns = env.data?.namespace;
  const cols = useLoad(() => (ns ? listCollections(ns) : Promise.resolve([])), [ns]);
  const [newKey, setNewKey] = useState(false);
  usePageTitle(env.data ? `${env.data.name}, ${project}` : undefined);

  if (env.error) return <LoadError error={env.error} retry={env.reload} />;
  if (!env.data) return <Loading />;
  const e = env.data;

  return (
    <>
      <PageHead
        eyebrow="Environment"
        title={
          <span className="title-row">
            <span className="env-dot lg" data-slug={e.slug} aria-hidden="true" />
            {e.name}
            {e.protected && (
              <StatusTag status="progress">
                <Lock size={11} aria-hidden="true" /> Protected
              </StatusTag>
            )}
          </span>
        }
        actions={
          <Button variant="primary" onClick={() => setNewKey(true)}>
            <KeyRound size={15} aria-hidden="true" /> Create API key
          </Button>
        }
      >
        Namespace <Badge>{e.namespace}</Badge> in <Badge>{e.bucket}</Badge> ({e.region})
      </PageHead>

      <Stats>
        <Stat
          label="Documents"
          value={formatNumber(e.stats.documents)}
          detail={`${e.stats.collections} collections`}
        />
        <Stat
          label="Storage"
          value={formatBytes(e.stats.storage_bytes)}
          detail="Lance and Tantivy in the bucket"
        />
        <Stat label="Queries, 24 h" value={formatNumber(e.stats.queries_24h)} />
        <Stat label="Writes, 24 h" value={formatNumber(e.stats.writes_24h)} />
      </Stats>

      <Card title="Queries, last 30 days" style={{ marginTop: 16 }}>
        {usage.data ? (
          <UsageChart series={usage.data.series} />
        ) : usage.error ? (
          <LoadError error={usage.error} />
        ) : (
          <Loading />
        )}
      </Card>

      <Card title="Collections" flush style={{ marginTop: 16 }}>
        {cols.loading && !cols.data ? (
          <Loading />
        ) : cols.error ? (
          <div className="pad">
            <LoadError error={cols.error} retry={cols.reload} />
          </div>
        ) : (
          <Table<CollectionInfo>
            rows={cols.data ?? []}
            rowKey={(c) => String(c.id)}
            empty={
              <div className="pad">
                <Empty title="No collections yet">
                  Create one with the SDK or the native API; it appears here.
                </Empty>
              </div>
            }
            columns={[
              {
                key: 'name',
                header: 'Collection',
                cell: (c) => (
                  <Primary
                    title={c.name}
                    detail={[
                      ...c.schema.vectors.map((v) => `${v.name} ${v.dim}d`),
                      ...c.schema.sparse_vectors.map((s) => `${s.name} sparse`),
                      `${c.schema.fields.length} fields`,
                    ].join(' · ')}
                  />
                ),
              },
              {
                key: 'docs',
                header: 'Documents',
                numeric: true,
                cell: (c) => formatNumber(c.live_doc_count),
              },
              {
                key: 'size',
                header: 'Size',
                numeric: true,
                cell: (c) => formatBytes(c.size_bytes),
              },
              {
                key: 'ver',
                header: 'Manifest',
                numeric: true,
                cell: (c) => <Badge>v{c.manifest_version}</Badge>,
              },
              {
                key: 'hot',
                header: 'Hot tier',
                cell: (c) =>
                  c.hot.vectors.state === 'ready' ? (
                    <StatusTag status="done">Ready</StatusTag>
                  ) : c.hot.vectors.state === 'building' ? (
                    <StatusTag status="progress">Building</StatusTag>
                  ) : (
                    <StatusTag status="neutral">Off</StatusTag>
                  ),
              },
              {
                key: 'lag',
                header: 'Link lag',
                numeric: true,
                cell: (c) =>
                  c.link_lag_records ? (
                    `${c.link_lag_records} records`
                  ) : (
                    <span className="muted">none</span>
                  ),
              },
            ]}
          />
        )}
      </Card>

      <div className="grid-2" style={{ marginTop: 16 }}>
        <Card title="API keys" flush>
          <Table<Schemas['ApiKey']>
            rows={keys.data?.keys ?? []}
            rowKey={(k) => k.id}
            empty={
              <div className="pad">
                <Empty title="No API keys" grain="silt" seed={21}>
                  Agents never need one: they get short-lived tokens. Keys are for SDKs and service
                  accounts.
                </Empty>
              </div>
            }
            columns={[
              {
                key: 'name',
                header: 'Key',
                cell: (k) => <Primary title={k.name} detail={<code>{k.prefix}…</code>} />,
              },
              { key: 'who', header: 'Principal', cell: (k) => k.principal.name },
              {
                key: 'used',
                header: 'Last used',
                cell: (k) =>
                  k.last_used_at ? (
                    formatRelative(k.last_used_at)
                  ) : (
                    <span className="muted">never</span>
                  ),
              },
              {
                key: 'exp',
                header: 'Expires',
                cell: (k) =>
                  k.expires_at ? formatDate(k.expires_at) : <span className="muted">never</span>,
              },
              {
                key: 'revoke',
                header: '',
                cell: (k) => (
                  <Button
                    variant="danger"
                    size="sm"
                    onClick={async () => {
                      const r = await api.DELETE('/api/v1/keys/{key}', {
                        params: { path: { key: k.id } },
                      });
                      toast(
                        r.response.ok ? `Revoked ${k.name}` : message(r.error),
                        r.response.ok ? 'success' : 'danger',
                      );
                    }}
                  >
                    Revoke
                  </Button>
                ),
              },
            ]}
          />
        </Card>
        <Card title="Connect">
          <p>Query this environment's namespace over the native API with a token or a key:</p>
          <Snippet>{`curl -H "Authorization: Bearer $LOAM_TOKEN" \\\n  -X POST ${window.location.origin}/v1/namespaces/${e.namespace}/query -d @query.json`}</Snippet>
          <p>
            Agents exchange their workload identity for a token scoped to it; see an agent's page.
          </p>
        </Card>
      </div>

      <NewKey
        project={project}
        environment={environment}
        protectedEnv={e.protected}
        open={newKey}
        onClose={() => setNewKey(false)}
      />
    </>
  );
}

function NewKey({
  project,
  environment,
  protectedEnv,
  open,
  onClose,
}: {
  project: string;
  environment: string;
  protectedEnv: boolean;
  open: boolean;
  onClose: () => void;
}) {
  const sas = useLoad(
    () => api.GET('/api/v1/projects/{project}/service-accounts', { params: { path: { project } } }),
    [project],
  );
  const [name, setName] = useState('');
  const [sa, setSa] = useState('');
  const [days, setDays] = useState(90);
  const [scopes, setScopes] = useState<Schemas['Action'][]>(['query']);
  const [created, setCreated] = useState<Schemas['ApiKeyCreated']>();
  const [error, setError] = useState<string>();

  const close = () => {
    setCreated(undefined);
    setName('');
    onClose();
  };
  const submit = async (ev: React.FormEvent) => {
    ev.preventDefault();
    if (!name.trim()) return setError('Name the key after what uses it.');
    if (!scopes.length) return setError('Pick at least one action.');
    const r = await api.POST('/api/v1/projects/{project}/environments/{environment}/keys', {
      params: { path: { project, environment } },
      body: { name: name.trim(), service_account: sa || null, scopes, expires_in_days: days },
    });
    if (r.data) setCreated(r.data);
    else setError(message(r.error));
  };

  return (
    <Dialog
      open={open}
      onClose={close}
      title={created ? 'Copy your key now' : 'Create API key'}
      footer={
        created ? (
          <Button variant="primary" onClick={close}>
            I've stored it
          </Button>
        ) : (
          <>
            <Button onClick={close}>Cancel</Button>
            <Button variant="primary" type="submit" form="new-key">
              Create key
            </Button>
          </>
        )
      }
    >
      {created ? (
        <>
          <Notice tone="warn" title="This is the only time the secret is shown">
            Loam keeps only its hash. If you lose it, revoke the key and create another.
          </Notice>
          <Snippet prompt={false}>{created.secret}</Snippet>
        </>
      ) : (
        <form id="new-key" onSubmit={submit} className="form">
          {protectedEnv && (
            <Notice tone="warn" title="Protected environment">
              Creating a key here needs the project admin role.
            </Notice>
          )}
          <Field label="Name" error={error}>
            {(p) => (
              <Input
                {...p}
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="ci-deploy"
                autoFocus
              />
            )}
          </Field>
          <Field label="For" hint="A service account owns the key; without one, you do.">
            {(p) => (
              <Select {...p} value={sa} onChange={(e) => setSa(e.target.value)}>
                <option value="">Me</option>
                {(sas.data?.service_accounts ?? []).map((s) => (
                  <option key={s.id} value={s.id}>
                    {s.name}
                  </option>
                ))}
              </Select>
            )}
          </Field>
          <fieldset className="checks">
            <legend>Actions</legend>
            {ACTIONS.filter((a) => a.id !== 'mcp:tools').map((a) => (
              <Checkbox
                key={a.id}
                label={a.label}
                checked={scopes.includes(a.id)}
                onChange={(e) =>
                  setScopes((s) => (e.target.checked ? [...s, a.id] : s.filter((x) => x !== a.id)))
                }
              />
            ))}
          </fieldset>
          <Field label="Expires after" hint="Keys expire; 90 days is the default.">
            {(p) => (
              <Select {...p} value={days} onChange={(e) => setDays(Number(e.target.value))}>
                {[7, 30, 90, 180, 365].map((d) => (
                  <option key={d} value={d}>
                    {d} days
                  </option>
                ))}
              </Select>
            )}
          </Field>
        </form>
      )}
    </Dialog>
  );
}
