import {
  Avatar,
  Badge,
  Button,
  Card,
  Checkbox,
  Dialog,
  Empty,
  Field,
  formatDuration,
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
  Textarea,
} from '@loam/ui';
import { Bot, Fingerprint, Pause, Play, Plus } from 'lucide-react';
import { useState } from 'react';
import { Link, useNavigate, useParams } from 'react-router';
import { api, message, type Schemas } from '../api/client';
import { useLoad, useNow } from '../api/use';
import { LoadError, Loading, PageHead, usePageTitle } from '../page';
import { ACTIONS } from '../parts/actions';
import { AuditTable } from '../parts/audit';
import { useSession } from '../session';
import { toast } from '../toast';

type Agent = Schemas['Agent'];

const flowLabel: Record<Schemas['TokenFlow'], string> = {
  federation: 'Workload identity',
  delegation: 'Delegated by a user',
  vending: 'Vended from a parent',
};

export function AgentsPage() {
  const { project = '' } = useParams();
  const { projects } = useSession();
  const navigate = useNavigate();
  const p = projects.find((x) => x.slug === project);
  usePageTitle(p ? `Agents, ${p.name}` : 'Agents');
  const agents = useLoad(
    () => api.GET('/api/v1/projects/{project}/agents', { params: { path: { project } } }),
    [project],
  );
  const [creating, setCreating] = useState(false);

  return (
    <>
      <PageHead
        eyebrow={p?.name}
        title="Agents"
        actions={
          <Button variant="primary" onClick={() => setCreating(true)}>
            <Plus size={15} aria-hidden="true" /> Register agent
          </Button>
        }
      >
        Agents never hold long-lived secrets. Each one gets short-lived tokens, scoped by its
        policy, through workload identity, a user's delegation, or a parent token.
      </PageHead>
      <Card flush>
        {agents.error ? (
          <div className="pad">
            <LoadError error={agents.error} retry={agents.reload} />
          </div>
        ) : !agents.data ? (
          <Loading />
        ) : (
          <Table<Agent>
            rows={agents.data.agents}
            rowKey={(a) => a.id}
            onRowClick={(a) => navigate(`/projects/${project}/agents/${a.id}`)}
            empty={
              <div className="pad">
                <Empty
                  title="No agents yet"
                  actions={
                    <Button variant="primary" size="sm" onClick={() => setCreating(true)}>
                      Register agent
                    </Button>
                  }
                >
                  Register an agent to give it a policy and let it exchange its identity for tokens.
                </Empty>
              </div>
            }
            columns={[
              {
                key: 'name',
                header: 'Agent',
                cell: (a) => (
                  <Link to={`/projects/${project}/agents/${a.id}`} className="row-link">
                    <Avatar name={a.name} kind="agent" size={28} />
                    <Primary title={a.name} detail={a.description} />
                  </Link>
                ),
              },
              {
                key: 'status',
                header: 'Status',
                cell: (a) =>
                  a.status === 'active' ? (
                    <StatusTag status="done">Active</StatusTag>
                  ) : (
                    <StatusTag status="failed">Suspended</StatusTag>
                  ),
              },
              {
                key: 'envs',
                header: 'Environments',
                cell: (a) => (
                  <span className="chips">
                    {a.policy.map((r) => (
                      <span key={r.environment} className="env-chip">
                        <span className="env-dot" data-slug={r.environment} aria-hidden="true" />
                        {r.environment}
                      </span>
                    ))}
                  </span>
                ),
              },
              { key: 'ttl', header: 'Token TTL', cell: (a) => formatDuration(a.max_token_ttl_s) },
              { key: 'tokens', header: 'Live tokens', numeric: true, cell: (a) => a.active_tokens },
              {
                key: 'seen',
                header: 'Last active',
                cell: (a) =>
                  a.last_active_at ? (
                    formatRelative(a.last_active_at)
                  ) : (
                    <span className="muted">never</span>
                  ),
              },
            ]}
          />
        )}
      </Card>
      <RegisterAgent
        project={project}
        open={creating}
        onClose={() => setCreating(false)}
        onCreated={(a) => {
          setCreating(false);
          toast(`Registered ${a.name}`);
          agents.reload();
        }}
      />
    </>
  );
}

function RegisterAgent({
  project,
  open,
  onClose,
  onCreated,
}: {
  project: string;
  open: boolean;
  onClose: () => void;
  onCreated: (a: Agent) => void;
}) {
  const envs = useLoad(
    () => api.GET('/api/v1/projects/{project}/environments', { params: { path: { project } } }),
    [project],
  );
  const teams = useLoad(() => api.GET('/api/v1/teams'), []);
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [owner, setOwner] = useState('');
  const [envChoice, setEnv] = useState<string>();
  const loadedEnvs = envs.data?.environments ?? [];
  // Default to an environment that exists, preferring an unprotected one.
  const env = envChoice ?? (loadedEnvs.find((e) => !e.protected) ?? loadedEnvs[0])?.slug ?? '';
  const [actions, setActions] = useState<Schemas['Action'][]>(['query']);
  const [ttl, setTtl] = useState(900);
  const [error, setError] = useState<string>();
  const { session } = useSession();

  const submit = async (ev: React.FormEvent) => {
    ev.preventDefault();
    if (!/^[a-z0-9][a-z0-9-]{1,62}$/.test(name))
      return setError('Use lowercase letters, digits and dashes, like docs-qa.');
    if (!actions.length) return setError('Allow at least one action.');
    if (!env) return setError('Pick an environment.');
    const team = teams.data?.teams.find((t) => t.id === owner);
    const r = await api.POST('/api/v1/projects/{project}/agents', {
      params: { path: { project } },
      body: {
        name,
        description,
        owner: team ? { kind: 'team', id: team.id } : { kind: 'user', id: session.user.id },
        policy: [{ environment: env, actions, collections: null }],
        max_token_ttl_s: ttl,
        limits: { requests_per_s: 20, concurrent_queries: 4 },
      },
    });
    if (r.data) onCreated(r.data);
    else setError(message(r.error));
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title="Register agent"
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button variant="primary" type="submit" form="new-agent">
            Register agent
          </Button>
        </>
      }
    >
      <form id="new-agent" onSubmit={submit} className="form">
        <Field label="Name" error={error}>
          {(p) => (
            <Input
              {...p}
              value={name}
              onChange={(e) => setName(e.target.value.toLowerCase())}
              placeholder="docs-qa"
              autoFocus
            />
          )}
        </Field>
        <Field label="What it does">
          {(p) => (
            <Textarea
              {...p}
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              rows={2}
            />
          )}
        </Field>
        <Field label="Owner" hint="Accountable for the agent and notified when it is denied.">
          {(p) => (
            <Select {...p} value={owner} onChange={(e) => setOwner(e.target.value)}>
              <option value="">Me ({session.user.name})</option>
              {(teams.data?.teams ?? []).map((t) => (
                <option key={t.id} value={t.id}>
                  Team: {t.name}
                </option>
              ))}
            </Select>
          )}
        </Field>
        <Field label="Environment" hint="Start in development; widen the policy later.">
          {(p) => (
            <Select {...p} value={env} onChange={(e) => setEnv(e.target.value)}>
              {(envs.data?.environments ?? []).map((e) => (
                <option key={e.slug} value={e.slug}>
                  {e.name}
                  {e.protected ? ' (protected)' : ''}
                </option>
              ))}
            </Select>
          )}
        </Field>
        <fieldset className="checks">
          <legend>Actions</legend>
          {ACTIONS.map((a) => (
            <Checkbox
              key={a.id}
              label={a.label}
              checked={actions.includes(a.id)}
              onChange={(e) =>
                setActions((s) => (e.target.checked ? [...s, a.id] : s.filter((x) => x !== a.id)))
              }
            />
          ))}
        </fieldset>
        <Field
          label="Token lifetime"
          hint="The longest any of its tokens may live. Shorter is safer."
        >
          {(p) => (
            <Select {...p} value={ttl} onChange={(e) => setTtl(Number(e.target.value))}>
              <option value={300}>5 minutes</option>
              <option value={900}>15 minutes</option>
              <option value={1800}>30 minutes</option>
              <option value={3600}>1 hour</option>
            </Select>
          )}
        </Field>
      </form>
    </Dialog>
  );
}

export function AgentPage() {
  const { project = '', agent = '' } = useParams();
  const a = useLoad(
    () => api.GET('/api/v1/agents/{agent}', { params: { path: { agent } } }),
    [agent],
  );
  const tokens = useLoad(
    () => api.GET('/api/v1/agents/{agent}/tokens', { params: { path: { agent } } }),
    [agent],
  );
  const trust = useLoad(
    () => api.GET('/api/v1/agents/{agent}/trust-policies', { params: { path: { agent } } }),
    [agent],
  );
  const audit = useLoad(
    () => api.GET('/api/v1/audit', { params: { query: { actor: agent, limit: 6 } } }),
    [agent],
  );
  const [status, setStatus] = useState<Agent['status']>();
  const [confirm, setConfirm] = useState(false);
  const [addTrust, setAddTrust] = useState(false);
  const now = useNow();
  usePageTitle(a.data?.name, a.data?.name);

  if (a.error) return <LoadError error={a.error} retry={a.reload} />;
  if (!a.data) return <Loading />;
  const agentData = a.data;
  const current = status ?? agentData.status;
  const live = (tokens.data?.tokens ?? []).filter((t) => new Date(t.expires_at).getTime() > now);

  const toggle = async () => {
    const r =
      current === 'active'
        ? await api.POST('/api/v1/agents/{agent}/suspend', { params: { path: { agent } } })
        : await api.POST('/api/v1/agents/{agent}/resume', { params: { path: { agent } } });
    setConfirm(false);
    if (r.data) {
      setStatus(r.data.status);
      toast(
        r.data.status === 'suspended'
          ? `Suspended ${agentData.name}; its tokens are revoked`
          : `Resumed ${agentData.name}`,
      );
    } else toast(message(r.error), 'danger');
  };

  const example = agentData.policy[0];
  const exchange = `curl -X POST ${window.location.origin}/api/v1/oauth/token \\
  -d grant_type=urn:ietf:params:oauth:grant-type:token-exchange \\
  -d subject_token="$WORKLOAD_OIDC_TOKEN" \\
  -d subject_token_type=urn:ietf:params:oauth:token-type:id_token \\
  -d audience=${project}/${example?.environment ?? 'development'} \\
  -d scope="${(example?.actions ?? ['query']).join(' ')}"`;

  return (
    <>
      <PageHead
        eyebrow={
          <Link to={`/projects/${project}/agents`} className="eyebrow-link">
            <Bot size={13} aria-hidden="true" /> Agents
          </Link>
        }
        title={
          <span className="title-row">
            {agentData.name}
            {current === 'active' ? (
              <StatusTag status="done">Active</StatusTag>
            ) : (
              <StatusTag status="failed">Suspended</StatusTag>
            )}
          </span>
        }
        actions={
          current === 'active' ? (
            <Button variant="danger" onClick={() => setConfirm(true)}>
              <Pause size={15} aria-hidden="true" /> Suspend
            </Button>
          ) : (
            <Button variant="primary" onClick={toggle}>
              <Play size={15} aria-hidden="true" /> Resume
            </Button>
          )
        }
      >
        {agentData.description} Owned by {agentData.owner.name}.
      </PageHead>

      <Stats>
        <Stat
          label="Live tokens"
          value={current === 'suspended' ? 0 : live.length}
          detail="Unexpired, right now"
        />
        <Stat
          label="Token lifetime cap"
          value={formatDuration(agentData.max_token_ttl_s)}
          detail="No refresh tokens"
        />
        <Stat
          label="Rate limit"
          value={`${agentData.limits.requests_per_s}/s`}
          detail={`${agentData.limits.concurrent_queries} concurrent queries`}
        />
        <Stat
          label="Last active"
          value={agentData.last_active_at ? formatRelative(agentData.last_active_at, now) : 'never'}
        />
      </Stats>

      <div className="grid-2" style={{ marginTop: 16 }}>
        <Card title="Policy">
          {agentData.policy.map((r) => (
            <div key={r.environment} className="rule">
              <span className="env-chip">
                <span className="env-dot" data-slug={r.environment} aria-hidden="true" />
                {r.environment}
              </span>
              <span className="chips">
                {r.actions.map((x) => (
                  <Badge key={x}>{x}</Badge>
                ))}
              </span>
              <span className="muted small">
                {r.collections ? `Only ${r.collections.join(', ')}` : 'All collections'}
              </span>
            </div>
          ))}
        </Card>
        <Card
          title="Trust policies"
          actions={
            <Button size="sm" onClick={() => setAddTrust(true)}>
              <Plus size={13} aria-hidden="true" /> Trust an identity
            </Button>
          }
        >
          {(trust.data?.trust_policies ?? []).length === 0 ? (
            <p>
              None. This agent gets tokens only by user delegation (OAuth with PKCE, for example
              from an MCP client) or from a parent token.
            </p>
          ) : (
            (trust.data?.trust_policies ?? []).map((t) => (
              <div key={t.id} className="trust">
                <Fingerprint size={16} aria-hidden="true" />
                <div>
                  <strong>{t.name}</strong>
                  <code className="small">{t.issuer}</code>
                  <span className="small">
                    subject <code>{t.subject_pattern}</code> → {t.environments.join(', ')}
                  </span>
                </div>
              </div>
            ))
          )}
        </Card>
      </div>

      <Card title="Tokens" flush style={{ marginTop: 16 }}>
        {current === 'suspended' ? (
          <div className="pad">
            <Notice tone="danger" title="Suspended">
              Every token was revoked, and exchanges are refused until the agent is resumed.
            </Notice>
          </div>
        ) : (
          <Table<Schemas['AgentToken']>
            rows={live}
            rowKey={(t) => t.jti}
            empty={
              <div className="pad">
                <Empty title="No live tokens" grain="clay" seed={7}>
                  Tokens appear here while they are valid, and expire on their own.
                </Empty>
              </div>
            }
            columns={[
              { key: 'jti', header: 'Token', cell: (t) => <Badge>{t.jti}</Badge> },
              {
                key: 'flow',
                header: 'How',
                cell: (t) => (
                  <Primary title={flowLabel[t.flow]} detail={<code>{t.subject}</code>} />
                ),
              },
              {
                key: 'for',
                header: 'Acting for',
                cell: (t) => t.acting_for?.name ?? <span className="muted">itself</span>,
              },
              { key: 'env', header: 'Environment', cell: (t) => t.environment },
              {
                key: 'scopes',
                header: 'Scopes',
                cell: (t) => (
                  <span className="chips">
                    {t.scopes.map((s) => (
                      <Badge key={s}>{s}</Badge>
                    ))}
                  </span>
                ),
              },
              {
                key: 'exp',
                header: 'Expires',
                cell: (t) => <Countdown until={t.expires_at} issued={t.issued_at} now={now} />,
              },
              {
                key: 'revoke',
                header: '',
                cell: (t) => (
                  <Button
                    variant="danger"
                    size="sm"
                    onClick={async () => {
                      const r = await api.DELETE('/api/v1/tokens/{jti}', {
                        params: { path: { jti: t.jti } },
                      });
                      toast(
                        r.response.ok ? `Revoked ${t.jti}` : message(r.error),
                        r.response.ok ? 'success' : 'danger',
                      );
                      if (r.response.ok) tokens.reload();
                    }}
                  >
                    Revoke
                  </Button>
                ),
              },
            ]}
          />
        )}
      </Card>

      <Card title="Get a token" style={{ marginTop: 16 }}>
        <p>
          From a runtime with workload identity (GitHub Actions, Kubernetes, a cloud role), exchange
          its OIDC token:
        </p>
        <Snippet>{exchange}</Snippet>
        <p>
          The answer is a token for {formatDuration(agentData.max_token_ttl_s)} at most; exchange
          again when it expires.
        </p>
      </Card>
      <Card title="Activity" flush style={{ marginTop: 16 }}>
        <AuditTable events={audit.data?.events ?? []} showProject={false} />
      </Card>

      <Dialog
        open={confirm}
        onClose={() => setConfirm(false)}
        title={`Suspend ${agentData.name}?`}
        footer={
          <>
            <Button onClick={() => setConfirm(false)}>Keep it running</Button>
            <Button variant="danger" onClick={toggle}>
              Suspend and revoke tokens
            </Button>
          </>
        }
      >
        <p className="muted">
          Its {live.length} live tokens stop working within seconds, and new exchanges are refused.
          You can resume it later.
        </p>
      </Dialog>
      <AddTrust
        agent={agent}
        open={addTrust}
        onClose={() => setAddTrust(false)}
        onCreated={() => trust.reload()}
      />
    </>
  );
}

function Countdown({ until, issued, now }: { until: string; issued: string; now: number }) {
  const end = new Date(until).getTime();
  const start = new Date(issued).getTime();
  const left = Math.max(0, Math.round((end - now) / 1000));
  const ratio = Math.max(0, Math.min(1, (end - now) / Math.max(1, end - start)));
  return (
    <span className="countdown" title={new Date(until).toLocaleString()}>
      <span
        className="countdown-bar"
        style={{ transform: `scaleX(${ratio})` }}
        data-low={ratio < 0.2 || undefined}
      />
      <span className="nowrap">
        {Math.floor(left / 60)}:{String(left % 60).padStart(2, '0')}
      </span>
    </span>
  );
}

function AddTrust({
  agent,
  open,
  onClose,
  onCreated,
}: {
  agent: string;
  open: boolean;
  onClose: () => void;
  onCreated: () => void;
}) {
  const presets = [
    {
      name: 'GitHub Actions',
      issuer: 'https://token.actions.githubusercontent.com',
      subject: 'repo:org/repo:ref:refs/heads/main',
    },
    {
      name: 'Kubernetes service account',
      issuer: 'https://oidc.eks.<region>.amazonaws.com/id/<cluster>',
      subject: 'system:serviceaccount:<ns>:<name>',
    },
    {
      name: 'Google Cloud',
      issuer: 'https://accounts.google.com',
      subject: '<service-account-unique-id>',
    },
  ];
  const [preset, setPreset] = useState(0);
  const chosen = presets[preset] ?? presets[0];
  const [issuer, setIssuer] = useState(chosen?.issuer ?? '');
  const [subject, setSubject] = useState(chosen?.subject ?? '');
  const [audience, setAudience] = useState('loam');
  const [envs, setEnvs] = useState('development');
  const [error, setError] = useState<string>();

  const submit = async (ev: React.FormEvent) => {
    ev.preventDefault();
    if (!issuer.startsWith('https://')) return setError('The issuer is an https URL.');
    const r = await api.POST('/api/v1/agents/{agent}/trust-policies', {
      params: { path: { agent } },
      body: {
        name: chosen?.name ?? 'Workload identity',
        issuer,
        audience,
        subject_pattern: subject,
        environments: envs
          .split(',')
          .map((s) => s.trim())
          .filter(Boolean),
      },
    });
    if (r.data) {
      toast(`Trusted ${r.data.name}`);
      onCreated();
      onClose();
    } else setError(message(r.error));
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title="Trust a workload identity"
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button variant="primary" type="submit" form="new-trust">
            Add trust policy
          </Button>
        </>
      }
    >
      <form id="new-trust" onSubmit={submit} className="form">
        <Field label="Runtime">
          {(p) => (
            <Select
              {...p}
              value={preset}
              onChange={(e) => {
                const i = Number(e.target.value);
                setPreset(i);
                setIssuer(presets[i]?.issuer ?? '');
                setSubject(presets[i]?.subject ?? '');
              }}
            >
              {presets.map((x, i) => (
                <option key={x.name} value={i}>
                  {x.name}
                </option>
              ))}
            </Select>
          )}
        </Field>
        <Field label="Issuer" error={error}>
          {(p) => <Input {...p} value={issuer} onChange={(e) => setIssuer(e.target.value)} />}
        </Field>
        <Field
          label="Subject"
          hint="A glob over the token's sub claim; * matches any run of characters."
        >
          {(p) => (
            <Input
              {...p}
              value={subject}
              onChange={(e) => setSubject(e.target.value)}
              className="loam-mono"
            />
          )}
        </Field>
        <Field label="Audience">
          {(p) => <Input {...p} value={audience} onChange={(e) => setAudience(e.target.value)} />}
        </Field>
        <Field label="Environments" hint="Comma-separated; the agent's policy still applies.">
          {(p) => <Input {...p} value={envs} onChange={(e) => setEnvs(e.target.value)} />}
        </Field>
      </form>
    </Dialog>
  );
}
