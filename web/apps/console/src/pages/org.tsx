import {
  Avatar,
  Badge,
  Button,
  Card,
  Checkbox,
  Dialog,
  Empty,
  Field,
  formatDate,
  formatRelative,
  Input,
  Primary,
  Select,
  StatusTag,
  Table,
} from '@loam/ui';
import { Plus } from 'lucide-react';
import { useState } from 'react';
import { Link, useNavigate, useParams } from 'react-router';
import { api, message, type Schemas } from '../api/client';
import { useLoad } from '../api/use';
import { LoadError, Loading, PageHead, usePageTitle } from '../page';
import { AuditTable } from '../parts/audit';
import { useSession } from '../session';
import { toast } from '../toast';

const roleText: Record<Schemas['ProjectRole'], string> = {
  admin: 'Admin',
  developer: 'Developer',
  viewer: 'Viewer',
};

export function AccessPage() {
  const { project = '' } = useParams();
  const { projects } = useSession();
  const p = projects.find((x) => x.slug === project);
  usePageTitle(p ? `Access, ${p.name}` : 'Access');
  const grants = useLoad(
    () => api.GET('/api/v1/projects/{project}/access', { params: { path: { project } } }),
    [project],
  );
  const sas = useLoad(
    () => api.GET('/api/v1/projects/{project}/service-accounts', { params: { path: { project } } }),
    [project],
  );
  const [granting, setGranting] = useState(false);

  return (
    <>
      <PageHead
        eyebrow={p?.name}
        title="Access"
        actions={
          <Button variant="primary" onClick={() => setGranting(true)}>
            <Plus size={15} aria-hidden="true" /> Grant access
          </Button>
        }
      >
        Roles apply to every environment in the project. Protected environments also need the admin
        role for destructive actions.
      </PageHead>
      <Card title="People and teams" flush>
        {grants.error ? (
          <div className="pad">
            <LoadError error={grants.error} retry={grants.reload} />
          </div>
        ) : !grants.data ? (
          <Loading />
        ) : (
          <Table<Schemas['Grant']>
            rows={grants.data.grants}
            rowKey={(g) => g.id}
            columns={[
              {
                key: 'who',
                header: 'Who',
                cell: (g) => (
                  <span className="who">
                    <Avatar name={g.subject.name} kind={g.subject.kind} size={26} />
                    <Primary
                      title={g.subject.name}
                      detail={g.subject.kind === 'team' ? 'Team' : 'Member'}
                    />
                  </span>
                ),
              },
              { key: 'role', header: 'Role', cell: (g) => <Badge>{roleText[g.role]}</Badge> },
              { key: 'since', header: 'Since', cell: (g) => formatDate(g.created_at) },
            ]}
          />
        )}
      </Card>
      <Card title="Service accounts" flush style={{ marginTop: 16 }}>
        <Table<Schemas['ServiceAccount']>
          rows={sas.data?.service_accounts ?? []}
          rowKey={(s) => s.id}
          empty={
            <div className="pad">
              <Empty title="No service accounts" grain="silt" seed={5}>
                For CI jobs and batch workloads that are not agents. They can federate, or hold API
                keys.
              </Empty>
            </div>
          }
          columns={[
            {
              key: 'name',
              header: 'Service account',
              cell: (s) => (
                <span className="who">
                  <Avatar name={s.name} kind="service_account" size={26} />
                  <Primary title={s.name} detail={s.description} />
                </span>
              ),
            },
            { key: 'keys', header: 'Keys', numeric: true, cell: (s) => s.key_count },
            {
              key: 'seen',
              header: 'Last active',
              cell: (s) =>
                s.last_active_at ? (
                  formatRelative(s.last_active_at)
                ) : (
                  <span className="muted">never</span>
                ),
            },
          ]}
        />
      </Card>
      <GrantDialog
        project={project}
        open={granting}
        onClose={() => setGranting(false)}
        onDone={grants.reload}
      />
    </>
  );
}

function GrantDialog({
  project,
  open,
  onClose,
  onDone,
}: {
  project: string;
  open: boolean;
  onClose: () => void;
  onDone: () => void;
}) {
  const teams = useLoad(() => api.GET('/api/v1/teams'), []);
  const members = useLoad(() => api.GET('/api/v1/org/members'), []);
  const [subject, setSubject] = useState('');
  const [role, setRole] = useState<Schemas['ProjectRole']>('developer');
  const [error, setError] = useState<string>();
  const submit = async (ev: React.FormEvent) => {
    ev.preventDefault();
    const [kind, id] = subject.split(':');
    if (!id || (kind !== 'team' && kind !== 'user')) return setError('Pick a team or a member.');
    const r = await api.POST('/api/v1/projects/{project}/access', {
      params: { path: { project } },
      body: { subject: { kind, id }, role },
    });
    if (r.data) {
      toast(`Granted ${roleText[role].toLowerCase()} on ${project}`);
      onDone();
      onClose();
    } else setError(message(r.error));
  };
  return (
    <Dialog
      open={open}
      onClose={onClose}
      title="Grant access"
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button variant="primary" type="submit" form="grant">
            Grant access
          </Button>
        </>
      }
    >
      <form id="grant" className="form" onSubmit={submit}>
        <Field label="Team or member" error={error}>
          {(p) => (
            <Select {...p} value={subject} onChange={(e) => setSubject(e.target.value)}>
              <option value="">Choose…</option>
              <optgroup label="Teams">
                {(teams.data?.teams ?? []).map((t) => (
                  <option key={t.id} value={`team:${t.id}`}>
                    {t.name}
                  </option>
                ))}
              </optgroup>
              <optgroup label="Members">
                {(members.data?.members ?? []).map((m) => (
                  <option key={m.user.id} value={`user:${m.user.id}`}>
                    {m.user.name}
                  </option>
                ))}
              </optgroup>
            </Select>
          )}
        </Field>
        <Field
          label="Role"
          hint="Admin manages access and protected environments; viewer only reads."
        >
          {(p) => (
            <Select
              {...p}
              value={role}
              onChange={(e) => setRole(e.target.value as Schemas['ProjectRole'])}
            >
              <option value="admin">Admin</option>
              <option value="developer">Developer</option>
              <option value="viewer">Viewer</option>
            </Select>
          )}
        </Field>
      </form>
    </Dialog>
  );
}

export function TeamsPage() {
  usePageTitle('Teams');
  const teams = useLoad(() => api.GET('/api/v1/teams'), []);
  const navigate = useNavigate();
  const [creating, setCreating] = useState(false);
  const [name, setName] = useState('');
  const [group, setGroup] = useState('');
  return (
    <>
      <PageHead
        eyebrow="Organization"
        title="Teams"
        actions={
          <Button variant="primary" onClick={() => setCreating(true)}>
            <Plus size={15} aria-hidden="true" /> New team
          </Button>
        }
      >
        Teams receive project roles. A team linked to an SSO group gains and loses members as the
        group does.
      </PageHead>
      <Card flush>
        {teams.error ? (
          <div className="pad">
            <LoadError error={teams.error} retry={teams.reload} />
          </div>
        ) : !teams.data ? (
          <Loading />
        ) : (
          <Table<Schemas['Team']>
            rows={teams.data.teams}
            rowKey={(t) => t.id}
            onRowClick={(t) => navigate(`/teams/${t.id}`)}
            columns={[
              {
                key: 'name',
                header: 'Team',
                cell: (t) => (
                  <Link to={`/teams/${t.id}`} className="row-link">
                    <Avatar name={t.name} kind="team" size={28} />
                    <Primary title={t.name} detail={t.description} />
                  </Link>
                ),
              },
              { key: 'members', header: 'Members', numeric: true, cell: (t) => t.member_count },
              {
                key: 'sso',
                header: 'SSO group',
                cell: (t) =>
                  t.idp_group ? (
                    <Badge>{t.idp_group}</Badge>
                  ) : (
                    <span className="muted">not linked</span>
                  ),
              },
            ]}
          />
        )}
      </Card>
      <Dialog
        open={creating}
        onClose={() => setCreating(false)}
        title="New team"
        footer={
          <>
            <Button onClick={() => setCreating(false)}>Cancel</Button>
            <Button
              variant="primary"
              onClick={async () => {
                if (!name.trim()) return;
                const r = await api.POST('/api/v1/teams', {
                  body: { name: name.trim(), idp_group: group || null },
                });
                setCreating(false);
                toast(
                  r.data ? `Created ${name.trim()}` : message(r.error),
                  r.data ? 'success' : 'danger',
                );
              }}
            >
              Create team
            </Button>
          </>
        }
      >
        <div className="form">
          <Field label="Name">
            {(p) => (
              <Input {...p} value={name} onChange={(e) => setName(e.target.value)} autoFocus />
            )}
          </Field>
          <Field label="SSO group" hint="Optional: the OIDC groups claim value to sync from.">
            {(p) => (
              <Input
                {...p}
                value={group}
                onChange={(e) => setGroup(e.target.value)}
                placeholder="acme-search"
              />
            )}
          </Field>
        </div>
      </Dialog>
    </>
  );
}

export function TeamPage() {
  const { team = '' } = useParams();
  const t = useLoad(() => api.GET('/api/v1/teams/{team}', { params: { path: { team } } }), [team]);
  usePageTitle(t.data?.team.name, t.data?.team.name);
  if (t.error) return <LoadError error={t.error} retry={t.reload} />;
  if (!t.data) return <Loading />;
  const { team: info, members, projects } = t.data;
  return (
    <>
      <PageHead
        eyebrow={
          <Link to="/teams" className="eyebrow-link">
            Teams
          </Link>
        }
        title={info.name}
      >
        {info.description} {info.idp_group && <Badge>SSO group {info.idp_group}</Badge>}
      </PageHead>
      <div className="grid-2">
        <Card title="Members" flush>
          <Table<Schemas['User']>
            rows={members}
            rowKey={(u) => u.id}
            columns={[
              {
                key: 'who',
                header: 'Member',
                cell: (u) => (
                  <span className="who">
                    <Avatar name={u.name} size={26} />
                    <Primary title={u.name} detail={u.email} />
                  </span>
                ),
              },
            ]}
          />
        </Card>
        <Card title="Projects" flush>
          <Table<Schemas['ProjectGrantRef']>
            rows={projects}
            rowKey={(g) => g.project}
            empty={<p className="pad muted">This team has no project roles yet.</p>}
            columns={[
              {
                key: 'p',
                header: 'Project',
                cell: (g) => <Link to={`/projects/${g.project}`}>{g.project}</Link>,
              },
              { key: 'r', header: 'Role', cell: (g) => <Badge>{roleText[g.role]}</Badge> },
            ]}
          />
        </Card>
      </div>
    </>
  );
}

export function MembersPage() {
  usePageTitle('Members');
  const members = useLoad(() => api.GET('/api/v1/org/members'), []);
  const invitations = useLoad(() => api.GET('/api/v1/org/invitations'), []);
  const [inviting, setInviting] = useState(false);
  const [email, setEmail] = useState('');
  const [role, setRole] = useState<Schemas['OrgRole']>('member');
  return (
    <>
      <PageHead
        eyebrow="Organization"
        title="Members"
        actions={
          <Button variant="primary" onClick={() => setInviting(true)}>
            <Plus size={15} aria-hidden="true" /> Invite
          </Button>
        }
      />
      <Card flush>
        {members.error ? (
          <div className="pad">
            <LoadError error={members.error} retry={members.reload} />
          </div>
        ) : !members.data ? (
          <Loading />
        ) : (
          <Table<Schemas['Member']>
            rows={members.data.members}
            rowKey={(m) => m.user.id}
            columns={[
              {
                key: 'who',
                header: 'Member',
                cell: (m) => (
                  <span className="who">
                    <Avatar name={m.user.name} size={28} />
                    <Primary title={m.user.name} detail={m.user.email} />
                  </span>
                ),
              },
              { key: 'role', header: 'Role', cell: (m) => <Badge>{m.role}</Badge> },
              {
                key: 'auth',
                header: 'Sign-in',
                cell: (m) => (
                  <span className="chips">
                    {m.user.sso ? <Badge>SSO</Badge> : <Badge>Password</Badge>}
                    {m.user.two_factor ? (
                      <StatusTag status="done">2FA</StatusTag>
                    ) : (
                      <StatusTag status="progress">No 2FA</StatusTag>
                    )}
                  </span>
                ),
              },
              { key: 'teams', header: 'Teams', numeric: true, cell: (m) => m.teams.length },
              {
                key: 'seen',
                header: 'Last seen',
                cell: (m) =>
                  m.user.last_seen_at ? (
                    formatRelative(m.user.last_seen_at)
                  ) : (
                    <span className="muted">never</span>
                  ),
              },
            ]}
          />
        )}
      </Card>
      {(invitations.data?.invitations.length ?? 0) > 0 && (
        <Card title="Pending invitations" flush style={{ marginTop: 16 }}>
          <Table<Schemas['Invitation']>
            rows={invitations.data?.invitations ?? []}
            rowKey={(i) => i.id}
            columns={[
              { key: 'email', header: 'Email', cell: (i) => i.email },
              { key: 'role', header: 'Role', cell: (i) => <Badge>{i.role}</Badge> },
              { key: 'by', header: 'Invited by', cell: (i) => i.invited_by.name },
              { key: 'exp', header: 'Expires', cell: (i) => formatRelative(i.expires_at) },
            ]}
          />
        </Card>
      )}
      <Dialog
        open={inviting}
        onClose={() => setInviting(false)}
        title="Invite someone"
        footer={
          <>
            <Button onClick={() => setInviting(false)}>Cancel</Button>
            <Button
              variant="primary"
              onClick={async () => {
                if (!email.includes('@')) return;
                const r = await api.POST('/api/v1/org/invitations', { body: { email, role } });
                setInviting(false);
                toast(
                  r.data ? `Invited ${email}` : message(r.error),
                  r.data ? 'success' : 'danger',
                );
              }}
            >
              Send invitation
            </Button>
          </>
        }
      >
        <div className="form">
          <Field label="Email">
            {(p) => (
              <Input
                {...p}
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                autoFocus
              />
            )}
          </Field>
          <Field label="Role">
            {(p) => (
              <Select
                {...p}
                value={role}
                onChange={(e) => setRole(e.target.value as Schemas['OrgRole'])}
              >
                <option value="member">Member</option>
                <option value="admin">Admin</option>
              </Select>
            )}
          </Field>
        </div>
      </Dialog>
    </>
  );
}

export function AuditPage() {
  usePageTitle('Audit log');
  const { projects } = useSession();
  const [project, setProject] = useState('');
  const audit = useLoad(
    () => api.GET('/api/v1/audit', { params: { query: project ? { project } : {} } }),
    [project],
  );
  const [deniedOnly, setDeniedOnly] = useState(false);
  const events = (audit.data?.events ?? []).filter((e) => !deniedOnly || e.outcome === 'denied');
  return (
    <>
      <PageHead eyebrow="Organization" title="Audit log">
        Every mutating call and every token issued, with the actor and whom it acted for.
      </PageHead>
      <div className="filters">
        <Field label="Project">
          {(p) => (
            <Select {...p} value={project} onChange={(e) => setProject(e.target.value)}>
              <option value="">All projects and the org</option>
              {projects.map((x) => (
                <option key={x.slug} value={x.slug}>
                  {x.name}
                </option>
              ))}
            </Select>
          )}
        </Field>
        <Checkbox
          label="Denied only"
          checked={deniedOnly}
          onChange={(e) => setDeniedOnly(e.target.checked)}
        />
      </div>
      <Card flush>
        {audit.error ? (
          <div className="pad">
            <LoadError error={audit.error} retry={audit.reload} />
          </div>
        ) : !audit.data ? (
          <Loading />
        ) : (
          <AuditTable events={events} />
        )}
      </Card>
    </>
  );
}

export function SettingsPage() {
  const { session, instance } = useSession();
  usePageTitle('Settings');
  const [name, setName] = useState(session.org.name);
  const [twoFactor, setTwoFactor] = useState(session.org.require_two_factor);
  const [domains, setDomains] = useState(session.org.allowed_domains.join(', '));
  const save = async (ev: React.FormEvent) => {
    ev.preventDefault();
    const r = await api.PATCH('/api/v1/org', {
      body: {
        name: name.trim(),
        require_two_factor: twoFactor,
        allowed_domains: domains
          .split(',')
          .map((d) => d.trim())
          .filter(Boolean),
      },
    });
    toast(r.data ? 'Saved' : message(r.error), r.data ? 'success' : 'danger');
  };
  const s = instance.sign_in;
  return (
    <>
      <PageHead eyebrow="Organization" title="Settings" />
      <div className="grid-2">
        <Card title="Organization">
          <form className="form" onSubmit={save}>
            <Field label="Name">
              {(p) => <Input {...p} value={name} onChange={(e) => setName(e.target.value)} />}
            </Field>
            <Field
              label="Allowed email domains"
              hint="Single sign-on creates members only for these domains."
            >
              {(p) => <Input {...p} value={domains} onChange={(e) => setDomains(e.target.value)} />}
            </Field>
            <Checkbox
              label="Require two-factor sign-in for password accounts"
              checked={twoFactor}
              onChange={(e) => setTwoFactor(e.target.checked)}
            />
            <div>
              <Button variant="primary" type="submit">
                Save changes
              </Button>
            </div>
          </form>
        </Card>
        <Card title="Sign-in methods">
          <ul className="methods">
            <li>
              <span>Email and password</span>
              {s.password ? (
                <StatusTag status="done">On</StatusTag>
              ) : (
                <StatusTag status="neutral">Off</StatusTag>
              )}
            </li>
            <li>
              <span>Two-factor codes (TOTP)</span>
              {s.totp ? (
                <StatusTag status="done">On</StatusTag>
              ) : (
                <StatusTag status="neutral">Off</StatusTag>
              )}
            </li>
            {s.oidc.map((o) => (
              <li key={o.id}>
                <span>
                  {o.name} <Badge>OIDC, {o.kind}</Badge>
                </span>
                <StatusTag status="done">On</StatusTag>
              </li>
            ))}
            <li>
              <span>Passkeys</span>
              {s.passkeys ? (
                <StatusTag status="done">On</StatusTag>
              ) : (
                <StatusTag status="planned">Planned</StatusTag>
              )}
            </li>
          </ul>
          <p>
            SAML and SCIM come through your identity provider: put Keycloak, Dex or Authentik in
            front and connect it over OIDC.
          </p>
        </Card>
      </div>
      <Card title="This install" style={{ marginTop: 16 }}>
        <dl className="facts">
          <div>
            <dt>Edition</dt>
            <dd>
              {instance.edition === 'oss'
                ? 'Open source'
                : instance.edition === 'cloud'
                  ? 'Loam Cloud'
                  : 'BYOC'}
            </dd>
          </div>
          <div>
            <dt>Version</dt>
            <dd>{instance.version}</dd>
          </div>
          <div>
            <dt>Org id</dt>
            <dd>
              <code>{session.org.id}</code>
            </dd>
          </div>
          <div>
            <dt>Created</dt>
            <dd>{formatDate(session.org.created_at)}</dd>
          </div>
        </dl>
      </Card>
    </>
  );
}
