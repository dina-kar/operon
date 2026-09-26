import { Badge, Button, buttonClass, Field, Input, Logo, Notice } from '@loam/ui';
import { KeyRound, ShieldCheck } from 'lucide-react';
import { type ReactNode, useState } from 'react';
import { useSearchParams } from 'react-router';
import { api, message, type Schemas, setCsrfToken } from '../api/client';
import { useLoad } from '../api/use';
import { usePageTitle } from '../page';

function AuthFrame({
  title,
  children,
  foot,
}: {
  title: string;
  children: ReactNode;
  foot?: ReactNode;
}) {
  return (
    <div className="auth">
      <div className="auth-card">
        <Logo height={22} />
        <h1>{title}</h1>
        {children}
      </div>
      {foot && <div className="auth-foot">{foot}</div>}
    </div>
  );
}

export function SignInPage() {
  usePageTitle('Sign in');
  const instance = useLoad(() => api.GET('/api/v1/instance'), []);
  const [params] = useSearchParams();
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [code, setCode] = useState('');
  const [error, setError] = useState<string>();
  const [busy, setBusy] = useState(false);
  const next = params.get('next') ?? '/';
  const methods = instance.data?.sign_in;

  const submit = async (ev: React.FormEvent) => {
    ev.preventDefault();
    setBusy(true);
    const r = await api.POST('/api/v1/session', {
      body: { email, password, ...(code ? { totp_code: code } : {}) },
    });
    setBusy(false);
    if (r.data) {
      setCsrfToken(r.data.csrf_token);
      // A full load, so the session gate starts from the new cookie.
      window.location.assign(`/ui${next.startsWith('/') ? next : '/'}`);
    } else setError(message(r.error));
  };

  return (
    <AuthFrame
      title={`Sign in to ${instance.data?.name ?? 'Loam'}`}
      foot={
        <>
          New here? Ask an admin for an invitation. <Badge>{instance.data?.version ?? ''}</Badge>
        </>
      }
    >
      {(methods?.oidc ?? []).map((o) => (
        <a
          key={o.id}
          href={`/api/v1/auth/oidc/${o.id}/start?next=${encodeURIComponent(next)}`}
          className={buttonClass({ variant: 'secondary', className: 'wide' })}
        >
          <ShieldCheck size={16} aria-hidden="true" /> Continue with {o.name}
        </a>
      ))}
      {methods?.password && (
        <>
          {(methods.oidc.length ?? 0) > 0 && <div className="or">or with your email</div>}
          <form className="form" onSubmit={submit}>
            {error && (
              <Notice tone="danger" title="Couldn't sign you in">
                {error}
              </Notice>
            )}
            <Field label="Email">
              {(p) => (
                <Input
                  {...p}
                  type="email"
                  autoComplete="username"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  required
                />
              )}
            </Field>
            <Field label="Password">
              {(p) => (
                <Input
                  {...p}
                  type="password"
                  autoComplete="current-password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  required
                />
              )}
            </Field>
            {methods.totp && (
              <Field label="Two-factor code" hint="If your account uses one.">
                {(p) => (
                  <Input
                    {...p}
                    inputMode="numeric"
                    autoComplete="one-time-code"
                    value={code}
                    onChange={(e) => setCode(e.target.value)}
                    maxLength={6}
                  />
                )}
              </Field>
            )}
            <Button variant="primary" type="submit" disabled={busy} className="wide">
              Sign in
            </Button>
          </form>
        </>
      )}
      {instance.error && (
        <Notice tone="danger" title="The console can't reach its API">
          {instance.error}
        </Notice>
      )}
    </AuthFrame>
  );
}

export function SetupPage() {
  usePageTitle('Set up Loam');
  const [form, setForm] = useState<Schemas['SetupRequest']>({
    setup_token: '',
    org_name: '',
    name: '',
    email: '',
    password: '',
  });
  const [error, setError] = useState<string>();
  const set = (k: keyof Schemas['SetupRequest']) => (e: React.ChangeEvent<HTMLInputElement>) =>
    setForm({ ...form, [k]: e.target.value });
  const submit = async (ev: React.FormEvent) => {
    ev.preventDefault();
    if (form.password.length < 12) return setError('Use a password of at least 12 characters.');
    const r = await api.POST('/api/v1/setup', { body: form });
    if (r.data) window.location.assign('/ui/');
    else setError(message(r.error));
  };
  return (
    <AuthFrame title="Set up this install">
      <p className="muted">
        The server printed a one-time setup token to its log when it started. It creates the
        organization and its owner, then stops working.
      </p>
      <form className="form" onSubmit={submit}>
        {error && (
          <Notice tone="danger" title="Setup failed">
            {error}
          </Notice>
        )}
        <Field label="Setup token">
          {(p) => (
            <Input
              {...p}
              value={form.setup_token}
              onChange={set('setup_token')}
              className="loam-mono"
              required
            />
          )}
        </Field>
        <Field label="Organization name">
          {(p) => (
            <Input
              {...p}
              value={form.org_name}
              onChange={set('org_name')}
              placeholder="Acme"
              required
            />
          )}
        </Field>
        <Field label="Your name">
          {(p) => <Input {...p} value={form.name} onChange={set('name')} required />}
        </Field>
        <Field label="Email">
          {(p) => <Input {...p} type="email" value={form.email} onChange={set('email')} required />}
        </Field>
        <Field
          label="Password"
          hint="12 characters or more. You can add single sign-on after setup."
        >
          {(p) => (
            <Input
              {...p}
              type="password"
              autoComplete="new-password"
              value={form.password}
              onChange={set('password')}
              required
            />
          )}
        </Field>
        <Button variant="primary" type="submit" className="wide">
          Create organization
        </Button>
      </form>
    </AuthFrame>
  );
}

/** The consent screen of user delegation (design §19 §5.2, flow 2). */
export function ConsentPage() {
  usePageTitle('Allow access');
  const [params] = useSearchParams();
  const client = params.get('client_id') ?? 'claude-code';
  const scope = (params.get('scope') ?? 'query mcp:tools').split(' ').filter(Boolean);
  const audience = params.get('audience') ?? 'code-index/development';
  const [done, setDone] = useState<'allowed' | 'denied'>();
  if (done)
    return (
      <AuthFrame title={done === 'allowed' ? 'Access allowed' : 'Access denied'}>
        <p className="muted">
          {done === 'allowed'
            ? `Return to ${client}. It now holds a token that acts as you, expires on its own, and never exceeds your rights.`
            : `${client} got nothing. You can close this tab.`}
        </p>
      </AuthFrame>
    );
  return (
    <AuthFrame title={`${client} wants to act as you`}>
      <div className="consent">
        <div>
          <span className="muted small">In</span>
          <Badge>{audience}</Badge>
        </div>
        <div>
          <span className="muted small">It may</span>
          <span className="chips">
            {scope.map((s) => (
              <Badge key={s}>{s}</Badge>
            ))}
          </span>
        </div>
        <p className="muted small">
          <KeyRound size={13} aria-hidden="true" /> Its token lasts at most the agent's lifetime cap
          and is logged under both of you. Revoke it from the agent's page.
        </p>
      </div>
      <div className="consent-actions">
        <Button onClick={() => setDone('denied')}>Deny</Button>
        <Button variant="primary" onClick={() => setDone('allowed')}>
          Allow
        </Button>
      </div>
    </AuthFrame>
  );
}

export function NotFound() {
  usePageTitle('Not found');
  return (
    <div className="page-head">
      <div>
        <h1>Nothing here</h1>
        <p className="page-intro">This page doesn't exist in the console.</p>
      </div>
    </div>
  );
}
