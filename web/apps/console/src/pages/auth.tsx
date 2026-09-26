import { Badge, Button, buttonClass, Field, Input, Logo, Notice } from '@loam/ui';
import { KeyRound, ShieldCheck } from 'lucide-react';
import { type ReactNode, useState } from 'react';
import { useSearchParams } from 'react-router';
import { api, message, type Schemas, setCsrfToken } from '../api/client';
import { useLoad } from '../api/use';
import { usePageTitle } from '../page';
import { useSession } from '../session';

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
    setError(undefined);
    try {
      const r = await api.POST('/api/v1/session', {
        body: { email, password, ...(code ? { totp_code: code } : {}) },
      });
      if (r.data) {
        setCsrfToken(r.data.csrf_token);
        // A full load, so the session gate starts from the new cookie. Only
        // same-origin paths are followed.
        window.location.assign(`/ui${next.startsWith('/') && !next.startsWith('//') ? next : '/'}`);
      } else setError(message(r.error));
    } catch (e) {
      setError(message(e));
    } finally {
      setBusy(false);
    }
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
  const [busy, setBusy] = useState(false);
  const set = (k: keyof Schemas['SetupRequest']) => (e: React.ChangeEvent<HTMLInputElement>) =>
    setForm({ ...form, [k]: e.target.value });
  const submit = async (ev: React.FormEvent) => {
    ev.preventDefault();
    if (form.password.length < 12) return setError('Use a password of at least 12 characters.');
    setBusy(true);
    setError(undefined);
    try {
      const r = await api.POST('/api/v1/setup', { body: form });
      if (r.data) window.location.assign('/ui/');
      else setError(message(r.error));
    } catch (e) {
      setError(message(e));
    } finally {
      setBusy(false);
    }
  };
  return (
    <AuthFrame title="Set up this install">
      <p className="muted">
        When it started, the server wrote a one-time setup token to the file it named in its log,
        readable only by the server's user. The token lasts an hour, creates the organization and
        its owner, then stops working.
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
        <Button variant="primary" type="submit" className="wide" disabled={busy}>
          Create organization
        </Button>
      </form>
    </AuthFrame>
  );
}

/** The consent screen of user delegation (design §19 §5.2, flow 2). */
/** The consent screen of user delegation (design §19 §5.2, flow 2). */
export function ConsentPage() {
  usePageTitle('Allow access');
  const { session } = useSession();
  const [params] = useSearchParams();
  const request = {
    client_id: params.get('client_id') ?? '',
    redirect_uri: params.get('redirect_uri') ?? '',
    scope: params.get('scope') ?? '',
    audience: params.get('audience') ?? '',
    state: params.get('state') ?? '',
    code_challenge: params.get('code_challenge') ?? '',
    code_challenge_method: 'S256' as const,
  };
  const scopes = request.scope.split(' ').filter(Boolean);
  const complete =
    Boolean(
      request.client_id && request.redirect_uri && request.audience && request.code_challenge,
    ) && params.get('code_challenge_method') === 'S256';
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();

  const decide = async (decision: 'allow' | 'deny') => {
    setBusy(true);
    setError(undefined);
    try {
      const r = await api.POST('/api/v1/oauth/consent', { body: { ...request, decision } });
      if (r.data) window.location.assign(r.data.redirect_to);
      else setError(message(r.error));
    } catch (e) {
      setError(message(e));
    } finally {
      setBusy(false);
    }
  };

  if (!complete)
    return (
      <AuthFrame title="This link is incomplete">
        <Notice tone="danger" title="Missing authorization parameters">
          Start again from the app that sent you here. A consent request needs a client, a redirect,
          an environment and a PKCE S256 challenge.
        </Notice>
      </AuthFrame>
    );
  return (
    <AuthFrame title={`${request.client_id} wants to act as you`}>
      <div className="consent">
        <div>
          <span className="muted small">As</span>
          <span>
            {session.user.name} <span className="muted small">{session.user.email}</span>
          </span>
        </div>
        <div>
          <span className="muted small">In</span>
          <Badge>{request.audience}</Badge>
        </div>
        <div>
          <span className="muted small">It may</span>
          <span className="chips">
            {scopes.map((s) => (
              <Badge key={s}>{s}</Badge>
            ))}
          </span>
        </div>
        <p className="muted small">
          <KeyRound size={13} aria-hidden="true" /> Its token lasts at most the agent's lifetime
          cap, never exceeds your rights, and is logged under both of you. Revoke it from the
          agent's page.
        </p>
      </div>
      {error && (
        <Notice tone="danger" title="The decision didn't go through">
          {error}
        </Notice>
      )}
      <div className="consent-actions">
        <Button onClick={() => decide('deny')} disabled={busy}>
          Deny
        </Button>
        <Button variant="primary" onClick={() => decide('allow')} disabled={busy}>
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
