import { Avatar, Badge, Logo } from '@loam/ui';
import {
  BookOpen,
  Bot,
  ChevronsUpDown,
  Layers,
  LayoutGrid,
  Lock,
  LogOut,
  Menu,
  Moon,
  ScrollText,
  Settings,
  Shield,
  Sun,
  UserRound,
  Users,
  X,
} from 'lucide-react';
import { type ReactNode, useEffect, useRef, useState } from 'react';
import { Link, NavLink, Outlet, useLocation, useNavigate, useParams } from 'react-router';
import { api, type Schemas } from './api/client';
import { useLoad } from './api/use';
import { CrumbProvider, useCrumbTail } from './page';
import { useSession } from './session';
import { Toaster } from './toast';

function useTheme(): [boolean, () => void] {
  const [dark, setDark] = useState(() => document.documentElement.classList.contains('dark'));
  const toggle = () => {
    const next = !dark;
    document.documentElement.classList.toggle('dark', next);
    try {
      localStorage.setItem('loam-theme', next ? 'dark' : 'light');
    } catch {
      // Private mode: the choice lasts for this page only.
    }
    setDark(next);
  };
  return [dark, toggle];
}

/** The last project the viewer opened, so org pages keep the project nav. */
function useCurrentProject(projects: Schemas['Project'][]): Schemas['Project'] | undefined {
  const { project } = useParams();
  const [last, setLast] = useState<string | undefined>(() => {
    try {
      return localStorage.getItem('loam-project') ?? undefined;
    } catch {
      return undefined;
    }
  });
  useEffect(() => {
    if (!project) return;
    setLast(project);
    try {
      localStorage.setItem('loam-project', project);
    } catch {
      // Not saved; fine.
    }
  }, [project]);
  const slug = project ?? last;
  return projects.find((p) => p.slug === slug) ?? projects[0];
}

export function Shell() {
  const { session, projects, instance } = useSession();
  const current = useCurrentProject(projects);
  const [open, setOpen] = useState(false);
  const location = useLocation();

  // biome-ignore lint/correctness/useExhaustiveDependencies: close the drawer per route
  useEffect(() => setOpen(false), [location.pathname]);

  const envs = useLoad(
    () =>
      current
        ? api.GET('/api/v1/projects/{project}/environments', {
            params: { path: { project: current.slug } },
          })
        : Promise.resolve(undefined),
    [current?.slug],
  );

  return (
    <CrumbProvider>
      <div className="app">
        <aside id="nav" className="side" data-open={open || undefined}>
          <div className="side-brand">
            <Link to="/" aria-label="Console home">
              <Logo height={18} />
            </Link>
            <Badge>
              {instance.edition === 'oss' ? 'OSS' : instance.edition === 'cloud' ? 'Cloud' : 'BYOC'}
            </Badge>
            <button
              type="button"
              className="side-close"
              aria-label="Close navigation"
              onClick={() => setOpen(false)}
            >
              <X size={18} />
            </button>
          </div>

          <ProjectSwitcher projects={projects} current={current} />

          <nav className="side-nav" aria-label="Console">
            {current && (
              <div className="side-group">
                <h2>{current.name}</h2>
                <NavItem to={`/projects/${current.slug}`} end icon={<LayoutGrid size={16} />}>
                  Overview
                </NavItem>
                <div className="side-sub">
                  <span className="side-sub-title">
                    <Layers size={16} aria-hidden="true" /> Environments
                  </span>
                  {(envs.data?.environments ?? []).map((e) => (
                    <NavItem
                      key={e.slug}
                      to={`/projects/${current.slug}/environments/${e.slug}`}
                      sub
                    >
                      <span className="env-dot" data-slug={e.slug} aria-hidden="true" />
                      {e.name}
                      {e.protected && (
                        <Lock size={12} aria-label="protected" className="side-lock" />
                      )}
                    </NavItem>
                  ))}
                </div>
                <NavItem to={`/projects/${current.slug}/agents`} icon={<Bot size={16} />}>
                  Agents
                  <span className="side-count">{current.agent_count}</span>
                </NavItem>
                <NavItem to={`/projects/${current.slug}/access`} icon={<Shield size={16} />}>
                  Access
                </NavItem>
              </div>
            )}
            <div className="side-group">
              <h2>{session.org.name}</h2>
              <NavItem to="/" end icon={<LayoutGrid size={16} />}>
                All projects
              </NavItem>
              <NavItem to="/teams" icon={<Users size={16} />}>
                Teams
              </NavItem>
              <NavItem to="/members" icon={<UserRound size={16} />}>
                Members
              </NavItem>
              <NavItem to="/audit" icon={<ScrollText size={16} />}>
                Audit log
              </NavItem>
              <NavItem to="/settings" icon={<Settings size={16} />}>
                Settings
              </NavItem>
            </div>
          </nav>

          <div className="side-foot">
            <a href="https://loams.dev/docs" target="_blank" rel="noreferrer">
              <BookOpen size={16} aria-hidden="true" /> Documentation
            </a>
            <span className="side-version">Loam {instance.version}</span>
          </div>
        </aside>
        {open && (
          <button
            type="button"
            className="scrim"
            aria-label="Close navigation"
            onClick={() => setOpen(false)}
          />
        )}

        <div className="main">
          <TopBar onMenu={() => setOpen(true)} menuOpen={open} />
          <main id="main" className="content">
            <Outlet />
          </main>
        </div>
        <Toaster />
      </div>
    </CrumbProvider>
  );
}

function NavItem({
  to,
  icon,
  children,
  end,
  sub,
}: {
  to: string;
  icon?: ReactNode;
  children: ReactNode;
  end?: boolean;
  sub?: boolean;
}) {
  return (
    <NavLink
      to={to}
      end={end}
      className={({ isActive }) =>
        `side-link${sub ? ' side-link-sub' : ''}${isActive ? ' active' : ''}`
      }
    >
      {icon && <span aria-hidden="true">{icon}</span>}
      {children}
    </NavLink>
  );
}

function ProjectSwitcher({
  projects,
  current,
}: {
  projects: Schemas['Project'][];
  current: Schemas['Project'] | undefined;
}) {
  const navigate = useNavigate();
  if (!current) return null;
  return (
    <label className="switcher">
      <span className="sr-only">Project</span>
      <span className="switcher-mark" aria-hidden="true">
        {current.name.slice(0, 1)}
      </span>
      <select value={current.slug} onChange={(e) => navigate(`/projects/${e.target.value}`)}>
        {projects.map((p) => (
          <option key={p.slug} value={p.slug}>
            {p.name}
          </option>
        ))}
      </select>
      <ChevronsUpDown size={14} aria-hidden="true" />
    </label>
  );
}

function TopBar({ onMenu, menuOpen }: { onMenu: () => void; menuOpen: boolean }) {
  const { session, signOut } = useSession();
  const [dark, toggle] = useTheme();
  const [userOpen, setUserOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const navigate = useNavigate();

  useEffect(() => {
    if (!userOpen) return;
    const close = (e: PointerEvent) =>
      !ref.current?.contains(e.target as Node) && setUserOpen(false);
    const key = (e: KeyboardEvent) => e.key === 'Escape' && setUserOpen(false);
    window.addEventListener('pointerdown', close);
    window.addEventListener('keydown', key);
    return () => {
      window.removeEventListener('pointerdown', close);
      window.removeEventListener('keydown', key);
    };
  }, [userOpen]);

  return (
    <header className="top">
      <button
        type="button"
        className="top-menu"
        aria-label="Open navigation"
        aria-controls="nav"
        aria-expanded={menuOpen}
        onClick={onMenu}
      >
        <Menu size={18} />
      </button>
      <Crumbs />
      <div className="top-tools">
        <button
          type="button"
          className="icon-btn"
          onClick={toggle}
          aria-label={dark ? 'Use light theme' : 'Use dark theme'}
        >
          {dark ? <Sun size={16} /> : <Moon size={16} />}
        </button>
        <div className="user" ref={ref}>
          <button
            type="button"
            className="user-btn"
            aria-expanded={userOpen}
            aria-haspopup="menu"
            onClick={() => setUserOpen((o) => !o)}
          >
            <Avatar name={session.user.name} size={26} />
            <span className="sr-only">Account</span>
          </button>
          {userOpen && (
            <div className="user-menu" role="menu">
              <div className="user-who">
                <strong>{session.user.name}</strong>
                <span>{session.user.email}</span>
                <Badge>{session.role}</Badge>
              </div>
              <button
                type="button"
                role="menuitem"
                onClick={async () => {
                  await signOut();
                  navigate('/sign-in');
                }}
              >
                <LogOut size={15} aria-hidden="true" /> Sign out
              </button>
            </div>
          )}
        </div>
      </div>
    </header>
  );
}

const labels: Record<string, string> = {
  projects: '',
  environments: '',
  agents: 'Agents',
  access: 'Access',
  teams: 'Teams',
  members: 'Members',
  audit: 'Audit log',
  settings: 'Settings',
};

function Crumbs() {
  const { session, projects } = useSession();
  const { pathname } = useLocation();
  const tail = useCrumbTail();
  const parts = pathname.split('/').filter(Boolean);
  const crumbs: { to: string; label: string }[] = [{ to: '/', label: session.org.name }];
  let path = '';
  for (let i = 0; i < parts.length; i++) {
    const part = parts[i] ?? '';
    path += `/${part}`;
    const prev = parts[i - 1];
    if (prev === 'projects')
      crumbs.push({ to: path, label: projects.find((p) => p.slug === part)?.name ?? part });
    else if (prev === 'environments')
      crumbs.push({ to: path, label: part[0]?.toUpperCase() + part.slice(1) });
    else if (prev === 'agents' || prev === 'teams') crumbs.push({ to: path, label: tail ?? '…' });
    else if (labels[part]) crumbs.push({ to: path, label: labels[part] });
  }
  return (
    <nav aria-label="Breadcrumb" className="crumbs">
      <ol>
        {crumbs.map((c, i) => (
          <li key={c.to} aria-current={i === crumbs.length - 1 ? 'page' : undefined}>
            {i === crumbs.length - 1 ? <span>{c.label}</span> : <Link to={c.to}>{c.label}</Link>}
          </li>
        ))}
      </ol>
    </nav>
  );
}
