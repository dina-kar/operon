import '@fontsource-variable/archivo/standard.css';
import '@fontsource-variable/martian-mono';
import '@loam/ui/styles.css';
import './app.css';

import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { createBrowserRouter, Outlet } from 'react-router';
import { RouterProvider } from 'react-router/dom';
import { AgentPage, AgentsPage } from './pages/agents';
import { ConsentPage, NotFound, SetupPage, SignInPage } from './pages/auth';
import { EnvironmentPage } from './pages/environment';
import { Home } from './pages/home';
import { AccessPage, AuditPage, MembersPage, SettingsPage, TeamPage, TeamsPage } from './pages/org';
import { ProjectPage } from './pages/project';
import { SessionGate } from './session';
import { Shell } from './shell';

// The engine serves the console at /ui with an SPA fallback (design §19 §3).
const router = createBrowserRouter(
  [
    { path: '/sign-in', element: <SignInPage /> },
    { path: '/setup', element: <SetupPage /> },
    { path: '/consent', element: <ConsentPage /> },
    {
      element: (
        <SessionGate>
          <Outlet />
        </SessionGate>
      ),
      children: [
        {
          element: <Shell />,
          children: [
            { index: true, element: <Home /> },
            { path: 'projects/:project', element: <ProjectPage /> },
            { path: 'projects/:project/environments/:environment', element: <EnvironmentPage /> },
            { path: 'projects/:project/agents', element: <AgentsPage /> },
            { path: 'projects/:project/agents/:agent', element: <AgentPage /> },
            { path: 'projects/:project/access', element: <AccessPage /> },
            { path: 'teams', element: <TeamsPage /> },
            { path: 'teams/:team', element: <TeamPage /> },
            { path: 'members', element: <MembersPage /> },
            { path: 'audit', element: <AuditPage /> },
            { path: 'settings', element: <SettingsPage /> },
            { path: '*', element: <NotFound /> },
          ],
        },
      ],
    },
  ],
  { basename: '/ui' },
);

const root = document.getElementById('root');
if (root)
  createRoot(root).render(
    <StrictMode>
      <RouterProvider router={router} />
    </StrictMode>,
  );
