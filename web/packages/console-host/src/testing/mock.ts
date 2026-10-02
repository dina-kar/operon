// An in-memory mock of the app protos for tests and the console's demo mode
// (no server needed). It follows `loams-apps-mock`'s seed and its decision
// rules closely enough for UI work; the Rust mock (`cargo run -p
// loams-apps-mock`) is the reference the apps' conformance runs against.

import { create, type MessageInitShape } from '@bufbuild/protobuf';
import { timestampFromDate } from '@bufbuild/protobuf/wkt';
import { Code, ConnectError, createRouterTransport, type Transport } from '@connectrpc/connect';
import { approvals, devices, instance, notifications, operations } from '@loams/proto';

const { ApprovalState, DecisionKind, Risk, StepUp } = approvals;

export interface MockOptions {
  /** `GetInstance.features`; the demo turns sandboxed third-party plugins on. */
  features?: Record<string, boolean>;
  /** Which packages `GetInstance.api_versions` lists. */
  apiVersions?: string[];
  /** When the mock session authenticated (older than 5 min → step-up). */
  authenticatedAt?: Date;
  /** The signed-in principal id: usr_omar (an approver) or usr_dana. */
  principal?: 'usr_omar' | 'usr_dana';
}

export interface MockControl {
  transport: Transport;
  /** Adds a pending approval, as an agent's request would. */
  addApproval(init: MessageInitShape<typeof approvals.ApprovalSchema>): void;
  /** The current approvals, for assertions. */
  approvals(): approvals.Approval[];
}

const STEP_UP_WINDOW_MS = 5 * 60 * 1000;

function principal(id: string, name: string, kind: instance.PrincipalKind, email = '') {
  return create(instance.PrincipalSchema, { id, displayName: name, kind, email });
}

export function createMockControl(options: MockOptions = {}): MockControl {
  const now = Date.now();
  const dana = principal('usr_dana', 'Dana', instance.PrincipalKind.USER, 'dana@example.com');
  const omar = principal('usr_omar', 'Omar', instance.PrincipalKind.USER, 'omar@example.com');
  const agent = principal('agt_claude', 'Claude (for Dana)', instance.PrincipalKind.AGENT);
  const me = options.principal === 'usr_dana' ? dana : omar;
  const authenticatedAt = options.authenticatedAt ?? new Date();
  const dev = create(instance.EnvironmentSchema, {
    id: 'env_dev',
    project: 'prj_search',
    name: 'development',
    namespace: 'search-dev',
  });
  const prod = create(instance.EnvironmentSchema, {
    id: 'env_prod',
    project: 'prj_search',
    name: 'production',
    namespace: 'search-prod',
    protected: true,
  });

  const store = new Map<string, approvals.Approval>();
  const requesterUser = new Map<string, string>([['agt_claude', 'usr_dana']]);
  let seq = 0;
  const listeners = new Set<(a: approvals.Approval) => void>();
  const put = (a: approvals.Approval) => {
    store.set(a.id, a);
    seq++;
    for (const l of listeners) l(a);
  };
  const addApproval = (init: MessageInitShape<typeof approvals.ApprovalSchema>) =>
    put(create(approvals.ApprovalSchema, init));

  addApproval({
    id: 'apr_01J9ZDROPDOCS',
    revision: 1n,
    operationId: 'op-5f0c1e2d3b4a59687766554433221100',
    kind: 'collection.drop',
    environment: prod,
    requestedBy: agent,
    actorChain: [agent, dana],
    summary: 'Drop the collection docs in production',
    detailLines: [
      'Collection: docs (1 204 331 documents)',
      'Requested by Claude, acting for Dana',
      'This cannot be undone.',
    ],
    target: { name: 'docs' },
    risk: Risk.DESTRUCTIVE,
    policy: { requiredApprovals: 1, approverRoles: ['project_admin'], stepUp: StepUp.SESSION },
    state: ApprovalState.PENDING,
    createdAt: timestampFromDate(new Date(now - 120_000)),
    expiresAt: timestampFromDate(new Date(now + 72 * 3600_000)),
  });
  addApproval({
    id: 'apr_01J9ZCREATEKEY',
    revision: 1n,
    kind: 'key.create',
    environment: dev,
    requestedBy: dana,
    actorChain: [dana],
    summary: 'Create an API key for search-dev',
    detailLines: ['Scopes: collections:read, query'],
    risk: Risk.LOW,
    policy: { requiredApprovals: 1, approverRoles: ['project_admin'], stepUp: StepUp.NONE },
    state: ApprovalState.PENDING,
    createdAt: timestampFromDate(new Date(now - 30_000)),
    expiresAt: timestampFromDate(new Date(now + 72 * 3600_000)),
  });

  const fail = (code: Code, reason: string, message: string): never => {
    // Reasons ride in the message for the in-memory mock; the Rust mock
    // sends a loams.errors.v1.ErrorInfo detail.
    throw new ConnectError(`${message} [${reason}]`, code);
  };

  const apiVersions = options.apiVersions ?? [
    'loams.instance.v1',
    'loams.approvals.v1',
    'loams.operations.v1',
    'loams.devices.v1',
    'loams.notifications.v1',
  ];
  const pending = (states: approvals.ApprovalState[]) => {
    const wanted = states.length ? states : [ApprovalState.PENDING];
    return [...store.values()].filter((a) => wanted.includes(a.state));
  };

  const transport = createRouterTransport(({ service }) => {
    service(instance.InstanceService, {
      getInstance: () => ({
        instanceId: '01J9Z3MOCKINSTANCE00000000',
        name: 'Loams (in-browser mock)',
        edition: instance.Edition.OSS,
        serverVersion: '0.0.1',
        apiVersions,
        features: { billing: false, ...options.features },
        signInMethods: [{ kind: instance.SignInKind.NONE }],
      }),
      whoAmI: () => ({
        principal: me,
        org: { id: 'org_acme', name: 'Acme' },
        environments: [dev, prod],
        authenticatedAt: timestampFromDate(authenticatedAt),
      }),
    });
    service(approvals.ApprovalService, {
      listApprovals: (req) => ({ approvals: pending(req.states) }),
      getApproval: (req) => {
        const a = store.get(req.approvalId);
        if (!a) return fail(Code.NotFound, 'not_found', `no approval ${req.approvalId}`);
        return { approval: a };
      },
      async *watchApprovals(req, context) {
        const queue: approvals.Approval[] = [];
        let wake: (() => void) | undefined;
        const listener = (a: approvals.Approval) => {
          queue.push(a);
          wake?.();
        };
        listeners.add(listener);
        try {
          yield {
            event: { case: 'snapshot', value: { approvals: pending(req.states) } },
            cursor: `c${seq}`,
          };
          while (!context.signal.aborted) {
            const next = queue.shift();
            if (next) {
              const matches = (req.states.length ? req.states : [ApprovalState.PENDING]).includes(
                next.state,
              );
              yield matches
                ? { event: { case: 'upsert', value: next }, cursor: `c${seq}` }
                : { event: { case: 'remove', value: next.id }, cursor: `c${seq}` };
              continue;
            }
            await new Promise<void>((resolve) => {
              wake = resolve;
              context.signal.addEventListener('abort', () => resolve(), { once: true });
            });
            wake = undefined;
          }
        } finally {
          listeners.delete(listener);
        }
      },
      decideApproval: (req) => {
        const a = store.get(req.approvalId);
        if (!a) return fail(Code.NotFound, 'not_found', `no approval ${req.approvalId}`);
        if (a.state !== ApprovalState.PENDING) {
          return fail(Code.FailedPrecondition, 'approval_already_decided', 'already decided');
        }
        if (req.revision !== a.revision) {
          return fail(Code.FailedPrecondition, 'approval_stale_revision', 'the approval changed');
        }
        if (
          (req.decision === DecisionKind.REJECT || a.risk === Risk.DESTRUCTIVE) &&
          !req.reason.trim()
        ) {
          return fail(Code.InvalidArgument, 'reason_required', 'a reason is required');
        }
        const requester = a.requestedBy?.id ?? '';
        const user = requesterUser.get(requester) ?? requester;
        if ((requester === me.id || user === me.id) && !a.policy?.requesterMayApprove) {
          return fail(
            Code.PermissionDenied,
            'requester_cannot_approve',
            'you requested this, someone else must decide',
          );
        }
        const fresh = Date.now() - authenticatedAt.getTime() < STEP_UP_WINDOW_MS;
        if (
          !req.decisionProof &&
          !(a.policy?.stepUp === StepUp.NONE || (fresh && a.policy?.stepUp === StepUp.SESSION))
        ) {
          return fail(Code.Unauthenticated, 'step_up_required', 'sign in again to decide');
        }
        const decided = create(approvals.ApprovalSchema, {
          ...a,
          revision: a.revision + 1n,
          state:
            req.decision === DecisionKind.APPROVE ? ApprovalState.APPROVED : ApprovalState.REJECTED,
          decisions: [
            ...a.decisions,
            create(approvals.DecisionSchema, {
              by: me,
              decision: req.decision,
              reason: req.reason,
              at: timestampFromDate(new Date()),
            }),
          ],
        });
        put(decided);
        return { approval: decided };
      },
    });
    service(operations.OperationsService, {
      getOperation: () => fail(Code.NotFound, 'not_found', 'no such operation'),
      listOperations: () => ({ operations: [] }),
      async *watchOperations() {
        yield { event: { case: 'snapshot', value: { operations: [] } }, cursor: 'c0' };
      },
      cancelOperation: () => fail(Code.Unimplemented, 'not_implemented', 'not in the mock'),
    });
    service(devices.DeviceService, {
      createPairing: () => fail(Code.Unimplemented, 'not_implemented', 'use loams-apps-mock'),
      listDevices: () => ({ devices: [] }),
      renameDevice: () => fail(Code.Unimplemented, 'not_implemented', 'not in the mock'),
      revokeDevice: () => fail(Code.Unimplemented, 'not_implemented', 'not in the mock'),
      registerPushTarget: () => fail(Code.Unimplemented, 'not_implemented', 'not in the mock'),
      unregisterPushTarget: () => fail(Code.Unimplemented, 'not_implemented', 'not in the mock'),
      getNotificationPreferences: () =>
        fail(Code.Unimplemented, 'not_implemented', 'not in the mock'),
      setNotificationPreferences: () =>
        fail(Code.Unimplemented, 'not_implemented', 'not in the mock'),
      sendTestNotification: () => fail(Code.Unimplemented, 'not_implemented', 'not in the mock'),
    });
    service(notifications.NotificationService, {
      listNotifications: () => ({ notifications: [] }),
      async *watchNotifications() {
        yield { event: { case: 'snapshot', value: { notifications: [] } }, cursor: 'c0' };
      },
      markRead: () => ({ marked: 0 }),
    });
  });

  return { transport, addApproval, approvals: () => [...store.values()] };
}

/** Just the transport, for the demo mode. */
export function createMockTransport(options?: MockOptions): Transport {
  return createMockControl(options).transport;
}
