//! Seed data: one org, two environments (one protected), three principals
//! (two people and an agent acting for one of them), pending approvals, a
//! running operation, an inbox and a paired phone. The same story every
//! app's screenshots and conformance scenarios use (§37 §2.1's personas).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use buffa::MessageField;
use buffa_types::google::protobuf::Timestamp;

use crate::proto::loams::approvals::v1::{Approval, ApprovalPolicy, ApprovalState, Risk, StepUp};
use crate::proto::loams::devices::v1::{Device, NotificationCategory, Platform};
use crate::proto::loams::instance::v1::{
    Edition, Environment, GetInstanceResponse, Org, Principal, PrincipalKind, SignInKind,
    SignInMethod,
};
use crate::proto::loams::notifications::v1::__buffa::oneof::notification::Ref;
use crate::proto::loams::notifications::v1::Notification;
use crate::proto::loams::operations::v1::{Operation, OperationState, Progress};

/// The proto packages the mock serves, as `GetInstance.api_versions` lists them.
pub const API_VERSIONS: [&str; 5] = [
    "loams.instance.v1",
    "loams.devices.v1",
    "loams.approvals.v1",
    "loams.operations.v1",
    "loams.notifications.v1",
];

/// Everything the mock starts with.
#[derive(Debug, Clone)]
pub struct Seed {
    /// The `GetInstance` answer.
    pub instance: GetInstanceResponse,
    pub org: Org,
    pub principals: Vec<Principal>,
    /// Agent id → the user it acts for (the RFC 8693 `act` chain).
    pub acts_for: Vec<(String, String)>,
    pub environments: Vec<Environment>,
    pub approvals: Vec<Approval>,
    pub operations: Vec<Operation>,
    pub notifications: Vec<Notification>,
    /// Owner principal id → device.
    pub devices: Vec<(String, Device)>,
    /// Principals whose tokens answer `device_revoked`.
    pub revoked_principals: Vec<String>,
}

impl Seed {
    /// The default seed, anchored at the current time.
    #[must_use]
    pub fn demo() -> Self {
        Self::demo_at(SystemTime::now())
    }

    /// The default seed with every timestamp relative to `now`.
    #[must_use]
    pub fn demo_at(now: SystemTime) -> Self {
        let dana = principal(
            "usr_dana",
            PrincipalKind::PRINCIPAL_KIND_USER,
            "Dana",
            "dana@example.com",
        );
        let omar = principal(
            "usr_omar",
            PrincipalKind::PRINCIPAL_KIND_USER,
            "Omar",
            "omar@example.com",
        );
        let agent = principal(
            "agt_claude",
            PrincipalKind::PRINCIPAL_KIND_AGENT,
            "Claude (for Dana)",
            "",
        );
        let dev = environment("env_dev", "prj_search", "development", "search-dev", false);
        let prod = environment("env_prod", "prj_search", "production", "search-prod", true);

        let drop_docs = Approval {
            id: "apr_01J9ZDROPDOCS".into(),
            revision: 1,
            operation_id: "op-5f0c1e2d3b4a59687766554433221100".into(),
            promise_id: "prm_drop_docs".into(),
            kind: "collection.drop".into(),
            environment: MessageField::some(prod.clone()),
            requested_by: MessageField::some(agent.clone()),
            actor_chain: vec![agent.clone(), dana.clone()],
            summary: "Drop the collection docs in production".into(),
            detail_lines: vec![
                "Collection: docs (1 204 331 documents)".into(),
                "Requested by Claude, acting for Dana".into(),
                "This cannot be undone.".into(),
            ],
            target: [("name".to_owned(), "docs".to_owned())]
                .into_iter()
                .collect(),
            risk: Risk::RISK_DESTRUCTIVE.into(),
            policy: MessageField::some(ApprovalPolicy {
                required_approvals: 1,
                approver_roles: vec!["project_admin".into()],
                requester_may_approve: false,
                step_up: StepUp::STEP_UP_SESSION.into(),
                ..Default::default()
            }),
            state: ApprovalState::APPROVAL_STATE_PENDING.into(),
            created_at: ts(now - Duration::from_secs(120)),
            expires_at: ts(now + Duration::from_secs(72 * 3600)),
            ..Default::default()
        };
        let create_key = Approval {
            id: "apr_01J9ZCREATEKEY".into(),
            revision: 1,
            operation_id: "op-0a1b2c3d4e5f60718293a4b5c6d7e8f9".into(),
            promise_id: "prm_create_key".into(),
            kind: "key.create".into(),
            environment: MessageField::some(dev.clone()),
            requested_by: MessageField::some(dana.clone()),
            actor_chain: vec![dana.clone()],
            summary: "Create an API key for search-dev".into(),
            detail_lines: vec!["Scopes: collections:read, query".into()],
            risk: Risk::RISK_LOW.into(),
            policy: MessageField::some(ApprovalPolicy {
                required_approvals: 1,
                approver_roles: vec!["project_admin".into()],
                requester_may_approve: false,
                step_up: StepUp::STEP_UP_NONE.into(),
                ..Default::default()
            }),
            state: ApprovalState::APPROVAL_STATE_PENDING.into(),
            created_at: ts(now - Duration::from_secs(30)),
            expires_at: ts(now + Duration::from_secs(72 * 3600)),
            ..Default::default()
        };
        let expired = Approval {
            id: "apr_01J9ZEXPIRED".into(),
            revision: 2,
            kind: "restore".into(),
            environment: MessageField::some(prod.clone()),
            requested_by: MessageField::some(dana.clone()),
            summary: "Restore search-prod to yesterday 18:00".into(),
            risk: Risk::RISK_HIGH.into(),
            policy: MessageField::some(ApprovalPolicy {
                required_approvals: 1,
                step_up: StepUp::STEP_UP_SESSION.into(),
                ..Default::default()
            }),
            state: ApprovalState::APPROVAL_STATE_EXPIRED.into(),
            created_at: ts(now - Duration::from_secs(80 * 3600)),
            expires_at: ts(now - Duration::from_secs(8 * 3600)),
            ..Default::default()
        };

        let import = Operation {
            id: "op-1122334455667788990011223344556".into(),
            kind: "collection.import".into(),
            namespace: dev.namespace.clone(),
            target: [("collection".to_owned(), "articles".to_owned())]
                .into_iter()
                .collect(),
            state: OperationState::OPERATION_STATE_RUNNING.into(),
            progress: MessageField::some(Progress {
                done: 41_000,
                total: 100_000,
                unit: "documents".into(),
                phase: "indexing".into(),
                ..Default::default()
            }),
            created_at: ts(now - Duration::from_secs(300)),
            updated_at: ts(now),
            ..Default::default()
        };
        let awaiting = Operation {
            id: drop_docs.operation_id.clone(),
            kind: "collection.drop".into(),
            namespace: prod.namespace.clone(),
            target: [("collection".to_owned(), "docs".to_owned())]
                .into_iter()
                .collect(),
            state: OperationState::OPERATION_STATE_AWAITING_APPROVAL.into(),
            approval_id: drop_docs.id.clone(),
            created_at: ts(now - Duration::from_secs(120)),
            updated_at: ts(now - Duration::from_secs(120)),
            ..Default::default()
        };

        let notification = Notification {
            id: "01J9ZNOTE0000000000000001".into(),
            category: NotificationCategory::NOTIFICATION_CATEGORY_APPROVALS.into(),
            cloudevent_type: "io.loams.dev.approval.requested.v1".into(),
            subject: drop_docs.id.clone(),
            title: "Approval needed".into(),
            body: drop_docs.summary.clone(),
            r#ref: Some(Ref::ApprovalId(drop_docs.id.clone())),
            environment: prod.id.clone(),
            created_at: ts(now - Duration::from_secs(120)),
            ..Default::default()
        };

        let phone = Device {
            id: "dev_01J9ZPHONE".into(),
            name: "Omar's iPhone".into(),
            platform: Platform::PLATFORM_IOS.into(),
            model: "iPhone16,2".into(),
            app_version: "0.1.0".into(),
            created_at: ts(now - Duration::from_secs(7 * 86_400)),
            last_seen_at: ts(now - Duration::from_secs(3_600)),
            decision_key_thumbprint: "NzbLsXh8uDCcd-6MNwXF4W_7noWXFZAfHkxZsRGC9Xs".into(),
            ..Default::default()
        };

        let instance = GetInstanceResponse {
            instance_id: "01J9Z3MOCKINSTANCE00000000".into(),
            name: "Loams (mock)".into(),
            edition: Edition::EDITION_OSS.into(),
            server_version: env!("CARGO_PKG_VERSION").into(),
            api_versions: API_VERSIONS.iter().map(|s| (*s).to_owned()).collect(),
            features: [("billing".to_owned(), false), ("passkeys".to_owned(), true)]
                .into_iter()
                .collect(),
            // Placeholders: the mock has no token endpoint yet (the
            // pairing grant and RFC 8693 exchange belong to the auth plan).
            issuer: "https://mock.loams.invalid".into(),
            jwks_uri: "https://mock.loams.invalid/.well-known/jwks.json".into(),
            sign_in_methods: vec![SignInMethod {
                kind: SignInKind::SIGN_IN_KIND_AUTHENTIK.into(),
                issuer: "https://authentik.mock.loams.invalid/application/o/loams/".into(),
                client_id: "loams-desktop".into(),
                display_name: "Sign in with Authentik".into(),
                ..Default::default()
            }],
            min_app_versions: [
                ("ios".to_owned(), "0.1.0".to_owned()),
                ("android".to_owned(), "0.1.0".to_owned()),
                ("desktop".to_owned(), "0.1.0".to_owned()),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        };

        Self {
            instance,
            org: Org {
                id: "org_acme".into(),
                name: "Acme".into(),
                ..Default::default()
            },
            principals: vec![dana, omar.clone(), agent],
            acts_for: vec![("agt_claude".into(), "usr_dana".into())],
            environments: vec![dev, prod],
            approvals: vec![drop_docs, create_key, expired],
            operations: vec![import, awaiting],
            notifications: vec![notification],
            devices: vec![(omar.id, phone)],
            revoked_principals: Vec::new(),
        }
    }

    /// A seed principal by id.
    #[must_use]
    pub fn principal(&self, id: &str) -> Option<Principal> {
        self.principals.iter().find(|p| p.id == id).cloned()
    }

    /// The user an agent acts for, if `id` is an agent in the seed.
    #[must_use]
    pub fn acts_for(&self, id: &str) -> Option<&str> {
        self.acts_for
            .iter()
            .find(|(agent, _)| agent == id)
            .map(|(_, user)| user.as_str())
    }
}

fn principal(id: &str, kind: PrincipalKind, name: &str, email: &str) -> Principal {
    Principal {
        id: id.into(),
        kind: kind.into(),
        display_name: name.into(),
        email: email.into(),
        ..Default::default()
    }
}

fn environment(
    id: &str,
    project: &str,
    name: &str,
    namespace: &str,
    protected: bool,
) -> Environment {
    Environment {
        id: id.into(),
        project: project.into(),
        name: name.into(),
        namespace: namespace.into(),
        protected,
        ..Default::default()
    }
}

/// A protobuf timestamp field from a `SystemTime`.
pub(crate) fn ts(at: SystemTime) -> MessageField<Timestamp, buffa::Inline<Timestamp>> {
    let since = at.duration_since(UNIX_EPOCH).unwrap_or_default();
    MessageField::some(Timestamp {
        seconds: i64::try_from(since.as_secs()).unwrap_or(i64::MAX),
        nanos: i32::try_from(since.subsec_nanos()).unwrap_or(0),
        ..Default::default()
    })
}

/// A `SystemTime` from a protobuf timestamp field (the epoch when unset).
pub(crate) fn time_of(field: &MessageField<Timestamp, buffa::Inline<Timestamp>>) -> SystemTime {
    field.as_option().map_or(UNIX_EPOCH, |t| {
        UNIX_EPOCH
            + Duration::from_secs(u64::try_from(t.seconds).unwrap_or(0))
            + Duration::from_nanos(u64::try_from(t.nanos).unwrap_or(0))
    })
}
