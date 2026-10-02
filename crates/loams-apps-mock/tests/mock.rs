//! The mock over real sockets, through the generated connect-rust clients,
//! on every protocol (AP0 Task 5).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use connectrpc::client::{CallOptions, ClientConfig, HttpClient};
use connectrpc::{ConnectError, ErrorCode, Protocol};
use loams_apps_mock::proto::loams::approvals::v1::__buffa::oneof::watch_approvals_response::Event;
use loams_apps_mock::proto::loams::approvals::v1::{
    ApprovalServiceClient, ApprovalState, DecideApprovalRequest, DecisionKind, GetApprovalRequest,
    ListApprovalsRequest, WatchApprovalsRequest,
};
use loams_apps_mock::proto::loams::devices::v1::{
    CreatePairingRequest, DeviceServiceClient, ListDevicesRequest, RegisterPushTargetRequest,
};
use loams_apps_mock::proto::loams::errors::v1::ErrorInfo;
use loams_apps_mock::proto::loams::instance::v1::{
    Edition, GetInstanceRequest, InstanceServiceClient, WhoAmIRequest,
};
use loams_apps_mock::proto::loams::notifications::v1::{
    ListNotificationsRequest, MarkReadRequest, NotificationServiceClient,
};
use loams_apps_mock::proto::loams::operations::v1::{
    ListOperationsRequest, OperationsServiceClient,
};
use loams_apps_mock::{MockConfig, MockHandle, Seed, serve};

const OMAR: &str = "Bearer mock-access-usr_omar";
const OMAR_STALE: &str = "Bearer mock-stale-usr_omar";
const DANA: &str = "Bearer mock-access-usr_dana";
const DROP_DOCS: &str = "apr_01J9ZDROPDOCS";
const CREATE_KEY: &str = "apr_01J9ZCREATEKEY";

async fn start(heartbeat: Duration) -> MockHandle {
    serve(MockConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        seed: Seed::demo(),
        heartbeat,
    })
    .await
    .unwrap()
}

fn config(mock: &MockHandle, protocol: Protocol) -> ClientConfig {
    ClientConfig::new(mock.url().parse().unwrap()).with_protocol(protocol)
}

fn http(protocol: Protocol) -> HttpClient {
    if protocol == Protocol::Grpc {
        HttpClient::plaintext_http2_only()
    } else {
        HttpClient::plaintext()
    }
}

fn auth(token: &str) -> CallOptions {
    CallOptions::default().with_header("authorization", token)
}

fn reason(err: &ConnectError) -> String {
    use buffa::Message as _;
    use connectrpc::ErrorDetail;
    let detail: &ErrorDetail = err.details.first().expect("an ErrorInfo detail");
    assert_eq!(detail.type_url, "loams.errors.v1.ErrorInfo");
    let bytes = base64_decode(detail.value.as_deref().unwrap());
    ErrorInfo::decode_from_slice(&bytes).unwrap().reason
}

fn base64_decode(s: &str) -> Vec<u8> {
    // Unpadded standard alphabet (the Connect error-detail encoding).
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = Vec::new();
    let mut buf = 0u32;
    let mut bits = 0;
    for c in s.bytes().filter(|c| *c != b'=') {
        let v = ALPHABET.iter().position(|a| *a == c).unwrap() as u32;
        buf = (buf << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
            buf &= (1 << bits) - 1;
        }
    }
    out
}

#[tokio::test]
async fn every_protocol_serves_get_instance() {
    let mock = start(Duration::from_secs(15)).await;
    for protocol in [Protocol::Connect, Protocol::Grpc, Protocol::GrpcWeb] {
        let client = InstanceServiceClient::new(http(protocol), config(&mock, protocol));
        let info = client
            .get_instance(GetInstanceRequest::default())
            .await
            .unwrap_or_else(|e| panic!("{protocol:?}: {e}"))
            .into_owned();
        assert_eq!(info.edition.as_known(), Some(Edition::EDITION_OSS));
        assert!(info.api_versions.iter().any(|v| v == "loams.approvals.v1"));
    }
    // JSON over Connect too.
    let client = InstanceServiceClient::new(
        HttpClient::plaintext(),
        config(&mock, Protocol::Connect).json(),
    );
    let info = client
        .get_instance(GetInstanceRequest::default())
        .await
        .unwrap()
        .into_owned();
    assert_eq!(info.name, "Loams (mock)");
    mock.stop().await;
}

#[tokio::test]
async fn who_am_i_needs_a_token_and_reports_the_actor_chain() {
    let mock = start(Duration::from_secs(15)).await;
    let client =
        InstanceServiceClient::new(HttpClient::plaintext(), config(&mock, Protocol::Connect));
    let err = client.who_am_i(WhoAmIRequest::default()).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::Unauthenticated);
    let me = client
        .who_am_i_with_options(
            WhoAmIRequest::default(),
            auth("Bearer mock-access-agt_claude"),
        )
        .await
        .unwrap()
        .into_owned();
    assert_eq!(me.principal.as_option().unwrap().id, "usr_dana");
    assert_eq!(me.actor_chain[0].id, "agt_claude");
    mock.stop().await;
}

#[tokio::test]
async fn approval_decides_once_with_a_fresh_session() {
    let mock = start(Duration::from_secs(15)).await;
    let client =
        ApprovalServiceClient::new(HttpClient::plaintext(), config(&mock, Protocol::Connect));
    let pending = client
        .list_approvals_with_options(ListApprovalsRequest::default(), auth(OMAR))
        .await
        .unwrap()
        .into_owned();
    assert_eq!(
        pending.approvals.len(),
        2,
        "expired approvals are not pending"
    );

    let request = DecideApprovalRequest {
        approval_id: CREATE_KEY.into(),
        revision: 1,
        decision: DecisionKind::DECISION_KIND_APPROVE.into(),
        idempotency_key: "01J9ZIDEMPOTENT".into(),
        ..Default::default()
    };
    let decided = client
        .decide_approval_with_options(request.clone(), auth(OMAR))
        .await
        .unwrap()
        .into_owned();
    let approval = decided.approval.as_option().unwrap();
    assert_eq!(
        approval.state.as_known(),
        Some(ApprovalState::APPROVAL_STATE_APPROVED)
    );
    assert_eq!(approval.revision, 2);

    // A retry with the same idempotency key returns the same answer.
    let again = client
        .decide_approval_with_options(request.clone(), auth(OMAR))
        .await
        .unwrap()
        .into_owned();
    assert_eq!(again.approval.as_option().unwrap().revision, 2);

    // A new decision is refused.
    let err = client
        .decide_approval_with_options(
            DecideApprovalRequest {
                idempotency_key: "01J9ZOTHER".into(),
                revision: 2,
                ..request
            },
            auth(OMAR),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::FailedPrecondition);
    assert_eq!(reason(&err), "approval_already_decided");
    mock.stop().await;
}

#[tokio::test]
async fn stale_session_requires_step_up_and_requester_cannot_approve() {
    let mock = start(Duration::from_secs(15)).await;
    let client =
        ApprovalServiceClient::new(HttpClient::plaintext(), config(&mock, Protocol::Connect));
    let request = DecideApprovalRequest {
        approval_id: DROP_DOCS.into(),
        revision: 1,
        decision: DecisionKind::DECISION_KIND_APPROVE.into(),
        reason: "ticket 42".into(),
        ..Default::default()
    };
    let err = client
        .decide_approval_with_options(request.clone(), auth(OMAR_STALE))
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::Unauthenticated);
    assert_eq!(reason(&err), "step_up_required");

    let err = client
        .decide_approval_with_options(request.clone(), auth(DANA))
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::PermissionDenied);
    assert_eq!(reason(&err), "requester_cannot_approve");

    // An agent acting for Dana cannot approve Dana's own request either.
    let err = client
        .decide_approval_with_options(
            DecideApprovalRequest {
                approval_id: CREATE_KEY.into(),
                revision: 1,
                decision: DecisionKind::DECISION_KIND_APPROVE.into(),
                ..Default::default()
            },
            auth("Bearer mock-access-agt_claude"),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::PermissionDenied);
    assert_eq!(reason(&err), "requester_cannot_approve");
    let key = client
        .get_approval_with_options(
            GetApprovalRequest {
                approval_id: CREATE_KEY.into(),
                ..Default::default()
            },
            auth(OMAR),
        )
        .await
        .unwrap()
        .into_owned();
    let key = key.approval.as_option().unwrap();
    assert_eq!(key.requested_by.as_option().unwrap().id, "usr_dana");
    assert_eq!(
        key.state.as_known(),
        Some(ApprovalState::APPROVAL_STATE_PENDING)
    );

    let err = client
        .decide_approval_with_options(
            DecideApprovalRequest {
                decision_proof: "eyJhbGciOiJFUzI1NiJ9.e30.sig".into(),
                ..request.clone()
            },
            auth(OMAR_STALE),
        )
        .await
        .unwrap_err();
    assert_eq!(
        reason(&err),
        "not_implemented",
        "proofs are refused, never trusted"
    );

    let still = client
        .get_approval_with_options(
            GetApprovalRequest {
                approval_id: DROP_DOCS.into(),
                ..Default::default()
            },
            auth(OMAR),
        )
        .await
        .unwrap()
        .into_owned();
    assert_eq!(
        still.approval.as_option().unwrap().state.as_known(),
        Some(ApprovalState::APPROVAL_STATE_PENDING)
    );
    mock.stop().await;
}

/// The next stream message, owned, within 5 s.
macro_rules! next {
    ($stream:expr) => {
        tokio::time::timeout(Duration::from_secs(5), $stream.message())
            .await
            .expect("a stream message within 5 s")
            .unwrap()
            .expect("the stream is open")
            .to_owned_message()
    };
}

#[tokio::test]
async fn watch_sends_snapshot_changes_and_heartbeats() {
    let mock = start(Duration::from_millis(200)).await;
    let client =
        ApprovalServiceClient::new(HttpClient::plaintext(), config(&mock, Protocol::Connect));
    let mut stream = client
        .watch_approvals_with_options(WatchApprovalsRequest::default(), auth(OMAR))
        .await
        .unwrap();
    let first = next!(stream);
    let Some(Event::Snapshot(snapshot)) = first.event else {
        panic!("expected a snapshot first, got {first:?}");
    };
    assert_eq!(snapshot.approvals.len(), 2);
    let cursor = first.cursor.clone();

    client
        .decide_approval_with_options(
            DecideApprovalRequest {
                approval_id: CREATE_KEY.into(),
                revision: 1,
                decision: DecisionKind::DECISION_KIND_REJECT.into(),
                reason: "not now".into(),
                ..Default::default()
            },
            auth(OMAR),
        )
        .await
        .unwrap();
    // Heartbeats may come first; the decision arrives as a remove (it left
    // the PENDING filter).
    let removed = loop {
        let msg = next!(stream);
        match msg.event {
            Some(Event::Heartbeat(_)) => continue,
            Some(Event::Remove(id)) => break (id, msg.cursor),
            other => panic!("unexpected {other:?}"),
        }
    };
    assert_eq!(removed.0, CREATE_KEY);
    assert_ne!(removed.1, cursor);
    let beat = next!(stream);
    assert!(matches!(beat.event, Some(Event::Heartbeat(_))));
    drop(stream);

    // Resuming from the snapshot's cursor replays the missed change only.
    let mut resumed = client
        .watch_approvals_with_options(
            WatchApprovalsRequest {
                resume_cursor: cursor,
                ..Default::default()
            },
            auth(OMAR),
        )
        .await
        .unwrap();
    let replay = next!(resumed);
    assert!(!replay.snapshot_reset);
    assert!(matches!(replay.event, Some(Event::Remove(ref id)) if id == CREATE_KEY));

    // An unknown cursor resets to a fresh snapshot.
    let mut reset = client
        .watch_approvals_with_options(
            WatchApprovalsRequest {
                resume_cursor: "c999".into(),
                ..Default::default()
            },
            auth(OMAR),
        )
        .await
        .unwrap();
    let fresh = next!(reset);
    assert!(fresh.snapshot_reset);
    assert!(matches!(fresh.event, Some(Event::Snapshot(ref s)) if s.approvals.len() == 1));
    mock.stop().await;
}

#[tokio::test]
async fn devices_operations_and_inbox() {
    let mock = start(Duration::from_secs(15)).await;
    let devices =
        DeviceServiceClient::new(HttpClient::plaintext(), config(&mock, Protocol::Connect));
    let pairing = devices
        .create_pairing_with_options(CreatePairingRequest::default(), auth(OMAR))
        .await
        .unwrap()
        .into_owned();
    let qr: serde_json::Value = serde_json::from_str(&pairing.qr_payload).unwrap();
    assert_eq!(qr["v"], 1);
    assert_eq!(qr["user_code"], pairing.user_code);
    let mine = devices
        .list_devices_with_options(ListDevicesRequest::default(), auth(OMAR))
        .await
        .unwrap()
        .into_owned();
    assert_eq!(mine.devices.len(), 1);
    let err = devices
        .register_push_target_with_options(RegisterPushTargetRequest::default(), auth(OMAR))
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::Unimplemented);
    assert_eq!(reason(&err), "not_implemented");

    let operations =
        OperationsServiceClient::new(HttpClient::plaintext(), config(&mock, Protocol::Connect));
    let ops = operations
        .list_operations_with_options(ListOperationsRequest::default(), auth(OMAR))
        .await
        .unwrap()
        .into_owned();
    assert_eq!(ops.operations.len(), 2);

    let inbox =
        NotificationServiceClient::new(HttpClient::plaintext(), config(&mock, Protocol::Connect));
    let marked = inbox
        .mark_read_with_options(
            MarkReadRequest {
                all: true,
                ..Default::default()
            },
            auth(OMAR),
        )
        .await
        .unwrap()
        .into_owned();
    assert_eq!(marked.marked, 1);
    let unread = inbox
        .list_notifications_with_options(
            ListNotificationsRequest {
                unread_only: true,
                ..Default::default()
            },
            auth(OMAR),
        )
        .await
        .unwrap()
        .into_owned();
    assert!(unread.notifications.is_empty());
    mock.stop().await;
}

#[tokio::test]
async fn non_loopback_is_refused() {
    let err = serve(MockConfig {
        listen: "0.0.0.0:0".parse().unwrap(),
        ..MockConfig::default()
    })
    .await
    .unwrap_err();
    assert!(err.to_string().contains("loopback"), "{err}");
}
