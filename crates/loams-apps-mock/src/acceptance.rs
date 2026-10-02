//! The validation rules of the app services, in one place (AP0 Task 7).
//!
//! The mock calls these, and the real server (AP4) is meant to call the same
//! functions, so the mock refuses exactly what the server will refuse (the
//! harness's `QuestionAcceptance.kt` lesson, §37 §3.2). Every refusal is a
//! Connect code plus a stable `reason` (AP0 Ruling 6).

use std::time::{Duration, SystemTime};

use connectrpc::ErrorCode;

use crate::proto::loams::approvals::v1::{
    Approval, ApprovalState, DecideApprovalRequest, DecisionKind, Risk, StepUp,
};

/// A session younger than this may decide without a device proof when the
/// policy says `STEP_UP_SESSION` (AP0 Ruling 7).
pub const STEP_UP_WINDOW: Duration = Duration::from_secs(5 * 60);

/// Why a request is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub code: ErrorCode,
    /// The stable `loams.errors.v1.ErrorInfo.reason`.
    pub reason: &'static str,
    pub message: String,
}

impl Refusal {
    fn new(code: ErrorCode, reason: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            reason,
            message: message.into(),
        }
    }
}

/// Who is deciding, as far as the decision rules care.
#[derive(Debug, Clone)]
pub struct Decider<'a> {
    pub principal_id: &'a str,
    /// When the session last authenticated the person.
    pub authenticated_at: SystemTime,
}

/// Checks a `DecideApproval` request against the approval it names.
///
/// `requester_user` is the user the request is attributed to: the requester
/// itself, or the user an agent acted for (Q432). `expires_at` is the
/// approval's deadline. The order of checks is part of the contract: a
/// stale card reports `approval_stale_revision` before anything about its
/// content.
///
/// # Errors
///
/// The first rule the request breaks.
pub fn check_decision(
    approval: &Approval,
    request: &DecideApprovalRequest,
    decider: &Decider<'_>,
    requester_user: Option<&str>,
    expires_at: SystemTime,
    now: SystemTime,
) -> Result<(), Refusal> {
    let state = approval.state.as_known();
    if state == Some(ApprovalState::APPROVAL_STATE_EXPIRED)
        || (state == Some(ApprovalState::APPROVAL_STATE_PENDING) && now >= expires_at)
    {
        return Err(Refusal::new(
            ErrorCode::FailedPrecondition,
            "approval_expired",
            "the approval expired",
        ));
    }
    if state != Some(ApprovalState::APPROVAL_STATE_PENDING) {
        return Err(Refusal::new(
            ErrorCode::FailedPrecondition,
            "approval_already_decided",
            "the approval was already decided",
        ));
    }
    if request.revision != approval.revision {
        return Err(Refusal::new(
            ErrorCode::FailedPrecondition,
            "approval_stale_revision",
            format!(
                "the approval changed (revision {} is now {}); reload it",
                request.revision, approval.revision
            ),
        ));
    }
    let decision = request.decision.as_known();
    if !matches!(
        decision,
        Some(DecisionKind::DECISION_KIND_APPROVE | DecisionKind::DECISION_KIND_REJECT)
    ) {
        return Err(Refusal::new(
            ErrorCode::InvalidArgument,
            "invalid_decision",
            "decision must be APPROVE or REJECT",
        ));
    }
    let destructive = approval.risk.as_known() == Some(Risk::RISK_DESTRUCTIVE);
    if (decision == Some(DecisionKind::DECISION_KIND_REJECT) || destructive)
        && request.reason.trim().is_empty()
    {
        return Err(Refusal::new(
            ErrorCode::InvalidArgument,
            "reason_required",
            "a reason is required to reject, and for destructive approvals",
        ));
    }
    let policy = approval.policy.as_option();
    let requester_may_approve = policy.is_some_and(|p| p.requester_may_approve);
    let requester = approval.requested_by.as_option().map(|p| p.id.as_str());
    let is_requester =
        requester == Some(decider.principal_id) || requester_user == Some(decider.principal_id);
    if is_requester && !requester_may_approve {
        return Err(Refusal::new(
            ErrorCode::PermissionDenied,
            "requester_cannot_approve",
            "you requested this operation (or an agent did for you), so someone else must decide",
        ));
    }
    let step_up = policy
        .and_then(|p| p.step_up.as_known())
        .unwrap_or(StepUp::STEP_UP_DEVICE);
    if request.decision_proof.is_empty() {
        let fresh = now
            .duration_since(decider.authenticated_at)
            .is_ok_and(|age| age < STEP_UP_WINDOW);
        let allowed = match step_up {
            StepUp::STEP_UP_NONE => true,
            StepUp::STEP_UP_SESSION => fresh,
            _ => false,
        };
        if !allowed {
            return Err(Refusal::new(
                ErrorCode::Unauthenticated,
                "step_up_required",
                "sign in again (or decide from a paired device) to decide this approval",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::seed::{Seed, time_of};

    fn setup(id: &str) -> (Approval, SystemTime, SystemTime) {
        let now = SystemTime::now();
        let seed = Seed::demo_at(now);
        let approval = seed.approvals.into_iter().find(|a| a.id == id).unwrap();
        let expires = time_of(&approval.expires_at);
        (approval, expires, now)
    }

    fn request(approval: &Approval, decision: DecisionKind, reason: &str) -> DecideApprovalRequest {
        DecideApprovalRequest {
            approval_id: approval.id.clone(),
            revision: approval.revision,
            decision: decision.into(),
            reason: reason.into(),
            ..Default::default()
        }
    }

    fn omar(at: SystemTime) -> Decider<'static> {
        Decider {
            principal_id: "usr_omar",
            authenticated_at: at,
        }
    }

    #[test]
    fn fresh_session_with_reason_approves_destructive() {
        let (a, exp, now) = setup("apr_01J9ZDROPDOCS");
        let req = request(&a, DecisionKind::DECISION_KIND_APPROVE, "cleanup ticket 42");
        assert_eq!(
            check_decision(&a, &req, &omar(now), Some("usr_dana"), exp, now),
            Ok(())
        );
    }

    #[test]
    fn decision_without_proof_is_step_up_required() {
        let (a, exp, now) = setup("apr_01J9ZDROPDOCS");
        let req = request(&a, DecisionKind::DECISION_KIND_APPROVE, "ok");
        let stale = omar(now - Duration::from_secs(600));
        let err = check_decision(&a, &req, &stale, Some("usr_dana"), exp, now).unwrap_err();
        assert_eq!(
            (err.code, err.reason),
            (ErrorCode::Unauthenticated, "step_up_required")
        );
    }

    #[test]
    fn step_up_none_accepts_a_stale_session() {
        let (a, exp, now) = setup("apr_01J9ZCREATEKEY");
        let req = request(&a, DecisionKind::DECISION_KIND_APPROVE, "");
        let stale = omar(now - Duration::from_secs(600));
        assert_eq!(
            check_decision(&a, &req, &stale, Some("usr_dana"), exp, now),
            Ok(())
        );
    }

    #[test]
    fn stale_revision_is_failed_precondition() {
        let (a, exp, now) = setup("apr_01J9ZCREATEKEY");
        let mut req = request(&a, DecisionKind::DECISION_KIND_APPROVE, "");
        req.revision = 0;
        let err = check_decision(&a, &req, &omar(now), None, exp, now).unwrap_err();
        assert_eq!(
            (err.code, err.reason),
            (ErrorCode::FailedPrecondition, "approval_stale_revision")
        );
    }

    #[test]
    fn requester_cannot_approve() {
        let (a, exp, now) = setup("apr_01J9ZDROPDOCS");
        let req = request(&a, DecisionKind::DECISION_KIND_APPROVE, "mine");
        // Dana is the user the agent acted for.
        let dana = Decider {
            principal_id: "usr_dana",
            authenticated_at: now,
        };
        let err = check_decision(&a, &req, &dana, Some("usr_dana"), exp, now).unwrap_err();
        assert_eq!(err.reason, "requester_cannot_approve");
        assert_eq!(err.code, ErrorCode::PermissionDenied);
    }

    #[test]
    fn expired_is_failed_precondition_approval_expired() {
        let (a, exp, now) = setup("apr_01J9ZEXPIRED");
        let req = request(&a, DecisionKind::DECISION_KIND_APPROVE, "late");
        let err = check_decision(&a, &req, &omar(now), None, exp, now).unwrap_err();
        assert_eq!(err.reason, "approval_expired");
        // A pending approval past its deadline is expired too.
        let (a, exp, _) = setup("apr_01J9ZCREATEKEY");
        let req = request(&a, DecisionKind::DECISION_KIND_APPROVE, "");
        let later = exp + Duration::from_secs(1);
        let err = check_decision(&a, &req, &omar(later), None, exp, later).unwrap_err();
        assert_eq!(err.reason, "approval_expired");
    }

    #[test]
    fn second_decision_is_already_decided() {
        let (mut a, exp, now) = setup("apr_01J9ZCREATEKEY");
        a.state = ApprovalState::APPROVAL_STATE_APPROVED.into();
        let req = request(&a, DecisionKind::DECISION_KIND_REJECT, "no");
        let err = check_decision(&a, &req, &omar(now), None, exp, now).unwrap_err();
        assert_eq!(err.reason, "approval_already_decided");
    }

    #[test]
    fn reject_and_destructive_need_a_reason() {
        let (a, exp, now) = setup("apr_01J9ZCREATEKEY");
        let req = request(&a, DecisionKind::DECISION_KIND_REJECT, "  ");
        let err = check_decision(&a, &req, &omar(now), None, exp, now).unwrap_err();
        assert_eq!(err.reason, "reason_required");
        let (a, exp, now) = setup("apr_01J9ZDROPDOCS");
        let req = request(&a, DecisionKind::DECISION_KIND_APPROVE, "");
        let err = check_decision(&a, &req, &omar(now), None, exp, now).unwrap_err();
        assert_eq!(err.reason, "reason_required");
    }

    #[test]
    fn unspecified_decision_is_invalid_argument() {
        let (a, exp, now) = setup("apr_01J9ZCREATEKEY");
        let req = request(&a, DecisionKind::DECISION_KIND_UNSPECIFIED, "");
        let err = check_decision(&a, &req, &omar(now), None, exp, now).unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
    }
}
