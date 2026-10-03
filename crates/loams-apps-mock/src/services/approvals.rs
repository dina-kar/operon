//! `loams.approvals.v1.ApprovalService`: list, get, a resumable watch and
//! decisions checked by [`crate::acceptance::check_decision`].

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::SystemTime;

use connectrpc::{
    ConnectError, ErrorCode, RequestContext, Response, ServiceRequest, ServiceResult, ServiceStream,
};
use tokio::sync::broadcast::error::RecvError;

use super::watch::heartbeat_timer;
use crate::acceptance::{Decider, check_decision};
use crate::auth::caller;
use crate::proto::loams::approvals::v1::__buffa::oneof::watch_approvals_response::Event;
use crate::proto::loams::approvals::v1::{
    Approval, ApprovalService, ApprovalSnapshot, ApprovalState, DecideApprovalRequest,
    DecideApprovalResponse, Decision, DecisionKind, GetApprovalRequest, GetApprovalResponse,
    ListApprovalsRequest, ListApprovalsResponse, WatchApprovalsRequest, WatchApprovalsResponse,
};
use crate::refuse;
use crate::seed::{time_of, ts};
use crate::store::{ApprovalChange, Store, cursor, parse_cursor};
use buffa::EnumValue;

pub(crate) struct Approvals(pub(crate) Arc<Store>);

/// Which approvals a list or watch request selects.
#[derive(Debug, Clone)]
struct Filter {
    environments: Vec<String>,
    states: Vec<EnumValue<ApprovalState>>,
}

impl Filter {
    fn new(environments: Vec<String>, states: Vec<EnumValue<ApprovalState>>) -> Self {
        let states = if states.is_empty() {
            vec![ApprovalState::APPROVAL_STATE_PENDING.into()]
        } else {
            states
        };
        Self {
            environments,
            states,
        }
    }

    fn matches(&self, approval: &Approval) -> bool {
        let env = approval.environment.as_option().map(|e| e.id.as_str());
        self.states.contains(&approval.state)
            && (self.environments.is_empty()
                || env.is_some_and(|e| self.environments.iter().any(|f| f == e)))
    }
}

impl ApprovalService for Approvals {
    async fn list_approvals(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ListApprovalsRequest>,
    ) -> ServiceResult<ListApprovalsResponse> {
        caller(&self.0.seed, &ctx)?;
        let request = request.to_owned_message();
        let filter = Filter::new(request.environments, request.states);
        let approvals = self
            .0
            .lock()
            .approvals
            .values()
            .filter(|a| filter.matches(a))
            .cloned()
            .collect();
        Response::ok(ListApprovalsResponse {
            approvals,
            ..Default::default()
        })
    }

    async fn get_approval(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, GetApprovalRequest>,
    ) -> ServiceResult<GetApprovalResponse> {
        caller(&self.0.seed, &ctx)?;
        let id = request.approval_id.to_owned();
        let approval = self.0.lock().approvals.get(&id).cloned();
        let approval =
            approval.ok_or_else(|| ConnectError::not_found(format!("no approval `{id}`")))?;
        Response::ok(GetApprovalResponse {
            approval: approval.into(),
            ..Default::default()
        })
    }

    async fn watch_approvals(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, WatchApprovalsRequest>,
    ) -> ServiceResult<ServiceStream<WatchApprovalsResponse>> {
        caller(&self.0.seed, &ctx)?;
        let request = request.to_owned_message();
        let filter = Filter::new(request.environments, request.states);
        let resume =
            parse_cursor(&request.resume_cursor).filter(|_| !request.resume_cursor.is_empty());

        // Subscribe before reading the state, under the lock, so no change
        // falls between the snapshot (or replay) and the live stream.
        let (rx, queue, last_seq) = {
            let state = self.0.lock();
            let rx = self.0.changes.subscribe();
            let mut queue = VecDeque::new();
            let replay = resume.and_then(|c| state.since(c));
            match replay {
                Some(changes) => {
                    for change in changes {
                        queue.push_back(change_event(&filter, &change));
                    }
                }
                None => {
                    let approvals = state
                        .approvals
                        .values()
                        .filter(|a| filter.matches(a))
                        .cloned()
                        .collect();
                    queue.push_back(WatchApprovalsResponse {
                        event: Some(Event::Snapshot(Box::new(ApprovalSnapshot {
                            approvals,
                            ..Default::default()
                        }))),
                        cursor: cursor(state.seq),
                        snapshot_reset: resume.is_some() || !request.resume_cursor.is_empty(),
                        ..Default::default()
                    });
                }
            }
            (rx, queue, state.seq)
        };

        let timer = heartbeat_timer(self.0.heartbeat);
        let stream = futures::stream::unfold(
            (queue, rx, timer, last_seq, filter, self.0.clone()),
            |(mut queue, mut rx, mut timer, mut last_seq, filter, store)| async move {
                loop {
                    if let Some(item) = queue.pop_front() {
                        return Some((Ok(item), (queue, rx, timer, last_seq, filter, store)));
                    }
                    tokio::select! {
                        change = rx.recv() => match change {
                            Ok(change) if change.seq > last_seq => {
                                last_seq = change.seq;
                                queue.push_back(change_event(&filter, &change));
                            }
                            Ok(_) => {}
                            Err(RecvError::Lagged(_)) => {
                                // Too far behind: send a fresh snapshot.
                                let state = store.lock();
                                last_seq = state.seq;
                                let approvals = state
                                    .approvals
                                    .values()
                                    .filter(|a| filter.matches(a))
                                    .cloned()
                                    .collect();
                                queue.push_back(WatchApprovalsResponse {
                                    event: Some(Event::Snapshot(Box::new(ApprovalSnapshot {
                                        approvals,
                                        ..Default::default()
                                    }))),
                                    cursor: cursor(last_seq),
                                    snapshot_reset: true,
                                    ..Default::default()
                                });
                            }
                            Err(RecvError::Closed) => return None,
                        },
                        _ = timer.tick() => {
                            queue.push_back(WatchApprovalsResponse {
                                event: Some(Event::Heartbeat(Box::default())),
                                cursor: cursor(last_seq),
                                ..Default::default()
                            });
                        }
                    }
                }
            },
        );
        Response::stream_ok(stream)
    }

    async fn decide_approval(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, DecideApprovalRequest>,
    ) -> ServiceResult<DecideApprovalResponse> {
        let me = caller(&self.0.seed, &ctx)?;
        let request = request.to_owned_message();
        let now = SystemTime::now();
        let (response, change) = {
            let mut state = self.0.lock();
            if !request.idempotency_key.is_empty()
                && let Some(previous) = state.decided.get(&request.idempotency_key)
            {
                return Response::ok(previous.clone());
            }
            let approval = state
                .approvals
                .get(&request.approval_id)
                .cloned()
                .ok_or_else(|| {
                    ConnectError::not_found(format!("no approval `{}`", request.approval_id))
                })?;
            let requester = approval.requested_by.as_option().map(|p| p.id.clone());
            let requester_user = requester
                .as_deref()
                .and_then(|r| self.0.seed.acts_for(r))
                .or(requester.as_deref());
            // An agent decides on behalf of its user (RFC 8693 `act`), so
            // Q432 compares the user, not the agent; `Decision.by` keeps
            // the caller itself.
            let decider_user = self
                .0
                .seed
                .acts_for(&me.principal.id)
                .unwrap_or(&me.principal.id);
            let decider = Decider {
                principal_id: decider_user,
                authenticated_at: me.authenticated_at,
            };
            check_decision(
                &approval,
                &request,
                &decider,
                requester_user,
                time_of(&approval.expires_at),
                now,
            )
            .map_err(|r| refuse(r.code, r.reason, r.message))?;
            if !request.decision_proof.is_empty() {
                // TODO(AP0 Task 5): verify the compact JWS over DecisionClaims
                // against the seed device keys (seed/keys/, test-only), with
                // `jti` single use. Until then a proof is refused, not trusted.
                return Err(refuse(
                    ErrorCode::Unimplemented,
                    "not_implemented",
                    "decision-proof verification is not implemented in loams-apps-mock yet; \
                     decide from a fresh session instead",
                ));
            }
            let mut decided = approval;
            decided.revision += 1;
            decided.decisions.push(Decision {
                by: me.principal.clone().into(),
                decision: request.decision,
                reason: request.reason.clone(),
                at: ts(now),
                ..Default::default()
            });
            decided.state =
                if request.decision.as_known() == Some(DecisionKind::DECISION_KIND_APPROVE) {
                    ApprovalState::APPROVAL_STATE_APPROVED.into()
                } else {
                    ApprovalState::APPROVAL_STATE_REJECTED.into()
                };
            let change = state.record(decided.clone());
            let response = DecideApprovalResponse {
                approval: decided.into(),
                ..Default::default()
            };
            if !request.idempotency_key.is_empty() {
                state
                    .decided
                    .insert(request.idempotency_key.clone(), response.clone());
            }
            (response, change)
        };
        let _ = self.0.changes.send(change);
        Response::ok(response)
    }
}

/// The stream event for one change: an upsert while the approval still
/// matches the filter, a remove once it leaves it.
fn change_event(filter: &Filter, change: &ApprovalChange) -> WatchApprovalsResponse {
    let event = if filter.matches(&change.approval) {
        Event::Upsert(Box::new(change.approval.clone()))
    } else {
        Event::Remove(change.approval.id.clone())
    };
    WatchApprovalsResponse {
        event: Some(event),
        cursor: cursor(change.seq),
        ..Default::default()
    }
}
