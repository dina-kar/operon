//! The service implementations. Each is a thin layer over [`Store`]; the
//! decision rules live in [`crate::acceptance`].

// The generated traits return `impl Encodable<_>`; these impls name the
// concrete message type, which is the intended refinement.
#![allow(refining_impl_trait)]

mod approvals;
mod devices;
mod watch;

use std::sync::Arc;

use connectrpc::{
    ConnectError, RequestContext, Response, ServiceRequest, ServiceResult, ServiceStream,
};

use crate::auth::caller;
use crate::not_implemented;
use crate::proto::loams::instance::v1::{
    DeviceRef, GetInstanceRequest, GetInstanceResponse, InstanceService, WhoAmIRequest,
    WhoAmIResponse,
};
use crate::proto::loams::notifications::v1::__buffa::oneof::watch_notifications_response::Event as NotificationEvent;
use crate::proto::loams::notifications::v1::{
    ListNotificationsRequest, ListNotificationsResponse, MarkReadRequest, MarkReadResponse,
    NotificationService, NotificationSnapshot, WatchNotificationsRequest,
    WatchNotificationsResponse,
};
use crate::proto::loams::operations::v1::__buffa::oneof::watch_operations_response::Event as OperationEvent;
use crate::proto::loams::operations::v1::{
    CancelOperationRequest, CancelOperationResponse, GetOperationRequest, GetOperationResponse,
    ListOperationsRequest, ListOperationsResponse, OperationSnapshot, OperationsService,
    WatchOperationsRequest, WatchOperationsResponse,
};
use crate::seed::ts;
use crate::store::Store;

pub(crate) use approvals::Approvals;
pub(crate) use devices::Devices;

/// `loams.instance.v1.InstanceService`.
pub(crate) struct Instance(pub(crate) Arc<Store>);

impl InstanceService for Instance {
    async fn get_instance(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, GetInstanceRequest>,
    ) -> ServiceResult<GetInstanceResponse> {
        Response::ok(self.0.seed.instance.clone())
    }

    async fn who_am_i(
        &self,
        ctx: RequestContext,
        _request: ServiceRequest<'_, WhoAmIRequest>,
    ) -> ServiceResult<WhoAmIResponse> {
        let seed = &self.0.seed;
        let me = caller(seed, &ctx)?;
        let mut actor_chain = Vec::new();
        let mut principal = me.principal.clone();
        if let Some(user) = seed
            .acts_for(&me.principal.id)
            .and_then(|u| seed.principal(u))
        {
            actor_chain.push(me.principal.clone());
            principal = user;
        }
        let device = {
            let state = self.0.lock();
            state
                .devices
                .values()
                .find(|(owner, _)| *owner == principal.id)
                .map(|(_, d)| DeviceRef {
                    id: d.id.clone(),
                    name: d.name.clone(),
                    ..Default::default()
                })
        };
        Response::ok(WhoAmIResponse {
            principal: principal.into(),
            actor_chain,
            org: seed.org.clone().into(),
            environments: seed.environments.clone(),
            device: device.map(Into::into).unwrap_or_default(),
            authenticated_at: ts(me.authenticated_at),
            ..Default::default()
        })
    }
}

/// `loams.operations.v1.OperationsService`.
pub(crate) struct Operations(pub(crate) Arc<Store>);

impl OperationsService for Operations {
    async fn get_operation(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, GetOperationRequest>,
    ) -> ServiceResult<GetOperationResponse> {
        caller(&self.0.seed, &ctx)?;
        let id = request.operation_id.to_owned();
        let operation = self.0.lock().operations.get(&id).cloned();
        let operation =
            operation.ok_or_else(|| ConnectError::not_found(format!("no operation `{id}`")))?;
        Response::ok(GetOperationResponse {
            operation: operation.into(),
            ..Default::default()
        })
    }

    async fn list_operations(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ListOperationsRequest>,
    ) -> ServiceResult<ListOperationsResponse> {
        caller(&self.0.seed, &ctx)?;
        let request = request.to_owned_message();
        let operations = self
            .0
            .lock()
            .operations
            .values()
            .filter(|o| request.namespace.is_empty() || o.namespace == request.namespace)
            .filter(|o| request.states.is_empty() || request.states.contains(&o.state))
            .cloned()
            .collect();
        Response::ok(ListOperationsResponse {
            operations,
            ..Default::default()
        })
    }

    async fn watch_operations(
        &self,
        ctx: RequestContext,
        _request: ServiceRequest<'_, WatchOperationsRequest>,
    ) -> ServiceResult<ServiceStream<WatchOperationsResponse>> {
        caller(&self.0.seed, &ctx)?;
        let operations = self.0.lock().operations.values().cloned().collect();
        // Changes to operations are not simulated yet (scenarios, AP0 Ruling 9):
        // the stream is a snapshot followed by heartbeats.
        let first = WatchOperationsResponse {
            event: Some(OperationEvent::Snapshot(Box::new(OperationSnapshot {
                operations,
                ..Default::default()
            }))),
            cursor: "c0".into(),
            ..Default::default()
        };
        Response::stream_ok(watch::snapshot_then_heartbeats(
            first,
            self.0.heartbeat,
            || WatchOperationsResponse {
                event: Some(OperationEvent::Heartbeat(Box::default())),
                cursor: "c0".into(),
                ..Default::default()
            },
        ))
    }

    async fn cancel_operation(
        &self,
        ctx: RequestContext,
        _request: ServiceRequest<'_, CancelOperationRequest>,
    ) -> ServiceResult<CancelOperationResponse> {
        caller(&self.0.seed, &ctx)?;
        Err(not_implemented("CancelOperation"))
    }
}

/// `loams.notifications.v1.NotificationService`.
pub(crate) struct Notifications(pub(crate) Arc<Store>);

impl NotificationService for Notifications {
    async fn list_notifications(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ListNotificationsRequest>,
    ) -> ServiceResult<ListNotificationsResponse> {
        caller(&self.0.seed, &ctx)?;
        let unread_only = request.unread_only;
        let notifications = self
            .0
            .lock()
            .notifications
            .values()
            .rev()
            .filter(|n| !unread_only || !n.read_at.is_set())
            .cloned()
            .collect();
        Response::ok(ListNotificationsResponse {
            notifications,
            ..Default::default()
        })
    }

    async fn watch_notifications(
        &self,
        ctx: RequestContext,
        _request: ServiceRequest<'_, WatchNotificationsRequest>,
    ) -> ServiceResult<ServiceStream<WatchNotificationsResponse>> {
        caller(&self.0.seed, &ctx)?;
        let notifications = self
            .0
            .lock()
            .notifications
            .values()
            .filter(|n| !n.read_at.is_set())
            .cloned()
            .collect();
        let first = WatchNotificationsResponse {
            event: Some(NotificationEvent::Snapshot(Box::new(
                NotificationSnapshot {
                    notifications,
                    ..Default::default()
                },
            ))),
            cursor: "c0".into(),
            ..Default::default()
        };
        Response::stream_ok(watch::snapshot_then_heartbeats(
            first,
            self.0.heartbeat,
            || WatchNotificationsResponse {
                event: Some(NotificationEvent::Heartbeat(Box::default())),
                cursor: "c0".into(),
                ..Default::default()
            },
        ))
    }

    async fn mark_read(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, MarkReadRequest>,
    ) -> ServiceResult<MarkReadResponse> {
        caller(&self.0.seed, &ctx)?;
        let request = request.to_owned_message();
        let now = ts(std::time::SystemTime::now());
        let mut state = self.0.lock();
        let mut marked = 0u32;
        for n in state.notifications.values_mut() {
            if !n.read_at.is_set() && (request.all || request.notification_ids.contains(&n.id)) {
                n.read_at = now.clone();
                marked += 1;
            }
        }
        Response::ok(MarkReadResponse {
            marked,
            ..Default::default()
        })
    }
}
