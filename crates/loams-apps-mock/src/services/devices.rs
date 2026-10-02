//! `loams.devices.v1.DeviceService`: pairing payloads, the device list,
//! rename and revoke. Push targets, preferences and test notifications are
//! stubs until AP4.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use connectrpc::{ConnectError, RequestContext, Response, ServiceRequest, ServiceResult};
use rand::Rng as _;

use crate::auth::caller;
use crate::not_implemented;
use crate::proto::loams::devices::v1::{
    CreatePairingRequest, CreatePairingResponse, DeviceService, GetNotificationPreferencesRequest,
    GetNotificationPreferencesResponse, ListDevicesRequest, ListDevicesResponse,
    RegisterPushTargetRequest, RegisterPushTargetResponse, RenameDeviceRequest,
    RenameDeviceResponse, RevokeDeviceRequest, RevokeDeviceResponse, SendTestNotificationRequest,
    SendTestNotificationResponse, SetNotificationPreferencesRequest,
    SetNotificationPreferencesResponse, UnregisterPushTargetRequest, UnregisterPushTargetResponse,
};
use crate::seed::ts;
use crate::store::Store;

/// A pairing is valid this long (§37 §7.2.1).
pub(crate) const PAIRING_TTL: Duration = Duration::from_secs(5 * 60);

pub(crate) struct Devices(pub(crate) Arc<Store>);

/// The QR payload of §37 §7.2.1, version 1.
pub(crate) fn qr_payload(
    issuer: &str,
    instance_id: &str,
    code: &str,
    user_code: &str,
    exp: u64,
) -> String {
    serde_json::json!({
        "v": 1,
        "kind": "loams-pair",
        "issuer": issuer,
        "instance_id": instance_id,
        // Public CAs in the mock: no pinned SPKI (§37 §7.2.1).
        "spki": null,
        "jkt": "NzbLsXh8uDCcd-6MNwXF4W_7noWXFZAfHkxZsRGC9Xs",
        "code": code,
        "user_code": user_code,
        "exp": exp,
    })
    .to_string()
}

impl DeviceService for Devices {
    async fn create_pairing(
        &self,
        ctx: RequestContext,
        _request: ServiceRequest<'_, CreatePairingRequest>,
    ) -> ServiceResult<CreatePairingResponse> {
        caller(&self.0.seed, &ctx)?;
        let mut rng = rand::rng();
        // 128 random bits in Crockford base32 (26 characters).
        let code = ulid::Ulid::from(rng.random::<u128>()).to_string();
        let user_code = format!("{:08}", rng.random_range(0..100_000_000u32));
        let expires = SystemTime::now() + PAIRING_TTL;
        let exp = expires
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let instance = &self.0.seed.instance;
        Response::ok(CreatePairingResponse {
            pairing_id: format!("pair_{}", ulid::Ulid::from(rng.random::<u128>())),
            qr_payload: qr_payload(
                &instance.issuer,
                &instance.instance_id,
                &code,
                &user_code,
                exp,
            ),
            user_code,
            expires_at: ts(expires),
            ..Default::default()
        })
    }

    async fn list_devices(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ListDevicesRequest>,
    ) -> ServiceResult<ListDevicesResponse> {
        let me = caller(&self.0.seed, &ctx)?;
        let include_revoked = request.include_revoked;
        let owner = if request.user_id.is_empty() {
            me.principal.id
        } else {
            request.user_id.to_owned()
        };
        let devices = self
            .0
            .lock()
            .devices
            .values()
            .filter(|(o, d)| *o == owner && (include_revoked || !d.revoked_at.is_set()))
            .map(|(_, d)| d.clone())
            .collect();
        Response::ok(ListDevicesResponse {
            devices,
            ..Default::default()
        })
    }

    async fn rename_device(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, RenameDeviceRequest>,
    ) -> ServiceResult<RenameDeviceResponse> {
        caller(&self.0.seed, &ctx)?;
        let request = request.to_owned_message();
        let mut state = self.0.lock();
        let (_, device) = state
            .devices
            .get_mut(&request.device_id)
            .ok_or_else(|| ConnectError::not_found(format!("no device `{}`", request.device_id)))?;
        device.name = request.name;
        Response::ok(RenameDeviceResponse {
            device: device.clone().into(),
            ..Default::default()
        })
    }

    async fn revoke_device(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, RevokeDeviceRequest>,
    ) -> ServiceResult<RevokeDeviceResponse> {
        caller(&self.0.seed, &ctx)?;
        let id = request.device_id.to_owned();
        let mut state = self.0.lock();
        let (_, device) = state
            .devices
            .get_mut(&id)
            .ok_or_else(|| ConnectError::not_found(format!("no device `{id}`")))?;
        if !device.revoked_at.is_set() {
            device.revoked_at = ts(SystemTime::now());
            device.push_targets.clear();
        }
        Response::ok(RevokeDeviceResponse {
            device: device.clone().into(),
            ..Default::default()
        })
    }

    async fn register_push_target(
        &self,
        ctx: RequestContext,
        _request: ServiceRequest<'_, RegisterPushTargetRequest>,
    ) -> ServiceResult<RegisterPushTargetResponse> {
        caller(&self.0.seed, &ctx)?;
        Err(not_implemented("RegisterPushTarget"))
    }

    async fn unregister_push_target(
        &self,
        ctx: RequestContext,
        _request: ServiceRequest<'_, UnregisterPushTargetRequest>,
    ) -> ServiceResult<UnregisterPushTargetResponse> {
        caller(&self.0.seed, &ctx)?;
        Err(not_implemented("UnregisterPushTarget"))
    }

    async fn get_notification_preferences(
        &self,
        ctx: RequestContext,
        _request: ServiceRequest<'_, GetNotificationPreferencesRequest>,
    ) -> ServiceResult<GetNotificationPreferencesResponse> {
        caller(&self.0.seed, &ctx)?;
        Err(not_implemented("GetNotificationPreferences"))
    }

    async fn set_notification_preferences(
        &self,
        ctx: RequestContext,
        _request: ServiceRequest<'_, SetNotificationPreferencesRequest>,
    ) -> ServiceResult<SetNotificationPreferencesResponse> {
        caller(&self.0.seed, &ctx)?;
        Err(not_implemented("SetNotificationPreferences"))
    }

    async fn send_test_notification(
        &self,
        ctx: RequestContext,
        _request: ServiceRequest<'_, SendTestNotificationRequest>,
    ) -> ServiceResult<SendTestNotificationResponse> {
        caller(&self.0.seed, &ctx)?;
        Err(not_implemented("SendTestNotification"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pairing_payload_is_v1() {
        let payload = qr_payload(
            "https://loams.example",
            "01J9",
            "CODE",
            "12345678",
            1_790_899_500,
        );
        let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(v["v"], 1);
        assert_eq!(v["kind"], "loams-pair");
        assert!(v["issuer"].as_str().unwrap().starts_with("https://"));
        assert_eq!(v["user_code"].as_str().unwrap().len(), 8);
        let keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            [
                "v",
                "kind",
                "issuer",
                "instance_id",
                "spki",
                "jkt",
                "code",
                "user_code",
                "exp"
            ]
        );
    }
}
