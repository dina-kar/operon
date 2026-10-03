//! The Connect API on the main port (design §44 §4, D600; API1 Task 1).
//!
//! One port serves the Connect protocol, gRPC and gRPC-Web at once:
//! connect-rust answers all three from one handler, so a `curl` POST of JSON,
//! a `grpcurl` call and a browser's gRPC-Web fetch are the same RPC. This
//! module owns that wiring and nothing else:
//!
//! - the **catalogue** (`CATALOGUE`): every public `loams.*.v1` package that
//!   exists, its services, and whether this binary serves them;
//! - [`InstanceService`], which an SDK calls first, before sign-in, to
//!   feature-detect from `GetInstance.services[]`;
//! - the stub an **unavailable** package answers with: `unimplemented` with
//!   the reason `feature_not_in_variant` and the variant in `metadata`;
//! - `grpc.health.v1`, and `grpc.reflection.v1` where reflection is on.
//!
//! The native REST routes in [`super`] are **not** replaced here. Tasks 2–8
//! add one RPC per route and Task 9 deletes the route, so [`super::router`]
//! serves both for now. The Connect paths are registered as their own axum
//! routes rather than as the router's fallback, precisely so that the native
//! `no_route` fallback stays the answer for a path neither API knows
//! (`it/http.rs::framework_rejections_use_the_json_error_body`).

// The generated service traits return `impl Encodable<_>`; these impls name
// the concrete message type, which is the intended refinement.
#![allow(refining_impl_trait)]

use std::sync::Arc;
use std::sync::LazyLock;

use axum::Router as AxumRouter;
use buffa::enumeration::EnumValue;
use connectrpc::{
    ConnectError, ErrorCode, ErrorDetail, RequestContext, Response, Router, ServiceRequest,
    ServiceResult, ServiceStream,
};
use loams_live_proto::loams::live::v1::{
    DeployRequest, DeployResponse, LiveService, LiveServiceExt, ModifyQuerySetRequest,
    ModifyQuerySetResponse, MutateRequest, MutateResponse, QueryRequest, QueryResponse, Transition,
    WatchRequest,
};
use loams_proto::loams::errors::v1::ErrorInfo;
use loams_proto::loams::instance::v1::{
    Edition, GetInstanceRequest, GetInstanceResponse, InstanceService, InstanceServiceExt,
    ServiceStatus, SignInKind, SignInMethod, WhoAmIRequest, WhoAmIResponse,
};
use ulid::Ulid;

use super::AppState;

/// The build variant this binary is (§30 §9, D286): `full` when an engine
/// only `full` carries is compiled in, `standard` otherwise. The variant
/// never changes the wire contract; it only decides which catalogue packages
/// answer `feature_not_in_variant` (§44 §4). CLI2 Task 3 replaces this with
/// the release variant file; until then the cargo features are the variant.
pub const VARIANT: &str = if cfg!(any(
    feature = "tikv",
    feature = "mysql-wire",
    feature = "stream-grpc"
)) {
    "full"
} else {
    "standard"
};

/// What this binary calls itself in `GetInstance.name`. The auth plan (MT)
/// makes it configurable per instance; until then it is the product name.
const INSTANCE_NAME: &str = "Loams";

/// One package of the API catalogue (design §44 §4, D600).
struct Package {
    /// The proto package, for example `loams.collection.v1`.
    package: &'static str,
    /// Its services, fully qualified.
    services: &'static [&'static str],
    /// Whether this binary serves it. False means every one of its RPCs
    /// answers `unimplemented` with reason `feature_not_in_variant`.
    available: bool,
    /// Whether its wire contract may still change (`ModuleOptions.unstable`,
    /// §44 §10.3): SDKs mark the module experimental and `buf breaking`
    /// skips it.
    unstable: bool,
}

/// Every public package of the API, and what this binary does with it.
///
/// API1 Task 1 starts the list with the two packages whose protos exist and
/// whose availability is decided: the one this binary serves and the one it
/// does not. Tasks 2–8 append `loams.collection.v1`, `loams.sql.v1`,
/// `loams.link.v1`, `loams.stream.v1`, `loams.admin.v1`, `loams.auth.v1`,
/// and the cluster-listener-only `loams.internal.v1`, flipping each row's
/// `available` as its handler lands. A package with no proto yet has no row,
/// so `GetInstance.services[]` never advertises a contract that does not
/// exist.
const CATALOGUE: &[Package] = &[
    Package {
        package: "loams.instance.v1",
        services: &["loams.instance.v1.InstanceService"],
        available: true,
        unstable: false,
    },
    Package {
        // The live sync engine is a `full`-variant engine (§30 §8.2) and R1's
        // `live` cargo feature has not merged, so no variant serves it yet.
        // R1 Task 12 adds the engine and flips this row on under that
        // feature; until then the package is listed, advertised as
        // unavailable, and every one of its RPCs refuses with
        // `feature_not_in_variant` (see `LiveAbsent`).
        package: "loams.live.v1",
        services: &["loams.live.v1.LiveService"],
        available: false,
        unstable: true,
    },
];

/// The catalogue as `GetInstance.services[]`.
fn statuses() -> Vec<ServiceStatus> {
    CATALOGUE
        .iter()
        .map(|entry| ServiceStatus {
            package: entry.package.to_owned(),
            version: "v1".to_owned(),
            available: entry.available,
            services: entry
                .services
                .iter()
                .map(|name| (*name).to_owned())
                .collect(),
            unstable: entry.unstable,
            ..Default::default()
        })
        .collect()
}

/// The services a `grpc.health.v1` probe may ask about. The whole-process
/// entry (the empty name) is pre-registered by `connectrpc-health` and is not
/// repeated here.
fn served_services() -> Vec<&'static str> {
    CATALOGUE
        .iter()
        .filter(|entry| entry.available)
        .flat_map(|entry| entry.services.iter().copied())
        .collect()
}

/// This process's instance id: a ULID generated when the server starts. The
/// auth plan (MT) persists one id per install and serves it here; until then
/// it identifies the running process.
static INSTANCE_ID: LazyLock<String> = LazyLock::new(|| Ulid::generate().to_string());

/// `loams.instance.v1.InstanceService` for the OSS server.
#[derive(Debug)]
struct Instance;

impl InstanceService for Instance {
    /// No credentials: an app calls this before sign-in.
    async fn get_instance(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, GetInstanceRequest>,
    ) -> ServiceResult<GetInstanceResponse> {
        Response::ok(GetInstanceResponse {
            instance_id: INSTANCE_ID.clone(),
            name: INSTANCE_NAME.to_owned(),
            // This binary is the open-source edition; `EDITION_CLOUD` and
            // `EDITION_BYOC` are the hosted gateway's.
            edition: EnumValue::Known(Edition::EDITION_OSS),
            server_version: env!("CARGO_PKG_VERSION").to_owned(),
            // Only what is served, which is what the console needs to build an
            // `rpc.<service>` (AP1a Ruling 6); `services` is the long form.
            api_versions: CATALOGUE
                .iter()
                .filter(|entry| entry.available)
                .map(|entry| entry.package.to_owned())
                .collect(),
            sign_in_methods: vec![SignInMethod {
                kind: EnumValue::Known(SignInKind::SIGN_IN_KIND_NONE),
                display_name: "No sign-in".to_owned(),
                ..Default::default()
            }],
            services: statuses(),
            // Empty until their plans land, not because they are off: `issuer`
            // and `jwks_uri` with the auth plan (MT), `tls_pins` and `push`
            // with the phone plans (§37 §7), `min_app_versions` with release
            // (CLI2), `setup_required` with first-run setup, `key_rotation`
            // with signing-key rotation. A client must treat an empty
            // `features` map as "nothing announced", not "nothing enabled".
            ..Default::default()
        })
    }

    /// There is no authentication on this port yet (design §19 §5 is the auth
    /// plan; MT and API1 Task 7 build it), so there is no principal to
    /// report. The refusal names the RPC and the reason so a caller can tell
    /// it apart from a token that was rejected.
    async fn who_am_i(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, WhoAmIRequest>,
    ) -> ServiceResult<WhoAmIResponse> {
        Err(not_implemented("loams.instance.v1.InstanceService/WhoAmI"))
    }
}

/// `loams.live.v1.LiveService` as this binary answers it: every RPC refuses
/// with `feature_not_in_variant`.
///
/// Registering the service rather than leaving its paths unrouted is what
/// makes the failure typed. A caller gets the Connect code, the stable reason
/// and the variant in `metadata` instead of a bare `404`, and
/// `GetInstance.services[]` can list the package at all (§44 §4). R1 Task 12
/// replaces this with the live engine.
#[derive(Debug)]
struct LiveAbsent;

impl LiveService for LiveAbsent {
    async fn watch(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, WatchRequest>,
    ) -> ServiceResult<ServiceStream<Transition>> {
        Err(not_in_variant("loams.live.v1.LiveService/Watch"))
    }

    async fn modify_query_set(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, ModifyQuerySetRequest>,
    ) -> ServiceResult<ModifyQuerySetResponse> {
        Err(not_in_variant("loams.live.v1.LiveService/ModifyQuerySet"))
    }

    async fn query(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, QueryRequest>,
    ) -> ServiceResult<QueryResponse> {
        Err(not_in_variant("loams.live.v1.LiveService/Query"))
    }

    async fn mutate(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, MutateRequest>,
    ) -> ServiceResult<MutateResponse> {
        Err(not_in_variant("loams.live.v1.LiveService/Mutate"))
    }

    async fn deploy(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, DeployRequest>,
    ) -> ServiceResult<DeployResponse> {
        Err(not_in_variant("loams.live.v1.LiveService/Deploy"))
    }
}

/// A failed RPC: the Connect `code`, and one `loams.errors.v1.ErrorInfo` in
/// the details carrying the stable `reason` callers branch on plus the
/// `metadata` that goes with it (design §44 §7.4, D611). `metadata` never
/// holds a secret. Every reason this module raises is registered in
/// `docs/api/reasons.md`.
fn refuse(
    code: ErrorCode,
    reason: &str,
    message: impl Into<String>,
    metadata: &[(&str, &str)],
) -> ConnectError {
    let info = ErrorInfo {
        reason: reason.to_owned(),
        metadata: metadata
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect(),
        ..Default::default()
    };
    ConnectError::new(code, message).with_detail(ErrorDetail::from_message(
        "loams.errors.v1.ErrorInfo",
        &info,
    ))
}

/// What every RPC of a catalogue package this binary does not serve answers
/// (design §44 §4, D600): `unimplemented`, the reason
/// `feature_not_in_variant`, and the variant that was asked for, so a caller
/// can tell "not in this build" from "not written yet" (`not_implemented`).
fn not_in_variant(rpc: &str) -> ConnectError {
    refuse(
        ErrorCode::Unimplemented,
        "feature_not_in_variant",
        format!("{rpc} is not in the {VARIANT} variant"),
        &[("variant", VARIANT)],
    )
}

/// What an RPC whose service exists in the protos but is not implemented by
/// this binary yet answers (AP0's `not_implemented`).
fn not_implemented(rpc: &str) -> ConnectError {
    refuse(
        ErrorCode::Unimplemented,
        "not_implemented",
        format!("{rpc} is not implemented by this build yet"),
        &[],
    )
}

/// The reflector for `grpc.reflection.v1`, or `None` when the descriptor set
/// `loams-proto` emitted does not parse. Reflection is a convenience, so a
/// broken descriptor set costs reflection and nothing else: it is logged and
/// the port still serves.
fn reflector(state: &AppState) -> Option<connectrpc_reflection::Reflector> {
    if !state.reflection {
        return None;
    }
    match connectrpc_reflection::Reflector::from_descriptor_set_bytes(
        loams_proto::FILE_DESCRIPTOR_SET,
    ) {
        Ok(reflector) => Some(reflector),
        Err(err) => {
            tracing::warn!(%err, "gRPC reflection is off: the descriptor set does not parse");
            None
        }
    }
}

/// The Connect routes of the main port, as axum routes.
///
/// Every RPC path of the connect router becomes its own axum route. The
/// alternative — one catch-all at the router's fallback — would take the
/// native API's `404` and `405` away, which Task 9's `no_native_rest_route_
/// remains` still expects from the paths that survive it. A Connect path is
/// `/<package>.<Service>/<Method>` and every native path is under `/v1`,
/// `/internal`, `/health` or `/ready`, so the two sets cannot collide.
pub(crate) fn routes(state: &AppState) -> AxumRouter {
    let mut rpc = Router::new();
    rpc = Arc::new(Instance).register(rpc);
    rpc = Arc::new(LiveAbsent).register(rpc);
    let (rpc, _health) = connectrpc_health::install_static(rpc, served_services());
    let rpc = match reflector(state) {
        Some(reflector) => connectrpc_reflection::install(rpc, reflector),
        None => rpc,
    };
    // Before it is consumed: the paths are what the axum router keys on.
    let paths: Vec<String> = rpc.methods().map(|path| format!("/{path}")).collect();
    let service = rpc.into_axum_service();
    let mut axum = AxumRouter::new();
    for path in paths {
        axum = axum.route_service(&path, service.clone());
    }
    axum
}
