//! CORS for the console and desktop development builds only (AP0 Task 5):
//! Vite on `http://localhost:5173` and the Tauri webview origins. The
//! desktop's release build never needs it, because its requests go through
//! the Rust bridge (§37 §6.4).

use axum::body::Body;
use axum::middleware::Next;
use http::{HeaderValue, Method, Request, Response, StatusCode, header};

pub(crate) const ALLOWED_ORIGINS: [&str; 4] = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "tauri://localhost",
    "http://tauri.localhost",
];

const ALLOW_HEADERS: &str = "authorization, content-type, connect-protocol-version, \
    connect-timeout-ms, x-grpc-web, x-user-agent, grpc-timeout";
const EXPOSE_HEADERS: &str = "grpc-status, grpc-message, grpc-status-details-bin";

pub(crate) async fn cors(request: Request<Body>, next: Next) -> Response<Body> {
    let origin = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|o| o.to_str().ok())
        .filter(|o| ALLOWED_ORIGINS.contains(o))
        .map(str::to_owned);
    let preflight = request.method() == Method::OPTIONS;
    let mut response = if preflight {
        let mut r = Response::new(Body::empty());
        *r.status_mut() = StatusCode::NO_CONTENT;
        r
    } else {
        next.run(request).await
    };
    if let Some(origin) = origin {
        let headers = response.headers_mut();
        if let Ok(value) = HeaderValue::from_str(&origin) {
            headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, value);
        }
        headers.insert(header::VARY, HeaderValue::from_static("origin"));
        headers.insert(
            header::ACCESS_CONTROL_EXPOSE_HEADERS,
            HeaderValue::from_static(EXPOSE_HEADERS),
        );
        if preflight {
            headers.insert(
                header::ACCESS_CONTROL_ALLOW_METHODS,
                HeaderValue::from_static("GET, POST, OPTIONS"),
            );
            headers.insert(
                header::ACCESS_CONTROL_ALLOW_HEADERS,
                HeaderValue::from_static(ALLOW_HEADERS),
            );
            headers.insert(
                header::ACCESS_CONTROL_MAX_AGE,
                HeaderValue::from_static("600"),
            );
        }
    }
    response
}
