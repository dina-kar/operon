//! The bridge against a real local server (AP1 Task 5).

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use common::{Res, serve};
use loams_desktop_lib::envs::{EnvKind, Environment};
use loams_desktop_lib::net::{FetchEvent, FetchRequest, Net, NetError};
use url::Url;

fn local(url: &str) -> Environment {
    Environment {
        id: "local".into(),
        name: "local".into(),
        kind: EnvKind::Local,
        base_url: Url::parse(url).ok(),
    }
}

fn request(url: String, headers: &[(&str, &str)]) -> FetchRequest {
    FetchRequest {
        url,
        method: "POST".into(),
        headers: headers
            .iter()
            .map(|(a, b)| ((*a).into(), (*b).into()))
            .collect(),
        body: Some(STANDARD.encode("{}")),
    }
}

async fn collect(
    net: &Net,
    env: &Environment,
    req: FetchRequest,
) -> (Result<(), NetError>, Vec<FetchEvent>) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let sink = events.clone();
    let result = net
        .perform(env, req, move |e| sink.lock().unwrap().push(e))
        .await;
    let events = events.lock().unwrap().clone();
    (result, events)
}

fn body(events: &[FetchEvent]) -> String {
    events
        .iter()
        .filter_map(|e| match e {
            FetchEvent::Chunk { data } => {
                Some(String::from_utf8(STANDARD.decode(data).unwrap()).unwrap())
            }
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn bridge_streams_and_strips_js_auth_headers() {
    let server = serve(|req| {
        let leaked =
            req.headers.contains_key("authorization") || req.headers.contains_key("cookie");
        Res {
            status: 200,
            headers: vec![
                ("content-type".into(), "application/connect+json".into()),
                ("set-cookie".into(), "s=1".into()),
            ],
            parts: vec![format!("{{\"leaked\":{leaked},"), "\"n\":1}".into()],
            gap: Duration::from_millis(30),
        }
    })
    .await;
    let env = local(&server.url("/"));
    let net = Net::new().unwrap();
    // Even with a token held, a local environment never sends it.
    net.set_token("local", Some("secret".into()));
    let (result, events) = collect(
        &net,
        &env,
        request(
            server.url("/x"),
            &[
                ("Authorization", "Bearer js-token"),
                ("Cookie", "c=1"),
                ("Content-Type", "application/json"),
            ],
        ),
    )
    .await;
    result.unwrap();
    let FetchEvent::Head { status, headers } = &events[0] else {
        panic!("{events:?}")
    };
    assert_eq!(*status, 200);
    assert!(
        headers.iter().all(|(k, _)| k != "set-cookie"),
        "set-cookie never reaches JavaScript"
    );
    assert!(
        events
            .iter()
            .filter(|e| matches!(e, FetchEvent::Chunk { .. }))
            .count()
            >= 2,
        "{events:?}"
    );
    assert_eq!(events.last(), Some(&FetchEvent::End));
    assert_eq!(body(&events), "{\"leaked\":false,\"n\":1}");
    let seen = server.requests.lock().unwrap();
    assert_eq!(
        seen[0].headers.get("content-type").map(String::as_str),
        Some("application/json")
    );
    assert_eq!(seen[0].body, "{}");
}

#[tokio::test]
async fn bridge_refuses_cross_origin_redirect() {
    let other = serve(|_| Res::json(200, "{}")).await;
    let target = other.url("/stolen");
    let server = serve(move |_| Res::redirect(&target)).await;
    let (result, events) = collect(
        &Net::new().unwrap(),
        &local(&server.url("/")),
        request(server.url("/x"), &[]),
    )
    .await;
    assert!(matches!(result, Err(NetError::Redirect(_))), "{result:?}");
    assert!(events.is_empty());
    assert!(
        other.requests.lock().unwrap().is_empty(),
        "the other origin was never contacted"
    );
}

#[tokio::test]
async fn bridge_follows_same_origin_redirects_up_to_three() {
    let server = serve(|req| match req.path.as_str() {
        "/a" => Res::redirect("/b"),
        "/b" => Res::json(200, "{\"ok\":true}"),
        _ => Res::redirect("/loop"),
    })
    .await;
    let net = Net::new().unwrap();
    let env = local(&server.url("/"));
    let (result, events) = collect(&net, &env, request(server.url("/a"), &[])).await;
    result.unwrap();
    assert_eq!(body(&events), "{\"ok\":true}");
    let (result, _) = collect(&net, &env, request(server.url("/loop"), &[])).await;
    assert_eq!(result, Err(NetError::TooManyRedirects));
}

#[tokio::test]
async fn bridge_refuses_foreign_origin_before_any_request() {
    let server = serve(|_| Res::json(200, "{}")).await;
    let env = local("http://127.0.0.1:9/");
    let (result, _) = collect(&Net::new().unwrap(), &env, request(server.url("/x"), &[])).await;
    assert!(matches!(result, Err(NetError::ForeignOrigin(_))));
    assert!(server.requests.lock().unwrap().is_empty());
}
