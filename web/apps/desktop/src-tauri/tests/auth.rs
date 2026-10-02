//! Sign-in against a mock Authentik and a mock Loams gateway (AP1 Task 7).

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use common::{Res, serve};
use loams_desktop_lib::auth::{self, AuthError, Loopback};
use loams_desktop_lib::keychain::{KeyStore, MemoryKeyStore, account};
use sha2::Digest as _;
use url::Url;

#[derive(Default)]
struct Seen {
    challenge: String,
    exchanged_subject: String,
}

async fn mock_instance() -> (common::Server, Arc<Mutex<Seen>>) {
    let seen = Arc::new(Mutex::new(Seen::default()));
    let addr_cell: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
    let (s, a) = (seen.clone(), addr_cell.clone());
    let server = serve(move |req| {
        let base = a.lock().unwrap().clone();
        match (req.method.as_str(), req.path.as_str()) {
            ("POST", "/loams.instance.v1.InstanceService/GetInstance") => Res::json(
                200,
                &format!(
                    r#"{{"instanceId":"01J9MOCK","issuer":"{base}","signInMethods":[{{"kind":"SIGN_IN_KIND_AUTHENTIK","issuer":"{base}application/o/loams/","clientId":"loams-desktop"}}]}}"#
                ),
            ),
            ("GET", "/application/o/loams/.well-known/openid-configuration") => Res::json(
                200,
                &format!(
                    r#"{{"authorization_endpoint":"{base}authorize","token_endpoint":"{base}token","issuer":"{base}application/o/loams/"}}"#
                ),
            ),
            ("POST", "/token") => {
                let form = req.form();
                let verifier = form.get("code_verifier").cloned().unwrap_or_default();
                let challenge = URL_SAFE_NO_PAD.encode(sha2::Sha256::digest(verifier.as_bytes()));
                let ok = form.get("grant_type").map(String::as_str) == Some("authorization_code")
                    && form.get("code").map(String::as_str) == Some("code-123")
                    && form.get("client_id").map(String::as_str) == Some("loams-desktop")
                    && challenge == s.lock().unwrap().challenge;
                if ok {
                    Res::json(200, r#"{"access_token":"idp-access","id_token":"idp-id","refresh_token":"idp-refresh","token_type":"Bearer"}"#)
                } else {
                    Res::json(400, r#"{"error":"invalid_grant"}"#)
                }
            }
            ("POST", "/api/v1/oauth/token") => {
                let form = req.form();
                match form.get("grant_type").map(String::as_str) {
                    Some("urn:ietf:params:oauth:grant-type:token-exchange") => {
                        s.lock().unwrap().exchanged_subject = form.get("subject_token").cloned().unwrap_or_default();
                        Res::json(200, r#"{"access_token":"loams-access","refresh_token":"loams-refresh-1","expires_in":3600,"token_type":"Bearer"}"#)
                    }
                    Some("refresh_token") if form.get("refresh_token").map(String::as_str) == Some("loams-refresh-1") => {
                        Res::json(200, r#"{"access_token":"loams-access-2","refresh_token":"loams-refresh-2","expires_in":3600}"#)
                    }
                    _ => Res::json(400, r#"{"error":"invalid_grant"}"#),
                }
            }
            _ => Res::json(404, "{}"),
        }
    })
    .await;
    *addr_cell.lock().unwrap() = server.url("/");
    (server, seen)
}

/// Plays the system browser: records the PKCE challenge, then follows the
/// redirect back to the loopback listener as Authentik would.
fn browser(seen: Arc<Mutex<Seen>>, tamper_state: bool) -> impl FnOnce(Url) {
    move |authorize: Url| {
        let q: std::collections::HashMap<_, _> = authorize.query_pairs().into_owned().collect();
        assert_eq!(q["code_challenge_method"], "S256");
        assert_eq!(q["response_type"], "code");
        seen.lock().unwrap().challenge = q["code_challenge"].clone();
        let state = if tamper_state {
            "forged".to_owned()
        } else {
            q["state"].clone()
        };
        let redirect = format!("{}?code=code-123&state={state}", q["redirect_uri"]);
        assert!(redirect.starts_with("http://127.0.0.1:"));
        tokio::spawn(async move {
            let _ = reqwest::get(redirect).await;
        });
    }
}

#[tokio::test]
async fn pkce_round_trip_against_mock_issuer() {
    let (server, seen) = mock_instance().await;
    let keystore = MemoryKeyStore::default();
    let client = reqwest::Client::new();
    let base = Url::parse(&server.url("/")).unwrap();
    let (signed_in, tokens) = auth::sign_in(
        &client,
        &base,
        "me",
        &keystore,
        browser(seen.clone(), false),
        false,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(signed_in.instance_id, "01J9MOCK");
    assert!(!signed_in.persistent);
    assert_eq!(tokens.access_token, "loams-access");
    // exchange_discards_idp_tokens: Authentik's access token was exchanged;
    // only Loams's refresh token is stored.
    assert_eq!(seen.lock().unwrap().exchanged_subject, "idp-access");
    let stored = keystore.get(&account("01J9MOCK", "me")).unwrap();
    assert_eq!(stored.as_deref(), Some("loams-refresh-1"));
    // refresh_rotates_and_stores
    let refreshed = auth::refresh(&client, &base, "01J9MOCK", "me", &keystore)
        .await
        .unwrap();
    assert_eq!(refreshed.access_token, "loams-access-2");
    assert_eq!(
        keystore.get(&account("01J9MOCK", "me")).unwrap().as_deref(),
        Some("loams-refresh-2")
    );
}

#[tokio::test]
async fn state_mismatch_is_refused_and_nothing_is_stored() {
    let (server, seen) = mock_instance().await;
    let keystore = MemoryKeyStore::default();
    let err = auth::sign_in(
        &reqwest::Client::new(),
        &Url::parse(&server.url("/")).unwrap(),
        "me",
        &keystore,
        browser(seen, true),
        false,
        Duration::from_secs(10),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, AuthError::Callback("state mismatch")),
        "{err:?}"
    );
    assert_eq!(keystore.get(&account("01J9MOCK", "me")).unwrap(), None);
}

#[tokio::test]
async fn loopback_accepts_one_request() {
    let loopback = Loopback::bind().await.unwrap();
    let redirect = loopback.redirect_uri.clone();
    let waiter = tokio::spawn(async move { loopback.wait("s1", Duration::from_secs(5)).await });
    let first = reqwest::get(format!("{redirect}?code=abc&state=s1"))
        .await
        .unwrap();
    assert_eq!(first.status(), 200);
    assert!(first.text().await.unwrap().contains("close this tab"));
    assert_eq!(waiter.await.unwrap().unwrap(), "abc");
    // The listener is gone: a second request cannot reach it.
    assert!(
        reqwest::get(format!("{redirect}?code=again&state=s1"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn an_instance_without_authentik_has_no_sign_in() {
    let server = serve(|_| {
        Res::json(
            200,
            r#"{"instanceId":"x","issuer":"","signInMethods":[{"kind":"SIGN_IN_KIND_NONE"}]}"#,
        )
    })
    .await;
    let err = auth::discover_instance(
        &reqwest::Client::new(),
        &Url::parse(&server.url("/")).unwrap(),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, AuthError::NoSignIn));
}
