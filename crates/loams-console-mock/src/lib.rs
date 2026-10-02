//! A mock of the console API with seed data (design §19 P10).
//!
//! The contract is `api/console/openapi.json`; the seed is `seed/seed.json`.
//! [`routes`] turns the seed into one concrete response per path, and
//! [`register`] installs them on an [`httpmock::MockServer`]. The mock is
//! static: a `POST` answers with a plausible created object, and the next
//! `GET` does not include it.
//!
//! Seed times are relative so the data always looks current: a string
//! `@now`, `@now-5m` or `@now+12h` becomes an RFC 3339 timestamp,
//! `@date-3d` a date, `@ms-80d` Unix milliseconds (a number), and
//! `@month-start` / `@month-end` the current month's bounds.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use httpmock::{Method, MockServer};
use serde_json::{Value, json};

/// The console API contract, OpenAPI 3.1.
pub const CONTRACT: &str = include_str!("../../../api/console/openapi.json");

/// The seed data, with relative times unresolved.
pub const SEED: &str = include_str!("../seed/seed.json");

/// Loads [`SEED`] and resolves its relative times against `now`.
pub fn load_seed(now: SystemTime) -> Result<Value> {
    let mut seed: Value = serde_json::from_str(SEED).context("parsing seed/seed.json")?;
    resolve(&mut seed, now)?;
    Ok(seed)
}

fn resolve(value: &mut Value, now: SystemTime) -> Result<()> {
    match value {
        Value::String(s) if s.starts_with('@') => *value = resolve_one(s, now)?,
        Value::Array(items) => items.iter_mut().try_for_each(|v| resolve(v, now))?,
        Value::Object(map) => map.values_mut().try_for_each(|v| resolve(v, now))?,
        _ => {}
    }
    Ok(())
}

/// `now ± <humantime duration>`.
fn offset(now: SystemTime, rest: &str) -> Result<SystemTime> {
    if rest.is_empty() {
        return Ok(now);
    }
    let (sign, amount) = rest.split_at(1);
    let d = humantime::parse_duration(amount).with_context(|| format!("bad offset {rest}"))?;
    match sign {
        "-" => now
            .checked_sub(d)
            .ok_or_else(|| anyhow!("offset {rest} underflows")),
        "+" => now
            .checked_add(d)
            .ok_or_else(|| anyhow!("offset {rest} overflows")),
        _ => bail!("an offset starts with + or -, got {rest}"),
    }
}

fn rfc3339(t: SystemTime) -> String {
    humantime::format_rfc3339_seconds(t).to_string()
}

fn resolve_one(token: &str, now: SystemTime) -> Result<Value> {
    if let Some(rest) = token.strip_prefix("@now") {
        return Ok(json!(rfc3339(offset(now, rest)?)));
    }
    if let Some(rest) = token.strip_prefix("@date") {
        return Ok(json!(rfc3339(offset(now, rest)?)[..10].to_string()));
    }
    if let Some(rest) = token.strip_prefix("@ms") {
        let t = offset(now, rest)?;
        let ms = t
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis();
        return Ok(json!(u64::try_from(ms)?));
    }
    let today = rfc3339(now);
    let (year, month) = (today[..4].parse::<u32>()?, today[5..7].parse::<u32>()?);
    match token {
        "@month-start" => Ok(json!(format!("{year:04}-{month:02}-01T00:00:00Z"))),
        "@month-end" => {
            let (y, m) = if month == 12 {
                (year + 1, 1)
            } else {
                (year, month + 1)
            };
            Ok(json!(format!("{y:04}-{m:02}-01T00:00:00Z")))
        }
        _ => bail!("unknown seed placeholder {token}"),
    }
}

/// One mocked response.
#[derive(Clone, Debug, PartialEq)]
pub struct Route {
    pub method: &'static str,
    /// The contract's path template, for example `/api/v1/projects/{project}`.
    pub template: &'static str,
    /// The concrete path this response answers.
    pub path: String,
    /// Query parameters that must match, if any.
    pub query: Vec<(String, String)>,
    pub status: u16,
    /// A JSON body, or `None` for an empty one.
    pub body: Option<Value>,
    /// A `Location` header, for redirects.
    pub location: Option<String>,
}

impl Route {
    fn json(
        method: &'static str,
        template: &'static str,
        path: String,
        status: u16,
        body: Value,
    ) -> Self {
        Route {
            method,
            template,
            path,
            query: Vec::new(),
            status,
            body: Some(body),
            location: None,
        }
    }
    fn empty(method: &'static str, template: &'static str, path: String) -> Self {
        Route {
            method,
            template,
            path,
            query: Vec::new(),
            status: 204,
            body: None,
            location: None,
        }
    }
    fn redirect(template: &'static str, path: String, location: &str) -> Self {
        Route {
            method: "GET",
            template,
            path,
            query: Vec::new(),
            status: 302,
            body: None,
            location: Some(location.to_string()),
        }
    }
}

fn get<'a>(seed: &'a Value, key: &str) -> Result<&'a Value> {
    seed.get(key).ok_or_else(|| anyhow!("seed has no {key}"))
}

fn items<'a>(seed: &'a Value, key: &str) -> Result<&'a Vec<Value>> {
    get(seed, key)?
        .as_array()
        .ok_or_else(|| anyhow!("seed {key} is not an array"))
}

fn entries<'a>(seed: &'a Value, key: &str) -> Result<&'a serde_json::Map<String, Value>> {
    get(seed, key)?
        .as_object()
        .ok_or_else(|| anyhow!("seed {key} is not an object"))
}

fn str_field<'a>(v: &'a Value, key: &str) -> Result<&'a str> {
    v.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("seed record has no string {key}: {v}"))
}

fn example(seed: &Value, name: &str) -> Result<Value> {
    get(get(seed, "examples")?, name).cloned()
}

fn error(code: &str, message: &str) -> Value {
    json!({ "error": code, "message": message })
}

/// Every mocked response for `seed`. With `signed_in` false, the session
/// endpoint answers 401, so the console shows its sign-in screen.
pub fn routes(seed: &Value, signed_in: bool) -> Result<Vec<Route>> {
    const A: &str = "/api/v1";
    let mut r = Vec::new();
    let p = |s: &str| format!("{A}{s}");

    // Session.
    r.push(Route::json(
        "GET",
        "/api/v1/instance",
        p("/instance"),
        200,
        get(seed, "instance")?.clone(),
    ));
    let session = get(seed, "session")?.clone();
    r.push(Route::json(
        "POST",
        "/api/v1/setup",
        p("/setup"),
        201,
        session.clone(),
    ));
    r.push(if signed_in {
        Route::json(
            "GET",
            "/api/v1/session",
            p("/session"),
            200,
            session.clone(),
        )
    } else {
        Route::json(
            "GET",
            "/api/v1/session",
            p("/session"),
            401,
            error("unauthenticated", "sign in to continue"),
        )
    });
    r.push(Route::json(
        "POST",
        "/api/v1/session",
        p("/session"),
        201,
        session,
    ));
    r.push(Route::empty("DELETE", "/api/v1/session", p("/session")));
    for provider in get(seed, "instance")?["sign_in"]["oidc"]
        .as_array()
        .into_iter()
        .flatten()
    {
        let id = str_field(provider, "id")?;
        r.push(Route::redirect(
            "/api/v1/auth/oidc/{provider}/start",
            p(&format!("/auth/oidc/{id}/start")),
            "/ui/",
        ));
    }

    // Org.
    let org = get(seed, "org")?.clone();
    r.push(Route::json(
        "GET",
        "/api/v1/org",
        p("/org"),
        200,
        org.clone(),
    ));
    r.push(Route::json("PATCH", "/api/v1/org", p("/org"), 200, org));
    r.push(Route::json(
        "GET",
        "/api/v1/org/members",
        p("/org/members"),
        200,
        json!({ "members": items(seed, "members")? }),
    ));
    for m in items(seed, "members")? {
        let id = str_field(&m["user"], "id")?;
        r.push(Route::empty(
            "DELETE",
            "/api/v1/org/members/{user}",
            p(&format!("/org/members/{id}")),
        ));
    }
    r.push(Route::json(
        "GET",
        "/api/v1/org/invitations",
        p("/org/invitations"),
        200,
        json!({ "invitations": items(seed, "invitations")? }),
    ));
    r.push(Route::json(
        "POST",
        "/api/v1/org/invitations",
        p("/org/invitations"),
        201,
        example(seed, "invitation")?,
    ));

    // Teams.
    r.push(Route::json(
        "GET",
        "/api/v1/teams",
        p("/teams"),
        200,
        json!({ "teams": items(seed, "teams")? }),
    ));
    r.push(Route::json(
        "POST",
        "/api/v1/teams",
        p("/teams"),
        201,
        example(seed, "team")?,
    ));
    for (id, detail) in entries(seed, "team_details")? {
        r.push(Route::json(
            "GET",
            "/api/v1/teams/{team}",
            p(&format!("/teams/{id}")),
            200,
            detail.clone(),
        ));
        r.push(Route::empty(
            "POST",
            "/api/v1/teams/{team}/members",
            p(&format!("/teams/{id}/members")),
        ));
    }

    // Projects and environments.
    r.push(Route::json(
        "GET",
        "/api/v1/projects",
        p("/projects"),
        200,
        json!({ "projects": items(seed, "projects")? }),
    ));
    r.push(Route::json(
        "POST",
        "/api/v1/projects",
        p("/projects"),
        201,
        example(seed, "project")?,
    ));
    let environments = items(seed, "environments")?;
    for project in items(seed, "projects")? {
        let slug = str_field(project, "slug")?;
        let base = format!("/projects/{slug}");
        r.push(Route::json(
            "GET",
            "/api/v1/projects/{project}",
            p(&base),
            200,
            project.clone(),
        ));
        let grants = entries(seed, "grants")?
            .get(slug)
            .cloned()
            .unwrap_or(json!([]));
        r.push(Route::json(
            "GET",
            "/api/v1/projects/{project}/access",
            p(&format!("{base}/access")),
            200,
            json!({ "grants": grants }),
        ));
        r.push(Route::json(
            "POST",
            "/api/v1/projects/{project}/access",
            p(&format!("{base}/access")),
            201,
            example(seed, "grant")?,
        ));
        let envs: Vec<&Value> = environments
            .iter()
            .filter(|e| e["project"] == slug)
            .collect();
        r.push(Route::json(
            "GET",
            "/api/v1/projects/{project}/environments",
            p(&format!("{base}/environments")),
            200,
            json!({ "environments": envs }),
        ));
        r.push(Route::json(
            "POST",
            "/api/v1/projects/{project}/environments",
            p(&format!("{base}/environments")),
            201,
            example(seed, "environment")?,
        ));
        for env in envs {
            let es = str_field(env, "slug")?;
            let eb = format!("{base}/environments/{es}");
            r.push(Route::json(
                "GET",
                "/api/v1/projects/{project}/environments/{environment}",
                p(&eb),
                200,
                env.clone(),
            ));
            let usage = entries(seed, "usage")?
                .get(&format!("{slug}/{es}"))
                .cloned()
                .ok_or_else(|| anyhow!("no usage for {slug}/{es}"))?;
            r.push(Route::json(
                "GET",
                "/api/v1/projects/{project}/environments/{environment}/usage",
                p(&format!("{eb}/usage")),
                200,
                usage,
            ));
            let keys = entries(seed, "keys")?
                .get(&format!("{slug}/{es}"))
                .cloned()
                .unwrap_or(json!([]));
            r.push(Route::json(
                "GET",
                "/api/v1/projects/{project}/environments/{environment}/keys",
                p(&format!("{eb}/keys")),
                200,
                json!({ "keys": keys }),
            ));
            r.push(Route::json(
                "POST",
                "/api/v1/projects/{project}/environments/{environment}/keys",
                p(&format!("{eb}/keys")),
                201,
                example(seed, "api_key_created")?,
            ));
        }
        let agents: Vec<&Value> = items(seed, "agents")?
            .iter()
            .filter(|a| a["project"] == slug)
            .collect();
        r.push(Route::json(
            "GET",
            "/api/v1/projects/{project}/agents",
            p(&format!("{base}/agents")),
            200,
            json!({ "agents": agents }),
        ));
        r.push(Route::json(
            "POST",
            "/api/v1/projects/{project}/agents",
            p(&format!("{base}/agents")),
            201,
            example(seed, "agent")?,
        ));
        let sas = entries(seed, "service_accounts")?
            .get(slug)
            .cloned()
            .unwrap_or(json!([]));
        r.push(Route::json(
            "GET",
            "/api/v1/projects/{project}/service-accounts",
            p(&format!("{base}/service-accounts")),
            200,
            json!({ "service_accounts": sas }),
        ));
        r.push(Route::json(
            "POST",
            "/api/v1/projects/{project}/service-accounts",
            p(&format!("{base}/service-accounts")),
            201,
            example(seed, "service_account")?,
        ));
    }

    // Agents.
    for agent in items(seed, "agents")? {
        let id = str_field(agent, "id")?;
        let base = format!("/agents/{id}");
        r.push(Route::json(
            "GET",
            "/api/v1/agents/{agent}",
            p(&base),
            200,
            agent.clone(),
        ));
        r.push(Route::json(
            "PATCH",
            "/api/v1/agents/{agent}",
            p(&base),
            200,
            agent.clone(),
        ));
        let mut suspended = agent.clone();
        suspended["status"] = json!("suspended");
        suspended["active_tokens"] = json!(0);
        r.push(Route::json(
            "POST",
            "/api/v1/agents/{agent}/suspend",
            p(&format!("{base}/suspend")),
            200,
            suspended,
        ));
        let mut active = agent.clone();
        active["status"] = json!("active");
        r.push(Route::json(
            "POST",
            "/api/v1/agents/{agent}/resume",
            p(&format!("{base}/resume")),
            200,
            active,
        ));
        let tokens = entries(seed, "tokens")?
            .get(id)
            .cloned()
            .unwrap_or(json!([]));
        for t in tokens.as_array().into_iter().flatten() {
            let jti = str_field(t, "jti")?;
            r.push(Route::empty(
                "DELETE",
                "/api/v1/tokens/{jti}",
                p(&format!("/tokens/{jti}")),
            ));
        }
        r.push(Route::json(
            "GET",
            "/api/v1/agents/{agent}/tokens",
            p(&format!("{base}/tokens")),
            200,
            json!({ "tokens": tokens }),
        ));
        let trust = entries(seed, "trust_policies")?
            .get(id)
            .cloned()
            .unwrap_or(json!([]));
        r.push(Route::json(
            "GET",
            "/api/v1/agents/{agent}/trust-policies",
            p(&format!("{base}/trust-policies")),
            200,
            json!({ "trust_policies": trust }),
        ));
        r.push(Route::json(
            "POST",
            "/api/v1/agents/{agent}/trust-policies",
            p(&format!("{base}/trust-policies")),
            201,
            example(seed, "trust_policy")?,
        ));
    }

    // API keys.
    for keys in entries(seed, "keys")?.values() {
        for k in keys.as_array().into_iter().flatten() {
            let id = str_field(k, "id")?;
            r.push(Route::empty(
                "DELETE",
                "/api/v1/keys/{key}",
                p(&format!("/keys/{id}")),
            ));
        }
    }

    // Audit: filtered mocks first (the first matching mock wins), then all.
    let audit = items(seed, "audit")?;
    for agent in items(seed, "agents")? {
        let id = str_field(agent, "id")?;
        let events: Vec<&Value> = audit.iter().filter(|e| e["actor"]["id"] == id).collect();
        let mut route = Route::json(
            "GET",
            "/api/v1/audit",
            p("/audit"),
            200,
            json!({ "events": events, "next_before": null }),
        );
        route.query.push(("actor".to_string(), id.to_string()));
        r.push(route);
    }
    for project in items(seed, "projects")? {
        let slug = str_field(project, "slug")?;
        let events: Vec<&Value> = audit.iter().filter(|e| e["project"] == slug).collect();
        let mut route = Route::json(
            "GET",
            "/api/v1/audit",
            p("/audit"),
            200,
            json!({ "events": events, "next_before": null }),
        );
        route.query.push(("project".to_string(), slug.to_string()));
        r.push(route);
    }
    r.push(Route::json(
        "GET",
        "/api/v1/audit",
        p("/audit"),
        200,
        json!({ "events": audit, "next_before": null }),
    ));

    // OAuth.
    r.push(Route::json(
        "POST",
        "/api/v1/oauth/token",
        p("/oauth/token"),
        200,
        example(seed, "token")?,
    ));
    r.push(Route::json(
        "POST",
        "/api/v1/oauth/consent",
        p("/oauth/consent"),
        200,
        example(seed, "consent")?,
    ));
    r.push(Route::json(
        "GET",
        "/.well-known/oauth-protected-resource",
        "/.well-known/oauth-protected-resource".into(),
        200,
        get(seed, "protected_resource")?.clone(),
    ));
    r.push(Route::redirect(
        "/api/v1/oauth/authorize",
        p("/oauth/authorize"),
        "/ui/consent?client_id=claude-code&redirect_uri=http%3A%2F%2F127.0.0.1%3A33418%2Fcallback&scope=query%20mcp%3Atools&audience=code-index%2Fdevelopment&state=mock&code_challenge=mock&code_challenge_method=S256",
    ));
    r.push(Route::json(
        "GET",
        "/.well-known/oauth-authorization-server",
        "/.well-known/oauth-authorization-server".into(),
        200,
        get(seed, "oauth_metadata")?.clone(),
    ));
    r.push(Route::json(
        "GET",
        "/.well-known/jwks.json",
        "/.well-known/jwks.json".into(),
        200,
        get(seed, "jwks")?.clone(),
    ));

    // The engine's data API for the seeded namespaces (M1.6 wire contract
    // W7, W8), so the console's collection pages have data.
    r.push(Route::empty("GET", "/health", "/health".into()));
    r.push(Route::empty("GET", "/ready", "/ready".into()));
    for (ns, cols) in entries(seed, "collections")? {
        r.push(Route::json(
            "GET",
            "/v1/namespaces/{ns}/collections",
            format!("/v1/namespaces/{ns}/collections"),
            200,
            json!({ "collections": cols }),
        ));
        for c in cols.as_array().into_iter().flatten() {
            let name = str_field(c, "name")?;
            r.push(Route::json(
                "GET",
                "/v1/namespaces/{ns}/collections/{c}",
                format!("/v1/namespaces/{ns}/collections/{name}"),
                200,
                c.clone(),
            ));
        }
    }
    Ok(r)
}

/// The engine data API routes the mock serves beside the contract.
pub const ENGINE_TEMPLATES: [&str; 4] = [
    "/health",
    "/ready",
    "/v1/namespaces/{ns}/collections",
    "/v1/namespaces/{ns}/collections/{c}",
];

/// Installs `routes` on `server`.
pub async fn register(server: &MockServer, routes: &[Route]) -> Result<()> {
    for route in routes {
        let method: Method = route
            .method
            .parse()
            .map_err(|e| anyhow!("method {}: {e:?}", route.method))?;
        server
            .mock_async(|when, then| {
                let mut when = when.method(method).path(route.path.clone());
                for (k, v) in &route.query {
                    when = when.query_param(k.clone(), v.clone());
                }
                let mut then = then.status(route.status);
                if let Some(location) = &route.location {
                    then = then.header("location", location.clone());
                }
                if let Some(body) = &route.body {
                    then.header("content-type", "application/json")
                        .json_body(body.clone());
                }
            })
            .await;
    }
    Ok(())
}
