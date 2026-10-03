//! The route map (API1 plan Task 0): `docs/api/route-map.md` lists every
//! HTTP route the binary serves and every path of the console contract,
//! with the RPC that replaces it. This test reads the routers' source and
//! the contract, and fails when a route is missing from the map or the map
//! lists a route that no longer exists.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

/// A route: an upper-case HTTP method and an axum path.
type Route = (String, String);

const METHODS: [&str; 7] = ["get", "post", "put", "delete", "patch", "head", "options"];

/// How many `/v1` method+path pairs the as-built router serves (ruling 0.1 in
/// `docs/api/route-map.md`): 25 registered in `api::router` plus `hot` and
/// `warm`, which `hot::routes()` registers separately and `api::router`
/// merges. `/health`, `/ready` and the `/internal/*` routes are not app
/// routes, so they are not counted. Changing a router without updating this
/// number and ruling 0.1 fails here.
const APP_ROUTE_COUNT: usize = 27;

/// Whether a path is an app route (under `/v1`, but not the console's
/// `/api/v1`).
fn is_app_path(path: &str) -> bool {
    matches!(path.strip_prefix("/v1"), Some(rest) if rest.is_empty() || rest.starts_with('/'))
}

/// The `/v1` method+path pairs of a route set.
fn app_routes(routes: &BTreeSet<Route>) -> BTreeSet<Route> {
    routes
        .iter()
        .filter(|(_, path)| is_app_path(path))
        .cloned()
        .collect()
}

fn workspace() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The source files whose `.route(...)` calls make up the HTTP surface.
fn router_sources() -> Vec<PathBuf> {
    let root = workspace();
    let mut files: Vec<PathBuf> = fs::read_dir(root.join("crates/loams/src/api"))
        .expect("read crates/loams/src/api")
        .map(|entry| entry.expect("dir entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "rs"))
        .collect();
    files.sort();
    files.push(root.join("crates/loams/src/server.rs"));
    files.push(root.join("crates/loams-meta/src/rpc.rs"));
    files
}

/// Files that only define path constants the routers use.
fn constant_sources() -> Vec<PathBuf> {
    vec![workspace().join("crates/loams-hot/src/remote.rs")]
}

/// The non-test, non-comment part of a source file.
fn code(path: &Path) -> String {
    let text = fs::read_to_string(path).unwrap_or_else(|err| panic!("read {path:?}: {err}"));
    let text = match text.find("#[cfg(test)]") {
        Some(at) => &text[..at],
        None => &text[..],
    };
    text.lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// `name = "value"` pairs from `const NAME: &str = "…";` and
/// `let name = "…";`.
fn string_bindings(text: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        let rest = if let Some(rest) = line.strip_prefix("pub const ") {
            rest
        } else if let Some(rest) = line.strip_prefix("const ") {
            rest
        } else if let Some(rest) = line.strip_prefix("let ") {
            rest
        } else {
            continue;
        };
        let name: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        let Some(eq) = rest.find("= \"") else {
            continue;
        };
        let value = &rest[eq + 3..];
        let Some(end) = value.find('"') else { continue };
        if !name.is_empty() {
            out.push((name, value[..end].to_owned()));
        }
    }
    out
}

/// The text between the `(` at `open` and its matching `)`.
fn balanced(text: &str, open: usize) -> &str {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut in_str = false;
    for (i, &b) in bytes.iter().enumerate().skip(open) {
        match b {
            b'"' if i == 0 || bytes[i - 1] != b'\\' => in_str = !in_str,
            b'(' if !in_str => depth += 1,
            b')' if !in_str => {
                depth -= 1;
                if depth == 0 {
                    return &text[open + 1..i];
                }
            }
            _ => {}
        }
    }
    panic!("unbalanced parentheses after offset {open}");
}

/// Splits a call's arguments at the first top-level comma.
fn first_arg(args: &str) -> (&str, &str) {
    let mut depth = 0i32;
    let mut in_str = false;
    for (i, c) in args.char_indices() {
        match c {
            '"' => in_str = !in_str,
            '(' | '[' | '{' if !in_str => depth += 1,
            ')' | ']' | '}' if !in_str => depth -= 1,
            ',' if !in_str && depth == 0 => return (args[..i].trim(), args[i + 1..].trim()),
            _ => {}
        }
    }
    panic!("no handler argument in .route({args})");
}

fn lookup(bindings: &[(String, String)], name: &str) -> String {
    bindings
        .iter()
        .rev()
        .find(|(n, _)| n == name)
        .map(|(_, v)| v.clone())
        .unwrap_or_else(|| panic!("route path uses `{name}`, which is not a string constant"))
}

/// Renders a `format!` string: `{{`/`}}` are braces, `{name}` a binding.
fn render(format: &str, bindings: &[(String, String)]) -> String {
    let mut out = String::new();
    let mut rest = format;
    while let Some(c) = rest.chars().next() {
        if let Some(tail) = rest.strip_prefix("{{") {
            out.push('{');
            rest = tail;
        } else if let Some(tail) = rest.strip_prefix("}}") {
            out.push('}');
            rest = tail;
        } else if c == '{' {
            let end = rest.find('}').expect("closing brace in format string");
            out.push_str(&lookup(bindings, &rest[1..end]));
            rest = &rest[end + 1..];
        } else {
            out.push(c);
            rest = &rest[c.len_utf8()..];
        }
    }
    out
}

fn route_path(expr: &str, bindings: &[(String, String)]) -> String {
    let expr = expr.trim();
    if let Some(literal) = expr.strip_prefix('"') {
        return literal.trim_end_matches('"').to_owned();
    }
    if let Some(inner) = expr.strip_prefix("&format!(") {
        let inner = inner.trim_end_matches(')').trim();
        let literal = inner.trim_start_matches('"').trim_end_matches('"');
        return render(literal, bindings);
    }
    lookup(bindings, expr.trim_start_matches('&'))
}

/// The HTTP methods a handler expression such as `post(a).get(b)` serves.
fn route_methods(handler: &str) -> Vec<String> {
    let bytes = handler.as_bytes();
    let mut out = Vec::new();
    for method in METHODS {
        let mut from = 0;
        while let Some(at) = handler[from..].find(&format!("{method}(")) {
            let at = from + at;
            let boundary =
                at == 0 || !(bytes[at - 1].is_ascii_alphanumeric() || bytes[at - 1] == b'_');
            if boundary {
                out.push(method.to_ascii_uppercase());
            }
            from = at + method.len();
        }
    }
    out
}

/// Every route in the routers' source.
fn served_routes() -> BTreeSet<Route> {
    let sources = router_sources();
    let mut constants = Vec::new();
    for path in sources.iter().chain(constant_sources().iter()) {
        constants.extend(
            string_bindings(&code(path))
                .into_iter()
                .filter(|(name, _)| name.chars().all(|c| c.is_ascii_uppercase() || c == '_')),
        );
    }
    let mut routes = BTreeSet::new();
    for path in &sources {
        let text = code(path);
        let mut bindings = constants.clone();
        bindings.extend(string_bindings(&text));
        let mut from = 0;
        while let Some(at) = text[from..].find(".route(") {
            let open = from + at + ".route".len();
            let args = balanced(&text, open);
            let (path_expr, handler) = first_arg(args);
            let route = route_path(path_expr, &bindings);
            let methods = route_methods(handler);
            assert!(
                !methods.is_empty(),
                "no method in .route({args}) in {path:?}"
            );
            for method in methods {
                routes.insert((method, route.clone()));
            }
            from = open;
        }
    }
    routes
}

/// Every operation of the console contract.
fn contract_routes() -> BTreeSet<Route> {
    let path = workspace().join("api/console/openapi.json");
    let text = fs::read_to_string(&path).expect("read the console contract");
    let contract: serde_json::Value = serde_json::from_str(&text).expect("contract is JSON");
    let mut routes = BTreeSet::new();
    for (route, item) in contract["paths"].as_object().expect("paths") {
        for method in METHODS {
            if item.get(method).is_some() {
                routes.insert((method.to_ascii_uppercase(), route.clone()));
            }
        }
    }
    routes
}

/// The `| METHOD | `path` | …` rows of the route map.
fn mapped_routes() -> BTreeSet<Route> {
    let path = workspace().join("docs/api/route-map.md");
    let text = fs::read_to_string(&path).expect("read docs/api/route-map.md");
    let mut routes = BTreeSet::new();
    for line in text.lines() {
        let cells: Vec<&str> = line.split('|').map(str::trim).collect();
        if cells.len() < 4 || !cells[0].is_empty() {
            continue;
        }
        let method = cells[1];
        if !METHODS.iter().any(|m| m.eq_ignore_ascii_case(method))
            || method != method.to_ascii_uppercase()
        {
            continue;
        }
        let Some(route) = cells[2].strip_prefix('`').and_then(|r| r.strip_suffix('`')) else {
            panic!("route-map row without a backticked path: {line}");
        };
        routes.insert((method.to_owned(), route.to_owned()));
    }
    routes
}

#[test]
fn route_map_covers_every_route() {
    let mut actual = served_routes();
    // Sanity: the parser found the routers, not an empty set.
    assert!(actual.contains(&("POST".into(), "/v1/namespaces/{ns}/query".into())));
    assert!(actual.contains(&("POST".into(), "/internal/v1/reads/{op}".into())));
    assert!(actual.contains(&(
        "PUT".into(),
        "/v1/namespaces/{ns}/collections/{c}/hot".into()
    )));
    // The exact count of the app routes, so the coverage check above cannot
    // stay green while ruling 0.1 claims a different number.
    assert_eq!(
        app_routes(&actual).len(),
        APP_ROUTE_COUNT,
        "the routers serve {APP_ROUTE_COUNT} `/v1` method+path pairs; a route was added or removed, so update this number, ruling 0.1 of docs/api/route-map.md and the execution ruling in docs/plans/2026-10-02-api1-unified-connect.md"
    );
    actual.extend(contract_routes());
    let mapped = mapped_routes();
    assert_eq!(
        app_routes(&mapped).len(),
        APP_ROUTE_COUNT,
        "docs/api/route-map.md must list exactly the {APP_ROUTE_COUNT} `/v1` method+path pairs the routers serve, the count ruling 0.1 records"
    );
    let missing: Vec<_> = actual.difference(&mapped).collect();
    let stale: Vec<_> = mapped.difference(&actual).collect();
    assert!(
        missing.is_empty() && stale.is_empty(),
        "docs/api/route-map.md is out of date.\nmissing from the map: {missing:#?}\nmapped but not served: {stale:#?}"
    );
}

#[test]
fn chained_head_and_options_are_inventoried() {
    let methods = route_methods("get(read).head(head_only).options(preflight)");
    assert_eq!(methods, vec!["GET", "HEAD", "OPTIONS"]);
}
