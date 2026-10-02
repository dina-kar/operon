//! `loams://` deep links are navigation only (§37 §6.7, D432, AP1 Ruling 10).
//!
//! Deep links are reachable from any web page or document, so they are
//! parsed in Rust against an allowlist and the console receives a typed
//! route, never a raw URL. No deep link performs an action: there is no
//! "approve", "start" or "delete" link.

use serde::Serialize;
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DeepLink {
    /// `loams://open/<env>/<console path>`
    Open { env: String, path: String },
    /// `loams://approvals/<id>`
    Approval { id: String },
    /// `loams://stacks/<name>`
    Stack { name: String },
}

impl DeepLink {
    /// The console route this link navigates to.
    #[must_use]
    pub fn route(&self) -> String {
        match self {
            Self::Open { path, .. } => format!("/{path}"),
            Self::Approval { .. } => "/approvals".into(),
            Self::Stack { name } => format!("/stacks/{name}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DeepLinkError {
    #[error("not a loams:// link")]
    Scheme,
    #[error("unknown deep link")]
    Unknown,
    #[error("invalid segment")]
    Segment,
}

const MAX_LEN: usize = 512;

/// A path segment: ids, names and console path parts.
fn segment(s: &str) -> Result<String, DeepLinkError> {
    let ok = !s.is_empty()
        && s.len() <= 128
        && s != "."
        && s != ".."
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b));
    ok.then(|| s.to_owned()).ok_or(DeepLinkError::Segment)
}

/// Parses a `loams://` URL against the allowlist.
///
/// # Errors
///
/// Anything that is not exactly one of the three patterns.
pub fn parse(raw: &str) -> Result<DeepLink, DeepLinkError> {
    if raw.len() > MAX_LEN {
        return Err(DeepLinkError::Unknown);
    }
    let url = Url::parse(raw).map_err(|_| DeepLinkError::Scheme)?;
    if url.scheme() != "loams" {
        return Err(DeepLinkError::Scheme);
    }
    if url.query().is_some() || url.fragment().is_some() || !url.username().is_empty() {
        return Err(DeepLinkError::Unknown);
    }
    // `loams://approvals/x`: the first part parses as the host.
    let host = url.host_str().ok_or(DeepLinkError::Unknown)?;
    let rest: Vec<&str> = url.path().trim_start_matches('/').split('/').collect();
    if raw.contains("%2") || raw.contains('\\') || raw.contains("..") {
        return Err(DeepLinkError::Segment);
    }
    match (host, rest.as_slice()) {
        ("approvals", [id]) => Ok(DeepLink::Approval { id: segment(id)? }),
        ("stacks", [name]) => Ok(DeepLink::Stack {
            name: segment(name)?,
        }),
        ("open", [env, path @ ..]) if !path.is_empty() && path.len() <= 8 => {
            let env = segment(env)?;
            let parts = path
                .iter()
                .map(|p| segment(p))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(DeepLink::Open {
                env,
                path: parts.join("/"),
            })
        }
        _ => Err(DeepLinkError::Unknown),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng as _;

    #[test]
    fn deeplink_parses_allowlisted_patterns() {
        assert_eq!(
            parse("loams://approvals/apr_01J9ZDROPDOCS"),
            Ok(DeepLink::Approval {
                id: "apr_01J9ZDROPDOCS".into()
            })
        );
        assert_eq!(
            parse("loams://stacks/dev"),
            Ok(DeepLink::Stack { name: "dev".into() })
        );
        let open = parse("loams://open/env_prod/namespaces").unwrap();
        assert_eq!(
            open,
            DeepLink::Open {
                env: "env_prod".into(),
                path: "namespaces".into()
            }
        );
        assert_eq!(open.route(), "/namespaces");
    }

    #[test]
    fn deeplink_rejects_unknown_patterns() {
        for raw in [
            "loams://approvals/apr_1/approve",
            "loams://approve/apr_1",
            "loams://stacks/dev/start",
            "loams://stacks",
            "loams://open/env_prod",
            "loams://approvals/apr_1?decision=approve",
            "loams://approvals/apr_1#x",
            "https://approvals/apr_1",
            "javascript:alert(1)",
        ] {
            assert!(parse(raw).is_err(), "{raw}");
        }
    }

    #[test]
    fn deeplink_rejects_path_traversal() {
        for raw in [
            "loams://open/env/../../etc/passwd",
            "loams://open/env/%2e%2e/x",
            "loams://stacks/%2F..",
            "loams://open/env/a\\b",
            "loams://stacks/..",
        ] {
            assert!(parse(raw).is_err(), "{raw}");
        }
    }

    #[test]
    fn deeplink_never_dispatches_mutations() {
        // A property test over random links: whatever parses is one of the
        // three navigations, and its route is a plain console path.
        let mut rng = rand::rng();
        let alphabet: Vec<char> = "abc/._-%2e?#:&=approvalsstacksopen01".chars().collect();
        for _ in 0..20_000 {
            let len = rng.random_range(0..40);
            let tail: String = (0..len)
                .map(|_| alphabet[rng.random_range(0..alphabet.len())])
                .collect();
            let raw = format!("loams://{tail}");
            if let Ok(link) = parse(&raw) {
                let route = link.route();
                assert!(
                    route.starts_with('/') && !route.contains(".."),
                    "{raw} → {route}"
                );
                assert!(
                    !route.contains('?') && !route.contains('#'),
                    "{raw} → {route}"
                );
            }
        }
    }
}
