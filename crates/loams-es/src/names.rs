//! Index names and index expressions (Task 1 rules 9 and 10; Ruling 9).
//!
//! Aliases are read only through `CollectionService::{resolve_name,
//! list_aliases}`: an alias names a set of collections, at most one of them
//! its write target (D57).

use std::collections::BTreeMap;

use loams_query::{AliasInfo, CollectionService, NameInfo, ServiceError};

use crate::error::{ErrorContext, EsError};

/// The characters an index name must not contain (ES's
/// `Strings.INVALID_FILENAME_CHARS`, in ES's order).
const INVALID_CHARS: [char; 10] = [' ', ',', '"', '*', '\\', '<', '|', ',', '>', '/'];
/// ES's rendering of [`INVALID_CHARS`] (`?` included).
const INVALID_CHARS_TEXT: &str = r#"[' ','"','*',',','/','<','>','?','\','|']"#;

/// ES's index-name rules; each violation is 400 `invalid_index_name_exception`.
pub fn validate_index_name(name: &str) -> Result<(), EsError> {
    let invalid = |why: String| {
        EsError::new(
            400,
            "invalid_index_name_exception",
            format!("Invalid index name [{name}], {why}"),
        )
        .with("index_uuid", "_na_")
        .with("index", name)
    };
    if name.is_empty() {
        return Err(invalid("must not be empty".to_string()));
    }
    if name.contains(INVALID_CHARS) || name.contains('?') {
        return Err(invalid(format!(
            "must not contain the following characters {INVALID_CHARS_TEXT}"
        )));
    }
    if name.contains('#') {
        return Err(invalid("must not contain '#'".to_string()));
    }
    if name.contains(':') {
        return Err(invalid("must not contain ':'".to_string()));
    }
    if name.starts_with(['_', '-', '+']) {
        return Err(invalid("must not start with '_', '-', or '+'".to_string()));
    }
    if name.len() > 255 {
        return Err(invalid(format!(
            "index name is too long, ({} > 255)",
            name.len()
        )));
    }
    if name == "." || name == ".." {
        return Err(invalid("must not be '.' or '..'".to_string()));
    }
    if name.to_lowercase() != name {
        return Err(invalid("must be lowercase".to_string()));
    }
    Ok(())
}

/// An index expression from a path segment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexExpr {
    /// `_all` or `*`.
    All,
    /// Names, aliases and `*` patterns.
    List(Vec<String>),
}

impl IndexExpr {
    /// Splits on `,`; `_all` and `*` alone are [`IndexExpr::All`].
    pub fn parse(path_segment: &str) -> IndexExpr {
        match path_segment {
            "_all" | "*" => IndexExpr::All,
            _ => IndexExpr::List(
                path_segment
                    .split(',')
                    .filter(|item| !item.is_empty())
                    .map(str::to_string)
                    .collect(),
            ),
        }
    }
}

/// A concrete index an expression covers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resolved {
    /// The collection.
    pub name: String,
    /// The alias it was reached through, if any.
    pub via_alias: Option<String>,
}

/// Where a write goes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteTarget {
    /// The collection.
    pub name: String,
    /// The alias it was reached through, if any.
    pub via_alias: Option<String>,
}

/// ES's `IndicesOptions`, as far as Phase A reads them.
#[derive(Clone, Copy, Debug)]
pub struct ResolveOptions {
    /// A missing concrete name is left out rather than 404.
    pub ignore_unavailable: bool,
    /// A wildcard matching nothing is allowed.
    pub allow_no_indices: bool,
    /// `*` is a pattern; otherwise it is part of a (missing) name.
    pub allow_wildcards: bool,
}

impl Default for ResolveOptions {
    fn default() -> Self {
        Self {
            ignore_unavailable: false,
            allow_no_indices: true,
            allow_wildcards: true,
        }
    }
}

fn service_error(error: ServiceError) -> EsError {
    EsError::from_service(error, ErrorContext::Read)
}

fn is_missing(error: &ServiceError) -> bool {
    matches!(
        error,
        ServiceError::NotFound {
            kind: "collection" | "alias",
            ..
        }
    )
}

/// Whether `name` matches `pattern`, where `*` matches any run of
/// characters.
pub(crate) fn glob(pattern: &str, name: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    let (first, rest) = parts.split_first().unwrap_or((&"", &[]));
    let Some(mut tail) = name.strip_prefix(first) else {
        return false;
    };
    let Some((last, middle)) = rest.split_last() else {
        return tail.is_empty();
    };
    for part in middle {
        match tail.find(part) {
            Some(at) => tail = &tail[at + part.len()..],
            None => return false,
        }
    }
    tail.len() >= last.len() && tail.ends_with(last)
}

/// The collections `expr` covers in `ns`, deduplicated by name (a direct
/// hit wins over an alias) and sorted by name (rule 10).
pub async fn resolve(
    service: &CollectionService,
    ns: &str,
    expr: &IndexExpr,
    opts: ResolveOptions,
) -> Result<Vec<Resolved>, EsError> {
    let mut found: BTreeMap<String, Option<String>> = BTreeMap::new();
    let mut add = |name: &str, via: Option<&str>| match found.get_mut(name) {
        Some(existing) if via.is_none() => *existing = None,
        Some(_) => {}
        None => {
            found.insert(name.to_string(), via.map(str::to_string));
        }
    };
    let patterns: Vec<String> = match expr {
        IndexExpr::All => vec!["*".to_string()],
        IndexExpr::List(items) => items.clone(),
    };
    let mut catalog: Option<(Vec<String>, Vec<AliasInfo>)> = None;
    for item in &patterns {
        if opts.allow_wildcards && item.contains('*') {
            if catalog.is_none() {
                let collections = service
                    .collection_records(ns)
                    .await
                    .map_err(service_error)?
                    .into_iter()
                    .map(|c| c.name)
                    .collect();
                let aliases = service.list_aliases(ns).await.map_err(service_error)?;
                catalog = Some((collections, aliases));
            }
            let Some((collections, aliases)) = &catalog else {
                continue;
            };
            let mut matched = false;
            for name in collections.iter().filter(|name| glob(item, name)) {
                add(name, None);
                matched = true;
            }
            for alias in aliases.iter().filter(|alias| glob(item, &alias.alias)) {
                for member in &alias.members {
                    add(&member.collection, Some(&alias.alias));
                }
                matched = true;
            }
            if !matched && !opts.allow_no_indices {
                return Err(EsError::index_not_found(item));
            }
            continue;
        }
        match service.resolve_name(ns, item).await {
            Ok(NameInfo::Collection(name)) => add(&name, None),
            Ok(NameInfo::Alias(alias)) => {
                for member in &alias.members {
                    add(&member.collection, Some(&alias.alias));
                }
            }
            Err(err) if is_missing(&err) => {
                if !opts.ignore_unavailable {
                    return Err(EsError::index_not_found(item));
                }
            }
            Err(err) => return Err(service_error(err)),
        }
    }
    Ok(found
        .into_iter()
        .map(|(name, via_alias)| Resolved { name, via_alias })
        .collect())
}

/// The index a write to `name` goes to (Ruling 9); `Ok(None)` when `name` is
/// neither an index nor an alias (auto-create decides). A comma list or a
/// wildcard is not a write target: ES refuses it as an invalid index name
/// (row T1-5).
pub async fn resolve_write(
    service: &CollectionService,
    ns: &str,
    name: &str,
) -> Result<Option<WriteTarget>, EsError> {
    if name.contains([',', '*']) {
        validate_index_name(name)?;
    }
    match service.resolve_name(ns, name).await {
        Ok(NameInfo::Collection(collection)) => Ok(Some(WriteTarget {
            name: collection,
            via_alias: None,
        })),
        Ok(NameInfo::Alias(alias)) => match alias.write_target {
            Some(target) => Ok(Some(WriteTarget {
                name: target,
                via_alias: Some(alias.alias),
            })),
            None => Err(EsError::illegal_argument(format!(
                "no write index is defined for alias [{}]. The write index may be explicitly \
                 disabled using is_write_index=false or the alias points to multiple indices \
                 without one being designated as a write index",
                alias.alias
            ))),
        },
        Err(err) if is_missing(&err) => Ok(None),
        Err(err) => Err(service_error(err)),
    }
}

/// The one index a single-document read of `name` addresses (Ruling 9).
pub async fn resolve_single(
    service: &CollectionService,
    ns: &str,
    name: &str,
) -> Result<Resolved, EsError> {
    match service.resolve_name(ns, name).await {
        Ok(NameInfo::Collection(collection)) => Ok(Resolved {
            name: collection,
            via_alias: None,
        }),
        Ok(NameInfo::Alias(alias)) => {
            let mut members: Vec<String> =
                alias.members.iter().map(|m| m.collection.clone()).collect();
            members.sort();
            match members.as_slice() {
                [only] => Ok(Resolved {
                    name: only.clone(),
                    via_alias: Some(alias.alias),
                }),
                _ => Err(EsError::illegal_argument(format!(
                    "alias [{}] has more than one index associated with it [{}], can't execute \
                     a single index op",
                    alias.alias,
                    members.join(", ")
                ))),
            }
        }
        Err(err) if is_missing(&err) => Err(EsError::index_not_found(name)),
        Err(err) => Err(service_error(err)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn why(name: &str) -> String {
        validate_index_name(name).expect_err(name).reason
    }

    #[test]
    fn index_names_follow_es_rules() {
        assert!(why("Abc").ends_with("must be lowercase"), "{}", why("Abc"));
        assert!(why("_x").contains("must not start with '_', '-', or '+'"));
        assert!(why("-x").contains("must not start with"));
        for bad in ["a b", "a*b", "a,b", "a/b", "a?b", "a\"b", "a<b", "a|b"] {
            assert!(
                why(bad).contains(
                    r#"must not contain the following characters [' ','"','*',',','/','<','>','?','\','|']"#
                ),
                "{bad}: {}",
                why(bad)
            );
        }
        assert!(why("a#b").contains("must not contain '#'"));
        assert!(why("a:b").contains("must not contain ':'"));
        assert!(why(".").contains("must not be '.' or '..'"));
        assert!(why("").contains("must not be empty"));
        let long = "a".repeat(256);
        assert_eq!(
            why(&long),
            format!("Invalid index name [{long}], index name is too long, (256 > 255)")
        );
        let error = validate_index_name("Abc").expect_err("Abc");
        assert_eq!(error.status, 400);
        assert_eq!(error.kind, "invalid_index_name_exception");
        assert_eq!(error.extra["index"], "Abc");
        assert_eq!(error.extra["index_uuid"], "_na_");
        for good in ["test_0f3a", "logs-2026.09", &"a".repeat(255), ".hidden"] {
            assert!(validate_index_name(good).is_ok(), "{good}");
        }
    }

    #[test]
    fn expressions_split_on_commas() {
        assert_eq!(IndexExpr::parse("_all"), IndexExpr::All);
        assert_eq!(IndexExpr::parse("*"), IndexExpr::All);
        assert_eq!(
            IndexExpr::parse("a,b*,,c"),
            IndexExpr::List(vec!["a".into(), "b*".into(), "c".into()])
        );
    }

    #[test]
    fn globs_match_like_es_wildcards() {
        assert!(glob("*", "anything"));
        assert!(glob("logs-*", "logs-2026"));
        assert!(!glob("logs-*", "log"));
        assert!(glob("*-2026", "logs-2026"));
        assert!(glob("a*b*c", "aXbYc"));
        assert!(!glob("a*b*c", "aXcYb"));
        assert!(glob("ab*b", "abb"));
        assert!(!glob("ab*ab", "ab"));
        assert!(glob("exact", "exact"));
        assert!(!glob("exact", "exactly"));
    }
}
