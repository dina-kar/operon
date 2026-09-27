//! The plan step's listing (D1 Task 8 semantics 1): the source's files,
//! filtered by the pattern and sorted by key.

use serde::{Deserialize, Serialize};

use super::{ImportEnv, ImportError, ImportRequest};

/// A file as the plan fixes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannedFile {
    /// The key after the source prefix.
    pub key: String,
    pub size: u64,
    /// Every read of the file is conditional on it (`If-Match`); a store
    /// without etags reads unconditionally.
    pub etag: Option<String>,
}

/// The files of `request.source` that match its pattern, sorted by key.
pub(super) async fn list(
    env: &ImportEnv,
    request: &ImportRequest,
) -> Result<Vec<PlannedFile>, ImportError> {
    let store = env
        .sources
        .open(&request.source)
        .map_err(|e| ImportError::Invalid(format!("source {:?}: {e}", request.source)))?;
    let infos = store.list("").await.map_err(|e| {
        ImportError::Unavailable(format!("listing source {:?}: {e}", request.source))
    })?;
    let mut files: Vec<PlannedFile> = infos
        .into_iter()
        .filter(|info| {
            request
                .pattern
                .as_deref()
                .is_none_or(|pattern| glob_matches(pattern, &info.path))
        })
        .map(|info| PlannedFile {
            key: info.path,
            size: info.size,
            etag: info.version.e_tag,
        })
        .collect();
    files.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(files)
}

/// Whether `text` matches `pattern`: `*` is any run of characters (`/`
/// included), `?` any one character, everything else itself.
pub fn glob_matches(pattern: &str, text: &str) -> bool {
    let (p, t): (Vec<char>, Vec<char>) = (pattern.chars().collect(), text.chars().collect());
    let (mut pi, mut ti) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some((pi, ti));
            pi += 1;
        } else if let Some((sp, st)) = star {
            pi = sp + 1;
            ti = st + 1;
            star = Some((sp, st + 1));
        } else {
            return false;
        }
    }
    p[pi..].iter().all(|&c| c == '*')
}

#[cfg(test)]
mod tests {
    use super::glob_matches;

    #[test]
    fn globs_match_the_key_suffix() {
        assert!(glob_matches("*.parquet", "a.parquet"));
        assert!(glob_matches("*.parquet", "day=1/a.parquet"));
        assert!(!glob_matches("*.parquet", "a.parquet.tmp"));
        assert!(glob_matches("part-?.json", "part-1.json"));
        assert!(!glob_matches("part-?.json", "part-12.json"));
        assert!(glob_matches("*", ""));
        assert!(glob_matches("a*b*c", "aXbYc"));
        assert!(!glob_matches("a*b*c", "aXbY"));
    }
}
