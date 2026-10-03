//! The blessed statements table: one TSV row per digest (§31 §15 step 6).
//!
//! Columns: `digest, component, source, example, class, ref_hash, target_hash, note, issue`.
//! Tabs, newlines, carriage returns and backslashes inside a field are written as `\t`, `\n`,
//! `\r` and `\\`, so a row is always one line.

use crate::classify::Class;

pub const HEADER: [&str; 9] = [
    "digest",
    "component",
    "source",
    "example",
    "class",
    "ref_hash",
    "target_hash",
    "note",
    "issue",
];

/// Which engine's component vocabulary applies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Engine {
    Postgres,
    Mysql,
}

impl Engine {
    pub fn components(self) -> &'static [&'static str] {
        match self {
            Engine::Postgres => &[
                "pool",
                "health",
                "schema-sync",
                "copy",
                "replication",
                "2pc",
                "query",
            ],
            Engine::Mysql => &[
                "query",
                "health",
                "schema-engine",
                "sidecar",
                "vreplication",
                "vdiff",
                "2pc",
                "onlineddl",
                "reparent",
            ],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    pub digest: String,
    pub component: String,
    pub source: String,
    pub example: String,
    pub class: Class,
    pub ref_hash: String,
    pub target_hash: String,
    pub note: String,
    pub issue: String,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TsvError {
    #[error("row {digest}: class unsupported needs a non-empty note")]
    UnsupportedNeedsNote { digest: String },
    #[error("row {digest}: unknown component {component:?}")]
    UnknownComponent { digest: String, component: String },
    #[error("line {line}: expected 9 columns, found {found}")]
    Columns { line: usize, found: usize },
    #[error("line {line}: unknown class {class:?}")]
    UnknownClass { line: usize, class: String },
    #[error("missing or wrong header")]
    Header,
}

impl Row {
    /// Checks the rules a row must obey before it is written or after it is read.
    pub fn validate(&self, engine: Engine) -> Result<(), TsvError> {
        if self.class == Class::Unsupported && self.note.trim().is_empty() {
            return Err(TsvError::UnsupportedNeedsNote {
                digest: self.digest.clone(),
            });
        }
        if !engine.components().contains(&self.component.as_str()) {
            return Err(TsvError::UnknownComponent {
                digest: self.digest.clone(),
                component: self.component.clone(),
            });
        }
        Ok(())
    }
}

fn escape(s: &str) -> String {
    let mut o = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => o.push_str("\\\\"),
            '\t' => o.push_str("\\t"),
            '\n' => o.push_str("\\n"),
            '\r' => o.push_str("\\r"),
            c => o.push(c),
        }
    }
    o
}

fn unescape(s: &str) -> String {
    let mut o = String::with_capacity(s.len());
    let mut it = s.chars();
    while let Some(c) = it.next() {
        if c != '\\' {
            o.push(c);
            continue;
        }
        match it.next() {
            Some('t') => o.push('\t'),
            Some('n') => o.push('\n'),
            Some('r') => o.push('\r'),
            Some('\\') => o.push('\\'),
            Some(x) => {
                o.push('\\');
                o.push(x);
            }
            None => o.push('\\'),
        }
    }
    o
}

/// Renders rows, sorted by (component, digest), after validating each.
pub fn write(rows: &[Row], engine: Engine) -> Result<String, TsvError> {
    let mut sorted: Vec<&Row> = rows.iter().collect();
    sorted.sort_by(|a, b| (&a.component, &a.digest).cmp(&(&b.component, &b.digest)));
    let mut out = HEADER.join("\t");
    out.push('\n');
    for r in sorted {
        r.validate(engine)?;
        let cols = [
            &r.digest,
            &r.component,
            &r.source,
            &r.example,
            &r.class.as_str().to_string(),
            &r.ref_hash,
            &r.target_hash,
            &r.note,
            &r.issue,
        ];
        let line: Vec<String> = cols.iter().map(|c| escape(c)).collect();
        out.push_str(&line.join("\t"));
        out.push('\n');
    }
    Ok(out)
}

/// Parses a table written by [`write`], validating each row.
pub fn read(text: &str, engine: Engine) -> Result<Vec<Row>, TsvError> {
    let mut lines = text.lines();
    if lines.next().map(|h| h.split('\t').collect::<Vec<_>>()) != Some(HEADER.to_vec()) {
        return Err(TsvError::Header);
    }
    let mut rows = Vec::new();
    for (i, line) in lines.enumerate() {
        let n = i + 2;
        if line.is_empty() {
            continue;
        }
        let f: Vec<String> = line.split('\t').map(unescape).collect();
        if f.len() != 9 {
            return Err(TsvError::Columns {
                line: n,
                found: f.len(),
            });
        }
        let class = Class::parse(&f[4]).ok_or_else(|| TsvError::UnknownClass {
            line: n,
            class: f[4].clone(),
        })?;
        let row = Row {
            digest: f[0].clone(),
            component: f[1].clone(),
            source: f[2].clone(),
            example: f[3].clone(),
            class,
            ref_hash: f[5].clone(),
            target_hash: f[6].clone(),
            note: f[7].clone(),
            issue: f[8].clone(),
        };
        row.validate(engine)?;
        rows.push(row);
    }
    Ok(rows)
}

/// Counts of rows per (component, class), for the as-built notes.
pub fn summarize(rows: &[Row]) -> std::collections::BTreeMap<(String, &'static str), usize> {
    let mut m = std::collections::BTreeMap::new();
    for r in rows {
        *m.entry((r.component.clone(), r.class.as_str()))
            .or_insert(0) += 1;
    }
    m
}
