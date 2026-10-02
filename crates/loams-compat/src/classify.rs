//! Results, canonical hashes and the class of a replayed statement.

use sha2::{Digest, Sha256};

/// What running one statement produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Rows(ResultSet),
    Failed(DbError),
}

/// A result set in text form. `ordered` is true when the statement has a total `ORDER BY`
/// (approximated by the keyword), in which case row order is part of the hash.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResultSet {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
    /// Command tags and affected-row counts, in statement order (`INSERT 0 1`, `affected:3`).
    pub tags: Vec<String>,
    pub warnings: Vec<String>,
    pub ordered: bool,
}

/// An engine error. `code` is the SQLSTATE (Postgres) or the numeric error code (MySQL); the
/// message is kept for the note but never hashed, because messages embed names and positions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DbError {
    pub code: String,
    pub message: String,
}

/// The five classes of §31 §15 step 4 plus `pending-target` (Ruling 7).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    Same,
    Differs,
    Error,
    Unsupported,
    PendingTarget,
}

impl Class {
    pub const ALL: [Class; 5] = [
        Class::Same,
        Class::Differs,
        Class::Error,
        Class::Unsupported,
        Class::PendingTarget,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Class::Same => "same",
            Class::Differs => "differs",
            Class::Error => "error",
            Class::Unsupported => "unsupported",
            Class::PendingTarget => "pending-target",
        }
    }

    pub fn parse(s: &str) -> Option<Class> {
        Class::ALL.into_iter().find(|c| c.as_str() == s)
    }
}

/// The classifier's verdict for one statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Classified {
    pub class: Class,
    pub ref_hash: String,
    pub target_hash: String,
    pub note: String,
}

fn feed(h: &mut Sha256, s: &str) {
    h.update((s.len() as u64).to_be_bytes());
    h.update(s.as_bytes());
}

fn feed_cell(h: &mut Sha256, c: &Option<String>) {
    match c {
        None => h.update([0u8]),
        Some(v) => {
            h.update([1u8]);
            feed(h, v);
        }
    }
}

/// The canonical hash of an outcome: column names, rows (sorted unless ordered), warnings.
/// An error hashes its code only.
pub fn canonical_hash(o: &Outcome) -> String {
    let mut h = Sha256::new();
    match o {
        Outcome::Failed(e) => {
            h.update(b"error");
            feed(&mut h, &e.code);
        }
        Outcome::Rows(r) => {
            h.update(b"rows");
            for c in &r.columns {
                feed(&mut h, c);
            }
            let mut rows = r.rows.clone();
            if !r.ordered {
                rows.sort();
            }
            for row in &rows {
                h.update(b"r");
                for c in row {
                    feed_cell(&mut h, c);
                }
            }
            for t in &r.tags {
                h.update(b"t");
                feed(&mut h, t);
            }
            let mut w = r.warnings.clone();
            w.sort();
            for x in &w {
                h.update(b"w");
                feed(&mut h, x);
            }
        }
    }
    hex::encode(h.finalize())
}

/// The shape of an outcome: column names and row count only. Used where the reference itself
/// is not deterministic (clocks, LSNs, counters), so that content cannot be compared.
pub fn shape_hash(o: &Outcome) -> String {
    let mut h = Sha256::new();
    match o {
        Outcome::Failed(e) => {
            h.update(b"error");
            feed(&mut h, &e.code);
        }
        Outcome::Rows(r) => {
            h.update(b"shape");
            for c in &r.columns {
                feed(&mut h, c);
            }
            h.update((r.rows.len() as u64).to_be_bytes());
            for t in &r.tags {
                h.update(b"t");
                feed(&mut h, t);
            }
        }
    }
    hex::encode(h.finalize())
}

/// The column names of an outcome only (or the error code). Used for statements that read an instance's
/// identity, whose row counts differ between engines too (`SHOW BINARY LOGS`).
pub fn columns_hash(o: &Outcome) -> String {
    let mut h = Sha256::new();
    match o {
        Outcome::Failed(e) => {
            h.update(b"error");
            feed(&mut h, &e.code);
        }
        Outcome::Rows(r) => {
            h.update(b"columns");
            for c in &r.columns {
                feed(&mut h, c);
            }
        }
    }
    hex::encode(h.finalize())
}

/// What the replay learned about one statement.
#[derive(Clone, Debug)]
pub struct Observation<'a> {
    pub reference: &'a Outcome,
    /// The reference run a second time, in a fresh connection. When its canonical hash differs
    /// from `reference`, the statement is volatile and results compare by shape.
    pub reference_again: Option<&'a Outcome>,
    /// `None` when the target is not runnable (Ruling 7).
    pub target: Option<&'a Outcome>,
    /// The reason from the unsupported list, when the owner has ruled that the target refuses
    /// this statement by design.
    pub unsupported_note: Option<&'a str>,
    /// Compare by shape whatever the reference does: the statement reads an instance's identity
    /// (version, uuid, binlog position), which two engines never share.
    pub shape_only: bool,
}

/// Classify one statement.
pub fn classify(obs: &Observation<'_>) -> Classified {
    let volatile = obs.shape_only
        || obs
            .reference_again
            .is_some_and(|a| canonical_hash(a) != canonical_hash(obs.reference));
    let hash = |o: &Outcome| {
        if obs.shape_only {
            columns_hash(o)
        } else if volatile {
            shape_hash(o)
        } else {
            canonical_hash(o)
        }
    };
    let ref_hash = hash(obs.reference);
    let mut notes: Vec<String> = Vec::new();
    if obs.shape_only {
        notes.push(
            "instance identity (version, uuid, binlog position): compared by column names"
                .to_string(),
        );
    } else if volatile {
        notes.push("volatile on the reference: compared by shape".to_string());
    }
    let Some(target) = obs.target else {
        if let Outcome::Failed(e) = obs.reference {
            notes.push(format!(
                "reference error {}: {}",
                e.code,
                one_line(&e.message)
            ));
        }
        return Classified {
            class: Class::PendingTarget,
            ref_hash,
            target_hash: String::new(),
            note: notes.join("; "),
        };
    };
    let target_hash = hash(target);
    let class = if ref_hash == target_hash {
        Class::Same
    } else {
        match (obs.reference, target) {
            (Outcome::Rows(_), Outcome::Failed(te)) => {
                notes.push(format!(
                    "target error {}: {}",
                    te.code,
                    one_line(&te.message)
                ));
                match obs.unsupported_note {
                    Some(n) => {
                        notes.push(n.to_string());
                        Class::Unsupported
                    }
                    None => Class::Error,
                }
            }
            (Outcome::Failed(re), Outcome::Rows(_)) => {
                notes.push(format!(
                    "reference error {}: {}; target succeeded",
                    re.code,
                    one_line(&re.message)
                ));
                Class::Differs
            }
            (Outcome::Failed(re), Outcome::Failed(te)) => {
                notes.push(format!("error codes differ: {} vs {}", re.code, te.code));
                Class::Differs
            }
            (Outcome::Rows(a), Outcome::Rows(b)) => {
                notes.push(diff_note(a, b));
                Class::Differs
            }
        }
    };
    Classified {
        class,
        ref_hash,
        target_hash,
        note: notes.join("; "),
    }
}

fn diff_note(a: &ResultSet, b: &ResultSet) -> String {
    if a.columns != b.columns {
        return "column names differ".to_string();
    }
    if a.rows.len() != b.rows.len() {
        return format!("row count {} vs {}", a.rows.len(), b.rows.len());
    }
    if a.rows != b.rows && a.ordered {
        return "row values or order differ".to_string();
    }
    if a.warnings != b.warnings {
        return "warnings differ".to_string();
    }
    "row values differ".to_string()
}

fn one_line(s: &str) -> String {
    let t: String = s.split_whitespace().collect::<Vec<_>>().join(" ");
    t.chars().take(160).collect()
}
