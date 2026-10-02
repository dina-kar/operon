//! Classifier, TSV and replay tests over in-process mock connectors (RT0 plan Tasks 5 and 6).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use loams_compat::classify::{Class, DbError, Outcome, ResultSet, canonical_hash};
use loams_compat::replay::{self, CaptureEntry, Connect, Connector, Options, Request};
use loams_compat::tsv::{self, Engine, Row, TsvError};

type Script = Arc<dyn Fn(&str) -> Outcome + Send + Sync>;

/// An engine that answers each statement from a function.
struct Mock(Script);

struct MockConn(Script);

#[async_trait]
impl Connector for MockConn {
    async fn run(&mut self, req: &Request<'_>) -> Outcome {
        (self.0)(req.statement)
    }
}

#[async_trait]
impl Connect for Mock {
    async fn connect(&self, _db: Option<&str>) -> anyhow::Result<Box<dyn Connector>> {
        Ok(Box::new(MockConn(self.0.clone())))
    }
}

fn mock(f: impl Fn(&str) -> Outcome + Send + Sync + 'static) -> Mock {
    Mock(Arc::new(f))
}

fn rows(cols: &[&str], data: &[&[&str]]) -> Outcome {
    Outcome::Rows(ResultSet {
        columns: cols.iter().map(|c| c.to_string()).collect(),
        rows: data
            .iter()
            .map(|r| r.iter().map(|c| Some(c.to_string())).collect())
            .collect(),
        ..ResultSet::default()
    })
}

fn failed(code: &str, msg: &str) -> Outcome {
    Outcome::Failed(DbError {
        code: code.into(),
        message: msg.into(),
    })
}

fn entry(digest: &str, component: &str, example: &str) -> CaptureEntry {
    CaptureEntry {
        digest: digest.into(),
        component: component.into(),
        source: "dynamic:test".into(),
        example: example.into(),
        session: vec![],
        db: None,
        mode: None,
        issue: None,
    }
}

fn entries() -> Vec<CaptureEntry> {
    vec![
        entry("d-same", "query", "select 1"),
        entry("d-differs", "query", "select 2"),
        entry("d-error", "health", "select pg_is_in_recovery()"),
        entry("d-both-fail", "pool", "select * from missing"),
    ]
}

/// The reference answers; the target agrees, differs in one value, errors, and fails alike.
fn reference() -> Mock {
    mock(|s| match s {
        "select 1" => rows(&["a"], &[&["1"]]),
        "select 2" => rows(&["a"], &[&["2"]]),
        "select pg_is_in_recovery()" => rows(&["pg_is_in_recovery"], &[&["f"]]),
        _ => failed("42P01", "relation \"missing\" does not exist"),
    })
}

fn target() -> Mock {
    mock(|s| match s {
        "select 1" => rows(&["a"], &[&["1"]]),
        "select 2" => rows(&["a"], &[&["3"]]),
        "select pg_is_in_recovery()" => failed("0A000", "function not supported"),
        _ => failed(
            "42P01",
            "relation \"missing\" does not exist in some other words",
        ),
    })
}

fn by_digest(rows: &[Row]) -> HashMap<&str, &Row> {
    rows.iter().map(|r| (r.digest.as_str(), r)).collect()
}

#[tokio::test]
async fn classify_same_differs_error() {
    let r = reference();
    let t = target();
    let out = replay::replay_all(
        Engine::Postgres,
        &r,
        Some(&t),
        &entries(),
        &HashMap::new(),
        &Options::default(),
    )
    .await;
    let m = by_digest(&out);
    assert_eq!(m["d-same"].class, Class::Same);
    assert_eq!(m["d-same"].ref_hash, m["d-same"].target_hash);
    assert_eq!(m["d-differs"].class, Class::Differs);
    assert_ne!(m["d-differs"].ref_hash, m["d-differs"].target_hash);
    assert!(
        m["d-differs"].note.contains("row values differ"),
        "{}",
        m["d-differs"].note
    );
    assert_eq!(m["d-error"].class, Class::Error);
    assert!(m["d-error"].note.contains("0A000"));
    // The same error code with a different message is the same result: messages are not hashed.
    assert_eq!(m["d-both-fail"].class, Class::Same);
}

#[tokio::test]
async fn ruled_unsupported_carries_the_note_and_a_missing_target_is_pending() {
    let r = reference();
    let t = target();
    let mut u = HashMap::new();
    u.insert(
        "d-error".to_string(),
        "recovery state is the control plane's, not the compute's".to_string(),
    );
    let out = replay::replay_all(
        Engine::Postgres,
        &r,
        Some(&t),
        &entries(),
        &u,
        &Options::default(),
    )
    .await;
    let row = &by_digest(&out)["d-error"];
    assert_eq!(row.class, Class::Unsupported);
    assert!(row.note.contains("control plane"));

    let pending = replay::replay_all(
        Engine::Postgres,
        &r,
        None,
        &entries(),
        &HashMap::new(),
        &Options::default(),
    )
    .await;
    assert!(
        pending
            .iter()
            .all(|r| r.class == Class::PendingTarget && r.target_hash.is_empty())
    );
    assert!(
        pending.iter().all(|r| !r.ref_hash.is_empty()),
        "the reference result is recorded"
    );
}

#[tokio::test]
async fn a_volatile_reference_compares_by_shape() {
    use std::sync::atomic::{AtomicU64, Ordering};
    let n = Arc::new(AtomicU64::new(0));
    let clock = |n: Arc<AtomicU64>| {
        mock(move |_| {
            let v = n.fetch_add(1, Ordering::Relaxed).to_string();
            rows(&["now"], &[&[v.as_str()]])
        })
    };
    let r = clock(n.clone());
    let t = clock(n);
    let e = vec![entry("d-clock", "query", "select now()")];
    let out = replay::replay_all(
        Engine::Postgres,
        &r,
        Some(&t),
        &e,
        &HashMap::new(),
        &Options::default(),
    )
    .await;
    assert_eq!(out[0].class, Class::Same);
    assert!(out[0].note.contains("volatile"));
}

#[test]
fn row_order_counts_only_when_ordered() {
    let a = ResultSet {
        columns: vec!["a".into()],
        rows: vec![vec![Some("1".into())], vec![Some("2".into())]],
        ..ResultSet::default()
    };
    let mut b = a.clone();
    b.rows.reverse();
    assert_eq!(
        canonical_hash(&Outcome::Rows(a.clone())),
        canonical_hash(&Outcome::Rows(b.clone()))
    );
    let (mut a, mut b) = (a, b);
    a.ordered = true;
    b.ordered = true;
    assert_ne!(
        canonical_hash(&Outcome::Rows(a)),
        canonical_hash(&Outcome::Rows(b))
    );
}

fn row(digest: &str, class: Class, note: &str) -> Row {
    Row {
        digest: digest.into(),
        component: "query".into(),
        source: "dynamic:resharding".into(),
        example: "select 'a\tb\\c'\nfrom t".into(),
        class,
        ref_hash: "aa".into(),
        target_hash: "bb".into(),
        note: note.into(),
        issue: String::new(),
    }
}

#[test]
fn tsv_round_trip() {
    let rows = vec![
        row("d2", Class::Differs, "note with\ttab"),
        row("d1", Class::Same, ""),
        row("d3", Class::Unsupported, "by design"),
    ];
    let text = tsv::write(&rows, Engine::Postgres).expect("valid rows");
    assert_eq!(
        text.lines().count(),
        4,
        "header plus one line per row, whatever the fields contain"
    );
    let back = tsv::read(&text, Engine::Postgres).expect("reads back");
    let mut want = rows;
    want.sort_by(|a, b| a.digest.cmp(&b.digest));
    assert_eq!(back, want);
    assert_eq!(
        tsv::write(&back, Engine::Postgres).expect("rewrites"),
        text,
        "write is stable"
    );
}

#[test]
fn unsupported_needs_note() {
    let bad = vec![row("d1", Class::Unsupported, "  ")];
    assert_eq!(
        tsv::write(&bad, Engine::Postgres),
        Err(TsvError::UnsupportedNeedsNote {
            digest: "d1".into()
        })
    );
    let text = "digest\tcomponent\tsource\texample\tclass\tref_hash\ttarget_hash\tnote\tissue\nd1\tquery\ts\te\tunsupported\ta\tb\t\t\n";
    assert_eq!(
        tsv::read(text, Engine::Postgres),
        Err(TsvError::UnsupportedNeedsNote {
            digest: "d1".into()
        })
    );
    let mut other = row("d2", Class::Same, "");
    other.component = "reparent".into();
    assert!(matches!(
        tsv::write(&[other], Engine::Postgres),
        Err(TsvError::UnknownComponent { .. })
    ));
}

/// MySQL replay: the same classes over a mock, with MySQL numeric error codes and the
/// component vocabulary of §31 §15 (`sidecar`, `vreplication`, …).
#[tokio::test]
async fn mysql_classify_same_differs_error() {
    let r = mock(|s| match s {
        "SELECT 1" => rows(&["1"], &[&["1"]]),
        "SHOW TABLES" => rows(&["Tables_in_vt"], &[&["dt_state"], &["redo_state"]]),
        _ => rows(&["x"], &[]),
    });
    let t = mock(|s| match s {
        "SELECT 1" => rows(&["1"], &[&["1"]]),
        "SHOW TABLES" => rows(&["Tables_in_vt"], &[&["dt_state"]]),
        _ => failed(
            "1235",
            "SE currently doesn't support foreign key constraints",
        ),
    });
    let e = vec![
        entry("m-same", "query", "SELECT 1"),
        entry("m-differs", "sidecar", "SHOW TABLES"),
        entry(
            "m-error",
            "vreplication",
            "ALTER TABLE a ADD CONSTRAINT fk FOREIGN KEY (b) REFERENCES c (d)",
        ),
    ];
    let mut u = HashMap::new();
    u.insert(
        "m-error".to_string(),
        "SmartEngine has no foreign keys".to_string(),
    );
    let out = replay::replay_all(
        Engine::Mysql,
        &r,
        Some(&t),
        &e,
        &HashMap::new(),
        &Options::default(),
    )
    .await;
    let m = by_digest(&out);
    assert_eq!(m["m-same"].class, Class::Same);
    assert_eq!(m["m-differs"].class, Class::Differs);
    assert!(m["m-differs"].note.contains("row count 2 vs 1"));
    assert_eq!(m["m-error"].class, Class::Error);
    assert!(m["m-error"].note.contains("1235"));
    let out = replay::replay_all(Engine::Mysql, &r, Some(&t), &e, &u, &Options::default()).await;
    assert_eq!(by_digest(&out)["m-error"].class, Class::Unsupported);
    tsv::write(&out, Engine::Mysql).expect("every MySQL component is accepted");
}

#[test]
fn mode_for_routes_transaction_and_replication_commands_to_scratch() {
    use loams_compat::replay::{Mode, mode_for};
    let m = |sql: &str| mode_for(Engine::Postgres, &entry("d", "query", sql));
    assert_eq!(m("select 1"), Mode::RolledBack);
    assert_eq!(m("PREPARE TRANSACTION 'x'"), Mode::Scratch);
    assert_eq!(m("START_REPLICATION SLOT s LOGICAL 0/0"), Mode::Scratch);
    assert_eq!(m("  commit prepared 'x'"), Mode::Scratch);
}

/// An engine whose answer depends on the database: `app` already has the table, `empty` does not.
struct DbMock;

struct DbConn(Option<String>);

#[async_trait]
impl Connector for DbConn {
    async fn run(&mut self, req: &Request<'_>) -> Outcome {
        match (self.0.as_deref(), req.statement) {
            (Some("empty"), "create table t (i int)") => rows(&[], &[]),
            (_, "create table t (i int)") => failed("42P07", "relation \"t\" already exists"),
            _ => failed("XX000", "unexpected"),
        }
    }
}

#[async_trait]
impl Connect for DbMock {
    async fn connect(&self, db: Option<&str>) -> anyhow::Result<Box<dyn Connector>> {
        Ok(Box::new(DbConn(db.map(str::to_string))))
    }
}

#[tokio::test]
async fn an_already_exists_error_moves_the_replay_to_the_empty_database() {
    let mut e = entry("d-create", "schema-sync", "create table t (i int)");
    e.db = Some("app".into());
    let opts = Options {
        empty_db: Some("empty".into()),
        ..Options::default()
    };
    let out = replay::replay_all(
        Engine::Postgres,
        &DbMock,
        Some(&DbMock),
        &[e.clone()],
        &HashMap::new(),
        &opts,
    )
    .await;
    assert_eq!(out[0].class, Class::Same);
    assert!(out[0].note.contains("empty database"), "{}", out[0].note);
    // Without an empty database the statement stays an error on both sides: still `same`, no move.
    let out = replay::replay_all(
        Engine::Postgres,
        &DbMock,
        Some(&DbMock),
        &[e],
        &HashMap::new(),
        &Options::default(),
    )
    .await;
    assert!(!out[0].note.contains("empty database"));
}

#[tokio::test]
async fn a_rule_turns_a_matching_target_error_into_unsupported() {
    let r = mock(|_| rows(&["x"], &[&["1"]]));
    let t = mock(|s| match s {
        "set tx serializable" => failed(
            "1105",
            "SE only supports READ COMMITTED and REPEATABLE READ isolation levels",
        ),
        _ => failed("1105", "something else"),
    });
    let e = vec![
        entry("m-iso", "query", "set tx serializable"),
        entry("m-other", "query", "select 1"),
    ];
    let opts = Options {
        unsupported_rules: replay::read_unsupported_rules(
            "# comment\nSE only supports READ COMMITTED\tSmartEngine isolation (design 29 s4)\n",
        ),
        ..Options::default()
    };
    let out = replay::replay_all(Engine::Mysql, &r, Some(&t), &e, &HashMap::new(), &opts).await;
    let m = by_digest(&out);
    assert_eq!(m["m-iso"].class, Class::Unsupported);
    assert!(m["m-iso"].note.contains("SmartEngine isolation"));
    assert_eq!(m["m-other"].class, Class::Error);
}

#[tokio::test]
async fn an_identity_statement_compares_by_shape() {
    let r = mock(|_| rows(&["@@version"], &[&["8.0.46"]]));
    let t = mock(|_| rows(&["@@version"], &[&["8.0.35-wesql"]]));
    let e = vec![
        entry("m-version", "health", "select @@version"),
        entry("m-other", "health", "select @@sql_mode"),
    ];
    let opts = Options {
        shape_only: vec!["@@version".to_string()],
        ..Options::default()
    };
    let out = replay::replay_all(Engine::Mysql, &r, Some(&t), &e, &HashMap::new(), &opts).await;
    let m = by_digest(&out);
    assert_eq!(m["m-version"].class, Class::Same);
    assert!(m["m-version"].note.contains("instance identity"));
    assert_eq!(m["m-other"].class, Class::Differs);
}
