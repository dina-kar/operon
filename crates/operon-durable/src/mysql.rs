//! The MySQL (TiDB) store's schema check before serving (D1 Task 4).
//!
//! Resonate's MySQL plugin creates the schema in an empty database even with
//! `migrate = false`. Loam serves a MySQL store only once
//! `operon durable migrate` has run, so the embed looks first, on one
//! connection of its own, and refuses an unmigrated database.

use sqlx::{Connection, MySqlConnection};

use crate::config::{MysqlTls, mysql_url, redact_url, scrub};
use crate::error::DurableError;

/// Refuse `url` unless Resonate's migrations table is there and records at
/// least one applied migration. A schema that is there but behind or edited
/// is Resonate's to refuse, when it opens the store.
pub(crate) async fn check_schema(url: &str, tls: MysqlTls) -> Result<(), DurableError> {
    let shown = redact_url(url);
    let target = mysql_url(url, tls)?;
    let failed = |what: &str, e: sqlx::Error| {
        let mut message = format!("{what} the durable store {shown}: {e}");
        if message.contains("Unknown database") {
            message.push_str("; create the database first");
        }
        DurableError::Start(scrub(&message, url))
    };
    let mut conn = MySqlConnection::connect(&target)
        .await
        .map_err(|e| failed("cannot connect to", e))?;
    let applied = applied_migrations(&mut conn).await;
    // Best effort: the check is over either way.
    let _ = conn.close().await;
    match applied.map_err(|e| failed("cannot read the schema of", e))? {
        0 => Err(DurableError::Start(format!(
            "the durable store {shown} has no durable schema; run 'operon durable migrate' first"
        ))),
        _ => Ok(()),
    }
}

/// How many migrations `_sqlx_migrations` records as applied; 0 when the
/// table is not there.
async fn applied_migrations(conn: &mut MySqlConnection) -> Result<i64, sqlx::Error> {
    let tables: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables \
         WHERE table_schema = DATABASE() AND table_name = '_sqlx_migrations'",
    )
    .fetch_one(&mut *conn)
    .await?;
    if tables == 0 {
        return Ok(0);
    }
    sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations WHERE success")
        .fetch_one(&mut *conn)
        .await
}
