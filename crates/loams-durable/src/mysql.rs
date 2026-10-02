//! The MySQL (TiDB) store's schema check before serving (D1 Task 4).
//!
//! Resonate's MySQL plugin creates the schema in an empty database even with
//! `migrate = false`. Loams serves a MySQL store only once
//! `loams durable migrate` has run, so the embed looks first, on one
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
            "the durable store {shown} has no durable schema; run 'loams durable migrate' first"
        ))),
        _ => Ok(()),
    }
}

/// Make sure rustls has a process default crypto provider before a TLS
/// connection to the store (T5-4). rustls picks one by itself only when
/// exactly one of its `ring` and `aws-lc-rs` features is on; `loams`'s graph
/// has both, and sqlx's `verify_ca` verifier then panics. This installs
/// `ring` (sqlx's own) only when no default is set, so a host that installed
/// one first keeps it; a plain-text store changes nothing.
pub(crate) fn ensure_crypto_provider(tls: MysqlTls) {
    if tls == MysqlTls::Disabled || rustls::crypto::CryptoProvider::get_default().is_some() {
        return;
    }
    // Losing a race to another installer is fine: a default is set either way.
    let _ = rustls::crypto::ring::default_provider().install_default();
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

#[cfg(test)]
mod tests {
    use super::*;

    /// T5-4: in `loams`'s graph rustls has both the `ring` and `aws-lc-rs`
    /// providers, so it cannot pick a process default on its own, and sqlx
    /// panics building the `verify_ca` verifier. A TLS store makes sure a
    /// default exists first; a host that installed its own keeps it.
    #[test]
    fn a_tls_store_has_a_crypto_provider() {
        ensure_crypto_provider(MysqlTls::Disabled);
        ensure_crypto_provider(MysqlTls::VerifyCa);
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
        // Idempotent.
        ensure_crypto_provider(MysqlTls::VerifyIdentity);
    }
}
