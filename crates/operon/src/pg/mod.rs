//! PostgreSQL wire access to Operon's collection SQL surface.
//!
//! The protocol terminates in this process. The listener is deliberately
//! loopback-only while authentication and TLS are absent. Dapr's gRPC service
//! invocation is for calls from this gateway to internal services; it does
//! not parse the PostgreSQL TCP protocol.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::ParamValues;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion_postgres::DfSessionService;
use datafusion_postgres::datafusion_pg_catalog::{
    pg_catalog::context::EmptyContextProvider, setup_pg_catalog,
};
use datafusion_postgres::hooks::{
    HookClient, QueryHook, cursor::CursorStatementHook, set_show::SetShowHook,
    transactions::TransactionStatementHook,
};
use datafusion_postgres::pgwire::api::auth::StartupHandler;
use datafusion_postgres::pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use datafusion_postgres::pgwire::api::results::Response;
use datafusion_postgres::pgwire::api::{ClientInfo, NoopHandler, PgWireServerHandlers};
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use datafusion_postgres::pgwire::tokio::process_socket;
use operon_query::CollectionService;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// PostgreSQL's read-only transaction SQLSTATE.
pub const READ_ONLY_SQLSTATE: &str = "25006";

/// How long the accept loop waits after a failed `accept` before retrying.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// Configuration for the read-only PostgreSQL wire listener.
#[derive(Clone, Debug)]
pub struct PgConfig {
    /// The loopback address to listen on.
    pub listen: SocketAddr,
    /// The namespace whose collections the listener serves.
    pub namespace: String,
    /// The most concurrent client connections; further ones are closed.
    pub max_connections: usize,
}

impl PgConfig {
    /// A configuration for `listen` and `namespace` with at most 256
    /// concurrent connections.
    pub fn new(listen: SocketAddr, namespace: impl Into<String>) -> Self {
        Self {
            listen,
            namespace: namespace.into(),
            max_connections: 256,
        }
    }
}

/// Bind only to loopback until the SQL listener has authentication and TLS.
pub async fn bind(addr: SocketAddr) -> io::Result<TcpListener> {
    if !addr.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "only loopback addresses are served until the unified auth plan (D111)",
        ));
    }
    TcpListener::bind(addr).await
}

/// A running listener and its active client connections.
#[derive(Debug)]
pub struct PgHandle {
    pub addr: SocketAddr,
    shutdown: CancellationToken,
    disconnect_clients: CancellationToken,
    accept_task: JoinHandle<()>,
    clients: TaskTracker,
}

impl PgHandle {
    /// Stops accepting connections, waits up to `grace` for connected
    /// clients to finish, then disconnects the rest.
    pub async fn stop_within(self, grace: Duration) {
        self.shutdown.cancel();
        let _ = self.accept_task.await;
        self.clients.close();
        if tokio::time::timeout(grace, self.clients.wait())
            .await
            .is_err()
        {
            self.disconnect_clients.cancel();
            self.clients.wait().await;
        }
    }
}

/// A bound PostgreSQL listener, not yet serving: [`listen`] runs before the
/// server starts any task, [`prepare`] turns it into a [`PgPrepared`] once
/// the collection service exists, and [`serve`] serves that.
#[derive(Debug)]
pub struct PgListener {
    listener: TcpListener,
    addr: SocketAddr,
}

/// Validates `config` and binds its (loopback) address.
pub async fn listen(config: &PgConfig) -> io::Result<PgListener> {
    if config.max_connections == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "PostgreSQL max_connections must be greater than zero",
        ));
    }
    let listener = bind(config.listen).await?;
    let addr = listener.local_addr()?;
    Ok(PgListener { listener, addr })
}

/// A bound listener whose `pg_catalog` is set up: [`prepare`] runs every
/// fallible step before the server starts its worker, and [`serve`] only
/// spawns the accept loop.
pub struct PgPrepared {
    listener: TcpListener,
    addr: SocketAddr,
    handlers: Arc<Handlers>,
    max_connections: usize,
}

impl std::fmt::Debug for PgPrepared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgPrepared")
            .field("addr", &self.addr)
            .finish()
    }
}

/// Builds the namespace's SQL context and its `pg_catalog`; the only step
/// after [`listen`] that can fail.
pub async fn prepare(
    service: Arc<CollectionService>,
    bound: PgListener,
    config: PgConfig,
) -> io::Result<PgPrepared> {
    let PgListener { listener, addr } = bound;
    let context = Arc::new(service.sql_context(&config.namespace));
    setup_pg_catalog(&context, &config.namespace, EmptyContextProvider)
        .map_err(io::Error::other)?;
    let handlers = Arc::new(Handlers {
        query: Arc::new(DfSessionService::new_with_hooks(
            context,
            vec![
                Arc::new(ReadOnlyHook),
                Arc::new(CursorStatementHook),
                Arc::new(SetShowHook),
                Arc::new(TransactionStatementHook),
            ],
        )),
    });
    Ok(PgPrepared {
        listener,
        addr,
        handlers,
        max_connections: config.max_connections,
    })
}

/// Serves a prepared read-only PostgreSQL wire listener. It cannot fail.
pub fn serve(prepared: PgPrepared) -> PgHandle {
    let PgPrepared {
        listener,
        addr,
        handlers,
        max_connections,
    } = prepared;

    let shutdown = CancellationToken::new();
    let disconnect_clients = CancellationToken::new();
    let clients = TaskTracker::new();
    let stop = shutdown.clone();
    let force_disconnect = disconnect_clients.clone();
    let active = clients.clone();
    let limit = Arc::new(tokio::sync::Semaphore::new(max_connections));
    let accept_task = tokio::spawn(async move {
        loop {
            let socket = tokio::select! {
                _ = stop.cancelled() => break,
                result = listener.accept() => match result {
                    Ok((socket, _)) => socket,
                    Err(error) => {
                        tracing::warn!(%error, "PostgreSQL accept failed");
                        // A persistent failure (e.g. EMFILE) must not spin.
                        tokio::select! {
                            _ = stop.cancelled() => break,
                            _ = tokio::time::sleep(ACCEPT_BACKOFF) => {}
                        }
                        continue;
                    }
                },
            };
            let Ok(permit) = limit.clone().try_acquire_owned() else {
                // The PostgreSQL protocol cannot send a valid ErrorResponse
                // before startup negotiation. Closing here is unambiguous.
                drop(socket);
                continue;
            };
            let handlers = handlers.clone();
            let disconnect = force_disconnect.clone();
            active.spawn(async move {
                let _permit = permit;
                tokio::select! {
                    _ = disconnect.cancelled() => {}
                    result = process_socket(socket, None, handlers) => {
                        if let Err(error) = result {
                            tracing::debug!(%error, "PostgreSQL connection ended");
                        }
                    }
                }
            });
        }
    });

    PgHandle {
        addr,
        shutdown,
        disconnect_clients,
        accept_task,
        clients,
    }
}

struct Handlers {
    query: Arc<DfSessionService>,
}

impl PgWireServerHandlers for Handlers {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.query.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.query.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(NoopHandler)
    }
}

struct ReadOnlyHook;

fn read_only_error() -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        READ_ONLY_SQLSTATE.to_string(),
        "this PostgreSQL listener accepts read-only queries".to_string(),
    )))
}

fn unsupported_type_error() -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "0A000".to_string(),
        "the PostgreSQL encoder cannot return fixed-size or large list columns; select scalar columns until the encoder is upgraded".to_string(),
    )))
}

fn unsafe_for_encoder(data_type: &DataType) -> bool {
    match data_type {
        DataType::FixedSizeList(_, _) | DataType::LargeList(_) => true,
        DataType::List(field) => unsafe_for_encoder(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| unsafe_for_encoder(field.data_type())),
        _ => false,
    }
}

fn ensure_encodable(schema: &Schema) -> PgWireResult<()> {
    if schema
        .fields()
        .iter()
        .any(|field| unsafe_for_encoder(field.data_type()))
    {
        return Err(unsupported_type_error());
    }
    Ok(())
}

fn is_query(statement: &Statement) -> bool {
    matches!(statement, Statement::Query(_))
}

fn is_session_statement(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::Set { .. }
            | Statement::ShowVariable { .. }
            | Statement::StartTransaction { .. }
            | Statement::Commit { .. }
            | Statement::Rollback { .. }
    )
}

fn is_cursor_statement(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::Declare { .. } | Statement::Fetch { .. } | Statement::Close { .. }
    )
}

fn cursor_unsupported_error() -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "0A000".to_string(),
        "DECLARE CURSOR is not supported: cursors would bypass the SQL row and time limits"
            .to_string(),
    )))
}

/// `CursorStatementHook` runs after this hook, re-plans a `DECLARE`'s query
/// with `SessionContext::sql` and materializes the whole result, outside the
/// SQL row and time limits. So `DECLARE` is refused here; `FETCH` and
/// `CLOSE` pass through and find no cursor.
fn check_cursor_statement(statement: &Statement) -> PgWireResult<()> {
    if matches!(statement, Statement::Declare { .. }) {
        return Err(cursor_unsupported_error());
    }
    Ok(())
}

#[async_trait]
impl QueryHook for ReadOnlyHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        if is_session_statement(statement) {
            return None;
        }
        if is_cursor_statement(statement) {
            return check_cursor_statement(statement).err().map(Err);
        }
        if !is_query(statement) {
            return Some(Err(read_only_error()));
        }
        match operon_query::sql::plan_read_only(context, &statement.to_string()).await {
            Ok(frame) => ensure_encodable(frame.schema().as_arrow()).err().map(Err),
            Err(error) => Some(Err(PgWireError::ApiError(Box::new(error)))),
        }
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        if is_session_statement(statement) {
            return None;
        }
        if is_cursor_statement(statement) {
            return check_cursor_statement(statement).err().map(Err);
        }
        if !is_query(statement) {
            return Some(Err(read_only_error()));
        }
        Some(
            operon_query::sql::plan_read_only(context, &statement.to_string())
                .await
                .map_err(|error| PgWireError::ApiError(Box::new(error)))
                .and_then(|frame| {
                    ensure_encodable(frame.schema().as_arrow())?;
                    Ok(frame.into_unoptimized_plan())
                }),
        )
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _plan: &LogicalPlan,
        _params: &ParamValues,
        _context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        if is_cursor_statement(statement) {
            return check_cursor_statement(statement).err().map(Err);
        }
        (!is_query(statement) && !is_session_statement(statement)).then(|| Err(read_only_error()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Schema};

    #[tokio::test]
    async fn listener_refuses_non_loopback() {
        let addr = "0.0.0.0:5432".parse().unwrap();
        assert_eq!(
            bind(addr).await.unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
    }

    fn parse(sql: &str) -> Statement {
        use datafusion::sql::sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
        Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .unwrap()
            .remove(0)
    }

    #[test]
    fn cursor_statements_are_told_apart() {
        assert!(is_cursor_statement(&parse("DECLARE c CURSOR FOR SELECT 1")));
        assert!(is_cursor_statement(&parse("FETCH NEXT FROM c")));
        assert!(is_cursor_statement(&parse("CLOSE c")));
        assert!(!is_cursor_statement(&parse("SELECT 1")));
        assert!(!is_cursor_statement(&parse("DELETE FROM t")));
    }

    #[test]
    fn declare_is_refused_and_fetch_and_close_pass_through() {
        let declare = parse("DECLARE c CURSOR FOR SELECT 1");
        assert!(check_cursor_statement(&declare).is_err());
        assert!(check_cursor_statement(&parse("FETCH NEXT FROM c")).is_ok());
        assert!(check_cursor_statement(&parse("CLOSE c")).is_ok());
    }

    #[test]
    fn vector_columns_are_blocked_before_the_encoder_can_panic() {
        let schema = Schema::new(vec![Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
            false,
        )]);
        assert!(ensure_encodable(&schema).is_err());
    }
}
