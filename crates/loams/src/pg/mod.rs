//! PostgreSQL wire access to Loams's collection SQL surface.
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
use datafusion::datasource::MemTable;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion_postgres::DfSessionService;
use datafusion_postgres::arrow_pg::datatypes::df::encode_dataframe;
use datafusion_postgres::datafusion_pg_catalog::{
    pg_catalog::context::EmptyContextProvider, setup_pg_catalog,
};
use datafusion_postgres::hooks::{
    HookClient, QueryHook, cursor::CursorStatementHook, set_show::SetShowHook,
    transactions::TransactionStatementHook,
};
use datafusion_postgres::pgwire::api::ClientPortalStore;
use datafusion_postgres::pgwire::api::auth::StartupHandler;
use datafusion_postgres::pgwire::api::portal::{Format, Portal};
use datafusion_postgres::pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use datafusion_postgres::pgwire::api::results::Response;
use datafusion_postgres::pgwire::api::store::PortalStore;
use datafusion_postgres::pgwire::api::{ClientInfo, NoopHandler, PgWireServerHandlers};
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::PgWireBackendMessage;
use datafusion_postgres::pgwire::tokio::process_socket;
use datafusion_postgres::pgwire::types::format::FormatOptions;
use loams_query::{CollectionService, ServiceError, SqlConfig};
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
    let sql = service.config().sql.clone();
    let query = Arc::new(DfSessionService::new_with_hooks(
        context,
        vec![
            Arc::new(ReadOnlyHook { sql }),
            Arc::new(CursorStatementHook),
            Arc::new(SetShowHook),
            Arc::new(TransactionStatementHook),
        ],
    ));
    let handlers = Arc::new(Handlers {
        extended: Arc::new(ExtendedService {
            inner: query.clone(),
        }),
        query,
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
    extended: Arc<ExtendedService>,
}

impl PgWireServerHandlers for Handlers {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.query.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.extended.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(NoopHandler)
    }
}

/// Bounds every query by the SQL row and time limits every other SQL surface
/// enforces (`loams_query::sql::run_bounded`).
struct ReadOnlyHook {
    sql: SqlConfig,
}

/// Where [`ExtendedService`] leaves the portal's result-column format for
/// [`ReadOnlyHook`], which is handed no portal.
const RESULT_FORMAT_KEY: &str = "loams.result_format";

fn format_to_metadata(format: &Format) -> String {
    match format {
        Format::UnifiedText => "text".to_string(),
        Format::UnifiedBinary => "binary".to_string(),
        Format::Individual(codes) => codes
            .iter()
            .map(i16::to_string)
            .collect::<Vec<_>>()
            .join(","),
    }
}

fn format_from_metadata(value: Option<&String>) -> Format {
    match value.map(String::as_str) {
        None | Some("text") => Format::UnifiedText,
        Some("binary") => Format::UnifiedBinary,
        Some(codes) => {
            Format::Individual(codes.split(',').filter_map(|c| c.parse().ok()).collect())
        }
    }
}

/// The wire error of a bounded run: 57014 `query_canceled` past the time
/// limit, 54000 `program_limit_exceeded` past the row limit.
fn limit_error(sqlstate: &str, message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        sqlstate.to_string(),
        message.to_string(),
    )))
}

fn service_error(error: ServiceError) -> PgWireError {
    match error {
        ServiceError::Timeout => {
            limit_error("57014", "canceling statement due to statement timeout")
        }
        other => PgWireError::ApiError(Box::new(other)),
    }
}

impl ReadOnlyHook {
    /// Runs the planned `frame` within the SQL limits and encodes the rows.
    async fn respond(
        &self,
        context: &SessionContext,
        frame: impl std::future::Future<Output = Result<datafusion::prelude::DataFrame, ServiceError>>,
        format: &Format,
        metadata: &std::collections::HashMap<String, String>,
    ) -> PgWireResult<Response> {
        let result = loams_query::sql::run_bounded(&self.sql, frame)
            .await
            .map_err(service_error)?;
        if result.truncated {
            return Err(limit_error(
                "54000",
                "SQL result exceeds the configured row limit",
            ));
        }
        let table = MemTable::try_new(result.schema, vec![result.batches])
            .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
        let frame = context
            .read_table(Arc::new(table))
            .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
        let options = Arc::new(FormatOptions::from_client_metadata(metadata));
        Ok(Response::Query(
            encode_dataframe(frame, format, Some(options)).await?,
        ))
    }
}

/// The extended-protocol handler: `DfSessionService`, told each portal's
/// result format for the hook to encode with.
struct ExtendedService {
    inner: Arc<DfSessionService>,
}

#[async_trait]
impl ExtendedQueryHandler for ExtendedService {
    type Statement = <DfSessionService as ExtendedQueryHandler>::Statement;
    type QueryParser = <DfSessionService as ExtendedQueryHandler>::QueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.inner.query_parser()
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo
            + ClientPortalStore
            + futures::Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as futures::Sink<PgWireBackendMessage>>::Error>,
    {
        client.metadata_mut().insert(
            RESULT_FORMAT_KEY.to_string(),
            format_to_metadata(&portal.result_column_format),
        );
        let response = ExtendedQueryHandler::do_query(&*self.inner, client, portal, max_rows).await;
        client.metadata_mut().remove(RESULT_FORMAT_KEY);
        response
    }
}

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
        client: &mut dyn HookClient,
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
        let sql = statement.to_string();
        let unencodable = std::sync::atomic::AtomicBool::new(false);
        let frame = async {
            let frame = loams_query::sql::plan_read_only(context, &sql).await?;
            if ensure_encodable(frame.schema().as_arrow()).is_err() {
                unencodable.store(true, std::sync::atomic::Ordering::Relaxed);
                return Err(ServiceError::InvalidArgument("unencodable column".into()));
            }
            Ok(frame)
        };
        let response = self
            .respond(context, frame, &Format::UnifiedText, client.metadata())
            .await;
        if unencodable.load(std::sync::atomic::Ordering::Relaxed) {
            return Some(Err(unsupported_type_error()));
        }
        Some(response)
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
            loams_query::sql::plan_read_only(context, &statement.to_string())
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
        plan: &LogicalPlan,
        params: &ParamValues,
        context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        if is_cursor_statement(statement) {
            return check_cursor_statement(statement).err().map(Err);
        }
        if is_session_statement(statement) {
            return None;
        }
        if !is_query(statement) {
            return Some(Err(read_only_error()));
        }
        let format = format_from_metadata(client.metadata().get(RESULT_FORMAT_KEY));
        let frame = async {
            let plan = plan
                .clone()
                .replace_params_with_values(params)
                .map_err(|error| ServiceError::InvalidArgument(format!("sql: {error}")))?;
            context
                .execute_logical_plan(plan)
                .await
                .map_err(loams_query::sql::planning)
        };
        Some(
            self.respond(context, frame, &format, client.metadata())
                .await,
        )
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

    /// A server with a PostgreSQL listener and SQL limits of `max_rows` rows
    /// and `timeout`, and a client of it.
    async fn limited(
        max_rows: usize,
        timeout: Duration,
    ) -> (crate::Server, tokio_postgres::Client, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let mut config = crate::ServerConfig::new(dir.path());
        config.listen = SocketAddr::from(([127, 0, 0, 1], 0));
        config.query.sql.max_rows = max_rows;
        config.query.sql.timeout = timeout;
        config.pg = Some(PgConfig::new(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            "default",
        ));
        let server = crate::Server::start(config).await.unwrap();
        let addr = server.pg_addr().unwrap();
        let (client, connection) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(addr.port())
            .user("loams")
            .connect(tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(connection);
        (server, client, dir)
    }

    fn sqlstate(error: &tokio_postgres::Error) -> String {
        error
            .as_db_error()
            .unwrap_or_else(|| panic!("not a database error: {error:?}"))
            .code()
            .code()
            .to_string()
    }

    const ROWS: &str = "SELECT value FROM generate_series(1, 100)";
    const FOREVER: &str = "SELECT sum(value) FROM generate_series(1, 5000000000)";

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn row_limit_applies_to_both_protocols() {
        let (server, client, _dir) = limited(10, Duration::from_secs(30)).await;
        // Under the limit, both protocols return the rows.
        let under = "SELECT value FROM generate_series(1, 10)";
        assert_eq!(client.simple_query(under).await.unwrap().len(), 10 + 2);
        assert_eq!(client.query(under, &[]).await.unwrap().len(), 10);
        let statement = client.prepare(under).await.unwrap();
        assert_eq!(client.query(&statement, &[]).await.unwrap().len(), 10);
        // Over it, both are refused with program_limit_exceeded.
        let error = client.simple_query(ROWS).await.unwrap_err();
        assert_eq!(sqlstate(&error), "54000", "{error:?}");
        let error = client.query(ROWS, &[]).await.unwrap_err();
        assert_eq!(sqlstate(&error), "54000", "{error:?}");
        let statement = client
            .prepare("SELECT value FROM generate_series(1, 100) WHERE value <= $1")
            .await
            .unwrap();
        assert_eq!(client.query(&statement, &[&5i64]).await.unwrap().len(), 5);
        let error = client.query(&statement, &[&100i64]).await.unwrap_err();
        assert_eq!(sqlstate(&error), "54000", "{error:?}");
        // The session survives the refusals.
        assert_eq!(client.query(under, &[]).await.unwrap().len(), 10);
        server.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn time_limit_applies_to_both_protocols() {
        let (server, client, _dir) = limited(10_000, Duration::from_millis(300)).await;
        let started = std::time::Instant::now();
        let error = client.simple_query(FOREVER).await.unwrap_err();
        assert_eq!(sqlstate(&error), "57014", "{error:?}");
        let error = client.query(FOREVER, &[]).await.unwrap_err();
        assert_eq!(sqlstate(&error), "57014", "{error:?}");
        let statement = client.prepare(FOREVER).await.unwrap();
        let error = client.query(&statement, &[]).await.unwrap_err();
        assert_eq!(sqlstate(&error), "57014", "{error:?}");
        assert!(started.elapsed() < Duration::from_secs(10));
        // The session survives the cancellations.
        assert_eq!(client.query("SELECT 1", &[]).await.unwrap().len(), 1);
        server.shutdown().await.unwrap();
    }
}
