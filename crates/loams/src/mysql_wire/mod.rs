//! A small read-only MySQL wire surface over Loams's collection SQL engine.
//!
//! This accepts MySQL client TCP connections and implements text-protocol
//! SELECT queries. It deliberately rejects writes and prepared statements;
//! multi-statement transactions require a transactional SQL backend.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::MySqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use loams_query::{CollectionService, SqlConfig};
use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim, Column, ColumnFlags, ColumnType, ErrorKind, InitWriter,
    ParamParser, QueryResultWriter, StatementMetaWriter,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::{Instant, Sleep};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// How long the accept loop waits after a failed `accept` before retrying.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

#[derive(Clone, Debug)]
pub struct MysqlConfig {
    pub listen: SocketAddr,
    pub namespace: String,
    pub max_connections: usize,
    /// How long a new connection may take to send its handshake response.
    pub handshake_timeout: Duration,
    /// How long a connection may wait between commands. A running query is
    /// never interrupted: the deadline only runs while the server waits for
    /// the client.
    pub idle_timeout: Duration,
}

impl MysqlConfig {
    pub fn new(listen: SocketAddr, namespace: impl Into<String>) -> Self {
        Self {
            listen,
            namespace: namespace.into(),
            max_connections: 256,
            handshake_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(3600),
        }
    }
}

pub async fn bind(addr: SocketAddr) -> io::Result<TcpListener> {
    if !addr.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("MySQL listener {addr} requires a loopback address"),
        ));
    }
    TcpListener::bind(addr).await
}

#[derive(Debug)]
pub struct MysqlHandle {
    pub addr: SocketAddr,
    shutdown: CancellationToken,
    disconnect_clients: CancellationToken,
    accept_task: JoinHandle<()>,
    clients: TaskTracker,
}

impl MysqlHandle {
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

/// A bound MySQL listener, not yet serving: [`listen`] runs before the
/// server starts any task, and [`start`] serves it once the collection
/// service exists.
#[derive(Debug)]
pub struct MysqlListener {
    listener: TcpListener,
    addr: SocketAddr,
}

/// Validates `config` and binds its (loopback) address.
pub async fn listen(config: &MysqlConfig) -> io::Result<MysqlListener> {
    if config.max_connections == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "MySQL max_connections must be greater than zero",
        ));
    }
    let listener = bind(config.listen).await?;
    let addr = listener.local_addr()?;
    Ok(MysqlListener { listener, addr })
}

/// Serves a read-only MySQL wire listener for one namespace.
pub fn start(
    service: Arc<CollectionService>,
    bound: MysqlListener,
    config: MysqlConfig,
) -> MysqlHandle {
    let MysqlListener { listener, addr } = bound;
    let context = Arc::new(service.sql_context(&config.namespace));
    let sql_config = service.config().sql.clone();
    let shutdown = CancellationToken::new();
    let disconnect_clients = CancellationToken::new();
    let clients = TaskTracker::new();
    let stop = shutdown.clone();
    let force_disconnect = disconnect_clients.clone();
    let active = clients.clone();
    let limit = Arc::new(tokio::sync::Semaphore::new(config.max_connections));
    let namespace = config.namespace;
    let (handshake_timeout, idle_timeout) = (config.handshake_timeout, config.idle_timeout);
    let accept_task = tokio::spawn(async move {
        loop {
            let socket = tokio::select! {
                _ = stop.cancelled() => break,
                result = listener.accept() => match result {
                    Ok((socket, _)) => socket,
                    Err(error) => {
                        tracing::warn!(%error, "MySQL accept failed");
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
                drop(socket);
                continue;
            };
            let backend = Backend {
                context: context.clone(),
                sql_config: sql_config.clone(),
                namespace: namespace.clone(),
            };
            let disconnect = force_disconnect.clone();
            active.spawn(async move {
                let _permit = permit;
                let (reader, writer) = socket.into_split();
                let reader = DeadlineReader::new(reader, handshake_timeout, idle_timeout);
                tokio::select! {
                    _ = disconnect.cancelled() => {}
                    result = AsyncMysqlIntermediary::run_on(backend, reader, writer) => {
                        if let Err(error) = result {
                            tracing::debug!(%error, "MySQL connection ended");
                        }
                    }
                }
            });
        }
    });
    MysqlHandle {
        addr,
        shutdown,
        disconnect_clients,
        accept_task,
        clients,
    }
}

/// A reader that fails with `TimedOut` when the client sends nothing for
/// too long: `first` until the first bytes (the handshake response), then
/// `idle` from when the server starts waiting for the next command. It frees
/// a connection permit held by a client that never finishes the handshake or
/// leaves its connection idle.
struct DeadlineReader<R> {
    inner: R,
    idle: Duration,
    deadline: Pin<Box<Sleep>>,
    /// Whether `deadline` counts the current wait. It is re-armed when a read
    /// first has to wait, so time spent running a query never counts.
    armed: bool,
}

impl<R> DeadlineReader<R> {
    fn new(inner: R, first: Duration, idle: Duration) -> Self {
        Self {
            inner,
            idle,
            deadline: Box::pin(tokio::time::sleep(first)),
            armed: true,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for DeadlineReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = &mut *self;
        match Pin::new(&mut this.inner).poll_read(cx, buf) {
            Poll::Ready(result) => {
                this.armed = false;
                Poll::Ready(result)
            }
            Poll::Pending => {
                if !this.armed {
                    this.armed = true;
                    let next = Instant::now() + this.idle;
                    this.deadline.as_mut().reset(next);
                }
                match this.deadline.as_mut().poll(cx) {
                    Poll::Ready(()) => Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "MySQL client sent nothing before the deadline",
                    ))),
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }
}

struct Backend {
    context: Arc<SessionContext>,
    sql_config: SqlConfig,
    namespace: String,
}

fn column_type(data_type: &DataType) -> (ColumnType, ColumnFlags) {
    use ColumnType::*;
    match data_type {
        DataType::Boolean => (MYSQL_TYPE_TINY, ColumnFlags::empty()),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            (MYSQL_TYPE_LONGLONG, ColumnFlags::empty())
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            (MYSQL_TYPE_LONGLONG, ColumnFlags::UNSIGNED_FLAG)
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            (MYSQL_TYPE_DOUBLE, ColumnFlags::empty())
        }
        _ => (MYSQL_TYPE_VAR_STRING, ColumnFlags::empty()),
    }
}

fn columns(schema: &Schema) -> Vec<Column> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let (coltype, colflags) = column_type(field.data_type());
            Column {
                table: String::new(),
                column: field.name().clone(),
                coltype,
                colflags,
            }
        })
        .collect()
}

fn one_read_query(sql: &str) -> Result<(), ErrorKind> {
    let statements =
        Parser::parse_sql(&MySqlDialect {}, sql).map_err(|_| ErrorKind::ER_PARSE_ERROR)?;
    if statements.len() != 1 || !matches!(statements[0], Statement::Query(_)) {
        return Err(ErrorKind::ER_READ_ONLY_MODE);
    }
    Ok(())
}

#[async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for Backend {
    type Error = io::Error;

    async fn on_prepare<'a>(
        &'a mut self,
        _query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        info.error(
            ErrorKind::ER_UNSUPPORTED_PS,
            b"prepared statements are not supported by this read-only gateway",
        )
        .await
    }

    async fn on_execute<'a>(
        &'a mut self,
        _id: u32,
        _params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        results
            .error(
                ErrorKind::ER_UNSUPPORTED_PS,
                b"prepared statements are not supported by this read-only gateway",
            )
            .await
    }

    async fn on_close<'a>(&'a mut self, _stmt: u32)
    where
        W: 'async_trait,
    {
    }

    async fn on_init<'a>(
        &'a mut self,
        database: &'a str,
        writer: InitWriter<'a, W>,
    ) -> io::Result<()> {
        if database == self.namespace {
            writer.ok().await
        } else {
            writer
                .error(ErrorKind::ER_BAD_DB_ERROR, b"unknown namespace")
                .await
        }
    }

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        if let Err(kind) = one_read_query(query) {
            return results
                .error(kind, b"only one read-only SELECT is supported")
                .await;
        }
        let result =
            match loams_query::sql::run_read_only(&self.context, query, &self.sql_config).await {
                Ok(result) => result,
                Err(error) => {
                    return results
                        .error(
                            ErrorKind::ER_NOT_SUPPORTED_YET,
                            error.to_string().as_bytes(),
                        )
                        .await;
                }
            };
        if result.truncated {
            return results
                .error(
                    ErrorKind::ER_OUT_OF_RESOURCES,
                    b"SQL result exceeds the configured row limit",
                )
                .await;
        }
        let metadata = columns(&result.schema);
        let mut rows = results.start(&metadata).await?;
        for batch in &result.batches {
            for row in 0..batch.num_rows() {
                for (index, field) in result.schema.fields().iter().enumerate() {
                    let value = loams_query::sql::value_to_json(batch.column(index).as_ref(), row);
                    if value.is_null() {
                        rows.write_col(Option::<String>::None)?;
                        continue;
                    }
                    let (kind, flags) = column_type(field.data_type());
                    match kind {
                        ColumnType::MYSQL_TYPE_TINY => {
                            rows.write_col(u8::from(value.as_bool().unwrap_or(false)))?;
                        }
                        ColumnType::MYSQL_TYPE_LONGLONG
                            if flags.contains(ColumnFlags::UNSIGNED_FLAG) =>
                        {
                            rows.write_col(value.as_u64().ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "invalid unsigned SQL value",
                                )
                            })?)?;
                        }
                        ColumnType::MYSQL_TYPE_LONGLONG => {
                            rows.write_col(value.as_i64().ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "invalid signed SQL value",
                                )
                            })?)?;
                        }
                        ColumnType::MYSQL_TYPE_DOUBLE => {
                            let number = value
                                .as_f64()
                                .or_else(|| value.as_str().and_then(|v| v.parse().ok()));
                            rows.write_col(number)?;
                        }
                        _ => {
                            let text = value
                                .as_str()
                                .map(str::to_owned)
                                .unwrap_or_else(|| value.to_string());
                            rows.write_col(text)?;
                        }
                    }
                }
                rows.end_row().await?;
            }
        }
        rows.finish().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn listener_refuses_non_loopback() {
        let addr = "0.0.0.0:3306".parse().unwrap();
        assert_eq!(
            bind(addr).await.unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
    }

    #[tokio::test]
    async fn a_silent_client_times_out_and_a_talking_one_does_not() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut client, server) = tokio::io::duplex(64);
        let first = Duration::from_millis(200);
        let idle = Duration::from_millis(600);
        let mut reader = DeadlineReader::new(server, first, idle);
        let mut buf = [0u8; 4];
        client.write_all(b"ping").await.unwrap();
        reader.read_exact(&mut buf).await.unwrap();
        // Longer than the idle deadline with no read waiting (a query
        // running): it does not count.
        tokio::time::sleep(Duration::from_millis(800)).await;
        client.write_all(b"pong").await.unwrap();
        reader.read_exact(&mut buf).await.unwrap();
        tokio::time::sleep(Duration::from_millis(800)).await;
        let late = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            client.write_all(b"late").await.unwrap();
            client
        });
        // The wait starts now: 300 ms is within the idle deadline.
        reader.read_exact(&mut buf).await.unwrap();
        let _client = late.await.unwrap();
        let err = reader.read_exact(&mut buf).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);

        let (_client, server) = tokio::io::duplex(64);
        let mut reader = DeadlineReader::new(server, first, idle);
        let err = reader.read_exact(&mut buf).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    }

    #[test]
    fn only_one_select_is_accepted() {
        assert!(one_read_query("SELECT 1").is_ok());
        assert!(one_read_query("INSERT INTO x VALUES (1)").is_err());
        assert!(one_read_query("SELECT 1; DELETE FROM x").is_err());
    }
}
