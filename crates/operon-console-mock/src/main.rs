//! `cargo run -p operon-console-mock`: the console API mock on a fixed port.
//! The console's dev server (`pnpm dev` in `web/`) proxies `/api`, `/v1` and
//! `/.well-known` to it.

use std::time::SystemTime;

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use httpmock::MockServer;
use httpmock::server::HttpMockServerBuilder;
use operon_console_mock::{load_seed, register, routes};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(about = "Serve the console API mock with seed data")]
struct Args {
    /// The port to listen on.
    #[arg(long, default_value_t = 8081)]
    port: u16,
    /// Answer `GET /api/v1/session` with 401, to build the sign-in screens.
    #[arg(long)]
    signed_out: bool,
    /// Log every request.
    #[arg(long)]
    access_log: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let args = Args::parse();

    let seed = load_seed(SystemTime::now())?;
    let routes = routes(&seed, !args.signed_out)?;

    let server = HttpMockServerBuilder::new()
        .port(args.port)
        .print_access_log(args.access_log)
        .build()
        .map_err(|e| anyhow!("building the mock server: {e}"))?;
    let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
    let running = tokio::spawn(server.start_with_signals(Some(bound_tx), async {
        let _ = tokio::signal::ctrl_c().await;
    }));
    let addr = bound_rx.await.context("the mock server did not start")?;

    let client = MockServer::connect_async(&format!("127.0.0.1:{}", addr.port())).await;
    register(&client, &routes).await?;
    tracing::info!(
        "console API mock on http://127.0.0.1:{} ({} routes, signed {})",
        addr.port(),
        routes.len(),
        if args.signed_out {
            "out"
        } else {
            "in as the org owner"
        },
    );

    running
        .await
        .context("the mock server task panicked")?
        .map_err(|e| anyhow!("the mock server stopped: {e}"))?;
    Ok(())
}
