//! `loams-apps-mock [--listen 127.0.0.1:8084] [--heartbeat-secs 15]`
//!
//! Serves the app protos with seed data until Ctrl-C. Fake tokens:
//! `Authorization: Bearer mock-access-usr_omar` (a fresh session) or
//! `mock-stale-usr_omar` (a session past the 5-minute step-up window).

use std::net::SocketAddr;
use std::time::Duration;

use clap::Parser;
use loams_apps_mock::{DEFAULT_LISTEN, MockConfig, Seed, serve};

#[derive(Debug, Parser)]
#[command(about = "A mock of the Loams app protos (design §37, AP0)")]
struct Args {
    /// Loopback address to listen on.
    #[arg(long, default_value = DEFAULT_LISTEN)]
    listen: SocketAddr,
    /// Heartbeat period of every watch stream, in seconds.
    #[arg(long, default_value_t = 15)]
    heartbeat_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    let handle = serve(MockConfig {
        listen: args.listen,
        seed: Seed::demo(),
        heartbeat: Duration::from_secs(args.heartbeat_secs.max(1)),
    })
    .await?;
    println!("loams-apps-mock: {}", handle.url());
    println!("  tokens: Bearer mock-access-usr_omar (fresh), Bearer mock-stale-usr_omar (stale)");
    tokio::signal::ctrl_c().await?;
    handle.stop().await;
    Ok(())
}
