use petscan_rs::app_state::AppState;
use petscan_rs::command_line::{command_line_usage, get_petscan_config};
use petscan_rs::webserver::WebServer;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    let petscan_config = get_petscan_config();
    let app_state = Arc::new(AppState::new_from_config(&petscan_config).await?);

    let args = std::env::args();
    if args.len() > 1 {
        command_line_usage(app_state).await?;
    } else {
        spawn_shutdown_signal_handler(app_state.clone());
        let webserver = WebServer::new(app_state, petscan_config);
        webserver.run().await?;
    }
    Ok(())
}

/// Spawn a task that listens for SIGTERM/SIGINT and triggers the existing
/// drain-shutdown logic on `AppState`. New requests after the signal arrives
/// receive "Temporary maintenance"; the process exits via `try_shutdown`
/// once in-flight requests drain to zero.
#[cfg(unix)]
fn spawn_shutdown_signal_handler(app_state: Arc<AppState>) {
    tokio::spawn(async move {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("Failed to install SIGTERM handler: {e}");
                return;
            }
        };
        let mut sigint = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("Failed to install SIGINT handler: {e}");
                return;
            }
        };
        let received = tokio::select! {
            _ = sigterm.recv() => "SIGTERM",
            _ = sigint.recv() => "SIGINT",
        };
        tracing::info!("Received {received}; draining in-flight requests");
        app_state.shut_down();
        // If nothing is in flight right now, exit immediately; otherwise the
        // last decrement of `threads_running` will trip `try_shutdown`.
        app_state.try_shutdown();
    });
}

#[cfg(not(unix))]
fn spawn_shutdown_signal_handler(_app_state: Arc<AppState>) {
    tracing::warn!("Signal-based graceful shutdown is only wired on Unix targets");
}
