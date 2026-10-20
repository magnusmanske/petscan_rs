use petscan_rs::app_state::AppState;
use petscan_rs::command_line::{command_line_usage, get_petscan_config};
use petscan_rs::webserver::WebServer;
use std::sync::Arc;

/// glibc's malloc kept the memory of finished queries resident (3.8 GB on an
/// idle server), so each big query started from an inflated baseline toward
/// the 7 GB limit. jemalloc's background threads return freed memory to the
/// OS within seconds, and its peaks under PetScan-like load were about half
/// of glibc's.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    wikimisc::logging::init();
    petscan_rs::raise_open_file_limit();

    let petscan_config = get_petscan_config();
    let app_state = Arc::new(AppState::new_from_config(&petscan_config).await?);

    let args = std::env::args();
    if args.len() > 1 {
        command_line_usage(app_state).await?;
    } else {
        petscan_rs::telemetry::start(&petscan_config.telemetry, app_state.clone());
        tokio::spawn(petscan_rs::previous_run::report(app_state.clone()));
        spawn_shutdown_signal_handler(app_state.clone());
        let webserver = WebServer::new(app_state, petscan_config);
        webserver.run().await?;
    }
    Ok(())
}

/// Spawn a task that listens for SIGTERM/SIGINT and triggers the existing
/// drain-shutdown logic on `AppState`. New requests after the signal arrives
/// receive "Temporary maintenance"; the process exits via `try_shutdown`
/// once in-flight requests drain to zero, after the final telemetry push.
fn spawn_shutdown_signal_handler(app_state: Arc<AppState>) {
    tokio::spawn(async move {
        let received = wikimisc::shutdown::shutdown_signal().await;
        tracing::info!("Received {received}; draining in-flight requests");
        app_state.shut_down(received.as_str());
        // If nothing is in flight right now, exit immediately; otherwise the
        // last decrement of `threads_running` will trip `try_shutdown`.
        app_state.try_shutdown();
    });
}
