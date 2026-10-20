//! Health and metrics for the service dashboard (see `wikimisc::telemetry`).
//! Metric names live here so the set stays small and bounded.

use crate::app_state::AppState;
use std::sync::Arc;
use std::time::Duration;
use wikimisc::metrics::{counter, gauge, histogram};
use wikimisc::telemetry::{Telemetry, TelemetryConfig};

/// How often the running/waiting request gauges are sampled.
const LOAD_SAMPLE_INTERVAL: Duration = Duration::from_secs(10);

/// Starts pushing to the dashboard. Never stops the server from starting:
/// without a token, telemetry is simply off. Signals are left to the
/// server's own graceful shutdown, which sends the final push once drained
/// (see [`AppState::try_shutdown`]).
pub fn start(config: &TelemetryConfig, app_state: Arc<AppState>) {
    let installed = Telemetry::builder("web")
        .config(config)
        .version(env!("CARGO_PKG_VERSION"))
        .handle_signals(false)
        .install_or_log();
    if installed.is_some() {
        spawn_load_gauges(app_state);
    }
}

/// Records one query that got past admission control, by output format.
pub fn record_query(format: &'static str, duration: Duration) {
    counter!("petscan_queries_total", "format" => format).increment(1);
    histogram!("petscan_query_duration_seconds", "format" => format).record(duration.as_secs_f64());
}

fn spawn_load_gauges(app_state: Arc<AppState>) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(LOAD_SAMPLE_INTERVAL);
        loop {
            ticker.tick().await;
            gauge!("petscan_requests_running").set(app_state.requests_running() as f64);
            gauge!("petscan_requests_waiting").set(app_state.requests_waiting() as f64);
        }
    });
}
