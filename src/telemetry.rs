//! Health and metrics for the service dashboard (see `wikimisc::telemetry`).
//! Metric names live here so the set stays small and bounded.

use crate::app_state::AppState;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::task::JoinHandle;
use wikimisc::metrics::{counter, gauge, histogram};
use wikimisc::telemetry::{Telemetry, TelemetryConfig};

/// How often the running/waiting request gauges are sampled.
const LOAD_SAMPLE_INTERVAL: Duration = Duration::from_secs(10);

/// How often a running query samples the process' resident set size.
/// Queries are typically measured in seconds, so a few samples are enough to
/// see whether one is pushing the process toward its memory limit.
const RSS_SAMPLE_INTERVAL: Duration = Duration::from_millis(250);

/// Whether [`start`] installed a metrics recorder. When it did not, every
/// metric macro is a no-op, so the RSS sampler would only burn CPU.
static TELEMETRY_ENABLED: AtomicBool = AtomicBool::new(false);

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
    TELEMETRY_ENABLED.store(installed.is_some(), Ordering::Relaxed);
    if installed.is_some() {
        spawn_load_gauges(app_state);
    }
}

/// Records one query that got past admission control, by output format.
///
/// `result_entries` is the size of the result list before rendering and
/// `peak_rss_bytes` the highest process RSS observed while the query ran.
/// RSS is process-wide: with concurrent queries each one reports the peak of
/// the process during its lifetime, so read this together with the
/// `petscan_requests_running` gauge rather than as a per-query attribution.
pub fn record_query(
    format: &'static str,
    duration: Duration,
    result_entries: usize,
    peak_rss_bytes: Option<u64>,
) {
    counter!("petscan_queries_total", "format" => format).increment(1);
    histogram!("petscan_query_duration_seconds", "format" => format).record(duration.as_secs_f64());
    histogram!("petscan_result_entries", "format" => format).record(result_entries as f64);
    if let Some(bytes) = peak_rss_bytes {
        histogram!("petscan_query_peak_rss_bytes", "format" => format).record(bytes as f64);
    }
}

fn spawn_load_gauges(app_state: Arc<AppState>) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(LOAD_SAMPLE_INTERVAL);
        loop {
            ticker.tick().await;
            gauge!("petscan_requests_running").set(app_state.requests_running() as f64);
            gauge!("petscan_requests_waiting").set(app_state.requests_waiting() as f64);
            if let Some(rss) = current_rss_bytes() {
                gauge!("petscan_process_rss_bytes").set(rss as f64);
            }
        }
    });
}

/// Samples the process' resident set size while it is alive, so a query can
/// report the peak it observed. Dropping the sampler stops the task; when
/// telemetry is disabled, no task is started at all.
#[derive(Debug)]
pub struct RssSampler {
    peak: Option<Arc<AtomicU64>>,
    task: Option<JoinHandle<()>>,
}

impl RssSampler {
    pub fn start() -> Self {
        if !TELEMETRY_ENABLED.load(Ordering::Relaxed) {
            return Self {
                peak: None,
                task: None,
            };
        }
        let peak = Arc::new(AtomicU64::new(current_rss_bytes().unwrap_or(0)));
        let task = tokio::spawn(sample_rss(peak.clone()));
        Self {
            peak: Some(peak),
            task: Some(task),
        }
    }

    /// The highest RSS seen so far, or `None` when sampling is unavailable
    /// (telemetry off, or not Linux). Takes a final sample so queries shorter
    /// than [`RSS_SAMPLE_INTERVAL`] still report their end-of-query RSS.
    pub fn peak_bytes(&self) -> Option<u64> {
        let peak = self.peak.as_ref()?;
        if let Some(rss) = current_rss_bytes() {
            peak.fetch_max(rss, Ordering::Relaxed);
        }
        let value = peak.load(Ordering::Relaxed);
        (value > 0).then_some(value)
    }
}

impl Drop for RssSampler {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn sample_rss(peak: Arc<AtomicU64>) {
    let mut ticker = tokio::time::interval(RSS_SAMPLE_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        if let Some(rss) = current_rss_bytes() {
            peak.fetch_max(rss, Ordering::Relaxed);
        }
    }
}

/// Current resident set size of this process, if the OS exposes it.
fn current_rss_bytes() -> Option<u64> {
    parse_vm_rss_bytes(&std::fs::read_to_string("/proc/self/status").ok()?)
}

/// Extracts `VmRSS` (in kB) from the contents of `/proc/self/status`.
/// Split out so the non-Linux test host can exercise the parsing.
fn parse_vm_rss_bytes(proc_status: &str) -> Option<u64> {
    let line = proc_status
        .lines()
        .find(|line| line.starts_with("VmRSS:"))?;
    let kb = line.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    Some(kb * 1024)
}

#[cfg(test)]
mod tests {
    use super::parse_vm_rss_bytes;

    #[test]
    fn parses_vm_rss_in_kilobytes() {
        let status = "Name:\tpetscan\nVmPeak:\t  900000 kB\nVmRSS:\t  123456 kB\n";
        assert_eq!(parse_vm_rss_bytes(status), Some(123456 * 1024));
    }

    #[test]
    fn missing_vm_rss_yields_none() {
        assert_eq!(parse_vm_rss_bytes("Name:\tpetscan\n"), None);
    }

    #[test]
    fn malformed_vm_rss_yields_none() {
        assert_eq!(parse_vm_rss_bytes("VmRSS:\t   not-a-number kB\n"), None);
    }
}
