//! Reports how the previous `PetScan` process ended. One killed by SIGKILL (in
//! practice the kernel enforcing the systemd unit's `MemoryMax`) cannot
//! report that itself, so its successor does, naming the queries it was
//! running, and then clears their `started_queries` rows.
//!
//! `run.sh` passes the previous run's exit status in [`EXIT_STATUS_VAR`];
//! the previous process's PID and start time come from [`PID_FILE`], which
//! every server process overwrites at startup.

use crate::app_state::AppState;
use crate::database_manager::query_log_timestamp;
use std::sync::Arc;
use wikimisc::telemetry::Level;

const PID_FILE: &str = "petscan.pid";
const EXIT_STATUS_VAR: &str = "PETSCAN_PREVIOUS_EXIT";
/// Keeps the report readable when a process died with many queries running.
const MAX_REPORTED_QUERIES: usize = 10;
const MAX_REPORTED_QUERY_CHARS: usize = 300;

/// The previous server process, as recorded in [`PID_FILE`].
#[derive(Debug, PartialEq, Eq)]
struct PreviousProcess {
    pid: u32,
    /// Its start, in `started_queries.created` form.
    started: String,
}

impl PreviousProcess {
    fn parse(file: &str) -> Option<Self> {
        let (pid, started) = file.trim().split_once(' ')?;
        Some(Self {
            pid: pid.parse().ok()?,
            started: started.to_string(),
        })
    }
}

/// Records this process in [`PID_FILE`], then reports how the previous one
/// ended, if it did not end cleanly.
pub async fn report(app_state: Arc<AppState>) {
    let previous = std::fs::read_to_string(PID_FILE)
        .ok()
        .and_then(|file| PreviousProcess::parse(&file));
    let current = format!("{} {}", std::process::id(), query_log_timestamp());
    if let Err(e) = std::fs::write(PID_FILE, current) {
        tracing::warn!("Could not write {PID_FILE}: {e}");
    }

    let exit_status = std::env::var(EXIT_STATUS_VAR)
        .ok()
        .and_then(|s| s.parse().ok());
    let queries = match &previous {
        Some(p) => app_state
            .take_orphaned_queries(p.pid, &p.started)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!("Could not read the previous process's queries: {e}");
                vec![]
            }),
        None => vec![],
    };
    if let Some(message) = report_message(previous.map(|p| p.pid), exit_status, &queries) {
        tracing::error!("{message}");
        wikimisc::telemetry::event(Level::Error, message);
    }
}

/// How a process with this exit status ended; `None` for a clean exit.
fn describe_exit(status: i32) -> Option<String> {
    match status {
        0 => None,
        137 => Some(
            "was killed by SIGKILL (exit status 137), almost certainly for exceeding the memory limit"
                .to_string(),
        ),
        s if s > 128 => Some(format!("was killed by signal {} (exit status {s})", s - 128)),
        s => Some(format!("exited with status {s}")),
    }
}

/// The report on the previous process, or `None` if there is nothing to say.
fn report_message(
    pid: Option<u32>,
    exit_status: Option<i32>,
    queries: &[String],
) -> Option<String> {
    let ending = exit_status.and_then(describe_exit);
    if ending.is_none() && queries.is_empty() {
        return None;
    }
    let who = match pid {
        Some(pid) => format!("The previous instance (pid {pid})"),
        None => "The previous instance".to_string(),
    };
    let mut message = format!("{who} {}", ending.as_deref().unwrap_or("ended"));
    if !queries.is_empty() {
        let shown: Vec<String> = queries
            .iter()
            .take(MAX_REPORTED_QUERIES)
            .map(|q| q.chars().take(MAX_REPORTED_QUERY_CHARS).collect())
            .collect();
        message += &format!(
            " while running {} queries: {}",
            queries.len(),
            shown.join(" | ")
        );
        if queries.len() > MAX_REPORTED_QUERIES {
            message += " | …";
        }
    }
    Some(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_pid_file() {
        assert_eq!(
            PreviousProcess::parse("4242 2026-10-15 16:15:21\n"),
            Some(PreviousProcess {
                pid: 4242,
                started: "2026-10-15 16:15:21".to_string()
            })
        );
        assert_eq!(PreviousProcess::parse("garbage"), None);
    }

    #[test]
    fn describes_exit_statuses() {
        assert_eq!(describe_exit(0), None);
        assert!(describe_exit(137).unwrap().contains("memory limit"));
        assert_eq!(
            describe_exit(134).unwrap(),
            "was killed by signal 6 (exit status 134)"
        );
        assert_eq!(describe_exit(1).unwrap(), "exited with status 1");
    }

    #[test]
    fn clean_exit_without_queries_is_not_reported() {
        assert_eq!(report_message(Some(1), Some(0), &[]), None);
        assert_eq!(report_message(None, None, &[]), None);
    }

    #[test]
    fn oom_kill_names_the_queries_it_interrupted() {
        let queries: Vec<String> = (0..12).map(|i| format!("q{i}")).collect();
        let message = report_message(Some(7), Some(137), &queries).unwrap();
        assert!(
            message.starts_with("The previous instance (pid 7) was killed by SIGKILL"),
            "{message}"
        );
        assert!(
            message.contains("while running 12 queries: q0 | q1"),
            "{message}"
        );
        assert!(message.ends_with("q9 | …"), "{message}");
    }
}
