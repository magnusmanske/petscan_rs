use crate::config::Config;
use crate::content_type::ContentType;
use crate::database_manager::DatabaseManager;
use crate::database_manager::DbCluster;
use crate::form_parameters::FormParameters;
use crate::platform::MyResponse;
use anyhow::{Context, Result, anyhow};
use serde_json::Value;
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;
use wikimisc::jsonp::{self, CallbackRules};
use wikimisc::mediawiki::api::Api;
use wikimisc::mysql_async as my;
use wikimisc::retry::{Retry, RetryPolicy};
use wikimisc::site_matrix::SiteMatrix;
use wikimisc::telemetry::ExitReason;

/// Inbound concurrency cap: at most this many `process_form` calls run at
/// once. Excess requests queue on the semaphore, up to
/// [`MAX_QUEUED_REQUESTS`] of them for at most [`QUEUE_WAIT_TIMEOUT`].
/// 50 is roughly 5× the per-user MySQL connection budget, so the
/// `max_user_connections` backoff still has headroom and a request burst
/// cannot fork-bomb the worker pool.
const MAX_CONCURRENT_REQUESTS: usize = 50;

/// Admission queue cap: at most this many requests wait for a permit; any
/// further one is turned away at once with a 503. An unbounded queue let a
/// request flood hold ~1000 sockets open, exhausting the process's file
/// descriptors, after which nothing (not even `accept()`) worked.
const MAX_QUEUED_REQUESTS: usize = 200;

/// How long a queued request may wait for a permit before being turned
/// away. Beyond this the client (or the Cloud VPS proxy) has usually given
/// up, so starting the query would only waste a slot.
const QUEUE_WAIT_TIMEOUT: Duration = Duration::from_secs(120);

/// Retry policy for startup network calls (Wikidata API + `SiteMatrix`).
/// 5 attempts with 1s → 2s → 4s → 8s exponential backoff, capped at 16s
/// (total worst-case ~15s of waiting). Long enough to ride out a transient
/// blip on systemd restart; short enough that a real outage surfaces quickly.
const STARTUP_MAX_ATTEMPTS: u32 = 5;
const STARTUP_BASE_DELAY: Duration = Duration::from_secs(1);
const STARTUP_MAX_DELAY: Duration = Duration::from_secs(16);

/// Retry an async operation that returns `Result<T, anyhow::Error>` with
/// bounded exponential backoff, whatever the error. Logs each retry at
/// `warn` level.
async fn retry_with_backoff<F, Fut, T>(label: &str, op: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    RetryPolicy::new(STARTUP_MAX_ATTEMPTS - 1)
        .with_initial_backoff(STARTUP_BASE_DELAY)
        .with_max_wait(STARTUP_MAX_DELAY)
        .on_retry(|retry: &Retry<'_, anyhow::Error>| {
            tracing::warn!(
                label = retry.endpoint,
                attempt = retry.retry,
                max_attempts = STARTUP_MAX_ATTEMPTS,
                delay_ms = retry.wait.as_millis(),
                error = %retry.error,
                "{} startup failed; retrying",
                retry.endpoint
            );
        })
        .run_if(label, |_| true, op)
        .await
}

/// JSONP callback names PetScan accepts: dotted paths and indexes, such as
/// jQuery's generated `jQuery123_456` or a gadget's `mw.foo.cb`.
const JSONP_CALLBACK_RULES: CallbackRules = CallbackRules {
    allow_dotted: true,
    max_len: 256,
};

/// `callback` if it is a safe JSONP callback name. The name ends up verbatim
/// in a script the browser runs, so a response with an invalid one is sent
/// as plain JSON instead.
pub fn valid_jsonp_callback(callback: &str) -> Option<&str> {
    jsonp::is_valid_callback(callback, JSONP_CALLBACK_RULES).then_some(callback)
}

// ---------------------------------------------------------------------------
// AppState – top-level application state; delegates DB work to DatabaseManager
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct AppState {
    db_manager: DatabaseManager,
    threads_running: Arc<RwLock<i64>>,
    /// Set once a graceful shutdown starts; says why (e.g. `"SIGTERM"`).
    shutdown_reason: Arc<OnceLock<String>>,
    /// Set once the drained process has started exiting.
    exiting: Arc<AtomicBool>,
    site_matrix: SiteMatrix,
    main_page: String,
    /// Caps inbound request concurrency. See [`MAX_CONCURRENT_REQUESTS`].
    request_semaphore: Arc<Semaphore>,
    /// Caps how many requests may wait for `request_semaphore`. See
    /// [`MAX_QUEUED_REQUESTS`].
    queue_semaphore: Arc<Semaphore>,
    /// See [`Config::max_result_entries`].
    max_result_entries: usize,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            db_manager: DatabaseManager::default(),
            threads_running: Arc::new(RwLock::new(0)),
            shutdown_reason: Arc::default(),
            exiting: Arc::default(),
            site_matrix: SiteMatrix::default(),
            main_page: String::default(),
            request_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS)),
            queue_semaphore: Arc::new(Semaphore::new(MAX_QUEUED_REQUESTS)),
            max_result_entries: crate::config::DEFAULT_MAX_RESULT_ENTRIES,
        }
    }
}

impl AppState {
    pub async fn new_from_config(config: &Config) -> Result<Self> {
        let main_page_path = "./html/index.html";
        // Both the Wikidata API handshake and the SiteMatrix fetch are
        // network-dependent and previously hard-failed at startup if
        // Wikidata was even briefly unreachable (e.g. during a systemd
        // restart that races with the upstream). Retry with backoff so
        // the service survives transient blips.
        let wikidata_api = retry_with_backoff("Wikidata API", || async {
            Api::new("https://www.wikidata.org/w/api.php")
                .await
                .map_err(|e| anyhow!("Can't talk to Wikidata API: {e}"))
        })
        .await?;
        let main_page_bytes = fs::read(main_page_path)
            .map_err(|e| anyhow!("Could not read index.html file from disk: {e}"))?;
        let main_page = String::from_utf8_lossy(&main_page_bytes)
            .parse()
            .map_err(|e: std::convert::Infallible| anyhow!("Parsing index.html failed: {e}"))?;

        let db_manager = DatabaseManager::new_from_config(config);

        let site_matrix = retry_with_backoff("Wikidata SiteMatrix", || async {
            SiteMatrix::new(&wikidata_api)
                .await
                .map_err(|e| anyhow!("Can't get site matrix: {e}"))
        })
        .await?;

        Ok(Self {
            db_manager,
            site_matrix,
            main_page,
            max_result_entries: config.max_result_entries(),
            ..Self::default()
        })
    }

    /// The most pages one query may load. See [`Config::max_result_entries`].
    pub const fn max_result_entries(&self) -> usize {
        self.max_result_entries
    }

    /// Acquire a permit before doing heavy per-request work. The permit is
    /// released when the returned guard is dropped.
    ///
    /// Overload is reported, not absorbed: when [`MAX_QUEUED_REQUESTS`]
    /// callers are already waiting, or this one has waited
    /// [`QUEUE_WAIT_TIMEOUT`], the request is turned away with an
    /// [`OverloadError`] that says so, instead of holding its socket open
    /// for minutes.
    pub async fn acquire_request_permit(&self) -> Result<OwnedSemaphorePermit, OverloadError> {
        let Ok(_queue_slot) = self.queue_semaphore.clone().try_acquire_owned() else {
            return Err(self.overload_error("the waiting queue is full"));
        };
        match timeout(
            QUEUE_WAIT_TIMEOUT,
            self.request_semaphore.clone().acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => Ok(permit),
            Ok(Err(_closed)) => Err(self.overload_error("the server is shutting down")),
            Err(_elapsed) => Err(self.overload_error(&format!(
                "no slot became free within {} seconds",
                QUEUE_WAIT_TIMEOUT.as_secs()
            ))),
        }
    }

    fn overload_error(&self, reason: &str) -> OverloadError {
        OverloadError {
            running: self.requests_running(),
            waiting: self.requests_waiting(),
            reason: reason.to_string(),
        }
    }

    /// Requests currently holding a permit, i.e. doing real work.
    pub fn requests_running(&self) -> usize {
        MAX_CONCURRENT_REQUESTS - self.request_semaphore.available_permits()
    }

    /// Requests currently queued for a permit.
    pub fn requests_waiting(&self) -> usize {
        MAX_QUEUED_REQUESTS - self.queue_semaphore.available_permits()
    }

    /// Test-only: occupy every running and queued slot, so the next
    /// `acquire_request_permit` is turned away immediately.
    #[cfg(test)]
    pub(crate) fn saturate_for_test(&self) -> (OwnedSemaphorePermit, OwnedSemaphorePermit) {
        let running = self
            .request_semaphore
            .clone()
            .try_acquire_many_owned(MAX_CONCURRENT_REQUESTS as u32)
            .expect("fresh state");
        let queued = self
            .queue_semaphore
            .clone()
            .try_acquire_many_owned(MAX_QUEUED_REQUESTS as u32)
            .expect("fresh state");
        (running, queued)
    }

    /// Test-only constructor: builds an `AppState` over a default
    /// `DatabaseManager` / empty `SiteMatrix`, but with a chosen `main_page`
    /// body. Used by `webserver::tests` to pin assertions on response bodies.
    #[cfg(test)]
    pub(crate) fn for_test_with_main_page(main_page: impl Into<String>) -> Self {
        Self {
            main_page: main_page.into(),
            ..Self::default()
        }
    }

    /// Test-only constructor: like [`Self::for_test_with_main_page`], but with
    /// a `SiteMatrix` built from a fixture. Renderer tests need this to
    /// resolve wiki URLs (e.g. `wikidatawiki`) that the empty default matrix
    /// does not know.
    #[cfg(test)]
    pub(crate) fn for_test_with_site_matrix(site_matrix: SiteMatrix) -> Self {
        Self {
            site_matrix,
            ..Self::default()
        }
    }

    // ------------------------------------------------------------------
    // Delegating accessors – config feature flags
    // ------------------------------------------------------------------

    pub const fn using_file_table(&self) -> bool {
        self.db_manager.using_file_table()
    }

    pub fn get_restart_code(&self) -> Option<&str> {
        self.db_manager.get_restart_code()
    }

    // ------------------------------------------------------------------
    // Delegating accessors – server / schema name resolution
    // ------------------------------------------------------------------

    /// The single cluster able to serve a query reading all of `tables`.
    pub fn cluster_for_tables(&self, wiki: &str, tables: &[&str]) -> Result<DbCluster> {
        self.db_manager.cluster_for_tables(wiki, tables)
    }

    /// Whether a query on `cluster` can read every one of `tables` for `wiki`.
    pub fn cluster_hosts_tables(&self, wiki: &str, cluster: DbCluster, tables: &[&str]) -> bool {
        self.db_manager.cluster_hosts_tables(wiki, cluster, tables)
    }

    /// Whether `wiki` has this cluster at all.
    pub fn wiki_has_cluster(&self, wiki: &str, cluster: DbCluster) -> bool {
        self.db_manager.wiki_has_cluster(wiki, cluster)
    }

    // ------------------------------------------------------------------
    // Delegating accessors – database connections
    // ------------------------------------------------------------------

    pub async fn get_wiki_db_connection(&self, wiki: &str) -> Result<my::Conn> {
        self.db_manager.get_wiki_db_connection(wiki).await
    }

    /// Connects to one cluster of a wiki, falling back to [`DbCluster::Core`]
    /// for wikis that do not have it.
    pub async fn get_wiki_db_connection_for_cluster(
        &self,
        wiki: &str,
        cluster: DbCluster,
    ) -> Result<my::Conn> {
        self.db_manager
            .get_wiki_db_connection_for_cluster(wiki, cluster)
            .await
    }

    /// Connects to the cluster able to serve a query reading all of `tables`.
    pub async fn get_wiki_db_connection_for_tables(
        &self,
        wiki: &str,
        tables: &[&str],
    ) -> Result<my::Conn> {
        self.db_manager
            .get_wiki_db_connection_for_tables(wiki, tables)
            .await
    }

    /// Connects to the X3 / Wikidata term-store cluster.
    pub async fn get_x3_db_connection(&self) -> Result<my::Conn> {
        self.db_manager.get_x3_db_connection().await
    }

    /// Opens a connection to the tool database.
    pub async fn get_tool_db_connection(&self) -> Result<my::Conn> {
        self.db_manager.get_tool_db_connection().await
    }

    // ------------------------------------------------------------------
    // Delegating accessors – PSID / query logging
    // ------------------------------------------------------------------

    pub async fn get_query_from_psid(&self, psid: &str) -> Result<String> {
        self.db_manager.get_query_from_psid(psid).await
    }

    /// Records a query as running in `started_queries`. The row is removed
    /// when the returned entry is finished or dropped, so a query that
    /// fails, panics, times out or loses its client does not leave it behind.
    /// A failure to log is only warned about: it must not fail the query.
    pub async fn log_query_start(&self, query_string: &str) -> QueryLogEntry {
        let id = match self.db_manager.log_query_start(query_string).await {
            Ok(id) => Some(id),
            Err(e) => {
                tracing::warn!("Could not log query start: {e}\n{query_string}");
                None
            }
        };
        QueryLogEntry {
            db_manager: self.db_manager.clone(),
            id,
        }
    }

    pub async fn take_orphaned_queries(&self, process_id: u32, since: &str) -> Result<Vec<String>> {
        self.db_manager
            .take_orphaned_queries(process_id, since)
            .await
    }

    pub async fn get_or_create_psid_for_query(&self, query_string: &str) -> Result<u64> {
        self.db_manager
            .get_or_create_psid_for_query(query_string)
            .await
    }

    // ------------------------------------------------------------------
    // Native AppState behaviour – main page / rendering
    // ------------------------------------------------------------------

    pub fn get_main_page(&self, interface_language: String) -> String {
        let direction = if self.site_matrix.is_language_rtl(&interface_language) {
            "rtl"
        } else {
            "ltr"
        };
        let h = format!(
            "<html dir='{}' lang='{}'>",
            direction,
            interface_language.replace('\'', "")
        );
        let ret = self.main_page.replace("<html>", &h);
        if self.site_matrix.is_language_rtl(&interface_language) {
            ret.replace("bootstrap.min.css", "bootstrap-rtl.min.css")
        } else {
            ret
        }
    }

    pub fn render_error(&self, error: String, form_parameters: &FormParameters) -> MyResponse {
        let status = AppError::classify(&error).status();
        // Server-side log so monitoring/alerting can detect failures without
        // needing to scrape response bodies. The audit's P1 #9 noted that
        // `render_error` was the silent path: HTTP 200 + no log.
        tracing::error!(error = %error, http_status = status, "rendering error response");
        let mut response = match form_parameters.params.get("format").map(|s| s.as_str()) {
            Some("") | Some("html") => {
                // Error texts can echo user input (wiki names, PSIDs, …).
                let output = format!(
                    "<div class='alert alert-danger' role='alert'>{}</div>",
                    wikimisc::escape::html(&error)
                );
                let interface_language = form_parameters
                    .params
                    .get("interface_language")
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "en".to_string());
                let html = self.get_main_page(interface_language);
                let html = html.replace("<!--querystring-->", form_parameters.to_string().as_str());
                let html = &html.replace("<!--output-->", &output);
                MyResponse {
                    s: html.to_string(),
                    content_type: ContentType::HTML,
                    status: 200,
                }
            }
            Some("json") => {
                let value = json!({ "error": error });
                self.output_json(&value, form_parameters.params.get("callback"))
            }
            _ => MyResponse {
                s: error,
                content_type: ContentType::Plain,
                status: 200,
            },
        };
        response.status = status;
        response
    }

    pub fn output_json(&self, value: &Value, callback: Option<&String>) -> MyResponse {
        let json_string = ::serde_json::to_string(&value)
            .unwrap_or_else(|e| format!("{{\"error\":\"JSON serialization failed: {e}\"}}"));
        match callback.and_then(|callback| valid_jsonp_callback(callback)) {
            Some(callback) => {
                let text = format!("{callback}({json_string})");
                MyResponse {
                    s: text,
                    content_type: ContentType::JSONP,
                    status: 200,
                }
            }
            None => MyResponse {
                s: json_string,
                content_type: ContentType::JSON,
                status: 200,
            },
        }
    }

    /// The API client for `wiki`, built on first use and then shared by every
    /// request. Building one costs a TLS client plus a `siteinfo` round trip,
    /// and a single query asks for the same wiki's client several times.
    pub async fn get_api_for_wiki(&self, wiki: String) -> Result<Api> {
        self.site_matrix
            .cached_api_for_wiki(&wiki)
            .await
            .with_context(|| format!("Could not initialise the MediaWiki API client for {wiki}"))
    }

    // ------------------------------------------------------------------
    // Native AppState behaviour – thread / shutdown management
    // ------------------------------------------------------------------

    /// Exits once a shutdown has been requested and nothing is in flight.
    /// The exit waits (a few seconds at most) for the final telemetry push.
    pub fn try_shutdown(&self) {
        let Some(reason) = self.shutdown_reason.get() else {
            return;
        };
        if self.threads_running() != 0 || self.exiting.swap(true, Ordering::SeqCst) {
            return;
        }
        tracing::info!("Nothing in flight; exiting ({reason})");
        let exit = telemetry_shutdown_then_exit(reason.clone());
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => drop(runtime.spawn(exit)),
            Err(_) => std::process::exit(0),
        }
    }

    pub fn modify_threads_running(&self, diff: i64) {
        if let Ok(mut tr) = self.threads_running.write() {
            *tr += diff;
        }
        self.try_shutdown();
    }

    /// Read-only snapshot of the in-flight request counter.
    pub fn threads_running(&self) -> i64 {
        self.threads_running.read().map(|x| *x).unwrap_or(0)
    }

    /// Increment the in-flight request counter and return an RAII guard that
    /// decrements on drop. Use this in preference to direct
    /// `modify_threads_running` pairs so that a panic on the request path
    /// cannot leave the counter inflated forever.
    pub fn track_thread(&self) -> ThreadGuard {
        self.modify_threads_running(1);
        ThreadGuard {
            app_state: self.clone(),
        }
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutdown_reason.get().is_some()
    }

    /// Starts a graceful shutdown; `reason` (e.g. `"SIGTERM"`) is reported
    /// to the dashboard. The first reason given wins.
    pub fn shut_down(&self, reason: &str) {
        let _ = self.shutdown_reason.set(reason.to_string());
    }

    pub const fn site_matrix(&self) -> &SiteMatrix {
        &self.site_matrix
    }

    /// Expose the underlying [`DatabaseManager`] for callers that need direct access.
    pub const fn db_manager(&self) -> &DatabaseManager {
        &self.db_manager
    }
}

async fn telemetry_shutdown_then_exit(reason: String) {
    wikimisc::telemetry::shutdown(ExitReason::Shutdown(reason)).await;
    std::process::exit(0);
}

/// RAII guard returned by [`AppState::track_thread`]. Decrements the
/// in-flight request counter when dropped, including on unwind.
#[must_use = "ThreadGuard decrements the counter when dropped; binding it to `_` drops it immediately"]
#[derive(Debug)]
pub struct ThreadGuard {
    app_state: AppState,
}

impl Drop for ThreadGuard {
    fn drop(&mut self) {
        self.app_state.modify_threads_running(-1);
    }
}

/// A running query's `started_queries` row, returned by
/// [`AppState::log_query_start`]. [`Self::finish`] removes it; if the entry
/// is dropped instead (the request future was cancelled because the client
/// went away or the time budget ran out, or a panic is unwinding), `Drop`
/// removes it in the background. Only rows of a process that really died
/// remain, for [`crate::previous_run`] to report.
#[must_use = "dropping the entry ends the query log row at once"]
#[derive(Debug)]
pub struct QueryLogEntry {
    db_manager: DatabaseManager,
    /// `None` once finished, or if the row could not be inserted.
    id: Option<u64>,
}

impl QueryLogEntry {
    /// Removes the row, waiting until that is done.
    pub async fn finish(mut self) {
        if let Some(id) = self.id.take() {
            Self::end(&self.db_manager, id).await;
        }
    }

    async fn end(db_manager: &DatabaseManager, id: u64) {
        if let Err(e) = db_manager.log_query_end(id).await {
            tracing::warn!("Could not log query {id} end: {e}");
        }
    }
}

impl Drop for QueryLogEntry {
    fn drop(&mut self) {
        let Some(id) = self.id.take() else {
            return;
        };
        // `Drop` cannot await, so the DELETE runs as a task of its own.
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                let db_manager = std::mem::take(&mut self.db_manager);
                drop(runtime.spawn(async move { Self::end(&db_manager, id).await }));
            }
            Err(_) => tracing::warn!("Query {id} stays in started_queries: no runtime"),
        }
    }
}

/// HTTP-category classification of free-form error strings produced by
/// the request pipeline. Used by [`AppState::render_error`] to pick the
/// HTTP status code; previously every failure rendered as 200, which
/// meant monitoring could not distinguish a DB outage from a successful
/// zero-result query.
/// Why a request was turned away at admission. Shown to the user, so it
/// says what is happening and what to do about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverloadError {
    running: usize,
    waiting: usize,
    reason: String,
}

impl std::fmt::Display for OverloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PetScan is overloaded ({} queries running, {} waiting): {}. Please try again in a few minutes.",
            self.running, self.waiting, self.reason
        )
    }
}

impl std::error::Error for OverloadError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppError {
    /// 400 — user input was rejected.
    BadRequest,
    /// 503 — admission control turned the request away; retry later.
    Overloaded,
    /// 502 — a replica or external API failed.
    Upstream,
    /// 500 — anything else (treat as a server bug).
    Internal,
}

impl AppError {
    pub const fn status(self) -> u16 {
        match self {
            AppError::BadRequest => 400,
            AppError::Overloaded => 503,
            AppError::Upstream => 502,
            AppError::Internal => 500,
        }
    }

    /// Heuristic classification by substring match. Errors arrive here as
    /// `anyhow::Error::to_string()` so a perfect classifier would need
    /// structured errors at every callsite (P4 #32). This catches the
    /// common shapes so most user-visible failures get the right status.
    pub fn classify(error: &str) -> Self {
        let lc = error.to_lowercase();
        // Admission control turned the request away (503).
        if lc.contains("overloaded") {
            AppError::Overloaded
        // The query asked for more than the result cap, or the SPARQL
        // endpoint rejected it as malformed; checked before the upstream
        // keywords, since either message may mention SPARQL (400).
        } else if lc.contains("too many results") || lc.contains("endpoint rejected the query") {
            AppError::BadRequest
        // Upstream / infrastructure failures (502).
        } else if lc.contains("max_user_connections")
            || lc.contains("connection refused")
            || lc.contains("connection reset")
            || lc.contains("timed out")
            || lc.contains("timeout")
            || lc.contains("no route to host")
            || lc.contains("os error 61")
            || lc.contains("can't talk to wikidata api")
            || lc.contains("cannot connect to")
            || lc.contains("site matrix")
            || lc.contains("sparql")
            || lc.contains("pagepile")
        {
            AppError::Upstream
        // Client-side input errors (400).
        } else if lc.contains("invalid")
            || lc.contains("no command line argument")
            || lc.contains("missing parameter")
            || lc.contains("not allowed")
            || lc.contains("malformed")
            || lc.contains("no wiki in result")
        {
            AppError::BadRequest
        } else {
            AppError::Internal
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    async fn get_new_state() -> Arc<AppState> {
        let basedir = env::current_dir()
            .expect("Can't get CWD")
            .to_str()
            .unwrap()
            .to_string();
        let path = basedir.to_owned() + "/config.json";
        let petscan_config = Config::from_file(&path).expect("config.json load failed in test");
        Arc::new(
            AppState::new_from_config(&petscan_config)
                .await
                .expect("AppState::new_from_config failed in test"),
        )
    }

    async fn get_state() -> Arc<AppState> {
        get_new_state().await
    }

    /// Build a minimal config for unit tests that don't need a real DB connection.
    fn make_minimal_config() -> Config {
        Config {
            schema: "test_schema".into(),
            ..Default::default()
        }
    }

    /// Helper: build an [`AppState`] whose [`DatabaseManager`] is seeded with the given config.
    fn state_with_config(config: Config) -> AppState {
        AppState {
            db_manager: DatabaseManager::with_config(config),
            ..Default::default()
        }
    }

    #[test]
    fn test_using_file_table() {
        let state_true = state_with_config(Config {
            use_file_table: true,
            ..Default::default()
        });
        assert!(state_true.using_file_table());

        let state_false = state_with_config(Config {
            use_file_table: false,
            ..Default::default()
        });
        assert!(!state_false.using_file_table());

        let state_missing = state_with_config(Config::default());
        assert!(!state_missing.using_file_table());
    }

    #[test]
    fn test_get_restart_code() {
        let state = state_with_config(Config {
            restart_code: Some("abc123".into()),
            ..Default::default()
        });
        assert_eq!(state.get_restart_code(), Some("abc123"));

        let state2 = state_with_config(Config::default());
        assert_eq!(state2.get_restart_code(), None);
    }

    #[test]
    fn test_cluster_for_tables() {
        let state = state_with_config(make_minimal_config());
        // `page` exists on both Commons clusters, so links joins still work.
        assert_eq!(
            state
                .cluster_for_tables("commonswiki", &["page", "categorylinks", "linktarget"])
                .unwrap(),
            DbCluster::Links
        );
        assert_eq!(
            state
                .cluster_for_tables("enwiki", &["page", "categorylinks", "linktarget"])
                .unwrap(),
            DbCluster::Core
        );
        assert_eq!(
            state
                .cluster_for_tables("commonswiki", &["page", "page_props"])
                .unwrap(),
            DbCluster::Core
        );
        // A join across the Commons split has no single host to run on.
        assert!(
            state
                .cluster_for_tables("commonswiki", &["categorylinks", "revision"])
                .is_err()
        );
        // The same join is fine anywhere else.
        assert!(
            state
                .cluster_for_tables("dewiki", &["categorylinks", "revision"])
                .is_ok()
        );
    }

    #[tokio::test]
    #[ignore = "requires live network/DB (loads config.json + Wikidata SiteMatrix); run with --ignored"]
    async fn test_render_error_html() {
        // HTML render_error requires a fully initialized AppState (SiteMatrix),
        // so we use the full state loaded from config.
        let state = get_state().await;
        let mut params = crate::form_parameters::FormParameters::new();
        params
            .params
            .insert("format".to_string(), "html".to_string());
        let response = state.render_error("Test error".to_string(), &params);
        assert!(response.s.contains("Test error"));
        assert_eq!(response.content_type, ContentType::HTML);
    }

    #[test]
    fn test_render_error_json() {
        let state = state_with_config(make_minimal_config());
        let mut params = crate::form_parameters::FormParameters::new();
        params
            .params
            .insert("format".to_string(), "json".to_string());
        let response = state.render_error("Test error".to_string(), &params);
        assert!(response.s.contains("Test error"));
        assert_eq!(response.content_type, ContentType::JSON);
    }

    #[test]
    fn test_render_error_plain() {
        let state = state_with_config(make_minimal_config());
        let mut params = crate::form_parameters::FormParameters::new();
        params
            .params
            .insert("format".to_string(), "plaintext".to_string());
        let response = state.render_error("Test error".to_string(), &params);
        assert_eq!(response.s, "Test error");
        assert_eq!(response.content_type, ContentType::Plain);
    }

    #[test]
    fn test_is_shutting_down_default() {
        let state = AppState::default();
        // Default state should not be shutting down
        assert!(!state.is_shutting_down());
    }

    #[test]
    fn test_shut_down() {
        let state = AppState::default();
        assert!(!state.is_shutting_down());
        state.shut_down("SIGTERM");
        assert!(state.is_shutting_down());
        state.shut_down("restart requested");
        assert_eq!(
            state.shutdown_reason.get().map(String::as_str),
            Some("SIGTERM")
        );
    }

    #[tokio::test]
    async fn test_requests_running_and_waiting() {
        let state = AppState::default();
        assert_eq!((state.requests_running(), state.requests_waiting()), (0, 0));
        let _slots = state.saturate_for_test();
        assert_eq!(
            (state.requests_running(), state.requests_waiting()),
            (MAX_CONCURRENT_REQUESTS, MAX_QUEUED_REQUESTS)
        );
    }

    #[test]
    fn test_track_thread_guard_increments_and_decrements() {
        let state = AppState::default();
        assert_eq!(state.threads_running(), 0);
        {
            let _guard = state.track_thread();
            assert_eq!(state.threads_running(), 1);
        }
        assert_eq!(state.threads_running(), 0);
    }

    #[test]
    fn test_track_thread_guards_stack() {
        let state = AppState::default();
        let g1 = state.track_thread();
        let g2 = state.track_thread();
        assert_eq!(state.threads_running(), 2);
        drop(g1);
        assert_eq!(state.threads_running(), 1);
        drop(g2);
        assert_eq!(state.threads_running(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn test_retry_with_backoff_eventual_success() {
        // Fails 3 times, then succeeds — should return Ok on attempt 4.
        let attempts = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let attempts_for_op = attempts.clone();
        let result = retry_with_backoff("test", move || {
            let counter = attempts_for_op.clone();
            async move {
                let n = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if n < 3 {
                    Err(anyhow!("simulated transient failure {n}"))
                } else {
                    Ok::<_, anyhow::Error>(42)
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(result, 42);
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn test_retry_with_backoff_exhausts_attempts() {
        // Always fails — should return Err after STARTUP_MAX_ATTEMPTS.
        let attempts = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let attempts_for_op = attempts.clone();
        let result: Result<i32> = retry_with_backoff("test", move || {
            let counter = attempts_for_op.clone();
            async move {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Err(anyhow!("persistent failure"))
            }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            STARTUP_MAX_ATTEMPTS
        );
    }

    #[test]
    fn test_app_error_status_codes() {
        assert_eq!(AppError::BadRequest.status(), 400);
        assert_eq!(AppError::Upstream.status(), 502);
        assert_eq!(AppError::Internal.status(), 500);
    }

    #[test]
    fn test_app_error_classify_upstream() {
        for msg in [
            "Too many connections: max_user_connections exceeded",
            "Connection refused (os error 61)",
            "Connection reset by peer",
            "operation timed out",
            "Timeout after 30 seconds",
            "no route to host",
            "Can't talk to Wikidata API: foo",
            "DatabaseManager::get_tool_db_connection cannot connect to host:3306",
            "Can't get site matrix",
            "SPARQL endpoint returned 503",
            "PagePile fetch failed",
        ] {
            assert_eq!(
                AppError::classify(msg),
                AppError::Upstream,
                "expected Upstream for: {msg}"
            );
        }
    }

    #[test]
    fn test_app_error_classify_bad_request() {
        for msg in [
            "Invalid regex: foo",
            "No command line argument provided",
            "Missing parameter: psid",
            "Operation not allowed for this user",
            "Malformed PSID",
            "No wiki in result",
        ] {
            assert_eq!(
                AppError::classify(msg),
                AppError::BadRequest,
                "expected BadRequest for: {msg}"
            );
        }
    }

    #[test]
    fn test_app_error_classify_internal_default() {
        for msg in [
            "panicked at something",
            "platform.run failed unexpectedly",
            "Could not insert new PSID",
            "JSON serialization failed",
        ] {
            assert_eq!(
                AppError::classify(msg),
                AppError::Internal,
                "expected Internal for: {msg}"
            );
        }
    }

    #[test]
    fn test_render_error_sets_status_from_classification() {
        let state = state_with_config(make_minimal_config());
        let mut params = crate::form_parameters::FormParameters::new();
        params
            .params
            .insert("format".to_string(), "json".to_string());

        let resp_upstream =
            state.render_error("Connection refused (os error 61)".to_string(), &params);
        assert_eq!(resp_upstream.status, 502);

        let resp_bad = state.render_error("Invalid parameter foo".to_string(), &params);
        assert_eq!(resp_bad.status, 400);

        let resp_internal = state.render_error("Could not insert new PSID".to_string(), &params);
        assert_eq!(resp_internal.status, 500);
    }

    #[test]
    fn test_track_thread_guard_decrements_on_panic_unwind() {
        // Even if a request panics, the guard's Drop must restore the
        // counter. Verify via catch_unwind.
        let state = AppState::default();
        let state_for_closure = state.clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _guard = state_for_closure.track_thread();
            assert_eq!(state_for_closure.threads_running(), 1);
            panic!("simulated request failure");
        }));
        assert!(result.is_err());
        assert_eq!(state.threads_running(), 0);
    }

    #[tokio::test]
    async fn test_query_log_entry_drop_never_panics() {
        // `Drop` runs while unwinding from a panic, where a second panic
        // would abort the process; with or without a runtime, it must not.
        // (The DELETE itself needs the tools DB; here it just fails.)
        let entry = || QueryLogEntry {
            db_manager: DatabaseManager::default(),
            id: Some(1),
        };
        drop(entry());
        std::thread::spawn(move || drop(entry())).join().unwrap();
        entry().finish().await;
    }

    #[tokio::test]
    #[ignore = "requires live network/DB (loads config.json + Wikidata SiteMatrix); run with --ignored"]
    async fn test_get_wiki_for_server_url() {
        let state = get_state().await;
        assert_eq!(
            state
                .site_matrix
                .get_wiki_for_server_url("https://am.wiktionary.org"),
            Some("amwiktionary".to_string())
        );
        assert_eq!(
            state
                .site_matrix
                .get_wiki_for_server_url("https://outreach.wikimedia.org"),
            Some("outreachwiki".to_string())
        );
    }

    #[tokio::test]
    async fn acquire_request_permit_succeeds_when_idle() {
        let state = AppState::default();
        assert!(state.acquire_request_permit().await.is_ok());
    }

    #[tokio::test]
    async fn acquire_request_permit_rejects_when_queue_is_full() {
        let state = AppState::default();
        let _slots = state.saturate_for_test();
        let err = state.acquire_request_permit().await.unwrap_err();
        assert_eq!(
            (err.running, err.waiting),
            (MAX_CONCURRENT_REQUESTS, MAX_QUEUED_REQUESTS)
        );
        assert!(err.to_string().contains("queue is full"), "{err}");
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_request_permit_gives_up_after_queue_wait_timeout() {
        let state = AppState::default();
        let _running = state
            .request_semaphore
            .clone()
            .try_acquire_many_owned(MAX_CONCURRENT_REQUESTS as u32)
            .expect("fresh state");
        let err = state.acquire_request_permit().await.unwrap_err();
        assert!(err.to_string().contains("no slot became free"), "{err}");
        assert_eq!(err.running, MAX_CONCURRENT_REQUESTS);
        // The queue slot is released once the caller has been turned away.
        assert_eq!(
            state.queue_semaphore.available_permits(),
            MAX_QUEUED_REQUESTS
        );
    }

    #[test]
    fn overloaded_errors_map_to_503() {
        let state = AppState::default();
        let message = state
            .overload_error("the waiting queue is full")
            .to_string();
        assert_eq!(AppError::classify(&message), AppError::Overloaded);
        assert_eq!(AppError::Overloaded.status(), 503);
    }

    #[tokio::test]
    async fn get_api_for_wiki_error_names_the_wiki() {
        let err = AppState::default()
            .get_api_for_wiki("nosuchwiki".to_string())
            .await
            .unwrap_err();
        // The wiki is named, and the underlying cause stays in the chain.
        let chain = format!("{err:#}");
        assert!(
            chain.starts_with("Could not initialise the MediaWiki API client for nosuchwiki: "),
            "{chain}"
        );
        assert!(err.chain().count() > 1, "{chain}");
    }

    #[test]
    fn render_error_escapes_html() {
        let state = AppState::for_test_with_main_page("<!--output-->");
        let params =
            FormParameters::new_from_pairs([("format".to_string(), "html".to_string())].into());
        let response = state.render_error("Invalid PSID '<script>x</script>'".to_string(), &params);
        assert!(!response.s.contains("<script>"), "{}", response.s);
        assert!(response.s.contains("&lt;script&gt;"), "{}", response.s);
    }
}
