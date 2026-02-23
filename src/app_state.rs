use crate::content_type::ContentType;
use crate::database_manager::DatabaseManager;
use crate::form_parameters::FormParameters;
use crate::pagelist::DatabaseCluster;
use crate::platform::MyResponse;
use anyhow::{Result, anyhow};
use mysql_async as my;
use serde_json::Value;
use std::fs;
use std::sync::{Arc, RwLock};
use wikimisc::mediawiki::api::Api;
use wikimisc::site_matrix::SiteMatrix;

// ---------------------------------------------------------------------------
// AppState – top-level application state; delegates DB work to DatabaseManager
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct AppState {
    db_manager: DatabaseManager,
    threads_running: Arc<RwLock<i64>>,
    shutting_down: Arc<RwLock<bool>>,
    site_matrix: SiteMatrix,
    main_page: String,
}

impl AppState {
    pub async fn new_from_config(config: &Value) -> Result<Self> {
        let main_page_path = "./html/index.html";
        let wikidata_api = Api::new("https://www.wikidata.org/w/api.php")
            .await
            .map_err(|e| anyhow!("Can't talk to Wikidata API: {e}"))?;
        let main_page_bytes = fs::read(main_page_path)
            .map_err(|e| anyhow!("Could not read index.html file from disk: {e}"))?;
        let main_page = String::from_utf8_lossy(&main_page_bytes)
            .parse()
            .map_err(|e: std::convert::Infallible| anyhow!("Parsing index.html failed: {e}"))?;

        let db_manager = DatabaseManager::new_from_config(config);

        Ok(Self {
            db_manager,
            threads_running: Arc::new(RwLock::new(0)),
            shutting_down: Arc::new(RwLock::new(false)),
            site_matrix: SiteMatrix::new(&wikidata_api)
                .await
                .map_err(|e| anyhow!("Can't get site matrix: {e}"))?,
            main_page,
        })
    }

    // ------------------------------------------------------------------
    // Delegating accessors – config feature flags
    // ------------------------------------------------------------------

    pub fn using_file_table(&self) -> bool {
        self.db_manager.using_file_table()
    }

    pub fn using_new_categorylinks_table(&self) -> bool {
        self.db_manager.using_new_categorylinks_table()
    }

    pub fn get_restart_code(&self) -> Option<&str> {
        self.db_manager.get_restart_code()
    }

    // ------------------------------------------------------------------
    // Delegating accessors – server / schema name resolution
    // ------------------------------------------------------------------

    pub fn fix_wiki_name(&self, wiki: &str) -> String {
        self.db_manager.fix_wiki_name(wiki)
    }

    /// Returns the canonical Toolforge host and `_p`-suffixed database name
    /// for a wiki replica, as a `(host, schema)` tuple.
    pub fn db_host_and_schema_for_wiki(
        &self,
        wiki: &str,
        cluster: DatabaseCluster,
    ) -> (String, String) {
        self.db_manager.db_host_and_schema_for_wiki(wiki, cluster)
    }

    // ------------------------------------------------------------------
    // Delegating accessors – database connections
    // ------------------------------------------------------------------

    pub async fn get_wiki_db_connection(&self, wiki: &str) -> Result<my::Conn> {
        self.db_manager.get_wiki_db_connection(wiki).await
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

    pub async fn log_query_start(&self, query_string: &str) -> Result<u64> {
        self.db_manager.log_query_start(query_string).await
    }

    pub async fn log_query_end(&self, query_id: u64) -> Result<()> {
        self.db_manager.log_query_end(query_id).await
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
        match form_parameters.params.get("format").map(|s| s.as_str()) {
            Some("") | Some("html") => {
                let output = format!(
                    "<div class='alert alert-danger' role='alert'>{}</div>",
                    &error
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
                }
            }
            Some("json") => {
                let value = json!({ "error": error });
                self.output_json(&value, form_parameters.params.get("callback"))
            }
            _ => MyResponse {
                s: error,
                content_type: ContentType::Plain,
            },
        }
    }

    pub fn output_json(&self, value: &Value, callback: Option<&String>) -> MyResponse {
        let json_string = ::serde_json::to_string(&value)
            .unwrap_or_else(|e| format!("{{\"error\":\"JSON serialization failed: {e}\"}}"));
        match callback {
            Some(callback) => {
                let text = format!("{callback}({json_string})");
                MyResponse {
                    s: text,
                    content_type: ContentType::JSONP,
                }
            }
            None => MyResponse {
                s: json_string,
                content_type: ContentType::JSON,
            },
        }
    }

    pub async fn get_api_for_wiki(&self, wiki: String) -> Result<Api> {
        self.site_matrix.get_api_for_wiki(&wiki).await
    }

    // ------------------------------------------------------------------
    // Native AppState behaviour – thread / shutdown management
    // ------------------------------------------------------------------

    pub fn try_shutdown(&self) {
        if !self.is_shutting_down() {
            return;
        }
        if let Ok(tr) = self.threads_running.read()
            && *tr == 0
        {
            ::std::process::exit(0);
        }
    }

    pub fn modify_threads_running(&self, diff: i64) {
        if let Ok(mut tr) = self.threads_running.write() {
            *tr += diff;
        }
        self.try_shutdown();
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.read().map_or(false, |x| *x)
    }

    pub fn shut_down(&self) {
        if let Ok(mut sd) = self.shutting_down.write() {
            *sd = true;
        }
    }

    pub const fn site_matrix(&self) -> &SiteMatrix {
        &self.site_matrix
    }

    /// Expose the underlying [`DatabaseManager`] for callers that need direct access.
    pub const fn db_manager(&self) -> &DatabaseManager {
        &self.db_manager
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::env;
    use std::fs::File;

    async fn get_new_state() -> Arc<AppState> {
        let basedir = env::current_dir()
            .expect("Can't get CWD")
            .to_str()
            .unwrap()
            .to_string();
        let path = basedir.to_owned() + "/config.json";
        let file = File::open(path).expect("Can not open config file");
        let petscan_config: Value =
            serde_json::from_reader(file).expect("Can not parse JSON from config file");
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
    fn make_minimal_config() -> Value {
        serde_json::json!({
            "schema": "test_schema",
        })
    }

    /// Helper: build an [`AppState`] whose [`DatabaseManager`] is seeded with the given config.
    fn state_with_config(config: Value) -> AppState {
        AppState {
            db_manager: DatabaseManager::with_config(config),
            ..Default::default()
        }
    }

    #[test]
    fn test_fix_wiki_name_be_tarask() {
        let state = state_with_config(make_minimal_config());
        assert_eq!(state.fix_wiki_name("be-taraskwiki"), "be_x_oldwiki");
        assert_eq!(state.fix_wiki_name("be-x-oldwiki"), "be_x_oldwiki");
        assert_eq!(state.fix_wiki_name("be_taraskwiki"), "be_x_oldwiki");
        assert_eq!(state.fix_wiki_name("be_x_oldwiki"), "be_x_oldwiki");
    }

    #[test]
    fn test_fix_wiki_name_normal() {
        let state = state_with_config(make_minimal_config());
        assert_eq!(state.fix_wiki_name("enwiki"), "enwiki");
        assert_eq!(state.fix_wiki_name("wikidatawiki"), "wikidatawiki");
        // Hyphens converted to underscores for non-special wikis
        assert_eq!(state.fix_wiki_name("zh-min-nanwiki"), "zh_min_nanwiki");
    }

    #[test]
    fn test_using_file_table() {
        let state_true = state_with_config(serde_json::json!({ "use_file_table": true }));
        assert!(state_true.using_file_table());

        let state_false = state_with_config(serde_json::json!({ "use_file_table": false }));
        assert!(!state_false.using_file_table());

        let state_missing = state_with_config(serde_json::json!({}));
        assert!(!state_missing.using_file_table());
    }

    #[test]
    fn test_using_new_categorylinks_table() {
        let state = state_with_config(serde_json::json!({ "use_new_categorylinks_table": true }));
        assert!(state.using_new_categorylinks_table());
    }

    #[test]
    fn test_get_restart_code() {
        let state = state_with_config(serde_json::json!({ "restart-code": "abc123" }));
        assert_eq!(state.get_restart_code(), Some("abc123"));

        let state2 = state_with_config(serde_json::json!({}));
        assert_eq!(state2.get_restart_code(), None);
    }

    #[test]
    fn test_db_host_and_schema_for_wiki_web() {
        let state = state_with_config(make_minimal_config());
        let (host, schema) = state.db_host_and_schema_for_wiki("enwiki", DatabaseCluster::Default);
        assert_eq!(host, "enwiki.web.db.svc.wikimedia.cloud");
        assert_eq!(schema, "enwiki_p");
    }

    #[test]
    fn test_db_host_and_schema_for_wiki_x3() {
        let state = state_with_config(make_minimal_config());
        let (host, schema) = state.db_host_and_schema_for_wiki("wikidatawiki", DatabaseCluster::X3);
        assert_eq!(
            host,
            "termstore.wikidatawiki.analytics.db.svc.wikimedia.cloud"
        );
        assert_eq!(schema, "wikidatawiki_p");
    }

    #[test]
    fn test_db_host_and_schema_normalises_wiki_name() {
        let state = state_with_config(make_minimal_config());
        let (_host, schema) =
            state.db_host_and_schema_for_wiki("be-taraskwiki", DatabaseCluster::Default);
        assert_eq!(schema, "be_x_oldwiki_p");
    }

    #[tokio::test]
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
        state.shut_down();
        assert!(state.is_shutting_down());
    }

    #[tokio::test]
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

    #[test]
    fn test_db_host_and_schema_for_wiki_schema_names() {
        let state = state_with_config(make_minimal_config());
        assert_eq!(
            "enwiki_p".to_string(),
            state
                .db_host_and_schema_for_wiki("enwiki", DatabaseCluster::Default)
                .1
        );
        assert_eq!(
            "be_x_oldwiki_p".to_string(),
            state
                .db_host_and_schema_for_wiki("be-taraskwiki", DatabaseCluster::Default)
                .1
        );
    }
}
