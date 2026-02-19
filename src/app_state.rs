use crate::content_type::ContentType;
use crate::form_parameters::FormParameters;
use crate::pagelist::DatabaseCluster;
use crate::platform::MyResponse;
use anyhow::{anyhow, Result};
use chrono::prelude::*;
use mysql_async as my;
use mysql_async::from_row;
use mysql_async::prelude::Queryable;
use mysql_async::Value as MyValue;
use rand::seq::SliceRandom;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;
use tracing::{instrument, trace};
use wikimisc::mediawiki::api::Api;
use wikimisc::site_matrix::SiteMatrix;

const TERMSTORE_SERVER: &str = "termstore.wikidatawiki.analytics.db.svc.wikimedia.cloud";

pub type DbUserPass = (String, String);

#[derive(Debug, Clone, Default)]
pub struct AppState {
    db_pool: Arc<Mutex<Vec<DbUserPass>>>,
    config: Value,
    tool_db_mutex: Arc<Mutex<DbUserPass>>,
    threads_running: Arc<RwLock<i64>>,
    shutting_down: Arc<RwLock<bool>>,
    site_matrix: SiteMatrix,
    main_page: String,
    port_mapping: HashMap<String, u16>, // Local testing only
}

impl AppState {
    pub async fn new_from_config(config: &Value) -> Result<Self> {
        let main_page_path = "./html/index.html";
        let user = config["user"]
            .as_str()
            .ok_or_else(|| anyhow!("No user key in config file"))?
            .to_string();
        let password = config["password"]
            .as_str()
            .ok_or_else(|| anyhow!("No password key in config file"))?
            .to_string();
        let tool_db_access_tuple = (user, password);
        let port_mapping = config["port_mapping"]
            .as_object()
            .map(|x| x.to_owned())
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.as_i64().unwrap_or_default() as u16))
            .collect();
        let wikidata_api = Api::new("https://www.wikidata.org/w/api.php")
            .await
            .map_err(|e| anyhow!("Can't talk to Wikidata API: {e}"))?;
        let main_page_bytes = fs::read(main_page_path)
            .map_err(|e| anyhow!("Could not read index.html file from disk: {e}"))?;
        let main_page = String::from_utf8_lossy(&main_page_bytes)
            .parse()
            .map_err(|e: std::convert::Infallible| anyhow!("Parsing index.html failed: {e}"))?;
        let ret = Self {
            db_pool: Arc::new(Mutex::new(vec![])),
            config: config.to_owned(),
            port_mapping,
            threads_running: Arc::new(RwLock::new(0)),
            shutting_down: Arc::new(RwLock::new(false)),
            site_matrix: SiteMatrix::new(&wikidata_api)
                .await
                .map_err(|e| anyhow!("Can't get site matrix: {e}"))?,
            tool_db_mutex: Arc::new(Mutex::new(tool_db_access_tuple)),
            main_page,
        };

        if let Some(up_list) = config["mysql"].as_array() {
            let mut pool = ret.db_pool.lock().await;
            for up in up_list {
                let username = up[0]
                    .as_str()
                    .ok_or_else(|| anyhow!("Parsing user from mysql array in config failed"))?
                    .to_string();
                let pass = up[1]
                    .as_str()
                    .ok_or_else(|| anyhow!("Parsing pass from mysql array in config failed"))?
                    .to_string();
                let connections = up[2].as_u64().unwrap_or(5);
                // Ignore toolname up[3]

                for _num in 1..connections {
                    pool.push((username.to_string(), pass.to_string()));
                }
            }
            let mut rng = rand::rng();
            pool.shuffle(&mut rng);
        }
        if ret.db_pool.lock().await.is_empty() {
            return Err(anyhow!("No database access config available"));
        }
        Ok(ret)
    }

    pub fn using_file_table(&self) -> bool {
        self.config["use_file_table"].as_bool().unwrap_or(false)
    }

    pub fn using_new_categorylinks_table(&self) -> bool {
        self.config["use_new_categorylinks_table"]
            .as_bool()
            .unwrap_or(false)
    }

    pub fn get_restart_code(&self) -> Option<&str> {
        self.config["restart-code"].as_str()
    }

    fn get_mysql_opts_for_wiki(
        &self,
        wiki: &str,
        user: &str,
        pass: &str,
        cluster: DatabaseCluster,
    ) -> Result<my::Opts> {
        let (host, schema) = self.db_host_and_schema_for_wiki(wiki, cluster)?;
        let port: u16 = self.port_mapping.get(wiki).map_or_else(
            || self.config["db_port"].as_u64().unwrap_or(3306) as u16,
            |port| *port,
        );
        let opts = my::OptsBuilder::default()
            .ip_or_hostname(host)
            .db_name(Some(schema))
            .user(Some(user))
            .pass(Some(pass))
            .tcp_port(port)
            .into();
        Ok(opts)
    }

    fn get_mysql_opts_for_term_store(&self, user: &str, pass: &str) -> Result<my::Opts> {
        let (host, schema) =
            self.db_host_and_schema_for_wiki("wikidatawiki", DatabaseCluster::X3)?;
        let port: u16 = self.port_mapping.get("x3").map_or_else(
            || self.config["db_port"].as_u64().unwrap_or(3306) as u16,
            |port| *port,
        );
        let opts = my::OptsBuilder::default()
            .ip_or_hostname(host)
            .db_name(Some(schema))
            .user(Some(user))
            .pass(Some(pass))
            .tcp_port(port)
            .into();
        Ok(opts)
    }

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

    fn get_db_server_group(&self) -> &str {
        self.config["dbservergroup"]
            .as_str()
            .unwrap_or(".web.db.svc.eqiad.wmflabs")
    }

    pub fn fix_wiki_name(&self, wiki: &str) -> String {
        match wiki {
            "be-taraskwiki" | "be-x-oldwiki" | "be_taraskwiki" | "be_x_oldwiki" => "be_x_oldwiki",
            other => other,
        }
        .to_string()
        .replace('-', "_")
    }

    /// Returns the server and database name for the wiki, as a tuple
    /// # Panics
    /// Panics if the host key is missing from the config file
    pub fn db_host_and_schema_for_wiki(
        &self,
        wiki: &str,
        cluster: DatabaseCluster,
    ) -> Result<(String, String)> {
        // TESTING
        /*
        ssh magnus@login.toolforge.org -L 3307:dewiki.web.db.svc.eqiad.wmflabs:3306 -N &
        ssh magnus@login.toolforge.org -L 3309:wikidatawiki.web.db.svc.eqiad.wmflabs:3306 -N &
        ssh magnus@login.toolforge.org -L 3305:commonswiki.web.db.svc.eqiad.wmflabs:3306 -N &
        ssh magnus@login.toolforge.org -L 3310:enwiki.web.db.svc.eqiad.wmflabs:3306 -N &
         */

        let wiki = self.fix_wiki_name(wiki);
        let host = match self.config["host"].as_str() {
            Some("127.0.0.1") => "127.0.0.1".to_string(),
            Some(_host) => {
                if cluster == DatabaseCluster::X3 {
                    TERMSTORE_SERVER.to_string()
                } else {
                    wiki.to_owned() + self.get_db_server_group()
                }
            }
            None => return Err(anyhow!("No host in config file")),
        };
        let schema = format!("{wiki}_p");
        Ok((host, schema))
    }

    /// Returns the server and database name for the tool db, as a tuple
    pub fn db_host_and_schema_for_tool_db(&self) -> Result<(String, String)> {
        // TESTING
        /*
        ssh magnus@login.toolforge.org -L 3308:tools-db:3306 -N &
        */
        let host = self.config["host"]
            .as_str()
            .ok_or_else(|| anyhow!("No host key in config file"))?
            .to_string();
        let schema = self.config["schema"]
            .as_str()
            .ok_or_else(|| anyhow!("No schema key in config file"))?
            .to_string();
        Ok((host, schema))
    }

    async fn set_group_concat_max_len(&self, wiki: &str, conn: &mut my::Conn) -> Result<()> {
        if wiki == "commonswiki" {
            conn.exec_drop("SET SESSION group_concat_max_len = 1000000000", ())
                .await?;
        }
        Ok(())
    }

    #[instrument(skip(self), err)]
    pub async fn get_wiki_db_connection(&self, wiki: &str) -> Result<my::Conn> {
        let (wiki, cluster) = match wiki {
            "x3" => ("wikidatawiki", DatabaseCluster::X3),
            other => (other, DatabaseCluster::Default),
        };

        let mut pool = self.db_pool.lock().await;
        if pool.is_empty() {
            return Err(anyhow!("Database connection pool is empty"));
        }
        pool.rotate_left(1);
        let last = pool.len() - 1;
        let opts = match &cluster {
            DatabaseCluster::X3 => {
                self.get_mysql_opts_for_term_store(&pool[last].0, &pool[last].1)?
            }
            _ => self.get_mysql_opts_for_wiki(wiki, &pool[last].0, &pool[last].1, cluster)?,
        };

        trace!(user = opts.user());
        let mut conn;
        loop {
            conn = match my::Conn::new(opts.to_owned())
                .await
                .map_err(|e| format!("{e:?}"))
            {
                Ok(conn2) => conn2,
                Err(s) => {
                    // Checking if max_user_connections was exceeded. That should not happen but sometimes it does.
                    if s.contains("max_user_connections") {
                        // trace!(s);
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        continue;
                    }
                    return Err(anyhow!(s));
                }
            };
            self.set_group_concat_max_len(wiki, &mut conn).await?;
            break;
        }
        Ok(conn)
    }

    /// Connects to the X3 cluster TBD
    pub async fn get_x3_db_connection(&self) -> Result<my::Conn> {
        self.get_wiki_db_connection("x3").await
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

    pub async fn get_tool_db_connection(&self, tool_db_user_pass: DbUserPass) -> Result<my::Conn> {
        let (host, schema) = self.db_host_and_schema_for_tool_db()?;
        let (user, pass) = tool_db_user_pass.clone();
        let port: u16 = match self.config["host"].as_str() {
            Some("127.0.0.1") => 3308,
            Some(_host) => self.config["db_port"].as_u64().unwrap_or(3306) as u16,
            None => 3306, // Fallback
        };
        let opts = my::OptsBuilder::default()
            .ip_or_hostname(host.to_owned())
            .db_name(Some(schema))
            .user(Some(user))
            .pass(Some(pass))
            .tcp_port(port);

        match my::Conn::new(opts).await {
            Ok(conn) => Ok(conn),
            Err(e) => Err(anyhow!(
                "AppState::get_tool_db_connection can't get DB connection to {host}:{port} : '{e}'"
            )),
        }
    }

    pub const fn get_tool_db_user_pass(&self) -> &Arc<Mutex<DbUserPass>> {
        &self.tool_db_mutex
    }

    pub async fn get_query_from_psid(&self, psid: &str) -> Result<String> {
        let mut conn = self
            .get_tool_db_connection(self.tool_db_mutex.lock().await.clone())
            .await?;

        let psid = match psid.parse::<usize>() {
            Ok(psid) => psid,
            Err(e) => return Err(anyhow!(e)),
        };
        let sql = format!("SELECT querystring FROM query WHERE id={psid}");

        let rows = conn
            .exec_iter(sql.as_str(), ())
            .await
            .map_err(|e| anyhow!(e))?
            .map_and_drop(from_row::<Vec<u8>>)
            .await
            .map_err(|e| anyhow!(e))?;

        match rows.first() {
            Some(ret) => Ok(String::from_utf8_lossy(ret).into_owned()),
            None => Err(anyhow!("No such PSID in the database")),
        }
    }

    pub async fn log_query_start(&self, query_string: &str) -> Result<u64> {
        let utc: DateTime<Utc> = Utc::now();
        let now = utc.format("%Y-%m-%d %H:%M:%S").to_string();
        let sql = (
            "INSERT INTO `started_queries` (querystring,created,process_id) VALUES (?,?,?)"
                .to_string(),
            vec![
                MyValue::Bytes(query_string.to_owned().into()),
                MyValue::Bytes(now.into()),
                MyValue::UInt(std::process::id().into()),
            ],
        );

        let tool_db_user_pass = self.tool_db_mutex.lock().await;
        let mut conn = self
            .get_tool_db_connection(tool_db_user_pass.clone())
            .await?;
        conn.exec_drop(sql.0.as_str(), mysql_async::Params::Positional(sql.1))
            .await
            .map_err(|e| anyhow!(e))?;
        conn.last_insert_id()
            .ok_or_else(|| anyhow!("AppState::log_query_start: Could not insert"))
    }

    pub async fn log_query_end(&self, query_id: u64) -> Result<()> {
        let sql = (
            "DELETE FROM `started_queries` WHERE id=?",
            vec![MyValue::UInt(query_id)],
        );
        let tool_db_user_pass = self.tool_db_mutex.lock().await;
        self.get_tool_db_connection(tool_db_user_pass.clone())
            .await
            .map_err(|e| anyhow!(e))?
            .exec_drop(sql.0, mysql_async::Params::Positional(sql.1))
            .await
            .map_err(|e| anyhow!(e))
    }

    #[instrument(skip_all, ret)]
    pub async fn get_or_create_psid_for_query(&self, query_string: &str) -> Result<u64> {
        let tool_db_user_pass = self.tool_db_mutex.lock().await;
        let mut conn = self
            .get_tool_db_connection(tool_db_user_pass.clone())
            .await?;

        // Check for existing entry
        let sql1 = (
            "SELECT id FROM query WHERE querystring=? LIMIT 1",
            vec![MyValue::Bytes(query_string.to_owned().into())],
        );

        let rows = conn
            .exec_iter(sql1.0, mysql_async::Params::Positional(sql1.1))
            .await
            .map_err(|e| anyhow!(e))?
            .map_and_drop(from_row::<u64>)
            .await
            .map_err(|e| anyhow!(e))?;

        if let Some(id) = rows.first() {
            return Ok(*id);
        }

        // Create new entry
        let utc: DateTime<Utc> = Utc::now();
        let now = utc.format("%Y-%m-%d %H:%M:%S").to_string();
        let sql2 = (
            "INSERT IGNORE INTO `query` (querystring,created) VALUES (?,?)",
            vec![
                MyValue::Bytes(query_string.to_owned().into()),
                MyValue::Bytes(now.into()),
            ],
        );

        conn.exec_drop(sql2.0, mysql_async::Params::Positional(sql2.1))
            .await
            .map_err(|e| anyhow!(e))?;
        match conn.last_insert_id() {
            Some(id) => Ok(id),
            None => Err(anyhow!(
                "get_or_create_psid_for_query: Could not insert new PSID"
            )),
        }
    }

    pub fn try_shutdown(&self) {
        if !self.is_shutting_down() {
            return;
        }
        if let Ok(tr) = self.threads_running.read() {
            if *tr == 0 {
                ::std::process::exit(0);
            }
        }
    }

    pub fn modify_threads_running(&self, diff: i64) {
        if let Ok(mut tr) = self.threads_running.write() {
            *tr += diff;
        }
        self.try_shutdown();
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.read().map_or(true, |x| *x)
    }

    pub fn shut_down(&self) {
        if let Ok(mut sd) = self.shutting_down.write() {
            *sd = true;
        }
    }

    pub const fn site_matrix(&self) -> &SiteMatrix {
        &self.site_matrix
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    //use crate::app_state::AppState;
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
        get_new_state().await // TODO use static
                              /*
                              lazy_static! {
                                  static ref STATE: Arc<AppState> = get_new_state();
                              }
                              STATE.clone()
                              */
    }

    /// Build a minimal AppState-like config for unit tests that don't need DB
    fn make_minimal_config(host: &str) -> Value {
        serde_json::json!({
            "host": host,
            "user": "testuser",
            "password": "testpass",
            "schema": "test_schema",
            "db_port": 3306
        })
    }

    #[test]
    fn test_fix_wiki_name_be_tarask() {
        let config = make_minimal_config("127.0.0.1");
        let state = AppState {
            config: config.clone(),
            ..Default::default()
        };
        assert_eq!(state.fix_wiki_name("be-taraskwiki"), "be_x_oldwiki");
        assert_eq!(state.fix_wiki_name("be-x-oldwiki"), "be_x_oldwiki");
        assert_eq!(state.fix_wiki_name("be_taraskwiki"), "be_x_oldwiki");
        assert_eq!(state.fix_wiki_name("be_x_oldwiki"), "be_x_oldwiki");
    }

    #[test]
    fn test_fix_wiki_name_normal() {
        let state = AppState {
            config: make_minimal_config("127.0.0.1"),
            ..Default::default()
        };
        assert_eq!(state.fix_wiki_name("enwiki"), "enwiki");
        assert_eq!(state.fix_wiki_name("wikidatawiki"), "wikidatawiki");
        // Hyphens converted to underscores for non-special wikis
        assert_eq!(state.fix_wiki_name("zh-min-nanwiki"), "zh_min_nanwiki");
    }

    #[test]
    fn test_using_file_table() {
        let config_true = serde_json::json!({ "use_file_table": true });
        let state_true = AppState {
            config: config_true,
            ..Default::default()
        };
        assert!(state_true.using_file_table());

        let config_false = serde_json::json!({ "use_file_table": false });
        let state_false = AppState {
            config: config_false,
            ..Default::default()
        };
        assert!(!state_false.using_file_table());

        let config_missing = serde_json::json!({});
        let state_missing = AppState {
            config: config_missing,
            ..Default::default()
        };
        assert!(!state_missing.using_file_table());
    }

    #[test]
    fn test_using_new_categorylinks_table() {
        let config = serde_json::json!({ "use_new_categorylinks_table": true });
        let state = AppState {
            config,
            ..Default::default()
        };
        assert!(state.using_new_categorylinks_table());
    }

    #[test]
    fn test_get_restart_code() {
        let config = serde_json::json!({ "restart-code": "abc123" });
        let state = AppState {
            config,
            ..Default::default()
        };
        assert_eq!(state.get_restart_code(), Some("abc123"));

        let config2 = serde_json::json!({});
        let state2 = AppState {
            config: config2,
            ..Default::default()
        };
        assert_eq!(state2.get_restart_code(), None);
    }

    #[test]
    fn test_db_host_and_schema_for_wiki_local() {
        let config = make_minimal_config("127.0.0.1");
        let state = AppState {
            config,
            ..Default::default()
        };
        let (host, schema) = state
            .db_host_and_schema_for_wiki("enwiki", DatabaseCluster::Default)
            .unwrap();
        assert_eq!(host, "127.0.0.1");
        assert_eq!(schema, "enwiki_p");
    }

    #[test]
    fn test_db_host_and_schema_for_wiki_no_host() {
        let config = serde_json::json!({});
        let state = AppState {
            config,
            ..Default::default()
        };
        let result = state.db_host_and_schema_for_wiki("enwiki", DatabaseCluster::Default);
        assert!(result.is_err());
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
        let config = make_minimal_config("127.0.0.1");
        let state = AppState {
            config,
            ..Default::default()
        };
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
        let config = make_minimal_config("127.0.0.1");
        let state = AppState {
            config,
            ..Default::default()
        };
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

    #[tokio::test]
    async fn test_db_host_and_schema_for_wiki() {
        let state = get_state().await;
        assert_eq!(
            "enwiki_p".to_string(),
            state
                .db_host_and_schema_for_wiki("enwiki", DatabaseCluster::Default)
                .unwrap()
                .1
        );
        assert_eq!(
            "be_x_oldwiki_p".to_string(),
            state
                .db_host_and_schema_for_wiki("be-taraskwiki", DatabaseCluster::Default)
                .unwrap()
                .1
        );
    }
}
