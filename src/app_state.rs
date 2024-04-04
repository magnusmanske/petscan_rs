use crate::form_parameters::FormParameters;
use crate::platform::{ContentType, MyResponse};
use chrono::prelude::*;
use mysql_async as my;
use mysql_async::from_row;
use mysql_async::prelude::Queryable;
use mysql_async::Value as MyValue;
use rand::prelude::thread_rng;
use rand::seq::SliceRandom;
use rayon::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;
use tracing::{instrument, trace};
use wikibase::mediawiki::api::Api;

pub type DbUserPass = (String, String);

#[derive(Debug, Clone, Default)]
pub struct AppState {
    db_pool: Arc<Mutex<Vec<DbUserPass>>>,
    config: Value,
    tool_db_mutex: Arc<Mutex<DbUserPass>>,
    threads_running: Arc<RwLock<i64>>,
    shutting_down: Arc<RwLock<bool>>,
    site_matrix: Value,
    main_page: String,
    port_mapping: HashMap<String, u16>, // Local testing only
}

impl AppState {
    pub async fn new_from_config(config: &Value) -> Self {
        let main_page_path = "./html/index.html";
        let tool_db_access_tuple = (
            config["user"]
                .as_str()
                .expect("No user key in config file")
                .to_string(),
            config["password"]
                .as_str()
                .expect("No password key in config file")
                .to_string(),
        );
        let port_mapping = config["port_mapping"]
            .as_object()
            .map(|x| x.to_owned())
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.as_i64().unwrap_or_default() as u16))
            .collect();
        let ret = Self {
            db_pool: Arc::new(Mutex::new(vec![])),
            config: config.to_owned(),
            port_mapping,
            threads_running: Arc::new(RwLock::new(0)),
            shutting_down: Arc::new(RwLock::new(false)),
            site_matrix: AppState::load_site_matrix().await,
            tool_db_mutex: Arc::new(Mutex::new(tool_db_access_tuple)),
            main_page: String::from_utf8_lossy(
                &fs::read(main_page_path).expect("Could not read index.html file form disk"),
            )
            .parse()
            .expect("Parsing index.html failed"),
        };

        if let Some(up_list) = config["mysql"].as_array() {
            let mut pool = ret.db_pool.lock().await;
            for up in up_list {
                let user = up[0]
                    .as_str()
                    .expect("Parsing user from mysql array in config failed")
                    .to_string();
                let pass = up[1]
                    .as_str()
                    .expect("Parsing pass from mysql array in config failed")
                    .to_string();
                let connections = up[2].as_u64().unwrap_or(5);
                // Ignore toolname up[3]

                for _num in 1..connections {
                    pool.push((user.to_string(), pass.to_string()));
                }
            }
            pool.shuffle(&mut thread_rng());
        }
        if ret.db_pool.lock().await.is_empty() {
            panic!("No database access config available");
        }
        ret
    }

    pub fn get_restart_code(&self) -> Option<&str> {
        self.config["restart-code"].as_str()
    }

    fn get_mysql_opts_for_wiki(
        &self,
        wiki: &str,
        user: &str,
        pass: &str,
    ) -> Result<my::Opts, String> {
        let (host, schema) = self.db_host_and_schema_for_wiki(wiki)?;
        let port: u16 = match self.port_mapping.get(wiki) {
            Some(port) => *port,
            None => self.config["db_port"].as_u64().unwrap_or(3306) as u16,
        };
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
        let direction = if self.is_language_rtl(&interface_language) {
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
        if self.is_language_rtl(&interface_language) {
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
    pub fn db_host_and_schema_for_wiki(&self, wiki: &str) -> Result<(String, String), String> {
        // TESTING
        /*
        ssh magnus@tools-login.wmflabs.org -L 3307:dewiki.web.db.svc.eqiad.wmflabs:3306 -N &
        ssh magnus@tools-login.wmflabs.org -L 3309:wikidatawiki.web.db.svc.eqiad.wmflabs:3306 -N &
        ssh magnus@tools-login.wmflabs.org -L 3305:commonswiki.web.db.svc.eqiad.wmflabs:3306 -N &
        ssh magnus@tools-login.wmflabs.org -L 3310:enwiki.web.db.svc.eqiad.wmflabs:3306 -N &
         */
        let wiki = self.fix_wiki_name(wiki);
        let host = match self.config["host"].as_str() {
            Some("127.0.0.1") => "127.0.0.1".to_string(),
            Some(_host) => wiki.to_owned() + self.get_db_server_group(),
            None => panic!("No host in config file"),
        };
        let schema = format!("{}_p", wiki);
        Ok((host, schema))
    }

    /// Returns the server and database name for the tool db, as a tuple
    pub fn db_host_and_schema_for_tool_db(&self) -> (String, String) {
        // TESTING
        // ssh magnus@tools-login.wmflabs.org -L 3308:tools-db:3306 -N &
        let host = self.config["host"]
            .as_str()
            .expect("No host key in config file")
            .to_string();
        let schema = self.config["schema"]
            .as_str()
            .expect("No schema key in config file")
            .to_string();
        (host, schema)
    }

    async fn set_group_concat_max_len(
        &self,
        wiki: &str,
        conn: &mut my::Conn,
    ) -> Result<(), String> {
        if wiki == "commonswiki" {
            conn.exec_drop("SET SESSION group_concat_max_len = 1000000000", ())
                .await
                .map_err(|e| format!("{:?}", e))?;
        }
        Ok(())
    }

    #[instrument(skip(self), err)]
    pub async fn get_wiki_db_connection(&self, wiki: &str) -> Result<my::Conn, String> {
        let mut pool = self.db_pool.lock().await;
        if pool.is_empty() {
            panic!("pool is empty");
        }
        pool.rotate_left(1);
        let last = pool.len() - 1;
        let opts = self.get_mysql_opts_for_wiki(wiki, &pool[last].0, &pool[last].1)?;
        trace!(user = opts.user());
        let mut conn;
        loop {
            conn = match my::Conn::new(opts.to_owned())
                .await
                .map_err(|e| format!("{:?}", e))
            {
                Ok(conn) => conn,
                Err(s) => {
                    // Checking if max_user_connections was exceeded. That should not happen but sometimes it does.
                    if s.contains("max_user_connections") {
                        // trace!(s);
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        continue;
                    }
                    return Err(s);
                }
            };
            self.set_group_concat_max_len(wiki, &mut conn).await?;
            break;
        }
        Ok(conn)
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
        match callback {
            Some(callback) => {
                let mut text = callback.to_owned();
                text += "(";
                text += &::serde_json::to_string(&value)
                    .expect("app_state::output_json can't stringify JSON [1]");
                text += ")";
                MyResponse {
                    s: text,
                    content_type: ContentType::JSONP,
                }
            }
            None => MyResponse {
                s: ::serde_json::to_string(&value)
                    .expect("app_state::output_json can't stringify JSON [2]"),
                content_type: ContentType::JSON,
            },
        }
    }

    pub async fn get_api_for_wiki(&self, wiki: String) -> Result<Api, String> {
        // TODO cache url and/or api object?
        let url = self.get_server_url_for_wiki(&wiki)? + "/w/api.php";
        match Api::new(&url).await {
            Ok(api) => Ok(api),
            Err(e) => Err(format!("{:?}", e)),
        }
    }

    fn get_value_from_site_matrix_entry(
        &self,
        value: &str,
        site: &Value,
        key_match: &str,
        key_return: &str,
    ) -> Option<String> {
        if site["closed"].as_str().is_some() {
            return None;
        }
        if site["private"].as_str().is_some() {
            return None;
        }
        match site[key_match].as_str() {
            Some(site_url) => {
                if value == site_url {
                    site[key_return].as_str().map(|url| url.to_string())
                } else {
                    None
                }
            }
            None => None,
        }
    }

    fn get_wiki_for_server_url_from_site(&self, url: &str, site: &Value) -> Option<String> {
        self.get_value_from_site_matrix_entry(url, site, "url", "dbname")
    }

    fn get_url_for_wiki_from_site(&self, wiki: &str, site: &Value) -> Option<String> {
        self.get_value_from_site_matrix_entry(wiki, site, "dbname", "url")
    }

    pub fn is_language_rtl(&self, language: &str) -> bool {
        self.site_matrix["sitematrix"]
            .as_object()
            .expect("AppState::get_wiki_for_server_url: sitematrix not an object")
            .iter()
            .any(
                |(_id, data)| match (data["code"].as_str(), data["dir"].as_str()) {
                    (Some(lang), Some("rtl")) => lang == language,
                    _ => false,
                },
            )
    }

    pub fn get_wiki_for_server_url(&self, url: &str) -> Option<String> {
        self.site_matrix["sitematrix"]
            .as_object()
            .expect("AppState::get_wiki_for_server_url: sitematrix not an object")
            .iter()
            .filter_map(|(id, data)| match id.as_str() {
                "count" => None,
                "specials" => data
                    .as_array()
                    .expect("AppState::get_wiki_for_server_url: 'specials' is not an array")
                    .iter()
                    .filter_map(|site| self.get_wiki_for_server_url_from_site(url, site))
                    .next(),
                _other => match data["site"].as_array() {
                    Some(sites) => sites
                        .iter()
                        .filter_map(|site| self.get_wiki_for_server_url_from_site(url, site))
                        .next(),
                    None => None,
                },
            })
            .next()
    }

    pub fn get_server_url_for_wiki(&self, wiki: &str) -> Result<String, String> {
        match wiki.replace('_', "-").as_str() {
            "be-taraskwiki" | "be-x-oldwiki" => {
                return Ok("https://be-tarask.wikipedia.org".to_string())
            }
            _ => {}
        }
        self.site_matrix["sitematrix"]
            .as_object()
            .expect("AppState::get_server_url_for_wiki: sitematrix not an object")
            .iter()
            .filter_map(|(id, data)| match id.as_str() {
                "count" => None,
                "specials" => data
                    .as_array()
                    .expect("AppState::get_server_url_for_wiki: 'specials' is not an array")
                    .iter()
                    .filter_map(|site| self.get_url_for_wiki_from_site(wiki, site))
                    .next(),
                _other => match data["site"].as_array() {
                    Some(sites) => sites
                        .iter()
                        .filter_map(|site| self.get_url_for_wiki_from_site(wiki, site))
                        .next(),
                    None => None,
                },
            })
            .next()
            .ok_or(format!(
                "AppState::get_server_url_for_wiki: Cannot find server for wiki '{}'",
                &wiki
            ))
    }

    pub async fn get_tool_db_connection(
        &self,
        tool_db_user_pass: DbUserPass,
    ) -> Result<my::Conn, String> {
        let (host, schema) = self.db_host_and_schema_for_tool_db();
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
            Err(e) => Err(format!(
                "AppState::get_tool_db_connection can't get DB connection to {}:{} : '{}'",
                &host, port, &e
            )),
        }
    }

    pub fn get_tool_db_user_pass(&self) -> &Arc<Mutex<DbUserPass>> {
        &self.tool_db_mutex
    }

    pub async fn get_query_from_psid(&self, psid: &str) -> Result<String, String> {
        let mut conn = self
            .get_tool_db_connection(self.tool_db_mutex.lock().await.clone())
            .await?;

        let psid = match psid.parse::<usize>() {
            Ok(psid) => psid,
            Err(e) => return Err(format!("{:?}", e)),
        };
        let sql = format!("SELECT querystring FROM query WHERE id={}", psid);

        let rows = conn
            .exec_iter(sql.as_str(), ())
            .await
            .map_err(|e| format!("{:?}", e))?
            .map_and_drop(from_row::<Vec<u8>>)
            .await
            .map_err(|e| format!("{:?}", e))?;

        match rows.first() {
            Some(ret) => Ok(String::from_utf8_lossy(ret).into_owned()),
            None => Err("No such PSID in the database".to_string()),
        }
    }

    pub async fn log_query_start(&self, query_string: &str) -> Result<u64, String> {
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
            .map_err(|e| format!("{:?}", e))?;
        conn.last_insert_id()
            .ok_or_else(|| "AppState::log_query_start: Could not insert".to_string())
    }

    pub async fn log_query_end(&self, query_id: u64) -> Result<(), String> {
        let sql = (
            "DELETE FROM `started_queries` WHERE id=?",
            vec![MyValue::UInt(query_id)],
        );
        let tool_db_user_pass = self.tool_db_mutex.lock().await;
        self.get_tool_db_connection(tool_db_user_pass.clone())
            .await
            .map_err(|e| format!("{:?}", e))?
            .exec_drop(sql.0, mysql_async::Params::Positional(sql.1))
            .await
            .map_err(|e| format!("{:?}", e))
    }

    #[instrument(skip_all, ret)]
    pub async fn get_or_create_psid_for_query(&self, query_string: &str) -> Result<u64, String> {
        let tool_db_user_pass = self.tool_db_mutex.lock().await;
        let mut conn = self
            .get_tool_db_connection(tool_db_user_pass.clone())
            .await?;

        // Check for existing entry
        let sql = (
            "SELECT id FROM query WHERE querystring=? LIMIT 1",
            vec![MyValue::Bytes(query_string.to_owned().into())],
        );

        let rows = conn
            .exec_iter(sql.0, mysql_async::Params::Positional(sql.1))
            .await
            .map_err(|e| format!("{:?}", e))?
            .map_and_drop(from_row::<u64>)
            .await
            .map_err(|e| format!("{:?}", e))?;

        if let Some(id) = rows.first() {
            return Ok(*id);
        }

        // Create new entry
        let utc: DateTime<Utc> = Utc::now();
        let now = utc.format("%Y-%m-%d %H:%M:%S").to_string();
        let sql = (
            "INSERT IGNORE INTO `query` (querystring,created) VALUES (?,?)",
            vec![
                MyValue::Bytes(query_string.to_owned().into()),
                MyValue::Bytes(now.into()),
            ],
        );

        conn.exec_drop(sql.0, mysql_async::Params::Positional(sql.1))
            .await
            .map_err(|e| format!("{:?}", e))?;
        match conn.last_insert_id() {
            Some(id) => Ok(id),
            None => Err("get_or_create_psid_for_query: Could not insert new PSID".to_string()),
        }
    }

    async fn load_site_matrix() -> Value {
        let api = Api::new("https://www.wikidata.org/w/api.php")
            .await
            .expect("Can't talk to Wikidata API");
        let params: HashMap<String, String> = vec![("action", "sitematrix")]
            .par_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        api.get_query_api_json(&params)
            .await
            .expect("Can't run action=sitematrix on Wikidata API")
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
            *tr += diff
        }
        self.try_shutdown()
    }

    pub fn is_shutting_down(&self) -> bool {
        match self.shutting_down.read() {
            Ok(x) => *x,
            _ => true,
        }
    }

    pub fn shut_down(&self) {
        if let Ok(mut sd) = self.shutting_down.write() {
            *sd = true;
        }
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
        Arc::new(AppState::new_from_config(&petscan_config).await)
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

    #[tokio::test]
    async fn test_get_wiki_for_server_url() {
        let state = get_state().await;
        assert_eq!(
            state.get_wiki_for_server_url(&"https://am.wiktionary.org".to_string()),
            Some("amwiktionary".to_string())
        );
        assert_eq!(
            state.get_wiki_for_server_url(&"https://outreach.wikimedia.org".to_string()),
            Some("outreachwiki".to_string())
        );
    }

    #[tokio::test]
    async fn test_db_host_and_schema_for_wiki() {
        let state = get_state().await;
        assert_eq!(
            "enwiki_p".to_string(),
            state
                .db_host_and_schema_for_wiki(&"enwiki".to_string())
                .unwrap()
                .1
        );
        assert_eq!(
            "be_x_oldwiki_p".to_string(),
            state
                .db_host_and_schema_for_wiki(&"be-taraskwiki".to_string())
                .unwrap()
                .1
        );
    }

    #[tokio::test]
    async fn is_language_rtl() {
        let state = get_state().await;
        assert!(!state.is_language_rtl("en"));
        assert!(state.is_language_rtl("ar"));
        assert!(!state.is_language_rtl("de"));
        assert!(state.is_language_rtl("he"));
    }
}
