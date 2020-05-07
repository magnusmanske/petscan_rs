use tokio::sync::Mutex;
use crate::form_parameters::FormParameters;
use crate::platform::{ContentType, MyResponse};
use chrono::prelude::*;
use mysql_async::prelude::Queryable;
use mysql_async::from_row;
use mysql_async as my;
use mysql_async::Value as MyValue;
use rayon::prelude::*;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, RwLock};
use wikibase::mediawiki::api::Api;

/*
static MAX_CONCURRENT_DB_CONNECTIONS: u64 = 10;
static MYSQL_MAX_CONNECTION_ATTEMPTS: u64 = 15;
static MYSQL_CONNECTION_INITIAL_DELAY_MS: u64 = 100;
static MYSQL_CONNECTION_MAX_DELAY_MS: u64 = 5000;
*/

pub type DbUserPass = (String, String);

#[derive(Debug, Clone)]
pub struct AppState {
    db_pools:Vec<my::Pool>,
    pub config: Value,
    tool_db_mutex: Arc<Mutex<DbUserPass>>,
    threads_running: Arc<RwLock<i64>>,
    shutting_down: Arc<RwLock<bool>>,
    site_matrix: Value,
    main_page: String,
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
        let mut ret = Self {
            db_pools : vec![],
            config: config.to_owned(),
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

        match config["mysql"].as_array() {
            Some(up_list) => {
                up_list.iter().for_each(|up| {
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

                    let ( host , schema ) = ret.db_host_and_schema_for_wiki(&"wikidatawiki".to_string()).unwrap();
                    let pool_opts = my::PoolOpts::default()
                        .with_constraints(my::PoolConstraints::new(1, connections as usize).unwrap())
                        . with_inactive_connection_ttl( core::time::Duration::new(120, 0));
                    let opts = my::OptsBuilder::default()
                        .ip_or_hostname(host.to_owned())
                        .db_name(Some(schema.to_owned()))
                        .pool_opts(pool_opts)
                        .user(Some(user))
                        .pass(Some(pass))
                        .tcp_port(config["db_port"].as_u64().unwrap_or(3306) as u16);
                    let pool = my::Pool::new(opts);
                    ret.db_pools.push ( pool ) ;
                });
            }
            None => {
                /*
                for _x in 1..MAX_CONCURRENT_DB_CONNECTIONS {
                    let tuple = (
                        config["user"]
                            .as_str()
                            .expect("No user key in config file")
                            .to_string(),
                        config["password"]
                            .as_str()
                            .expect("No password key in config file")
                            .to_string(),
                    );
                    ret.db_pool.push(Arc::new(Mutex::new(tuple)));
                }
                */
            }
        }
        if ret.db_pools.is_empty() {
            panic!("No database access config available");
        }
        ret
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
            interface_language.replace("'", "")
        );
        self.main_page.replace("<html>", &h).to_string()
    }

    fn get_db_server_group(&self) -> &str {
        match self.config["dbservergroup"].as_str() {
            Some(s) => s,
            None => ".web.db.svc.eqiad.wmflabs", // ".analytics.db.svc.eqiad.wmflabs"
        }
    }

    /// Returns the server and database name for the wiki, as a tuple
    pub fn db_host_and_schema_for_wiki(&self, wiki: &String) -> Result<(String, String), String> {
        // TESTING
        // ssh magnus@tools-login.wmflabs.org -L 3307:wikidatawiki.web.db.svc.eqiad.wmflabs:3306 -N
        lazy_static! {
            static ref REMOVE_WIKI: Regex = Regex::new(r"wiki$")
                .expect("AppState::get_url_for_wiki_from_site: Regex is invalid");
        }

        let wiki = match wiki.as_str() {
            "be-taraskwiki" | "be-x-oldwiki" | "be_taraskwiki" | "be_x_oldwiki" => "be_x_oldwiki",
            other => other,
        }
        .to_string();

        let host = match self.config["host"].as_str() {
            Some("127.0.0.1") => "127.0.0.1".to_string(),
            Some(_host) => wiki.to_owned() + self.get_db_server_group(),
            None => panic!("No host in config file"),
        };
        let schema = wiki.to_string() + "_p";
        Ok((host, schema))
    }

    /// Returns the server and database name for the tool db, as a tuple
    pub fn db_host_and_schema_for_tool_db(&self) -> (String, String) {
        // TESTING
        // ssh magnus@tools-login.wmflabs.org -L 3308:tools-db:3306 -N
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

    async fn set_group_concat_max_len(&self, wiki: &String, conn: &mut my::Conn) -> Result<(), String> {
        if wiki != "commonswiki" {
            return Ok(()); // Only needed for commonswiki, in platform::process_files
        }
        conn.exec_drop("SET SESSION group_concat_max_len = 1000000000",()).await.map_err(|e|format!("{:?}",e))?;
        Ok(())
    }

    pub async fn get_wiki_db_connection(
        &self,
        wiki: &String,
    ) -> Result<my::Conn, String> {
        loop {
            let pool_id = rand::random::<usize>() % self.db_pools.len() ;
            match self.db_pools.get(pool_id) {
                Some(pool) => {
                    println!("get_wiki_db_connection: 1 [{}/{}]",&wiki,pool_id);
                    match pool.get_conn().await {
                        Ok(mut conn) => {
                            println!("get_wiki_db_connection: 2");
                            let (_host, schema) = self.db_host_and_schema_for_wiki(wiki)?;
                            println!("get_wiki_db_connection: 3");
                            conn.query_drop("USE ".to_owned()+&schema).await.map_err(|e|format!("{:?}",e))?;
                            println!("get_wiki_db_connection: 4");
                            self.set_group_concat_max_len(wiki,&mut conn).await?;
                            println!("get_wiki_db_connection: 5");
                            return Ok(conn) ;
                        }
                        Err(_e) => { println!("AGAIN A"); }
                    }
                }
                None => {
                    println!("AGAIN B");
                }
            }
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
                    .unwrap_or("en".to_string());
                let html = self
                    .get_main_page(interface_language.to_string())
                    .to_owned();
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
                s: error.to_string(),
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
        value: &String,
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
                    match site[key_return].as_str() {
                        Some(url) => Some(url.to_string()),
                        None => None,
                    }
                } else {
                    None
                }
            }
            None => None,
        }
    }

    fn get_wiki_for_server_url_from_site(&self, url: &String, site: &Value) -> Option<String> {
        self.get_value_from_site_matrix_entry(url, site, "url", "dbname")
    }

    fn get_url_for_wiki_from_site(&self, wiki: &String, site: &Value) -> Option<String> {
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

    pub fn get_wiki_for_server_url(&self, url: &String) -> Option<String> {
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

    pub fn get_server_url_for_wiki(&self, wiki: &String) -> Result<String, String> {
        match wiki.replace("_", "-").as_str() {
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

    pub async fn get_query_from_psid(&self, psid: &String) -> Result<String, String> {
        let tool_db_user_pass = self.tool_db_mutex.lock().await;
        let mut conn = self.get_tool_db_connection(tool_db_user_pass.clone()).await?;

        let psid = match psid.parse::<usize>() {
            Ok(psid) => psid,
            Err(e) => return Err(format!("{:?}", e)),
        };
        let sql = format!("SELECT querystring FROM query WHERE id={}", psid);

        let rows = conn.exec_iter(sql.as_str(),()).await
            .map_err(|e|format!("{:?}",e))?
            .map_and_drop(|row| from_row::<Vec<u8>>(row))
            .await
            .map_err(|e|format!("{:?}",e))?;

        match rows.get(0) {
            Some(ret) => Ok(String::from_utf8_lossy(&ret).into_owned()),
            None => Err("No such PSID in the database".to_string()),
        }
    }

    pub async fn log_query_start(&self, query_string: &String) -> Result<u64, String> {
        let tool_db_user_pass = self.tool_db_mutex.lock().await;
        let mut conn = self.get_tool_db_connection(tool_db_user_pass.clone()).await?;
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


        conn.exec_drop(sql.0.as_str(),mysql_async::Params::Positional(sql.1)).await.map_err(|e|format!("{:?}",e))?;

        match conn.last_insert_id() {
            Some(id) => Ok(id),
            None => Err(format!("AppState::log_query_start: Could not insert"))
        }
    }

    pub async fn log_query_end(&self, query_id: u64) -> Result<(),String> {
        let sql = (
            "DELETE FROM `started_queries` WHERE id=?",
            vec![MyValue::UInt(query_id)],
        );
        let tool_db_user_pass = self.tool_db_mutex.lock().await;
        self
            .get_tool_db_connection(tool_db_user_pass.clone())
            .await
            .map_err(|e|format!("{:?}",e))?
            .exec_drop(sql.0,mysql_async::Params::Positional(sql.1))
            .await
            .map_err(|e|format!("{:?}",e))
    }

    pub async fn get_or_create_psid_for_query(&self, query_string: &String) -> Result<u64, String> {
        let tool_db_user_pass = self.tool_db_mutex.lock().await;
        let mut conn = self.get_tool_db_connection(tool_db_user_pass.clone()).await?;

        // Check for existing entry
        let sql = (
            "SELECT id FROM query WHERE querystring=? LIMIT 1",
            vec![MyValue::Bytes(query_string.to_owned().into())],
        );

        let rows = conn.exec_iter(sql.0,mysql_async::Params::Positional(sql.1)).await
            .map_err(|e|format!("{:?}",e))?
            .map_and_drop(|row| from_row::<u64>(row))
            .await
            .map_err(|e|format!("{:?}",e))?;

        for id in rows {
            return Ok(id);
        }

        // Create new entry
        let utc: DateTime<Utc> = Utc::now();
        let now = utc.format("%Y-%m-%d %H:%M:%S").to_string();
        let sql = (
            "INSERT IGNORE INTO `query` (querystring,created) VALUES (?,?)",
            vec![MyValue::Bytes(query_string.to_owned().into()), MyValue::Bytes(now.into())],
        );

        conn.exec_drop(sql.0,mysql_async::Params::Positional(sql.1)).await.map_err(|e|format!("{:?}",e))?;
        match conn.last_insert_id() {
            Some(id) => Ok(id),
            None => Err(format!("get_or_create_psid_for_query: Could not insert new PSID"))
        }
    }

    async fn load_site_matrix() -> Value {
        let api =
            Api::new("https://www.wikidata.org/w/api.php").await.expect("Can't talk to Wikidata API");
        let params: HashMap<String, String> = vec![("action", "sitematrix")]
            .par_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        api.get_query_api_json(&params).await
            .expect("Can't run action=sitematrix on Wikidata API")
    }

    pub fn try_shutdown(&self) {
        if self.is_shutting_down() && *self.threads_running.read().unwrap() == 0 {
            ::std::process::exit(0);
        }
    }

    pub fn modify_threads_running(&self, diff: i64) {
        *self.threads_running.write().unwrap() += diff;
        self.try_shutdown()
    }

    pub fn is_shutting_down(&self) -> bool {
        *self.shutting_down.read().unwrap()
    }

    pub fn shut_down(&self) {
        *self.shutting_down.write().unwrap() = true;
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
