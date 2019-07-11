extern crate rocket;

use mediawiki::api::Api;
use mysql as my;
use rayon::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub struct AppState {
    //pub db: Arc<my::Pool>,
    pub db_pool: Vec<Arc<Mutex<u8>>>,
    pub config: Value,
    threads_running: Arc<Mutex<i64>>,
    shutting_down: Arc<Mutex<bool>>,
    site_matrix: Value,
}

impl AppState {
    pub fn new_from_config(config: &Value) -> Self {
        let mut ret = Self {
            //db: Arc::new(AppState::db_pool_from_config(config)),
            db_pool: vec![],
            config: config.to_owned(),
            threads_running: Arc::new(Mutex::new(0)),
            shutting_down: Arc::new(Mutex::new(false)),
            site_matrix: AppState::load_site_matrix(),
        };
        for _x in 1..5 {
            ret.db_pool.push(Arc::new(Mutex::new(0)));
        }
        ret
    }

    pub fn db_host_and_schema_for_wiki(wiki: &String) -> (String, String) {
        // TESTING
        // ssh magnus@tools-login.wmflabs.org -L 3307:wikidatawiki.analytics.db.svc.eqiad.wmflabs:3306 -N
        let host = "127.0.0.1".to_string(); // TESTING wiki.to_owned() + ".analytics.db.svc.eqiad.wmflabs";
        let schema = wiki.to_owned() + "_p";
        (host, schema)
    }

    pub fn get_db_mutex(&self) -> &Arc<Mutex<u8>> {
        &self.db_pool[0]
    }

    pub fn get_wiki_db_connection(&self, wiki: &String) -> Option<my::Conn> {
        let (host, schema) = AppState::db_host_and_schema_for_wiki(wiki);
        let mut builder = my::OptsBuilder::new();
        builder
            .ip_or_hostname(Some(host))
            .db_name(Some(schema))
            .user(self.config["user"].as_str())
            .pass(self.config["password"].as_str());
        builder.tcp_port(self.config["db_port"].as_u64().unwrap_or(3306) as u16);
        my::Conn::new(builder).ok()
    }

    /*
    pub fn run_sql_on_wiki(&self, wiki: &String, sql: &String) -> Result<QueryResult> {
        let (host, schema) = self.db_host_and_schema_for_wiki(wiki);
        let mut builder = my::OptsBuilder::new();
        builder
            .ip_or_hostname(Some(host))
            .db_name(Some(schema))
            .user(self.config["user"].as_str())
            .pass(self.config["password"].as_str());
        builder.tcp_port(self.config["db_port"].as_u64().unwrap_or(3306) as u16);

        let mut connection = my::Conn::new(builder)?;
        let result = connection.query(sql)?;
        Ok(result)
    }
    */

    pub fn get_api_for_wiki(&self, wiki: String) -> Option<Api> {
        // TODO cache url and/or api object?
        let url = self.get_server_url_for_wiki(&wiki)? + "/w/api.php";
        Api::new(&url).ok()
    }

    fn get_url_for_wiki_from_site(&self, wiki: &String, site: &Value) -> Option<String> {
        if site["closed"].as_str().is_some() {
            return None;
        }
        if site["private"].as_str().is_some() {
            return None;
        }
        match site["dbname"].as_str() {
            Some(dbname) => {
                if wiki == dbname {
                    match site["url"].as_str() {
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

    pub fn get_server_url_for_wiki(&self, wiki: &String) -> Option<String> {
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
    }

    fn load_site_matrix() -> Value {
        let api =
            Api::new("https://www.wikidata.org/w/api.php").expect("Can't talk to Wikidata API");
        let params: HashMap<String, String> = vec![("action", "sitematrix")]
            .par_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        api.get_query_api_json(&params)
            .expect("Can't run action=sitematrix on Wikidata API")
    }

    /*
    fn db_pool_from_config(config: &Value) -> my::Pool {
        let (host, schema) = Self::db_host_and_schema_for_wiki(&"wikidatawiki".to_string());
        let mut builder = my::OptsBuilder::new();
        builder
            .ip_or_hostname(Some(host))
            .db_name(Some(schema))
            .user(config["user"].as_str())
            .pass(config["password"].as_str());
        builder.tcp_port(config["db_port"].as_u64().unwrap_or(3306) as u16);

        // Min 1, max 7 connections
        match my::Pool::new_manual(1, 7, builder) {
            Ok(pool) => pool,
            Err(e) => panic!("Could not initialize DB connection pool: {}", &e),
        }
    }
    */

    pub fn modify_threads_running(&self, diff: i64) {
        let mut threads_running = self.threads_running.lock().unwrap();
        *threads_running += diff;
        if self.is_shutting_down() && *threads_running == 0 {
            panic!("Planned shutdown")
        }
    }

    pub fn is_shutting_down(&self) -> bool {
        *self.shutting_down.lock().unwrap()
    }
}
