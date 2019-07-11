extern crate rocket;

use mediawiki::api::Api;
use mysql as my;
use rand::seq::SliceRandom;
use rayon::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::{thread, time};

static MAX_CONCURRENT_DB_CONNECTIONS: u64 = 10;
static MYSQL_MAX_CONNECTION_ATTEMPTS: u64 = 15;
static MYSQL_CONNECTION_INITIAL_DELAY_MS: u64 = 100;

#[derive(Debug, Clone)]
pub struct AppState {
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
        for _x in 1..MAX_CONCURRENT_DB_CONNECTIONS {
            ret.db_pool.push(Arc::new(Mutex::new(0)));
        }
        ret
    }

    /// Returns the server and database name for the wiki, as a tuple
    pub fn db_host_and_schema_for_wiki(wiki: &String) -> (String, String) {
        // TESTING
        // ssh magnus@tools-login.wmflabs.org -L 3307:wikidatawiki.analytics.db.svc.eqiad.wmflabs:3306 -N
        let host = "127.0.0.1".to_string(); // TESTING wiki.to_owned() + ".analytics.db.svc.eqiad.wmflabs";
        let schema = wiki.to_owned() + "_p";
        (host, schema)
    }

    /// Returns a random mutex. The mutex value itself is just a placeholder!
    pub fn get_db_mutex(&self) -> &Arc<Mutex<u8>> {
        &self.db_pool.choose(&mut rand::thread_rng()).unwrap()
    }

    /// Returns a random user/password tuple for database access, from the array of arrays in config["mysql"].
    /// Falls back on the default tool user/password.
    fn get_random_db_user(&self) -> (String, String) {
        match self.config["mysql"].as_array() {
            Some(a) => {
                let up = a
                    .choose(&mut rand::thread_rng())
                    .unwrap()
                    .as_array()
                    .unwrap();
                (
                    up[0].as_str().unwrap().to_string(),
                    up[1].as_str().unwrap().to_string(),
                )
            }
            None => (
                self.config["user"].as_str().unwrap().to_string(),
                self.config["password"].as_str().unwrap().to_string(),
            ),
        }
    }

    pub fn get_wiki_db_connection(&self, wiki: &String) -> Option<my::Conn> {
        let mut loops_left = MYSQL_MAX_CONNECTION_ATTEMPTS;
        let mut milliseconds = MYSQL_CONNECTION_INITIAL_DELAY_MS;
        loop {
            let (host, schema) = AppState::db_host_and_schema_for_wiki(wiki);
            let (user, pass) = self.get_random_db_user();
            let mut builder = my::OptsBuilder::new();
            builder
                .ip_or_hostname(Some(host))
                .db_name(Some(schema))
                .user(Some(user))
                .pass(Some(pass));
            builder.tcp_port(self.config["db_port"].as_u64().unwrap_or(3306) as u16);

            match my::Conn::new(builder) {
                Ok(con) => return Some(con),
                Err(e) => {
                    println!("CONNECTION ERROR: {:?}", e);
                    if loops_left == 0 {
                        break;
                    }
                    loops_left -= 1;
                    let sleep_ms = time::Duration::from_millis(milliseconds);
                    milliseconds *= 2;
                    thread::sleep(sleep_ms);
                }
            }
        }
        None
    }

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
