use crate::pagelist::DatabaseCluster;
use anyhow::{Result, anyhow};
use chrono::prelude::*;
use mysql_async as my;
use mysql_async::Value as MyValue;
use mysql_async::from_row;
use mysql_async::prelude::Queryable;
use rand::seq::SliceRandom;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{instrument, trace};

const TERMSTORE_SERVER: &str = "termstore.wikidatawiki.analytics.db.svc.wikimedia.cloud";

pub type DbUserPass = (String, String);

// ---------------------------------------------------------------------------
// DatabaseManager – owns all database-related state and logic
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct DatabaseManager {
    db_pool: Arc<Mutex<Vec<DbUserPass>>>,
    config: Value,
    tool_db_mutex: Arc<Mutex<DbUserPass>>,
    port_mapping: HashMap<String, u16>, // Local testing only
}

impl DatabaseManager {
    /// Initialise from the application config JSON value.
    pub async fn new_from_config(config: &Value) -> Result<Self> {
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

        let ret = Self {
            db_pool: Arc::new(Mutex::new(vec![])),
            config: config.to_owned(),
            port_mapping,
            tool_db_mutex: Arc::new(Mutex::new(tool_db_access_tuple)),
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

    // ------------------------------------------------------------------
    // Test / minimal constructor
    // ------------------------------------------------------------------

    /// Create a [`DatabaseManager`] seeded with only a config value and default
    /// values for all other fields.  Intended for unit tests that exercise
    /// config-derived logic without needing a real database connection.
    #[cfg(test)]
    pub(crate) fn with_config(config: Value) -> Self {
        Self {
            config,
            ..Default::default()
        }
    }

    // ------------------------------------------------------------------
    // Config-based feature flags
    // ------------------------------------------------------------------

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

    // ------------------------------------------------------------------
    // Internal helpers: MySQL connection options
    // ------------------------------------------------------------------

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

    fn get_db_server_group(&self) -> &str {
        self.config["dbservergroup"]
            .as_str()
            .unwrap_or(".web.db.svc.eqiad.wmflabs")
    }

    // ------------------------------------------------------------------
    // Server / schema name resolution
    // ------------------------------------------------------------------

    pub fn fix_wiki_name(&self, wiki: &str) -> String {
        match wiki {
            "be-taraskwiki" | "be-x-oldwiki" | "be_taraskwiki" | "be_x_oldwiki" => "be_x_oldwiki",
            other => other,
        }
        .to_string()
        .replace('-', "_")
    }

    /// Returns the server and database name for the wiki, as a tuple.
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

    /// Returns the server and database name for the tool db, as a tuple.
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

    // ------------------------------------------------------------------
    // Connection pool
    // ------------------------------------------------------------------

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

    // ------------------------------------------------------------------
    // Tool-DB query helpers (PSID, query logging)
    // ------------------------------------------------------------------

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
}
