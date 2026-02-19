use crate::pagelist::DatabaseCluster;
use anyhow::{Result, anyhow};
use chrono::prelude::*;
use mysql_async as my;
use mysql_async::Value as MyValue;
use mysql_async::from_row;
use mysql_async::prelude::Queryable;
use serde_json::Value;
use std::collections::HashMap;
use tracing::{instrument, trace};

/// The termstore host for the X3 / Wikidata term-store cluster.
/// This is a non-standard hostname that toolforge does not generate, so we
/// keep it as a constant and supply credentials separately.
const TERMSTORE_SERVER: &str = "termstore.wikidatawiki.analytics.db.svc.wikimedia.cloud";

// ---------------------------------------------------------------------------
// Credential source – toolforge (replica.my.cnf) or config.json fallback
// ---------------------------------------------------------------------------

/// A resolved user/password pair, obtained from either `~/replica.my.cnf`
/// (via the `toolforge` crate) or the legacy `config.json` fields.
#[derive(Debug, Clone)]
struct Credentials {
    user: String,
    password: String,
}

// ---------------------------------------------------------------------------
// DatabaseManager – owns all database-related state and logic
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct DatabaseManager {
    /// Full application config (used for feature flags, schema name, and the
    /// local-dev fallback credentials / port-mapping).
    config: Value,
    /// Port overrides for local SSH-tunnel testing.  Only populated when the
    /// config contains a `port_mapping` object.
    port_mapping: HashMap<String, u16>,
}

impl DatabaseManager {
    /// Initialise from the application config JSON value.
    ///
    /// On Toolforge, database credentials are supplied by `~/replica.my.cnf`
    /// (read on-demand by the `toolforge` crate).  When that file is absent
    /// (local development), the legacy `config["user"]` / `config["password"]`
    /// fields and `config["port_mapping"]` are used as a fallback so that
    /// existing SSH-tunnel workflows continue to work unchanged.
    pub fn new_from_config(config: &Value) -> Self {
        let port_mapping = config["port_mapping"]
            .as_object()
            .map(|x| x.to_owned())
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.as_i64().unwrap_or_default() as u16))
            .collect();

        Self {
            config: config.to_owned(),
            port_mapping,
        }
    }

    // ------------------------------------------------------------------
    // Test / minimal constructor
    // ------------------------------------------------------------------

    /// Create a [`DatabaseManager`] seeded with only a config value.
    /// Intended for unit tests that exercise config-derived logic without
    /// needing a real database connection.
    #[cfg(test)]
    pub(crate) fn with_config(config: Value) -> Self {
        Self {
            config,
            port_mapping: HashMap::new(),
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
    // Credential resolution
    // ------------------------------------------------------------------

    /// Resolve database credentials.
    ///
    /// Tries `~/replica.my.cnf` first (standard Toolforge setup).  When that
    /// file is absent – e.g. during local development – falls back to the
    /// `user` / `password` fields in the JSON config.
    fn credentials(&self) -> Result<Credentials> {
        // Attempt toolforge / replica.my.cnf first.
        if let Ok(info) = toolforge::connection_info!("enwiki") {
            return Ok(Credentials {
                user: info.user,
                password: info.password,
            });
        }

        // Fall back to config.json (local dev).
        let user = self.config["user"]
            .as_str()
            .ok_or_else(|| {
                anyhow!(
                    "No ~/replica.my.cnf found and no 'user' key in config – \
                     cannot resolve database credentials"
                )
            })?
            .to_string();
        let password = self.config["password"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        Ok(Credentials { user, password })
    }

    // ------------------------------------------------------------------
    // Server / schema name resolution (credential-free helpers)
    // ------------------------------------------------------------------

    pub fn fix_wiki_name(&self, wiki: &str) -> String {
        match wiki {
            "be-taraskwiki" | "be-x-oldwiki" | "be_taraskwiki" | "be_x_oldwiki" => "be_x_oldwiki",
            other => other,
        }
        .to_string()
        .replace('-', "_")
    }

    /// Returns the canonical Toolforge host and `_p`-suffixed database name
    /// for a wiki replica, as a `(host, schema)` tuple.
    ///
    /// For the default (WEB) cluster the host follows the standard Toolforge
    /// pattern `{wiki}.web.db.svc.wikimedia.cloud`.  For the X3 cluster the
    /// hard-coded termstore hostname is returned.
    ///
    /// This method is credential-free; use [`Self::get_wiki_db_connection`]
    /// when you need an actual connection.
    pub fn db_host_and_schema_for_wiki(
        &self,
        wiki: &str,
        cluster: DatabaseCluster,
    ) -> (String, String) {
        let wiki = self.fix_wiki_name(wiki);
        let host = match cluster {
            DatabaseCluster::X3 => TERMSTORE_SERVER.to_string(),
            _ => format!("{wiki}.web.db.svc.wikimedia.cloud"),
        };
        let schema = format!("{wiki}_p");
        (host, schema)
    }

    // ------------------------------------------------------------------
    // Connection helpers
    // ------------------------------------------------------------------

    /// Build [`my::Opts`] for a wiki-replica or termstore connection.
    ///
    /// On Toolforge, credentials come from `~/replica.my.cnf` (via the
    /// `toolforge` crate).  Locally they fall back to `config["user"]` /
    /// `config["password"]`, and the port is taken from `port_mapping` (for
    /// SSH-tunnel setups) or `config["db_port"]`.
    fn get_mysql_opts_for_wiki(&self, wiki: &str, cluster: DatabaseCluster) -> Result<my::Opts> {
        let creds = self.credentials()?;

        let (host, schema) = self.db_host_and_schema_for_wiki(wiki, cluster);

        // Port: prefer an explicit port_mapping entry (local SSH tunnels),
        // then fall back to config["db_port"], then the default 3306.
        let port_key = match cluster {
            DatabaseCluster::X3 => "x3",
            _ => wiki,
        };
        let port: u16 = self
            .port_mapping
            .get(port_key)
            .copied()
            .unwrap_or_else(|| self.config["db_port"].as_u64().unwrap_or(3306) as u16);

        // When running locally (host = 127.0.0.1 in config), always bind to
        // 127.0.0.1 regardless of what db_host_and_schema_for_wiki computed.
        let effective_host = if self.config["host"].as_str() == Some("127.0.0.1") {
            "127.0.0.1".to_string()
        } else {
            host
        };

        Ok(my::OptsBuilder::default()
            .ip_or_hostname(effective_host)
            .db_name(Some(schema))
            .user(Some(creds.user))
            .pass(Some(creds.password))
            .tcp_port(port)
            .into())
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

        let opts = self.get_mysql_opts_for_wiki(wiki, cluster)?;

        trace!(user = opts.user());
        let mut conn;
        loop {
            conn = match my::Conn::new(opts.to_owned())
                .await
                .map_err(|e| format!("{e:?}"))
            {
                Ok(conn2) => conn2,
                Err(s) => {
                    // Retry when the per-user connection limit is momentarily exceeded.
                    if s.contains("max_user_connections") {
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

    /// Connects to the X3 / Wikidata term-store cluster.
    pub async fn get_x3_db_connection(&self) -> Result<my::Conn> {
        self.get_wiki_db_connection("x3").await
    }

    /// Opens a connection to the tool database.
    ///
    /// The schema name is read from `config["schema"]`.  On Toolforge,
    /// credentials come from `~/replica.my.cnf` via `toolforge::db::toolsdb`;
    /// locally they fall back to `config["user"]` / `config["password"]` and
    /// the port is taken from `config["db_port"]` (defaulting to 3308 when
    /// the host is 127.0.0.1, matching the conventional local SSH-tunnel
    /// mapping).
    pub async fn get_tool_db_connection(&self) -> Result<my::Conn> {
        let schema = self.config["schema"]
            .as_str()
            .ok_or_else(|| anyhow!("No schema key in config file"))?
            .to_string();

        // Try toolforge (replica.my.cnf) first.
        let (host, user, password, port) = if let Ok(info) = toolforge::db::toolsdb(schema.clone())
        {
            let port = 3306_u16;
            (info.host, info.user, info.password, port)
        } else {
            // Local-dev fallback: use config.json credentials.
            let host = self.config["host"]
                .as_str()
                .ok_or_else(|| anyhow!("No host key in config file"))?
                .to_string();
            let user = self.config["user"]
                .as_str()
                .ok_or_else(|| anyhow!("No user key in config file"))?
                .to_string();
            let password = self.config["password"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let port: u16 = if host == "127.0.0.1" {
                // Conventional local SSH-tunnel port for tools-db.
                self.config["db_port"].as_u64().unwrap_or(3308) as u16
            } else {
                self.config["db_port"].as_u64().unwrap_or(3306) as u16
            };
            (host, user, password, port)
        };

        let opts = my::OptsBuilder::default()
            .ip_or_hostname(host.clone())
            .db_name(Some(schema))
            .user(Some(user))
            .pass(Some(password))
            .tcp_port(port);

        my::Conn::new(opts).await.map_err(|e| {
            anyhow!(
                "DatabaseManager::get_tool_db_connection cannot connect to {host}:{port}: '{e}'"
            )
        })
    }

    // ------------------------------------------------------------------
    // Tool-DB query helpers (PSID, query logging)
    // ------------------------------------------------------------------

    pub async fn get_query_from_psid(&self, psid: &str) -> Result<String> {
        let mut conn = self.get_tool_db_connection().await?;

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

        let mut conn = self.get_tool_db_connection().await?;
        conn.exec_drop(sql.0.as_str(), mysql_async::Params::Positional(sql.1))
            .await
            .map_err(|e| anyhow!(e))?;
        conn.last_insert_id()
            .ok_or_else(|| anyhow!("DatabaseManager::log_query_start: Could not insert"))
    }

    pub async fn log_query_end(&self, query_id: u64) -> Result<()> {
        let sql = (
            "DELETE FROM `started_queries` WHERE id=?",
            vec![MyValue::UInt(query_id)],
        );
        self.get_tool_db_connection()
            .await
            .map_err(|e| anyhow!(e))?
            .exec_drop(sql.0, mysql_async::Params::Positional(sql.1))
            .await
            .map_err(|e| anyhow!(e))
    }

    #[instrument(skip_all, ret)]
    pub async fn get_or_create_psid_for_query(&self, query_string: &str) -> Result<u64> {
        let mut conn = self.get_tool_db_connection().await?;

        // Check for existing entry.
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

        // Create new entry.
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
