use crate::config::Config;
use anyhow::{Result, anyhow};
use chrono::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tracing::{instrument, trace};
use wikimisc::mysql_async;
use wikimisc::mysql_async as my;
use wikimisc::mysql_async::Value as MyValue;
use wikimisc::mysql_async::from_row;
use wikimisc::mysql_async::prelude::Queryable;
use wikimisc::toolforge;
use wikimisc::toolforge::db::{Cluster, DBConnectionInfo, ExtensionDb, normalize_dbname};
use wikimisc::toolforge::pool::CustomPool;

/// The DNS suffix shared by every Wiki Replica service. Only needed for
/// credentials from config.json; everywhere else `toolforge` names the host.
const REPLICA_DOMAIN: &str = ".db.svc.wikimedia.cloud";

/// Where the local SSH tunnels listen. A config `host` of this value routes
/// every connection through the tunnels in `port_mapping`.
const LOCALHOST: &str = "127.0.0.1";

/// Retry policy for `max_user_connections` rejections. Wikimedia replicas
/// enforce a per-user limit (typically 10 concurrent connections); a brief
/// backoff lets other in-flight requests release their connection.
const MAX_RETRY_ATTEMPTS: u32 = 10;
const BASE_DELAY_MS: u64 = 100;
const MAX_DELAY_MS: u64 = 5000;

/// Hard upper bound on leasing a pooled connection, including waiting for a
/// free one. Wikimedia replicas should always answer the TCP+auth handshake
/// within a few seconds; anything longer means the replica is unreachable and
/// we should fail fast rather than pin a worker.
const DB_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// TCP keepalive interval. Surfaces an OS-level error on dead sockets so a
/// dropped connection cannot leave an `exec_*` call blocked forever.
/// (True per-query read timeouts need `tokio::time::timeout` at each call
/// site and belong with the request-level wall-clock budget — see P1 #7.)
const TCP_KEEPALIVE: std::time::Duration = std::time::Duration::from_secs(30);

/// Maximum live connections held in any one per-wiki pool. Wikimedia
/// replicas enforce ~10 connections per user; 5 leaves headroom for
/// concurrent requests that hit the same wiki.
const WIKI_POOL_MAX: usize = 5;
const TOOL_DB_POOL_MAX: usize = 5;
/// Idle pool connections older than this are closed and recreated. Avoids
/// retaining sockets the upstream has silently dropped.
const POOL_INACTIVE_TTL: std::time::Duration = std::time::Duration::from_secs(300);
/// A per-wiki pool unused for this long is closed. Each pool keeps two
/// background tasks alive for as long as it exists, so without this the
/// task count grew with every wiki ever queried. Longer than
/// [`POOL_INACTIVE_TTL`], so a closed pool never had an idle connection left
/// to lose.
const POOL_EVICT_AFTER: std::time::Duration = std::time::Duration::from_secs(600);

/// The replica database a query runs on: a wiki's core database, or one of
/// the extension databases holding tables split off from it.
///
/// Which tables live where, and which wikis have which extension database,
/// is `toolforge`'s [`ExtensionDb`]'s business; this only names the clusters
/// PetScan routes queries to, so they can key pools and group clauses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum DbCluster {
    /// The wiki's core database, holding every table not split off elsewhere.
    #[default]
    Core,
    /// The Commons links tables, split off in September 2026.
    Links,
    /// The Wikidata term store, i.e. the `wbt_*` tables (the legacy `x3`).
    TermStore,
}

impl DbCluster {
    const fn extension(self) -> Option<ExtensionDb> {
        match self {
            Self::Core => None,
            Self::Links => Some(ExtensionDb::Links),
            Self::TermStore => Some(ExtensionDb::TermStore),
        }
    }

    fn from_extension(extension: Option<ExtensionDb>) -> Result<Self> {
        match extension {
            None => Ok(Self::Core),
            Some(ExtensionDb::Links) => Ok(Self::Links),
            Some(ExtensionDb::TermStore) => Ok(Self::TermStore),
            Some(other) => Err(anyhow!("Unsupported extension database '{other}'")),
        }
    }

    /// Which replica service group serves this cluster.
    ///
    /// Everything uses `web`, whose short query timeout suits a user waiting
    /// for a result. The Wikidata term store is the exception: PetScan has
    /// always read it from the `analytics` host, whose longer timeout the
    /// label queries need.
    const fn server_group(self) -> Cluster {
        match self {
            Self::TermStore => Cluster::Analytics,
            _ => Cluster::Web,
        }
    }

    /// A table only this cluster's extension database has (none for
    /// [`Self::Core`]). `toolforge` picks the database to connect to by the
    /// tables a query reads, so this is how to ask it for this cluster.
    const fn routing_tables(self) -> &'static [&'static str] {
        match self {
            Self::Core => &[],
            Self::Links => &["linktarget"],
            Self::TermStore => &["wbt_text"],
        }
    }

    /// The leading labels of the replica host serving this cluster of `wiki`
    /// (e.g. `links.commonswiki`), which is also how `toolforge` and
    /// `port_mapping` name its SSH tunnel.
    fn tunnel_target(self, wiki: &str) -> String {
        match self.extension() {
            Some(extension) => format!("{extension}.{wiki}"),
            None => wiki.to_string(),
        }
    }
}

/// A database server and the credentials to log in with. Deliberately not
/// `Debug`, so the password cannot end up in a log.
struct Server {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
}

impl From<DBConnectionInfo> for Server {
    fn from(info: DBConnectionInfo) -> Self {
        Self {
            host: info.host,
            port: info.port,
            user: info.user,
            password: info.password,
            database: info.database,
        }
    }
}

impl Server {
    /// A connection pool for this server holding at most `pool_max`
    /// connections, none of them while idle for [`POOL_INACTIVE_TTL`].
    fn pool(self, pool_max: usize) -> Result<CustomPool> {
        Ok(CustomPool::builder(self.host, self.user, self.password)
            .port(self.port)
            .database(self.database)
            .pool_max(pool_max)
            .inactive_connection_ttl(POOL_INACTIVE_TTL)
            .connect_timeout(DB_CONNECT_TIMEOUT)
            .tcp_keepalive(TCP_KEEPALIVE)
            .build()?)
    }
}

/// Whether `wiki` can be part of a replica host name: a wiki database name
/// only consists of lowercase ASCII letters, digits and underscores, so
/// anything else could send the connection, and the credentials, elsewhere.
fn is_valid_wiki(wiki: &str) -> bool {
    !wiki.is_empty()
        && wiki
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

type PoolKey = (String, DbCluster);

/// A per-wiki pool and when a connection was last leased from it.
#[derive(Debug)]
struct WikiPool {
    pool: CustomPool,
    last_used: Instant,
}

/// Deterministic exponential backoff (no jitter) capped at `MAX_DELAY_MS`.
/// Pulled out as a free function so it is unit-testable; jitter is applied
/// at the callsite.
fn exponential_backoff_ms(attempt: u32) -> u64 {
    // Saturating shift so attempt values up to u32::MAX cannot overflow.
    let shift = attempt.min(20);
    BASE_DELAY_MS
        .saturating_mul(1_u64 << shift)
        .min(MAX_DELAY_MS)
}

// ---------------------------------------------------------------------------
// DatabaseManager – owns all database-related state and logic
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct DatabaseManager {
    /// Typed application config (feature flags, schema name, and local-dev
    /// fallback credentials / port-mapping).
    config: Config,
    /// Per-`(wiki, cluster)` connection pools, lazily created and closed
    /// after [`POOL_EVICT_AFTER`] without use.
    wiki_pools: Arc<Mutex<HashMap<PoolKey, WikiPool>>>,
    /// Shared pool for the tool DB (PSID + query log).
    tool_db_pool: Arc<Mutex<Option<CustomPool>>>,
}

impl DatabaseManager {
    /// Initialise from the application config.
    ///
    /// Database servers and credentials are found by the `toolforge` crate:
    /// from the `TOOL_REPLICA_*` / `TOOL_TOOLSDB_*` environment variables, or
    /// `replica.my.cnf` in `$TOOL_DATA_DIR` or the home directory. Without
    /// them (local development, or a server outside Toolforge),
    /// `config.user` / `config.password` are used instead, and
    /// `config.port_mapping` so existing SSH-tunnel workflows continue to
    /// work unchanged.
    pub fn new_from_config(config: &Config) -> Self {
        Self {
            config: config.clone(),
            wiki_pools: Arc::new(Mutex::new(HashMap::new())),
            tool_db_pool: Arc::new(Mutex::new(None)),
        }
    }

    // ------------------------------------------------------------------
    // Test / minimal constructor
    // ------------------------------------------------------------------

    /// Create a [`DatabaseManager`] seeded with only a config value.
    /// Intended for unit tests that exercise config-derived logic without
    /// needing a real database connection.
    #[cfg(test)]
    pub(crate) fn with_config(config: Config) -> Self {
        Self {
            config,
            wiki_pools: Arc::new(Mutex::new(HashMap::new())),
            tool_db_pool: Arc::new(Mutex::new(None)),
        }
    }

    // ------------------------------------------------------------------
    // Config-based feature flags
    // ------------------------------------------------------------------

    pub const fn using_file_table(&self) -> bool {
        self.config.use_file_table
    }

    pub fn get_restart_code(&self) -> Option<&str> {
        self.config.restart_code.as_deref()
    }

    // ------------------------------------------------------------------
    // Wiki name and cluster resolution (credential-free helpers)
    // ------------------------------------------------------------------

    /// Whether `wiki` has this cluster at all. Every wiki has a
    /// [`DbCluster::Core`]; an extension database exists only for the wikis
    /// whose tables were actually split off.
    pub fn wiki_has_cluster(&self, wiki: &str, cluster: DbCluster) -> bool {
        let wiki = normalize_dbname(wiki);
        cluster
            .extension()
            .is_none_or(|extension| extension.exists_for(&wiki))
    }

    /// The cluster actually used for `wiki`: the requested one where the wiki
    /// has it, [`DbCluster::Core`] otherwise, where those tables still live.
    /// So a caller may always ask for [`DbCluster::Links`] when reading
    /// `pagelinks`, whatever the wiki.
    fn effective_cluster(&self, wiki: &str, cluster: DbCluster) -> DbCluster {
        if self.wiki_has_cluster(wiki, cluster) {
            cluster
        } else {
            DbCluster::Core
        }
    }

    /// Whether a query on `cluster` can read every one of `tables` for `wiki`.
    ///
    /// Unlike [`Self::cluster_for_tables`] this answers about a cluster the
    /// caller has already picked, which is what routing a query's individual
    /// clauses needs: `page` is on the Commons links cluster as well as its
    /// core one, so a clause reading only `page` fits either.
    pub fn cluster_hosts_tables(&self, wiki: &str, cluster: DbCluster, tables: &[&str]) -> bool {
        let wiki = normalize_dbname(wiki);
        match self.effective_cluster(&wiki, cluster).extension() {
            Some(extension) => tables.iter().all(|table| extension.has_table(table)),
            // The core database has every table not split off elsewhere.
            None => ExtensionDb::for_tables(&wiki, tables).is_ok_and(|e| e.is_none()),
        }
    }

    /// The single cluster able to serve a query reading all of `tables`.
    ///
    /// Errors when they are split across clusters — on Commons since the
    /// September 2026 links-table split — which no one connection can join;
    /// such a query has to be run in parts and combined in code.
    pub fn cluster_for_tables(&self, wiki: &str, tables: &[&str]) -> Result<DbCluster> {
        let wiki = normalize_dbname(wiki);
        DbCluster::from_extension(ExtensionDb::for_tables(&wiki, tables)?)
    }

    // ------------------------------------------------------------------
    // Connection helpers
    // ------------------------------------------------------------------

    /// The `MySQL` port for `(wiki, cluster)` with credentials from
    /// config.json, or with `config.host` set to `127.0.0.1`.
    ///
    /// Locally each replica is reached through its own SSH tunnel, so
    /// `config.port_mapping` maps the leading labels of the replica host —
    /// `commonswiki`, `links.commonswiki`, `termstore.wikidatawiki` — to a
    /// local port. `x3` is still accepted as a legacy alias for the term
    /// store.
    fn tunnel_port(&self, wiki: &str, cluster: DbCluster) -> u16 {
        let wiki = normalize_dbname(wiki);
        let cluster = self.effective_cluster(&wiki, cluster);
        let legacy_key = matches!(cluster, DbCluster::TermStore).then_some("x3");
        self.config
            .port_mapping
            .get(&cluster.tunnel_target(&wiki))
            .or_else(|| legacy_key.and_then(|k| self.config.port_mapping.get(k)))
            .copied()
            .unwrap_or_else(|| self.config.db_port.unwrap_or(3306))
    }

    /// The server holding `cluster` of `wiki`, and the credentials for it.
    ///
    /// `toolforge` resolves both, including its own local mode and tunnels
    /// (`local='true'` and `[tunnels]` in `replica.my.cnf`). When it finds no
    /// credentials, those from config.json are used. A `config.host` of
    /// `127.0.0.1` routes every replica through the SSH tunnels in
    /// `config.port_mapping`, wherever the credentials came from.
    fn replica_server(&self, wiki: &str, cluster: DbCluster) -> Result<Server> {
        let wiki = normalize_dbname(wiki);
        let cluster = self.effective_cluster(&wiki, cluster);
        let info = toolforge::db::connection_info_for_tables(
            &wiki,
            cluster.server_group(),
            cluster.routing_tables(),
        );
        let mut server = match info {
            Ok(info) => Server::from(info),
            Err(e) => self.config_replica_server(&wiki, cluster, e)?,
        };
        if self.config.host == LOCALHOST {
            server.host = LOCALHOST.to_string();
            server.port = self.tunnel_port(&wiki, cluster);
        }
        Ok(server)
    }

    /// The replica server for `cluster` of `wiki` (already normalised), with
    /// the credentials from config.json, for when `toolforge` found none
    /// (`error`).
    fn config_replica_server(
        &self,
        wiki: &str,
        cluster: DbCluster,
        error: toolforge::Error,
    ) -> Result<Server> {
        if matches!(error, toolforge::Error::InvalidDatabase(_)) {
            return Err(error.into());
        }
        if self.config.user.is_empty() {
            return Err(anyhow!(error).context(
                "Cannot resolve database credentials, and there is no 'user' key in config",
            ));
        }
        if !is_valid_wiki(wiki) {
            return Err(anyhow!("Invalid wiki database name {wiki:?}"));
        }
        Ok(Server {
            host: format!(
                "{target}.{group}{REPLICA_DOMAIN}",
                target = cluster.tunnel_target(wiki),
                group = cluster.server_group(),
            ),
            port: self.tunnel_port(wiki, cluster),
            user: self.config.user.clone(),
            password: self.config.password.clone(),
            database: format!("{wiki}_p"),
        })
    }

    async fn set_group_concat_max_len(&self, wiki: &str, conn: &mut my::Conn) -> Result<()> {
        if wiki == "commonswiki" {
            conn.exec_drop("SET SESSION group_concat_max_len = 1000000000", ())
                .await?;
        }
        Ok(())
    }

    /// Get or lazily create the connection pool for `(wiki, cluster)`, and
    /// close the pools nobody has used for [`POOL_EVICT_AFTER`].
    async fn get_wiki_pool(&self, wiki: &str, cluster: DbCluster) -> Result<CustomPool> {
        let now = Instant::now();
        let mut pools = self.wiki_pools.lock().await;
        for stale in take_stale_pools(&mut pools, now) {
            // Waits for any connection still leased, then ends the pool's
            // background tasks.
            tokio::spawn(stale.disconnect());
        }
        let key = (wiki.to_string(), cluster);
        if let Some(entry) = pools.get_mut(&key) {
            entry.last_used = now;
            return Ok(entry.pool.clone());
        }
        let pool = self.replica_server(wiki, cluster)?.pool(WIKI_POOL_MAX)?;
        pools.insert(
            key,
            WikiPool {
                pool: pool.clone(),
                last_used: now,
            },
        );
        Ok(pool)
    }

    /// Connects to a wiki's core cluster. `"x3"` is a legacy alias for the
    /// Wikidata term store.
    pub async fn get_wiki_db_connection(&self, wiki: &str) -> Result<my::Conn> {
        let (wiki, cluster) = match wiki {
            "x3" => ("wikidatawiki", DbCluster::TermStore),
            other => (other, DbCluster::Core),
        };
        self.get_wiki_db_connection_for_cluster(wiki, cluster).await
    }

    /// Connects to the cluster able to serve a query reading all of `tables`.
    ///
    /// Errors when they span clusters; see [`Self::cluster_for_tables`].
    pub async fn get_wiki_db_connection_for_tables(
        &self,
        wiki: &str,
        tables: &[&str],
    ) -> Result<my::Conn> {
        let cluster = self.cluster_for_tables(wiki, tables)?;
        self.get_wiki_db_connection_for_cluster(wiki, cluster).await
    }

    /// Connects to one cluster of a wiki, falling back to
    /// [`DbCluster::Core`] for wikis that do not have it.
    #[instrument(skip(self), err)]
    pub async fn get_wiki_db_connection_for_cluster(
        &self,
        wiki: &str,
        cluster: DbCluster,
    ) -> Result<my::Conn> {
        // Normalised once here so that spellings of one wiki share a pool.
        let wiki = normalize_dbname(wiki);
        let wiki = wiki.as_str();
        let cluster = self.effective_cluster(wiki, cluster);
        let pool = self.get_wiki_pool(wiki, cluster).await?;
        trace!(wiki, ?cluster, "leasing connection from pool");
        let mut attempt: u32 = 0;
        let mut conn = loop {
            let error = match pool.connect().await {
                Ok(conn) => break conn,
                Err(e) => e.to_string(),
            };
            // Retry when the per-user connection limit is momentarily
            // exceeded, but bound the retries so a chronically-full pool
            // can't pin a worker forever.
            if !error.contains("max_user_connections") || attempt >= MAX_RETRY_ATTEMPTS {
                return Err(anyhow!("Cannot connect to {wiki} ({cluster:?}): {error}"));
            }
            let base = exponential_backoff_ms(attempt);
            // ±25% jitter to avoid lockstep retries across workers.
            let jitter: f64 = rand::random_range(0.75..1.25);
            let delay_ms = ((base as f64) * jitter) as u64;
            tracing::warn!(
                wiki,
                attempt = attempt + 1,
                max_attempts = MAX_RETRY_ATTEMPTS,
                delay_ms,
                "max_user_connections hit; backing off"
            );
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            attempt += 1;
        };
        self.set_group_concat_max_len(wiki, &mut conn).await?;
        Ok(conn)
    }

    /// Connects to the Wikidata term-store cluster (`x3`).
    pub async fn get_x3_db_connection(&self) -> Result<my::Conn> {
        self.get_wiki_db_connection_for_cluster("wikidatawiki", DbCluster::TermStore)
            .await
    }

    /// The tool database's server: `ToolsDB` as found by `toolforge`, or else
    /// `config.host` with the credentials from config.json and the port from
    /// `config.db_port` (defaulting to 3308 when the host is 127.0.0.1,
    /// matching the conventional local SSH-tunnel mapping).
    fn tool_db_server(&self) -> Result<Server> {
        let schema = &self.config.schema;
        if schema.is_empty() {
            return Err(anyhow!("No schema key in config file"));
        }
        if let Ok(info) = toolforge::db::toolsdb(schema.as_str()) {
            return Ok(info.into());
        }
        if self.config.host.is_empty() {
            return Err(anyhow!("No host key in config file"));
        }
        if self.config.user.is_empty() {
            return Err(anyhow!("No user key in config file"));
        }
        let default_port: u16 = if self.config.host == LOCALHOST {
            3308
        } else {
            3306
        };
        Ok(Server {
            host: self.config.host.clone(),
            port: self.config.db_port.unwrap_or(default_port),
            user: self.config.user.clone(),
            password: self.config.password.clone(),
            database: schema.clone(),
        })
    }

    /// Opens a connection to the tool database (`config.schema`); see
    /// [`Self::tool_db_server`] for where it is.
    pub async fn get_tool_db_connection(&self) -> Result<my::Conn> {
        let pool = {
            let mut slot = self.tool_db_pool.lock().await;
            match slot.as_ref() {
                Some(pool) => pool.clone(),
                None => slot
                    .insert(self.tool_db_server()?.pool(TOOL_DB_POOL_MAX)?)
                    .clone(),
            }
        };
        pool.connect()
            .await
            .map_err(|e| anyhow!("Cannot connect to the tool database: {e}"))
    }

    // ------------------------------------------------------------------
    // Tool-DB query helpers (PSID, query logging)
    // ------------------------------------------------------------------

    pub async fn get_query_from_psid(&self, psid: &str) -> Result<String> {
        let psid: usize = psid
            .trim()
            .parse()
            .map_err(|_| anyhow!("Invalid PSID '{psid}': expected a number"))?;
        let mut conn = self.get_tool_db_connection().await?;
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
        let now = query_log_timestamp();
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

    /// Removes and returns the `started_queries` rows a process left behind,
    /// i.e. the queries it was running when it ended. `since` (the process's
    /// start, in [`query_log_timestamp`] form) keeps the rows of an older
    /// process that had the same PID out.
    pub async fn take_orphaned_queries(&self, process_id: u32, since: &str) -> Result<Vec<String>> {
        let params = vec![
            MyValue::UInt(process_id.into()),
            MyValue::Bytes(since.into()),
        ];
        let mut conn = self.get_tool_db_connection().await?;
        let queries = conn
            .exec_iter(
                "SELECT querystring FROM `started_queries` WHERE process_id=? AND created>=?",
                mysql_async::Params::Positional(params.clone()),
            )
            .await?
            .map_and_drop(|row| String::from_utf8_lossy(&from_row::<Vec<u8>>(row)).into_owned())
            .await?;
        conn.exec_drop(
            "DELETE FROM `started_queries` WHERE process_id=? AND created>=?",
            mysql_async::Params::Positional(params),
        )
        .await?;
        Ok(queries)
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

/// The current UTC time as stored in `started_queries.created`; the format
/// sorts as text, so such values compare correctly in SQL.
pub fn query_log_timestamp() -> String {
    Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

/// Removes and returns the pools last used [`POOL_EVICT_AFTER`] or longer
/// before `now`.
fn take_stale_pools(pools: &mut HashMap<PoolKey, WikiPool>, now: Instant) -> Vec<CustomPool> {
    let stale: Vec<PoolKey> = pools
        .iter()
        .filter(|(_, entry)| now.duration_since(entry.last_used) >= POOL_EVICT_AFTER)
        .map(|(key, _)| key.clone())
        .collect();
    stale
        .iter()
        .filter_map(|key| pools.remove(key))
        .map(|entry| entry.pool)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manager_with_ports(pairs: &[(&str, u16)]) -> DatabaseManager {
        DatabaseManager::with_config(Config {
            port_mapping: pairs.iter().map(|(k, v)| ((*k).to_string(), *v)).collect(),
            db_port: Some(3999),
            ..Default::default()
        })
    }

    #[test]
    fn tunnel_port_is_keyed_by_replica_host_labels() {
        let dbm = manager_with_ports(&[
            ("commonswiki", 3305),
            ("links.commonswiki", 3315),
            ("termstore.wikidatawiki", 3317),
        ]);
        assert_eq!(dbm.tunnel_port("commonswiki", DbCluster::Core), 3305);
        assert_eq!(dbm.tunnel_port("commonswiki", DbCluster::Links), 3315);
        assert_eq!(dbm.tunnel_port("wikidatawiki", DbCluster::TermStore), 3317);
        // Wikis without a links cluster read those tables from their core, so
        // they must not fall through to the Commons links tunnel.
        assert_eq!(dbm.tunnel_port("enwiki", DbCluster::Links), 3999);
    }

    #[test]
    fn tunnel_port_accepts_legacy_x3_key() {
        let dbm = manager_with_ports(&[("x3", 3317)]);
        assert_eq!(dbm.tunnel_port("wikidatawiki", DbCluster::TermStore), 3317);
    }

    #[test]
    fn cluster_extension_round_trips() {
        for cluster in [DbCluster::Core, DbCluster::Links, DbCluster::TermStore] {
            assert_eq!(
                DbCluster::from_extension(cluster.extension()).unwrap(),
                cluster
            );
        }
    }

    #[test]
    fn routing_tables_select_their_cluster() {
        for (wiki, cluster) in [
            ("enwiki", DbCluster::Core),
            ("commonswiki", DbCluster::Links),
            ("wikidatawiki", DbCluster::TermStore),
        ] {
            assert_eq!(
                ExtensionDb::for_tables(wiki, cluster.routing_tables()).unwrap(),
                cluster.extension()
            );
        }
    }

    #[test]
    fn cluster_hosts_tables_per_cluster() {
        let dbm = DatabaseManager::default();
        let hosts =
            |wiki, cluster, tables: &[&str]| dbm.cluster_hosts_tables(wiki, cluster, tables);
        // `page` is on both Commons clusters, `revision` only on the core one,
        // the links tables only on the links one.
        assert!(hosts("commonswiki", DbCluster::Core, &["page", "revision"]));
        assert!(!hosts("commonswiki", DbCluster::Core, &["pagelinks"]));
        assert!(hosts("commonswiki", DbCluster::Links, &[]));
        assert!(hosts("commonswiki", DbCluster::Links, &["page"]));
        assert!(hosts(
            "commonswiki",
            DbCluster::Links,
            &["categorylinks", "linktarget"]
        ));
        assert!(!hosts("commonswiki", DbCluster::Links, &["revision"]));
        // Elsewhere the core cluster has everything.
        assert!(hosts("enwiki", DbCluster::Core, &["pagelinks", "revision"]));
        assert!(hosts(
            "enwiki",
            DbCluster::Links,
            &["pagelinks", "revision"]
        ));
        // The term store has the `wbt_*` tables and nothing else.
        assert!(hosts(
            "wikidatawiki",
            DbCluster::TermStore,
            &["wbt_item_terms"]
        ));
        assert!(!hosts("wikidatawiki", DbCluster::TermStore, &["page"]));
        assert!(!hosts("wikidatawiki", DbCluster::Core, &["wbt_item_terms"]));
    }

    #[test]
    fn wiki_has_cluster_follows_extension_databases() {
        let dbm = DatabaseManager::default();
        assert!(dbm.wiki_has_cluster("enwiki", DbCluster::Core));
        assert!(dbm.wiki_has_cluster("commonswiki", DbCluster::Links));
        assert!(!dbm.wiki_has_cluster("enwiki", DbCluster::Links));
        assert!(dbm.wiki_has_cluster("wikidatawiki", DbCluster::TermStore));
        assert!(!dbm.wiki_has_cluster("commonswiki", DbCluster::TermStore));
    }

    fn manager_with_config_credentials(host: &str) -> DatabaseManager {
        DatabaseManager::with_config(Config {
            host: host.to_string(),
            user: "u1234".to_string(),
            password: "secret".to_string(),
            port_mapping: [("links.commonswiki".to_string(), 3315)].into(),
            ..Default::default()
        })
    }

    #[test]
    fn config_credentials_reach_the_replica_hosts() {
        let dbm = manager_with_config_credentials("tools.db.svc.wikimedia.cloud");
        let server = |wiki, cluster| {
            dbm.config_replica_server(wiki, cluster, toolforge::Error::NoHomeDir)
                .unwrap()
        };
        let core = server("enwiki", DbCluster::Core);
        assert_eq!(core.host, "enwiki.web.db.svc.wikimedia.cloud");
        assert_eq!(core.database, "enwiki_p");
        assert_eq!(core.user, "u1234");
        assert_eq!(
            server("commonswiki", DbCluster::Links).host,
            "links.commonswiki.web.db.svc.wikimedia.cloud"
        );
        assert_eq!(
            server("wikidatawiki", DbCluster::TermStore).host,
            "termstore.wikidatawiki.analytics.db.svc.wikimedia.cloud"
        );
    }

    #[test]
    fn config_credentials_refuse_invalid_wiki_names() {
        let dbm = manager_with_config_credentials("tools.db.svc.wikimedia.cloud");
        for wiki in ["", "EnWiki", "x.evil", "evil.example:3306/x#"] {
            assert!(
                dbm.config_replica_server(wiki, DbCluster::Core, toolforge::Error::NoHomeDir)
                    .is_err(),
                "{wiki:?} was accepted"
            );
        }
    }

    #[test]
    fn config_credentials_are_required_without_toolforge_ones() {
        let dbm = DatabaseManager::default();
        assert!(
            dbm.config_replica_server("enwiki", DbCluster::Core, toolforge::Error::NoHomeDir)
                .is_err()
        );
    }

    #[test]
    fn local_config_routes_replicas_through_port_mapping() {
        // Whether or not toolforge finds credentials on this machine, a
        // config host of 127.0.0.1 sends every replica to its local tunnel.
        let dbm = manager_with_config_credentials(LOCALHOST);
        let server = dbm.replica_server("commonswiki", DbCluster::Links).unwrap();
        assert_eq!(server.host, LOCALHOST);
        assert_eq!(server.port, 3315);
        assert_eq!(server.database, "commonswiki_p");
        let tarask = dbm
            .replica_server("be-taraskwiki", DbCluster::Core)
            .unwrap();
        assert_eq!(tarask.database, "be_x_oldwiki_p");
    }

    #[test]
    fn exponential_backoff_starts_at_base() {
        assert_eq!(exponential_backoff_ms(0), BASE_DELAY_MS);
    }

    #[test]
    fn exponential_backoff_doubles_each_step() {
        assert_eq!(exponential_backoff_ms(1), 200);
        assert_eq!(exponential_backoff_ms(2), 400);
        assert_eq!(exponential_backoff_ms(3), 800);
        assert_eq!(exponential_backoff_ms(4), 1600);
        assert_eq!(exponential_backoff_ms(5), 3200);
    }

    #[test]
    fn exponential_backoff_caps_at_max() {
        // 100 * 2^6 = 6400 > 5000 cap.
        assert_eq!(exponential_backoff_ms(6), MAX_DELAY_MS);
        assert_eq!(exponential_backoff_ms(10), MAX_DELAY_MS);
    }

    #[test]
    fn exponential_backoff_saturates_on_huge_attempt() {
        // Must not overflow or panic for any u32 input.
        assert_eq!(exponential_backoff_ms(u32::MAX), MAX_DELAY_MS);
    }

    #[tokio::test]
    async fn get_query_from_psid_rejects_non_numeric_psid() {
        let err = DatabaseManager::default()
            .get_query_from_psid("abc")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Invalid PSID 'abc'"), "{err}");
    }

    #[test]
    fn take_stale_pools_closes_only_long_unused_pools() {
        let pool = || {
            CustomPool::builder("localhost", "user", "password")
                .build()
                .unwrap()
        };
        let now = Instant::now();
        let mut pools = HashMap::new();
        pools.insert(
            ("enwiki".to_string(), DbCluster::Core),
            WikiPool {
                pool: pool(),
                last_used: now - POOL_EVICT_AFTER,
            },
        );
        pools.insert(
            ("dewiki".to_string(), DbCluster::Core),
            WikiPool {
                pool: pool(),
                last_used: now - POOL_EVICT_AFTER / 2,
            },
        );
        assert_eq!(take_stale_pools(&mut pools, now).len(), 1);
        assert_eq!(
            pools.keys().collect::<Vec<_>>(),
            vec![&("dewiki".to_string(), DbCluster::Core)]
        );
    }
}
