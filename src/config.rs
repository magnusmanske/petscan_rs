//! Typed application configuration.
//!
//! Replaces the legacy `serde_json::Value` probes (e.g.
//! `config["x"].as_u64().unwrap_or(D)`) that were previously scattered across
//! modules. Unknown fields are tolerated so deployments can carry extra keys
//! without breaking startup.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use wikimisc::telemetry::TelemetryConfig;

/// Default for [`Config::max_result_entries`]. A million entries take about
/// half a gigabyte while a query runs; the service's memory limit is 7 GB.
pub const DEFAULT_MAX_RESULT_ENTRIES: usize = 1_000_000;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Tool DB schema name (e.g. `s51156__petscan`). The database servers
    /// and credentials come from `replica.my.cnf`, see
    /// [`crate::database_manager::DatabaseManager::new_from_config`].
    pub schema: String,
    /// A `replica.my.cnf`-style file to read the database credentials from,
    /// instead of looking them up. Lets a Toolforge tool use another
    /// account's credentials, overriding the `TOOL_*` variables Toolforge
    /// sets for its own.
    pub db_credentials_file: Option<PathBuf>,
    /// A single live MediaWiki database and wiki, instead of the Wikimedia
    /// replicas and Wikidata's site matrix. Set by an embedding service.
    #[serde(default)]
    pub single_wiki: Option<SingleWiki>,
    /// The tool's public base URL, for links in the output. Defaults to the
    /// Toolforge deployment.
    #[serde(default)]
    pub tool_url: Option<String>,
    /// Directory with `index.html`, `main.js` and the other static files.
    /// Defaults to `html`, relative to the service's working directory.
    pub html_root: Option<PathBuf>,
    /// HTTP listen port. Default 80 is applied at the webserver call site.
    pub http_port: Option<u16>,
    /// HTTP bind address. Default `0.0.0.0` is applied at the webserver call site.
    pub http_server: Option<String>,
    pub use_file_table: bool,
    /// Secret accepted via `?restart=…` to trigger a graceful drain.
    /// `None` disables the restart endpoint.
    #[serde(rename = "restart-code")]
    pub restart_code: Option<String>,
    /// Service-dashboard push target. Both fields are optional; without a
    /// token here or in `$DASHBOARD_TOKEN`, telemetry stays off.
    pub telemetry: TelemetryConfig,
    /// The most pages a single query may load before it is refused with an
    /// explanation. Defaults to [`DEFAULT_MAX_RESULT_ENTRIES`].
    pub max_result_entries: Option<usize>,
}

/// A single wiki's database and URLs, for an embedded deployment.
#[derive(Debug, Clone, Deserialize)]
pub struct SingleWiki {
    pub host: String,
    #[serde(default = "default_mysql_port")]
    pub port: u16,
    pub user: String,
    #[serde(default)]
    pub password: String,
    /// The wiki's database name.
    pub database: String,
    /// The wiki's server root, e.g. `https://example.org`.
    pub server: String,
    /// The exact action API URL.
    pub api: String,
    /// Optional display name.
    #[serde(default)]
    pub name: String,
}

const fn default_mysql_port() -> u16 {
    3306
}

impl Config {
    pub fn max_result_entries(&self) -> usize {
        self.max_result_entries
            .unwrap_or(DEFAULT_MAX_RESULT_ENTRIES)
    }

    /// The secret that enables `?restart=…`, if any.
    ///
    /// An empty value (as in `config.json.template`) disables the endpoint,
    /// just like an absent one. Treating `Some("")` as enabled made
    /// `?restart=` shut the service down with the template's default config.
    pub fn restart_code(&self) -> Option<&str> {
        self.restart_code.as_deref().filter(|code| !code.is_empty())
    }

    /// Load the config from a JSON file. Unknown JSON fields are ignored.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file = std::fs::File::open(path)
            .with_context(|| format!("Cannot open config file at {}", path.display()))?;
        serde_json::from_reader(file)
            .with_context(|| format!("Cannot parse JSON from config file at {}", path.display()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_json_uses_defaults() {
        let c: Config = serde_json::from_str("{}").unwrap();
        assert_eq!(c.schema, "");
        assert_eq!(c.http_port, None);
        assert_eq!(c.http_server, None);
        assert_eq!(c.db_credentials_file, None);
        assert_eq!(c.html_root, None);
        assert!(c.single_wiki.is_none());
        assert!(c.tool_url.is_none());
        assert!(!c.use_file_table);
        assert_eq!(c.restart_code, None);
        assert_eq!(c.telemetry.token, None);
    }

    #[test]
    fn legacy_config_keys_are_tolerated() {
        // Deployed config.json files still carry the keys that used to hold
        // database credentials and tunnel ports, now in replica.my.cnf.
        let json = r#"{
            "host":"127.0.0.1",
            "user":"s51156",
            "password":"secret",
            "schema":"s51156__petscan",
            "db_port":3307,
            "http_port":3000,
            "timeout":60000,
            "restart-code":"abc",
            "use_file_table": false,
            "use_new_categorylinks_table": false,
            "port_mapping":{"enwiki":3310},
            "mysql":[["uid","pw"]]
        }"#;
        let c: Config = serde_json::from_str(json).unwrap();
        assert_eq!(c.schema, "s51156__petscan");
        assert_eq!(c.http_port, Some(3000));
        assert_eq!(c.restart_code.as_deref(), Some("abc"));
        assert!(!c.use_file_table);
    }

    #[test]
    fn single_wiki_config() {
        let c: Config = serde_json::from_str(
            r#"{"single_wiki":{"host":"db","user":"u","password":"p","database":"my_wiki","server":"https://w.example","api":"https://w.example/w/api.php"}}"#,
        )
        .unwrap();
        let single = c.single_wiki.unwrap();
        assert_eq!(single.port, 3306);
        assert_eq!(single.database, "my_wiki");
    }

    #[test]
    fn db_credentials_file() {
        let c: Config = serde_json::from_str(r#"{"db_credentials_file":"/x/s1.my.cnf"}"#).unwrap();
        assert_eq!(c.db_credentials_file, Some(PathBuf::from("/x/s1.my.cnf")));
    }

    #[test]
    fn kebab_case_restart_code_field() {
        let c: Config = serde_json::from_str(r#"{"restart-code":"x"}"#).unwrap();
        assert_eq!(c.restart_code.as_deref(), Some("x"));
    }

    #[test]
    fn unknown_fields_are_ignored() {
        // Forward compatibility: deployments may add fields we don't yet read.
        let c: Config = serde_json::from_str(r#"{"completely_made_up": 42}"#).unwrap();
        assert_eq!(c.schema, "");
    }

    #[test]
    fn telemetry_block() {
        let c: Config = serde_json::from_str(r#"{"telemetry":{"token":"t"}}"#).unwrap();
        assert_eq!(c.telemetry.token.as_deref(), Some("t"));
        assert_eq!(c.telemetry.url, None);
    }

    #[test]
    fn empty_restart_code_disables_the_endpoint() {
        let c: Config = serde_json::from_str(r#"{"restart-code":""}"#).unwrap();
        assert_eq!(c.restart_code(), None);
        let enabled: Config = serde_json::from_str(r#"{"restart-code":"secret"}"#).unwrap();
        assert_eq!(enabled.restart_code(), Some("secret"));
    }

    #[test]
    fn max_result_entries_defaults_and_overrides() {
        let default: Config = serde_json::from_str("{}").unwrap();
        assert_eq!(default.max_result_entries(), DEFAULT_MAX_RESULT_ENTRIES);
        let custom: Config = serde_json::from_str(r#"{"max_result_entries": 250000}"#).unwrap();
        assert_eq!(custom.max_result_entries(), 250_000);
    }
}
