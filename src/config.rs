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

impl Config {
    pub fn max_result_entries(&self) -> usize {
        self.max_result_entries
            .unwrap_or(DEFAULT_MAX_RESULT_ENTRIES)
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
    fn max_result_entries_defaults_and_overrides() {
        let default: Config = serde_json::from_str("{}").unwrap();
        assert_eq!(default.max_result_entries(), DEFAULT_MAX_RESULT_ENTRIES);
        let custom: Config = serde_json::from_str(r#"{"max_result_entries": 250000}"#).unwrap();
        assert_eq!(custom.max_result_entries(), 250_000);
    }
}
