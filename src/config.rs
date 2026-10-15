//! Typed application configuration.
//!
//! Replaces the legacy `serde_json::Value` probes (e.g.
//! `config["x"].as_u64().unwrap_or(D)`) that were previously scattered across
//! modules. Unknown fields are tolerated so deployments can carry extra keys
//! without breaking startup.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use wikimisc::telemetry::TelemetryConfig;

/// Default for [`Config::max_result_entries`]. A million entries take about
/// half a gigabyte while a query runs; the service's memory limit is 7 GB.
pub const DEFAULT_MAX_RESULT_ENTRIES: usize = 1_000_000;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Tool DB host. When `"127.0.0.1"`, the manager routes wiki-replica
    /// connections through the local SSH-tunnel setup instead of cloud DNS.
    pub host: String,
    pub user: String,
    pub password: String,
    /// Tool DB schema name (e.g. `s51156__petscan`).
    pub schema: String,
    /// Override for the `MySQL` TCP port. Defaults differ per call site
    /// (3306 for replicas, 3308 for the local-tunnelled tool DB) so this stays
    /// optional and the default is applied where it's read.
    pub db_port: Option<u16>,
    /// HTTP listen port. Default 80 is applied at the webserver call site.
    pub http_port: Option<u16>,
    /// HTTP bind address. Default `0.0.0.0` is applied at the webserver call site.
    pub http_server: Option<String>,
    pub use_file_table: bool,
    /// Secret accepted via `?restart=…` to trigger a graceful drain.
    /// `None` disables the restart endpoint.
    #[serde(rename = "restart-code")]
    pub restart_code: Option<String>,
    /// Local-dev SSH-tunnel port overrides, keyed by wiki (e.g. `enwiki`) or
    /// the literal `x3` for the term-store cluster.
    pub port_mapping: HashMap<String, u16>,
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
        assert_eq!(c.host, "");
        assert_eq!(c.user, "");
        assert_eq!(c.password, "");
        assert_eq!(c.schema, "");
        assert_eq!(c.db_port, None);
        assert_eq!(c.http_port, None);
        assert_eq!(c.http_server, None);
        assert!(!c.use_file_table);
        assert_eq!(c.restart_code, None);
        assert!(c.port_mapping.is_empty());
        assert_eq!(c.telemetry.token, None);
    }

    #[test]
    fn template_shape_deserializes() {
        // Mirrors the layout of config.json.template, including legacy /
        // currently-unused keys that must still be tolerated.
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
            "mysql":[["uid","pw"]]
        }"#;
        let c: Config = serde_json::from_str(json).unwrap();
        assert_eq!(c.host, "127.0.0.1");
        assert_eq!(c.user, "s51156");
        assert_eq!(c.password, "secret");
        assert_eq!(c.schema, "s51156__petscan");
        assert_eq!(c.db_port, Some(3307));
        assert_eq!(c.http_port, Some(3000));
        assert_eq!(c.restart_code.as_deref(), Some("abc"));
        assert!(!c.use_file_table);
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
        assert_eq!(c.host, "");
    }

    #[test]
    fn telemetry_block() {
        let c: Config = serde_json::from_str(r#"{"telemetry":{"token":"t"}}"#).unwrap();
        assert_eq!(c.telemetry.token.as_deref(), Some("t"));
        assert_eq!(c.telemetry.url, None);
    }

    #[test]
    fn port_mapping_object_with_integer_values() {
        let json = r#"{"port_mapping":{"enwiki":3309,"x3":3310}}"#;
        let c: Config = serde_json::from_str(json).unwrap();
        assert_eq!(c.port_mapping.get("enwiki"), Some(&3309));
        assert_eq!(c.port_mapping.get("x3"), Some(&3310));
    }

    #[test]
    fn max_result_entries_defaults_and_overrides() {
        let default: Config = serde_json::from_str("{}").unwrap();
        assert_eq!(default.max_result_entries(), DEFAULT_MAX_RESULT_ENTRIES);
        let custom: Config = serde_json::from_str(r#"{"max_result_entries": 250000}"#).unwrap();
        assert_eq!(custom.max_result_entries(), 250_000);
    }
}
