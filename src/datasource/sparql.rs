use crate::datasource::DataSource;
use crate::pagelist::PageList;
use crate::platform::Platform;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde_json::value::Value;
use std::collections::HashMap;
use std::time::{self, Duration};
use wikimisc::mediawiki::api::Api;
use wikimisc::mediawiki::reqwest::{self, ClientBuilder};

const SPARQL_TIMEOUT_SEC: u64 = 60 * 10; // 10 min
const QLEVER_WD_PREFIX: &str = "PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>";

enum SparqlServer {
    QLeverWd,
    Wikidata,
}

impl SparqlServer {
    fn from(s: Option<String>) -> Self {
        match s.as_deref() {
            Some("qlever_wd") => SparqlServer::QLeverWd,
            _ => SparqlServer::Wikidata,
        }
    }

    const fn url(&self) -> &str {
        match self {
            SparqlServer::QLeverWd => "https://qlever.cs.uni-freiburg.de/api/wikidata",
            SparqlServer::Wikidata => "https://query.wikidata.org/sparql",
        }
    }

    fn add_prefix(&self, sparql: &str) -> String {
        match self {
            SparqlServer::QLeverWd => format!("{QLEVER_WD_PREFIX}\n{sparql}"),
            SparqlServer::Wikidata => sparql.to_string(),
        }
    }

    /// Parse a standard SPARQL JSON response. Both WDQS and QLever emit the same
    /// W3C SPARQL 1.1 JSON format, so one parser handles both endpoints.
    fn parse_response(response: &str, api: &Api) -> Result<PageList> {
        Self::parse_response_standard(response, api)
    }

    /// Replace stray control characters (the endpoints occasionally emit them)
    /// with spaces, keeping the JSON-significant whitespace intact.
    fn sanitize_control_chars(response: &str) -> String {
        response
            .chars()
            .map(|c| {
                if c.is_control() && c != '\n' && c != '\r' && c != '\t' {
                    ' '
                } else {
                    c
                }
            })
            .collect()
    }

    /// Parse the SPARQL endpoint's body as JSON, turning the opaque serde error
    /// (`expected value at line 1 column 1`, issue #209) into an actionable
    /// message: a non-JSON body almost always means a transient endpoint
    /// problem (timeout, throttling, maintenance, or an HTML error page).
    fn parse_sparql_json(response: &str) -> Result<Value> {
        let sanitized = Self::sanitize_control_chars(response);
        serde_json::from_str(&sanitized).map_err(|e| {
            let snippet: String = sanitized.trim().chars().take(200).collect();
            anyhow!(
                "SPARQL endpoint did not return valid JSON ({e}). This is usually a transient endpoint error (timeout, rate limit, or maintenance); please retry. Response began: {snippet:?}"
            )
        })
    }

    fn parse_response_standard(response: &str, api: &Api) -> Result<PageList> {
        let result = Self::parse_sparql_json(response)?;
        let first_var = result["head"]["vars"][0]
            .as_str()
            .ok_or_else(|| anyhow!("No variables found in SPARQL result"))?;
        let ret = PageList::new_from_wiki("wikidatawiki");
        api.entities_from_sparql_result(&result, first_var)
            .iter()
            .filter_map(|e| Platform::entry_from_entity(e))
            .for_each(|entry| ret.add_entry(entry));
        Ok(ret)
    }
}

#[derive(Debug, Clone, PartialEq, Default, Copy)]
pub struct SourceSparql;

#[async_trait]
impl DataSource for SourceSparql {
    fn name(&self) -> String {
        "sparql".to_string()
    }

    fn can_run(&self, platform: &Platform) -> bool {
        platform.has_param("sparql")
    }

    async fn run(&mut self, platform: &Platform) -> Result<PageList> {
        let sparql_param = platform
            .get_param("sparql")
            .ok_or_else(|| anyhow!("Missing parameter \'sparql\'"))?;

        let timeout = time::Duration::from_secs(3600);
        let builder = ClientBuilder::new().timeout(timeout);
        let api = Api::new_from_builder("https://www.wikidata.org/w/api.php", builder).await?;

        // let sparql_url = api.get_site_info_string("general", "wikibase-sparql")?;
        let sparql_server = SparqlServer::from(platform.get_param("sparql_server"));
        let sparql_url = sparql_server.url();
        let sparql = sparql_server.add_prefix(&sparql_param);

        let mut params: HashMap<String, String> = HashMap::new();
        params.insert("query".to_string(), sparql.to_string());
        params.insert("format".to_string(), "json".to_string());

        let response = match api
            .client()
            .post(sparql_url)
            .header(reqwest::header::USER_AGENT, "PetScan")
            .timeout(Duration::from_secs(SPARQL_TIMEOUT_SEC))
            .form(&params)
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) => return Err(anyhow!("SPARQL: {e}")),
        };

        let response = response.text().await?;
        tokio::task::spawn_blocking(move || SparqlServer::parse_response(&response, &api))
            .await
            .map_err(|e| anyhow!("SPARQL parse task failed: {e}"))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::make_platform;

    // ── SparqlServer::from ───────────────────────────────────────────────────

    #[test]
    fn test_sparql_server_from_none() {
        assert!(matches!(SparqlServer::from(None), SparqlServer::Wikidata));
    }

    #[test]
    fn test_sparql_server_from_qlever_wd() {
        assert!(matches!(
            SparqlServer::from(Some("qlever_wd".to_string())),
            SparqlServer::QLeverWd
        ));
    }

    #[test]
    fn test_sparql_server_from_unknown_falls_back_to_wikidata() {
        assert!(matches!(
            SparqlServer::from(Some("unknown_server".to_string())),
            SparqlServer::Wikidata
        ));
    }

    // ── SparqlServer::url ────────────────────────────────────────────────────

    #[test]
    fn test_sparql_server_wikidata_url() {
        let url = SparqlServer::Wikidata.url();
        assert!(
            url.contains("wikidata.org"),
            "Expected wikidata.org in: {url}"
        );
    }

    #[test]
    fn test_sparql_server_qlever_url() {
        let url = SparqlServer::QLeverWd.url();
        assert!(url.contains("qlever"), "Expected qlever in: {url}");
    }

    // ── SparqlServer::add_prefix ─────────────────────────────────────────────

    #[test]
    fn test_add_prefix_wikidata_returns_sparql_unchanged() {
        let sparql = "SELECT ?item WHERE { ?item wdt:P31 wd:Q5 }";
        let result = SparqlServer::Wikidata.add_prefix(sparql);
        assert_eq!(result, sparql);
    }

    #[test]
    fn test_add_prefix_qlever_prepends_prefix_block() {
        let sparql = "SELECT ?item WHERE { ?item wdt:P31 wd:Q5 }";
        let result = SparqlServer::QLeverWd.add_prefix(sparql);
        assert!(
            result.starts_with("PREFIX"),
            "Expected PREFIX at start, got: {result}"
        );
        assert!(result.contains(sparql), "Expected original query in result");
    }

    // ── can_run / name ───────────────────────────────────────────────────────

    #[test]
    fn test_name() {
        assert_eq!(SourceSparql.name(), "sparql");
    }

    #[test]
    fn test_can_run_with_sparql_param() {
        let p = make_platform(vec![("sparql", "SELECT ?x WHERE {}")]);
        assert!(SourceSparql.can_run(&p));
    }

    #[test]
    fn test_can_run_without_sparql_param() {
        let p = make_platform(vec![]);
        assert!(!SourceSparql.can_run(&p));
    }

    // ── sanitize_control_chars ───────────────────────────────────────────────

    #[test]
    fn test_sanitize_replaces_control_chars_but_keeps_whitespace() {
        let input = "a\u{0}b\tc\nd\re";
        // NUL becomes a space; tab/newline/carriage-return are preserved.
        assert_eq!(SparqlServer::sanitize_control_chars(input), "a b\tc\nd\re");
    }

    // ── parse_sparql_json (issue #209) ───────────────────────────────────────

    #[test]
    fn test_parse_sparql_json_valid() {
        let json = r#"{"head":{"vars":["item"]},"results":{"bindings":[]}}"#;
        let value = SparqlServer::parse_sparql_json(json).expect("valid JSON should parse");
        assert_eq!(value["head"]["vars"][0], "item");
    }

    #[test]
    fn test_parse_sparql_json_non_json_gives_actionable_error() {
        // An HTML error page (e.g. WDQS 429/503) — the exact failure behind #209.
        let html = "<html><body>429 Too Many Requests</body></html>";
        let err = SparqlServer::parse_sparql_json(html)
            .expect_err("non-JSON must be an error")
            .to_string();
        assert!(err.contains("did not return valid JSON"), "got: {err}");
        assert!(err.contains("retry"), "got: {err}");
        // The original response is surfaced to aid diagnosis.
        assert!(err.contains("429 Too Many Requests"), "got: {err}");
    }

    #[test]
    fn test_parse_sparql_json_empty_body_is_error() {
        let err = SparqlServer::parse_sparql_json("")
            .expect_err("empty body must be an error")
            .to_string();
        assert!(err.contains("did not return valid JSON"), "got: {err}");
    }
}
