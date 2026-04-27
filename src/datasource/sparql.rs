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

    fn parse_response_standard(response: &str, api: &Api) -> Result<PageList> {
        // println!("Sanitize before: {}", response.len());
        // println!("End of response: {}", &response[response.len() - 20..]);
        let sanitized: String = response
            .chars()
            .map(|c| {
                if c.is_control() && c != '\n' && c != '\r' && c != '\t' {
                    ' '
                } else {
                    c
                }
            })
            .collect();
        // println!("Sanitize after: {}", sanitized.len());
        let result: Value = serde_json::from_str(&sanitized)?;
        // println!("JSON parsing complete");
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
    use crate::app_state::AppState;
    use crate::form_parameters::FormParameters;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_platform(pairs: Vec<(&str, &str)>) -> Platform {
        let mut params = HashMap::new();
        for (k, v) in pairs {
            params.insert(k.to_string(), v.to_string());
        }
        let fp = FormParameters::new_from_pairs(params);
        Platform::new_from_parameters(&fp, Arc::new(AppState::default()))
    }

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
        assert!(url.contains("wikidata.org"), "Expected wikidata.org in: {url}");
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
        assert!(result.starts_with("PREFIX"), "Expected PREFIX at start, got: {result}");
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
}
