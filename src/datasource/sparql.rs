use crate::datasource::DataSource;
use crate::pagelist::PageList;
use crate::platform::Platform;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde_json::value::Value;
use std::collections::HashMap;
use std::time;
use wikimisc::mediawiki::api::Api;
use wikimisc::mediawiki::reqwest::{self, ClientBuilder};

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
    fn parse_response(&self, response: &str, api: &Api) -> Result<PageList> {
        Self::parse_response_standard(response, api)
    }

    fn parse_response_standard(response: &str, api: &Api) -> Result<PageList> {
        let result: Value = serde_json::from_str(response)?;
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

        let timeout = time::Duration::from_secs(120);
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
            .form(&params)
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) => return Err(anyhow!("SPARQL: {e}")),
        };

        let response = response.text().await.map_err(|e| anyhow!(e))?;
        tokio::task::spawn_blocking(move || sparql_server.parse_response(&response, &api))
            .await
            .map_err(|e| anyhow!("SPARQL parse task failed: {e}"))?
    }
}
