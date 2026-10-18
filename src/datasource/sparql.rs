use crate::datasource::DataSource;
use crate::pagelist::{PageList, too_many_results};
use crate::platform::Platform;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use std::io::Read;
use std::sync::OnceLock;
use std::time::Duration;
use wikimisc::mediawiki::reqwest;
use wikimisc::sparql_client::{SparqlClient, SparqlError};
use wikimisc::sparql_endpoint::SparqlEndpoint;

/// The endpoints stop a query after a minute or so; this leaves room for
/// sending a large result.
const SPARQL_TIMEOUT: Duration = Duration::from_secs(5 * 60);
/// Bytes a SPARQL response may average per allowed row before it is refused
/// unread. Generous for `?item` plus a label or two; it bounds what is
/// buffered before the rows can be counted.
const MAX_SPARQL_BYTES_PER_ROW: usize = 300;
/// What SPARQL requests say they are. Wikimedia asks for contact information.
const USER_AGENT: &str = concat!(
    "PetScan/",
    env!("CARGO_PKG_VERSION"),
    " (https://petscan.wmcloud.org; https://github.com/magnusmanske/petscan_rs)"
);
/// Where Wikidata's entity URIs start.
const ENTITY_PREFIX: &str = "http://www.wikidata.org/entity/";

/// The endpoint a query goes to, from the `sparql_server` parameter.
///
/// Wikidata keeps scholarly articles in a graph of their own, on WDQS v1
/// (Blazegraph) and v2 (QLever) alike. v2 admits only user agents Wikimedia
/// has allow-listed until its public launch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SparqlServer {
    Wikidata,
    WikidataScholarly,
    WikidataV2,
    WikidataV2Scholarly,
    QLeverWd,
}

impl SparqlServer {
    fn from(s: Option<&str>) -> Self {
        match s {
            Some("wikidata_scholarly") => Self::WikidataScholarly,
            Some("wikidata_v2") => Self::WikidataV2,
            Some("wikidata_v2_scholarly") => Self::WikidataV2Scholarly,
            Some("qlever_wd") => Self::QLeverWd,
            _ => Self::Wikidata,
        }
    }

    /// The endpoint, with the prefixes it needs: QLever's own Wikidata
    /// instance declares none.
    fn endpoint(self) -> SparqlEndpoint {
        match self {
            Self::Wikidata => SparqlEndpoint::wdqs(),
            Self::WikidataScholarly => SparqlEndpoint::wdqs_scholarly(),
            Self::WikidataV2 => SparqlEndpoint::wikidata_main(),
            Self::WikidataV2Scholarly => SparqlEndpoint::wikidata_scholarly(),
            Self::QLeverWd => SparqlEndpoint::qlever_wikidata(),
        }
    }
}

/// The shared HTTP client for SPARQL requests.
fn http_client() -> Result<reqwest::Client> {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    if let Some(client) = CLIENT.get() {
        return Ok(client.clone());
    }
    let client = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(SPARQL_TIMEOUT)
        .build()?;
    Ok(CLIENT.get_or_init(|| client).clone())
}

/// The Wikidata entities in the first column of a CSV result, refused once
/// there are more than `limit` rows. The result is read row by row, so an
/// oversized one costs no more memory than one row.
fn entities_from_csv<R: Read>(mut reader: csv::Reader<R>, limit: usize) -> Result<PageList> {
    let ret = PageList::new_from_wiki("wikidatawiki");
    for (row, record) in reader.records().enumerate() {
        if row >= limit {
            return Err(too_many_results("the SPARQL result has", limit));
        }
        if let Some(entry) = record?
            .get(0)
            .and_then(|uri| uri.strip_prefix(ENTITY_PREFIX))
            .and_then(Platform::entry_from_entity)
        {
            ret.add_entry(entry);
        }
    }
    Ok(ret)
}

/// A SPARQL failure as PetScan reports it. A query the endpoint rejects is
/// the user's to fix, so it does not mention SPARQL as an upstream failure.
fn sparql_error(error: SparqlError, limit: usize) -> anyhow::Error {
    match error {
        SparqlError::Endpoint {
            status: 400,
            message,
        } => {
            anyhow!("The endpoint rejected the query: {message}")
        }
        SparqlError::ResponseTooLarge { limit: bytes } => too_many_results(
            &format!(
                "the SPARQL result exceeds {} MB, so it likely has",
                bytes / 1_000_000
            ),
            limit,
        ),
        other => anyhow!("SPARQL: {other}"),
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
        let sparql = platform
            .get_param("sparql")
            .ok_or_else(|| anyhow!("Missing parameter \'sparql\'"))?;
        let server = SparqlServer::from(platform.get_param("sparql_server").as_deref());
        let limit = platform.state().max_result_entries();
        let client = SparqlClient::with_client(server.endpoint(), http_client()?)
            .with_max_response_bytes(limit.saturating_mul(MAX_SPARQL_BYTES_PER_ROW));

        let reader = client
            .query_csv(&sparql)
            .await
            .map_err(|e| sparql_error(e, limit))?;
        tokio::task::spawn_blocking(move || entities_from_csv(reader, limit))
            .await
            .map_err(|e| anyhow!("SPARQL parse task failed: {e}"))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::make_platform;

    // ── SparqlServer ─────────────────────────────────────────────────────────

    #[test]
    fn test_sparql_server_from_parameter() {
        for (param, server) in [
            (None, SparqlServer::Wikidata),
            (Some("wikidata"), SparqlServer::Wikidata),
            (Some("unknown_server"), SparqlServer::Wikidata),
            (Some("wikidata_scholarly"), SparqlServer::WikidataScholarly),
            (Some("wikidata_v2"), SparqlServer::WikidataV2),
            (
                Some("wikidata_v2_scholarly"),
                SparqlServer::WikidataV2Scholarly,
            ),
            (Some("qlever_wd"), SparqlServer::QLeverWd),
        ] {
            assert_eq!(SparqlServer::from(param), server, "{param:?}");
        }
    }

    #[test]
    fn test_sparql_server_endpoints() {
        assert_eq!(SparqlServer::Wikidata.endpoint(), SparqlEndpoint::wdqs());
        assert_eq!(
            SparqlServer::WikidataV2Scholarly.endpoint(),
            SparqlEndpoint::wikidata_scholarly()
        );
        // QLever's own instance needs the Wikidata prefixes; WDQS v1 has them.
        let query = "SELECT ?item WHERE { ?item wdt:P31 wd:Q5 }";
        assert!(
            SparqlServer::QLeverWd
                .endpoint()
                .prepare_query(query)
                .starts_with("PREFIX")
        );
        assert_eq!(
            SparqlServer::Wikidata.endpoint().prepare_query(query),
            query
        );
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

    // ── entities_from_csv ────────────────────────────────────────────────────

    const THREE_ROWS: &str = "item,label\r\n\
        http://www.wikidata.org/entity/Q1,\"say \"\"hi\"\"\"\r\n\
        ,no item\r\n\
        http://www.wikidata.org/entity/P31,\r\n";

    fn csv_reader(text: &str) -> csv::Reader<&[u8]> {
        csv::ReaderBuilder::new()
            .flexible(true)
            .from_reader(text.as_bytes())
    }

    #[test]
    fn test_entities_from_csv_keeps_entities_in_the_first_column() {
        let list = entities_from_csv(csv_reader(THREE_ROWS), 10).unwrap();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_entities_from_csv_refuses_more_rows_than_the_limit() {
        let err = entities_from_csv(csv_reader(THREE_ROWS), 2)
            .unwrap_err()
            .to_string();
        assert!(
            err.starts_with("Too many results: the SPARQL result has more than 2 pages"),
            "{err}"
        );
    }

    // ── sparql_error ─────────────────────────────────────────────────────────

    #[test]
    fn test_a_rejected_query_is_a_user_error() {
        let error = SparqlError::Endpoint {
            status: 400,
            message: "Encountered EOF".to_string(),
        };
        let message = sparql_error(error, 10).to_string();
        assert_eq!(
            crate::app_state::AppError::classify(&message),
            crate::app_state::AppError::BadRequest,
            "{message}"
        );
    }

    #[test]
    fn test_an_endpoint_failure_is_upstream() {
        let error = SparqlError::Endpoint {
            status: 503,
            message: "Service Unavailable".to_string(),
        };
        let message = sparql_error(error, 10).to_string();
        assert_eq!(
            crate::app_state::AppError::classify(&message),
            crate::app_state::AppError::Upstream,
            "{message}"
        );
    }

    #[test]
    fn test_an_oversized_result_is_too_many_results() {
        let message =
            sparql_error(SparqlError::ResponseTooLarge { limit: 3_000_000 }, 10).to_string();
        assert!(
            message.starts_with("Too many results: the SPARQL result exceeds 3 MB"),
            "{message}"
        );
    }
}
