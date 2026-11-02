use crate::datasource::DataSource;
use crate::key_set::{KeySet, KeySetLimits, MAX_SPILLED_SOURCE_KEYS};
use crate::pagelist::{PageList, too_many_results};
use crate::platform::Platform;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use std::io::Read;
use std::sync::OnceLock;
use std::time::Duration;
use wikimisc::entity_id::EntityId;
use wikimisc::mediawiki::reqwest;
use wikimisc::sparql_client::{SparqlClient, SparqlError};
use wikimisc::sparql_endpoint::SparqlEndpoint;
use wikimisc::sparql_retry::{Retry, RetryPolicy};

/// The endpoints stop a query after a minute or so; this leaves room for
/// sending a large result.
const SPARQL_TIMEOUT: Duration = Duration::from_secs(5 * 60);
/// Bytes a SPARQL response may average per allowed row before it is refused
/// unread. Generous for `?item` plus a label or two; it bounds what is
/// buffered before the rows can be counted.
const MAX_SPARQL_BYTES_PER_ROW: usize = 300;
/// Cap on a key-only SPARQL response, which is buffered to a temp file rather
/// than memory. Generous for millions of entity IDs; the row cap still applies.
const MAX_KEYED_RESPONSE_BYTES: usize = 1 << 30;
/// How often a SPARQL request that failed transiently (rate limiting, a
/// briefly unavailable endpoint, a broken connection) is retried, and the
/// waits between attempts: 2, 4, 8, 16 and 30 s. Toolforge's outbound
/// address is shared by many tools, so unrelated traffic can get PetScan a
/// 429 even though its own request rate is unremarkable; waiting usually
/// clears that. A bad query, a timeout or an oversized result is not retried.
const SPARQL_RETRIES: u32 = 5;
const SPARQL_INITIAL_BACKOFF: Duration = Duration::from_secs(2);
const SPARQL_MAX_RETRY_WAIT: Duration = Duration::from_secs(30);
/// What SPARQL requests say they are. Wikimedia asks for contact information.
const USER_AGENT: &str = wikimisc::user_agent!(
    name = "PetScan",
    "https://petscan.toolforge.org; https://github.com/magnusmanske/petscan_rs"
);

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

/// Retries a SPARQL request that failed in a way that may clear on its own.
fn retry_policy() -> RetryPolicy {
    RetryPolicy::new(SPARQL_RETRIES)
        .with_initial_backoff(SPARQL_INITIAL_BACKOFF)
        .with_max_wait(SPARQL_MAX_RETRY_WAIT)
        .with_jitter()
        .on_retry(|retry: &Retry<'_>| {
            tracing::warn!(
                endpoint = retry.endpoint,
                attempt = retry.retry,
                delay_ms = retry.wait.as_millis(),
                error = %retry.error,
                "SPARQL request failed; retrying"
            );
        })
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
            .and_then(|uri| EntityId::from_uri(uri).ok())
            .and_then(Platform::entry_from_entity_id)
        {
            ret.add_entry(entry);
        }
    }
    Ok(ret)
}

/// Key-only counterpart of [`entities_from_csv`]: the entity IDs go straight
/// into a spilled key set, so a large result never becomes a page list.
fn entity_keys_from_csv<R: Read>(
    mut reader: csv::Reader<R>,
    limit: usize,
    limits: KeySetLimits,
) -> Result<KeySet> {
    let mut keys = KeySet::builder(limits, Some("wikidatawiki".to_string()));
    for (row, record) in reader.records().enumerate() {
        if row >= limit {
            return Err(too_many_results("the SPARQL result has", limit));
        }
        if let Some(entry) = record?
            .get(0)
            .and_then(|uri| EntityId::from_uri(uri).ok())
            .and_then(Platform::entry_from_entity_id)
        {
            keys.insert_entry(&entry)?;
        }
    }
    keys.finish()
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
        SparqlError::RateLimited { message, .. } => anyhow!(
            "The SPARQL endpoint is rate-limiting PetScan ({message}). \
             Please try again in a minute, or choose a different SPARQL server."
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
        let limit = platform.state().max_result_entries();
        let reader = Self::fetch_csv(
            platform,
            limit,
            limit.saturating_mul(MAX_SPARQL_BYTES_PER_ROW),
        )
        .await?;
        tokio::task::spawn_blocking(move || entities_from_csv(reader, limit))
            .await
            .map_err(|e| anyhow!("SPARQL parse task failed: {e}"))?
    }

    async fn run_keyed(&mut self, platform: &Platform, limits: KeySetLimits) -> Result<KeySet> {
        // Key-only results may exceed the final cap; they are reduced by the
        // following set operation and never become the output list.
        let limit = MAX_SPILLED_SOURCE_KEYS;
        let reader = Self::fetch_csv(platform, limit, MAX_KEYED_RESPONSE_BYTES).await?;
        tokio::task::spawn_blocking(move || entity_keys_from_csv(reader, limit, limits))
            .await
            .map_err(|e| anyhow!("SPARQL parse task failed: {e}"))?
    }
}

impl SourceSparql {
    /// Fetches the query result into a temporary file and returns a CSV reader
    /// over it. The body is never held in memory.
    async fn fetch_csv(
        platform: &Platform,
        limit: usize,
        max_bytes: usize,
    ) -> Result<csv::Reader<std::fs::File>> {
        let sparql = platform
            .get_param("sparql")
            .ok_or_else(|| anyhow!("Missing parameter 'sparql'"))?;
        let server = SparqlServer::from(platform.get_param("sparql_server").as_deref());
        let client = SparqlClient::with_client(server.endpoint(), http_client()?)
            .with_max_response_bytes(max_bytes)
            .with_retry_policy(retry_policy());
        client
            .query_csv(&sparql)
            .await
            .map_err(|e| sparql_error(e, limit))
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
    fn test_entities_from_csv_skips_what_is_not_a_wikidata_entity() {
        let text = "item\r\n\
            https://www.wikidata.org/entity/Q2\r\n\
            http://www.wikidata.org/entity/L7-F2\r\n\
            https://commons.wikimedia.org/entity/M5\r\n\
            http://www.wikidata.org/entity/Q0\r\n\
            Q3\r\n";
        let list = entities_from_csv(csv_reader(text), 10).unwrap();
        assert_eq!(list.len(), 2, "Q2 and the page of L7-F2");
    }

    #[test]
    fn test_user_agent_names_petscan() {
        assert!(USER_AGENT.starts_with(concat!("PetScan/", env!("CARGO_PKG_VERSION"), " (")));
        assert!(USER_AGENT.ends_with(
            " (https://petscan.toolforge.org; https://github.com/magnusmanske/petscan_rs)"
        ));
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

    #[test]
    fn test_entity_keys_from_csv_matches_the_page_list() {
        let keys = entity_keys_from_csv(csv_reader(THREE_ROWS), 10, KeySetLimits { threshold: 1 })
            .unwrap();
        assert!(keys.is_spilled(), "threshold 1 must spill two keys");
        assert_eq!(keys.wiki().as_deref(), Some("wikidatawiki"));
        let mut titles: Vec<String> = keys
            .to_page_list()
            .unwrap()
            .as_vec()
            .iter()
            .map(|entry| entry.title().pretty().to_string())
            .collect();
        titles.sort();
        assert_eq!(titles, vec!["P31", "Q1"]);
    }

    #[test]
    fn test_entity_keys_from_csv_refuses_more_rows_than_the_limit() {
        let err = entity_keys_from_csv(csv_reader(THREE_ROWS), 2, KeySetLimits { threshold: 10 })
            .unwrap_err()
            .to_string();
        assert!(
            err.starts_with("Too many results: the SPARQL result has more than 2 pages"),
            "{err}"
        );
    }

    // ── retry policy ─────────────────────────────────────────────────────────

    fn rate_limited() -> SparqlError {
        SparqlError::RateLimited {
            retry_after: None,
            message: "429 Too Many Requests".to_string(),
        }
    }

    #[test]
    fn test_rate_limits_and_server_errors_are_retried() {
        let policy = retry_policy();
        let first_wait = policy.delay(&rate_limited(), 0).expect("must retry");
        assert!(
            first_wait >= SPARQL_INITIAL_BACKOFF
                && first_wait <= SPARQL_INITIAL_BACKOFF + SPARQL_INITIAL_BACKOFF / 2,
            "{first_wait:?}"
        );
        assert!(
            policy
                .delay(
                    &SparqlError::Endpoint {
                        status: 503,
                        message: String::new(),
                    },
                    0
                )
                .is_some()
        );
        assert!(policy.delay(&rate_limited(), SPARQL_RETRIES - 1).is_some());
        assert_eq!(policy.delay(&rate_limited(), SPARQL_RETRIES), None);
    }

    #[test]
    fn test_bad_queries_and_timeouts_are_not_retried() {
        let policy = retry_policy();
        assert_eq!(
            policy.delay(
                &SparqlError::Endpoint {
                    status: 400,
                    message: String::new(),
                },
                0
            ),
            None
        );
        assert_eq!(
            policy.delay(
                &SparqlError::Timeout {
                    status: 429,
                    message: "Operation timed out.".to_string(),
                },
                0
            ),
            None
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
    fn test_an_exhausted_rate_limit_asks_the_user_to_retry() {
        let message = sparql_error(rate_limited(), 10).to_string();
        assert!(message.contains("try again"), "{message}");
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
