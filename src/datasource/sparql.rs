use crate::datasource::DataSource;
use crate::pagelist::{PageList, too_many_results};
use crate::platform::Platform;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde::de::{Deserialize, DeserializeOwned, Deserializer, IgnoredAny, SeqAccess, Visitor};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::time::{self, Duration};
use wikimisc::mediawiki::api::Api;
use wikimisc::mediawiki::reqwest::{self, ClientBuilder};

const SPARQL_TIMEOUT_SEC: u64 = 60 * 10; // 10 min
/// Bytes a SPARQL response may average per allowed row before it is refused
/// unread. Generous for `?item` plus a label or two; it bounds what is
/// buffered before the rows can be counted.
const MAX_SPARQL_BYTES_PER_ROW: usize = 300;
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
    fn parse_response(response: &str, api: &Api, limit: usize) -> Result<PageList> {
        let ret = PageList::new_from_wiki("wikidatawiki");
        Self::first_var_values(response, limit)?
            .iter()
            .filter_map(|uri| api.extract_entity_from_uri(uri).ok())
            .filter_map(|entity| Platform::entry_from_entity(&entity))
            .for_each(|entry| ret.add_entry(entry));
        Ok(ret)
    }

    /// The values of the result's first variable, refused once there are
    /// more than `limit` rows. The rows are counted before any is kept, so an
    /// oversized result costs no more memory than its text.
    fn first_var_values(response: &str, limit: usize) -> Result<Vec<String>> {
        let response = Self::sanitize_control_chars(response);
        let counted: SparqlJson<ArrayLen> = Self::parse_sparql_json(&response)?;
        if counted.results.bindings.0 > limit {
            return Err(too_many_results("the SPARQL result has", limit));
        }
        let first_var = counted
            .head
            .vars
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("No variables found in SPARQL result"))?;
        let rows: SparqlJson<Vec<SparqlRow>> = Self::parse_sparql_json(&response)?;
        Ok(rows
            .results
            .bindings
            .into_iter()
            .filter_map(|mut row| row.remove(&first_var))
            .map(|binding| binding.value)
            .collect())
    }

    /// Replace stray control characters (the endpoints occasionally emit them)
    /// with spaces, keeping the JSON-significant whitespace intact. Borrows
    /// the response when there is nothing to replace.
    fn sanitize_control_chars(response: &str) -> Cow<'_, str> {
        let is_bad = |c: char| c.is_control() && c != '\n' && c != '\r' && c != '\t';
        if !response.contains(is_bad) {
            return Cow::Borrowed(response);
        }
        Cow::Owned(
            response
                .chars()
                .map(|c| if is_bad(c) { ' ' } else { c })
                .collect(),
        )
    }

    /// Parse the SPARQL endpoint's body as JSON, turning the opaque serde error
    /// (`expected value at line 1 column 1`, issue #209) into an actionable
    /// message: a non-JSON body almost always means a transient endpoint
    /// problem (timeout, throttling, maintenance, or an HTML error page).
    fn parse_sparql_json<T: DeserializeOwned>(response: &str) -> Result<T> {
        serde_json::from_str(response).map_err(|e| {
            let snippet: String = response.trim().chars().take(200).collect();
            anyhow!(
                "SPARQL endpoint did not return valid JSON ({e}). This is usually a transient endpoint error (timeout, rate limit, or maintenance); please retry. Response began: {snippet:?}"
            )
        })
    }

    /// Reads the response body, refusing one larger than `limit` rows could
    /// plausibly need before it is buffered in full.
    async fn read_capped(mut response: reqwest::Response, limit: usize) -> Result<String> {
        let max_bytes = limit.saturating_mul(MAX_SPARQL_BYTES_PER_ROW);
        let too_large = || {
            too_many_results(
                &format!(
                    "the SPARQL result exceeds {} MB, so it likely has",
                    max_bytes / 1_000_000
                ),
                limit,
            )
        };
        if response
            .content_length()
            .is_some_and(|n| n as usize > max_bytes)
        {
            return Err(too_large());
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if body.len() + chunk.len() > max_bytes {
                return Err(too_large());
            }
            body.extend_from_slice(&chunk);
        }
        String::from_utf8(body).map_err(|e| anyhow!("SPARQL response is not UTF-8: {e}"))
    }
}

/// A SPARQL JSON result, with `bindings` read as `B`.
#[derive(Debug, serde::Deserialize)]
struct SparqlJson<B> {
    head: SparqlHead,
    results: SparqlResults<B>,
}

#[derive(Debug, serde::Deserialize)]
struct SparqlHead {
    vars: Vec<String>,
}

#[derive(Debug, serde::Deserialize)]
struct SparqlResults<B> {
    bindings: B,
}

type SparqlRow = HashMap<String, SparqlBinding>;

#[derive(Debug, serde::Deserialize)]
struct SparqlBinding {
    value: String,
}

/// Reads a JSON array as just its length, skipping the elements.
#[derive(Debug)]
struct ArrayLen(usize);

impl<'de> Deserialize<'de> for ArrayLen {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct LenVisitor;
        impl<'de> Visitor<'de> for LenVisitor {
            type Value = ArrayLen;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an array")
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<ArrayLen, A::Error> {
                let mut len = 0;
                while seq.next_element::<IgnoredAny>()?.is_some() {
                    len += 1;
                }
                Ok(ArrayLen(len))
            }
        }
        deserializer.deserialize_seq(LenVisitor)
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

        let limit = platform.state().max_result_entries();
        let response = SparqlServer::read_capped(response, limit).await?;
        tokio::task::spawn_blocking(move || SparqlServer::parse_response(&response, &api, limit))
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
        let parsed: SparqlJson<ArrayLen> =
            SparqlServer::parse_sparql_json(json).expect("valid JSON should parse");
        assert_eq!(parsed.head.vars, vec!["item"]);
        assert_eq!(parsed.results.bindings.0, 0);
    }

    const TWO_ROWS: &str = r#"{"head":{"vars":["item","label"]},"results":{"bindings":[
        {"item":{"type":"uri","value":"http://www.wikidata.org/entity/Q1"},"label":{"type":"literal","value":"say \"hi\""}},
        {"label":{"type":"literal","value":"no item"}},
        {"item":{"type":"uri","value":"http://www.wikidata.org/entity/Q2"}}
    ]}}"#;

    #[test]
    fn test_first_var_values_keeps_bound_first_variable_only() {
        let values = SparqlServer::first_var_values(TWO_ROWS, 10).unwrap();
        assert_eq!(
            values,
            vec![
                "http://www.wikidata.org/entity/Q1",
                "http://www.wikidata.org/entity/Q2"
            ]
        );
    }

    #[test]
    fn test_first_var_values_refuses_more_rows_than_the_limit() {
        // Three rows, counted before any is kept; two are allowed.
        let err = SparqlServer::first_var_values(TWO_ROWS, 2)
            .unwrap_err()
            .to_string();
        assert!(
            err.starts_with("Too many results: the SPARQL result has more than 2 pages"),
            "{err}"
        );
    }

    #[test]
    fn test_sanitize_borrows_clean_input() {
        assert!(matches!(
            SparqlServer::sanitize_control_chars("clean"),
            Cow::Borrowed("clean")
        ));
    }

    #[test]
    fn test_parse_sparql_json_non_json_gives_actionable_error() {
        // An HTML error page (e.g. WDQS 429/503) — the exact failure behind #209.
        let html = "<html><body>429 Too Many Requests</body></html>";
        let err = SparqlServer::parse_sparql_json::<SparqlJson<ArrayLen>>(html)
            .expect_err("non-JSON must be an error")
            .to_string();
        assert!(err.contains("did not return valid JSON"), "got: {err}");
        assert!(err.contains("retry"), "got: {err}");
        // The original response is surfaced to aid diagnosis.
        assert!(err.contains("429 Too Many Requests"), "got: {err}");
    }

    #[test]
    fn test_parse_sparql_json_empty_body_is_error() {
        let err = SparqlServer::parse_sparql_json::<SparqlJson<ArrayLen>>("")
            .expect_err("empty body must be an error")
            .to_string();
        assert!(err.contains("did not return valid JSON"), "got: {err}");
    }
}
