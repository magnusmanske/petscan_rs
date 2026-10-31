use crate::datasource::{DataSource, MAX_SEED_PAGES, SQLtuple};
use crate::pagelist::PageList;
use crate::platform::Platform;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use wikimisc::mysql_async::Value as MyValue;
use wikimisc::mysql_async::from_row;
use wikimisc::mysql_async::prelude::Queryable;

#[derive(Debug, Clone, PartialEq, Default, Copy)]
pub struct SourceWikidata;

#[async_trait]
impl DataSource for SourceWikidata {
    fn name(&self) -> String {
        "wikidata".to_string()
    }

    fn can_run(&self, platform: &Platform) -> bool {
        platform.has_param("wpiu_no_statements") && platform.has_param("wikidata_source_sites")
    }

    async fn run(&mut self, platform: &Platform) -> Result<PageList> {
        let sql = Self::generate_sql_query(platform)?;
        self.run_sql_query(sql, platform).await
    }

    fn seed_priority(&self) -> Option<u8> {
        Some(1)
    }

    async fn run_seeded(&mut self, platform: &Platform, seed: &PageList) -> Result<PageList> {
        // Other wikis are converted by the combination layer; their titles are
        // not item IDs.
        if seed.wiki().as_deref() != Some("wikidatawiki") {
            return self.run(platform).await;
        }
        let ids = item_ids_from_seed(seed);
        if ids.is_empty() {
            return Ok(PageList::new_from_wiki("wikidatawiki"));
        }
        if ids.len() > MAX_SEED_PAGES {
            return self.run(platform).await;
        }
        let mut sql = Self::generate_sql_query(platform)?;
        sql.0 += " AND ips_item_id IN (";
        sql.0 += &super::get_placeholders(ids.len());
        sql.0 += ")";
        sql.1.extend(ids.into_iter().map(MyValue::UInt));
        self.run_sql_query(sql, platform).await
    }
}

impl SourceWikidata {
    fn generate_sql_query(platform: &Platform) -> Result<SQLtuple> {
        let no_statements = platform.has_param("wpiu_no_statements");
        let sites = platform
            .get_param("wikidata_source_sites")
            .ok_or_else(|| anyhow!("Missing parameter 'wikidata_source_sites'"))?;
        let sites: Vec<String> = sites.split(',').map(|s| s.to_string()).collect();
        if sites.is_empty() {
            return Err(anyhow!("SourceWikidata: No wikidata source sites given"));
        }

        let sites = super::prep_quote(&sites);

        let mut sql = SQLtuple(
            "SELECT DISTINCT ips_item_id FROM wb_items_per_site".to_string(),
            vec![],
        );
        if no_statements {
            sql.0 += ",page_props,page";
        }
        sql.0 += " WHERE ips_site_id IN (";
        sql.0 += &sites.0;
        sql.0 += ")";
        sql.1 = sites.1;
        if no_statements {
            sql.0 += " AND page_namespace=0 AND ips_item_id=substr(page_title,2)*1 AND page_id=pp_page AND pp_propname='wb-claims' AND pp_sortkey=0";
        }
        Ok(sql)
    }

    async fn run_sql_query(&self, mut sql: SQLtuple, platform: &Platform) -> Result<PageList> {
        let limit = platform.state().max_result_entries();
        // Bound the query at the database, not after all rows are in memory:
        // a source-site filter can match millions of items.
        crate::datasource::append_limit(&mut sql, limit);
        // Perform DB query
        let mut conn = platform
            .state()
            .get_wiki_db_connection("wikidatawiki")
            .await?;
        let rows = conn
            .exec_iter(
                sql.0.as_str(),
                wikimisc::mysql_async::Params::Positional(sql.1),
            )
            .await
            .map_err(|e| anyhow!(e))?
            .map_and_drop(from_row::<usize>)
            .await
            .map_err(|e| anyhow!(e))?;
        // `conn` is pooled; drop returns it.
        drop(conn);
        if rows.len() > limit {
            return Err(crate::pagelist::too_many_results(
                "this Wikidata source query yields",
                limit,
            ));
        }
        let ret = PageList::new_from_wiki("wikidatawiki");
        for ips_item_id in rows {
            let term_full_entity_id = format!("Q{ips_item_id}");
            if let Some(entry) = Platform::entry_from_entity(&term_full_entity_id) {
                ret.add_entry(entry);
            }
        }
        Ok(ret)
    }
}

/// Numeric IDs of the `Q…` titles in `seed`, ignoring other namespaces and
/// non-item titles.
fn item_ids_from_seed(seed: &PageList) -> Vec<u64> {
    seed.to_titles_namespaces()
        .into_iter()
        .filter(|(_, namespace_id)| *namespace_id == 0)
        .filter_map(|(title, _)| title.strip_prefix('Q')?.parse::<u64>().ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::make_platform;

    // ── can_run / name ───────────────────────────────────────────────────────

    #[test]
    fn test_name() {
        assert_eq!(SourceWikidata.name(), "wikidata");
    }

    #[test]
    fn test_can_run_requires_both_params() {
        let p = make_platform(vec![
            ("wpiu_no_statements", "1"),
            ("wikidata_source_sites", "enwiki"),
        ]);
        assert!(SourceWikidata.can_run(&p));
    }

    #[test]
    fn test_can_run_missing_no_statements() {
        let p = make_platform(vec![("wikidata_source_sites", "enwiki")]);
        assert!(!SourceWikidata.can_run(&p));
    }

    #[test]
    fn test_can_run_missing_source_sites() {
        let p = make_platform(vec![("wpiu_no_statements", "1")]);
        assert!(!SourceWikidata.can_run(&p));
    }

    // ── generate_sql_query ───────────────────────────────────────────────────

    // Exact-string SQL asserts rather than `contains(...)`. The audit
    // (P2 #18) flagged the older substring-match form: it would have let
    // a structurally malformed query (missing `WHERE`, wrong join order,
    // duplicate clauses) silently pass. Pinning the full string forces
    // any builder change to also update the test, surfacing intent.

    #[test]
    fn test_generate_sql_query_basic_two_sites() {
        let p = make_platform(vec![("wikidata_source_sites", "enwiki,frwiki")]);
        let sql = SourceWikidata::generate_sql_query(&p).unwrap();
        assert_eq!(
            sql.0,
            "SELECT DISTINCT ips_item_id FROM wb_items_per_site WHERE ips_site_id IN (?,?)"
        );
        assert_eq!(params_as_strings(&sql.1), vec!["enwiki", "frwiki"]);
    }

    #[test]
    fn test_generate_sql_query_basic_single_site() {
        let p = make_platform(vec![("wikidata_source_sites", "enwiki")]);
        let sql = SourceWikidata::generate_sql_query(&p).unwrap();
        assert_eq!(
            sql.0,
            "SELECT DISTINCT ips_item_id FROM wb_items_per_site WHERE ips_site_id IN (?)"
        );
        assert_eq!(params_as_strings(&sql.1), vec!["enwiki"]);
    }

    #[test]
    fn test_generate_sql_query_no_statements_adds_joins() {
        let p = make_platform(vec![
            ("wpiu_no_statements", "1"),
            ("wikidata_source_sites", "enwiki"),
        ]);
        let sql = SourceWikidata::generate_sql_query(&p).unwrap();
        assert_eq!(
            sql.0,
            "SELECT DISTINCT ips_item_id FROM wb_items_per_site,page_props,page \
             WHERE ips_site_id IN (?) \
             AND page_namespace=0 \
             AND ips_item_id=substr(page_title,2)*1 \
             AND page_id=pp_page \
             AND pp_propname='wb-claims' \
             AND pp_sortkey=0"
        );
        assert_eq!(params_as_strings(&sql.1), vec!["enwiki"]);
    }

    #[test]
    fn test_generate_sql_query_missing_sites_returns_error() {
        let p = make_platform(vec![("wpiu_no_statements", "1")]);
        assert!(SourceWikidata::generate_sql_query(&p).is_err());
    }

    /// Defense in depth: even if a caller passes a hostile site name, the
    /// site list goes through `prep_quote` which emits `?` placeholders and
    /// puts the raw values into the parameter Vec — they should never
    /// reach the SQL string itself. We assert that here so a future
    /// refactor cannot accidentally interpolate them.
    #[test]
    fn test_generate_sql_query_does_not_inline_site_names() {
        let p = make_platform(vec![(
            "wikidata_source_sites",
            "enwiki,'); DROP TABLE page;--",
        )]);
        let sql = SourceWikidata::generate_sql_query(&p).unwrap();
        assert!(
            !sql.0.contains("DROP"),
            "hostile site name leaked into SQL: {}",
            sql.0
        );
        assert!(
            !sql.0.contains("enwiki"),
            "site name leaked into SQL string instead of params: {}",
            sql.0
        );
        assert_eq!(
            params_as_strings(&sql.1),
            vec!["enwiki", "'); DROP TABLE page;--"]
        );
    }

    #[test]
    fn item_ids_from_seed_keeps_only_q_items_in_main_namespace() {
        use crate::pagelist_entry::PageListEntry;
        use std::collections::BTreeSet;
        use wikimisc::mediawiki::title::Title;

        let seed = PageList::new_from_wiki("wikidatawiki");
        for (title, namespace_id) in [
            ("Q1", 0),
            ("Q42", 0),
            ("P31", 0),
            ("Q2", 120),
            ("Not an id", 0),
            ("Qnotanumber", 0),
        ] {
            seed.add_entry(PageListEntry::new(Title::new(title, namespace_id)));
        }

        let ids: BTreeSet<u64> = item_ids_from_seed(&seed).into_iter().collect();
        assert_eq!(ids, BTreeSet::from([1, 42]));
    }

    fn params_as_strings(params: &[MyValue]) -> Vec<String> {
        params
            .iter()
            .map(|value| match value {
                MyValue::Bytes(bytes) => String::from_utf8_lossy(bytes).to_string(),
                other => format!("{other:?}"),
            })
            .collect()
    }
}
