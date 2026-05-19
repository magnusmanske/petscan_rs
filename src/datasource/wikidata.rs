use crate::datasource::DataSource;
use crate::pagelist::PageList;
use crate::platform::Platform;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use mysql_async::from_row;
use mysql_async::prelude::Queryable;

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
        self.run_sql_query(&sql, platform).await
    }
}

impl SourceWikidata {
    fn generate_sql_query(platform: &Platform) -> Result<String> {
        let no_statements = platform.has_param("wpiu_no_statements");
        let sites = platform
            .get_param("wikidata_source_sites")
            .ok_or_else(|| anyhow!("Missing parameter 'wikidata_source_sites'"))?;
        let sites: Vec<String> = sites.split(',').map(|s| s.to_string()).collect();
        if sites.is_empty() {
            return Err(anyhow!("SourceWikidata: No wikidata source sites given"));
        }

        let sites = super::prep_quote(&sites);

        let mut sql = "SELECT ips_item_id FROM wb_items_per_site".to_string();
        if no_statements {
            sql += ",page_props,page";
        }
        sql += " WHERE ips_site_id IN (";
        sql += &sites.0;
        sql += ")";
        if no_statements {
            sql += " AND page_namespace=0 AND ips_item_id=substr(page_title,2)*1 AND page_id=pp_page AND pp_propname='wb-claims' AND pp_sortkey=0" ;
        }
        Ok(sql)
    }

    async fn run_sql_query(&self, sql: &str, platform: &Platform) -> Result<PageList> {
        // Perform DB query
        let mut conn = platform
            .state()
            .get_wiki_db_connection("wikidatawiki")
            .await?;
        let rows = conn
            .exec_iter(sql, ())
            .await
            .map_err(|e| anyhow!(e))?
            .map_and_drop(from_row::<usize>)
            .await
            .map_err(|e| anyhow!(e))?;
        // `conn` is pooled; drop returns it.
        drop(conn);
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
            sql,
            "SELECT ips_item_id FROM wb_items_per_site WHERE ips_site_id IN (?,?)"
        );
    }

    #[test]
    fn test_generate_sql_query_basic_single_site() {
        let p = make_platform(vec![("wikidata_source_sites", "enwiki")]);
        let sql = SourceWikidata::generate_sql_query(&p).unwrap();
        assert_eq!(
            sql,
            "SELECT ips_item_id FROM wb_items_per_site WHERE ips_site_id IN (?)"
        );
    }

    #[test]
    fn test_generate_sql_query_no_statements_adds_joins() {
        let p = make_platform(vec![
            ("wpiu_no_statements", "1"),
            ("wikidata_source_sites", "enwiki"),
        ]);
        let sql = SourceWikidata::generate_sql_query(&p).unwrap();
        assert_eq!(
            sql,
            "SELECT ips_item_id FROM wb_items_per_site,page_props,page \
             WHERE ips_site_id IN (?) \
             AND page_namespace=0 \
             AND ips_item_id=substr(page_title,2)*1 \
             AND page_id=pp_page \
             AND pp_propname='wb-claims' \
             AND pp_sortkey=0"
        );
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
            !sql.contains("DROP"),
            "hostile site name leaked into SQL: {sql}"
        );
        assert!(
            !sql.contains("enwiki"),
            "site name leaked into SQL string instead of params: {sql}"
        );
    }
}
