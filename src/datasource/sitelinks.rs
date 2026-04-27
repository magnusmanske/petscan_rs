use crate::datasource::{DataSource, SQLtuple};
use crate::pagelist::PageList;
use crate::pagelist_entry::PageListEntry;
use crate::platform::Platform;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use mysql_async::from_row;
use mysql_async::prelude::Queryable;
use wikimisc::mediawiki::title::Title;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct SourceSitelinks {
    main_wiki: String,
    use_min_max: bool,
}

#[async_trait]
impl DataSource for SourceSitelinks {
    fn name(&self) -> String {
        "sitelinks".to_string()
    }

    fn can_run(&self, platform: &Platform) -> bool {
        platform.has_param("sitelinks_yes") || platform.has_param("sitelinks_any")
    }

    async fn run(&mut self, platform: &Platform) -> Result<PageList> {
        let sql = self.generate_sql_query(platform)?;
        let rows = self.get_result_rows(platform, sql).await?;
        let ret = PageList::new_from_wiki_with_capacity(&self.main_wiki, rows.len());
        if self.use_min_max {
            ret.set_has_sitelink_counts(true);
        }
        rows.iter()
            .map(|row| (String::from_utf8_lossy(&row.0), row.1))
            .map(|(page, sitelinks)| {
                let mut tmp = PageListEntry::new(Title::new(&page, 0));
                if self.use_min_max {
                    tmp.set_sitelink_count(Some(sitelinks));
                }
                tmp
            })
            .for_each(|entry| ret.add_entry(entry));
        Ok(ret)
    }
}

impl SourceSitelinks {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    fn site2lang(&self, site: &str) -> Option<String> {
        if *site == self.main_wiki {
            return None;
        }
        let ret = if site.ends_with("wiki") {
            site.split_at(site.len() - 4).0.to_owned()
        } else {
            site.to_owned()
        };
        Some(ret)
    }

    fn generate_sql_query(&mut self, platform: &Platform) -> Result<SQLtuple> {
        let sitelinks_yes = platform.get_param_as_vec("sitelinks_yes", "\n");
        let sitelinks_any = platform.get_param_as_vec("sitelinks_any", "\n");
        let sitelinks_no = platform.get_param_as_vec("sitelinks_no", "\n");
        let sitelinks_min = platform.get_param_blank("min_sitelink_count");
        let sitelinks_max = platform.get_param_blank("max_sitelink_count");

        self.use_min_max = !sitelinks_min.is_empty() || !sitelinks_max.is_empty();

        let mut yes_any = vec![];
        yes_any.extend(&sitelinks_yes);
        yes_any.extend(&sitelinks_any);
        self.main_wiki = match yes_any.first() {
            Some(wiki) => wiki.to_string(),
            None => return Err(anyhow!("No yes/any sitelink found in SourceSitelinks::run")),
        };

        let sitelinks_any: Vec<String> = sitelinks_any
            .iter()
            .filter_map(|site| self.site2lang(site))
            .collect();
        let sitelinks_no: Vec<String> = sitelinks_no
            .iter()
            .filter_map(|site| self.site2lang(site))
            .collect();

        let mut sql: SQLtuple = (String::new(), vec![]);
        sql.0 += "SELECT ";
        if self.use_min_max {
            sql.0 += "page_title,(SELECT count(*) FROM langlinks WHERE ll_from=page_id) AS sitelink_count" ;
        } else {
            sql.0 += "DISTINCT page_title,0";
        }
        sql.0 += " FROM page WHERE page_namespace=0";

        sitelinks_yes
            .iter()
            .filter_map(|site| self.site2lang(site))
            .for_each(|lang| {
                sql.0 += " AND page_id IN (SELECT ll_from FROM langlinks WHERE ll_lang=?)";
                sql.1.push(lang.into());
            });
        if !sitelinks_any.is_empty() {
            sql.0 += " AND page_id IN (SELECT ll_from FROM langlinks WHERE ll_lang IN (";
            let tmp = super::prep_quote(&sitelinks_any);
            super::append_sql(&mut sql, tmp);
            sql.0 += "))";
        }
        if !sitelinks_no.is_empty() {
            sql.0 += " AND page_id NOT IN (SELECT ll_from FROM langlinks WHERE ll_lang IN (";
            let tmp = super::prep_quote(&sitelinks_no);
            super::append_sql(&mut sql, tmp);
            sql.0 += "))";
        }

        let mut having: Vec<String> = vec![];
        if let Ok(s) = sitelinks_min.parse::<usize>() {
            having.push(format!("sitelink_count>={s}"));
        }
        if let Ok(s) = sitelinks_max.parse::<usize>() {
            having.push(format!("sitelink_count<={s}"));
        }

        if self.use_min_max {
            sql.0 += " GROUP BY page_title";
        }
        if !having.is_empty() {
            sql.0 += " HAVING ";
            sql.0 += &having.join(" AND ");
        }
        Ok(sql)
    }

    async fn get_result_rows(
        &self,
        platform: &Platform,
        sql: SQLtuple,
    ) -> Result<Vec<(Vec<u8>, u32)>> {
        let mut conn = platform
            .state()
            .get_wiki_db_connection(&self.main_wiki)
            .await?;
        let rows = conn
            .exec_iter(sql.0.as_str(), mysql_async::Params::Positional(sql.1))
            .await
            .map_err(|e| anyhow!(e))?
            .map_and_drop(from_row::<(Vec<u8>, u32)>)
            .await
            .map_err(|e| anyhow!(e))?;
        conn.disconnect().await.map_err(|e| anyhow!(e))?;
        Ok(rows)
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

    // ── can_run / name ───────────────────────────────────────────────────────

    #[test]
    fn test_name() {
        assert_eq!(SourceSitelinks::new().name(), "sitelinks");
    }

    #[test]
    fn test_can_run_sitelinks_yes() {
        let src = SourceSitelinks::new();
        let p = make_platform(vec![("sitelinks_yes", "enwiki")]);
        assert!(src.can_run(&p));
    }

    #[test]
    fn test_can_run_sitelinks_any() {
        let src = SourceSitelinks::new();
        let p = make_platform(vec![("sitelinks_any", "frwiki")]);
        assert!(src.can_run(&p));
    }

    #[test]
    fn test_can_run_neither_param() {
        let src = SourceSitelinks::new();
        let p = make_platform(vec![]);
        assert!(!src.can_run(&p));
    }

    // ── site2lang ────────────────────────────────────────────────────────────

    #[test]
    fn test_site2lang_strips_wiki_suffix() {
        let src = SourceSitelinks { main_wiki: "dewiki".to_string(), use_min_max: false };
        assert_eq!(src.site2lang("enwiki"), Some("en".to_string()));
        assert_eq!(src.site2lang("frwiki"), Some("fr".to_string()));
        assert_eq!(src.site2lang("commonswiki"), Some("commons".to_string()));
    }

    #[test]
    fn test_site2lang_returns_none_for_main_wiki() {
        let src = SourceSitelinks { main_wiki: "enwiki".to_string(), use_min_max: false };
        assert_eq!(src.site2lang("enwiki"), None);
    }

    #[test]
    fn test_site2lang_no_wiki_suffix_returns_as_is() {
        let src = SourceSitelinks { main_wiki: "enwiki".to_string(), use_min_max: false };
        assert_eq!(src.site2lang("some_other_site"), Some("some_other_site".to_string()));
    }

    // ── generate_sql_query ───────────────────────────────────────────────────

    #[test]
    fn test_generate_sql_query_sitelinks_yes() {
        let mut src = SourceSitelinks::new();
        // enwiki becomes the main wiki (first); frwiki generates the ll_lang=? condition
        let p = make_platform(vec![("sitelinks_yes", "enwiki\nfrwiki")]);
        let (sql, params) = src.generate_sql_query(&p).unwrap();
        assert!(sql.contains("ll_lang=?"), "Expected ll_lang=? in: {sql}");
        assert_eq!(params.len(), 1); // only "fr"; enwiki is the main wiki
        assert_eq!(src.main_wiki, "enwiki");
    }

    #[test]
    fn test_generate_sql_query_sitelinks_any() {
        let mut src = SourceSitelinks::new();
        // enwiki is main wiki; fr and de generate the IN (?,?) condition
        let p = make_platform(vec![("sitelinks_any", "enwiki\nfrwiki\ndewiki")]);
        let (sql, params) = src.generate_sql_query(&p).unwrap();
        assert!(sql.contains("ll_lang IN ("), "Expected ll_lang IN in: {sql}");
        assert_eq!(params.len(), 2); // "fr" and "de"; enwiki filtered out as main wiki
    }

    #[test]
    fn test_generate_sql_query_no_sitelinks_returns_error() {
        let mut src = SourceSitelinks::new();
        let p = make_platform(vec![]);
        assert!(src.generate_sql_query(&p).is_err());
    }

    #[test]
    fn test_generate_sql_query_min_max_adds_having() {
        let mut src = SourceSitelinks::new();
        let p = make_platform(vec![
            ("sitelinks_yes", "enwiki"),
            ("min_sitelink_count", "10"),
            ("max_sitelink_count", "100"),
        ]);
        let (sql, _) = src.generate_sql_query(&p).unwrap();
        assert!(sql.contains("HAVING"), "Expected HAVING in: {sql}");
        assert!(sql.contains("sitelink_count>=10"), "Expected sitelink_count>=10 in: {sql}");
        assert!(sql.contains("sitelink_count<=100"), "Expected sitelink_count<=100 in: {sql}");
    }
}
