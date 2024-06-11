use crate::datasource::{DataSource, SQLtuple};
use crate::pagelist::*;
use crate::pagelist_entry::PageListEntry;
use crate::platform::Platform;
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

    async fn run(&mut self, platform: &Platform) -> Result<PageList, String> {
        let sql = self.generate_sql_query(platform)?;
        let rows = self.get_result_rows(platform, sql).await?;
        let ret = PageList::new_from_wiki_with_capacity(&self.main_wiki, rows.len());
        if self.use_min_max {
            ret.set_has_sitelink_counts(true)?;
        }
        rows.iter()
            .map(|row| (String::from_utf8_lossy(&row.0), row.1))
            .map(|(page, sitelinks)| {
                let mut ret = PageListEntry::new(Title::new(&page, 0));
                if self.use_min_max {
                    ret.set_sitelink_count(Some(sitelinks));
                }
                ret
            })
            .for_each(|entry| ret.add_entry(entry).unwrap_or(()));
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

    fn generate_sql_query(&mut self, platform: &Platform) -> Result<SQLtuple, String> {
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
            None => return Err("No yes/any sitelink found in SourceSitelinks::run".to_string()),
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
            let tmp = Platform::prep_quote(&sitelinks_any);
            Platform::append_sql(&mut sql, tmp);
            sql.0 += "))";
        }
        if !sitelinks_no.is_empty() {
            sql.0 += " AND page_id NOT IN (SELECT ll_from FROM langlinks WHERE ll_lang IN (";
            let tmp = Platform::prep_quote(&sitelinks_no);
            Platform::append_sql(&mut sql, tmp);
            sql.0 += "))";
        }

        let mut having: Vec<String> = vec![];
        if let Ok(s) = sitelinks_min.parse::<usize>() {
            having.push(format!("sitelink_count>={}", s))
        }
        if let Ok(s) = sitelinks_max.parse::<usize>() {
            having.push(format!("sitelink_count<={}", s))
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
    ) -> Result<Vec<(Vec<u8>, u32)>, String> {
        let mut conn = platform
            .state()
            .get_wiki_db_connection(&self.main_wiki)
            .await?;
        let rows = conn
            .exec_iter(sql.0.as_str(), mysql_async::Params::Positional(sql.1))
            .await
            .map_err(|e| format!("{:?}", e))?
            .map_and_drop(from_row::<(Vec<u8>, u32)>)
            .await
            .map_err(|e| format!("{:?}", e))?;
        conn.disconnect().await.map_err(|e| format!("{:?}", e))?;
        Ok(rows)
    }
}
