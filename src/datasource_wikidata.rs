use crate::datasource::DataSource;
use crate::pagelist::*;
use crate::platform::Platform;
use async_trait::async_trait;
use mysql_async::from_row;
use mysql_async::prelude::Queryable;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct SourceWikidata {}

#[async_trait]
impl DataSource for SourceWikidata {
    fn name(&self) -> String {
        "wikidata".to_string()
    }

    fn can_run(&self, platform: &Platform) -> bool {
        platform.has_param("wpiu_no_statements") && platform.has_param("wikidata_source_sites")
    }

    async fn run(&mut self, platform: &Platform) -> Result<PageList, String> {
        let no_statements = platform.has_param("wpiu_no_statements");
        let sites = platform
            .get_param("wikidata_source_sites")
            .ok_or_else(|| "Missing parameter \'wikidata_source_sites\'".to_string())?;
        let sites: Vec<String> = sites.split(',').map(|s| s.to_string()).collect();
        if sites.is_empty() {
            return Err("SourceWikidata: No wikidata source sites given".to_string());
        }

        let sites = Platform::prep_quote(&sites);

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

        // Perform DB query
        let mut conn = platform
            .state()
            .get_wiki_db_connection("wikidatawiki")
            .await?;
        let rows = conn
            .exec_iter(sql.as_str(), ())
            .await
            .map_err(|e| format!("{:?}", e))?
            .map_and_drop(from_row::<usize>)
            .await
            .map_err(|e| format!("{:?}", e))?;
        conn.disconnect().await.map_err(|e| format!("{:?}", e))?;
        let ret = PageList::new_from_wiki("wikidatawiki");
        for ips_item_id in rows {
            let term_full_entity_id = format!("Q{}", ips_item_id);
            if let Some(entry) = Platform::entry_from_entity(&term_full_entity_id) {
                ret.add_entry(entry).unwrap_or(());
            }
        }
        Ok(ret)
    }
}

impl SourceWikidata {
    pub fn new() -> Self {
        Self {}
    }
}
