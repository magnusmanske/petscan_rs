use crate::datasource::DataSource;
use crate::pagelist::*;
use crate::platform::Platform;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use mysql_async::from_row;
use mysql_async::prelude::Queryable;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct SourceLabels {}

#[async_trait]
impl DataSource for SourceLabels {
    fn name(&self) -> String {
        "labels".to_string()
    }

    fn can_run(&self, platform: &Platform) -> bool {
        platform.has_param("labels_yes") || platform.has_param("labels_any")
    }

    async fn run(&mut self, platform: &Platform) -> Result<PageList> {
        let sql = platform.get_label_sql();
        let mut conn = platform
            .state()
            .get_wiki_db_connection("wikidatawiki")
            .await?;
        let rows = conn
            .exec_iter(sql.0.as_str(), mysql_async::Params::Positional(sql.1))
            .await
            .map_err(|e| anyhow!(e))?
            .map_and_drop(from_row::<(Vec<u8>,)>)
            .await
            .map_err(|e| anyhow!(e))?;
        conn.disconnect().await.map_err(|e| anyhow!(e))?;
        let ret = PageList::new_from_wiki_with_capacity("wikidatawiki", rows.len());
        rows.iter()
            .map(|row| String::from_utf8_lossy(&row.0))
            .filter_map(|item| Platform::entry_from_entity(&item))
            .for_each(|entry| ret.add_entry(entry).unwrap_or(()));
        Ok(ret)
    }
}

impl SourceLabels {
    pub fn new() -> Self {
        Self {}
    }
}
