use crate::datasource::DataSource;
use crate::pagelist::*;
use crate::pagelist_entry::PageListEntry;
use crate::platform::Platform;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde_json::value::Value;
use std::time;
use wikimisc::mediawiki::api::Api;
use wikimisc::mediawiki::title::Title;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct SourcePagePile {}

#[async_trait]
impl DataSource for SourcePagePile {
    fn name(&self) -> String {
        "pagepile".to_string()
    }

    fn can_run(&self, platform: &Platform) -> bool {
        platform.has_param("pagepile")
    }

    async fn run(&mut self, platform: &Platform) -> Result<PageList> {
        let pagepile = platform
            .get_param("pagepile")
            .ok_or_else(|| anyhow!("Missing parameter 'pagepile'"))?;
        let v = self.get_pagepile_json(&pagepile).await?;
        let wiki = v["wiki"]
            .as_str()
            .ok_or(anyhow!("PagePile {pagepile} does not specify a wiki"))?;
        let api = platform.state().get_api_for_wiki(wiki.to_string()).await?; // Just because we need query_raw
        let ret = PageList::new_from_wiki(wiki);
        v["pages"]
            .as_array()
            .ok_or(anyhow!("PagePile {pagepile} does not have a 'pages' array"))?
            .iter()
            .filter_map(|title| title.as_str())
            .map(|title| PageListEntry::new(Title::new_from_full(title, &api)))
            .for_each(|entry| ret.add_entry(entry));
        if ret.is_empty() {
            platform.warn("<span tt=\'warn_pagepile\'></span>".to_string())?;
        }
        Ok(ret)
    }
}

impl SourcePagePile {
    pub fn new() -> Self {
        Self {}
    }

    async fn get_pagepile_json(&self, pagepile: &str) -> Result<Value> {
        let timeout = time::Duration::from_secs(240);
        let builder = reqwest::ClientBuilder::new().timeout(timeout);
        let api = Api::new_from_builder("https://www.wikidata.org/w/api.php", builder).await?;
        let params = api.params_into(&[
            ("id", pagepile),
            ("action", "get_data"),
            ("format", "json"),
            ("doit", "1"),
        ]);
        let text = api
            .query_raw("https://tools.wmflabs.org/pagepile/api.php", &params, "GET")
            .await
            .map_err(|e| anyhow!("PagePile: {e}"))?;
        let v: Value = serde_json::from_str(&text).map_err(|e| anyhow!("PagePile JSON: {e}"))?;
        Ok(v)
    }
}
