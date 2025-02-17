use crate::datasource::DataSource;
use crate::pagelist::*;
use crate::pagelist_entry::PageListEntry;
use crate::platform::Platform;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use wikimisc::mediawiki::title::Title;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct SourceManual {}

#[async_trait]
impl DataSource for SourceManual {
    fn name(&self) -> String {
        "manual".to_string()
    }

    fn can_run(&self, platform: &Platform) -> bool {
        platform.has_param("manual_list") && platform.has_param("manual_list_wiki")
    }

    async fn run(&mut self, platform: &Platform) -> Result<PageList> {
        let wiki = platform
            .get_param("manual_list_wiki")
            .ok_or_else(|| anyhow!("Missing parameter \'manual_list_wiki\'"))?;
        let api = platform.state().get_api_for_wiki(wiki.to_string()).await?;
        let ret = PageList::new_from_wiki(&wiki);
        platform
            .get_param("manual_list")
            .ok_or_else(|| anyhow!("Missing parameter \'manual_list\'"))?
            .split('\n')
            .filter_map(|line| {
                let line = line.trim().to_string();
                if !line.is_empty() {
                    let title = Title::new_from_full(&line, &api);
                    let entry = PageListEntry::new(title);
                    Some(entry)
                } else {
                    None
                }
            })
            .for_each(|entry| ret.add_entry(entry).unwrap_or(()));
        Ok(ret)
    }
}

impl SourceManual {
    pub fn new() -> Self {
        Self {}
    }
}
