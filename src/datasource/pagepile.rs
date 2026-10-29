use crate::datasource::DataSource;
use crate::pagelist::PageList;
use crate::pagelist_entry::PageListEntry;
use crate::platform::Platform;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use pagepile::client::{Client, Query};
use std::sync::LazyLock;
use wikimisc::mediawiki::title::Title;

/// One client for all `PagePile` requests, for its connection pool.
static CLIENT: LazyLock<Result<Client, String>> =
    LazyLock::new(|| Client::new().map_err(|e| e.to_string()));

/// The `PagePile` client.
pub fn client() -> Result<&'static Client> {
    CLIENT.as_ref().map_err(|e| anyhow!("PagePile client: {e}"))
}

#[derive(Debug, Clone, PartialEq, Default, Copy)]
pub struct SourcePagePile;

#[async_trait]
impl DataSource for SourcePagePile {
    fn name(&self) -> String {
        "pagepile".to_string()
    }

    fn can_run(&self, platform: &Platform) -> bool {
        platform.has_param("pagepile")
    }

    async fn run(&mut self, platform: &Platform) -> Result<PageList> {
        let param = platform
            .get_param("pagepile")
            .ok_or_else(|| anyhow!("Missing parameter 'pagepile'"))?;
        let id: u64 = param
            .trim()
            .parse()
            .map_err(|_| anyhow!("Invalid PagePile ID '{param}'"))?;
        let limit = platform.state().max_result_entries();
        // One more than allowed, to tell a pile that is too large without loading it all.
        let query = Query {
            limit: Some(limit as u64 + 1),
            ..Default::default()
        };
        let pile = client()?
            .pile_with(id, &query)
            .await
            .map_err(|e| anyhow!("PagePile fetch failed: {e}"))?;
        if pile.pages_total > limit as u64 {
            return Err(crate::pagelist::too_many_results(
                &format!("PagePile {id} holds"),
                limit,
            ));
        }
        let ret = PageList::new_from_wiki(&pile.wiki);
        pile.pages
            .iter()
            .map(|page| PageListEntry::new(Title::new(&page.text(), page.namespace)))
            .for_each(|entry| ret.add_entry(entry));
        if ret.is_empty() {
            platform.warn("<span tt=\'warn_pagepile\'></span>".to_string())?;
        }
        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::make_platform;

    #[test]
    fn test_name() {
        assert_eq!(SourcePagePile.name(), "pagepile");
    }

    #[test]
    fn test_can_run_with_pagepile_param() {
        let p = make_platform(vec![("pagepile", "12345")]);
        assert!(SourcePagePile.can_run(&p));
    }

    #[test]
    fn test_can_run_without_pagepile_param() {
        let p = make_platform(vec![]);
        assert!(!SourcePagePile.can_run(&p));
    }
}
