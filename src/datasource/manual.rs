use crate::datasource::DataSource;
use crate::pagelist::PageList;
use crate::pagelist_entry::PageListEntry;
use crate::platform::Platform;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use wikimisc::mediawiki::title::Title;

#[derive(Debug, Clone, PartialEq, Default, Copy)]
pub struct SourceManual;

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
            .for_each(|entry| ret.add_entry(entry));
        Ok(ret)
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

    #[test]
    fn test_can_run_requires_both_params() {
        let src = SourceManual;
        let platform_both = make_platform(vec![
            ("manual_list", "Article one\nArticle two"),
            ("manual_list_wiki", "enwiki"),
        ]);
        assert!(src.can_run(&platform_both));
    }

    #[test]
    fn test_can_run_missing_manual_list() {
        let src = SourceManual;
        let platform = make_platform(vec![("manual_list_wiki", "enwiki")]);
        assert!(!src.can_run(&platform));
    }

    #[test]
    fn test_can_run_missing_manual_list_wiki() {
        let src = SourceManual;
        let platform = make_platform(vec![("manual_list", "Article one")]);
        assert!(!src.can_run(&platform));
    }

    #[test]
    fn test_can_run_missing_both() {
        let src = SourceManual;
        let platform = make_platform(vec![]);
        assert!(!src.can_run(&platform));
    }

    #[test]
    fn test_name() {
        assert_eq!(SourceManual.name(), "manual");
    }
}
