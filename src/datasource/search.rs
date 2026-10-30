use crate::datasource::DataSource;
use crate::pagelist::PageList;
use crate::pagelist_entry::PageListEntry;
use crate::platform::Platform;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use std::collections::HashSet;
use wikimisc::mediawiki::api::Api;

#[derive(Debug, Clone, PartialEq, Default, Copy)]
pub struct SourceSearch;

#[async_trait]
impl DataSource for SourceSearch {
    fn name(&self) -> String {
        "search".to_string()
    }

    fn can_run(&self, platform: &Platform) -> bool {
        platform.has_param("search_query")
            && platform.has_param("search_wiki")
            && platform.has_param("search_max_results")
            && !platform.is_param_blank("search_query")
            && !platform.is_param_blank("search_wiki")
    }

    async fn run(&mut self, platform: &Platform) -> Result<PageList> {
        let wiki = platform
            .get_param("search_wiki")
            .ok_or_else(|| anyhow!("Missing parameter 'search_wiki'"))?;
        let query = platform
            .get_param("search_query")
            .ok_or_else(|| anyhow!("Missing parameter 'search_query'"))?;
        let max = match platform
            .get_param("search_max_results")
            .ok_or_else(|| anyhow!("Missing parameter 'search_max_results'"))?
            .parse::<usize>()
        {
            Ok(max) => max,
            Err(e) => return Err(anyhow!(e)),
        };
        let api = platform.state().get_api_for_wiki(wiki.to_string()).await?;
        let srlimit = if max > 500 { 500 } else { max };
        let srlimit = srlimit.to_string();
        let namespace_ids = srnamespace(&platform.form_parameters().ns);
        let params = api.params_into(&[
            ("action", "query"),
            ("list", "search"),
            ("srlimit", srlimit.as_str()),
            ("srsearch", query.as_str()),
            ("srnamespace", namespace_ids.as_str()),
        ]);
        let result = match api.get_query_api_json_limit(&params, Some(max)).await {
            Ok(result) => result,
            Err(e) => return Err(anyhow!(e)),
        };
        let titles = Api::result_array_to_titles(&result);
        let ret = PageList::new_from_wiki(&wiki);
        titles
            .iter()
            .map(|title| PageListEntry::new(title.to_owned()))
            .for_each(|entry| ret.add_entry(entry));
        if ret.is_empty() {
            platform.warn("<span tt=\'warn_search\'></span>".to_string())?;
        }
        Ok(ret)
    }
}

/// The `srnamespace` value searching these namespaces, or all of them if
/// there are none. MediaWiki separates multiple values with `|`; given
/// anything else, it ignores the parameter and searches the wiki's default
/// namespaces instead.
fn srnamespace(namespace_ids: &HashSet<usize>) -> String {
    if namespace_ids.is_empty() {
        return "*".to_string();
    }
    let mut ids: Vec<usize> = namespace_ids.iter().copied().collect();
    ids.sort_unstable();
    ids.iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join("|")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::make_platform;

    #[test]
    fn test_name() {
        assert_eq!(SourceSearch.name(), "search");
    }

    #[test]
    fn test_srnamespace() {
        assert_eq!(srnamespace(&HashSet::new()), "*");
        assert_eq!(srnamespace(&HashSet::from([6])), "6");
        assert_eq!(srnamespace(&HashSet::from([14, 0, 6])), "0|6|14");
    }

    #[test]
    fn test_can_run_all_required_params() {
        let p = make_platform(vec![
            ("search_query", "test query"),
            ("search_wiki", "enwiki"),
            ("search_max_results", "50"),
        ]);
        assert!(SourceSearch.can_run(&p));
    }

    #[test]
    fn test_can_run_missing_query() {
        let p = make_platform(vec![
            ("search_wiki", "enwiki"),
            ("search_max_results", "50"),
        ]);
        assert!(!SourceSearch.can_run(&p));
    }

    #[test]
    fn test_can_run_missing_wiki() {
        let p = make_platform(vec![
            ("search_query", "test query"),
            ("search_max_results", "50"),
        ]);
        assert!(!SourceSearch.can_run(&p));
    }

    #[test]
    fn test_can_run_missing_max_results() {
        let p = make_platform(vec![
            ("search_query", "test query"),
            ("search_wiki", "enwiki"),
        ]);
        assert!(!SourceSearch.can_run(&p));
    }

    #[test]
    fn test_can_run_blank_query_not_allowed() {
        // Empty search_query should not satisfy can_run even if key is present
        let p = make_platform(vec![
            ("search_query", "   "),
            ("search_wiki", "enwiki"),
            ("search_max_results", "50"),
        ]);
        assert!(!SourceSearch.can_run(&p));
    }
}
