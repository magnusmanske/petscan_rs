use crate::datasource::DataSource;
use crate::pagelist::PageList;
use crate::pagelist_entry::PageListEntry;
use crate::platform::Platform;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use rayon::prelude::*;
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
        let namespace_ids = platform
            .form_parameters()
            .ns
            .par_iter()
            .cloned()
            .collect::<Vec<usize>>();
        let namespace_ids = if namespace_ids.is_empty() {
            "*".to_string()
        } else {
            namespace_ids
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<String>>()
                .join(",")
        };
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
    fn test_name() {
        assert_eq!(SourceSearch.name(), "search");
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
