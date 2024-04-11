use crate::datasource::DataSource;
use crate::pagelist::*;
use crate::pagelist_entry::PageListEntry;
use crate::platform::Platform;
use async_trait::async_trait;
use rayon::prelude::*;
use wikimisc::mediawiki::api::Api;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct SourceSearch {}

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

    async fn run(&mut self, platform: &Platform) -> Result<PageList, String> {
        let wiki = platform
            .get_param("search_wiki")
            .ok_or_else(|| "Missing parameter \'search_wiki\'".to_string())?;
        let query = platform
            .get_param("search_query")
            .ok_or_else(|| "Missing parameter \'search_query\'".to_string())?;
        let max = match platform
            .get_param("search_max_results")
            .ok_or_else(|| "Missing parameter \'search_max_results\'".to_string())?
            .parse::<usize>()
        {
            Ok(max) => max,
            Err(e) => return Err(format!("{:?}", e)),
        };
        let api = platform.state().get_api_for_wiki(wiki.to_string()).await?;
        let srlimit = if max > 500 { 500 } else { max };
        let srlimit = format!("{}", srlimit);
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
            Err(e) => return Err(format!("{:?}", e)),
        };
        let titles = Api::result_array_to_titles(&result);
        let ret = PageList::new_from_wiki(&wiki);
        titles
            .iter()
            .map(|title| PageListEntry::new(title.to_owned()))
            .for_each(|entry| ret.add_entry(entry).unwrap_or(()));
        if ret.is_empty()? {
            platform.warn("<span tt=\'warn_search\'></span>".to_string())?;
        }
        Ok(ret)
    }
}

impl SourceSearch {
    pub fn new() -> Self {
        Self {}
    }
}
