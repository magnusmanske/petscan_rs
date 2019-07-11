use crate::pagelist::*;
use crate::platform::Platform;
use mediawiki::api::Api;
use mediawiki::title::Title;
use rayon::prelude::*;
use serde_json::value::Value;

pub trait DataSource {
    fn can_run(&self, platform: &Platform) -> bool;
    fn run(&self, platform: &Platform) -> Option<PageList>;
    fn name(&self) -> String;
}

// TODO
// SourceLabels

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone, PartialEq)]
pub struct SourceWikidata {}

impl DataSource for SourceWikidata {
    fn name(&self) -> String {
        "wikidata".to_string()
    }

    fn can_run(&self, _platform: &Platform) -> bool {
        true
    }

    fn run(&self, platform: &Platform) -> Option<PageList> {
        let no_statements = platform.form_parameters().wpiu_no_statements.is_some();
        let _no_sitelinks = platform.form_parameters().wpiu_no_sitelinks.is_some();
        let sites = "".to_string();
        let _label_language = platform
            .form_parameters()
            .wikidata_label_language
            .as_ref()?; // .or(platform.form_parameters().interface_language.as_ref())
        let _prop_item_use = platform.form_parameters().wikidata_prop_item_use.as_ref()?;
        let _wpiu = platform
            .form_parameters()
            .wpiu
            .as_ref()
            .unwrap_or(&"any".to_string());
        let _lock = platform.state.get_db_mutex().lock().unwrap(); // Force DB connection placeholder
        let _conn = platform
            .state
            .get_wiki_db_connection(&"wikidatawiki".to_string());

        let mut sql = "SELECT ips_item_id FROM wb_items_per_site".to_string();
        if no_statements {
            sql += ",page_props,page";
        }
        sql += " WHERE ips_site_id IN (";
        sql += &sites; // TODO
        sql += ")";
        if no_statements {
            sql += " AND page_namespace=0 AND ips_item_id=substr(page_title,2)*1 AND page_id=pp_page AND pp_propname='wb-claims' AND pp_sortkey=0" ;
        }

        println!("{}", &sql);

        None
    }
}

impl SourceWikidata {
    pub fn new() -> Self {
        Self {}
    }
}

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone, PartialEq)]
pub struct SourcePagePile {}

impl DataSource for SourcePagePile {
    fn name(&self) -> String {
        "pagepile".to_string()
    }

    fn can_run(&self, platform: &Platform) -> bool {
        platform.form_parameters().pagepile.is_some()
    }

    fn run(&self, platform: &Platform) -> Option<PageList> {
        let pagepile = platform.form_parameters().pagepile?;
        let api = platform
            .state
            .get_api_for_wiki("wikidatawiki".to_string())?; // Just because we need query_raw
        let params = api.params_into(&vec![
            ("id", &pagepile.to_string()),
            ("action", "get_data"),
            ("format", "json"),
            ("doit", "1"),
        ]);
        let text = api
            .query_raw("https://tools.wmflabs.org/pagepile/api.php", &params, "GET")
            .ok()?;
        let v: Value = serde_json::from_str(&text).ok()?;
        let wiki = v["wiki"].as_str()?;
        let api = platform.state.get_api_for_wiki(wiki.to_string())?; // Just because we need query_raw
        let entries = v["pages"]
            .as_array()?
            .iter()
            .filter_map(|title| title.as_str())
            .map(|title| PageListEntry::new(Title::new_from_full(&title.to_string(), &api)))
            .collect();
        let pagelist = PageList::new_from_vec(wiki, entries);
        Some(pagelist)
    }
}

impl SourcePagePile {
    pub fn new() -> Self {
        Self {}
    }
}

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone, PartialEq)]
pub struct SourceSearch {}

impl DataSource for SourceSearch {
    fn name(&self) -> String {
        "search".to_string()
    }

    fn can_run(&self, platform: &Platform) -> bool {
        if platform.form_parameters().search_query.is_none()
            || platform.form_parameters().search_wiki.is_none()
            || platform.form_parameters().search_max_results.is_none()
        {
            return false;
        }
        true
    }

    fn run(&self, platform: &Platform) -> Option<PageList> {
        let wiki = platform.form_parameters().search_wiki.as_ref()?;
        let query = platform.form_parameters().search_query.as_ref()?;
        let max = platform.form_parameters().search_max_results.as_ref()?;
        let api = platform.state.get_api_for_wiki(wiki.to_string())?;
        let params = api.params_into(&vec![
            ("action", "query"),
            ("list", "search"),
            ("srsearch", query.as_str()),
        ]);
        let result = api.get_query_api_json_limit(&params, Some(*max)).ok()?;
        let titles = Api::result_array_to_titles(&result);
        let entries = titles
            .iter()
            .map(|title| PageListEntry::new(title.to_owned()))
            .collect();
        let pagelist = PageList::new_from_vec(wiki, entries);
        Some(pagelist)
    }
}

impl SourceSearch {
    pub fn new() -> Self {
        Self {}
    }
}

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone, PartialEq)]
pub struct SourceManual {}

impl DataSource for SourceManual {
    fn name(&self) -> String {
        "manual".to_string()
    }

    fn can_run(&self, platform: &Platform) -> bool {
        match &platform.form_parameters().manual_list {
            Some(_) => match &platform.form_parameters().manual_list_wiki {
                Some(wiki) => !wiki.is_empty(),
                None => false,
            },
            None => false,
        }
    }

    fn run(&self, platform: &Platform) -> Option<PageList> {
        let wiki = platform.form_parameters().manual_list_wiki.as_ref()?;
        let api = platform.state.get_api_for_wiki(wiki.to_string())?;
        let entries: Vec<PageListEntry> = platform
            .form_parameters()
            .manual_list
            .as_ref()?
            .split("\n")
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
            .collect();
        let pagelist = PageList::new_from_vec(wiki, entries);
        Some(pagelist)
    }
}

impl SourceManual {
    pub fn new() -> Self {
        Self {}
    }
}

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone, PartialEq)]
pub struct SourceSparql {}

impl DataSource for SourceSparql {
    fn name(&self) -> String {
        "sparql".to_string()
    }

    fn can_run(&self, platform: &Platform) -> bool {
        match &platform.form_parameters().sparql {
            Some(sparql) => !sparql.is_empty(),
            None => false,
        }
    }

    fn run(&self, platform: &Platform) -> Option<PageList> {
        let sparql = platform.form_parameters().sparql.as_ref()?;
        let api = Api::new("https://www.wikidata.org/w/api.php").ok()?;
        let result = api.sparql_query(sparql.as_str()).ok()?;
        let first_var = result["head"]["vars"][0].as_str()?;
        let entities = api.entities_from_sparql_result(&result, first_var);

        // TODO letters/namespaces are hardcoded?
        // TODO M for commons?
        let ple: Vec<PageListEntry> = entities
            .par_iter()
            .filter_map(|e| match e.chars().next() {
                Some('Q') => Some(PageListEntry::new(Title::new(&e.to_string(), 0))),
                Some('P') => Some(PageListEntry::new(Title::new(&e.to_string(), 120))),
                Some('L') => Some(PageListEntry::new(Title::new(&e.to_string(), 146))),
                _ => None,
            })
            .collect();
        Some(PageList::new_from_vec("wikidatawiki", ple))
    }
}

impl SourceSparql {
    pub fn new() -> Self {
        Self {}
    }
}

//________________________________________________________________________________________________________________________

#[derive(Debug, Clone, PartialEq)]
pub struct SourceDatabase {}

impl DataSource for SourceDatabase {
    fn name(&self) -> String {
        "categories".to_string()
    }

    fn can_run(&self, _platform: &Platform) -> bool {
        false
    }

    fn run(&self, _platform: &Platform) -> Option<PageList> {
        None // TODO
    }
}

impl SourceDatabase {
    pub fn new() -> Self {
        Self {}
    }
}
