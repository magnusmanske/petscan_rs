use crate::pagelist::*;
use crate::platform::Platform;
use mediawiki::api::Api;
use rayon::prelude::*;

pub trait DataSource {
    fn can_run(&self, platform: &Platform) -> bool;
    fn run(&self, platform: &Platform) -> Option<PageList>;
    fn name(&self) -> String;
}

// TODO
// SourceLabels
// SourcePagePile = pagepile
// SourceSearch
// SourceWikidata = wikidata

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
        println!("{}", &wiki);
        let _api = platform.state.get_api_for_wiki(wiki.to_string());
        None
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
                Some('Q') => Some(PageListEntry::new(e.to_string(), 0)),
                Some('P') => Some(PageListEntry::new(e.to_string(), 120)),
                Some('L') => Some(PageListEntry::new(e.to_string(), 146)),
                _ => None,
            })
            .collect();

        let mut ret = PageList::new_from_wiki("wikidatawiki");
        ret.set_entries_from_vec(ple);

        Some(ret)
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
